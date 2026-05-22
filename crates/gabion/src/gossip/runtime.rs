//! Single-threaded gossip event loop.
//!
//! One `tokio::select!` drives five arms — limit/shutdown requests, inbound
//! UDP, outbound writable, peer membership churn, gossip tick. After each
//! iteration the runtime calls [`AggregateStore::apply`] once with whatever
//! rows the arm produced. The runtime owns the [`CellStore`], pre-allocated
//! scratch buffers, the outbound send pool, and the peer table; nothing in
//! the loop allocates after construction.

use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Interval;

use crate::crdt::{
    CellHandle, CellStore, Count, DeltaSink, ExpirationSink, Incarnation, NodeId, Observation,
    ObservationBatch,
};
use crate::discovery::PeerEvent;
use crate::wire::{self, PacketBuf, Packets, WireScratch};

use super::admin::{AdminCommand, AdminSnapshot, PeerEntry};
use super::client::{GossipClient, LimitRequest, Request};
use super::clock::{Clock, TokioClock};
use super::store::AggregateStore;
use super::transport::{GossipTransport, UdpTransport};
use super::{GossipConfig, GossipError};

/// One peer the runtime gossips with. `node_id` is `None` until we receive
/// our first inbound packet from this peer; `peer_slot` caches the
/// `PeerFrontierTable` slot once interned.
struct Peer {
    addr: SocketAddr,
    node_id: Option<NodeId>,
    peer_slot: Option<u16>,
}

/// Data-oriented scratch for per-row frontier acks decoded from one inbound
/// packet. The columns are allocated once at runtime construction and reused
/// for every inbound arm.
struct AckColumns {
    origin_node_ids: Vec<NodeId>,
    incarnations: Vec<Incarnation>,
    origin_sequences: Vec<u64>,
}

impl AckColumns {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            origin_node_ids: Vec::with_capacity(capacity),
            incarnations: Vec::with_capacity(capacity),
            origin_sequences: Vec::with_capacity(capacity),
        }
    }

    fn clear(&mut self) {
        self.origin_node_ids.clear();
        self.incarnations.clear();
        self.origin_sequences.clear();
    }

    fn push(&mut self, origin_node_id: NodeId, incarnation: Incarnation, origin_sequence: u64) {
        self.origin_node_ids.push(origin_node_id);
        self.incarnations.push(incarnation);
        self.origin_sequences.push(origin_sequence);
    }

    fn len(&self) -> usize {
        self.origin_node_ids.len()
    }
}

/// Single-threaded gossip event loop. `!Send + !Sync`.
pub struct GossipRuntime<C, S, T, K>
where
    C: Count,
    S: AggregateStore<C>,
    T: GossipTransport,
    K: Clock,
{
    config: GossipConfig,
    store: CellStore<C>,
    transport: T,
    clock: K,
    requests_rx: mpsc::Receiver<Request>,
    /// Optional admin command channel. `None` in embedded / test setups where
    /// no observability surface is wired up — the matching `select!` arm is
    /// guarded so the production hot path is byte-identical to the no-admin
    /// case.
    admin_rx: Option<mpsc::Receiver<AdminCommand>>,

    // Downstream count store (write-only).
    aggregates: S,

    // Pre-allocated reusable buffers.
    recv_buf: Box<[u8]>,
    scratch: WireScratch,
    obs_buf: ObservationBatch<C>,
    ack_buf: AckColumns,
    sink_buf: DeltaSink<C>,
    expiration_buf: ExpirationSink<C>,
    frame_handles: Vec<CellHandle>,

    // Outbound send pool.
    send_pool: Vec<PacketBuf>,
    send_pending: VecDeque<(SocketAddr, u8)>,
    send_free: Vec<u8>,

    peers: Vec<Peer>,
    rng: SplitMix64,

    // Pending reply for the in-flight limit request. Sent after the
    // bottom-of-loop apply so callers observe "ack ⇒ AggregateStore reflects
    // the increment". Sized to one because only one limit request per
    // iteration is processed.
    pending_reply: Option<oneshot::Sender<()>>,

    // Cumulative count of dropped inbound frames. Used to rate-limit the
    // decode-failure `warn!` to power-of-two transitions so a peer stuck on
    // the wrong version / cluster secret can't flood the log.
    decode_reject_count: u64,

    // High-water mark of `send_pending.len()`. Strictly observability — the
    // `WouldBlock` re-queue path in `drain_one_send` is otherwise invisible
    // to external testers, so this field lets the property test for that
    // path verify the queue actually grew under saturation.
    max_send_pending_depth: usize,

    // Threshold-triggered anti-entropy state. One column per interned rule
    // slot; sized to `STORAGE_RULE_DICTIONARY_CAPACITY` at construction and
    // reused for the runtime's lifetime. Total cost ~1 KiB regardless of
    // rule churn. Indexed by `RuleSlot` to keep the hot path's single
    // dictionary lookup (`RuleDictionary::find`) the only identity work.
    //
    // - `rule_pending[slot]`     — local hits accumulated against rule `slot` since the last
    //   gossip emit. Zeroed by the post-emit sweep in `handle_gossip_tick`.
    // - `rule_last_emit_ms[slot]` — wall-clock stamp of the last emit that included this rule
    //   (also stamped post-emit).
    rule_pending: Box<[u32]>,
    rule_last_emit_ms: Box<[u64]>,

    // Set by `handle_limit_request` when the rule's per-site error budget
    // would be exceeded by the new hit and the `min_emit_interval` floor
    // has elapsed. Read at the top of the run loop, which then skips the
    // `tokio::select!` wait and dispatches a synthetic `Tick` outcome.
    want_immediate_flush: bool,

    // Observability counters surfaced through `AdminSnapshot`. None of
    // them feed back into the hot path — they exist purely so the
    // benchmark harness can split heartbeat-driven emits from
    // threshold-driven ones and compute the *effective* fanout (packets
    // emitted ÷ dirty ticks). Single-threaded `!Send` runtime, so plain
    // `u64`s are cheaper and clearer than atomics.
    ticks_total: u64,
    threshold_fires: u64,
    dirty_ticks: u64,

    _not_send: PhantomData<*const ()>,
}

impl<C, S> GossipRuntime<C, S, UdpTransport, TokioClock>
where
    C: Count,
    S: AggregateStore<C>,
{
    /// Bind a UDP socket on `bind_addr` and assemble the runtime with the
    /// production transport and a wall-clock-anchored `TokioClock`.
    pub async fn bind(
        bind_addr: SocketAddr,
        config: GossipConfig,
        store: CellStore<C>,
        aggregates: S,
    ) -> Result<(Self, GossipClient<C>), GossipError> {
        let transport = UdpTransport::bind(bind_addr).await?;
        Ok(Self::from_parts(
            transport,
            TokioClock::new(),
            config,
            store,
            aggregates,
        ))
    }

    /// Like [`Self::bind`] but threads in an admin command channel. The
    /// admin select arm is only polled when this constructor is used.
    pub async fn bind_with_admin(
        bind_addr: SocketAddr,
        config: GossipConfig,
        store: CellStore<C>,
        aggregates: S,
        admin_rx: mpsc::Receiver<AdminCommand>,
    ) -> Result<(Self, GossipClient<C>), GossipError> {
        let transport = UdpTransport::bind(bind_addr).await?;
        Ok(Self::from_parts_with_admin(
            transport,
            TokioClock::new(),
            config,
            store,
            aggregates,
            Some(admin_rx),
        ))
    }
}

impl<C, S, T, K> GossipRuntime<C, S, T, K>
where
    C: Count,
    S: AggregateStore<C>,
    T: GossipTransport,
    K: Clock,
{
    /// Generic entry point used by tests, simulators, and any non-UDP setup.
    /// Equivalent to [`Self::from_parts_with_admin`] with `admin_rx = None`.
    pub fn from_parts(
        transport: T,
        clock: K,
        config: GossipConfig,
        store: CellStore<C>,
        aggregates: S,
    ) -> (Self, GossipClient<C>) {
        Self::from_parts_with_admin(transport, clock, config, store, aggregates, None)
    }

    /// Generic entry point with an optional admin command channel. The
    /// runtime polls the admin arm only when `admin_rx.is_some()`, so the
    /// no-admin case keeps the original five-arm `select!` shape.
    pub fn from_parts_with_admin(
        transport: T,
        clock: K,
        config: GossipConfig,
        store: CellStore<C>,
        aggregates: S,
        admin_rx: Option<mpsc::Receiver<AdminCommand>>,
    ) -> (Self, GossipClient<C>) {
        let (req_tx, req_rx) = mpsc::channel(config.limit_queue_capacity);

        let recv_buf = vec![0u8; config.wire_limits.max_payload_bytes].into_boxed_slice();
        let scratch = WireScratch::for_store(&store);
        let obs_buf = ObservationBatch::<C>::with_capacity(config.max_cells_per_tick);
        let ack_buf = AckColumns::with_capacity(config.wire_limits.max_cells as usize);
        let sink_buf = DeltaSink::<C>::with_capacity(config.max_cells_per_tick);
        let frame_handles = Vec::with_capacity(config.max_cells_per_tick);
        let expiration_buf = ExpirationSink::<C>::with_capacity(config.max_cells_per_tick);

        let send_pool_size = config.send_queue_capacity.max(1);
        let send_pool: Vec<PacketBuf> = (0..send_pool_size)
            .map(|_| PacketBuf::for_limits(config.wire_limits))
            .collect();
        let send_free: Vec<u8> = (0..send_pool_size as u8).rev().collect();
        let send_pending = VecDeque::with_capacity(send_pool_size);

        let peers: Vec<Peer> = config
            .bootstrap_peers
            .iter()
            .copied()
            .map(|addr| Peer {
                addr,
                node_id: None,
                peer_slot: None,
            })
            .collect();
        let rng = SplitMix64::new(config.rng_seed);

        let rule_capacity = store.rule_dictionary().capacity() as usize;
        let rule_pending = vec![0_u32; rule_capacity].into_boxed_slice();
        let rule_last_emit_ms = vec![0_u64; rule_capacity].into_boxed_slice();

        let runtime = Self {
            config,
            store,
            transport,
            clock,
            requests_rx: req_rx,
            admin_rx,
            aggregates,
            recv_buf,
            scratch,
            obs_buf,
            ack_buf,
            sink_buf,
            expiration_buf,
            frame_handles,
            send_pool,
            send_pending,
            send_free,
            peers,
            rng,
            pending_reply: None,
            decode_reject_count: 0,
            max_send_pending_depth: 0,
            rule_pending,
            rule_last_emit_ms,
            want_immediate_flush: false,
            ticks_total: 0,
            threshold_fires: 0,
            dirty_ticks: 0,
            _not_send: PhantomData,
        };
        let client = GossipClient::new(req_tx);
        (runtime, client)
    }

    /// Local transport address (after binding). Useful for tests that need
    /// the port the kernel chose.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.transport.local_addr()
    }

    /// Run the event loop until shutdown is requested. `peer_events` is the
    /// fourth select arm; pass `futures::stream::empty()` if discovery is
    /// not yet wired up.
    pub async fn run<St>(mut self, peer_events: St) -> Result<(), GossipError>
    where
        St: Stream<Item = PeerEvent>,
    {
        let mut peer_events = std::pin::pin!(peer_events);
        let mut tick = make_tick(&self.config);
        // Once the peer-event stream returns `None` (e.g. `stream::empty()`),
        // never poll it again — otherwise it returns `Ready(None)` on every
        // iteration and the loop spins.
        let mut peer_events_done = false;

        loop {
            // Decide outside the `select!` whether the send arm has work to do.
            // Reading `self.send_pending.is_empty()` inside the `if` guard would
            // re-borrow `self` after we've already split-borrowed for the I/O
            // futures below.
            let have_send = !self.send_pending.is_empty();
            let watch_peers = !peer_events_done;
            let watch_admin = self.admin_rx.is_some();

            let outcome = if self.want_immediate_flush {
                // A previous limit request crossed the per-rule error
                // budget; skip the wait and dispatch a synthetic tick to
                // emit the dirty rows immediately. `tick.tick()` is not
                // consumed, so the heartbeat cadence naturally drifts
                // forward (`MissedTickBehavior::Delay`).
                //
                // Count one threshold-fire per dispatched synthetic
                // tick. Multiple requests in the same `select!`
                // iteration can all set `want_immediate_flush=true`,
                // but only one synthetic tick consumes the flag, so
                // bumping here gives the bench an accurate split
                // between threshold fires and heartbeat fires.
                self.want_immediate_flush = false;
                self.threshold_fires = self.threshold_fires.saturating_add(1);
                ArmOutcome::Tick
            } else {
                // Split-borrow scope: we hand the I/O futures non-overlapping
                // pieces of `self` so they compose inside `select!`.
                let Self {
                    transport,
                    recv_buf,
                    requests_rx,
                    admin_rx,
                    ..
                } = &mut self;

                tokio::select! {
                    biased;

                    req = requests_rx.recv() => ArmOutcome::Request(req),
                    recv = transport.recv_from(recv_buf) => ArmOutcome::Inbound(recv),
                    _ = transport.writable(), if have_send => ArmOutcome::Writable,
                    evt = peer_events.next(), if watch_peers => ArmOutcome::PeerEvent(evt),
                    cmd = recv_admin(admin_rx), if watch_admin => ArmOutcome::Admin(cmd),
                    _ = tick.tick() => ArmOutcome::Tick,
                }
            };

            match outcome {
                ArmOutcome::Request(None) | ArmOutcome::Request(Some(Request::Shutdown)) => {
                    return Ok(());
                }
                ArmOutcome::Request(Some(Request::Limit(r))) => self.handle_limit_request(r),
                ArmOutcome::Inbound(Ok((n, src))) => self.handle_inbound(n, src),
                ArmOutcome::Inbound(Err(e)) => return Err(e.into()),
                ArmOutcome::Writable => self.drain_one_send()?,
                ArmOutcome::PeerEvent(Some(evt)) => self.handle_peer_event(evt),
                ArmOutcome::PeerEvent(None) => peer_events_done = true,
                ArmOutcome::Admin(Some(cmd)) => self.handle_admin_command(cmd),
                ArmOutcome::Admin(None) => self.admin_rx = None,
                ArmOutcome::Tick => self.handle_gossip_tick(),
            }

            // One apply per iteration covering both row sources. CRDT-free
            // arms (peer event, drain, idle ticks) leave both empty; the
            // branch short-circuits then.
            if !self.sink_buf.is_empty() || !self.expiration_buf.is_empty() {
                self.aggregates.apply(&self.sink_buf, &self.expiration_buf);
                self.sink_buf.clear();
                self.expiration_buf.clear();
            }

            if self.send_pending.len() > self.max_send_pending_depth {
                self.max_send_pending_depth = self.send_pending.len();
            }

            // Ack any pending limit-request AFTER the apply ran — callers
            // observe "ack ⇒ AggregateStore reflects the increment".
            // Receiver may have dropped; that is not an error.
            if let Some(reply) = self.pending_reply.take() {
                let _ = reply.send(());
            }
        }
    }

    // -- per-arm handlers ---------------------------------------------------

    fn handle_limit_request(&mut self, req: LimitRequest) {
        // Threshold-triggered anti-entropy: track local hits per rule slot
        // and fire an immediate flush once the per-site safe zone (Sharfman,
        // Schuster, Keren — SIGMOD 2006) would be crossed, calibrated by
        // the Olston/Jiang/Widom (SIGMOD 2003) error budget. Per-site
        // budget: ε_R = max(1, L × target_err_bps / (10_000 × N)). Total
        // cluster-wide unreplicated error per rule is bounded by N × ε_R.
        // `saturating_add` keeps the column monotone across the gap between
        // crossing and reset; the post-emit sweep in `handle_gossip_tick`
        // zeroes the slot once a frame is queued.
        //
        // Falls through silently when the rule isn't interned yet (first
        // hit for that fingerprint) — the next heartbeat picks it up.
        if let Some(slot) = self.store.rule_dictionary().find(req.rule_fingerprint) {
            let idx = slot as usize;
            let pending =
                self.rule_pending[idx].saturating_add(req.hits.min(u32::MAX as u64) as u32);
            self.rule_pending[idx] = pending;
            let peers = self.peers.len().max(1) as u64;
            let limit = req.rule_limit.max(1);
            let bps = self.config.target_err_bps.max(1) as u64;
            let epsilon = ((limit.saturating_mul(bps)) / (10_000 * peers)).max(1);
            if (pending as u64) > epsilon {
                // The runtime's clock is the canonical floor reference, not
                // `req.now_millis`. Adapter-supplied wall clock and
                // `self.clock.now_millis()` may live on different epochs
                // (wall vs paused-virtual time in tests), and only the
                // runtime-local clock is monotone with the post-emit
                // stamp written by `handle_gossip_tick`. Subtraction is
                // still well-defined in monotonic-rate terms — both fire
                // and reset use the same clock — so the floor is correct.
                // `last == 0` is the "never emitted" sentinel so the very
                // first crossing always fires; the post-emit sweep stamps
                // `max(1)` so the sentinel is not re-introduced.
                let now = self.clock.now_millis();
                let last = self.rule_last_emit_ms[idx];
                let floor = self.config.min_emit_interval.as_millis() as u64;
                if last == 0 || now.saturating_sub(last) >= floor {
                    self.want_immediate_flush = true;
                }
            }
        }

        self.obs_buf.clear();
        self.sink_buf.clear();
        self.obs_buf.push(Observation {
            rule_fingerprint: req.rule_fingerprint,
            key_hash: req.key_hash,
            bucket: req.bucket,
            // ingest_local ignores origin/incarnation and forces local identity.
            origin: NodeId(0),
            incarnation: 0,
            count: C::saturating_from_u64(req.hits),
            last_update_millis: req.now_millis,
        });
        self.store.ingest_local(&self.obs_buf, &mut self.sink_buf);
        // Reply is sent at the bottom of this loop iteration, AFTER the
        // post-iteration apply runs. Caller observes "ack ⇒ AggregateStore
        // reflects the increment".
        self.pending_reply = Some(req.reply);
    }

    fn handle_inbound(&mut self, n: usize, src: SocketAddr) {
        self.obs_buf.clear();
        self.ack_buf.clear();
        self.sink_buf.clear();
        let bytes = &self.recv_buf[..n];
        let decoded = {
            let obs_buf = &mut self.obs_buf;
            let ack_buf = &mut self.ack_buf;
            let mut on_cell = |cell: wire::WireCell<C>| {
                ack_buf.push(
                    cell.origin_node_id,
                    cell.origin_incarnation,
                    cell.origin_sequence,
                );
                obs_buf.push(Observation {
                    rule_fingerprint: cell.rule_fingerprint,
                    key_hash: cell.key_hash,
                    bucket: cell.bucket,
                    origin: cell.origin_node_id,
                    incarnation: cell.origin_incarnation,
                    count: cell.count,
                    last_update_millis: cell.last_update_millis,
                });
            };
            match self.config.auth_key.as_ref() {
                Some(key) => wire::decode_auth_visit::<C>(
                    bytes,
                    key,
                    self.config.wire_limits,
                    |_| true,
                    &mut on_cell,
                ),
                None => wire::decode_unauth_visit::<C>(
                    bytes,
                    self.config.wire_limits,
                    |_| true,
                    &mut on_cell,
                ),
            }
        };
        let summary = match decoded {
            Ok(s) => s,
            Err(err) => {
                self.decode_reject_count = self.decode_reject_count.saturating_add(1);
                if self.decode_reject_count.is_power_of_two() {
                    tracing::warn!(
                        peer = %src,
                        error = %err,
                        rejected_total = self.decode_reject_count,
                        "Could not understand a gossip message from this \
                         peer; dropping it. Common causes: the peer is \
                         running a different gabion version, the peers are \
                         configured with different counter sizes \
                         (`storage.count_width`), the cluster authentication \
                         key (`gossip.auth_key`) does not match between \
                         peers, or the peer is sending messages larger than \
                         this node's `gossip.max_payload_bytes`. Check the \
                         peer's version and gabion config to find the \
                         mismatch.",
                    );
                }
                return;
            }
        };

        self.store.merge_remote(&self.obs_buf, &mut self.sink_buf);

        // Frontier update — latency optimization. Best-effort: missing slot
        // or missing node entry just means we don't get the skip.
        let sender_id = NodeId(summary.header.sender_node_id);
        if let Some(peer_slot) = self.store.peer_frontiers_mut().intern_peer(sender_id) {
            for i in 0..self.ack_buf.len() {
                let origin_id = self.ack_buf.origin_node_ids[i];
                let incarnation = self.ack_buf.incarnations[i];
                let sequence = self.ack_buf.origin_sequences[i];
                if let Some(node_slot) = self.store.node_dictionary().find(origin_id, incarnation) {
                    self.store
                        .peer_frontiers_mut()
                        .record_acked(peer_slot, node_slot, sequence);
                }
            }
            // Cache the peer_slot on the matching peer entry so future ticks
            // can prune without re-interning. `node_id` and `peer_slot` are
            // always set together — see `handle_gossip_tick`, which relies
            // on this invariant.
            if let Some(peer) = self.peers.iter_mut().find(|p| p.addr == src) {
                peer.node_id = Some(sender_id);
                peer.peer_slot = Some(peer_slot);
            }
        }
    }

    fn drain_one_send(&mut self) -> Result<(), GossipError> {
        let Some((dst, slot)) = self.send_pending.pop_front() else {
            return Ok(());
        };
        let buf = &self.send_pool[slot as usize];
        match self.transport.try_send_to(buf.as_bytes(), dst) {
            Ok(_) => self.send_free.push(slot),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.send_pending.push_front((dst, slot));
            }
            Err(e) => {
                self.send_free.push(slot);
                return Err(e.into());
            }
        }
        Ok(())
    }

    fn handle_admin_command(&mut self, cmd: AdminCommand) {
        match cmd {
            AdminCommand::Snapshot { reply } => {
                let snapshot = AdminSnapshot {
                    local_identity: self.store.local_identity(),
                    peers: self
                        .peers
                        .iter()
                        .map(|p| PeerEntry {
                            addr: p.addr,
                            node_id: p.node_id,
                            peer_slot: p.peer_slot,
                        })
                        .collect(),
                    store_stats: self.store.stats(),
                    local_dirty_len: self.store.local_dirty().len(),
                    forwarded_dirty_len: self.store.forwarded_dirty().len(),
                    send_pending_depth: self.send_pending.len(),
                    decode_reject_count: self.decode_reject_count,
                    max_send_pending_depth: self.max_send_pending_depth,
                    ticks_total: self.ticks_total,
                    threshold_fires: self.threshold_fires,
                    dirty_ticks: self.dirty_ticks,
                };
                // Caller may have dropped the receiver; not an error.
                let _ = reply.send(snapshot);
            }
        }
    }

    fn handle_peer_event(&mut self, evt: PeerEvent) {
        match evt {
            PeerEvent::Added(p) => {
                if !self.peers.iter().any(|x| x.addr == p.addr) {
                    self.peers.push(Peer {
                        addr: p.addr,
                        node_id: None,
                        peer_slot: None,
                    });
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.peers.len(),
                        "Peer joined the cluster.",
                    );
                }
            }
            PeerEvent::Removed(p) => {
                if let Some(pos) = self.peers.iter().position(|x| x.addr == p.addr) {
                    let removed = self.peers.remove(pos);
                    if let Some(node_id) = removed.node_id {
                        self.store.peer_frontiers_mut().remove_peer(node_id);
                    }
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.peers.len(),
                        "Peer left the cluster.",
                    );
                }
            }
        }
    }

    fn handle_gossip_tick(&mut self) {
        // Step 0: bump the total-ticks counter. Tracked unconditionally
        // (even for empty / no-peer ticks) so the bench can split the
        // run's wall time into heartbeat ticks vs threshold fires.
        self.ticks_total = self.ticks_total.saturating_add(1);

        // Step 1: expire aged-out cells so we don't gossip them.
        let now_millis = self.clock.now_millis();
        self.expiration_buf.clear();
        self.store.expire_at(now_millis, &mut self.expiration_buf);

        if self.store.is_empty() || self.peers.is_empty() || self.send_free.is_empty() {
            return;
        }

        // The store was non-empty when this tick fired — bench harness
        // counts this as a "dirty tick" so effective-fanout reports
        // `packets / dirty_ticks` rather than `packets / wall_ticks`.
        // Bumped before the peer-pick math runs so synthetic threshold
        // ticks (which by construction have local_dirty>0) always count.
        self.dirty_ticks = self.dirty_ticks.saturating_add(1);

        // Step 2: pick `fanout` distinct peers via a partial Fisher-Yates
        // shuffle of `self.peers` in place. With-replacement sampling would
        // silently break Demers' O(log N) convergence bound — selecting the
        // same peer twice in one tick burns a send-pool slot encoding the
        // same frame for the same peer. `self.peers` has no ordering
        // contract elsewhere (lookups are by `addr`), so shuffling is free.
        //
        // Adaptive fanout (Verma & Ooi, ICDCS 2005): grow the per-tick
        // fanout with the dirty-set bit length so a sudden burst converges
        // in O(log N) rounds rather than O(N / fanout). `bit_length` for
        // a u64 is `64 - leading_zeros`; for n ≥ 1 this is
        // `floor(log2(n)) + 1`, which is what we want — a single dirty
        // cell already deserves fanout ≥ 1. Capped at the peer count;
        // falls back to `config.fanout` when dirty is small.
        let dirty = self.store.local_dirty().len() + self.store.forwarded_dirty().len();
        let log_dirty = (64 - (dirty as u64).leading_zeros()) as usize;
        let n = self.peers.len();
        let pick_count = self.config.fanout.max(log_dirty).min(n);
        for i in 0..pick_count {
            let j = i + (self.rng.next_u64() as usize) % (n - i);
            self.peers.swap(i, j);
        }

        for peer_idx in 0..pick_count {
            if self.send_free.is_empty() {
                break;
            }
            let peer_addr = self.peers[peer_idx].addr;
            // `handle_inbound` writes `node_id` and `peer_slot` together —
            // when one is `Some`, the other is too. So a single read of
            // `peer_slot` is the full pruning decision.
            let peer_slot = self.peers[peer_idx].peer_slot;

            self.frame_handles.clear();
            match peer_slot {
                Some(slot) => {
                    self.store.fill_gossip_frame_for_peer(
                        self.config.max_cells_per_tick,
                        slot,
                        &mut self.frame_handles,
                    );
                }
                None => {
                    // Bootstrap fallback: we haven't heard from this peer
                    // yet, so we don't know what to prune. Send an unpruned
                    // frame; the next inbound from them caches the slot.
                    self.store
                        .fill_gossip_frame(self.config.max_cells_per_tick, &mut self.frame_handles);
                }
            }
            if self.frame_handles.is_empty() {
                continue;
            }
            self.encode_packets_for(peer_addr);
        }

        // Reset per-rule pending. The sweep treats every rule with
        // non-zero pending as "touched in expectation by the tick" — the
        // dirty-ring drain in `fill_gossip_frame*` doesn't tell us which
        // rule slots actually made it onto the wire, but mass conservation
        // (Kempe, Dobra, Gehrke — FOCS 2003) says every pending hit either
        // rode out in this frame or sits in the dirty ring awaiting the
        // next one. Under UDP loss the residual is repaired by the next
        // round; the N × ε_R bound holds in expectation. A strict bound
        // would require ack-aware reset against `peer_frontiers`,
        // deferred until measurements show drift.
        let stamp = now_millis.max(1);
        for slot in 0..self.rule_pending.len() {
            if self.rule_pending[slot] != 0 {
                self.rule_pending[slot] = 0;
                self.rule_last_emit_ms[slot] = stamp;
            }
        }
    }

    fn encode_packets_for(&mut self, dst: SocketAddr) {
        let header = wire::Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender_node_id: self.store.local_identity().node_id.0,
            sender_incarnation: self.store.local_identity().incarnation,
            count_width: 0,
            cell_count: 0,
            body_len: 0,
            min_origin_sequence: 0,
            max_origin_sequence: 0,
            flags: 0,
        };

        let packets_result = match self.config.auth_key.as_ref() {
            Some(key) => Packets::<C>::auth(
                header,
                &self.store,
                &self.frame_handles,
                key,
                &mut self.scratch,
                self.config.wire_limits,
            ),
            None => Packets::<C>::unauth(
                header,
                &self.store,
                &self.frame_handles,
                &mut self.scratch,
                self.config.wire_limits,
            ),
        };
        let mut packets = match packets_result {
            Ok(p) => p,
            Err(_) => return,
        };

        while let Some(slot) = self.send_free.pop() {
            let buf = &mut self.send_pool[slot as usize];
            match packets.next_into(buf) {
                Ok(Some(_)) => self.send_pending.push_back((dst, slot)),
                Ok(None) => {
                    self.send_free.push(slot);
                    break;
                }
                Err(_) => {
                    self.send_free.push(slot);
                    break;
                }
            }
        }
        drop(packets);
    }
}

enum ArmOutcome {
    Request(Option<Request>),
    Inbound(io::Result<(usize, SocketAddr)>),
    Writable,
    PeerEvent(Option<PeerEvent>),
    Admin(Option<AdminCommand>),
    Tick,
}

/// Read one admin command. Only invoked when `admin_rx.is_some()` — the
/// `select!` guard ensures the unwrap is safe.
async fn recv_admin(rx: &mut Option<mpsc::Receiver<AdminCommand>>) -> Option<AdminCommand> {
    rx.as_mut()
        .expect("admin arm gated by admin_rx.is_some()")
        .recv()
        .await
}

fn make_tick(config: &GossipConfig) -> Interval {
    let mut tick = tokio::time::interval(config.tick_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick
}

/// SplitMix64 — small, fast, deterministic. Used only for peer sampling so
/// no security properties are required.
pub(super) struct SplitMix64(u64);

impl SplitMix64 {
    pub(super) fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
    }

    pub(super) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}
