//! Single-threaded gossip event loop.
//!
//! One `tokio::select!` drives five arms — limit/shutdown requests, inbound
//! UDP, outbound writable, peer membership churn, gossip tick. After each
//! iteration the runtime calls [`AggregateStore::apply`] once with whatever
//! rows the arm produced. The runtime owns the [`CellStore`], pre-allocated
//! scratch buffers, the outbound send pool, and the peer table; nothing in
//! the loop allocates after construction.
//!
//! Dissemination is direct mail with cumulative acks: every round each origin
//! sends its own changes to every peer, nothing is forwarded, and each packet
//! carries the sender's cumulative ack of the receiver's own cells. See
//! [`super::peer`] for the per-peer state and the README's "Design notes" for
//! the source and adaptations.

use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::crdt::{
    CellHandle, CellStore, Count, DeltaSink, ExpirationSink, NodeId, Observation, ObservationBatch,
};
use crate::discovery::PeerEvent;
use crate::wire::{self, PacketBuf, Packets, WireScratch};

use super::admin::{AdminCommand, AdminSnapshot, PeerEntry};
use super::client::{GossipClient, LimitRequest, Request};
#[cfg(feature = "transport-udp")]
use super::clock::TokioClock;
use super::clock::{Clock, Ticker};
use super::peer::Peer;
use super::store::AggregateStore;
use super::transport::GossipTransport;
#[cfg(feature = "transport-udp")]
use super::transport::UdpTransport;
use super::{GossipConfig, GossipError};

mod pending;
use pending::PendingHits;

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
    sink_buf: DeltaSink<C>,
    expiration_buf: ExpirationSink<C>,
    frame_handles: Vec<CellHandle>,

    // Outbound send pool.
    send_pool: Vec<PacketBuf>,
    send_pending: VecDeque<(SocketAddr, SendSlot)>,
    send_free: Vec<SendSlot>,
    // Floor of the per-peer retransmission timeout: one tick.
    rto_floor_ms: u64,

    peers: Vec<Peer>,
    /// Most peers `peers` may hold; it is allocated for that many.
    peer_capacity: usize,
    /// Peer a round starts with, advanced every round so that a send pool
    /// smaller than the peer list does not starve the peers at its tail.
    next_round_start: usize,

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

    // Threshold-triggered anti-entropy state, sized at construction and
    // reused for the runtime's lifetime.
    //
    // - `pending` — local hits per `(rule, key)` not yet carried by a gossip frame. Sized to the
    //   local dirty ring, which bounds the keys that can be in flight between emits. Cleared per
    //   emitted cell by `clear_emitted_pending`.
    // - `rule_last_emit_ms[slot]` — clock stamp of the last threshold fire or emit that carried
    //   pending hits of rule `slot`; the `min_emit_interval` floor measures from it. One column
    //   per interned rule slot, indexed by `RuleSlot`.
    pending: PendingHits,
    rule_last_emit_ms: Box<[u64]>,

    // Set by `handle_limit_request` when a key's per-site error budget
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
    // The adaptive-decision outputs, so observers can *see* the runtime
    // react rather than infer it. `last_effective_fanout` is the
    // number of peers the most recent data-carrying round sent to (direct
    // mail sends to every peer with something outstanding);
    // `peak_effective_fanout` is its high-water mark since startup.
    // `last_error_budget` is the per-key
    // error budget ε the most recent `handle_limit_request` computed — the
    // threshold one key's accumulated `pending` must cross to trigger
    // an eager (threshold) flush. All three are decision *outputs* that are
    // otherwise lost when their enclosing scope ends.
    last_effective_fanout: usize,
    peak_effective_fanout: usize,
    last_error_budget: u64,

    _not_send: PhantomData<*const ()>,
}

#[cfg(feature = "transport-udp")]
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
        let sink_buf = DeltaSink::<C>::with_capacity(config.max_cells_per_tick);
        let frame_handles = Vec::with_capacity(config.max_cells_per_tick);
        let expiration_buf = ExpirationSink::<C>::with_capacity(config.max_cells_per_tick);

        let peer_capacity = usize::from(store.peer_capacity());
        let mut peers: Vec<Peer> = Vec::with_capacity(peer_capacity);
        for &addr in &config.bootstrap_peers {
            if peers.len() == peer_capacity {
                warn_peer_capacity(addr, peer_capacity, config.bootstrap_peers.len());
                break;
            }
            peers.push(Peer::new(addr));
        }

        // Direct mail queues a packet for every peer every round, so the pool
        // holds `GOSSIP_SEND_SLOTS_PER_PEER` slots for every peer the table can
        // hold, or `send_queue_capacity` if that is more.
        let send_pool_size = config
            .send_queue_capacity
            .max(1)
            .max(peer_capacity.saturating_mul(crate::defaults::GOSSIP_SEND_SLOTS_PER_PEER));
        let send_pool: Vec<PacketBuf> = (0..send_pool_size)
            .map(|_| PacketBuf::for_limits(config.wire_limits))
            .collect();
        let send_free: Vec<SendSlot> = (0..send_pool_size as SendSlot).rev().collect();
        let send_pending = VecDeque::with_capacity(send_pool_size);
        let rto_floor_ms = u64::try_from(config.tick_interval.as_millis()).unwrap_or(u64::MAX);

        let rule_capacity = store.rule_dictionary().capacity() as usize;
        let pending = PendingHits::with_capacity(store.local_dirty().capacity() as usize);
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
            sink_buf,
            expiration_buf,
            frame_handles,
            send_pool,
            send_pending,
            send_free,
            rto_floor_ms,
            peers,
            peer_capacity,
            next_round_start: 0,
            pending_reply: None,
            decode_reject_count: 0,
            max_send_pending_depth: 0,
            pending,
            rule_last_emit_ms,
            want_immediate_flush: false,
            ticks_total: 0,
            threshold_fires: 0,
            dirty_ticks: 0,
            last_effective_fanout: 0,
            peak_effective_fanout: 0,
            last_error_budget: 0,
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
        let mut tick = self.clock.ticker(self.config.tick_interval);
        // Once the peer-event stream returns `None` (e.g. `stream::empty()`),
        // never poll it again — otherwise it returns `Ready(None)` on every
        // iteration and the loop spins.
        let mut peer_events_done = false;

        loop {
            // Decide outside the `select!` whether the send arm has work to do.
            // Reading `self.send_pending.is_empty()` inside the `if` guard
            // would re-borrow `self` after we've already
            // split-borrowed for the I/O futures below.
            let have_send = !self.send_pending.is_empty();
            let watch_peers = !peer_events_done;
            let watch_admin = self.admin_rx.is_some();

            let outcome = if self.want_immediate_flush {
                // A previous limit request crossed a key's error
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
        // Threshold-triggered anti-entropy: track local hits per (rule, key)
        // and fire an immediate flush once the per-site safe zone (Sharfman,
        // Schuster, Keren — SIGMOD 2006) would be crossed, calibrated by
        // the Olston/Jiang/Widom (SIGMOD 2003) error budget. The limit L
        // applies to each key, so the budget does too:
        // ε_R = max(1, L × target_err_bps / (10_000 × N)), and the
        // cluster-wide unreplicated error per key is bounded by N × ε_R.
        // The count saturates across the gap between crossing and reset;
        // `clear_emitted_pending` zeroes it once the key's cell is framed.
        //
        // Falls through silently when the rule isn't interned yet (first
        // hit for that fingerprint) — the next heartbeat picks it up.
        if let Some(slot) = self.store.rule_dictionary().find(req.rule_fingerprint) {
            let idx = slot as usize;
            let pending = self.pending.add(slot, req.key_hash, req.hits);
            let peers = self.peers.len().max(1) as u64;
            let limit = req.rule_limit.max(1);
            let bps = self.config.target_err_bps.max(1) as u64;
            let epsilon = ((limit.saturating_mul(bps)) / (10_000 * peers)).max(1);
            // Surface the budget so observers can see the threshold a key's
            // accumulated `pending` must cross to trigger an eager flush.
            self.last_error_budget = epsilon;
            if pending > epsilon {
                // The runtime's clock is the canonical floor reference, not
                // `req.now_millis`. Adapter-supplied wall clock and
                // `self.clock.now_millis()` may live on different epochs
                // (wall vs paused-virtual time in tests), and only the
                // runtime-local clock is monotone with the post-emit
                // stamp written by `handle_gossip_tick`. Subtraction is
                // still well-defined in monotonic-rate terms — both fire
                // and reset use the same clock — so the floor is correct.
                // `last == 0` is the "never emitted" sentinel so the very
                // first crossing always fires; the stamp is `max(1)` so the
                // sentinel is not re-introduced.
                //
                // The fire stamps the floor itself: whether the firing key's
                // cell makes the next frame is up to `max_cells_per_tick` and
                // the store's capacity, and a rule must not fire on every hit
                // when it does not.
                let now = self.clock.now_millis();
                let last = self.rule_last_emit_ms[idx];
                let floor = self.config.min_emit_interval.as_millis() as u64;
                if last == 0 || now.saturating_sub(last) >= floor {
                    self.want_immediate_flush = true;
                    self.rule_last_emit_ms[idx] = now.max(1);
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
        self.pending_reply = req.reply;
    }

    fn handle_inbound(&mut self, n: usize, src: SocketAddr) {
        self.obs_buf.clear();
        self.sink_buf.clear();
        let bytes = &self.recv_buf[..n];
        let decoded = {
            let obs_buf = &mut self.obs_buf;
            let mut on_cell = |cell: wire::WireCell<C>| {
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

        let rejects_before = self.store_rejects();
        self.store.merge_remote(&self.obs_buf, &mut self.sink_buf);
        let merged_all = self.store_rejects() == rejects_before;

        // Acks and reliability state. A packet from an address that discovery
        // has not announced yet is still merged, but cannot be acknowledged:
        // its sender re-sends after its retransmission timeout.
        let now_ms = self.clock.now_millis();
        let local_incarnation = self.store.local_identity().incarnation;
        let min_rto_ms = self.rto_floor_ms;
        let Some(idx) = self.peers.iter().position(|p| p.addr == src) else {
            return;
        };
        let peer = &mut self.peers[idx];
        peer.receive(
            &summary.header,
            local_incarnation,
            merged_all,
            now_ms,
            min_rto_ms,
        );
        // The peer can be reached and is owed a retransmission (typically it
        // was backed off and has just been heard again): send it now rather
        // than at the next round.
        if peer.retransmit_due(now_ms) {
            let share = (self.send_free.len() / self.peers.len()).max(1);
            self.send_round_to(idx, share, now_ms);
        }
    }

    /// Cells the store refused for want of capacity, ever. A merge that
    /// raises it must not be acknowledged.
    fn store_rejects(&self) -> u64 {
        let stats = self.store.stats();
        stats
            .cell_store_full_rejects
            .saturating_add(stats.rule_dictionary_full_rejects)
            .saturating_add(stats.node_dictionary_full_rejects)
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
                            node_id: p.node_id(),
                        })
                        .collect(),
                    store_stats: self.store.stats(),
                    local_dirty_len: self.store.local_dirty().len(),
                    send_pending_depth: self.send_pending.len(),
                    decode_reject_count: self.decode_reject_count,
                    max_send_pending_depth: self.max_send_pending_depth,
                    ticks_total: self.ticks_total,
                    threshold_fires: self.threshold_fires,
                    dirty_ticks: self.dirty_ticks,
                    last_effective_fanout: self.last_effective_fanout,
                    peak_effective_fanout: self.peak_effective_fanout,
                    last_error_budget: self.last_error_budget,
                };
                // Caller may have dropped the receiver; not an error.
                let _ = reply.send(snapshot);
            }
            #[cfg(feature = "cell-dump")]
            AdminCommand::CellDump { reply } => {
                let _ = reply.send(self.build_cell_dump());
            }
        }
    }

    /// Build a full per-cell snapshot of the store. Walks every active cell,
    /// resolving its rule and origin descriptors into owned scalars so the
    /// reply crosses the admin channel without aliasing `self.store`. Off the
    /// hot path — feature-gated and only reachable via the admin arm.
    #[cfg(feature = "cell-dump")]
    fn build_cell_dump(&self) -> super::admin::CellDumpSnapshot {
        use super::admin::{CellDumpEntry, CellDumpSnapshot};

        let store = &self.store;
        let rules = store.rule_dictionary();
        let node_dict = store.node_dictionary();
        let mut cells = Vec::with_capacity(store.stats().active_cells as usize);
        for handle in store.active_handles() {
            let Some(row) = store.get(handle) else {
                continue;
            };
            let rule = rules.descriptor(row.key.rule);
            let origin = node_dict.descriptor(row.key.origin);
            let count: u64 = row.count.into();
            cells.push(CellDumpEntry {
                rule_fingerprint: rule.map_or(0, |r| r.fingerprint),
                key_hash: row.key.key_hash.0,
                bucket: row.key.bucket,
                count,
                last_update_millis: row.last_update_millis,
                origin_sequence: row.origin_sequence,
                origin_node_id: origin.map(|n| n.node_id.0),
                origin_incarnation: origin.map(|n| n.incarnation),
                rule_window_millis: rule.map_or(0, |r| r.window_millis),
                rule_bucket_millis: rule.map_or(0, |r| r.bucket_millis),
                rule_limit: rule.map_or(0, |r| r.limit),
                applies_locally: rule.is_some_and(|r| r.applies_locally()),
            });
        }
        CellDumpSnapshot {
            local_identity: store.local_identity(),
            cells,
        }
    }

    fn handle_peer_event(&mut self, evt: PeerEvent) {
        match evt {
            PeerEvent::Added(p) => {
                if self.peers.iter().any(|x| x.addr == p.addr) {
                    return;
                }
                if self.peers.len() == self.peer_capacity {
                    warn_peer_capacity(p.addr, self.peer_capacity, self.peers.len() + 1);
                } else {
                    self.peers.push(Peer::new(p.addr));
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.peers.len(),
                        "Peer joined the cluster.",
                    );
                }
            }
            PeerEvent::Removed(p) => {
                if let Some(pos) = self.peers.iter().position(|x| x.addr == p.addr) {
                    self.peers.remove(pos);
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

        if self.peers.is_empty() || self.send_free.is_empty() {
            return;
        }

        // Step 2: one round. Every peer gets, in the same pass, the cells it
        // has not been sent, the cells whose retransmission timeout has
        // expired, and our ack of its own cells. Peers are visited from a
        // rotating start so a send pool smaller than the peer list is shared
        // fairly across rounds.
        let n = self.peers.len();
        let start = self.next_round_start % n;
        let mut sent_data_to = 0;
        let mut visited = 0;
        for k in 0..n {
            if self.send_free.is_empty() {
                break;
            }
            visited += 1;
            // An even share of the free slots, so one large delta cannot take
            // the whole pool and leave later peers with nothing.
            let share = (self.send_free.len() / (n - k)).max(1);
            if self.send_round_to((start + k) % n, share, now_millis) {
                sent_data_to += 1;
            }
        }
        // Next round starts at the first peer this one did not reach, so a
        // pool that runs dry mid-round cannot starve the same tail of peers.
        self.next_round_start = start + visited;
        if sent_data_to > 0 {
            self.dirty_ticks = self.dirty_ticks.saturating_add(1);
            self.last_effective_fanout = sent_data_to;
            self.peak_effective_fanout = self.peak_effective_fanout.max(sent_data_to);
        }
    }

    /// Queue this round's packets for one peer: its delta, or failing that a
    /// standalone ack if one is owed. Returns whether data was queued.
    fn send_round_to(&mut self, peer_idx: usize, max_packets: usize, now_ms: u64) -> bool {
        let min_rto_ms = self.rto_floor_ms;
        let (after, retransmit) = self.peers[peer_idx].delta_after(now_ms);
        self.store.delta_since(
            after,
            self.config.max_cells_per_tick,
            &mut self.frame_handles,
        );
        let peer = &mut self.peers[peer_idx];
        let (addr, ack_owed) = (peer.addr, peer.ack_owed());
        let (ack_sequence, ack_incarnation) = peer.ack_fields();
        let header = wire::Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender_node_id: self.store.local_identity().node_id.0,
            sender_incarnation: self.store.local_identity().incarnation,
            range_start: after,
            ack_sequence,
            ack_incarnation,
            ..wire::Header::default()
        };

        if self.frame_handles.is_empty() {
            if retransmit {
                peer.nothing_outstanding();
            }
            if ack_owed && let Some(slot) = self.send_free.pop() {
                wire::encode_ack_only::<C>(
                    header,
                    self.config.auth_key.as_ref(),
                    &mut self.send_pool[slot as usize],
                );
                self.send_pending.push_back((addr, slot));
                self.peers[peer_idx].ack_sent();
            }
            return false;
        }

        let Some((top, whole_frame)) = self.encode_packets_for(addr, max_packets, header) else {
            return false;
        };
        if whole_frame {
            self.clear_emitted_pending(now_ms.max(1));
        }
        self.peers[peer_idx].sent(top, retransmit, now_ms, min_rto_ms);
        true
    }

    /// Zero the pending count of every local cell in the frame just encoded
    /// and stamp its rule's `rule_last_emit_ms`, so the `min_emit_interval`
    /// floor measures from this emit. A key whose cell did not fit in any
    /// frame keeps its count and can fire again on its next hit. Clearing on
    /// send, not on ack, means the N × ε_R bound holds in expectation under
    /// UDP loss; retransmission resends what was lost. Only called once the
    /// whole frame is queued for sending: a frame cut short leaves its counts.
    fn clear_emitted_pending(&mut self, stamp: u64) {
        let local = self.store.local_node_slot();
        for &handle in &self.frame_handles {
            if self.pending.is_empty() {
                return;
            }
            let Some(row) = self.store.get(handle) else {
                continue;
            };
            if row.key.origin == local && self.pending.clear(row.key.rule, row.key.key_hash) {
                self.rule_last_emit_ms[usize::from(row.key.rule)] = stamp;
            }
        }
    }

    /// Encode `frame_handles` into at most `max_packets` send slots. Returns
    /// the highest origin sequence of the last packet queued and whether the
    /// whole frame was queued, or `None` if no packet was. Cells that did not
    /// fit are not sent; they stay above the peer's `sent_upto` and go out in
    /// the next round.
    fn encode_packets_for(
        &mut self,
        dst: SocketAddr,
        max_packets: usize,
        header: wire::Header,
    ) -> Option<(u64, bool)> {
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
        let mut packets = packets_result.ok()?;

        let mut top = None;
        let mut whole_frame = false;
        for _ in 0..max_packets {
            let Some(slot) = self.send_free.pop() else {
                break;
            };
            match packets.next_into(&mut self.send_pool[slot as usize]) {
                Ok(Some(written)) => {
                    self.send_pending.push_back((dst, slot));
                    if written.cells_emitted > 0 {
                        top = Some(written.max_origin_sequence);
                    }
                    if written.flags & wire::FLAG_MORE == 0 {
                        whole_frame = true;
                        break;
                    }
                }
                Ok(None) => {
                    self.send_free.push(slot);
                    whole_frame = true;
                    break;
                }
                Err(_) => {
                    self.send_free.push(slot);
                    break;
                }
            }
        }
        packets.discard_rest();
        top.map(|top| (top, whole_frame))
    }
}

/// Index of a slot in the send pool.
type SendSlot = u32;

/// A peer was announced while the peer table is full. It is not gossiped
/// with: it never receives this node's changes, and its own are merged but
/// never acknowledged. Discovery announces peers once, so this logs once per
/// refused peer.
fn warn_peer_capacity(peer: SocketAddr, peer_capacity: usize, cluster_size: usize) {
    tracing::warn!(
        peer = %peer,
        peer_capacity,
        cluster_size,
        "Not gossiping with a peer discovery announced: the peer table is \
         full. That peer will not see this node's rate-limit counts, so it \
         under-counts every key this node admits, and this node keeps \
         merging its counts without acknowledging them. The cluster has \
         grown past `storage.peer_capacity` (`gabion_storage_peer_capacity` \
         in nginx); raise it above the largest cluster size you expect and \
         restart.",
    );
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
