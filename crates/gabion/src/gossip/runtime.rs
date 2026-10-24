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
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::crdt::{
    CellHandle, CellStore, Count, DeltaSink, ExpirationSink, NodeId, NodeIdentity, Observation,
    ObservationBatch,
};
use crate::discovery::PeerEvent;
use crate::wire::{self, PacketBuf, Packets, WireScratch};

use super::admin::{AdminCommand, AdminSnapshot, PeerEntry};
use super::client::{GossipClient, LimitRequest, Request};
#[cfg(feature = "transport-udp")]
use super::clock::TokioClock;
use super::clock::{Clock, Ticker};
use super::store::AggregateStore;
use super::transport::GossipTransport;
#[cfg(feature = "transport-udp")]
use super::transport::UdpTransport;
use super::{GossipConfig, GossipError};

mod pending;
use pending::PendingHits;

/// One peer the runtime gossips with. `identity` is `None` until we receive
/// our first inbound packet from this peer, whose header names it.
struct Peer {
    addr: SocketAddr,
    identity: Option<NodeIdentity>,
    /// When a packet from this peer last arrived; `None` until one has.
    last_heard_ms: Option<u64>,
    welcome: Welcome,
}

impl Peer {
    fn new(addr: SocketAddr, welcome: Welcome) -> Self {
        Self {
            addr,
            identity: None,
            last_heard_ms: None,
            welcome,
        }
    }
}

/// Whether a peer is owed this node's own cells (see `serve_welcomes`).
#[derive(Clone, Copy)]
enum Welcome {
    Settled,
    /// The cells from store slot `next_slot` on are still to send.
    Owed {
        next_slot: u32,
    },
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
    sink_buf: DeltaSink<C>,
    expiration_buf: ExpirationSink<C>,
    // A round's frame, shared by every pick, and the pick's copy of it
    // without the cells that peer already holds (bp-skip).
    round_handles: Vec<CellHandle>,
    frame_handles: Vec<CellHandle>,
    // The fewest cells one packet carries; sizes a round to the free send
    // slots so that every pick's frame fits.
    cells_per_packet: usize,

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
    // `pick_count` the most recent dirty tick chose (the coverage fanout
    // `⌈ln(n)+c⌉`, scaled by cluster size n, not the dirty set);
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
        let round_handles = Vec::with_capacity(config.max_cells_per_tick);
        let frame_handles = Vec::with_capacity(config.max_cells_per_tick);
        let cells_per_packet =
            wire::min_cells_per_packet::<C>(config.wire_limits, config.auth_key.is_some());
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
            .map(|addr| Peer::new(addr, Welcome::Settled))
            .collect();
        let rng = SplitMix64::new(config.rng_seed);

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
            round_handles,
            frame_handles,
            cells_per_packet,
            send_pool,
            send_pending,
            send_free,
            peers,
            rng,
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
        // Paces relay rounds: forwarded changes leave in a round of their
        // own, at most one per `GOSSIP_RELAY_INTERVAL_MILLIS`, rather than
        // waiting for the heartbeat.
        let mut relay = self.clock.ticker(Duration::from_millis(
            crate::defaults::GOSSIP_RELAY_INTERVAL_MILLIS,
        ));
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
            let relay_due = !self.store.forwarded_dirty().is_empty();

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
                ArmOutcome::Flush
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
                    _ = relay.tick(), if relay_due => ArmOutcome::Relay,
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
                ArmOutcome::Writable => {
                    self.drain_one_send()?;
                    // The pool is whole again: send the next welcome chunk.
                    if self.send_pending.is_empty() {
                        self.serve_welcomes();
                    }
                }
                ArmOutcome::PeerEvent(Some(evt)) => self.handle_peer_event(evt),
                ArmOutcome::PeerEvent(None) => peer_events_done = true,
                ArmOutcome::Admin(Some(cmd)) => self.handle_admin_command(cmd),
                ArmOutcome::Admin(None) => self.admin_rx = None,
                ArmOutcome::Tick => self.handle_gossip_tick(Round::Heartbeat),
                ArmOutcome::Flush => self.handle_gossip_tick(Round::Flush),
                ArmOutcome::Relay => self.handle_gossip_tick(Round::Relay),
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

        let sender = NodeIdentity::new(
            NodeId(summary.header.sender_node_id),
            summary.header.sender_incarnation,
        );
        self.store
            .merge_remote(sender, &self.obs_buf, &mut self.sink_buf);
        let now = self.clock.now_millis();
        if let Some(peer) = self.peers.iter_mut().find(|p| p.addr == src) {
            peer.identity = Some(sender);
            // A peer silent this long was likely cut off, and missed every
            // change sent meanwhile; welcome it back like a new peer.
            let silence = crate::defaults::GOSSIP_REWELCOME_SILENCE_MILLIS;
            if peer
                .last_heard_ms
                .is_some_and(|t| now.saturating_sub(t) >= silence)
            {
                peer.welcome = Welcome::Owed { next_slot: 0 };
            }
            peer.last_heard_ms = Some(now);
        }
        self.serve_welcomes();
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
                            node_id: p.identity.map(|id| id.node_id),
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
                if !self.peers.iter().any(|x| x.addr == p.addr) {
                    self.peers
                        .push(Peer::new(p.addr, Welcome::Owed { next_slot: 0 }));
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.peers.len(),
                        "Peer joined the cluster.",
                    );
                    self.serve_welcomes();
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

    /// Cells of the repair lane one heartbeat carries: at most
    /// `GOSSIP_REPAIR_CELLS_PER_HEARTBEAT`, and no more than sweeps the store
    /// once per `GOSSIP_REPAIR_MIN_SWEEP_MILLIS`, so a small store is not
    /// resent to every pick on every heartbeat.
    fn repair_slice(&self) -> usize {
        use crate::defaults::{GOSSIP_REPAIR_CELLS_PER_HEARTBEAT, GOSSIP_REPAIR_MIN_SWEEP_MILLIS};
        let tick = (self.config.tick_interval.as_millis() as u64).max(1);
        let heartbeats = GOSSIP_REPAIR_MIN_SWEEP_MILLIS.div_ceil(tick) as usize;
        (self.store.active_len() as usize)
            .div_ceil(heartbeats.max(1))
            .min(GOSSIP_REPAIR_CELLS_PER_HEARTBEAT)
    }

    /// Send this node's own live cells to every peer owed them: one that
    /// discovery has just added, or one heard from again after
    /// `GOSSIP_REWELCOME_SILENCE_MILLIS` of silence, such as across a healed
    /// partition. Each origin welcomes the peer with its own share, so it
    /// holds the whole window once every member has, without waiting for
    /// the repair lane. A welcome goes out in chunks of what the free send
    /// slots carry, resuming where the last one stopped; every round and
    /// every inbound packet sends the next.
    fn serve_welcomes(&mut self) {
        for i in 0..self.peers.len() {
            let Welcome::Owed { next_slot } = self.peers[i].welcome else {
                continue;
            };
            let budget =
                (self.send_free.len() * self.cells_per_packet).min(self.config.max_cells_per_tick);
            if budget == 0 {
                return;
            }
            let resume = self
                .store
                .fill_local_cells(next_slot, budget, &mut self.frame_handles);
            if !self.frame_handles.is_empty() {
                self.encode_packets_for(self.peers[i].addr);
            }
            self.peers[i].welcome = match resume {
                Some(next_slot) => Welcome::Owed { next_slot },
                None => Welcome::Settled,
            };
        }
    }

    fn handle_gossip_tick(&mut self, round: Round) {
        // Step 0: bump the total-ticks counter. Tracked unconditionally
        // (even for empty / no-peer ticks) so the bench can split the
        // run's wall time into heartbeat ticks vs threshold fires.
        self.ticks_total = self.ticks_total.saturating_add(1);

        // Step 1: expire aged-out cells so we don't gossip them. The sweep
        // walks the whole store, so it runs on heartbeats only; flush rounds
        // come as often as every `min_emit_interval`, and a cell that
        // outlives its window by one heartbeat no longer counts toward it.
        let now_millis = self.clock.now_millis();
        if let Round::Heartbeat = round {
            self.expiration_buf.clear();
            self.store.expire_at(now_millis, &mut self.expiration_buf);
        }

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
        // Coverage fanout (Kermarrec, Massoulié & Ganesh, IEEE TPDS 2003,
        // Theorem 1): a directed gossip round with mean fanout `ln(n) + c`
        // reaches *every* node with probability → e^(−e^(−c)) — "there is a
        // sharp threshold in the required fanout at log n." Fanout is
        // governed by cluster size `n`, not data volume: KMG note the
        // fanout need only grow by 1 each time `n` rises by a factor of e,
        // and that a round "can easily be modified … to send several
        // notifications per gossip message." Gabion already packs the whole
        // dirty set into one fat frame (`max_cells_per_tick` + packet
        // splitting), so a burst rides along without widening fanout — the
        // dirty-set size drives frame fill, not the peer pick. `config.fanout`
        // is the floor; `.min(n)` makes small clusters full-mesh (at small n
        // coverage demands it and the bandwidth is trivial). The early return
        // above guarantees n ≥ 1, so `ln` is never called on 0 and `.min(n)`
        // never clamps below 1.
        let stamp = now_millis.max(1);
        let n = self.peers.len();
        let coverage = ((n as f64).ln() + crate::defaults::GOSSIP_COVERAGE_MARGIN).ceil() as usize;
        let pick_count = self.config.fanout.max(coverage).min(n);
        // Record the coverage-fanout decision so observers can read the pick
        // directly instead of reconstructing the formula. Only dirty ticks
        // reach here, so `last_effective_fanout` is the fanout of the most
        // recent tick that actually gossiped.
        self.last_effective_fanout = pick_count;
        self.peak_effective_fanout = self.peak_effective_fanout.max(pick_count);
        for i in 0..pick_count {
            let j = i + (self.rng.next_u64() as usize) % (n - i);
            self.peers.swap(i, j);
        }

        // One frame for the whole round, sized so that every pick's copy
        // fits the free send slots: changes it takes leave their dirty ring,
        // and the rest wait for the next round instead of being cut short
        // for the later picks. Only heartbeat rounds add a repair slice.
        let per_pick_cells = self.send_free.len() / pick_count * self.cells_per_packet;
        let budget = per_pick_cells.min(self.config.max_cells_per_tick);
        if budget == 0 {
            return;
        }
        match round {
            Round::Heartbeat => {
                let repair = self.repair_slice();
                self.store
                    .fill_gossip_frame(budget, repair, &mut self.round_handles);
            }
            Round::Flush => {
                self.store
                    .fill_gossip_frame(budget, 0, &mut self.round_handles);
            }
            Round::Relay => {
                self.store.fill_relay_frame(budget, &mut self.round_handles);
            }
        }

        for peer_idx in 0..pick_count {
            let peer = &self.peers[peer_idx];
            let peer_addr = peer.addr;
            // bp-skip: never send a cell to its origin or back to the peer
            // its value came from. A peer whose identity no cell mentions
            // has no dictionary slot, and nothing to skip.
            let peer_node = peer
                .identity
                .and_then(|id| self.store.find_node(id.node_id, id.incarnation));
            self.frame_handles.clear();
            let store = &self.store;
            self.frame_handles.extend(
                self.round_handles
                    .iter()
                    .filter(|&&h| peer_node.is_none_or(|node| !store.known_to(h, node))),
            );
            if self.frame_handles.is_empty() {
                continue;
            }
            if self.encode_packets_for(peer_addr) {
                self.clear_emitted_pending(stamp);
            }
        }

        self.serve_welcomes();
    }

    /// Zero the pending count of every local cell in the frame just encoded
    /// and stamp its rule's `rule_last_emit_ms`, so the `min_emit_interval`
    /// floor measures from this emit. A key whose cell did not fit in any
    /// frame keeps its count and can fire again on its next hit. Clearing on
    /// send, not on ack, means the N × ε_R bound holds in expectation under
    /// UDP loss; the repair lane resends what was lost. Only called once the
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

    /// Queue the packets of `frame_handles` for `dst`. Returns whether the
    /// whole frame was queued, so the caller clears pending only for cells
    /// that will go out.
    fn encode_packets_for(&mut self, dst: SocketAddr) -> bool {
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
            Err(_) => return false,
        };

        while let Some(slot) = self.send_free.pop() {
            let buf = &mut self.send_pool[slot as usize];
            match packets.next_into(buf) {
                Ok(Some(written)) => {
                    self.send_pending.push_back((dst, slot));
                    if written.flags & wire::FLAG_MORE == 0 {
                        return true;
                    }
                }
                Ok(None) => {
                    self.send_free.push(slot);
                    return true;
                }
                Err(_) => {
                    self.send_free.push(slot);
                    return false;
                }
            }
        }
        false
    }
}

enum ArmOutcome {
    Request(Option<Request>),
    Inbound(io::Result<(usize, SocketAddr)>),
    Writable,
    PeerEvent(Option<PeerEvent>),
    Admin(Option<AdminCommand>),
    Tick,
    Flush,
    Relay,
}

/// Why a gossip round runs, and so what it sends. The heartbeat and a
/// threshold flush send this node's own unsent changes, the schedule that
/// bounds how long they wait; the heartbeat adds a slice of the repair lane.
/// Both also forward what other nodes sent. A relay round, paced faster so
/// forwarded changes do not wait for the heartbeat, sends only those.
#[derive(Clone, Copy)]
enum Round {
    Heartbeat,
    Flush,
    Relay,
}

/// Read one admin command. Only invoked when `admin_rx.is_some()` — the
/// `select!` guard ensures the unwrap is safe.
async fn recv_admin(rx: &mut Option<mpsc::Receiver<AdminCommand>>) -> Option<AdminCommand> {
    rx.as_mut()
        .expect("admin arm gated by admin_rx.is_some()")
        .recv()
        .await
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
