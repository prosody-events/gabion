//! Single-threaded gossip event loop.
//!
//! One `tokio::select!` drives six arms — limit/shutdown requests, inbound
//! UDP, outbound writable, peer membership churn, gossip tick, and (only while
//! a broadcast awaits acknowledgements) the acknowledgement timer. After each
//! iteration the runtime calls [`AggregateStore::apply`] once with whatever
//! rows the arm produced. The runtime owns the [`CellStore`], pre-allocated
//! scratch buffers, the outbound send pool, and the membership ring; nothing
//! in the loop allocates after construction.
//!
//! Local changes reach the cluster over a Snow broadcast tree; see
//! `runtime/snow.rs`.

use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;

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
use super::snow::{Child, Ctrl, MAX_CHILDREN, Origins, Relays, Ring, RttEstimator, TreeParams};
use super::store::AggregateStore;
use super::transport::GossipTransport;
#[cfg(feature = "transport-udp")]
use super::transport::UdpTransport;
use super::{GossipConfig, GossipError};

/// The outbound send pool: pre-allocated packet buffers, the queue of
/// filled buffers waiting for the socket, and the free list.
struct SendQueue {
    pool: Vec<PacketBuf>,
    pending: VecDeque<(SocketAddr, u8)>,
    free: Vec<u8>,
}

impl SendQueue {
    fn new(size: usize, limits: wire::FrameLimits) -> Self {
        let size = size.max(1);
        Self {
            pool: (0..size).map(|_| PacketBuf::for_limits(limits)).collect(),
            pending: VecDeque::with_capacity(size),
            free: (0..size as u8).rev().collect(),
        }
    }

    /// Fill a free buffer with `fill` and queue it for `dst`. `fill` returns
    /// whether it produced a packet. Returns `false` when no buffer was free
    /// or `fill` declined; the packet is then not sent.
    fn enqueue(&mut self, dst: SocketAddr, fill: impl FnOnce(&mut PacketBuf) -> bool) -> bool {
        let Some(slot) = self.free.pop() else {
            return false;
        };
        if fill(&mut self.pool[slot as usize]) {
            self.pending.push_back((dst, slot));
            true
        } else {
            self.free.push(slot);
            false
        }
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
    sink_buf: DeltaSink<C>,
    expiration_buf: ExpirationSink<C>,
    frame_handles: Vec<CellHandle>,

    send: SendQueue,

    // Membership view: every node discovery has reported, plus this one,
    // sorted. The Snow child function reads it; nothing else is peer state.
    ring: Ring,
    rng: SplitMix64,

    // Snow broadcast state. See `runtime/snow.rs`.
    tree: TreeParams,
    origins: Origins,
    relays: Relays,
    rtt: RttEstimator,
    children_buf: Vec<Child>,
    /// Highest local origin sequence already handed to a broadcast.
    sent_seq: u64,
    last_repair_ms: u64,
    unacked_total: u64,

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
    // The adaptive-decision outputs, so observers can *see* the runtime
    // react rather than infer it. `last_effective_fanout` is the number of
    // children the most recent broadcast this node rooted went to;
    // `peak_effective_fanout` is its high-water mark since startup.
    // `last_error_budget` is the per-rule
    // error budget ε the most recent `handle_limit_request` computed — the
    // threshold a burst's accumulated `rule_pending` must cross to trigger
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

        let config_fanout = config.fanout;
        let send = SendQueue::new(config.send_queue_capacity, config.wire_limits);

        let local_addr = transport.local_addr().unwrap_or_else(|err| {
            tracing::warn!(
                error = %err,
                "Could not read this node's gossip address, so it cannot place \
                 itself among its peers and will send broadcasts as if it were \
                 at 0.0.0.0:0. Usually the gossip socket is not bound yet. \
                 Check the gossip bind address in your gabion config.",
            );
            SocketAddr::from(([0, 0, 0, 0], 0))
        });
        let mut ring = Ring::new(local_addr, config.bootstrap_peers.len());
        for addr in config.bootstrap_peers.iter().copied() {
            ring.insert(addr);
        }
        let mut rng = SplitMix64::new(config.rng_seed);

        let origins = Origins::new(
            crate::defaults::GOSSIP_TREE_ORIGIN_CAPACITY,
            config.wire_limits,
            rng.next_u32(),
        );
        let relays = Relays::new(crate::defaults::GOSSIP_TREE_RELAY_CAPACITY);
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
            sink_buf,
            expiration_buf,
            frame_handles,
            send,
            ring,
            rng,
            tree: TreeParams::new(config_fanout),
            origins,
            relays,
            rtt: RttEstimator::default(),
            children_buf: Vec::with_capacity(MAX_CHILDREN),
            sent_seq: 0,
            last_repair_ms: 0,
            unacked_total: 0,
            pending_reply: None,
            decode_reject_count: 0,
            max_send_pending_depth: 0,
            rule_pending,
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

    /// Replace the Snow tuning knobs, which production code takes from
    /// `gabion::defaults`.
    #[cfg(test)]
    pub(super) fn with_tree_params(mut self, edit: impl FnOnce(&mut TreeParams)) -> Self {
        edit(&mut self.tree);
        self
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
        let mut ack_tick = self.clock.ticker(Duration::from_millis(
            crate::defaults::GOSSIP_TREE_TIMER_TICK_MS,
        ));
        // Once the peer-event stream returns `None` (e.g. `stream::empty()`),
        // never poll it again — otherwise it returns `Ready(None)` on every
        // iteration and the loop spins.
        let mut peer_events_done = false;

        loop {
            // Decide outside the `select!` whether the send arm has work to do.
            // Reading `self.send.pending.is_empty()` inside the `if` guard
            // would re-borrow `self` after we've already
            // split-borrowed for the I/O futures below.
            let have_send = !self.send.pending.is_empty();
            let watch_acks = self.origins.live() > 0;
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
                    _ = ack_tick.tick(), if watch_acks => ArmOutcome::AckTimer,
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
                ArmOutcome::AckTimer => self.handle_ack_timer(),
            }

            // One apply per iteration covering both row sources. CRDT-free
            // arms (peer event, drain, idle ticks) leave both empty; the
            // branch short-circuits then.
            if !self.sink_buf.is_empty() || !self.expiration_buf.is_empty() {
                self.aggregates.apply(&self.sink_buf, &self.expiration_buf);
                self.sink_buf.clear();
                self.expiration_buf.clear();
            }

            if self.send.pending.len() > self.max_send_pending_depth {
                self.max_send_pending_depth = self.send.pending.len();
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
            let peers = (self.ring.len() - 1).max(1) as u64;
            let limit = req.rule_limit.max(1);
            let bps = self.config.target_err_bps.max(1) as u64;
            let epsilon = ((limit.saturating_mul(bps)) / (10_000 * peers)).max(1);
            // Surface the budget so observers can see the threshold a burst's
            // accumulated `pending` must cross to trigger an eager flush.
            self.last_error_budget = epsilon;
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
                self.note_decode_reject(src, &err);
                return;
            }
        };

        self.store.merge_remote(&self.obs_buf, &mut self.sink_buf);

        if !summary.ext.is_empty() {
            match Ctrl::decode(summary.ext) {
                Some(ctrl) => self.handle_tree_packet(src, ctrl, n),
                None => self.note_decode_reject(src, &wire::DecodeError::BodyLenMismatch),
            }
        }
    }

    /// Count a frame this node could not decode and, on power-of-two totals,
    /// tell the operator why that usually happens.
    fn note_decode_reject(&mut self, src: SocketAddr, err: &wire::DecodeError) {
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
    }

    fn drain_one_send(&mut self) -> Result<(), GossipError> {
        let Some((dst, slot)) = self.send.pending.pop_front() else {
            return Ok(());
        };
        let buf = &self.send.pool[slot as usize];
        match self.transport.try_send_to(buf.as_bytes(), dst) {
            Ok(_) => self.send.free.push(slot),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.send.pending.push_front((dst, slot));
            }
            Err(e) => {
                self.send.free.push(slot);
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
                        .ring
                        .peers()
                        .map(|addr| PeerEntry {
                            addr,
                            node_id: None,
                        })
                        .collect(),
                    store_stats: self.store.stats(),
                    local_dirty_len: self.store.local_dirty().len(),
                    forwarded_dirty_len: self.store.forwarded_dirty().len(),
                    send_pending_depth: self.send.pending.len(),
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
                if self.ring.insert(p.addr) {
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.ring.len() - 1,
                        "Peer joined the cluster.",
                    );
                }
            }
            PeerEvent::Removed(p) => {
                if self.ring.remove(p.addr) {
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.ring.len() - 1,
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

        if self.store.is_empty() || self.ring.len() == 1 || self.send.free.is_empty() {
            return;
        }

        // Step 2: broadcast the local changes made since the last round down
        // this node's tree, and run the periodic repair pass.
        self.start_broadcasts(now_millis);
        self.repair_pass(now_millis);

        // Reset per-rule pending. The sweep treats every rule with
        // non-zero pending as "touched in expectation by the tick": every
        // pending hit either rode out in this round's broadcasts or was
        // skipped for want of a free send buffer, in which case it stays
        // above `sent_seq` for the next round. A broadcast the tree could not
        // complete is repaired by the repair pass; the N × ε_R bound holds in
        // expectation.
        let stamp = now_millis.max(1);
        for slot in 0..self.rule_pending.len() {
            if self.rule_pending[slot] != 0 {
                self.rule_pending[slot] = 0;
                self.rule_last_emit_ms[slot] = stamp;
            }
        }
    }

    fn encode_packets_for(&mut self, dst: SocketAddr) {
        let header = self.sender_header();

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

        while let Some(slot) = self.send.free.pop() {
            let buf = &mut self.send.pool[slot as usize];
            match packets.next_into(buf) {
                Ok(Some(_)) => self.send.pending.push_back((dst, slot)),
                Ok(None) => {
                    self.send.free.push(slot);
                    break;
                }
                Err(_) => {
                    self.send.free.push(slot);
                    break;
                }
            }
        }
        packets.finish();
    }
}

mod tree;

enum ArmOutcome {
    Request(Option<Request>),
    Inbound(io::Result<(usize, SocketAddr)>),
    Writable,
    PeerEvent(Option<PeerEvent>),
    Admin(Option<AdminCommand>),
    Tick,
    AckTimer,
}

/// Read one admin command. Only invoked when `admin_rx.is_some()` — the
/// `select!` guard ensures the unwrap is safe.
async fn recv_admin(rx: &mut Option<mpsc::Receiver<AdminCommand>>) -> Option<AdminCommand> {
    rx.as_mut()
        .expect("admin arm gated by admin_rx.is_some()")
        .recv()
        .await
}

/// SplitMix64 — small, fast, deterministic. Used for repair-peer sampling and
/// the broadcast sequence seed, so no security properties are required.
pub(super) struct SplitMix64(u64);

impl SplitMix64 {
    pub(super) fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E37_79B9_7F4A_7C15))
    }

    pub(super) fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    pub(super) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}
