//! Single-threaded gossip event loop.
//!
//! One `tokio::select!` drives six arms — limit/shutdown requests, inbound
//! UDP, outbound writable, peer membership churn, the heartbeat tick, and a
//! wake-up for relay rounds and protocol timers. After each iteration the
//! runtime calls [`AggregateStore::apply`] once with whatever rows the arm
//! produced. The runtime owns the [`CellStore`], pre-allocated scratch
//! buffers, the outbound send pool, and the peer table; nothing in the loop
//! allocates after construction. Dissemination is Plumtree; see [`plumtree`].

mod plumtree;

use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::crdt::{
    CellHandle, CellStore, Count, DeltaSink, ExpirationSink, Incarnation, NodeId, Observation,
    ObservationBatch,
};
use crate::discovery::PeerEvent;
use crate::wire::{PacketBuf, WireScratch};

use super::admin::{AdminCommand, AdminSnapshot, PeerEntry};
use super::client::{GossipClient, LimitRequest, Request};
#[cfg(feature = "transport-udp")]
use super::clock::TokioClock;
use super::clock::{Clock, Ticker};
use super::store::AggregateStore;
use super::transport::GossipTransport;
#[cfg(feature = "transport-udp")]
use super::transport::UdpTransport;
use super::{GossipConfig, GossipError, PlumtreeConfig};
use plumtree::{Edge, NO_POS, Plumtree, Round};

/// One peer the runtime gossips with. `node_id` is `None` until we receive
/// our first inbound packet from this peer; `peer_slot` caches the
/// `PeerFrontierTable` slot once interned.
struct Peer {
    addr: SocketAddr,
    node_id: Option<NodeId>,
    peer_slot: Option<u16>,
    /// Position in the active view, or [`NO_POS`] when outside it.
    view_pos: u8,
    edge: Edge,
}

impl Peer {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            node_id: None,
            peer_slot: None,
            view_pos: NO_POS,
            edge: Edge::default(),
        }
    }
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

    // Plumtree overlay state, and the timer armed for its next wake-up.
    pt: Plumtree,
    wake: Option<(u64, K::Ticker)>,

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
    // react rather than infer it. `last_effective_fanout` is the
    // `pick_count` the most recent dirty tick chose (the coverage fanout
    // `⌈ln(n)+c⌉`, scaled by cluster size n, not the dirty set);
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
            .map(Peer::new)
            .collect();
        let rng = SplitMix64::new(config.rng_seed);
        let pt = Plumtree::new(PlumtreeConfig::default(), &store, config.max_cells_per_tick);

        let rule_capacity = store.rule_dictionary().capacity() as usize;
        let rule_pending = vec![0_u32; rule_capacity].into_boxed_slice();
        let rule_last_emit_ms = vec![0_u64; rule_capacity].into_boxed_slice();

        let mut runtime = Self {
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
            pt,
            wake: None,
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
        for index in 0..runtime.peers.len() {
            runtime.view_consider(index, index + 1);
        }
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
            self.arm_wake().await;
            let watch_wake = self.wake.is_some();

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
                ArmOutcome::Tick(Round::Threshold)
            } else {
                // Split-borrow scope: we hand the I/O futures non-overlapping
                // pieces of `self` so they compose inside `select!`.
                let Self {
                    transport,
                    recv_buf,
                    requests_rx,
                    admin_rx,
                    wake,
                    ..
                } = &mut self;

                tokio::select! {
                    biased;

                    req = requests_rx.recv() => ArmOutcome::Request(req),
                    recv = transport.recv_from(recv_buf) => ArmOutcome::Inbound(recv),
                    _ = transport.writable(), if have_send => ArmOutcome::Writable,
                    evt = peer_events.next(), if watch_peers => ArmOutcome::PeerEvent(evt),
                    cmd = recv_admin(admin_rx), if watch_admin => ArmOutcome::Admin(cmd),
                    () = wake_up(wake), if watch_wake => ArmOutcome::Wake,
                    _ = tick.tick() => ArmOutcome::Tick(Round::Heartbeat),
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
                ArmOutcome::Wake => {
                    self.wake = None;
                    self.handle_wake();
                }
                ArmOutcome::Tick(round) => self.handle_gossip_tick(round),
            }

            // One apply per iteration covering both row sources. CRDT-free
            // arms (peer event, drain, idle ticks) leave both empty; the
            // branch short-circuits then.
            if !self.sink_buf.is_empty() || !self.expiration_buf.is_empty() {
                self.aggregates.apply(&self.sink_buf, &self.expiration_buf);
                self.sink_buf.clear();
                self.expiration_buf.clear();
                // Changes are queued for their rounds as they happen; the
                // store's own dirty rings are not read.
                self.store.clear_dirty();
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
            origin_sequence: 0,
        });
        self.store.ingest_local(&self.obs_buf, &mut self.sink_buf);
        self.queue_local_changes();
        // Reply is sent at the bottom of this loop iteration, AFTER the
        // post-iteration apply runs. Caller observes "ack ⇒ AggregateStore
        // reflects the increment".
        self.pending_reply = req.reply;
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
                    local_dirty_len: self.pt.queued_local() as u32,
                    forwarded_dirty_len: self.pt.queued_relay() as u32,
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
                    self.peers.push(Peer::new(p.addr));
                    self.view_consider(self.peers.len() - 1, self.peers.len());
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.peers.len(),
                        "Peer joined the cluster.",
                    );
                }
            }
            PeerEvent::Removed(p) => {
                if let Some(pos) = self.peers.iter().position(|x| x.addr == p.addr) {
                    if self.peers[pos].view_pos != NO_POS {
                        self.view_evict(self.peers[pos].view_pos as usize);
                    }
                    let removed = self.peers.remove(pos);
                    self.view_refill();
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
}

enum ArmOutcome {
    Request(Option<Request>),
    Inbound(io::Result<(usize, SocketAddr)>),
    Writable,
    PeerEvent(Option<PeerEvent>),
    Admin(Option<AdminCommand>),
    Wake,
    Tick(Round),
}

/// Resolve when the armed wake-up timer fires. Only polled while one is armed.
async fn wake_up<T: Ticker>(wake: &mut Option<(u64, T)>) {
    match wake {
        Some((_, ticker)) => ticker.tick().await,
        None => std::future::pending().await,
    }
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
