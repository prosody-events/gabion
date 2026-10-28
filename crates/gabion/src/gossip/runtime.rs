//! Single-threaded gossip event loop.
//!
//! One `tokio::select!` drives four arms — limit/shutdown requests, inbound
//! UDP, peer membership churn, gossip tick — plus admin commands when wired.
//! After each iteration the runtime calls [`AggregateStore::apply`] once with
//! whatever rows the arm produced, and once more per full expiry batch. The
//! runtime owns the [`CellStore`], pre-allocated scratch buffers, one packet
//! buffer, and the peer table; the request and packet arms reuse those
//! buffers; admin commands may allocate.

use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::crdt::{
    CellHandle, CellStore, DeltaSink, ExpirationSink, Expiry, NodeId, Observation, ObservationBatch,
};
use crate::defaults;
use crate::discovery::PeerEvent;
use crate::settings::Setting;
use crate::throttle;
use crate::wire::{self, DecodeError, Packets, WireScratch};

use super::admin::{AdminCommand, AdminSnapshot, GossipCounters, PeerEntry};
use super::client::{GossipClient, LimitRequest, Request};
#[cfg(feature = "transport-udp")]
use super::clock::TokioClock;
use super::clock::{Clock, Ticker};
use super::peer::{Insert, PeerTable};
use super::store::AggregateStore;
#[cfg(feature = "transport-udp")]
use super::transport::UdpTransport;
use super::transport::{GossipTransport, Outbox};
use super::{GossipError, ValidGossipConfig};

/// Single-threaded gossip event loop. `!Send + !Sync`.
pub struct GossipRuntime<S, T, K>
where
    S: AggregateStore,
    T: GossipTransport,
    K: Clock,
{
    config: ValidGossipConfig,
    store: CellStore,
    transport: T,
    clock: K,
    requests_rx: mpsc::Receiver<Request>,
    /// Optional admin command channel. `None` in embedded / test setups where
    /// no observability surface is wired up; without a receiver the admin arm
    /// never fires.
    admin_rx: Option<mpsc::Receiver<AdminCommand>>,

    // Downstream count store (write-only).
    aggregates: S,

    // Pre-allocated reusable buffers. `recv_buf` holds any UDP datagram, so
    // an oversized packet arrives whole and is reported as one.
    recv_buf: Box<[u8]>,
    scratch: WireScratch,
    obs_buf: ObservationBatch,
    sink_buf: DeltaSink,
    expiration_buf: ExpirationSink,
    frame_handles: Vec<CellHandle>,
    outbox: Outbox,

    peers: PeerTable,
    rng: SplitMix64,

    // Pending reply for the in-flight limit request. Sent after the
    // bottom-of-loop apply so callers observe "ack ⇒ AggregateStore reflects
    // the increment". Sized to one because only one limit request per
    // iteration is processed.
    pending_reply: Option<oneshot::Sender<()>>,

    counters: GossipCounters,

    // Threshold-triggered anti-entropy state. One column per interned rule slot; sized to the
    // rule dictionary's capacity at construction and reused for the runtime's lifetime, 12 bytes
    // per rule-dictionary slot. Indexed by `RuleSlot` to keep the hot path's single dictionary
    // lookup (`RuleDictionary::find`) the only identity work.
    //
    // - `rule_pending[slot]`     — local hits accumulated against rule `slot` since the last
    //   gossip emit. Zeroed when a round settles (`settle`).
    // - `rule_last_emit_ms[slot]` — wall-clock stamp of the last emit that included this rule
    //   (stamped when a round settles).
    rule_pending: Box<[u32]>,
    rule_last_emit_ms: Box<[u64]>,

    // Set by `handle_limit_request` when the rule's per-site error budget
    // would be exceeded by the new hit and the `min_emit_interval` floor
    // has elapsed. Read at the top of the run loop, which then skips the
    // `tokio::select!` wait and dispatches a synthetic `Tick` outcome.
    want_immediate_flush: bool,

    // Observability counters surfaced through `AdminSnapshot`. None of
    // them feed back into the hot path — they let observers (the wasm
    // visualizer) split heartbeat-driven emits from threshold-driven ones
    // and compute the *effective* fanout (packets emitted ÷ dirty ticks).
    // Single-threaded `!Send` runtime, so plain `u64`s are cheaper and
    // clearer than atomics.
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
impl<S> GossipRuntime<S, UdpTransport, TokioClock>
where
    S: AggregateStore,
{
    /// Bind a UDP socket on `bind_addr` and assemble the runtime with the
    /// production transport and a wall-clock-anchored `TokioClock`.
    pub async fn bind(
        bind_addr: SocketAddr,
        config: ValidGossipConfig,
        store: CellStore,
        aggregates: S,
    ) -> Result<(Self, GossipClient), GossipError> {
        let transport = UdpTransport::bind(bind_addr, &config).await?;
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
        config: ValidGossipConfig,
        store: CellStore,
        aggregates: S,
        admin_rx: mpsc::Receiver<AdminCommand>,
    ) -> Result<(Self, GossipClient), GossipError> {
        let transport = UdpTransport::bind(bind_addr, &config).await?;
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

impl<S, T, K> GossipRuntime<S, T, K>
where
    S: AggregateStore,
    T: GossipTransport,
    K: Clock,
{
    /// Generic entry point used by tests, simulators, and any non-UDP setup.
    /// Equivalent to [`Self::from_parts_with_admin`] with `admin_rx = None`.
    pub fn from_parts(
        transport: T,
        clock: K,
        config: ValidGossipConfig,
        store: CellStore,
        aggregates: S,
    ) -> (Self, GossipClient) {
        Self::from_parts_with_admin(transport, clock, config, store, aggregates, None)
    }

    /// Generic entry point with an optional admin command channel; without a
    /// receiver the admin arm never fires.
    pub fn from_parts_with_admin(
        transport: T,
        clock: K,
        mut config: ValidGossipConfig,
        store: CellStore,
        aggregates: S,
        admin_rx: Option<mpsc::Receiver<AdminCommand>>,
    ) -> (Self, GossipClient) {
        let (req_tx, req_rx) = mpsc::channel(config.limit_queue_capacity.get());

        let recv_buf = vec![0u8; wire::MAX_PAYLOAD_BYTES].into_boxed_slice();
        let scratch = WireScratch::for_store(&store);
        // Sized to the most cells one inbound packet carries; a limit request
        // carries one.
        let packet_cells = config.wire_limits.cell_bound();
        let obs_buf = ObservationBatch::with_capacity(packet_cells);
        let sink_buf = DeltaSink::with_capacity(packet_cells);
        let frame_handles = Vec::with_capacity(config.max_cells_per_tick.get());
        let expiration_buf = ExpirationSink::with_capacity(defaults::GOSSIP_EXPIRATION_BATCH);
        let outbox = Outbox::new(&config);

        let bootstrap = std::mem::take(&mut config.bootstrap_peers);
        let peers = PeerTable::with_capacity(config.peer_capacity);
        let rng = SplitMix64::new(config.rng_seed);

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
            sink_buf,
            expiration_buf,
            frame_handles,
            outbox,
            peers,
            rng,
            pending_reply: None,
            counters: GossipCounters::default(),
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
        for addr in bootstrap {
            runtime.add_peer(addr);
        }
        let client = GossipClient::new(req_tx);
        (runtime, client)
    }

    /// Local transport address (after binding). Useful for tests that need
    /// the port the kernel chose.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.transport.local_addr()
    }

    /// Run the event loop until shutdown is requested. `peer_events` feeds
    /// the peer-event arm; pass `futures::stream::empty()` if discovery is
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
            let watch_peers = !peer_events_done;

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
                // bumping here gives observers an accurate split
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
                    evt = peer_events.next(), if watch_peers => ArmOutcome::PeerEvent(evt),
                    cmd = recv_admin(admin_rx) => ArmOutcome::Admin(cmd),
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
                ArmOutcome::PeerEvent(Some(evt)) => self.handle_peer_event(evt),
                ArmOutcome::PeerEvent(None) => peer_events_done = true,
                ArmOutcome::Admin(Some(cmd)) => self.handle_admin_command(cmd),
                ArmOutcome::Admin(None) => self.admin_rx = None,
                ArmOutcome::Tick => self.handle_gossip_tick(),
            }

            // One apply per iteration covering both row sources. CRDT-free
            // arms (peer event, idle ticks) leave both empty; the branch
            // short-circuits then.
            if !self.sink_buf.is_empty() || !self.expiration_buf.is_empty() {
                self.aggregates.apply(&self.sink_buf, &self.expiration_buf);
                self.sink_buf.clear();
                self.expiration_buf.clear();
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
        // crossing and reset; `settle` zeroes the slot at the start of the
        // next round.
        //
        // Falls through silently when the rule isn't interned yet (first
        // hit for that fingerprint) — the next heartbeat picks it up.
        let hits = u32::try_from(req.hits).unwrap_or(u32::MAX);
        if let Some(slot) = self.store.rule_dictionary().find(req.rule_fingerprint) {
            let idx = slot as usize;
            let pending = self.rule_pending[idx].saturating_add(hits);
            self.rule_pending[idx] = pending;
            let peers = self.peers.len().max(1) as u64;
            let bps = u64::from(self.config.target_err_bps.get());
            let epsilon = (req.rule_limit.saturating_mul(bps) / (10_000 * peers)).max(1);
            // Surface the budget so observers can see the threshold a burst's
            // accumulated `pending` must cross to trigger an eager flush.
            self.last_error_budget = epsilon;
            if (pending as u64) > epsilon {
                // The runtime's clock is the canonical floor reference, not
                // `req.now_millis`. Adapter-supplied wall clock and
                // `self.clock.now_millis()` may live on different epochs
                // (wall vs paused-virtual time in tests), and only the
                // runtime-local clock is monotone with the stamp `settle`
                // writes. Subtraction is still well-defined in monotonic-rate
                // terms — both fire and reset use the same clock — so the
                // floor is correct. `last == 0` is the "never emitted"
                // sentinel so the very first crossing always fires; `settle`
                // stamps `max(1)` so the sentinel is not re-introduced.
                let now = self.clock.now_millis();
                let last = self.rule_last_emit_ms[idx];
                let floor = self.config.min_emit_interval.0.get();
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
            count: hits,
            last_update_millis: req.now_millis,
        });
        self.store.ingest_local(&self.obs_buf, &mut self.sink_buf);
        // Reply is sent at the bottom of this loop iteration, AFTER the
        // post-iteration apply runs. Caller observes "ack ⇒ AggregateStore
        // reflects the increment".
        self.pending_reply = req.reply;
    }

    fn handle_inbound(&mut self, n: usize, src: SocketAddr) {
        self.counters.packets_received += 1;
        self.counters.bytes_received += n as u64;
        self.obs_buf.clear();
        self.sink_buf.clear();
        let obs_buf = &mut self.obs_buf;
        let decoded = wire::decode(
            &self.recv_buf[..n],
            self.config.wire_limits,
            self.config.cluster_id_hash,
            |cell| obs_buf.push(cell),
        );
        let header = match decoded {
            Ok(header) => header,
            Err(error) => return self.note_decode_reject(error, src, n),
        };
        // Every data kind merges the same way; header-only kinds carry no
        // cells.
        self.store.merge_remote(&self.obs_buf, &mut self.sink_buf);
        self.peers.set_identity(src, header.sender);
    }

    /// Count a dropped packet against its cause, and log at powers of two of
    /// that cause's count, so a flood of one cause cannot hide another.
    fn note_decode_reject(&mut self, error: DecodeError, peer: SocketAddr, length: usize) {
        let max_payload_bytes = self.config.wire_limits.max_payload_bytes();
        match error {
            DecodeError::BadVersion { found } => {
                if let Some(rejected_total) =
                    throttle::bump(&mut self.counters.decode_rejects_version)
                {
                    tracing::warn!(
                        code = "peer_version_mismatch",
                        peer = %peer,
                        peer_version = found,
                        local_version = wire::VERSION,
                        reason = "version",
                        rejected_total,
                        "Ignoring gossip from this peer: it speaks another gossip protocol version \
                         (`peer_version`) than this node (`local_version`), so the two do not \
                         share counts. During a rolling upgrade between those versions this is \
                         expected and stops once every pod runs the same release; until then each \
                         version enforces limits on its own share of the traffic, so the cluster \
                         can admit up to about twice a key's limit per window, and requests are \
                         allowed. Finish the rollout quickly, or roll back. If it continues after \
                         the rollout ends, a pod is still on the other release: find it by the \
                         `peer` field and upgrade or restart it.",
                    );
                }
            }
            DecodeError::ForeignCluster { found } => {
                if let Some(rejected_total) =
                    throttle::bump(&mut self.counters.decode_rejects_cluster)
                {
                    tracing::warn!(
                        code = "decode_rejected",
                        peer = %peer,
                        error = %error,
                        reason = "cluster",
                        rejected_total,
                        peer_cluster = %format_args!("{found:#x}"),
                        local_cluster = %format_args!("{:#x}", self.config.cluster_id_hash),
                        config_key = Setting::GossipClusterIdHash.yaml_key(),
                        "Ignored gossip from this peer: it belongs to another gabion cluster \
                         (`peer_cluster`) than this node (`local_cluster`), so this node does not \
                         merge its counts. Two gabion clusters share a network and gossip port. \
                         If they are one cluster, set the same {} on every node. If they are \
                         separate, also give them different gossip ports or networks.",
                        Setting::GossipClusterIdHash,
                    );
                }
            }
            DecodeError::PayloadTooLarge => {
                if let Some(rejected_total) =
                    throttle::bump(&mut self.counters.decode_rejects_peer_limits)
                {
                    tracing::warn!(
                        code = "decode_rejected",
                        peer = %peer,
                        error = %error,
                        reason = "peer_limits",
                        rejected_total,
                        length,
                        max_payload_bytes,
                        config_key = Setting::GossipMaxPayloadBytes.yaml_key(),
                        "Dropped a packet from this peer larger than this node accepts. The peer \
                         has a higher {} than this node, or something on the path between them \
                         changes packet sizes. Set it to the same value on every node. Until \
                         then, limits here under-count the traffic that peer handled.",
                        Setting::GossipMaxPayloadBytes,
                    );
                }
            }
            DecodeError::TooShort
            | DecodeError::BadMagic
            | DecodeError::BadKind
            | DecodeError::BadFlags
            | DecodeError::BadRange
            | DecodeError::BodyLength
            | DecodeError::BadSlot => {
                if let Some(rejected_total) =
                    throttle::bump(&mut self.counters.decode_rejects_framing)
                {
                    tracing::warn!(
                        code = "decode_rejected",
                        peer = %peer,
                        error = %error,
                        reason = "framing",
                        rejected_total,
                        max_payload_bytes,
                        config_key = Setting::GossipMaxPayloadBytes.yaml_key(),
                        "Dropped a packet from this peer that is not valid gabion gossip. Usually \
                         another program is sending UDP to this node's gossip port, or something \
                         on the path truncates packets. If the peer is a gabion node, check the \
                         path MTU between the two: gossip packets are at most {max_payload_bytes} \
                         bytes, set by {}. Counts from healthy peers are not affected.",
                        Setting::GossipMaxPayloadBytes,
                    );
                }
            }
        }
    }

    fn handle_admin_command(&mut self, cmd: AdminCommand) {
        match cmd {
            AdminCommand::Snapshot { reply } => {
                let snapshot = AdminSnapshot {
                    local_identity: self.store.local_identity(),
                    peers: self
                        .peers
                        .rows()
                        .map(|(addr, identity)| PeerEntry {
                            addr,
                            node_id: identity.map(|identity| identity.node_id),
                        })
                        .collect(),
                    store_stats: self.store.stats(),
                    local_dirty_len: self.store.local_dirty().len(),
                    forwarded_dirty_len: self.store.forwarded_dirty().len(),
                    counters: self.counters,
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
            PeerEvent::Added(p) => self.add_peer(p.addr),
            PeerEvent::Removed(p) => {
                if self.peers.remove(p.addr) {
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.peers.len(),
                        "Peer left the cluster.",
                    );
                }
            }
        }
    }

    /// Add a peer to the table: a known one is ignored, and past
    /// `gossip.peer_capacity` it is declined, its packets still merged.
    fn add_peer(&mut self, addr: SocketAddr) {
        match self.peers.insert(addr) {
            Insert::Added => tracing::info!(
                peer = %addr,
                cluster_size = self.peers.len(),
                "Peer joined the cluster.",
            ),
            Insert::Present => {}
            Insert::Full => {
                if let Some(declined_total) = throttle::bump(&mut self.counters.peers_declined) {
                    let capacity = self.config.peer_capacity;
                    tracing::warn!(
                        code = "peer_table_full",
                        peer = %addr,
                        capacity,
                        declined_total,
                        config_key = Setting::GossipPeerCapacity.yaml_key(),
                        "This node's peer table is full ({capacity} peers), so it ignored a new \
                         peer, {addr}, reported by discovery. This node does not send its counts \
                         to {addr}, so limits enforced at {addr} under-count this node's traffic \
                         and allow requests over the limit; counts {addr} sends here are still \
                         merged. The cluster has grown past the table. Raise {} above the \
                         cluster's pod count; each slot costs about 100 bytes.",
                        Setting::GossipPeerCapacity,
                    );
                }
            }
        }
    }

    /// A heartbeat or threshold round. The work that needs no socket comes
    /// first, so a round with no peers or a held packet still does it.
    fn handle_gossip_tick(&mut self) {
        // Tracked unconditionally (even for empty / no-peer ticks) so
        // observers can split the run's wall time into heartbeat ticks vs
        // threshold fires.
        self.ticks_total = self.ticks_total.saturating_add(1);
        self.expire_cells();
        self.settle();
        self.outbox.retry(&self.transport, &mut self.counters);
        self.gossip();
    }

    /// Free aged-out cells so we don't gossip them. Each full batch is applied
    /// before the next; the last one goes with the iteration's other rows.
    fn expire_cells(&mut self) {
        let now_millis = self.clock.now_millis();
        while self.store.expire_at(now_millis, &mut self.expiration_buf) == Expiry::More {
            debug_assert!(self.sink_buf.is_empty());
            self.aggregates.apply(&self.sink_buf, &self.expiration_buf);
            self.expiration_buf.clear();
        }
    }

    /// Settle the error budget: reset each rule's unsent local hits and stamp
    /// its emit floor. Every round does it before it can return early, so a
    /// lone node or a blocked socket does not run a threshold round per
    /// request. The hits ride this round's frames or wait in the dirty rings.
    fn settle(&mut self) {
        let stamp = self.clock.now_millis().max(1);
        for slot in 0..self.rule_pending.len() {
            if self.rule_pending[slot] != 0 {
                self.rule_pending[slot] = 0;
                self.rule_last_emit_ms[slot] = stamp;
            }
        }
    }

    /// Send this round's frames, stopping at the first packet the kernel
    /// refuses. A round that falls due while a packet is held sends nothing.
    fn gossip(&mut self) {
        if self.outbox.is_held() || self.store.is_empty() || self.peers.len() == 0 {
            return;
        }

        // The store was non-empty when this tick fired: count it as a
        // "dirty tick" so effective fanout is `packets / dirty_ticks`
        // rather than `packets / wall_ticks`. Bumped before the peer-pick
        // math runs so synthetic threshold ticks (which by construction
        // have local_dirty>0) always count.
        self.dirty_ticks = self.dirty_ticks.saturating_add(1);

        // Pick `fanout` distinct peers by shuffling them to the table's front
        // rows. With-replacement sampling would silently break Demers'
        // O(log N) convergence bound — selecting the same peer twice in one
        // tick encodes and sends the same frame twice.
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
        let n = self.peers.len();
        let coverage = ((n as f64).ln() + defaults::GOSSIP_COVERAGE_MARGIN).ceil() as usize;
        let pick_count = self.config.fanout.get().max(coverage).min(n);
        // Record the coverage-fanout decision so observers can read the pick
        // directly instead of reconstructing the formula. Only dirty ticks
        // reach here, so `last_effective_fanout` is the fanout of the most
        // recent tick that actually gossiped.
        self.last_effective_fanout = pick_count;
        self.peak_effective_fanout = self.peak_effective_fanout.max(pick_count);
        self.peers.shuffle_front(pick_count, || self.rng.next_u64());

        for peer_idx in 0..pick_count {
            let peer_addr = self.peers.addr(peer_idx);
            self.store.fill_gossip_frame(
                self.config.max_cells_per_tick.get(),
                &mut self.frame_handles,
            );
            if self.frame_handles.is_empty() {
                continue;
            }
            self.send_frame(peer_addr);
            if self.outbox.is_held() {
                return;
            }
        }
    }

    /// Encode the frame in `frame_handles` for `dst` and send it.
    fn send_frame(&mut self, dst: SocketAddr) {
        // Every packet is a sweep: cells outside any stream.
        let header = wire::Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender: self.store.local_identity(),
            sender_zone: 0,
            acks: wire::AckInfo::default(),
            payload: wire::CellPayload::Sweep,
        };

        let Self {
            config,
            store,
            frame_handles,
            scratch,
            transport,
            outbox,
            counters,
            ..
        } = self;
        let packets = Packets::new(header, store, frame_handles, scratch, config.wire_limits);
        outbox.send_frame(packets, dst, transport, counters);
    }
}

enum ArmOutcome {
    Request(Option<Request>),
    Inbound(io::Result<(usize, SocketAddr)>),
    PeerEvent(Option<PeerEvent>),
    Admin(Option<AdminCommand>),
    Tick,
}

/// Read one admin command; never resolves once the channel is gone.
async fn recv_admin(rx: &mut Option<mpsc::Receiver<AdminCommand>>) -> Option<AdminCommand> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
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
