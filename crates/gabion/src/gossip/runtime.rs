//! Single-threaded gossip event loop.
//!
//! One `tokio::select!` drives five arms — limit/shutdown requests, the
//! heartbeat, the prompt-round pacer (polled only while there is prompt
//! work), inbound UDP and peer membership churn — plus admin commands when
//! wired. Every round reads at most `B_r` waiting datagrams, then serves each
//! peer in one send loop: its own-stream cells above what it was sent, a
//! resend with `PROBE` when a heartbeat passed without an ack, and owed acks,
//! gap reports and probe answers. After each iteration the runtime calls
//! [`AggregateStore::apply`] once with the rows the arm produced, and once
//! more per full expiry batch or drained datagram. Nothing on a request,
//! packet or round path allocates; admin commands may.

use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::crdt::{
    CellHandle, CellStore, DeltaSink, ExpirationSink, Expiry, NodeId, Observation,
    ObservationBatch, hash_fingerprint,
};
use crate::defaults::{self, GOSSIP_GAP_REPORTS, GOSSIP_MIN_DATAGRAM_CHARGE};
use crate::discovery::PeerEvent;
use crate::settings::Setting;
use crate::throttle;
use crate::wire::{
    self, AckInfo, CellPayload, DecodeError, Header, OwnAck, Packets, Payload, StreamRange,
    WireScratch,
};

use super::admin::{AdminCommand, AdminSnapshot, GossipCounters};
use super::client::{GossipClient, LimitRequest, Request};
#[cfg(feature = "transport-udp")]
use super::clock::TokioClock;
use super::clock::{Clock, Ticker};
use super::peer::{Contact, GapDue, Insert, PeerFlags, PeerTable, Role};
use super::store::AggregateStore;
use super::sweep::{Accept, Sweep, SweepWant, Sweeps};
#[cfg(feature = "transport-udp")]
use super::transport::UdpTransport;
use super::transport::{GossipTransport, Outbox};
use super::{GossipError, ValidGossipConfig};

/// A round's kind, ordered by strength: a cut round resumes as the strongest
/// kind that came due while it waited.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Round {
    /// While there is prompt work, at most every `gossip.min_emit_interval`:
    /// resends, gap reports, probe answers and sweep pages.
    Prompt,
    /// A rule's unsent local hits crossed its error budget.
    Threshold,
    /// Every `gossip.tick_interval`: expiry, standalone acks, sweep retries.
    Heartbeat,
}

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
    /// One stream frame or sweep page, at most `config.quantum` cells.
    frame_handles: Vec<CellHandle>,
    /// The `sent` value whose own-stream frame `frame_handles` holds this
    /// round, so peers at one position share one walk of the own list.
    frame_from: Option<u64>,
    outbox: Outbox,

    peers: PeerTable,
    /// `B_r`: the most waiting datagrams one round reads, fixed after bind.
    inbound_budget: usize,
    /// Whether a resend, gap report, probe answer, sweep or held packet waits
    /// for a prompt round; the prompt ticker is polled only while it does.
    prompt_work: bool,
    /// The strongest round cut short by a held packet, resumed from
    /// `next_peer` once the packet leaves.
    cut: Option<Round>,
    next_peer: usize,
    /// This node's join sweep, from start until enough sources answered.
    sweep_want: Option<SweepWant>,
    sweeps: Sweeps,
    /// Cells per sweep page: `max(q / gossip.sweep_concurrency, cells in one
    /// packet)`.
    sweep_page: usize,

    // Pending reply for the in-flight limit request. Sent after the
    // bottom-of-loop apply so callers observe "ack ⇒ AggregateStore reflects
    // the increment". Sized to one because only one limit request per
    // iteration is processed.
    pending_reply: Option<oneshot::Sender<()>>,

    counters: GossipCounters,

    // Threshold-triggered anti-entropy state. One column per interned rule
    // slot; sized to the rule dictionary's capacity at construction and reused
    // for the runtime's lifetime, 12 bytes per rule-dictionary slot. Indexed by
    // `RuleSlot` to keep the hot path's single dictionary
    // lookup (`RuleDictionary::find`) the only identity work.
    //
    // - `rule_pending[slot]`     — local hits accumulated against rule `slot`
    //   since the last gossip emit. Zeroed when a round settles (`settle`).
    // - `rule_last_emit_ms[slot]` — wall-clock stamp of the last emit that
    //   included this rule (stamped when a round settles).
    rule_pending: Box<[u32]>,
    rule_last_emit_ms: Box<[u64]>,

    // Set by `handle_limit_request` when the rule's per-site error budget
    // would be exceeded by the new hit and the `min_emit_interval` floor
    // has elapsed. Read at the top of the run loop, which then skips the
    // `tokio::select!` wait and runs a threshold round.
    want_immediate_flush: bool,

    // The per-rule error budget ε the most recent `handle_limit_request`
    // computed — the threshold a burst's accumulated `rule_pending` must
    // cross to trigger a threshold round, surfaced so observers can see it.
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
    /// receiver the admin arm never fires. The runtime starts wanting a join
    /// sweep.
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
        let frame_handles = Vec::with_capacity(config.quantum.get());
        let expiration_buf = ExpirationSink::with_capacity(defaults::GOSSIP_EXPIRATION_BATCH);
        let outbox = Outbox::new(&config);

        let bootstrap = std::mem::take(&mut config.bootstrap_peers);
        let local_key = hash_fingerprint(store.local_identity().node_id.0);
        let rto_ms = config.tick_interval.as_millis() as u64;
        let peers = PeerTable::with_capacity(config.peer_capacity, local_key, rto_ms);
        let inbound_budget = transport.recv_buffer_bytes().map_or(usize::MAX, |bytes| {
            (bytes / GOSSIP_MIN_DATAGRAM_CHARGE).max(1)
        });
        let sweeps = Sweeps::new(config.sweep_concurrency);
        let sweep_page =
            (config.quantum.get() / usize::from(config.sweep_concurrency.get())).max(packet_cells);

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
            frame_from: None,
            outbox,
            peers,
            inbound_budget,
            prompt_work: false,
            cut: None,
            next_peer: 0,
            sweep_want: Some(SweepWant::default()),
            sweeps,
            sweep_page,
            pending_reply: None,
            counters: GossipCounters::default(),
            rule_pending,
            rule_last_emit_ms,
            want_immediate_flush: false,
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
        let mut heartbeat = self.clock.ticker(self.config.tick_interval);
        let mut prompt = self.clock.ticker(self.config.min_emit_interval.duration());
        // Once the peer-event stream returns `None` (e.g. `stream::empty()`),
        // never poll it again — otherwise it returns `Ready(None)` on every
        // iteration and the loop spins.
        let mut peer_events_done = false;

        loop {
            let watch_peers = !peer_events_done;

            let outcome = if self.want_immediate_flush {
                // A previous limit request crossed the per-rule error
                // budget; run a threshold round without waiting. Several
                // requests in one iteration set the flag once.
                self.want_immediate_flush = false;
                ArmOutcome::Round(Round::Threshold, None)
            } else {
                // Split-borrow scope: we hand the I/O futures non-overlapping
                // pieces of `self` so they compose inside `select!`.
                let Self {
                    transport,
                    recv_buf,
                    requests_rx,
                    admin_rx,
                    prompt_work,
                    ..
                } = &mut self;
                let prompt_work = *prompt_work;

                // The tickers come before the inbound arm, so a flood of
                // datagrams cannot starve rounds; rounds read the socket
                // themselves.
                tokio::select! {
                    biased;

                    req = requests_rx.recv() => ArmOutcome::Request(req),
                    at = heartbeat.tick() => ArmOutcome::Round(Round::Heartbeat, Some(at)),
                    _ = prompt.tick(), if prompt_work => ArmOutcome::Round(Round::Prompt, None),
                    recv = transport.recv_from(recv_buf) => ArmOutcome::Inbound(recv),
                    evt = peer_events.next(), if watch_peers => ArmOutcome::PeerEvent(evt),
                    cmd = recv_admin(admin_rx) => ArmOutcome::Admin(cmd),
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
                // A round reads the socket with this task's context, so a
                // transport that arms a wake-up there wakes this loop.
                ArmOutcome::Round(kind, scheduled) => {
                    std::future::poll_fn(|cx| {
                        self.round(kind, scheduled, cx);
                        Poll::Ready(())
                    })
                    .await
                }
            }

            self.apply_rows();

            // Ack any pending limit-request AFTER the apply ran — callers
            // observe "ack ⇒ AggregateStore reflects the increment".
            // Receiver may have dropped; that is not an error.
            if let Some(reply) = self.pending_reply.take() {
                let _ = reply.send(());
            }
        }
    }

    /// Apply the rows the last step produced, if any.
    fn apply_rows(&mut self) {
        if !self.sink_buf.is_empty() || !self.expiration_buf.is_empty() {
            self.aggregates.apply(&self.sink_buf, &self.expiration_buf);
            self.sink_buf.clear();
            self.expiration_buf.clear();
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
            // ingest_local ignores origin/incarnation and forces local
            // identity.
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
        // Every sender's cells merge, a peer's or not; header-only kinds
        // carry none.
        self.store.merge_remote(&self.obs_buf, &mut self.sink_buf);
        self.counters.cells_merged += self.sink_buf.len() as u64;
        let Some(row) = self.peers.find(src) else {
            if header.payload == Payload::SweepRequest {
                self.counters.sweeps_ignored += 1;
            }
            return;
        };
        match self.peers.contact(row, header.sender) {
            Contact::Late => return,
            Contact::First => {
                if let Some(want) = &mut self.sweep_want {
                    want.due = true;
                }
            }
            Contact::Restarted => self.sweeps.end(src),
            Contact::Current => {}
        }
        self.prompt_work |= self.peers.restore(row);

        let now = self.clock.now_millis();
        let head = self.store.own_head();
        // Acks written for another incarnation of this node, or for the
        // flagged mark a `Zone` row does not use, credit nothing.
        if header.acks.incarnation == self.store.local_identity().incarnation
            && let OwnAck::Plain(ack) = header.acks.own
        {
            let mut own = self.peers.own_tx(row);
            own.note_ack(ack, head, now);
            if header.acks.gap_own && own.gap_report(ack, head, now) {
                self.peers.set_flag(row, PeerFlags::RESEND_OWN, true);
            }
        }
        let probe = match header.payload {
            Payload::Cells(CellPayload::Own { range, probe, .. }) => {
                let floor = self.config.min_emit_interval.0.get();
                self.peers.own_in(row).receive(range, now, floor);
                self.peers.set_flag(row, PeerFlags::ACK_DUE, true);
                probe
            }
            Payload::Cells(CellPayload::Relay { probe, .. }) | Payload::Ack { probe } => probe,
            Payload::Cells(CellPayload::Sweep) => {
                if let Some(want) = &mut self.sweep_want {
                    want.answered_by(src);
                }
                false
            }
            Payload::SweepRequest => {
                self.accept_sweep(src);
                false
            }
        };
        if probe {
            self.peers.set_flag(row, PeerFlags::REPLY_DUE, true);
        }
        self.peers.refresh_deadline(row);
        self.prompt_work |= self.sweep_due() || self.peers.flags(row).has(PeerFlags::PROMPT);
    }

    /// Count a dropped packet against its cause, and log at powers of two of
    /// that cause's count, so a flood of one cause cannot hide another.
    fn note_decode_reject(&mut self, error: DecodeError, peer: SocketAddr, length: usize) {
        let max_payload_bytes = self.config.wire_limits.max_payload_bytes();
        match error {
            DecodeError::BadVersion { found } => {
                // A peer on another version is sent nothing until it speaks
                // this one.
                if let Some(row) = self.peers.find(peer) {
                    self.peers.mute(row);
                }
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
                    peers: self.peers.entries().collect(),
                    store_stats: self.store.stats(),
                    counters: self.counters,
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
        for handle in store.active_handles(0) {
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
                let Some(link) = self.peers.remove(p.addr) else {
                    return;
                };
                tracing::info!(
                    peer = %p.addr,
                    cluster_size = self.peers.len(),
                    "Peer left the cluster.",
                );
                // A member that left before sending this node anything may
                // have held cells no stream will bring: sweep again.
                if link.identity.is_none() {
                    self.sweep_want.get_or_insert_default();
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
                         cluster's pod count; each slot costs about 200 bytes.",
                        Setting::GossipPeerCapacity,
                    );
                }
            }
        }
    }

    // -- rounds -------------------------------------------------------------

    /// One round of `kind`. A heartbeat stamps its timers with the instant it
    /// was scheduled for, or `F_r` before the clock after a stall. The work
    /// that needs no socket comes first, so a round with no peers or a held
    /// packet still does it.
    fn round(&mut self, kind: Round, scheduled: Option<u64>, cx: &mut Context<'_>) {
        let clock = self.clock.now_millis();
        let floor = self.config.min_emit_interval.0.get();
        let now = scheduled.map_or(clock, |at| at.max(clock.saturating_sub(floor)));
        self.read_waiting(cx);
        match kind {
            Round::Heartbeat => {
                self.counters.rounds_heartbeat += 1;
                self.expire_cells();
                self.settle();
            }
            Round::Threshold => {
                self.counters.rounds_threshold += 1;
                self.settle();
            }
            Round::Prompt => self.counters.rounds_prompt += 1,
        }

        let kind = self.cut.take().map_or(kind, |cut| cut.max(kind));
        self.outbox.retry(&self.transport, &mut self.counters);
        if self.outbox.is_held() {
            self.cut = Some(kind);
            self.prompt_work = true;
            return;
        }

        self.prompt_work = false;
        self.frame_from = None;
        let head = self.store.own_head();
        let len = self.peers.len();
        for step in 0..len {
            let row = (self.next_peer + step) % len;
            self.serve_row(kind, row, now, head);
            if self.outbox.is_held() {
                self.next_peer = row;
                self.cut = Some(kind);
                self.prompt_work = true;
                return;
            }
        }
        self.ask_sweeps(kind);
        if kind == Round::Prompt {
            self.serve_sweeps();
        }
        self.prompt_work |= self.sweep_due() || self.sweeps.active() > 0 || self.outbox.is_held();
    }

    /// Read the datagrams already waiting, at most `B_r`, so the round sees
    /// the newest acks and an inbound flood cannot hold it. Each datagram's
    /// rows are applied before the next is read. Any error ends the read; a
    /// fatal one surfaces again on the inbound arm.
    fn read_waiting(&mut self, cx: &mut Context<'_>) {
        for _ in 0..self.inbound_budget {
            let Ok((n, src)) = self.transport.try_recv_from(cx, &mut self.recv_buf) else {
                return;
            };
            self.handle_inbound(n, src);
            self.apply_rows();
        }
    }

    /// Service one peer row: time out its stream, report a due gap, send it
    /// its own-stream cells, and send owed acks or probe answers alone when no
    /// data carries them.
    fn serve_row(&mut self, kind: Round, row: usize, now: u64, head: u64) {
        if self.peers.role(row) == Role::Incompatible {
            return;
        }
        let due = self.peers.due(row, now);
        let retry = due && self.peers.own_tx(row).timed_out(now);
        if retry {
            self.peers.own_tx(row).rewind(now);
            self.counters.rto_rewinds_own += 1;
        }
        let floor = self.config.min_emit_interval.0.get();
        let gap = if due {
            self.peers.own_in(row).take_report(now, floor)
        } else {
            GapDue::Nothing
        };
        if let GapDue::Abandoned { missing } = gap {
            self.note_gap_abandoned(row, missing);
        }
        let gap_own = gap == GapDue::Report;

        let flags = self.peers.flags(row);
        let own_due = kind != Round::Prompt || retry || flags.has(PeerFlags::RESEND_OWN);
        let sent = self.peers.own_sent(row);
        let mut sent_any = false;
        if own_due && sent < head {
            let closed = self.send_own(row, sent, head, gap_own, retry);
            self.peers.own_tx(row).note_sent(closed, now);
            // A stream capped by the quantum goes on in the next prompt round.
            self.peers
                .set_flag(row, PeerFlags::RESEND_OWN, closed < head);
            sent_any = true;
        } else if own_due {
            self.peers.set_flag(row, PeerFlags::RESEND_OWN, false);
        }
        let ack_due = kind == Round::Heartbeat && flags.has(PeerFlags::ACK_DUE);
        if !sent_any && (gap_own || ack_due || flags.has(PeerFlags::REPLY_DUE)) {
            let header = self.header_for(row, gap_own, Payload::Ack { probe: false });
            let dst = self.peers.addr(row);
            self.outbox
                .send_header(&header, dst, &self.transport, &mut self.counters);
            sent_any = true;
        }
        if sent_any {
            self.peers.set_flag(row, PeerFlags::ACK_DUE, false);
            self.peers.set_flag(row, PeerFlags::REPLY_DUE, false);
        }
        if gap_own {
            self.counters.gap_reports_sent += 1;
        }
        if due || sent_any {
            self.peers.refresh_deadline(row);
        }
        self.prompt_work |= self.peers.flags(row).has(PeerFlags::PROMPT);
    }

    /// Send `row` the own-stream frame above `sent` and return the position
    /// it closes through. A frame that holds every live cell up to the head
    /// closes through the head, so cells that expired unsent are closed, not
    /// owed; with no live cell left it is one closing packet without cells.
    fn send_own(&mut self, row: usize, sent: u64, head: u64, gap_own: bool, probe: bool) -> u64 {
        let quantum = self.config.quantum.get();
        if self.frame_from != Some(sent) {
            self.store
                .stream_after(sent, quantum, &mut self.frame_handles);
            self.frame_from = Some(sent);
        }
        let upper = match self.frame_handles.last() {
            Some(&last) if self.frame_handles.len() == quantum => self
                .store
                .get(last)
                .map_or(head, |cell| cell.origin_sequence),
            _ => head,
        };
        let Some(range) = StreamRange::new(sent, upper) else {
            return sent;
        };
        let payload = CellPayload::Own {
            range,
            relay_into_zone: false,
            probe,
        };
        let header = self.header_for(row, gap_own, payload);
        let dst = self.peers.addr(row);
        if self.frame_handles.is_empty() {
            self.outbox
                .send_header(&header, dst, &self.transport, &mut self.counters);
            return upper;
        }
        let packets = Packets::new(
            header,
            &self.store,
            &self.frame_handles,
            &mut self.scratch,
            self.config.wire_limits,
        );
        self.outbox
            .send_frame(packets, dst, &self.transport, &mut self.counters)
    }

    /// A header to `row`, carrying this node's acks for the peer's stream as
    /// of the peer's incarnation this node knows.
    fn header_for<P>(&self, row: usize, gap_own: bool, payload: P) -> Header<P> {
        let link = self.peers.link(row);
        Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender: self.store.local_identity(),
            sender_zone: 0,
            acks: AckInfo {
                incarnation: link.identity.map_or(0, |identity| identity.incarnation),
                own: OwnAck::Plain(link.own_in.have()),
                relay: 0,
                gap_own,
                gap_relay: false,
                cannot_relay: false,
                probe_reply: self.peers.flags(row).has(PeerFlags::REPLY_DUE),
            },
            payload,
        }
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
    /// its emit floor. Every heartbeat and threshold round does it before it
    /// can return early, so a lone node or a blocked socket does not run a
    /// threshold round per request. The hits ride this round's streams or
    /// stay above each peer's `sent` for the next.
    fn settle(&mut self) {
        let stamp = self.clock.now_millis().max(1);
        for slot in 0..self.rule_pending.len() {
            if self.rule_pending[slot] != 0 {
                self.rule_pending[slot] = 0;
                self.rule_last_emit_ms[slot] = stamp;
            }
        }
    }

    // -- join sweeps --------------------------------------------------------

    /// Whether a peer was first heard while the want waits.
    fn sweep_due(&self) -> bool {
        self.sweep_want.as_ref().is_some_and(|want| want.due)
    }

    /// Ask for the join sweep this node wants: at every heartbeat, and on any
    /// round once a peer is first heard. Each source asked is sent a
    /// `SweepRequest` at once and again at each heartbeat until a page
    /// arrives.
    fn ask_sweeps(&mut self, kind: Round) {
        let Some(mut want) = self
            .sweep_want
            .take_if(|want| kind == Round::Heartbeat || want.due)
        else {
            return;
        };
        want.due = false;
        if want.advance(|| self.peers.sweep_candidates()) {
            return;
        }
        for ask in want.asks() {
            if (kind != Round::Heartbeat && ask.tries > 0) || self.outbox.is_held() {
                continue;
            }
            let Some(row) = self.peers.find(ask.addr) else {
                continue;
            };
            let header = self.header_for(row, false, Payload::SweepRequest);
            self.outbox
                .send_header(&header, ask.addr, &self.transport, &mut self.counters);
            ask.tries += 1;
            self.counters.sweeps_requested += 1;
        }
        self.sweep_want = Some(want);
    }

    /// A peer asked for a join sweep. One already being swept is not swept
    /// twice; past `gossip.sweep_concurrency` the request is declined.
    fn accept_sweep(&mut self, src: SocketAddr) {
        match self.sweeps.accept(src) {
            Accept::Started => {
                self.counters.sweeps_served += 1;
                self.prompt_work = true;
            }
            Accept::Running => {}
            Accept::Full => {
                if let Some(refused_total) = throttle::bump(&mut self.counters.sweeps_refused) {
                    let (active, limit) = (self.sweeps.active(), self.sweeps.limit());
                    tracing::warn!(
                        code = "sweep_declined",
                        peer = %src,
                        active,
                        limit,
                        refused_total,
                        config_key = Setting::GossipSweepConcurrency.yaml_key(),
                        "Declined a catch-up request from {src}, a newly started node, because \
                         this node is already sending its live counts to {active} other new \
                         nodes, the most it serves at once. {src} asks again at its next \
                         heartbeat and then asks another peer, so it most likely still catches \
                         up. Many of these together mean many pods started at once (a rollout or \
                         scale-out). No action is needed unless new pods keep under-counting for \
                         longer than one rate-limit window; then slow the rollout. Raising {} \
                         reduces these refusals, but each sweep then runs slower, so it does not \
                         speed the rollout.",
                        Setting::GossipSweepConcurrency,
                    );
                }
            }
        }
    }

    /// Send each running sweep its next page of live cells in slot order; a
    /// requester that left or speaks another version is dropped. A page cut
    /// by a held packet is sent again whole: pages are max-merges.
    fn serve_sweeps(&mut self) {
        // Pages overwrite `frame_handles`: no own frame survives them.
        self.frame_from = None;
        for i in 0..self.sweeps.limit() {
            let Some(Sweep { dst, next_slot }) = self.sweeps.get(i) else {
                continue;
            };
            if self.outbox.is_held() {
                return;
            }
            let live = self.peers.find(dst);
            let Some(row) = live.filter(|&row| self.peers.role(row) != Role::Incompatible) else {
                self.sweeps.advance(i, None);
                continue;
            };
            self.frame_handles.clear();
            self.frame_handles
                .extend(self.store.active_handles(next_slot).take(self.sweep_page));
            let cells = self.frame_handles.len();
            if cells == 0 && next_slot != 0 {
                self.sweeps.advance(i, None);
                continue;
            }
            // A store with no live cell still answers with one empty page.
            let header = self.header_for(row, false, CellPayload::Sweep);
            if cells == 0 {
                self.outbox
                    .send_header(&header, dst, &self.transport, &mut self.counters);
            } else {
                let packets = Packets::new(
                    header,
                    &self.store,
                    &self.frame_handles,
                    &mut self.scratch,
                    self.config.wire_limits,
                );
                self.outbox
                    .send_frame(packets, dst, &self.transport, &mut self.counters);
            }
            if self.outbox.is_held() {
                return;
            }
            self.counters.sweep_cells_sent += cells as u64;
            let next = match self.frame_handles.last() {
                Some(last) if cells == self.sweep_page => Some(last.index + 1),
                _ => None,
            };
            self.sweeps.advance(i, next);
        }
    }

    /// Log `gap_abandoned` for `row`'s own stream at powers of two.
    fn note_gap_abandoned(&mut self, row: usize, missing: u64) {
        let Some(abandoned_total) = throttle::bump(&mut self.counters.gaps_abandoned) else {
            return;
        };
        let peer = self.peers.addr(row);
        let reports = GOSSIP_GAP_REPORTS;
        // Reports go out `F_r` after the gap opens and `4 · F_r` apart.
        let waited_ms = self.config.min_emit_interval.0.get() * (1 + 4 * u64::from(reports));
        tracing::warn!(
            code = "gap_abandoned",
            peer = %peer,
            stream = "own",
            missing,
            reports,
            waited_ms,
            abandoned_total,
            "Stopped asking {peer} to resend {missing} changes missing from its own stream: it did \
             not answer {reports} requests over {waited_ms} ms. If {peer} still sends this \
             stream, its retransmission timer resends the missing changes; if its gateway role \
             moved or it restarted, they arrive from the new sender. Until they arrive, limits \
             here under-count the traffic {peer} handled. If this repeats for a peer that is \
             running, UDP from {peer} to this node is usually blocked or lossy in one direction: \
             check NetworkPolicies and security groups between the two.",
        );
    }
}

enum ArmOutcome {
    Request(Option<Request>),
    Inbound(io::Result<(usize, SocketAddr)>),
    PeerEvent(Option<PeerEvent>),
    Admin(Option<AdminCommand>),
    Round(Round, Option<u64>),
}

/// Read one admin command; never resolves once the channel is gone.
async fn recv_admin(rx: &mut Option<mpsc::Receiver<AdminCommand>>) -> Option<AdminCommand> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}
