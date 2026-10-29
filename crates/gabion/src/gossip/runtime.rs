//! Single-threaded gossip event loop.
//!
//! One `tokio::select!` drives five arms — limit/shutdown requests, the
//! heartbeat, the prompt-round pacer (polled only while there is prompt
//! work), inbound UDP and peer membership churn — plus admin commands when
//! wired. Every round reads at most `B_r` waiting datagrams, then judges
//! which peers are suspect (never while a refused packet is held), refreshes
//! the roles, and serves each peer in one send loop: its own-stream cells
//! (to zone peers, and flagged for relay to one gateway per remote zone),
//! the relay stream to zone peers, a resend with `PROBE` when a heartbeat
//! passed without an ack (a header-only probe to a suspect), and owed acks,
//! gap reports and probe answers. After each iteration the runtime calls
//! [`AggregateStore::apply`] once with the rows the arm produced, and once
//! more per full expiry batch or drained datagram. Nothing on a request,
//! packet or round path allocates; admin commands may.

use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::task::{Context, Poll};

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};

use crate::crdt::{
    CellHandle, CellStore, DeltaSink, ExpirationSink, Expiry, NodeId, Observation,
    ObservationBatch, RelayMark, Stream, hash_fingerprint,
};
use crate::defaults::{self, GOSSIP_GAP_REPORTS, GOSSIP_MIN_DATAGRAM_CHARGE, GOSSIP_SWEEP_PEERS};
use crate::discovery::PeerEvent;
use crate::settings::Setting;
use crate::throttle;
use crate::wire::{
    self, AckInfo, CellPayload, DecodeError, Header, Packets, Payload, StreamRange, WireScratch,
};
use crate::zone::{ZoneHash, ZoneName};

use super::admin::{AdminCommand, AdminSnapshot, GossipCounters};
use super::client::{GossipClient, LimitRequest, Request};
#[cfg(feature = "transport-udp")]
use super::clock::TokioClock;
use super::clock::{Clock, Ticker};
use super::peer::{Contact, Failover, GapDue, Insert, PeerFlags, PeerTable, Role, Stall};
use super::store::AggregateStore;
use super::sweep::{Accept, Sweep, SweepWant, Sweeps};
#[cfg(feature = "transport-udp")]
use super::transport::UdpTransport;
use super::transport::{GossipTransport, Outbox, Sent};
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

/// One stream's frame: its handles, and the `sent` value they were read
/// above this round, so peers at one position share one walk of the list.
struct Frame {
    handles: Vec<CellHandle>,
    from: Option<u64>,
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
    /// One frame per stream, indexed by [`Stream`]; a sweep page uses the
    /// own stream's.
    frames: [Frame; 2],
    outbox: Outbox,
    /// This node's zone, announced in every header.
    zone: Option<ZoneHash>,
    /// Most of the zone peers this node has heard are suspect: every packet
    /// says it cannot relay. Judged at each heartbeat.
    cannot_relay: bool,

    peers: PeerTable,
    /// `B_r`: the most waiting datagrams one round reads, fixed after bind.
    inbound_budget: usize,
    /// Whether a resend, gap report, probe answer or sweep waits for a prompt
    /// round; the prompt ticker is polled only while one does or a packet is
    /// held.
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
        let frame = || Frame {
            handles: Vec::with_capacity(config.quantum.get()),
            from: None,
        };
        let frames = [frame(), frame()];
        let expiration_buf = ExpirationSink::with_capacity(defaults::GOSSIP_EXPIRATION_BATCH);
        let outbox = Outbox::new(&config);

        let bootstrap = std::mem::take(&mut config.bootstrap_peers);
        let local_key = hash_fingerprint(store.local_identity().node_id.0);
        let rto_ms = config.tick_interval.as_millis() as u64;
        let suspect_after = config.suspect_after.0.get();
        let mut peers =
            PeerTable::with_capacity(config.peer_capacity, local_key, rto_ms, suspect_after);
        let zone = config.zone.as_ref().map(ZoneName::hash);
        if let Some(zone) = zone {
            peers.set_local_zone(zone);
        }
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
            frames,
            outbox,
            zone,
            cannot_relay: false,
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
        St: futures::Stream<Item = PeerEvent>,
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
                    outbox,
                    ..
                } = &mut self;
                let prompt_work = *prompt_work || outbox.is_held();

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
        let flagged = matches!(
            header.payload,
            Payload::Cells(CellPayload::Own {
                relay_into_zone: true,
                ..
            })
        );
        // A sender outside the table is merge-only: merged, never acked,
        // never relayed.
        let Some(row) = self.peers.find(src) else {
            if header.payload == Payload::SweepRequest {
                self.counters.sweeps_ignored += 1;
            }
            return self.merge(RelayMark::Clear);
        };
        match self.peers.contact(row, header.sender, header.sender_zone) {
            // Its freshness cannot be judged, but what it raised is relayed.
            Contact::Late if flagged => return self.merge(RelayMark::Changed),
            Contact::Late => return self.merge(RelayMark::Clear),
            Contact::First => {
                if let Some(want) = &mut self.sweep_want {
                    want.due = true;
                }
            }
            Contact::Restarted => self.sweeps.end(src),
            Contact::Current => {}
        }
        self.counters.role_changes += u64::from(self.peers.restore(row));

        let now = self.clock.now_millis();
        let floor = self.config.min_emit_interval.0.get();
        let (mark, probe) = match header.payload {
            Payload::Cells(CellPayload::Own {
                range,
                relay_into_zone,
                probe,
            }) => {
                let fresh = self
                    .peers
                    .receive_own(row, range, relay_into_zone, now, floor);
                let mark = match (relay_into_zone, fresh) {
                    (false, _) => RelayMark::Clear,
                    (true, false) => RelayMark::Changed,
                    (true, true) => RelayMark::All,
                };
                (mark, probe)
            }
            Payload::Cells(CellPayload::Relay { range, probe }) => {
                self.peers.receive_relay(row, range, now, floor);
                (RelayMark::Clear, probe)
            }
            Payload::Ack { probe } => (RelayMark::Clear, probe),
            Payload::Cells(CellPayload::Sweep) => {
                if let Some(want) = &mut self.sweep_want {
                    want.answered_by(src);
                }
                (RelayMark::Clear, false)
            }
            Payload::SweepRequest => {
                self.accept_sweep(src);
                (RelayMark::Clear, false)
            }
        };
        let relay_head = self.store.head(Stream::Relay);
        self.merge(mark);
        let heads = [self.store.head(Stream::Own), self.store.head(Stream::Relay)];
        let incarnation = self.store.local_identity().incarnation;
        let credit = self.peers.credit(row, header.acks, incarnation, heads, now);
        self.counters.rto_rewinds_own += u64::from(credit.answer_rewound[0]);
        self.counters.rto_rewinds_relay += u64::from(credit.answer_rewound[1]);
        if probe {
            self.peers.set_flag(row, PeerFlags::REPLY_DUE, true);
        }
        self.prompt_work |= heads[1] > relay_head
            || credit.resend_relay
            || self.peers.stale()
            || self.sweep_due()
            || self.peers.flags(row).has(PeerFlags::PROMPT);
    }

    /// Merge the decoded cells, moving remote ones on or off the relay list
    /// by `mark`.
    fn merge(&mut self, mark: RelayMark) {
        self.store
            .merge_remote(&self.obs_buf, &mut self.sink_buf, mark);
        self.counters.cells_merged += self.sink_buf.len() as u64;
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
                    self.counters.role_changes += u64::from(self.peers.mute(row));
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
                        incompatible_peers = self.peers.role_counts().incompatible,
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
                    peers_by_role: self.peers.role_counts(),
                    peers: self.peers.entries(self.clock.now_millis()).collect(),
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
                self.prompt_work = true;
            }
            // A configured or already learned zone wins.
            PeerEvent::OwnEntry { zone } if self.zone.is_none() => {
                self.zone = Some(zone);
                self.peers.set_local_zone(zone);
                // Same-zone peers may now differ: sweep again.
                let mut want = SweepWant::default();
                want.due = true;
                self.sweep_want = Some(want);
                self.prompt_work = true;
            }
            PeerEvent::OwnEntry { .. } => {}
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
                         cluster's pod count; each slot costs about 330 bytes.",
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
        if let Some((held, sent)) = self.outbox.retry(&self.transport, &mut self.counters) {
            // No peer could answer while the packet was held.
            self.peers
                .shift_owed(now.saturating_sub(held.since_ms), now);
            if let Some(row) = self.peers.find(held.dst) {
                self.account(row, sent, held.asks, now);
            }
        }
        if self.outbox.is_held() {
            self.cut = Some(kind);
            return;
        }

        // Judged after the read and before any timer, never while held.
        let counters = &mut self.counters;
        self.peers
            .judge(now, |stall| note_relay_stall(counters, stall));
        if kind == Round::Heartbeat {
            self.cannot_relay = self.peers.impaired();
        }
        let suspect_after_ms = self.config.suspect_after.0.get();
        let counters = &mut self.counters;
        let changes = self.peers.refresh(now, |failover| {
            note_failover(counters, failover, suspect_after_ms)
        });
        self.counters.role_changes += changes;

        self.prompt_work = false;
        for frame in &mut self.frames {
            frame.from = None;
        }
        let heads = [self.store.head(Stream::Own), self.store.head(Stream::Relay)];
        let len = self.peers.len();
        for step in 0..len {
            let row = (self.next_peer + step) % len;
            self.serve_row(kind, row, now, heads);
            if self.outbox.is_held() {
                self.next_peer = row;
                self.cut = Some(kind);
                return;
            }
        }
        self.ask_sweeps(kind, now);
        if kind == Round::Prompt {
            self.serve_sweeps(now);
        }
        self.prompt_work |= self.sweep_due() || self.sweeps.active() > 0;
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

    /// Service one peer row: retry a timed-out stream (a header-only probe
    /// to a suspect, or to a peer owed nothing), report due gaps, send the
    /// streams its role gets, and send owed acks or probe answers alone when
    /// no data carries them.
    fn serve_row(&mut self, kind: Round, row: usize, now: u64, heads: [u64; 2]) {
        let role = self.peers.role(row);
        if role == Role::Incompatible {
            return;
        }
        let due = self.peers.due(row, now);
        let fired = |timer: Option<u64>| due && timer.is_some_and(|at| at <= now);
        let own_retry = fired(self.peers.own_timer(row));
        let relay_retry = fired(self.peers.relay_timer(row));
        let own_in_flight = role != Role::Remote && self.peers.tx(row, Stream::Own).in_flight();
        let suspect = self.peers.suspect(row);
        let probe_only =
            (own_retry || relay_retry) && (suspect || (!own_in_flight && !relay_retry));
        for (stream, retry, counter) in [
            (Stream::Own, own_retry, &mut self.counters.rto_rewinds_own),
            (
                Stream::Relay,
                relay_retry,
                &mut self.counters.rto_rewinds_relay,
            ),
        ] {
            if !retry {
                continue;
            }
            let mut tx = self.peers.tx(row, stream);
            if probe_only {
                tx.restart(now);
            } else {
                *counter += u64::from(tx.in_flight());
                tx.rewind(now);
            }
        }
        let floor = self.config.min_emit_interval.0.get();
        let reports = if due {
            self.peers.take_reports(row, now, floor)
        } else {
            [GapDue::Nothing; 2]
        };
        for (stream, report) in [Stream::Own, Stream::Relay].into_iter().zip(reports) {
            if let GapDue::Abandoned { missing } = report {
                self.note_gap_abandoned(row, stream, missing);
            }
        }
        let gaps = reports.map(|report| report == GapDue::Report);

        let flags = self.peers.flags(row);
        let resend_own = own_retry && !probe_only;
        let mut sent_any = false;
        if role != Role::Remote
            && (kind != Round::Prompt || resend_own || flags.has(PeerFlags::RESEND_OWN))
        {
            let closed = self.send_stream(Stream::Own, row, heads[0], gaps, resend_own, now);
            // A stream capped by the quantum goes on in the next prompt round.
            self.peers.set_flag(
                row,
                PeerFlags::RESEND_OWN,
                closed.is_some_and(|c| c < heads[0]),
            );
            sent_any |= closed.is_some();
        }
        if role == Role::Zone {
            let resend = relay_retry && !probe_only;
            let closed = self.send_stream(Stream::Relay, row, heads[1], gaps, resend, now);
            self.prompt_work |= closed.is_some_and(|c| c < heads[1]);
            sent_any |= closed.is_some();
        }
        // Data asks for an answer too, so only a suspect is probed beside it.
        let probe = probe_only && (suspect || !sent_any);
        let ack_due = kind == Round::Heartbeat && flags.has(PeerFlags::ACK_DUE);
        let owed = gaps.contains(&true) || ack_due || flags.has(PeerFlags::REPLY_DUE);
        if probe || (!sent_any && owed) {
            let header = self.header_for(row, gaps, Payload::Ack { probe });
            let dst = self.peers.addr(row);
            let sent =
                (self.outbox).send_header(&header, dst, now, &self.transport, &mut self.counters);
            self.account(row, sent, probe, now);
            self.counters.liveness_probes += u64::from(probe);
            sent_any = true;
        }
        if sent_any {
            self.peers.set_flag(row, PeerFlags::ACK_DUE, false);
            self.peers.set_flag(row, PeerFlags::REPLY_DUE, false);
        }
        self.counters.gap_reports_sent += gaps.iter().filter(|&&gap| gap).count() as u64;
        if due || sent_any {
            self.peers.refresh_deadline(row);
        }
        self.prompt_work |= self.peers.flags(row).has(PeerFlags::PROMPT);
    }

    /// Send `row` the frame of `stream` above its `sent`, if any, and note
    /// it; returns the position it closes through. A frame that holds every
    /// live cell up to the head closes through the head, so cells that
    /// expired unsent are closed, not owed; with no live cell left it is one
    /// closing packet without cells. Own frames to a gateway are flagged for
    /// relay.
    fn send_stream(
        &mut self,
        stream: Stream,
        row: usize,
        head: u64,
        gaps: [bool; 2],
        probe: bool,
        now: u64,
    ) -> Option<u64> {
        let sent = self.peers.sent(row, stream);
        if sent >= head {
            return None;
        }
        let quantum = self.config.quantum.get();
        let frame = &mut self.frames[stream as usize];
        if frame.from != Some(sent) {
            self.store
                .stream_after(stream, sent, quantum, &mut frame.handles);
            frame.from = Some(sent);
        }
        let upper = match frame.handles.last() {
            Some(&last) if frame.handles.len() == quantum => {
                self.store.sequence(stream, last).unwrap_or(head)
            }
            _ => head,
        };
        let range = StreamRange::new(sent, upper)?;
        let payload = match stream {
            Stream::Own => CellPayload::Own {
                range,
                relay_into_zone: self.peers.role(row) == Role::Gateway,
                probe,
            },
            Stream::Relay => CellPayload::Relay { range, probe },
        };
        let header = self.header_for(row, gaps, payload);
        let dst = self.peers.addr(row);
        let handles = &self.frames[stream as usize].handles;
        let (closed, sent) = if handles.is_empty() {
            let sent =
                (self.outbox).send_header(&header, dst, now, &self.transport, &mut self.counters);
            (upper, sent)
        } else {
            let packets = Packets::new(
                header,
                &self.store,
                handles,
                &mut self.scratch,
                self.config.wire_limits,
            );
            (self.outbox).send_frame(packets, dst, now, &self.transport, &mut self.counters)
        };
        self.peers.tx(row, stream).note_sent(closed, now);
        self.account(row, sent, true, now);
        Some(closed)
    }

    /// Count a send to `row`: its bytes when the peer is in another known
    /// zone, and the wait for an answer it may start.
    fn account(&mut self, row: usize, sent: Sent, asks: bool, now: u64) {
        if matches!(self.peers.role(row), Role::Gateway | Role::Remote) {
            self.counters.bytes_sent_cross_zone += sent.bytes;
        }
        self.peers.note_send(row, sent, asks, now);
    }

    /// A header to `row`, carrying this node's zone, its acks for the peer's
    /// streams as of the peer's incarnation this node knows, and its signals.
    fn header_for<P>(&self, row: usize, gaps: [bool; 2], payload: P) -> Header<P> {
        let link = self.peers.link(row);
        Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender: self.store.local_identity(),
            sender_zone: self.zone,
            acks: AckInfo {
                incarnation: link.identity.map_or(0, |identity| identity.incarnation),
                own: link.own_ack(),
                relay: link.relay_ack(),
                gap_own: gaps[0],
                gap_relay: gaps[1],
                cannot_relay: self.cannot_relay,
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
    /// arrives. A node that has heard no peer of its zone asks one peer of
    /// any zone.
    fn ask_sweeps(&mut self, kind: Round, now: u64) {
        let Some(mut want) = self
            .sweep_want
            .take_if(|want| kind == Round::Heartbeat || want.due)
        else {
            return;
        };
        want.due = false;
        let any_zone = self.peers.sweep_candidates(false).next().is_none();
        let sources = if any_zone { 1 } else { GOSSIP_SWEEP_PEERS };
        if want.advance(|| self.peers.sweep_candidates(any_zone), sources) {
            return;
        }
        for ask in want.asks() {
            if (kind != Round::Heartbeat && ask.tries > 0) || self.outbox.is_held() {
                continue;
            }
            let Some(row) = self.peers.find(ask.addr) else {
                continue;
            };
            let header = self.header_for(row, [false; 2], Payload::SweepRequest);
            self.outbox
                .send_header(&header, ask.addr, now, &self.transport, &mut self.counters);
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
    fn serve_sweeps(&mut self, now: u64) {
        // Pages overwrite the own frame: none survives them.
        let frame = &mut self.frames[Stream::Own as usize];
        frame.from = None;
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
            let handles = &mut self.frames[Stream::Own as usize].handles;
            handles.clear();
            handles.extend(self.store.active_handles(next_slot).take(self.sweep_page));
            let cells = handles.len();
            if cells == 0 && next_slot != 0 {
                self.sweeps.advance(i, None);
                continue;
            }
            // A store with no live cell still answers with one empty page.
            let header = self.header_for(row, [false; 2], CellPayload::Sweep);
            let handles = &self.frames[Stream::Own as usize].handles;
            if cells == 0 {
                self.outbox
                    .send_header(&header, dst, now, &self.transport, &mut self.counters);
            } else {
                let packets = Packets::new(
                    header,
                    &self.store,
                    handles,
                    &mut self.scratch,
                    self.config.wire_limits,
                );
                self.outbox
                    .send_frame(packets, dst, now, &self.transport, &mut self.counters);
            }
            if self.outbox.is_held() {
                return;
            }
            self.counters.sweep_cells_sent += cells as u64;
            let next = match self.frames[Stream::Own as usize].handles.last() {
                Some(last) if cells == self.sweep_page => Some(last.index + 1),
                _ => None,
            };
            self.sweeps.advance(i, next);
        }
    }

    /// Log `gap_abandoned` for `row`'s `stream` at powers of two.
    fn note_gap_abandoned(&mut self, row: usize, stream: Stream, missing: u64) {
        let Some(abandoned_total) = throttle::bump(&mut self.counters.gaps_abandoned) else {
            return;
        };
        let peer = self.peers.addr(row);
        let reports = GOSSIP_GAP_REPORTS;
        // Reports go out `F_r` after the gap opens and `4 · F_r` apart.
        let waited_ms = self.config.min_emit_interval.0.get() * (1 + 4 * u64::from(reports));
        let stream = match stream {
            Stream::Own => "own",
            Stream::Relay => "relay",
        };
        tracing::warn!(
            code = "gap_abandoned",
            peer = %peer,
            stream,
            missing,
            reports,
            waited_ms,
            abandoned_total,
            "Stopped asking {peer} to resend {missing} changes missing from its {stream} stream: it did \
             not answer {reports} requests over {waited_ms} ms. If {peer} still sends this \
             stream, its retransmission timer resends the missing changes; if its gateway role \
             moved or it restarted, they arrive from the new sender. Until they arrive, limits \
             here under-count the traffic {peer} handled. If this repeats for a peer that is \
             running, UDP from {peer} to this node is usually blocked or lossy in one direction: \
             check NetworkPolicies and security groups between the two.",
        );
    }
}

/// Count a zone peer's stalled relay, and log `relay_stalled` at powers of
/// two.
fn note_relay_stall(counters: &mut GossipCounters, stall: Stall) {
    let Some(stall_total) = throttle::bump(&mut counters.relay_stalls) else {
        return;
    };
    let Stall {
        peer,
        stalled_ms,
        relay_unacked,
    } = stall;
    tracing::warn!(
        code = "relay_stalled",
        peer = %peer,
        stalled_ms,
        relay_unacked,
        stall_total,
        "{peer}, a peer in this node's zone, has not acknowledged the changes this node relays \
         into the zone for {stalled_ms} ms, while this node keeps receiving them from other \
         zones. {peer} is missing those counts, so limits enforced there under-count and allow \
         requests over the limit. gabion keeps resending, and the counts arrive as soon as the \
         path works. Usually UDP from this node to {peer} is blocked in one direction, or {peer} \
         is overloaded. Check NetworkPolicies and security groups between the two pods, and the \
         peer's CPU.",
    );
}

/// Count a gateway failover. The first logs at info, with its reason; later
/// ones warn at powers of two, since repeated failovers delay counts.
fn note_failover(counters: &mut GossipCounters, failover: Failover, suspect_after_ms: u64) {
    let Some(failover_total) = throttle::bump(&mut counters.gateway_failovers) else {
        return;
    };
    let Failover {
        zone,
        old,
        new,
        cannot_relay,
        unanswered_ms,
    } = failover;
    let reason = if cannot_relay {
        "cannot_relay"
    } else {
        "unanswered"
    };
    let config_key = Setting::GossipSuspectAfter.yaml_key();
    match (failover_total, cannot_relay) {
        (1, false) => tracing::info!(
            code = "gateway_failover",
            zone_hash = %zone,
            old_gateway = %old,
            new_gateway = %new,
            reason,
            unanswered_ms,
            suspect_after_ms,
            failover_total,
            config_key,
            "Switched this node's gateway for zone {zone} from {old} to {new}, because {old} \
             answered none of this node's packets or liveness checks for {unanswered_ms} ms \
             (`gossip.suspect_after` is {suspect_after_ms} ms). {new} now receives every live \
             change from this node that it has not acknowledged, so nothing is lost. Nothing to \
             do if {old} restarted or left the cluster.",
        ),
        (1, true) => tracing::info!(
            code = "gateway_failover",
            zone_hash = %zone,
            old_gateway = %old,
            new_gateway = %new,
            reason,
            unanswered_ms,
            suspect_after_ms,
            failover_total,
            config_key,
            "Switched this node's gateway for zone {zone} from {old} to {new}, because {old} \
             reported that it cannot reach most of its own zone. {new} now receives and relays \
             every live change from this node, so nothing is lost. {old} logs which peers it \
             cannot reach; start there.",
        ),
        _ => tracing::warn!(
            code = "gateway_failover",
            zone_hash = %zone,
            old_gateway = %old,
            new_gateway = %new,
            reason,
            unanswered_ms,
            suspect_after_ms,
            failover_total,
            config_key,
            "This node has switched gateways {failover_total} times since it started. Each switch \
             delays this node's counts in the affected zone by up to `gossip.suspect_after` plus \
             three heartbeats, so limits there briefly under-count and allow requests over the \
             limit. Usually pods in that zone keep restarting, or UDP between this node and that \
             zone drops packets. Check restarts in zone {zone}, then the network path and any \
             NetworkPolicy or security group between the zones. If the pods are healthy and the \
             network between zones loses packets, raising {} makes switches rarer at the cost of \
             slower failover.",
            Setting::GossipSuspectAfter,
        ),
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
