//! Single-threaded gossip event loop.
//!
//! One `tokio::select!` drives six arms — limit/shutdown requests, inbound
//! UDP, outbound writable, peer membership churn, gossip tick, relay tick.
//! After each iteration the runtime calls [`AggregateStore::apply`] once with
//! whatever rows the arm produced. The runtime owns the [`CellStore`],
//! pre-allocated scratch buffers, the outbound send pool, and the peer table;
//! nothing in the loop allocates after construction.
//!
//! Dissemination is two-level, with zones (availability zones) as the levels.
//! A node sends its own cells to every peer in its zone and to one *gateway*
//! in each other zone; a gateway re-sends what it received from the origin to
//! every other member of its own zone. Each hop is a cumulatively
//! acknowledged *stream*, so loss is repaired hop by hop. See "Design notes"
//! in `crates/gabion/README.md`.

use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::crdt::{
    CellHandle, CellStore, CompactCellKey, Count, DeltaSink, ExpirationSink, NodeId, Observation,
    ObservationBatch,
};
use crate::defaults::{GOSSIP_SWEEP_CELLS_PER_ROUND, GOSSIP_SWEEP_CONCURRENCY};
use crate::discovery::PeerEvent;
use crate::wire::{
    self, FLAG_GAP_OWN, FLAG_GAP_RELAY, FLAG_RELAY, FLAG_SWEEP, FLAG_SWEEP_REQUEST, PacketBuf,
    Packets, WireScratch,
};

use super::admin::{AdminCommand, AdminSnapshot, PeerEntry};
use super::client::{GossipClient, LimitRequest, Request};
#[cfg(feature = "transport-udp")]
use super::clock::TokioClock;
use super::clock::{Clock, Ticker};
use super::peer::{Peer, refresh_gateways, sweep_sources};
use super::store::AggregateStore;
use super::topology::Zone;
use super::transport::GossipTransport;
#[cfg(feature = "transport-udp")]
use super::transport::UdpTransport;
use super::{GossipConfig, GossipError};

/// Which stream a packet's cells belong to.
#[derive(Clone, Copy, Eq, PartialEq)]
enum StreamKind {
    /// The sender's own-origin cells, sequenced by `origin_sequence`.
    Own,
    /// Cells a gateway forwards into its zone, sequenced by relay stamps.
    Relay,
}

/// A sweep this node is answering: its live window, paced out to `dst` in
/// cell-slot order from `next`.
#[derive(Clone, Copy)]
struct Sweep {
    dst: SocketAddr,
    next: u32,
}

/// What woke the gossip loop to send.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Round {
    /// The heartbeat tick: expire, send both streams, owe standalone acks.
    Heartbeat,
    /// An error-budget flush between heartbeats: as `Heartbeat`, minus the
    /// standalone acks.
    Threshold,
    /// Work that should not wait for the heartbeat: a gateway forwarding
    /// freshly received cells into its zone, a stream a peer reported a gap
    /// in, and gap reports of our own. Sends the relay stream, the own stream
    /// only to peers that reported a gap in it, and the reports.
    Prompt,
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
    /// Stream sequence of each `frame_handles` entry, for the packet header's
    /// range.
    frame_stamps: Vec<u64>,
    /// Live own cells and relayable cells as `(stream sequence, handle)`,
    /// ascending, rebuilt each round that has something to send. Sized to the
    /// cell store so a round never grows them.
    own_scratch: Vec<(u64, CellHandle)>,
    relay_scratch: Vec<(u64, CellHandle)>,

    // Dissemination state.
    local_zone: Zone,
    /// Relay stream sequence of each cell slot: non-zero only for a cell that
    /// arrived straight from an origin in another zone, which this node
    /// forwards into its own zone.
    relay_stamp: Box<[u64]>,
    /// Highest relay stamp issued.
    relay_head: u64,
    /// Highest own-cell `origin_sequence` issued.
    own_head: u64,
    /// Cells were stamped for relay since the last prompt round, or a peer
    /// reported a gap in our relay stream.
    relay_pending: bool,
    /// A peer reported a gap in our own stream.
    resend_pending: bool,
    /// A gap is open in some peer's stream to us.
    gaps_open: bool,
    /// Ask zone peers for their live window at the next round: set at start
    /// and when a zone member leaves before ever reaching us.
    sweep_wanted: bool,
    /// Sweep requests being answered; at most `GOSSIP_SWEEP_CONCURRENCY`.
    sweeps: Vec<Sweep>,

    // Outbound send pool.
    send_pool: Vec<PacketBuf>,
    send_pending: VecDeque<(SocketAddr, u8)>,
    send_free: Vec<u8>,

    peers: Vec<Peer>,
    /// The peer the next round serves first: where the last round stopped
    /// when the socket pushed back, so no peer is starved by its position.
    next_peer: usize,

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
        let sink_buf = DeltaSink::<C>::with_capacity(config.max_cells_per_tick);
        let frame_handles = Vec::with_capacity(config.max_cells_per_tick);
        let frame_stamps = Vec::with_capacity(config.max_cells_per_tick);
        let cell_capacity = store.capacity() as usize;
        let own_scratch = Vec::with_capacity(cell_capacity);
        let relay_scratch = Vec::with_capacity(cell_capacity);
        let relay_stamp = vec![0_u64; cell_capacity].into_boxed_slice();
        // A transport with no local address (never the case for a bound
        // socket) falls into the zone of the unspecified address, so every
        // peer reads as remote and gets a gateway rather than a direct send.
        let local_zone = Zone::of(
            transport
                .local_addr()
                .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0))),
            config.zone_prefix_len,
        );
        let local_node = store.local_identity().node_id;
        let expiration_buf = ExpirationSink::<C>::with_capacity(config.max_cells_per_tick);

        let send_pool_size = config.send_queue_capacity.max(1);
        let send_pool: Vec<PacketBuf> = (0..send_pool_size)
            .map(|_| PacketBuf::for_limits(config.wire_limits))
            .collect();
        let send_free: Vec<u8> = (0..send_pool_size as u8).rev().collect();
        let send_pending = VecDeque::with_capacity(send_pool_size);

        let mut peers: Vec<Peer> = config
            .bootstrap_peers
            .iter()
            .map(|&addr| Peer::new(addr, local_node, Zone::of(addr, config.zone_prefix_len)))
            .collect();
        refresh_gateways(&mut peers, local_zone);

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
            frame_stamps,
            own_scratch,
            relay_scratch,
            local_zone,
            relay_stamp,
            relay_head: 0,
            own_head: 0,
            relay_pending: false,
            resend_pending: false,
            gaps_open: false,
            sweep_wanted: true,
            sweeps: Vec::with_capacity(GOSSIP_SWEEP_CONCURRENCY),
            send_pool,
            send_pending,
            send_free,
            peers,
            next_peer: 0,
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
        // Paces prompt rounds: polled only while there is prompt work, and
        // its first tick is immediate, so the floor bounds the rate without
        // delaying a relay that follows a quiet spell.
        let mut prompt_tick = self.clock.ticker(self.config.relay_floor);
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
            let watch_prompt = self.relay_pending
                || self.resend_pending
                || self.gaps_open
                || self.sweep_wanted
                || !self.sweeps.is_empty();

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
                ArmOutcome::Threshold
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
                    _ = tick.tick() => ArmOutcome::Heartbeat,
                    _ = prompt_tick.tick(), if watch_prompt => ArmOutcome::Prompt,
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
                ArmOutcome::Heartbeat => self.handle_gossip_round(Round::Heartbeat)?,
                ArmOutcome::Threshold => self.handle_gossip_round(Round::Threshold)?,
                ArmOutcome::Prompt => self.handle_gossip_round(Round::Prompt)?,
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
        // The dirty rings feed no lane in this design (streams are rebuilt
        // from per-cell sequences), so keep them from filling and warning.
        self.store.clear_dirty();
        for &handle in &self.sink_buf.handles {
            if let Some(row) = self.store.get(handle) {
                self.own_head = self.own_head.max(row.origin_sequence);
            }
        }
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
        let header = summary.header;
        if header.flags & FLAG_SWEEP_REQUEST != 0 {
            self.accept_sweep(src);
            return;
        }

        self.store.merge_remote(&self.obs_buf, &mut self.sink_buf);
        self.store.clear_dirty();

        // A cell that arrives straight from an origin in another zone is one
        // this node forwards into its own zone. Cells that arrive from a
        // zone peer or in a relay stream were already delivered by their
        // sender, so they are never forwarded again (stamp 0).
        let from_origin_across_zones = header.flags & (FLAG_RELAY | FLAG_SWEEP) == 0
            && Zone::of(src, self.config.zone_prefix_len) != self.local_zone;
        let pi = self.peers.iter().position(|p| p.addr == src);
        if from_origin_across_zones {
            // Forward every cell of the origin's stream we had not seen from
            // it before, changed here or not: a cell this node already holds
            // may have reached the zone through a previous gateway, which
            // could not reach a member that joined after it, or left before
            // relaying it. A packet wholly inside what the origin sent us
            // before is a resend, and only its changes are new to the zone.
            let fresh = pi.is_some_and(|i| header.stream_upper > self.peers[i].own_in.seen());
            self.stamp_for_relay(fresh);
        } else {
            for &handle in &self.sink_buf.handles {
                if let Some(slot) = self.relay_stamp.get_mut(handle.index as usize) {
                    *slot = 0;
                }
            }
        }

        if header.flags & FLAG_SWEEP != 0 {
            // A sweep answer belongs to no stream: nothing to ack or track.
            return;
        }
        let now_ms = self.clock.now_millis();
        let Some(pi) = pi else {
            // Not (yet) in discovery: the cells are merged, but with no
            // stream state there is nothing to ack.
            return;
        };
        let identity = (NodeId(header.sender_node_id), header.sender_incarnation);
        let peer = &mut self.peers[pi];
        if peer.identity.is_some_and(|known| known != identity) {
            // A new process behind the same address: what the old one held
            // or acked is void. First contact keeps what we already sent.
            peer.reset_streams();
        }
        peer.identity = Some(identity);

        // Hearing from a gateway clears its suspicion.
        let was_suspect = std::mem::take(&mut peer.suspect);

        // What the peer holds of our two streams.
        let samples = [
            peer.own.note_ack(header.ack_own, now_ms),
            peer.relay.note_ack(header.ack_relay, now_ms),
        ];
        for sample in samples.into_iter().flatten() {
            peer.note_rtt(sample);
        }

        // A gap the peer reports in one of our streams is resent from its
        // ack at the next prompt round. Nothing in flight means the resend
        // already went out.
        if header.flags & FLAG_GAP_OWN != 0 && peer.own.sent > peer.own.acked {
            peer.own.rewind(now_ms);
            peer.resend_own = true;
            self.resend_pending = true;
        }
        if header.flags & FLAG_GAP_RELAY != 0 && peer.relay.sent > peer.relay.acked {
            peer.relay.rewind(now_ms);
            self.relay_pending = true;
        }

        // The peer's stream, closed over `(stream_lower, stream_upper]`: the
        // range is contiguous with what we hold only if it starts at or
        // below it. A packet beyond a gap (an earlier one was lost) is
        // merged but not acked past the gap, and the gap is reported once it
        // has outlived one relay floor.
        let inbound = if header.flags & FLAG_RELAY == 0 {
            &mut peer.own_in
        } else {
            &mut peer.relay_in
        };
        let hold_ms = self.config.relay_floor.as_millis() as u64;
        inbound.receive(header.stream_lower, header.stream_upper, now_ms, hold_ms);
        self.gaps_open |= inbound.gap();
        peer.ack_due |= header.cell_count > 0;

        if was_suspect {
            refresh_gateways(&mut self.peers, self.local_zone);
        }
    }

    /// Stamp the cells just merged from an origin in another zone for relay:
    /// every cell of the packet that the store holds when `everything`,
    /// otherwise only the ones the merge changed.
    fn stamp_for_relay(&mut self, everything: bool) {
        let before = self.relay_head;
        if everything {
            let obs = &self.obs_buf;
            for i in 0..obs.len() {
                let handle = self
                    .store
                    .find_rule(obs.rule_fingerprints[i])
                    .zip(
                        self.store
                            .find_node(obs.origin_node_ids[i], obs.incarnations[i]),
                    )
                    .and_then(|(rule, origin)| {
                        self.store.find(CompactCellKey {
                            rule,
                            key_hash: obs.key_hashes[i],
                            bucket: obs.buckets[i],
                            origin,
                            incarnation: obs.incarnations[i],
                        })
                    });
                if let Some(slot) = handle.and_then(|h| self.relay_stamp.get_mut(h.index as usize))
                {
                    self.relay_head += 1;
                    *slot = self.relay_head;
                }
            }
        } else {
            for &handle in &self.sink_buf.handles {
                if let Some(slot) = self.relay_stamp.get_mut(handle.index as usize) {
                    self.relay_head += 1;
                    *slot = self.relay_head;
                }
            }
        }
        self.relay_pending |= self.relay_head > before;
    }

    /// Answer a zone member's sweep request with our live window over the
    /// next prompt rounds. The requester need not be in discovery yet: a
    /// member that just started is the one that asks. A request from another
    /// zone is ignored, since cells cross zones only from origin to gateway.
    fn accept_sweep(&mut self, src: SocketAddr) {
        if Zone::of(src, self.config.zone_prefix_len) != self.local_zone
            || self.sweeps.iter().any(|s| s.dst == src)
            || self.sweeps.len() >= GOSSIP_SWEEP_CONCURRENCY
        {
            return;
        }
        self.sweeps.push(Sweep { dst: src, next: 0 });
    }

    /// Ask the top-ranked zone members for their live window, once there are
    /// any. Left wanted when the socket pushes back.
    fn request_sweep(&mut self) -> Result<(), GossipError> {
        let sources = sweep_sources(&self.peers, self.local_zone);
        if !self.sweep_wanted || sources[0].is_none() {
            return Ok(());
        }
        for pi in sources.into_iter().flatten() {
            let Some(slot) = take_send_slot(
                &self.transport,
                &self.send_pool,
                &mut self.send_pending,
                &mut self.send_free,
            )?
            else {
                return Ok(());
            };
            let header = self.header(FLAG_SWEEP_REQUEST);
            wire::encode_header_only::<C>(
                header,
                self.config.auth_key.as_ref(),
                &mut self.send_pool[slot as usize],
            );
            self.send_pending.push_back((self.peers[pi].addr, slot));
        }
        self.sweep_wanted = false;
        Ok(())
    }

    /// Send each sweep being answered its next `GOSSIP_SWEEP_CELLS_PER_ROUND`
    /// live cells, in cell-slot order, and retire the sweeps that reach the
    /// end of the store. Cells that arrive behind a sweep's position reach
    /// the requester through the streams.
    fn serve_sweeps(&mut self) -> Result<(), GossipError> {
        let budget = GOSSIP_SWEEP_CELLS_PER_ROUND.min(self.config.max_cells_per_tick);
        let mut i = 0;
        while i < self.sweeps.len() {
            let Sweep { dst, next } = self.sweeps[i];
            self.frame_handles.clear();
            self.frame_handles.extend(
                self.store
                    .active_handles()
                    .filter(|h| h.index >= next)
                    .take(budget),
            );
            let last = self.frame_handles.len() < budget;
            self.frame_stamps.clear();
            self.frame_stamps.resize(self.frame_handles.len(), 0);
            let (consumed, blocked) = if self.frame_handles.is_empty() {
                (0, false)
            } else {
                let header = self.header(FLAG_SWEEP);
                self.encode_packets_for(dst, header)?
            };
            if blocked {
                if let Some(&h) = consumed
                    .checked_sub(1)
                    .and_then(|c| self.frame_handles.get(c))
                {
                    self.sweeps[i].next = h.index + 1;
                }
                return Ok(());
            }
            if last {
                self.sweeps.swap_remove(i);
            } else {
                self.sweeps[i].next = self.frame_handles[consumed - 1].index + 1;
                i += 1;
            }
        }
        Ok(())
    }

    fn drain_one_send(&mut self) -> Result<(), GossipError> {
        let Self {
            transport,
            send_pool,
            send_pending,
            send_free,
            ..
        } = self;
        if let Some(slot) = send_queued(transport, send_pool, send_pending, send_free)? {
            send_free.push(slot);
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
                            node_id: p.identity.map(|(node_id, _)| node_id),
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
                    let zone = Zone::of(p.addr, self.config.zone_prefix_len);
                    let local = self.store.local_identity().node_id;
                    self.peers.push(Peer::new(p.addr, local, zone));
                    refresh_gateways(&mut self.peers, self.local_zone);
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.peers.len(),
                        "Peer joined the cluster.",
                    );
                }
            }
            PeerEvent::Removed(p) => {
                if let Some(pos) = self.peers.iter().position(|x| x.addr == p.addr) {
                    // A zone member is the only sender of its own cells to
                    // the rest of its zone. One that leaves before ever
                    // reaching us (it had not yet learned of us) took cells we
                    // lack, which our zone peers hold.
                    let removed = &self.peers[pos];
                    self.sweep_wanted |=
                        removed.zone == self.local_zone && removed.own_in.seen() == 0;
                    self.peers.remove(pos);
                    refresh_gateways(&mut self.peers, self.local_zone);
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.peers.len(),
                        "Peer left the cluster.",
                    );
                }
            }
        }
    }

    /// One send round. Every round sends each stream's new cells to its
    /// recipients and resends what a recipient has left unacknowledged for a
    /// retransmission timeout:
    ///
    /// - the **own** stream (this node's cells) goes to every peer in the local
    ///   zone and to one gateway in each other zone;
    /// - the **relay** stream (cells this node, as a gateway, received straight
    ///   from an origin in another zone) goes to every peer in the local zone.
    ///
    /// A `Prompt` round sends the relay stream, and the own stream only to
    /// peers that reported a gap in it. Heartbeat rounds also ack peers that
    /// sent data and received nothing from us to piggyback the ack on, and
    /// every round reports gaps that have outlived their hold.
    fn handle_gossip_round(&mut self, round: Round) -> Result<(), GossipError> {
        let now_ms = self.clock.now_millis();
        if round != Round::Prompt {
            // Step 0: bump the total-ticks counter. Tracked unconditionally
            // (even for empty / no-peer ticks) so the bench can split the
            // run's wall time into heartbeat ticks vs threshold fires.
            self.ticks_total = self.ticks_total.saturating_add(1);

            // Step 1: expire aged-out cells so we don't gossip them.
            self.expiration_buf.clear();
            self.store.expire_at(now_ms, &mut self.expiration_buf);
        } else {
            self.relay_pending = false;
        }

        if self.peers.is_empty() {
            self.resend_pending = false;
            self.gaps_open = false;
            return Ok(());
        }
        if round != Round::Prompt && !self.store.is_empty() {
            // The store was non-empty when this tick fired — bench harness
            // counts this as a "dirty tick" so effective-fanout reports
            // `packets / dirty_ticks` rather than `packets / wall_ticks`.
            self.dirty_ticks = self.dirty_ticks.saturating_add(1);
        }

        // Step 2: a stream with data in flight past its retransmission
        // timeout resumes from its last ack. A gateway that misses the
        // deadline on our own stream is suspected, and the next-ranked member
        // of its zone takes over.
        let rto_floor_ms = self.config.rto_floor.as_millis() as u64;
        let mut gateway_missed = false;
        for peer in &mut self.peers {
            let rto_ms = peer.rto_ms(rto_floor_ms);
            if round != Round::Prompt && peer.own.timed_out(now_ms, rto_ms) {
                peer.own.rewind(now_ms);
                if peer.gateway {
                    peer.suspect = true;
                    gateway_missed = true;
                }
            }
            if peer.relay.timed_out(now_ms, rto_ms) {
                peer.relay.rewind(now_ms);
            }
        }
        if gateway_missed {
            refresh_gateways(&mut self.peers, self.local_zone);
        }

        // Step 3: order each stream's candidate cells by sequence, from the
        // lowest sequence any recipient still lacks.
        let local_zone = self.local_zone;
        let own_floor = self
            .peers
            .iter()
            .filter(|p| p.zone == local_zone || p.gateway)
            .filter(|p| round != Round::Prompt || p.resend_own)
            .map(|p| p.own.sent)
            .min()
            .filter(|&sent| sent < self.own_head);
        let relay_floor = self
            .peers
            .iter()
            .filter(|p| p.zone == local_zone)
            .map(|p| p.relay.sent)
            .min()
            .filter(|&sent| sent < self.relay_head);
        self.gather_streams(own_floor, relay_floor);

        // Step 4: send, peer by peer, starting where the socket last pushed
        // back. A gap report rides on any packet to the peer, or goes alone.
        let retry_ms = 4 * self.config.relay_floor.as_millis() as u64;
        let mut recipients = 0;
        let n = self.peers.len();
        let first = self.next_peer % n;
        for i in (first..n).chain(0..first) {
            let peer = &self.peers[i];
            let in_zone = peer.zone == local_zone;
            let own_due = (in_zone || peer.gateway) && (round != Round::Prompt || peer.resend_own);
            let gaps = gap_flags(peer, now_ms);
            let mut sent = Sent::Nothing;
            if own_due {
                sent = sent.or(self.send_stream(i, StreamKind::Own, now_ms, gaps)?);
            }
            if in_zone {
                sent = sent.or(self.send_stream(i, StreamKind::Relay, now_ms, gaps)?);
            }
            if sent == Sent::Nothing
                && (gaps != 0 || (round == Round::Heartbeat && self.peers[i].ack_due))
            {
                sent = self.send_ack(i, gaps)?;
            }
            if sent == Sent::Blocked {
                self.next_peer = i;
                break;
            }
            let peer = &mut self.peers[i];
            peer.resend_own = false;
            if gaps & FLAG_GAP_OWN != 0 {
                peer.own_in.reported(now_ms, retry_ms);
            }
            if gaps & FLAG_GAP_RELAY != 0 {
                peer.relay_in.reported(now_ms, retry_ms);
            }
            recipients += usize::from(sent == Sent::Data);
        }
        self.request_sweep()?;
        self.serve_sweeps()?;
        self.resend_pending = self.peers.iter().any(|p| p.resend_own);
        self.gaps_open = self
            .peers
            .iter()
            .any(|p| p.own_in.gap() || p.relay_in.gap());
        if recipients > 0 {
            self.last_effective_fanout = recipients;
            self.peak_effective_fanout = self.peak_effective_fanout.max(recipients);
        }

        // Reset per-rule pending. The sweep treats every rule with
        // non-zero pending as "touched in expectation by the tick": a hit
        // either rode out in this round's own-stream packets or waits in a
        // stream for the next one.
        if round != Round::Prompt {
            let stamp = now_ms.max(1);
            for slot in 0..self.rule_pending.len() {
                if self.rule_pending[slot] != 0 {
                    self.rule_pending[slot] = 0;
                    self.rule_last_emit_ms[slot] = stamp;
                }
            }
        }
        Ok(())
    }

    /// Rebuild `own_scratch` / `relay_scratch`: the live cells of each stream
    /// above its floor, by ascending sequence. `None` skips a stream that has
    /// nothing new for anyone. One pass over the store serves both.
    fn gather_streams(&mut self, own_floor: Option<u64>, relay_floor: Option<u64>) {
        self.own_scratch.clear();
        self.relay_scratch.clear();
        if own_floor.is_none() && relay_floor.is_none() {
            return;
        }
        let local = self.store.local_node_slot();
        for handle in self.store.active_handles() {
            let Some(row) = self.store.get(handle) else {
                continue;
            };
            if row.key.origin == local {
                if own_floor.is_some_and(|floor| row.origin_sequence > floor) {
                    self.own_scratch.push((row.origin_sequence, handle));
                }
            } else if let Some(floor) = relay_floor {
                let stamp = self.relay_stamp[handle.index as usize];
                if stamp > floor {
                    self.relay_scratch.push((stamp, handle));
                }
            }
        }
        self.own_scratch.sort_unstable_by_key(|&(seq, _)| seq);
        self.relay_scratch.sort_unstable_by_key(|&(seq, _)| seq);
    }

    /// Send peer `pi` the cells of `stream` above what we already sent it, up
    /// to `max_cells_per_tick`.
    fn send_stream(
        &mut self,
        pi: usize,
        stream: StreamKind,
        now_ms: u64,
        gaps: u16,
    ) -> Result<Sent, GossipError> {
        let candidates = match stream {
            StreamKind::Own => &self.own_scratch,
            StreamKind::Relay => &self.relay_scratch,
        };
        let peer = &self.peers[pi];
        let out = match stream {
            StreamKind::Own => peer.own,
            StreamKind::Relay => peer.relay,
        };
        let start = candidates.partition_point(|&(seq, _)| seq <= out.sent);
        self.frame_handles.clear();
        self.frame_stamps.clear();
        for &(seq, handle) in candidates[start..]
            .iter()
            .take(self.config.max_cells_per_tick)
        {
            self.frame_handles.push(handle);
            self.frame_stamps.push(seq);
        }
        if self.frame_handles.is_empty() {
            return Ok(Sent::Nothing);
        }
        let header = self.header_for(
            pi,
            gaps | if stream == StreamKind::Relay {
                FLAG_RELAY
            } else {
                0
            },
            out.sent,
        );
        let dst = self.peers[pi].addr;
        let (consumed, blocked) = self.encode_packets_for(dst, header)?;
        let Some(&upper) = consumed
            .checked_sub(1)
            .and_then(|last| self.frame_stamps.get(last))
        else {
            return Ok(if blocked {
                Sent::Blocked
            } else {
                Sent::Nothing
            });
        };
        let peer = &mut self.peers[pi];
        match stream {
            StreamKind::Own => peer.own.note_sent(upper, now_ms),
            StreamKind::Relay => peer.relay.note_sent(upper, now_ms),
        }
        peer.ack_due = false;
        Ok(if blocked { Sent::Blocked } else { Sent::Data })
    }

    /// Send peer `pi` a header-only packet carrying our acks and the gap
    /// reports `gaps`.
    fn send_ack(&mut self, pi: usize, gaps: u16) -> Result<Sent, GossipError> {
        let Some(slot) = take_send_slot(
            &self.transport,
            &self.send_pool,
            &mut self.send_pending,
            &mut self.send_free,
        )?
        else {
            return Ok(Sent::Blocked);
        };
        let header = self.header_for(pi, gaps, 0);
        let buf = &mut self.send_pool[slot as usize];
        wire::encode_header_only::<C>(header, self.config.auth_key.as_ref(), buf);
        let peer = &mut self.peers[pi];
        peer.ack_due = false;
        self.send_pending.push_back((peer.addr, slot));
        Ok(Sent::Ack)
    }

    /// The header for a packet to peer `pi`: our identity, the acks we owe it,
    /// and the stream range starting after `stream_lower`.
    fn header_for(&self, pi: usize, flags: u16, stream_lower: u64) -> wire::Header {
        let peer = &self.peers[pi];
        wire::Header {
            ack_own: peer.own_in.have,
            ack_relay: peer.relay_in.have,
            stream_lower,
            ..self.header(flags)
        }
    }

    /// A header with our identity and `flags`, acking nothing.
    fn header(&self, flags: u16) -> wire::Header {
        wire::Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender_node_id: self.store.local_identity().node_id.0,
            sender_incarnation: self.store.local_identity().incarnation,
            flags,
            ..wire::Header::default()
        }
    }

    /// Encode `frame_handles` (stamped by `frame_stamps`) into send slots for
    /// `dst`. Returns how many handles went into queued packets, and whether
    /// the socket pushed back before all of them did; the rest wait for the
    /// next round.
    fn encode_packets_for(
        &mut self,
        dst: SocketAddr,
        header: wire::Header,
    ) -> Result<(usize, bool), GossipError> {
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
            Ok(p) => p.with_stamps(&self.frame_stamps),
            Err(_) => return Ok((0, false)),
        };

        let mut consumed = 0;
        let mut blocked = false;
        while packets.remaining() > 0 {
            let slot = match take_send_slot(
                &self.transport,
                &self.send_pool,
                &mut self.send_pending,
                &mut self.send_free,
            ) {
                Ok(Some(slot)) => slot,
                Ok(None) => {
                    // The socket pushed back: the rest of the frame waits a
                    // round.
                    packets.abandon();
                    blocked = true;
                    break;
                }
                Err(e) => {
                    packets.abandon();
                    return Err(e);
                }
            };
            let buf = &mut self.send_pool[slot as usize];
            match packets.next_into(buf) {
                Ok(Some(written)) => {
                    consumed += (written.cells_emitted + written.cells_dropped) as usize;
                    self.send_pending.push_back((dst, slot));
                }
                Ok(None) => {
                    self.send_free.push(slot);
                    break;
                }
                Err(_) => {
                    self.send_free.push(slot);
                    packets.abandon();
                    break;
                }
            }
        }
        drop(packets);
        Ok((consumed, blocked))
    }
}

/// The gap reports due to `peer`: a flag per stream of its with a gap that
/// has stayed open past its hold.
fn gap_flags(peer: &Peer, now_ms: u64) -> u16 {
    let own = if peer.own_in.report_due(now_ms) {
        FLAG_GAP_OWN
    } else {
        0
    };
    let relay = if peer.relay_in.report_due(now_ms) {
        FLAG_GAP_RELAY
    } else {
        0
    };
    own | relay
}

/// A free send slot. When every slot holds a queued packet, the oldest is
/// handed to the socket first: one round can owe a zone more packets than the
/// pool holds (two streams to each of 100 peers), and the queue exists only to
/// absorb socket backpressure. `None` when the socket pushes back.
fn take_send_slot<T: GossipTransport>(
    transport: &T,
    pool: &[PacketBuf],
    pending: &mut VecDeque<(SocketAddr, u8)>,
    free: &mut Vec<u8>,
) -> Result<Option<u8>, GossipError> {
    match free.pop() {
        Some(slot) => Ok(Some(slot)),
        None => send_queued(transport, pool, pending, free),
    }
}

/// Hand the oldest queued packet to the socket. Returns its slot once sent,
/// or `None` when nothing is queued or the socket pushes back (the packet then
/// stays at the head of the queue).
fn send_queued<T: GossipTransport>(
    transport: &T,
    pool: &[PacketBuf],
    pending: &mut VecDeque<(SocketAddr, u8)>,
    free: &mut Vec<u8>,
) -> Result<Option<u8>, GossipError> {
    let Some((dst, slot)) = pending.pop_front() else {
        return Ok(None);
    };
    match transport.try_send_to(pool[slot as usize].as_bytes(), dst) {
        Ok(_) => Ok(Some(slot)),
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
            pending.push_front((dst, slot));
            Ok(None)
        }
        Err(e) => {
            free.push(slot);
            Err(e.into())
        }
    }
}

/// What a round sent one peer.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Sent {
    Nothing,
    Ack,
    Data,
    /// The socket pushed back before the peer's packets were all queued.
    Blocked,
}

impl Sent {
    /// Combine the outcomes of two sends to one peer: a push-back dominates,
    /// then data.
    fn or(self, other: Sent) -> Sent {
        match (self, other) {
            (Sent::Blocked, _) | (_, Sent::Blocked) => Sent::Blocked,
            (Sent::Data, _) | (_, Sent::Data) => Sent::Data,
            (Sent::Ack, _) | (_, Sent::Ack) => Sent::Ack,
            (Sent::Nothing, Sent::Nothing) => Sent::Nothing,
        }
    }
}

enum ArmOutcome {
    Request(Option<Request>),
    Inbound(io::Result<(usize, SocketAddr)>),
    Writable,
    PeerEvent(Option<PeerEvent>),
    Admin(Option<AdminCommand>),
    Heartbeat,
    Threshold,
    Prompt,
}

/// Read one admin command. Only invoked when `admin_rx.is_some()` — the
/// `select!` guard ensures the unwrap is safe.
async fn recv_admin(rx: &mut Option<mpsc::Receiver<AdminCommand>>) -> Option<AdminCommand> {
    rx.as_mut()
        .expect("admin arm gated by admin_rx.is_some()")
        .recv()
        .await
}
