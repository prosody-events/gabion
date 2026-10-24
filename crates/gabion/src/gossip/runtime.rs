//! Single-threaded gossip event loop.
//!
//! One `tokio::select!` drives five arms — limit/shutdown requests, inbound
//! UDP, outbound writable, peer membership churn, gossip tick. After each
//! iteration the runtime calls [`AggregateStore::apply`] once with whatever
//! rows the arm produced. The runtime owns the [`CellStore`], pre-allocated
//! scratch buffers, the outbound send pool, and the peer table; nothing in
//! the loop allocates after construction.
//!
//! Replication is Scuttlebutt push-pull anti-entropy. A tick sends a peer a
//! digest (the watermark per origin). The peer answers with the cells the
//! digest lacks and a request naming the origins the digest is ahead on, with
//! its own watermark for each, which the initiator answers the same way.
//! Cells move only in response to a digest.

use std::cmp::Reverse;
use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::crdt::{
    CellHandle, CellStore, Count, DeltaSink, ExpirationSink, NodeId, NodeSlot, Observation,
    ObservationBatch,
};
use crate::discovery::PeerEvent;
use crate::wire::{self, DecodeError, DigestEntry, DigestRange, PacketBuf, Packets, WireScratch};

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

/// One origin this node owes cells to a peer: everything above `after`.
/// `backlog` orders origins for the reply when the cell budget binds.
#[derive(Clone, Copy)]
struct Owed {
    origin: NodeSlot,
    after: u64,
    backlog: u64,
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

    // Digest scratch. `digest_in` holds the frame being answered,
    // `digest_out` the frame being built, `origin_order` the origins a digest
    // is cut from, `owed` the origins a reply draws cells from. The two
    // `digest_*` columns map a node slot to the entry the peer listed for it,
    // valid when `digest_marks[slot] == digest_epoch`.
    digest_in: Vec<DigestEntry>,
    digest_out: Vec<DigestEntry>,
    origin_order: Vec<NodeSlot>,
    owed: Vec<Owed>,
    digest_marks: Box<[u32]>,
    digest_versions: Box<[u64]>,
    digest_epoch: u32,

    // Outbound send pool.
    send_pool: Vec<PacketBuf>,
    send_pending: VecDeque<(SocketAddr, u8)>,
    send_free: Vec<u8>,

    peers: Vec<SocketAddr>,
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
    // - `pending` — local hits per `(rule, key)` not yet carried by a gossip reply. Sized to the
    //   cell store, which bounds the keys that can be in flight between emits. Cleared per emitted
    //   cell by `clear_emitted_pending`.
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
        let mut store = store;
        store.seed_local_version(clock.now_millis().saturating_mul(1_000));

        let recv_buf = vec![0u8; config.wire_limits.max_payload_bytes].into_boxed_slice();
        let scratch = WireScratch::for_store(&store);
        let obs_buf = ObservationBatch::<C>::with_capacity(config.max_cells_per_tick);
        let sink_buf = DeltaSink::<C>::with_capacity(config.max_cells_per_tick);
        let frame_handles = Vec::with_capacity(config.max_cells_per_tick);
        let expiration_buf = ExpirationSink::<C>::with_capacity(config.max_cells_per_tick);

        let send_pool_size = config.send_queue_capacity.max(1);
        let send_pool: Vec<PacketBuf> = (0..send_pool_size)
            .map(|_| PacketBuf::for_limits(config.wire_limits))
            .collect();
        let send_free: Vec<u8> = (0..send_pool_size as u8).rev().collect();
        let send_pending = VecDeque::with_capacity(send_pool_size);

        let peers = config.bootstrap_peers.clone();

        let node_capacity = store.node_dictionary().capacity() as usize;
        let digest_capacity =
            wire::max_digest_entries(config.wire_limits, config.auth_key.is_some());
        let rng = SplitMix64::new(config.rng_seed);

        let rule_capacity = store.rule_dictionary().capacity() as usize;
        let pending = PendingHits::with_capacity(store.capacity() as usize);
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
            digest_in: Vec::with_capacity(digest_capacity),
            digest_out: Vec::with_capacity(digest_capacity),
            origin_order: Vec::with_capacity(node_capacity),
            owed: Vec::with_capacity(node_capacity),
            digest_marks: vec![0; node_capacity].into_boxed_slice(),
            digest_versions: vec![0; node_capacity].into_boxed_slice(),
            digest_epoch: 0,
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
            origin_sequence: 0,
            claim_gap: 0,
        });
        self.store.ingest_local(&self.obs_buf, &mut self.sink_buf);
        // Reply is sent at the bottom of this loop iteration, AFTER the
        // post-iteration apply runs. Caller observes "ack ⇒ AggregateStore
        // reflects the increment".
        self.pending_reply = req.reply;
    }

    fn handle_inbound(&mut self, n: usize, src: SocketAddr) {
        match self.decode_inbound(n) {
            Ok(Inbound::Cells) => {
                let now_millis = self.clock.now_millis();
                self.store
                    .merge_remote(&self.obs_buf, now_millis, &mut self.sink_buf);
            }
            Ok(Inbound::Digest { range, syn }) => {
                // The request goes first: it is small, and the cells below can
                // use up the send pool.
                if syn {
                    self.send_request(src, range);
                }
                self.serve_digest(src, syn.then_some(range));
            }
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
            }
        }
    }

    /// Decode the datagram in `recv_buf[..n]` into `obs_buf` (cells) or
    /// `digest_in` (digest).
    fn decode_inbound(&mut self, n: usize) -> Result<Inbound, DecodeError> {
        self.obs_buf.clear();
        self.sink_buf.clear();
        self.digest_in.clear();
        let frame = wire::open(
            &self.recv_buf[..n],
            self.config.auth_key.as_ref(),
            self.config.wire_limits,
        )?;
        if frame.is_digest() {
            if frame.header.cell_count as usize > self.digest_in.capacity() {
                return Err(DecodeError::CapacityExceeded);
            }
            let digest_in = &mut self.digest_in;
            let range = frame.visit_digest(|entry| digest_in.push(entry))?;
            return Ok(Inbound::Digest {
                range,
                syn: frame.is_syn(),
            });
        }
        let obs_buf = &mut self.obs_buf;
        frame.visit_cells::<C>(|cell| obs_buf.push(wire::observation_from_wire(cell)))?;
        Ok(Inbound::Cells)
    }

    /// Answer the digest in `digest_in` with the cells it lacks. For a SYN,
    /// `range` is the slice it covers, and origins in it the digest does not
    /// list are answered from version `0`; a request (`None`) is owed only the
    /// origins it lists. Origins are served deepest backlog first
    /// (Scuttlebutt's "scuttle-depth" order), each oldest version first, until
    /// the per-reply cell budget runs out, so what one origin receives is
    /// always a prefix of what it owes.
    fn serve_digest(&mut self, dst: SocketAddr, range: Option<DigestRange>) {
        self.digest_epoch = self.digest_epoch.wrapping_add(1);
        if self.digest_epoch == 0 {
            self.digest_marks.fill(0);
            self.digest_epoch = 1;
        }
        for entry in &self.digest_in {
            if let Some(slot) = self.store.find_node(entry.node_id, entry.incarnation) {
                self.digest_marks[slot as usize] = self.digest_epoch;
                self.digest_versions[slot as usize] = entry.version;
            }
        }

        self.owed.clear();
        for slot in 0..self.store.node_dictionary().capacity() {
            let Some(origin) = self.store.node_dictionary().descriptor(slot) else {
                continue;
            };
            let after = if self.digest_marks[slot as usize] == self.digest_epoch {
                self.digest_versions[slot as usize]
            } else if range.is_some_and(|r| r.contains(origin.node_id)) {
                0
            } else {
                continue;
            };
            let newest = self.store.newest_version(slot);
            if newest > after {
                self.owed.push(Owed {
                    origin: slot,
                    after,
                    backlog: newest - after,
                });
            }
        }
        self.owed
            .sort_unstable_by_key(|o| (Reverse(o.backlog), o.origin));

        self.frame_handles.clear();
        for o in &self.owed {
            if self.frame_handles.len() >= self.config.max_cells_per_tick {
                break;
            }
            self.store.collect_newer(
                o.origin,
                o.after,
                self.config.max_cells_per_tick,
                &mut self.frame_handles,
            );
        }
        if !self.frame_handles.is_empty() && self.encode_packets_for(dst) {
            self.clear_emitted_pending(self.clock.now_millis().max(1));
        }
    }

    /// Answer the SYN frame in `digest_in`, which covers `range`, with a
    /// request: this node's watermark for each origin the SYN is ahead on,
    /// `0` for an origin this node does not know (Cassandra's
    /// `GossipDigestAck`). An origin the SYN is not ahead on has nothing for
    /// this node, so a full digest would only repeat what the SYN's sender
    /// can already work out. The request never outgrows the SYN frame it
    /// answers, so it is one frame, and none at all when nothing is asked.
    fn send_request(&mut self, dst: SocketAddr, range: DigestRange) {
        self.digest_out.clear();
        for entry in &self.digest_in {
            let held = self
                .store
                .find_node(entry.node_id, entry.incarnation)
                .map_or(0, |slot| self.store.watermark(slot));
            if entry.version > held {
                self.digest_out.push(DigestEntry {
                    version: held,
                    ..*entry
                });
            }
        }
        if !self.digest_out.is_empty() {
            self.queue_digest_frame(dst, false, range);
        }
    }

    /// Open an exchange with `dst`: send this node's watermark for every
    /// origin it knows, as one or more SYN frames whose ranges tile the
    /// node-id space. With no origins it still sends one frame: it tells the
    /// receiver this node holds nothing. Every frame is sent even when the
    /// peer is already current, because the digest is how the peer learns
    /// that.
    fn send_digest(&mut self, dst: SocketAddr) {
        let dictionary = self.store.node_dictionary();
        self.origin_order.clear();
        self.origin_order.extend(
            (0..dictionary.capacity()).filter(|&slot| dictionary.descriptor(slot).is_some()),
        );
        self.origin_order.sort_unstable_by_key(|&slot| {
            dictionary
                .descriptor(slot)
                .map_or((0, 0), |o| (o.node_id.0, o.incarnation))
        });

        let per_frame = self.digest_out.capacity().max(1);
        let frames = self.origin_order.len().div_ceil(per_frame).max(1);
        for frame in 0..frames {
            let dictionary = self.store.node_dictionary();
            let start = (frame * per_frame).min(self.origin_order.len());
            let end = (start + per_frame).min(self.origin_order.len());
            self.digest_out.clear();
            for &slot in &self.origin_order[start..end] {
                if let Some(origin) = dictionary.descriptor(slot) {
                    self.digest_out.push(DigestEntry {
                        node_id: origin.node_id,
                        incarnation: origin.incarnation,
                        version: self.store.watermark(slot),
                    });
                }
            }
            // The frames' ranges tile the node-id space: each ends just before
            // the next frame's first origin, so an origin the sender lacks
            // falls inside exactly one frame. Two incarnations of one node id
            // can straddle a cut, in which case neighbouring ranges share that
            // id.
            let first = self.digest_out.first().map(|e| e.node_id.0);
            let last = self.digest_out.last().map(|e| e.node_id.0);
            let next_first = self
                .origin_order
                .get((frame + 1) * per_frame)
                .and_then(|&slot| dictionary.descriptor(slot))
                .map(|o| o.node_id.0);
            let frame_range = DigestRange {
                lo: if frame == 0 {
                    DigestRange::ALL.lo
                } else {
                    first.unwrap_or(DigestRange::ALL.lo)
                },
                hi: match (next_first, last) {
                    (Some(next), Some(last)) => next.saturating_sub(1).max(last),
                    _ => DigestRange::ALL.hi,
                },
            };

            if !self.queue_digest_frame(dst, true, frame_range) {
                return;
            }
        }
    }

    /// Queue `digest_out` as one digest frame covering `range` for `dst`.
    /// Returns `false` when the send pool is exhausted or the frame does not
    /// encode.
    fn queue_digest_frame(&mut self, dst: SocketAddr, syn: bool, range: DigestRange) -> bool {
        let Some(slot) = self.send_free.pop() else {
            return false;
        };
        let header = self.node_header();
        let buf = &mut self.send_pool[slot as usize];
        match wire::encode_digest(
            header,
            syn,
            range,
            &self.digest_out,
            self.config.auth_key.as_ref(),
            self.config.wire_limits,
            buf,
        ) {
            Ok(()) => {
                self.send_pending.push_back((dst, slot));
                true
            }
            Err(_) => {
                self.send_free.push(slot);
                false
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
                    peers: self.peers.iter().map(|&addr| PeerEntry { addr }).collect(),
                    store_stats: self.store.stats(),
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
                if !self.peers.contains(&p.addr) {
                    self.peers.push(p.addr);
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.peers.len(),
                        "Peer joined the cluster.",
                    );
                }
            }
            PeerEvent::Removed(p) => {
                if let Some(pos) = self.peers.iter().position(|&addr| addr == p.addr) {
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
        // Tracked unconditionally so the bench can split the run's ticks
        // into heartbeat ticks vs threshold fires.
        self.ticks_total = self.ticks_total.saturating_add(1);

        // Expire aged-out cells so the digest and the replies drawn from it
        // describe live state only.
        let now_millis = self.clock.now_millis();
        self.expiration_buf.clear();
        self.store.expire_at(now_millis, &mut self.expiration_buf);

        if self.peers.is_empty() || self.send_free.is_empty() {
            return;
        }
        self.dirty_ticks = self.dirty_ticks.saturating_add(1);

        // Open `fanout` exchanges, each with a distinct random peer: a
        // partial Fisher-Yates shuffle of `self.peers` in place (`self.peers`
        // has no ordering contract elsewhere). The early return above
        // guarantees n >= 1.
        let n = self.peers.len();
        let pick_count = self.config.fanout.clamp(1, n);
        self.last_effective_fanout = pick_count;
        self.peak_effective_fanout = self.peak_effective_fanout.max(pick_count);
        for i in 0..pick_count {
            let j = i + (self.rng.next_u64() as usize) % (n - i);
            self.peers.swap(i, j);
        }
        for i in 0..pick_count {
            self.send_digest(self.peers[i]);
        }
    }

    /// Zero the pending count of every local cell in the reply just encoded
    /// and stamp its rule's `rule_last_emit_ms`, so the `min_emit_interval`
    /// floor measures from this emit. A key whose cell did not fit in the
    /// reply keeps its count and can fire again on its next hit. Clearing on
    /// send, not on ack, means the N × ε_R bound holds in expectation under
    /// UDP loss; the next exchange resends what was lost. Only called once
    /// the whole reply is queued for sending: a reply cut short leaves its
    /// counts.
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

    fn node_header(&self) -> wire::Header {
        wire::Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender_node_id: self.store.local_identity().node_id.0,
            sender_incarnation: self.store.local_identity().incarnation,
            count_width: 0,
            cell_count: 0,
            body_len: 0,
            min_origin_sequence: 0,
            max_origin_sequence: 0,
            flags: 0,
        }
    }

    /// Queue the packets of `frame_handles` for `dst`. Returns whether the
    /// whole reply was queued, so the caller clears pending only for cells
    /// that will go out.
    fn encode_packets_for(&mut self, dst: SocketAddr) -> bool {
        let header = self.node_header();

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

/// What one inbound datagram turned out to be.
enum Inbound {
    /// Cells, decoded into `obs_buf`.
    Cells,
    /// A digest, decoded into `digest_in`.
    Digest { range: DigestRange, syn: bool },
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
