//! Single-threaded gossip event loop.
//!
//! One `tokio::select!` drives the arms: limit/shutdown requests, inbound
//! UDP, outbound writable, peer membership churn, admin, and three tickers
//! (expiry housekeeping, hit sync, owner broadcast). After each iteration the
//! runtime calls [`AggregateStore::apply`] once with whatever rows the arm
//! produced. The runtime owns the [`CellStore`], pre-allocated scratch
//! buffers, the outbound send pool, the membership table and the delivery
//! queues; nothing in the loop allocates after construction.
//!
//! Every `(rule, key)` has one owner, chosen by rendezvous hash over the
//! discovered membership. A hit is recorded locally at once and queued for
//! the owner; every `sync_wait` the node sends its unacknowledged cells to
//! their owners, which fold them, acknowledge them, and every
//! `broadcast_interval` broadcast the changed cells to every peer. See the
//! "Design notes" in `crates/gabion/README.md`.

use std::cell::Cell;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::crdt::{
    CellHandle, CellStore, Count, DeltaSink, ExpirationSink, NodeId, Observation, ObservationBatch,
};
use crate::discovery::PeerEvent;
use crate::wire::{self, FrameKind, PacketBuf, Packets, WireScratch};

use super::admin::{AdminCommand, AdminSnapshot, PeerEntry};
use super::client::{GossipClient, LimitRequest, Request};
#[cfg(feature = "transport-udp")]
use super::clock::TokioClock;
use super::clock::{Clock, Ticker};
use super::membership::{Added, Membership, Owner, Token};
use super::outbox::Outbox;
use super::send_queue::SendQueue;
use super::store::AggregateStore;
use super::transport::GossipTransport;
#[cfg(feature = "transport-udp")]
use super::transport::UdpTransport;
use super::{GossipConfig, GossipError};

/// Where [`GossipRuntime::send_frame`] sends a frame.
#[derive(Clone, Copy)]
enum Audience {
    One(SocketAddr),
    EveryPeer,
}

/// What a local cell needs next, given its owner and what the owner holds.
#[derive(Clone, Copy)]
enum Route<C> {
    /// Nothing: the owner holds it, or the cell has expired.
    Settled,
    /// This node owns it; queue it for broadcast at this count.
    Broadcast(C),
    /// Send it to the peer at this index of the membership table.
    Send(usize),
}

/// Counts of work dropped because a bounded structure was full, so each
/// warning can fire on power-of-two totals only.
#[derive(Default)]
struct Overflows {
    sync_queue: u64,
    broadcast_queue: u64,
    peer_table: u64,
}

impl Overflows {
    fn sync_queue_full(&mut self, capacity: usize) {
        self.sync_queue = self.sync_queue.saturating_add(1);
        if self.sync_queue.is_power_of_two() {
            tracing::warn!(
                overflow_total = self.sync_queue,
                capacity,
                config_key = "storage.local_dirty_capacity",
                "Local gossip dirty ring overflowed. Hits for some keys cannot be queued for \
                 delivery to their owners, so peers will under-count requests handled here \
                 (requests were still allowed). Raise `storage.local_dirty_capacity` (currently \
                 {capacity}) or reduce write/cardinality pressure.",
            );
        }
    }

    fn broadcast_queue_full(&mut self, capacity: usize) {
        self.broadcast_queue = self.broadcast_queue.saturating_add(1);
        if self.broadcast_queue.is_power_of_two() {
            tracing::warn!(
                overflow_total = self.broadcast_queue,
                capacity,
                config_key = "storage.forwarded_dirty_capacity",
                "Forwarded gossip dirty ring overflowed. Changed counts for keys this node owns \
                 cannot be queued for broadcast, so peers will under-count them until the keys \
                 next change (requests were still allowed). Raise \
                 `storage.forwarded_dirty_capacity` (currently {capacity}) or reduce \
                 write/cardinality pressure.",
            );
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
    /// `(peer index, cell)` rows of one sync, sorted to group by owner.
    sync_batch: Vec<(usize, CellHandle)>,
    encode_buf: PacketBuf,

    membership: Membership,
    outbox: Outbox<C>,
    send_queue: SendQueue,
    /// Set when membership changed: the next sync re-evaluates the owner of
    /// every local cell, so hits an old owner never acknowledged reach the
    /// new one.
    requeue_local_cells: bool,

    // Pending reply for the in-flight limit request. Sent after the
    // bottom-of-loop apply so callers observe "ack ⇒ AggregateStore reflects
    // the increment". Sized to one because only one limit request per
    // iteration is processed.
    pending_reply: Option<oneshot::Sender<()>>,

    overflows: Overflows,

    // Observability counters surfaced through `AdminSnapshot`. None of them
    // feed back into the hot path.
    decode_reject_count: u64,
    ticks_total: u64,
    dirty_ticks: u64,
    last_effective_fanout: usize,
    peak_effective_fanout: usize,

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
    /// runtime polls the admin arm only when `admin_rx.is_some()`.
    pub fn from_parts_with_admin(
        transport: T,
        clock: K,
        config: GossipConfig,
        store: CellStore<C>,
        aggregates: S,
        admin_rx: Option<mpsc::Receiver<AdminCommand>>,
    ) -> (Self, GossipClient<C>) {
        let (req_tx, req_rx) = mpsc::channel(config.limit_queue_capacity);

        let sizing = store.config();
        let frame_cells = config.max_cells_per_tick.max(config.broadcast_batch_limit);
        let batch_cells = frame_cells.max(config.wire_limits.max_cells as usize);
        let recv_buf = vec![0u8; config.wire_limits.max_payload_bytes].into_boxed_slice();
        let scratch = WireScratch::for_store(&store);
        let obs_buf = ObservationBatch::<C>::with_capacity(batch_cells);
        let sink_buf = DeltaSink::<C>::with_capacity(batch_cells);
        let expiration_buf = ExpirationSink::<C>::with_capacity(config.max_cells_per_tick);
        // A broadcast gathers every live cell of each changed key, each at
        // most once, so the store's capacity bounds it.
        let frame_handles = Vec::with_capacity(batch_cells.max(store.capacity() as usize));
        let sync_batch = Vec::with_capacity(config.max_cells_per_tick);
        let encode_buf = PacketBuf::for_limits(config.wire_limits);

        let mut membership = Membership::new(sizing.peer_capacity.into());
        for addr in config.bootstrap_peers.iter().copied() {
            membership.add(addr);
        }
        let outbox = Outbox::new(
            store.capacity() as usize,
            sizing.local_dirty_capacity,
            sizing.forwarded_dirty_capacity,
        );
        let send_queue = SendQueue::new(config.send_queue_capacity, config.wire_limits);

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
            sync_batch,
            encode_buf,
            membership,
            outbox,
            send_queue,
            requeue_local_cells: false,
            pending_reply: None,
            overflows: Overflows::default(),
            decode_reject_count: 0,
            ticks_total: 0,
            dirty_ticks: 0,
            last_effective_fanout: 0,
            peak_effective_fanout: 0,
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
    /// peer-membership arm; pass `futures::stream::empty()` if discovery is
    /// not yet wired up.
    ///
    /// Owners are computed from peers' discovered addresses, so the
    /// transport's local address must be the address peers discover this
    /// node by.
    pub async fn run<St>(mut self, peer_events: St) -> Result<(), GossipError>
    where
        St: Stream<Item = PeerEvent>,
    {
        self.membership.set_local(self.transport.local_addr()?);
        let mut peer_events = std::pin::pin!(peer_events);
        let mut housekeeping = self.clock.ticker(self.config.tick_interval);
        let mut sync_tick = self.clock.ticker(self.config.sync_wait);
        let mut broadcast_tick = self.clock.ticker(self.config.broadcast_interval);
        // Once the peer-event stream returns `None` (e.g. `stream::empty()`),
        // never poll it again — otherwise it returns `Ready(None)` on every
        // iteration and the loop spins.
        let mut peer_events_done = false;

        loop {
            // Decide outside the `select!` whether the send arm has work to do.
            // Reading `self.send_queue` inside the `if` guard would re-borrow
            // `self` after we've already split-borrowed for the I/O futures
            // below.
            let have_send = !self.send_queue.is_empty();
            let watch_peers = !peer_events_done;
            let watch_admin = self.admin_rx.is_some();

            // Split-borrow scope: we hand the I/O futures non-overlapping
            // pieces of `self` so they compose inside `select!`.
            let outcome = {
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
                    _ = sync_tick.tick() => ArmOutcome::SyncTick,
                    _ = broadcast_tick.tick() => ArmOutcome::BroadcastTick,
                    _ = housekeeping.tick() => ArmOutcome::Housekeeping,
                }
            };

            match outcome {
                ArmOutcome::Request(None) | ArmOutcome::Request(Some(Request::Shutdown)) => {
                    return Ok(());
                }
                ArmOutcome::Request(Some(Request::Limit(r))) => self.handle_limit_request(r),
                ArmOutcome::Inbound(Ok((n, src))) => self.handle_inbound(n, src)?,
                ArmOutcome::Inbound(Err(e)) => return Err(e.into()),
                ArmOutcome::Writable => self.send_queue.drain_one(&self.transport)?,
                ArmOutcome::PeerEvent(Some(evt)) => self.handle_peer_event(evt),
                ArmOutcome::PeerEvent(None) => peer_events_done = true,
                ArmOutcome::Admin(Some(cmd)) => self.handle_admin_command(cmd),
                ArmOutcome::Admin(None) => self.admin_rx = None,
                ArmOutcome::SyncTick => self.handle_sync_tick()?,
                ArmOutcome::BroadcastTick => self.handle_broadcast_tick()?,
                ArmOutcome::Housekeeping => self.handle_housekeeping_tick(),
            }

            // One apply per iteration covering both row sources. CRDT-free
            // arms (peer event, drain, idle ticks) leave both empty; the
            // branch short-circuits then.
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
        if let Some(&handle) = self.sink_buf.handles.first() {
            self.queue_local_cell(handle);
        }
        // Reply is sent at the bottom of this loop iteration, AFTER the
        // post-iteration apply runs. Caller observes "ack ⇒ AggregateStore
        // reflects the increment".
        self.pending_reply = req.reply;
    }

    fn handle_inbound(&mut self, n: usize, src: SocketAddr) -> Result<(), GossipError> {
        self.obs_buf.clear();
        self.sink_buf.clear();
        let kind = Cell::new(FrameKind::Broadcast);
        let decoded = {
            let Self {
                recv_buf,
                config,
                membership,
                obs_buf,
                ..
            } = &mut *self;
            let bytes = &recv_buf[..n];
            let accept = |header: &wire::Header| {
                kind.set(FrameKind::from_flags(header.flags).unwrap_or(FrameKind::Broadcast));
                true
            };
            let mut on_cell = |cell: wire::WireCell<C>| {
                // A sync belongs to the owner of its keys. A node that
                // believes another member owns a key neither folds nor
                // acknowledges its hits; the sender re-sends them until its
                // own view of the membership agrees.
                if kind.get() == FrameKind::Sync
                    && membership.owner(cell.rule_fingerprint, cell.key_hash) != Owner::Local
                {
                    return;
                }
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
            match config.auth_key.as_ref() {
                Some(key) => wire::decode_auth_visit::<C>(
                    bytes,
                    key,
                    config.wire_limits,
                    accept,
                    &mut on_cell,
                ),
                None => {
                    wire::decode_unauth_visit::<C>(bytes, config.wire_limits, accept, &mut on_cell)
                }
            }
        };
        let summary = match decoded {
            Ok(summary) => summary,
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
                return Ok(());
            }
        };
        self.membership
            .learn_node_id(src, NodeId(summary.header.sender_node_id));

        match kind.get() {
            // The owner's totals: fold them, relay nothing.
            FrameKind::Broadcast => self.store.merge_remote(&self.obs_buf, &mut self.sink_buf),
            // Hits for keys this node owns: fold them, broadcast whatever
            // changed, and acknowledge every cell so the sender stops
            // re-sending it.
            FrameKind::Sync => {
                self.store.merge_remote(&self.obs_buf, &mut self.sink_buf);
                let capacity = self.store.config().forwarded_dirty_capacity;
                for &handle in &self.sink_buf.handles {
                    if self.outbox.queue_broadcast(handle).is_err() {
                        self.overflows.broadcast_queue_full(capacity);
                    }
                }
                self.frame_handles.clear();
                for row in 0..self.obs_buf.len() {
                    if let Some(handle) = self.store.find_observed(&self.obs_buf, row) {
                        self.frame_handles.push(handle);
                    }
                }
                if !self.frame_handles.is_empty() {
                    self.send_frame(FrameKind::Ack, Audience::One(src))?;
                }
            }
            // An owner holds these cells of ours.
            FrameKind::Ack => {
                let owner = Token::of(src);
                for row in 0..self.obs_buf.len() {
                    if let Some(handle) = self.store.find_observed(&self.obs_buf, row) {
                        self.outbox
                            .delivered(handle, self.obs_buf.counts[row], owner);
                    }
                }
            }
        }
        Ok(())
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
            PeerEvent::Added(p) => match self.membership.add(p.addr) {
                Added::Joined => {
                    self.requeue_local_cells = true;
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.membership.peers().len() + 1,
                        "Peer joined the cluster.",
                    );
                }
                Added::Known => {}
                Added::Full => {
                    self.overflows.peer_table = self.overflows.peer_table.saturating_add(1);
                    if self.overflows.peer_table.is_power_of_two() {
                        tracing::warn!(
                            peer = %p.addr,
                            rejected_total = self.overflows.peer_table,
                            capacity = self.membership.capacity(),
                            config_key = "storage.peer_capacity",
                            "A new peer was discovered but this node cannot track more peers. \
                             Keys this node would have owned or sent to that peer are \
                             assigned as if it did not exist, so nodes that see it will \
                             disagree with this one about key owners and counts will \
                             under-count until they agree (requests were still allowed). Raise \
                             `storage.peer_capacity` (currently {}) above the cluster size.",
                            self.membership.capacity(),
                        );
                    }
                }
            },
            PeerEvent::Removed(p) => {
                if self.membership.remove(p.addr) {
                    self.requeue_local_cells = true;
                    tracing::info!(
                        peer = %p.addr,
                        cluster_size = self.membership.peers().len() + 1,
                        "Peer left the cluster.",
                    );
                }
            }
        }
    }

    /// Expire cells whose bucket has left its rule's window.
    fn handle_housekeeping_tick(&mut self) {
        self.expiration_buf.clear();
        self.store
            .expire_at(self.clock.now_millis(), &mut self.expiration_buf);
        for &handle in &self.expiration_buf.handles {
            self.outbox.forget(handle);
        }
    }

    /// Send every local cell an owner has not acknowledged to that owner,
    /// one batch per owner. A cell stays queued, and is re-sent on the next
    /// sync, until its owner acknowledges it or it expires.
    fn handle_sync_tick(&mut self) -> Result<(), GossipError> {
        self.ticks_total = self.ticks_total.saturating_add(1);
        self.collect_sync_batch();
        if self.requeue_local_cells {
            // Membership changed or the queue overflowed. Entries the first
            // pass settled have made room, so sweep every local cell in and
            // gather the batch again.
            self.requeue_local_cells = false;
            self.queue_all_local_cells();
            self.collect_sync_batch();
        }
        if self.sync_batch.is_empty() {
            return Ok(());
        }
        self.dirty_ticks = self.dirty_ticks.saturating_add(1);

        self.sync_batch
            .sort_unstable_by_key(|&(peer, handle)| (peer, handle.index));
        let mut start = 0;
        while start < self.sync_batch.len() {
            let peer = self.sync_batch[start].0;
            self.frame_handles.clear();
            let mut end = start;
            while end < self.sync_batch.len() && self.sync_batch[end].0 == peer {
                self.frame_handles.push(self.sync_batch[end].1);
                end += 1;
            }
            let owner = self.membership.peers()[peer].addr;
            self.send_frame(FrameKind::Sync, Audience::One(owner))?;
            start = end;
        }
        Ok(())
    }

    /// Fill `sync_batch` with the queued cells that still owe their owner
    /// something, dropping those that no longer do.
    fn collect_sync_batch(&mut self) {
        self.sync_batch.clear();
        let mut i = 0;
        while i < self.outbox.sync_len() && self.sync_batch.len() < self.config.max_cells_per_tick {
            let handle = self.outbox.sync_at(i);
            match self.route(handle) {
                Route::Settled => self.outbox.unqueue_sync(i),
                Route::Broadcast(count) => {
                    self.hand_to_broadcast(handle, count);
                    self.outbox.unqueue_sync(i);
                }
                Route::Send(peer) => {
                    self.sync_batch.push((peer, handle));
                    i += 1;
                }
            }
        }
    }

    /// Broadcast the whole state of the keys this node owns that changed
    /// since the last broadcast, at most `broadcast_batch_limit` keys, to
    /// every peer. A key's state is every live cell the node holds for it,
    /// so a peer that missed an earlier broadcast catches up on the next
    /// change to the key.
    fn handle_broadcast_tick(&mut self) -> Result<(), GossipError> {
        self.ticks_total = self.ticks_total.saturating_add(1);
        self.frame_handles.clear();
        self.outbox.begin_broadcast_round();
        let mut keys = 0;
        while keys < self.config.broadcast_batch_limit {
            let Some(handle) = self.outbox.pop_broadcast() else {
                break;
            };
            if self.outbox.is_gathered(handle) {
                continue;
            }
            // Only the current owner broadcasts a key. A cell whose owner has
            // since moved is dropped here: its new owner gets the hits from
            // their contributors and broadcasts them.
            if !matches!(self.owner_of(handle), Some(Owner::Local)) {
                continue;
            }
            let Some(row) = self.store.get(handle) else {
                continue;
            };
            keys += 1;
            for cell in self.store.key_cells(row.key.rule, row.key.key_hash) {
                self.outbox.mark_gathered(cell);
                self.frame_handles.push(cell);
            }
        }
        if self.frame_handles.is_empty() || self.membership.peers().is_empty() {
            return Ok(());
        }
        self.dirty_ticks = self.dirty_ticks.saturating_add(1);
        self.last_effective_fanout = self.membership.peers().len();
        self.peak_effective_fanout = self.peak_effective_fanout.max(self.last_effective_fanout);
        self.send_frame(FrameKind::Broadcast, Audience::EveryPeer)
    }

    // -- ownership and delivery ----------------------------------------------

    fn owner_of(&self, handle: CellHandle) -> Option<Owner> {
        let row = self.store.get(handle)?;
        let rule = self.store.rule_dictionary().descriptor(row.key.rule)?;
        Some(self.membership.owner(rule.fingerprint, row.key.key_hash))
    }

    fn route(&self, handle: CellHandle) -> Route<C> {
        let Some(row) = self.store.get(handle) else {
            return Route::Settled;
        };
        let Some(owner) = self.owner_of(handle) else {
            return Route::Settled;
        };
        let (token, route) = match owner {
            Owner::Local => (self.membership.local_token(), Route::Broadcast(row.count)),
            Owner::Peer(peer) => (self.membership.peers()[peer].token, Route::Send(peer)),
        };
        if self.outbox.is_delivered(handle, row.count, token) {
            Route::Settled
        } else {
            route
        }
    }

    /// A local hit changed `handle`: broadcast it if this node owns the key,
    /// otherwise queue it for the next sync to the owner.
    fn queue_local_cell(&mut self, handle: CellHandle) {
        match self.route(handle) {
            Route::Settled => {}
            Route::Broadcast(count) => self.hand_to_broadcast(handle, count),
            Route::Send(_) => self.queue_sync(handle),
        }
    }

    /// Queue `handle` for the next sync. If the queue is full the cell is
    /// left to the next sweep of every local cell, so its owner still hears
    /// of it once the queue has drained.
    fn queue_sync(&mut self, handle: CellHandle) {
        if self.outbox.queue_sync(handle).is_err() {
            self.requeue_local_cells = true;
            self.overflows
                .sync_queue_full(self.store.config().local_dirty_capacity);
        }
    }

    fn hand_to_broadcast(&mut self, handle: CellHandle, count: C) {
        if self.outbox.queue_broadcast(handle).is_err() {
            self.overflows
                .broadcast_queue_full(self.store.config().forwarded_dirty_capacity);
            return;
        }
        self.outbox
            .delivered(handle, count, self.membership.local_token());
    }

    /// Queue every live local cell its owner does not yet hold, as the
    /// membership now stands.
    fn queue_all_local_cells(&mut self) {
        let local = self.store.local_node_slot();
        for handle in self.store.active_handles() {
            let owes = self
                .store
                .get(handle)
                .is_some_and(|row| row.key.origin == local)
                && !matches!(self.route(handle), Route::Settled);
            if owes && self.outbox.queue_sync(handle).is_err() {
                self.requeue_local_cells = true;
                self.overflows
                    .sync_queue_full(self.store.config().local_dirty_capacity);
            }
        }
    }

    // -- sending
    // ---------------------------------------------------------------

    /// Encode `self.frame_handles` as packets of `kind` and offer each to
    /// `audience`. The packet is encoded once however many peers receive it.
    fn send_frame(&mut self, kind: FrameKind, audience: Audience) -> Result<(), GossipError> {
        let identity = self.store.local_identity();
        let header = wire::Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender_node_id: identity.node_id.0,
            sender_incarnation: identity.incarnation,
            flags: kind.flags(),
            ..wire::Header::default()
        };
        let Self {
            config,
            store,
            scratch,
            frame_handles,
            encode_buf,
            membership,
            send_queue,
            transport,
            ..
        } = self;
        let mut packets = match config.auth_key.as_ref() {
            Some(key) => Packets::<C>::auth(
                header,
                store,
                frame_handles,
                key,
                scratch,
                config.wire_limits,
            )?,
            None => {
                Packets::<C>::unauth(header, store, frame_handles, scratch, config.wire_limits)?
            }
        };
        while packets.next_into(encode_buf)?.is_some() {
            match audience {
                Audience::One(dst) => send_queue.transmit(transport, encode_buf, dst)?,
                Audience::EveryPeer => {
                    for peer in membership.peers() {
                        send_queue.transmit(transport, encode_buf, peer.addr)?;
                    }
                }
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
                        .membership
                        .peers()
                        .iter()
                        .map(|p| PeerEntry {
                            addr: p.addr,
                            node_id: p.node_id,
                        })
                        .collect(),
                    store_stats: self.store.stats(),
                    local_dirty_len: self.outbox.sync_len() as u32,
                    forwarded_dirty_len: self.outbox.broadcast_len() as u32,
                    send_pending_depth: self.send_queue.depth(),
                    decode_reject_count: self.decode_reject_count,
                    max_send_pending_depth: self.send_queue.max_depth(),
                    ticks_total: self.ticks_total,
                    threshold_fires: 0,
                    dirty_ticks: self.dirty_ticks,
                    last_effective_fanout: self.last_effective_fanout,
                    peak_effective_fanout: self.peak_effective_fanout,
                    last_error_budget: 0,
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
}

enum ArmOutcome {
    Request(Option<Request>),
    Inbound(io::Result<(usize, SocketAddr)>),
    Writable,
    PeerEvent(Option<PeerEvent>),
    Admin(Option<AdminCommand>),
    SyncTick,
    BroadcastTick,
    Housekeeping,
}

/// Read one admin command. Only invoked when `admin_rx.is_some()` — the
/// `select!` guard ensures the unwrap is safe.
async fn recv_admin(rx: &mut Option<mpsc::Receiver<AdminCommand>>) -> Option<AdminCommand> {
    rx.as_mut()
        .expect("admin arm gated by admin_rx.is_some()")
        .recv()
        .await
}
