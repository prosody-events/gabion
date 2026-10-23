//! Single-threaded gossip event loop, local-only reference arm.
//!
//! This runtime never sends a datagram. One `tokio::select!` drives four
//! arms — limit/shutdown requests, inbound UDP (read and discarded), peer
//! membership churn, and the expiry tick — plus the optional admin arm. After
//! each iteration the runtime calls [`AggregateStore::apply`] once with the
//! rows the arm produced, so every node's aggregate holds only its own hits.
//! The runtime owns the [`CellStore`] and pre-allocated scratch buffers;
//! nothing in the loop allocates after construction.

use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;

use futures::{Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};

use crate::crdt::{
    CellStore, Count, DeltaSink, ExpirationSink, NodeId, Observation, ObservationBatch,
};
use crate::discovery::PeerEvent;

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
    obs_buf: ObservationBatch<C>,
    sink_buf: DeltaSink<C>,
    expiration_buf: ExpirationSink<C>,

    /// Membership as announced by discovery. Kept only so the admin snapshot
    /// can list it; nothing is ever sent to these addresses.
    peers: Vec<SocketAddr>,

    // Pending reply for the in-flight limit request. Sent after the
    // bottom-of-loop apply so callers observe "ack ⇒ AggregateStore reflects
    // the increment". Sized to one because only one limit request per
    // iteration is processed.
    pending_reply: Option<oneshot::Sender<()>>,

    // Expiry ticks observed, surfaced through `AdminSnapshot`.
    ticks_total: u64,

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
        let obs_buf = ObservationBatch::<C>::with_capacity(config.max_cells_per_tick);
        let sink_buf = DeltaSink::<C>::with_capacity(config.max_cells_per_tick);
        let expiration_buf = ExpirationSink::<C>::with_capacity(config.max_cells_per_tick);
        let peers = config.bootstrap_peers.clone();

        let runtime = Self {
            config,
            store,
            transport,
            clock,
            requests_rx: req_rx,
            admin_rx,
            aggregates,
            recv_buf,
            obs_buf,
            sink_buf,
            expiration_buf,
            peers,
            pending_reply: None,
            ticks_total: 0,
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
                // Local-only: peers' counts are never merged, so inbound
                // datagrams are read off the socket and dropped.
                ArmOutcome::Inbound(Ok(_)) => {}
                ArmOutcome::Inbound(Err(e)) => return Err(e.into()),
                ArmOutcome::PeerEvent(Some(evt)) => self.handle_peer_event(evt),
                ArmOutcome::PeerEvent(None) => peer_events_done = true,
                ArmOutcome::Admin(Some(cmd)) => self.handle_admin_command(cmd),
                ArmOutcome::Admin(None) => self.admin_rx = None,
                ArmOutcome::Tick => self.handle_expiry_tick(),
            }

            // One apply per iteration. CRDT-free arms leave both sinks
            // empty; the branch short-circuits then.
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
        // Reply is sent at the bottom of this loop iteration, AFTER the
        // post-iteration apply runs. Caller observes "ack ⇒ AggregateStore
        // reflects the increment".
        self.pending_reply = req.reply;
    }

    fn handle_admin_command(&mut self, cmd: AdminCommand) {
        match cmd {
            AdminCommand::Snapshot { reply } => {
                // Nothing is ever queued, forwarded, rejected or sent, so the
                // gossip-side counters are constant zero.
                let snapshot = AdminSnapshot {
                    local_identity: self.store.local_identity(),
                    peers: self
                        .peers
                        .iter()
                        .map(|&addr| PeerEntry {
                            addr,
                            node_id: None,
                            peer_slot: None,
                        })
                        .collect(),
                    store_stats: self.store.stats(),
                    local_dirty_len: self.store.local_dirty().len(),
                    forwarded_dirty_len: self.store.forwarded_dirty().len(),
                    send_pending_depth: 0,
                    decode_reject_count: 0,
                    max_send_pending_depth: 0,
                    ticks_total: self.ticks_total,
                    threshold_fires: 0,
                    dirty_ticks: 0,
                    last_effective_fanout: 0,
                    peak_effective_fanout: 0,
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
                if let Some(pos) = self.peers.iter().position(|&x| x == p.addr) {
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

    /// Age out expired cells so the local aggregate does not hold them past
    /// their window. The only periodic work the local-only runtime does.
    fn handle_expiry_tick(&mut self) {
        self.ticks_total = self.ticks_total.saturating_add(1);
        self.expiration_buf.clear();
        self.store
            .expire_at(self.clock.now_millis(), &mut self.expiration_buf);
    }
}

enum ArmOutcome {
    Request(Option<Request>),
    Inbound(io::Result<(usize, SocketAddr)>),
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
