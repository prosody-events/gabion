//! The single long-lived simulation engine.
//!
//! There is no new simulator here: [`run_engine`] stands up N
//! [`GossipRuntime`]s on one shared [`SimRouter`] under paused virtual time,
//! then loops over a command channel, replying on a `oneshot` per command. The
//! same function backs both faces of the visualizer: playback (`step`) and
//! live interaction (`submit_request`, `set_link_policy`).
//!
//! **Spawn once, drive forever.** `run_engine` owns the `Vec<NodeHandle>` and
//! the `LocalSet` for the whole session; the runtimes are `spawn_local`'d once
//! and driven by advancing virtual time inside command handlers. The runtimes
//! are `!Send`, which is fine — the engine is single-threaded everywhere
//! (one `LocalSet` natively, the single wasm thread in the browser).
//!
//! The caller must run `run_engine` on a runtime with **paused time** (native:
//! `Builder::new_current_thread().enable_all().start_paused(true)`); virtual
//! time only moves when a `Step` / `StepTo` handler calls `sim_advance`.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU128;
use std::rc::Rc;
use std::time::Duration;

use futures::channel::mpsc::{UnboundedSender, unbounded};
use gabion::crdt::{CellStore, KeyHash, NodeId, NodeIdentity, RuleDescriptor, ValidStorageConfig};
use gabion::discovery::{Peer, PeerEvent};
use gabion::gossip::sim::{LinkPolicy, SimRouter};
use gabion::gossip::{
    AdminCommand, AdminSnapshot, CellDumpSnapshot, GossipClient, GossipCounters, GossipError,
    GossipRuntime, PeerZone, Role,
};
use gabion::rules::RuleTable;
use gabion::settings::{SettingError, Settings};
use gabion::zone::{ZoneHash, ZoneName};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, LocalSet};

use crate::clock::{ManualClock, SharedNow, Tickers};
use crate::config::{MAX_NODES, MAX_ZONES, SimConfig, ValidSimConfig, ZONE_NAMES, ZoneMode};
use crate::event::{
    BytesByTier, CellView, ClusterState, Event, EventBatch, EventKind, Inspected, NodeState,
    PeerView, ZoneView,
};
use crate::shims::{
    AddressBook, EventEmittingAggregateStore, EventLog, LoggingSimTransport, Member,
};

/// First UDP port the engine assigns; node `i` binds `BASE_PORT + i`.
const BASE_PORT: u16 = 40_000;
/// The last stable id with a port of its own, `BASE_PORT + MAX_ID = 65535`.
const MAX_ID: u32 = (u16::MAX - BASE_PORT) as u32;
/// Cluster id mixed into every packet header (matches the bench). Non-zero,
/// checked when the constant is evaluated.
pub(crate) const CLUSTER_ID_HASH: NonZeroU128 = NonZeroU128::new(0xC1).unwrap();

/// Each node's inbound queue: two packets from each of the other
/// `MAX_NODES − 1` nodes, 510 at 256 nodes, for every node in one zone. A
/// heartbeat sends each peer one own-stream frame, 33 cells to a packet at the
/// default payload, so two packets carry a frame of up to 66 cells; the
/// visualizer's one watched key leaves each node at most `live_buckets + 1`
/// own cells (11 at the default window). A step that hands a node more (a
/// window over 65 buckets, or many keys through the engine API) overflows the
/// queue: the logging transport reports the excess as `PacketDropped`, and
/// each stream resends it after one heartbeat.
pub(crate) const VIZ_ROUTER_CAPACITY: usize = 2 * (MAX_NODES - 1);

/// Per-(src, dst) link policy in a serde-friendly shape. Maps onto the
/// simulator's [`LinkPolicy`].
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(tag = "kind")]
pub enum LinkPolicyKind {
    Pass,
    Block,
    DropFirst { count: u32 },
    DropProb { p: f64 },
}

impl From<LinkPolicyKind> for LinkPolicy {
    fn from(value: LinkPolicyKind) -> Self {
        match value {
            LinkPolicyKind::Pass => LinkPolicy::Pass,
            LinkPolicyKind::Block => LinkPolicy::Block,
            LinkPolicyKind::DropFirst { count } => LinkPolicy::DropFirst { count },
            LinkPolicyKind::DropProb { p } => LinkPolicy::DropProb { p },
        }
    }
}

/// Things the engine cannot do, surfaced back to the caller.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(
        "node {id} is not a member of this cluster — it never joined, or it has already left. \
         Node ids are stable and are never reused, so a removed node's id stays gone. Pick an id \
         from the current cluster (the live ids appear on the stage and in the Cluster control)."
    )]
    UnknownNode { id: u32 },
    #[error(
        "a node cannot join zone {zone}: this cluster has {zones} zones, indexed from 0 (zone-a). \
         Pass an index below {zones}, or rebuild with more zones."
    )]
    UnknownZone { zone: u32, zones: u8 },
    #[error(
        "no node can join: the cluster already has {MAX_NODES} live nodes, the most the \
         visualizer simulates, and every node's peer table and cell store is sized for that many. \
         Remove a node first."
    )]
    ClusterFull,
    #[error(
        "no node can join: this session has handed out all {count} node ids, and ids are never \
         reused, so every join spends one. Reset or reload the page to start a new session.",
        count = MAX_ID + 1
    )]
    IdsExhausted,
    #[error(
        "the gossip runtime for node {node} has stopped, so the request could not be recorded. \
         The simulation has ended; reload to start over."
    )]
    Gossip {
        node: u32,
        #[source]
        source: GossipError,
    },
    /// A node's settings, refused as `Settings::validate` refuses them for
    /// both adapters: a joiner whose peers overflow `gossip.peer_capacity`.
    #[error(transparent)]
    Setting(#[from] SettingError),
}

/// A command sent into the engine. Each carries a `oneshot` the engine replies
/// on once it has done the work; the reply ordering across commands is FIFO
/// because the engine processes one at a time.
pub enum Command {
    /// Inject `hits` for `key` at node `node`, at the current virtual time.
    SubmitRequest {
        node: u32,
        key: u128,
        hits: u64,
        reply: oneshot::Sender<Result<EventBatch, EngineError>>,
    },
    /// Install a directed link policy between two nodes.
    SetLinkPolicy {
        src: u32,
        dst: u32,
        policy: LinkPolicyKind,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    /// Restore every directed link between live nodes to lossless `Pass` —
    /// undo any partition or per-link drop. Engine-driven (it owns the live
    /// membership) so it stays correct as nodes join and leave.
    Heal { reply: oneshot::Sender<()> },
    /// Spawn a fresh cold-start member that bootstraps knowing every current
    /// node. The newcomer takes the next never-reused id; survivors are told it
    /// joined so they gossip to it. Replies with the events the join produced,
    /// [`EngineError::UnknownZone`] for a zone the cluster does not have,
    /// [`EngineError::ClusterFull`] or [`EngineError::IdsExhausted`] when no
    /// node can join, or [`EngineError::Setting`] if the newcomer's settings
    /// fail to validate.
    AddNode {
        /// The zone to join; `None` joins the zone with the fewest live
        /// members, the lowest index on a tie. A `u32`, so that an index past
        /// 255 is refused rather than wrapped onto a zone; wasm-bindgen still
        /// truncates a JS number to 32 bits on the way in.
        zone: Option<u32>,
        reply: oneshot::Sender<Result<EventBatch, EngineError>>,
    },
    /// Remove the node with stable id `id`: tell every survivor it left, stop
    /// its runtime, and unbind its transport. Replies with the events the
    /// removal produced, or [`EngineError::UnknownNode`] if no such live node.
    RemoveNode {
        id: u32,
        reply: oneshot::Sender<Result<EventBatch, EngineError>>,
    },
    /// Advance virtual time by `delta_ms`, collecting the events produced.
    Step {
        delta_ms: u64,
        reply: oneshot::Sender<EventBatch>,
    },
    /// Advance virtual time to absolute `virtual_ms` (no-op if already past).
    StepTo {
        virtual_ms: u64,
        reply: oneshot::Sender<EventBatch>,
    },
    /// Per-node cluster snapshot, with the cells and peer rows of node
    /// `inspect` alone.
    Snapshot {
        inspect: Option<u32>,
        reply: oneshot::Sender<ClusterState>,
    },
    /// End the session and shut every runtime down.
    Shutdown,
}

/// Spawn the engine and process commands until the channel closes or a
/// `Shutdown` arrives.
pub async fn run_engine(
    valid: ValidSimConfig,
    cmd_rx: mpsc::Receiver<Command>,
) -> Result<(), EngineError> {
    let local = LocalSet::new();
    local.run_until(engine_loop(valid, cmd_rx)).await
}

async fn engine_loop(
    valid: ValidSimConfig,
    mut cmd_rx: mpsc::Receiver<Command>,
) -> Result<(), EngineError> {
    let mut state = EngineState::build(valid)?;
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            Command::SubmitRequest {
                node,
                key,
                hits,
                reply,
            } => {
                let _ = reply.send(state.submit_request(node, key, hits).await);
            }
            Command::SetLinkPolicy {
                src,
                dst,
                policy,
                reply,
            } => {
                let _ = reply.send(state.set_link_policy(src, dst, policy));
            }
            Command::Heal { reply } => {
                state.heal();
                let _ = reply.send(());
            }
            Command::AddNode { zone, reply } => {
                let _ = reply.send(state.add_node(zone).await);
            }
            Command::RemoveNode { id, reply } => {
                let _ = reply.send(state.remove_node(id).await);
            }
            Command::Step { delta_ms, reply } => {
                let _ = reply.send(state.step(delta_ms).await);
            }
            Command::StepTo { virtual_ms, reply } => {
                let delta = virtual_ms.saturating_sub(state.virtual_ms);
                let _ = reply.send(state.step(delta).await);
            }
            Command::Snapshot { inspect, reply } => {
                let _ = reply.send(state.snapshot(inspect).await);
            }
            Command::Shutdown => break,
        }
    }
    state.shutdown().await;
    Ok(())
}

/// One node's handles, owned by the engine for the session. The `Vec` of these
/// stays insertion-ordered (append on add, `remove` on delete), so a handle's
/// position in the `Vec` is the node's **rank** — what the frontend turns into
/// a ring slot — while `id` is its stable identity. The two are equal only
/// until the first removal opens a gap.
struct NodeHandle {
    /// Stable id (see [`crate::event`]). Assigned once, never reused.
    id: u32,
    /// The zone it is placed in.
    zone: u8,
    /// This node's gossip address, `127.0.0.1:(BASE_PORT + id)`. Cached so a
    /// removal can unbind it without recomputing.
    addr: SocketAddr,
    client: GossipClient,
    aggregate: Rc<EventEmittingAggregateStore>,
    admin_tx: mpsc::Sender<AdminCommand>,
    /// Fires this node's heartbeat and prompt tickers, and shows whether the
    /// prompt ticker is armed.
    tickers: Tickers,
    /// The discovery stream this node's runtime watches. The engine pushes
    /// `PeerEvent::Added`/`Removed` here as other nodes join and leave, so
    /// membership is dynamic rather than the static `stream::empty()` the bench
    /// uses. An `UnboundedReceiver` *is* a `Stream`, which is what `run` wants.
    peer_tx: UnboundedSender<PeerEvent>,
    join: JoinHandle<Result<(), GossipError>>,
    identity: NodeIdentity,
    /// This runtime's counters as last read, for the round and role diffs.
    seen: GossipCounters,
    /// Its gateway into each zone as last read.
    gateways: [Option<u32>; MAX_ZONES],
}

/// All engine state, owned as a local inside [`engine_loop`] (no `Rc<RefCell>`
/// wrapper — the task owns it outright).
struct EngineState {
    config: SimConfig,
    /// Every node's settings, before its gossip bind and static peers.
    settings: Settings,
    /// Every node's storage, sized by [`SimConfig::settings`].
    storage: ValidStorageConfig,
    /// The watched rule, which every node interns at construction.
    rules: RuleTable,
    /// That rule's descriptor: the bucket epoch the submission path stamps
    /// and the window the snapshot reports.
    rule: RuleDescriptor,
    /// Every zone's name, parsed and hashed once.
    zone_names: Box<[ZoneName]>,
    router: SimRouter,
    nodes: Vec<NodeHandle>,
    addresses: AddressBook,
    /// Reverse map: gossip `NodeId` (the `u128` on the wire) → stable id, for
    /// resolving a cell's origin identity back to the node that owns it. An
    /// entry is removed when its node leaves.
    node_id_index: HashMap<u128, u32>,
    /// The next stable id to hand out. Monotonic and never reset, so ids are
    /// never reused across joins and leaves — a departed node's id stays gone,
    /// and addresses / gossip `NodeId`s derived from it never collide.
    next_id: u32,
    log: EventLog,
    /// Shared virtual time every node reads. The engine moves it and fires
    /// gossip ticks per node, so the runtimes never touch `tokio::time` (which
    /// panics on wasm at `std::time::Instant::now`).
    now: SharedNow,
    /// Virtual time elapsed since the session began, in milliseconds.
    virtual_ms: u64,
    /// Sub-steps `step` has taken: one per heartbeat in a quiet cluster.
    substeps: u64,
    /// Bytes every sender put on the wire, drained from the log.
    bytes_by_tier: BytesByTier,
    /// Which nodes' prompt tickers are armed at the start of a sub-step;
    /// reserved once so no sub-step allocates it.
    armed: Vec<bool>,
    /// Admin snapshots the gateway diff has fetched: one per node whose roles
    /// moved.
    gateway_fetches: u64,
}

impl EngineState {
    fn build(
        ValidSimConfig {
            config,
            settings,
            storage,
            rules,
            rule,
            zone_names,
        }: ValidSimConfig,
    ) -> Result<Self, EngineError> {
        let log: EventLog = Rc::new(RefCell::new(Vec::new()));
        let addresses: AddressBook = Rc::new(RefCell::new(HashMap::new()));
        // One shared time base for the whole cluster; each node gets its own
        // tick channel so a fired tick queues rather than coalescing.
        let now: SharedNow = Rc::new(Cell::new(0));

        let initial_nodes = config.nodes;
        let mut state = Self {
            config,
            settings,
            storage,
            rules,
            rule,
            zone_names,
            router: SimRouter::with_channel_capacity(VIZ_ROUTER_CAPACITY),
            nodes: Vec::with_capacity(initial_nodes),
            addresses,
            node_id_index: HashMap::with_capacity(initial_nodes),
            next_id: 0,
            log,
            now,
            virtual_ms: 0,
            substeps: 0,
            bytes_by_tier: BytesByTier::default(),
            armed: Vec::with_capacity(MAX_NODES),
            gateway_fetches: 0,
        };

        // The initial members are ids 0..N, so the address of id `i` is
        // `BASE_PORT + i` — the same set every member bootstraps against, and
        // the set a later newcomer will be handed too.
        let addrs: Vec<SocketAddr> = (0..initial_nodes).map(|i| addr_of_id(i as u32)).collect();
        let zones = state.zone_names.len();
        for _ in 0..initial_nodes {
            let id = state.next_id;
            state.next_id += 1;
            state.spawn_node(id, (id as usize % zones) as u8, &addrs)?;
        }

        // Apply the uniform i.i.d. loss model to every directed link.
        if state.config.uniform_loss > 0.0 {
            let p = state.config.uniform_loss;
            for src in &addrs {
                for dst in &addrs {
                    if src != dst {
                        state
                            .router
                            .set_link_policy(*src, *dst, LinkPolicy::DropProb { p });
                    }
                }
            }
        }

        Ok(state)
    }

    /// Build, spawn, and register one node with stable id `id`, bootstrapped
    /// against `bootstrap` (its own address is filtered out if present). The
    /// single node-construction path: both [`EngineState::build`] and
    /// [`EngineState::add_node`] route through here.
    ///
    /// Every node shares the storage config and settings `SimConfig::validate`
    /// built once, so a joiner is sized like the initial members; its gossip
    /// config adds only its bind and peers, through `Settings::validate`.
    fn spawn_node(
        &mut self,
        id: u32,
        zone: u8,
        bootstrap: &[SocketAddr],
    ) -> Result<(), SettingError> {
        let addr = addr_of_id(id);
        let peers: Box<[SocketAddr]> = bootstrap.iter().copied().filter(|a| *a != addr).collect();
        let (gossip_cfg, _) = Settings {
            // A bind with no static peers would mean Kubernetes discovery: a
            // node with no peers yet is alone until the engine reports one.
            gossip_bind: (!peers.is_empty()).then_some(addr),
            bootstrap_peers: peers,
            zone: match self.config.zone_mode {
                ZoneMode::Configured => Some(self.zone_names[usize::from(zone)].clone()),
                ZoneMode::Unset => None,
            },
            ..self.settings.clone()
        }
        .validate()?;
        let identity = NodeIdentity::new(NodeId((id as u128) * 0x100 + 1), 1);

        let store = CellStore::new(self.storage, identity, &self.rules);

        let transport = LoggingSimTransport::new(
            self.router.bind(addr),
            self.router.clone(),
            id,
            zone,
            self.addresses.clone(),
            self.log.clone(),
        );

        let aggregate = Rc::new(EventEmittingAggregateStore::new(id, self.log.clone()));
        let (admin_tx, admin_rx) = mpsc::channel::<AdminCommand>(1);
        let heartbeat = Duration::from_millis(self.config.tick_interval_ms);
        let (clock, tickers) = ManualClock::new(self.now.clone(), heartbeat);
        let (runtime, client) = GossipRuntime::from_parts_with_admin(
            transport,
            clock,
            gossip_cfg,
            store,
            aggregate.clone(),
            Some(admin_rx),
        );
        // The discovery stream: an `UnboundedReceiver` is already a
        // `Stream<Item = PeerEvent>`, so it drops straight into `run` where the
        // bench passes `stream::empty()`. The engine keeps `peer_tx` to feed
        // this node membership changes as others join and leave.
        let (peer_tx, peer_rx) = unbounded::<PeerEvent>();
        let join = tokio::task::spawn_local(runtime.run(peer_rx));

        self.node_id_index.insert(identity.node_id.0, id);
        self.addresses
            .borrow_mut()
            .insert(addr, Member { id, zone });
        self.nodes.push(NodeHandle {
            id,
            zone,
            addr,
            client,
            aggregate,
            admin_tx,
            tickers,
            peer_tx,
            join,
            identity,
            seen: GossipCounters::default(),
            gateways: [None; MAX_ZONES],
        });
        Ok(())
    }

    fn current_tick(&self) -> u64 {
        self.virtual_ms / self.config.tick_interval_ms
    }

    /// Position in the `nodes` `Vec` of the live node with stable id `id`,
    /// or `None` if no live node has it. The `Vec` is small (≤ `MAX_NODES`)
    /// and rarely scanned (only on a targeted command), so a linear search is
    /// simpler than maintaining a second id→position map that would have to
    /// stay in sync across every add and remove.
    fn position_of(&self, id: u32) -> Option<usize> {
        self.nodes.iter().position(|h| h.id == id)
    }

    async fn submit_request(
        &mut self,
        node: u32,
        key: u128,
        hits: u64,
    ) -> Result<EventBatch, EngineError> {
        let handle = self
            .position_of(node)
            .map(|pos| &self.nodes[pos])
            .ok_or(EngineError::UnknownNode { id: node })?;
        let bucket = self.rule.current_epoch(self.virtual_ms);
        handle
            .client
            .record(
                self.rule.fingerprint,
                KeyHash(key),
                bucket,
                hits,
                self.rule.limit,
                self.virtual_ms,
            )
            .await
            .map_err(|source| EngineError::Gossip { node, source })?;

        // `record` returns only after the aggregate `apply` ran, so the cell
        // events are already buffered. Drain and stamp them at the current
        // virtual time.
        let mut events = Vec::new();
        self.drain_log(&mut events);
        Ok(EventBatch {
            events,
            virtual_ms: self.virtual_ms,
            tick: self.current_tick(),
        })
    }

    fn set_link_policy(
        &mut self,
        src: u32,
        dst: u32,
        policy: LinkPolicyKind,
    ) -> Result<(), EngineError> {
        for id in [src, dst] {
            if self.position_of(id).is_none() {
                return Err(EngineError::UnknownNode { id });
            }
        }
        self.router
            .set_link_policy(addr_of_id(src), addr_of_id(dst), policy.into());
        Ok(())
    }

    /// Restore every directed link between live nodes to lossless `Pass`.
    /// Sweeps the live address set (not a stale `0..N`), so it stays correct
    /// after churn; `set_link_policy(Pass)` also clears any drop counter.
    fn heal(&mut self) {
        let addrs: Vec<SocketAddr> = self.nodes.iter().map(|h| h.addr).collect();
        for &src in &addrs {
            for &dst in &addrs {
                if src != dst {
                    self.router.set_link_policy(src, dst, LinkPolicy::Pass);
                }
            }
        }
    }

    /// Spawn a fresh cold-start member. It takes the next never-reused id and
    /// bootstraps knowing every current node; each survivor is told it joined
    /// (via that survivor's discovery stream) so gossip flows both ways. The
    /// newcomer starts with an empty store and catches up by anti-entropy. It
    /// joins `zone`, or the zone with the fewest live members.
    async fn add_node(&mut self, zone: Option<u32>) -> Result<EventBatch, EngineError> {
        if self.nodes.len() >= MAX_NODES {
            return Err(EngineError::ClusterFull);
        }
        if self.next_id > MAX_ID {
            return Err(EngineError::IdsExhausted);
        }
        let zones = self.zone_names.len();
        let zone = match zone {
            None => self.smallest_zone(),
            Some(zone) => u8::try_from(zone)
                .ok()
                .filter(|&z| usize::from(z) < zones)
                .ok_or(EngineError::UnknownZone {
                    zone,
                    zones: zones as u8,
                })?,
        };
        let id = self.next_id;
        self.next_id += 1;
        let newcomer_addr = addr_of_id(id);
        // The newcomer bootstraps against everyone currently live.
        let live_addrs: Vec<SocketAddr> = self.nodes.iter().map(|h| h.addr).collect();
        self.spawn_node(id, zone, &live_addrs)?;

        // Tell every survivor (everyone but the just-pushed newcomer) the
        // newcomer joined, so they add it to their peer table and gossip to it.
        let joined = PeerEvent::Added(Peer::new(newcomer_addr));
        for handle in &self.nodes {
            if handle.id != id {
                let _ = handle.peer_tx.unbounded_send(joined.clone());
            }
        }

        // Let the newcomer bind and the survivors register the join before we
        // return, so the immediate snapshot already reflects the new member.
        self.drain_pending().await;
        let mut events = Vec::new();
        self.drain_log(&mut events);
        Ok(EventBatch {
            events,
            virtual_ms: self.virtual_ms,
            tick: self.current_tick(),
        })
    }

    /// Remove the live node with stable id `id`. Symmetric with
    /// [`EngineState::add_node`]: tell every survivor it left, stop its
    /// runtime, unbind its transport, and drop every trace of it. Its id is
    /// never reused, so survivors keep their own ids and addresses unchanged.
    /// Every survivor whose gateway it was reports that gateway's zone anew.
    async fn remove_node(&mut self, id: u32) -> Result<EventBatch, EngineError> {
        let pos = self
            .position_of(id)
            .ok_or(EngineError::UnknownNode { id })?;
        let addr = self.nodes[pos].addr;
        let node_id = self.nodes[pos].identity.node_id.0;

        // Tell every *other* node it left first, so they drop it from their
        // peer tables and stop gossiping to it.
        let left = PeerEvent::Removed(Peer::new(addr));
        for (i, handle) in self.nodes.iter().enumerate() {
            if i != pos {
                let _ = handle.peer_tx.unbounded_send(left.clone());
            }
        }

        // Stop its runtime: aborting drops the task's transport, which drops
        // its router receiver, so survivors' in-flight sends to it now hit the
        // UDP floor. Then unbind so no dead sender lingers in the router.
        self.nodes[pos].join.abort();
        self.router.unbind(addr);

        // Drop every trace: the handle (closing its client / admin / tick /
        // peer channels), the address-book entry, and the id reverse-map entry.
        let handle = self.nodes.remove(pos);
        drop(handle);
        self.addresses.borrow_mut().remove(&addr);
        self.node_id_index.remove(&node_id);

        // Let survivors process the departure and the aborted task fully drop.
        self.drain_pending().await;
        let mut events = Vec::new();
        self.drain_log(&mut events);
        // Removing a row moves no role, so the role-count diff misses the
        // departed gateway whenever no row left in its zone can replace it
        // (none there, or none heard yet): diff every survivor that used it.
        for pos in 0..self.nodes.len() {
            if self.nodes[pos].gateways.contains(&Some(id)) {
                self.diff_gateways(pos, &mut events).await;
            }
        }
        Ok(EventBatch {
            events,
            virtual_ms: self.virtual_ms,
            tick: self.current_tick(),
        })
    }

    /// Live members in each zone, by zone index.
    fn live_by_zone(&self) -> [u32; MAX_ZONES] {
        let mut live = [0; MAX_ZONES];
        for node in &self.nodes {
            live[usize::from(node.zone)] += 1;
        }
        live
    }

    /// The zone with the fewest live members, the lowest index on a tie.
    fn smallest_zone(&self) -> u8 {
        let live = self.live_by_zone();
        let zones = self.zone_names.len();
        (0..zones)
            .min_by_key(|&zone| (live[zone], zone))
            .map_or(0, |zone| zone as u8)
    }

    async fn step(&mut self, delta_ms: u64) -> EventBatch {
        let tick_ms = self.config.tick_interval_ms;
        let floor_ms = self.config.min_emit_interval_ms;
        let target = self.virtual_ms.saturating_add(delta_ms);
        let mut events = Vec::new();

        // Advance event by event: to the next heartbeat, or one relay floor
        // when any node's prompt ticker is armed (it has resends, gap
        // reports, probe answers or sweeps waiting), whichever is sooner. At
        // each sub-step the bucket clock moves, the heartbeat fires on every
        // node if it is due, and the prompt ticker fires on every node armed
        // before the step, so a relay leaves one floor after it is owed, as in
        // production, and a quiet cluster costs one sub-step per heartbeat.
        //
        // Delivery is deferred across the sub-step: a sim send buffers its
        // packet, the first drain lets every fired node run its round with
        // nothing yet delivered, `release_deferred` delivers the sub-step's
        // packets and the second drain lets receivers merge them. Their
        // ticks are already consumed, so a freshly merged count goes out no
        // earlier than the next sub-step. `submit` and churn paths never arm
        // deferral.
        //
        // After each sub-step the engine reads every runtime's counters, emits
        // its rounds, and fetches a node's snapshot only when its role count
        // moved.
        loop {
            let t = self.virtual_ms;
            let heartbeat = (t / tick_ms + 1).saturating_mul(tick_ms);
            self.armed.clear();
            self.armed
                .extend(self.nodes.iter().map(|n| n.tickers.armed.get()));
            let next = if self.armed.contains(&true) {
                heartbeat.min(t + floor_ms)
            } else {
                heartbeat
            };
            if next > target {
                break;
            }
            self.virtual_ms = next;
            self.now.set(next);
            self.substeps += 1;
            self.router.begin_deferred();
            self.fire(next, next == heartbeat).await;
            self.drain_pending().await;
            self.router.release_deferred();
            self.drain_pending().await;
            self.drain_log(&mut events);
            self.observe(&mut events).await;
        }

        // A remainder short of the next event moves the clock without firing
        // anything, so `virtual_ms` and the bucket clock still reach exactly
        // `target`.
        if self.virtual_ms < target {
            self.virtual_ms = target;
            self.now.set(target);
            self.drain_log(&mut events);
        }

        EventBatch {
            events,
            virtual_ms: self.virtual_ms,
            tick: self.current_tick(),
        }
    }

    /// Fire, in node order, every heartbeat when `heartbeat` is set and the
    /// prompt ticker of every node `self.armed` marks, each for the instant
    /// `at`. Each send queues in that node's bounded inbox, so a busy runtime
    /// still consumes the tick on its next idle poll rather than dropping it. A
    /// closed inbox (the runtime stopped) is ignored.
    async fn fire(&self, at: u64, heartbeat: bool) {
        for (node, &armed) in self.nodes.iter().zip(&self.armed) {
            if heartbeat {
                let _ = node.tickers.heartbeat.send(at).await;
            }
            if armed {
                let _ = node.tickers.prompt.send(at).await;
            }
        }
    }

    /// Drain the shims' raw events, stamping each with the current virtual
    /// time and gossip-tick index; every packet sent or dropped also counts
    /// into `bytes_by_tier`.
    fn drain_log(&mut self, events: &mut Vec<Event>) {
        let tick = self.current_tick();
        let virtual_ms = self.virtual_ms;
        let mut log = self.log.borrow_mut();
        events.extend(log.drain(..).map(|kind| {
            if let EventKind::PacketSent {
                bytes, kind, tier, ..
            }
            | EventKind::PacketDropped {
                bytes, kind, tier, ..
            } = &kind
            {
                self.bytes_by_tier.add(*tier, *kind, *bytes);
            }
            Event {
                tick,
                virtual_ms,
                kind,
            }
        }));
    }

    /// Number of yields needed so every spawned runtime task gets polled after
    /// a virtual-time advance. Single-thread tokio polls one task per yield,
    /// so a large cluster needs an explicit drain or it under-polls.
    async fn drain_pending(&self) {
        // Scale with the *live* node count, not the initial config, so a
        // cluster grown by joins is still fully polled.
        let budget = self.nodes.len().saturating_mul(4).max(16);
        for _ in 0..budget {
            tokio::task::yield_now().await;
        }
    }

    /// After a sub-step: read every runtime's counters, emit one `Tick` /
    /// `ThresholdFire` per round since the last read, and diff the gateways of
    /// each node whose role count moved, the only nodes whose snapshot is
    /// fetched. A node's events come in that order, so the stream is
    /// deterministic.
    async fn observe(&mut self, events: &mut Vec<Event>) {
        for pos in 0..self.nodes.len() {
            let Some(now) = counters(&self.nodes[pos].admin_tx).await else {
                continue;
            };
            let seen = std::mem::replace(&mut self.nodes[pos].seen, now);
            let node = self.nodes[pos].id;
            let tick = self.current_tick();
            let virtual_ms = self.virtual_ms;
            let rounds = [
                (
                    now.rounds_heartbeat - seen.rounds_heartbeat,
                    EventKind::Tick { node },
                ),
                (
                    now.rounds_threshold - seen.rounds_threshold,
                    EventKind::ThresholdFire { node },
                ),
            ];
            for (count, kind) in rounds {
                events.extend((0..count).map(|_| Event {
                    tick,
                    virtual_ms,
                    kind: kind.clone(),
                }));
            }
            if now.role_changes != seen.role_changes {
                self.diff_gateways(pos, events).await;
            }
        }
    }

    /// Compare node `pos`'s gateway into each zone, as its runtime reports
    /// it, with the last read, and emit a `GatewayChanged` for each that moved.
    async fn diff_gateways(&mut self, pos: usize, events: &mut Vec<Event>) {
        let Some(snap) = admin_snapshot(&self.nodes[pos].admin_tx).await else {
            return;
        };
        self.gateway_fetches += 1;
        let mut gateways = [None; MAX_ZONES];
        {
            let addresses = self.addresses.borrow();
            for peer in snap.peers.iter().filter(|p| p.role == Role::Gateway) {
                if let PeerZone::Other(hash) = peer.zone
                    && let Some(zone) = self.zone_index(hash)
                {
                    gateways[usize::from(zone)] = addresses.get(&peer.addr).map(|m| m.id);
                }
            }
        }
        let (tick, virtual_ms) = (self.current_tick(), self.virtual_ms);
        let node = &mut self.nodes[pos];
        let before = std::mem::replace(&mut node.gateways, gateways);
        for zone in 0..self.zone_names.len() {
            if before[zone] != gateways[zone] {
                events.push(Event {
                    tick,
                    virtual_ms,
                    kind: EventKind::GatewayChanged {
                        origin: node.id,
                        zone: zone as u8,
                        from: before[zone],
                        to: gateways[zone],
                    },
                });
            }
        }
    }

    /// The index of the zone whose name hashes to `hash`, through the
    /// library's own zone hash.
    fn zone_index(&self, hash: ZoneHash) -> Option<u8> {
        self.zone_names
            .iter()
            .position(|zone| zone.hash() == hash)
            .map(|index| index as u8)
    }

    /// Every node's scalar state, plus the cells and peer rows of `inspect`
    /// alone: the page shows one node's rows, and every node's would cost a
    /// row per node pair each frame.
    async fn snapshot(&self, inspect: Option<u32>) -> ClusterState {
        let fingerprint = self.rule.fingerprint;
        let mut nodes = Vec::with_capacity(self.nodes.len());
        let mut inspected = None;
        let mut oracle_total = 0;
        // Insertion order, so a node's position in `nodes` is its rank — the
        // frontend turns that into a ring slot, while `id` stays its identity.
        for handle in &self.nodes {
            let dump = cell_dump(&handle.admin_tx).await;
            // The ground-truth windowed total, derived from the live CRDT
            // cells rather than a monotonic accumulator: sum every node's
            // locally-originated cells for the watched rule. Each hit is
            // recorded once at its origin (`merge_remote` preserves the wire
            // origin, so a forwarded replica is never local on a non-origin
            // holder), so the sum counts each hit exactly once — and it decays
            // with the window as cells age out. This is the honest target the
            // convergence fan races toward.
            let own = Some(handle.identity.node_id.0);
            oracle_total += dump
                .iter()
                .flat_map(|d| &d.cells)
                .filter(|cell| cell.origin_node_id == own && cell.rule_fingerprint == fingerprint)
                .map(|cell| cell.count)
                .sum::<u64>();
            // `node_id` / `incarnation` come from the engine's own record of
            // the node, so they survive even if the runtime has
            // already stopped and the admin round-trip returns
            // `None` (defaults for the rest).
            let snap = admin_snapshot(&handle.admin_tx).await;
            if inspect == Some(handle.id) {
                inspected = Some(Inspected {
                    id: handle.id,
                    cells: dump
                        .as_ref()
                        .map(|d| self.cells_from_dump(handle.identity, d))
                        .unwrap_or_default(),
                    peers: snap
                        .as_ref()
                        .map(|s| self.peers_from_snapshot(handle.zone, s))
                        .unwrap_or_default(),
                });
            }
            let counters = snap.as_ref().map(|s| s.counters).unwrap_or_default();
            nodes.push(NodeState {
                id: handle.id,
                zone: handle.zone,
                node_id: handle.identity.node_id.0,
                incarnation: handle.identity.incarnation,
                aggregate_total: handle.aggregate.total(),
                rounds_heartbeat: counters.rounds_heartbeat,
                rounds_threshold: counters.rounds_threshold,
                gateway_failovers: counters.gateway_failovers,
                gap_reports_sent: counters.gap_reports_sent,
                rto_rewinds_own: counters.rto_rewinds_own,
                rto_rewinds_relay: counters.rto_rewinds_relay,
                sweeps_served: counters.sweeps_served,
                error_budget: snap.as_ref().map_or(0, |s| s.last_error_budget),
                decode_reject_count: counters.decode_rejects(),
                store_stats: snap
                    .as_ref()
                    .map(|s| s.store_stats.into())
                    .unwrap_or_default(),
            });
        }
        let live = self.live_by_zone();
        let zones = ZONE_NAMES[..self.zone_names.len()]
            .iter()
            .zip(live)
            .map(|(&name, live)| ZoneView { name, live })
            .collect();
        ClusterState {
            virtual_ms: self.virtual_ms,
            tick: self.current_tick(),
            nodes,
            inspected,
            zones,
            bytes_by_tier: self.bytes_by_tier,
            oracle_total,
            // The CRDT's own window position at this instant — the Strata
            // right-anchors its fixed-width grid on this epoch instead of
            // recomputing the boundary in TypeScript.
            bucket_epoch_now: self.rule.current_epoch(self.virtual_ms),
        }
    }

    fn cells_from_dump(&self, identity: NodeIdentity, dump: &CellDumpSnapshot) -> Vec<CellView> {
        dump.cells
            .iter()
            .map(|cell| CellView {
                rule: cell.rule_fingerprint,
                key: cell.key_hash,
                bucket: cell.bucket,
                count: cell.count,
                age_ms: self.virtual_ms.saturating_sub(cell.last_update_millis),
                origin: cell
                    .origin_node_id
                    .and_then(|id| self.node_id_index.get(&id).copied()),
                is_local: cell.origin_node_id == Some(identity.node_id.0),
            })
            .collect()
    }

    /// Node `own_zone`'s peer rows. `PeerZone::Same` means the observer knows
    /// its own zone, which happens only in `Configured`, where the reported
    /// zone is the placement.
    fn peers_from_snapshot(&self, own_zone: u8, snap: &AdminSnapshot) -> Vec<PeerView> {
        let addresses = self.addresses.borrow();
        snap.peers
            .iter()
            .map(|peer| PeerView {
                id: addresses.get(&peer.addr).map(|member| member.id),
                zone: match peer.zone {
                    PeerZone::Unknown => None,
                    PeerZone::Same => Some(own_zone),
                    PeerZone::Other(hash) => self.zone_index(hash),
                },
                node_id: peer.node_id.map(|id| id.0),
                role: peer.role,
                answer: peer.answer,
                unanswered_ms: peer.unanswered_ms,
                own_unacked: peer.own_unacked,
                relay_unacked: peer.relay_unacked,
            })
            .collect()
    }

    async fn shutdown(&mut self) {
        for node in &self.nodes {
            let _ = node.client.shutdown().await;
        }
        for node in &mut self.nodes {
            let join =
                std::mem::replace(&mut node.join, tokio::task::spawn_local(async { Ok(()) }));
            let _ = join.await;
        }
    }
}

/// The gossip address of the node with stable id `id`. A pure function of the
/// id — and since ids are never reused, addresses never collide across churn.
/// It is therefore also the address a newcomer bootstraps against and the one a
/// removal unbinds, with no lookup needed.
fn addr_of_id(id: u32) -> SocketAddr {
    debug_assert!(id <= MAX_ID, "add_node spends no id past MAX_ID");
    SocketAddr::from(([127, 0, 0, 1], BASE_PORT + id as u16))
}

/// One `Snapshot` admin round-trip. `None` if the runtime has already stopped.
async fn admin_snapshot(tx: &mpsc::Sender<AdminCommand>) -> Option<AdminSnapshot> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(AdminCommand::Snapshot { reply: reply_tx })
        .await
        .ok()?;
    reply_rx.await.ok()
}

/// One `Counters` admin round-trip, which allocates one `oneshot`. `None` if
/// the runtime has already stopped.
async fn counters(tx: &mpsc::Sender<AdminCommand>) -> Option<GossipCounters> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(AdminCommand::Counters { reply: reply_tx })
        .await
        .ok()?;
    reply_rx.await.ok()
}

/// One `CellDump` admin round-trip. `None` if the runtime has already stopped.
async fn cell_dump(tx: &mpsc::Sender<AdminCommand>) -> Option<CellDumpSnapshot> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(AdminCommand::CellDump { reply: reply_tx })
        .await
        .ok()?;
    reply_rx.await.ok()
}

#[cfg(test)]
mod tests;
