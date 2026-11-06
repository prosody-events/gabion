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
//! time moves only inside a `Step` or `StepTo` handler, which sets the shared
//! clock every node reads.
//!
//! **Sub-steps.** Time moves from one due instant to the next: a node's
//! heartbeat (each node at its own fixed phase), one emit floor after a node
//! armed its prompt ticker, or, stepping by [`StepUntil::Event`], the instant
//! admitted hits leave the window. A sub-step has two halves. The open half
//! fires what is due while the router holds every packet sent; the close half
//! delivers them and reads the counters of the nodes it touched. An admitted
//! request opens a sub-step too, so its eager-flush packets are held. Between
//! the halves the sub-step is open, and every command that is carried out,
//! `Snapshot` aside, closes it first, so no count changes before its packet
//! lands.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroU128};
use std::rc::Rc;
use std::time::Duration;

use futures::channel::mpsc::{UnboundedSender, unbounded};
use gabion::aggregate::AggregateTable;
use gabion::crdt::{BucketEpoch, CellStore, KeyHash, NodeId, NodeIdentity, ValidStorageConfig};
use gabion::discovery::{Peer, PeerEvent};
use gabion::gossip::sim::{LinkPolicy, SimRouter};
use gabion::gossip::{
    AdminCommand, AdminSnapshot, CellDumpSnapshot, GossipClient, GossipCounters, GossipError,
    GossipRuntime, PeerZone, Role,
};
use gabion::rules::{RuleSpec, RuleTable};
use gabion::settings::{SettingError, Settings};
use gabion::zone::{ZoneHash, ZoneName};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, LocalSet};

use crate::clock::{ManualClock, SharedNow, Tickers};
use crate::config::{MAX_NODES, MAX_ZONES, SimConfig, ValidSimConfig, ZONE_NAMES, ZoneMode};
use crate::event::{
    Admission, BytesByTier, CellView, ClusterState, Event, EventBatch, EventKind, Inspected, Lag,
    NodeState, PacketKind, PeerView, Reopens, ZoneView,
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

/// Each node's inbound queue: three packets from each of the other
/// `MAX_NODES − 1` nodes, 765 at 256 nodes, for every node in one zone. A
/// round sends each peer one own-stream frame, 33 cells to a packet at the
/// default payload, so two packets carry a frame of up to 66 cells; the
/// visualizer's one watched key leaves each node at most `live_buckets + 1`
/// own cells (11 at the default window). The third is first contact: a peer
/// that hears a node's greeting answers it in its next prompt round, frame
/// and sweep request together, and with heartbeat phases spread every peer's
/// answer can land in the same sub-step. A bounded channel allocates as it
/// fills, so the bound costs nothing unused. A step that hands a node more (a
/// window over 65 buckets, or many keys through the engine API) overflows the
/// queue: the logging transport reports the excess as `PacketDropped`, and
/// each stream resends it after one heartbeat.
pub(crate) const VIZ_ROUTER_CAPACITY: usize = 3 * (MAX_NODES - 1);

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

/// Where a `Step` stops.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum StepUntil {
    /// At the requested instant.
    Time,
    /// At the first of: the requested instant; right after a sub-step's fired
    /// rounds sent a packet that carries cells, before it lands, with the
    /// sub-step left open; or the instant admitted hits leave the window.
    Event,
}

/// The key the snapshot watches, and the weight of the request its
/// inspected node's `next_send` decides.
#[derive(Clone, Copy, Debug)]
pub struct Watch {
    pub key: KeyHash,
    pub probe_hits: NonZeroU64,
}

/// Whether a sub-step's packets are still held. `Open` exactly while the
/// router defers delivery: only [`EngineState::open_sub_step`] and an admitted
/// request begin deferring, and only [`EngineState::close_sub_step`], which
/// every command that is carried out calls first, releases.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubStep {
    Closed,
    Open,
}

/// Words of a [`NodeSet`].
const NODE_SET_WORDS: usize = MAX_NODES / 64;
const _: () = assert!(
    MAX_NODES.is_multiple_of(64),
    "a NodeSet covers MAX_NODES exactly"
);

/// A set of positions in the engine's `nodes`, one bit each: fixed size, so
/// it never allocates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NodeSet([u64; NODE_SET_WORDS]);

impl NodeSet {
    const EMPTY: Self = Self([0; NODE_SET_WORDS]);

    /// Positions `0..n`.
    #[inline]
    fn first(n: usize) -> Self {
        let mut set = Self::EMPTY;
        for (w, word) in set.0.iter_mut().enumerate() {
            let below = n.saturating_sub(w * 64);
            *word = if below >= 64 {
                u64::MAX
            } else {
                (1u64 << below) - 1
            };
        }
        set
    }

    #[inline]
    fn insert(&mut self, i: usize) {
        self.0[i / 64] |= 1 << (i % 64);
    }

    #[inline]
    fn contains(&self, i: usize) -> bool {
        self.0[i / 64] & (1 << (i % 64)) != 0
    }

    #[inline]
    fn set(&mut self, i: usize, on: bool) {
        let bit = 1 << (i % 64);
        if on {
            self.0[i / 64] |= bit;
        } else {
            self.0[i / 64] &= !bit;
        }
    }

    /// Delete position `i` and move every higher position down by one, as
    /// `Vec::remove` moves the nodes above a removed one.
    #[inline]
    fn remove_shift(&mut self, i: usize) {
        let (w, b) = (i / 64, i % 64);
        let word = self.0[w];
        let below = word & ((1u64 << b) - 1);
        // Two shifts, so that `b == 63` never shifts by the word's width.
        let above = (word >> b) >> 1;
        self.0[w] = below | (above << b);
        for k in w + 1..NODE_SET_WORDS {
            self.0[k - 1] |= (self.0[k] & 1) << 63;
            self.0[k] >>= 1;
        }
    }

    #[inline]
    fn union_assign(&mut self, other: Self) {
        for (word, other) in self.0.iter_mut().zip(other.0) {
            *word |= other;
        }
    }

    /// The positions of `self` that `other` lacks.
    #[inline]
    fn and_not(mut self, other: Self) -> Self {
        for (word, other) in self.0.iter_mut().zip(other.0) {
            *word &= !other;
        }
        self
    }

    #[inline]
    fn intersects(&self, other: &Self) -> bool {
        self.0.iter().zip(other.0).any(|(a, b)| a & b != 0)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.0.iter().all(|&word| word == 0)
    }

    #[inline]
    fn len(&self) -> usize {
        self.0.iter().map(|word| word.count_ones() as usize).sum()
    }

    /// The positions, ascending.
    #[inline]
    fn iter(self) -> impl Iterator<Item = usize> {
        self.0.into_iter().enumerate().flat_map(|(w, mut bits)| {
            std::iter::from_fn(move || {
                (bits != 0).then(|| {
                    let bit = bits.trailing_zeros() as usize;
                    bits &= bits - 1;
                    w * 64 + bit
                })
            })
        })
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
    #[error(
        "a request must carry at least one hit: gabion admits or rejects a request whole by its \
         weight, and a request of zero hits would count nothing. Set Hits per Send to a whole \
         number of 1 or more."
    )]
    NoHits,
}

/// A command sent into the engine. Each carries a `oneshot` the engine replies
/// on once it has done the work; the reply ordering across commands is FIFO
/// because the engine processes one at a time.
pub enum Command {
    /// A request of `hits` for `key` at node `node`, at the current virtual
    /// time. The node decides it first, from its own count table; an admitted
    /// request is recorded, and its eager-flush packets are held until the
    /// next command other than a snapshot.
    SubmitRequest {
        node: u32,
        key: KeyHash,
        hits: NonZeroU64,
        reply: oneshot::Sender<Result<EventBatch, EngineError>>,
    },
    /// Install a directed link policy between two nodes.
    SetLinkPolicy {
        src: u32,
        dst: u32,
        policy: LinkPolicyKind,
        reply: oneshot::Sender<Result<(), EngineError>>,
    },
    /// Restore the configured network on every directed link between live
    /// nodes, the scenario's uniform loss included, undoing any partition or
    /// per-link policy. Engine-driven (it owns the live membership) so it
    /// stays correct as nodes join and leave.
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
    /// Advance virtual time by at most `delta_ms`, stopping where `until`
    /// says, collecting the events produced.
    Step {
        delta_ms: u64,
        until: StepUntil,
        reply: oneshot::Sender<EventBatch>,
    },
    /// Advance virtual time to absolute `virtual_ms` (no-op if already past).
    StepTo {
        virtual_ms: u64,
        reply: oneshot::Sender<EventBatch>,
    },
    /// Per-node cluster snapshot of `watch`'s key, with the cells and peer
    /// rows of node `inspect` alone.
    Snapshot {
        inspect: Option<u32>,
        watch: Watch,
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
                let _ = reply.send(state.set_link_policy(src, dst, policy).await);
            }
            Command::Heal { reply } => {
                state.heal().await;
                let _ = reply.send(());
            }
            Command::AddNode { zone, reply } => {
                let _ = reply.send(state.add_node(zone).await);
            }
            Command::RemoveNode { id, reply } => {
                let _ = reply.send(state.remove_node(id).await);
            }
            Command::Step {
                delta_ms,
                until,
                reply,
            } => {
                let _ = reply.send(state.step(delta_ms, until).await);
            }
            Command::StepTo { virtual_ms, reply } => {
                let delta = virtual_ms.saturating_sub(state.virtual_ms);
                let _ = reply.send(state.step(delta, StepUntil::Time).await);
            }
            Command::Snapshot {
                inspect,
                watch,
                reply,
            } => {
                let _ = reply.send(state.snapshot(inspect, watch).await);
            }
            Command::Shutdown => break,
        }
    }
    state.shutdown().await;
    Ok(())
}

/// One node's handles, owned by the engine for the session. The engine's `Vec`
/// of these is sorted by ascending `id`: `spawn_node` alone appends, with the
/// next unused id, and `remove_node` keeps the order, so a lookup by id is a
/// binary search (`position_of`).
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
    /// Bytes of `Own` packets this node sent, from the log.
    own_data_bytes: u64,
    /// The next instant its heartbeat fires: its phase, then every period.
    next_heartbeat: u64,
    /// The instant its prompt ticker fires: one emit floor after the engine
    /// first saw it armed, as a ticker of that interval would fire.
    prompt_due: Option<u64>,
    /// The positions whose link into this node blocks.
    blocked_from: NodeSet,
    /// Hits this node admitted since it joined.
    admitted_hits: u64,
    /// Hits this node rejected since it joined.
    rejected_hits: u64,
}

/// All engine state, owned as a local inside [`engine_loop`] (no `Rc<RefCell>`
/// wrapper — the task owns it outright).
struct EngineState {
    config: SimConfig,
    /// Every node's settings, before its gossip bind and static peers.
    settings: Settings,
    /// Every node's storage, sized by [`SimConfig::settings`].
    storage: ValidStorageConfig,
    /// Every node's count table's storage, sized for the watched keys.
    aggregate: ValidStorageConfig,
    /// The watched rule, which every node interns at construction.
    rules: RuleTable,
    /// That rule as admission reads it: the bucket a request is recorded in,
    /// the limit, and the window.
    spec: RuleSpec,
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
    /// Sub-steps opened: one per distinct heartbeat phase in a quiet cluster.
    substeps: u64,
    /// Whether the current sub-step's packets are still held.
    sub_step: SubStep,
    /// Positions fired, prompted, sent a packet, or admitting a request in the
    /// current sub-step: the close half reads the counters of, and sizes its
    /// drain by, these.
    touched: NodeSet,
    /// Events drained and stamped, waiting for the next batch.
    outbox: Vec<Event>,
    /// Hits the cluster admitted, and rejected, since the session began.
    admitted_hits: u64,
    rejected_hits: u64,
    /// Admitted hits by bucket, one slot per live bucket, each stamped with
    /// the bucket it counts; a slot whose stamp is out of the window counts
    /// nothing. Allocated once.
    admitted_by_bucket: Box<[(BucketEpoch, u64)]>,
    /// Bytes every sender put on the wire, drained from the log.
    bytes_by_tier: BytesByTier,
    /// Admin snapshots the gateway diff has fetched: one per node whose roles
    /// moved.
    gateway_fetches: u64,
    /// `Counters` round trips `observe` has made.
    counter_reads: u64,
    /// Yields the drains have spent.
    drain_yields: u64,
}

impl EngineState {
    fn build(
        ValidSimConfig {
            config,
            settings,
            storage,
            aggregate,
            rules,
            spec,
            zone_names,
        }: ValidSimConfig,
    ) -> Result<Self, EngineError> {
        let log: EventLog = Rc::new(RefCell::new(Vec::new()));
        let addresses: AddressBook = Rc::new(RefCell::new(HashMap::new()));
        // One shared time base for the whole cluster; each node gets its own
        // tick channel so a fired tick queues rather than coalescing.
        let now: SharedNow = Rc::new(Cell::new(0));

        let initial_nodes = config.nodes;
        let live_buckets = spec.live_buckets.get() as usize;
        let mut state = Self {
            config,
            settings,
            storage,
            aggregate,
            rules,
            spec,
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
            sub_step: SubStep::Closed,
            touched: NodeSet::EMPTY,
            outbox: Vec::new(),
            admitted_hits: 0,
            rejected_hits: 0,
            admitted_by_bucket: vec![(BucketEpoch::default(), 0); live_buckets].into_boxed_slice(),
            bytes_by_tier: BytesByTier::default(),
            gateway_fetches: 0,
            counter_reads: 0,
            drain_yields: 0,
        };

        // The initial members are ids 0..N, so the address of id `i` is
        // `BASE_PORT + i` — the same set every member bootstraps against, and
        // the set a later newcomer will be handed too.
        let addrs: Vec<SocketAddr> = (0..initial_nodes).map(|i| addr_of_id(i as u32)).collect();
        let zones = state.zone_names.len();
        for i in 0..initial_nodes {
            state.spawn_node((i % zones) as u8, &addrs)?;
            state.baseline_links(i);
        }
        Ok(state)
    }

    /// The configured network: the scenario's uniform loss on every link.
    fn baseline_policy(&self) -> LinkPolicyKind {
        if self.config.uniform_loss > 0.0 {
            LinkPolicyKind::DropProb {
                p: self.config.uniform_loss,
            }
        } else {
            LinkPolicyKind::Pass
        }
    }

    /// Install `policy` on the link from position `src` to position `dst`:
    /// the one path that writes the router's policy and the reachability bit
    /// the snapshot's lag reads.
    fn set_link(&mut self, src: usize, dst: usize, policy: LinkPolicyKind) {
        self.router
            .set_link_policy(self.nodes[src].addr, self.nodes[dst].addr, policy.into());
        self.nodes[dst].blocked_from.set(src, blocks(policy));
    }

    /// Give every link between position `pos` and a lower position the
    /// baseline, both ways: called after each spawn, so every pair is set
    /// once. A new address has no policy, which the router treats as `Pass`
    /// and the reachability bits as open, so a lossless baseline writes
    /// nothing.
    fn baseline_links(&mut self, pos: usize) {
        let baseline = self.baseline_policy();
        if matches!(baseline, LinkPolicyKind::Pass) {
            return;
        }
        for other in 0..pos {
            self.set_link(pos, other, baseline);
            self.set_link(other, pos, baseline);
        }
    }

    /// Build, spawn, and register one node with the next unused stable id,
    /// bootstrapped against `bootstrap` (its own address is filtered out if
    /// present), and return that id. The single node-construction path and the
    /// only push onto `nodes`, so `nodes` stays sorted by id: both
    /// [`EngineState::build`] and [`EngineState::add_node`] route through here.
    ///
    /// Every node shares the storage config and settings `SimConfig::validate`
    /// built once, so a joiner is sized like the initial members; its gossip
    /// config adds only its bind and peers, through `Settings::validate`. Its
    /// first heartbeat is the first instant after now at its phase.
    fn spawn_node(&mut self, zone: u8, bootstrap: &[SocketAddr]) -> Result<u32, SettingError> {
        let id = self.next_id;
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

        let aggregate = Rc::new(EventEmittingAggregateStore::new(
            id,
            self.log.clone(),
            &self.aggregate,
        ));
        let (admin_tx, admin_rx) = mpsc::channel::<AdminCommand>(1);
        let period = self.config.tick_interval_ms;
        let (clock, tickers) = ManualClock::new(self.now.clone(), Duration::from_millis(period));
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
            own_data_bytes: 0,
            next_heartbeat: first_heartbeat(id, period, self.virtual_ms),
            prompt_due: None,
            blocked_from: NodeSet::EMPTY,
            admitted_hits: 0,
            rejected_hits: 0,
        });
        self.next_id += 1;
        Ok(id)
    }

    /// The events drained since the last batch, and the instant reached.
    fn batch(&mut self) -> EventBatch {
        // Sized like the batch just taken, so a quiet command allocates
        // nothing and a busy one never regrows.
        let len = self.outbox.len();
        EventBatch {
            events: std::mem::replace(&mut self.outbox, Vec::with_capacity(len)),
            virtual_ms: self.virtual_ms,
        }
    }

    /// The open half of a sub-step at `at`: move the clock, hold every packet
    /// sent, fire every heartbeat due and every armed prompt, and let the
    /// fired rounds run. Whether those rounds sent or lost a packet that
    /// carries cells: a wave.
    async fn open_sub_step(&mut self, at: u64) -> bool {
        self.virtual_ms = at;
        self.now.set(at);
        self.substeps += 1;
        self.router.begin_deferred();
        self.sub_step = SubStep::Open;
        self.fire(at).await;
        self.drain_pending().await;
        let before = self.outbox.len();
        self.drain_log();
        self.outbox[before..].iter().any(|event| match event.kind {
            EventKind::PacketSent { kind, .. } | EventKind::PacketDropped { kind, .. } => {
                kind.carries_cells()
            }
            _ => false,
        })
    }

    /// The close half: deliver the held packets at the current instant, let
    /// the receivers merge them, and read the touched nodes' counters. No
    /// time passes. A no-op while no sub-step is open.
    async fn close_sub_step(&mut self) {
        if self.sub_step == SubStep::Closed {
            return;
        }
        self.router.release_deferred();
        self.drain_pending().await;
        self.drain_log();
        self.observe().await;
        self.sub_step = SubStep::Closed;
    }

    /// Node `node` decides a request of `hits` for `key` from its own count
    /// table, as the adapters decide. A rejected request is counted and
    /// nothing else happens. An admitted one is recorded, and the sub-step it
    /// opens holds its eager-flush packets until the next command other than
    /// a snapshot.
    async fn submit_request(
        &mut self,
        node: u32,
        key: KeyHash,
        hits: NonZeroU64,
    ) -> Result<EventBatch, EngineError> {
        let pos = position_of(&self.nodes, node).ok_or(EngineError::UnknownNode { id: node })?;
        self.close_sub_step().await;
        let (spec, now) = (self.spec, self.virtual_ms);
        let table = self.nodes[pos].aggregate.table();
        let total = table.window_total(spec, key, now);
        let admission = decide(spec, table, key, now, total, hits);
        let hits = hits.get();
        self.outbox.push(Event {
            virtual_ms: now,
            kind: EventKind::Decided {
                node,
                hits,
                admission,
            },
        });
        match admission {
            Admission::Rejected { .. } => {
                let handle = &mut self.nodes[pos];
                handle.rejected_hits = handle.rejected_hits.saturating_add(hits);
                self.rejected_hits = self.rejected_hits.saturating_add(hits);
            }
            Admission::Admitted { .. } => {
                self.router.begin_deferred();
                self.sub_step = SubStep::Open;
                self.touched.insert(pos);
                let bucket = spec.bucket_at(now);
                self.nodes[pos]
                    .client
                    .record(spec.fingerprint, key, bucket, hits, spec.limit, now)
                    .await
                    .map_err(|source| EngineError::Gossip { node, source })?;
                // An eager flush's threshold round runs on the runtime's next
                // turn, so the drain puts its packets in this batch.
                self.drain_pending().await;
                self.drain_log();
                let handle = &mut self.nodes[pos];
                handle.admitted_hits = handle.admitted_hits.saturating_add(hits);
                self.admitted_hits = self.admitted_hits.saturating_add(hits);
                let live = self.admitted_by_bucket.len();
                let slot = &mut self.admitted_by_bucket[bucket.0 as usize % live];
                if slot.0 != bucket {
                    *slot = (bucket, 0);
                }
                slot.1 = slot.1.saturating_add(hits);
            }
        }
        Ok(self.batch())
    }

    /// Install `policy` on the link from node `src` to node `dst`. Packets
    /// already sent keep the fate the link gave them when they left.
    async fn set_link_policy(
        &mut self,
        src: u32,
        dst: u32,
        policy: LinkPolicyKind,
    ) -> Result<(), EngineError> {
        let [src, dst] = [src, dst]
            .map(|id| position_of(&self.nodes, id).ok_or(EngineError::UnknownNode { id }));
        let (src, dst) = (src?, dst?);
        self.close_sub_step().await;
        self.set_link(src, dst, policy);
        Ok(())
    }

    /// Restore the configured network on every directed link between live
    /// nodes: the scenario's uniform loss, never lossless when the scenario
    /// has loss. Sweeps the live nodes (not a stale `0..N`), so it stays
    /// correct after churn; writing a policy also clears any drop counter.
    async fn heal(&mut self) {
        self.close_sub_step().await;
        let baseline = self.baseline_policy();
        for src in 0..self.nodes.len() {
            for dst in 0..self.nodes.len() {
                if src != dst {
                    self.set_link(src, dst, baseline);
                }
            }
        }
    }

    /// Spawn a fresh cold-start member. It takes the next never-reused id and
    /// bootstraps knowing every current node; each survivor is told it joined
    /// (via that survivor's discovery stream) so gossip flows both ways. The
    /// newcomer starts with an empty store and catches up by anti-entropy. It
    /// joins `zone`, or the zone with the fewest live members, on links with
    /// the configured network.
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
        self.close_sub_step().await;
        // The newcomer bootstraps against everyone currently live.
        let live_addrs: Vec<SocketAddr> = self.nodes.iter().map(|h| h.addr).collect();
        let id = self.spawn_node(zone, &live_addrs)?;
        self.baseline_links(self.nodes.len() - 1);
        let newcomer_addr = addr_of_id(id);

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
        self.touched = NodeSet::first(self.nodes.len());
        self.drain_pending().await;
        self.drain_log();
        self.observe().await;
        Ok(self.batch())
    }

    /// Remove the live node with stable id `id`. Symmetric with
    /// [`EngineState::add_node`]: tell every survivor it left, stop its
    /// runtime, unbind its transport, and drop every trace of it. Its id is
    /// never reused, so survivors keep their own ids and addresses unchanged.
    /// Every survivor whose gateway it was reports that gateway's zone anew.
    async fn remove_node(&mut self, id: u32) -> Result<EventBatch, EngineError> {
        let pos = position_of(&self.nodes, id).ok_or(EngineError::UnknownNode { id })?;
        self.close_sub_step().await;
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
        // peer channels), its reachability bit in every survivor, the
        // address-book entry, and the id reverse-map entry.
        let handle = self.nodes.remove(pos);
        drop(handle);
        for survivor in &mut self.nodes {
            survivor.blocked_from.remove_shift(pos);
        }
        self.addresses.borrow_mut().remove(&addr);
        self.node_id_index.remove(&node_id);

        // Let survivors process the departure and the aborted task fully drop.
        // Every survivor is touched, which also drops any bit a remainder's
        // drain left at a position the removal has shifted.
        self.touched = NodeSet::first(self.nodes.len());
        self.drain_pending().await;
        self.drain_log();
        self.observe().await;
        // Removing a row moves no role, so the role-count diff misses the
        // departed gateway whenever no row left in its zone can replace it
        // (none there, or none heard yet): diff every survivor that used it.
        for pos in 0..self.nodes.len() {
            if self.nodes[pos].gateways.contains(&Some(id)) {
                self.diff_gateways(pos).await;
            }
        }
        Ok(self.batch())
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

    /// Advance by at most `delta_ms`, sub-step by sub-step, stopping where
    /// `until` says.
    async fn step(&mut self, delta_ms: u64, until: StepUntil) -> EventBatch {
        self.close_sub_step().await;
        let floor_ms = self.config.min_emit_interval_ms;
        let target = self.virtual_ms.saturating_add(delta_ms);
        // No request arrives inside a step, so the next expiry is fixed until
        // the step passes it, and stepping by event stops there.
        let expiry = match until {
            StepUntil::Event => self.next_expiry_ms(),
            StepUntil::Time => None,
        };

        // Each sub-step runs at the next due instant: a node's heartbeat, one
        // emit floor after a node armed its prompt ticker (it has relays,
        // resends, gap reports, probe answers or sweeps waiting), or the
        // expiry. So a relay leaves one floor after it is owed, as in
        // production, and a quiet cluster costs one sub-step per distinct
        // heartbeat phase.
        //
        // The open half fires the due nodes with delivery deferred, so every
        // fired node runs its round with nothing yet delivered; the close
        // half delivers the sub-step's packets and lets receivers merge them.
        // Their ticks are already consumed, so a freshly merged count goes out
        // no earlier than the next sub-step. Stepping by event returns between
        // the halves of the first sub-step that sent a wave, with its packets
        // held; the next command delivers them.
        loop {
            let t = self.virtual_ms;
            let mut next = u64::MAX;
            for node in &mut self.nodes {
                node.prompt_due = match (node.tickers.armed.get(), node.prompt_due) {
                    (false, _) => None,
                    (true, None) => Some(t.saturating_add(floor_ms)),
                    (true, due) => due,
                };
                next = next
                    .min(node.next_heartbeat)
                    .min(node.prompt_due.unwrap_or(u64::MAX));
            }
            if let Some(expiry) = expiry {
                next = next.min(expiry);
            }
            if next > target {
                break;
            }
            let wave = self.open_sub_step(next).await;
            if until == StepUntil::Event && wave {
                return self.batch();
            }
            self.close_sub_step().await;
            if expiry == Some(next) {
                return self.batch();
            }
        }

        // A remainder short of the next event moves the clock without firing
        // anything, so `virtual_ms` and the bucket clock still reach exactly
        // `target`.
        if self.virtual_ms < target {
            self.virtual_ms = target;
            self.now.set(target);
            self.drain_log();
        }
        self.batch()
    }

    /// Fire, in node order, the heartbeat and the prompt ticker of every node
    /// due at `at`, and mark each touched.
    /// Each send queues in that node's bounded inbox, so a busy runtime still
    /// consumes the tick on its next idle poll rather than dropping it. A
    /// closed inbox (the runtime stopped) is ignored.
    async fn fire(&mut self, at: u64) {
        let period = self.config.tick_interval_ms;
        for (pos, node) in self.nodes.iter_mut().enumerate() {
            if node.next_heartbeat == at {
                let _ = node.tickers.heartbeat.send(at).await;
                node.next_heartbeat = at.saturating_add(period);
                self.touched.insert(pos);
            }
            if node.prompt_due == Some(at) {
                let _ = node.tickers.prompt.send(at).await;
                node.prompt_due = None;
                self.touched.insert(pos);
            }
        }
    }

    /// Drain the shims' raw events into the outbox, stamping each with the
    /// current virtual time. Every packet sent or dropped also counts into
    /// `bytes_by_tier`, and into its sender's `own_data_bytes` when it is an
    /// `Own` packet; a packet sent wakes its receiver, which is marked
    /// touched.
    fn drain_log(&mut self) {
        let virtual_ms = self.virtual_ms;
        let nodes = &mut self.nodes;
        let touched = &mut self.touched;
        let bytes_by_tier = &mut self.bytes_by_tier;
        let mut log = self.log.borrow_mut();
        self.outbox.extend(log.drain(..).map(|kind| {
            if let EventKind::PacketSent {
                src,
                bytes,
                kind,
                tier,
                ..
            }
            | EventKind::PacketDropped {
                src,
                bytes,
                kind,
                tier,
                ..
            } = &kind
            {
                bytes_by_tier.add(*tier, *kind, *bytes);
                // A sender removed since (its last packets drain after the
                // removal) is skipped: the page weights live origins only.
                if *kind == PacketKind::Own
                    && let Some(pos) = position_of(nodes, *src)
                {
                    nodes[pos].own_data_bytes += u64::from(*bytes);
                }
            }
            if let EventKind::PacketSent { dst, .. } = &kind
                && let Some(pos) = position_of(nodes, *dst)
            {
                touched.insert(pos);
            }
            Event { virtual_ms, kind }
        }));
    }

    /// Yield until every touched runtime has been polled after a change, only
    /// the touched nodes having work. Single-thread tokio polls one task per
    /// yield, so the budget is four yields for each touched node, at least
    /// [`MIN_DRAIN_YIELDS`]; a join or a departure marks every node.
    async fn drain_pending(&mut self) {
        let budget = self.touched.len().saturating_mul(4).max(MIN_DRAIN_YIELDS);
        self.drain_yields += budget as u64;
        for _ in 0..budget {
            tokio::task::yield_now().await;
        }
    }

    /// After a sub-step: read the counters of every touched node, clearing
    /// the set, emit one `Tick` / `ThresholdFire` per round since the last
    /// read, and diff the gateways of each node whose role count moved, the
    /// only nodes whose snapshot is fetched. A node's events come in that
    /// order, so the stream is deterministic.
    async fn observe(&mut self) {
        let touched = std::mem::replace(&mut self.touched, NodeSet::EMPTY);
        for pos in touched.iter() {
            self.counter_reads += 1;
            let Some(now) = counters(&self.nodes[pos].admin_tx).await else {
                continue;
            };
            let seen = std::mem::replace(&mut self.nodes[pos].seen, now);
            let node = self.nodes[pos].id;
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
                self.outbox.extend((0..count).map(|_| Event {
                    virtual_ms,
                    kind: kind.clone(),
                }));
            }
            if now.role_changes != seen.role_changes {
                self.diff_gateways(pos).await;
            }
        }
    }

    /// Compare node `pos`'s gateway into each zone, as its runtime reports
    /// it, with the last read, and emit a `GatewayChanged` for each that moved.
    async fn diff_gateways(&mut self, pos: usize) {
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
        let virtual_ms = self.virtual_ms;
        let node = &mut self.nodes[pos];
        let before = std::mem::replace(&mut node.gateways, gateways);
        for zone in 0..self.zone_names.len() {
            if before[zone] != gateways[zone] {
                self.outbox.push(Event {
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

    /// The next instant admitted hits leave the window: the fall-off of the
    /// oldest live bucket that holds any, always after now.
    fn next_expiry_ms(&self) -> Option<u64> {
        let current = self.spec.bucket_at(self.virtual_ms);
        let live = self.spec.live_buckets.get();
        let oldest = self
            .admitted_by_bucket
            .iter()
            .filter(|&&(_, hits)| hits > 0)
            .map(|&(bucket, _)| current.age_since(bucket))
            .filter(|age| (0..live as i32).contains(age))
            .max()?;
        let bucket_ms = self.spec.bucket_millis.get();
        let fall_off = self.virtual_ms / bucket_ms - oldest as u64 + u64::from(live);
        Some(fall_off.saturating_mul(bucket_ms))
    }

    /// Whether some position of `from` has a path of links that do not
    /// block into position `pos`: a reverse breadth-first search by frontier
    /// sets that stops at the first such position, so nothing is queued or
    /// allocated, and a cluster with no cut into `pos` answers in one round.
    fn reaches(&self, from: NodeSet, pos: usize) -> bool {
        let all = NodeSet::first(self.nodes.len());
        let mut reach = NodeSet::EMPTY;
        reach.insert(pos);
        let mut frontier = reach;
        loop {
            let mut next = NodeSet::EMPTY;
            for m in frontier.iter() {
                next.union_assign(all.and_not(self.nodes[m].blocked_from));
            }
            let next = next.and_not(reach);
            if next.intersects(&from) {
                return true;
            }
            if next.is_empty() {
                return false;
            }
            reach.union_assign(next);
            frontier = next;
        }
    }

    /// Every node's scalar state for `watch`'s key, plus the cells and peer
    /// rows of `inspect` alone: the page shows one node's rows, and every
    /// node's would cost a row per node pair each frame. Reads only: an open
    /// sub-step stays open, so the snapshot shows its packets still held.
    async fn snapshot(&self, inspect: Option<u32>, watch: Watch) -> ClusterState {
        let spec = self.spec;
        let key = watch.key;
        let now = self.virtual_ms;
        let current = spec.bucket_at(now);
        let live = spec.live_buckets.get();
        let in_window = |bucket: BucketEpoch| (0..live as i32).contains(&current.age_since(bucket));
        let watched = |rule: u128, kh: KeyHash, bucket: BucketEpoch| {
            rule == spec.fingerprint && kh == key && in_window(bucket)
        };

        // Every origin's own in-window cells of the watched key: what every
        // node's replicas are held against. Each hit is recorded once at its
        // origin (`merge_remote` preserves the wire origin, so a forwarded
        // replica is never local on a non-origin holder), so their sum, the
        // ground-truth windowed total, counts each hit exactly once. Each dump
        // is dropped before the next is taken: a node's store holds a replica
        // of every origin's cells, so keeping every dump would hold N stores
        // at once. No virtual time passes between the passes.
        let mut origins: Vec<OriginCell> = Vec::new();
        let mut oracle_buckets = vec![0; live as usize];
        for handle in &self.nodes {
            let own = handle.identity.node_id.0;
            let dump = cell_dump(&handle.admin_tx).await;
            for cell in dump.iter().flat_map(|d| &d.cells) {
                if cell.origin_node_id == Some(own)
                    && watched(cell.rule_fingerprint, cell.key_hash, cell.bucket)
                {
                    let age = current.age_since(cell.bucket) as usize;
                    oracle_buckets[live as usize - 1 - age] += cell.count;
                    origins.push(OriginCell {
                        node_id: own,
                        bucket: cell.bucket,
                        count: cell.count,
                        updated_ms: cell.last_update_millis,
                        holders: NodeSet::EMPTY,
                    });
                }
            }
        }
        origins.sort_unstable_by_key(|cell| (cell.node_id, cell.bucket.0));
        let oracle_total = oracle_buckets.iter().sum();

        // Which nodes hold each origin cell at its origin's count.
        let mut inspected_cells = None;
        for (pos, handle) in self.nodes.iter().enumerate() {
            let dump = cell_dump(&handle.admin_tx).await;
            for cell in dump.iter().flat_map(|d| &d.cells) {
                let Some(origin) = cell.origin_node_id else {
                    continue;
                };
                if !watched(cell.rule_fingerprint, cell.key_hash, cell.bucket) {
                    continue;
                }
                if let Ok(i) = origins
                    .binary_search_by_key(&(origin, cell.bucket.0), |c| (c.node_id, c.bucket.0))
                    && cell.count >= origins[i].count
                {
                    origins[i].holders.insert(pos);
                }
            }
            if inspect == Some(handle.id) {
                inspected_cells = dump
                    .as_ref()
                    .map(|d| self.cells_from_dump(handle.identity, d));
            }
        }

        // Each origin's gateways, inverted: how many origins route through
        // each node.
        let mut gateway_for = vec![0u32; self.nodes.len()];
        for origin in &self.nodes {
            for &g in origin.gateways[..self.zone_names.len()].iter().flatten() {
                if let Some(pos) = position_of(&self.nodes, g) {
                    gateway_for[pos] += 1;
                }
            }
        }
        let mut nodes = Vec::with_capacity(self.nodes.len());
        let mut window_buckets = Vec::with_capacity(self.nodes.len() * live as usize);
        let mut inspected = None;
        // Ascending id order, the order the page lays the nodes out in.
        for (pos, handle) in self.nodes.iter().enumerate() {
            let table = handle.aggregate.table();
            let window_total = table.window_total(spec, key, now);
            window_buckets.extend((0..live).rev().map(|back| {
                table
                    .get(spec.fingerprint, key, current.back(back))
                    .map_or(0, |cell| cell.count)
            }));
            // The oldest change the node lacks was made no later than its
            // cell's last update at the origin, so the time since that update
            // never overstates how long the node has been behind.
            let lacking = || origins.iter().filter(|cell| !cell.holders.contains(pos));
            let lag = match lacking().map(|cell| cell.updated_ms).min() {
                None => Lag::CaughtUp,
                Some(updated) => {
                    let missing_for_ms = now.saturating_sub(updated);
                    let mut holders = NodeSet::EMPTY;
                    for cell in lacking() {
                        holders.union_assign(cell.holders);
                    }
                    if self.reaches(holders, pos) {
                        Lag::Behind { missing_for_ms }
                    } else {
                        Lag::Stranded { missing_for_ms }
                    }
                }
            };
            // `node_id` / `incarnation` come from the engine's own record of
            // the node, so they survive even if the runtime has already
            // stopped and the admin round-trip returns `None` (defaults for
            // the rest).
            let snap = admin_snapshot(&handle.admin_tx).await;
            if inspect == Some(handle.id) {
                inspected = Some(Inspected {
                    id: handle.id,
                    cells: inspected_cells.take().unwrap_or_default(),
                    peers: snap
                        .as_ref()
                        .map(|s| self.peers_from_snapshot(handle.zone, s))
                        .unwrap_or_default(),
                    next_send: decide(spec, table, key, now, window_total, watch.probe_hits),
                });
            }
            let counters = snap.as_ref().map(|s| s.counters).unwrap_or_default();
            nodes.push(NodeState {
                id: handle.id,
                zone: handle.zone,
                node_id: handle.identity.node_id.0,
                incarnation: handle.identity.incarnation,
                window_total,
                brim: decide(spec, table, key, now, window_total, NonZeroU64::MIN),
                lag,
                next_heartbeat_ms: handle.next_heartbeat,
                admitted_hits: handle.admitted_hits,
                rejected_hits: handle.rejected_hits,
                rounds_heartbeat: counters.rounds_heartbeat,
                gateway_failovers: counters.gateway_failovers,
                gap_reports_sent: counters.gap_reports_sent,
                rto_rewinds_own: counters.rto_rewinds_own,
                rto_rewinds_relay: counters.rto_rewinds_relay,
                sweeps_served: counters.sweeps_served,
                gateway_for: gateway_for[pos],
                own_data_bytes: handle.own_data_bytes,
                error_budget: snap.as_ref().map_or(0, |s| s.last_error_budget),
                store_stats: snap
                    .as_ref()
                    .map(|s| s.store_stats.into())
                    .unwrap_or_default(),
            });
        }
        let live_by_zone = self.live_by_zone();
        let zones = ZONE_NAMES[..self.zone_names.len()]
            .iter()
            .zip(live_by_zone)
            .map(|(&name, live)| ZoneView { name, live })
            .collect();
        ClusterState {
            virtual_ms: now,
            nodes,
            inspected,
            zones,
            bytes_by_tier: self.bytes_by_tier,
            oracle_total,
            oracle_buckets,
            window_buckets,
            admitted_hits: self.admitted_hits,
            rejected_hits: self.rejected_hits,
            next_expiry_ms: self.next_expiry_ms(),
            // The CRDT's own window position at this instant — the Strata
            // right-anchors its fixed-width grid on this epoch instead of
            // recomputing the boundary in TypeScript.
            bucket_epoch_now: current.0,
        }
    }

    fn cells_from_dump(&self, identity: NodeIdentity, dump: &CellDumpSnapshot) -> Vec<CellView> {
        dump.cells
            .iter()
            .map(|cell| CellView {
                rule: cell.rule_fingerprint,
                key: cell.key_hash.0,
                bucket: cell.bucket.0,
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

/// One origin's own in-window cell of the watched key, as the snapshot
/// reads it.
struct OriginCell {
    /// The origin's gossip identity.
    node_id: u128,
    bucket: BucketEpoch,
    count: u64,
    /// When the origin last recorded into it, in virtual ms.
    updated_ms: u64,
    /// The positions that hold it at its origin's count.
    holders: NodeSet,
}

/// The fewest yields a drain spends, however few nodes it waits for.
const MIN_DRAIN_YIELDS: usize = 16;

/// Node `id`'s heartbeat phase within a period of `period_ms`: the integer
/// Weyl sequence `((id · 0x9E37_79B9 mod 2³²) · period) >> 32`, which spreads
/// ids 0 to 11 over twelve distinct phases at every period from 50 to 1,000
/// ms, as production's unrelated process starts spread theirs. Node 0's phase
/// is 0. The product is taken in `u128` because the period has no upper bound.
fn heartbeat_phase(id: u32, period_ms: u64) -> u64 {
    let weyl = u128::from(id.wrapping_mul(0x9E37_79B9));
    ((weyl * u128::from(period_ms)) >> 32) as u64
}

/// The first instant strictly after `now` at node `id`'s heartbeat phase.
fn first_heartbeat(id: u32, period_ms: u64, now: u64) -> u64 {
    let phase = heartbeat_phase(id, period_ms);
    if now < phase {
        return phase;
    }
    let periods = (now - phase) / period_ms + 1;
    phase.saturating_add(periods.saturating_mul(period_ms))
}

/// A node's decision on a request of `hits` when its window holds `total`
/// of `key`: gabion's own comparison, [`RuleSpec::admits`]. A request heavier
/// than the limit never fits; otherwise the reopening time is the library's
/// sliding-window walk, and since `hits ≤ limit` makes the excess over the
/// limit at most `total`, the walk always finds the bucket whose fall-off
/// admits it and never falls back to a whole window.
fn decide(
    spec: RuleSpec,
    table: AggregateTable<'_>,
    key: KeyHash,
    now: u64,
    total: u64,
    hits: NonZeroU64,
) -> Admission {
    let hits = hits.get();
    if spec.admits(total, hits) {
        return Admission::Admitted {
            total_before: total,
        };
    }
    let reopens = if hits > spec.limit {
        Reopens::Never
    } else {
        Reopens::In {
            ms: table.time_until_admit_millis(spec, key, now, total, hits),
        }
    };
    Admission::Rejected { total, reopens }
}

/// Whether `policy` stops every packet, so waiting cannot carry a change
/// across the link: a block, or certain loss.
fn blocks(policy: LinkPolicyKind) -> bool {
    match policy {
        LinkPolicyKind::Block => true,
        LinkPolicyKind::DropProb { p } => p >= 1.0,
        LinkPolicyKind::Pass | LinkPolicyKind::DropFirst { .. } => false,
    }
}

/// Position in `nodes` of the live node with stable id `id`, or `None` if no
/// live node has it; `nodes` is sorted by id (see `NodeHandle`).
fn position_of(nodes: &[NodeHandle], id: u32) -> Option<usize> {
    nodes.binary_search_by_key(&id, |h| h.id).ok()
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
