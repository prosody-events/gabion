//! The single long-lived simulation engine.
//!
//! There is no new simulator here: [`run_engine`] stands up N
//! [`GossipRuntime`]s on one shared [`SimRouter`] under paused virtual time —
//! exactly the shape `gossip-bench`'s `run_inner` proves — and then loops over
//! a command channel, replying on a `oneshot` per command. The same function
//! backs both faces of the visualizer: playback (`step`) and live interaction
//! (`submit_request`, `set_link_policy`).
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
use std::rc::Rc;
use std::time::Duration;

use futures::channel::mpsc::{UnboundedSender, unbounded};
use gabion::crdt::{
    BucketEpoch, CellStore, CellStoreConfig, KeyHash, NodeId, NodeIdentity, RuleDescriptor,
};
use gabion::discovery::{Peer, PeerEvent};
use gabion::gossip::sim::{LinkPolicy, SimRouter};
use gabion::gossip::{
    AdminCommand, AdminSnapshot, CellDumpSnapshot, GossipClient, GossipConfig, GossipError,
    GossipRuntime,
};
use gabion::wire::FrameLimits;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, LocalSet};

use crate::clock::{ManualClock, SharedNow};
use crate::config::{ConfigError, SimConfig};
use crate::event::{CellView, ClusterState, Event, EventBatch, EventKind, NodeState, PeerView};
use crate::shims::{AddressBook, EventEmittingAggregateStore, EventLog, LoggingSimTransport};

/// First UDP port the engine assigns; node `i` binds `BASE_PORT + i`.
const BASE_PORT: u16 = 40_000;
/// Cluster id mixed into every packet header (matches the bench).
const CLUSTER_ID_HASH: u128 = 0xC1;
/// Local rule id for the single watched rule. Any value other than
/// `u32::MAX` marks the rule as locally applicable so its cells count toward
/// the aggregate (see `RuleDescriptor::applies_locally`).
const LOCAL_RULE_ID: u32 = 0;

/// The count width every node uses. `u32` matches the bench and is plenty for
/// the visualizer's hit volumes.
type Count = u32;

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
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(
        "node {id} is not a member of this cluster — it never joined, or it has \
         already left. Node ids are stable and are never reused, so a removed \
         node's id stays gone. Pick an id from the current cluster (the live \
         ids appear on the stage and in the Cluster control)."
    )]
    UnknownNode { id: u32 },
    #[error(
        "the gossip runtime for node {node} has stopped, so the request could \
         not be recorded. The simulation has ended; reload to start over."
    )]
    Gossip {
        node: u32,
        #[source]
        source: GossipError,
    },
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
    /// joined so they gossip to it. Replies with the events the join produced.
    AddNode { reply: oneshot::Sender<EventBatch> },
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
    /// Full per-node cluster snapshot, for seek / re-render.
    Snapshot {
        reply: oneshot::Sender<ClusterState>,
    },
    /// End the session and shut every runtime down.
    Shutdown,
}

/// Spawn the engine and process commands until the channel closes or a
/// `Shutdown` arrives. Validates the config up front so a bad config fails
/// before any runtime is built.
pub async fn run_engine(
    config: SimConfig,
    cmd_rx: mpsc::Receiver<Command>,
) -> Result<(), EngineError> {
    config.validate()?;
    let local = LocalSet::new();
    local.run_until(engine_loop(config, cmd_rx)).await
}

async fn engine_loop(
    config: SimConfig,
    mut cmd_rx: mpsc::Receiver<Command>,
) -> Result<(), EngineError> {
    let mut state = EngineState::build(config);
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
            Command::AddNode { reply } => {
                let _ = reply.send(state.add_node().await);
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
            Command::Snapshot { reply } => {
                let _ = reply.send(state.snapshot().await);
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
    /// This node's gossip address, `127.0.0.1:(BASE_PORT + id)`. Cached so a
    /// removal can unbind it without recomputing.
    addr: SocketAddr,
    client: GossipClient<Count>,
    aggregate: Rc<EventEmittingAggregateStore<Count>>,
    admin_tx: mpsc::Sender<AdminCommand>,
    /// Fires one gossip tick on this node (its `ManualClock`'s ticker drains
    /// the matching receiver).
    tick_tx: mpsc::Sender<()>,
    /// The discovery stream this node's runtime watches. The engine pushes
    /// `PeerEvent::Added`/`Removed` here as other nodes join and leave, so
    /// membership is dynamic rather than the static `stream::empty()` the bench
    /// uses. An `UnboundedReceiver` *is* a `Stream`, which is what `run` wants.
    peer_tx: UnboundedSender<PeerEvent>,
    join: JoinHandle<Result<(), GossipError>>,
    identity: NodeIdentity,
}

/// All engine state, owned as a local inside [`engine_loop`] (no `Rc<RefCell>`
/// wrapper — the task owns it outright).
struct EngineState {
    config: SimConfig,
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
    /// Ground-truth total hits submitted for the watched rule — the
    /// simulator-only oracle the convergence fan races toward.
    oracle_total: u64,
}

impl EngineState {
    fn build(config: SimConfig) -> Self {
        let log: EventLog = Rc::new(RefCell::new(Vec::new()));
        let addresses: AddressBook = Rc::new(RefCell::new(HashMap::new()));
        // One shared time base for the whole cluster; each node gets its own
        // tick channel so a fired tick queues rather than coalescing.
        let now: SharedNow = Rc::new(Cell::new(0));

        let initial_nodes = config.nodes;
        let mut state = Self {
            config,
            router: SimRouter::with_channel_capacity(256),
            nodes: Vec::with_capacity(initial_nodes),
            addresses,
            node_id_index: HashMap::with_capacity(initial_nodes),
            next_id: 0,
            log,
            now,
            virtual_ms: 0,
            oracle_total: 0,
        };

        // The initial members are ids 0..N, so the address of id `i` is
        // `BASE_PORT + i` — the same set every member bootstraps against, and
        // the set a later newcomer will be handed too.
        let addrs: Vec<SocketAddr> = (0..initial_nodes).map(|i| addr_of_id(i as u32)).collect();
        for _ in 0..initial_nodes {
            let id = state.next_id;
            state.next_id += 1;
            state.spawn_node(id, &addrs);
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

        state
    }

    /// Build, spawn, and register one node with stable id `id`, bootstrapped
    /// against `bootstrap` (its own address is filtered out if present). The
    /// single node-construction path: both [`EngineState::build`] and
    /// [`EngineState::add_node`] route through here, so the rule-intern footgun
    /// guard below is honored identically on every path.
    ///
    /// Per-node storage is sized from the *initial* `config.nodes`, but every
    /// capacity floors at a value that holds the whole `MAX_NODES`-bounded
    /// cluster (node dictionary ≥ 1024, peer table ≥ 256, cells ≥ 4096). So a
    /// node added to a cluster that started small still has room for every
    /// peer it will ever learn about — no resize on join.
    fn spawn_node(&mut self, id: u32, bootstrap: &[SocketAddr]) {
        let addr = addr_of_id(id);
        let identity = NodeIdentity::new(NodeId((id as u128) * 0x100 + 1), 1);

        let n = self.config.nodes as u32;
        let cell_capacity = self
            .config
            .cell_capacity
            .max((n.saturating_mul(4)).max(4_096));
        let node_dict_capacity = ((n + 16).max(1_024)).min(u16::MAX as u32) as u16;
        let peer_capacity = ((n + 16).max(256)).min(u16::MAX as u32) as u16;
        let local_dirty_capacity = (cell_capacity as usize).max(8_192);
        let forwarded_dirty_capacity = ((cell_capacity as usize) * 16).max(65_536);
        let max_cells_per_tick = (self.config.nodes * 4).max(4_096);
        let max_cells_per_frame = (max_cells_per_tick as u32).max(4_096);
        let tick_interval = Duration::from_millis(self.config.tick_interval_ms);

        let mut store = CellStore::<Count>::new(
            CellStoreConfig {
                cell_capacity,
                rule_dictionary_capacity: 64,
                node_dictionary_capacity: node_dict_capacity,
                local_dirty_capacity,
                forwarded_dirty_capacity,
                peer_capacity,
            },
            identity,
        );
        // Footgun guard: intern the watched rule with the configured
        // window/bucket *before* any request. Unknown rules otherwise intern
        // with `RuleDescriptor::default()` (60 s / 1 s), making bucket and
        // expiry math silently wrong.
        store.intern_rule(RuleDescriptor {
            fingerprint: self.config.rule_fingerprint,
            window_millis: self.config.rule_window_ms,
            bucket_millis: self.config.rule_bucket_ms,
            limit: self.config.rule_limit,
            flags: 0,
            local_rule_id: LOCAL_RULE_ID,
        });

        let bootstrap_peers: Vec<SocketAddr> =
            bootstrap.iter().copied().filter(|a| *a != addr).collect();

        let transport = LoggingSimTransport::new(
            self.router.bind(addr),
            self.router.clone(),
            id,
            self.addresses.clone(),
            self.log.clone(),
        );

        let gossip_cfg = GossipConfig {
            local_identity: identity,
            cluster_id_hash: CLUSTER_ID_HASH,
            bootstrap_peers,
            fanout: self.config.fanout,
            max_cells_per_tick,
            wire_limits: FrameLimits {
                max_payload_bytes: 1_400,
                max_cells: max_cells_per_frame,
            },
            send_queue_capacity: 128,
            limit_queue_capacity: 8_192,
            tick_interval,
            auth_key: None,
            rng_seed: self.config.rng_seed.wrapping_add(id as u64),
            target_err_bps: self.config.target_err_bps,
            min_emit_interval: Duration::from_millis(self.config.min_emit_interval_ms),
        };

        let aggregate = Rc::new(EventEmittingAggregateStore::<Count>::new(
            id,
            self.log.clone(),
        ));
        let (admin_tx, admin_rx) = mpsc::channel::<AdminCommand>(1);
        let (clock, tick_tx) = ManualClock::new(self.now.clone());
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
        self.addresses.borrow_mut().insert(addr, id);
        self.nodes.push(NodeHandle {
            id,
            addr,
            client,
            aggregate,
            admin_tx,
            tick_tx,
            peer_tx,
            join,
            identity,
        });
    }

    fn current_tick(&self) -> u64 {
        self.virtual_ms / self.config.tick_interval_ms.max(1)
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
        let bucket = (self.virtual_ms / self.config.rule_bucket_ms.max(1) as u64) as BucketEpoch;
        handle
            .client
            .record(
                self.config.rule_fingerprint,
                KeyHash(key),
                bucket,
                hits,
                self.config.rule_limit,
                self.virtual_ms,
            )
            .await
            .map_err(|source| EngineError::Gossip { node, source })?;
        self.oracle_total = self.oracle_total.saturating_add(hits);

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
    /// newcomer starts with an empty store and catches up by anti-entropy.
    async fn add_node(&mut self) -> EventBatch {
        let id = self.next_id;
        self.next_id += 1;
        let newcomer_addr = addr_of_id(id);
        // The newcomer bootstraps against everyone currently live.
        let live_addrs: Vec<SocketAddr> = self.nodes.iter().map(|h| h.addr).collect();
        self.spawn_node(id, &live_addrs);

        // Tell every survivor (everyone but the just-pushed newcomer) the
        // newcomer joined, so they add it to their peer table and gossip to it.
        let joined = PeerEvent::Added(Peer::new(newcomer_addr));
        for handle in &self.nodes {
            if handle.id != id {
                let _ = handle.peer_tx.unbounded_send(joined);
            }
        }

        // Let the newcomer bind and the survivors register the join before we
        // return, so the immediate snapshot already reflects the new member.
        self.drain_pending().await;
        let mut events = Vec::new();
        self.drain_log(&mut events);
        EventBatch {
            events,
            virtual_ms: self.virtual_ms,
            tick: self.current_tick(),
        }
    }

    /// Remove the live node with stable id `id`. Symmetric with
    /// [`EngineState::add_node`]: tell every survivor it left, stop its
    /// runtime, unbind its transport, and drop every trace of it. Its id is
    /// never reused, so survivors keep their own ids and addresses unchanged.
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
                let _ = handle.peer_tx.unbounded_send(left);
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
        Ok(EventBatch {
            events,
            virtual_ms: self.virtual_ms,
            tick: self.current_tick(),
        })
    }

    async fn step(&mut self, delta_ms: u64) -> EventBatch {
        let tick_ms = self.config.tick_interval_ms.max(1);
        let target = self.virtual_ms.saturating_add(delta_ms);
        let mut events = Vec::new();
        let mut prev = self.admin_counters().await;

        // Advance one whole gossip tick at a time. Each boundary moves the
        // bucket clock and fires exactly one tick on every runtime — the manual
        // analogue of a single `tokio::time::advance` firing every node's
        // interval — then drains so the tick and any gossip it triggers fully
        // propagate before the next.
        loop {
            let next_tick = (self.virtual_ms / tick_ms + 1).saturating_mul(tick_ms);
            if next_tick > target {
                break;
            }
            self.virtual_ms = next_tick;
            self.now.set(next_tick);
            self.fire_tick().await;
            self.drain_pending().await;
            self.drain_log(&mut events);
            let now = self.admin_counters().await;
            self.push_tick_events(&prev, &now, &mut events);
            prev = now;
        }

        // A sub-tick remainder moves the clock without firing a heartbeat, so
        // `virtual_ms` and the bucket clock still reach exactly `target`.
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

    /// Fire one gossip tick on every runtime, in node order. Each send queues
    /// in that node's bounded tick channel, so a busy runtime still consumes
    /// the tick on its next idle poll rather than dropping it. A closed channel
    /// (the runtime stopped) is ignored — the tick simply has nowhere to land.
    async fn fire_tick(&self) {
        for node in &self.nodes {
            let _ = node.tick_tx.send(()).await;
        }
    }

    /// Drain the shims' raw events, stamping each with the current virtual
    /// time and gossip-tick index.
    fn drain_log(&self, events: &mut Vec<Event>) {
        let tick = self.current_tick();
        let virtual_ms = self.virtual_ms;
        let mut log = self.log.borrow_mut();
        events.extend(log.drain(..).map(|kind| Event {
            tick,
            virtual_ms,
            kind,
        }));
    }

    /// Number of yields needed so every spawned runtime task gets polled after
    /// a virtual-time advance. Mirrors `gossip-bench`'s `drain_pending_tasks`:
    /// single-thread tokio polls one task per yield, so a large cluster needs
    /// an explicit drain or it under-polls.
    async fn drain_pending(&self) {
        // Scale with the *live* node count, not the initial config, so a
        // cluster grown by joins is still fully polled.
        let budget = self.nodes.len().saturating_mul(4).max(16);
        for _ in 0..budget {
            tokio::task::yield_now().await;
        }
    }

    /// Pull `(ticks_total, threshold_fires)` from every runtime via its admin
    /// channel. One `oneshot` round-trip per node, done sequentially.
    async fn admin_counters(&self) -> Vec<(u64, u64)> {
        let mut out = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            match admin_snapshot(&node.admin_tx).await {
                Some(snap) => out.push((snap.ticks_total, snap.threshold_fires)),
                None => out.push((0, 0)),
            }
        }
        out
    }

    /// Emit one `Tick`/`ThresholdFire` event per tick each node fired this
    /// sub-step. `threshold_fires` is a subset of `ticks_total`, so the
    /// difference of the two is the heartbeat count.
    fn push_tick_events(&self, prev: &[(u64, u64)], now: &[(u64, u64)], events: &mut Vec<Event>) {
        let tick = self.current_tick();
        let virtual_ms = self.virtual_ms;
        // `prev`/`now` are aligned to `self.nodes` order (both built by
        // iterating it), so position `index` is `self.nodes[index]` — map it to
        // that node's stable id for the emitted event.
        for (index, (before, after)) in prev.iter().zip(now.iter()).enumerate() {
            let node = self.nodes[index].id;
            let total_delta = after.0.saturating_sub(before.0);
            let threshold_delta = after.1.saturating_sub(before.1);
            let heartbeat_delta = total_delta.saturating_sub(threshold_delta);
            for _ in 0..heartbeat_delta {
                events.push(Event {
                    tick,
                    virtual_ms,
                    kind: EventKind::Tick { node },
                });
            }
            for _ in 0..threshold_delta {
                events.push(Event {
                    tick,
                    virtual_ms,
                    kind: EventKind::ThresholdFire { node },
                });
            }
        }
    }

    async fn snapshot(&self) -> ClusterState {
        let mut nodes = Vec::with_capacity(self.nodes.len());
        // Insertion order, so a node's position in `nodes` is its rank — the
        // frontend turns that into a ring slot, while `id` stays its identity.
        for handle in &self.nodes {
            let cells = match cell_dump(&handle.admin_tx).await {
                Some(dump) => self.cells_from_dump(handle.identity, &dump),
                None => Vec::new(),
            };
            let (ticks_total, threshold_fires, peers) = match admin_snapshot(&handle.admin_tx).await
            {
                Some(snap) => (
                    snap.ticks_total,
                    snap.threshold_fires,
                    self.peers_from_snapshot(&snap),
                ),
                None => (0, 0, Vec::new()),
            };
            nodes.push(NodeState {
                id: handle.id,
                aggregate_total: handle.aggregate.total(),
                ticks_total,
                threshold_fires,
                cells,
                peers,
            });
        }
        ClusterState {
            virtual_ms: self.virtual_ms,
            tick: self.current_tick(),
            nodes,
            oracle_total: self.oracle_total,
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

    fn peers_from_snapshot(&self, snap: &AdminSnapshot) -> Vec<PeerView> {
        let addresses = self.addresses.borrow();
        snap.peers
            .iter()
            .map(|peer| PeerView {
                id: addresses.get(&peer.addr).copied(),
                node_id: peer.node_id.map(|id| id.0),
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
