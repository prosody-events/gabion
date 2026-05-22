//! Leader-thread main: spins up a single-threaded tokio runtime + `LocalSet`,
//! wires the `GossipRuntime` to the SHM aggregate writer, and drains the SHM
//! queue into per-record `GossipClient::record` calls.
//!
//! `GossipRuntime` is `!Send`, so its select loop runs on a `LocalSet`
//! `spawn_local` task. The drain task lives next to it on the same
//! `LocalSet` so the two communicate via plain `Rc<...>` shares.

use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::task::LocalSet;

use gabion::crdt::{CellStore, CellStoreConfig, KeyHash, NodeIdentity, RuleDescriptor};
use gabion::defaults;
use gabion::discovery::{self, DiscoveryConfig, PeerDiscovery, PeerEvent};
use gabion::gossip::{AdminCommand, GossipClient, GossipConfig, GossipError, GossipRuntime};
use gabion::wire::FrameLimits;

use crate::identity::fresh_incarnation;
use crate::rules::CompiledRules;
use crate::shm::ShmRegion;
use crate::shm::aggregate::ShmAggregateStore;
use crate::shm::queue::QueueEvent;

/// Drain batch size: how many records the leader keeps in flight against the
/// gossip runtime simultaneously.
pub const DEFAULT_MAX_INFLIGHT: usize = 32;

/// Polling interval for the queue drain when in-flight slots are empty.
pub const DEFAULT_DRAIN_TICK: Duration = Duration::from_millis(5);

/// Per-cycle tick interval at which the leader thread renews its SHM lease.
pub const DEFAULT_LEASE_TICK: Duration = Duration::from_millis(300);

/// Default lease TTL — three lease ticks so a renew can miss once without
/// flipping the leadership.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(1);

/// Configuration the leader thread needs to bring up its tokio runtime.
#[derive(Clone, Debug)]
pub struct LeaderConfig {
    pub worker_id: u32,
    pub gossip_bind: SocketAddr,
    pub gossip: GossipSettings,
    pub discovery: DiscoveryConfig,
    pub cell_store: CellStoreConfig,
    pub rng_seed: u64,
    pub admin_bind: Option<SocketAddr>,
    pub max_inflight: usize,
    pub drain_tick: Duration,
    pub lease_tick: Duration,
    pub lease_ttl: Duration,
    pub identity_seed: Option<Box<str>>,
}

impl Default for LeaderConfig {
    fn default() -> Self {
        Self {
            worker_id: 0,
            gossip_bind: "0.0.0.0:0".parse().expect("ipv4"),
            gossip: GossipSettings::default(),
            discovery: DiscoveryConfig::default(),
            cell_store: production_cell_store_config(),
            rng_seed: defaults::random_rng_seed().expect("OS entropy for gossip RNG seed"),
            admin_bind: None,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            drain_tick: DEFAULT_DRAIN_TICK,
            lease_tick: DEFAULT_LEASE_TICK,
            lease_ttl: DEFAULT_LEASE_TTL,
            identity_seed: None,
        }
    }
}

/// Gossip tuning knobs. Mirrors `gabion_server::config::GossipSettings`.
#[derive(Clone, Debug)]
pub struct GossipSettings {
    pub tick_interval: Duration,
    pub fanout: usize,
    pub max_payload_bytes: usize,
    pub max_cells_per_frame: u32,
    pub max_cells_per_tick: usize,
    pub send_queue_capacity: usize,
    pub limit_queue_capacity: usize,
    pub cluster_id_hash: u128,
    /// Per-rule error budget for threshold-triggered anti-entropy, in
    /// basis points of the rule's own limit. See
    /// [`gabion::defaults::GOSSIP_TARGET_ERR_BPS`] for the derivation.
    pub target_err_bps: u32,
    /// Floor between two threshold-fire emissions. See
    /// [`gabion::defaults::GOSSIP_MIN_EMIT_INTERVAL_MS`].
    pub min_emit_interval: Duration,
}

impl Default for GossipSettings {
    /// Matches `gabion_server::config::GossipSettings::default()` so an
    /// out-of-the-box nginx + gabiond cluster gossips with identical
    /// frame/queue tuning.
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_millis(defaults::GOSSIP_TICK_INTERVAL_MILLIS),
            fanout: defaults::GOSSIP_FANOUT,
            max_payload_bytes: defaults::GOSSIP_MAX_PAYLOAD_BYTES,
            max_cells_per_frame: defaults::GOSSIP_MAX_CELLS_PER_FRAME,
            max_cells_per_tick: defaults::GOSSIP_MAX_CELLS_PER_TICK,
            send_queue_capacity: defaults::GOSSIP_SEND_QUEUE_CAPACITY,
            limit_queue_capacity: defaults::GOSSIP_LIMIT_QUEUE_CAPACITY,
            cluster_id_hash: defaults::GOSSIP_CLUSTER_ID_HASH,
            target_err_bps: defaults::GOSSIP_TARGET_ERR_BPS,
            min_emit_interval: Duration::from_millis(defaults::GOSSIP_MIN_EMIT_INTERVAL_MS),
        }
    }
}

pub fn production_cell_store_config() -> CellStoreConfig {
    CellStoreConfig {
        cell_capacity: defaults::STORAGE_MAX_CELLS as u32,
        rule_dictionary_capacity: defaults::STORAGE_RULE_DICTIONARY_CAPACITY,
        node_dictionary_capacity: defaults::STORAGE_NODE_DICTIONARY_CAPACITY,
        local_dirty_capacity: defaults::STORAGE_LOCAL_DIRTY_CAPACITY,
        forwarded_dirty_capacity: defaults::STORAGE_FORWARDED_DIRTY_CAPACITY,
        peer_capacity: defaults::STORAGE_PEER_CAPACITY,
    }
}

#[cfg(test)]
mod tests;

impl GossipSettings {
    pub fn into_runtime_config(self, identity: NodeIdentity, rng_seed: u64) -> GossipConfig {
        GossipConfig {
            local_identity: identity,
            cluster_id_hash: self.cluster_id_hash,
            bootstrap_peers: Vec::new(),
            fanout: self.fanout,
            max_cells_per_tick: self.max_cells_per_tick,
            wire_limits: FrameLimits {
                max_payload_bytes: self.max_payload_bytes,
                max_cells: self.max_cells_per_frame,
            },
            send_queue_capacity: self.send_queue_capacity,
            limit_queue_capacity: self.limit_queue_capacity,
            tick_interval: self.tick_interval,
            auth_key: None,
            rng_seed,
            target_err_bps: self.target_err_bps,
            min_emit_interval: self.min_emit_interval,
        }
    }
}

/// Spawn a leader thread. The thread owns its own `current_thread` tokio
/// runtime + `LocalSet`. Returns the join handle so callers can await
/// termination (e.g. on nginx worker shutdown).
pub fn spawn(
    shm: ShmRegion,
    rules: Arc<CompiledRules>,
    config: LeaderConfig,
) -> std::thread::JoinHandle<anyhow::Result<()>> {
    std::thread::Builder::new()
        .name("gabion-leader".to_string())
        .spawn(move || run_leader(shm, rules, config))
        .expect("spawn gabion-leader thread")
}

fn run_leader(
    shm: ShmRegion,
    rules: Arc<CompiledRules>,
    config: LeaderConfig,
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build leader tokio runtime")?;
    let local = LocalSet::new();
    local.block_on(
        &runtime,
        async move { leader_main(shm, rules, config).await },
    )
}

async fn leader_main(
    shm: ShmRegion,
    rules: Arc<CompiledRules>,
    config: LeaderConfig,
) -> anyhow::Result<()> {
    // Stamp a fresh incarnation into the SHM header so peers see a new
    // `(node_id, incarnation)` for this takeover.
    let new_incarnation = fresh_incarnation();
    shm.header().identity.store_incarnation(new_incarnation);
    let node_id = shm.header().identity.load_node_id();
    let identity = NodeIdentity::new(gabion::crdt::NodeId(node_id), new_incarnation);

    let mut cell_store = CellStore::<u32>::new(config.cell_store, identity);
    register_rules(&mut cell_store, &rules);
    // SAFETY: `ShmAggregateStore::new`'s preconditions (see its `# Safety`
    // doc) are upheld:
    // * `shm.aggregate_slots_ptr()` returns the address of slot 0 of the aggregate
    //   region. The nginx master called `ShmRegion::initialize` in `set_zone`
    //   before forking workers, which `ptr::write`s an `AggregateSlot::empty()`
    //   into each of `layout.aggregate_capacity` slots, so the pointer is non-null,
    //   cacheline-aligned (and hence aligned for `AggregateSlot`), and points at a
    //   fully initialized contiguous array of exactly that many slots.
    // * The `capacity` argument is `shm.layout.aggregate_capacity` — the same
    //   `Layout` value the master used at `initialize` time, so the slot count
    //   agrees with the mapping. `Layout::new` enforces it is a power of two with
    //   `>= 2`, and the total byte length fits in the mapping (well under
    //   `isize::MAX`).
    // * The `MAP_SHARED | MAP_ANONYMOUS` region is held by the nginx master for the
    //   lifetime of the worker process, and this leader thread is joined before the
    //   worker exits, so the mapping outlives `store` (and any `AggregateTable<'_>`
    //   derived from it).
    // * Single-writer: the SHM `LeaderLease` elects at most one leader thread per
    //   worker process at a time; `ShmAggregateStore` is `!Send + !Sync` (it holds
    //   `*mut _` + `Cell`), so the type system prevents this store from being
    //   shared with another thread. Concurrent worker readers go through the
    //   seqlock/atomic accessors on `AggregateSlot`, which is the data-race-free
    //   protocol the nomicon's "Send and Sync" / atomics rules require.
    let store = Rc::new(unsafe {
        ShmAggregateStore::new(shm.aggregate_slots_ptr(), shm.layout.aggregate_capacity)
    });
    let gossip_cfg = config
        .gossip
        .clone()
        .into_runtime_config(identity, config.rng_seed);

    let (admin_tx, admin_rx) = mpsc::channel::<AdminCommand>(8);
    let (gossip_rt, gossip_client) = GossipRuntime::bind_with_admin(
        config.gossip_bind,
        gossip_cfg,
        cell_store,
        store.clone(),
        admin_rx,
    )
    .await
    .context("bind gossip runtime")?;

    let peer_events = discovery_stream(config.discovery.clone());

    let gossip_task = tokio::task::spawn_local(async move { gossip_rt.run(peer_events).await });

    let drain_task = tokio::task::spawn_local(drain_loop(
        gossip_client.clone(),
        shm,
        rules.clone(),
        config.max_inflight,
        config.drain_tick,
    ));

    let lease_task = tokio::task::spawn_local(lease_renewer(
        shm,
        config.worker_id,
        config.lease_tick,
        config.lease_ttl,
    ));

    let admin_task = tokio::task::spawn_local(admin_listener(admin_tx, config.admin_bind));

    let outcome = tokio::select! {
        result = gossip_task => result
            .context("gossip task panicked")?
            .context("gossip runtime exited"),
        result = drain_task => result
            .context("drain task panicked")?
            .context("drain loop exited"),
        result = lease_task => result
            .context("lease task panicked")?,
        result = admin_task => result.context("admin task panicked")?,
    };

    // Best-effort: tell the gossip runtime to shut down.
    let _ = gossip_client.shutdown().await;
    outcome
}

fn discovery_stream(cfg: DiscoveryConfig) -> impl futures::Stream<Item = PeerEvent> {
    discovery::from_config(cfg)
        .peer_events()
        .filter_map(|res| async move {
            match res {
                Ok(event) => Some(event),
                Err(error) => {
                    tracing::warn!(error = %error, "peer discovery error");
                    None
                }
            }
        })
}

fn register_rules(cell_store: &mut CellStore<u32>, rules: &CompiledRules) {
    for compiled in rules.rules() {
        let spec = compiled.rule.spec();
        let _ = cell_store.intern_rule(RuleDescriptor {
            fingerprint: spec.fingerprint,
            window_millis: spec.window_millis.min(u32::MAX as u64) as u32,
            bucket_millis: spec.bucket_millis.min(u32::MAX as u64) as u32,
            limit: spec.limit,
            flags: 0,
            local_rule_id: spec.id,
        });
    }
}

async fn drain_loop(
    client: GossipClient<u32>,
    shm: ShmRegion,
    _rules: Arc<CompiledRules>,
    max_inflight: usize,
    tick: Duration,
) -> anyhow::Result<()> {
    let queue = shm.queue();
    let stats = shm.stats();
    let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        // Refill in_flight from the SHM queue.
        while in_flight.len() < max_inflight {
            let Some(ev) = queue.pop() else { break };
            stats.record_queue_drain(1);
            in_flight.push(record_one(&client, ev));
        }

        if in_flight.is_empty() {
            interval.tick().await;
            continue;
        }

        tokio::select! {
            _ = interval.tick() => {}
            Some(result) = in_flight.next() => {
                if let Err(GossipError::RuntimeShutDown) = result {
                    return Err(anyhow::anyhow!("gossip runtime shut down"));
                }
                // Transient errors (IO, Encode) are logged at trace and
                // otherwise ignored — they do not indicate the leader has
                // lost its grip.
                if let Err(error) = result {
                    tracing::trace!(error = ?error, "drain record error");
                }
            }
        }
    }
}

fn record_one(
    client: &GossipClient<u32>,
    ev: QueueEvent,
) -> impl std::future::Future<Output = Result<(), GossipError>> + use<> {
    let client = client.clone();
    async move {
        client
            .record(
                ev.rule_fingerprint,
                KeyHash(ev.key_hash),
                ev.bucket,
                ev.hits,
                ev.rule_limit,
                ev.now_millis,
            )
            .await
    }
}

async fn lease_renewer(
    shm: ShmRegion,
    worker_id: u32,
    tick: Duration,
    ttl: Duration,
) -> anyhow::Result<()> {
    let lease = shm.lease();
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let now = wall_millis();
        if !lease.try_acquire(worker_id, now, ttl.as_millis() as u64) {
            return Err(anyhow::anyhow!("leader lease lost by worker {worker_id}"));
        }
    }
}

async fn admin_listener(
    _admin_tx: mpsc::Sender<AdminCommand>,
    _bind: Option<SocketAddr>,
) -> anyhow::Result<()> {
    // Admin HTTP is out of scope for v1; the channel sender is kept alive
    // (so the gossip runtime's admin arm doesn't immediately drop) by the
    // owning task. Wait indefinitely.
    std::future::pending::<()>().await;
    unreachable!()
}

fn wall_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// Type-level sanity: confirm the Rc impl works for our store at compile time.
const _: fn() = || {
    fn assert_aggregate_store<S: gabion::gossip::AggregateStore<u32>>() {}
    assert_aggregate_store::<Rc<ShmAggregateStore>>();
};
