//! Leader-thread main: spins up a single-threaded tokio runtime + `LocalSet`,
//! wires the `GossipRuntime` to the SHM aggregate writer, and drains the SHM
//! queue into the runtime's request queue.
//!
//! Every nginx worker runs one of these threads. The SHM lease decides
//! which one leads; the others campaign each lease tick so they can take
//! over when the leader's worker begins a graceful shutdown or exits.
//!
//! `GossipRuntime` is `!Send`, so each leadership term is a `spawn_local`
//! task on a `LocalSet` that polls the gossip runtime and the queue drain
//! in one `select!`, sharing plain `Rc<...>` handles.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use tokio::sync::watch;
use tokio::task::LocalSet;

use gabion::crdt::{CellStore, CellStoreConfig, KeyHash, NodeIdentity, RuleDescriptor};
use gabion::defaults;
use gabion::discovery::{self, DiscoveryConfig, PeerDiscovery, PeerEvent};
use gabion::gossip::{GossipClient, GossipConfig, GossipRuntime, TokioClock, UdpTransport};
use gabion::wire::FrameLimits;

use crate::identity::fresh_incarnation;
use crate::rules::CompiledRules;
use crate::shm::ShmRegion;
use crate::shm::aggregate::ShmAggregateStore;
use crate::shm::lease::now_millis;

/// Events the drain hands the runtime per pass, and the depth of the runtime's
/// request queue. The drain waits for the runtime to take one batch before
/// popping the next, so a backlog waits in SHM, and a queue this shallow
/// recycles tokio's storage instead of allocating.
const DRAIN_BATCH: usize = 32;

/// How often the drain polls an empty SHM queue.
const DRAIN_TICK: Duration = Duration::from_millis(5);

/// How often a worker that doesn't hold the SHM lease tries to take it.
pub const DEFAULT_LEASE_TICK: Duration = Duration::from_millis(300);

/// Lease TTL. The leader renews before every drain pass, so only a
/// leader thread stalled this long loses the lease.
const LEASE_TTL: Duration = Duration::from_secs(1);
const LEASE_TTL_MILLIS: u64 = LEASE_TTL.as_millis() as u64;

/// Ceiling on the back-off between leadership attempts after a leader
/// fails. The back-off starts at the lease TTL, doubles per failure, and
/// starts over once a leader has run at least this long.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Where a lone node's gossip runtime binds when `gabion_gossip_bind` is unset.
const LONE_NODE_BIND: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));

/// Configuration the leader thread needs to bring up its tokio runtime.
#[derive(Clone, Debug)]
pub struct LeaderConfig {
    pub worker_id: u32,
    /// `None` runs this node alone: the leader still folds its hits into
    /// the shared counts, but nothing can reach it to gossip.
    pub gossip_bind: Option<SocketAddr>,
    pub gossip: GossipSettings,
    pub discovery: DiscoveryConfig,
    pub cell_store: CellStoreConfig,
    pub rng_seed: u64,
    pub lease_tick: Duration,
}

impl Default for LeaderConfig {
    fn default() -> Self {
        Self {
            worker_id: 0,
            gossip_bind: None,
            gossip: GossipSettings::default(),
            discovery: DiscoveryConfig::default(),
            cell_store: production_cell_store_config(),
            rng_seed: defaults::random_rng_seed().expect("OS entropy for gossip RNG seed"),
            lease_tick: DEFAULT_LEASE_TICK,
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
            limit_queue_capacity: DRAIN_BATCH,
            tick_interval: self.tick_interval,
            sync_wait: Duration::from_millis(defaults::GOSSIP_SYNC_WAIT_MS),
            broadcast_interval: Duration::from_millis(defaults::GOSSIP_BROADCAST_INTERVAL_MS),
            broadcast_batch_limit: defaults::GOSSIP_BROADCAST_BATCH_LIMIT,
            auth_key: None,
            rng_seed,
            target_err_bps: self.target_err_bps,
            min_emit_interval: self.min_emit_interval,
        }
    }
}

/// A worker's leader thread, and the means to stop it.
pub struct LeaderHandle {
    stop: watch::Sender<bool>,
    thread: std::thread::JoinHandle<()>,
}

impl LeaderHandle {
    /// Ask the thread to stop, then wait for it. A leading thread shuts
    /// its gossip tasks down and releases the lease before it returns, so
    /// another worker can take over as soon as this call does. `Err`
    /// means the thread panicked.
    pub fn stop(self) -> std::thread::Result<()> {
        self.request_stop();
        self.thread.join()
    }

    /// Ask the thread to stop without waiting for it.
    pub fn request_stop(&self) {
        self.stop.send_replace(true);
    }
}

/// Spawn a worker's leader thread. The thread owns its own
/// `current_thread` tokio runtime + `LocalSet` and campaigns for the SHM
/// lease every `lease_tick`: whichever worker holds it drives gossip, and
/// the rest stand by to take over when that worker exits or its leader
/// fails. Call [`LeaderHandle::stop`] before the worker exits.
///
/// Returns the OS error if the thread can't be spawned; the caller is an
/// `extern "C"` hook, so it logs and leaves the worker up without a leader
/// thread rather than panicking across FFI.
pub fn spawn(
    shm: ShmRegion,
    rules: Arc<CompiledRules>,
    config: LeaderConfig,
) -> std::io::Result<LeaderHandle> {
    let (stop, stopped) = watch::channel(false);
    let thread = std::thread::Builder::new()
        .name("gabion-leader".to_string())
        .spawn(move || run_leader(shm, rules, config, stopped))?;
    Ok(LeaderHandle { stop, thread })
}

fn run_leader(
    shm: ShmRegion,
    rules: Arc<CompiledRules>,
    config: LeaderConfig,
    stopped: watch::Receiver<bool>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(
                worker_id = config.worker_id,
                %error,
                "gabion: could not start the leader thread's async runtime, so \
                 this worker will never drive gossip. Requests are still \
                 admitted against the shared SHM aggregate, and another worker \
                 can still lead. Usually the worker hit its file-descriptor or \
                 memory limit; check `worker_rlimit_nofile` and `dmesg`, then \
                 restart nginx.",
            );
            return;
        }
    };
    LocalSet::new().block_on(&runtime, campaign(shm, rules, config, stopped));
    // Dropping the runtime would wait on any `spawn_blocking` DNS lookup the
    // Kubernetes client left behind, holding up the worker's exit.
    runtime.shutdown_background();
}

/// Hold or wait for the SHM lease until told to stop. A worker that wins
/// the lease runs [`leader_main`] until it is stopped, loses the lease,
/// or fails, then releases the lease. After a failure it backs off
/// before campaigning again, so a leader that cannot start does not spin.
async fn campaign(
    shm: ShmRegion,
    rules: Arc<CompiledRules>,
    config: LeaderConfig,
    mut stopped: watch::Receiver<bool>,
) {
    let lease = shm.lease();
    let mut retry_delay = LEASE_TTL;
    let mut announced_follower = false;
    let mut tick = tokio::time::interval(config.lease_tick);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = stopped.wait_for(|stop| *stop) => return,
            _ = tick.tick() => {}
        }
        if !lease.try_acquire(config.worker_id, now_millis(), LEASE_TTL_MILLIS) {
            if !announced_follower {
                tracing::info!(
                    worker_id = config.worker_id,
                    "gabion: worker did not win leader lease; standing by to take over gossip if \
                     the leader exits",
                );
                announced_follower = true;
            }
            continue;
        }

        tracing::info!(
            worker_id = config.worker_id,
            "gabion: worker won leader lease"
        );
        let led_since = tokio::time::Instant::now();
        // A panic ends only this term and is retried like any other failure.
        let term = leader_main(shm, rules.clone(), config.clone(), stopped.clone());
        let outcome = tokio::task::spawn_local(term)
            .await
            .unwrap_or_else(|panic| Err(anyhow::Error::new(panic).context("the leader panicked")));
        lease.release(config.worker_id);
        let Err(error) = outcome else {
            return;
        };
        // A leader that ran for a while was healthy; back off from scratch.
        if led_since.elapsed() >= MAX_RETRY_DELAY {
            retry_delay = LEASE_TTL;
        }
        tracing::error!(
            worker_id = config.worker_id,
            error = format!("{error:#}"),
            retry_in = ?retry_delay,
            "gabion: this worker stopped leading gossip and released the \
             leader lease; it will campaign again after `retry_in`. Requests \
             are still admitted against the shared SHM aggregate, but counts \
             are not gossiped to or from peers until some worker leads. \
             Usually the `gabion_gossip_bind` address is already in use, or \
             the worker was starved long enough to miss its lease renewals. \
             The `error` field names the step that failed.",
        );
        tick.reset_after(retry_delay);
        retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
        announced_follower = false;
    }
}

/// Lead gossip until `stopped` fires (`Ok`) or the gossip runtime or the
/// drain loop ends on its own (`Err`). Both are dropped before this
/// returns, so the gossip socket is closed and nothing renews the lease
/// once the caller releases it.
async fn leader_main(
    shm: ShmRegion,
    rules: Arc<CompiledRules>,
    config: LeaderConfig,
    mut stopped: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    // Stamp a fresh incarnation into the SHM header so peers see a new
    // `(node_id, incarnation)` for this takeover. `fresh_incarnation` has
    // one-second resolution, so step past the previous leader's in case it
    // led within the same second; reusing its identity with an empty
    // `CellStore` would have peers max-merge away this leader's counts.
    let new_incarnation =
        fresh_incarnation().max(shm.header().identity.load_incarnation().saturating_add(1));
    shm.header().identity.store_incarnation(new_incarnation);
    let node_id = shm.header().identity.load_node_id();
    let identity = NodeIdentity::new(gabion::crdt::NodeId(node_id), new_incarnation);

    let mut cell_store = CellStore::<u32>::new(config.cell_store, identity);
    register_rules(&mut cell_store, &rules);
    let gossip_cfg = config
        .gossip
        .clone()
        .into_runtime_config(identity, config.rng_seed);

    // A lone node still needs a socket for the runtime; an ephemeral
    // loopback port can't collide with another nginx on the same host, and
    // there is no one to discover.
    let (bind, peers) = match config.gossip_bind {
        Some(bind) => (bind, discovery_stream(config.discovery).left_stream()),
        None => (LONE_NODE_BIND, futures::stream::empty().right_stream()),
    };
    // Bind before the store empties the count table: a term that cannot
    // start leaves the last counts in force, with no leader to refill them.
    let transport = UdpTransport::bind(bind)
        .await
        .context("bind the gossip socket")?;
    let (gossip_rt, gossip_client) = GossipRuntime::from_parts(
        transport,
        TokioClock::new(),
        gossip_cfg,
        cell_store,
        Rc::new(ShmAggregateStore::new(shm)),
    );
    let drain = drain_loop(gossip_client, shm, config.worker_id);

    tokio::select! {
        _ = stopped.wait_for(|stop| *stop) => Ok(()),
        result = gossip_rt.run(peers) => Err(match result {
            Ok(()) => anyhow::anyhow!("the gossip runtime exited without being asked to"),
            Err(error) => anyhow::Error::new(error).context("the gossip runtime failed"),
        }),
        error = drain => Err(error.context("the drain loop failed")),
    }
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

async fn drain_loop(client: GossipClient<u32>, shm: ShmRegion, worker_id: u32) -> anyhow::Error {
    let lease = shm.lease();
    let queue = shm.queue();
    let stats = shm.stats();
    loop {
        // Renew the lease before popping: a leader that stalled past the TTL
        // and lost it stops here instead of draining beside its successor.
        if !lease.try_acquire(worker_id, now_millis(), LEASE_TTL_MILLIS) {
            return anyhow::anyhow!("worker {worker_id} lost the leader lease to another worker");
        }
        let Ok(permits) = client.reserve(DRAIN_BATCH).await else {
            return anyhow::anyhow!("gossip runtime shut down");
        };
        // `zip` takes a permit before each pop, so no event leaves SHM
        // without a slot to land in.
        let mut drained = 0;
        for (permit, ev) in permits.zip(std::iter::from_fn(|| queue.pop())) {
            permit.record(
                ev.rule_fingerprint,
                KeyHash(ev.key_hash),
                ev.bucket,
                ev.hits,
                ev.rule_limit,
                ev.now_millis,
            );
            drained += 1;
        }
        stats.record_queue_drain(drained);
        if drained == 0 {
            tokio::time::sleep(DRAIN_TICK).await;
        }
    }
}
