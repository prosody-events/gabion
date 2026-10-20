//! Leader-thread main: spins up a single-threaded tokio runtime + `LocalSet`,
//! wires the `GossipRuntime` to the SHM aggregate writer, and drains the SHM
//! queue into per-record `GossipClient::record` calls.
//!
//! Every nginx worker runs one of these threads. The SHM lease decides
//! which one leads; the others campaign each lease tick so they can take
//! over when the leader's worker begins a graceful shutdown or exits.
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
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinSet, LocalSet};

use gabion::crdt::{CellStore, CellStoreConfig, KeyHash, NodeIdentity, RuleDescriptor};
use gabion::defaults;
use gabion::discovery::{self, DiscoveryConfig, PeerDiscovery, PeerEvent};
use gabion::gossip::{AdminCommand, GossipClient, GossipConfig, GossipError, GossipRuntime};
use gabion::wire::FrameLimits;

use crate::identity::fresh_incarnation;
use crate::rules::CompiledRules;
use crate::shm::ShmRegion;
use crate::shm::aggregate::ShmAggregateStore;
use crate::shm::lease::now_millis;
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

/// Ceiling on the back-off between leadership attempts after a leader
/// fails. The back-off starts at the lease TTL, doubles per failure, and
/// starts over once a leader has run at least this long.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

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
/// Returns an `io::Error` if the OS refuses the thread spawn (out of
/// memory, `RLIMIT_NPROC`, `EAGAIN` under fork pressure). The previous
/// implementation called `.expect(...)` on the spawn, which would
/// panic — but this function is reached from `gabion_init_process`,
/// an `extern "C"` callback invoked by nginx's worker setup, and a
/// panic across an FFI boundary is UB. Surface the error instead so
/// the caller can log it and degrade gracefully (worker stays up
/// without a leader thread, consistent with gabion's allow-by-default
/// posture when internal machinery falters — see CLAUDE.md).
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
    let ttl_millis = config.lease_ttl.as_millis() as u64;
    let mut retry_delay = config.lease_ttl;
    let mut announced_follower = false;
    let mut tick = tokio::time::interval(config.lease_tick);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = stopped.wait_for(|stop| *stop) => return,
            _ = tick.tick() => {}
        }
        if !lease.try_acquire(config.worker_id, now_millis(), ttl_millis) {
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
        let outcome = leader_main(shm, &rules, &config, &mut stopped).await;
        lease.release(config.worker_id);
        let Err(error) = outcome else {
            return;
        };
        // A leader that ran for a while was healthy; back off from scratch.
        if led_since.elapsed() >= MAX_RETRY_DELAY {
            retry_delay = config.lease_ttl;
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
        tokio::select! {
            _ = stopped.wait_for(|stop| *stop) => return,
            _ = tokio::time::sleep(retry_delay) => {}
        }
        retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
        announced_follower = false;
    }
}

/// Lead gossip until `stopped` fires (`Ok`) or one of the leader's tasks
/// ends on its own (`Err`). Every task is shut down before this returns,
/// so the gossip socket is closed and nothing renews the lease once the
/// caller releases it.
async fn leader_main(
    shm: ShmRegion,
    rules: &CompiledRules,
    config: &LeaderConfig,
    stopped: &mut watch::Receiver<bool>,
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
    register_rules(&mut cell_store, rules);
    // SAFETY: `ShmAggregateStore::new`'s preconditions (see its `# Safety`
    // doc) are upheld:
    // * `shm.aggregate_slots_ptr()` returns the address of slot 0 of the
    //   aggregate region. The nginx master called `ShmRegion::initialize` in
    //   `set_zone` before forking workers, which `ptr::write`s an
    //   `AggregateSlot::empty()` into each of `layout.aggregate_capacity`
    //   slots, so the pointer is non-null, cacheline-aligned (and hence aligned
    //   for `AggregateSlot`), and points at a fully initialized contiguous
    //   array of exactly that many slots.
    // * The `capacity` argument is `shm.layout.aggregate_capacity` — the same
    //   `Layout` value the master used at `initialize` time, so the slot count
    //   agrees with the mapping. `Layout::new` enforces it is a power of two
    //   with `>= 2`, and the total byte length fits in the mapping (well under
    //   `isize::MAX`).
    // * The `MAP_SHARED | MAP_ANONYMOUS` region is held by the nginx master for
    //   the lifetime of the worker process, and this leader thread is joined
    //   before the worker exits, so the mapping outlives `store` (and any
    //   `AggregateTable<'_>` derived from it).
    // * Single-writer: the SHM `LeaderLease` elects at most one leader thread
    //   per worker process at a time, and this function drops `store` and every
    //   task holding a clone of it before `campaign` releases the lease to the
    //   next leader; `ShmAggregateStore` is `!Send + !Sync` (it holds `*mut _`
    //   + `Cell`), so the type system prevents this store from being shared
    //   with another thread. Concurrent worker readers go through the
    //   seqlock/atomic accessors on `AggregateSlot`, which is the
    //   data-race-free protocol the nomicon's "Send and Sync" / atomics rules
    //   require.
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

    let gossip = async move {
        gossip_rt
            .run(peer_events)
            .await
            .map_err(anyhow::Error::from)
    };
    let drain = drain_loop(
        gossip_client,
        shm,
        config.worker_id,
        config.max_inflight,
        config.drain_tick,
    );
    let renewer = lease_renewer(shm, config.worker_id, config.lease_tick, config.lease_ttl);
    let admin = admin_listener(admin_tx, config.admin_bind);

    let mut tasks = JoinSet::new();
    let names = [
        (tasks.spawn_local(gossip).id(), "gossip runtime"),
        (tasks.spawn_local(drain).id(), "drain loop"),
        (tasks.spawn_local(renewer).id(), "lease renewer"),
        (tasks.spawn_local(admin).id(), "admin listener"),
    ];
    let name_of = |id| {
        names
            .iter()
            .find_map(|&(task, name)| (task == id).then_some(name))
            .unwrap_or("leader task")
    };

    let outcome = tokio::select! {
        _ = stopped.wait_for(|stop| *stop) => Ok(()),
        Some(joined) = tasks.join_next_with_id() => Err(match joined {
            Ok((id, Ok(()))) => {
                anyhow::anyhow!("the {} exited without being asked to", name_of(id))
            }
            Ok((id, Err(error))) => error.context(format!("the {} failed", name_of(id))),
            Err(error) => {
                let name = name_of(error.id());
                anyhow::Error::new(error).context(format!("the {name} panicked"))
            }
        }),
    };

    tasks.shutdown().await;
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
    worker_id: u32,
    max_inflight: usize,
    tick: Duration,
) -> anyhow::Result<()> {
    let lease = shm.lease();
    let queue = shm.queue();
    let stats = shm.stats();
    let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
    let mut interval = tokio::time::interval(tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        // The queue allows one consumer. If this thread stalled past the
        // lease TTL and another worker took the lease over, stop before
        // popping rather than waiting for `lease_renewer` to notice.
        if !lease.is_held_by(worker_id) {
            return Err(anyhow::anyhow!(
                "worker {worker_id} lost the leader lease to another worker"
            ));
        }

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
        if !lease.try_acquire(worker_id, now_millis(), ttl.as_millis() as u64) {
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

// Type-level sanity: confirm the Rc impl works for our store at compile time.
const _: fn() = || {
    fn assert_aggregate_store<S: gabion::gossip::AggregateStore<u32>>() {}
    assert_aggregate_store::<Rc<ShmAggregateStore>>();
};
