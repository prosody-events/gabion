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
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use tokio::sync::watch;
use tokio::task::LocalSet;

use gabion::crdt::{CellStore, KeyHash, NodeIdentity, ValidStorageConfig};
use gabion::discovery::{self, DiscoveryConfig};
use gabion::gossip::{GossipClient, GossipRuntime, TokioClock, UdpTransport, ValidGossipConfig};
use gabion::settings::MIN_LIMIT_QUEUE_CAPACITY;

use crate::rules::CompiledRules;
use crate::shm::ShmRegion;
use crate::shm::aggregate::ShmAggregateStore;
use crate::shm::lease::now_millis;

/// Events the drain hands the runtime per pass: the whole depth of the
/// runtime's request queue under `with_shallow_limit_queue`. The drain waits
/// for the runtime to take one batch before popping the next, so a backlog
/// waits in SHM.
const DRAIN_BATCH: NonZeroUsize = MIN_LIMIT_QUEUE_CAPACITY;

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
    pub gossip: ValidGossipConfig,
    pub discovery: DiscoveryConfig,
    pub cell_store: ValidStorageConfig,
    pub lease_tick: Duration,
}

#[cfg(test)]
mod tests;

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
    // `(node_id, incarnation)` for this takeover. It comes from wall-clock
    // milliseconds and steps past the previous leader's, so a takeover
    // never reuses it: peers would max-merge away this leader's counts into
    // the old incarnation's cells, and credit acks written for the old one.
    let new_incarnation =
        gabion::identity::incarnation(Some(shm.header().identity.load_incarnation()));
    shm.header().identity.store_incarnation(new_incarnation);
    let node_id = shm.header().identity.load_node_id();
    let identity = NodeIdentity::new(gabion::crdt::NodeId(node_id), new_incarnation);

    let cell_store = CellStore::new(config.cell_store, identity, rules.table());

    // A lone node still needs a socket for the runtime; an ephemeral
    // loopback port can't collide with another nginx on the same host, there
    // is no one to discover, and it asks for the smallest buffers, since it
    // sends nothing.
    let (bind, peers, gossip) = match config.gossip_bind {
        Some(bind) => {
            config.gossip.note_default_cluster_id();
            (
                bind,
                discovery::peer_events(config.discovery).left_stream(),
                config.gossip,
            )
        }
        None => (
            LONE_NODE_BIND,
            futures::stream::empty().right_stream(),
            config.gossip.with_minimal_socket_buffers(),
        ),
    };
    // Bind before the store empties the count table: a term that cannot
    // start leaves the last counts in force, with no leader to refill them.
    let transport = UdpTransport::bind(bind, &gossip)
        .await
        .context("bind the gossip socket")?;
    let (gossip_rt, gossip_client) = GossipRuntime::from_parts(
        transport,
        TokioClock::new(),
        // The setting sizes the SHM queue; the runtime's own queue only needs
        // room for one drain batch.
        gossip.with_shallow_limit_queue(),
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

async fn drain_loop(client: GossipClient, shm: ShmRegion, worker_id: u32) -> anyhow::Error {
    let lease = shm.lease();
    let queue = shm.queue();
    let stats = shm.stats();
    loop {
        // Renew the lease before popping: a leader that stalled past the TTL
        // and lost it stops here instead of draining beside its successor.
        if !lease.try_acquire(worker_id, now_millis(), LEASE_TTL_MILLIS) {
            return anyhow::anyhow!("worker {worker_id} lost the leader lease to another worker");
        }
        let Ok(permits) = client.reserve(DRAIN_BATCH.get()).await else {
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
