//! Generic scenario runner. Spins up N gossip runtimes on a single
//! `SimRouter`, drives the configured workload, samples store state on
//! a fixed cadence, and produces a `ScenarioResult`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{mpsc, oneshot};
use tokio::task::LocalSet;

use gabion::crdt::{
    BucketEpoch, CellStore, CellStoreConfig, Count, DeltaSink, ExpirationSink, KeyHash, NodeId,
    NodeIdentity,
};
use gabion::gossip::sim::{LinkPolicy, SimRouter, sim_advance};
use gabion::gossip::{
    AdminCommand, AdminSnapshot, AggregateStore, GossipClient, GossipConfig, GossipRuntime,
    TokioClock,
};
use gabion::wire::FrameLimits;

use crate::metrics::{Headline, NodeMetrics, ScenarioResult, TickSnapshot};
use crate::scenario::{LinkAction, Scenario, ScenarioKind, Workload};
use crate::transport::{CountingHandle, CountingTransport};

const RULE_FINGERPRINT: u128 = 0xC0FE_DEAD_BEEF_BABE_F00D;
const KEY: u128 = 0x0123_4567_89AB_CDEF;
const BUCKET: BucketEpoch = 1;
const RULE_LIMIT: u64 = 1_000_000;

// Two-rule mix uses these in addition to the single-rule constants above.
const HOT_RULE_FINGERPRINT: u128 = 0xA110_7B07_7B07_7B07;
const COLD_RULE_FINGERPRINT: u128 = 0xC01D_CAFE_CAFE_CAFE;
const HOT_KEY: u128 = 0x1111_1111_1111_1111;
const COLD_KEY: u128 = 0x2222_2222_2222_2222;

/// Run one scenario to completion and return its `ScenarioResult`.
///
/// Uses `tokio::time::pause()` so the runner can spin many simulated
/// gossip runtimes through their event loops at sub-realtime speed
/// without losing determinism. Each node owns its own `GossipRuntime`
/// driven via `GossipRuntime::from_parts` over a `CountingTransport`
/// wrapping the shared `SimRouter`.
pub async fn run_scenario(scenario: Scenario) -> Result<ScenarioResult> {
    if scenario.nodes < 2 {
        anyhow::bail!("scenario requires at least 2 nodes");
    }
    let local = LocalSet::new();
    let scenario_clone = scenario.clone();
    local
        .run_until(async move { run_inner(scenario_clone).await })
        .await
}

async fn run_inner(scenario: Scenario) -> Result<ScenarioResult> {
    let router = SimRouter::with_channel_capacity(256);

    let addrs: Vec<SocketAddr> = (0..scenario.nodes)
        .map(|i| SocketAddr::from(([127, 0, 0, 1], 40_000 + i as u16)))
        .collect();

    // Apply initial link policy.
    apply_links(&router, &addrs, scenario.network.uniform_loss, &[]);
    for link in &scenario.network.links {
        apply_link(&router, &addrs, link);
    }

    // Match (or exceed) the server's production `StorageConfig`
    // defaults — see `crates/server/src/config.rs::StorageConfig::default`
    // for the floors. The server ships with cell_capacity=4096,
    // node_dict=1024, local_dirty=8192, forwarded_dirty=65536,
    // peer=256. Bench runs scale all of those up further when N
    // demands it: cell_capacity ≥ 4·N (4096 floor) so per-origin cells
    // fit; node_dict ≥ N+16 (1024 floor); peer ≥ N+16 (256 floor).
    let cell_capacity = scenario
        .cell_capacity
        .max(((scenario.nodes as u32).saturating_mul(4)).max(4_096));
    let node_dict_capacity =
        (((scenario.nodes as u32) + 16).max(1_024)).min(u16::MAX as u32) as u16;
    let peer_capacity = (((scenario.nodes as u32) + 16).max(256)).min(u16::MAX as u32) as u16;
    let local_dirty_capacity = (cell_capacity as usize).max(8_192);
    let forwarded_dirty_capacity = ((cell_capacity as usize) * 16).max(65_536);

    let mut nodes: Vec<NodeHandle> = Vec::with_capacity(scenario.nodes);
    for (i, addr) in addrs.iter().enumerate() {
        let identity = NodeIdentity::new(NodeId((i as u128) * 0x100 + 1), 1);
        let store = CellStore::<u32>::new(
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

        let (transport, counters) = CountingTransport::new(router.bind(*addr));

        let bootstrap = addrs
            .iter()
            .copied()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, a)| a)
            .collect();

        // Match (or exceed) the server's production `GossipSettings`
        // defaults: max_cells_per_frame=4096, max_cells_per_tick=4096,
        // send_queue=128, limit_queue=8192, max_payload_bytes=1400.
        // Bench scales the cell-count caps to 4·N when N is large
        // (one cell per known origin per tick); leaves the queue and
        // payload sizes at the production floors.
        let max_cells_per_tick = scenario
            .max_cells_per_tick
            .max((scenario.nodes * 4).max(4_096));
        let max_cells_per_frame = (max_cells_per_tick as u32).max(4_096);

        let gossip_cfg = GossipConfig {
            local_identity: identity,
            cluster_id_hash: 0xC1,
            bootstrap_peers: bootstrap,
            fanout: scenario.fanout,
            max_cells_per_tick,
            wire_limits: FrameLimits {
                // Production caps the UDP datagram at 1400 bytes (the
                // safe IPv4-MSS floor that avoids fragmentation). The
                // codec emits multiple packets per frame when the cell
                // list is bigger than the budget.
                max_payload_bytes: 1_400,
                max_cells: max_cells_per_frame,
            },
            send_queue_capacity: 128,
            limit_queue_capacity: 8_192,
            tick_interval: scenario.tick_interval,
            auth_key: None,
            rng_seed: scenario.seed.wrapping_add(i as u64),
            target_err_bps: scenario
                .target_err_bps
                .unwrap_or(gabion::defaults::GOSSIP_TARGET_ERR_BPS),
            min_emit_interval: scenario.min_emit_interval.unwrap_or_else(|| {
                Duration::from_millis(gabion::defaults::GOSSIP_MIN_EMIT_INTERVAL_MS)
            }),
        };

        let aggregate = Rc::new(BenchAggregateStore::<u32>::default());

        // Each node gets its own admin command channel so the bench can
        // pull threshold-fire / tick / dirty-tick counters out of the
        // runtime on every sample. Capacity of one is enough: the bench
        // is single-threaded and awaits each request before issuing the
        // next.
        let (admin_tx, admin_rx) = mpsc::channel::<AdminCommand>(1);
        let (runtime, client) = GossipRuntime::from_parts_with_admin(
            transport,
            TokioClock::from_millis(0),
            gossip_cfg,
            store,
            aggregate.clone(),
            Some(admin_rx),
        );
        let join = tokio::task::spawn_local(runtime.run(futures::stream::empty()));

        nodes.push(NodeHandle {
            client,
            counters,
            aggregate,
            admin_tx,
            join,
        });
    }

    // Drive the scenario forward in `sample_interval` steps; at each step
    // we (a) snapshot every node's state, (b) apply any scheduled
    // network change whose `at` we just crossed, and (c) issue the
    // workload's writes for the elapsed window.
    let tick = scenario.tick_interval;
    let sample = scenario.sample_interval.max(tick);
    let duration = scenario.duration;

    let mut samples: Vec<TickSnapshot> = Vec::new();
    let mut elapsed = Duration::ZERO;
    let mut ground_truth: u64 = 0;
    let mut ground_truth_hot: u64 = 0;
    let mut ground_truth_cold: u64 = 0;
    let mut next_schedule_idx = 0;
    let is_two_rule = matches!(scenario.workload, Workload::TwoRule { .. });

    // Initial sample at t=0.
    samples.push(snapshot(0, &nodes, ground_truth, 0, 0, is_two_rule).await);

    while elapsed < duration {
        // Compute the workload hits issued in (elapsed, elapsed+sample].
        let step_end = (elapsed + sample).min(duration);

        // Apply any scheduled network change whose `at` is in (elapsed, step_end].
        while next_schedule_idx < scenario.network.schedule.len() {
            let change = &scenario.network.schedule[next_schedule_idx];
            if change.at <= step_end {
                for link in &change.apply {
                    apply_link(&router, &addrs, link);
                }
                next_schedule_idx += 1;
            } else {
                break;
            }
        }

        // Issue workload writes for this window. We slice fine-grained
        // enough that the per-tick workload still lands on its node.
        // For sustained / burst workloads we issue one batch per
        // sample_interval (granularity is fine for the metrics we
        // collect).
        let issued = apply_workload(&scenario.workload, &nodes, elapsed, step_end).await?;
        ground_truth = ground_truth.saturating_add(issued.total);
        ground_truth_hot = ground_truth_hot.saturating_add(issued.hot);
        ground_truth_cold = ground_truth_cold.saturating_add(issued.cold);

        // Advance virtual time in tick-sized chunks so the runtimes get
        // their per-tick gossip exchange. `sim_advance` itself yields
        // exactly once — that's enough at small N but starves the
        // remaining runtimes when N is large (single-thread tokio
        // current-thread). After each advance we additionally yield
        // until the scheduler has nothing else queued, so every
        // runtime's tick arm has a chance to fire before the next
        // virtual step.
        let mut remaining = step_end - elapsed;
        while remaining > Duration::ZERO {
            let step = remaining.min(tick);
            sim_advance(step).await;
            drain_pending_tasks(scenario.nodes).await;
            remaining -= step;
        }
        elapsed = step_end;
        samples.push(
            snapshot(
                elapsed.as_millis() as u64,
                &nodes,
                ground_truth,
                ground_truth_hot,
                ground_truth_cold,
                is_two_rule,
            )
            .await,
        );
    }

    // Shut down every runtime; ignore individual errors — the metric
    // collection happened in-line.
    for node in &nodes {
        let _ = node.client.shutdown().await;
    }
    for node in nodes.iter_mut() {
        // The join future was spawned on the LocalSet; awaiting it
        // signals the runtime completed. We don't surface its
        // error since shutdown is best-effort.
        let join = std::mem::replace(&mut node.join, tokio::task::spawn_local(async { Ok(()) }));
        let _ = join.await;
    }

    let nodes_summary: Vec<NodeMetrics> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| NodeMetrics {
            node_index: i,
            final_total: n.aggregate.total(),
            bytes_sent: n.counters.bytes_sent(),
            packets_sent: n.counters.packets_sent(),
            apply_calls: n.aggregate.apply_calls(),
            aggregate_rows: n.aggregate.rows(),
        })
        .collect();

    let headline = compute_headline(&scenario, &samples, &nodes_summary, duration);
    Ok(ScenarioResult {
        scenario,
        samples,
        nodes: nodes_summary,
        headline,
    })
}

struct NodeHandle {
    client: GossipClient<u32>,
    counters: CountingHandle,
    aggregate: Rc<BenchAggregateStore<u32>>,
    /// Admin command sender used by the bench to snapshot threshold-fire,
    /// total-tick, and dirty-tick counters from the runtime on every
    /// sample. Sent off the hot path (one snapshot every
    /// `sample_interval`), never on the per-request path.
    admin_tx: mpsc::Sender<AdminCommand>,
    join: tokio::task::JoinHandle<Result<(), gabion::gossip::GossipError>>,
}

fn apply_links(
    router: &SimRouter,
    addrs: &[SocketAddr],
    uniform_loss: f64,
    _extra: &[crate::scenario::LinkModel],
) {
    if uniform_loss <= 0.0 {
        return;
    }
    // Bimodal Multicast / SWIM style i.i.d. drop: each packet on each
    // directional link is dropped independently with probability
    // `uniform_loss`. The simulator owns a deterministic per-link
    // splitmix so re-runs replay the same drop pattern.
    for src in addrs {
        for dst in addrs {
            if src == dst {
                continue;
            }
            router.set_link_policy(*src, *dst, LinkPolicy::DropProb { p: uniform_loss });
        }
    }
}

fn apply_link(router: &SimRouter, addrs: &[SocketAddr], link: &crate::scenario::LinkModel) {
    let from = addrs[link.from];
    let to = addrs[link.to];
    let policy = match link.action {
        LinkAction::Pass => LinkPolicy::Pass,
        LinkAction::Block => LinkPolicy::Block,
        LinkAction::DropFirst { count } => LinkPolicy::DropFirst { count },
        LinkAction::DropProb { p } => LinkPolicy::DropProb { p },
    };
    router.set_link_policy(from, to, policy);
}

/// Hits issued during one workload window, split across the rules the
/// scenario tracks. `hot` + `cold` ≤ `total`; for single-rule workloads
/// the entire count lands in `total` and the two splits stay zero.
#[derive(Clone, Copy, Debug, Default)]
struct IssuedHits {
    total: u64,
    hot: u64,
    cold: u64,
}

async fn apply_workload(
    workload: &Workload,
    nodes: &[NodeHandle],
    window_start: Duration,
    window_end: Duration,
) -> Result<IssuedHits> {
    let mut issued = IssuedHits::default();
    match workload {
        Workload::SingleWrite { node, hits, at } => {
            if *at >= window_start && *at < window_end {
                let n = nodes
                    .get(*node)
                    .with_context(|| format!("workload node {node} out of range"))?;
                n.client
                    .record(
                        RULE_FINGERPRINT,
                        KeyHash(KEY),
                        BUCKET,
                        *hits,
                        RULE_LIMIT,
                        at.as_millis() as u64,
                    )
                    .await?;
                issued.total = issued.total.saturating_add(*hits);
            }
        }
        Workload::Sustained {
            sources,
            per_tick,
            rule_limit,
        } => {
            // Treat "per_tick" as "per sample window" for simplicity.
            // Sustained scenarios sample every tick so this matches the
            // intended cadence.
            let limit = rule_limit.unwrap_or(RULE_LIMIT);
            for source in sources {
                let n = nodes
                    .get(*source)
                    .with_context(|| format!("workload source {source} out of range"))?;
                n.client
                    .record(
                        RULE_FINGERPRINT,
                        KeyHash(KEY),
                        BUCKET,
                        *per_tick,
                        limit,
                        window_end.as_millis() as u64,
                    )
                    .await?;
                issued.total = issued.total.saturating_add(*per_tick);
            }
        }
        Workload::Burst {
            node,
            per_burst,
            interval,
        } => {
            // Issue one burst per `interval` that falls inside the
            // window.
            let mut t = ceil_to_multiple(window_start, *interval);
            while t < window_end {
                let n = nodes
                    .get(*node)
                    .with_context(|| format!("workload burst node {node} out of range"))?;
                n.client
                    .record(
                        RULE_FINGERPRINT,
                        KeyHash(KEY),
                        BUCKET,
                        *per_burst,
                        RULE_LIMIT,
                        t.as_millis() as u64,
                    )
                    .await?;
                issued.total = issued.total.saturating_add(*per_burst);
                t = t.checked_add(*interval).unwrap_or(window_end);
            }
        }
        Workload::BurstCompressed {
            node,
            hits,
            at,
            burst_span,
        } => {
            let burst_end = at.checked_add(*burst_span).unwrap_or(window_end);
            if *at < window_end && burst_end > window_start {
                // Distribute `hits` evenly across `burst_span` so virtual
                // time can advance between sub-bursts. Each sub-burst
                // bumps `now_millis` by 1 ms — that's what makes
                // `min_emit_interval` enforceable; if every hit shared a
                // timestamp the floor check would saturate to zero.
                let overlap_start = (*at).max(window_start);
                let overlap_end = burst_end.min(window_end);
                let total_span_ms = burst_span.as_millis().max(1) as u64;
                let overlap_span_ms =
                    overlap_end.saturating_sub(overlap_start).as_millis().max(1) as u64;
                // Hits attributable to this window are a proportional
                // slice of the configured total.
                let in_window = (((*hits) as u128) * (overlap_span_ms as u128)
                    / (total_span_ms as u128)) as u64;
                if in_window > 0 {
                    let n = nodes
                        .get(*node)
                        .with_context(|| format!("burst_compressed node {node} out of range"))?;
                    // Step in 1 ms-spaced sub-bursts so the floor check
                    // sees forward progress. `step_count` caps at the
                    // window span so we don't pile thousands of micro-
                    // writes into one millisecond.
                    let step_count = overlap_span_ms.max(1);
                    let base_now_ms = overlap_start.as_millis() as u64;
                    let per_step = in_window / step_count;
                    let remainder = in_window - per_step * step_count;
                    for step in 0..step_count {
                        let count = per_step + if step < remainder { 1 } else { 0 };
                        if count > 0 {
                            // limit=1 forces ε to saturate at 1 — every
                            // hit crosses the budget. That's the
                            // adversarial shape `min_emit_clamp` is
                            // meant to exercise.
                            n.client
                                .record(
                                    RULE_FINGERPRINT,
                                    KeyHash(KEY),
                                    BUCKET,
                                    count,
                                    1,
                                    base_now_ms + step,
                                )
                                .await?;
                        }
                        // Advance virtual time one millisecond between
                        // every sub-step so the runtime's own clock
                        // tracks `req.now_millis` — that's what makes
                        // `min_emit_interval` enforceable. Without this,
                        // `self.clock.now_millis()` would stay pinned
                        // and the floor check would saturate to zero.
                        gabion::gossip::sim::sim_advance(Duration::from_millis(1)).await;
                    }
                    issued.total = issued.total.saturating_add(in_window);
                }
            }
        }
        Workload::DistinctKeyBurst { node, cells, at } => {
            if *at >= window_start && *at < window_end {
                let n = nodes
                    .get(*node)
                    .with_context(|| format!("distinct_key_burst node {node} out of range"))?;
                // One hit per distinct key — the cardinality of dirty
                // cells produced is `cells`, which is the lever
                // `adaptive_fanout` uses to drive `log₂(dirty)`.
                for i in 0..*cells {
                    n.client
                        .record(
                            RULE_FINGERPRINT,
                            // Mix the index into the high bits so each
                            // call hits a different `KeyHash` slot in
                            // the CellStore.
                            KeyHash(KEY ^ ((i as u128) << 64)),
                            BUCKET,
                            1,
                            RULE_LIMIT,
                            at.as_millis() as u64,
                        )
                        .await?;
                }
                issued.total = issued.total.saturating_add(*cells as u64);
            }
        }
        Workload::TwoRule {
            hot_node,
            hot_per_tick,
            hot_limit,
            cold_node,
            cold_per_interval,
            cold_interval,
            cold_limit,
        } => {
            let hot = nodes
                .get(*hot_node)
                .with_context(|| format!("two_rule hot_node {hot_node} out of range"))?;
            hot.client
                .record(
                    HOT_RULE_FINGERPRINT,
                    KeyHash(HOT_KEY),
                    BUCKET,
                    *hot_per_tick,
                    *hot_limit,
                    window_end.as_millis() as u64,
                )
                .await?;
            issued.hot = issued.hot.saturating_add(*hot_per_tick);
            issued.total = issued.total.saturating_add(*hot_per_tick);

            // Cold trickle: issue exactly once at each `cold_interval`
            // boundary that falls inside this window.
            let cold = nodes
                .get(*cold_node)
                .with_context(|| format!("two_rule cold_node {cold_node} out of range"))?;
            let mut t = ceil_to_multiple(window_start, *cold_interval);
            while t < window_end {
                cold.client
                    .record(
                        COLD_RULE_FINGERPRINT,
                        KeyHash(COLD_KEY),
                        BUCKET,
                        *cold_per_interval,
                        *cold_limit,
                        t.as_millis() as u64,
                    )
                    .await?;
                issued.cold = issued.cold.saturating_add(*cold_per_interval);
                issued.total = issued.total.saturating_add(*cold_per_interval);
                t = t.checked_add(*cold_interval).unwrap_or(window_end);
            }
        }
    }
    Ok(issued)
}

fn ceil_to_multiple(value: Duration, modulus: Duration) -> Duration {
    if modulus.is_zero() {
        return value;
    }
    let v_nanos = value.as_nanos();
    let m_nanos = modulus.as_nanos();
    let rounded = v_nanos.div_ceil(m_nanos) * m_nanos;
    Duration::from_nanos(rounded as u64)
}

/// Yield to the scheduler enough times that every spawned runtime
/// task has had a chance to be polled and either complete its current
/// iteration or block on its next `select!` arm. Single-thread tokio
/// runs one task per yield; without this drain step the simulation
/// under-polls large clusters and underestimates per-tick gossip
/// progress.
///
/// **This is a simulation-only artifact.** Production deployments run
/// one `GossipRuntime` per process (one per nginx pod, one per gabiond
/// pod). Each pod has its own kernel thread plus its own tokio
/// runtime, so the OS scheduler — not this loop — gives every runtime
/// a fair chance to fire its tick. The artifact here is that the
/// in-process simulator co-locates N runtimes onto one single-threaded
/// tokio scheduler. When virtual time advances by `tick_interval`, all
/// N runtime tasks become ready simultaneously, but `yield_now()` only
/// runs ONE before resuming the test driver. Without an explicit drain
/// the simulator under-polls at large N and reports artificially slow
/// convergence (we observed ~55 rounds at N=1024 before adding this
/// drain, vs ~7 rounds with it).
async fn drain_pending_tasks(node_count: usize) {
    // The runtime needs at most one yield per task per tick to complete
    // its iteration. 4× headroom covers iterations that re-await on a
    // socket, plus the time-driven repair-frame composition. The yield
    // loop terminates by the budget — there's no quiescence signal we
    // can poll cheaply on `current_thread`.
    let budget = node_count.saturating_mul(4).max(16);
    for _ in 0..budget {
        tokio::task::yield_now().await;
    }
}

async fn snapshot(
    t_millis: u64,
    nodes: &[NodeHandle],
    ground_truth: u64,
    ground_truth_hot: u64,
    ground_truth_cold: u64,
    is_two_rule: bool,
) -> TickSnapshot {
    let per_node_total: Vec<u64> = nodes.iter().map(|n| n.aggregate.total()).collect();
    let bytes_total: u64 = nodes.iter().map(|n| n.counters.bytes_sent()).sum();
    let packets_total: u64 = nodes.iter().map(|n| n.counters.packets_sent()).sum();

    // Pull threshold/heartbeat/dirty counters out of every runtime via
    // its admin channel. Each request is one `oneshot` round-trip — done
    // sequentially because the runner is single-threaded.
    let mut threshold_fires_total = 0_u64;
    let mut ticks_total = 0_u64;
    let mut dirty_ticks_total = 0_u64;
    for n in nodes {
        if let Some(snap) = admin_snapshot(&n.admin_tx).await {
            threshold_fires_total = threshold_fires_total.saturating_add(snap.threshold_fires);
            ticks_total = ticks_total.saturating_add(snap.ticks_total);
            dirty_ticks_total = dirty_ticks_total.saturating_add(snap.dirty_ticks);
        }
    }

    let (per_node_hot_total, per_node_cold_total) = if is_two_rule {
        (
            nodes
                .iter()
                .map(|n| n.aggregate.total_for_rule(HOT_RULE_FINGERPRINT))
                .collect(),
            nodes
                .iter()
                .map(|n| n.aggregate.total_for_rule(COLD_RULE_FINGERPRINT))
                .collect(),
        )
    } else {
        (Vec::new(), Vec::new())
    };

    TickSnapshot {
        t_millis,
        per_node_total,
        ground_truth_total: ground_truth,
        bytes_sent_total: bytes_total,
        packets_sent_total: packets_total,
        threshold_fires_total,
        ticks_total,
        dirty_ticks_total,
        per_node_hot_total,
        per_node_cold_total,
        ground_truth_hot_total: ground_truth_hot,
        ground_truth_cold_total: ground_truth_cold,
    }
}

/// Pull one `AdminSnapshot` out of a runtime. Returns `None` if the
/// runtime has already shut down or the channel was dropped — the
/// caller treats that as "no new counters to merge", which keeps the
/// final snapshot well-defined even after the shutdown path runs.
async fn admin_snapshot(tx: &mpsc::Sender<AdminCommand>) -> Option<AdminSnapshot> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(AdminCommand::Snapshot { reply: reply_tx })
        .await
        .ok()?;
    reply_rx.await.ok()
}

fn compute_headline(
    scenario: &Scenario,
    samples: &[TickSnapshot],
    nodes: &[NodeMetrics],
    duration: Duration,
) -> Headline {
    let convergence_millis = samples.iter().find_map(|s| {
        if s.ground_truth_total > 0 && s.per_node_total.iter().all(|t| *t == s.ground_truth_total) {
            Some(s.t_millis)
        } else {
            None
        }
    });
    let convergence_rounds =
        Headline::convergence_rounds_from_millis(convergence_millis, scenario.tick_interval);

    let final_sample = samples.last();
    let final_divergence = final_sample
        .map(|s| {
            let max = s.per_node_total.iter().copied().max().unwrap_or(0);
            let min = s.per_node_total.iter().copied().min().unwrap_or(0);
            max - min
        })
        .unwrap_or(0);

    let total_bytes: u64 = nodes.iter().map(|n| n.bytes_sent).sum();
    let total_packets: u64 = nodes.iter().map(|n| n.packets_sent).sum();
    let seconds = duration.as_secs_f64().max(1e-6);
    let n_nodes = scenario.nodes as f64;
    let bytes_per_node_per_second = total_bytes as f64 / n_nodes / seconds;
    let packets_per_node_per_second = total_packets as f64 / n_nodes / seconds;

    // Staleness: for each sample, for each node, the gap between the
    // current sample's t_millis and the earliest sample where ground_truth
    // first reached the value the node now shows. Only meaningful when
    // ground_truth changes over time (Sustained / Burst workloads).
    let (p50_staleness_millis, p95_staleness_millis) = staleness_quantiles(samples);

    // Threshold fires: average per node across the run. Pulled from the
    // last snapshot which carries the cumulative count.
    let threshold_fires_per_node = samples
        .last()
        .map(|s| s.threshold_fires_total as f64 / n_nodes)
        .unwrap_or(0.0);

    // Effective fanout: across each sample window, how many distinct
    // peer-bound packets did the cluster emit per dirty tick? Quantiles
    // are taken over the per-window ratios so a steady-state run reports
    // the same number for p50 and p95.
    let (effective_fanout_p50, effective_fanout_p95) = effective_fanout_quantiles(samples);

    // Max lag: peak `ground_truth - min(per_node_total)` across the run.
    // For the `error_budget` suite this is the empirical bound to
    // compare against `N × ε_R`.
    let max_lag = samples
        .iter()
        .map(|s| {
            let min = s.per_node_total.iter().copied().min().unwrap_or(0);
            s.ground_truth_total.saturating_sub(min)
        })
        .max()
        .unwrap_or(0);

    let mut extras = HashMap::new();
    if scenario.kind == ScenarioKind::Partition {
        // If the run scheduled a heal, record time-to-reconverge from
        // the heal point.
        if let Some(heal) = scenario.network.schedule.first() {
            let heal_millis = heal.at.as_millis() as u64;
            let reconv = samples
                .iter()
                .filter(|s| s.t_millis >= heal_millis)
                .find_map(|s| {
                    if s.ground_truth_total > 0
                        && s.per_node_total.iter().all(|t| *t == s.ground_truth_total)
                    {
                        Some(s.t_millis - heal_millis)
                    } else {
                        None
                    }
                });
            if let Some(reconv) = reconv {
                extras.insert(
                    "reconvergence_millis_after_heal".to_string(),
                    serde_json::Value::from(reconv),
                );
            }
        }
    }

    if scenario.kind == ScenarioKind::HeartbeatThresholdMix {
        // Surface per-rule convergence so the bench can confirm both
        // paths drained: hot via threshold fires, cold via heartbeat.
        if let Some(hot) = samples.iter().find_map(|s| {
            if s.ground_truth_hot_total > 0
                && s.per_node_hot_total
                    .iter()
                    .all(|t| *t >= s.ground_truth_hot_total)
            {
                Some(s.t_millis)
            } else {
                None
            }
        }) {
            extras.insert(
                "hot_convergence_millis".to_string(),
                serde_json::Value::from(hot),
            );
        }
        if let Some(cold) = samples.iter().find_map(|s| {
            if s.ground_truth_cold_total > 0
                && s.per_node_cold_total
                    .iter()
                    .all(|t| *t >= s.ground_truth_cold_total)
            {
                Some(s.t_millis)
            } else {
                None
            }
        }) {
            extras.insert(
                "cold_convergence_millis".to_string(),
                serde_json::Value::from(cold),
            );
        }
    }

    if scenario.kind == ScenarioKind::ErrorBudget {
        // Quote the theoretical bound alongside the empirical one. ε_R
        // = max(1, L_R × bps / 10_000 / N); cluster-wide bound is N × ε_R
        // = max(N, L_R × bps / 10_000). Use the workload's actual
        // rule_limit (Sustained may override the bench default).
        let bps = scenario
            .target_err_bps
            .unwrap_or(gabion::defaults::GOSSIP_TARGET_ERR_BPS) as u64;
        let n = scenario.nodes as u64;
        let l = match &scenario.workload {
            Workload::Sustained {
                rule_limit: Some(l),
                ..
            } => *l,
            _ => RULE_LIMIT,
        };
        let cluster_bound = (l * bps / 10_000).max(n);
        extras.insert(
            "theoretical_max_lag".to_string(),
            serde_json::Value::from(cluster_bound),
        );
        extras.insert("target_err_bps".to_string(), serde_json::Value::from(bps));
        extras.insert("rule_limit".to_string(), serde_json::Value::from(l));
    }

    if scenario.kind == ScenarioKind::MinEmitClamp
        && let Some(floor) = scenario.min_emit_interval
    {
        extras.insert(
            "min_emit_interval_ms".to_string(),
            serde_json::Value::from(floor.as_millis() as u64),
        );
    }

    Headline {
        convergence_millis,
        convergence_rounds,
        final_divergence,
        bytes_per_node_per_second,
        packets_per_node_per_second,
        p50_staleness_millis,
        p95_staleness_millis,
        threshold_fires_per_node,
        effective_fanout_p50,
        effective_fanout_p95,
        max_lag,
        extras,
    }
}

/// Effective fanout = packets emitted between two consecutive sample
/// windows divided by the number of dirty ticks in the same window. We
/// compute one ratio per window in which the cluster did any dirty work
/// at all and then take quantiles. Returns `None` for both quantiles
/// when no window had dirty activity.
fn effective_fanout_quantiles(samples: &[TickSnapshot]) -> (Option<f64>, Option<f64>) {
    if samples.len() < 2 {
        return (None, None);
    }
    let mut ratios: Vec<f64> = Vec::new();
    for window in samples.windows(2) {
        let dirty = window[1]
            .dirty_ticks_total
            .saturating_sub(window[0].dirty_ticks_total);
        if dirty == 0 {
            continue;
        }
        let packets = window[1]
            .packets_sent_total
            .saturating_sub(window[0].packets_sent_total);
        ratios.push(packets as f64 / dirty as f64);
    }
    if ratios.is_empty() {
        return (None, None);
    }
    ratios.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pick = |q: f64| -> f64 {
        let idx = ((ratios.len() as f64) * q).clamp(0.0, (ratios.len() - 1) as f64);
        ratios[idx as usize]
    };
    (Some(pick(0.5)), Some(pick(0.95)))
}

fn staleness_quantiles(samples: &[TickSnapshot]) -> (Option<u64>, Option<u64>) {
    // Per-hit delivery delay: for each ground-truth level L that the
    // workload ever reached, find the virtual time at which ground-truth
    // first reached L (`t_gt`), and for each node the virtual time at
    // which the node's local total first reached L (`t_node`). The
    // staleness sample for that (node, level) is `t_node - t_gt`.
    //
    // This gives "how long after the hit was issued did each node see
    // it" — which is what Astrolabe-style staleness actually measures.
    // It does NOT grow over time after convergence, unlike a naive
    // `t_now - t_first_gt` formulation.
    if samples.is_empty() {
        return (None, None);
    }
    let node_count = samples[0].per_node_total.len();
    let max_gt = samples.last().map(|s| s.ground_truth_total).unwrap_or(0);
    if max_gt == 0 {
        return (None, None);
    }

    let gt_first_reached = |level: u64| -> Option<u64> {
        samples
            .iter()
            .find(|s| s.ground_truth_total >= level)
            .map(|s| s.t_millis)
    };
    let node_first_reached = |node: usize, level: u64| -> Option<u64> {
        samples
            .iter()
            .find(|s| s.per_node_total.get(node).copied().unwrap_or(0) >= level)
            .map(|s| s.t_millis)
    };

    // Sample at every level the ground truth actually visited. For a
    // single write that's just one point; for sustained workloads it's
    // every per-tick increment.
    let mut levels: Vec<u64> = samples
        .iter()
        .map(|s| s.ground_truth_total)
        .filter(|v| *v > 0)
        .collect();
    levels.sort_unstable();
    levels.dedup();

    let mut lags: Vec<u64> = Vec::new();
    for level in levels {
        let Some(t_gt) = gt_first_reached(level) else {
            continue;
        };
        for node in 0..node_count {
            if let Some(t_node) = node_first_reached(node, level) {
                lags.push(t_node.saturating_sub(t_gt));
            }
        }
    }
    if lags.is_empty() {
        return (None, None);
    }
    lags.sort_unstable();
    let pick = |q: f64| -> u64 {
        let idx = ((lags.len() as f64) * q).clamp(0.0, (lags.len() - 1) as f64);
        lags[idx as usize]
    };
    (Some(pick(0.5)), Some(pick(0.95)))
}

/// In-bench aggregate store. Same shape as the test fixture in
/// `gabion::gossip::tests::InMemoryAggregateStore` but adds a row-count
/// query for the headline metric.
#[derive(Default)]
pub(crate) struct BenchAggregateStore<C: Count> {
    inner: RefCell<HashMap<(u128, KeyHash, BucketEpoch), u64>>,
    apply_calls: RefCell<u64>,
    _marker: std::marker::PhantomData<C>,
}

impl<C: Count> BenchAggregateStore<C> {
    pub fn total(&self) -> u64 {
        self.inner.borrow().values().copied().sum()
    }

    /// Sum of stored counts whose rule fingerprint matches `rule`. Used
    /// by the two-rule mix workload to split hot vs cold totals at
    /// snapshot time without needing to teach the aggregate store about
    /// rule names.
    pub fn total_for_rule(&self, rule: u128) -> u64 {
        self.inner
            .borrow()
            .iter()
            .filter(|((r, ..), _)| *r == rule)
            .map(|(_, c)| *c)
            .sum()
    }

    pub fn apply_calls(&self) -> u64 {
        *self.apply_calls.borrow()
    }

    pub fn rows(&self) -> usize {
        self.inner.borrow().len()
    }
}

impl<C: Count> AggregateStore<C> for BenchAggregateStore<C> {
    fn apply(&self, deltas: &DeltaSink<C>, expirations: &ExpirationSink<C>) {
        *self.apply_calls.borrow_mut() += 1;
        let mut map = self.inner.borrow_mut();
        for i in 0..deltas.len() {
            let key = &deltas.keys[i];
            let v: u64 = deltas.deltas[i].into();
            *map.entry((key.rule_fingerprint, key.key_hash, key.bucket))
                .or_insert(0) += v;
        }
        for i in 0..expirations.len() {
            let key = &expirations.keys[i];
            let v: u64 = expirations.last_counts[i].into();
            let entry = map
                .entry((key.rule_fingerprint, key.key_hash, key.bucket))
                .or_insert(0);
            *entry = entry.saturating_sub(v);
            if *entry == 0 {
                map.remove(&(key.rule_fingerprint, key.key_hash, key.bucket));
            }
        }
    }
}
