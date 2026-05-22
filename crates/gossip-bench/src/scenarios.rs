//! Generic scenario runner. Spins up N gossip runtimes on a single
//! `SimRouter`, drives the configured workload, samples store state on
//! a fixed cadence, and produces a `ScenarioResult`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::task::LocalSet;

use gabion::crdt::{
    BucketEpoch, CellStore, CellStoreConfig, Count, DeltaSink, ExpirationSink, KeyHash, NodeId,
    NodeIdentity,
};
use gabion::gossip::sim::{LinkPolicy, SimRouter, sim_advance};
use gabion::gossip::{AggregateStore, GossipClient, GossipConfig, GossipRuntime, TokioClock};
use gabion::wire::FrameLimits;

use crate::metrics::{Headline, NodeMetrics, ScenarioResult, TickSnapshot};
use crate::scenario::{LinkAction, Scenario, ScenarioKind, Workload};
use crate::transport::{CountingHandle, CountingTransport};

const RULE_FINGERPRINT: u128 = 0xC0FE_DEADBEEF_BABE_F00D;
const KEY: u128 = 0x0123_4567_89AB_CDEF;
const BUCKET: BucketEpoch = 1;

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

    let mut nodes: Vec<NodeHandle> = Vec::with_capacity(scenario.nodes);
    for (i, addr) in addrs.iter().enumerate() {
        let identity = NodeIdentity::new(NodeId((i as u128) * 0x100 + 1), 1);
        let store = CellStore::<u32>::new(
            CellStoreConfig {
                cell_capacity: scenario.cell_capacity,
                ..CellStoreConfig::default()
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

        let gossip_cfg = GossipConfig {
            local_identity: identity,
            cluster_id_hash: 0xC1,
            bootstrap_peers: bootstrap,
            fanout: scenario.fanout,
            max_cells_per_tick: scenario.max_cells_per_tick,
            wire_limits: FrameLimits {
                max_payload_bytes: 1400,
                max_cells: scenario.max_cells_per_tick as u32,
            },
            send_queue_capacity: 32,
            limit_queue_capacity: 1024,
            tick_interval: scenario.tick_interval,
            auth_key: None,
            rng_seed: scenario.seed.wrapping_add(i as u64),
        };

        let aggregate = Rc::new(BenchAggregateStore::<u32>::default());

        let (runtime, client) = GossipRuntime::from_parts(
            transport,
            TokioClock::from_millis(0),
            gossip_cfg,
            store,
            aggregate.clone(),
        );
        let join = tokio::task::spawn_local(runtime.run(futures::stream::empty()));

        nodes.push(NodeHandle {
            client,
            counters,
            aggregate,
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
    let mut next_schedule_idx = 0;

    // Initial sample at t=0.
    samples.push(snapshot(0, &nodes, ground_truth));

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
        ground_truth = ground_truth.saturating_add(issued);

        // Advance virtual time in tick-sized chunks so the runtimes get
        // their per-tick gossip exchange. `sim_advance` yields after
        // each step so the runtime task is dispatched.
        let mut remaining = step_end - elapsed;
        while remaining > Duration::ZERO {
            let step = remaining.min(tick);
            sim_advance(step).await;
            remaining -= step;
        }
        elapsed = step_end;
        samples.push(snapshot(elapsed.as_millis() as u64, &nodes, ground_truth));
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
        let join = std::mem::replace(
            &mut node.join,
            tokio::task::spawn_local(async { Ok(()) }),
        );
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
            router.set_link_policy(
                *src,
                *dst,
                LinkPolicy::DropProb { p: uniform_loss },
            );
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

async fn apply_workload(
    workload: &Workload,
    nodes: &[NodeHandle],
    window_start: Duration,
    window_end: Duration,
) -> Result<u64> {
    let mut issued = 0_u64;
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
                        at.as_millis() as u64,
                    )
                    .await?;
                issued = issued.saturating_add(*hits);
            }
        }
        Workload::Sustained { sources, per_tick } => {
            // Treat "per_tick" as "per sample window" for simplicity.
            // Sustained scenarios sample every tick so this matches the
            // intended cadence.
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
                        window_end.as_millis() as u64,
                    )
                    .await?;
                issued = issued.saturating_add(*per_tick);
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
                        t.as_millis() as u64,
                    )
                    .await?;
                issued = issued.saturating_add(*per_burst);
                t = t.checked_add(*interval).unwrap_or(window_end);
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

fn snapshot(t_millis: u64, nodes: &[NodeHandle], ground_truth: u64) -> TickSnapshot {
    let per_node_total: Vec<u64> = nodes.iter().map(|n| n.aggregate.total()).collect();
    let bytes_total: u64 = nodes.iter().map(|n| n.counters.bytes_sent()).sum();
    let packets_total: u64 = nodes.iter().map(|n| n.counters.packets_sent()).sum();
    TickSnapshot {
        t_millis,
        per_node_total,
        ground_truth_total: ground_truth,
        bytes_sent_total: bytes_total,
        packets_sent_total: packets_total,
    }
}

fn compute_headline(
    scenario: &Scenario,
    samples: &[TickSnapshot],
    nodes: &[NodeMetrics],
    duration: Duration,
) -> Headline {
    let convergence_millis = samples.iter().find_map(|s| {
        if s.ground_truth_total > 0
            && s.per_node_total.iter().all(|t| *t == s.ground_truth_total)
        {
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

    Headline {
        convergence_millis,
        convergence_rounds,
        final_divergence,
        bytes_per_node_per_second,
        packets_per_node_per_second,
        p50_staleness_millis,
        p95_staleness_millis,
        extras,
    }
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
