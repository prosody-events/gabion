//! Result schema: what every scenario emits. The shape is stable JSON so
//! the Python plot harness can ingest it without re-implementing
//! parsing.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::scenario::Scenario;

/// End-to-end output of one scenario run.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScenarioResult {
    /// The exact scenario we ran. Round-tripped so plot tooling has the
    /// settings without a second file.
    pub scenario: Scenario,
    /// Time-series of cluster state at each `sample_interval` step.
    pub samples: Vec<TickSnapshot>,
    /// Final per-node metrics.
    pub nodes: Vec<NodeMetrics>,
    /// Derived headline metrics — see `Headline` for fields.
    pub headline: Headline,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TickSnapshot {
    /// Virtual-time millis since the scenario started.
    pub t_millis: u64,
    /// Per-node total observed via the local `AggregateStore`. Indexed
    /// by node position in `Scenario.nodes`. For the two-rule mixed
    /// workload, this collapses across rules; per-rule totals live in
    /// `per_node_hot_total` / `per_node_cold_total` when set.
    pub per_node_total: Vec<u64>,
    /// Ground-truth total — every hit ever issued by every workload at
    /// or before `t_millis`. The Demers/Astrolabe convergence metric is
    /// `(max(per_node_total) == ground_truth) AND
    /// (min(per_node_total) == ground_truth)`.
    pub ground_truth_total: u64,
    /// Cumulative bytes sent across all nodes up to this sample.
    pub bytes_sent_total: u64,
    /// Cumulative packets sent across all nodes up to this sample.
    pub packets_sent_total: u64,
    /// Cumulative threshold-fire emit count summed across all nodes.
    /// `0` when the runtime never crossed the per-rule error budget
    /// between the previous tick and this one.
    #[serde(default)]
    pub threshold_fires_total: u64,
    /// Cumulative tick count summed across all nodes (heartbeat + threshold
    /// fires combined). Combined with `threshold_fires_total`, the bench can
    /// derive `heartbeat_fires_total = ticks_total - threshold_fires_total`.
    #[serde(default)]
    pub ticks_total: u64,
    /// Cumulative count of ticks (summed across nodes) that found at least
    /// one dirty cell. Used to compute the *effective* fanout —
    /// `packets_sent_total / dirty_ticks_total` across a sample window.
    #[serde(default)]
    pub dirty_ticks_total: u64,
    /// Per-rule per-node totals for the two-rule mix workload. Empty for
    /// every other workload.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub per_node_hot_total: Vec<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub per_node_cold_total: Vec<u64>,
    /// Ground-truth split for the two-rule mix.
    #[serde(default)]
    pub ground_truth_hot_total: u64,
    #[serde(default)]
    pub ground_truth_cold_total: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct NodeMetrics {
    pub node_index: usize,
    pub final_total: u64,
    pub bytes_sent: u64,
    pub packets_sent: u64,
    pub apply_calls: u64,
    /// Aggregate-store row count at end. CellStore-side row count is on
    /// the runtime, not the store, so this is the per-bucket count.
    pub aggregate_rows: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Headline {
    /// Wall (virtual) millis between the first write and the first
    /// sample at which every node's total equals the ground-truth total.
    /// `None` if the run ended before convergence.
    pub convergence_millis: Option<u64>,
    /// Time-to-convergence expressed in gossip rounds
    /// (`convergence_millis / tick_interval_millis`). The classic
    /// Demers/Karp metric.
    pub convergence_rounds: Option<f64>,
    /// Max divergence at the end of the run (max_total - min_total).
    /// 0 means perfect convergence.
    pub final_divergence: u64,
    /// Cluster-wide bytes / node / second over the entire run.
    pub bytes_per_node_per_second: f64,
    /// Cluster-wide packets / node / second over the entire run.
    pub packets_per_node_per_second: f64,
    /// Median per-node staleness in millis: for each (node, sample), the
    /// number of millis between when ground-truth crossed the threshold
    /// the node now shows and the sample's wall time. Useful when the
    /// workload is sustained.
    pub p50_staleness_millis: Option<u64>,
    pub p95_staleness_millis: Option<u64>,
    /// Average threshold-fire emits per node across the run. Zero when
    /// the workload never crosses the per-rule error budget — for those
    /// scenarios every emit was a heartbeat tick.
    #[serde(default)]
    pub threshold_fires_per_node: f64,
    /// Effective per-tick fanout — the median (p50) and 95th percentile
    /// of `packets_emitted / nodes_with_dirty_cells` across all sample
    /// windows in which at least one node was dirty. With static
    /// `fanout=f`, both numbers sit at `f` in steady state; under burst
    /// the runtime's adaptive widening pushes them up toward `log₂
    /// dirty`. `None` when no sample window contained dirty work.
    #[serde(default)]
    pub effective_fanout_p50: Option<f64>,
    #[serde(default)]
    pub effective_fanout_p95: Option<f64>,
    /// Peak `ground_truth_total - min(per_node_total)` over the run.
    /// For the `error_budget` suite this is the empirical worst-case
    /// staleness operators would see if every node converged to its
    /// laggiest peer.
    #[serde(default)]
    pub max_lag: u64,
    /// Optional extra fields scenarios use to surface their headline
    /// number (e.g. partition `reconvergence_millis`).
    #[serde(default)]
    pub extras: HashMap<String, serde_json::Value>,
}

impl Headline {
    pub fn convergence_rounds_from_millis(
        convergence_millis: Option<u64>,
        tick: Duration,
    ) -> Option<f64> {
        convergence_millis.map(|m| m as f64 / tick.as_millis().max(1) as f64)
    }
}
