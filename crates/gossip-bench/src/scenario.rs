//! Scenario specification: the data we hand the runner and persist
//! alongside results so a graph can be re-derived from the JSON output.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// One end-to-end experiment. Persisted as JSON in the scenario file and
/// echoed into the result so the plot harness knows what it's looking at.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Scenario {
    /// Free-form scenario identifier — appears in result rows for the plot
    /// harness to group/colour by.
    pub name: String,
    /// Cluster size N.
    pub nodes: usize,
    /// Per-node `GossipConfig.fanout`.
    pub fanout: usize,
    /// Per-node `GossipConfig.tick_interval`.
    #[serde(with = "humantime_serde")]
    pub tick_interval: Duration,
    /// Hard duration cap for the run; the runner exits at or before this
    /// virtual-time deadline.
    #[serde(with = "humantime_serde")]
    pub duration: Duration,
    /// Sampling cadence — how often the runner snapshots store state for
    /// the time-series plots.
    #[serde(with = "humantime_serde")]
    pub sample_interval: Duration,
    /// Network conditions across the run (or across phases of the run).
    pub network: NetworkModel,
    /// Workload driving the cluster.
    pub workload: Workload,
    /// What flavor of metrics this scenario emits as its headline. Other
    /// metrics are always computed; this is the lens the plot harness
    /// uses for the summary view.
    pub kind: ScenarioKind,
    /// Deterministic seed for sim-side coin flips (peer sampling, etc.).
    /// Use the same seed across runs to make A/B comparisons noise-free.
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// CRDT cell-store capacity per node.
    #[serde(default = "default_cell_capacity")]
    pub cell_capacity: u32,
    /// Per-tick gossip frame ceiling — also stored on `GossipConfig`.
    #[serde(default = "default_max_cells_per_tick")]
    pub max_cells_per_tick: usize,
}

fn default_seed() -> u64 {
    0x9E37_79B9_7F4A_7C15
}

fn default_cell_capacity() -> u32 {
    256
}

fn default_max_cells_per_tick() -> usize {
    256
}

/// The metric this scenario is "about". Drives the headline summary in
/// the JSON result and the plot the harness produces.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioKind {
    /// Demers-style: how long until every node has the same total?
    Convergence,
    /// Bimodal Multicast: bytes / packets per node per second.
    NetworkCost,
    /// SWIM-style failure tolerance: convergence under message loss.
    LossTolerance,
    /// SWIM-style partition + heal: time-to-reconverge after a split.
    Partition,
    /// Astrolabe-style staleness: median local lag under sustained writes.
    Staleness,
    /// Karp / Bimodal: convergence as a function of cluster size.
    ScaleN,
}

/// Per-link network conditions. Pairs are referenced by node *index*
/// (0..N) — the runner translates indices to sim socket addresses.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct NetworkModel {
    /// Loss probability applied uniformly to every link. 0.0 = lossless.
    #[serde(default)]
    pub uniform_loss: f64,
    /// Optional per-link overrides. Useful for partition scenarios.
    #[serde(default)]
    pub links: Vec<LinkModel>,
    /// Schedule of policy changes during the run, e.g. healing a partition
    /// at t=5s. Applied in order; each entry's `at` is wall-clock virtual
    /// time relative to scenario start.
    #[serde(default)]
    pub schedule: Vec<ScheduledNetworkChange>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct LinkModel {
    pub from: usize,
    pub to: usize,
    pub action: LinkAction,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkAction {
    Pass,
    Block,
    /// Drop the first `count` packets, then pass.
    DropFirst {
        count: u32,
    },
    /// i.i.d. Bernoulli drop with probability `p`.
    DropProb {
        p: f64,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScheduledNetworkChange {
    #[serde(with = "humantime_serde")]
    pub at: Duration,
    pub apply: Vec<LinkModel>,
}

/// Workload driving the cluster. All workloads share the same rule
/// fingerprint and key so the convergence metric is well-defined.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "shape", rename_all = "snake_case")]
pub enum Workload {
    /// Single write at node 0 at t=`at`. Used for the canonical
    /// Demers-style convergence experiment.
    SingleWrite {
        node: usize,
        hits: u64,
        #[serde(with = "humantime_serde")]
        at: Duration,
    },
    /// `per_tick` writes per source node per tick — the steady-state
    /// workload used by Astrolabe-style staleness measurements.
    Sustained { sources: Vec<usize>, per_tick: u64 },
    /// Burst at one node every `interval`. Used to stress the gossip
    /// pipeline.
    Burst {
        node: usize,
        per_burst: u64,
        #[serde(with = "humantime_serde")]
        interval: Duration,
    },
}
