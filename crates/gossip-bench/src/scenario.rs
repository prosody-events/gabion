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
    /// Per-rule error budget in basis points of the rule limit, used for
    /// threshold-triggered anti-entropy. `None` means: use
    /// `gabion::defaults::GOSSIP_TARGET_ERR_BPS` (100 bps = 1 %). Lower
    /// values fire emits sooner and burn more bandwidth; higher values
    /// let local error accumulate before replicating.
    #[serde(default)]
    pub target_err_bps: Option<u32>,
    /// Minimum gap between two threshold-fire emissions. `None` means:
    /// use `gabion::defaults::GOSSIP_MIN_EMIT_INTERVAL_MS` (5 ms). Caps
    /// worst-case bandwidth when the error budget saturates under
    /// adversarial request rates.
    #[serde(default, with = "humantime_serde::option")]
    pub min_emit_interval: Option<Duration>,
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
    /// Kermarrec/Massoulié/Ganesh-style: the per-tick fanout the runtime
    /// picks is the coverage threshold `⌈ln(n)+c⌉`, a function of cluster
    /// size — flat as the dirty-set cardinality changes, scaling with `n`.
    CoverageFanout,
    /// Olston / Sharfman-style: bandwidth and max lag as the per-rule
    /// `target_err_bps` budget changes. Confirms the cluster-wide error
    /// bound `N × ε_R` and shows the bandwidth/accuracy trade.
    ErrorBudget,
    /// Adversarial saturating write rate: confirms `min_emit_interval`
    /// caps worst-case emit rate while the cluster still converges
    /// after the burst.
    MinEmitClamp,
    /// One cold rule (well under ε) + one hot rule (saturating ε)
    /// running concurrently: both must converge — hot via threshold
    /// fires, cold via the proactive heartbeat.
    HeartbeatThresholdMix,
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

/// Workload driving the cluster. Single-rule workloads share the same
/// rule fingerprint and key so the convergence metric is well-defined;
/// `TwoRule` exposes a hot/cold pair with distinct fingerprints so the
/// `heartbeat_threshold_mix` scenario can measure both code paths in one
/// run.
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
    /// `rule_limit` overrides the bench's default for scenarios that
    /// need to exercise the per-rule error budget at a different
    /// `pending > ε` crossing point.
    Sustained {
        sources: Vec<usize>,
        per_tick: u64,
        #[serde(default)]
        rule_limit: Option<u64>,
    },
    /// Burst at one node every `interval`. Used to stress the gossip
    /// pipeline.
    Burst {
        node: usize,
        per_burst: u64,
        #[serde(with = "humantime_serde")]
        interval: Duration,
    },
    /// Burst at one node, but compressed into a tight window of size
    /// `burst_span` starting at `at`. The bench issues `hits` writes
    /// distributed across `burst_span` so virtual time has a chance to
    /// advance between them — used by `min_emit_clamp` to drive the
    /// runtime under an adversarial sustained rate.
    BurstCompressed {
        node: usize,
        hits: u64,
        #[serde(with = "humantime_serde")]
        at: Duration,
        #[serde(with = "humantime_serde")]
        burst_span: Duration,
    },
    /// Issue `cells` distinct-key writes at one node in a single
    /// instant. Each write lands in its own CRDT cell — that's what
    /// makes the local dirty ring's cardinality jump to `cells` in one
    /// step. The `coverage_fanout` suite varies `cells` to confirm the
    /// per-tick fanout is *independent* of dirty-set volume.
    DistinctKeyBurst {
        node: usize,
        cells: u32,
        #[serde(with = "humantime_serde")]
        at: Duration,
    },
    /// Two-rule workload for `heartbeat_threshold_mix`. The `hot` rule
    /// receives a saturating burst that should fire the threshold path;
    /// the `cold` rule receives a slow trickle that should ride the
    /// proactive heartbeat. Both rules use the same node so we can
    /// observe both gossip paths in one run.
    TwoRule {
        hot_node: usize,
        hot_per_tick: u64,
        hot_limit: u64,
        cold_node: usize,
        cold_per_interval: u64,
        #[serde(with = "humantime_serde")]
        cold_interval: Duration,
        cold_limit: u64,
    },
}
