//! Result schema of one study run (`gabion-study/1`). Every field is a pure
//! function of the regime, the seed, and the solution binary; nothing
//! depends on the host's wall clock.

use serde::Serialize;

use super::net::NetCounters;
use super::node::{KeyKind, Sizing};
use super::oracle::{Joiner, WeightedHist, WindowSamples};
use super::regime::Regime;
use super::warnings::WarningReport;

pub const SCHEMA: &str = "gabion-study/2";

#[derive(Debug, Serialize)]
pub struct StudyResult {
    pub schema: &'static str,
    pub regime_id: Box<str>,
    pub seed: u64,
    /// `false` when a health invariant failed; the judge discards the run.
    pub valid: bool,
    pub invalid_reasons: Vec<String>,
    /// Saturation the solution reported without failing an invariant (for
    /// example dirty-ring overflow). The run stays valid; the judge shows
    /// these next to its numbers.
    pub flags: Vec<String>,
    pub regime: Regime,
    pub sizing: Sizing,
    pub windows: Windows,
    pub traffic: TrafficReport,
    pub network: NetworkReport,
    pub staleness: StalenessReport,
    pub convergence: ConvergenceReport,
    pub accuracy: Vec<KeyAccuracy>,
    pub churn: ChurnReport,
    /// Per-phase metrics of the regime's network partition, if it has one.
    pub partition: Option<PartitionReport>,
    pub health: Health,
    pub sim: SimStats,
}

#[derive(Debug, Serialize)]
pub struct Windows {
    pub warmup_s: u64,
    pub measure_s: u64,
    /// Integral of the running-pod count, in pod-seconds.
    pub node_seconds_measure: f64,
    pub node_seconds_run: f64,
    pub mean_nodes_measure: f64,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct TrafficCounts {
    pub offered: u64,
    pub admitted: u64,
    pub rejected: u64,
    /// Rejected although the true cluster-wide window total plus the request
    /// was within the limit.
    pub false_rejects: u64,
    /// Arrived while no pod was running.
    pub unrouted: u64,
}

#[derive(Debug, Serialize)]
pub struct TrafficReport {
    pub run: TrafficCounts,
    pub measure: TrafficCounts,
}

#[derive(Debug, Serialize)]
pub struct NetworkReport {
    pub run: NetCounters,
    pub measure: NetCounters,
    pub measure_bytes_per_node_per_s: f64,
    pub measure_packets_per_node_per_s: f64,
    pub measure_bytes_per_s: f64,
    pub measure_bytes_per_admitted: Option<f64>,
    pub measure_bytes_per_offered: Option<f64>,
    pub run_bytes_per_node_per_s: f64,
}

/// Distribution summary of a signed error `truth − view`. `weight` is
/// seconds of (node, key) exposure for time-weighted distributions and a
/// sample count otherwise. `*_pct_limit` express the value as a percentage
/// of the rule's limit. `mean` is signed; `under_mean` and `over_mean`
/// average the under-count `max(0, error)` and the over-count
/// `max(0, −error)` separately, so an over-counting solution cannot offset
/// its under-count. `over_time_fraction` is the share of weight with an
/// over-count.
#[derive(Debug, Default, Serialize)]
pub struct Dist {
    pub weight: f64,
    pub mean: Option<f64>,
    pub under_mean: Option<f64>,
    pub over_mean: Option<f64>,
    pub over_max: Option<f64>,
    pub over_time_fraction: Option<f64>,
    pub p50: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
    pub min: Option<f64>,
    pub mean_pct_limit: Option<f64>,
    pub p99_pct_limit: Option<f64>,
    pub max_pct_limit: Option<f64>,
}

impl Dist {
    pub fn from_hist(hist: &WeightedHist, limit: u64) -> Self {
        let pct = |v: Option<f64>| v.map(|v| 100.0 * v / limit as f64);
        let mean = hist.mean();
        let p99 = hist.quantile(0.99).map(|v| v as f64);
        let max = hist.max().map(|v| v as f64);
        Self {
            weight: hist.total() as f64 / 1e9,
            mean,
            under_mean: hist.mean_by(|v| v.max(0)),
            over_mean: hist.mean_by(|v| (-v).max(0)),
            over_max: hist.min().map(|v| (-v).max(0) as f64),
            over_time_fraction: hist.fraction(|v| v < 0),
            p50: hist.quantile(0.5).map(|v| v as f64),
            p99,
            max,
            min: hist.min().map(|v| v as f64),
            mean_pct_limit: pct(mean),
            p99_pct_limit: pct(p99),
            max_pct_limit: pct(max),
        }
    }

    pub fn from_samples(samples: &[i64], limit: u64) -> Self {
        let mut hist = WeightedHist::default();
        for &s in samples {
            hist.add(s, 1_000_000_000);
        }
        Self::from_hist(&hist, limit)
    }
}

#[derive(Debug, Serialize)]
pub struct StalenessReport {
    /// Hot and burst keys: demand at or above the limit.
    pub limited: Dist,
    /// Sampled ordinary sites.
    pub sampled: Dist,
    pub residual_limited: Dist,
    pub residual_sampled: Dist,
}

/// Convergence latencies of tracked admitted hits. `mean_ms`..`max_ms`
/// cover converged hits only and are therefore right-censored: read them
/// with `never_fraction`. The `*_incl_never_ms` figures rank every hit,
/// valuing a never-converged hit at the time from its admission until its
/// bucket left the window: a lower bound on its latency, so they are lower
/// bounds too, but comparable across solutions with different
/// never-converged fractions.
#[derive(Debug, Serialize)]
pub struct ConvergenceReport {
    pub converged: u64,
    /// Bucket left the window before every node covered the hit.
    pub never_converged: u64,
    /// Still in flight when the run ended.
    pub pending_at_end: u64,
    pub never_fraction: Option<f64>,
    pub mean_ms: Option<f64>,
    pub p50_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub max_ms: Option<f64>,
    pub mean_incl_never_ms: Option<f64>,
    pub p50_incl_never_ms: Option<f64>,
    pub p99_incl_never_ms: Option<f64>,
}

/// `(mean, p50, p99)` in milliseconds of nanosecond samples, sorted.
fn ms_summary(sorted_ns: &[u64]) -> (Option<f64>, Option<f64>, Option<f64>) {
    let ms = |ns: u64| ns as f64 / 1e6;
    let rank = |q: f64| {
        (!sorted_ns.is_empty()).then(|| {
            let i = ((q * sorted_ns.len() as f64).ceil() as usize).clamp(1, sorted_ns.len());
            ms(sorted_ns[i - 1])
        })
    };
    let mean = (!sorted_ns.is_empty())
        .then(|| sorted_ns.iter().map(|&n| n as f64).sum::<f64>() / sorted_ns.len() as f64 / 1e6);
    (mean, rank(0.5), rank(0.99))
}

impl ConvergenceReport {
    pub fn new(mut latencies_ns: Vec<u64>, censored_ns: &[u64], pending: u64) -> Self {
        latencies_ns.sort_unstable();
        let converged = latencies_ns.len() as u64;
        let never = censored_ns.len() as u64;
        let (mean_ms, p50_ms, p99_ms) = ms_summary(&latencies_ns);
        let max_ms = latencies_ns.last().map(|&n| n as f64 / 1e6);
        latencies_ns.extend_from_slice(censored_ns);
        latencies_ns.sort_unstable();
        let (mean_incl_never_ms, p50_incl_never_ms, p99_incl_never_ms) = ms_summary(&latencies_ns);
        Self {
            converged,
            never_converged: never,
            pending_at_end: pending,
            never_fraction: (converged + never > 0)
                .then(|| never as f64 / (converged + never) as f64),
            mean_ms,
            p50_ms,
            p99_ms,
            max_ms,
            mean_incl_never_ms,
            p50_incl_never_ms,
            p99_incl_never_ms,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct KeyAccuracy {
    pub key: u32,
    pub kind: KeyKind,
    pub limit: u64,
    pub measure: TrafficCounts,
    pub windows: WindowSamples,
    pub over_max_pct_limit: f64,
    pub over_mean_pct_limit: f64,
}

#[derive(Debug, Serialize)]
pub struct ChurnReport {
    pub joiners: Vec<Joiner>,
    pub t95_p50_ms: Option<f64>,
    pub t95_max_ms: Option<f64>,
    pub t99_p50_ms: Option<f64>,
    pub t99_max_ms: Option<f64>,
    pub t_parity_p50_ms: Option<f64>,
    pub t_parity_max_ms: Option<f64>,
    pub never_reached_95: u64,
    pub never_reached_99: u64,
    pub never_reached_parity: u64,
    pub undercount_hit_s_total: f64,
    pub undercount_hit_s_mean: Option<f64>,
}

impl ChurnReport {
    pub fn new(joiners: Vec<Joiner>) -> Self {
        let pick = |f: fn(&Joiner) -> Option<f64>| {
            let mut v: Vec<f64> = joiners.iter().filter_map(f).collect();
            v.sort_by(f64::total_cmp);
            let p50 = (!v.is_empty()).then(|| v[(v.len() - 1) / 2]);
            (p50, v.last().copied(), (joiners.len() - v.len()) as u64)
        };
        let (t95_p50_ms, t95_max_ms, never_reached_95) = pick(|j| j.t95_ms);
        let (t99_p50_ms, t99_max_ms, never_reached_99) = pick(|j| j.t99_ms);
        let (t_parity_p50_ms, t_parity_max_ms, never_reached_parity) = pick(|j| j.t_parity_ms);
        let undercount_hit_s_total = joiners.iter().fold(0.0, |sum, j| sum + j.undercount_hit_s);
        Self {
            undercount_hit_s_mean: (!joiners.is_empty())
                .then(|| undercount_hit_s_total / joiners.len() as f64),
            joiners,
            t95_p50_ms,
            t95_max_ms,
            t99_p50_ms,
            t99_max_ms,
            t_parity_p50_ms,
            t_parity_max_ms,
            never_reached_95,
            never_reached_99,
            never_reached_parity,
            undercount_hit_s_total,
        }
    }
}

/// Metrics of a regime with a network partition, by phase: before the cut,
/// during it, and after the heal (see `phases.rs`).
#[derive(Debug, Serialize)]
pub struct PartitionReport {
    /// The availability zone that was cut off.
    pub zone_az: u8,
    pub at_s: u64,
    pub heal_s: u64,
    pub before: PhaseReport,
    pub during: PhaseReport,
    pub after: PhaseReport,
    /// Mean hits missing per (pod, tracked key) at the heal instant, over
    /// buckets that closed before it: the repair the solution owes.
    pub deficit_at_heal_hits: Option<f64>,
    /// From the heal until no pod that ran at the heal misses a hit of a
    /// bucket that closed before it and is still in the window. `None` when
    /// the run ended first.
    pub time_to_heal_ms: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct PhaseReport {
    pub convergence: SideConvergence,
    pub staleness: SideStaleness,
}

/// Convergence of hits admitted in a phase, by whether the pods that must
/// cover them share the origin's side of the cut.
#[derive(Debug, Serialize)]
pub struct SideConvergence {
    pub same_side: ConvergenceReport,
    pub cross_side: ConvergenceReport,
}

/// Staleness of the pods inside the cut zone and of the rest.
#[derive(Debug, Serialize)]
pub struct SideStaleness {
    pub in_zone: ObserverStaleness,
    pub rest: ObserverStaleness,
}

#[derive(Debug, Serialize)]
pub struct ObserverStaleness {
    pub limited: Dist,
    pub sampled: Dist,
}

/// Health invariants. Any of `cell_store_full_rejects`,
/// `rule_dictionary_full_rejects`, `node_dictionary_full_rejects`,
/// `oversize_packets`, `decode_rejects`, `runtime_exits`, `record_failures`,
/// `foreign_rows`, `snapshot_failures`, or `nodes_never_snapshotted` being
/// non-zero makes the run invalid. Dirty-ring overflow and gabion ERROR
/// events are flagged instead (see [`Health::flags`]).
#[derive(Debug, Default, Serialize)]
pub struct Health {
    pub cell_store_full_rejects: u64,
    pub rule_dictionary_full_rejects: u64,
    pub node_dictionary_full_rejects: u64,
    pub peak_active_cells: u32,
    pub cell_capacity: u32,
    pub oversize_packets: u64,
    pub decode_rejects: u64,
    pub max_send_pending_depth: usize,
    pub runtime_exits: u64,
    pub runtime_exit_errors: Vec<String>,
    pub record_failures: u64,
    pub foreign_rows: u64,
    /// Admin snapshots that could not be sent or were not answered after
    /// the cluster went quiescent.
    pub snapshot_failures: u64,
    /// Nodes asked for a snapshot that never answered one, whose health
    /// counters are therefore unknown.
    pub nodes_never_snapshotted: u64,
    /// WARN and ERROR events gabion logged.
    pub gabion_warnings: WarningReport,
}

impl Health {
    pub fn invalid_reasons(&self) -> Vec<String> {
        let checks = [
            ("cell_store_full_rejects", self.cell_store_full_rejects),
            (
                "rule_dictionary_full_rejects",
                self.rule_dictionary_full_rejects,
            ),
            (
                "node_dictionary_full_rejects",
                self.node_dictionary_full_rejects,
            ),
            ("oversize_packets", self.oversize_packets),
            ("decode_rejects", self.decode_rejects),
            ("runtime_exits", self.runtime_exits),
            ("record_failures", self.record_failures),
            ("foreign_rows", self.foreign_rows),
            ("snapshot_failures", self.snapshot_failures),
            ("nodes_never_snapshotted", self.nodes_never_snapshotted),
        ];
        checks
            .iter()
            .filter(|(_, n)| *n > 0)
            .map(|(name, n)| format!("{name} = {n}"))
            .collect()
    }

    pub fn flags(&self) -> Vec<String> {
        let w = &self.gabion_warnings;
        let mut flags = Vec::new();
        if w.dirty_overflow_nodes > 0 {
            flags.push(format!(
                "dirty_ring_overflow: {} nodes, >= {} entries overwritten",
                w.dirty_overflow_nodes, w.dirty_overflow_lower_bound
            ));
        }
        if w.error_events > 0 {
            flags.push(format!("gabion_error_events = {}", w.error_events));
        }
        flags
    }
}

#[derive(Debug, Serialize)]
pub struct SimStats {
    pub virtual_s: f64,
    pub events: u64,
    pub polls: u64,
}

#[cfg(test)]
mod tests;
