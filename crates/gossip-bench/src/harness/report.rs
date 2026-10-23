//! Result schema of one study run (`gabion-study/1`). Every field is a pure
//! function of the regime, the seed, and the solution binary; nothing
//! depends on the host's wall clock.

use serde::Serialize;

use super::net::NetCounters;
use super::node::{KeyKind, Sizing};
use super::oracle::{Joiner, WeightedHist, WindowSamples};
use super::regime::Regime;

pub const SCHEMA: &str = "gabion-study/1";

#[derive(Debug, Serialize)]
pub struct StudyResult {
    pub schema: &'static str,
    pub regime_id: Box<str>,
    pub seed: u64,
    /// `false` when a health invariant failed; the judge discards the run.
    pub valid: bool,
    pub invalid_reasons: Vec<String>,
    pub regime: Regime,
    pub sizing: Sizing,
    pub windows: Windows,
    pub traffic: TrafficReport,
    pub network: NetworkReport,
    pub staleness: StalenessReport,
    pub convergence: ConvergenceReport,
    pub accuracy: Vec<KeyAccuracy>,
    pub churn: ChurnReport,
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

/// Distribution summary. `weight` is seconds of (node, key) exposure for
/// time-weighted distributions and a sample count otherwise. `*_pct_limit`
/// express the value as a percentage of the rule's limit.
#[derive(Debug, Default, Serialize)]
pub struct Dist {
    pub weight: f64,
    pub mean: Option<f64>,
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
}

impl ConvergenceReport {
    pub fn new(mut latencies_ns: Vec<u64>, never: u64, pending: u64) -> Self {
        latencies_ns.sort_unstable();
        let ms = |ns: u64| ns as f64 / 1e6;
        let rank = |q: f64| {
            (!latencies_ns.is_empty()).then(|| {
                let i =
                    ((q * latencies_ns.len() as f64).ceil() as usize).clamp(1, latencies_ns.len());
                ms(latencies_ns[i - 1])
            })
        };
        let converged = latencies_ns.len() as u64;
        Self {
            converged,
            never_converged: never,
            pending_at_end: pending,
            never_fraction: (converged + never > 0)
                .then(|| never as f64 / (converged + never) as f64),
            mean_ms: (converged > 0).then(|| {
                latencies_ns.iter().map(|&n| n as f64).sum::<f64>() / converged as f64 / 1e6
            }),
            p50_ms: rank(0.5),
            p99_ms: rank(0.99),
            max_ms: latencies_ns.last().map(|&n| ms(n)),
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

/// Health invariants. Any of `cell_store_full_rejects`,
/// `rule_dictionary_full_rejects`, `node_dictionary_full_rejects`,
/// `oversize_packets`, `runtime_exits`, `record_failures`, or `foreign_rows`
/// being non-zero makes the run invalid.
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
            ("runtime_exits", self.runtime_exits),
            ("record_failures", self.record_failures),
            ("foreign_rows", self.foreign_rows),
        ];
        checks
            .iter()
            .filter(|(_, n)| *n > 0)
            .map(|(name, n)| format!("{name} = {n}"))
            .collect()
    }
}

#[derive(Debug, Serialize)]
pub struct SimStats {
    pub virtual_s: f64,
    pub events: u64,
    pub polls: u64,
}
