//! Regime schema: the data file that fully determines one study scenario.
//!
//! A regime fixes the cluster, the rule, the offered load, the network, and
//! the churn schedule. Together with a seed it determines the offered-load
//! trace and the membership timeline exactly, independent of the solution
//! under test. Regime files live in `crates/gossip-bench/regimes/`.

use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Regime {
    pub id: Box<str>,
    pub description: Box<str>,
    /// Total virtual run length, warmup included.
    pub duration_s: u64,
    /// Leading interval excluded from every windowed metric.
    pub warmup_s: u64,
    /// Initial cluster size.
    pub nodes: u32,
    pub rule: RuleConfig,
    pub traffic: TrafficConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub churn: ChurnConfig,
    /// Site keys the oracle samples for staleness tracking, in addition to
    /// every hot and burst key. Clamped to the site-key population.
    #[serde(default = "default_tracked_sample_keys")]
    pub tracked_sample_keys: u32,
}

fn default_tracked_sample_keys() -> u32 {
    200
}

/// The single enforced rule every key in the regime is limited by.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuleConfig {
    pub limit: u64,
    pub window_ms: u64,
    pub bucket_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrafficConfig {
    /// Aggregate request rate of the ordinary site population. The number of
    /// site keys is `round(site_rate_rps * site_period_ms / 1000)`.
    pub site_rate_rps: f64,
    /// Mean gap between two requests of one site.
    pub site_period_ms: u64,
    /// Each gap is `site_period_ms * U(1 - jitter, 1 + jitter)`.
    pub site_jitter: f64,
    /// Keys whose Poisson demand is `limit_multiple` times the rule's
    /// sustainable rate (`limit / window`).
    #[serde(default)]
    pub hot_keys: Vec<HotKey>,
    /// Keys that send a Poisson burst during one interval.
    #[serde(default)]
    pub bursts: Vec<Burst>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HotKey {
    pub limit_multiple: f64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Burst {
    pub rate_rps: f64,
    pub start_s: u64,
    pub duration_s: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct NetworkConfig {
    /// i.i.d. Bernoulli loss probability per datagram.
    pub loss: f64,
    /// Optional Gilbert-Elliott burst loss, applied per directed link on top
    /// of `loss`.
    pub burst_loss: Option<GilbertElliott>,
    /// Receive-buffer budget per node, in payload bytes. Linux's
    /// `net.core.rmem_default` is 212,992.
    pub rcvbuf_bytes: u64,
    /// One-way latency inside an availability zone: `base + U(0, jitter_max)`.
    pub intra_az_base_ns: u64,
    pub intra_az_jitter_max_ns: u64,
    /// One-way latency across availability zones.
    pub cross_az_base_ns: u64,
    pub cross_az_jitter_max_ns: u64,
    /// Receive-path service time charged per datagram the node reads:
    /// `rx_ns_per_datagram + rx_ns_per_byte × payload`. While a node is
    /// still serving the previous datagram, later arrivals wait in its
    /// receive buffer (and overflow it). Everything else a node does takes
    /// no virtual time.
    pub rx_ns_per_datagram: u64,
    pub rx_ns_per_byte: u64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            loss: 0.0,
            burst_loss: None,
            rcvbuf_bytes: 212_992,
            intra_az_base_ns: 200_000,
            intra_az_jitter_max_ns: 100_000,
            cross_az_base_ns: 800_000,
            cross_az_jitter_max_ns: 400_000,
            rx_ns_per_datagram: 1_500,
            rx_ns_per_byte: 3,
        }
    }
}

/// Two-state Markov loss model (Gilbert 1960, Elliott 1963). Mean loss is
/// `loss_in_bad * p_good_to_bad / (p_good_to_bad + p_bad_to_good)`; mean
/// burst length is `1 / p_bad_to_good` datagrams on one link.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GilbertElliott {
    pub p_good_to_bad: f64,
    pub p_bad_to_good: f64,
    pub loss_in_bad: f64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ChurnConfig {
    pub rollouts: Vec<Rollout>,
    pub crashes: Vec<Crash>,
    pub partitions: Vec<Partition>,
}

/// Replace `count` pods: replacement `i` starts at `at_s + i * interval_ms
/// + U(0, 1 s)` with a fresh identity and address; the oldest surviving
/// initial pod leaves gracefully `overlap_ms` after its replacement starts.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Rollout {
    pub at_s: u64,
    pub count: u32,
    pub interval_ms: u64,
    pub overlap_ms: u64,
}

/// One pod stops silently at `at_s`; discovery removes it 10 s later.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Crash {
    pub at_s: u64,
}

/// Datagrams between availability zone `az` and the rest of the cluster are
/// dropped during `[at_s, heal_s)`.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Partition {
    pub at_s: u64,
    pub heal_s: u64,
    pub az: u8,
}

impl Regime {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read regime file {}", path.display()))?;
        let regime: Self = serde_json::from_str(&raw)
            .with_context(|| format!("parse regime file {}", path.display()))?;
        regime.validate()?;
        Ok(regime)
    }

    pub fn validate(&self) -> Result<()> {
        if self.nodes == 0 {
            bail!("regime {}: `nodes` must be at least 1", self.id);
        }
        if self.warmup_s >= self.duration_s {
            bail!("regime {}: `warmup_s` must be below `duration_s`", self.id);
        }
        let rule = self.rule;
        if rule.bucket_ms == 0 || rule.window_ms < rule.bucket_ms || rule.limit == 0 {
            bail!(
                "regime {}: the rule needs limit > 0 and window_ms >= bucket_ms > 0",
                self.id
            );
        }
        if !(0.0..1.0).contains(&self.traffic.site_jitter) {
            bail!("regime {}: `site_jitter` must be in [0, 1)", self.id);
        }
        if self.traffic.site_period_ms == 0 {
            bail!("regime {}: `site_period_ms` must be positive", self.id);
        }
        if !(0.0..=1.0).contains(&self.network.loss) {
            bail!("regime {}: `network.loss` must be in [0, 1]", self.id);
        }
        // The per-phase metrics are defined for one cut.
        let partitions = &self.churn.partitions;
        if partitions.len() > 1 || partitions.iter().any(|p| p.at_s >= p.heal_s) {
            bail!(
                "regime {}: `churn.partitions` takes at most one partition, with at_s < heal_s",
                self.id
            );
        }
        Ok(())
    }

    pub fn live_buckets(&self) -> u32 {
        self.rule.window_ms.div_ceil(self.rule.bucket_ms).max(1) as u32
    }

    pub fn site_keys(&self) -> u32 {
        (self.traffic.site_rate_rps * self.traffic.site_period_ms as f64 / 1000.0).round() as u32
    }

    /// The highest offered request rate at any instant: every site, every
    /// hot key, and every burst that overlaps another.
    pub fn peak_offered_rps(&self) -> f64 {
        let sustainable = self.rule.limit as f64 * 1000.0 / self.rule.window_ms as f64;
        let hot: f64 = self
            .traffic
            .hot_keys
            .iter()
            .map(|h| h.limit_multiple * sustainable)
            .sum();
        let bursts: f64 = self.traffic.bursts.iter().map(|b| b.rate_rps).sum();
        self.traffic.site_rate_rps + hot + bursts
    }
}
