//! Session configuration handed to [`crate::engine::run_engine`].
//!
//! v1 models **one rule, many keys**: a single `(fingerprint, limit, window,
//! bucket)` rule the user drives with requests for arbitrary keys. A
//! multi-rule mix is a future extension (it becomes a `Vec` here).

use gabion::defaults;
use serde::{Deserialize, Serialize};

use crate::hex::u128_hex;

/// Largest cluster the visualizer constructs. Bounded so a stray config from
/// the URL or a fuzzed control can't ask the engine to build an unbounded
/// number of runtimes; mirrors the gossip peer-table capacity floor.
pub const MAX_NODES: usize = 256;

/// Deterministic given `(rng_seed, SimConfig, command-script)` — the same
/// triple the shareable URL encodes. Every field has a production-aligned
/// default drawn from [`gabion::defaults`].
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct SimConfig {
    /// Number of cluster members. Clamped to `1..=MAX_NODES` by
    /// [`SimConfig::validate`].
    pub nodes: usize,
    /// Minimum peers contacted per gossip tick (the runtime grows it under
    /// burst). See [`gabion::gossip::GossipConfig::fanout`].
    pub fanout: usize,
    /// Period between proactive gossip ticks, in milliseconds.
    pub tick_interval_ms: u64,
    /// Per-rule threshold-anti-entropy error budget, in basis points.
    pub target_err_bps: u32,
    /// Floor between two threshold-fire emissions, in milliseconds.
    pub min_emit_interval_ms: u64,
    /// The single rule's stable fingerprint (hex across the JS boundary).
    #[serde(with = "u128_hex")]
    pub rule_fingerprint: u128,
    /// Admission limit for the rule.
    pub rule_limit: u64,
    /// Rolling window width, in milliseconds.
    pub rule_window_ms: u32,
    /// Bucket width within the window, in milliseconds.
    pub rule_bucket_ms: u32,
    /// Seed for every node's peer-sampling RNG (each node offsets it by its
    /// index, matching the bench).
    pub rng_seed: u64,
    /// i.i.d. per-link packet drop probability applied to every directed
    /// link at startup. `0.0` is a lossless network.
    pub uniform_loss: f64,
    /// Per-node CRDT cell capacity. Floored to fit one cell per origin.
    pub cell_capacity: u32,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            nodes: 12,
            fanout: defaults::GOSSIP_FANOUT,
            tick_interval_ms: defaults::GOSSIP_TICK_INTERVAL_MILLIS,
            target_err_bps: defaults::GOSSIP_TARGET_ERR_BPS,
            min_emit_interval_ms: defaults::GOSSIP_MIN_EMIT_INTERVAL_MS,
            // A memorable but arbitrary default fingerprint; the UI overrides it.
            rule_fingerprint: 0xC0FE_DEAD_BEEF_BABE_F00D,
            // Viz-friendly defaults: a 10 s window of 1 s buckets reads as a
            // legible row of bucket bars (the production 60 s / 1 s default is
            // 60 thin bands), and a limit of 1 000 is low enough that the
            // no-preset "Tune the cluster" path can cross it within a window.
            // The narrative presets override the limit back to 1 000 000 so a
            // burst still spreads lazily by heartbeat (see `web/.../presets.ts`).
            rule_limit: 1_000,
            rule_window_ms: 10_000,
            rule_bucket_ms: 1_000,
            rng_seed: 0,
            uniform_loss: 0.0,
            cell_capacity: 4_096,
        }
    }
}

/// Rejected configs that the engine cannot honor. Surfaced to the operator
/// (the page) rather than silently clamped, so a typo in a shared URL is
/// visible instead of producing a quietly different simulation.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(
        "cluster size {got} is out of range: the visualizer simulates 1 to \
         {MAX_NODES} nodes. Lower `nodes` in the controls or shared URL."
    )]
    NodeCountOutOfRange { got: usize },
    #[error(
        "rule window ({window_ms} ms) and bucket ({bucket_ms} ms) must both \
         be non-zero and the window must be a whole number of buckets. Adjust \
         `rule_window_ms` / `rule_bucket_ms` in the controls."
    )]
    WindowBucketMismatch { window_ms: u32, bucket_ms: u32 },
    #[error(
        "packet-loss probability {got} is outside 0.0–1.0. Set `uniform_loss` \
         to a fraction (e.g. 0.1 for 10% loss)."
    )]
    LossOutOfRange { got: f64 },
}

impl SimConfig {
    /// Validate the user-facing invariants. Bucket/window math is checked here
    /// because a zero bucket silently disables expiry inside the CRDT
    /// (`RuleDescriptor::default`'s footgun), which would mislead rather than
    /// inform.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.nodes < 1 || self.nodes > MAX_NODES {
            return Err(ConfigError::NodeCountOutOfRange { got: self.nodes });
        }
        if self.rule_window_ms == 0
            || self.rule_bucket_ms == 0
            || !self.rule_window_ms.is_multiple_of(self.rule_bucket_ms)
        {
            return Err(ConfigError::WindowBucketMismatch {
                window_ms: self.rule_window_ms,
                bucket_ms: self.rule_bucket_ms,
            });
        }
        if !(0.0..=1.0).contains(&self.uniform_loss) {
            return Err(ConfigError::LossOutOfRange {
                got: self.uniform_loss,
            });
        }
        Ok(())
    }
}
