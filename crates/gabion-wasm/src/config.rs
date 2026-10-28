//! Session configuration handed to [`crate::engine::run_engine`].
//!
//! v1 models **one rule, many keys**: a single `(limit, window, bucket)` rule
//! the user drives with requests for arbitrary keys, built as the adapters
//! build theirs. A multi-rule mix is a future extension (it becomes a `Vec`
//! here).

use std::time::Duration;

use gabion::crdt::{RuleDescriptor, ValidStorageConfig};
use gabion::defaults;
use gabion::gossip::ValidGossipConfig;
use gabion::rules::{EnforcementMode, RuleDef, RuleError, RuleTable};
use gabion::settings::{Setting, SettingError, Settings};
use serde::{Deserialize, Serialize};

use crate::engine::{CLUSTER_ID_HASH, VIZ_MAX_CELLS_PER_TICK};

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
    /// Number of cluster members. Must be in `1..=MAX_NODES`;
    /// [`SimConfig::validate`] rejects anything else.
    pub nodes: usize,
    /// Floor on peers contacted per gossip tick; the runtime scales the
    /// actual fanout to the `⌈ln(n)+c⌉` coverage threshold (`n` = peer count).
    /// See `gossip.fanout` in [`gabion::settings`].
    pub fanout: usize,
    /// Period between proactive gossip ticks, in milliseconds.
    pub tick_interval_ms: u64,
    /// Per-rule threshold-anti-entropy error budget, in basis points.
    pub target_err_bps: u32,
    /// Floor between two threshold-fire emissions, in milliseconds.
    pub min_emit_interval_ms: u64,
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
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            nodes: 12,
            fanout: defaults::GOSSIP_FANOUT,
            tick_interval_ms: defaults::GOSSIP_TICK_INTERVAL_MILLIS,
            target_err_bps: defaults::GOSSIP_TARGET_ERR_BPS,
            min_emit_interval_ms: defaults::GOSSIP_MIN_EMIT_INTERVAL_MS,
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
        }
    }
}

/// Rejected configs that the engine cannot honor. Surfaced to the operator
/// (the page) rather than silently clamped, so a typo in a shared URL is
/// visible instead of producing a quietly different simulation.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(
        "cluster size {got} is out of range: the visualizer simulates 1 to {MAX_NODES} nodes. \
         Lower `nodes` in the controls or shared URL."
    )]
    NodeCountOutOfRange { got: usize },
    #[error(
        "packet-loss probability {got} is outside 0.0–1.0. Set `uniform_loss` to a fraction (e.g. \
         0.1 for 10% loss)."
    )]
    LossOutOfRange { got: f64 },
    #[error(transparent)]
    Setting(#[from] SettingError),
    /// The watched rule, as `RuleDef::build` refuses it for both adapters.
    #[error(transparent)]
    Rule(#[from] RuleError),
}

/// A [`SimConfig`] with every node's gossip and storage configs, built only by
/// [`SimConfig::validate`].
pub struct ValidSimConfig {
    pub(crate) config: SimConfig,
    pub(crate) gossip: ValidGossipConfig,
    pub(crate) storage: ValidStorageConfig,
    /// The watched rule, which every node interns at construction.
    pub(crate) rules: RuleTable,
    /// That rule as the cell store holds it: its fingerprint, limit and
    /// epoch math.
    pub(crate) rule: RuleDescriptor,
}

impl SimConfig {
    /// Validate the user-facing invariants and build every node's gossip and
    /// storage configs and the watched rule, once. The rule goes through
    /// `RuleDef::build`, as the adapters' rules do, so a zero window or
    /// bucket fails here instead of silently disabling expiry.
    pub fn validate(self) -> Result<ValidSimConfig, ConfigError> {
        if self.nodes < 1 || self.nodes > MAX_NODES {
            return Err(ConfigError::NodeCountOutOfRange { got: self.nodes });
        }
        if !(0.0..=1.0).contains(&self.uniform_loss) {
            return Err(ConfigError::LossOutOfRange {
                got: self.uniform_loss,
            });
        }
        let (gossip, storage) = self.settings()?.validate()?;
        let rule = RuleDef {
            name: "watched",
            domain: "viz",
            patterns: &[("key", "*")],
            rate: &format!("{}r/{}ms", self.rule_limit, self.rule_window_ms),
            window: None,
            bucket: Some(Duration::from_millis(self.rule_bucket_ms.into())),
            mode: EnforcementMode::Enforce,
        }
        .build(&storage.cardinality)?;
        let descriptor = rule.descriptor();
        let rules = RuleTable::new(Box::new([rule]), storage.rule_dictionary_capacity())?;
        Ok(ValidSimConfig {
            config: self,
            gossip,
            storage,
            rules,
            rule: descriptor,
        })
    }

    /// The node settings for this session: the controls' gossip tuning and
    /// per-node CRDT capacities derived from the watched rule's live-bucket
    /// count and the cluster's *growth ceiling*, passed through
    /// [`Settings::apply`] so a bad control value reports the same error an
    /// adapter would. The single sizing site: every node ([`crate::engine`]'s
    /// initial build and `add_node`) uses the configs [`Self::validate`]
    /// builds from it.
    ///
    /// The node-count term is [`MAX_NODES`], **not** `self.nodes`. A
    /// [`gabion::crdt::CellStore`] allocates once and never resizes, and the
    /// visualizer grows the live cluster by live joins up to `MAX_NODES` with
    /// no rebuild (the `live-node-membership-requirement`). Sizing from
    /// `self.nodes` would overflow the node dictionary and cell store
    /// the moment a user joined past `config.nodes`, so every
    /// cluster is sized for the ceiling it can grow to.
    ///
    /// Unlike the adapters' production defaults (`Settings::default`), these
    /// carry **no production floors**: the visualizer watches one rule for a
    /// handful of keys, so even at the ceiling the working set is the
    /// cross-node replica set — `MAX_NODES × live_buckets` cells — a few
    /// thousand entries, not the hundreds of thousands a real deployment
    /// sizes for. A small constant `SLACK` keeps the largest cluster clear of
    /// eviction; if a probe ever overflows a ring, raise `SLACK` here — the
    /// one place — not a scattered floor.
    pub fn settings(&self) -> Result<Settings, ConfigError> {
        // Headroom over the exact working set, absorbing churn and the bucket
        // just emerging under "now". One knob for the whole sizing.
        const SLACK: u32 = 2;
        // The ceiling the cluster can grow to (see the doc note) — not
        // `self.nodes`, which is only the *initial* member count.
        let ceiling = MAX_NODES as u32;
        // Each origin contributes its `live_buckets` cells plus the one
        // emerging. A node holds a replica of every origin's set, so the cell
        // store and forwarded-dirty ring both scale with `ceiling ×
        // per_origin`.
        let live_buckets = RuleDescriptor {
            window_millis: self.rule_window_ms,
            bucket_millis: self.rule_bucket_ms,
            ..RuleDescriptor::default()
        }
        .live_buckets();
        let per_origin = live_buckets + 1;
        let cross_node = ceiling
            .saturating_mul(per_origin)
            .saturating_mul(SLACK)
            .max(SLACK);
        let mut settings = Settings::default();
        for (setting, value) in [
            (Setting::GossipFanout, self.fanout.to_string()),
            (
                Setting::GossipTickInterval,
                format!("{}ms", self.tick_interval_ms),
            ),
            (Setting::GossipTargetErrBps, self.target_err_bps.to_string()),
            (
                Setting::GossipMinEmitInterval,
                format!("{}ms", self.min_emit_interval_ms),
            ),
            (Setting::GossipClusterIdHash, CLUSTER_ID_HASH.to_string()),
            (
                Setting::GossipMaxCellsPerTick,
                VIZ_MAX_CELLS_PER_TICK.min(cross_node as usize).to_string(),
            ),
            (Setting::StorageMaxCells, cross_node.to_string()),
            // One watched rule; headroom for an interned default / wire-only
            // rule.
            (Setting::StorageRuleDictionaryCapacity, "4".to_owned()),
            (
                Setting::StorageNodeDictionaryCapacity,
                (ceiling + SLACK).to_string(),
            ),
            // Local-origin dirty cells are this node's *own* live buckets —
            // independent of cluster size, so this stays small at any ceiling.
            (
                Setting::StorageLocalDirtyCapacity,
                (per_origin * SLACK).to_string(),
            ),
            (
                Setting::StorageForwardedDirtyCapacity,
                cross_node.to_string(),
            ),
        ] {
            settings.apply(setting, &value)?;
        }
        Ok(settings)
    }
}

#[cfg(test)]
mod tests;
