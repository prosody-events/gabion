//! Session configuration handed to [`crate::engine::run_engine`].
//!
//! v1 models **one rule, many keys**: a single `(fingerprint, limit, window,
//! bucket)` rule the user drives with requests for arbitrary keys. A
//! multi-rule mix is a future extension (it becomes a `Vec` here).

use gabion::crdt::CellStoreConfig;
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
    /// Floor on peers contacted per gossip tick; the runtime scales the
    /// actual fanout to the `⌈ln(n)+c⌉` coverage threshold (`n` = peer count).
    /// See [`gabion::gossip::GossipConfig::fanout`].
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

    /// Per-node CRDT capacities for the browser sim, derived from the watched
    /// rule's live-bucket count and the cluster's *growth ceiling*. The single
    /// sizing site: every node ([`crate::engine`]'s initial build and `add_node`
    /// both route through `spawn_node`, which calls this).
    ///
    /// The node-count term is [`MAX_NODES`], **not** `self.nodes`. A
    /// [`gabion::crdt::CellStore`] allocates once and never resizes, and the
    /// visualizer grows the live cluster by live joins up to `MAX_NODES` with no
    /// rebuild (the `live-node-membership-requirement`). Sizing from `self.nodes`
    /// would overflow the node dictionary, peer table, and cell store the moment
    /// a user joined past `config.nodes`, so every cluster is sized for the
    /// ceiling it can grow to — the same guarantee the old per-`spawn_node`
    /// floors gave, now in one expression.
    ///
    /// Unlike the adapters' production configs
    /// ([`gabion::defaults::STORAGE_*`] via `server::config::cell_store_config`
    /// and `nginx::leader::production_cell_store_config`), these carry **no
    /// production floors**: the visualizer watches one rule for a handful of
    /// keys, so even at the ceiling the working set is the cross-node replica set
    /// — `MAX_NODES × live_buckets` cells — a few thousand entries, not the
    /// hundreds of thousands a real deployment sizes for. A small constant
    /// `SLACK` keeps the largest cluster clear of eviction; if a probe ever
    /// overflows a ring, raise `SLACK` here — the one place — not a scattered
    /// floor.
    pub fn cell_store_config(&self, live_buckets: u32) -> CellStoreConfig {
        // Headroom over the exact working set, absorbing churn and the bucket
        // just emerging under "now". One knob for the whole sizing.
        const SLACK: u32 = 2;
        // The ceiling the cluster can grow to (see the doc note) — not
        // `self.nodes`, which is only the *initial* member count.
        let ceiling = MAX_NODES as u32;
        // Each origin contributes its `live_buckets` cells plus the one
        // emerging. A node holds a replica of every origin's set, so the cell
        // store and forwarded-dirty ring both scale with `ceiling × per_origin`.
        let per_origin = live_buckets + 1;
        let cross_node = ceiling
            .saturating_mul(per_origin)
            .saturating_mul(SLACK)
            .max(SLACK);
        CellStoreConfig {
            cell_capacity: cross_node,
            // One watched rule; headroom for an interned default / wire-only rule.
            rule_dictionary_capacity: 4,
            node_dictionary_capacity: (ceiling + SLACK).min(u16::MAX as u32) as u16,
            // Local-origin dirty cells are this node's *own* live buckets —
            // independent of cluster size, so this stays small at any ceiling.
            local_dirty_capacity: (per_origin * SLACK) as usize,
            forwarded_dirty_capacity: cross_node as usize,
            peer_capacity: (ceiling + SLACK).min(u16::MAX as u32) as u16,
        }
    }
}
