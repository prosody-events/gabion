//! Session configuration handed to [`crate::engine::run_engine`].
//!
//! v1 models **one rule, many keys**: a single `(limit, window, bucket)` rule
//! the user drives with requests for arbitrary keys, built as the adapters
//! build theirs. A multi-rule mix is a future extension (it becomes a `Vec`
//! here).

use std::time::Duration;

use gabion::crdt::{RuleDescriptor, ValidStorageConfig};
use gabion::defaults;
use gabion::rules::{EnforcementMode, RuleDef, RuleError, RuleSpec, RuleTable};
use gabion::settings::{Setting, SettingError, Settings};
use gabion::zone::ZoneName;
use serde::{Deserialize, Serialize};

use crate::engine::CLUSTER_ID_HASH;

/// Largest cluster the visualizer constructs. Bounded so a stray config from
/// the URL or a fuzzed control can't ask the engine to build an unbounded
/// number of runtimes; every node's peer table is sized to it.
pub const MAX_NODES: usize = 256;

/// Most zones the visualizer places nodes in; production regions usually have
/// three to six.
pub const MAX_ZONES: usize = 6;

/// Keys each node's count table is budgeted for: the page watches one, and
/// the engine accepts any key a command names, so a few others stay counted.
/// The table is indexed at twice the next power of two of `AGGREGATE_KEYS ×
/// per_origin × SLACK` cells (256 slots at the default 10 s window), so many
/// more keys fit before a write finds no free slot within the probe limit.
/// Past that, a key's hits go uncounted there: the library's `aggregate_full`
/// drop, which gabion counts and logs.
pub const AGGREGATE_KEYS: u32 = 4;

/// Headroom over each exact working set, absorbing churn: the one knob for
/// every size [`SimConfig`] derives.
const SLACK: u32 = 2;

/// Zone index `i` is `ZONE_NAMES[i]`.
pub const ZONE_NAMES: [&str; MAX_ZONES] =
    ["zone-a", "zone-b", "zone-c", "zone-d", "zone-e", "zone-f"];

/// Whether nodes are told the zone they are placed in. `Unset` places them
/// but tells them nothing, so every node sends to every peer directly.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum ZoneMode {
    #[default]
    Configured,
    Unset,
}

/// Deterministic given `(SimConfig, command-script)` — the pair the
/// shareable URL encodes. Every field has a production-aligned
/// default drawn from [`gabion::defaults`].
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct SimConfig {
    /// Number of cluster members. Must be in `1..=MAX_NODES`;
    /// [`SimConfig::validate`] rejects anything else.
    pub nodes: usize,
    /// Heartbeat interval, in milliseconds.
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
    /// i.i.d. per-link packet drop probability applied to every directed
    /// link at startup. `0.0` is a lossless network.
    pub uniform_loss: f64,
    /// Zones nodes are placed in: node `i` at build in zone `i mod zones`.
    /// Must be in `1..=MAX_ZONES`; [`SimConfig::validate`] rejects anything
    /// else, so a zero from a shared URL gets the same message as a seven.
    pub zones: usize,
    pub zone_mode: ZoneMode,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            nodes: 12,
            tick_interval_ms: defaults::GOSSIP_TICK_INTERVAL_MILLIS,
            target_err_bps: defaults::GOSSIP_TARGET_ERR_BPS,
            min_emit_interval_ms: defaults::GOSSIP_MIN_EMIT_INTERVAL_MS,
            // Viz-friendly defaults: a 10 s window of 1 s buckets reads as a
            // legible row of bucket bars (the production 60 s / 1 s default is
            // 60 thin bands), and a limit of 1 000 is low enough that the
            // no-preset "Tune the cluster" path can cross it within a window.
            // The narrative presets override the limit back to 1 000 000 so a
            // burst still spreads lazily by heartbeat (see
            // `web/.../presets.ts`).
            rule_limit: 1_000,
            rule_window_ms: 10_000,
            rule_bucket_ms: 1_000,
            uniform_loss: 0.0,
            zones: 1,
            zone_mode: ZoneMode::Configured,
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
    #[error(
        "zone count {got} is out of range: the visualizer places nodes in 1 to {MAX_ZONES} zones. \
         Set `zones` in the controls or shared URL to a count in that range."
    )]
    ZonesOutOfRange { got: usize },
    #[error(transparent)]
    Setting(#[from] SettingError),
    /// The watched rule, as `RuleDef::build` refuses it for both adapters.
    #[error(transparent)]
    Rule(#[from] RuleError),
}

/// A [`SimConfig`] with every node's settings and storage config, built only
/// by [`SimConfig::validate`].
pub struct ValidSimConfig {
    pub(crate) config: SimConfig,
    /// Every node's settings, before its gossip bind and static peers.
    pub(crate) settings: Settings,
    pub(crate) storage: ValidStorageConfig,
    /// Every node's count table, sized for [`AGGREGATE_KEYS`] keys rather
    /// than for the cell store's every replica.
    pub(crate) aggregate: ValidStorageConfig,
    /// The watched rule, which every node interns at construction.
    pub(crate) rules: RuleTable,
    /// That rule as admission reads it: its fingerprint, limit and window.
    pub(crate) spec: RuleSpec,
    /// Every zone's name, parsed and hashed once.
    pub(crate) zone_names: Box<[ZoneName]>,
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
        if !(1..=MAX_ZONES).contains(&self.zones) {
            return Err(ConfigError::ZonesOutOfRange { got: self.zones });
        }
        let settings = self.settings()?;
        let (_, storage) = settings.validate()?;
        // One table per node, sized for the watched keys: sized from the cell
        // store it would take 1 MiB.
        let cells = AGGREGATE_KEYS
            .saturating_mul(self.per_origin())
            .saturating_mul(SLACK);
        let mut aggregate = settings.clone();
        aggregate.apply(Setting::StorageMaxCells, &cells.to_string())?;
        let (_, aggregate) = aggregate.validate()?;
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
        let rules = RuleTable::new(Box::new([rule]), storage.rule_dictionary_capacity())?;
        // The table was built from a one-element array on the line above, so
        // it holds exactly one rule: a constant shape, like `ZONE_NAMES`.
        let spec = rules.rules()[0].spec();
        // The input is the compile-time `ZONE_NAMES` table, the
        // constant-input exception CLAUDE.md allows (as for `stable_hasher`'s
        // secret); `built_in_zone_names_are_valid_and_hash_apart` pins it.
        let zone_names = ZONE_NAMES[..self.zones]
            .iter()
            .map(|name| ZoneName::parse(name).expect("a built-in zone name is a valid zone name"))
            .collect();
        Ok(ValidSimConfig {
            config: self,
            settings,
            storage,
            aggregate,
            rules,
            spec,
            zone_names,
        })
    }

    /// Each origin's cells of one key: its live buckets plus the one emerging.
    fn per_origin(&self) -> u32 {
        RuleDescriptor {
            window_millis: self.rule_window_ms,
            bucket_millis: self.rule_bucket_ms,
            ..RuleDescriptor::default()
        }
        .live_buckets()
            + 1
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
    /// eviction; if a probe ever fills the store, raise `SLACK` here — the
    /// one place — not a scattered floor.
    pub fn settings(&self) -> Result<Settings, ConfigError> {
        // The ceiling the cluster can grow to (see the doc note) — not
        // `self.nodes`, which is only the *initial* member count.
        let ceiling = MAX_NODES as u32;
        // A node holds a replica of every origin's set, so the cell store
        // scales with `ceiling × per_origin`.
        let cross_node = ceiling
            .saturating_mul(self.per_origin())
            .saturating_mul(SLACK)
            .max(SLACK);
        let mut settings = Settings::default();
        for (setting, value) in [
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
            (Setting::StorageMaxCells, cross_node.to_string()),
            // One watched rule; headroom for an interned default / wire-only
            // rule.
            (Setting::StorageRuleDictionaryCapacity, "4".to_owned()),
            (Setting::GossipPeerCapacity, ceiling.to_string()),
        ] {
            settings.apply(setting, &value)?;
        }
        Ok(settings)
    }
}

#[cfg(test)]
mod tests;
