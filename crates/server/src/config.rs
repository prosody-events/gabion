//! YAML configuration for the `gabiond` binary.
//!
//! The library knows nothing about YAML — every field below is server-only.
//! Parsing produces typed primitives that the binary's startup code feeds
//! into [`gabion::rules::Rule::new`], [`gabion::gossip::GossipConfig`], and so
//! on.

use std::net::SocketAddr;
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use gabion::crdt::{CellStoreConfig, NodeIdentity};
use gabion::discovery::DiscoveryConfig;
use gabion::gossip::GossipConfig;
use gabion::rules::{DescriptorPattern, EnforcementMode, Rule, RuleId, RuleTable};
use gabion::wire::FrameLimits;

use crate::admission::CardinalityLimits;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub envoy_bind: Option<SocketAddr>,
    pub admin_bind: Option<SocketAddr>,
    pub storage: StorageConfig,
    pub limits: Vec<LimitRuleConfig>,
    pub runtime: RuntimeTuningConfig,
    pub discovery: DiscoveryConfig,
    pub gossip: GossipSettings,
}

impl AppConfig {
    pub fn parse_yaml(text: &str) -> Result<Self, ConfigError> {
        serde_yaml::from_str(text).map_err(ConfigError::Yaml)
    }

    pub fn cardinality_limits(&self) -> CardinalityLimits {
        CardinalityLimits {
            max_descriptor_count: self.storage.max_descriptor_count,
            max_descriptor_bytes: self.storage.max_descriptor_bytes,
            max_key_bytes: self.storage.max_key_bytes,
        }
    }

    /// Construct the runtime [`RuleTable`]. Rule ids are assigned in
    /// declaration order starting at 1.
    pub fn rule_table(&self) -> Result<RuleTable, ConfigError> {
        let rules = self
            .limits
            .iter()
            .enumerate()
            .map(|(i, limit)| limit.to_rule(i as RuleId + 1))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RuleTable::new(rules))
    }

    pub fn cell_store_config(&self) -> CellStoreConfig {
        let max_cells = self.storage.max_cells.unwrap_or(4096);
        CellStoreConfig {
            cell_capacity: max_cells.max(1) as u32,
            rule_dictionary_capacity: self.storage.rule_dictionary_capacity,
            node_dictionary_capacity: self.storage.node_dictionary_capacity,
            local_dirty_capacity: self.storage.local_dirty_capacity,
            forwarded_dirty_capacity: self.storage.forwarded_dirty_capacity,
            peer_capacity: self.storage.peer_capacity,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// CRDT cell-store capacity. Caps the number of distinct
    /// `(rule, key, bucket, origin)` cells held locally.
    pub max_cells: Option<usize>,
    pub rule_dictionary_capacity: u16,
    pub node_dictionary_capacity: u16,
    pub local_dirty_capacity: usize,
    pub forwarded_dirty_capacity: usize,
    pub peer_capacity: u16,

    pub max_descriptor_count: usize,
    pub max_descriptor_bytes: usize,
    pub max_key_bytes: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_cells: None,
            rule_dictionary_capacity: 64,
            node_dictionary_capacity: 64,
            local_dirty_capacity: 256,
            forwarded_dirty_capacity: 256,
            peer_capacity: 32,
            max_descriptor_count: 16,
            max_descriptor_bytes: 512,
            max_key_bytes: 128,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct RuntimeTuningConfig {
    /// Optional seed for the node id. Falls back to `whoami::fallible::hostname()`
    /// then a fixed constant.
    pub node_id_seed: Option<String>,
    /// Deterministic peer-sampling seed.
    pub rng_seed: u64,
}

impl Default for RuntimeTuningConfig {
    fn default() -> Self {
        Self {
            node_id_seed: None,
            rng_seed: 0x9E37_79B9_7F4A_7C15,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct GossipSettings {
    pub enabled: bool,
    pub bind: Option<SocketAddr>,
    #[serde(with = "humantime_serde")]
    pub tick_interval: Duration,
    pub fanout: usize,
    pub max_payload_bytes: usize,
    pub max_cells_per_frame: u32,
    pub max_cells_per_tick: usize,
    pub send_queue_capacity: usize,
    pub limit_queue_capacity: usize,
    pub cluster_id_hash: u128,
}

impl Default for GossipSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: None,
            tick_interval: Duration::from_millis(100),
            fanout: 3,
            max_payload_bytes: 256 * 1024,
            max_cells_per_frame: 4096,
            max_cells_per_tick: 1024,
            send_queue_capacity: 32,
            limit_queue_capacity: 1024,
            cluster_id_hash: 1,
        }
    }
}

impl GossipSettings {
    /// Translate the user-facing config into the runtime's [`GossipConfig`].
    /// `bootstrap_peers` comes in from the discovery layer; we leave it
    /// empty here and let the runtime's peer-event stream populate it.
    pub fn into_runtime_config(self, identity: NodeIdentity, rng_seed: u64) -> GossipConfig {
        GossipConfig {
            local_identity: identity,
            cluster_id_hash: self.cluster_id_hash,
            bootstrap_peers: Vec::new(),
            fanout: self.fanout,
            max_cells_per_tick: self.max_cells_per_tick,
            wire_limits: FrameLimits {
                max_payload_bytes: self.max_payload_bytes,
                max_cells: self.max_cells_per_frame,
            },
            send_queue_capacity: self.send_queue_capacity,
            limit_queue_capacity: self.limit_queue_capacity,
            tick_interval: self.tick_interval,
            auth_key: None,
            rng_seed,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct LimitRuleConfig {
    pub name: String,
    pub domain: String,
    pub descriptors: Vec<DescriptorConfig>,
    pub limit: u64,
    #[serde(with = "humantime_serde")]
    pub window: Duration,
    #[serde(with = "humantime_serde")]
    pub bucket: Duration,
    #[serde(default)]
    pub mode: EnforcementModeConfig,
}

impl LimitRuleConfig {
    pub fn to_rule(&self, id: RuleId) -> Result<Rule, ConfigError> {
        if self.descriptors.is_empty() {
            return Err(ConfigError::EmptyDescriptorSet(self.name.clone()));
        }
        let descriptors = self
            .descriptors
            .iter()
            .map(|d| DescriptorPattern {
                key: d.key.clone(),
                value: d.value.clone(),
            })
            .collect();
        Ok(Rule::new(
            id,
            self.domain.clone(),
            descriptors,
            self.limit,
            duration_millis(self.window),
            duration_millis(self.bucket),
            self.mode.into(),
        ))
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct DescriptorConfig {
    pub key: String,
    #[serde(default = "any_value")]
    pub value: String,
}

fn any_value() -> String {
    "*".to_string()
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EnforcementModeConfig {
    #[default]
    Enforce,
    Disabled,
}

impl From<EnforcementModeConfig> for EnforcementMode {
    fn from(value: EnforcementModeConfig) -> Self {
        match value {
            EnforcementModeConfig::Enforce => EnforcementMode::Enforce,
            EnforcementModeConfig::Disabled => EnforcementMode::Disabled,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("yaml parse error: {0}")]
    Yaml(#[source] serde_yaml::Error),
    #[error("rule {0} has no descriptors")]
    EmptyDescriptorSet(String),
    #[error("gossip.enabled requires gossip.bind")]
    MissingGossipBind,
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX).max(1)
}
