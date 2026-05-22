//! Configuration for the `gabiond` binary.
//!
//! Configuration is layered (each later source overrides earlier ones):
//!
//! 1. Built-in defaults from the per-struct `Default` impls below.
//! 2. An optional YAML file passed on the command line.
//! 3. Environment variables prefixed `GABION_`.
//!
//! ## Environment variables
//!
//! - Nested keys use a double-underscore separator. For example,
//!   `GABION_STORAGE__MAX_CELLS=131072` maps to `storage.max_cells`.
//! - Scalar lists (e.g. `Vec<String>`, `Vec<SocketAddr>`) are comma-
//!   separated: `GABION_DISCOVERY__NAMESPACE_WHITELIST=ns-a,ns-b`.
//! - Structured lists (notably `limits`, where each entry is itself a
//!   struct with nested fields and durations) cannot be expressed through
//!   env vars and must come from the YAML file.
//! - Optional scalars accept their natural string form:
//!   `GABION_ENVOY_BIND=0.0.0.0:8081`, `GABION_GOSSIP__TICK_INTERVAL=100ms`.
//!
//! The library itself knows nothing about YAML or env — every field below
//! is server-only. Parsing produces typed primitives that the binary's
//! startup code feeds into [`gabion::rules::Rule::new`],
//! [`gabion::gossip::GossipConfig`], and so on.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use config::{Config, Environment, File, FileFormat};
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

/// Env-var prefix for every configurable field. Set e.g.
/// `GABION_STORAGE__MAX_CELLS=131072` to override `storage.max_cells`.
pub const ENV_PREFIX: &str = "GABION";

/// Separator between nested field names in env var keys. Single underscores
/// are kept for snake_case field names (`max_cells`); the double underscore
/// signals a level of nesting.
pub const ENV_SEPARATOR: &str = "__";

/// Separator for `Vec<scalar>` env values. `Vec<String>`/`Vec<SocketAddr>`
/// fields like `discovery.namespace_whitelist` accept a comma-separated
/// list.
pub const ENV_LIST_SEPARATOR: &str = ",";

/// Every `Vec<scalar>` field that `Environment` should split on the list
/// separator. Struct lists (notably `limits`) are intentionally excluded
/// because env vars cannot express nested struct entries.
const LIST_PARSE_KEYS: &[&str] = &[
    "discovery.namespace_whitelist",
    "discovery.service_whitelist",
];

impl AppConfig {
    /// Build the final config from defaults → YAML → env. If `yaml_path`
    /// is `None`, the YAML layer is skipped and the server can be
    /// configured purely from env vars and built-in defaults.
    pub fn load(yaml_path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut builder = Config::builder();
        if let Some(path) = yaml_path {
            let path_str = path.to_string_lossy().into_owned();
            builder = builder.add_source(File::new(&path_str, FileFormat::Yaml));
        }

        let mut env = Environment::with_prefix(ENV_PREFIX)
            .separator(ENV_SEPARATOR)
            .list_separator(ENV_LIST_SEPARATOR)
            .try_parsing(true)
            .prefix_separator("_");
        for key in LIST_PARSE_KEYS {
            env = env.with_list_parse_key(key);
        }
        builder = builder.add_source(env);

        let raw = builder.build().map_err(ConfigError::Config)?;
        raw.try_deserialize().map_err(ConfigError::Config)
    }

    /// Parse YAML text directly, ignoring any env layering. Retained for
    /// tests; production paths should use [`Self::load`].
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
        let max_cells = self.storage.max_cells.unwrap_or(131_072);
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
            node_dictionary_capacity: 1024,
            local_dirty_capacity: 8192,
            forwarded_dirty_capacity: 65536,
            peer_capacity: 256,
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
            bind: None,
            tick_interval: Duration::from_millis(100),
            fanout: 6,
            max_payload_bytes: 1400,
            max_cells_per_frame: 4096,
            max_cells_per_tick: 4096,
            send_queue_capacity: 128,
            limit_queue_capacity: 8192,
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
    #[error("config error: {0}")]
    Config(#[source] config::ConfigError),
    #[error("yaml parse error: {0}")]
    Yaml(#[source] serde_yaml::Error),
    #[error("rule {0} has no descriptors")]
    EmptyDescriptorSet(String),
    #[error("gossip.bind is required")]
    MissingGossipBind,
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    //! Env-var tests serialize through a single mutex because `std::env`
    //! is process-global. Cargo runs tests within a binary in parallel by
    //! default; without the lock, two tests can read each other's env vars
    //! and produce nonsense failures.

    use super::*;
    use std::sync::Mutex;

    // Hand-rolled tempfile to avoid pulling a new dev-dep just for two tests.
    mod tempfile_workaround {
        use std::io::Write;
        use std::path::PathBuf;

        pub struct YamlTempFile {
            path: PathBuf,
        }

        impl YamlTempFile {
            pub fn new(contents: &str) -> Self {
                use std::sync::atomic::{AtomicU32, Ordering};
                static COUNTER: AtomicU32 = AtomicU32::new(0);
                let n = COUNTER.fetch_add(1, Ordering::Relaxed);
                let path = std::env::temp_dir().join(format!(
                    "gabion-config-test-{}-{}.yaml",
                    std::process::id(),
                    n
                ));
                let mut f = std::fs::File::create(&path).expect("create temp yaml");
                f.write_all(contents.as_bytes()).expect("write temp yaml");
                Self { path }
            }

            pub fn path(&self) -> &std::path::Path {
                &self.path
            }
        }

        impl Drop for YamlTempFile {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
    use tempfile_workaround::YamlTempFile;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Env vars under our prefix that any test in this module might touch.
    /// Cleared at the start and end of each test so leftovers from a prior
    /// run can't leak across cases.
    const TEST_ENV_KEYS: &[&str] = &[
        "GABION_ENVOY_BIND",
        "GABION_ADMIN_BIND",
        "GABION_STORAGE__MAX_CELLS",
        "GABION_STORAGE__RULE_DICTIONARY_CAPACITY",
        "GABION_GOSSIP__BIND",
        "GABION_GOSSIP__FANOUT",
        "GABION_GOSSIP__TICK_INTERVAL",
        "GABION_DISCOVERY__NAMESPACE_WHITELIST",
        "GABION_DISCOVERY__SERVICE_WHITELIST",
        "GABION_RUNTIME__RNG_SEED",
    ];

    fn clear_env() {
        for key in TEST_ENV_KEYS {
            // SAFETY: serialized via `ENV_LOCK`; no concurrent reader/writer.
            unsafe { std::env::remove_var(key) };
        }
    }

    fn set_env(key: &str, value: &str) {
        // SAFETY: serialized via `ENV_LOCK`; no concurrent reader/writer.
        unsafe { std::env::set_var(key, value) };
    }

    #[test]
    fn defaults_apply_when_neither_yaml_nor_env_set_a_value() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_env();

        let cfg = AppConfig::load(None).expect("load with neither yaml nor env");

        assert_eq!(cfg.envoy_bind, None);
        assert_eq!(cfg.storage.rule_dictionary_capacity, 64);
        assert_eq!(cfg.gossip.fanout, 6);
        assert!(cfg.discovery.namespace_whitelist.is_empty());

        clear_env();
    }

    #[test]
    fn yaml_values_load_when_no_env_overrides() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_env();

        let yaml = YamlTempFile::new(
            "envoy_bind: \"127.0.0.1:8000\"\n\
             storage:\n  \
               max_cells: 256\n  \
               rule_dictionary_capacity: 8\n\
             gossip:\n  bind: \"127.0.0.1:9000\"\n  fanout: 3\n",
        );

        let cfg = AppConfig::load(Some(yaml.path())).expect("load yaml");
        assert_eq!(cfg.envoy_bind, Some("127.0.0.1:8000".parse().unwrap()));
        assert_eq!(cfg.storage.max_cells, Some(256));
        assert_eq!(cfg.storage.rule_dictionary_capacity, 8);
        assert_eq!(cfg.gossip.fanout, 3);

        clear_env();
    }

    #[test]
    fn env_overrides_yaml_for_scalars() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_env();

        let yaml = YamlTempFile::new(
            "storage:\n  max_cells: 256\n  rule_dictionary_capacity: 8\n\
             gossip:\n  fanout: 3\n",
        );
        set_env("GABION_STORAGE__MAX_CELLS", "9999");
        set_env("GABION_GOSSIP__FANOUT", "12");

        let cfg = AppConfig::load(Some(yaml.path())).expect("load yaml + env");
        assert_eq!(cfg.storage.max_cells, Some(9999));
        assert_eq!(cfg.gossip.fanout, 12);
        // Untouched YAML value stays.
        assert_eq!(cfg.storage.rule_dictionary_capacity, 8);

        clear_env();
    }

    #[test]
    fn env_only_with_no_yaml_file() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_env();

        set_env("GABION_STORAGE__MAX_CELLS", "5555");
        set_env("GABION_ENVOY_BIND", "0.0.0.0:8081");
        set_env("GABION_RUNTIME__RNG_SEED", "42");

        let cfg = AppConfig::load(None).expect("load env-only");
        assert_eq!(cfg.storage.max_cells, Some(5555));
        assert_eq!(cfg.envoy_bind, Some("0.0.0.0:8081".parse().unwrap()));
        assert_eq!(cfg.runtime.rng_seed, 42);

        clear_env();
    }

    #[test]
    fn comma_separated_lists_split_into_vec() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_env();

        set_env(
            "GABION_DISCOVERY__NAMESPACE_WHITELIST",
            "ns-a,ns-b,ns-c",
        );
        set_env("GABION_DISCOVERY__SERVICE_WHITELIST", "svc-1,svc-2");

        let cfg = AppConfig::load(None).expect("load env-only with lists");
        assert_eq!(
            cfg.discovery.namespace_whitelist,
            vec!["ns-a".to_string(), "ns-b".to_string(), "ns-c".to_string()],
        );
        assert_eq!(
            cfg.discovery.service_whitelist,
            vec!["svc-1".to_string(), "svc-2".to_string()],
        );

        clear_env();
    }

    #[test]
    fn duration_env_uses_humantime_syntax() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_env();

        set_env("GABION_GOSSIP__TICK_INTERVAL", "250ms");
        let cfg = AppConfig::load(None).expect("load tick_interval from env");
        assert_eq!(cfg.gossip.tick_interval, Duration::from_millis(250));

        clear_env();
    }

    #[test]
    fn bad_scalar_env_value_returns_error_not_panic() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        clear_env();

        set_env("GABION_STORAGE__MAX_CELLS", "not_a_number");
        let err = AppConfig::load(None).expect_err("non-integer max_cells should error");
        let msg = err.to_string();
        assert!(
            msg.contains("max_cells"),
            "error should name the offending key, got: {msg}",
        );

        clear_env();
    }
}
