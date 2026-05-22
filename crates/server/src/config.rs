//! Configuration for the `gabiond` binary.
//!
//! Configuration is layered (each later source overrides earlier ones):
//!
//! 1. Built-in defaults from the per-struct `Default` impls below.
//! 2. An optional YAML file passed on the command line.
//! 3. Environment variables — see [`ENV_BINDINGS`] for the full list.
//!
//! ## Environment variables
//!
//! Every overridable config field is mapped to a single env var. There is
//! no automatic `STRUCT__FIELD` nesting — each binding is explicit so the
//! variable names operators type are flat and unambiguous (no
//! double-underscores).
//!
//! Examples:
//!
//! - `GABION_STORAGE_MAX_CELLS=131072`
//! - `GABION_GOSSIP_BIND=0.0.0.0:9090`
//! - `GABION_GOSSIP_TICK_INTERVAL=100ms`
//! - `GABION_DISCOVERY_NAMESPACE_WHITELIST=ns-a,ns-b` (comma-separated)
//!
//! Structured lists (notably `limits`, where each entry is itself a
//! struct with nested fields and durations) cannot be expressed through
//! env vars and must come from the YAML file.
//!
//! The library itself knows nothing about YAML or env — every field below
//! is server-only. Parsing produces typed primitives that the binary's
//! startup code feeds into [`gabion::rules::Rule::new`],
//! [`gabion::gossip::GossipConfig`], and so on.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use config::{Config, ConfigBuilder, File, FileFormat, Value, builder::DefaultState};
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

/// Whether an env var value should be parsed as a scalar string or a
/// comma-separated list. Comma-separated lists feed `Vec<String>` /
/// `Vec<SocketAddr>` fields; lists of nested structs (like `limits`) are
/// not env-configurable and must come from the YAML file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnvValueKind {
    Scalar,
    List,
}

/// How a single env var maps onto a config field.
#[derive(Clone, Copy, Debug)]
pub struct EnvBinding {
    /// Exact env var name. Always upper-snake_case, single underscores.
    pub env_name: &'static str,
    /// Dotted path into the [`AppConfig`] tree, e.g. `storage.max_cells`.
    pub config_path: &'static str,
    /// How to interpret the raw env value.
    pub kind: EnvValueKind,
}

impl EnvBinding {
    /// Bind an env var to a scalar field.
    pub const fn scalar(env_name: &'static str, config_path: &'static str) -> Self {
        Self {
            env_name,
            config_path,
            kind: EnvValueKind::Scalar,
        }
    }

    /// Bind an env var to a comma-separated list field.
    pub const fn list(env_name: &'static str, config_path: &'static str) -> Self {
        Self {
            env_name,
            config_path,
            kind: EnvValueKind::List,
        }
    }
}

/// Every overridable config field, paired with the env var that overrides
/// it. Names use single underscores throughout — there is no nesting
/// separator, just an explicit table. To add a new env-overridable field:
/// add an entry here and confirm `config_path` matches the field path in
/// the YAML schema.
pub const ENV_BINDINGS: &[EnvBinding] = &[
    // Top-level binds.
    EnvBinding::scalar("GABION_ENVOY_BIND", "envoy_bind"),
    EnvBinding::scalar("GABION_ADMIN_BIND", "admin_bind"),
    // storage.*
    EnvBinding::scalar("GABION_STORAGE_MAX_CELLS", "storage.max_cells"),
    EnvBinding::scalar(
        "GABION_STORAGE_RULE_DICTIONARY_CAPACITY",
        "storage.rule_dictionary_capacity",
    ),
    EnvBinding::scalar(
        "GABION_STORAGE_NODE_DICTIONARY_CAPACITY",
        "storage.node_dictionary_capacity",
    ),
    EnvBinding::scalar(
        "GABION_STORAGE_LOCAL_DIRTY_CAPACITY",
        "storage.local_dirty_capacity",
    ),
    EnvBinding::scalar(
        "GABION_STORAGE_FORWARDED_DIRTY_CAPACITY",
        "storage.forwarded_dirty_capacity",
    ),
    EnvBinding::scalar("GABION_STORAGE_PEER_CAPACITY", "storage.peer_capacity"),
    EnvBinding::scalar(
        "GABION_STORAGE_MAX_DESCRIPTOR_COUNT",
        "storage.max_descriptor_count",
    ),
    EnvBinding::scalar(
        "GABION_STORAGE_MAX_DESCRIPTOR_BYTES",
        "storage.max_descriptor_bytes",
    ),
    EnvBinding::scalar("GABION_STORAGE_MAX_KEY_BYTES", "storage.max_key_bytes"),
    // runtime.*
    EnvBinding::scalar("GABION_RUNTIME_NODE_ID_SEED", "runtime.node_id_seed"),
    EnvBinding::scalar("GABION_RUNTIME_RNG_SEED", "runtime.rng_seed"),
    // gossip.*
    EnvBinding::scalar("GABION_GOSSIP_BIND", "gossip.bind"),
    EnvBinding::scalar("GABION_GOSSIP_TICK_INTERVAL", "gossip.tick_interval"),
    EnvBinding::scalar("GABION_GOSSIP_FANOUT", "gossip.fanout"),
    EnvBinding::scalar("GABION_GOSSIP_MAX_PAYLOAD_BYTES", "gossip.max_payload_bytes"),
    EnvBinding::scalar(
        "GABION_GOSSIP_MAX_CELLS_PER_FRAME",
        "gossip.max_cells_per_frame",
    ),
    EnvBinding::scalar(
        "GABION_GOSSIP_MAX_CELLS_PER_TICK",
        "gossip.max_cells_per_tick",
    ),
    EnvBinding::scalar(
        "GABION_GOSSIP_SEND_QUEUE_CAPACITY",
        "gossip.send_queue_capacity",
    ),
    EnvBinding::scalar(
        "GABION_GOSSIP_LIMIT_QUEUE_CAPACITY",
        "gossip.limit_queue_capacity",
    ),
    EnvBinding::scalar("GABION_GOSSIP_CLUSTER_ID_HASH", "gossip.cluster_id_hash"),
    // discovery.*
    EnvBinding::scalar("GABION_DISCOVERY_SELF_ADDR", "discovery.self_addr"),
    EnvBinding::list(
        "GABION_DISCOVERY_NAMESPACE_WHITELIST",
        "discovery.namespace_whitelist",
    ),
    EnvBinding::list(
        "GABION_DISCOVERY_SERVICE_WHITELIST",
        "discovery.service_whitelist",
    ),
];

impl AppConfig {
    /// Build the final config from defaults → YAML → env. If `yaml_path`
    /// is `None`, the YAML layer is skipped and the server can be
    /// configured purely from env vars and built-in defaults.
    pub fn load(yaml_path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut builder = Config::builder();
        if let Some(path) = yaml_path {
            builder =
                builder.add_source(File::new(&path.to_string_lossy(), FileFormat::Yaml));
        }
        finalize(builder)
    }

    /// Parse YAML text directly, ignoring any env layering. Retained for
    /// tests; production paths should use [`Self::load`].
    pub fn parse_yaml(text: &str) -> Result<Self, ConfigError> {
        serde_yaml::from_str(text).map_err(ConfigError::Yaml)
    }

    /// Test-only loader that takes inline YAML instead of a file path.
    /// Same env-layering semantics as [`Self::load`].
    #[cfg(test)]
    fn load_with_yaml_str(yaml: &str) -> Result<Self, ConfigError> {
        let builder = Config::builder().add_source(File::from_str(yaml, FileFormat::Yaml));
        finalize(builder)
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

/// Apply env overrides from [`ENV_BINDINGS`] on top of a builder already
/// seeded with defaults and (optionally) a YAML file, then deserialize.
fn finalize(mut builder: ConfigBuilder<DefaultState>) -> Result<AppConfig, ConfigError> {
    for binding in ENV_BINDINGS {
        let Some(raw) = read_env(binding.env_name)? else {
            continue;
        };
        let value = match binding.kind {
            EnvValueKind::Scalar => Value::from(raw),
            EnvValueKind::List => Value::from(parse_csv(&raw)),
        };
        builder = builder
            .set_override(binding.config_path, value)
            .map_err(ConfigError::Config)?;
    }
    builder
        .build()
        .map_err(ConfigError::Config)?
        .try_deserialize()
        .map_err(ConfigError::Config)
}

/// Read an env var, distinguishing unset (`Ok(None)`) from non-UTF-8 (error).
fn read_env(name: &'static str) -> Result<Option<String>, ConfigError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::NonUtf8EnvVar(name)),
    }
}

/// Split a comma-separated env value, trimming whitespace and skipping empty
/// segments so a trailing comma or double comma never produces a phantom
/// element.
fn parse_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
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
    #[error("environment variable {0} is not valid UTF-8")]
    NonUtf8EnvVar(&'static str),
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    //! Env-var tests serialize through a single mutex because `std::env` is
    //! process-global; running tests in parallel without the lock can let
    //! one test see another's env state and fail in non-obvious ways.

    use super::*;
    use std::sync::Mutex;

    /// Held for the duration of any test that mutates the env. Guarantees
    /// `set_env` / `clear_all` see a consistent view of the process env.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Remove every env var declared in [`ENV_BINDINGS`]. Run at the start
    /// of each test so leftovers from a prior test (in the same binary
    /// run) can't leak across cases.
    fn clear_all_env() {
        for binding in ENV_BINDINGS {
            // SAFETY: serialized through `ENV_LOCK`; no concurrent access.
            unsafe { std::env::remove_var(binding.env_name) };
        }
    }

    /// Set an env var. Asserts the key is a known binding so a typo in the
    /// test surfaces immediately instead of silently doing nothing.
    fn set_env(key: &str, value: &str) {
        assert!(
            ENV_BINDINGS.iter().any(|b| b.env_name == key),
            "test set unknown env var: {key} (add it to ENV_BINDINGS)",
        );
        // SAFETY: serialized through `ENV_LOCK`; no concurrent access.
        unsafe { std::env::set_var(key, value) };
    }

    /// RAII wrapper that clears every gabion env var on drop, so a panicking
    /// test still cleans up after itself.
    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn lock() -> Self {
            let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            clear_all_env();
            Self { _lock }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            clear_all_env();
        }
    }

    #[test]
    fn defaults_apply_when_neither_yaml_nor_env_set_a_value() {
        let _env = EnvGuard::lock();

        let cfg = AppConfig::load(None).expect("load with neither yaml nor env");

        assert_eq!(cfg.envoy_bind, None);
        assert_eq!(cfg.storage.rule_dictionary_capacity, 64);
        assert_eq!(cfg.gossip.fanout, 6);
        assert!(cfg.discovery.namespace_whitelist.is_empty());
    }

    #[test]
    fn yaml_values_load_when_no_env_overrides() {
        let _env = EnvGuard::lock();

        let cfg = AppConfig::load_with_yaml_str(
            "envoy_bind: \"127.0.0.1:8000\"\n\
             storage:\n  \
               max_cells: 256\n  \
               rule_dictionary_capacity: 8\n\
             gossip:\n  bind: \"127.0.0.1:9000\"\n  fanout: 3\n",
        )
        .expect("load yaml");

        assert_eq!(cfg.envoy_bind, Some("127.0.0.1:8000".parse().unwrap()));
        assert_eq!(cfg.storage.max_cells, Some(256));
        assert_eq!(cfg.storage.rule_dictionary_capacity, 8);
        assert_eq!(cfg.gossip.fanout, 3);
    }

    #[test]
    fn env_overrides_yaml_for_scalars() {
        let _env = EnvGuard::lock();
        set_env("GABION_STORAGE_MAX_CELLS", "9999");
        set_env("GABION_GOSSIP_FANOUT", "12");

        let cfg = AppConfig::load_with_yaml_str(
            "storage:\n  max_cells: 256\n  rule_dictionary_capacity: 8\n\
             gossip:\n  fanout: 3\n",
        )
        .expect("load yaml + env");

        assert_eq!(cfg.storage.max_cells, Some(9999));
        assert_eq!(cfg.gossip.fanout, 12);
        // Untouched YAML value stays.
        assert_eq!(cfg.storage.rule_dictionary_capacity, 8);
    }

    #[test]
    fn env_only_with_no_yaml_file() {
        let _env = EnvGuard::lock();
        set_env("GABION_STORAGE_MAX_CELLS", "5555");
        set_env("GABION_ENVOY_BIND", "0.0.0.0:8081");
        set_env("GABION_RUNTIME_RNG_SEED", "42");

        let cfg = AppConfig::load(None).expect("load env-only");

        assert_eq!(cfg.storage.max_cells, Some(5555));
        assert_eq!(cfg.envoy_bind, Some("0.0.0.0:8081".parse().unwrap()));
        assert_eq!(cfg.runtime.rng_seed, 42);
    }

    #[test]
    fn comma_separated_lists_split_into_vec() {
        let _env = EnvGuard::lock();
        set_env("GABION_DISCOVERY_NAMESPACE_WHITELIST", "ns-a,ns-b,ns-c");
        set_env("GABION_DISCOVERY_SERVICE_WHITELIST", "svc-1,svc-2");

        let cfg = AppConfig::load(None).expect("load env-only with lists");

        assert_eq!(
            cfg.discovery.namespace_whitelist,
            ["ns-a", "ns-b", "ns-c"].map(String::from),
        );
        assert_eq!(
            cfg.discovery.service_whitelist,
            ["svc-1", "svc-2"].map(String::from),
        );
    }

    #[test]
    fn list_parsing_trims_whitespace_and_skips_empties() {
        let _env = EnvGuard::lock();
        set_env(
            "GABION_DISCOVERY_NAMESPACE_WHITELIST",
            " ns-a , ns-b ,, ns-c , ",
        );

        let cfg = AppConfig::load(None).expect("load env list");

        assert_eq!(
            cfg.discovery.namespace_whitelist,
            ["ns-a", "ns-b", "ns-c"].map(String::from),
        );
    }

    #[test]
    fn duration_env_uses_humantime_syntax() {
        let _env = EnvGuard::lock();
        set_env("GABION_GOSSIP_TICK_INTERVAL", "250ms");

        let cfg = AppConfig::load(None).expect("load tick_interval from env");

        assert_eq!(cfg.gossip.tick_interval, Duration::from_millis(250));
    }

    #[test]
    fn bad_scalar_env_value_returns_error_not_panic() {
        let _env = EnvGuard::lock();
        set_env("GABION_STORAGE_MAX_CELLS", "not_a_number");

        let err = AppConfig::load(None).expect_err("non-integer max_cells should error");

        assert!(
            err.to_string().contains("max_cells"),
            "error should name the offending key, got: {err}",
        );
    }

    #[test]
    fn env_binding_names_use_single_underscores_only() {
        for binding in ENV_BINDINGS {
            assert!(
                binding.env_name.starts_with("GABION_"),
                "{} should be GABION_-prefixed",
                binding.env_name,
            );
            assert!(
                !binding.env_name.contains("__"),
                "{} contains a double underscore",
                binding.env_name,
            );
        }
    }
}
