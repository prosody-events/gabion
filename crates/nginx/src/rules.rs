//! Bridge from operator-provided nginx configuration (`gabion_limit_rule`)
//! to `gabion::rules::Rule` + a precomputed `RuleSpec` table.
//!
//! `RuleSpec` itself lives in `gabion::rules` so both adapters share the
//! same hot-path-ready summary; this module re-exports it for source
//! compatibility.

use std::sync::Arc;
use std::time::Duration;

use gabion::defaults;
use gabion::rules::{DescriptorPattern, EnforcementMode, Rule, RuleId, RuleTable};
use thiserror::Error;

pub use gabion::rules::RuleSpec;

/// Default domain assigned to rules that don't name one explicitly. nginx
/// requests carry no inherent "domain" the way Envoy descriptors do, so we
/// default to a single per-zone bucket.
pub const DEFAULT_DOMAIN: &str = "nginx";

/// Hard cap on descriptors per rule. Mirrors the server's cardinality
/// envelope so cross-node fingerprints can never grow past what one side can
/// admit.
pub const MAX_DESCRIPTORS: usize = defaults::STORAGE_MAX_DESCRIPTOR_COUNT;

/// Operator-visible cardinality envelope, enforced both at config-compile
/// time (descriptor count, key length) and per-request (byte budget).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CardinalitySettings {
    pub max_descriptor_count: usize,
    pub max_descriptor_bytes: usize,
    pub max_key_bytes: usize,
}

impl Default for CardinalitySettings {
    fn default() -> Self {
        Self {
            max_descriptor_count: defaults::STORAGE_MAX_DESCRIPTOR_COUNT,
            max_descriptor_bytes: defaults::STORAGE_MAX_DESCRIPTOR_BYTES,
            max_key_bytes: defaults::STORAGE_MAX_KEY_BYTES,
        }
    }
}

/// One descriptor binding declared in nginx config — a descriptor `key` paired
/// with the nginx variable name to read at request time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptorBinding {
    pub key: String,
    /// Nginx variable name (no `$`). The access path reads this value at
    /// request time.
    pub variable: String,
}

/// Operator-facing rule configuration. Parsed from `gabion_limit_rule`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleConfig {
    pub name: String,
    pub domain: String,
    pub bindings: Vec<DescriptorBinding>,
    pub limit: u64,
    pub window: Duration,
    pub bucket: Duration,
    pub mode: EnforcementMode,
}

/// Compiled rule data ready for the runtime. Holds the gossip `Rule`, the
/// operator-facing name (so locations can be wired up by string), and the
/// per-request descriptor bindings. The hot-path `RuleSpec` is reachable
/// via `rule.spec()`.
#[derive(Debug)]
pub struct CompiledRule {
    pub name: String,
    pub rule: Rule,
    pub bindings: Vec<DescriptorBinding>,
}

/// Composite of `RuleTable` + per-rule `Vec<DescriptorBinding>`. Shared (via
/// `Arc`) between every worker's location config and the leader's drain
/// task.
#[derive(Debug)]
pub struct CompiledRules {
    table: Arc<RuleTable>,
    rules: Vec<CompiledRule>,
}

impl CompiledRules {
    /// Compile a list of `RuleConfig`s into a `RuleTable` + per-rule specs.
    /// Uses the default cardinality envelope (16 descriptors / 128B keys).
    pub fn compile(configs: &[RuleConfig]) -> Result<Self, RuleConfigError> {
        Self::compile_with_cardinality(configs, CardinalitySettings::default())
    }

    pub fn compile_with_cardinality(
        configs: &[RuleConfig],
        cardinality: CardinalitySettings,
    ) -> Result<Self, RuleConfigError> {
        if configs.is_empty() {
            return Err(RuleConfigError::Empty);
        }
        let mut compiled = Vec::with_capacity(configs.len());
        let mut rules = Vec::with_capacity(configs.len());
        for (idx, cfg) in configs.iter().enumerate() {
            if cfg.bindings.is_empty() {
                return Err(RuleConfigError::EmptyBindings(cfg.name.clone()));
            }
            if cfg.bindings.len() > cardinality.max_descriptor_count {
                return Err(RuleConfigError::TooManyBindings(cfg.name.clone()));
            }
            for binding in &cfg.bindings {
                if binding.key.len() > cardinality.max_key_bytes {
                    return Err(RuleConfigError::KeyTooLong {
                        rule: cfg.name.clone(),
                        key: binding.key.clone(),
                    });
                }
            }
            let id: RuleId = (idx + 1) as RuleId;
            let descriptors = cfg
                .bindings
                .iter()
                .map(|b| DescriptorPattern {
                    key: b.key.clone(),
                    value: "*".to_string(),
                })
                .collect();
            let rule = Rule::new(
                id,
                cfg.domain.clone(),
                descriptors,
                cfg.limit.max(1),
                duration_millis(cfg.window),
                duration_millis(cfg.bucket),
                cfg.mode,
            );
            rules.push(rule.clone());
            compiled.push(CompiledRule {
                name: cfg.name.clone(),
                rule,
                bindings: cfg.bindings.clone(),
            });
        }
        Ok(Self {
            table: Arc::new(RuleTable::new(rules)),
            rules: compiled,
        })
    }

    pub fn table(&self) -> &Arc<RuleTable> {
        &self.table
    }

    pub fn rules(&self) -> &[CompiledRule] {
        &self.rules
    }

    pub fn by_name(&self, name: &str) -> Option<&CompiledRule> {
        self.rules.iter().find(|r| r.name == name)
    }

    pub fn index_by_name(&self, name: &str) -> Option<usize> {
        self.rules.iter().position(|r| r.name == name)
    }

    pub fn get(&self, index: usize) -> Option<&CompiledRule> {
        self.rules.get(index)
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum RuleConfigError {
    #[error("no rules configured")]
    Empty,
    #[error("rule {0} has no descriptor bindings")]
    EmptyBindings(String),
    #[error("rule {0} declares too many bindings")]
    TooManyBindings(String),
    #[error("rule {rule} binding key '{key}' exceeds max_key_bytes")]
    KeyTooLong { rule: String, key: String },
}

fn duration_millis(d: Duration) -> u64 {
    d.as_millis().try_into().unwrap_or(u64::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(key: &str, var: &str) -> DescriptorBinding {
        DescriptorBinding {
            key: key.to_string(),
            variable: var.to_string(),
        }
    }

    fn cfg(name: &str, bindings: Vec<DescriptorBinding>) -> RuleConfig {
        RuleConfig {
            name: name.to_string(),
            domain: DEFAULT_DOMAIN.to_string(),
            bindings,
            limit: 10,
            window: Duration::from_secs(60),
            bucket: Duration::from_secs(1),
            mode: EnforcementMode::Enforce,
        }
    }

    #[test]
    fn compiles_simple_rules() {
        let rules = CompiledRules::compile(&[
            cfg("per_tenant", vec![binding("tenant", "http_x_tenant")]),
            cfg("per_uri", vec![binding("uri", "uri")]),
        ])
        .expect("compile");
        assert_eq!(rules.len(), 2);
        assert_eq!(rules.rules()[0].rule.spec().limit, 10);
        assert_eq!(rules.rules()[0].rule.spec().live_buckets, 60);
        let table = rules.table();
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn empty_bindings_reject() {
        let err = CompiledRules::compile(&[cfg("bad", vec![])]).unwrap_err();
        assert!(matches!(err, RuleConfigError::EmptyBindings(_)));
    }

    #[test]
    fn empty_set_rejects() {
        let err = CompiledRules::compile(&[]).unwrap_err();
        assert_eq!(err, RuleConfigError::Empty);
    }

    #[test]
    fn key_too_long_at_compile() {
        let long_key = "k".repeat(200);
        let cardinality = CardinalitySettings::default();
        let err = CompiledRules::compile_with_cardinality(
            &[cfg("long", vec![binding(&long_key, "http_x")])],
            cardinality,
        )
        .unwrap_err();
        assert!(matches!(err, RuleConfigError::KeyTooLong { .. }));
    }

    #[test]
    fn too_many_bindings_at_compile() {
        let bindings = (0..MAX_DESCRIPTORS + 1)
            .map(|i| binding(&format!("k{i}"), &format!("v{i}")))
            .collect();
        let err = CompiledRules::compile(&[cfg("wide", bindings)]).unwrap_err();
        assert!(matches!(err, RuleConfigError::TooManyBindings(_)));
    }
}
