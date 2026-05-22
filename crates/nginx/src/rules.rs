//! Bridge from operator-provided nginx configuration (`gabion_limit_rule`)
//! to `gabion::rules::Rule` + a precomputed `RuleSpec` table.
//!
//! Mirrors the server's `RuleSpec` pattern (`crates/server/src/lib.rs:51-57`).
//! Each `RuleSpec` carries the values the access hot path needs without
//! re-deriving them per request: the cross-node `fingerprint`, the limit,
//! the bucket size, and the live-bucket count.

use std::sync::Arc;
use std::time::Duration;

use gabion::rules::{DescriptorPattern, EnforcementMode, Rule, RuleId, RuleTable};
use thiserror::Error;

/// Default domain assigned to rules that don't name one explicitly. nginx
/// requests carry no inherent "domain" the way Envoy descriptors do, so we
/// default to a single per-zone bucket.
pub const DEFAULT_DOMAIN: &str = "nginx";

/// Hard cap on descriptors per rule. Mirrors the server's cardinality
/// envelope so cross-node fingerprints can never grow past what one side can
/// admit.
pub const MAX_DESCRIPTORS: usize = 16;

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
/// operator-facing name (so locations can be wired up by string), the
/// per-request descriptor bindings, and the hot-path `RuleSpec`.
#[derive(Debug)]
pub struct CompiledRule {
    pub name: String,
    pub rule: Rule,
    pub bindings: Vec<DescriptorBinding>,
    pub spec: RuleSpec,
}

/// Hot-path-ready summary of one rule. Copied around freely.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuleSpec {
    pub id: RuleId,
    pub fingerprint: u128,
    pub limit: u64,
    pub bucket_millis: u64,
    pub window_millis: u64,
    pub live_buckets: u32,
}

impl RuleSpec {
    pub fn from_rule(rule: &Rule) -> Self {
        Self {
            id: rule.id,
            fingerprint: rule.fingerprint,
            limit: rule.limit,
            bucket_millis: rule.bucket_millis,
            window_millis: rule.window_millis,
            live_buckets: rule.live_buckets(),
        }
    }
}

/// Composite of `RuleTable` + per-rule `Vec<DescriptorBinding>` + per-rule
/// `RuleSpec`. Shared (via `Arc`) between every worker's location config and
/// the leader's drain task.
#[derive(Debug)]
pub struct CompiledRules {
    table: Arc<RuleTable>,
    rules: Vec<CompiledRule>,
}

impl CompiledRules {
    /// Compile a list of `RuleConfig`s into a `RuleTable` + per-rule specs.
    pub fn compile(configs: &[RuleConfig]) -> Result<Self, RuleConfigError> {
        if configs.is_empty() {
            return Err(RuleConfigError::Empty);
        }
        let mut compiled = Vec::with_capacity(configs.len());
        let mut rules = Vec::with_capacity(configs.len());
        for (idx, cfg) in configs.iter().enumerate() {
            if cfg.bindings.is_empty() {
                return Err(RuleConfigError::EmptyBindings(cfg.name.clone()));
            }
            if cfg.bindings.len() > MAX_DESCRIPTORS {
                return Err(RuleConfigError::TooManyBindings(cfg.name.clone()));
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
            let spec = RuleSpec::from_rule(&rule);
            rules.push(rule.clone());
            compiled.push(CompiledRule {
                name: cfg.name.clone(),
                rule,
                bindings: cfg.bindings.clone(),
                spec,
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
        assert_eq!(rules.rules()[0].spec.limit, 10);
        assert_eq!(rules.rules()[0].spec.live_buckets, 60);
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
}
