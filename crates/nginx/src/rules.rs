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
/// with the nginx variable specification to evaluate at request time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptorBinding {
    pub key: String,
    /// The variable specification as it appears in config after the `key`
    /// prefix is stripped: a single `$identifier` for the indexed-variable
    /// fast path or a template (anything that includes literal text or
    /// multiple `$identifier` substitutions) compiled via
    /// `ngx_http_compile_complex_value` at config phase.
    pub source: String,
}

/// Runtime-ready binding produced by compiling a [`DescriptorBinding`].
///
/// Resolution dispatches on three cases:
///
/// * Inline fast-path arms (`$uri`, `$args`, `$remote_addr`, …) read
///   straight off the nginx request struct with no FFI hop.
/// * `IndexedVariable` resolves single-variable bindings (`$geoip2_asn`,
///   `$bot_class`, anything else) through `ngx_http_get_indexed_variable`
///   — O(1) array lookup, zero allocation per request.
/// * `ComplexValue` evaluates templates compiled via
///   `ngx_http_compile_complex_value` and is allowed a small per-request
///   allocation against `r->pool`.
#[derive(Debug)]
pub enum BindingLookup {
    Uri,
    RequestUri,
    Args,
    RemoteAddr,
    /// `$arg_<name>` lookup against the request's query string.
    Arg(Box<str>),
    /// Single-variable binding resolved via `ngx_http_get_indexed_variable`.
    /// The `index` field is the value returned by
    /// `ngx_http_get_variable_index` at config phase; under the test
    /// harness (no nginx) the index is a synthetic value and resolution
    /// goes through the variable name instead.
    IndexedVariable { name: Box<str>, index: i64 },
    /// Template binding compiled via `ngx_http_compile_complex_value`.
    /// `compiled_value` is the type-erased pointer to the
    /// `ngx_http_complex_value_t` allocated against the cycle pool; the
    /// pointer is valid for the cycle's lifetime (longer than any worker
    /// that reads it) and may only be evaluated against an
    /// `ngx_http_request_t`. The string `source` is the operator-visible
    /// template, kept for diagnostics.
    ComplexValue {
        source: Box<str>,
        compiled_value: usize,
    },
}

// SAFETY: The only !Send/!Sync component of `BindingLookup` is the
// `compiled_value: usize` inside `ComplexValue`, which type-erases a
// `*const ngx_http_complex_value_t`. nginx allocates that struct against
// the cycle pool during the master-process config phase; the pool lives
// for the cycle's lifetime, the struct is never mutated after parsing,
// and `ngx_http_complex_value` is a read-only evaluator. Workers read the
// struct from inside their request handlers under nginx's single-threaded
// event loop. Crossing the type as `Send + Sync` between the master and
// every forked worker (the same shape as the SHM region) is the same
// soundness story.
unsafe impl Send for BindingLookup {}
unsafe impl Sync for BindingLookup {}

/// Compiled binding ready for the runtime — pairs the operator-visible
/// descriptor key with its [`BindingLookup`].
#[derive(Debug)]
pub struct CompiledBinding {
    pub key: String,
    pub lookup: BindingLookup,
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
    /// Optional per-rule predicate variable (nginx variable name with `$`
    /// stripped). When the variable resolves to a truthy value at request
    /// time, this rule is skipped. The empty string and the strings `"0"`,
    /// `"false"`, `"off"`, `"no"` (case-insensitive) are falsy; anything
    /// else is truthy. Resolved through the same binding machinery as the
    /// descriptor variables — but never counted toward request cardinality.
    pub except_if: Option<Box<str>>,
}

/// Compiled rule data ready for the runtime. Holds the gossip `Rule`, the
/// operator-facing name (so locations can be wired up by string), and the
/// per-request descriptor bindings already compiled into the
/// [`CompiledBinding`] form the access path can dispatch over. The hot-path
/// `RuleSpec` is reachable via `rule.spec()`.
#[derive(Debug)]
pub struct CompiledRule {
    pub name: String,
    pub rule: Rule,
    pub bindings: Vec<CompiledBinding>,
    /// Optional compiled predicate. When `Some` and the variable resolves
    /// to a truthy value at request time, the access path skips this rule
    /// — see `access::decide_one`.
    pub except_if: Option<BindingLookup>,
}

/// Composite of `RuleTable` + per-rule `Vec<DescriptorBinding>`. Shared (via
/// `Arc`) between every worker's location config and the leader's drain
/// task.
#[derive(Debug)]
pub struct CompiledRules {
    table: Arc<RuleTable>,
    rules: Vec<CompiledRule>,
}

/// Resolves binding sources into runtime-ready [`BindingLookup`] values.
///
/// The nginx adapter's production compiler calls `ngx_http_get_variable_index`
/// and `ngx_http_compile_complex_value`. Tests use [`NopBindingCompiler`],
/// which only knows about the inline fast-path arms and `IndexedVariable`
/// (with the index field a synthetic stand-in).
pub trait BindingCompiler {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Compile a variable source string (`$uri`, `$arg_tenant`, `$bot_class`,
    /// `"prefix-$foo-$bar"`, …) into a runtime-ready [`BindingLookup`].
    fn compile(&mut self, source: &str) -> Result<BindingLookup, Self::Error>;
}

/// Best-effort compiler used in tests and any non-FFI build. Maps inline
/// variables to their fast-path arms and synthesises an `IndexedVariable`
/// for anything else, with the index field set to zero. Always fails on
/// templates because the test harness has no `ngx_http_complex_value_t` to
/// hand back.
#[derive(Default)]
pub struct NopBindingCompiler;

impl BindingCompiler for NopBindingCompiler {
    type Error = RuleConfigError;

    fn compile(&mut self, source: &str) -> Result<BindingLookup, RuleConfigError> {
        compile_inline(source)
            .or_else(|| compile_single_variable(source, 0))
            .ok_or_else(|| RuleConfigError::UnsupportedBinding {
                spec: source.to_string(),
            })
    }
}

/// Map an inline-supported variable name to its fast-path arm. Returns
/// `None` for variables that don't have a hot-path bypass (in which case
/// the caller should fall back to `ngx_http_get_indexed_variable`).
pub fn compile_inline(source: &str) -> Option<BindingLookup> {
    let stripped = source.strip_prefix('$').unwrap_or(source);
    // Only single-identifier sources hit the fast path; templates with
    // literal text or multiple `$` substitutions fall through.
    if !is_single_ident(stripped) {
        return None;
    }
    match stripped {
        "uri" => Some(BindingLookup::Uri),
        "request_uri" => Some(BindingLookup::RequestUri),
        "args" => Some(BindingLookup::Args),
        "remote_addr" => Some(BindingLookup::RemoteAddr),
        other => other
            .strip_prefix("arg_")
            .map(|arg| BindingLookup::Arg(arg.into())),
    }
}

/// If `source` is a single `$identifier`, build an `IndexedVariable`
/// against the supplied index. Returns `None` for templates.
pub fn compile_single_variable(source: &str, index: i64) -> Option<BindingLookup> {
    let stripped = source.strip_prefix('$')?;
    if !is_single_ident(stripped) {
        return None;
    }
    Some(BindingLookup::IndexedVariable {
        name: stripped.into(),
        index,
    })
}

/// True if `s` is a single legal nginx variable identifier — alphanumeric +
/// `_`, starting with a letter or `_`.
pub fn is_single_ident(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

impl CompiledRules {
    /// Compile a list of `RuleConfig`s into a `RuleTable` + per-rule specs.
    /// Uses the default cardinality envelope and [`NopBindingCompiler`];
    /// for production paths, use [`Self::compile_with`] with an FFI-backed
    /// compiler.
    pub fn compile(configs: &[RuleConfig]) -> Result<Self, RuleConfigError> {
        Self::compile_with(
            configs,
            CardinalitySettings::default(),
            &mut NopBindingCompiler,
        )
    }

    /// Compile with operator-supplied cardinality limits and the default
    /// [`NopBindingCompiler`] (suitable for tests).
    pub fn compile_with_cardinality(
        configs: &[RuleConfig],
        cardinality: CardinalitySettings,
    ) -> Result<Self, RuleConfigError> {
        Self::compile_with(configs, cardinality, &mut NopBindingCompiler)
    }

    /// Full compile path: resolves each binding source through the supplied
    /// [`BindingCompiler`], including the per-rule `except_if` predicate.
    pub fn compile_with<C: BindingCompiler>(
        configs: &[RuleConfig],
        cardinality: CardinalitySettings,
        compiler: &mut C,
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
            let mut bindings = Vec::with_capacity(cfg.bindings.len());
            for binding in &cfg.bindings {
                let lookup = compiler.compile(&binding.source).map_err(|err| {
                    RuleConfigError::CompileBinding {
                        rule: cfg.name.clone(),
                        spec: binding.source.clone(),
                        message: err.to_string(),
                    }
                })?;
                bindings.push(CompiledBinding {
                    key: binding.key.clone(),
                    lookup,
                });
            }
            let except_if = match cfg.except_if.as_deref() {
                Some(var) => {
                    let source = format!("${var}");
                    let lookup = compiler.compile(&source).map_err(|err| {
                        RuleConfigError::CompileBinding {
                            rule: cfg.name.clone(),
                            spec: source,
                            message: err.to_string(),
                        }
                    })?;
                    Some(lookup)
                }
                None => None,
            };
            compiled.push(CompiledRule {
                name: cfg.name.clone(),
                rule,
                bindings,
                except_if,
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
    #[error(
        "rule {rule} could not compile binding `{spec}`: {message}; \
         to use this kind of binding the FFI-backed compiler is required"
    )]
    CompileBinding {
        rule: String,
        spec: String,
        message: String,
    },
    #[error(
        "binding source `{spec}` is not supported by the test compiler; \
         use a `$identifier` form or build with the `ngx-module` feature"
    )]
    UnsupportedBinding { spec: String },
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
            source: format!("${var}"),
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
            except_if: None,
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
