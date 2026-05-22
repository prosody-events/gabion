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
    pub key: Box<str>,
    /// The variable specification as it appears in config after the `key`
    /// prefix is stripped: a single `$identifier` for the indexed-variable
    /// fast path or a template (anything that includes literal text or
    /// multiple `$identifier` substitutions) compiled via
    /// `ngx_http_compile_complex_value` at config phase.
    pub source: Box<str>,
}

/// Runtime-ready binding produced by compiling a [`DescriptorBinding`].
///
/// Resolution dispatches on three cases:
///
/// * Inline fast-path arms (`$uri`, `$args`, `$remote_addr`, …) read straight
///   off the nginx request struct with no FFI hop.
/// * `IndexedVariable` resolves single-variable bindings (`$geoip2_asn`,
///   `$bot_class`, anything else) through `ngx_http_get_indexed_variable` —
///   O(1) array lookup, zero allocation per request.
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
    IndexedVariable {
        name: Box<str>,
        index: i64,
    },
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
    pub key: Box<str>,
    pub lookup: BindingLookup,
}

/// Operator-facing rule configuration. Parsed from `gabion_limit_rule`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleConfig {
    pub name: Box<str>,
    pub domain: Box<str>,
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
    pub name: Box<str>,
    pub rule: Rule,
    pub bindings: Box<[CompiledBinding]>,
    /// Optional compiled predicate. When `Some` and the variable resolves
    /// to a truthy value at request time, the access path skips this rule
    /// — see `access::decide_one`.
    pub except_if: Option<BindingLookup>,
}

/// Composite of `RuleTable` + per-rule [`CompiledBinding`]s. Shared (via
/// `Arc`) between every worker's location config and the leader's drain
/// task.
#[derive(Debug)]
pub struct CompiledRules {
    table: Arc<RuleTable>,
    rules: Box<[CompiledRule]>,
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

/// Match a descriptor key against `[A-Za-z_][A-Za-z0-9_.-]*`. Identifier-
/// like with `.` and `-` so operator-readable names (`tenant-id`,
/// `app.tenant`) round-trip through the directive parser and the YAML
/// adapter unchanged.
pub fn is_descriptor_key(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c == '.' || c == '-' || c.is_ascii_alphanumeric())
}

/// Match an nginx-style zone name against `[A-Za-z0-9_]+`, the same shape
/// nginx core's `limit_req_zone` imposes on its own zone names.
pub fn is_zone_name(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c == '_' || c.is_ascii_alphanumeric())
}

/// Match a Kubernetes DNS label per RFC 1123:
/// `[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?`. Lower-case, 1–63 chars, must
/// start and end with `[a-z0-9]`. Used for namespace / service allowlist
/// arguments so a typo doesn't silently match nothing.
pub fn is_dns_label(s: &str) -> bool {
    let len = s.len();
    if !(1..=63).contains(&len) {
        return false;
    }
    let bytes = s.as_bytes();
    let head_ok = bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit();
    let tail_ok = bytes[len - 1].is_ascii_lowercase() || bytes[len - 1].is_ascii_digit();
    if !head_ok || !tail_ok {
        return false;
    }
    bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// Parse a `rate=` value into `(count, period)`.
///
/// The suffix after `r/` is either a single-letter unit (`s`, `m`, `h`,
/// `d`) or a [humantime] duration (`30s`, `5m`, `2h30m`). Zero counts and
/// zero periods are rejected at parse time so `cfg.limit.max(1)` is never
/// load-bearing in downstream code.
///
/// [humantime]: https://docs.rs/humantime
pub fn parse_rate(input: &str) -> Result<(u64, Duration), &'static str> {
    let input = input.trim();
    let Some((number, period)) = input.split_once("r/") else {
        return Err(
            "expected `rate=Nr/<unit>` where unit is `s|m|h|d` or a duration (e.g. `rate=100r/s`, \
             `rate=10r/5m`)",
        );
    };
    let count: u64 = number
        .parse()
        .map_err(|_| "rate count must be a non-negative integer (e.g. `rate=100r/s`)")?;
    if count == 0 {
        return Err("`rate=` must be a positive integer; zero would deny all traffic");
    }
    let duration = match period {
        "s" => Duration::from_secs(1),
        "m" => Duration::from_secs(60),
        "h" => Duration::from_secs(60 * 60),
        "d" => Duration::from_secs(60 * 60 * 24),
        other => humantime::parse_duration(other).map_err(|_| {
            "rate period must be `s`, `m`, `h`, `d`, or a duration like `30s`, `5m`"
        })?,
    };
    if duration.is_zero() {
        return Err("rate period must be greater than zero");
    }
    Ok((count, duration))
}

/// Parse a positional descriptor binding argument. Accepts:
/// - `$uri` → key="uri", source="$uri" (auto-keyed; only when the source is a
///   single legal `$identifier`)
/// - `tenant:$http_x_tenant` → key="tenant", source="$http_x_tenant"
/// - `combo:prefix-$asn-$ua` → key="combo", source="prefix-$asn-$ua" (template;
///   compiled via `ngx_http_compile_complex_value` at config phase)
///
/// Templates (anything with literal text or multiple `$` substitutions)
/// require the explicit `key:source` form — there's no useful auto-name
/// to derive. Keys are validated against [`is_descriptor_key`] so spaces,
/// punctuation, or anything else not matching `[A-Za-z_][A-Za-z0-9_.-]*`
/// is rejected at parse time. Keys longer than the default
/// `gabion_storage_max_key_bytes` are also rejected here so the error
/// names the directive line (a second, authoritative check runs at
/// compile time once any per-zone override takes effect).
pub fn parse_binding(rest: &str) -> Result<DescriptorBinding, &'static str> {
    if let Some(stripped) = rest.strip_prefix('$') {
        if is_single_ident(stripped) {
            if stripped.len() > defaults::STORAGE_MAX_KEY_BYTES {
                return Err(
                    "binding key exceeds the default `gabion_storage_max_key_bytes` budget; \
                     tighten the key or raise the directive",
                );
            }
            return Ok(DescriptorBinding {
                key: stripped.into(),
                source: rest.into(),
            });
        }
        return Err(
            "expected `$variable`, `name:$variable`, or one of `rate=`, `bucket=`, `mode=`, \
             `dry_run`, `except_if=`, `domain=`",
        );
    }
    let Some((key, source)) = rest.split_once(':') else {
        return Err(
            "expected `$variable`, `name:$variable`, or one of `rate=`, `bucket=`, `mode=`, \
             `dry_run`, `except_if=`, `domain=`",
        );
    };
    if !is_descriptor_key(key) {
        return Err(
            "binding key must match `[A-Za-z_][A-Za-z0-9_.-]*` (e.g. `tenant`, `app.tenant`, \
             `tenant-id`)",
        );
    }
    if source.is_empty() {
        return Err("binding source after `:` is empty; supply a `$variable` or template");
    }
    if key.len() > defaults::STORAGE_MAX_KEY_BYTES {
        return Err(
            "binding key exceeds the default `gabion_storage_max_key_bytes` budget; tighten the \
             key or raise the directive",
        );
    }
    Ok(DescriptorBinding {
        key: key.into(),
        source: source.into(),
    })
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
                return Err(RuleConfigError::EmptyBindings(cfg.name.to_string()));
            }
            if cfg.bindings.len() > cardinality.max_descriptor_count {
                return Err(RuleConfigError::TooManyBindings(cfg.name.to_string()));
            }
            for binding in &cfg.bindings {
                if binding.key.len() > cardinality.max_key_bytes {
                    return Err(RuleConfigError::KeyTooLong {
                        rule: cfg.name.to_string(),
                        key: binding.key.to_string(),
                    });
                }
            }
            let id: RuleId = (idx + 1) as RuleId;
            let descriptors: Vec<DescriptorPattern> = cfg
                .bindings
                .iter()
                .map(|b| DescriptorPattern {
                    key: b.key.clone(),
                    value: Box::from("*"),
                })
                .collect();
            if cfg.limit == 0 {
                return Err(RuleConfigError::ZeroLimit(cfg.name.to_string()));
            }
            let rule = Rule::new(
                id,
                cfg.domain.to_string(),
                descriptors,
                cfg.limit,
                duration_millis(cfg.window),
                duration_millis(cfg.bucket),
                cfg.mode,
            );
            rules.push(rule.clone());
            let mut bindings = Vec::with_capacity(cfg.bindings.len());
            for binding in &cfg.bindings {
                let lookup = compiler.compile(&binding.source).map_err(|err| {
                    RuleConfigError::CompileBinding {
                        rule: cfg.name.to_string(),
                        spec: binding.source.to_string(),
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
                            rule: cfg.name.to_string(),
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
                bindings: bindings.into_boxed_slice(),
                except_if,
            });
        }
        Ok(Self {
            table: Arc::new(RuleTable::new(rules)),
            rules: compiled.into_boxed_slice(),
        })
    }

    pub fn table(&self) -> &Arc<RuleTable> {
        &self.table
    }

    pub fn rules(&self) -> &[CompiledRule] {
        &self.rules
    }

    pub fn by_name(&self, name: &str) -> Option<&CompiledRule> {
        self.rules.iter().find(|r| &*r.name == name)
    }

    pub fn index_by_name(&self, name: &str) -> Option<usize> {
        self.rules.iter().position(|r| &*r.name == name)
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
    #[error("rule {0} has `rate=0`, which would deny all traffic")]
    ZeroLimit(String),
    #[error("rule {rule} binding key '{key}' exceeds max_key_bytes")]
    KeyTooLong { rule: String, key: String },
    #[error(
        "rule {rule} could not compile binding `{spec}`: {message}; to use this kind of binding \
         the FFI-backed compiler is required"
    )]
    CompileBinding {
        rule: String,
        spec: String,
        message: String,
    },
    #[error(
        "binding source `{spec}` is not supported by the test compiler; use a `$identifier` form \
         or build with the `ngx-module` feature"
    )]
    UnsupportedBinding { spec: String },
}

fn duration_millis(d: Duration) -> u64 {
    d.as_millis().try_into().unwrap_or(u64::MAX).max(1)
}

#[cfg(test)]
mod tests;
