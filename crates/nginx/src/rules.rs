//! Bridge from `gabion_limit_rule` configuration to `gabion::rules::Rule`
//! plus compiled bindings.

use std::time::Duration;

use gabion::crdt::ValidStorageConfig;
use gabion::rules::{EnforcementMode, Rule, RuleDef, RuleError, RuleTable, parse_rate};
use thiserror::Error;

/// Default domain assigned to rules that don't name one explicitly. nginx
/// requests carry no inherent "domain" the way Envoy descriptors do, so we
/// default to a single per-zone bucket.
pub const DEFAULT_DOMAIN: &str = "nginx";

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
    /// The value pattern: `*` (any value) unless `value=KEY:LITERAL` names
    /// this binding.
    pub value: Box<str>,
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

/// Operator-facing rule configuration, parsed from `gabion_limit_rule` by
/// [`parse_rule_directive`] and built into a [`Rule`] by
/// [`CompiledRules::compile_with`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleConfig {
    pub name: Box<str>,
    pub domain: Box<str>,
    pub bindings: Box<[DescriptorBinding]>,
    /// `rate=`, whose syntax [`parse_rate`] already accepted.
    pub rate: Box<str>,
    pub window: Option<Duration>,
    pub bucket: Option<Duration>,
    pub mode: EnforcementMode,
    /// Optional per-rule predicate variable (nginx variable name with `$`
    /// stripped). When the variable resolves to a truthy value at request
    /// time, this rule is skipped. The empty string and the strings `"0"`,
    /// `"false"`, `"off"`, `"no"` (case-insensitive) are falsy; anything
    /// else is truthy. Resolved through the same binding machinery as the
    /// descriptor variables — but never counted toward request cardinality.
    pub except_if: Option<Box<str>>,
}

/// One rule's compiled request-time bindings, in the order of its
/// [`Rule::descriptors`], whose patterns give each binding's key and value.
#[derive(Debug)]
pub struct CompiledRule {
    pub bindings: Box<[BindingLookup]>,
    /// Optional compiled predicate. When `Some` and the variable resolves
    /// to a truthy value at request time, the access path skips this rule
    /// — see `access::decide_one`.
    pub except_if: Option<BindingLookup>,
}

/// Every compiled `gabion_limit_rule`, in declaration order: the library's
/// [`RuleTable`] and, at the same index, each rule's compiled bindings.
/// Shared (via `Arc`) between every worker's location config and the
/// leader's drain task.
#[derive(Debug)]
pub struct CompiledRules {
    table: RuleTable,
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

/// Best-effort compiler used by unit tests (via `rules::mock`) and
/// tests/safety.rs. Maps inline
/// variables to their fast-path arms and synthesises an `IndexedVariable`
/// for anything else, with the index field set to zero. Always fails on
/// templates because the test harness has no `ngx_http_complex_value_t` to
/// hand back.
pub struct NopBindingCompiler;

impl BindingCompiler for NopBindingCompiler {
    type Error = RuleConfigError;

    fn compile(&mut self, source: &str) -> Result<BindingLookup, RuleConfigError> {
        compile_inline(source)
            .or_else(|| compile_single_variable(source, 0))
            .ok_or_else(|| RuleConfigError::UnsupportedBinding {
                spec: source.into(),
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

/// Match an nginx-style zone name against `[A-Za-z0-9_]+`, the same shape
/// nginx core's `limit_req_zone` imposes on its own zone names.
pub fn is_zone_name(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c == '_' || c.is_ascii_alphanumeric())
}

const BINDING_EXPECTED: &str = "expected `$variable`, `name:$variable`, or one of `rate=`, \
                                `window=`, `bucket=`, `mode=`, `dry_run`, `except_if=`, \
                                `domain=`, `value=`";

/// Parse a positional descriptor binding argument. Accepts:
/// - `$uri` → key="uri", source="$uri" (auto-keyed; only when the source is a
///   single legal `$identifier`)
/// - `tenant:$http_x_tenant` → key="tenant", source="$http_x_tenant"
/// - `combo:prefix-$asn-$ua` → key="combo", source="prefix-$asn-$ua" (template;
///   compiled via `ngx_http_compile_complex_value` at config phase)
///
/// Templates (anything with literal text or multiple `$` substitutions)
/// require the explicit `key:source` form — there's no useful auto-name
/// to derive. The key grammar is the library's, checked when the rule is
/// built.
pub fn parse_binding(rest: &str) -> Result<DescriptorBinding, &'static str> {
    if let Some(stripped) = rest.strip_prefix('$') {
        if is_single_ident(stripped) {
            return Ok(DescriptorBinding {
                key: stripped.into(),
                source: rest.into(),
                value: "*".into(),
            });
        }
        return Err(BINDING_EXPECTED);
    }
    let Some((key, source)) = rest.split_once(':') else {
        return Err(BINDING_EXPECTED);
    };
    if source.is_empty() {
        return Err("binding source after `:` is empty; supply a `$variable` or template");
    }
    Ok(DescriptorBinding {
        key: key.into(),
        source: source.into(),
        value: "*".into(),
    })
}

/// Parse one `gabion_limit_rule` directive's arguments: the rule name, then
/// descriptor bindings — `$var` auto-keyed by the variable name, or
/// `name:$var` with an explicit key — and named arguments recognised by
/// their `keyword=` prefix; the bare `dry_run` flag is an alias for
/// `mode=dry_run`.
///
/// `rate=` carries the sustained allowance, `window=` widens the time
/// horizon (defaults to the rate's period), and `bucket=` controls the
/// granularity inside the window (defaults to the window). `value=KEY:LITERAL`
/// makes the binding keyed `KEY` match only `LITERAL`. Argument syntax is
/// reported here, at the directive's line; key grammar, durations and
/// identity when the rule is built by [`CompiledRules::compile_with`], once
/// every setting is read.
pub fn parse_rule_directive(args: &[&str]) -> Result<RuleConfig, RuleArgError> {
    let Some((&name, args)) = args.split_first() else {
        return Err(RuleArgError::MissingName);
    };
    let invalid = |arg: &str, reason| RuleArgError::Invalid {
        arg: arg.into(),
        reason,
    };
    let mut domain = DEFAULT_DOMAIN;
    let (mut window, mut bucket, mut except_if) = (None, None, None);
    let mut bindings = Vec::new();
    let mut values = Vec::new();
    let mut rate = None;
    let mut mode = None;
    let mut dry_run = false;
    for &arg in args {
        // `window=` and `bucket=` must parse as durations here; a zero one
        // is refused when the rule is built, in the words gabiond uses too.
        let duration = |rest: &str, expected| {
            humantime::parse_duration(rest).map_err(|_| invalid(arg, expected))
        };
        if arg == "dry_run" {
            dry_run = true;
        } else if let Some(rest) = arg.strip_prefix("rate=") {
            parse_rate(rest).map_err(|reason| invalid(arg, reason))?;
            rate = Some(rest);
        } else if let Some(rest) = arg.strip_prefix("window=") {
            window = Some(duration(
                rest,
                "expected `window=DURATION` (e.g. `window=5h`)",
            )?);
        } else if let Some(rest) = arg.strip_prefix("bucket=") {
            bucket = Some(duration(
                rest,
                "expected `bucket=DURATION` (e.g. `bucket=1s`)",
            )?);
        } else if let Some(rest) = arg.strip_prefix("domain=") {
            domain = rest;
        } else if let Some(rest) = arg.strip_prefix("mode=") {
            mode = Some(match rest {
                "enforce" => EnforcementMode::Enforce,
                "dry_run" => EnforcementMode::DryRun,
                "disabled" => EnforcementMode::Disabled,
                _ => return Err(invalid(arg, "expected `mode=enforce|dry_run|disabled`")),
            });
        } else if let Some(rest) = arg.strip_prefix("except_if=") {
            let var = rest.strip_prefix('$').unwrap_or(rest);
            if !is_single_ident(var) {
                return Err(invalid(arg, "`except_if=` expects `$variable_name`"));
            }
            except_if = Some(var.into());
        } else if let Some(rest) = arg.strip_prefix("value=") {
            match rest.split_once(':') {
                Some((key, literal)) if !key.is_empty() => values.push((key, literal)),
                _ => {
                    return Err(invalid(
                        arg,
                        "expected `value=KEY:LITERAL` (e.g. `value=tenant:acme`)",
                    ));
                }
            }
        } else {
            bindings.push(parse_binding(arg).map_err(|reason| invalid(arg, reason))?);
        }
    }
    if dry_run && mode.is_some_and(|m| m != EnforcementMode::DryRun) {
        return Err(RuleArgError::DryRunConflict { rule: name.into() });
    }
    let Some(rate) = rate else {
        return Err(RuleArgError::MissingRate { rule: name.into() });
    };
    for (key, literal) in values {
        let Some(binding) = bindings.iter_mut().find(|b| &*b.key == key) else {
            return Err(RuleArgError::UnknownValueKey {
                rule: name.into(),
                key: key.into(),
            });
        };
        binding.value = literal.into();
    }
    Ok(RuleConfig {
        name: name.into(),
        domain: domain.into(),
        bindings: bindings.into_boxed_slice(),
        rate: rate.into(),
        window,
        bucket,
        mode: if dry_run {
            EnforcementMode::DryRun
        } else {
            mode.unwrap_or_default()
        },
        except_if,
    })
}

/// A `gabion_limit_rule` directive `nginx -t` refuses at its line.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum RuleArgError {
    #[error("requires a rule name: `gabion_limit_rule NAME $variable rate=10r/s;`")]
    MissingName,
    #[error("argument `{arg}` is invalid: {reason}")]
    Invalid { arg: Box<str>, reason: &'static str },
    #[error(
        "rule `{rule}` sets both `dry_run` and a different `mode=`; remove one of them, then run \
         `nginx -t`"
    )]
    DryRunConflict { rule: Box<str> },
    #[error(
        "rule `{rule}` has no `rate=`, so it has no limit to enforce. Add one (for example \
         `rate=10r/s`), then run `nginx -t`"
    )]
    MissingRate { rule: Box<str> },
    #[error(
        "`value={key}:…` in rule `{rule}` names no binding of that rule. Add a binding with key \
         `{key}` (for example `{key}:$arg_{key}`) or correct the key. Run `nginx -t` again"
    )]
    UnknownValueKey { rule: Box<str>, key: Box<str> },
}

impl CompiledRules {
    /// The one constructor: builds every rule through the library and
    /// resolves each binding source through the supplied [`BindingCompiler`],
    /// including the per-rule `except_if` predicate. Tests pass
    /// [`NopBindingCompiler`].
    pub fn compile_with<C: BindingCompiler>(
        configs: &[RuleConfig],
        storage: &ValidStorageConfig,
        compiler: &mut C,
    ) -> Result<Self, RuleConfigError> {
        let rules = configs
            .iter()
            .map(|cfg| {
                let patterns: Box<[(&str, &str)]> =
                    cfg.bindings.iter().map(|b| (&*b.key, &*b.value)).collect();
                RuleDef {
                    name: &cfg.name,
                    domain: &cfg.domain,
                    patterns: &patterns,
                    rate: &cfg.rate,
                    window: cfg.window,
                    bucket: cfg.bucket,
                    mode: cfg.mode,
                }
                .build(&storage.cardinality)
            })
            .collect::<Result<_, _>>()?;
        let table = RuleTable::new(rules, storage.rule_dictionary_capacity())?;
        let mut compile = |rule: &str, source: &str| {
            compiler
                .compile(source)
                .map_err(|err| RuleConfigError::CompileBinding {
                    rule: rule.into(),
                    spec: source.into(),
                    message: err.to_string().into(),
                })
        };
        let rules = configs
            .iter()
            .map(|cfg| {
                let bindings = cfg
                    .bindings
                    .iter()
                    .map(|binding| compile(&cfg.name, &binding.source))
                    .collect::<Result<_, _>>()?;
                let except_if = cfg
                    .except_if
                    .as_deref()
                    .map(|var| compile(&cfg.name, &format!("${var}")))
                    .transpose()?;
                Ok(CompiledRule {
                    bindings,
                    except_if,
                })
            })
            .collect::<Result<_, RuleConfigError>>()?;
        Ok(Self { table, rules })
    }

    pub fn table(&self) -> &RuleTable {
        &self.table
    }

    /// The rule at `index` and its compiled bindings.
    pub fn get(&self, index: usize) -> Option<(&Rule, &CompiledRule)> {
        Some((self.table.rules().get(index)?, self.rules.get(index)?))
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum RuleConfigError {
    #[error(transparent)]
    Rule(#[from] RuleError),
    #[error(
        "rule `{rule}`: nginx could not compile binding `{spec}`: {message}. Fix the rule, then \
         run `nginx -t`."
    )]
    CompileBinding {
        rule: Box<str>,
        spec: Box<str>,
        message: Box<str>,
    },
    #[error(
        "binding source `{spec}` is not supported by the test compiler; use a `$identifier` form \
         or build with the `ngx-module` feature"
    )]
    UnsupportedBinding { spec: Box<str> },
}

#[cfg(test)]
pub(crate) mod mock;
#[cfg(test)]
mod tests;
