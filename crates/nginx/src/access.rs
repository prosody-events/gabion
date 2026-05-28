//! Per-request access decision. Pure logic — no `ngx` imports — so the
//! hot path is exercisable from unit tests.
//!
//! Allocation discipline: descriptors live in a stack-resident `ArrayVec`;
//! `RuleTable::matching` returns an iterator we walk without collecting;
//! the SHM aggregate is read through `AggregateTable::window_total` which
//! does only atomic loads.

use arrayvec::ArrayVec;
use gabion::defaults;
use gabion::rules::{Descriptor, EnforcementMode, hash_key};

use gabion::rules::RuleId;

use crate::rules::{BindingLookup, CompiledRules, MAX_DESCRIPTORS, RuleSpec};
use crate::shm::aggregate::AggregateTable;
use crate::shm::queue::{QueueEvent, RequestQueue};
use crate::shm::stats::Stats;

/// Cap on rules that may match a single request. Mirrors the server limit
/// (`gabion::defaults::STORAGE_MAX_MATCHED_RULES`).
const MAX_MATCHED_RULES: usize = defaults::STORAGE_MAX_MATCHED_RULES;

/// Maximum descriptor bytes per request (key + value, summed across all
/// descriptors plus the domain). Matches the server's default cardinality
/// envelope.
pub const MAX_DESCRIPTOR_BYTES: usize = defaults::STORAGE_MAX_DESCRIPTOR_BYTES;

/// Maximum per-descriptor key length in bytes.
pub const MAX_KEY_BYTES: usize = defaults::STORAGE_MAX_KEY_BYTES;

pub use crate::rules::CardinalitySettings;

/// Resolve a compiled binding against the in-flight request. The access
/// path borrows directly from nginx-owned buffers; the `&[u8]` returned
/// must live for the duration of `decide`. A return value of `None` skips
/// the rule for the request (fail-open under the allow-by-default
/// principle).
pub trait VariableLookup {
    fn lookup(&self, binding: &BindingLookup) -> Option<&[u8]>;
}

/// Result of the access decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessOutcome {
    Allow,
    /// Rate limit exceeded. Carries the rule-specific info the headers
    /// builder needs.
    Reject(RejectInfo),
    /// Cardinality envelope violated. Map to `400 Bad Request`.
    Cardinality,
    /// No rule matched or a referenced variable was missing — let the
    /// request through without recording a hit.
    Decline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RejectInfo {
    pub spec: RuleSpec,
    pub total: u64,
    pub now_millis: u64,
    /// Wall-clock ms from `now_millis` until a request of the same weight
    /// would be admitted under the sliding-window model. Computed once at
    /// reject time so `Retry-After` / `X-RateLimit-Reset` are a pure
    /// format step.
    pub delta_until_admit_millis: u64,
}

/// Borrowed view of the rule set + SHM region passed into the access path.
/// One per nginx worker per zone.
#[derive(Clone, Copy)]
pub struct AccessCtx<'a> {
    pub rules: &'a CompiledRules,
    pub aggregate: AggregateTable<'a>,
    pub queue: RequestQueue<'a>,
    pub stats: &'a Stats,
    /// Default domain assigned to requests when the location config doesn't
    /// override it.
    pub domain: &'a str,
    pub cardinality: CardinalitySettings,
}

/// Per-rule outcome before [`decide_all`] folds it into a single
/// [`AccessOutcome`] for the request.
#[derive(Clone, Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "the Allow variant carries the plan-then-commit ArrayVec on the stack; boxing it \
              would force a heap allocation on the hot path, violating the no-alloc-on-admit rule \
              in CLAUDE.md."
)]
enum RuleOutcome {
    /// Rule evaluated cleanly and would allow the request. Buffered queue
    /// events ride along so a sibling-rule reject can drop them without
    /// committing partial work.
    Allow(ArrayVec<QueueEvent, MAX_MATCHED_RULES>),
    /// Rule evaluated and crossed its configured limit while in
    /// `Enforce` mode.
    Reject(RejectInfo),
    /// Rule had no opinion on this request — a binding variable was
    /// missing or no `RuleTable::matching` row applied. Fail-open: the
    /// request still allows for this rule.
    Decline,
    /// Rule was over the per-request byte budget. The sibling rules may
    /// still evaluate independently; if *every* configured rule
    /// cardinality-skipped, [`decide_all`] surfaces the operator-visible
    /// 400 just like the single-rule path.
    Cardinality,
    /// Predicate `except_if=` resolved truthy; rule is skipped for this
    /// request. Carries the rule id so the orchestrator can attribute the
    /// per-rule exempt counter to the right slot.
    Exempt(RuleId),
}

/// Evaluate one request against the rule indicated by `rule_index`.
/// Wraps [`decide_all`] for the common single-rule case (and the legacy
/// in-crate tests).
pub fn decide(
    ctx: AccessCtx<'_>,
    rule_index: usize,
    vars: &impl VariableLookup,
    now_millis: u64,
) -> AccessOutcome {
    decide_all(ctx, &[rule_index], vars, now_millis)
}

/// Evaluate one request against every rule in `rule_indices`. Rules
/// stack: rejection from any enforcing rule rejects the request, with
/// `Retry-After` and the `X-RateLimit-*` triplet pinned to the rule with
/// the longest window (so the client doesn't see a short retry that
/// puts it right back into 429 against the wider rule).
pub fn decide_all(
    ctx: AccessCtx<'_>,
    rule_indices: &[usize],
    vars: &impl VariableLookup,
    now_millis: u64,
) -> AccessOutcome {
    ctx.stats.record_request();

    let mut worst_reject: Option<RejectInfo> = None;
    let mut all_events: ArrayVec<QueueEvent, MAX_MATCHED_RULES> = ArrayVec::new();
    let mut had_non_exempt_outcome = false;
    let mut any_exempt = false;
    let mut any_cardinality = false;

    for &index in rule_indices {
        match decide_one(ctx, index, vars, now_millis) {
            RuleOutcome::Allow(events) => {
                had_non_exempt_outcome = true;
                for ev in events {
                    if all_events.try_push(ev).is_err() {
                        // The combined buffer can hold up to one event per
                        // matched-rules slot; if siblings push us past
                        // that, treat the overflow like the single-rule
                        // case: under-count rather than over-reject.
                        ctx.stats.record_matched_rule_overflow();
                        break;
                    }
                }
            }
            RuleOutcome::Reject(info) => {
                had_non_exempt_outcome = true;
                worst_reject = Some(pick_longest_window(worst_reject, info));
            }
            RuleOutcome::Decline => {
                // Rule had no opinion (missing variable, no match, etc.).
                // Siblings still get to evaluate.
            }
            RuleOutcome::Cardinality => {
                any_cardinality = true;
                ctx.stats.record_cardinality_reject();
            }
            RuleOutcome::Exempt(rule_id) => {
                any_exempt = true;
                ctx.stats.record_exempt(rule_id);
            }
        }
    }

    if let Some(info) = worst_reject {
        ctx.stats.record_reject();
        return AccessOutcome::Reject(info);
    }
    if had_non_exempt_outcome {
        for event in all_events {
            match ctx.queue.push(event) {
                Ok(()) => ctx.stats.record_queue_push(),
                Err(_) => ctx.stats.record_queue_drop(),
            }
        }
        ctx.stats.record_allow();
        return AccessOutcome::Allow;
    }
    if any_exempt {
        // Exempt-only outcome: every rule's `except_if=` predicate fired,
        // so the request is allowed through without crediting either the
        // generic `allowed` counter or the gossip aggregate.
        return AccessOutcome::Allow;
    }
    if any_cardinality {
        return AccessOutcome::Cardinality;
    }
    AccessOutcome::Decline
}

/// Per-rule worker for [`decide_all`]. Builds descriptors on the stack,
/// runs `RuleTable::matching`, and returns the per-rule outcome without
/// committing to the queue — the orchestrator does the commit only if no
/// enforcing rule rejected the request.
fn decide_one(
    ctx: AccessCtx<'_>,
    rule_index: usize,
    vars: &impl VariableLookup,
    now_millis: u64,
) -> RuleOutcome {
    let Some(compiled) = ctx.rules.get(rule_index) else {
        return RuleOutcome::Decline;
    };

    // Predicate gate. Evaluated before descriptors so a truthy predicate
    // exempts the request without billing the cardinality budget. A
    // predicate variable that resolves to None (variable absent at request
    // time) falls through as "rule applies" — same fail-open shape as a
    // missing descriptor variable.
    if let Some(predicate) = compiled.except_if.as_ref()
        && let Some(value) = vars.lookup(predicate)
        && is_truthy(value)
    {
        return RuleOutcome::Exempt(compiled.rule.id);
    }

    // Build descriptors on the stack in one pass over `bindings`. Variable
    // misses decline; UTF-8 failures decline (and bump a stat so the bypass
    // is observable); the dynamic byte budget is the only per-request
    // cardinality check — count and key length are gated at compile time.
    let mut descriptors: ArrayVec<Descriptor<'_>, MAX_DESCRIPTORS> = ArrayVec::new();
    let mut bytes = ctx.domain.len();
    for binding in &compiled.bindings {
        let Some(value) = vars.lookup(&binding.lookup) else {
            return RuleOutcome::Decline;
        };
        bytes = bytes
            .saturating_add(binding.key.len())
            .saturating_add(value.len());
        if bytes > ctx.cardinality.max_descriptor_bytes {
            return RuleOutcome::Cardinality;
        }
        let value_str = match std::str::from_utf8(value) {
            Ok(s) => s,
            Err(_) => {
                ctx.stats.record_decline_invalid_descriptor();
                return RuleOutcome::Decline;
            }
        };
        descriptors.push(Descriptor {
            key: &binding.key,
            value: value_str,
        });
    }

    // Evaluate this one specific rule. The rule's `RuleTable` row is
    // looked up by id; we don't walk `RuleTable::matching` here because
    // (a) the descriptors were built from this rule's bindings — the
    // pattern matches by construction — and (b) under multi-rule per
    // location, each `rule_index` evaluates exactly that rule, not every
    // sibling that happens to share the same descriptor shape.
    let descriptors_slice: &[Descriptor<'_>] = descriptors.as_slice();
    let rule = &compiled.rule;
    if rule.mode == EnforcementMode::Disabled {
        return RuleOutcome::Decline;
    }
    let spec = rule.spec();
    let key_hash = hash_key(spec.id, ctx.domain, descriptors_slice);
    let total = ctx.aggregate.window_total(
        spec.fingerprint,
        key_hash.0,
        now_millis,
        spec.bucket_millis,
        spec.live_buckets,
    );
    // DryRun rules count and gossip but never reject — they let operators
    // observe what enforcement would do before flipping the switch.
    let hits = 1u64;
    if total.saturating_add(hits) > spec.limit && rule.mode == EnforcementMode::Enforce {
        let delta_until_admit_millis = ctx
            .aggregate
            .time_until_admit_millis(spec, key_hash.0, now_millis, total, hits);
        return RuleOutcome::Reject(RejectInfo {
            spec,
            total,
            now_millis,
            delta_until_admit_millis,
        });
    }
    let bucket = (now_millis / spec.bucket_millis) as u32;
    let mut planned: ArrayVec<QueueEvent, MAX_MATCHED_RULES> = ArrayVec::new();
    planned.push(QueueEvent {
        rule_fingerprint: spec.fingerprint,
        key_hash: key_hash.0,
        bucket,
        hits: 1,
        rule_limit: spec.limit,
        now_millis,
    });
    RuleOutcome::Allow(planned)
}

/// Pick the rule with the longer window among a candidate and a current
/// "worst" choice. `Retry-After` and the rate-limit headers come from
/// this rule so the client never sees a short retry that puts it right
/// back into 429 against the wider rule.
fn pick_longest_window(current: Option<RejectInfo>, candidate: RejectInfo) -> RejectInfo {
    match current {
        Some(c) if c.spec.window_millis >= candidate.spec.window_millis => c,
        _ => candidate,
    }
}

/// True if `value` should be treated as "truthy" by the `except_if=`
/// predicate. An empty value is falsy. The strings `0`, `false`, `off`,
/// and `no` (case-insensitive) are falsy — matching what operators write
/// in `set $trusted off;` or `map ... { default no; }`. Anything else,
/// including `1`, `true`, `yes`, and any non-empty arbitrary string, is
/// truthy.
pub fn is_truthy(value: &[u8]) -> bool {
    if value.is_empty() {
        return false;
    }
    // Case-insensitive ASCII comparison without allocating.
    !matches!(
        value,
        b"0" | b"false" | b"False" | b"FALSE" | b"off" | b"Off" | b"OFF" | b"no" | b"No" | b"NO"
    )
}

#[cfg(test)]
mod mock;
#[cfg(test)]
pub(crate) use mock::MockVars;
#[cfg(test)]
mod tests;
