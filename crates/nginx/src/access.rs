//! Per-request access decision. Pure logic — no `ngx` imports — so the
//! hot path is exercisable from unit tests.
//!
//! Allocation discipline: each binding's value is fed into the rule's
//! `KeyHasher` as it is read, so no buffer holds descriptors; the matched
//! rules' events go to the worker's admission scratch, reserved once; and the
//! SHM aggregate is read through `AggregateTable::window_total`, which does
//! only atomic loads.

use std::num::NonZeroUsize;

use gabion::aggregate::AggregateTable;
use gabion::rules::{CardinalityLimits, EnforcementMode, KeyHasher, RuleSpec};

use crate::rules::{BindingLookup, CompiledRules};
use crate::shm::queue::{QueueEvent, RequestQueue};
use crate::shm::stats::Stats;

/// Resolve a compiled binding against the in-flight request. The access
/// path borrows directly from nginx-owned buffers; the `&[u8]` returned
/// must live for the duration of `decide_all`. A return value of `None` skips
/// the rule for the request (fail-open under the allow-by-default
/// principle).
pub trait VariableLookup {
    fn lookup(&self, binding: &BindingLookup) -> Option<&[u8]>;
}

/// The value of the first `name=value` pair in the query string `args`, the
/// name matched without regard to ASCII case as nginx's own `$arg_name`
/// (`ngx_http_arg`) matches it: what a `$arg_name` binding reads.
pub fn find_query_arg<'a>(args: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    args.split(|byte| *byte == b'&').find_map(|pair| {
        let eq = pair.iter().position(|byte| *byte == b'=')?;
        pair[..eq]
            .eq_ignore_ascii_case(name)
            .then(|| &pair[eq + 1..])
    })
}

/// Result of the access decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessOutcome {
    /// Request was admitted. Carries optional quota context for the
    /// rule the client is closest to tripping — the headers builder
    /// renders `X-RateLimit-*` from it. `None` means no rule with a
    /// meaningful quota matched (e.g. every rule declined or was
    /// exempt), so no headers should be emitted.
    Allow(Option<AllowInfo>),
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

/// Per-request quota snapshot for the rule with the smallest budget
/// remaining. Drives the `X-RateLimit-*` triplet on allowed responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AllowInfo {
    pub spec: RuleSpec,
    /// Budget left under this rule after admitting the current request.
    /// Saturating: a `DryRun` rule already over its limit reports `0`.
    pub remaining: u64,
    pub now_millis: u64,
}

/// Borrowed view of the rule set + SHM region passed into the access path.
/// One per nginx worker per zone.
#[derive(Clone, Copy)]
pub struct AccessCtx<'a> {
    pub rules: &'a CompiledRules,
    pub aggregate: AggregateTable<'a>,
    pub queue: RequestQueue<'a>,
    pub stats: &'a Stats,
    pub cardinality: CardinalityLimits,
    /// `gabion_storage_max_matched_rules`: the most rules one request is
    /// counted against.
    pub max_matched_rules: NonZeroUsize,
}

/// Per-rule outcome before [`decide_all`] folds it into a single
/// [`AccessOutcome`] for the request.
#[derive(Clone, Copy, Debug)]
enum RuleOutcome {
    /// Rule evaluated cleanly and would allow the request. Its queue event
    /// rides along so a sibling-rule reject can drop it without committing
    /// partial work, and the rule's quota snapshot feeds the
    /// most-constrained-rule selection that drives the `X-RateLimit-*`
    /// headers on the allowed response.
    Allow(QuotaSnapshot, QueueEvent),
    /// Rule evaluated and crossed its configured limit while in
    /// `Enforce` mode.
    Reject(RejectInfo),
    /// Rule had no opinion on this request — a binding variable was
    /// missing or not UTF-8, or the value differs from the rule's `value=`
    /// literal, or the rule is disabled. Fail-open: the request still allows
    /// for this rule.
    Decline,
    /// Rule was over the per-request byte budget. The sibling rules may
    /// still evaluate independently; if *every* configured rule
    /// cardinality-skipped, [`decide_all`] surfaces the operator-visible
    /// 400 just like the single-rule path.
    Cardinality,
    /// Predicate `except_if=` resolved truthy; rule is skipped for this
    /// request.
    Exempt,
}

/// Per-rule quota view captured at the same instant the admission
/// decision is made. Trivially `Copy` so [`decide_all`] can stash it
/// while folding sibling outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QuotaSnapshot {
    spec: RuleSpec,
    /// Budget remaining after this request. Saturating: a `DryRun` rule
    /// already past `spec.limit` reports `0`.
    remaining: u64,
}

/// Evaluate one request against every rule in `rule_indices`. Rules
/// stack: rejection from any enforcing rule rejects the request, with
/// `Retry-After` and the `X-RateLimit-*` triplet pinned to the rule with
/// the longest window (so the client doesn't see a short retry that
/// puts it right back into 429 against the wider rule).
///
/// `events` is the worker's admission scratch: reserved here, once, for the
/// most rules a request can be counted against, and never grown after.
pub fn decide_all(
    ctx: AccessCtx<'_>,
    rule_indices: &[usize],
    vars: &impl VariableLookup,
    now_millis: u64,
    events: &mut Vec<QueueEvent>,
) -> AccessOutcome {
    ctx.stats.record_request();
    let bound = ctx
        .max_matched_rules
        .get()
        .min(ctx.rules.table().rules().len());
    if events.capacity() < bound {
        *events = Vec::with_capacity(bound);
    }
    events.clear();

    let mut worst_reject: Option<RejectInfo> = None;
    let mut tightest_allow: Option<QuotaSnapshot> = None;
    let mut had_non_exempt_outcome = false;
    let mut overflowed = false;
    let mut any_exempt = false;
    let mut any_cardinality = false;

    for &index in rule_indices {
        match decide_one(ctx, index, vars, now_millis) {
            RuleOutcome::Allow(snapshot, event) => {
                had_non_exempt_outcome = true;
                tightest_allow = Some(pick_min_remaining(tightest_allow, snapshot));
                // Past the bound the rule is allowed but not counted: the
                // bound is gabion's own, so the request under-counts rather
                // than over-rejects.
                if events.len() < bound {
                    events.push(event);
                } else {
                    overflowed = true;
                }
            }
            RuleOutcome::Reject(info) => {
                had_non_exempt_outcome = true;
                worst_reject = Some(pick_longest_window(worst_reject, info));
            }
            RuleOutcome::Decline => {
                // Siblings still get to evaluate.
            }
            RuleOutcome::Cardinality => {
                any_cardinality = true;
                ctx.stats.record_cardinality_reject();
            }
            RuleOutcome::Exempt => {
                any_exempt = true;
                ctx.stats.record_exempt();
            }
        }
    }

    if let Some(info) = worst_reject {
        ctx.stats.record_reject();
        return AccessOutcome::Reject(info);
    }
    if had_non_exempt_outcome {
        // One shared write per counter per request, not one per event.
        let (mut pushed, mut dropped) = (0, 0);
        for &event in events.iter() {
            match ctx.queue.push(event) {
                Ok(()) => pushed += 1,
                Err(_) => dropped += 1,
            }
        }
        ctx.stats.record_queue_push(pushed);
        ctx.stats.record_queue_drop(dropped);
        if overflowed {
            ctx.stats.record_matched_rule_overflow();
        }
        ctx.stats.record_allow();
        return AccessOutcome::Allow(tightest_allow.map(|snap| AllowInfo {
            spec: snap.spec,
            remaining: snap.remaining,
            now_millis,
        }));
    }
    if any_exempt {
        // Exempt-only outcome: every rule's `except_if=` predicate fired,
        // so the request is allowed through without crediting either the
        // generic `allowed` counter or the gossip aggregate. No headers —
        // the operator's intent for `except_if=` is "pretend the rule
        // wasn't there for this request".
        return AccessOutcome::Allow(None);
    }
    if any_cardinality {
        return AccessOutcome::Cardinality;
    }
    AccessOutcome::Decline
}

/// Per-rule worker for [`decide_all`]. Returns the per-rule outcome without
/// committing to the queue — the orchestrator does the commit only if no
/// enforcing rule rejected the request.
fn decide_one(
    ctx: AccessCtx<'_>,
    rule_index: usize,
    vars: &impl VariableLookup,
    now_millis: u64,
) -> RuleOutcome {
    let Some((rule, compiled)) = ctx.rules.get(rule_index) else {
        return RuleOutcome::Decline;
    };
    if rule.mode() == EnforcementMode::Disabled {
        return RuleOutcome::Decline;
    }

    // Predicate gate. Evaluated before descriptors so a truthy predicate
    // exempts the request without billing the cardinality budget. A
    // predicate variable that resolves to None (variable absent at request
    // time) falls through as "rule applies" — same fail-open shape as a
    // missing descriptor variable.
    if let Some(predicate) = compiled.except_if.as_ref()
        && let Some(value) = vars.lookup(predicate)
        && is_truthy(value)
    {
        return RuleOutcome::Exempt;
    }

    // One pass over the bindings, each value hashed as it is read under the
    // rule's own domain. A variable miss declines; so does a value that is
    // not UTF-8 (counted, so the bypass is observable) or that differs from
    // the rule's `value=` literal. Count and key length passed the shared
    // cardinality check when the rule was built, so only the byte budget can
    // fail here.
    let mut hasher = KeyHasher::new(rule.domain());
    let mut bytes = rule.domain().len();
    for (lookup, pattern) in compiled.bindings.iter().zip(rule.descriptors()) {
        let Some(value) = vars.lookup(lookup) else {
            return RuleOutcome::Decline;
        };
        let Ok(value) = std::str::from_utf8(value) else {
            ctx.stats.record_decline_invalid_descriptor();
            return RuleOutcome::Decline;
        };
        if !pattern.accepts(value) {
            return RuleOutcome::Decline;
        }
        hasher.push(&pattern.key, value);
        bytes = bytes
            .saturating_add(pattern.key.len())
            .saturating_add(value.len());
    }
    if bytes > ctx.cardinality.max_descriptor_bytes() {
        return RuleOutcome::Cardinality;
    }

    let spec = rule.spec();
    let key_hash = hasher.finish();
    let total = ctx.aggregate.window_total(spec, key_hash, now_millis);
    // DryRun rules count and gossip but never reject — they let operators
    // observe what enforcement would do before flipping the switch.
    let hits = 1u64;
    if total.saturating_add(hits) > spec.limit && rule.mode() == EnforcementMode::Enforce {
        let delta_until_admit_millis = ctx
            .aggregate
            .time_until_admit_millis(spec, key_hash, now_millis, total, hits);
        return RuleOutcome::Reject(RejectInfo {
            spec,
            total,
            now_millis,
            delta_until_admit_millis,
        });
    }
    let event = QueueEvent {
        rule_fingerprint: spec.fingerprint,
        key_hash,
        bucket: spec.bucket_at(now_millis),
        hits,
        rule_limit: spec.limit,
        now_millis,
    };
    // Saturating remaining: under-limit Enforce/DryRun reports the real
    // budget; over-limit DryRun (which the gate above lets through)
    // reports 0 rather than underflowing.
    let remaining = spec.limit.saturating_sub(total).saturating_sub(hits);
    RuleOutcome::Allow(QuotaSnapshot { spec, remaining }, event)
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

/// Pick the rule the client is closest to tripping (smallest `remaining`)
/// among a candidate and the current best choice. On ties, prefer the
/// rule with the *longer* window — mirroring [`pick_longest_window`]'s
/// rationale: the client should see the most-constraining context, not
/// the one that resets soonest.
fn pick_min_remaining(current: Option<QuotaSnapshot>, candidate: QuotaSnapshot) -> QuotaSnapshot {
    match current {
        Some(c) if c.remaining < candidate.remaining => c,
        Some(c)
            if c.remaining == candidate.remaining
                && c.spec.window_millis >= candidate.spec.window_millis =>
        {
            c
        }
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
    !value.is_empty()
        && ![&b"0"[..], b"false", b"off", b"no"]
            .iter()
            .any(|falsy| value.eq_ignore_ascii_case(falsy))
}

#[cfg(test)]
mod mock;
#[cfg(test)]
pub(crate) use mock::MockVars;
#[cfg(test)]
mod tests;
