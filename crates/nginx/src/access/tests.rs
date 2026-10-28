use super::*;
use crate::rules::mock::{compile, directive, rules};
use crate::rules::{CompiledRules, DEFAULT_DOMAIN};
use crate::shm::mock::{TestZone, seed};
use crate::shm::{ShmAggregateStore, ShmRegion};
use gabion::defaults;
use gabion::rules::{Descriptor, KeyHash, hash_key};
use gabion::settings::Settings;

/// `per_tenant`: 2 hits a second on `tenant`, in 250 ms buckets.
const PER_TENANT: &str = "per_tenant tenant:$http_x_tenant rate=2r/s bucket=250ms";

/// The access context over `region` for `rules`, with the test settings
/// then `tune`.
fn ctx<'a>(
    rules: &'a CompiledRules,
    region: &'a ShmRegion,
    tune: impl FnOnce(&mut Settings),
) -> AccessCtx<'a> {
    let (_, storage) = crate::mock::validated(tune);
    AccessCtx {
        rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        cardinality: storage.cardinality,
        max_matched_rules: storage.max_matched_rules(),
    }
}

/// [`decide_all`] with a fresh scratch.
fn decide(
    ctx: AccessCtx<'_>,
    indices: &[usize],
    vars: &MockVars,
    now_millis: u64,
) -> AccessOutcome {
    decide_all(ctx, indices, vars, now_millis, &mut Vec::new())
}

/// How `key=value` hashes in the default domain.
fn key_hash(key: &str, value: &str) -> KeyHash {
    hash_key(DEFAULT_DOMAIN, [Descriptor { key, value }])
}

fn queued(region: &ShmRegion) -> usize {
    region.queue().drain(&mut [QueueEvent::default(); 64])
}

#[test]
fn allow_on_empty_aggregate() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rules(&[PER_TENANT]);
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0], &vars, 1_000);
    // Single matched rule, aggregate empty: limit=2, hits=1 -> remaining=1.
    match outcome {
        AccessOutcome::Allow(Some(info)) => {
            assert_eq!(info.spec.limit, 2);
            assert_eq!(info.remaining, 1);
            assert_eq!(info.now_millis, 1_000);
        }
        other => panic!("expected Allow(Some), got {other:?}"),
    }
    // The queued hit lands in the 250 ms bucket holding now=1000ms: bucket 4.
    let mut events = [QueueEvent::default(); 2];
    assert_eq!(region.queue().drain(&mut events), 1);
    assert_eq!(
        events[0],
        QueueEvent {
            rule_fingerprint: rules.table().rules()[0].spec().fingerprint,
            key_hash: key_hash("tenant", "alice").0,
            bucket: 4,
            hits: 1,
            rule_limit: 2,
            now_millis: 1_000,
        }
    );
}

#[test]
fn reject_when_window_total_exceeds_limit() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rules(&[PER_TENANT]);
    let spec = rules.table().rules()[0].spec();
    let bucket = (1_000_u64 / spec.bucket_millis) as u32;
    // Pre-populate aggregate so total >= limit (limit is 2).
    let store = ShmAggregateStore::new(region);
    seed(
        &store,
        spec.fingerprint,
        key_hash("tenant", "alice"),
        bucket,
        5,
    );

    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0], &vars, 1_000);
    match outcome {
        AccessOutcome::Reject(info) => {
            assert_eq!(info.spec.id, spec.id);
            assert_eq!(info.total, 5);
            // Rule: limit=2, window=1s, bucket=250ms (live=4). All 5 hits
            // sit in bucket 4 (now=1000ms). need = 5+1-2 = 4. Walking
            // buckets 1..4 oldest-first: 0,0,0,5 — bucket 4 alone covers
            // need. Its fall-off is (4+4)*250 = 2000ms, so delta from
            // now=1000 is 1000ms.
            assert_eq!(info.delta_until_admit_millis, 1_000);
        }
        other => panic!("expected Reject, got {other:?}"),
    }
    assert_eq!(queued(&region), 0);
}

#[test]
fn reject_delta_reflects_oldest_non_empty_bucket() {
    // Build a rule with limit=2, window=1s, bucket=250ms (live=4). At
    // now=1_000ms the live window is buckets [1..4]. Spread the 5
    // existing hits across buckets so the *oldest non-empty* bucket
    // governs the delta: bucket 2 has 5 hits, the rest are empty. need
    // = 5+1-2 = 4, satisfied at bucket 2; fall_off = (2+4)*250 = 1500ms;
    // delta from now=1_000 is 500ms.
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rules(&[PER_TENANT]);
    let spec = rules.table().rules()[0].spec();
    let store = ShmAggregateStore::new(region);
    seed(&store, spec.fingerprint, key_hash("tenant", "alice"), 2, 5);

    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0], &vars, 1_000);
    match outcome {
        AccessOutcome::Reject(info) => {
            assert_eq!(info.total, 5);
            assert_eq!(info.delta_until_admit_millis, 500);
        }
        other => panic!("expected Reject, got {other:?}"),
    }
}

#[test]
fn decline_when_variable_missing() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rules(&[PER_TENANT]);
    let vars = MockVars::new(); // no http_x_tenant set
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0], &vars, 1_000);
    assert!(matches!(outcome, AccessOutcome::Decline));
}

/// NGINX rejects exactly the requests the library's one cardinality check
/// flags, on both sides of the byte budget.
#[test]
fn one_cardinality_check_for_both_adapters() {
    const BUDGET: usize = 32;
    let budget = |s: &mut Settings| s.max_descriptor_bytes = BUDGET.try_into().unwrap();
    let limits = crate::mock::validated(budget).1.cardinality;
    let rules = rules(&[PER_TENANT]);
    let flagged: Vec<bool> = [BUDGET - 1, BUDGET, BUDGET + 1]
        .into_iter()
        .map(|bytes| {
            let (_buf, region) = TestZone::new(8, 16);
            let value = "v".repeat(bytes - DEFAULT_DOMAIN.len() - "tenant".len());
            let entry = Descriptor {
                key: "tenant",
                value: &value,
            };
            let flagged = limits
                .exceeded(DEFAULT_DOMAIN, [entry].into_iter())
                .is_some();
            let vars = MockVars::new().set("http_x_tenant", &value);
            let outcome = decide(ctx(&rules, &region, budget), &[0], &vars, 1_000);
            assert_eq!(
                outcome == AccessOutcome::Cardinality,
                flagged,
                "{bytes} bytes"
            );
            flagged
        })
        .collect();
    assert_eq!(flagged, [false, false, true]);
}

#[test]
fn invalid_utf8_descriptor_declines_and_bumps_counter() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rules(&[PER_TENANT]);
    // Over budget too, so a budget check moved before UTF-8 rejects.
    let ctx = ctx(&rules, &region, |s| {
        s.max_descriptor_bytes = DEFAULT_DOMAIN.len().try_into().unwrap();
    });
    // `0xFF` is never a valid UTF-8 lead byte — guaranteed decline.
    let vars = MockVars::new().set_bytes("http_x_tenant", &[0xFFu8, 0xFE, 0xFD]);
    let outcome = decide(ctx, &[0], &vars, 1_000);
    assert!(matches!(outcome, AccessOutcome::Decline));
    let snap = region.stats().snapshot();
    assert_eq!(snap.declines_invalid_descriptor, 1);
    // Allow-by-default: a UTF-8 decline does NOT increment the
    // user-facing reject counter.
    assert_eq!(snap.rejected, 0);
    assert_eq!(snap.rejected_cardinality, 0);
}

/// A `mode=disabled` rule declines before its predicate, UTF-8 and byte
/// budget checks, so none of them answers or moves a counter.
#[test]
fn disabled_rule_declines_before_any_check() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rules(&[&format!("{PER_TENANT} except_if=$trusted mode=disabled")]);
    let ctx = ctx(&rules, &region, |_| {});
    let over_budget = "a".repeat(defaults::STORAGE_MAX_DESCRIPTOR_BYTES);
    // In the order the checks run last to first, so a disabled check moved
    // past any one of them fails on that check's own case.
    for vars in [
        MockVars::new().set("http_x_tenant", &over_budget),
        MockVars::new().set_bytes("http_x_tenant", &[0xFF]),
        MockVars::new().set("trusted", "1"),
    ] {
        let outcome = decide(ctx, &[0], &vars, 1_000);
        assert!(matches!(outcome, AccessOutcome::Decline), "{outcome:?}");
        let snap = region.stats().snapshot();
        assert_eq!(snap.exempted, 0);
        assert_eq!(snap.rejected_cardinality, 0);
        assert_eq!(snap.declines_invalid_descriptor, 0);
    }
}

#[test]
fn is_truthy_matches_documented_falsy_set() {
    for falsy in [
        b"".as_ref(),
        b"0",
        b"false",
        b"False",
        b"FALSE",
        b"off",
        b"Off",
        b"OFF",
        b"no",
        b"No",
        b"NO",
        b"fAlSe",
        b"oFf",
        b"nO",
    ] {
        assert!(!is_truthy(falsy), "{:?} should be falsy", falsy);
    }
    for truthy in [b"1".as_ref(), b"true", b"yes", b"on", b"anything", b" "] {
        assert!(is_truthy(truthy), "{:?} should be truthy", truthy);
    }
}

fn rule_with_predicate() -> CompiledRules {
    rules(&[&format!("{PER_TENANT} except_if=$trusted")])
}

#[test]
fn except_truthy_skips_rule() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rule_with_predicate();
    let vars = MockVars::new()
        .set("http_x_tenant", "alice")
        .set("trusted", "1");
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0], &vars, 0);
    // Exempt-only outcome: no rule produced a quota view, so no
    // `X-RateLimit-*` headers should be emitted on the response.
    assert!(matches!(outcome, AccessOutcome::Allow(None)));
    let snap = region.stats().snapshot();
    assert_eq!(snap.exempted, 1, "exempt counter should fire");
    assert_eq!(snap.allowed, 0, "exempt is not a normal allow");
    // No queue event from an exempted request — under-count rather
    // than over-count when the predicate fires.
    assert_eq!(queued(&region), 0);
}

/// A falsy, missing or empty predicate leaves the rule applying: the
/// operator's intent for `except_if=` is opt-in exemption, not opt-in
/// enforcement.
#[test]
fn except_falsy_missing_or_empty_applies_rule() {
    let rules = rule_with_predicate();
    for trusted in [Some("off"), None, Some("")] {
        let (_buf, region) = TestZone::new(8, 16);
        let mut vars = MockVars::new().set("http_x_tenant", "alice");
        if let Some(value) = trusted {
            vars = vars.set("trusted", value);
        }
        let outcome = decide(ctx(&rules, &region, |_| {}), &[0], &vars, 0);
        assert!(matches!(outcome, AccessOutcome::Allow(_)), "{trusted:?}");
        let snap = region.stats().snapshot();
        assert_eq!((snap.exempted, snap.allowed), (0, 1), "{trusted:?}");
    }
}

/// Two rules on `tenant`, `first_limit` hits over `first_window_secs` and
/// `second_limit` over `second_window_secs`, so a single request matches
/// both — the stacked-rule shape `decide_all` exists for.
fn build_two_rules(
    first_limit: u64,
    first_window_secs: u64,
    second_limit: u64,
    second_window_secs: u64,
) -> CompiledRules {
    rules(&[
        &format!(
            "first tenant:$http_x_tenant rate={first_limit}r/{first_window_secs}s bucket=250ms"
        ),
        &format!(
            "second tenant:$http_x_tenant rate={second_limit}r/{second_window_secs}s bucket=250ms"
        ),
    ])
}

#[test]
fn decide_all_allow_when_all_pass() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = build_two_rules(10, 1, 10, 60);
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0, 1], &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Allow(_)));
    // Both rules push a QueueEvent because both matched.
    assert_eq!(queued(&region), 2);
    let snap = region.stats().snapshot();
    assert_eq!(snap.allowed, 1);
    assert_eq!(snap.rejected, 0);
}

#[test]
fn decide_all_decline_when_all_decline() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = build_two_rules(10, 1, 10, 60);
    // No variable set — every rule declines.
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0, 1], &MockVars::new(), 0);
    assert!(matches!(outcome, AccessOutcome::Decline));
    let snap = region.stats().snapshot();
    assert_eq!(snap.allowed, 0);
    assert_eq!(snap.rejected, 0);
}

#[test]
fn decide_all_picks_longest_window() {
    let (_buf, region) = TestZone::new(8, 16);
    // First rule: 1s window, limit 1; second: 60s window, limit 1.
    let rules = build_two_rules(1, 1, 1, 60);
    // Pre-populate aggregate so BOTH rules are over their budget.
    let store = ShmAggregateStore::new(region);
    for rule in rules.table().rules() {
        seed(
            &store,
            rule.fingerprint(),
            key_hash("tenant", "alice"),
            0,
            5,
        );
    }
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0, 1], &vars, 0);
    match outcome {
        AccessOutcome::Reject(info) => {
            assert_eq!(
                info.spec.window_millis, 60_000,
                "should pick the 60s rule for `Retry-After`"
            );
        }
        other => panic!("expected Reject, got {other:?}"),
    }
}

#[test]
fn decide_all_one_declines_one_allows() {
    let (_buf, region) = TestZone::new(8, 16);
    // First rule keyed on a missing variable so it declines; second
    // rule has the variable set so it allows.
    let rules = rules(&[
        "missing nonexistent:$does_not_exist rate=10r/s bucket=250ms",
        "tenant tenant:$http_x_tenant rate=10r/s bucket=250ms",
    ]);
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0, 1], &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Allow(_)));
    // Only one rule (the second) produced an event.
    assert_eq!(queued(&region), 1);
}

#[test]
fn dry_run_records_but_never_rejects() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rules(&["shadow tenant:$http_x_tenant rate=1r/s bucket=250ms dry_run"]);
    // Pre-populate so the aggregate is way over the limit.
    let store = ShmAggregateStore::new(region);
    let spec = rules.table().rules()[0].spec();
    seed(&store, spec.fingerprint, key_hash("tenant", "alice"), 0, 99);

    let vars = MockVars::new().set("http_x_tenant", "alice");
    // DryRun: over the limit but never rejects.
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0], &vars, 0);
    match outcome {
        // Over-limit DryRun: saturating subtraction collapses remaining
        // to 0 so the response still gets `Remaining: 0` headers,
        // letting operators graph "share that would have been 429'd".
        AccessOutcome::Allow(Some(info)) => {
            assert_eq!(info.spec.limit, 1);
            assert_eq!(
                info.remaining, 0,
                "DryRun over its budget reports remaining=0"
            );
        }
        other => panic!("expected Allow(Some), got {other:?}"),
    }
    // Still records the hit for the gossip aggregate.
    assert_eq!(queued(&region), 1);
    let snap = region.stats().snapshot();
    assert_eq!(snap.rejected, 0);
    assert_eq!(snap.allowed, 1);
}

/// Build two rules with different descriptor keys so the per-request
/// cardinality budget can trip one without affecting the other.
fn build_two_rules_varied(key_a: &str, key_b: &str) -> CompiledRules {
    rules(&[
        &format!("a {key_a}:$http_x_tenant rate=10r/s bucket=250ms"),
        &format!("b {key_b}:$http_x_tenant rate=10r/s bucket=250ms"),
    ])
}

#[test]
fn decide_all_one_rule_cardinality_others_still_evaluate() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = build_two_rules_varied("long_descriptor_key_name", "k");
    let budget = DEFAULT_DOMAIN.len() + 1 + "alice".len();
    let ctx = ctx(&rules, &region, |s| {
        s.max_descriptor_bytes = budget.try_into().unwrap();
    });
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx, &[0, 1], &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Allow(_)));
    let snap = region.stats().snapshot();
    assert_eq!(snap.allowed, 1);
    assert_eq!(snap.rejected_cardinality, 1, "one rule tripped cardinality");
    assert_eq!(queued(&region), 1);
}

#[test]
fn decide_all_all_rules_cardinality_returns_cardinality() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = build_two_rules_varied("long_descriptor_key_name_a", "long_descriptor_key_name_b");
    let ctx = ctx(&rules, &region, |s| {
        s.max_descriptor_bytes = (DEFAULT_DOMAIN.len() + 1).try_into().unwrap();
    });
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx, &[0, 1], &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Cardinality));
    let snap = region.stats().snapshot();
    assert_eq!(snap.rejected_cardinality, 2);
    assert_eq!(snap.allowed, 0);
}

#[test]
fn decide_all_mixed_decline_and_cardinality_returns_cardinality() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rules(&[
        "a k:$does_not_exist rate=10r/s bucket=250ms",
        "b long_descriptor_key:$http_x_tenant rate=10r/s bucket=250ms",
    ]);
    let ctx = ctx(&rules, &region, |s| {
        s.max_descriptor_bytes = (DEFAULT_DOMAIN.len() + 1).try_into().unwrap();
    });
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx, &[0, 1], &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Cardinality));
}

#[test]
fn decide_all_one_rule_exempt_one_rejects() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rules(&[
        "a tenant:$http_x_tenant rate=10r/s bucket=250ms except_if=$trusted",
        "b k:$http_x_tenant rate=1r/s bucket=250ms",
    ]);
    let store = ShmAggregateStore::new(region);
    let spec_b = rules.table().rules()[1].spec();
    seed(&store, spec_b.fingerprint, key_hash("k", "alice"), 0, 5);

    let vars = MockVars::new()
        .set("http_x_tenant", "alice")
        .set("trusted", "1");
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0, 1], &vars, 0);
    match outcome {
        AccessOutcome::Reject(info) => assert_eq!(info.spec.id, spec_b.id),
        other => panic!("expected Reject, got {other:?}"),
    }
    let snap = region.stats().snapshot();
    assert_eq!(snap.exempted, 1, "rule a still recorded its exemption");
    assert_eq!(snap.rejected, 1);
}

#[test]
fn cardinality_skip_does_not_push_queue_event() {
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rules(&[PER_TENANT]);
    let ctx = ctx(&rules, &region, |s| {
        s.max_descriptor_bytes = (DEFAULT_DOMAIN.len() + 1).try_into().unwrap();
    });
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx, &[0], &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Cardinality));
    assert_eq!(queued(&region), 0);
}

#[test]
fn except_does_not_count_cardinality() {
    let (_buf, region) = TestZone::new(8, 16);
    // Predicate truthy — request is exempted before the byte-budget check.
    let rules = rule_with_predicate();
    let ctx = ctx(&rules, &region, |s| {
        s.max_descriptor_bytes = 1.try_into().unwrap();
    });
    let vars = MockVars::new()
        .set("http_x_tenant", "alice")
        .set("trusted", "1");
    let outcome = decide(ctx, &[0], &vars, 0);
    // Exempt-only outcome — no rule produced a quota view.
    assert!(matches!(outcome, AccessOutcome::Allow(None)));
    let snap = region.stats().snapshot();
    assert_eq!(
        snap.rejected_cardinality, 0,
        "exempt path skips budget check"
    );
    assert_eq!(snap.exempted, 1);
}

#[test]
fn unknown_predicate_variable_fails_at_compile() {
    use crate::rules::RuleConfigError;
    // The NopBindingCompiler rejects anything that isn't a single
    // `$identifier` or one of the inline fast-path arms. A hyphen in the
    // identifier trips the legal-ident check, so the source `$bad-ident`
    // forwarded through `except_if=` fails compilation.
    let mut config = directive(PER_TENANT);
    config.except_if = Some("bad-ident".into());
    let err = compile(&[config]).expect_err("predicate compile rejected");
    assert!(
        matches!(err, RuleConfigError::CompileBinding { .. }),
        "expected CompileBinding, got {err:?}"
    );
}

#[test]
fn allow_info_picks_min_remaining_across_rules() {
    // Two rules. Rule 0 has the lower budget left — its quota should
    // drive the `X-RateLimit-*` headers on the allowed response so the
    // client sees the most-constraining rule.
    let (_buf, region) = TestZone::new(8, 16);
    // Rule 0: limit=2, window=1s. Rule 1: limit=10, window=60s.
    let rules = build_two_rules(2, 1, 10, 60);
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0, 1], &vars, 0);
    match outcome {
        AccessOutcome::Allow(Some(info)) => {
            // Rule 0 reports the smaller remaining (limit=2, hits=1) -> 1.
            assert_eq!(info.spec.limit, 2);
            assert_eq!(info.remaining, 1);
            assert_eq!(info.spec.window_millis, 1_000);
        }
        other => panic!("expected Allow(Some), got {other:?}"),
    }
}

#[test]
fn allow_info_tie_break_prefers_longer_window() {
    // Both rules have the same remaining (limit=5, hits=1 -> 4). The
    // tie-break picks the longer window, mirroring the reject path's
    // most-constraining-context rationale.
    let (_buf, region) = TestZone::new(8, 16);
    let rules = build_two_rules(5, 1, 5, 60);
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0, 1], &vars, 0);
    match outcome {
        AccessOutcome::Allow(Some(info)) => {
            assert_eq!(info.remaining, 4);
            assert_eq!(
                info.spec.window_millis, 60_000,
                "ties go to the longer window"
            );
        }
        other => panic!("expected Allow(Some), got {other:?}"),
    }
}

#[test]
fn allow_info_under_limit_dry_run_emits_positive_remaining() {
    // DryRun rule under its limit should produce an AllowInfo with the
    // real remaining budget — same as Enforce. Mirrors the
    // over-limit case, which is exercised by
    // `dry_run_records_but_never_rejects`.
    let (_buf, region) = TestZone::new(8, 16);
    let rules = rules(&["shadow tenant:$http_x_tenant rate=5r/s bucket=250ms dry_run"]);
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0], &vars, 0);
    match outcome {
        AccessOutcome::Allow(Some(info)) => {
            assert_eq!(info.spec.limit, 5);
            assert_eq!(info.remaining, 4, "limit=5, hits=1 -> remaining=4");
        }
        other => panic!("expected Allow(Some), got {other:?}"),
    }
}

/// `value=tenant:acme` makes the rule count only `tenant=acme`; any other
/// tenant declines without touching a counter, and a `value=` naming no
/// binding fails `nginx -t`.
#[test]
fn nginx_value_argument_matches_only_its_literal() {
    use crate::rules::{RuleArgError, parse_rule_directive};
    let config = directive("acme tenant:$arg_t rate=10r/s value=tenant:acme");
    assert_eq!(&*config.bindings[0].value, "acme");
    let rules = compile(&[config]).expect("compile");
    let (_buf, region) = TestZone::new(8, 16);
    let ctx = ctx(&rules, &region, |_| {});
    let request = |tenant: &str| {
        let vars = MockVars {
            args: format!("t={tenant}").into_bytes(),
            ..MockVars::new()
        };
        decide(ctx, &[0], &vars, 0)
    };

    assert!(matches!(request("acme"), AccessOutcome::Allow(Some(_))));
    assert_eq!(queued(&region), 1);
    let before = region.stats().snapshot();
    assert_eq!(request("other"), AccessOutcome::Decline);
    assert_eq!(queued(&region), 0);
    let after = region.stats().snapshot();
    assert_eq!(
        (after.allowed, after.rejected),
        (before.allowed, before.rejected)
    );

    assert_eq!(
        parse_rule_directive(&["acme", "tenant:$arg_t", "rate=10r/s", "value=missing:acme"]),
        Err(RuleArgError::UnknownValueKey {
            rule: "acme".into(),
            key: "missing".into(),
        })
    );
}

/// A rule's `domain=` is the domain its keys hash in, as `gabiond` hashes
/// the same request.
#[test]
fn nginx_rule_domain_hashes_its_keys() {
    let rules = rules(&["api tenant:$arg_t rate=10r/s domain=api"]);
    let (_buf, region) = TestZone::new(8, 16);
    let vars = MockVars {
        args: b"t=x".to_vec(),
        ..MockVars::new()
    };
    let outcome = decide(ctx(&rules, &region, |_| {}), &[0], &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Allow(Some(_))));
    let mut events = [QueueEvent::default(); 1];
    assert_eq!(region.queue().drain(&mut events), 1);
    let expected = hash_key(
        "api",
        [Descriptor {
            key: "tenant",
            value: "x",
        }],
    );
    assert_eq!(events[0].key_hash, expected.0);
}

/// `storage.max_matched_rules` is how many of a request's allowed rules
/// count it: at 20, a location with 20 matching rules queues 20 hits and
/// overflows nothing, and one with 22 queues 20 and counts one overflowing
/// request, not one per uncounted rule.
#[test]
fn matched_rule_bound_is_the_setting() {
    for (matched, expected_queued, overflows) in [(20, 20, 0), (22, 20, 1)] {
        let directives: Vec<String> = (1..=matched)
            .map(|i| format!("r{i} tenant:$http_x_tenant rate=100r/{i}s"))
            .collect();
        let directives: Vec<&str> = directives.iter().map(String::as_str).collect();
        let rules = rules(&directives);
        let (_buf, region) = TestZone::new(32, 64);
        let ctx = ctx(&rules, &region, |s| {
            s.max_matched_rules = 20.try_into().unwrap();
        });
        let vars = MockVars::new().set("http_x_tenant", "alice");
        let indices: Vec<usize> = (0..matched).collect();
        let outcome = decide(ctx, &indices, &vars, 0);
        assert!(matches!(outcome, AccessOutcome::Allow(Some(_))));
        assert_eq!(
            (
                queued(&region),
                region.stats().snapshot().matched_rule_overflows
            ),
            (expected_queued, overflows),
            "{matched} rules"
        );
    }
}
