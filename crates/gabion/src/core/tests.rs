
use super::*;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;

#[derive(Clone, Debug)]
struct WindowTotalsCase {
    ops: Vec<WindowOp>,
}

#[derive(Clone, Debug)]
struct WindowOp {
    tenant: u8,
    now_millis: u16,
}

impl Arbitrary for WindowTotalsCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut ops = Vec::<WindowOp>::arbitrary(g);
        ops.truncate(256);
        Self { ops }
    }
}

impl Arbitrary for WindowOp {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            tenant: u8::arbitrary(g),
            now_millis: u16::arbitrary(g) % 3_500,
        }
    }
}

#[derive(Clone, Debug)]
struct LimitCase {
    fallback_limit: u8,
    attempts: u8,
}

impl Arbitrary for LimitCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            fallback_limit: u8::arbitrary(g) % 8,
            attempts: (u8::arbitrary(g) % 32).max(1),
        }
    }
}

#[derive(Clone, Debug)]
struct DescriptorMatchCase {
    request_value: u8,
    exact_value: u8,
    include_wildcard: bool,
    disabled_exact: bool,
    wrong_domain: bool,
}

impl Arbitrary for DescriptorMatchCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            request_value: u8::arbitrary(g) % 8,
            exact_value: u8::arbitrary(g) % 8,
            include_wildcard: bool::arbitrary(g),
            disabled_exact: bool::arbitrary(g),
            wrong_domain: bool::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct OverflowPolicyCase {
    policy: OverflowPolicy,
    attempts: u8,
}

impl Arbitrary for OverflowPolicyCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let policy = match u8::arbitrary(g) % 4 {
            0 => OverflowPolicy::UseOverflowKey,
            1 => OverflowPolicy::AllowUntracked,
            2 => OverflowPolicy::Reject,
            _ => OverflowPolicy::Sample,
        };
        Self {
            policy,
            attempts: (u8::arbitrary(g) % 32).max(2),
        }
    }
}

fn rule(id: RuleId) -> Rule {
    Rule {
        id,
        domain_hash: hash_domain("api"),
        descriptor_matcher: DescriptorMatcher::exact_keys(["tenant"]),
        limit: 10,
        window: WindowSpec {
            size_millis: 1_000,
            bucket_count: 10,
        },
        local_fallback_limit: 3,
        local_absolute_limit: 6,
        stale_after_millis: 500,
        safety_margin: SafetyMargin { hits: 0 },
        overflow_policy: OverflowPolicy::UseOverflowKey,
        mode: EnforcementMode::Enforce,
    }
}

fn exact_value_rule(id: RuleId, value: &str) -> Rule {
    Rule {
        descriptor_matcher: DescriptorMatcher::exact([("tenant", value)]),
        ..rule(id)
    }
}

fn check(engine: &mut LocalEngine, value: &str, now_millis: u64) -> Decision {
    let descriptors = [Descriptor {
        key: "tenant",
        value,
    }];
    engine.check_and_record(
        LimitRequest {
            domain: "api",
            descriptors: &descriptors,
            hits: 1,
        },
        now_millis,
    )
}

fn request_with_descriptors<'a>(descriptors: &'a [Descriptor<'a>]) -> LimitRequest<'a> {
    LimitRequest {
        domain: "api",
        descriptors,
        hits: 1,
    }
}

#[test]
fn allows_up_to_local_fallback_when_gossip_is_stale() {
    let rules = RuleTable::new(vec![rule(1)]);
    let mut engine = LocalEngine::new(rules, 8, 10);

    assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
    assert_eq!(check(&mut engine, "a", 1), Decision::Allow);
    assert_eq!(check(&mut engine, "a", 2), Decision::Allow);
    assert_eq!(
        check(&mut engine, "a", 3),
        Decision::Reject(RejectReason::LocalFallbackLimit)
    );

    let metrics = engine.metrics();
    assert_eq!(metrics.allowed, 3);
    assert_eq!(metrics.local_fallback_rejected, 1);
}

#[test]
fn uses_global_limit_when_estimate_is_fresh() {
    let rules = RuleTable::new(vec![rule(1)]);
    let mut engine = LocalEngine::new(rules, 8, 10);
    engine.mark_global_estimate_updated(0);

    for i in 0..6 {
        assert_eq!(check(&mut engine, "a", i), Decision::Allow);
    }

    assert_eq!(
        check(&mut engine, "a", 6),
        Decision::Reject(RejectReason::LocalAbsoluteLimit)
    );
}

#[test]
fn expires_sliding_window_buckets() {
    let rules = RuleTable::new(vec![rule(1)]);
    let mut engine = LocalEngine::new(rules, 8, 10);

    assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
    assert_eq!(check(&mut engine, "a", 100), Decision::Allow);
    assert_eq!(check(&mut engine, "a", 200), Decision::Allow);
    assert_eq!(
        check(&mut engine, "a", 300),
        Decision::Reject(RejectReason::LocalFallbackLimit)
    );
    assert_eq!(check(&mut engine, "a", 1_201), Decision::Allow);
}

#[test]
fn overflow_key_keeps_serving_when_capacity_is_exhausted() {
    let rules = RuleTable::new(vec![rule(1)]);
    let mut engine = LocalEngine::new(rules, 1, 10);

    assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
    assert_eq!(check(&mut engine, "b", 0), Decision::Allow);
    assert_eq!(engine.active_keys(), 1);
    assert_eq!(engine.metrics().overflow_key_uses, 1);
}

#[test]
fn reject_overflow_policy_rejects_new_keys_when_capacity_is_exhausted() {
    let mut reject_rule = rule(1);
    reject_rule.overflow_policy = OverflowPolicy::Reject;
    let rules = RuleTable::new(vec![reject_rule]);
    let mut engine = LocalEngine::new(rules, 1, 10);

    assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
    assert_eq!(
        check(&mut engine, "b", 0),
        Decision::Reject(RejectReason::LocalFallbackLimit)
    );
    assert_eq!(engine.metrics().overflow_rejected, 1);
}

#[test]
fn unmatched_requests_are_allowed_without_allocating_keys() {
    let rules = RuleTable::new(vec![rule(1)]);
    let mut engine = LocalEngine::new(rules, 8, 10);
    let request = LimitRequest {
        domain: "other",
        descriptors: &[Descriptor {
            key: "tenant",
            value: "a",
        }],
        hits: 1,
    };

    assert_eq!(engine.check_and_record(request, 0), Decision::Allow);
    assert_eq!(engine.active_keys(), 0);
}

#[test]
fn exact_descriptor_key_matching_uses_request_slice_without_allocation() {
    let rules = RuleTable::new(vec![rule(1)]);
    let descriptors = [Descriptor {
        key: "tenant",
        value: "a",
    }];
    let request = request_with_descriptors(&descriptors);

    let matched: Vec<_> = rules.matching(&request).map(|rule| rule.id).collect();

    assert_eq!(matched, vec![1]);
}

#[test]
fn descriptor_matching_honors_exact_values_and_wildcards() {
    let rules = RuleTable::new(vec![exact_value_rule(1, "paid"), exact_value_rule(2, "*")]);
    let paid = [Descriptor {
        key: "tenant",
        value: "paid",
    }];
    let free = [Descriptor {
        key: "tenant",
        value: "free",
    }];

    let paid_matches: Vec<_> = rules
        .matching(&request_with_descriptors(&paid))
        .map(|rule| rule.id)
        .collect();
    let free_matches: Vec<_> = rules
        .matching(&request_with_descriptors(&free))
        .map(|rule| rule.id)
        .collect();

    assert_eq!(paid_matches, vec![1, 2]);
    assert_eq!(free_matches, vec![2]);
}

#[test]
fn cardinality_limits_reject_large_descriptor_sets() {
    let descriptors = [
        Descriptor {
            key: "tenant",
            value: "a",
        },
        Descriptor {
            key: "route",
            value: "/v1",
        },
    ];
    let request = LimitRequest {
        domain: "api",
        descriptors: &descriptors,
        hits: 1,
    };

    assert_eq!(
        request.validate_cardinality(CardinalityLimits {
            max_descriptor_count: 1,
            max_descriptor_bytes: 512,
            max_key_bytes: 128,
        }),
        Err(CardinalityError::DescriptorCount)
    );
    assert_eq!(
        request.validate_cardinality(CardinalityLimits {
            max_descriptor_count: 2,
            max_descriptor_bytes: 4,
            max_key_bytes: 128,
        }),
        Err(CardinalityError::DescriptorBytes)
    );
}

#[test]
fn remote_estimate_participates_in_fresh_global_decision() {
    let rules = RuleTable::new(vec![rule(1)]);
    let mut engine = LocalEngine::new(rules, 8, 10);
    let descriptors = [Descriptor {
        key: "tenant",
        value: "a",
    }];
    let request = LimitRequest {
        domain: "api",
        descriptors: &descriptors,
        hits: 1,
    };
    let key_hash = hash_key(1, &request);

    assert!(engine.add_remote_estimate(1, key_hash, 0, 0, 10));

    assert_eq!(
        engine.check_and_record(request, 1),
        Decision::Reject(RejectReason::GlobalLimit)
    );
}

#[test]
fn freshness_is_rule_scoped() {
    let rule_a = rule(1);
    let mut rule_b = rule(2);
    rule_b.descriptor_matcher = DescriptorMatcher::exact([("route", "*")]);
    let rules = RuleTable::new(vec![rule_a, rule_b]);
    let mut engine = LocalEngine::new(rules, 8, 10);
    let descriptors_a = [Descriptor {
        key: "tenant",
        value: "a",
    }];
    let request_a = LimitRequest {
        domain: "api",
        descriptors: &descriptors_a,
        hits: 1,
    };
    let descriptors_b = [Descriptor {
        key: "route",
        value: "/v1",
    }];
    let request_b = LimitRequest {
        domain: "api",
        descriptors: &descriptors_b,
        hits: 1,
    };

    assert!(engine.add_remote_estimate(1, hash_key(1, &request_a), 0, 0, 10));

    assert_eq!(
        engine.check_and_record(request_a, 1),
        Decision::Reject(RejectReason::GlobalLimit)
    );
    assert_eq!(engine.check_and_record(request_b, 1), Decision::Allow);
    assert_eq!(engine.check_and_record(request_b, 2), Decision::Allow);
    assert_eq!(engine.check_and_record(request_b, 3), Decision::Allow);
    assert_eq!(
        engine.check_and_record(request_b, 4),
        Decision::Reject(RejectReason::LocalFallbackLimit)
    );
}

#[test]
fn successful_local_increments_create_dirty_cells() {
    let rules = RuleTable::new(vec![rule(1)]);
    let identity = NodeIdentity {
        node_id: NodeId::from((7_u128 << 64) | 9),
        incarnation: 11,
    };
    let mut engine = LocalEngine::with_identity(rules, 8, 10, 8, 8, identity);

    assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
    assert_eq!(check(&mut engine, "a", 1), Decision::Allow);

    let cells: Vec<_> = engine.cells().collect();
    let dirty: Vec<_> = engine.dirty_cells().collect();

    assert_eq!(engine.active_cells(), 1);
    assert_eq!(cells.len(), 1);
    assert_eq!(cells[0].count, 2);
    assert_eq!(cells[0].origin_node_id, identity.node_id);
    assert_eq!(cells[0].origin_incarnation, identity.incarnation);
    assert_eq!(dirty.len(), 2);
    assert!(!engine.dirty_overflowed());
}

#[test]
fn local_cell_table_reports_dirty_overflow_without_unbounded_growth() {
    let rules = RuleTable::new(vec![rule(1)]);
    let identity = NodeIdentity::default();
    let mut engine = LocalEngine::with_identity(rules, 8, 10, 1, 1, identity);

    assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
    assert_eq!(check(&mut engine, "b", 0), Decision::Allow);

    assert_eq!(engine.active_cells(), 1);
    assert!(engine.dirty_overflowed());
}

#[test]
fn allow_untracked_overflow_policy_allows_without_recording_new_key() {
    let mut allow_rule = rule(1);
    allow_rule.overflow_policy = OverflowPolicy::AllowUntracked;
    let rules = RuleTable::new(vec![allow_rule]);
    let mut engine = LocalEngine::new(rules, 1, 10);

    assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
    assert_eq!(check(&mut engine, "b", 0), Decision::Allow);
    assert_eq!(engine.active_keys(), 1);
    assert_eq!(engine.active_cells(), 1);
    assert_eq!(engine.metrics().overflow_untracked, 1);
}

#[test]
fn sample_overflow_policy_uses_bounded_tracking_subset() {
    let mut sample_rule = rule(1);
    sample_rule.overflow_policy = OverflowPolicy::Sample;
    sample_rule.limit = 1_000;
    sample_rule.local_fallback_limit = 1_000;
    sample_rule.local_absolute_limit = 1_000;
    let rules = RuleTable::new(vec![sample_rule]);
    let mut engine = LocalEngine::new(rules, 1, 10);

    assert_eq!(check(&mut engine, "seed", 0), Decision::Allow);
    for index in 0..32 {
        let value = format!("tenant-{index}");
        assert_eq!(check(&mut engine, &value, index + 1), Decision::Allow);
    }

    let metrics = engine.metrics();
    assert_eq!(engine.active_keys(), 1);
    assert!(metrics.overflow_sampled > 0);
    assert!(metrics.overflow_untracked > 0);
}

#[quickcheck]
fn quickcheck_descriptor_matching_is_deterministic_for_exact_values_and_wildcards(
    case: DescriptorMatchCase,
) -> TestResult {
    let request_value = format!("tenant-{}", case.request_value);
    let exact_value = format!("tenant-{}", case.exact_value);
    let request_domain = if case.wrong_domain { "other" } else { "api" };
    let mut exact = exact_value_rule(1, &exact_value);
    if case.disabled_exact {
        exact.mode = EnforcementMode::Disabled;
    }
    let mut rules = vec![exact];
    if case.include_wildcard {
        rules.push(exact_value_rule(2, "*"));
    }
    let rules = RuleTable::new(rules);
    let descriptors = [Descriptor {
        key: "tenant",
        value: request_value.as_str(),
    }];
    let request = LimitRequest {
        domain: request_domain,
        descriptors: &descriptors,
        hits: 1,
    };
    let matched = rules
        .matching(&request)
        .map(|rule| rule.id)
        .collect::<Vec<_>>();
    let mut expected = Vec::new();

    if !case.wrong_domain && !case.disabled_exact && case.request_value == case.exact_value {
        expected.push(1);
    }
    if !case.wrong_domain && case.include_wildcard {
        expected.push(2);
    }

    if matched == expected {
        TestResult::passed()
    } else {
        TestResult::error("descriptor matcher returned a rule set that diverged from model")
    }
}

#[quickcheck]
fn quickcheck_overflow_policies_never_exceed_key_or_cell_capacity(
    case: OverflowPolicyCase,
) -> TestResult {
    let mut checked_rule = rule(1);
    checked_rule.overflow_policy = case.policy;
    checked_rule.limit = 1_000;
    checked_rule.local_fallback_limit = 1_000;
    checked_rule.local_absolute_limit = 1_000;
    let rules = RuleTable::new(vec![checked_rule]);
    let mut engine = LocalEngine::new(rules, 1, 10);

    for index in 0..case.attempts {
        let value = format!("tenant-{index}");
        let _ = check(&mut engine, &value, u64::from(index));
        if engine.active_keys() > 1 || engine.active_cells() > 10 {
            return TestResult::error("overflow policy grew storage beyond configured capacity");
        }
    }

    let metrics = engine.metrics();
    match case.policy {
        OverflowPolicy::UseOverflowKey if metrics.overflow_key_uses == 0 => {
            TestResult::error("overflow-key policy did not record overflow use")
        }
        OverflowPolicy::AllowUntracked if metrics.overflow_untracked == 0 => {
            TestResult::error("allow-untracked policy did not record untracked overflow")
        }
        OverflowPolicy::Reject if metrics.overflow_rejected == 0 => {
            TestResult::error("reject overflow policy did not reject overflow")
        }
        OverflowPolicy::Sample
            if metrics.overflow_sampled == 0 && metrics.overflow_untracked == 0 =>
        {
            TestResult::error("sample overflow policy did not sample or allow overflow")
        }
        _ => TestResult::passed(),
    }
}

#[quickcheck]
fn quickcheck_window_totals_match_live_buckets(case: WindowTotalsCase) -> TestResult {
    let mut checked_rule = rule(1);
    checked_rule.limit = 1_000;
    checked_rule.local_fallback_limit = 1_000;
    checked_rule.local_absolute_limit = 1_000;
    let rules = RuleTable::new(vec![checked_rule]);
    let mut engine = LocalEngine::new(rules, 4, 10);
    let tenants = ["a", "b", "c", "d"];

    for op in case.ops {
        let tenant = tenants[op.tenant as usize % tenants.len()];
        let now_millis = u64::from(op.now_millis);
        if check(&mut engine, tenant, now_millis) != Decision::Allow {
            return TestResult::error("high-limit generated request was rejected");
        }

        for entry in engine.store.entries.iter().filter(|entry| entry.occupied) {
            let local_total = entry
                .buckets
                .iter()
                .map(|bucket| bucket.local_count)
                .sum::<u64>();
            let estimated_total = entry
                .buckets
                .iter()
                .map(|bucket| bucket.estimated_total)
                .sum::<u64>();

            if entry.local_window_total != local_total
                || entry.estimated_window_total != estimated_total
                || entry.estimated_window_total < entry.local_window_total
            {
                return TestResult::error("stored window totals diverged from live bucket sums");
            }
        }
    }
    TestResult::passed()
}

#[quickcheck]
fn quickcheck_limits_never_exceed_fallback_or_absolute_caps(case: LimitCase) -> TestResult {
    let local_fallback_limit = u64::from(case.fallback_limit);
    let attempts = u64::from(case.attempts);
    let mut checked_rule = rule(1);
    checked_rule.limit = 64;
    checked_rule.local_fallback_limit = local_fallback_limit;
    checked_rule.local_absolute_limit = local_fallback_limit + 2;
    let rules = RuleTable::new(vec![checked_rule]);
    let mut stale_engine = LocalEngine::new(rules, 4, 10);
    let mut allowed = 0_u64;

    for now_millis in 0..attempts {
        match check(&mut stale_engine, "a", now_millis) {
            Decision::Allow => allowed = allowed.saturating_add(1),
            Decision::Reject(RejectReason::LocalFallbackLimit) => break,
            Decision::Reject(_) => {
                return TestResult::error("stale decision rejected for non-fallback reason");
            }
        }
    }
    if allowed > local_fallback_limit {
        return TestResult::error("stale decisions exceeded local fallback limit");
    }

    let mut checked_rule = rule(1);
    checked_rule.limit = 64;
    checked_rule.local_fallback_limit = 64;
    checked_rule.local_absolute_limit = local_fallback_limit + 2;
    let rules = RuleTable::new(vec![checked_rule]);
    let mut fresh_engine = LocalEngine::new(rules, 4, 10);
    fresh_engine.mark_global_estimate_updated(0);
    let mut allowed = 0_u64;

    for now_millis in 0..attempts {
        match check(&mut fresh_engine, "a", now_millis) {
            Decision::Allow => allowed = allowed.saturating_add(1),
            Decision::Reject(RejectReason::LocalAbsoluteLimit) => break,
            Decision::Reject(_) => {
                return TestResult::error("fresh decision rejected for non-absolute reason");
            }
        }
    }
    if allowed <= local_fallback_limit + 2 {
        TestResult::passed()
    } else {
        TestResult::error("fresh decisions exceeded local absolute limit")
    }
}
