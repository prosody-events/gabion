use std::time::Duration;

use super::*;
use crate::settings::Settings;

/// A rule on `tenant` in domain `api`, built as both adapters build theirs.
fn build(
    name: &str,
    rate: &str,
    window_secs: u64,
    bucket_secs: u64,
    mode: EnforcementMode,
) -> Rule {
    RuleDef {
        name,
        domain: "api",
        patterns: &[("tenant", "*")],
        rate,
        window: Some(Duration::from_secs(window_secs)),
        bucket: Some(Duration::from_secs(bucket_secs)),
        mode,
    }
    .build(&Settings::default().validate().unwrap().1.cardinality)
    .unwrap()
}

/// A rule's identity covers its window and bucket, not its limit or mode;
/// two rules with one identity fail to load; and a rule keeps its identity
/// wherever it sits in the table. A key's hash has no rule input at all.
#[test]
fn rule_identity_separates_windows_and_survives_reordering() {
    use EnforcementMode::{DryRun, Enforce};
    let base = build("a", "10r/s", 60, 1, Enforce);
    assert_ne!(
        base.fingerprint(),
        build("a", "10r/s", 120, 1, Enforce).fingerprint()
    );
    assert_ne!(
        base.fingerprint(),
        build("a", "10r/s", 60, 2, Enforce).fingerprint()
    );
    assert_eq!(
        base.fingerprint(),
        build("a", "20r/s", 60, 1, Enforce).fingerprint()
    );
    assert_eq!(
        base.fingerprint(),
        build("a", "10r/s", 60, 1, DryRun).fingerprint()
    );
    assert_eq!(
        RuleTable::new(
            Box::new([base.clone(), build("b", "20r/s", 60, 1, DryRun)]),
            None
        ),
        Err(RuleError::DuplicateRuleIdentity {
            first: "a".into(),
            second: "b".into(),
        })
    );
    let other = build("b", "10r/s", 120, 1, Enforce);
    let forward = RuleTable::new(Box::new([base.clone(), other.clone()]), None).unwrap();
    let backward = RuleTable::new(Box::new([other, base]), None).unwrap();
    for rule in forward.rules() {
        let twin = backward
            .rules()
            .iter()
            .find(|r| r.name == rule.name)
            .unwrap();
        assert_eq!(twin.fingerprint(), rule.fingerprint(), "{}", rule.name);
    }
}

/// The separators keep pairs that run together apart.
#[test]
fn key_hash_separates_pairs_that_run_together() {
    let hash = |pairs: &[(&'static str, &'static str)]| {
        hash_key(
            "api",
            pairs.iter().map(|&(key, value)| Descriptor { key, value }),
        )
    };
    assert_ne!(hash(&[("ab", "c")]), hash(&[("a", "bc")]));
    assert_ne!(hash(&[("a", "b"), ("c", "d")]), hash(&[("a", "bc\0d")]));
}

/// `parse_rate` with the count unwrapped, for comparing against literals.
fn rate(input: &str) -> (u64, Duration) {
    let (count, period) = parse_rate(input).unwrap();
    (count.get(), period)
}

/// `resolve_rate` with a literal count.
fn resolve(
    count: u64,
    period: Duration,
    window: Option<Duration>,
    bucket: Option<Duration>,
) -> Result<ResolvedRate, RateResolveError> {
    resolve_rate(NonZeroU64::new(count).unwrap(), period, window, bucket)
}

// -- parse_rate -------------------------------------------------------------

#[test]
fn parse_rate_unit_letters() {
    assert_eq!(rate("100r/s"), (100, Duration::from_secs(1)));
    assert_eq!(rate("100r/m"), (100, Duration::from_secs(60)));
    assert_eq!(rate("1r/h"), (1, Duration::from_secs(3600)));
    assert_eq!(rate("1r/d"), (1, Duration::from_secs(86400)));
}

#[test]
fn parse_rate_humantime_periods() {
    assert_eq!(rate("5r/30s"), (5, Duration::from_secs(30)));
    assert_eq!(rate("100r/5m"), (100, Duration::from_secs(300)));
    assert_eq!(
        rate("100r/2h30m"),
        (100, Duration::from_secs(2 * 3600 + 30 * 60))
    );
    assert_eq!(rate("100r/500ms"), (100, Duration::from_millis(500)));
}

#[test]
fn parse_rate_rejects_unknown_unit() {
    assert!(parse_rate("100r/fortnight").is_err());
    assert!(parse_rate("100r/").is_err());
    assert!(parse_rate("100").is_err());
    assert!(parse_rate("100/s").is_err());
}

#[test]
fn parse_rate_rejects_zero_count() {
    assert!(parse_rate("0r/s").is_err());
}

#[test]
fn parse_rate_rejects_zero_period() {
    assert!(parse_rate("10r/0s").is_err());
}

#[test]
fn parse_rate_rejects_sub_millisecond_period() {
    assert!(parse_rate("10r/500us").is_err());
    assert!(parse_rate("1r/999us").is_err());
    assert_eq!(rate("1r/1ms"), (1, Duration::from_millis(1)));
}

// -- resolve_rate -----------------------------------------------------------

#[test]
fn resolve_rate_window_defaults_to_period() {
    let r = resolve(10, Duration::from_secs(1), None, None).unwrap();
    assert_eq!(
        r,
        ResolvedRate {
            limit: NonZeroU64::new(10).unwrap(),
            window_millis: 1_000,
            bucket_millis: NonZeroU64::new(1_000).unwrap(),
            live_buckets: NonZeroU32::MIN,
        }
    );
}

#[test]
fn resolve_rate_bucket_defaults_to_window() {
    let r = resolve(
        10,
        Duration::from_secs(1),
        Some(Duration::from_secs(60)),
        None,
    )
    .unwrap();
    assert_eq!(r.window_millis, 60_000);
    assert_eq!(r.bucket_millis.get(), 60_000);
}

#[test]
fn resolve_rate_scales_to_window() {
    // 10 r/s over 5h → 180_000 over 5h.
    let r = resolve(
        10,
        Duration::from_secs(1),
        Some(Duration::from_secs(5 * 3600)),
        Some(Duration::from_secs(3600)),
    )
    .unwrap();
    assert_eq!(r.limit.get(), 180_000);
    assert_eq!(r.window_millis, 5 * 3600 * 1_000);
    assert_eq!(r.bucket_millis.get(), 3600 * 1_000);
}

#[test]
fn resolve_rate_floors_non_multiple_windows() {
    // 10 r/m over 85s → floor(10 * 85000 / 60000) = 14.
    let r = resolve(
        10,
        Duration::from_secs(60),
        Some(Duration::from_secs(85)),
        None,
    )
    .unwrap();
    assert_eq!(r.limit.get(), 14);

    // 10 r/m over 90s → floor(10 * 90000 / 60000) = 15.
    let r = resolve(
        10,
        Duration::from_secs(60),
        Some(Duration::from_secs(90)),
        None,
    )
    .unwrap();
    assert_eq!(r.limit.get(), 15);
}

#[test]
fn resolve_rate_window_equal_to_period_keeps_count() {
    // rate=10r/m window=60s → limit 10. (window == period, no scaling.)
    let r = resolve(
        10,
        Duration::from_secs(60),
        Some(Duration::from_secs(60)),
        Some(Duration::from_secs(1)),
    )
    .unwrap();
    assert_eq!(r.limit.get(), 10);
    assert_eq!(r.bucket_millis.get(), 1_000);
}

#[test]
fn resolve_rate_rejects_window_shorter_than_period() {
    // A window shorter than the rate's period is refused, whatever it would
    // scale to.
    let err = resolve(
        10,
        Duration::from_secs(60),
        Some(Duration::from_millis(500)),
        None,
    )
    .unwrap_err();
    assert_eq!(err, RateResolveError::WindowShorterThanPeriod);
}

#[test]
fn resolve_rate_rejects_zero_window() {
    let err = resolve(10, Duration::from_secs(1), Some(Duration::ZERO), None).unwrap_err();
    assert_eq!(err, RateResolveError::ZeroWindow);
}

#[test]
fn resolve_rate_rejects_sub_millisecond_window() {
    // Only the library API reaches this (`parse_rate` refuses a period under
    // 1 ms): the window floors to 0 ms, and so would the limit.
    let period = Duration::from_micros(500);
    assert_eq!(
        resolve(1, period, None, None).unwrap_err(),
        RateResolveError::ZeroWindow
    );
}

#[test]
fn resolve_rate_rejects_zero_bucket() {
    let err = resolve(
        10,
        Duration::from_secs(1),
        Some(Duration::from_secs(60)),
        Some(Duration::ZERO),
    )
    .unwrap_err();
    assert_eq!(err, RateResolveError::ZeroBucket);
}

#[test]
fn resolve_rate_rejects_bucket_longer_than_window() {
    let second = Some(Duration::from_secs(1));
    assert_eq!(
        resolve(
            10,
            Duration::from_secs(1),
            second,
            Some(Duration::from_secs(2))
        )
        .unwrap_err(),
        RateResolveError::BucketLongerThanWindow
    );
    assert!(resolve(10, Duration::from_secs(1), second, second).is_ok());
}

/// A window of 2^31 buckets or more is refused, counting a partial last
/// bucket as the rule's `live_buckets` does.
#[test]
fn resolve_rate_rejects_2_31_buckets() {
    let millis = |ms| Some(Duration::from_millis(ms));
    let day = Duration::from_secs(86_400);
    assert_eq!(
        resolve(1, day, Some(30 * day), millis(1)).unwrap_err(),
        RateResolveError::WindowTooLong
    );
    let period = Duration::from_millis(1);
    assert_eq!(
        resolve(1, period, millis(4_294_967_295), millis(2)).unwrap_err(),
        RateResolveError::WindowTooLong
    );
    assert!(resolve(1, period, millis(4_294_967_294), millis(2)).is_ok());
}

#[test]
fn resolve_rate_rejects_overflow() {
    // rate=u64::MAX, window=2s, period=1s → u64::MAX * 2000 overflows u64.
    let err = resolve(
        u64::MAX,
        Duration::from_secs(1),
        Some(Duration::from_secs(2)),
        None,
    )
    .unwrap_err();
    assert_eq!(err, RateResolveError::LimitOverflow);
}

/// The stable hasher is the cross-node identity backbone — two *separate*
/// builds with the same rule must emit the same fingerprint. The same-build
/// determinism the gossip tests rely on would not catch a uniform shift (e.g.
/// a `twox-hash` feature change altering XXH3 output), so the values are pinned
/// to known constants here.
#[test]
fn stable_hash_values_are_pinned() {
    assert_eq!(
        rule_fingerprint("api.requests", &[("tenant", "acme")], 60_000, 1_000),
        0x928b_2f28_849a_da90_aa48_c704_2b6e_aa38,
    );
    assert_eq!(
        hash_key(
            "api.requests",
            [Descriptor {
                key: "tenant",
                value: "acme",
            }]
        )
        .0,
        0x1cdc_0717_91aa_9e3b_3a1d_3f93_4b2b_54d6,
    );
    assert_eq!(
        hash_domain("api.requests").0,
        0xec42_b938_8905_54a6_8a84_ad23_b6e1_13ba,
    );
}

/// A rule at exactly the default descriptor-count and key-length bounds
/// builds: the bounds are inclusive.
#[test]
fn rule_at_the_cardinality_bounds_builds() {
    use crate::defaults::{STORAGE_MAX_DESCRIPTOR_COUNT, STORAGE_MAX_KEY_BYTES};
    let limits = Settings::default().validate().unwrap().1.cardinality;
    let keys: Vec<String> = (0..STORAGE_MAX_DESCRIPTOR_COUNT)
        .map(|i| format!("k{i}"))
        .collect();
    let long_key = "k".repeat(STORAGE_MAX_KEY_BYTES);
    let full: Vec<(&str, &str)> = keys.iter().map(|key| (key.as_str(), "*")).collect();
    for patterns in [&full[..], &[(long_key.as_str(), "*")]] {
        let def = RuleDef {
            name: "r",
            domain: "api",
            patterns,
            rate: "10r/s",
            window: None,
            bucket: None,
            mode: EnforcementMode::Enforce,
        };
        assert!(def.build(&limits).is_ok(), "{} patterns", patterns.len());
    }
}

#[test]
fn is_descriptor_key_matches_grammar() {
    assert!(is_descriptor_key("tenant"));
    assert!(is_descriptor_key("tenant_id"));
    assert!(is_descriptor_key("tenant.id"));
    assert!(is_descriptor_key("tenant-id"));
    assert!(is_descriptor_key("_internal"));
    assert!(is_descriptor_key("a"));
    for key in [
        "",
        "9leading",
        ".dot-first",
        "with space",
        "tenant:id",
        "tenant/id",
        "té",
        "-x",
    ] {
        assert!(!is_descriptor_key(key), "{key:?}");
    }
}
