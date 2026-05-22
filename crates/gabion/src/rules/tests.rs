use std::time::Duration;

use super::*;

// -- parse_rate -------------------------------------------------------------

#[test]
fn parse_rate_unit_letters() {
    assert_eq!(parse_rate("100r/s").unwrap(), (100, Duration::from_secs(1)));
    assert_eq!(
        parse_rate("100r/m").unwrap(),
        (100, Duration::from_secs(60))
    );
    assert_eq!(parse_rate("1r/h").unwrap(), (1, Duration::from_secs(3600)));
    assert_eq!(parse_rate("1r/d").unwrap(), (1, Duration::from_secs(86400)));
}

#[test]
fn parse_rate_humantime_periods() {
    assert_eq!(parse_rate("5r/30s").unwrap(), (5, Duration::from_secs(30)));
    assert_eq!(
        parse_rate("100r/5m").unwrap(),
        (100, Duration::from_secs(300))
    );
    assert_eq!(
        parse_rate("100r/2h30m").unwrap(),
        (100, Duration::from_secs(2 * 3600 + 30 * 60))
    );
    assert_eq!(
        parse_rate("100r/500ms").unwrap(),
        (100, Duration::from_millis(500))
    );
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

// -- resolve_rate -----------------------------------------------------------

#[test]
fn resolve_rate_window_defaults_to_period() {
    let r = resolve_rate(10, Duration::from_secs(1), None, None).unwrap();
    assert_eq!(
        r,
        ResolvedRate {
            limit: 10,
            window_millis: 1_000,
            bucket_millis: 1_000,
        }
    );
}

#[test]
fn resolve_rate_bucket_defaults_to_window() {
    let r = resolve_rate(
        10,
        Duration::from_secs(1),
        Some(Duration::from_secs(60)),
        None,
    )
    .unwrap();
    assert_eq!(r.window_millis, 60_000);
    assert_eq!(r.bucket_millis, 60_000);
}

#[test]
fn resolve_rate_scales_to_window() {
    // 10 r/s over 5h → 180_000 over 5h.
    let r = resolve_rate(
        10,
        Duration::from_secs(1),
        Some(Duration::from_secs(5 * 3600)),
        Some(Duration::from_secs(3600)),
    )
    .unwrap();
    assert_eq!(r.limit, 180_000);
    assert_eq!(r.window_millis, 5 * 3600 * 1_000);
    assert_eq!(r.bucket_millis, 3600 * 1_000);
}

#[test]
fn resolve_rate_floors_non_multiple_windows() {
    // 10 r/m over 85s → floor(10 * 85000 / 60000) = 14.
    let r = resolve_rate(
        10,
        Duration::from_secs(60),
        Some(Duration::from_secs(85)),
        None,
    )
    .unwrap();
    assert_eq!(r.limit, 14);

    // 10 r/m over 90s → floor(10 * 90000 / 60000) = 15.
    let r = resolve_rate(
        10,
        Duration::from_secs(60),
        Some(Duration::from_secs(90)),
        None,
    )
    .unwrap();
    assert_eq!(r.limit, 15);
}

#[test]
fn resolve_rate_window_equal_to_period_keeps_count() {
    // rate=10r/m window=60s → limit 10. (window == period, no scaling.)
    let r = resolve_rate(
        10,
        Duration::from_secs(60),
        Some(Duration::from_secs(60)),
        Some(Duration::from_secs(1)),
    )
    .unwrap();
    assert_eq!(r.limit, 10);
    assert_eq!(r.bucket_millis, 1_000);
}

#[test]
fn resolve_rate_rejects_window_shorter_than_period() {
    // rate=10r/m window=500ms would give limit=0; reject up front.
    let err = resolve_rate(
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
    let err = resolve_rate(10, Duration::from_secs(1), Some(Duration::ZERO), None).unwrap_err();
    assert_eq!(err, RateResolveError::ZeroWindow);
}

#[test]
fn resolve_rate_rejects_zero_bucket() {
    let err = resolve_rate(
        10,
        Duration::from_secs(1),
        Some(Duration::from_secs(60)),
        Some(Duration::ZERO),
    )
    .unwrap_err();
    assert_eq!(err, RateResolveError::ZeroBucket);
}

#[test]
fn resolve_rate_rejects_overflow() {
    // rate=u64::MAX, window=2s, period=1s → u64::MAX * 2000 overflows u64.
    let err = resolve_rate(
        u64::MAX,
        Duration::from_secs(1),
        Some(Duration::from_secs(2)),
        None,
    )
    .unwrap_err();
    assert_eq!(err, RateResolveError::LimitOverflow);
}
