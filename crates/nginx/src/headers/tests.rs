use super::*;
use crate::rules::RuleSpec;

fn info(limit: u64, delta_until_admit_millis: u64, now_millis: u64) -> RejectInfo {
    RejectInfo {
        spec: RuleSpec {
            id: 1,
            fingerprint: 0,
            limit,
            bucket_millis: 1_000,
            window_millis: 60_000,
            live_buckets: 60,
        },
        total: limit,
        now_millis,
        delta_until_admit_millis,
    }
}

#[test]
fn limit_and_remaining_format_into_buffers() {
    let h = RejectHeaders::build(info(10, 59_500, 500));
    assert_eq!(h.limit.as_str(), "10");
    assert_eq!(h.remaining.as_str(), "0");
}

#[test]
fn retry_after_matches_library_helper() {
    // 59500ms rounds up to 60s — both fields go through the library.
    let h = RejectHeaders::build(info(10, 59_500, 500));
    assert_eq!(
        h.retry_after.as_str(),
        gabion::window::retry_after_seconds(59_500).to_string(),
    );
    assert_eq!(h.retry_after.as_str(), "60");
}

#[test]
fn reset_matches_library_helper() {
    let h = RejectHeaders::build(info(10, 59_500, 500));
    let expected = gabion::window::reset_unix_seconds(500, 59_500);
    assert_eq!(h.reset.as_str(), expected.to_string());
    // now=500ms -> 0s; retry_after=60s; reset = 0 + 60 = 60.
    assert_eq!(h.reset.as_str(), "60");
}

#[test]
fn sub_second_delta_floors_retry_after_at_one() {
    // delta=200ms rounds up to 1s; reset = floor(100/1000)+1 = 1.
    let h = RejectHeaders::build(info(10, 200, 100));
    assert_eq!(h.retry_after.as_str(), "1");
    assert_eq!(h.reset.as_str(), "1");
}

#[test]
fn body_says_over_by() {
    let h = RejectHeaders::build(info(3, 60_000, 0));
    assert!(h.body.as_str().contains("limit=3"));
    assert!(h.body.as_str().contains("over_by=1"));
}
