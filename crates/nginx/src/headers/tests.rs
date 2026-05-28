use super::*;
use crate::access::AllowInfo;
use crate::rules::RuleSpec;

fn spec(limit: u64) -> RuleSpec {
    RuleSpec {
        id: 1,
        fingerprint: 0,
        limit,
        bucket_millis: 1_000,
        window_millis: 60_000,
        live_buckets: 60,
    }
}

fn reject_info(limit: u64, delta_until_admit_millis: u64, now_millis: u64) -> RejectInfo {
    RejectInfo {
        spec: spec(limit),
        total: limit,
        now_millis,
        delta_until_admit_millis,
    }
}

#[test]
fn reject_limit_and_remaining_format_into_buffers() {
    let h = AdmissionHeaders::build_for_reject(reject_info(10, 59_500, 500));
    assert_eq!(h.limit.as_str(), "10");
    assert_eq!(h.remaining.as_str(), "0");
}

#[test]
fn reject_retry_after_matches_library_helper() {
    // 59500ms rounds up to 60s — both fields go through the library.
    let h = AdmissionHeaders::build_for_reject(reject_info(10, 59_500, 500));
    let retry = h.retry_after.expect("retry_after on reject");
    assert_eq!(
        retry.as_str(),
        gabion::window::retry_after_seconds(59_500).to_string(),
    );
    assert_eq!(retry.as_str(), "60");
}

#[test]
fn reject_reset_matches_library_helper() {
    let h = AdmissionHeaders::build_for_reject(reject_info(10, 59_500, 500));
    let expected = gabion::window::reset_unix_seconds(500, 59_500);
    assert_eq!(h.reset.as_str(), expected.to_string());
    // now=500ms -> 0s; retry_after=60s; reset = 0 + 60 = 60.
    assert_eq!(h.reset.as_str(), "60");
}

#[test]
fn reject_sub_second_delta_floors_retry_after_at_one() {
    // delta=200ms rounds up to 1s; reset = floor(100/1000)+1 = 1.
    let h = AdmissionHeaders::build_for_reject(reject_info(10, 200, 100));
    let retry = h.retry_after.expect("retry_after on reject");
    assert_eq!(retry.as_str(), "1");
    assert_eq!(h.reset.as_str(), "1");
}

#[test]
fn reject_body_says_over_by() {
    let body = RejectBody::build(reject_info(3, 60_000, 0));
    assert!(body.as_str().contains("limit=3"));
    assert!(body.as_str().contains("over_by=1"));
}

#[test]
fn allow_emits_triplet_without_retry_after() {
    // bucket=1000ms, now=250ms: next boundary at 1000ms -> 750ms away.
    // reset = floor(250/1000) + ceil(750/1000) = 0 + 1 = 1.
    let info = AllowInfo {
        spec: spec(10),
        remaining: 7,
        now_millis: 250,
    };
    let h = AdmissionHeaders::build_for_allow(info);
    assert_eq!(h.limit.as_str(), "10");
    assert_eq!(h.remaining.as_str(), "7");
    assert_eq!(h.reset.as_str(), "1");
    assert!(h.retry_after.is_none(), "no Retry-After on allow");
}

#[test]
fn allow_reset_matches_next_bucket_boundary() {
    // now is exactly on a bucket boundary -> full bucket until the next.
    let info = AllowInfo {
        spec: spec(10),
        remaining: 9,
        now_millis: 5_000,
    };
    let h = AdmissionHeaders::build_for_allow(info);
    let expected_delta = gabion::window::time_until_next_bucket_boundary_millis(5_000, 1_000);
    let expected_reset = gabion::window::reset_unix_seconds(5_000, expected_delta);
    assert_eq!(h.reset.as_str(), expected_reset.to_string());
}

#[test]
fn allow_with_zero_remaining_still_emits_headers() {
    // DryRun rule already over its limit: remaining = 0 but headers
    // still ride the response — operators graph "share where Remaining=0"
    // as the would-have-rejected rate.
    let info = AllowInfo {
        spec: spec(5),
        remaining: 0,
        now_millis: 0,
    };
    let h = AdmissionHeaders::build_for_allow(info);
    assert_eq!(h.limit.as_str(), "5");
    assert_eq!(h.remaining.as_str(), "0");
    assert!(h.retry_after.is_none());
}
