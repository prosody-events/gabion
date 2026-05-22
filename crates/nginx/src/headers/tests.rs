use super::*;
use crate::rules::RuleSpec;

fn info(limit: u64) -> RejectInfo {
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
        now_millis: 500,
    }
}

#[test]
fn limit_remaining_reset_are_formatted() {
    let h = RejectHeaders::build(info(10));
    assert_eq!(h.limit.as_str(), "10");
    assert_eq!(h.remaining.as_str(), "0");
    assert!(!h.reset.as_str().is_empty());
    assert_eq!(h.reset.as_str(), h.retry_after.as_str());
}

#[test]
fn body_says_over_by() {
    let h = RejectHeaders::build(info(3));
    assert!(h.body.as_str().contains("limit=3"));
    assert!(h.body.as_str().contains("over_by=1"));
}
