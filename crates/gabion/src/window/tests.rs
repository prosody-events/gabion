use super::*;

/// Build a closure backed by an inline `(bucket, count)` table. Returns
/// `0` for any bucket not listed — matches the SHM/DashMap "no row" case.
fn counts_from(table: &[(BucketEpoch, u64)]) -> impl Fn(BucketEpoch) -> u64 + '_ {
    move |bucket| {
        table
            .iter()
            .find_map(|(b, c)| if *b == bucket { Some(*c) } else { None })
            .unwrap_or(0)
    }
}

#[test]
fn already_admittable_returns_zero() {
    // total + hits <= limit: no waiting needed.
    let delta = time_until_admit_millis(10_000, 1_000, 5, 10, 5, 3, counts_from(&[(6, 5)]));
    assert_eq!(delta, 0);
}

#[test]
fn single_bucket_window_falls_off_at_end_of_current() {
    // live_buckets = 1: equivalent to a fixed window.
    // current = now / bm = 500 / 1000 = 0. Bucket 0 falls off at
    // (0 + 1) * 1000 = 1000 ms; delta from now (500) is 500.
    let delta = time_until_admit_millis(500, 1_000, 1, 10, 10, 1, counts_from(&[(0, 10)]));
    assert_eq!(delta, 500);
}

#[test]
fn oldest_bucket_alone_covers_need() {
    // Window = [6..10]. oldest=6 has 15 hits, need=11. The first bucket
    // we walk already covers `need`, so the delta is the oldest's
    // fall-off: (6+5)*1000 - 10000 = 1000.
    let delta = time_until_admit_millis(
        10_000,
        1_000,
        5,
        20,
        30,
        1,
        counts_from(&[(6, 15), (7, 4), (8, 4), (9, 4), (10, 3)]),
    );
    assert_eq!(delta, 1_000);
}

#[test]
fn empty_oldest_skips_to_next_non_empty() {
    // Window = [6..10]. bucket 6 is empty (0 hits). bucket 7 has 15,
    // which alone covers need=11. Delta = (7+5)*1000 - 10000 = 2000.
    let delta = time_until_admit_millis(
        10_000,
        1_000,
        5,
        20,
        30,
        1,
        counts_from(&[(6, 0), (7, 15), (8, 5), (9, 5), (10, 5)]),
    );
    assert_eq!(delta, 2_000);
}

#[test]
fn need_accumulates_across_multiple_buckets() {
    // Window = [6..10]. Each bucket holds 5; need=6 is satisfied only
    // after aging both 6 and 7 off. Delta = (7+5)*1000 - 10000 = 2000.
    let delta = time_until_admit_millis(
        10_000,
        1_000,
        5,
        20,
        25,
        1,
        counts_from(&[(6, 5), (7, 5), (8, 5), (9, 5), (10, 5)]),
    );
    assert_eq!(delta, 2_000);
}

#[test]
fn multi_hit_request_needs_more_aged_off() {
    // hits=5 with total=18, limit=20 — need = 18+5-20 = 3. Oldest
    // (bucket 6) has 4, which alone covers it.
    let delta = time_until_admit_millis(
        10_000,
        1_000,
        5,
        20,
        18,
        5,
        counts_from(&[(6, 4), (7, 4), (8, 4), (9, 3), (10, 3)]),
    );
    assert_eq!(delta, 1_000);
}

#[test]
fn saturated_walk_falls_back_to_full_window() {
    // Closure deliberately under-reports vs. the supplied `total`: the
    // walk completes without ever crossing `need`, so the helper falls
    // back to the newest bucket's fall-off — the full-window upper
    // bound (live * bm = 5 * 1000 = 5000) from `now`.
    let delta = time_until_admit_millis(
        10_000,
        1_000,
        5,
        10,
        30,
        1,
        counts_from(&[(6, 3), (7, 3), (8, 3), (9, 3), (10, 3)]),
    );
    assert_eq!(delta, 5_000);
}

#[test]
fn limit_zero_returns_full_window_fallback() {
    // limit=0 means no request can ever be admitted from current state;
    // every bucket is empty so aged_off stays at 0 and we fall back to
    // the full-window upper bound.
    let delta = time_until_admit_millis(10_000, 1_000, 5, 0, 0, 1, counts_from(&[]));
    assert_eq!(delta, 5_000);
}

#[test]
fn now_off_bucket_boundary_subtracts_exactly() {
    // now=10_500 -> current=10, window=[6..10]. Oldest covers need.
    // Delta = (6+5)*1000 - 10_500 = 500 (not rounded to 1000).
    let delta = time_until_admit_millis(10_500, 1_000, 5, 10, 20, 1, counts_from(&[(6, 20)]));
    assert_eq!(delta, 500);
}

#[test]
fn retry_after_rounds_up_with_floor_of_one() {
    assert_eq!(retry_after_seconds(0), 1);
    assert_eq!(retry_after_seconds(1), 1);
    assert_eq!(retry_after_seconds(999), 1);
    assert_eq!(retry_after_seconds(1_000), 1);
    assert_eq!(retry_after_seconds(1_001), 2);
    assert_eq!(retry_after_seconds(59_500), 60);
    assert_eq!(retry_after_seconds(60_000), 60);
}

#[test]
fn reset_anchors_on_now_seconds_plus_retry_after() {
    // now=500ms -> 0s. delta=59500ms -> retry_after=60. reset = 0 + 60.
    assert_eq!(reset_unix_seconds(500, 59_500), 60);
    // Sub-second delta: now=100ms -> 0s, retry_after floors at 1.
    assert_eq!(reset_unix_seconds(100, 200), 1);
    // Wall-clock now: 1_770_000_000_500 -> 1_770_000_000s, delta=1000ms.
    assert_eq!(
        reset_unix_seconds(1_770_000_000_500, 1_000),
        1_770_000_000 + 1
    );
}

#[test]
fn limit_remaining_saturates_at_zero() {
    assert_eq!(limit_remaining(10, 4), 6);
    assert_eq!(limit_remaining(10, 10), 0);
    assert_eq!(limit_remaining(10, 25), 0);
}

#[test]
fn next_bucket_boundary_is_full_bucket_at_boundary() {
    // now exactly on the boundary -> a full bucket until the next one.
    assert_eq!(time_until_next_bucket_boundary_millis(0, 1_000), 1_000);
    assert_eq!(time_until_next_bucket_boundary_millis(10_000, 1_000), 1_000);
}

#[test]
fn next_bucket_boundary_off_boundary_subtracts_remainder() {
    // 500ms into a 1000ms bucket leaves 500ms.
    assert_eq!(time_until_next_bucket_boundary_millis(500, 1_000), 500);
    // 1_234ms into a 250ms bucket: 234 % 250 = 234 -> 16ms left.
    assert_eq!(time_until_next_bucket_boundary_millis(1_234, 250), 16);
}

#[test]
fn next_bucket_boundary_handles_zero_bucket() {
    // Defensive: treat 0 as 1ms (same convention as time_until_admit_millis).
    assert_eq!(time_until_next_bucket_boundary_millis(10, 0), 1);
}
