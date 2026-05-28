//! Sliding-window header math shared by both adapters.
//!
//! gabion's admission model is a uniform sliding window: per `(rule, key)`,
//! `total = sum(count_for(b))` over the last `live_buckets` sub-buckets of
//! width `bucket_millis`. A bucket `b` ages out at wall-clock time
//! `(b + live_buckets) * bucket_millis`.
//!
//! When a request is rejected, the precise "time until a new request of
//! weight `hits` would succeed" is the smallest such fall-off time at which
//! enough old hits have aged out that `total - aged_out + hits ≤ limit`.
//! [`time_until_admit_millis`] walks buckets oldest → newest accumulating
//! counts, terminating as soon as the running aged-off sum crosses
//! `need = total + hits - limit`.
//!
//! All math is allocation-free and saturating; the helpers are pure
//! functions over caller-supplied closures so both the nginx seqlocked SHM
//! table and the server's DashMap reader can feed them without forcing
//! materialisation of every bucket's count.

use crate::crdt::BucketEpoch;

/// Wall-clock milliseconds until a new request of weight `hits` would
/// succeed under sliding-window admission, given the current per-bucket
/// counts.
///
/// `count_for(b)` returns the count for bucket `b`. The helper queries only
/// buckets in `[current - live_buckets + 1, current]`.
///
/// Returns `0` if a request would already be admitted (defensive — callers
/// already gate on `total + hits > limit`).
pub fn time_until_admit_millis<F>(
    now_millis: u64,
    bucket_millis: u64,
    live_buckets: u32,
    limit: u64,
    total: u64,
    hits: u64,
    mut count_for: F,
) -> u64
where
    F: FnMut(BucketEpoch) -> u64,
{
    let bm = bucket_millis.max(1);
    let live = live_buckets.max(1);
    let need = total.saturating_add(hits).saturating_sub(limit);
    if need == 0 {
        return 0;
    }
    let current = (now_millis / bm) as u32;
    let oldest = current.saturating_sub(live - 1);
    let mut aged_off: u64 = 0;
    let mut bucket = oldest;
    loop {
        aged_off = aged_off.saturating_add(count_for(bucket));
        if aged_off >= need {
            let fall_off = (bucket as u64 + live as u64).saturating_mul(bm);
            return fall_off.saturating_sub(now_millis);
        }
        if bucket == current {
            break;
        }
        bucket = bucket.saturating_add(1);
    }
    // Newest bucket falls off at (current + live) * bm — full-window
    // fallback covers degenerate cases (limit == 0, sum disagrees with
    // breakdown, etc.).
    let fall_off = (current as u64 + live as u64).saturating_mul(bm);
    fall_off.saturating_sub(now_millis)
}

/// `Retry-After` per RFC 7231 §7.1.3. Rounds up so a client retrying at
/// the returned delta is not put back into 429 by sub-second slip.
pub fn retry_after_seconds(delta_millis: u64) -> u64 {
    delta_millis.div_ceil(1_000).max(1)
}

/// `X-RateLimit-Reset` as a unix timestamp in seconds. Anchored on
/// `floor(now_millis / 1000) + retry_after_seconds(delta_millis)` so Reset
/// and `Retry-After` agree relative to whatever clock the `Date:` header
/// used.
pub fn reset_unix_seconds(now_millis: u64, delta_millis: u64) -> u64 {
    (now_millis / 1_000).saturating_add(retry_after_seconds(delta_millis))
}

/// `X-RateLimit-Remaining` floor. On the reject path this is always 0; on
/// the (future) allow-success path it's `limit - total` saturated at u64.
pub fn limit_remaining(limit: u64, total: u64) -> u64 {
    limit.saturating_sub(total)
}

#[cfg(test)]
mod tests;
