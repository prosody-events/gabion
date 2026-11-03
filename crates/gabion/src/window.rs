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
//! functions over caller-supplied closures so the shared aggregate table can
//! feed them without materialising every bucket's count.

use std::num::{NonZeroU32, NonZeroU64};

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
    bucket_millis: NonZeroU64,
    live_buckets: NonZeroU32,
    limit: u64,
    total: u64,
    hits: u64,
    mut count_for: F,
) -> u64
where
    F: FnMut(BucketEpoch) -> u64,
{
    let bm = bucket_millis.get();
    let live = u64::from(live_buckets.get());
    let need = total.saturating_add(hits).saturating_sub(limit);
    if need == 0 {
        return 0;
    }
    // Walk unwrapped `u64` epochs; a stored epoch is the same number cut to
    // `u32`, so the walk and the fall-off times hold across the `u32` wrap.
    let current = now_millis / bm;
    let fall_off = |bucket: u64| {
        (bucket + live)
            .saturating_mul(bm)
            .saturating_sub(now_millis)
    };
    let mut aged_off: u64 = 0;
    for bucket in current.saturating_sub(live - 1)..=current {
        aged_off = aged_off.saturating_add(count_for(BucketEpoch::wrap(bucket)));
        if aged_off >= need {
            return fall_off(bucket);
        }
    }
    // Newest bucket falls off at (current + live) * bm — full-window
    // fallback covers degenerate cases (limit == 0, sum disagrees with
    // breakdown, etc.).
    fall_off(current)
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

/// Wall-clock ms until the next bucket boundary — equivalently, until the
/// oldest live bucket ages off in gabion's uniform sliding window.
///
/// Used as `X-RateLimit-Reset` on the **allow** path. By construction the
/// oldest live bucket has epoch `current - live + 1` and ages off at
/// `(current + 1) * bucket_millis`, so "time until the next bucket
/// boundary" and "time until the oldest live bucket ages off" are the
/// same value — and that value is the next moment quota *might* be
/// reclaimed. No SHM walk required; this is pure arithmetic. The
/// expensive [`time_until_admit_millis`] is reserved for the reject path,
/// where it answers a different question ("when can *this* client retry
/// without being rejected again") that depends on the actual per-bucket
/// distribution.
pub fn time_until_next_bucket_boundary_millis(now_millis: u64, bucket_millis: NonZeroU64) -> u64 {
    bucket_millis.get() - now_millis % bucket_millis
}

#[cfg(test)]
mod tests;
