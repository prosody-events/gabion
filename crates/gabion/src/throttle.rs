//! The one power-of-two log throttle. A message that can repeat counts each
//! occurrence here and logs only on the 1st, 2nd, 4th, 8th … occurrence, so N
//! events write about log₂(N) lines: one event per millisecond for a day
//! writes 27. A count someone else keeps, read now and then, logs through
//! [`catch_up`] when it passes one of those powers.

use std::sync::atomic::{AtomicU64, Ordering};

/// Count one occurrence on a counter its owner holds exclusively, such as a
/// field of the gossip runtime. Returns the new count when it is a power of
/// two, the occurrences to log.
pub fn bump(count: &mut u64) -> Option<u64> {
    *count = count.saturating_add(1);
    count.is_power_of_two().then_some(*count)
}

/// [`bump`] for a counter that threads or processes share: an `Arc`, a
/// `static`, or a field in NGINX's shared memory.
pub fn bump_shared(count: &AtomicU64) -> Option<u64> {
    let n = count.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    n.is_power_of_two().then_some(n)
}

/// For a count others keep and its owner reads now and then, such as an
/// NGINX shared-memory counter that workers bump and the gossip leader reads
/// each tick: moves `seen` up to `total` and returns `total` when a power of
/// two lies in `(seen, total]`, so the count logs once per power of two it
/// passes, as [`bump`] would, however far it moved between reads.
pub fn catch_up(seen: &AtomicU64, total: u64) -> Option<u64> {
    let before = seen.fetch_max(total, Ordering::Relaxed);
    // `None` orders below `Some`, so a mark of 0 has passed no power yet.
    (total.checked_ilog2() > before.checked_ilog2()).then_some(total)
}

#[cfg(test)]
mod tests;
