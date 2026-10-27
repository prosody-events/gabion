//! The one power-of-two log throttle. A message that can repeat counts each
//! occurrence here and logs only on the 1st, 2nd, 4th, 8th … occurrence, so N
//! events write about log₂(N) lines: one event per millisecond for a day
//! writes 27.

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

#[cfg(test)]
mod tests;
