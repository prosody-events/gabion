//! Unified time source for the gossip runtime.
//!
//! The runtime reads `now_millis()` once per gossip tick to feed
//! [`crate::crdt::CellStore::expire_at`]; the tick interval itself is driven
//! by `tokio::time::interval`, which reads from the same underlying
//! `tokio::time::Instant`, so a single `tokio::time::advance(d)` call moves
//! both the tick scheduler and the bucket clock in lockstep.

use std::cell::Cell;

/// Monotonic, non-decreasing wall-clock source in unix-epoch milliseconds.
pub trait Clock {
    /// Current time in milliseconds since the unix epoch.
    fn now_millis(&self) -> u64;
}

/// Production + simulator default. Bridges `tokio::time::Instant` (monotonic,
/// mockable via `tokio::time::pause()`/`advance()`) to unix-epoch millis via
/// a startup-captured offset.
///
/// Construction requires a tokio runtime in scope so it can capture an
/// `Instant`; use [`FixedClock`] in unit tests that don't have a runtime.
#[derive(Clone, Debug)]
pub struct TokioClock {
    base_millis: u64,
    base_instant: tokio::time::Instant,
}

impl TokioClock {
    /// Capture `SystemTime::now()` as the base and the current
    /// `tokio::time::Instant` as the offset. Production entry point.
    pub fn new() -> Self {
        let base_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            base_millis,
            base_instant: tokio::time::Instant::now(),
        }
    }

    /// Capture an explicit base. Tests using `tokio::time::pause()` pass
    /// `0` (or any seed) so the bucket clock starts at a known value.
    pub fn from_millis(base_millis: u64) -> Self {
        Self {
            base_millis,
            base_instant: tokio::time::Instant::now(),
        }
    }
}

impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TokioClock {
    #[inline]
    fn now_millis(&self) -> u64 {
        let elapsed = tokio::time::Instant::now()
            .saturating_duration_since(self.base_instant)
            .as_millis() as u64;
        self.base_millis.saturating_add(elapsed)
    }
}

/// Hand-driven clock for CRDT bucket-arithmetic unit tests that don't have a
/// tokio runtime. Single-threaded `Cell` interior mutability keeps the trait
/// method `&self`.
#[derive(Debug, Default)]
pub struct FixedClock(Cell<u64>);

impl FixedClock {
    pub fn new(now_millis: u64) -> Self {
        Self(Cell::new(now_millis))
    }

    pub fn set(&self, now_millis: u64) {
        self.0.set(now_millis);
    }

    pub fn advance(&self, delta_millis: u64) {
        self.0.set(self.0.get().saturating_add(delta_millis));
    }
}

impl Clock for FixedClock {
    #[inline]
    fn now_millis(&self) -> u64 {
        self.0.get()
    }
}
