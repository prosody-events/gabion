//! Lightweight per-zone counters surfaced via `/snapshot` and logs. All
//! mutation is `Relaxed` — these are best-effort observability values, not
//! synchronization signals.

use std::sync::atomic::{AtomicU64, Ordering};

#[repr(C)]
#[derive(Debug, Default)]
pub struct Stats {
    pub requests: AtomicU64,
    pub allowed: AtomicU64,
    pub rejected: AtomicU64,
    pub rejected_cardinality: AtomicU64,
    pub queue_pushed: AtomicU64,
    pub queue_drained: AtomicU64,
    pub queue_dropped: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatsSnapshot {
    pub requests: u64,
    pub allowed: u64,
    pub rejected: u64,
    pub rejected_cardinality: u64,
    pub queue_pushed: u64,
    pub queue_drained: u64,
    pub queue_dropped: u64,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            allowed: self.allowed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            rejected_cardinality: self.rejected_cardinality.load(Ordering::Relaxed),
            queue_pushed: self.queue_pushed.load(Ordering::Relaxed),
            queue_drained: self.queue_drained.load(Ordering::Relaxed),
            queue_dropped: self.queue_dropped.load(Ordering::Relaxed),
        }
    }

    pub fn record_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_allow(&self) {
        self.allowed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_reject(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cardinality_reject(&self) {
        self.rejected_cardinality.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_queue_push(&self) {
        self.queue_pushed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_queue_drop(&self) {
        self.queue_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_queue_drain(&self, n: u64) {
        if n > 0 {
            self.queue_drained.fetch_add(n, Ordering::Relaxed);
        }
    }
}
