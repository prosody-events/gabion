//! Per-zone counters. Workers bump the first eight on requests and share
//! their line, as every worker bumps the same ones. Everything the leader
//! writes (each drain pass, each aggregate overflow, the marks below) starts
//! on the next `CACHELINE`, together with `queue_dropped`, which workers bump
//! only when the queue is full.
//! The leader logs four of them at powers of two (`leader::log_bypasses`),
//! and `aggregate_dropped` logs as `aggregate_full`. All mutation is
//! `Relaxed`: observability values, not synchronization.

use std::mem::offset_of;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{CACHELINE, CachePadded};

#[repr(C)]
#[derive(Debug, Default)]
pub struct Stats {
    pub requests: AtomicU64,
    pub allowed: AtomicU64,
    pub rejected: AtomicU64,
    pub rejected_cardinality: AtomicU64,
    pub declines_invalid_descriptor: AtomicU64,
    pub matched_rule_overflows: AtomicU64,
    pub exempted: AtomicU64,
    pub queue_pushed: AtomicU64,
    /// Starts the leader's line: zero bytes, `CACHELINE`-aligned.
    _leader_line: CachePadded<()>,
    pub queue_drained: AtomicU64,
    pub queue_dropped: AtomicU64,
    pub aggregate_dropped: AtomicU64,
    /// The total of each logged counter the leader last read, advanced by
    /// `gabion::throttle::catch_up`; written by the leader only. It lives in
    /// the zone so a new leader carries on from the last across a reload.
    pub logged: Logged,
}

/// The marks `Stats::logged` holds, one per counter the leader logs.
#[repr(C)]
#[derive(Debug, Default)]
pub struct Logged {
    pub matched_rule_overflows: AtomicU64,
    pub queue_dropped: AtomicU64,
    pub declines_invalid_descriptor: AtomicU64,
    pub rejected_cardinality: AtomicU64,
}

// The leader's writes start on the line after the workers' eight counters.
const _: () = assert!(
    offset_of!(Stats, queue_drained) == CACHELINE && align_of::<Stats>() == CACHELINE,
    "keep the counters workers bump on every request on Stats' first cache line, and the ones the \
     leader writes after it",
);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatsSnapshot {
    pub requests: u64,
    pub allowed: u64,
    pub rejected: u64,
    pub rejected_cardinality: u64,
    pub declines_invalid_descriptor: u64,
    pub matched_rule_overflows: u64,
    pub exempted: u64,
    pub queue_pushed: u64,
    pub queue_drained: u64,
    pub queue_dropped: u64,
    pub aggregate_dropped: u64,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            allowed: self.allowed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            rejected_cardinality: self.rejected_cardinality.load(Ordering::Relaxed),
            declines_invalid_descriptor: self.declines_invalid_descriptor.load(Ordering::Relaxed),
            matched_rule_overflows: self.matched_rule_overflows.load(Ordering::Relaxed),
            exempted: self.exempted.load(Ordering::Relaxed),
            queue_pushed: self.queue_pushed.load(Ordering::Relaxed),
            queue_drained: self.queue_drained.load(Ordering::Relaxed),
            queue_dropped: self.queue_dropped.load(Ordering::Relaxed),
            aggregate_dropped: self.aggregate_dropped.load(Ordering::Relaxed),
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

    /// A descriptor's value could not be read as a UTF-8 string; the
    /// request was declined (passed through without recording).
    pub fn record_decline_invalid_descriptor(&self) {
        self.declines_invalid_descriptor
            .fetch_add(1, Ordering::Relaxed);
    }

    /// One per allowed request whose allowed rules outnumbered
    /// `gabion_storage_max_matched_rules`; the request was still allowed and
    /// the rules past the bound under-count.
    pub fn record_matched_rule_overflow(&self) {
        self.matched_rule_overflows.fetch_add(1, Ordering::Relaxed);
    }

    /// A rule's `except_if=` predicate resolved truthy, so the rule was
    /// skipped for this request.
    pub fn record_exempt(&self) {
        self.exempted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_queue_push(&self, n: u64) {
        if n > 0 {
            self.queue_pushed.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Hits the full request queue dropped: their requests were allowed and
    /// never counted. Only a drop writes, so a request that drops nothing
    /// leaves the leader's line alone.
    pub fn record_queue_drop(&self, n: u64) {
        if n > 0 {
            self.queue_dropped.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn record_queue_drain(&self, n: u64) {
        if n > 0 {
            self.queue_drained.fetch_add(n, Ordering::Relaxed);
        }
    }
}
