//! Per-zone counters, in two groups. Workers bump `Stats::workers` on
//! requests and share its line, as every worker bumps the same ones.
//! Everything the leader writes (each drain pass, each aggregate overflow,
//! the marks below) sits in `Stats::leader`, on the lines after it, together
//! with `queue_dropped`, which workers bump only when the queue is full.
//! Leader threads log four of them at powers of two (`leader::log_bypasses`),
//! and `aggregate_dropped` logs as `aggregate_full`. All mutation is
//! `Relaxed`: observability values, not synchronization.

use std::sync::atomic::{AtomicU64, Ordering};

use super::{CACHELINE, CachePadded};

#[repr(C)]
#[derive(Debug, Default)]
pub struct Stats {
    pub workers: WorkerLine,
    /// Starts a line of its own, so no leader write takes the workers' line.
    pub leader: CachePadded<LeaderLine>,
}

/// The counters workers bump on every request.
#[repr(C)]
#[derive(Debug, Default)]
pub struct WorkerLine {
    pub requests: AtomicU64,
    pub allowed: AtomicU64,
    pub rejected: AtomicU64,
    pub rejected_cardinality: AtomicU64,
    pub declines_invalid_descriptor: AtomicU64,
    pub matched_rule_overflows: AtomicU64,
    pub exempted: AtomicU64,
    pub queue_pushed: AtomicU64,
}

// A request that bumps two lines pays two line transfers.
const _: () = assert!(
    size_of::<WorkerLine>() <= CACHELINE,
    "keep the counters workers bump on every request within one cache line"
);

/// What the leader writes, and the counter only a full queue bumps.
#[repr(C)]
#[derive(Debug, Default)]
pub struct LeaderLine {
    pub queue_drained: AtomicU64,
    pub queue_dropped: AtomicU64,
    pub aggregate_dropped: AtomicU64,
    /// The total of each logged counter a leader thread last read, advanced
    /// by `gabion::throttle::catch_up`; written by leader threads only. It
    /// lives in the zone so the threads of every worker, across a reload,
    /// log each power of two once between them.
    pub logged: Logged,
}

/// The marks `LeaderLine::logged` holds, one per counter leader threads log.
#[repr(C)]
#[derive(Debug, Default)]
pub struct Logged {
    pub matched_rule_overflows: AtomicU64,
    pub queue_dropped: AtomicU64,
    pub declines_invalid_descriptor: AtomicU64,
    pub rejected_cardinality: AtomicU64,
}

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
            requests: self.workers.requests.load(Ordering::Relaxed),
            allowed: self.workers.allowed.load(Ordering::Relaxed),
            rejected: self.workers.rejected.load(Ordering::Relaxed),
            rejected_cardinality: self.workers.rejected_cardinality.load(Ordering::Relaxed),
            declines_invalid_descriptor: self
                .workers
                .declines_invalid_descriptor
                .load(Ordering::Relaxed),
            matched_rule_overflows: self.workers.matched_rule_overflows.load(Ordering::Relaxed),
            exempted: self.workers.exempted.load(Ordering::Relaxed),
            queue_pushed: self.workers.queue_pushed.load(Ordering::Relaxed),
            queue_drained: self.leader.queue_drained.load(Ordering::Relaxed),
            queue_dropped: self.leader.queue_dropped.load(Ordering::Relaxed),
            aggregate_dropped: self.leader.aggregate_dropped.load(Ordering::Relaxed),
        }
    }

    pub fn record_request(&self) {
        self.workers.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_allow(&self) {
        self.workers.allowed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_reject(&self) {
        self.workers.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cardinality_reject(&self) {
        self.workers
            .rejected_cardinality
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A descriptor's value could not be read as a UTF-8 string; the
    /// request was declined (passed through without recording).
    pub fn record_decline_invalid_descriptor(&self) {
        self.workers
            .declines_invalid_descriptor
            .fetch_add(1, Ordering::Relaxed);
    }

    /// One per allowed request whose allowed rules outnumbered
    /// `gabion_storage_max_matched_rules`; the request was still allowed and
    /// the rules past the bound under-count.
    pub fn record_matched_rule_overflow(&self) {
        self.workers
            .matched_rule_overflows
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A rule's `except_if=` predicate resolved truthy, so the rule was
    /// skipped for this request.
    pub fn record_exempt(&self) {
        self.workers.exempted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_queue_push(&self, n: u64) {
        if n > 0 {
            self.workers.queue_pushed.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Hits the full request queue dropped: their requests were allowed and
    /// never counted. Only a drop writes, so a request that drops nothing
    /// leaves the leader's line alone.
    pub fn record_queue_drop(&self, n: u64) {
        if n > 0 {
            self.leader.queue_dropped.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn record_queue_drain(&self, n: u64) {
        if n > 0 {
            self.leader.queue_drained.fetch_add(n, Ordering::Relaxed);
        }
    }
}
