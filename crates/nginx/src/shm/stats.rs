//! Lightweight per-zone counters, read by tests and the leader; only
//! `aggregate_dropped` reaches a log line today. All
//! mutation is `Relaxed` — these are best-effort observability values, not
//! synchronization signals.

use std::sync::atomic::{AtomicU64, Ordering};

use gabion::defaults;
use gabion::rules::RuleId;

/// Cap on per-rule counters held in [`Stats::exempted_per_rule`]. A build
/// constant equal to the default rule-dictionary capacity;
/// `gabion_storage_rule_dictionary_capacity` does not change it.
pub const STATS_MAX_RULES: usize = defaults::STORAGE_RULE_DICTIONARY_CAPACITY as usize;

#[repr(C)]
#[derive(Debug)]
pub struct Stats {
    pub requests: AtomicU64,
    pub allowed: AtomicU64,
    pub rejected: AtomicU64,
    pub rejected_cardinality: AtomicU64,
    pub declines_invalid_descriptor: AtomicU64,
    pub matched_rule_overflows: AtomicU64,
    pub exempted: AtomicU64,
    /// Per-rule exempt counter, indexed by `rule_id - 1`. A rule whose id
    /// falls outside `1..=STATS_MAX_RULES` contributes only to the global
    /// `exempted` counter. `STATS_MAX_RULES` is a build constant equal to the
    /// default rule-dictionary capacity;
    /// `gabion_storage_rule_dictionary_capacity` does not change it.
    pub exempted_per_rule: [AtomicU64; STATS_MAX_RULES],
    pub queue_pushed: AtomicU64,
    pub queue_drained: AtomicU64,
    pub queue_dropped: AtomicU64,
    pub aggregate_dropped: AtomicU64,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            requests: AtomicU64::new(0),
            allowed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            rejected_cardinality: AtomicU64::new(0),
            declines_invalid_descriptor: AtomicU64::new(0),
            matched_rule_overflows: AtomicU64::new(0),
            exempted: AtomicU64::new(0),
            exempted_per_rule: [const { AtomicU64::new(0) }; STATS_MAX_RULES],
            queue_pushed: AtomicU64::new(0),
            queue_drained: AtomicU64::new(0),
            queue_dropped: AtomicU64::new(0),
            aggregate_dropped: AtomicU64::new(0),
        }
    }
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
    pub exempted_per_rule: [u64; STATS_MAX_RULES],
    pub queue_pushed: u64,
    pub queue_drained: u64,
    pub queue_dropped: u64,
    pub aggregate_dropped: u64,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        let mut exempted_per_rule = [0_u64; STATS_MAX_RULES];
        for (slot, atom) in exempted_per_rule
            .iter_mut()
            .zip(self.exempted_per_rule.iter())
        {
            *slot = atom.load(Ordering::Relaxed);
        }
        StatsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            allowed: self.allowed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            rejected_cardinality: self.rejected_cardinality.load(Ordering::Relaxed),
            declines_invalid_descriptor: self.declines_invalid_descriptor.load(Ordering::Relaxed),
            matched_rule_overflows: self.matched_rule_overflows.load(Ordering::Relaxed),
            exempted: self.exempted.load(Ordering::Relaxed),
            exempted_per_rule,
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

    /// One per allowed rule whose count did not fit in the request's
    /// `STORAGE_MAX_MATCHED_RULES`-event buffer; the request was still
    /// allowed and that rule under-counts.
    pub fn record_matched_rule_overflow(&self) {
        self.matched_rule_overflows.fetch_add(1, Ordering::Relaxed);
    }

    /// A rule's `except_if=` predicate resolved truthy, so the rule was
    /// skipped for this request. Operators watch this counter to detect a
    /// misconfigured predicate that always-exempts; the per-rule slot
    /// (indexed by `rule_id - 1`) lets them attribute the bypass to a
    /// specific rule rather than the aggregate.
    pub fn record_exempt(&self, rule_id: RuleId) {
        self.exempted.fetch_add(1, Ordering::Relaxed);
        let idx = (rule_id as usize).wrapping_sub(1);
        if idx < STATS_MAX_RULES {
            self.exempted_per_rule[idx].fetch_add(1, Ordering::Relaxed);
        }
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
