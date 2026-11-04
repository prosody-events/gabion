//! Loom model checks for the SHM request queue, leader lease and aggregate
//! slot seqlock. Where a Miri run sees the one schedule it happens to hit,
//! loom runs each model under every interleaving (up to a preemption bound),
//! including the stale values a relaxed load may return. Run with
//! `make loom`, whose `loom` feature makes the SHM atomics loom's, and
//! the library aggregate table's slot atomics too.
#![cfg(feature = "loom")]

use std::ops::Range;

use gabion::aggregate::{AggregateCell, AggregateSlot, AggregateTable};
use gabion::crdt::{BucketEpoch, KeyHash};
use gabion_nginx::shm::queue::QueueEvent;
use gabion_nginx::shm::{LeaderLease, QueueControl, QueueSlot, RequestQueue};
use loom::sync::Arc;
use loom::thread;

/// Checks `f` under every interleaving with up to two preemptions, where
/// most concurrency bugs surface; `LOOM_MAX_PREEMPTIONS` overrides.
fn model(f: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound.get_or_insert(2);
    builder.check(f);
}

/// A ring of SHM queue slots shared by a model's threads.
struct Ring {
    control: QueueControl,
    slots: Box<[QueueSlot]>,
}

impl Ring {
    fn new(capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            control: QueueControl::default(),
            slots: (0..capacity).map(QueueSlot::empty).collect(),
        })
    }

    fn queue(&self) -> RequestQueue<'_> {
        RequestQueue::from_parts(&self.control, &self.slots)
    }

    /// Pop until the ring is empty.
    fn rest(&self) -> impl Iterator<Item = QueueEvent> + '_ {
        std::iter::from_fn(|| self.queue().pop())
    }
}

/// `id` in both halves, so a torn `u128` doesn't match.
fn wide(id: u64) -> u128 {
    u128::from(id) << 64 | u128::from(id)
}

/// Every field derives from `id`, so a torn read matches no pushed event.
fn event(id: u64) -> QueueEvent {
    QueueEvent {
        rule_fingerprint: wide(id),
        key_hash: KeyHash(wide(id)),
        bucket: BucketEpoch::wrap(id),
        hits: id,
        rule_limit: id,
        now_millis: id,
    }
}

/// Push `ids` from one thread each, returning the events the ring accepted.
fn spawn_producers(
    ring: &Arc<Ring>,
    ids: Range<u64>,
) -> Vec<thread::JoinHandle<Option<QueueEvent>>> {
    ids.map(|id| {
        let ring = ring.clone();
        thread::spawn(move || ring.queue().push(event(id)).ok().map(|()| event(id)))
    })
    .collect()
}

fn joined<T>(threads: Vec<thread::JoinHandle<Option<T>>>) -> Vec<T> {
    threads
        .into_iter()
        .filter_map(|t| t.join().unwrap())
        .collect()
}

fn sorted(mut events: Vec<QueueEvent>) -> Vec<QueueEvent> {
    events.sort_by_key(|e| e.hits);
    events
}

/// Two consumers stand in for a stalled leader overlapping its successor.
#[test]
fn overlapping_consumers_pop_each_event_once() {
    model(|| {
        let ring = Ring::new(2);
        let producers = spawn_producers(&ring, 1..3);
        let consumers = (0..2)
            .map(|_| {
                let ring = ring.clone();
                thread::spawn(move || ring.queue().pop())
            })
            .collect();

        let pushed = joined(producers);
        let mut popped = joined(consumers);
        popped.extend(ring.rest());
        assert_eq!(pushed.len(), 2, "a ring with room rejected a push");
        assert_eq!(sorted(popped), sorted(pushed));
    });
}

#[test]
fn full_ring_drops_exactly_the_overflow() {
    model(|| {
        let ring = Ring::new(2);
        let pushed = joined(spawn_producers(&ring, 1..4));
        assert_eq!(pushed.len(), 2);
        assert_eq!(sorted(ring.rest().collect()), sorted(pushed));
    });
}

/// Each worker's clock only moves forward, but the workers' readings
/// interleave arbitrarily with each other's lease operations. Every grant
/// is a term `[now, now + TTL)`; terms held by different workers must not
/// overlap.
#[test]
fn at_most_one_worker_holds_the_lease() {
    const TTL: u64 = 10;
    model(|| {
        let lease = Arc::new(LeaderLease::default());
        let campaigns = [(1, [0, 8, 30]), (2, [5, 12, 20])].map(|(worker, clock)| {
            let lease = lease.clone();
            thread::spawn(move || {
                clock
                    .into_iter()
                    .filter(|&now| lease.try_acquire(worker, now, TTL))
                    .map(|now| (worker, now..now + TTL))
                    .collect::<Vec<_>>()
            })
        });
        let terms: Vec<_> = campaigns
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect();
        for (a, held) in &terms {
            for (b, other) in &terms {
                assert!(
                    a == b || held.end <= other.start || other.end <= held.start,
                    "workers {a} and {b} both held the lease: {held:?} and {other:?}",
                );
            }
        }
    });
}

/// The leader rewrites slot 0 from `old` to `new` while a worker looks both
/// up: each lookup returns the whole cell or misses, never a mix.
fn race_a_rewrite(old: AggregateCell, new: AggregateCell) {
    model(move || {
        let slots = Arc::new([AggregateSlot::empty(), AggregateSlot::empty()]);
        slots[0].write(2, old);
        let table = AggregateTable::from_slots(&slots[..]);
        let lookup =
            |cell: AggregateCell| table.get(cell.rule_fingerprint, cell.key_hash, cell.bucket);
        // Each lookup must read slot 0, the one being rewritten.
        assert_eq!(
            lookup(old),
            Some(old),
            "{old:?} does not hash home to slot 0"
        );

        let writer = {
            let slots = slots.clone();
            thread::spawn(move || slots[0].write(4, new))
        };
        for cell in [old, new] {
            if let Some(seen) = lookup(cell) {
                assert_eq!(seen, cell, "torn read of {cell:?}");
            }
        }
        writer.join().unwrap();
        assert_eq!(
            lookup(new),
            Some(new),
            "{new:?} does not hash home to slot 0"
        );
    });
}

/// A lookup matches on every key word, so a read mixing two keys that differ
/// in several words matches neither and passes for a miss. Each rewrite
/// below changes one stored word of the key, and the count: a stale copy of
/// that word still matches `old`'s lookup and carries `new`'s count, so a
/// store outside the seqlock in any word turns this red. 23 is the first
/// replacement word with which every rewrite still hashes home to slot 0.
#[test]
fn aggregate_readers_never_see_a_torn_cell() {
    const LO: u128 = u64::MAX as u128;
    let old = AggregateCell {
        rule_fingerprint: wide(5),
        key_hash: KeyHash(wide(5)),
        bucket: BucketEpoch::wrap(5),
        count: 5,
    };
    let rewrite = AggregateCell { count: 23, ..old };
    let (low_word, high_word) = (wide(5) & !LO | 23, 23 << 64 | wide(5) & LO);
    for new in [
        AggregateCell {
            rule_fingerprint: low_word,
            ..rewrite
        },
        AggregateCell {
            rule_fingerprint: high_word,
            ..rewrite
        },
        AggregateCell {
            key_hash: KeyHash(low_word),
            ..rewrite
        },
        AggregateCell {
            key_hash: KeyHash(high_word),
            ..rewrite
        },
        AggregateCell {
            bucket: BucketEpoch::wrap(23),
            ..rewrite
        },
    ] {
        race_a_rewrite(old, new);
    }
}
