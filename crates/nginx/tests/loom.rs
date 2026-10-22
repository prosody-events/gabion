//! Loom model checks for the SHM request queue, leader lease and aggregate
//! slot seqlock. Where a Miri run sees the one schedule it happens to hit,
//! loom runs each model under every interleaving (up to a preemption bound),
//! including the stale values a relaxed load may return. Run with
//! `make loom`, whose `loom` feature makes the SHM atomics loom's.
#![cfg(feature = "loom")]

use std::ops::Range;

use gabion_nginx::shm::queue::QueueEvent;
use gabion_nginx::shm::{
    AggregateCell, AggregateSlot, AggregateTable, LeaderLease, QueueControl, QueueSlot,
    RequestQueue,
};
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
            control: QueueControl::new(capacity as usize),
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
        key_hash: wide(id),
        bucket: id as u32,
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
        assert_eq!(ring.queue().dropped(), 1);
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

/// Every field derives from `id`, so a cell mixing two writes matches
/// neither.
fn cell(id: u64) -> AggregateCell {
    AggregateCell {
        rule_fingerprint: wide(id),
        key_hash: wide(id),
        bucket: id as u32,
        count: id,
        last_update_millis: id,
    }
}

/// The leader rewrites slot 0 from one cell to another while a worker looks
/// both up: each lookup returns the whole cell or misses, never a mix.
#[test]
fn aggregate_readers_never_see_a_torn_cell() {
    // Both ids hash home to slot 0, so each lookup reads the slot being
    // rewritten and stops at empty slot 1. The asserts outside the race
    // check that.
    const OLD: u64 = 5;
    const NEW: u64 = 9;
    model(|| {
        let slots = Arc::new([AggregateSlot::empty(), AggregateSlot::empty()]);
        slots[0].write(2, cell(OLD));
        let table = AggregateTable::from_slots(&slots[..]);
        let lookup = |id| {
            let key = cell(id);
            table.get(key.rule_fingerprint, key.key_hash, key.bucket)
        };
        assert_eq!(lookup(OLD), Some(cell(OLD)));

        let writer = {
            let slots = slots.clone();
            thread::spawn(move || slots[0].write(4, cell(NEW)))
        };
        for id in [OLD, NEW] {
            if let Some(seen) = lookup(id) {
                assert_eq!(seen, cell(id), "torn read of cell {id}");
            }
        }
        writer.join().unwrap();
        assert_eq!(lookup(NEW), Some(cell(NEW)));
    });
}
