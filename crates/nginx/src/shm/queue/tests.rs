use super::*;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::sync::Arc;
use std::thread;

/// A key with `i` in its high half and `!i` in its low half, so a slot that
/// drops or swaps a half hands back a different key.
fn wide_key(i: u64) -> KeyHash {
    KeyHash(u128::from(i) << 64 | u128::from(!i))
}

fn event(value: u8) -> QueueEvent {
    QueueEvent {
        rule_fingerprint: u128::from(value) << 64 | 0xF1,
        key_hash: wide_key(value.into()),
        bucket: BucketEpoch(u32::from(value)),
        hits: 1,
        rule_limit: 1_000,
        now_millis: u64::from(value),
    }
}

fn build(capacity: usize) -> (Arc<QueueControl>, Arc<Vec<QueueSlot>>) {
    let control = Arc::new(QueueControl::default());
    let slots: Vec<QueueSlot> = (0..capacity).map(|i| QueueSlot::empty(i as u64)).collect();
    (control, Arc::new(slots))
}

#[derive(Clone, Debug)]
struct QueueCase {
    capacity_exp: u8,
    events: Vec<u8>,
}

impl Arbitrary for QueueCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut events = Vec::<u8>::arbitrary(g);
        events.truncate(64);
        Self {
            capacity_exp: (u8::arbitrary(g) % 4).saturating_add(1),
            events,
        }
    }
}

#[quickcheck]
fn fifo_in_one_thread(case: QueueCase) -> TestResult {
    let capacity = 1_usize << case.capacity_exp;
    let (control, slots) = build(capacity);
    let queue = RequestQueue::from_parts(&control, &slots);

    let mut expected = Vec::new();
    for v in &case.events {
        let ev = event(*v);
        if queue.push(ev).is_ok() {
            expected.push(ev);
        }
    }
    if expected.len() != case.events.len().min(capacity) {
        return TestResult::error(format!(
            "accepted {} of {} pushes into {capacity} slots",
            expected.len(),
            case.events.len()
        ));
    }

    let mut out = vec![QueueEvent::default(); expected.len() + 4];
    let drained = queue.drain(&mut out);
    if drained != expected.len() {
        return TestResult::error(format!("drained {drained}, expected {}", expected.len()));
    }
    if out[..drained] != expected[..] {
        return TestResult::error("not FIFO");
    }
    TestResult::passed()
}

#[test]
fn wraparound_preserves_fifo() {
    let (control, slots) = build(4);
    let queue = RequestQueue::from_parts(&control, &slots);
    for i in 0..3 {
        queue.push(event(i)).expect("push");
    }
    // Drain two, then push three more — exercises the index wrap.
    let mut out = [QueueEvent::default(); 2];
    assert_eq!(queue.drain(&mut out), 2);
    assert_eq!(out, [event(0), event(1)]);
    for i in 3..6 {
        queue.push(event(i)).expect("push");
    }
    let mut tail = [QueueEvent::default(); 4];
    assert_eq!(queue.drain(&mut tail), 4);
    assert_eq!(tail, [event(2), event(3), event(4), event(5)]);
}

#[test]
fn mpsc_concurrent_producers() {
    #[cfg(not(miri))]
    const PRODUCERS: usize = 4;
    #[cfg(miri)]
    const PRODUCERS: usize = 2;
    #[cfg(not(miri))]
    const PER_PRODUCER: usize = 256;
    #[cfg(miri)]
    const PER_PRODUCER: usize = 16;
    const CAPACITY: usize = 64;

    /// The `i`th event of `producer`: the producer in both fingerprint
    /// halves and every other field distinct per event, so a torn, swapped
    /// or reordered slot cannot pass for the event it should have carried.
    fn produced(producer: usize, i: usize) -> QueueEvent {
        QueueEvent {
            rule_fingerprint: u128::from(producer as u64) << 64 | producer as u128,
            key_hash: wide_key((producer as u64) << 32 | i as u64),
            bucket: BucketEpoch(i as u32),
            hits: 1,
            rule_limit: 1_000,
            now_millis: i as u64,
        }
    }

    let (control, slots) = build(CAPACITY);
    let mut handles = Vec::new();
    for producer in 0..PRODUCERS {
        let control = control.clone();
        let slots = slots.clone();
        handles.push(thread::spawn(move || {
            let queue = RequestQueue::from_parts(&control, &slots);
            for i in 0..PER_PRODUCER {
                while queue.push(produced(producer, i)).is_err() {
                    std::hint::spin_loop();
                }
            }
        }));
    }

    // Each producer pushes in order into the positions it claims and the one
    // consumer pops in position order, so every producer's events arrive in
    // the order it pushed them.
    let consumer_control = control.clone();
    let consumer_slots = slots.clone();
    let consumer = thread::spawn(move || {
        let queue = RequestQueue::from_parts(&consumer_control, &consumer_slots);
        let mut next = [0_usize; PRODUCERS];
        for _ in 0..PRODUCERS * PER_PRODUCER {
            let ev = loop {
                match queue.pop() {
                    Some(ev) => break ev,
                    None => std::hint::spin_loop(),
                }
            };
            let p = ev.rule_fingerprint as u64 as usize;
            assert!(p < PRODUCERS, "popped an event from no producer: {ev:?}");
            assert_eq!(ev, produced(p, next[p]), "producer {p}, event {}", next[p]);
            next[p] += 1;
        }
        next
    });

    // The consumer first: a failed check there leaves the producers spinning
    // on a full queue, and joining them first would hang the test.
    assert_eq!(consumer.join().unwrap(), [PER_PRODUCER; PRODUCERS]);
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn overlapping_consumers_each_take_distinct_events() {
    #[cfg(not(miri))]
    const TOTAL: usize = 4096;
    #[cfg(miri)]
    const TOTAL: usize = 64;
    const CAPACITY: usize = 16;

    let (control, slots) = build(CAPACITY);
    let popped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let consumers: Vec<_> = (0..2)
        .map(|_| {
            let (control, slots, popped) = (control.clone(), slots.clone(), popped.clone());
            thread::spawn(move || {
                let queue = RequestQueue::from_parts(&control, &slots);
                let mut keys = Vec::new();
                while popped.load(Ordering::Relaxed) < TOTAL {
                    match queue.pop() {
                        Some(ev) => {
                            keys.push(ev.key_hash);
                            popped.fetch_add(1, Ordering::Relaxed);
                        }
                        None => std::hint::spin_loop(),
                    }
                }
                keys
            })
        })
        .collect();

    let queue = RequestQueue::from_parts(&control, &slots);
    for i in 0..TOTAL {
        let ev = QueueEvent {
            key_hash: wide_key(i as u64),
            ..event(0)
        };
        while queue.push(ev).is_err() {
            std::hint::spin_loop();
        }
    }

    let mut keys: Vec<KeyHash> = consumers
        .into_iter()
        .flat_map(|consumer| consumer.join().unwrap())
        .collect();
    keys.sort_unstable();
    // `wide_key` orders keys by `i`, so the sorted keys are its range.
    assert_eq!(keys, (0..TOTAL as u64).map(wide_key).collect::<Vec<_>>());
}
