use super::*;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::sync::Arc;
use std::thread;

fn event(value: u8) -> QueueEvent {
    QueueEvent {
        rule_fingerprint: u128::from(value),
        key_hash: u128::from(value) << 1,
        bucket: u32::from(value),
        hits: 1,
        rule_limit: 1_000,
        now_millis: u64::from(value),
    }
}

fn build(capacity: usize) -> (Arc<QueueControl>, Arc<Vec<QueueSlot>>) {
    let control = Arc::new(QueueControl::new(capacity));
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
    assert_eq!(out[0].now_millis, 0);
    assert_eq!(out[1].now_millis, 1);
    for i in 3..6 {
        queue.push(event(i)).expect("push");
    }
    let mut tail = [QueueEvent::default(); 4];
    assert_eq!(queue.drain(&mut tail), 4);
    assert_eq!(tail[0].now_millis, 2);
    assert_eq!(tail[1].now_millis, 3);
    assert_eq!(tail[2].now_millis, 4);
    assert_eq!(tail[3].now_millis, 5);
}

#[test]
fn overflow_returns_err_and_bumps_dropped() {
    let (control, slots) = build(2);
    let queue = RequestQueue::from_parts(&control, &slots);
    assert!(queue.push(event(1)).is_ok());
    assert!(queue.push(event(2)).is_ok());
    assert_eq!(queue.dropped(), 0, "no drops before the ring fills");
    // Each push beyond capacity bumps `dropped` by exactly one.
    assert!(queue.push(event(3)).is_err());
    assert_eq!(queue.dropped(), 1);
    assert!(queue.push(event(4)).is_err());
    assert_eq!(queue.dropped(), 2);
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

    let (control, slots) = build(CAPACITY);
    let mut handles = Vec::new();
    for producer in 0..PRODUCERS {
        let control = control.clone();
        let slots = slots.clone();
        handles.push(thread::spawn(move || {
            let queue = RequestQueue::from_parts(&control, &slots);
            let mut pushed = 0_u64;
            for i in 0..PER_PRODUCER {
                let ev = QueueEvent {
                    rule_fingerprint: producer as u128,
                    key_hash: i as u128,
                    bucket: i as u32,
                    hits: 1,
                    rule_limit: 1_000,
                    now_millis: i as u64,
                };
                loop {
                    match queue.push(ev) {
                        Ok(()) => {
                            pushed += 1;
                            break;
                        }
                        Err(_) => std::hint::spin_loop(),
                    }
                }
            }
            pushed
        }));
    }

    let consumer_control = control.clone();
    let consumer_slots = slots.clone();
    let consumer = thread::spawn(move || {
        let queue = RequestQueue::from_parts(&consumer_control, &consumer_slots);
        let target = (PRODUCERS * PER_PRODUCER) as u64;
        let mut seen = 0_u64;
        while seen < target {
            if queue.pop().is_some() {
                seen += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        seen
    });

    let mut total_pushed = 0_u64;
    for h in handles {
        total_pushed += h.join().unwrap();
    }
    let seen = consumer.join().unwrap();
    assert_eq!(total_pushed, (PRODUCERS * PER_PRODUCER) as u64);
    assert_eq!(seen, total_pushed);
}
