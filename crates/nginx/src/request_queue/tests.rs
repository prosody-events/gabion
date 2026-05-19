use super::*;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;

#[derive(Clone, Debug)]
struct QueueFillCase {
    capacity: u8,
    events: Vec<u8>,
}

impl Arbitrary for QueueFillCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut events = Vec::<u8>::arbitrary(g);
        events.truncate(96);
        Self {
            capacity: (u8::arbitrary(g) % 32).saturating_add(1),
            events,
        }
    }
}

#[derive(Clone, Debug)]
struct QueueWrapCase {
    capacity: u8,
    first: Vec<u8>,
    drain_first: u8,
    second: Vec<u8>,
}

impl Arbitrary for QueueWrapCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let capacity = (u8::arbitrary(g) % 32).saturating_add(1);
        let mut first = Vec::<u8>::arbitrary(g);
        let mut second = Vec::<u8>::arbitrary(g);
        first.truncate(capacity as usize);
        second.truncate(capacity as usize);
        Self {
            capacity,
            first,
            drain_first: u8::arbitrary(g) % capacity,
            second,
        }
    }
}

fn event(value: u8) -> RequestEvent {
    RequestEvent {
        rule_id: 1,
        key_hash: u128::from(value),
        now_millis: u64::from(value),
        hits: 1,
    }
}

fn queue_storage(capacity: usize) -> (SharedRequestRingControl, Vec<SharedRequestEventRecord>) {
    (
        SharedRequestRingControl::empty(),
        vec![SharedRequestEventRecord::default(); capacity],
    )
}

#[quickcheck]
fn queue_preserves_fifo_order_and_reports_overflow(case: QueueFillCase) -> TestResult {
    let capacity = usize::from(case.capacity);
    let (mut control, mut records) = queue_storage(capacity);
    SharedRequestQueue::initialize(&mut control, &mut records);
    let mut queue = SharedRequestQueue::new(&control, &mut records);
    let mut expected = Vec::new();
    let mut dropped = 0_u64;

    for value in case.events {
        let next = event(value);
        match queue.push(next) {
            Ok(()) => expected.push(next),
            Err(_) => dropped = dropped.saturating_add(1),
        }
    }

    let mut actual = vec![RequestEvent::default(); capacity + 1];
    let count = queue.drain(&mut actual);
    if count != expected.len() {
        return TestResult::error(format!(
            "drained {count} events, expected {}",
            expected.len()
        ));
    }
    if actual[..count] != expected {
        return TestResult::error("drained events were not FIFO");
    }
    if queue.dropped() != dropped {
        return TestResult::error(format!("dropped {}, expected {dropped}", queue.dropped()));
    }
    TestResult::passed()
}

#[quickcheck]
fn queue_wraparound_preserves_fifo_order(case: QueueWrapCase) -> TestResult {
    let capacity = usize::from(case.capacity);
    let (mut control, mut records) = queue_storage(capacity);
    SharedRequestQueue::initialize(&mut control, &mut records);
    let mut queue = SharedRequestQueue::new(&control, &mut records);
    let mut expected = Vec::new();

    for value in &case.first {
        let next = event(*value);
        if queue.push(next).is_ok() {
            expected.push(next);
        }
    }

    let drain_first = usize::from(case.drain_first).min(expected.len());
    let mut first_drain = vec![RequestEvent::default(); drain_first];
    let count = queue.drain(&mut first_drain);
    if count != drain_first {
        return TestResult::error(format!("first drain got {count}, expected {drain_first}"));
    }
    if first_drain != expected[..drain_first] {
        return TestResult::error("first drain broke FIFO order");
    }
    expected.drain(..drain_first);

    for value in &case.second {
        let next = event(*value);
        if queue.push(next).is_ok() {
            expected.push(next);
        }
    }

    let mut final_drain = vec![RequestEvent::default(); capacity];
    let count = queue.drain(&mut final_drain);
    if final_drain[..count] != expected {
        return TestResult::error("wraparound drain broke FIFO order");
    }
    TestResult::passed()
}
