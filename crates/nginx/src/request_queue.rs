use std::sync::atomic::{AtomicU64, Ordering};

use gabion::{HashedLimitRequest, RuleId};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RequestEvent {
    pub rule_id: RuleId,
    pub key_hash: u128,
    pub now_millis: u64,
    pub hits: u64,
}

impl RequestEvent {
    pub fn from_hashed(request: HashedLimitRequest, now_millis: u64) -> Self {
        Self {
            rule_id: request.rule_id(),
            key_hash: request.key_hash().into(),
            now_millis,
            hits: request.hits(),
        }
    }

    pub fn as_hashed(self) -> HashedLimitRequest {
        HashedLimitRequest::new(self.rule_id, self.key_hash, self.hits)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestQueueFull;

#[repr(C)]
#[derive(Debug)]
pub struct SharedRequestRingControl {
    head: AtomicU64,
    tail: AtomicU64,
    dropped: AtomicU64,
}

impl SharedRequestRingControl {
    pub fn empty() -> Self {
        Self {
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SharedRequestEventRecord {
    rule_id: u32,
    key_hash: u128,
    now_millis: u64,
    hits: u64,
}

impl SharedRequestEventRecord {
    fn from_event(event: RequestEvent) -> Self {
        Self {
            rule_id: event.rule_id,
            key_hash: event.key_hash,
            now_millis: event.now_millis,
            hits: event.hits,
        }
    }

    pub fn as_event(self) -> RequestEvent {
        RequestEvent {
            rule_id: self.rule_id,
            key_hash: self.key_hash,
            now_millis: self.now_millis,
            hits: self.hits,
        }
    }
}

#[derive(Debug)]
pub struct SharedRequestQueue<'a> {
    control: &'a SharedRequestRingControl,
    events: &'a mut [SharedRequestEventRecord],
    capacity: usize,
}

impl<'a> SharedRequestQueue<'a> {
    pub fn initialize(
        control: &mut SharedRequestRingControl,
        events: &mut [SharedRequestEventRecord],
    ) {
        *control = SharedRequestRingControl::empty();
        for event in events {
            *event = SharedRequestEventRecord::default();
        }
    }

    pub fn new(
        control: &'a SharedRequestRingControl,
        events: &'a mut [SharedRequestEventRecord],
    ) -> Self {
        Self {
            control,
            capacity: events.len(),
            events,
        }
    }

    pub fn push(&mut self, event: RequestEvent) -> Result<(), RequestQueueFull> {
        let head = self.control.head.load(Ordering::Acquire);
        let tail = self.control.tail.load(Ordering::Acquire);
        if head.saturating_sub(tail) as usize >= self.capacity {
            self.control.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(RequestQueueFull);
        }

        let index = head as usize % self.capacity;
        self.events[index] = SharedRequestEventRecord::from_event(event);
        self.control
            .head
            .store(head.saturating_add(1), Ordering::Release);
        Ok(())
    }

    pub fn drain(&mut self, out: &mut [RequestEvent]) -> usize {
        if out.is_empty() {
            return 0;
        }

        let head = self.control.head.load(Ordering::Acquire);
        let mut tail = self.control.tail.load(Ordering::Acquire);
        let available = head.saturating_sub(tail) as usize;
        let count = available.min(out.len()).min(self.capacity);
        for slot in out.iter_mut().take(count) {
            let index = tail as usize % self.capacity;
            *slot = self.events[index].as_event();
            tail = tail.saturating_add(1);
        }
        self.control.tail.store(tail, Ordering::Release);
        count
    }

    pub fn dropped(&self) -> u64 {
        self.control.dropped()
    }
}

#[cfg(test)]
mod tests {
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
}
