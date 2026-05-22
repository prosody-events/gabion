//! Multi-producer single-consumer ring buffer laid out in shared memory.
//!
//! Workers push request events concurrently; the leader's drain task is the
//! sole consumer. Implementation follows the classic Vyukov bounded MPMC
//! design — each slot carries a sequence counter that doubles as occupancy
//! and ordering. Capacity is a power of two so the modulo collapses to a
//! mask.

use std::sync::atomic::{AtomicU64, Ordering};

/// Drop reason returned by `push` when the ring is at capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueOverflow;

/// One queued hit. Wire-compatible across processes — every field is an
/// atomic so producers can write via `&QueueSlot` without raw `*mut`
/// reborrow gymnastics (Stacked Borrows compatible).
///
/// Layout-wise, `AtomicU64` is identical to `u64`/`AtomicU32` to `u32`, so
/// the SHM byte image is unchanged.
#[repr(C)]
#[derive(Debug)]
pub struct QueueSlot {
    pub seq: AtomicU64,
    pub rule_fingerprint_lo: AtomicU64,
    pub rule_fingerprint_hi: AtomicU64,
    pub key_hash_lo: AtomicU64,
    pub key_hash_hi: AtomicU64,
    pub bucket: std::sync::atomic::AtomicU32,
    pub _pad: std::sync::atomic::AtomicU32,
    pub hits: AtomicU64,
    pub rule_limit: AtomicU64,
    pub now_millis: AtomicU64,
}

impl QueueSlot {
    pub fn empty(seq: u64) -> Self {
        Self {
            seq: AtomicU64::new(seq),
            rule_fingerprint_lo: AtomicU64::new(0),
            rule_fingerprint_hi: AtomicU64::new(0),
            key_hash_lo: AtomicU64::new(0),
            key_hash_hi: AtomicU64::new(0),
            bucket: std::sync::atomic::AtomicU32::new(0),
            _pad: std::sync::atomic::AtomicU32::new(0),
            hits: AtomicU64::new(0),
            rule_limit: AtomicU64::new(0),
            now_millis: AtomicU64::new(0),
        }
    }
}

/// Plain-data envelope passed across the queue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueueEvent {
    pub rule_fingerprint: u128,
    pub key_hash: u128,
    pub bucket: u32,
    pub hits: u64,
    pub rule_limit: u64,
    pub now_millis: u64,
}

#[repr(C)]
#[derive(Debug)]
pub struct QueueControl {
    pub enqueue_pos: AtomicU64,
    pub dequeue_pos: AtomicU64,
    pub capacity_mask: AtomicU64,
    pub dropped: AtomicU64,
}

impl QueueControl {
    pub fn new(capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two() && capacity >= 2);
        Self {
            enqueue_pos: AtomicU64::new(0),
            dequeue_pos: AtomicU64::new(0),
            capacity_mask: AtomicU64::new((capacity - 1) as u64),
            dropped: AtomicU64::new(0),
        }
    }
}

/// Borrowed view of the ring shared between producers and the consumer. All
/// access goes through atomics; `&self` is sufficient.
#[derive(Clone, Copy, Debug)]
pub struct RequestQueue<'a> {
    control: &'a QueueControl,
    slots: &'a [QueueSlot],
}

impl<'a> RequestQueue<'a> {
    pub fn from_parts(control: &'a QueueControl, slots: &'a [QueueSlot]) -> Self {
        Self { control, slots }
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub fn dropped(&self) -> u64 {
        self.control.dropped.load(Ordering::Relaxed)
    }

    /// Producer entry point. Canonical Vyukov bounded MPMC enqueue: read
    /// `enqueue_pos`, inspect the slot's sequence to decide whether it's
    /// claimable, CAS the position, then write payload + publish via the
    /// slot's seq store. Returns `QueueOverflow` when the slot's seq says
    /// the ring is full and bumps the dropped counter.
    pub fn push(&self, event: QueueEvent) -> Result<(), QueueOverflow> {
        let mask = self.control.capacity_mask.load(Ordering::Relaxed);
        loop {
            let pos = self.control.enqueue_pos.load(Ordering::Relaxed);
            let slot = &self.slots[(pos & mask) as usize];
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = (seq as i64).wrapping_sub(pos as i64);
            if diff == 0 {
                if self
                    .control
                    .enqueue_pos
                    .compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    write_slot(slot, event);
                    slot.seq.store(pos + 1, Ordering::Release);
                    return Ok(());
                }
            } else if diff < 0 {
                self.control.dropped.fetch_add(1, Ordering::Relaxed);
                return Err(QueueOverflow);
            }
            std::hint::spin_loop();
        }
    }

    /// Consumer entry point. Single-consumer: no CAS on `dequeue_pos`
    /// because no other thread mutates it.
    pub fn pop(&self) -> Option<QueueEvent> {
        let mask = self.control.capacity_mask.load(Ordering::Relaxed);
        let capacity = self.slots.len() as u64;
        let pos = self.control.dequeue_pos.load(Ordering::Relaxed);
        let slot = &self.slots[(pos & mask) as usize];
        let seq = slot.seq.load(Ordering::Acquire);
        let diff = (seq as i64).wrapping_sub((pos + 1) as i64);
        if diff < 0 {
            return None;
        }
        // diff > 0 would mean the producer skipped ahead, which can't
        // happen for a single-consumer ring — treat it like data being
        // available at this pos (read the slot we own).
        let event = read_slot(slot);
        slot.seq.store(pos + capacity, Ordering::Release);
        self.control.dequeue_pos.store(pos + 1, Ordering::Release);
        Some(event)
    }

    /// Pop up to `out.len()` events into `out`. Returns count drained.
    pub fn drain(&self, out: &mut [QueueEvent]) -> usize {
        let mut filled = 0;
        while filled < out.len() {
            match self.pop() {
                Some(ev) => {
                    out[filled] = ev;
                    filled += 1;
                }
                None => break,
            }
        }
        filled
    }
}

/// Publish payload into a slot the caller has just claimed.
///
/// All fields on `QueueSlot` are atomics, so direct writes through
/// `&QueueSlot` are sound (no `*const → *mut` reborrow needed). The
/// `Release`/`Acquire` pair on `slot.seq` (stored by the caller after this
/// returns, loaded by the consumer in `pop`) establishes the happens-before
/// edge that makes these `Relaxed` writes visible. Single-producer access
/// per `pos` is enforced by `enqueue_pos.compare_exchange_weak` in `push`.
fn write_slot(slot: &QueueSlot, event: QueueEvent) {
    // All slot fields are atomics; writes through `&QueueSlot` are sound
    // under both Stacked Borrows and Tree Borrows. Single-writer per
    // `(pos & mask)` is enforced by the seq protocol (see push()).
    let fp = event.rule_fingerprint;
    let kh = event.key_hash;
    slot.rule_fingerprint_lo.store(fp as u64, Ordering::Relaxed);
    slot.rule_fingerprint_hi
        .store((fp >> 64) as u64, Ordering::Relaxed);
    slot.key_hash_lo.store(kh as u64, Ordering::Relaxed);
    slot.key_hash_hi.store((kh >> 64) as u64, Ordering::Relaxed);
    slot.bucket.store(event.bucket, Ordering::Relaxed);
    slot.hits.store(event.hits, Ordering::Relaxed);
    slot.rule_limit.store(event.rule_limit, Ordering::Relaxed);
    slot.now_millis.store(event.now_millis, Ordering::Relaxed);
}

fn read_slot(slot: &QueueSlot) -> QueueEvent {
    QueueEvent {
        rule_fingerprint: ((slot.rule_fingerprint_hi.load(Ordering::Relaxed) as u128) << 64)
            | slot.rule_fingerprint_lo.load(Ordering::Relaxed) as u128,
        key_hash: ((slot.key_hash_hi.load(Ordering::Relaxed) as u128) << 64)
            | slot.key_hash_lo.load(Ordering::Relaxed) as u128,
        bucket: slot.bucket.load(Ordering::Relaxed),
        hits: slot.hits.load(Ordering::Relaxed),
        rule_limit: slot.rule_limit.load(Ordering::Relaxed),
        now_millis: slot.now_millis.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests;
