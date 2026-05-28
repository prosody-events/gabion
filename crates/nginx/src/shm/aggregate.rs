//! Single-writer multi-reader hash table of aggregate counts, laid out in
//! shared memory.
//!
//! Keys on the portable cell identity `(rule_fingerprint, key_hash, bucket)`.
//! The gossip leader is the only writer (via `AggregateStore::apply`); every
//! worker process reads from this table on the request hot path.
//!
//! Each slot uses a seqlock-style sequence counter:
//! - `0` ⇒ empty
//! - `u64::MAX` ⇒ tombstone
//! - even ⇒ stable
//! - odd ⇒ mid-write
//!
//! Readers load `seq`, abort on `0`, skip on tombstone, retry on `odd`, then
//! re-verify `seq` after the read. Writers bump `seq` to odd, mutate, then
//! bump back to even (`seq + 1`).

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use gabion::crdt::{DeltaSink, ExpirationSink};
use gabion::gossip::AggregateStore;
use gabion::rules::RuleSpec;

use super::AggregateCell;

pub const TOMBSTONE: u64 = u64::MAX;

#[repr(C)]
#[derive(Debug)]
pub struct AggregateSlot {
    pub seq: AtomicU64,
    pub rule_fingerprint_lo: AtomicU64,
    pub rule_fingerprint_hi: AtomicU64,
    pub key_hash_lo: AtomicU64,
    pub key_hash_hi: AtomicU64,
    pub bucket: AtomicU64, // u32 logically, stored as u64 for alignment+clarity
    pub count: AtomicU64,
    pub last_update_millis: AtomicU64,
}

impl AggregateSlot {
    pub const fn empty() -> Self {
        Self {
            seq: AtomicU64::new(0),
            rule_fingerprint_lo: AtomicU64::new(0),
            rule_fingerprint_hi: AtomicU64::new(0),
            key_hash_lo: AtomicU64::new(0),
            key_hash_hi: AtomicU64::new(0),
            bucket: AtomicU64::new(0),
            count: AtomicU64::new(0),
            last_update_millis: AtomicU64::new(0),
        }
    }

    fn matches(&self, fp: u128, kh: u128, bucket: u32) -> bool {
        self.rule_fingerprint_lo.load(Ordering::Relaxed) == fp as u64
            && self.rule_fingerprint_hi.load(Ordering::Relaxed) == (fp >> 64) as u64
            && self.key_hash_lo.load(Ordering::Relaxed) == kh as u64
            && self.key_hash_hi.load(Ordering::Relaxed) == (kh >> 64) as u64
            && self.bucket.load(Ordering::Relaxed) == bucket as u64
    }

    fn write_identity(&self, fp: u128, kh: u128, bucket: u32) {
        self.rule_fingerprint_lo.store(fp as u64, Ordering::Relaxed);
        self.rule_fingerprint_hi
            .store((fp >> 64) as u64, Ordering::Relaxed);
        self.key_hash_lo.store(kh as u64, Ordering::Relaxed);
        self.key_hash_hi.store((kh >> 64) as u64, Ordering::Relaxed);
        self.bucket.store(bucket as u64, Ordering::Relaxed);
    }
}

/// Read-side view of the SHM-resident hash table. Carries a borrowed slot
/// slice; constructed by [`super::ShmRegion::aggregate`] and used by the
/// worker access path. `&self` is enough for every operation — readers go
/// through seqlocks, the single writer's per-slot updates are serialized via
/// the seq counter.
#[derive(Clone, Copy, Debug)]
pub struct AggregateTable<'a> {
    slots: &'a [AggregateSlot],
}

impl<'a> AggregateTable<'a> {
    pub fn from_slots(slots: &'a [AggregateSlot]) -> Self {
        debug_assert!(slots.len().is_power_of_two() && slots.len() >= 2);
        Self { slots }
    }

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    fn mask(&self) -> usize {
        self.slots.len() - 1
    }

    /// Reader: probe forward, return the stored count for
    /// `(rule_fingerprint, key_hash, bucket)`. Returns `None` when the slot is
    /// empty (the key has never been written) or when probing terminates
    /// without a hit.
    pub fn get(&self, fp: u128, kh: u128, bucket: u32) -> Option<AggregateCell> {
        let mut probe = mix_index(fp, kh, bucket) & self.mask();
        let capacity = self.slots.len();
        let mut walked = 0;
        while walked < capacity {
            let cell = self.read_slot(probe);
            match cell {
                ProbeResult::Empty => return None,
                ProbeResult::Tombstone => {
                    probe = (probe + 1) & self.mask();
                    walked += 1;
                    continue;
                }
                ProbeResult::Filled(cell) => {
                    if cell.rule_fingerprint == fp && cell.key_hash == kh && cell.bucket == bucket {
                        return Some(cell);
                    }
                    probe = (probe + 1) & self.mask();
                    walked += 1;
                }
                ProbeResult::Retry => {
                    // Mid-write; spin and re-read the same slot.
                    std::hint::spin_loop();
                }
            }
        }
        None
    }

    /// Sum the counts for the live `[bucket - live_buckets + 1 .. bucket]`
    /// range. Drops back to `bucket = 0` if the subtraction underflows.
    pub fn window_total(
        &self,
        fp: u128,
        kh: u128,
        now_millis: u64,
        bucket_millis: u64,
        live_buckets: u32,
    ) -> u64 {
        let bm = bucket_millis.max(1);
        let current = (now_millis / bm) as u32;
        let live = live_buckets.max(1);
        let mut total = 0_u64;
        for offset in 0..live {
            let Some(bucket) = current.checked_sub(offset) else {
                break;
            };
            if let Some(cell) = self.get(fp, kh, bucket) {
                total = total.saturating_add(cell.count);
            }
        }
        total
    }

    /// Wall-clock ms until a request of weight `hits` for `(spec, kh)`
    /// would be admitted under the sliding-window model. Walks the live
    /// buckets oldest → newest via [`gabion::window::time_until_admit_millis`],
    /// so typical-case cost is one seqlocked SHM read.
    pub fn time_until_admit_millis(
        &self,
        spec: RuleSpec,
        kh: u128,
        now_millis: u64,
        total: u64,
        hits: u64,
    ) -> u64 {
        gabion::window::time_until_admit_millis(
            now_millis,
            spec.bucket_millis,
            spec.live_buckets,
            spec.limit,
            total,
            hits,
            |bucket| {
                self.get(spec.fingerprint, kh, bucket)
                    .map_or(0, |cell| cell.count)
            },
        )
    }

    fn read_slot(&self, index: usize) -> ProbeResult {
        let slot = &self.slots[index];
        let s1 = slot.seq.load(Ordering::Acquire);
        if s1 == 0 {
            return ProbeResult::Empty;
        }
        if s1 == TOMBSTONE {
            return ProbeResult::Tombstone;
        }
        if s1 & 1 == 1 {
            return ProbeResult::Retry;
        }
        let fp = ((slot.rule_fingerprint_hi.load(Ordering::Relaxed) as u128) << 64)
            | slot.rule_fingerprint_lo.load(Ordering::Relaxed) as u128;
        let kh = ((slot.key_hash_hi.load(Ordering::Relaxed) as u128) << 64)
            | slot.key_hash_lo.load(Ordering::Relaxed) as u128;
        let bucket = slot.bucket.load(Ordering::Relaxed) as u32;
        let count = slot.count.load(Ordering::Relaxed);
        let last_update_millis = slot.last_update_millis.load(Ordering::Relaxed);
        let s2 = slot.seq.load(Ordering::Acquire);
        if s1 != s2 {
            return ProbeResult::Retry;
        }
        ProbeResult::Filled(AggregateCell {
            rule_fingerprint: fp,
            key_hash: kh,
            bucket,
            count,
            last_update_millis,
        })
    }
}

enum ProbeResult {
    Empty,
    Tombstone,
    Retry,
    Filled(AggregateCell),
}

/// `AggregateStore<u32>` implementation backed by SHM. Only the gossip
/// leader's thread owns one of these; readers use `AggregateTable`
/// directly. Holds a raw pointer so the type is naturally `!Send + !Sync`.
pub struct ShmAggregateStore {
    slots_ptr: *mut AggregateSlot,
    capacity: usize,
    next_write_seq: Cell<u64>,
}

impl ShmAggregateStore {
    /// # Safety
    /// All of the following must hold for the lifetime of the returned
    /// `ShmAggregateStore`:
    ///
    /// * `slots_ptr` is non-null, properly aligned for `AggregateSlot`, and
    ///   points to the first element of a contiguous region of exactly
    ///   `capacity` `AggregateSlot` values that have been fully initialized
    ///   (e.g. via `AggregateSlot::empty()` performed by the nginx master
    ///   before fork).
    /// * `capacity` is a power of two, at least 2, and the total size `capacity
    ///   * size_of::<AggregateSlot>()` does not exceed `isize::MAX` (this is
    ///     naturally satisfied by any realistic SHM region size).
    /// * The backing memory remains live and mapped for as long as this store —
    ///   and any `AggregateTable<'_>` derived from it via `view()` — is in use.
    ///   In practice that means the `MAP_SHARED | MAP_ANONYMOUS` region
    ///   established by the nginx master must not be unmapped while a worker or
    ///   the leader still references it; the mapping is effectively `'static`
    ///   from the leader's perspective.
    /// * The caller upholds the single-writer invariant: at most one
    ///   `ShmAggregateStore` may issue mutating calls (`write_delta` /
    ///   `write_expiration` / `apply`) against this backing region at a time.
    ///   The store is `!Send + !Sync` (because of `*mut _` and `Cell`), so this
    ///   is enforced at the thread level by the type system; the cross-process
    ///   exclusion is upheld by the gossip leader election.
    /// * Concurrent readers (other processes/threads holding an
    ///   `AggregateTable<'_>` over the same region) are permitted: every
    ///   per-slot field is an `AtomicU64`, and all writes go through the
    ///   seqlock protocol (`seq` odd ⇒ mid-write, even ⇒ stable), so there is
    ///   no data race in the Rust memory model.
    pub unsafe fn new(slots_ptr: *mut AggregateSlot, capacity: usize) -> Self {
        debug_assert!(capacity.is_power_of_two() && capacity >= 2);
        Self {
            slots_ptr,
            capacity,
            next_write_seq: Cell::new(2),
        }
    }

    fn slots(&self) -> &[AggregateSlot] {
        // SAFETY: By the contract of `ShmAggregateStore::new`, `slots_ptr`
        // points to `capacity` fully-initialized, properly-aligned
        // `AggregateSlot`s in a region that stays mapped for the lifetime of
        // `self`. All fields are `AtomicU64`, so concurrent readers holding
        // `&[AggregateSlot]` over the same region and our own atomic stores
        // through `slots_ptr` do not violate Rust's aliasing rules: shared
        // `&` references to atomic types permit interior mutation, and we
        // never hand out a `&mut AggregateSlot`. The returned slice borrows
        // from `&self`, so its lifetime cannot outlive the store, and the
        // total byte length `capacity * size_of::<AggregateSlot>()` fits in
        // `isize` per the same `new` contract.
        unsafe { std::slice::from_raw_parts(self.slots_ptr, self.capacity) }
    }

    fn mask(&self) -> usize {
        self.capacity - 1
    }

    /// Locate the slot for `(fp, kh, bucket)`, returning `(index,
    /// was_present)`. On a miss returns the first empty/tombstone slot
    /// encountered. Returns `None` only when the entire table is full of
    /// live keys (rare; treated as a drop).
    fn probe_for_write(&self, fp: u128, kh: u128, bucket: u32) -> Option<(usize, bool)> {
        let slots = self.slots();
        let mut probe = mix_index(fp, kh, bucket) & self.mask();
        let mut first_vacancy: Option<usize> = None;
        for _ in 0..self.capacity {
            let slot = &slots[probe];
            let seq = slot.seq.load(Ordering::Acquire);
            if seq == 0 {
                return Some((first_vacancy.unwrap_or(probe), false));
            }
            if seq == TOMBSTONE {
                if first_vacancy.is_none() {
                    first_vacancy = Some(probe);
                }
                probe = (probe + 1) & self.mask();
                continue;
            }
            if slot.matches(fp, kh, bucket) {
                return Some((probe, true));
            }
            probe = (probe + 1) & self.mask();
        }
        first_vacancy.map(|i| (i, false))
    }

    fn next_seq(&self) -> u64 {
        // Even sequences only (odd = mid-write); the writer bumps by 1 inside
        // each apply and the visible "stable" value is even.
        let s = self.next_write_seq.get();
        self.next_write_seq.set(s.wrapping_add(2));
        s
    }

    pub(crate) fn write_delta(&self, fp: u128, kh: u128, bucket: u32, delta: u64, now_millis: u64) {
        let Some((index, present)) = self.probe_for_write(fp, kh, bucket) else {
            return;
        };
        let slot = &self.slots()[index];
        let new_seq = self.next_seq();
        slot.seq.store(new_seq.wrapping_sub(1), Ordering::Release); // odd
        if !present {
            slot.write_identity(fp, kh, bucket);
            slot.count.store(delta, Ordering::Relaxed);
        } else {
            let cur = slot.count.load(Ordering::Relaxed);
            slot.count
                .store(cur.saturating_add(delta), Ordering::Relaxed);
        }
        slot.last_update_millis.store(now_millis, Ordering::Relaxed);
        slot.seq.store(new_seq, Ordering::Release); // even
    }

    fn write_expiration(&self, fp: u128, kh: u128, bucket: u32, last_count: u64) {
        let Some((index, present)) = self.probe_for_write(fp, kh, bucket) else {
            return;
        };
        if !present {
            return;
        }
        let slot = &self.slots()[index];
        let new_seq = self.next_seq();
        slot.seq.store(new_seq.wrapping_sub(1), Ordering::Release);
        let cur = slot.count.load(Ordering::Relaxed);
        let next = cur.saturating_sub(last_count);
        slot.count.store(next, Ordering::Relaxed);
        if next == 0 {
            // Convert to tombstone so subsequent reads see the cell as gone.
            slot.seq.store(TOMBSTONE, Ordering::Release);
        } else {
            slot.seq.store(new_seq, Ordering::Release);
        }
    }

    pub fn view(&self) -> AggregateTable<'_> {
        AggregateTable::from_slots(self.slots())
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl AggregateStore<u32> for ShmAggregateStore {
    fn apply(&self, deltas: &DeltaSink<u32>, expirations: &ExpirationSink<u32>) {
        for i in 0..deltas.len() {
            if deltas.applies_locally[i] == 0 {
                continue;
            }
            let key = &deltas.keys[i];
            let d: u64 = deltas.deltas[i].into();
            if d == 0 {
                continue;
            }
            self.write_delta(key.rule_fingerprint, key.key_hash.0, key.bucket, d, 0);
        }
        for i in 0..expirations.len() {
            if expirations.applies_locally[i] == 0 {
                continue;
            }
            let key = &expirations.keys[i];
            let last: u64 = expirations.last_counts[i].into();
            self.write_expiration(key.rule_fingerprint, key.key_hash.0, key.bucket, last);
        }
    }
}

/// Mix `(rule_fingerprint, key_hash, bucket)` into a uniform table index.
/// Uses xxhash3 of the concatenated bytes — fast, stable, low collision rate.
fn mix_index(fp: u128, kh: u128, bucket: u32) -> usize {
    use twox_hash::xxhash3_128;
    let mut buf = [0_u8; 16 + 16 + 4];
    buf[..16].copy_from_slice(&fp.to_le_bytes());
    buf[16..32].copy_from_slice(&kh.to_le_bytes());
    buf[32..].copy_from_slice(&bucket.to_le_bytes());
    let h = xxhash3_128::Hasher::oneshot(&buf);
    (h as u64 ^ (h >> 64) as u64) as usize
}

#[cfg(test)]
mod tests;
