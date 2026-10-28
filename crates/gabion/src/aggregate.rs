//! The cluster-aggregate table both adapters read on admission: NGINX over
//! slots in its shared memory, `gabiond` over [`Aggregate`]'s heap slots.
//!
//! Keys on the portable cell identity `(rule_fingerprint, key_hash, bucket)`.
//! Every request reads through [`AggregateTable`], which cannot write; the
//! gossip runtime is the only writer, through [`AggregateWriter`].
//!
//! The table is linear-probed, and every cell sits within [`MAX_PROBE`]
//! slots of its home slot: a write that finds no free slot that close is
//! dropped (and counted) rather than placed further away, so no lookup ever
//! walks more than `MAX_PROBE` slots. Removal shifts the cells behind the
//! freed slot back toward their homes (Knuth's Algorithm R), so expiry
//! returns slots to empty and lookups for absent keys stay short.
//!
//! Each slot is a seqlock: `seq == 0` ⇒ empty, odd ⇒ mid-write, even ⇒
//! stable. Readers never wait: a slot that is mid-write, or changes under
//! the read, is skipped. A reader can therefore miss a cell the writer is
//! touching or moving at that instant and under-count it for one request,
//! which the allow-by-default rule accepts.

// The slots' atomics are loom's under the `loom` feature, which
// `gabion-nginx`'s `make loom` turns on; the write sequence and the drop
// counter live outside the slots and stay `std`'s.
#[cfg(feature = "loom")]
use loom::sync::atomic::{AtomicU64, Ordering, fence};
use std::sync::atomic as std_atomic;
#[cfg(not(feature = "loom"))]
use std::sync::atomic::{AtomicU64, Ordering, fence};

use crate::crdt::{
    BucketEpoch, Count, DeltaSink, ExpirationSink, KeyHash, ValidStorageConfig,
    pow2_index_capacity_for,
};
use crate::gossip::AggregateStore;
use crate::rules::RuleSpec;
use crate::settings::Setting;
use crate::throttle;

/// The furthest a cell may sit from its home slot, and so the most slots a
/// lookup reads. At half load, under one insert in 10^10 needs more.
pub const MAX_PROBE: usize = 128;

/// One slot. The layout is NGINX's shared-memory format: each logical `u128`
/// is two `u64`s, because the mapping only guarantees 8-byte alignment.
#[repr(C)]
#[derive(Debug)]
pub struct AggregateSlot {
    seq: AtomicU64,
    rule_fingerprint_lo: AtomicU64,
    rule_fingerprint_hi: AtomicU64,
    key_hash_lo: AtomicU64,
    key_hash_hi: AtomicU64,
    bucket: AtomicU64, // u32 logically, stored as u64 for alignment+clarity
    count: AtomicU64,
    /// Keeps a slot one 64-byte line; the slot layout is NGINX's SHM format.
    _reserved: AtomicU64,
}

/// What one slot holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AggregateCell {
    pub rule_fingerprint: u128,
    pub key_hash: u128,
    pub bucket: BucketEpoch,
    pub count: u64,
}

impl AggregateSlot {
    pub fn empty() -> Self {
        Self {
            seq: AtomicU64::new(0),
            rule_fingerprint_lo: AtomicU64::new(0),
            rule_fingerprint_hi: AtomicU64::new(0),
            key_hash_lo: AtomicU64::new(0),
            key_hash_hi: AtomicU64::new(0),
            bucket: AtomicU64::new(0),
            count: AtomicU64::new(0),
            _reserved: AtomicU64::new(0),
        }
    }

    fn read(&self) -> Read {
        let seq = match self.seq.load(Ordering::Acquire) {
            0 => return Read::Empty,
            seq if seq & 1 == 1 => return Read::Busy,
            seq => seq,
        };
        let cell = AggregateCell {
            rule_fingerprint: ((self.rule_fingerprint_hi.load(Ordering::Relaxed) as u128) << 64)
                | self.rule_fingerprint_lo.load(Ordering::Relaxed) as u128,
            key_hash: ((self.key_hash_hi.load(Ordering::Relaxed) as u128) << 64)
                | self.key_hash_lo.load(Ordering::Relaxed) as u128,
            bucket: self.bucket.load(Ordering::Relaxed) as BucketEpoch,
            count: self.count.load(Ordering::Relaxed),
        };
        // Order the field loads before the re-check of `seq`.
        fence(Ordering::Acquire);
        if self.seq.load(Ordering::Relaxed) == seq {
            Read::Live(cell)
        } else {
            Read::Busy
        }
    }

    /// Publish `cell` under the even sequence `seq`. Only the table's single
    /// writer calls this, with a fresh `seq` each time.
    pub fn write(&self, seq: u64, cell: AggregateCell) {
        debug_assert!(seq & 1 == 0 && seq != 0);
        self.seq.store(seq.wrapping_sub(1), Ordering::Relaxed);
        // Order the odd `seq` before the field stores.
        fence(Ordering::Release);
        let (fp, kh) = (cell.rule_fingerprint, cell.key_hash);
        self.rule_fingerprint_lo.store(fp as u64, Ordering::Relaxed);
        self.rule_fingerprint_hi
            .store((fp >> 64) as u64, Ordering::Relaxed);
        self.key_hash_lo.store(kh as u64, Ordering::Relaxed);
        self.key_hash_hi.store((kh >> 64) as u64, Ordering::Relaxed);
        self.bucket.store(cell.bucket as u64, Ordering::Relaxed);
        self.count.store(cell.count, Ordering::Relaxed);
        self.seq.store(seq, Ordering::Release);
    }
}

enum Read {
    Empty,
    /// Mid-write, or changed while being read.
    Busy,
    Live(AggregateCell),
}

/// Where a lookup for one cell ended.
enum Probe {
    Found(usize, AggregateCell),
    /// Absent; this empty slot is where it would go.
    Vacant(usize),
    /// Absent, with no empty slot within `MAX_PROBE` of its home.
    Full,
}

/// The table over a borrowed run of slots. `&self` is enough for every
/// operation: readers go through seqlocks, and the single writer's per-slot
/// updates are serialized by the write sequence.
#[derive(Clone, Copy, Debug)]
pub struct AggregateTable<'a> {
    slots: &'a [AggregateSlot],
}

impl<'a> AggregateTable<'a> {
    /// `slots.len()` is a power of two, at least 2.
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

    /// The stored count for `(rule_fingerprint, key_hash, bucket)`, or
    /// `None` when it is absent (or being written or moved right now).
    pub fn get(&self, fp: u128, kh: u128, bucket: BucketEpoch) -> Option<AggregateCell> {
        match self.probe(fp, kh, bucket) {
            Probe::Found(_, cell) => Some(cell),
            Probe::Vacant(_) | Probe::Full => None,
        }
    }

    /// Walk at most `MAX_PROBE` slots from the cell's home, stopping at the
    /// cell or at the first empty slot.
    fn probe(&self, fp: u128, kh: u128, bucket: BucketEpoch) -> Probe {
        let home = mix_index(fp, kh, bucket) & self.mask();
        for distance in 0..MAX_PROBE {
            let index = (home + distance) & self.mask();
            match self.slots[index].read() {
                Read::Empty => return Probe::Vacant(index),
                Read::Live(cell)
                    if cell.rule_fingerprint == fp
                        && cell.key_hash == kh
                        && cell.bucket == bucket =>
                {
                    return Probe::Found(index, cell);
                }
                Read::Live(_) | Read::Busy => {}
            }
        }
        Probe::Full
    }

    /// The count of `key_hash` under `spec` over the rule's live buckets,
    /// the current one and the `live_buckets - 1` before it. Epochs wrap,
    /// so the walk does too (a window spans fewer than 2^31 buckets).
    pub fn window_total(&self, spec: RuleSpec, key_hash: KeyHash, now_millis: u64) -> u64 {
        let current = spec.bucket_at(now_millis);
        (0..spec.live_buckets.get())
            .filter_map(|offset| {
                self.get(spec.fingerprint, key_hash.0, current.wrapping_sub(offset))
            })
            .fold(0, |total, cell| total.saturating_add(cell.count))
    }

    /// Wall-clock ms until a request of weight `hits` for `(spec, key_hash)`
    /// would be admitted under the sliding-window model, through
    /// [`crate::window::time_until_admit_millis`]; typically one read.
    pub fn time_until_admit_millis(
        &self,
        spec: RuleSpec,
        key_hash: KeyHash,
        now_millis: u64,
        total: u64,
        hits: u64,
    ) -> u64 {
        crate::window::time_until_admit_millis(
            now_millis,
            spec.bucket_millis,
            spec.live_buckets,
            spec.limit,
            total,
            hits,
            |bucket| {
                self.get(spec.fingerprint, key_hash.0, bucket)
                    .map_or(0, |cell| cell.count)
            },
        )
    }

    /// Slots that hold a cell; a scan of the whole table, for the admin
    /// snapshot only.
    pub fn occupied(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| slot.seq.load(Ordering::Relaxed) != 0)
            .count()
    }
}

/// The table's single writer, the gossip runtime (through
/// [`crate::gossip::AggregateStore::apply`]): the only handle that changes
/// slots. `write_seq` stamps each write, and `dropped` counts the writes that
/// found no free slot near their home, which log `aggregate_full`.
pub struct AggregateWriter<'a> {
    table: AggregateTable<'a>,
    write_seq: &'a std_atomic::AtomicU64,
    dropped: &'a std_atomic::AtomicU64,
}

impl<'a> AggregateWriter<'a> {
    pub fn new(
        table: AggregateTable<'a>,
        write_seq: &'a std_atomic::AtomicU64,
        dropped: &'a std_atomic::AtomicU64,
    ) -> Self {
        Self {
            table,
            write_seq,
            dropped,
        }
    }

    /// Empty every slot, returning how many held a cell. Slots already empty
    /// are left untouched, which spares every reader's cached copy of them.
    pub fn clear(&self) -> usize {
        let mut cleared = 0;
        for slot in self.table.slots {
            if slot.seq.load(Ordering::Relaxed) != 0 {
                slot.seq.store(0, Ordering::Release);
                cleared += 1;
            }
        }
        cleared
    }

    /// Fold one batch of the gossip runtime's rows in.
    pub fn apply<C: Count>(&self, deltas: &DeltaSink<C>, expirations: &ExpirationSink<C>) {
        for i in 0..deltas.len() {
            let delta: u64 = deltas.deltas[i].into();
            if deltas.applies_locally[i] == 0 || delta == 0 {
                continue;
            }
            let key = &deltas.keys[i];
            if !self.add(key.rule_fingerprint, key.key_hash.0, key.bucket, delta) {
                note_aggregate_full(self.table.capacity(), self.dropped);
            }
        }
        for i in 0..expirations.len() {
            if expirations.applies_locally[i] == 0 {
                continue;
            }
            let key = &expirations.keys[i];
            self.subtract(
                key.rule_fingerprint,
                key.key_hash.0,
                key.bucket,
                expirations.last_counts[i].into(),
            );
        }
    }

    fn write(&self, index: usize, cell: AggregateCell) {
        // Even sequences only; `AggregateSlot::write` publishes `seq - 1`
        // (odd) while the fields change.
        let seq = self.write_seq.fetch_add(2, std_atomic::Ordering::Relaxed);
        self.table.slots[index].write(seq, cell);
    }

    /// Add `delta` to the cell; false when it has no slot near its home.
    fn add(&self, fp: u128, kh: u128, bucket: BucketEpoch, delta: u64) -> bool {
        let (index, count) = match self.table.probe(fp, kh, bucket) {
            Probe::Found(index, cell) => (index, cell.count.saturating_add(delta)),
            Probe::Vacant(index) => (index, delta),
            Probe::Full => return false,
        };
        let cell = AggregateCell {
            rule_fingerprint: fp,
            key_hash: kh,
            bucket,
            count,
        };
        self.write(index, cell);
        true
    }

    /// Take an expired origin's `last_count` off the cell, freeing its slot
    /// at zero.
    fn subtract(&self, fp: u128, kh: u128, bucket: BucketEpoch, last_count: u64) {
        let Probe::Found(index, mut cell) = self.table.probe(fp, kh, bucket) else {
            return;
        };
        cell.count = cell.count.saturating_sub(last_count);
        if cell.count == 0 {
            self.remove(index);
        } else {
            self.write(index, cell);
        }
    }

    /// Free slot `hole`, then pull each later cell of its run that may sit
    /// there (its home is at or before the hole) back into it, until an
    /// empty slot or `MAX_PROBE` slots without such a cell. No cell further
    /// on can have its home at or before the hole, so the last hole becomes
    /// empty. A moved cell is copied before its old slot is reused, so a
    /// reader finds it in one place or the other unless the reader passed
    /// the new slot before the copy landed.
    fn remove(&self, mut hole: usize) {
        let mask = self.table.mask();
        let mut gap = 0;
        while gap < MAX_PROBE {
            gap += 1;
            let index = (hole + gap) & mask;
            match self.table.slots[index].read() {
                Read::Empty => break,
                Read::Live(cell) => {
                    let home = mix_index(cell.rule_fingerprint, cell.key_hash, cell.bucket);
                    if index.wrapping_sub(home) & mask >= gap {
                        self.write(hole, cell);
                        hole = index;
                        gap = 0;
                    }
                }
                // Only a former writer still writing after it lost the right
                // to (NGINX's lease) leaves a slot busy; it stays put.
                Read::Busy => {}
            }
        }
        self.table.slots[hole].seq.store(0, Ordering::Release);
    }
}

/// The table had no free slot near a cell's home, so its count was not
/// recorded.
fn note_aggregate_full(capacity: usize, dropped: &std_atomic::AtomicU64) {
    if let Some(dropped_total) = throttle::bump_shared(dropped) {
        tracing::warn!(
            code = "aggregate_full",
            capacity,
            dropped_total,
            config_key = Setting::StorageMaxCells.yaml_key(),
            "The count table had no free slot for a rate-limit key, so that key's hits were not \
             counted: its requests were allowed and it is now under-counted. The table needs a \
             slot per rule × active key × live bucket (window / bucket + 1), and it is too small \
             for the current traffic. Raise `storage.max_cells` (`GABION_STORAGE_MAX_CELLS`) in \
             gabiond, or SIZE in `gabion_limit_zone zone=NAME:SIZE` in NGINX; the table works \
             best under half full.",
        );
    }
}

/// Mix `(rule_fingerprint, key_hash, bucket)` into a uniform table index.
fn mix_index(fp: u128, kh: u128, bucket: BucketEpoch) -> usize {
    use twox_hash::xxhash3_128;
    let mut buf = [0_u8; 16 + 16 + 4];
    buf[..16].copy_from_slice(&fp.to_le_bytes());
    buf[16..32].copy_from_slice(&kh.to_le_bytes());
    buf[32..].copy_from_slice(&bucket.to_le_bytes());
    let h = xxhash3_128::Hasher::oneshot(&buf);
    (h as u64 ^ (h >> 64) as u64) as usize
}

/// `gabiond`'s table, allocated once with its write sequence and drop
/// counter. It holds a cell per live `(rule, key, bucket)`, at most one per
/// cell-store cell, so it is sized as the cell store's index is: twice
/// `storage.max_cells`, rounded up to a power of two, which keeps it at most
/// half full.
pub struct Aggregate {
    slots: Box<[AggregateSlot]>,
    write_seq: std_atomic::AtomicU64,
    dropped: std_atomic::AtomicU64,
}

impl Aggregate {
    pub fn new(storage: &ValidStorageConfig) -> Self {
        Self::with_capacity(pow2_index_capacity_for(storage.cell_capacity) as usize)
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: (0..capacity).map(|_| AggregateSlot::empty()).collect(),
            write_seq: std_atomic::AtomicU64::new(2),
            dropped: std_atomic::AtomicU64::new(0),
        }
    }

    pub fn table(&self) -> AggregateTable<'_> {
        AggregateTable::from_slots(&self.slots)
    }
}

impl<C: Count> AggregateStore<C> for Aggregate {
    fn apply(&self, deltas: &DeltaSink<C>, expirations: &ExpirationSink<C>) {
        AggregateWriter::new(self.table(), &self.write_seq, &self.dropped)
            .apply(deltas, expirations);
    }
}

#[cfg(test)]
mod tests;
