//! Single-writer multi-reader hash table of aggregate counts, laid out in
//! shared memory.
//!
//! Keys on the portable cell identity `(rule_fingerprint, key_hash, bucket)`.
//! The gossip leader is the only writer (via `AggregateStore::apply`); every
//! worker process reads from this table on the request hot path.
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

use std::sync::atomic::{AtomicU64, Ordering, fence};

use gabion::crdt::{DeltaSink, ExpirationSink};
use gabion::gossip::AggregateStore;
use gabion::rules::RuleSpec;

use super::{AggregateCell, ShmRegion};

/// The furthest a cell may sit from its home slot, and so the most slots a
/// lookup reads. At half load, under one insert in 10^10 needs more.
pub const MAX_PROBE: usize = 128;

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
            bucket: self.bucket.load(Ordering::Relaxed) as u32,
            count: self.count.load(Ordering::Relaxed),
            last_update_millis: self.last_update_millis.load(Ordering::Relaxed),
        };
        // Order the field loads before the re-check of `seq`.
        fence(Ordering::Acquire);
        if self.seq.load(Ordering::Relaxed) == seq {
            Read::Live(cell)
        } else {
            Read::Busy
        }
    }

    /// Publish `cell` under the even sequence `seq`.
    fn write(&self, seq: u64, cell: AggregateCell) {
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
        self.last_update_millis
            .store(cell.last_update_millis, Ordering::Relaxed);
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

    /// The stored count for `(rule_fingerprint, key_hash, bucket)`, or
    /// `None` when it is absent (or being written or moved right now).
    pub fn get(&self, fp: u128, kh: u128, bucket: u32) -> Option<AggregateCell> {
        match self.probe(fp, kh, bucket) {
            Probe::Found(_, cell) => Some(cell),
            Probe::Vacant(_) | Probe::Full => None,
        }
    }

    /// Walk at most `MAX_PROBE` slots from the cell's home, stopping at the
    /// cell or at the first empty slot.
    fn probe(&self, fp: u128, kh: u128, bucket: u32) -> Probe {
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
}

/// `AggregateStore<u32>` implementation backed by SHM. Only the gossip
/// leader's thread owns one of these; readers use `AggregateTable`
/// directly.
///
/// Every slot is an atomic, so a second writer could not cause undefined
/// behaviour, only wrong counts; the gossip leader lease keeps the writer
/// single across processes.
pub struct ShmAggregateStore {
    region: ShmRegion,
    /// The lease epoch this leader won; a later one means it lost the lease.
    epoch: u64,
}

impl ShmAggregateStore {
    /// The writer for `region`'s table, which it empties first. Each leader
    /// builds its counts afresh, from its own hits and peers' gossip, so
    /// counts an earlier leader left behind would be added a second time and
    /// never expire. Readers under-count until gossip refills the table.
    pub fn new(region: ShmRegion) -> Self {
        let mut cleared = 0_usize;
        for slot in region.aggregate().slots {
            // Most slots are usually empty; leaving those untouched spares
            // every worker's cached copy of them.
            if slot.seq.load(Ordering::Relaxed) != 0 {
                slot.seq.store(0, Ordering::Release);
                cleared += 1;
            }
        }
        if cleared > 0 {
            tracing::info!(
                cleared,
                "gabion: this worker took over gossip and emptied the previous leader's counts, \
                 so that peers' gossip refills them without counting them twice. Until then, \
                 usually a gossip tick or two, this node under-counts and may admit requests over \
                 their limits. Nothing to do unless that persists; see `gabion_limit_zone` in the \
                 gabion-nginx README.",
            );
        }
        Self {
            region,
            epoch: region.lease().epoch.load(Ordering::Acquire),
        }
    }

    fn write(&self, slot: &AggregateSlot, cell: AggregateCell) {
        // Even sequences only; `AggregateSlot::write` publishes `seq - 1`
        // (odd) while the fields change.
        let seq = self
            .region
            .header()
            .aggregate_write_seq
            .fetch_add(2, Ordering::Relaxed);
        slot.write(seq, cell);
    }

    pub(crate) fn write_delta(&self, fp: u128, kh: u128, bucket: u32, delta: u64, now_millis: u64) {
        let table = self.region.aggregate();
        let (index, count) = match table.probe(fp, kh, bucket) {
            Probe::Found(index, cell) => (index, cell.count.saturating_add(delta)),
            Probe::Vacant(index) => (index, delta),
            Probe::Full => return self.note_dropped_write(),
        };
        let cell = AggregateCell {
            rule_fingerprint: fp,
            key_hash: kh,
            bucket,
            count,
            last_update_millis: now_millis,
        };
        self.write(&table.slots[index], cell);
    }

    fn write_expiration(&self, fp: u128, kh: u128, bucket: u32, last_count: u64) {
        let table = self.region.aggregate();
        let Probe::Found(index, mut cell) = table.probe(fp, kh, bucket) else {
            return;
        };
        cell.count = cell.count.saturating_sub(last_count);
        if cell.count == 0 {
            self.remove(table, index);
        } else {
            self.write(&table.slots[index], cell);
        }
    }

    /// Free slot `hole`, then pull each later cell of its run that may sit
    /// there (its home is at or before the hole) back into it, until an
    /// empty slot or `MAX_PROBE` slots without such a cell. No cell further
    /// on can have its home at or before the hole, so the last hole becomes
    /// empty. A moved cell is copied before its old slot is reused, so a
    /// reader finds it in one place or the other unless the reader passed
    /// the new slot before the copy landed.
    fn remove(&self, table: AggregateTable<'_>, mut hole: usize) {
        let mut gap = 0;
        while gap < MAX_PROBE {
            gap += 1;
            let index = (hole + gap) & table.mask();
            match table.slots[index].read() {
                Read::Empty => break,
                Read::Live(cell) => {
                    let home = mix_index(cell.rule_fingerprint, cell.key_hash, cell.bucket);
                    if index.wrapping_sub(home) & table.mask() >= gap {
                        self.write(&table.slots[hole], cell);
                        hole = index;
                        gap = 0;
                    }
                }
                // Only a former leader still writing after it lost the lease
                // leaves a slot busy for the writer; it stays put.
                Read::Busy => {}
            }
        }
        table.slots[hole].seq.store(0, Ordering::Release);
    }

    /// The table had no free slot near this cell's home, so its count was
    /// not recorded.
    fn note_dropped_write(&self) {
        let n = self.region.stats().record_aggregate_drop();
        if n.is_power_of_two() {
            tracing::warn!(
                capacity = self.region.layout().aggregate_capacity,
                dropped_total = n,
                "gabion: the shared-memory count table had no free slot for a rate-limit key, so \
                 that key's hits were not counted: its requests were allowed and it is now \
                 under-counted. The table needs a slot per rule × active key × live bucket \
                 (window / bucket + 1), and this zone is too small for the current traffic. Raise \
                 SIZE in `gabion_limit_zone zone=NAME:SIZE`: each doubling of SIZE doubles the \
                 slots (64m holds 524288), and the table works best under half full.",
            );
        }
    }
}

impl AggregateStore<u32> for ShmAggregateStore {
    fn apply(&self, deltas: &DeltaSink<u32>, expirations: &ExpirationSink<u32>) {
        // A leader that stalled past its lease may still be applying gossip
        // after its successor emptied the table, which would count it twice.
        if self.region.lease().epoch.load(Ordering::Acquire) != self.epoch {
            return;
        }
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
