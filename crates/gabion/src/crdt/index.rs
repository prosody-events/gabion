//! Robin Hood open-addressing index with backshift deletion.

const EMPTY_INDEX_SLOT: u32 = u32::MAX;

/// Hash index mapping a hash + caller-provided key equality to a slot id.
///
/// Stores `(hash, slot_index)` pairs in two parallel columns. The capacity is
/// always a power of two so the probe step is `(i + 1) & mask`. On insert we
/// use Robin Hood swapping to keep the variance of probe distance bounded;
/// on delete we backshift instead of leaving tombstones, so no rebuild pass
/// is ever needed.
#[derive(Clone, Debug)]
pub struct CellIndex {
    slot_indexes: Box<[u32]>,
    hashes: Box<[u64]>,
    len: u32,
    capacity: u32,
    mask: u32,
}

impl CellIndex {
    /// Construct an index with `capacity` buckets. `capacity` must be a power
    /// of two greater than zero.
    pub fn with_capacity(capacity: u32) -> Self {
        assert!(capacity > 0, "CellIndex capacity must be > 0");
        assert!(
            capacity.is_power_of_two(),
            "CellIndex capacity must be power of two"
        );
        Self {
            slot_indexes: vec![EMPTY_INDEX_SLOT; capacity as usize].into_boxed_slice(),
            hashes: vec![0_u64; capacity as usize].into_boxed_slice(),
            len: 0,
            capacity,
            mask: capacity - 1,
        }
    }

    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    fn probe_distance(&self, slot: u32, hash: u64) -> u32 {
        let ideal = (hash as u32) & self.mask;
        slot.wrapping_sub(ideal) & self.mask
    }

    /// Look up the slot for an entry whose stored hash matches `hash` and
    /// for which `eq(slot_index)` returns true.
    pub fn find<F: FnMut(u32) -> bool>(&self, hash: u64, mut eq: F) -> Option<u32> {
        if self.len == 0 {
            return None;
        }
        let mut slot = (hash as u32) & self.mask;
        let mut dist: u32 = 0;
        loop {
            let stored_slot = self.slot_indexes[slot as usize];
            if stored_slot == EMPTY_INDEX_SLOT {
                return None;
            }
            let stored_hash = self.hashes[slot as usize];
            let stored_dist = self.probe_distance(slot, stored_hash);
            if dist > stored_dist {
                return None;
            }
            if stored_hash == hash && eq(stored_slot) {
                return Some(stored_slot);
            }
            slot = (slot + 1) & self.mask;
            dist += 1;
            if dist == self.capacity {
                return None;
            }
        }
    }

    /// Insert `(hash, slot_index)`. Caller must have already verified that
    /// the entry is not already present.
    pub fn insert_unchecked(&mut self, hash: u64, slot_index: u32) {
        assert!(slot_index != EMPTY_INDEX_SLOT, "slot index reserved");
        assert!(self.len < self.capacity, "CellIndex full");
        let mut slot = (hash as u32) & self.mask;
        let mut cur_hash = hash;
        let mut cur_slot_idx = slot_index;
        let mut cur_dist: u32 = 0;
        loop {
            let existing = self.slot_indexes[slot as usize];
            if existing == EMPTY_INDEX_SLOT {
                self.slot_indexes[slot as usize] = cur_slot_idx;
                self.hashes[slot as usize] = cur_hash;
                self.len += 1;
                return;
            }
            let existing_hash = self.hashes[slot as usize];
            let existing_dist = self.probe_distance(slot, existing_hash);
            if cur_dist > existing_dist {
                // Steal: place ours here, continue moving the displaced entry.
                std::mem::swap(&mut self.hashes[slot as usize], &mut cur_hash);
                std::mem::swap(&mut self.slot_indexes[slot as usize], &mut cur_slot_idx);
                cur_dist = existing_dist;
            }
            slot = (slot + 1) & self.mask;
            cur_dist += 1;
            debug_assert!(cur_dist < self.capacity, "CellIndex insert wrapped");
        }
    }

    /// Remove the entry mapping `hash` to `slot_index`. No-op if not present.
    pub fn remove(&mut self, hash: u64, slot_index: u32) {
        if self.len == 0 {
            return;
        }
        let mut slot = (hash as u32) & self.mask;
        let mut dist: u32 = 0;
        loop {
            let stored_slot = self.slot_indexes[slot as usize];
            if stored_slot == EMPTY_INDEX_SLOT {
                return;
            }
            let stored_hash = self.hashes[slot as usize];
            let stored_dist = self.probe_distance(slot, stored_hash);
            if dist > stored_dist {
                return;
            }
            if stored_hash == hash && stored_slot == slot_index {
                self.backshift(slot);
                self.len -= 1;
                return;
            }
            slot = (slot + 1) & self.mask;
            dist += 1;
            if dist == self.capacity {
                return;
            }
        }
    }

    fn backshift(&mut self, mut slot: u32) {
        loop {
            let next = (slot + 1) & self.mask;
            let next_slot_idx = self.slot_indexes[next as usize];
            if next_slot_idx == EMPTY_INDEX_SLOT {
                self.slot_indexes[slot as usize] = EMPTY_INDEX_SLOT;
                self.hashes[slot as usize] = 0;
                return;
            }
            let next_hash = self.hashes[next as usize];
            let next_dist = self.probe_distance(next, next_hash);
            if next_dist == 0 {
                self.slot_indexes[slot as usize] = EMPTY_INDEX_SLOT;
                self.hashes[slot as usize] = 0;
                return;
            }
            self.slot_indexes[slot as usize] = next_slot_idx;
            self.hashes[slot as usize] = next_hash;
            slot = next;
        }
    }

    pub fn clear(&mut self) {
        self.slot_indexes
            .iter_mut()
            .for_each(|s| *s = EMPTY_INDEX_SLOT);
        self.hashes.iter_mut().for_each(|h| *h = 0);
        self.len = 0;
    }

    /// Maximum probe distance currently in use. O(capacity).
    pub fn max_probe_distance(&self) -> u32 {
        let mut max = 0_u32;
        for slot in 0..self.capacity {
            let stored = self.slot_indexes[slot as usize];
            if stored == EMPTY_INDEX_SLOT {
                continue;
            }
            let d = self.probe_distance(slot, self.hashes[slot as usize]);
            if d > max {
                max = d;
            }
        }
        max
    }
}
