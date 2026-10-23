//! Bounded dirty-change ring used by gossip lanes.

use super::CellHandle;

/// One entry in a [`DirtyRing`]. The `origin_sequence` is validated against
/// the cell's current `origin_sequences[slot]` on consumption — a later
/// update on the same cell raises the per-origin sequence and invalidates
/// the older ring entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirtyEntry {
    pub handle: CellHandle,
    pub origin_sequence: u64,
}

/// Bounded ring of recent change records, pushed in ascending
/// `origin_sequence` order. Overflow bumps `overflow_seq` and raises
/// `evicted_through` to the highest sequence the ring no longer holds, so a
/// reader can tell whether the ring still contains every change newer than a
/// sequence it has already seen.
#[derive(Clone, Debug)]
pub struct DirtyRing {
    entries: Box<[DirtyEntry]>,
    head: u32,
    len: u32,
    overflow_seq: u64,
    evicted_through: u64,
}

impl DirtyRing {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: vec![
                DirtyEntry {
                    handle: CellHandle::default(),
                    origin_sequence: 0,
                };
                capacity
            ]
            .into_boxed_slice(),
            head: 0,
            len: 0,
            overflow_seq: 0,
            evicted_through: 0,
        }
    }

    pub fn capacity(&self) -> u32 {
        self.entries.len() as u32
    }

    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn overflow_seq(&self) -> u64 {
        self.overflow_seq
    }

    /// Highest `origin_sequence` of any entry the ring has overwritten or
    /// cleared. The ring holds every change newer than this.
    pub fn evicted_through(&self) -> u64 {
        self.evicted_through
    }

    pub fn overflowed(&self) -> bool {
        self.overflow_seq > 0
    }

    pub fn push(&mut self, entry: DirtyEntry) {
        let cap = self.entries.len() as u32;
        if cap == 0 {
            self.overflow_seq = self.overflow_seq.saturating_add(1);
            self.evicted_through = self.evicted_through.max(entry.origin_sequence);
            return;
        }
        if self.len == cap {
            self.overflow_seq = self.overflow_seq.saturating_add(1);
            let oldest = self.entries[self.head as usize];
            self.evicted_through = self.evicted_through.max(oldest.origin_sequence);
        } else {
            self.len += 1;
        }
        self.entries[self.head as usize] = entry;
        self.head = (self.head + 1) % cap;
    }

    /// Iterate ring entries in insertion order (oldest first).
    pub fn iter(&self) -> impl Iterator<Item = DirtyEntry> + '_ {
        let cap = self.entries.len() as u32;
        let len = self.len;
        let head = self.head;
        let start = if len == cap { head } else { 0 };
        (0..len).map(move |offset| self.entries[((start + offset) % cap) as usize])
    }

    pub fn clear(&mut self) {
        if let Some(newest) = self.iter().last() {
            self.evicted_through = self.evicted_through.max(newest.origin_sequence);
        }
        self.head = 0;
        self.len = 0;
        self.overflow_seq = 0;
    }
}

/// Read a single ring entry by insertion-order offset. Function-local borrow
/// of the ring releases before the caller's mutating method calls, keeping
/// the hot send path allocation-free.
#[inline]
pub(super) fn ring_entry_at(ring: &DirtyRing, offset: u32) -> DirtyEntry {
    let cap = ring.entries.len() as u32;
    let start = if ring.len == cap { ring.head } else { 0 };
    ring.entries[((start + offset) % cap) as usize]
}
