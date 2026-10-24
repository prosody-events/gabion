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

/// Bounded ring of recent change records. Overflow bumps `overflow_seq` so
/// observers can detect that the recent-change stream is no longer
/// authoritative.
#[derive(Clone, Debug)]
pub struct DirtyRing {
    entries: Box<[DirtyEntry]>,
    head: u32,
    len: u32,
    overflow_seq: u64,
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

    pub fn overflowed(&self) -> bool {
        self.overflow_seq > 0
    }

    pub fn push(&mut self, entry: DirtyEntry) {
        let cap = self.entries.len() as u32;
        if cap == 0 {
            self.overflow_seq = self.overflow_seq.saturating_add(1);
            return;
        }
        if self.len == cap {
            self.overflow_seq = self.overflow_seq.saturating_add(1);
        } else {
            self.len += 1;
        }
        self.entries[self.head as usize] = entry;
        self.head = (self.head + 1) % cap;
    }

    /// Drop the `n` oldest entries; a gossip frame has taken them.
    pub fn consume(&mut self, n: u32) {
        debug_assert!(n <= self.len);
        self.len -= n.min(self.len);
    }

    /// Index of the oldest entry. Only meaningful while `len > 0`.
    fn start(&self) -> u32 {
        let cap = self.entries.len() as u32;
        (self.head + cap - self.len) % cap
    }

    /// Iterate ring entries in insertion order (oldest first).
    pub fn iter(&self) -> impl Iterator<Item = DirtyEntry> + '_ {
        (0..self.len).map(move |offset| ring_entry_at(self, offset))
    }

    pub fn clear(&mut self) {
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
    debug_assert!(offset < ring.len);
    let cap = ring.entries.len() as u32;
    ring.entries[((ring.start() + offset) % cap) as usize]
}
