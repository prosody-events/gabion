//! Bounded dirty-change ring used by gossip lanes.

use super::CellHandle;
use crate::throttle;

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

    pub fn overflowed(&self) -> bool {
        self.overflow_seq > 0
    }

    /// Returns the overflow count when it reaches a power of two, for the
    /// caller to log.
    pub fn push(&mut self, entry: DirtyEntry) -> Option<u64> {
        let cap = self.entries.len() as u32;
        if cap == 0 {
            return throttle::bump(&mut self.overflow_seq);
        }
        let overflow = if self.len == cap {
            throttle::bump(&mut self.overflow_seq)
        } else {
            self.len += 1;
            None
        };
        self.entries[self.head as usize] = entry;
        self.head = (self.head + 1) % cap;
        overflow
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
