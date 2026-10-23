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

    /// Ring index of the oldest entry.
    fn start(&self) -> u32 {
        let cap = self.entries.len() as u32;
        (self.head + cap - self.len) % cap.max(1)
    }

    /// Iterate ring entries in insertion order (oldest first).
    pub fn iter(&self) -> impl Iterator<Item = DirtyEntry> + '_ {
        let cap = self.entries.len() as u32;
        let start = self.start();
        (0..self.len).map(move |offset| self.entries[((start + offset) % cap) as usize])
    }

    /// The oldest entry.
    pub fn front(&self) -> Option<DirtyEntry> {
        (self.len > 0).then(|| self.entries[self.start() as usize])
    }

    /// Drop the oldest entry.
    pub fn pop_front(&mut self) {
        self.len = self.len.saturating_sub(1);
    }

    pub fn clear(&mut self) {
        self.head = 0;
        self.len = 0;
        self.overflow_seq = 0;
    }
}
