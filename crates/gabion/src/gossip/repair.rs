//! Digest repair: working state for the second phase of the protocol.
//!
//! [`DigestBuilder`] summarises which origin sequences the local store holds;
//! [`DigestReader`] compares a peer's digest packet against the store and
//! names the cells the peer lacks. Both own their scratch, sized once from
//! the store's node dictionary.

use crate::crdt::{CellHandle, CellStore, Count, NodeSlot};
use crate::wire::{DigestEntry, DigestScope, OriginKey};

const NO_ENTRY: u16 = u16::MAX;

fn origin_key<C: Count>(store: &CellStore<C>, slot: NodeSlot) -> Option<OriginKey> {
    store
        .node_dictionary()
        .descriptor(slot)
        .map(|node| OriginKey {
            node_id: node.node_id,
            incarnation: node.incarnation,
        })
}

/// Summarises the local store as one [`DigestEntry`] per origin.
pub(super) struct DigestBuilder {
    entries: Vec<DigestEntry>,
    entry_of_slot: Box<[u16]>,
}

impl DigestBuilder {
    pub(super) fn for_store<C: Count>(store: &CellStore<C>) -> Self {
        let nodes = store.node_dictionary().capacity() as usize;
        Self {
            entries: Vec::with_capacity(nodes),
            entry_of_slot: vec![NO_ENTRY; nodes].into_boxed_slice(),
        }
    }

    /// Rebuild the digest from `store`. The result is sorted by origin.
    pub(super) fn build<C: Count>(&mut self, store: &CellStore<C>) -> &[DigestEntry] {
        self.entries.clear();
        self.entry_of_slot.fill(NO_ENTRY);

        store.visit_active(|_, slot, sequence| {
            let index = match self.entry_of_slot[slot as usize] {
                NO_ENTRY => {
                    let Some(origin) = origin_key(store, slot) else {
                        return true;
                    };
                    self.entry_of_slot[slot as usize] = self.entries.len() as u16;
                    self.entries.push(DigestEntry::new(origin, 0));
                    self.entries.len() - 1
                }
                index => index as usize,
            };
            let entry = &mut self.entries[index];
            entry.max_sequence = entry.max_sequence.max(sequence);
            true
        });
        store.visit_active(|_, slot, sequence| {
            if let Some(entry) = self
                .entries
                .get_mut(self.entry_of_slot[slot as usize] as usize)
            {
                entry.mark(sequence);
            }
            true
        });

        self.entries.sort_unstable_by_key(|entry| entry.origin);
        &self.entries
    }
}

/// How a received digest packet treats one local origin.
#[derive(Clone, Copy)]
enum Verdict {
    /// The packet does not speak for this origin.
    Unspoken,
    /// The packet speaks for it and lists nothing: the sender holds none.
    Absent,
    /// The packet lists it at this entry index.
    Listed(u16),
}

/// Compares a peer's digest packet with the local store.
pub(super) struct DigestReader {
    scope: DigestScope,
    entries: Vec<DigestEntry>,
    verdicts: Box<[Verdict]>,
}

impl DigestReader {
    pub(super) fn for_store<C: Count>(store: &CellStore<C>) -> Self {
        let nodes = store.node_dictionary().capacity() as usize;
        Self {
            scope: DigestScope {
                from: OriginKey::default(),
                to: OriginKey::default(),
            },
            entries: Vec::with_capacity(nodes),
            verdicts: vec![Verdict::Unspoken; nodes].into_boxed_slice(),
        }
    }

    /// Forget the previous packet, ready for the entries of the next.
    pub(super) fn begin(&mut self) {
        self.entries.clear();
    }

    /// Set the origins the packet whose entries were just pushed speaks for.
    pub(super) fn set_scope(&mut self, scope: DigestScope) {
        self.scope = scope;
    }

    /// Take one decoded entry. Entries past the node dictionary's capacity
    /// are dropped: the origins they name are then treated as unlisted, which
    /// at worst sends cells the peer already holds.
    pub(super) fn push(&mut self, entry: DigestEntry) {
        if self.entries.len() < self.entries.capacity() {
            self.entries.push(entry);
        }
    }

    /// Collect up to `max_cells` cells the sender of the packet lacks.
    pub(super) fn cells_lacked<C: Count>(
        &mut self,
        store: &CellStore<C>,
        max_cells: usize,
        out: &mut Vec<CellHandle>,
    ) {
        out.clear();
        if max_cells == 0 {
            return;
        }
        for (slot, verdict) in self.verdicts.iter_mut().enumerate() {
            *verdict = match origin_key(store, slot as NodeSlot) {
                Some(origin) if self.scope.contains(origin) => {
                    match self.entries.binary_search_by_key(&origin, |e| e.origin) {
                        Ok(index) => Verdict::Listed(index as u16),
                        Err(_) => Verdict::Absent,
                    }
                }
                _ => Verdict::Unspoken,
            };
        }
        store.visit_active(|handle, slot, sequence| {
            let lacked = match self.verdicts[slot as usize] {
                Verdict::Unspoken => false,
                Verdict::Absent => true,
                Verdict::Listed(index) => !self.entries[index as usize].holds(sequence),
            };
            if lacked {
                out.push(handle);
            }
            out.len() < max_cells
        });
    }
}

#[cfg(test)]
mod tests;
