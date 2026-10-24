//! Digest repair: the working state behind the heartbeat repair slice.
//!
//! [`DigestBuilder`] summarises which origin sequences the local store holds;
//! [`DigestReader`] compares a peer's digest packet against the store and
//! names the cells the peer lacks. Both own their scratch, sized once from
//! the store's node dictionary.

use std::net::SocketAddr;

use crate::crdt::{BucketEpoch, CellHandle, CellStore, Count, NodeSlot, RuleSlot};
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

    /// Rebuild the digest from `store`, in one pass over its cells, sorted by
    /// origin. Read it with [`Self::last_built`].
    pub(super) fn build<C: Count>(&mut self, store: &CellStore<C>) {
        self.entries.clear();
        self.entry_of_slot.fill(NO_ENTRY);

        store.visit_active(|cell| {
            let index = match self.entry_of_slot[cell.origin as usize] {
                NO_ENTRY => {
                    let Some(origin) = origin_key(store, cell.origin) else {
                        return true;
                    };
                    self.entry_of_slot[cell.origin as usize] = self.entries.len() as u16;
                    self.entries.push(DigestEntry::new(origin, 0));
                    self.entries.len() - 1
                }
                index => index as usize,
            };
            self.entries[index].mark_raising(cell.origin_sequence);
            true
        });

        self.entries.sort_unstable_by_key(|entry| entry.origin);
    }

    /// The digest last built.
    pub(super) fn last_built(&self) -> &[DigestEntry] {
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

/// Who sent the digest being read, and whether it answers this node's own.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DigestSource {
    pub(super) addr: SocketAddr,
    pub(super) reply: bool,
}

/// Compares a peer's digest with the local store. A digest larger than one
/// packet is gathered packet by packet, in whatever order its packets arrive,
/// so the store is scanned once per digest rather than once per packet.
pub(super) struct DigestReader {
    source: Option<DigestSource>,
    /// The ranges of origins the packets gathered so far speak for.
    scopes: Vec<DigestScope>,
    entries: Vec<DigestEntry>,
    verdicts: Box<[Verdict]>,
    /// Per rule slot, the oldest bucket still in the window at the time of
    /// the comparison.
    oldest_live: Box<[BucketEpoch]>,
}

impl DigestReader {
    pub(super) fn for_store<C: Count>(store: &CellStore<C>) -> Self {
        let nodes = store.node_dictionary().capacity() as usize;
        Self {
            source: None,
            // Every packet but an empty digest's lists at least one origin.
            scopes: Vec::with_capacity(nodes + 1),
            entries: Vec::with_capacity(nodes),
            verdicts: vec![Verdict::Unspoken; nodes].into_boxed_slice(),
            oldest_live: vec![0; store.rule_dictionary().capacity() as usize].into_boxed_slice(),
        }
    }

    /// Whether the digest being gathered came from `source`.
    pub(super) fn gathering(&self, source: DigestSource) -> bool {
        self.source == Some(source)
    }

    /// Start gathering a new digest.
    pub(super) fn begin(&mut self, source: DigestSource) {
        self.scopes.clear();
        self.entries.clear();
        self.source = Some(source);
    }

    /// Add one packet: the range of origins it speaks for and its entries.
    /// Entries past the node dictionary's capacity are dropped: the origins
    /// they name are then treated as unlisted, which at worst sends cells the
    /// peer already holds.
    pub(super) fn add_packet(&mut self, scope: DigestScope, entries: &[DigestEntry]) {
        if self.scopes.len() < self.scopes.capacity() {
            self.scopes.push(scope);
        }
        let room = self.entries.capacity() - self.entries.len();
        self.entries
            .extend_from_slice(&entries[..entries.len().min(room)]);
    }

    /// Whether the packets gathered so far speak for every origin, so the
    /// digest is whole.
    pub(super) fn complete(&mut self) -> bool {
        self.scopes.sort_unstable_by_key(|scope| scope.from);
        let mut covered = OriginKey::MIN;
        for scope in &self.scopes {
            if scope.from > covered {
                return false;
            }
            covered = covered.max(scope.to);
        }
        covered == OriginKey::END
    }

    /// End the digest being gathered, returning its sender. Its entries stay
    /// readable until the next `begin`.
    pub(super) fn finish(&mut self) -> Option<DigestSource> {
        self.entries.sort_unstable_by_key(|entry| entry.origin);
        self.source.take()
    }

    /// Whether the digest shows its sender holding a sequence that `ours`,
    /// this node's digest, does not.
    pub(super) fn holds_any_missing_from(&self, ours: &[DigestEntry]) -> bool {
        self.entries.iter().any(|theirs| {
            match ours.binary_search_by_key(&theirs.origin, |e| e.origin) {
                Ok(index) => theirs.holds_any_missing_from(&ours[index]),
                Err(_) => theirs.holds_any_missing_from(&DigestEntry::new(theirs.origin, 0)),
            }
        })
    }

    /// Collect up to `max_cells` cells the sender of the digest lacks. A
    /// cell whose origin changed it less than `settle_millis` before
    /// `now_millis` is left out, since it is most likely still on its way
    /// through the forwarders, and so is one whose bucket has left the
    /// window: the sender expired it before building the digest, and the
    /// store expires it only on its next round.
    pub(super) fn cells_lacked<C: Count>(
        &mut self,
        store: &CellStore<C>,
        max_cells: usize,
        now_millis: u64,
        settle_millis: u64,
        out: &mut Vec<CellHandle>,
    ) {
        out.clear();
        if max_cells == 0 {
            return;
        }
        for (slot, verdict) in self.verdicts.iter_mut().enumerate() {
            *verdict = match origin_key(store, slot as NodeSlot) {
                Some(origin) if self.scopes.iter().any(|scope| scope.contains(origin)) => {
                    match self.entries.binary_search_by_key(&origin, |e| e.origin) {
                        Ok(index) => Verdict::Listed(index as u16),
                        Err(_) => Verdict::Absent,
                    }
                }
                _ => Verdict::Unspoken,
            };
        }
        for (slot, oldest) in self.oldest_live.iter_mut().enumerate() {
            *oldest = store
                .rule_dictionary()
                .descriptor(slot as RuleSlot)
                .map_or(0, |rule| rule.oldest_live_epoch(now_millis));
        }
        store.visit_active(|cell| {
            if cell.last_update_millis.saturating_add(settle_millis) > now_millis
                || cell.bucket < self.oldest_live[cell.rule as usize]
            {
                return true;
            }
            let lacked = match self.verdicts[cell.origin as usize] {
                Verdict::Unspoken => false,
                Verdict::Absent => true,
                Verdict::Listed(index) => !self.entries[index as usize].holds(cell.origin_sequence),
            };
            if lacked {
                out.push(cell.handle);
            }
            out.len() < max_cells
        });
    }
}

#[cfg(test)]
mod tests;
