use crate::crdt::{KeyHash, RuleSlot};

#[cfg(test)]
mod tests;

/// Local hits per `(rule, key)` that no gossip reply has carried yet: the
/// quantity the threshold flush holds under the error budget ε, which is a
/// fraction of the rule's per-key limit.
///
/// Direct-mapped and sized once at construction, so recording a hit is one
/// index and one add. Two keys that share a slot share its counter, and the
/// shared count is their sum: a collision can only make a key cross ε
/// *earlier* (an extra threshold flush), never later. Clearing a shared slot
/// when only one of its keys went out under-counts the other until its next
/// hit; that key's cell is still above every peer's watermark, so the next
/// exchange carries it anyway.
///
/// A hit whose cell cannot be stored (cell store or rule dictionary full)
/// has no cell for a reply to carry, so its count stays until a colliding key
/// clears the slot. Such a key keeps crossing ε, and `min_emit_interval`
/// bounds how often that fires.
pub(super) struct PendingHits {
    slots: Box<[u32]>,
    // Number of non-zero slots, so the post-emit clear can stop as soon as
    // nothing is pending instead of resolving every cell in the reply.
    live: usize,
}

impl PendingHits {
    /// `capacity` is rounded up to a power of two for mask indexing.
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            slots: vec![0; capacity.next_power_of_two()].into_boxed_slice(),
            live: 0,
        }
    }

    /// Add `hits` to the key's count and return the new count.
    pub(super) fn add(&mut self, rule: RuleSlot, key: KeyHash, hits: u64) -> u64 {
        let i = self.index(rule, key);
        let before = self.slots[i];
        let after = before.saturating_add(u32::try_from(hits).unwrap_or(u32::MAX));
        self.live += usize::from(before == 0 && after != 0);
        self.slots[i] = after;
        u64::from(after)
    }

    /// Zero the key's count. Returns whether it had pending hits.
    pub(super) fn clear(&mut self, rule: RuleSlot, key: KeyHash) -> bool {
        let i = self.index(rule, key);
        let had = std::mem::take(&mut self.slots[i]) != 0;
        self.live -= usize::from(had);
        had
    }

    pub(super) fn is_empty(&self) -> bool {
        self.live == 0
    }

    fn index(&self, rule: RuleSlot, key: KeyHash) -> usize {
        // `KeyHash` is already a uniform 128-bit hash of the key bytes: fold
        // it rather than hash it again, and spread the rule slot across the
        // word so one key under two rules lands in different slots.
        let folded = (key.0 as u64)
            ^ ((key.0 >> 64) as u64)
            ^ u64::from(rule).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        folded as usize & (self.slots.len() - 1)
    }
}
