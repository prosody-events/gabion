//! Rule and node identity dictionaries.

use super::hash::{hash_fingerprint, hash_node_identity};
use super::index::CellIndex;
use super::{Incarnation, NodeId, NodeSlot, RuleSlot};

const EMPTY_DICT_SLOT: u16 = u16::MAX;

/// Descriptor stored per interned rule slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuleDescriptor {
    /// Canonical hash of the rule shape and parameters.
    pub fingerprint: u128,
    pub window_millis: u32,
    pub bucket_millis: u32,
    pub limit: u64,
    pub flags: u32,
    /// `u32::MAX` means the rule is known on the wire only — cells are stored,
    /// dirty-tracked, and expired the same way, but [`super::CellDelta::applies_locally`]
    /// is `false` so the local rate-limit aggregator ignores them.
    pub local_rule_id: u32,
}

impl Default for RuleDescriptor {
    fn default() -> Self {
        Self {
            fingerprint: 0,
            window_millis: 0,
            bucket_millis: 0,
            limit: 0,
            flags: 0,
            local_rule_id: u32::MAX,
        }
    }
}

impl RuleDescriptor {
    pub fn applies_locally(&self) -> bool {
        self.local_rule_id != u32::MAX
    }
}

/// Interns rule fingerprints to a small [`RuleSlot`]. Bounded; rejects on full.
#[derive(Clone, Debug)]
pub struct RuleDictionary {
    descriptors: Box<[RuleDescriptor]>,
    refcounts: Box<[u32]>,
    index: CellIndex,
    free_next: Box<[u16]>,
    free_head: u16,
    len: u16,
    capacity: u16,
}

impl RuleDictionary {
    pub fn with_capacity(capacity: u16) -> Self {
        assert!(capacity > 0, "RuleDictionary capacity must be > 0");
        assert!(
            capacity < EMPTY_DICT_SLOT,
            "RuleDictionary capacity must be < u16::MAX"
        );
        let index_cap = pow2_index_capacity_for(capacity as u32);
        let mut free_next = vec![EMPTY_DICT_SLOT; capacity as usize].into_boxed_slice();
        for i in 0..capacity {
            free_next[i as usize] = if i + 1 < capacity {
                i + 1
            } else {
                EMPTY_DICT_SLOT
            };
        }
        Self {
            descriptors: vec![RuleDescriptor::default(); capacity as usize].into_boxed_slice(),
            refcounts: vec![0_u32; capacity as usize].into_boxed_slice(),
            index: CellIndex::with_capacity(index_cap),
            free_next,
            free_head: 0,
            len: 0,
            capacity,
        }
    }

    pub fn capacity(&self) -> u16 {
        self.capacity
    }
    pub fn len(&self) -> u16 {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn descriptor(&self, slot: RuleSlot) -> Option<&RuleDescriptor> {
        if (slot as u32) < self.capacity as u32 && self.refcounts[slot as usize] > 0 {
            Some(&self.descriptors[slot as usize])
        } else {
            None
        }
    }
    pub fn refcount(&self, slot: RuleSlot) -> u32 {
        self.refcounts.get(slot as usize).copied().unwrap_or(0)
    }

    /// Look up an existing rule by fingerprint, without inserting.
    pub fn find(&self, fingerprint: u128) -> Option<RuleSlot> {
        let h = hash_fingerprint(fingerprint);
        let descriptors = &self.descriptors;
        self.index
            .find(h, |slot| descriptors[slot as usize].fingerprint == fingerprint)
            .map(|s| s as RuleSlot)
    }

    /// Find or create a rule slot. Refcount is unchanged — callers manage it
    /// through [`super::CellStore`].
    pub(super) fn intern(&mut self, descriptor: RuleDescriptor) -> Option<RuleSlot> {
        if let Some(slot) = self.find(descriptor.fingerprint) {
            // Replace descriptor metadata (e.g. local_rule_id may have changed).
            self.descriptors[slot as usize] = descriptor;
            return Some(slot);
        }
        if self.free_head == EMPTY_DICT_SLOT {
            return None;
        }
        let slot = self.free_head;
        self.free_head = self.free_next[slot as usize];
        self.descriptors[slot as usize] = descriptor;
        self.refcounts[slot as usize] = 0;
        self.index
            .insert_unchecked(hash_fingerprint(descriptor.fingerprint), slot as u32);
        self.len += 1;
        Some(slot)
    }

    pub(super) fn inc_ref(&mut self, slot: RuleSlot) {
        self.refcounts[slot as usize] = self.refcounts[slot as usize].saturating_add(1);
    }

    pub(super) fn dec_ref(&mut self, slot: RuleSlot) {
        let rc = &mut self.refcounts[slot as usize];
        if *rc == 0 {
            return;
        }
        *rc -= 1;
        if *rc == 0 {
            let fingerprint = self.descriptors[slot as usize].fingerprint;
            self.index.remove(hash_fingerprint(fingerprint), slot as u32);
            self.descriptors[slot as usize] = RuleDescriptor::default();
            self.free_next[slot as usize] = self.free_head;
            self.free_head = slot;
            self.len -= 1;
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeDescriptor {
    pub node_id: NodeId,
    pub incarnation: Incarnation,
}

/// Interns `(NodeId, Incarnation)` pairs to a small [`NodeSlot`]. Each
/// distinct pair gets its own slot — incarnation changes always allocate
/// a fresh slot, so cells from different incarnations cannot alias.
#[derive(Clone, Debug)]
pub struct NodeDictionary {
    descriptors: Box<[NodeDescriptor]>,
    refcounts: Box<[u32]>,
    index: CellIndex,
    free_next: Box<[u16]>,
    free_head: u16,
    len: u16,
    capacity: u16,
}

impl NodeDictionary {
    pub fn with_capacity(capacity: u16) -> Self {
        assert!(capacity > 0, "NodeDictionary capacity must be > 0");
        assert!(
            capacity < EMPTY_DICT_SLOT,
            "NodeDictionary capacity must be < u16::MAX"
        );
        let index_cap = pow2_index_capacity_for(capacity as u32);
        let mut free_next = vec![EMPTY_DICT_SLOT; capacity as usize].into_boxed_slice();
        for i in 0..capacity {
            free_next[i as usize] = if i + 1 < capacity {
                i + 1
            } else {
                EMPTY_DICT_SLOT
            };
        }
        Self {
            descriptors: vec![NodeDescriptor::default(); capacity as usize].into_boxed_slice(),
            refcounts: vec![0_u32; capacity as usize].into_boxed_slice(),
            index: CellIndex::with_capacity(index_cap),
            free_next,
            free_head: 0,
            len: 0,
            capacity,
        }
    }

    pub fn capacity(&self) -> u16 {
        self.capacity
    }
    pub fn len(&self) -> u16 {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn descriptor(&self, slot: NodeSlot) -> Option<&NodeDescriptor> {
        if (slot as u32) < self.capacity as u32 && self.refcounts[slot as usize] > 0 {
            Some(&self.descriptors[slot as usize])
        } else {
            None
        }
    }
    pub fn refcount(&self, slot: NodeSlot) -> u32 {
        self.refcounts.get(slot as usize).copied().unwrap_or(0)
    }
    pub fn find(&self, node_id: NodeId, incarnation: Incarnation) -> Option<NodeSlot> {
        let h = hash_node_identity(node_id, incarnation);
        let descriptors = &self.descriptors;
        self.index
            .find(h, |slot| {
                let d = &descriptors[slot as usize];
                d.node_id == node_id && d.incarnation == incarnation
            })
            .map(|s| s as NodeSlot)
    }

    pub(super) fn intern(&mut self, node_id: NodeId, incarnation: Incarnation) -> Option<NodeSlot> {
        if let Some(slot) = self.find(node_id, incarnation) {
            return Some(slot);
        }
        if self.free_head == EMPTY_DICT_SLOT {
            return None;
        }
        let slot = self.free_head;
        self.free_head = self.free_next[slot as usize];
        self.descriptors[slot as usize] = NodeDescriptor {
            node_id,
            incarnation,
        };
        self.refcounts[slot as usize] = 0;
        self.index
            .insert_unchecked(hash_node_identity(node_id, incarnation), slot as u32);
        self.len += 1;
        Some(slot)
    }

    pub(super) fn inc_ref(&mut self, slot: NodeSlot) {
        self.refcounts[slot as usize] = self.refcounts[slot as usize].saturating_add(1);
    }

    /// Decrement refcount. Returns `true` if the slot was freed.
    pub(super) fn dec_ref(&mut self, slot: NodeSlot) -> bool {
        let rc = &mut self.refcounts[slot as usize];
        if *rc == 0 {
            return false;
        }
        *rc -= 1;
        if *rc == 0 {
            let d = self.descriptors[slot as usize];
            self.index
                .remove(hash_node_identity(d.node_id, d.incarnation), slot as u32);
            self.descriptors[slot as usize] = NodeDescriptor::default();
            self.free_next[slot as usize] = self.free_head;
            self.free_head = slot;
            self.len -= 1;
            true
        } else {
            false
        }
    }
}

pub(super) fn pow2_index_capacity_for(capacity: u32) -> u32 {
    // Load factor target ~50%: index buckets = next_power_of_two(capacity) * 2.
    let base = capacity.max(1).next_power_of_two();
    base.saturating_mul(2).max(2)
}
