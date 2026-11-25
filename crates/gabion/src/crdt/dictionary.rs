//! Rule and node identity dictionaries.

use std::num::{NonZeroU16, NonZeroU32};

use super::hash::{hash_fingerprint, hash_node_identity};
use super::index::CellIndex;
use super::{BucketEpoch, Incarnation, NodeId, NodeSlot, RuleSlot};

const EMPTY_DICT_SLOT: u16 = u16::MAX;

/// The most entries a dictionary holds: its slots are numbered by a `u16`
/// whose largest value marks an empty slot.
pub(crate) const MAX_DICTIONARY_CAPACITY: u16 = EMPTY_DICT_SLOT - 1;

/// Descriptor stored per interned rule slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuleDescriptor {
    /// The rule's identity: a hash of its domain, descriptor patterns, window
    /// and bucket (`rules::RuleDef::build`).
    pub fingerprint: u128,
    pub window_millis: u32,
    pub bucket_millis: u32,
    pub limit: u64,
    /// `false` for a rule known on the wire only: its cells are stored,
    /// forwarded and expired, but [`super::DeltaSink::applies_locally`] is
    /// `false` so the local rate-limit aggregator ignores them.
    pub applies_locally: bool,
}

impl Default for RuleDescriptor {
    fn default() -> Self {
        Self {
            fingerprint: 0,
            // How long a wire-only rule's cell lives past its last update
            // (`RuleAge::Idle`).
            window_millis: 60_000,
            bucket_millis: 1_000,
            limit: 0,
            applies_locally: false,
        }
    }
}

impl RuleDescriptor {
    /// The bucket epoch `now_millis` falls in: `now_millis / bucket_millis`.
    /// The single source of truth for this floor used by both
    /// [`super::CellStore::expire_at`] and the visualizer. `0` for a zero
    /// bucket, which no built rule has; only the visualizer's sizing reads it
    /// before validation.
    pub fn current_epoch(&self, now_millis: u64) -> BucketEpoch {
        if self.bucket_millis == 0 {
            BucketEpoch(0)
        } else {
            BucketEpoch::wrap(now_millis / u64::from(self.bucket_millis))
        }
    }

    /// Number of live buckets in the window: `ceil(window_millis /
    /// bucket_millis)`, at least 1. `0` for a zero bucket, which no built
    /// rule has; only the visualizer's sizing reads it before validation.
    pub fn live_buckets(&self) -> u32 {
        if self.bucket_millis == 0 {
            0
        } else {
            self.window_millis.div_ceil(self.bucket_millis).max(1)
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeDescriptor {
    pub node_id: NodeId,
    pub incarnation: Incarnation,
}

/// What a [`Dictionary`] interns: a descriptor, the key it is found by, and
/// the slot newtype it hands out.
pub trait Entry: Copy + Default {
    type Key: Copy + PartialEq;
    type Slot: Copy;
    fn key(&self) -> Self::Key;
    fn hash(key: Self::Key) -> u64;
    fn slot(row: u16) -> Self::Slot;
    fn row(slot: Self::Slot) -> u16;
}

impl Entry for RuleDescriptor {
    type Key = u128;
    type Slot = RuleSlot;

    fn key(&self) -> u128 {
        self.fingerprint
    }

    fn hash(fingerprint: u128) -> u64 {
        hash_fingerprint(fingerprint)
    }

    fn slot(row: u16) -> RuleSlot {
        RuleSlot(row)
    }

    fn row(slot: RuleSlot) -> u16 {
        slot.0
    }
}

impl Entry for NodeDescriptor {
    type Key = (NodeId, Incarnation);
    type Slot = NodeSlot;

    fn key(&self) -> (NodeId, Incarnation) {
        (self.node_id, self.incarnation)
    }

    fn hash((node_id, incarnation): (NodeId, Incarnation)) -> u64 {
        hash_node_identity(node_id, incarnation)
    }

    fn slot(row: u16) -> NodeSlot {
        NodeSlot(row)
    }

    fn row(slot: NodeSlot) -> u16 {
        slot.0
    }
}

/// Interns rule fingerprints to a small [`RuleSlot`]. Bounded; rejects on full.
pub type RuleDictionary = Dictionary<RuleDescriptor>;

/// Interns `(NodeId, Incarnation)` pairs to a small [`NodeSlot`]. Each
/// distinct pair gets its own slot — incarnation changes always allocate
/// a fresh slot, so cells from different incarnations cannot alias.
pub type NodeDictionary = Dictionary<NodeDescriptor>;

/// Interns descriptors to small slots. Bounded; rejects on full. Each cell
/// holds a reference to its rule and node slots, and a slot frees when its
/// last reference goes.
#[derive(Clone, Debug)]
pub struct Dictionary<E: Entry> {
    descriptors: Box<[E]>,
    refcounts: Box<[u32]>,
    index: CellIndex,
    free_next: Box<[u16]>,
    free_head: u16,
    len: u16,
    capacity: u16,
}

impl<E: Entry> Dictionary<E> {
    pub(crate) fn with_capacity(capacity: NonZeroU16) -> Self {
        let index_cap = pow2_index_capacity_for(capacity.into());
        let capacity = capacity.get();
        debug_assert!(
            capacity < EMPTY_DICT_SLOT,
            "`Settings::validate` bounds dictionary capacity below u16::MAX"
        );
        let mut free_next = vec![EMPTY_DICT_SLOT; capacity as usize].into_boxed_slice();
        for i in 0..capacity {
            free_next[i as usize] = if i + 1 < capacity {
                i + 1
            } else {
                EMPTY_DICT_SLOT
            };
        }
        Self {
            descriptors: vec![E::default(); capacity as usize].into_boxed_slice(),
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

    pub fn descriptor(&self, slot: E::Slot) -> Option<&E> {
        (self.refcount(slot) > 0).then(|| &self.descriptors[usize::from(E::row(slot))])
    }

    /// The descriptor of a slot a cell holds a reference to.
    pub(super) fn get(&self, slot: E::Slot) -> &E {
        let row = usize::from(E::row(slot));
        debug_assert!(
            self.refcounts[row] > 0,
            "a cell's reference keeps its slot live"
        );
        &self.descriptors[row]
    }

    pub fn refcount(&self, slot: E::Slot) -> u32 {
        self.refcounts
            .get(usize::from(E::row(slot)))
            .copied()
            .unwrap_or(0)
    }

    /// Look up an existing entry by key, without inserting.
    pub fn find(&self, key: E::Key) -> Option<E::Slot> {
        let descriptors = &self.descriptors;
        self.index
            .find(E::hash(key), |row| descriptors[row as usize].key() == key)
            .map(|row| E::slot(row as u16))
    }

    /// Intern a descriptor whose key [`Self::find`] just missed, at refcount
    /// zero: callers manage references through [`super::CellStore`]. `None`
    /// when the dictionary is full.
    pub(super) fn insert(&mut self, descriptor: E) -> Option<E::Slot> {
        if self.free_head == EMPTY_DICT_SLOT {
            return None;
        }
        let row = self.free_head;
        self.free_head = self.free_next[row as usize];
        self.descriptors[row as usize] = descriptor;
        self.refcounts[row as usize] = 0;
        self.index
            .insert_unchecked(E::hash(descriptor.key()), u32::from(row));
        self.len += 1;
        Some(E::slot(row))
    }

    pub(super) fn inc_ref(&mut self, slot: E::Slot) {
        let rc = &mut self.refcounts[usize::from(E::row(slot))];
        *rc = rc.saturating_add(1);
    }

    /// Decrement refcount. Returns `true` if the slot was freed.
    pub(super) fn dec_ref(&mut self, slot: E::Slot) -> bool {
        let row = E::row(slot);
        let rc = &mut self.refcounts[usize::from(row)];
        if *rc == 0 {
            return false;
        }
        *rc -= 1;
        if *rc > 0 {
            return false;
        }
        let key = self.descriptors[usize::from(row)].key();
        self.index.remove(E::hash(key), u32::from(row));
        self.descriptors[usize::from(row)] = E::default();
        self.free_next[usize::from(row)] = self.free_head;
        self.free_head = row;
        self.len -= 1;
        true
    }
}

/// Index buckets for `capacity` entries: a power of two at a ~50% load
/// factor. Cells are capped at 2^30, so the result fits a `u32`.
pub(crate) fn pow2_index_capacity_for(capacity: NonZeroU32) -> u32 {
    capacity.get().next_power_of_two() * 2
}
