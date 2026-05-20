//! Per-origin counter CRDT with a data-oriented layout.
//!
//! Storage is a single global structure-of-arrays [`CellStore`] holding one
//! row per active counter. Identity is interned through two small bounded
//! dictionaries — [`RuleDictionary`] turns a rule fingerprint into a 16-bit
//! [`RuleSlot`] and [`NodeDictionary`] turns a `(NodeId, Incarnation)` pair
//! into a 16-bit [`NodeSlot`] — so each cell's identity is six small columns
//! totalling 28 bytes instead of the 52-byte AoS struct the previous layout
//! used.
//!
//! Every mutation that raises a stored count appends one row to a
//! caller-owned [`DeltaSink`], whose SoA shape lets a higher-level
//! rate-limit aggregator fold deltas without per-record dispatch.
//!
//! Two dirty rings track recent changes — `local_dirty` for cells whose
//! origin is this node, `forwarded_dirty` for cells learned from peers. A
//! third lane, the repair cursor, rotates over the entire active set so
//! anti-entropy still converges when the dirty rings overflow.
//!
//! Slot lifecycle uses an intrusive freelist (`free_next` linking inactive
//! slots) and a generation counter per slot (low bit = active flag, high
//! bits = ABA tag). Handles carry the expected generation so a freed-and-
//! reused slot can never be confused with the original.
//!
//! The hash index is a power-of-two Robin Hood table with backshift
//! deletion — no tombstones — so probe distance stays bounded under
//! churn.
//!
//! Nothing in this module allocates after construction.

use std::fmt;

/// Application-level rule identifier — opaque to the CRDT.
pub type RuleId = u32;

/// Bucket index = `now_millis / rule.bucket_millis`.
pub type BucketEpoch = u32;

/// Node incarnation counter — distinguishes successive instances of one node.
pub type Incarnation = u32;

/// Interned rule slot. Index into [`RuleDictionary`].
pub type RuleSlot = u16;

/// Interned `(node_id, incarnation)` slot. Index into [`NodeDictionary`].
pub type NodeSlot = u16;

/// Application-level key identity, hashed to 128 bits.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KeyHash(pub u128);

/// A peer's stable identity.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub u128);

/// Stable owner identity for cells created by one node incarnation.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct NodeIdentity {
    pub node_id: NodeId,
    pub incarnation: Incarnation,
}

impl NodeIdentity {
    pub fn new(node_id: NodeId, incarnation: Incarnation) -> Self {
        Self {
            node_id,
            incarnation,
        }
    }
}

/// Count column abstraction. One `CellStore<C>` is monomorphized to a single
/// count width — narrow for high-throughput tables, wide for large limits.
pub trait Count: Copy + Eq + Ord + Default + Into<u64> + 'static {
    const MAX: Self;

    fn saturating_from_u64(value: u64) -> Self;
    fn saturating_add_hits(self, hits: u64) -> Self;
    fn saturating_delta(new: Self, old: Self) -> Self;
}

impl Count for u16 {
    const MAX: Self = u16::MAX;

    fn saturating_from_u64(value: u64) -> Self {
        value.min(u16::MAX as u64) as u16
    }
    fn saturating_add_hits(self, hits: u64) -> Self {
        let sum = (self as u64).saturating_add(hits);
        sum.min(u16::MAX as u64) as u16
    }
    fn saturating_delta(new: Self, old: Self) -> Self {
        new.saturating_sub(old)
    }
}

impl Count for u32 {
    const MAX: Self = u32::MAX;

    fn saturating_from_u64(value: u64) -> Self {
        value.min(u32::MAX as u64) as u32
    }
    fn saturating_add_hits(self, hits: u64) -> Self {
        let sum = (self as u64).saturating_add(hits);
        sum.min(u32::MAX as u64) as u32
    }
    fn saturating_delta(new: Self, old: Self) -> Self {
        new.saturating_sub(old)
    }
}

impl Count for u64 {
    const MAX: Self = u64::MAX;

    fn saturating_from_u64(value: u64) -> Self {
        value
    }
    fn saturating_add_hits(self, hits: u64) -> Self {
        self.saturating_add(hits)
    }
    fn saturating_delta(new: Self, old: Self) -> Self {
        new.saturating_sub(old)
    }
}

/// Interned counter identity. All fields are dictionary slots or small ints;
/// the struct never carries a raw `NodeId` or `RuleId`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CompactCellKey {
    pub rule: RuleSlot,
    pub key_hash: KeyHash,
    pub bucket: BucketEpoch,
    pub origin: NodeSlot,
    pub incarnation: Incarnation,
}

/// Generation-stamped slot identifier. The `generation` field's low bit
/// reflects the active flag at handle-creation time; high bits are an ABA
/// tag that lets a freed-and-reused slot be detected via a single equality
/// check.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CellHandle {
    pub index: u32,
    pub generation: u32,
}

/// One row appended to [`DeltaSink`] when a stored count rose.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellDelta<C: Count> {
    pub handle: CellHandle,
    pub key: CompactCellKey,
    pub previous_count: C,
    pub current_count: C,
    pub delta: C,
    pub applies_locally: bool,
}

/// Result of a single-row update — useful for callers who do not want the
/// SoA `DeltaSink` shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UpdateOutcome<C: Count> {
    pub handle: CellHandle,
    pub changed: bool,
    pub delta: Option<CellDelta<C>>,
}

/// Snapshot of one stored cell.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellRow<C: Count> {
    pub handle: CellHandle,
    pub key: CompactCellKey,
    pub count: C,
    pub last_update_millis: u64,
    pub origin_sequence: u64,
}

/// Error returned when an insertion would exceed bounded capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertReject {
    CellStoreFull,
    RuleDictionaryFull,
    NodeDictionaryFull,
}

impl fmt::Display for InsertReject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InsertReject::CellStoreFull => f.write_str("cell store at capacity"),
            InsertReject::RuleDictionaryFull => f.write_str("rule dictionary at capacity"),
            InsertReject::NodeDictionaryFull => f.write_str("node dictionary at capacity"),
        }
    }
}

impl std::error::Error for InsertReject {}

/// Counters surfaced for observability.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CellStoreStats {
    pub active_cells: u32,
    pub cell_capacity: u32,
    pub rule_slots_used: u16,
    pub rule_slots_capacity: u16,
    pub node_slots_used: u16,
    pub node_slots_capacity: u16,
    pub cell_store_full_rejects: u64,
    pub rule_dictionary_full_rejects: u64,
    pub node_dictionary_full_rejects: u64,
}

// ---------------------------------------------------------------------------
// Hash mixer
// ---------------------------------------------------------------------------

#[inline(always)]
fn mix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

#[inline(always)]
fn hash_compact_cell_key(key: &CompactCellKey) -> u64 {
    let lo = key.key_hash.0 as u64;
    let hi = (key.key_hash.0 >> 64) as u64;
    let pack_a = (key.rule as u64) | ((key.origin as u64) << 16) | ((key.bucket as u64) << 32);
    let pack_b = key.incarnation as u64;
    mix64(lo ^ pack_a) ^ mix64(hi.wrapping_add(pack_b).wrapping_add(0x9E37_79B9_7F4A_7C15))
}

#[inline(always)]
fn hash_fingerprint(fingerprint: u128) -> u64 {
    let lo = fingerprint as u64;
    let hi = (fingerprint >> 64) as u64;
    mix64(lo) ^ mix64(hi.wrapping_add(0x517C_C1B7_2722_0A95))
}

#[inline(always)]
fn hash_node_identity(node_id: NodeId, incarnation: Incarnation) -> u64 {
    let lo = node_id.0 as u64;
    let hi = (node_id.0 >> 64) as u64;
    mix64(lo ^ incarnation as u64) ^ mix64(hi.wrapping_add(0xBF58_476D_1CE4_E5B9))
}

// ---------------------------------------------------------------------------
// CellIndex — Robin Hood open addressing with backshift deletion
// ---------------------------------------------------------------------------

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
        self.slot_indexes.iter_mut().for_each(|s| *s = EMPTY_INDEX_SLOT);
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

// ---------------------------------------------------------------------------
// DirtyRing
// ---------------------------------------------------------------------------

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
/// observers can detect that the recent-change stream is no longer authoritative.
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

// ---------------------------------------------------------------------------
// Dictionaries
// ---------------------------------------------------------------------------

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
    /// dirty-tracked, and expired the same way, but [`CellDelta::applies_locally`]
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
    /// through [`CellStore`].
    fn intern(&mut self, descriptor: RuleDescriptor) -> Option<RuleSlot> {
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

    fn inc_ref(&mut self, slot: RuleSlot) {
        self.refcounts[slot as usize] = self.refcounts[slot as usize].saturating_add(1);
    }

    fn dec_ref(&mut self, slot: RuleSlot) {
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

    fn intern(&mut self, node_id: NodeId, incarnation: Incarnation) -> Option<NodeSlot> {
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

    fn inc_ref(&mut self, slot: NodeSlot) {
        self.refcounts[slot as usize] = self.refcounts[slot as usize].saturating_add(1);
    }

    /// Decrement refcount. Returns `true` if the slot was freed.
    fn dec_ref(&mut self, slot: NodeSlot) -> bool {
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

fn pow2_index_capacity_for(capacity: u32) -> u32 {
    // Load factor target ~50%: index buckets = next_power_of_two(capacity) * 2.
    let base = capacity.max(1).next_power_of_two();
    base.saturating_mul(2).max(2)
}

// ---------------------------------------------------------------------------
// PeerFrontierTable
// ---------------------------------------------------------------------------

/// Per-peer × per-origin frontier: what we last sent to / heard acked from a
/// given peer for each origin. **Latency optimization only** — convergence
/// is guaranteed by the repair lane, not by this table.
#[derive(Clone, Debug)]
pub struct PeerFrontierTable {
    peer_ids: Box<[Option<NodeId>]>,
    last_acked_seq: Box<[u64]>,
    last_sent_seq: Box<[u64]>,
    peer_capacity: u16,
    node_capacity: u16,
}

impl PeerFrontierTable {
    pub fn new(peer_capacity: u16, node_capacity: u16) -> Self {
        let flat = (peer_capacity as usize).saturating_mul(node_capacity as usize);
        Self {
            peer_ids: vec![None; peer_capacity as usize].into_boxed_slice(),
            last_acked_seq: vec![0_u64; flat].into_boxed_slice(),
            last_sent_seq: vec![0_u64; flat].into_boxed_slice(),
            peer_capacity,
            node_capacity,
        }
    }

    pub fn peer_capacity(&self) -> u16 {
        self.peer_capacity
    }
    pub fn node_capacity(&self) -> u16 {
        self.node_capacity
    }

    pub fn find_peer(&self, peer: NodeId) -> Option<u16> {
        self.peer_ids
            .iter()
            .position(|p| *p == Some(peer))
            .map(|i| i as u16)
    }

    pub fn intern_peer(&mut self, peer: NodeId) -> Option<u16> {
        if let Some(slot) = self.find_peer(peer) {
            return Some(slot);
        }
        for (i, slot) in self.peer_ids.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(peer);
                return Some(i as u16);
            }
        }
        None
    }

    pub fn remove_peer(&mut self, peer: NodeId) {
        let Some(peer_slot) = self.find_peer(peer) else {
            return;
        };
        self.peer_ids[peer_slot as usize] = None;
        let base = (peer_slot as usize) * (self.node_capacity as usize);
        let end = base + self.node_capacity as usize;
        self.last_acked_seq[base..end].fill(0);
        self.last_sent_seq[base..end].fill(0);
    }

    #[inline]
    fn flat_index(&self, peer_slot: u16, node_slot: NodeSlot) -> usize {
        (peer_slot as usize) * (self.node_capacity as usize) + node_slot as usize
    }

    pub fn last_acked(&self, peer_slot: u16, node_slot: NodeSlot) -> u64 {
        self.last_acked_seq[self.flat_index(peer_slot, node_slot)]
    }
    pub fn last_sent(&self, peer_slot: u16, node_slot: NodeSlot) -> u64 {
        self.last_sent_seq[self.flat_index(peer_slot, node_slot)]
    }
    pub fn record_sent(&mut self, peer_slot: u16, node_slot: NodeSlot, sequence: u64) {
        let i = self.flat_index(peer_slot, node_slot);
        if self.last_sent_seq[i] < sequence {
            self.last_sent_seq[i] = sequence;
        }
    }
    pub fn record_acked(&mut self, peer_slot: u16, node_slot: NodeSlot, sequence: u64) {
        let i = self.flat_index(peer_slot, node_slot);
        if self.last_acked_seq[i] < sequence {
            self.last_acked_seq[i] = sequence;
        }
    }

    /// Zero the per-peer rows for `node_slot`. Called when the dictionary
    /// frees the slot — prevents the next `(NodeId, Incarnation)` reusing
    /// the slot from inheriting acked state.
    pub fn clear_node_slot(&mut self, node_slot: NodeSlot) {
        for peer_slot in 0..self.peer_capacity {
            let i = self.flat_index(peer_slot, node_slot);
            self.last_acked_seq[i] = 0;
            self.last_sent_seq[i] = 0;
        }
    }

    /// Brute-force "what does this peer lack" scan against the supplied
    /// origin-sequence column. Used by tests and by digest-mismatch repair.
    pub fn lacks_indices(
        &self,
        peer_slot: u16,
        active_origins: &[NodeSlot],
        origin_sequences: &[u64],
        active_indices: &[u32],
        out: &mut Vec<u32>,
    ) {
        out.clear();
        for &idx in active_indices {
            let origin = active_origins[idx as usize];
            let last = self.last_acked(peer_slot, origin);
            if origin_sequences[idx as usize] > last {
                out.push(idx);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ObservationBatch and DeltaSink — column-oriented IO
// ---------------------------------------------------------------------------

/// SoA input for `ingest_local` / `merge_remote`. For `ingest_local`, the
/// `counts` column carries the hit delta to saturating-add; for
/// `merge_remote`, it carries the observed absolute count to max-merge.
#[derive(Clone, Debug, Default)]
pub struct ObservationBatch<C: Count> {
    pub rule_fingerprints: Vec<u128>,
    pub key_hashes: Vec<KeyHash>,
    pub buckets: Vec<BucketEpoch>,
    pub origin_node_ids: Vec<NodeId>,
    pub incarnations: Vec<Incarnation>,
    pub counts: Vec<C>,
    pub last_update_millis: Vec<u64>,
}

impl<C: Count> ObservationBatch<C> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rule_fingerprints: Vec::with_capacity(capacity),
            key_hashes: Vec::with_capacity(capacity),
            buckets: Vec::with_capacity(capacity),
            origin_node_ids: Vec::with_capacity(capacity),
            incarnations: Vec::with_capacity(capacity),
            counts: Vec::with_capacity(capacity),
            last_update_millis: Vec::with_capacity(capacity),
        }
    }

    pub fn assert_consistent(&self) {
        let n = self.rule_fingerprints.len();
        debug_assert_eq!(self.key_hashes.len(), n);
        debug_assert_eq!(self.buckets.len(), n);
        debug_assert_eq!(self.origin_node_ids.len(), n);
        debug_assert_eq!(self.incarnations.len(), n);
        debug_assert_eq!(self.counts.len(), n);
        debug_assert_eq!(self.last_update_millis.len(), n);
    }

    pub fn len(&self) -> usize {
        self.rule_fingerprints.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn clear(&mut self) {
        self.rule_fingerprints.clear();
        self.key_hashes.clear();
        self.buckets.clear();
        self.origin_node_ids.clear();
        self.incarnations.clear();
        self.counts.clear();
        self.last_update_millis.clear();
    }

    /// Append one observation row. Caller must keep all columns the same length.
    pub fn push(
        &mut self,
        rule_fingerprint: u128,
        key_hash: KeyHash,
        bucket: BucketEpoch,
        origin: NodeId,
        incarnation: Incarnation,
        count: C,
        last_update_millis: u64,
    ) {
        self.rule_fingerprints.push(rule_fingerprint);
        self.key_hashes.push(key_hash);
        self.buckets.push(bucket);
        self.origin_node_ids.push(origin);
        self.incarnations.push(incarnation);
        self.counts.push(count);
        self.last_update_millis.push(last_update_millis);
    }
}

/// SoA delta channel. The CRDT module appends one column row per cell whose
/// stored count rose. The aggregator folds without per-record dispatch.
#[derive(Clone, Debug, Default)]
pub struct DeltaSink<C: Count> {
    pub handles: Vec<CellHandle>,
    pub keys: Vec<CompactCellKey>,
    pub previous: Vec<C>,
    pub current: Vec<C>,
    pub deltas: Vec<C>,
    /// `0`/`1` flags — kept as a byte column rather than `Vec<bool>`.
    pub applies_locally: Vec<u8>,
}

impl<C: Count> DeltaSink<C> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            handles: Vec::with_capacity(capacity),
            keys: Vec::with_capacity(capacity),
            previous: Vec::with_capacity(capacity),
            current: Vec::with_capacity(capacity),
            deltas: Vec::with_capacity(capacity),
            applies_locally: Vec::with_capacity(capacity),
        }
    }
    pub fn len(&self) -> usize {
        self.handles.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn clear(&mut self) {
        self.handles.clear();
        self.keys.clear();
        self.previous.clear();
        self.current.clear();
        self.deltas.clear();
        self.applies_locally.clear();
    }

    pub fn row(&self, i: usize) -> Option<CellDelta<C>> {
        if i >= self.len() {
            return None;
        }
        Some(CellDelta {
            handle: self.handles[i],
            key: self.keys[i],
            previous_count: self.previous[i],
            current_count: self.current[i],
            delta: self.deltas[i],
            applies_locally: self.applies_locally[i] != 0,
        })
    }

    fn push(
        &mut self,
        handle: CellHandle,
        key: CompactCellKey,
        previous: C,
        current: C,
        delta: C,
        applies_locally: bool,
    ) {
        self.handles.push(handle);
        self.keys.push(key);
        self.previous.push(previous);
        self.current.push(current);
        self.deltas.push(delta);
        self.applies_locally.push(applies_locally as u8);
    }
}

// ---------------------------------------------------------------------------
// CellStore
// ---------------------------------------------------------------------------

/// Construction-time configuration.
#[derive(Clone, Copy, Debug)]
pub struct CellStoreConfig {
    pub cell_capacity: u32,
    pub rule_dictionary_capacity: u16,
    pub node_dictionary_capacity: u16,
    pub local_dirty_capacity: usize,
    pub forwarded_dirty_capacity: usize,
    pub peer_capacity: u16,
}

impl Default for CellStoreConfig {
    fn default() -> Self {
        Self {
            cell_capacity: 256,
            rule_dictionary_capacity: 32,
            node_dictionary_capacity: 32,
            local_dirty_capacity: 64,
            forwarded_dirty_capacity: 64,
            peer_capacity: 16,
        }
    }
}

/// Single global structure-of-arrays cell store.
#[derive(Clone, Debug)]
pub struct CellStore<C: Count = u32> {
    // Cold key columns — only touched on identity work.
    rules: Box<[RuleSlot]>,
    key_hashes: Box<[KeyHash]>,
    buckets: Box<[BucketEpoch]>,
    origins: Box<[NodeSlot]>,
    incarnations: Box<[Incarnation]>,

    // Hot value columns — touched on every merge.
    counts: Box<[C]>,
    last_update_millis: Box<[u64]>,
    origin_sequences: Box<[u64]>,

    // Slot lifecycle.
    generations: Box<[u32]>,
    free_next: Box<[u32]>,
    free_head: u32,
    active_len: u32,
    capacity: u32,

    // Identity index.
    index: CellIndex,

    // Gossip lanes.
    local_dirty: DirtyRing,
    forwarded_dirty: DirtyRing,
    repair_cursor: u32,

    // Frame-level dedup.
    selection_marks: Box<[u32]>,
    selection_epoch: u32,

    // Per-origin sequence allocator (indexed by NodeSlot).
    next_sequence_by_origin: Box<[u64]>,

    // Identity dictionaries.
    rule_dictionary: RuleDictionary,
    node_dictionary: NodeDictionary,

    // Peer state.
    peer_frontiers: PeerFrontierTable,

    // Local identity.
    local_identity: NodeIdentity,
    local_node_slot: NodeSlot,

    // Counters.
    cell_store_full_rejects: u64,
    rule_dictionary_full_rejects: u64,
    node_dictionary_full_rejects: u64,
}

const NO_FREE: u32 = u32::MAX;

impl<C: Count> CellStore<C> {
    pub fn new(config: CellStoreConfig, local_identity: NodeIdentity) -> Self {
        assert!(config.cell_capacity > 0, "cell capacity must be > 0");
        assert!(
            config.rule_dictionary_capacity > 0,
            "rule dictionary capacity must be > 0"
        );
        assert!(
            config.node_dictionary_capacity > 0,
            "node dictionary capacity must be > 0"
        );

        let cap = config.cell_capacity;
        let index_cap = pow2_index_capacity_for(cap);

        // Build the intrusive freelist: 0 -> 1 -> 2 -> ... -> cap-1 -> NO_FREE
        let mut free_next = vec![NO_FREE; cap as usize].into_boxed_slice();
        for i in 0..cap {
            free_next[i as usize] = if i + 1 < cap { i + 1 } else { NO_FREE };
        }

        let mut node_dictionary = NodeDictionary::with_capacity(config.node_dictionary_capacity);
        let local_node_slot = node_dictionary
            .intern(local_identity.node_id, local_identity.incarnation)
            .expect("node dictionary capacity > 0");
        // Pin the local slot so it cannot be freed via dec_ref.
        node_dictionary.inc_ref(local_node_slot);

        Self {
            rules: vec![0_u16; cap as usize].into_boxed_slice(),
            key_hashes: vec![KeyHash(0); cap as usize].into_boxed_slice(),
            buckets: vec![0_u32; cap as usize].into_boxed_slice(),
            origins: vec![0_u16; cap as usize].into_boxed_slice(),
            incarnations: vec![0_u32; cap as usize].into_boxed_slice(),

            counts: vec![C::default(); cap as usize].into_boxed_slice(),
            last_update_millis: vec![0_u64; cap as usize].into_boxed_slice(),
            origin_sequences: vec![0_u64; cap as usize].into_boxed_slice(),

            generations: vec![0_u32; cap as usize].into_boxed_slice(),
            free_next,
            free_head: 0,
            active_len: 0,
            capacity: cap,

            index: CellIndex::with_capacity(index_cap),

            local_dirty: DirtyRing::with_capacity(config.local_dirty_capacity),
            forwarded_dirty: DirtyRing::with_capacity(config.forwarded_dirty_capacity),
            repair_cursor: 0,

            selection_marks: vec![0_u32; cap as usize].into_boxed_slice(),
            selection_epoch: 0,

            next_sequence_by_origin: vec![0_u64; config.node_dictionary_capacity as usize]
                .into_boxed_slice(),

            rule_dictionary: RuleDictionary::with_capacity(config.rule_dictionary_capacity),
            node_dictionary,

            peer_frontiers: PeerFrontierTable::new(
                config.peer_capacity,
                config.node_dictionary_capacity,
            ),

            local_identity,
            local_node_slot,

            cell_store_full_rejects: 0,
            rule_dictionary_full_rejects: 0,
            node_dictionary_full_rejects: 0,
        }
    }

    // -- Accessors ----------------------------------------------------------

    pub fn capacity(&self) -> u32 {
        self.capacity
    }
    pub fn active_len(&self) -> u32 {
        self.active_len
    }
    pub fn is_empty(&self) -> bool {
        self.active_len == 0
    }
    pub fn local_identity(&self) -> NodeIdentity {
        self.local_identity
    }
    pub fn local_node_slot(&self) -> NodeSlot {
        self.local_node_slot
    }
    pub fn rule_dictionary(&self) -> &RuleDictionary {
        &self.rule_dictionary
    }
    pub fn node_dictionary(&self) -> &NodeDictionary {
        &self.node_dictionary
    }
    pub fn peer_frontiers(&self) -> &PeerFrontierTable {
        &self.peer_frontiers
    }
    pub fn peer_frontiers_mut(&mut self) -> &mut PeerFrontierTable {
        &mut self.peer_frontiers
    }
    pub fn local_dirty(&self) -> &DirtyRing {
        &self.local_dirty
    }
    pub fn forwarded_dirty(&self) -> &DirtyRing {
        &self.forwarded_dirty
    }
    pub fn repair_cursor(&self) -> u32 {
        self.repair_cursor
    }
    pub fn index(&self) -> &CellIndex {
        &self.index
    }

    pub fn stats(&self) -> CellStoreStats {
        CellStoreStats {
            active_cells: self.active_len,
            cell_capacity: self.capacity,
            rule_slots_used: self.rule_dictionary.len(),
            rule_slots_capacity: self.rule_dictionary.capacity(),
            node_slots_used: self.node_dictionary.len(),
            node_slots_capacity: self.node_dictionary.capacity(),
            cell_store_full_rejects: self.cell_store_full_rejects,
            rule_dictionary_full_rejects: self.rule_dictionary_full_rejects,
            node_dictionary_full_rejects: self.node_dictionary_full_rejects,
        }
    }

    // -- Dictionary access (test-public; runtime-public for orchestration) --

    pub fn intern_rule(&mut self, descriptor: RuleDescriptor) -> Option<RuleSlot> {
        let result = self.rule_dictionary.intern(descriptor);
        if result.is_none() {
            self.rule_dictionary_full_rejects =
                self.rule_dictionary_full_rejects.saturating_add(1);
        }
        result
    }

    pub fn intern_node(&mut self, node_id: NodeId, incarnation: Incarnation) -> Option<NodeSlot> {
        let result = self.node_dictionary.intern(node_id, incarnation);
        if result.is_none() {
            self.node_dictionary_full_rejects =
                self.node_dictionary_full_rejects.saturating_add(1);
        }
        result
    }

    pub fn find_rule(&self, fingerprint: u128) -> Option<RuleSlot> {
        self.rule_dictionary.find(fingerprint)
    }

    pub fn find_node(&self, node_id: NodeId, incarnation: Incarnation) -> Option<NodeSlot> {
        self.node_dictionary.find(node_id, incarnation)
    }

    // -- Cell lookups -------------------------------------------------------

    fn handle_for(&self, index: u32) -> CellHandle {
        CellHandle {
            index,
            generation: self.generations[index as usize],
        }
    }

    fn is_active(&self, index: u32) -> bool {
        (self.generations[index as usize] & 1) == 1
    }

    fn lookup_index(&self, key: CompactCellKey) -> Option<u32> {
        let h = hash_compact_cell_key(&key);
        let rules = &self.rules;
        let hashes = &self.key_hashes;
        let buckets = &self.buckets;
        let origins = &self.origins;
        let incs = &self.incarnations;
        self.index.find(h, |slot| {
            rules[slot as usize] == key.rule
                && hashes[slot as usize] == key.key_hash
                && buckets[slot as usize] == key.bucket
                && origins[slot as usize] == key.origin
                && incs[slot as usize] == key.incarnation
        })
    }

    pub fn find(&self, key: CompactCellKey) -> Option<CellHandle> {
        self.lookup_index(key).map(|i| self.handle_for(i))
    }

    pub fn resolve(&self, handle: CellHandle) -> Option<u32> {
        if handle.index >= self.capacity {
            return None;
        }
        if self.generations[handle.index as usize] != handle.generation {
            return None;
        }
        if (handle.generation & 1) != 1 {
            return None;
        }
        Some(handle.index)
    }

    pub fn get(&self, handle: CellHandle) -> Option<CellRow<C>> {
        let i = self.resolve(handle)? as usize;
        Some(CellRow {
            handle,
            key: CompactCellKey {
                rule: self.rules[i],
                key_hash: self.key_hashes[i],
                bucket: self.buckets[i],
                origin: self.origins[i],
                incarnation: self.incarnations[i],
            },
            count: self.counts[i],
            last_update_millis: self.last_update_millis[i],
            origin_sequence: self.origin_sequences[i],
        })
    }

    pub fn count_of(&self, handle: CellHandle) -> Option<C> {
        self.resolve(handle).map(|i| self.counts[i as usize])
    }

    /// Iterate all active cell handles in storage order.
    ///
    /// Walks the full `capacity`, not `active_len` — suitable for diagnostics
    /// and tests, not hot paths.
    pub fn active_handles(&self) -> impl Iterator<Item = CellHandle> + '_ {
        (0..self.capacity).filter_map(move |i| {
            if self.is_active(i) {
                Some(self.handle_for(i))
            } else {
                None
            }
        })
    }

    // -- Slot lifecycle -----------------------------------------------------

    fn alloc_slot(&mut self) -> Option<u32> {
        if self.free_head == NO_FREE {
            return None;
        }
        let slot = self.free_head;
        self.free_head = self.free_next[slot as usize];
        self.free_next[slot as usize] = NO_FREE;
        // Flip to active: bump generation.
        self.generations[slot as usize] = self.generations[slot as usize].wrapping_add(1);
        debug_assert!(self.is_active(slot));
        self.active_len += 1;
        Some(slot)
    }

    fn free_slot(&mut self, slot: u32) {
        debug_assert!(self.is_active(slot));
        // Flip to inactive: bump generation again.
        self.generations[slot as usize] = self.generations[slot as usize].wrapping_add(1);
        self.free_next[slot as usize] = self.free_head;
        self.free_head = slot;
        self.active_len -= 1;
    }

    fn free_cell_at(&mut self, slot: u32) {
        let rule = self.rules[slot as usize];
        let origin = self.origins[slot as usize];
        let key = CompactCellKey {
            rule,
            key_hash: self.key_hashes[slot as usize],
            bucket: self.buckets[slot as usize],
            origin,
            incarnation: self.incarnations[slot as usize],
        };
        let h = hash_compact_cell_key(&key);
        self.index.remove(h, slot);
        self.rule_dictionary.dec_ref(rule);
        let freed_node = self.node_dictionary.dec_ref(origin);
        if freed_node {
            self.peer_frontiers.clear_node_slot(origin);
            self.next_sequence_by_origin[origin as usize] = 0;
        }
        self.free_slot(slot);
    }

    // -- Update primitives --------------------------------------------------

    fn next_origin_sequence(&mut self, origin: NodeSlot) -> u64 {
        let seq = &mut self.next_sequence_by_origin[origin as usize];
        *seq = seq.saturating_add(1);
        *seq
    }

    fn push_dirty(&mut self, origin: NodeSlot, handle: CellHandle, sequence: u64) {
        let entry = DirtyEntry {
            handle,
            origin_sequence: sequence,
        };
        if origin == self.local_node_slot {
            self.local_dirty.push(entry);
        } else {
            self.forwarded_dirty.push(entry);
        }
    }

    /// Append a row to the SoA delta sink.
    fn emit_delta(
        &self,
        sink: &mut DeltaSink<C>,
        slot: u32,
        previous: C,
        current: C,
        delta: C,
        rule_slot: RuleSlot,
    ) {
        let handle = self.handle_for(slot);
        let applies_locally = self
            .rule_dictionary
            .descriptor(rule_slot)
            .map(|d| d.applies_locally())
            .unwrap_or(false);
        let key = CompactCellKey {
            rule: rule_slot,
            key_hash: self.key_hashes[slot as usize],
            bucket: self.buckets[slot as usize],
            origin: self.origins[slot as usize],
            incarnation: self.incarnations[slot as usize],
        };
        sink.push(handle, key, previous, current, delta, applies_locally);
    }

    /// Insert or update a row: returns the resulting handle and whether the
    /// stored count rose.
    fn upsert(
        &mut self,
        rule_slot: RuleSlot,
        key_hash: KeyHash,
        bucket: BucketEpoch,
        origin_slot: NodeSlot,
        incarnation: Incarnation,
        new_count: C,
        accumulate: bool,
        hits_for_local: u64,
        now_millis: u64,
        sink: &mut DeltaSink<C>,
    ) -> Result<UpdateOutcome<C>, InsertReject> {
        let key = CompactCellKey {
            rule: rule_slot,
            key_hash,
            bucket,
            origin: origin_slot,
            incarnation,
        };
        if let Some(slot) = self.lookup_index(key) {
            let previous = self.counts[slot as usize];
            let next = if accumulate {
                previous.saturating_add_hits(hits_for_local)
            } else if new_count > previous {
                new_count
            } else {
                previous
            };
            if next == previous {
                return Ok(UpdateOutcome {
                    handle: self.handle_for(slot),
                    changed: false,
                    delta: None,
                });
            }
            self.counts[slot as usize] = next;
            self.last_update_millis[slot as usize] = now_millis;
            let seq = self.next_origin_sequence(origin_slot);
            self.origin_sequences[slot as usize] = seq;
            let handle = self.handle_for(slot);
            self.push_dirty(origin_slot, handle, seq);
            let delta = C::saturating_delta(next, previous);
            self.emit_delta(sink, slot, previous, next, delta, rule_slot);
            return Ok(UpdateOutcome {
                handle,
                changed: true,
                delta: sink.row(sink.len() - 1),
            });
        }

        // Insert path.
        let slot = match self.alloc_slot() {
            Some(s) => s,
            None => {
                self.cell_store_full_rejects = self.cell_store_full_rejects.saturating_add(1);
                return Err(InsertReject::CellStoreFull);
            }
        };
        let initial = if accumulate {
            C::default().saturating_add_hits(hits_for_local)
        } else {
            new_count
        };
        self.rules[slot as usize] = rule_slot;
        self.key_hashes[slot as usize] = key_hash;
        self.buckets[slot as usize] = bucket;
        self.origins[slot as usize] = origin_slot;
        self.incarnations[slot as usize] = incarnation;
        self.counts[slot as usize] = initial;
        self.last_update_millis[slot as usize] = now_millis;
        let seq = self.next_origin_sequence(origin_slot);
        self.origin_sequences[slot as usize] = seq;

        // Hold dictionary references on behalf of this cell.
        self.rule_dictionary.inc_ref(rule_slot);
        self.node_dictionary.inc_ref(origin_slot);

        self.index.insert_unchecked(hash_compact_cell_key(&key), slot);

        let handle = self.handle_for(slot);
        self.push_dirty(origin_slot, handle, seq);
        let delta = initial;
        self.emit_delta(sink, slot, C::default(), initial, delta, rule_slot);
        Ok(UpdateOutcome {
            handle,
            changed: true,
            delta: sink.row(sink.len() - 1),
        })
    }

    /// Translate a batch row's `(fingerprint, node_id, incarnation)` into
    /// dictionary slots. Returns `None` (with the rejection counter bumped)
    /// when either dictionary is full.
    fn translate_identity(
        &mut self,
        rule_fingerprint: u128,
        node_id: NodeId,
        incarnation: Incarnation,
    ) -> Option<(RuleSlot, NodeSlot)> {
        let rule_slot = match self.rule_dictionary.find(rule_fingerprint) {
            Some(slot) => slot,
            None => {
                // Unknown rule — admit with default descriptor so cells are
                // still stored, forwarded, and expired. `applies_locally`
                // will be `false` because `local_rule_id == u32::MAX`.
                let descriptor = RuleDescriptor {
                    fingerprint: rule_fingerprint,
                    ..RuleDescriptor::default()
                };
                match self.rule_dictionary.intern(descriptor) {
                    Some(slot) => slot,
                    None => {
                        self.rule_dictionary_full_rejects =
                            self.rule_dictionary_full_rejects.saturating_add(1);
                        return None;
                    }
                }
            }
        };
        let node_slot = match self.node_dictionary.find(node_id, incarnation) {
            Some(slot) => slot,
            None => match self.node_dictionary.intern(node_id, incarnation) {
                Some(slot) => slot,
                None => {
                    self.node_dictionary_full_rejects =
                        self.node_dictionary_full_rejects.saturating_add(1);
                    return None;
                }
            },
        };
        Some((rule_slot, node_slot))
    }

    // -- Primary mutation entry points --------------------------------------

    /// Record locally-observed hits. The local node is always the origin —
    /// `origin_node_ids` / `incarnations` columns on the batch are ignored.
    pub fn ingest_local(&mut self, obs: &ObservationBatch<C>, sink: &mut DeltaSink<C>) {
        obs.assert_consistent();
        for i in 0..obs.len() {
            let rule_fingerprint = obs.rule_fingerprints[i];
            let rule_slot = match self.rule_dictionary.find(rule_fingerprint) {
                Some(slot) => slot,
                None => {
                    let descriptor = RuleDescriptor {
                        fingerprint: rule_fingerprint,
                        ..RuleDescriptor::default()
                    };
                    match self.rule_dictionary.intern(descriptor) {
                        Some(slot) => slot,
                        None => {
                            self.rule_dictionary_full_rejects =
                                self.rule_dictionary_full_rejects.saturating_add(1);
                            continue;
                        }
                    }
                }
            };
            let hits_u64: u64 = obs.counts[i].into();
            let _ = self.upsert(
                rule_slot,
                obs.key_hashes[i],
                obs.buckets[i],
                self.local_node_slot,
                self.local_identity.incarnation,
                C::default(),
                true,
                hits_u64,
                obs.last_update_millis[i],
                sink,
            );
        }
    }

    /// Merge observations received from a peer. Each row is max-merged into
    /// the stored count; the resulting deltas (when the stored count rose)
    /// are appended to `sink`.
    pub fn merge_remote(&mut self, obs: &ObservationBatch<C>, sink: &mut DeltaSink<C>) {
        obs.assert_consistent();
        for i in 0..obs.len() {
            let Some((rule_slot, node_slot)) = self.translate_identity(
                obs.rule_fingerprints[i],
                obs.origin_node_ids[i],
                obs.incarnations[i],
            ) else {
                continue;
            };
            let _ = self.upsert(
                rule_slot,
                obs.key_hashes[i],
                obs.buckets[i],
                node_slot,
                obs.incarnations[i],
                obs.counts[i],
                false,
                0,
                obs.last_update_millis[i],
                sink,
            );
        }
    }

    /// Free cells whose bucket has aged out of the per-rule live window.
    ///
    /// For each rule slot `r`, the cell is kept iff
    /// `buckets[slot] + live_buckets[r] >= current_epoch_by_rule[r]`.
    pub fn expire(&mut self, current_epoch_by_rule: &[BucketEpoch], live_buckets: &[u32]) {
        for slot in 0..self.capacity {
            if !self.is_active(slot) {
                continue;
            }
            let rule = self.rules[slot as usize] as usize;
            if rule >= current_epoch_by_rule.len() || rule >= live_buckets.len() {
                continue;
            }
            let current = current_epoch_by_rule[rule];
            let live = live_buckets[rule];
            let bucket = self.buckets[slot as usize];
            let expired = (bucket as u64) + (live as u64) < (current as u64);
            if expired {
                self.free_cell_at(slot);
            }
        }
    }

    /// Drop dirty records. Stored cells are untouched.
    pub fn clear_dirty(&mut self) {
        self.local_dirty.clear();
        self.forwarded_dirty.clear();
    }

    /// Reset to an empty state — preserves the local identity registration
    /// but clears every other column. Mostly useful in tests.
    pub fn clear(&mut self) {
        // Free each active cell — this also decrements dictionary refcounts
        // and clears frontier rows.
        for slot in 0..self.capacity {
            if self.is_active(slot) {
                self.free_cell_at(slot);
            }
        }
        // Reset the dirty rings and repair cursor explicitly.
        self.clear_dirty();
        self.repair_cursor = 0;
        self.selection_epoch = 0;
        for m in self.selection_marks.iter_mut() {
            *m = 0;
        }
    }

    // -- Gossip frame composition -------------------------------------------

    fn bump_selection_epoch(&mut self) {
        let next = self.selection_epoch.wrapping_add(1);
        if next == 0 {
            // Wraparound guard: reset all marks so the new epoch (`0`) is
            // unambiguous. Pathological in practice — u32 epochs cover
            // billions of frames — but cheap insurance.
            for m in self.selection_marks.iter_mut() {
                *m = 0;
            }
            self.selection_epoch = 1;
        } else {
            self.selection_epoch = next;
        }
    }

    fn mark_selected(&mut self, slot: u32) -> bool {
        if self.selection_marks[slot as usize] == self.selection_epoch {
            return false;
        }
        self.selection_marks[slot as usize] = self.selection_epoch;
        true
    }

    /// Visit dirty cells originating on the local node, newest cell state only.
    /// `visit` returning `false` halts the scan.
    ///
    /// Streams every current entry in the ring through `visit` with no
    /// intra-call dedup — in contrast to `fill_gossip_frame`, which dedups
    /// via `selection_marks`. Callers composing multi-lane frames must
    /// dedup themselves.
    pub fn visit_local_dirty(&self, mut visit: impl FnMut(CellHandle, &CellStore<C>) -> bool) {
        for entry in self.local_dirty.iter() {
            if !self.dirty_entry_current(entry) {
                continue;
            }
            if !visit(entry.handle, self) {
                return;
            }
        }
    }

    /// Visit dirty cells originating on other peers.
    ///
    /// Streams every current entry in the ring through `visit` with no
    /// intra-call dedup — in contrast to `fill_gossip_frame`, which dedups
    /// via `selection_marks`. Callers composing multi-lane frames must
    /// dedup themselves.
    pub fn visit_forwarded_dirty(&self, mut visit: impl FnMut(CellHandle, &CellStore<C>) -> bool) {
        for entry in self.forwarded_dirty.iter() {
            if !self.dirty_entry_current(entry) {
                continue;
            }
            if !visit(entry.handle, self) {
                return;
            }
        }
    }

    fn dirty_entry_current(&self, entry: DirtyEntry) -> bool {
        let i = entry.handle.index as usize;
        if i >= self.capacity as usize {
            return false;
        }
        if self.generations[i] != entry.handle.generation {
            return false;
        }
        if self.origin_sequences[i] != entry.origin_sequence {
            return false;
        }
        true
    }

    /// Visit a fair rotating slice of active cells. Anti-entropy lane.
    pub fn visit_repair_slice(
        &mut self,
        max_cells: usize,
        mut visit: impl FnMut(CellHandle) -> bool,
    ) -> usize {
        if self.capacity == 0 || max_cells == 0 || self.active_len == 0 {
            return 0;
        }
        self.bump_selection_epoch();
        let cap = self.capacity;
        let mut visited = 0_u32;
        let mut emitted = 0_usize;
        let mut next_cursor = self.repair_cursor;
        while visited < cap && emitted < max_cells {
            let slot = (self.repair_cursor + visited) % cap;
            visited += 1;
            next_cursor = (slot + 1) % cap;
            if !self.is_active(slot) {
                continue;
            }
            if !self.mark_selected(slot) {
                continue;
            }
            let handle = self.handle_for(slot);
            emitted += 1;
            if !visit(handle) {
                break;
            }
        }
        self.repair_cursor = next_cursor;
        emitted
    }

    /// Fill one outgoing gossip frame: local dirty first, then forwarded
    /// dirty, then a rotating repair slice. Returns the number of handles
    /// pushed. Uses `selection_marks` for O(1) intra-frame dedup.
    pub fn fill_gossip_frame(
        &mut self,
        max_cells: usize,
        out: &mut Vec<CellHandle>,
    ) -> usize {
        out.clear();
        if max_cells == 0 {
            return 0;
        }
        self.bump_selection_epoch();

        // Lane 1: local dirty.
        let local_len = self.local_dirty.len;
        for offset in 0..local_len {
            if out.len() >= max_cells {
                break;
            }
            let entry = ring_entry_at(&self.local_dirty, offset);
            if !self.dirty_entry_current(entry) {
                continue;
            }
            if !self.mark_selected(entry.handle.index) {
                continue;
            }
            out.push(entry.handle);
        }
        if out.len() >= max_cells {
            return out.len();
        }

        // Lane 2: forwarded dirty.
        let forwarded_len = self.forwarded_dirty.len;
        for offset in 0..forwarded_len {
            if out.len() >= max_cells {
                break;
            }
            let entry = ring_entry_at(&self.forwarded_dirty, offset);
            if !self.dirty_entry_current(entry) {
                continue;
            }
            if !self.mark_selected(entry.handle.index) {
                continue;
            }
            out.push(entry.handle);
        }
        if out.len() >= max_cells {
            return out.len();
        }

        // Lane 3: rotating repair slice. selection_marks has already been
        // set for dirty cells in lanes 1+2, so repair cannot re-emit them.
        let cap = self.capacity;
        let mut visited = 0_u32;
        let mut next_cursor = self.repair_cursor;
        while visited < cap && out.len() < max_cells {
            let slot = (self.repair_cursor + visited) % cap;
            visited += 1;
            next_cursor = (slot + 1) % cap;
            if !self.is_active(slot) {
                continue;
            }
            if !self.mark_selected(slot) {
                continue;
            }
            out.push(self.handle_for(slot));
        }
        self.repair_cursor = next_cursor;
        out.len()
    }
}

/// Read a single ring entry by insertion-order offset. Function-local borrow
/// of the ring releases before the caller's mutating method calls, keeping
/// the hot send path allocation-free.
#[inline]
fn ring_entry_at(ring: &DirtyRing, offset: u32) -> DirtyEntry {
    let cap = ring.entries.len() as u32;
    let start = if ring.len == cap { ring.head } else { 0 };
    ring.entries[((start + offset) % cap) as usize]
}

#[cfg(test)]
mod tests;
