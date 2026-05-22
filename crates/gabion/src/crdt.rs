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
//! Expirations are surfaced symmetrically via [`ExpirationSink`] — one row
//! per freed cell — so an external aggregate store can keep its summary
//! consistent with the CRDT's active set.
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

mod dictionary;
mod dirty_ring;
mod hash;
mod index;
mod io;
mod peer_frontier;

#[cfg(test)]
mod tests;

pub use dictionary::{NodeDescriptor, NodeDictionary, RuleDescriptor, RuleDictionary};
pub use dirty_ring::{DirtyEntry, DirtyRing};
pub use index::CellIndex;
pub use io::{DeltaSink, ExpirationSink, Observation, ObservationBatch};
pub use peer_frontier::PeerFrontierTable;

use dictionary::pow2_index_capacity_for;
use dirty_ring::ring_entry_at;
use hash::hash_compact_cell_key;

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
/// the struct never carries a raw `NodeId` or `RuleId`. Internal to one
/// process — see [`CellIdentity`] for the portable identity exported through
/// [`DeltaSink`] / [`ExpirationSink`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CompactCellKey {
    pub rule: RuleSlot,
    pub key_hash: KeyHash,
    pub bucket: BucketEpoch,
    pub origin: NodeSlot,
    pub incarnation: Incarnation,
}

/// Portable cell identity used at the
/// [`AggregateStore`](crate::gossip::AggregateStore) boundary. The `RuleSlot`
/// interning index is node-local and not meaningful to downstream stores;
/// `rule_fingerprint` is the same on every node by construction. Origin
/// identity is intentionally omitted — the aggregate store keys on the cell's
/// `(rule_fingerprint, key_hash, bucket)` tuple and has no business
/// interpreting the originator.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct CellIdentity {
    pub rule_fingerprint: u128,
    pub key_hash: KeyHash,
    pub bucket: BucketEpoch,
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

/// One row appended to [`DeltaSink`] when a stored count rose. The `key`
/// carries the portable [`CellIdentity`] — downstream aggregate stores key on
/// `(rule_fingerprint, key_hash, bucket)` without ever seeing a node-local
/// slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellDelta<C: Count> {
    pub handle: CellHandle,
    pub key: CellIdentity,
    pub previous_count: C,
    pub current_count: C,
    pub delta: C,
    pub applies_locally: bool,
}

/// One row appended to [`ExpirationSink`] when a cell ages out.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellExpiration<C: Count> {
    pub handle: CellHandle,
    pub key: CellIdentity,
    pub last_count: C,
    pub last_update_millis: u64,
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

    // Scratch arrays for `expire_at` — sized to rule_dictionary capacity.
    // Reused across every call; never allocated past `new()`.
    expire_current_epoch_scratch: Box<[BucketEpoch]>,
    expire_live_buckets_scratch: Box<[u32]>,

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

/// Internal arguments for [`CellStore::upsert`]. The mode discriminates the
/// two valid update shapes; sharing a struct prevents callers from passing a
/// remote `observed` count alongside a local `hits` delta or vice versa.
struct UpsertSpec<C: Count> {
    rule_slot: RuleSlot,
    key_hash: KeyHash,
    bucket: BucketEpoch,
    origin_slot: NodeSlot,
    incarnation: Incarnation,
    now_millis: u64,
    mode: UpsertMode<C>,
}

enum UpsertMode<C: Count> {
    /// Local hit. The stored count rises by saturating-adding `hits`.
    Accumulate { hits: u64 },
    /// Remote observation. The stored count rises to `observed` if and only
    /// if `observed` is strictly greater (CRDT max-merge).
    MaxMerge { observed: C },
}

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

            expire_current_epoch_scratch: vec![0_u32; config.rule_dictionary_capacity as usize]
                .into_boxed_slice(),
            expire_live_buckets_scratch: vec![0_u32; config.rule_dictionary_capacity as usize]
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
            self.note_rule_dictionary_full(descriptor.fingerprint);
        }
        result
    }

    pub fn intern_node(&mut self, node_id: NodeId, incarnation: Incarnation) -> Option<NodeSlot> {
        let result = self.node_dictionary.intern(node_id, incarnation);
        if result.is_none() {
            self.note_node_dictionary_full(node_id, incarnation);
        }
        result
    }

    // -- Capacity-pressure warnings -----------------------------------------
    //
    // Each helper bumps the relevant rejection counter and emits a
    // `tracing::warn!` only on power-of-two thresholds (1, 2, 4, 8, ...) so
    // log volume stays bounded at ~log2(N) lines regardless of how many rows
    // are being dropped per second. The message names the `CellStoreConfig`
    // field the operator should raise.

    fn note_cell_store_full(&mut self) {
        self.cell_store_full_rejects = self.cell_store_full_rejects.saturating_add(1);
        if self.cell_store_full_rejects.is_power_of_two() {
            tracing::warn!(
                rejected_total = self.cell_store_full_rejects,
                capacity = self.capacity,
                in_use = self.active_len,
                config_key = "storage.max_cells",
                "Too many distinct rate-limit keys are being tracked at once. New keys are not \
                 being limited and will not contribute to global counts until older time buckets \
                 expire. To fix, raise `storage.max_cells` in your gabion config (currently {}).",
                self.capacity,
            );
        }
    }

    fn note_rule_dictionary_full(&mut self, rule_fingerprint: u128) {
        self.rule_dictionary_full_rejects = self.rule_dictionary_full_rejects.saturating_add(1);
        if self.rule_dictionary_full_rejects.is_power_of_two() {
            tracing::warn!(
                rejected_total = self.rule_dictionary_full_rejects,
                capacity = self.rule_dictionary.capacity(),
                in_use = self.rule_dictionary.len(),
                rule_fingerprint = %format!("{:032x}", rule_fingerprint),
                config_key = "storage.rule_dictionary_capacity",
                "Too many distinct rate-limit rules are in flight. New \
                 rules cannot be registered on this node and will not \
                 enforce limits here. To fix, raise \
                 `storage.rule_dictionary_capacity` in your gabion config \
                 (currently {}).",
                self.rule_dictionary.capacity(),
            );
        }
    }

    fn note_node_dictionary_full(&mut self, node_id: NodeId, incarnation: Incarnation) {
        self.node_dictionary_full_rejects = self.node_dictionary_full_rejects.saturating_add(1);
        if self.node_dictionary_full_rejects.is_power_of_two() {
            tracing::warn!(
                rejected_total = self.node_dictionary_full_rejects,
                capacity = self.node_dictionary.capacity(),
                in_use = self.node_dictionary.len(),
                peer_node_id = %format!("{:032x}", node_id.0),
                peer_incarnation = incarnation,
                config_key = "storage.node_dictionary_capacity",
                "Too many gabion peer instances are being tracked (every \
                 peer restart counts as a new instance until its data \
                 ages out). Counts from some peers are now being dropped, \
                 so cluster-wide rate limits will under-count requests \
                 handled by those peers. To fix, raise \
                 `storage.node_dictionary_capacity` in your gabion config \
                 (currently {}); size it comfortably above 2× the \
                 cluster's peer count to absorb rolling restarts.",
                self.node_dictionary.capacity(),
            );
        }
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
        let descriptor = self
            .rule_dictionary
            .descriptor(rule_slot)
            .expect("rule slot live during emit_delta — pinned by cell refcount");
        let key = CellIdentity {
            rule_fingerprint: descriptor.fingerprint,
            key_hash: self.key_hashes[slot as usize],
            bucket: self.buckets[slot as usize],
        };
        sink.push(
            handle,
            key,
            previous,
            current,
            delta,
            descriptor.applies_locally(),
        );
    }

    /// Append a row to the SoA expiration sink. Must run before
    /// `free_cell_at`, since the latter decrements the rule descriptor's
    /// refcount and may invalidate the `applies_locally` lookup.
    fn emit_expiration(&self, sink: &mut ExpirationSink<C>, slot: u32) {
        let rule_slot = self.rules[slot as usize];
        let handle = self.handle_for(slot);
        let descriptor = self
            .rule_dictionary
            .descriptor(rule_slot)
            .expect("rule slot live during emit_expiration — pinned by cell refcount");
        let key = CellIdentity {
            rule_fingerprint: descriptor.fingerprint,
            key_hash: self.key_hashes[slot as usize],
            bucket: self.buckets[slot as usize],
        };
        sink.push(
            handle,
            key,
            self.counts[slot as usize],
            self.last_update_millis[slot as usize],
            descriptor.applies_locally(),
        );
    }

    /// Insert or update a row: returns the resulting handle and whether the
    /// stored count rose.
    fn upsert(
        &mut self,
        spec: UpsertSpec<C>,
        sink: &mut DeltaSink<C>,
    ) -> Result<UpdateOutcome<C>, InsertReject> {
        let UpsertSpec {
            rule_slot,
            key_hash,
            bucket,
            origin_slot,
            incarnation,
            now_millis,
            mode,
        } = spec;
        let key = CompactCellKey {
            rule: rule_slot,
            key_hash,
            bucket,
            origin: origin_slot,
            incarnation,
        };
        if let Some(slot) = self.lookup_index(key) {
            let previous = self.counts[slot as usize];
            let next = match mode {
                UpsertMode::Accumulate { hits } => previous.saturating_add_hits(hits),
                UpsertMode::MaxMerge { observed } => {
                    if observed > previous {
                        observed
                    } else {
                        previous
                    }
                }
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
                self.note_cell_store_full();
                return Err(InsertReject::CellStoreFull);
            }
        };
        let initial = match mode {
            UpsertMode::Accumulate { hits } => C::default().saturating_add_hits(hits),
            UpsertMode::MaxMerge { observed } => observed,
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

        self.index
            .insert_unchecked(hash_compact_cell_key(&key), slot);

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
                        self.note_rule_dictionary_full(rule_fingerprint);
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
                    self.note_node_dictionary_full(node_id, incarnation);
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
                            self.note_rule_dictionary_full(rule_fingerprint);
                            continue;
                        }
                    }
                }
            };
            let hits_u64: u64 = obs.counts[i].into();
            let _ = self.upsert(
                UpsertSpec {
                    rule_slot,
                    key_hash: obs.key_hashes[i],
                    bucket: obs.buckets[i],
                    origin_slot: self.local_node_slot,
                    incarnation: self.local_identity.incarnation,
                    now_millis: obs.last_update_millis[i],
                    mode: UpsertMode::Accumulate { hits: hits_u64 },
                },
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
                UpsertSpec {
                    rule_slot,
                    key_hash: obs.key_hashes[i],
                    bucket: obs.buckets[i],
                    origin_slot: node_slot,
                    incarnation: obs.incarnations[i],
                    now_millis: obs.last_update_millis[i],
                    mode: UpsertMode::MaxMerge {
                        observed: obs.counts[i],
                    },
                },
                sink,
            );
        }
    }

    /// Free cells whose bucket has aged out of the per-rule live window.
    ///
    /// For each rule slot `r`, the cell is kept iff
    /// `buckets[slot] + live_buckets[r] >= current_epoch_by_rule[r]`. Each
    /// freed cell is emitted into `sink` *before* its slot is released, so
    /// external aggregators see one row per active-set departure.
    pub fn expire(
        &mut self,
        current_epoch_by_rule: &[BucketEpoch],
        live_buckets: &[u32],
        sink: &mut ExpirationSink<C>,
    ) {
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
                self.emit_expiration(sink, slot);
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
    pub fn fill_gossip_frame(&mut self, max_cells: usize, out: &mut Vec<CellHandle>) -> usize {
        out.clear();
        if max_cells == 0 {
            return 0;
        }
        self.bump_selection_epoch();

        // Lane 1: local dirty.
        let local_len = self.local_dirty.len();
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
        let forwarded_len = self.forwarded_dirty.len();
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

    /// Convenience wrapper around [`Self::expire`] that derives the per-rule
    /// epoch and live-bucket arrays from the current wall-clock time and each
    /// rule's descriptor. Uses scratch storage allocated at construction time
    /// so the call is allocation-free.
    pub fn expire_at(&mut self, now_millis: u64, sink: &mut ExpirationSink<C>) {
        let dict_cap = self.rule_dictionary.capacity() as usize;
        debug_assert_eq!(self.expire_current_epoch_scratch.len(), dict_cap);
        debug_assert_eq!(self.expire_live_buckets_scratch.len(), dict_cap);

        for slot in 0..dict_cap {
            let (current, live) = match self.rule_dictionary.descriptor(slot as RuleSlot) {
                Some(d) if d.bucket_millis > 0 => {
                    let current = (now_millis / d.bucket_millis as u64) as BucketEpoch;
                    let live = (d.window_millis / d.bucket_millis).max(1);
                    (current, live)
                }
                _ => (0, 0),
            };
            self.expire_current_epoch_scratch[slot] = current;
            self.expire_live_buckets_scratch[slot] = live;
        }

        // Swap the scratch slices out so we can pass them as `&[T]` borrows
        // to `self.expire(...)` (which needs `&mut self`). `Box::<[T]>::default`
        // returns an empty boxed slice that does not allocate.
        let current = std::mem::take(&mut self.expire_current_epoch_scratch);
        let live = std::mem::take(&mut self.expire_live_buckets_scratch);
        self.expire(&current, &live, sink);
        self.expire_current_epoch_scratch = current;
        self.expire_live_buckets_scratch = live;
    }

    /// Peer-aware sibling of [`Self::fill_gossip_frame`]. Walks the same
    /// three lanes (local dirty → forwarded dirty → repair) in the same
    /// order, dedup'd via `selection_marks`, but skips any cell whose
    /// `origin_sequence` is already at or below the peer's recorded
    /// `last_acked` for that origin slot.
    pub fn fill_gossip_frame_for_peer(
        &mut self,
        max_cells: usize,
        peer_slot: u16,
        out: &mut Vec<CellHandle>,
    ) -> usize {
        out.clear();
        if max_cells == 0 {
            return 0;
        }
        self.bump_selection_epoch();

        // Lane 1: local dirty.
        let local_len = self.local_dirty.len();
        for offset in 0..local_len {
            if out.len() >= max_cells {
                break;
            }
            let entry = ring_entry_at(&self.local_dirty, offset);
            if !self.dirty_entry_current(entry) {
                continue;
            }
            let i = entry.handle.index as usize;
            if !self.peer_lacks(peer_slot, i) {
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
        let forwarded_len = self.forwarded_dirty.len();
        for offset in 0..forwarded_len {
            if out.len() >= max_cells {
                break;
            }
            let entry = ring_entry_at(&self.forwarded_dirty, offset);
            if !self.dirty_entry_current(entry) {
                continue;
            }
            let i = entry.handle.index as usize;
            if !self.peer_lacks(peer_slot, i) {
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

        // Lane 3: rotating repair slice.
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
            if !self.peer_lacks(peer_slot, slot as usize) {
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

    #[inline]
    fn peer_lacks(&self, peer_slot: u16, cell_index: usize) -> bool {
        let origin = self.origins[cell_index];
        let last = self.peer_frontiers.last_acked(peer_slot, origin);
        self.origin_sequences[cell_index] > last
    }
}
