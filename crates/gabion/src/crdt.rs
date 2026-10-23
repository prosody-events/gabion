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
//! Every cell carries the **version** its origin assigned when the cell last
//! changed. Cells of one origin are threaded onto a version-ordered intrusive
//! list, and each origin has a **watermark**: the highest version below which
//! this node holds every cell that still exists. The watermark is the entry
//! this node advertises in a Scuttlebutt digest; the list is how a peer's
//! digest is answered with exactly the cells it lacks, oldest first.
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
mod hash;
mod index;
mod io;

#[cfg(test)]
mod tests;

pub use dictionary::{NodeDescriptor, NodeDictionary, RuleDescriptor, RuleDictionary};
pub use index::CellIndex;
pub use io::{DeltaSink, ExpirationSink, Observation, ObservationBatch};

use dictionary::pow2_index_capacity_for;
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
    /// Unused: Scuttlebutt has no dirty rings. Kept so adapter and harness
    /// configuration keeps compiling.
    pub local_dirty_capacity: usize,
    /// Unused: see [`Self::local_dirty_capacity`].
    pub forwarded_dirty_capacity: usize,
    /// Unused: Scuttlebutt keeps no per-peer frontier. See
    /// [`Self::local_dirty_capacity`].
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

    // Per-origin version log. Cells of one origin form a doubly linked list
    // in ascending `origin_sequences` order; `log_tail` (indexed by NodeSlot)
    // is its newest cell.
    log_prev: Box<[u32]>,
    log_next: Box<[u32]>,
    log_tail: Box<[u32]>,

    // Per-origin watermark (indexed by NodeSlot): every version at or below
    // it either is held or no longer exists. For the local origin it is also
    // the version allocator.
    watermarks: Box<[u64]>,

    // Scratch arrays for `expire_at` — sized to rule_dictionary capacity.
    // Reused across every call; never allocated past `new()`.
    expire_current_epoch_scratch: Box<[BucketEpoch]>,
    expire_live_buckets_scratch: Box<[u32]>,

    // Identity dictionaries.
    rule_dictionary: RuleDictionary,
    node_dictionary: NodeDictionary,

    // Local identity.
    local_identity: NodeIdentity,
    local_node_slot: NodeSlot,

    // Counters.
    cell_store_full_rejects: u64,
    rule_dictionary_full_rejects: u64,
    node_dictionary_full_rejects: u64,
}

/// Null link for the slot freelist and the per-origin version log.
const NIL: u32 = u32::MAX;

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
    /// if `observed` is strictly greater (CRDT max-merge), and the cell takes
    /// the `version` its origin assigned to that change.
    MaxMerge { observed: C, version: u64 },
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

        // Build the intrusive freelist: 0 -> 1 -> 2 -> ... -> cap-1 -> NIL
        let mut free_next = vec![NIL; cap as usize].into_boxed_slice();
        for i in 0..cap {
            free_next[i as usize] = if i + 1 < cap { i + 1 } else { NIL };
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

            log_prev: vec![NIL; cap as usize].into_boxed_slice(),
            log_next: vec![NIL; cap as usize].into_boxed_slice(),
            log_tail: vec![NIL; config.node_dictionary_capacity as usize].into_boxed_slice(),
            watermarks: vec![0_u64; config.node_dictionary_capacity as usize].into_boxed_slice(),

            expire_current_epoch_scratch: vec![0_u32; config.rule_dictionary_capacity as usize]
                .into_boxed_slice(),
            expire_live_buckets_scratch: vec![0_u32; config.rule_dictionary_capacity as usize]
                .into_boxed_slice(),

            rule_dictionary: RuleDictionary::with_capacity(config.rule_dictionary_capacity),
            node_dictionary,

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

    /// Register a configured rule and **pin** it: the first registration holds
    /// one persistent reference so the slot survives cell churn. Without the
    /// pin, a configured rule's descriptor was released the moment its last
    /// cell aged out (cell refs were the only refs), and the next hit
    /// re-interned the [`RuleDescriptor::default()`] fallback — a 60 s window
    /// with `applies_locally = false` — so the rule silently stopped expiring
    /// on its real window and stopped counting locally. Cells interned via
    /// gossip/local fallback (an *unconfigured*, wire-only rule) are not pinned
    /// and still free when their cells expire, so stale peer rules can't
    /// accumulate. Re-registering an already-known rule refreshes its
    /// descriptor without adding a second pin.
    ///
    /// Edge case: if a rule were first learned wire-only (via gossip) and only
    /// later registered here, the `already_known` guard would skip the pin and
    /// leave it freeable. That ordering doesn't occur in practice — every
    /// adapter calls `intern_rule` for its configured rules at startup, before
    /// the gossip runtime spawns — so the pin always lands on a fresh slot.
    pub fn intern_rule(&mut self, descriptor: RuleDescriptor) -> Option<RuleSlot> {
        let already_known = self.rule_dictionary.find(descriptor.fingerprint).is_some();
        let result = self.rule_dictionary.intern(descriptor);
        match result {
            Some(slot) => {
                if !already_known {
                    self.rule_dictionary.inc_ref(slot);
                }
                Some(slot)
            }
            None => {
                self.note_rule_dictionary_full(descriptor.fingerprint);
                None
            }
        }
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
        if self.free_head == NIL {
            return None;
        }
        let slot = self.free_head;
        self.free_head = self.free_next[slot as usize];
        self.free_next[slot as usize] = NIL;
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
        self.log_unlink(slot, origin);
        self.rule_dictionary.dec_ref(rule);
        let freed_node = self.node_dictionary.dec_ref(origin);
        if freed_node {
            self.watermarks[origin as usize] = 0;
        }
        self.free_slot(slot);
    }

    // -- Update primitives --------------------------------------------------

    /// Allocate the next version for a locally originated change.
    fn next_local_version(&mut self) -> u64 {
        let w = &mut self.watermarks[self.local_node_slot as usize];
        *w = w.saturating_add(1);
        *w
    }

    /// Thread `slot` onto `origin`'s version log at the position its
    /// `origin_sequences` entry dictates. Local changes and in-order remote
    /// changes land on the tail in O(1); a reordered remote change walks back
    /// past the newer cells that overtook it.
    fn log_link(&mut self, slot: u32, origin: NodeSlot) {
        let version = self.origin_sequences[slot as usize];
        let mut before = NIL;
        let mut after = self.log_tail[origin as usize];
        while after != NIL && self.origin_sequences[after as usize] > version {
            before = after;
            after = self.log_prev[after as usize];
        }
        self.log_prev[slot as usize] = after;
        self.log_next[slot as usize] = before;
        if after != NIL {
            self.log_next[after as usize] = slot;
        }
        if before == NIL {
            self.log_tail[origin as usize] = slot;
        } else {
            self.log_prev[before as usize] = slot;
        }
    }

    fn log_unlink(&mut self, slot: u32, origin: NodeSlot) {
        let after = self.log_prev[slot as usize];
        let before = self.log_next[slot as usize];
        if after != NIL {
            self.log_next[after as usize] = before;
        }
        if before == NIL {
            self.log_tail[origin as usize] = after;
        } else {
            self.log_prev[before as usize] = after;
        }
        self.log_prev[slot as usize] = NIL;
        self.log_next[slot as usize] = NIL;
    }

    /// The version this node advertises for `origin` in a digest: it holds
    /// every cell of `origin` at or below it that still exists.
    pub fn watermark(&self, origin: NodeSlot) -> u64 {
        self.watermarks[origin as usize]
    }

    /// Version of `origin`'s newest cell, or `0` when it has none.
    pub fn newest_version(&self, origin: NodeSlot) -> u64 {
        match self.log_tail[origin as usize] {
            NIL => 0,
            tail => self.origin_sequences[tail as usize],
        }
    }

    /// Append to `out` the cells of `origin` with a version above `after`, in
    /// ascending version order, stopping once `out` holds `limit` handles.
    /// Because the order is by version, the cells delivered for one origin
    /// are always a prefix of what it owes.
    pub fn collect_newer(
        &self,
        origin: NodeSlot,
        after: u64,
        limit: usize,
        out: &mut Vec<CellHandle>,
    ) {
        let mut first = NIL;
        let mut cursor = self.log_tail[origin as usize];
        while cursor != NIL && self.origin_sequences[cursor as usize] > after {
            first = cursor;
            cursor = self.log_prev[cursor as usize];
        }
        while first != NIL && out.len() < limit {
            out.push(self.handle_for(first));
            first = self.log_next[first as usize];
        }
    }

    /// How many versions below this cell's own the sender certifies as
    /// absent, for the wire's per-cell claim. A cell at or below the
    /// origin's watermark asserts that nothing exists between it and its
    /// predecessor on the log. A cell above the watermark sits past a
    /// possible hole, so it asserts only that nothing sits between it and
    /// the version just before it.
    pub(crate) fn claim_gap(&self, slot: u32) -> u32 {
        let version = self.origin_sequences[slot as usize];
        let origin = self.origins[slot as usize];
        if version > self.watermarks[origin as usize] {
            return 1;
        }
        let before = match self.log_prev[slot as usize] {
            NIL => 0,
            prev => self.origin_sequences[prev as usize],
        };
        (version - before).min(u32::MAX as u64) as u32
    }

    /// Advance `origin`'s watermark to `version` if the sender's claim ties
    /// it to what this node already holds: nothing exists strictly between
    /// `version - gap` and `version`, and the watermark has reached
    /// `version - gap`.
    fn advance_watermark(&mut self, origin: NodeSlot, version: u64, gap: u32) {
        let w = &mut self.watermarks[origin as usize];
        if version > *w && version.saturating_sub(gap as u64) <= *w {
            *w = version;
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
                UpsertMode::MaxMerge { observed, .. } => {
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
            let version = match mode {
                UpsertMode::Accumulate { .. } => self.next_local_version(),
                UpsertMode::MaxMerge { version, .. } => version,
            };
            self.log_unlink(slot, origin_slot);
            self.origin_sequences[slot as usize] = version;
            self.log_link(slot, origin_slot);
            let handle = self.handle_for(slot);
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
            UpsertMode::MaxMerge { observed, .. } => observed,
        };
        self.rules[slot as usize] = rule_slot;
        self.key_hashes[slot as usize] = key_hash;
        self.buckets[slot as usize] = bucket;
        self.origins[slot as usize] = origin_slot;
        self.incarnations[slot as usize] = incarnation;
        self.counts[slot as usize] = initial;
        self.last_update_millis[slot as usize] = now_millis;
        let version = match mode {
            UpsertMode::Accumulate { .. } => self.next_local_version(),
            UpsertMode::MaxMerge { version, .. } => version,
        };
        self.origin_sequences[slot as usize] = version;
        self.log_link(slot, origin_slot);

        // Hold dictionary references on behalf of this cell.
        self.rule_dictionary.inc_ref(rule_slot);
        self.node_dictionary.inc_ref(origin_slot);

        self.index
            .insert_unchecked(hash_compact_cell_key(&key), slot);

        let handle = self.handle_for(slot);
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
    /// are appended to `sink`. A row that was applied (or was already
    /// covered) also advances its origin's watermark when the row's claim
    /// chains onto it; a row the store had to reject never does, so a digest
    /// cannot advertise a version this node lacks.
    ///
    /// A row for a known rule whose bucket has already left the live window at
    /// `now_millis` is acknowledged without being stored. A peer that has not
    /// yet expired such a cell would otherwise hand it back the moment this
    /// node's last cell for the origin expired and its watermark reset.
    pub fn merge_remote(
        &mut self,
        obs: &ObservationBatch<C>,
        now_millis: u64,
        sink: &mut DeltaSink<C>,
    ) {
        obs.assert_consistent();
        for i in 0..obs.len() {
            if self.is_expired(obs.rule_fingerprints[i], obs.buckets[i], now_millis) {
                if let Some(origin) = self.find_node(obs.origin_node_ids[i], obs.incarnations[i]) {
                    self.advance_watermark(origin, obs.origin_sequences[i], obs.claim_gaps[i]);
                }
                continue;
            }
            let Some((rule_slot, node_slot)) = self.translate_identity(
                obs.rule_fingerprints[i],
                obs.origin_node_ids[i],
                obs.incarnations[i],
            ) else {
                continue;
            };
            let applied = self.upsert(
                UpsertSpec {
                    rule_slot,
                    key_hash: obs.key_hashes[i],
                    bucket: obs.buckets[i],
                    origin_slot: node_slot,
                    incarnation: obs.incarnations[i],
                    now_millis: obs.last_update_millis[i],
                    mode: UpsertMode::MaxMerge {
                        observed: obs.counts[i],
                        version: obs.origin_sequences[i],
                    },
                },
                sink,
            );
            if applied.is_ok() {
                self.advance_watermark(node_slot, obs.origin_sequences[i], obs.claim_gaps[i]);
            }
        }
    }

    /// Whether a cell of the configured or already-seen rule `fingerprint` in
    /// `bucket` is outside its live window at `now_millis`. `false` for a rule
    /// this node has never seen, whose window it cannot know.
    fn is_expired(&self, fingerprint: u128, bucket: BucketEpoch, now_millis: u64) -> bool {
        let Some(rule) = self
            .rule_dictionary
            .find(fingerprint)
            .and_then(|slot| self.rule_dictionary.descriptor(slot))
        else {
            return false;
        };
        (bucket as u64) + (rule.live_buckets() as u64) < (rule.current_epoch(now_millis) as u64)
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

    /// Reset to an empty state — preserves the local identity registration
    /// but clears every other column. Mostly useful in tests.
    pub fn clear(&mut self) {
        // Free each active cell — this also decrements dictionary refcounts
        // and unthreads the version logs.
        for slot in 0..self.capacity {
            if self.is_active(slot) {
                self.free_cell_at(slot);
            }
        }
        self.watermarks[self.local_node_slot as usize] = 0;
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
                Some(d) => (d.current_epoch(now_millis), d.live_buckets()),
                None => (0, 0),
            };
            self.expire_current_epoch_scratch[slot] = current;
            self.expire_live_buckets_scratch[slot] = live;
        }

        // Swap the scratch slices out so we can pass them as `&[T]` borrows
        // to `self.expire(...)` (which needs `&mut self`).
        // `Box::<[T]>::default` returns an empty boxed slice that does
        // not allocate.
        let current = std::mem::take(&mut self.expire_current_epoch_scratch);
        let live = std::mem::take(&mut self.expire_live_buckets_scratch);
        self.expire(&current, &live, sink);
        self.expire_current_epoch_scratch = current;
        self.expire_live_buckets_scratch = live;
    }
}
