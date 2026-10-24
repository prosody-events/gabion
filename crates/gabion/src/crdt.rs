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
//! A node's view of a `(rule, key, bucket)` is the sum of two counted cells:
//! its own hits (origin: the local identity) and its count of every other
//! node's hits (origin: [`OTHERS`], which never goes on the wire). The others
//! cell only ever rises, and only to a lower bound of the truth: a key
//! owner's total less this node's own hits ([`CellStore::merge_totals`]), or,
//! on the owner, the sum of the per-contributor cells it has folded
//! ([`CellStore::merge_remote`]). Per-contributor cells are exact (max-merged
//! per origin) but are the owner's bookkeeping: they never reach a sink.
//!
//! Every mutation that raises a counted cell appends one row to a
//! caller-owned [`DeltaSink`], whose SoA shape lets a higher-level
//! rate-limit aggregator fold deltas without per-record dispatch.
//! Expirations are surfaced symmetrically via [`ExpirationSink`] — one row
//! per freed counted cell — so an external aggregate store can keep its
//! summary consistent with the view.
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
use hash::{hash_compact_cell_key, hash_rule_key};

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

/// Origin of the cell in which a node counts every other node's hits on a
/// `(rule, key, bucket)`. Node-local: it is never sent on the wire, and a
/// peer row that claims it is ignored.
pub const OTHERS: NodeIdentity = NodeIdentity {
    node_id: NodeId(u128::MAX),
    incarnation: 0,
};

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

/// A node's view total of one bucket of a key: its own hits plus its count
/// of everyone else's. What a key's owner broadcasts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyTotal<C: Count> {
    pub rule: RuleSlot,
    pub key_hash: KeyHash,
    pub bucket: BucketEpoch,
    pub count: C,
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
    /// Capacity of the runtime's sync queue: local cells awaiting an owner's
    /// acknowledgement.
    pub local_dirty_capacity: usize,
    /// Capacity of the runtime's broadcast queue: owned cells that changed
    /// since the last broadcast.
    pub forwarded_dirty_capacity: usize,
    /// Capacity of the runtime's membership table.
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
    // For an others cell, the sum of the contributor cells this node folded
    // into it as the key's owner; its count never falls below it. Unused
    // for every other cell.
    contributions: Box<[C]>,

    // Slot lifecycle.
    generations: Box<[u32]>,
    free_next: Box<[u32]>,
    free_head: u32,
    active_len: u32,
    capacity: u32,

    // Identity index.
    index: CellIndex,

    // Every cell of one `(rule, key)` (all buckets, all origins) is linked
    // into a doubly linked chain, so an owner can gather a key's totals
    // without scanning the store. `key_heads` maps `(rule, key)` to the
    // chain's head slot.
    key_heads: CellIndex,
    key_next: Box<[u32]>,
    key_prev: Box<[u32]>,

    // Per-origin sequence allocator (indexed by NodeSlot).
    next_sequence_by_origin: Box<[u64]>,

    // Scratch arrays for `expire_at` — sized to rule_dictionary capacity.
    // Reused across every call; never allocated past `new()`.
    expire_current_epoch_scratch: Box<[BucketEpoch]>,
    expire_live_buckets_scratch: Box<[u32]>,

    // Identity dictionaries.
    rule_dictionary: RuleDictionary,
    node_dictionary: NodeDictionary,

    // Construction-time sizing, read back by the runtime to size its queues.
    config: CellStoreConfig,

    // Local identity.
    local_identity: NodeIdentity,
    local_node_slot: NodeSlot,
    others_node_slot: NodeSlot,

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
            config.node_dictionary_capacity >= 2,
            "node dictionary capacity must hold the local and others identities (>= 2)"
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
            .expect("node dictionary capacity >= 2");
        debug_assert_ne!(local_identity, OTHERS, "the local identity is reserved");
        let others_node_slot = node_dictionary
            .intern(OTHERS.node_id, OTHERS.incarnation)
            .expect("node dictionary capacity >= 2");
        // Pin both slots so they cannot be freed via dec_ref.
        node_dictionary.inc_ref(local_node_slot);
        node_dictionary.inc_ref(others_node_slot);

        Self {
            rules: vec![0_u16; cap as usize].into_boxed_slice(),
            key_hashes: vec![KeyHash(0); cap as usize].into_boxed_slice(),
            buckets: vec![0_u32; cap as usize].into_boxed_slice(),
            origins: vec![0_u16; cap as usize].into_boxed_slice(),
            incarnations: vec![0_u32; cap as usize].into_boxed_slice(),

            counts: vec![C::default(); cap as usize].into_boxed_slice(),
            last_update_millis: vec![0_u64; cap as usize].into_boxed_slice(),
            origin_sequences: vec![0_u64; cap as usize].into_boxed_slice(),
            contributions: vec![C::default(); cap as usize].into_boxed_slice(),

            generations: vec![0_u32; cap as usize].into_boxed_slice(),
            free_next,
            free_head: 0,
            active_len: 0,
            capacity: cap,

            index: CellIndex::with_capacity(index_cap),

            key_heads: CellIndex::with_capacity(index_cap),
            key_next: vec![NO_FREE; cap as usize].into_boxed_slice(),
            key_prev: vec![NO_FREE; cap as usize].into_boxed_slice(),

            next_sequence_by_origin: vec![0_u64; config.node_dictionary_capacity as usize]
                .into_boxed_slice(),

            expire_current_epoch_scratch: vec![0_u32; config.rule_dictionary_capacity as usize]
                .into_boxed_slice(),
            expire_live_buckets_scratch: vec![0_u32; config.rule_dictionary_capacity as usize]
                .into_boxed_slice(),

            rule_dictionary: RuleDictionary::with_capacity(config.rule_dictionary_capacity),
            node_dictionary,

            config,

            local_identity,
            local_node_slot,
            others_node_slot,

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

    pub fn others_node_slot(&self) -> NodeSlot {
        self.others_node_slot
    }

    /// Whether cells of `origin` are part of this node's view: its own hits
    /// and its count of the others'. Contributor cells are not.
    fn in_view(&self, origin: NodeSlot) -> bool {
        origin == self.local_node_slot || origin == self.others_node_slot
    }

    pub fn rule_dictionary(&self) -> &RuleDictionary {
        &self.rule_dictionary
    }

    pub fn node_dictionary(&self) -> &NodeDictionary {
        &self.node_dictionary
    }

    pub fn config(&self) -> CellStoreConfig {
        self.config
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

    /// The cell that row `row` of `obs` names, if this store holds it.
    pub fn find_observed(&self, obs: &ObservationBatch<C>, row: usize) -> Option<CellHandle> {
        self.find(CompactCellKey {
            rule: self.rule_dictionary.find(obs.rule_fingerprints[row])?,
            key_hash: obs.key_hashes[row],
            bucket: obs.buckets[row],
            origin: self
                .node_dictionary
                .find(obs.origin_node_ids[row], obs.incarnations[row])?,
            incarnation: obs.incarnations[row],
        })
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

    /// Every live cell of `(rule, key_hash)`, whatever its bucket or origin.
    pub fn key_cells(
        &self,
        rule: RuleSlot,
        key_hash: KeyHash,
    ) -> impl Iterator<Item = CellHandle> + '_ {
        let head = self.find_key_head(rule, key_hash);
        std::iter::successors(head, |&slot| {
            Some(self.key_next[slot as usize]).filter(|&next| next != NO_FREE)
        })
        .map(|slot| self.handle_for(slot))
    }

    fn find_key_head(&self, rule: RuleSlot, key_hash: KeyHash) -> Option<u32> {
        self.key_heads.find(hash_rule_key(rule, key_hash), |slot| {
            self.rules[slot as usize] == rule && self.key_hashes[slot as usize] == key_hash
        })
    }

    /// Add the freshly filled `slot` to its key's chain.
    fn link_key(&mut self, slot: u32) {
        let rule = self.rules[slot as usize];
        let key_hash = self.key_hashes[slot as usize];
        match self.find_key_head(rule, key_hash) {
            Some(head) => {
                // Splice in behind the head so the head, and with it the
                // `key_heads` entry, stays put.
                let next = self.key_next[head as usize];
                self.key_next[slot as usize] = next;
                self.key_prev[slot as usize] = head;
                self.key_next[head as usize] = slot;
                if next != NO_FREE {
                    self.key_prev[next as usize] = slot;
                }
            }
            None => {
                self.key_next[slot as usize] = NO_FREE;
                self.key_prev[slot as usize] = NO_FREE;
                self.key_heads
                    .insert_unchecked(hash_rule_key(rule, key_hash), slot);
            }
        }
    }

    /// Remove `slot` from its key's chain.
    fn unlink_key(&mut self, slot: u32) {
        let next = self.key_next[slot as usize];
        let prev = self.key_prev[slot as usize];
        if prev != NO_FREE {
            self.key_next[prev as usize] = next;
            if next != NO_FREE {
                self.key_prev[next as usize] = prev;
            }
            return;
        }
        let h = hash_rule_key(self.rules[slot as usize], self.key_hashes[slot as usize]);
        self.key_heads.remove(h, slot);
        if next != NO_FREE {
            self.key_prev[next as usize] = NO_FREE;
            self.key_heads.insert_unchecked(h, next);
        }
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
        self.unlink_key(slot);
        self.rule_dictionary.dec_ref(rule);
        let freed_node = self.node_dictionary.dec_ref(origin);
        if freed_node {
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

    /// Append a row to the SoA delta sink.
    fn emit_delta(&self, sink: &mut DeltaSink<C>, slot: u32, previous: C, current: C) {
        let handle = self.handle_for(slot);
        let rule_slot = self.rules[slot as usize];
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
            C::saturating_delta(current, previous),
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

    /// The slot of the cell `key`, inserted at count zero if absent.
    fn find_or_insert(&mut self, key: CompactCellKey) -> Result<u32, InsertReject> {
        if let Some(slot) = self.lookup_index(key) {
            return Ok(slot);
        }
        let Some(slot) = self.alloc_slot() else {
            self.note_cell_store_full();
            return Err(InsertReject::CellStoreFull);
        };
        let i = slot as usize;
        self.rules[i] = key.rule;
        self.key_hashes[i] = key.key_hash;
        self.buckets[i] = key.bucket;
        self.origins[i] = key.origin;
        self.incarnations[i] = key.incarnation;
        self.counts[i] = C::default();
        self.contributions[i] = C::default();
        self.last_update_millis[i] = 0;
        self.origin_sequences[i] = 0;

        // Hold dictionary references on behalf of this cell.
        self.rule_dictionary.inc_ref(key.rule);
        self.node_dictionary.inc_ref(key.origin);

        self.index
            .insert_unchecked(hash_compact_cell_key(&key), slot);
        self.link_key(slot);
        Ok(slot)
    }

    /// Raise the cell at `slot` to `next`, if that is higher, and report a
    /// cell in the view to `sink`. Returns the rise.
    fn raise(&mut self, slot: u32, next: C, now_millis: u64, sink: &mut DeltaSink<C>) -> C {
        let i = slot as usize;
        let previous = self.counts[i];
        if next <= previous {
            return C::default();
        }
        self.counts[i] = next;
        self.last_update_millis[i] = now_millis;
        self.origin_sequences[i] = self.next_origin_sequence(self.origins[i]);
        if self.in_view(self.origins[i]) {
            self.emit_delta(sink, slot, previous, next);
        }
        C::saturating_delta(next, previous)
    }

    /// The slot of this node's others cell for `(rule, key_hash, bucket)`.
    fn others_slot(
        &mut self,
        rule: RuleSlot,
        key_hash: KeyHash,
        bucket: BucketEpoch,
    ) -> Result<u32, InsertReject> {
        self.find_or_insert(CompactCellKey {
            rule,
            key_hash,
            bucket,
            origin: self.others_node_slot,
            incarnation: OTHERS.incarnation,
        })
    }

    /// The count of the cell `(rule, key_hash, bucket, origin)`, zero if
    /// absent.
    fn count_at(
        &self,
        rule: RuleSlot,
        key_hash: KeyHash,
        bucket: BucketEpoch,
        origin: NodeSlot,
    ) -> C {
        let incarnation = if origin == self.others_node_slot {
            OTHERS.incarnation
        } else {
            self.local_identity.incarnation
        };
        self.lookup_index(CompactCellKey {
            rule,
            key_hash,
            bucket,
            origin,
            incarnation,
        })
        .map_or_else(C::default, |slot| self.counts[slot as usize])
    }

    /// This node's view of `(rule, key_hash, bucket)`: its own hits plus its
    /// count of everyone else's.
    pub fn view(&self, rule: RuleSlot, key_hash: KeyHash, bucket: BucketEpoch) -> C {
        let own = self.count_at(rule, key_hash, bucket, self.local_node_slot);
        let others = self.count_at(rule, key_hash, bucket, self.others_node_slot);
        own.saturating_add_hits(others.into())
    }

    /// The view total of every live bucket of `(rule, key_hash)`, one row per
    /// bucket this node holds a counted cell for.
    pub fn key_totals(
        &self,
        rule: RuleSlot,
        key_hash: KeyHash,
    ) -> impl Iterator<Item = KeyTotal<C>> + '_ {
        self.key_cells(rule, key_hash)
            .filter_map(|handle| self.reported_total(handle.index))
    }

    /// The view total of every bucket of every key, one row per bucket this
    /// node holds a counted cell for. Walks the whole store.
    pub fn view_totals(&self) -> impl Iterator<Item = KeyTotal<C>> + '_ {
        (0..self.capacity)
            .filter(|&slot| self.is_active(slot))
            .filter_map(|slot| self.reported_total(slot))
    }

    /// The view total of the bucket of the cell at `slot`, if that cell is
    /// the one that reports it: the bucket's others cell, or its local cell
    /// when it has no others cell. Each bucket in the view has exactly one.
    fn reported_total(&self, slot: u32) -> Option<KeyTotal<C>> {
        let i = slot as usize;
        let (rule, key_hash, bucket) = (self.rules[i], self.key_hashes[i], self.buckets[i]);
        let origin = self.origins[i];
        let reports = origin == self.others_node_slot
            || (origin == self.local_node_slot
                && self.count_at(rule, key_hash, bucket, self.others_node_slot) == C::default());
        reports.then(|| KeyTotal {
            rule,
            key_hash,
            bucket,
            count: self.view(rule, key_hash, bucket),
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
        let rule_slot = self.translate_rule(rule_fingerprint)?;
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

    /// The slot of `rule_fingerprint`, interned with the default descriptor
    /// if unknown so its cells are still stored and expired (with
    /// `applies_locally = false`). `None`, with the rejection counter bumped,
    /// when the dictionary is full.
    fn translate_rule(&mut self, rule_fingerprint: u128) -> Option<RuleSlot> {
        if let Some(slot) = self.rule_dictionary.find(rule_fingerprint) {
            return Some(slot);
        }
        let descriptor = RuleDescriptor {
            fingerprint: rule_fingerprint,
            ..RuleDescriptor::default()
        };
        let slot = self.rule_dictionary.intern(descriptor);
        if slot.is_none() {
            self.note_rule_dictionary_full(rule_fingerprint);
        }
        slot
    }

    // -- Primary mutation entry points --------------------------------------

    /// Record locally-observed hits. The local node is always the origin —
    /// `origin_node_ids` / `incarnations` columns on the batch are ignored.
    pub fn ingest_local(&mut self, obs: &ObservationBatch<C>, sink: &mut DeltaSink<C>) {
        obs.assert_consistent();
        for i in 0..obs.len() {
            let Some(rule) = self.translate_rule(obs.rule_fingerprints[i]) else {
                continue;
            };
            let Ok(slot) = self.find_or_insert(CompactCellKey {
                rule,
                key_hash: obs.key_hashes[i],
                bucket: obs.buckets[i],
                origin: self.local_node_slot,
                incarnation: self.local_identity.incarnation,
            }) else {
                continue;
            };
            let next = self.counts[slot as usize].saturating_add_hits(obs.counts[i].into());
            self.raise(slot, next, obs.last_update_millis[i], sink);
        }
    }

    /// Fold contributors' cells into this node as the owner of their keys.
    /// Each row is max-merged into its origin's cell; whatever a cell rose
    /// by is added to the bucket's contributions, and the others cell rises
    /// to at least their sum. Only others cells reach `sink`. A row that
    /// claims this node's own identity, or [`OTHERS`], is ignored.
    pub fn merge_remote(&mut self, obs: &ObservationBatch<C>, sink: &mut DeltaSink<C>) {
        obs.assert_consistent();
        for i in 0..obs.len() {
            let origin = NodeIdentity::new(obs.origin_node_ids[i], obs.incarnations[i]);
            if origin == self.local_identity || origin == OTHERS {
                continue;
            }
            let Some((rule, node)) = self.translate_identity(
                obs.rule_fingerprints[i],
                origin.node_id,
                origin.incarnation,
            ) else {
                continue;
            };
            let (key_hash, bucket, now) =
                (obs.key_hashes[i], obs.buckets[i], obs.last_update_millis[i]);
            let Ok(slot) = self.find_or_insert(CompactCellKey {
                rule,
                key_hash,
                bucket,
                origin: node,
                incarnation: origin.incarnation,
            }) else {
                continue;
            };
            let rise = self.raise(slot, obs.counts[i], now, sink);
            if rise == C::default() {
                continue;
            }
            let Ok(others) = self.others_slot(rule, key_hash, bucket) else {
                continue;
            };
            let o = others as usize;
            self.contributions[o] = self.contributions[o].saturating_add_hits(rise.into());
            self.raise(others, self.contributions[o], now, sink);
        }
    }

    /// Fold key owners' totals. Each row's count is the owner's view total
    /// of `(rule, key, bucket)`; this node's others cell rises to that total
    /// less its own hits, a lower bound of everyone else's since the owner
    /// cannot hold more of this node's hits than it has. The rows' origin
    /// columns are ignored.
    pub fn merge_totals(&mut self, obs: &ObservationBatch<C>, sink: &mut DeltaSink<C>) {
        obs.assert_consistent();
        for i in 0..obs.len() {
            let Some(rule) = self.translate_rule(obs.rule_fingerprints[i]) else {
                continue;
            };
            let (key_hash, bucket) = (obs.key_hashes[i], obs.buckets[i]);
            let own = self.count_at(rule, key_hash, bucket, self.local_node_slot);
            let others = C::saturating_delta(obs.counts[i], own);
            if others <= self.count_at(rule, key_hash, bucket, self.others_node_slot) {
                continue;
            }
            if let Ok(slot) = self.others_slot(rule, key_hash, bucket) {
                self.raise(slot, others, obs.last_update_millis[i], sink);
            }
        }
    }

    /// Free cells whose bucket has aged out of the per-rule live window.
    ///
    /// For each rule slot `r`, the cell is kept iff
    /// `buckets[slot] + live_buckets[r] >= current_epoch_by_rule[r]`. Each
    /// freed cell in the view is emitted into `sink` *before* its slot is
    /// released, so external aggregators see one row per counted departure.
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
                if self.in_view(self.origins[slot as usize]) {
                    self.emit_expiration(sink, slot);
                }
                self.free_cell_at(slot);
            }
        }
    }

    /// Reset to an empty state — preserves the local identity registration
    /// but clears every other column. Mostly useful in tests.
    pub fn clear(&mut self) {
        // Free each active cell — this also decrements dictionary refcounts.
        for slot in 0..self.capacity {
            if self.is_active(slot) {
                self.free_cell_at(slot);
            }
        }
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
