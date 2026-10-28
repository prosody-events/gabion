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

#[cfg(test)]
mod tests;

pub use dictionary::{NodeDescriptor, NodeDictionary, RuleDescriptor, RuleDictionary};
pub use dirty_ring::{DirtyEntry, DirtyRing};
pub use io::{DeltaSink, ExpirationSink, Observation, ObservationBatch};

pub(crate) use dictionary::MAX_DICTIONARY_CAPACITY;
pub(crate) use dictionary::pow2_index_capacity_for;
use dirty_ring::ring_entry_at;
use hash::hash_compact_cell_key;
use index::CellIndex;

use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};

use crate::rules::{CardinalityLimits, RuleTable};
use crate::settings::Setting;
use crate::throttle;

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
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct KeyHash(#[cfg_attr(feature = "serde", serde(serialize_with = "crate::hex"))] pub u128);

/// A peer's stable identity.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct NodeId(#[cfg_attr(feature = "serde", serde(serialize_with = "crate::hex"))] pub u128);

/// Stable owner identity for cells created by one node incarnation.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
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
pub struct CellDelta {
    pub handle: CellHandle,
    pub key: CellIdentity,
    pub previous_count: u32,
    pub current_count: u32,
    pub delta: u32,
    pub applies_locally: bool,
}

/// One row appended to [`ExpirationSink`] when a cell ages out.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellExpiration {
    pub handle: CellHandle,
    pub key: CellIdentity,
    pub last_count: u32,
    pub last_update_millis: u64,
    pub applies_locally: bool,
}

/// Snapshot of one stored cell.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellRow {
    pub handle: CellHandle,
    pub key: CompactCellKey,
    pub count: u32,
    pub last_update_millis: u64,
    pub origin_sequence: u64,
}

/// Counters surfaced for observability.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
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

/// Where an expiry pass stands after one call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum Expiry {
    /// Every slot has been checked; the next call starts a new pass.
    Done,
    /// The sink is full: apply and clear it, then call again to resume.
    More,
}

// ---------------------------------------------------------------------------
// CellStore
// ---------------------------------------------------------------------------

/// The storage half of the validated settings: the cell store's capacities,
/// the rule-dictionary override, the matched-rule bound, and the cardinality
/// limits both adapters' admission enforces. Built only by
/// [`crate::settings::Settings::validate`].
#[derive(Clone, Copy, Debug)]
pub struct ValidStorageConfig {
    pub(crate) cell_capacity: NonZeroU32,
    pub(crate) rule_dictionary_capacity: Option<NonZeroU16>,
    pub(crate) max_matched_rules: NonZeroU16,
    pub(crate) node_dictionary_capacity: NonZeroU16,
    pub(crate) local_dirty_capacity: NonZeroU32,
    pub(crate) forwarded_dirty_capacity: NonZeroU32,
    /// The per-request descriptor limits both adapters' admission enforces.
    pub cardinality: CardinalityLimits,
}

impl ValidStorageConfig {
    /// `storage.rule_dictionary_capacity`, which overrides the default
    /// [`RuleTable::new`] derives from the rule count.
    pub fn rule_dictionary_capacity(&self) -> Option<NonZeroU16> {
        self.rule_dictionary_capacity
    }

    /// `storage.max_matched_rules`: the most rules one request is counted
    /// against, and so the size of each admission scratch.
    pub fn max_matched_rules(&self) -> NonZeroUsize {
        self.max_matched_rules.into()
    }
}

/// Single global structure-of-arrays cell store.
#[derive(Clone, Debug)]
pub struct CellStore {
    // Cold key columns — only touched on identity work.
    rules: Box<[RuleSlot]>,
    key_hashes: Box<[KeyHash]>,
    buckets: Box<[BucketEpoch]>,
    origins: Box<[NodeSlot]>,
    incarnations: Box<[Incarnation]>,

    // Hot value columns — touched on every merge.
    counts: Box<[u32]>,
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
    /// Slot where the next `expire` call resumes; 0 starts a pass.
    expire_cursor: u32,

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
struct UpsertSpec {
    rule_slot: RuleSlot,
    key_hash: KeyHash,
    bucket: BucketEpoch,
    origin_slot: NodeSlot,
    incarnation: Incarnation,
    now_millis: u64,
    mode: UpsertMode,
}

enum UpsertMode {
    /// Local hit. The stored count rises by saturating-adding `hits`.
    Accumulate { hits: u32 },
    /// Remote observation. The stored count rises to `observed` if and only
    /// if `observed` is strictly greater (CRDT max-merge).
    MaxMerge { observed: u32 },
}

impl CellStore {
    /// A store sized by `config`, with every rule of `rules` interned and
    /// pinned before any gossip runtime can exist.
    pub fn new(
        config: ValidStorageConfig,
        local_identity: NodeIdentity,
        rules: &RuleTable,
    ) -> Self {
        let cap = config.cell_capacity.get();
        let rule_capacity = usize::from(rules.dictionary_capacity().get());
        let node_capacity = config.node_dictionary_capacity.get();
        let index_cap = pow2_index_capacity_for(config.cell_capacity);

        // Build the intrusive freelist: 0 -> 1 -> 2 -> ... -> cap-1 -> NO_FREE
        let mut free_next = vec![NO_FREE; cap as usize].into_boxed_slice();
        for i in 0..cap {
            free_next[i as usize] = if i + 1 < cap { i + 1 } else { NO_FREE };
        }

        let mut node_dictionary = NodeDictionary::with_capacity(config.node_dictionary_capacity);
        let local_node_slot = node_dictionary
            .intern(local_identity.node_id, local_identity.incarnation)
            .expect("a fresh dictionary with a non-zero capacity has a free slot");
        // Pin the local slot so it cannot be freed via dec_ref.
        node_dictionary.inc_ref(local_node_slot);

        let mut store = Self {
            rules: vec![0_u16; cap as usize].into_boxed_slice(),
            key_hashes: vec![KeyHash(0); cap as usize].into_boxed_slice(),
            buckets: vec![0_u32; cap as usize].into_boxed_slice(),
            origins: vec![0_u16; cap as usize].into_boxed_slice(),
            incarnations: vec![0_u32; cap as usize].into_boxed_slice(),

            counts: vec![0_u32; cap as usize].into_boxed_slice(),
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
            expire_cursor: 0,

            selection_marks: vec![0_u32; cap as usize].into_boxed_slice(),
            selection_epoch: 0,

            next_sequence_by_origin: vec![0_u64; usize::from(node_capacity)].into_boxed_slice(),

            expire_current_epoch_scratch: vec![0_u32; rule_capacity].into_boxed_slice(),
            expire_live_buckets_scratch: vec![0_u32; rule_capacity].into_boxed_slice(),

            rule_dictionary: RuleDictionary::with_capacity(rules.dictionary_capacity()),
            node_dictionary,

            local_identity,
            local_node_slot,

            cell_store_full_rejects: 0,
            rule_dictionary_full_rejects: 0,
            node_dictionary_full_rejects: 0,
        };
        for rule in rules.rules() {
            let slot = store.intern_rule(rule.descriptor());
            debug_assert!(
                slot.is_some(),
                "`RuleTable::new` sizes the dictionary for every rule and keeps fingerprints \
                 unique"
            );
        }
        store
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

    pub fn local_dirty(&self) -> &DirtyRing {
        &self.local_dirty
    }

    pub fn forwarded_dirty(&self) -> &DirtyRing {
        &self.forwarded_dirty
    }

    pub fn repair_cursor(&self) -> u32 {
        self.repair_cursor
    }

    #[cfg(test)]
    pub(crate) fn index(&self) -> &CellIndex {
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

    // -- Rule registration --

    /// Intern and pin a configured rule: the pin keeps its slot, real window
    /// and `applies_locally` through cell churn, where a wire-only rule's slot
    /// frees with its last cell. Only [`Self::new`] calls it, on a fresh
    /// dictionary.
    pub(crate) fn intern_rule(&mut self, descriptor: RuleDescriptor) -> Option<RuleSlot> {
        debug_assert!(
            self.rule_dictionary.find(descriptor.fingerprint).is_none(),
            "a configured rule is interned once"
        );
        let slot = self.rule_dictionary.intern(descriptor)?;
        self.rule_dictionary.inc_ref(slot);
        Some(slot)
    }

    // -- Capacity-pressure warnings -----------------------------------------
    //
    // Each helper counts through `crate::throttle` and names its setting in
    // both adapters' spellings.

    fn note_cell_store_full(&mut self) {
        if let Some(rejected_total) = throttle::bump(&mut self.cell_store_full_rejects) {
            tracing::warn!(
                code = "cell_store_full",
                rejected_total,
                capacity = self.capacity,
                in_use = self.active_len,
                config_key = Setting::StorageMaxCells.yaml_key(),
                "Too many distinct rate-limit keys are being tracked at once. New keys were \
                 allowed and are not counted until older time buckets expire, so their limits \
                 under-count. Usually many clients, an attack spraying keys, or a store smaller \
                 than the number of keys live within the rules' windows. Raise {}.",
                Setting::StorageMaxCells,
            );
        }
    }

    fn note_rule_dictionary_full(&mut self, rule_fingerprint: u128) {
        if let Some(rejected_total) = throttle::bump(&mut self.rule_dictionary_full_rejects) {
            tracing::warn!(
                code = "rule_dictionary_full",
                rejected_total,
                capacity = self.rule_dictionary.capacity(),
                in_use = self.rule_dictionary.len(),
                rule_fingerprint = %format!("{:032x}", rule_fingerprint),
                config_key = Setting::StorageRuleDictionaryCapacity.yaml_key(),
                "A rate-limit rule this node does not configure, gossiped by a peer or \
                 recorded by a worker still on an earlier configuration, found the rule \
                 dictionary full, so this node drops its hits and that rule under-counts across \
                 the cluster. Usually more rules are live across the cluster than the \
                 dictionary holds, for example during a configuration rollout. The default is \
                 twice the configured rules, at least 256; set {} only above that.",
                Setting::StorageRuleDictionaryCapacity,
            );
        }
    }

    fn note_node_dictionary_full(&mut self, node_id: NodeId, incarnation: Incarnation) {
        if let Some(rejected_total) = throttle::bump(&mut self.node_dictionary_full_rejects) {
            tracing::warn!(
                code = "node_dictionary_full",
                rejected_total,
                capacity = self.node_dictionary.capacity(),
                in_use = self.node_dictionary.len(),
                peer_node_id = %format!("{:032x}", node_id.0),
                peer_incarnation = incarnation,
                config_key = Setting::StorageNodeDictionaryCapacity.yaml_key(),
                "Too many gabion peer instances are being tracked (every peer restart counts \
                 as a new instance until its data ages out). Counts from some peers are now \
                 being dropped, so cluster-wide rate limits will under-count requests handled \
                 by those peers. Raise {}; size it above pods × (1 + restarts within your \
                 longest rule window).",
                Setting::StorageNodeDictionaryCapacity,
            );
        }
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

    pub fn get(&self, handle: CellHandle) -> Option<CellRow> {
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
        sink: &mut DeltaSink,
        slot: u32,
        previous: u32,
        current: u32,
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
            // Both updates only raise a count, so this cannot underflow.
            current - previous,
            descriptor.applies_locally(),
        );
    }

    /// Append a row to the SoA expiration sink. Must run before
    /// `free_cell_at`, since the latter decrements the rule descriptor's
    /// refcount and may invalidate the `applies_locally` lookup.
    fn emit_expiration(&self, sink: &mut ExpirationSink, slot: u32) {
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

    /// Insert or update a row, emitting a delta when the stored count rose.
    fn upsert(&mut self, spec: UpsertSpec, sink: &mut DeltaSink) {
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
                UpsertMode::Accumulate { hits } => previous.saturating_add(hits),
                UpsertMode::MaxMerge { observed } => observed.max(previous),
            };
            if next == previous {
                return;
            }
            self.counts[slot as usize] = next;
            self.last_update_millis[slot as usize] = now_millis;
            let seq = self.next_origin_sequence(origin_slot);
            self.origin_sequences[slot as usize] = seq;
            let handle = self.handle_for(slot);
            self.push_dirty(origin_slot, handle, seq);
            self.emit_delta(sink, slot, previous, next, rule_slot);
            return;
        }

        // Insert path.
        let Some(slot) = self.alloc_slot() else {
            self.note_cell_store_full();
            return;
        };
        let initial = match mode {
            UpsertMode::Accumulate { hits } => hits,
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
        self.emit_delta(sink, slot, 0, initial, rule_slot);
    }

    /// The slot for `rule_fingerprint`. An unknown rule is interned with the
    /// default descriptor so its cells are still stored, forwarded and
    /// expired; `applies_locally` is `false` because `local_rule_id ==
    /// u32::MAX`. `None` (with the rejection counter bumped) when the rule
    /// dictionary is full.
    fn rule_slot_for(&mut self, rule_fingerprint: u128) -> Option<RuleSlot> {
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

    /// Translate a batch row's `(fingerprint, node_id, incarnation)` into
    /// dictionary slots. Returns `None` (with the rejection counter bumped)
    /// when either dictionary is full.
    fn translate_identity(
        &mut self,
        rule_fingerprint: u128,
        node_id: NodeId,
        incarnation: Incarnation,
    ) -> Option<(RuleSlot, NodeSlot)> {
        let rule_slot = self.rule_slot_for(rule_fingerprint)?;
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
    pub fn ingest_local(&mut self, obs: &ObservationBatch, sink: &mut DeltaSink) {
        obs.assert_consistent();
        for i in 0..obs.len() {
            let Some(rule_slot) = self.rule_slot_for(obs.rule_fingerprints[i]) else {
                continue;
            };
            self.upsert(
                UpsertSpec {
                    rule_slot,
                    key_hash: obs.key_hashes[i],
                    bucket: obs.buckets[i],
                    origin_slot: self.local_node_slot,
                    incarnation: self.local_identity.incarnation,
                    now_millis: obs.last_update_millis[i],
                    mode: UpsertMode::Accumulate {
                        hits: obs.counts[i],
                    },
                },
                sink,
            );
        }
    }

    /// Merge observations received from a peer. Each row is max-merged into
    /// the stored count; the resulting deltas (when the stored count rose)
    /// are appended to `sink`.
    pub fn merge_remote(&mut self, obs: &ObservationBatch, sink: &mut DeltaSink) {
        obs.assert_consistent();
        for i in 0..obs.len() {
            let Some((rule_slot, node_slot)) = self.translate_identity(
                obs.rule_fingerprints[i],
                obs.origin_node_ids[i],
                obs.incarnations[i],
            ) else {
                continue;
            };
            self.upsert(
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
    /// For each rule slot `r`, the cell is kept iff its age,
    /// `current_epoch_by_rule[r] - buckets[slot]` in serial-number
    /// arithmetic, is at most `live_buckets[r]`. Each
    /// freed cell is emitted into `sink` *before* its slot is released, so
    /// external aggregators see one row per active-set departure.
    ///
    /// The sink never grows: when it is full the call stops with
    /// [`Expiry::More`], and the next call resumes at the same slot.
    pub fn expire(
        &mut self,
        current_epoch_by_rule: &[BucketEpoch],
        live_buckets: &[u32],
        sink: &mut ExpirationSink,
    ) -> Expiry {
        debug_assert!(
            sink.handles.capacity() > 0,
            "a zero-capacity sink takes no row"
        );
        for slot in self.expire_cursor..self.capacity {
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
            // Serial-number age, so the comparison holds across the `u32`
            // epoch wrap; a window spans fewer than 2^31 buckets.
            let expired = current.wrapping_sub(bucket) as i32 > live as i32;
            if expired {
                if sink.is_full() {
                    self.expire_cursor = slot;
                    return Expiry::More;
                }
                self.emit_expiration(sink, slot);
                self.free_cell_at(slot);
            }
        }
        self.expire_cursor = 0;
        Expiry::Done
    }

    /// Drop dirty records. Stored cells are untouched.
    pub fn clear_dirty(&mut self) {
        self.local_dirty.clear();
        self.forwarded_dirty.clear();
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

    /// Visit dirty cells originating on other peers.
    ///
    /// Streams every current entry in the ring through `visit` with no
    /// intra-call dedup — in contrast to `fill_gossip_frame`, which dedups
    /// via `selection_marks`. Callers composing multi-lane frames must
    /// dedup themselves.
    pub fn visit_forwarded_dirty(&self, mut visit: impl FnMut(CellHandle, &CellStore) -> bool) {
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
    pub fn expire_at(&mut self, now_millis: u64, sink: &mut ExpirationSink) -> Expiry {
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
        let expiry = self.expire(&current, &live, sink);
        self.expire_current_epoch_scratch = current;
        self.expire_live_buckets_scratch = live;
        expiry
    }
}
