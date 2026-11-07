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
//! Two stream lists thread live cells through two link columns: the own list
//! holds this node's cells in ascending `origin_sequence`, the relay list the
//! remote cells [`CellStore::merge_remote`] marked for relay, in ascending
//! relay sequence. The gossip runtime reads the cells a peer has not been
//! sent ([`CellStore::stream_after`]) without a scan. A cell raised (own) or
//! marked again (relay) moves to its list's tail; a freed cell leaves it. No
//! cell is in both: own cells are never marked.
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
pub use io::{DeltaSink, ExpirationSink, Observation, ObservationBatch};

pub(crate) use dictionary::MAX_DICTIONARY_CAPACITY;
pub(crate) use dictionary::pow2_index_capacity_for;
use hash::hash_compact_cell_key;
pub(crate) use hash::{hash_fingerprint, mix64};
pub(crate) use index::CellIndex;

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};

use crate::rules::{CardinalityLimits, RuleTable};
use crate::settings::Setting;
use crate::throttle;

/// A rule's bucket: `now_millis / bucket_millis`, cut to `u32`. Epochs wrap,
/// so they carry no order; age one against another with [`Self::age_since`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct BucketEpoch(pub u32);

impl BucketEpoch {
    /// The epoch of the unwrapped bucket number `bucket`: the cut to `u32`
    /// is the wrap.
    pub fn wrap(bucket: u64) -> Self {
        Self(bucket as u32)
    }

    /// The epoch `n` buckets before this one.
    pub fn back(self, n: u32) -> Self {
        Self(self.0.wrapping_sub(n))
    }

    /// Buckets from `older` to this epoch by serial-number arithmetic: right
    /// across the wrap while the two lie fewer than 2^31 buckets apart.
    pub fn age_since(self, older: Self) -> i32 {
        self.0.wrapping_sub(older.0) as i32
    }
}

/// Node incarnation counter — distinguishes successive instances of one node.
pub type Incarnation = u32;

/// Interned rule slot: a row of [`RuleDictionary`]. Only the `crdt` module
/// makes one; a live cell's slot came from the dictionary.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuleSlot(u16);

impl RuleSlot {
    /// The slot's row in its dictionary's columns.
    pub fn index(self) -> usize {
        usize::from(self.0)
    }
}

/// Interned `(node_id, incarnation)` slot: a row of [`NodeDictionary`]. Only
/// the `crdt` module makes one; a live cell's slot came from the dictionary.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeSlot(u16);

impl NodeSlot {
    /// The slot's row in its dictionary's columns.
    pub fn index(self) -> usize {
        usize::from(self.0)
    }
}

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
/// the struct never carries a raw `NodeId` or rule fingerprint. Internal to one
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

/// How [`CellStore::expire`] ages the cells of one rule slot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RuleAge {
    /// No rule holds the slot, so no cell refers to it.
    #[default]
    Unused,
    /// A cell is kept while its bucket is at most `live` buckets older than
    /// the `current` bucket.
    Epoch { current: BucketEpoch, live: u32 },
    /// A rule this node does not configure. The wire carries a cell's bucket
    /// but not the bucket's width, so a cell is kept while it was updated at
    /// or after `since_ms`: long enough to relay it, and bounded.
    Idle { since_ms: u64 },
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

    /// The node dictionary's capacity: `storage.node_dictionary_capacity`, or
    /// twice `gossip.peer_capacity` by default.
    pub fn node_dictionary_capacity(&self) -> NonZeroU16 {
        self.node_dictionary_capacity
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

    /// Slot where the next `expire` call resumes; 0 starts a pass.
    expire_cursor: u32,

    // The stream lists: this node's live cells in ascending
    // `origin_sequences`, and the remote cells marked for relay in ascending
    // `relay_seq` (`None`: not on the relay list). A slot in no list links to
    // `SlotLink::NONE`.
    stream_prev: Box<[SlotLink]>,
    stream_next: Box<[SlotLink]>,
    relay_seq: Box<[Option<NonZeroU64>]>,
    own_list: ListEnds,
    relay_list: ListEnds,
    /// The last relay sequence issued: the relay stream's head.
    relay_head: u64,

    // Per-origin sequence allocator (indexed by NodeSlot).
    next_sequence_by_origin: Box<[u64]>,

    // Scratch for `expire_at` — sized to rule_dictionary capacity.
    // Reused across every call; never allocated past `new()`.
    expire_ages_scratch: Box<[RuleAge]>,

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

/// A slot index or none. `NONE` is the only sentinel and never a slot, since
/// the capacity is at most 2^30; readers see [`SlotLink::get`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SlotLink(u32);

impl SlotLink {
    const NONE: Self = Self(u32::MAX);

    fn get(self) -> Option<u32> {
        (self != Self::NONE).then_some(self.0)
    }
}

/// The first and last slot of a list.
#[derive(Clone, Copy, Debug)]
struct ListEnds {
    head: SlotLink,
    tail: SlotLink,
}

impl ListEnds {
    const EMPTY: Self = Self {
        head: SlotLink::NONE,
        tail: SlotLink::NONE,
    };
}

/// One of the two streams a node sends each peer: the cells it originated,
/// and the remote cells it relays into its zone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Stream {
    Own,
    Relay,
}

/// How [`CellStore::merge_remote`] treats the relay list for a packet's
/// remote cells.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelayMark {
    /// The cells arrived another way (a zone peer, a relay, a sweep): a cell
    /// the merge raised leaves the relay list, its sender being responsible
    /// for it here.
    Clear,
    /// A flagged packet not judged fresh: mark the cells the merge raised.
    Changed,
    /// A fresh flagged packet: mark every cell, raised or not.
    All,
}

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
    /// if `observed` is strictly greater (CRDT max-merge); `mark` says how the
    /// cell's relay-list place follows.
    MaxMerge { observed: u32, mark: RelayMark },
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
            rules: vec![RuleSlot(0); cap as usize].into_boxed_slice(),
            key_hashes: vec![KeyHash(0); cap as usize].into_boxed_slice(),
            buckets: vec![BucketEpoch(0); cap as usize].into_boxed_slice(),
            origins: vec![NodeSlot(0); cap as usize].into_boxed_slice(),
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

            expire_cursor: 0,

            stream_prev: vec![SlotLink::NONE; cap as usize].into_boxed_slice(),
            stream_next: vec![SlotLink::NONE; cap as usize].into_boxed_slice(),
            relay_seq: vec![None; cap as usize].into_boxed_slice(),
            own_list: ListEnds::EMPTY,
            relay_list: ListEnds::EMPTY,
            relay_head: 0,

            next_sequence_by_origin: vec![0_u64; usize::from(node_capacity)].into_boxed_slice(),

            expire_ages_scratch: vec![RuleAge::Unused; rule_capacity].into_boxed_slice(),

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
                 longest rule window). The default is twice `gossip.peer_capacity`.",
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

    /// Iterate the active cell handles at slot `from` and above, in storage
    /// order.
    ///
    /// Walks the slots up to `capacity`, not `active_len`: a join sweep takes
    /// one page of it per round; otherwise for diagnostics and tests.
    pub fn active_handles(&self, from: u32) -> impl Iterator<Item = CellHandle> + '_ {
        (from..self.capacity).filter_map(move |i| {
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
        self.unlink(slot);
        self.rule_dictionary.dec_ref(rule);
        let freed_node = self.node_dictionary.dec_ref(origin);
        if freed_node {
            self.next_sequence_by_origin[origin.index()] = 0;
        }
        self.free_slot(slot);
    }

    // -- The stream lists ---------------------------------------------------

    /// The list `slot` is on, if any: an own cell is on the own list, a
    /// marked remote cell on the relay list.
    fn list_of(&self, slot: u32) -> Option<Stream> {
        if self.origins[slot as usize] == self.local_node_slot {
            Some(Stream::Own)
        } else {
            self.relay_seq[slot as usize].map(|_| Stream::Relay)
        }
    }

    fn ends(&self, stream: Stream) -> ListEnds {
        match stream {
            Stream::Own => self.own_list,
            Stream::Relay => self.relay_list,
        }
    }

    fn ends_mut(&mut self, stream: Stream) -> &mut ListEnds {
        match stream {
            Stream::Own => &mut self.own_list,
            Stream::Relay => &mut self.relay_list,
        }
    }

    /// `slot`'s position on `stream`.
    fn seq(&self, stream: Stream, slot: u32) -> u64 {
        match stream {
            Stream::Own => self.origin_sequences[slot as usize],
            Stream::Relay => self.relay_seq[slot as usize].map_or(0, NonZeroU64::get),
        }
    }

    /// Append `slot`, which holds `stream`'s newest position, at its tail.
    fn link_tail(&mut self, stream: Stream, slot: u32) {
        let tail = self.ends(stream).tail;
        self.stream_prev[slot as usize] = tail;
        self.stream_next[slot as usize] = SlotLink::NONE;
        match tail.get() {
            Some(tail) => self.stream_next[tail as usize] = SlotLink(slot),
            None => self.ends_mut(stream).head = SlotLink(slot),
        }
        self.ends_mut(stream).tail = SlotLink(slot);
    }

    /// Take `slot` out of its list, if it is on one; a relay cell leaves the
    /// relay stream.
    fn unlink(&mut self, slot: u32) {
        let Some(stream) = self.list_of(slot) else {
            return;
        };
        self.relay_seq[slot as usize] = None;
        let prev = std::mem::replace(&mut self.stream_prev[slot as usize], SlotLink::NONE);
        let next = std::mem::replace(&mut self.stream_next[slot as usize], SlotLink::NONE);
        match prev.get() {
            Some(prev) => self.stream_next[prev as usize] = next,
            None => self.ends_mut(stream).head = next,
        }
        match next.get() {
            Some(next) => self.stream_prev[next as usize] = prev,
            None => self.ends_mut(stream).tail = prev,
        }
    }

    /// Put remote `slot` at the relay tail with the next relay sequence.
    fn mark_relay(&mut self, slot: u32) {
        self.unlink(slot);
        self.relay_head += 1;
        self.relay_seq[slot as usize] = NonZeroU64::new(self.relay_head);
        self.link_tail(Stream::Relay, slot);
    }

    /// `stream`'s head: the last position issued on it. Neither head ever
    /// decreases: the local node slot is pinned, and relay sequences are
    /// never reused.
    pub fn head(&self, stream: Stream) -> u64 {
        match stream {
            Stream::Own => self.next_sequence_by_origin[self.local_node_slot.index()],
            Stream::Relay => self.relay_head,
        }
    }

    /// `handle`'s position on `stream`, while it is live.
    pub fn sequence(&self, stream: Stream, handle: CellHandle) -> Option<u64> {
        self.resolve(handle).map(|slot| self.seq(stream, slot))
    }

    /// Clear `out`, then fill it with the live cells of `stream` whose
    /// position is above `sent`, ascending, at most `max`. The walk to the
    /// first such cell starts from whichever end is nearer by position, so it
    /// costs at most the smaller of `sent − first` and `last − sent` steps: a
    /// new peer, a rewound stream and a draining backlog walk from the head,
    /// steady state from the tail. `out` must hold `max` handles, so nothing
    /// allocates.
    pub fn stream_after(&self, stream: Stream, sent: u64, max: usize, out: &mut Vec<CellHandle>) {
        debug_assert!(
            out.capacity() >= max,
            "stream_after fills a pre-sized buffer"
        );
        out.clear();
        let seq = |slot: u32| self.seq(stream, slot);
        let ends = self.ends(stream);
        let (Some(head), Some(tail)) = (ends.head.get(), ends.tail.get()) else {
            return;
        };
        let mut cursor = if sent.saturating_sub(seq(head)) <= seq(tail).saturating_sub(sent) {
            let mut cursor = SlotLink(head);
            while let Some(slot) = cursor.get()
                && seq(slot) <= sent
            {
                cursor = self.stream_next[slot as usize];
            }
            cursor
        } else {
            let (mut first, mut cursor) = (SlotLink::NONE, SlotLink(tail));
            while let Some(slot) = cursor.get()
                && seq(slot) > sent
            {
                first = cursor;
                cursor = self.stream_prev[slot as usize];
            }
            first
        };
        while let Some(slot) = cursor.get()
            && out.len() < max
        {
            out.push(self.handle_for(slot));
            cursor = self.stream_next[slot as usize];
        }
    }

    // -- Update primitives --------------------------------------------------

    fn next_origin_sequence(&mut self, origin: NodeSlot) -> u64 {
        let seq = &mut self.next_sequence_by_origin[origin.index()];
        *seq = seq.saturating_add(1);
        *seq
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
            descriptor.applies_locally,
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
            descriptor.applies_locally,
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
        let mark = match mode {
            // Own cells are never relayed, so no cell is on both lists.
            UpsertMode::MaxMerge { mark, .. } if origin_slot != self.local_node_slot => mark,
            _ => RelayMark::Clear,
        };
        if let Some(slot) = self.lookup_index(key) {
            let previous = self.counts[slot as usize];
            let next = match mode {
                UpsertMode::Accumulate { hits } => previous.saturating_add(hits),
                UpsertMode::MaxMerge { observed, .. } => observed.max(previous),
            };
            if next == previous {
                if mark == RelayMark::All {
                    self.mark_relay(slot);
                }
                return;
            }
            self.counts[slot as usize] = next;
            self.last_update_millis[slot as usize] = now_millis;
            let seq = self.next_origin_sequence(origin_slot);
            self.origin_sequences[slot as usize] = seq;
            if origin_slot == self.local_node_slot {
                self.unlink(slot);
                self.link_tail(Stream::Own, slot);
            } else if mark == RelayMark::Clear {
                self.unlink(slot);
            } else {
                self.mark_relay(slot);
            }
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
            UpsertMode::MaxMerge { observed, .. } => observed,
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

        if origin_slot == self.local_node_slot {
            self.link_tail(Stream::Own, slot);
        } else if mark != RelayMark::Clear {
            self.mark_relay(slot);
        }
        self.emit_delta(sink, slot, 0, initial, rule_slot);
    }

    /// The slot for `rule_fingerprint`. An unknown rule is interned with the
    /// default descriptor so its cells are still stored, forwarded and
    /// expired; `applies_locally` is `false`. `None` (with the rejection
    /// counter bumped) when the rule dictionary is full.
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
    /// are appended to `sink`. `mark` moves remote cells onto or off the
    /// relay list; own cells are never marked.
    pub fn merge_remote(&mut self, obs: &ObservationBatch, sink: &mut DeltaSink, mark: RelayMark) {
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
                        mark,
                    },
                },
                sink,
            );
        }
    }

    /// Free the cells that have aged out, each by `ages[r]` of its rule slot
    /// `r`. Each freed cell is emitted into `sink` *before* its slot is
    /// released, so external aggregators see one row per active-set
    /// departure.
    ///
    /// The sink never grows: when it is full the call stops with
    /// [`Expiry::More`], and the next call resumes at the same slot.
    pub fn expire(&mut self, ages: &[RuleAge], sink: &mut ExpirationSink) -> Expiry {
        debug_assert!(
            sink.handles.capacity() > 0,
            "a zero-capacity sink takes no row"
        );
        for slot in self.expire_cursor..self.capacity {
            if !self.is_active(slot) {
                continue;
            }
            let expired = match ages.get(self.rules[slot as usize].index()) {
                // A window spans fewer than 2^31 buckets.
                Some(&RuleAge::Epoch { current, live }) => {
                    current.age_since(self.buckets[slot as usize]) > live as i32
                }
                Some(&RuleAge::Idle { since_ms }) => {
                    self.last_update_millis[slot as usize] < since_ms
                }
                Some(RuleAge::Unused) | None => false,
            };
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

    /// Convenience wrapper around [`Self::expire`] that derives each rule's
    /// [`RuleAge`] from the current wall-clock time and its descriptor; a
    /// wire-only rule's cells live for the default descriptor's window past
    /// their last update. Uses scratch storage allocated at construction time
    /// so the call is allocation-free.
    pub fn expire_at(&mut self, now_millis: u64, sink: &mut ExpirationSink) -> Expiry {
        debug_assert_eq!(
            self.expire_ages_scratch.len(),
            self.rule_dictionary.capacity() as usize
        );
        let idle_ms = u64::from(RuleDescriptor::default().window_millis);
        for (slot, age) in self.expire_ages_scratch.iter_mut().enumerate() {
            *age = match self.rule_dictionary.descriptor(RuleSlot(slot as u16)) {
                Some(d) if d.applies_locally => RuleAge::Epoch {
                    current: d.current_epoch(now_millis),
                    live: d.live_buckets(),
                },
                Some(_) => RuleAge::Idle {
                    since_ms: now_millis.saturating_sub(idle_ms),
                },
                None => RuleAge::Unused,
            };
        }

        // Swap the scratch slice out so we can pass it as a `&[T]` borrow
        // to `self.expire(...)` (which needs `&mut self`).
        // `Box::<[T]>::default` returns an empty boxed slice that does
        // not allocate.
        let ages = std::mem::take(&mut self.expire_ages_scratch);
        let expiry = self.expire(&ages, sink);
        self.expire_ages_scratch = ages;
        expiry
    }
}
