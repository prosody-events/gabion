//! GossipSub state that is independent of the runtime: configuration, the
//! bounded message-id index, and the message cache.
//!
//! A message is one gossip datagram, named by the 64-bit hash of its bytes
//! (libp2p lets a deployment choose its message-id function; Eth2 uses a
//! content hash). Both caches are rings sized at construction, so nothing
//! here grows with time, traffic or departed peers.

use std::time::Duration;

use crate::defaults;

#[cfg(test)]
mod tests;

/// GossipSub v1.1 parameters, defaulting to the specification's values
/// (`crate::defaults::GOSSIPSUB_*`).
#[derive(Clone, Debug)]
pub struct PubSubConfig {
    /// Target mesh degree `D`.
    pub d: usize,
    /// Mesh lower bound `D_lo`.
    pub d_lo: usize,
    /// Mesh upper bound `D_hi`.
    pub d_hi: usize,
    /// Minimum non-mesh peers sent `IHAVE` per heartbeat.
    pub d_lazy: usize,
    /// Fraction of non-mesh peers sent `IHAVE` per heartbeat when larger.
    pub gossip_factor: f64,
    /// Heartbeats a message stays servable to `IWANT`.
    pub mcache_len: u32,
    /// Newest heartbeats whose message ids are advertised in `IHAVE`.
    pub mcache_gossip: u32,
    /// Mesh heartbeat period.
    pub heartbeat: Duration,
    /// Seconds a pruned peer must wait before being grafted again.
    pub prune_backoff_secs: u16,
    /// How long a message id is remembered for duplicate suppression.
    pub seen_ttl: Duration,
    /// Bound on cached messages.
    pub mcache_capacity: usize,
    /// Bytes reserved per cached message.
    pub mcache_slot_bytes: usize,
    /// Bound on remembered message ids.
    pub seen_capacity: usize,
    /// Most ids requested from one peer per heartbeat with `IWANT`.
    pub max_iwant_ids: usize,
    /// Cells per answer of the catch-up sweep; `0` disables it.
    pub sweep_slice_cells: usize,
}

impl Default for PubSubConfig {
    fn default() -> Self {
        Self {
            d: defaults::GOSSIPSUB_D,
            d_lo: defaults::GOSSIPSUB_D_LO,
            d_hi: defaults::GOSSIPSUB_D_HI,
            d_lazy: defaults::GOSSIPSUB_D_LAZY,
            gossip_factor: defaults::GOSSIPSUB_GOSSIP_FACTOR,
            mcache_len: defaults::GOSSIPSUB_MCACHE_LEN,
            mcache_gossip: defaults::GOSSIPSUB_MCACHE_GOSSIP,
            heartbeat: Duration::from_millis(defaults::GOSSIPSUB_HEARTBEAT_MS),
            prune_backoff_secs: defaults::GOSSIPSUB_PRUNE_BACKOFF_SECS,
            seen_ttl: Duration::from_secs(defaults::GOSSIPSUB_SEEN_TTL_SECS),
            mcache_capacity: defaults::GOSSIPSUB_MCACHE_CAPACITY,
            mcache_slot_bytes: defaults::GOSSIPSUB_MCACHE_SLOT_BYTES,
            seen_capacity: defaults::GOSSIPSUB_SEEN_CAPACITY,
            max_iwant_ids: defaults::GOSSIPSUB_MAX_IWANT_IDS,
            sweep_slice_cells: defaults::GOSSIPSUB_SWEEP_SLICE_CELLS,
        }
    }
}

/// Name of a message: the hash of its datagram bytes.
pub(super) fn message_id(datagram: &[u8]) -> u64 {
    twox_hash::XxHash3_128::oneshot(datagram) as u64
}

const NIL: u32 = u32::MAX;

/// Bounded FIFO set of message ids with O(1) lookup. Ids are already hashes,
/// so their low bits index the chained buckets directly. Entries are stamped
/// on insertion; stamps never decrease, so expiry only looks at the oldest.
pub(super) struct IdRing {
    ids: Box<[u64]>,
    stamps: Box<[u64]>,
    next: Box<[u32]>,
    heads: Box<[u32]>,
    first: u32,
    len: u32,
}

impl IdRing {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.clamp(1, (u32::MAX / 2) as usize);
        Self {
            ids: vec![0; capacity].into_boxed_slice(),
            stamps: vec![0; capacity].into_boxed_slice(),
            next: vec![NIL; capacity].into_boxed_slice(),
            heads: vec![NIL; capacity.next_power_of_two()].into_boxed_slice(),
            first: 0,
            len: 0,
        }
    }

    fn bucket(&self, id: u64) -> usize {
        id as usize & (self.heads.len() - 1)
    }

    pub(super) fn find(&self, id: u64) -> Option<u32> {
        let mut slot = self.heads[self.bucket(id)];
        while slot != NIL {
            if self.ids[slot as usize] == id {
                return Some(slot);
            }
            slot = self.next[slot as usize];
        }
        None
    }

    pub(super) fn contains(&self, id: u64) -> bool {
        self.find(id).is_some()
    }

    /// Insert an id known to be absent, evicting the oldest when full.
    /// Returns the slot it occupies.
    pub(super) fn insert(&mut self, id: u64, stamp: u64) -> u32 {
        debug_assert!(!self.contains(id));
        let capacity = self.ids.len() as u32;
        if self.len == capacity {
            self.evict_oldest();
        }
        let slot = (self.first + self.len) % capacity;
        let bucket = self.bucket(id);
        self.ids[slot as usize] = id;
        self.stamps[slot as usize] = stamp;
        self.next[slot as usize] = self.heads[bucket];
        self.heads[bucket] = slot;
        self.len += 1;
        slot
    }

    fn evict_oldest(&mut self) {
        if self.len == 0 {
            return;
        }
        let slot = self.first;
        let bucket = self.bucket(self.ids[slot as usize]);
        if self.heads[bucket] == slot {
            self.heads[bucket] = self.next[slot as usize];
        } else {
            let mut prev = self.heads[bucket];
            while prev != NIL && self.next[prev as usize] != slot {
                prev = self.next[prev as usize];
            }
            debug_assert_ne!(prev, NIL, "evicted id missing from its bucket chain");
            if prev != NIL {
                self.next[prev as usize] = self.next[slot as usize];
            }
        }
        self.first = (self.first + 1) % self.ids.len() as u32;
        self.len -= 1;
    }

    /// Evict every entry stamped before `stamp`.
    pub(super) fn expire_before(&mut self, stamp: u64) {
        while self.len > 0 && self.stamps[self.first as usize] < stamp {
            self.evict_oldest();
        }
    }

    /// `(slot, id, stamp)` from the newest entry to the oldest.
    pub(super) fn newest_first(&self) -> impl Iterator<Item = (u32, u64, u64)> + '_ {
        let capacity = self.ids.len() as u32;
        (0..self.len).rev().map(move |offset| {
            let slot = (self.first + offset) % capacity;
            (slot, self.ids[slot as usize], self.stamps[slot as usize])
        })
    }
}

/// GossipSub's `mcache`: recent messages, retrievable by id to answer
/// `IWANT`, whose ids feed `IHAVE`. Entries are stamped with the heartbeat
/// count at insertion.
pub(super) struct MessageCache {
    ring: IdRing,
    payload: Box<[u8]>,
    lens: Box<[u16]>,
    slot_bytes: usize,
}

impl MessageCache {
    pub(super) fn new(capacity: usize, slot_bytes: usize) -> Self {
        let slot_bytes = slot_bytes.min(usize::from(u16::MAX));
        let ring = IdRing::with_capacity(capacity);
        let slots = ring.ids.len();
        Self {
            ring,
            payload: vec![0; slots * slot_bytes].into_boxed_slice(),
            lens: vec![0; slots].into_boxed_slice(),
            slot_bytes,
        }
    }

    /// Cache `datagram` under `id`. Datagrams over the slot size and ids
    /// already cached are ignored.
    pub(super) fn insert(&mut self, id: u64, heartbeat: u64, datagram: &[u8]) {
        if datagram.len() > self.slot_bytes || self.ring.contains(id) {
            return;
        }
        let slot = self.ring.insert(id, heartbeat) as usize;
        let start = slot * self.slot_bytes;
        self.payload[start..start + datagram.len()].copy_from_slice(datagram);
        self.lens[slot] = datagram.len() as u16;
    }

    pub(super) fn get(&self, id: u64) -> Option<&[u8]> {
        let slot = self.ring.find(id)? as usize;
        let start = slot * self.slot_bytes;
        Some(&self.payload[start..start + usize::from(self.lens[slot])])
    }

    /// Drop messages that have lived `history` heartbeats as of `heartbeat`.
    pub(super) fn expire(&mut self, heartbeat: u64, history: u32) {
        self.ring
            .expire_before((heartbeat + 1).saturating_sub(u64::from(history)));
    }

    /// Append, newest first, the ids of messages younger than `window`
    /// heartbeats as of `heartbeat`.
    pub(super) fn gossip_ids(&self, heartbeat: u64, window: u32, out: &mut Vec<u64>) {
        out.extend(
            self.ring
                .newest_first()
                .take_while(|&(_, _, stamp)| heartbeat - stamp < u64::from(window))
                .map(|(_, id, _)| id),
        );
    }
}
