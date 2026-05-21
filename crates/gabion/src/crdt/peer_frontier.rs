//! Per-peer × per-origin frontier table.

use super::{NodeId, NodeSlot};

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
