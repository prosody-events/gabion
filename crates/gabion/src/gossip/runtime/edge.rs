//! Loss recovery on one eager edge.
//!
//! The Thicket and Plumtree papers assume reliable links; gabion's are UDP.
//! The smallest adaptation that keeps their eager push usable is to number the
//! packets on each directed edge: a receiver that sees a number skipped asks
//! the sender for it (NACK), and the sender answers from a short history of
//! what it sent. A lost packet at the end of a burst leaves no later number to
//! reveal it, so a heartbeat PROBE names the latest number sent.

use crate::crdt::{CellHandle, NodeId};
use crate::defaults::{
    THICKET_EDGE_GAPS, THICKET_NACK_ATTEMPTS, THICKET_NACK_RETRY_MS, THICKET_RETX_CELLS,
    THICKET_RETX_PACKETS,
};
use crate::wire::{ControlEntry, ControlKind, Header};

use super::super::clock::Clock;
use super::super::store::AggregateStore;
use super::super::transport::GossipTransport;
use super::thicket::NONE;
use super::{GossipRuntime, Lane};
use crate::crdt::Count;

#[cfg(test)]
mod tests;

/// An eager packet known lost and asked for.
struct Gap {
    seq: u64,
    asked_ms: u64,
    tries: u8,
}

/// Send and receive state of the eager stream between this node and one peer.
pub(super) struct Edge {
    /// Number of the last packet sent to the peer.
    pub send_seq: u64,
    /// Number the last PROBE named.
    pub probed_seq: u64,
    /// Incarnation of the peer whose numbering `recv_high` follows.
    recv_incarnation: u32,
    /// Highest number seen from the peer.
    recv_high: u64,
    gaps: Vec<Gap>,
}

impl Edge {
    pub(super) fn new() -> Self {
        Self {
            send_seq: 0,
            probed_seq: 0,
            recv_incarnation: 0,
            recv_high: 0,
            gaps: Vec::with_capacity(THICKET_EDGE_GAPS),
        }
    }

    /// Numbers up to `high` have been sent; open a gap for each not yet seen
    /// and add it to `lost`. Only the latest `THICKET_EDGE_GAPS` are tracked:
    /// anything older the lazy path recovers.
    fn lose_through(&mut self, high: u64, now_ms: u64, lost: &mut Vec<ControlEntry>) {
        let floor = high.saturating_sub(THICKET_EDGE_GAPS as u64 - 1).max(1);
        for seq in (self.recv_high + 1).max(floor)..=high {
            if self.gaps.len() < THICKET_EDGE_GAPS {
                self.gaps.push(Gap {
                    seq,
                    asked_ms: now_ms,
                    tries: 1,
                });
                lost.push(nack(seq));
            }
        }
        self.recv_high = self.recv_high.max(high);
    }

    fn follow(&mut self, incarnation: u32) {
        if self.recv_incarnation != incarnation {
            self.recv_incarnation = incarnation;
            self.recv_high = 0;
            self.gaps.clear();
        }
    }

    /// Account for eager packet `seq`. Returns whether it is new: in order,
    /// or after a gap. A late or retransmitted packet is not.
    pub(super) fn receive(
        &mut self,
        incarnation: u32,
        seq: u64,
        now_ms: u64,
        lost: &mut Vec<ControlEntry>,
    ) -> bool {
        self.follow(incarnation);
        if seq <= self.recv_high {
            self.gaps.retain(|g| g.seq != seq);
            return false;
        }
        self.lose_through(seq - 1, now_ms, lost);
        self.recv_high = seq;
        true
    }

    /// The peer says its latest packet to us is `seq`.
    pub(super) fn probe(
        &mut self,
        incarnation: u32,
        seq: u64,
        now_ms: u64,
        lost: &mut Vec<ControlEntry>,
    ) {
        self.follow(incarnation);
        self.lose_through(seq, now_ms, lost);
    }

    /// Ask again for gaps unanswered for the retry interval; give up on those
    /// asked `THICKET_NACK_ATTEMPTS` times.
    pub(super) fn retry(&mut self, now_ms: u64, lost: &mut Vec<ControlEntry>) {
        self.gaps.retain_mut(|g| {
            if now_ms.saturating_sub(g.asked_ms) < THICKET_NACK_RETRY_MS {
                return true;
            }
            if g.tries >= THICKET_NACK_ATTEMPTS {
                return false;
            }
            g.tries += 1;
            g.asked_ms = now_ms;
            lost.push(nack(g.seq));
            true
        });
    }

    /// When the earliest open gap is due for another NACK.
    pub(super) fn next_retry(&self) -> Option<u64> {
        self.gaps
            .iter()
            .map(|g| g.asked_ms + THICKET_NACK_RETRY_MS)
            .min()
    }
}

fn nack(seq: u64) -> ControlEntry {
    ControlEntry {
        node_id: NodeId(0),
        incarnation: 0,
        value: seq,
    }
}

/// The packets most recently sent on any edge, for answering a NACK.
pub(super) struct SentRing {
    peer: Box<[u16]>,
    seq: Box<[u64]>,
    len: Box<[u8]>,
    cells: Box<[CellHandle]>,
    head: usize,
}

impl SentRing {
    pub(super) fn new() -> Self {
        Self {
            peer: vec![NONE; THICKET_RETX_PACKETS].into_boxed_slice(),
            seq: vec![0; THICKET_RETX_PACKETS].into_boxed_slice(),
            len: vec![0; THICKET_RETX_PACKETS].into_boxed_slice(),
            cells: vec![CellHandle::default(); THICKET_RETX_PACKETS * THICKET_RETX_CELLS]
                .into_boxed_slice(),
            head: 0,
        }
    }

    pub(super) fn push(&mut self, peer: u16, seq: u64, handles: &[CellHandle]) {
        let i = self.head;
        self.head = (self.head + 1) % THICKET_RETX_PACKETS;
        if handles.len() > THICKET_RETX_CELLS {
            self.peer[i] = NONE;
            return;
        }
        self.peer[i] = peer;
        self.seq[i] = seq;
        self.len[i] = handles.len() as u8;
        self.cells[i * THICKET_RETX_CELLS..][..handles.len()].copy_from_slice(handles);
    }

    pub(super) fn find(&self, peer: u16, seq: u64) -> Option<&[CellHandle]> {
        (0..THICKET_RETX_PACKETS)
            .map(|k| (self.head + THICKET_RETX_PACKETS - 1 - k) % THICKET_RETX_PACKETS)
            .find(|&i| self.peer[i] == peer && self.seq[i] == seq)
            .map(|i| &self.cells[i * THICKET_RETX_CELLS..][..usize::from(self.len[i])])
    }

    /// Drop what was sent to `peer`: its index now means someone else.
    pub(super) fn forget(&mut self, peer: u16) {
        for p in self.peer.iter_mut().filter(|p| **p == peer) {
            *p = NONE;
        }
    }
}

impl<C, S, T, K> GossipRuntime<C, S, T, K>
where
    C: Count,
    S: AggregateStore<C>,
    T: GossipTransport,
    K: Clock,
{
    /// Account for an eager packet from `peer`, asking for any it skipped.
    /// Returns whether the packet is new on the edge.
    pub(super) fn edge_receive(&mut self, peer: u16, header: &Header, now_ms: u64) -> bool {
        let mut lost = std::mem::take(&mut self.thicket.entries);
        lost.clear();
        let fresh = self.thicket.peers[usize::from(peer)].edge.receive(
            header.sender_incarnation,
            header.edge_sequence,
            now_ms,
            &mut lost,
        );
        self.send_nack(peer, &lost, now_ms);
        self.thicket.entries = lost;
        fresh
    }

    pub(super) fn handle_probe(&mut self, peer: u16, header: &Header, seq: u64, now_ms: u64) {
        let mut lost = std::mem::take(&mut self.thicket.entries);
        lost.clear();
        self.thicket.peers[usize::from(peer)].edge.probe(
            header.sender_incarnation,
            seq,
            now_ms,
            &mut lost,
        );
        self.send_nack(peer, &lost, now_ms);
        self.thicket.entries = lost;
    }

    fn send_nack(&mut self, peer: u16, lost: &[ControlEntry], now_ms: u64) {
        if lost.is_empty() {
            return;
        }
        self.send_control(peer, ControlKind::Nack, 0, lost);
        self.note_deadline(now_ms + THICKET_NACK_RETRY_MS);
    }

    /// Ask again for the packets that are still missing.
    pub(super) fn edge_retry(&mut self, now_ms: u64) {
        let mut lost = std::mem::take(&mut self.thicket.entries);
        for idx in 0..self.thicket.peers.len() {
            let peer = &mut self.thicket.peers[idx];
            if !peer.alive {
                continue;
            }
            lost.clear();
            peer.edge.retry(now_ms, &mut lost);
            self.send_nack(idx as u16, &lost, now_ms);
        }
        self.thicket.entries = lost;
    }

    /// Heartbeat: tell each peer we sent to the number of our latest packet.
    pub(super) fn edge_probes(&mut self) {
        let mut probe = std::mem::take(&mut self.thicket.entries);
        for idx in 0..self.thicket.peers.len() {
            let peer = &mut self.thicket.peers[idx];
            if !peer.alive || peer.edge.send_seq <= peer.edge.probed_seq {
                continue;
            }
            peer.edge.probed_seq = peer.edge.send_seq;
            probe.clear();
            probe.push(ControlEntry {
                node_id: NodeId(0),
                incarnation: 0,
                value: peer.edge.send_seq,
            });
            self.send_control(idx as u16, ControlKind::Probe, 0, &probe);
        }
        self.thicket.entries = probe;
    }

    /// Answer a NACK: resend each named packet we still hold, under its
    /// original number, so the receiver's accounting closes the gap.
    pub(super) fn handle_nack(&mut self, peer: u16, asked: &[ControlEntry]) {
        let mut frame = std::mem::take(&mut self.frame_handles);
        for entry in asked {
            let Some(handles) = self.thicket.sent.find(peer, entry.value) else {
                continue;
            };
            frame.clear();
            frame.extend_from_slice(handles);
            self.send_cells(peer, &frame, Lane::Retransmit(entry.value));
        }
        self.frame_handles = frame;
    }
}
