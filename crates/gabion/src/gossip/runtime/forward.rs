//! Sending: cells down the trees, and control frames.

use std::net::SocketAddr;

use crate::crdt::{CellHandle, CellStore, Count};
use crate::defaults::THICKET_PROBE_REPEATS;
use crate::wire::{self, ControlEntry, ControlKind, FLAG_EDGE, Packets, encode_control};

use super::super::clock::Clock;
use super::super::store::AggregateStore;
use super::super::transport::GossipTransport;
use super::GossipRuntime;
use super::thicket::{NONE, Root, tree_of};

/// Which stream a frame of cells belongs to.
#[derive(Clone, Copy)]
pub(super) enum Lane {
    /// Eager push: numbered on the edge, remembered for retransmission, and a
    /// duplicate of it makes the receiver PRUNE the edge.
    Push,
    /// A lost eager packet sent again under its original number.
    Retransmit(u64),
    /// Cells answering a GRAFT. Not part of the eager stream.
    Pull,
}

impl<C, S, T, K> GossipRuntime<C, S, T, K>
where
    C: Count,
    S: AggregateStore<C>,
    T: GossipTransport,
    K: Clock,
{
    fn frame_header(&self) -> wire::Header {
        wire::Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender_node_id: self.store.local_identity().node_id.0,
            sender_incarnation: self.store.local_identity().incarnation,
            ..wire::Header::default()
        }
    }

    /// Encode `handles` for peer `dst` into free send slots. Returns how many
    /// handles did not fit the send queue and were dropped.
    pub(super) fn send_cells(&mut self, dst: u16, handles: &[CellHandle], lane: Lane) -> usize {
        let addr = self.thicket.peers[usize::from(dst)].addr;
        let mut header = self.frame_header();
        match lane {
            Lane::Push => {
                header.flags = FLAG_EDGE;
                header.edge_sequence = self.thicket.peers[usize::from(dst)].edge.send_seq + 1;
            }
            Lane::Retransmit(seq) => {
                header.flags = FLAG_EDGE;
                header.edge_sequence = seq;
            }
            Lane::Pull => {}
        }
        let unsent = {
            let built = match self.config.auth_key.as_ref() {
                Some(key) => Packets::<C>::auth(
                    header,
                    &self.store,
                    handles,
                    key,
                    &mut self.scratch,
                    self.config.wire_limits,
                ),
                None => Packets::<C>::unauth(
                    header,
                    &self.store,
                    handles,
                    &mut self.scratch,
                    self.config.wire_limits,
                ),
            };
            let mut packets = match built {
                Ok(packets) => packets,
                Err(_) => return handles.len(),
            };
            while let Some(slot) = self.send_free.pop() {
                let before = packets.remaining();
                match packets.next_into(&mut self.send_pool[usize::from(slot)]) {
                    Ok(Some(written)) => {
                        self.send_pending.push_back((addr, slot));
                        if let Lane::Push = lane {
                            let start = handles.len() - before;
                            let consumed = before - packets.remaining();
                            let edge = &mut self.thicket.peers[usize::from(dst)].edge;
                            edge.send_seq = written.edge_sequence;
                            edge.probes_left = THICKET_PROBE_REPEATS;
                            self.thicket.sent.push(
                                dst,
                                written.edge_sequence,
                                &handles[start..start + consumed],
                            );
                        }
                        if let Lane::Retransmit(_) = lane {
                            break;
                        }
                    }
                    Ok(None) | Err(_) => {
                        self.send_free.push(slot);
                        break;
                    }
                }
            }
            packets.abandon()
        };
        if unsent > 0 && !matches!(lane, Lane::Retransmit(_)) {
            self.note_send_queue_full(addr, unsent);
        }
        unsent
    }

    /// Send a control frame to peer `dst`, as many frames as `entries` need.
    pub(super) fn send_control(
        &mut self,
        dst: u16,
        kind: ControlKind,
        tree: u8,
        entries: &[ControlEntry],
    ) {
        let addr = self.thicket.peers[usize::from(dst)].addr;
        let header = self.frame_header();
        let mut rest = entries;
        loop {
            let Some(slot) = self.send_free.pop() else {
                self.note_send_queue_full(addr, rest.len().max(1));
                return;
            };
            let written = encode_control(
                header,
                kind,
                tree,
                rest,
                self.config.auth_key.as_ref(),
                self.config.wire_limits,
                &mut self.send_pool[usize::from(slot)],
            );
            let Ok(written) = written else {
                self.send_free.push(slot);
                return;
            };
            self.send_pending.push_back((addr, slot));
            rest = &rest[written..];
            if rest.is_empty() {
                return;
            }
        }
    }

    fn note_send_queue_full(&mut self, peer: SocketAddr, unsent: usize) {
        self.cut_frames = self.cut_frames.saturating_add(1);
        if self.cut_frames.is_power_of_two() {
            tracing::warn!(
                %peer,
                unsent,
                cut_frames_total = self.cut_frames,
                send_queue_capacity = self.send_pool.len(),
                "Gossip frames were not sent because the send queue was full, \
                 so this peer will hear of these changes late. They were \
                 dropped, not queued: the peer finds the gap through the \
                 IHAVE announcement and asks for the cells with a GRAFT. \
                 Usually a burst of new keys, or a socket that is not draining; \
                 if it keeps happening, raise `gossip.send_queue_capacity` \
                 (nginx: `gabion_gossip_send_queue_capacity`).",
            );
        }
    }

    /// Push this node's own changes of `tree` made since `since_ms`, less one
    /// heartbeat, to `parent`: a new parent there, standing in for one that
    /// went silent or left and may have dropped them. The heartbeat's margin
    /// covers a change made earlier and sent at the next heartbeat. Walks the
    /// store, which only a change of parent does.
    pub(super) fn resend_own(&mut self, tree: u8, parent: u16, since_ms: u64) {
        let since = since_ms.saturating_sub(self.config.tick_interval.as_millis() as u64);
        let me = self.store.local_identity().node_id;
        let local = self.store.local_node_slot();
        let cap = self.config.max_cells_per_tick;
        let mut frame = std::mem::take(&mut self.frame_handles);
        frame.clear();
        for handle in self.store.active_handles() {
            if frame.len() == cap {
                break;
            }
            let Some(row) = self.store.get(handle) else {
                continue;
            };
            if row.key.origin == local
                && row.last_update_millis >= since
                && tree_of(me, row.origin_sequence) == tree
            {
                frame.push(handle);
            }
        }
        if !frame.is_empty() {
            self.send_cells(parent, &frame, Lane::Push);
        }
        self.frame_handles = frame;
    }

    /// Push the changes in the dirty rings along the trees: to each backbone
    /// neighbour that is eager for a change's root, to leaf children, and for
    /// this node's own changes in a tree it is a leaf of, to its parent. A
    /// relay round (`include_local == false`) forwards only what was merged
    /// from peers; a heartbeat or threshold round sends local changes too.
    pub(super) fn forward_round(&mut self, include_local: bool) {
        self.thicket.collect_candidates();
        let dests = std::mem::take(&mut self.thicket.dests);
        let mut frame = std::mem::take(&mut self.frame_handles);
        let mut sent_to = 0;
        for &dest in &dests {
            frame.clear();
            let full = self.collect_for(dest, include_local, &mut frame);
            if frame.is_empty() {
                continue;
            }
            sent_to += 1;
            self.send_cells(dest, &frame, Lane::Push);
            if full {
                let addr = self.thicket.peers[usize::from(dest)].addr;
                self.note_send_queue_full(addr, 1);
            }
        }
        self.frame_handles = frame;
        self.thicket.dests = dests;
        if include_local {
            self.store.clear_dirty();
        } else {
            self.store.clear_forwarded_dirty();
        }
        if sent_to > 0 {
            self.dirty_ticks = self.dirty_ticks.saturating_add(1);
            self.last_effective_fanout = sent_to;
            self.peak_effective_fanout = self.peak_effective_fanout.max(sent_to);
        }
    }

    /// The dirty cells that go to `dest`, up to one frame. Returns whether the
    /// frame filled and cells were left out.
    fn collect_for(&mut self, dest: u16, include_local: bool, out: &mut Vec<CellHandle>) -> bool {
        let cap = self.config.max_cells_per_tick;
        let Self { store, thicket, .. } = self;
        let mut full = false;
        let mut visit = |handle: CellHandle, store: &CellStore<C>| {
            let Some(row) = store.get(handle) else {
                return true;
            };
            let Some(origin) = store.node_dictionary().descriptor(row.key.origin) else {
                return true;
            };
            let local = row.key.origin == store.local_node_slot();
            let from = if local {
                NONE
            } else {
                thicket.from_peer[handle.index as usize]
            };
            if from == dest {
                return true;
            }
            let root = Root {
                slot: row.key.origin,
                node_id: origin.node_id,
                incarnation: origin.incarnation,
                local,
            };
            let route =
                thicket.pushes_to(dest, &root, tree_of(origin.node_id, row.origin_sequence));
            if !route {
                return true;
            }
            if out.len() >= cap {
                full = true;
                return false;
            }
            out.push(handle);
            true
        };
        if include_local {
            store.visit_local_dirty(&mut visit);
        }
        store.visit_forwarded_dirty(&mut visit);
        full
    }
}
