//! The runtime half of Snow tree dissemination.
//!
//! Every round the node packs the local cells that changed since the last
//! round into packets and roots one broadcast per packet: it computes its
//! children for the whole ring, sends each child the packet with the range
//! that child becomes responsible for, and waits for their acknowledgements.
//! A node that receives a broadcast merges its cells, computes its own
//! children inside the carried range from its own view and forwards the same
//! cells to them. A leaf acknowledges its parent at once; an interior node
//! acknowledges only after every child has. A root still missing
//! acknowledgements after the timeout rebroadcasts, to the children that
//! have not answered, up to `TreeParams::rebroadcasts` times; beyond that the
//! periodic repair pass is the backstop.

use std::net::SocketAddr;

use crate::crdt::Count;
use crate::wire::{self, EXT_PREFIX_LEN, FrameLimits, Header, Packets};

use super::{Ctrl, GossipRuntime};
use crate::gossip::clock::Clock;
use crate::gossip::snow::{CTRL_MAX_LEN, Child, Children, Relay, full_mask, tree_height};
use crate::gossip::store::AggregateStore;
use crate::gossip::transport::GossipTransport;

impl<C, S, T, K> GossipRuntime<C, S, T, K>
where
    C: Count,
    S: AggregateStore<C>,
    T: GossipTransport,
    K: Clock,
{
    pub(super) fn sender_header(&self) -> Header {
        let identity = self.store.local_identity();
        Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender_node_id: identity.node_id.0,
            sender_incarnation: identity.incarnation,
            ..Header::default()
        }
    }

    pub(super) fn branching(&self) -> usize {
        self.tree.branching
    }

    /// Twice (by default) the expected tree round trip: the tree height
    /// times the measured hop round trip, or the measured round trip of
    /// whole broadcasts when that is longer (see [`RttEstimator`]).
    ///
    /// [`RttEstimator`]: crate::gossip::snow::RttEstimator
    pub(super) fn ack_timeout_ms(&self) -> u64 {
        let height = tree_height(self.ring.len(), self.branching());
        let expected = self
            .rtt
            .expected_ms(height, crate::defaults::GOSSIP_TREE_INITIAL_HOP_RTT_MS);
        let multiple = u64::from(self.tree.ack_timeout_rtt_multiple.max(1));
        (multiple * expected).max(2 * crate::defaults::GOSSIP_TREE_TIMER_TICK_MS)
    }

    // -- root side ----------------------------------------------------------

    /// Root one broadcast per packet of local cells changed since the last
    /// round.
    pub(super) fn start_broadcasts(&mut self, now_millis: u64) {
        self.frame_handles.clear();
        let cap = self.config.max_cells_per_tick;
        let handles = &mut self.frame_handles;
        let mut newest = self.sent_seq;
        self.store
            .visit_local_dirty_after(self.sent_seq, |handle, seq| {
                handles.push(handle);
                newest = seq;
                handles.len() < cap
            });
        self.sent_seq = newest;
        if self.frame_handles.is_empty() {
            return;
        }
        self.dirty_ticks = self.dirty_ticks.saturating_add(1);

        let mut cursor = 0;
        while cursor < self.frame_handles.len() {
            cursor += self.begin_broadcast(cursor, now_millis);
        }
    }

    /// Encode the next packet of `frame_handles[cursor..]` as a broadcast and
    /// send it to this node's children. Returns the handles consumed.
    fn begin_broadcast(&mut self, cursor: usize, now_millis: u64) -> usize {
        let remaining = self.frame_handles.len() - cursor;
        let (idx, _displaced) = self.origins.claim(now_millis);
        let header = self.sender_header();
        // Leave room for the control extension each hop adds, so that a
        // packet that came out full still fits once it is reframed.
        let limits = FrameLimits {
            max_payload_bytes: (self.config.wire_limits.max_payload_bytes)
                .saturating_sub(EXT_PREFIX_LEN + CTRL_MAX_LEN),
            ..self.config.wire_limits
        };
        let encoded = {
            let handles = &self.frame_handles[cursor..];
            let packets = match self.config.auth_key.as_ref() {
                Some(key) => {
                    Packets::<C>::auth(header, &self.store, handles, key, &mut self.scratch, limits)
                }
                None => {
                    Packets::<C>::unauth(header, &self.store, handles, &mut self.scratch, limits)
                }
            };
            packets.ok().map(|mut packets| {
                let written = packets.next_into(&mut self.origins.get_mut(idx).packet);
                (written, packets.finish())
            })
        };
        let Some((written, consumed)) = encoded else {
            self.origins.release(idx);
            return remaining;
        };
        if !matches!(written, Ok(Some(w)) if w.cells_emitted > 0) {
            self.origins.release(idx);
            return consumed.max(1);
        }

        let k = self.branching();
        let timeout = self.ack_timeout_ms();
        let origin = self.origins.get_mut(idx);
        self.ring.root_children(k, &mut origin.children);
        if origin.children.is_empty() {
            self.origins.release(idx);
            return consumed;
        }
        origin.deadline_ms = now_millis + timeout;
        origin.rebroadcasts_left = self.tree.rebroadcasts;
        let children = origin.children.len();
        self.last_effective_fanout = children;
        self.peak_effective_fanout = self.peak_effective_fanout.max(children);
        for child in 0..children {
            self.send_to_child_from_origin(idx, child);
        }
        consumed
    }

    fn send_to_child_from_origin(&mut self, idx: usize, child: usize) {
        let origin = self.origins.get(idx);
        let Child { addr, lo, hi } = origin.children[child];
        let ctrl = Ctrl::Data {
            root: self.ring.me(),
            seq: origin.seq,
            lo,
            hi,
        };
        let mut ext = [0_u8; CTRL_MAX_LEN];
        let ext = ctrl.encode(&mut ext);
        let (header, key, limits) = (
            self.sender_header(),
            self.config.auth_key.as_ref(),
            self.config.wire_limits,
        );
        let queued = self.send.enqueue(addr, |out| {
            wire::reframe(origin.packet.as_bytes(), header, ext, key, limits, out).is_ok()
        });
        if !queued {
            self.note_send_dropped(addr);
        }
    }

    /// Acknowledgement deadlines: rebroadcast to the children that have not
    /// answered, or give the broadcast up to the repair pass.
    pub(super) fn handle_ack_timer(&mut self) {
        let now = self.clock.now_millis();
        let mut backed_off = false;
        for idx in 0..self.origins.len() {
            let origin = self.origins.get(idx);
            if !origin.is_live() || origin.deadline_ms > now {
                continue;
            }
            if !std::mem::replace(&mut backed_off, true) {
                self.rtt.timed_out();
            }
            if origin.rebroadcasts_left == 0 {
                self.note_unacked(idx);
                self.origins.release(idx);
                continue;
            }
            let timeout = self.ack_timeout_ms();
            let origin = self.origins.get_mut(idx);
            origin.rebroadcasts_left -= 1;
            origin.deadline_ms = now + timeout;
            let (children, acked) = (origin.children.len(), origin.acked);
            for child in (0..children).filter(|c| acked & (1 << c) == 0) {
                self.send_to_child_from_origin(idx, child);
            }
        }
        self.pull_due(now);
    }

    /// A tree packet could not be queued: every send buffer was in use, or
    /// the frame did not fit `max_payload_bytes`.
    pub(super) fn note_send_dropped(&mut self, dst: SocketAddr) {
        self.send_dropped_total = self.send_dropped_total.saturating_add(1);
        if self.send_dropped_total.is_power_of_two() {
            tracing::warn!(
                send_dropped_total = self.send_dropped_total,
                peer = %dst,
                send_queue_capacity = self.config.send_queue_capacity,
                "A gossip packet for this peer was dropped before it was sent, so the \
                 peer and the nodes behind it may under-count recent requests until \
                 the periodic repair pass reaches them, and the cluster can briefly \
                 admit more than the limit. Usually the node is sending in bursts \
                 faster than the socket drains, or `max_payload_bytes` is too small \
                 for the tree header. Raise `send_queue_capacity` in the gossip \
                 config, and check that `max_payload_bytes` leaves room for the \
                 control header.",
            );
        }
    }

    fn note_unacked(&mut self, idx: usize) {
        self.unacked_total = self.unacked_total.saturating_add(1);
        if self.unacked_total.is_power_of_two() {
            let origin = self.origins.get(idx);
            tracing::warn!(
                unacked_total = self.unacked_total,
                missing_children = origin.children.len() - origin.acked.count_ones() as usize,
                rebroadcasts = self.tree.rebroadcasts,
                "A change this node broadcast was not acknowledged by every part of the cluster, \
                 even after being sent again. Peers behind the missing acknowledgements may \
                 under-count this node's recent requests until the periodic repair pass reaches \
                 them, so the cluster can briefly admit more than the limit. Usually a peer is \
                 down or slow, or the network is dropping packets. Check peer health first; if \
                 peers are healthy but the network is slow, raise \
                 `GOSSIP_ACK_TIMEOUT_RTT_MULTIPLE`.",
            );
        }
    }

    // -- receiving ----------------------------------------------------------

    /// A frame with a control extension arrived from `src`; its cells are
    /// already merged. `n` is the frame length in `recv_buf`, `seqs` the
    /// lowest and highest origin sequence of its cells.
    pub(super) fn handle_tree_packet(
        &mut self,
        src: SocketAddr,
        ctrl: Ctrl,
        n: usize,
        seqs: (u64, u64),
    ) {
        match ctrl {
            Ctrl::Data { root, seq, lo, hi } => self.on_data(src, (root, seq), lo, hi, n, seqs),
            Ctrl::Ack { root, seq } => self.on_ack(src, root, seq),
            Ctrl::Pull { after, before } => self.serve_pull(src, after, before),
            Ctrl::Fill { after, through } => self.on_fill(src, after, through),
        }
    }

    fn on_data(
        &mut self,
        src: SocketAddr,
        (root, seq): (SocketAddr, u32),
        lo: SocketAddr,
        hi: SocketAddr,
        n: usize,
        seqs: (u64, u64),
    ) {
        if root == self.ring.me() {
            return;
        }
        if let Some(idx) = self.relays.find(root, seq) {
            // The root rebroadcast. Acknowledge again if this node already
            // did. Otherwise push the cells to the children still silent, but
            // only when the rebroadcast came down from the parent: views that
            // disagree can name an ancestor as a child, and following that
            // edge would circulate the packet forever.
            let Some(relay) = self.relays.get_mut(idx).cloned() else {
                return;
            };
            if relay.done {
                self.send_ack(src, root, seq);
            } else if src == relay.parent {
                if let Some(stored) = self.relays.get_mut(idx) {
                    stored.resent = true;
                }
                for (i, child) in relay.children.into_iter().enumerate() {
                    if relay.acked & (1 << i) == 0 {
                        self.forward_to_child(child, root, seq, n);
                    }
                }
            }
            return;
        }

        let now = self.clock.now_millis();
        let due = now + self.pull_grace_ms();
        self.roots.broadcast(root, seq, seqs, now, due);

        let k = self.branching();
        let mut children = Children::new();
        self.ring.children(lo, hi, k, &mut children);
        let leaf = children.is_empty();
        self.relays.insert(Relay {
            root,
            seq,
            parent: src,
            children: children.clone(),
            acked: 0,
            done: leaf,
            sent_ms: now,
            resent: false,
        });
        if leaf {
            self.send_ack(src, root, seq);
        }
        for child in children {
            self.forward_to_child(child, root, seq, n);
        }
    }

    /// Forward the frame in `recv_buf[..n]` to `child` with the range it is
    /// responsible for.
    fn forward_to_child(&mut self, child: Child, root: SocketAddr, seq: u32, n: usize) {
        let Child { addr, lo, hi } = child;
        let mut ext = [0_u8; CTRL_MAX_LEN];
        let ext = Ctrl::Data { root, seq, lo, hi }.encode(&mut ext);
        let (header, key, limits) = (
            self.sender_header(),
            self.config.auth_key.as_ref(),
            self.config.wire_limits,
        );
        let frame = &self.recv_buf[..n];
        let queued = self.send.enqueue(addr, |out| {
            wire::reframe(frame, header, ext, key, limits, out).is_ok()
        });
        if !queued {
            self.note_send_dropped(addr);
        }
    }

    fn send_ack(&mut self, dst: SocketAddr, root: SocketAddr, seq: u32) {
        let mut ext = [0_u8; CTRL_MAX_LEN];
        let ext = Ctrl::Ack { root, seq }.encode(&mut ext);
        let (header, key, limits) = (
            self.sender_header(),
            self.config.auth_key.as_ref(),
            self.config.wire_limits,
        );
        let queued = self.send.enqueue(dst, |out| {
            wire::encode_ext_only::<C>(header, ext, key, limits, out).is_ok()
        });
        if !queued {
            self.note_send_dropped(dst);
        }
    }

    fn on_ack(&mut self, src: SocketAddr, root: SocketAddr, seq: u32) {
        if root == self.ring.me() {
            let Some(idx) = self.origins.find(seq) else {
                return;
            };
            let now = self.clock.now_millis();
            let origin = self.origins.get_mut(idx);
            let first_send = origin.rebroadcasts_left == self.tree.rebroadcasts;
            let elapsed = now.saturating_sub(origin.started_ms);
            if let Some(child) = origin.children.iter().position(|c| c.addr == src) {
                if first_send && is_leaf(&origin.children[child]) {
                    self.rtt.sample_hop(elapsed);
                }
                origin.acked |= 1 << child;
            }
            if origin.all_acked() {
                if first_send {
                    self.rtt.sample_tree(elapsed);
                }
                self.origins.release(idx);
            }
            return;
        }

        let Some(relay) = self
            .relays
            .find(root, seq)
            .and_then(|idx| self.relays.get_mut(idx))
        else {
            return;
        };
        if relay.done {
            return;
        }
        let Some(child) = relay.children.iter().position(|c| c.addr == src) else {
            return;
        };
        relay.acked |= 1 << child;
        if !relay.resent && is_leaf(&relay.children[child]) {
            let elapsed = self.clock.now_millis().saturating_sub(relay.sent_ms);
            self.rtt.sample_hop(elapsed);
        }
        if relay.acked == full_mask(relay.children.len()) {
            relay.done = true;
            let parent = relay.parent;
            self.send_ack(parent, root, seq);
        }
    }

    // -- repair -------------------------------------------------------------

    /// Once per heartbeat, send a rotating slice of the whole cell store to
    /// one random peer. This is the anti-entropy backstop for broadcasts that
    /// stayed incomplete and for cells a new node missed.
    pub(super) fn repair_pass(&mut self, now_millis: u64) {
        let interval = self.config.tick_interval.as_millis() as u64;
        if now_millis.saturating_sub(self.last_repair_ms) < interval {
            return;
        }
        self.last_repair_ms = now_millis;
        let peers = self.ring.len() - 1;
        let pick = self.rng.next_u64() as usize % peers;
        let Some(peer) = self.ring.peers().nth(pick) else {
            return;
        };
        self.frame_handles.clear();
        let handles = &mut self.frame_handles;
        self.store
            .visit_repair_slice(self.tree.repair_cells_per_tick, |handle| {
                handles.push(handle);
                true
            });
        if !self.frame_handles.is_empty() {
            self.encode_packets_for(peer);
        }
    }
}

/// A child handed a range of just itself: it acknowledges as soon as the data
/// arrives, so its ack times one hop.
fn is_leaf(child: &Child) -> bool {
    child.lo == child.addr
}
