//! GossipSub dissemination for the runtime: publishing local changes to the
//! mesh, forwarding what arrives, and the control plane (`GRAFT`, `PRUNE`,
//! `IHAVE`, `IWANT`) that keeps the mesh and repairs what it misses.
//!
//! A *message* is one datagram of cells, named by [`message_id`]. Messages
//! are forwarded byte-for-byte, so a receiver identifies the forwarding peer
//! by source address and the publisher by the datagram's header.

use std::net::SocketAddr;

use crate::crdt::{Count, NodeId};
use crate::wire::{self, Control, FrameLimits, Header, HmacKey, Packets};

use super::super::clock::Clock;
use super::super::pubsub::{IdRing, MessageCache, PubSubConfig, message_id};
use super::super::send_queue::SendQueue;
use super::super::store::AggregateStore;
use super::super::transport::GossipTransport;
use super::{GossipRuntime, SplitMix64};

/// Initial capacity of the peer-selection scratch lists: the production peer
/// bound. They grow only for clusters beyond it.
const PEER_SCRATCH: usize = crate::defaults::STORAGE_PEER_CAPACITY as usize;

/// One peer the runtime gossips with.
pub(super) struct Peer {
    pub(super) addr: SocketAddr,
    /// Learned from the header of a control frame the peer sent; control
    /// frames are never forwarded, so it is the peer's own id.
    pub(super) node_id: Option<NodeId>,
    pub(super) mesh: bool,
    /// No GRAFT in either direction before this time (milliseconds).
    backoff_until_ms: u64,
    /// This node does not graft the peer before this time (milliseconds):
    /// the peer refused a GRAFT because discovery had not reported this node
    /// to it yet. Unlike the backoff, the peer's own GRAFT is still welcome.
    graft_after_ms: u64,
}

impl Peer {
    pub(super) fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            node_id: None,
            mesh: false,
            backoff_until_ms: 0,
            graft_after_ms: 0,
        }
    }
}

/// What every outgoing control frame needs to be encoded and signed.
pub(super) struct Framing<'a> {
    pub(super) header: Header,
    pub(super) key: Option<&'a HmacKey>,
    pub(super) limits: FrameLimits,
}

/// Mesh, message caches and the send queue.
pub(super) struct PubSub {
    cfg: PubSubConfig,
    pub(super) peers: Vec<Peer>,
    pub(super) sendq: SendQueue,
    mcache: MessageCache,
    seen: IdRing,
    heartbeats: u64,
    /// Highest local origin sequence already published.
    published_sequence: u64,
    /// Message ids to advertise (`IHAVE`) or request (`IWANT`).
    pub(super) ids: Vec<u64>,
    targets: Vec<SocketAddr>,
    candidates: Vec<u32>,
    rng: SplitMix64,
}

impl PubSub {
    pub(super) fn new(
        cfg: PubSubConfig,
        peers: Vec<Peer>,
        sendq: SendQueue,
        limits: FrameLimits,
        rng_seed: u64,
    ) -> Self {
        let ids = Vec::with_capacity(
            cfg.mcache_capacity
                .max(wire::max_ids_per_frame(limits, false)),
        );
        Self {
            mcache: MessageCache::new(cfg.mcache_capacity, cfg.mcache_slot_bytes),
            seen: IdRing::with_capacity(cfg.seen_capacity),
            cfg,
            peers,
            sendq,
            heartbeats: 0,
            published_sequence: 0,
            ids,
            targets: Vec::with_capacity(PEER_SCRATCH),
            candidates: Vec::with_capacity(PEER_SCRATCH),
            rng: SplitMix64::new(rng_seed),
        }
    }

    /// The same peers and send queue under new parameters; the message
    /// caches start empty.
    pub(super) fn reconfigured(self, cfg: PubSubConfig, limits: FrameLimits) -> Self {
        Self {
            peers: self.peers,
            sendq: self.sendq,
            rng: self.rng,
            ..Self::new(cfg, Vec::new(), SendQueue::new(1, limits), limits, 0)
        }
    }

    pub(super) fn heartbeat_interval_ms(&self) -> u64 {
        self.cfg.heartbeat.as_millis() as u64
    }

    pub(super) fn mesh_degree(&self) -> usize {
        self.peers.iter().filter(|p| p.mesh).count()
    }

    pub(super) fn has_seen(&self, id: u64) -> bool {
        self.seen.contains(id)
    }

    /// Remember `datagram` and push it to every mesh peer except the one it
    /// came from and its publisher. Returns `false` when no send buffer was
    /// free, in which case the mesh peers were not reached.
    pub(super) fn accept_message(
        &mut self,
        datagram: &[u8],
        now_ms: u64,
        from: Option<SocketAddr>,
        publisher: Option<NodeId>,
    ) -> bool {
        let id = message_id(datagram);
        if !self.seen.contains(id) {
            self.seen.insert(id, now_ms);
        }
        self.mcache.insert(id, self.heartbeats, datagram);
        let Some(slot) = self.sendq.claim("forward a message to the mesh") else {
            return false;
        };
        if self.sendq.buf_mut(slot).set_bytes(datagram) {
            for peer in &self.peers {
                if peer.mesh
                    && Some(peer.addr) != from
                    && (publisher.is_none() || peer.node_id != publisher)
                {
                    self.sendq.send_to(slot, peer.addr);
                }
            }
        }
        self.sendq.settle(slot);
        true
    }

    // -- control plane ------------------------------------------------------

    pub(super) fn on_control(
        &mut self,
        framing: &Framing<'_>,
        control: Control,
        sender: NodeId,
        src: SocketAddr,
        now_ms: u64,
    ) {
        if let Some(peer) = self.peers.iter_mut().find(|p| p.addr == src) {
            peer.node_id = Some(sender);
        }
        match control {
            Control::Graft => self.on_graft(framing, src, now_ms),
            Control::Prune { backoff_secs } => {
                // A refusal without a backoff comes from a peer that has not
                // discovered this node yet: sit out the next heartbeat, so the
                // peer stays an IHAVE target meanwhile, then graft again.
                let retry_ms = now_ms + 2 * self.heartbeat_interval_ms();
                if let Some(peer) = self.peers.iter_mut().find(|p| p.addr == src) {
                    peer.mesh = false;
                    peer.backoff_until_ms = now_ms + u64::from(backoff_secs) * 1_000;
                    if backoff_secs == 0 {
                        peer.graft_after_ms = retry_ms;
                    }
                }
            }
            Control::IHave => {
                // Ask for the advertised messages we have not seen. Ids are
                // requested at once (`iwant_delay_ms = 0`).
                let seen = &self.seen;
                self.ids.retain(|&id| !seen.contains(id));
                self.targets.clear();
                self.targets.push(src);
                self.send_ids(framing, Control::IWant);
            }
            Control::IWant => {
                for &id in &self.ids {
                    let Some(datagram) = self.mcache.get(id) else {
                        continue;
                    };
                    let Some(slot) = self.sendq.claim("answer an IWANT") else {
                        break;
                    };
                    if self.sendq.buf_mut(slot).set_bytes(datagram) {
                        self.sendq.send_to(slot, src);
                    }
                    self.sendq.settle(slot);
                }
            }
        }
    }

    /// Accept a GRAFT unless the sender is unknown, still in backoff, or the
    /// mesh is already at `D_hi`; refuse with a PRUNE. A sender discovery has
    /// not reported yet is refused without a backoff, so it grafts again on a
    /// later heartbeat instead of shunning this node for `prune_backoff`.
    fn on_graft(&mut self, framing: &Framing<'_>, src: SocketAddr, now_ms: u64) {
        let degree = self.mesh_degree();
        let backoff = now_ms + u64::from(self.cfg.prune_backoff_secs) * 1_000;
        match self.peers.iter_mut().find(|p| p.addr == src) {
            Some(peer) if peer.mesh => {}
            Some(peer) if now_ms >= peer.backoff_until_ms && degree < self.cfg.d_hi => {
                peer.mesh = true;
            }
            Some(peer) => {
                peer.backoff_until_ms = backoff;
                self.send_control(framing, src, self.prune());
            }
            None => self.send_control(framing, src, Control::Prune { backoff_secs: 0 }),
        }
    }

    fn prune(&self) -> Control {
        Control::Prune {
            backoff_secs: self.cfg.prune_backoff_secs,
        }
    }

    fn send_control(&mut self, framing: &Framing<'_>, dst: SocketAddr, control: Control) {
        let Some(slot) = self.sendq.claim("send a GRAFT or PRUNE") else {
            return;
        };
        let encoded = wire::encode_control(
            framing.header,
            control,
            &[],
            framing.key,
            framing.limits,
            self.sendq.buf_mut(slot),
        );
        if encoded.is_ok() {
            self.sendq.send_to(slot, dst);
        }
        self.sendq.settle(slot);
    }

    /// Send `self.ids` as `control` (`IHave` or `IWant`) to every address in
    /// `self.targets`, one datagram per `max_ids_per_frame` ids.
    fn send_ids(&mut self, framing: &Framing<'_>, control: Control) {
        let per_frame = wire::max_ids_per_frame(framing.limits, framing.key.is_some()).max(1);
        let what = match control {
            Control::IHave => "send an IHAVE",
            _ => "send an IWANT",
        };
        for chunk in self.ids.chunks(per_frame) {
            let Some(slot) = self.sendq.claim(what) else {
                break;
            };
            let encoded = wire::encode_control(
                framing.header,
                control,
                chunk,
                framing.key,
                framing.limits,
                self.sendq.buf_mut(slot),
            );
            if encoded.is_ok() {
                for &dst in &self.targets {
                    self.sendq.send_to(slot, dst);
                }
            }
            self.sendq.settle(slot);
        }
    }

    // -- heartbeat ----------------------------------------------------------

    pub(super) fn heartbeat(&mut self, framing: &Framing<'_>, now_ms: u64) {
        let seen_ttl_ms = self.cfg.seen_ttl.as_millis() as u64;
        self.seen.expire_before(now_ms.saturating_sub(seen_ttl_ms));
        self.maintain_mesh(framing, now_ms);
        self.emit_gossip(framing);
        self.heartbeats += 1;
        self.mcache.expire(self.heartbeats, self.cfg.mcache_len);
    }

    /// Bring the mesh back into `[D_lo, D_hi]`: graft up to `D` when below the
    /// lower bound, prune down to `D` when above the upper.
    fn maintain_mesh(&mut self, framing: &Framing<'_>, now_ms: u64) {
        let degree = self.mesh_degree();
        if degree < self.cfg.d_lo {
            self.select(|p| !p.mesh && p.backoff_until_ms <= now_ms && p.graft_after_ms <= now_ms);
            let take = self.cfg.d.saturating_sub(degree).min(self.candidates.len());
            self.sample_candidates(take);
            for i in 0..take {
                let peer = &mut self.peers[self.candidates[i] as usize];
                peer.mesh = true;
                let addr = peer.addr;
                self.send_control(framing, addr, Control::Graft);
            }
        } else if degree > self.cfg.d_hi {
            self.select(|p| p.mesh);
            let take = degree.saturating_sub(self.cfg.d).min(self.candidates.len());
            self.sample_candidates(take);
            let backoff = now_ms + u64::from(self.cfg.prune_backoff_secs) * 1_000;
            for i in 0..take {
                let peer = &mut self.peers[self.candidates[i] as usize];
                peer.mesh = false;
                peer.backoff_until_ms = backoff;
                let addr = peer.addr;
                self.send_control(framing, addr, self.prune());
            }
        }
    }

    /// Advertise the ids of recent messages to `max(D_lazy, gossip_factor ×
    /// non-mesh peers)` random non-mesh peers.
    fn emit_gossip(&mut self, framing: &Framing<'_>) {
        self.ids.clear();
        self.mcache
            .gossip_ids(self.heartbeats, self.cfg.mcache_gossip, &mut self.ids);
        if self.ids.is_empty() {
            return;
        }
        self.select(|p| !p.mesh);
        let by_factor = (self.cfg.gossip_factor * self.candidates.len() as f64) as usize;
        let take = by_factor.max(self.cfg.d_lazy).min(self.candidates.len());
        self.sample_candidates(take);
        self.targets.clear();
        for &i in &self.candidates[..take] {
            self.targets.push(self.peers[i as usize].addr);
        }
        self.send_ids(framing, Control::IHave);
    }

    fn select(&mut self, keep: impl Fn(&Peer) -> bool) {
        self.candidates.clear();
        for (i, peer) in self.peers.iter().enumerate() {
            if keep(peer) {
                self.candidates.push(i as u32);
            }
        }
    }

    /// Move `take` uniformly chosen candidates to the front (partial
    /// Fisher-Yates), so `candidates[..take]` is a random sample.
    fn sample_candidates(&mut self, take: usize) {
        let n = self.candidates.len();
        for i in 0..take.min(n) {
            let j = i + (self.rng.next_u64() as usize) % (n - i);
            self.candidates.swap(i, j);
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
    pub(super) fn header(&self) -> Header {
        let identity = self.store.local_identity();
        Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender_node_id: identity.node_id.0,
            sender_incarnation: identity.incarnation,
            ..Header::default()
        }
    }

    pub(super) fn handle_heartbeat(&mut self, now_ms: u64) {
        let framing = Framing {
            header: self.header(),
            key: self.config.auth_key.as_ref(),
            limits: self.config.wire_limits,
        };
        self.pubsub.heartbeat(&framing, now_ms);
    }

    /// A datagram of cells: merge it, then forward it if it is new.
    pub(super) fn handle_message(&mut self, n: usize, src: SocketAddr) {
        let bytes = &self.recv_buf[..n];
        if self.pubsub.has_seen(message_id(bytes)) {
            return;
        }
        self.obs_buf.clear();
        self.sink_buf.clear();
        let decoded = {
            let obs_buf = &mut self.obs_buf;
            let mut on_cell =
                |cell: wire::WireCell<C>| obs_buf.push(wire::observation_from_wire(cell));
            match self.config.auth_key.as_ref() {
                Some(key) => wire::decode_auth_visit::<C>(
                    bytes,
                    key,
                    self.config.wire_limits,
                    |_| true,
                    &mut on_cell,
                ),
                None => wire::decode_unauth_visit::<C>(
                    bytes,
                    self.config.wire_limits,
                    |_| true,
                    &mut on_cell,
                ),
            }
        };
        let summary = match decoded {
            Ok(summary) => summary,
            Err(err) => return self.note_decode_reject(src, &err),
        };
        self.store.merge_remote(&self.obs_buf, &mut self.sink_buf);
        // The datagram is forwarded as received, so nothing remains to send
        // from the forwarded dirty ring.
        self.store.clear_forwarded_dirty();
        let now_ms = self.clock.now_millis();
        self.pubsub.accept_message(
            &self.recv_buf[..n],
            now_ms,
            Some(src),
            Some(NodeId(summary.header.sender_node_id)),
        );
    }

    pub(super) fn handle_control(&mut self, n: usize, src: SocketAddr) {
        let decoded = wire::decode_control(
            &self.recv_buf[..n],
            self.config.auth_key.as_ref(),
            self.config.wire_limits,
            &mut self.pubsub.ids,
        );
        let (header, control) = match decoded {
            Ok(decoded) => decoded,
            Err(err) => return self.note_decode_reject(src, &err),
        };
        let now_ms = self.clock.now_millis();
        let framing = Framing {
            header: self.header(),
            key: self.config.auth_key.as_ref(),
            limits: self.config.wire_limits,
        };
        self.pubsub.on_control(
            &framing,
            control,
            NodeId(header.sender_node_id),
            src,
            now_ms,
        );
    }

    /// Publish the local cells changed since the last publish to the mesh.
    /// Cells that do not fit in this round's send buffers stay unpublished
    /// for the next one.
    pub(super) fn publish_local_changes(&mut self) {
        let published = self.pubsub.published_sequence;
        let limit = self.config.max_cells_per_tick;
        let handles = &mut self.frame_handles;
        handles.clear();
        self.store.visit_local_dirty(|handle, store| {
            let changed = store
                .get(handle)
                .is_some_and(|row| row.origin_sequence > published);
            if changed {
                handles.push(handle);
            }
            handles.len() < limit
        });
        let total = self.frame_handles.len();
        if total == 0 {
            return;
        }

        let header = self.header();
        let limits = self.config.wire_limits;
        let packets = match self.config.auth_key.as_ref() {
            Some(key) => Packets::<C>::auth(
                header,
                &self.store,
                &self.frame_handles,
                key,
                &mut self.scratch,
                limits,
            ),
            None => Packets::<C>::unauth(
                header,
                &self.store,
                &self.frame_handles,
                &mut self.scratch,
                limits,
            ),
        };
        let Ok(mut packets) = packets else {
            return;
        };
        let now_ms = self.clock.now_millis();
        let mut sent = 0;
        let mut stalled = false;
        while let Ok(Some(_)) = packets.next_into(&mut self.msg_buf) {
            // Keep encoding after a stall so `packets` is fully drained.
            if !stalled
                && self
                    .pubsub
                    .accept_message(self.msg_buf.as_bytes(), now_ms, None, None)
            {
                sent = total - packets.remaining();
            } else {
                stalled = true;
            }
        }
        drop(packets);

        if let Some(last) = self.frame_handles[..sent].last()
            && let Some(row) = self.store.get(*last)
        {
            self.pubsub.published_sequence = row.origin_sequence;
        }
        let degree = self.pubsub.mesh_degree();
        self.last_effective_fanout = degree;
        self.peak_effective_fanout = self.peak_effective_fanout.max(degree);
    }
}
