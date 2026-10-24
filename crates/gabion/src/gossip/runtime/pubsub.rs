//! GossipSub dissemination for the runtime: publishing local changes to the
//! mesh, forwarding what arrives, and the control plane (`GRAFT`, `PRUNE`,
//! `IHAVE`, `IWANT`) that keeps the mesh and repairs what it misses, plus the
//! catch-up sweep (`SWEEP`, `SWEEP_DONE`) for gaps the message cache cannot
//! fill.
//!
//! A *message* is one datagram of cells, named by [`message_id`]. Messages
//! are forwarded byte-for-byte, so a receiver identifies the forwarding peer
//! by source address and the publisher by the datagram's header.

use std::net::SocketAddr;

use crate::crdt::{Count, NodeId};
use crate::wire::{self, Control, FrameLimits, Header, HmacKey, Packets, SWEEP_COMPLETE};

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
    /// When the peer last sent this node cells or joined its mesh.
    heard: Heard,
    /// Cells from the peer before this time may answer this node's `IWANT`,
    /// which any peer serves, so they say nothing about the mesh link.
    iwant_until_ms: u64,
    /// Ids requested from the peer with `IWANT` since the last heartbeat.
    iwant_asked: usize,
}

impl Peer {
    pub(super) fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            node_id: None,
            mesh: false,
            backoff_until_ms: 0,
            graft_after_ms: 0,
            heard: Heard::default(),
            iwant_until_ms: 0,
            iwant_asked: 0,
        }
    }
}

/// A point in a peer's traffic: [`PubSub::accepted`] and the clock then.
#[derive(Clone, Copy, Default)]
struct Heard {
    accepted: u64,
    at_ms: u64,
}

/// New messages the rest of the mesh must push to this node while one mesh
/// peer sends it nothing for that peer to count as cut off (besides a silence
/// longer than `IHAVE` can repair). A mesh peer forwards every message it does
/// not first receive from this node, about half of them, so it misses this
/// many in a row by chance about once in 256 tries, while a quiet cluster,
/// where silence is normal, never qualifies.
const SWEEP_SILENCE_MESSAGES: u64 = 8;

/// Times a catch-up sweep request is re-sent, one heartbeat apart, before the
/// sweep is abandoned.
const SWEEP_RETRIES: u8 = 3;

/// A catch-up sweep in progress: the peer's live cells from storage slot
/// `cursor` on are still to come.
struct Sweep {
    peer: SocketAddr,
    cursor: u32,
    asked_ms: u64,
    retries: u8,
    /// Frames of the slice asked for that have arrived.
    received: u16,
    /// The peer's `SWEEP_DONE` for that slice: where the next slice starts
    /// and how many frames this one had. Frames may arrive after it.
    done: Option<(u32, u16)>,
    cause: SweepCause,
}

/// Why a sweep started, weakest first: a stronger cause takes over a sweep
/// under way for a weaker one.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SweepCause {
    /// An `IHAVE` frame of mostly unseen ids.
    Advertisement,
    /// A mesh peer swept across a healed cut and said so (`CAUGHT_UP`).
    CaughtUp,
    /// A mesh peer back from a cut; the mesh hears `CAUGHT_UP` when done.
    Cut,
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
    sweep: Option<Sweep>,
    /// No sweep starts on an advertisement before this time (milliseconds).
    sweep_idle_until_ms: u64,
    /// No sweep starts on a `CAUGHT_UP` before this time (milliseconds).
    caught_up_until_ms: u64,
    /// New messages mesh peers have pushed to this node so far (answers to
    /// its `IWANT`s excluded).
    accepted: u64,
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
            sweep: None,
            sweep_idle_until_ms: 0,
            caught_up_until_ms: 0,
            accepted: 0,
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

    pub(super) fn is_peer(&self, addr: SocketAddr) -> bool {
        self.peers.iter().any(|p| p.addr == addr)
    }

    pub(super) fn sweep_slice_cells(&self) -> usize {
        self.cfg.sweep_slice_cells
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
        if self
            .peers
            .iter()
            .any(|p| Some(p.addr) == from && p.mesh && now_ms >= p.iwant_until_ms)
        {
            self.accepted += 1;
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
                // requested at once (`iwant_delay_ms = 0`). An advertisement
                // that is mostly news means this node missed more than the
                // message cache can serve, so it also sweeps the advertiser.
                // The swept peer's store holds every message it advertises,
                // so while the sweep runs those count as seen.
                let advertised = self.ids.len();
                let seen = &self.seen;
                self.ids.retain(|&id| !seen.contains(id));
                if self.ids.is_empty() {
                    return;
                }
                match &self.sweep {
                    Some(sweep) if sweep.peer == src => return self.mark_seen(now_ms),
                    Some(_) => {}
                    None if 2 * self.ids.len() > advertised
                        && now_ms >= self.sweep_idle_until_ms =>
                    {
                        self.begin_sweep(framing, src, now_ms, SweepCause::Advertisement);
                    }
                    None => {}
                }
                // Ask a random sample, within the peer's budget for this
                // heartbeat (a peer discovery has not reported yet is not
                // tracked, so each of its frames gets the whole budget).
                let answer_by_ms = now_ms + self.heartbeat_interval_ms();
                let mut peer = self.peers.iter_mut().find(|p| p.addr == src);
                let asked = peer.as_ref().map_or(0, |p| p.iwant_asked);
                let ask = self
                    .ids
                    .len()
                    .min(self.cfg.max_iwant_ids.saturating_sub(asked));
                if ask == 0 {
                    return;
                }
                if let Some(peer) = peer.as_mut() {
                    peer.iwant_asked += ask;
                    peer.iwant_until_ms = answer_by_ms;
                }
                for i in 0..ask {
                    let j = i + (self.rng.next_u64() as usize) % (self.ids.len() - i);
                    self.ids.swap(i, j);
                }
                self.ids.truncate(ask);
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
            // The runtime answers a sweep request: it owns the store.
            Control::Sweep { .. } => {}
            Control::CaughtUp => {
                if now_ms >= self.caught_up_until_ms {
                    self.begin_sweep(framing, src, now_ms, SweepCause::CaughtUp);
                }
            }
            Control::SweepDone {
                cursor,
                next,
                frames,
            } => {
                if let Some(sweep) = self
                    .sweep
                    .as_mut()
                    .filter(|s| s.peer == src && s.cursor == cursor)
                {
                    sweep.done = Some((next, frames));
                    self.advance_sweep(framing, now_ms);
                }
            }
        }
    }

    // -- catch-up sweep -----------------------------------------------------

    /// Count the unseen ids in `self.ids` as seen: a sweep brings their cells.
    fn mark_seen(&mut self, now_ms: u64) {
        for &id in &self.ids {
            self.seen.insert(id, now_ms);
        }
    }

    /// Cells arrived from `src`. A mesh peer that sent nothing for longer than
    /// `IHAVE` advertises a message (`mcache_gossip` heartbeats), while the
    /// rest of the mesh pushed [`SWEEP_SILENCE_MESSAGES`] new messages, was
    /// cut off from this node, so sweep it. A peer back while another cut-off
    /// peer is swept is left to that sweep: a cut heals for every peer on its
    /// far side at once.
    pub(super) fn note_cells_from(&mut self, framing: &Framing<'_>, src: SocketAddr, now_ms: u64) {
        let accepted = self.accepted;
        let repairable_ms = u64::from(self.cfg.mcache_gossip) * self.heartbeat_interval_ms();
        let Some(peer) = self
            .peers
            .iter_mut()
            .find(|p| p.addr == src && now_ms >= p.iwant_until_ms)
        else {
            return;
        };
        let cut_off = peer.mesh
            && accepted - peer.heard.accepted >= SWEEP_SILENCE_MESSAGES
            && now_ms - peer.heard.at_ms >= repairable_ms;
        peer.heard = Heard {
            accepted,
            at_ms: now_ms,
        };
        if cut_off {
            self.begin_sweep(framing, src, now_ms, SweepCause::Cut);
        }
    }

    /// Start pulling `src`'s live cells, unless sweeps are off, `src` is not a
    /// discovered peer, or a sweep for an equal or stronger cause runs.
    fn begin_sweep(
        &mut self,
        framing: &Framing<'_>,
        src: SocketAddr,
        now_ms: u64,
        cause: SweepCause,
    ) {
        if self.cfg.sweep_slice_cells == 0
            || self.sweep.as_ref().is_some_and(|s| s.cause >= cause)
            || !self.is_peer(src)
        {
            return;
        }
        self.sweep = Some(Sweep {
            peer: src,
            cursor: 0,
            asked_ms: now_ms,
            retries: 0,
            received: 0,
            done: None,
            cause,
        });
        self.send_control(framing, src, Control::Sweep { cursor: 0 });
    }

    /// A frame of cells answering this node's sweep arrived from `src`.
    pub(super) fn on_sweep_frame(&mut self, framing: &Framing<'_>, src: SocketAddr, now_ms: u64) {
        if let Some(sweep) = self.sweep.as_mut().filter(|s| s.peer == src) {
            sweep.received = sweep.received.saturating_add(1);
            self.advance_sweep(framing, now_ms);
        }
    }

    /// Once every frame of the slice asked for has arrived, ask for the next
    /// one, or finish when the store is exhausted. A slice that lost a frame
    /// is asked for again by [`Self::retry_sweep`].
    fn advance_sweep(&mut self, framing: &Framing<'_>, now_ms: u64) {
        let Some(sweep) = self.sweep.as_mut() else {
            return;
        };
        let Some((next, _)) = sweep.done.filter(|&(_, frames)| sweep.received >= frames) else {
            return;
        };
        if next == SWEEP_COMPLETE {
            let (swept, cause) = (sweep.peer, sweep.cause);
            self.finish_sweep(now_ms);
            if cause == SweepCause::Cut {
                // A mesh peer with no link across the cut of its own learns
                // of the heal from this node.
                self.select(|p| p.mesh && p.addr != swept);
                for i in 0..self.candidates.len() {
                    let addr = self.peers[self.candidates[i] as usize].addr;
                    self.send_control(framing, addr, Control::CaughtUp);
                }
            }
            return;
        }
        *sweep = Sweep {
            cursor: next,
            asked_ms: now_ms,
            retries: 0,
            received: 0,
            done: None,
            ..*sweep
        };
        let peer = sweep.peer;
        self.send_control(framing, peer, Control::Sweep { cursor: next });
    }

    /// Re-ask a slice not complete within a heartbeat, giving up
    /// after [`SWEEP_RETRIES`] or once the peer has left.
    fn retry_sweep(&mut self, framing: &Framing<'_>, now_ms: u64) {
        let heartbeat_ms = self.heartbeat_interval_ms();
        let peers = &self.peers;
        let Some(sweep) = self
            .sweep
            .as_mut()
            .filter(|s| now_ms >= s.asked_ms + heartbeat_ms)
        else {
            return;
        };
        if sweep.retries >= SWEEP_RETRIES || !peers.iter().any(|p| p.addr == sweep.peer) {
            return self.abandon_sweep(framing, now_ms);
        }
        sweep.retries += 1;
        sweep.asked_ms = now_ms;
        sweep.received = 0;
        sweep.done = None;
        let (peer, cursor) = (sweep.peer, sweep.cursor);
        self.send_control(framing, peer, Control::Sweep { cursor });
    }

    /// Take a sweep whose peer left or stopped answering up again from a
    /// random other peer: the node is still short, and the advertisements the
    /// sweep counted as seen will not start another.
    fn abandon_sweep(&mut self, framing: &Framing<'_>, now_ms: u64) {
        let Some(failed) = self.sweep.take() else {
            return;
        };
        self.select(|p| p.addr != failed.peer);
        if self.candidates.is_empty() {
            return;
        }
        let pick = self.candidates[(self.rng.next_u64() as usize) % self.candidates.len()];
        let peer = self.peers[pick as usize].addr;
        self.begin_sweep(framing, peer, now_ms, failed.cause);
    }

    /// Forget a peer discovery reported gone. Returns whether it was known.
    pub(super) fn remove_peer(
        &mut self,
        framing: &Framing<'_>,
        addr: SocketAddr,
        now_ms: u64,
    ) -> bool {
        let Some(pos) = self.peers.iter().position(|p| p.addr == addr) else {
            return false;
        };
        self.peers.remove(pos);
        if self.sweep.as_ref().is_some_and(|s| s.peer == addr) {
            self.abandon_sweep(framing, now_ms);
        }
        true
    }

    /// After a sweep, wait until the advertisements it may have raced (the
    /// ids of `mcache_gossip` heartbeats) have aged out before another one
    /// starts on an advertisement, and after one that repaired a cut, until
    /// every neighbour's `CAUGHT_UP` about that cut has come. A cut-off mesh
    /// peer starts one at once.
    fn finish_sweep(&mut self, now_ms: u64) {
        let Some(sweep) = self.sweep.take() else {
            return;
        };
        let idle_until = now_ms + u64::from(self.cfg.mcache_gossip) * self.heartbeat_interval_ms();
        self.sweep_idle_until_ms = idle_until;
        if sweep.cause > SweepCause::Advertisement {
            self.caught_up_until_ms = idle_until;
        }
    }

    /// Accept a GRAFT unless the sender is unknown, still in backoff, or the
    /// mesh is already at `D_hi`; refuse with a PRUNE. A sender discovery has
    /// not reported yet is refused without a backoff, so it grafts again on a
    /// later heartbeat instead of shunning this node for `prune_backoff`.
    fn on_graft(&mut self, framing: &Framing<'_>, src: SocketAddr, now_ms: u64) {
        let degree = self.mesh_degree();
        let backoff = now_ms + u64::from(self.cfg.prune_backoff_secs) * 1_000;
        let heard = Heard {
            accepted: self.accepted,
            at_ms: now_ms,
        };
        match self.peers.iter_mut().find(|p| p.addr == src) {
            // Already in this node's mesh: the peer (re)joined from its side,
            // so its silence until now says nothing about the link.
            Some(peer) if peer.mesh => peer.heard = heard,
            Some(peer) if now_ms >= peer.backoff_until_ms && degree < self.cfg.d_hi => {
                peer.mesh = true;
                peer.heard = heard;
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

    pub(super) fn send_control(
        &mut self,
        framing: &Framing<'_>,
        dst: SocketAddr,
        control: Control,
    ) {
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
        self.retry_sweep(framing, now_ms);
        for peer in &mut self.peers {
            peer.iwant_asked = 0;
        }
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
                peer.heard = Heard {
                    accepted: self.accepted,
                    at_ms: now_ms,
                };
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

    /// A datagram of cells: merge it, then forward it if it is a new message.
    /// A sweep answer is merged only.
    pub(super) fn handle_message(&mut self, n: usize, src: SocketAddr) {
        let now_ms = self.clock.now_millis();
        let framing = Framing {
            header: self.header(),
            key: self.config.auth_key.as_ref(),
            limits: self.config.wire_limits,
        };
        self.pubsub.note_cells_from(&framing, src, now_ms);
        let bytes = &self.recv_buf[..n];
        let sweep = wire::is_sweep(bytes);
        if !sweep && self.pubsub.has_seen(message_id(bytes)) {
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
        // The datagram is forwarded as received (or, answering a sweep, not
        // at all), so nothing remains to send from the forwarded dirty ring.
        self.store.clear_forwarded_dirty();
        if sweep {
            let framing = Framing {
                header: self.header(),
                key: self.config.auth_key.as_ref(),
                limits: self.config.wire_limits,
            };
            return self.pubsub.on_sweep_frame(&framing, src, now_ms);
        }
        self.pubsub.accept_message(
            &self.recv_buf[..n],
            now_ms,
            Some(src),
            Some(NodeId(summary.header.sender_node_id)),
        );
    }

    /// Answer a sweep request from a discovered peer with up to
    /// `sweep_slice_cells` live cells from storage slot `cursor` on, then a
    /// `SWEEP_DONE` naming where the next slice starts. The cells go out as
    /// `FLAG_SWEEP` frames, which the requester merges without forwarding.
    fn answer_sweep(&mut self, src: SocketAddr, cursor: u32) {
        let slice = self
            .pubsub
            .sweep_slice_cells()
            .min(self.config.max_cells_per_tick);
        if slice == 0 || !self.pubsub.is_peer(src) {
            return;
        }
        let handles = &mut self.frame_handles;
        handles.clear();
        let mut next = SWEEP_COMPLETE;
        for handle in self.store.active_handles_from(cursor) {
            if handles.len() == slice {
                next = handle.index;
                break;
            }
            handles.push(handle);
        }

        let framing = Framing {
            header: self.header(),
            key: self.config.auth_key.as_ref(),
            limits: self.config.wire_limits,
        };
        let header = Header {
            flags: wire::FLAG_SWEEP,
            ..framing.header
        };
        let packets = match framing.key {
            Some(key) => Packets::<C>::auth(
                header,
                &self.store,
                &self.frame_handles,
                key,
                &mut self.scratch,
                framing.limits,
            ),
            None => Packets::<C>::unauth(
                header,
                &self.store,
                &self.frame_handles,
                &mut self.scratch,
                framing.limits,
            ),
        };
        let mut frames: u16 = 0;
        if let Ok(mut packets) = packets {
            let sendq = &mut self.pubsub.sendq;
            loop {
                let unsent = packets.remaining();
                if unsent == 0 {
                    break;
                }
                let Some(slot) = sendq.claim("answer a sweep") else {
                    // Resume at the first cell not sent.
                    next = self.frame_handles[self.frame_handles.len() - unsent].index;
                    break;
                };
                let written = packets.next_into(sendq.buf_mut(slot));
                if let Ok(Some(_)) = written {
                    sendq.send_to(slot, src);
                    frames += 1;
                }
                sendq.settle(slot);
                if !matches!(written, Ok(Some(_))) {
                    break;
                }
            }
        }
        self.pubsub.send_control(
            &framing,
            src,
            Control::SweepDone {
                cursor,
                next,
                frames,
            },
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
        if let Control::Sweep { cursor } = control {
            return self.answer_sweep(src, cursor);
        }
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
