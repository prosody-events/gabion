//! Plumtree dissemination (Leitao, Pereira and Rodrigues, SRDS 2007), kept per
//! root as in `riak_core_broadcast`. The mechanism, the UDP adaptations and
//! every parameter are described under "Design notes" in
//! `crates/gabion/README.md`.
//!
//! The shared scheduler still decides *when* a node originates changes (the
//! heartbeat and the error-budget threshold). This module decides *to whom*:
//! each change goes eagerly to the peers of the active view that are eager for
//! the change's origin, and lazily, as an IHAVE digest, to the rest.

use std::net::SocketAddr;
use std::time::Duration;

use crate::crdt::{
    BucketEpoch, CellHandle, CellStore, Count, Incarnation, NodeDictionary, NodeId, NodeSlot,
    RuleDescriptor,
};
use crate::defaults;
use crate::wire::{self, ControlEntry, ControlKind, FrameKind, Header, Packets};

use super::super::PlumtreeConfig;
use super::{AggregateStore, Clock, GossipRuntime, GossipTransport, Ticker};

/// Largest active view. `⌈ln n + 3⌉` stays below it up to `n ≈ 10^12`.
const VIEW_MAX: usize = 32;
/// `Peer::view_pos` of a peer outside the active view.
pub(super) const NO_POS: u8 = u8::MAX;
/// Distinct announcers one missing origin remembers, to GRAFT in turn.
const ANNOUNCERS: usize = 4;

/// Why a round runs, which decides what it sends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Round {
    /// The 500 ms tick: local and relayed changes, IHAVE digests, a repair
    /// slice.
    Heartbeat,
    /// The error budget of some rule was crossed: local and relayed changes.
    Threshold,
    /// A change arrived `relay_floor` ago: relayed changes only. Local changes
    /// wait for the shared scheduler.
    Relay,
}

/// How a frame's cells are numbered on their edge.
#[derive(Clone, Copy)]
enum Numbering {
    /// Not part of the eager stream.
    None,
    /// The next number of the edge to the destination.
    Next,
    /// A resend under the number the lost packet had.
    Resend(u64),
}

/// Per-peer edge state. The send side numbers eager packets; the receive side
/// tracks the last 64 numbers and asks again for the ones that did not arrive.
#[derive(Default)]
pub(super) struct Edge {
    send_seq: u64,
    recv_incarnation: u32,
    /// The first number seen from this sender incarnation; earlier ones were
    /// never ours to miss.
    recv_first: u64,
    recv_last: u64,
    /// Bit `i` set: number `recv_last - i` arrived.
    recv_mask: u64,
    nack_at: Option<u64>,
    nack_tries: u8,
}

impl Edge {
    /// Numbers missing from the receive window, as the first number of the
    /// window and a mask in which bit `j` stands for number `base + j`.
    fn missing(&self) -> (u64, u64) {
        let base = self.recv_last.saturating_sub(63);
        let mut mask = 0;
        for i in 0..64.min(self.recv_last.saturating_sub(self.recv_first)) {
            if self.recv_mask & (1 << i) == 0 {
                mask |= 1 << (self.recv_last - i - base);
            }
        }
        (base, mask)
    }

    /// Account for the arrival of packet `number`. Returns true when it opened
    /// a new gap.
    ///
    /// A jump past the whole window is an outage (a partition, a pause), not
    /// loss: the edge starts afresh at `number`. NACKing the last 63 numbers of
    /// an outage would resend a sliver of it from the resend ring, mostly
    /// stale; the IHAVE and repair digests catch the node up instead.
    fn arrived(&mut self, number: u64, sender_incarnation: u32) -> bool {
        if self.recv_last == 0
            || self.recv_incarnation != sender_incarnation
            || number >= self.recv_last + 64
        {
            *self = Self {
                send_seq: self.send_seq,
                recv_incarnation: sender_incarnation,
                recv_first: number,
                recv_last: number,
                recv_mask: 1,
                ..Self::default()
            };
        } else if number > self.recv_last {
            let shift = number - self.recv_last;
            self.recv_mask = (self.recv_mask << shift) | 1;
            self.recv_last = number;
            return shift > 1;
        } else if self.recv_last - number < 64 {
            self.recv_mask |= 1 << (self.recv_last - number);
        }
        false
    }
}

/// A change waiting for a round: the cell, the sequence it had when queued,
/// and who delivered it, who must not get it back.
#[derive(Clone, Copy)]
struct Queued {
    handle: CellHandle,
    sequence: u64,
    from: Option<SocketAddr>,
}

/// A change a round pushes: one per cell, in its newest state.
#[derive(Clone, Copy)]
struct Change {
    handle: CellHandle,
    origin: NodeSlot,
    from: Option<SocketAddr>,
}

/// Lazily announced changes of one origin that the tree has not delivered.
///
/// The paper runs a timer per announced message and, when it expires, GRAFTs
/// the first announcer and moves on to the next one for the retry. Changes of
/// one origin are announced as a watermark here, so the record keeps the
/// oldest announcement still running with its own deadline, the newest one
/// with its own, and the first few distinct announcers to GRAFT in turn.
struct Missing {
    node_id: NodeId,
    incarnation: Incarnation,
    /// The announced sequence whose timer runs, and when it expires.
    target: u64,
    deadline: u64,
    /// The highest sequence announced after `target`, and when the
    /// announcement that raised it expires; `0` when there is none.
    newer: u64,
    newer_deadline: u64,
    announcers: [Option<SocketAddr>; ANNOUNCERS],
    /// The announcer the next GRAFT goes to.
    next: usize,
    tries: u8,
}

/// Per-root tree state. `lazy` holds the view positions that are lazy for this
/// origin; every other occupied position is eager, so the two sets partition
/// the view.
#[derive(Clone, Copy)]
struct Root {
    node_id: NodeId,
    incarnation: Incarnation,
    lazy: u32,
    /// The highest sequence of this origin that the tree (an eager, resent or
    /// grafted frame) delivered here. A version that arrived off the tree does
    /// not raise it, so an announcement above it means the tree has not
    /// delivered the change, however the cell got here.
    delivered: u64,
}

/// The last eager packets this node sent, newest overwriting oldest, so a NACK
/// can be answered with the cells the lost packet carried.
struct SentRing {
    dst: Box<[Option<SocketAddr>]>,
    sequence: Box<[u64]>,
    len: Box<[u8]>,
    cells: Box<[CellHandle]>,
    head: usize,
}

impl SentRing {
    fn new() -> Self {
        let n = defaults::PLUMTREE_RESEND_PACKETS;
        Self {
            dst: vec![None; n].into_boxed_slice(),
            sequence: vec![0; n].into_boxed_slice(),
            len: vec![0; n].into_boxed_slice(),
            cells: vec![CellHandle::default(); n * defaults::PLUMTREE_RESEND_CELLS_PER_PACKET]
                .into_boxed_slice(),
            head: 0,
        }
    }

    /// Remember a packet. One with more cells than a slot holds is not kept; a
    /// NACK for it goes unanswered and the repair digests recover its cells.
    fn push(&mut self, dst: SocketAddr, sequence: u64, handles: &[CellHandle]) {
        let width = defaults::PLUMTREE_RESEND_CELLS_PER_PACKET;
        let i = self.head;
        self.head = (self.head + 1) % self.dst.len();
        if handles.len() > width {
            self.dst[i] = None;
            return;
        }
        self.dst[i] = Some(dst);
        self.sequence[i] = sequence;
        self.len[i] = handles.len() as u8;
        self.cells[i * width..i * width + handles.len()].copy_from_slice(handles);
    }

    fn find(&self, dst: SocketAddr, sequence: u64) -> Option<&[CellHandle]> {
        let width = defaults::PLUMTREE_RESEND_CELLS_PER_PACKET;
        let n = self.dst.len();
        (0..n)
            .map(|k| (self.head + n - 1 - k) % n)
            .find(|&i| self.dst[i] == Some(dst) && self.sequence[i] == sequence)
            .map(|i| &self.cells[i * width..i * width + self.len[i] as usize])
    }
}

/// Everything Plumtree adds to the runtime, sized at construction.
pub(super) struct Plumtree {
    cfg: PlumtreeConfig,
    /// The active view: peers this node pushes to, by position. Per-root
    /// lazy masks are indexed by position.
    view: [Option<SocketAddr>; VIEW_MAX],
    /// View positions in use, as a bit mask.
    occupied: u32,
    /// Per origin node slot.
    roots: Box<[Root]>,
    /// Changes waiting for a round, capped at `max_cells_per_tick` each.
    local: Vec<Queued>,
    relay: Vec<Queued>,
    queue_capacity: usize,
    overflow_total: u64,
    /// Frames the full send queue cut short or dropped.
    send_cut_total: u64,
    relay_due: Option<u64>,
    missing: Vec<Missing>,
    /// Earliest time a missing-announcement or NACK timer may fire.
    deadline: Option<u64>,
    sent: SentRing,
    /// Per cell slot: the stored version arrived off the tree (a pull, a
    /// random push) and has not been forwarded. The tree's own
    /// delivery of it is a first delivery, not a duplicate.
    off_tree: Box<[bool]>,
    heartbeats: u64,
    // Scratch, reused every round.
    batch: Vec<Queued>,
    unique: Vec<Change>,
    /// Per cell slot: the round that last collected it, and that round.
    marks: Box<[u32]>,
    epoch: u32,
    routes: Vec<(u8, u32)>,
    entries: Vec<ControlEntry>,
    inbound: Vec<ControlEntry>,
    pulls: Vec<ControlEntry>,
    prunes: Vec<ControlEntry>,
    grafts: Vec<(SocketAddr, ControlEntry)>,
    nacks: Vec<(SocketAddr, u64, u64)>,
    /// Per origin node slot: send cells above this sequence (`u64::MAX`: not
    /// asked for).
    want: Box<[u64]>,
    /// Per origin node slot: the hits held in its settled cells
    /// ([`settled`]) as of `sums_at`, for repair digests.
    sums: Box<[u64]>,
    sums_at: Option<u64>,
    /// The cell slot the next GRAFT or PULL reply starts from.
    reply_cursor: u32,
    /// Per rule slot: its settled cells as of `sums_at`, and scratch for
    /// another instant's.
    settled: Box<[Option<Settled>]>,
    settled_scratch: Box<[Option<Settled>]>,
}

/// Which cells of a rule a repair digest counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Settled {
    /// Cells in these buckets, oldest and newest included.
    Buckets(BucketEpoch, BucketEpoch),
    /// A rule without buckets never expires its cells: those last updated at
    /// or before this instant.
    UpdatedBy(u64),
}

impl Settled {
    fn covers(self, bucket: BucketEpoch, last_update_millis: u64) -> bool {
        match self {
            Self::Buckets(oldest, newest) => (oldest..=newest).contains(&bucket),
            Self::UpdatedBy(cutoff) => last_update_millis <= cutoff,
        }
    }
}

/// The cells of `rule` a repair digest stamped `at` counts: those in buckets
/// closed at least [`defaults::PLUMTREE_DIGEST_SETTLE_MS`] before `at` and
/// still live, so a change still in flight does not read as a hole. Every node
/// keeps the oldest live bucket until its own clock passes the next boundary,
/// so sender and receiver agree on it. `None` when no bucket has settled yet.
fn settled(rule: &RuleDescriptor, at: u64) -> Option<Settled> {
    let width = u64::from(rule.bucket_millis);
    if width == 0 {
        return at
            .checked_sub(defaults::PLUMTREE_DIGEST_SETTLE_MS)
            .map(Settled::UpdatedBy);
    }
    let newest = (at.saturating_sub(defaults::PLUMTREE_DIGEST_SETTLE_MS) / width).checked_sub(1)?;
    let oldest = (at / width).saturating_sub(u64::from(rule.live_buckets()));
    (oldest <= newest).then_some(Settled::Buckets(
        oldest as BucketEpoch,
        newest as BucketEpoch,
    ))
}

impl Plumtree {
    pub(super) fn new<C: Count>(
        cfg: PlumtreeConfig,
        store: &CellStore<C>,
        queue_capacity: usize,
    ) -> Self {
        let nodes = store.node_dictionary().capacity() as usize;
        Self {
            cfg,
            view: [None; VIEW_MAX],
            occupied: 0,
            roots: vec![
                Root {
                    node_id: NodeId(0),
                    incarnation: 0,
                    lazy: 0,
                    delivered: 0,
                };
                nodes
            ]
            .into_boxed_slice(),
            local: Vec::with_capacity(queue_capacity),
            relay: Vec::with_capacity(queue_capacity),
            queue_capacity,
            overflow_total: 0,
            send_cut_total: 0,
            relay_due: None,
            missing: Vec::with_capacity(nodes),
            deadline: None,
            sent: SentRing::new(),
            off_tree: vec![false; store.capacity() as usize].into_boxed_slice(),
            heartbeats: 0,
            batch: Vec::with_capacity(2 * queue_capacity),
            unique: Vec::with_capacity(2 * queue_capacity),
            marks: vec![0; store.capacity() as usize].into_boxed_slice(),
            epoch: 0,
            routes: Vec::with_capacity(8 * queue_capacity),
            entries: Vec::with_capacity(nodes),
            inbound: Vec::with_capacity(nodes),
            pulls: Vec::with_capacity(nodes),
            prunes: Vec::with_capacity(nodes),
            grafts: Vec::with_capacity(nodes),
            nacks: Vec::with_capacity(store.peer_frontiers().peer_capacity() as usize),
            want: vec![u64::MAX; nodes].into_boxed_slice(),
            sums: vec![0; nodes].into_boxed_slice(),
            sums_at: None,
            reply_cursor: 0,
            settled: vec![None; store.rule_dictionary().capacity() as usize].into_boxed_slice(),
            settled_scratch: vec![None; store.rule_dictionary().capacity() as usize]
                .into_boxed_slice(),
        }
    }

    /// The soonest moment the run loop must wake on its own.
    pub(super) fn wake_at(&self) -> Option<u64> {
        match (self.relay_due, self.deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    pub(super) fn queued_local(&self) -> usize {
        self.local.len()
    }

    pub(super) fn queued_relay(&self) -> usize {
        self.relay.len()
    }

    fn view_len(&self) -> usize {
        self.occupied.count_ones() as usize
    }

    fn arm_deadline(&mut self, at: u64) {
        self.deadline = Some(self.deadline.map_or(at, |d| d.min(at)));
    }

    fn arm_relay(&mut self, now: u64) {
        if self.relay_due.is_none() {
            self.relay_due = Some(now + self.cfg.relay_floor.as_millis() as u64);
        }
    }

    /// The root in `slot`. A slot freed and reused by another origin starts
    /// again with every view position eager and nothing delivered.
    fn root_mut(&mut self, nodes: &NodeDictionary, slot: NodeSlot) -> &mut Root {
        let root = &mut self.roots[slot as usize];
        if let Some(d) = nodes.descriptor(slot)
            && (root.node_id, root.incarnation) != (d.node_id, d.incarnation)
        {
            *root = Root {
                node_id: d.node_id,
                incarnation: d.incarnation,
                lazy: 0,
                delivered: 0,
            };
        }
        root
    }

    fn lazy_mut(&mut self, nodes: &NodeDictionary, slot: NodeSlot) -> &mut u32 {
        &mut self.root_mut(nodes, slot).lazy
    }

    fn eager_mask(&mut self, nodes: &NodeDictionary, slot: NodeSlot) -> u32 {
        let occupied = self.occupied;
        occupied & !*self.lazy_mut(nodes, slot)
    }

    /// Queue a change for the next round that may carry it.
    fn queue(&mut self, change: Queued) {
        let list = if change.from.is_some() {
            &mut self.relay
        } else {
            &mut self.local
        };
        if list.len() < self.queue_capacity {
            list.push(change);
            return;
        }
        self.overflow_total = self.overflow_total.saturating_add(1);
        if self.overflow_total.is_power_of_two() {
            tracing::warn!(
                overflow_total = self.overflow_total,
                capacity = self.queue_capacity,
                "Gossip dirty ring overflowed: more changes arrived than the next round can \
                 forward, so the newest are not being pushed along their trees. They stay in this \
                 node's store, and the IHAVE and repair digests recover them, so peers may lag \
                 until then. This is usually a burst of distinct keys; raise \
                 `gossip.max_cells_per_tick` to forward bigger bursts.",
            );
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
    /// Replace the Plumtree parameters. For tests and experiments; a runtime
    /// starts with [`PlumtreeConfig::default`].
    pub fn with_plumtree(mut self, cfg: PlumtreeConfig) -> Self {
        self.pt.cfg = cfg;
        self
    }

    // -- membership -----------------------------------------------------------

    /// The active view holds `⌈ln n + c⌉` peers, as many as one flat gossip
    /// round picks, or every peer in a smaller cluster.
    fn view_target(&self) -> usize {
        let n = self.peers.len();
        let coverage = ((n.max(1) as f64).ln() + defaults::GOSSIP_COVERAGE_MARGIN).ceil() as usize;
        coverage.min(n).min(VIEW_MAX)
    }

    /// Offer the peer at `index`, the `seen`-th to join, to the active view.
    /// While the view has room it joins; after that it replaces a random
    /// member with probability `view / seen` (reservoir sampling), so the view
    /// stays a uniform random sample of the peers however they arrive.
    pub(super) fn view_consider(&mut self, index: usize, seen: usize) {
        let target = self.view_target();
        if self.pt.view_len() < target {
            self.view_insert(index);
        } else if target > 0 && (self.rng.next_u64() % seen as u64) < target as u64 {
            let pick = (self.rng.next_u64() % self.pt.view_len() as u64) as usize;
            if let Some(pos) = (0..VIEW_MAX)
                .filter(|&p| self.pt.occupied & (1 << p) != 0)
                .nth(pick)
            {
                self.view_evict(pos);
                self.view_insert(index);
            }
        }
    }

    /// Put the peer at `index` in a free view position, eager for every root
    /// (a new neighbour starts on every tree, as in the paper).
    fn view_insert(&mut self, index: usize) {
        let free = !self.pt.occupied & ((1u64 << VIEW_MAX) - 1) as u32;
        if free == 0 {
            return;
        }
        let pos = free.trailing_zeros() as usize;
        self.pt.view[pos] = Some(self.peers[index].addr);
        self.pt.occupied |= 1 << pos;
        self.peers[index].view_pos = pos as u8;
        for root in self.pt.roots.iter_mut() {
            root.lazy &= !(1 << pos);
        }
    }

    pub(super) fn view_evict(&mut self, pos: usize) {
        let Some(addr) = self.pt.view[pos].take() else {
            return;
        };
        self.pt.occupied &= !(1 << pos);
        if let Some(peer) = self.peers.iter_mut().find(|p| p.addr == addr) {
            peer.view_pos = NO_POS;
        }
        for root in self.pt.roots.iter_mut() {
            root.lazy &= !(1 << pos);
        }
    }

    /// Fill view positions left empty by a departure or by cluster growth with
    /// uniformly random peers from outside the view.
    pub(super) fn view_refill(&mut self) {
        let target = self.view_target();
        while self.pt.view_len() < target {
            let outside = self.peers.iter().filter(|p| p.view_pos == NO_POS).count();
            if outside == 0 {
                return;
            }
            let pick = (self.rng.next_u64() % outside as u64) as usize;
            let Some(index) = self
                .peers
                .iter()
                .enumerate()
                .filter(|(_, p)| p.view_pos == NO_POS)
                .nth(pick)
                .map(|(i, _)| i)
            else {
                return;
            };
            self.view_insert(index);
        }
    }

    /// How far the tree has delivered the origin in `slot` here: what an
    /// announcement is compared with. This node's own changes need no tree.
    fn tree_held(&mut self, slot: NodeSlot) -> u64 {
        if slot == self.store.local_node_slot() {
            self.store.origin_watermark(slot)
        } else {
            self.pt
                .root_mut(self.store.node_dictionary(), slot)
                .delivered
        }
    }

    fn random_peer(&mut self) -> Option<SocketAddr> {
        if self.peers.is_empty() {
            return None;
        }
        let i = (self.rng.next_u64() % self.peers.len() as u64) as usize;
        Some(self.peers[i].addr)
    }

    // -- rounds -----------------------------------------------------------------

    /// Queue the cells the last local hit raised, to go out in the next
    /// heartbeat or threshold round.
    pub(super) fn queue_local_changes(&mut self) {
        for &handle in &self.sink_buf.handles {
            if let Some(row) = self.store.get(handle) {
                self.pt.off_tree[handle.index as usize] = false;
                self.pt.queue(Queued {
                    handle,
                    sequence: row.origin_sequence,
                    from: None,
                });
            }
        }
    }

    pub(super) fn handle_gossip_tick(&mut self, round: Round) {
        let now = self.clock.now_millis();
        if round != Round::Relay {
            self.ticks_total = self.ticks_total.saturating_add(1);
            self.expiration_buf.clear();
            self.store.expire_at(now, &mut self.expiration_buf);
        }
        self.pt.relay_due = None;
        if self.store.is_empty() || self.peers.is_empty() {
            self.pt.local.clear();
            self.pt.relay.clear();
            return;
        }
        if self.send_free.is_empty() {
            // No room to send: keep the changes for the next round.
            if !self.pt.relay.is_empty() || !self.pt.local.is_empty() {
                self.pt.arm_relay(now);
            }
            return;
        }
        if round == Round::Heartbeat {
            self.pt.heartbeats += 1;
            self.view_refill();
        }
        self.eager_push(round);
        if round == Round::Heartbeat {
            self.lazy_announce();
            self.send_digest(now);
        }
        if round != Round::Relay {
            self.sweep_rule_pending(now);
        }
        self.check_timers(now);
    }

    /// Reset the per-rule pending counters the threshold trigger reads. Every
    /// rule with pending hits counts as touched by the round: its hits either
    /// rode out in a frame or wait in the queue for the next one.
    fn sweep_rule_pending(&mut self, now_millis: u64) {
        let stamp = now_millis.max(1);
        for slot in 0..self.rule_pending.len() {
            if self.rule_pending[slot] != 0 {
                self.rule_pending[slot] = 0;
                self.rule_last_emit_ms[slot] = stamp;
            }
        }
    }

    /// Send the queued changes along their roots' eager edges: one frame per
    /// destination, never back to the peer a change came from.
    fn eager_push(&mut self, round: Round) {
        self.collect_changes(round);
        let changes = std::mem::take(&mut self.pt.unique);

        self.pt.routes.clear();
        for (i, change) in changes.iter().enumerate() {
            let mut eager = self
                .pt
                .eager_mask(self.store.node_dictionary(), change.origin);
            while eager != 0 {
                let pos = eager.trailing_zeros() as usize;
                eager &= eager - 1;
                if self.pt.view[pos] != change.from {
                    self.pt.routes.push((pos as u8, i as u32));
                }
            }
        }
        self.pt.routes.sort_unstable();

        let frame_cap = self.config.max_cells_per_tick;
        let mut fanout = 0;
        let mut k = 0;
        while k < self.pt.routes.len() {
            let pos = self.pt.routes[k].0;
            self.frame_handles.clear();
            while k < self.pt.routes.len() && self.pt.routes[k].0 == pos {
                if self.frame_handles.len() < frame_cap {
                    self.frame_handles
                        .push(changes[self.pt.routes[k].1 as usize].handle);
                }
                k += 1;
            }
            if let Some(dst) = self.pt.view[pos as usize] {
                fanout += 1;
                self.send_cells(dst, FrameKind::Eager, Numbering::Next);
            }
        }
        if fanout > 0 {
            if round != Round::Relay {
                self.dirty_ticks = self.dirty_ticks.saturating_add(1);
            }
            self.last_effective_fanout = fanout;
            self.peak_effective_fanout = self.peak_effective_fanout.max(fanout);
        }

        for _ in 0..self.pt.cfg.extra_random {
            self.random_push(&changes);
        }
        self.pt.unique = changes;
    }

    /// Move the queued changes into `pt.unique`, one entry per cell. A frame
    /// always carries a cell's newest state, so only the newest entry for a
    /// cell counts. Its sender is skipped only if it sent the newest state;
    /// when the state moved on since (a pull brought a newer version), nobody
    /// is skipped.
    fn collect_changes(&mut self, round: Round) {
        let mut batch = std::mem::take(&mut self.pt.batch);
        batch.clear();
        if round != Round::Relay {
            batch.append(&mut self.pt.local);
        }
        batch.append(&mut self.pt.relay);

        self.pt.epoch = self.pt.epoch.wrapping_add(1);
        if self.pt.epoch == 0 {
            self.pt.marks.fill(0);
            self.pt.epoch = 1;
        }
        self.pt.unique.clear();
        for change in batch.iter().rev() {
            let Some(row) = self.store.get(change.handle) else {
                continue;
            };
            let mark = &mut self.pt.marks[change.handle.index as usize];
            if *mark == self.pt.epoch {
                continue;
            }
            *mark = self.pt.epoch;
            let fresh = row.origin_sequence == change.sequence;
            self.pt.unique.push(Change {
                handle: change.handle,
                origin: row.key.origin,
                from: change.from.filter(|_| fresh),
            });
        }
        batch.clear();
        self.pt.batch = batch;
    }

    /// Hybrid: give the round's changes to one random peer that is not on the
    /// tree of their roots. The cells are kept but not forwarded; the tree
    /// still delivers them.
    fn random_push(&mut self, changes: &[Change]) {
        let Some(dst) = self.random_peer() else {
            return;
        };
        let pos = self
            .peers
            .iter()
            .find(|p| p.addr == dst)
            .map(|p| p.view_pos);
        self.frame_handles.clear();
        for change in changes {
            if change.from == Some(dst)
                || self.frame_handles.len() >= self.config.max_cells_per_tick
            {
                continue;
            }
            let eager = self
                .pt
                .eager_mask(self.store.node_dictionary(), change.origin);
            if pos.is_some_and(|p| p != NO_POS && eager & (1 << p) != 0) {
                continue;
            }
            self.frame_handles.push(change.handle);
        }
        if !self.frame_handles.is_empty() {
            self.send_cells(dst, FrameKind::Random, Numbering::None);
        }
    }

    /// IHAVE: tell `lazy_targets` lazy peers how far this node holds each
    /// origin they are lazy for. Between full digests only origins that moved
    /// since the peer last heard from us, or told us, are listed.
    fn lazy_announce(&mut self) {
        let every = u64::from(self.pt.cfg.full_ihave_every);
        let full = every != 0 && self.pt.heartbeats.is_multiple_of(every);
        let slots = self.store.node_dictionary().capacity();

        let mut any_lazy = 0u32;
        for slot in 0..slots {
            if self.store.origin_watermark(slot) != 0 {
                any_lazy |= *self.pt.lazy_mut(self.store.node_dictionary(), slot);
            }
        }
        let mut candidates = [0u8; VIEW_MAX];
        let mut count = 0;
        for pos in 0..VIEW_MAX {
            if any_lazy & self.pt.occupied & (1 << pos) != 0 {
                candidates[count] = pos as u8;
                count += 1;
            }
        }
        let targets = self.pt.cfg.lazy_targets.min(count);
        for i in 0..targets {
            let j = i + (self.rng.next_u64() % (count - i) as u64) as usize;
            candidates.swap(i, j);
            let pos = candidates[i] as usize;
            let Some(dst) = self.pt.view[pos] else {
                continue;
            };
            let peer_slot = self
                .peers
                .iter()
                .find(|p| p.addr == dst)
                .and_then(|p| p.peer_slot);

            let mut entries = std::mem::take(&mut self.pt.entries);
            entries.clear();
            for slot in 0..slots {
                let watermark = self.store.origin_watermark(slot);
                if watermark == 0
                    || *self.pt.lazy_mut(self.store.node_dictionary(), slot) & (1 << pos) == 0
                {
                    continue;
                }
                let Some(node) = self.store.node_dictionary().descriptor(slot) else {
                    continue;
                };
                let entry = ControlEntry {
                    node_id: node.node_id,
                    incarnation: node.incarnation,
                    sequence: watermark,
                };
                if let Some(ps) = peer_slot {
                    let frontier = self.store.peer_frontiers_mut();
                    let acked = frontier.last_acked(ps, slot);
                    let known = if full {
                        acked
                    } else {
                        acked.max(frontier.last_sent(ps, slot))
                    };
                    if watermark <= known {
                        continue;
                    }
                    frontier.record_sent(ps, slot, watermark);
                }
                entries.push(entry);
            }
            if !entries.is_empty() {
                self.send_control(dst, ControlKind::IHave, (0, 0), &entries);
            }
            self.pt.entries = entries;
        }
    }

    /// The repair lane behind the tree: tell one random peer how many hits this
    /// node holds of each origin in buckets that should have settled. A peer
    /// that holds fewer has a hole the tree will not fill, and pulls the
    /// origin's cells.
    fn send_digest(&mut self, now: u64) {
        let Some(dst) = self.random_peer() else {
            return;
        };
        self.pt.sums_at = None;
        self.settled_sums(now);
        let mut entries = std::mem::take(&mut self.pt.entries);
        entries.clear();
        for slot in 0..self.store.node_dictionary().capacity() {
            let sum = self.pt.sums[slot as usize];
            if sum == 0 {
                continue;
            }
            if let Some(node) = self.store.node_dictionary().descriptor(slot) {
                entries.push(ControlEntry {
                    node_id: node.node_id,
                    incarnation: node.incarnation,
                    sequence: sum,
                });
            }
        }
        if !entries.is_empty() {
            self.send_control(dst, ControlKind::Digest, (now, 0), &entries);
        }
        self.pt.entries = entries;
    }

    /// A repair digest from `src`, stamped `at`: pull every origin of which
    /// this node holds fewer settled hits.
    fn on_digest(&mut self, src: SocketAddr, at: u64) {
        self.settled_sums(at);
        self.pt.pulls.clear();
        for i in 0..self.pt.inbound.len() {
            let e = self.pt.inbound[i];
            let held = self
                .store
                .find_node(e.node_id, e.incarnation)
                .map_or(0, |slot| self.pt.sums[slot as usize]);
            if held < e.sequence {
                self.pt.pulls.push(ControlEntry { sequence: 0, ..e });
            }
        }
        self.send_prunes_and_pulls(src);
    }

    /// Fill `pt.sums` with the hits held per origin in the buckets settled at
    /// `at`. One pass over the store; skipped when the sums already cover the
    /// same buckets. A heartbeat recomputes them before its own digest, so
    /// digests received in between compare against sums at most one
    /// heartbeat old.
    fn settled_sums(&mut self, at: u64) {
        let rules = self.store.rule_dictionary();
        for slot in 0..rules.capacity() {
            self.pt.settled_scratch[slot as usize] =
                rules.descriptor(slot).and_then(|rule| settled(rule, at));
        }
        if self.pt.sums_at.is_some() && self.pt.settled == self.pt.settled_scratch {
            return;
        }
        std::mem::swap(&mut self.pt.settled, &mut self.pt.settled_scratch);
        self.pt.sums.fill(0);
        for handle in self.store.active_handles() {
            let Some(row) = self.store.get(handle) else {
                continue;
            };
            if self.pt.settled[row.key.rule as usize]
                .is_some_and(|s| s.covers(row.key.bucket, row.last_update_millis))
            {
                let sum = &mut self.pt.sums[row.key.origin as usize];
                *sum = sum.saturating_add(row.count.into());
            }
        }
        self.pt.sums_at = Some(at);
    }

    // -- wake-ups
    // ---------------------------------------------------------------

    /// Keep one timer armed for the soonest relay round or protocol timer.
    /// The clock only offers periodic tickers, so a deadline is a fresh
    /// ticker whose first tick, which is immediate, is consumed here; the
    /// second arrives `at - now` later.
    pub(super) async fn arm_wake(&mut self) {
        let Some(at) = self.pt.wake_at() else {
            self.wake = None;
            return;
        };
        if self.wake.as_ref().is_some_and(|(armed, _)| *armed == at) {
            return;
        }
        let wait = at.saturating_sub(self.clock.now_millis()).max(1);
        let mut ticker = self.clock.ticker(Duration::from_millis(wait));
        ticker.tick().await;
        self.wake = Some((at, ticker));
    }

    pub(super) fn handle_wake(&mut self) {
        let now = self.clock.now_millis();
        if self.pt.relay_due.is_some_and(|due| due <= now) {
            self.handle_gossip_tick(Round::Relay);
        } else {
            self.check_timers(now);
        }
    }

    /// Fire due timers: a GRAFT for each announcement whose change did not
    /// arrive within `missing_timeout`, a NACK for each gap that outlived its
    /// grace. Then recompute when the next one is due.
    fn check_timers(&mut self, now: u64) {
        let timeout = self.pt.cfg.missing_timeout.as_millis() as u64;
        let retry = self.pt.cfg.nack_retry.as_millis() as u64;

        self.pt.grafts.clear();
        let mut k = 0;
        while k < self.pt.missing.len() {
            let (node_id, incarnation) =
                (self.pt.missing[k].node_id, self.pt.missing[k].incarnation);
            let held = self
                .store
                .find_node(node_id, incarnation)
                .map_or(0, |slot| self.tree_held(slot));
            let m = &mut self.pt.missing[k];
            if held >= m.target {
                if m.newer <= held {
                    self.pt.missing.swap_remove(k);
                    continue;
                }
                // The oldest announcement was delivered; the newest one's
                // timer takes over.
                m.target = m.newer;
                m.deadline = m.newer_deadline;
                m.newer = 0;
                m.tries = 0;
            }
            if m.deadline <= now {
                if m.tries >= defaults::PLUMTREE_MAX_GRAFT_TRIES {
                    self.pt.missing.swap_remove(k);
                    continue;
                }
                // The announced change never came by the tree: GRAFT the next
                // announcer, asking for everything above what the tree
                // delivered.
                let count = m.announcers.iter().flatten().count();
                let Some(announcer) = m.announcers[m.next % count] else {
                    self.pt.missing.swap_remove(k);
                    continue;
                };
                self.pt.grafts.push((
                    announcer,
                    ControlEntry {
                        node_id,
                        incarnation,
                        sequence: held,
                    },
                ));
                m.next = (m.next + 1) % count;
                m.tries += 1;
                m.deadline = now + timeout;
                if m.newer != 0 && m.newer_deadline <= now {
                    m.target = m.target.max(m.newer);
                    m.newer = 0;
                }
            }
            k += 1;
        }
        self.pt.grafts.sort_unstable_by_key(|(addr, _)| *addr);
        let mut entries = std::mem::take(&mut self.pt.entries);
        let mut i = 0;
        while i < self.pt.grafts.len() {
            let dst = self.pt.grafts[i].0;
            entries.clear();
            while i < self.pt.grafts.len() && self.pt.grafts[i].0 == dst {
                entries.push(self.pt.grafts[i].1);
                i += 1;
            }
            self.send_control(dst, ControlKind::Graft, (0, 0), &entries);
        }
        self.pt.entries = entries;

        self.pt.nacks.clear();
        let mut next = self.pt.missing.iter().map(|m| m.deadline).min();
        for peer in self.peers.iter_mut() {
            let edge = &mut peer.edge;
            if edge.nack_at.is_some_and(|at| at <= now) {
                let (base, mask) = edge.missing();
                if mask == 0 || edge.nack_tries >= defaults::PLUMTREE_MAX_NACK_TRIES {
                    edge.nack_at = None;
                } else {
                    edge.nack_tries += 1;
                    edge.nack_at = Some(now + retry);
                    self.pt.nacks.push((peer.addr, base, mask));
                }
            }
            if let Some(at) = edge.nack_at {
                next = Some(next.map_or(at, |n| n.min(at)));
            }
        }
        for i in 0..self.pt.nacks.len() {
            let (dst, base, mask) = self.pt.nacks[i];
            self.send_control(dst, ControlKind::Nack, (base, mask), &[]);
        }
        self.pt.deadline = next;
    }

    // -- inbound --------------------------------------------------------------

    pub(super) fn handle_inbound(&mut self, n: usize, src: SocketAddr) {
        match wire::frame_kind(&self.recv_buf[..n]) {
            Some(FrameKind::Control) => self.handle_control(n, src),
            Some(kind) => self.handle_data(n, src, kind),
            None => self.note_decode_reject(src, wire::DecodeError::BadControl),
        }
    }

    fn note_decode_reject(&mut self, src: SocketAddr, err: wire::DecodeError) {
        self.decode_reject_count = self.decode_reject_count.saturating_add(1);
        if self.decode_reject_count.is_power_of_two() {
            tracing::warn!(
                peer = %src,
                error = %err,
                rejected_total = self.decode_reject_count,
                "Could not understand a gossip message from this \
                 peer; dropping it. Common causes: the peer is \
                 running a different gabion version, the peers are \
                 configured with different counter sizes \
                 (`storage.count_width`), the cluster authentication \
                 key (`gossip.auth_key`) does not match between \
                 peers, or the peer is sending messages larger than \
                 this node's `gossip.max_payload_bytes`. Check the \
                 peer's version and gabion config to find the \
                 mismatch.",
            );
        }
    }

    fn handle_data(&mut self, n: usize, src: SocketAddr, kind: FrameKind) {
        self.obs_buf.clear();
        self.ack_buf.clear();
        self.sink_buf.clear();
        let bytes = &self.recv_buf[..n];
        let decoded = {
            let obs_buf = &mut self.obs_buf;
            let ack_buf = &mut self.ack_buf;
            let mut on_cell = |cell: wire::WireCell<C>| {
                ack_buf.push(
                    cell.origin_node_id,
                    cell.origin_incarnation,
                    cell.origin_sequence,
                );
                obs_buf.push(crate::crdt::Observation {
                    rule_fingerprint: cell.rule_fingerprint,
                    key_hash: cell.key_hash,
                    bucket: cell.bucket,
                    origin: cell.origin_node_id,
                    incarnation: cell.origin_incarnation,
                    count: cell.count,
                    last_update_millis: cell.last_update_millis,
                    origin_sequence: cell.origin_sequence,
                });
            };
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
            Ok(s) => s,
            Err(err) => {
                self.note_decode_reject(src, err);
                return;
            }
        };
        let now = self.clock.now_millis();
        let peer_index = self.peers.iter().position(|p| p.addr == src);
        if let (Some(pi), FrameKind::Eager | FrameKind::Resent) = (peer_index, kind) {
            self.edge_receive(pi, &summary.header, now);
        }

        // First contact: an origin this node has no cells of yet. Ask the
        // sender, who has just relayed it, for the origin's older cells.
        self.pt.pulls.clear();
        if self.pt.cfg.first_contact_pull {
            for i in 0..self.obs_buf.len() {
                let node_id = self.obs_buf.origin_node_ids[i];
                let incarnation = self.obs_buf.incarnations[i];
                if self.store.find_node(node_id, incarnation).is_none()
                    && !self
                        .pt
                        .pulls
                        .iter()
                        .any(|e| e.node_id == node_id && e.incarnation == incarnation)
                {
                    self.pt.pulls.push(ControlEntry {
                        node_id,
                        incarnation,
                        sequence: 0,
                    });
                }
            }
        }

        // Merge row by row: a row that raised its cell is a change to forward
        // (unless it came on a frame kind that never forwards); a duplicate on
        // an eager frame is the PRUNE signal for that origin.
        self.pt.prunes.clear();
        let forwards = matches!(
            kind,
            FrameKind::Eager | FrameKind::Resent | FrameKind::Grafted
        );
        for i in 0..self.obs_buf.len() {
            let Some(outcome) = self
                .store
                .merge_remote_row(&self.obs_buf, i, &mut self.sink_buf)
            else {
                continue;
            };
            let slot = outcome.handle.index as usize;
            if forwards && let Some(row) = self.store.get(outcome.handle) {
                let root = self
                    .pt
                    .root_mut(self.store.node_dictionary(), row.key.origin);
                root.delivered = root.delivered.max(self.obs_buf.origin_sequences[i]);
            }
            if outcome.changed {
                self.pt.off_tree[slot] = !forwards;
                if forwards {
                    self.pt.queue(Queued {
                        handle: outcome.handle,
                        sequence: self.obs_buf.origin_sequences[i],
                        from: Some(src),
                    });
                }
            } else if forwards {
                if std::mem::take(&mut self.pt.off_tree[slot]) {
                    // First tree delivery of a version that arrived off the
                    // tree (a pull, a random push): it was
                    // never forwarded, so forward it now. It says nothing
                    // against this edge.
                    if let Some(row) = self.store.get(outcome.handle) {
                        self.pt.queue(Queued {
                            handle: outcome.handle,
                            sequence: row.origin_sequence,
                            from: Some(src),
                        });
                    }
                } else if kind == FrameKind::Eager {
                    let node_id = self.obs_buf.origin_node_ids[i];
                    let incarnation = self.obs_buf.incarnations[i];
                    if !self
                        .pt
                        .prunes
                        .iter()
                        .any(|e| e.node_id == node_id && e.incarnation == incarnation)
                    {
                        self.pt.prunes.push(ControlEntry {
                            node_id,
                            incarnation,
                            sequence: 0,
                        });
                    }
                }
            }
        }
        if !self.pt.relay.is_empty() {
            self.pt.arm_relay(now);
        }

        // Frontier update: the sender holds each origin up to the sequences it
        // just sent. A hint that only trims IHAVE digests.
        let sender_id = NodeId(summary.header.sender_node_id);
        if let Some(peer_slot) = self.store.peer_frontiers_mut().intern_peer(sender_id) {
            for i in 0..self.ack_buf.len() {
                let origin_id = self.ack_buf.origin_node_ids[i];
                let incarnation = self.ack_buf.incarnations[i];
                let sequence = self.ack_buf.origin_sequences[i];
                if let Some(node_slot) = self.store.node_dictionary().find(origin_id, incarnation) {
                    self.store
                        .peer_frontiers_mut()
                        .record_acked(peer_slot, node_slot, sequence);
                }
            }
            // `node_id` and `peer_slot` are always set together.
            if let Some(pi) = peer_index {
                self.peers[pi].node_id = Some(sender_id);
                self.peers[pi].peer_slot = Some(peer_slot);
            }
        }

        self.send_prunes_and_pulls(src);
    }

    /// Send what a frame from `src` earned: a PRUNE for each origin it
    /// duplicated, and a PULL for each origin it introduced.
    fn send_prunes_and_pulls(&mut self, src: SocketAddr) {
        for (kind, mut entries) in [
            (ControlKind::Prune, std::mem::take(&mut self.pt.prunes)),
            (ControlKind::Pull, std::mem::take(&mut self.pt.pulls)),
        ] {
            if !entries.is_empty() {
                self.send_control(src, kind, (0, 0), &entries);
                entries.clear();
            }
            match kind {
                ControlKind::Prune => self.pt.prunes = entries,
                _ => self.pt.pulls = entries,
            }
        }
    }

    /// Track the numbers of eager packets from `pi`. A number that skips
    /// opens a gap; the NACK for it waits one relay floor, since datagrams of
    /// one burst may reorder.
    fn edge_receive(&mut self, pi: usize, header: &Header, now: u64) {
        let number = header.min_origin_sequence;
        if number == 0 {
            return;
        }
        let grace = self.pt.cfg.relay_floor.as_millis() as u64;
        let edge = &mut self.peers[pi].edge;
        if edge.arrived(number, header.sender_incarnation) {
            edge.nack_tries = 0;
            edge.nack_at = Some(edge.nack_at.map_or(now + grace, |at| at.min(now + grace)));
            self.pt.arm_deadline(now + grace);
        }
    }

    fn handle_control(&mut self, n: usize, src: SocketAddr) {
        self.pt.inbound.clear();
        let bytes = &self.recv_buf[..n];
        let inbound = &mut self.pt.inbound;
        let visit = |entry| inbound.push(entry);
        let decoded = match self.config.auth_key.as_ref() {
            Some(key) => wire::decode_control_auth(bytes, key, self.config.wire_limits, visit),
            None => wire::decode_control_unauth(bytes, self.config.wire_limits, visit),
        };
        let (header, kind) = match decoded {
            Ok(d) => d,
            Err(err) => {
                self.note_decode_reject(src, err);
                return;
            }
        };
        let now = self.clock.now_millis();
        let peer_index = self.peers.iter().position(|p| p.addr == src);
        let view_pos = peer_index
            .map(|pi| self.peers[pi].view_pos)
            .filter(|&p| p != NO_POS);
        match kind {
            ControlKind::IHave => self.on_ihave(src, &header, now),
            ControlKind::Graft | ControlKind::Pull => {
                if kind == ControlKind::Graft
                    && let Some(pos) = view_pos
                {
                    // The requester missed what we announced: eager again.
                    for i in 0..self.pt.inbound.len() {
                        let e = self.pt.inbound[i];
                        if let Some(slot) = self.store.find_node(e.node_id, e.incarnation) {
                            *self.pt.lazy_mut(self.store.node_dictionary(), slot) &= !(1 << pos);
                        }
                    }
                }
                let reply = if kind == ControlKind::Graft {
                    FrameKind::Grafted
                } else {
                    FrameKind::Plain
                };
                self.reply_with_cells(src, reply);
            }
            ControlKind::Prune => {
                if let Some(pos) = view_pos {
                    for i in 0..self.pt.inbound.len() {
                        let e = self.pt.inbound[i];
                        if let Some(slot) = self.store.find_node(e.node_id, e.incarnation) {
                            *self.pt.lazy_mut(self.store.node_dictionary(), slot) |= 1 << pos;
                        }
                    }
                }
            }
            ControlKind::Nack => {
                self.resend(src, header.min_origin_sequence, header.max_origin_sequence)
            }
            ControlKind::Digest => self.on_digest(src, header.min_origin_sequence),
        }
    }

    /// An IHAVE digest from `src`. Each entry the node holds less of than
    /// announced starts (or tightens) a missing-timeout.
    fn on_ihave(&mut self, src: SocketAddr, header: &Header, now: u64) {
        let timeout = self.pt.cfg.missing_timeout.as_millis() as u64;
        let sender = NodeId(header.sender_node_id);
        let peer_slot = self.store.peer_frontiers_mut().intern_peer(sender);
        self.pt.pulls.clear();
        for i in 0..self.pt.inbound.len() {
            let e = self.pt.inbound[i];
            let Some(slot) = self.store.find_node(e.node_id, e.incarnation) else {
                if self.pt.cfg.first_contact_pull {
                    self.pt.pulls.push(ControlEntry { sequence: 0, ..e });
                    continue;
                }
                self.note_missing(&e, src, now + timeout);
                continue;
            };
            if let Some(ps) = peer_slot {
                self.store
                    .peer_frontiers_mut()
                    .record_acked(ps, slot, e.sequence);
            }
            if e.sequence > self.tree_held(slot) {
                self.note_missing(&e, src, now + timeout);
            }
        }
        self.send_prunes_and_pulls(src);
    }

    /// Remember that `announcer` announced `e`, which the tree has not
    /// delivered; its timer expires at `deadline`.
    fn note_missing(&mut self, e: &ControlEntry, announcer: SocketAddr, deadline: u64) {
        if let Some(m) = self
            .pt
            .missing
            .iter_mut()
            .find(|m| m.node_id == e.node_id && m.incarnation == e.incarnation)
        {
            if let Some(free) = m
                .announcers
                .iter_mut()
                .find(|a| a.is_none_or(|a| a == announcer))
            {
                *free = Some(announcer);
            }
            if e.sequence > m.target && e.sequence > m.newer {
                m.newer = e.sequence;
                m.newer_deadline = deadline;
            }
        } else if self.pt.missing.len() < self.pt.missing.capacity() {
            let mut announcers = [None; ANNOUNCERS];
            announcers[0] = Some(announcer);
            self.pt.missing.push(Missing {
                node_id: e.node_id,
                incarnation: e.incarnation,
                target: e.sequence,
                deadline,
                newer: 0,
                newer_deadline: 0,
                announcers,
                next: 0,
                tries: 0,
            });
            self.pt.arm_deadline(deadline);
        }
    }

    /// Answer a GRAFT or PULL: the live cells of each requested origin above
    /// the requested sequence, in one pass over the store, at most
    /// [`defaults::PLUMTREE_REPLY_CELLS`] of them so a reply never floods the
    /// requester's receive buffer. What did not fit shows up as a difference
    /// in the requester's next repair digest, which pulls it; each reply
    /// starts where the last one stopped, so repeated pulls make progress.
    fn reply_with_cells(&mut self, dst: SocketAddr, kind: FrameKind) {
        for i in 0..self.pt.inbound.len() {
            let e = self.pt.inbound[i];
            if let Some(slot) = self.store.find_node(e.node_id, e.incarnation) {
                self.pt.want[slot as usize] = e.sequence;
            }
        }
        self.frame_handles.clear();
        let start = self.pt.reply_cursor;
        let rotated = self
            .store
            .active_handles()
            .filter(|h| h.index >= start)
            .chain(self.store.active_handles().filter(|h| h.index < start));
        let mut rest = None;
        for handle in rotated {
            if self.frame_handles.len() >= defaults::PLUMTREE_REPLY_CELLS {
                rest = Some(handle.index);
                break;
            }
            let Some(row) = self.store.get(handle) else {
                continue;
            };
            let above = self.pt.want[row.key.origin as usize];
            if above != u64::MAX && row.origin_sequence > above {
                self.frame_handles.push(handle);
            }
        }
        for i in 0..self.pt.inbound.len() {
            let e = self.pt.inbound[i];
            if let Some(slot) = self.store.find_node(e.node_id, e.incarnation) {
                self.pt.want[slot as usize] = u64::MAX;
            }
        }
        if !self.frame_handles.is_empty() {
            let sent = self.send_cells(dst, kind, Numbering::None);
            if let Some(unsent) = self.frame_handles.get(sent) {
                rest = Some(unsent.index);
            }
        }
        if let Some(index) = rest {
            self.pt.reply_cursor = index;
        }
    }

    /// Answer a NACK: send again each remembered eager packet it names (bit
    /// `j` of `missing` stands for number `base + j`), under its original
    /// number.
    fn resend(&mut self, dst: SocketAddr, base: u64, missing: u64) {
        for j in (0..64).filter(|j| missing & (1 << j) != 0) {
            let number = base + j;
            let Some(handles) = self.pt.sent.find(dst, number) else {
                continue;
            };
            self.frame_handles.clear();
            self.frame_handles.extend_from_slice(handles);
            self.send_cells(dst, FrameKind::Resent, Numbering::Resend(number));
        }
    }

    // -- outbound -------------------------------------------------------------

    fn header(&self, flags: u16) -> Header {
        Header {
            cluster_id_hash: self.config.cluster_id_hash,
            sender_node_id: self.store.local_identity().node_id.0,
            sender_incarnation: self.store.local_identity().incarnation,
            count_width: 0,
            cell_count: 0,
            body_len: 0,
            min_origin_sequence: 0,
            max_origin_sequence: 0,
            flags,
        }
    }

    /// Encode `self.frame_handles` into free send slots, as many packets as the
    /// slots allow, and return how many of the cells went out; cells that do
    /// not fit are left to the IHAVE and repair digests.
    fn send_cells(&mut self, dst: SocketAddr, kind: FrameKind, numbering: Numbering) -> usize {
        let (cursor, cut) = {
            let header = self.header(kind.flags());
            let packets = match self.config.auth_key.as_ref() {
                Some(key) => Packets::<C>::auth(
                    header,
                    &self.store,
                    &self.frame_handles,
                    key,
                    &mut self.scratch,
                    self.config.wire_limits,
                ),
                None => Packets::<C>::unauth(
                    header,
                    &self.store,
                    &self.frame_handles,
                    &mut self.scratch,
                    self.config.wire_limits,
                ),
            };
            let Ok(mut packets) = packets else {
                return 0;
            };
            let peer = self.peers.iter().position(|p| p.addr == dst);
            let mut resend = match numbering {
                Numbering::Resend(number) => Some(number),
                _ => None,
            };
            let mut cursor = 0;
            let mut cut = false;
            while packets.remaining() > 0 {
                let Some(slot) = self.send_free.pop() else {
                    cut = true;
                    break;
                };
                let before = packets.remaining();
                let buf = &mut self.send_pool[slot as usize];
                let number = match (numbering, peer) {
                    (Numbering::Next, Some(pi)) => Some(self.peers[pi].edge.send_seq + 1),
                    (Numbering::Resend(_), _) => Some(resend.take().unwrap_or(0)),
                    _ => None,
                };
                let written = match number {
                    Some(number) => packets.next_into_numbered(buf, number),
                    None => packets.next_into(buf),
                };
                if !matches!(written, Ok(Some(_))) {
                    self.send_free.push(slot);
                    break;
                }
                let consumed = before - packets.remaining();
                if let (Numbering::Next, Some(number), Some(pi)) = (numbering, number, peer) {
                    self.peers[pi].edge.send_seq = number;
                    self.pt
                        .sent
                        .push(dst, number, &self.frame_handles[cursor..cursor + consumed]);
                }
                cursor += consumed;
                self.send_pending.push_back((dst, slot));
            }
            packets.abandon();
            (cursor, cut)
        };
        if cut {
            self.note_send_cut(dst, kind, self.frame_handles.len() - cursor);
        }
        cursor
    }

    /// A frame to `dst` found the send queue full and lost its tail
    /// (`cells_unsent` cells, none for a control frame). Warned on
    /// power-of-two totals.
    fn note_send_cut(&mut self, dst: SocketAddr, kind: FrameKind, cells_unsent: usize) {
        self.pt.send_cut_total = self.pt.send_cut_total.saturating_add(1);
        if self.pt.send_cut_total.is_power_of_two() {
            tracing::warn!(
                peer = %dst,
                frame_kind = ?kind,
                cells_unsent,
                send_cut_total = self.pt.send_cut_total,
                capacity = self.config.send_queue_capacity,
                "Gossip send queue is full, so part of a message to this peer was not sent. \
                 Unsent changes stay in this node's store and the IHAVE and repair digests \
                 deliver them, so the peer may lag by a few seconds. This happens when a large \
                 catch-up reply or a burst of changes meets a queue that is already full; if it \
                 keeps happening, raise `gossip.send_queue_capacity`.",
            );
        }
    }

    /// Encode `entries` as control packets of `kind`. `range` is the edge
    /// sequence range of a NACK.
    fn send_control(
        &mut self,
        dst: SocketAddr,
        kind: ControlKind,
        range: (u64, u64),
        entries: &[ControlEntry],
    ) {
        let header = Header {
            min_origin_sequence: range.0,
            max_origin_sequence: range.1,
            ..self.header(0)
        };
        let mut sent = 0;
        loop {
            let Some(slot) = self.send_free.pop() else {
                self.note_send_cut(dst, FrameKind::Control, 0);
                return;
            };
            let n = wire::encode_control(
                &mut self.send_pool[slot as usize],
                header,
                kind,
                &entries[sent..],
                self.config.auth_key.as_ref(),
                self.config.wire_limits,
            );
            self.send_pending.push_back((dst, slot));
            sent += n;
            if sent >= entries.len() || n == 0 {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests;
