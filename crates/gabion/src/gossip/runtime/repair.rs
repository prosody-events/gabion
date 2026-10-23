//! The tree protocol's control plane: IHAVE, GRAFT and PRUNE, and the upkeep
//! that keeps every node interior in one tree and attached to the others.
//!
//! Plumtree's rules, as Thicket constrains them:
//!
//! - A duplicate eager delivery makes the receiver PRUNE the sender for that
//!   root. A leaf that is pushed to by a node that is not its parent detaches
//!   from it.
//! - Each heartbeat a node sends an IHAVE digest to a few random peers. A
//!   digest entry for a change not delivered within the missing timeout makes
//!   the receiver GRAFT the announcer: ask for the cells, and ask to become its
//!   tree neighbour.
//! - A GRAFT may only make a node interior in the tree it already is interior
//!   in. In another tree the grafting node attaches as a leaf, to an interior
//!   node of that tree, and the grafted node honours it only if it is interior
//!   there.

use crate::crdt::{Count, NodeId};
use crate::defaults::{
    THICKET_LAZY_TARGETS, THICKET_MISSING_TIMEOUT_MS, THICKET_NACK_RETRY_MS, THICKET_REPAIR_CELLS,
    THICKET_TREES,
};
use crate::wire::{ControlEntry, ControlKind, PULL_ONLY_TREE, decode_control};

use super::super::clock::Clock;
use super::super::store::AggregateStore;
use super::super::transport::GossipTransport;
use super::GossipRuntime;
use super::forward::Lane;
use super::thicket::{Arrival, Missing, NONE, Root, tree_of};

/// Value of a GRAFT entry that asks for no cells, only a place in the tree.
const NO_CELLS: u64 = u64::MAX;

impl<C, S, T, K> GossipRuntime<C, S, T, K>
where
    C: Count,
    S: AggregateStore<C>,
    T: GossipTransport,
    K: Clock,
{
    pub(super) fn note_deadline(&mut self, at_ms: u64) {
        let next = &mut self.thicket.next_deadline;
        *next = Some(next.map_or(at_ms, |d| d.min(at_ms)));
    }

    fn root_of(&self, node_id: NodeId, incarnation: u32) -> Option<Root> {
        let slot = self.store.find_node(node_id, incarnation)?;
        Some(Root {
            slot,
            node_id,
            incarnation,
            local: slot == self.store.local_node_slot(),
        })
    }

    // -- Upkeep ----------------------------------------------------------

    /// Keep the backbone view at its target size and a parent in every tree
    /// this node is a leaf of. Cheap when nothing changed.
    pub(super) fn maintain_structure(&mut self) {
        let class = self.thicket.class;
        while self.thicket.view_count() < self.thicket.view_target() {
            let pick = self
                .thicket
                .random_of_class(class, &mut self.rng, |i, _| self.thicket.in_view(i));
            match pick {
                Some(mate) if self.thicket.view_add(mate) => {}
                _ => break,
            }
        }
        for tree in (0..THICKET_TREES as u8).filter(|&t| t != class) {
            let parent = self.thicket.parents[usize::from(tree)];
            if parent != NONE {
                continue;
            }
            let pick = self
                .thicket
                .random_of_class(tree, &mut self.rng, |_, _| false);
            if let Some(parent) = pick {
                self.attach(tree, parent);
            }
        }
    }

    /// Become a leaf child of `parent` in `tree`.
    fn attach(&mut self, tree: u8, parent: u16) {
        self.thicket.parents[usize::from(tree)] = parent;
        self.send_control(parent, ControlKind::Graft, tree, &[]);
    }

    /// One heartbeat of upkeep: edge probes and the lazy digest.
    pub(super) fn heartbeat(&mut self) {
        self.thicket.heartbeats += 1;
        self.edge_probes();
        let class = self.thicket.class;
        for tree in (0..THICKET_TREES as u8).filter(|&t| t != class) {
            let parent = self.thicket.parents[usize::from(tree)];
            if parent != NONE {
                self.attach(tree, parent);
            }
        }
        self.send_repair_slice();
        let full = self.thicket.full_digest_due();
        self.fill_digest(!full, true);
        if self.thicket.entries.is_empty() {
            return;
        }
        let digest = std::mem::take(&mut self.thicket.entries);
        let mut chosen = [NONE; THICKET_LAZY_TARGETS];
        for k in 0..THICKET_LAZY_TARGETS {
            let pick = self
                .thicket
                .random_alive(&mut self.rng, |i| chosen.contains(&i));
            let Some(pick) = pick else { break };
            chosen[k] = pick;
            self.send_control(pick, ControlKind::Ihave, 0, &digest);
        }
        self.thicket.entries = digest;
    }

    /// Send one random peer the next slice of live cells, whatever their
    /// state: the anti-entropy backstop.
    fn send_repair_slice(&mut self) {
        let Some(peer) = self.thicket.random_alive(&mut self.rng, |_| false) else {
            return;
        };
        let mut frame = std::mem::take(&mut self.frame_handles);
        frame.clear();
        let cells = THICKET_REPAIR_CELLS.min(self.config.max_cells_per_tick);
        self.store.visit_repair_slice(cells, |handle| {
            frame.push(handle);
            true
        });
        if !frame.is_empty() {
            self.send_cells(peer, &frame, Lane::Pull);
        }
        self.frame_handles = frame;
    }

    /// Tell a peer we just learned of everything we hold, so it can ask for
    /// what it lacks. A node that starts with no state hears the cluster's
    /// within the discovery lag instead of waiting for random digests.
    pub(super) fn send_digest_to(&mut self, peer: u16) {
        self.fill_digest(false, false);
        let digest = std::mem::take(&mut self.thicket.entries);
        if !digest.is_empty() {
            self.send_control(peer, ControlKind::Ihave, 0, &digest);
        }
        self.thicket.entries = digest;
    }

    /// Fill `thicket.entries` with the highest sequence held per origin.
    /// `advanced_only` leaves out origins not advanced since the last digest
    /// that was `mark`ed.
    fn fill_digest(&mut self, advanced_only: bool, mark: bool) {
        let th = &mut self.thicket;
        th.entries.clear();
        for slot in 0..self.store.node_dictionary().capacity() {
            let Some(origin) = self.store.node_dictionary().descriptor(slot) else {
                continue;
            };
            // An origin with no live cell has nothing to announce; the local
            // slot is pinned, so it holds one reference of its own.
            let pinned = u32::from(slot == self.store.local_node_slot());
            let seq = self.store.highest_origin_sequence(slot);
            if seq == 0 || self.store.node_dictionary().refcount(slot) <= pinned {
                continue;
            }
            if !advanced_only || seq > th.announced[usize::from(slot)] {
                th.entries.push(ControlEntry {
                    node_id: origin.node_id,
                    incarnation: origin.incarnation,
                    value: seq,
                });
            }
            if mark {
                th.announced[usize::from(slot)] = seq;
            }
        }
    }

    // -- Inbound control ---------------------------------------------------

    pub(super) fn handle_control(&mut self, n: usize, src: std::net::SocketAddr) {
        let mut entries = std::mem::take(&mut self.thicket.inbound);
        entries.clear();
        let decoded = decode_control(
            &self.recv_buf[..n],
            self.config.auth_key.as_ref(),
            self.config.wire_limits,
            |e| entries.push(e),
        );
        match decoded {
            Err(err) => self.note_decode_reject(src, &err),
            Ok(frame) => {
                let now = self.clock.now_millis();
                if let Some(peer) = self.thicket.index_of(src) {
                    self.thicket.peers[usize::from(peer)].node_id =
                        Some(NodeId(frame.header.sender_node_id));
                    match frame.kind {
                        ControlKind::Ihave => self.on_ihave(peer, &entries, now),
                        ControlKind::Graft => self.on_graft(peer, frame.tree, &entries),
                        ControlKind::Prune => self.on_prune(peer, frame.tree, &entries),
                        ControlKind::Nack => self.handle_nack(peer, &entries),
                        ControlKind::Probe => {
                            if let Some(e) = entries.first() {
                                self.handle_probe(peer, &frame.header, e.value, now);
                            }
                        }
                    }
                }
            }
        }
        self.thicket.inbound = entries;
    }

    /// A lazy digest: for each origin we lack changes of, start the missing
    /// timer for the announcer.
    fn on_ihave(&mut self, peer: u16, entries: &[ControlEntry], now: u64) {
        let me = self.store.local_identity();
        for e in entries {
            if e.node_id == me.node_id && e.incarnation == me.incarnation {
                continue;
            }
            let have = self
                .store
                .find_node(e.node_id, e.incarnation)
                .map_or(0, |slot| self.store.highest_origin_sequence(slot));
            if e.value <= have {
                continue;
            }
            let known = self
                .thicket
                .missing
                .iter_mut()
                .find(|m| m.node_id == e.node_id && m.incarnation == e.incarnation);
            if let Some(m) = known {
                m.target = m.target.max(e.value);
            } else if self.thicket.missing.len() < self.thicket.announced.len() {
                self.thicket.missing.push(Missing {
                    node_id: e.node_id,
                    incarnation: e.incarnation,
                    target: e.value,
                    announcer: peer,
                    deadline_ms: now + THICKET_MISSING_TIMEOUT_MS,
                });
                self.note_deadline(now + THICKET_MISSING_TIMEOUT_MS);
            }
        }
    }

    /// `peer` asks for cells and for a place in tree `tree`.
    fn on_graft(&mut self, peer: u16, tree: u8, entries: &[ControlEntry]) {
        let class = self.thicket.class;
        // The tree rule: this node is interior only in its own tree, so only
        // a GRAFT for that tree changes who it pushes to. A class-mate joins
        // the backbone; anyone else attaches as a leaf.
        if tree == class {
            if self.thicket.peers[usize::from(peer)].class == class {
                if self.thicket.view_add(peer) {
                    for e in entries {
                        if let Some(root) = self.root_of(e.node_id, e.incarnation) {
                            self.thicket.set_eager(peer, &root);
                        }
                    }
                }
            } else {
                self.thicket.peers[usize::from(peer)].leaf_child = true;
            }
        }
        let wants_cells = entries.iter().any(|e| e.value != NO_CELLS);
        if wants_cells {
            self.answer_pull(peer, entries);
        }
    }

    /// Send `peer` the cells of the origins in `asked` above the sequence it
    /// holds, oldest first, so that what it receives is a prefix.
    fn answer_pull(&mut self, peer: u16, asked: &[ControlEntry]) {
        for e in asked.iter().filter(|e| e.value != NO_CELLS) {
            if let Some(slot) = self.store.find_node(e.node_id, e.incarnation) {
                self.thicket.want[usize::from(slot)] = e.value;
            }
        }
        let mut frame = std::mem::take(&mut self.frame_handles);
        frame.clear();
        let cap = self.config.max_cells_per_tick;
        self.store.visit_cells_after(&self.thicket.want, |h| {
            frame.push(h);
            frame.len() < cap
        });
        for e in asked {
            if let Some(slot) = self.store.find_node(e.node_id, e.incarnation) {
                self.thicket.want[usize::from(slot)] = u64::MAX;
            }
        }
        let store = &self.store;
        frame.sort_unstable_by_key(|&h| store.get(h).map_or(0, |row| row.origin_sequence));
        if !frame.is_empty() {
            self.send_cells(peer, &frame, Lane::Pull);
        }
        self.frame_handles = frame;
    }

    /// `peer` no longer wants pushes: for the roots listed, or, with none, as
    /// our leaf child.
    fn on_prune(&mut self, peer: u16, tree: u8, entries: &[ControlEntry]) {
        if tree != self.thicket.class {
            return;
        }
        if entries.is_empty() {
            self.thicket.peers[usize::from(peer)].leaf_child = false;
            return;
        }
        for e in entries {
            if let Some(root) = self.root_of(e.node_id, e.incarnation) {
                self.thicket.set_lazy(peer, &root);
            }
        }
    }

    // -- Duplicate feedback ------------------------------------------------

    /// After merging a new eager packet from `peer`: PRUNE the edge for each
    /// root all of whose rows were duplicates. The rows are `obs_buf`, the
    /// cells they changed `sink_buf`.
    ///
    /// A row is a duplicate when this node already held exactly that change,
    /// delivered by a tree edge or made here: the stored cell has the row's
    /// origin sequence, the merge did not change it, and it did not arrive
    /// by a pull or a repair slice, which say nothing about the trees. A row
    /// older than what is stored is not: it is the first
    /// copy of a change a newer one has superseded, which arrives late when
    /// the newer one travelled a faster tree. Counting it a duplicate would
    /// prune the tree edge that delivered it.
    pub(super) fn eager_feedback(&mut self, peer: u16, now: u64) {
        let class = self.thicket.class;
        let th = &mut self.thicket;
        th.touched.clear();
        let mut prune = std::mem::take(&mut th.entries);
        prune.clear();
        // Per root: rows that were not duplicates (`fresh`), and rows seen.
        for i in 0..self.obs_buf.len() {
            let origin = self.obs_buf.origin_node_ids[i];
            let incarnation = self.obs_buf.incarnations[i];
            let Some(slot) = self.store.find_node(origin, incarnation) else {
                continue;
            };
            let duplicate = self
                .store
                .find_cell(
                    self.obs_buf.rule_fingerprints[i],
                    self.obs_buf.key_hashes[i],
                    self.obs_buf.buckets[i],
                    origin,
                    incarnation,
                )
                .is_some_and(|(handle, sequence)| {
                    sequence == self.obs_buf.origin_sequences[i]
                        && !self.sink_buf.handles.contains(&handle)
                        && th.arrival[handle.index as usize] != Arrival::Other
                });
            let slot = usize::from(slot);
            if th.seen[slot] == 0 {
                th.touched.push(slot as u16);
            }
            th.seen[slot] += 1;
            if !duplicate {
                th.fresh[slot] += 1;
            }
        }

        let mut detach = 0u32;
        for i in 0..self.obs_buf.len() {
            let origin = self.obs_buf.origin_node_ids[i];
            let incarnation = self.obs_buf.incarnations[i];
            let Some(slot) = self.store.find_node(origin, incarnation) else {
                continue;
            };
            let slot = usize::from(slot);
            if th.fresh[slot] != 0 {
                continue;
            }
            let tree = tree_of(origin, self.obs_buf.origin_sequences[i]);
            if tree == class {
                if th.seen[slot] != u32::MAX {
                    th.seen[slot] = u32::MAX;
                    prune.push(ControlEntry {
                        node_id: origin,
                        incarnation,
                        value: 0,
                    });
                }
            } else if th.parents[usize::from(tree)] != peer {
                detach |= 1 << tree;
            }
        }
        for &slot in &th.touched {
            th.seen[usize::from(slot)] = 0;
            th.fresh[usize::from(slot)] = 0;
        }

        let due =
            now.saturating_sub(th.peers[usize::from(peer)].last_prune_ms) >= THICKET_NACK_RETRY_MS;
        if due && (!prune.is_empty() || detach != 0) {
            th.peers[usize::from(peer)].last_prune_ms = now;
            if !prune.is_empty() {
                self.send_control(peer, ControlKind::Prune, class, &prune);
            }
            for tree in (0..THICKET_TREES as u8).filter(|t| detach & (1 << t) != 0) {
                self.send_control(peer, ControlKind::Prune, tree, &[]);
            }
        }
        self.thicket.entries = prune;
    }

    // -- Timers ------------------------------------------------------------

    /// Act on whatever is due: announced changes still undelivered, and eager
    /// packets still missing.
    pub(super) fn check_timers(&mut self, now: u64) {
        if self.thicket.next_deadline.is_none_or(|d| d > now) {
            return;
        }
        self.graft_overdue(now);
        self.edge_retry(now);
        let missing = self.thicket.missing.iter().map(|m| m.deadline_ms);
        let gaps = self
            .thicket
            .peers
            .iter()
            .filter(|p| p.alive)
            .filter_map(|p| p.edge.next_retry());
        self.thicket.next_deadline = missing.chain(gaps).min();
    }

    /// GRAFT, per announcer, the origins whose missing timer ran out.
    fn graft_overdue(&mut self, now: u64) {
        while let Some(first) = self
            .thicket
            .missing
            .iter()
            .position(|m| m.deadline_ms <= now)
        {
            let announcer = match self.thicket.missing[first].announcer {
                NONE => self.thicket.random_alive(&mut self.rng, |_| false),
                a => Some(a),
            };
            let from = self.thicket.missing[first].announcer;
            let mut asked = std::mem::take(&mut self.thicket.entries);
            asked.clear();
            let mut trees = 0u32;
            let mut i = 0;
            while i < self.thicket.missing.len() {
                let m = self.thicket.missing[i];
                if m.deadline_ms > now || m.announcer != from {
                    i += 1;
                    continue;
                }
                self.thicket.missing.swap_remove(i);
                let have = self
                    .store
                    .find_node(m.node_id, m.incarnation)
                    .map_or(0, |slot| self.store.highest_origin_sequence(slot));
                if have < m.target {
                    asked.push(ControlEntry {
                        node_id: m.node_id,
                        incarnation: m.incarnation,
                        value: have,
                    });
                    trees |= trees_between(m.node_id, have, m.target);
                }
            }
            if let Some(announcer) = announcer
                && !asked.is_empty()
            {
                self.graft_for(announcer, trees, &asked);
            }
            self.thicket.entries = asked;
        }
    }

    /// Ask `announcer` for the cells in `asked`, and repair this node's own
    /// tree if the missing changes travel in it: GRAFT a backbone link.
    ///
    /// In any other tree this node is a leaf and does not change parent: a
    /// change of parent moves the point where this node's own changes enter
    /// that tree, and the tree for each root was pruned around the old one.
    /// The cells come from the announcer; the parent link is refreshed on
    /// every heartbeat.
    fn graft_for(&mut self, announcer: u16, trees: u32, asked: &[ControlEntry]) {
        self.send_control(announcer, ControlKind::Graft, PULL_ONLY_TREE, asked);
        let class = self.thicket.class;
        let announcer_class = self.thicket.peers[usize::from(announcer)].class;
        for tree in (0..THICKET_TREES as u8).filter(|t| trees & (1 << t) != 0) {
            if tree == class {
                let target = if announcer_class == tree {
                    Some(announcer)
                } else {
                    self.thicket
                        .random_of_class(tree, &mut self.rng, |_, _| false)
                };
                if let Some(target) = target {
                    let mut roots = std::mem::take(&mut self.thicket.inbound);
                    roots.clear();
                    roots.extend(asked.iter().map(|e| ControlEntry {
                        value: NO_CELLS,
                        ..*e
                    }));
                    self.send_control(target, ControlKind::Graft, tree, &roots);
                    self.thicket.inbound = roots;
                }
            }
        }
    }
}

/// The trees of changes `(have, target]` of `origin`.
fn trees_between(origin: NodeId, have: u64, target: u64) -> u32 {
    let span = target - have;
    if span >= THICKET_TREES as u64 {
        return (1 << THICKET_TREES) - 1;
    }
    (1..=span).fold(0, |mask, k| mask | 1 << tree_of(origin, have + k))
}
