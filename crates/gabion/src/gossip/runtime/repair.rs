//! The tree protocol's control plane: IHAVE, GRAFT and PRUNE, and the upkeep
//! that keeps every node interior in one tree and attached to the others.
//!
//! Plumtree's rules, as Thicket constrains them:
//!
//! - A duplicate eager delivery makes the receiver move the sender to lazy for
//!   that root and PRUNE it; the first copy of a change makes the sender eager.
//!   A leaf that is pushed to by a node that is not its parent detaches from
//!   it.
//! - Each heartbeat a node sends an IHAVE to a few random peers naming each
//!   change it made or was pushed since the last one. A change a receiver does
//!   not hold, and that is not delivered within the missing timeout, makes it
//!   GRAFT the announcer: ask for the cell, and repair the change's tree
//!   through the announcer, as its backbone neighbour or its leaf child.
//! - A GRAFT may only make a node interior in the tree it already is interior
//!   in. In another tree the grafting node attaches as a leaf, to an interior
//!   node of that tree, and the grafted node honours it only if it is interior
//!   there and has room: past its leaf cap it refuses with a PRUNE and the leaf
//!   picks another parent.
//!
//! A node that joined with no state asks one peer after another for its next
//! slice of cells, which is outside the papers.

use crate::crdt::CellHandle;
use crate::crdt::{Count, NodeId};
use crate::defaults::{
    THICKET_CATCH_UP_ATTEMPTS, THICKET_GRAFT_ATTEMPTS, THICKET_LAZY_TARGETS,
    THICKET_MISSING_CAPACITY, THICKET_MISSING_TIMEOUT_MS, THICKET_REPAIR_CELLS, THICKET_TREES,
};
use crate::wire::{ControlEntry, ControlKind, FLAG_MORE, NO_CELLS, PULL_ONLY_TREE, decode_control};

use super::super::clock::Clock;
use super::super::store::AggregateStore;
use super::super::transport::GossipTransport;
use super::GossipRuntime;
use super::forward::Lane;
use super::thicket::{Arrival, CatchUp, Missing, NONE, Root, tree_of};

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

    /// A peer discovery just added takes its place in the structure: see
    /// `Thicket::place_new`. A parent it replaces is detached from, so the
    /// old parent stops pushing to this node.
    pub(super) fn peer_added(&mut self, peer: u16) {
        if let Some(r) = self.thicket.place_new(peer, &mut self.rng) {
            if r.old != NONE {
                self.send_control(r.old, ControlKind::Prune, r.tree, &[]);
            }
            self.attach(r.tree, r.new);
        }
        self.maintain_structure();
    }

    /// Keep the backbone view at its target size and a parent in every tree
    /// this node is a leaf of. Cheap when nothing changed.
    pub(super) fn maintain_structure(&mut self) {
        self.thicket.fill_view(&mut self.rng);
        let class = self.thicket.class;
        for tree in (0..THICKET_TREES as u8).filter(|&t| t != class) {
            if self.thicket.parents[usize::from(tree)] == NONE
                && let Some(parent) = self.thicket.choose_parent(tree, &mut self.rng, NONE)
            {
                self.attach(tree, parent);
            }
        }
    }

    /// Become a leaf child of `parent` in `tree`. This is a GRAFT, as the
    /// papers' join of a tree is, and is repeated every heartbeat; the parent
    /// refuses it with a PRUNE when it has taken all the leaves it will.
    fn attach(&mut self, tree: u8, parent: u16) {
        self.thicket.parents[usize::from(tree)] = parent;
        self.send_control(parent, ControlKind::Graft, tree, &[]);
    }

    /// One heartbeat of upkeep: edge probes, parent links, the repair digest
    /// and the lazy announcement.
    pub(super) fn heartbeat(&mut self, now: u64) {
        self.edge_probes();
        self.catch_up_retry(now);
        let class = self.thicket.class;
        for tree in (0..THICKET_TREES as u8).filter(|&t| t != class) {
            let parent = self.thicket.parents[usize::from(tree)];
            if parent != NONE {
                self.attach(tree, parent);
            }
        }
        self.digest_due(now);
        self.announce();
    }

    /// Send `peer` the next slice of live cells, whatever their state, for a
    /// node catching up.
    fn send_slice(&mut self, peer: u16) {
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

    /// The change a cell holds, as IHAVE and GRAFT name it.
    fn change_of(&self, handle: CellHandle) -> Option<ControlEntry> {
        let row = self.store.get(handle)?;
        let origin = self.store.node_dictionary().descriptor(row.key.origin)?;
        let rule = self.store.rule_dictionary().descriptor(row.key.rule)?;
        Some(ControlEntry {
            node_id: origin.node_id,
            incarnation: origin.incarnation,
            value: row.origin_sequence,
            rule_fingerprint: rule.fingerprint,
            key_hash: row.key.key_hash,
            bucket: row.key.bucket,
        })
    }

    /// Lazy push: name each change made here or pushed here since the last
    /// heartbeat to a few random peers. A cell that changed again since it
    /// was queued is named once, at its newest version.
    fn announce(&mut self) {
        let n = self.thicket.lazy.len().min(self.thicket.entries.capacity());
        let mut digest = std::mem::take(&mut self.thicket.entries);
        digest.clear();
        for &handle in &self.thicket.lazy[..n] {
            digest.extend(self.change_of(handle));
        }
        self.thicket.dequeue_lazy(n);
        // Grouped by origin and rule on the wire: sorted, they share groups.
        digest.sort_unstable_by_key(|e| (e.node_id.0, e.incarnation, e.rule_fingerprint, e.value));
        if !digest.is_empty() {
            let mut chosen = [NONE; THICKET_LAZY_TARGETS];
            for k in 0..THICKET_LAZY_TARGETS {
                let pick = self
                    .thicket
                    .random_alive(&mut self.rng, |i| chosen.contains(&i));
                let Some(pick) = pick else { break };
                chosen[k] = pick;
                self.send_control(pick, ControlKind::Ihave, 0, &digest);
            }
        }
        self.thicket.entries = digest;
    }

    // -- Catching up -------------------------------------------------------

    /// When discovery adds a peer to a node with no state, the node asks a
    /// random peer for its next slice of cells.
    /// Each slice that brings something new is followed by a request for the
    /// next, so a node that joins is brought up to date at network speed
    /// without the cluster sending it everything at once.
    pub(super) fn begin_catch_up(&mut self) {
        if self.thicket.catch_up.is_none() && self.store.is_empty() {
            let now = self.clock.now_millis();
            self.request_slice_from_anyone(now, 1);
        }
    }

    fn request_slice_from_anyone(&mut self, now: u64, tries: u8) {
        self.thicket.catch_up = None;
        if let Some(peer) = self.thicket.random_alive(&mut self.rng, |_| false) {
            self.request_slice(peer, now, tries);
        }
    }

    fn request_slice(&mut self, peer: u16, now: u64, tries: u8) {
        self.thicket.catch_up = Some(CatchUp {
            peer,
            asked_ms: now,
            tries,
            fresh: false,
        });
        self.send_control(peer, ControlKind::Pull, 0, &[]);
    }

    /// A packet of cells outside the trees arrived from `peer`. At the end of
    /// a slice, ask for the next if the slice taught anything.
    pub(super) fn catch_up_received(&mut self, peer: u16, flags: u16, news: bool, now: u64) {
        let Some(c) = self.thicket.catch_up.as_mut() else {
            return;
        };
        if c.peer != peer {
            return;
        }
        c.fresh |= news;
        if flags & FLAG_MORE != 0 {
            return;
        }
        if c.fresh {
            // The peer that answered knows this node: stay with it.
            self.request_slice(peer, now, 1);
        } else {
            self.thicket.catch_up = None;
        }
    }

    /// A request that nothing answered for a heartbeat goes to another peer.
    fn catch_up_retry(&mut self, now: u64) {
        let Some(c) = self.thicket.catch_up else {
            return;
        };
        if now.saturating_sub(c.asked_ms) < THICKET_MISSING_TIMEOUT_MS {
            return;
        }
        if c.tries < THICKET_CATCH_UP_ATTEMPTS {
            self.request_slice_from_anyone(now, c.tries + 1);
        } else {
            self.thicket.catch_up = None;
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
                    let p = &mut self.thicket.peers[usize::from(peer)];
                    p.node_id = Some(NodeId(frame.header.sender_node_id));
                    p.heard_ms = now;
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
                        ControlKind::Pull => self.send_slice(peer),
                    }
                }
            }
        }
        self.thicket.inbound = entries;
    }

    /// Whether this node needs nothing for the announced change `e`: it holds
    /// that version or a newer one, made it, or the cell is out of its window.
    fn has_change(&self, e: &ControlEntry, now: u64) -> bool {
        let me = self.store.local_identity();
        (e.node_id == me.node_id && e.incarnation == me.incarnation)
            || self
                .store
                .find_cell(
                    e.rule_fingerprint,
                    e.key_hash,
                    e.bucket,
                    e.node_id,
                    e.incarnation,
                )
                .is_some_and(|(_, sequence)| sequence >= e.value)
            || self.store.is_expired_at(e.rule_fingerprint, e.bucket, now)
    }

    /// A lazy announcement: start the missing timer for each change we do
    /// not hold, as Plumtree does for each IHAVE of a message not received.
    pub(super) fn on_ihave(&mut self, peer: u16, entries: &[ControlEntry], now: u64) {
        for e in entries {
            if self.has_change(e, now) {
                continue;
            }
            let known = self.thicket.missing.iter_mut().find(|m| {
                let c = &m.change;
                (
                    c.node_id,
                    c.incarnation,
                    c.rule_fingerprint,
                    c.key_hash,
                    c.bucket,
                ) == (
                    e.node_id,
                    e.incarnation,
                    e.rule_fingerprint,
                    e.key_hash,
                    e.bucket,
                )
            });
            if let Some(m) = known {
                m.change.value = m.change.value.max(e.value);
                // Thicket repairs a tree through a node interior in it: keep
                // an announcer that is, so the GRAFT can re-attach there.
                let tree = tree_of(e.node_id, e.value);
                let interior =
                    |i: u16| i != NONE && self.thicket.peers[usize::from(i)].class == tree;
                if !interior(m.announcer) && interior(peer) {
                    m.announcer = peer;
                }
            } else if self.thicket.missing.len() < THICKET_MISSING_CAPACITY {
                self.thicket.missing.push(Missing {
                    change: *e,
                    announcer: peer,
                    deadline_ms: now + THICKET_MISSING_TIMEOUT_MS,
                    tries: 0,
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
            } else if !self.thicket.adopt_leaf(peer) {
                self.send_control(peer, ControlKind::Prune, class, &[]);
            }
        }
        self.answer_pull(peer, entries);
    }

    /// Send `peer` the current cell of each change in `asked` that asks for
    /// cells.
    fn answer_pull(&mut self, peer: u16, asked: &[ControlEntry]) {
        let mut frame = std::mem::take(&mut self.frame_handles);
        frame.clear();
        let wanted = asked.iter().filter(|e| e.value != NO_CELLS);
        for e in wanted.take(self.config.max_cells_per_tick) {
            let cell = self.store.find_cell(
                e.rule_fingerprint,
                e.key_hash,
                e.bucket,
                e.node_id,
                e.incarnation,
            );
            frame.extend(cell.map(|(handle, _)| handle));
        }
        if !frame.is_empty() {
            self.send_cells(peer, &frame, Lane::Pull);
        }
        self.frame_handles = frame;
    }

    /// `peer` no longer wants pushes: for the roots listed, or, with none, as
    /// our leaf child.
    fn on_prune(&mut self, peer: u16, tree: u8, entries: &[ControlEntry]) {
        if tree != self.thicket.class {
            // A parent that has all the leaves it takes refuses the attach:
            // pick another one.
            let refused = self.thicket.parents.get(usize::from(tree)) == Some(&peer);
            if refused && let Some(parent) = self.thicket.choose_parent(tree, &mut self.rng, peer) {
                self.attach(tree, parent);
            }
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
    pub(super) fn eager_feedback(&mut self, peer: u16) {
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
            let root = Root {
                slot,
                node_id: origin,
                incarnation,
                local: slot == self.store.local_node_slot(),
            };
            let slot = usize::from(slot);
            let tree = tree_of(origin, self.obs_buf.origin_sequences[i]);
            if th.fresh[slot] != 0 {
                // Plumtree: the first copy of a message makes its sender an
                // eager link for that root.
                if tree == class {
                    th.set_eager(peer, &root);
                }
                continue;
            }
            if tree == class {
                if th.seen[slot] != u32::MAX {
                    th.seen[slot] = u32::MAX;
                    th.set_lazy(peer, &root);
                    prune.push(ControlEntry {
                        node_id: origin,
                        incarnation,
                        ..ControlEntry::default()
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

        if !prune.is_empty() {
            self.send_control(peer, ControlKind::Prune, class, &prune);
        }
        for tree in (0..THICKET_TREES as u8).filter(|t| detach & (1 << t) != 0) {
            self.send_control(peer, ControlKind::Prune, tree, &[]);
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

    /// GRAFT, per announcer, the changes whose missing timer ran out and that
    /// are still undelivered. A GRAFT that went unanswered is sent again, up
    /// to `THICKET_GRAFT_ATTEMPTS` times.
    fn graft_overdue(&mut self, now: u64) {
        while let Some(first) = self
            .thicket
            .missing
            .iter()
            .position(|m| m.deadline_ms <= now)
        {
            let from = self.thicket.missing[first].announcer;
            let announcer = match from {
                NONE => self.thicket.random_alive(&mut self.rng, |_| false),
                a => Some(a),
            };
            let mut asked = std::mem::take(&mut self.thicket.entries);
            asked.clear();
            let mut i = 0;
            while i < self.thicket.missing.len() {
                let m = self.thicket.missing[i];
                if m.deadline_ms > now || m.announcer != from {
                    i += 1;
                } else if self.has_change(&m.change, now) {
                    self.thicket.missing.swap_remove(i);
                } else if m.tries + 1 >= THICKET_GRAFT_ATTEMPTS {
                    asked.push(m.change);
                    self.thicket.missing.swap_remove(i);
                } else {
                    asked.push(m.change);
                    let again = &mut self.thicket.missing[i];
                    again.tries += 1;
                    again.deadline_ms = now + THICKET_MISSING_TIMEOUT_MS;
                    i += 1;
                }
            }
            if let Some(announcer) = announcer
                && !asked.is_empty()
            {
                self.graft_for(announcer, &asked, now);
            }
            self.thicket.entries = asked;
        }
    }

    /// Ask `announcer` for the cells of the changes in `asked`, and repair the
    /// trees they travel in through it, as Thicket does:
    ///
    /// - When some of them travel in a tree this node is a leaf of, its parent
    ///   there has not been heard from for a missing timeout, and the announcer
    ///   is interior in that tree, the announcer becomes this node's parent
    ///   there: the GRAFT that asks for the cells also attaches to it, the old
    ///   parent is detached, and this node's own recent changes of that tree,
    ///   which the old parent may never have passed on, go to the new one. A
    ///   GRAFT never makes a node interior in a second tree, because the
    ///   announcer already is interior in this one. A parent that is heard from
    ///   stays: a change lost on a live tree is pulled, and moving the leaf
    ///   would move where its own changes enter the tree, whose eager links
    ///   every root pruned around the old entry.
    /// - When some travel in this node's own tree, a class-mate is GRAFTed for
    ///   a backbone link: the announcer when it is one.
    pub(super) fn graft_for(&mut self, announcer: u16, asked: &[ControlEntry], now: u64) {
        let class = self.thicket.class;
        let tree = self.thicket.peers[usize::from(announcer)].class;
        let old = self.thicket.parents[usize::from(tree)];
        let since = match old {
            NONE => 0,
            old => self.thicket.peers[usize::from(old)].heard_ms,
        };
        let reparent = tree != class
            && now.saturating_sub(since) >= THICKET_MISSING_TIMEOUT_MS
            && asked.iter().any(|e| tree_of(e.node_id, e.value) == tree);
        if reparent {
            if old != NONE {
                self.send_control(old, ControlKind::Prune, tree, &[]);
            }
            self.thicket.parents[usize::from(tree)] = announcer;
            self.send_control(announcer, ControlKind::Graft, tree, asked);
            self.resend_own(tree, announcer, since);
        } else {
            self.send_control(announcer, ControlKind::Graft, PULL_ONLY_TREE, asked);
        }
        let mut roots = std::mem::take(&mut self.thicket.inbound);
        roots.clear();
        roots.extend(
            asked
                .iter()
                .filter(|e| tree_of(e.node_id, e.value) == class)
                .map(|e| ControlEntry {
                    value: NO_CELLS,
                    ..*e
                }),
        );
        if !roots.is_empty() {
            let target = if self.thicket.peers[usize::from(announcer)].class == class {
                Some(announcer)
            } else {
                self.thicket
                    .random_of_class(class, &mut self.rng, |_, _| false)
            };
            if let Some(target) = target {
                self.send_control(target, ControlKind::Graft, class, &roots);
            }
        }
        self.thicket.inbound = roots;
    }
}
