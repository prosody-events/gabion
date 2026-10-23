//! Thicket tree state: which neighbours a change is pushed to.
//!
//! Ferreira, Leitão & Rodrigues, "Thicket: A Protocol for Building and
//! Maintaining Multiple Trees in a P2P Overlay" (SRDS 2010), over Plumtree
//! (Leitão, Pereira & Rodrigues, SRDS 2007).
//!
//! There are `T` trees. Every node is *interior* in exactly one of them, its
//! class `hash(address) mod T`, and a *leaf* in the other `T − 1`:
//!
//! - In its own tree a node keeps a backbone view of random class-mates and,
//!   per root (the origin of a change), an eager subset of that view. The
//!   subset starts as the whole view and shrinks by PRUNE on duplicate eager
//!   deliveries, which is Plumtree's rule, kept per root as
//!   `riak_core_broadcast` does. It also pushes to the leaves that attached to
//!   it.
//! - In every other tree it has one parent, an interior node of that tree,
//!   which it attached to. It never relays that tree's changes. It sends its
//!   own changes of that tree to the parent.
//!
//! A change is a cell version named `(origin, incarnation, origin sequence)`.
//! Its tree is a pure function of origin and sequence, so consecutive changes
//! of one origin rotate through the trees.

use std::net::SocketAddr;

use crate::crdt::{CellHandle, Incarnation, NodeId, NodeSlot};
use crate::defaults::{
    GOSSIP_COVERAGE_MARGIN, THICKET_LEAF_CHILDREN_MIN, THICKET_MISSING_CAPACITY, THICKET_TREES,
    THICKET_VIEW_CAPACITY,
};
use crate::wire::ControlEntry;

use super::SplitMix64;
use super::edge::{Edge, SentRing};

#[cfg(test)]
mod tests;

/// "No peer" in a `u16` peer index.
pub(super) const NONE: u16 = u16::MAX;
/// "Not in the backbone view" in a view position.
const NO_POS: u8 = u8::MAX;

const _: () = assert!(THICKET_VIEW_CAPACITY <= u32::BITS as usize);
const _: () = assert!(THICKET_TREES >= 1 && THICKET_TREES <= u8::MAX as usize);

/// One peer from discovery, with its place in the trees and its eager edge.
pub(super) struct TreePeer {
    pub addr: SocketAddr,
    /// Learned from the first packet this peer sends. Observability only.
    pub node_id: Option<NodeId>,
    pub alive: bool,
    /// The tree this peer is interior in.
    pub class: u8,
    /// Position in our backbone view, or `NO_POS`.
    view_pos: u8,
    /// Attached to us as its parent in our tree.
    pub leaf_child: bool,
    pub edge: Edge,
}

/// A parent chosen or replaced for a tree this node is a leaf of.
#[derive(Clone, Copy)]
pub(super) struct Reparent {
    pub tree: u8,
    /// The parent replaced, or `NONE`.
    pub old: u16,
    pub new: u16,
}

/// How the version of a cell this node holds reached it.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Arrival {
    /// Made here.
    Local,
    /// Pushed by a tree neighbour. Only a second push of the same version
    /// shows that two tree edges deliver it, which is what PRUNE answers.
    Eager,
    /// Answering a GRAFT, or in a repair slice: outside the trees.
    Other,
}

/// A change announced by IHAVE and not yet delivered: Plumtree's missing
/// message timer, one per announced change.
#[derive(Clone, Copy)]
pub(super) struct Missing {
    /// The announced version: origin, sequence (`value`) and the cell.
    pub change: ControlEntry,
    /// Who announced it first, and so who is asked.
    pub announcer: u16,
    pub deadline_ms: u64,
    /// GRAFTs sent for it so far.
    pub tries: u8,
}

/// A node that joined with no state asking one peer after another for its
/// next slice of cells, until a slice brings nothing new.
#[derive(Clone, Copy)]
pub(super) struct CatchUp {
    pub peer: u16,
    pub asked_ms: u64,
    /// Requests sent since the last slice arrived.
    pub tries: u8,
    /// Some packet of the slice being received taught this node something.
    pub fresh: bool,
}

/// The origin of a change, as routing needs it.
#[derive(Clone, Copy)]
pub(super) struct Root {
    pub slot: NodeSlot,
    pub node_id: NodeId,
    pub incarnation: Incarnation,
    /// Originated on this node.
    pub local: bool,
}

pub(super) struct Thicket {
    /// The tree this node is interior in.
    pub class: u8,
    pub peers: Vec<TreePeer>,
    /// Alive peers per class, this node excluded.
    pub class_alive: [u16; THICKET_TREES],
    pub alive: usize,
    /// Backbone view: peer indices by position, `NONE` for a hole.
    view: [u16; THICKET_VIEW_CAPACITY],
    /// Per origin slot: whose mask this is, and its eager view positions.
    root_key: Box<[(NodeId, Incarnation)]>,
    root_mask: Box<[u32]>,
    /// Our parent in each tree but our own, or `NONE`.
    pub parents: [u16; THICKET_TREES],
    /// Per cell slot: the peer whose delivery last changed it, and how.
    pub from_peer: Box<[u16]>,
    pub arrival: Box<[Arrival]>,
    /// Changes merged since the last relay round.
    pub relay_pending: bool,
    pub missing: Vec<Missing>,
    /// Earliest time `missing` or an edge gap needs attention.
    pub next_deadline: Option<u64>,
    pub catch_up: Option<CatchUp>,
    /// Cells changed here or delivered by a tree since the last heartbeat,
    /// which the next IHAVE announces, and per cell slot the generation
    /// queued, so a cell is queued once however often it changes.
    pub lazy: Vec<CellHandle>,
    lazy_queued: Box<[u32]>,
    pub sent: SentRing,
    // Scratch, sized at construction.
    pub entries: Vec<ControlEntry>,
    pub inbound: Vec<ControlEntry>,
    pub seen: Box<[u32]>,
    pub fresh: Box<[u32]>,
    pub touched: Vec<NodeSlot>,
    pub dests: Vec<u16>,
}

impl Thicket {
    pub(super) fn new(
        local: SocketAddr,
        bootstrap: &[SocketAddr],
        peer_capacity: usize,
        node_capacity: usize,
        cell_capacity: usize,
        control_entries: usize,
    ) -> Self {
        let peer_capacity = peer_capacity.max(bootstrap.len());
        let mut thicket = Self {
            class: class_of(&local),
            peers: Vec::with_capacity(peer_capacity),
            class_alive: [0; THICKET_TREES],
            alive: 0,
            view: [NONE; THICKET_VIEW_CAPACITY],
            root_key: vec![(NodeId(0), 0); node_capacity].into_boxed_slice(),
            root_mask: vec![0; node_capacity].into_boxed_slice(),
            parents: [NONE; THICKET_TREES],
            from_peer: vec![NONE; cell_capacity].into_boxed_slice(),
            arrival: vec![Arrival::Local; cell_capacity].into_boxed_slice(),
            relay_pending: false,
            missing: Vec::with_capacity(THICKET_MISSING_CAPACITY),
            next_deadline: None,
            catch_up: None,
            lazy: Vec::with_capacity(cell_capacity),
            lazy_queued: vec![0; cell_capacity].into_boxed_slice(),
            sent: SentRing::new(),
            entries: Vec::with_capacity(control_entries.max(node_capacity)),
            inbound: Vec::with_capacity(control_entries),
            seen: vec![0; node_capacity].into_boxed_slice(),
            fresh: vec![0; node_capacity].into_boxed_slice(),
            touched: Vec::with_capacity(node_capacity),
            dests: Vec::with_capacity(peer_capacity),
        };
        for &addr in bootstrap {
            thicket.add_peer(addr);
        }
        thicket
    }

    // -- Membership -------------------------------------------------------

    pub(super) fn index_of(&self, addr: SocketAddr) -> Option<u16> {
        self.peers
            .iter()
            .position(|p| p.alive && p.addr == addr)
            .map(|i| i as u16)
    }

    /// Register a peer from discovery, reusing the slot of a departed one so
    /// the table is bounded by peak membership. Returns its index, and
    /// whether it is new.
    pub(super) fn add_peer(&mut self, addr: SocketAddr) -> (u16, bool) {
        if let Some(i) = self.index_of(addr) {
            return (i, false);
        }
        let class = class_of(&addr);
        let peer = TreePeer {
            addr,
            node_id: None,
            alive: true,
            class,
            view_pos: NO_POS,
            leaf_child: false,
            edge: Edge::new(),
        };
        let idx = match self.peers.iter().position(|p| !p.alive) {
            Some(i) => {
                self.peers[i] = peer;
                self.sent.forget(i as u16);
                i
            }
            None => {
                self.peers.push(peer);
                self.peers.len() - 1
            }
        };
        self.alive += 1;
        self.class_alive[usize::from(class)] += 1;
        (idx as u16, true)
    }

    /// Forget a departed peer everywhere it held a place.
    pub(super) fn remove_peer(&mut self, addr: SocketAddr) {
        let Some(idx) = self.index_of(addr) else {
            return;
        };
        self.view_remove(idx);
        self.sent.forget(idx);
        for parent in &mut self.parents {
            if *parent == idx {
                *parent = NONE;
            }
        }
        let peer = &mut self.peers[usize::from(idx)];
        peer.alive = false;
        peer.leaf_child = false;
        self.alive -= 1;
        self.class_alive[usize::from(peer.class)] -= 1;
        for m in &mut self.missing {
            if m.announcer == idx {
                m.announcer = NONE;
            }
        }
    }

    pub(super) fn is_parent(&self, idx: u16) -> bool {
        self.parents.contains(&idx)
    }

    // -- Leaf children ------------------------------------------------------

    fn leaf_children(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| p.alive && p.leaf_child)
            .count()
    }

    /// Most leaf children this node takes: twice the even share of the known
    /// leaves over the known interior nodes of its tree, and never fewer than
    /// `THICKET_LEAF_CHILDREN_MIN`. Thicket bounds an interior node's fanout;
    /// the bound keeps one popular node from becoming the hub of a tree.
    fn leaf_cap(&self) -> usize {
        let mates = usize::from(self.class_alive[usize::from(self.class)]);
        let leaves = self.alive - mates;
        (2 * leaves.div_ceil(mates + 1)).max(THICKET_LEAF_CHILDREN_MIN)
    }

    /// Take `idx` as a leaf child. `false`, with nothing changed, when this
    /// node already has as many as it takes: the leaf must pick another parent.
    pub(super) fn adopt_leaf(&mut self, idx: u16) -> bool {
        debug_assert!(self.peers[usize::from(idx)].class != self.class);
        if self.peers[usize::from(idx)].leaf_child {
            return true;
        }
        if self.leaf_children() >= self.leaf_cap() {
            return false;
        }
        self.peers[usize::from(idx)].leaf_child = true;
        true
    }

    // -- Joining ------------------------------------------------------------

    /// A peer discovery just added is offered a place, so that each view
    /// member and each parent is uniform over the membership known so far
    /// (reservoir sampling), however discovery orders the peers.
    ///
    /// A class-mate replaces a random view member with probability `view /
    /// mates`, or joins it when every mate is in it; `fill_view` then tops the
    /// view up to its target, which grows with the mates, at random. A peer of
    /// another class replaces that tree's parent with probability `1 / peers
    /// of that class`. The replaced parent is returned for the caller to
    /// detach from and attach to the new one.
    pub(super) fn place_new(&mut self, idx: u16, rng: &mut SplitMix64) -> Option<Reparent> {
        let class = self.peers[usize::from(idx)].class;
        let draw = (rng.next_u64() as usize) % usize::from(self.class_alive[usize::from(class)]);
        if class == self.class {
            let members = self.view_count();
            let mates = usize::from(self.class_alive[usize::from(class)]);
            if members < self.view_target() && members + 1 >= mates {
                // Every mate known so far is in the view: so is this one.
                self.view_add(idx);
            } else if draw < members {
                let victim = self.view.iter().filter(|&&s| s != NONE).nth(draw).copied();
                if let Some(victim) = victim {
                    self.view_remove(victim);
                    self.view_add(idx);
                }
            }
            return None;
        }
        let old = self.parents[usize::from(class)];
        if old != NONE && draw != 0 {
            return None;
        }
        self.parents[usize::from(class)] = idx;
        Some(Reparent {
            tree: class,
            old,
            new: idx,
        })
    }

    /// Give `tree` a parent chosen uniformly among its known interior nodes
    /// except `avoid`. `None` when there is no such node.
    pub(super) fn choose_parent(
        &mut self,
        tree: u8,
        rng: &mut SplitMix64,
        avoid: u16,
    ) -> Option<u16> {
        let parent = self.random_of_class(tree, rng, |i, _| i == avoid)?;
        self.parents[usize::from(tree)] = parent;
        Some(parent)
    }

    /// Top the backbone view up to its target with random class-mates.
    pub(super) fn fill_view(&mut self, rng: &mut SplitMix64) {
        let class = self.class;
        while self.view_count() < self.view_target() {
            let pick = self.random_of_class(class, rng, |i, _| self.in_view(i));
            match pick {
                Some(mate) if self.view_add(mate) => {}
                _ => break,
            }
        }
    }

    // -- Backbone view ----------------------------------------------------

    pub(super) fn view_count(&self) -> usize {
        self.view.iter().filter(|&&s| s != NONE).count()
    }

    /// `⌈ln m + c⌉` of the `m` class-mates, the coverage law the rest of
    /// gabion sizes fanout by, capped at what exists.
    pub(super) fn view_target(&self) -> usize {
        let mates = usize::from(self.class_alive[usize::from(self.class)]);
        let law = ((mates.max(1) as f64).ln() + GOSSIP_COVERAGE_MARGIN).ceil() as usize;
        law.min(mates).min(THICKET_VIEW_CAPACITY)
    }

    fn all_view_bits(&self) -> u32 {
        self.view
            .iter()
            .enumerate()
            .filter(|&(_, &s)| s != NONE)
            .fold(0, |bits, (pos, _)| bits | (1 << pos))
    }

    /// Put a class-mate in the view as an eager link for every root, as
    /// Plumtree does a new neighbour. `false` when the view is full.
    pub(super) fn view_add(&mut self, idx: u16) -> bool {
        let peer = &self.peers[usize::from(idx)];
        debug_assert!(peer.alive && peer.class == self.class);
        if peer.view_pos != NO_POS {
            return true;
        }
        let Some(pos) = self.view.iter().position(|&s| s == NONE) else {
            return false;
        };
        self.view[pos] = idx;
        self.peers[usize::from(idx)].view_pos = pos as u8;
        for mask in self.root_mask.iter_mut() {
            *mask |= 1 << pos;
        }
        true
    }

    pub(super) fn view_remove(&mut self, idx: u16) {
        let pos = self.peers[usize::from(idx)].view_pos;
        if pos == NO_POS {
            return;
        }
        self.view[usize::from(pos)] = NONE;
        self.peers[usize::from(idx)].view_pos = NO_POS;
        for mask in self.root_mask.iter_mut() {
            *mask &= !(1 << pos);
        }
    }

    pub(super) fn in_view(&self, idx: u16) -> bool {
        self.peers[usize::from(idx)].view_pos != NO_POS
    }

    /// `root`'s eager mask over view positions. A root seen for the first
    /// time, or a slot reused by another origin, starts eager to the whole
    /// view.
    fn mask_mut(&mut self, root: &Root) -> &mut u32 {
        let slot = usize::from(root.slot);
        if self.root_key[slot] != (root.node_id, root.incarnation) {
            self.root_key[slot] = (root.node_id, root.incarnation);
            self.root_mask[slot] = self.all_view_bits();
        }
        &mut self.root_mask[slot]
    }

    /// Make `idx` eager for `root`. A peer outside the view cannot be.
    pub(super) fn set_eager(&mut self, idx: u16, root: &Root) {
        let pos = self.peers[usize::from(idx)].view_pos;
        if pos != NO_POS {
            *self.mask_mut(root) |= 1 << pos;
        }
    }

    /// Move `idx` to lazy for `root`.
    pub(super) fn set_lazy(&mut self, idx: u16, root: &Root) {
        let pos = self.peers[usize::from(idx)].view_pos;
        if pos != NO_POS {
            *self.mask_mut(root) &= !(1 << pos);
        }
    }

    // -- Routing ----------------------------------------------------------

    /// Whether `tree` has no interior node at all among the alive peers and
    /// this node, so its changes can only go straight from the origin.
    pub(super) fn tree_is_empty(&self, tree: u8) -> bool {
        tree != self.class && self.class_alive[usize::from(tree)] == 0
    }

    /// Whether a change of `root` in `tree` goes to alive peer `dest`.
    ///
    /// Interior in `tree`: to its eager backbone neighbours for the root and
    /// to its leaf children. Leaf in `tree`: never relays; its own changes
    /// go to its parent, or to everyone when the tree has no interior node.
    pub(super) fn pushes_to(&mut self, dest: u16, root: &Root, tree: u8) -> bool {
        if tree == self.class {
            let peer = &self.peers[usize::from(dest)];
            let (leaf, pos) = (peer.leaf_child, peer.view_pos);
            leaf || (pos != NO_POS && *self.mask_mut(root) & (1 << pos) != 0)
        } else {
            root.local && (self.parents[usize::from(tree)] == dest || self.tree_is_empty(tree))
        }
    }

    /// Alive peers a change could go to: backbone members, leaf children,
    /// parents, and everyone when some tree has no interior node.
    pub(super) fn collect_candidates(&mut self) {
        let any_empty = (0..THICKET_TREES as u8).any(|t| self.tree_is_empty(t));
        let mut dests = std::mem::take(&mut self.dests);
        dests.clear();
        for (i, p) in self.peers.iter().enumerate() {
            let i = i as u16;
            if p.alive && (any_empty || p.view_pos != NO_POS || p.leaf_child || self.is_parent(i)) {
                dests.push(i);
            }
        }
        self.dests = dests;
    }

    // -- Backbone and parent upkeep ---------------------------------------

    /// The alive peer of `class`, chosen at random, not already `skip`.
    pub(super) fn random_of_class(
        &self,
        class: u8,
        rng: &mut SplitMix64,
        mut skip: impl FnMut(u16, &TreePeer) -> bool,
    ) -> Option<u16> {
        let candidates = self
            .peers
            .iter()
            .enumerate()
            .filter(|&(i, p)| p.alive && p.class == class && !skip(i as u16, p))
            .count();
        if candidates == 0 {
            return None;
        }
        let pick = (rng.next_u64() as usize) % candidates;
        self.peers
            .iter()
            .enumerate()
            .filter(|&(i, p)| p.alive && p.class == class && !skip(i as u16, p))
            .nth(pick)
            .map(|(i, _)| i as u16)
    }

    pub(super) fn random_alive(
        &self,
        rng: &mut SplitMix64,
        mut skip: impl FnMut(u16) -> bool,
    ) -> Option<u16> {
        let candidates = (0..self.peers.len() as u16)
            .filter(|&i| self.peers[usize::from(i)].alive && !skip(i))
            .count();
        if candidates == 0 {
            return None;
        }
        let pick = (rng.next_u64() as usize) % candidates;
        (0..self.peers.len() as u16)
            .filter(|&i| self.peers[usize::from(i)].alive && !skip(i))
            .nth(pick)
    }

    /// Queue `handle` for the next IHAVE, unless this version of its slot
    /// already is. A full queue drops it: the repair slice covers it.
    pub(super) fn queue_lazy(&mut self, handle: CellHandle) {
        let queued = &mut self.lazy_queued[handle.index as usize];
        if *queued != handle.generation && self.lazy.len() < self.lazy.capacity() {
            *queued = handle.generation;
            self.lazy.push(handle);
        }
    }

    /// The first `n` queued cells leave the queue.
    pub(super) fn dequeue_lazy(&mut self, n: usize) {
        for handle in self.lazy.drain(..n) {
            let queued = &mut self.lazy_queued[handle.index as usize];
            if *queued == handle.generation {
                *queued = 0;
            }
        }
    }
}

fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The tree a node is interior in: a hash of its address, so every node
/// computes every peer's class from discovery alone.
pub(super) fn class_of(addr: &SocketAddr) -> u8 {
    let ip = match addr.ip() {
        std::net::IpAddr::V4(ip) => u64::from(u32::from(ip)),
        std::net::IpAddr::V6(ip) => {
            let bits = u128::from(ip);
            (bits as u64) ^ ((bits >> 64) as u64)
        }
    };
    (mix(ip ^ (u64::from(addr.port()) << 40)) % THICKET_TREES as u64) as u8
}

/// The tree a change travels in: deterministic from origin and sequence, and
/// consecutive sequences of one origin take consecutive trees.
pub(super) fn tree_of(origin: NodeId, origin_sequence: u64) -> u8 {
    let id = (origin.0 as u64) ^ ((origin.0 >> 64) as u64);
    (mix(id).wrapping_add(origin_sequence) % THICKET_TREES as u64) as u8
}
