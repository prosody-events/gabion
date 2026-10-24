use std::net::SocketAddr;

use super::*;
use crate::crdt::CellHandle;
use crate::defaults::THICKET_TREES;

fn addr(i: u16) -> SocketAddr {
    SocketAddr::from(([10, 0, (i / 256) as u8, (i % 256) as u8], 7000))
}

/// A node at `addr(0)` that knows peers `addr(1)..=addr(n)`.
fn cluster(n: u16) -> Thicket {
    let peers: Vec<SocketAddr> = (1..=n).map(addr).collect();
    Thicket::new(addr(0), &peers, 0, 64, 64, 256, 64)
}

fn root(slot: NodeSlot, local: bool) -> Root {
    Root {
        slot,
        node_id: NodeId(0x1000 + u128::from(slot)),
        incarnation: 1,
        local,
    }
}

fn peer_of_class(t: &Thicket, class: u8) -> u16 {
    t.peers.iter().position(|p| p.class == class).unwrap() as u16
}

#[test]
fn a_change_takes_each_tree_in_turn() {
    let origin = NodeId(0xABCD_EF01_2345_6789_0000_1111_2222_3333);
    for start in [1u64, 100, 4_000_000_007] {
        let mut seen = [0usize; THICKET_TREES];
        for seq in start..start + 3 * THICKET_TREES as u64 {
            seen[usize::from(tree_of(origin, seq))] += 1;
        }
        assert_eq!(seen, [3; THICKET_TREES], "start {start}");
    }
    assert_eq!(tree_of(origin, 77), tree_of(origin, 77));
}

#[test]
fn classes_spread_a_subnet_across_the_trees() {
    let mut count = [0usize; THICKET_TREES];
    for i in 0..90 {
        count[usize::from(class_of(&addr(i)))] += 1;
    }
    for (tree, n) in count.iter().enumerate() {
        assert!(*n >= 15, "tree {tree} has only {n} of 90 nodes: {count:?}");
    }
    assert_eq!(class_of(&addr(5)), class_of(&addr(5)));
}

#[test]
fn a_leaf_never_relays_a_change_of_a_tree_it_is_a_leaf_of() {
    let mut t = cluster(30);
    t.maintain_for_test();
    let class = t.class;
    for tree in (0..THICKET_TREES as u8).filter(|&tree| tree != class) {
        for dest in 0..t.peers.len() as u16 {
            // A change that came from elsewhere is never forwarded, whoever
            // the destination is: view member, leaf child, or parent.
            t.peers[usize::from(dest)].leaf_child = true;
            assert!(
                !t.pushes_to(dest, &root(3, false), tree),
                "relayed a tree {tree} change to {dest}"
            );
        }
    }
}

#[test]
fn an_interior_node_pushes_to_its_eager_view_and_leaf_children_only() {
    let mut t = cluster(30);
    t.maintain_for_test();
    let class = t.class;
    let r = root(5, false);
    let in_view: Vec<u16> = (0..t.peers.len() as u16)
        .filter(|&i| t.in_view(i))
        .collect();
    assert_eq!(in_view.len(), t.view_target());
    for &mate in &in_view {
        assert!(t.pushes_to(mate, &r, class), "new view member is eager");
    }
    let outsider = (0..t.peers.len() as u16)
        .find(|&i| !t.in_view(i) && t.peers[usize::from(i)].class == class)
        .unwrap();
    assert!(!t.pushes_to(outsider, &r, class));

    // Pruned for one root: lazy there, still eager for the others.
    t.set_lazy(in_view[0], &r);
    assert!(!t.pushes_to(in_view[0], &r, class));
    assert!(t.pushes_to(in_view[0], &root(6, false), class));
    t.set_eager(in_view[0], &r);
    assert!(t.pushes_to(in_view[0], &r, class));

    // A leaf child gets every change of this tree, from any root.
    let leaf = peer_of_class(&t, (class + 1) % THICKET_TREES as u8);
    assert!(!t.pushes_to(leaf, &r, class));
    t.peers[usize::from(leaf)].leaf_child = true;
    assert!(t.pushes_to(leaf, &r, class));
    assert!(t.pushes_to(leaf, &root(9, false), class));
}

#[test]
fn a_local_change_in_another_tree_goes_to_the_parent_alone() {
    let mut t = cluster(30);
    t.maintain_for_test();
    let me = root(0, true);
    let class = t.class;
    for tree in (0..THICKET_TREES as u8).filter(|&tree| tree != class) {
        let parent = t.parents[usize::from(tree)];
        assert_ne!(parent, NONE);
        assert_eq!(t.peers[usize::from(parent)].class, tree);
        for dest in 0..t.peers.len() as u16 {
            assert_eq!(t.pushes_to(dest, &me, tree), dest == parent);
        }
    }
}

#[test]
fn a_tree_with_no_interior_node_is_served_directly() {
    // Peers of one class only: the other trees have no interior node here.
    let mut t = Thicket::new(addr(0), &[], 0, 64, 64, 256, 64);
    let mates: Vec<SocketAddr> = (1..400)
        .map(addr)
        .filter(|a| class_of(a) == t.class)
        .take(4)
        .collect();
    for a in &mates {
        t.add_peer(*a, 0);
    }
    let empty = (0..THICKET_TREES as u8)
        .find(|&tree| t.tree_is_empty(tree))
        .unwrap();
    for dest in 0..t.peers.len() as u16 {
        assert!(t.pushes_to(dest, &root(0, true), empty));
        assert!(!t.pushes_to(dest, &root(4, false), empty));
    }
}

#[test]
fn peer_slots_are_reused_and_class_counts_follow_membership() {
    let mut t = cluster(20);
    let total: u16 = t.class_alive.iter().sum();
    assert_eq!(usize::from(total), t.alive);
    t.maintain_for_test();

    let gone = t.peers[3].addr;
    let class = t.peers[3].class;
    let before = t.class_alive[usize::from(class)];
    t.remove_peer(gone);
    assert_eq!(t.alive, 19);
    assert_eq!(t.class_alive[usize::from(class)], before - 1);
    assert!(t.index_of(gone).is_none());

    let (idx, new) = t.add_peer(addr(500), 0);
    assert!(new);
    assert_eq!(idx, 3, "the freed slot is reused");
    assert_eq!(t.peers.len(), 20);
    assert!(!t.add_peer(addr(500), 0).1, "a known peer is not new");
}

#[test]
fn removing_a_parent_or_view_member_clears_every_reference() {
    let mut t = cluster(30);
    t.maintain_for_test();
    let tree = (0..THICKET_TREES as u8).find(|&x| x != t.class).unwrap();
    let parent = t.parents[usize::from(tree)];
    let parent_addr = t.peers[usize::from(parent)].addr;
    t.remove_peer(parent_addr);
    assert_eq!(t.parents[usize::from(tree)], NONE);

    let mate = (0..t.peers.len() as u16).find(|&i| t.in_view(i)).unwrap();
    let mate_addr = t.peers[usize::from(mate)].addr;
    let r = root(2, false);
    t.remove_peer(mate_addr);
    assert!(!t.in_view(mate));
    assert!(!t.pushes_to(mate, &r, t.class));
}

#[test]
fn a_slot_taken_by_another_origin_starts_eager_to_the_whole_view() {
    let mut t = cluster(30);
    t.maintain_for_test();
    let mate = (0..t.peers.len() as u16).find(|&i| t.in_view(i)).unwrap();
    let first = root(7, false);
    t.set_lazy(mate, &first);
    assert!(!t.pushes_to(mate, &first, t.class));
    let reused = Root {
        node_id: NodeId(0x9999),
        ..first
    };
    assert!(t.pushes_to(mate, &reused, t.class));
}

impl Thicket {
    /// Fill the backbone view and pick a parent in every other tree, as the
    /// runtime's upkeep does.
    fn maintain_for_test(&mut self) {
        let mut rng = SplitMix64::new(7);
        self.fill_view(&mut rng);
        let class = self.class;
        for tree in (0..THICKET_TREES as u8).filter(|&t| t != class) {
            self.choose_parent(tree, &mut rng, NONE);
        }
    }
}

#[test]
fn a_cell_is_queued_for_ihave_once_per_version_and_again_once_taken() {
    let mut t = cluster(3);
    let cell = |index, generation| CellHandle { index, generation };

    t.queue_lazy(cell(4, 1));
    t.queue_lazy(cell(4, 1));
    t.queue_lazy(cell(5, 1));
    assert_eq!(t.lazy.len(), 2, "a cell that changed again is named once");

    // The slot was freed and reused before the next heartbeat: the new cell
    // is another version and is queued as well.
    t.queue_lazy(cell(4, 3));
    assert_eq!(t.lazy.len(), 3);

    t.dequeue_lazy(2);
    assert_eq!(t.lazy, vec![cell(4, 3)]);
    t.queue_lazy(cell(4, 3));
    assert_eq!(
        t.lazy.len(),
        1,
        "the version still queued is not queued twice"
    );
    t.queue_lazy(cell(5, 1));
    assert_eq!(t.lazy.len(), 2, "a taken cell can be queued again");
}

/// Addresses as the benchmark harness assigns them.
fn harness_addr(i: u16) -> SocketAddr {
    SocketAddr::from(([10, (i % 3) as u8, 0, i as u8], 7946))
}

/// `n` nodes that each learn the others by discovery events in index order,
/// then attach their parents, the parents refusing leaves over their cap.
fn discovered_in_index_order(n: u16) -> Vec<Thicket> {
    let mut nodes: Vec<Thicket> = (0..n)
        .map(|i| Thicket::new(harness_addr(i), &[], 0, usize::from(n), 64, 256, 64))
        .collect();
    let mut rngs: Vec<SplitMix64> = (0..n).map(|i| SplitMix64::new(u64::from(i))).collect();
    for peer in 0..n {
        for (i, node) in nodes.iter_mut().enumerate() {
            if i as u16 == peer {
                continue;
            }
            let (idx, new) = node.add_peer(harness_addr(peer), 0);
            assert!(new);
            node.place_new(idx, &mut rngs[i]);
            node.fill_view(&mut rngs[i]);
            let class = node.class;
            for tree in (0..THICKET_TREES as u8).filter(|&t| t != class) {
                if node.parents[usize::from(tree)] == NONE {
                    node.choose_parent(tree, &mut rngs[i], NONE);
                }
            }
        }
    }
    // Attach every leaf; a refused one picks another parent, as the runtime
    // does on the parent's PRUNE.
    for _round in 0..10 {
        for child in 0..nodes.len() {
            let class = nodes[child].class;
            for tree in (0..THICKET_TREES as u8).filter(|&t| t != class) {
                let parent = nodes[child].parents[usize::from(tree)];
                if parent == NONE {
                    continue;
                }
                let at = nodes[child].peers[usize::from(parent)].addr;
                let p = (0..nodes.len())
                    .find(|&p| harness_addr(p as u16) == at)
                    .unwrap();
                let me = nodes[p].index_of(harness_addr(child as u16)).unwrap();
                if !nodes[p].adopt_leaf(me) {
                    nodes[child].choose_parent(tree, &mut rngs[child], parent);
                }
            }
        }
    }
    nodes
}

#[test]
fn parents_and_views_are_spread_when_discovery_orders_the_peers() {
    const N: u16 = 80;
    let nodes = discovered_in_index_order(N);
    let index = |a: SocketAddr| (0..N).find(|&i| harness_addr(i) == a).unwrap();

    // Parents: each tree's leaves hang off many interior nodes, and none has
    // more children than it takes.
    for tree in 0..THICKET_TREES as u8 {
        let mut children = vec![0usize; usize::from(N)];
        for node in nodes.iter().filter(|n| n.class != tree) {
            let parent = node.parents[usize::from(tree)];
            assert_ne!(parent, NONE, "a leaf without a parent in tree {tree}");
            children[usize::from(index(node.peers[usize::from(parent)].addr))] += 1;
        }
        let distinct = children.iter().filter(|&&c| c > 0).count();
        let interior = nodes.iter().filter(|n| n.class == tree).count();
        assert!(
            distinct * 2 >= interior,
            "tree {tree}: {distinct} of {interior} interior nodes have a child"
        );
        for (p, &c) in children.iter().enumerate() {
            assert!(
                c <= nodes[p].leaf_cap(),
                "node {p} has {c} children in tree {tree}, over its cap {}",
                nodes[p].leaf_cap()
            );
        }
    }

    // Views: every node is in some view and none is in a disproportionate
    // share of them.
    let mut in_views = vec![0usize; usize::from(N)];
    let mut members = 0;
    for node in &nodes {
        for p in node.peers.iter().filter(|p| p.view_pos != NO_POS) {
            in_views[usize::from(index(p.addr))] += 1;
            members += 1;
        }
    }
    let mean = members as f64 / f64::from(N);
    let absent = in_views.iter().filter(|&&c| c == 0).count();
    let busiest = *in_views.iter().max().unwrap();
    assert!(absent <= 4, "{absent} of {N} nodes are in no view");
    assert!(
        (busiest as f64) <= 3.0 * mean,
        "a node is in {busiest} views, mean {mean:.1}"
    );
}

#[test]
fn a_parent_over_its_cap_refuses_and_the_leaf_picks_another() {
    let mut t = cluster(60);
    let class = t.class;
    let cap = t.leaf_cap();
    let leaves: Vec<u16> = (0..t.peers.len() as u16)
        .filter(|&i| t.peers[usize::from(i)].class != class)
        .collect();
    assert!(leaves.len() > cap);
    for &leaf in &leaves[..cap] {
        assert!(t.adopt_leaf(leaf));
    }
    assert!(!t.adopt_leaf(leaves[cap]), "over the cap");
    assert!(t.adopt_leaf(leaves[0]), "a child it already has stays");
    t.peers[usize::from(leaves[0])].leaf_child = false;
    assert!(t.adopt_leaf(leaves[cap]), "a freed place is taken");

    let tree = (0..THICKET_TREES as u8).find(|&x| x != class).unwrap();
    let mut rng = SplitMix64::new(3);
    let refuser = t.choose_parent(tree, &mut rng, NONE).unwrap();
    for _ in 0..20 {
        let other = t.choose_parent(tree, &mut rng, refuser).unwrap();
        assert_ne!(other, refuser);
        assert_eq!(t.peers[usize::from(other)].class, tree);
    }
}
