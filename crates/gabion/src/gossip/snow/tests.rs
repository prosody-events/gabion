use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use super::flight::full_mask;
use super::*;

fn addr(i: u16) -> SocketAddr {
    SocketAddr::from(([10, 0, (i >> 8) as u8, i as u8], 4000 + i))
}

/// A node's view: itself and the listed peers.
fn ring_of(me: u16, members: impl IntoIterator<Item = u16>) -> Ring {
    let mut ring = Ring::new(addr(me), 16);
    for m in members {
        ring.insert(addr(m));
    }
    ring
}

/// Walk the whole tree for `root` with every node holding the same view.
/// Returns, per node, the parent it was reached from and its depth.
fn walk(n: u16, root: u16, k: usize) -> BTreeMap<SocketAddr, (SocketAddr, u32)> {
    let mut reached = BTreeMap::new();
    let mut out = Vec::new();
    let mut queue = Vec::new();
    ring_of(root, 0..n).root_children(k, &mut out);
    assert!(out.len() <= k, "root fans out to at most k");
    queue.extend(out.iter().map(|c| (c.addr, addr(root), 1_u32, *c)));
    while let Some((node, parent, depth, child)) = queue.pop() {
        let previous = reached.insert(node, (parent, depth));
        assert!(previous.is_none(), "{node} reached twice");
        let me = (0..n).find(|&i| addr(i) == node).expect("known node");
        let mut ring = ring_of(me, 0..n);
        ring.children(addr(root), child.lo, child.hi, k, &mut out);
        assert!(out.len() <= k, "interior node fans out to at most k");
        queue.extend(out.iter().map(|c| (c.addr, node, depth + 1, *c)));
    }
    reached
}

#[test]
fn every_node_is_reached_exactly_once_within_the_tree_height() {
    for k in [2, 4, 8] {
        for n in [1_u16, 2, 3, 5, 9, 17, 80, 183, 300] {
            for root in [0, n / 3, n - 1] {
                let reached = walk(n, root, k);
                let expected: BTreeSet<_> = (0..n).filter(|&i| i != root).map(addr).collect();
                assert_eq!(
                    reached.keys().copied().collect::<BTreeSet<_>>(),
                    expected,
                    "n={n} k={k} root={root}"
                );
                let deepest = reached.values().map(|&(_, d)| d).max().unwrap_or(0);
                assert!(
                    deepest <= tree_height(usize::from(n), k),
                    "n={n} k={k}: depth {deepest} above height {}",
                    tree_height(usize::from(n), k)
                );
            }
        }
    }
}

#[test]
fn small_clusters_are_flat() {
    // With at most k other nodes the root reaches all of them directly.
    let mut out = Vec::new();
    ring_of(0, 0..5).root_children(4, &mut out);
    assert_eq!(out.len(), 4);
    assert!(
        out.iter().all(|c| c.lo == c.addr),
        "every child is a one-node range"
    );
    let reached = walk(5, 0, 4);
    assert!(
        reached
            .values()
            .all(|&(parent, depth)| depth == 1 && parent == addr(0))
    );
}

#[test]
fn children_depend_only_on_view_root_and_range() {
    let mut a = ring_of(7, 0..50);
    let mut b = ring_of(7, (0..50).rev());
    let (mut ca, mut cb) = (Vec::new(), Vec::new());
    a.children(addr(20), addr(30), Some(addr(45)), 4, &mut ca);
    b.children(addr(20), addr(30), Some(addr(45)), 4, &mut cb);
    assert_eq!(ca, cb);
    // Asking twice gives the same answer: no hidden state.
    a.children(addr(20), addr(30), Some(addr(45)), 4, &mut cb);
    assert_eq!(ca, cb);
}

#[test]
fn a_boundary_missing_from_the_local_view_is_inserted() {
    // This node has not discovered 40, which the parent names as the start of
    // the range it hands over.
    let mut stale = ring_of(41, (0..40).chain(41..50));
    let mut full = ring_of(41, 0..50);
    let (mut from_stale, mut from_full) = (Vec::new(), Vec::new());
    stale.children(addr(0), addr(40), Some(addr(50 - 1)), 4, &mut from_stale);
    full.children(addr(0), addr(40), Some(addr(50 - 1)), 4, &mut from_full);
    assert!(!from_stale.is_empty());
    assert_eq!(from_stale, from_full);
    assert_eq!(stale.len(), 49, "the view itself is not changed");
}

#[test]
fn a_root_with_a_stale_view_misses_at_most_the_node_at_the_rotation_cut() {
    // The root has already dropped node 13; everyone else still lists it.
    // Rotation puts the root at `len / 2`, so the two views cut the ring one
    // position apart and the node at the cut can fall out of every carried
    // range. The rest of the shared view is still reached. This is the cost
    // the carried-range rule accepts; the repair pass covers the one node.
    let (n, root, k) = (40_u16, 5_u16, 4);
    let mut reached = BTreeSet::new();
    let mut out = Vec::new();
    let mut queue = Vec::new();
    ring_of(root, (0..n).filter(|&i| i != 13)).root_children(k, &mut out);
    queue.extend(out.iter().copied());
    while let Some(child) = queue.pop() {
        assert!(reached.insert(child.addr));
        let me = (0..n).find(|&i| addr(i) == child.addr).expect("node");
        ring_of(me, 0..n).children(addr(root), child.lo, child.hi, k, &mut out);
        queue.extend(out.iter().copied());
    }
    let shared: BTreeSet<_> = (0..n).filter(|&i| i != root && i != 13).map(addr).collect();
    let missed: Vec<_> = shared.difference(&reached).collect();
    assert!(missed.len() <= 1, "missed {missed:?}");
}

#[test]
fn ring_membership_edits() {
    let mut ring = ring_of(3, [1, 2, 4]);
    assert_eq!(ring.len(), 4);
    assert!(!ring.insert(addr(2)));
    assert!(!ring.insert(addr(3)), "the local node is already a member");
    assert!(ring.remove(addr(2)));
    assert!(!ring.remove(addr(2)));
    assert!(
        !ring.remove(addr(3)),
        "the local node cannot leave its own ring"
    );
    assert_eq!(ring.peers().collect::<Vec<_>>(), vec![addr(1), addr(4)]);
}

#[test]
fn tree_height_matches_the_paper() {
    assert_eq!(tree_height(1, 4), 2);
    assert_eq!(tree_height(80, 4), 5);
    assert_eq!(tree_height(500, 4), 7);
    assert_eq!(tree_height(500, 2), 10);
}

#[test]
fn ctrl_roundtrips_both_address_families() {
    let v4 = addr(7);
    let v6 = SocketAddr::from((Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 9), 4100));
    let cases = [
        Ctrl::Data {
            root: v4,
            seq: 9,
            lo: v4,
            hi: None,
        },
        Ctrl::Data {
            root: v6,
            seq: u32::MAX,
            lo: v6,
            hi: Some(v4),
        },
        Ctrl::Data {
            root: v6,
            seq: 0,
            lo: v6,
            hi: Some(v6),
        },
        Ctrl::Ack { root: v4, seq: 1 },
        Ctrl::Ack { root: v6, seq: 2 },
    ];
    for ctrl in cases {
        let mut buf = [0; CTRL_MAX_LEN];
        let bytes = ctrl.encode(&mut buf).to_vec();
        assert_eq!(Ctrl::decode(&bytes), Some(ctrl));
        assert!(bytes.len() <= CTRL_MAX_LEN);
        // Any truncation or trailing byte is rejected.
        for cut in 0..bytes.len() {
            assert_eq!(Ctrl::decode(&bytes[..cut]), None, "cut at {cut}");
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert_eq!(Ctrl::decode(&longer), None);
    }
    let worst = Ctrl::Data {
        root: v6,
        seq: 0,
        lo: v6,
        hi: Some(v6),
    };
    assert_eq!(worst.encode(&mut [0; CTRL_MAX_LEN]).len(), CTRL_MAX_LEN);
    assert_eq!(Ctrl::decode(&[9, 0]), None);
    let _ = Ipv4Addr::LOCALHOST;
}

fn relay(root: u16, seq: u32) -> Relay {
    Relay {
        root: addr(root),
        seq,
        parent: addr(0),
        lo: addr(1),
        hi: None,
        acked: 0,
        done: false,
    }
}

#[test]
fn relays_forget_the_oldest_when_full() {
    let mut relays = Relays::new(3);
    for seq in 0..3 {
        relays.insert(relay(1, seq));
    }
    assert!(relays.find(addr(1), 0).is_some());
    relays.insert(relay(1, 3));
    assert!(
        relays.find(addr(1), 0).is_none(),
        "oldest entry was overwritten"
    );
    for seq in 1..4 {
        let idx = relays.find(addr(1), seq).expect("newer entries stay");
        assert_eq!(relays.get_mut(idx).map(|r| r.seq), Some(seq));
    }
    assert!(
        relays.find(addr(2), 1).is_none(),
        "the root is part of the key"
    );
}

#[test]
fn origins_reuse_free_slots_then_displace_the_oldest() {
    let limits = crate::wire::FrameLimits::default();
    let mut origins = Origins::new(2, limits, 100);
    let (a, displaced) = origins.claim(10);
    assert!(!displaced);
    let (b, displaced) = origins.claim(20);
    assert!(!displaced);
    assert_ne!(a, b);
    assert_eq!(origins.live(), 2);
    assert_eq!(origins.get(a).seq, 101);
    assert_eq!(origins.find(102), Some(b));

    let (c, displaced) = origins.claim(30);
    assert!(displaced);
    assert_eq!(c, a, "the oldest broadcast is displaced");
    assert_eq!(origins.find(101), None);
    assert_eq!(origins.live(), 2);

    origins.release(b);
    origins.release(b);
    assert_eq!(origins.live(), 1);
    assert_eq!(origins.find(102), None);
}

#[test]
fn ack_masks_cover_exactly_the_children() {
    assert_eq!(full_mask(0), 0);
    assert_eq!(full_mask(4), 0b1111);
    assert_eq!(full_mask(16), u16::MAX);
}

#[test]
fn rtt_starts_at_the_seed_then_follows_samples() {
    let mut rtt = RttEstimator::default();
    assert_eq!(rtt.expected_ms(20), 20);
    rtt.sample(10);
    assert_eq!(
        rtt.expected_ms(20),
        10,
        "the first sample replaces the seed"
    );
    for _ in 0..200 {
        rtt.sample(30);
    }
    assert_eq!(rtt.expected_ms(20), 30);
    rtt.sample(0);
    assert!(rtt.expected_ms(20) >= 1);
}
