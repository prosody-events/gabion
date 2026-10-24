use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use super::digest::{self, Entry};
use super::flight::full_mask;
use super::roots::{Gap, MAX_GAPS};
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
    let mut out = Children::new();
    let mut queue = Vec::new();
    ring_of(root, 0..n).root_children(k, &mut out);
    assert!(out.len() <= k, "root fans out to at most k");
    queue.extend(out.iter().map(|c| (c.addr, addr(root), 1_u32, *c)));
    while let Some((node, parent, depth, child)) = queue.pop() {
        let previous = reached.insert(node, (parent, depth));
        assert!(previous.is_none(), "{node} reached twice");
        let me = (0..n).find(|&i| addr(i) == node).expect("known node");
        let mut ring = ring_of(me, 0..n);
        ring.children(child.lo, child.hi, k, &mut out);
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
    let mut out = Children::new();
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
    let (mut ca, mut cb) = (Children::new(), Children::new());
    a.children(addr(30), addr(45), 4, &mut ca);
    b.children(addr(30), addr(45), 4, &mut cb);
    assert_eq!(ca, cb);
    // Asking twice gives the same answer: no hidden state.
    a.children(addr(30), addr(45), 4, &mut cb);
    assert_eq!(ca, cb);
}

#[test]
fn a_boundary_missing_from_the_local_view_is_inserted() {
    // This node has not discovered 40, which the parent names as the start of
    // the range it hands over.
    let mut stale = ring_of(41, (0..40).chain(41..50));
    let mut full = ring_of(41, 0..50);
    let (mut from_stale, mut from_full) = (Children::new(), Children::new());
    stale.children(addr(40), addr(49), 4, &mut from_stale);
    full.children(addr(40), addr(49), 4, &mut from_full);
    assert!(!from_stale.is_empty());
    assert_eq!(from_stale, from_full);
    assert_eq!(stale.len(), 49, "the view itself is not changed");
}

/// Walk the tree for `root` when each node's view is given by `view_of`.
/// Returns every node reached; panics if one is reached twice.
fn walk_with_views(
    n: u16,
    root: u16,
    k: usize,
    view_of: impl Fn(u16) -> Vec<u16>,
) -> BTreeSet<SocketAddr> {
    let mut reached = BTreeSet::new();
    let mut out = Children::new();
    let mut queue = Vec::new();
    ring_of(root, view_of(root)).root_children(k, &mut out);
    queue.extend(out.iter().copied());
    while let Some(child) = queue.pop() {
        assert!(reached.insert(child.addr), "{} reached twice", child.addr);
        let me = (0..n).find(|&i| addr(i) == child.addr).expect("node");
        ring_of(me, view_of(me)).children(child.lo, child.hi, k, &mut out);
        queue.extend(out.iter().copied());
    }
    reached
}

#[test]
fn a_node_with_a_stale_view_loses_at_most_the_member_it_does_not_know() {
    // One node (the root, an interior node or a leaf) has not discovered one
    // member. Ranges are positions on the ring rather than offsets into a
    // view-sized sequence, so the only node that can be missed is the one that
    // node does not know about.
    let k = 4;
    for n in [8_u16, 20, 40] {
        for root in (0..n).step_by(if n <= 20 { 1 } else { 7 }) {
            for stale in (0..n).step_by(if n <= 20 { 1 } else { 3 }) {
                for unknown in (0..n).filter(|&u| u != stale && u != root) {
                    let reached = walk_with_views(n, root, k, |me| {
                        (0..n).filter(|&i| !(me == stale && i == unknown)).collect()
                    });
                    let expected: BTreeSet<_> = (0..n).filter(|&i| i != root).map(addr).collect();
                    let missed: Vec<_> = expected.difference(&reached).collect();
                    assert!(
                        missed.iter().all(|&&a| a == addr(unknown)),
                        "n={n} root={root} stale={stale} unknown={unknown}: missed {missed:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn a_view_that_lacks_a_member_does_not_cut_a_range_in_two() {
    // 80 nodes, root 0. Node 50 has not discovered node 1. The range its
    // parent hands it still starts and ends on shared members, so it keeps
    // its subtree rather than treating the range as empty.
    let (n, k) = (80_u16, 4);
    let reached = walk_with_views(n, 0, k, |me| {
        (0..n).filter(|&i| !(me == 50 && i == 1)).collect()
    });
    let expected: BTreeSet<_> = (1..n).map(addr).collect();
    let missed: Vec<_> = expected.difference(&reached).collect();
    assert!(missed.iter().all(|&&a| a == addr(1)), "missed {missed:?}");
}

#[test]
fn a_node_given_a_range_with_other_nodes_in_it_is_never_a_leaf() {
    let mut ring = ring_of(5, 0..20);
    let mut out = Children::new();
    for lo in 0..20 {
        for len in 2..19 {
            ring.children(addr(lo), addr((lo + len) % 20), 4, &mut out);
            assert!(!out.is_empty(), "range {lo}+{len} gave no children");
        }
    }
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
            hi: v6,
        },
        Ctrl::Data {
            root: v6,
            seq: u32::MAX,
            lo: v6,
            hi: v4,
        },
        Ctrl::Data {
            root: v6,
            seq: 0,
            lo: v6,
            hi: v6,
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
        hi: v6,
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
        children: Children::new(),
        acked: 0,
        done: false,
        sent_ms: 0,
        resent: false,
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
    assert_eq!(full_mask(8), u8::MAX);
}

#[test]
fn rtt_is_the_height_times_the_seed_until_measured() {
    let mut rtt = RttEstimator::default();
    assert_eq!(rtt.expected_ms(5, 4), 20);
    rtt.sample_hop(2);
    assert_eq!(
        rtt.expected_ms(5, 4),
        10,
        "the first hop sample replaces the seed"
    );
    for _ in 0..200 {
        rtt.sample_hop(3);
    }
    assert_eq!(rtt.expected_ms(5, 4), 15);
    rtt.sample_hop(0);
    assert!(rtt.expected_ms(5, 4) >= 5, "a sample is at least 1 ms");
}

#[test]
fn a_slower_whole_tree_measurement_wins_over_the_hop_estimate() {
    let mut rtt = RttEstimator::default();
    rtt.sample_hop(1);
    rtt.sample_tree(40);
    assert_eq!(rtt.expected_ms(5, 4), 40);
    rtt.sample_tree(1);
    assert_eq!(rtt.expected_ms(5, 4), 40 - 40 / 8 + 1);
}

#[test]
fn rtt_backs_off_only_while_unmeasured() {
    let mut rtt = RttEstimator::default();
    for _ in 0..10 {
        rtt.timed_out();
    }
    assert_eq!(
        rtt.expected_ms(5, 4),
        20 << RttEstimator::MAX_BACKOFF,
        "backoff is capped"
    );
    rtt.sample_hop(2);
    assert_eq!(rtt.expected_ms(5, 4), 10, "a sample ends the backoff");
    rtt.timed_out();
    assert_eq!(
        rtt.expected_ms(5, 4),
        10,
        "loss does not stretch a measured round trip"
    );
}

fn gap(after: u64, before: u64) -> Gap {
    Gap { after, before }
}

#[test]
fn ctrl_roundtrips_pull_fill_and_sweep() {
    for ctrl in [
        Ctrl::Pull {
            after: 7,
            before: u64::MAX,
        },
        Ctrl::Fill {
            after: 0,
            through: 1 << 40,
        },
    ] {
        let mut buf = [0; CTRL_MAX_LEN];
        let bytes = ctrl.encode(&mut buf);
        assert_eq!(bytes.len(), 17);
        assert_eq!(Ctrl::decode(bytes), Some(ctrl));
        assert_eq!(Ctrl::decode(&bytes[..bytes.len() - 1]), None);
    }
    for ctrl in [
        Ctrl::Sweep { from: 7 },
        Ctrl::Swept {
            from: 7,
            next: SWEEP_DONE,
        },
    ] {
        let mut buf = [0; CTRL_MAX_LEN];
        let bytes = ctrl.encode(&mut buf);
        assert_eq!(Ctrl::decode(bytes), Some(ctrl));
        assert_eq!(Ctrl::decode(&bytes[..bytes.len() - 1]), None);
    }
}

#[test]
fn consecutive_broadcasts_leave_no_gap_and_a_skipped_one_does() {
    let mut roots = Roots::new(4);
    roots.broadcast(addr(1), 10, (1, 3), 0, 5);
    roots.broadcast(addr(1), 11, (5, 9), 0, 5);
    assert!(
        roots.gaps(addr(1)).is_empty(),
        "4 was superseded by a later change"
    );
    roots.broadcast(addr(1), 13, (14, 15), 0, 5);
    assert_eq!(roots.gaps(addr(1)), vec![gap(9, 14)]);
    assert!(roots.maybe_gaps());
}

#[test]
fn a_late_copy_fills_what_it_carries() {
    let mut roots = Roots::new(4);
    roots.broadcast(addr(1), 1, (1, 1), 0, 0);
    roots.broadcast(addr(1), 5, (20, 20), 0, 0);
    assert_eq!(roots.gaps(addr(1)), vec![gap(1, 20)]);
    roots.broadcast(addr(1), 3, (8, 10), 0, 0);
    assert_eq!(roots.gaps(addr(1)), vec![gap(1, 8), gap(10, 20)]);
    roots.broadcast(addr(1), 2, (2, 7), 0, 0);
    roots.broadcast(addr(1), 4, (11, 19), 0, 0);
    assert!(roots.gaps(addr(1)).is_empty());
}

#[test]
fn the_first_broadcast_from_a_root_opens_the_gap_below_it() {
    let mut roots = Roots::new(4);
    roots.broadcast(addr(1), 1, (1, 2), 0, 0);
    assert!(roots.gaps(addr(1)).is_empty(), "nothing came before");
    roots.broadcast(addr(2), 40, (300, 310), 0, 0);
    assert_eq!(roots.gaps(addr(2)), vec![gap(0, 300)]);
}

#[test]
fn a_restarted_root_starts_a_new_log() {
    let mut roots = Roots::new(4);
    roots.broadcast(addr(1), 1, (1, 500), 0, 0);
    roots.broadcast(addr(1), 2, (3, 4), 0, 0);
    assert_eq!(roots.gaps(addr(1)), vec![gap(0, 3)]);
}

#[test]
fn pulls_wait_for_their_due_time_and_back_off_until_answered() {
    let mut roots = Roots::new(4);
    roots.broadcast(addr(1), 1, (1, 1), 0, 0);
    roots.broadcast(addr(1), 3, (9, 9), 0, 100);
    let mut sent = Vec::new();
    let mut pull = |roots: &mut Roots, now| {
        roots.pull_due(
            now,
            10,
            500,
            |_| true,
            |root, g| {
                sent.push((now, root, g));
                true
            },
        )
    };
    pull(&mut roots, 99);
    pull(&mut roots, 100);
    pull(&mut roots, 105);
    pull(&mut roots, 110);
    pull(&mut roots, 125);
    pull(&mut roots, 130);
    let times: Vec<u64> = sent.iter().map(|s| s.0).collect();
    assert_eq!(times, vec![100, 110, 130], "retries after 10, then 20 ms");
    assert!(sent.iter().all(|s| s.2 == gap(1, 9)));

    assert!(roots.filled(addr(1), 1, 4, 140), "5..=8 remain");
    assert_eq!(roots.gaps(addr(1)), vec![gap(4, 9)]);
    assert!(
        !roots.filled(addr(1), 1, 6, 141),
        "a stale answer is ignored"
    );
    assert!(!roots.filled(addr(1), 4, 8, 142));
    assert!(roots.gaps(addr(1)).is_empty());
}

#[test]
fn pulls_to_a_root_that_is_not_a_member_stop() {
    let mut roots = Roots::new(4);
    roots.broadcast(addr(1), 5, (9, 9), 0, 0);
    roots.broadcast(addr(2), 5, (9, 9), 0, 0);
    let mut sent = [0; 3];
    for now in 0..10_000 {
        roots.pull_due(
            now,
            10,
            500,
            |root| root == addr(2),
            |root, _| {
                sent[usize::from(root.port() - 4000)] += 1;
                true
            },
        );
    }
    assert_eq!(sent[1], u32::from(Roots::MAX_ATTEMPTS));
    assert!(sent[2] > 20, "a member keeps being asked");
    assert!(roots.gaps(addr(1)).is_empty());
}

#[test]
fn gaps_beyond_the_table_merge_with_a_neighbour() {
    let mut roots = Roots::new(4);
    roots.broadcast(addr(1), 1, (1, 1), 0, 0);
    let mut seq = 1;
    for i in 0..(MAX_GAPS as u64 + 2) {
        seq += 2;
        roots.broadcast(addr(1), seq, (10 * i + 5, 10 * i + 5), 0, 0);
    }
    let gaps = roots.gaps(addr(1));
    assert_eq!(gaps.len(), MAX_GAPS);
    assert!(gaps.windows(2).all(|w| w[0].before <= w[1].after));
    assert_eq!(gaps[0].after, 1);
    assert_eq!(gaps.last().unwrap().before, 10 * (MAX_GAPS as u64 + 1) + 5);
}

#[test]
fn a_full_table_forgets_the_root_heard_from_longest_ago() {
    let mut roots = Roots::new(2);
    roots.broadcast(addr(1), 1, (5, 5), 10, 0);
    roots.broadcast(addr(2), 1, (5, 5), 20, 0);
    roots.broadcast(addr(3), 1, (5, 5), 30, 0);
    assert!(roots.gaps(addr(1)).is_empty());
    assert_eq!(roots.gaps(addr(2)), vec![gap(0, 5)]);
    assert_eq!(roots.gaps(addr(3)), vec![gap(0, 5)]);
}

#[test]
fn digests_roundtrip_and_stop_at_a_torn_entry() {
    let entries = [
        Entry {
            root: addr(1),
            seq: 7,
            high: 99,
        },
        Entry {
            root: SocketAddr::from((Ipv6Addr::LOCALHOST, 9)),
            seq: u32::MAX,
            high: u64::MAX,
        },
    ];
    let mut buf = Vec::new();
    digest::begin(&mut buf);
    assert!(!digest::has_entries(&buf));
    for e in entries {
        digest::push(&mut buf, e);
    }
    assert!(digest::is_digest(&buf));
    assert!(
        Ctrl::decode(&buf).is_none(),
        "the tree codec leaves digests alone"
    );
    assert_eq!(digest::entries(&buf).collect::<Vec<_>>(), entries);
    let torn = &buf[..buf.len() - 1];
    assert_eq!(digest::entries(torn).collect::<Vec<_>>(), entries[..1]);
}

#[test]
fn a_digest_ahead_of_this_node_opens_the_gap_to_it() {
    let mut roots = Roots::new(4);
    roots.broadcast(addr(1), 10, (1, 5), 0, 0);
    roots.advertised(addr(1), 9, 3, 0, 0);
    roots.advertised(addr(1), 10, 5, 0, 0);
    assert!(
        roots.gaps(addr(1)).is_empty(),
        "behind or level: nothing to pull"
    );
    roots.advertised(addr(1), 12, 20, 0, 0);
    assert_eq!(roots.gaps(addr(1)), vec![gap(5, 21)]);
    roots.broadcast(addr(1), 13, (21, 22), 0, 0);
    assert_eq!(
        roots.gaps(addr(1)),
        vec![gap(5, 21)],
        "13 continues the run"
    );
    roots.advertised(addr(2), 4, 8, 0, 0);
    assert_eq!(
        roots.gaps(addr(2)),
        vec![gap(0, 9)],
        "an unknown root: pull it all"
    );
}

#[test]
fn branching_is_even_and_within_the_ack_mask() {
    let k = |fanout| TreeParams::new(fanout).branching;
    assert_eq!(
        [0, 1, 2, 3, 4, 5, 7, 8, 16].map(k),
        [2, 2, 2, 2, 4, 4, 6, 8, 8]
    );
}
