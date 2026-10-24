use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use super::*;
use crate::crdt::{CellStoreConfig, KeyHash, NodeIdentity, RuleDescriptor};
use crate::discovery::{Peer as DiscoveredPeer, PeerEvent};
use crate::gossip::GossipConfig;
use crate::gossip::client::LimitRequest;
use crate::gossip::clock::PendingTicker;
use crate::gossip::runtime::SplitMix64;
use crate::gossip::sim::{SimRouter, SimTransport};
use crate::gossip::tests::InMemoryAggregateStore;
use crate::wire::FrameLimits;

// -- fixtures ---------------------------------------------------------------

/// A clock the test moves by hand, shared by every node of a scenario.
#[derive(Clone, Default)]
struct TestClock(Rc<Cell<u64>>);

impl TestClock {
    fn advance(&self, millis: u64) {
        self.0.set(self.0.get() + millis);
    }
}

impl Clock for TestClock {
    type Ticker = PendingTicker;

    fn now_millis(&self) -> u64 {
        self.0.get()
    }

    fn ticker(&self, _interval: Duration) -> PendingTicker {
        PendingTicker
    }
}

type Rt = GossipRuntime<u32, Rc<InMemoryAggregateStore<u32>>, SimTransport, TestClock>;

const RULE: u128 = 0xABCD;

fn addr(i: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 52_000 + i))
}

struct Node {
    rt: Rt,
    addr: SocketAddr,
}

/// Node `i`, peered with `peers`, on `router`. Nothing runs: tests call the
/// runtime's handlers and move packets between nodes themselves.
fn node(router: &SimRouter, clock: &TestClock, i: u16, peers: &[u16], cfg: PlumtreeConfig) -> Node {
    let identity = NodeIdentity::new(NodeId(0x7000 + u128::from(i)), 1);
    let config = CellStoreConfig {
        cell_capacity: 4_096,
        ..CellStoreConfig::default()
    };
    let mut store = CellStore::<u32>::new(config, identity);
    store.intern_rule(RuleDescriptor {
        fingerprint: RULE,
        window_millis: 600_000,
        bucket_millis: 60_000,
        limit: 1_000,
        flags: 0,
        local_rule_id: 0,
    });
    let config = GossipConfig {
        local_identity: identity,
        cluster_id_hash: 0xC1,
        bootstrap_peers: peers.iter().map(|&p| addr(p)).collect(),
        rng_seed: 0x5EED + u64::from(i),
        ..GossipConfig::default()
    };
    let (rt, _client) = GossipRuntime::from_parts(
        router.bind(addr(i)),
        clock.clone(),
        config,
        store,
        Rc::new(InMemoryAggregateStore::<u32>::new()),
    );
    Node {
        rt: rt.with_plumtree(cfg),
        addr: addr(i),
    }
}

/// `n` nodes that all know each other.
fn cluster(n: u16, cfg: PlumtreeConfig) -> (Vec<Node>, TestClock) {
    let router = SimRouter::new();
    let clock = TestClock::default();
    let all = (0..n)
        .map(|i| {
            let peers: Vec<u16> = (0..n).filter(|&p| p != i).collect();
            node(&router, &clock, i, &peers, cfg)
        })
        .collect();
    (all, clock)
}

fn no_pull() -> PlumtreeConfig {
    PlumtreeConfig {
        first_contact_pull: false,
        ..PlumtreeConfig::default()
    }
}

/// One local hit on `key`, as the limit-request arm handles it.
fn hit(rt: &mut Rt, key: u128) {
    hit_in(rt, key, 1);
}

fn hit_in(rt: &mut Rt, key: u128, bucket: u32) {
    rt.handle_limit_request(LimitRequest {
        rule_fingerprint: RULE,
        key_hash: KeyHash(key),
        bucket,
        hits: 1,
        rule_limit: 1_000,
        now_millis: rt.clock.now_millis(),
        reply: None,
    });
    rt.sink_buf.clear();
}

/// A packet taken off a node's send queue.
struct Packet {
    from: usize,
    to: Option<usize>,
    bytes: Vec<u8>,
}

impl Packet {
    fn kind(&self) -> FrameKind {
        wire::frame_kind(&self.bytes).expect("known frame kind")
    }

    fn control(&self) -> Option<ControlKind> {
        (self.kind() == FrameKind::Control).then(|| {
            wire::decode_control_unauth(&self.bytes, FrameLimits::default(), |_| {})
                .expect("control frame decodes")
                .1
        })
    }

    fn entries(&self) -> Vec<ControlEntry> {
        let mut out = Vec::new();
        wire::decode_control_unauth(&self.bytes, FrameLimits::default(), |e| out.push(e))
            .expect("control frame decodes");
        out
    }
}

/// Take every queued packet off every node, in node order.
fn outbox(nodes: &mut [Node]) -> Vec<Packet> {
    let mut out = Vec::new();
    for from in 0..nodes.len() {
        while let Some((dst, slot)) = nodes[from].rt.send_pending.pop_front() {
            let bytes = nodes[from].rt.send_pool[slot as usize].as_bytes().to_vec();
            nodes[from].rt.send_free.push(slot);
            let to = nodes.iter().position(|n| n.addr == dst);
            out.push(Packet { from, to, bytes });
        }
    }
    out
}

fn receive(nodes: &mut [Node], packet: &Packet) {
    let Some(to) = packet.to else {
        return;
    };
    let from = nodes[packet.from].addr;
    let rt = &mut nodes[to].rt;
    rt.recv_buf[..packet.bytes.len()].copy_from_slice(&packet.bytes);
    rt.handle_inbound(packet.bytes.len(), from);
    rt.sink_buf.clear();
    rt.store.clear_dirty();
}

/// Deliver packets, and the packets they provoke, until the network is quiet;
/// `keep` chooses which packets survive. Returns everything that was sent.
fn settle(nodes: &mut [Node], mut keep: impl FnMut(&Packet) -> bool) -> Vec<Packet> {
    let mut log = Vec::new();
    for _ in 0..50 {
        let batch = outbox(nodes);
        if batch.is_empty() {
            break;
        }
        for packet in batch {
            if keep(&packet) {
                receive(nodes, &packet);
            }
            log.push(packet);
        }
    }
    log
}

fn count(log: &[Packet], control: ControlKind) -> usize {
    log.iter().filter(|p| p.control() == Some(control)).count()
}

fn heartbeat(node: &mut Node) {
    node.rt.handle_gossip_tick(Round::Heartbeat);
}

/// Let the relay floor pass and run each node's due relay round.
fn relay(nodes: &mut [Node], clock: &TestClock) {
    clock.advance(5);
    for n in nodes.iter_mut() {
        n.rt.handle_wake();
    }
}

fn slot_of(n: &Node, origin: &Node) -> NodeSlot {
    let id = origin.rt.store.local_identity();
    n.rt.store
        .find_node(id.node_id, id.incarnation)
        .expect("origin known")
}

/// Is the peer at `peer` lazy for `origin`'s root at `n`?
fn is_lazy(n: &mut Node, origin: NodeSlot, peer: SocketAddr) -> bool {
    let pos =
        n.rt.peers
            .iter()
            .find(|p| p.addr == peer)
            .expect("peer known")
            .view_pos;
    assert_ne!(pos, NO_POS, "peer is in the view");
    *n.rt.pt.lazy_mut(n.rt.store.node_dictionary(), origin) & (1 << pos) != 0
}

// -- active view ------------------------------------------------------------

#[test]
fn view_holds_coverage_many_peers_and_samples_them_uniformly() {
    const PEERS: u16 = 20;
    const SEEDS: u64 = 400;
    let router = SimRouter::new();
    let clock = TestClock::default();
    let expected = ((f64::from(PEERS)).ln() + defaults::GOSSIP_COVERAGE_MARGIN).ceil() as usize;
    let mut seen = [0u32; PEERS as usize];
    for seed in 0..SEEDS {
        let peers: Vec<u16> = (1..=PEERS).collect();
        let mut n = node(&router, &clock, 0, &peers, PlumtreeConfig::default());
        // The peers arrive one by one, as discovery events deliver them.
        n.rt.rng = SplitMix64::new(seed);
        n.rt.pt.view = [None; VIEW_MAX];
        n.rt.pt.occupied = 0;
        for p in n.rt.peers.iter_mut() {
            p.view_pos = NO_POS;
        }
        for i in 0..n.rt.peers.len() {
            n.rt.view_consider(i, i + 1);
        }
        assert_eq!(n.rt.pt.view_len(), expected);
        for (i, p) in n.rt.peers.iter().enumerate() {
            seen[i] += u32::from(p.view_pos != NO_POS);
        }
    }
    // Each peer is in the view with probability `expected / PEERS`; allow five
    // standard deviations of the binomial.
    let p = expected as f64 / f64::from(PEERS);
    let mean = SEEDS as f64 * p;
    let sigma = (SEEDS as f64 * p * (1.0 - p)).sqrt();
    for (i, &count) in seen.iter().enumerate() {
        assert!(
            (f64::from(count) - mean).abs() < 5.0 * sigma,
            "peer {i} was in the view {count} of {SEEDS} times, expected about {mean}"
        );
    }
}

#[test]
fn a_departing_view_member_is_replaced_from_outside_the_view() {
    let router = SimRouter::new();
    let clock = TestClock::default();
    let peers: Vec<u16> = (1..=20).collect();
    let mut n = node(&router, &clock, 0, &peers, PlumtreeConfig::default());
    let size = n.rt.pt.view_len();
    let member =
        n.rt.peers
            .iter()
            .find(|p| p.view_pos != NO_POS)
            .map(|p| p.addr)
            .expect("a view member");
    n.rt.handle_peer_event(PeerEvent::Removed(DiscoveredPeer::new(member)));
    assert!(n.rt.pt.view.iter().flatten().all(|a| *a != member));
    assert_eq!(n.rt.pt.view_len(), size);
}

// -- trees: PRUNE -----------------------------------------------------------

#[test]
fn a_duplicate_eager_delivery_prunes_the_sender_for_that_root_only() {
    let (mut all, clock) = cluster(3, no_pull());
    hit(&mut all[0].rt, 1);
    heartbeat(&mut all[0]);
    settle(&mut all, |p| p.kind() == FrameKind::Eager);
    // B and C each got the change from A and relay it to each other: each
    // receives a duplicate and prunes the other.
    relay(&mut all, &clock);
    let log = settle(&mut all, |_| true);
    assert_eq!(count(&log, ControlKind::Prune), 2);
    let a_root = slot_of(&all[1], &all[0]);
    let (b, c) = (all[1].addr, all[2].addr);
    assert!(is_lazy(&mut all[1], a_root, c));
    assert!(is_lazy(&mut all[2], a_root, b));

    // The prune is for A's root: B's own root still pushes to C.
    hit(&mut all[1].rt, 2);
    heartbeat(&mut all[1]);
    let log = settle(&mut all, |_| true);
    assert!(
        log.iter()
            .any(|p| p.from == 1 && p.to == Some(2) && p.kind() == FrameKind::Eager)
    );
}

#[test]
fn a_relay_round_carries_the_local_changes_queued_with_it() {
    let (mut all, clock) = cluster(3, no_pull());
    hit(&mut all[0].rt, 1);
    heartbeat(&mut all[0]);
    settle(&mut all, |p| p.kind() == FrameKind::Eager);

    // B relays A's change in a relay round; its own hit, queued meanwhile,
    // goes with it instead of waiting for B's heartbeat.
    hit(&mut all[1].rt, 2);
    relay(&mut all, &clock);
    settle(&mut all, |_| true);
    let id = all[1].rt.store.local_identity();
    assert!(
        all[2]
            .rt
            .store
            .find_node(id.node_id, id.incarnation)
            .is_some()
    );
}

#[test]
fn a_pruned_edge_carries_no_cells_for_that_root() {
    let (mut all, clock) = cluster(3, no_pull());
    hit(&mut all[0].rt, 1);
    heartbeat(&mut all[0]);
    settle(&mut all, |p| p.kind() == FrameKind::Eager);
    relay(&mut all, &clock);
    settle(&mut all, |_| true);

    hit(&mut all[0].rt, 2);
    heartbeat(&mut all[0]);
    settle(&mut all, |p| p.kind() == FrameKind::Eager);
    relay(&mut all, &clock);
    let log = settle(&mut all, |_| true);
    let between_b_and_c = log
        .iter()
        .filter(|p| matches!((p.from, p.to), (1, Some(2)) | (2, Some(1))))
        .filter(|p| p.kind() == FrameKind::Eager)
        .count();
    assert_eq!(between_b_and_c, 0);
}

#[test]
fn a_version_that_arrived_off_the_tree_is_forwarded_on_its_first_tree_delivery() {
    for off_tree in [FrameKind::Plain, FrameKind::Random] {
        let (mut all, _clock) = cluster(2, no_pull());
        hit(&mut all[0].rt, 1);
        let handle = all[0].rt.store.active_handles().next().expect("a cell");
        // The cell reaches B off the tree first: kept, not forwarded.
        all[0].rt.frame_handles.clear();
        all[0].rt.frame_handles.push(handle);
        let b = all[1].addr;
        all[0].rt.send_cells(b, off_tree, Numbering::None);
        settle(&mut all, |_| true);
        assert_eq!(all[1].rt.store.active_len(), 1);
        assert_eq!(all[1].rt.pt.queued_relay(), 0);

        // Then the tree delivers it: a first delivery, so it is forwarded and
        // the sender is not pruned.
        all[0].rt.handle_gossip_tick(Round::Threshold);
        let log = settle(&mut all, |p| p.kind() == FrameKind::Eager);
        assert_eq!(count(&log, ControlKind::Prune), 0, "{off_tree:?}");
        assert_eq!(all[1].rt.pt.queued_relay(), 1, "{off_tree:?}");
    }
}

#[test]
fn an_origin_whose_cells_expired_comes_back_to_its_pruned_tree() {
    let (mut all, clock) = cluster(3, no_pull());
    hit(&mut all[0].rt, 1);
    heartbeat(&mut all[0]);
    settle(&mut all, |p| p.kind() == FrameKind::Eager);
    relay(&mut all, &clock);
    settle(&mut all, |_| true);
    let c = all[2].addr;
    let a_root = slot_of(&all[1], &all[0]);
    assert!(is_lazy(&mut all[1], a_root, c));

    // A goes quiet until its cells expire at B, which frees A's node slot;
    // C's next change takes that slot.
    clock.advance(12 * 60_000);
    heartbeat(&mut all[1]);
    settle(&mut all, |_| false);
    assert_eq!(all[1].rt.store.active_len(), 0);
    let bucket = (clock.now_millis() / 60_000) as u32;
    hit_in(&mut all[2].rt, 2, bucket);
    all[2].rt.handle_gossip_tick(Round::Threshold);
    settle(&mut all, |p| {
        p.kind() == FrameKind::Eager && p.to == Some(1)
    });
    assert_eq!(slot_of(&all[1], &all[2]), a_root, "C took A's slot");

    // A's next change finds B's edge to C still lazy for A's root.
    hit_in(&mut all[0].rt, 3, bucket);
    all[0].rt.handle_gossip_tick(Round::Threshold);
    settle(&mut all, |p| {
        p.kind() == FrameKind::Eager && p.to == Some(1)
    });
    let a_root = slot_of(&all[1], &all[0]);
    assert!(is_lazy(&mut all[1], a_root, c));
}

// -- trees: IHAVE and GRAFT ---------------------------------------------------

/// Three nodes where B and C have pruned each other for A's root, then A hits
/// a key whose push to C is lost. Returns the nodes and the clock, with C
/// still missing the change.
fn pruned_with_change_missing_at_c() -> (Vec<Node>, TestClock) {
    let (mut all, clock) = cluster(3, no_pull());
    hit(&mut all[0].rt, 1);
    heartbeat(&mut all[0]);
    settle(&mut all, |p| p.kind() == FrameKind::Eager);
    relay(&mut all, &clock);
    settle(&mut all, |_| true);

    hit(&mut all[0].rt, 2);
    heartbeat(&mut all[0]);
    // A's push to C is lost, and no repair digest runs.
    settle(&mut all, |p| {
        p.kind() == FrameKind::Eager && p.to != Some(2)
    });
    relay(&mut all, &clock);
    settle(&mut all, |p| p.kind() == FrameKind::Eager);
    assert_eq!(all[1].rt.store.active_len(), 2);
    assert_eq!(all[2].rt.store.active_len(), 1);
    (all, clock)
}

#[test]
fn a_lazy_peer_announces_what_the_tree_did_not_deliver() {
    let (mut all, _clock) = pruned_with_change_missing_at_c();
    heartbeat(&mut all[1]);
    let log = settle(&mut all, |p| p.control() == Some(ControlKind::IHave));
    let ihave = log
        .iter()
        .find(|p| p.control() == Some(ControlKind::IHave))
        .expect("B announces to C");
    assert_eq!((ihave.from, ihave.to), (1, Some(2)));
    let a = all[0].rt.store.local_identity();
    let entry = ihave.entries()[0];
    assert_eq!(
        (entry.node_id, entry.incarnation),
        (a.node_id, a.incarnation)
    );
    assert_eq!(
        entry.sequence,
        all[1].rt.store.origin_watermark(slot_of(&all[1], &all[0]))
    );
    assert_eq!(all[2].rt.pt.missing.len(), 1);
}

#[test]
fn graft_waits_for_the_missing_timeout_then_repairs_and_makes_the_edge_eager() {
    let (mut all, clock) = pruned_with_change_missing_at_c();
    heartbeat(&mut all[1]);
    settle(&mut all, |p| p.control() == Some(ControlKind::IHave));
    let timeout = all[2].rt.pt.cfg.missing_timeout.as_millis() as u64;

    clock.advance(timeout - 1);
    all[2].rt.handle_wake();
    assert_eq!(count(&outbox(&mut all), ControlKind::Graft), 0);

    clock.advance(1);
    all[2].rt.handle_wake();
    let log = settle(&mut all, |_| true);
    assert_eq!(count(&log, ControlKind::Graft), 1);
    assert_eq!(all[2].rt.store.active_len(), 2, "the GRAFT reply delivers");
    // C forwards what the GRAFT reply brought.
    assert_eq!(all[2].rt.pt.queued_relay(), 1);
    // The satisfied announcement is dropped at the next timer, with no
    // second GRAFT.
    clock.advance(timeout);
    all[2].rt.handle_wake();
    assert!(all[2].rt.pt.missing.is_empty());
    assert_eq!(count(&outbox(&mut all), ControlKind::Graft), 0);
    // B made the edge to C eager again for A's root.
    let a_root = slot_of(&all[1], &all[0]);
    let c = all[2].addr;
    assert!(!is_lazy(&mut all[1], a_root, c));
}

#[test]
fn a_change_the_tree_delivers_in_time_cancels_the_graft() {
    let (mut all, clock) = pruned_with_change_missing_at_c();
    heartbeat(&mut all[1]);
    settle(&mut all, |p| p.control() == Some(ControlKind::IHave));

    // The tree catches up before the timeout: A's push arrives late.
    heartbeat(&mut all[0]);
    hit(&mut all[0].rt, 3);
    heartbeat(&mut all[0]);
    settle(&mut all, |p| p.kind() == FrameKind::Eager);
    clock.advance(all[2].rt.pt.cfg.missing_timeout.as_millis() as u64);
    all[2].rt.handle_wake();
    let log = settle(&mut all, |p| p.kind() != FrameKind::Plain);
    assert_eq!(count(&log, ControlKind::Graft), 0);
    assert!(all[2].rt.pt.missing.is_empty());
}

/// Deliver to `to` an IHAVE from `from` announcing `origin`'s changes up to
/// `sequence`.
fn announce(all: &mut [Node], from: usize, to: usize, origin: usize, sequence: u64) {
    let o = all[origin].rt.store.local_identity();
    let entry = ControlEntry {
        node_id: o.node_id,
        incarnation: o.incarnation,
        sequence,
    };
    let mut buf = crate::wire::PacketBuf::for_limits(FrameLimits::default());
    let header = all[from].rt.header(0);
    wire::encode_control(
        &mut buf,
        header,
        ControlKind::IHave,
        &[entry],
        None,
        FrameLimits::default(),
    );
    let packet = Packet {
        from,
        to: Some(to),
        bytes: buf.as_bytes().to_vec(),
    };
    receive(all, &packet);
}

#[test]
fn a_version_that_arrived_off_the_tree_does_not_hide_a_missing_tree_delivery() {
    let (mut all, clock) = pruned_with_change_missing_at_c();
    // The change reaches C off the tree, as a repair or a pull would bring it.
    let handle = all[0]
        .rt
        .store
        .active_handles()
        .find(|&h| {
            all[0]
                .rt
                .store
                .get(h)
                .is_some_and(|r| r.key.key_hash == KeyHash(2))
        })
        .expect("A holds key 2");
    all[0].rt.frame_handles.clear();
    all[0].rt.frame_handles.push(handle);
    let c = all[2].addr;
    all[0].rt.send_cells(c, FrameKind::Plain, Numbering::None);
    settle(&mut all, |_| true);
    assert_eq!(all[2].rt.store.active_len(), 2);

    // B's announcement still finds the tree behind at C, so C grafts.
    heartbeat(&mut all[1]);
    settle(&mut all, |p| p.control() == Some(ControlKind::IHave));
    assert_eq!(all[2].rt.pt.missing.len(), 1);
    clock.advance(all[2].rt.pt.cfg.missing_timeout.as_millis() as u64);
    all[2].rt.handle_wake();
    let log = settle(&mut all, |_| true);
    assert_eq!(count(&log, ControlKind::Graft), 1);
    let a_root = slot_of(&all[1], &all[0]);
    assert!(
        !is_lazy(&mut all[1], a_root, c),
        "B's edge to C is eager again"
    );
}

#[test]
fn graft_retries_go_to_the_next_announcer() {
    let (mut all, clock) = cluster(4, no_pull());
    hit(&mut all[0].rt, 1);
    let seq = all[0]
        .rt
        .store
        .origin_watermark(all[0].rt.store.local_node_slot());
    // D hears of A's change from B, then from C, and the tree never delivers.
    announce(&mut all, 1, 3, 0, seq);
    clock.advance(100);
    announce(&mut all, 2, 3, 0, seq);
    let timeout = all[3].rt.pt.cfg.missing_timeout.as_millis() as u64;
    let mut grafted = Vec::new();
    for _ in 0..usize::from(defaults::PLUMTREE_MAX_GRAFT_TRIES) + 1 {
        clock.advance(timeout);
        all[3].rt.handle_wake();
        for p in outbox(&mut all) {
            if p.control() == Some(ControlKind::Graft) {
                grafted.push(p.to.expect("a node"));
            }
        }
    }
    assert_eq!(grafted, [1, 2, 1], "B, then C, then B again, then give up");
    assert!(all[3].rt.pt.missing.is_empty());
}

#[test]
fn a_graft_waits_for_its_own_announcement_to_time_out() {
    let (mut all, clock) = cluster(3, no_pull());
    hit(&mut all[0].rt, 1);
    let seq = all[0]
        .rt
        .store
        .origin_watermark(all[0].rt.store.local_node_slot());
    let timeout = all[2].rt.pt.cfg.missing_timeout.as_millis() as u64;
    announce(&mut all, 1, 2, 0, seq);
    // The announced change arrives on the tree in time.
    all[0].rt.handle_gossip_tick(Round::Threshold);
    settle(&mut all, |p| {
        p.kind() == FrameKind::Eager && p.to == Some(2)
    });
    hit(&mut all[0].rt, 2);
    clock.advance(timeout - 100);
    // A newer change is announced just before the first timer expires.
    announce(&mut all, 1, 2, 0, seq + 1);
    clock.advance(100);
    all[2].rt.handle_wake();
    assert_eq!(
        count(&outbox(&mut all), ControlKind::Graft),
        0,
        "the newer timer still runs"
    );
    clock.advance(timeout - 100);
    all[2].rt.handle_wake();
    assert_eq!(count(&outbox(&mut all), ControlKind::Graft), 1);
}

#[test]
fn an_announcement_for_an_unknown_origin_pulls_at_once_or_grafts_after_the_timeout() {
    let ihave = |all: &mut Vec<Node>| {
        let a = all[0].rt.store.local_identity();
        all[1].rt.pt.entries.clear();
        let entry = ControlEntry {
            node_id: a.node_id,
            incarnation: a.incarnation,
            sequence: 7,
        };
        let mut buf = crate::wire::PacketBuf::for_limits(FrameLimits::default());
        let header = all[0].rt.header(0);
        wire::encode_control(
            &mut buf,
            header,
            ControlKind::IHave,
            &[entry],
            None,
            FrameLimits::default(),
        );
        let packet = Packet {
            from: 0,
            to: Some(1),
            bytes: buf.as_bytes().to_vec(),
        };
        receive(all, &packet);
    };

    let (mut all, clock) = cluster(2, PlumtreeConfig::default());
    ihave(&mut all);
    let sent = outbox(&mut all);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].control(), Some(ControlKind::Pull));
    assert!(all[1].rt.pt.missing.is_empty());

    let (mut all, clock2) = cluster(2, no_pull());
    ihave(&mut all);
    assert!(outbox(&mut all).is_empty());
    assert_eq!(all[1].rt.pt.missing.len(), 1);
    clock2.advance(all[1].rt.pt.cfg.missing_timeout.as_millis() as u64);
    all[1].rt.handle_wake();
    let sent = outbox(&mut all);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].control(), Some(ControlKind::Graft));
    let _ = clock;
}

#[test]
fn first_contact_with_an_origin_pulls_its_older_cells() {
    let (mut all, _clock) = cluster(2, PlumtreeConfig::default());
    for key in 1..=5 {
        hit(&mut all[0].rt, key);
    }
    // Everything A pushes now is lost: B never hears of keys 1..=5.
    heartbeat(&mut all[0]);
    settle(&mut all, |_| false);
    hit(&mut all[0].rt, 6);
    all[0].rt.handle_gossip_tick(Round::Threshold);
    // Only the eager push of key 6 arrives.
    let batch = outbox(&mut all);
    for packet in batch.iter().filter(|p| p.kind() == FrameKind::Eager) {
        receive(&mut all, packet);
    }
    assert_eq!(all[1].rt.store.active_len(), 1);
    let log = settle(&mut all, |_| true);
    assert_eq!(count(&log, ControlKind::Pull), 1);
    assert_eq!(all[1].rt.store.active_len(), 6);
}

// -- repair digest
// ----------------------------------------------------------------

#[test]
fn a_digest_counts_cells_settled_and_still_live() {
    let rule = RuleDescriptor {
        fingerprint: RULE,
        window_millis: 10_000,
        bucket_millis: 1_000,
        limit: 100,
        flags: 0,
        local_rule_id: 0,
    };
    // At 20.5 s bucket 20 is open and bucket 19 closed only 0.5 s ago; bucket
    // 10 is the oldest the store still keeps.
    assert_eq!(settled(&rule, 20_500), Some(Settled::Buckets(10, 18)));
    assert_eq!(settled(&rule, 21_000), Some(Settled::Buckets(11, 19)));
    assert_eq!(settled(&rule, 1_500), None, "nothing has settled yet");
    // A rule without buckets never expires: a cell counts once it has gone
    // a settle time without changing.
    let no_buckets = RuleDescriptor {
        bucket_millis: 0,
        ..rule
    };
    let cutoff = settled(&no_buckets, 20_500).expect("settled");
    assert_eq!(cutoff, Settled::UpdatedBy(19_500));
    assert!(cutoff.covers(0, 19_500) && !cutoff.covers(0, 19_501));
}

/// The fixture rule has one-minute buckets and `hit` lands in bucket 1, which
/// has settled at this instant.
const SETTLED: u64 = 121_000;

#[test]
fn a_digest_pulls_a_hole_the_tree_left_behind() {
    let (mut all, clock) = cluster(2, no_pull());
    hit(&mut all[0].rt, 1);
    all[0].rt.handle_gossip_tick(Round::Threshold);
    settle(&mut all, |_| true);
    hit(&mut all[0].rt, 2);
    hit(&mut all[0].rt, 3);
    all[0].rt.handle_gossip_tick(Round::Threshold);
    // The push of keys 2 and 3 is lost, and nothing on the edge follows it, so
    // there is no gap to NACK and the watermark has nothing to announce.
    settle(&mut all, |_| false);
    assert_eq!(all[1].rt.store.active_len(), 1);
    let relayed = all[1].rt.pt.queued_relay();

    clock.advance(SETTLED);
    heartbeat(&mut all[0]);
    let log = settle(&mut all, |p| {
        p.kind() != FrameKind::Control || p.control() != Some(ControlKind::IHave)
    });
    assert_eq!(count(&log, ControlKind::Digest), 1);
    assert_eq!(count(&log, ControlKind::Pull), 1);
    assert_eq!(all[1].rt.store.active_len(), 3);
    let pulled = log.iter().filter(|p| p.kind() == FrameKind::Plain).count();
    assert_eq!(pulled, 1);
    assert_eq!(
        all[1].rt.pt.queued_relay(),
        relayed,
        "the reply is kept off the tree"
    );
}

#[test]
fn settled_sums_follow_merges_without_another_pass_over_the_store() {
    let (mut all, clock) = cluster(2, no_pull());
    hit(&mut all[0].rt, 1);
    all[0].rt.handle_gossip_tick(Round::Threshold);
    settle(&mut all, |_| true);
    clock.advance(SETTLED);
    let at = clock.now_millis();
    all[1].rt.settled_sums(at);
    let a = slot_of(&all[1], &all[0]);
    assert_eq!(all[1].rt.pt.sums[a as usize], 1);

    // Late changes to the settled bucket arrive: the sums follow them.
    for key in [1, 2, 2] {
        hit(&mut all[0].rt, key);
        all[0].rt.handle_gossip_tick(Round::Threshold);
        settle(&mut all, |_| true);
    }
    let tracked = all[1].rt.pt.sums[a as usize];
    all[1].rt.pt.sums_at = None;
    all[1].rt.settled_sums(at);
    assert_eq!(tracked, all[1].rt.pt.sums[a as usize]);
    assert_eq!(tracked, 4);
}

#[test]
fn equal_settled_counts_pull_nothing() {
    let (mut all, clock) = cluster(2, no_pull());
    for key in 1..=3 {
        hit(&mut all[0].rt, key);
    }
    all[0].rt.handle_gossip_tick(Round::Threshold);
    settle(&mut all, |_| true);
    clock.advance(SETTLED);
    heartbeat(&mut all[0]);
    let log = settle(&mut all, |_| true);
    assert_eq!(count(&log, ControlKind::Digest), 1);
    assert_eq!(count(&log, ControlKind::Pull), 0);
}

#[test]
fn a_reply_stops_at_its_cap_and_the_next_one_picks_up_after_it() {
    let (mut all, _clock) = cluster(2, no_pull());
    let keys = defaults::PLUMTREE_REPLY_CELLS + 10;
    for key in 0..keys as u128 {
        hit(&mut all[0].rt, key);
    }
    let a = all[0].rt.store.local_identity();
    let mut held = Vec::new();
    while held.last() != Some(&keys) && held.len() < 1_000 {
        all[0].rt.pt.inbound.clear();
        all[0].rt.pt.inbound.push(ControlEntry {
            node_id: a.node_id,
            incarnation: a.incarnation,
            sequence: 0,
        });
        let b = all[1].addr;
        all[0].rt.reply_with_cells(b, FrameKind::Plain);
        settle(&mut all, |_| true);
        held.push(all[1].rt.store.active_len() as usize);
    }
    assert!(held[0] <= defaults::PLUMTREE_REPLY_CELLS, "{held:?}");
    assert!(
        held.windows(2).all(|w| w[0] < w[1]),
        "every reply adds cells: {held:?}"
    );
    assert_eq!(held.last(), Some(&keys));
}

#[test]
fn a_frame_the_full_send_queue_cuts_short_is_counted() {
    let (mut all, _clock) = cluster(2, no_pull());
    hit(&mut all[0].rt, 1);
    let handle = all[0].rt.store.active_handles().next().expect("a cell");
    let b = all[1].addr;
    let free = std::mem::take(&mut all[0].rt.send_free);
    all[0].rt.frame_handles.clear();
    all[0].rt.frame_handles.push(handle);
    assert_eq!(
        all[0].rt.send_cells(b, FrameKind::Plain, Numbering::None),
        0
    );
    all[0].rt.send_control(b, ControlKind::Prune, (0, 0), &[]);
    assert_eq!(all[0].rt.pt.send_cut_total, 2);
    all[0].rt.send_free = free;
    assert_eq!(
        all[0].rt.send_cells(b, FrameKind::Plain, Numbering::None),
        1
    );
    assert_eq!(all[0].rt.pt.send_cut_total, 2);
}

// -- UDP adaptation: edge numbers, NACK, resend -------------------------------

#[test]
fn a_lost_eager_packet_is_nacked_and_resent_under_its_number() {
    let (mut all, clock) = cluster(2, no_pull());
    for key in 10..13 {
        hit(&mut all[0].rt, key);
        all[0].rt.handle_gossip_tick(Round::Threshold);
        let batch = outbox(&mut all);
        assert_eq!(batch.len(), 1, "one eager packet per round");
        assert_eq!(batch[0].kind(), FrameKind::Eager);
        // The second packet is lost.
        if key != 11 {
            receive(&mut all, &batch[0]);
        }
    }
    assert_eq!(all[1].rt.store.active_len(), 2);
    let a = all[0].addr;
    let missing = |all: &[Node]| {
        all[1]
            .rt
            .peers
            .iter()
            .find(|p| p.addr == a)
            .unwrap()
            .edge
            .missing()
    };
    // Packets 1 and 3 arrived, so number 2 is the one missing.
    assert_eq!(missing(&all), (0, 0b100));

    // The NACK waits one relay floor, then asks A for packet 2.
    all[1].rt.handle_wake();
    assert!(outbox(&mut all).is_empty(), "no NACK before the grace");
    clock.advance(5);
    all[1].rt.handle_wake();
    let log = settle(&mut all, |_| true);
    assert_eq!(count(&log, ControlKind::Nack), 1);
    let resent = log.iter().filter(|p| p.kind() == FrameKind::Resent).count();
    assert_eq!(resent, 1);
    assert_eq!(all[1].rt.store.active_len(), 3);
    assert_eq!(missing(&all).1, 0);
}

#[test]
fn the_receive_window_tracks_numbers_across_reordering_and_long_gaps() {
    let mut edge = Edge::default();
    assert!(!edge.arrived(10, 1), "the first packet opens no gap");
    assert!(!edge.arrived(11, 1));
    assert!(edge.arrived(14, 1), "12 and 13 are skipped");
    assert_eq!(edge.missing(), (0, (1 << 12) | (1 << 13)));
    assert!(!edge.arrived(13, 1), "a late packet fills its place");
    assert_eq!(edge.missing(), (0, 1 << 12));
    assert!(!edge.arrived(13, 1), "a repeat changes nothing");
    // A jump within the window opens a gap (15 to 75; 12 has left the
    // window); a jump past it is an outage, and the edge starts afresh.
    assert!(edge.arrived(76, 1));
    assert_eq!(edge.missing(), (13, ((1u64 << 61) - 1) << 2));
    assert!(!edge.arrived(140, 1));
    assert_eq!(edge.missing(), (77, 0));
}

#[test]
fn a_nack_for_a_forgotten_packet_goes_unanswered() {
    let (mut all, _clock) = cluster(2, no_pull());
    let b = all[1].addr;
    all[0].rt.resend(b, 99, 99);
    assert!(outbox(&mut all).is_empty());
}

// -- hybrid ---------------------------------------------------------------------

#[test]
fn random_pushes_go_only_to_peers_off_the_roots_tree() {
    let (mut all, _clock) = cluster(
        2,
        PlumtreeConfig {
            extra_random: 1,
            ..no_pull()
        },
    );
    for key in 0..20 {
        hit(&mut all[0].rt, key);
        all[0].rt.handle_gossip_tick(Round::Threshold);
    }
    // The only peer is on the tree, so no random push has anyone to go to.
    let log = outbox(&mut all);
    assert!(log.iter().all(|p| p.kind() != FrameKind::Random));

    // With peers outside the 5-peer view, some rounds do push at random.
    let router = SimRouter::new();
    let clock = TestClock::default();
    let peers: Vec<u16> = (1..=6).collect();
    let mut n = node(
        &router,
        &clock,
        0,
        &peers,
        PlumtreeConfig {
            extra_random: 1,
            ..no_pull()
        },
    );
    let mut random_dsts = Vec::new();
    for key in 0..200 {
        hit(&mut n.rt, key);
        n.rt.handle_gossip_tick(Round::Threshold);
        while let Some((dst, slot)) = n.rt.send_pending.pop_front() {
            if wire::frame_kind(n.rt.send_pool[slot as usize].as_bytes()) == Some(FrameKind::Random)
            {
                random_dsts.push(dst);
            }
            n.rt.send_free.push(slot);
        }
    }
    assert!(!random_dsts.is_empty());
    for dst in random_dsts {
        let p = n.rt.peers.iter().find(|p| p.addr == dst).expect("a peer");
        assert_eq!(p.view_pos, NO_POS, "{dst} is on the tree");
    }
}

// -- whole cluster
// -----------------------------------------------------------------

#[test]
fn twelve_nodes_converge_through_pruned_trees() {
    let (mut all, clock) = cluster(12, PlumtreeConfig::default());
    let n = all.len();
    for step in 0..400u64 {
        let origin = (step as usize * 7) % n;
        hit(&mut all[origin].rt, u128::from(step % 9));
        for node in all.iter_mut() {
            node.rt.handle_gossip_tick(Round::Threshold);
        }
        clock.advance(5);
        for node in all.iter_mut() {
            node.rt.handle_wake();
            if step % 20 == 19 {
                heartbeat(node);
            }
        }
        settle(&mut all, |_| true);
    }
    for _ in 0..20 {
        clock.advance(100);
        for node in all.iter_mut() {
            heartbeat(node);
        }
        settle(&mut all, |_| true);
    }
    let totals = |node: &Node| -> Vec<(u128, u64)> {
        let mut by_key = std::collections::BTreeMap::<u128, u64>::new();
        for h in node.rt.store.active_handles() {
            let row = node.rt.store.get(h).expect("live");
            *by_key.entry(row.key.key_hash.0).or_default() += u64::from(row.count);
        }
        by_key.into_iter().collect()
    };
    let want = totals(&all[0]);
    assert_eq!(want.iter().map(|(_, c)| c).sum::<u64>(), 400);
    for node in &all {
        assert_eq!(totals(node), want);
    }
    // Trees formed: some node pruned some edge for some root.
    let pruned = all
        .iter_mut()
        .any(|n| (0..n.rt.pt.roots.len()).any(|s| n.rt.pt.roots[s].lazy != 0));
    assert!(pruned);
}
