//! The tree protocol end to end, on the simulated network.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;

use super::SplitMix64;
use super::thicket::{NONE, class_of, tree_of};
use crate::crdt::{BucketEpoch, NodeId, NodeIdentity};
use crate::defaults::THICKET_TREES;
use crate::discovery::{Peer, PeerEvent};
use crate::gossip::sim::{LinkPolicy, SimRouter, SimTransport, sim_advance_ticks};
use crate::gossip::tests::{
    InMemoryAggregateStore, key_hash, rule_fp, run_paused, sim_config, store_for_property_rules,
};
use crate::gossip::{GossipClient, GossipRuntime, TokioClock};
use crate::wire::{ControlEntry, ControlKind, PULL_ONLY_TREE, decode_control};

type Model = BTreeMap<(u128, u128, BucketEpoch), u64>;

/// `n` distinct addresses, spread over three /16s like three zones.
fn addrs(n: usize) -> Vec<SocketAddr> {
    (0..n)
        .map(|i| SocketAddr::from(([10, (i % 3) as u8, (i / 256) as u8, (i % 256) as u8], 7000)))
        .collect()
}

struct Node {
    client: GossipClient<u32>,
    agg: Rc<InMemoryAggregateStore<u32>>,
    peer_events: mpsc::UnboundedSender<PeerEvent>,
    handle: tokio::task::JoinHandle<Result<(), crate::gossip::GossipError>>,
}

fn spawn_node(
    router: &SimRouter,
    all: &[SocketAddr],
    index: usize,
    peers: &[SocketAddr],
    tick: Duration,
) -> Node {
    let identity = NodeIdentity::new(NodeId(0xA000 + index as u128), 1);
    let mut cfg = sim_config(identity, peers.to_vec(), 0x51 + index as u64);
    cfg.tick_interval = tick;
    cfg.send_queue_capacity = 256;
    let agg = Rc::new(InMemoryAggregateStore::<u32>::new());
    let (rt, client) = GossipRuntime::from_parts(
        router.bind(all[index]),
        TokioClock::from_millis(0),
        cfg,
        store_for_property_rules(identity, all.len() as u16 + 8),
        agg.clone(),
    );
    let (tx, rx) = mpsc::unbounded_channel();
    let stream =
        futures::stream::unfold(
            rx,
            |mut rx| async move { rx.recv().await.map(|evt| (evt, rx)) },
        )
        .boxed_local();
    Node {
        client,
        agg,
        peer_events: tx,
        handle: tokio::task::spawn_local(rt.run(stream)),
    }
}

async fn record(node: &Node, model: &mut Model, key: u8, hits: u64) {
    node.client
        .record(rule_fp(0), key_hash(key), 0, hits, 0, 0)
        .await
        .unwrap();
    *model.entry((rule_fp(0), key_hash(key).0, 0)).or_insert(0) += hits;
}

async fn run_for(millis: u64) {
    sim_advance_ticks(Duration::from_millis(5), (millis / 5) as u32).await;
}

async fn stop(nodes: Vec<Node>) {
    for node in &nodes {
        let _ = node.client.shutdown().await;
    }
    for node in nodes {
        let _ = node.handle.await;
    }
}

#[test]
fn trees_deliver_every_change_to_every_node_without_waiting_for_repair() {
    run_paused(async {
        const N: usize = 45;
        let all = addrs(N);
        let router = SimRouter::with_channel_capacity(1024);
        // A tick long enough that no heartbeat falls in the run: nothing but
        // the trees, the threshold rounds and the relays moves a cell.
        let tick = Duration::from_secs(3600);
        let nodes: Vec<Node> = (0..N)
            .map(|i| {
                let peers: Vec<_> = all.iter().copied().filter(|a| *a != all[i]).collect();
                spawn_node(&router, &all, i, &peers, tick)
            })
            .collect();

        run_for(50).await;
        let mut model = Model::new();
        for round in 0..6u8 {
            for (i, node) in nodes.iter().enumerate() {
                record(node, &mut model, (i as u8 + round) % 9, 3).await;
            }
            run_for(100).await;
        }
        run_for(1000).await;

        let behind: Vec<usize> = (0..N)
            .filter(|&i| nodes[i].agg.snapshot() != model)
            .collect();
        assert!(behind.is_empty(), "nodes without every change: {behind:?}");
        stop(nodes).await;
    });
}

#[test]
fn a_lost_eager_packet_is_asked_for_and_resent() {
    run_paused(async {
        // Two nodes of one class, so each is in the other's backbone view and
        // pushes its own changes to it directly.
        let all: Vec<SocketAddr> = addrs(60);
        let a = all[0];
        let b = *all[1..]
            .iter()
            .find(|x| class_of(x) == class_of(&a))
            .unwrap();
        let pair = [a, b];
        let router = SimRouter::with_channel_capacity(64);
        let tick = Duration::from_secs(3600);
        let node_a = spawn_node(&router, &pair, 0, &[b], tick);
        let node_b = spawn_node(&router, &pair, 1, &[a], tick);

        // Each record crosses the error budget and is sent at once. The first
        // packet is lost; the second shows B the gap. Let the start-up
        // traffic go by first.
        run_for(10).await;
        router.set_link_policy(a, b, LinkPolicy::DropFirst { count: 1 });
        let mut model = Model::new();
        record(&node_a, &mut model, 1, 5).await;
        run_for(50).await;
        assert!(
            node_b.agg.snapshot().is_empty(),
            "the first packet should have been lost: {:?}, classes {} {}",
            node_b.agg.snapshot(),
            class_of(&a),
            class_of(&b),
        );
        record(&node_a, &mut model, 2, 5).await;
        run_for(300).await;

        assert_eq!(node_b.agg.snapshot(), model);
        stop(vec![node_a, node_b]).await;
    });
}

#[test]
fn a_change_lost_below_the_high_water_mark_is_found_by_ihave() {
    run_paused(async {
        let all: Vec<SocketAddr> = addrs(60);
        let a = all[0];
        let b = *all[1..]
            .iter()
            .find(|x| class_of(x) == class_of(&a))
            .unwrap();
        let pair = [a, b];
        let router = SimRouter::with_channel_capacity(64);
        // Only A has a heartbeat, so only A announces.
        let node_a = spawn_node(&router, &pair, 0, &[b], Duration::from_millis(500));
        let node_b = spawn_node(&router, &pair, 1, &[a], Duration::from_secs(3600));
        run_for(10).await;

        // A's first packet to B is lost, and so are B's first three control
        // frames to A: the NACK that would ask for it, and its retries. A's
        // second change reaches B, which then holds a later change of A's
        // than the one it lacks.
        router.set_link_policy(a, b, LinkPolicy::DropFirst { count: 1 });
        router.set_link_policy(b, a, LinkPolicy::DropFirst { count: 3 });
        let mut model = Model::new();
        record(&node_a, &mut model, 1, 5).await;
        run_for(50).await;
        record(&node_a, &mut model, 2, 7).await;
        run_for(300).await;
        assert_eq!(
            node_b.agg.snapshot(),
            Model::from([((rule_fp(0), key_hash(2).0, 0), 7)]),
            "B should hold the later change and lack the earlier"
        );

        // A's next heartbeat names both changes. B misses one, waits out the
        // missing timeout, and asks for it.
        run_for(1500).await;
        assert_eq!(node_b.agg.snapshot(), model);
        stop(vec![node_a, node_b]).await;
    });
}

#[test]
fn a_node_that_joins_late_pulls_the_state_from_a_peer() {
    run_paused(async {
        const OLD: usize = 6;
        let all = addrs(OLD + 1);
        let router = SimRouter::with_channel_capacity(1024);
        let tick = Duration::from_secs(3600);
        let mut nodes: Vec<Node> = (0..OLD)
            .map(|i| {
                let peers: Vec<_> = all[..OLD]
                    .iter()
                    .copied()
                    .filter(|a| *a != all[i])
                    .collect();
                spawn_node(&router, &all, i, &peers, tick)
            })
            .collect();

        run_for(50).await;
        let mut model = Model::new();
        for (i, node) in nodes.iter().enumerate() {
            record(node, &mut model, i as u8, 4).await;
            run_for(10).await;
            record(node, &mut model, 20 + i as u8, 4).await;
            run_for(10).await;
        }
        run_for(500).await;

        // Discovery tells the newcomer of every member, and the members of
        // the newcomer. The newcomer's first request may reach a member
        // before discovery has told it of the newcomer, which ignores it, so
        // the newcomer asks again at its next heartbeat.
        let late = spawn_node(&router, &all, OLD, &[], Duration::from_millis(500));
        for member in &all[..OLD] {
            late.peer_events
                .send(PeerEvent::Added(Peer::new(*member)))
                .unwrap();
        }
        for node in &nodes {
            node.peer_events
                .send(PeerEvent::Added(Peer::new(all[OLD])))
                .unwrap();
        }
        run_for(2000).await;

        assert_eq!(late.agg.snapshot(), model);
        nodes.push(late);
        stop(nodes).await;
    });
}

#[test]
fn a_departed_parent_is_replaced_and_changes_keep_flowing() {
    run_paused(async {
        const N: usize = 12;
        let all = addrs(N);
        let router = SimRouter::with_channel_capacity(256);
        let tick = Duration::from_secs(3600);
        let nodes: Vec<Node> = (0..N)
            .map(|i| {
                let peers: Vec<_> = all.iter().copied().filter(|a| *a != all[i]).collect();
                spawn_node(&router, &all, i, &peers, tick)
            })
            .collect();
        // Let the trees form: every leaf attaches to its parents at start.
        run_for(50).await;
        let mut model = Model::new();
        record(&nodes[0], &mut model, 1, 5).await;
        run_for(500).await;

        // Node 5 leaves: discovery tells everyone else. Whoever had it as a
        // parent or backbone neighbour picks another.
        for (i, node) in nodes.iter().enumerate() {
            if i != 5 {
                node.peer_events
                    .send(PeerEvent::Removed(Peer::new(all[5])))
                    .unwrap();
            }
        }
        run_for(100).await;
        for (i, node) in nodes.iter().enumerate().filter(|(i, _)| *i != 5) {
            record(node, &mut model, 10 + i as u8, 2).await;
        }
        run_for(1500).await;

        for (i, node) in nodes.iter().enumerate().filter(|(i, _)| *i != 5) {
            assert_eq!(node.agg.snapshot(), model, "node {i}");
        }
        stop(nodes).await;
    });
}

type IdleRuntime = GossipRuntime<u32, Rc<InMemoryAggregateStore<u32>>, SimTransport, TokioClock>;

/// Node `index` of `all`, built but not running, so a test can drive its
/// handlers directly. It knows every other address and has attached to a
/// parent in each tree it is a leaf of.
fn idle_runtime(router: &SimRouter, all: &[SocketAddr], index: usize) -> IdleRuntime {
    let identity = NodeIdentity::new(NodeId(0xA000 + index as u128), 1);
    let peers: Vec<_> = all.iter().copied().filter(|a| *a != all[index]).collect();
    let (mut rt, _client) = GossipRuntime::from_parts(
        router.bind(all[index]),
        TokioClock::from_millis(0),
        sim_config(identity, peers, 0x51 + index as u64),
        store_for_property_rules(identity, all.len() as u16 + 8),
        Rc::new(InMemoryAggregateStore::<u32>::new()),
    );
    rt.maintain_structure();
    rt.send_pending.clear();
    rt
}

/// A change of another origin that travels in `tree`.
fn change_in(tree: u8) -> ControlEntry {
    let origin = NodeId(0xBEEF);
    let value = (1..).find(|&s| tree_of(origin, s) == tree).unwrap();
    ControlEntry {
        node_id: origin,
        incarnation: 1,
        value,
        rule_fingerprint: rule_fp(0),
        key_hash: key_hash(1),
        bucket: 0,
    }
}

/// The control frames `rt` has queued, as `(destination peer, kind, tree)`.
fn queued_control(rt: &IdleRuntime) -> Vec<(u16, ControlKind, u8)> {
    rt.send_pending
        .iter()
        .filter_map(|&(addr, slot)| {
            let bytes = rt.send_pool[usize::from(slot)].as_bytes();
            let frame = decode_control(bytes, None, rt.config.wire_limits, |_| {}).ok()?;
            Some((rt.thicket.index_of(addr)?, frame.kind, frame.tree))
        })
        .collect()
}

#[test]
fn a_leaf_regrafts_to_an_announcer_interior_in_the_tree_that_failed_it() {
    run_paused(async {
        let all = addrs(30);
        let router = SimRouter::with_channel_capacity(64);
        let mut rt = idle_runtime(&router, &all, 0);
        let tree = (rt.thicket.class + 1) % THICKET_TREES as u8;
        let old = rt.thicket.parents[usize::from(tree)];
        assert_ne!(old, NONE);
        let announcer = rt
            .thicket
            .random_of_class(tree, &mut SplitMix64::new(7), |i, _| i == old)
            .unwrap();

        rt.graft_for(announcer, &[change_in(tree)]);

        assert_eq!(rt.thicket.parents[usize::from(tree)], announcer);
        assert_eq!(
            queued_control(&rt),
            vec![
                (old, ControlKind::Prune, tree),
                (announcer, ControlKind::Graft, tree),
            ],
            "detach from the old parent, then attach and ask in one GRAFT"
        );
    });
}

#[test]
fn an_announcer_that_is_a_leaf_of_the_tree_is_only_asked_for_cells() {
    run_paused(async {
        let all = addrs(30);
        let router = SimRouter::with_channel_capacity(64);
        let mut rt = idle_runtime(&router, &all, 0);
        let class = rt.thicket.class;
        let tree = (class + 1) % THICKET_TREES as u8;
        let leaf_there = (class + 2) % THICKET_TREES as u8;
        let parents = rt.thicket.parents;
        let announcer = rt
            .thicket
            .random_of_class(leaf_there, &mut SplitMix64::new(7), |_, _| false)
            .unwrap();

        rt.graft_for(announcer, &[change_in(tree)]);

        assert_eq!(rt.thicket.parents, parents);
        assert_eq!(
            queued_control(&rt),
            vec![(announcer, ControlKind::Graft, PULL_ONLY_TREE)]
        );
    });
}

#[test]
fn a_missing_change_keeps_an_announcer_interior_in_its_tree() {
    run_paused(async {
        let all = addrs(30);
        let router = SimRouter::with_channel_capacity(64);
        let mut rt = idle_runtime(&router, &all, 0);
        let tree = (rt.thicket.class + 1) % THICKET_TREES as u8;
        let mut rng = SplitMix64::new(7);
        let outside = rt
            .thicket
            .random_of_class((tree + 1) % THICKET_TREES as u8, &mut rng, |_, _| false)
            .unwrap();
        let inside = rt
            .thicket
            .random_of_class(tree, &mut rng, |_, _| false)
            .unwrap();
        let change = change_in(tree);

        rt.on_ihave(outside, &[change], 0);
        rt.on_ihave(inside, &[change], 0);
        rt.on_ihave(outside, &[change], 0);

        assert_eq!(rt.thicket.missing.len(), 1);
        assert_eq!(rt.thicket.missing[0].announcer, inside);
    });
}
