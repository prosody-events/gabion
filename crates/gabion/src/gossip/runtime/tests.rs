//! The tree protocol end to end, on the simulated network.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::mpsc;

use super::thicket::class_of;
use crate::crdt::{BucketEpoch, NodeId, NodeIdentity};
use crate::discovery::{Peer, PeerEvent};
use crate::gossip::sim::{LinkPolicy, SimRouter, sim_advance_ticks};
use crate::gossip::tests::{
    InMemoryAggregateStore, key_hash, rule_fp, run_paused, sim_config, store_for_property_rules,
};
use crate::gossip::{GossipClient, GossipRuntime, TokioClock};

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
        router.set_link_policy(a, b, LinkPolicy::DropFirst { count: 1 });
        let tick = Duration::from_secs(3600);
        let node_a = spawn_node(&router, &pair, 0, &[b], tick);
        let node_b = spawn_node(&router, &pair, 1, &[a], tick);

        // Each record crosses the error budget and is sent at once. The first
        // packet is lost; the second shows B the gap.
        // Let the heartbeat that fires at start go by.
        run_for(10).await;
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
fn a_node_that_joins_late_learns_the_state_from_digests() {
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

        // The newcomer lists every member; the members hear of it from
        // discovery, and each tells it what it holds.
        let late = spawn_node(&router, &all, OLD, &all[..OLD], tick);
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
