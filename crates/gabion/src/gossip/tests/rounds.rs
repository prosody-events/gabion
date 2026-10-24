//! How a round spends its send slots and when it repairs a peer's stream.

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::crdt::{KeyHash, NodeId, NodeIdentity};
use crate::discovery::{Peer, PeerEvent};
use crate::gossip::sim::{LinkPolicy, SimRouter, sim_advance_ticks};
use crate::gossip::topology::rendezvous_weight;
use crate::gossip::{GossipClient, GossipConfig, GossipRuntime, TokioClock};

use super::{
    InMemoryAggregateStore, full_mesh, rule_fp, run_paused, sim_config, store_for_property_rules,
};

/// `n` addresses in one zone.
fn zone_addrs(n: u8) -> Vec<SocketAddr> {
    (1..=n)
        .map(|i| SocketAddr::from(([10, 0, 0, i], 7946)))
        .collect()
}

struct Nodes {
    clients: Vec<GossipClient<u32>>,
    /// Discovery events for each node, on top of its bootstrap peers.
    events: Vec<mpsc::UnboundedSender<PeerEvent>>,
    aggregates: Vec<Rc<InMemoryAggregateStore<u32>>>,
    handles: Vec<tokio::task::JoinHandle<Result<(), crate::gossip::GossipError>>>,
}

impl Nodes {
    /// Start a full mesh on `router`; `tune` adjusts each node's config by
    /// index.
    fn start(
        router: &SimRouter,
        addrs: &[SocketAddr],
        tune: impl Fn(usize, &mut GossipConfig),
    ) -> Self {
        let mut nodes = Self {
            clients: Vec::new(),
            events: Vec::new(),
            aggregates: Vec::new(),
            handles: Vec::new(),
        };
        for (i, &addr) in addrs.iter().enumerate() {
            let identity = NodeIdentity::new(NodeId(0xC000 + i as u128), 1);
            let mut config = sim_config(identity, full_mesh(addrs, i), i as u64);
            tune(i, &mut config);
            let aggregate = Rc::new(InMemoryAggregateStore::<u32>::new());
            let (runtime, client) = GossipRuntime::from_parts(
                router.bind(addr),
                TokioClock::from_millis(0),
                config,
                store_for_property_rules(identity, 64),
                aggregate.clone(),
            );
            let (events, rx) = mpsc::unbounded_channel();
            let discovery = futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|evt| (evt, rx))
            });
            nodes
                .handles
                .push(tokio::task::spawn_local(runtime.run(discovery)));
            nodes.events.push(events);
            nodes.clients.push(client);
            nodes.aggregates.push(aggregate);
        }
        nodes
    }

    /// Nodes whose aggregate total is not `expected`.
    fn lagging(&self, expected: u64) -> Vec<usize> {
        (0..self.aggregates.len())
            .filter(|&n| self.aggregates[n].total() != expected)
            .collect()
    }

    async fn stop(self) {
        for client in &self.clients {
            let _ = client.shutdown().await;
        }
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}

#[test]
fn one_round_reaches_more_zone_peers_than_the_send_pool_holds() {
    run_paused(async {
        let router = SimRouter::new();
        let nodes = Nodes::start(&router, &zone_addrs(13), |i, c| {
            if i == 0 {
                c.send_queue_capacity = 4;
            }
        });
        nodes.clients[0]
            .record(rule_fp(0), KeyHash(1), 0, 1, 0, 0)
            .await
            .expect("record");
        // One heartbeat (100 ms) and a little delivery time, no second round.
        sim_advance_ticks(Duration::from_millis(10), 15).await;
        assert_eq!(
            nodes.lagging(1),
            Vec::<usize>::new(),
            "the first round sends to all twelve peers through four slots",
        );
        nodes.stop().await;
    });
}

/// A packet lost from the middle of a stream is reported by the receiver
/// once a later packet shows the gap, and resent long before the
/// retransmission timeout.
#[test]
fn a_reported_gap_is_resent_before_the_retransmission_timeout() {
    run_paused(async {
        let router = SimRouter::new();
        let addrs = zone_addrs(2);
        router.set_link_policy(addrs[0], addrs[1], LinkPolicy::DropFirst { count: 1 });
        let nodes = Nodes::start(&router, &addrs, |_, c| {
            c.tick_interval = Duration::from_millis(crate::defaults::GOSSIP_TICK_INTERVAL_MILLIS);
        });
        sim_advance_ticks(Duration::from_millis(10), 1).await;
        // Two hits cross the error budget, so each record flushes at once.
        for key in [1, 2] {
            nodes.clients[0]
                .record(rule_fp(0), KeyHash(key), 0, 2, 0, 0)
                .await
                .expect("record");
            sim_advance_ticks(Duration::from_millis(10), 4).await;
        }
        assert_eq!(
            nodes.aggregates[1].total(),
            4,
            "the first flush was lost, reported and resent within 50 ms of the second",
        );
        nodes.stop().await;
    });
}

/// A gateway that sees a gap in an origin's stream, after the origin has
/// moved on to a better-ranked gateway, reports it a few times and gives up:
/// the origin no longer sends that stream there, so no resend would ever
/// close it.
#[test]
fn a_gap_report_stops_when_the_origin_moves_to_another_gateway() {
    run_paused(async {
        let origin = SocketAddr::from(([10, 0, 0, 1], 7946));
        let mut zone: Vec<SocketAddr> = (1..=2)
            .map(|i| SocketAddr::from(([10, 1, 0, i], 7946)))
            .collect();
        zone.sort_by_key(|&m| std::cmp::Reverse(rendezvous_weight(NodeId(0xC000), m)));
        let (top, low) = (zone[0], zone[1]);
        let addrs = [origin, low, top];
        let router = SimRouter::new();
        let nodes = Nodes::start(&router, &addrs, |i, c| {
            if i == 0 {
                // The origin has not yet discovered the better gateway.
                c.bootstrap_peers.retain(|&p| p != top);
            }
        });
        sim_advance_ticks(Duration::from_millis(100), 5).await;
        router.set_link_policy(origin, low, LinkPolicy::Block);
        nodes.clients[0]
            .record(rule_fp(0), KeyHash(1), 0, 2, 0, 0)
            .await
            .expect("record");
        sim_advance_ticks(Duration::from_millis(10), 2).await;
        router.set_link_policy(origin, low, LinkPolicy::Pass);
        nodes.clients[0]
            .record(rule_fp(0), KeyHash(2), 0, 2, 0, 0)
            .await
            .expect("record");
        nodes.events[0]
            .send(PeerEvent::Added(Peer::new(top)))
            .expect("discovery open");
        sim_advance_ticks(Duration::from_millis(100), 20).await;
        assert_eq!(nodes.lagging(4), Vec::<usize>::new(), "everyone converges");

        let before = router.received_count(origin);
        sim_advance_ticks(Duration::from_millis(100), 100).await;
        assert_eq!(
            router.received_count(origin),
            before,
            "the quiet cluster hears no more reports about the abandoned stream",
        );
        nodes.stop().await;
    });
}

/// A gateway that takes over forwards the origin's whole live window into
/// its zone, even the cells it already holds: the previous gateway may have
/// relayed them to every member but one it did not know of.
#[test]
fn a_new_gateway_forwards_cells_the_old_one_could_not() {
    run_paused(async {
        let origin = SocketAddr::from(([10, 0, 0, 1], 7946));
        let mut zone: Vec<SocketAddr> = (1..=3)
            .map(|i| SocketAddr::from(([10, 1, 0, i], 7946)))
            .collect();
        zone.sort_by_key(|&m| std::cmp::Reverse(rendezvous_weight(NodeId(0xC000), m)));
        let (first, second, member) = (zone[0], zone[1], zone[2]);
        let addrs = [origin, first, second, member];
        let router = SimRouter::new();
        let nodes = Nodes::start(&router, &addrs, |i, c| {
            if addrs[i] == first {
                // The first gateway never learns of `member`.
                c.bootstrap_peers.retain(|&p| p != member);
            }
        });
        // Let the start-up sweeps of the still empty stores finish.
        sim_advance_ticks(Duration::from_millis(100), 10).await;
        nodes.clients[0]
            .record(rule_fp(0), KeyHash(1), 0, 1, 0, 0)
            .await
            .expect("record");
        sim_advance_ticks(Duration::from_millis(100), 10).await;
        assert_eq!(
            nodes.aggregates[3].total(),
            0,
            "the first gateway skipped it"
        );

        router.set_link_policy(origin, first, LinkPolicy::Block);
        router.set_link_policy(first, origin, LinkPolicy::Block);
        nodes.clients[0]
            .record(rule_fp(0), KeyHash(2), 0, 1, 0, 0)
            .await
            .expect("record");
        sim_advance_ticks(Duration::from_millis(100), 30).await;
        assert_eq!(nodes.lagging(2), Vec::<usize>::new(), "all hold both hits");
        nodes.stop().await;
    });
}

/// A node that joins a zone pulls the live window from zone members that
/// have not yet learned of it, instead of waiting for them to.
#[test]
fn a_joiner_sweeps_the_window_from_its_zone() {
    run_paused(async {
        let addrs = zone_addrs(3);
        let joiner = addrs[2];
        let router = SimRouter::new();
        let nodes = Nodes::start(&router, &addrs, |i, c| {
            // Nobody knows the joiner, and it learns of the zone later.
            c.bootstrap_peers.retain(|&p| p != joiner);
            if i == 2 {
                c.bootstrap_peers.clear();
            }
        });
        for (node, key) in [(0, 1), (1, 2)] {
            nodes.clients[node]
                .record(rule_fp(0), KeyHash(key), 0, 1, 0, 0)
                .await
                .expect("record");
        }
        sim_advance_ticks(Duration::from_millis(100), 5).await;
        assert_eq!(nodes.lagging(2), vec![2]);

        for &member in &addrs[..2] {
            nodes.events[2]
                .send(PeerEvent::Added(Peer::new(member)))
                .expect("discovery open");
        }
        sim_advance_ticks(Duration::from_millis(10), 5).await;
        assert_eq!(nodes.lagging(2), Vec::<usize>::new(), "swept within 50 ms");
        nodes.stop().await;
    });
}

/// A zone member that leaves before it ever reached us took cells only it
/// would have sent us; the rest of the zone holds them, and we sweep them.
#[test]
fn a_member_leaving_unheard_triggers_a_sweep() {
    run_paused(async {
        let addrs = zone_addrs(3);
        let (leaver, observer) = (addrs[1], addrs[2]);
        let router = SimRouter::new();
        let nodes = Nodes::start(&router, &addrs, |i, c| {
            if i == 1 {
                // The leaver never learns of the observer.
                c.bootstrap_peers.retain(|&p| p != observer);
            }
        });
        // Let the start-up sweeps of the still empty stores finish.
        sim_advance_ticks(Duration::from_millis(100), 5).await;
        nodes.clients[1]
            .record(rule_fp(0), KeyHash(1), 0, 1, 0, 0)
            .await
            .expect("record");
        sim_advance_ticks(Duration::from_millis(100), 5).await;
        assert_eq!(nodes.lagging(1), vec![2]);

        nodes.events[2]
            .send(PeerEvent::Removed(Peer::new(leaver)))
            .expect("discovery open");
        sim_advance_ticks(Duration::from_millis(10), 5).await;
        assert_eq!(nodes.aggregates[2].total(), 1, "swept from the zone");
        nodes.stop().await;
    });
}
