//! How a round spends its send slots and when it repairs a peer's stream.

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use crate::crdt::{KeyHash, NodeId, NodeIdentity};
use crate::gossip::sim::{LinkPolicy, SimRouter, sim_advance_ticks};
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
            nodes.handles.push(tokio::task::spawn_local(
                runtime.run(futures::stream::empty()),
            ));
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
