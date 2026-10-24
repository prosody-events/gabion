//! How a round spends its send slots and when it repairs a peer's stream.

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use crate::crdt::{KeyHash, NodeId, NodeIdentity};
use crate::gossip::sim::{SimRouter, sim_advance_ticks};
use crate::gossip::{GossipClient, GossipRuntime, TokioClock};

use super::{InMemoryAggregateStore, full_mesh, run_paused, sim_config, store_for};

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
    /// Start a full mesh on `router`; `send_slots` sizes node 0's send pool.
    fn start(router: &SimRouter, addrs: &[SocketAddr], send_slots: usize) -> Self {
        let mut nodes = Self {
            clients: Vec::new(),
            aggregates: Vec::new(),
            handles: Vec::new(),
        };
        for (i, &addr) in addrs.iter().enumerate() {
            let identity = NodeIdentity::new(NodeId(0xC000 + i as u128), 1);
            let mut config = sim_config(identity, full_mesh(addrs, i), i as u64);
            if i == 0 {
                config.send_queue_capacity = send_slots;
            }
            let aggregate = Rc::new(InMemoryAggregateStore::<u32>::new());
            let (runtime, client) = GossipRuntime::from_parts(
                router.bind(addr),
                TokioClock::from_millis(0),
                config,
                store_for(identity),
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
        let nodes = Nodes::start(&router, &zone_addrs(13), 4);
        nodes.clients[0]
            .record(0xC0FE, KeyHash(1), 0, 1, 0, 0)
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
