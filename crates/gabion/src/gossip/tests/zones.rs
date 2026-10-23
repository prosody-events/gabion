//! Two-level dissemination across zones: three zones of three nodes, one
//! subnet per zone, full membership.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use crate::crdt::{BucketEpoch, NodeId, NodeIdentity, ObservationBatch};
use crate::gossip::sim::{LinkPolicy, SimRouter, sim_advance_ticks};
use crate::gossip::topology::{Zone, rendezvous_weight};
use crate::gossip::{GossipClient, GossipRuntime, TokioClock};
use crate::wire::{self, FLAG_RELAY, FrameLimits};

use super::mock::{RecordingTransport, Sent, SentLog};
use super::{
    InMemoryAggregateStore, key_hash, rule_fp, run_paused, sim_config, store_for_property_rules,
};

const ZONES: usize = 3;
const PER_ZONE: usize = 3;
const NODES: usize = ZONES * PER_ZONE;

fn addr(node: usize) -> SocketAddr {
    SocketAddr::from((
        [10, (node / PER_ZONE) as u8, 0, (node % PER_ZONE) as u8 + 1],
        7946,
    ))
}

fn node_id(node: usize) -> NodeId {
    NodeId(0xB000 + node as u128)
}

fn zone(addr: SocketAddr) -> Zone {
    Zone::of(addr, crate::defaults::GOSSIP_ZONE_PREFIX_LEN)
}

/// The rendezvous winner among `members` for `origin`.
fn gateway(origin: usize, members: impl Iterator<Item = usize>) -> usize {
    members
        .max_by_key(|&m| rendezvous_weight(node_id(origin), addr(m)))
        .expect("zone has members")
}

fn zone_members(z: usize) -> impl Iterator<Item = usize> {
    z * PER_ZONE..(z + 1) * PER_ZONE
}

struct Cluster {
    clients: Vec<GossipClient<u32>>,
    aggregates: Vec<Rc<InMemoryAggregateStore<u32>>>,
    handles: Vec<tokio::task::JoinHandle<Result<(), crate::gossip::GossipError>>>,
    log: SentLog,
}

impl Cluster {
    /// Start every node, then let `shape` set link policies.
    fn start(shape: impl FnOnce(&SimRouter)) -> Self {
        let router = SimRouter::new();
        shape(&router);
        let log = SentLog::default();
        let mut cluster = Self {
            clients: Vec::new(),
            aggregates: Vec::new(),
            handles: Vec::new(),
            log: Rc::clone(&log),
        };
        for n in 0..NODES {
            let identity = NodeIdentity::new(node_id(n), 1);
            let peers = (0..NODES).filter(|&m| m != n).map(addr).collect();
            let aggregate = Rc::new(InMemoryAggregateStore::<u32>::new());
            let (runtime, client) = GossipRuntime::from_parts(
                RecordingTransport::new(router.bind(addr(n)), Rc::clone(&log)),
                TokioClock::from_millis(0),
                sim_config(identity, peers, n as u64),
                store_for_property_rules(identity, 64),
                aggregate.clone(),
            );
            cluster.handles.push(tokio::task::spawn_local(
                runtime.run(futures::stream::empty()),
            ));
            cluster.clients.push(client);
            cluster.aggregates.push(aggregate);
        }
        cluster
    }

    async fn record(&self, node: usize, hits: u64) {
        self.clients[node]
            .record(rule_fp(0), key_hash(1), 0, hits, 0, 0)
            .await
            .expect("record");
    }

    /// Run `seconds` of virtual time, then report each node whose aggregate
    /// differs from `expected`.
    async fn lagging(&self, seconds: u32, expected: u64) -> Vec<usize> {
        sim_advance_ticks(Duration::from_millis(100), seconds * 10).await;
        let want: BTreeMap<(u128, u128, BucketEpoch), u64> =
            BTreeMap::from([((rule_fp(0), key_hash(1).0, 0), expected)]);
        (0..NODES)
            .filter(|&n| self.aggregates[n].snapshot() != want)
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

/// Decoded view of one sent datagram.
struct Packet {
    relay: bool,
    cell_origins: Vec<NodeId>,
}

fn decode(sent: &Sent) -> Packet {
    let mut obs = ObservationBatch::<u32>::with_capacity(64);
    let summary = wire::decode_unauth::<u32>(&sent.bytes, FrameLimits::default(), &mut obs)
        .expect("every datagram the runtime sends decodes");
    Packet {
        relay: summary.header.flags & FLAG_RELAY != 0,
        cell_origins: obs.origin_node_ids.clone(),
    }
}

#[test]
fn cells_cross_zones_only_from_origin_to_its_gateway() {
    run_paused(async {
        let cluster = Cluster::start(|_| {});
        cluster.record(0, 5).await;
        cluster.record(4, 7).await;
        assert_eq!(cluster.lagging(10, 12).await, Vec::<usize>::new());

        let mut cross_zone_data = 0;
        let mut relay_data = 0;
        for sent in cluster.log.borrow().iter() {
            let packet = decode(sent);
            if packet.cell_origins.is_empty() {
                continue;
            }
            let src = (0..NODES).find(|&n| addr(n) == sent.src).expect("sender");
            let dst = (0..NODES)
                .find(|&n| addr(n) == sent.dst)
                .expect("recipient");
            if packet.relay {
                relay_data += 1;
                assert_eq!(
                    zone(sent.src),
                    zone(sent.dst),
                    "a gateway relays only into its own zone"
                );
            } else {
                assert!(
                    packet.cell_origins.iter().all(|&o| o == node_id(src)),
                    "an own-stream packet carries only the sender's cells",
                );
            }
            if zone(sent.src) != zone(sent.dst) {
                cross_zone_data += 1;
                assert!(!packet.relay);
                let remote = dst / PER_ZONE;
                assert_eq!(
                    dst,
                    gateway(src, zone_members(remote)),
                    "cells cross a zone boundary only to the origin's gateway",
                );
            }
        }
        assert!(cross_zone_data > 0, "the origins reached the other zones");
        assert!(relay_data > 0, "gateways relayed into their zones");
        cluster.stop().await;
    });
}

#[test]
fn unreachable_gateway_is_replaced_by_the_next_rank() {
    run_paused(async {
        let primary = gateway(0, zone_members(1));
        let cluster = Cluster::start(|router| {
            router.set_link_policy(addr(0), addr(primary), LinkPolicy::Block);
            router.set_link_policy(addr(primary), addr(0), LinkPolicy::Block);
        });
        cluster.record(0, 9).await;
        assert_eq!(cluster.lagging(20, 9).await, Vec::<usize>::new());

        let delivered_to: Vec<usize> = (0..NODES)
            .filter(|&n| zone(addr(n)) == zone(addr(3)))
            .filter(|&n| {
                cluster.log.borrow().iter().any(|s| {
                    s.src == addr(0) && s.dst == addr(n) && !decode(s).cell_origins.is_empty()
                })
            })
            .collect();
        assert!(
            delivered_to.contains(&primary),
            "the primary was tried first"
        );
        assert!(delivered_to.len() >= 2, "a second-ranked gateway took over");
        cluster.stop().await;
    });
}

#[test]
fn lost_relay_packets_are_resent_by_the_gateway() {
    run_paused(async {
        let gw = gateway(0, zone_members(1));
        let cluster = Cluster::start(|router| {
            for m in zone_members(1).filter(|&m| m != gw) {
                router.set_link_policy(addr(gw), addr(m), LinkPolicy::DropFirst { count: 3 });
            }
        });
        cluster.record(0, 4).await;
        assert_eq!(cluster.lagging(20, 4).await, Vec::<usize>::new());
        cluster.stop().await;
    });
}

#[test]
fn lost_origin_packets_are_resent_by_the_origin() {
    run_paused(async {
        let cluster = Cluster::start(|router| {
            for m in 1..NODES {
                router.set_link_policy(addr(0), addr(m), LinkPolicy::DropFirst { count: 2 });
            }
        });
        cluster.record(0, 6).await;
        assert_eq!(cluster.lagging(20, 6).await, Vec::<usize>::new());
        cluster.stop().await;
    });
}
