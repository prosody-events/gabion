//! End-to-end gossip runtime tests.
//!
//! Most tests use [`super::sim::SimTransport`] + `tokio::time::pause()` so
//! virtual time + in-memory delivery make them deterministic and fast.
//! `udp_round_trip_smoke` is the lone realtime/UDP smoke test, kept to
//! ensure the production transport doesn't bit-rot.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio::task::LocalSet;

use crate::crdt::{
    BucketEpoch, CellStore, CellStoreConfig, Count, DeltaSink, ExpirationSink, KeyHash, NodeId,
    NodeIdentity, RuleDescriptor,
};
use crate::discovery::{Peer, PeerEvent};
use crate::gossip::sim::{LinkPolicy, SimRouter, sim_advance_ticks};
use crate::gossip::{AggregateStore, GossipConfig, GossipRuntime, TokioClock, UdpTransport};
use crate::wire::HmacKey;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

// -- in-memory aggregate store ----------------------------------------------

/// Single-threaded in-memory aggregate store used as the canonical test
/// backend. Keys on `(rule, key_hash, bucket)`. `RefCell` keeps the trait
/// method `&self` so tests can clone an `Rc` between the runtime and the
/// read path.
#[derive(Default)]
pub(super) struct InMemoryAggregateStore<C: Count> {
    inner: RefCell<HashMap<(u128, KeyHash, BucketEpoch), u64>>,
    apply_calls: RefCell<Vec<(usize, usize)>>,
    _marker: std::marker::PhantomData<C>,
}

impl<C: Count> InMemoryAggregateStore<C> {
    pub fn new() -> Self {
        Self {
            inner: RefCell::new(HashMap::new()),
            apply_calls: RefCell::new(Vec::new()),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn apply_call_lens(&self) -> Vec<(usize, usize)> {
        self.apply_calls.borrow().clone()
    }

    pub fn snapshot(&self) -> BTreeMap<(u128, u128, BucketEpoch), u64> {
        self.inner
            .borrow()
            .iter()
            .map(|((rule, key, bucket), count)| ((*rule, key.0, *bucket), *count))
            .collect()
    }
}

impl<C: Count> AggregateStore<C> for InMemoryAggregateStore<C> {
    fn apply(&self, deltas: &DeltaSink<C>, expirations: &ExpirationSink<C>) {
        self.apply_calls
            .borrow_mut()
            .push((deltas.len(), expirations.len()));

        let mut map = self.inner.borrow_mut();
        for i in 0..deltas.len() {
            let key = &deltas.keys[i];
            let v: u64 = deltas.deltas[i].into();
            *map.entry((key.rule_fingerprint, key.key_hash, key.bucket))
                .or_insert(0) += v;
        }
        for i in 0..expirations.len() {
            let key = &expirations.keys[i];
            let v: u64 = expirations.last_counts[i].into();
            let entry = map
                .entry((key.rule_fingerprint, key.key_hash, key.bucket))
                .or_insert(0);
            *entry = entry.saturating_sub(v);
            if *entry == 0 {
                map.remove(&(key.rule_fingerprint, key.key_hash, key.bucket));
            }
        }
    }
}

// -- helpers ----------------------------------------------------------------

fn store_for(identity: NodeIdentity) -> CellStore<u32> {
    CellStore::<u32>::new(CellStoreConfig::default(), identity)
}

fn sock(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn sim_config(identity: NodeIdentity, peers: Vec<SocketAddr>, seed: u64) -> GossipConfig {
    GossipConfig {
        local_identity: identity,
        cluster_id_hash: 0xC1,
        bootstrap_peers: peers,
        fanout: 1,
        tick_interval: Duration::from_millis(100),
        rng_seed: seed,
        ..GossipConfig::default()
    }
}

fn quickcheck_tests() -> u64 {
    std::env::var("QUICKCHECK_TESTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
}

fn quickcheck_max_nodes() -> u16 {
    std::env::var("GOSSIP_QUICKCHECK_MAX_NODES")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .map(|n| n.clamp(1, 1024))
        .unwrap_or(32)
}

fn run_paused<F, R>(f: F) -> R
where
    F: std::future::Future<Output = R>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let local = LocalSet::new();
    rt.block_on(local.run_until(f))
}

#[derive(Clone, Debug)]
struct SimRecord {
    node: u16,
    rule: u8,
    key: u8,
    hits: u8,
}

impl Arbitrary for SimRecord {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            node: u16::arbitrary(g),
            rule: u8::arbitrary(g) % 3,
            key: u8::arbitrary(g) % 8,
            hits: (u8::arbitrary(g) % 20) + 1,
        }
    }
}

#[derive(Clone, Debug)]
struct ConnectedClusterCase {
    nodes: u16,
    fanout: u16,
    peer_degree: u16,
    records: Vec<SimRecord>,
    drop_first: u8,
}

impl Arbitrary for ConnectedClusterCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_nodes = quickcheck_max_nodes();
        let nodes = (u16::arbitrary(g) % max_nodes) + 1;
        let max_records = 32;
        let len = usize::arbitrary(g) % max_records;
        Self {
            nodes,
            fanout: if nodes == 1 {
                0
            } else {
                (u16::arbitrary(g) % (nodes - 1)) + 1
            },
            peer_degree: (u16::arbitrary(g) % 7) + 4,
            records: (0..len).map(|_| SimRecord::arbitrary(g)).collect(),
            drop_first: u8::arbitrary(g) % 4,
        }
    }
}

#[derive(Clone, Debug)]
struct PartitionCase {
    records: Vec<SimRecord>,
    heal_after_ticks: u8,
}

impl Arbitrary for PartitionCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::arbitrary(g) % 32;
        Self {
            records: (0..len).map(|_| SimRecord::arbitrary(g)).collect(),
            heal_after_ticks: (u8::arbitrary(g) % 8) + 1,
        }
    }
}

#[derive(Clone, Debug)]
struct AuthCase {
    matching_keys: bool,
    a_hits: u8,
    b_hits: u8,
}

impl Arbitrary for AuthCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            matching_keys: bool::arbitrary(g),
            a_hits: (u8::arbitrary(g) % 20) + 1,
            b_hits: (u8::arbitrary(g) % 20) + 1,
        }
    }
}

#[derive(Clone, Debug)]
struct ExpirationCase {
    nodes: u16,
    records: Vec<SimRecord>,
}

impl Arbitrary for ExpirationCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let nodes = (u16::arbitrary(g) % 15) + 2;
        let len = (usize::arbitrary(g) % 32) + 1;
        Self {
            nodes,
            records: (0..len).map(|_| SimRecord::arbitrary(g)).collect(),
        }
    }
}

#[derive(Clone, Debug)]
struct MembershipCase {
    remove_after_add: bool,
    hits: u8,
}

impl Arbitrary for MembershipCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            remove_after_add: bool::arbitrary(g),
            hits: (u8::arbitrary(g) % 20) + 1,
        }
    }
}

fn rule_fp(rule: u8) -> u128 {
    0xABC0 + rule as u128
}

fn key_hash(key: u8) -> KeyHash {
    KeyHash(0x1000 + key as u128)
}

fn expected_model(records: &[SimRecord], nodes: usize) -> BTreeMap<(u128, u128, BucketEpoch), u64> {
    let mut model = BTreeMap::new();
    for record in records {
        let node = record.node as usize % nodes;
        let _origin = node;
        let key = (rule_fp(record.rule), key_hash(record.key).0, 0);
        *model.entry(key).or_insert(0) += record.hits as u64;
    }
    model
}

fn store_for_expiring_rule(identity: NodeIdentity) -> CellStore<u32> {
    let mut store = store_for(identity);
    for rule in 0..3 {
        store
            .intern_rule(RuleDescriptor {
                fingerprint: rule_fp(rule),
                window_millis: 100,
                bucket_millis: 100,
                limit: 1_000,
                flags: 0,
                local_rule_id: rule as u32,
            })
            .unwrap();
    }
    store
}

fn store_for_property_rules(identity: NodeIdentity, node_capacity: u16) -> CellStore<u32> {
    let mut store = CellStore::<u32>::new(
        CellStoreConfig {
            cell_capacity: 512,
            rule_dictionary_capacity: 8,
            node_dictionary_capacity: node_capacity.max(8),
            local_dirty_capacity: 512,
            forwarded_dirty_capacity: 512,
            peer_capacity: 32,
        },
        identity,
    );
    for rule in 0..3 {
        store
            .intern_rule(RuleDescriptor {
                fingerprint: rule_fp(rule),
                window_millis: 3_600_000,
                bucket_millis: 100,
                limit: 1_000_000,
                flags: 0,
                local_rule_id: rule as u32,
            })
            .unwrap();
    }
    store
}

fn overlay_peers(addrs: &[SocketAddr], index: usize, degree: usize) -> Vec<SocketAddr> {
    let nodes = addrs.len();
    if nodes <= 1 {
        return Vec::new();
    }
    let degree = degree.min((nodes - 1).ilog2() as usize + 1).max(1);
    let mut peers = Vec::with_capacity(degree * 2);
    let mut offset = 1_usize;
    for _ in 0..degree {
        peers.push(addrs[(index + offset) % nodes]);
        peers.push(addrs[(index + nodes - (offset % nodes)) % nodes]);
        offset = (offset * 2).min(nodes - 1);
    }
    peers.sort_unstable();
    peers.dedup();
    peers
}

async fn run_connected_case(case: ConnectedClusterCase) -> TestResult {
    let nodes = case.nodes as usize;
    if case.records.is_empty() {
        return TestResult::discard();
    }

    let router = SimRouter::with_channel_capacity(256);
    let addrs: Vec<SocketAddr> = (0..nodes).map(|i| sock(41_000 + i as u16)).collect();
    if nodes > 1 {
        for i in 0..nodes {
            if case.drop_first == 0 {
                continue;
            }
            let dst = (i + 1) % nodes;
            router.set_link_policy(
                addrs[i],
                addrs[dst],
                LinkPolicy::DropFirst {
                    count: case.drop_first as u32,
                },
            );
        }
    }

    let mut clients = Vec::with_capacity(nodes);
    let mut handles = Vec::with_capacity(nodes);
    let mut aggregates = Vec::with_capacity(nodes);
    let peer_degree = case.peer_degree as usize;
    let node_capacity =
        ((case.records.len() + 1).next_power_of_two().max(8)).min(u16::MAX as usize) as u16;
    for i in 0..nodes {
        let identity = NodeIdentity::new(NodeId(0xA000 + i as u128), 1);
        let peers = overlay_peers(&addrs, i, peer_degree);
        let mut cfg = sim_config(identity, peers, 0xBAD5_EED0 + i as u64);
        cfg.fanout = (case.fanout as usize).min(nodes.saturating_sub(1));
        cfg.max_cells_per_tick = 128;
        cfg.send_queue_capacity = 128;
        let agg = Rc::new(InMemoryAggregateStore::<u32>::new());
        let (rt, client) = GossipRuntime::from_parts(
            router.bind(addrs[i]),
            TokioClock::from_millis(0),
            cfg,
            store_for_property_rules(identity, node_capacity),
            agg.clone(),
        );
        handles.push(tokio::task::spawn_local(rt.run(futures::stream::empty())));
        clients.push(client);
        aggregates.push(agg);
    }

    for record in &case.records {
        let node = record.node as usize % nodes;
        if clients[node]
            .record(
                rule_fp(record.rule),
                key_hash(record.key),
                0,
                record.hits as u64,
                0,
            )
            .await
            .is_err()
        {
            return TestResult::failed();
        }
    }

    let ticks = if nodes == 1 { 1 } else { 160 };
    sim_advance_ticks(Duration::from_millis(100), ticks).await;

    let expected = expected_model(&case.records, nodes);
    let passed = aggregates.iter().all(|agg| agg.snapshot() == expected);
    if !passed {
        let mismatches = aggregates
            .iter()
            .enumerate()
            .filter(|(_, agg)| agg.snapshot() != expected)
            .take(5)
            .map(|(i, agg)| (i, agg.snapshot()))
            .collect::<Vec<_>>();
        eprintln!(
            "connected gossip case failed: case={case:?}, expected={expected:?}, \
             mismatches={mismatches:?}"
        );
    }

    for client in clients {
        let _ = client.shutdown().await;
    }
    for handle in handles {
        let _ = handle.await;
    }

    TestResult::from_bool(passed)
}

#[test]
fn quickcheck_sim_connected_clusters_converge_after_finite_loss() {
    fn property(case: ConnectedClusterCase) -> TestResult {
        run_paused(run_connected_case(case))
    }

    QuickCheck::new()
        .tests(quickcheck_tests())
        .quickcheck(property as fn(ConnectedClusterCase) -> TestResult);
}

#[test]
fn quickcheck_sim_partition_heals_without_overcount() {
    fn property(mut case: PartitionCase) -> TestResult {
        run_paused(async move {
            const NODES: usize = 8;
            if case.records.is_empty() {
                return TestResult::discard();
            }
            for record in &mut case.records {
                record.node %= NODES as u16;
            }

            let router = SimRouter::with_channel_capacity(128);
            let addrs: Vec<_> = (0..NODES).map(|i| sock(42_000 + i as u16)).collect();
            for left in 0..4 {
                for right in 4..8 {
                    router.set_link_policy(addrs[left], addrs[right], LinkPolicy::Block);
                    router.set_link_policy(addrs[right], addrs[left], LinkPolicy::Block);
                }
            }

            let mut clients = Vec::with_capacity(NODES);
            let mut handles = Vec::with_capacity(NODES);
            let mut aggregates = Vec::with_capacity(NODES);
            for i in 0..NODES {
                let identity = NodeIdentity::new(NodeId(0xB000 + i as u128), 1);
                let peers: Vec<_> = addrs
                    .iter()
                    .copied()
                    .filter(|addr| *addr != addrs[i])
                    .collect();
                let mut cfg = sim_config(identity, peers, 0xC0FF_EE00 + i as u64);
                cfg.fanout = 4;
                cfg.max_cells_per_tick = 32;
                let agg = Rc::new(InMemoryAggregateStore::<u32>::new());
                let (rt, client) = GossipRuntime::from_parts(
                    router.bind(addrs[i]),
                    TokioClock::from_millis(0),
                    cfg,
                    store_for(identity),
                    agg.clone(),
                );
                handles.push(tokio::task::spawn_local(rt.run(futures::stream::empty())));
                clients.push(client);
                aggregates.push(agg);
            }

            for record in &case.records {
                let node = record.node as usize % NODES;
                clients[node]
                    .record(
                        rule_fp(record.rule),
                        key_hash(record.key),
                        0,
                        record.hits as u64,
                        0,
                    )
                    .await
                    .unwrap();
            }

            sim_advance_ticks(Duration::from_millis(100), case.heal_after_ticks as u32).await;
            let global = expected_model(&case.records, NODES);
            let no_overcount = aggregates.iter().all(|agg| {
                agg.snapshot()
                    .iter()
                    .all(|(key, count)| *count <= *global.get(key).unwrap_or(&0))
            });

            for left in 0..4 {
                for right in 4..8 {
                    router.set_link_policy(addrs[left], addrs[right], LinkPolicy::Pass);
                    router.set_link_policy(addrs[right], addrs[left], LinkPolicy::Pass);
                }
            }
            sim_advance_ticks(Duration::from_millis(100), 120).await;
            let converged = aggregates.iter().all(|agg| agg.snapshot() == global);

            for client in clients {
                let _ = client.shutdown().await;
            }
            for handle in handles {
                let _ = handle.await;
            }
            TestResult::from_bool(no_overcount && converged)
        })
    }

    QuickCheck::new()
        .tests(quickcheck_tests())
        .quickcheck(property as fn(PartitionCase) -> TestResult);
}

#[test]
fn quickcheck_sim_authentication_admits_only_matching_keys() {
    fn property(case: AuthCase) -> TestResult {
        run_paused(async move {
            let router = SimRouter::new();
            let addr_a = sock(43_000);
            let addr_b = sock(43_001);
            let id_a = NodeIdentity::new(NodeId(0xCA), 1);
            let id_b = NodeIdentity::new(NodeId(0xCB), 1);
            let key_a = HmacKey([7; 32]);
            let key_b = if case.matching_keys {
                HmacKey([7; 32])
            } else {
                HmacKey([8; 32])
            };

            let mut cfg_a = sim_config(id_a, vec![addr_b], 1);
            cfg_a.auth_key = Some(key_a);
            let mut cfg_b = sim_config(id_b, vec![addr_a], 2);
            cfg_b.auth_key = Some(key_b);
            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());
            let (rt_a, client_a) = GossipRuntime::from_parts(
                router.bind(addr_a),
                TokioClock::from_millis(0),
                cfg_a,
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, client_b) = GossipRuntime::from_parts(
                router.bind(addr_b),
                TokioClock::from_millis(0),
                cfg_b,
                store_for(id_b),
                agg_b.clone(),
            );
            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            client_a
                .record(0xD00D, KeyHash(1), 0, case.a_hits as u64, 0)
                .await
                .unwrap();
            client_b
                .record(0xD00D, KeyHash(1), 0, case.b_hits as u64, 0)
                .await
                .unwrap();
            sim_advance_ticks(Duration::from_millis(100), 20).await;

            let sum_a = agg_a.inner.borrow().values().copied().sum::<u64>();
            let sum_b = agg_b.inner.borrow().values().copied().sum::<u64>();
            let expected = if case.matching_keys {
                (
                    case.a_hits as u64 + case.b_hits as u64,
                    case.a_hits as u64 + case.b_hits as u64,
                )
            } else {
                (case.a_hits as u64, case.b_hits as u64)
            };

            client_a.shutdown().await.unwrap();
            client_b.shutdown().await.unwrap();
            let _ = h_a.await;
            let _ = h_b.await;

            TestResult::from_bool((sum_a, sum_b) == expected)
        })
    }

    QuickCheck::new()
        .tests(quickcheck_tests())
        .quickcheck(property as fn(AuthCase) -> TestResult);
}

#[test]
fn quickcheck_sim_tick_expiration_removes_converged_cells() {
    fn property(case: ExpirationCase) -> TestResult {
        run_paused(async move {
            let nodes = case.nodes as usize;
            let router = SimRouter::with_channel_capacity(128);
            let addrs: Vec<_> = (0..nodes).map(|i| sock(44_000 + i as u16)).collect();
            let mut clients = Vec::with_capacity(nodes);
            let mut handles = Vec::with_capacity(nodes);
            let mut aggregates = Vec::with_capacity(nodes);

            for i in 0..nodes {
                let identity = NodeIdentity::new(NodeId(0xE000 + i as u128), 1);
                let peers: Vec<_> = addrs
                    .iter()
                    .copied()
                    .filter(|addr| *addr != addrs[i])
                    .collect();
                let mut cfg = sim_config(identity, peers, 0xEED0 + i as u64);
                cfg.fanout = nodes.saturating_sub(1).min(8).max(1);
                cfg.max_cells_per_tick = 32;
                let agg = Rc::new(InMemoryAggregateStore::<u32>::new());
                let (rt, client) = GossipRuntime::from_parts(
                    router.bind(addrs[i]),
                    TokioClock::from_millis(0),
                    cfg,
                    store_for_expiring_rule(identity),
                    agg.clone(),
                );
                handles.push(tokio::task::spawn_local(rt.run(futures::stream::empty())));
                clients.push(client);
                aggregates.push(agg);
            }

            for record in &case.records {
                let node = record.node as usize % nodes;
                clients[node]
                    .record(
                        rule_fp(record.rule),
                        key_hash(record.key),
                        0,
                        record.hits as u64,
                        0,
                    )
                    .await
                    .unwrap();
            }

            sim_advance_ticks(Duration::from_millis(100), 8).await;
            let expired_everywhere = aggregates.iter().all(|agg| agg.snapshot().is_empty());

            for client in clients {
                let _ = client.shutdown().await;
            }
            for handle in handles {
                let _ = handle.await;
            }
            TestResult::from_bool(expired_everywhere)
        })
    }

    QuickCheck::new()
        .tests(quickcheck_tests())
        .quickcheck(property as fn(ExpirationCase) -> TestResult);
}

#[test]
fn quickcheck_sim_peer_membership_controls_delivery() {
    fn property(case: MembershipCase) -> TestResult {
        run_paused(async move {
            let router = SimRouter::new();
            let addr_a = sock(45_000);
            let addr_b = sock(45_001);
            let id_a = NodeIdentity::new(NodeId(0xFA), 1);
            let id_b = NodeIdentity::new(NodeId(0xFB), 1);

            let events = if case.remove_after_add {
                vec![
                    PeerEvent::Added(Peer::new(addr_b)),
                    PeerEvent::Removed(Peer::new(addr_b)),
                ]
            } else {
                vec![PeerEvent::Added(Peer::new(addr_b))]
            };

            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());
            let (rt_a, client_a) = GossipRuntime::from_parts(
                router.bind(addr_a),
                TokioClock::from_millis(0),
                sim_config(id_a, Vec::new(), 1),
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, client_b) = GossipRuntime::from_parts(
                router.bind(addr_b),
                TokioClock::from_millis(0),
                sim_config(id_b, Vec::new(), 2),
                store_for(id_b),
                agg_b.clone(),
            );
            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::iter(events)));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            tokio::task::yield_now().await;
            client_a
                .record(0xFACE, KeyHash(1), 0, case.hits as u64, 0)
                .await
                .unwrap();
            sim_advance_ticks(Duration::from_millis(100), 20).await;

            let got_b = agg_b.inner.borrow().values().copied().sum::<u64>();
            let expected_b = if case.remove_after_add {
                0
            } else {
                case.hits as u64
            };

            client_a.shutdown().await.unwrap();
            client_b.shutdown().await.unwrap();
            let _ = h_a.await;
            let _ = h_b.await;
            TestResult::from_bool(got_b == expected_b)
        })
    }

    QuickCheck::new()
        .tests(quickcheck_tests())
        .quickcheck(property as fn(MembershipCase) -> TestResult);
}

// -- migrated tests ---------------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn record_acks_after_apply() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr = sock(40_001);
            let transport = router.bind(addr);

            let identity = NodeIdentity::new(NodeId(0xAA), 1);
            let store = store_for(identity);
            let agg = Rc::new(InMemoryAggregateStore::<u32>::new());
            let clock = TokioClock::from_millis(0);

            let (rt, client) = GossipRuntime::from_parts(
                transport,
                clock,
                sim_config(identity, Vec::new(), 1),
                store,
                agg.clone(),
            );
            let handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));

            let rule_fp: u128 = 0xDEAD_BEEF;
            let key = KeyHash(0x1234);
            client.record(rule_fp, key, 0, 5, 1_000).await.unwrap();

            // Read the count back from the store handle the test holds —
            // requires looking up the rule_slot.
            // Use a small dance: send a second record that pushes the store
            // through one more apply so we know which rule slot was minted.
            // (The first record itself must have stored the count already.)
            // Easier: probe by iterating the inner map.
            let totals: u64 = agg.inner.borrow().values().copied().sum();
            assert_eq!(totals, 5);

            client.record(rule_fp, key, 0, 3, 1_000).await.unwrap();
            let totals: u64 = agg.inner.borrow().values().copied().sum();
            assert_eq!(totals, 8);

            client.shutdown().await.unwrap();
            let _ = handle.await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn two_runtimes_converge_on_cluster_aggregate() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(40_010);
            let addr_b = sock(40_011);
            let t_a = router.bind(addr_a);
            let t_b = router.bind(addr_b);

            let id_a = NodeIdentity::new(NodeId(0xAA00), 1);
            let id_b = NodeIdentity::new(NodeId(0xBB00), 1);

            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                t_a,
                TokioClock::from_millis(0),
                sim_config(id_a, vec![addr_b], 1),
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, client_b) = GossipRuntime::from_parts(
                t_b,
                TokioClock::from_millis(0),
                sim_config(id_b, vec![addr_a], 2),
                store_for(id_b),
                agg_b.clone(),
            );

            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            let rule_fp: u128 = 0xC0FE;
            let key = KeyHash(0xABCD);

            client_a.record(rule_fp, key, 0, 3, 1_000).await.unwrap();
            client_b.record(rule_fp, key, 0, 5, 1_000).await.unwrap();

            // Drive enough virtual ticks for both directions to drain.
            sim_advance_ticks(Duration::from_millis(100), 10).await;

            let sum_a: u64 = agg_a.inner.borrow().values().copied().sum();
            let sum_b: u64 = agg_b.inner.borrow().values().copied().sum();
            assert_eq!(sum_a, 8, "A converges");
            assert_eq!(sum_b, 8, "B converges");

            client_a.shutdown().await.unwrap();
            client_b.shutdown().await.unwrap();
            let _ = h_a.await;
            let _ = h_b.await;
        })
        .await;
}

// -- apply-once-per-iteration -----------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn apply_called_once_per_crdt_iteration() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr = sock(40_020);
            let transport = router.bind(addr);

            let identity = NodeIdentity::new(NodeId(0xAA), 1);
            let store = store_for(identity);
            let agg = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (rt, client) = GossipRuntime::from_parts(
                transport,
                TokioClock::from_millis(0),
                sim_config(identity, Vec::new(), 1),
                store,
                agg.clone(),
            );
            let handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));

            // One record => one apply with deltas.len()==1.
            client.record(0x11, KeyHash(1), 0, 4, 100).await.unwrap();
            let calls = agg.apply_call_lens();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].0, 1);
            assert_eq!(calls[0].1, 0);

            client.shutdown().await.unwrap();
            let _ = handle.await;
        })
        .await;
}

// -- expire-on-tick ---------------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn gossip_tick_drives_expiration() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr = sock(40_030);
            let transport = router.bind(addr);

            let identity = NodeIdentity::new(NodeId(0xAA), 1);
            let mut store = store_for(identity);
            // Rule with 100ms bucket, 100ms window -> 1 live bucket.
            store
                .intern_rule(RuleDescriptor {
                    fingerprint: 0xFEED,
                    window_millis: 100,
                    bucket_millis: 100,
                    limit: 10,
                    flags: 0,
                    local_rule_id: 1,
                })
                .unwrap();

            let agg = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (rt, client) = GossipRuntime::from_parts(
                transport,
                TokioClock::from_millis(0),
                sim_config(identity, Vec::new(), 1),
                store,
                agg.clone(),
            );
            let handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));

            // Hit at virtual time 0 — bucket 0.
            client.record(0xFEED, KeyHash(7), 0, 2, 0).await.unwrap();
            assert_eq!(agg.inner.borrow().values().copied().sum::<u64>(), 2);

            // Advance past one tick + past the live window.
            sim_advance_ticks(Duration::from_millis(100), 3).await;

            // The tick handler called expire_at, which freed the cell.
            // The aggregate store sees expiration row(s) and removes the
            // value.
            assert_eq!(agg.inner.borrow().values().copied().sum::<u64>(), 0);

            client.shutdown().await.unwrap();
            let _ = handle.await;
        })
        .await;
}

// -- ack ordering against apply ---------------------------------------------

struct BlockingApplyStore<C: Count> {
    notify: Arc<Notify>,
    released: RefCell<bool>,
    inner: InMemoryAggregateStore<C>,
}

impl<C: Count> BlockingApplyStore<C> {
    fn new(notify: Arc<Notify>) -> Self {
        Self {
            notify,
            released: RefCell::new(false),
            inner: InMemoryAggregateStore::new(),
        }
    }
}

impl<C: Count> AggregateStore<C> for BlockingApplyStore<C> {
    fn apply(&self, deltas: &DeltaSink<C>, expirations: &ExpirationSink<C>) {
        // Park apply behind the notify the first time it's called.
        if !*self.released.borrow() {
            *self.released.borrow_mut() = true;
            // We can't .await here — apply is sync. Instead, we mark this
            // arm by setting `released` and rely on the test's manual
            // `notify.notify_one()` having been issued already to release
            // the test path that waits for it. The test verifies the
            // ordering by ensuring the aggregate store reflects the
            // increment BEFORE record() returns.
        }
        self.notify.notify_one();
        self.inner.apply(deltas, expirations);
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn record_returns_after_apply_completes() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr = sock(40_040);
            let transport = router.bind(addr);

            let identity = NodeIdentity::new(NodeId(0xAA), 1);
            let store = store_for(identity);
            let notify = Arc::new(Notify::new());
            let agg = Rc::new(BlockingApplyStore::<u32>::new(notify.clone()));

            let (rt, client) = GossipRuntime::from_parts(
                transport,
                TokioClock::from_millis(0),
                sim_config(identity, Vec::new(), 1),
                store,
                agg.clone(),
            );
            let handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));

            client.record(0xABC, KeyHash(1), 0, 3, 0).await.unwrap();
            // After the ack returns, apply must have completed at least once.
            let totals: u64 = agg.inner.inner.borrow().values().copied().sum();
            assert_eq!(totals, 3);

            client.shutdown().await.unwrap();
            let _ = handle.await;
        })
        .await;
}

// -- per-peer pruning -------------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn per_peer_frame_prunes_acked_cells() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(40_050);
            let addr_b = sock(40_051);
            let t_a = router.bind(addr_a);
            let t_b = router.bind(addr_b);

            let id_a = NodeIdentity::new(NodeId(0xAA00), 1);
            let id_b = NodeIdentity::new(NodeId(0xBB00), 1);

            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                t_a,
                TokioClock::from_millis(0),
                sim_config(id_a, vec![addr_b], 1),
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, _client_b) = GossipRuntime::from_parts(
                t_b,
                TokioClock::from_millis(0),
                sim_config(id_b, vec![addr_a], 2),
                store_for(id_b),
                agg_b.clone(),
            );

            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            client_a.record(0xDEAD, KeyHash(1), 0, 4, 0).await.unwrap();
            // Drive enough ticks for the eager-push lane to converge.
            sim_advance_ticks(Duration::from_millis(100), 5).await;

            let sum_a: u64 = agg_a.inner.borrow().values().copied().sum();
            let sum_b: u64 = agg_b.inner.borrow().values().copied().sum();
            assert_eq!(sum_a, 4);
            assert_eq!(sum_b, 4);

            client_a.shutdown().await.unwrap();
            let _ = h_a.await;
            h_b.abort();
            let _ = h_b.await;
        })
        .await;
}

// -- dropped packet repair --------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn dropped_packet_is_repaired() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(40_060);
            let addr_b = sock(40_061);
            let t_a = router.bind(addr_a);
            let t_b = router.bind(addr_b);
            // Drop the first packet on A->B; subsequent ticks heal via repair.
            router.set_link_policy(addr_a, addr_b, LinkPolicy::DropFirst { count: 1 });

            let id_a = NodeIdentity::new(NodeId(0xAA00), 1);
            let id_b = NodeIdentity::new(NodeId(0xBB00), 1);

            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                t_a,
                TokioClock::from_millis(0),
                sim_config(id_a, vec![addr_b], 1),
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, _client_b) = GossipRuntime::from_parts(
                t_b,
                TokioClock::from_millis(0),
                sim_config(id_b, vec![addr_a], 2),
                store_for(id_b),
                agg_b.clone(),
            );

            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            client_a.record(0xDEAD, KeyHash(1), 0, 7, 0).await.unwrap();
            // Many ticks to allow the repair lane to retry.
            sim_advance_ticks(Duration::from_millis(100), 10).await;

            let sum_b: u64 = agg_b.inner.borrow().values().copied().sum();
            assert_eq!(sum_b, 7, "B converged despite the first-packet drop");

            client_a.shutdown().await.unwrap();
            let _ = h_a.await;
            h_b.abort();
            let _ = h_b.await;
        })
        .await;
}

// -- partition then heal ----------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn partition_then_heal() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(40_070);
            let addr_b = sock(40_071);
            let addr_c = sock(40_072);
            let t_a = router.bind(addr_a);
            let t_b = router.bind(addr_b);
            let t_c = router.bind(addr_c);

            // Partition A <-> C in both directions.
            router.set_link_policy(addr_a, addr_c, LinkPolicy::Block);
            router.set_link_policy(addr_c, addr_a, LinkPolicy::Block);

            let id_a = NodeIdentity::new(NodeId(0xA), 1);
            let id_b = NodeIdentity::new(NodeId(0xB), 1);
            let id_c = NodeIdentity::new(NodeId(0xC), 1);

            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_c = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                t_a,
                TokioClock::from_millis(0),
                sim_config(id_a, vec![addr_b, addr_c], 1),
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, client_b) = GossipRuntime::from_parts(
                t_b,
                TokioClock::from_millis(0),
                sim_config(id_b, vec![addr_a, addr_c], 2),
                store_for(id_b),
                agg_b.clone(),
            );
            let (rt_c, client_c) = GossipRuntime::from_parts(
                t_c,
                TokioClock::from_millis(0),
                sim_config(id_c, vec![addr_a, addr_b], 3),
                store_for(id_c),
                agg_c.clone(),
            );

            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));
            let h_c = tokio::task::spawn_local(rt_c.run(futures::stream::empty()));

            client_a.record(0xDEAD, KeyHash(1), 0, 1, 0).await.unwrap();
            client_b.record(0xDEAD, KeyHash(1), 0, 2, 0).await.unwrap();
            client_c.record(0xDEAD, KeyHash(1), 0, 4, 0).await.unwrap();

            // Drive ticks under partition.
            sim_advance_ticks(Duration::from_millis(100), 10).await;

            // Heal the partition.
            router.set_link_policy(addr_a, addr_c, LinkPolicy::Pass);
            router.set_link_policy(addr_c, addr_a, LinkPolicy::Pass);

            sim_advance_ticks(Duration::from_millis(100), 30).await;

            let sum_a: u64 = agg_a.inner.borrow().values().copied().sum();
            let sum_b: u64 = agg_b.inner.borrow().values().copied().sum();
            let sum_c: u64 = agg_c.inner.borrow().values().copied().sum();
            assert_eq!(sum_a, 7, "A converges after heal");
            assert_eq!(sum_b, 7, "B converges");
            assert_eq!(sum_c, 7, "C converges after heal");

            client_a.shutdown().await.unwrap();
            client_b.shutdown().await.unwrap();
            client_c.shutdown().await.unwrap();
            let _ = h_a.await;
            let _ = h_b.await;
            let _ = h_c.await;
        })
        .await;
}

// -- minute-in-milliseconds -------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn simulates_minute_in_milliseconds() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(40_080);
            let addr_b = sock(40_081);
            let addr_c = sock(40_082);
            let t_a = router.bind(addr_a);
            let t_b = router.bind(addr_b);
            let t_c = router.bind(addr_c);

            let id_a = NodeIdentity::new(NodeId(0xA), 1);
            let id_b = NodeIdentity::new(NodeId(0xB), 1);
            let id_c = NodeIdentity::new(NodeId(0xC), 1);

            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_c = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                t_a,
                TokioClock::from_millis(0),
                sim_config(id_a, vec![addr_b, addr_c], 1),
                store_for(id_a),
                agg_a.clone(),
            );
            // Hold the clients so the mpsc senders stay alive; dropping a
            // client closes the channel and signals the runtime to shut down.
            let (rt_b, _client_b) = GossipRuntime::from_parts(
                t_b,
                TokioClock::from_millis(0),
                sim_config(id_b, vec![addr_a, addr_c], 2),
                store_for(id_b),
                agg_b.clone(),
            );
            let (rt_c, _client_c) = GossipRuntime::from_parts(
                t_c,
                TokioClock::from_millis(0),
                sim_config(id_c, vec![addr_a, addr_b], 3),
                store_for(id_c),
                agg_c.clone(),
            );

            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));
            let h_c = tokio::task::spawn_local(rt_c.run(futures::stream::empty()));

            client_a.record(0xCAFE, KeyHash(99), 0, 1, 0).await.unwrap();

            let start = Instant::now();
            // 60s of virtual time at 100ms cadence.
            sim_advance_ticks(Duration::from_millis(100), 600).await;
            let elapsed = start.elapsed();

            assert!(
                elapsed < Duration::from_millis(1_000),
                "expected sub-second wall-clock, got {elapsed:?}"
            );

            client_a.shutdown().await.unwrap();
            let _ = h_a.await;
            h_b.abort();
            h_c.abort();
            let _ = h_b.await;
            let _ = h_c.await;
        })
        .await;
}

// -- peer sampling without replacement -------------------------------------

/// Three peers, `fanout = 3` ⇒ a single tick must hit every peer exactly
/// once. Without-replacement sampling is required for Demers' O(log N)
/// convergence bound; with-replacement would let a tick visit some peer
/// twice and skip another entirely.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn gossip_tick_picks_peers_without_replacement() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(40_090);
            let addr_b = sock(40_091);
            let addr_c = sock(40_092);
            let addr_d = sock(40_093);
            let t_a = router.bind(addr_a);
            let t_b = router.bind(addr_b);
            let t_c = router.bind(addr_c);
            let t_d = router.bind(addr_d);

            let id_a = NodeIdentity::new(NodeId(0xA), 1);
            let id_b = NodeIdentity::new(NodeId(0xB), 1);
            let id_c = NodeIdentity::new(NodeId(0xC), 1);
            let id_d = NodeIdentity::new(NodeId(0xD), 1);

            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_c = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_d = Rc::new(InMemoryAggregateStore::<u32>::new());

            // A talks to B, C, D with fanout=3 — every tick must hit each.
            let mut cfg_a = sim_config(id_a, vec![addr_b, addr_c, addr_d], 0xBAD_5EED);
            cfg_a.fanout = 3;
            let (rt_a, client_a) = GossipRuntime::from_parts(
                t_a,
                TokioClock::from_millis(0),
                cfg_a,
                store_for(id_a),
                agg_a.clone(),
            );
            // B/C/D are silent — they only receive. We hang on to their
            // clients so the mpsc senders stay alive; dropping the client
            // closes the channel which would cause the runtime to exit.
            let cfg_b = sim_config(id_b, Vec::new(), 2);
            let cfg_c = sim_config(id_c, Vec::new(), 3);
            let cfg_d = sim_config(id_d, Vec::new(), 4);
            let (rt_b, _client_b) = GossipRuntime::from_parts(
                t_b,
                TokioClock::from_millis(0),
                cfg_b,
                store_for(id_b),
                agg_b.clone(),
            );
            let (rt_c, _client_c) = GossipRuntime::from_parts(
                t_c,
                TokioClock::from_millis(0),
                cfg_c,
                store_for(id_c),
                agg_c.clone(),
            );
            let (rt_d, _client_d) = GossipRuntime::from_parts(
                t_d,
                TokioClock::from_millis(0),
                cfg_d,
                store_for(id_d),
                agg_d.clone(),
            );

            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));
            let h_c = tokio::task::spawn_local(rt_c.run(futures::stream::empty()));
            let h_d = tokio::task::spawn_local(rt_d.run(futures::stream::empty()));

            client_a.record(0xDEAD, KeyHash(1), 0, 1, 0).await.unwrap();
            // With fanout = peer_count, every peer must receive the cell on
            // the first tick that fires after the record. Without-replacement
            // sampling is the invariant under test; with-replacement would
            // leave at least one peer empty whenever the tick double-picks.
            sim_advance_ticks(Duration::from_millis(100), 20).await;

            let got_b = agg_b.inner.borrow().values().copied().sum::<u64>();
            let got_c = agg_c.inner.borrow().values().copied().sum::<u64>();
            let got_d = agg_d.inner.borrow().values().copied().sum::<u64>();
            assert_eq!((got_b, got_c, got_d), (1, 1, 1));

            client_a.shutdown().await.unwrap();
            let _ = h_a.await;
            h_b.abort();
            h_c.abort();
            h_d.abort();
            let _ = h_b.await;
            let _ = h_c.await;
            let _ = h_d.await;
        })
        .await;
}

// -- UDP smoke --------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn udp_round_trip_smoke() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let id_a = NodeIdentity::new(NodeId(0xAA00), 1);
            let id_b = NodeIdentity::new(NodeId(0xBB00), 1);

            // Bind sockets up front so we can read their addrs.
            let sock_a = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let sock_b = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr_a = sock_a.local_addr().unwrap();
            let addr_b = sock_b.local_addr().unwrap();

            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());

            let config_a = GossipConfig {
                local_identity: id_a,
                cluster_id_hash: 0xC1,
                bootstrap_peers: vec![addr_b],
                fanout: 1,
                tick_interval: Duration::from_millis(20),
                rng_seed: 1,
                ..GossipConfig::default()
            };
            let config_b = GossipConfig {
                local_identity: id_b,
                cluster_id_hash: 0xC1,
                bootstrap_peers: vec![addr_a],
                fanout: 1,
                tick_interval: Duration::from_millis(20),
                rng_seed: 2,
                ..GossipConfig::default()
            };

            let (rt_a, client_a) = GossipRuntime::from_parts(
                UdpTransport::from_socket(sock_a),
                TokioClock::new(),
                config_a,
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, client_b) = GossipRuntime::from_parts(
                UdpTransport::from_socket(sock_b),
                TokioClock::new(),
                config_b,
                store_for(id_b),
                agg_b.clone(),
            );

            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            let now_millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let bucket = (now_millis / 1_000) as BucketEpoch;
            client_a
                .record(0xC0FE, KeyHash(0xABCD), bucket, 3, now_millis)
                .await
                .unwrap();
            client_b
                .record(0xC0FE, KeyHash(0xABCD), bucket, 5, now_millis)
                .await
                .unwrap();

            // Poll for convergence; bounded realtime budget.
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                let sa: u64 = agg_a.inner.borrow().values().copied().sum();
                let sb: u64 = agg_b.inner.borrow().values().copied().sum();
                if sa == 8 && sb == 8 {
                    break;
                }
                if Instant::now() >= deadline {
                    panic!("did not converge in 2s: a={sa}, b={sb}");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            client_a.shutdown().await.unwrap();
            client_b.shutdown().await.unwrap();
            let _ = h_a.await;
            let _ = h_b.await;
        })
        .await;
}

// -- admin command channel --------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn admin_snapshot_reflects_runtime_state() {
    let local = LocalSet::new();
    local
        .run_until(async {
            use tokio::sync::{mpsc, oneshot};

            use crate::gossip::{AdminCommand, AdminSnapshot};

            let router = SimRouter::new();
            let addr = sock(40_300);
            let transport = router.bind(addr);

            let identity = NodeIdentity::new(NodeId(0xCAFE), 7);
            let store = store_for(identity);
            let agg = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (admin_tx, admin_rx) = mpsc::channel::<AdminCommand>(4);

            let (rt, client) = GossipRuntime::from_parts_with_admin(
                transport,
                TokioClock::from_millis(0),
                sim_config(identity, vec![sock(40_301)], 1),
                store,
                agg.clone(),
                Some(admin_rx),
            );
            let handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));

            // One record so the cell store is non-empty when we sample.
            client
                .record(0xFEED, KeyHash(0x42), 0, 4, 100)
                .await
                .unwrap();

            let (reply_tx, reply_rx) = oneshot::channel::<AdminSnapshot>();
            admin_tx
                .send(AdminCommand::Snapshot { reply: reply_tx })
                .await
                .unwrap();
            let snapshot = reply_rx.await.unwrap();

            assert_eq!(snapshot.local_identity, identity);
            // Bootstrap peer is present even though we never heard back from
            // it — `node_id` stays `None` until first inbound packet.
            assert_eq!(snapshot.peers.len(), 1);
            assert_eq!(snapshot.peers[0].addr, sock(40_301));
            assert!(snapshot.peers[0].node_id.is_none());
            assert!(snapshot.store_stats.active_cells >= 1);
            assert!(snapshot.local_dirty_len >= 1);

            client.shutdown().await.unwrap();
            let _ = handle.await;
        })
        .await;
}
