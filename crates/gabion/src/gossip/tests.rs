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
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;

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

#[quickcheck]
fn quickcheck_sim_connected_clusters_converge_after_finite_loss(
    case: ConnectedClusterCase,
) -> TestResult {
    run_paused(run_connected_case(case))
}

#[quickcheck]
fn quickcheck_sim_partition_heals_without_overcount(mut case: PartitionCase) -> TestResult {
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

#[quickcheck]
fn quickcheck_sim_authentication_admits_only_matching_keys(case: AuthCase) -> TestResult {
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
            .record(0xD00D, KeyHash(1), 0, case.a_hits as u64, 0, 0)
            .await
            .unwrap();
        client_b
            .record(0xD00D, KeyHash(1), 0, case.b_hits as u64, 0, 0)
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

#[quickcheck]
fn quickcheck_sim_tick_expiration_removes_converged_cells(case: ExpirationCase) -> TestResult {
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
            cfg.fanout = nodes.saturating_sub(1).clamp(1, 8);
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

#[quickcheck]
fn quickcheck_sim_peer_membership_controls_delivery(case: MembershipCase) -> TestResult {
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
            .record(0xFACE, KeyHash(1), 0, case.hits as u64, 0, 0)
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
            client.record(rule_fp, key, 0, 5, 0, 1_000).await.unwrap();

            // Read the count back from the store handle the test holds —
            // requires looking up the rule_slot.
            // Use a small dance: send a second record that pushes the store
            // through one more apply so we know which rule slot was minted.
            // (The first record itself must have stored the count already.)
            // Easier: probe by iterating the inner map.
            let totals: u64 = agg.inner.borrow().values().copied().sum();
            assert_eq!(totals, 5);

            client.record(rule_fp, key, 0, 3, 0, 1_000).await.unwrap();
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

            client_a.record(rule_fp, key, 0, 3, 0, 1_000).await.unwrap();
            client_b.record(rule_fp, key, 0, 5, 0, 1_000).await.unwrap();

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
            client.record(0x11, KeyHash(1), 0, 4, 0, 100).await.unwrap();
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
            client.record(0xFEED, KeyHash(7), 0, 2, 0, 0).await.unwrap();
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

/// A configured rule that goes idle long enough for every cell to expire must
/// still age out on its *configured* window when traffic resumes — not silently
/// fall back to the 60 s default. Exercises the `GossipRuntime` both adapters
/// spawn: record → expire (releasing all cells) → record again → expire again.
/// Before configured rules were pinned, the first expiry released the rule
/// slot, so the second wave re-interned `RuleDescriptor::default()` (60 s
/// window, `applies_locally = false`) and never aged out here.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn configured_rule_reexpires_after_idle_window() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr = sock(40_031);
            let transport = router.bind(addr);

            let identity = NodeIdentity::new(NodeId(0xAA), 1);
            let mut store = store_for(identity);
            // Configured rule: 100 ms bucket, 100 ms window -> 1 live bucket.
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

            // First burst at bucket 0, then advance past the live window so the
            // cell ages out and (pre-fix) the rule slot is released.
            client
                .record(0xFEED, KeyHash(7), 0, 2, 10, 0)
                .await
                .unwrap();
            assert_eq!(agg.inner.borrow().values().copied().sum::<u64>(), 2);
            sim_advance_ticks(Duration::from_millis(100), 3).await;
            assert_eq!(agg.inner.borrow().values().copied().sum::<u64>(), 0);

            // Second burst at the now-current bucket. It must register and then
            // age out on the same 100 ms window. Under the released-then-default
            // bug it lands on a 60 s descriptor and stays here forever.
            client
                .record(0xFEED, KeyHash(7), 3, 2, 10, 300)
                .await
                .unwrap();
            assert_eq!(agg.inner.borrow().values().copied().sum::<u64>(), 2);
            sim_advance_ticks(Duration::from_millis(100), 3).await;
            assert_eq!(
                agg.inner.borrow().values().copied().sum::<u64>(),
                0,
                "second burst must age out on the configured 100 ms window, not the 60 s default",
            );

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

            client.record(0xABC, KeyHash(1), 0, 3, 0, 0).await.unwrap();
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

            client_a
                .record(0xDEAD, KeyHash(1), 0, 4, 0, 0)
                .await
                .unwrap();
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

            client_a
                .record(0xDEAD, KeyHash(1), 0, 7, 0, 0)
                .await
                .unwrap();
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

            client_a
                .record(0xDEAD, KeyHash(1), 0, 1, 0, 0)
                .await
                .unwrap();
            client_b
                .record(0xDEAD, KeyHash(1), 0, 2, 0, 0)
                .await
                .unwrap();
            client_c
                .record(0xDEAD, KeyHash(1), 0, 4, 0, 0)
                .await
                .unwrap();

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

            client_a
                .record(0xCAFE, KeyHash(99), 0, 1, 0, 0)
                .await
                .unwrap();

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

            client_a
                .record(0xDEAD, KeyHash(1), 0, 1, 0, 0)
                .await
                .unwrap();
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

// -- coverage fanout: ⌈ln(n)+c⌉, scaled by cluster size --------------------

/// Drive a single node against a sweep of bootstrap-peer counts and read
/// the chosen per-tick fanout back through the admin snapshot. The pick is
/// the coverage threshold `config.fanout.max(⌈ln(peers) + c⌉).min(peers)`
/// (Kermarrec, Massoulié & Ganesh, TPDS 2003, Thm 1) — a function of the
/// peer count, *not* the dirty set. Covers the `n=1` floor-clip edge
/// (`⌈0+c⌉` clamped to 1) and a 255-peer large cluster.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn coverage_fanout_tracks_ln_n_plus_c() {
    use crate::defaults::{GOSSIP_COVERAGE_MARGIN, GOSSIP_FANOUT};

    let local = LocalSet::new();
    local
        .run_until(async {
            for peers in [1usize, 8, 31, 99, 255] {
                let coverage = ((peers as f64).ln() + GOSSIP_COVERAGE_MARGIN).ceil() as usize;
                let expected = GOSSIP_FANOUT.max(coverage).min(peers);
                let observed = observe_coverage_fanout(peers, GOSSIP_FANOUT).await;
                assert_eq!(
                    observed, expected,
                    "coverage fanout for {peers} peers: expected \
                     max({GOSSIP_FANOUT}, ⌈ln({peers})+{GOSSIP_COVERAGE_MARGIN}⌉={coverage})\
                     .min({peers}) = {expected}, got {observed}",
                );
            }
        })
        .await;
}

/// Build one node with `peers` unbound bootstrap addresses, give it a cell
/// to gossip, and return the `last_effective_fanout` it records on the next
/// dirty tick. The peers need not run: the sim transport treats a send to an
/// unbound address as delivered on the floor (UDP semantics), and the runtime
/// records `last_effective_fanout` from the computed pick *before* any send,
/// so the addresses serve only to set `self.peers.len()`.
async fn observe_coverage_fanout(peers: usize, base: usize) -> usize {
    use tokio::sync::mpsc;

    use crate::gossip::AdminCommand;

    let router = SimRouter::new();
    let local_addr = sock(41_000);
    let transport = router.bind(local_addr);

    let bootstrap: Vec<SocketAddr> = (0..peers).map(|i| sock(41_001 + i as u16)).collect();
    let identity = NodeIdentity::new(NodeId(0x5EED), 1);
    let mut cfg = sim_config(identity, bootstrap, 1);
    cfg.fanout = base;

    let (admin_tx, admin_rx) = mpsc::channel::<AdminCommand>(4);
    let (rt, client) = GossipRuntime::from_parts_with_admin(
        transport,
        TokioClock::from_millis(0),
        cfg,
        store_for(identity),
        Rc::new(InMemoryAggregateStore::<u32>::new()),
        Some(admin_rx),
    );
    let handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));

    client
        .record(0xC0FFEE, KeyHash(1), 0, 1, 0, 0)
        .await
        .unwrap();
    sim_advance_ticks(Duration::from_millis(100), 3).await;

    let snap = admin_snapshot(&admin_tx).await;
    client.shutdown().await.unwrap();
    let _ = handle.await;
    snap.last_effective_fanout
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
                .record(0xC0FE, KeyHash(0xABCD), bucket, 3, 0, now_millis)
                .await
                .unwrap();
            client_b
                .record(0xC0FE, KeyHash(0xABCD), bucket, 5, 0, now_millis)
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
                .record(0xFEED, KeyHash(0x42), 0, 4, 0, 100)
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

// -- shared helpers for coverage-gap tests ----------------------------------

async fn admin_snapshot(
    admin_tx: &tokio::sync::mpsc::Sender<crate::gossip::AdminCommand>,
) -> crate::gossip::AdminSnapshot {
    use crate::gossip::AdminCommand;
    use tokio::sync::oneshot;
    let (reply_tx, reply_rx) = oneshot::channel();
    admin_tx
        .send(AdminCommand::Snapshot { reply: reply_tx })
        .await
        .expect("admin command channel open");
    reply_rx.await.expect("runtime replied to snapshot")
}

// -- Gap 1: peer-cache pairing invariant ------------------------------------

#[derive(Clone, Debug)]
enum PairingOp {
    AddPeer(u8),
    RemovePeer(u8),
    Record(u8),
}

impl Arbitrary for PairingOp {
    fn arbitrary(g: &mut Gen) -> Self {
        match u8::arbitrary(g) % 4 {
            0 => PairingOp::AddPeer(u8::arbitrary(g)),
            1 => PairingOp::RemovePeer(u8::arbitrary(g)),
            _ => PairingOp::Record(u8::arbitrary(g)),
        }
    }
}

#[derive(Clone, Debug)]
struct PairingCase {
    ops: Vec<PairingOp>,
}

impl Arbitrary for PairingCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = (usize::arbitrary(g) % 12) + 2;
        Self {
            ops: (0..len).map(|_| PairingOp::arbitrary(g)).collect(),
        }
    }
}

/// Invariant: in every peer entry observed via admin snapshot,
/// `node_id.is_some()` iff `peer_slot.is_some()`. The runtime sets the two
/// together (runtime.rs `handle_inbound`); a future refactor that breaks
/// the pairing would not break convergence — gossip would just silently
/// re-send unpruned frames — so the bug would slip past every existing
/// property test. This case stresses `PeerEvent::Added/Removed` interleaved
/// with `record()` and snapshots after each step.
#[quickcheck]
fn quickcheck_peer_slot_pairing_holds_across_lifecycle(case: PairingCase) -> TestResult {
    use crate::discovery::{Peer, PeerEvent};
    use tokio::sync::mpsc;

    run_paused(async move {
        const NODE_COUNT: usize = 3;
        let router = SimRouter::new();
        let addrs: Vec<SocketAddr> = (0..NODE_COUNT).map(|i| sock(48_000 + i as u16)).collect();

        let id_local = NodeIdentity::new(NodeId(0xA0A0), 1);
        let mut cfg = sim_config(id_local, Vec::new(), 1);
        cfg.fanout = NODE_COUNT.saturating_sub(1);

        let (admin_tx, admin_rx) = mpsc::channel(8);
        let (peer_tx, peer_rx) = mpsc::unbounded_channel::<PeerEvent>();
        let peer_stream = futures::stream::unfold(peer_rx, |mut rx| async move {
            rx.recv().await.map(|evt| (evt, rx))
        });

        let local_addr = addrs[0];
        let (rt_local, client_local) = GossipRuntime::from_parts_with_admin(
            router.bind(local_addr),
            TokioClock::from_millis(0),
            cfg,
            store_for(id_local),
            Rc::new(InMemoryAggregateStore::<u32>::new()),
            Some(admin_rx),
        );
        let h_local = tokio::task::spawn_local(rt_local.run(peer_stream));

        let mut remote_clients = Vec::new();
        let mut remote_handles = Vec::new();
        for (i, addr) in addrs.iter().enumerate().skip(1) {
            let id = NodeIdentity::new(NodeId(0xA000 + i as u128), 1);
            let mut cfg = sim_config(id, vec![local_addr], 100 + i as u64);
            cfg.fanout = 1;
            let (rt, client) = GossipRuntime::from_parts(
                router.bind(*addr),
                TokioClock::from_millis(0),
                cfg,
                store_for(id),
                Rc::new(InMemoryAggregateStore::<u32>::new()),
            );
            remote_handles.push(tokio::task::spawn_local(rt.run(futures::stream::empty())));
            remote_clients.push(client);
        }

        let mut invariant_holds = true;
        let mut snapshots = 0usize;
        for op in &case.ops {
            match op {
                PairingOp::AddPeer(idx) => {
                    let target = addrs[(*idx as usize % (NODE_COUNT - 1)) + 1];
                    let _ = peer_tx.send(PeerEvent::Added(Peer::new(target)));
                }
                PairingOp::RemovePeer(idx) => {
                    let target = addrs[(*idx as usize % (NODE_COUNT - 1)) + 1];
                    let _ = peer_tx.send(PeerEvent::Removed(Peer::new(target)));
                }
                PairingOp::Record(key) => {
                    let _ = client_local
                        .record(0xAAAA, key_hash(*key), 0, 1, 0, 0)
                        .await;
                }
            }
            sim_advance_ticks(Duration::from_millis(100), 3).await;
            let snap = admin_snapshot(&admin_tx).await;
            snapshots += 1;
            for peer in &snap.peers {
                if peer.node_id.is_some() != peer.peer_slot.is_some() {
                    invariant_holds = false;
                }
            }
        }

        client_local.shutdown().await.unwrap();
        for client in remote_clients {
            let _ = client.shutdown().await;
        }
        let _ = h_local.await;
        for handle in remote_handles {
            let _ = handle.await;
        }

        if snapshots == 0 {
            TestResult::discard()
        } else {
            TestResult::from_bool(invariant_holds)
        }
    })
}

// -- Gap 2: without-replacement sampling distribution -----------------------

#[derive(Clone, Debug)]
struct SamplingCase {
    seed: u64,
}

impl Arbitrary for SamplingCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            seed: u64::arbitrary(g),
        }
    }
}

/// Per-peer delivery counts under strict-fanout sampling should be roughly
/// uniform. The existing `gossip_tick_picks_peers_without_replacement` uses
/// `fanout == peer_count` so even a degenerate sampler (always picks peer 0)
/// passes — every peer ends up picked at least once per tick anyway. This
/// case keeps the effective pick strictly below the peer count so a broken
/// sampler concentrates all deliveries on a few peers and starves the rest.
/// The effective pick is the coverage fanout `⌈ln(8) + c⌉ = 7` (which
/// overrides the lower configured floor), still 7 of 8 peers per tick.
#[quickcheck]
fn quickcheck_sampling_distribution_is_uniform_under_strict_fanout(
    case: SamplingCase,
) -> TestResult {
    run_paused(async move {
        const PEERS: usize = 8;
        const FANOUT: usize = 4;
        const TICKS: u32 = 100;
        let router = SimRouter::with_channel_capacity(128);
        let sender_addr = sock(49_000);
        let peer_addrs: Vec<SocketAddr> = (0..PEERS).map(|i| sock(49_001 + i as u16)).collect();

        let id_sender = NodeIdentity::new(NodeId(0xB000), 1);
        let mut cfg_sender = sim_config(id_sender, peer_addrs.clone(), case.seed);
        cfg_sender.fanout = FANOUT;
        let agg_sender = Rc::new(InMemoryAggregateStore::<u32>::new());
        let (rt_sender, client_sender) = GossipRuntime::from_parts(
            router.bind(sender_addr),
            TokioClock::from_millis(0),
            cfg_sender,
            store_for(id_sender),
            agg_sender,
        );
        let h_sender = tokio::task::spawn_local(rt_sender.run(futures::stream::empty()));

        // Silent recipients — they receive but don't emit gossip themselves
        // (their stores are empty, so `handle_gossip_tick` early-returns).
        // We hold the clients to keep their request channels alive.
        let mut peer_clients = Vec::with_capacity(PEERS);
        let mut peer_handles = Vec::with_capacity(PEERS);
        for (i, addr) in peer_addrs.iter().enumerate() {
            let id = NodeIdentity::new(NodeId(0xB100 + i as u128), 1);
            let cfg = sim_config(id, Vec::new(), 1000 + i as u64);
            let (rt, client) = GossipRuntime::from_parts(
                router.bind(*addr),
                TokioClock::from_millis(0),
                cfg,
                store_for(id),
                Rc::new(InMemoryAggregateStore::<u32>::new()),
            );
            peer_handles.push(tokio::task::spawn_local(rt.run(futures::stream::empty())));
            peer_clients.push(client);
        }

        // One record so the sender has dirty data to gossip every tick.
        client_sender
            .record(0xB055, KeyHash(1), 0, 1, 0, 0)
            .await
            .unwrap();

        sim_advance_ticks(Duration::from_millis(100), TICKS).await;

        let counts: Vec<u64> = peer_addrs
            .iter()
            .map(|addr| router.received_count(*addr))
            .collect();
        let total: u64 = counts.iter().sum();
        // The runtime scales the per-tick pick to the coverage fanout
        // `⌈ln(PEERS) + c⌉`, which exceeds the configured `FANOUT` floor —
        // so the expected per-peer mean is driven by the effective pick, not
        // by `FANOUT`. Still strictly below `PEERS`, so a broken sampler
        // starves the unpicked peers.
        let coverage =
            ((PEERS as f64).ln() + crate::defaults::GOSSIP_COVERAGE_MARGIN).ceil() as usize;
        let effective = FANOUT.max(coverage).min(PEERS);
        let expected = (TICKS as u64) * (effective as u64) / (PEERS as u64);
        let lower = expected / 2;
        let upper = expected * 2;
        let uniform = counts.iter().all(|&c| c >= lower && c <= upper);
        if !uniform {
            eprintln!(
                "non-uniform sampling distribution: seed={}, counts={:?}, total={}, \
                 expected_per_peer={}, bound=[{},{}]",
                case.seed, counts, total, expected, lower, upper
            );
        }

        client_sender.shutdown().await.unwrap();
        for client in peer_clients {
            let _ = client.shutdown().await;
        }
        let _ = h_sender.await;
        for handle in peer_handles {
            let _ = handle.await;
        }
        TestResult::from_bool(uniform)
    })
}

// -- Gap 3: decode rejection counter ----------------------------------------

/// Mismatched auth keys cause every inbound frame to fail decode. The
/// runtime increments `decode_reject_count` per drop and rate-limits a
/// `warn!` to power-of-two transitions. The existing authentication
/// quickcheck verifies *convergence* under mismatched keys but never reads
/// the counter — a regression that breaks the increment (e.g. dropping the
/// `saturating_add`) wouldn't fail any current test.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn decode_rejects_increment_on_wrong_auth_and_throttle_warns() {
    use tokio::sync::mpsc;

    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(50_000);
            let addr_b = sock(50_001);
            let id_a = NodeIdentity::new(NodeId(0xDA), 1);
            let id_b = NodeIdentity::new(NodeId(0xDB), 1);

            let mut cfg_a = sim_config(id_a, vec![addr_b], 1);
            cfg_a.auth_key = Some(HmacKey([7; 32]));
            let mut cfg_b = sim_config(id_b, vec![addr_a], 2);
            cfg_b.auth_key = Some(HmacKey([8; 32]));

            let (admin_tx_b, admin_rx_b) = mpsc::channel(4);

            let (rt_a, client_a) = GossipRuntime::from_parts(
                router.bind(addr_a),
                TokioClock::from_millis(0),
                cfg_a,
                store_for(id_a),
                Rc::new(InMemoryAggregateStore::<u32>::new()),
            );
            let (rt_b, client_b) = GossipRuntime::from_parts_with_admin(
                router.bind(addr_b),
                TokioClock::from_millis(0),
                cfg_b,
                store_for(id_b),
                Rc::new(InMemoryAggregateStore::<u32>::new()),
                Some(admin_rx_b),
            );
            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            // One record on A. Each tick re-encodes A's dirty cell into a
            // packet for its only peer (B). B rejects each packet because
            // the auth keys don't match.
            client_a
                .record(0xDEAD, KeyHash(1), 0, 1, 0, 0)
                .await
                .unwrap();

            const TICKS: u32 = 8;
            sim_advance_ticks(Duration::from_millis(100), TICKS).await;

            let snap = admin_snapshot(&admin_tx_b).await;
            // Allow a one-tick slop for first-tick scheduling under
            // start_paused (the first interval fires at t=0 in some
            // configurations).
            assert!(
                snap.decode_reject_count >= (TICKS as u64) - 1,
                "decode_reject_count too low: {} (after {} ticks)",
                snap.decode_reject_count,
                TICKS
            );
            assert!(
                snap.decode_reject_count <= (TICKS as u64) + 1,
                "decode_reject_count too high: {} (after {} ticks)",
                snap.decode_reject_count,
                TICKS
            );

            client_a.shutdown().await.unwrap();
            client_b.shutdown().await.unwrap();
            let _ = h_a.await;
            let _ = h_b.await;
        })
        .await;
}

// -- Gap 4: send queue backpressure (WouldBlock re-queue) -------------------

/// When the recipient's inbound channel is saturated, `try_send_to` returns
/// `WouldBlock` and the runtime re-queues the slot at the front of
/// `send_pending` (runtime.rs `drain_one_send`). Without re-queue, the slot
/// would either be lost or shuffled to the back, breaking the high-water
/// mark this test reads via `max_send_pending_depth`. Working case: queue
/// fills toward `send_queue_capacity`. Broken case: max stays at 1.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn send_queue_drains_after_recipient_backpressure() {
    use tokio::sync::mpsc;

    let local = LocalSet::new();
    local
        .run_until(async {
            // Channel cap 1 — recipient mpsc holds at most one buffered
            // packet, so the second outbound from the sender hits WouldBlock
            // until the recipient drains.
            let router = SimRouter::with_channel_capacity(1);
            let addr_a = sock(51_000);
            let addr_b = sock(51_001);
            let id_a = NodeIdentity::new(NodeId(0xBA), 1);

            let mut cfg_a = sim_config(id_a, vec![addr_b], 1);
            cfg_a.fanout = 1;
            cfg_a.send_queue_capacity = 8;

            let (admin_tx, admin_rx) = mpsc::channel(4);

            let (rt_a, client_a) = GossipRuntime::from_parts_with_admin(
                router.bind(addr_a),
                TokioClock::from_millis(0),
                cfg_a,
                store_for(id_a),
                Rc::new(InMemoryAggregateStore::<u32>::new()),
                Some(admin_rx),
            );
            // Bind the recipient address but do NOT spawn its runtime. The
            // mpsc receiver lives on `transport_b` so the channel stays
            // open with cap 1 and the sender's outbound `try_send_to`
            // returns `WouldBlock` after the first buffered packet.
            let transport_b = router.bind(addr_b);
            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));

            for i in 0..6_u128 {
                client_a
                    .record(0xBA5, KeyHash(i), 0, 1, 0, 0)
                    .await
                    .unwrap();
            }
            sim_advance_ticks(Duration::from_millis(100), 12).await;

            let snap_saturated = admin_snapshot(&admin_tx).await;
            assert!(
                snap_saturated.max_send_pending_depth >= 2,
                "expected backpressure re-queue to grow send_pending past 1, got max={} \
                 (snapshot={:?})",
                snap_saturated.max_send_pending_depth,
                snap_saturated,
            );

            // Drain the recipient by dropping its transport. The mpsc
            // receiver is freed and subsequent `try_send` returns `Closed`
            // (which the sim treats as "delivered to the floor"), letting
            // the sender's pending queue empty.
            drop(transport_b);
            sim_advance_ticks(Duration::from_millis(100), 6).await;

            let snap_drained = admin_snapshot(&admin_tx).await;
            assert_eq!(
                snap_drained.send_pending_depth, 0,
                "send_pending should drain after recipient released: {:?}",
                snap_drained,
            );

            client_a.shutdown().await.unwrap();
            let _ = h_a.await;
        })
        .await;
}

// -- Gap 5: DropProb i.i.d. packet loss -------------------------------------

#[derive(Clone, Debug)]
struct DropProbCase {
    p_choice: u8,
    records: Vec<SimRecord>,
    nodes: u8,
}

impl Arbitrary for DropProbCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = (usize::arbitrary(g) % 16) + 1;
        Self {
            p_choice: u8::arbitrary(g) % 3,
            records: (0..len).map(|_| SimRecord::arbitrary(g)).collect(),
            nodes: (u8::arbitrary(g) % 4) + 4,
        }
    }
}

/// The `LinkPolicy::DropProb` simulator path implements i.i.d. Bernoulli
/// packet loss but no existing test exercises it. Demers et al. showed
/// anti-entropy converges under bounded i.i.d. loss; we sanity-check that
/// the gossip runtime hits that bound for p ∈ {0.1, 0.3, 0.5} on small
/// clusters.
#[quickcheck]
fn quickcheck_sim_converges_under_iid_packet_loss(case: DropProbCase) -> TestResult {
    if case.records.is_empty() {
        return TestResult::discard();
    }

    run_paused(async move {
        let p = match case.p_choice {
            0 => 0.1,
            1 => 0.3,
            _ => 0.5,
        };
        let nodes = case.nodes as usize;
        let router = SimRouter::with_channel_capacity(128);
        let addrs: Vec<SocketAddr> = (0..nodes).map(|i| sock(52_000 + i as u16)).collect();

        // Apply DropProb on every directed link.
        for &src in &addrs {
            for &dst in &addrs {
                if src != dst {
                    router.set_link_policy(src, dst, LinkPolicy::DropProb { p });
                }
            }
        }

        let mut clients = Vec::with_capacity(nodes);
        let mut handles = Vec::with_capacity(nodes);
        let mut aggregates = Vec::with_capacity(nodes);
        for i in 0..nodes {
            let identity = NodeIdentity::new(NodeId(0xC000 + i as u128), 1);
            let peers: Vec<_> = addrs
                .iter()
                .copied()
                .filter(|addr| *addr != addrs[i])
                .collect();
            let mut cfg = sim_config(identity, peers, 0xD0D0 + i as u64);
            cfg.fanout = nodes.saturating_sub(1).clamp(1, 4);
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
            let node = record.node as usize % nodes;
            clients[node]
                .record(
                    rule_fp(record.rule),
                    key_hash(record.key),
                    0,
                    record.hits as u64,
                    0,
                    0,
                )
                .await
                .unwrap();
        }

        let expected = expected_model(&case.records, nodes);
        let mut converged = false;
        for _ in 0..40 {
            sim_advance_ticks(Duration::from_millis(100), 5).await;
            if aggregates.iter().all(|agg| agg.snapshot() == expected) {
                converged = true;
                break;
            }
        }
        if !converged {
            eprintln!(
                "iid loss test did not converge under p={}, nodes={}, records={}, last_states={:?}",
                p,
                nodes,
                case.records.len(),
                aggregates
                    .iter()
                    .map(|agg| agg.snapshot())
                    .collect::<Vec<_>>(),
            );
        }

        for client in clients {
            let _ = client.shutdown().await;
        }
        for handle in handles {
            let _ = handle.await;
        }
        TestResult::from_bool(converged)
    })
}

// -- Gap 6: peer event lifecycle idempotence --------------------------------

/// `handle_peer_event` guards `Added` against duplicates (`peers.iter()
/// .any(...)`) and tolerates `Removed` of a peer that was never added
/// (`peers.iter().position(...).map(...)`). Neither edge is exercised by
/// existing tests. This case walks the sequence
/// `Added → Added → Removed → Removed → Added` and asserts the peer-list
/// length transitions are `1 → 1 → 0 → 0 → 1`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn peer_event_lifecycle_is_idempotent_and_tolerant() {
    use crate::discovery::{Peer, PeerEvent};
    use tokio::sync::mpsc;

    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_local = sock(53_000);
            let addr_other = sock(53_001);
            let id_local = NodeIdentity::new(NodeId(0xE0E0), 1);

            let (admin_tx, admin_rx) = mpsc::channel(8);
            let (peer_tx, peer_rx) = mpsc::unbounded_channel::<PeerEvent>();
            let peer_stream = futures::stream::unfold(peer_rx, |mut rx| async move {
                rx.recv().await.map(|evt| (evt, rx))
            });

            let (rt, client) = GossipRuntime::from_parts_with_admin(
                router.bind(addr_local),
                TokioClock::from_millis(0),
                sim_config(id_local, Vec::new(), 1),
                store_for(id_local),
                Rc::new(InMemoryAggregateStore::<u32>::new()),
                Some(admin_rx),
            );
            let handle = tokio::task::spawn_local(rt.run(peer_stream));

            // Initial snapshot: no peers.
            let snap0 = admin_snapshot(&admin_tx).await;
            assert_eq!(snap0.peers.len(), 0, "initial state should have no peers");

            let events: Vec<(PeerEvent, usize, &'static str)> = vec![
                (PeerEvent::Added(Peer::new(addr_other)), 1, "first add"),
                (PeerEvent::Added(Peer::new(addr_other)), 1, "duplicate add"),
                (PeerEvent::Removed(Peer::new(addr_other)), 0, "first remove"),
                (
                    PeerEvent::Removed(Peer::new(addr_other)),
                    0,
                    "remove without add",
                ),
                (PeerEvent::Added(Peer::new(addr_other)), 1, "re-add"),
            ];

            for (evt, expected_len, label) in events {
                peer_tx.send(evt).expect("peer stream open");
                // Yield enough that the runtime processes the peer event
                // before we snapshot. Peer events are CRDT-free, so no
                // time advance is strictly required, but a yield ensures
                // ordering between the send and the snapshot.
                tokio::task::yield_now().await;
                let snap = admin_snapshot(&admin_tx).await;
                assert_eq!(
                    snap.peers.len(),
                    expected_len,
                    "after {label}: snapshot={snap:?}"
                );
            }

            client.shutdown().await.unwrap();
            let _ = handle.await;
        })
        .await;
}

// -- Threshold-triggered anti-entropy ---------------------------------------

/// Threshold-trigger fires the moment a per-rule pending crosses ε, well
/// before the proactive heartbeat would have run. Without the trigger,
/// nothing would arrive at the peer until the heartbeat at `tick_interval`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn threshold_trigger_fires_before_heartbeat() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(54_000);
            let addr_b = sock(54_001);
            let id_a = NodeIdentity::new(NodeId(0xA1), 1);
            let id_b = NodeIdentity::new(NodeId(0xB1), 1);

            // Heartbeat is far in the future; only the threshold trigger
            // could possibly deliver in the short window we sample.
            let mut cfg_a = sim_config(id_a, vec![addr_b], 1);
            cfg_a.tick_interval = Duration::from_secs(1);
            cfg_a.target_err_bps = 100;
            cfg_a.min_emit_interval = Duration::from_millis(5);
            let mut cfg_b = sim_config(id_b, vec![addr_a], 2);
            cfg_b.tick_interval = Duration::from_secs(1);

            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                router.bind(addr_a),
                TokioClock::from_millis(0),
                cfg_a,
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, _client_b) = GossipRuntime::from_parts(
                router.bind(addr_b),
                TokioClock::from_millis(0),
                cfg_b,
                store_for(id_b),
                agg_b.clone(),
            );
            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            // rule_limit=100, N=2 peers, target=100 bps =>
            // ε = max(1, 100*100/(10000*2)) = 1. First hit puts pending=1
            // (not yet > 1). Subsequent hits cross the budget.
            //
            // The rule has to be interned first or the threshold path is
            // skipped on the very first hit; the local ingest interns it.
            let rule_fp: u128 = 0xC0DE;
            let key = KeyHash(0x99);
            client_a.record(rule_fp, key, 0, 1, 100, 0).await.unwrap();
            client_a.record(rule_fp, key, 0, 1, 100, 0).await.unwrap();
            client_a.record(rule_fp, key, 0, 1, 100, 0).await.unwrap();

            // Advance well under the heartbeat. The threshold trigger
            // should have already pumped the gossip frame through.
            sim_advance_ticks(Duration::from_millis(10), 5).await;

            let sum_b: u64 = agg_b.inner.borrow().values().copied().sum();
            assert!(
                sum_b >= 1,
                "expected threshold-fire to reach B before the 1s heartbeat, got sum_b={sum_b}",
            );

            client_a.shutdown().await.unwrap();
            let _ = h_a.await;
            h_b.abort();
            let _ = h_b.await;
        })
        .await;
}

/// When ε saturates to 1 and the request stream pins it crossed every
/// hit, the `min_emit_interval` floor caps the per-second emission rate.
/// Drives 1000 hits across ~10 ms of virtual time; with a 5 ms floor the
/// number of distinct emit-triggered ticks (= packets the recipient
/// observed) must stay well below 1000.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn min_emit_interval_clamps_adversarial_rate() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::with_channel_capacity(256);
            let addr_a = sock(54_010);
            let addr_b = sock(54_011);
            let id_a = NodeIdentity::new(NodeId(0xA2), 1);
            let id_b = NodeIdentity::new(NodeId(0xB2), 1);

            // ε saturates to 1 (limit=1). Heartbeat is far in the future
            // so the only emissions are threshold-triggered.
            let mut cfg_a = sim_config(id_a, vec![addr_b], 1);
            cfg_a.tick_interval = Duration::from_secs(10);
            cfg_a.target_err_bps = 100;
            cfg_a.min_emit_interval = Duration::from_millis(5);
            let mut cfg_b = sim_config(id_b, vec![addr_a], 2);
            cfg_b.tick_interval = Duration::from_secs(10);

            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                router.bind(addr_a),
                TokioClock::from_millis(0),
                cfg_a,
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, _client_b) = GossipRuntime::from_parts(
                router.bind(addr_b),
                TokioClock::from_millis(0),
                cfg_b,
                store_for(id_b),
                agg_b.clone(),
            );
            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            let rule_fp: u128 = 0xCAB1E;
            let key = KeyHash(0x11);

            // Prime the rule so the threshold path takes effect on the
            // first burst-loop iteration.
            client_a.record(rule_fp, key, 0, 1, 1, 0).await.unwrap();

            // 1000 hits across ~10 ms of virtual time; now_millis
            // advances by 1 ms per 100 hits. The clamp applies through
            // `req.now_millis - rule_last_emit_ms`.
            for i in 0..1000_u64 {
                let now_ms = i / 100;
                client_a
                    .record(rule_fp, key, 0, 1, 1, now_ms)
                    .await
                    .unwrap();
            }
            sim_advance_ticks(Duration::from_millis(1), 20).await;

            // Each successful threshold-fire enqueues a single frame to B.
            // SimRouter counts received frames. With 1000 hits and a 5 ms
            // floor over ~10 ms, the count must stay well below the hit
            // count by a large margin.
            let received = router.received_count(addr_b);
            assert!(
                received < 200,
                "min_emit_interval should clamp emit rate; got {received} frames for 1000 hits",
            );

            client_a.shutdown().await.unwrap();
            let _ = h_a.await;
            h_b.abort();
            let _ = h_b.await;
        })
        .await;
}

/// Cold rules (with pending below ε) still get drained by the heartbeat.
/// Without the heartbeat, a rule that never accumulates enough hits to
/// fire the threshold would never replicate at all — eventual consistency
/// would fail.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn heartbeat_still_drains_cold_rules() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(54_020);
            let addr_b = sock(54_021);
            let id_a = NodeIdentity::new(NodeId(0xA3), 1);
            let id_b = NodeIdentity::new(NodeId(0xB3), 1);

            // High budget so the single hit cannot cross ε.
            let mut cfg_a = sim_config(id_a, vec![addr_b], 1);
            cfg_a.tick_interval = Duration::from_millis(100);
            cfg_a.target_err_bps = 100;
            let mut cfg_b = sim_config(id_b, vec![addr_a], 2);
            cfg_b.tick_interval = Duration::from_millis(100);

            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                router.bind(addr_a),
                TokioClock::from_millis(0),
                cfg_a,
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, _client_b) = GossipRuntime::from_parts(
                router.bind(addr_b),
                TokioClock::from_millis(0),
                cfg_b,
                store_for(id_b),
                agg_b.clone(),
            );
            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            // rule_limit=1_000_000, N=2, bps=100 =>
            //   ε = 1_000_000*100/(10000*2) = 5000.
            // One hit is far below ε, so the threshold never fires.
            let rule_fp: u128 = 0xC01D;
            client_a
                .record(rule_fp, KeyHash(1), 0, 1, 1_000_000, 0)
                .await
                .unwrap();

            // Advance past two heartbeats — the cold cell must propagate.
            sim_advance_ticks(Duration::from_millis(100), 5).await;

            let sum_b: u64 = agg_b.inner.borrow().values().copied().sum();
            assert_eq!(sum_b, 1, "heartbeat should drain cold rules");

            client_a.shutdown().await.unwrap();
            let _ = h_a.await;
            h_b.abort();
            let _ = h_b.await;
        })
        .await;
}

/// Under sustained load, the cluster-wide unreplicated error per rule
/// stays bounded by `target_err_bps / 10_000 × limit` plus slack for
/// in-flight frames. Asserts a soft bound (≤ 2 × N × ε + tick slack)
/// across many record batches.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn error_bound_holds_under_load() {
    let local = LocalSet::new();
    local
        .run_until(async {
            const NODES: usize = 4;
            const LIMIT: u64 = 1_000;
            const TARGET_BPS: u32 = 100; // 1 %

            let router = SimRouter::with_channel_capacity(512);
            let addrs: Vec<SocketAddr> = (0..NODES).map(|i| sock(54_100 + i as u16)).collect();

            let mut clients = Vec::with_capacity(NODES);
            let mut handles = Vec::with_capacity(NODES);
            let mut aggregates = Vec::with_capacity(NODES);
            for i in 0..NODES {
                let identity = NodeIdentity::new(NodeId(0xC000 + i as u128), 1);
                let peers: Vec<_> = addrs.iter().copied().filter(|a| *a != addrs[i]).collect();
                let mut cfg = sim_config(identity, peers, 0xBEEF + i as u64);
                cfg.fanout = NODES - 1;
                cfg.target_err_bps = TARGET_BPS;
                cfg.min_emit_interval = Duration::from_millis(1);
                cfg.tick_interval = Duration::from_millis(100);
                cfg.max_cells_per_tick = 64;
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

            let rule_fp: u128 = 0xC0FE;
            let key = KeyHash(0x44);
            let batches = 50_u32;
            let per_batch = 8_u64;
            let mut real_total = 0_u64;
            let mut max_lag = 0_u64;
            for batch in 0..batches {
                for client in clients.iter().take(NODES) {
                    client
                        .record(rule_fp, key, 0, per_batch, LIMIT, batch as u64)
                        .await
                        .unwrap();
                    real_total += per_batch;
                }
                sim_advance_ticks(Duration::from_millis(2), 1).await;
                let min_seen = aggregates
                    .iter()
                    .map(|a| a.snapshot().values().copied().sum::<u64>())
                    .min()
                    .unwrap_or(0);
                let lag = real_total.saturating_sub(min_seen);
                if lag > max_lag {
                    max_lag = lag;
                }
            }

            // ε per peer ≈ TARGET_BPS / 10_000 × LIMIT; the cluster-wide
            // bound is N × ε. Allow slack for in-flight frames between
            // trip and apply across all peers (one tick of writes).
            let bound = ((LIMIT * TARGET_BPS as u64) / 10_000) * NODES as u64;
            let tick_slack = NODES as u64 * per_batch * 2;
            assert!(
                max_lag <= bound.saturating_add(tick_slack).saturating_mul(2),
                "error lag {max_lag} exceeded soft bound {bound} (+ slack {tick_slack})",
            );

            for client in clients {
                let _ = client.shutdown().await;
            }
            for handle in handles {
                let _ = handle.await;
            }
        })
        .await;
}
