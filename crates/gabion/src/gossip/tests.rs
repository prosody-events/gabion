//! End-to-end gossip runtime tests.
//!
//! Most tests use [`super::sim::SimTransport`] + `tokio::time::pause()` so
//! virtual time + in-memory delivery make them deterministic and fast.
//! `udp_round_trip_smoke` is the lone realtime/UDP smoke test, kept to
//! ensure the production transport doesn't bit-rot; it waits on the
//! aggregate store's apply signal rather than polling the clock.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::num::NonZeroU128;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::LocalSet;

use crate::crdt::{
    BucketEpoch, CellStore, CellStoreConfig, Count, DeltaSink, ExpirationSink, KeyHash, NodeId,
    NodeIdentity, RuleDescriptor,
};
use crate::discovery::{Peer, PeerEvent};
#[cfg(feature = "transport-udp")]
use crate::gossip::UdpTransport;
use crate::gossip::membership::{Membership, Owner};
use crate::gossip::sim::{LinkPolicy, SimRouter, sim_advance_ticks};
use crate::gossip::{AggregateStore, GossipConfig, GossipRuntime, TokioClock, parse_cluster_id};
use crate::wire::HmacKey;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;

// -- in-memory aggregate store ----------------------------------------------

/// Single-threaded in-memory aggregate store used as the canonical test
/// backend. Keys on `(rule, key_hash, bucket)`. `RefCell` keeps the trait
/// method `&self` so tests can clone an `Rc` between the runtime and the
/// read path. `applied` fires after every `apply`, so realtime tests wait
/// on it instead of sleeping.
#[derive(Default)]
pub(super) struct InMemoryAggregateStore<C: Count> {
    inner: RefCell<HashMap<(u128, KeyHash, BucketEpoch), u64>>,
    apply_calls: RefCell<Vec<(usize, usize)>>,
    applied: Notify,
    _marker: std::marker::PhantomData<C>,
}

impl<C: Count> InMemoryAggregateStore<C> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn total(&self) -> u64 {
        self.inner.borrow().values().sum()
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
        self.applied.notify_one();
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
    records: Vec<SimRecord>,
}

impl Arbitrary for ConnectedClusterCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let max_nodes = quickcheck_max_nodes();
        let nodes = (u16::arbitrary(g) % max_nodes) + 1;
        let max_records = 32;
        let len = usize::arbitrary(g) % max_records;
        Self {
            nodes,
            records: (0..len).map(|_| SimRecord::arbitrary(g)).collect(),
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

/// The membership node `index` of the full mesh `addrs` holds, so a test can
/// ask who owns a key the way the runtimes do.
fn full_mesh_membership(addrs: &[SocketAddr], index: usize) -> Membership {
    let mut membership = Membership::new(addrs.len());
    membership.set_local(addrs[index]);
    for (i, addr) in addrs.iter().enumerate() {
        if i != index {
            membership.add(*addr);
        }
    }
    membership
}

/// Index into `addrs` of the node that owns `(rule_fp, key)`.
fn owner_index(addrs: &[SocketAddr], fingerprint: u128, key: KeyHash) -> usize {
    let membership = full_mesh_membership(addrs, 0);
    match membership.owner(fingerprint, key) {
        Owner::Local => 0,
        Owner::Peer(peer) => addrs
            .iter()
            .position(|a| *a == membership.peers()[peer].addr)
            .unwrap(),
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

async fn run_connected_case(case: ConnectedClusterCase) -> TestResult {
    let nodes = case.nodes as usize;
    if case.records.is_empty() {
        return TestResult::discard();
    }

    let router = SimRouter::with_channel_capacity(256);
    let addrs: Vec<SocketAddr> = (0..nodes).map(|i| sock(41_000 + i as u16)).collect();
    let mut clients = Vec::with_capacity(nodes);
    let mut handles = Vec::with_capacity(nodes);
    let mut aggregates = Vec::with_capacity(nodes);
    let node_capacity =
        ((case.records.len() + 1).next_power_of_two().max(8)).min(u16::MAX as usize) as u16;
    for i in 0..nodes {
        let identity = NodeIdentity::new(NodeId(0xA000 + i as u128), 1);
        let peers = addrs.iter().copied().filter(|a| *a != addrs[i]).collect();
        let mut cfg = sim_config(identity, peers, 0xBAD5_EED0 + i as u64);
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

    let ticks = if nodes == 1 { 1 } else { 20 };
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
fn quickcheck_sim_clusters_converge_on_every_hit(case: ConnectedClusterCase) -> TestResult {
    run_paused(run_connected_case(case))
}

#[quickcheck]
fn quickcheck_sim_partition_heals_without_overcount(case: PartitionCase) -> TestResult {
    if case.records.is_empty() {
        return TestResult::discard();
    }
    // Surface the failing invariant *and the generating case* rather than a
    // bare `false`: `PartitionCase` has no `shrink`, so `from_bool(false)`
    // prints only quickcheck's "No Arguments Provided" — useless for a rare
    // flake. `TestResult::error` carries the message to the log.
    match run_paused(partition_heal_violation(case.clone(), 120)) {
        Some(reason) => TestResult::error(format!("{reason}; case={case:?}")),
        None => TestResult::passed(),
    }
}

/// Drive the eight-node partition→heal scenario for `case` and return a
/// human-readable description of the first invariant it violates, or `None`
/// if the cluster behaved correctly: while the partition holds no node may
/// over-count any key (over-admission is the dangerous direction for a
/// limiter), and `heal_ticks` ticks after the links are restored every node
/// must converge to the exact cluster-wide model. Shared by the property test
/// above and the deterministic regression below.
async fn partition_heal_violation(mut case: PartitionCase, heal_ticks: u32) -> Option<String> {
    const NODES: usize = 8;
    if case.records.is_empty() {
        return None;
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
    let mut overcount = None;
    for (node, agg) in aggregates.iter().enumerate() {
        for (key, count) in agg.snapshot() {
            let cap = *global.get(&key).unwrap_or(&0);
            if count > cap {
                overcount = Some(format!(
                    "node {node} over-counted {key:?}: {count} > cluster total {cap} during the \
                     partition"
                ));
            }
        }
    }

    for left in 0..4 {
        for right in 4..8 {
            router.set_link_policy(addrs[left], addrs[right], LinkPolicy::Pass);
            router.set_link_policy(addrs[right], addrs[left], LinkPolicy::Pass);
        }
    }
    // An owner broadcasts a key when it changes and does not retry, so a key
    // that stays quiet after the heal keeps whatever the partition made
    // peers miss. One more hit to each key, from any single node, makes its
    // owner broadcast the key's whole state and repairs every peer.
    let mut touched = BTreeSet::new();
    let touches: Vec<SimRecord> = case
        .records
        .iter()
        .filter(|r| touched.insert((r.rule, r.key)))
        .map(|r| SimRecord {
            node: 0,
            hits: 1,
            ..r.clone()
        })
        .collect();
    for record in &touches {
        clients[0]
            .record(rule_fp(record.rule), key_hash(record.key), 0, 1, 0, 0)
            .await
            .unwrap();
    }
    let global = {
        let mut model = global;
        for (key, hits) in expected_model(&touches, NODES) {
            *model.entry(key).or_insert(0) += hits;
        }
        model
    };
    sim_advance_ticks(Duration::from_millis(100), heal_ticks).await;
    let mut divergence = None;
    for (node, agg) in aggregates.iter().enumerate() {
        let snap = agg.snapshot();
        if snap != global {
            divergence = Some(format!(
                "node {node} did not converge {heal_ticks} ticks after heal: has {} keys, cluster \
                 model has {}; snapshot={snap:?} model={global:?}",
                snap.len(),
                global.len(),
            ));
        }
    }

    for client in clients {
        let _ = client.shutdown().await;
    }
    for handle in handles {
        let _ = handle.await;
    }

    overcount.or(divergence)
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

        let sum_a = agg_a.total();
        let sum_b = agg_b.total();
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

        let got_b = agg_b.total();
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
            let totals = agg.total();
            assert_eq!(totals, 5);

            client.record(rule_fp, key, 0, 3, 0, 1_000).await.unwrap();
            let totals = agg.total();
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

            let sum_a = agg_a.total();
            let sum_b = agg_b.total();
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
            assert_eq!(agg.total(), 2);

            // Advance past one tick + past the live window.
            sim_advance_ticks(Duration::from_millis(100), 3).await;

            // The tick handler called expire_at, which freed the cell.
            // The aggregate store sees expiration row(s) and removes the
            // value.
            assert_eq!(agg.total(), 0);

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
            assert_eq!(agg.total(), 2);
            sim_advance_ticks(Duration::from_millis(100), 3).await;
            assert_eq!(agg.total(), 0);

            // Second burst at the now-current bucket. It must register and then
            // age out on the same 100 ms window. Under the
            // released-then-default bug it lands on a 60 s
            // descriptor and stays here forever.
            client
                .record(0xFEED, KeyHash(7), 3, 2, 10, 300)
                .await
                .unwrap();
            assert_eq!(agg.total(), 2);
            sim_advance_ticks(Duration::from_millis(100), 3).await;
            assert_eq!(
                agg.total(),
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
            let totals = agg.inner.total();
            assert_eq!(totals, 3);

            client.shutdown().await.unwrap();
            let _ = handle.await;
        })
        .await;
}

// -- per-peer pruning -------------------------------------------------------

// -- dropped sync is re-sent -------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn dropped_sync_is_resent_until_the_owner_acknowledges() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(40_060);
            let addr_b = sock(40_061);
            let t_a = router.bind(addr_a);
            let t_b = router.bind(addr_b);
            // Drop the first packet on A->B: A's first sync of a key B owns.
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

            let key = (0..)
                .map(KeyHash)
                .find(|&key| owner_index(&[addr_a, addr_b], 0xDEAD, key) == 1)
                .unwrap();
            client_a.record(0xDEAD, key, 0, 7, 0, 0).await.unwrap();
            // The unacknowledged cell is re-sent on the next sync.
            sim_advance_ticks(Duration::from_millis(100), 10).await;

            let sum_b = agg_b.total();
            assert_eq!(
                sum_b, 7,
                "B owns the key and received it despite the first-packet drop"
            );

            client_a.shutdown().await.unwrap();
            let _ = h_a.await;
            h_b.abort();
            let _ = h_b.await;
        })
        .await;
}

/// A hit that found the sync queue full is not forgotten: the next sweep of
/// the node's local cells queues it again once the queue has drained.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn hits_that_overflowed_the_sync_queue_still_reach_their_owner() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(40_070);
            let addr_b = sock(40_071);
            let id_a = NodeIdentity::new(NodeId(0xAA00), 1);
            let id_b = NodeIdentity::new(NodeId(0xBB00), 1);
            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());
            let tiny_queue = CellStore::<u32>::new(
                CellStoreConfig {
                    local_dirty_capacity: 2,
                    ..CellStoreConfig::default()
                },
                id_a,
            );
            let (rt_a, client_a) = GossipRuntime::from_parts(
                router.bind(addr_a),
                TokioClock::from_millis(0),
                sim_config(id_a, vec![addr_b], 1),
                tiny_queue,
                agg_a.clone(),
            );
            let (rt_b, _client_b) = GossipRuntime::from_parts(
                router.bind(addr_b),
                TokioClock::from_millis(0),
                sim_config(id_b, vec![addr_a], 2),
                store_for(id_b),
                agg_b.clone(),
            );
            let h_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let h_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            let keys: Vec<_> = (0..)
                .map(KeyHash)
                .filter(|&key| owner_index(&[addr_a, addr_b], 0xDEAD, key) == 1)
                .take(6)
                .collect();
            for &key in &keys {
                client_a.record(0xDEAD, key, 0, 1, 0, 0).await.unwrap();
            }
            sim_advance_ticks(Duration::from_millis(100), 20).await;
            assert_eq!(agg_b.total(), 6, "the owner holds every hit");

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

            let sum_a = agg_a.total();
            let sum_b = agg_b.total();
            let sum_c = agg_c.total();
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

// -- owner aggregation -------------------------------------------------------

/// `n` runtimes in a full mesh, each with a discovery channel the test can
/// send peer events into.
struct Mesh {
    router: SimRouter,
    addrs: Vec<SocketAddr>,
    clients: Vec<crate::gossip::GossipClient<u32>>,
    aggregates: Vec<Rc<InMemoryAggregateStore<u32>>>,
    handles: Vec<tokio::task::JoinHandle<Result<(), crate::gossip::GossipError>>>,
    events: Vec<tokio::sync::mpsc::UnboundedSender<PeerEvent>>,
}

fn spawn_mesh(first_port: u16, n: usize) -> Mesh {
    let router = SimRouter::new();
    let addrs: Vec<SocketAddr> = (0..n).map(|i| sock(first_port + i as u16)).collect();
    let mut mesh = Mesh {
        router: router.clone(),
        addrs: addrs.clone(),
        clients: Vec::new(),
        aggregates: Vec::new(),
        handles: Vec::new(),
        events: Vec::new(),
    };
    for i in 0..n {
        let identity = NodeIdentity::new(NodeId(0xF000 + i as u128), 1);
        let peers = addrs.iter().copied().filter(|a| *a != addrs[i]).collect();
        let agg = Rc::new(InMemoryAggregateStore::<u32>::new());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<PeerEvent>();
        let stream =
            futures::stream::unfold(
                rx,
                |mut rx| async move { rx.recv().await.map(|evt| (evt, rx)) },
            );
        let (rt, client) = GossipRuntime::from_parts(
            router.bind(addrs[i]),
            TokioClock::from_millis(0),
            sim_config(identity, peers, i as u64),
            store_for(identity),
            agg.clone(),
        );
        mesh.handles.push(tokio::task::spawn_local(rt.run(stream)));
        mesh.clients.push(client);
        mesh.aggregates.push(agg);
        mesh.events.push(tx);
    }
    mesh
}

impl Mesh {
    async fn shutdown(self) {
        for client in &self.clients {
            let _ = client.shutdown().await;
        }
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn hits_reach_other_nodes_only_through_the_owner() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let mesh = spawn_mesh(54_000, 3);
            let key = (0..)
                .map(KeyHash)
                .find(|&key| owner_index(&mesh.addrs, 0xDEAD, key) == 1)
                .unwrap();
            // The owner cannot reach C. A never relays, so C learns nothing.
            mesh.router
                .set_link_policy(mesh.addrs[1], mesh.addrs[2], LinkPolicy::Block);
            mesh.clients[0]
                .record(0xDEAD, key, 0, 3, 0, 0)
                .await
                .unwrap();
            sim_advance_ticks(Duration::from_millis(100), 10).await;
            assert_eq!(
                mesh.aggregates[0].total(),
                3,
                "the contributor counts its own hit"
            );
            assert_eq!(mesh.aggregates[1].total(), 3, "the owner folds it");
            assert_eq!(mesh.aggregates[2].total(), 0, "no non-owner relays a hit");

            // Once the owner can reach C, its next broadcast carries the
            // whole cell, so C catches up.
            mesh.router
                .set_link_policy(mesh.addrs[1], mesh.addrs[2], LinkPolicy::Pass);
            mesh.clients[0]
                .record(0xDEAD, key, 0, 2, 0, 0)
                .await
                .unwrap();
            sim_advance_ticks(Duration::from_millis(100), 10).await;
            assert_eq!(mesh.aggregates[2].total(), 5);
            mesh.shutdown().await;
        })
        .await;
}

/// A peer that missed an owner's broadcast catches up on the next change to
/// the key, whichever cell of the key changed: the broadcast carries the
/// key's whole state.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn next_change_to_a_key_repairs_a_peer_that_missed_a_broadcast() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let mesh = spawn_mesh(54_050, 3);
            let key = (0..)
                .map(KeyHash)
                .find(|&key| owner_index(&mesh.addrs, 0xDEAD, key) == 1)
                .unwrap();
            mesh.router
                .set_link_policy(mesh.addrs[1], mesh.addrs[2], LinkPolicy::Block);
            mesh.clients[0]
                .record(0xDEAD, key, 0, 3, 0, 0)
                .await
                .unwrap();
            sim_advance_ticks(Duration::from_millis(100), 10).await;
            assert_eq!(mesh.aggregates[2].total(), 0);

            // C's own hit changes a different cell (another origin) and a
            // different bucket of the key; the owner's broadcast carries A's
            // earlier cell along.
            mesh.router
                .set_link_policy(mesh.addrs[1], mesh.addrs[2], LinkPolicy::Pass);
            mesh.clients[2]
                .record(0xDEAD, key, 1, 2, 0, 0)
                .await
                .unwrap();
            sim_advance_ticks(Duration::from_millis(100), 10).await;
            assert_eq!(mesh.aggregates[2].total(), 5);
            assert_eq!(mesh.aggregates[1].total(), 5);
            mesh.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn unacknowledged_hits_move_to_the_new_owner() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let mesh = spawn_mesh(54_100, 3);
            // A key B owns while all three are members, and C owns once B is
            // gone.
            let without_b = [mesh.addrs[0], mesh.addrs[2]];
            let key = (0..)
                .map(KeyHash)
                .find(|&key| {
                    owner_index(&mesh.addrs, 0xDEAD, key) == 1
                        && owner_index(&without_b, 0xDEAD, key) == 1
                })
                .unwrap();
            mesh.router
                .set_link_policy(mesh.addrs[0], mesh.addrs[1], LinkPolicy::Block);
            mesh.router
                .set_link_policy(mesh.addrs[1], mesh.addrs[0], LinkPolicy::Block);
            mesh.clients[0]
                .record(0xDEAD, key, 0, 4, 0, 0)
                .await
                .unwrap();
            sim_advance_ticks(Duration::from_millis(100), 10).await;
            assert_eq!(
                mesh.aggregates[2].total(),
                0,
                "B owns the key and never got the hit"
            );

            // Discovery reports B gone to A and C: C now owns the key, and A
            // re-sends the hit B never acknowledged.
            for node in [0, 2] {
                mesh.events[node]
                    .send(PeerEvent::Removed(Peer::new(mesh.addrs[1])))
                    .unwrap();
            }
            sim_advance_ticks(Duration::from_millis(100), 10).await;
            assert_eq!(mesh.aggregates[2].total(), 4);
            mesh.shutdown().await;
        })
        .await;
}

// -- UDP smoke --------------------------------------------------------------

// `UdpTransport` and the `tokio::net::UdpSocket` it wraps live behind the
// `transport-udp` feature (see `crates/gabion/src/gossip.rs:42-43`). Without
// the gate, `cargo nextest run -p gabion --no-default-features` — the form
// the kubernetes smokes use — fails to compile this whole test crate.
#[cfg(feature = "transport-udp")]
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
                tick_interval: Duration::from_millis(20),
                rng_seed: 1,
                ..GossipConfig::default()
            };
            let config_b = GossipConfig {
                local_identity: id_b,
                cluster_id_hash: 0xC1,
                bootstrap_peers: vec![addr_a],
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

            // Re-check after each apply (`notify_one` keeps a permit, so none
            // is missed); the timeout only bounds a broken transport.
            let converged = tokio::time::timeout(Duration::from_secs(30), async {
                for agg in [&agg_a, &agg_b] {
                    while agg.total() != 8 {
                        agg.applied.notified().await;
                    }
                }
            });
            assert!(
                converged.await.is_ok(),
                "did not converge: a={}, b={}",
                agg_a.total(),
                agg_b.total(),
            );

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

            // One record on A for a key B owns. B rejects every sync because
            // the auth keys don't match, so A re-sends it each sync_wait.
            let key = (0..)
                .map(KeyHash)
                .find(|&key| owner_index(&[addr_a, addr_b], 0xDEAD, key) == 1)
                .unwrap();
            client_a.record(0xDEAD, key, 0, 1, 0, 0).await.unwrap();

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
fn quickcheck_sim_owners_receive_every_hit_under_iid_packet_loss(case: DropProbCase) -> TestResult {
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

        // Hits are acknowledged by the owner and re-sent until it does, so
        // however many packets are lost the owner ends up with every hit.
        // (Its broadcast is not retried, so other nodes may lag until the
        // key's next hit; they must never run ahead of the truth.)
        let expected = expected_model(&case.records, nodes);
        let mut converged = false;
        for _ in 0..80 {
            sim_advance_ticks(Duration::from_millis(100), 5).await;
            converged = aggregates.iter().enumerate().all(|(node, agg)| {
                let snapshot = agg.snapshot();
                expected.iter().all(|(&(fp, key, bucket), &total)| {
                    let held = snapshot.get(&(fp, key, bucket)).copied().unwrap_or(0);
                    held <= total
                        && (owner_index(&addrs, fp, KeyHash(key)) != node || held == total)
                })
            });
            if converged {
                break;
            }
        }
        if !converged {
            eprintln!(
                "an owner never received every hit under p={}, nodes={}, records={}, \
                 last_states={:?}",
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

#[test]
fn parse_cluster_id_accepts_decimal_and_prefixed_hex() {
    let parse = |value| parse_cluster_id(value).map(NonZeroU128::get);
    assert_eq!(parse("32201612552594806"), Some(32_201_612_552_594_806));
    assert_eq!(parse("0xc0ffee"), Some(0x00c0_ffee));
    assert_eq!(parse("0XC0FFEE"), Some(0x00c0_ffee));
    assert_eq!(parse(" 1 "), Some(1));
    assert_eq!(parse("0xffffffffffffffffffffffffffffffff"), Some(u128::MAX));
}

#[test]
fn parse_cluster_id_rejects_zero_signs_unprefixed_hex_and_garbage() {
    for value in [
        "0", "0x0", "+1", "0x+1", "-1", "c0ffee", "0x", "", "0xg1", "1e3",
    ] {
        assert_eq!(parse_cluster_id(value), None, "{value:?} should not parse");
    }
}
