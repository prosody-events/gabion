//! End-to-end gossip runtime tests.
//!
//! Most tests use [`super::sim::SimTransport`] + `tokio::time::pause()` so
//! virtual time + in-memory delivery make them deterministic and fast.
//! `udp_round_trip_smoke` is the lone realtime/UDP smoke test, kept to
//! ensure the production transport doesn't bit-rot; it waits on the
//! aggregate store's apply signal rather than polling the clock.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::num::{NonZeroU16, NonZeroUsize};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::{Notify, mpsc};
use tokio::task::{JoinHandle, LocalSet};

use crate::crdt::{
    BucketEpoch, CellStore, DeltaSink, ExpirationSink, KeyHash, NodeId, NodeIdentity, Observation,
    ObservationBatch, RuleDescriptor,
};
use crate::defaults::GOSSIP_EXPIRATION_BATCH;
use crate::discovery::{Peer, PeerEvent};
#[cfg(feature = "transport-udp")]
use crate::gossip::UdpTransport;
use crate::gossip::mock::{MockTransport, Wire};
use crate::gossip::sim::{LinkPolicy, SimRouter, sim_advance_ticks};
use crate::gossip::{
    AdminCommand, AdminSnapshot, AggregateStore, GossipClient, GossipCounters, GossipError,
    GossipRuntime, GossipTransport, TokioClock, ValidGossipConfig,
};
use crate::mock::with_logs;
use crate::settings::mock::{rules, storage};
use crate::settings::{Millis, Settings};
use crate::wire::{self, AckInfo, CellPayload, Header, MIN_PAYLOAD_BYTES};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;

// -- in-memory aggregate store ----------------------------------------------

/// Single-threaded in-memory aggregate store used as the canonical test
/// backend. Keys on `(rule, key_hash, bucket)`. `RefCell` keeps the trait
/// method `&self` so tests can clone an `Rc` between the runtime and the
/// read path. `applied` fires after every `apply`, so realtime tests wait
/// on it instead of sleeping.
#[derive(Default)]
pub(super) struct InMemoryAggregateStore {
    inner: RefCell<HashMap<(u128, KeyHash, BucketEpoch), u64>>,
    apply_calls: RefCell<Vec<(usize, usize)>>,
    applied: Notify,
}

impl InMemoryAggregateStore {
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

impl AggregateStore for InMemoryAggregateStore {
    fn apply(&self, deltas: &DeltaSink, expirations: &ExpirationSink) {
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

fn store_for(identity: NodeIdentity) -> CellStore {
    CellStore::new(storage(256, 32, 64), identity, &rules(32))
}

fn sock(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn sim_config(peers: Vec<SocketAddr>, seed: u64) -> ValidGossipConfig {
    tuned_config(peers, seed, |_| {})
}

/// The tests' gossip settings, then `tune`, built through
/// `Settings::validate` as production builds them.
fn tuned_config(
    peers: Vec<SocketAddr>,
    seed: u64,
    tune: impl FnOnce(&mut Settings),
) -> ValidGossipConfig {
    let mut settings = Settings {
        cluster_id_hash: 0xC1.try_into().unwrap(),
        fanout: NonZeroUsize::MIN,
        tick_interval: ms(100),
        max_cells_per_tick: 1024.try_into().unwrap(),
        max_payload_bytes: 65_507,
        limit_queue_capacity: 1024.try_into().unwrap(),
        ..Settings::default()
    };
    tune(&mut settings);
    let (mut config, _) = settings.validate().expect("test gossip settings validate");
    config.bootstrap_peers = peers;
    config.rng_seed = seed;
    config
}

fn ms(millis: u64) -> Millis {
    Millis(millis.try_into().unwrap())
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
struct ClusterCase {
    same_cluster: bool,
    a_hits: u8,
    b_hits: u8,
}

impl Arbitrary for ClusterCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            same_cluster: bool::arbitrary(g),
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

fn store_for_expiring_rule(identity: NodeIdentity) -> CellStore {
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

fn store_for_property_rules(identity: NodeIdentity, node_capacity: u16) -> CellStore {
    let mut store = CellStore::new(storage(512, node_capacity.max(8), 512), identity, &rules(8));
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
        let cfg = tuned_config(peers, 0xBAD5_EED0 + i as u64, |s| {
            s.fanout = (case.fanout as usize)
                .min(nodes.saturating_sub(1))
                .max(1)
                .try_into()
                .unwrap();
            s.max_cells_per_tick = 128.try_into().unwrap();
        });
        let agg = Rc::new(InMemoryAggregateStore::new());
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
        let cfg = tuned_config(peers, 0xC0FF_EE00 + i as u64, |s| {
            s.fanout = 4.try_into().unwrap();
            s.max_cells_per_tick = 32.try_into().unwrap();
        });
        let agg = Rc::new(InMemoryAggregateStore::new());
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

/// Deterministic partition-heal regression: a captured `PartitionCase` in
/// which node 3's `(rule 0, key 0)` cell once never reached node 1. Pinned so
/// convergence cannot regress behind the ~1-in-100k random trigger.
#[test]
fn sim_partition_heal_converges_on_captured_stuck_cell() {
    let records: Vec<SimRecord> = [
        (1419u16, 0u8, 6u8, 8u8),
        (36630, 1, 1, 10),
        (6923, 0, 6, 3),
        (54319, 0, 4, 14),
        (29105, 1, 2, 2),
        (41658, 2, 4, 13),
        (33226, 2, 4, 11),
        (11907, 2, 0, 7),
        (18610, 2, 6, 15),
        (49507, 0, 0, 16),
        (57336, 0, 6, 10),
        (22238, 0, 1, 12),
        (44550, 0, 4, 8),
        (41284, 2, 2, 17),
        (1, 2, 7, 5),
        (37755, 0, 3, 16),
        (52572, 1, 1, 8),
        (50717, 1, 6, 11),
        (25066, 0, 1, 13),
    ]
    .into_iter()
    .map(|(node, rule, key, hits)| SimRecord {
        node,
        rule,
        key,
        hits,
    })
    .collect();
    let case = PartitionCase {
        records,
        heal_after_ticks: 8,
    };
    assert_eq!(
        run_paused(partition_heal_violation(case, 120)),
        None,
        "captured partition counterexample must converge",
    );
}

/// A packet from another cluster merges nothing: the receiver keeps only its
/// own cell and counts the drop as `cluster`. The same-cluster case proves
/// the packets arrive.
#[quickcheck]
fn foreign_cluster_packet_changes_nothing(case: ClusterCase) -> TestResult {
    run_paused(async move {
        let router = SimRouter::new();
        let addr_a = sock(43_000);
        let addr_b = sock(43_001);
        let cluster_b = if case.same_cluster { 0xC1 } else { 0xC2 };
        let cfg_b = tuned_config(vec![addr_a], 2, |s| {
            s.cluster_id_hash = cluster_b.try_into().unwrap();
        });
        let agg_a = Rc::new(InMemoryAggregateStore::new());
        let agg_b = Rc::new(InMemoryAggregateStore::new());
        let (admin_b, admin_rx_b) = mpsc::channel(4);
        let (rt_a, client_a) = GossipRuntime::from_parts(
            router.bind(addr_a),
            TokioClock::from_millis(0),
            sim_config(vec![addr_b], 1),
            store_for(NodeIdentity::new(NodeId(0xCA), 1)),
            agg_a.clone(),
        );
        let (rt_b, client_b) = GossipRuntime::from_parts_with_admin(
            router.bind(addr_b),
            TokioClock::from_millis(0),
            cfg_b,
            store_for(NodeIdentity::new(NodeId(0xCB), 1)),
            agg_b.clone(),
            Some(admin_rx_b),
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

        let (a, b) = (u64::from(case.a_hits), u64::from(case.b_hits));
        let snapshot = admin_snapshot(&admin_b).await;
        let counters = snapshot.counters;
        let others = (
            counters.decode_rejects_framing,
            counters.decode_rejects_peer_limits,
            counters.decode_rejects_version,
        );
        let held = if case.same_cluster {
            agg_a.total() == a + b && agg_b.total() == a + b && counters.decode_rejects_cluster == 0
        } else {
            agg_a.total() == a
                && agg_b.total() == b
                && snapshot.store_stats.active_cells == 1
                && counters.decode_rejects_cluster >= 1
        };

        client_a.shutdown().await.unwrap();
        client_b.shutdown().await.unwrap();
        let _ = h_a.await;
        let _ = h_b.await;

        TestResult::from_bool(held && others == (0, 0, 0))
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
            let cfg = tuned_config(peers, 0xEED0 + i as u64, |s| {
                s.fanout = nodes.saturating_sub(1).clamp(1, 8).try_into().unwrap();
                s.max_cells_per_tick = 32.try_into().unwrap();
            });
            let agg = Rc::new(InMemoryAggregateStore::new());
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

        let agg_a = Rc::new(InMemoryAggregateStore::new());
        let agg_b = Rc::new(InMemoryAggregateStore::new());
        let (rt_a, client_a) = GossipRuntime::from_parts(
            router.bind(addr_a),
            TokioClock::from_millis(0),
            sim_config(Vec::new(), 1),
            store_for(id_a),
            agg_a.clone(),
        );
        let (rt_b, client_b) = GossipRuntime::from_parts(
            router.bind(addr_b),
            TokioClock::from_millis(0),
            sim_config(Vec::new(), 2),
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
            let agg = Rc::new(InMemoryAggregateStore::new());
            let clock = TokioClock::from_millis(0);

            let (rt, client) = GossipRuntime::from_parts(
                transport,
                clock,
                sim_config(Vec::new(), 1),
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

            let agg_a = Rc::new(InMemoryAggregateStore::new());
            let agg_b = Rc::new(InMemoryAggregateStore::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                t_a,
                TokioClock::from_millis(0),
                sim_config(vec![addr_b], 1),
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, client_b) = GossipRuntime::from_parts(
                t_b,
                TokioClock::from_millis(0),
                sim_config(vec![addr_a], 2),
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
            let agg = Rc::new(InMemoryAggregateStore::new());

            let (rt, client) = GossipRuntime::from_parts(
                transport,
                TokioClock::from_millis(0),
                sim_config(Vec::new(), 1),
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

            let agg = Rc::new(InMemoryAggregateStore::new());

            let (rt, client) = GossipRuntime::from_parts(
                transport,
                TokioClock::from_millis(0),
                sim_config(Vec::new(), 1),
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

/// A boundary that ages out more cells than one expiration batch applies each
/// full batch before taking the next, all in the round that crosses it.
#[test]
fn expiry_applies_each_full_batch_before_the_next() {
    let cells = GOSSIP_EXPIRATION_BATCH + 1;
    let mut store = CellStore::new(
        storage(cells as u32, 32, cells as u32),
        node(0xA),
        &rules(32),
    );
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
    let mut batch = ObservationBatch::with_capacity(cells);
    for key in 0..cells as u128 {
        batch.push(Observation {
            rule_fingerprint: 0xFEED,
            key_hash: KeyHash(key),
            bucket: 0,
            origin: NodeId(0),
            incarnation: 0,
            count: 1,
            last_update_millis: 0,
        });
    }
    store.ingest_local(&batch, &mut DeltaSink::with_capacity(cells));
    run_paused(async {
        let agg = Rc::new(InMemoryAggregateStore::new());
        let (rt, _client) = GossipRuntime::from_parts(
            MockTransport(Rc::default()),
            TokioClock::from_millis(0),
            sim_config(vec![], 1),
            store,
            agg.clone(),
        );
        tokio::task::spawn_local(rt.run(futures::stream::empty()));
        heartbeats(1).await;
        assert_eq!(agg.apply_call_lens(), []);
        heartbeats(1).await;
        assert_eq!(
            agg.apply_call_lens(),
            [(0, GOSSIP_EXPIRATION_BATCH), (0, 1)]
        );
    });
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

            let agg = Rc::new(InMemoryAggregateStore::new());

            let (rt, client) = GossipRuntime::from_parts(
                transport,
                TokioClock::from_millis(0),
                sim_config(Vec::new(), 1),
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

struct BlockingApplyStore {
    notify: Arc<Notify>,
    released: RefCell<bool>,
    inner: InMemoryAggregateStore,
}

impl BlockingApplyStore {
    fn new(notify: Arc<Notify>) -> Self {
        Self {
            notify,
            released: RefCell::new(false),
            inner: InMemoryAggregateStore::new(),
        }
    }
}

impl AggregateStore for BlockingApplyStore {
    fn apply(&self, deltas: &DeltaSink, expirations: &ExpirationSink) {
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
            let agg = Rc::new(BlockingApplyStore::new(notify.clone()));

            let (rt, client) = GossipRuntime::from_parts(
                transport,
                TokioClock::from_millis(0),
                sim_config(Vec::new(), 1),
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

            let agg_a = Rc::new(InMemoryAggregateStore::new());
            let agg_b = Rc::new(InMemoryAggregateStore::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                t_a,
                TokioClock::from_millis(0),
                sim_config(vec![addr_b], 1),
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, _client_b) = GossipRuntime::from_parts(
                t_b,
                TokioClock::from_millis(0),
                sim_config(vec![addr_a], 2),
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

            let sum_b = agg_b.total();
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

            let agg_a = Rc::new(InMemoryAggregateStore::new());
            let agg_b = Rc::new(InMemoryAggregateStore::new());
            let agg_c = Rc::new(InMemoryAggregateStore::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                t_a,
                TokioClock::from_millis(0),
                sim_config(vec![addr_b, addr_c], 1),
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, client_b) = GossipRuntime::from_parts(
                t_b,
                TokioClock::from_millis(0),
                sim_config(vec![addr_a, addr_c], 2),
                store_for(id_b),
                agg_b.clone(),
            );
            let (rt_c, client_c) = GossipRuntime::from_parts(
                t_c,
                TokioClock::from_millis(0),
                sim_config(vec![addr_a, addr_b], 3),
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

            let agg_a = Rc::new(InMemoryAggregateStore::new());
            let agg_b = Rc::new(InMemoryAggregateStore::new());
            let agg_c = Rc::new(InMemoryAggregateStore::new());
            let agg_d = Rc::new(InMemoryAggregateStore::new());

            // A talks to B, C, D with fanout=3 — every tick must hit each.
            let cfg_a = tuned_config(vec![addr_b, addr_c, addr_d], 0xBAD_5EED, |s| {
                s.fanout = 3.try_into().unwrap();
            });
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
            let cfg_b = sim_config(Vec::new(), 2);
            let cfg_c = sim_config(Vec::new(), 3);
            let cfg_d = sim_config(Vec::new(), 4);
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

            let got_b = agg_b.total();
            let got_c = agg_c.total();
            let got_d = agg_d.total();
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
                    "coverage fanout for {peers} peers: expected max({GOSSIP_FANOUT}, \
                     ⌈ln({peers})+{GOSSIP_COVERAGE_MARGIN}⌉={coverage}).min({peers}) = \
                     {expected}, got {observed}",
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
    let router = SimRouter::new();
    let local_addr = sock(41_000);
    let transport = router.bind(local_addr);

    let bootstrap: Vec<SocketAddr> = (0..peers).map(|i| sock(41_001 + i as u16)).collect();
    let identity = NodeIdentity::new(NodeId(0x5EED), 1);
    let cfg = tuned_config(bootstrap, 1, |s| s.fanout = base.try_into().unwrap());

    let (admin_tx, admin_rx) = mpsc::channel::<AdminCommand>(4);
    let (rt, client) = GossipRuntime::from_parts_with_admin(
        transport,
        TokioClock::from_millis(0),
        cfg,
        store_for(identity),
        Rc::new(InMemoryAggregateStore::new()),
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

// `UdpTransport` and the `tokio::net::UdpSocket` it wraps live behind the
// `transport-udp` feature (see the `transport-udp` gate on the `UdpTransport`
// re-export in `gossip.rs`). Without the gate, `cargo nextest run -p gabion
// --no-default-features` — the form the kubernetes smokes use — fails to
// compile this whole test crate.
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

            let agg_a = Rc::new(InMemoryAggregateStore::new());
            let agg_b = Rc::new(InMemoryAggregateStore::new());

            let config_a = tuned_config(vec![addr_b], 1, |s| s.tick_interval = ms(20));
            let config_b = tuned_config(vec![addr_a], 2, |s| s.tick_interval = ms(20));

            let (rt_a, client_a) = GossipRuntime::from_parts(
                UdpTransport(sock_a),
                TokioClock::new(),
                config_a,
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, client_b) = GossipRuntime::from_parts(
                UdpTransport(sock_b),
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
            use tokio::sync::oneshot;

            let router = SimRouter::new();
            let addr = sock(40_300);
            let transport = router.bind(addr);

            let identity = NodeIdentity::new(NodeId(0xCAFE), 7);
            let store = store_for(identity);
            let agg = Rc::new(InMemoryAggregateStore::new());

            let (admin_tx, admin_rx) = mpsc::channel::<AdminCommand>(4);

            let (rt, client) = GossipRuntime::from_parts_with_admin(
                transport,
                TokioClock::from_millis(0),
                sim_config(vec![sock(40_301)], 1),
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

async fn admin_snapshot(admin_tx: &mpsc::Sender<AdminCommand>) -> AdminSnapshot {
    use tokio::sync::oneshot;
    let (reply_tx, reply_rx) = oneshot::channel();
    admin_tx
        .send(AdminCommand::Snapshot { reply: reply_tx })
        .await
        .expect("admin command channel open");
    reply_rx.await.expect("runtime replied to snapshot")
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
        let cfg_sender = tuned_config(peer_addrs.clone(), case.seed, |s| {
            s.fanout = FANOUT.try_into().unwrap();
        });
        let agg_sender = Rc::new(InMemoryAggregateStore::new());
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
            let cfg = sim_config(Vec::new(), 1000 + i as u64);
            let (rt, client) = GossipRuntime::from_parts(
                router.bind(*addr),
                TokioClock::from_millis(0),
                cfg,
                store_for(id),
                Rc::new(InMemoryAggregateStore::new()),
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

// -- Encode and send ----------------------------------------------------------

fn node(id: u128) -> NodeIdentity {
    NodeIdentity::new(NodeId(id), 1)
}

/// A runtime on a scripted transport with an admin channel, after it has spent
/// its immediate first tick with nothing to send. Heartbeats then fire at
/// t = 100, 200, … ms.
async fn mocked(
    wire: &Rc<Wire>,
    store: CellStore,
    config: ValidGossipConfig,
) -> (
    GossipClient,
    mpsc::Sender<AdminCommand>,
    JoinHandle<Result<(), GossipError>>,
) {
    let (admin_tx, admin_rx) = mpsc::channel(4);
    let (rt, client) = GossipRuntime::from_parts_with_admin(
        MockTransport(wire.clone()),
        TokioClock::from_millis(0),
        config,
        store,
        Rc::new(InMemoryAggregateStore::new()),
        Some(admin_rx),
    );
    let runtime = tokio::task::spawn_local(rt.run(futures::stream::empty()));
    sim_advance_ticks(Duration::from_millis(10), 1).await;
    (client, admin_tx, runtime)
}

/// One hit on `key` with a limit so high that no threshold round runs.
async fn hit(client: &GossipClient, key: u128) {
    client
        .record(0xBEE, KeyHash(key), 0, 1, 1_000_000, 0)
        .await
        .unwrap();
}

async fn heartbeats(n: u32) {
    sim_advance_ticks(Duration::from_millis(100), n).await;
}

/// Exactly `count` log lines carry `code`, and each holds every one of
/// `fields`.
fn assert_logged(logs: &str, code: &str, count: usize, fields: &[&str]) {
    let code = format!("code=\"{code}\"");
    let lines: Vec<&str> = logs.lines().filter(|line| line.contains(&code)).collect();
    assert_eq!(lines.len(), count, "{logs}");
    for line in lines {
        for field in fields {
            assert!(holds(line, field), "{field} missing from {line}");
        }
    }
}

/// `line` holds `field`. A `key=value` field must be a whole token, so
/// `cluster_id_hash=1` does not pass on `cluster_id_hash=193`; a bare `key=`
/// or any other text may appear anywhere.
fn holds(line: &str, field: &str) -> bool {
    match field.split_once('=') {
        Some((_, value)) if !value.is_empty() => line.split(' ').any(|token| token == field),
        _ => line.contains(field),
    }
}

/// A heartbeat that sends one packet (held, if the socket refuses it), then
/// twelve requests at one instant against a rule whose error budget is one
/// hit (limit 100), then the snapshot. The third request crosses the budget
/// with no emit yet; if that round settles, every later crossing falls inside
/// the 5 ms emit floor, so one threshold round runs.
async fn threshold_rounds_in_a_burst(peers: Vec<SocketAddr>, refuse: bool) -> AdminSnapshot {
    let wire = Rc::new(Wire::default());
    wire.refuse.set(refuse);
    let (client, admin, _runtime) = mocked(&wire, store_for(node(0xA)), sim_config(peers, 1)).await;
    hit(&client, 1).await;
    heartbeats(1).await;
    for _ in 0..12 {
        client
            .record(0xC0DE, KeyHash(9), 0, 1, 100, 10)
            .await
            .unwrap();
    }
    admin_snapshot(&admin).await
}

/// A lone node settles the error budget although it has no one to send
/// to, so a burst runs one threshold round, not one per request.
#[test]
fn lone_node_settles_the_error_budget() {
    let snapshot = run_paused(threshold_rounds_in_a_burst(vec![], false));
    assert_eq!(snapshot.threshold_fires, 1);
}

/// A round that falls due while a packet is held still settles and stamps
/// the emit floor, so a blocked socket does not turn every later request into
/// another threshold round (and another refused retry).
#[test]
fn blocked_socket_does_not_fire_a_threshold_round_per_request() {
    let snapshot = run_paused(threshold_rounds_in_a_burst(vec![sock(2)], true));
    assert_eq!(snapshot.threshold_fires, 1);
    // The heartbeat's refusal, then the threshold round's refused retry.
    assert_eq!(snapshot.counters.send_blocked, 2);
}

/// A refused packet is held and retried first on every later round,
/// unchanged although newer counts arrived; nothing encodes over it, and the
/// round that finally sends it goes on to gossip. One `u32` cell fills a
/// packet, so the refusal cuts a two-packet frame after its first packet.
#[test]
fn refused_send_holds_one_packet() {
    run_paused(async {
        let wire = Rc::new(Wire::default());
        wire.refuse.set(true);
        let peers = vec![sock(2), sock(3)];
        let config = tuned_config(peers, 1, |s| s.max_payload_bytes = MIN_PAYLOAD_BYTES);
        let (client, admin, _runtime) = mocked(&wire, store_for(node(0xA)), config).await;
        hit(&client, 1).await;
        hit(&client, 2).await;
        heartbeats(1).await;
        let refused = wire.attempts.borrow()[0].clone();
        assert!(!refused.accepted);
        assert_eq!(wire.attempts.borrow().len(), 1);

        // Both cells change, so any re-encode would differ from the held one.
        hit(&client, 1).await;
        hit(&client, 2).await;
        heartbeats(2).await;
        assert_eq!(*wire.attempts.borrow(), vec![refused.clone(); 3]);
        let counters = admin_snapshot(&admin).await.counters;
        assert_eq!((counters.send_blocked, counters.packets_sent), (3, 0));

        wire.refuse.set(false);
        heartbeats(1).await;
        let accepted = wire.accepted();
        // The held packet first, then two packets for each peer.
        assert_eq!(accepted.len(), 5, "{accepted:?}");
        assert_eq!(
            (accepted[0].dst, &accepted[0].bytes),
            (refused.dst, &refused.bytes)
        );
        assert_eq!(
            accepted.iter().filter(|a| a.bytes == refused.bytes).count(),
            1
        );
        let counters = admin_snapshot(&admin).await.counters;
        assert_eq!((counters.send_blocked, counters.packets_sent), (3, 5));
        let sent: u64 = accepted.iter().map(|a| a.bytes.len() as u64).sum();
        assert_eq!(counters.bytes_sent, sent);
    });
}

/// `send_blocked` logs the 1st, 2nd and 4th of five refusals, each naming the
/// sysctl and the size gabion asked for.
#[test]
fn send_blocked_warns_at_powers_of_two_naming_wmem_max() {
    let ((), logs) = with_logs(|| {
        run_paused(async {
            let wire = Rc::new(Wire::default());
            wire.refuse.set(true);
            let (client, admin, _runtime) =
                mocked(&wire, store_for(node(0xA)), sim_config(vec![sock(2)], 1)).await;
            hit(&client, 1).await;
            heartbeats(5).await;
            assert_eq!(admin_snapshot(&admin).await.counters.send_blocked, 5);
        })
    });
    assert_logged(
        &logs,
        "send_blocked",
        3,
        &[
            "sysctl=\"net.core.wmem_max\"",
            "send_blocked_total=",
            "requested_send_buffer_bytes=1048576",
            "granted_send_buffer_bytes=212992",
        ],
    );
}

/// A send error drops that one packet: the runtime keeps running
/// and keeps gossiping with the other peer.
#[test]
fn send_error_to_one_peer_keeps_gossiping() {
    let ((), logs) = with_logs(|| {
        run_paused(async {
            let wire = Rc::new(Wire::default());
            wire.forbidden.set(Some(sock(2)));
            let peers = vec![sock(2), sock(3)];
            let (client, admin, _runtime) =
                mocked(&wire, store_for(node(0xA)), sim_config(peers, 1)).await;
            hit(&client, 1).await;
            heartbeats(3).await;
            let to_other = wire.accepted().iter().filter(|a| a.dst == sock(3)).count();
            assert_eq!(to_other, 3);
            let counters = admin_snapshot(&admin).await.counters;
            assert_eq!((counters.send_errors, counters.packets_sent), (3, 3));
        })
    });
    let peer = format!("peer={}", sock(2));
    assert_logged(&logs, "send_error", 2, &[&peer, "send_errors_total="]);
}

/// A version 1 header: magic, version 1, and zeros.
fn v1_packet() -> Vec<u8> {
    let mut bytes = vec![0; wire::HEADER_LEN];
    bytes[0..4].copy_from_slice(&wire::MAGIC.to_le_bytes());
    bytes[4..6].copy_from_slice(&1_u16.to_le_bytes());
    bytes
}

/// Send `packets` from a raw socket at `sock(9)` to a runtime of cluster
/// `0xC1` that accepts 1,400-byte packets, one heartbeat apart, and return
/// the runtime's counters.
async fn send_raw(packets: &[Vec<u8>]) -> GossipCounters {
    let router = SimRouter::new();
    let target = sock(44_000);
    let (admin, admin_rx) = mpsc::channel(4);
    let (rt, client) = GossipRuntime::from_parts_with_admin(
        router.bind(target),
        TokioClock::from_millis(0),
        tuned_config(vec![], 1, |s| s.max_payload_bytes = 1400),
        store_for(node(0xA)),
        Rc::new(InMemoryAggregateStore::new()),
        Some(admin_rx),
    );
    let runtime = tokio::task::spawn_local(rt.run(futures::stream::empty()));
    let raw = router.bind(sock(9));
    for packet in packets {
        raw.try_send_to(packet, target).unwrap();
        heartbeats(1).await;
    }
    let counters = admin_snapshot(&admin).await.counters;
    client.shutdown().await.unwrap();
    let _ = runtime.await;
    counters
}

/// Each decode cause counts and logs on its own, so four framing errors
/// before them do not silence the first version, cluster and size drops.
#[test]
fn each_decode_cause_has_its_own_throttle() {
    let foreign = wire::write_header(
        &Header {
            cluster_id_hash: 0xC2.try_into().unwrap(),
            sender: node(0xB),
            sender_zone: 0,
            acks: AckInfo::default(),
            payload: CellPayload::Sweep,
        },
        0,
    );
    let mut packets = vec![vec![0; 10]; 4];
    packets.extend([v1_packet(), foreign.to_vec(), vec![0; 2_000]]);
    let (counters, logs) = with_logs(|| run_paused(send_raw(&packets)));
    assert_eq!(
        (
            counters.decode_rejects_framing,
            counters.decode_rejects_peer_limits,
            counters.decode_rejects_version,
            counters.decode_rejects_cluster,
        ),
        (4, 1, 1, 1)
    );
    assert_eq!(counters.decode_rejects(), 7);
    let lines = |fields: &[&str]| {
        logs.lines()
            .filter(|line| fields.iter().all(|field| holds(line, field)))
            .count()
    };
    let framing = ["reason=\"framing\"", "packets are at most 1400 bytes"];
    assert_eq!(lines(&framing), 3, "{logs}");
    assert_eq!(lines(&["code=\"peer_version_mismatch\""]), 1, "{logs}");
    let cluster = [
        "reason=\"cluster\"",
        "peer_cluster=0xc2",
        "local_cluster=0xc1",
    ];
    assert_eq!(lines(&cluster), 1, "{logs}");
    assert_eq!(
        lines(&["reason=\"peer_limits\"", "length=2000"]),
        1,
        "{logs}"
    );
}

/// A peer past `gossip.peer_capacity` is declined: counted, logged once, and
/// never sent to. The counts it sends are still merged.
#[test]
fn full_table_declines_and_still_merges() {
    let (a, b, c) = (sock(46_000), sock(46_001), sock(46_002));
    let ((), logs) = with_logs(|| {
        run_paused(async {
            let router = SimRouter::new();
            let agg_a = Rc::new(InMemoryAggregateStore::new());
            let (admin, admin_rx) = mpsc::channel(4);
            let (rt_a, client_a) = GossipRuntime::from_parts_with_admin(
                router.bind(a),
                TokioClock::from_millis(0),
                tuned_config(vec![], 1, |s| s.peer_capacity = NonZeroU16::MIN),
                store_for(node(0xA)),
                agg_a.clone(),
                Some(admin_rx),
            );
            let events = futures::stream::iter([
                PeerEvent::Added(Peer::new(b)),
                PeerEvent::Added(Peer::new(c)),
            ]);
            let h_a = tokio::task::spawn_local(rt_a.run(events));
            let (rt_c, client_c) = GossipRuntime::from_parts(
                router.bind(c),
                TokioClock::from_millis(0),
                sim_config(vec![a], 2),
                store_for(node(0xC)),
                Rc::new(InMemoryAggregateStore::new()),
            );
            let h_c = tokio::task::spawn_local(rt_c.run(futures::stream::empty()));
            hit(&client_a, 1).await;
            hit(&client_c, 2).await;
            heartbeats(3).await;
            let snapshot = admin_snapshot(&admin).await;
            assert_eq!(snapshot.counters.peers_declined, 1);
            assert_eq!(
                snapshot.peers.iter().map(|p| p.addr).collect::<Vec<_>>(),
                [b]
            );
            assert_eq!(agg_a.total(), 2, "A counts its own hit and the one C sent");
            assert_eq!(
                router.received_count(c),
                0,
                "A never sends to the declined peer"
            );
            client_a.shutdown().await.unwrap();
            client_c.shutdown().await.unwrap();
            let _ = h_a.await;
            let _ = h_c.await;
        })
    });
    assert_logged(
        &logs,
        "peer_table_full",
        1,
        &[
            &format!("peer={c}"),
            "capacity=1",
            "declined_total=1",
            "config_key=\"gossip.peer_capacity\"",
        ],
    );
}

/// A packet of another protocol version names that version.
#[test]
fn bad_version_reports_the_peer_version() {
    let (counters, logs) = with_logs(|| run_paused(send_raw(&[v1_packet()])));
    assert_eq!(counters.decode_rejects_version, 1);
    assert_logged(
        &logs,
        "peer_version_mismatch",
        1,
        &[
            "peer_version=1",
            "local_version=2",
            "reason=\"version\"",
            "rejected_total=1",
            &format!("peer={}", sock(9)),
        ],
    );
}

/// A node on the default cluster id says so; a node on its own id says
/// nothing.
#[test]
fn default_cluster_id_is_logged_only_on_the_default() {
    let ((), logs) = with_logs(|| {
        tuned_config(vec![], 1, |s| {
            s.cluster_id_hash = Settings::default().cluster_id_hash;
        })
        .note_default_cluster_id();
        sim_config(vec![], 1).note_default_cluster_id();
    });
    assert_logged(
        &logs,
        "default_cluster_id",
        1,
        &["cluster_id_hash=1", "config_key=\"gossip.cluster_id_hash\""],
    );
}

/// `socket_buffer_capped` fires when the kernel grants less than the setting
/// asked for, naming the setting in every spelling and the sysctl; a real
/// socket gets the configured request, not the default, read back as granted
/// rather than as Linux reports it, and the minimal buffers a lone node asks
/// for never warn.
#[cfg(feature = "transport-udp")]
#[test]
fn socket_buffer_warning_when_granted_is_smaller() {
    use super::transport::{grant, warn_if_capped};
    use crate::settings::Setting;

    let requested = 65_536.try_into().unwrap();
    for (granted, warnings) in [(65_535, 1), (65_536, 0), (131_072, 0)] {
        let ((), logs) = with_logs(|| {
            let setting = Setting::GossipRecvBufferBytes;
            warn_if_capped("receive", "net.core.rmem_max", setting, requested, granted);
        });
        assert_logged(
            &logs,
            "socket_buffer_capped",
            warnings,
            &[
                "gossip.recv_buffer_bytes",
                "GABION_GOSSIP_RECV_BUFFER_BYTES",
                "gabion_gossip_recv_buffer_bytes",
                "sysctl=\"net.core.rmem_max\"",
            ],
        );
    }

    let config = tuned_config(vec![], 0, |s| {
        s.recv_buffer_bytes = 65_536.try_into().unwrap();
        s.send_buffer_bytes = ((1 << 30) - 1).try_into().unwrap();
    });
    let (receive, logs) = with_logs(|| {
        run_paused(async {
            let transport = UdpTransport::bind(sock(0), &config).await.unwrap();
            socket2::SockRef::from(&transport.0)
                .recv_buffer_size()
                .unwrap()
        })
    });
    assert_eq!(grant(receive), 65_536);
    assert_logged(
        &logs,
        "socket_buffer_capped",
        1,
        &[
            "buffer=\"send\"",
            "requested_bytes=1073741823",
            "sysctl=\"net.core.wmem_max\"",
            "gossip.send_buffer_bytes",
        ],
    );

    let minimal = config.with_minimal_socket_buffers();
    let ((), logs) = with_logs(|| {
        run_paused(async {
            UdpTransport::bind(sock(0), &minimal).await.unwrap();
        })
    });
    assert_logged(&logs, "socket_buffer_capped", 0, &[]);
}

#[cfg(feature = "transport-udp")]
#[test]
fn enobufs_is_held_like_a_full_buffer() {
    use std::io;

    use super::transport::refusal;

    let enobufs = io::Error::from_raw_os_error(libc::ENOBUFS);
    assert_eq!(refusal(enobufs).kind(), io::ErrorKind::WouldBlock);
    let denied = io::ErrorKind::PermissionDenied.into();
    assert_eq!(refusal(denied).kind(), io::ErrorKind::PermissionDenied);
}

/// A deferred round that overflows a receiver's queue drops the excess and
/// says so: the packet is not counted delivered, so the visualiser's logging
/// transport reports it as dropped, and the receiver gets only what fit.
#[test]
fn router_overflow_is_reported_as_a_drop() {
    let router = SimRouter::with_channel_capacity(1);
    let (src, dst) = (router.bind(sock(1)), router.bind(sock(2)));
    router.begin_deferred();
    for _ in 0..2 {
        assert_eq!(src.try_send_to(b"gossip", dst.addr()).unwrap(), 6);
    }
    assert_eq!(router.received_count(dst.addr()), 1);
    router.release_deferred();
    let mut buf = [0; 16];
    let first = dst.recv_from(&mut buf).now_or_never();
    assert_eq!(first.map(Result::unwrap), Some((6, src.addr())));
    assert!(dst.recv_from(&mut buf).now_or_never().is_none());
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
            let cfg = tuned_config(peers, 0xD0D0 + i as u64, |s| {
                s.fanout = nodes.saturating_sub(1).clamp(1, 4).try_into().unwrap();
                s.max_cells_per_tick = 32.try_into().unwrap();
            });
            let agg = Rc::new(InMemoryAggregateStore::new());
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
                sim_config(Vec::new(), 1),
                store_for(id_local),
                Rc::new(InMemoryAggregateStore::new()),
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
            let cfg_a = tuned_config(vec![addr_b], 1, |s| {
                s.tick_interval = ms(1_000);
                s.target_err_bps = 100;
            });
            let cfg_b = tuned_config(vec![addr_a], 2, |s| s.tick_interval = ms(1_000));

            let agg_a = Rc::new(InMemoryAggregateStore::new());
            let agg_b = Rc::new(InMemoryAggregateStore::new());

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

            // A tokio interval's first tick is immediate: let both runtimes
            // spend it while nothing is dirty, so it cannot deliver the hits.
            sim_advance_ticks(Duration::from_millis(10), 1).await;

            // rule_limit=100, N=1 peer, target=100 bps =>
            // ε = max(1, 100*100/(10000*1)) = 1. First hit puts pending=1
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

            let sum_b = agg_b.total();
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
            let cfg_a = tuned_config(vec![addr_b], 1, |s| {
                s.tick_interval = ms(10_000);
                s.min_emit_interval = ms(5);
            });
            let cfg_b = tuned_config(vec![addr_a], 2, |s| s.tick_interval = ms(10_000));

            let agg_a = Rc::new(InMemoryAggregateStore::new());
            let agg_b = Rc::new(InMemoryAggregateStore::new());

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
            let cfg_a = tuned_config(vec![addr_b], 1, |s| s.target_err_bps = 100);
            let cfg_b = sim_config(vec![addr_a], 2);

            let agg_a = Rc::new(InMemoryAggregateStore::new());
            let agg_b = Rc::new(InMemoryAggregateStore::new());

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

            let sum_b = agg_b.total();
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
                let cfg = tuned_config(peers, 0xBEEF + i as u64, |s| {
                    s.fanout = (NODES - 1).try_into().unwrap();
                    s.target_err_bps = TARGET_BPS;
                    s.min_emit_interval = ms(1);
                    s.max_cells_per_tick = 64.try_into().unwrap();
                });
                let agg = Rc::new(InMemoryAggregateStore::new());
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
