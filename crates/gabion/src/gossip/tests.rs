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
use crate::gossip::sim::{SimRouter, sim_advance_ticks};
use crate::gossip::{AggregateStore, GossipConfig, GossipRuntime, TokioClock, parse_cluster_id};
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
        fanout: 1,
        tick_interval: Duration::from_millis(100),
        rng_seed: seed,
        ..GossipConfig::default()
    }
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

fn rule_fp(rule: u8) -> u128 {
    0xABC0 + rule as u128
}

fn key_hash(key: u8) -> KeyHash {
    KeyHash(0x1000 + key as u128)
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

// -- dropped packet repair --------------------------------------------------

// -- partition then heal ----------------------------------------------------

// -- peer sampling without replacement -------------------------------------

// -- coverage fanout: ⌈ln(n)+c⌉, scaled by cluster size --------------------

// -- UDP smoke --------------------------------------------------------------

/// The local-only arm never sends: two peers that know each other, one of
/// which records hits, exchange zero datagrams, and the other's aggregate
/// stays empty.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn local_hits_are_never_disseminated() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let router = SimRouter::new();
            let addr_a = sock(40_400);
            let addr_b = sock(40_401);
            let id_a = NodeIdentity::new(NodeId(0xA5), 1);
            let id_b = NodeIdentity::new(NodeId(0xB5), 1);
            let agg_a = Rc::new(InMemoryAggregateStore::<u32>::new());
            let agg_b = Rc::new(InMemoryAggregateStore::<u32>::new());

            let (rt_a, client_a) = GossipRuntime::from_parts(
                router.bind(addr_a),
                TokioClock::from_millis(0),
                sim_config(id_a, vec![addr_b], 1),
                store_for(id_a),
                agg_a.clone(),
            );
            let (rt_b, client_b) = GossipRuntime::from_parts(
                router.bind(addr_b),
                TokioClock::from_millis(0),
                sim_config(id_b, vec![addr_a], 2),
                store_for(id_b),
                agg_b.clone(),
            );
            let handle_a = tokio::task::spawn_local(rt_a.run(futures::stream::empty()));
            let handle_b = tokio::task::spawn_local(rt_b.run(futures::stream::empty()));

            client_a
                .record(0xFEED, KeyHash(7), 0, 5, 100, 0)
                .await
                .unwrap();
            sim_advance_ticks(Duration::from_millis(100), 40).await;

            assert_eq!(agg_a.total(), 5);
            assert_eq!(agg_b.total(), 0);
            assert_eq!(router.received_count(addr_a), 0);
            assert_eq!(router.received_count(addr_b), 0);

            client_a.shutdown().await.unwrap();
            client_b.shutdown().await.unwrap();
            let _ = handle_a.await;
            let _ = handle_b.await;
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

// -- Gap 2: without-replacement sampling distribution -----------------------

// -- Gap 3: decode rejection counter ----------------------------------------

// -- Gap 4: send queue backpressure (WouldBlock re-queue) -------------------

// -- Gap 5: DropProb i.i.d. packet loss -------------------------------------

// -- Gap 6: peer event lifecycle idempotence --------------------------------

/// `handle_peer_event` guards `Added` against duplicates (`peers.iter()
/// .any(...)`) and tolerates `Removed` of a peer that was never added
/// (`peers.iter().position(...).map(...)`). Neither edge is exercised by
/// existing tests. This case walks the sequence
/// `Added → Added → Removed → Removed → Added` and asserts the peer-list
/// length transitions are `1 → 1 → 0 → 0 → 1`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn peer_event_lifecycle_is_idempotent_and_tolerant() {
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
