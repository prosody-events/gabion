//! Integration tests targeting the nginx-exercised cross-process paths.
//!
//! The production flow is:
//!
//! 1. nginx master maps the shared zone; its `init_zone` callback runs
//!    `ShmRegion::initialize` and stamps the identity (`node_id`) before fork.
//! 2. nginx workers fork. Each one runs the access phase, which reads the
//!    aggregate via `AggregateTable::window_total` and pushes hits onto the SHM
//!    queue. Every worker spawns a leader thread; the one holding the lease
//!    (`LeaderLease::try_acquire`) stamps the incarnation, drains the queue,
//!    drives the gossip runtime, and writes back into the aggregate via
//!    `ShmAggregateStore::apply` (which folds each row of the `DeltaSink`/
//!    `ExpirationSink` into the library's table over the zone's slots).
//!
//! These tests run under `cargo +nightly miri test --test safety` and exercise
//! every memory-unsafe entry point on the production hot paths. Networking
//! and tokio are not invoked — we manufacture `DeltaSink` /
//! `ExpirationSink` rows directly and feed them through
//! `AggregateStore::apply`.
//!
//! Exempt: `module.rs`'s nginx FFI, which needs nginx itself and so can't run
//! under Miri. That includes `watch_for_shutdown`'s by-value read of
//! `ngx_exiting` on the worker's event thread (the only thread that writes
//! it), `init_zone`, which carves the region out of a zone nginx maps, and
//! the access handler's `$gabion_decided` slot, which lives in the
//! `r->variables` array nginx allocates per request.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread;
use std::time::Duration;

use gabion::crdt::ValidStorageConfig;
use gabion::crdt::{CellHandle, CellIdentity, DeltaSink, ExpirationSink, KeyHash};
use gabion::gossip::AggregateStore;
use gabion::rules::{EnforcementMode, Rule, RuleDef, hash_key};
use gabion::settings::Settings;

use gabion::crdt::{NodeId, NodeIdentity};

use gabion_nginx::access::{AccessCtx, AccessOutcome, VariableLookup, decide_all};
use gabion_nginx::rules::{
    BindingLookup, CompiledRules, NopBindingCompiler, RuleConfig, parse_rule_directive,
};
use gabion_nginx::shm::aggregate::ShmAggregateStore;
use gabion_nginx::shm::header::{SHM_MAGIC, SHM_VERSION};
use gabion_nginx::shm::queue::QueueEvent;
use gabion_nginx::shm::{Layout, ShmRegion};

const DEFAULT_DOMAIN: &str = "nginx";

/// Aligned heap allocation that mimics the `mmap` mapping nginx hands the
/// adapter in production. `Box<[u64]>` gives us 8-byte alignment — enough
/// for every atomic field in the region. `Box::into_raw` puts the pointer
/// at a stable address so Stacked-/Tree-Borrows reborrow rules aren't
/// violated when the test moves the wrapper.
pub struct TestZone {
    ptr: *mut u8,
    words: usize,
}

impl TestZone {
    pub fn allocate(words: usize) -> Self {
        let buf: Box<[u64]> = vec![0_u64; words].into_boxed_slice();
        let raw = Box::into_raw(buf);
        Self {
            ptr: raw as *mut u8,
            words,
        }
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for TestZone {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` came from `Box::into_raw` of a `Box<[u64]>`
        // with length `self.words`. The caller pairs the `TestZone` with
        // its `ShmRegion` such that the region is dropped first; no live
        // references remain when this Drop runs.
        unsafe {
            let slice = std::ptr::slice_from_raw_parts_mut(self.ptr as *mut u64, self.words);
            let _ = Box::from_raw(slice);
        }
    }
}

// SAFETY: `TestZone` is a thin owner of an aligned heap allocation. All
// access goes through atomics on the SHM region; the raw pointer's
// `*mut u8` is treated as a cross-thread share by convention (matches
// production `mmap` semantics).
unsafe impl Send for TestZone {}
unsafe impl Sync for TestZone {}

fn build_zone(queue_cap: usize, agg_cap: usize) -> (TestZone, ShmRegion) {
    let layout = Layout::new(queue_cap, agg_cap).expect("layout");
    let words = layout.total_bytes.div_ceil(8);
    let zone = TestZone::allocate(words);
    // SAFETY: zone.as_ptr() is 8-byte aligned, fresh, exclusive, and lives
    // until TestZone drops. layout matches the allocation size.
    let region = unsafe { ShmRegion::initialize(zone.as_ptr(), layout) };
    (zone, region)
}

fn storage() -> ValidStorageConfig {
    Settings::default().validate().expect("defaults validate").1
}

/// A rule of `rate` on `key`, in 250 ms buckets, built as both adapters
/// build theirs.
fn rule(key: &str, rate: &str) -> Rule {
    RuleDef {
        name: key,
        domain: DEFAULT_DOMAIN,
        patterns: &[(key, "*")],
        rate,
        window: None,
        bucket: Some(Duration::from_millis(250)),
        mode: EnforcementMode::Enforce,
    }
    .build(&storage().cardinality)
    .expect("rule")
}

/// One rule per `gabion_limit_rule` directive, compiled as nginx compiles
/// them.
fn compile(directives: &[&str]) -> CompiledRules {
    let configs: Vec<RuleConfig> = directives
        .iter()
        .map(|text| {
            let args: Vec<&str> = text.split_whitespace().collect();
            parse_rule_directive(&args).expect("directive")
        })
        .collect();
    CompiledRules::compile_with(&configs, &storage(), &mut NopBindingCompiler)
        .expect("compile rules")
}

fn build_rules() -> CompiledRules {
    compile(&["per_tenant tenant:$http_x_tenant rate=2r/s bucket=250ms"])
}

/// The access context over `region` for `rules`, with the default settings.
fn ctx<'a>(rules: &'a CompiledRules, region: &'a ShmRegion) -> AccessCtx<'a> {
    let storage = storage();
    AccessCtx {
        rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        cardinality: storage.cardinality,
        max_matched_rules: storage.max_matched_rules(),
    }
}

struct MockVars {
    tenant: Vec<u8>,
}

impl VariableLookup for MockVars {
    fn lookup(&self, binding: &BindingLookup) -> Option<&[u8]> {
        match binding {
            BindingLookup::IndexedVariable { name, .. } if name.as_ref() == "http_x_tenant" => {
                Some(&self.tenant)
            }
            _ => None,
        }
    }
}

/// Build a synthetic `(DeltaSink, ExpirationSink)` pair the gossip runtime
/// would normally hand the leader's store. Used to exercise
/// `ShmAggregateStore::apply` without booting tokio.
fn deltas_and_expirations(
    rule: &Rule,
    deltas: &[(KeyHash, u32, u32)],
    expirations: &[(KeyHash, u32, u32)],
) -> (DeltaSink, ExpirationSink) {
    let mut d = DeltaSink::with_capacity(deltas.len());
    let mut e = ExpirationSink::with_capacity(expirations.len());
    for &(kh, bucket, delta) in deltas {
        let key = CellIdentity {
            rule_fingerprint: rule.fingerprint(),
            key_hash: kh,
            bucket,
        };
        d.handles.push(CellHandle::default());
        d.keys.push(key);
        d.previous.push(0);
        d.current.push(delta);
        d.deltas.push(delta);
        d.applies_locally.push(1);
    }
    for &(kh, bucket, last_count) in expirations {
        let key = CellIdentity {
            rule_fingerprint: rule.fingerprint(),
            key_hash: kh,
            bucket,
        };
        e.handles.push(CellHandle::default());
        e.keys.push(key);
        e.last_counts.push(last_count);
        e.last_update_millis.push(0);
        e.applies_locally.push(1);
    }
    (d, e)
}

/// Deterministic identity for miri-compatible tests. The production
/// `gabion::identity::node_id` calls `whoami::hostname()` and
/// `UdpSocket::bind` (network I/O), which miri rejects without
/// `-Zmiri-disable-isolation`.
fn test_identity(node_id_lo: u64, incarnation: u32) -> NodeIdentity {
    NodeIdentity::new(NodeId(node_id_lo as u128), incarnation)
}

// -- 1. Master phase: SHM init + identity stamp ----------------------------

#[test]
fn master_stamps_node_id_and_initializes_region() {
    let (_zone, region) = build_zone(8, 16);
    let identity = test_identity(0xdead_beef_cafe, 1);
    region.header().identity.store_node_id(identity.node_id.0);
    assert_eq!(region.header().identity.load_node_id(), identity.node_id.0);
    assert_eq!(region.header().magic.load(Ordering::Acquire), SHM_MAGIC);
    assert_eq!(region.header().version.load(Ordering::Acquire), SHM_VERSION);
    // Geometry is reachable through the region.
    assert_eq!(region.queue().capacity(), 8);
    assert_eq!(region.aggregate().capacity(), 16);
}

// -- 2. Leader takeover: incarnation stamp + apply round-trip -------------

#[test]
fn leader_stamps_incarnation_and_applies_deltas() {
    let (_zone, region) = build_zone(8, 16);

    // Master would have stamped node_id; do it here.
    let identity = test_identity(0xc0ffee, 1);
    region.header().identity.store_node_id(identity.node_id.0);

    // Leader stamps the incarnation.
    let incarnation = 42_u32;
    region.header().identity.store_incarnation(incarnation);
    assert_eq!(region.header().identity.load_incarnation(), incarnation);

    // Build the store and feed it a synthetic delta batch.
    let store = ShmAggregateStore::new(region);

    let rule = rule("tenant", "2r/s");
    let key_hash = KeyHash(0xABCD);
    let (deltas, expirations) = deltas_and_expirations(&rule, &[(key_hash, 3, 5)], &[]);
    store.apply(&deltas, &expirations);

    // Workers should read the new value through the seqlock-protected view.
    let view = region.aggregate();
    let cell = view.get(rule.fingerprint(), key_hash.0, 3).expect("found");
    assert_eq!(cell.count, 5);
}

#[test]
fn expiration_subtracts_then_empties_when_zero() {
    let (_zone, region) = build_zone(8, 16);
    let store = ShmAggregateStore::new(region);
    let rule = rule("k", "100r/s");
    let kh = KeyHash(0x42);

    let (d1, e_empty) = deltas_and_expirations(&rule, &[(kh, 1, 7)], &[]);
    store.apply(&d1, &e_empty);
    assert_eq!(
        region
            .aggregate()
            .get(rule.fingerprint(), kh.0, 1)
            .unwrap()
            .count,
        7
    );

    // Subtract part of the count.
    let (d_empty, e_partial) = deltas_and_expirations(&rule, &[], &[(kh, 1, 3)]);
    store.apply(&d_empty, &e_partial);
    assert_eq!(
        region
            .aggregate()
            .get(rule.fingerprint(), kh.0, 1)
            .unwrap()
            .count,
        4
    );

    // Subtract the rest — should empty the slot.
    let (d_empty, e_full) = deltas_and_expirations(&rule, &[], &[(kh, 1, 4)]);
    store.apply(&d_empty, &e_full);
    assert!(
        region
            .aggregate()
            .get(rule.fingerprint(), kh.0, 1)
            .is_none()
    );
}

// -- 3. Access path: full decision logic via SHM views ---------------------

#[test]
fn access_path_allows_then_rejects_via_aggregate_seqlock() {
    let (_zone, region) = build_zone(8, 16);
    let rules = build_rules();
    let ctx = ctx(&rules, &region);
    let vars = MockVars {
        tenant: b"alice".to_vec(),
    };
    let mut events = Vec::new();

    // The first request is under the limit of 2, so it is allowed.
    assert!(matches!(
        decide_all(ctx, &[0], &vars, 0, &mut events),
        AccessOutcome::Allow(_)
    ));

    // Simulate leader applying the recorded hits before the third arrives.
    let store = ShmAggregateStore::new(region);
    let descriptors = [gabion::rules::Descriptor {
        key: "tenant",
        value: "alice",
    }];
    let spec = rules.table().rules()[0].spec();
    let key_hash = hash_key(DEFAULT_DOMAIN, descriptors);
    let bucket = 0_u32;
    let (deltas, expirations) =
        deltas_and_expirations(&rules.table().rules()[0], &[(key_hash, bucket, 3)], &[]);
    store.apply(&deltas, &expirations);

    // Third request must now be rejected — total (3) + 1 > limit (2).
    let outcome = decide_all(ctx, &[0], &vars, 0, &mut events);
    match outcome {
        AccessOutcome::Reject(info) => {
            assert_eq!(info.spec.id, spec.id);
            assert!(info.total >= 3);
        }
        other => panic!("expected Reject after apply, got {other:?}"),
    }

    // The allowed request queued one event; the rejected one queued none.
    let mut drained = [QueueEvent::default(); 4];
    assert_eq!(region.queue().drain(&mut drained), 1);
}

// -- 4. Concurrent leader writer + worker readers --------------------------

/// The leader inserts and expires cells around a steady one in a small,
/// crowded table, so expiry keeps shifting cells back over freed slots
/// while workers read. A reader may miss a cell mid-move, but whatever it
/// does read must be exact: never torn, never another cell's count.
#[test]
fn readers_see_exact_counts_while_the_leader_moves_cells() {
    #[cfg(not(miri))]
    const ROUNDS: u32 = 2_000;
    #[cfg(miri)]
    const ROUNDS: u32 = 20;
    const STEADY: u32 = 1_000_000;

    let (_zone, region) = build_zone(8, 16);
    let rule = rule("k", &format!("{}r/s", u64::MAX / 1_000));
    let fp = rule.fingerprint();
    let steady = KeyHash(0x777);
    let store = ShmAggregateStore::new(region);
    let (d, e) = deltas_and_expirations(&rule, &[(steady, 0, STEADY)], &[]);
    store.apply(&d, &e);

    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let stop = stop.clone();
            thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    let view = region.aggregate();
                    if let Some(cell) = view.get(fp, steady.0, 0) {
                        assert_eq!(cell.count, u64::from(STEADY));
                    }
                    for bucket in 1..=8 {
                        if let Some(cell) = view.get(fp, steady.0, bucket) {
                            assert_eq!(cell.count, u64::from(bucket));
                        }
                    }
                }
            })
        })
        .collect();

    for _ in 0..ROUNDS {
        let cells: Vec<_> = (1..=8).map(|bucket| (steady, bucket, bucket)).collect();
        let (d, e) = deltas_and_expirations(&rule, &cells, &[]);
        store.apply(&d, &e);
        let (d, e) = deltas_and_expirations(&rule, &[], &cells);
        store.apply(&d, &e);
    }
    stop.store(true, Ordering::Release);
    for reader in readers {
        reader.join().expect("reader joined");
    }

    let view = region.aggregate();
    assert_eq!(
        view.get(fp, steady.0, 0).expect("steady").count,
        u64::from(STEADY)
    );
    assert!((1..=8).all(|bucket| view.get(fp, steady.0, bucket).is_none()));
}

// -- 5. Lease takeover under contention ------------------------------------

#[test]
fn lease_takeover_under_contention() {
    let (_zone, region) = build_zone(8, 16);
    let lease = region.lease();

    // Millisecond precision now that the lease packs `(owner: u24,
    // expires_millis: u40)` into a single atomic.
    assert!(lease.try_acquire(1, 0, 100));
    // Worker 2 cannot steal a live lease.
    assert!(!lease.try_acquire(2, 50, 100));
    // After the lease expires (now=200ms > expires=100ms), worker 2 takes over.
    assert!(lease.try_acquire(2, 200, 100));
    // Worker 1's renewal must now fail.
    assert!(!lease.try_acquire(1, 210, 100));
    // Release lets the next worker grab it immediately.
    assert!(lease.release(2));
    assert!(lease.try_acquire(3, 300, 100));
}

// -- 6. End-to-end nginx flow: workers push, leader drains+applies, workers
// reread

/// Simulated end-to-end nginx flow: multiple worker threads push hits onto
/// the SHM queue while a separate "leader" thread drains them, applies the
/// resulting deltas through `ShmAggregateStore::apply`, and the workers
/// re-read the aggregate to confirm increments are visible. This exercises:
///
/// * `ShmRegion::initialize` (master), whose region every worker copies.
/// * `RequestQueue::push` (MPSC producer) and `RequestQueue::pop`.
/// * `ShmAggregateStore::new` (leader) and `apply()` (per drained batch).
/// * `AggregateTable::get` and `window_total` (worker readers).
/// * All `// SAFETY:` blocks inside these methods.
#[test]
fn end_to_end_workers_push_leader_drains_workers_read() {
    #[cfg(not(miri))]
    const PER_WORKER: usize = 64;
    #[cfg(miri)]
    const PER_WORKER: usize = 8;
    const WORKER_COUNT: usize = 3;
    const TOTAL: u64 = (PER_WORKER * WORKER_COUNT) as u64;

    let layout = Layout::new(32, 32).expect("layout");
    let words = layout.total_bytes.div_ceil(8);
    let zone = Arc::new(TestZone::allocate(words));
    // SAFETY: see build_zone.
    let region = unsafe { ShmRegion::initialize(zone.as_ptr(), layout) };

    // Build a rule and its fingerprint+key the same way nginx would.
    let rule = rule("tenant", &format!("{}r/s", TOTAL * 2));
    let fingerprint = rule.fingerprint();
    let descriptors = [gabion::rules::Descriptor {
        key: "tenant",
        value: "alice",
    }];
    let key_hash = hash_key(DEFAULT_DOMAIN, descriptors);
    let bucket = 1_u32;

    let stop = Arc::new(AtomicBool::new(false));

    // Worker producers: each replays the access path's queue.push.
    let mut producers = Vec::new();
    for _ in 0..WORKER_COUNT {
        producers.push(thread::spawn(move || {
            let r = region;
            let queue = r.queue();
            for _ in 0..PER_WORKER {
                loop {
                    match queue.push(QueueEvent {
                        rule_fingerprint: fingerprint,
                        key_hash: key_hash.0,
                        bucket,
                        hits: 1,
                        rule_limit: 1_000,
                        now_millis: 0,
                    }) {
                        Ok(()) => break,
                        Err(_) => std::hint::spin_loop(),
                    }
                }
            }
        }));
    }

    // Leader drainer: pops from the queue, batches into one apply().
    let leader_stop = stop.clone();
    let leader = thread::spawn(move || {
        let r = region;
        let store = ShmAggregateStore::new(r);
        let queue = r.queue();
        let mut total = 0_u64;
        while total < TOTAL || !leader_stop.load(Ordering::Acquire) {
            let mut batched: u64 = 0;
            // Drain whatever's queued right now.
            while let Some(ev) = queue.pop() {
                batched = batched.saturating_add(ev.hits);
                if batched >= 16 {
                    break;
                }
            }
            if batched > 0 {
                let (d, e) = deltas_and_expirations(
                    &rule,
                    &[(KeyHash(key_hash.0), bucket, batched as u32)],
                    &[],
                );
                store.apply(&d, &e);
                total = total.saturating_add(batched);
            }
            if total >= TOTAL && leader_stop.load(Ordering::Acquire) {
                break;
            }
            std::thread::yield_now();
        }
        total
    });

    // Reader threads (mimic workers reading window_total during access).
    let mut readers = Vec::new();
    for _ in 0..2 {
        let reader_stop = stop.clone();
        readers.push(thread::spawn(move || {
            let r = region;
            let view = r.aggregate();
            while !reader_stop.load(Ordering::Acquire) {
                let _ = view.get(fingerprint, key_hash.0, bucket);
            }
        }));
    }

    for h in producers {
        h.join().expect("producer joined");
    }
    // Tell the leader to stop after it has drained everything.
    stop.store(true, Ordering::Release);
    let drained = leader.join().expect("leader joined");
    for r in readers {
        r.join().expect("reader joined");
    }
    assert_eq!(drained, TOTAL);

    let final_view = region.aggregate();
    let cell = final_view
        .get(fingerprint, key_hash.0, bucket)
        .expect("aggregate cell present");
    assert_eq!(cell.count, TOTAL);
}

// -- 7. decide_all + leader apply running concurrently ----------------------

#[test]
fn decide_all_and_leader_apply_concurrent() {
    // Smaller iteration counts under miri to keep runtime reasonable.
    #[cfg(not(miri))]
    const ITERATIONS: usize = 64;
    #[cfg(miri)]
    const ITERATIONS: usize = 8;

    let layout = Layout::new(32, 32).expect("layout");
    let words = layout.total_bytes.div_ceil(8);
    let zone = Arc::new(TestZone::allocate(words));
    // SAFETY: zone is fresh, 8-byte aligned, exclusive during init.
    let region = unsafe { ShmRegion::initialize(zone.as_ptr(), layout) };

    let rules = Arc::new(build_rules());
    let stop = Arc::new(AtomicBool::new(false));

    // Worker thread runs the access path (decide_all()) repeatedly. This
    // exercises every read-side unsafe accessor as well as queue push and
    // aggregate seqlock retries when the writer races us.
    let worker_rules = rules.clone();
    let worker_stop = stop.clone();
    let worker = thread::spawn(move || {
        let r = region;
        let vars = MockVars {
            tenant: b"alice".to_vec(),
        };
        let mut events = Vec::new();
        for _ in 0..ITERATIONS {
            let ctx = ctx(&worker_rules, &r);
            assert!(matches!(
                decide_all(ctx, &[0], &vars, 0, &mut events),
                AccessOutcome::Allow(_)
            ));
        }
        worker_stop.store(true, Ordering::Release);
    });

    // Leader thread applies a hit and its expiry until the worker stops, so
    // the cell keeps appearing and vanishing but never reaches the limit. It
    // applies at least once, so the final check sees an expiry, not a table
    // the leader never touched.
    let leader_stop = stop.clone();
    let leader_rules = rules.clone();
    let leader = thread::spawn(move || {
        let r = region;
        let store = ShmAggregateStore::new(r);
        let descriptors = [gabion::rules::Descriptor {
            key: "tenant",
            value: "alice",
        }];
        let key_hash = hash_key(DEFAULT_DOMAIN, descriptors);
        let hit = [(key_hash, 0, 1)];
        let (d, e) = deltas_and_expirations(&leader_rules.table().rules()[0], &hit, &hit);
        for _ in 0..=1_000 {
            store.apply(&d, &e);
            if leader_stop.load(Ordering::Acquire) {
                break;
            }
        }
    });

    worker.join().expect("worker joined");
    leader.join().expect("leader joined");

    // Every allowed request queued one event, up to the queue's capacity.
    let mut drained = [QueueEvent::default(); 32];
    assert_eq!(region.queue().drain(&mut drained), ITERATIONS.min(32));
    // Every hit the leader applied was expired again.
    let descriptors = [gabion::rules::Descriptor {
        key: "tenant",
        value: "alice",
    }];
    let spec = rules.table().rules()[0].spec();
    let key_hash = hash_key(DEFAULT_DOMAIN, descriptors);
    assert!(
        region
            .aggregate()
            .get(spec.fingerprint, key_hash.0, 0)
            .is_none()
    );
}

// -- 8b. Multi-rule decide_all + leader apply running concurrently --------

/// Same shape as `decide_all_and_leader_apply_concurrent`, but with two stacked
/// rules so the multi-rule queue-flush path is exercised under the
/// writer/reader race. Catches regressions in the queue-batching logic
/// that single-rule tests can't surface.
#[test]
fn decide_all_multi_rule_concurrent() {
    #[cfg(not(miri))]
    const ITERATIONS: usize = 64;
    #[cfg(miri)]
    const ITERATIONS: usize = 8;

    let layout = Layout::new(32, 32).expect("layout");
    let words = layout.total_bytes.div_ceil(8);
    let zone = Arc::new(TestZone::allocate(words));
    // SAFETY: zone is fresh, 8-byte aligned, exclusive during init.
    let region = unsafe { ShmRegion::initialize(zone.as_ptr(), layout) };

    let rules = Arc::new(compile(&[
        "rule_a tenant_a:$http_x_tenant rate=9223372036854775r/s bucket=250ms",
        "rule_b tenant_b:$http_x_tenant rate=9223372036854775r/s bucket=250ms",
    ]));
    let stop = Arc::new(AtomicBool::new(false));
    let worker_rules = rules.clone();
    let worker_stop = stop.clone();
    let worker = thread::spawn(move || {
        let r = region;
        let vars = MockVars {
            tenant: b"alice".to_vec(),
        };
        let mut events = Vec::new();
        for _ in 0..ITERATIONS {
            let ctx = ctx(&worker_rules, &r);
            assert!(matches!(
                decide_all(ctx, &[0, 1], &vars, 0, &mut events),
                AccessOutcome::Allow(_)
            ));
        }
        worker_stop.store(true, Ordering::Release);
    });
    let leader_stop = stop.clone();
    let leader_rules = rules.clone();
    let leader = thread::spawn(move || {
        let r = region;
        let store = ShmAggregateStore::new(r);
        for _ in 0..=1_000 {
            for rule in leader_rules.table().rules() {
                let descriptors = [gabion::rules::Descriptor {
                    key: &rule.descriptors()[0].key,
                    value: "alice",
                }];
                let key_hash = hash_key(DEFAULT_DOMAIN, descriptors);
                let (d, e) = deltas_and_expirations(rule, &[(key_hash, 0, 1)], &[]);
                store.apply(&d, &e);
            }
            if leader_stop.load(Ordering::Acquire) {
                break;
            }
        }
    });

    worker.join().expect("worker joined");
    leader.join().expect("leader joined");

    // The leader wrote each rule's cell, so the reads raced real writes.
    assert_eq!(region.aggregate().occupied(), 2);
    // Every allowed request queued one event per rule, up to the capacity.
    let mut drained = [QueueEvent::default(); 32];
    assert_eq!(region.queue().drain(&mut drained), (2 * ITERATIONS).min(32));
}

// -- 8. Concurrent lease contention: only one winner among many threads ---

#[test]
fn lease_concurrent_acquire_distinct_winners() {
    // A lost-update race shows in well under 1% of rounds, so run many.
    #[cfg(not(miri))]
    const ROUNDS: u64 = 4096;
    #[cfg(miri)]
    const ROUNDS: u64 = 4;

    let (_zone, region) = build_zone(8, 16);
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let winners: Arc<[AtomicU32]> = (0..ROUNDS).map(|_| AtomicU32::new(0)).collect();
    let handles: Vec<_> = (1..=4_u32)
        .map(|worker| {
            let (barrier, winners) = (barrier.clone(), winners.clone());
            thread::spawn(move || {
                for round in 0..ROUNDS {
                    barrier.wait();
                    // Each round starts after the previous round's lease
                    // expired.
                    if region.lease().try_acquire(worker, round * 2_000, 1_000) {
                        winners[round as usize].fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    for (round, won) in winners.iter().enumerate() {
        let won = won.load(Ordering::Relaxed);
        assert_eq!(won, 1, "round {round}: expected exactly one lease winner");
    }
}

// -- 9. A configured queue capacity bounds the ring ------------------------

/// `gabion_gossip_limit_queue_capacity` of 100 lays the zone out with a
/// 128-slot ring: producers fill exactly those slots, the rest drop, and the
/// leader pops every accepted event intact. Under Miri, every slot write
/// stays inside the region `Layout::for_zone` sized.
#[test]
fn custom_queue_capacity_bounds_the_ring() {
    const PER_PRODUCER: u64 = 100;
    let layout = Layout::for_zone(256 * 1024, 4096, 100).expect("layout");
    assert_eq!(layout.queue_capacity, 128);
    let zone = Arc::new(TestZone::allocate(layout.total_bytes.div_ceil(8)));
    // SAFETY: see build_zone.
    let region = unsafe { ShmRegion::initialize(zone.as_ptr(), layout) };
    let event = |producer: u64, i: u64| QueueEvent {
        rule_fingerprint: u128::from(producer) << 64 | u128::from(i),
        key_hash: u128::from(i) << 64 | u128::from(producer),
        bucket: i as u32,
        hits: producer + 1,
        rule_limit: i + 1,
        now_millis: producer * 1_000 + i,
    };
    let producers: Vec<_> = (0..2_u64)
        .map(|producer| {
            thread::spawn(move || {
                let queue = region.queue();
                (0..PER_PRODUCER)
                    .map(|i| event(producer, i))
                    .filter(|event| queue.push(*event).is_ok())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut accepted: Vec<QueueEvent> = producers
        .into_iter()
        .flat_map(|producer| producer.join().expect("producer joined"))
        .collect();
    let queue = region.queue();
    assert_eq!(accepted.len(), 128);
    assert_eq!(queue.dropped(), 2 * PER_PRODUCER - 128);
    let mut popped: Vec<QueueEvent> = std::iter::from_fn(|| queue.pop()).collect();
    let key = |event: &QueueEvent| (event.rule_fingerprint, event.key_hash);
    accepted.sort_by_key(key);
    popped.sort_by_key(key);
    assert_eq!(popped, accepted);
    drop(zone);
}
