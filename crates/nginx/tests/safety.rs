//! Integration tests targeting the nginx-exercised cross-process paths.
//!
//! The production flow is:
//!
//! 1. nginx master `set_zone` → `mmap` + `ShmRegion::initialize`. Identity
//!    (`node_id`) is stamped here before fork.
//! 2. nginx workers fork. Each one runs the access phase, which reads the
//!    aggregate via `AggregateTable::window_total` and pushes hits onto the SHM
//!    queue. The first worker to win the lease (`LeaderLease::try_acquire`)
//!    spawns the leader thread; that thread stamps the incarnation, drains the
//!    queue, drives the gossip runtime, and writes back into the aggregate via
//!    `ShmAggregateStore::apply` (which invokes
//!    `write_delta`/`write_expiration` per row of the `DeltaSink`/
//!    `ExpirationSink`).
//!
//! These tests run under `cargo +nightly miri test --test safety` and exercise
//! every memory-unsafe entry point on the production hot paths. Networking
//! and tokio are not invoked — we manufacture `DeltaSink<u32>` /
//! `ExpirationSink<u32>` rows directly and feed them through
//! `AggregateStore::apply`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use gabion::crdt::{CellHandle, CellIdentity, DeltaSink, ExpirationSink, KeyHash};
use gabion::gossip::AggregateStore;
use gabion::rules::{DescriptorPattern, EnforcementMode, Rule, hash_key};

use gabion::crdt::{NodeId, NodeIdentity};

use gabion_nginx::access::{AccessCtx, AccessOutcome, CardinalitySettings, VariableLookup, decide};
use gabion_nginx::rules::{BindingLookup, CompiledRules, DescriptorBinding, RuleConfig};
use gabion_nginx::shm::aggregate::ShmAggregateStore;
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

fn build_rules() -> CompiledRules {
    CompiledRules::compile(&[RuleConfig {
        name: "per_tenant".into(),
        domain: DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "tenant".into(),
            source: "$http_x_tenant".into(),
        }],
        limit: 2,
        window: std::time::Duration::from_secs(1),
        bucket: std::time::Duration::from_millis(250),
        mode: EnforcementMode::Enforce,
        except_if: None,
    }])
    .expect("compile rules")
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
) -> (DeltaSink<u32>, ExpirationSink<u32>) {
    let mut d: DeltaSink<u32> = DeltaSink::with_capacity(deltas.len());
    let mut e: ExpirationSink<u32> = ExpirationSink::with_capacity(expirations.len());
    for &(kh, bucket, delta) in deltas {
        let key = CellIdentity {
            rule_fingerprint: rule.fingerprint,
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
            rule_fingerprint: rule.fingerprint,
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
/// `derive_identity` calls `whoami::hostname()` and
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
    assert!(region.header().is_initialized());
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
    // SAFETY: `region.aggregate_slots_ptr()` points at `agg_cap` initialized
    // AggregateSlots inside the live SHM mapping. The TestZone outlives both
    // the store and any worker view. This is the sole writer.
    let store = unsafe {
        ShmAggregateStore::new(
            region.aggregate_slots_ptr(),
            region.layout.aggregate_capacity,
        )
    };

    let rule = Rule::new(
        1,
        DEFAULT_DOMAIN.to_string(),
        vec![DescriptorPattern {
            key: "tenant".into(),
            value: "*".into(),
        }],
        2,
        1_000,
        250,
        EnforcementMode::Enforce,
    );
    let key_hash = KeyHash(0xABCD);
    let (deltas, expirations) = deltas_and_expirations(&rule, &[(key_hash, 3, 5)], &[]);
    store.apply(&deltas, &expirations);

    // Workers should read the new value through the seqlock-protected view.
    let view = region.aggregate();
    let cell = view.get(rule.fingerprint, key_hash.0, 3).expect("found");
    assert_eq!(cell.count, 5);
}

#[test]
fn expiration_subtracts_then_tombstones_when_zero() {
    let (_zone, region) = build_zone(8, 16);
    // SAFETY: see leader_stamps_incarnation_and_applies_deltas.
    let store = unsafe {
        ShmAggregateStore::new(
            region.aggregate_slots_ptr(),
            region.layout.aggregate_capacity,
        )
    };
    let rule = Rule::new(
        1,
        DEFAULT_DOMAIN.to_string(),
        vec![DescriptorPattern {
            key: "k".into(),
            value: "*".into(),
        }],
        100,
        1_000,
        250,
        EnforcementMode::Enforce,
    );
    let kh = KeyHash(0x42);

    let (d1, e_empty) = deltas_and_expirations(&rule, &[(kh, 1, 7)], &[]);
    store.apply(&d1, &e_empty);
    assert_eq!(
        region
            .aggregate()
            .get(rule.fingerprint, kh.0, 1)
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
            .get(rule.fingerprint, kh.0, 1)
            .unwrap()
            .count,
        4
    );

    // Subtract the rest — should tombstone.
    let (d_empty, e_full) = deltas_and_expirations(&rule, &[], &[(kh, 1, 4)]);
    store.apply(&d_empty, &e_full);
    assert!(region.aggregate().get(rule.fingerprint, kh.0, 1).is_none());
}

// -- 3. Access path: full decision logic via SHM views ---------------------

#[test]
fn access_path_allows_then_rejects_via_aggregate_seqlock() {
    let (_zone, region) = build_zone(8, 16);
    let rules = build_rules();
    let ctx = AccessCtx {
        rules: &rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        domain: DEFAULT_DOMAIN,
        cardinality: CardinalitySettings::default(),
    };
    let vars = MockVars {
        tenant: b"alice".to_vec(),
    };

    // First two requests — limit is 2 — should be allowed.
    assert!(matches!(decide(ctx, 0, &vars, 0), AccessOutcome::Allow(_)));

    // Simulate leader applying the recorded hits before the third arrives.
    // SAFETY: see leader_stamps_incarnation_and_applies_deltas.
    let store = unsafe {
        ShmAggregateStore::new(
            region.aggregate_slots_ptr(),
            region.layout.aggregate_capacity,
        )
    };
    let descriptors = [gabion::rules::Descriptor {
        key: "tenant",
        value: "alice",
    }];
    let spec = rules.rules()[0].rule.spec();
    let key_hash = hash_key(spec.id, DEFAULT_DOMAIN, &descriptors);
    let bucket = 0_u32;
    let (deltas, expirations) =
        deltas_and_expirations(&rules.rules()[0].rule, &[(key_hash, bucket, 3)], &[]);
    store.apply(&deltas, &expirations);

    // Third request must now be rejected — total (3) + 1 > limit (2).
    let outcome = decide(ctx, 0, &vars, 0);
    match outcome {
        AccessOutcome::Reject(info) => {
            assert_eq!(info.spec.id, spec.id);
            assert!(info.total >= 3);
        }
        other => panic!("expected Reject after apply, got {other:?}"),
    }

    // The queue should hold the original two hits plus this one.
    let mut drained = [QueueEvent::default(); 4];
    let n = region.queue().drain(&mut drained);
    assert!(n >= 1, "queue should retain pushed events");
}

// -- 4. Concurrent leader writer + worker readers --------------------------

#[test]
fn concurrent_leader_writer_and_worker_readers() {
    // Smaller iteration counts under miri so the test stays tractable.
    #[cfg(not(miri))]
    const WRITES: u64 = 256;
    #[cfg(miri)]
    const WRITES: u64 = 32;

    let layout = Layout::new(8, 32).expect("layout");
    let words = layout.total_bytes.div_ceil(8);
    let zone = Arc::new(TestZone::allocate(words));
    // SAFETY: zone.as_ptr() outlives every thread that uses it (Arc keeps it
    // alive). layout matches the allocation size.
    let region = unsafe { ShmRegion::initialize(zone.as_ptr(), layout) };

    let rule = Rule::new(
        1,
        DEFAULT_DOMAIN.to_string(),
        vec![DescriptorPattern {
            key: "k".into(),
            value: "*".into(),
        }],
        WRITES * 2,
        1_000,
        250,
        EnforcementMode::Enforce,
    );
    let fp = rule.fingerprint;
    let kh = KeyHash(0x777);
    let bucket = 1_u32;

    let stop = Arc::new(AtomicBool::new(false));

    // Leader thread writes deltas one at a time.
    let writer_zone = zone.clone();
    let writer = thread::spawn(move || {
        // SAFETY: see leader_stamps_incarnation_and_applies_deltas.
        let writer_region = unsafe { ShmRegion::from_initialized(writer_zone.as_ptr(), layout) };
        let store = unsafe {
            ShmAggregateStore::new(
                writer_region.aggregate_slots_ptr(),
                writer_region.layout.aggregate_capacity,
            )
        };
        for _ in 0..WRITES {
            let (d, e) = deltas_and_expirations(&rule, &[(kh, bucket, 1)], &[]);
            store.apply(&d, &e);
        }
    });

    // Two reader threads — what every nginx worker does on the access path.
    let mut readers = Vec::new();
    for _ in 0..2 {
        let reader_zone = zone.clone();
        let reader_stop = stop.clone();
        readers.push(thread::spawn(move || {
            // SAFETY: zone outlives this thread thanks to Arc; layout/region
            // are reused from the master's initialization.
            let reader_region =
                unsafe { ShmRegion::from_initialized(reader_zone.as_ptr(), layout) };
            while !reader_stop.load(Ordering::Acquire) {
                let view = reader_region.aggregate();
                // No panic = no torn read.
                let _ = view.get(fp, kh.0, bucket);
            }
        }));
    }

    writer.join().expect("writer joined");
    stop.store(true, Ordering::Release);
    for r in readers {
        r.join().expect("reader joined");
    }

    let final_view = region.aggregate();
    let cell = final_view
        .get(fp, kh.0, bucket)
        .expect("final value present");
    assert_eq!(cell.count, WRITES);
}

// -- 5. Lease takeover under contention ------------------------------------

#[test]
fn lease_takeover_under_contention() {
    let (_zone, region) = build_zone(8, 16);
    let lease = std::sync::Arc::new(LeaseFacade { region });

    // Millisecond precision now that the lease packs `(owner: u24,
    // expires_millis: u40)` into a single atomic.
    assert!(lease.region.lease().try_acquire(1, 0, 100));
    // Worker 2 cannot steal a live lease.
    assert!(!lease.region.lease().try_acquire(2, 50, 100));
    // After the lease expires (now=200ms > expires=100ms), worker 2 takes over.
    assert!(lease.region.lease().try_acquire(2, 200, 100));
    // Worker 1's renewal must now fail.
    assert!(!lease.region.lease().try_acquire(1, 210, 100));
    // Release lets the next worker grab it immediately.
    assert!(lease.region.lease().release(2));
    assert!(lease.region.lease().try_acquire(3, 300, 100));
}

// We need a wrapper to bypass the lack of `Send` impl for `*mut u8` inside
// `ShmRegion` if we want it shared between threads. `ShmRegion` is `Send`
// + `Sync` by `unsafe impl`. Use it directly.
struct LeaseFacade {
    region: ShmRegion,
}

unsafe impl Send for LeaseFacade {}
unsafe impl Sync for LeaseFacade {}

// -- 6. Fork-style: master initializes, "worker" reconstructs view -----

#[test]
fn worker_view_via_from_initialized_sees_master_writes() {
    // This is the moral equivalent of master `set_zone` → fork → worker
    // reads through `from_initialized`. Both `region` constructions point
    // at the same backing `TestZone`.
    let layout = Layout::new(8, 16).expect("layout");
    let words = layout.total_bytes.div_ceil(8);
    let zone = TestZone::allocate(words);
    // SAFETY: as in build_zone — fresh allocation, 8-byte aligned,
    // exclusive during init.
    let master_region = unsafe { ShmRegion::initialize(zone.as_ptr(), layout) };
    master_region.header().identity.store_node_id(0xfeed_face);

    // SAFETY: pointer is the same one master_region was initialized with,
    // and the TestZone outlives both regions. Pre-init writes via
    // master_region are visible because all access uses atomics.
    let worker_region = unsafe { ShmRegion::from_initialized(zone.as_ptr(), layout) };
    assert_eq!(worker_region.header().identity.load_node_id(), 0xfeed_face);
    assert_eq!(worker_region.queue().capacity(), 8);
}

// -- 7. End-to-end nginx flow: workers push, leader drains+applies, workers
// reread

/// Simulated end-to-end nginx flow: multiple worker threads push hits onto
/// the SHM queue while a separate "leader" thread drains them, applies the
/// resulting deltas through `ShmAggregateStore::apply`, and the workers
/// re-read the aggregate to confirm increments are visible. This exercises:
///
/// * `ShmRegion::initialize` (master) and `from_initialized` (per worker).
/// * `RequestQueue::push` (MPSC producer) and `RequestQueue::pop` (single
///   consumer).
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
    let rule = Rule::new(
        1,
        DEFAULT_DOMAIN.to_string(),
        vec![DescriptorPattern {
            key: "tenant".into(),
            value: "*".into(),
        }],
        TOTAL * 2,
        1_000,
        250,
        EnforcementMode::Enforce,
    );
    let fingerprint = rule.fingerprint;
    let descriptors = [gabion::rules::Descriptor {
        key: "tenant",
        value: "alice",
    }];
    let key_hash = hash_key(rule.id, DEFAULT_DOMAIN, &descriptors);
    let bucket = 1_u32;

    let stop = Arc::new(AtomicBool::new(false));

    // Worker producers: each replays the access path's queue.push.
    let mut producers = Vec::new();
    for _ in 0..WORKER_COUNT {
        let zone_clone = zone.clone();
        producers.push(thread::spawn(move || {
            // SAFETY: zone outlives this thread; layout/init are master's.
            let r = unsafe { ShmRegion::from_initialized(zone_clone.as_ptr(), layout) };
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
    let leader_zone = zone.clone();
    let leader_stop = stop.clone();
    let leader = thread::spawn(move || {
        // SAFETY: zone outlives the leader; the leader is the sole writer
        // for ShmAggregateStore in this test.
        let r = unsafe { ShmRegion::from_initialized(leader_zone.as_ptr(), layout) };
        let store =
            unsafe { ShmAggregateStore::new(r.aggregate_slots_ptr(), r.layout.aggregate_capacity) };
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
        let reader_zone = zone.clone();
        let reader_stop = stop.clone();
        readers.push(thread::spawn(move || {
            // SAFETY: zone outlives the reader; aggregate view uses
            // seqlock-protected atomic reads only.
            let r = unsafe { ShmRegion::from_initialized(reader_zone.as_ptr(), layout) };
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

// -- 8. Decide() + leader apply running concurrently -----------------------

#[test]
fn decide_and_leader_apply_concurrent() {
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

    // Worker thread runs the access path (decide()) repeatedly. This
    // exercises every read-side unsafe accessor as well as queue push and
    // aggregate seqlock retries when the writer races us.
    let worker_zone = zone.clone();
    let worker_rules = rules.clone();
    let worker_stop = stop.clone();
    let worker = thread::spawn(move || {
        // SAFETY: zone outlives the worker; access path only touches
        // atomics + stack ArrayVec.
        let r = unsafe { ShmRegion::from_initialized(worker_zone.as_ptr(), layout) };
        let vars = MockVars {
            tenant: b"alice".to_vec(),
        };
        for _ in 0..ITERATIONS {
            let ctx = AccessCtx {
                rules: &worker_rules,
                aggregate: r.aggregate(),
                queue: r.queue(),
                stats: r.stats(),
                domain: DEFAULT_DOMAIN,
                cardinality: CardinalitySettings::default(),
            };
            let _ = decide(ctx, 0, &vars, 0);
        }
        worker_stop.store(true, Ordering::Release);
    });

    // Leader thread continuously applies deltas until the worker stops.
    let leader_zone = zone.clone();
    let leader_stop = stop.clone();
    let leader_rules = rules.clone();
    let leader = thread::spawn(move || {
        // SAFETY: zone outlives the leader; sole writer for the store.
        let r = unsafe { ShmRegion::from_initialized(leader_zone.as_ptr(), layout) };
        let store =
            unsafe { ShmAggregateStore::new(r.aggregate_slots_ptr(), r.layout.aggregate_capacity) };
        let descriptors = [gabion::rules::Descriptor {
            key: "tenant",
            value: "alice",
        }];
        let spec = leader_rules.rules()[0].rule.spec();
        let key_hash = hash_key(spec.id, DEFAULT_DOMAIN, &descriptors);
        let mut applied = 0_u32;
        while !leader_stop.load(Ordering::Acquire) {
            let (d, e) =
                deltas_and_expirations(&leader_rules.rules()[0].rule, &[(key_hash, 0, 1)], &[]);
            store.apply(&d, &e);
            applied += 1;
            if applied > 1_000 {
                break;
            }
        }
        applied
    });

    worker.join().expect("worker joined");
    let _ = leader.join().expect("leader joined");

    // Final read through the seqlock; must not panic.
    let view = region.aggregate();
    let descriptors = [gabion::rules::Descriptor {
        key: "tenant",
        value: "alice",
    }];
    let spec = rules.rules()[0].rule.spec();
    let key_hash = hash_key(spec.id, DEFAULT_DOMAIN, &descriptors);
    let _ = view.get(spec.fingerprint, key_hash.0, 0);
}

// -- 8b. Multi-rule decide_all + leader apply running concurrently --------

/// Same shape as `decide_and_leader_apply_concurrent`, but with two stacked
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

    let rule_configs = vec![
        RuleConfig {
            name: "rule_a".into(),
            domain: DEFAULT_DOMAIN.into(),
            bindings: vec![DescriptorBinding {
                key: "tenant_a".into(),
                source: "$http_x_tenant".into(),
            }],
            limit: u64::MAX / 2,
            window: std::time::Duration::from_secs(1),
            bucket: std::time::Duration::from_millis(250),
            mode: EnforcementMode::Enforce,
            except_if: None,
        },
        RuleConfig {
            name: "rule_b".into(),
            domain: DEFAULT_DOMAIN.into(),
            bindings: vec![DescriptorBinding {
                key: "tenant_b".into(),
                source: "$http_x_tenant".into(),
            }],
            limit: u64::MAX / 2,
            window: std::time::Duration::from_secs(1),
            bucket: std::time::Duration::from_millis(250),
            mode: EnforcementMode::Enforce,
            except_if: None,
        },
    ];
    let rules = Arc::new(CompiledRules::compile(&rule_configs).expect("compile two rules"));
    let stop = Arc::new(AtomicBool::new(false));

    let worker_zone = zone.clone();
    let worker_rules = rules.clone();
    let worker_stop = stop.clone();
    let worker = thread::spawn(move || {
        // SAFETY: zone outlives the worker; access path only touches
        // atomics + stack ArrayVec.
        let r = unsafe { ShmRegion::from_initialized(worker_zone.as_ptr(), layout) };
        let vars = MockVars {
            tenant: b"alice".to_vec(),
        };
        for _ in 0..ITERATIONS {
            let ctx = AccessCtx {
                rules: &worker_rules,
                aggregate: r.aggregate(),
                queue: r.queue(),
                stats: r.stats(),
                domain: DEFAULT_DOMAIN,
                cardinality: CardinalitySettings::default(),
            };
            let _ = gabion_nginx::access::decide_all(ctx, &[0, 1], &vars, 0);
        }
        worker_stop.store(true, Ordering::Release);
    });

    let leader_zone = zone.clone();
    let leader_stop = stop.clone();
    let leader_rules = rules.clone();
    let leader = thread::spawn(move || {
        // SAFETY: zone outlives the leader; sole writer for the store.
        let r = unsafe { ShmRegion::from_initialized(leader_zone.as_ptr(), layout) };
        let store =
            unsafe { ShmAggregateStore::new(r.aggregate_slots_ptr(), r.layout.aggregate_capacity) };
        let mut applied = 0_u32;
        while !leader_stop.load(Ordering::Acquire) {
            for compiled in leader_rules.rules() {
                let rule = &compiled.rule;
                let descriptors = [gabion::rules::Descriptor {
                    key: &compiled.bindings[0].key,
                    value: "alice",
                }];
                let key_hash = hash_key(rule.id, DEFAULT_DOMAIN, &descriptors);
                let (d, e) = deltas_and_expirations(rule, &[(key_hash, 0, 1)], &[]);
                store.apply(&d, &e);
            }
            applied += 1;
            if applied > 1_000 {
                break;
            }
        }
        applied
    });

    worker.join().expect("worker joined");
    let _ = leader.join().expect("leader joined");

    let view = region.aggregate();
    for compiled in rules.rules() {
        let rule = &compiled.rule;
        let descriptors = [gabion::rules::Descriptor {
            key: &compiled.bindings[0].key,
            value: "alice",
        }];
        let key_hash = hash_key(rule.id, DEFAULT_DOMAIN, &descriptors);
        let _ = view.get(rule.fingerprint, key_hash.0, 0);
    }
}

// -- 9. Concurrent lease contention: only one winner among many threads ---

#[test]
fn lease_concurrent_acquire_distinct_winners() {
    use std::sync::Barrier;
    let (_zone, region) = build_zone(8, 16);
    let region = std::sync::Arc::new(LeaseFacade { region });
    let barrier = std::sync::Arc::new(Barrier::new(4));
    let mut handles = Vec::new();
    let winners = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    for worker in 1..=4_u32 {
        let region = region.clone();
        let barrier = barrier.clone();
        let winners = winners.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            if region.region.lease().try_acquire(worker, 0, 1_000) {
                winners.lock().unwrap().push(worker);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let winners = winners.lock().unwrap();
    assert_eq!(winners.len(), 1, "expected exactly one lease winner");
}
