//! Realistic hot-path benchmarks for `gabion::crdt::CellStore`.
//!
//! Each scenario models a state a production rate-limiter actually sits in,
//! rather than abstract "warm vs cold" knobs:
//!
//! * `ingest_local/steady_state`  — 95% updates on existing cells + 5% inserts;
//!   traffic Pareto-distributed across 8 rules and 4 active buckets.
//! * `ingest_local/traffic_burst` — 99% updates on a tight hot subset; a
//!   Slashdot/spike pattern under one rule.
//! * `ingest_local/cold_start`    — empty store filling under realistic key
//!   distribution (per-window startup).
//!
//! * `merge_remote/digest_repair` — what a digest-driven repair frame looks
//!   like: 50% no-op (we are ahead) + 40% real update + 10% insert, across 8
//!   peers.
//! * `merge_remote/bulk_fan_in`   — multiple peers gossiping overlapping cells
//!   (max-merge collapses duplicates).
//! * `merge_remote/all_noop`      — peer is behind us on every row.
//!
//! * `find/realistic_95_hit`      — what the rate-limit aggregator does on
//!   every request: 95% of queries hit an existing cell.
//! * `find/miss`                  — pure miss path, for the Robin Hood
//!   early-exit baseline.
//!
//! * `fill_gossip_frame/steady_state` — moderately loaded node: some dirty
//!   entries + repair lane filling the rest of a 128-cell frame budget.
//! * `fill_gossip_frame/repair_only`  — dirty rings empty (gossip went idle);
//!   pure repair sweep.
//!
//! * `expire/window_turnover`     — one bucket out of four ages out per call;
//!   the steady-state expiry pattern.
//! * `expire/no_expirations`      — scan-only cost, no frees.
//! * `expire/bucket_boundary`     — a production-sized bucket ages out and is
//!   freed in the runtime's batches.
//!
//! * `aggregate/window_total_95_5` — the aggregate table both adapters read on
//!   admission, in production steady state: one request's window total over 10
//!   buckets, 95% for a key active in the window, 5% for a key never seen.
//! * `aggregate/apply_one_second`  — the table's writer for one second of that
//!   traffic: 650 inserts plus 650 expiries.
//!
//! * `inbound/peer_lookup`         — a node with 300 peers takes a 2-cell
//!   packet from each in turn, through the public runtime.

use std::cell::Cell;
use std::hint::black_box;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use gabion::aggregate::Aggregate;
use gabion::crdt::{
    BucketEpoch, CellHandle, CellIdentity, CellStore, CompactCellKey, DeltaSink, ExpirationSink,
    Expiry, Incarnation, KeyHash, NodeId, NodeIdentity, NodeSlot, Observation, ObservationBatch,
    RuleSlot, ValidStorageConfig,
};
use gabion::defaults::{GOSSIP_EXPIRATION_BATCH, GOSSIP_MAX_PAYLOAD_BYTES};
use gabion::gossip::{AggregateStore, FixedClock, GossipRuntime, GossipTransport};
use gabion::rules::{EnforcementMode, RuleDef, RuleSpec, RuleTable};
use gabion::settings::Settings;

/// Build an `Observation` row with positional args. Used by the bench
/// fixtures, where the field-named struct literal would be very verbose.
#[inline]
fn obs_row(
    rule_fingerprint: u128,
    key_hash: KeyHash,
    bucket: BucketEpoch,
    origin: NodeId,
    incarnation: Incarnation,
    count: u32,
    last_update_millis: u64,
) -> Observation {
    Observation {
        rule_fingerprint,
        key_hash,
        bucket,
        origin,
        incarnation,
        count,
        last_update_millis,
    }
}
use gabion::wire::{
    AckInfo, CellPayload, FrameLimits, Header, PacketBuf, Packets, WireScratch, decode,
};

// ---------------------------------------------------------------------------
// SplitMix64 RNG. Deterministic so iteration order is reproducible.
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in [0, n).
    fn range(&mut self, n: u32) -> u32 {
        (self.next_u64() % (n as u64)) as u32
    }
}

// ---------------------------------------------------------------------------
// Fixture: a production-shaped single-node store.
// ---------------------------------------------------------------------------

const CELL_CAPACITY: u32 = 16_384;
const RULE_CAPACITY: u16 = 64;
const NODE_CAPACITY: u16 = 64; // 1 local + ~63 peers
const DIRTY_CAPACITY: u32 = 256;

/// Distinct rate-limit rules a node enforces. Production fleets typically
/// have anywhere from a handful to a few hundred; 8 is enough to exercise
/// the rule-dictionary lookup without dominating the bench.
const NUM_RULES: usize = 8;

/// Active bucket epochs in the live window. A 60 s window with 15 s
/// buckets gives 4 simultaneously live buckets.
const NUM_BUCKETS: usize = 4;

/// Size of the "hot" portion of the key space. ~5% of `WARM_KEYS`.
const HOT_KEYS: usize = 512;

/// Size of the "warm" portion (cells that exist but aren't hot). Together
/// with HOT_KEYS this fills ~50% of cell capacity, leaving room for the
/// cold-insert tail.
const WARM_KEYS: usize = 8_192;

const LOCAL_NODE: NodeId = NodeId(0x1111_2222_3333_4444_5555_6666_7777_8888);
const LOCAL_INCARNATION: Incarnation = 1;

fn local_identity() -> NodeIdentity {
    NodeIdentity::new(LOCAL_NODE, LOCAL_INCARNATION)
}

fn config() -> ValidStorageConfig {
    let nz = "bench capacities are non-zero";
    let settings = Settings {
        max_cells: CELL_CAPACITY.try_into().expect(nz),
        rule_dictionary_capacity: Some(RULE_CAPACITY.try_into().expect(nz)),
        node_dictionary_capacity: Some(NODE_CAPACITY.try_into().expect(nz)),
        peer_capacity: (NODE_CAPACITY - 1).try_into().expect(nz),
        local_dirty_capacity: DIRTY_CAPACITY.try_into().expect(nz),
        forwarded_dirty_capacity: DIRTY_CAPACITY.try_into().expect(nz),
        ..Settings::default()
    };
    settings.validate().expect("bench settings validate").1
}

/// `n` rules of `rate` in `bucket`s, each on its own key, built as the
/// adapters build theirs.
fn rules(n: usize, rate: &str, bucket: Duration, storage: &ValidStorageConfig) -> RuleTable {
    let keys: Vec<String> = (0..n).map(|i| format!("key{i}")).collect();
    let rules = keys
        .iter()
        .map(|key| {
            RuleDef {
                name: key,
                domain: "bench",
                patterns: &[(key, "*")],
                rate,
                window: None,
                bucket: Some(bucket),
                mode: EnforcementMode::Enforce,
            }
            .build(&storage.cardinality)
            .expect("bench rule builds")
        })
        .collect();
    RuleTable::new(rules, storage.rule_dictionary_capacity()).expect("bench rules fit")
}

fn peer_node(i: usize) -> NodeId {
    NodeId(0xAAAA_0000_0000_0000_0000_0000_0000_0000 ^ (i as u128))
}

/// A pre-built, populated store the bench can clone per-iteration.
struct Fixture {
    store: CellStore,
    rule_fps: Vec<u128>,
    rule_slots: Vec<RuleSlot>,
    local_node_slot: NodeSlot,
    hot_keys: Vec<u128>,
    warm_keys: Vec<u128>,
    /// Cells inserted under remote origins, addressed as `(key, rule_idx,
    /// bucket, peer_idx)`. Bench can sample these to drive merge_remote.
    remote_rows: Vec<RemoteRow>,
    peers: Vec<(NodeId, Incarnation)>,
}

#[derive(Clone, Copy)]
struct RemoteRow {
    key: u128,
    rule_idx: u32,
    bucket: BucketEpoch,
    peer_idx: u32,
}

/// Build a fresh store with N rules of a 60 s window in 15 s buckets, and
/// return the seed material the benches share.
fn new_store() -> (CellStore, Vec<u128>, Vec<RuleSlot>, NodeSlot) {
    let table = rules(NUM_RULES, "1000000r/m", Duration::from_secs(15), &config());
    let store = CellStore::new(config(), local_identity(), &table);
    let rule_fps: Vec<u128> = table
        .rules()
        .iter()
        .map(|rule| rule.fingerprint())
        .collect();
    let rule_slots = rule_fps
        .iter()
        .map(|&fp| store.rule_dictionary().find(fp).expect("rule interned"))
        .collect();
    let local_node_slot = store.local_node_slot();
    (store, rule_fps, rule_slots, local_node_slot)
}

/// Seed `count` cells under the local origin, sampling rule and bucket
/// from the realistic Pareto-shaped distributions.
fn seed_local(store: &mut CellStore, rule_fps: &[u128], keys: &[u128], count: usize, seed: u64) {
    let mut rng = Rng::new(seed);
    let mut obs = ObservationBatch::with_capacity(count);
    let mut sink = DeltaSink::with_capacity(count);
    for i in 0..count {
        // Walk keys deterministically so every key gets exactly one cell;
        // rotate rule/bucket to spread the population.
        let key = keys[i % keys.len()];
        let rule_idx = pick_rule(&mut rng) as usize;
        let bucket = pick_bucket(&mut rng);
        obs.push(obs_row(
            rule_fps[rule_idx],
            KeyHash(key),
            bucket,
            LOCAL_NODE,
            LOCAL_INCARNATION,
            1,
            0,
        ));
    }
    store.ingest_local(&obs, &mut sink);
}

/// Seed `count` cells under remote origins. Returns the rows so the
/// benchmark can re-target the same cells.
fn seed_remote(
    store: &mut CellStore,
    rule_fps: &[u128],
    keys: &[u128],
    peers: &[(NodeId, Incarnation)],
    count: usize,
    initial_count: u32,
    seed: u64,
) -> Vec<RemoteRow> {
    let mut rng = Rng::new(seed);
    let mut obs = ObservationBatch::with_capacity(count);
    let mut sink = DeltaSink::with_capacity(count);
    let mut rows = Vec::with_capacity(count);
    for i in 0..count {
        let key = keys[i % keys.len()];
        let rule_idx = pick_rule(&mut rng);
        let bucket = pick_bucket(&mut rng);
        let peer_idx = (i % peers.len()) as u32;
        let (origin, inc) = peers[peer_idx as usize];
        rows.push(RemoteRow {
            key,
            rule_idx,
            bucket,
            peer_idx,
        });
        obs.push(obs_row(
            rule_fps[rule_idx as usize],
            KeyHash(key),
            bucket,
            origin,
            inc,
            initial_count,
            0,
        ));
    }
    store.merge_remote(&obs, &mut sink);
    rows
}

/// Pareto-ish rule pick: 80% of traffic goes to the top 20% of rules.
fn pick_rule(rng: &mut Rng) -> u32 {
    let head = (NUM_RULES as u32 / 5).max(1);
    if rng.range(100) < 80 {
        rng.range(head)
    } else {
        rng.range(NUM_RULES as u32)
    }
}

/// Bucket pick: 90% in the most recent bucket, tail in older buckets.
fn pick_bucket(rng: &mut Rng) -> BucketEpoch {
    let r = rng.range(100);
    if r < 90 {
        0
    } else if r < 98 {
        1
    } else {
        rng.range(NUM_BUCKETS as u32)
    }
}

/// Pareto-ish key sampler: 80% of draws hit the hot set, 20% the warm set.
fn pareto_key(rng: &mut Rng, hot: &[u128], warm: &[u128]) -> u128 {
    if rng.range(100) < 80 {
        hot[rng.range(hot.len() as u32) as usize]
    } else {
        warm[rng.range(warm.len() as u32) as usize]
    }
}

/// Fresh key never previously inserted. Encoded outside the warm/hot
/// universe so it can't collide.
fn fresh_key(rng: &mut Rng) -> u128 {
    0xFFFF_0000_0000_0000_0000_0000_0000_0000_u128 | (rng.next_u64() as u128)
}

fn build_fixture() -> Fixture {
    let (mut store, rule_fps, rule_slots, local_node_slot) = new_store();

    // Pre-generate the key universe deterministically.
    let mut key_rng = Rng::new(0xA110_CA7E_FACE_BEEF);
    let mut hot_keys = Vec::with_capacity(HOT_KEYS);
    for _ in 0..HOT_KEYS {
        hot_keys.push((key_rng.next_u64() as u128) | ((key_rng.next_u64() as u128) << 64));
    }
    let mut warm_keys = Vec::with_capacity(WARM_KEYS);
    for _ in 0..WARM_KEYS {
        warm_keys.push((key_rng.next_u64() as u128) | ((key_rng.next_u64() as u128) << 64));
    }

    // Seed half the capacity with local cells: all hot keys + half the
    // warm keys. ~4608 cells out of 16384 capacity, leaving room for
    // cold-insert tails and remote merges to grow the population.
    let mut local_seed: Vec<u128> = hot_keys.to_vec();
    local_seed.extend(warm_keys.iter().take(WARM_KEYS / 2).copied());
    seed_local(
        &mut store,
        &rule_fps,
        &local_seed,
        local_seed.len(),
        0xD0D0_C0DE,
    );

    // Seed remote cells under 8 peers using the other half of warm keys.
    let peers: Vec<(NodeId, Incarnation)> = (0..8).map(|i| (peer_node(i), 1)).collect();
    let remote_seed: Vec<u128> = warm_keys.iter().skip(WARM_KEYS / 2).copied().collect();
    let remote_rows = seed_remote(
        &mut store,
        &rule_fps,
        &remote_seed,
        &peers,
        remote_seed.len(),
        100,
        0xFADE_C0DE,
    );

    Fixture {
        store,
        rule_fps,
        rule_slots,
        local_node_slot,
        hot_keys,
        warm_keys,
        remote_rows,
        peers,
    }
}

// ---------------------------------------------------------------------------
// ingest_local — per-request write path.
// ---------------------------------------------------------------------------

fn bench_ingest_local(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt/ingest_local");

    let fixture = build_fixture();

    // ─── steady_state ───────────────────────────────────────────────────
    // Realistic production mix: 95% updates on existing cells (Pareto
    // across hot+warm), 5% inserts on fresh keys. Multi-rule, multi-bucket.
    for &batch in &[1usize, 32, 128] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_function(BenchmarkId::new("steady_state", batch), |b| {
            b.iter_batched_ref(
                || {
                    (
                        fixture.store.clone(),
                        ObservationBatch::with_capacity(batch),
                        DeltaSink::with_capacity(batch),
                        Rng::new(0x1234_5678),
                    )
                },
                |(store, obs, sink, rng)| {
                    obs.clear();
                    sink.clear();
                    for _ in 0..batch {
                        let pick = rng.range(100);
                        let key = if pick < 95 {
                            pareto_key(rng, &fixture.hot_keys, &fixture.warm_keys)
                        } else {
                            fresh_key(rng)
                        };
                        let rule_idx = pick_rule(rng) as usize;
                        let bucket = pick_bucket(rng);
                        obs.push(obs_row(
                            fixture.rule_fps[rule_idx],
                            KeyHash(key),
                            bucket,
                            LOCAL_NODE,
                            LOCAL_INCARNATION,
                            1,
                            0,
                        ));
                    }
                    store.ingest_local(black_box(obs), black_box(sink));
                },
                BatchSize::SmallInput,
            );
        });
    }

    // ─── traffic_burst ──────────────────────────────────────────────────
    // Slashdot/spike: 99% of hits land on a 64-key hot subset; one rule;
    // one bucket. Pure update path — no inserts — so we iter without
    // cloning and let counts saturate naturally (which they won't, in
    // practice).
    let burst_keys: Vec<u128> = fixture.hot_keys.iter().take(64).copied().collect();
    for &batch in &[1usize, 32, 128] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_function(BenchmarkId::new("traffic_burst", batch), |b| {
            let mut store = fixture.store.clone();
            let mut obs = ObservationBatch::with_capacity(batch);
            let mut sink = DeltaSink::with_capacity(batch);
            let mut rng = Rng::new(0xCAFE_F00D);
            b.iter(|| {
                obs.clear();
                sink.clear();
                for _ in 0..batch {
                    let key = burst_keys[rng.range(burst_keys.len() as u32) as usize];
                    obs.push(obs_row(
                        fixture.rule_fps[0],
                        KeyHash(key),
                        0,
                        LOCAL_NODE,
                        LOCAL_INCARNATION,
                        1,
                        0,
                    ));
                }
                store.ingest_local(black_box(&obs), black_box(&mut sink));
            });
        });
    }

    // ─── threshold_trigger_steady_state ────────────────────────────────
    // Models the runtime's additional per-request work (`handle_limit_request`
    // in `crates/gabion/src/gossip/runtime.rs`): one `rule_dictionary.find`
    // lookup, one saturating-add into a per-rule pending column, one
    // mul/div for ε, one compare. Target: < 1% regression vs `steady_state`
    // with `batch == 128`.
    for &batch in &[1usize, 32, 128] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_function(
            BenchmarkId::new("threshold_trigger_steady_state", batch),
            |b| {
                b.iter_batched_ref(
                    || {
                        (
                            fixture.store.clone(),
                            ObservationBatch::with_capacity(batch),
                            DeltaSink::with_capacity(batch),
                            Rng::new(0x7E57_7E57),
                            vec![0_u32; 64].into_boxed_slice(),
                        )
                    },
                    |(store, obs, sink, rng, pending)| {
                        obs.clear();
                        sink.clear();
                        for _ in 0..batch {
                            let pick = rng.range(100);
                            let key = if pick < 95 {
                                pareto_key(rng, &fixture.hot_keys, &fixture.warm_keys)
                            } else {
                                fresh_key(rng)
                            };
                            let rule_idx = pick_rule(rng) as usize;
                            let bucket = pick_bucket(rng);
                            let fp = fixture.rule_fps[rule_idx];
                            // Mirror the runtime's threshold-trigger
                            // arithmetic:
                            // lookup the rule slot, bump per-rule pending,
                            // compute ε, compare. We don't store the flag; the
                            // compare's side effects are observed via
                            // `black_box`.
                            if let Some(slot) = store.rule_dictionary().find(fp) {
                                let idx = slot as usize;
                                let p = pending[idx].saturating_add(1);
                                pending[idx] = p;
                                let epsilon = (1_000_000_u64 * 100) / (10_000 * 8);
                                black_box((p as u64) > epsilon);
                            }
                            obs.push(obs_row(
                                fp,
                                KeyHash(key),
                                bucket,
                                LOCAL_NODE,
                                LOCAL_INCARNATION,
                                1,
                                0,
                            ));
                        }
                        store.ingest_local(black_box(obs), black_box(sink));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    // ─── threshold_trigger_burst ───────────────────────────────────────
    // Burst of 50k hits to a previously-quiet single rule: measures
    // crossing-detection + repeated immediate-tick cost on the
    // adversarial path. ε saturates to 1, so every iteration past the
    // first crosses. We don't measure the runtime's tick dispatch here
    // — only the per-request lookup/add/compare — but the bench is
    // sized so a future regression in those primitives is visible.
    let burst_keys: Vec<u128> = fixture.hot_keys.iter().take(64).copied().collect();
    group.throughput(Throughput::Elements(128));
    group.bench_function("threshold_trigger_burst", |b| {
        let mut store = fixture.store.clone();
        let mut obs = ObservationBatch::with_capacity(128);
        let mut sink = DeltaSink::with_capacity(128);
        let mut rng = Rng::new(0xB0_B0_B0);
        let mut pending = vec![0_u32; 64].into_boxed_slice();
        b.iter(|| {
            obs.clear();
            sink.clear();
            for _ in 0..128 {
                let key = burst_keys[rng.range(burst_keys.len() as u32) as usize];
                let fp = fixture.rule_fps[0];
                if let Some(slot) = store.rule_dictionary().find(fp) {
                    let idx = slot as usize;
                    let p = pending[idx].saturating_add(1);
                    pending[idx] = p;
                    let epsilon = 1_u64; // saturated case
                    black_box((p as u64) > epsilon);
                }
                obs.push(obs_row(
                    fp,
                    KeyHash(key),
                    0,
                    LOCAL_NODE,
                    LOCAL_INCARNATION,
                    1,
                    0,
                ));
            }
            store.ingest_local(black_box(&obs), black_box(&mut sink));
        });
    });

    // ─── cold_start ─────────────────────────────────────────────────────
    // Empty store at process start: each ingest pushes 32 fresh keys per
    // realistic Pareto distribution. Pure insert path. Use the smaller
    // empty fixture so the freelist actually has room across many
    // iterations.
    let empty = {
        let (store, rule_fps, ..) = new_store();
        (store, rule_fps)
    };
    group.throughput(Throughput::Elements(32));
    group.bench_function("cold_start", |b| {
        b.iter_batched_ref(
            || {
                (
                    empty.0.clone(),
                    ObservationBatch::with_capacity(32),
                    DeltaSink::with_capacity(32),
                    Rng::new(0xC01D_57A7),
                )
            },
            |(store, obs, sink, rng)| {
                obs.clear();
                sink.clear();
                for _ in 0..32 {
                    let rule_idx = pick_rule(rng) as usize;
                    let bucket = pick_bucket(rng);
                    obs.push(obs_row(
                        empty.1[rule_idx],
                        KeyHash(fresh_key(rng)),
                        bucket,
                        LOCAL_NODE,
                        LOCAL_INCARNATION,
                        1,
                        0,
                    ));
                }
                store.ingest_local(black_box(obs), black_box(sink));
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// merge_remote — per-gossip-frame peer merge.
// ---------------------------------------------------------------------------

fn bench_merge_remote(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt/merge_remote");

    let fixture = build_fixture();

    // ─── digest_repair ──────────────────────────────────────────────────
    // What an incoming digest-repair frame looks like: 50% no-op (we are
    // ahead), 40% real update (peer is ahead), 10% insert (cell new to
    // us). Multi-peer, multi-rule.
    for &batch in &[64usize, 128] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_function(BenchmarkId::new("digest_repair", batch), |b| {
            b.iter_batched_ref(
                || {
                    (
                        fixture.store.clone(),
                        ObservationBatch::with_capacity(batch),
                        DeltaSink::with_capacity(batch),
                        Rng::new(0xBEEF_F00D),
                    )
                },
                |(store, obs, sink, rng)| {
                    obs.clear();
                    sink.clear();
                    for _ in 0..batch {
                        let pick = rng.range(100);
                        let (key, rule_idx, bucket, peer_idx, count) = if pick < 50 {
                            // No-op: peer behind us. Pick an existing cell
                            // and report a count less than ours (100).
                            let row = fixture.remote_rows
                                [rng.range(fixture.remote_rows.len() as u32) as usize];
                            (row.key, row.rule_idx, row.bucket, row.peer_idx, 1_u32)
                        } else if pick < 90 {
                            // Update: peer ahead of us.
                            let row = fixture.remote_rows
                                [rng.range(fixture.remote_rows.len() as u32) as usize];
                            (row.key, row.rule_idx, row.bucket, row.peer_idx, 10_000_u32)
                        } else {
                            // Insert: brand-new cell.
                            let peer = rng.range(fixture.peers.len() as u32);
                            let r = pick_rule(rng);
                            let b = pick_bucket(rng);
                            (fresh_key(rng), r, b, peer, 1_u32)
                        };
                        let (origin, inc) = fixture.peers[peer_idx as usize];
                        obs.push(obs_row(
                            fixture.rule_fps[rule_idx as usize],
                            KeyHash(key),
                            bucket,
                            origin,
                            inc,
                            count,
                            0,
                        ));
                    }
                    store.merge_remote(black_box(obs), black_box(sink));
                },
                BatchSize::SmallInput,
            );
        });
    }

    // ─── bulk_fan_in ────────────────────────────────────────────────────
    // Multiple peers gossiping overlapping cells (the same keys reported
    // by different peers, with varying counts). Stresses max-merge and the
    // sequence allocator. Pure update path — iter without cloning.
    for &batch in &[64usize, 128] {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_function(BenchmarkId::new("bulk_fan_in", batch), |b| {
            let mut store = fixture.store.clone();
            let mut obs = ObservationBatch::with_capacity(batch);
            let mut sink = DeltaSink::with_capacity(batch);
            let mut rng = Rng::new(0xFA10_1110);
            let mut counter: u32 = 200;
            b.iter(|| {
                obs.clear();
                sink.clear();
                for _ in 0..batch {
                    // Pick a remote row, then rotate the reporting peer
                    // so the same key shows up under different origins.
                    let row =
                        fixture.remote_rows[rng.range(fixture.remote_rows.len() as u32) as usize];
                    let peer_idx = rng.range(fixture.peers.len() as u32);
                    let (origin, inc) = fixture.peers[peer_idx as usize];
                    obs.push(obs_row(
                        fixture.rule_fps[row.rule_idx as usize],
                        KeyHash(row.key),
                        row.bucket,
                        origin,
                        inc,
                        counter,
                        0,
                    ));
                }
                counter = counter.wrapping_add(1).max(200);
                store.merge_remote(black_box(&obs), black_box(&mut sink));
            });
        });
    }

    // ─── all_noop ───────────────────────────────────────────────────────
    // Pure fast-path: every row is below stored count ⇒ next == previous.
    // No state change ⇒ iter without cloning.
    group.throughput(Throughput::Elements(128));
    group.bench_function("all_noop", |b| {
        let mut store = fixture.store.clone();
        let mut obs = ObservationBatch::with_capacity(128);
        let mut sink = DeltaSink::with_capacity(128);
        let mut rng = Rng::new(0x4E0E_4E0E);
        b.iter(|| {
            obs.clear();
            sink.clear();
            for _ in 0..128 {
                let row = fixture.remote_rows[rng.range(fixture.remote_rows.len() as u32) as usize];
                let (origin, inc) = fixture.peers[row.peer_idx as usize];
                obs.push(obs_row(
                    fixture.rule_fps[row.rule_idx as usize],
                    KeyHash(row.key),
                    row.bucket,
                    origin,
                    inc,
                    1, // ≪ stored 100 ⇒ no-op
                    0,
                ));
            }
            store.merge_remote(black_box(&obs), black_box(&mut sink));
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// find — read-only lookup (the rate-limit aggregator's per-request query).
// ---------------------------------------------------------------------------

fn bench_find(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt/find");
    group.throughput(Throughput::Elements(1));

    let fixture = build_fixture();

    // Materialise the keys we know live in the store (local seed).
    let present_keys: Vec<u128> = {
        let mut v = fixture.hot_keys.clone();
        v.extend(fixture.warm_keys.iter().take(WARM_KEYS / 2).copied());
        v
    };

    // ─── realistic_95_hit ───────────────────────────────────────────────
    // 95% of queries hit an existing cell, 5% miss. This is what an
    // aggregator's per-request `find` actually does.
    group.bench_function("realistic_95_hit", |b| {
        let mut rng = Rng::new(0x9595_9595);
        b.iter(|| {
            let pick = rng.range(100);
            let (key, rule_idx, bucket) = if pick < 95 {
                let key = present_keys[rng.range(present_keys.len() as u32) as usize];
                let rule_idx = pick_rule(&mut rng) as usize;
                let bucket = pick_bucket(&mut rng);
                (key, rule_idx, bucket)
            } else {
                (fresh_key(&mut rng), 0, 0)
            };
            let rule_slot = fixture.rule_slots[rule_idx];
            let h = fixture.store.find(black_box(CompactCellKey {
                rule: rule_slot,
                key_hash: KeyHash(key),
                bucket,
                origin: fixture.local_node_slot,
                incarnation: LOCAL_INCARNATION,
            }));
            black_box(h);
        });
    });

    // ─── miss ───────────────────────────────────────────────────────────
    // Pure miss path — Robin Hood early-exit baseline.
    group.bench_function("miss", |b| {
        let rule_slot = fixture.rule_slots[0];
        let mut rng = Rng::new(0xDEAD_BEEF);
        b.iter(|| {
            let h = fixture.store.find(black_box(CompactCellKey {
                rule: rule_slot,
                key_hash: KeyHash(fresh_key(&mut rng)),
                bucket: 0,
                origin: fixture.local_node_slot,
                incarnation: LOCAL_INCARNATION,
            }));
            black_box(h);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// admission_headers — overhead of populating `X-RateLimit-*` on the allow
// path. We do *not* keep a separate "cheap allow" code path; this bench is
// the audit trail confirming the header math is genuinely free enough not
// to need one.
// ---------------------------------------------------------------------------

fn bench_admission_headers(c: &mut Criterion) {
    use std::fmt::Write;
    let mut group = c.benchmark_group("crdt/admission_headers");
    group.throughput(Throughput::Elements(1));

    let fixture = build_fixture();
    let present_keys: Vec<u128> = {
        let mut v = fixture.hot_keys.clone();
        v.extend(fixture.warm_keys.iter().take(WARM_KEYS / 2).copied());
        v
    };

    // Pick a representative bucket_millis matching the production
    // 60s / 15s shape the rest of this file's fixture uses.
    const BUCKET_MILLIS: u64 = 15_000;
    const RULE_LIMIT: u64 = 1_000;

    // ─── baseline (A) ───────────────────────────────────────────────────
    // Today's allow path — just the `find` lookup. Used as the reference
    // the headers variant is compared against.
    group.bench_function("baseline", |b| {
        let mut rng = Rng::new(0x9595_9595);
        let mut now: u64 = 0;
        b.iter(|| {
            let pick = rng.range(100);
            let (key, rule_idx, bucket) = if pick < 95 {
                let key = present_keys[rng.range(present_keys.len() as u32) as usize];
                let rule_idx = pick_rule(&mut rng) as usize;
                let bucket = pick_bucket(&mut rng);
                (key, rule_idx, bucket)
            } else {
                (fresh_key(&mut rng), 0, 0)
            };
            let rule_slot = fixture.rule_slots[rule_idx];
            let h = fixture.store.find(black_box(CompactCellKey {
                rule: rule_slot,
                key_hash: KeyHash(key),
                bucket,
                origin: fixture.local_node_slot,
                incarnation: LOCAL_INCARNATION,
            }));
            black_box(h);
            now = now.wrapping_add(1);
        });
    });

    // ─── with_headers (B) ───────────────────────────────────────────────
    // Same allow path plus the arithmetic that `AdmissionHeaders` emits on
    // a 200 response: bucket-boundary modulo + saturating remaining +
    // `reset_unix_seconds` + three u64 format passes into a stack buffer.
    group.bench_function("with_headers", |b| {
        let mut rng = Rng::new(0x9595_9595);
        let mut now: u64 = 0;
        let mut scratch = [0_u8; 96];
        b.iter(|| {
            let pick = rng.range(100);
            let (key, rule_idx, bucket) = if pick < 95 {
                let key = present_keys[rng.range(present_keys.len() as u32) as usize];
                let rule_idx = pick_rule(&mut rng) as usize;
                let bucket = pick_bucket(&mut rng);
                (key, rule_idx, bucket)
            } else {
                (fresh_key(&mut rng), 0, 0)
            };
            let rule_slot = fixture.rule_slots[rule_idx];
            let handle = fixture.store.find(black_box(CompactCellKey {
                rule: rule_slot,
                key_hash: KeyHash(key),
                bucket,
                origin: fixture.local_node_slot,
                incarnation: LOCAL_INCARNATION,
            }));
            // Treat the existing count as the window total. Realistic
            // enough for the cost we want to measure — the bench is
            // about the header math, not about an exact admission walk.
            let total: u64 = handle
                .and_then(|h| fixture.store.get(h))
                .map(|row| u64::from(row.count))
                .unwrap_or(0);
            let hits: u64 = 1;
            let remaining = RULE_LIMIT.saturating_sub(total).saturating_sub(hits);
            let delta = gabion::window::time_until_next_bucket_boundary_millis(
                now,
                BUCKET_MILLIS.try_into().unwrap(),
            );
            let reset = gabion::window::reset_unix_seconds(now, delta);
            // Format the three header values into a scratch buffer so the
            // bench measures the same Display passes the production
            // builder runs.
            let mut writer = SliceWriter::new(&mut scratch);
            let _ = write!(&mut writer, "{}", RULE_LIMIT);
            let _ = write!(&mut writer, "{}", remaining);
            let _ = write!(&mut writer, "{}", reset);
            black_box(writer.len);
            now = now.wrapping_add(BUCKET_MILLIS / 4);
        });
    });

    group.finish();
}

/// Allocation-free `core::fmt::Write` backed by a stack slice. Truncates
/// silently on overflow; the bench buffer is sized to hold three `u64`
/// decimal strings.
struct SliceWriter<'a> {
    buf: &'a mut [u8],
    len: usize,
}

impl<'a> SliceWriter<'a> {
    fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, len: 0 }
    }
}

impl std::fmt::Write for SliceWriter<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let room = self.buf.len().saturating_sub(self.len);
        let to_copy = s.len().min(room);
        self.buf[self.len..self.len + to_copy].copy_from_slice(&s.as_bytes()[..to_copy]);
        self.len += to_copy;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// fill_gossip_frame — outbound gossip composition.
// ---------------------------------------------------------------------------

fn bench_fill_gossip_frame(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt/fill_gossip_frame");
    group.throughput(Throughput::Elements(128));

    let fixture = build_fixture();

    // ─── steady_state ───────────────────────────────────────────────────
    // Moderately loaded node: dirty rings contain ~64 recent updates;
    // repair lane fills the remaining 64 slots of a 128-handle frame.
    let mut steady = fixture.store.clone();
    {
        // Generate ~64 recent local updates so local_dirty has entries.
        let mut rng = Rng::new(0xD1_D1_D1);
        let mut obs = ObservationBatch::with_capacity(64);
        let mut sink = DeltaSink::with_capacity(64);
        for _ in 0..64 {
            let key = pareto_key(&mut rng, &fixture.hot_keys, &fixture.warm_keys);
            let rule_idx = pick_rule(&mut rng) as usize;
            obs.push(obs_row(
                fixture.rule_fps[rule_idx],
                KeyHash(key),
                0,
                LOCAL_NODE,
                LOCAL_INCARNATION,
                1,
                0,
            ));
        }
        steady.ingest_local(&obs, &mut sink);
    }
    group.bench_function("steady_state", |b| {
        // fill_gossip_frame only mutates selection_marks (per-frame
        // ephemeral) and repair_cursor (advances round-robin); over many
        // iterations the repair cursor wraps but no identity changes ⇒
        // iter without cloning.
        let mut out = Vec::<CellHandle>::with_capacity(128);
        b.iter(|| {
            steady.fill_gossip_frame(black_box(128), black_box(&mut out));
        });
    });

    // ─── repair_only ────────────────────────────────────────────────────
    // Dirty rings drained; frame composition is entirely the repair scan.
    let mut repair = fixture.store.clone();
    repair.clear_dirty();
    group.bench_function("repair_only", |b| {
        let mut out = Vec::<CellHandle>::with_capacity(128);
        b.iter(|| {
            repair.fill_gossip_frame(black_box(128), black_box(&mut out));
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// expire — periodic sweep.
// ---------------------------------------------------------------------------

fn bench_expire(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt/expire");

    let fixture = build_fixture();

    // ─── window_turnover ────────────────────────────────────────────────
    // The steady-state expiry pattern: every `bucket_millis` interval,
    // one of `NUM_BUCKETS` buckets ages out. About 25% of the populated
    // cells get freed in a single call.
    group.bench_function("window_turnover", |b| {
        // Build current_epoch / live_buckets such that bucket 3 is just
        // past the live window (it ages out), buckets 0..3 remain.
        let mut current_epoch = vec![0_u32; RULE_CAPACITY as usize];
        let mut live_buckets = vec![0_u32; RULE_CAPACITY as usize];
        for i in 0..NUM_RULES {
            current_epoch[i] = NUM_BUCKETS as u32; // 4
            live_buckets[i] = (NUM_BUCKETS as u32) - 1; // 3
        }
        let mut sink = ExpirationSink::with_capacity(fixture.store.capacity() as usize);
        b.iter_batched_ref(
            || fixture.store.clone(),
            |store| {
                sink.clear();
                store.expire(
                    black_box(&current_epoch),
                    black_box(&live_buckets),
                    black_box(&mut sink),
                )
            },
            BatchSize::SmallInput,
        );
    });

    // ─── no_expirations ─────────────────────────────────────────────────
    // Live window covers every bucket. Pure scan cost.
    group.bench_function("no_expirations", |b| {
        let mut current_epoch = vec![0_u32; RULE_CAPACITY as usize];
        let mut live_buckets = vec![0_u32; RULE_CAPACITY as usize];
        for i in 0..NUM_RULES {
            current_epoch[i] = 0;
            live_buckets[i] = 1000;
        }
        let mut sink = ExpirationSink::with_capacity(fixture.store.capacity() as usize);
        b.iter_batched_ref(
            || fixture.store.clone(),
            |store| {
                sink.clear();
                store.expire(
                    black_box(&current_epoch),
                    black_box(&live_buckets),
                    black_box(&mut sink),
                )
            },
            BatchSize::SmallInput,
        );
    });

    // ─── bucket_boundary ────────────────────────────────────────────────
    // What the runtime does when a bucket ages out under load: a
    // production-sized store holding 11 live one-second buckets of one rule,
    // the oldest of which expires, freed in batches of the runtime's sink
    // until `Done`. `prod_peak` frees 1,825 cells (one batch), `prod_4x`
    // 7,222 (two).
    for (name, cells) in [("prod_peak", 20_075_usize), ("prod_4x", 79_442)] {
        let (_, storage) = Settings::default().validate().expect("defaults validate");
        let table = rules(1, "1000000r/10s", Duration::from_secs(1), &storage);
        let mut store = CellStore::new(storage, local_identity(), &table);
        let rule = &table.rules()[0];
        let mut obs = ObservationBatch::with_capacity(cells);
        for i in 0..cells {
            let bucket = (i % 11) as BucketEpoch;
            obs.push(obs_row(
                rule.fingerprint(),
                KeyHash(i as u128),
                bucket,
                LOCAL_NODE,
                LOCAL_INCARNATION,
                1,
                0,
            ));
        }
        store.ingest_local(&obs, &mut DeltaSink::with_capacity(cells));
        let mut sink = ExpirationSink::with_capacity(GOSSIP_EXPIRATION_BATCH);
        group.throughput(Throughput::Elements(cells.div_ceil(11) as u64));
        group.bench_function(BenchmarkId::new("bucket_boundary", name), |b| {
            b.iter_batched_ref(
                || store.clone(),
                |store| {
                    while store.expire_at(black_box(11_000), &mut sink) == Expiry::More {
                        sink.clear();
                    }
                    sink.clear();
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// wire — gossip frame encode / decode.
// ---------------------------------------------------------------------------

fn wire_header() -> Header<CellPayload> {
    Header {
        cluster_id_hash: 0xC0FE_FACE_DEAD_BEEF_1234_5678_9ABC_DEF0_u128
            .try_into()
            .expect("non-zero"),
        sender: local_identity(),
        sender_zone: 0,
        acks: AckInfo::default(),
        payload: CellPayload::Sweep,
    }
}

fn bench_wire(c: &mut Criterion) {
    let mut group = c.benchmark_group("crdt/wire");
    group.throughput(Throughput::Elements(128));

    let fixture = build_fixture();
    let limits = FrameLimits::default();

    // ─── encode_128 ────────────────────────────────────────────────────
    // Compose a 128-cell frame from the fixture, then encode into a
    // pre-allocated buffer. Resembles the runtime's per-tick send path.
    let mut encode_store = fixture.store.clone();
    let mut handles = Vec::<CellHandle>::with_capacity(128);
    encode_store.fill_gossip_frame(128, &mut handles);
    let handles = handles; // freeze for the closure
    group.bench_function("encode_128", |b| {
        let mut scratch = WireScratch::for_store(&encode_store);
        let mut buf = PacketBuf::for_limits(limits);
        b.iter(|| {
            let mut packets = Packets::new(
                wire_header(),
                black_box(&encode_store),
                black_box(&handles),
                &mut scratch,
                limits,
            );
            while let Some(pkt) = packets.next_into(&mut buf) {
                black_box(pkt);
                black_box(buf.as_bytes());
            }
        });
    });

    // ─── encode_multi_packet_4096 ─────────────────────────────────────
    // 4096-cell fixture under the production 1,400-byte budget, at most 33
    // cells a packet. Measures the steady-state multi-packet emission rate —
    // drains the full handle list across however many packets it takes.
    let mut multi_store = fixture.store.clone();
    let mut multi_handles = Vec::<CellHandle>::with_capacity(4096);
    multi_store.fill_gossip_frame(4096, &mut multi_handles);
    let multi_handles = multi_handles;
    let multi_limits = FrameLimits::new(GOSSIP_MAX_PAYLOAD_BYTES).unwrap();
    // Sanity-check the bench actually exercises the multi-packet path.
    {
        let mut scratch = WireScratch::for_store(&multi_store);
        let mut buf = PacketBuf::for_limits(multi_limits);
        let mut packets = Packets::new(
            wire_header(),
            &multi_store,
            &multi_handles,
            &mut scratch,
            multi_limits,
        );
        let mut n_packets = 0;
        while packets.next_into(&mut buf).is_some() {
            n_packets += 1;
        }
        assert!(
            n_packets > 1,
            "encode_multi_packet_4096 emitted only {n_packets} packet(s); raise the cell count or \
             lower max_payload_bytes",
        );
    }
    group.throughput(Throughput::Elements(multi_handles.len() as u64));
    group.bench_function("encode_multi_packet_4096", |b| {
        let mut scratch = WireScratch::for_store(&multi_store);
        let mut buf = PacketBuf::for_limits(multi_limits);
        b.iter(|| {
            let mut packets = Packets::new(
                wire_header(),
                black_box(&multi_store),
                black_box(&multi_handles),
                &mut scratch,
                multi_limits,
            );
            while let Some(pkt) = packets.next_into(&mut buf) {
                black_box(pkt);
                black_box(buf.as_bytes());
            }
        });
    });

    // Restore throughput to 128 for the decode bench below.
    group.throughput(Throughput::Elements(128));

    // ─── decode_128 ────────────────────────────────────────────────────
    // Pre-encode one 128-cell packet, then decode it into a recycled
    // ObservationBatch on every iteration, as the runtime's inbound arm does.
    let mut preflight_scratch = WireScratch::for_store(&encode_store);
    let mut preflight_buf = PacketBuf::for_limits(limits);
    {
        let mut packets = Packets::new(
            wire_header(),
            &encode_store,
            &handles,
            &mut preflight_scratch,
            limits,
        );
        let cells = packets
            .next_into(&mut preflight_buf)
            .expect("at least one packet");
        // 128 cells fits one packet at the default limits.
        assert_eq!(cells, 128);
    }
    let frame_buf: Vec<u8> = preflight_buf.as_bytes().to_vec();
    let cluster = wire_header().cluster_id_hash;
    group.bench_function("decode_128", |b| {
        let mut obs = ObservationBatch::with_capacity(128);
        b.iter(|| {
            obs.clear();
            let header = decode(black_box(&frame_buf), limits, cluster, |cell| {
                obs.push(cell)
            })
            .expect("decode");
            black_box(header);
        });
    });

    group.finish();
}

// ─── aggregate ──────────────────────────────────────────────────────────
// The table in production steady state, modelled on a measured fleet: a
// `rate=20r/s window=10s bucket=1s` rule, about 650 distinct keys a second
// drawn from 2,200 active over ten seconds, and the CRDT keeping 11 buckets
// live per key, so about 7,000 live cells in a 16,384-slot table. Each
// second the writer inserts the new bucket's cells and expires the bucket
// that aged out. Warm-up runs two minutes of that churn first, so the table
// holds the residue of ~80,000 cells.

const AGGREGATE_SLOTS: u32 = 16_384;
const ACTIVE_KEYS: u64 = 2_200;
const KEYS_PER_SECOND: usize = 650;
const AGGREGATE_LIVE_BUCKETS: u32 = 11;

struct Traffic {
    rng: u64,
    aggregate: Aggregate,
    rule: RuleSpec,
    second: u32,
    deltas: DeltaSink,
    expirations: ExpirationSink,
    /// The keys written in each live bucket, by `bucket % LIVE_BUCKETS`.
    history: Vec<Vec<KeyHash>>,
}

impl Traffic {
    fn new() -> Self {
        let settings = Settings {
            max_cells: AGGREGATE_SLOTS.try_into().expect("non-zero"),
            ..Settings::default()
        };
        let (_, storage) = settings.validate().expect("bench settings validate");
        let mut traffic = Self {
            rng: 0x9E37_79B9_7F4A_7C15,
            aggregate: Aggregate::new(&storage),
            rule: rules(1, "200r/10s", Duration::from_secs(1), &storage).rules()[0].spec(),
            second: AGGREGATE_LIVE_BUCKETS,
            deltas: DeltaSink::with_capacity(KEYS_PER_SECOND),
            expirations: ExpirationSink::with_capacity(KEYS_PER_SECOND),
            history: vec![Vec::with_capacity(KEYS_PER_SECOND); AGGREGATE_LIVE_BUCKETS as usize],
        };
        for _ in 0..120 {
            traffic.advance();
        }
        traffic
    }

    fn next(&mut self) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng
    }

    fn active_key(&mut self) -> KeyHash {
        KeyHash(u128::from(self.next() % ACTIVE_KEYS) * 0x9E37_79B9_7F4A_7C15)
    }

    fn cell(&self, key_hash: KeyHash, bucket: BucketEpoch) -> CellIdentity {
        CellIdentity {
            rule_fingerprint: self.rule.fingerprint,
            key_hash,
            bucket,
        }
    }

    /// One second: expire the bucket that aged out, record the new one.
    fn advance(&mut self) {
        self.second += 1;
        let slot = (self.second % AGGREGATE_LIVE_BUCKETS) as usize;
        let expired = self.second - AGGREGATE_LIVE_BUCKETS;
        let mut keys = std::mem::take(&mut self.history[slot]);
        self.deltas.clear();
        self.expirations.clear();
        for &key_hash in &keys {
            let key = self.cell(key_hash, expired);
            self.expirations.handles.push(CellHandle::default());
            self.expirations.keys.push(key);
            self.expirations.last_counts.push(1);
            self.expirations.last_update_millis.push(0);
            self.expirations.applies_locally.push(1);
        }
        keys.clear();
        for _ in 0..KEYS_PER_SECOND {
            let key_hash = self.active_key();
            keys.push(key_hash);
            let key = self.cell(key_hash, self.second);
            self.deltas.handles.push(CellHandle::default());
            self.deltas.keys.push(key);
            self.deltas.previous.push(0);
            self.deltas.current.push(1);
            self.deltas.deltas.push(1);
            self.deltas.applies_locally.push(1);
        }
        self.history[slot] = keys;
        self.aggregate.apply(&self.deltas, &self.expirations);
    }
}

fn bench_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate");
    let mut traffic = Traffic::new();

    group.throughput(Throughput::Elements(1));
    group.bench_function("window_total_95_5", |b| {
        b.iter(|| {
            let key = if traffic.next().is_multiple_of(20) {
                KeyHash(u128::from(traffic.next()) << 64)
            } else {
                traffic.active_key()
            };
            let now = u64::from(traffic.second) * 1_000;
            black_box(
                traffic
                    .aggregate
                    .table()
                    .window_total(traffic.rule, key, now),
            )
        });
    });

    group.throughput(Throughput::Elements(2 * KEYS_PER_SECOND as u64));
    group.bench_function("apply_one_second", |b| b.iter(|| traffic.advance()));
    group.finish();
}

// ---------------------------------------------------------------------------
// inbound — the runtime's per-packet path with a production-sized peer table.
// ---------------------------------------------------------------------------

const PEERS: u16 = 300;
const INBOUND_PACKETS: usize = 3_000;

/// Hands the runtime one copy of `packet` per call, each from the next peer,
/// and fails after `left` calls so `run` returns.
struct Replay {
    packet: Box<[u8]>,
    left: Cell<usize>,
}

impl GossipTransport for Replay {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(peer_addr(PEERS))
    }

    fn try_send_to(&self, buf: &[u8], _dst: SocketAddr) -> io::Result<usize> {
        Ok(buf.len())
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let left = self
            .left
            .get()
            .checked_sub(1)
            .ok_or(io::ErrorKind::UnexpectedEof)?;
        self.left.set(left);
        buf[..self.packet.len()].copy_from_slice(&self.packet);
        Ok((self.packet.len(), peer_addr(left as u16 % PEERS)))
    }
}

fn peer_addr(i: u16) -> SocketAddr {
    SocketAddr::from(([10, 0, (i >> 8) as u8, i as u8], 9000))
}

fn bench_inbound(c: &mut Criterion) {
    let nz = "bench capacities are non-zero";
    let settings = Settings {
        max_cells: CELL_CAPACITY.try_into().expect(nz),
        local_dirty_capacity: DIRTY_CAPACITY.try_into().expect(nz),
        forwarded_dirty_capacity: DIRTY_CAPACITY.try_into().expect(nz),
        cluster_id_hash: wire_header().cluster_id_hash,
        ..Settings::default()
    };
    let (mut gossip, storage) = settings.validate().expect("bench settings validate");
    gossip.bootstrap_peers = (0..PEERS).map(peer_addr).collect();
    let rules = rules(1, "100r/s", Duration::from_secs(1), &storage);

    // The packet: two cells of the bench rule, sent by `local_identity()`.
    let mut sender = CellStore::new(storage, local_identity(), &rules);
    let fingerprint = rules.rules()[0].fingerprint();
    let mut obs = ObservationBatch::with_capacity(2);
    for key in [1, 2] {
        obs.push(obs_row(
            fingerprint,
            KeyHash(key),
            0,
            LOCAL_NODE,
            LOCAL_INCARNATION,
            1,
            1,
        ));
    }
    sender.ingest_local(&obs, &mut DeltaSink::with_capacity(2));
    let mut handles = Vec::with_capacity(2);
    sender.fill_gossip_frame(2, &mut handles);
    let limits = FrameLimits::new(GOSSIP_MAX_PAYLOAD_BYTES).expect("default payload");
    let mut buf = PacketBuf::for_limits(limits);
    let mut scratch = WireScratch::for_store(&sender);
    let mut packets = Packets::new(wire_header(), &sender, &handles, &mut scratch, limits);
    assert_eq!(packets.next_into(&mut buf), Some(2));
    let packet: Box<[u8]> = buf.as_bytes().into();

    let tokio = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("inbound");
    group.throughput(Throughput::Elements(INBOUND_PACKETS as u64));
    group.bench_function(BenchmarkId::new("peer_lookup", PEERS), |b| {
        b.iter_batched(
            || {
                GossipRuntime::from_parts(
                    Replay {
                        packet: packet.clone(),
                        left: Cell::new(INBOUND_PACKETS),
                    },
                    FixedClock::new(1),
                    gossip.clone(),
                    CellStore::new(storage, NodeIdentity::new(peer_node(0), 1), &rules),
                    Aggregate::new(&storage),
                )
            },
            // Holding the client keeps the request arm pending, so `run`
            // serves every packet instead of shutting down.
            |(runtime, _client)| tokio.block_on(runtime.run(futures::stream::empty())),
            BatchSize::LargeInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_ingest_local,
    bench_merge_remote,
    bench_find,
    bench_admission_headers,
    bench_fill_gossip_frame,
    bench_expire,
    bench_wire,
    bench_aggregate,
    bench_inbound,
);
criterion_main!(benches);
