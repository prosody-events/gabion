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

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use gabion::crdt::{
    BucketEpoch, CellHandle, CellStore, CellStoreConfig, CompactCellKey, DeltaSink, ExpirationSink,
    Incarnation, KeyHash, NodeId, NodeIdentity, NodeSlot, Observation, ObservationBatch,
    RuleDescriptor,
};

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
) -> Observation<u32> {
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
use gabion::wire::{FrameLimits, Header, PacketBuf, Packets, WireScratch, decode_unauth};

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
const DIRTY_CAPACITY: usize = 256;
const PEER_CAPACITY: u16 = 32;

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

fn config() -> CellStoreConfig {
    CellStoreConfig {
        cell_capacity: CELL_CAPACITY,
        rule_dictionary_capacity: RULE_CAPACITY,
        node_dictionary_capacity: NODE_CAPACITY,
        local_dirty_capacity: DIRTY_CAPACITY,
        forwarded_dirty_capacity: DIRTY_CAPACITY,
        peer_capacity: PEER_CAPACITY,
    }
}

fn rule_fingerprint(i: usize) -> u128 {
    // Distinct, deterministic per-rule fingerprints.
    0xC0FE_BEEF_DEAD_F00D_0000_0000_0000_0000_u128 ^ ((i as u128) << 64) ^ (i as u128)
}

fn rule_descriptor(fp: u128, local_rule_id: u32) -> RuleDescriptor {
    RuleDescriptor {
        fingerprint: fp,
        window_millis: 60_000,
        bucket_millis: 15_000,
        limit: 1_000_000,
        flags: 0,
        local_rule_id,
    }
}

fn peer_node(i: usize) -> NodeId {
    NodeId(0xAAAA_0000_0000_0000_0000_0000_0000_0000 ^ (i as u128))
}

/// A pre-built, populated store the bench can clone per-iteration.
struct Fixture {
    store: CellStore<u32>,
    rule_fps: Vec<u128>,
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

/// Build a fresh store, intern N rules, and return the seed material the
/// benches share.
fn new_store() -> (CellStore<u32>, Vec<u128>, NodeSlot) {
    let mut store = CellStore::<u32>::new(config(), local_identity());
    let mut rule_fps = Vec::with_capacity(NUM_RULES);
    for i in 0..NUM_RULES {
        let fp = rule_fingerprint(i);
        store
            .intern_rule(rule_descriptor(fp, i as u32))
            .expect("rule interned");
        rule_fps.push(fp);
    }
    let local_node_slot = store.local_node_slot();
    (store, rule_fps, local_node_slot)
}

/// Seed `count` cells under the local origin, sampling rule and bucket
/// from the realistic Pareto-shaped distributions.
fn seed_local(
    store: &mut CellStore<u32>,
    rule_fps: &[u128],
    keys: &[u128],
    count: usize,
    seed: u64,
) {
    let mut rng = Rng::new(seed);
    let mut obs = ObservationBatch::<u32>::with_capacity(count);
    let mut sink = DeltaSink::<u32>::with_capacity(count);
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
    store: &mut CellStore<u32>,
    rule_fps: &[u128],
    keys: &[u128],
    peers: &[(NodeId, Incarnation)],
    count: usize,
    initial_count: u32,
    seed: u64,
) -> Vec<RemoteRow> {
    let mut rng = Rng::new(seed);
    let mut obs = ObservationBatch::<u32>::with_capacity(count);
    let mut sink = DeltaSink::<u32>::with_capacity(count);
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
    let (mut store, rule_fps, local_node_slot) = new_store();

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
                        ObservationBatch::<u32>::with_capacity(batch),
                        DeltaSink::<u32>::with_capacity(batch),
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
            let mut obs = ObservationBatch::<u32>::with_capacity(batch);
            let mut sink = DeltaSink::<u32>::with_capacity(batch);
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
                            ObservationBatch::<u32>::with_capacity(batch),
                            DeltaSink::<u32>::with_capacity(batch),
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
                            // Mirror the runtime's threshold-trigger arithmetic:
                            // lookup the rule slot, bump per-rule pending,
                            // compute ε, compare. We don't store the flag; the
                            // compare's side effects are observed via `black_box`.
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
        let mut obs = ObservationBatch::<u32>::with_capacity(128);
        let mut sink = DeltaSink::<u32>::with_capacity(128);
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
        let (store, rule_fps, _) = new_store();
        (store, rule_fps)
    };
    group.throughput(Throughput::Elements(32));
    group.bench_function("cold_start", |b| {
        b.iter_batched_ref(
            || {
                (
                    empty.0.clone(),
                    ObservationBatch::<u32>::with_capacity(32),
                    DeltaSink::<u32>::with_capacity(32),
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
                        ObservationBatch::<u32>::with_capacity(batch),
                        DeltaSink::<u32>::with_capacity(batch),
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
            let mut obs = ObservationBatch::<u32>::with_capacity(batch);
            let mut sink = DeltaSink::<u32>::with_capacity(batch);
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
        let mut obs = ObservationBatch::<u32>::with_capacity(128);
        let mut sink = DeltaSink::<u32>::with_capacity(128);
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
            let rule_slot = fixture
                .store
                .find_rule(fixture.rule_fps[rule_idx])
                .expect("rule");
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
        let rule_slot = fixture.store.find_rule(fixture.rule_fps[0]).expect("rule");
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
        let mut obs = ObservationBatch::<u32>::with_capacity(64);
        let mut sink = DeltaSink::<u32>::with_capacity(64);
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
        let mut sink = ExpirationSink::<u32>::with_capacity(fixture.store.capacity() as usize);
        b.iter_batched_ref(
            || fixture.store.clone(),
            |store| {
                sink.clear();
                store.expire(
                    black_box(&current_epoch),
                    black_box(&live_buckets),
                    black_box(&mut sink),
                );
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
        let mut sink = ExpirationSink::<u32>::with_capacity(fixture.store.capacity() as usize);
        b.iter_batched_ref(
            || fixture.store.clone(),
            |store| {
                sink.clear();
                store.expire(
                    black_box(&current_epoch),
                    black_box(&live_buckets),
                    black_box(&mut sink),
                );
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// wire — gossip frame encode / decode.
// ---------------------------------------------------------------------------

fn wire_header() -> Header {
    Header {
        cluster_id_hash: 0xC0FE_FACE_DEAD_BEEF_1234_5678_9ABC_DEF0,
        sender_node_id: LOCAL_NODE.0,
        sender_incarnation: LOCAL_INCARNATION,
        count_width: 0,
        cell_count: 0,
        body_len: 0,
        min_origin_sequence: 0,
        max_origin_sequence: 0,
        flags: 0,
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
            let mut packets = Packets::<u32>::unauth(
                wire_header(),
                black_box(&encode_store),
                black_box(&handles),
                &mut scratch,
                limits,
            )
            .expect("ctor");
            while let Some(pkt) = packets.next_into(&mut buf).expect("encode") {
                black_box(pkt);
                black_box(buf.as_bytes());
            }
        });
    });

    // ─── encode_multi_packet_4096 ─────────────────────────────────────
    // 4096-cell fixture under a 64 KiB UDP-style budget. Measures the
    // steady-state multi-packet emission rate — drains the full handle
    // list across however many packets it takes (target ~9–10).
    let mut multi_store = fixture.store.clone();
    let mut multi_handles = Vec::<CellHandle>::with_capacity(4096);
    multi_store.fill_gossip_frame(4096, &mut multi_handles);
    let multi_handles = multi_handles;
    let multi_limits = FrameLimits {
        max_payload_bytes: 64 * 1024,
        max_cells: 4096,
    };
    // Sanity-check the bench actually exercises the multi-packet path.
    {
        let mut scratch = WireScratch::for_store(&multi_store);
        let mut buf = PacketBuf::for_limits(multi_limits);
        let mut packets = Packets::<u32>::unauth(
            wire_header(),
            &multi_store,
            &multi_handles,
            &mut scratch,
            multi_limits,
        )
        .expect("ctor");
        let mut n_packets = 0;
        while packets.next_into(&mut buf).expect("encode").is_some() {
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
            let mut packets = Packets::<u32>::unauth(
                wire_header(),
                black_box(&multi_store),
                black_box(&multi_handles),
                &mut scratch,
                multi_limits,
            )
            .expect("ctor");
            while let Some(pkt) = packets.next_into(&mut buf).expect("encode") {
                black_box(pkt);
                black_box(buf.as_bytes());
            }
        });
    });

    // Restore throughput to 128 for the decode bench below.
    group.throughput(Throughput::Elements(128));

    // ─── decode_128 ────────────────────────────────────────────────────
    // Pre-encode one 128-cell packet, then decode it into a recycled
    // ObservationBatch on every iteration.
    let mut preflight_scratch = WireScratch::for_store(&encode_store);
    let mut preflight_buf = PacketBuf::for_limits(limits);
    {
        let mut packets = Packets::<u32>::unauth(
            wire_header(),
            &encode_store,
            &handles,
            &mut preflight_scratch,
            limits,
        )
        .expect("preflight ctor");
        packets
            .next_into(&mut preflight_buf)
            .expect("preflight encode")
            .expect("at least one packet");
        // 128 cells fits in the default 256 KiB budget, so it's one packet.
        assert_eq!(packets.remaining(), 0);
    }
    let frame_buf: Vec<u8> = preflight_buf.as_bytes().to_vec();
    group.bench_function("decode_128", |b| {
        let mut obs = ObservationBatch::<u32>::with_capacity(128);
        b.iter(|| {
            obs.clear();
            let summary =
                decode_unauth::<u32>(black_box(&frame_buf), limits, &mut obs).expect("decode");
            black_box(summary);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_ingest_local,
    bench_merge_remote,
    bench_find,
    bench_fill_gossip_frame,
    bench_expire,
    bench_wire,
);
criterion_main!(benches);
