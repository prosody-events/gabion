//! The SHM aggregate table in production steady state, modelled on a
//! measured fleet: a `rate=20r/s window=10s bucket=1s` rule, about 650
//! distinct keys a second drawn from 2,200 active over ten seconds, and the
//! CRDT keeping 11 buckets live per key, so about 7,000 live cells in a
//! 16,384-slot table. Each second the leader inserts the new bucket's cells
//! and expires the bucket that aged out. Warm-up runs two minutes of that
//! churn first, so the table holds the residue of ~80,000 cells.
//!
//! * `aggregate/window_total` — one request's read: 10 bucket lookups for a
//!   key, 95% of them a key active in the window (present in some buckets,
//!   absent from the rest), 5% a key never seen.
//! * `aggregate/apply_one_second` — the leader's write side for one second of
//!   traffic: 650 inserts plus 650 expiries.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use gabion::crdt::{CellHandle, CellIdentity, DeltaSink, ExpirationSink, KeyHash};
use gabion::gossip::AggregateStore;
use gabion_nginx::shm::{Layout, ShmAggregateStore, ShmRegion};

const SLOTS: usize = 16_384;
const ACTIVE_KEYS: u64 = 2_200;
const KEYS_PER_SECOND: usize = 650;
const LIVE_BUCKETS: u32 = 11;
const RULE: u128 = 0xC0FE_BEEF;

struct Traffic {
    rng: u64,
    store: ShmAggregateStore,
    region: ShmRegion,
    second: u32,
    deltas: DeltaSink<u32>,
    expirations: ExpirationSink<u32>,
    /// The keys written in each live bucket, by `bucket % LIVE_BUCKETS`.
    history: Vec<Vec<KeyHash>>,
}

impl Traffic {
    fn new() -> Self {
        let layout = Layout::new(2, SLOTS).expect("layout");
        let zone = Box::leak(vec![0_u64; layout.total_bytes.div_ceil(8)].into_boxed_slice());
        // SAFETY: `zone` is a fresh, writable, 8-byte-aligned allocation of
        // at least `layout.total_bytes`, `initialize` is its first user, and
        // the leak keeps it alive for the whole bench.
        let region = unsafe { ShmRegion::initialize(zone.as_mut_ptr().cast(), layout) };
        let mut traffic = Self {
            rng: 0x9E37_79B9_7F4A_7C15,
            store: ShmAggregateStore::new(region),
            region,
            second: LIVE_BUCKETS,
            deltas: DeltaSink::with_capacity(KEYS_PER_SECOND),
            expirations: ExpirationSink::with_capacity(KEYS_PER_SECOND),
            history: vec![Vec::with_capacity(KEYS_PER_SECOND); LIVE_BUCKETS as usize],
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

    /// One second: expire the bucket that aged out, record the new one.
    fn advance(&mut self) {
        self.second += 1;
        let slot = (self.second % LIVE_BUCKETS) as usize;
        let expired = self.second - LIVE_BUCKETS;
        let mut keys = std::mem::take(&mut self.history[slot]);
        self.deltas.clear();
        self.expirations.clear();
        for &key_hash in &keys {
            let key = cell(key_hash, expired);
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
            self.deltas.handles.push(CellHandle::default());
            self.deltas.keys.push(cell(key_hash, self.second));
            self.deltas.previous.push(0);
            self.deltas.current.push(1);
            self.deltas.deltas.push(1);
            self.deltas.applies_locally.push(1);
        }
        self.history[slot] = keys;
        self.store.apply(&self.deltas, &self.expirations);
    }
}

fn cell(key_hash: KeyHash, bucket: u32) -> CellIdentity {
    CellIdentity {
        rule_fingerprint: RULE,
        key_hash,
        bucket,
    }
}

fn bench_aggregate(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate");
    let mut traffic = Traffic::new();

    group.throughput(Throughput::Elements(1));
    group.bench_function("window_total", |b| {
        b.iter(|| {
            let key = if traffic.next().is_multiple_of(20) {
                KeyHash(u128::from(traffic.next()) << 64)
            } else {
                traffic.active_key()
            };
            let now = u64::from(traffic.second) * 1_000;
            black_box(
                traffic
                    .region
                    .aggregate()
                    .window_total(RULE, key.0, now, 1_000, 10),
            )
        });
    });

    group.throughput(Throughput::Elements(2 * KEYS_PER_SECOND as u64));
    group.bench_function("apply_one_second", |b| b.iter(|| traffic.advance()));
    group.finish();
}

criterion_group!(benches, bench_aggregate);
criterion_main!(benches);
