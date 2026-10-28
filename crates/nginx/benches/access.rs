//! The NGINX access phase in production steady state: one request on a
//! location whose 1, 16 or 64 rules all match, with
//! `gabion_storage_max_matched_rules` at 64, 95% of tenants already counted
//! in the shared aggregate and 5% new. The queue is drained between batches,
//! outside the timed section, as the leader drains it.
//!
//! * `admission/matched_rules_95_5` — one `decide_all`; the cost per matched
//!   rule stays flat as the bound grows, because the scratch it fills is
//!   reserved once per worker.

use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gabion::crdt::{CellHandle, CellIdentity, DeltaSink, ExpirationSink, ValidStorageConfig};
use gabion::gossip::AggregateStore;
use gabion::rules::{Descriptor, hash_key};
use gabion::settings::Settings;
use gabion_nginx::access::{AccessCtx, VariableLookup, decide_all};
use gabion_nginx::rules::{
    BindingLookup, CompiledRules, NopBindingCompiler, RuleConfig, parse_rule_directive,
};
use gabion_nginx::shm::queue::QueueEvent;
use gabion_nginx::shm::{Layout, ShmAggregateStore, ShmRegion};

/// Requests between drains.
const BATCH: u64 = 64;

/// The request's `$http_x_tenant`.
struct Tenant<'a>(&'a [u8]);

impl VariableLookup for Tenant<'_> {
    fn lookup(&self, binding: &BindingLookup) -> Option<&[u8]> {
        matches!(binding, BindingLookup::IndexedVariable { name, .. } if &**name == "http_x_tenant")
            .then_some(self.0)
    }
}

/// A worker's view: `rules` rules keyed on the tenant, and a zone whose
/// aggregate holds the 50 hot tenants' counts, as the leader wrote them.
struct Worker {
    rules: CompiledRules,
    storage: ValidStorageConfig,
    region: ShmRegion,
    hot: Vec<String>,
}

impl Worker {
    fn new(rules: u16) -> Self {
        let settings = Settings {
            max_matched_rules: 64.try_into().unwrap(),
            ..Settings::default()
        };
        let (_, storage) = settings.validate().expect("settings validate");
        let configs: Vec<RuleConfig> = (0..rules)
            .map(|i| {
                let name = format!("r{i}");
                let binding = format!("k{i}:$http_x_tenant");
                parse_rule_directive(&[&name, &binding, "rate=1000000000r/s"]).expect("directive")
            })
            .collect();
        let rules = CompiledRules::compile_with(&configs, &storage, &mut NopBindingCompiler)
            .expect("compile");
        let layout = Layout::new(1 << 13, 1 << 14).expect("layout");
        let zone = Box::leak(vec![0_u64; layout.total_bytes.div_ceil(8)].into_boxed_slice());
        // SAFETY: `zone` is a fresh, writable, 8-byte-aligned allocation of
        // at least `layout.total_bytes`, `initialize` is its first user, and
        // the leak keeps it alive for the whole bench.
        let region = unsafe { ShmRegion::initialize(zone.as_mut_ptr().cast(), layout) };
        let hot: Vec<String> = (0..50).map(|i| format!("hot-{i}")).collect();
        let mut deltas = DeltaSink::<u32>::default();
        for (i, rule) in rules.table().rules().iter().enumerate() {
            let key = format!("k{i}");
            for tenant in &hot {
                deltas.handles.push(CellHandle::default());
                deltas.keys.push(CellIdentity {
                    rule_fingerprint: rule.fingerprint,
                    key_hash: hash_key(
                        "nginx",
                        [Descriptor {
                            key: &key,
                            value: tenant,
                        }],
                    ),
                    bucket: 0,
                });
                deltas.previous.push(0);
                deltas.current.push(1);
                deltas.deltas.push(1);
                deltas.applies_locally.push(1);
            }
        }
        ShmAggregateStore::new(region).apply(&deltas, &ExpirationSink::default());
        Self {
            rules,
            storage,
            region,
            hot,
        }
    }

    fn ctx(&self) -> AccessCtx<'_> {
        AccessCtx {
            rules: &self.rules,
            aggregate: self.region.aggregate(),
            queue: self.region.queue(),
            stats: self.region.stats(),
            cardinality: self.storage.cardinality,
            max_matched_rules: self.storage.max_matched_rules(),
        }
    }
}

fn bench_admission(c: &mut Criterion) {
    let mut group = c.benchmark_group("admission");
    group.throughput(Throughput::Elements(1));
    for rules in [1_u16, 16, 64] {
        let worker = Worker::new(rules);
        let indices: Vec<usize> = (0..usize::from(rules)).collect();
        let mut events = Vec::new();
        let mut drained = vec![QueueEvent::default(); 1 << 13];
        let mut fresh = 0_u64;
        group.bench_function(BenchmarkId::new("matched_rules_95_5", rules), |b| {
            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                let mut left = iters;
                while left > 0 {
                    let batch = left.min(BATCH);
                    // Built outside the timed section: 5% new tenants.
                    let tenants: Vec<String> = (0..batch)
                        .map(|i| {
                            if i % 20 == 0 {
                                fresh += 1;
                                format!("new-{fresh}")
                            } else {
                                worker.hot[i as usize % worker.hot.len()].clone()
                            }
                        })
                        .collect();
                    let start = Instant::now();
                    for tenant in &tenants {
                        let vars = Tenant(tenant.as_bytes());
                        black_box(decide_all(worker.ctx(), &indices, &vars, 0, &mut events));
                    }
                    elapsed += start.elapsed();
                    worker.region.queue().drain(&mut drained);
                    left -= batch;
                }
                elapsed
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_admission);
criterion_main!(benches);
