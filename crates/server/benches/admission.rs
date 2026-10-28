//! `gabiond`'s admission section in production steady state: one Envoy
//! descriptor matching 1, 16 or 64 rules, with `storage.max_matched_rules` at
//! 64, 95% of keys already counted and 5% new. The gossip runtime drains the
//! request queue between batches, outside the timed section, as a running
//! node's does between requests.
//!
//! * `admission/matched_rules_95_5` — one decision; the cost per matched rule
//!   stays flat as the bound grows, because the scratch it fills is reserved
//!   once per thread.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gabion::aggregate::Aggregate;
use gabion::crdt::{CellStore, NodeId, NodeIdentity};
use gabion::gossip::sim::SimRouter;
use gabion::gossip::{GossipClient, GossipRuntime, TokioClock};
use gabion::rules::{EnforcementMode, RuleDef, RuleTable};
use gabion::settings::Settings;
use gabion_server::{RateLimitDescriptor, SharedLimiter, rate_limit_descriptor};
use tokio::runtime::Runtime;
use tokio::task::LocalSet;

/// Hits queued between drains: tokio's request channel keeps three spare
/// 32-slot blocks, so a deeper queue would time its block allocations.
const QUEUED_HITS: u64 = 96;

fn descriptor(tenant: String) -> RateLimitDescriptor {
    RateLimitDescriptor {
        entries: vec![rate_limit_descriptor::Entry {
            key: "tenant".into(),
            value: tenant,
        }],
        ..Default::default()
    }
}

struct Node {
    runtime: Runtime,
    local: LocalSet,
    limiter: SharedLimiter<u32>,
    client: GossipClient<u32>,
}

impl Node {
    fn new(rules: u16) -> Self {
        let settings = Settings {
            max_matched_rules: 64.try_into().unwrap(),
            limit_queue_capacity: (1 << 20).try_into().unwrap(),
            rng_seed: Some(1),
            ..Settings::default()
        };
        let (gossip, storage) = settings.validate().expect("settings validate");
        let built = (1..=rules)
            .map(|i| {
                RuleDef {
                    name: &format!("r{i}"),
                    domain: "api",
                    patterns: &[("tenant", "*")],
                    rate: &format!("1000000000000r/{i}s"),
                    window: None,
                    bucket: None,
                    mode: EnforcementMode::Enforce,
                }
                .build(&storage.cardinality)
                .expect("rule")
            })
            .collect();
        let table = Arc::new(RuleTable::new(built, None).expect("table"));
        let counts = Arc::new(Aggregate::new(&storage));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime");
        let local = LocalSet::new();
        let (gossip_rt, client) = local.block_on(&runtime, async {
            GossipRuntime::from_parts(
                SimRouter::new().bind("127.0.0.1:7000".parse().unwrap()),
                TokioClock::from_millis(0),
                gossip,
                CellStore::<u32>::new(storage, NodeIdentity::new(NodeId(1), 1), &table),
                counts.clone(),
            )
        });
        local.spawn_local(gossip_rt.run(futures::stream::empty()));
        let limiter = SharedLimiter::new(table, client.clone(), counts, storage);
        Self {
            runtime,
            local,
            limiter,
            client,
        }
    }

    /// Run the gossip runtime until it has applied every queued hit.
    fn drain(&self) {
        let client = &self.client;
        self.local.block_on(&self.runtime, async {
            drop(client.reserve(client.capacity()).await);
        });
    }
}

fn bench_admission(c: &mut Criterion) {
    let mut group = c.benchmark_group("admission");
    group.throughput(Throughput::Elements(1));
    for rules in [1_u16, 16, 64] {
        let node = Node::new(rules);
        let hot: Vec<RateLimitDescriptor> =
            (0..50).map(|i| descriptor(format!("hot-{i}"))).collect();
        let mut fresh = 0_u64;
        group.bench_function(BenchmarkId::new("matched_rules_95_5", rules), |b| {
            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                let mut left = iters;
                while left > 0 {
                    let batch = left.min((QUEUED_HITS / u64::from(rules)).max(1));
                    // Built outside the timed section: 5% new tenants.
                    let requests: Vec<RateLimitDescriptor> = (0..batch)
                        .map(|i| {
                            if i % 20 == 0 {
                                fresh += 1;
                                descriptor(format!("new-{fresh}"))
                            } else {
                                hot[i as usize % hot.len()].clone()
                            }
                        })
                        .collect();
                    let start = Instant::now();
                    for request in &requests {
                        black_box(node.limiter.decide("api", request, 1, 0));
                    }
                    elapsed += start.elapsed();
                    node.drain();
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
