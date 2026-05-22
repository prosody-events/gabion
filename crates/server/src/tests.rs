//! gRPC adapter tests. Driven through a sim-routed gossip runtime so the
//! whole record-then-read flow exercises real CRDT + AggregateStore code.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use tokio::task::LocalSet;

use gabion::crdt::{CellStore, CellStoreConfig, NodeId, NodeIdentity, RuleDescriptor};
use gabion::gossip::sim::SimRouter;
use gabion::gossip::{GossipConfig, GossipRuntime, TokioClock};
use gabion::rules::{DescriptorPattern, EnforcementMode, Rule, RuleTable};

use super::*;
use crate::admission::CardinalityLimits;
use crate::store::DashMapStore;

static TEST_PORT: AtomicU16 = AtomicU16::new(40_500);

fn next_test_addr() -> SocketAddr {
    let port = TEST_PORT.fetch_add(1, Ordering::Relaxed);
    SocketAddr::from(([127, 0, 0, 1], port))
}

struct Harness {
    limiter: SharedLimiter<u32>,
    _gossip_handle: tokio::task::JoinHandle<Result<(), gabion::gossip::GossipError>>,
    client: gabion::gossip::GossipClient<u32>,
}

fn rule_tenant(limit: u64) -> Rule {
    Rule::new(
        1,
        "api",
        vec![DescriptorPattern {
            key: "tenant".into(),
            value: "*".into(),
        }],
        limit,
        1_000,
        100,
        EnforcementMode::Enforce,
    )
}

async fn harness(max_descriptor_bytes: usize, limit: u64) -> Harness {
    let router = SimRouter::new();
    let addr = next_test_addr();
    let transport = router.bind(addr);
    let identity = NodeIdentity::new(NodeId(0xAA00), 1);
    let rule = rule_tenant(limit);
    let mut store = CellStore::<u32>::new(CellStoreConfig::default(), identity);
    store.intern_rule(RuleDescriptor {
        fingerprint: rule.fingerprint,
        window_millis: rule.window_millis as u32,
        bucket_millis: rule.bucket_millis as u32,
        limit: rule.limit,
        flags: 0,
        local_rule_id: rule.id,
    });
    let counts = Arc::new(DashMapStore::<u32>::with_capacity(64));

    let gossip_config = GossipConfig {
        local_identity: identity,
        rng_seed: 1,
        ..GossipConfig::default()
    };

    let (rt, client) = GossipRuntime::from_parts(
        transport,
        TokioClock::from_millis(0),
        gossip_config,
        store,
        counts.clone(),
    );
    let gossip_handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));

    let rule_table = Arc::new(RuleTable::new(vec![rule]));
    let cardinality_limits = CardinalityLimits {
        max_descriptor_count: 16,
        max_descriptor_bytes,
        max_key_bytes: 64,
    };
    let limiter = SharedLimiter::new(rule_table, client.clone(), counts, cardinality_limits);

    Harness {
        limiter,
        _gossip_handle: gossip_handle,
        client,
    }
}

fn run_local<F: std::future::Future<Output = T>, T>(future: F) -> T {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    let local = LocalSet::new();
    local.block_on(&rt, future)
}

#[test]
fn maps_descriptors_and_returns_ok_or_over_limit() {
    run_local(async {
        let harness = harness(512, 10).await;

        let request = RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "a")])],
            hits_addend: 2,
        };

        let first = harness
            .limiter
            .should_rate_limit_at(request.clone(), 0)
            .await;
        let second = harness.limiter.should_rate_limit_at(request, 1).await;

        assert_eq!(first.overall_code, rate_limit_response::Code::Ok as i32);
        // The second request pushes the bucket from 2 to 4 — still under the
        // 10-hit limit.
        assert_eq!(second.overall_code, rate_limit_response::Code::Ok as i32);
        assert_eq!(second.statuses.len(), 1);

        // Pump until we exceed the limit.
        for tick in 2..10 {
            let request = RateLimitRequest {
                domain: "api".into(),
                descriptors: vec![descriptor([("tenant", "a")])],
                hits_addend: 2,
            };
            harness
                .limiter
                .should_rate_limit_at(request, tick as u64)
                .await;
        }
        let over = RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "a")])],
            hits_addend: 2,
        };
        let response = harness.limiter.should_rate_limit_at(over, 11).await;
        assert_eq!(
            response.overall_code,
            rate_limit_response::Code::OverLimit as i32
        );

        let _ = harness.client.shutdown().await;
    });
}

#[test]
fn rejected_requests_do_not_credit_rolling_window() {
    run_local(async {
        let harness = harness(512, 5).await;

        let fill = RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "a")])],
            hits_addend: 5,
        };
        assert_eq!(
            harness
                .limiter
                .should_rate_limit_at(fill, 0)
                .await
                .overall_code,
            rate_limit_response::Code::Ok as i32
        );

        // The rule uses a 1s window with 100ms buckets. Every over-limit
        // request in buckets 1..9 must reject without extending the rolling
        // window. If rejected requests were credited, the request at bucket 10
        // would still be over limit.
        for tick in 1..10 {
            let rejected = RateLimitRequest {
                domain: "api".into(),
                descriptors: vec![descriptor([("tenant", "a")])],
                hits_addend: 1,
            };
            assert_eq!(
                harness
                    .limiter
                    .should_rate_limit_at(rejected, tick * 100)
                    .await
                    .overall_code,
                rate_limit_response::Code::OverLimit as i32
            );
        }

        let recovered = RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "a")])],
            hits_addend: 1,
        };
        assert_eq!(
            harness
                .limiter
                .should_rate_limit_at(recovered, 1_000)
                .await
                .overall_code,
            rate_limit_response::Code::Ok as i32
        );

        let _ = harness.client.shutdown().await;
    });
}

#[test]
fn rejects_requests_over_cardinality_limits_before_recording() {
    run_local(async {
        let harness = harness(8, 10).await;
        let request = RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "long-value-here")])],
            hits_addend: 1,
        };

        let response = harness.limiter.should_rate_limit_at(request, 0).await;

        assert_eq!(
            response.overall_code,
            rate_limit_response::Code::OverLimit as i32
        );
        let _ = harness.client.shutdown().await;
    });
}

/// More rules match a single request than `STORAGE_MAX_MATCHED_RULES`
/// allows. Per the allow-by-default principle in CLAUDE.md, gabion's
/// internal cap is not a client-facing reject — the request must pass
/// through. Records for the rules that fit in the buffer are still
/// emitted; the rest are silently under-counted.
#[test]
fn matched_rule_overflow_allows_rather_than_rejects() {
    run_local(async {
        let router = SimRouter::new();
        let addr = next_test_addr();
        let transport = router.bind(addr);
        let identity = NodeIdentity::new(NodeId(0xAA01), 1);

        let overflow = gabion::defaults::STORAGE_MAX_MATCHED_RULES + 4;
        let rules: Vec<Rule> = (0..overflow)
            .map(|i| {
                Rule::new(
                    (i + 1) as u32,
                    "api",
                    vec![DescriptorPattern {
                        key: "tenant".into(),
                        value: "*".into(),
                    }],
                    100,
                    1_000,
                    100,
                    EnforcementMode::Enforce,
                )
            })
            .collect();

        let mut store = CellStore::<u32>::new(CellStoreConfig::default(), identity);
        for rule in &rules {
            store.intern_rule(RuleDescriptor {
                fingerprint: rule.fingerprint,
                window_millis: rule.window_millis as u32,
                bucket_millis: rule.bucket_millis as u32,
                limit: rule.limit,
                flags: 0,
                local_rule_id: rule.id,
            });
        }
        let counts = Arc::new(DashMapStore::<u32>::with_capacity(64));
        let gossip_config = GossipConfig {
            local_identity: identity,
            rng_seed: 1,
            ..GossipConfig::default()
        };
        let (rt, client) = GossipRuntime::from_parts(
            transport,
            TokioClock::from_millis(0),
            gossip_config,
            store,
            counts.clone(),
        );
        let _handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));

        let rule_table = Arc::new(RuleTable::new(rules));
        let cardinality_limits = CardinalityLimits {
            max_descriptor_count: 16,
            max_descriptor_bytes: 512,
            max_key_bytes: 64,
        };
        let limiter = SharedLimiter::new(rule_table, client.clone(), counts, cardinality_limits);

        let request = RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "a")])],
            hits_addend: 1,
        };
        let response = limiter.should_rate_limit_at(request, 0).await;

        assert_eq!(
            response.overall_code,
            rate_limit_response::Code::Ok as i32,
            "matched-rules overflow must allow, not reject — see CLAUDE.md",
        );
        let _ = client.shutdown().await;
    });
}

#[derive(Clone, Debug)]
struct EnvoyRequestCase {
    tenants: Vec<u8>,
    hits: u8,
}

impl Arbitrary for EnvoyRequestCase {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut tenants = Vec::<u8>::arbitrary(g);
        tenants.truncate(16);
        Self {
            tenants,
            hits: (u8::arbitrary(g) % 4).max(1),
        }
    }
}

#[quickcheck]
fn quickcheck_response_shape_matches_descriptor_shape(case: EnvoyRequestCase) -> TestResult {
    run_local(async move {
        let harness = harness(512, 1_000).await;
        let descriptors = case
            .tenants
            .iter()
            .map(|tenant| descriptor([("tenant", format!("tenant-{tenant}"))]))
            .collect::<Vec<_>>();
        let descriptor_count = descriptors.len();
        let request = RateLimitRequest {
            domain: "api".into(),
            descriptors,
            hits_addend: u32::from(case.hits),
        };

        let response = harness.limiter.should_rate_limit_at(request, 0).await;
        let any_over_limit = response
            .statuses
            .iter()
            .any(|status| status.code == rate_limit_response::Code::OverLimit as i32);
        let _ = harness.client.shutdown().await;

        if response.statuses.len() != descriptor_count {
            return TestResult::error(
                "envoy response status length did not match descriptor count",
            );
        }
        if (response.overall_code == rate_limit_response::Code::OverLimit as i32) != any_over_limit
        {
            return TestResult::error("envoy overall response code did not summarize statuses");
        }
        TestResult::passed()
    })
}
