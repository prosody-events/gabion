//! gRPC adapter tests. Driven through a sim-routed gossip runtime so the
//! whole record-then-read flow exercises real CRDT + AggregateStore code.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use tokio::task::LocalSet;

use gabion::crdt::{CellStore, CellStoreConfig, NodeId, NodeIdentity};
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
    let store = CellStore::<u32>::new(CellStoreConfig::default(), identity);
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

    let rule_table = Arc::new(RuleTable::new(vec![rule_tenant(limit)]));
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

        let first = harness.limiter.should_rate_limit_at(request.clone(), 0).await;
        let second = harness
            .limiter
            .should_rate_limit_at(request, 1)
            .await;

        assert_eq!(first.overall_code, rate_limit_response::Code::Ok as i32);
        // The second request pushes the bucket from 2 to 4 — still under the
        // 10-hit limit.
        assert_eq!(second.overall_code, rate_limit_response::Code::Ok as i32);
        assert_eq!(second.statuses.len(), 1);

        // Pump until we exceed the limit; record-then-read means the
        // rejecting request also credits the bucket.
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
fn record_then_read_credits_rejected_requests() {
    // The "all-or-nothing" semantic from the LocalEngine flow is gone.
    // Under record-then-read, a multi-descriptor request that crosses the
    // limit on one descriptor still credits the other descriptor's bucket.
    run_local(async {
        let harness = harness(512, 3).await;

        let fill_b = RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "b")])],
            hits_addend: 3,
        };
        assert_eq!(
            harness.limiter.should_rate_limit_at(fill_b, 0).await.overall_code,
            rate_limit_response::Code::Ok as i32
        );

        // Mixed request: tenant "a" still has room, tenant "b" is at limit.
        let mixed = RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "a")]), descriptor([("tenant", "b")])],
            hits_addend: 1,
        };
        let mixed_response = harness.limiter.should_rate_limit_at(mixed, 1).await;
        assert_eq!(
            mixed_response.overall_code,
            rate_limit_response::Code::OverLimit as i32
        );
        // The "a" descriptor should be allowed; "b" rejected.
        assert_eq!(mixed_response.statuses.len(), 2);
        assert_eq!(
            mixed_response.statuses[0].code,
            rate_limit_response::Code::Ok as i32
        );
        assert_eq!(
            mixed_response.statuses[1].code,
            rate_limit_response::Code::OverLimit as i32
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
