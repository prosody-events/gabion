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
    gossip_handle: tokio::task::JoinHandle<Result<(), gabion::gossip::GossipError>>,
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
        gossip_handle,
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

/// The gossip runtime can die out from under the request path (network
/// failure, transport panic recovered, etc.). Per allow-by-default the
/// decision must stay `Allow` even though the deltas can no longer be
/// recorded. Forcing the failure is straightforward: shut the runtime
/// down, await its exit, then issue another request — `record()` returns
/// `RuntimeShutDown` and `note_gossip_record_failure` swallows it.
#[test]
fn gossip_record_failure_does_not_reject_request() {
    run_local(async {
        let harness = harness(512, 100).await;

        // Bring the runtime down and wait for it to actually exit so the
        // next `record()` is guaranteed to hit the closed channel.
        harness.client.shutdown().await.expect("shutdown sent");
        harness
            .gossip_handle
            .await
            .expect("gossip task joined")
            .expect("gossip exited cleanly");

        let request = RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "a")])],
            hits_addend: 1,
        };
        let response = harness.limiter.should_rate_limit_at(request, 0).await;
        assert_eq!(
            response.overall_code,
            rate_limit_response::Code::Ok as i32,
            "a dead gossip runtime must not reject — see CLAUDE.md (allow-by-default)",
        );
    });
}

/// Boundary check for `MAX_DESCRIPTORS` on the request-shape cardinality
/// guard. Exactly the cap is accepted; cap + 1 is rejected. Catches an
/// off-by-one slip from `==` to `>`/`>=`.
#[test]
fn cardinality_accepts_max_descriptors() {
    run_local(async {
        let harness = harness(512, u64::from(u32::MAX)).await;

        let descriptors = (0..MAX_DESCRIPTORS)
            .map(|i| descriptor([("tenant", format!("t-{i}"))]))
            .collect::<Vec<_>>();
        assert_eq!(descriptors.len(), MAX_DESCRIPTORS);
        let request = RateLimitRequest {
            domain: "api".into(),
            descriptors,
            hits_addend: 1,
        };

        let response = harness.limiter.should_rate_limit_at(request, 0).await;
        assert_eq!(
            response.overall_code,
            rate_limit_response::Code::Ok as i32,
            "exactly MAX_DESCRIPTORS must be accepted",
        );
        assert_eq!(response.statuses.len(), MAX_DESCRIPTORS);

        let _ = harness.client.shutdown().await;
    });
}

#[test]
fn cardinality_rejects_max_plus_one_descriptors() {
    run_local(async {
        let harness = harness(512, u64::from(u32::MAX)).await;

        let descriptors = (0..=MAX_DESCRIPTORS)
            .map(|i| descriptor([("tenant", format!("t-{i}"))]))
            .collect::<Vec<_>>();
        assert_eq!(descriptors.len(), MAX_DESCRIPTORS + 1);
        let request = RateLimitRequest {
            domain: "api".into(),
            descriptors,
            hits_addend: 1,
        };

        let response = harness.limiter.should_rate_limit_at(request, 0).await;
        assert_eq!(
            response.overall_code,
            rate_limit_response::Code::OverLimit as i32,
            "MAX_DESCRIPTORS + 1 must trip the cardinality reject",
        );
        // Only the overflow descriptor (index == MAX_DESCRIPTORS) is rejected.
        let overflow_status = response
            .statuses
            .last()
            .expect("at least one status returned");
        assert_eq!(
            overflow_status.code,
            rate_limit_response::Code::OverLimit as i32,
        );

        let _ = harness.client.shutdown().await;
    });
}

/// Once a descriptor goes over its limit, the per-descriptor status must
/// carry both `limit_remaining` (0 on reject) and a non-empty
/// `duration_until_reset`. Today's response leaves both at proto defaults
/// — this test pins the populated shape so future refactors don't silently
/// regress to empty fields.
#[test]
fn over_limit_populates_descriptor_status_fields() {
    run_local(async {
        // limit=2, rule window=1s, bucket=100ms (live=10).
        let harness = harness(512, 2).await;
        let make_request = || RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "a")])],
            hits_addend: 1,
        };

        // Fill to the limit at now=0.
        for tick in 0..2_u64 {
            let r = harness
                .limiter
                .should_rate_limit_at(make_request(), tick)
                .await;
            assert_eq!(r.overall_code, rate_limit_response::Code::Ok as i32);
        }

        // 3rd request at now=0 trips the reject. With both prior hits
        // in bucket 0 (oldest visible), need=1 is satisfied as soon as
        // bucket 0 falls off — fall_off = (0+10)*100 = 1000ms, delta
        // from now=0 is 1000ms.
        let over = harness
            .limiter
            .should_rate_limit_at(make_request(), 0)
            .await;
        assert_eq!(
            over.overall_code,
            rate_limit_response::Code::OverLimit as i32,
        );
        let status = over.statuses.first().expect("one status");
        assert_eq!(status.code, rate_limit_response::Code::OverLimit as i32);
        assert_eq!(status.limit_remaining, 0, "remaining must be 0 on reject");
        let duration = status
            .duration_until_reset
            .as_ref()
            .expect("duration_until_reset must be populated");
        let millis = (duration.seconds as u64) * 1_000 + (duration.nanos as u64) / 1_000_000;
        assert_eq!(
            millis, 1_000,
            "delta should be the oldest bucket's fall-off"
        );

        let _ = harness.client.shutdown().await;
    });
}

/// An allowed descriptor whose rule matched cleanly must carry both
/// `limit_remaining` and `duration_until_reset` so the Envoy filter can
/// render `X-RateLimit-*` on the OK response — the same shape an Envoy
/// fleet with `enable_x_ratelimit_headers: DRAFT_VERSION_03` reads.
#[test]
fn under_limit_populates_descriptor_status_fields() {
    run_local(async {
        // limit=5, rule window=1s, bucket=100ms (live=10).
        let harness = harness(512, 5).await;
        let response = harness
            .limiter
            .should_rate_limit_at(
                RateLimitRequest {
                    domain: "api".into(),
                    descriptors: vec![descriptor([("tenant", "a")])],
                    hits_addend: 1,
                },
                0,
            )
            .await;
        assert_eq!(response.overall_code, rate_limit_response::Code::Ok as i32);
        let status = response.statuses.first().expect("one status");
        assert_eq!(status.code, rate_limit_response::Code::Ok as i32);
        assert_eq!(
            status.limit_remaining, 4,
            "limit=5, hits=1, total=0 -> remaining=4"
        );
        let duration = status
            .duration_until_reset
            .as_ref()
            .expect("duration_until_reset must be populated on allow");
        let millis = (duration.seconds as u64) * 1_000 + (duration.nanos as u64) / 1_000_000;
        // bucket=100ms, now=0 sits exactly on a boundary -> next
        // boundary is a full bucket away.
        assert_eq!(
            millis, 100,
            "allow-path reset = ms until the next bucket boundary"
        );
        let _ = harness.client.shutdown().await;
    });
}

/// DryRun rule already past its limit must still emit an OK status with
/// `limit_remaining = 0`, mirroring the nginx adapter's behaviour so the
/// proxy fleet can graph "share that would have been 429'd".
#[test]
fn dry_run_over_limit_populates_descriptor_status_fields() {
    run_local(async {
        let router = SimRouter::new();
        let addr = next_test_addr();
        let transport = router.bind(addr);
        let identity = NodeIdentity::new(NodeId(0xAA02), 1);
        let rule = Rule::new(
            1,
            "api",
            vec![DescriptorPattern {
                key: "tenant".into(),
                value: "*".into(),
            }],
            2,
            1_000,
            100,
            EnforcementMode::DryRun,
        );
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
        let _handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));

        let rule_table = Arc::new(RuleTable::new(vec![rule]));
        let cardinality_limits = CardinalityLimits {
            max_descriptor_count: 16,
            max_descriptor_bytes: 512,
            max_key_bytes: 64,
        };
        let limiter = SharedLimiter::new(rule_table, client.clone(), counts, cardinality_limits);

        // Drive enough hits to push past the DryRun budget. Each
        // request is allowed because the rule never rejects.
        for tick in 0..5_u64 {
            let r = limiter
                .should_rate_limit_at(
                    RateLimitRequest {
                        domain: "api".into(),
                        descriptors: vec![descriptor([("tenant", "a")])],
                        hits_addend: 1,
                    },
                    tick,
                )
                .await;
            assert_eq!(
                r.overall_code,
                rate_limit_response::Code::Ok as i32,
                "DryRun must never overall-reject",
            );
        }

        // Next request: still allowed, but the descriptor status must
        // report `Remaining = 0` so observers see the bypass.
        let over = limiter
            .should_rate_limit_at(
                RateLimitRequest {
                    domain: "api".into(),
                    descriptors: vec![descriptor([("tenant", "a")])],
                    hits_addend: 1,
                },
                5,
            )
            .await;
        assert_eq!(
            over.overall_code,
            rate_limit_response::Code::Ok as i32,
            "DryRun over-limit must still allow at the request level",
        );
        let status = over.statuses.first().expect("one status");
        assert_eq!(status.code, rate_limit_response::Code::Ok as i32);
        assert_eq!(
            status.limit_remaining, 0,
            "saturating: DryRun past limit -> Remaining = 0"
        );
        assert!(
            status.duration_until_reset.is_some(),
            "duration_until_reset must still be populated"
        );
        let _ = client.shutdown().await;
    });
}

/// Pre-admission rejects (cardinality, MAX_DESCRIPTORS overflow) carry no
/// rule scope, so the adapter cannot compute a real reset moment. Verify
/// the response leaves `duration_until_reset` unset rather than inventing
/// a value.
#[test]
fn cardinality_reject_leaves_duration_unset() {
    run_local(async {
        // max_descriptor_bytes = 8 forces the cardinality envelope to
        // trip on any descriptor with a non-trivial value.
        let harness = harness(8, 10).await;
        let response = harness
            .limiter
            .should_rate_limit_at(
                RateLimitRequest {
                    domain: "api".into(),
                    descriptors: vec![descriptor([("tenant", "long-value-here")])],
                    hits_addend: 1,
                },
                0,
            )
            .await;
        assert_eq!(
            response.overall_code,
            rate_limit_response::Code::OverLimit as i32,
        );
        let status = response.statuses.first().expect("one status");
        assert_eq!(status.code, rate_limit_response::Code::OverLimit as i32);
        assert_eq!(status.limit_remaining, 0);
        assert!(
            status.duration_until_reset.is_none(),
            "pre-admission reject must not invent a reset moment"
        );
        let _ = harness.client.shutdown().await;
    });
}

/// End-to-end gRPC round-trip against the actual tonic Server. Catches
/// proto field add/remove regressions and runtime mounting errors that
/// the in-process harness skips by calling `should_rate_limit_at`
/// directly.
#[test]
fn grpc_transport_round_trip() {
    use envoy_types::pb::envoy::service::ratelimit::v3::rate_limit_service_client::RateLimitServiceClient;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("multi-thread runtime");
    let local = LocalSet::new();
    local.block_on(&rt, async {
        let harness = harness(512, 2).await;
        let limiter = harness.limiter.clone();

        // Reserve an ephemeral port via a probe bind, drop it, then hand
        // the address to tonic. The kernel will reuse the port for a
        // brief window; the client retry loop covers the race.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
        let bound = probe.local_addr().expect("local_addr");
        drop(probe);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RateLimitServiceServer::new(EnvoyRateLimitService::new(
                    limiter,
                )))
                .serve_with_shutdown(bound, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let mut client = None;
        for attempt in 0..50 {
            match RateLimitServiceClient::connect(format!("http://{bound}")).await {
                Ok(c) => {
                    client = Some(c);
                    break;
                }
                Err(err) if attempt < 49 => {
                    tracing::debug!(?err, "waiting for tonic server");
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Err(err) => panic!("client could not connect: {err:?}"),
            }
        }
        let mut client = client.expect("client connected");

        // First call: under limit, expect OK with a single descriptor status.
        let response = client
            .should_rate_limit(RateLimitRequest {
                domain: "api".into(),
                descriptors: vec![descriptor([("tenant", "t-grpc")])],
                hits_addend: 1,
            })
            .await
            .expect("rpc OK")
            .into_inner();
        assert_eq!(response.overall_code, rate_limit_response::Code::Ok as i32,);
        assert_eq!(response.statuses.len(), 1);
        assert_eq!(
            response.statuses[0].code,
            rate_limit_response::Code::Ok as i32,
        );

        // Drive the same key over the configured limit (2) so the next
        // request must come back OverLimit through the full stack.
        for _ in 0..3 {
            let _ = client
                .should_rate_limit(RateLimitRequest {
                    domain: "api".into(),
                    descriptors: vec![descriptor([("tenant", "t-grpc")])],
                    hits_addend: 1,
                })
                .await
                .expect("rpc OK");
        }
        let over = client
            .should_rate_limit(RateLimitRequest {
                domain: "api".into(),
                descriptors: vec![descriptor([("tenant", "t-grpc")])],
                hits_addend: 1,
            })
            .await
            .expect("rpc OK")
            .into_inner();
        assert_eq!(
            over.overall_code,
            rate_limit_response::Code::OverLimit as i32,
        );

        // Empty descriptor list still round-trips successfully with no
        // statuses — guards against an accidental proto reshape that
        // forces the server to error on edge inputs.
        let empty = client
            .should_rate_limit(RateLimitRequest {
                domain: "api".into(),
                descriptors: vec![],
                hits_addend: 1,
            })
            .await
            .expect("rpc OK")
            .into_inner();
        assert_eq!(empty.overall_code, rate_limit_response::Code::Ok as i32,);
        assert!(empty.statuses.is_empty());

        drop(client);
        let _ = shutdown_tx.send(());
        server_handle
            .await
            .expect("join")
            .expect("server clean exit");
        let _ = harness.client.shutdown().await;
    });
}
