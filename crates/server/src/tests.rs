//! gRPC adapter tests. Driven through a sim-routed gossip runtime so the
//! whole record-then-read flow exercises real CRDT + AggregateStore code.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use tokio::task::LocalSet;

use gabion::crdt::{CellStore, NodeId, NodeIdentity, RuleDescriptor, ValidStorageConfig};
use gabion::gossip::sim::SimRouter;
use gabion::gossip::{GossipRuntime, TokioClock, ValidGossipConfig};
use gabion::rules::{DescriptorPattern, EnforcementMode, Rule, RuleTable};
use gabion::settings::{Setting, Settings};

use super::*;
use crate::store::DashMapStore;

static TEST_PORT: AtomicU16 = AtomicU16::new(40_500);

fn descriptor(
    entries: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
) -> RateLimitDescriptor {
    RateLimitDescriptor {
        entries: entries
            .into_iter()
            .map(|(key, value)| rate_limit_descriptor::Entry {
                key: key.into(),
                value: value.into(),
            })
            .collect(),
        ..Default::default()
    }
}

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

/// A small node's settings, then `tune`, validated as gabiond validates them.
fn validated(tune: impl FnOnce(&mut Settings)) -> (ValidGossipConfig, ValidStorageConfig) {
    let mut settings = Settings {
        max_cells: 256.try_into().unwrap(),
        rule_dictionary_capacity: 32.try_into().unwrap(),
        node_dictionary_capacity: 32.try_into().unwrap(),
        local_dirty_capacity: 64.try_into().unwrap(),
        forwarded_dirty_capacity: 64.try_into().unwrap(),
        peer_capacity: 16.try_into().unwrap(),
        rng_seed: Some(1),
        ..Settings::default()
    };
    tune(&mut settings);
    settings.validate().expect("test settings validate")
}

fn limits(count: u16, bytes: usize, key: usize) -> CardinalityLimits {
    let (_, storage) = validated(|s| {
        s.max_descriptor_count = count.try_into().unwrap();
        s.max_descriptor_bytes = bytes.try_into().unwrap();
        s.max_key_bytes = key.try_into().unwrap();
    });
    storage.cardinality
}

async fn harness(max_descriptor_bytes: usize, limit: u64) -> Harness {
    harness_with(
        vec![rule_tenant(limit)],
        limits(16, max_descriptor_bytes, 64),
    )
    .await
}

async fn harness_with(rules: Vec<Rule>, cardinality_limits: CardinalityLimits) -> Harness {
    let router = SimRouter::new();
    let addr = next_test_addr();
    let transport = router.bind(addr);
    let identity = NodeIdentity::new(NodeId(0xAA00), 1);
    let (gossip_config, storage) = validated(|_| {});
    let mut store = CellStore::<u32>::new(storage, identity);
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

    let (rt, client) = GossipRuntime::from_parts(
        transport,
        TokioClock::from_millis(0),
        gossip_config,
        store,
        counts.clone(),
    );
    let gossip_handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));

    let rule_table = Arc::new(RuleTable::new(rules));
    let limiter = SharedLimiter::new(rule_table, client.clone(), counts, cardinality_limits);

    Harness {
        limiter,
        gossip_handle,
        client,
    }
}

/// Collects `tracing` output so a test can read the line it asserts on.
#[derive(Clone, Default)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl io::Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Run `body` and return its value with everything it logged. `run_local`
/// polls every task on the calling thread, so the thread-local subscriber
/// sees every event.
fn with_logs<T>(body: impl FnOnce() -> T) -> (T, String) {
    let buffer = LogBuffer::default();
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .finish();
    let value = tracing::subscriber::with_default(subscriber, body);
    let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    (value, logs)
}

/// The one log line carrying `code`.
fn line_with<'a>(logs: &'a str, code: &str) -> &'a str {
    logs.lines()
        .find(|line| line.contains(code))
        .unwrap_or_else(|| panic!("no `{code}` line in {logs:?}"))
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
/// through, and the warning must say so.
#[test]
fn matched_rule_overflow_says_allowed() {
    let (code, logs) = with_logs(|| {
        run_local(async {
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
            let harness = harness_with(rules, limits(16, 512, 128)).await;
            let request = RateLimitRequest {
                domain: "api".into(),
                descriptors: vec![descriptor([("tenant", "a")])],
                hits_addend: 1,
            };
            let response = harness.limiter.should_rate_limit_at(request, 0).await;
            let _ = harness.client.shutdown().await;
            response.overall_code
        })
    });

    assert_eq!(code, rate_limit_response::Code::Ok as i32);
    let line = line_with(&logs, "matched_rule_overflow");
    assert!(line.contains("allowed"), "{line}");
    assert!(line.contains("under-count"), "{line}");
    assert!(line.contains("STORAGE_MAX_MATCHED_RULES"), "{line}");
    assert!(!line.contains("rejected"), "{line}");
    assert!(!line.contains("checked"), "{line}");
}

/// Send one request with `descriptors` through a limiter built with `limits`
/// and return its response with the logs it wrote.
fn cardinality_reject_logs(
    limits: CardinalityLimits,
    descriptors: Vec<RateLimitDescriptor>,
) -> (RateLimitResponse, String) {
    with_logs(|| {
        run_local(async {
            let harness = harness_with(vec![rule_tenant(u64::from(u32::MAX))], limits).await;
            let request = RateLimitRequest {
                domain: "api".into(),
                descriptors,
                hits_addend: 1,
            };
            let response = harness.limiter.should_rate_limit_at(request, 0).await;
            let _ = harness.client.shutdown().await;
            response
        })
    })
}

#[test]
fn cardinality_warning_names_the_exceeded_key() {
    let cases = [
        (
            Setting::StorageMaxDescriptorCount,
            "count",
            limits(1, 512, 64),
            descriptor([("tenant", "a"), ("x", "b")]),
            1,
        ),
        (
            Setting::StorageMaxDescriptorCount,
            "count",
            limits(1, 512, 64),
            descriptor((0..=MAX_DESCRIPTORS).map(|i| (format!("k{i}"), "v"))),
            1,
        ),
        (
            Setting::StorageMaxDescriptorBytes,
            "bytes",
            limits(16, 8, 64),
            descriptor([("tenant", "long-value")]),
            8,
        ),
        (
            Setting::StorageMaxKeyBytes,
            "key_bytes",
            limits(16, 512, 2),
            descriptor([("tenant", "a")]),
            2,
        ),
    ];
    let all = [
        Setting::StorageMaxDescriptorCount,
        Setting::StorageMaxDescriptorBytes,
        Setting::StorageMaxKeyBytes,
    ];
    for (setting, limit, limits, descriptor, max) in cases {
        let (response, logs) = cardinality_reject_logs(limits, vec![descriptor]);
        assert_eq!(
            response.overall_code,
            rate_limit_response::Code::OverLimit as i32
        );
        let line = line_with(&logs, "cardinality_rejected");
        assert!(line.contains(setting.yaml_key()), "{line}");
        assert!(line.contains(setting.env_var()), "{line}");
        assert!(line.contains(&format!("limit={limit:?} ")), "{line}");
        assert!(line.contains(&format!("max={max} ")), "{line}");
        for other in all.into_iter().filter(|other| *other != setting) {
            assert!(!line.contains(other.yaml_key()), "{line}");
        }
    }
}

/// gabiond rejects exactly the requests the library's one cardinality check
/// flags, on both sides of the byte budget.
#[test]
fn one_cardinality_check_for_both_adapters() {
    const BUDGET: usize = 32;
    let limits = limits(16, BUDGET, 64);
    let flagged: Vec<bool> = [BUDGET - 1, BUDGET, BUDGET + 1]
        .into_iter()
        .map(|bytes| {
            let value = "v".repeat(bytes - "api".len() - "tenant".len());
            let entry = Descriptor {
                key: "tenant",
                value: &value,
            };
            let flagged = limits.exceeded("api", &[entry]).is_some();
            let (response, _) =
                cardinality_reject_logs(limits, vec![descriptor([("tenant", value.as_str())])]);
            let rejected = response.overall_code == rate_limit_response::Code::OverLimit as i32;
            assert_eq!(rejected, flagged, "{bytes} bytes");
            flagged
        })
        .collect();
    assert_eq!(flagged, [false, false, true]);
}

#[test]
fn every_cardinality_reject_logs() {
    let defaults = limits(16, 512, 64);
    let too_many_descriptors = (0..=MAX_DESCRIPTORS)
        .map(|i| descriptor([("tenant", format!("t-{i}"))]))
        .collect();
    let too_many_entries = descriptor((0..=MAX_DESCRIPTORS).map(|i| (format!("k{i}"), "v")));
    let too_long = limits(16, 8, 64);
    let cases = [
        (
            "request descriptors",
            defaults,
            too_many_descriptors,
            "request_descriptors",
        ),
        (
            "mapping",
            defaults,
            vec![too_many_entries],
            "storage.max_descriptor_count",
        ),
        (
            "evaluate",
            too_long,
            vec![descriptor([("tenant", "long-value")])],
            "storage.max_descriptor_bytes",
        ),
    ];
    for (site, limits, descriptors, expected) in cases {
        let (response, logs) = cardinality_reject_logs(limits, descriptors);
        let status = response.statuses.last().expect("a status");
        assert_eq!(
            status.code,
            rate_limit_response::Code::OverLimit as i32,
            "{site}"
        );
        let line = line_with(&logs, "cardinality_rejected");
        assert!(line.contains(expected), "{site}: {line}");
    }
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

        // Fill to the limit at now=0. Between the two hits, a request of
        // weight 2 at total 1 is rejected with nothing remaining, not the
        // 1 left before it.
        for tick in 0..2_u64 {
            let r = harness
                .limiter
                .should_rate_limit_at(make_request(), tick)
                .await;
            assert_eq!(r.overall_code, rate_limit_response::Code::Ok as i32);
            if tick == 0 {
                let heavy = RateLimitRequest {
                    hits_addend: 2,
                    ..make_request()
                };
                let r = harness.limiter.should_rate_limit_at(heavy, 0).await;
                let status = r.statuses.first().expect("one status");
                assert_eq!(status.code, rate_limit_response::Code::OverLimit as i32);
                assert_eq!(status.limit_remaining, 0, "remaining must be 0 on reject");
            }
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

/// When two rules match, the status reports the one the client is closest to
/// tripping, and on a tie the one declared first.
#[test]
fn allow_reports_the_tightest_rule_and_the_first_on_a_tie() {
    run_local(async {
        let rule = |id, value: &str, limit, bucket_millis| {
            Rule::new(
                id,
                "api",
                vec![DescriptorPattern {
                    key: "tenant".into(),
                    value: value.into(),
                }],
                limit,
                1_000,
                bucket_millis,
                EnforcementMode::Enforce,
            )
        };
        // The first rule's buckets are 100 ms and the second's 200 ms, so the
        // reset says which rule the status reports.
        for (first, second, expected) in [(10, 5, (4, 200)), (5, 10, (4, 100)), (5, 5, (4, 100))] {
            let rules = vec![rule(1, "*", first, 100), rule(2, "a", second, 200)];
            let harness = harness_with(rules, validated(|_| {}).1.cardinality).await;
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
            let status = response.statuses.first().expect("one status");
            let reset = status.duration_until_reset.as_ref().expect("allow reset");
            let reset_millis = reset.seconds as u64 * 1_000 + reset.nanos as u64 / 1_000_000;
            assert_eq!(
                (status.limit_remaining, reset_millis),
                expected,
                "limits {first} then {second}"
            );
            let _ = harness.client.shutdown().await;
        }
    });
}

/// DryRun rule already past its limit must still emit an OK status with
/// `limit_remaining = 0`, mirroring the nginx adapter's behaviour so the
/// proxy fleet can graph "share that would have been 429'd".
#[test]
fn dry_run_over_limit_populates_descriptor_status_fields() {
    run_local(async {
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
        let harness = harness_with(vec![rule], validated(|_| {}).1.cardinality).await;
        let limiter = &harness.limiter;

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
        let _ = harness.client.shutdown().await;
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
    use tonic::transport::server::TcpIncoming;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("multi-thread runtime");
    let local = LocalSet::new();
    local.block_on(&rt, async {
        let harness = harness(512, 2).await;
        let limiter = harness.limiter.clone();

        // Bind before serving so the client connects on the first try: the
        // listener is already accepting, with no port to race for.
        let incoming =
            TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind gRPC listener");
        let bound = incoming.local_addr().expect("local_addr");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(RateLimitServiceServer::new(EnvoyRateLimitService::new(
                    limiter,
                )))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                }),
        );
        let mut client = RateLimitServiceClient::connect(format!("http://{bound}"))
            .await
            .expect("client connected");

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

        // The service stamps requests with the wall clock, so make the
        // OverLimit case independent of it: hits above the limit (2) are
        // rejected whatever the window already holds.
        let over = client
            .should_rate_limit(RateLimitRequest {
                domain: "api".into(),
                descriptors: vec![descriptor([("tenant", "t-grpc")])],
                hits_addend: 3,
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
