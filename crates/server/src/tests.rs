//! gRPC adapter tests. Driven through a sim-routed gossip runtime so the
//! whole record-then-read flow exercises real CRDT + AggregateStore code.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use tokio::task::LocalSet;

use gabion::crdt::{CellStore, KeyHash, NodeId, NodeIdentity, ValidStorageConfig};
use gabion::gossip::sim::SimRouter;
use gabion::gossip::{GossipRuntime, TokioClock, ValidGossipConfig};
use gabion::rules::{CardinalityLimits, Rule, RuleDef};
use gabion::settings::{Setting, Settings};

use super::*;

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

impl Harness {
    /// Admit `request` at `now_millis`, then wait until the runtime has
    /// applied the hits it recorded.
    async fn check(&self, request: RateLimitRequest, now_millis: u64) -> RateLimitResponse {
        let response = self.limiter.should_rate_limit_at(&request, now_millis);
        self.settle().await;
        response
    }

    /// Wait until the runtime has taken every queued hit. It releases a slot
    /// as it takes a hit and applies the hit before it next yields, so once
    /// every slot is free every recorded hit is in the aggregate.
    async fn settle(&self) {
        drop(self.client.reserve(self.client.capacity()).await);
    }
}

/// A rule on `api` built as gabiond builds its rules: `rate` over its
/// period, in buckets of `bucket_millis`.
fn rule(
    name: &str,
    patterns: &[(&str, &str)],
    rate: &str,
    bucket_millis: u64,
    mode: EnforcementMode,
) -> Rule {
    RuleDef {
        name,
        domain: "api",
        patterns,
        rate,
        window: None,
        bucket: Some(std::time::Duration::from_millis(bucket_millis)),
        mode,
    }
    .build(&validated(|_| {}).1.cardinality)
    .unwrap()
}

/// `tenant:*` allowing `limit` hits a second, in 100 ms buckets.
fn rule_tenant(limit: u64) -> Rule {
    rule(
        "tenant",
        &[("tenant", "*")],
        &format!("{limit}r/s"),
        100,
        EnforcementMode::Enforce,
    )
}

/// A small node's settings, then `tune`, validated as gabiond validates them.
fn validated(tune: impl FnOnce(&mut Settings)) -> (ValidGossipConfig, ValidStorageConfig) {
    let mut settings = Settings {
        max_cells: 256.try_into().unwrap(),
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

/// The cardinality limits of `count` entries, `bytes` and `key` bytes.
fn limits(count: u16, bytes: usize, key: usize) -> impl Fn(&mut Settings) + Copy {
    move |s| {
        s.max_descriptor_count = count.try_into().unwrap();
        s.max_descriptor_bytes = bytes.try_into().unwrap();
        s.max_key_bytes = key.try_into().unwrap();
    }
}

fn cardinality(tune: impl FnOnce(&mut Settings)) -> CardinalityLimits {
    validated(tune).1.cardinality
}

async fn harness(max_descriptor_bytes: usize, limit: u64) -> Harness {
    harness_with(
        vec![rule_tenant(limit)],
        limits(16, max_descriptor_bytes, 64),
    )
    .await
}

/// A limiter over `rules` with a running runtime, its settings tuned by
/// `tune`.
async fn harness_with(rules: Vec<Rule>, tune: impl FnOnce(&mut Settings)) -> Harness {
    let router = SimRouter::new();
    let addr = next_test_addr();
    let transport = router.bind(addr);
    let identity = NodeIdentity::new(NodeId(0xAA00), 1);
    let (gossip_config, storage) = validated(tune);
    let rule_table = Arc::new(
        RuleTable::new(rules.into_boxed_slice(), storage.rule_dictionary_capacity()).unwrap(),
    );
    let store = CellStore::<u32>::new(storage, identity, &rule_table);
    let counts = Arc::new(Aggregate::new(&storage));

    let (rt, client) = GossipRuntime::from_parts(
        transport,
        TokioClock::from_millis(0),
        gossip_config,
        store,
        counts.clone(),
    );
    let gossip_handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));
    let limiter = SharedLimiter::new(rule_table, client.clone(), counts, storage);

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

        let first = harness.check(request.clone(), 0).await;
        let second = harness.check(request, 1).await;

        assert_eq!(first.overall_code, rate_limit_response::Code::Ok as i32);
        // The second request pushes the bucket from 2 to 4 — still under the
        // 10-hit limit.
        assert_eq!(second.overall_code, rate_limit_response::Code::Ok as i32);
        assert_eq!(second.statuses.len(), 1);
        // Both requests, at 0 ms and 1 ms, count in the 100 ms bucket 0.
        let key = hash_key(
            "api",
            [Descriptor {
                key: "tenant",
                value: "a",
            }],
        );
        let fingerprint = harness.limiter.rule_table.rules()[0].spec().fingerprint;
        let cell = harness.limiter.counts.table().get(fingerprint, key.0, 0);
        assert_eq!(cell.map(|cell| cell.count), Some(4));

        // Pump until we exceed the limit.
        for tick in 2..10 {
            let request = RateLimitRequest {
                domain: "api".into(),
                descriptors: vec![descriptor([("tenant", "a")])],
                hits_addend: 2,
            };
            harness.check(request, tick as u64).await;
        }
        let over = RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "a")])],
            hits_addend: 2,
        };
        let response = harness.check(over, 11).await;
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
            harness.check(fill, 0).await.overall_code,
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
                harness.check(rejected, tick * 100).await.overall_code,
                rate_limit_response::Code::OverLimit as i32
            );
        }

        let recovered = RateLimitRequest {
            domain: "api".into(),
            descriptors: vec![descriptor([("tenant", "a")])],
            hits_addend: 1,
        };
        assert_eq!(
            harness.check(recovered, 1_000).await.overall_code,
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

        let response = harness.check(request, 0).await;

        assert_eq!(
            response.overall_code,
            rate_limit_response::Code::OverLimit as i32
        );
        let _ = harness.client.shutdown().await;
    });
}

/// `count` rules on `tenant`, each allowing 100 hits over its own window of
/// 1, 2, … seconds, so no two share an identity.
fn tenant_rules(count: usize) -> Vec<Rule> {
    (1..=count)
        .map(|i| {
            rule(
                &format!("r{i}"),
                &[("tenant", "*")],
                &format!("100r/{i}s"),
                100,
                EnforcementMode::Enforce,
            )
        })
        .collect()
}

fn tenant_request(tenant: &str) -> RateLimitRequest {
    RateLimitRequest {
        domain: "api".into(),
        descriptors: vec![descriptor([("tenant", tenant)])],
        hits_addend: 1,
    }
}

/// A request matching more rules than `storage.max_matched_rules` counts
/// against exactly that many and is allowed, per allow-by-default
/// (CLAUDE.md): the bound is gabion's own, not a client-facing reject, and
/// the warning says so and names the setting.
#[test]
fn matched_rule_bound_is_the_setting() {
    let (totals, logs) = with_logs(|| {
        run_local(async {
            let harness = harness_with(tenant_rules(21), |s| {
                s.max_matched_rules = 20.try_into().unwrap();
            })
            .await;
            let response = harness.check(tenant_request("a"), 0).await;
            assert_eq!(response.overall_code, rate_limit_response::Code::Ok as i32);
            let key = hash_key(
                "api",
                [Descriptor {
                    key: "tenant",
                    value: "a",
                }],
            );
            let table = harness.limiter.counts.table();
            let totals: Vec<u64> = harness
                .limiter
                .rule_table
                .rules()
                .iter()
                .map(|rule| table.window_total(rule.spec(), key, 0))
                .collect();
            let _ = harness.client.shutdown().await;
            totals
        })
    });
    assert_eq!(totals, [vec![1; 20], vec![0]].concat());
    let line = line_with(&logs, "matched_rule_overflow");
    for part in [
        "allowed",
        "under-count",
        "config_key=\"storage.max_matched_rules\"",
        "GABION_STORAGE_MAX_MATCHED_RULES",
        "cap=20 ",
    ] {
        assert!(line.contains(part), "{part}: {line}");
    }
    assert!(!line.contains("rejected"), "{line}");
}

/// Rules past `storage.max_matched_rules` are still checked, as in NGINX; they
/// only go uncounted, so a third rule over its limit rejects at a bound of 1.
#[test]
fn rules_past_the_matched_bound_still_reject() {
    run_local(async {
        let rules = [("a", "1000r/s"), ("b", "1000r/2s"), ("c", "1r/3s")].map(|(name, rate)| {
            rule(
                name,
                &[("tenant", "*")],
                rate,
                100,
                EnforcementMode::Enforce,
            )
        });
        let harness = harness_with(rules.into(), |s| {
            s.max_matched_rules = 1.try_into().unwrap();
        })
        .await;
        let request = RateLimitRequest {
            hits_addend: 2,
            ..tenant_request("a")
        };
        let response = harness.check(request, 0).await;
        assert_eq!(
            response.overall_code,
            rate_limit_response::Code::OverLimit as i32
        );
        let _ = harness.client.shutdown().await;
    });
}

/// A full gossip request queue never makes a request wait or fail: the hits
/// that do not fit are dropped and counted, and the warning logs at powers
/// of two.
#[test]
fn full_request_queue_allows_and_counts() {
    let ((code, dropped), logs) = with_logs(|| {
        run_local(async {
            let (gossip, storage) = validated(|s| {
                s.limit_queue_capacity = gabion::settings::MIN_LIMIT_QUEUE_CAPACITY;
            });
            let table = Arc::new(RuleTable::new(tenant_rules(6).into(), None).unwrap());
            let counts = Arc::new(Aggregate::new(&storage));
            // Built and never run, so nothing drains its queue.
            let (_runtime, client) = GossipRuntime::from_parts(
                SimRouter::new().bind(next_test_addr()),
                TokioClock::from_millis(0),
                gossip,
                CellStore::<u32>::new(storage, NodeIdentity::new(NodeId(1), 1), &table),
                counts.clone(),
            );
            for _ in 0..62 {
                client.try_record(1, KeyHash(1), 0, 1, 1, 0).unwrap();
            }
            let limiter = SharedLimiter::new(table, client, counts, storage);
            let response = limiter.should_rate_limit_at(&tenant_request("a"), 0);
            let dropped = limiter.warnings.request_queue_full.load(Ordering::Relaxed);
            (response.overall_code, dropped)
        })
    });
    assert_eq!(code, rate_limit_response::Code::Ok as i32);
    assert_eq!(dropped, 4);
    let lines: Vec<&str> = logs
        .lines()
        .filter(|line| line.contains("code=\"request_queue_full\""))
        .collect();
    assert_eq!(lines.len(), 3, "{logs}");
    for line in lines {
        assert!(line.contains("gossip.limit_queue_capacity"), "{line}");
    }
}

/// Send one request with `descriptors` through a limiter built with `limits`
/// and return its response with the logs it wrote.
fn cardinality_reject_logs(
    limits: impl FnOnce(&mut Settings),
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
            let response = harness.check(request, 0).await;
            let _ = harness.client.shutdown().await;
            response
        })
    })
}

/// Each cardinality limit's reject names its own setting and no other's.
#[test]
fn cardinality_warning_names_the_exceeded_key() {
    let cases = [
        (
            Setting::StorageMaxRequestDescriptors,
            "request_descriptors",
            limits(16, 512, 64),
            (0..=16)
                .map(|i| descriptor([("tenant", format!("t-{i}"))]))
                .collect(),
            16,
        ),
        (
            Setting::StorageMaxDescriptorCount,
            "count",
            limits(1, 512, 64),
            vec![descriptor([("tenant", "a"), ("x", "b")])],
            1,
        ),
        (
            Setting::StorageMaxDescriptorBytes,
            "bytes",
            limits(16, 8, 64),
            vec![descriptor([("tenant", "long-value")])],
            8,
        ),
        (
            Setting::StorageMaxKeyBytes,
            "key_bytes",
            limits(16, 512, 2),
            vec![descriptor([("tenant", "a")])],
            2,
        ),
    ];
    let all = cases.each_ref().map(|case| case.0);
    for (setting, limit, limits, descriptors, max) in cases {
        let (response, logs) = cardinality_reject_logs(limits, descriptors);
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
    let flags = cardinality(limits);
    let flagged: Vec<bool> = [BUDGET - 1, BUDGET, BUDGET + 1]
        .into_iter()
        .map(|bytes| {
            let value = "v".repeat(bytes - "api".len() - "tenant".len());
            let entry = Descriptor {
                key: "tenant",
                value: &value,
            };
            let flagged = flags.exceeded("api", [entry].into_iter()).is_some();
            let (response, _) =
                cardinality_reject_logs(limits, vec![descriptor([("tenant", value.as_str())])]);
            let rejected = response.overall_code == rate_limit_response::Code::OverLimit as i32;
            assert_eq!(rejected, flagged, "{bytes} bytes");
            flagged
        })
        .collect();
    assert_eq!(flagged, [false, false, true]);
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

        let response = harness.check(request, 0).await;
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
/// down, await its exit, then issue another request — `try_record()` returns
/// `ShutDown` and `note_gossip_record_failure` swallows it.
#[test]
fn gossip_record_failure_does_not_reject_request() {
    run_local(async {
        let harness = harness(512, 100).await;

        // Bring the runtime down and wait for it to actually exit so the
        // next `try_record()` is guaranteed to hit the closed channel.
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
        let response = harness.limiter.should_rate_limit_at(&request, 0);
        assert_eq!(
            response.overall_code,
            rate_limit_response::Code::Ok as i32,
            "a dead gossip runtime must not reject — see CLAUDE.md (allow-by-default)",
        );
        let warnings = &harness.limiter.warnings;
        assert_eq!(
            (
                warnings.gossip_record_failures.load(Ordering::Relaxed),
                warnings.request_queue_full.load(Ordering::Relaxed)
            ),
            (1, 0)
        );
    });
}

/// `storage.max_request_descriptors` decides how many of an Envoy request's
/// descriptors are evaluated; the first one past it is rejected and logged.
#[test]
fn request_descriptor_cap_is_the_setting() {
    let descriptors = (0..21)
        .map(|i| descriptor([("tenant", format!("t-{i}"))]))
        .collect();
    let (response, logs) = cardinality_reject_logs(
        |s| s.max_request_descriptors = 20.try_into().unwrap(),
        descriptors,
    );
    let codes: Vec<i32> = response.statuses.iter().map(|status| status.code).collect();
    let ok = rate_limit_response::Code::Ok as i32;
    let over = rate_limit_response::Code::OverLimit as i32;
    assert_eq!(codes, [vec![ok; 20], vec![over]].concat());
    let line = line_with(&logs, "cardinality_rejected");
    for part in [
        "limit=\"request_descriptors\"",
        "max=20 ",
        "storage.max_request_descriptors",
    ] {
        assert!(line.contains(part), "{part}: {line}");
    }
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
            let r = harness.check(make_request(), tick).await;
            assert_eq!(r.overall_code, rate_limit_response::Code::Ok as i32);
            if tick == 0 {
                let heavy = RateLimitRequest {
                    hits_addend: 2,
                    ..make_request()
                };
                let r = harness.check(heavy, 0).await;
                let status = r.statuses.first().expect("one status");
                assert_eq!(status.code, rate_limit_response::Code::OverLimit as i32);
                assert_eq!(status.limit_remaining, 0, "remaining must be 0 on reject");
            }
        }

        // 3rd request at now=0 trips the reject. With both prior hits
        // in bucket 0 (oldest visible), need=1 is satisfied as soon as
        // bucket 0 falls off — fall_off = (0+10)*100 = 1000ms, delta
        // from now=0 is 1000ms.
        let over = harness.check(make_request(), 0).await;
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
            .check(
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
        let rule = |name, value, limit: u64, bucket_millis| {
            rule(
                name,
                &[("tenant", value)],
                &format!("{limit}r/s"),
                bucket_millis,
                EnforcementMode::Enforce,
            )
        };
        // The first rule's buckets are 100 ms and the second's 200 ms, so the
        // reset says which rule the status reports.
        for (first, second, expected) in [(10, 5, (4, 200)), (5, 10, (4, 100)), (5, 5, (4, 100))] {
            let rules = vec![rule("one", "*", first, 100), rule("two", "a", second, 200)];
            let harness = harness_with(rules, |_| {}).await;
            let response = harness
                .check(
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
        let rule = rule(
            "tenant",
            &[("tenant", "*")],
            "2r/s",
            100,
            EnforcementMode::DryRun,
        );
        let harness = harness_with(vec![rule], |_| {}).await;

        // Drive enough hits to push past the DryRun budget. Each
        // request is allowed because the rule never rejects.
        for tick in 0..5_u64 {
            let r = harness
                .check(
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
        let over = harness
            .check(
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

/// Pre-admission rejects (cardinality, `max_request_descriptors`) carry no
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
            .check(
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
