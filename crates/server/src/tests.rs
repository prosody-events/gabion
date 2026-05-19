
use super::*;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;

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

#[derive(Clone, Debug)]
struct EnvoyAllOrNothingCase {
    filled_tenant: u8,
    allowed_tenant: u8,
    hits: u8,
}

impl Arbitrary for EnvoyAllOrNothingCase {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            filled_tenant: u8::arbitrary(g) % 16,
            allowed_tenant: u8::arbitrary(g) % 16,
            hits: (u8::arbitrary(g) % 3).max(1),
        }
    }
}

fn runtime(max_descriptor_bytes: usize) -> SharedLimiter {
    Runtime::with_count_update_handler(
        gabion::Config {
            storage: gabion::StorageConfig {
                max_keys: 32,
                max_cells: None,
                dirty_ring_entries: None,
                max_descriptor_count: 16,
                max_descriptor_bytes,
                max_key_bytes: 64,
                max_active_buckets: 64,
            },
            limits: vec![gabion::LimitRuleConfig {
                name: "tenant".to_string(),
                domain: "api".to_string(),
                descriptors: vec![gabion::DescriptorConfig {
                    key: "tenant".to_string(),
                    value: "*".to_string(),
                }],
                limit: 10,
                window: "1s".to_string(),
                bucket: "100ms".to_string(),
                local_fallback_limit: 3,
                local_absolute_limit: 6,
                stale_after: "500ms".to_string(),
                safety_margin: gabion::SafetyMarginConfig { hits: 0 },
                overflow_policy: gabion::OverflowPolicy::UseOverflowKey,
                mode: gabion::EnforcementMode::Enforce,
            }],
            runtime: gabion::RuntimeConfig::default(),
            server: gabion::ServerConfig::default(),
            discovery: gabion::DiscoveryConfig::default(),
            gossip: gabion::GossipConfig::default(),
        },
        NoOpCountUpdateHandler,
    )
    .expect("runtime")
}

#[test]
fn maps_descriptors_and_returns_ok_or_over_limit() {
    let service = EnvoyRateLimitService::new(runtime(512));
    let request = RateLimitRequest {
        domain: "api".to_string(),
        descriptors: vec![descriptor([("tenant", "a")])],
        hits_addend: 2,
    };

    let first = service.should_rate_limit_at(request.clone(), 0);
    let second = service.should_rate_limit_at(request, 1);

    assert_eq!(first.overall_code, rate_limit_response::Code::Ok as i32);
    assert_eq!(
        second.overall_code,
        rate_limit_response::Code::OverLimit as i32
    );
    assert_eq!(second.statuses.len(), 1);
}

#[test]
fn multiple_descriptors_are_all_or_nothing() {
    let service = EnvoyRateLimitService::new(runtime(512));

    let fill_b = RateLimitRequest {
        domain: "api".to_string(),
        descriptors: vec![descriptor([("tenant", "b")])],
        hits_addend: 3,
    };
    assert_eq!(
        service.should_rate_limit_at(fill_b, 0).overall_code,
        rate_limit_response::Code::Ok as i32
    );

    let mixed = RateLimitRequest {
        domain: "api".to_string(),
        descriptors: vec![descriptor([("tenant", "a")]), descriptor([("tenant", "b")])],
        hits_addend: 1,
    };
    assert_eq!(
        service.should_rate_limit_at(mixed, 1).overall_code,
        rate_limit_response::Code::OverLimit as i32
    );

    let a_only = RateLimitRequest {
        domain: "api".to_string(),
        descriptors: vec![descriptor([("tenant", "a")])],
        hits_addend: 3,
    };
    assert_eq!(
        service.should_rate_limit_at(a_only, 2).overall_code,
        rate_limit_response::Code::Ok as i32
    );
}

#[test]
fn rejects_requests_over_cardinality_limits_before_recording() {
    let service = EnvoyRateLimitService::new(runtime(8));
    let request = RateLimitRequest {
        domain: "api".to_string(),
        descriptors: vec![descriptor([("tenant", "long-value")])],
        hits_addend: 1,
    };

    let response = service.should_rate_limit_at(request, 0);

    assert_eq!(
        response.overall_code,
        rate_limit_response::Code::OverLimit as i32
    );
}

#[quickcheck]
fn quickcheck_multi_descriptor_requests_are_all_or_nothing(
    case: EnvoyAllOrNothingCase,
) -> TestResult {
    let filled = format!("tenant-{}", case.filled_tenant);
    let allowed = format!(
        "tenant-{}",
        case.allowed_tenant
            .saturating_add(1)
            .saturating_add(case.filled_tenant)
    );
    let service = EnvoyRateLimitService::new(runtime(512));

    let fill = RateLimitRequest {
        domain: "api".to_string(),
        descriptors: vec![descriptor([("tenant", filled.as_str())])],
        hits_addend: 3,
    };
    if service.should_rate_limit_at(fill, 0).overall_code != rate_limit_response::Code::Ok as i32 {
        return TestResult::error("failed to seed filled descriptor");
    }

    let mixed = RateLimitRequest {
        domain: "api".to_string(),
        descriptors: vec![
            descriptor([("tenant", allowed.as_str())]),
            descriptor([("tenant", filled.as_str())]),
        ],
        hits_addend: u32::from(case.hits),
    };
    let response = service.should_rate_limit_at(mixed, 1);

    if response.overall_code != rate_limit_response::Code::OverLimit as i32 {
        return TestResult::error("mixed request with rejected descriptor did not reject");
    }
    TestResult::passed()
}

#[quickcheck]
fn quickcheck_poisoned_limiter_lock_fails_open(case: EnvoyRequestCase) -> TestResult {
    let service = EnvoyRateLimitService::new(runtime(512));
    service.limiter.shutdown();

    let descriptors = case
        .tenants
        .iter()
        .map(|tenant| descriptor([("tenant", format!("tenant-{tenant}"))]))
        .collect::<Vec<_>>();
    let request = RateLimitRequest {
        domain: "api".to_string(),
        descriptors,
        hits_addend: u32::from(case.hits),
    };
    let expected_len = request.descriptors.len();
    let response = service.should_rate_limit_at(request, 0);

    if response.overall_code != rate_limit_response::Code::Ok as i32 {
        return TestResult::error("poisoned limiter did not fail open");
    }
    if response.statuses.len() != expected_len
        || response
            .statuses
            .iter()
            .any(|status| status.code != rate_limit_response::Code::Ok as i32)
    {
        return TestResult::error("poisoned limiter response shape or status was wrong");
    }
    TestResult::passed()
}

#[quickcheck]
fn quickcheck_response_shape_matches_descriptor_shape(case: EnvoyRequestCase) -> TestResult {
    let service = EnvoyRateLimitService::new(runtime(512));
    let descriptors = case
        .tenants
        .iter()
        .map(|tenant| descriptor([("tenant", format!("tenant-{tenant}"))]))
        .collect::<Vec<_>>();
    let request = RateLimitRequest {
        domain: "api".to_string(),
        descriptors,
        hits_addend: u32::from(case.hits),
    };

    let response = service.should_rate_limit_at(request.clone(), 0);
    let any_over_limit = response
        .statuses
        .iter()
        .any(|status| status.code == rate_limit_response::Code::OverLimit as i32);

    if response.statuses.len() != request.descriptors.len() {
        return TestResult::error("Envoy response status length did not match descriptor count");
    }
    if (response.overall_code == rate_limit_response::Code::OverLimit as i32) != any_over_limit {
        return TestResult::error("Envoy overall response code did not summarize statuses");
    }
    TestResult::passed()
}

#[quickcheck]
fn quickcheck_cardinality_rejects_before_recording(case: EnvoyRequestCase) -> TestResult {
    if case.tenants.is_empty() {
        return TestResult::discard();
    }

    let service = EnvoyRateLimitService::new(runtime(8));
    let descriptors = case
        .tenants
        .iter()
        .map(|tenant| descriptor([("tenant", format!("tenant-{tenant}-too-long"))]))
        .collect::<Vec<_>>();
    let request = RateLimitRequest {
        domain: "api".to_string(),
        descriptors,
        hits_addend: u32::from(case.hits),
    };
    let descriptor_count = request.descriptors.len();
    let response = service.should_rate_limit_at(request, 0);

    if response.statuses.len() != descriptor_count {
        return TestResult::error(
            "cardinality rejection response length did not match descriptor count",
        );
    }
    if response.overall_code != rate_limit_response::Code::OverLimit as i32 {
        return TestResult::error("cardinality violation did not reject");
    }
    TestResult::passed()
}
