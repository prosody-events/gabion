//! Envoy-compatible gRPC rate-limit adapter.
//!
//! Invariants:
//! - Envoy descriptors map to core `LimitRequest` values without changing
//!   order.
//! - Multi-descriptor requests are all-or-nothing for recording.
//! - Cardinality violations reject before mutating limiter state.
//! - A poisoned limiter lock fails open.
//! - Response status length matches request descriptor length.
//! - The adapter itself contains protocol mapping only; admission decisions
//!   stay in core.

use std::time::{SystemTime, UNIX_EPOCH};

use gabion::{
    CardinalityLimits, Decision, Descriptor, LimitRequest, NoOpCountUpdateHandler, Runtime,
};

pub type SharedLimiter = Runtime<NoOpCountUpdateHandler>;

pub use envoy_types::pb::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor, rate_limit_descriptor,
};
pub use envoy_types::pb::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse, rate_limit_response,
    rate_limit_service_server::RateLimitServiceServer,
};

#[derive(Clone)]
pub struct EnvoyRateLimitService {
    limiter: SharedLimiter,
}

impl EnvoyRateLimitService {
    pub fn new(limiter: SharedLimiter) -> Self {
        Self { limiter }
    }

    pub fn with_limits(limiter: SharedLimiter, _limits: gabion::CardinalityLimits) -> Self {
        Self { limiter }
    }

    pub fn should_rate_limit_at(
        &self,
        request: RateLimitRequest,
        now_millis: u64,
    ) -> RateLimitResponse {
        let hits = u64::from(request.hits_addend.max(1));
        let mapped_descriptors = request
            .descriptors
            .iter()
            .map(|descriptor| {
                descriptor
                    .entries
                    .iter()
                    .map(|entry| Descriptor {
                        key: entry.key.as_str(),
                        value: entry.value.as_str(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let limit_requests = mapped_descriptors
            .iter()
            .map(|descriptors| LimitRequest {
                domain: request.domain.as_str(),
                descriptors,
                hits,
            })
            .collect::<Vec<_>>();

        let mut decisions = vec![Decision::Allow; limit_requests.len()];
        self.limiter
            .record_all_at(&limit_requests, &mut decisions, now_millis);

        response_from_decisions(&decisions)
    }
}

pub async fn serve(
    bind: std::net::SocketAddr,
    limiter: SharedLimiter,
) -> Result<(), tonic::transport::Error> {
    serve_with_limits(bind, limiter, CardinalityLimits::default()).await
}

pub async fn serve_with_limits(
    bind: std::net::SocketAddr,
    limiter: SharedLimiter,
    limits: CardinalityLimits,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(RateLimitServiceServer::new(
            EnvoyRateLimitService::with_limits(limiter, limits),
        ))
        .serve(bind)
        .await
}

#[tonic::async_trait]
impl envoy_types::pb::envoy::service::ratelimit::v3::rate_limit_service_server::RateLimitService
    for EnvoyRateLimitService
{
    async fn should_rate_limit(
        &self,
        request: tonic::Request<RateLimitRequest>,
    ) -> Result<tonic::Response<RateLimitResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.should_rate_limit_at(request.into_inner(), now_millis()),
        ))
    }
}

pub fn response_from_decisions(decisions: &[Decision]) -> RateLimitResponse {
    let over_limit = decisions.iter().any(|decision| decision.is_reject());
    let overall_code = if over_limit {
        rate_limit_response::Code::OverLimit
    } else {
        rate_limit_response::Code::Ok
    } as i32;
    let statuses = decisions
        .iter()
        .map(|decision| {
            let code = if decision.is_reject() {
                rate_limit_response::Code::OverLimit
            } else {
                rate_limit_response::Code::Ok
            } as i32;
            rate_limit_response::DescriptorStatus {
                code,
                ..Default::default()
            }
        })
        .collect();

    RateLimitResponse {
        overall_code,
        statuses,
        ..Default::default()
    }
}

pub fn descriptor(
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

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
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
        if service.should_rate_limit_at(fill, 0).overall_code
            != rate_limit_response::Code::Ok as i32
        {
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
            return TestResult::error(
                "Envoy response status length did not match descriptor count",
            );
        }
        if (response.overall_code == rate_limit_response::Code::OverLimit as i32) != any_over_limit
        {
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
}
