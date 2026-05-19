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

#[cfg(test)]
mod tests;

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
