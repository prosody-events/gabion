//! Envoy-compatible gRPC rate-limit adapter built on the gossip CRDT.
//!
//! Architectural seams:
//! - [`admission`] — admission-time types (Decision, RejectReason, ...).
//! - [`store`] — `DashMapStore<C>`: the gossip aggregate target + the read
//!   surface used to evaluate window totals.
//! - [`config`] — YAML config (server-only; the library knows nothing
//!   about YAML).
//! - [`identity`] — `NodeIdentity` derivation at startup.
//! - [`admin`] — a single admin HTTP endpoint backed by the gossip
//!   `AdminCommand` channel.
//! - This module — `SharedLimiter`, the gRPC service trait impl, and Envoy
//!   descriptor mapping.
//!
//! Hit semantics are **record-then-read**: every matching `(rule, key)` pair
//! records its hits into the gossip runtime before the limit is evaluated,
//! so rejected requests still credit the bucket ("penalty rate"). Multi-
//! descriptor requests are no longer all-or-nothing for admission.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use gabion::crdt::Count;
use gabion::gossip::GossipClient;
use gabion::rules::{Descriptor, Rule, RuleId, RuleTable, hash_key};

use crate::admission::{CardinalityLimits, Decision, LimitRequest, RejectReason};
use crate::store::DashMapStore;

pub mod admission;
pub mod admin;
pub mod config;
pub mod identity;
pub mod store;

#[cfg(test)]
mod tests;

pub use envoy_types::pb::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor, rate_limit_descriptor,
};
pub use envoy_types::pb::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse, rate_limit_response,
    rate_limit_service_server::RateLimitServiceServer,
};

/// Precomputed hot-path data for one rule. Built once at startup so the
/// per-request path doesn't re-derive bucket arithmetic or look up the
/// fingerprint.
#[derive(Clone, Copy, Debug)]
struct RuleSpec {
    id: RuleId,
    fingerprint: u128,
    limit: u64,
    bucket_millis: u64,
    live_buckets: u32,
}

/// Adapter state shared between the gRPC service and admin endpoints.
///
/// Cheap to clone (`Arc` + `Clone` handles). The gossip runtime that backs
/// `gossip_client` is owned by `gabiond` and joined separately.
#[derive(Clone)]
pub struct SharedLimiter<C: Count = u32> {
    rule_table: Arc<RuleTable>,
    rule_specs: Arc<[RuleSpec]>,
    gossip_client: GossipClient<C>,
    counts: Arc<DashMapStore<C>>,
    cardinality_limits: CardinalityLimits,
}

impl<C: Count> SharedLimiter<C> {
    pub fn new(
        rule_table: Arc<RuleTable>,
        gossip_client: GossipClient<C>,
        counts: Arc<DashMapStore<C>>,
        cardinality_limits: CardinalityLimits,
    ) -> Self {
        let rule_specs: Arc<[RuleSpec]> = rule_table
            .iter()
            .map(rule_spec)
            .collect::<Vec<_>>()
            .into();
        Self {
            rule_table,
            rule_specs,
            gossip_client,
            counts,
            cardinality_limits,
        }
    }

    pub fn rule_table(&self) -> &Arc<RuleTable> {
        &self.rule_table
    }

    pub fn counts(&self) -> &Arc<DashMapStore<C>> {
        &self.counts
    }

    fn spec_for(&self, id: RuleId) -> &RuleSpec {
        self.rule_specs
            .iter()
            .find(|s| s.id == id)
            .expect("rule spec missing for an id that came from rule_table")
    }

    /// Evaluate one Envoy rate-limit request at the given wall-clock time.
    /// Each descriptor is admitted independently — rejected descriptors do
    /// not gate the others.
    pub async fn should_rate_limit_at(
        &self,
        request: RateLimitRequest,
        now_millis: u64,
    ) -> RateLimitResponse {
        let hits = u64::from(request.hits_addend.max(1));
        let mut decisions = Vec::with_capacity(request.descriptors.len());

        for envoy_descriptor in &request.descriptors {
            let mapped: Vec<Descriptor<'_>> = envoy_descriptor
                .entries
                .iter()
                .map(|entry| Descriptor {
                    key: entry.key.as_str(),
                    value: entry.value.as_str(),
                })
                .collect();
            let core_request = LimitRequest {
                domain: request.domain.as_str(),
                descriptors: &mapped,
                hits,
            };
            decisions.push(self.evaluate(core_request, now_millis).await);
        }

        response_from_decisions(&decisions)
    }

    async fn evaluate(&self, request: LimitRequest<'_>, now_millis: u64) -> Decision {
        if request.violates_cardinality(self.cardinality_limits) {
            return Decision::Reject(RejectReason::Cardinality);
        }

        let mut decision = Decision::Allow;
        for rule in self
            .rule_table
            .matching(request.domain, request.descriptors)
        {
            let spec = self.spec_for(rule.id);
            let key_hash = hash_key(rule.id, request.domain, request.descriptors);
            let bucket = (now_millis / spec.bucket_millis) as u32;

            // Record-then-read. If the gossip runtime has gone away the
            // record fails open — we still consult whatever the local
            // DashMap holds.
            let _ = self
                .gossip_client
                .record(spec.fingerprint, key_hash, bucket, request.hits, now_millis)
                .await;

            let total = self.counts.window_total(
                spec.fingerprint,
                key_hash,
                now_millis,
                spec.bucket_millis,
                spec.live_buckets,
            );
            if total > spec.limit {
                decision = Decision::Reject(RejectReason::GlobalLimit);
                // Continue iterating so every matching rule's bucket is
                // credited under record-then-read.
            }
        }
        decision
    }
}

fn rule_spec(rule: &Rule) -> RuleSpec {
    RuleSpec {
        id: rule.id,
        fingerprint: rule.fingerprint,
        limit: rule.limit,
        bucket_millis: rule.bucket_millis,
        live_buckets: rule.live_buckets(),
    }
}

/// Tonic-mounted gRPC service.
#[derive(Clone)]
pub struct EnvoyRateLimitService<C: Count = u32> {
    limiter: SharedLimiter<C>,
}

impl<C: Count> EnvoyRateLimitService<C> {
    pub fn new(limiter: SharedLimiter<C>) -> Self {
        Self { limiter }
    }
}

pub async fn serve<C>(
    bind: std::net::SocketAddr,
    limiter: SharedLimiter<C>,
) -> Result<(), tonic::transport::Error>
where
    C: Count + Send + Sync + 'static,
{
    tonic::transport::Server::builder()
        .add_service(RateLimitServiceServer::new(EnvoyRateLimitService::new(
            limiter,
        )))
        .serve(bind)
        .await
}

#[tonic::async_trait]
impl<C> envoy_types::pb::envoy::service::ratelimit::v3::rate_limit_service_server::RateLimitService
    for EnvoyRateLimitService<C>
where
    C: Count + Send + Sync + 'static,
{
    async fn should_rate_limit(
        &self,
        request: tonic::Request<RateLimitRequest>,
    ) -> Result<tonic::Response<RateLimitResponse>, tonic::Status> {
        let response = self
            .limiter
            .should_rate_limit_at(request.into_inner(), now_millis())
            .await;
        Ok(tonic::Response::new(response))
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
