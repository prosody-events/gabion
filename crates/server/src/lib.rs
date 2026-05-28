//! Envoy-compatible gRPC rate-limit adapter built on the gossip CRDT.
//!
//! Architectural seams:
//! - [`admission`] — admission-time types (Decision, RejectReason, ...).
//! - [`store`] — `DashMapStore<C>`: the gossip aggregate target + the read
//!   surface used to evaluate window totals.
//! - [`config`] — YAML config (server-only; the library knows nothing about
//!   YAML).
//! - [`identity`] — `NodeIdentity` derivation at startup.
//! - [`admin`] — a single admin HTTP endpoint backed by the gossip
//!   `AdminCommand` channel.
//! - This module — `SharedLimiter`, the gRPC service trait impl, and Envoy
//!   descriptor mapping.
//!
//! Hit semantics are **read-then-record**: each descriptor is evaluated against
//! the current aggregate window, and only allowed descriptors record hits into
//! the gossip runtime. Multi-descriptor requests are not all-or-nothing for
//! admission.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrayvec::ArrayVec;
use gabion::crdt::{Count, KeyHash};
use gabion::defaults;
use gabion::gossip::GossipClient;
use gabion::rules::{Descriptor, EnforcementMode, RuleSpec, RuleTable, hash_key};
use thiserror::Error;

use crate::admission::{CardinalityLimits, Decision, LimitRequest, RejectContext, RejectReason};
use crate::store::DashMapStore;

const MAX_DESCRIPTORS: usize = defaults::STORAGE_MAX_DESCRIPTOR_COUNT;
const MAX_MATCHED_RULES: usize = defaults::STORAGE_MAX_MATCHED_RULES;

pub mod admin;
pub mod admission;
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

#[derive(Debug, Error)]
pub enum ServeError {
    #[error("build gRPC reflection service: {0}")]
    Reflection(#[from] tonic_reflection::server::Error),
    #[error("serve gRPC transport: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Adapter state shared between the gRPC service and admin endpoints.
///
/// Cheap to clone (`Arc` + `Clone` handles). The gossip runtime that backs
/// `gossip_client` is owned by `gabiond` and joined separately.
#[derive(Clone)]
pub struct SharedLimiter<C: Count = u32> {
    rule_table: Arc<RuleTable>,
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
        Self {
            rule_table,
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

    /// Evaluate one Envoy rate-limit request at the given wall-clock time.
    /// Each descriptor is admitted independently — rejected descriptors do
    /// not gate the others.
    pub async fn should_rate_limit_at(
        &self,
        request: RateLimitRequest,
        now_millis: u64,
    ) -> RateLimitResponse {
        let hits = u64::from(request.hits_addend.max(1));
        let mut statuses = Vec::with_capacity(request.descriptors.len());
        let max_entries = request
            .descriptors
            .iter()
            .map(|descriptor| descriptor.entries.len())
            .max()
            .unwrap_or(0)
            .min(MAX_DESCRIPTORS);
        let mut mapped = Vec::with_capacity(max_entries);
        let mut over_limit = false;

        for (idx, envoy_descriptor) in request.descriptors.iter().enumerate() {
            let decision = if idx >= MAX_DESCRIPTORS {
                Decision::Reject(RejectReason::Cardinality, None)
            } else {
                match map_envoy_descriptor(envoy_descriptor, &mut mapped) {
                    Ok(()) => {
                        let core_request = LimitRequest {
                            domain: request.domain.as_str(),
                            descriptors: mapped.as_slice(),
                            hits,
                        };
                        self.evaluate(core_request, now_millis).await
                    }
                    Err(reason) => Decision::Reject(reason, None),
                }
            };

            over_limit |= decision.is_reject();
            statuses.push(descriptor_status_for(decision));
        }

        RateLimitResponse {
            overall_code: if over_limit {
                rate_limit_response::Code::OverLimit
            } else {
                rate_limit_response::Code::Ok
            } as i32,
            statuses,
            ..Default::default()
        }
    }

    async fn evaluate(&self, request: LimitRequest<'_>, now_millis: u64) -> Decision {
        if request.violates_cardinality(self.cardinality_limits) {
            note_cardinality_reject(request.domain, request.descriptors.len());
            return Decision::Reject(RejectReason::Cardinality, None);
        }

        // Pass 1: decide allow/reject and buffer (spec, key_hash, bucket)
        // for replay on the gossip-record path. One walk over `matching`,
        // one `hash_key` per matched rule, O(1) `Rule::spec()`. Early-exit
        // on the first enforcing reject — no events have been queued yet.
        // Rules in `DryRun` mode are evaluated and recorded but never
        // produce a reject verdict.
        let mut planned: ArrayVec<(RuleSpec, KeyHash, u32), MAX_MATCHED_RULES> = ArrayVec::new();
        for rule in self
            .rule_table
            .matching(request.domain, request.descriptors)
        {
            let spec = rule.spec();
            let key_hash = hash_key(rule.id, request.domain, request.descriptors);
            let total = self.counts.window_total(
                spec.fingerprint,
                key_hash,
                now_millis,
                spec.bucket_millis,
                spec.live_buckets,
            );
            if total.saturating_add(request.hits) > spec.limit
                && rule.mode == EnforcementMode::Enforce
            {
                let duration_until_reset_millis = self.counts.time_until_admit_millis(
                    spec,
                    key_hash,
                    now_millis,
                    total,
                    request.hits,
                );
                return Decision::Reject(
                    RejectReason::GlobalLimit,
                    Some(RejectContext {
                        limit: spec.limit,
                        remaining: gabion::window::limit_remaining(spec.limit, total),
                        duration_until_reset_millis,
                    }),
                );
            }
            let bucket = (now_millis / spec.bucket_millis) as u32;
            if planned.try_push((spec, key_hash, bucket)).is_err() {
                // Per the allow-by-default principle (see CLAUDE.md), a
                // matched-rules overflow is a gabion-internal limit, not a
                // client cardinality violation: let the request through
                // with the events we managed to buffer rather than
                // rejecting because our cap is undersized.
                note_matched_overflow(request.domain, MAX_MATCHED_RULES);
                break;
            }
        }

        // Pass 2: gossip-record. Only allowed requests reach here, so a
        // single drain of the buffer fans out the deltas.
        for (spec, key_hash, bucket) in &planned {
            if let Err(err) = self
                .gossip_client
                .record(
                    spec.fingerprint,
                    *key_hash,
                    *bucket,
                    request.hits,
                    spec.limit,
                    now_millis,
                )
                .await
            {
                note_gossip_record_failure(&err);
            }
        }
        Decision::Allow
    }
}

fn map_envoy_descriptor<'a>(
    envoy_descriptor: &'a RateLimitDescriptor,
    mapped: &mut Vec<Descriptor<'a>>,
) -> Result<(), RejectReason> {
    mapped.clear();
    for entry in &envoy_descriptor.entries {
        if mapped.len() == MAX_DESCRIPTORS {
            return Err(RejectReason::Cardinality);
        }
        mapped.push(Descriptor {
            key: entry.key.as_str(),
            value: entry.value.as_str(),
        });
    }
    Ok(())
}

// -- Rate-limited operator warnings -----------------------------------------
//
// These two failure modes can both fire at full request rate when something
// is wrong (mass-cardinality attack; gossip runtime dead). Each emits a
// `tracing::warn!` only on power-of-two transitions of its counter, so the
// log volume is bounded at ~log2(N) regardless of request rate.

static CARDINALITY_REJECTS: AtomicU64 = AtomicU64::new(0);
static GOSSIP_RECORD_FAILURES: AtomicU64 = AtomicU64::new(0);
static MATCHED_RULE_OVERFLOWS: AtomicU64 = AtomicU64::new(0);

fn note_cardinality_reject(domain: &str, descriptor_count: usize) {
    let n = CARDINALITY_REJECTS.fetch_add(1, Ordering::Relaxed) + 1;
    if n.is_power_of_two() {
        tracing::warn!(
            domain,
            descriptor_count,
            rejected_total = n,
            config_key = "cardinality_limits",
            "Rejecting requests that attach too many rate-limit descriptors. This is usually a \
             misbehaving client or an attack trying to exhaust gabion's tracking memory. If the \
             traffic is legitimate, raise the relevant key under `cardinality_limits` in your \
             gabion config.",
        );
    }
}

fn note_gossip_record_failure(err: &gabion::gossip::GossipError) {
    let n = GOSSIP_RECORD_FAILURES.fetch_add(1, Ordering::Relaxed) + 1;
    if n.is_power_of_two() {
        tracing::warn!(
            error = %err,
            failed_total = n,
            "This node can no longer share rate-limit counts with the \
             rest of the cluster. Rate limits are now using only this \
             node's local traffic, so they will under-count requests \
             handled by other nodes. The gossip background task has \
             stopped — look for an earlier error log entry to find out \
             why.",
        );
    }
}

fn note_matched_overflow(domain: &str, cap: usize) {
    let n = MATCHED_RULE_OVERFLOWS.fetch_add(1, Ordering::Relaxed) + 1;
    if n.is_power_of_two() {
        tracing::warn!(
            domain,
            cap,
            overflowed_total = n,
            "A single request matched more rules than gabion's per-request cap allows; the \
             request was rejected conservatively rather than truncated. Reduce overlapping rule \
             patterns or raise STORAGE_MAX_MATCHED_RULES."
        );
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

/// gRPC service name reported via the standard `grpc.health.v1.Health`
/// protocol. Liveness/readiness probes (kube-proxy, gRPC load balancers,
/// `grpc_health_probe`) should query for this exact name.
pub const RATE_LIMIT_SERVICE_NAME: &str = "envoy.service.ratelimit.v3.RateLimitService";

/// Run the gRPC server until `shutdown` resolves. Mounts:
///
/// 1. The Envoy rate-limit service itself.
/// 2. `grpc.health.v1.Health` — flip statuses via `health_reporter`. Set to
///    `NOT_SERVING` before triggering `shutdown` so external readiness probes
///    see the failure and route traffic away before in-flight requests start
///    being refused.
/// 3. `grpc.reflection.v1.ServerReflection` — lets `grpcurl list` and similar
///    tools enumerate services without ahead-of-time `.proto` files.
pub async fn serve<C, F, H>(
    bind: std::net::SocketAddr,
    limiter: SharedLimiter<C>,
    health_server: tonic_health::pb::health_server::HealthServer<H>,
    shutdown: F,
) -> Result<(), ServeError>
where
    C: Count + Send + Sync + 'static,
    F: std::future::Future<Output = ()>,
    H: tonic_health::pb::health_server::Health,
{
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .with_service_name(RATE_LIMIT_SERVICE_NAME)
        .build_v1()?;

    tonic::transport::Server::builder()
        .add_service(health_server)
        .add_service(reflection)
        .add_service(RateLimitServiceServer::new(EnvoyRateLimitService::new(
            limiter,
        )))
        .serve_with_shutdown(bind, shutdown)
        .await?;

    Ok(())
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
        .map(|decision| descriptor_status_for(*decision))
        .collect();

    RateLimitResponse {
        overall_code,
        statuses,
        ..Default::default()
    }
}

fn code_for_decision(decision: Decision) -> i32 {
    (if decision.is_reject() {
        rate_limit_response::Code::OverLimit
    } else {
        rate_limit_response::Code::Ok
    }) as i32
}

/// Build the `DescriptorStatus` Envoy renders into the response. Allows
/// and pre-admission rejects (no rule scope, no [`RejectContext`]) leave
/// `limit_remaining` and `duration_until_reset` at their proto defaults;
/// scoped rejects populate both from the [`RejectContext`] computed at
/// reject time.
///
/// `current_limit` (the `RateLimit { requests_per_unit, unit }`
/// sub-message) stays `None`: there is no lossless mapping from arbitrary
/// `(limit, window_millis)` pairs to the discrete `Second/Minute/Hour/Day`
/// enum, and reporting a wrong unit is worse than reporting none.
fn descriptor_status_for(decision: Decision) -> rate_limit_response::DescriptorStatus {
    let code = code_for_decision(decision);
    match decision {
        Decision::Reject(_, Some(ctx)) => rate_limit_response::DescriptorStatus {
            code,
            limit_remaining: u32::try_from(ctx.remaining).unwrap_or(u32::MAX),
            duration_until_reset: Some(envoy_types::pb::google::protobuf::Duration {
                seconds: (ctx.duration_until_reset_millis / 1_000) as i64,
                nanos: ((ctx.duration_until_reset_millis % 1_000) * 1_000_000) as i32,
            }),
            ..Default::default()
        },
        _ => rate_limit_response::DescriptorStatus {
            code,
            ..Default::default()
        },
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
