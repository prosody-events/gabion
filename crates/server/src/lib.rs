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
use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

use arrayvec::ArrayVec;
use gabion::crdt::{Count, KeyHash};
use gabion::defaults;
use gabion::gossip::GossipClient;
use gabion::rules::{
    CardinalityLimit, CardinalityLimits, Descriptor, EnforcementMode, RuleSpec, RuleTable, hash_key,
};
use gabion::settings::Setting;
use gabion::throttle;
use thiserror::Error;

use crate::admission::{Decision, LimitRequest, Quota, RejectReason};
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
    warnings: Arc<WarningCounts>,
}

/// Occurrence counters behind the rate-limited warnings, shared by every
/// clone of one limiter.
#[derive(Default)]
struct WarningCounts {
    cardinality_rejects: AtomicU64,
    gossip_record_failures: AtomicU64,
    matched_rule_overflows: AtomicU64,
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
            warnings: Arc::default(),
        }
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
                self.note_cardinality_reject(
                    &request.domain,
                    CardinalityLimit::RequestDescriptors,
                    MAX_DESCRIPTORS,
                )
            } else if map_envoy_descriptor(envoy_descriptor, &mut mapped) {
                let core_request = LimitRequest {
                    domain: request.domain.as_str(),
                    descriptors: mapped.as_slice(),
                    hits,
                };
                self.evaluate(core_request, now_millis).await
            } else {
                self.note_cardinality_reject(
                    &request.domain,
                    CardinalityLimit::Count,
                    self.cardinality_limits.max_descriptor_count(),
                )
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
        if let Some((limit, max)) = self
            .cardinality_limits
            .exceeded(request.domain, request.descriptors)
        {
            return self.note_cardinality_reject(request.domain, limit, max);
        }

        // Pass 1: decide allow/reject and buffer (spec, key_hash, bucket)
        // for replay on the gossip-record path. One walk over `matching`,
        // one `hash_key` per matched rule, O(1) `Rule::spec()`. Early-exit
        // on the first enforcing reject — no events have been queued yet.
        // Rules in `DryRun` mode are evaluated and recorded but never
        // produce a reject verdict.
        let mut planned: ArrayVec<(RuleSpec, KeyHash, u32), MAX_MATCHED_RULES> = ArrayVec::new();
        let mut tightest: Option<Quota> = None;
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
                return Decision::Reject(RejectReason::GlobalLimit(Quota {
                    remaining: 0,
                    duration_until_reset_millis,
                }));
            }
            // Saturating: DryRun rule past its limit (which the gate
            // above lets through) reports `remaining = 0` rather than
            // underflowing.
            let remaining = spec
                .limit
                .saturating_sub(total)
                .saturating_sub(request.hits);
            let duration_until_reset_millis =
                gabion::window::time_until_next_bucket_boundary_millis(
                    now_millis,
                    spec.bucket_millis,
                );
            let candidate = Quota {
                remaining,
                duration_until_reset_millis,
            };
            tightest = Some(pick_min_remaining(tightest, candidate));
            let bucket = (now_millis / spec.bucket_millis) as u32;
            if planned.try_push((spec, key_hash, bucket)).is_err() {
                // Per the allow-by-default principle (see CLAUDE.md), a
                // matched-rules overflow is a gabion-internal limit, not a
                // client cardinality violation: let the request through
                // with the events we managed to buffer rather than
                // rejecting because our cap is undersized.
                self.note_matched_overflow(request.domain);
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
                self.note_gossip_record_failure(&err);
            }
        }
        Decision::Allow(tightest)
    }
}

/// Pick the rule the client is closest to tripping (smallest
/// `remaining`). Ties keep the existing pick — first match wins,
/// preserving the rule-declaration order surfaced by `RuleTable::matching`.
fn pick_min_remaining(current: Option<Quota>, candidate: Quota) -> Quota {
    match current {
        Some(c) if c.remaining <= candidate.remaining => c,
        _ => candidate,
    }
}

/// Borrow an Envoy descriptor's entries into `mapped`. False when it has more
/// than `MAX_DESCRIPTORS` entries.
fn map_envoy_descriptor<'a>(
    envoy_descriptor: &'a RateLimitDescriptor,
    mapped: &mut Vec<Descriptor<'a>>,
) -> bool {
    mapped.clear();
    for entry in &envoy_descriptor.entries {
        if mapped.len() == MAX_DESCRIPTORS {
            return false;
        }
        mapped.push(Descriptor {
            key: entry.key.as_str(),
            value: entry.value.as_str(),
        });
    }
    true
}

// -- Rate-limited operator warnings -----------------------------------------
//
// These failure modes can all fire at full request rate when something is
// wrong (mass-cardinality attack; gossip runtime dead; overlapping rules
// matching past the per-request cap). Each counts through
// `gabion::throttle`, so the log volume is bounded at ~log2(N) regardless of
// request rate.

impl<C: Count> SharedLimiter<C> {
    /// Reject one descriptor that exceeds `limit`, logging at powers of two.
    fn note_cardinality_reject(
        &self,
        domain: &str,
        limit: CardinalityLimit,
        max: usize,
    ) -> Decision {
        if let Some(rejected_total) = throttle::bump_shared(&self.warnings.cardinality_rejects) {
            let (name, setting) = limit.describe();
            tracing::warn!(
                code = "cardinality_rejected",
                domain,
                limit = name,
                max,
                rejected_total,
                config_key = setting.map(Setting::yaml_key),
                "Rejecting requests that attach too many or too large rate-limit descriptors. \
                 This is usually a misbehaving client or an attack trying to exhaust gabion's \
                 tracking memory. {limit}",
            );
        }
        Decision::Reject(RejectReason::Cardinality)
    }

    fn note_gossip_record_failure(&self, err: &gabion::gossip::GossipError) {
        if let Some(failed_total) = throttle::bump_shared(&self.warnings.gossip_record_failures) {
            tracing::warn!(
                code = "gossip_record_failure",
                error = %err,
                failed_total,
                "This node can no longer share rate-limit counts with the \
                 rest of the cluster. Its limits have stopped counting: no \
                 hits from this node or its peers reach the counts it \
                 checks, so as their windows pass they allow traffic they \
                 should reject. The gossip background task has stopped — \
                 look for an earlier error log entry to find out why.",
            );
        }
    }

    fn note_matched_overflow(&self, domain: &str) {
        if let Some(overflowed_total) = throttle::bump_shared(&self.warnings.matched_rule_overflows)
        {
            tracing::warn!(
                code = "matched_rule_overflow",
                domain,
                cap = MAX_MATCHED_RULES,
                overflowed_total,
                "A request matched more than {MAX_MATCHED_RULES} rate-limit rules, the most this \
                 node counts per request. It was allowed, and only the first {MAX_MATCHED_RULES} \
                 matched rules counted it, so the rest under-count. Usually overlapping rule \
                 patterns. Merge or narrow the overlapping rules under `limits:`; the cap is a \
                 build-time constant (`STORAGE_MAX_MATCHED_RULES`) and cannot be set in config.",
            );
        }
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

fn code_for_decision(decision: Decision) -> i32 {
    (if decision.is_reject() {
        rate_limit_response::Code::OverLimit
    } else {
        rate_limit_response::Code::Ok
    }) as i32
}

/// Build the `DescriptorStatus` Envoy renders into the response. A decision
/// with a [`Quota`] populates `limit_remaining` and `duration_until_reset`;
/// the Envoy filter with `enable_x_ratelimit_headers: DRAFT_VERSION_03` turns
/// those into `X-RateLimit-*` response headers on the upstream response.
/// A cardinality reject leaves both fields at their proto defaults — there is
/// no rule to compute a real reset moment against.
///
/// `current_limit` (the `RateLimit { requests_per_unit, unit }`
/// sub-message) stays `None`: there is no lossless mapping from arbitrary
/// `(limit, window_millis)` pairs to the discrete `Second/Minute/Hour/Day`
/// enum, and reporting a wrong unit is worse than reporting none.
fn descriptor_status_for(decision: Decision) -> rate_limit_response::DescriptorStatus {
    let code = code_for_decision(decision);
    match decision {
        Decision::Reject(RejectReason::GlobalLimit(quota)) | Decision::Allow(Some(quota)) => {
            rate_limit_response::DescriptorStatus {
                code,
                limit_remaining: u32::try_from(quota.remaining).unwrap_or(u32::MAX),
                duration_until_reset: Some(duration_from_millis(quota.duration_until_reset_millis)),
                ..Default::default()
            }
        }
        _ => rate_limit_response::DescriptorStatus {
            code,
            ..Default::default()
        },
    }
}

fn duration_from_millis(millis: u64) -> envoy_types::pb::google::protobuf::Duration {
    envoy_types::pb::google::protobuf::Duration {
        seconds: (millis / 1_000) as i64,
        nanos: ((millis % 1_000) * 1_000_000) as i32,
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
