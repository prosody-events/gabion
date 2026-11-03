//! Envoy-compatible gRPC rate-limit adapter built on the gossip CRDT.
//!
//! Architectural seams:
//! - [`admission`] — admission-time types (Decision, RejectReason, ...).
//! - [`config`] — YAML config (server-only; the library knows nothing about
//!   YAML).
//! - [`admin`] — a single admin HTTP endpoint backed by the gossip
//!   `AdminCommand` channel.
//! - This module — `SharedLimiter`, the gRPC service trait impl, and admission.
//!
//! Hit semantics are **read-then-record**: each descriptor is evaluated against
//! the current aggregate window, and only allowed descriptors record hits into
//! the gossip runtime. Multi-descriptor requests are not all-or-nothing for
//! admission.

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

use gabion::aggregate::Aggregate;
use gabion::crdt::{BucketEpoch, ValidStorageConfig};
use gabion::gossip::{GossipClient, TryRecordError};
use gabion::rules::{CardinalityLimit, Descriptor, EnforcementMode, RuleTable, hash_key};
use gabion::settings::Setting;
use gabion::throttle;
use thiserror::Error;
use tonic::transport::server::TcpIncoming;

use crate::admission::{Decision, Quota, RejectReason};

pub mod admin;
pub mod admission;
pub mod config;

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
    #[error("could not build the gRPC reflection service")]
    Reflection(#[from] tonic_reflection::server::Error),
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// Adapter state shared between the gRPC service and admin endpoints.
///
/// Cheap to clone (`Arc` + `Clone` handles). The gossip runtime that backs
/// `gossip_client` is owned by `gabiond` and joined separately.
#[derive(Clone)]
pub struct SharedLimiter {
    rule_table: Arc<RuleTable>,
    gossip_client: GossipClient,
    counts: Arc<Aggregate>,
    storage: ValidStorageConfig,
    warnings: Arc<WarningCounts>,
}

/// Occurrence counters behind the rate-limited warnings, shared by every
/// clone of one limiter.
#[derive(Default)]
struct WarningCounts {
    cardinality_rejects: AtomicU64,
    gossip_record_failures: AtomicU64,
    matched_rule_overflows: AtomicU64,
    request_queue_full: AtomicU64,
}

/// One allowed rule's hit, held between deciding a descriptor and handing
/// its hits to the gossip runtime.
#[derive(Clone, Copy)]
struct Planned {
    fingerprint: u128,
    bucket: BucketEpoch,
    limit: u64,
}

// The READMEs and `gabion::defaults` quote 32 bytes a matched rule per
// runtime thread; `matched_rule_overflow` prints the live size.
const _: () = assert!(
    size_of::<Planned>() == 32,
    "update the 32 bytes a rule the READMEs and gabion::defaults quote"
);

thread_local! {
    /// This thread's admission scratch: reserved to its bound on the
    /// thread's first admission and never grown after. Admission takes it
    /// for one synchronous section and puts it back.
    static PLANNED: Cell<Vec<Planned>> = const { Cell::new(Vec::new()) };
}

impl SharedLimiter {
    pub fn new(
        rule_table: Arc<RuleTable>,
        gossip_client: GossipClient,
        counts: Arc<Aggregate>,
        storage: ValidStorageConfig,
    ) -> Self {
        Self {
            rule_table,
            gossip_client,
            counts,
            storage,
            warnings: Arc::default(),
        }
    }

    /// Evaluate one Envoy rate-limit request at the given wall-clock time.
    /// Each descriptor is admitted independently — rejected descriptors do
    /// not gate the others.
    pub fn should_rate_limit_at(
        &self,
        request: &RateLimitRequest,
        now_millis: u64,
    ) -> RateLimitResponse {
        let hits = u64::from(request.hits_addend.max(1));
        let max_descriptors = self.storage.cardinality.max_request_descriptors();
        let mut over_limit = false;
        let statuses = request
            .descriptors
            .iter()
            .enumerate()
            .map(|(i, descriptor)| {
                let decision = if i < max_descriptors {
                    self.decide(&request.domain, descriptor, hits, now_millis)
                } else {
                    self.note_cardinality_reject(
                        &request.domain,
                        CardinalityLimit::RequestDescriptors,
                        max_descriptors,
                    )
                };
                over_limit |= decision.is_reject();
                descriptor_status_for(decision)
            })
            .collect();
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

    /// Admit one Envoy descriptor in one synchronous section: read its
    /// entries in place, walk the matching rules once, and on an allow hand
    /// each matched rule's hit to the gossip runtime without waiting. Rules
    /// in `DryRun` mode are evaluated and recorded but never reject.
    pub fn decide(
        &self,
        domain: &str,
        descriptor: &RateLimitDescriptor,
        hits: u64,
        now_millis: u64,
    ) -> Decision {
        let entries = descriptor.entries.iter().map(|entry| Descriptor {
            key: &entry.key,
            value: &entry.value,
        });
        if let Some((limit, max)) = self.storage.cardinality.exceeded(domain, entries.clone()) {
            return self.note_cardinality_reject(domain, limit, max);
        }
        let mut planned = PLANNED.take();
        let decision = self.admit(domain, entries, hits, now_millis, &mut planned);
        PLANNED.set(planned);
        decision
    }

    /// Walk the rules matching `entries` once, deciding on each; on an
    /// allow, hand each matched rule's hit to the gossip runtime without
    /// waiting. `planned` holds the hits in between.
    fn admit<'a>(
        &self,
        domain: &str,
        entries: impl ExactSizeIterator<Item = Descriptor<'a>> + Clone,
        hits: u64,
        now_millis: u64,
        planned: &mut Vec<Planned>,
    ) -> Decision {
        let bound = self
            .storage
            .max_matched_rules()
            .get()
            .min(self.rule_table.rules().len());
        if planned.capacity() < bound {
            *planned = Vec::with_capacity(bound);
        }
        planned.clear();
        let table = self.counts.table();
        // Hashed once, on the first matching rule: every rule matched here
        // shares the descriptor's domain and entries.
        let mut key_hash = None;
        let mut tightest: Option<Quota> = None;
        let mut overflowed = false;
        for rule in self.rule_table.matching(domain, entries.clone()) {
            let key_hash = *key_hash.get_or_insert_with(|| hash_key(domain, entries.clone()));
            let spec = rule.spec();
            let total = table.window_total(spec, key_hash, now_millis);
            if total.saturating_add(hits) > spec.limit && rule.mode() == EnforcementMode::Enforce {
                return Decision::Reject(RejectReason::GlobalLimit(Quota {
                    remaining: 0,
                    duration_until_reset_millis: table
                        .time_until_admit_millis(spec, key_hash, now_millis, total, hits),
                }));
            }
            // Saturating: DryRun rule past its limit (which the gate above
            // lets through) reports `remaining = 0` rather than underflowing.
            let candidate = Quota {
                remaining: spec.limit.saturating_sub(total).saturating_sub(hits),
                duration_until_reset_millis: gabion::window::time_until_next_bucket_boundary_millis(
                    now_millis,
                    spec.bucket_millis,
                ),
            };
            tightest = Some(pick_min_remaining(tightest, candidate));
            // Allow by default (CLAUDE.md): rules past the bound are checked
            // but not counted, as in NGINX.
            if planned.len() < bound {
                planned.push(Planned {
                    fingerprint: spec.fingerprint,
                    bucket: spec.bucket_at(now_millis),
                    limit: spec.limit,
                });
            } else {
                overflowed = true;
            }
        }
        if overflowed {
            self.note_matched_overflow(domain);
        }
        if let Some(key_hash) = key_hash {
            for hit in planned.iter() {
                let recorded = self.gossip_client.try_record(
                    hit.fingerprint,
                    key_hash,
                    hit.bucket,
                    hits,
                    hit.limit,
                    now_millis,
                );
                match recorded {
                    Ok(()) => {}
                    Err(TryRecordError::Full) => self.note_request_queue_full(),
                    Err(TryRecordError::ShutDown) => self.note_gossip_record_failure(),
                }
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

// -- Rate-limited operator warnings -----------------------------------------
//
// These failure modes can all fire at full request rate when something is
// wrong (mass-cardinality attack; gossip runtime dead or starved; overlapping
// rules matching past the per-request bound). Each counts through
// `gabion::throttle`, so the log volume is bounded at ~log2(N) regardless of
// request rate.

impl SharedLimiter {
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
                config_key = setting.yaml_key(),
                "Rejecting requests that attach too many or too large rate-limit descriptors. \
                 This is usually a misbehaving client or an attack trying to exhaust gabion's \
                 tracking memory. {limit}",
            );
        }
        Decision::Reject(RejectReason::Cardinality)
    }

    fn note_gossip_record_failure(&self) {
        if let Some(failed_total) = throttle::bump_shared(&self.warnings.gossip_record_failures) {
            tracing::warn!(
                code = "gossip_record_failure",
                failed_total,
                "This node can no longer share rate-limit counts with the rest of the cluster. \
                 Its limits have stopped counting: no hits from this node or its peers reach the \
                 counts it checks, so as their windows pass they allow traffic they should \
                 reject. The gossip background task has stopped — look for an earlier error log \
                 entry to find out why.",
            );
        }
    }

    fn note_request_queue_full(&self) {
        if let Some(dropped_total) = throttle::bump_shared(&self.warnings.request_queue_full) {
            tracing::warn!(
                code = "request_queue_full",
                capacity = self.gossip_client.capacity(),
                dropped_total,
                config_key = Setting::GossipLimitQueueCapacity.yaml_key(),
                "The gossip request queue was full, so counted hits were dropped: those requests \
                 were allowed and the cluster's counts are missing them, so limits briefly \
                 under-count. Usually a burst larger than the queue, or the gossip task starved \
                 of CPU. Raise {} or give the pod more CPU.",
                Setting::GossipLimitQueueCapacity,
            );
        }
    }

    fn note_matched_overflow(&self, domain: &str) {
        if let Some(overflowed_total) = throttle::bump_shared(&self.warnings.matched_rule_overflows)
        {
            let cap = self.storage.max_matched_rules().get();
            tracing::warn!(
                code = "matched_rule_overflow",
                domain,
                cap,
                overflowed_total,
                config_key = Setting::StorageMaxMatchedRules.yaml_key(),
                "A request matched more than {cap} rate-limit rules, the most this node counts \
                 per request. It was allowed, and only the first {cap} matched rules counted it, \
                 so the rest under-count. Usually overlapping rule patterns. Merge or narrow the \
                 overlapping rules under `limits:`, or raise {}; each extra rule costs {} bytes \
                 per runtime thread, reserved once on the thread's first request.",
                Setting::StorageMaxMatchedRules,
                size_of::<Planned>(),
            );
        }
    }
}

/// Tonic-mounted gRPC service.
#[derive(Clone)]
pub struct EnvoyRateLimitService {
    limiter: SharedLimiter,
}

impl EnvoyRateLimitService {
    pub fn new(limiter: SharedLimiter) -> Self {
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
///
/// The caller binds `listener`, so a taken port fails before anything claims
/// to serve; accepted connections keep `TCP_NODELAY`, tonic's default.
pub async fn serve<F, H>(
    listener: tokio::net::TcpListener,
    limiter: SharedLimiter,
    health_server: tonic_health::pb::health_server::HealthServer<H>,
    shutdown: F,
) -> Result<(), ServeError>
where
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
        .serve_with_incoming_shutdown(
            TcpIncoming::from(listener).with_nodelay(Some(true)),
            shutdown,
        )
        .await?;

    Ok(())
}

#[tonic::async_trait]
impl envoy_types::pb::envoy::service::ratelimit::v3::rate_limit_service_server::RateLimitService
    for EnvoyRateLimitService
{
    async fn should_rate_limit(
        &self,
        request: tonic::Request<RateLimitRequest>,
    ) -> Result<tonic::Response<RateLimitResponse>, tonic::Status> {
        let response = self
            .limiter
            .should_rate_limit_at(request.get_ref(), now_millis());
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
