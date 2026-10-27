//! Admission-time types: the decision codes the server returns, the
//! cardinality envelope it enforces, and the borrowed request shape it
//! presents to the rule table.

use gabion::rules::Descriptor;
use gabion::settings::Setting;

/// Result of evaluating one descriptor's rules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    /// Request was admitted. When at least one matching rule produced a
    /// quota view, [`QuotaContext`] carries the rule the client is
    /// closest to tripping so the adapter can populate
    /// `limit_remaining` / `duration_until_reset` on the OK status —
    /// the Envoy filter then emits `X-RateLimit-*` on the allowed
    /// response. `None` means no rule matched (exempt or no rules at
    /// all), so no header info is exposed.
    Allow(Option<QuotaContext>),
    /// Carries the reason plus, when the reject was scoped to a specific
    /// rule, the precomputed header inputs Envoy renders into the response.
    /// Pre-admission rejects (no rule) leave the context `None` — the
    /// adapter still emits an `OverLimit` status, just without invented
    /// remaining/reset numbers it cannot compute.
    Reject(RejectReason, Option<RejectContext>),
}

impl Decision {
    pub fn is_reject(self) -> bool {
        matches!(self, Self::Reject(..))
    }
}

/// Header-shaped reject payload populated alongside
/// [`RejectReason::GlobalLimit`]. Mirrors what Envoy expects on a
/// per-descriptor `DescriptorStatus`: `limit_remaining` is the floor (0 on
/// reject), and `duration_until_reset_millis` is the sliding-window-precise
/// time until a same-weight request would be admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RejectContext {
    pub limit: u64,
    pub remaining: u64,
    pub duration_until_reset_millis: u64,
}

/// Per-allowed-descriptor quota view. Shape mirrors [`RejectContext`] so
/// the adapter can render `DescriptorStatus` with the same field
/// population, but on the allow path `remaining > 0` (or `0` for a
/// `DryRun` rule already past its limit) and `duration_until_reset_millis`
/// is the cheap bucket-boundary modulo from
/// [`gabion::window::time_until_next_bucket_boundary_millis`] — not the
/// sliding-window walk used on reject.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuotaContext {
    pub limit: u64,
    pub remaining: u64,
    pub duration_until_reset_millis: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectReason {
    /// One of the matching rules saw the window total exceed `rule.limit`.
    GlobalLimit,
    /// Request violated the configured cardinality envelope and was rejected
    /// before any counter was touched.
    Cardinality,
}

/// Upper bounds on the shape of an incoming request. Enforced before any
/// gossip work so a malicious or buggy client cannot drive cell-store growth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CardinalityLimits {
    pub max_descriptor_count: usize,
    pub max_descriptor_bytes: usize,
    pub max_key_bytes: usize,
}

impl Default for CardinalityLimits {
    fn default() -> Self {
        Self {
            max_descriptor_count: gabion::defaults::STORAGE_MAX_DESCRIPTOR_COUNT,
            max_descriptor_bytes: gabion::defaults::STORAGE_MAX_DESCRIPTOR_BYTES,
            max_key_bytes: gabion::defaults::STORAGE_MAX_KEY_BYTES,
        }
    }
}

/// Borrowed admission-time request shape. Maps 1:1 to the per-descriptor
/// slice the gRPC service walks before dispatching to gossip.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LimitRequest<'a> {
    pub domain: &'a str,
    pub descriptors: &'a [Descriptor<'a>],
    pub hits: u64,
}

/// Which part of the cardinality envelope a request exceeded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CardinalityLimit {
    /// More descriptors in one request than gabion evaluates (a build
    /// constant).
    RequestDescriptors,
    /// More entries in one descriptor than `max_descriptor_count`.
    Count,
    /// A descriptor's domain, keys and values total more than
    /// `max_descriptor_bytes`.
    Bytes,
    /// One descriptor key is longer than `max_key_bytes`.
    KeyBytes,
}

impl CardinalityLimit {
    /// The `limit` log field, the setting that raises this limit (none for a
    /// build constant), and the operator's next step.
    pub(crate) fn describe(self) -> (&'static str, Option<Setting>, &'static str) {
        match self {
            Self::RequestDescriptors => (
                "request_descriptors",
                None,
                "gabion evaluates at most `max` descriptors of one request, a build-time constant \
                 (`STORAGE_MAX_DESCRIPTOR_COUNT`) that config cannot raise; if the traffic is \
                 legitimate, have Envoy send fewer descriptors per request.",
            ),
            Self::Count => (
                "count",
                Some(Setting::StorageMaxDescriptorCount),
                "If the traffic is legitimate, raise `storage.max_descriptor_count` \
                 (`GABION_STORAGE_MAX_DESCRIPTOR_COUNT`); values above 16, the build-time cap \
                 `STORAGE_MAX_DESCRIPTOR_COUNT`, have no effect.",
            ),
            Self::Bytes => (
                "bytes",
                Some(Setting::StorageMaxDescriptorBytes),
                "If the traffic is legitimate, raise `storage.max_descriptor_bytes` \
                 (`GABION_STORAGE_MAX_DESCRIPTOR_BYTES`).",
            ),
            Self::KeyBytes => (
                "key_bytes",
                Some(Setting::StorageMaxKeyBytes),
                "If the traffic is legitimate, raise `storage.max_key_bytes` \
                 (`GABION_STORAGE_MAX_KEY_BYTES`).",
            ),
        }
    }
}

impl LimitRequest<'_> {
    /// The first limit this request exceeds and its bound, if any.
    pub fn exceeded_limit(self, limits: CardinalityLimits) -> Option<(CardinalityLimit, usize)> {
        if self.descriptors.len() > limits.max_descriptor_count {
            return Some((CardinalityLimit::Count, limits.max_descriptor_count));
        }
        let mut bytes = self.domain.len();
        for descriptor in self.descriptors {
            if descriptor.key.len() > limits.max_key_bytes {
                return Some((CardinalityLimit::KeyBytes, limits.max_key_bytes));
            }
            bytes = bytes
                .saturating_add(descriptor.key.len())
                .saturating_add(descriptor.value.len());
            if bytes > limits.max_descriptor_bytes {
                return Some((CardinalityLimit::Bytes, limits.max_descriptor_bytes));
            }
        }
        None
    }
}
