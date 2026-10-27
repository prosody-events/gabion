//! Admission-time types: the decision codes the server returns and the
//! borrowed request shape it presents to the rule table.

use gabion::rules::Descriptor;

/// Result of evaluating one descriptor's rules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    /// Request was admitted. `None` means no rule matched (exempt or no
    /// rules at all), so no header info is exposed.
    Allow(Option<Quota>),
    /// Carries the reason plus, when the reject was scoped to a specific
    /// rule, the header inputs Envoy renders into the response. Pre-admission
    /// rejects (no rule) leave it `None` — the adapter still emits an
    /// `OverLimit` status, just without invented remaining/reset numbers it
    /// cannot compute.
    Reject(RejectReason, Option<Quota>),
}

impl Decision {
    pub fn is_reject(self) -> bool {
        matches!(self, Self::Reject(..))
    }
}

/// The quota view of the rule a decision is scoped to, which the adapter
/// renders as a `DescriptorStatus` and Envoy as `X-RateLimit-*` headers. On a
/// reject `remaining` is 0 and `duration_until_reset_millis` is the
/// sliding-window-precise time until a same-weight request would be admitted.
/// On an allow it is the rule the client is closest to tripping: `remaining`
/// is what is left (0 for a `DryRun` rule already past its limit) and
/// `duration_until_reset_millis` the cheap bucket-boundary modulo from
/// [`gabion::window::time_until_next_bucket_boundary_millis`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Quota {
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

/// Borrowed admission-time request shape. Maps 1:1 to the per-descriptor
/// slice the gRPC service walks before dispatching to gossip.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LimitRequest<'a> {
    pub domain: &'a str,
    pub descriptors: &'a [Descriptor<'a>],
    pub hits: u64,
}
