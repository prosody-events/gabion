//! Admission-time types: the decision codes the server returns.

/// Result of evaluating one descriptor's rules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    /// Request was admitted. `None` means no rule matched (exempt or no
    /// rules at all), so no header info is exposed.
    Allow(Option<Quota>),
    /// Request was rejected, for the reason given.
    Reject(RejectReason),
}

impl Decision {
    pub fn is_reject(self) -> bool {
        matches!(self, Self::Reject(..))
    }
}

/// The quota of the rule a decision is scoped to, which the adapter renders
/// as a `DescriptorStatus` and Envoy as `X-RateLimit-*` headers. On a reject
/// `remaining` is 0 and the reset is the time until a same-weight request
/// would be admitted; on an allow it is the rule the client is closest to
/// tripping, and the reset is the next bucket boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Quota {
    pub remaining: u64,
    pub duration_until_reset_millis: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectReason {
    /// One of the matching rules saw the window total exceed `rule.limit`.
    GlobalLimit(Quota),
    /// Request violated the configured cardinality envelope and was rejected
    /// before any counter was touched.
    Cardinality,
}
