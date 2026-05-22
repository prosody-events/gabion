//! Admission-time types: the decision codes the server returns, the
//! cardinality envelope it enforces, and the borrowed request shape it
//! presents to the rule table.

use gabion::rules::Descriptor;

/// Result of evaluating one descriptor's rules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    Allow,
    Reject(RejectReason),
}

impl Decision {
    pub fn is_reject(self) -> bool {
        matches!(self, Self::Reject(_))
    }
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

impl LimitRequest<'_> {
    pub fn violates_cardinality(self, limits: CardinalityLimits) -> bool {
        if self.descriptors.len() > limits.max_descriptor_count {
            return true;
        }
        let mut bytes = self.domain.len();
        for descriptor in self.descriptors {
            if descriptor.key.len() > limits.max_key_bytes {
                return true;
            }
            bytes = bytes
                .saturating_add(descriptor.key.len())
                .saturating_add(descriptor.value.len());
            if bytes > limits.max_descriptor_bytes {
                return true;
            }
        }
        false
    }
}
