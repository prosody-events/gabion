//! Rate-limit rule representation and stable hashing shared between
//! `gabion-server` (gRPC) and `gabion-nginx` (in-process RLM).
//!
//! Both adapters feed the gossip CRDT a `(rule_fingerprint, key_hash, bucket)`
//! tuple. This module owns the deterministic functions that produce those
//! identifiers so two nodes with identical rule sets emit identical
//! identifiers without coordinating. Admission policy (decision codes,
//! cardinality enforcement, request mapping) is the adapter's job — kept out
//! of this module on purpose.

use twox_hash::xxhash3_128::{DEFAULT_SECRET_LENGTH, RawHasher, SecretBuffer};

pub use crate::crdt::KeyHash;

/// Application-level rule identifier. Internal to one process; gossip uses
/// [`Rule::fingerprint`] for cross-node identity instead.
pub type RuleId = u32;

/// Operator-facing enforcement mode for a rule.
///
/// * `Enforce` — evaluate and reject on overflow.
/// * `DryRun` — evaluate, record the hit (so metrics and gossip work), but
///   never reject. Closes the observability gap between `Enforce` and
///   `Disabled`. Excluded from no-op skipping in [`RuleTable::matching`];
///   adapters must check the mode before mapping a verdict to a rejection.
/// * `Disabled` — skip the rule entirely.
///
/// `Rule::fingerprint` deliberately does not hash `mode`: two nodes with the
/// same rule shape but different enforcement settings must still share a
/// cluster-wide counter identity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum EnforcementMode {
    #[default]
    Enforce,
    DryRun,
    Disabled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Descriptor<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

/// One descriptor pattern in a rule. `value == "*"` matches any value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptorPattern {
    pub key: Box<str>,
    pub value: Box<str>,
}

impl DescriptorPattern {
    fn matches(&self, descriptor: Descriptor<'_>) -> bool {
        &*self.key == descriptor.key && (&*self.value == "*" || &*self.value == descriptor.value)
    }
}

/// One rate-limit rule. Constructed by the adapter from its own typed config;
/// `fingerprint` and `domain_hash` are derived deterministically so two nodes
/// with identical rules produce identical fingerprints.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Rule {
    pub id: RuleId,
    /// Stable, cross-node identity. Hashed from `(domain, descriptors)`.
    pub fingerprint: u128,
    pub domain: Box<str>,
    pub domain_hash: KeyHash,
    pub descriptors: Box<[DescriptorPattern]>,
    pub limit: u64,
    pub window_millis: u64,
    pub bucket_millis: u64,
    pub live_buckets: u32,
    pub mode: EnforcementMode,
}

impl Rule {
    /// Construct a rule, deriving `fingerprint` and `domain_hash` from
    /// `domain` + `descriptors`. `domain` accepts anything `Into<String>`
    /// for operator-friendly construction; the field is stored as the
    /// owned-immutable `Box<str>` per the no-capacity-after-construction
    /// rule in CLAUDE.md.
    pub fn new(
        id: RuleId,
        domain: impl Into<String>,
        descriptors: Vec<DescriptorPattern>,
        limit: u64,
        window_millis: u64,
        bucket_millis: u64,
        mode: EnforcementMode,
    ) -> Self {
        let domain: Box<str> = domain.into().into_boxed_str();
        let descriptors: Box<[DescriptorPattern]> = descriptors.into_boxed_slice();
        let fingerprint = rule_fingerprint(&domain, &descriptors);
        let domain_hash = hash_domain(&domain);
        let window_millis = window_millis.max(1);
        let bucket_millis = bucket_millis.max(1);
        let live_buckets = window_millis.div_ceil(bucket_millis).max(1) as u32;
        Self {
            id,
            fingerprint,
            domain,
            domain_hash,
            descriptors,
            limit,
            window_millis,
            bucket_millis,
            live_buckets,
            mode,
        }
    }

    /// Hot-path-ready summary. O(1) field copies.
    pub fn spec(&self) -> RuleSpec {
        RuleSpec {
            id: self.id,
            fingerprint: self.fingerprint,
            limit: self.limit,
            bucket_millis: self.bucket_millis,
            window_millis: self.window_millis,
            live_buckets: self.live_buckets,
        }
    }
}

/// Hot-path-ready summary of one rule. The per-request decision path holds
/// this by value (Copy) so neither adapter needs an O(N) lookup back into
/// the rule table by id.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuleSpec {
    pub id: RuleId,
    pub fingerprint: u128,
    pub limit: u64,
    pub bucket_millis: u64,
    pub window_millis: u64,
    pub live_buckets: u32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuleTable {
    rules: Box<[Rule]>,
}

impl RuleTable {
    pub fn new(rules: Vec<Rule>) -> Self {
        Self {
            rules: rules.into_boxed_slice(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Rule> {
        self.rules.iter()
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn get(&self, id: RuleId) -> Option<&Rule> {
        self.rules.iter().find(|rule| rule.id == id)
    }

    /// Rules whose `(domain, descriptor pattern)` matches the request shape,
    /// excluding any rule in [`EnforcementMode::Disabled`]. Rules in
    /// [`EnforcementMode::DryRun`] are returned so adapters can still hash
    /// the bucket and gossip the hit; adapters must check `rule.mode`
    /// before mapping a verdict to a rejection.
    ///
    /// The server adapter uses this for pattern-based filtering of Envoy
    /// descriptors; the nginx adapter dispatches by `rule_index` (per
    /// configured `gabion_limit`) and does not walk `matching` per request.
    pub fn matching<'a>(
        &'a self,
        domain: &'a str,
        descriptors: &'a [Descriptor<'a>],
    ) -> impl Iterator<Item = &'a Rule> + 'a {
        let domain_hash = hash_domain(domain);
        self.rules.iter().filter(move |rule| {
            rule.mode != EnforcementMode::Disabled
                && rule.domain_hash == domain_hash
                && rule.descriptors.len() == descriptors.len()
                && rule
                    .descriptors
                    .iter()
                    .zip(descriptors)
                    .all(|(pattern, descriptor)| pattern.matches(*descriptor))
        })
    }
}

// -- hashing ----------------------------------------------------------------

pub fn hash_domain(domain: &str) -> KeyHash {
    let mut hasher = stable_hasher();
    hasher.write(b"gabion.domain.v1");
    hasher.write(&[0]);
    hasher.write(domain.as_bytes());
    KeyHash(hasher.finish_128())
}

pub fn hash_key(rule_id: RuleId, domain: &str, descriptors: &[Descriptor<'_>]) -> KeyHash {
    let mut hasher = stable_hasher();
    hasher.write(b"gabion.key.v1");
    hasher.write(&[0]);
    hasher.write(&u64::from(rule_id).to_le_bytes());
    hasher.write(domain.as_bytes());
    hasher.write(&[0]);
    write_descriptors(&mut hasher, descriptors.iter().map(|d| (d.key, d.value)));
    KeyHash(hasher.finish_128())
}

/// Canonical fingerprint of a rule's shape — the cross-node identity gossip
/// uses as its key, independent of any process-local `RuleId`.
pub fn rule_fingerprint(domain: &str, descriptors: &[DescriptorPattern]) -> u128 {
    let mut hasher = stable_hasher();
    hasher.write(b"gabion.rule.v1");
    hasher.write(&[0]);
    hasher.write(domain.as_bytes());
    hasher.write(&[0]);
    write_descriptors(
        &mut hasher,
        descriptors.iter().map(|p| (&*p.key, &*p.value)),
    );
    hasher.finish_128()
}

fn write_descriptors<'a>(
    hasher: &mut RawHasher<[u8; DEFAULT_SECRET_LENGTH]>,
    descriptors: impl IntoIterator<Item = (&'a str, &'a str)>,
) {
    for (key, value) in descriptors {
        hasher.write(key.as_bytes());
        hasher.write(&[0]);
        hasher.write(value.as_bytes());
        hasher.write(&[0xff]);
    }
}

fn stable_hasher() -> RawHasher<[u8; DEFAULT_SECRET_LENGTH]> {
    let secret =
        SecretBuffer::new(0, [0x9d; DEFAULT_SECRET_LENGTH]).expect("valid XXH3 secret length");
    RawHasher::new(secret)
}
