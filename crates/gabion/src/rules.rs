//! Rate-limit rule representation and stable hashing shared between
//! `gabion-server` (gRPC) and `gabion-nginx` (in-process RLM).
//!
//! Both adapters feed the gossip CRDT a `(rule_fingerprint, key_hash, bucket)`
//! tuple. This module owns the deterministic functions that produce those
//! identifiers so two nodes with identical rule sets emit identical
//! identifiers without coordinating. It also owns the one cardinality check
//! both adapters enforce on client-supplied descriptors, and the descriptor
//! key grammar both accept. Admission policy (decision codes, request
//! mapping) is the adapter's job — kept out of this module on purpose.

use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

use thiserror::Error;
use twox_hash::xxhash3_128::{DEFAULT_SECRET_LENGTH, RawHasher, SecretBuffer};

pub use crate::crdt::KeyHash;
use crate::defaults::STORAGE_MAX_DESCRIPTOR_COUNT;
use crate::settings::Setting;

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

/// Match a descriptor key against `[A-Za-z_][A-Za-z0-9_.-]*`, the grammar
/// both adapters accept, so equal rules produce equal identifiers.
pub fn is_descriptor_key(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c == '.' || c == '-' || c.is_ascii_alphanumeric())
}

// -- cardinality ------------------------------------------------------------

/// Upper bounds on the descriptors one request attaches, checked before any
/// counter is touched so a malicious or buggy client cannot drive cell-store
/// growth. Built only by [`crate::settings::Settings::validate`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CardinalityLimits {
    pub(crate) max_descriptor_count: usize,
    pub(crate) max_descriptor_bytes: usize,
    pub(crate) max_key_bytes: usize,
}

impl CardinalityLimits {
    pub fn max_descriptor_count(self) -> usize {
        self.max_descriptor_count
    }

    pub fn max_key_bytes(self) -> usize {
        self.max_key_bytes
    }

    /// The first limit `descriptors` exceed under `domain`, and its bound.
    pub fn exceeded(
        self,
        domain: &str,
        descriptors: &[Descriptor<'_>],
    ) -> Option<(CardinalityLimit, usize)> {
        if descriptors.len() > self.max_descriptor_count {
            return Some((CardinalityLimit::Count, self.max_descriptor_count));
        }
        let mut bytes = domain.len();
        for descriptor in descriptors {
            if descriptor.key.len() > self.max_key_bytes {
                return Some((CardinalityLimit::KeyBytes, self.max_key_bytes));
            }
            bytes = bytes
                .saturating_add(descriptor.key.len())
                .saturating_add(descriptor.value.len());
            if bytes > self.max_descriptor_bytes {
                return Some((CardinalityLimit::Bytes, self.max_descriptor_bytes));
            }
        }
        None
    }
}

/// Which part of the cardinality envelope a request exceeded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CardinalityLimit {
    /// More descriptors in one Envoy request than gabion evaluates (a build
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
    /// The `limit` log field and the setting that raises this limit (none for
    /// the build constant).
    pub fn describe(self) -> (&'static str, Option<Setting>) {
        match self {
            Self::RequestDescriptors => ("request_descriptors", None),
            Self::Count => ("count", Some(Setting::StorageMaxDescriptorCount)),
            Self::Bytes => ("bytes", Some(Setting::StorageMaxDescriptorBytes)),
            Self::KeyBytes => ("key_bytes", Some(Setting::StorageMaxKeyBytes)),
        }
    }
}

/// The operator's next step when a request exceeds this limit.
impl fmt::Display for CardinalityLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.describe().1 {
            Some(setting) => write!(f, "If the traffic is legitimate, raise {setting}."),
            None => write!(
                f,
                "gabion evaluates at most {STORAGE_MAX_DESCRIPTOR_COUNT} descriptors of one \
                 request, a build-time constant (`STORAGE_MAX_DESCRIPTOR_COUNT`) that config \
                 cannot raise; if the traffic is legitimate, have Envoy send fewer descriptors \
                 per request."
            ),
        }
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

// -- rate parsing & window/bucket resolution --------------------------------

const RATE_PERIOD_ERR: &str =
    "rate period must be `s`, `m`, `h`, `d`, or a duration like `30s`, `5m`";

/// Parse a `rate=` value into `(count, period)`.
///
/// The suffix after `r/` is either a single-letter unit (`s`, `m`, `h`,
/// `d`) or a [humantime] duration (`30s`, `5m`, `2h30m`). The count is
/// non-zero by type, and periods under 1 ms are rejected.
///
/// Shared by both adapters so two nodes with identical rule text agree on
/// what `rate=10r/s` means.
///
/// [humantime]: https://docs.rs/humantime
pub fn parse_rate(input: &str) -> Result<(NonZeroU64, Duration), &'static str> {
    let input = input.trim();
    let Some((number, period)) = input.split_once("r/") else {
        return Err(
            "expected `Nr/<unit>`, where unit is `s`, `m`, `h`, `d` or a duration (for example \
             `100r/s` or `10r/5m`)",
        );
    };
    let count: u64 = number.parse().map_err(|_| {
        "the request count before `r/` must be a whole number of at least 1 (for example `100r/s`)"
    })?;
    let count = NonZeroU64::new(count).ok_or(
        "the request count is 0, which would deny all traffic; use a whole number of at least 1, \
         or mode `disabled` to turn the rule off",
    )?;
    let duration = match period {
        "s" => Duration::from_secs(1),
        "m" => Duration::from_secs(60),
        "h" => Duration::from_secs(60 * 60),
        "d" => Duration::from_secs(60 * 60 * 24),
        other => humantime::parse_duration(other).map_err(|_| RATE_PERIOD_ERR)?,
    };
    if duration < Duration::from_millis(1) {
        return Err(
            "the rate's period is shorter than 1 ms, gabion's smallest time unit; use a period of \
             at least `1ms`",
        );
    }
    Ok((count, duration))
}

/// Resolved triple consumed by [`Rule::new`]. The limit has already been
/// scaled from the rate to fit the window so the CRDT representation
/// matches the operator's mental model exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedRate {
    pub limit: NonZeroU64,
    pub window_millis: u64,
    pub bucket_millis: u64,
}

/// Reasons [`resolve_rate`] can refuse a (rate, window, bucket) tuple. Each
/// variant's message answers what, why and what next.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RateResolveError {
    #[error(
        "the window is shorter than the rate's period, so the rate cannot be spread over it. To \
         enforce N requests in a shorter span, put the period in the rate itself (for example \
         `100r/500ms`)"
    )]
    WindowShorterThanPeriod,
    #[error(
        "the window is zero, so the rule could never admit a request. Omit the window to use the \
         rate's period; to turn a rule off, use mode `disabled`"
    )]
    ZeroWindow,
    #[error("the bucket is zero. `bucket` defaults to `window`; omit it for one bucket per window")]
    ZeroBucket,
    #[error(
        "the count multiplied by the window in milliseconds does not fit in 64 bits; usually a \
         count or window far larger than intended (extra zeros, or `y` where `s` was meant). Use \
         a smaller count or a shorter window"
    )]
    LimitOverflow,
}

/// Scale a `(rate_count, period)` rate up to a window-sized budget, applying
/// the operator-supplied `window` and `bucket` defaults documented in the
/// README.
///
/// * `window` defaults to `period` (preserves the historical "rate's period IS
///   the window" shape).
/// * `bucket` defaults to `window` (one fixed-window bucket — same shape as
///   before this surface existed).
/// * `limit = floor(rate_count * window_millis / period_millis)`, computed with
///   checked multiplication so an oversized count or window is a clean
///   `LimitOverflow` instead of wrap-around.
///
/// `window < period` is rejected: a window shorter than the rate's period has
/// no rate to scale down from; the operator writes the shorter period into
/// the rate instead.
pub fn resolve_rate(
    rate_count: NonZeroU64,
    period: Duration,
    window: Option<Duration>,
    bucket: Option<Duration>,
) -> Result<ResolvedRate, RateResolveError> {
    let window = window.unwrap_or(period);
    if window.is_zero() {
        return Err(RateResolveError::ZeroWindow);
    }
    if window < period {
        return Err(RateResolveError::WindowShorterThanPeriod);
    }
    let bucket = bucket.unwrap_or(window);
    if bucket.is_zero() {
        return Err(RateResolveError::ZeroBucket);
    }
    let window_millis = duration_to_millis(window);
    let period_millis = duration_to_millis(period).max(1);
    let bucket_millis = duration_to_millis(bucket);
    let limit = rate_count
        .get()
        .checked_mul(window_millis)
        .ok_or(RateResolveError::LimitOverflow)?
        / period_millis;
    // Only a window under 1 ms floors to a zero limit: from 1 ms up, the
    // window is at least the period, so the limit is at least `rate_count`.
    let limit = NonZeroU64::new(limit).ok_or(RateResolveError::ZeroWindow)?;
    Ok(ResolvedRate {
        limit,
        window_millis,
        bucket_millis,
    })
}

fn duration_to_millis(d: Duration) -> u64 {
    d.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
