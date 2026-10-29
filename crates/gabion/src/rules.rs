//! Rate-limit rule representation and stable hashing shared between
//! `gabion-server` (gRPC) and `gabion-nginx` (in-process RLM).
//!
//! Both adapters feed the gossip CRDT a `(rule_fingerprint, key_hash, bucket)`
//! tuple. Both build every rule through [`RuleDef::build`] and
//! [`RuleTable::new`], so they validate, default and identify rules one way:
//! two nodes with identical rules emit identical identifiers without
//! coordinating, whatever order either lists them in. This module also owns
//! the one cardinality check both adapters enforce on client-supplied
//! descriptors. Admission policy (decision codes, request mapping) is the
//! adapter's job — kept out of this module on purpose.

use std::fmt;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::time::Duration;

use thiserror::Error;
use twox_hash::xxhash3_128::{DEFAULT_SECRET_LENGTH, RawHasher, SecretBuffer};

pub use crate::crdt::KeyHash;
use crate::crdt::{BucketEpoch, MAX_DICTIONARY_CAPACITY, RuleDescriptor};
use crate::defaults::STORAGE_RULE_DICTIONARY_FLOOR;
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
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize),
    serde(rename_all = "snake_case")
)]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DescriptorPattern {
    pub key: Box<str>,
    pub value: Box<str>,
}

impl DescriptorPattern {
    fn matches(&self, descriptor: Descriptor<'_>) -> bool {
        &*self.key == descriptor.key && self.accepts(descriptor.value)
    }

    /// Whether `value` meets this pattern: any value for `*`, else only the
    /// literal.
    pub fn accepts(&self, value: &str) -> bool {
        &*self.value == "*" || &*self.value == value
    }
}

/// One rate-limit rule, built only by [`RuleDef::build`] and numbered only
/// by [`RuleTable::new`]; its fields are private so nothing can change a
/// rule after its identity is derived from them. Two nodes with identical
/// rules produce identical fingerprints.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Rule {
    name: Box<str>,
    domain: Box<str>,
    domain_hash: KeyHash,
    descriptors: Box<[DescriptorPattern]>,
    mode: EnforcementMode,
    #[cfg_attr(feature = "serde", serde(flatten))]
    spec: RuleSpec,
}

impl Rule {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn descriptors(&self) -> &[DescriptorPattern] {
        &self.descriptors
    }

    pub fn mode(&self) -> EnforcementMode {
        self.mode
    }

    /// Stable, cross-node identity: a hash of the domain, the descriptor
    /// patterns, the window and the bucket. The limit and the mode stay out,
    /// so changing either keeps the rule's counters.
    pub fn fingerprint(&self) -> u128 {
        self.spec.fingerprint
    }

    /// Hot-path-ready summary. O(1) copy.
    pub fn spec(&self) -> RuleSpec {
        self.spec
    }

    /// The rule as the cell store interns it. [`RuleDef::build`] keeps the
    /// window under 2^32 ms and the bucket no longer than the window, so both
    /// casts are lossless.
    pub fn descriptor(&self) -> RuleDescriptor {
        RuleDescriptor {
            fingerprint: self.spec.fingerprint,
            window_millis: self.spec.window_millis as u32,
            bucket_millis: self.spec.bucket_millis.get() as u32,
            limit: self.spec.limit,
            flags: 0,
            local_rule_id: self.spec.id,
        }
    }
}

/// Hot-path-ready summary of one rule. The per-request decision path holds
/// this by value (Copy) so neither adapter needs an O(N) lookup back into
/// the rule table by id. The bucket and the bucket count are non-zero by
/// type, so no summary can divide by a zero bucket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct RuleSpec {
    /// Position in its [`RuleTable`], from 1; assigned by [`RuleTable::new`].
    pub id: RuleId,
    #[cfg_attr(feature = "serde", serde(serialize_with = "crate::hex"))]
    pub fingerprint: u128,
    pub limit: u64,
    pub bucket_millis: NonZeroU64,
    pub window_millis: u64,
    pub live_buckets: NonZeroU32,
}

impl RuleSpec {
    /// The bucket epoch `now_millis` falls in: the one definition both
    /// adapters record into and the aggregate reads from.
    pub fn bucket_at(self, now_millis: u64) -> BucketEpoch {
        (now_millis / self.bucket_millis) as BucketEpoch
    }
}

/// One rule as an adapter writes it, before validation: the one input to
/// [`RuleDef::build`], which both adapters call.
#[derive(Clone, Copy, Debug)]
pub struct RuleDef<'a> {
    pub name: &'a str,
    pub domain: &'a str,
    /// Ordered `(key, value)` patterns; a value of `*` matches any value.
    pub patterns: &'a [(&'a str, &'a str)],
    /// `100r/s`, parsed by [`parse_rate`].
    pub rate: &'a str,
    /// Defaults to the rate's period.
    pub window: Option<Duration>,
    /// Defaults to the window.
    pub bucket: Option<Duration>,
    pub mode: EnforcementMode,
}

impl RuleDef<'_> {
    /// Validate the rule and derive its identity. Its id is assigned by
    /// [`RuleTable::new`].
    pub fn build(&self, limits: &CardinalityLimits) -> Result<Rule, RuleError> {
        let rule = || self.name.into();
        if let Some(reason) = domain_problem(self.domain) {
            return Err(RuleError::InvalidDomain {
                rule: rule(),
                reason,
            });
        }
        if self.patterns.is_empty() {
            return Err(RuleError::NoDescriptors { rule: rule() });
        }
        if self.patterns.len() > limits.max_descriptor_count {
            return Err(RuleError::TooManyDescriptors {
                rule: rule(),
                count: self.patterns.len(),
                max: limits.max_descriptor_count,
            });
        }
        for &(key, _) in self.patterns {
            if !is_descriptor_key(key) {
                return Err(RuleError::InvalidKey {
                    rule: rule(),
                    key: key.into(),
                });
            }
            if key.len() > limits.max_key_bytes {
                return Err(RuleError::KeyTooLong {
                    rule: rule(),
                    key: key.into(),
                    bytes: key.len(),
                    max: limits.max_key_bytes,
                });
            }
        }
        let (count, period) = parse_rate(self.rate).map_err(|reason| RuleError::Rate {
            rule: rule(),
            reason,
        })?;
        let resolved = resolve_rate(count, period, self.window, self.bucket).map_err(|source| {
            RuleError::Window {
                rule: rule(),
                source,
            }
        })?;
        let descriptors: Box<[DescriptorPattern]> = self
            .patterns
            .iter()
            .map(|&(key, value)| DescriptorPattern {
                key: key.into(),
                value: value.into(),
            })
            .collect();
        Ok(Rule {
            name: rule(),
            domain: self.domain.into(),
            domain_hash: hash_domain(self.domain),
            descriptors,
            mode: self.mode,
            spec: RuleSpec {
                id: 0,
                fingerprint: rule_fingerprint(
                    self.domain,
                    self.patterns,
                    resolved.window_millis,
                    resolved.bucket_millis.get(),
                ),
                limit: resolved.limit.get(),
                bucket_millis: resolved.bucket_millis,
                window_millis: resolved.window_millis,
                live_buckets: resolved.live_buckets,
            },
        })
    }
}

/// Why `domain` is not 1 to 255 bytes without whitespace or control
/// characters, or `None` when it is.
fn domain_problem(domain: &str) -> Option<&'static str> {
    if domain.is_empty() {
        Some("that is empty")
    } else if domain.len() > 255 {
        Some("longer than 255 bytes")
    } else if domain.chars().any(|c| c.is_whitespace() || c.is_control()) {
        Some("with whitespace or a control character")
    } else {
        None
    }
}

/// Every configured rule, in declaration order, and the capacity of the rule
/// dictionary that interns them. Built only by [`RuleTable::new`], so no two
/// rules share a name or an identity and the dictionary holds every rule.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize), serde(transparent))]
pub struct RuleTable {
    rules: Box<[Rule]>,
    #[cfg_attr(feature = "serde", serde(skip))]
    dictionary_capacity: NonZeroU16,
}

impl RuleTable {
    /// Number `rules` from 1 in order, and size the rule dictionary:
    /// `capacity` when `storage.rule_dictionary_capacity` sets it, else twice
    /// the rules and at least 256, leaving room for the rules peers gossip
    /// during a rollout.
    pub fn new(mut rules: Box<[Rule]>, capacity: Option<NonZeroU16>) -> Result<Self, RuleError> {
        let floor = const { NonZeroU16::new(STORAGE_RULE_DICTIONARY_FLOOR).unwrap() };
        let dictionary_capacity = capacity.unwrap_or_else(|| {
            NonZeroU16::new((2 * rules.len()).min(usize::from(MAX_DICTIONARY_CAPACITY)) as u16)
                .map_or(floor, |derived| derived.max(floor))
        });
        if rules.len() > usize::from(dictionary_capacity.get()) {
            return Err(RuleError::TooManyRules {
                rules: rules.len(),
                capacity: dictionary_capacity.get(),
            });
        }
        for (i, rule) in rules.iter().enumerate() {
            for earlier in &rules[..i] {
                if earlier.name == rule.name {
                    return Err(RuleError::DuplicateRuleName {
                        name: rule.name.clone(),
                    });
                }
                if earlier.fingerprint() == rule.fingerprint() {
                    return Err(RuleError::DuplicateRuleIdentity {
                        first: earlier.name.clone(),
                        second: rule.name.clone(),
                    });
                }
            }
        }
        for (i, rule) in rules.iter_mut().enumerate() {
            rule.spec.id = i as RuleId + 1;
        }
        Ok(Self {
            rules,
            dictionary_capacity,
        })
    }

    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }

    /// Slots in the rule dictionary, at least the number of rules.
    pub fn dictionary_capacity(&self) -> NonZeroU16 {
        self.dictionary_capacity
    }

    /// Rules whose `(domain, descriptor pattern)` matches the request shape,
    /// excluding any rule in [`EnforcementMode::Disabled`]. Rules in
    /// [`EnforcementMode::DryRun`] are returned so adapters can still hash
    /// the bucket and gossip the hit; adapters must check `rule.mode`
    /// before mapping a verdict to a rejection.
    ///
    /// `descriptors` is cloned once per rule, so an adapter passes an
    /// iterator over its request's entries in place rather than a buffer.
    pub fn matching<'r, 'd, I>(
        &'r self,
        domain: &str,
        descriptors: I,
    ) -> impl Iterator<Item = &'r Rule> + use<'r, 'd, I>
    where
        I: ExactSizeIterator<Item = Descriptor<'d>> + Clone,
    {
        let domain_hash = hash_domain(domain);
        self.rules.iter().filter(move |rule| {
            rule.mode != EnforcementMode::Disabled
                && rule.domain_hash == domain_hash
                && rule.descriptors.len() == descriptors.len()
                && rule
                    .descriptors
                    .iter()
                    .zip(descriptors.clone())
                    .all(|(pattern, descriptor)| pattern.matches(descriptor))
        })
    }
}

/// Match a descriptor key against `[A-Za-z_][A-Za-z0-9_.-]*`, the grammar
/// both adapters accept, so equal rules produce equal identifiers.
fn is_descriptor_key(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c == '.' || c == '-' || c.is_ascii_alphanumeric())
}

/// A rule [`RuleDef::build`] or [`RuleTable::new`] refused. Both adapters
/// print these at load: `gabiond` exits, `nginx -t` fails.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum RuleError {
    #[error("Two rules are named `{name}`. Rule names must be unique; rename one.")]
    DuplicateRuleName { name: Box<str> },
    #[error(
        "Rules `{first}` and `{second}` count the same keys with the same domain, descriptors, \
         window and bucket, so gabion cannot tell their counters apart. Give one of them a \
         different binding, window or bucket, or merge them."
    )]
    DuplicateRuleIdentity { first: Box<str>, second: Box<str> },
    #[error(
        "{rules} rate-limit rules are configured, but {setting} is {capacity}. Every configured \
         rule needs an entry, plus room for the rules other nodes gossip while a configuration \
         rolls out. Remove the setting to use the default (twice the configured rules, at least \
         256), or raise it to at least {wanted}.",
        setting = Setting::StorageRuleDictionaryCapacity,
        wanted = (2 * rules).min(usize::from(MAX_DICTIONARY_CAPACITY))
    )]
    TooManyRules { rules: usize, capacity: u16 },
    #[error(
        "Rule `{rule}` has a domain {reason}. A domain is 1 to 255 bytes with no whitespace or \
         control characters, the same in YAML and in `domain=`."
    )]
    InvalidDomain {
        rule: Box<str>,
        reason: &'static str,
    },
    #[error(
        "Rule `{rule}` has descriptor key `{key}`, but keys must match `[A-Za-z_][A-Za-z0-9_.-]*` \
         in both adapters (for example `tenant`, `app.tenant`). Rename the key."
    )]
    InvalidKey { rule: Box<str>, key: Box<str> },
    #[error(
        "Rule `{rule}` has no descriptors, so it would match every request in its domain. Add at \
         least one (`descriptors:` in YAML, a `$variable` binding in NGINX), or remove the rule."
    )]
    NoDescriptors { rule: Box<str> },
    #[error(
        "Rule `{rule}` has {count} descriptors, above {setting} ({max}). Remove descriptors from \
         the rule or raise that setting, which may go up to 65,535.",
        setting = Setting::StorageMaxDescriptorCount
    )]
    TooManyDescriptors {
        rule: Box<str>,
        count: usize,
        max: usize,
    },
    #[error(
        "Rule `{rule}` has descriptor key `{key}` of {bytes} bytes, above {setting} ({max}). \
         Shorten the key or raise that setting.",
        setting = Setting::StorageMaxKeyBytes
    )]
    KeyTooLong {
        rule: Box<str>,
        key: Box<str>,
        bytes: usize,
        max: usize,
    },
    #[error("Rule `{rule}` has an invalid rate: {reason}.")]
    Rate {
        rule: Box<str>,
        reason: &'static str,
    },
    #[error("Rule `{rule}`: {source}.")]
    Window {
        rule: Box<str>,
        #[source]
        source: RateResolveError,
    },
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
    pub(crate) max_request_descriptors: usize,
}

impl CardinalityLimits {
    pub fn max_descriptor_count(self) -> usize {
        self.max_descriptor_count
    }

    pub fn max_descriptor_bytes(self) -> usize {
        self.max_descriptor_bytes
    }

    pub fn max_request_descriptors(self) -> usize {
        self.max_request_descriptors
    }

    /// The first limit `descriptors` exceed under `domain`, and its bound.
    pub fn exceeded<'a>(
        self,
        domain: &str,
        descriptors: impl ExactSizeIterator<Item = Descriptor<'a>>,
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
    /// More descriptors in one Envoy request than `max_request_descriptors`.
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
    /// The `limit` log field and the setting that raises this limit.
    pub fn describe(self) -> (&'static str, Setting) {
        match self {
            Self::RequestDescriptors => {
                ("request_descriptors", Setting::StorageMaxRequestDescriptors)
            }
            Self::Count => ("count", Setting::StorageMaxDescriptorCount),
            Self::Bytes => ("bytes", Setting::StorageMaxDescriptorBytes),
            Self::KeyBytes => ("key_bytes", Setting::StorageMaxKeyBytes),
        }
    }
}

/// The operator's next step when a request exceeds this limit.
impl fmt::Display for CardinalityLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "If the traffic is legitimate, raise {}.",
            self.describe().1
        )
    }
}

// -- hashing ----------------------------------------------------------------

fn hash_domain(domain: &str) -> KeyHash {
    let mut hasher = stable_hasher();
    hasher.write(b"gabion.domain.v1");
    hasher.write(&[0]);
    hasher.write(domain.as_bytes());
    KeyHash(hasher.finish_128())
}

/// A key's hash, fed one descriptor at a time as an adapter reads them, so
/// neither adapter copies descriptors into a buffer to hash them. Rule order
/// and adapter never enter it: the fingerprint separates rules.
pub struct KeyHasher(RawHasher<[u8; DEFAULT_SECRET_LENGTH]>);

impl KeyHasher {
    pub fn new(domain: &str) -> Self {
        let mut hasher = stable_hasher();
        hasher.write(b"gabion.key.v2");
        hasher.write(&[0]);
        hasher.write(domain.as_bytes());
        hasher.write(&[0]);
        Self(hasher)
    }

    pub fn push(&mut self, key: &str, value: &str) {
        write_pair(&mut self.0, key, value);
    }

    pub fn finish(self) -> KeyHash {
        KeyHash(self.0.finish_128())
    }
}

/// [`KeyHasher`] over a whole request.
pub fn hash_key<'a>(
    domain: &str,
    descriptors: impl IntoIterator<Item = Descriptor<'a>>,
) -> KeyHash {
    let mut hasher = KeyHasher::new(domain);
    for descriptor in descriptors {
        hasher.push(descriptor.key, descriptor.value);
    }
    hasher.finish()
}

/// Canonical fingerprint of a rule — the cross-node identity gossip uses as
/// its key: the domain, the patterns, the window and the bucket.
fn rule_fingerprint(
    domain: &str,
    patterns: &[(&str, &str)],
    window_millis: u64,
    bucket_millis: u64,
) -> u128 {
    let mut hasher = stable_hasher();
    hasher.write(b"gabion.rule.v2");
    hasher.write(&[0]);
    hasher.write(domain.as_bytes());
    hasher.write(&[0]);
    for &(key, value) in patterns {
        write_pair(&mut hasher, key, value);
    }
    hasher.write(&window_millis.to_le_bytes());
    hasher.write(&bucket_millis.to_le_bytes());
    hasher.finish_128()
}

fn write_pair(hasher: &mut RawHasher<[u8; DEFAULT_SECRET_LENGTH]>, key: &str, value: &str) {
    hasher.write(key.as_bytes());
    hasher.write(&[0]);
    hasher.write(value.as_bytes());
    hasher.write(&[0xff]);
}

pub(crate) fn stable_hasher() -> RawHasher<[u8; DEFAULT_SECRET_LENGTH]> {
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

/// Resolved triple consumed by [`RuleDef::build`]. The limit has already
/// been scaled from the rate to fit the window so the CRDT representation
/// matches the operator's mental model exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedRate {
    limit: NonZeroU64,
    window_millis: u64,
    bucket_millis: NonZeroU64,
    live_buckets: NonZeroU32,
}

/// Reasons [`RuleDef::build`] can refuse a rule's (rate, window, bucket). Each
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
    #[error(
        "the bucket is shorter than 1 ms, gabion's smallest time unit. `bucket` defaults to \
         `window`; omit it for one bucket per window"
    )]
    ZeroBucket,
    #[error(
        "the bucket is longer than the window, so a counted hit would outlive the window it \
         counts toward. Use a bucket no longer than the window, or omit `bucket` for one bucket \
         per window"
    )]
    BucketLongerThanWindow,
    #[error(
        "the window is longer than gabion tracks: under 49 days and fewer than 2,147,483,648 \
         buckets. Use a shorter window or a larger bucket"
    )]
    WindowTooLong,
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
fn resolve_rate(
    rate_count: NonZeroU64,
    period: Duration,
    window: Option<Duration>,
    bucket: Option<Duration>,
) -> Result<ResolvedRate, RateResolveError> {
    let window = window.unwrap_or(period);
    let window_millis = duration_to_millis(window);
    // A window under 1 ms would floor to a zero limit.
    if window_millis == 0 {
        return Err(RateResolveError::ZeroWindow);
    }
    if window < period {
        return Err(RateResolveError::WindowShorterThanPeriod);
    }
    let period_millis = duration_to_millis(period).max(1);
    let bucket_millis = NonZeroU64::new(duration_to_millis(bucket.unwrap_or(window)))
        .ok_or(RateResolveError::ZeroBucket)?;
    if bucket_millis.get() > window_millis {
        return Err(RateResolveError::BucketLongerThanWindow);
    }
    let live_buckets = window_millis.div_ceil(bucket_millis.get());
    // Epochs are `u32` and compared by serial-number arithmetic, which holds
    // while a window spans fewer than 2^31 buckets.
    if window_millis > u64::from(u32::MAX) || live_buckets >= 1 << 31 {
        return Err(RateResolveError::WindowTooLong);
    }
    let limit = rate_count
        .get()
        .checked_mul(window_millis)
        .ok_or(RateResolveError::LimitOverflow)?
        / period_millis;
    // From 1 ms up the window is at least the period, so the limit is at
    // least `rate_count`.
    let limit = NonZeroU64::new(limit).ok_or(RateResolveError::ZeroWindow)?;
    // A window of at least 1 ms holds at least one bucket.
    let live_buckets = NonZeroU32::new(live_buckets as u32).ok_or(RateResolveError::ZeroWindow)?;
    Ok(ResolvedRate {
        limit,
        window_millis,
        bucket_millis,
        live_buckets,
    })
}

fn duration_to_millis(d: Duration) -> u64 {
    d.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
