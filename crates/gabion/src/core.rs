//! Core rate-limit engine.
//!
//! Invariants:
//! - Request admission never depends on gossip, network, Kubernetes, or
//!   wall-clock I/O.
//! - `local_window_total` equals the sum of live local buckets for a key.
//! - `estimated_window_total` equals local bucket counts plus accepted remote
//!   deltas.
//! - Fresh global decisions never exceed `local_absolute_limit`.
//! - Stale global decisions never exceed `local_fallback_limit`.
//! - Overflow policies never grow storage beyond configured key and cell
//!   capacity.
//! - Descriptor matching is deterministic for exact keys and wildcard values.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use twox_hash::xxhash3_128::{DEFAULT_SECRET_LENGTH, RawHasher as XxHash3RawHasher, SecretBuffer};

pub type RuleId = u32;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum Decision {
    Allow,
    Reject(RejectReason),
}

impl Decision {
    pub fn is_reject(self) -> bool {
        matches!(self, Self::Reject(_))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum RejectReason {
    LocalAbsoluteLimit,
    GlobalLimit,
    LocalFallbackLimit,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnforcementMode {
    Enforce,
    Disabled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    #[serde(alias = "aggregate")]
    UseOverflowKey,
    #[serde(alias = "allow")]
    AllowUntracked,
    Reject,
    Sample,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SafetyMargin {
    pub hits: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct WindowSpec {
    pub size_millis: u64,
    pub bucket_count: usize,
}

impl WindowSpec {
    pub fn bucket_millis(self) -> u64 {
        self.size_millis
            .max(1)
            .div_ceil(self.bucket_count.max(1) as u64)
            .max(1)
    }

    fn bucket_start(self, now_millis: u64) -> u64 {
        let bucket = self.bucket_millis();
        now_millis - (now_millis % bucket)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DescriptorMatcher {
    patterns: Vec<DescriptorPattern>,
}

impl DescriptorMatcher {
    pub fn exact_keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            patterns: keys
                .into_iter()
                .map(|key| DescriptorPattern {
                    key: key.into(),
                    value: ValueMatcher::Any,
                })
                .collect(),
        }
    }

    pub fn exact(
        descriptors: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        Self {
            patterns: descriptors
                .into_iter()
                .map(|(key, value)| {
                    let value = value.into();
                    DescriptorPattern {
                        key: key.into(),
                        value: if value == "*" {
                            ValueMatcher::Any
                        } else {
                            ValueMatcher::Exact(value)
                        },
                    }
                })
                .collect(),
        }
    }

    fn matches(&self, descriptors: &[Descriptor<'_>]) -> bool {
        self.patterns.len() == descriptors.len()
            && self
                .patterns
                .iter()
                .zip(descriptors)
                .all(|(expected, actual)| expected.matches(*actual))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct DescriptorPattern {
    key: String,
    value: ValueMatcher,
}

impl DescriptorPattern {
    fn matches(&self, descriptor: Descriptor<'_>) -> bool {
        self.key == descriptor.key && self.value.matches(descriptor.value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
enum ValueMatcher {
    Any,
    Exact(String),
}

impl ValueMatcher {
    fn matches(&self, value: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(expected) => expected == value,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Rule {
    pub id: RuleId,
    pub domain_hash: KeyHash,
    pub descriptor_matcher: DescriptorMatcher,
    pub limit: u64,
    pub window: WindowSpec,
    pub local_fallback_limit: u64,
    pub local_absolute_limit: u64,
    pub stale_after_millis: u64,
    pub safety_margin: SafetyMargin,
    pub overflow_policy: OverflowPolicy,
    pub mode: EnforcementMode,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct RuleTable {
    rules: Vec<Rule>,
}

impl RuleTable {
    pub fn new(rules: Vec<Rule>) -> Self {
        Self { rules }
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

    pub fn matching<'a>(
        &'a self,
        request: &'a LimitRequest<'a>,
    ) -> impl Iterator<Item = &'a Rule> + 'a {
        let domain_hash = hash_domain(request.domain);
        self.rules.iter().filter(move |rule| {
            rule.mode == EnforcementMode::Enforce
                && rule.domain_hash == domain_hash
                && rule.descriptor_matcher.matches(request.descriptors)
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Descriptor<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LimitRequest<'a> {
    pub domain: &'a str,
    pub descriptors: &'a [Descriptor<'a>],
    pub hits: u64,
}

impl LimitRequest<'_> {
    fn hits(self) -> u64 {
        self.hits.max(1)
    }

    pub fn validate_cardinality(self, limits: CardinalityLimits) -> Result<(), CardinalityError> {
        if self.descriptors.len() > limits.max_descriptor_count {
            return Err(CardinalityError::DescriptorCount);
        }

        let mut bytes = self.domain.len();
        for descriptor in self.descriptors {
            if descriptor.key.len() > limits.max_key_bytes {
                return Err(CardinalityError::KeyBytes);
            }
            bytes = bytes.saturating_add(descriptor.key.len());
            bytes = bytes.saturating_add(descriptor.value.len());
            if bytes > limits.max_descriptor_bytes {
                return Err(CardinalityError::DescriptorBytes);
            }
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HashedLimitRequest {
    rule_id: RuleId,
    key_hash: KeyHash,
    hits: u64,
}

impl HashedLimitRequest {
    pub fn new(rule_id: RuleId, key_hash: impl Into<KeyHash>, hits: u64) -> Self {
        Self {
            rule_id,
            key_hash: key_hash.into(),
            hits,
        }
    }

    pub fn rule_id(self) -> RuleId {
        self.rule_id
    }

    pub fn key_hash(self) -> KeyHash {
        self.key_hash
    }

    pub fn hits(self) -> u64 {
        self.hits.max(1)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimedHashedLimitRequest {
    request: HashedLimitRequest,
    now_millis: u64,
}

impl TimedHashedLimitRequest {
    pub fn new(request: HashedLimitRequest, now_millis: u64) -> Self {
        Self {
            request,
            now_millis,
        }
    }

    pub fn request(self) -> HashedLimitRequest {
        self.request
    }

    pub fn now_millis(self) -> u64 {
        self.now_millis
    }
}

#[derive(Clone)]
pub struct HashedLimitRequestBuilder {
    rule_id: RuleId,
    hits: u64,
    hash: StableKeyHasher,
}

impl HashedLimitRequestBuilder {
    pub fn new(rule_id: RuleId, hits: u64) -> Self {
        let mut hash = StableKeyHasher::new();
        hash.write_number(u64::from(rule_id));
        Self {
            rule_id,
            hits,
            hash,
        }
    }

    pub fn push_component(&mut self, key: &[u8], value: &[u8]) {
        self.hash.write_bytes(key);
        self.hash.write_bytes(&[0]);
        self.hash.write_bytes(value);
        self.hash.write_bytes(&[0xff]);
    }

    pub fn finish(self) -> HashedLimitRequest {
        HashedLimitRequest::new(self.rule_id, self.hash.finish(), self.hits)
    }
}

#[derive(Clone)]
struct StableKeyHasher {
    inner: XxHash3RawHasher<[u8; DEFAULT_SECRET_LENGTH]>,
}

impl StableKeyHasher {
    fn new() -> Self {
        let secret =
            SecretBuffer::new(0, [0x9d; DEFAULT_SECRET_LENGTH]).expect("valid XXH3 secret length");
        Self {
            inner: XxHash3RawHasher::new(secret),
        }
    }

    fn write_number(&mut self, value: u64) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        self.inner.write(bytes);
    }

    fn finish(&self) -> u128 {
        self.inner.finish_128()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CardinalityLimits {
    pub max_descriptor_count: usize,
    pub max_descriptor_bytes: usize,
    pub max_key_bytes: usize,
}

impl Default for CardinalityLimits {
    fn default() -> Self {
        Self {
            max_descriptor_count: 16,
            max_descriptor_bytes: 512,
            max_key_bytes: 128,
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CardinalityError {
    #[error("too many descriptors")]
    DescriptorCount,
    #[error("descriptor bytes exceeded")]
    DescriptorBytes,
    #[error("descriptor key bytes exceeded")]
    KeyBytes,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct Metrics {
    pub requests: u64,
    pub allowed: u64,
    pub rejected: u64,
    pub local_absolute_rejected: u64,
    pub global_estimate_rejected: u64,
    pub local_fallback_rejected: u64,
    pub overflow_key_uses: u64,
    pub overflow_rejected: u64,
    pub overflow_untracked: u64,
    pub overflow_sampled: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct KeyHash(u128);

impl KeyHash {
    pub fn value(self) -> u128 {
        self.0
    }

    fn index_bits(self) -> u64 {
        (self.0 >> 64) as u64 ^ self.0 as u64
    }

    fn sampled(self) -> bool {
        self.index_bits() & 1 == 0
    }
}

impl From<u128> for KeyHash {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

impl From<KeyHash> for u128 {
    fn from(value: KeyHash) -> Self {
        value.0
    }
}

pub fn hash_domain(domain: &str) -> KeyHash {
    let mut hasher = StableKeyHasher::new();
    hasher.write_bytes(domain.as_bytes());
    KeyHash::from(hasher.finish())
}

pub fn hash_key(rule_id: RuleId, request: &LimitRequest<'_>) -> KeyHash {
    let mut hash = StableKeyHasher::new();

    hash.write_number(u64::from(rule_id));
    hash.write_bytes(request.domain.as_bytes());
    hash.write_bytes(&[0]);

    for descriptor in request.descriptors {
        hash.write_bytes(descriptor.key.as_bytes());
        hash.write_bytes(&[0]);
        hash.write_bytes(descriptor.value.as_bytes());
        hash.write_bytes(&[0xff]);
    }

    KeyHash::from(hash.finish())
}

#[derive(Clone, Copy, Debug)]
struct BucketSlot {
    bucket_start_millis: u64,
    local_count: u64,
    estimated_total: u64,
}

impl BucketSlot {
    fn empty() -> Self {
        Self {
            bucket_start_millis: u64::MAX,
            local_count: 0,
            estimated_total: 0,
        }
    }
}

#[derive(Clone, Debug)]
struct KeyEntry {
    occupied: bool,
    rule_id: RuleId,
    key_hash: KeyHash,
    last_seen_millis: u64,
    local_window_total: u64,
    estimated_window_total: u64,
    buckets: Vec<BucketSlot>,
}

impl KeyEntry {
    fn empty(bucket_count: usize) -> Self {
        Self {
            occupied: false,
            rule_id: 0,
            key_hash: KeyHash::default(),
            last_seen_millis: 0,
            local_window_total: 0,
            estimated_window_total: 0,
            buckets: vec![BucketSlot::empty(); bucket_count],
        }
    }

    fn reset(&mut self, rule_id: RuleId, key_hash: KeyHash, now_millis: u64) {
        self.occupied = true;
        self.rule_id = rule_id;
        self.key_hash = key_hash;
        self.last_seen_millis = now_millis;
        self.local_window_total = 0;
        self.estimated_window_total = 0;
        for bucket in &mut self.buckets {
            *bucket = BucketSlot::empty();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct KeyHandle {
    index: usize,
    overflow: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyAdmission {
    Tracked(KeyHandle),
    AllowUntracked,
    Reject,
}

#[derive(Debug)]
pub struct HeapStore {
    entries: Vec<KeyEntry>,
    overflow_entries: Vec<KeyEntry>,
    bucket_count: usize,
    metrics: Metrics,
}

impl HeapStore {
    pub fn with_capacity(max_keys: usize, bucket_count: usize, rules: &RuleTable) -> Self {
        let bucket_count = bucket_count.max(1);
        let entries = (0..max_keys)
            .map(|_| KeyEntry::empty(bucket_count))
            .collect();
        let mut overflow_entries = Vec::with_capacity(rules.rules.len());
        for rule in rules.iter() {
            let mut entry = KeyEntry::empty(bucket_count);
            entry.reset(
                rule.id,
                KeyHash::from((u128::from(u64::MAX) << 64) | u128::from(rule.id)),
                0,
            );
            overflow_entries.push(entry);
        }

        Self {
            entries,
            overflow_entries,
            bucket_count,
            metrics: Metrics::default(),
        }
    }

    pub fn metrics(&self) -> Metrics {
        self.metrics
    }

    pub fn active_keys(&self) -> usize {
        self.entries.iter().filter(|entry| entry.occupied).count()
    }

    fn get_or_insert_key(
        &mut self,
        rule: &Rule,
        key_hash: KeyHash,
        now_millis: u64,
    ) -> KeyAdmission {
        match self.find_slot(rule.id, key_hash) {
            SlotSearch::Found(index) => {
                self.entries[index].last_seen_millis = now_millis;
                KeyAdmission::Tracked(KeyHandle {
                    index,
                    overflow: false,
                })
            }
            SlotSearch::Vacant(index) => {
                self.entries[index].reset(rule.id, key_hash, now_millis);
                KeyAdmission::Tracked(KeyHandle {
                    index,
                    overflow: false,
                })
            }
            SlotSearch::Full => match rule.overflow_policy {
                OverflowPolicy::UseOverflowKey => {
                    self.metrics.overflow_key_uses =
                        self.metrics.overflow_key_uses.saturating_add(1);
                    self.overflow_key(rule.id)
                        .map(KeyAdmission::Tracked)
                        .unwrap_or(KeyAdmission::Reject)
                }
                OverflowPolicy::AllowUntracked => {
                    self.metrics.overflow_untracked =
                        self.metrics.overflow_untracked.saturating_add(1);
                    KeyAdmission::AllowUntracked
                }
                OverflowPolicy::Reject => {
                    self.metrics.overflow_rejected =
                        self.metrics.overflow_rejected.saturating_add(1);
                    KeyAdmission::Reject
                }
                OverflowPolicy::Sample => {
                    if key_hash.sampled() {
                        self.metrics.overflow_sampled =
                            self.metrics.overflow_sampled.saturating_add(1);
                        self.overflow_key(rule.id)
                            .map(KeyAdmission::Tracked)
                            .unwrap_or(KeyAdmission::AllowUntracked)
                    } else {
                        self.metrics.overflow_untracked =
                            self.metrics.overflow_untracked.saturating_add(1);
                        KeyAdmission::AllowUntracked
                    }
                }
            },
        }
    }

    fn key_for_record(&self, rule: &Rule, key_hash: KeyHash) -> KeyAdmission {
        match self.find_slot(rule.id, key_hash) {
            SlotSearch::Found(index) => KeyAdmission::Tracked(KeyHandle {
                index,
                overflow: false,
            }),
            SlotSearch::Vacant(_) => KeyAdmission::AllowUntracked,
            SlotSearch::Full => match rule.overflow_policy {
                OverflowPolicy::UseOverflowKey => self
                    .overflow_key(rule.id)
                    .map(KeyAdmission::Tracked)
                    .unwrap_or(KeyAdmission::Reject),
                OverflowPolicy::AllowUntracked => KeyAdmission::AllowUntracked,
                OverflowPolicy::Reject => KeyAdmission::Reject,
                OverflowPolicy::Sample => {
                    if key_hash.sampled() {
                        self.overflow_key(rule.id)
                            .map(KeyAdmission::Tracked)
                            .unwrap_or(KeyAdmission::AllowUntracked)
                    } else {
                        KeyAdmission::AllowUntracked
                    }
                }
            },
        }
    }

    fn overflow_key(&self, rule_id: RuleId) -> Option<KeyHandle> {
        let index = self
            .overflow_entries
            .iter()
            .position(|entry| entry.rule_id == rule_id)?;
        Some(KeyHandle {
            index,
            overflow: true,
        })
    }

    fn find_slot(&self, rule_id: RuleId, key_hash: KeyHash) -> SlotSearch {
        if self.entries.is_empty() {
            return SlotSearch::Full;
        }

        let start = key_hash.index_bits() as usize % self.entries.len();
        for probe in 0..self.entries.len() {
            let index = (start + probe) % self.entries.len();
            let entry = &self.entries[index];
            if !entry.occupied {
                return SlotSearch::Vacant(index);
            }
            if entry.rule_id == rule_id && entry.key_hash == key_hash {
                return SlotSearch::Found(index);
            }
        }
        SlotSearch::Full
    }

    fn rotate_if_needed(&mut self, handle: KeyHandle, rule: &Rule, now_millis: u64) {
        let entry = self.entry_mut(handle);
        expire_old_buckets(entry, rule.window, now_millis);
    }

    fn local_window_total(&self, handle: KeyHandle) -> u64 {
        self.entry(handle).local_window_total
    }

    fn estimated_window_total(&self, handle: KeyHandle) -> u64 {
        self.entry(handle).estimated_window_total
    }

    fn key_hash(&self, handle: KeyHandle) -> KeyHash {
        self.entry(handle).key_hash
    }

    fn increment_local(&mut self, handle: KeyHandle, rule: &Rule, now_millis: u64, hits: u64) {
        let bucket_count = self.bucket_count;
        let entry = self.entry_mut(handle);
        expire_old_buckets(entry, rule.window, now_millis);

        let bucket_start = rule.window.bucket_start(now_millis);
        let bucket_index = ((bucket_start / rule.window.bucket_millis()) as usize) % bucket_count;
        let bucket = &mut entry.buckets[bucket_index];
        if bucket.bucket_start_millis != bucket_start {
            entry.local_window_total = entry.local_window_total.saturating_sub(bucket.local_count);
            entry.estimated_window_total = entry
                .estimated_window_total
                .saturating_sub(bucket.estimated_total);
            *bucket = BucketSlot {
                bucket_start_millis: bucket_start,
                local_count: 0,
                estimated_total: 0,
            };
        }

        bucket.local_count = bucket.local_count.saturating_add(hits);
        bucket.estimated_total = bucket.estimated_total.saturating_add(hits);
        entry.local_window_total = entry.local_window_total.saturating_add(hits);
        entry.estimated_window_total = entry.estimated_window_total.saturating_add(hits);
        entry.last_seen_millis = now_millis;
    }

    fn add_remote_estimate(
        &mut self,
        rule: &Rule,
        key_hash: KeyHash,
        bucket_start_millis: u64,
        now_millis: u64,
        delta: u64,
    ) {
        let key = match self.get_or_insert_key(rule, key_hash, now_millis) {
            KeyAdmission::Tracked(key) => key,
            KeyAdmission::AllowUntracked | KeyAdmission::Reject => match self.overflow_key(rule.id)
            {
                Some(key) => key,
                None => return,
            },
        };
        self.rotate_if_needed(key, rule, now_millis);

        let bucket_count = self.bucket_count;
        let entry = self.entry_mut(key);
        let bucket_index =
            ((bucket_start_millis / rule.window.bucket_millis()) as usize) % bucket_count;
        let bucket = &mut entry.buckets[bucket_index];
        if bucket.bucket_start_millis != bucket_start_millis {
            entry.local_window_total = entry.local_window_total.saturating_sub(bucket.local_count);
            entry.estimated_window_total = entry
                .estimated_window_total
                .saturating_sub(bucket.estimated_total);
            *bucket = BucketSlot {
                bucket_start_millis,
                local_count: 0,
                estimated_total: 0,
            };
        }

        bucket.estimated_total = bucket.estimated_total.saturating_add(delta);
        entry.estimated_window_total = entry.estimated_window_total.saturating_add(delta);
        entry.last_seen_millis = now_millis;
    }

    fn entry(&self, handle: KeyHandle) -> &KeyEntry {
        if handle.overflow {
            &self.overflow_entries[handle.index]
        } else {
            &self.entries[handle.index]
        }
    }

    fn entry_mut(&mut self, handle: KeyHandle) -> &mut KeyEntry {
        if handle.overflow {
            &mut self.overflow_entries[handle.index]
        } else {
            &mut self.entries[handle.index]
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotSearch {
    Found(usize),
    Vacant(usize),
    Full,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Serialize)]
pub struct NodeId(u128);

impl NodeId {
    pub fn value(self) -> u128 {
        self.0
    }
}

impl From<u128> for NodeId {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

impl From<NodeId> for u128 {
    fn from(value: NodeId) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct NodeIdentity {
    pub node_id: NodeId,
    pub incarnation: u64,
}

impl Default for NodeIdentity {
    fn default() -> Self {
        Self {
            node_id: NodeId::from(1_u128),
            incarnation: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
pub struct CounterCell {
    pub rule_id: RuleId,
    pub key_hash: KeyHash,
    pub bucket_start_millis: u64,
    pub origin_node_id: NodeId,
    pub origin_incarnation: u64,
    pub count: u64,
    pub last_update_millis: u64,
    pub sequence: u64,
}

pub trait RateLimitRecorder<Request> {
    type Decision;

    fn record_at(&self, request: Request, now_millis: u64) -> Self::Decision;
}

pub trait RateLimitRuntime<Request>: RateLimitRecorder<Request> {
    fn shutdown(&self);
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct StorageSummary {
    pub active_keys: usize,
    pub active_cells: usize,
    pub max_keys: usize,
    pub max_cells: usize,
    pub dirty_ring_len: usize,
    pub dirty_ring_capacity: usize,
    pub dirty_overflow: bool,
    pub bucket_count: usize,
    pub estimated_memory_bytes: usize,
}

impl CounterCell {
    fn local(
        rule_id: RuleId,
        key_hash: KeyHash,
        bucket_start_millis: u64,
        identity: NodeIdentity,
        hits: u64,
        now_millis: u64,
    ) -> Self {
        Self {
            rule_id,
            key_hash,
            bucket_start_millis,
            origin_node_id: identity.node_id,
            origin_incarnation: identity.incarnation,
            count: hits,
            last_update_millis: now_millis,
            sequence: 0,
        }
    }

    fn same_identity(self, other: Self) -> bool {
        self.rule_id == other.rule_id
            && self.key_hash == other.key_hash
            && self.bucket_start_millis == other.bucket_start_millis
            && self.origin_node_id == other.origin_node_id
            && self.origin_incarnation == other.origin_incarnation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirtyEntry {
    pub cell_index: usize,
    pub sequence: u64,
}

#[derive(Clone, Debug)]
struct DirtyRing {
    entries: Vec<Option<DirtyEntry>>,
    next: usize,
    len: usize,
    overflowed: bool,
}

impl DirtyRing {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: vec![None; capacity],
            next: 0,
            len: 0,
            overflowed: false,
        }
    }

    fn push(&mut self, entry: DirtyEntry) {
        if self.entries.is_empty() {
            self.overflowed = true;
            return;
        }
        if self.len == self.entries.len() {
            self.overflowed = true;
        } else {
            self.len += 1;
        }
        self.entries[self.next] = Some(entry);
        self.next = (self.next + 1) % self.entries.len();
    }

    fn iter(&self) -> impl Iterator<Item = DirtyEntry> + '_ {
        let len = self.len;
        let start = if len == self.entries.len() {
            self.next
        } else {
            0
        };
        (0..len).filter_map(move |offset| self.entries[(start + offset) % self.entries.len()])
    }

    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.entries.len()
    }
}

#[derive(Clone, Debug)]
struct LocalCellTable {
    cells: Vec<Option<CounterCell>>,
    active: usize,
    next_sequence: u64,
    dirty: DirtyRing,
}

impl LocalCellTable {
    fn with_capacity(max_cells: usize, dirty_capacity: usize) -> Self {
        Self {
            cells: vec![None; max_cells],
            active: 0,
            next_sequence: 0,
            dirty: DirtyRing::with_capacity(dirty_capacity),
        }
    }

    fn active_cell_count(&self) -> usize {
        self.active
    }

    fn capacity(&self) -> usize {
        self.cells.len()
    }

    fn dirty_len(&self) -> usize {
        self.dirty.len()
    }

    fn dirty_capacity(&self) -> usize {
        self.dirty.capacity()
    }

    fn dirty_overflowed(&self) -> bool {
        self.dirty.overflowed
    }

    fn cells(&self) -> impl Iterator<Item = CounterCell> + '_ {
        self.cells.iter().filter_map(|cell| *cell)
    }

    fn dirty_cells(&self) -> impl Iterator<Item = CounterCell> + '_ {
        self.dirty
            .iter()
            .filter_map(|dirty| self.cells.get(dirty.cell_index).and_then(|cell| *cell))
    }

    fn upsert_local(&mut self, incoming: CounterCell) -> Option<CounterCell> {
        if self.cells.is_empty() {
            self.dirty.overflowed = true;
            return None;
        }

        if let Some(index) = self.find_cell(incoming) {
            let Some(stored) = self.cells.get_mut(index).and_then(Option::as_mut) else {
                return None;
            };
            stored.count = stored.count.saturating_add(incoming.count);
            stored.last_update_millis = incoming.last_update_millis;
            self.next_sequence = self.next_sequence.saturating_add(1);
            stored.sequence = self.next_sequence;
            self.dirty.push(DirtyEntry {
                cell_index: index,
                sequence: stored.sequence,
            });
            return Some(*stored);
        }

        let Some(index) = self.cells.iter().position(Option::is_none) else {
            self.dirty.overflowed = true;
            return None;
        };

        self.next_sequence = self.next_sequence.saturating_add(1);
        let mut cell = incoming;
        cell.sequence = self.next_sequence;
        self.cells[index] = Some(cell);
        self.active += 1;
        self.dirty.push(DirtyEntry {
            cell_index: index,
            sequence: cell.sequence,
        });
        Some(cell)
    }

    fn find_cell(&self, incoming: CounterCell) -> Option<usize> {
        self.cells.iter().enumerate().find_map(|(index, cell)| {
            cell.and_then(|cell| {
                if cell.same_identity(incoming) {
                    Some(index)
                } else {
                    None
                }
            })
        })
    }
}

#[derive(Clone, Debug)]
struct FreshnessTable {
    entries: Vec<FreshnessEntry>,
}

impl FreshnessTable {
    fn new(rules: &RuleTable) -> Self {
        Self {
            entries: rules
                .iter()
                .map(|rule| FreshnessEntry {
                    rule_id: rule.id,
                    last_update_millis: None,
                })
                .collect(),
        }
    }

    fn mark_updated(&mut self, rule_id: RuleId, now_millis: u64) {
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.rule_id == rule_id)
        {
            entry.last_update_millis = Some(now_millis);
        }
    }

    fn is_fresh(&self, rule: &Rule, now_millis: u64) -> bool {
        self.entries
            .iter()
            .find(|entry| entry.rule_id == rule.id)
            .and_then(|entry| entry.last_update_millis)
            .map(|last| now_millis.saturating_sub(last) <= rule.stale_after_millis)
            .unwrap_or(false)
    }
}

#[derive(Clone, Copy, Debug)]
struct FreshnessEntry {
    rule_id: RuleId,
    last_update_millis: Option<u64>,
}

#[derive(Debug)]
pub struct LocalEngine {
    rules: RuleTable,
    store: HeapStore,
    cells: LocalCellTable,
    freshness: FreshnessTable,
    identity: NodeIdentity,
}

impl LocalEngine {
    pub fn new(rules: RuleTable, max_keys: usize, bucket_count: usize) -> Self {
        let max_cells = max_keys.saturating_mul(bucket_count.max(1));
        Self::with_identity(
            rules,
            max_keys,
            bucket_count,
            max_cells,
            max_cells,
            NodeIdentity::default(),
        )
    }

    pub fn with_identity(
        rules: RuleTable,
        max_keys: usize,
        bucket_count: usize,
        max_cells: usize,
        dirty_capacity: usize,
        identity: NodeIdentity,
    ) -> Self {
        let freshness = FreshnessTable::new(&rules);
        Self {
            store: HeapStore::with_capacity(max_keys, bucket_count, &rules),
            cells: LocalCellTable::with_capacity(max_cells, dirty_capacity),
            freshness,
            rules,
            identity,
        }
    }

    pub fn rules(&self) -> &RuleTable {
        &self.rules
    }

    pub fn mark_global_estimate_updated(&mut self, now_millis: u64) {
        for rule in self.rules.iter() {
            self.freshness.mark_updated(rule.id, now_millis);
        }
    }

    pub fn mark_rule_estimate_updated(&mut self, rule_id: RuleId, now_millis: u64) {
        self.freshness.mark_updated(rule_id, now_millis);
    }

    pub fn metrics(&self) -> Metrics {
        self.store.metrics()
    }

    pub fn active_keys(&self) -> usize {
        self.store.active_keys()
    }

    pub fn active_cells(&self) -> usize {
        self.cells.active_cell_count()
    }

    pub fn dirty_overflowed(&self) -> bool {
        self.cells.dirty_overflowed()
    }

    pub fn cells(&self) -> impl Iterator<Item = CounterCell> + '_ {
        self.cells.cells()
    }

    pub fn dirty_cells(&self) -> impl Iterator<Item = CounterCell> + '_ {
        self.cells.dirty_cells()
    }

    pub fn identity(&self) -> NodeIdentity {
        self.identity
    }

    pub fn storage_summary(&self) -> StorageSummary {
        let max_keys = self.store.entries.len();
        let max_cells = self.cells.capacity();
        let estimated_memory_bytes = std::mem::size_of::<Self>()
            .saturating_add(self.store.entries.capacity() * std::mem::size_of::<KeyEntry>())
            .saturating_add(
                self.store.overflow_entries.capacity() * std::mem::size_of::<KeyEntry>(),
            )
            .saturating_add(
                max_keys
                    .saturating_add(self.store.overflow_entries.len())
                    .saturating_mul(self.store.bucket_count)
                    .saturating_mul(std::mem::size_of::<BucketSlot>()),
            )
            .saturating_add(self.cells.capacity() * std::mem::size_of::<Option<CounterCell>>())
            .saturating_add(
                self.cells.dirty_capacity() * std::mem::size_of::<Option<DirtyEntry>>(),
            );

        StorageSummary {
            active_keys: self.active_keys(),
            active_cells: self.active_cells(),
            max_keys,
            max_cells,
            dirty_ring_len: self.cells.dirty_len(),
            dirty_ring_capacity: self.cells.dirty_capacity(),
            dirty_overflow: self.dirty_overflowed(),
            bucket_count: self.store.bucket_count,
            estimated_memory_bytes,
        }
    }

    pub fn add_remote_estimate(
        &mut self,
        rule_id: RuleId,
        key_hash: KeyHash,
        bucket_start_millis: u64,
        now_millis: u64,
        delta: u64,
    ) -> bool {
        let Some(rule) = self.rules.iter().find(|rule| rule.id == rule_id) else {
            return false;
        };
        self.store
            .add_remote_estimate(rule, key_hash, bucket_start_millis, now_millis, delta);
        self.mark_rule_estimate_updated(rule_id, now_millis);
        true
    }

    pub fn check_and_record(&mut self, request: LimitRequest<'_>, now_millis: u64) -> Decision {
        self.store.metrics.requests = self.store.metrics.requests.saturating_add(1);
        let hits = request.hits();
        let rules = &self.rules;
        let store = &mut self.store;
        let freshness = &self.freshness;

        for rule in rules.matching(&request) {
            let key_hash = hash_key(rule.id, &request);
            let key = match store.get_or_insert_key(rule, key_hash, now_millis) {
                KeyAdmission::Tracked(key) => key,
                KeyAdmission::AllowUntracked => continue,
                KeyAdmission::Reject => {
                    record_rejection(&mut store.metrics, RejectReason::LocalFallbackLimit);
                    return Decision::Reject(RejectReason::LocalFallbackLimit);
                }
            };

            store.rotate_if_needed(key, rule, now_millis);

            let local = store.local_window_total(key);
            let estimated = store.estimated_window_total(key);
            let fresh = freshness.is_fresh(rule, now_millis);
            let decision = decide(rule, local, estimated, fresh, rule.safety_margin.hits, hits);

            if let Decision::Reject(reason) = decision {
                record_rejection(&mut store.metrics, reason);
                return Decision::Reject(reason);
            }
        }

        for rule in rules.matching(&request) {
            let key_hash = hash_key(rule.id, &request);
            if let KeyAdmission::Tracked(key) = store.key_for_record(rule, key_hash) {
                let effective_key_hash = store.key_hash(key);
                store.increment_local(key, rule, now_millis, hits);
                let bucket_start = rule.window.bucket_start(now_millis);
                let cell = CounterCell::local(
                    rule.id,
                    effective_key_hash,
                    bucket_start,
                    self.identity,
                    hits,
                    now_millis,
                );
                self.cells.upsert_local(cell);
            }
        }

        store.metrics.allowed = store.metrics.allowed.saturating_add(1);
        Decision::Allow
    }

    pub fn check_and_record_hashed(
        &mut self,
        request: HashedLimitRequest,
        now_millis: u64,
    ) -> Decision {
        self.check_and_record_hashed_with_cell(request, now_millis)
            .0
    }

    pub(crate) fn check_and_record_hashed_with_cell(
        &mut self,
        request: HashedLimitRequest,
        now_millis: u64,
    ) -> (Decision, Option<CounterCell>) {
        self.store.metrics.requests = self.store.metrics.requests.saturating_add(1);
        let Some(rule) = self
            .rules
            .iter()
            .find(|rule| rule.id == request.rule_id && rule.mode == EnforcementMode::Enforce)
            .cloned()
        else {
            self.store.metrics.allowed = self.store.metrics.allowed.saturating_add(1);
            return (Decision::Allow, None);
        };
        let hits = request.hits();
        let key_hash = request.key_hash();
        let key = match self.store.get_or_insert_key(&rule, key_hash, now_millis) {
            KeyAdmission::Tracked(key) => key,
            KeyAdmission::AllowUntracked => {
                self.store.metrics.allowed = self.store.metrics.allowed.saturating_add(1);
                return (Decision::Allow, None);
            }
            KeyAdmission::Reject => {
                record_rejection(&mut self.store.metrics, RejectReason::LocalFallbackLimit);
                return (Decision::Reject(RejectReason::LocalFallbackLimit), None);
            }
        };

        self.store.rotate_if_needed(key, &rule, now_millis);

        let local = self.store.local_window_total(key);
        let estimated = self.store.estimated_window_total(key);
        let fresh = self.freshness.is_fresh(&rule, now_millis);
        let decision = decide(
            &rule,
            local,
            estimated,
            fresh,
            rule.safety_margin.hits,
            hits,
        );

        if let Decision::Reject(reason) = decision {
            record_rejection(&mut self.store.metrics, reason);
            return (Decision::Reject(reason), None);
        }

        let effective_key_hash = self.store.key_hash(key);
        self.store.increment_local(key, &rule, now_millis, hits);
        let bucket_start = rule.window.bucket_start(now_millis);
        let cell = CounterCell::local(
            rule.id,
            effective_key_hash,
            bucket_start,
            self.identity,
            hits,
            now_millis,
        );
        let updated = self.cells.upsert_local(cell);

        self.store.metrics.allowed = self.store.metrics.allowed.saturating_add(1);
        (Decision::Allow, updated)
    }

    pub fn check_and_record_all_detailed(
        &mut self,
        requests: &[LimitRequest<'_>],
        now_millis: u64,
    ) -> Vec<Decision> {
        self.store.metrics.requests = self.store.metrics.requests.saturating_add(1);
        let rules = &self.rules;
        let store = &mut self.store;
        let freshness = &self.freshness;
        let mut decisions = Vec::with_capacity(requests.len());

        for request in requests {
            let hits = request.hits();
            let mut request_decision = Decision::Allow;

            for rule in rules.matching(request) {
                let key_hash = hash_key(rule.id, request);
                let key = match store.get_or_insert_key(rule, key_hash, now_millis) {
                    KeyAdmission::Tracked(key) => key,
                    KeyAdmission::AllowUntracked => continue,
                    KeyAdmission::Reject => {
                        request_decision = Decision::Reject(RejectReason::LocalFallbackLimit);
                        break;
                    }
                };

                store.rotate_if_needed(key, rule, now_millis);

                let local = store.local_window_total(key);
                let estimated = store.estimated_window_total(key);
                let fresh = freshness.is_fresh(rule, now_millis);
                let decision = decide(rule, local, estimated, fresh, rule.safety_margin.hits, hits);

                if decision.is_reject() {
                    request_decision = decision;
                    break;
                }
            }

            decisions.push(request_decision);
        }

        if let Some(reason) = decisions.iter().find_map(|decision| match decision {
            Decision::Allow => None,
            Decision::Reject(reason) => Some(*reason),
        }) {
            record_rejection(&mut store.metrics, reason);
            return decisions;
        }

        for request in requests {
            let hits = request.hits();
            for rule in rules.matching(request) {
                let key_hash = hash_key(rule.id, request);
                if let KeyAdmission::Tracked(key) = store.key_for_record(rule, key_hash) {
                    let effective_key_hash = store.key_hash(key);
                    store.increment_local(key, rule, now_millis, hits);
                    let bucket_start = rule.window.bucket_start(now_millis);
                    let cell = CounterCell::local(
                        rule.id,
                        effective_key_hash,
                        bucket_start,
                        self.identity,
                        hits,
                        now_millis,
                    );
                    self.cells.upsert_local(cell);
                }
            }
        }

        store.metrics.allowed = store.metrics.allowed.saturating_add(1);
        decisions
    }

    pub fn check_and_record_all_into(
        &mut self,
        requests: &[LimitRequest<'_>],
        decisions: &mut [Decision],
        now_millis: u64,
    ) -> usize {
        let count = requests.len().min(decisions.len());
        self.store.metrics.requests = self.store.metrics.requests.saturating_add(1);
        let rules = &self.rules;
        let store = &mut self.store;
        let freshness = &self.freshness;

        for (request, decision) in requests[..count].iter().zip(&mut decisions[..count]) {
            let hits = request.hits();
            *decision = Decision::Allow;

            for rule in rules.matching(request) {
                let key_hash = hash_key(rule.id, request);
                let key = match store.get_or_insert_key(rule, key_hash, now_millis) {
                    KeyAdmission::Tracked(key) => key,
                    KeyAdmission::AllowUntracked => continue,
                    KeyAdmission::Reject => {
                        *decision = Decision::Reject(RejectReason::LocalFallbackLimit);
                        break;
                    }
                };

                store.rotate_if_needed(key, rule, now_millis);

                let local = store.local_window_total(key);
                let estimated = store.estimated_window_total(key);
                let fresh = freshness.is_fresh(rule, now_millis);
                let next = decide(rule, local, estimated, fresh, rule.safety_margin.hits, hits);

                if next.is_reject() {
                    *decision = next;
                    break;
                }
            }
        }

        if let Some(reason) = decisions[..count]
            .iter()
            .find_map(|decision| match decision {
                Decision::Allow => None,
                Decision::Reject(reason) => Some(*reason),
            })
        {
            record_rejection(&mut store.metrics, reason);
            return count;
        }

        for request in &requests[..count] {
            let hits = request.hits();
            for rule in rules.matching(request) {
                let key_hash = hash_key(rule.id, request);
                if let KeyAdmission::Tracked(key) = store.key_for_record(rule, key_hash) {
                    let effective_key_hash = store.key_hash(key);
                    store.increment_local(key, rule, now_millis, hits);
                    let bucket_start = rule.window.bucket_start(now_millis);
                    let cell = CounterCell::local(
                        rule.id,
                        effective_key_hash,
                        bucket_start,
                        self.identity,
                        hits,
                        now_millis,
                    );
                    self.cells.upsert_local(cell);
                }
            }
        }

        store.metrics.allowed = store.metrics.allowed.saturating_add(1);
        count
    }
}

pub fn decide(
    rule: &Rule,
    local_count: u64,
    estimated_global: u64,
    global_estimate_is_fresh: bool,
    safety_margin: u64,
    hits: u64,
) -> Decision {
    if local_count.saturating_add(hits) > rule.local_absolute_limit {
        return Decision::Reject(RejectReason::LocalAbsoluteLimit);
    }

    if global_estimate_is_fresh {
        if estimated_global
            .saturating_add(hits)
            .saturating_add(safety_margin)
            <= rule.limit
        {
            Decision::Allow
        } else {
            Decision::Reject(RejectReason::GlobalLimit)
        }
    } else if local_count.saturating_add(hits) <= rule.local_fallback_limit {
        Decision::Allow
    } else {
        Decision::Reject(RejectReason::LocalFallbackLimit)
    }
}

fn record_rejection(metrics: &mut Metrics, reason: RejectReason) {
    metrics.rejected = metrics.rejected.saturating_add(1);
    match reason {
        RejectReason::LocalAbsoluteLimit => {
            metrics.local_absolute_rejected = metrics.local_absolute_rejected.saturating_add(1);
        }
        RejectReason::GlobalLimit => {
            metrics.global_estimate_rejected = metrics.global_estimate_rejected.saturating_add(1);
        }
        RejectReason::LocalFallbackLimit => {
            metrics.local_fallback_rejected = metrics.local_fallback_rejected.saturating_add(1);
        }
    }
}

fn expire_old_buckets(entry: &mut KeyEntry, window: WindowSpec, now_millis: u64) {
    let window_start = now_millis.saturating_sub(window.size_millis);
    let bucket_millis = window.bucket_millis();
    for bucket in &mut entry.buckets {
        if bucket.bucket_start_millis != u64::MAX
            && bucket.bucket_start_millis.saturating_add(bucket_millis) <= window_start
        {
            entry.local_window_total = entry.local_window_total.saturating_sub(bucket.local_count);
            entry.estimated_window_total = entry
                .estimated_window_total
                .saturating_sub(bucket.estimated_total);
            *bucket = BucketSlot::empty();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen, TestResult};
    use quickcheck_macros::quickcheck;

    #[derive(Clone, Debug)]
    struct WindowTotalsCase {
        ops: Vec<WindowOp>,
    }

    #[derive(Clone, Debug)]
    struct WindowOp {
        tenant: u8,
        now_millis: u16,
    }

    impl Arbitrary for WindowTotalsCase {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut ops = Vec::<WindowOp>::arbitrary(g);
            ops.truncate(256);
            Self { ops }
        }
    }

    impl Arbitrary for WindowOp {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                tenant: u8::arbitrary(g),
                now_millis: u16::arbitrary(g) % 3_500,
            }
        }
    }

    #[derive(Clone, Debug)]
    struct LimitCase {
        fallback_limit: u8,
        attempts: u8,
    }

    impl Arbitrary for LimitCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                fallback_limit: u8::arbitrary(g) % 8,
                attempts: (u8::arbitrary(g) % 32).max(1),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct DescriptorMatchCase {
        request_value: u8,
        exact_value: u8,
        include_wildcard: bool,
        disabled_exact: bool,
        wrong_domain: bool,
    }

    impl Arbitrary for DescriptorMatchCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                request_value: u8::arbitrary(g) % 8,
                exact_value: u8::arbitrary(g) % 8,
                include_wildcard: bool::arbitrary(g),
                disabled_exact: bool::arbitrary(g),
                wrong_domain: bool::arbitrary(g),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct OverflowPolicyCase {
        policy: OverflowPolicy,
        attempts: u8,
    }

    impl Arbitrary for OverflowPolicyCase {
        fn arbitrary(g: &mut Gen) -> Self {
            let policy = match u8::arbitrary(g) % 4 {
                0 => OverflowPolicy::UseOverflowKey,
                1 => OverflowPolicy::AllowUntracked,
                2 => OverflowPolicy::Reject,
                _ => OverflowPolicy::Sample,
            };
            Self {
                policy,
                attempts: (u8::arbitrary(g) % 32).max(2),
            }
        }
    }

    fn rule(id: RuleId) -> Rule {
        Rule {
            id,
            domain_hash: hash_domain("api"),
            descriptor_matcher: DescriptorMatcher::exact_keys(["tenant"]),
            limit: 10,
            window: WindowSpec {
                size_millis: 1_000,
                bucket_count: 10,
            },
            local_fallback_limit: 3,
            local_absolute_limit: 6,
            stale_after_millis: 500,
            safety_margin: SafetyMargin { hits: 0 },
            overflow_policy: OverflowPolicy::UseOverflowKey,
            mode: EnforcementMode::Enforce,
        }
    }

    fn exact_value_rule(id: RuleId, value: &str) -> Rule {
        Rule {
            descriptor_matcher: DescriptorMatcher::exact([("tenant", value)]),
            ..rule(id)
        }
    }

    fn check(engine: &mut LocalEngine, value: &str, now_millis: u64) -> Decision {
        let descriptors = [Descriptor {
            key: "tenant",
            value,
        }];
        engine.check_and_record(
            LimitRequest {
                domain: "api",
                descriptors: &descriptors,
                hits: 1,
            },
            now_millis,
        )
    }

    fn request_with_descriptors<'a>(descriptors: &'a [Descriptor<'a>]) -> LimitRequest<'a> {
        LimitRequest {
            domain: "api",
            descriptors,
            hits: 1,
        }
    }

    #[test]
    fn allows_up_to_local_fallback_when_gossip_is_stale() {
        let rules = RuleTable::new(vec![rule(1)]);
        let mut engine = LocalEngine::new(rules, 8, 10);

        assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
        assert_eq!(check(&mut engine, "a", 1), Decision::Allow);
        assert_eq!(check(&mut engine, "a", 2), Decision::Allow);
        assert_eq!(
            check(&mut engine, "a", 3),
            Decision::Reject(RejectReason::LocalFallbackLimit)
        );

        let metrics = engine.metrics();
        assert_eq!(metrics.allowed, 3);
        assert_eq!(metrics.local_fallback_rejected, 1);
    }

    #[test]
    fn uses_global_limit_when_estimate_is_fresh() {
        let rules = RuleTable::new(vec![rule(1)]);
        let mut engine = LocalEngine::new(rules, 8, 10);
        engine.mark_global_estimate_updated(0);

        for i in 0..6 {
            assert_eq!(check(&mut engine, "a", i), Decision::Allow);
        }

        assert_eq!(
            check(&mut engine, "a", 6),
            Decision::Reject(RejectReason::LocalAbsoluteLimit)
        );
    }

    #[test]
    fn expires_sliding_window_buckets() {
        let rules = RuleTable::new(vec![rule(1)]);
        let mut engine = LocalEngine::new(rules, 8, 10);

        assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
        assert_eq!(check(&mut engine, "a", 100), Decision::Allow);
        assert_eq!(check(&mut engine, "a", 200), Decision::Allow);
        assert_eq!(
            check(&mut engine, "a", 300),
            Decision::Reject(RejectReason::LocalFallbackLimit)
        );
        assert_eq!(check(&mut engine, "a", 1_201), Decision::Allow);
    }

    #[test]
    fn overflow_key_keeps_serving_when_capacity_is_exhausted() {
        let rules = RuleTable::new(vec![rule(1)]);
        let mut engine = LocalEngine::new(rules, 1, 10);

        assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
        assert_eq!(check(&mut engine, "b", 0), Decision::Allow);
        assert_eq!(engine.active_keys(), 1);
        assert_eq!(engine.metrics().overflow_key_uses, 1);
    }

    #[test]
    fn reject_overflow_policy_rejects_new_keys_when_capacity_is_exhausted() {
        let mut reject_rule = rule(1);
        reject_rule.overflow_policy = OverflowPolicy::Reject;
        let rules = RuleTable::new(vec![reject_rule]);
        let mut engine = LocalEngine::new(rules, 1, 10);

        assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
        assert_eq!(
            check(&mut engine, "b", 0),
            Decision::Reject(RejectReason::LocalFallbackLimit)
        );
        assert_eq!(engine.metrics().overflow_rejected, 1);
    }

    #[test]
    fn unmatched_requests_are_allowed_without_allocating_keys() {
        let rules = RuleTable::new(vec![rule(1)]);
        let mut engine = LocalEngine::new(rules, 8, 10);
        let request = LimitRequest {
            domain: "other",
            descriptors: &[Descriptor {
                key: "tenant",
                value: "a",
            }],
            hits: 1,
        };

        assert_eq!(engine.check_and_record(request, 0), Decision::Allow);
        assert_eq!(engine.active_keys(), 0);
    }

    #[test]
    fn exact_descriptor_key_matching_uses_request_slice_without_allocation() {
        let rules = RuleTable::new(vec![rule(1)]);
        let descriptors = [Descriptor {
            key: "tenant",
            value: "a",
        }];
        let request = request_with_descriptors(&descriptors);

        let matched: Vec<_> = rules.matching(&request).map(|rule| rule.id).collect();

        assert_eq!(matched, vec![1]);
    }

    #[test]
    fn descriptor_matching_honors_exact_values_and_wildcards() {
        let rules = RuleTable::new(vec![exact_value_rule(1, "paid"), exact_value_rule(2, "*")]);
        let paid = [Descriptor {
            key: "tenant",
            value: "paid",
        }];
        let free = [Descriptor {
            key: "tenant",
            value: "free",
        }];

        let paid_matches: Vec<_> = rules
            .matching(&request_with_descriptors(&paid))
            .map(|rule| rule.id)
            .collect();
        let free_matches: Vec<_> = rules
            .matching(&request_with_descriptors(&free))
            .map(|rule| rule.id)
            .collect();

        assert_eq!(paid_matches, vec![1, 2]);
        assert_eq!(free_matches, vec![2]);
    }

    #[test]
    fn cardinality_limits_reject_large_descriptor_sets() {
        let descriptors = [
            Descriptor {
                key: "tenant",
                value: "a",
            },
            Descriptor {
                key: "route",
                value: "/v1",
            },
        ];
        let request = LimitRequest {
            domain: "api",
            descriptors: &descriptors,
            hits: 1,
        };

        assert_eq!(
            request.validate_cardinality(CardinalityLimits {
                max_descriptor_count: 1,
                max_descriptor_bytes: 512,
                max_key_bytes: 128,
            }),
            Err(CardinalityError::DescriptorCount)
        );
        assert_eq!(
            request.validate_cardinality(CardinalityLimits {
                max_descriptor_count: 2,
                max_descriptor_bytes: 4,
                max_key_bytes: 128,
            }),
            Err(CardinalityError::DescriptorBytes)
        );
    }

    #[test]
    fn remote_estimate_participates_in_fresh_global_decision() {
        let rules = RuleTable::new(vec![rule(1)]);
        let mut engine = LocalEngine::new(rules, 8, 10);
        let descriptors = [Descriptor {
            key: "tenant",
            value: "a",
        }];
        let request = LimitRequest {
            domain: "api",
            descriptors: &descriptors,
            hits: 1,
        };
        let key_hash = hash_key(1, &request);

        assert!(engine.add_remote_estimate(1, key_hash, 0, 0, 10));

        assert_eq!(
            engine.check_and_record(request, 1),
            Decision::Reject(RejectReason::GlobalLimit)
        );
    }

    #[test]
    fn freshness_is_rule_scoped() {
        let rule_a = rule(1);
        let mut rule_b = rule(2);
        rule_b.descriptor_matcher = DescriptorMatcher::exact([("route", "*")]);
        let rules = RuleTable::new(vec![rule_a, rule_b]);
        let mut engine = LocalEngine::new(rules, 8, 10);
        let descriptors_a = [Descriptor {
            key: "tenant",
            value: "a",
        }];
        let request_a = LimitRequest {
            domain: "api",
            descriptors: &descriptors_a,
            hits: 1,
        };
        let descriptors_b = [Descriptor {
            key: "route",
            value: "/v1",
        }];
        let request_b = LimitRequest {
            domain: "api",
            descriptors: &descriptors_b,
            hits: 1,
        };

        assert!(engine.add_remote_estimate(1, hash_key(1, &request_a), 0, 0, 10));

        assert_eq!(
            engine.check_and_record(request_a, 1),
            Decision::Reject(RejectReason::GlobalLimit)
        );
        assert_eq!(engine.check_and_record(request_b, 1), Decision::Allow);
        assert_eq!(engine.check_and_record(request_b, 2), Decision::Allow);
        assert_eq!(engine.check_and_record(request_b, 3), Decision::Allow);
        assert_eq!(
            engine.check_and_record(request_b, 4),
            Decision::Reject(RejectReason::LocalFallbackLimit)
        );
    }

    #[test]
    fn successful_local_increments_create_dirty_cells() {
        let rules = RuleTable::new(vec![rule(1)]);
        let identity = NodeIdentity {
            node_id: NodeId::from((7_u128 << 64) | 9),
            incarnation: 11,
        };
        let mut engine = LocalEngine::with_identity(rules, 8, 10, 8, 8, identity);

        assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
        assert_eq!(check(&mut engine, "a", 1), Decision::Allow);

        let cells: Vec<_> = engine.cells().collect();
        let dirty: Vec<_> = engine.dirty_cells().collect();

        assert_eq!(engine.active_cells(), 1);
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].count, 2);
        assert_eq!(cells[0].origin_node_id, identity.node_id);
        assert_eq!(cells[0].origin_incarnation, identity.incarnation);
        assert_eq!(dirty.len(), 2);
        assert!(!engine.dirty_overflowed());
    }

    #[test]
    fn local_cell_table_reports_dirty_overflow_without_unbounded_growth() {
        let rules = RuleTable::new(vec![rule(1)]);
        let identity = NodeIdentity::default();
        let mut engine = LocalEngine::with_identity(rules, 8, 10, 1, 1, identity);

        assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
        assert_eq!(check(&mut engine, "b", 0), Decision::Allow);

        assert_eq!(engine.active_cells(), 1);
        assert!(engine.dirty_overflowed());
    }

    #[test]
    fn allow_untracked_overflow_policy_allows_without_recording_new_key() {
        let mut allow_rule = rule(1);
        allow_rule.overflow_policy = OverflowPolicy::AllowUntracked;
        let rules = RuleTable::new(vec![allow_rule]);
        let mut engine = LocalEngine::new(rules, 1, 10);

        assert_eq!(check(&mut engine, "a", 0), Decision::Allow);
        assert_eq!(check(&mut engine, "b", 0), Decision::Allow);
        assert_eq!(engine.active_keys(), 1);
        assert_eq!(engine.active_cells(), 1);
        assert_eq!(engine.metrics().overflow_untracked, 1);
    }

    #[test]
    fn sample_overflow_policy_uses_bounded_tracking_subset() {
        let mut sample_rule = rule(1);
        sample_rule.overflow_policy = OverflowPolicy::Sample;
        sample_rule.limit = 1_000;
        sample_rule.local_fallback_limit = 1_000;
        sample_rule.local_absolute_limit = 1_000;
        let rules = RuleTable::new(vec![sample_rule]);
        let mut engine = LocalEngine::new(rules, 1, 10);

        assert_eq!(check(&mut engine, "seed", 0), Decision::Allow);
        for index in 0..32 {
            let value = format!("tenant-{index}");
            assert_eq!(check(&mut engine, &value, index + 1), Decision::Allow);
        }

        let metrics = engine.metrics();
        assert_eq!(engine.active_keys(), 1);
        assert!(metrics.overflow_sampled > 0);
        assert!(metrics.overflow_untracked > 0);
    }

    #[quickcheck]
    fn quickcheck_descriptor_matching_is_deterministic_for_exact_values_and_wildcards(
        case: DescriptorMatchCase,
    ) -> TestResult {
        let request_value = format!("tenant-{}", case.request_value);
        let exact_value = format!("tenant-{}", case.exact_value);
        let request_domain = if case.wrong_domain { "other" } else { "api" };
        let mut exact = exact_value_rule(1, &exact_value);
        if case.disabled_exact {
            exact.mode = EnforcementMode::Disabled;
        }
        let mut rules = vec![exact];
        if case.include_wildcard {
            rules.push(exact_value_rule(2, "*"));
        }
        let rules = RuleTable::new(rules);
        let descriptors = [Descriptor {
            key: "tenant",
            value: request_value.as_str(),
        }];
        let request = LimitRequest {
            domain: request_domain,
            descriptors: &descriptors,
            hits: 1,
        };
        let matched = rules
            .matching(&request)
            .map(|rule| rule.id)
            .collect::<Vec<_>>();
        let mut expected = Vec::new();

        if !case.wrong_domain && !case.disabled_exact && case.request_value == case.exact_value {
            expected.push(1);
        }
        if !case.wrong_domain && case.include_wildcard {
            expected.push(2);
        }

        if matched == expected {
            TestResult::passed()
        } else {
            TestResult::error("descriptor matcher returned a rule set that diverged from model")
        }
    }

    #[quickcheck]
    fn quickcheck_overflow_policies_never_exceed_key_or_cell_capacity(
        case: OverflowPolicyCase,
    ) -> TestResult {
        let mut checked_rule = rule(1);
        checked_rule.overflow_policy = case.policy;
        checked_rule.limit = 1_000;
        checked_rule.local_fallback_limit = 1_000;
        checked_rule.local_absolute_limit = 1_000;
        let rules = RuleTable::new(vec![checked_rule]);
        let mut engine = LocalEngine::new(rules, 1, 10);

        for index in 0..case.attempts {
            let value = format!("tenant-{index}");
            let _ = check(&mut engine, &value, u64::from(index));
            if engine.active_keys() > 1 || engine.active_cells() > 10 {
                return TestResult::error(
                    "overflow policy grew storage beyond configured capacity",
                );
            }
        }

        let metrics = engine.metrics();
        match case.policy {
            OverflowPolicy::UseOverflowKey if metrics.overflow_key_uses == 0 => {
                TestResult::error("overflow-key policy did not record overflow use")
            }
            OverflowPolicy::AllowUntracked if metrics.overflow_untracked == 0 => {
                TestResult::error("allow-untracked policy did not record untracked overflow")
            }
            OverflowPolicy::Reject if metrics.overflow_rejected == 0 => {
                TestResult::error("reject overflow policy did not reject overflow")
            }
            OverflowPolicy::Sample
                if metrics.overflow_sampled == 0 && metrics.overflow_untracked == 0 =>
            {
                TestResult::error("sample overflow policy did not sample or allow overflow")
            }
            _ => TestResult::passed(),
        }
    }

    #[quickcheck]
    fn quickcheck_window_totals_match_live_buckets(case: WindowTotalsCase) -> TestResult {
        let mut checked_rule = rule(1);
        checked_rule.limit = 1_000;
        checked_rule.local_fallback_limit = 1_000;
        checked_rule.local_absolute_limit = 1_000;
        let rules = RuleTable::new(vec![checked_rule]);
        let mut engine = LocalEngine::new(rules, 4, 10);
        let tenants = ["a", "b", "c", "d"];

        for op in case.ops {
            let tenant = tenants[op.tenant as usize % tenants.len()];
            let now_millis = u64::from(op.now_millis);
            if check(&mut engine, tenant, now_millis) != Decision::Allow {
                return TestResult::error("high-limit generated request was rejected");
            }

            for entry in engine.store.entries.iter().filter(|entry| entry.occupied) {
                let local_total = entry
                    .buckets
                    .iter()
                    .map(|bucket| bucket.local_count)
                    .sum::<u64>();
                let estimated_total = entry
                    .buckets
                    .iter()
                    .map(|bucket| bucket.estimated_total)
                    .sum::<u64>();

                if entry.local_window_total != local_total
                    || entry.estimated_window_total != estimated_total
                    || entry.estimated_window_total < entry.local_window_total
                {
                    return TestResult::error(
                        "stored window totals diverged from live bucket sums",
                    );
                }
            }
        }
        TestResult::passed()
    }

    #[quickcheck]
    fn quickcheck_limits_never_exceed_fallback_or_absolute_caps(case: LimitCase) -> TestResult {
        let local_fallback_limit = u64::from(case.fallback_limit);
        let attempts = u64::from(case.attempts);
        let mut checked_rule = rule(1);
        checked_rule.limit = 64;
        checked_rule.local_fallback_limit = local_fallback_limit;
        checked_rule.local_absolute_limit = local_fallback_limit + 2;
        let rules = RuleTable::new(vec![checked_rule]);
        let mut stale_engine = LocalEngine::new(rules, 4, 10);
        let mut allowed = 0_u64;

        for now_millis in 0..attempts {
            match check(&mut stale_engine, "a", now_millis) {
                Decision::Allow => allowed = allowed.saturating_add(1),
                Decision::Reject(RejectReason::LocalFallbackLimit) => break,
                Decision::Reject(_) => {
                    return TestResult::error("stale decision rejected for non-fallback reason");
                }
            }
        }
        if allowed > local_fallback_limit {
            return TestResult::error("stale decisions exceeded local fallback limit");
        }

        let mut checked_rule = rule(1);
        checked_rule.limit = 64;
        checked_rule.local_fallback_limit = 64;
        checked_rule.local_absolute_limit = local_fallback_limit + 2;
        let rules = RuleTable::new(vec![checked_rule]);
        let mut fresh_engine = LocalEngine::new(rules, 4, 10);
        fresh_engine.mark_global_estimate_updated(0);
        let mut allowed = 0_u64;

        for now_millis in 0..attempts {
            match check(&mut fresh_engine, "a", now_millis) {
                Decision::Allow => allowed = allowed.saturating_add(1),
                Decision::Reject(RejectReason::LocalAbsoluteLimit) => break,
                Decision::Reject(_) => {
                    return TestResult::error("fresh decision rejected for non-absolute reason");
                }
            }
        }
        if allowed <= local_fallback_limit + 2 {
            TestResult::passed()
        } else {
            TestResult::error("fresh decisions exceeded local absolute limit")
        }
    }
}
