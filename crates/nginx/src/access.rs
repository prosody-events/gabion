//! Per-request access decision. Pure logic — no `ngx` imports — so the
//! hot path is exercisable from unit tests.
//!
//! Allocation discipline: descriptors live in a stack-resident `ArrayVec`;
//! `RuleTable::matching` returns an iterator we walk without collecting;
//! the SHM aggregate is read through `AggregateTable::window_total` which
//! does only atomic loads.

use arrayvec::ArrayVec;
use gabion::defaults;
use gabion::rules::{Descriptor, EnforcementMode, hash_key};

use crate::rules::{BindingLookup, CompiledRules, MAX_DESCRIPTORS, RuleSpec};
use crate::shm::aggregate::AggregateTable;
use crate::shm::queue::{QueueEvent, RequestQueue};
use crate::shm::stats::Stats;

/// Cap on rules that may match a single request. Mirrors the server limit
/// (`gabion::defaults::STORAGE_MAX_MATCHED_RULES`).
const MAX_MATCHED_RULES: usize = defaults::STORAGE_MAX_MATCHED_RULES;

/// Maximum descriptor bytes per request (key + value, summed across all
/// descriptors plus the domain). Matches the server's default cardinality
/// envelope.
pub const MAX_DESCRIPTOR_BYTES: usize = defaults::STORAGE_MAX_DESCRIPTOR_BYTES;

/// Maximum per-descriptor key length in bytes.
pub const MAX_KEY_BYTES: usize = defaults::STORAGE_MAX_KEY_BYTES;

pub use crate::rules::CardinalitySettings;

/// Resolve a compiled binding against the in-flight request. The access
/// path borrows directly from nginx-owned buffers; the `&[u8]` returned
/// must live for the duration of `decide`. A return value of `None` skips
/// the rule for the request (fail-open under the allow-by-default
/// principle).
pub trait VariableLookup {
    fn lookup(&self, binding: &BindingLookup) -> Option<&[u8]>;
}

/// Result of the access decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessOutcome {
    Allow,
    /// Rate limit exceeded. Carries the rule-specific info the headers
    /// builder needs.
    Reject(RejectInfo),
    /// Cardinality envelope violated. Map to `400 Bad Request`.
    Cardinality,
    /// No rule matched or a referenced variable was missing — let the
    /// request through without recording a hit.
    Decline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RejectInfo {
    pub spec: RuleSpec,
    pub total: u64,
    pub now_millis: u64,
}

/// Borrowed view of the rule set + SHM region passed into the access path.
/// One per nginx worker per zone.
#[derive(Clone, Copy)]
pub struct AccessCtx<'a> {
    pub rules: &'a CompiledRules,
    pub aggregate: AggregateTable<'a>,
    pub queue: RequestQueue<'a>,
    pub stats: &'a Stats,
    /// Default domain assigned to requests when the location config doesn't
    /// override it.
    pub domain: &'a str,
    pub cardinality: CardinalitySettings,
}

/// Per-rule outcome before [`decide_all`] folds it into a single
/// [`AccessOutcome`] for the request.
#[derive(Clone, Debug)]
enum RuleOutcome {
    /// Rule evaluated cleanly and would allow the request. Buffered queue
    /// events ride along so a sibling-rule reject can drop them without
    /// committing partial work.
    Allow(ArrayVec<QueueEvent, MAX_MATCHED_RULES>),
    /// Rule evaluated and crossed its configured limit while in
    /// `Enforce` mode.
    Reject(RejectInfo),
    /// Rule had no opinion on this request — a binding variable was
    /// missing or no `RuleTable::matching` row applied. Fail-open: the
    /// request still allows for this rule.
    Decline,
    /// Rule was over the per-request byte budget. The sibling rules may
    /// still evaluate independently; if *every* configured rule
    /// cardinality-skipped, [`decide_all`] surfaces the operator-visible
    /// 400 just like the single-rule path.
    Cardinality,
    /// Predicate `except_if=` resolved truthy; rule is skipped for this
    /// request.
    Exempt,
}

/// Evaluate one request against the rule indicated by `rule_index`.
/// Wraps [`decide_all`] for the common single-rule case (and the legacy
/// in-crate tests).
pub fn decide(
    ctx: AccessCtx<'_>,
    rule_index: usize,
    vars: &impl VariableLookup,
    now_millis: u64,
) -> AccessOutcome {
    decide_all(ctx, &[rule_index], vars, now_millis)
}

/// Evaluate one request against every rule in `rule_indices`. Rules
/// stack: rejection from any enforcing rule rejects the request, with
/// `Retry-After` and the `X-RateLimit-*` triplet pinned to the rule with
/// the longest window (so the client doesn't see a short retry that
/// puts it right back into 429 against the wider rule).
pub fn decide_all(
    ctx: AccessCtx<'_>,
    rule_indices: &[usize],
    vars: &impl VariableLookup,
    now_millis: u64,
) -> AccessOutcome {
    ctx.stats.record_request();

    let mut worst_reject: Option<RejectInfo> = None;
    let mut all_events: ArrayVec<QueueEvent, MAX_MATCHED_RULES> = ArrayVec::new();
    let mut had_non_exempt_outcome = false;
    let mut any_exempt = false;
    let mut any_cardinality = false;

    for &index in rule_indices {
        match decide_one(ctx, index, vars, now_millis) {
            RuleOutcome::Allow(events) => {
                had_non_exempt_outcome = true;
                for ev in events {
                    if all_events.try_push(ev).is_err() {
                        // The combined buffer can hold up to one event per
                        // matched-rules slot; if siblings push us past
                        // that, treat the overflow like the single-rule
                        // case: under-count rather than over-reject.
                        ctx.stats.record_matched_rule_overflow();
                        break;
                    }
                }
            }
            RuleOutcome::Reject(info) => {
                had_non_exempt_outcome = true;
                worst_reject = Some(pick_longest_window(worst_reject, info));
            }
            RuleOutcome::Decline => {
                // Rule had no opinion (missing variable, no match, etc.).
                // Siblings still get to evaluate.
            }
            RuleOutcome::Cardinality => {
                any_cardinality = true;
                ctx.stats.record_cardinality_reject();
            }
            RuleOutcome::Exempt => {
                any_exempt = true;
                ctx.stats.record_exempt();
            }
        }
    }

    if let Some(info) = worst_reject {
        ctx.stats.record_reject();
        return AccessOutcome::Reject(info);
    }
    if had_non_exempt_outcome {
        for event in all_events {
            match ctx.queue.push(event) {
                Ok(()) => ctx.stats.record_queue_push(),
                Err(_) => ctx.stats.record_queue_drop(),
            }
        }
        ctx.stats.record_allow();
        return AccessOutcome::Allow;
    }
    if any_exempt {
        // Exempt-only outcome: every rule's `except_if=` predicate fired,
        // so the request is allowed through without crediting either the
        // generic `allowed` counter or the gossip aggregate.
        return AccessOutcome::Allow;
    }
    if any_cardinality {
        return AccessOutcome::Cardinality;
    }
    AccessOutcome::Decline
}

/// Per-rule worker for [`decide_all`]. Builds descriptors on the stack,
/// runs `RuleTable::matching`, and returns the per-rule outcome without
/// committing to the queue — the orchestrator does the commit only if no
/// enforcing rule rejected the request.
fn decide_one(
    ctx: AccessCtx<'_>,
    rule_index: usize,
    vars: &impl VariableLookup,
    now_millis: u64,
) -> RuleOutcome {
    let Some(compiled) = ctx.rules.get(rule_index) else {
        return RuleOutcome::Decline;
    };

    // Predicate gate. Evaluated before descriptors so a truthy predicate
    // exempts the request without billing the cardinality budget. A
    // predicate variable that resolves to None (variable absent at request
    // time) falls through as "rule applies" — same fail-open shape as a
    // missing descriptor variable.
    if let Some(predicate) = compiled.except_if.as_ref() {
        if let Some(value) = vars.lookup(predicate)
            && is_truthy(value)
        {
            return RuleOutcome::Exempt;
        }
    }

    // Build descriptors on the stack in one pass over `bindings`. Variable
    // misses decline; UTF-8 failures decline (and bump a stat so the bypass
    // is observable); the dynamic byte budget is the only per-request
    // cardinality check — count and key length are gated at compile time.
    let mut descriptors: ArrayVec<Descriptor<'_>, MAX_DESCRIPTORS> = ArrayVec::new();
    let mut bytes = ctx.domain.len();
    for binding in &compiled.bindings {
        let Some(value) = vars.lookup(&binding.lookup) else {
            return RuleOutcome::Decline;
        };
        bytes = bytes
            .saturating_add(binding.key.len())
            .saturating_add(value.len());
        if bytes > ctx.cardinality.max_descriptor_bytes {
            return RuleOutcome::Cardinality;
        }
        let value_str = match std::str::from_utf8(value) {
            Ok(s) => s,
            Err(_) => {
                ctx.stats.record_decline_invalid_descriptor();
                return RuleOutcome::Decline;
            }
        };
        descriptors.push(Descriptor {
            key: binding.key.as_str(),
            value: value_str,
        });
    }

    // Evaluate this one specific rule. The rule's `RuleTable` row is
    // looked up by id; we don't walk `RuleTable::matching` here because
    // (a) the descriptors were built from this rule's bindings — the
    // pattern matches by construction — and (b) under multi-rule per
    // location, each `rule_index` evaluates exactly that rule, not every
    // sibling that happens to share the same descriptor shape.
    let descriptors_slice: &[Descriptor<'_>] = descriptors.as_slice();
    let rule = &compiled.rule;
    if rule.mode == EnforcementMode::Disabled {
        return RuleOutcome::Decline;
    }
    let spec = rule.spec();
    let key_hash = hash_key(spec.id, ctx.domain, descriptors_slice);
    let total = ctx.aggregate.window_total(
        spec.fingerprint,
        key_hash.0,
        now_millis,
        spec.bucket_millis,
        spec.live_buckets,
    );
    // DryRun rules count and gossip but never reject — they let operators
    // observe what enforcement would do before flipping the switch.
    if total.saturating_add(1) > spec.limit && rule.mode == EnforcementMode::Enforce {
        return RuleOutcome::Reject(RejectInfo {
            spec,
            total,
            now_millis,
        });
    }
    let bucket = (now_millis / spec.bucket_millis) as u32;
    let mut planned: ArrayVec<QueueEvent, MAX_MATCHED_RULES> = ArrayVec::new();
    planned.push(QueueEvent {
        rule_fingerprint: spec.fingerprint,
        key_hash: key_hash.0,
        bucket,
        hits: 1,
        now_millis,
    });
    RuleOutcome::Allow(planned)
}

/// Pick the rule with the longer window among a candidate and a current
/// "worst" choice. `Retry-After` and the rate-limit headers come from
/// this rule so the client never sees a short retry that puts it right
/// back into 429 against the wider rule.
fn pick_longest_window(current: Option<RejectInfo>, candidate: RejectInfo) -> RejectInfo {
    match current {
        Some(c) if c.spec.window_millis >= candidate.spec.window_millis => c,
        _ => candidate,
    }
}

/// True if `value` should be treated as "truthy" by the `except_if=`
/// predicate. An empty value is falsy. The strings `0`, `false`, `off`,
/// and `no` (case-insensitive) are falsy — matching what operators write
/// in `set $trusted off;` or `map ... { default no; }`. Anything else,
/// including `1`, `true`, `yes`, and any non-empty arbitrary string, is
/// truthy.
pub fn is_truthy(value: &[u8]) -> bool {
    if value.is_empty() {
        return false;
    }
    // Case-insensitive ASCII comparison without allocating.
    !matches!(
        value,
        b"0" | b"false"
            | b"False"
            | b"FALSE"
            | b"off"
            | b"Off"
            | b"OFF"
            | b"no"
            | b"No"
            | b"NO"
    )
}

/// Delta-seconds the client should wait before retrying. Emitted as
/// `Retry-After` (RFC 7231 §7.1.3).
///
/// Under the fixed-window configuration, a rejected hit may still be visible
/// until the next full window boundary. The safest answer — and the one least
/// likely to send the client into another 429 — is the full window in
/// seconds. We deliberately don't emit "seconds until the next small bucket
/// boundary": if bucket granularity is configured below the user-facing
/// window, that number is misleading for clients.
pub fn retry_after_seconds(info: RejectInfo) -> u64 {
    info.spec.window_millis.div_ceil(1_000).max(1)
}

/// Unix-timestamp seconds at which `X-RateLimit-Reset` says the rate
/// limit will reset. Matches the Envoy ratelimit filter / GitHub /
/// Twitter conventions: emit an absolute time, not a delta.
///
/// Falls back to "now + window_seconds" if the caller's `now_millis`
/// looks like a relative clock (less than the unix epoch lower bound).
pub fn reset_unix_seconds(info: RejectInfo) -> u64 {
    let window_seconds = info.spec.window_millis.div_ceil(1_000).max(1);
    let now_seconds = info.now_millis / 1_000;
    now_seconds.saturating_add(window_seconds)
}

#[cfg(test)]
pub(crate) use mock::MockVars;

#[cfg(test)]
mod mock {
    use super::*;
    use std::collections::HashMap;

    /// Test-only [`VariableLookup`]. Dispatches on the same enum the
    /// production lookup does — the inline arms return canned values; the
    /// `IndexedVariable` arm reads from a `HashMap` keyed on the variable
    /// name (ignoring the synthetic index).
    pub struct MockVars {
        pub vars: HashMap<String, Vec<u8>>,
        pub uri: Vec<u8>,
        pub args: Vec<u8>,
        pub remote_addr: Vec<u8>,
        pub request_uri: Vec<u8>,
    }

    impl MockVars {
        pub fn new() -> Self {
            Self {
                vars: HashMap::new(),
                uri: Vec::new(),
                args: Vec::new(),
                remote_addr: Vec::new(),
                request_uri: Vec::new(),
            }
        }

        /// Set a value for an indexed-variable lookup keyed on the
        /// variable name (the `$`-stripped identifier).
        pub fn set(mut self, name: &str, value: &str) -> Self {
            self.vars
                .insert(name.to_string(), value.as_bytes().to_vec());
            self
        }

        pub fn set_bytes(mut self, name: &str, value: &[u8]) -> Self {
            self.vars.insert(name.to_string(), value.to_vec());
            self
        }
    }

    impl VariableLookup for MockVars {
        fn lookup(&self, binding: &BindingLookup) -> Option<&[u8]> {
            match binding {
                BindingLookup::Uri => Some(self.uri.as_slice()),
                BindingLookup::RequestUri => Some(self.request_uri.as_slice()),
                BindingLookup::Args => Some(self.args.as_slice()),
                BindingLookup::RemoteAddr => Some(self.remote_addr.as_slice()),
                BindingLookup::Arg(name) => {
                    find_query_arg_mock(self.args.as_slice(), name.as_bytes())
                }
                BindingLookup::IndexedVariable { name, .. } => {
                    self.vars.get(name.as_ref()).map(Vec::as_slice)
                }
                BindingLookup::ComplexValue { .. } => None,
            }
        }
    }

    fn find_query_arg_mock<'a>(args: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
        let mut rest = args;
        while !rest.is_empty() {
            let next = rest
                .iter()
                .position(|byte| *byte == b'&')
                .unwrap_or(rest.len());
            let pair = &rest[..next];
            if let Some(eq) = pair.iter().position(|byte| *byte == b'=')
                && &pair[..eq] == name
            {
                return Some(&pair[eq + 1..]);
            }
            if next == rest.len() {
                break;
            }
            rest = &rest[next + 1..];
        }
        None
    }

    use crate::shm::ShmRegion;

    #[allow(dead_code)]
    pub(crate) fn ctx<'a>(rules: &'a CompiledRules, region: &'a ShmRegion) -> AccessCtx<'a> {
        AccessCtx {
            rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::{DescriptorBinding, RuleConfig};
    use crate::shm::{Layout, ShmRegion};
    use gabion::rules::EnforcementMode;
    use std::time::Duration;

    /// RAII guard wrapping a leaked SHM-backing buffer. Reclaims memory in
    /// `Drop` so each test allocation is freed; in the meantime the
    /// underlying pointer lives at a stable address, sidestepping the
    /// Stacked-/Tree-Borrows reborrow-on-move semantics that bite when a
    /// `Box<[u8]>` is moved alongside the `ShmRegion` carrying its pointer.
    pub(crate) struct TestZone {
        ptr: *mut u8,
        words: usize,
    }

    impl TestZone {
        fn allocate(words: usize) -> Self {
            let buf: Box<[u64]> = vec![0_u64; words].into_boxed_slice();
            let raw = Box::into_raw(buf);
            // SAFETY: `Box::into_raw` returns a non-null pointer to a slice
            // of `words` `u64`s. Reading the slice length back out is
            // straightforward from `(*raw).len()`. We keep the raw pointer
            // and the length, and Drop reconstructs the Box.
            Self {
                ptr: raw as *mut u8,
                words,
            }
        }

        fn as_ptr(&self) -> *mut u8 {
            self.ptr
        }
    }

    impl Drop for TestZone {
        fn drop(&mut self) {
            // SAFETY: `self.ptr` came from `Box::into_raw` of
            // `Box<[u64]>` with `self.words` slice length. Reconstructing
            // the box here is the only thing that touches this allocation
            // (no aliasing borrows are live because the `ShmRegion` paired
            // with this zone is dropped before `self`).
            unsafe {
                let slice = std::ptr::slice_from_raw_parts_mut(self.ptr as *mut u64, self.words);
                let _ = Box::from_raw(slice);
            }
        }
    }

    fn build_zone(queue_cap: usize, agg_cap: usize) -> (TestZone, ShmRegion) {
        let layout = Layout::new(queue_cap, agg_cap).expect("layout");
        // Production allocates via `mmap`. In tests we back the zone with
        // a heap allocation rounded up to `u64` words so the buffer is
        // 8-byte aligned — matching the strictest alignment any field in
        // the region requires (`AtomicU64`).
        let words = layout.total_bytes.div_ceil(8);
        let zone = TestZone::allocate(words);
        // SAFETY: `ShmRegion::initialize`'s preconditions (see its
        // `# Safety` doc) are upheld:
        // * `zone.as_ptr()` is non-null, writable, and 8-byte aligned (the underlying
        //   allocation was a `Box<[u64]>`). `words * 8 >= layout.total_bytes`, so the
        //   mapping size is covered.
        // * `Layout::new` produced `layout` above; offsets are consistent.
        // * `zone` is a fresh allocation; `initialize` is its first user.
        // * The returned `TestZone` is paired with the `ShmRegion` and dropped after
        //   every use of the region in each `#[test]`.
        let region = unsafe { ShmRegion::initialize(zone.as_ptr(), layout) };
        (zone, region)
    }

    fn build_rules() -> CompiledRules {
        CompiledRules::compile(&[RuleConfig {
            name: "per_tenant".to_string(),
            domain: crate::rules::DEFAULT_DOMAIN.to_string(),
            bindings: vec![DescriptorBinding {
                key: "tenant".to_string(),
                source: "$http_x_tenant".to_string(),
            }],
            limit: 2,
            window: Duration::from_secs(1),
            bucket: Duration::from_millis(250),
            mode: EnforcementMode::Enforce,
            except_if: None,
        }])
        .expect("compile rules")
    }

    #[test]
    fn allow_on_empty_aggregate() {
        let (_buf, region) = build_zone(8, 16);
        let rules = build_rules();
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings::default(),
        };
        let vars = MockVars::new().set("http_x_tenant", "alice");
        let outcome = decide(ctx, 0, &vars, 1_000);
        assert!(matches!(outcome, AccessOutcome::Allow));
        // One queue event pushed.
        let mut out = [QueueEvent::default(); 1];
        assert_eq!(region.queue().drain(&mut out), 1);
    }

    #[test]
    fn reject_when_window_total_exceeds_limit() {
        let (_buf, region) = build_zone(8, 16);
        let rules = build_rules();
        let domain = crate::rules::DEFAULT_DOMAIN;
        let spec = rules.rules()[0].rule.spec();
        let descriptors = [Descriptor {
            key: "tenant",
            value: "alice",
        }];
        let key_hash = hash_key(spec.id, domain, &descriptors);
        let bucket = (1_000_u64 / spec.bucket_millis) as u32;
        // Pre-populate aggregate so total >= limit (limit is 2).
        // SAFETY: `ShmAggregateStore::new`'s preconditions (see its
        // `# Safety` doc) are upheld:
        // * `region` was produced by `build_zone`, which called
        //   `ShmRegion::initialize`. `initialize` `ptr::write`s an
        //   `AggregateSlot::empty()` into each of the `layout.aggregate_capacity`
        //   slots, so `region.aggregate_slots_ptr()` is non-null, aligned for
        //   `AggregateSlot`, and points at exactly that many fully initialized slots.
        // * The `capacity` argument is the same `aggregate_capacity` the region was
        //   built with (`Layout::new` guarantees it is a power of two `>= 2`, and the
        //   total byte length fits in the buffer and thus in `isize::MAX`).
        // * The backing `Vec<u8>` (`_buf` in the caller) lives until the end of the
        //   `#[test]`, which outlives both this `store` and any `AggregateTable<'_>`
        //   derived from it via `region.aggregate()`.
        // * Single-writer: this test is single-threaded and `store` is the sole
        //   `ShmAggregateStore` constructed against `region`'s aggregate slots; reads
        //   from `region.aggregate()` use the same seqlock/atomic protocol the
        //   production code does, so they cannot race with `write_delta` (Nomicon:
        //   atomic accesses are not data races even with concurrent reads).
        let store = unsafe {
            crate::shm::aggregate::ShmAggregateStore::new(
                region.aggregate_slots_ptr(),
                region.layout.aggregate_capacity,
            )
        };
        store.write_delta(spec.fingerprint, key_hash.0, bucket, 5, 1_000);

        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain,
            cardinality: CardinalitySettings::default(),
        };
        let vars = MockVars::new().set("http_x_tenant", "alice");
        let outcome = decide(ctx, 0, &vars, 1_000);
        match outcome {
            AccessOutcome::Reject(info) => {
                assert_eq!(info.spec.id, spec.id);
                assert_eq!(info.total, 5);
            }
            other => panic!("expected Reject, got {other:?}"),
        }
        let mut out = [QueueEvent::default(); 1];
        assert_eq!(region.queue().drain(&mut out), 0);
    }

    #[test]
    fn decline_when_variable_missing() {
        let (_buf, region) = build_zone(8, 16);
        let rules = build_rules();
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings::default(),
        };
        let vars = MockVars::new(); // no http_x_tenant set
        let outcome = decide(ctx, 0, &vars, 1_000);
        assert!(matches!(outcome, AccessOutcome::Decline));
    }

    #[test]
    fn cardinality_settings_reject_oversized_descriptors() {
        let (_buf, region) = build_zone(8, 16);
        let rules = build_rules();
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings {
                max_descriptor_count: defaults::STORAGE_MAX_DESCRIPTOR_COUNT,
                max_descriptor_bytes: crate::rules::DEFAULT_DOMAIN.len() + "tenant".len(),
                max_key_bytes: defaults::STORAGE_MAX_KEY_BYTES,
            },
        };
        let vars = MockVars::new().set("http_x_tenant", "alice");

        let outcome = decide(ctx, 0, &vars, 1_000);

        assert!(matches!(outcome, AccessOutcome::Cardinality));
    }

    #[test]
    fn default_cardinality_settings_match_shared_defaults() {
        let settings = CardinalitySettings::default();
        assert_eq!(
            settings.max_descriptor_count,
            defaults::STORAGE_MAX_DESCRIPTOR_COUNT
        );
        assert_eq!(
            settings.max_descriptor_bytes,
            defaults::STORAGE_MAX_DESCRIPTOR_BYTES
        );
        assert_eq!(settings.max_key_bytes, defaults::STORAGE_MAX_KEY_BYTES);
        assert_eq!(MAX_DESCRIPTOR_BYTES, defaults::STORAGE_MAX_DESCRIPTOR_BYTES);
        assert_eq!(MAX_KEY_BYTES, defaults::STORAGE_MAX_KEY_BYTES);
    }

    #[test]
    fn invalid_utf8_descriptor_declines_and_bumps_counter() {
        let (_buf, region) = build_zone(8, 16);
        let rules = build_rules();
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings::default(),
        };
        // `0xFF` is never a valid UTF-8 lead byte — guaranteed decline.
        let vars = MockVars::new().set_bytes("http_x_tenant", &[0xFFu8, 0xFE, 0xFD]);
        let outcome = decide(ctx, 0, &vars, 1_000);
        assert!(matches!(outcome, AccessOutcome::Decline));
        let snap = region.stats().snapshot();
        assert_eq!(snap.declines_invalid_descriptor, 1);
        // Allow-by-default: a UTF-8 decline does NOT increment the
        // user-facing reject counter.
        assert_eq!(snap.rejected, 0);
        assert_eq!(snap.rejected_cardinality, 0);
    }

    #[test]
    fn retry_after_seconds_matches_window() {
        let info = RejectInfo {
            spec: RuleSpec {
                id: 1,
                fingerprint: 0,
                limit: 1,
                bucket_millis: 1_000,
                window_millis: 60_000,
                live_buckets: 60,
            },
            total: 1,
            now_millis: 0,
        };
        assert_eq!(retry_after_seconds(info), 60);
    }

    #[test]
    fn is_truthy_matches_documented_falsy_set() {
        for falsy in [
            b"".as_ref(),
            b"0",
            b"false",
            b"False",
            b"FALSE",
            b"off",
            b"Off",
            b"OFF",
            b"no",
            b"No",
            b"NO",
        ] {
            assert!(!is_truthy(falsy), "{:?} should be falsy", falsy);
        }
        for truthy in [b"1".as_ref(), b"true", b"yes", b"on", b"anything", b" "] {
            assert!(is_truthy(truthy), "{:?} should be truthy", truthy);
        }
    }

    fn rule_with_predicate(predicate: Option<&str>) -> CompiledRules {
        CompiledRules::compile(&[RuleConfig {
            name: "per_tenant".to_string(),
            domain: crate::rules::DEFAULT_DOMAIN.to_string(),
            bindings: vec![DescriptorBinding {
                key: "tenant".to_string(),
                source: "$http_x_tenant".to_string(),
            }],
            limit: 2,
            window: Duration::from_secs(1),
            bucket: Duration::from_millis(250),
            mode: EnforcementMode::Enforce,
            except_if: predicate.map(Into::into),
        }])
        .expect("compile rules")
    }

    #[test]
    fn except_truthy_skips_rule() {
        let (_buf, region) = build_zone(8, 16);
        let rules = rule_with_predicate(Some("trusted"));
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings::default(),
        };
        let vars = MockVars::new()
            .set("http_x_tenant", "alice")
            .set("trusted", "1");
        let outcome = decide(ctx, 0, &vars, 0);
        assert!(matches!(outcome, AccessOutcome::Allow));
        let snap = region.stats().snapshot();
        assert_eq!(snap.exempted, 1, "exempt counter should fire");
        assert_eq!(snap.allowed, 0, "exempt is not a normal allow");
        // No queue event from an exempted request — under-count rather
        // than over-count when the predicate fires.
        let mut out = [QueueEvent::default(); 1];
        assert_eq!(region.queue().drain(&mut out), 0);
    }

    #[test]
    fn except_falsy_applies_rule() {
        let (_buf, region) = build_zone(8, 16);
        let rules = rule_with_predicate(Some("trusted"));
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings::default(),
        };
        let vars = MockVars::new()
            .set("http_x_tenant", "alice")
            .set("trusted", "off");
        let outcome = decide(ctx, 0, &vars, 0);
        assert!(matches!(outcome, AccessOutcome::Allow));
        let snap = region.stats().snapshot();
        assert_eq!(snap.exempted, 0);
        assert_eq!(snap.allowed, 1);
    }

    #[test]
    fn except_missing_applies_rule() {
        let (_buf, region) = build_zone(8, 16);
        let rules = rule_with_predicate(Some("trusted"));
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings::default(),
        };
        // `trusted` is not set — the predicate variable resolves to None;
        // per allow-by-default and the documented semantic, the rule still
        // applies (the operator's intent for predicate=set is opt-in
        // exemption, not opt-in enforcement).
        let vars = MockVars::new().set("http_x_tenant", "alice");
        let outcome = decide(ctx, 0, &vars, 0);
        assert!(matches!(outcome, AccessOutcome::Allow));
        let snap = region.stats().snapshot();
        assert_eq!(snap.exempted, 0);
        assert_eq!(snap.allowed, 1);
    }

    #[test]
    fn except_empty_applies_rule() {
        let (_buf, region) = build_zone(8, 16);
        let rules = rule_with_predicate(Some("trusted"));
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings::default(),
        };
        let vars = MockVars::new()
            .set("http_x_tenant", "alice")
            .set("trusted", "");
        let outcome = decide(ctx, 0, &vars, 0);
        assert!(matches!(outcome, AccessOutcome::Allow));
        let snap = region.stats().snapshot();
        assert_eq!(snap.exempted, 0);
        assert_eq!(snap.allowed, 1);
    }

    /// Build a `CompiledRules` with two rules sharing one descriptor key
    /// so a single `Descriptor { key: "tenant", ... }` matches both — the
    /// stacked-rule shape `decide_all` exists for.
    fn build_two_rules(
        first_limit: u64,
        first_window_secs: u64,
        second_limit: u64,
        second_window_secs: u64,
    ) -> CompiledRules {
        let mk = |name: &str, limit: u64, window_secs: u64| RuleConfig {
            name: name.to_string(),
            domain: crate::rules::DEFAULT_DOMAIN.to_string(),
            bindings: vec![DescriptorBinding {
                key: "tenant".to_string(),
                source: "$http_x_tenant".to_string(),
            }],
            limit,
            window: Duration::from_secs(window_secs),
            bucket: Duration::from_millis(250),
            mode: EnforcementMode::Enforce,
            except_if: None,
        };
        CompiledRules::compile(&[
            mk("first", first_limit, first_window_secs),
            mk("second", second_limit, second_window_secs),
        ])
        .expect("compile two rules")
    }

    #[test]
    fn decide_all_allow_when_all_pass() {
        let (_buf, region) = build_zone(8, 16);
        let rules = build_two_rules(10, 1, 10, 60);
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings::default(),
        };
        let vars = MockVars::new().set("http_x_tenant", "alice");
        let outcome = decide_all(ctx, &[0, 1], &vars, 0);
        assert!(matches!(outcome, AccessOutcome::Allow));
        // Both rules push a QueueEvent because both matched.
        let mut out = [QueueEvent::default(); 4];
        assert_eq!(region.queue().drain(&mut out), 2);
        let snap = region.stats().snapshot();
        assert_eq!(snap.allowed, 1);
        assert_eq!(snap.rejected, 0);
    }

    #[test]
    fn decide_all_decline_when_all_decline() {
        let (_buf, region) = build_zone(8, 16);
        let rules = build_two_rules(10, 1, 10, 60);
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings::default(),
        };
        // No variable set — every rule declines.
        let vars = MockVars::new();
        let outcome = decide_all(ctx, &[0, 1], &vars, 0);
        assert!(matches!(outcome, AccessOutcome::Decline));
        let snap = region.stats().snapshot();
        assert_eq!(snap.allowed, 0);
        assert_eq!(snap.rejected, 0);
    }

    #[test]
    fn decide_all_picks_longest_window() {
        let (_buf, region) = build_zone(8, 16);
        // First rule: 1s window, limit 1; second: 60s window, limit 1.
        let rules = build_two_rules(1, 1, 1, 60);
        let domain = crate::rules::DEFAULT_DOMAIN;
        let descriptors = [Descriptor {
            key: "tenant",
            value: "alice",
        }];
        // Pre-populate aggregate so BOTH rules are over their budget.
        // SAFETY: same as the other tests in this file (build_zone owns
        // the backing allocation; the store is the sole writer here).
        let store = unsafe {
            crate::shm::aggregate::ShmAggregateStore::new(
                region.aggregate_slots_ptr(),
                region.layout.aggregate_capacity,
            )
        };
        for rule in rules.rules() {
            let spec = rule.rule.spec();
            let key_hash = hash_key(spec.id, domain, &descriptors);
            store.write_delta(spec.fingerprint, key_hash.0, 0, 5, 0);
        }
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain,
            cardinality: CardinalitySettings::default(),
        };
        let vars = MockVars::new().set("http_x_tenant", "alice");
        let outcome = decide_all(ctx, &[0, 1], &vars, 0);
        match outcome {
            AccessOutcome::Reject(info) => {
                assert_eq!(
                    info.spec.window_millis, 60_000,
                    "should pick the 60s rule for `Retry-After`"
                );
            }
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    #[test]
    fn decide_all_one_declines_one_allows() {
        let (_buf, region) = build_zone(8, 16);
        // First rule keyed on a missing variable so it declines; second
        // rule has the variable set so it allows.
        let rule_a = RuleConfig {
            name: "missing".to_string(),
            domain: crate::rules::DEFAULT_DOMAIN.to_string(),
            bindings: vec![DescriptorBinding {
                key: "nonexistent".to_string(),
                source: "$does_not_exist".to_string(),
            }],
            limit: 10,
            window: Duration::from_secs(1),
            bucket: Duration::from_millis(250),
            mode: EnforcementMode::Enforce,
            except_if: None,
        };
        let rule_b = RuleConfig {
            name: "tenant".to_string(),
            domain: crate::rules::DEFAULT_DOMAIN.to_string(),
            bindings: vec![DescriptorBinding {
                key: "tenant".to_string(),
                source: "$http_x_tenant".to_string(),
            }],
            limit: 10,
            window: Duration::from_secs(1),
            bucket: Duration::from_millis(250),
            mode: EnforcementMode::Enforce,
            except_if: None,
        };
        let rules = CompiledRules::compile(&[rule_a, rule_b]).expect("compile");
        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain: crate::rules::DEFAULT_DOMAIN,
            cardinality: CardinalitySettings::default(),
        };
        let vars = MockVars::new().set("http_x_tenant", "alice");
        let outcome = decide_all(ctx, &[0, 1], &vars, 0);
        assert!(matches!(outcome, AccessOutcome::Allow));
        // Only one rule (the second) produced an event.
        let mut out = [QueueEvent::default(); 4];
        assert_eq!(region.queue().drain(&mut out), 1);
    }

    #[test]
    fn dry_run_records_but_never_rejects() {
        let (_buf, region) = build_zone(8, 16);
        let cfg = RuleConfig {
            name: "shadow".to_string(),
            domain: crate::rules::DEFAULT_DOMAIN.to_string(),
            bindings: vec![DescriptorBinding {
                key: "tenant".to_string(),
                source: "$http_x_tenant".to_string(),
            }],
            limit: 1,
            window: Duration::from_secs(1),
            bucket: Duration::from_millis(250),
            mode: EnforcementMode::DryRun,
            except_if: None,
        };
        let rules = CompiledRules::compile(&[cfg]).expect("compile");
        let domain = crate::rules::DEFAULT_DOMAIN;

        // Pre-populate so the aggregate is way over the limit.
        // SAFETY: see notes on the other ShmAggregateStore::new uses in
        // this module — same single-writer test pattern.
        let store = unsafe {
            crate::shm::aggregate::ShmAggregateStore::new(
                region.aggregate_slots_ptr(),
                region.layout.aggregate_capacity,
            )
        };
        let spec = rules.rules()[0].rule.spec();
        let descriptors = [Descriptor {
            key: "tenant",
            value: "alice",
        }];
        let key_hash = hash_key(spec.id, domain, &descriptors);
        store.write_delta(spec.fingerprint, key_hash.0, 0, 99, 0);

        let ctx = AccessCtx {
            rules: &rules,
            aggregate: region.aggregate(),
            queue: region.queue(),
            stats: region.stats(),
            domain,
            cardinality: CardinalitySettings::default(),
        };
        let vars = MockVars::new().set("http_x_tenant", "alice");
        // DryRun: over the limit but never rejects.
        let outcome = decide_all(ctx, &[0], &vars, 0);
        assert!(matches!(outcome, AccessOutcome::Allow));
        // Still records the hit for the gossip aggregate.
        let mut out = [QueueEvent::default(); 4];
        assert_eq!(region.queue().drain(&mut out), 1);
        let snap = region.stats().snapshot();
        assert_eq!(snap.rejected, 0);
        assert_eq!(snap.allowed, 1);
    }

    #[test]
    fn reset_unix_seconds_is_now_plus_window() {
        let info = RejectInfo {
            spec: RuleSpec {
                id: 1,
                fingerprint: 0,
                limit: 1,
                bucket_millis: 1_000,
                window_millis: 60_000,
                live_buckets: 60,
            },
            total: 1,
            now_millis: 1_770_000_000_000,
        };
        assert_eq!(reset_unix_seconds(info), 1_770_000_000 + 60);
    }
}
