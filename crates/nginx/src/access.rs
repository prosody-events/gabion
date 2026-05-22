//! Per-request access decision. Pure logic — no `ngx` imports — so the
//! hot path is exercisable from unit tests.
//!
//! Allocation discipline: descriptors live in a stack-resident `ArrayVec`;
//! `RuleTable::matching` returns an iterator we walk without collecting;
//! the SHM aggregate is read through `AggregateTable::window_total` which
//! does only atomic loads.

use arrayvec::ArrayVec;
use gabion::rules::{Descriptor, RuleTable, hash_key};

use crate::rules::{CompiledRules, MAX_DESCRIPTORS, RuleSpec};
use crate::shm::aggregate::AggregateTable;
use crate::shm::queue::{QueueEvent, RequestQueue};
use crate::shm::stats::Stats;

/// Maximum descriptor bytes per request (key + value, summed across all
/// descriptors plus the domain). Matches the server's default cardinality
/// envelope.
pub const MAX_DESCRIPTOR_BYTES: usize = 512;

/// Maximum per-descriptor key length in bytes.
pub const MAX_KEY_BYTES: usize = 128;

/// Look up an nginx variable's value. The access path borrows directly from
/// nginx-owned buffers; the `&[u8]` returned must live for the duration of
/// `decide`. A return value of `None` skips the rule for the request.
pub trait VariableLookup {
    fn value(&self, name: &str) -> Option<&[u8]>;
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
}

/// Evaluate one request against the rule indicated by `rule_index`.
///
/// Builds the descriptors from nginx variables on the stack, runs
/// `RuleTable::matching` to find all rules that apply (typically just the
/// one configured by location), then records hits into the SHM queue and
/// reads the aggregate window total. `RejectInfo` carries everything the
/// headers builder needs.
pub fn decide(
    ctx: AccessCtx<'_>,
    rule_index: usize,
    vars: &impl VariableLookup,
    now_millis: u64,
) -> AccessOutcome {
    ctx.stats.record_request();

    let Some(compiled) = ctx.rules.get(rule_index) else {
        return AccessOutcome::Decline;
    };

    // Build descriptors on the stack — no heap allocation.
    let mut value_storage: ArrayVec<&[u8], MAX_DESCRIPTORS> = ArrayVec::new();
    for binding in &compiled.bindings {
        match vars.value(&binding.variable) {
            Some(value) => {
                if value_storage.try_push(value).is_err() {
                    return AccessOutcome::Decline;
                }
            }
            None => return AccessOutcome::Decline,
        }
    }

    // Validate cardinality before any rule work. `bindings.len()` is
    // gated at compile time; only per-request byte length is dynamic.
    let mut bytes = ctx.domain.len();
    for (binding, value) in compiled.bindings.iter().zip(value_storage.iter()) {
        if binding.key.len() > MAX_KEY_BYTES {
            ctx.stats.record_cardinality_reject();
            return AccessOutcome::Cardinality;
        }
        bytes = bytes
            .saturating_add(binding.key.len())
            .saturating_add(value.len());
        if bytes > MAX_DESCRIPTOR_BYTES {
            ctx.stats.record_cardinality_reject();
            return AccessOutcome::Cardinality;
        }
    }

    // Build the `Descriptor<'_>` slice borrowed from the value storage.
    let mut descriptors: ArrayVec<Descriptor<'_>, MAX_DESCRIPTORS> = ArrayVec::new();
    for (binding, value) in compiled.bindings.iter().zip(value_storage.iter()) {
        // SAFETY: values come from nginx variables; they may be non-utf8 in
        // theory but the rate-limit hashing treats them as bytes via
        // `Descriptor`'s `&str` field. Defer the utf8 check to the borrow
        // site — `from_utf8` lossy is too expensive; we use `from_utf8` and
        // decline on error.
        let value_str = match std::str::from_utf8(value) {
            Ok(s) => s,
            Err(_) => return AccessOutcome::Decline,
        };
        descriptors.push(Descriptor {
            key: binding.key.as_str(),
            value: value_str,
        });
    }

    // Walk matching rules — typically a single rule (the one named by the
    // location), but we honor the rule table's semantics so multiple rules
    // could match a single request.
    let descriptors_slice: &[Descriptor<'_>] = descriptors.as_slice();
    let mut worst: Option<RejectInfo> = None;
    let matched: ArrayVec<RuleSpec, MAX_DESCRIPTORS> =
        collect_matching_specs(ctx.rules.table(), ctx.domain, descriptors_slice, ctx.rules);

    for spec in &matched {
        let key_hash = hash_key(spec.id, ctx.domain, descriptors_slice);
        let bucket = (now_millis / spec.bucket_millis.max(1)) as u32;
        let total = ctx.aggregate.window_total(
            spec.fingerprint,
            key_hash.0,
            now_millis,
            spec.bucket_millis,
            spec.live_buckets,
        );
        if total.saturating_add(1) > spec.limit && worst.is_none() {
            worst = Some(RejectInfo {
                spec: *spec,
                total,
                now_millis,
            });
        }
        // Record-then-read penalty rate: enqueue regardless of allow/reject.
        let push_result = ctx.queue.push(QueueEvent {
            rule_fingerprint: spec.fingerprint,
            key_hash: key_hash.0,
            bucket,
            hits: 1,
            now_millis,
        });
        match push_result {
            Ok(()) => ctx.stats.record_queue_push(),
            Err(_) => ctx.stats.record_queue_drop(),
        }
    }

    match worst {
        Some(info) => {
            ctx.stats.record_reject();
            AccessOutcome::Reject(info)
        }
        None => {
            if matched.is_empty() {
                AccessOutcome::Decline
            } else {
                ctx.stats.record_allow();
                AccessOutcome::Allow
            }
        }
    }
}

fn collect_matching_specs(
    table: &RuleTable,
    domain: &str,
    descriptors: &[Descriptor<'_>],
    rules: &CompiledRules,
) -> ArrayVec<RuleSpec, MAX_DESCRIPTORS> {
    let mut out: ArrayVec<RuleSpec, MAX_DESCRIPTORS> = ArrayVec::new();
    for rule in table.matching(domain, descriptors) {
        let Some(compiled) = rules.rules().iter().find(|r| r.rule.id == rule.id) else {
            continue;
        };
        if out.try_push(compiled.spec).is_err() {
            break;
        }
    }
    out
}

/// Delta-seconds the client should wait before retrying. Emitted as
/// `Retry-After` (RFC 7231 §7.1.3).
///
/// Under a sliding window, a hit recorded "now" stays in the window
/// until `now + window_millis`. The safest answer — and the one least
/// likely to send the client into another 429 — is the full window in
/// seconds. We deliberately don't emit "seconds until the next bucket
/// boundary" (often ~1s): under sliding windows that number is
/// misleading because the rejected client's earlier hits are still
/// counted at the next boundary.
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

    pub struct MockVars {
        pub vars: HashMap<String, Vec<u8>>,
    }

    impl MockVars {
        pub fn new() -> Self {
            Self {
                vars: HashMap::new(),
            }
        }

        pub fn set(mut self, name: &str, value: &str) -> Self {
            self.vars
                .insert(name.to_string(), value.as_bytes().to_vec());
            self
        }
    }

    impl VariableLookup for MockVars {
        fn value(&self, name: &str) -> Option<&[u8]> {
            self.vars.get(name).map(Vec::as_slice)
        }
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
        // * `zone.as_ptr()` is non-null, writable, and 8-byte aligned (the
        //   underlying allocation was a `Box<[u64]>`). `words * 8 >=
        //   layout.total_bytes`, so the mapping size is covered.
        // * `Layout::new` produced `layout` above; offsets are consistent.
        // * `zone` is a fresh allocation; `initialize` is its first user.
        // * The returned `TestZone` is paired with the `ShmRegion` and
        //   dropped after every use of the region in each `#[test]`.
        let region = unsafe { ShmRegion::initialize(zone.as_ptr(), layout) };
        (zone, region)
    }

    fn build_rules() -> CompiledRules {
        CompiledRules::compile(&[RuleConfig {
            name: "per_tenant".to_string(),
            domain: crate::rules::DEFAULT_DOMAIN.to_string(),
            bindings: vec![DescriptorBinding {
                key: "tenant".to_string(),
                variable: "http_x_tenant".to_string(),
            }],
            limit: 2,
            window: Duration::from_secs(1),
            bucket: Duration::from_millis(250),
            mode: EnforcementMode::Enforce,
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
        let spec = rules.rules()[0].spec;
        let descriptors = [Descriptor {
            key: "tenant",
            value: "alice",
        }];
        let key_hash = hash_key(spec.id, domain, &descriptors);
        let bucket = (1_000_u64 / spec.bucket_millis.max(1)) as u32;
        // Pre-populate aggregate so total >= limit (limit is 2).
        // SAFETY: `ShmAggregateStore::new`'s preconditions (see its
        // `# Safety` doc) are upheld:
        // * `region` was produced by `build_zone`, which called
        //   `ShmRegion::initialize`. `initialize` `ptr::write`s an
        //   `AggregateSlot::empty()` into each of the
        //   `layout.aggregate_capacity` slots, so
        //   `region.aggregate_slots_ptr()` is non-null, aligned for
        //   `AggregateSlot`, and points at exactly that many fully
        //   initialized slots.
        // * The `capacity` argument is the same `aggregate_capacity` the
        //   region was built with (`Layout::new` guarantees it is a power
        //   of two `>= 2`, and the total byte length fits in the buffer
        //   and thus in `isize::MAX`).
        // * The backing `Vec<u8>` (`_buf` in the caller) lives until the
        //   end of the `#[test]`, which outlives both this `store` and any
        //   `AggregateTable<'_>` derived from it via `region.aggregate()`.
        // * Single-writer: this test is single-threaded and `store` is the
        //   sole `ShmAggregateStore` constructed against `region`'s
        //   aggregate slots; reads from `region.aggregate()` use the same
        //   seqlock/atomic protocol the production code does, so they
        //   cannot race with `write_delta` (Nomicon: atomic accesses are
        //   not data races even with concurrent reads).
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
        };
        let vars = MockVars::new(); // no http_x_tenant set
        let outcome = decide(ctx, 0, &vars, 1_000);
        assert!(matches!(outcome, AccessOutcome::Decline));
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
