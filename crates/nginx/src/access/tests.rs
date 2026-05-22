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
        name: "per_tenant".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "tenant".into(),
            source: "$http_x_tenant".into(),
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
        name: "per_tenant".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "tenant".into(),
            source: "$http_x_tenant".into(),
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
        name: name.into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "tenant".into(),
            source: "$http_x_tenant".into(),
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
        name: "missing".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "nonexistent".into(),
            source: "$does_not_exist".into(),
        }],
        limit: 10,
        window: Duration::from_secs(1),
        bucket: Duration::from_millis(250),
        mode: EnforcementMode::Enforce,
        except_if: None,
    };
    let rule_b = RuleConfig {
        name: "tenant".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "tenant".into(),
            source: "$http_x_tenant".into(),
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
        name: "shadow".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "tenant".into(),
            source: "$http_x_tenant".into(),
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

/// Build two rules with different descriptor keys so the per-request
/// cardinality budget can trip one without affecting the other.
fn build_two_rules_varied(name_a: &str, key_a: &str, name_b: &str, key_b: &str) -> CompiledRules {
    let mk = |name: &str, key: &str| RuleConfig {
        name: name.into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: key.into(),
            source: "$http_x_tenant".into(),
        }],
        limit: 10,
        window: Duration::from_secs(1),
        bucket: Duration::from_millis(250),
        mode: EnforcementMode::Enforce,
        except_if: None,
    };
    CompiledRules::compile(&[mk(name_a, key_a), mk(name_b, key_b)]).expect("compile two rules")
}

#[test]
fn decide_all_one_rule_cardinality_others_still_evaluate() {
    let (_buf, region) = build_zone(8, 16);
    let rules = build_two_rules_varied("a", "long_descriptor_key_name", "b", "k");
    let domain = crate::rules::DEFAULT_DOMAIN;
    let budget = domain.len() + 1 + "alice".len();
    let ctx = AccessCtx {
        rules: &rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        domain,
        cardinality: CardinalitySettings {
            max_descriptor_bytes: budget,
            ..CardinalitySettings::default()
        },
    };
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide_all(ctx, &[0, 1], &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Allow));
    let snap = region.stats().snapshot();
    assert_eq!(snap.allowed, 1);
    assert_eq!(snap.rejected_cardinality, 1, "one rule tripped cardinality");
    let mut out = [QueueEvent::default(); 4];
    assert_eq!(region.queue().drain(&mut out), 1);
}

#[test]
fn decide_all_all_rules_cardinality_returns_cardinality() {
    let (_buf, region) = build_zone(8, 16);
    let rules = build_two_rules_varied(
        "a",
        "long_descriptor_key_name_a",
        "b",
        "long_descriptor_key_name_b",
    );
    let domain = crate::rules::DEFAULT_DOMAIN;
    let ctx = AccessCtx {
        rules: &rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        domain,
        cardinality: CardinalitySettings {
            max_descriptor_bytes: domain.len() + 1,
            ..CardinalitySettings::default()
        },
    };
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide_all(ctx, &[0, 1], &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Cardinality));
    let snap = region.stats().snapshot();
    assert_eq!(snap.rejected_cardinality, 2);
    assert_eq!(snap.allowed, 0);
}

#[test]
fn decide_all_mixed_decline_and_cardinality_returns_cardinality() {
    let (_buf, region) = build_zone(8, 16);
    let cfg_a = RuleConfig {
        name: "a".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "k".into(),
            source: "$does_not_exist".into(),
        }],
        limit: 10,
        window: Duration::from_secs(1),
        bucket: Duration::from_millis(250),
        mode: EnforcementMode::Enforce,
        except_if: None,
    };
    let cfg_b = RuleConfig {
        name: "b".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "long_descriptor_key".into(),
            source: "$http_x_tenant".into(),
        }],
        limit: 10,
        window: Duration::from_secs(1),
        bucket: Duration::from_millis(250),
        mode: EnforcementMode::Enforce,
        except_if: None,
    };
    let rules = CompiledRules::compile(&[cfg_a, cfg_b]).expect("compile");
    let domain = crate::rules::DEFAULT_DOMAIN;
    let ctx = AccessCtx {
        rules: &rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        domain,
        cardinality: CardinalitySettings {
            max_descriptor_bytes: domain.len() + 1,
            ..CardinalitySettings::default()
        },
    };
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide_all(ctx, &[0, 1], &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Cardinality));
}

#[test]
fn decide_all_one_rule_exempt_one_rejects() {
    let (_buf, region) = build_zone(8, 16);
    let cfg_a = RuleConfig {
        name: "a".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "tenant".into(),
            source: "$http_x_tenant".into(),
        }],
        limit: 10,
        window: Duration::from_secs(1),
        bucket: Duration::from_millis(250),
        mode: EnforcementMode::Enforce,
        except_if: Some("trusted".into()),
    };
    let cfg_b = RuleConfig {
        name: "b".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "k".into(),
            source: "$http_x_tenant".into(),
        }],
        limit: 1,
        window: Duration::from_secs(1),
        bucket: Duration::from_millis(250),
        mode: EnforcementMode::Enforce,
        except_if: None,
    };
    let rules = CompiledRules::compile(&[cfg_a, cfg_b]).expect("compile");
    let domain = crate::rules::DEFAULT_DOMAIN;

    // SAFETY: see notes on the other ShmAggregateStore::new uses in this
    // module — same single-writer test pattern.
    let store = unsafe {
        crate::shm::aggregate::ShmAggregateStore::new(
            region.aggregate_slots_ptr(),
            region.layout.aggregate_capacity,
        )
    };
    let spec_b = rules.rules()[1].rule.spec();
    let descriptors_b = [Descriptor {
        key: "k",
        value: "alice",
    }];
    let key_hash_b = hash_key(spec_b.id, domain, &descriptors_b);
    store.write_delta(spec_b.fingerprint, key_hash_b.0, 0, 5, 0);

    let ctx = AccessCtx {
        rules: &rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        domain,
        cardinality: CardinalitySettings::default(),
    };
    let vars = MockVars::new()
        .set("http_x_tenant", "alice")
        .set("trusted", "1");
    let outcome = decide_all(ctx, &[0, 1], &vars, 0);
    match outcome {
        AccessOutcome::Reject(info) => assert_eq!(info.spec.id, spec_b.id),
        other => panic!("expected Reject, got {other:?}"),
    }
    let snap = region.stats().snapshot();
    assert_eq!(snap.exempted, 1, "rule a still recorded its exemption");
    assert_eq!(snap.rejected, 1);
}

#[test]
fn cardinality_skip_does_not_push_queue_event() {
    let (_buf, region) = build_zone(8, 16);
    let rules = build_rules();
    let ctx = AccessCtx {
        rules: &rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        domain: crate::rules::DEFAULT_DOMAIN,
        cardinality: CardinalitySettings {
            max_descriptor_bytes: crate::rules::DEFAULT_DOMAIN.len() + 1,
            ..CardinalitySettings::default()
        },
    };
    let vars = MockVars::new().set("http_x_tenant", "alice");
    let outcome = decide(ctx, 0, &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Cardinality));
    let mut out = [QueueEvent::default(); 4];
    assert_eq!(region.queue().drain(&mut out), 0);
}

#[test]
fn except_does_not_count_cardinality() {
    let (_buf, region) = build_zone(8, 16);
    // Predicate truthy — request is exempted before the byte-budget check.
    let rules = rule_with_predicate(Some("trusted"));
    let ctx = AccessCtx {
        rules: &rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        domain: crate::rules::DEFAULT_DOMAIN,
        cardinality: CardinalitySettings {
            max_descriptor_bytes: 1,
            ..CardinalitySettings::default()
        },
    };
    let vars = MockVars::new()
        .set("http_x_tenant", "alice")
        .set("trusted", "1");
    let outcome = decide(ctx, 0, &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Allow));
    let snap = region.stats().snapshot();
    assert_eq!(
        snap.rejected_cardinality, 0,
        "exempt path skips budget check"
    );
    assert_eq!(snap.exempted, 1);
}

#[test]
fn per_rule_exempt_counter_bumps_only_target_rule() {
    let (_buf, region) = build_zone(8, 16);
    let cfg_a = RuleConfig {
        name: "a".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "tenant_a".into(),
            source: "$http_x_tenant".into(),
        }],
        limit: 10,
        window: Duration::from_secs(1),
        bucket: Duration::from_millis(250),
        mode: EnforcementMode::Enforce,
        except_if: Some("trusted_a".into()),
    };
    let cfg_b = RuleConfig {
        name: "b".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "tenant_b".into(),
            source: "$http_x_tenant".into(),
        }],
        limit: 10,
        window: Duration::from_secs(1),
        bucket: Duration::from_millis(250),
        mode: EnforcementMode::Enforce,
        except_if: Some("trusted_b".into()),
    };
    let rules = CompiledRules::compile(&[cfg_a, cfg_b]).expect("compile");
    let ctx = AccessCtx {
        rules: &rules,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        domain: crate::rules::DEFAULT_DOMAIN,
        cardinality: CardinalitySettings::default(),
    };
    // Rule a exempts (truthy); rule b applies (falsy predicate).
    let vars = MockVars::new()
        .set("http_x_tenant", "alice")
        .set("trusted_a", "1")
        .set("trusted_b", "0");
    let outcome = decide_all(ctx, &[0, 1], &vars, 0);
    assert!(matches!(outcome, AccessOutcome::Allow));
    let snap = region.stats().snapshot();
    // Rule ids are assigned 1, 2 in declaration order; per-rule slot is
    // indexed by `rule_id - 1`.
    assert_eq!(snap.exempted_per_rule[0], 1, "rule a exempted");
    assert_eq!(snap.exempted_per_rule[1], 0, "rule b not exempted");
    assert_eq!(snap.exempted, 1);
}

#[test]
fn unknown_predicate_variable_fails_at_compile() {
    use crate::rules::RuleConfigError;
    // The NopBindingCompiler rejects anything that isn't a single
    // `$identifier` or one of the inline fast-path arms. A hyphen in the
    // identifier trips the legal-ident check, so the source `$bad-ident`
    // forwarded through `except_if=` fails compilation.
    let cfg = RuleConfig {
        name: "bad".into(),
        domain: crate::rules::DEFAULT_DOMAIN.into(),
        bindings: vec![DescriptorBinding {
            key: "tenant".into(),
            source: "$http_x_tenant".into(),
        }],
        limit: 10,
        window: Duration::from_secs(1),
        bucket: Duration::from_millis(250),
        mode: EnforcementMode::Enforce,
        except_if: Some("bad-ident".into()),
    };
    let err = CompiledRules::compile(&[cfg]).expect_err("predicate compile rejected");
    assert!(
        matches!(err, RuleConfigError::CompileBinding { .. }),
        "expected CompileBinding, got {err:?}"
    );
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
