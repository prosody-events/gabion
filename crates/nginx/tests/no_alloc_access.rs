//! N2 for NGINX: the access phase allocates nothing once the worker's
//! admission scratch is reserved, even with the matched-rule bound at its
//! largest useful value and every rule on the location matching.
//!
//! A counting global allocator, armed on this thread only around each
//! `decide_all`, counts every allocation the section makes. The queue is
//! drained between requests, outside the counted window, as the leader
//! drains it.
//!
//! Miri exemption: the allocator only forwards to `System`, and the unsafe
//! it holds is that forwarding, not the cross-process code `safety.rs`
//! covers, so this binary runs under nextest only.

use std::alloc::{GlobalAlloc, Layout as AllocLayout, System};
use std::cell::Cell;
use std::hint::black_box;

use gabion::crdt::{CellHandle, CellIdentity, DeltaSink, ExpirationSink};
use gabion::gossip::AggregateStore;
use gabion::rules::{Descriptor, hash_key};
use gabion::settings::Settings;
use gabion_nginx::access::{AccessCtx, AccessOutcome, VariableLookup, decide_all};
use gabion_nginx::rules::{
    BindingLookup, CompiledRules, NopBindingCompiler, RuleConfig, parse_rule_directive,
};
use gabion_nginx::shm::queue::QueueEvent;
use gabion_nginx::shm::{Layout, ShmAggregateStore, ShmRegion};

thread_local! {
    /// Set on this thread while a counted section runs.
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

struct Counting;

// SAFETY: every method forwards its arguments unchanged to `System`, which
// upholds `GlobalAlloc`'s contract; the bookkeeping touches only
// const-initialised thread-locals with no destructor, so it never allocates
// or re-enters the allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: AllocLayout) -> *mut u8 {
        note();
        // SAFETY: the caller's guarantees for `layout` pass straight through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: AllocLayout) {
        // SAFETY: `ptr` came from `System` through `alloc` or `realloc`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: AllocLayout, new_size: usize) -> *mut u8 {
        note();
        // SAFETY: the caller's guarantees pass straight through.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn note() {
    if COUNTING.get() {
        ALLOCATIONS.set(ALLOCATIONS.get() + 1);
    }
}

/// Allocations `body` makes on this thread.
fn allocations<T>(body: impl FnOnce() -> T) -> (T, u64) {
    let before = ALLOCATIONS.get();
    COUNTING.set(true);
    let value = body();
    COUNTING.set(false);
    (value, ALLOCATIONS.get() - before)
}

/// The request's `$http_x_tenant`.
struct Tenant<'a>(&'a [u8]);

impl VariableLookup for Tenant<'_> {
    fn lookup(&self, binding: &BindingLookup) -> Option<&[u8]> {
        matches!(binding, BindingLookup::IndexedVariable { name, .. } if &**name == "http_x_tenant")
            .then_some(self.0)
    }
}

/// A region over a leaked allocation, built before counting.
fn region(queue: usize) -> ShmRegion {
    let layout = Layout::new(queue, 1 << 14).expect("layout");
    let zone: &'static mut [u64] =
        Box::leak(vec![0_u64; layout.total_bytes.div_ceil(8)].into_boxed_slice());
    // SAFETY: `zone` is a fresh, writable, 8-byte-aligned allocation of at
    // least `layout.total_bytes`, `initialize` is its first user, and the
    // leak keeps it alive for the whole test.
    unsafe { ShmRegion::initialize(zone.as_mut_ptr().cast(), layout) }
}

/// `rules` rules on one location, each keyed on the tenant and all matching,
/// with `storage.max_matched_rules` at `rules`; 1,000 requests after a
/// warm-up that matches no rule, 95% on keys the aggregate holds and 5% on
/// new ones.
fn admit_without_allocating(rules: u16) {
    let settings = Settings {
        max_matched_rules: rules.try_into().unwrap(),
        ..Settings::default()
    };
    let (_, storage) = settings.validate().expect("settings validate");
    let configs: Vec<RuleConfig> = (0..rules)
        .map(|i| {
            let name = format!("r{i}");
            let binding = format!("k{i}:$http_x_tenant");
            parse_rule_directive(&[&name, &binding, "rate=1000000000r/s"]).expect("directive")
        })
        .collect();
    let compiled =
        CompiledRules::compile_with(&configs, &storage, &mut NopBindingCompiler).expect("compile");
    let region = region(usize::from(rules).next_power_of_two().max(2));
    let ctx = AccessCtx {
        rules: &compiled,
        aggregate: region.aggregate(),
        queue: region.queue(),
        stats: region.stats(),
        cardinality: storage.cardinality,
        max_matched_rules: storage.max_matched_rules(),
    };

    // The hot keys' counts, as the leader would have written them.
    let hot: Vec<String> = (0..50).map(|i| format!("hot-{i}")).collect();
    let mut deltas = DeltaSink::<u32>::default();
    for (i, rule) in compiled.table().rules().iter().enumerate() {
        for tenant in &hot {
            let key = format!("k{i}");
            deltas.handles.push(CellHandle::default());
            deltas.keys.push(CellIdentity {
                rule_fingerprint: rule.fingerprint(),
                key_hash: hash_key(
                    "nginx",
                    [Descriptor {
                        key: &key,
                        value: tenant,
                    }],
                ),
                bucket: 0,
            });
            deltas.previous.push(0);
            deltas.current.push(1);
            deltas.deltas.push(1);
            deltas.applies_locally.push(1);
        }
    }
    ShmAggregateStore::new(region).apply(&deltas, &ExpirationSink::default());

    let requests: Vec<String> = (0..1_000)
        .map(|i| match i % 20 {
            0 => format!("new-{i}"),
            _ => hot[i % hot.len()].clone(),
        })
        .collect();
    let indices: Vec<usize> = (0..usize::from(rules)).collect();
    let mut events = Vec::new();
    let mut drained = vec![QueueEvent::default(); usize::from(rules)];
    // The warm-up request matches no rule, so the scratch must already hold
    // its bound from the settings: one that grew to fit the requests it had
    // seen would allocate on the first counted request.
    decide_all(ctx, &[], &Tenant(b""), 0, &mut events);
    assert!(
        allocations(|| black_box(Vec::<u8>::with_capacity(8))).1 > 0,
        "the counting allocator is live"
    );
    let mut total = 0;
    for tenant in &requests {
        let vars = Tenant(tenant.as_bytes());
        let (outcome, allocated) = allocations(|| decide_all(ctx, &indices, &vars, 0, &mut events));
        assert!(
            matches!(outcome, AccessOutcome::Allow(Some(_))),
            "{outcome:?}"
        );
        total += allocated;
        assert_eq!(region.queue().drain(&mut drained), usize::from(rules));
    }
    assert_eq!(total, 0, "{rules} rules: admission allocated");
    let stats = region.stats().snapshot();
    assert_eq!((stats.matched_rule_overflows, stats.queue_dropped), (0, 0));
}

#[test]
fn admission_never_allocates_at_the_largest_bound() {
    for rules in [1, 16, 64, 256] {
        admit_without_allocating(rules);
    }
}
