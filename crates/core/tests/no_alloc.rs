use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use gabion_core::{
    Decision, Descriptor, DescriptorMatcher, EnforcementMode, LimitRequest, LocalEngine,
    OverflowPolicy, Rule, RuleId, RuleTable, SafetyMargin, WindowSpec, hash_domain,
};

struct CountingAllocator;

static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_COUNT: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

fn allocations_during(operation: impl FnOnce()) -> usize {
    ALLOCATION_COUNT.store(0, Ordering::Relaxed);
    COUNT_ALLOCATIONS.store(true, Ordering::Relaxed);
    operation();
    COUNT_ALLOCATIONS.store(false, Ordering::Relaxed);
    ALLOCATION_COUNT.load(Ordering::Relaxed)
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

#[test]
fn existing_key_request_path_does_not_allocate() {
    let rules = RuleTable::new(vec![rule(1)]);
    let mut engine = LocalEngine::new(rules, 8, 10);
    assert_eq!(check(&mut engine, "a", 0), Decision::Allow);

    let mut decision = Decision::Reject(gabion_core::RejectReason::GlobalLimit);
    let allocations = allocations_during(|| {
        decision = check(&mut engine, "a", 1);
    });

    assert_eq!(decision, Decision::Allow);
    assert_eq!(allocations, 0);
}
