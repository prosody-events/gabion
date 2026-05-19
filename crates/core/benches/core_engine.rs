use std::hint::black_box;
use std::time::{Duration, Instant};

use gabion_core::{
    Descriptor, DescriptorMatcher, EnforcementMode, LimitRequest, LocalEngine, OverflowPolicy,
    Rule, RuleTable, SafetyMargin, WindowSpec, hash_domain,
};

fn rule() -> Rule {
    Rule {
        id: 1,
        domain_hash: hash_domain("api"),
        descriptor_matcher: DescriptorMatcher::exact_keys(["tenant"]),
        limit: 1_000_000,
        window: WindowSpec {
            size_millis: 60_000,
            bucket_count: 60,
        },
        local_fallback_limit: 1_000_000,
        local_absolute_limit: 1_000_000,
        stale_after_millis: 1_000,
        safety_margin: SafetyMargin { hits: 0 },
        overflow_policy: OverflowPolicy::UseOverflowKey,
        mode: EnforcementMode::Enforce,
    }
}

fn request<'a>(descriptors: &'a [Descriptor<'a>]) -> LimitRequest<'a> {
    LimitRequest {
        domain: "api",
        descriptors,
        hits: 1,
    }
}

fn run_for(duration: Duration, mut body: impl FnMut(u64)) -> u64 {
    let deadline = Instant::now() + duration;
    let mut iterations = 0_u64;
    while Instant::now() < deadline {
        body(iterations);
        iterations = iterations.saturating_add(1);
    }
    iterations
}

fn main() {
    let descriptors = [Descriptor {
        key: "tenant",
        value: "hot",
    }];
    let mut hot = LocalEngine::new(RuleTable::new(vec![rule()]), 1024, 60);
    let hot_iterations = run_for(Duration::from_millis(200), |iteration| {
        black_box(hot.check_and_record(request(&descriptors), iteration));
    });

    let mut cold = LocalEngine::new(RuleTable::new(vec![rule()]), 16_384, 60);
    let cold_iterations = run_for(Duration::from_millis(200), |iteration| {
        let value = if iteration & 1 == 0 {
            "cold-a"
        } else {
            "cold-b"
        };
        let descriptors = [Descriptor {
            key: "tenant",
            value,
        }];
        black_box(cold.check_and_record(request(&descriptors), iteration));
    });

    println!("core_hot_key_iterations_200ms {hot_iterations}");
    println!("core_cold_key_iterations_200ms {cold_iterations}");
}
