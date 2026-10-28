//! N2 for `gabiond`: admitting a descriptor allocates nothing once each
//! thread's admission scratch is reserved, even with the matched-rule bound
//! at its largest useful value and every configured rule matching.
//!
//! A counting global allocator, armed on this thread only around each
//! `SharedLimiter::decide`, counts every allocation the section makes. The
//! gossip runtime drains the request queue between requests, outside the
//! counted window, as a running node's would; the gRPC response, whose
//! `statuses` list tonic owns, is outside it too.
//!
//! Miri exemption: the allocator only forwards to `System`, and Miri cannot
//! usefully run a tokio runtime under a counting allocator, so this binary
//! runs under nextest only.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::Arc;

use gabion::aggregate::Aggregate;
use gabion::crdt::{CellStore, NodeId, NodeIdentity};
use gabion::gossip::sim::SimRouter;
use gabion::gossip::{GossipRuntime, TokioClock};
use gabion::rules::{Descriptor, EnforcementMode, RuleDef, RuleTable, hash_key};
use gabion::settings::Settings;
use gabion_server::admission::Decision;
use gabion_server::{RateLimitDescriptor, SharedLimiter, rate_limit_descriptor};
use tokio::task::LocalSet;

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
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note();
        // SAFETY: the caller's guarantees for `layout` pass straight through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System` through `alloc` or `realloc`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
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

fn descriptor(tenant: &str) -> RateLimitDescriptor {
    RateLimitDescriptor {
        entries: vec![rate_limit_descriptor::Entry {
            key: "tenant".into(),
            value: tenant.into(),
        }],
        ..Default::default()
    }
}

/// `rules` rules all matching every request, `storage.max_matched_rules` at
/// `rules`, and 1,000 requests after a warm-up that matches no rule, 95% on
/// existing keys and 5% on new ones.
async fn admit_without_allocating(rules: u16) {
    let settings = Settings {
        max_cells: (1 << 16).try_into().unwrap(),
        max_matched_rules: rules.try_into().unwrap(),
        limit_queue_capacity: (1 << 20).try_into().unwrap(),
        rng_seed: Some(1),
        ..Settings::default()
    };
    let (gossip, storage) = settings.validate().expect("settings validate");
    let built = (1..=rules)
        .map(|i| {
            RuleDef {
                name: &format!("r{i}"),
                domain: "api",
                patterns: &[("tenant", "*")],
                rate: &format!("1000000000000r/{i}s"),
                window: None,
                bucket: None,
                mode: EnforcementMode::Enforce,
            }
            .build(&storage.cardinality)
            .expect("rule")
        })
        .collect();
    let table = Arc::new(RuleTable::new(built, None).expect("table"));
    let counts = Arc::new(Aggregate::new(&storage));
    let (runtime, client) = GossipRuntime::from_parts(
        SimRouter::new().bind("127.0.0.1:7000".parse().unwrap()),
        TokioClock::from_millis(0),
        gossip,
        CellStore::<u32>::new(storage, NodeIdentity::new(NodeId(1), 1), &table),
        counts.clone(),
    );
    let gossip = tokio::task::spawn_local(runtime.run(futures::stream::empty()));
    let limiter = SharedLimiter::new(table.clone(), client.clone(), counts.clone(), storage);
    let settle = || async { drop(client.reserve(client.capacity()).await) };

    // Warm-up requests name a domain no rule has, so the scratch must already
    // hold its bound from the settings: one that grew to fit the requests
    // it had seen would allocate on the first counted request. The queue's
    // blocks are warmed by recording the same hits directly.
    let unmatched = descriptor("warm-up");
    let warm = hash_key(
        "api",
        [Descriptor {
            key: "tenant",
            value: "warm-up",
        }],
    );
    for _ in 0..200 {
        assert!(matches!(
            limiter.decide("web", &unmatched, 1, 0),
            Decision::Allow(None)
        ));
        for rule in table.rules() {
            let spec = rule.spec();
            client
                .try_record(spec.fingerprint, warm, 0, 1, spec.limit, 0)
                .expect("queue room");
        }
        settle().await;
    }
    let hot: Vec<RateLimitDescriptor> = (0..50).map(|i| descriptor(&format!("hot-{i}"))).collect();
    let requests: Vec<RateLimitDescriptor> = (0..1_000)
        .map(|i| match i % 20 {
            0 => descriptor(&format!("new-{i}")),
            _ => hot[i % hot.len()].clone(),
        })
        .collect();
    assert!(
        allocations(|| black_box(Vec::<u8>::with_capacity(8))).1 > 0,
        "the counting allocator is live"
    );
    let mut total = 0;
    for request in &requests {
        let (decision, allocated) = allocations(|| limiter.decide("api", request, 1, 0));
        assert!(matches!(decision, Decision::Allow(Some(_))), "{decision:?}");
        total += allocated;
        settle().await;
    }
    assert_eq!(total, 0, "{rules} rules: admission allocated");
    // Every matched rule counted every request: none overflowed the bound
    // and none found the queue full.
    let key = hash_key(
        "api",
        [Descriptor {
            key: "tenant",
            value: "hot-1",
        }],
    );
    let expected = requests
        .iter()
        .filter(|r| r.entries[0].value == "hot-1")
        .count() as u64;
    for rule in table.rules() {
        assert_eq!(
            counts.table().window_total(rule.spec(), key, 0),
            expected,
            "{}",
            rule.name
        );
    }
    let _ = client.shutdown().await;
    gossip.await.expect("join").expect("runtime");
}

#[test]
fn admission_never_allocates_at_the_largest_bound() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    // tokio's request channel keeps at most three spare 32-slot blocks, so
    // once more than about 96 hits are queued undrained (across all
    // in-flight requests) a `try_record` allocates a block; this test drains
    // between requests and measures the admission scratch, not the channel.
    for rules in [1, 16, 64] {
        LocalSet::new().block_on(&runtime, admit_without_allocating(rules));
    }
}
