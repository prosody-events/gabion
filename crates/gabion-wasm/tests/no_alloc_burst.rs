//! A burst allocates nothing per request: a Send of a thousand requests costs
//! exactly what a Send of one does, in allocations and in bytes, whether its
//! node rejects it whole, admits it whole or splits it.
//!
//! A counting global allocator, armed on this thread only around one
//! `SubmitRequest` round trip, counts every allocation and every byte the
//! engine makes deciding, recording and flushing the burst. The engine runs
//! on this thread (a current-thread runtime and a `LocalSet`), so nothing it
//! does escapes the count; each measurement builds a fresh engine and sends
//! it the same commands, so two measurements differ only in the burst's size.
//!
//! Miri exemption: the allocator only forwards to `System`, and Miri cannot
//! usefully run a tokio runtime under a counting allocator, so this binary
//! runs under nextest only.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::num::NonZeroU64;

use gabion::crdt::KeyHash;
use gabion_wasm::config::{SimConfig, ZoneMode};
use gabion_wasm::engine::{Command, StepUntil, run_engine};
use gabion_wasm::event::{Burst, EventKind};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot};
use tokio::task::LocalSet;

thread_local! {
    /// Set on this thread while a counted section runs.
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static BYTES: Cell<u64> = const { Cell::new(0) };
}

struct Counting;

// SAFETY: every method forwards its arguments unchanged to `System`, which
// upholds `GlobalAlloc`'s contract; the bookkeeping touches only
// const-initialised thread-locals with no destructor, so it never allocates
// or re-enters the allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: the caller's guarantees for `layout` pass straight through.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from `System` through `alloc` or `realloc`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        // SAFETY: the caller's guarantees pass straight through.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn note(bytes: usize) {
    if COUNTING.get() {
        ALLOCATIONS.set(ALLOCATIONS.get() + 1);
        BYTES.set(BYTES.get() + bytes as u64);
    }
}

/// The watched key.
const KEY: KeyHash = KeyHash(1);

/// Advance the engine `ms` of virtual time.
async fn step(tx: &mpsc::Sender<Command>, delta_ms: u64) {
    let (reply, rx) = oneshot::channel();
    let until = StepUntil::Time;
    let cmd = Command::Step {
        delta_ms,
        until,
        reply,
    };
    tx.send(cmd).await.expect("the engine runs");
    rx.await.expect("the engine replies");
}

/// A burst of `requests` requests at node `node`; with `counted`, the
/// allocations and bytes its round trip makes on this thread. Returns the
/// burst's decision.
async fn submit(tx: &mpsc::Sender<Command>, node: u32, requests: u64, counted: bool) -> Burst {
    let (reply, rx) = oneshot::channel();
    let cmd = Command::SubmitRequest {
        node,
        key: KEY,
        requests: NonZeroU64::new(requests).expect("a burst of one or more"),
        until: StepUntil::Time,
        reply,
    };
    COUNTING.set(counted);
    tx.send(cmd).await.expect("the engine runs");
    let batch = rx.await.expect("the engine replies");
    COUNTING.set(false);
    let batch = batch.expect("a live node decides");
    batch
        .events
        .iter()
        .find_map(|e| match e.kind {
            EventKind::Decided { burst, .. } => Some(burst),
            _ => None,
        })
        .expect("the batch carries the decision")
}

/// On a fresh twelve-node, three-zone cluster at `limit` per 10 s: after a
/// second of warm-up and each `prelude` burst followed by a second, the
/// allocations and bytes of one counted burst of `requests` at node 0, and
/// its decision.
fn measure(limit: u64, prelude: &[(u32, u64)], requests: u64) -> (u64, u64, Burst) {
    let config = SimConfig {
        rule_limit: limit,
        zones: 3,
        zone_mode: ZoneMode::Configured,
        ..SimConfig::default()
    };
    let rt = Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("build paused current-thread runtime");
    LocalSet::new().block_on(&rt, async move {
        let (tx, rx) = mpsc::channel(16);
        let valid = config.validate().expect("the config validates");
        let engine = tokio::task::spawn_local(run_engine(valid, rx));
        step(&tx, 1_000).await;
        for &(node, n) in prelude {
            submit(&tx, node, n, false).await;
            step(&tx, 1_000).await;
        }
        let (allocations, bytes) = (ALLOCATIONS.get(), BYTES.get());
        let burst = submit(&tx, 0, requests, true).await;
        let counted = (ALLOCATIONS.get() - allocations, BYTES.get() - bytes);
        drop(tx);
        engine
            .await
            .expect("engine task joined")
            .expect("engine ended cleanly");
        (counted.0, counted.1, burst)
    })
}

#[test]
fn a_burst_allocates_nothing_per_request() {
    // Process-wide lazy initialisation lands here, outside every comparison.
    measure(100, &[], 1);

    let full = [(1, 100)];
    let (one, one_bytes, burst) = measure(100, &full, 1);
    assert!(
        matches!(burst, Burst::Rejected { admitted: 0, .. }),
        "rejected whole: {burst:?}"
    );
    let (many, many_bytes, burst) = measure(100, &full, 1_000);
    assert!(
        matches!(burst, Burst::Rejected { admitted: 0, .. }),
        "rejected whole: {burst:?}"
    );
    println!(
        "rejected whole: 1 request {one} allocations, {one_bytes} B; 1,000 {many}, {many_bytes} B"
    );
    assert_eq!((one, one_bytes), (many, many_bytes), "rejected whole");

    // The eager-flush threshold is each node's share of the 1 % error
    // budget, tens of thousands of hits at this limit, so neither burst
    // flushes.
    let (one, one_bytes, burst) = measure(100_000_000, &[], 1);
    assert!(
        matches!(burst, Burst::Admitted { .. }),
        "admitted whole: {burst:?}"
    );
    let (many, many_bytes, burst) = measure(100_000_000, &[], 1_000);
    assert!(
        matches!(burst, Burst::Admitted { .. }),
        "admitted whole: {burst:?}"
    );
    println!(
        "admitted whole: 1 request {one} allocations, {one_bytes} B; 1,000 {many}, {many_bytes} B"
    );
    assert_eq!((one, one_bytes), (many, many_bytes), "admitted whole");

    let (one, one_bytes, burst) = measure(100, &[], 101);
    assert!(
        matches!(burst, Burst::Rejected { admitted: 100, .. }),
        "split: {burst:?}"
    );
    let (many, many_bytes, burst) = measure(100, &[], 1_000);
    assert!(
        matches!(burst, Burst::Rejected { admitted: 100, .. }),
        "split: {burst:?}"
    );
    println!("split: 101 requests {one} allocations, {one_bytes} B; 1,000 {many}, {many_bytes} B");
    assert_eq!((one, one_bytes), (many, many_bytes), "split");
}
