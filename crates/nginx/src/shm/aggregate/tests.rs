use super::*;

/// Stable raw allocation used by every aggregate test. Avoids the
/// `Vec<AggregateSlot>` + `&mut` + `&` mixing pattern that Tree
/// Borrows (correctly) flags as a retag conflict. The production path
/// uses `mmap`-derived raw pointers, so tests mirror it.
struct SlotBuffer {
    ptr: *mut AggregateSlot,
    capacity: usize,
}

impl SlotBuffer {
    fn allocate(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two() && capacity >= 2);
        let layout = std::alloc::Layout::array::<AggregateSlot>(capacity).expect("layout");
        // SAFETY: `Layout` is a valid non-zero array layout for T;
        // we initialize every slot below before the buffer is observed.
        let ptr = unsafe { std::alloc::alloc(layout) as *mut AggregateSlot };
        assert!(!ptr.is_null());
        for i in 0..capacity {
            // SAFETY: `ptr.add(i)` is within the allocation; writing
            // `AggregateSlot::empty()` initialises that slot's atomics.
            unsafe {
                std::ptr::write(ptr.add(i), AggregateSlot::empty());
            }
        }
        Self { ptr, capacity }
    }

    fn store(&self) -> ShmAggregateStore {
        // SAFETY: `self.ptr` points at `self.capacity` fully
        // initialised `AggregateSlot`s in a stable allocation that
        // outlives the returned store (its lifetime is the
        // `SlotBuffer`'s, which is kept alive by the test). Tests run
        // single-threaded except where they explicitly spawn additional
        // threads (and those tests share the same `SlotBuffer` under
        // `Arc`, with exactly one writer thread). This satisfies
        // every precondition in `ShmAggregateStore::new`'s `# Safety`.
        unsafe { ShmAggregateStore::new(self.ptr, self.capacity) }
    }

    fn view(&self) -> AggregateTable<'_> {
        // SAFETY: `self.ptr` is non-null, properly aligned, points at
        // `self.capacity` initialised `AggregateSlot`s, and outlives
        // the returned borrow. Both reader and writer derive from the
        // SAME raw pointer (no `&` / `&mut` mixing), so Stacked and
        // Tree Borrows see one pointer chain.
        let slots =
            unsafe { std::slice::from_raw_parts(self.ptr as *const AggregateSlot, self.capacity) };
        AggregateTable::from_slots(slots)
    }
}

impl Drop for SlotBuffer {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` was minted by `std::alloc::alloc` with
        // the matching `Layout`. `drop_in_place` runs the (trivial)
        // `AggregateSlot` destructors, then `dealloc` releases the
        // backing pages.
        unsafe {
            for i in 0..self.capacity {
                std::ptr::drop_in_place(self.ptr.add(i));
            }
            let layout = std::alloc::Layout::array::<AggregateSlot>(self.capacity).expect("layout");
            std::alloc::dealloc(self.ptr as *mut u8, layout);
        }
    }
}

// SAFETY: `SlotBuffer` is a thin owner of a raw allocation that hosts
// only `AtomicU64` fields. Producing a `Send`/`Sync` wrapper allows
// tests to share it across threads under `Arc` — this matches the
// production cross-process model (every fork-child sees the same SHM
// pointer).
unsafe impl Send for SlotBuffer {}
unsafe impl Sync for SlotBuffer {}

#[test]
fn write_then_read_one_cell() {
    let buf = SlotBuffer::allocate(8);
    let store = buf.store();
    store.write_delta(0x42, 0x99, 7, 3, 1_000);
    let cell = buf.view().get(0x42, 0x99, 7).expect("found");
    assert_eq!(cell.count, 3);
    store.write_delta(0x42, 0x99, 7, 5, 1_100);
    let cell = buf.view().get(0x42, 0x99, 7).expect("found");
    assert_eq!(cell.count, 8);
}

#[test]
fn expiration_tombstones_the_slot() {
    let buf = SlotBuffer::allocate(8);
    let store = buf.store();
    store.write_delta(0x42, 0x99, 7, 5, 1_000);
    store.write_expiration(0x42, 0x99, 7, 5);
    assert!(buf.view().get(0x42, 0x99, 7).is_none());
}

#[test]
fn window_total_sums_live_buckets() {
    let buf = SlotBuffer::allocate(16);
    let store = buf.store();
    store.write_delta(0xaa, 0xbb, 100, 4, 0);
    store.write_delta(0xaa, 0xbb, 101, 7, 0);
    store.write_delta(0xaa, 0xbb, 102, 2, 0);
    let total = buf.view().window_total(0xaa, 0xbb, 102 * 1_000, 1_000, 3);
    assert_eq!(total, 4 + 7 + 2);
}

#[test]
fn collisions_are_resolved_by_probing() {
    let buf = SlotBuffer::allocate(4);
    let store = buf.store();
    for i in 0..3_u128 {
        store.write_delta(i, i + 1, i as u32, 1, 0);
    }
    let view = buf.view();
    for i in 0..3_u128 {
        let cell = view.get(i, i + 1, i as u32).expect("found");
        assert_eq!(cell.count, 1);
    }
}

#[test]
fn tombstone_then_reinsert_recovers_count() {
    let buf = SlotBuffer::allocate(8);
    let store = buf.store();
    store.write_delta(1, 2, 3, 5, 100);
    store.write_expiration(1, 2, 3, 5);
    store.write_delta(1, 2, 3, 7, 200);
    let cell = buf.view().get(1, 2, 3).expect("re-inserted");
    assert_eq!(cell.count, 7);
}

#[test]
fn multiple_keys_in_full_table_dont_lose_data() {
    let buf = SlotBuffer::allocate(8);
    let store = buf.store();
    for i in 0..6_u128 {
        store.write_delta(i, i + 100, (i + 1) as u32, (i + 1) as u64, i as u64);
    }
    let view = buf.view();
    for i in 0..6_u128 {
        let cell = view
            .get(i, i + 100, (i + 1) as u32)
            .unwrap_or_else(|| panic!("key {i} missing"));
        assert_eq!(cell.count, (i + 1) as u64);
    }
}

fn test_spec(fp: u128, limit: u64, bucket_millis: u64, live_buckets: u32) -> RuleSpec {
    RuleSpec {
        id: 1,
        fingerprint: fp,
        limit,
        bucket_millis,
        window_millis: bucket_millis * live_buckets as u64,
        live_buckets,
    }
}

#[test]
fn time_until_admit_millis_uses_oldest_non_empty_bucket() {
    // Window = [6..10], bm=1000. Bucket 6 holds 15 hits — total=15,
    // limit=10, hits=1 → need=6. Bucket 6 alone covers it; delta =
    // (6+5)*1000 - 10000 = 1000ms.
    let buf = SlotBuffer::allocate(16);
    let store = buf.store();
    store.write_delta(0xaa, 0xbb, 6, 15, 0);
    let spec = test_spec(0xaa, 10, 1_000, 5);
    let delta = buf
        .view()
        .time_until_admit_millis(spec, 0xbb, 10_000, 15, 1);
    assert_eq!(delta, 1_000);
}

#[test]
fn time_until_admit_millis_walks_through_empty_then_tombstoned_buckets() {
    // Tombstone the oldest bucket (write then expire) to verify the
    // walker steps over a TOMBSTONE slot exactly like an Empty one.
    // Window = [6..10]. Bucket 6 expired → reads as None. Bucket 7
    // alone covers need = 15+1-10 = 6.
    let buf = SlotBuffer::allocate(16);
    let store = buf.store();
    store.write_delta(0xcc, 0xdd, 6, 4, 0);
    store.write_expiration(0xcc, 0xdd, 6, 4); // tombstone bucket 6
    store.write_delta(0xcc, 0xdd, 7, 15, 0);
    let spec = test_spec(0xcc, 10, 1_000, 5);
    let delta = buf
        .view()
        .time_until_admit_millis(spec, 0xdd, 10_000, 15, 1);
    assert_eq!(delta, 2_000);
}

#[test]
fn time_until_admit_millis_already_admittable_returns_zero() {
    let buf = SlotBuffer::allocate(8);
    let store = buf.store();
    store.write_delta(0xee, 0xff, 10, 5, 0);
    let spec = test_spec(0xee, 10, 1_000, 5);
    let delta = buf.view().time_until_admit_millis(spec, 0xff, 10_000, 5, 3);
    assert_eq!(delta, 0);
}

#[test]
fn concurrent_readers_see_consistent_counts() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering as O};
    use std::thread;

    const CAPACITY: usize = 64;
    #[cfg(not(miri))]
    const WRITES: u64 = 4096;
    #[cfg(miri)]
    const WRITES: u64 = 64;

    let buf = Arc::new(SlotBuffer::allocate(CAPACITY));
    let stop = Arc::new(AtomicBool::new(false));

    let writer_buf = buf.clone();
    let writer = thread::spawn(move || {
        let store = writer_buf.store();
        for _ in 0..WRITES {
            store.write_delta(0xabc, 0xdef, 1, 1, 0);
        }
    });

    let mut readers = Vec::new();
    for _ in 0..2 {
        let reader_buf = buf.clone();
        let reader_stop = stop.clone();
        readers.push(thread::spawn(move || {
            while !reader_stop.load(O::Acquire) {
                let _ = reader_buf.view().get(0xabc, 0xdef, 1);
            }
        }));
    }

    writer.join().unwrap();
    stop.store(true, O::Release);
    for r in readers {
        r.join().unwrap();
    }

    let cell = buf.view().get(0xabc, 0xdef, 1).expect("found");
    assert_eq!(cell.count, WRITES);
}
