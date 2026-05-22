use super::*;

/// Aligned heap allocation used to back a `ShmRegion` in tests so the
/// pointer stays at a stable address (no Stacked-Borrows reborrow on
/// move).
struct Zone {
    ptr: *mut u8,
    words: usize,
}

impl Zone {
    fn allocate(words: usize) -> Self {
        let buf: Box<[u64]> = vec![0_u64; words].into_boxed_slice();
        let raw = Box::into_raw(buf);
        Self {
            ptr: raw as *mut u8,
            words,
        }
    }
}

impl Drop for Zone {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` was minted by `Box::into_raw` of a
        // `Box<[u64]>` of length `self.words`. No live references into
        // the region exist at drop time (the `ShmRegion` paired with
        // this `Zone` is dropped first because it is created later in
        // the test).
        unsafe {
            let slice = std::ptr::slice_from_raw_parts_mut(self.ptr as *mut u64, self.words);
            let _ = Box::from_raw(slice);
        }
    }
}

#[test]
fn layout_minimum_capacities() {
    assert!(Layout::new(1, 2).is_none()); // queue must be ≥ 2
    assert!(Layout::new(2, 1).is_none()); // aggregate must be ≥ 2
    assert!(Layout::new(3, 2).is_none()); // queue must be power of 2
    let l = Layout::new(8, 16).expect("layout");
    assert_eq!(l.queue_capacity, 8);
    assert_eq!(l.aggregate_capacity, 16);
    assert!(l.total_bytes >= size_of::<Header>());
}

#[test]
fn initialize_then_read_round_trip() {
    let layout = Layout::new(8, 16).expect("layout");
    let words = layout.total_bytes.div_ceil(8);
    let zone = Zone::allocate(words);
    // SAFETY: see access::tests::TestZone for the same justification.
    // `zone.ptr` is 8-byte aligned, freshly allocated, lives until the
    // `Zone` drop at the end of this test, and `layout` matches the
    // allocation size.
    let region = unsafe { ShmRegion::initialize(zone.ptr, layout) };
    assert!(region.header().is_initialized());
    assert_eq!(region.queue().capacity(), 8);
    assert_eq!(region.aggregate().capacity(), 16);
    // Stats fields default to zero.
    let stats = region.stats().snapshot();
    assert_eq!(stats.requests, 0);
    // Keep `region` and `zone` alive through the end of the test.
    let _ = region;
    let _ = zone;
}

#[test]
fn from_initialized_sees_same_state() {
    let layout = Layout::new(8, 16).expect("layout");
    let words = layout.total_bytes.div_ceil(8);
    let zone = Zone::allocate(words);
    // SAFETY: as in initialize_then_read_round_trip.
    let first = unsafe { ShmRegion::initialize(zone.ptr, layout) };
    first.stats().record_allow();
    // SAFETY: `zone.ptr` points at the same initialized region; the
    // first ShmRegion has not freed anything.
    let second = unsafe { ShmRegion::from_initialized(zone.ptr, layout) };
    assert_eq!(second.stats().snapshot().allowed, 1);
    let _ = first;
    let _ = second;
    let _ = zone;
}
