//! Test-only stand-in for the zone nginx maps, and a leader-writer seed for
//! its aggregate.

use super::{CACHELINE, CachePadded, Layout, ShmRegion};

/// One cache line of a test zone: whole lines give the zone the `CACHELINE`
/// alignment `ShmRegion::initialize` requires.
type Line = CachePadded<[u64; CACHELINE / 8]>;

/// Heap block backing a test `ShmRegion`, freed on drop. Holding the raw
/// pointer (not a `Box`) keeps it at a stable address that Stacked and Tree
/// Borrows don't retag when the owner moves.
pub(crate) struct TestZone {
    ptr: *mut Line,
    lines: usize,
}

impl TestZone {
    /// A fresh zone and the region initialized in it. Drop the zone only
    /// after the last use of the region.
    pub(crate) fn new(queue_capacity: usize, aggregate_capacity: usize) -> (Self, ShmRegion) {
        let layout = Layout::new(queue_capacity, aggregate_capacity).expect("layout");
        let lines = layout.total_bytes.div_ceil(CACHELINE);
        let block: Box<[Line]> = (0..lines)
            .map(|_| CachePadded([0; CACHELINE / 8]))
            .collect();
        let ptr = Box::into_raw(block).cast::<Line>();
        // SAFETY: `ptr` is a fresh, writable, cache-line-aligned allocation
        // of at least `layout.total_bytes`, `initialize` is its first user,
        // and the returned zone keeps it alive until it is dropped.
        let region = unsafe { ShmRegion::initialize(ptr.cast(), layout) };
        (Self { ptr, lines }, region)
    }
}

impl Drop for TestZone {
    fn drop(&mut self) {
        // SAFETY: `ptr` came from `Box::into_raw` of a `Box<[Line]>` of
        // `lines` elements, and no use of its region outlives the zone.
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(self.ptr, self.lines)) });
    }
}

/// Apply one delta row of `count` hits for `(fingerprint, key_hash, bucket)`
/// through the leader's writer, as the gossip runtime would.
pub(crate) fn seed(
    store: &super::ShmAggregateStore,
    fingerprint: u128,
    key_hash: gabion::crdt::KeyHash,
    bucket: u32,
    count: u32,
) {
    use gabion::crdt::{CellHandle, CellIdentity, DeltaSink, ExpirationSink};
    use gabion::gossip::AggregateStore;
    let deltas = DeltaSink {
        handles: vec![CellHandle::default()],
        keys: vec![CellIdentity {
            rule_fingerprint: fingerprint,
            key_hash,
            bucket,
        }],
        previous: vec![0],
        current: vec![count],
        deltas: vec![count],
        applies_locally: vec![1],
    };
    store.apply(&deltas, &ExpirationSink::default());
}
