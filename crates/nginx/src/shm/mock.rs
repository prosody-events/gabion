//! Test-only stand-in for the zone nginx maps.

use super::{Layout, ShmRegion};

/// Heap block backing a test `ShmRegion`, freed on drop. Holding the raw
/// pointer (not a `Box`) keeps it at a stable address that Stacked and Tree
/// Borrows don't retag when the owner moves.
pub(crate) struct TestZone {
    ptr: *mut u64,
    words: usize,
}

impl TestZone {
    /// A fresh zone and the region initialized in it. Drop the zone only
    /// after the last use of the region.
    pub(crate) fn new(queue_capacity: usize, aggregate_capacity: usize) -> (Self, ShmRegion) {
        let layout = Layout::new(queue_capacity, aggregate_capacity).expect("layout");
        let words = layout.total_bytes.div_ceil(8);
        let ptr = Box::into_raw(vec![0_u64; words].into_boxed_slice()).cast::<u64>();
        // SAFETY: `ptr` is a fresh, writable, 8-byte-aligned allocation of
        // at least `layout.total_bytes`, `initialize` is its first user, and
        // the returned zone keeps it alive until it is dropped.
        let region = unsafe { ShmRegion::initialize(ptr.cast(), layout) };
        (Self { ptr, words }, region)
    }
}

impl Drop for TestZone {
    fn drop(&mut self) {
        // SAFETY: `ptr` came from `Box::into_raw` of a `Box<[u64]>` of
        // `words` elements, and no use of its region outlives the zone.
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(self.ptr, self.words)) });
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
        handles: vec![CellHandle {
            index: 0,
            generation: 0,
        }],
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
