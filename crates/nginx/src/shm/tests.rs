use super::*;
use crate::shm::mock::TestZone;

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
fn zone_size_sets_aggregate_capacity() {
    const MIB: usize = 1024 * 1024;
    for page in [4096, 16384, 65536] {
        assert!(Layout::for_zone(128 * 1024, page).is_none());
        assert!(Layout::for_zone(MIB, page).is_some());
        for mib in [1, 2, 3, 64, 1024] {
            let zone = mib * MIB;
            let layout = Layout::for_zone(zone, page).expect("layout");
            assert_eq!(layout.queue_capacity, QUEUE_CAPACITY);
            assert!(layout.aggregate_capacity.is_power_of_two());
            // It fits beside nginx's slab overhead, and twice as many slots
            // would not.
            assert!(layout.total_bytes + zone / 128 + 4 * page <= zone);
            let doubled = Layout::new(QUEUE_CAPACITY, 2 * layout.aggregate_capacity).unwrap();
            assert!(doubled.total_bytes + zone / 128 + 4 * page > zone);
        }
    }
    assert_eq!(
        Layout::for_zone(2 * MIB, 4096).unwrap().aggregate_capacity,
        16384
    );
    assert_eq!(
        Layout::for_zone(64 * MIB, 4096).unwrap().aggregate_capacity,
        524288
    );
}

#[test]
fn initialize_then_read_round_trip() {
    let (_zone, region) = TestZone::new(8, 16);
    assert!(region.header().is_initialized());
    assert_eq!(region.queue().capacity(), 8);
    assert_eq!(region.aggregate().capacity(), 16);
    // Stats fields default to zero.
    let stats = region.stats().snapshot();
    assert_eq!(stats.requests, 0);
}
