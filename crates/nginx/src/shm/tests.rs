use super::*;
use crate::shm::header::{SHM_MAGIC, SHM_VERSION};
use crate::shm::mock::TestZone;
use gabion::settings::Settings;
use std::sync::atomic::Ordering;

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
    let queue = Settings::default().limit_queue_capacity.get();
    for page in [4096, 16384, 65536] {
        assert!(Layout::for_zone(128 * 1024, page, queue).is_err());
        assert!(Layout::for_zone(MIB, page, queue).is_ok());
        for mib in [1, 2, 3, 64, 1024] {
            let zone = mib * MIB;
            let layout = Layout::for_zone(zone, page, queue).expect("layout");
            assert_eq!(layout.queue_capacity, queue);
            assert!(layout.aggregate_capacity.is_power_of_two());
            // It fits beside nginx's slab overhead, and twice as many slots
            // would not.
            assert!(layout.total_bytes + zone / 128 + 4 * page <= zone);
            let doubled = Layout::new(queue, 2 * layout.aggregate_capacity).unwrap();
            assert!(doubled.total_bytes + zone / 128 + 4 * page > zone);
        }
    }
    assert_eq!(
        Layout::for_zone(2 * MIB, 4096, queue)
            .unwrap()
            .aggregate_capacity,
        16384
    );
    assert_eq!(
        Layout::for_zone(64 * MIB, 4096, queue)
            .unwrap()
            .aggregate_capacity,
        524288
    );
}

/// `gabion_gossip_limit_queue_capacity` sizes the SHM queue, rounded up to a
/// power of two, and a zone too small for it reports the smallest that fits.
#[test]
fn nginx_queue_capacity_sizes_the_shm_queue() {
    const ZONE: usize = 128 * 1024 * 1024;
    const PAGE: usize = 4096;
    let default = Settings::default().limit_queue_capacity.get();
    for queue in [64, 100, 1 << 20, default] {
        let layout = Layout::for_zone(ZONE, PAGE, queue).expect("layout");
        assert_eq!(layout.queue_capacity, queue.next_power_of_two());
    }
    let needed =
        Layout::for_zone(PAGE, PAGE, default).expect_err("a one-page zone cannot hold the queue");
    assert_eq!(Layout::for_zone(needed - PAGE, PAGE, default), Err(needed));
    let layout = Layout::for_zone(needed, PAGE, default).expect("the reported size fits");
    assert_eq!(layout.aggregate_capacity, MIN_AGGREGATE_CAPACITY);
}

#[test]
fn initialize_then_read_round_trip() {
    let (_zone, region) = TestZone::new(8, 16);
    assert_eq!(region.header().magic.load(Ordering::Acquire), SHM_MAGIC);
    assert_eq!(region.header().version.load(Ordering::Acquire), SHM_VERSION);
    assert_eq!(region.queue().capacity(), 8);
    assert_eq!(region.aggregate().capacity(), 16);
    // Stats fields default to zero.
    let stats = region.stats().snapshot();
    assert_eq!(stats.requests, 0);
}
