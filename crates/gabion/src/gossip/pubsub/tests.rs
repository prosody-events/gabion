use super::*;

#[test]
fn id_ring_finds_inserted_ids_and_evicts_the_oldest_when_full() {
    let mut ring = IdRing::with_capacity(4);
    for id in 1..=4 {
        ring.insert(id, id);
    }
    assert!((1..=4).all(|id| ring.contains(id)));

    ring.insert(5, 5);
    assert!(!ring.contains(1), "the oldest id is evicted at capacity");
    assert!((2..=5).all(|id| ring.contains(id)));
}

#[test]
fn id_ring_unlinks_ids_that_share_a_bucket() {
    // Ids that agree in their low bits chain into one bucket.
    let ids = [0x10_u64, 0x20, 0x30, 0x40];
    let mut ring = IdRing::with_capacity(4);
    for (stamp, id) in ids.into_iter().enumerate() {
        ring.insert(id, stamp as u64);
    }
    // Evicts 0x10 from the tail of its chain, then 0x20 from the middle.
    ring.insert(0x50, 4);
    ring.insert(0x60, 5);
    assert!(!ring.contains(0x10) && !ring.contains(0x20));
    assert!(
        [0x30, 0x40, 0x50, 0x60]
            .into_iter()
            .all(|id| ring.contains(id))
    );
}

#[test]
fn id_ring_expires_by_stamp() {
    let mut ring = IdRing::with_capacity(8);
    for id in 1..=6 {
        ring.insert(id, id * 10);
    }
    ring.expire_before(35);
    assert!((1..=3).all(|id| !ring.contains(id)));
    assert!((4..=6).all(|id| ring.contains(id)));
    assert_eq!(
        ring.newest_first().map(|(_, id, _)| id).collect::<Vec<_>>(),
        [6, 5, 4]
    );
}

#[test]
fn message_cache_serves_datagrams_until_history_expires() {
    let mut cache = MessageCache::new(8, 16);
    cache.insert(1, 0, b"first");
    cache.insert(2, 1, b"second");
    assert_eq!(cache.get(1), Some(&b"first"[..]));
    assert_eq!(cache.get(2), Some(&b"second"[..]));

    // History of 3 heartbeats: at heartbeat 3 a message from heartbeat 0 is
    // three old and gone; one from heartbeat 1 is two old and kept.
    cache.expire(3, 3);
    assert_eq!(cache.get(1), None);
    assert_eq!(cache.get(2), Some(&b"second"[..]));
}

#[test]
fn message_cache_ignores_datagrams_over_the_slot_and_repeats() {
    let mut cache = MessageCache::new(4, 4);
    cache.insert(1, 0, b"toolong");
    assert_eq!(cache.get(1), None);

    cache.insert(2, 0, b"abcd");
    cache.insert(2, 1, b"wxyz");
    assert_eq!(cache.get(2), Some(&b"abcd"[..]));
}

#[test]
fn gossip_ids_cover_only_the_newest_heartbeats_newest_first() {
    let mut cache = MessageCache::new(8, 4);
    for (id, heartbeat) in [(1, 0), (2, 1), (3, 2), (4, 2)] {
        cache.insert(id, heartbeat, b"x");
    }
    let mut ids = Vec::new();
    // At heartbeat 3 with a 3-heartbeat gossip window, heartbeat 0 is out.
    cache.gossip_ids(3, 3, &mut ids);
    assert_eq!(ids, [4, 3, 2]);
}

#[test]
fn message_id_depends_on_every_byte() {
    assert_eq!(message_id(b"abc"), message_id(b"abc"));
    assert_ne!(message_id(b"abc"), message_id(b"abd"));
}
