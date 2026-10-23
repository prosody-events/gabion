use std::collections::HashMap;

use gabion::crdt::{
    CellStore, CellStoreConfig, KeyHash, NodeId, NodeIdentity, Observation, ObservationBatch,
    RuleDescriptor,
};

use super::*;
use crate::shm::mock::TestZone;

fn table(capacity: usize) -> (TestZone, ShmRegion, ShmAggregateStore) {
    let (zone, region) = TestZone::new(2, capacity);
    (zone, region, ShmAggregateStore::new(region))
}

#[test]
fn write_then_read_one_cell() {
    let (_zone, region, store) = table(8);
    store.write_delta(0x42, 0x99, 7, 3, 1_000);
    let cell = region.aggregate().get(0x42, 0x99, 7).expect("found");
    assert_eq!(cell.count, 3);
    store.write_delta(0x42, 0x99, 7, 5, 1_100);
    let cell = region.aggregate().get(0x42, 0x99, 7).expect("found");
    assert_eq!(cell.count, 8);
}

#[test]
fn expiration_subtracts_then_empties_the_slot() {
    let (_zone, region, store) = table(8);
    store.write_delta(0x42, 0x99, 7, 5, 1_000);
    store.write_expiration(0x42, 0x99, 7, 2);
    assert_eq!(
        region.aggregate().get(0x42, 0x99, 7).expect("found").count,
        3
    );
    store.write_expiration(0x42, 0x99, 7, 3);
    assert!(region.aggregate().get(0x42, 0x99, 7).is_none());
    assert!(
        region
            .aggregate()
            .slots
            .iter()
            .all(|slot| matches!(slot.read(), Read::Empty))
    );
}

#[test]
fn window_total_sums_live_buckets() {
    let (_zone, region, store) = table(16);
    store.write_delta(0xaa, 0xbb, 100, 4, 0);
    store.write_delta(0xaa, 0xbb, 101, 7, 0);
    store.write_delta(0xaa, 0xbb, 102, 2, 0);
    let total = region
        .aggregate()
        .window_total(0xaa, 0xbb, 102 * 1_000, 1_000, 3);
    assert_eq!(total, 4 + 7 + 2);
}

#[test]
fn collisions_are_resolved_by_probing() {
    let (_zone, region, store) = table(4);
    for i in 0..4_u128 {
        store.write_delta(i, i + 1, i as u32, i as u64 + 1, 0);
    }
    for i in 0..4_u128 {
        let cell = region.aggregate().get(i, i + 1, i as u32).expect("found");
        assert_eq!(cell.count, i as u64 + 1);
    }
}

/// Random inserts, updates and expiries against a model: every live cell
/// stays findable within `MAX_PROBE` of its home, and once everything has
/// expired the table is empty again rather than full of tombstones.
#[test]
fn churn_keeps_cells_findable_and_frees_every_slot() {
    #[cfg(not(miri))]
    const OPS: u64 = 200_000;
    #[cfg(miri)]
    const OPS: u64 = 500;
    let (_zone, region, store) = table(128);
    let view = region.aggregate();
    let mut model: HashMap<u32, u64> = HashMap::new();
    let mut rng = 0x9E37_79B9_7F4A_7C15_u64;
    for _ in 0..OPS {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let key = (rng % 64) as u32;
        let count = model.entry(key).or_default();
        if *count > 0 && rng & 0x100 != 0 {
            store.write_expiration(1, 2, key, *count);
            *count = 0;
        } else {
            store.write_delta(1, 2, key, 1, 0);
            *count += 1;
        }
    }
    assert_eq!(region.stats().snapshot().aggregate_dropped, 0);
    for (&key, &count) in &model {
        assert_eq!(view.get(1, 2, key).map_or(0, |cell| cell.count), count);
        if count > 0 {
            store.write_expiration(1, 2, key, count);
        }
    }
    assert!(
        view.slots
            .iter()
            .all(|slot| matches!(slot.read(), Read::Empty))
    );
}

/// One leader term on node 1: a fresh `CellStore` learns a peer's count
/// from gossip and `store` applies the resulting delta.
fn lead(store: &ShmAggregateStore, incarnation: u32) -> CellStore<u32> {
    let identity = NodeIdentity::new(NodeId(1), incarnation);
    let mut cells = CellStore::new(CellStoreConfig::default(), identity);
    let rule = RuleDescriptor {
        fingerprint: 0x42,
        local_rule_id: 0,
        ..RuleDescriptor::default()
    };
    cells.intern_rule(rule).expect("rule slot");
    let mut gossip = ObservationBatch::default();
    gossip.push(Observation {
        rule_fingerprint: 0x42,
        key_hash: KeyHash(0x99),
        bucket: 7,
        origin: NodeId(2),
        incarnation: 1,
        count: 5,
        last_update_millis: 7_000,
        origin_sequence: 1,
    });
    let mut deltas = DeltaSink::default();
    cells.merge_remote(&gossip, &mut deltas);
    store.apply(&deltas, &ExpirationSink::default());
    cells
}

fn expire(store: &ShmAggregateStore, cells: &mut CellStore<u32>) {
    let mut expired = ExpirationSink::default();
    cells.expire_at(100_000, &mut expired);
    store.apply(&DeltaSink::default(), &expired);
}

/// A leader taking over a zone its predecessor filled relearns the same
/// counts from peers. It counts each once, ignores the predecessor if it
/// keeps applying gossip, and expiring the counts frees every slot.
#[test]
fn a_new_leader_counts_regossiped_cells_once() {
    let (_zone, region, previous) = table(16);
    lead(&previous, 1);
    assert!(region.lease().try_acquire(2, 0, 1_000));

    let store = ShmAggregateStore::new(region);
    let mut cells = lead(&store, 2);
    lead(&previous, 1);
    let view = region.aggregate();
    assert_eq!(view.get(0x42, 0x99, 7).expect("found").count, 5);

    expire(&store, &mut cells);
    assert!(
        view.slots
            .iter()
            .all(|slot| matches!(slot.read(), Read::Empty))
    );
}

/// A new leader's writes continue past every sequence an earlier leader
/// issued, even to cells it has since freed, so a reader still holding one
/// cannot accept it over a different cell.
#[test]
fn a_new_leader_never_reuses_a_sequence() {
    let (_zone, region, previous) = table(16);
    let last_seq = || {
        region
            .aggregate()
            .slots
            .iter()
            .map(|slot| slot.seq.load(Ordering::Relaxed))
            .max()
    };
    let mut cells = lead(&previous, 1);
    let issued = last_seq();
    expire(&previous, &mut cells);

    lead(&ShmAggregateStore::new(region), 2);
    assert!(last_seq() > issued);
}

#[test]
fn full_table_drops_and_counts_the_write() {
    let (_zone, region, store) = table(MAX_PROBE);
    for bucket in 0..MAX_PROBE as u32 {
        store.write_delta(1, 2, bucket, 1, 0);
    }
    assert_eq!(region.stats().snapshot().aggregate_dropped, 0);
    store.write_delta(1, 2, MAX_PROBE as u32, 1, 0);
    store.write_delta(1, 2, 0, 1, 0);
    assert_eq!(region.stats().snapshot().aggregate_dropped, 1);
    let view = region.aggregate();
    assert!(view.get(1, 2, MAX_PROBE as u32).is_none());
    assert_eq!(view.get(1, 2, 0).expect("kept").count, 2);
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
    let (_zone, region, store) = table(16);
    store.write_delta(0xaa, 0xbb, 6, 15, 0);
    let spec = test_spec(0xaa, 10, 1_000, 5);
    let delta = region
        .aggregate()
        .time_until_admit_millis(spec, 0xbb, 10_000, 15, 1);
    assert_eq!(delta, 1_000);
}

#[test]
fn time_until_admit_millis_walks_past_expired_buckets() {
    // Window = [6..10]. Bucket 6 expired → reads as None. Bucket 7
    // alone covers need = 15+1-10 = 6.
    let (_zone, region, store) = table(16);
    store.write_delta(0xcc, 0xdd, 6, 4, 0);
    store.write_expiration(0xcc, 0xdd, 6, 4);
    store.write_delta(0xcc, 0xdd, 7, 15, 0);
    let spec = test_spec(0xcc, 10, 1_000, 5);
    let delta = region
        .aggregate()
        .time_until_admit_millis(spec, 0xdd, 10_000, 15, 1);
    assert_eq!(delta, 2_000);
}

#[test]
fn time_until_admit_millis_already_admittable_returns_zero() {
    let (_zone, region, store) = table(8);
    store.write_delta(0xee, 0xff, 10, 5, 0);
    let spec = test_spec(0xee, 10, 1_000, 5);
    let delta = region
        .aggregate()
        .time_until_admit_millis(spec, 0xff, 10_000, 5, 3);
    assert_eq!(delta, 0);
}
