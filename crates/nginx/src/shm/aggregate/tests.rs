use gabion::crdt::{
    CellStore, DeltaSink, Expiry, KeyHash, NodeId, NodeIdentity, Observation, ObservationBatch,
};
use gabion::rules::{EnforcementMode, RuleDef, RuleTable};

use super::*;
use crate::shm::mock::TestZone;

fn table(capacity: usize) -> (TestZone, ShmRegion, ShmAggregateStore) {
    let (zone, region) = TestZone::new(2, capacity);
    (zone, region, ShmAggregateStore::new(region))
}

/// One leader term on node 1: a fresh `CellStore` learns a peer's count
/// from gossip and `store` applies the resulting delta.
fn lead(store: &ShmAggregateStore, incarnation: u32) -> (CellStore<u32>, u128) {
    let identity = NodeIdentity::new(NodeId(1), incarnation);
    let storage = crate::mock::validated(|_| {}).1;
    let rule = RuleDef {
        name: "r",
        domain: "nginx",
        patterns: &[("tenant", "*")],
        rate: "10r/m",
        window: None,
        bucket: Some(std::time::Duration::from_secs(1)),
        mode: EnforcementMode::Enforce,
    }
    .build(&storage.cardinality)
    .expect("rule");
    let fingerprint = rule.fingerprint;
    let rules = RuleTable::new(Box::new([rule]), None).expect("table");
    let mut cells = CellStore::new(storage, identity, &rules);
    let mut gossip = ObservationBatch::default();
    gossip.push(Observation {
        rule_fingerprint: fingerprint,
        key_hash: KeyHash(0x99),
        bucket: 7,
        origin: NodeId(2),
        incarnation: 1,
        count: 5,
        last_update_millis: 7_000,
    });
    let mut deltas = DeltaSink::default();
    cells.merge_remote(&gossip, &mut deltas);
    store.apply(&deltas, &ExpirationSink::default());
    (cells, fingerprint)
}

fn expire(store: &ShmAggregateStore, cells: &mut CellStore<u32>) {
    let mut expired = ExpirationSink::with_capacity(cells.capacity() as usize);
    assert_eq!(cells.expire_at(100_000, &mut expired), Expiry::Done);
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
    let (mut cells, fingerprint) = lead(&store, 2);
    lead(&previous, 1);
    let view = region.aggregate();
    assert_eq!(view.get(fingerprint, 0x99, 7).expect("found").count, 5);

    expire(&store, &mut cells);
    assert_eq!(view.occupied(), 0);
}

/// A new leader's writes continue past every sequence an earlier leader
/// issued, even to cells it has since freed, so a reader still holding one
/// cannot accept it over a different cell.
#[test]
fn a_new_leader_never_reuses_a_sequence() {
    let (_zone, region, previous) = table(16);
    let next_seq = || region.header().aggregate_write_seq.load(Ordering::Relaxed);
    let (mut cells, _) = lead(&previous, 1);
    expire(&previous, &mut cells);
    let issued = next_seq();

    lead(&ShmAggregateStore::new(region), 2);
    assert!(next_seq() > issued);
}

/// A write the full table drops is counted where operators read it.
#[test]
fn a_full_zone_counts_aggregate_dropped() {
    let (_zone, region, store) = table(2);
    for key in 0..3 {
        crate::shm::mock::seed(&store, 1, KeyHash(key), 0, 1);
    }
    assert_eq!(region.stats().snapshot().aggregate_dropped, 1);
}
