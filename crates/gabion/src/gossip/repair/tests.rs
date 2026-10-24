use super::*;
use crate::crdt::{
    CellStoreConfig, DeltaSink, KeyHash, NodeId, NodeIdentity, Observation, ObservationBatch,
    RuleDescriptor,
};

const RULE: u128 = 0x11;

fn store(local: u128) -> CellStore<u32> {
    let mut store = CellStore::new(
        CellStoreConfig {
            cell_capacity: 256,
            rule_dictionary_capacity: 4,
            node_dictionary_capacity: 8,
            local_dirty_capacity: 64,
            forwarded_dirty_capacity: 0,
            peer_capacity: 0,
        },
        NodeIdentity::new(NodeId(local), 1),
    );
    store.intern_rule(RuleDescriptor {
        fingerprint: RULE,
        window_millis: 60_000,
        bucket_millis: 1_000,
        limit: 100,
        flags: 0,
        local_rule_id: 0,
    });
    store
}

fn merge(store: &mut CellStore<u32>, origin: u128, key: u128, count: u32, sequence: u64) {
    merge_at(store, origin, key, count, sequence, 0);
}

fn merge_at(
    store: &mut CellStore<u32>,
    origin: u128,
    key: u128,
    count: u32,
    sequence: u64,
    last_update_millis: u64,
) {
    let mut obs = ObservationBatch::with_capacity(1);
    obs.push(Observation {
        rule_fingerprint: RULE,
        key_hash: KeyHash(key),
        bucket: 0,
        origin: NodeId(origin),
        incarnation: 1,
        count,
        last_update_millis,
        origin_sequence: sequence,
    });
    store.merge_remote(&obs, &mut DeltaSink::with_capacity(1));
}

fn digest_of(store: &CellStore<u32>) -> (Vec<DigestEntry>, DigestScope) {
    let entries = DigestBuilder::for_store(store).build(store).to_vec();
    let scope = DigestScope {
        from: OriginKey::default(),
        to: OriginKey {
            node_id: NodeId(u128::MAX),
            incarnation: u32::MAX,
        },
    };
    (entries, scope)
}

/// What `holder` would send to a peer whose digest is `(entries, scope)`.
fn lacked(holder: &CellStore<u32>, entries: Vec<DigestEntry>, scope: DigestScope) -> Vec<u64> {
    let mut reader = DigestReader::for_store(holder);
    reader.begin();
    entries.into_iter().for_each(|e| reader.push(e));
    reader.set_scope(scope);
    let mut out = Vec::new();
    reader.cells_lacked(holder, 100, 0, 0, &mut out);
    let mut sequences: Vec<_> = out
        .iter()
        .map(|h| holder.get(*h).unwrap().origin_sequence)
        .collect();
    sequences.sort_unstable();
    sequences
}

#[test]
fn builder_lists_one_entry_per_origin_sorted() {
    let mut s = store(5);
    merge(&mut s, 30, 1, 1, 4);
    merge(&mut s, 20, 2, 1, 9);
    merge(&mut s, 20, 3, 1, 7);
    let (entries, _) = digest_of(&s);
    let ids: Vec<_> = entries.iter().map(|e| e.origin.node_id.0).collect();
    assert_eq!(ids, [20, 30]);
    assert_eq!(entries[0].max_sequence, 9);
    assert!(entries[0].holds(9) && entries[0].holds(7) && !entries[0].holds(8));
}

#[test]
fn a_gap_in_the_middle_is_named_and_served() {
    let mut holder = store(1);
    let mut peer = store(2);
    for sequence in 1..=5 {
        merge(&mut holder, 9, sequence as u128, 1, sequence);
        if sequence != 3 {
            merge(&mut peer, 9, sequence as u128, 1, sequence);
        }
    }
    let (entries, scope) = digest_of(&peer);
    assert_eq!(lacked(&holder, entries, scope), [3]);
}

#[test]
fn a_newer_version_of_a_held_cell_is_served() {
    let mut holder = store(1);
    let mut peer = store(2);
    merge(&mut peer, 9, 1, 1, 1);
    merge(&mut holder, 9, 1, 3, 6);
    let (entries, scope) = digest_of(&peer);
    assert_eq!(lacked(&holder, entries, scope), [6]);
}

#[test]
fn an_empty_digest_gets_everything_and_a_current_one_gets_nothing() {
    let mut holder = store(1);
    merge(&mut holder, 9, 1, 1, 1);
    merge(&mut holder, 8, 2, 1, 1);
    let (entries, scope) = digest_of(&store(2));
    assert_eq!(lacked(&holder, entries, scope).len(), 2);

    let (entries, scope) = digest_of(&holder);
    assert!(lacked(&holder, entries, scope).is_empty());
}

#[test]
fn origins_outside_the_packet_scope_are_left_alone() {
    let mut holder = store(1);
    merge(&mut holder, 9, 1, 1, 1);
    merge(&mut holder, 50, 2, 1, 1);
    let scope = DigestScope {
        from: OriginKey::default(),
        to: OriginKey {
            node_id: NodeId(10),
            incarnation: 0,
        },
    };
    assert_eq!(
        lacked(&holder, Vec::new(), scope),
        [1],
        "only origin 9 is in scope"
    );
}

#[test]
fn the_cell_budget_bounds_the_answer() {
    let mut holder = store(1);
    for key in 0..10 {
        merge(&mut holder, 9, key, 1, key as u64 + 1);
    }
    let (entries, scope) = digest_of(&store(2));
    let mut reader = DigestReader::for_store(&holder);
    reader.begin();
    entries.into_iter().for_each(|e| reader.push(e));
    reader.set_scope(scope);
    let mut out = Vec::new();
    reader.cells_lacked(&holder, 4, 0, 0, &mut out);
    assert_eq!(out.len(), 4);
}

#[test]
fn a_change_still_spreading_is_left_to_the_forwarders() {
    let mut holder = store(1);
    merge_at(&mut holder, 9, 1, 1, 1, 1_000);
    merge_at(&mut holder, 9, 2, 1, 2, 2_000);
    let (entries, scope) = digest_of(&store(2));
    let mut reader = DigestReader::for_store(&holder);
    reader.begin();
    entries.into_iter().for_each(|e| reader.push(e));
    reader.set_scope(scope);
    let mut out = Vec::new();
    reader.cells_lacked(&holder, 100, 2_000, 500, &mut out);
    let sequences: Vec<_> = out
        .iter()
        .map(|h| holder.get(*h).unwrap().origin_sequence)
        .collect();
    assert_eq!(sequences, [1], "only the change settled by 1,500 ms");
}

#[test]
fn a_cell_whose_bucket_left_the_window_is_not_resent() {
    let mut holder = store(1);
    merge(&mut holder, 9, 1, 1, 1);
    let (entries, scope) = digest_of(&store(2));
    let mut reader = DigestReader::for_store(&holder);
    reader.begin();
    entries.into_iter().for_each(|e| reader.push(e));
    reader.set_scope(scope);
    let mut out = Vec::new();
    // Bucket 0 of a 60 s window of 1 s buckets is live through 60,999 ms.
    reader.cells_lacked(&holder, 100, 60_999, 0, &mut out);
    assert_eq!(out.len(), 1);
    reader.cells_lacked(&holder, 100, 61_000, 0, &mut out);
    assert!(out.is_empty(), "the requester has expired it");
}
