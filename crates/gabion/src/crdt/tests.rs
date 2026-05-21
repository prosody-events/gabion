use super::hash::mix64;
use super::*;

fn local() -> NodeIdentity {
    NodeIdentity::new(NodeId(1), 1)
}

fn rule_descriptor(fingerprint: u128, local_id: u32) -> RuleDescriptor {
    RuleDescriptor {
        fingerprint,
        window_millis: 1000,
        bucket_millis: 1000,
        limit: 1000,
        flags: 0,
        local_rule_id: local_id,
    }
}

fn store_u32() -> CellStore<u32> {
    CellStore::new(
        CellStoreConfig {
            cell_capacity: 16,
            rule_dictionary_capacity: 8,
            node_dictionary_capacity: 8,
            local_dirty_capacity: 16,
            forwarded_dirty_capacity: 16,
            peer_capacity: 4,
        },
        local(),
    )
}

fn small_store(cell_cap: u32, dirty_cap: usize) -> CellStore<u32> {
    CellStore::new(
        CellStoreConfig {
            cell_capacity: cell_cap,
            rule_dictionary_capacity: 8,
            node_dictionary_capacity: 8,
            local_dirty_capacity: dirty_cap,
            forwarded_dirty_capacity: dirty_cap,
            peer_capacity: 4,
        },
        local(),
    )
}

/// Build an `Observation` row with positional args. Test convenience for the
/// many fixture rows in this file — production code constructs `Observation`
/// directly with field names.
fn observation<C: Count>(
    rule_fp: u128,
    key: u128,
    bucket: BucketEpoch,
    origin: u128,
    inc: Incarnation,
    count: C,
    ts: u64,
) -> Observation<C> {
    Observation {
        rule_fingerprint: rule_fp,
        key_hash: KeyHash(key),
        bucket,
        origin: NodeId(origin),
        incarnation: inc,
        count,
        last_update_millis: ts,
    }
}

fn remote_merge_one(
    store: &mut CellStore<u32>,
    rule_fp: u128,
    origin: u128,
    inc: Incarnation,
    count: u32,
    ts: u64,
) -> CellHandle {
    let mut obs = ObservationBatch::with_capacity(1);
    let mut sink = DeltaSink::with_capacity(1);
    obs.push(observation(rule_fp, 0xabc, 0, origin, inc, count, ts));
    store.merge_remote(&obs, &mut sink);
    // A no-op merge (count <= stored) leaves the sink empty; recover the
    // handle via key lookup instead.
    if let Some(handle) = sink.handles.first().copied() {
        return handle;
    }
    let rule_slot = store.find_rule(rule_fp).expect("rule interned");
    let node_slot = store
        .find_node(NodeId(origin), inc)
        .expect("origin interned");
    store
        .find(CompactCellKey {
            rule: rule_slot,
            key_hash: KeyHash(0xabc),
            bucket: 0,
            origin: node_slot,
            incarnation: inc,
        })
        .expect("cell present after merge")
}

fn local_increment_one(
    store: &mut CellStore<u32>,
    rule_fp: u128,
    key: u128,
    bucket: BucketEpoch,
    hits: u32,
    ts: u64,
) -> CellHandle {
    let mut obs = ObservationBatch::with_capacity(1);
    let mut sink = DeltaSink::with_capacity(1);
    obs.push(observation(
        rule_fp,
        key,
        bucket,
        local().node_id.0,
        local().incarnation,
        hits,
        ts,
    ));
    store.ingest_local(&obs, &mut sink);
    if let Some(handle) = sink.handles.first().copied() {
        return handle;
    }
    // Saturating-already-at-max case: recover the handle by key.
    let rule_slot = store.find_rule(rule_fp).expect("rule interned");
    let node_slot = store
        .find_node(local().node_id, local().incarnation)
        .expect("local node interned");
    store
        .find(CompactCellKey {
            rule: rule_slot,
            key_hash: KeyHash(key),
            bucket,
            origin: node_slot,
            incarnation: local().incarnation,
        })
        .expect("cell present after ingest")
}

#[test]
fn merge_remote_is_monotonic() {
    let mut store = store_u32();
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    let mut sink = DeltaSink::with_capacity(4);

    let mut obs = ObservationBatch::with_capacity(1);
    obs.push(observation(0x11, 0xabc, 0, 7, 1, 5, 100));
    store.merge_remote(&obs, &mut sink);
    assert_eq!(sink.len(), 1);
    let handle = sink.handles[0];
    assert_eq!(sink.previous[0], 0);
    assert_eq!(sink.current[0], 5);

    // Stale (3 < 5) — no delta.
    sink.clear();
    let mut obs2 = ObservationBatch::with_capacity(1);
    obs2.push(observation(0x11, 0xabc, 0, 7, 1, 3, 200));
    store.merge_remote(&obs2, &mut sink);
    assert_eq!(sink.len(), 0);

    // Higher (8 > 5) — delta of 3.
    sink.clear();
    let mut obs3 = ObservationBatch::with_capacity(1);
    obs3.push(observation(0x11, 0xabc, 0, 7, 1, 8, 300));
    store.merge_remote(&obs3, &mut sink);
    assert_eq!(sink.len(), 1);
    assert_eq!(sink.previous[0], 5);
    assert_eq!(sink.current[0], 8);
    assert_eq!(sink.deltas[0], 3);

    assert_eq!(store.count_of(handle).unwrap(), 8);
}

#[test]
fn different_origins_are_independent() {
    let mut store = store_u32();
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    let _a = remote_merge_one(&mut store, 0x11, 1, 1, 5, 0);
    let _b = remote_merge_one(&mut store, 0x11, 2, 1, 7, 0);
    assert_eq!(store.active_len(), 2);
}

#[test]
fn merge_order_does_not_matter() {
    let make = |order: &[(u128, u32)]| {
        let mut store = store_u32();
        store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
        for (origin, count) in order {
            remote_merge_one(&mut store, 0x11, *origin, 1, *count, 0);
        }
        let mut out: Vec<(u128, u32)> = store
            .active_handles()
            .map(|h| store.get(h).unwrap())
            .map(|row| {
                let node = store.node_dictionary().descriptor(row.key.origin).unwrap();
                (node.node_id.0, row.count)
            })
            .collect();
        out.sort();
        out
    };

    let a = (1_u128, 5_u32);
    let b = (1_u128, 9_u32);
    let c = (2_u128, 3_u32);
    assert_eq!(make(&[a, b, c]), make(&[c, b, a]));
    assert_eq!(make(&[a, b, c]), make(&[b, c, a]));
}

#[test]
fn increment_local_accumulates() {
    let mut store = store_u32();
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    let handle = local_increment_one(&mut store, 0x11, 0xabc, 0, 3, 10);
    let _ = local_increment_one(&mut store, 0x11, 0xabc, 0, 4, 20);
    let row = store.get(handle).unwrap();
    assert_eq!(row.count, 7);
    assert_eq!(row.last_update_millis, 20);
}

#[test]
fn capacity_is_enforced_without_growing() {
    let mut store = small_store(1, 4);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    let mut sink = DeltaSink::with_capacity(2);
    let mut a = ObservationBatch::with_capacity(1);
    a.push(observation(0x11, 0xabc, 0, 1, 1, 1, 0));
    store.merge_remote(&a, &mut sink);
    let mut b = ObservationBatch::with_capacity(1);
    b.push(observation(0x11, 0xabc, 0, 2, 1, 1, 0));
    sink.clear();
    store.merge_remote(&b, &mut sink);
    assert_eq!(sink.len(), 0); // second insert was rejected
    assert_eq!(store.active_len(), 1);
    assert_eq!(store.stats().cell_store_full_rejects, 1);
    assert_eq!(store.capacity(), 1);
}

#[test]
fn dirty_ring_yields_each_changed_cell_once() {
    let mut store = small_store(4, 16);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    // Non-local origins so both cells route through forwarded_dirty.
    remote_merge_one(&mut store, 0x11, 10, 1, 5, 0);
    remote_merge_one(&mut store, 0x11, 10, 1, 9, 0);
    remote_merge_one(&mut store, 0x11, 20, 1, 3, 0);

    // Count current entries in forwarded_dirty.
    let mut seen: Vec<u32> = Vec::new();
    store.visit_forwarded_dirty(|h, s| {
        if !seen.contains(&h.index) {
            seen.push(h.index);
        }
        // Also assert each is current — staleness filter applied internally.
        assert!(s.resolve(h).is_some());
        true
    });
    seen.sort();
    assert_eq!(seen.len(), 2);
}

#[test]
fn dirty_ring_overflows_when_full() {
    let mut store = small_store(8, 2);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    for origin in 1..=4 {
        remote_merge_one(&mut store, 0x11, origin, 1, 1, 0);
    }
    assert!(store.forwarded_dirty().overflowed());
    assert_eq!(store.forwarded_dirty().len(), 2);
}

#[test]
fn clear_dirty_keeps_cells_but_drops_change_record() {
    let mut store = small_store(2, 4);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    remote_merge_one(&mut store, 0x11, 1, 1, 5, 0);
    store.clear_dirty();
    assert_eq!(store.active_len(), 1);
    assert_eq!(store.forwarded_dirty().len(), 0);
    assert_eq!(store.local_dirty().len(), 0);
}

#[test]
fn gossip_frame_prioritizes_local_then_forwarded_then_repair() {
    let mut store = small_store(8, 8);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();

    remote_merge_one(&mut store, 0x11, 3, 1, 30, 0);
    local_increment_one(&mut store, 0x11, 0xabc, 0, 10, 0);
    remote_merge_one(&mut store, 0x11, 2, 1, 20, 0);
    store.clear_dirty();

    // Update everything: local rises and forwarded rises.
    remote_merge_one(&mut store, 0x11, 2, 1, 21, 1);
    local_increment_one(&mut store, 0x11, 0xabc, 0, 1, 1);

    let mut out = Vec::with_capacity(3);
    store.fill_gossip_frame(3, &mut out);
    let origins: Vec<u128> = out
        .iter()
        .map(|h| {
            let row = store.get(*h).unwrap();
            store
                .node_dictionary()
                .descriptor(row.key.origin)
                .unwrap()
                .node_id
                .0
        })
        .collect();

    // First two are dirty (local then forwarded), third is repair.
    assert_eq!(origins[0], 1);
    assert_eq!(origins[1], 2);
    assert_eq!(origins[2], 3);
}

#[test]
fn gossip_frame_forwards_remote_dirty_cells_without_local_dirty() {
    let mut store = small_store(4, 8);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    remote_merge_one(&mut store, 0x11, 2, 1, 20, 0);
    remote_merge_one(&mut store, 0x11, 3, 1, 30, 0);

    let mut out = Vec::with_capacity(2);
    store.fill_gossip_frame(2, &mut out);
    let mut origins: Vec<u128> = out
        .iter()
        .map(|h| {
            store
                .node_dictionary()
                .descriptor(store.get(*h).unwrap().key.origin)
                .unwrap()
                .node_id
                .0
        })
        .collect();
    origins.sort();
    assert_eq!(origins, vec![2, 3]);
}

#[test]
fn repair_slice_rotates_across_active_cells() {
    let mut store = small_store(4, 8);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    for origin in 1..=3 {
        remote_merge_one(&mut store, 0x11, origin, 1, origin as u32, 0);
    }
    store.clear_dirty();

    let mut out = Vec::with_capacity(1);
    store.fill_gossip_frame(1, &mut out);
    let first = store
        .node_dictionary()
        .descriptor(store.get(out[0]).unwrap().key.origin)
        .unwrap()
        .node_id
        .0;
    store.fill_gossip_frame(1, &mut out);
    let second = store
        .node_dictionary()
        .descriptor(store.get(out[0]).unwrap().key.origin)
        .unwrap()
        .node_id
        .0;
    store.fill_gossip_frame(1, &mut out);
    let third = store
        .node_dictionary()
        .descriptor(store.get(out[0]).unwrap().key.origin)
        .unwrap()
        .node_id
        .0;
    store.fill_gossip_frame(1, &mut out);
    let fourth = store
        .node_dictionary()
        .descriptor(store.get(out[0]).unwrap().key.origin)
        .unwrap()
        .node_id
        .0;

    let mut seen = vec![first, second, third];
    seen.sort();
    assert_eq!(seen, vec![1, 2, 3]);
    // Fourth wrap returns to one of the previously seen origins.
    assert!([1_u128, 2, 3].contains(&fourth));
}

// --- New tests required by the plan -----------------------------------

#[test]
fn delta_sink_records_exact_raises() {
    let mut store = small_store(4, 4);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    let mut sink = DeltaSink::with_capacity(4);
    let mut obs = ObservationBatch::with_capacity(2);
    obs.push(observation(0x11, 0xabc, 0, 1, 1, 5, 0));
    obs.push(observation(0x11, 0xabc, 0, 2, 1, 7, 0));
    store.merge_remote(&obs, &mut sink);
    assert_eq!(sink.len(), 2);

    sink.clear();
    // No-op merges (count <= stored) produce nothing.
    let mut obs2 = ObservationBatch::with_capacity(2);
    obs2.push(observation(0x11, 0xabc, 0, 1, 1, 5, 0));
    obs2.push(observation(0x11, 0xabc, 0, 2, 1, 1, 0));
    store.merge_remote(&obs2, &mut sink);
    assert_eq!(sink.len(), 0);
}

#[test]
fn delta_applies_locally_reflects_rule_dictionary() {
    let mut store = small_store(4, 4);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    // 0x22 fingerprint unknown locally.
    let mut sink = DeltaSink::with_capacity(2);
    let mut obs = ObservationBatch::with_capacity(2);
    obs.push(observation(0x11, 0xabc, 0, 9, 1, 1, 0));
    obs.push(observation(0x22, 0xabc, 0, 9, 1, 1, 0));
    store.merge_remote(&obs, &mut sink);
    assert_eq!(sink.len(), 2);
    assert_eq!(sink.applies_locally[0], 1);
    assert_eq!(sink.applies_locally[1], 0);
}

#[test]
fn unknown_rule_cells_round_trip() {
    let mut store = small_store(4, 8);
    // No descriptor registered for 0x33.
    remote_merge_one(&mut store, 0x33, 5, 1, 10, 0);
    let handle = remote_merge_one(&mut store, 0x33, 5, 1, 20, 1);
    let row = store.get(handle).unwrap();
    assert_eq!(row.count, 20);
    // The rule slot still has a descriptor, but applies_locally is false.
    let rule_desc = store.rule_dictionary().descriptor(row.key.rule).unwrap();
    assert!(!rule_desc.applies_locally());

    let mut sink = DeltaSink::with_capacity(1);
    let mut obs = ObservationBatch::with_capacity(1);
    obs.push(observation(0x33, 0xabc, 0, 5, 1, 30, 2));
    store.merge_remote(&obs, &mut sink);
    assert_eq!(sink.applies_locally[0], 0);

    // Expire: bucket 0, live = 0, current = 5 -> expired.
    let mut cur = vec![0_u32; store.rule_dictionary().capacity() as usize];
    let mut live = vec![0_u32; cur.len()];
    cur[row.key.rule as usize] = 5;
    live[row.key.rule as usize] = 0;
    let mut exp = ExpirationSink::<u32>::with_capacity(1);
    store.expire(&cur, &live, &mut exp);
    assert_eq!(store.active_len(), 0);
}

#[test]
fn compact_id_round_trip() {
    let mut store = store_u32();
    store.intern_rule(rule_descriptor(0xAA, 1)).unwrap();
    let rule_slot = store.find_rule(0xAA).unwrap();
    let _ = remote_merge_one(&mut store, 0xAA, 99, 7, 1, 0);
    let node_slot = store.find_node(NodeId(99), 7).unwrap();
    // Round-trip the dictionaries.
    let rd = store.rule_dictionary().descriptor(rule_slot).unwrap();
    assert_eq!(rd.fingerprint, 0xAA);
    let nd = store.node_dictionary().descriptor(node_slot).unwrap();
    assert_eq!(nd.node_id, NodeId(99));
    assert_eq!(nd.incarnation, 7);
}

#[test]
fn dictionary_refcount_frees_unreferenced_slots() {
    let mut store = small_store(4, 4);
    store.intern_rule(rule_descriptor(0xBB, 1)).unwrap();
    let h = remote_merge_one(&mut store, 0xBB, 100, 1, 1, 0);
    let row = store.get(h).unwrap();
    let node_slot = row.key.origin;
    let rule_slot = row.key.rule;
    assert_eq!(store.node_dictionary().refcount(node_slot), 1);
    assert_eq!(store.rule_dictionary().refcount(rule_slot), 1);

    // Expire the cell.
    let mut cur = vec![0_u32; store.rule_dictionary().capacity() as usize];
    let mut live = vec![0_u32; cur.len()];
    cur[rule_slot as usize] = 5;
    live[rule_slot as usize] = 0;
    let mut exp = ExpirationSink::<u32>::with_capacity(1);
    store.expire(&cur, &live, &mut exp);
    assert_eq!(store.active_len(), 0);
    // Refcounts return to 0 on both dictionaries; the corresponding slot
    // entries are freed and `descriptor()` reports None.
    assert_eq!(store.node_dictionary().refcount(node_slot), 0);
    assert!(store.node_dictionary().descriptor(node_slot).is_none());
    assert_eq!(store.rule_dictionary().refcount(rule_slot), 0);
    assert!(store.rule_dictionary().descriptor(rule_slot).is_none());
}

#[test]
fn dictionary_full_rejects_with_counter() {
    let mut store = CellStore::<u32>::new(
        CellStoreConfig {
            cell_capacity: 16,
            rule_dictionary_capacity: 1,
            node_dictionary_capacity: 4,
            local_dirty_capacity: 4,
            forwarded_dirty_capacity: 4,
            peer_capacity: 4,
        },
        local(),
    );
    // First merge interns a rule. Second merge with a distinct fingerprint
    // must fail.
    let h1 = remote_merge_one(&mut store, 0x10, 2, 1, 1, 0);
    assert!(store.get(h1).is_some());
    let mut sink = DeltaSink::with_capacity(1);
    let mut obs = ObservationBatch::with_capacity(1);
    obs.push(observation(0x20, 0xabc, 0, 3, 1, 1, 0));
    store.merge_remote(&obs, &mut sink);
    assert_eq!(sink.len(), 0);
    assert!(store.stats().rule_dictionary_full_rejects >= 1);
}

#[test]
fn selection_epoch_dedupes_within_frame() {
    let mut store = small_store(4, 8);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    // Same handle dirty as both local and forwarded paths? Forwarded
    // dirty entries with handle equal to a local-dirty handle should not
    // be re-emitted.  We construct two updates against the same handle
    // (both local-origin) — the dirty ring will hold two entries, only
    // the latest is current, and the gossip frame must emit exactly one.
    local_increment_one(&mut store, 0x11, 0xabc, 0, 1, 0);
    local_increment_one(&mut store, 0x11, 0xabc, 0, 2, 0);
    let mut out = Vec::with_capacity(8);
    store.fill_gossip_frame(8, &mut out);
    let mut indices: Vec<u32> = out.iter().map(|h| h.index).collect();
    indices.sort();
    indices.dedup();
    assert_eq!(out.len(), indices.len());
}

#[test]
fn robin_hood_backshift_keeps_probe_bounded() {
    // Deterministic churn — insert and remove many entries and verify
    // the index keeps probe distance bounded relative to log2(capacity).
    let cap = 1024_u32;
    let mut index = CellIndex::with_capacity(cap);
    let mut occupants: Vec<(u64, u32)> = Vec::new();
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for _ in 0..(cap as usize / 2) {
        state = mix64(state);
        let slot = (state as u32) % (cap / 2);
        let h = mix64(state.wrapping_add(slot as u64));
        index.insert_unchecked(h, slot);
        occupants.push((h, slot));
    }
    // Churn: remove half, insert different.
    for _ in 0..(occupants.len() / 2) {
        let (h, slot) = occupants.pop().unwrap();
        index.remove(h, slot);
    }
    for _ in 0..(cap as usize / 4) {
        state = mix64(state);
        let slot = ((state >> 16) as u32) % (cap / 2) + (cap / 2);
        let h = mix64(state.wrapping_add(slot as u64));
        index.insert_unchecked(h, slot);
        occupants.push((h, slot));
    }
    let max_dist = index.max_probe_distance();
    // Bound is generous: should be <= load * small constant. Cap log2 = 10,
    // and load factor is below 50%. Empirical worst case ~ a few dozen.
    assert!(
        max_dist < 64,
        "max probe distance {max_dist} too large under churn"
    );
}

#[test]
fn repair_cursor_skips_freed_slots() {
    let mut store = small_store(8, 16);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    // Insert 4 cells at known origins.
    let h1 = remote_merge_one(&mut store, 0x11, 1, 1, 1, 0);
    let _h2 = remote_merge_one(&mut store, 0x11, 2, 1, 1, 0);
    let h3 = remote_merge_one(&mut store, 0x11, 3, 1, 1, 0);
    let _h4 = remote_merge_one(&mut store, 0x11, 4, 1, 1, 0);
    store.clear_dirty();

    // Free h1 and h3 directly.
    store.free_cell_at(h1.index);
    store.free_cell_at(h3.index);
    assert_eq!(store.active_len(), 2);

    // Repair visits exactly the active 2.
    let mut seen = Vec::with_capacity(4);
    let mut out = Vec::with_capacity(4);
    for _ in 0..4 {
        store.fill_gossip_frame(1, &mut out);
        if let Some(h) = out.first() {
            seen.push(h.index);
        }
    }
    // Across 4 calls, only the 2 active slots are returned (each twice).
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 2);
}

#[test]
fn peer_frontier_lacks_matches_scan() {
    let mut store = small_store(8, 16);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    let h1 = remote_merge_one(&mut store, 0x11, 1, 1, 1, 0);
    let _h2 = remote_merge_one(&mut store, 0x11, 2, 1, 1, 0);
    let row1 = store.get(h1).unwrap();
    let peer_slot = store.peer_frontiers_mut().intern_peer(NodeId(7)).unwrap();
    // Mark origin slot of h1 as fully acked.
    store
        .peer_frontiers_mut()
        .record_acked(peer_slot, row1.key.origin, row1.origin_sequence);

    // Build active arrays.
    let mut active_indices = Vec::new();
    let mut active_origins = Vec::new();
    for slot in 0..store.capacity() {
        if (store.generations[slot as usize] & 1) == 1 {
            active_indices.push(slot);
            active_origins.push(store.origins[slot as usize]);
        }
    }
    // origin_sequences for active slots.
    let origin_sequences: Vec<u64> = (0..store.capacity())
        .map(|s| store.origin_sequences[s as usize])
        .collect();
    let active_origins_full: Vec<NodeSlot> = (0..store.capacity())
        .map(|s| store.origins[s as usize])
        .collect();

    let mut lacks = Vec::new();
    store.peer_frontiers().lacks_indices(
        peer_slot,
        &active_origins_full,
        &origin_sequences,
        &active_indices,
        &mut lacks,
    );
    // Brute-force expected: everything but h1 (which is at last_acked).
    let mut expected: Vec<u32> = active_indices
        .iter()
        .copied()
        .filter(|i| *i != h1.index)
        .collect();
    expected.sort();
    lacks.sort();
    assert_eq!(lacks, expected);
}

#[test]
fn node_slot_reuse_clears_frontier_rows() {
    let mut store = CellStore::<u32>::new(
        CellStoreConfig {
            cell_capacity: 4,
            rule_dictionary_capacity: 4,
            node_dictionary_capacity: 4,
            local_dirty_capacity: 4,
            forwarded_dirty_capacity: 4,
            peer_capacity: 2,
        },
        local(),
    );
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    let h = remote_merge_one(&mut store, 0x11, 42, 1, 5, 0);
    let row = store.get(h).unwrap();
    let original_slot = row.key.origin;
    let peer_slot = store.peer_frontiers_mut().intern_peer(NodeId(7)).unwrap();
    store
        .peer_frontiers_mut()
        .record_acked(peer_slot, original_slot, 99);
    store
        .peer_frontiers_mut()
        .record_sent(peer_slot, original_slot, 88);

    // Expire the cell — node slot freed (rule was pre-interned, stays).
    let mut cur = vec![0_u32; store.rule_dictionary().capacity() as usize];
    let mut live = vec![0_u32; cur.len()];
    cur[row.key.rule as usize] = 5;
    live[row.key.rule as usize] = 0;
    let mut exp = ExpirationSink::<u32>::with_capacity(1);
    store.expire(&cur, &live, &mut exp);

    // The slot is now free. Frontier rows must be zeroed.
    assert_eq!(
        store.peer_frontiers().last_acked(peer_slot, original_slot),
        0
    );
    assert_eq!(
        store.peer_frontiers().last_sent(peer_slot, original_slot),
        0
    );

    // Reassign to a different (node_id, incarnation). Since dict allocates
    // from freelist head, this should reuse the same slot.
    let _h2 = remote_merge_one(&mut store, 0x11, 99, 9, 1, 0);
    let reused_slot = store.node_dictionary().find(NodeId(99), 9).unwrap();
    assert_eq!(reused_slot, original_slot);
    // Frontier state must still be zero for the reused slot.
    assert_eq!(store.peer_frontiers().last_acked(peer_slot, reused_slot), 0);
}

#[test]
fn dirty_lanes_separate() {
    let mut store = small_store(4, 8);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    local_increment_one(&mut store, 0x11, 0xabc, 0, 1, 0);
    remote_merge_one(&mut store, 0x11, 99, 1, 1, 0);
    assert_eq!(store.local_dirty().len(), 1);
    assert_eq!(store.forwarded_dirty().len(), 1);
}

#[test]
fn count_widths_compile_and_saturate() {
    // u16 saturation.
    let mut s16 = CellStore::<u16>::new(
        CellStoreConfig {
            cell_capacity: 4,
            rule_dictionary_capacity: 4,
            node_dictionary_capacity: 4,
            local_dirty_capacity: 4,
            forwarded_dirty_capacity: 4,
            peer_capacity: 2,
        },
        local(),
    );
    s16.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    let mut obs = ObservationBatch::<u16>::with_capacity(1);
    let mut sink = DeltaSink::<u16>::with_capacity(1);
    obs.push(Observation {
        rule_fingerprint: 0x11,
        key_hash: KeyHash(1),
        bucket: 0,
        origin: NodeId(1),
        incarnation: 1,
        count: u16::MAX,
        last_update_millis: 0,
    });
    s16.ingest_local(&obs, &mut sink);
    // Add more hits — should saturate at u16::MAX, no overflow.
    let mut obs2 = ObservationBatch::<u16>::with_capacity(1);
    let mut sink2 = DeltaSink::<u16>::with_capacity(1);
    obs2.push(Observation {
        rule_fingerprint: 0x11,
        key_hash: KeyHash(1),
        bucket: 0,
        origin: NodeId(1),
        incarnation: 1,
        count: 1,
        last_update_millis: 0,
    });
    s16.ingest_local(&obs2, &mut sink2);
    // No raise because already at MAX.
    assert_eq!(sink2.len(), 0);

    // u64 stores big counts.
    let mut s64 = CellStore::<u64>::new(
        CellStoreConfig {
            cell_capacity: 4,
            rule_dictionary_capacity: 4,
            node_dictionary_capacity: 4,
            local_dirty_capacity: 4,
            forwarded_dirty_capacity: 4,
            peer_capacity: 2,
        },
        local(),
    );
    s64.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    let mut obs64 = ObservationBatch::<u64>::with_capacity(1);
    let mut sink64 = DeltaSink::<u64>::with_capacity(1);
    obs64.push(Observation {
        rule_fingerprint: 0x11,
        key_hash: KeyHash(1),
        bucket: 0,
        origin: NodeId(7),
        incarnation: 1,
        count: 1_000_000_000_000,
        last_update_millis: 0,
    });
    s64.merge_remote(&obs64, &mut sink64);
    assert_eq!(sink64.current[0], 1_000_000_000_000);
}

#[test]
fn incarnation_change_isolates() {
    let mut store = store_u32();
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    let h1 = remote_merge_one(&mut store, 0x11, 42, 1, 5, 0);
    let h2 = remote_merge_one(&mut store, 0x11, 42, 2, 5, 0);
    let r1 = store.get(h1).unwrap();
    let r2 = store.get(h2).unwrap();
    assert_ne!(r1.key.origin, r2.key.origin);
    assert_eq!(store.active_len(), 2);
}

#[test]
fn expire_emits_one_row_per_freed_cell() {
    // Two rules — one with applies_locally=true (local_rule_id < u32::MAX),
    // one whose descriptor `applies_locally()` returns false. Two origins.
    // Drive the doomed cells onto a single rule slot so its refcount falls
    // to zero — verifying emission happens *before* free_cell_at, since
    // a post-free descriptor() lookup would otherwise return None for the
    // doomed cells and applies_locally would silently degrade to false.
    let mut store = small_store(8, 16);
    let rule_local = store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    let rule_doomed = store
        .intern_rule(RuleDescriptor {
            fingerprint: 0x22,
            window_millis: 1000,
            bucket_millis: 1000,
            limit: 1000,
            flags: 0,
            local_rule_id: 1,
        })
        .unwrap();

    // Surviving cell on rule_local — kept because rule_local's live window
    // covers every bucket in the test.
    let _h_keep = remote_merge_one(&mut store, 0x11, 5, 1, 4, 0);

    // Doomed cells: rule_doomed, two origins, bucket 0.
    let mut obs = ObservationBatch::with_capacity(2);
    let mut sink = DeltaSink::with_capacity(2);
    obs.push(observation(0x22, 0xabc, 0, 5, 1, 7, 100));
    obs.push(observation(0x22, 0xdef, 0, 6, 1, 11, 200));
    store.merge_remote(&obs, &mut sink);

    let rule_local_slot = store.find_rule(0x11).unwrap();
    let rule_doomed_slot = store.find_rule(0x22).unwrap();
    assert_eq!(rule_local_slot, rule_local);
    assert_eq!(rule_doomed_slot, rule_doomed);
    let node5 = store.find_node(NodeId(5), 1).unwrap();
    let node6 = store.find_node(NodeId(6), 1).unwrap();
    let h_doomed_a = store
        .find(CompactCellKey {
            rule: rule_doomed,
            key_hash: KeyHash(0xabc),
            bucket: 0,
            origin: node5,
            incarnation: 1,
        })
        .unwrap();
    let h_doomed_b = store
        .find(CompactCellKey {
            rule: rule_doomed,
            key_hash: KeyHash(0xdef),
            bucket: 0,
            origin: node6,
            incarnation: 1,
        })
        .unwrap();

    let snap_a = store.get(h_doomed_a).unwrap();
    let snap_b = store.get(h_doomed_b).unwrap();
    assert_eq!(store.rule_dictionary().refcount(rule_doomed), 2);

    // Build thresholds: doomed cells (bucket 0, live 0, current 5) expire;
    // the keep cell on rule_local stays because its rule lives 1000 buckets.
    let mut cur = vec![0_u32; store.rule_dictionary().capacity() as usize];
    let mut live = vec![0_u32; cur.len()];
    cur[rule_doomed as usize] = 5;
    live[rule_doomed as usize] = 0;
    cur[rule_local as usize] = 0;
    live[rule_local as usize] = 1000;

    let mut exp = ExpirationSink::<u32>::with_capacity(store.capacity() as usize);
    store.expire(&cur, &live, &mut exp);

    assert_eq!(store.active_len(), 1);
    assert_eq!(exp.len(), 2);

    // Rule descriptor for the doomed slot should now be gone (refcount hit 0)
    // — proving emission ran *before* free_cell_at, since the rows captured
    // applies_locally=true from the live descriptor.
    assert_eq!(store.rule_dictionary().refcount(rule_doomed), 0);
    assert!(store.rule_dictionary().descriptor(rule_doomed).is_none());

    let mut rows: Vec<CellExpiration<u32>> = (0..exp.len()).map(|i| exp.row(i).unwrap()).collect();
    rows.sort_by_key(|r| r.handle.index);
    let mut expected = [snap_a, snap_b];
    expected.sort_by_key(|r| r.handle.index);
    for (got, snap) in rows.iter().zip(expected.iter()) {
        assert_eq!(got.handle, snap.handle);
        // The exported identity carries the rule fingerprint, not the
        // node-local rule slot — translate via the dictionary snapshot.
        assert_eq!(got.key.rule_fingerprint, 0x22);
        assert_eq!(got.key.key_hash, snap.key.key_hash);
        assert_eq!(got.key.bucket, snap.key.bucket);
        assert_eq!(got.last_count, snap.count);
        assert_eq!(got.last_update_millis, snap.last_update_millis);
        assert!(got.applies_locally);
    }
}

#[test]
fn expire_frees_slots_and_dictionary_refs() {
    let mut store = small_store(4, 4);
    let rule_slot = store.intern_rule(rule_descriptor(0xCC, 1)).unwrap();
    let _h = remote_merge_one(&mut store, 0xCC, 100, 1, 5, 0);
    assert_eq!(store.active_len(), 1);
    assert_eq!(
        store
            .node_dictionary()
            .refcount(store.node_dictionary().find(NodeId(100), 1).unwrap()),
        1
    );

    let mut cur = vec![0_u32; store.rule_dictionary().capacity() as usize];
    let mut live = vec![0_u32; cur.len()];
    cur[rule_slot as usize] = 5;
    live[rule_slot as usize] = 0;
    let mut exp = ExpirationSink::<u32>::with_capacity(1);
    store.expire(&cur, &live, &mut exp);
    assert_eq!(store.active_len(), 0);
    // Node dictionary no longer holds the (NodeId(100), 1) entry.
    assert!(store.node_dictionary().find(NodeId(100), 1).is_none());
}

#[test]
fn handle_generation_is_aba_safe() {
    let mut store = small_store(2, 4);
    store.intern_rule(rule_descriptor(0xDD, 1)).unwrap();
    let h1 = remote_merge_one(&mut store, 0xDD, 1, 1, 1, 0);
    // Free the slot directly.
    store.free_cell_at(h1.index);
    // Reuse the slot via a new merge.
    let h2 = remote_merge_one(&mut store, 0xDD, 2, 1, 1, 0);
    assert_eq!(h1.index, h2.index);
    assert_ne!(h1.generation, h2.generation);
    // Old handle does not resolve.
    assert!(store.get(h1).is_none());
    // New handle does.
    assert!(store.get(h2).is_some());
}

#[test]
fn selection_epoch_wraparound_resets_marks() {
    let mut store = small_store(4, 4);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();
    // Seed the store with a real cell so mark_selected has something to act on.
    remote_merge_one(&mut store, 0x11, 1, 1, 1, 0);
    // Force the epoch close to wraparound and seed a stale mark.
    store.selection_epoch = u32::MAX;
    store.selection_marks[0] = u32::MAX;
    store.bump_selection_epoch();
    // Wraparound branch must reset marks and set epoch to 1.
    assert_eq!(store.selection_epoch, 1);
    assert!(store.selection_marks.iter().all(|m| *m == 0));
    // mark_selected on a freshly-zeroed slot succeeds against epoch 1.
    assert!(store.mark_selected(0));
    assert_eq!(store.selection_marks[0], 1);
}

#[test]
fn expire_at_and_expire_agree_for_same_now() {
    let mk = || {
        let mut s = small_store(8, 8);
        s.intern_rule(RuleDescriptor {
            fingerprint: 0x11,
            window_millis: 100,
            bucket_millis: 50,
            limit: 10,
            flags: 0,
            local_rule_id: 1,
        })
        .unwrap();
        // Seed two cells: one at bucket 0 (will expire), one at bucket 5
        // (still inside the live window for now=300 -> current=6, live=2).
        let mut obs = ObservationBatch::with_capacity(2);
        let mut sink = DeltaSink::with_capacity(2);
        obs.push(observation(0x11, 0xabc, 0, 7, 1, 5, 0));
        obs.push(observation(0x11, 0xdef, 5, 8, 1, 5, 250));
        s.merge_remote(&obs, &mut sink);
        s
    };

    let now_millis: u64 = 300;
    // 300/50 = 6, window/bucket = 100/50 = 2 (live buckets)
    let bucket_millis: u32 = 50;
    let live = 2_u32;
    let current = (now_millis / bucket_millis as u64) as u32;

    let mut a = mk();
    let mut sink_a = ExpirationSink::<u32>::with_capacity(8);
    a.expire_at(now_millis, &mut sink_a);

    let mut b = mk();
    let dict_cap = b.rule_dictionary().capacity() as usize;
    let mut cur = vec![0_u32; dict_cap];
    let mut lv = vec![0_u32; dict_cap];
    let rule_slot = b.find_rule(0x11).unwrap();
    cur[rule_slot as usize] = current;
    lv[rule_slot as usize] = live;
    let mut sink_b = ExpirationSink::<u32>::with_capacity(8);
    b.expire(&cur, &lv, &mut sink_b);

    assert_eq!(sink_a.len(), sink_b.len());
    assert_eq!(a.active_len(), b.active_len());
}

#[test]
fn fill_gossip_frame_for_peer_skips_acked_cells() {
    let mut store = small_store(8, 8);
    store.intern_rule(rule_descriptor(0x11, 1)).unwrap();

    // Seed three cells (three distinct origins).
    let h1 = remote_merge_one(&mut store, 0x11, 1, 1, 5, 0);
    let h2 = remote_merge_one(&mut store, 0x11, 2, 1, 5, 0);
    let h3 = remote_merge_one(&mut store, 0x11, 3, 1, 5, 0);

    let row1 = store.get(h1).unwrap();
    let row2 = store.get(h2).unwrap();
    let _ = h3; // used only via the frame composition

    // Intern a peer and ack the first two origins through their current
    // sequences. The third remains unacked.
    let peer_slot = store.peer_frontiers_mut().intern_peer(NodeId(99)).unwrap();
    store
        .peer_frontiers_mut()
        .record_acked(peer_slot, row1.key.origin, row1.origin_sequence);
    store
        .peer_frontiers_mut()
        .record_acked(peer_slot, row2.key.origin, row2.origin_sequence);

    let mut out = Vec::with_capacity(8);
    store.fill_gossip_frame_for_peer(8, peer_slot, &mut out);
    // Only the unacked cell survives the per-peer prune.
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].index, h3.index);
}
