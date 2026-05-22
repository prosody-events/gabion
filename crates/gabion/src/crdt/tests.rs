use super::hash::mix64;
use super::*;
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::collections::{BTreeMap, BTreeSet};

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

fn quickcheck_store() -> CellStore<u32> {
    CellStore::new(
        CellStoreConfig {
            cell_capacity: 256,
            rule_dictionary_capacity: 16,
            node_dictionary_capacity: 16,
            local_dirty_capacity: 512,
            forwarded_dirty_capacity: 512,
            peer_capacity: 8,
        },
        local(),
    )
}

#[derive(Clone, Debug)]
struct RemoteRow {
    rule: u8,
    key: u8,
    bucket: u8,
    origin: u8,
    incarnation: u8,
    count: u32,
    ts: u16,
}

impl Arbitrary for RemoteRow {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            rule: u8::arbitrary(g) % 4,
            key: u8::arbitrary(g) % 4,
            bucket: u8::arbitrary(g) % 4,
            origin: (u8::arbitrary(g) % 4) + 2,
            incarnation: (u8::arbitrary(g) % 2) + 1,
            count: u32::arbitrary(g) % 10_000,
            ts: u16::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct RemoteRows(Vec<RemoteRow>);

impl Arbitrary for RemoteRows {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::arbitrary(g) % 64;
        Self((0..len).map(|_| RemoteRow::arbitrary(g)).collect())
    }
}

#[derive(Clone, Debug)]
struct LocalRow {
    rule: u8,
    key: u8,
    bucket: u8,
    ignored_origin: u8,
    ignored_incarnation: u8,
    hits: u32,
    ts: u16,
}

impl Arbitrary for LocalRow {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            rule: u8::arbitrary(g) % 4,
            key: u8::arbitrary(g) % 4,
            bucket: u8::arbitrary(g) % 4,
            ignored_origin: u8::arbitrary(g),
            ignored_incarnation: u8::arbitrary(g),
            hits: u32::arbitrary(g) % 10_000,
            ts: u16::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct LocalRows(Vec<LocalRow>);

impl Arbitrary for LocalRows {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::arbitrary(g) % 64;
        Self((0..len).map(|_| LocalRow::arbitrary(g)).collect())
    }
}

fn remote_observation(row: &RemoteRow) -> Observation<u32> {
    observation(
        0x100 + row.rule as u128,
        0x200 + row.key as u128,
        row.bucket as BucketEpoch,
        row.origin as u128,
        row.incarnation as Incarnation,
        row.count,
        row.ts as u64,
    )
}

fn apply_remote_rows(store: &mut CellStore<u32>, rows: &[RemoteRow]) -> DeltaSink<u32> {
    let mut sink = DeltaSink::with_capacity(rows.len());
    for row in rows {
        let mut obs = ObservationBatch::with_capacity(1);
        obs.push(remote_observation(row));
        store.merge_remote(&obs, &mut sink);
    }
    sink
}

fn apply_local_rows(store: &mut CellStore<u32>, rows: &[LocalRow]) -> DeltaSink<u32> {
    let mut sink = DeltaSink::with_capacity(rows.len());
    for row in rows {
        let mut obs = ObservationBatch::with_capacity(1);
        obs.push(observation(
            0x100 + row.rule as u128,
            0x200 + row.key as u128,
            row.bucket as BucketEpoch,
            row.ignored_origin as u128,
            row.ignored_incarnation as Incarnation,
            row.hits,
            row.ts as u64,
        ));
        store.ingest_local(&obs, &mut sink);
    }
    sink
}

type PortableKey = (u128, u128, BucketEpoch, u128, Incarnation);

fn remote_model(rows: &[RemoteRow]) -> BTreeMap<PortableKey, u32> {
    let mut model = BTreeMap::new();
    for row in rows {
        let key = (
            0x100 + row.rule as u128,
            0x200 + row.key as u128,
            row.bucket as BucketEpoch,
            row.origin as u128,
            row.incarnation as Incarnation,
        );
        model
            .entry(key)
            .and_modify(|stored: &mut u32| *stored = (*stored).max(row.count))
            .or_insert(row.count);
    }
    model
}

fn local_model(rows: &[LocalRow]) -> BTreeMap<PortableKey, u32> {
    let mut model = BTreeMap::new();
    for row in rows {
        let key = (
            0x100 + row.rule as u128,
            0x200 + row.key as u128,
            row.bucket as BucketEpoch,
            local().node_id.0,
            local().incarnation,
        );
        model
            .entry(key)
            .and_modify(|stored: &mut u32| *stored = stored.saturating_add(row.hits))
            .or_insert(row.hits);
    }
    model
}

fn portable_snapshot(store: &CellStore<u32>) -> BTreeMap<PortableKey, u32> {
    let mut rows = BTreeMap::new();
    for handle in store.active_handles() {
        let row = store.get(handle).unwrap();
        let rule = store.rule_dictionary().descriptor(row.key.rule).unwrap();
        let origin = store.node_dictionary().descriptor(row.key.origin).unwrap();
        rows.insert(
            (
                rule.fingerprint,
                row.key.key_hash.0,
                row.key.bucket,
                origin.node_id.0,
                origin.incarnation,
            ),
            row.count,
        );
    }
    rows
}

fn assert_structural_invariants(store: &CellStore<u32>) -> bool {
    let mut active = 0_u32;
    let mut rules = BTreeMap::<RuleSlot, u32>::new();
    let mut nodes = BTreeMap::<NodeSlot, u32>::new();
    let mut identities = BTreeSet::<(RuleSlot, u128, BucketEpoch, NodeSlot, Incarnation)>::new();

    for handle in store.active_handles() {
        active += 1;
        let Some(row) = store.get(handle) else {
            return false;
        };
        if store.resolve(handle) != Some(handle.index) {
            return false;
        }
        if store.find(row.key) != Some(handle) {
            return false;
        }
        if !identities.insert((
            row.key.rule,
            row.key.key_hash.0,
            row.key.bucket,
            row.key.origin,
            row.key.incarnation,
        )) {
            return false;
        }
        *rules.entry(row.key.rule).or_default() += 1;
        *nodes.entry(row.key.origin).or_default() += 1;
    }

    *nodes.entry(store.local_node_slot()).or_default() += 1;

    if active != store.active_len() || store.index().len() != active {
        return false;
    }
    for slot in 0..store.rule_dictionary().capacity() {
        let expected = rules.get(&slot).copied().unwrap_or(0);
        if store.rule_dictionary().refcount(slot) != expected {
            return false;
        }
        if (store.rule_dictionary().descriptor(slot).is_some()) != (expected > 0) {
            return false;
        }
    }
    for slot in 0..store.node_dictionary().capacity() {
        let expected = nodes.get(&slot).copied().unwrap_or(0);
        if store.node_dictionary().refcount(slot) != expected {
            return false;
        }
        if (store.node_dictionary().descriptor(slot).is_some()) != (expected > 0) {
            return false;
        }
    }
    true
}

#[quickcheck]
fn quickcheck_remote_merge_is_gcounter_crdt(rows: RemoteRows) -> bool {
    let mut forward = quickcheck_store();
    apply_remote_rows(&mut forward, &rows.0);

    let mut reverse = quickcheck_store();
    let mut reversed = rows.0.clone();
    reversed.reverse();
    apply_remote_rows(&mut reverse, &reversed);

    let mut duplicated = quickcheck_store();
    let mut dup_rows = rows.0.clone();
    dup_rows.extend(rows.0.iter().cloned());
    apply_remote_rows(&mut duplicated, &dup_rows);

    let model = remote_model(&rows.0);
    portable_snapshot(&forward) == model
        && portable_snapshot(&reverse) == model
        && portable_snapshot(&duplicated) == model
        && assert_structural_invariants(&forward)
        && assert_structural_invariants(&reverse)
        && assert_structural_invariants(&duplicated)
}

#[quickcheck]
fn quickcheck_local_ingest_accumulates_saturating_and_ignores_batch_origin(
    rows: LocalRows,
) -> bool {
    let mut store = quickcheck_store();
    let sink = apply_local_rows(&mut store, &rows.0);
    let model = local_model(&rows.0);

    let deltas_are_exact = (0..sink.len()).all(|i| {
        sink.current[i] >= sink.previous[i]
            && sink.deltas[i] == sink.current[i].saturating_sub(sink.previous[i])
            && sink.applies_locally[i] == 0
    });

    portable_snapshot(&store) == model
        && deltas_are_exact
        && assert_structural_invariants(&store)
        && store.active_handles().all(|h| {
            let row = store.get(h).unwrap();
            row.key.origin == store.local_node_slot()
                && row.key.incarnation == store.local_identity().incarnation
        })
}

#[quickcheck]
fn quickcheck_delta_sink_rows_are_exact_remote_raises(rows: RemoteRows) -> bool {
    let mut store = quickcheck_store();
    let mut model = BTreeMap::<PortableKey, u32>::new();
    let mut sink = DeltaSink::with_capacity(rows.0.len());

    for row in &rows.0 {
        let key = (
            0x100 + row.rule as u128,
            0x200 + row.key as u128,
            row.bucket as BucketEpoch,
            row.origin as u128,
            row.incarnation as Incarnation,
        );
        let previous = model.get(&key).copied().unwrap_or(0);
        let mut obs = ObservationBatch::with_capacity(1);
        obs.push(remote_observation(row));
        let before = sink.len();
        store.merge_remote(&obs, &mut sink);
        if row.count > previous || !model.contains_key(&key) {
            let after = sink.len();
            if after != before + 1 {
                return false;
            }
            let i = after - 1;
            if sink.previous[i] != previous
                || sink.current[i] != row.count
                || sink.deltas[i] != row.count.saturating_sub(previous)
                || sink.keys[i].rule_fingerprint != key.0
                || sink.keys[i].key_hash.0 != key.1
                || sink.keys[i].bucket != key.2
            {
                return false;
            }
            model.insert(key, row.count);
        } else if sink.len() != before {
            return false;
        }
    }

    portable_snapshot(&store) == model && assert_structural_invariants(&store)
}

#[quickcheck]
fn quickcheck_expire_frees_exactly_aged_out_cells(rows: RemoteRows, current_seed: u8) -> bool {
    let mut store = quickcheck_store();
    apply_remote_rows(&mut store, &rows.0);
    let before: Vec<CellRow<u32>> = store
        .active_handles()
        .map(|h| store.get(h).unwrap())
        .collect();

    let dict_cap = store.rule_dictionary().capacity() as usize;
    let mut current = vec![0_u32; dict_cap];
    let mut live = vec![0_u32; dict_cap];
    for slot in 0..dict_cap {
        current[slot] = (current_seed as u32 % 4) + 2;
        live[slot] = current_seed as u32 % 2;
    }

    let expected_expired: BTreeSet<u32> = before
        .iter()
        .filter(|row| {
            (row.key.bucket as u64) + (live[row.key.rule as usize] as u64)
                < current[row.key.rule as usize] as u64
        })
        .map(|row| row.handle.index)
        .collect();

    let mut exp = ExpirationSink::<u32>::with_capacity(before.len());
    store.expire(&current, &live, &mut exp);
    let expired: BTreeSet<u32> = exp.handles.iter().map(|h| h.index).collect();
    if expired != expected_expired || exp.len() != expected_expired.len() {
        return false;
    }
    for handle in &exp.handles {
        if store.get(*handle).is_some() {
            return false;
        }
    }
    for row in before {
        let should_survive = !expected_expired.contains(&row.handle.index);
        if store.get(row.handle).is_some() != should_survive {
            return false;
        }
    }
    assert_structural_invariants(&store)
}

#[quickcheck]
fn quickcheck_gossip_frames_are_bounded_current_unique_and_repair_covers_all(
    rows: RemoteRows,
    max_seed: u8,
) -> TestResult {
    let mut store = quickcheck_store();
    apply_remote_rows(&mut store, &rows.0);
    if store.is_empty() {
        return TestResult::discard();
    }

    let max_cells = (max_seed as usize % 32) + 1;
    let mut out = Vec::with_capacity(max_cells);
    let emitted = store.fill_gossip_frame(max_cells, &mut out);
    if emitted != out.len() || out.len() > max_cells {
        return TestResult::failed();
    }
    let mut unique = BTreeSet::new();
    for handle in &out {
        if store.get(*handle).is_none() || !unique.insert(handle.index) {
            return TestResult::failed();
        }
    }

    store.clear_dirty();
    let expected: BTreeSet<u32> = store.active_handles().map(|h| h.index).collect();
    let mut seen = BTreeSet::new();
    let mut one = Vec::with_capacity(1);
    for _ in 0..(store.capacity() * 2) {
        store.fill_gossip_frame(1, &mut one);
        if let Some(handle) = one.first() {
            if store.get(*handle).is_none() {
                return TestResult::failed();
            }
            seen.insert(handle.index);
        }
        if seen == expected {
            return TestResult::passed();
        }
    }
    TestResult::from_bool(seen == expected)
}

#[quickcheck]
fn quickcheck_peer_frontier_prunes_exactly_acked_origins(rows: RemoteRows, ack_seed: u8) -> bool {
    let mut store = quickcheck_store();
    apply_remote_rows(&mut store, &rows.0);
    store.clear_dirty();
    let peer_slot = store
        .peer_frontiers_mut()
        .intern_peer(NodeId(0x999))
        .unwrap();

    let active: Vec<CellRow<u32>> = store
        .active_handles()
        .map(|h| store.get(h).unwrap())
        .collect();
    for row in &active {
        if ((row.key.origin as u8).wrapping_add(ack_seed) & 1) == 0 {
            store
                .peer_frontiers_mut()
                .record_acked(peer_slot, row.key.origin, row.origin_sequence);
        }
    }

    let expected: BTreeSet<u32> = active
        .iter()
        .filter(|row| {
            row.origin_sequence > store.peer_frontiers().last_acked(peer_slot, row.key.origin)
        })
        .map(|row| row.handle.index)
        .collect();

    let mut out = Vec::with_capacity(store.capacity() as usize);
    store.fill_gossip_frame_for_peer(store.capacity() as usize, peer_slot, &mut out);
    let got: BTreeSet<u32> = out.iter().map(|h| h.index).collect();

    got == expected
        && out.len() == got.len()
        && out.iter().all(|h| store.get(*h).is_some())
        && assert_structural_invariants(&store)
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
fn unknown_rule_cells_expire_at_default_window() {
    let mut store = small_store(4, 8);
    let handle = remote_merge_one(&mut store, 0x44, 5, 1, 10, 0);
    assert!(store.get(handle).is_some());

    let mut exp = ExpirationSink::<u32>::with_capacity(1);
    store.expire_at(61_000, &mut exp);

    assert!(store.get(handle).is_none());
    assert_eq!(exp.len(), 1);
    assert_eq!(exp.applies_locally[0], 0);
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
fn expire_at_uses_ceil_for_partial_bucket_windows() {
    let mut store = small_store(8, 8);
    store
        .intern_rule(RuleDescriptor {
            fingerprint: 0x11,
            window_millis: 101,
            bucket_millis: 50,
            limit: 10,
            flags: 0,
            local_rule_id: 1,
        })
        .unwrap();

    let mut obs = ObservationBatch::with_capacity(1);
    let mut deltas = DeltaSink::with_capacity(1);
    obs.push(observation(0x11, 0xabc, 0, 7, 1, 5, 0));
    store.merge_remote(&obs, &mut deltas);

    let mut expirations = ExpirationSink::<u32>::with_capacity(1);
    store.expire_at(150, &mut expirations);

    // now=150ms -> current bucket 3. A 101ms window spans ceil(101/50)=3
    // buckets, so bucket 0 is still live. Floor division would use 2 live
    // buckets and expire this cell.
    assert_eq!(expirations.len(), 0);
    assert_eq!(store.active_len(), 1);
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

// --- Coverage extensions (Gaps 1-6) -----------------------------------

/// Like `quickcheck_store()` but pre-interns rules for fingerprints `0x100`
/// and `0x101` with `local_rule_id != u32::MAX`, then `inc_ref`s each so the
/// pin survives across expirations. Fingerprints `0x102` and `0x103` remain
/// auto-interned on first use with `applies_locally = false`.
fn quickcheck_store_with_pinned_rules() -> CellStore<u32> {
    let mut store = quickcheck_store();
    let slot_a = store
        .intern_rule(RuleDescriptor {
            fingerprint: 0x100,
            window_millis: 1000,
            bucket_millis: 1000,
            limit: 1000,
            flags: 0,
            local_rule_id: 1,
        })
        .unwrap();
    let slot_b = store
        .intern_rule(RuleDescriptor {
            fingerprint: 0x101,
            window_millis: 1000,
            bucket_millis: 1000,
            limit: 1000,
            flags: 0,
            local_rule_id: 1,
        })
        .unwrap();
    // Pin so the descriptor (and thus `applies_locally`) survives even when
    // every cell on that rule expires.
    store.rule_dictionary.inc_ref(slot_a);
    store.rule_dictionary.inc_ref(slot_b);
    store
}

/// Variant of `assert_structural_invariants` that allows one extra refcount
/// on each slot in `pinned_rule_slots` — those are pinned via direct
/// `inc_ref`, mirroring how the local node slot is pinned.
fn assert_invariants_with_pinned_rules(
    store: &CellStore<u32>,
    pinned_rule_slots: &[RuleSlot],
) -> bool {
    let mut active = 0_u32;
    let mut rules = BTreeMap::<RuleSlot, u32>::new();
    let mut nodes = BTreeMap::<NodeSlot, u32>::new();
    let mut identities = BTreeSet::<(RuleSlot, u128, BucketEpoch, NodeSlot, Incarnation)>::new();

    for handle in store.active_handles() {
        active += 1;
        let Some(row) = store.get(handle) else {
            return false;
        };
        if store.resolve(handle) != Some(handle.index) {
            return false;
        }
        if store.find(row.key) != Some(handle) {
            return false;
        }
        if !identities.insert((
            row.key.rule,
            row.key.key_hash.0,
            row.key.bucket,
            row.key.origin,
            row.key.incarnation,
        )) {
            return false;
        }
        *rules.entry(row.key.rule).or_default() += 1;
        *nodes.entry(row.key.origin).or_default() += 1;
    }
    for slot in pinned_rule_slots {
        *rules.entry(*slot).or_default() += 1;
    }
    *nodes.entry(store.local_node_slot()).or_default() += 1;

    if active != store.active_len() || store.index().len() != active {
        return false;
    }
    for slot in 0..store.rule_dictionary().capacity() {
        let expected = rules.get(&slot).copied().unwrap_or(0);
        if store.rule_dictionary().refcount(slot) != expected {
            return false;
        }
        if (store.rule_dictionary().descriptor(slot).is_some()) != (expected > 0) {
            return false;
        }
    }
    for slot in 0..store.node_dictionary().capacity() {
        let expected = nodes.get(&slot).copied().unwrap_or(0);
        if store.node_dictionary().refcount(slot) != expected {
            return false;
        }
        if (store.node_dictionary().descriptor(slot).is_some()) != (expected > 0) {
            return false;
        }
    }
    true
}

/// Predict the exact ordered handle sequence `fill_gossip_frame` will emit
/// for the current store state, mirroring the lane priority (local dirty →
/// forwarded dirty → repair) and the dedup / staleness filters.
fn predict_gossip_order(store: &CellStore<u32>, max_cells: usize) -> Vec<CellHandle> {
    let mut out: Vec<CellHandle> = Vec::with_capacity(max_cells);
    if max_cells == 0 {
        return out;
    }
    let cap = store.capacity();
    let mut marks: BTreeSet<u32> = BTreeSet::new();

    let entry_current = |entry: DirtyEntry| -> bool {
        let i = entry.handle.index as usize;
        if (entry.handle.index) >= cap {
            return false;
        }
        if store.generations[i] != entry.handle.generation {
            return false;
        }
        if store.origin_sequences[i] != entry.origin_sequence {
            return false;
        }
        true
    };

    // Lane 1: local dirty.
    for entry in store.local_dirty().iter() {
        if out.len() >= max_cells {
            break;
        }
        if !entry_current(entry) {
            continue;
        }
        if !marks.insert(entry.handle.index) {
            continue;
        }
        out.push(entry.handle);
    }
    if out.len() >= max_cells {
        return out;
    }

    // Lane 2: forwarded dirty.
    for entry in store.forwarded_dirty().iter() {
        if out.len() >= max_cells {
            break;
        }
        if !entry_current(entry) {
            continue;
        }
        if !marks.insert(entry.handle.index) {
            continue;
        }
        out.push(entry.handle);
    }
    if out.len() >= max_cells {
        return out;
    }

    // Lane 3: rotating repair slice.
    let mut visited = 0_u32;
    while visited < cap && out.len() < max_cells {
        let slot = (store.repair_cursor() + visited) % cap;
        visited += 1;
        if (store.generations[slot as usize] & 1) != 1 {
            continue;
        }
        if !marks.insert(slot) {
            continue;
        }
        out.push(CellHandle {
            index: slot,
            generation: store.generations[slot as usize],
        });
    }
    out
}

// --- Shared scaffolding for mixed-ops properties (Gaps 1, 2, 5) ----------

#[derive(Clone, Debug)]
struct ExpireTick {
    current_by_fp: [u8; 4],
    live_by_fp: [u8; 4],
}

impl Arbitrary for ExpireTick {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut current = [0_u8; 4];
        let mut live = [0_u8; 4];
        for i in 0..4 {
            current[i] = u8::arbitrary(g) % 8;
            live[i] = u8::arbitrary(g) % 8;
        }
        Self {
            current_by_fp: current,
            live_by_fp: live,
        }
    }
}

#[derive(Clone, Debug)]
enum MixedOp {
    Remote(RemoteRow),
    Local(LocalRow),
    Expire(ExpireTick),
}

impl Arbitrary for MixedOp {
    fn arbitrary(g: &mut Gen) -> Self {
        // ~40% Remote, ~40% Local, ~20% Expire.
        let pick = u8::arbitrary(g) % 10;
        if pick < 4 {
            MixedOp::Remote(RemoteRow::arbitrary(g))
        } else if pick < 8 {
            MixedOp::Local(LocalRow::arbitrary(g))
        } else {
            MixedOp::Expire(ExpireTick::arbitrary(g))
        }
    }
}

#[derive(Clone, Debug)]
struct MixedOps(Vec<MixedOp>);

impl Arbitrary for MixedOps {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::arbitrary(g) % 32;
        Self((0..len).map(|_| MixedOp::arbitrary(g)).collect())
    }
}

fn apply_mixed_op_with_sinks(
    store: &mut CellStore<u32>,
    op: &MixedOp,
    sink: &mut DeltaSink<u32>,
    exp_sink: &mut ExpirationSink<u32>,
) {
    match op {
        MixedOp::Remote(row) => {
            let mut obs = ObservationBatch::with_capacity(1);
            obs.push(remote_observation(row));
            store.merge_remote(&obs, sink);
        }
        MixedOp::Local(row) => {
            let mut obs = ObservationBatch::with_capacity(1);
            obs.push(observation(
                0x100 + row.rule as u128,
                0x200 + row.key as u128,
                row.bucket as BucketEpoch,
                row.ignored_origin as u128,
                row.ignored_incarnation as Incarnation,
                row.hits,
                row.ts as u64,
            ));
            store.ingest_local(&obs, sink);
        }
        MixedOp::Expire(tick) => {
            // Build slot-indexed arrays from the fingerprint-indexed model.
            let dict_cap = store.rule_dictionary().capacity() as usize;
            let mut cur = vec![0_u32; dict_cap];
            let mut live = vec![0_u32; dict_cap];
            for fp_idx in 0..4 {
                let fp = 0x100 + fp_idx as u128;
                if let Some(slot) = store.find_rule(fp) {
                    cur[slot as usize] = tick.current_by_fp[fp_idx] as u32;
                    live[slot as usize] = tick.live_by_fp[fp_idx] as u32;
                }
            }
            store.expire(&cur, &live, exp_sink);
        }
    }
}

fn apply_mixed_op_to_model(model: &mut BTreeMap<PortableKey, u32>, op: &MixedOp) {
    match op {
        MixedOp::Remote(row) => {
            let key = (
                0x100 + row.rule as u128,
                0x200 + row.key as u128,
                row.bucket as BucketEpoch,
                row.origin as u128,
                row.incarnation as Incarnation,
            );
            model
                .entry(key)
                .and_modify(|stored: &mut u32| *stored = (*stored).max(row.count))
                .or_insert(row.count);
        }
        MixedOp::Local(row) => {
            let key = (
                0x100 + row.rule as u128,
                0x200 + row.key as u128,
                row.bucket as BucketEpoch,
                local().node_id.0,
                local().incarnation,
            );
            model
                .entry(key)
                .and_modify(|stored: &mut u32| *stored = stored.saturating_add(row.hits))
                .or_insert(row.hits);
        }
        MixedOp::Expire(tick) => {
            model.retain(|key, _| {
                let fp_idx = (key.0 - 0x100) as usize;
                if fp_idx >= 4 {
                    return true;
                }
                let bucket = key.2 as u64;
                let live = tick.live_by_fp[fp_idx] as u64;
                let current = tick.current_by_fp[fp_idx] as u64;
                bucket + live >= current
            });
        }
    }
}

// Gap 1 + Gap 2: mixed local+remote+expire ops, with `applies_locally`
// driven by the pinned-fingerprint set so the flag is no longer uniformly
// false across the QC suite.
#[quickcheck]
fn quickcheck_mixed_ops_match_model(ops: MixedOps) -> bool {
    let mut store = quickcheck_store_with_pinned_rules();
    let pinned_rule_slots = [
        store.find_rule(0x100).unwrap(),
        store.find_rule(0x101).unwrap(),
    ];
    let pinned_fps: BTreeSet<u128> = [0x100_u128, 0x101_u128].into_iter().collect();
    let mut model: BTreeMap<PortableKey, u32> = BTreeMap::new();

    for op in &ops.0 {
        let mut sink = DeltaSink::with_capacity(1);
        let mut exp_sink = ExpirationSink::<u32>::with_capacity(8);
        apply_mixed_op_with_sinks(&mut store, op, &mut sink, &mut exp_sink);
        apply_mixed_op_to_model(&mut model, op);

        for i in 0..sink.len() {
            let expected = pinned_fps.contains(&sink.keys[i].rule_fingerprint);
            if (sink.applies_locally[i] != 0) != expected {
                return false;
            }
        }
        for i in 0..exp_sink.len() {
            let expected = pinned_fps.contains(&exp_sink.keys[i].rule_fingerprint);
            if (exp_sink.applies_locally[i] != 0) != expected {
                return false;
            }
        }

        if portable_snapshot(&store) != model {
            return false;
        }
        if !assert_invariants_with_pinned_rules(&store, &pinned_rule_slots) {
            return false;
        }
    }
    true
}

// --- Gap 3: saturation and dictionary-full paths -------------------------

#[derive(Clone, Debug)]
struct TightRemoteRow {
    rule: u8,
    key: u8,
    bucket: u8,
    origin: u8,
    incarnation: u8,
    count: u32,
    ts: u16,
}

impl Arbitrary for TightRemoteRow {
    fn arbitrary(g: &mut Gen) -> Self {
        Self {
            rule: u8::arbitrary(g) % 4,
            key: u8::arbitrary(g) % 4,
            bucket: u8::arbitrary(g) % 4,
            origin: u8::arbitrary(g) % 6,
            incarnation: (u8::arbitrary(g) % 2) + 1,
            count: u32::arbitrary(g) % 10_000,
            ts: u16::arbitrary(g),
        }
    }
}

#[derive(Clone, Debug)]
struct TightRemoteRows(Vec<TightRemoteRow>);

impl Arbitrary for TightRemoteRows {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = usize::arbitrary(g) % 64;
        Self((0..len).map(|_| TightRemoteRow::arbitrary(g)).collect())
    }
}

fn quickcheck_store_tight() -> CellStore<u32> {
    CellStore::new(
        CellStoreConfig {
            cell_capacity: 4,
            rule_dictionary_capacity: 2,
            node_dictionary_capacity: 3,
            local_dirty_capacity: 16,
            forwarded_dirty_capacity: 16,
            peer_capacity: 4,
        },
        local(),
    )
}

#[quickcheck]
fn quickcheck_capacity_rejections_are_counted_and_invariants_hold(
    rows: TightRemoteRows,
) -> TestResult {
    let mut store = quickcheck_store_tight();
    // Model mirrors translate_identity → upsert: rule intern first, then
    // node intern, then cell alloc. A failing intern is permanent for the
    // row (skip) but the slot it already took stays consumed.
    let mut interned_rules: BTreeSet<u128> = BTreeSet::new();
    let mut interned_nodes: BTreeSet<(u128, Incarnation)> = BTreeSet::new();
    interned_nodes.insert((local().node_id.0, local().incarnation));
    let mut cells: BTreeMap<PortableKey, u32> = BTreeMap::new();
    let mut model_rule_full = 0_u64;
    let mut model_node_full = 0_u64;
    let mut model_cell_full = 0_u64;

    for row in &rows.0 {
        let fp = 0x100 + row.rule as u128;
        let origin = row.origin as u128;
        let inc = row.incarnation as Incarnation;
        let mut obs = ObservationBatch::with_capacity(1);
        obs.push(observation(
            fp,
            0x200 + row.key as u128,
            row.bucket as BucketEpoch,
            origin,
            inc,
            row.count,
            row.ts as u64,
        ));
        let mut sink = DeltaSink::with_capacity(1);
        store.merge_remote(&obs, &mut sink);

        if !interned_rules.contains(&fp) {
            if interned_rules.len() >= 2 {
                model_rule_full += 1;
                continue;
            }
            interned_rules.insert(fp);
        }
        if !interned_nodes.contains(&(origin, inc)) {
            if interned_nodes.len() >= 3 {
                model_node_full += 1;
                continue;
            }
            interned_nodes.insert((origin, inc));
        }
        let key = (
            fp,
            0x200 + row.key as u128,
            row.bucket as BucketEpoch,
            origin,
            inc,
        );
        if let Some(stored) = cells.get_mut(&key) {
            *stored = (*stored).max(row.count);
        } else if cells.len() >= 4 {
            model_cell_full += 1;
        } else {
            cells.insert(key, row.count);
        }
    }

    let stats = store.stats();
    let any_rejected = stats.rule_dictionary_full_rejects > 0
        || stats.node_dictionary_full_rejects > 0
        || stats.cell_store_full_rejects > 0;
    if !any_rejected {
        return TestResult::discard();
    }

    if stats.rule_dictionary_full_rejects != model_rule_full
        || stats.node_dictionary_full_rejects != model_node_full
        || stats.cell_store_full_rejects != model_cell_full
    {
        return TestResult::failed();
    }
    if portable_snapshot(&store) != cells {
        return TestResult::failed();
    }
    if !assert_structural_invariants(&store) {
        return TestResult::failed();
    }
    TestResult::passed()
}

#[derive(Clone, Debug)]
struct U16HitRow {
    hits: u16,
}

impl Arbitrary for U16HitRow {
    fn arbitrary(g: &mut Gen) -> Self {
        // Draw near u16::MAX so a small number of accumulations saturate.
        let base = (u16::MAX - 16) as u32;
        let jitter = u32::arbitrary(g) % 32;
        let hits = (base + jitter).min(u16::MAX as u32) as u16;
        Self { hits }
    }
}

#[derive(Clone, Debug)]
struct U16HitRows(Vec<U16HitRow>);

impl Arbitrary for U16HitRows {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = (usize::arbitrary(g) % 16) + 1;
        Self((0..len).map(|_| U16HitRow::arbitrary(g)).collect())
    }
}

#[quickcheck]
fn quickcheck_count_saturates_without_overflow(rows: U16HitRows) -> bool {
    let mut store = CellStore::<u16>::new(
        CellStoreConfig {
            cell_capacity: 4,
            rule_dictionary_capacity: 4,
            node_dictionary_capacity: 4,
            local_dirty_capacity: 32,
            forwarded_dirty_capacity: 32,
            peer_capacity: 4,
        },
        local(),
    );
    let mut model_sum: u64 = 0;
    for row in &rows.0 {
        let mut obs = ObservationBatch::<u16>::with_capacity(1);
        obs.push(Observation {
            rule_fingerprint: 0x100,
            key_hash: KeyHash(0xabc),
            bucket: 0,
            origin: NodeId(1),
            incarnation: 1,
            count: row.hits,
            last_update_millis: 0,
        });
        let mut sink = DeltaSink::<u16>::with_capacity(1);
        store.ingest_local(&obs, &mut sink);
        model_sum = model_sum.saturating_add(row.hits as u64);
    }
    let stored = store
        .active_handles()
        .next()
        .and_then(|h| store.get(h).map(|r| r.count))
        .unwrap_or(0);
    let expected = model_sum.min(u16::MAX as u64) as u16;
    stored == expected
}

// --- Gap 4: peer-aware frame across all three lanes ----------------------

#[quickcheck]
fn quickcheck_gossip_frame_for_peer_across_all_lanes(rows: RemoteRows, ack_seed: u8) -> bool {
    let mut store = quickcheck_store();
    apply_remote_rows(&mut store, &rows.0);
    // No `clear_dirty` here — exercise the union across local, forwarded,
    // and repair lanes simultaneously.
    let peer_slot = store
        .peer_frontiers_mut()
        .intern_peer(NodeId(0x999))
        .unwrap();

    let active: Vec<CellRow<u32>> = store
        .active_handles()
        .map(|h| store.get(h).unwrap())
        .collect();
    for row in &active {
        if ((row.key.origin as u8).wrapping_add(ack_seed) & 1) == 0 {
            store
                .peer_frontiers_mut()
                .record_acked(peer_slot, row.key.origin, row.origin_sequence);
        }
    }

    let expected: BTreeSet<u32> = active
        .iter()
        .filter(|row| {
            row.origin_sequence > store.peer_frontiers().last_acked(peer_slot, row.key.origin)
        })
        .map(|row| row.handle.index)
        .collect();

    let mut out = Vec::with_capacity(store.capacity() as usize);
    store.fill_gossip_frame_for_peer(store.capacity() as usize, peer_slot, &mut out);
    let got: BTreeSet<u32> = out.iter().map(|h| h.index).collect();

    got == expected
        && out.len() == got.len()
        && out.iter().all(|h| store.get(*h).is_some())
        && assert_structural_invariants(&store)
}

// --- Gap 5: lane priority as ordered prediction --------------------------

#[quickcheck]
fn quickcheck_gossip_frame_order_matches_lane_model(ops: MixedOps, max_seed: u8) -> TestResult {
    let mut store = quickcheck_store_with_pinned_rules();
    for op in &ops.0 {
        let mut sink = DeltaSink::with_capacity(1);
        let mut exp_sink = ExpirationSink::<u32>::with_capacity(8);
        apply_mixed_op_with_sinks(&mut store, op, &mut sink, &mut exp_sink);
    }
    if store.is_empty() {
        return TestResult::discard();
    }
    let max_cells = (max_seed as usize % 32) + 1;
    let predicted = predict_gossip_order(&store, max_cells);
    let mut got = Vec::with_capacity(max_cells);
    store.fill_gossip_frame(max_cells, &mut got);
    TestResult::from_bool(predicted == got)
}

// --- Gap 6: expire_at vs expire for varied rule windows ------------------

#[derive(Clone, Debug)]
struct RuleParams {
    window: u32,
    bucket: u32,
}

impl Arbitrary for RuleParams {
    fn arbitrary(g: &mut Gen) -> Self {
        let window = match u8::arbitrary(g) % 3 {
            0 => 50_u32,
            1 => 100_u32,
            _ => 200_u32,
        };
        let bucket = match u8::arbitrary(g) % 3 {
            0 => 25_u32,
            1 => 50_u32,
            _ => 100_u32,
        };
        Self { window, bucket }
    }
}

#[quickcheck]
fn quickcheck_expire_at_matches_expire_for_varied_rules(
    rule_a: RuleParams,
    rule_b: RuleParams,
    rows: RemoteRows,
    now_seed: u32,
) -> bool {
    let mut base = quickcheck_store();
    base.intern_rule(RuleDescriptor {
        fingerprint: 0x100,
        window_millis: rule_a.window,
        bucket_millis: rule_a.bucket,
        limit: 1000,
        flags: 0,
        local_rule_id: 1,
    })
    .unwrap();
    base.intern_rule(RuleDescriptor {
        fingerprint: 0x101,
        window_millis: rule_b.window,
        bucket_millis: rule_b.bucket,
        limit: 1000,
        flags: 0,
        local_rule_id: 1,
    })
    .unwrap();

    // Restrict to the two pre-interned rules; widen bucket spread so cells
    // land across multiple live windows.
    let varied: Vec<RemoteRow> = rows
        .0
        .iter()
        .map(|r| RemoteRow {
            rule: r.rule % 2,
            key: r.key,
            bucket: r.bucket % 8,
            origin: r.origin,
            incarnation: r.incarnation,
            count: r.count,
            ts: r.ts,
        })
        .collect();
    apply_remote_rows(&mut base, &varied);

    let now_millis: u64 = (now_seed as u64) % 4000;

    let mut store_a = base.clone();
    let mut sink_a = ExpirationSink::<u32>::with_capacity(base.capacity() as usize);
    store_a.expire_at(now_millis, &mut sink_a);

    let mut store_b = base.clone();
    let dict_cap = store_b.rule_dictionary().capacity() as usize;
    let mut cur = vec![0_u32; dict_cap];
    let mut live = vec![0_u32; dict_cap];
    for slot in 0..dict_cap {
        if let Some(d) = store_b.rule_dictionary().descriptor(slot as RuleSlot) {
            if d.bucket_millis > 0 {
                cur[slot] = (now_millis / d.bucket_millis as u64) as BucketEpoch;
                live[slot] = (d.window_millis / d.bucket_millis).max(1);
            }
        }
    }
    let mut sink_b = ExpirationSink::<u32>::with_capacity(base.capacity() as usize);
    store_b.expire(&cur, &live, &mut sink_b);

    let handles_a: BTreeSet<u32> = sink_a.handles.iter().map(|h| h.index).collect();
    let handles_b: BTreeSet<u32> = sink_b.handles.iter().map(|h| h.index).collect();

    handles_a == handles_b
        && store_a.active_len() == store_b.active_len()
        && portable_snapshot(&store_a) == portable_snapshot(&store_b)
}
