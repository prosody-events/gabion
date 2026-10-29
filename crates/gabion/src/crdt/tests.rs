use std::time::Duration;

use super::*;
use crate::aggregate::Aggregate;
use crate::gossip::AggregateStore;
use crate::mock::with_logs;
use crate::rules::{EnforcementMode, RuleDef};
use crate::settings::mock::{rules, storage};
use quickcheck::{Arbitrary, Gen, TestResult};
use quickcheck_macros::quickcheck;
use std::collections::{BTreeMap, BTreeSet};

fn local() -> NodeIdentity {
    NodeIdentity::new(NodeId(1), 1)
}

fn descriptor_for(fingerprint: u128, local_id: u32) -> RuleDescriptor {
    RuleDescriptor {
        fingerprint,
        window_millis: 1000,
        bucket_millis: 1000,
        limit: 1000,
        flags: 0,
        local_rule_id: local_id,
    }
}

fn store() -> CellStore {
    CellStore::new(storage(16, 8), local(), &rules(8))
}

fn small_store(cell_cap: u32) -> CellStore {
    CellStore::new(storage(cell_cap, 8), local(), &rules(8))
}

/// Build an `Observation` row with positional args. Test convenience for the
/// many fixture rows in this file — production code constructs `Observation`
/// directly with field names.
fn observation(
    rule_fp: u128,
    key: u128,
    bucket: BucketEpoch,
    origin: u128,
    inc: Incarnation,
    count: u32,
    ts: u64,
) -> Observation {
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
    store: &mut CellStore,
    rule_fp: u128,
    origin: u128,
    inc: Incarnation,
    count: u32,
    ts: u64,
) -> CellHandle {
    let mut obs = ObservationBatch::with_capacity(1);
    let mut sink = DeltaSink::with_capacity(1);
    obs.push(observation(rule_fp, 0xabc, 0, origin, inc, count, ts));
    store.merge_remote(&obs, &mut sink, RelayMark::Clear);
    // A no-op merge (count <= stored) leaves the sink empty; recover the
    // handle via key lookup instead.
    if let Some(handle) = sink.handles.first().copied() {
        return handle;
    }
    let rule_slot = store.rule_dictionary.find(rule_fp).expect("rule interned");
    let node_slot = store
        .node_dictionary
        .find(NodeId(origin), inc)
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
    store: &mut CellStore,
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
    let rule_slot = store.rule_dictionary.find(rule_fp).expect("rule interned");
    let node_slot = store
        .node_dictionary
        .find(local().node_id, local().incarnation)
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

fn quickcheck_store() -> CellStore {
    CellStore::new(storage(256, 16), local(), &rules(16))
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
            origin: u8::arbitrary(g) % 5 + 1,
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

fn remote_observation(row: &RemoteRow) -> Observation {
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

fn apply_remote_rows(store: &mut CellStore, rows: &[RemoteRow]) -> DeltaSink {
    let mut sink = DeltaSink::with_capacity(rows.len());
    for row in rows {
        let mut obs = ObservationBatch::with_capacity(1);
        obs.push(remote_observation(row));
        store.merge_remote(&obs, &mut sink, RelayMark::Clear);
    }
    sink
}

fn apply_local_rows(store: &mut CellStore, rows: &[LocalRow]) -> DeltaSink {
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

/// A live cell's identity in portable values.
fn portable_key(store: &CellStore, handle: CellHandle) -> PortableKey {
    let row = store.get(handle).unwrap();
    let rule = store.rule_dictionary().descriptor(row.key.rule).unwrap();
    let origin = store.node_dictionary().descriptor(row.key.origin).unwrap();
    (
        rule.fingerprint,
        row.key.key_hash.0,
        row.key.bucket,
        origin.node_id.0,
        origin.incarnation,
    )
}

fn portable_snapshot(store: &CellStore) -> BTreeMap<PortableKey, u32> {
    let mut rows = BTreeMap::new();
    for handle in store.active_handles(0) {
        rows.insert(
            portable_key(store, handle),
            store.get(handle).unwrap().count,
        );
    }
    rows
}

fn assert_structural_invariants(store: &CellStore) -> bool {
    let mut active = 0_u32;
    let mut rules = BTreeMap::<RuleSlot, u32>::new();
    let mut nodes = BTreeMap::<NodeSlot, u32>::new();
    let mut identities = BTreeSet::<(RuleSlot, u128, BucketEpoch, NodeSlot, Incarnation)>::new();

    for handle in store.active_handles(0) {
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
        && store.active_handles(0).all(|h| {
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
        store.merge_remote(&obs, &mut sink, RelayMark::Clear);
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
fn quickcheck_expire_frees_exactly_aged_out_cells(
    rows: RemoteRows,
    current_seed: u8,
    cap_seed: u8,
) -> bool {
    let mut store = quickcheck_store();
    apply_remote_rows(&mut store, &rows.0);
    let before: Vec<CellRow> = store
        .active_handles(0)
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

    let cap = cap_seed as usize % 8 + 1;
    let mut exp = ExpirationSink::with_capacity(cap);
    let mut expired = BTreeSet::new();
    let (mut emitted, mut calls) = (0, 0);
    loop {
        let expiry = store.expire(&current, &live, &mut exp);
        calls += 1;
        emitted += exp.len();
        expired.extend(exp.handles.iter().map(|h| h.index));
        match expiry {
            Expiry::More if exp.len() != cap || store.expire_cursor == 0 => return false,
            Expiry::More => exp.clear(),
            Expiry::Done if store.expire_cursor != 0 => return false,
            Expiry::Done => break,
        }
    }
    if expired != expected_expired
        || emitted != expected_expired.len()
        || calls != expected_expired.len().div_ceil(cap).max(1)
    {
        return false;
    }
    for row in before {
        let should_survive = !expected_expired.contains(&row.handle.index);
        if store.get(row.handle).is_some() != should_survive {
            return false;
        }
    }
    assert_structural_invariants(&store)
}

#[test]
fn merge_remote_is_monotonic() {
    let mut store = store();
    store.intern_rule(descriptor_for(0x11, 1)).unwrap();
    let mut sink = DeltaSink::with_capacity(4);

    let mut obs = ObservationBatch::with_capacity(1);
    obs.push(observation(0x11, 0xabc, 0, 7, 1, 5, 100));
    store.merge_remote(&obs, &mut sink, RelayMark::Clear);
    assert_eq!(sink.len(), 1);
    let handle = sink.handles[0];
    assert_eq!(sink.previous[0], 0);
    assert_eq!(sink.current[0], 5);

    // Stale (3 < 5) — no delta.
    sink.clear();
    let mut obs2 = ObservationBatch::with_capacity(1);
    obs2.push(observation(0x11, 0xabc, 0, 7, 1, 3, 200));
    store.merge_remote(&obs2, &mut sink, RelayMark::Clear);
    assert_eq!(sink.len(), 0);

    // Higher (8 > 5) — delta of 3.
    sink.clear();
    let mut obs3 = ObservationBatch::with_capacity(1);
    obs3.push(observation(0x11, 0xabc, 0, 7, 1, 8, 300));
    store.merge_remote(&obs3, &mut sink, RelayMark::Clear);
    assert_eq!(sink.len(), 1);
    assert_eq!(sink.previous[0], 5);
    assert_eq!(sink.current[0], 8);
    assert_eq!(sink.deltas[0], 3);

    assert_eq!(store.get(handle).unwrap().count, 8);
}

#[test]
fn different_origins_are_independent() {
    let mut store = store();
    store.intern_rule(descriptor_for(0x11, 1)).unwrap();
    let _a = remote_merge_one(&mut store, 0x11, 1, 1, 5, 0);
    let _b = remote_merge_one(&mut store, 0x11, 2, 1, 7, 0);
    assert_eq!(store.active_len(), 2);
}

#[test]
fn merge_order_does_not_matter() {
    let make = |order: &[(u128, u32)]| {
        let mut store = store();
        store.intern_rule(descriptor_for(0x11, 1)).unwrap();
        for (origin, count) in order {
            remote_merge_one(&mut store, 0x11, *origin, 1, *count, 0);
        }
        let mut out: Vec<(u128, u32)> = store
            .active_handles(0)
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
    let mut store = store();
    store.intern_rule(descriptor_for(0x11, 1)).unwrap();
    let handle = local_increment_one(&mut store, 0x11, 0xabc, 0, 3, 10);
    let _ = local_increment_one(&mut store, 0x11, 0xabc, 0, 4, 20);
    let row = store.get(handle).unwrap();
    assert_eq!(row.count, 7);
    assert_eq!(row.last_update_millis, 20);
}

#[test]
fn capacity_is_enforced_without_growing() {
    let mut store = small_store(1);
    store.intern_rule(descriptor_for(0x11, 1)).unwrap();
    let mut sink = DeltaSink::with_capacity(2);
    let mut a = ObservationBatch::with_capacity(1);
    a.push(observation(0x11, 0xabc, 0, 1, 1, 1, 0));
    store.merge_remote(&a, &mut sink, RelayMark::Clear);
    let mut b = ObservationBatch::with_capacity(1);
    b.push(observation(0x11, 0xabc, 0, 2, 1, 1, 0));
    sink.clear();
    store.merge_remote(&b, &mut sink, RelayMark::Clear);
    assert_eq!(sink.len(), 0); // second insert was rejected
    assert_eq!(store.active_len(), 1);
    assert_eq!(store.stats().cell_store_full_rejects, 1);
    assert_eq!(store.capacity(), 1);
}

/// A bucket boundary that expires more cells than the sink holds
/// frees them in batches: each call stops when the sink is full, the next
/// resumes where it stopped, and the sink never grows.
#[test]
fn bucket_boundary_expires_in_batches_without_growth() {
    let mut store = small_store(64);
    store.intern_rule(descriptor_for(0x11, 1)).unwrap();
    let mut batch = ObservationBatch::with_capacity(20);
    for key in 0..20 {
        batch.push(observation(0x11, key, 0, 1, 1, 1_u32, 0));
    }
    store.ingest_local(&batch, &mut DeltaSink::with_capacity(20));
    let mut sink = ExpirationSink::with_capacity(8);
    let capacities = |sink: &ExpirationSink| {
        [
            sink.handles.capacity(),
            sink.keys.capacity(),
            sink.last_counts.capacity(),
            sink.last_update_millis.capacity(),
            sink.applies_locally.capacity(),
        ]
    };
    let before = capacities(&sink);
    let mut calls = Vec::new();
    loop {
        let expiry = store.expire_at(2_000, &mut sink);
        assert_eq!(capacities(&sink), before);
        calls.push((expiry, sink.len()));
        sink.clear();
        if expiry == Expiry::Done {
            break;
        }
    }
    assert_eq!(
        calls,
        [(Expiry::More, 8), (Expiry::More, 8), (Expiry::Done, 4)]
    );
    assert_eq!(store.active_len(), 0);
}

#[test]
fn library_warning_names_both_spellings() {
    let ((), logs) = with_logs(|| {
        let mut store = small_store(1);
        store.intern_rule(descriptor_for(0x11, 1)).unwrap();
        let mut sink = DeltaSink::with_capacity(2);
        for origin in [1, 2] {
            let mut batch = ObservationBatch::with_capacity(1);
            batch.push(observation(0x11, 0xabc, 0, origin, 1, 1, 0));
            store.merge_remote(&batch, &mut sink, RelayMark::Clear);
        }
    });
    let line = logs
        .lines()
        .find(|line| line.contains("cell_store_full"))
        .unwrap_or_else(|| panic!("no cell_store_full line in {logs:?}"));
    for spelling in [
        "storage.max_cells",
        "GABION_STORAGE_MAX_CELLS",
        "gabion_storage_max_cells",
        "allowed",
    ] {
        assert!(line.contains(spelling), "missing `{spelling}` in {line}");
    }
}

/// An index stays at most half full: a power of two with at least twice the
/// buckets of the entries it holds, up to the 2^30 cell cap.
#[test]
fn index_capacity_keeps_half_the_buckets_free() {
    for capacity in [1, 2, 3, 1_000, 1 << 16, (1 << 29) + 1, 1 << 30] {
        let buckets = pow2_index_capacity_for(NonZeroU32::new(capacity).unwrap());
        assert!(
            buckets.is_power_of_two() && buckets / 2 >= capacity && buckets / 4 < capacity,
            "{capacity} entries get {buckets} buckets"
        );
    }
}

// --- New tests required by the plan -----------------------------------

#[test]
fn delta_sink_records_exact_raises() {
    let mut store = small_store(4);
    store.intern_rule(descriptor_for(0x11, 1)).unwrap();
    let mut sink = DeltaSink::with_capacity(4);
    let mut obs = ObservationBatch::with_capacity(2);
    obs.push(observation(0x11, 0xabc, 0, 1, 1, 5, 0));
    obs.push(observation(0x11, 0xabc, 0, 2, 1, 7, 0));
    store.merge_remote(&obs, &mut sink, RelayMark::Clear);
    assert_eq!(sink.len(), 2);

    sink.clear();
    // No-op merges (count <= stored) produce nothing.
    let mut obs2 = ObservationBatch::with_capacity(2);
    obs2.push(observation(0x11, 0xabc, 0, 1, 1, 5, 0));
    obs2.push(observation(0x11, 0xabc, 0, 2, 1, 1, 0));
    store.merge_remote(&obs2, &mut sink, RelayMark::Clear);
    assert_eq!(sink.len(), 0);
}

#[test]
fn delta_applies_locally_reflects_rule_dictionary() {
    let mut store = small_store(4);
    store.intern_rule(descriptor_for(0x11, 1)).unwrap();
    // 0x22 fingerprint unknown locally.
    let mut sink = DeltaSink::with_capacity(2);
    let mut obs = ObservationBatch::with_capacity(2);
    obs.push(observation(0x11, 0xabc, 0, 9, 1, 1, 0));
    obs.push(observation(0x22, 0xabc, 0, 9, 1, 1, 0));
    store.merge_remote(&obs, &mut sink, RelayMark::Clear);
    assert_eq!(sink.len(), 2);
    assert_eq!(sink.applies_locally[0], 1);
    assert_eq!(sink.applies_locally[1], 0);
}

#[test]
fn unknown_rule_cells_round_trip() {
    let mut store = small_store(4);
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
    store.merge_remote(&obs, &mut sink, RelayMark::Clear);
    assert_eq!(sink.applies_locally[0], 0);

    // Expire: bucket 0, live = 0, current = 5 -> expired.
    let mut cur = vec![0_u32; store.rule_dictionary().capacity() as usize];
    let mut live = vec![0_u32; cur.len()];
    cur[row.key.rule as usize] = 5;
    live[row.key.rule as usize] = 0;
    let mut exp = ExpirationSink::with_capacity(store.capacity() as usize);
    assert_eq!(store.expire(&cur, &live, &mut exp), Expiry::Done);
    assert_eq!(store.active_len(), 0);
}

#[test]
fn unknown_rule_cells_expire_at_default_window() {
    let mut store = small_store(4);
    let handle = remote_merge_one(&mut store, 0x44, 5, 1, 10, 0);
    assert!(store.get(handle).is_some());

    let mut exp = ExpirationSink::with_capacity(store.capacity() as usize);
    assert_eq!(store.expire_at(61_000, &mut exp), Expiry::Done);

    assert!(store.get(handle).is_none());
    assert_eq!(exp.len(), 1);
    assert_eq!(exp.applies_locally[0], 0);
}

#[test]
fn compact_id_round_trip() {
    let mut store = store();
    store.intern_rule(descriptor_for(0xAA, 1)).unwrap();
    let rule_slot = store.rule_dictionary.find(0xAA).unwrap();
    let _ = remote_merge_one(&mut store, 0xAA, 99, 7, 1, 0);
    let node_slot = store.node_dictionary.find(NodeId(99), 7).unwrap();
    // Round-trip the dictionaries.
    let rd = store.rule_dictionary().descriptor(rule_slot).unwrap();
    assert_eq!(rd.fingerprint, 0xAA);
    let nd = store.node_dictionary().descriptor(node_slot).unwrap();
    assert_eq!(nd.node_id, NodeId(99));
    assert_eq!(nd.incarnation, 7);
}

#[test]
fn dictionary_refcount_frees_unreferenced_slots() {
    let mut store = small_store(4);
    store.intern_rule(descriptor_for(0xBB, 1)).unwrap();
    let h = remote_merge_one(&mut store, 0xBB, 100, 1, 1, 0);
    let row = store.get(h).unwrap();
    let node_slot = row.key.origin;
    let rule_slot = row.key.rule;
    assert_eq!(store.node_dictionary().refcount(node_slot), 1);
    // A configured rule carries its registration pin (1) plus the cell's ref.
    assert_eq!(store.rule_dictionary().refcount(rule_slot), 2);

    // Expire the cell.
    let mut cur = vec![0_u32; store.rule_dictionary().capacity() as usize];
    let mut live = vec![0_u32; cur.len()];
    cur[rule_slot as usize] = 5;
    live[rule_slot as usize] = 0;
    let mut exp = ExpirationSink::with_capacity(store.capacity() as usize);
    assert_eq!(store.expire(&cur, &live, &mut exp), Expiry::Done);
    assert_eq!(store.active_len(), 0);
    // The unpinned remote-origin node slot frees: refcount 0, descriptor None.
    assert_eq!(store.node_dictionary().refcount(node_slot), 0);
    assert!(store.node_dictionary().descriptor(node_slot).is_none());
    // The configured rule slot keeps its registration pin, so it survives the
    // cell's expiry and stays interned — this is what stops the next hit from
    // re-interning the 60 s `RuleDescriptor::default()` fallback.
    assert_eq!(store.rule_dictionary().refcount(rule_slot), 1);
    assert!(store.rule_dictionary().descriptor(rule_slot).is_some());
}

/// A configured rule (applies_locally = true) must keep its descriptor across a
/// full cell-expiry cycle. `intern_rule` holds a registration pin so the slot
/// is never released to the default fallback; the next hit re-uses the
/// configured descriptor — its real window, applies_locally true — instead of
/// re-interning the 60 s `RuleDescriptor::default()`. Direct regression for the
/// rule-dictionary release bug.
#[test]
fn configured_rule_pinned_survives_cell_expiry() {
    let mut store = small_store(4);
    let configured = RuleDescriptor {
        fingerprint: 0xCAFE,
        window_millis: 10_000,
        bucket_millis: 1_000,
        limit: 5,
        flags: 0,
        local_rule_id: 7,
    };
    let slot = store.intern_rule(configured).unwrap();
    // The registration pin alone holds the slot — no cells yet.
    assert_eq!(store.rule_dictionary().refcount(slot), 1);
    assert_eq!(store.rule_dictionary().descriptor(slot), Some(&configured));

    // One cell on the rule: pin (1) + cell ref (1).
    let h = remote_merge_one(&mut store, 0xCAFE, 100, 1, 3, 0);
    assert_eq!(store.rule_dictionary().refcount(slot), 2);

    // Force the cell to expire (bucket 0, current 20, live 0).
    let mut cur = vec![0_u32; store.rule_dictionary().capacity() as usize];
    let mut live = vec![0_u32; cur.len()];
    cur[slot as usize] = 20;
    live[slot as usize] = 0;
    let mut exp = ExpirationSink::with_capacity(store.capacity() as usize);
    assert_eq!(store.expire(&cur, &live, &mut exp), Expiry::Done);
    assert!(store.get(h).is_none());
    assert_eq!(store.active_len(), 0);

    // The pin survives: refcount back to 1, descriptor unchanged (the real 10 s
    // window and local_rule_id 7 — NOT the 60 s default).
    assert_eq!(store.rule_dictionary().refcount(slot), 1);
    assert_eq!(store.rule_dictionary().descriptor(slot), Some(&configured));

    // A fresh hit re-uses the configured slot and records applies_locally =
    // true. Under the released-then-default-reinterned bug this would land on a
    // new wire-only slot with applies_locally = false, so the local aggregator
    // would ignore it.
    let mut obs = ObservationBatch::with_capacity(1);
    let mut sink = DeltaSink::with_capacity(1);
    obs.push(observation(
        0xCAFE,
        0xabc,
        21,
        local().node_id.0,
        local().incarnation,
        2,
        21_000,
    ));
    store.ingest_local(&obs, &mut sink);
    assert_eq!(store.rule_dictionary.find(0xCAFE), Some(slot));
    assert_eq!(sink.len(), 1);
    assert_ne!(
        sink.applies_locally[0], 0,
        "re-recorded cell must apply locally (configured descriptor preserved)"
    );
    assert_eq!(store.rule_dictionary().descriptor(slot), Some(&configured));
}

/// A wire-only rule — learned from a peer via `merge_remote`, never registered
/// locally — carries no registration pin, so its slot must free when its last
/// cell expires. Guards that pinning configured rules didn't make stale peer
/// rules immortal.
#[test]
fn wire_only_rule_frees_when_cells_expire() {
    let mut store = small_store(4);
    let h = remote_merge_one(&mut store, 0xBEEF, 100, 1, 3, 0);
    let slot = store.get(h).unwrap().key.rule;
    // No pin: the cell is the only reference.
    assert_eq!(store.rule_dictionary().refcount(slot), 1);
    assert!(
        !store
            .rule_dictionary()
            .descriptor(slot)
            .unwrap()
            .applies_locally(),
        "an unregistered rule is wire-only"
    );

    let mut cur = vec![0_u32; store.rule_dictionary().capacity() as usize];
    let mut live = vec![0_u32; cur.len()];
    cur[slot as usize] = 20;
    live[slot as usize] = 0;
    let mut exp = ExpirationSink::with_capacity(store.capacity() as usize);
    assert_eq!(store.expire(&cur, &live, &mut exp), Expiry::Done);
    assert_eq!(store.active_len(), 0);

    // Slot freed: no lingering descriptor for a rule nobody configured.
    assert_eq!(store.rule_dictionary().refcount(slot), 0);
    assert!(store.rule_dictionary().descriptor(slot).is_none());
}

#[test]
fn dictionary_full_rejects_with_counter() {
    let mut store = CellStore::new(storage(16, 4), local(), &rules(1));
    // First merge interns a rule. Second merge with a distinct fingerprint
    // must fail.
    let h1 = remote_merge_one(&mut store, 0x10, 2, 1, 1, 0);
    assert!(store.get(h1).is_some());
    // A rejected row must not drop the valid row behind it, on either path.
    let mut sink = DeltaSink::with_capacity(2);
    let mut obs = ObservationBatch::with_capacity(2);
    obs.push(observation(0x20, 0xabc, 0, 3, 1, 1, 0));
    obs.push(observation(0x10, 0xabd, 0, 3, 1, 1, 0));
    store.merge_remote(&obs, &mut sink, RelayMark::Clear);
    assert_eq!(sink.len(), 1);
    store.ingest_local(&obs, &mut sink);
    assert_eq!(sink.len(), 2);
    assert_eq!(store.stats().rule_dictionary_full_rejects, 2);
}

#[test]
fn incarnation_change_isolates() {
    let mut store = store();
    store.intern_rule(descriptor_for(0x11, 1)).unwrap();
    let h1 = remote_merge_one(&mut store, 0x11, 42, 1, 5, 0);
    let h2 = remote_merge_one(&mut store, 0x11, 42, 2, 5, 0);
    let r1 = store.get(h1).unwrap();
    let r2 = store.get(h2).unwrap();
    assert_ne!(r1.key.origin, r2.key.origin);
    assert_eq!(store.active_len(), 2);
}

#[test]
fn expire_emits_one_row_per_freed_cell() {
    // Two configured rules (applies_locally = true), two origins. Both cells on
    // `rule_doomed` age out together; the rule's registration pin keeps its
    // descriptor live, so each freed cell still emits exactly one expiration
    // row carrying applies_locally = true read from that live descriptor.
    // (Before configured rules were pinned, the last cell's free_cell_at
    // released the slot mid-expiry and a post-free descriptor() lookup would
    // have degraded applies_locally to false. The pin removes that hazard;
    // emission still runs before free_cell_at for the cell rows.)
    let mut store = small_store(8);
    let rule_local = store.intern_rule(descriptor_for(0x11, 1)).unwrap();
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
    store.merge_remote(&obs, &mut sink, RelayMark::Clear);

    let rule_local_slot = store.rule_dictionary.find(0x11).unwrap();
    let rule_doomed_slot = store.rule_dictionary.find(0x22).unwrap();
    assert_eq!(rule_local_slot, rule_local);
    assert_eq!(rule_doomed_slot, rule_doomed);
    let node5 = store.node_dictionary.find(NodeId(5), 1).unwrap();
    let node6 = store.node_dictionary.find(NodeId(6), 1).unwrap();
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
    // Registration pin (1) + two cell refs.
    assert_eq!(store.rule_dictionary().refcount(rule_doomed), 3);

    // Build thresholds: doomed cells (bucket 0, live 0, current 5) expire;
    // the keep cell on rule_local stays because its rule lives 1000 buckets.
    let mut cur = vec![0_u32; store.rule_dictionary().capacity() as usize];
    let mut live = vec![0_u32; cur.len()];
    cur[rule_doomed as usize] = 5;
    live[rule_doomed as usize] = 0;
    cur[rule_local as usize] = 0;
    live[rule_local as usize] = 1000;

    let mut exp = ExpirationSink::with_capacity(store.capacity() as usize);
    assert_eq!(store.expire(&cur, &live, &mut exp), Expiry::Done);

    assert_eq!(store.active_len(), 1);
    assert_eq!(exp.len(), 2);

    // Both cell refs drop, but the registration pin keeps the doomed rule
    // interned (refcount 1, descriptor still present). The expired rows still
    // carry applies_locally=true (asserted below), read from the descriptor the
    // pin keeps live.
    assert_eq!(store.rule_dictionary().refcount(rule_doomed), 1);
    assert!(store.rule_dictionary().descriptor(rule_doomed).is_some());

    let mut rows: Vec<usize> = (0..exp.len()).collect();
    rows.sort_by_key(|&i| exp.handles[i].index);
    let mut expected = [snap_a, snap_b];
    expected.sort_by_key(|r| r.handle.index);
    for (&i, snap) in rows.iter().zip(expected.iter()) {
        assert_eq!(exp.handles[i], snap.handle);
        // The exported identity carries the rule fingerprint, not the
        // node-local rule slot — translate via the dictionary snapshot.
        assert_eq!(exp.keys[i].rule_fingerprint, 0x22);
        assert_eq!(exp.keys[i].key_hash, snap.key.key_hash);
        assert_eq!(exp.keys[i].bucket, snap.key.bucket);
        assert_eq!(exp.last_counts[i], snap.count);
        assert_eq!(exp.last_update_millis[i], snap.last_update_millis);
        assert_eq!(exp.applies_locally[i], 1);
    }
}

#[test]
fn expire_frees_slots_and_dictionary_refs() {
    let mut store = small_store(4);
    let rule_slot = store.intern_rule(descriptor_for(0xCC, 1)).unwrap();
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
    let mut exp = ExpirationSink::with_capacity(store.capacity() as usize);
    assert_eq!(store.expire(&cur, &live, &mut exp), Expiry::Done);
    assert_eq!(store.active_len(), 0);
    // Node dictionary no longer holds the (NodeId(100), 1) entry.
    assert!(store.node_dictionary().find(NodeId(100), 1).is_none());
}

#[test]
fn handle_generation_is_aba_safe() {
    let mut store = small_store(2);
    store.intern_rule(descriptor_for(0xDD, 1)).unwrap();
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
fn expire_at_and_expire_agree_for_same_now() {
    let rule = RuleDescriptor {
        fingerprint: 0x11,
        window_millis: 100,
        bucket_millis: 50,
        limit: 10,
        flags: 0,
        local_rule_id: 1,
    };
    let mk = || {
        let mut s = small_store(8);
        s.intern_rule(rule).unwrap();
        // Seed two cells: one at bucket 0 (will expire), one at bucket 5
        // (still inside the live window for now=300 -> current=6, live=2).
        let mut obs = ObservationBatch::with_capacity(2);
        let mut sink = DeltaSink::with_capacity(2);
        obs.push(observation(0x11, 0xabc, 0, 7, 1, 5, 0));
        obs.push(observation(0x11, 0xdef, 5, 8, 1, 5, 250));
        s.merge_remote(&obs, &mut sink, RelayMark::Clear);
        s
    };

    let now_millis: u64 = 300;
    // 300/50 = 6 (current epoch), 100/50 = 2 (live buckets) — computed through
    // the production helpers so this test breaks if their semantics drift.
    let current = rule.current_epoch(now_millis);
    let live = rule.live_buckets();

    let mut a = mk();
    let mut sink_a = ExpirationSink::with_capacity(a.capacity() as usize);
    assert_eq!(a.expire_at(now_millis, &mut sink_a), Expiry::Done);

    let mut b = mk();
    let dict_cap = b.rule_dictionary().capacity() as usize;
    let mut cur = vec![0_u32; dict_cap];
    let mut lv = vec![0_u32; dict_cap];
    let rule_slot = b.rule_dictionary.find(0x11).unwrap();
    cur[rule_slot as usize] = current;
    lv[rule_slot as usize] = live;
    let mut sink_b = ExpirationSink::with_capacity(b.capacity() as usize);
    assert_eq!(b.expire(&cur, &lv, &mut sink_b), Expiry::Done);

    assert_eq!(sink_a.len(), sink_b.len());
    assert_eq!(a.active_len(), b.active_len());
}

#[test]
fn descriptor_epoch_helpers_pin_window_math() {
    let rd = RuleDescriptor {
        fingerprint: 0x1,
        window_millis: 10_000,
        bucket_millis: 1_000,
        limit: 100,
        flags: 0,
        local_rule_id: 1,
    };
    assert_eq!(rd.current_epoch(12_345), 12);
    assert_eq!(rd.live_buckets(), 10);

    // A zero bucket has no epoch: both helpers return 0.
    let wire_only = RuleDescriptor {
        window_millis: 0,
        bucket_millis: 0,
        ..rd
    };
    assert_eq!(wire_only.current_epoch(12_345), 0);
    assert_eq!(wire_only.live_buckets(), 0);
}

#[test]
fn expire_at_uses_ceil_for_partial_bucket_windows() {
    let mut store = small_store(8);
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
    store.merge_remote(&obs, &mut deltas, RelayMark::Clear);

    let mut expirations = ExpirationSink::with_capacity(store.capacity() as usize);
    assert_eq!(store.expire_at(150, &mut expirations), Expiry::Done);

    // now=150ms -> current bucket 3. A 101ms window spans ceil(101/50)=3
    // buckets, so bucket 0 is still live. Floor division would use 2 live
    // buckets and expire this cell.
    assert_eq!(expirations.len(), 0);
    assert_eq!(store.active_len(), 1);
}

// --- Coverage extensions (Gaps 1-6) -----------------------------------

/// Like `quickcheck_store()` but pre-registers configured rules for
/// fingerprints `0x100` and `0x101` (`local_rule_id != u32::MAX`).
/// `intern_rule` pins each on registration, so their descriptors (and thus
/// `applies_locally`) survive even when every cell on the rule expires.
/// Fingerprints `0x102` and `0x103` remain auto-interned wire-only on first
/// use with `applies_locally = false`.
fn quickcheck_store_with_pinned_rules() -> CellStore {
    let mut store = quickcheck_store();
    store
        .intern_rule(RuleDescriptor {
            fingerprint: 0x100,
            window_millis: 1000,
            bucket_millis: 1000,
            limit: 1000,
            flags: 0,
            local_rule_id: 1,
        })
        .unwrap();
    store
        .intern_rule(RuleDescriptor {
            fingerprint: 0x101,
            window_millis: 1000,
            bucket_millis: 1000,
            limit: 1000,
            flags: 0,
            local_rule_id: 1,
        })
        .unwrap();
    store
}

/// Variant of `assert_structural_invariants` that allows one extra refcount
/// on each slot in `pinned_rule_slots` — those carry the registration pin
/// `intern_rule` holds for a configured rule, on top of their cell refs.
fn assert_invariants_with_pinned_rules(store: &CellStore, pinned_rule_slots: &[RuleSlot]) -> bool {
    let mut active = 0_u32;
    let mut rules = BTreeMap::<RuleSlot, u32>::new();
    let mut nodes = BTreeMap::<NodeSlot, u32>::new();
    let mut identities = BTreeSet::<(RuleSlot, u128, BucketEpoch, NodeSlot, Incarnation)>::new();

    for handle in store.active_handles(0) {
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
    Remote(RemoteRow, RelayMark),
    Local(LocalRow),
    Expire(ExpireTick),
}

impl Arbitrary for MixedOp {
    fn arbitrary(g: &mut Gen) -> Self {
        // ~40% Remote, ~40% Local, ~20% Expire.
        let pick = u8::arbitrary(g) % 10;
        if pick < 4 {
            let marks = [RelayMark::Clear, RelayMark::Changed, RelayMark::All];
            MixedOp::Remote(RemoteRow::arbitrary(g), *g.choose(&marks).unwrap())
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
    store: &mut CellStore,
    op: &MixedOp,
    sink: &mut DeltaSink,
    exp_sink: &mut ExpirationSink,
) {
    match op {
        MixedOp::Remote(row, mark) => {
            let mut obs = ObservationBatch::with_capacity(1);
            obs.push(remote_observation(row));
            store.merge_remote(&obs, sink, *mark);
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
                if let Some(slot) = store.rule_dictionary.find(fp) {
                    cur[slot as usize] = tick.current_by_fp[fp_idx] as u32;
                    live[slot as usize] = tick.live_by_fp[fp_idx] as u32;
                }
            }
            assert_eq!(store.expire(&cur, &live, exp_sink), Expiry::Done);
        }
    }
}

fn apply_mixed_op_to_model(model: &mut BTreeMap<PortableKey, u32>, op: &MixedOp) {
    match op {
        MixedOp::Remote(row, _) => {
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
        store.rule_dictionary.find(0x100).unwrap(),
        store.rule_dictionary.find(0x101).unwrap(),
    ];
    let pinned_fps: BTreeSet<u128> = [0x100_u128, 0x101_u128].into_iter().collect();
    let mut model: BTreeMap<PortableKey, u32> = BTreeMap::new();

    for op in &ops.0 {
        let mut sink = DeltaSink::with_capacity(1);
        let mut exp_sink = ExpirationSink::with_capacity(store.capacity() as usize);
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

/// The model of the own list: the live cells this node originated, by
/// ascending sequence.
fn own_cells(store: &CellStore) -> Vec<(u64, CellHandle)> {
    let mut own: Vec<_> = store
        .active_handles(0)
        .filter_map(|handle| store.get(handle))
        .filter(|row| row.key.origin == store.local_node_slot())
        .map(|row| (row.origin_sequence, row.handle))
        .collect();
    own.sort_by_key(|&(seq, _)| seq);
    own
}

/// Read `stream`'s list from its tail back to its head, at most `cap`
/// slots, so a cycle cannot loop.
fn backward(store: &CellStore, stream: Stream, cap: usize) -> Vec<CellHandle> {
    let mut backward = Vec::with_capacity(cap);
    let mut cursor = store.ends(stream).tail;
    while let Some(slot) = cursor.get()
        && backward.len() < cap
    {
        backward.push(store.handle_for(slot));
        cursor = store.stream_prev[slot as usize];
    }
    backward.reverse();
    backward
}

/// After every insert, raise, merge and expiry, the own list holds exactly
/// the live own cells in ascending sequence, and the relay list exactly the
/// remote cells marked for relay in the order they were marked, each read
/// forward and backward: a raised own cell or a remarked remote one moves to
/// the tail, a freed one or one raised by an unflagged packet leaves, no cell
/// is on both lists, and nothing repeats or cycles (each walk is capped one
/// past the capacity).
#[quickcheck]
fn stream_lists_match_model(ops: MixedOps) -> bool {
    let mut store = quickcheck_store();
    let cap = store.capacity() as usize + 1;
    let mut out = Vec::with_capacity(cap);
    let mut relay: Vec<PortableKey> = Vec::new();
    for op in &ops.0 {
        let mut sink = DeltaSink::with_capacity(1);
        let mut exp_sink = ExpirationSink::with_capacity(store.capacity() as usize);
        let before = portable_snapshot(&store);
        apply_mixed_op_with_sinks(&mut store, op, &mut sink, &mut exp_sink);
        let after = portable_snapshot(&store);
        if let MixedOp::Remote(row, mark) = op {
            let key = (
                0x100 + row.rule as u128,
                0x200 + row.key as u128,
                row.bucket as BucketEpoch,
                row.origin as u128,
                row.incarnation as Incarnation,
            );
            let rose = after.get(&key) > before.get(&key);
            let own = (key.3, key.4) == (local().node_id.0, local().incarnation);
            let marked = match mark {
                RelayMark::All => true,
                RelayMark::Changed => rose,
                RelayMark::Clear => false,
            };
            if !own && (marked || rose) {
                relay.retain(|k| *k != key);
            }
            if !own && marked && after.contains_key(&key) {
                relay.push(key);
            }
        }
        relay.retain(|key| after.contains_key(key));
        store.stream_after(Stream::Relay, 0, cap, &mut out);
        let relayed: Vec<_> = out.iter().map(|&h| portable_key(&store, h)).collect();
        if relayed != relay || backward(&store, Stream::Relay, cap) != out {
            return false;
        }
        let on_relay = out.clone();
        store.stream_after(Stream::Own, 0, cap, &mut out);
        let model: Vec<_> = own_cells(&store).into_iter().map(|(_, h)| h).collect();
        if out != model || out.iter().any(|&h| store.get(h).is_none()) {
            return false;
        }
        if let MixedOp::Local(_) = op
            && let Some(&changed) = sink.handles.first()
            && (out.last() != Some(&changed)
                || store.get(changed).map(|row| row.origin_sequence)
                    != Some(store.head(Stream::Own)))
        {
            return false;
        }
        if backward(&store, Stream::Own, cap) != model
            || on_relay.iter().any(|handle| model.contains(handle))
        {
            return false;
        }
    }
    true
}

/// `stream_after(sent, max)` returns exactly the live own cells above
/// `sent`, ascending, at most `max`, whichever end its walk starts from.
/// Half the draws put `sent` on a live cell's sequence, the boundary case.
#[quickcheck]
fn stream_after_returns_exactly_the_cells_above_sent(ops: MixedOps, pick: u8, max: u8) -> bool {
    let mut store = quickcheck_store();
    for op in &ops.0 {
        let mut sink = DeltaSink::with_capacity(1);
        let mut exp_sink = ExpirationSink::with_capacity(store.capacity() as usize);
        apply_mixed_op_with_sinks(&mut store, op, &mut sink, &mut exp_sink);
    }
    let own = own_cells(&store);
    let sent = match own.len() {
        len if len > 0 && pick.is_multiple_of(2) => own[usize::from(pick / 2) % len].0,
        _ => u64::from(pick) % (store.head(Stream::Own) + 3),
    };
    let max = usize::from(max % 8) + 1;
    let mut out = Vec::with_capacity(max);
    store.stream_after(Stream::Own, sent, max, &mut out);
    let expected: Vec<_> = own
        .iter()
        .filter(|&&(seq, _)| seq > sent)
        .take(max)
        .map(|&(_, h)| h)
        .collect();
    out == expected
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

fn quickcheck_store_tight() -> CellStore {
    CellStore::new(storage(4, 3), local(), &rules(2))
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
        store.merge_remote(&obs, &mut sink, RelayMark::Clear);

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
struct HitRow {
    hits: u32,
}

impl Arbitrary for HitRow {
    fn arbitrary(g: &mut Gen) -> Self {
        // Draw near u32::MAX so a small number of accumulations saturate.
        let jitter = u32::arbitrary(g) % 32;
        Self {
            hits: (u32::MAX - 16).saturating_add(jitter),
        }
    }
}

#[derive(Clone, Debug)]
struct HitRows(Vec<HitRow>);

impl Arbitrary for HitRows {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = (usize::arbitrary(g) % 16) + 1;
        Self((0..len).map(|_| HitRow::arbitrary(g)).collect())
    }
}

#[quickcheck]
fn quickcheck_count_saturates_without_overflow(rows: HitRows) -> bool {
    let mut store = CellStore::new(storage(4, 4), local(), &rules(4));
    let mut model_sum: u64 = 0;
    let mut delta_sum: u64 = 0;
    for row in &rows.0 {
        let mut obs = ObservationBatch::with_capacity(1);
        obs.push(Observation {
            rule_fingerprint: 0x100,
            key_hash: KeyHash(0xabc),
            bucket: 0,
            origin: NodeId(1),
            incarnation: 1,
            count: row.hits,
            last_update_millis: 0,
        });
        let mut sink = DeltaSink::with_capacity(1);
        store.ingest_local(&obs, &mut sink);
        delta_sum += sink.deltas.iter().map(|&d| u64::from(d)).sum::<u64>();
        model_sum += u64::from(row.hits);
    }
    let stored = store
        .active_handles(0)
        .next()
        .and_then(|h| store.get(h).map(|r| r.count))
        .unwrap_or(0);
    let expected = model_sum.min(u64::from(u32::MAX)) as u32;
    stored == expected && delta_sum == u64::from(stored)
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
    let mut sink_a = ExpirationSink::with_capacity(base.capacity() as usize);
    assert_eq!(store_a.expire_at(now_millis, &mut sink_a), Expiry::Done);

    let mut store_b = base.clone();
    let dict_cap = store_b.rule_dictionary().capacity() as usize;
    let mut cur = vec![0_u32; dict_cap];
    let mut live = vec![0_u32; dict_cap];
    for slot in 0..dict_cap {
        if let Some(d) = store_b.rule_dictionary().descriptor(slot as RuleSlot)
            && d.bucket_millis > 0
        {
            cur[slot] = (now_millis / d.bucket_millis as u64) as BucketEpoch;
            live[slot] = (d.window_millis / d.bucket_millis).max(1);
        }
    }
    let mut sink_b = ExpirationSink::with_capacity(base.capacity() as usize);
    assert_eq!(store_b.expire(&cur, &live, &mut sink_b), Expiry::Done);

    let handles_a: BTreeSet<u32> = sink_a.handles.iter().map(|h| h.index).collect();
    let handles_b: BTreeSet<u32> = sink_b.handles.iter().map(|h| h.index).collect();

    handles_a == handles_b
        && store_a.active_len() == store_b.active_len()
        && portable_snapshot(&store_a) == portable_snapshot(&store_b)
}

/// `n` rules on distinct keys, each `rate` over its own period and `bucket`.
fn configured(n: usize, rate: &str, bucket: Duration) -> RuleTable {
    let limits = storage(16, 8).cardinality;
    let keys: Vec<String> = (0..n).map(|i| format!("tenant{i}")).collect();
    let rules = keys
        .iter()
        .map(|key| {
            RuleDef {
                name: key,
                domain: "api",
                patterns: &[(key, "*")],
                rate,
                window: None,
                bucket: Some(bucket),
                mode: EnforcementMode::Enforce,
            }
            .build(&limits)
            .unwrap()
        })
        .collect();
    RuleTable::new(rules, None).unwrap()
}

/// One key hit once a millisecond from `t0` for 10 ms (twice at `t0`, so a
/// walk that stops at epoch 0 instead of wrapping misses a hit), and once
/// more at `t0 + 16`, under a 10 ms window of 1 ms buckets: its window total
/// at `t0 + 9`, how long until one more hit admits there, and how many of
/// its cells `expire_at(t0 + 15)` frees (the cell ahead of that epoch stays).
fn wrap_run(t0: u64) -> (u64, u64, usize) {
    let table = configured(1, "10r/10ms", Duration::from_millis(1));
    let rule = &table.rules()[0];
    let config = storage(64, 8);
    let mut store = CellStore::new(config, local(), &table);
    let aggregate = Aggregate::new(&config);
    let mut batch = ObservationBatch::with_capacity(11);
    for t in (t0..t0 + 10).chain([t0 + 16]) {
        let node = local();
        batch.push(observation(
            rule.fingerprint(),
            7,
            t as BucketEpoch,
            node.node_id.0,
            node.incarnation,
            1 + u32::from(t == t0),
            t,
        ));
    }
    let mut deltas = DeltaSink::with_capacity(11);
    store.ingest_local(&batch, &mut deltas);
    aggregate.apply(&deltas, &ExpirationSink::default());
    let table = aggregate.table();
    let total = table.window_total(rule.spec(), KeyHash(7), t0 + 9);
    let until = table.time_until_admit_millis(rule.spec(), KeyHash(7), t0 + 9, total, 1);
    let mut expired = ExpirationSink::with_capacity(64);
    assert_eq!(store.expire_at(t0 + 15, &mut expired), Expiry::Done);
    (total, until, expired.len())
}

/// The window total and expiry give the same answers when the `u32` epoch
/// wraps inside the window as when it does not.
#[test]
fn epoch_comparisons_survive_wrap() {
    assert_eq!(wrap_run(1_000), (11, 1, 5));
    assert_eq!(wrap_run((1 << 32) - 4), (11, 1, 5));
}

/// With no override, the rule dictionary holds twice the configured rules,
/// so 300 rules load where a fixed 256 would refuse them.
#[test]
fn rule_dictionary_default_grows_with_rules() {
    let table = configured(300, "10r/s", Duration::from_secs(1));
    assert_eq!(table.dictionary_capacity().get(), 600);
    assert_eq!(
        configured(3, "10r/s", Duration::from_secs(1))
            .dictionary_capacity()
            .get(),
        256
    );
    let stats = CellStore::new(storage(16, 8), local(), &table).stats();
    assert_eq!(
        (stats.rule_slots_capacity, stats.rule_slots_used),
        (600, 300)
    );
}

/// Every configured rule is interned and pinned before any runtime exists,
/// with its own window and bucket.
#[test]
fn cell_store_interns_every_rule_at_construction() {
    let table = configured(3, "10r/s", Duration::from_millis(250));
    let store = CellStore::new(storage(16, 8), local(), &table);
    for rule in table.rules() {
        let slot = store
            .rule_dictionary()
            .find(rule.fingerprint())
            .expect("interned");
        assert_eq!(store.rule_dictionary().refcount(slot), 1, "{}", rule.name());
        let interned = store.rule_dictionary().descriptor(slot).unwrap();
        assert_eq!(
            (interned.window_millis, interned.bucket_millis),
            (1_000, 250),
            "{}",
            rule.name()
        );
    }
}
