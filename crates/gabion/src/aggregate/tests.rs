use std::collections::HashMap;

use quickcheck_macros::quickcheck;

use super::*;
use crate::crdt::{CellHandle, CellIdentity};
use crate::mock::with_logs;
use crate::settings::mock::storage;

/// An empty table of `capacity` slots.
fn aggregate(capacity: usize) -> Aggregate {
    Aggregate::with_capacity(capacity)
}

fn writer(aggregate: &Aggregate) -> AggregateWriter<'_> {
    AggregateWriter::new(aggregate.table(), &aggregate.write_seq, &aggregate.dropped)
}

fn spec(fingerprint: u128, limit: u64, bucket_millis: u64, live_buckets: u32) -> RuleSpec {
    RuleSpec {
        id: 1,
        fingerprint,
        limit,
        bucket_millis: bucket_millis.try_into().unwrap(),
        window_millis: bucket_millis * u64::from(live_buckets),
        live_buckets: live_buckets.try_into().unwrap(),
    }
}

fn all_empty(aggregate: &Aggregate) -> bool {
    aggregate
        .slots
        .iter()
        .all(|slot| matches!(slot.read(), Read::Empty))
}

#[test]
fn write_then_read_one_cell() {
    let table = aggregate(8);
    assert!(writer(&table).add(0x42, 0x99, 7, 3));
    assert_eq!(table.table().get(0x42, 0x99, 7).expect("found").count, 3);
    assert!(writer(&table).add(0x42, 0x99, 7, 5));
    assert_eq!(table.table().get(0x42, 0x99, 7).expect("found").count, 8);
}

#[test]
fn expiration_subtracts_then_empties_the_slot() {
    let table = aggregate(8);
    writer(&table).add(0x42, 0x99, 7, 5);
    writer(&table).subtract(0x42, 0x99, 7, 2);
    assert_eq!(table.table().get(0x42, 0x99, 7).expect("found").count, 3);
    writer(&table).subtract(0x42, 0x99, 7, 3);
    assert!(table.table().get(0x42, 0x99, 7).is_none());
    assert!(all_empty(&table));
}

#[test]
fn window_total_sums_live_buckets() {
    let table = aggregate(16);
    for (bucket, count) in [(99, 100), (100, 4), (101, 7), (102, 2)] {
        writer(&table).add(0xaa, 0xbb, bucket, count);
    }
    let total = table
        .table()
        .window_total(spec(0xaa, 100, 1_000, 3), KeyHash(0xbb), 102 * 1_000);
    assert_eq!(total, 4 + 7 + 2);
}

/// Four cells with one home slot fill a four-slot table, so three of them
/// are written and found only by walking past their home.
#[test]
fn collisions_are_resolved_by_probing() {
    let table = aggregate(4);
    let home = |key| mix_index(1, key, 0) & table.table().mask();
    let keys: Vec<u128> = (0..).filter(|&key| home(key) == home(0)).take(4).collect();
    for (count, &key) in (1..).zip(&keys) {
        assert!(writer(&table).add(1, key, 0, count));
    }
    for (count, &key) in (1..).zip(&keys) {
        assert_eq!(table.table().get(1, key, 0).expect("found").count, count);
    }
}

/// Random raises and expiries against a `HashMap`: every key's window total
/// matches the model, no write is dropped, and once everything has expired
/// the table is empty again rather than full of holes.
#[quickcheck]
fn aggregate_table_matches_model(ops: Vec<(u8, u8, bool)>) -> bool {
    let table = aggregate(128);
    let mut model: HashMap<(u128, u32), u64> = HashMap::new();
    for (key, bucket, expire) in ops {
        let cell = (u128::from(key % 16), u32::from(bucket % 4));
        let count = model.entry(cell).or_default();
        if expire && *count > 0 {
            writer(&table).subtract(1, cell.0, cell.1, *count);
            *count = 0;
        } else {
            assert!(writer(&table).add(1, cell.0, cell.1, 1), "dropped a write");
            *count += 1;
        }
    }
    let rule = spec(1, u64::MAX, 1, 4);
    let totals_match = (0..16_u128).all(|key| {
        let expected: u64 = (0..4)
            .map(|bucket| model.get(&(key, bucket)).copied().unwrap_or(0))
            .sum();
        table.table().window_total(rule, KeyHash(key), 3) == expected
    });
    for (&(key, bucket), &count) in &model {
        if count > 0 {
            writer(&table).subtract(1, key, bucket, count);
        }
    }
    totals_match && table.table().occupied() == 0
}

/// One delta row of `delta` hits for `(1, 2, bucket)`.
fn delta(bucket: BucketEpoch, delta: u32) -> DeltaSink {
    DeltaSink {
        handles: vec![CellHandle::default()],
        keys: vec![CellIdentity {
            rule_fingerprint: 1,
            key_hash: KeyHash(2),
            bucket,
        }],
        previous: vec![0],
        current: vec![delta],
        deltas: vec![delta],
        applies_locally: vec![1],
    }
}

/// A write with no free slot near its home is dropped and counted: the cell
/// reads as absent, so an adapter checking it allows the request, and
/// `aggregate_full` names both adapters' knobs.
#[test]
fn full_aggregate_allows_and_logs() {
    let table = aggregate(MAX_PROBE);
    let none = ExpirationSink::default();
    let ((), logs) = with_logs(|| {
        for bucket in 0..MAX_PROBE as u32 {
            table.apply(&delta(bucket, 1), &none);
        }
        assert_eq!(table.dropped.load(std_atomic::Ordering::Relaxed), 0);
        table.apply(&delta(MAX_PROBE as u32, 1), &none);
        table.apply(&delta(0, 1), &none);
    });
    assert_eq!(table.dropped.load(std_atomic::Ordering::Relaxed), 1);
    let view = table.table();
    assert!(view.get(1, 2, MAX_PROBE as u32).is_none());
    assert_eq!(
        view.window_total(spec(1, 1, 1, 1), KeyHash(2), u64::from(MAX_PROBE as u32)),
        0
    );
    assert_eq!(view.get(1, 2, 0).expect("kept").count, 2);
    assert_eq!(logs.matches("code=\"aggregate_full\"").count(), 1, "{logs}");
    for knob in [
        "storage.max_cells",
        "GABION_STORAGE_MAX_CELLS",
        "gabion_limit_zone",
    ] {
        assert!(logs.contains(knob), "{knob}: {logs}");
    }
}

/// `gabiond`'s table has room for a cell per cell-store cell at most half
/// full, so a full cell store (one origin per key, as on a lone node) loses
/// no counts to `aggregate_full`.
#[test]
fn gabiond_table_holds_a_full_cell_store() {
    let cells = 4_096;
    let aggregate = Aggregate::new(&storage(cells, 64));
    let writer = writer(&aggregate);
    let dropped = (0..u128::from(cells))
        .filter(|&key| !writer.add(1, key, 0, 1))
        .count();
    assert_eq!(dropped, 0);
}

#[test]
fn time_until_admit_millis_uses_oldest_non_empty_bucket() {
    // Window = [6..10], bm=1000. Bucket 6 holds 15 hits — total=15,
    // limit=10, hits=1 → need=6. Bucket 6 alone covers it; delta =
    // (6+5)*1000 - 10000 = 1000ms. Bucket 5 has left the window but not yet
    // the table (expiry runs on the gossip tick), so the walk must skip it.
    let table = aggregate(16);
    writer(&table).add(0xaa, 0xbb, 5, 100);
    writer(&table).add(0xaa, 0xbb, 6, 15);
    let delta = table.table().time_until_admit_millis(
        spec(0xaa, 10, 1_000, 5),
        KeyHash(0xbb),
        10_000,
        15,
        1,
    );
    assert_eq!(delta, 1_000);
}

#[test]
fn time_until_admit_millis_walks_past_expired_buckets() {
    // Window = [6..10]. Bucket 6 expired → reads as None. Bucket 7
    // alone covers need = 15+1-10 = 6.
    let table = aggregate(16);
    writer(&table).add(0xcc, 0xdd, 6, 4);
    writer(&table).subtract(0xcc, 0xdd, 6, 4);
    writer(&table).add(0xcc, 0xdd, 7, 15);
    let delta = table.table().time_until_admit_millis(
        spec(0xcc, 10, 1_000, 5),
        KeyHash(0xdd),
        10_000,
        15,
        1,
    );
    assert_eq!(delta, 2_000);
}

#[test]
fn time_until_admit_millis_already_admittable_returns_zero() {
    let table = aggregate(8);
    writer(&table).add(0xee, 0xff, 10, 5);
    let delta = table.table().time_until_admit_millis(
        spec(0xee, 10, 1_000, 5),
        KeyHash(0xff),
        10_000,
        5,
        3,
    );
    assert_eq!(delta, 0);
}
