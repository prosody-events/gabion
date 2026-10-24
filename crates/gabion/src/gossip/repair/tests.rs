use std::num::NonZeroU64;

use super::*;

fn total(key: u128, bucket: BucketEpoch) -> KeyTotal<u32> {
    KeyTotal {
        rule: 1,
        key_hash: KeyHash(key),
        bucket,
        count: 9,
    }
}

fn missed(latest: u64, missing: u64) -> Option<Nack> {
    Some(Nack::Missed {
        latest,
        missing: NonZeroU64::new(missing).unwrap(),
    })
}

/// A receiver that has caught up through a snapshot covering `through`.
fn caught_up(through: u64) -> Heard {
    let mut heard = Heard::new();
    heard.snapshot_part(through, 0, true);
    assert_eq!(heard.request(), None);
    heard
}

#[test]
fn history_returns_what_each_packet_carried() {
    let mut history = History::new(4, 16);
    history.record(1, &[total(1, 0), total(2, 0)]);
    history.record(2, &[total(3, 5)]);
    let got: Vec<_> = history.get(1).unwrap().collect();
    assert_eq!(got, vec![(1, KeyHash(1), 0), (1, KeyHash(2), 0)]);
    let got: Vec<_> = history.get(2).unwrap().collect();
    assert_eq!(got, vec![(1, KeyHash(3), 5)]);
    assert!(history.get(3).is_none());
    assert!(history.get(0).is_none());
}

#[test]
fn history_forgets_packets_whose_slot_or_buckets_were_reused() {
    // Four packet slots: packet 5 takes packet 1's slot.
    let mut history = History::new(4, 16);
    for seq in 1..=5 {
        history.record(seq, &[total(u128::from(seq), 0)]);
    }
    assert!(history.get(1).is_none());
    assert!(history.get(2).is_some());

    // Eight bucket slots: three packets of three buckets overwrite the first.
    let mut history = History::new(16, 8);
    let three = [total(1, 0), total(2, 0), total(3, 0)];
    for seq in 1..=3 {
        history.record(seq, &three);
    }
    assert!(history.get(1).is_none());
    assert_eq!(history.get(3).unwrap().count(), 3);

    // A packet larger than the bucket ring is not kept at all.
    history.record(4, &[total(1, 0); 9]);
    assert!(history.get(4).is_none());
}

#[test]
fn a_new_owner_is_asked_for_a_snapshot() {
    let mut heard = Heard::new();
    assert_eq!(heard.request(), Some(Nack::Snapshot));
    // Broadcasts alone do not stand in for it: earlier ones are unknown.
    // The request waits for the sync, which paces snapshot requests.
    assert_eq!(heard.broadcast(3), None);
    assert_eq!(heard.request(), Some(Nack::Snapshot));
}

#[test]
fn gaps_are_asked_for_and_cleared_by_repairs_or_late_arrivals() {
    let mut heard = caught_up(10);
    assert_eq!(heard.broadcast(11), None);
    assert_eq!(heard.request(), None);
    // 12 and 13 missing: bits 1 (= 14 - 1 - 12) and 0, asked for at once.
    assert_eq!(heard.broadcast(14), missed(14, 0b11));
    assert_eq!(heard.request(), missed(14, 0b11));
    assert_eq!(heard.broadcast(13), None); // late, reordered
    assert_eq!(heard.request(), missed(14, 0b10));
    assert_eq!(heard.broadcast(16), missed(16, 0b1001));
    heard.repaired(12);
    heard.repaired(15);
    assert_eq!(heard.request(), None);
}

#[test]
fn falling_too_far_behind_asks_for_a_snapshot() {
    let mut heard = caught_up(1);
    heard.broadcast(3); // 2 missing
    assert_eq!(heard.broadcast(70), None); // 2's bit falls off the top
    assert_eq!(heard.request(), Some(Nack::Snapshot));

    let mut heard = caught_up(1);
    heard.broadcast(500);
    assert_eq!(heard.request(), Some(Nack::Snapshot));
}

#[test]
fn a_snapshot_counts_only_once_every_part_arrived() {
    let mut heard = Heard::new();
    heard.snapshot_part(7, 2, true);
    heard.snapshot_part(7, 0, false);
    assert_eq!(heard.request(), Some(Nack::Snapshot));
    // A part of a different snapshot starts over.
    heard.snapshot_part(9, 1, false);
    heard.snapshot_part(9, 0, false);
    assert_eq!(heard.request(), Some(Nack::Snapshot));
    heard.snapshot_part(9, 2, true);
    assert_eq!(heard.request(), None);
    heard.broadcast(10);
    assert_eq!(heard.request(), None);
}

#[test]
fn a_snapshot_covers_the_gaps_before_it_but_not_after() {
    let mut heard = caught_up(1);
    heard.broadcast(3); // 2 missing
    heard.broadcast(70); // lost track: snapshot wanted, 4..=69 missing
    heard.snapshot_part(60, 0, true);
    // 61..=69 are newer than the snapshot and still missing: bits 0..=8.
    assert_eq!(heard.request(), missed(70, 0b1_1111_1111));
}

#[test]
fn an_unrequested_snapshot_changes_nothing() {
    let mut heard = caught_up(5);
    heard.broadcast(8);
    heard.snapshot_part(20, 0, true);
    assert_eq!(heard.request(), missed(8, 0b11));
}
