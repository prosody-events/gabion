use std::collections::BTreeSet;

use super::super::tests::{IdleRuntime, addrs, idle_runtime};
use super::*;
use crate::crdt::{DeltaSink, NodeId, Observation, ObservationBatch};
use crate::gossip::sim::SimRouter;
use crate::gossip::tests::{key_hash, rule_fp, run_paused};
use crate::wire;

/// 100 ms buckets: at 3 s the current bucket is 30 and buckets up to 28 are
/// settled.
const NOW: u64 = 3_000;

/// Merge cells `(origin, bucket, count)` of rule 0, one key per origin and
/// bucket, as if a peer had sent them.
fn hold(rt: &mut IdleRuntime, cells: &[(u128, BucketEpoch, u32)]) {
    let mut batch = ObservationBatch::<u32>::with_capacity(cells.len());
    for &(origin, bucket, count) in cells {
        batch.push(Observation {
            rule_fingerprint: rule_fp(0),
            key_hash: key_hash(bucket as u8),
            bucket,
            origin: NodeId(origin),
            incarnation: 1,
            count,
            last_update_millis: NOW,
            origin_sequence: u64::from(bucket),
        });
    }
    let mut sink = DeltaSink::<u32>::with_capacity(cells.len());
    rt.store.merge_remote(&batch, &mut sink);
    assert_eq!(sink.len(), cells.len());
}

/// Deliver every frame `from` has queued to `to`, as from peer `peer`.
fn deliver_digests(from: &mut IdleRuntime, to: &mut IdleRuntime, peer: u16) {
    for (_, slot) in std::mem::take(&mut from.send_pending) {
        let bytes = from.send_pool[usize::from(slot)].as_bytes();
        assert!(wire::is_digest(bytes));
        to.recv_buf[..bytes.len()].copy_from_slice(bytes);
        to.handle_digest(peer, bytes.len());
    }
}

/// The cells `rt` has queued: `(origin, bucket, count)`.
fn pushed(rt: &IdleRuntime) -> BTreeSet<(u128, BucketEpoch, u32)> {
    let mut cells = BTreeSet::new();
    for &(_, slot) in &rt.send_pending {
        let bytes = rt.send_pool[usize::from(slot)].as_bytes();
        wire::decode_unauth_visit::<u32>(
            bytes,
            rt.config.wire_limits,
            |_| true,
            |cell| {
                cells.insert((cell.origin_node_id.0, cell.bucket, cell.count));
            },
        )
        .unwrap();
    }
    cells
}

fn digest(rt: &mut IdleRuntime, to: u16) {
    let rule = rt.store.find_rule(rule_fp(0)).unwrap();
    let settled = rt.settled(rule, NOW).unwrap();
    rt.send_digest(to, rule, settled);
}

#[test]
fn settled_buckets_skip_the_newest_two_and_the_oldest_in_the_window() {
    run_paused(async {
        let rt = idle_runtime(&SimRouter::new(), &addrs(4), 0);
        let rule = rt.store.find_rule(rule_fp(0)).unwrap();
        let s = rt.settled(rule, NOW).unwrap();
        assert_eq!((s.first, s.last), (0, 28));
        // An hour of 100 ms buckets: only the newest 64 settled ones.
        let late = 10 * 3_600_000;
        let s = rt.settled(rule, late).unwrap();
        assert_eq!((s.first, s.last), (359_998 - 63, 359_998));
        assert!(rt.settled(rule, 150).is_none(), "nothing has settled yet");
    });
}

#[test]
fn a_digest_is_answered_with_what_its_sender_lacks() {
    run_paused(async {
        let all = addrs(4);
        let router = SimRouter::new();
        let mut a = idle_runtime(&router, &all, 0);
        let mut b = idle_runtime(&router, &all, 1);
        let (x, y, z) = (0xF1, 0xF2, 0xF3);
        // B holds more of X's bucket 11, all of Z, and a bucket of X too new
        // to be settled. A and B agree on X's bucket 10 and Y's bucket 12.
        hold(&mut a, &[(x, 10, 2), (x, 11, 1), (y, 12, 1)]);
        hold(
            &mut b,
            &[(x, 10, 2), (x, 11, 3), (y, 12, 1), (z, 13, 4), (x, 29, 5)],
        );

        let b_in_a = a.thicket.index_of(all[1]).unwrap();
        let a_in_b = b.thicket.index_of(all[0]).unwrap();
        digest(&mut a, b_in_a);
        deliver_digests(&mut a, &mut b, a_in_b);

        assert_eq!(pushed(&b), BTreeSet::from([(x, 11, 3), (z, 13, 4)]));
    });
}

#[test]
fn nodes_that_agree_send_nothing_but_the_digest() {
    run_paused(async {
        let all = addrs(4);
        let router = SimRouter::new();
        let mut a = idle_runtime(&router, &all, 0);
        let mut b = idle_runtime(&router, &all, 1);
        let cells = [(0xF1, 10, 2), (0xF2, 12, 1), (0xF3, 28, 7)];
        hold(&mut a, &cells);
        hold(&mut b, &cells);

        let b_in_a = a.thicket.index_of(all[1]).unwrap();
        let a_in_b = b.thicket.index_of(all[0]).unwrap();
        digest(&mut a, b_in_a);
        deliver_digests(&mut a, &mut b, a_in_b);

        assert!(b.send_pending.is_empty());
    });
}

#[test]
fn a_digest_spanning_frames_lists_every_origin_in_one_of_them() {
    run_paused(async {
        let all = addrs(48);
        let router = SimRouter::new();
        let mut a = idle_runtime(&router, &all, 0);
        let mut b = idle_runtime(&router, &all, 1);
        // More origins than one frame of 29 buckets holds; both nodes agree
        // on all of them, so a frame that misstated its range would make B
        // push the origins it thinks are missing.
        let cells: Vec<_> = (0..40).map(|o| (0x100 + o, 5, 1)).collect();
        a.config.wire_limits.max_payload_bytes = 1400;
        hold(&mut a, &cells);
        hold(&mut b, &cells);

        let b_in_a = a.thicket.index_of(all[1]).unwrap();
        let a_in_b = b.thicket.index_of(all[0]).unwrap();
        digest(&mut a, b_in_a);
        assert!(
            a.send_pending.len() > 1,
            "the digest should take several frames"
        );
        deliver_digests(&mut a, &mut b, a_in_b);

        assert!(b.send_pending.is_empty(), "pushed {:?}", pushed(&b));
    });
}
