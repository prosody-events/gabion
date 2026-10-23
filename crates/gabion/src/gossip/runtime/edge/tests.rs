use super::*;
use crate::defaults::{THICKET_EDGE_GAPS, THICKET_NACK_ATTEMPTS, THICKET_NACK_RETRY_MS};

fn lost(entries: &[ControlEntry]) -> Vec<u64> {
    entries.iter().map(|e| e.value).collect()
}

fn receive(edge: &mut Edge, seq: u64, now: u64) -> (bool, Vec<u64>) {
    let mut out = Vec::new();
    let fresh = edge.receive(1, seq, now, &mut out);
    (fresh, lost(&out))
}

#[test]
fn packets_in_order_open_no_gap() {
    let mut edge = Edge::new();
    for seq in 1..=5 {
        assert_eq!(receive(&mut edge, seq, 0), (true, vec![]));
    }
    assert_eq!(edge.next_retry(), None);
}

#[test]
fn a_skipped_number_is_asked_for_and_closed_by_the_late_packet() {
    let mut edge = Edge::new();
    receive(&mut edge, 1, 0);
    assert_eq!(receive(&mut edge, 4, 10), (true, vec![2, 3]));
    assert_eq!(edge.next_retry(), Some(10 + THICKET_NACK_RETRY_MS));
    // The retransmission is not new: it must not count as a duplicate
    // delivery, and it closes its gap.
    assert_eq!(receive(&mut edge, 2, 20), (false, vec![]));
    assert_eq!(receive(&mut edge, 3, 21), (false, vec![]));
    assert_eq!(edge.next_retry(), None);
}

#[test]
fn the_first_packet_seen_asks_for_the_few_before_it() {
    let mut edge = Edge::new();
    let (fresh, asked) = receive(&mut edge, 40, 0);
    assert!(fresh);
    let first = 40 - THICKET_EDGE_GAPS as u64;
    assert_eq!(asked, (first..40).collect::<Vec<_>>());
}

#[test]
fn a_probe_reveals_a_lost_tail() {
    let mut edge = Edge::new();
    receive(&mut edge, 1, 0);
    receive(&mut edge, 2, 0);
    let mut out = Vec::new();
    edge.probe(1, 4, 5, &mut out);
    assert_eq!(lost(&out), vec![3, 4]);
    // Probing again names nothing new.
    out.clear();
    edge.probe(1, 4, 6, &mut out);
    assert!(out.is_empty());
}

#[test]
fn unanswered_gaps_are_retried_then_given_up() {
    let mut edge = Edge::new();
    receive(&mut edge, 1, 0);
    receive(&mut edge, 3, 0);
    let mut now = 0;
    let mut asks = 1;
    loop {
        let mut out = Vec::new();
        edge.retry(now + THICKET_NACK_RETRY_MS - 1, &mut out);
        assert!(out.is_empty(), "not due before the retry interval");
        now += THICKET_NACK_RETRY_MS;
        edge.retry(now, &mut out);
        if out.is_empty() {
            break;
        }
        assert_eq!(lost(&out), vec![2]);
        asks += 1;
    }
    assert_eq!(asks, THICKET_NACK_ATTEMPTS);
    assert_eq!(edge.next_retry(), None);
}

#[test]
fn a_new_incarnation_restarts_the_numbering() {
    let mut edge = Edge::new();
    receive(&mut edge, 1, 0);
    receive(&mut edge, 9, 0);
    let mut out = Vec::new();
    assert!(edge.receive(2, 1, 5, &mut out));
    assert!(out.is_empty());
    assert_eq!(edge.next_retry(), None);
}

#[test]
fn the_history_answers_by_peer_and_number_and_forgets_a_peer() {
    let mut ring = SentRing::new();
    let a = [
        CellHandle {
            index: 1,
            generation: 1,
        },
        CellHandle {
            index: 2,
            generation: 1,
        },
    ];
    let b = [CellHandle {
        index: 3,
        generation: 1,
    }];
    ring.push(7, 1, &a);
    ring.push(8, 1, &b);
    assert_eq!(ring.find(7, 1), Some(&a[..]));
    assert_eq!(ring.find(8, 1), Some(&b[..]));
    assert_eq!(ring.find(7, 2), None);
    ring.forget(7);
    assert_eq!(ring.find(7, 1), None);
    assert_eq!(ring.find(8, 1), Some(&b[..]));
}

#[test]
fn a_packet_too_big_to_keep_is_not_kept_and_the_ring_wraps() {
    let mut ring = SentRing::new();
    let big = vec![CellHandle::default(); THICKET_RETX_CELLS + 1];
    ring.push(1, 1, &big);
    assert_eq!(ring.find(1, 1), None);
    for seq in 0..THICKET_RETX_PACKETS as u64 + 10 {
        ring.push(
            1,
            seq + 10,
            &[CellHandle {
                index: seq as u32,
                generation: 1,
            }],
        );
    }
    assert_eq!(ring.find(1, 10), None, "the oldest was overwritten");
    assert!(ring.find(1, THICKET_RETX_PACKETS as u64 + 19).is_some());
}
