use std::net::SocketAddr;

use super::*;

const LOCAL_INCARNATION: Incarnation = 7;
const MIN_RTO_MS: u64 = 500;

fn peer() -> Peer {
    Peer::new(SocketAddr::from(([127, 0, 0, 1], 9000)))
}

fn header(range_start: u64, max: u64, cells: u32) -> wire::Header {
    wire::Header {
        sender_node_id: 42,
        sender_incarnation: 1,
        cell_count: cells,
        range_start,
        max_origin_sequence: max,
        ack_incarnation: LOCAL_INCARNATION,
        ..wire::Header::default()
    }
}

fn ack(sequence: u64) -> wire::Header {
    wire::Header {
        ack_sequence: sequence,
        ..header(0, 0, 0)
    }
}

fn receive(peer: &mut Peer, header: &wire::Header, now_ms: u64) {
    peer.receive(header, LOCAL_INCARNATION, true, now_ms, MIN_RTO_MS);
}

#[test]
fn have_advances_only_over_a_contiguous_range() {
    let mut peer = peer();
    receive(&mut peer, &header(0, 5, 3), 0);
    assert_eq!(peer.ack_fields().0, 5);

    // A packet that starts above `have` leaves a hole: its cells are merged
    // but must not be acknowledged.
    receive(&mut peer, &header(9, 12, 2), 0);
    assert_eq!(peer.ack_fields().0, 5);

    // The packet that fills the hole lets the next in-order one count.
    receive(&mut peer, &header(5, 9, 2), 0);
    assert_eq!(peer.ack_fields().0, 9);
    receive(&mut peer, &header(9, 12, 2), 0);
    assert_eq!(peer.ack_fields().0, 12);
}

#[test]
fn have_never_moves_backwards_on_a_duplicate() {
    let mut peer = peer();
    receive(&mut peer, &header(0, 8, 4), 0);
    receive(&mut peer, &header(0, 3, 1), 0);
    assert_eq!(peer.ack_fields().0, 8);
}

#[test]
fn have_does_not_claim_cells_the_store_refused() {
    let mut peer = peer();
    peer.receive(&header(0, 5, 3), LOCAL_INCARNATION, false, 0, MIN_RTO_MS);
    assert_eq!(peer.ack_fields().0, 0);
    assert!(peer.ack_owed(), "the sender is still told what we do hold");
}

#[test]
fn ack_is_owed_after_data_and_cleared_by_any_send() {
    let mut peer = peer();
    receive(&mut peer, &ack(0), 0);
    assert!(!peer.ack_owed(), "a pure ack needs no ack");
    receive(&mut peer, &header(0, 1, 1), 0);
    assert!(peer.ack_owed());
    peer.sent(4, false, 0, MIN_RTO_MS);
    assert!(!peer.ack_owed(), "data piggybacks the ack");
}

#[test]
fn acks_are_monotone_and_never_exceed_what_was_sent() {
    let mut peer = peer();
    peer.sent(10, false, 0, MIN_RTO_MS);
    receive(&mut peer, &ack(6), 10);
    assert_eq!(peer.delta_after(10), (10, false));
    receive(&mut peer, &ack(3), 20);
    receive(&mut peer, &ack(99), 30);
    assert_eq!(peer.delta_after(31), (10, false), "clamped to sent_upto");
    // Everything sent is acknowledged: nothing is outstanding, so the timer
    // is off and later rounds only send what is new.
    assert_eq!(peer.delta_after(100_000), (10, false));
}

#[test]
fn an_ack_for_another_incarnation_of_us_is_ignored() {
    let mut peer = peer();
    peer.sent(10, false, 0, MIN_RTO_MS);
    let stale = wire::Header {
        ack_incarnation: LOCAL_INCARNATION + 1,
        ack_sequence: 10,
        ..header(0, 0, 0)
    };
    receive(&mut peer, &stale, 10);
    assert_eq!(peer.delta_after(600), (0, true), "still unacknowledged");
}

#[test]
fn unacknowledged_data_is_retransmitted_after_the_timeout_only() {
    let mut peer = peer();
    peer.sent(10, false, 1_000, MIN_RTO_MS);
    assert_eq!(peer.delta_after(1_499), (10, false));
    assert_eq!(peer.delta_after(1_500), (0, true));

    // A retransmission restarts the timer.
    peer.sent(10, true, 1_500, MIN_RTO_MS);
    assert_eq!(peer.delta_after(1_600), (10, false));
    assert_eq!(peer.delta_after(2_000), (0, true));
}

#[test]
fn new_data_does_not_postpone_the_retransmission_of_old_data() {
    let mut peer = peer();
    peer.sent(10, false, 1_000, MIN_RTO_MS);
    peer.sent(20, false, 1_400, MIN_RTO_MS);
    assert_eq!(peer.delta_after(1_500), (0, true));
}

#[test]
fn a_partial_ack_restarts_the_timer_from_the_ack() {
    let mut peer = peer();
    peer.sent(10, false, 1_000, MIN_RTO_MS);
    peer.sent(20, false, 1_400, MIN_RTO_MS);
    receive(&mut peer, &ack(10), 1_450);
    // The ack took 450 ms, so the restarted timeout is 900 ms.
    assert_eq!(peer.delta_after(2_349), (20, false));
    assert_eq!(peer.delta_after(2_350), (10, true));
}

#[test]
fn rtt_samples_raise_the_timeout_above_the_floor() {
    let mut peer = peer();
    peer.sent(5, false, 0, MIN_RTO_MS);
    receive(&mut peer, &ack(5), 400);
    peer.sent(9, false, 1_000, MIN_RTO_MS);
    // srtt = 400 ms, so the timeout is 800 ms, not the 500 ms floor.
    assert_eq!(peer.delta_after(1_799), (9, false));
    assert_eq!(peer.delta_after(1_800), (5, true));
}

#[test]
fn a_retransmission_is_never_timed() {
    let mut peer = peer();
    peer.sent(5, false, 0, MIN_RTO_MS);
    peer.sent(5, true, 500, MIN_RTO_MS);
    receive(&mut peer, &ack(5), 510);
    peer.sent(9, false, 1_000, MIN_RTO_MS);
    // No sample was taken, so the timeout is still the floor.
    assert_eq!(peer.delta_after(1_500), (5, true));
}

#[test]
fn nothing_outstanding_drops_expired_data_from_the_books() {
    let mut peer = peer();
    peer.sent(10, false, 0, MIN_RTO_MS);
    receive(&mut peer, &ack(4), 100);
    peer.nothing_outstanding();
    assert_eq!(peer.delta_after(100_000), (4, false));
}

#[test]
fn a_restarted_peer_voids_all_sequence_state() {
    let mut peer = peer();
    receive(&mut peer, &header(0, 8, 2), 0);
    peer.sent(10, false, 0, MIN_RTO_MS);
    let restarted = wire::Header {
        sender_incarnation: 2,
        ..header(0, 3, 1)
    };
    receive(&mut peer, &restarted, 50);
    assert_eq!(peer.ack_fields(), (3, 2));
    assert_eq!(
        peer.delta_after(100_000),
        (0, false),
        "everything is resent"
    );
}
