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

    // The packet that fills the hole lets the one beyond it count too.
    receive(&mut peer, &header(5, 9, 2), 0);
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

    // A retransmission restarts the timer, doubled.
    peer.sent(10, true, 1_500, MIN_RTO_MS);
    assert_eq!(peer.delta_after(1_600), (10, false));
    assert_eq!(peer.delta_after(2_499), (10, false));
    assert_eq!(peer.delta_after(2_500), (0, true));
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

#[test]
fn a_reordered_chain_is_acknowledged_once_the_hole_fills() {
    let mut peer = peer();
    // Three packets of one chain arrive last first.
    receive(&mut peer, &header(9, 12, 2), 0);
    receive(&mut peer, &header(5, 9, 2), 0);
    assert_eq!(peer.ack_fields().0, 0, "nothing contiguous yet");
    receive(&mut peer, &header(0, 5, 2), 0);
    assert_eq!(peer.ack_fields().0, 12);
}

#[test]
fn a_range_beyond_the_hole_is_kept_until_a_disjoint_newer_one_replaces_it() {
    let mut peer = peer();
    receive(&mut peer, &header(0, 2, 1), 0);
    receive(&mut peer, &header(4, 6, 1), 0);
    // Disjoint from (4, 6]: the newer range is kept instead.
    receive(&mut peer, &header(8, 10, 1), 0);
    receive(&mut peer, &header(2, 4, 1), 0);
    assert_eq!(peer.ack_fields().0, 4, "(4, 6] was dropped");
    receive(&mut peer, &header(4, 8, 1), 0);
    assert_eq!(peer.ack_fields().0, 10, "(8, 10] was kept");
}

#[test]
fn a_retransmission_cut_short_resumes_next_round() {
    let mut peer = peer();
    peer.sent(20, false, 0, MIN_RTO_MS);
    assert_eq!(peer.delta_after(500), (0, true));
    // The send pool took the chain only up to 8.
    peer.sent(8, true, 500, MIN_RTO_MS);
    assert_eq!(
        peer.delta_after(501),
        (8, false),
        "the rest follows at once"
    );
    peer.sent(20, false, 501, MIN_RTO_MS);
    assert_eq!(peer.delta_after(502), (20, false));
}

#[test]
fn a_late_ack_above_the_rewound_cursor_moves_it() {
    let mut peer = peer();
    peer.sent(20, false, 0, MIN_RTO_MS);
    peer.sent(8, true, 500, MIN_RTO_MS);
    receive(&mut peer, &ack(15), 510);
    assert_eq!(peer.delta_after(511), (15, false));
    // The ack is still clamped to the highest sequence ever sent.
    receive(&mut peer, &ack(99), 520);
    assert_eq!(peer.delta_after(100_000), (20, false));
}

#[test]
fn timeouts_without_an_ack_back_off_up_to_the_cap() {
    let mut peer = peer();
    peer.sent(10, false, 0, MIN_RTO_MS);
    let mut now = MIN_RTO_MS;
    for doublings in 1..=GOSSIP_MAX_RTO_BACKOFF + 2 {
        assert_eq!(peer.delta_after(now), (0, true));
        peer.sent(10, true, now, MIN_RTO_MS);
        let timeout = MIN_RTO_MS << doublings.min(GOSSIP_MAX_RTO_BACKOFF);
        assert_eq!(peer.delta_after(now + timeout - 1), (10, false));
        now += timeout;
    }
}

#[test]
fn an_ack_ends_the_backoff() {
    let mut peer = peer();
    peer.sent(10, false, 0, MIN_RTO_MS);
    peer.sent(10, true, 500, MIN_RTO_MS);
    receive(&mut peer, &ack(4), 600);
    // Back to the base timeout, timed from the ack, not the doubled one.
    assert_eq!(peer.delta_after(1_099), (10, false));
    assert_eq!(peer.delta_after(1_100), (4, true));
}

#[test]
fn hearing_from_a_peer_after_two_unanswered_timeouts_resends_at_once() {
    let mut peer = peer();
    peer.sent(10, false, 0, MIN_RTO_MS);
    peer.sent(10, true, 500, MIN_RTO_MS);
    // One unanswered timeout: the peer is heard without catching up (its ack
    // is for another incarnation of us), which ends the backoff but resends
    // no sooner than the base timeout after the last resend.
    let stale = wire::Header {
        ack_incarnation: LOCAL_INCARNATION + 1,
        ..header(0, 3, 1)
    };
    receive(&mut peer, &stale, 600);
    assert!(!peer.retransmit_due(600));
    assert_eq!(peer.delta_after(999), (10, false));
    assert_eq!(peer.delta_after(1_000), (0, true));
    // Two in a row: the next packet from the peer makes the resend due at
    // once.
    peer.sent(10, true, 1_000, MIN_RTO_MS);
    peer.sent(10, true, 2_000, MIN_RTO_MS);
    receive(&mut peer, &stale, 2_200);
    assert!(peer.retransmit_due(2_200));
    assert_eq!(peer.delta_after(2_200), (0, true));
    // That resend backs off again, so further packets resend it no sooner
    // than the base timeout.
    peer.sent(10, true, 2_200, MIN_RTO_MS);
    receive(&mut peer, &stale, 2_300);
    assert_eq!(peer.delta_after(2_699), (10, false));
    assert_eq!(peer.delta_after(2_700), (0, true));
}
#[test]
fn an_ack_from_a_peer_heard_again_does_not_delay_the_resend() {
    let mut peer = peer();
    peer.sent(10, false, 0, MIN_RTO_MS);
    peer.sent(10, true, 500, MIN_RTO_MS);
    peer.sent(10, true, 1_500, MIN_RTO_MS);
    // The peer comes back acknowledging part of what it was sent before it
    // went quiet: the rest is resent at once, not a timeout later.
    receive(&mut peer, &ack(4), 3_000);
    assert_eq!(peer.delta_after(3_000), (4, true));
}

#[test]
fn the_rounds_after_a_retransmission_are_not_timed() {
    let mut peer = peer();
    peer.sent(20, false, 0, MIN_RTO_MS);
    peer.sent(8, true, 500, MIN_RTO_MS);
    // The rest of the chain is re-sent, not sent for the first time.
    peer.sent(20, false, 510, MIN_RTO_MS);
    receive(&mut peer, &ack(20), 900);
    peer.sent(30, false, 1_000, MIN_RTO_MS);
    // No sample, so the timeout is still the floor.
    assert_eq!(peer.delta_after(1_499), (30, false));
    assert_eq!(peer.delta_after(1_500), (20, true));
}

#[test]
fn a_packet_from_a_peer_that_is_not_backed_off_changes_no_timer() {
    let mut peer = peer();
    peer.sent(10, false, 0, MIN_RTO_MS);
    receive(&mut peer, &header(0, 3, 1), 100);
    assert_eq!(peer.delta_after(499), (10, false));
    assert_eq!(peer.delta_after(500), (0, true));
}
