use std::net::SocketAddr;

use super::*;

fn addr(s: &str) -> SocketAddr {
    s.parse().expect("socket address")
}

fn peer(s: &str) -> Peer {
    let a = addr(s);
    Peer::new(a, NodeId(7), Zone::of(a, 16))
}

#[test]
fn ack_progress_returns_rtt_sample_once() {
    let mut out = Outbound::default();
    out.note_sent(10, 1_000);
    out.note_sent(20, 1_001);
    assert_eq!(out.note_ack(10, 1_040), Some(40));
    assert_eq!(
        out.note_ack(20, 1_050),
        None,
        "the probe was already consumed"
    );
    assert_eq!((out.acked, out.sent), (20, 20));
}

#[test]
fn stale_ack_does_not_regress() {
    let mut out = Outbound::default();
    out.note_sent(10, 0);
    out.note_ack(10, 5);
    assert_eq!(out.note_ack(4, 6), None);
    assert_eq!(out.acked, 10);
}

#[test]
fn timeout_fires_only_with_data_in_flight() {
    let mut out = Outbound::default();
    assert!(!out.timed_out(10_000, 500));
    out.note_sent(5, 1_000);
    assert!(!out.timed_out(1_499, 500));
    assert!(out.timed_out(1_500, 500));
    out.note_ack(5, 1_200);
    assert!(!out.timed_out(9_000, 500));
}

#[test]
fn ack_progress_restarts_the_timer() {
    let mut out = Outbound::default();
    out.note_sent(5, 1_000);
    out.note_sent(9, 1_100);
    out.note_ack(5, 1_400);
    assert!(!out.timed_out(1_800, 500));
    assert!(out.timed_out(1_900, 500));
}

#[test]
fn rewind_resumes_from_the_last_ack_and_no_sample_spans_a_resend() {
    let mut out = Outbound::default();
    out.note_sent(5, 0);
    out.note_ack(3, 10);
    out.note_sent(9, 20);
    out.rewind(600);
    assert_eq!((out.acked, out.sent), (3, 3));
    out.note_sent(9, 601);
    assert_eq!(
        out.note_ack(9, 700),
        None,
        "an ack of resent data may answer the original send (Karn's rule)",
    );
    out.note_sent(12, 800);
    assert_eq!(out.note_ack(12, 850), Some(50), "new data is timed again");
}

#[test]
fn late_ack_after_rewind_lifts_sent() {
    let mut out = Outbound::default();
    out.note_sent(9, 0);
    out.rewind(600);
    out.note_ack(9, 610);
    assert_eq!((out.acked, out.sent), (9, 9));
}

#[test]
fn rto_is_twice_srtt_above_the_floor() {
    let mut p = peer("10.1.0.1:1");
    assert_eq!(p.rto_ms(500), 500);
    p.note_rtt(400);
    assert_eq!(p.rto_ms(500), 800);
    p.note_rtt(0);
    assert_eq!(p.rto_ms(500), 700, "the estimate decays by one eighth");
}

#[test]
fn one_gateway_per_remote_zone_and_none_locally() {
    let local = Zone::of(addr("10.0.0.1:1"), 16);
    let mut peers: Vec<Peer> = [
        "10.0.0.2:1",
        "10.0.0.3:1",
        "10.1.0.1:1",
        "10.1.0.2:1",
        "10.1.0.3:1",
        "10.2.0.1:1",
        "10.2.0.2:1",
    ]
    .into_iter()
    .map(peer)
    .collect();
    refresh_gateways(&mut peers, local);
    let gateways = |zone_prefix: &str| {
        peers
            .iter()
            .filter(|p| p.gateway && p.addr.to_string().starts_with(zone_prefix))
            .count()
    };
    assert_eq!(gateways("10.0."), 0);
    assert_eq!(gateways("10.1."), 1);
    assert_eq!(gateways("10.2."), 1);
}

#[test]
fn suspect_gateway_is_replaced_and_restored_when_unsuspected() {
    let local = Zone::of(addr("10.0.0.1:1"), 16);
    let mut peers: Vec<Peer> = ["10.1.0.1:1", "10.1.0.2:1", "10.1.0.3:1"]
        .into_iter()
        .map(peer)
        .collect();
    refresh_gateways(&mut peers, local);
    let first = peers.iter().position(|p| p.gateway).expect("gateway");
    peers[first].suspect = true;
    refresh_gateways(&mut peers, local);
    let second = peers.iter().position(|p| p.gateway).expect("gateway");
    assert_ne!(first, second);
    peers[first].suspect = false;
    refresh_gateways(&mut peers, local);
    assert_eq!(peers.iter().position(|p| p.gateway), Some(first));
}

#[test]
fn all_suspect_zone_keeps_a_gateway() {
    let local = Zone::of(addr("10.0.0.1:1"), 16);
    let mut peers: Vec<Peer> = ["10.1.0.1:1", "10.1.0.2:1"].into_iter().map(peer).collect();
    for p in &mut peers {
        p.suspect = true;
    }
    refresh_gateways(&mut peers, local);
    assert_eq!(peers.iter().filter(|p| p.gateway).count(), 1);
}

#[test]
fn a_gap_is_reported_after_its_hold_and_again_until_filled() {
    let mut inbound = Inbound::default();
    inbound.receive(0, 4, 100, 5);
    assert!(!inbound.gap(), "contiguous data opens no gap");
    inbound.receive(6, 9, 110, 5);
    assert_eq!(inbound.have, 4, "nothing past the gap is acked");
    assert!(inbound.gap());
    assert!(!inbound.report_due(114), "reordering gets the hold");
    assert!(inbound.report_due(115));
    inbound.reported(115, 20);
    assert!(!inbound.report_due(134));
    assert!(inbound.report_due(135), "an unanswered report repeats");
    inbound.receive(4, 9, 140, 5);
    assert_eq!(inbound.have, 9);
    assert!(!inbound.gap(), "the resend closes the gap");
}

#[test]
fn a_late_packet_from_below_closes_a_reordered_gap() {
    let mut inbound = Inbound::default();
    inbound.receive(10, 20, 100, 5);
    assert!(inbound.gap());
    inbound.receive(0, 10, 101, 5);
    assert_eq!(inbound.have, 20, "the stashed range joins what is held");
    assert!(!inbound.gap(), "reordering is not loss");
}

#[test]
fn a_gap_nobody_answers_is_dropped_after_its_reports() {
    let mut inbound = Inbound::default();
    inbound.receive(0, 4, 0, 5);
    inbound.receive(6, 9, 1, 5);
    let mut now = 6;
    for _ in 0..GOSSIP_GAP_REPORTS {
        assert!(inbound.gap());
        assert!(inbound.report_due(now));
        inbound.reported(now, 20);
        now += 20;
    }
    assert!(!inbound.gap(), "the sender never answered");
    assert_eq!(inbound.have, 4);
    assert_eq!(inbound.seen(), 4);
    inbound.receive(7, 12, now, 5);
    assert!(inbound.gap(), "a later packet beyond the hole reopens it");
}

#[test]
fn sweep_sources_are_the_top_ranked_members_of_the_zone() {
    let peers: Vec<Peer> = [
        "10.0.0.2:1",
        "10.0.0.3:1",
        "10.0.0.4:1",
        "10.0.0.5:1",
        "10.1.0.2:1",
        "10.1.0.3:1",
    ]
    .iter()
    .map(|s| peer(s))
    .collect();
    let zone = peers[0].zone;
    let mut ranked: Vec<usize> = (0..4).collect();
    ranked.sort_by_key(|&i| std::cmp::Reverse((peers[i].weight, peers[i].addr)));
    assert_eq!(
        sweep_sources(&peers, zone),
        [Some(ranked[0]), Some(ranked[1])]
    );
    assert_eq!(sweep_sources(&peers[..1], zone), [Some(0), None]);
    assert_eq!(sweep_sources(&peers[4..], zone), [None, None]);
}
