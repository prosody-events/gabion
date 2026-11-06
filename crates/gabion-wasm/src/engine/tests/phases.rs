//! Each node heartbeats at its own fixed phase; the timings the page narrates
//! are measured here into a committed record; and the configured network is
//! the one heal and every joiner use.

use serde_json::json;

use super::super::{MIN_DRAIN_YIELDS, SubStep, first_heartbeat, heartbeat_phase};
use super::*;
use crate::event::NodeState;

/// `gossip.suspect_after` at period `period`, as `Settings::validate`
/// derives it.
fn suspect_after(period: u64) -> u64 {
    (defaults::GOSSIP_SUSPECT_AFTER_TICKS * period).max(defaults::GOSSIP_SUSPECT_AFTER_FLOOR_MS)
}

/// Ids 0 to 11 get twelve distinct phases at every period from 50 to 1,000
/// ms, node 0's phase is 0, and at 500 ms they are the measured ones.
#[test]
fn heartbeat_phases_are_distinct_for_the_first_twelve_ids() {
    for period in (50..=1_000).step_by(50) {
        let phases: BTreeSet<u64> = (0..12).map(|id| heartbeat_phase(id, period)).collect();
        assert_eq!(phases.len(), 12, "at {period} ms: {phases:?}");
        assert!(phases.iter().all(|&phase| phase < period));
        assert_eq!(heartbeat_phase(0, period), 0);
    }
    let at_500: Vec<u64> = (0..12).map(|id| heartbeat_phase(id, 500)).collect();
    assert_eq!(
        at_500,
        [0, 309, 118, 427, 236, 45, 354, 163, 472, 281, 90, 399]
    );
    assert_eq!(first_heartbeat(0, 500, 0), 500, "node 0 first beats at T");
    assert_eq!(first_heartbeat(1, 500, 0), 309);
    assert_eq!(first_heartbeat(1, 500, 309), 809, "strictly after now");
}

/// With phases spread, an acknowledgement never loses the race with its
/// sender's retransmission timeout: no stream rewinds, in the Propagation
/// and the Rate limiting scripts, at a 500 ms and a 50 ms heartbeat.
#[test]
fn phased_heartbeats_never_rewind_a_stream() {
    for period in [500, 50] {
        let propagation = SimConfig {
            tick_interval_ms: period,
            ..propagation()
        };
        with_state(propagation, |mut state| async move {
            warm(&mut state).await;
            send(&mut state, 0, 50).await;
            settle(&mut state).await;
            send(&mut state, 0, 25).await;
            settle(&mut state).await;
            assert_no_rewinds(&state, period, "Propagation").await;
            state
        });
        let rate_limiting = SimConfig {
            tick_interval_ms: period,
            ..rate_limiting()
        };
        with_state(rate_limiting, |mut state| async move {
            ready(&mut state).await;
            for node in [0, 1, 2, 2, 2, 3] {
                send(&mut state, node, 25).await;
                settle(&mut state).await;
            }
            assert_no_rewinds(&state, period, "Rate limiting").await;
            state
        });
    }
}

async fn assert_no_rewinds(state: &EngineState, period: u64, script: &str) {
    for node in state.snapshot(None, WATCH).await.nodes {
        assert_eq!(
            (node.rto_rewinds_own, node.rto_rewinds_relay),
            (0, 0),
            "{script} at {period} ms: node {}",
            node.id
        );
    }
}

/// The due instants of a node with id `id`, spawned at `spawned`, up to and
/// including `now`.
fn due_instants(id: u32, spawned: u64, now: u64) -> u64 {
    let first = first_heartbeat(id, P, spawned);
    if first > now {
        0
    } else {
        (now - first) / P + 1
    }
}

/// A quiet minute costs one sub-step per due heartbeat, one counter read per
/// fired heartbeat, and the fewest yields per drain; every node, a joiner too,
/// runs exactly the heartbeats due since it spawned.
#[test]
fn quiet_cost_by_counters() {
    with_state(propagation(), |mut state| async move {
        ready(&mut state).await;
        state.counter_reads = 0;
        state.drain_yields = 0;
        state.substeps = 0;
        state.step(60_000, StepUntil::Time).await;
        assert_eq!(state.substeps, 12 * 120, "one sub-step per phase");
        assert_eq!(state.counter_reads, 12 * 120, "one read per heartbeat");
        assert_eq!(
            state.drain_yields,
            2 * MIN_DRAIN_YIELDS as u64 * state.substeps
        );

        let joined_at = state.virtual_ms;
        state.add_node(None).await.unwrap();
        let joiner = state.nodes.last().expect("the joiner").id;
        state.step(60_000, StepUntil::Time).await;
        let now = state.virtual_ms;
        for node in state.snapshot(None, WATCH).await.nodes {
            let spawned = if node.id == joiner { joined_at } else { 0 };
            assert_eq!(
                node.rounds_heartbeat,
                due_instants(node.id, spawned, now),
                "node {}",
                node.id
            );
        }
        state
    });
}

/// A wave's close reads the counters of its sender and of every node its
/// packets reached, and of no other node.
#[test]
fn a_wave_reads_its_sender_and_receivers() {
    with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        let held = send(&mut state, 0, 25).await;
        let mut read: BTreeSet<u32> = held
            .events
            .iter()
            .filter_map(|e| match e.kind {
                EventKind::PacketSent { dst, .. } => Some(dst),
                _ => None,
            })
            .collect();
        assert_eq!(read.len(), 5, "the eager flush reached five nodes");
        read.insert(0);
        let reads = state.counter_reads;
        state.step(0, StepUntil::Time).await;
        assert_eq!(state.counter_reads - reads, read.len() as u64);
        state
    });
}

/// One send's timeline, in ms after the send.
#[derive(Debug, Default)]
struct Timeline {
    /// When the node's own wave left.
    own_ms: u64,
    /// When the gateways' relays left.
    relay_ms: u64,
    /// When every node first held the hits.
    agreed_ms: u64,
    /// The last packet sent or lost before a whole period with none.
    quiet_ms: u64,
    /// Relays in the relay wave.
    relays: usize,
}

impl Timeline {
    fn record(&self) -> serde_json::Value {
        json!({
            "own_ms": self.own_ms,
            "relay_ms": self.relay_ms,
            "agreed_ms": self.agreed_ms,
            "quiet_ms": self.quiet_ms,
        })
    }
}

/// The last instant a packet was sent or lost in `events`, if any.
fn last_packet(events: &[Event]) -> Option<u64> {
    events
        .iter()
        .filter(|e| {
            matches!(
                e.kind,
                EventKind::PacketSent { .. } | EventKind::PacketDropped { .. }
            )
        })
        .map(|e| e.virtual_ms)
        .max()
}

/// How many `kind` packets from `src` in `events` were sent at `at`.
fn sent_at(events: &[Event], src: Option<u32>, kind: PacketKind, at: u64) -> usize {
    events
        .iter()
        .filter(|e| {
            e.virtual_ms == at
                && matches!(e.kind, EventKind::PacketSent { src: s, kind: k, .. }
                    if k == kind && src.is_none_or(|src| src == s))
        })
        .count()
}

/// One stop of stepping by event towards `to`: the events, with a held
/// wave delivered at its own instant, and whether the stop was a wave.
async fn next_stop(state: &mut EngineState, to: u64) -> (Vec<Event>, bool) {
    let mut events = state
        .step(to.saturating_sub(state.virtual_ms), StepUntil::Event)
        .await
        .events;
    let wave = state.sub_step == SubStep::Open;
    if wave {
        events.extend(state.step(0, StepUntil::Time).await.events);
    }
    (events, wave)
}

fn caught_up(snap: &ClusterState, keep: impl Fn(&NodeState) -> bool) -> bool {
    snap.nodes
        .iter()
        .filter(|n| keep(n))
        .all(|n| n.lag == Lag::CaughtUp)
}

/// Send `hits` at `node` now, and time its waves, its agreement and its
/// quiet point, stepping by event until every node holds the hits, then by
/// period until a period passes with no packet.
async fn timeline(state: &mut EngineState, node: u32, hits: u64) -> Timeline {
    let t0 = state.virtual_ms;
    let deadline = t0 + 40 * P;
    let mut timeline = Timeline::default();
    let mut own = None;
    let mut relay = None;
    let mut events = send(state, node, hits).await.events;
    if sent_at(&events, Some(node), PacketKind::Own, t0) > 0 {
        own = Some(t0);
    }
    events.extend(state.step(0, StepUntil::Time).await.events);
    let mut last = last_packet(&events);
    let agreed = loop {
        assert!(state.virtual_ms < deadline, "no agreement: {timeline:?}");
        if caught_up(&state.snapshot(None, WATCH).await, |_| true) {
            break state.virtual_ms;
        }
        let (events, wave) = next_stop(state, deadline).await;
        let at = state.virtual_ms;
        last = last_packet(&events).or(last);
        if wave && own.is_none() && sent_at(&events, Some(node), PacketKind::Own, at) > 0 {
            own = Some(at);
        } else if wave && own.is_some() && relay.is_none() {
            let relays = sent_at(&events, None, PacketKind::Relay, at);
            if relays > 0 {
                relay = Some(at);
                timeline.relays = relays;
            }
        }
    };
    loop {
        assert!(state.virtual_ms < deadline, "never quiet: {timeline:?}");
        let events = state.step(P, StepUntil::Time).await.events;
        match last_packet(&events) {
            Some(at) => last = Some(at),
            None => break,
        }
    }
    let since = |at: Option<u64>| at.map_or(0, |at| at - t0);
    timeline.own_ms = since(own);
    timeline.relay_ms = since(relay);
    timeline.agreed_ms = agreed - t0;
    timeline.quiet_ms = since(last);
    timeline
}

/// Time a send at each of the twelve nodes of Propagation, each on its own
/// cluster seeded with 50 hits at node 0.
fn propagation_record() -> serde_json::Value {
    let mut by_node = Vec::new();
    let mut send_at = 0;
    let mut relays = BTreeSet::new();
    for node in 0..12 {
        with_state(propagation(), |mut state| {
            let by_node = &mut by_node;
            let send_at = &mut send_at;
            let relays = &mut relays;
            async move {
                ready(&mut state).await;
                send(&mut state, 0, 50).await;
                settle(&mut state).await;
                let t0 = state.virtual_ms;
                *send_at = t0;
                let timeline = timeline(&mut state, node, 25).await;
                // Far from the limit, the wave leaves at the node's own
                // heartbeat, the relays one floor later, and the cluster
                // agrees when they land and is quiet within a period.
                assert_eq!(
                    timeline.own_ms,
                    first_heartbeat(node, P, t0) - t0,
                    "node {node}"
                );
                assert_eq!(timeline.relay_ms, timeline.own_ms + F_R, "node {node}");
                assert_eq!(timeline.agreed_ms, timeline.relay_ms, "node {node}");
                assert!(timeline.quiet_ms <= timeline.agreed_ms + P, "node {node}");
                relays.insert(timeline.relays);
                by_node.push(timeline.record());
                state
            }
        });
    }
    assert_eq!(relays.len(), 1, "every node's wave draws as many relays");
    json!({
        "send_at_ms": send_at,
        "by_node": by_node,
        "relays_per_own_round": relays.first(),
    })
}

/// Rate limiting: a send of 25 at node 0 flushes at once.
fn rate_limiting_record() -> serde_json::Value {
    run_with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        let record = timeline(&mut state, 0, 25).await.record();
        (state, record)
    })
}

/// Packet loss: a send of 25 at node 0 with 30 % of packets lost.
fn packet_loss_record() -> serde_json::Value {
    let lossy = SimConfig {
        uniform_loss: 0.3,
        ..propagation()
    };
    run_with_state(lossy, |mut state| async move {
        ready(&mut state).await;
        let timeline = timeline(&mut state, 0, 25).await;
        let record = json!({
            "agreed_ms": timeline.agreed_ms,
            "quiet_ms": timeline.quiet_ms,
        });
        (state, record)
    })
}

/// A gateway fault, in ms after it: when node 0 left its gateway into zone b,
/// and when the rest of zone b first held every hit.
#[derive(Debug, Default)]
struct Failover {
    failover_ms: Option<u64>,
    zone_caught_up_ms: Option<u64>,
}

impl Failover {
    fn record(&self) -> serde_json::Value {
        json!({
            "failover_ms": self.failover_ms,
            "zone_caught_up_ms": self.zone_caught_up_ms,
        })
    }
}

/// Step by event from the fault at `t0`, sending a one-hit request at node 0
/// at each instant `feed` yields, until node 0 has left `g` and the rest of
/// zone b has every hit.
async fn fail_over(
    state: &mut EngineState,
    g: u32,
    t0: u64,
    feed: impl Fn(u64) -> Option<u64>,
) -> Failover {
    let deadline = t0 + suspect_after(P) + 20 * P;
    let mut out = Failover::default();
    let mut request = 0;
    while out.zone_caught_up_ms.is_none() {
        assert!(state.virtual_ms < deadline, "no repair: {out:?}");
        let next = feed(request).unwrap_or(deadline).min(deadline);
        let events = if state.virtual_ms == next {
            request += 1;
            let mut events = send(state, 0, 1).await.events;
            events.extend(state.step(0, StepUntil::Time).await.events);
            events
        } else {
            next_stop(state, next).await.0
        };
        if out.failover_ms.is_none() {
            out.failover_ms = failover(&events, 0, 1, g).map(|(at, _)| at - t0);
        }
        if out.failover_ms.is_some()
            && caught_up(&state.snapshot(None, WATCH).await, |n| {
                n.zone == 1 && n.id != g
            })
        {
            out.zone_caught_up_ms = Some(state.virtual_ms - t0);
        }
    }
    out
}

/// Gateway failure: node 0's gateway into zone b silenced, then 25 hits.
fn gateway_failure_record() -> serde_json::Value {
    run_with_state(propagation(), |mut state| async move {
        ready(&mut state).await;
        let g = gateway_of(&state, 0, 1).await;
        silence(&mut state, g).await;
        let t0 = state.virtual_ms;
        send(&mut state, 0, 25).await;
        let repair = fail_over(&mut state, g, t0, |_| None).await;
        let failover_ms = repair.failover_ms.expect("a failover");
        assert!(
            failover_ms <= suspect_after(P) + 2 * P,
            "failed over {failover_ms} ms after the silence"
        );
        (state, repair.record())
    })
}

/// Gateway cut: node 0's gateway into zone b cut off from its zone peers, one
/// way, with a trickle of one hit every 500 ms at node 0.
fn gateway_cut_record() -> serde_json::Value {
    run_with_state(propagation(), |mut state| async move {
        ready(&mut state).await;
        let g = gateway_of(&state, 0, 1).await;
        for m in members(&state, 1).into_iter().filter(|&m| m != g) {
            state
                .set_link_policy(g, m, LinkPolicyKind::Block)
                .await
                .unwrap();
        }
        let t0 = state.virtual_ms;
        let repair = fail_over(&mut state, g, t0, |i| Some(t0 + 500 * i)).await;
        (state, repair.record())
    })
}

/// A fed cluster: 10 hits at the first node of each zone, then the i-th
/// one-hit request at `⌈i · 1000 / 3⌉` ms, at node i mod 12. The feed is the
/// page's carry-save feed idealized to exact instants, so a page check reads
/// statistics from these numbers, not frame-exact times. Returns the data
/// waves per period over 10 s to 30 s and the share of data bytes that
/// crossed a zone over 20 s to 30 s.
fn fed_record(config: SimConfig) -> (f64, f64) {
    run_with_state(config, |mut state| async move {
        ready(&mut state).await;
        for node in [0, 4, 8] {
            send(&mut state, node, 10).await;
        }
        let t0 = state.virtual_ms;
        let (count_from, share_from, end) = (t0 + 10_000, t0 + 20_000, t0 + 30_000);
        let mut waves = 0u32;
        let mut at_share_from = None;
        let mut request = 0u64;
        while state.virtual_ms < end {
            let next_request = t0 + (request * 1_000).div_ceil(3);
            if state.virtual_ms == next_request {
                send(&mut state, (request % 12) as u32, 1).await;
                request += 1;
                continue;
            }
            let mut to = next_request.min(end);
            if at_share_from.is_none() {
                to = to.min(share_from);
            }
            let (_, wave) = next_stop(&mut state, to).await;
            if wave && (count_from..=end).contains(&state.virtual_ms) {
                waves += 1;
            }
            if at_share_from.is_none() && state.virtual_ms == share_from {
                at_share_from = Some(state.snapshot(None, WATCH).await.bytes_by_tier);
            }
        }
        let from = at_share_from.expect("the run passed 20 s");
        let to = state.snapshot(None, WATCH).await.bytes_by_tier;
        let cross = to.cross_zone.data - from.cross_zone.data;
        let all = cross + to.in_zone.data - from.in_zone.data;
        let periods = (end - count_from) / P;
        let record = (f64::from(waves) / periods as f64, cross as f64 / all as f64);
        (state, record)
    })
}

/// Four decimal places, so the record reads and diffs cleanly.
fn rounded(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

/// The wave, agreement, quiet and failover times the page narrates, measured
/// from the engine and checked against the protocol's bounds, equal the
/// committed `web/tests/phase-timings.json`. `UPDATE_PHASE_TIMINGS=1`
/// rewrites the record after the bounds pass, so a drift fails here and shows
/// in review.
#[test]
fn phase_timings_match_the_record() {
    let (steady_waves, steady_share) = fed_record(propagation());
    let (no_zone_waves, no_zone_share) = fed_record(SimConfig {
        zone_mode: ZoneMode::Unset,
        ..propagation()
    });
    let record = json!({
        "heartbeat_ms": P,
        "floor_ms": F_R,
        "suspect_after_ms": suspect_after(P),
        "propagation": propagation_record(),
        "rate_limiting": rate_limiting_record(),
        "packet_loss": packet_loss_record(),
        "gateway_failure": gateway_failure_record(),
        "gateway_cut": gateway_cut_record(),
        "data_waves_per_period": {
            "steady_state": rounded(steady_waves),
            "no_zone": rounded(no_zone_waves),
        },
        "cross_zone_share_at_30s": {
            "steady_state": rounded(steady_share),
            "no_zone": rounded(no_zone_share),
        },
    });
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/web/tests/phase-timings.json");
    if std::env::var_os("UPDATE_PHASE_TIMINGS").is_some_and(|v| v == "1") {
        let text = serde_json::to_string_pretty(&record).expect("a JSON value serializes") + "\n";
        std::fs::write(path, text).expect("write the timing record");
        return;
    }
    let committed: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("the committed timing record"))
            .expect("the timing record is JSON");
    assert_eq!(
        record, committed,
        "the engine's timings moved; rerun with UPDATE_PHASE_TIMINGS=1 and review the diff"
    );
}

/// Heal restores the configured network, loss included, and a joiner's links
/// carry it too.
#[test]
fn heal_restores_the_configured_loss() {
    let lossy = SimConfig {
        uniform_loss: 0.3,
        ..propagation()
    };
    with_state(lossy, |mut state| async move {
        warm(&mut state).await;
        cut_zone(&mut state, 2).await;
        state.heal().await;
        let rate = drop_rate(&mut state, |_, _| true).await;
        assert!(
            (0.2..=0.4).contains(&rate),
            "{rate} of packets lost after heal"
        );

        state.add_node(None).await.unwrap();
        let joiner = state.nodes.last().expect("the joiner").id;
        let rate = drop_rate(&mut state, |src, dst| src == joiner || dst == joiner).await;
        assert!(
            (0.2..=0.4).contains(&rate),
            "{rate} of the joiner's packets lost"
        );
        state
    });
}

/// Two hundred one-hit sends, one per node in turn, each followed by a
/// period: the share of the packets `on` picks that were lost.
async fn drop_rate(state: &mut EngineState, on: impl Fn(u32, u32) -> bool) -> f64 {
    let ids: Vec<u32> = state.nodes.iter().map(|n| n.id).collect();
    let (mut sent, mut dropped) = (0u32, 0u32);
    for i in 0..200 {
        let mut events = send(state, ids[i % ids.len()], 1).await.events;
        events.extend(state.step(P, StepUntil::Time).await.events);
        for event in events {
            match event.kind {
                EventKind::PacketSent { src, dst, .. } if on(src, dst) => sent += 1,
                EventKind::PacketDropped { src, dst, .. } if on(src, dst) => dropped += 1,
                _ => {}
            }
        }
    }
    f64::from(dropped) / f64::from(sent + dropped)
}
