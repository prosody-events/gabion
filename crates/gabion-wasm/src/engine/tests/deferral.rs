//! Stepping by event stops before each wave lands, with the wave held until
//! the next engine call, and at the window's edge; an admitted send's eager
//! flush lands before the submit returns, unless the send asked to stop by
//! event.

use super::super::{SubStep, first_heartbeat};
use super::*;

/// The `kind` packets sent in `events`, as `(src, dst, tier, virtual_ms)`.
fn packets(events: &[Event], kind: PacketKind) -> Vec<(u32, u32, Tier, u64)> {
    events
        .iter()
        .filter_map(|e| match e.kind {
            EventKind::PacketSent {
                src,
                dst,
                kind: k,
                tier,
                ..
            } if k == kind => Some((src, dst, tier, e.virtual_ms)),
            _ => None,
        })
        .collect()
}

/// The destinations of `packets`.
fn destinations(packets: &[(u32, u32, Tier, u64)]) -> BTreeSet<u32> {
    packets.iter().map(|p| p.1).collect()
}

/// An admitted send's eager flush lands before the submit returns: every node
/// it reached has counted the hits at the send's instant, no other has, and
/// stepping by event then stops with the relays held.
#[test]
fn a_sends_eager_flush_lands_before_the_submit_returns() {
    with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        let at = state.virtual_ms;
        let submitted = send(&mut state, 0, 25).await;
        assert!(matches!(
            decisions(&submitted.events)[..],
            [(0, Admission::Admitted { total_before: 0 })]
        ));
        assert!(submitted.events.iter().any(|e| matches!(
            e.kind,
            EventKind::CellCreated {
                node: 0,
                count: 25,
                ..
            }
        )));
        let own = packets(&submitted.events, PacketKind::Own);
        assert_eq!(own.len(), 5, "{own:?}");
        assert!(own.iter().all(|p| p.0 == 0));
        assert_eq!(own.iter().filter(|p| p.2 == Tier::CrossZone).count(), 2);
        assert!(matches!(state.sub_step, SubStep::Closed));
        let mut counted = destinations(&own);
        counted.insert(0);
        assert_eq!(cell_nodes(&submitted.events), counted);
        assert!(submitted.events.iter().all(|e| e.virtual_ms == at));

        let snap = state.snapshot(None, WATCH).await;
        for node in &snap.nodes {
            let want = if counted.contains(&node.id) { 25 } else { 0 };
            assert_eq!(node.window_total, want, "node {}", node.id);
        }

        let relayed = state.step(1_000, EVENT).await;
        assert_eq!(relayed.virtual_ms, at + F_R);
        let relays = packets(&relayed.events, PacketKind::Relay);
        assert_eq!(relays.len(), 6, "{relays:?}");
        assert!(
            cell_nodes(&relayed.events).is_disjoint(&destinations(&relays)),
            "the relays have not landed"
        );
        assert!(matches!(state.sub_step, SubStep::Open(_)));
        state
    });
}

/// A send by event holds its eager flush as stepping by event holds a wave:
/// its packets are in the batch, only the sender has counted the hits, and the
/// next command delivers them to the nodes the flush reached.
#[test]
fn a_send_by_event_holds_its_eager_flush_until_the_next_command() {
    with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        let at = state.virtual_ms;
        let submitted = state
            .submit_request(0, KeyHash(WATCHED_KEY), hits(25), EVENT)
            .await
            .expect("a live node");
        let own = packets(&submitted.events, PacketKind::Own);
        assert_eq!(own.len(), 5, "{own:?}");
        assert!(matches!(state.sub_step, SubStep::Open(_)));
        assert_eq!(cell_nodes(&submitted.events), BTreeSet::from([0]));
        let snap = state.snapshot(None, WATCH).await;
        for node in &snap.nodes {
            let want = if node.id == 0 { 25 } else { 0 };
            assert_eq!(node.window_total, want, "held: node {}", node.id);
        }

        let delivered = state.step(0, StepUntil::Time).await;
        assert_eq!(delivered.virtual_ms, at);
        assert!(matches!(state.sub_step, SubStep::Closed));
        assert_eq!(cell_nodes(&delivered.events), destinations(&own));
        let snap = state.snapshot(None, WATCH).await;
        for node in &snap.nodes {
            let want = if node.id == 0 || destinations(&own).contains(&node.id) {
                25
            } else {
                0
            };
            assert_eq!(node.window_total, want, "delivered: node {}", node.id);
        }
        state
    });
}

/// One run's flattened events and final snapshot.
type Run = (Vec<Event>, serde_json::Value);

/// A command run while the relays of node 0's send are held.
#[derive(Clone, Copy, Debug)]
enum HeldCommand {
    Submit,
    AddNode,
    RemoveNode,
    Cut,
    Heal,
}

impl HeldCommand {
    async fn run(self, state: &mut EngineState) -> Vec<Event> {
        match self {
            Self::Submit => send(state, 3, 25).await.events,
            Self::AddNode => state.add_node(None).await.unwrap().events,
            Self::RemoveNode => state.remove_node(3).await.unwrap().events,
            Self::Cut => {
                state
                    .set_link_policy(0, 4, LinkPolicyKind::Block)
                    .await
                    .unwrap();
                Vec::new()
            }
            Self::Heal => {
                state.heal().await;
                Vec::new()
            }
        }
    }
}

/// Every command closes an open sub-step first: running it while the
/// gateways' relays of node 0's send are held gives the snapshot right after
/// it, and the events, of running it after `step(0)` delivered the relays.
#[test]
fn every_command_closes_an_open_sub_step_first() {
    fn run(command: HeldCommand, close_first: bool) -> Run {
        run_with_state(rate_limiting(), |mut state| async move {
            ready(&mut state).await;
            let mut events = send(&mut state, 0, 25).await.events;
            events.extend(state.step(1_000, EVENT).await.events);
            assert!(
                matches!(state.sub_step, SubStep::Open(_)),
                "the relays are held"
            );
            if close_first {
                events.extend(state.step(0, StepUntil::Time).await.events);
            }
            events.extend(command.run(&mut state).await);
            let snap = state.snapshot(Some(0), WATCH).await;
            events.extend(state.step(0, StepUntil::Time).await.events);
            let run = (events, serde_json::to_value(&snap).unwrap());
            (state, run)
        })
    }
    for command in [
        HeldCommand::Submit,
        HeldCommand::AddNode,
        HeldCommand::RemoveNode,
        HeldCommand::Cut,
        HeldCommand::Heal,
    ] {
        let (held, held_snap) = run(command, false);
        let (closed, closed_snap) = run(command, true);
        assert_eq!(held, closed, "{command:?}: events");
        assert_eq!(held_snap, closed_snap, "{command:?}: snapshot");
    }
}

/// The script of the equivalence test: two sends, 700 ms apart, and time to
/// 12 s after the first, past its window, stepped by `until`; and how many
/// steps returned with a wave held.
fn stepped(until: StepUntil) -> (Run, usize) {
    run_with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        let t0 = state.virtual_ms;
        let mut events = Vec::new();
        let mut holds = 0;
        for (node, to) in [(0, t0 + 700), (1, t0 + 12_000)] {
            events.extend(send(&mut state, node, 25).await.events);
            while state.virtual_ms < to {
                let (from, delta) = (state.virtual_ms, to - state.virtual_ms);
                events.extend(state.step(delta, until).await.events);
                assert!(state.virtual_ms > from, "a step from {from} stood still");
                holds += usize::from(matches!(state.sub_step, SubStep::Open(_)));
            }
            // A wave at `to` itself is still held: deliver it.
            events.extend(state.step(0, StepUntil::Time).await.events);
        }
        let snap = state.snapshot(Some(1), WATCH).await;
        let run = (events, serde_json::to_value(&snap).unwrap());
        (state, (run, holds))
    })
}

/// Stepping by event to an instant, call after call, stops at each wave and
/// still gives exactly the events and the snapshot of stepping there by time
/// in one call.
#[test]
fn event_stepping_equals_time_stepping() {
    let ((by_event, event_snap), holds) = stepped(EVENT);
    let ((by_time, time_snap), time_holds) = stepped(StepUntil::Time);
    assert_eq!(holds, 2, "stepping by event stops at each send's relays");
    assert_eq!(time_holds, 0, "stepping by time holds no wave");
    assert_eq!(by_event, by_time);
    assert_eq!(event_snap, time_snap);
}

/// Far from the limit a send waits for its node's heartbeat. Stepping by
/// event stops before its wave lands, then before the relays land, runs a
/// bounded step through the acknowledgements, and stops at the instant the
/// hits leave the window. The heartbeat period does not divide the 1 s
/// bucket, so no heartbeat is due at the window's edge and only the expiry
/// can end the last step there.
#[test]
fn event_stops_at_each_wave_and_at_the_window_edge() {
    const PERIOD: u64 = 700;
    let config = SimConfig {
        tick_interval_ms: PERIOD,
        ..propagation()
    };
    with_state(config, |mut state| async move {
        ready(&mut state).await;
        let t0 = state.virtual_ms;
        send(&mut state, 0, 25).await;

        let own = state.step(2_000, EVENT).await;
        let wave = first_heartbeat(0, PERIOD, t0);
        assert_eq!(own.virtual_ms, wave);
        let sent = packets(&own.events, PacketKind::Own);
        assert_eq!(sent.len(), 5, "{sent:?}");
        assert_eq!(sent.iter().filter(|p| p.2 == Tier::CrossZone).count(), 2);
        assert!(cell_nodes(&own.events).is_empty(), "nothing has landed");
        assert!(matches!(state.sub_step, SubStep::Open(_)));

        let relay = state.step(2_000, EVENT).await;
        let landed: Vec<_> = relay
            .events
            .iter()
            .filter(|e| e.virtual_ms == wave)
            .cloned()
            .collect();
        assert_eq!(cell_nodes(&landed), destinations(&sent));
        let relays = packets(&relay.events, PacketKind::Relay);
        assert_eq!(relays.len(), 6, "{relays:?}");
        assert!(relays.iter().all(|p| p.3 == wave + F_R));
        assert_eq!(relay.virtual_ms, wave + F_R);
        assert!(relay.events.iter().all(|e| e.virtual_ms <= wave + F_R));
        assert!(matches!(state.sub_step, SubStep::Open(_)));

        let acks = state.step(PERIOD, EVENT).await;
        assert_eq!(acks.virtual_ms, wave + F_R + PERIOD, "bounded by its delta");
        let relayed: Vec<_> = acks
            .events
            .iter()
            .filter(|e| e.virtual_ms == wave + F_R)
            .cloned()
            .collect();
        assert_eq!(cell_nodes(&relayed), destinations(&relays));
        assert!(matches!(state.sub_step, SubStep::Closed));

        let edge_ms = (t0 / 1_000 + 10) * 1_000;
        assert!(
            state
                .nodes
                .iter()
                .all(|n| first_heartbeat(n.id, PERIOD, edge_ms - 1) != edge_ms),
            "a heartbeat is due at the window's edge {edge_ms}"
        );
        let edge = state.step(15_000, EVENT).await;
        assert_eq!(edge.virtual_ms, edge_ms);
        for kind in [PacketKind::Own, PacketKind::Relay, PacketKind::Sweep] {
            assert!(packets(&edge.events, kind).is_empty(), "no data moves");
        }
        let snap = state.snapshot(None, WATCH).await;
        assert_eq!(snap.next_expiry_ms, None);
        assert!(snap.nodes.iter().all(|n| n.window_total == 0));
        state
    });
}
