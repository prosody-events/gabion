//! A wave's packets stay in flight until the next engine call, and stepping
//! by event stops before each wave lands and at the window's edge.

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

/// An admitted send's eager flush is held: only its node has counted it until
/// the next command delivers the wave at the same instant, and stepping by
/// event then stops with the relays held.
#[test]
fn a_send_is_held_until_the_next_engine_call() {
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
        assert_eq!(state.sub_step, SubStep::Open);

        let held = state.snapshot(None, WATCH).await;
        let views: Vec<u64> = held.nodes.iter().map(|n| n.window_total).collect();
        assert_eq!(views[0], 25);
        assert!(views[1..].iter().all(|&v| v == 0), "{views:?}");

        let landed = state.step(0, StepUntil::Time).await;
        assert_eq!(cell_nodes(&landed.events), destinations(&own));
        assert!(landed.events.iter().all(|e| e.virtual_ms == at));

        let relayed = state.step(1_000, StepUntil::Event).await;
        assert_eq!(relayed.virtual_ms, at + F_R);
        let relays = packets(&relayed.events, PacketKind::Relay);
        assert_eq!(relays.len(), 6, "{relays:?}");
        assert!(
            cell_nodes(&relayed.events).is_disjoint(&destinations(&relays)),
            "the relays have not landed"
        );
        assert_eq!(state.sub_step, SubStep::Open);
        state
    });
}

/// One run's flattened events and final snapshot.
type Run = (Vec<Event>, serde_json::Value);

/// A command run while node 0's send is held.
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

/// Every command closes an open sub-step first: running it while node 0's
/// send is held gives the snapshot right after it, and the events, of running
/// it after `step(0)` delivered the send.
#[test]
fn every_command_closes_an_open_sub_step_first() {
    fn run(command: HeldCommand, close_first: bool) -> Run {
        run_with_state(rate_limiting(), |mut state| async move {
            ready(&mut state).await;
            let mut events = send(&mut state, 0, 25).await.events;
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
/// 12 s after the first, past its window, stepped by `until`.
fn stepped(until: StepUntil) -> Run {
    run_with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        let t0 = state.virtual_ms;
        let mut events = Vec::new();
        for (node, to) in [(0, t0 + 700), (1, t0 + 12_000)] {
            events.extend(send(&mut state, node, 25).await.events);
            while state.virtual_ms < to {
                let delta = to - state.virtual_ms;
                events.extend(state.step(delta, until).await.events);
            }
            // A wave at `to` itself is still held: deliver it.
            events.extend(state.step(0, StepUntil::Time).await.events);
        }
        let snap = state.snapshot(Some(1), WATCH).await;
        let run = (events, serde_json::to_value(&snap).unwrap());
        (state, run)
    })
}

/// Stepping by event to an instant, call after call, gives exactly the
/// events and the snapshot of stepping there by time in one call.
#[test]
fn event_stepping_equals_time_stepping() {
    let (by_event, event_snap) = stepped(StepUntil::Event);
    let (by_time, time_snap) = stepped(StepUntil::Time);
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

        let own = state.step(2_000, StepUntil::Event).await;
        let wave = first_heartbeat(0, PERIOD, t0);
        assert_eq!(own.virtual_ms, wave);
        let sent = packets(&own.events, PacketKind::Own);
        assert_eq!(sent.len(), 5, "{sent:?}");
        assert_eq!(sent.iter().filter(|p| p.2 == Tier::CrossZone).count(), 2);
        assert!(cell_nodes(&own.events).is_empty(), "nothing has landed");
        assert_eq!(state.sub_step, SubStep::Open);

        let relay = state.step(2_000, StepUntil::Event).await;
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
        assert_eq!(state.sub_step, SubStep::Open);

        let acks = state.step(PERIOD, StepUntil::Event).await;
        assert_eq!(acks.virtual_ms, wave + F_R + PERIOD, "bounded by its delta");
        let relayed: Vec<_> = acks
            .events
            .iter()
            .filter(|e| e.virtual_ms == wave + F_R)
            .cloned()
            .collect();
        assert_eq!(cell_nodes(&relayed), destinations(&relays));
        assert_eq!(state.sub_step, SubStep::Closed);

        let edge_ms = (t0 / 1_000 + 10) * 1_000;
        assert!(
            state
                .nodes
                .iter()
                .all(|n| first_heartbeat(n.id, PERIOD, edge_ms - 1) != edge_ms),
            "a heartbeat is due at the window's edge {edge_ms}"
        );
        let edge = state.step(15_000, StepUntil::Event).await;
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
