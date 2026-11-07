//! Each node's view is gabion's admission window, and each node's lag says
//! whether waiting can still help.

use super::super::NodeSet;
use super::*;

/// Every node's `window_total`.
fn views(state: &ClusterState) -> Vec<u64> {
    state.nodes.iter().map(|n| n.window_total).collect()
}

/// A bucket leaves every node's view, and the truth, at its fall-off
/// `(b + 10) · 1000` and not a millisecond before.
#[test]
fn window_total_drops_a_bucket_at_its_fall_off() {
    let config = SimConfig {
        rule_limit: 1_000,
        ..rate_limiting()
    };
    with_state(config, |mut state| async move {
        ready(&mut state).await;
        let t0 = state.virtual_ms;
        send(&mut state, 0, 25).await;
        let fall_off = (t0 / 1_000 + 10) * 1_000;
        settle(&mut state).await;
        while state.virtual_ms + 100 < fall_off {
            state.step(100, StepUntil::Time).await;
            let snap = state.snapshot(None, WATCH).await;
            let at = snap.virtual_ms;
            assert_eq!(views(&snap), [25; 12], "at {at}");
            // Oldest first: the send's bucket moves one slot left each second.
            let age = (at / 1_000 - t0 / 1_000) as usize;
            let buckets = &snap.oracle_buckets;
            assert_eq!(buckets[buckets.len() - 1 - age], 25, "at {at}: {buckets:?}");
            assert_eq!(buckets.iter().sum::<u64>(), 25, "at {at}: {buckets:?}");
        }
        state
            .step(fall_off - 1 - state.virtual_ms, StepUntil::Time)
            .await;
        assert_eq!(views(&state.snapshot(None, WATCH).await), [25; 12]);
        state.step(1, StepUntil::Time).await;
        let snap = state.snapshot(None, WATCH).await;
        assert_eq!(snap.virtual_ms, fall_off);
        assert_eq!(views(&snap), [0; 12]);
        assert_eq!(snap.oracle_total, 0);
        state
    });
}

/// Across an expiry, at every 5 ms, every node's view equals the windowed
/// truth and every node is caught up: a bucket leaves both at one instant,
/// whatever each node's heartbeat phase.
#[test]
fn views_equal_the_windowed_truth_across_an_expiry() {
    with_state(rate_limiting(), |mut state| async move {
        ready(&mut state).await;
        let fall_off = (state.virtual_ms / 1_000 + 10) * 1_000;
        for node in [0, 1, 2, 2] {
            send(&mut state, node, 25).await;
            settle(&mut state).await;
        }
        state
            .step(fall_off - 600 - state.virtual_ms, StepUntil::Time)
            .await;
        let mut dropped = false;
        while state.virtual_ms < fall_off + 600 {
            state.step(5, StepUntil::Time).await;
            let snap = state.snapshot(None, WATCH).await;
            let at = snap.virtual_ms;
            assert_eq!(snap.oracle_buckets.iter().sum::<u64>(), snap.oracle_total);
            let rows = snap.window_buckets.chunks_exact(snap.oracle_buckets.len());
            assert_eq!(rows.len(), snap.nodes.len());
            for (node, row) in snap.nodes.iter().zip(rows) {
                assert_eq!(
                    node.window_total, snap.oracle_total,
                    "node {} at {at}",
                    node.id
                );
                assert_eq!(row, snap.oracle_buckets, "node {} at {at}", node.id);
                assert_eq!(node.lag, Lag::CaughtUp, "node {} at {at}", node.id);
            }
            dropped |= snap.oracle_total < 100;
        }
        assert!(dropped, "the first send left the window");
        state
    });
}

/// Every node's lag in `zone`, other than `except`.
fn lags(snap: &ClusterState, zone: u8, except: Option<u32>) -> Vec<Lag> {
    snap.nodes
        .iter()
        .filter(|n| n.zone == zone && Some(n.id) != except)
        .map(|n| n.lag)
        .collect()
}

fn stranded(lag: Lag) -> bool {
    matches!(lag, Lag::Stranded { .. })
}

fn behind(lag: Lag) -> bool {
    matches!(lag, Lag::Behind { .. })
}

/// Step one period at a time until `done` holds of the snapshot, checking
/// `always` of every snapshot on the way, within `periods` periods.
async fn step_until(
    state: &mut EngineState,
    periods: u64,
    mut always: impl FnMut(&ClusterState),
    done: impl Fn(&ClusterState) -> bool,
) -> ClusterState {
    for _ in 0..periods {
        state.step(P, StepUntil::Time).await;
        let snap = state.snapshot(None, WATCH).await;
        always(&snap);
        if done(&snap) {
            return snap;
        }
    }
    panic!("not within {periods} periods");
}

/// Zone c cut off: a send at node 0 leaves zone c stranded while the rest
/// catch up.
async fn zone_c_stays_stranded(state: &mut EngineState) {
    send(state, 0, 25).await;
    let zone_c_stranded = |snap: &ClusterState| {
        assert!(lags(snap, 2, None).into_iter().all(stranded), "{snap:?}");
    };
    step_until(state, 4, zone_c_stranded, |snap| {
        [0, 1]
            .iter()
            .all(|&z| lags(snap, z, None).iter().all(|l| *l == Lag::CaughtUp))
    })
    .await;
}

/// A node that lacks a change is behind when some holder still reaches it
/// over links that do not block, and stranded otherwise: under a zone cut,
/// a silenced gateway, a one-way cut, partial loss, a removal that shifts
/// positions, and certain loss.
#[test]
fn lag_tells_behind_from_stranded() {
    // (a) Zone c cut off: zone c is stranded, the rest catch up.
    with_state(propagation(), |mut state| async move {
        ready(&mut state).await;
        cut_zone(&mut state, 2).await;
        zone_c_stays_stranded(&mut state).await;
        state
    });

    // (b) The gateway into zone b silenced: zone b is behind until node 0
    // fails over, the gateway stranded throughout.
    with_state(propagation(), |mut state| async move {
        ready(&mut state).await;
        let g = gateway_of(&state, 0, 1).await;
        silence(&mut state, g).await;
        send(&mut state, 0, 25).await;
        let deadline = state.virtual_ms + T_U + 3 * P + 2 * F_R;
        let mut failed_over = false;
        while !failed_over {
            assert!(state.virtual_ms < deadline, "no failover");
            let events = state.step(P, StepUntil::Time).await.events;
            failed_over = failover(&events, 0, 1, g).is_some();
            let snap = state.snapshot(None, WATCH).await;
            assert!(stranded(snap.nodes[position(&snap, g)].lag));
            if !failed_over {
                assert!(lags(&snap, 1, Some(g)).into_iter().all(behind), "{snap:?}");
            }
        }
        let g_stranded = |snap: &ClusterState| {
            assert!(stranded(snap.nodes[position(snap, g)].lag));
        };
        step_until(&mut state, 4, g_stranded, |snap| {
            lags(snap, 1, Some(g)).iter().all(|l| *l == Lag::CaughtUp)
        })
        .await;
        state
    });

    // (c) The gateway cut from its zone peers, one way: they are behind, it
    // is caught up.
    with_state(propagation(), |mut state| async move {
        ready(&mut state).await;
        let g = gateway_of(&state, 0, 1).await;
        for m in members(&state, 1).into_iter().filter(|&m| m != g) {
            state
                .set_link_policy(g, m, LinkPolicyKind::Block)
                .await
                .unwrap();
        }
        send(&mut state, 0, 25).await;
        let snap = step_until(
            &mut state,
            2,
            |_| {},
            |snap| snap.nodes[position(snap, g)].lag == Lag::CaughtUp,
        )
        .await;
        assert!(lags(&snap, 1, Some(g)).into_iter().all(behind), "{snap:?}");
        state
    });

    // (d) Loss of 0.3 never strands anyone, and waiting helps: every node
    // catches up.
    let lossy = SimConfig {
        uniform_loss: 0.3,
        ..propagation()
    };
    with_state(lossy, |mut state| async move {
        ready(&mut state).await;
        send(&mut state, 0, 25).await;
        let no_one_stranded = |snap: &ClusterState| {
            assert!(!snap.nodes.iter().any(|n| stranded(n.lag)), "{snap:?}");
        };
        step_until(&mut state, 16, no_one_stranded, |snap| {
            snap.nodes.iter().all(|n| n.lag == Lag::CaughtUp)
        })
        .await;
        state
    });

    // (e) Zone c cut, then a zone-b node removed, which moves every higher
    // position down by one: zone c stays stranded, the rest catch up.
    with_state(propagation(), |mut state| async move {
        ready(&mut state).await;
        cut_zone(&mut state, 2).await;
        state.remove_node(1).await.unwrap();
        zone_c_stays_stranded(&mut state).await;
        state
    });

    // (f) Certain loss blocks as a cut does: every other node is stranded.
    let lost = SimConfig {
        uniform_loss: 1.0,
        ..propagation()
    };
    with_state(lost, |mut state| async move {
        warm(&mut state).await;
        send(&mut state, 0, 25).await;
        state.step(P, StepUntil::Time).await;
        let snap = state.snapshot(None, WATCH).await;
        assert_eq!(snap.nodes[0].lag, Lag::CaughtUp);
        assert!(snap.nodes[1..].iter().all(|n| stranded(n.lag)), "{snap:?}");
        state
    });
}

/// A departed node's hits still count while a survivor holds them: the truth
/// stays with the views, and a lag still sees the cut its change cannot
/// cross. Hits that left with their node before any replica did are in no
/// window, so no expiry is due for them, and another key's hits move neither
/// the watched key's expiry nor where stepping by event stops.
#[test]
fn a_departed_origins_hits_count_while_survivors_hold_them() {
    // (a) Replicated, then removed: the truth equals every survivor's view.
    with_state(propagation(), |mut state| async move {
        ready(&mut state).await;
        send(&mut state, 0, 25).await;
        step_until(
            &mut state,
            8,
            |_| {},
            |snap| snap.nodes.iter().all(|n| n.lag == Lag::CaughtUp),
        )
        .await;
        state.remove_node(0).await.unwrap();
        let snap = state.snapshot(None, WATCH).await;
        assert_eq!(snap.oracle_total, 25);
        assert_eq!(views(&snap), [25; 11]);
        assert!(
            snap.nodes.iter().all(|n| n.lag == Lag::CaughtUp),
            "{snap:?}"
        );
        assert!(snap.next_expiry_ms.is_some());
        state
    });

    // (b) Zone c cut, then the origin removed: zone c stays stranded from the
    // survivors that hold its change.
    with_state(propagation(), |mut state| async move {
        ready(&mut state).await;
        cut_zone(&mut state, 2).await;
        zone_c_stays_stranded(&mut state).await;
        state.remove_node(0).await.unwrap();
        let snap = state.snapshot(None, WATCH).await;
        assert_eq!(snap.oracle_total, 25);
        assert!(lags(&snap, 2, None).into_iter().all(stranded), "{snap:?}");
        for zone in [0, 1] {
            assert!(
                lags(&snap, zone, None).iter().all(|l| *l == Lag::CaughtUp),
                "{snap:?}"
            );
        }
        state
    });

    // (c) Removed before its heartbeat sent the change: no window holds it.
    with_state(propagation(), |mut state| async move {
        ready(&mut state).await;
        send(&mut state, 0, 25).await;
        state.remove_node(0).await.unwrap();
        let snap = state.snapshot(None, WATCH).await;
        assert_eq!((snap.oracle_total, snap.next_expiry_ms), (0, None));
        assert_eq!(views(&snap), [0; 11]);
        state
            .submit_request(1, KeyHash(7), hits(25), StepUntil::Time)
            .await
            .expect("a live node");
        settle(&mut state).await;
        let other = Watch {
            key: KeyHash(7),
            probe_hits: hits(1),
        };
        assert!(state.snapshot(None, other).await.next_expiry_ms.is_some());
        assert_eq!(state.snapshot(None, WATCH).await.next_expiry_ms, None);
        let target = state.virtual_ms + 15_000;
        let stop = state.step(15_000, EVENT).await;
        assert_eq!(stop.virtual_ms, target, "no expiry of the watched key");
        state
    });
}

/// The position of live node `id` in the snapshot.
fn position(snap: &ClusterState, id: u32) -> usize {
    snap.nodes
        .iter()
        .position(|n| n.id == id)
        .expect("a live node")
}

/// Removing a position moves every higher one down by one, across words.
#[test]
fn node_set_remove_shift_crosses_words() {
    let mut set = NodeSet::EMPTY;
    for i in [0, 63, 64, 65, 255] {
        set.insert(i);
    }
    assert_eq!(set.len(), 5);
    let mut shifted = set;
    shifted.remove_shift(63);
    assert_eq!(shifted.iter().collect::<Vec<_>>(), [0, 63, 64, 254]);
    let mut shifted = set;
    shifted.remove_shift(0);
    assert_eq!(shifted.iter().collect::<Vec<_>>(), [62, 63, 64, 254]);
    assert_eq!(set.iter().collect::<Vec<_>>(), [0, 63, 64, 65, 255]);
    assert_eq!(NodeSet::first(65).iter().count(), 65);
    assert!(NodeSet::first(0).is_empty());
}
