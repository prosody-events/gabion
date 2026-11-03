//! Native tests for the engine, run with `cargo nextest` (no wasm toolchain).
//!
//! These exercise the exact `run_engine` the wasm `Sim` spawns: the engine is
//! spawned as a `spawn_local` task on a paused current-thread runtime and
//! driven over the command channel, validating "spawn once, drive forever"
//! and the `CellDump` + logging shims.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroU16;

use gabion::defaults;
use gabion::gossip::{AnswerState, Role};
use gabion::settings::SettingError;
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot};
use tokio::task::LocalSet;

use super::{Command, EngineError, EngineState, LinkPolicyKind, VIZ_ROUTER_CAPACITY, run_engine};
use crate::config::{MAX_NODES, SimConfig, ZoneMode};
use crate::event::{
    CellView, ClusterState, Event, EventBatch, EventKind, Inspected, PacketKind, PeerView, Tier,
};

const WATCHED_KEY: u128 = 0x1;

/// The heartbeat interval of [`test_config`].
const T: u64 = 100;
/// The relay floor: how long after a packet is owed its prompt round runs.
const F_R: u64 = defaults::GOSSIP_MIN_EMIT_INTERVAL_MS;
/// `gossip.suspect_after`'s default: `max(GOSSIP_SUSPECT_AFTER_TICKS × T,
/// GOSSIP_SUSPECT_AFTER_FLOOR_MS)`.
const T_U: u64 = {
    let ticks = defaults::GOSSIP_SUSPECT_AFTER_TICKS * T;
    if ticks > defaults::GOSSIP_SUSPECT_AFTER_FLOOR_MS {
        ticks
    } else {
        defaults::GOSSIP_SUSPECT_AFTER_FLOOR_MS
    }
};

fn test_config(nodes: usize) -> SimConfig {
    SimConfig {
        nodes,
        tick_interval_ms: T,
        rule_window_ms: 60_000,
        rule_bucket_ms: 1_000,
        rule_limit: 1_000_000,
        uniform_loss: 0.0,
        ..SimConfig::default()
    }
}

/// Stand up `run_engine` on a paused current-thread runtime and run `driver`
/// against its command channel. Closing the channel ends the engine cleanly.
fn with_engine<F, Fut>(config: SimConfig, driver: F)
where
    F: FnOnce(mpsc::Sender<Command>) -> Fut,
    Fut: Future<Output = ()>,
{
    let rt = Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("build paused current-thread runtime");
    let local = LocalSet::new();
    local.block_on(&rt, async move {
        let (tx, rx) = mpsc::channel(16);
        let valid = config.validate().expect("test config validates");
        let engine = tokio::task::spawn_local(run_engine(valid, rx));
        driver(tx.clone()).await;
        drop(tx);
        engine
            .await
            .expect("engine task joined")
            .expect("engine ended cleanly");
    });
}

async fn step(tx: &mpsc::Sender<Command>, delta_ms: u64) -> EventBatch {
    let (reply, rx) = oneshot::channel();
    tx.send(Command::Step { delta_ms, reply }).await.unwrap();
    rx.await.unwrap()
}

async fn submit(tx: &mpsc::Sender<Command>, node: u32, key: u128, hits: u64) -> EventBatch {
    let (reply, rx) = oneshot::channel();
    tx.send(Command::SubmitRequest {
        node,
        key,
        hits,
        reply,
    })
    .await
    .unwrap();
    rx.await.unwrap().expect("submit_request succeeded")
}

async fn snapshot(tx: &mpsc::Sender<Command>) -> ClusterState {
    snapshot_inspecting(tx, None).await
}

async fn snapshot_inspecting(tx: &mpsc::Sender<Command>, inspect: Option<u32>) -> ClusterState {
    let (reply, rx) = oneshot::channel();
    tx.send(Command::Snapshot { inspect, reply }).await.unwrap();
    rx.await.unwrap()
}

/// Node `id`'s cells and peer rows, as the snapshot inspecting it reports them.
async fn inspect(tx: &mpsc::Sender<Command>, id: u32) -> Inspected {
    snapshot_inspecting(tx, Some(id))
        .await
        .inspected
        .expect("a live node")
}

async fn set_link(tx: &mpsc::Sender<Command>, src: u32, dst: u32, policy: LinkPolicyKind) {
    let (reply, rx) = oneshot::channel();
    tx.send(Command::SetLinkPolicy {
        src,
        dst,
        policy,
        reply,
    })
    .await
    .unwrap();
    rx.await.unwrap().expect("set_link_policy succeeded");
}

async fn add_node(tx: &mpsc::Sender<Command>) -> EventBatch {
    let (reply, rx) = oneshot::channel();
    tx.send(Command::AddNode { zone: None, reply })
        .await
        .unwrap();
    rx.await.unwrap().expect("the node joins")
}

async fn remove_node(tx: &mpsc::Sender<Command>, id: u32) -> EventBatch {
    let (reply, rx) = oneshot::channel();
    tx.send(Command::RemoveNode { id, reply }).await.unwrap();
    rx.await.unwrap().expect("remove_node succeeded")
}

fn node_ids(state: &ClusterState) -> Vec<u32> {
    state.nodes.iter().map(|n| n.id).collect()
}

/// A spawned engine survives many `step` commands, with the gossip
/// tick index and per-node `rounds_heartbeat` accumulating monotonically.
#[test]
fn engine_accumulates_ticks_across_many_steps() {
    with_engine(test_config(2), |tx| async move {
        let mut last_tick = 0;
        for _ in 0..5 {
            let batch = step(&tx, 500).await; // 5 ticks at 100 ms each
            assert!(batch.tick >= last_tick, "tick index must not go backward");
            last_tick = batch.tick;
        }
        assert_eq!(last_tick, 25, "2500 ms at 100 ms/tick is 25 ticks");

        let state = snapshot(&tx).await;
        assert_eq!(state.nodes.len(), 2);
        assert_eq!(state.virtual_ms, 2_500);
        assert!(
            state.nodes.iter().all(|n| n.rounds_heartbeat > 0),
            "every runtime should have fired heartbeat ticks"
        );
    });
}

/// Submitting a request creates a cell on the local node (visible
/// both as a `CellCreated` event and via the `CellDump` snapshot) and the hit
/// gossips to the other node, where the aggregate converges to the oracle.
#[test]
fn submit_creates_and_gossips_a_cell() {
    with_engine(test_config(2), |tx| async move {
        let submit_batch = submit(&tx, 0, WATCHED_KEY, 5).await;
        let local_created = submit_batch.events.iter().any(|e| {
            matches!(
                &e.kind,
                EventKind::CellCreated { node, key, count, .. }
                    if *node == 0 && *key == WATCHED_KEY && *count == 5
            )
        });
        assert!(local_created, "submit must create a cell on the local node");

        // The submitting node sees the cell immediately as a local origin.
        let before = snapshot(&tx).await;
        assert_eq!(before.oracle_total, 5);
        assert_eq!(before.nodes[0].aggregate_total, 5);
        assert!(
            inspect(&tx, 0)
                .await
                .cells
                .iter()
                .any(|c| c.key == WATCHED_KEY && c.count == 5 && c.is_local),
            "CellDump should report the local origin cell"
        );

        // Drive gossip forward and gather every event across the whole stream
        // (the submit batch plus every step). The runtime's first interval
        // tick fires at t=0, so propagation can begin during the submit call
        // itself — we assert on the cumulative stream, not on one batch.
        let mut all_events = submit_batch.events;
        for _ in 0..10 {
            all_events.extend(step(&tx, 100).await.events);
        }
        let node1_cell_event = all_events.iter().any(|e| {
            matches!(
                &e.kind,
                EventKind::CellCreated { node: 1, .. } | EventKind::CellUpdated { node: 1, .. }
            )
        });
        let packet_observed = all_events.iter().any(|e| {
            matches!(
                &e.kind,
                EventKind::PacketSent { .. } | EventKind::PacketDelivered { .. }
            )
        });
        assert!(node1_cell_event, "the hit should gossip to node 1");
        assert!(packet_observed, "gossip should produce packet events");

        let after = snapshot(&tx).await;
        assert_eq!(after.oracle_total, 5);
        assert!(
            after.nodes.iter().all(|n| n.aggregate_total == 5),
            "both nodes converge on the oracle total"
        );
        // Node 1's copy is a remote (non-local) cell.
        assert!(
            inspect(&tx, 1)
                .await
                .cells
                .iter()
                .any(|c| c.key == WATCHED_KEY && c.count == 5 && !c.is_local),
            "node 1 holds the cell as a remote origin"
        );
    });
}

/// The error budget the most recent request computed reaches the snapshot,
/// so the inspector shows the threshold a burst must cross, and each peer row
/// and gateway counter reaches it as the runtime reports it.
#[test]
fn error_budget_and_gateway_counters_surface_in_snapshot() {
    let config = SimConfig {
        rule_limit: 1_000,
        target_err_bps: 100,
        ..zoned(12, 3)
    };
    with_state(config, |mut state| async move {
        state.submit_request(0, 0, 7).await.unwrap();
        let origin = state.snapshot(None).await.nodes.swap_remove(0);
        assert_eq!(origin.id, 0);
        assert!(
            origin.error_budget > 0,
            "the error budget must be recorded once a request is seen",
        );

        warm(&mut state).await;
        let g = gateway_of(&state, 0, 1).await;
        silence(&mut state, g);
        state.submit_request(0, 0, 1).await.unwrap();
        state.step(T_U / 2 + T).await;

        // Checkpoint A, the peer mirror: the rows the page shows are the rows
        // the runtime reports, read at the same virtual instant.
        let snap = state.snapshot(Some(0)).await;
        let direct = super::admin_snapshot(&state.nodes[0].admin_tx)
            .await
            .expect("node 0 runs");
        let rows = &snap.inspected.expect("node 0 is live").peers;
        assert_eq!(rows.len(), direct.peers.len());
        for (row, entry) in rows.iter().zip(&direct.peers) {
            assert_eq!(row.role, entry.role);
            assert_eq!(row.answer, entry.answer);
            assert_eq!(row.unanswered_ms, entry.unanswered_ms);
            assert_eq!(row.own_unacked, entry.own_unacked);
            assert_eq!(row.relay_unacked, entry.relay_unacked);
        }
        let silent = rows
            .iter()
            .find(|row| row.id == Some(g))
            .expect("row for g");
        assert_eq!(silent.answer, AnswerState::Owed);
        assert!(silent.own_unacked >= 1);

        // Checkpoint B, the counter mirror: each node's counters are the
        // library's own fields.
        state.step(T_U + 3 * T).await;
        let snap = state.snapshot(None).await;
        for (node, handle) in snap.nodes.iter().zip(&state.nodes) {
            let c = super::counters(&handle.admin_tx).await.expect("runs");
            assert_eq!(node.gateway_failovers, c.gateway_failovers);
            assert_eq!(node.gap_reports_sent, c.gap_reports_sent);
            assert_eq!(node.rto_rewinds_own, c.rto_rewinds_own);
            assert_eq!(node.rto_rewinds_relay, c.rto_rewinds_relay);
            assert_eq!(node.sweeps_served, c.sweeps_served);
        }
        assert!(snap.nodes[0].gateway_failovers >= 1);
        assert!(snap.nodes[0].rto_rewinds_own >= 1);
        state
    });
}

/// The windowed oracle is the sum of every node's locally-originated *live*
/// cells: a gossiped replica never inflates it (one origin → counted once), and
/// it decays to zero as the window slides past the only bucket — the behaviour
/// a monotonic accumulator could not show.
#[test]
fn oracle_total_is_windowed_and_counts_each_origin_once() {
    let config = SimConfig {
        rule_window_ms: 2_000,
        rule_bucket_ms: 1_000,
        ..test_config(2)
    };
    with_engine(config, |tx| async move {
        submit(&tx, 0, WATCHED_KEY, 9).await;
        assert_eq!(
            snapshot(&tx).await.oracle_total,
            9,
            "the windowed oracle counts the fresh burst at its origin"
        );

        // Spread it so node 1 holds a remote replica of node 0's cell.
        for _ in 0..5 {
            step(&tx, 100).await;
        }
        let converged = snapshot(&tx).await;
        assert!(
            converged.nodes.iter().all(|n| n.aggregate_total == 9),
            "both nodes hold the cell"
        );
        assert_eq!(
            converged.oracle_total, 9,
            "the replica on node 1 is not local, so the oracle still counts the hit once at its \
             single origin"
        );

        // Step well past the 2 s window so the only bucket ages out everywhere.
        for _ in 0..40 {
            step(&tx, 100).await;
        }
        let aged = snapshot(&tx).await;
        assert_eq!(
            aged.oracle_total, 0,
            "the windowed oracle decays to zero as the bucket expires"
        );
        assert!(
            aged.nodes.iter().all(|n| n.aggregate_total == 0),
            "node views age out in lockstep with the oracle"
        );
    });
}

/// A blocked link stops propagation (the logging transport records
/// drops), and healing the link lets the cluster converge.
#[test]
fn partition_blocks_then_heal_converges() {
    with_engine(test_config(2), |tx| async move {
        // Cut both directions between node 0 and node 1.
        set_link(&tx, 0, 1, LinkPolicyKind::Block).await;
        set_link(&tx, 1, 0, LinkPolicyKind::Block).await;

        submit(&tx, 0, WATCHED_KEY, 7).await;

        let mut saw_drop = false;
        for _ in 0..10 {
            let batch = step(&tx, 100).await;
            if batch
                .events
                .iter()
                .any(|e| matches!(&e.kind, EventKind::PacketDropped { .. }))
            {
                saw_drop = true;
            }
        }
        assert!(saw_drop, "blocked link must produce dropped-packet events");

        let partitioned = snapshot(&tx).await;
        assert_eq!(partitioned.nodes[0].aggregate_total, 7);
        assert_eq!(
            partitioned.nodes[1].aggregate_total, 0,
            "node 1 is isolated and learns nothing"
        );

        // Heal both directions and let anti-entropy reconcile.
        set_link(&tx, 0, 1, LinkPolicyKind::Pass).await;
        set_link(&tx, 1, 0, LinkPolicyKind::Pass).await;
        for _ in 0..10 {
            step(&tx, 100).await;
        }

        let healed = snapshot(&tx).await;
        assert!(
            healed.nodes.iter().all(|n| n.aggregate_total == 7),
            "convergence after heal"
        );
    });
}

/// `missing_for_ms` dates the oldest change a node lacks from its origin
/// cell's last update: a lower count in a cell it holds counts as missing,
/// a cell it never received counts too, and the older of the two wins.
#[test]
fn missing_for_dates_the_oldest_change_a_node_lacks() {
    with_engine(test_config(2), |tx| async move {
        let missing = |state: &ClusterState| -> Vec<Option<u64>> {
            state.nodes.iter().map(|n| n.missing_for_ms).collect()
        };
        submit(&tx, 0, WATCHED_KEY, 7).await;
        for _ in 0..5 {
            step(&tx, T).await;
        }
        let caught_up = snapshot(&tx).await;
        assert!(caught_up.nodes.iter().all(|n| n.aggregate_total == 7));
        assert_eq!(
            missing(&caught_up),
            [None, None],
            "both nodes hold the 7 hits"
        );

        set_link(&tx, 0, 1, LinkPolicyKind::Block).await;
        set_link(&tx, 1, 0, LinkPolicyKind::Block).await;
        // At 500 ms, into the bucket node 1 already holds at a count of 7.
        submit(&tx, 0, WATCHED_KEY, 3).await;
        for _ in 0..4 {
            step(&tx, T).await;
        }
        let lower = snapshot(&tx).await;
        assert_eq!(lower.virtual_ms, 900);
        assert_eq!(
            lower.nodes[1].aggregate_total, 7,
            "node 1 holds the cell at 7 of 10"
        );
        assert_eq!(
            missing(&lower),
            [None, Some(400)],
            "the cell was last updated at 500 ms"
        );

        // At 1200 ms, into a bucket node 1 has never received.
        for _ in 0..3 {
            step(&tx, T).await;
        }
        submit(&tx, 0, WATCHED_KEY, 2).await;
        for _ in 0..3 {
            step(&tx, T).await;
        }
        let absent = snapshot(&tx).await;
        assert_eq!(absent.virtual_ms, 1_500);
        assert_eq!(
            missing(&absent),
            [None, Some(1_000)],
            "the change at 500 ms is the oldest"
        );

        set_link(&tx, 0, 1, LinkPolicyKind::Pass).await;
        set_link(&tx, 1, 0, LinkPolicyKind::Pass).await;
        for _ in 0..10 {
            step(&tx, T).await;
        }
        let healed = snapshot(&tx).await;
        assert!(healed.nodes.iter().all(|n| n.aggregate_total == 12));
        assert_eq!(
            missing(&healed),
            [None, None],
            "node 1 caught up after the heal"
        );

        // At 2500 ms, into a new bucket, with node 1 holding every older one:
        // a cell it lacks outright is its only missing change, read after
        // node 0, which holds that cell.
        set_link(&tx, 0, 1, LinkPolicyKind::Block).await;
        set_link(&tx, 1, 0, LinkPolicyKind::Block).await;
        submit(&tx, 0, WATCHED_KEY, 4).await;
        for _ in 0..3 {
            step(&tx, T).await;
        }
        let only_absent = snapshot(&tx).await;
        assert_eq!(only_absent.virtual_ms, 2_800);
        assert_eq!(
            missing(&only_absent),
            [None, Some(300)],
            "node 1 lacks only the cell made at 2500 ms"
        );
    });
}

/// Determinism: the same `(config, command-script)` produces an identical
/// event log, tier tags and gateway events included — the invariant the
/// shareable-URL replay relies on.
#[test]
fn same_script_produces_identical_events() {
    fn run() -> Vec<EventBatch> {
        let mut batches = Vec::new();
        // N=16 so the determinism check exercises ticks arriving *while*
        // packets are in flight across a sizeable fan — the combined load
        // under which a coalescing (watch) tick source diverged.
        with_engine(zoned(16, 3), |tx| {
            let batches = &mut batches;
            async move {
                batches.push(submit(&tx, 0, WATCHED_KEY, 3).await);
                batches.push(submit(&tx, 2, WATCHED_KEY, 4).await);
                for _ in 0..6 {
                    batches.push(step(&tx, 100).await);
                }
            }
        });
        batches
    }

    let first = run();
    let second = run();
    assert_eq!(
        first, second,
        "identical script must yield identical event batches"
    );
    assert!(
        first.iter().any(|b| !b.events.is_empty()),
        "the script should produce some events"
    );
    assert!(
        first
            .iter()
            .flat_map(|b| &b.events)
            .any(|e| matches!(e.kind, EventKind::GatewayChanged { .. })),
        "the zones should elect gateways, and the script should report them"
    );
}

/// At a larger cluster the per-node tick channel must deliver *every* fired
/// tick — no coalescing — so each runtime's `rounds_heartbeat` equals the
/// number of whole ticks stepped, identically across nodes and across runs. (A
/// `watch` tick source failed exactly here: a busy runtime silently dropped
/// ticks.)
#[test]
fn many_nodes_receive_every_tick_deterministically() {
    fn run() -> Vec<u64> {
        let mut totals = Vec::new();
        with_engine(test_config(32), |tx| {
            let totals = &mut totals;
            async move {
                // 1000 ms at 100 ms/tick = 10 whole ticks, and no requests (so
                // no threshold rounds): every node's rounds_heartbeat must be
                // 10.
                step(&tx, 1_000).await;
                let state = snapshot(&tx).await;
                *totals = state.nodes.iter().map(|n| n.rounds_heartbeat).collect();
            }
        });
        totals
    }

    let first = run();
    assert_eq!(first.len(), 32);
    assert!(
        first.iter().all(|&t| t == 10),
        "every node must consume all 10 ticks (no coalescing); got {first:?}"
    );
    assert_eq!(run(), first, "tick delivery must be deterministic at N=32");
}

/// A node added after the cluster has converged catches up to the cluster
/// total by anti-entropy — survivors push their settled cells to the cold
/// newcomer — and it appears in snapshots under a fresh stable id (`N`, the
/// next never-reused id) without renumbering any survivor.
#[test]
fn added_node_catches_up_by_gossip() {
    with_engine(test_config(3), |tx| async move {
        // Seed and converge a 3-node cluster on a single burst.
        submit(&tx, 0, WATCHED_KEY, 50).await;
        for _ in 0..10 {
            step(&tx, 100).await;
        }
        let before = snapshot(&tx).await;
        assert_eq!(node_ids(&before), vec![0, 1, 2]);
        assert!(
            before.nodes.iter().all(|n| n.aggregate_total == 50),
            "cluster converged before the add"
        );

        // Add a fresh member. It takes id 3 (next_id); survivors keep 0..2.
        add_node(&tx).await;
        let joined = snapshot(&tx).await;
        assert_eq!(
            node_ids(&joined),
            vec![0, 1, 2, 3],
            "ids are stable; the newcomer is 3, not a renumber"
        );
        let newcomer = joined.nodes.iter().find(|n| n.id == 3).unwrap();
        assert_eq!(newcomer.aggregate_total, 0, "the newcomer starts cold");

        // Drive gossip; the newcomer catches up to the settled total.
        for _ in 0..15 {
            step(&tx, 100).await;
        }
        let after = snapshot(&tx).await;
        assert!(
            after.nodes.iter().all(|n| n.aggregate_total == 50),
            "every node, the newcomer included, converges on 50: {:?}",
            after
                .nodes
                .iter()
                .map(|n| (n.id, n.aggregate_total))
                .collect::<Vec<_>>(),
        );
    });
}

/// Removing a node drops it from snapshots — its stable id leaves a gap rather
/// than renumbering survivors — and the survivors keep gossiping correctly: a
/// burst after the removal still converges across exactly the remaining nodes.
#[test]
fn removed_node_leaves_and_survivors_keep_converging() {
    with_engine(test_config(3), |tx| async move {
        submit(&tx, 0, WATCHED_KEY, 50).await;
        for _ in 0..10 {
            step(&tx, 100).await;
        }

        // Remove the middle node by its stable id.
        remove_node(&tx, 1).await;
        let removed = snapshot(&tx).await;
        assert_eq!(
            node_ids(&removed),
            vec![0, 2],
            "node 1 left; 0 and 2 keep their ids (a gap, not a renumber)"
        );
        assert!(
            removed.nodes.iter().all(|n| n.aggregate_total == 50),
            "survivors still hold the converged total"
        );

        // The survivors keep gossiping: a new burst on a survivor converges
        // across exactly the two that remain.
        submit(&tx, 2, WATCHED_KEY, 10).await;
        for _ in 0..15 {
            step(&tx, 100).await;
        }
        let after = snapshot(&tx).await;
        assert_eq!(after.oracle_total, 60);
        assert_eq!(node_ids(&after), vec![0, 2]);
        assert!(
            after.nodes.iter().all(|n| n.aggregate_total == 60),
            "the two survivors converge on 60: {:?}",
            after
                .nodes
                .iter()
                .map(|n| (n.id, n.aggregate_total))
                .collect::<Vec<_>>(),
        );
    });
}

/// Determinism across churn: the same command script — including a join and a
/// leave — yields an identical event log *and* identical stable ids on every
/// run. This is the invariant the shareable-URL replay relies on once churn is
/// part of the command stream.
#[test]
fn churn_script_is_deterministic() {
    fn run() -> (Vec<EventBatch>, Vec<u32>) {
        let mut batches = Vec::new();
        let mut ids = Vec::new();
        with_engine(test_config(4), |tx| {
            let batches = &mut batches;
            let ids = &mut ids;
            async move {
                batches.push(submit(&tx, 0, WATCHED_KEY, 7).await);
                batches.push(add_node(&tx).await);
                for _ in 0..4 {
                    batches.push(step(&tx, 100).await);
                }
                batches.push(remove_node(&tx, 1).await);
                for _ in 0..4 {
                    batches.push(step(&tx, 100).await);
                }
                *ids = node_ids(&snapshot(&tx).await);
            }
        });
        (batches, ids)
    }

    let (first_batches, first_ids) = run();
    let (second_batches, second_ids) = run();
    assert_eq!(
        first_batches, second_batches,
        "identical churn script must yield identical event batches"
    );
    assert_eq!(
        first_ids, second_ids,
        "identical churn script must yield identical stable ids"
    );
    assert_eq!(
        first_ids,
        vec![0, 2, 3, 4],
        "built 0..3, joined id 4, removed id 1 ⇒ a {{0,2,3,4}} cluster"
    );
}

/// One heartbeat sends a new count to the origin's zone and one gateway in
/// each remote zone, and the submit alone sends nothing; the gateways relay it
/// into their zones one relay floor after they hear it, which the next
/// heartbeat's step covers. `hits` stays well under the eager-flush budget
/// (`epsilon = rule_limit × bps / (10_000 × peers) = 1_000_000 / 1_100 ≈ 909`)
/// so no threshold round runs. The relay floor is far below a heartbeat, so a
/// step that ends at the heartbeat itself ends before the relay leaves.
#[test]
fn one_heartbeat_reaches_the_zone_and_one_gateway_per_remote_zone() {
    // A node "knows" the count if it holds the cell (a nonzero aggregate, since
    // the single key is the only traffic).
    fn known(state: &ClusterState) -> Vec<u8> {
        let mut zones: Vec<u8> = state
            .nodes
            .iter()
            .filter(|n| n.aggregate_total > 0)
            .map(|n| n.zone)
            .collect();
        zones.sort_unstable();
        zones
    }

    with_state(zoned(12, 3), |mut state| async move {
        warm(&mut state).await;
        state.submit_request(0, WATCHED_KEY, 5).await.unwrap();
        assert_eq!(
            known(&state.snapshot(None).await),
            [0],
            "the submit alone gossips nothing"
        );
        state.step(T).await;
        assert_eq!(
            known(&state.snapshot(None).await),
            [0, 0, 0, 0, 1, 2],
            "the zone's four nodes and one gateway in each other zone"
        );
        state.step(T).await;
        assert_eq!(known(&state.snapshot(None).await).len(), 12, "the relays");
        state
    });
}

/// Run `body` on an engine state built from `config` on a paused runtime, so
/// a test reads the state's own fields.
fn with_state<F, Fut>(config: SimConfig, body: F)
where
    F: FnOnce(super::EngineState) -> Fut,
    Fut: Future<Output = super::EngineState>,
{
    let rt = Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("build paused current-thread runtime");
    LocalSet::new().block_on(&rt, async move {
        let state = super::EngineState::build(config.validate().expect("config validates"))
            .expect("every node's settings validate");
        let mut state = body(state).await;
        state.shutdown().await;
    });
}

/// `test_config` with its nodes placed in `zones` zones.
fn zoned(nodes: usize, zones: usize) -> SimConfig {
    SimConfig {
        zones,
        ..test_config(nodes)
    }
}

/// One second of heartbeats, enough for every peer to be heard and every role
/// to settle.
async fn warm(state: &mut EngineState) -> Vec<Event> {
    state.step(1_000).await.events
}

/// The zone node `id` is placed in, as the snapshot reports it.
fn zone_of(state: &ClusterState, id: u32) -> u8 {
    state
        .nodes
        .iter()
        .find(|n| n.id == id)
        .expect("a live node")
        .zone
}

/// `origin`'s gateway into `zone`, as its runtime reports it.
async fn gateway_of(state: &EngineState, origin: u32, zone: u8) -> u32 {
    peers_of(state, origin)
        .await
        .iter()
        .find(|p| p.role == Role::Gateway && p.zone == Some(zone))
        .and_then(|p| p.id)
        .expect("a gateway")
}

/// Node `id`'s cells, as the snapshot inspecting it reports them.
async fn cells_of(state: &EngineState, id: u32) -> Vec<CellView> {
    inspected(state, id).await.cells
}

/// Node `id`'s peer rows, as the snapshot inspecting it reports them.
async fn peers_of(state: &EngineState, id: u32) -> Vec<PeerView> {
    inspected(state, id).await.peers
}

async fn inspected(state: &EngineState, id: u32) -> Inspected {
    state
        .snapshot(Some(id))
        .await
        .inspected
        .expect("a live node")
}

/// Cut `g` off from every other live node, in both directions.
fn silence(state: &mut EngineState, g: u32) {
    let ids: Vec<u32> = state.nodes.iter().map(|n| n.id).collect();
    for x in ids.into_iter().filter(|&x| x != g) {
        state.set_link_policy(g, x, LinkPolicyKind::Block).unwrap();
        state.set_link_policy(x, g, LinkPolicyKind::Block).unwrap();
    }
}

/// The `(dst, tier, virtual_ms)` of every `kind` packet `src` sent.
fn sent(events: &[Event], src: u32, kind: PacketKind) -> Vec<(u32, Tier, u64)> {
    events
        .iter()
        .filter_map(|e| match e.kind {
            EventKind::PacketSent {
                src: s,
                dst,
                kind: k,
                tier,
                ..
            } if s == src && k == kind => Some((dst, tier, e.virtual_ms)),
            _ => None,
        })
        .collect()
}

/// The first `GatewayChanged` of `origin`'s gateway into `zone` away from
/// `from`, with its time and new gateway.
fn failover(events: &[Event], origin: u32, zone: u8, from: u32) -> Option<(u64, Option<u32>)> {
    events.iter().find_map(|e| match e.kind {
        EventKind::GatewayChanged {
            origin: o,
            zone: z,
            from: Some(f),
            to,
        } if (o, z, f) == (origin, zone, from) => Some((e.virtual_ms, to)),
        _ => None,
    })
}

/// The summed count of `rule`'s cells at `bucket`.
fn bucket_count<'a>(cells: impl Iterator<Item = &'a CellView>, rule: u128, bucket: u32) -> u64 {
    cells
        .filter(|c| c.rule == rule && c.bucket == bucket)
        .map(|c| c.count)
        .sum()
}

/// Every sent or dropped packet's bytes, summed by tier and class here, equal
/// the snapshot's `bytes_by_tier`, and each live node's `own_data_bytes` equal
/// the `Own` bytes it sent. The classification is this test's own, so it
/// checks the engine's rather than reusing it.
fn assert_bytes_by_tier_and_sender(events: &[Event], state: &ClusterState) {
    let mut sums = BTreeMap::new();
    let mut own_by_sender = BTreeMap::new();
    for event in events {
        if let EventKind::PacketSent {
            src,
            bytes,
            kind,
            tier,
            ..
        }
        | EventKind::PacketDropped {
            src,
            bytes,
            kind,
            tier,
            ..
        } = event.kind
        {
            let data = matches!(
                kind,
                PacketKind::Own | PacketKind::Relay | PacketKind::Sweep
            );
            *sums.entry((tier == Tier::InZone, data)).or_insert(0u64) += u64::from(bytes);
            if kind == PacketKind::Own {
                *own_by_sender.entry(src).or_insert(0u64) += u64::from(bytes);
            }
        }
    }
    for node in &state.nodes {
        assert_eq!(
            node.own_data_bytes,
            own_by_sender.get(&node.id).copied().unwrap_or(0),
            "node {} own data bytes",
            node.id
        );
    }
    let sum = |in_zone, data| sums.get(&(in_zone, data)).copied().unwrap_or(0);
    let by = &state.bytes_by_tier;
    assert_eq!(by.in_zone.data, sum(true, true), "in-zone data");
    assert_eq!(by.in_zone.control, sum(true, false), "in-zone control");
    assert_eq!(by.cross_zone.data, sum(false, true), "cross-zone data");
    assert_eq!(
        by.cross_zone.control,
        sum(false, false),
        "cross-zone control"
    );
    assert!(
        sums.values().any(|&bytes| bytes > 0),
        "the run sent something"
    );
}

/// Each node's `gateway_for` counts the origins whose runtime reports it as
/// their gateway into its zone, read here from the runtimes' own peer rows.
#[test]
fn gateway_for_counts_the_origins_routing_through_each_node() {
    with_state(zoned(12, 3), |mut state| async move {
        warm(&mut state).await;
        // Remove a node that is some origin's gateway, so the count follows the
        // failover, not a cached row.
        let g = gateway_of(&state, 0, 1).await;
        state.remove_node(g).await.unwrap();
        state.step(3 * T).await;
        let snap = state.snapshot(None).await;
        let mut want = BTreeMap::new();
        for node in &snap.nodes {
            for peer in peers_of(&state, node.id).await {
                if peer.role == Role::Gateway {
                    *want
                        .entry(peer.id.expect("a resolved gateway"))
                        .or_insert(0u32) += 1;
                }
            }
        }
        for node in &snap.nodes {
            assert_eq!(
                node.gateway_for,
                want.get(&node.id).copied().unwrap_or(0),
                "node {}",
                node.id
            );
        }
        assert_eq!(
            snap.nodes.iter().map(|n| n.gateway_for).sum::<u32>(),
            11 * 2,
            "each of the eleven origins has a gateway into both other zones"
        );
        state
    });
}

/// Zones come from configuration, not from the address: all twelve nodes bind
/// 127.0.0.1 (`addr_of_id`), yet the origin's own packets split three in its
/// zone, two to one node in each other.
#[test]
fn zones_come_from_config_not_address() {
    with_state(zoned(12, 3), |mut state| async move {
        warm(&mut state).await;
        state.submit_request(0, WATCHED_KEY, 5).await.unwrap();
        let events = state.step(T).await.events;
        let snap = state.snapshot(None).await;

        let own = sent(&events, 0, PacketKind::Own);
        assert_eq!(
            own.len(),
            5,
            "one own packet to each of 3 zone peers and 2 gateways: {own:?}"
        );
        let in_zone: BTreeSet<u32> = own
            .iter()
            .filter(|p| p.1 == Tier::InZone)
            .map(|p| p.0)
            .collect();
        assert_eq!(in_zone, BTreeSet::from([3, 6, 9]));
        let cross: BTreeSet<u32> = own
            .iter()
            .filter(|p| p.1 == Tier::CrossZone)
            .map(|p| p.0)
            .collect();
        let cross_zones: Vec<u8> = cross.iter().map(|&id| zone_of(&snap, id)).collect();
        assert_eq!(cross.len(), 2, "one gateway in each other zone: {cross:?}");
        assert_eq!(
            cross_zones.iter().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([1, 2])
        );
        state
    });
}

/// A gateway relays into its zone one relay floor after it hears the origin:
/// the prompt round, not the next heartbeat.
#[test]
fn relay_follows_one_prompt_interval_after_the_gateway_hears() {
    with_state(zoned(12, 3), |mut state| async move {
        warm(&mut state).await;
        let g = gateway_of(&state, 0, 1).await;
        state.submit_request(0, WATCHED_KEY, 1).await.unwrap();
        let events = state.step(2 * T).await.events;

        let t_send = sent(&events, 0, PacketKind::Own)
            .iter()
            .find_map(|&(dst, _, at)| (dst == g).then_some(at))
            .expect("the origin sends its gateway");
        let t_recv = events
            .iter()
            .find_map(|e| match e.kind {
                EventKind::PacketDelivered { src: 0, dst, .. }
                    if dst == g && e.virtual_ms >= t_send =>
                {
                    Some(e.virtual_ms)
                }
                _ => None,
            })
            .expect("the gateway receives it");
        let snap = state.snapshot(None).await;
        let relays = sent(&events, g, PacketKind::Relay);
        assert_eq!(relays.len(), 3, "to the gateway's three zone peers");
        for (dst, _, at) in relays {
            assert_eq!(at, t_recv + F_R, "one relay floor after it hears");
            assert_eq!(zone_of(&snap, dst), 1);
        }
        state
    });
}

/// A gateway that answers nothing is replaced once a packet to it has gone
/// unanswered for `suspect_after`, and the replacement relays what it missed.
#[test]
fn silent_gateway_fails_over_within_suspect_after() {
    with_state(zoned(12, 3), |mut state| async move {
        warm(&mut state).await;
        let g = gateway_of(&state, 0, 1).await;
        let t_s = state.virtual_ms;
        silence(&mut state, g);
        state.submit_request(0, WATCHED_KEY, 1).await.unwrap();
        let mut events = Vec::new();
        while state.virtual_ms < t_s + T_U + 3 * T + 2 * F_R + T {
            events.extend(state.step(T).await.events);
        }
        let (at, to) = failover(&events, 0, 1, g).expect("a failover");
        assert!(
            (t_s + T_U..=t_s + T_U + 3 * T + 2 * F_R).contains(&at),
            "failed over at {at}, silenced at {t_s}"
        );
        let to = to.expect("a replacement gateway");
        assert_ne!(to, g);

        state.step(3 * T).await;
        let snap = state.snapshot(None).await;
        for node in snap.nodes.iter().filter(|n| n.zone == 1 && n.id != g) {
            assert_eq!(node.aggregate_total, snap.oracle_total, "node {}", node.id);
        }
        assert_eq!(snap.nodes[0].gateway_failovers, 1);
        assert_eq!(zone_of(&snap, to), 1);
        state
    });
}

/// A gateway cut off from its zone in one direction still answers the origin,
/// so it is never suspected; the zone is repaired by the gateway telling the
/// origin it cannot relay. `CANNOT_RELAY` rides only on packets the gateway
/// sends the origin, which are its answers to the origin's data, so the origin
/// keeps sending until the repair.
#[test]
fn one_way_cut_is_repaired_by_the_backstop() {
    with_state(zoned(12, 3), |mut state| async move {
        warm(&mut state).await;
        let g = gateway_of(&state, 0, 1).await;
        let members: Vec<u32> = state
            .nodes
            .iter()
            .filter(|n| n.zone == 1 && n.id != g)
            .map(|n| n.id)
            .collect();
        for &m in &members {
            state.set_link_policy(g, m, LinkPolicyKind::Block).unwrap();
        }
        let t_cut = state.virtual_ms;
        let mut deadline = t_cut + T_U + 10 * T;
        let mut t1 = None;
        let mut failed_over = false;
        while state.virtual_ms < deadline {
            if !failed_over {
                state.submit_request(0, WATCHED_KEY, 1).await.unwrap();
            }
            let events = state.step(T.min(deadline - state.virtual_ms)).await.events;
            failed_over |= failover(&events, 0, 1, g).is_some();
            if t1.is_none() {
                t1 = events.iter().find_map(|e| match e.kind {
                    EventKind::PacketDropped {
                        src,
                        kind: PacketKind::Relay,
                        ..
                    } if src == g => Some(e.virtual_ms),
                    _ => None,
                });
                if let Some(t1) = t1 {
                    deadline = t1 + T_U + 3 * T + 2 * F_R;
                }
            }
        }
        assert!(failed_over, "the origin left the gateway that cannot relay");
        let t1 = t1.expect("the cut swallowed a relay, but none was dropped");
        assert_eq!(state.virtual_ms, t1 + T_U + 3 * T + 2 * F_R);
        let snap = state.snapshot(None).await;
        for node in snap.nodes.iter().filter(|n| n.zone == 1) {
            assert_eq!(node.aggregate_total, snap.oracle_total, "node {}", node.id);
        }
        state
    });
}

/// A zone cut off from the rest for longer than `suspect_after` turns every
/// row across the cut that is owed data suspect; after the heal, every node
/// holds every bucket's exact count, and every byte sent or dropped is
/// accounted by tier. Zone c has one member, so the gateway into it cannot
/// fail over: a replaced gateway that wins the role back at the heal gets its
/// stream from 0, but this one keeps its role, so only the rewind its probe
/// answer brings resends what the cut swallowed.
#[test]
fn zone_partition_heals_to_exact_counts() {
    with_state(zoned(5, 3), |mut state| async move {
        let mut all = warm(&mut state).await;
        let lone = state.snapshot(None).await.zones[2].live;
        assert_eq!(lone, 1, "zone c's gateway has no replacement");
        let cut: Vec<u32> = state
            .nodes
            .iter()
            .filter(|n| n.zone == 2)
            .map(|n| n.id)
            .collect();
        let rest: Vec<u32> = state
            .nodes
            .iter()
            .filter(|n| n.zone != 2)
            .map(|n| n.id)
            .collect();
        for &a in &cut {
            for &b in &rest {
                state.set_link_policy(a, b, LinkPolicyKind::Block).unwrap();
                state.set_link_policy(b, a, LinkPolicyKind::Block).unwrap();
            }
        }
        all.extend(
            state
                .submit_request(0, WATCHED_KEY, 3)
                .await
                .unwrap()
                .events,
        );
        all.extend(
            state
                .submit_request(2, WATCHED_KEY, 4)
                .await
                .unwrap()
                .events,
        );
        all.extend(state.step(3_000).await.events);
        for node in &state.snapshot(None).await.nodes {
            let side = if node.zone == 2 { 4 } else { 3 };
            assert_eq!(
                node.aggregate_total, side,
                "node {} sees only its side of the cut",
                node.id
            );
        }
        state.heal();
        all.extend(state.step(3 * T).await.events);
        let snap = state.snapshot(None).await;
        // Pinned to the 3 + 4 hits submitted, not to counts the cells derive,
        // so a hit lost at its origin or an oracle that sums replicas fails.
        assert_eq!(snap.oracle_total, 7, "the oracle counts both sides once");
        for node in &snap.nodes {
            assert_eq!(node.aggregate_total, 7, "node {} sees both sides", node.id);
        }
        let mut cells = Vec::new();
        for node in &snap.nodes {
            cells.push((node.id, cells_of(&state, node.id).await));
        }

        let rule = state.rule.fingerprint;
        let buckets: BTreeSet<u32> = cells
            .iter()
            .flat_map(|(_, cells)| cells)
            .filter(|c| c.rule == rule)
            .map(|c| c.bucket)
            .collect();
        assert!(!buckets.is_empty(), "the hits left cells");
        let mut origin_total = 0;
        for bucket in buckets {
            let oracle = bucket_count(
                cells
                    .iter()
                    .flat_map(|(_, cells)| cells)
                    .filter(|c| c.is_local),
                rule,
                bucket,
            );
            origin_total += oracle;
            for (id, cells) in &cells {
                assert_eq!(
                    bucket_count(cells.iter(), rule, bucket),
                    oracle,
                    "node {id} bucket {bucket}"
                );
            }
        }
        assert_eq!(origin_total, 7, "the origins' own cells hold every hit");
        assert_bytes_by_tier_and_sender(&all, &snap);
        state
    });
}

/// With no zone configured the nodes send to every peer directly, as one zone
/// would, while each packet's tier still follows placement.
#[test]
fn unset_zone_mode_runs_as_one_zone() {
    let config = SimConfig {
        zone_mode: ZoneMode::Unset,
        ..zoned(12, 3)
    };
    with_state(config, |mut state| async move {
        let mut all = warm(&mut state).await;
        for node in 0..12 {
            all.extend(
                state
                    .submit_request(node, WATCHED_KEY, 1)
                    .await
                    .unwrap()
                    .events,
            );
        }
        let events = state.step(T).await.events;
        all.extend(events.iter().cloned());
        for node in 0..12 {
            let own = sent(&events, node, PacketKind::Own);
            let dsts: BTreeSet<u32> = own.iter().map(|p| p.0).collect();
            assert_eq!(
                (own.len(), dsts.len()),
                (11, 11),
                "node {node} sends every peer"
            );
            let cross = own.iter().filter(|p| p.1 == Tier::CrossZone).count();
            assert_eq!(cross, 8, "node {node}: eight peers are in other zones");
        }
        let snap = state.snapshot(None).await;
        for node in 0..12 {
            assert!(
                peers_of(&state, node)
                    .await
                    .iter()
                    .all(|p| p.zone.is_none()),
                "node {node}, told no zone, reports none"
            );
        }
        assert_bytes_by_tier_and_sender(&all, &snap);
        state
    });
}

/// A quiet cluster fetches no snapshot for gateway events; a gateway going
/// silent moves a role count, and only then is one fetched.
#[test]
fn gateway_events_fetch_snapshots_only_on_role_change() {
    with_state(zoned(12, 3), |mut state| async move {
        warm(&mut state).await;
        state.gateway_fetches = 0;
        let substeps = state.substeps;
        for _ in 0..10 {
            state.submit_request(0, WATCHED_KEY, 1).await.unwrap();
            state.step(T).await;
        }
        assert_eq!(
            state.gateway_fetches, 0,
            "no role moved in a healthy cluster"
        );
        let quiet_substeps = state.substeps - substeps;

        let g = gateway_of(&state, 0, 1).await;
        silence(&mut state, g);
        state.submit_request(0, WATCHED_KEY, 1).await.unwrap();
        let t_s = state.virtual_ms;
        let mut events = Vec::new();
        while failover(&events, 0, 1, g).is_none() {
            assert!(
                state.virtual_ms <= t_s + T_U + 5 * T,
                "no failover reported"
            );
            events.extend(state.step(T).await.events);
        }
        assert!(state.gateway_fetches >= 1);
        eprintln!(
            "{} snapshot fetches; {quiet_substeps} sub-steps over the ten quiet heartbeats, {} in \
             all",
            state.gateway_fetches, state.substeps
        );
        state
    });
}

/// Joiners go to the zone they ask for, or the smallest; an unknown zone is
/// refused without spending an id.
#[test]
fn joiner_goes_to_the_zone_it_asks_or_the_smallest() {
    with_state(zoned(5, 3), |mut state| async move {
        let live = |snap: &ClusterState| snap.zones.iter().map(|z| z.live).collect::<Vec<_>>();
        let snap = state.snapshot(None).await;
        assert_eq!(live(&snap), [2, 2, 1], "placement is node i mod 3");
        let names: Vec<_> = snap.zones.iter().map(|z| z.name).collect();
        assert_eq!(names, ["zone-a", "zone-b", "zone-c"]);

        // The last ask is for the largest zone, so only honouring it lands
        // there.
        for (ask, lands) in [(None, 2), (None, 0), (Some(0), 0)] {
            state.add_node(ask).await.expect("the node joins");
            let snap = state.snapshot(None).await;
            let joiner = snap.nodes.last().expect("a joiner");
            assert_eq!(joiner.zone, lands, "asked for {ask:?}");
        }
        let refused = state.add_node(Some(3)).await;
        assert!(
            matches!(refused, Err(EngineError::UnknownZone { zone: 3, zones: 3 })),
            "{:?}",
            refused.err()
        );
        let refused = state.add_node(Some(256)).await;
        assert!(
            matches!(
                refused,
                Err(EngineError::UnknownZone {
                    zone: 256,
                    zones: 3
                })
            ),
            "{:?}",
            refused.err()
        );
        state.add_node(None).await.expect("the node joins");
        let snap = state.snapshot(None).await;
        assert_eq!(
            snap.nodes.last().map(|n| n.id),
            Some(8),
            "the refusal spent no id"
        );
        state
    });
}

/// The `(origin, from, to)` of every `GatewayChanged` into `zone`.
fn gateway_moves(events: &[Event], zone: u8) -> Vec<(u32, Option<u32>, Option<u32>)> {
    events
        .iter()
        .filter_map(|e| match e.kind {
            EventKind::GatewayChanged {
                origin,
                zone: z,
                from,
                to,
            } if z == zone => Some((origin, from, to)),
            _ => None,
        })
        .collect()
}

/// A zone's last member leaving moves no role, yet every origin hears its
/// gateway into that zone go, once, at the removal: a later role change
/// diffs those origins again and reports nothing more for that zone.
#[test]
fn last_member_leaving_a_zone_reports_its_gateway_gone() {
    with_state(zoned(5, 3), |mut state| async move {
        warm(&mut state).await;
        // Node i sits in zone i mod 3, so node 2 is zone c's only member.
        for origin in [0, 1, 3, 4] {
            assert_eq!(gateway_of(&state, origin, 2).await, 2, "origin {origin}");
        }
        let removal = state.remove_node(2).await.unwrap().events;
        assert_eq!(
            gateway_moves(&removal, 2),
            [0, 1, 3, 4].map(|o| (o, Some(2), None)),
            "one report per origin"
        );

        // Silencing origin 0's gateway into zone b fails it over, which moves
        // its roles and diffs it again.
        let g = gateway_of(&state, 0, 1).await;
        silence(&mut state, g);
        state.submit_request(0, WATCHED_KEY, 1).await.unwrap();
        let later = state.step(T_U + 3 * T + 2 * F_R + T).await.events;
        assert!(failover(&later, 0, 1, g).is_some(), "origin 0 fails over");
        assert_eq!(
            gateway_moves(&later, 2),
            [],
            "reported once, not again at the next role change"
        );
        state
    });
}

/// A quiet cluster costs one sub-step per heartbeat: once the acks and join
/// sweeps a count set off are done, no prompt ticker stays armed.
#[test]
fn quiet_cluster_takes_one_substep_per_heartbeat() {
    with_state(test_config(12), |mut state| async move {
        state.submit_request(0, WATCHED_KEY, 1).await.unwrap();
        state.step(500).await;
        assert!(
            state.substeps > 5,
            "the count armed prompt rounds: {}",
            state.substeps
        );
        // One heartbeat per step: a ticker left armed adds a sub-step per floor
        // and fails the count before its full inbox can block `step`.
        state.substeps = 0;
        for heartbeats in 1..=10 {
            state.step(100).await;
            assert_eq!(state.substeps, heartbeats);
        }
        state
    });
}

/// A full heartbeat at `MAX_NODES`, every node sending every peer two own
/// packets (34 cells at 33 a packet), fits every node's inbound queue: no
/// packet is dropped.
#[test]
fn full_round_at_max_nodes_drops_nothing_under_pass() {
    let config = SimConfig {
        rule_limit: 1_000_000_000_000,
        ..test_config(MAX_NODES)
    };
    with_state(config, |mut state| async move {
        let ids: Vec<u32> = state.nodes.iter().map(|n| n.id).collect();
        for &node in &ids {
            for key in 0..34 {
                state
                    .submit_request(node, u128::from(node) << 8 | key, 1)
                    .await
                    .unwrap();
            }
        }
        let batch = state.step(100).await;
        let count =
            |pick: fn(&EventKind) -> bool| batch.events.iter().filter(|e| pick(&e.kind)).count();
        let dropped = count(|kind| matches!(kind, EventKind::PacketDropped { .. }));
        let delivered = count(|kind| matches!(kind, EventKind::PacketDelivered { .. }));
        assert_eq!(dropped, 0);
        assert_eq!(
            delivered,
            MAX_NODES * VIZ_ROUTER_CAPACITY,
            "every node received two packets from every peer"
        );
        state
    });
}

/// The wire form renders 128-bit identifiers as hex strings (JS-safe) while
/// the Rust types keep `u128`.
#[test]
fn events_serialize_with_hex_identifiers() {
    let event = crate::event::Event {
        tick: 3,
        virtual_ms: 300,
        kind: EventKind::CellCreated {
            node: 1,
            rule: 0xC0FE_DEAD_BEEF_BABE_F00D,
            key: 0x1,
            bucket: 0,
            count: 5,
        },
    };
    let json = serde_json::to_value(&event).unwrap();
    assert_eq!(json["kind"]["type"], "CellCreated");
    assert_eq!(json["kind"]["rule"], "000000000000c0fedeadbeefbabef00d");
    assert_eq!(json["kind"]["key"], "00000000000000000000000000000001");
}

/// The derived per-node sizing (`SimConfig::settings`) is anchored on
/// the `MAX_NODES` *growth ceiling*, not the initial cluster size — so every
/// node, however small the cluster starts, holds the full cross-node replica
/// working set it could ever grow into (the `CellStore` never resizes, and
/// `add_node` grows the live set with no rebuild). It must cover that ceiling
/// without overflow while staying far below the production defaults, and the
/// ceiling-sized settings must validate. The runtime path is exercised by
/// [`moderate_cluster_runtime_honors_the_shrunk_sizing`] and the growth path by
/// [`grown_cluster_holds_every_origin_without_overflow`].
#[test]
fn settings_are_ceiling_sized_and_hold_the_working_set() {
    let defaults = SimConfig::default();
    let live_buckets = (defaults.rule_window_ms / defaults.rule_bucket_ms) as usize;

    // Ceiling-anchored: a 12-node cluster and a `MAX_NODES` cluster get
    // byte-for-byte identical caps. This is the property that makes live
    // growth safe — size is a function of the ceiling and the rule, never
    // of `config.nodes`.
    let small = SimConfig::default().settings().expect("default settings");
    let ceiling = SimConfig {
        nodes: MAX_NODES,
        ..SimConfig::default()
    };
    let full = ceiling.settings().expect("full settings");
    let node_dictionary_capacity = ceiling
        .validate()
        .expect("ceiling-sized config validates")
        .storage
        .node_dictionary_capacity();
    assert_eq!(small.max_cells, full.max_cells);
    assert_eq!(small.peer_capacity, full.peer_capacity);

    // Each origin holds its live buckets plus the one emerging, and every node
    // holds a replica of every origin's set — so the cell store must cover
    // `MAX_NODES × (live_buckets + 1)`.
    assert!(
        full.max_cells.get() as usize >= MAX_NODES * (live_buckets + 1),
        "max_cells {} under the cross-node working set",
        full.max_cells,
    );
    // The node dictionary must intern every origin.
    assert!(
        usize::from(node_dictionary_capacity.get()) >= MAX_NODES,
        "node_dictionary_capacity {node_dictionary_capacity} cannot hold {MAX_NODES} origins",
    );

    // …and the shrink is real: even at the ceiling the store is a few
    // thousand cells, well under the production default.
    assert!((full.max_cells.get() as usize) < gabion::defaults::STORAGE_MAX_CELLS / 20);
}

/// The live runtime honors the shrunk, derived sizing: a cluster the size of
/// the heaviest existing test (N=32) takes the watched key spread across nodes
/// and buckets, gossips to convergence, and reports the *derived* (small) store
/// capacity with not a single eviction on any node.
#[test]
fn moderate_cluster_runtime_honors_the_shrunk_sizing() {
    const NODES: usize = 32;
    let config = SimConfig {
        tick_interval_ms: 500,
        rule_window_ms: 10_000,
        rule_bucket_ms: 1_000,
        ..test_config(NODES)
    };
    let caps = config.settings().expect("settings");

    with_engine(config, |tx| async move {
        // Eight origins across two buckets: enough cross-node replication to
        // populate every node's store, while light enough not to saturate the
        // sim transport.
        for node in [0u32, 8, 16, 24] {
            submit(&tx, node, WATCHED_KEY, 5).await;
        }
        step(&tx, 1_000).await;
        for node in [4u32, 12, 20, 28] {
            submit(&tx, node, WATCHED_KEY, 5).await;
        }
        // 16 rounds — ample for an 8-origin set to converge across 32 nodes —
        // while staying inside the 10 s window so the first batch (epoch 0)
        // does not age out (1 000 + 16 × 500 = 9 000 ms, the window
        // holds epoch 0).
        for _ in 0..16 {
            step(&tx, 500).await;
        }

        let state = snapshot(&tx).await;
        assert_eq!(state.nodes.len(), NODES);
        for node in &state.nodes {
            let s = &node.store_stats;
            assert_eq!(
                s.cell_store_full_rejects, 0,
                "node {} evicted cells (cell_capacity {})",
                node.id, s.cell_capacity,
            );
            assert_eq!(s.rule_dictionary_full_rejects, 0, "node {}", node.id);
            assert_eq!(s.node_dictionary_full_rejects, 0, "node {}", node.id);
            // The runtime was built with the derived (small) capacity, not a
            // production floor — proof the shrink is in effect end-to-end.
            assert_eq!(s.cell_capacity, caps.max_cells.get());
        }
        // The 40 hits (8 origins × 5) propagate to every node within the
        // window.
        assert_eq!(state.oracle_total, 40);
        assert!(
            state.nodes.iter().all(|n| n.aggregate_total == 40),
            "every node converges on 40: {:?}",
            state
                .nodes
                .iter()
                .map(|n| (n.id, n.aggregate_total))
                .collect::<Vec<_>>(),
        );
    });
}

/// Live growth must not overflow the derived caps: a cluster built small and
/// then grown by `add_node` (the visualizer's join, with no rebuild — see the
/// `live-node-membership-requirement`) interns *every* origin into a node
/// dictionary and cell store sized for the `MAX_NODES` ceiling, not the initial
/// `config.nodes`. Sizing from `config.nodes` regressed exactly here — growing
/// 12 → 24 overflowed the 14-slot dictionary; this locks the ceiling sizing.
#[test]
fn grown_cluster_holds_every_origin_without_overflow() {
    let config = SimConfig {
        tick_interval_ms: 500,
        rule_window_ms: 10_000,
        rule_bucket_ms: 1_000,
        ..test_config(12)
    };
    with_engine(config, |tx| async move {
        // Grow 12 → 24 by live joins (well past `config.nodes + slack`, the
        // point the old per-cluster sizing overflowed).
        for _ in 0..12 {
            add_node(&tx).await;
        }
        let grown = snapshot(&tx).await;
        assert_eq!(grown.nodes.len(), 24, "twelve joins grow the cluster to 24");

        // Every node originates, so all 24 origins must intern on every peer.
        for id in grown.nodes.iter().map(|n| n.id).collect::<Vec<_>>() {
            submit(&tx, id, WATCHED_KEY, 1).await;
        }
        for _ in 0..16 {
            step(&tx, 500).await;
        }

        let state = snapshot(&tx).await;
        for node in &state.nodes {
            let s = &node.store_stats;
            assert_eq!(
                s.node_dictionary_full_rejects, 0,
                "node {} overflowed its node dictionary after growth ({} of {} origins)",
                node.id, s.node_slots_used, s.node_slots_capacity,
            );
            assert_eq!(
                s.cell_store_full_rejects, 0,
                "node {} evicted cells after growth (cell_capacity {})",
                node.id, s.cell_capacity,
            );
        }
    });
}

/// A burst whose cell fully expires must not poison expiry for the *next*
/// burst. Regression for the rule-dictionary release bug: the engine
/// pre-interns the watched rule, but interning held no reference, so once the
/// first burst's cell aged out the rule slot was freed — and the next burst
/// re-interned the *default* descriptor (60 s window, `applies_locally =
/// false`), so it never expired in the 10 s window and never counted locally.
/// Drives whole ticks (deterministic; this is a local-store bug, no gossip
/// race needed): burst, age out, burst again, and confirm the second ages out
/// on the same 10 s schedule as the first.
#[test]
fn second_burst_after_expiry_still_ages_out() {
    let config = SimConfig {
        nodes: 3,
        rule_window_ms: 10_000,
        rule_bucket_ms: 1_000,
        rule_limit: 1_000_000,
        ..test_config(3)
    };
    with_engine(config, |tx| async move {
        // First burst, spread and aged out across the whole cluster.
        submit(&tx, 0, WATCHED_KEY, 25).await;
        for _ in 0..130 {
            step(&tx, 100).await; // 13 s ≫ the 10 s window
        }
        let after_first = snapshot(&tx).await;
        assert!(
            after_first.nodes.iter().all(|n| n.aggregate_total == 0),
            "first burst must age out everywhere: {:?}",
            after_first
                .nodes
                .iter()
                .map(|n| n.aggregate_total)
                .collect::<Vec<_>>(),
        );

        // Second burst, same key, same node — the rule slot was just released.
        submit(&tx, 0, WATCHED_KEY, 25).await;
        let after_second_submit = snapshot(&tx).await;
        assert!(
            after_second_submit
                .nodes
                .iter()
                .any(|n| n.aggregate_total == 25),
            "second burst must register on its origin"
        );

        // It must age out on the *same* 10 s schedule, not the 60 s default.
        for _ in 0..130 {
            step(&tx, 100).await;
        }
        let aged = snapshot(&tx).await;
        assert!(
            aged.nodes.iter().all(|n| n.aggregate_total == 0),
            "second burst must age out in the 10 s window like the first — a non-zero total here \
             means it was stored under the default 60 s descriptor: {:?}",
            aged.nodes
                .iter()
                .map(|n| n.aggregate_total)
                .collect::<Vec<_>>(),
        );
    });
}

/// A departed gateway whose zone still has members is reported gone too,
/// though no row left in that zone can replace it: node 0 has never heard
/// node 3, so node 1 was its only gateway into zone b. Only the origins that
/// used node 1 report it.
#[test]
fn a_departed_gateway_is_reported_gone_while_its_zone_lives_on() {
    with_state(zoned(4, 2), |mut state| async move {
        state.set_link_policy(0, 3, LinkPolicyKind::Block).unwrap();
        state.set_link_policy(3, 0, LinkPolicyKind::Block).unwrap();
        warm(&mut state).await;
        assert_eq!(gateway_of(&state, 0, 1).await, 1);
        let mut users = Vec::new();
        for origin in [0, 2] {
            if gateway_of(&state, origin, 1).await == 1 {
                users.push((origin, Some(1), None));
            }
        }
        let removal = state.remove_node(1).await.unwrap().events;
        assert_eq!(gateway_moves(&removal, 1), users);
        assert!(
            gateway_moves(&removal, 0).is_empty(),
            "no gateway into zone a moved"
        );
        state
    });
}

/// The snapshot carries the inspected node's rows alone, read from that
/// node's runtime, and none when no live node is inspected.
#[test]
fn snapshot_carries_only_the_inspected_nodes_rows() {
    with_state(zoned(6, 3), |mut state| async move {
        state.submit_request(4, WATCHED_KEY, 3).await.unwrap();
        warm(&mut state).await;
        let snap = state.snapshot(Some(4)).await;
        let rows = snap.inspected.expect("node 4 is live");
        assert_eq!(rows.id, 4);
        let peers: BTreeSet<_> = rows.peers.iter().filter_map(|p| p.id).collect();
        assert_eq!(peers, BTreeSet::from([0, 1, 2, 3, 5]), "node 4's peers");
        assert!(
            rows.cells.iter().any(|c| c.is_local && c.count == 3),
            "node 4's own cell"
        );
        let replica = cells_of(&state, 0).await;
        assert!(
            !replica.is_empty() && replica.iter().all(|c| !c.is_local && c.origin == Some(4)),
            "node 0 holds node 4's cell as a replica: {replica:?}"
        );
        assert!(state.snapshot(None).await.inspected.is_none());
        state.remove_node(4).await.unwrap();
        assert!(
            state.snapshot(Some(4)).await.inspected.is_none(),
            "a departed node has no rows"
        );
        state
    });
}

/// A join past `MAX_NODES` live nodes, or past the last id with a port, is
/// refused before it spends an id.
#[test]
fn a_join_past_the_node_or_id_ceiling_is_refused() {
    with_state(test_config(MAX_NODES), |mut state| async move {
        let refused = state.add_node(None).await;
        assert!(
            matches!(refused, Err(EngineError::ClusterFull)),
            "{:?}",
            refused.err()
        );
        assert_eq!(state.next_id, MAX_NODES as u32, "the refusal spent no id");
        state.remove_node(0).await.unwrap();
        state.next_id = super::MAX_ID + 1;
        let refused = state.add_node(None).await;
        assert!(
            matches!(refused, Err(EngineError::IdsExhausted)),
            "{:?}",
            refused.err()
        );
        state.next_id = super::MAX_ID;
        state.add_node(None).await.expect("the last id joins");
        state
    });
}

/// A join its settings refuse (here, more bootstrap peers than
/// `gossip.peer_capacity` holds) leaves no trace: no id spent, no member added.
#[test]
fn a_join_its_settings_refuse_spends_no_id() {
    with_state(test_config(3), |mut state| async move {
        let capacity = state.settings.peer_capacity;
        state.settings.peer_capacity = NonZeroU16::new(2).unwrap();
        let refused = state.add_node(None).await;
        assert!(
            matches!(
                refused,
                Err(EngineError::Setting(SettingError::TooManyBootstrapPeers {
                    listed: 3,
                    ..
                }))
            ),
            "{:?}",
            refused.err()
        );
        assert_eq!(state.next_id, 3, "the refusal spent no id");
        assert_eq!(state.nodes.len(), 3, "the refusal added no member");
        state.settings.peer_capacity = capacity;
        state.add_node(None).await.expect("the join succeeds");
        assert_eq!(
            state.nodes.last().map(|n| n.id),
            Some(3),
            "the joiner takes id 3"
        );
        state
    });
}
