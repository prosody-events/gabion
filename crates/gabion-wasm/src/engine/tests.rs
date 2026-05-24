//! Native tests for the engine, run with `cargo nextest` (no wasm toolchain).
//!
//! These exercise the exact `run_engine` the wasm `Sim` spawns: the engine is
//! spawned as a `spawn_local` task on a paused current-thread runtime and
//! driven over the command channel, validating "spawn once, drive forever"
//! (Phase 0.5) and the `CellDump` + logging shims (Phase 1).

use std::future::Future;

use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot};
use tokio::task::LocalSet;

use super::{Command, LinkPolicyKind, run_engine};
use crate::config::SimConfig;
use crate::event::{ClusterState, EventBatch, EventKind};

const WATCHED_KEY: u128 = 0x1;

fn test_config(nodes: usize) -> SimConfig {
    SimConfig {
        nodes,
        fanout: 3,
        tick_interval_ms: 100,
        rule_window_ms: 60_000,
        rule_bucket_ms: 1_000,
        rule_limit: 1_000_000,
        rng_seed: 42,
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
        let engine = tokio::task::spawn_local(run_engine(config, rx));
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
    let (reply, rx) = oneshot::channel();
    tx.send(Command::Snapshot { reply }).await.unwrap();
    rx.await.unwrap()
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
    tx.send(Command::AddNode { reply }).await.unwrap();
    rx.await.unwrap()
}

async fn remove_node(tx: &mpsc::Sender<Command>, id: u32) -> EventBatch {
    let (reply, rx) = oneshot::channel();
    tx.send(Command::RemoveNode { id, reply }).await.unwrap();
    rx.await.unwrap().expect("remove_node succeeded")
}

fn node_ids(state: &ClusterState) -> Vec<u32> {
    state.nodes.iter().map(|n| n.id).collect()
}

/// Phase 0.5: a spawned engine survives many `step` commands, with the gossip
/// tick index and per-node `ticks_total` accumulating monotonically.
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
            state.nodes.iter().all(|n| n.ticks_total > 0),
            "every runtime should have fired heartbeat ticks"
        );
    });
}

/// Phase 1: submitting a request creates a cell on the local node (visible
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
            before.nodes[0]
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
            after.nodes[1]
                .cells
                .iter()
                .any(|c| c.key == WATCHED_KEY && c.count == 5 && !c.is_local),
            "node 1 holds the cell as a remote origin"
        );
    });
}

/// Phase 1: a blocked link stops propagation (the logging transport records
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

/// Determinism: the same `(seed, config, command-script)` produces an
/// identical event log — the invariant the shareable-URL replay relies on.
#[test]
fn same_seed_and_script_produce_identical_events() {
    fn run() -> Vec<EventBatch> {
        let mut batches = Vec::new();
        // N=16 so the determinism check exercises ticks arriving *while*
        // packets are in flight across a sizeable fan — the combined load
        // under which a coalescing (watch) tick source diverged.
        with_engine(test_config(16), |tx| {
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
        "identical seed + script must yield identical event batches"
    );
    assert!(
        first.iter().any(|b| !b.events.is_empty()),
        "the script should produce some events"
    );
}

/// At a larger cluster the per-node tick channel must deliver *every* fired
/// tick — no coalescing — so each runtime's `ticks_total` equals the number of
/// whole ticks stepped, identically across nodes and across runs. (A `watch`
/// tick source failed exactly here: a busy runtime silently dropped ticks.)
#[test]
fn many_nodes_receive_every_tick_deterministically() {
    fn run() -> Vec<u64> {
        let mut totals = Vec::new();
        with_engine(test_config(32), |tx| {
            let totals = &mut totals;
            async move {
                // 1000 ms at 100 ms/tick = 10 whole ticks, and no requests (so
                // no threshold fires): every node's ticks_total must be 10.
                step(&tx, 1_000).await;
                let state = snapshot(&tx).await;
                *totals = state.nodes.iter().map(|n| n.ticks_total).collect();
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
    // Round-trips back to the same u128.
    let back: crate::event::Event = serde_json::from_value(json).unwrap();
    assert_eq!(back, event);
}
