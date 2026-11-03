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

use super::{Command, LinkPolicyKind, VIZ_ROUTER_CAPACITY, run_engine};
use crate::config::{MAX_NODES, SimConfig};
use crate::event::{ClusterState, EventBatch, EventKind};

const WATCHED_KEY: u128 = 0x1;

fn test_config(nodes: usize) -> SimConfig {
    SimConfig {
        nodes,
        tick_interval_ms: 100,
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

/// Phase 0.5: a spawned engine survives many `step` commands, with the gossip
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

/// The error budget the most recent request computed reaches the snapshot,
/// so the inspector shows the threshold a burst must cross.
#[test]
fn error_budget_surfaces_in_snapshot() {
    let config = SimConfig {
        rule_limit: 1_000,
        target_err_bps: 100,
        ..test_config(12)
    };
    with_engine(config, |tx| async move {
        submit(&tx, 0, 0, 7).await;
        let origin = snapshot(&tx)
            .await
            .nodes
            .into_iter()
            .find(|n| n.id == 0)
            .expect("node 0 present");
        assert!(
            origin.error_budget > 0,
            "the error budget must be recorded once a request is seen",
        );
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

/// Determinism: the same `(config, command-script)` produces an identical
/// event log — the invariant the shareable-URL replay relies on.
#[test]
fn same_script_produces_identical_events() {
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
        "identical script must yield identical event batches"
    );
    assert!(
        first.iter().any(|b| !b.events.is_empty()),
        "the script should produce some events"
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

/// One heartbeat sends a new count straight to every peer, and the submit
/// alone sends nothing. `hits` stays well under the eager-flush budget
/// (`epsilon = rule_limit × bps / (10_000 × peers) = 1_000_000 / 1_100 ≈ 909`)
/// so no threshold round runs.
#[test]
fn one_heartbeat_reaches_every_peer_directly() {
    // A node "knows" the count if it holds the cell (a nonzero aggregate, since
    // the single key is the only traffic).
    fn known(state: &ClusterState) -> usize {
        state.nodes.iter().filter(|n| n.aggregate_total > 0).count()
    }

    with_engine(test_config(12), |tx| async move {
        submit(&tx, 0, WATCHED_KEY, 5).await;
        assert_eq!(
            known(&snapshot(&tx).await),
            1,
            "the submit alone gossips nothing"
        );
        step(&tx, 100).await;
        assert_eq!(known(&snapshot(&tx).await), 12);
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
    // Round-trips back to the same u128.
    let back: crate::event::Event = serde_json::from_value(json).unwrap();
    assert_eq!(back, event);
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
