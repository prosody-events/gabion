//! Tests for the parent [`crate::admin`] module — the axum router that
//! surfaces the gossip [`AdminCommand`] snapshot over HTTP.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::LocalSet;
use tower::ServiceExt;

use gabion::aggregate::Aggregate;
use gabion::crdt::{
    CellHandle, CellIdentity, CellStore, DeltaSink, ExpirationSink, KeyHash, NodeId, NodeIdentity,
};
use gabion::gossip::sim::SimRouter;
use gabion::gossip::{AdminCommand, AggregateStore, GossipRuntime, TokioClock};
use gabion::rules::{EnforcementMode, RuleDef, RuleTable};
use gabion::settings::Settings;

use crate::admin::{AdminState, admin_channel, router};

static TEST_PORT: AtomicU16 = AtomicU16::new(41_500);

fn next_addr() -> SocketAddr {
    let port = TEST_PORT.fetch_add(1, Ordering::Relaxed);
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn run_local<F: std::future::Future<Output = T>, T>(f: F) -> T {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    let local = LocalSet::new();
    local.block_on(&rt, f)
}

/// A small node's settings.
fn settings() -> Settings {
    Settings {
        max_cells: 256.try_into().unwrap(),
        ..Settings::default()
    }
}

/// One `tenant:*` rule.
fn sample_rules() -> RuleTable {
    let rule = RuleDef {
        name: "tenant",
        domain: "api",
        patterns: &[("tenant", "*")],
        rate: "10r/s",
        window: None,
        bucket: Some(std::time::Duration::from_millis(100)),
        mode: EnforcementMode::Enforce,
    }
    .build(&settings().validate().unwrap().1.cardinality)
    .unwrap();
    RuleTable::new(Box::new([rule]), None).unwrap()
}

fn build_router(gossip_admin: mpsc::Sender<AdminCommand>) -> axum::Router {
    let counts = Arc::new(Aggregate::new(&settings().validate().unwrap().1));
    let state = AdminState::new(Arc::new(sample_rules()), counts, gossip_admin);
    router(state)
}

/// The `/snapshot` JSON is the library's snapshot flattened, with the rules
/// and the aggregate row count, so a renamed field or an id written in decimal
/// changes this shape.
#[test]
fn snapshot_json_shape_is_pinned() {
    run_local(async {
        let sim = SimRouter::new();
        let bind_addr = next_addr();
        let transport = sim.bind(bind_addr);
        let identity = NodeIdentity::new(NodeId(0xBEEF_CAFE), 11);
        let (mut gossip_config, storage) = settings().validate().expect("settings validate");
        let rule_table = Arc::new(sample_rules());
        let store = CellStore::new(storage, identity, &rule_table);
        let counts = Arc::new(Aggregate::new(&storage));
        let row = DeltaSink {
            handles: vec![CellHandle::default()],
            keys: vec![CellIdentity {
                rule_fingerprint: rule_table.rules()[0].fingerprint(),
                key_hash: KeyHash(1),
                bucket: 0,
            }],
            previous: vec![0],
            current: vec![1],
            deltas: vec![1],
            applies_locally: vec![1],
        };
        counts.apply(&row, &ExpirationSink::default());

        let bootstrap = next_addr();
        gossip_config.bootstrap_peers = vec![bootstrap];
        let tick = settings().tick_interval.duration();

        let (admin_tx, admin_rx) = admin_channel();
        let (rt, client) = GossipRuntime::from_parts_with_admin(
            transport,
            TokioClock::from_millis(0),
            gossip_config,
            store,
            counts.clone(),
            Some(admin_rx),
        );
        let handle = tokio::task::spawn_local(rt.run(futures::stream::empty()));

        let state = AdminState::new(rule_table.clone(), counts, admin_tx);
        let app = router(state);
        let snapshot = async || {
            let request = Request::builder().uri("/snapshot").body(Body::empty());
            let response =
                (app.clone().oneshot(request.expect("request")).await).expect("router responded");
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("body");
            serde_json::from_slice::<Value>(&bytes).expect("json")
        };
        let body = snapshot().await;

        let keys = |value: &Value| {
            let mut keys: Vec<String> =
                value.as_object().expect("object").keys().cloned().collect();
            keys.sort();
            keys
        };
        assert_eq!(
            keys(&body),
            [
                "aggregate_rows",
                "counters",
                "last_error_budget",
                "local_identity",
                "peers",
                "peers_by_role",
                "rules",
                "store_stats",
            ]
        );
        assert_eq!(
            body["peers_by_role"],
            serde_json::json!({
                "unheard": 1,
                "zone": 0,
                "gateway": 0,
                "remote": 0,
                "incompatible": 0
            })
        );
        assert_eq!(keys(&body["local_identity"]), ["incarnation", "node_id"]);
        assert_eq!(
            keys(&body["counters"]),
            [
                "bytes_received",
                "bytes_sent",
                "bytes_sent_cross_zone",
                "cells_merged",
                "decode_rejects_cluster",
                "decode_rejects_framing",
                "decode_rejects_peer_limits",
                "decode_rejects_version",
                "gap_reports_sent",
                "gaps_abandoned",
                "gateway_failovers",
                "liveness_probes",
                "packets_received",
                "packets_sent",
                "peers_declined",
                "relay_stalls",
                "role_changes",
                "rounds_heartbeat",
                "rounds_prompt",
                "rounds_threshold",
                "rto_rewinds_own",
                "rto_rewinds_relay",
                "send_blocked",
                "send_errors",
                "sweep_cells_sent",
                "sweeps_ignored",
                "sweeps_refused",
                "sweeps_requested",
                "sweeps_served",
            ]
        );
        assert_eq!(
            keys(&body["store_stats"]),
            [
                "active_cells",
                "cell_capacity",
                "cell_store_full_rejects",
                "node_dictionary_full_rejects",
                "node_slots_capacity",
                "node_slots_used",
                "rule_dictionary_full_rejects",
                "rule_slots_capacity",
                "rule_slots_used",
            ]
        );
        assert_eq!(
            keys(&body["peers"][0]),
            [
                "addr",
                "answer",
                "node_id",
                "own_unacked",
                "relay_unacked",
                "role",
                "unanswered_ms",
                "zone"
            ]
        );
        assert_eq!(body["peers"][0]["role"], "unheard");
        assert_eq!(body["peers"][0]["zone"], "unknown");
        // A wait is shown exactly while an answer is awaited: none before
        // the first send, and from the heartbeat that sends a hit.
        assert_eq!(body["peers"][0]["answer"], "settled");
        assert!(body["peers"][0]["unanswered_ms"].is_null());
        let fingerprint = rule_table.rules()[0].fingerprint();
        (client
            .record(fingerprint, KeyHash(2), 0, 1, 1_000_000, 0)
            .await)
            .expect("recorded");
        tokio::time::sleep(tick + tick / 2).await;
        let peer = &snapshot().await["peers"][0];
        assert_eq!(peer["answer"], "owed");
        assert_eq!(peer["unanswered_ms"], tick.as_millis() as u64 / 2);
        assert_eq!(
            keys(&body["rules"][0]),
            [
                "bucket_millis",
                "descriptors",
                "domain",
                "domain_hash",
                "fingerprint",
                "id",
                "limit",
                "live_buckets",
                "mode",
                "name",
                "window_millis",
            ]
        );
        assert_eq!(keys(&body["rules"][0]["descriptors"][0]), ["key", "value"]);

        assert_eq!(
            body["local_identity"]["node_id"],
            format!("{:#034x}", 0xBEEF_CAFE_u128)
        );
        assert_eq!(body["local_identity"]["incarnation"], 11);
        assert_eq!(body["peers"][0]["addr"], bootstrap.to_string());
        assert!(body["peers"][0]["node_id"].is_null());
        assert_eq!(
            body["rules"][0]["fingerprint"],
            format!("{:#034x}", rule_table.rules()[0].fingerprint())
        );
        assert!(
            body["rules"][0]["domain_hash"]
                .as_str()
                .is_some_and(|hash| hash.len() == 34 && hash.starts_with("0x"))
        );
        assert_eq!(body["rules"][0]["mode"], "enforce");
        assert_eq!(body["aggregate_rows"], 1);

        client.shutdown().await.unwrap();
        let _ = handle.await;
    });
}

#[test]
fn readyz_returns_200_with_static_body() {
    run_local(async {
        // `/readyz` is intentionally independent of the gossip runtime: a
        // liveness probe must answer while the runtime is still converging
        // on peers.
        let (admin_tx, admin_rx) = admin_channel();
        drop(admin_rx);

        let app = build_router(admin_tx);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("router responded");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024).await.expect("body");
        let text = std::str::from_utf8(&bytes).expect("utf8");
        assert!(text.contains("ready"), "unexpected body: {text:?}");
    });
}

#[test]
fn snapshot_returns_service_unavailable_when_gossip_is_down() {
    run_local(async {
        // No runtime ever started — drop the receiver so the admin
        // sender's first `send` errs, exercising the 503 path.
        let (admin_tx, admin_rx) = admin_channel();
        drop(admin_rx);

        let app = build_router(admin_tx);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/snapshot")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("router responded");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let bytes = to_bytes(response.into_body(), 4 * 1024)
            .await
            .expect("body");
        let text = std::str::from_utf8(&bytes).expect("utf8");
        assert!(
            text.contains("gossip runtime is shut down"),
            "unexpected error body: {text:?}",
        );
    });
}
