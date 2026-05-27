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

use gabion::crdt::{CellStore, CellStoreConfig, NodeId, NodeIdentity};
use gabion::gossip::sim::SimRouter;
use gabion::gossip::{AdminCommand, GossipConfig, GossipRuntime, TokioClock};
use gabion::rules::{DescriptorPattern, EnforcementMode, Rule, RuleTable};

use crate::admin::{AdminState, admin_channel, router};
use crate::store::DashMapStore;

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

fn sample_rule() -> Rule {
    Rule::new(
        1,
        "api",
        vec![DescriptorPattern {
            key: "tenant".into(),
            value: "*".into(),
        }],
        10,
        1_000,
        100,
        EnforcementMode::Enforce,
    )
}

fn build_router(gossip_admin: mpsc::Sender<AdminCommand>) -> axum::Router {
    let rule_table = Arc::new(RuleTable::new(vec![sample_rule()]));
    let counts = Arc::new(DashMapStore::<u32>::with_capacity(64));
    let state = AdminState::new(rule_table, counts, gossip_admin);
    router(state)
}

#[test]
fn snapshot_returns_full_state_via_axum_route() {
    run_local(async {
        let sim = SimRouter::new();
        let bind_addr = next_addr();
        let transport = sim.bind(bind_addr);
        let identity = NodeIdentity::new(NodeId(0xBEEF_CAFE), 11);
        let store = CellStore::<u32>::new(CellStoreConfig::default(), identity);
        let counts = Arc::new(DashMapStore::<u32>::with_capacity(64));

        let bootstrap = next_addr();
        let gossip_config = GossipConfig {
            local_identity: identity,
            bootstrap_peers: vec![bootstrap],
            rng_seed: 7,
            ..GossipConfig::default()
        };

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

        let rule_table = Arc::new(RuleTable::new(vec![sample_rule()]));
        let state = AdminState::new(rule_table, counts, admin_tx);
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/snapshot")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("router responded");
        assert_eq!(response.status(), StatusCode::OK);

        let bytes = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");

        let node_id = body["identity"]["node_id"].as_str().expect("node_id");
        assert!(!node_id.is_empty());
        assert!(
            node_id.chars().all(|c| c.is_ascii_hexdigit()),
            "node_id should be hex: {node_id:?}",
        );
        assert_eq!(body["identity"]["incarnation"], 11);

        let peers = body["peers"].as_array().expect("peers");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0]["addr"], bootstrap.to_string());

        // Full CellStoreStats projection — these are the regression
        // targets for a serde rename or removed field.
        let stats = &body["store"]["cell_store"];
        for field in [
            "active_cells",
            "cell_capacity",
            "rule_slots_used",
            "rule_slots_capacity",
            "node_slots_used",
            "node_slots_capacity",
            "cell_store_full_rejects",
            "rule_dictionary_full_rejects",
            "node_dictionary_full_rejects",
        ] {
            assert!(
                stats.get(field).is_some(),
                "missing store.cell_store.{field} in snapshot",
            );
        }

        assert!(body["rules"].is_array(), "rules array missing");
        assert!(body["gossip"]["local_dirty_len"].is_u64());
        assert!(body["gossip"]["forwarded_dirty_len"].is_u64());
        assert!(body["gossip"]["send_pending_depth"].is_u64());

        client.shutdown().await.unwrap();
        let _ = handle.await;
    });
}

#[test]
fn readyz_returns_200_with_static_body() {
    run_local(async {
        // `/readyz` is intentionally independent of the gossip runtime —
        // the bench needs it to confirm the port-forward attached
        // before it starts sampling, and the runtime might still be
        // converging on peers at that point.
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
fn introspection_returns_remote_cells_and_active_peers() {
    run_local(async {
        let sim = SimRouter::new();
        let bind_addr = next_addr();
        let transport = sim.bind(bind_addr);
        let identity = NodeIdentity::new(NodeId(0xBEEF_CAFE), 11);
        let store = CellStore::<u32>::new(CellStoreConfig::default(), identity);
        let counts = Arc::new(DashMapStore::<u32>::with_capacity(64));

        let bootstrap = next_addr();
        let gossip_config = GossipConfig {
            local_identity: identity,
            bootstrap_peers: vec![bootstrap],
            rng_seed: 7,
            ..GossipConfig::default()
        };

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

        let rule_table = Arc::new(RuleTable::new(vec![sample_rule()]));
        let state = AdminState::new(rule_table, counts, admin_tx);
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/debug/introspection?max_cells=128&max_peers=8")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("router responded");
        assert_eq!(response.status(), StatusCode::OK);

        let bytes = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");

        // The bench reads `peers.active_peers` (nested), not a top-level
        // `peers` list — regression target if anyone "flattens" the
        // shape.
        let active_peers = body["peers"]["active_peers"]
            .as_array()
            .expect("peers.active_peers");
        assert_eq!(active_peers.len(), 1);
        assert_eq!(active_peers[0]["addr"], bootstrap.to_string());

        // `remote_cells` is the list the bench sums over for
        // `final_remote_total`. A fresh runtime with no inbound traffic
        // has nothing, but the field must exist as an array.
        assert!(body["remote_cells"].is_array(), "remote_cells missing");
        // Gossip chart-only fields must serialize even when the runtime
        // doesn't track them yet — the bench reader does
        // `gossip.get("merge_cells", 0)` and similar.
        for field in [
            "remote_active_cells",
            "decode_errors",
            "merge_cells",
            "send_bytes",
            "recv_bytes",
            "digest_mismatch",
        ] {
            assert!(
                body["gossip"].get(field).is_some(),
                "missing gossip.{field}",
            );
        }

        client.shutdown().await.unwrap();
        let _ = handle.await;
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
