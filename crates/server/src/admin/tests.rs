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

use gabion::crdt::{CellStore, NodeId, NodeIdentity};
use gabion::gossip::sim::SimRouter;
use gabion::gossip::{AdminCommand, GossipRuntime, TokioClock};
use gabion::rules::{DescriptorPattern, EnforcementMode, Rule, RuleTable};
use gabion::settings::Settings;

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
        let settings = Settings {
            max_cells: 256.try_into().unwrap(),
            rng_seed: Some(7),
            ..Settings::default()
        };
        let (mut gossip_config, storage) = settings.validate().expect("settings validate");
        let store = CellStore::<u32>::new(storage, identity);
        let counts = Arc::new(DashMapStore::<u32>::with_capacity(64));

        let bootstrap = next_addr();
        gossip_config.bootstrap_peers = vec![bootstrap];

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
                "decode_reject_count",
                "dirty_ticks",
                "forwarded_dirty_len",
                "last_effective_fanout",
                "last_error_budget",
                "local_dirty_len",
                "local_identity",
                "peak_effective_fanout",
                "peers",
                "rules",
                "store_stats",
                "threshold_fires",
                "ticks_total",
            ]
        );
        assert_eq!(keys(&body["local_identity"]), ["incarnation", "node_id"]);
        assert_eq!(
            keys(&body["counters"]),
            [
                "bytes_received",
                "bytes_sent",
                "encode_failures",
                "packets_received",
                "packets_sent",
                "send_blocked",
                "send_errors",
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
        assert_eq!(keys(&body["peers"][0]), ["addr", "node_id", "peer_slot"]);
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
            format!("{:#034x}", sample_rule().fingerprint)
        );
        assert_eq!(
            body["rules"][0]["domain_hash"],
            format!("{:#034x}", sample_rule().domain_hash.0)
        );
        assert_eq!(body["rules"][0]["mode"], "enforce");
        assert_eq!(body["aggregate_rows"], 0);

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
