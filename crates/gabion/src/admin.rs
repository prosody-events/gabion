use crate::RuleId;
use crate::SharedLimiter;
use crate::core::{CounterCell, Metrics, NodeIdentity, Rule, StorageSummary};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;

use crate::gossip_runtime::GossipAdminSnapshot;
use crate::gossip_runtime::SharedGossipAdminSnapshot;

const DEFAULT_DEBUG_LIMIT: usize = 64;

#[derive(Clone)]
pub struct AdminState {
    limiter: SharedLimiter,
    gossip: Option<SharedGossipAdminSnapshot>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AdminSnapshot {
    pub mode: &'static str,
    pub identity: NodeIdentity,
    pub storage: StorageSummary,
    pub metrics: Metrics,
}

#[derive(Clone, Debug, Serialize)]
pub struct RulesSnapshot {
    pub rules: Vec<Rule>,
    pub truncated: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct PeersSnapshot {
    pub active_peers: Vec<SocketAddr>,
    pub recent_peers: Vec<crate::gossip_runtime::RecentPeerSnapshot>,
    pub discovery_generation: u64,
    pub local_only: bool,
    pub discovery_stale: bool,
    pub truncated: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct IntrospectionSnapshot {
    pub mode: &'static str,
    pub identity: NodeIdentity,
    pub cluster_id_hash: u128,
    pub active_rule_ids: Vec<RuleId>,
    pub storage: StorageSummary,
    pub local_cells: Vec<CounterCell>,
    pub remote_cells: Vec<crate::gossip::CounterCell>,
    pub peers: PeersSnapshot,
    pub gossip: Option<GossipAdminSnapshotSummary>,
    pub metrics: Metrics,
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct GossipAdminSnapshotSummary {
    pub send_bytes: u64,
    pub recv_bytes: u64,
    pub merge_cells: u64,
    pub digest_mismatch: u64,
    pub truncated_frames: u64,
    pub auth_failures: u64,
    pub decode_errors: u64,
    pub dirty_overflow: u64,
    pub remote_active_cells: usize,
    pub remote_cell_capacity: usize,
    pub remote_dirty_ring_len: usize,
    pub remote_dirty_overflow: bool,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct DebugLimits {
    pub max_rules: Option<usize>,
    pub max_cells: Option<usize>,
    pub max_peers: Option<usize>,
}

impl DebugLimits {
    fn max_rules(self) -> usize {
        self.max_rules.unwrap_or(DEFAULT_DEBUG_LIMIT)
    }

    fn max_cells(self) -> usize {
        self.max_cells.unwrap_or(DEFAULT_DEBUG_LIMIT)
    }

    fn max_peers(self) -> usize {
        self.max_peers.unwrap_or(DEFAULT_DEBUG_LIMIT)
    }
}

impl AdminState {
    pub fn new(limiter: SharedLimiter, gossip: Option<SharedGossipAdminSnapshot>) -> Self {
        Self { limiter, gossip }
    }

    fn gossip_snapshot(&self) -> Option<GossipAdminSnapshot> {
        self.gossip
            .as_ref()
            .and_then(|snapshot| snapshot.lock().ok().map(|snapshot| snapshot.clone()))
    }
}

pub fn snapshot(limiter: &SharedLimiter) -> AdminSnapshot {
    match limiter.lock() {
        Ok(limiter) => AdminSnapshot {
            mode: "local_only",
            identity: limiter.identity(),
            storage: limiter.storage_summary(),
            metrics: limiter.metrics(),
        },
        Err(_) => AdminSnapshot {
            mode: "local_only",
            identity: NodeIdentity::default(),
            storage: StorageSummary::default(),
            metrics: Metrics::default(),
        },
    }
}

pub fn rules_snapshot(limiter: &SharedLimiter, limits: DebugLimits) -> RulesSnapshot {
    match limiter.lock() {
        Ok(limiter) => {
            let max_rules = limits.max_rules();
            let rules = limiter
                .rules()
                .iter()
                .take(max_rules)
                .cloned()
                .collect::<Vec<_>>();
            RulesSnapshot {
                truncated: limiter.rules().len() > rules.len(),
                rules,
            }
        }
        Err(_) => RulesSnapshot {
            rules: Vec::new(),
            truncated: false,
        },
    }
}

pub fn peers_snapshot(gossip: Option<GossipAdminSnapshot>, limits: DebugLimits) -> PeersSnapshot {
    match gossip {
        Some(gossip) => {
            let max_peers = limits.max_peers();
            let active_peers = gossip
                .active_peers
                .iter()
                .copied()
                .take(max_peers)
                .collect::<Vec<_>>();
            let recent_peers = gossip
                .recent_peers
                .iter()
                .copied()
                .take(max_peers)
                .collect::<Vec<_>>();
            PeersSnapshot {
                truncated: gossip.active_peers.len() > active_peers.len()
                    || gossip.recent_peers.len() > recent_peers.len(),
                active_peers,
                recent_peers,
                discovery_generation: gossip.discovery_generation,
                local_only: gossip.local_only,
                discovery_stale: gossip.discovery_stale,
            }
        }
        None => PeersSnapshot {
            active_peers: Vec::new(),
            recent_peers: Vec::new(),
            discovery_generation: 0,
            local_only: true,
            discovery_stale: false,
            truncated: false,
        },
    }
}

pub fn introspection_snapshot(state: &AdminState, limits: DebugLimits) -> IntrospectionSnapshot {
    let gossip = state.gossip_snapshot();
    let peers = peers_snapshot(gossip.clone(), limits);
    let mut truncated = peers.truncated;
    let max_rules = limits.max_rules();
    let max_cells = limits.max_cells();

    let (identity, storage, metrics, active_rule_ids, local_cells) = match state.limiter.lock() {
        Ok(limiter) => {
            let active_rule_ids = limiter
                .rules()
                .iter()
                .take(max_rules)
                .map(|rule| rule.id)
                .collect::<Vec<_>>();
            truncated |= limiter.rules().len() > active_rule_ids.len();
            let local_cells = limiter.cells().take(max_cells).collect::<Vec<_>>();
            truncated |= limiter.active_cells() > local_cells.len();
            (
                limiter.identity(),
                limiter.storage_summary(),
                limiter.metrics(),
                active_rule_ids,
                local_cells,
            )
        }
        Err(_) => (
            NodeIdentity::default(),
            StorageSummary::default(),
            Metrics::default(),
            Vec::new(),
            Vec::new(),
        ),
    };

    let (cluster_id_hash, remote_cells, gossip_summary) = match gossip {
        Some(gossip) => {
            let remote_cells = gossip
                .remote_cells_sample
                .iter()
                .copied()
                .take(max_cells)
                .collect::<Vec<_>>();
            truncated |= gossip.remote_active_cells > remote_cells.len();
            (
                gossip.cluster_id_hash,
                remote_cells,
                Some(GossipAdminSnapshotSummary {
                    send_bytes: gossip.metrics.send_bytes,
                    recv_bytes: gossip.metrics.recv_bytes,
                    merge_cells: gossip.metrics.merge_cells,
                    digest_mismatch: gossip.metrics.digest_mismatch,
                    truncated_frames: gossip.metrics.truncated,
                    auth_failures: gossip.metrics.auth_failures,
                    decode_errors: gossip.metrics.decode_errors,
                    dirty_overflow: gossip.metrics.dirty_overflow,
                    remote_active_cells: gossip.remote_active_cells,
                    remote_cell_capacity: gossip.remote_cell_capacity,
                    remote_dirty_ring_len: gossip.remote_dirty_ring_len,
                    remote_dirty_overflow: gossip.remote_dirty_overflow,
                }),
            )
        }
        None => (0, Vec::new(), None),
    };

    IntrospectionSnapshot {
        mode: "local_only",
        identity,
        cluster_id_hash,
        active_rule_ids,
        storage,
        local_cells,
        remote_cells,
        peers,
        gossip: gossip_summary,
        metrics,
        truncated,
    }
}

pub fn prometheus_metrics(state: &AdminState) -> String {
    let snapshot = snapshot(&state.limiter);
    let metrics = snapshot.metrics;
    let gossip = state.gossip_snapshot();
    let (local_only, discovery_stale, peers, gossip_metrics) = match &gossip {
        Some(gossip) => (
            gossip.local_only,
            gossip.discovery_stale,
            gossip.active_peers.len(),
            gossip.metrics,
        ),
        None => (true, false, 0, crate::gossip::GossipMetrics::default()),
    };
    format!(
        concat!(
            "limiter_mode{{mode=\"local_only\"}} 1\n",
            "limiter_local_only {}\n",
            "limiter_discovery_stale {}\n",
            "limiter_peers {}\n",
            "limiter_requests_total {}\n",
            "limiter_allowed_total {}\n",
            "limiter_rejected_total {}\n",
            "limiter_rejected_total{{reason=\"local_absolute\"}} {}\n",
            "limiter_rejected_total{{reason=\"global_estimate\"}} {}\n",
            "limiter_rejected_total{{reason=\"local_fallback\"}} {}\n",
            "limiter_overflow_key_total {}\n",
            "limiter_overflow_rejected_total {}\n",
            "limiter_active_keys {}\n",
            "limiter_active_cells {}\n",
            "limiter_dirty_ring_len {}\n",
            "limiter_dirty_overflow {}\n",
            "gossip_send_bytes_total {}\n",
            "gossip_recv_bytes_total {}\n",
            "gossip_merge_cells_total {}\n",
            "gossip_digest_mismatch_total {}\n",
            "gossip_auth_failures_total {}\n",
            "gossip_decode_errors_total {}\n"
        ),
        u8::from(local_only),
        u8::from(discovery_stale),
        peers,
        metrics.requests,
        metrics.allowed,
        metrics.rejected,
        metrics.local_absolute_rejected,
        metrics.global_estimate_rejected,
        metrics.local_fallback_rejected,
        metrics.overflow_key_uses,
        metrics.overflow_rejected,
        snapshot.storage.active_keys,
        snapshot.storage.active_cells,
        snapshot.storage.dirty_ring_len,
        u8::from(snapshot.storage.dirty_overflow),
        gossip_metrics.send_bytes,
        gossip_metrics.recv_bytes,
        gossip_metrics.merge_cells,
        gossip_metrics.digest_mismatch,
        gossip_metrics.auth_failures,
        gossip_metrics.decode_errors,
    )
}

pub fn router(limiter: SharedLimiter) -> Router {
    router_with_gossip(limiter, None)
}

pub fn router_for_runtime<H: crate::CountUpdateHandler>(runtime: crate::Runtime<H>) -> Router {
    router_with_gossip(
        Arc::clone(&runtime.inner.limiter),
        runtime.inner.admin_snapshot.clone(),
    )
}

pub fn router_with_gossip(
    limiter: SharedLimiter,
    gossip: Option<SharedGossipAdminSnapshot>,
) -> Router {
    let admin_state = Arc::new(AdminState::new(limiter, gossip));
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/debug/rules", get(debug_rules))
        .route("/debug/peers", get(debug_peers))
        .route("/debug/storage", get(debug_storage))
        .route("/debug/introspection", get(debug_introspection))
        .route("/state", get(state_endpoint))
        .with_state(admin_state)
}

pub async fn serve(bind: SocketAddr, limiter: SharedLimiter) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router(limiter)).await
}

pub async fn serve_for_runtime<H: crate::CountUpdateHandler>(
    bind: SocketAddr,
    runtime: crate::Runtime<H>,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router_for_runtime(runtime)).await
}

pub async fn serve_with_gossip(
    bind: SocketAddr,
    limiter: SharedLimiter,
    gossip: Option<SharedGossipAdminSnapshot>,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router_with_gossip(limiter, gossip)).await
}

async fn healthz() -> &'static str {
    "ok\n"
}

async fn readyz(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    if state.limiter.lock().is_ok() {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

async fn metrics(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    prometheus_metrics(&state)
}

async fn debug_rules(
    State(state): State<Arc<AdminState>>,
    Query(limits): Query<DebugLimits>,
) -> impl IntoResponse {
    Json(rules_snapshot(&state.limiter, limits))
}

async fn debug_peers(
    State(state): State<Arc<AdminState>>,
    Query(limits): Query<DebugLimits>,
) -> impl IntoResponse {
    Json(peers_snapshot(state.gossip_snapshot(), limits))
}

async fn debug_storage(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    Json(snapshot(&state.limiter).storage)
}

async fn debug_introspection(
    State(state): State<Arc<AdminState>>,
    Query(limits): Query<DebugLimits>,
) -> impl IntoResponse {
    Json(introspection_snapshot(&state, limits))
}

async fn state_endpoint(
    State(state): State<Arc<AdminState>>,
    Query(limits): Query<DebugLimits>,
) -> impl IntoResponse {
    Json(introspection_snapshot(&state, limits))
}
