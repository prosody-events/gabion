//! Admin HTTP — endpoints that surface the running server's state for
//! operators and the gossip-propagation bench. Composed of:
//!
//! - `GET /snapshot`           — top-level structured snapshot of the gossip
//!   runtime, rule table, and aggregate store.
//! - `GET /readyz`             — readiness probe that returns 200 once the
//!   admin router is serving (i.e. the gossip runtime + admin state are wired
//!   up).
//! - `GET /debug/introspection` — per-cell + per-peer detail view, used by the
//!   bench to compute remote-count convergence. Walks the `CellStore` via the
//!   `cell-dump` feature so it's off the hot path.
//!
//! All responses are built inside the gossip task in response to an
//! `AdminCommand` round-trip, so the caller never gets a `Sync` borrow
//! of the runtime's internals.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use gabion::crdt::CellStoreStats;
use gabion::gossip::{AdminCommand, AdminSnapshot, CellDumpSnapshot};
use gabion::rules::{DescriptorPattern, EnforcementMode, Rule, RuleId, RuleTable};

use crate::store::DashMapStore;
use gabion::crdt::Count;

/// Adapter state for the admin endpoint. Holds:
/// - the rule table for `/snapshot` to surface rules
/// - the DashMap aggregate (for size + sampling)
/// - the gossip admin channel (for runtime/peer/store stats)
pub struct AdminState<C: Count> {
    rule_table: Arc<RuleTable>,
    counts: Arc<DashMapStore<C>>,
    gossip_admin: mpsc::Sender<AdminCommand>,
}

impl<C: Count> AdminState<C> {
    pub fn new(
        rule_table: Arc<RuleTable>,
        counts: Arc<DashMapStore<C>>,
        gossip_admin: mpsc::Sender<AdminCommand>,
    ) -> Self {
        Self {
            rule_table,
            counts,
            gossip_admin,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Snapshot {
    pub identity: IdentitySnapshot,
    pub peers: Vec<PeerSnapshot>,
    pub store: StoreSnapshot,
    pub rules: Vec<RuleSnapshot>,
    pub gossip: GossipSnapshot,
}

#[derive(Debug, Serialize)]
pub struct IdentitySnapshot {
    pub node_id: String,
    pub incarnation: u32,
}

#[derive(Debug, Serialize)]
pub struct PeerSnapshot {
    pub addr: String,
    pub node_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StoreSnapshot {
    pub aggregate_rows: usize,
    pub cell_store: CellStoreStatsSnapshot,
}

#[derive(Debug, Serialize)]
pub struct CellStoreStatsSnapshot {
    pub active_cells: u32,
    pub cell_capacity: u32,
    pub rule_slots_used: u16,
    pub rule_slots_capacity: u16,
    pub node_slots_used: u16,
    pub node_slots_capacity: u16,
    pub cell_store_full_rejects: u64,
    pub rule_dictionary_full_rejects: u64,
    pub node_dictionary_full_rejects: u64,
}

impl From<CellStoreStats> for CellStoreStatsSnapshot {
    fn from(stats: CellStoreStats) -> Self {
        Self {
            active_cells: stats.active_cells,
            cell_capacity: stats.cell_capacity,
            rule_slots_used: stats.rule_slots_used,
            rule_slots_capacity: stats.rule_slots_capacity,
            node_slots_used: stats.node_slots_used,
            node_slots_capacity: stats.node_slots_capacity,
            cell_store_full_rejects: stats.cell_store_full_rejects,
            rule_dictionary_full_rejects: stats.rule_dictionary_full_rejects,
            node_dictionary_full_rejects: stats.node_dictionary_full_rejects,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RuleSnapshot {
    pub id: RuleId,
    pub fingerprint: String,
    pub domain: Box<str>,
    pub descriptors: Box<[DescriptorPatternSnapshot]>,
    pub limit: u64,
    pub window_millis: u64,
    pub bucket_millis: u64,
    /// One of `"enforce"`, `"dry_run"`, `"disabled"`.
    pub mode: &'static str,
}

#[derive(Debug, Serialize)]
pub struct DescriptorPatternSnapshot {
    pub key: Box<str>,
    pub value: Box<str>,
}

impl From<&DescriptorPattern> for DescriptorPatternSnapshot {
    fn from(p: &DescriptorPattern) -> Self {
        Self {
            key: p.key.clone(),
            value: p.value.clone(),
        }
    }
}

impl From<&Rule> for RuleSnapshot {
    fn from(rule: &Rule) -> Self {
        Self {
            id: rule.id,
            fingerprint: format!("{:032x}", rule.fingerprint),
            domain: rule.domain.clone(),
            descriptors: rule.descriptors.iter().map(Into::into).collect(),
            limit: rule.limit,
            window_millis: rule.window_millis,
            bucket_millis: rule.bucket_millis,
            mode: match rule.mode {
                EnforcementMode::Enforce => "enforce",
                EnforcementMode::DryRun => "dry_run",
                EnforcementMode::Disabled => "disabled",
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub struct GossipSnapshot {
    pub local_dirty_len: u32,
    pub forwarded_dirty_len: u32,
    pub send_pending_depth: usize,
}

/// Build an axum router exposing the admin endpoints. See the module
/// docs for the route map.
pub fn router<C: Count + 'static>(state: AdminState<C>) -> Router {
    Router::new()
        .route("/snapshot", get(snapshot_handler::<C>))
        .route("/readyz", get(readyz_handler))
        .route("/debug/introspection", get(introspection_handler::<C>))
        .with_state(Arc::new(state))
}

pub async fn serve<C: Count + 'static>(
    bind: SocketAddr,
    state: AdminState<C>,
) -> std::io::Result<()> {
    serve_with_shutdown(bind, state, std::future::pending()).await
}

/// Like [`serve`] but stops accepting new connections and drains in-flight
/// admin requests when `shutdown` resolves.
pub async fn serve_with_shutdown<C, F>(
    bind: SocketAddr,
    state: AdminState<C>,
    shutdown: F,
) -> std::io::Result<()>
where
    C: Count + 'static,
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await
}

async fn snapshot_handler<C: Count + 'static>(
    State(state): State<Arc<AdminState<C>>>,
) -> Result<Json<Snapshot>, (StatusCode, String)> {
    let gossip = admin_snapshot(&state.gossip_admin).await?;

    let rules = state.rule_table.iter().map(Into::into).collect();
    let snapshot = Snapshot {
        identity: IdentitySnapshot {
            node_id: format!("{:032x}", gossip.local_identity.node_id.0),
            incarnation: gossip.local_identity.incarnation,
        },
        peers: gossip
            .peers
            .into_iter()
            .map(|p| PeerSnapshot {
                addr: p.addr.to_string(),
                node_id: p.node_id.map(|id| format!("{:032x}", id.0)),
            })
            .collect(),
        store: StoreSnapshot {
            aggregate_rows: state.counts.len(),
            cell_store: gossip.store_stats.into(),
        },
        rules,
        gossip: GossipSnapshot {
            local_dirty_len: gossip.local_dirty_len,
            forwarded_dirty_len: gossip.forwarded_dirty_len,
            send_pending_depth: gossip.send_pending_depth,
        },
    };
    Ok(Json(snapshot))
}

/// Readiness probe. The mere fact that the admin router is serving HTTP
/// implies `AdminState` was built (which means the gossip runtime is
/// alive and the aggregate store is constructed), so a no-op `200 OK`
/// is the correct shape. Used by the bench's port-forward readiness
/// poll and by any operator-side liveness probe that wants something
/// cheaper than `/snapshot`.
async fn readyz_handler() -> &'static str {
    "ready\n"
}

/// Query string for `/debug/introspection`. Both fields are caps — the
/// runtime can have many more cells than the consumer wants to render.
#[derive(Debug, Deserialize)]
struct IntrospectionParams {
    #[serde(default = "default_max_cells")]
    max_cells: usize,
    #[serde(default = "default_max_peers")]
    max_peers: usize,
}

fn default_max_cells() -> usize {
    1024
}
fn default_max_peers() -> usize {
    64
}

#[derive(Debug, Serialize)]
struct IntrospectionResponse {
    identity: IdentitySnapshot,
    gossip: IntrospectionGossip,
    peers: IntrospectionPeers,
    remote_cells: Vec<IntrospectionCell>,
}

/// Gossip counters surfaced to the bench. Mirrors what the
/// `AdminSnapshot` already tracks; the chart-only fields the bench reads
/// (`merge_cells`, `send_bytes`, `recv_bytes`, `digest_mismatch`) are
/// zero-filled because the runtime does not currently count them. They
/// remain in the shape so the bench's JSON reader doesn't have to
/// branch — a future runtime change can populate them without touching
/// the consumer.
#[derive(Debug, Serialize)]
struct IntrospectionGossip {
    remote_active_cells: u64,
    decode_errors: u64,
    merge_cells: u64,
    send_bytes: u64,
    recv_bytes: u64,
    digest_mismatch: u64,
}

#[derive(Debug, Serialize)]
struct IntrospectionPeers {
    active_peers: Vec<PeerSnapshot>,
}

/// One CRDT cell whose origin is a remote node. Each entry's `count` is
/// the live observation count; the bench's `final_remote_total` is a
/// sum across this list.
#[derive(Debug, Serialize)]
struct IntrospectionCell {
    rule_fingerprint: String,
    key_hash: String,
    bucket: u64,
    count: u64,
    last_update_millis: u64,
    origin_node_id: Option<String>,
}

async fn introspection_handler<C: Count + 'static>(
    State(state): State<Arc<AdminState<C>>>,
    Query(params): Query<IntrospectionParams>,
) -> Result<Json<IntrospectionResponse>, (StatusCode, String)> {
    let snapshot = admin_snapshot(&state.gossip_admin).await?;
    let cell_dump = admin_cell_dump(&state.gossip_admin).await?;

    let local_node_id = snapshot.local_identity.node_id;

    // Filter to cells whose origin is *not* this node. Two gabiond pods
    // running the bench each contribute their local hits via gossip; on
    // any one pod, the "remote" view is everything not originated here.
    // The bench sums `count` across this list and compares against
    // `load.ok` — see `convergence_summary` in
    // `deploy/kubernetes/gossip-propagation-bench.py`.
    let mut remote_cells: Vec<IntrospectionCell> = cell_dump
        .cells
        .iter()
        .filter(|c| {
            c.origin_node_id
                .map(|id| id != local_node_id.0)
                .unwrap_or(true)
        })
        .take(params.max_cells)
        .map(|c| IntrospectionCell {
            rule_fingerprint: format!("{:032x}", c.rule_fingerprint),
            key_hash: format!("{:032x}", c.key_hash),
            bucket: c.bucket.into(),
            count: c.count,
            last_update_millis: c.last_update_millis,
            origin_node_id: c.origin_node_id.map(|id| format!("{:032x}", id)),
        })
        .collect();
    // Stable ordering so successive samples render predictably and
    // truncation is deterministic when the runtime holds more cells than
    // the cap.
    remote_cells.sort_by(|a, b| {
        (&a.rule_fingerprint, &a.key_hash, a.bucket).cmp(&(
            &b.rule_fingerprint,
            &b.key_hash,
            b.bucket,
        ))
    });

    let active_peers: Vec<PeerSnapshot> = snapshot
        .peers
        .into_iter()
        .take(params.max_peers)
        .map(|p| PeerSnapshot {
            addr: p.addr.to_string(),
            node_id: p.node_id.map(|id| format!("{:032x}", id.0)),
        })
        .collect();

    let remote_active_cells = remote_cells.len() as u64;

    let response = IntrospectionResponse {
        identity: IdentitySnapshot {
            node_id: format!("{:032x}", snapshot.local_identity.node_id.0),
            incarnation: snapshot.local_identity.incarnation,
        },
        gossip: IntrospectionGossip {
            remote_active_cells,
            decode_errors: snapshot.decode_reject_count,
            merge_cells: 0,
            send_bytes: 0,
            recv_bytes: 0,
            digest_mismatch: 0,
        },
        peers: IntrospectionPeers { active_peers },
        remote_cells,
    };
    Ok(Json(response))
}

/// Issue an `AdminCommand::Snapshot` and await the reply. Maps both
/// failure modes (send-side closed, reply-side dropped) onto the same
/// 503 shape `/snapshot` uses, since they have the same operator
/// remedy.
async fn admin_snapshot(
    tx: &mpsc::Sender<AdminCommand>,
) -> Result<AdminSnapshot, (StatusCode, String)> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(AdminCommand::Snapshot { reply: reply_tx })
        .await
        .map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "gossip runtime is shut down".to_string(),
            )
        })?;
    reply_rx.await.map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "gossip runtime dropped admin reply".to_string(),
        )
    })
}

async fn admin_cell_dump(
    tx: &mpsc::Sender<AdminCommand>,
) -> Result<CellDumpSnapshot, (StatusCode, String)> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(AdminCommand::CellDump { reply: reply_tx })
        .await
        .map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "gossip runtime is shut down".to_string(),
            )
        })?;
    reply_rx.await.map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "gossip runtime dropped admin reply".to_string(),
        )
    })
}

/// Default channel size for the admin command receiver. Admin requests are
/// infrequent — a tiny queue is plenty.
pub const DEFAULT_ADMIN_CHANNEL_SIZE: usize = 8;

/// Convenience: build the mpsc pair the runtime + admin module need.
pub fn admin_channel() -> (mpsc::Sender<AdminCommand>, mpsc::Receiver<AdminCommand>) {
    mpsc::channel(DEFAULT_ADMIN_CHANNEL_SIZE)
}

#[cfg(test)]
mod tests;
