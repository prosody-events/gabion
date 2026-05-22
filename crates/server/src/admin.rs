//! Admin HTTP — a single endpoint that returns a structured snapshot of
//! the running server, including the gossip runtime's view via the
//! [`gabion::gossip::AdminCommand`] channel.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

use gabion::crdt::CellStoreStats;
use gabion::gossip::AdminCommand;
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

/// Build an axum router with a single `GET /snapshot` endpoint.
pub fn router<C: Count + 'static>(state: AdminState<C>) -> Router {
    Router::new()
        .route("/snapshot", get(snapshot_handler::<C>))
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
    let (tx, rx) = oneshot::channel();
    state
        .gossip_admin
        .send(AdminCommand::Snapshot { reply: tx })
        .await
        .map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "gossip runtime is shut down".to_string(),
            )
        })?;
    let gossip = rx.await.map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "gossip runtime dropped admin reply".to_string(),
        )
    })?;

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

/// Default channel size for the admin command receiver. Admin requests are
/// infrequent — a tiny queue is plenty.
pub const DEFAULT_ADMIN_CHANNEL_SIZE: usize = 8;

/// Convenience: build the mpsc pair the runtime + admin module need.
pub fn admin_channel() -> (mpsc::Sender<AdminCommand>, mpsc::Receiver<AdminCommand>) {
    mpsc::channel(DEFAULT_ADMIN_CHANNEL_SIZE)
}

#[cfg(test)]
mod tests;
