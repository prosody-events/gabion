//! Admin HTTP — endpoints that surface the running server's state for
//! operators. Composed of:
//!
//! - `GET /snapshot`           — top-level structured snapshot of the gossip
//!   runtime, rule table, and aggregate store.
//! - `GET /readyz`             — readiness probe that returns 200 once the
//!   admin router is serving (i.e. the gossip runtime + admin state are wired
//!   up).
//!
//! All responses are built inside the gossip task in response to an
//! `AdminCommand` round-trip, so the caller never gets a `Sync` borrow
//! of the runtime's internals.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

use gabion::aggregate::Aggregate;
use gabion::gossip::{AdminCommand, AdminSnapshot};
use gabion::rules::RuleTable;

/// Adapter state for the admin endpoint. Holds:
/// - the rule table for `/snapshot` to surface rules
/// - the aggregate table (for its row count)
/// - the gossip admin channel (for runtime/peer/store stats)
pub struct AdminState {
    rule_table: Arc<RuleTable>,
    counts: Arc<Aggregate>,
    gossip_admin: mpsc::Sender<AdminCommand>,
}

impl AdminState {
    pub fn new(
        rule_table: Arc<RuleTable>,
        counts: Arc<Aggregate>,
        gossip_admin: mpsc::Sender<AdminCommand>,
    ) -> Self {
        Self {
            rule_table,
            counts,
            gossip_admin,
        }
    }
}

/// What `/snapshot` serves: the runtime's snapshot, flattened, plus what only
/// this adapter knows.
#[derive(Serialize)]
struct SnapshotJson<'a> {
    #[serde(flatten)]
    runtime: AdminSnapshot,
    rules: &'a RuleTable,
    aggregate_rows: usize,
}

/// Build an axum router exposing the admin endpoints. See the module
/// docs for the route map.
pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/snapshot", get(snapshot_handler))
        .route("/readyz", get(readyz_handler))
        .with_state(Arc::new(state))
}

pub async fn serve(bind: SocketAddr, state: AdminState) -> std::io::Result<()> {
    serve_with_shutdown(bind, state, std::future::pending()).await
}

/// Like [`serve`] but stops accepting new connections and drains in-flight
/// admin requests when `shutdown` resolves.
pub async fn serve_with_shutdown<F>(
    bind: SocketAddr,
    state: AdminState,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await
}

async fn snapshot_handler(
    State(state): State<Arc<AdminState>>,
) -> Result<Response, (StatusCode, String)> {
    let snapshot = SnapshotJson {
        runtime: admin_snapshot(&state.gossip_admin).await?,
        rules: &state.rule_table,
        aggregate_rows: state.counts.table().occupied(),
    };
    Ok(Json(snapshot).into_response())
}

/// Readiness probe. The mere fact that the admin router is serving HTTP
/// implies `AdminState` was built (which means the gossip runtime is
/// alive and the aggregate store is constructed), so a no-op `200 OK`
/// is the correct shape. Fits any operator-side liveness probe that wants
/// something cheaper than `/snapshot`.
async fn readyz_handler() -> &'static str {
    "ready\n"
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

/// Default channel size for the admin command receiver. Admin requests are
/// infrequent — a tiny queue is plenty.
pub const DEFAULT_ADMIN_CHANNEL_SIZE: usize = 8;

/// Convenience: build the mpsc pair the runtime + admin module need.
pub fn admin_channel() -> (mpsc::Sender<AdminCommand>, mpsc::Receiver<AdminCommand>) {
    mpsc::channel(DEFAULT_ADMIN_CHANNEL_SIZE)
}

#[cfg(test)]
mod tests;
