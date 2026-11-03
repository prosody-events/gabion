//! Admin HTTP — endpoints that surface the running server's state for
//! operators. Composed of:
//!
//! - `GET /snapshot`           — top-level structured snapshot of the gossip
//!   runtime, rule table, and aggregate store.
//! - `GET /readyz`             — readiness probe: 200 whenever the admin HTTP
//!   server answers, and nothing more.
//!
//! `/snapshot`'s runtime part is built inside the gossip task in response
//! to an `AdminCommand` round-trip, so the handler never gets a `Sync`
//! borrow of the runtime's internals.

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

/// Serve the admin endpoints on `bind` until `shutdown` resolves, then drain
/// in-flight requests.
pub async fn serve<F>(bind: SocketAddr, state: AdminState, shutdown: F) -> std::io::Result<()>
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
) -> Result<Response, (StatusCode, &'static str)> {
    let snapshot = SnapshotJson {
        runtime: admin_snapshot(&state.gossip_admin).await?,
        rules: &state.rule_table,
        aggregate_rows: state.counts.table().occupied(),
    };
    Ok(Json(snapshot).into_response())
}

/// Readiness probe: `ready` whenever the admin HTTP server answers. It checks
/// nothing else, because gabiond exits when its gossip task or a listener
/// stops.
async fn readyz_handler() -> &'static str {
    "ready\n"
}

/// `/snapshot`'s answer once the gossip task has stopped (`admin_unavailable`).
const UNAVAILABLE: (StatusCode, &str) = (
    StatusCode::SERVICE_UNAVAILABLE,
    "The gossip task has stopped, so this node has no live state to report. Look for an earlier \
     `gossip_stopped` log line; the node is exiting or must be restarted.\n",
);

/// Ask the gossip task for its snapshot. A closed channel and a dropped reply
/// both mean the task has stopped.
async fn admin_snapshot(
    tx: &mpsc::Sender<AdminCommand>,
) -> Result<AdminSnapshot, (StatusCode, &'static str)> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.send(AdminCommand::Snapshot { reply: reply_tx })
        .await
        .map_err(|_| UNAVAILABLE)?;
    reply_rx.await.map_err(|_| UNAVAILABLE)
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
