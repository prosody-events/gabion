//! Production binary: ties the gossip runtime, gRPC service, and admin
//! endpoint together. Single-threaded `current_thread` runtime under a
//! `LocalSet` because the gossip runtime is `!Send`.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use futures::StreamExt;
use tokio::task::LocalSet;

use gabion::crdt::CellStore;
use gabion::discovery::{self, PeerDiscovery, PeerEvent};
use gabion::gossip::GossipRuntime;

use gabion_server::admin::{self, AdminState};
use gabion_server::config::{AppConfig, ConfigError};
use gabion_server::identity::derive_identity;
use gabion_server::store::DashMapStore;
use gabion_server::{SharedLimiter, serve};

fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    let local = LocalSet::new();
    local.block_on(&runtime, run())
}

async fn run() -> anyhow::Result<()> {
    let config = load_config()?;

    let identity = derive_identity(config.runtime.node_id_seed.as_deref());
    let rule_table = Arc::new(config.rule_table()?);
    let cell_store = CellStore::<u32>::new(config.cell_store_config(), identity);
    let counts = Arc::new(DashMapStore::<u32>::with_capacity(
        config.storage.max_cells.unwrap_or(4096),
    ));

    let gossip_bind = config
        .gossip
        .bind
        .ok_or(ConfigError::MissingGossipBind)
        .context("gossip.bind missing")?;
    let gossip_runtime_config = config
        .gossip
        .clone()
        .into_runtime_config(identity, config.runtime.rng_seed);

    let (admin_tx, admin_rx) = admin::admin_channel();

    let (gossip_rt, gossip_client) = GossipRuntime::bind_with_admin(
        gossip_bind,
        gossip_runtime_config,
        cell_store,
        counts.clone(),
        admin_rx,
    )
    .await
    .context("bind gossip runtime")?;

    let limiter = SharedLimiter::<u32>::new(
        rule_table.clone(),
        gossip_client.clone(),
        counts.clone(),
        config.cardinality_limits(),
    );

    let peer_events = discovery_stream(config.discovery.clone());

    let gossip_task = tokio::task::spawn_local(async move { gossip_rt.run(peer_events).await });

    let envoy_task = config.envoy_bind.map(|bind| {
        let limiter = limiter.clone();
        tokio::task::spawn_local(async move { serve(bind, limiter).await })
    });

    let admin_task = config.admin_bind.map(|bind| {
        let state = AdminState::new(rule_table.clone(), counts.clone(), admin_tx.clone());
        tokio::task::spawn_local(async move { admin::serve(bind, state).await })
    });

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let outcome = tokio::select! {
        result = gossip_task => result
            .context("gossip task panicked")?
            .context("gossip runtime exited with error"),
        result = task_or_pending(envoy_task) => result
            .context("envoy task panicked")?
            .context("envoy server exited with error"),
        result = task_or_pending(admin_task) => result
            .context("admin task panicked")?
            .context("admin server exited with error"),
        _ = &mut shutdown => Ok(()),
    };

    drop(gossip_client);
    outcome
}

fn discovery_stream(
    cfg: gabion::discovery::DiscoveryConfig,
) -> impl futures::Stream<Item = PeerEvent> {
    discovery::from_config(cfg).peer_events().filter_map(|res| async move {
        match res {
            Ok(event) => Some(event),
            Err(error) => {
                tracing::warn!(error = %error, "peer discovery error");
                None
            }
        }
    })
}

/// Resolve to the task's result if it exists, otherwise hang. Lets
/// `tokio::select!` arms be conditionally present without leaking that into
/// the match shape.
async fn task_or_pending<T>(
    task: Option<tokio::task::JoinHandle<T>>,
) -> Result<T, tokio::task::JoinError> {
    match task {
        Some(handle) => handle.await,
        None => std::future::pending().await,
    }
}

fn load_config() -> anyhow::Result<AppConfig> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: gabiond <config.yaml>")?;
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("read config {}", path.display()))?;
    Ok(AppConfig::parse_yaml(&text)?)
}
