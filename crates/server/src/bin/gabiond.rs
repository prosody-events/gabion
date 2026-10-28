//! Production binary: ties the gossip runtime, gRPC service, and admin
//! endpoint together. Single-threaded `current_thread` runtime under a
//! `LocalSet` because the gossip runtime is `!Send`.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tokio::task::LocalSet;
use tracing_subscriber::{EnvFilter, fmt};

use gabion::aggregate::Aggregate;
use gabion::crdt::CellStore;
use gabion::discovery;
use gabion::gossip::GossipRuntime;

use gabion_server::admin::{self, AdminState};
use gabion_server::config::{AppConfig, ConfigError};
use gabion_server::identity::derive_identity;
use gabion_server::{RATE_LIMIT_SERVICE_NAME, SharedLimiter, serve};

fn main() -> anyhow::Result<()> {
    init_tracing();
    let result = (|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build tokio runtime")?;
        let local = LocalSet::new();
        local.block_on(&runtime, run())
    })();
    if let Err(ref err) = result {
        tracing::error!(
            error = ?err,
            "Gabion stopped because of an unrecoverable error. Details \
             above (with the chain of causes) explain what went wrong.",
        );
    }
    result
}

/// Install a stdout `tracing` subscriber. Verbosity follows `RUST_LOG`
/// (defaults to `info` for gabion crates, `warn` elsewhere).
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,gabion=info,gabion_server=info"));
    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stdout)
        .with_target(true)
        .init();
}

async fn run() -> anyhow::Result<()> {
    // Install signal handlers **before** any other work so a SIGTERM
    // arriving during config loading, gossip-runtime binding, or task
    // spawning gets caught and translated into a clean shutdown instead of
    // killing the process with the default disposition.
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;

    let config = load_config()?;
    let rule_table = Arc::new(config.rules);
    tracing::info!(
        gossip_bind = ?config.settings.gossip_bind,
        envoy_bind = ?config.settings.envoy_bind,
        admin_bind = ?config.settings.admin_bind,
        rules_loaded = rule_table.rules().len(),
        max_keys = config.settings.max_cells,
        max_rules = rule_table.dictionary_capacity(),
        max_peers = config.settings.peer_capacity,
        max_peer_instances = config.storage.node_dictionary_capacity(),
        "Starting gabion.",
    );

    let identity = derive_identity(config.settings.node_id_seed.as_deref());
    tracing::info!(
        node_id = %format!("{:032x}", identity.node_id.0),
        incarnation = identity.incarnation,
        "Generated this node's identity. The incarnation number changes on \
         every restart so peers can tell restarts apart from new nodes.",
    );

    let cell_store = CellStore::new(config.storage, identity, &rule_table);
    let counts = Arc::new(Aggregate::new(&config.storage));

    let gossip_bind = config
        .settings
        .gossip_bind
        .ok_or(ConfigError::MissingGossipBind)?;
    let mut gossip_runtime_config = config.gossip;
    if config.settings.rng_seed.is_none() {
        gossip_runtime_config.rng_seed =
            gabion::defaults::random_rng_seed().context("draw gossip RNG seed")?;
    }

    let (admin_tx, admin_rx) = admin::admin_channel();

    gossip_runtime_config.note_default_cluster_id();
    let (gossip_rt, gossip_client) = GossipRuntime::bind_with_admin(
        gossip_bind,
        gossip_runtime_config,
        cell_store,
        counts.clone(),
        admin_rx,
    )
    .await
    .context("bind gossip runtime")?;
    tracing::info!(
        listen_addr = %gossip_bind,
        "Listening for gossip from other gabion nodes.",
    );

    let limiter = SharedLimiter::new(
        rule_table.clone(),
        gossip_client.clone(),
        counts.clone(),
        config.storage,
    );

    let peer_events = discovery::peer_events(config.settings.discovery());

    let gossip_task = tokio::task::spawn_local(async move { gossip_rt.run(peer_events).await });

    // Shutdown broadcasts to every long-running task. `false` = keep serving,
    // `true` = drain. `watch` is the right primitive here because every
    // subscriber sees the latest value regardless of when it joins, and
    // `wait_for` lets each subscriber suspend until the value flips to
    // `true`.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Health protocol: report SERVING for the rate-limit service so external
    // readiness probes (kube-proxy gRPC checks, `grpc_health_probe`,
    // grpc-go load balancers) treat this pod as ready. We flip the status to
    // NOT_SERVING the moment a shutdown signal arrives so endpoints get
    // removed from upstreams **before** tonic starts refusing connections.
    let (health_reporter, health_server) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status(
            RATE_LIMIT_SERVICE_NAME,
            tonic_health::ServingStatus::Serving,
        )
        .await;

    let envoy_task = config.settings.envoy_bind.map(|bind| {
        tracing::info!(
            listen_addr = %bind,
            health_service = "grpc.health.v1.Health",
            reflection_service = "grpc.reflection.v1.ServerReflection",
            rate_limit_service = RATE_LIMIT_SERVICE_NAME,
            "Accepting rate-limit decisions from Envoy. Health, reflection, \
             and the rate-limit service are mounted on this address.",
        );
        let limiter = limiter.clone();
        let health_server = health_server.clone();
        let mut shutdown_rx = shutdown_rx.clone();
        tokio::task::spawn_local(async move {
            let shutdown = async move {
                if shutdown_rx.wait_for(|drain| *drain).await.is_err() {
                    tracing::debug!(
                        "Shutdown channel closed before the gRPC server observed a drain signal.",
                    );
                }
            };
            serve(bind, limiter, health_server, shutdown).await
        })
    });

    let admin_task = config.settings.admin_bind.map(|bind| {
        tracing::info!(
            listen_addr = %bind,
            "Admin HTTP endpoint is up. Hit /snapshot for a live view of \
             this node's rate-limit state.",
        );
        let state = AdminState::new(rule_table.clone(), counts.clone(), admin_tx.clone());
        let mut shutdown_rx = shutdown_rx.clone();
        tokio::task::spawn_local(async move {
            let shutdown = async move {
                if shutdown_rx.wait_for(|drain| *drain).await.is_err() {
                    tracing::debug!(
                        "Shutdown channel closed before the admin server observed a drain signal.",
                    );
                }
            };
            admin::serve_with_shutdown(bind, state, shutdown).await
        })
    });

    // Wait for whichever happens first: a SIGTERM/SIGINT (clean shutdown)
    // or the gossip runtime exiting unexpectedly (already a failure; we
    // still want to drain the gRPC/admin servers before exiting).
    let early_exit: Result<(), anyhow::Error> = tokio::select! {
        result = gossip_task => Err(result
            .context("gossip task panicked")
            .and_then(|inner| inner.context("gossip runtime exited with error"))
            .err()
            .unwrap_or_else(|| anyhow::anyhow!("gossip runtime exited unexpectedly"))),
        _ = sigterm.recv() => {
            tracing::info!(
                signal = "SIGTERM",
                "Received shutdown signal; draining in-flight requests and \
                 stopping cleanly.",
            );
            Ok(())
        }
        _ = sigint.recv() => {
            tracing::info!(
                signal = "SIGINT",
                "Received shutdown signal (Ctrl-C); draining in-flight \
                 requests and stopping cleanly.",
            );
            Ok(())
        }
    };

    // Flip health to NOT_SERVING first so readiness probes (which run
    // independently of in-flight traffic) immediately fail. External load
    // balancers will stop sending new requests; in-flight requests continue
    // to be served by `serve_with_shutdown` until they complete.
    health_reporter
        .set_service_status(
            RATE_LIMIT_SERVICE_NAME,
            tonic_health::ServingStatus::NotServing,
        )
        .await;
    tracing::info!(
        "Marked the rate-limit service as NOT_SERVING in the health protocol. Load balancers \
         should now route traffic elsewhere.",
    );

    // Trigger graceful shutdown of the gRPC and admin servers. Each task
    // observes the flip via `wait_for(|drain| *drain)` and tonic's
    // `serve_with_shutdown` drains in-flight requests before returning.
    if shutdown_tx.send(true).is_err() {
        tracing::warn!(
            "Shutdown signal had no remaining subscribers; gRPC/admin tasks may have already \
             exited.",
        );
    }

    // Drain the gRPC server. tonic finishes any in-flight `should_rate_limit`
    // calls before this future resolves.
    if let Some(task) = envoy_task {
        match task.await {
            Ok(Ok(())) => {
                tracing::info!("Rate-limit gRPC server drained and stopped.");
            }
            Ok(Err(err)) => {
                tracing::warn!(
                    error = %err,
                    "Rate-limit gRPC server stopped with an error during drain.",
                );
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "Rate-limit gRPC task panicked during shutdown.",
                );
            }
        }
    }

    if let Some(task) = admin_task {
        match task.await {
            Ok(Ok(())) => tracing::info!("Admin HTTP server stopped."),
            Ok(Err(err)) => tracing::warn!(
                error = %err,
                "Admin HTTP server stopped with an error.",
            ),
            Err(err) => tracing::error!(
                error = %err,
                "Admin HTTP task panicked during shutdown.",
            ),
        }
    }

    // Drop the gossip client so the gossip runtime's request channel closes
    // and `gossip_rt.run()` returns. The gossip task was already consumed by
    // the select above (if it fired the gossip arm); if the signal arm
    // fired, gossip is still running and this drop ends it.
    drop(gossip_client);
    tracing::info!("Gabion shut down cleanly.");

    early_exit
}

fn load_config() -> anyhow::Result<AppConfig> {
    // Layer order: built-in defaults → YAML file (if provided) → `GABION_*`
    // env vars. Operators can run with no config file at all and configure
    // the server entirely through env vars; the file path argument is
    // optional. Passing `--help` (or any single flag-looking argument) is
    // not supported — config-rs handles structural errors directly.
    let path = std::env::args_os().nth(1).map(PathBuf::from);
    if let Some(ref p) = path
        && !p.exists()
    {
        return Err(anyhow::anyhow!(
            "Config file not found: {}\nEither pass a real path or omit the argument entirely \
             (env vars and defaults will be used).",
            p.display(),
        ));
    }
    Ok(AppConfig::load(path.as_deref())?)
}
