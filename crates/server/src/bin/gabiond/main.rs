//! Production binary: ties the gossip runtime, gRPC service, and admin
//! endpoint together. Single-threaded `current_thread` runtime under a
//! `LocalSet` because the gossip runtime is `!Send`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle, LocalSet};
use tracing_subscriber::EnvFilter;

use gabion::aggregate::Aggregate;
use gabion::crdt::{CellStore, NodeIdentity};
use gabion::gossip::{GossipError, GossipRuntime};
use gabion::identity;
use gabion::zone::ZoneName;

use gabion_server::admin::{self, AdminState};
use gabion_server::config::{AppConfig, ConfigError};
use gabion_server::{RATE_LIMIT_SERVICE_NAME, SharedLimiter, serve};

mod exit;
use exit::{Exit, Listener, Reported, Server, Stage, startup};

fn main() -> ExitCode {
    init_tracing();
    let served = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(startup(Stage::Runtime))
        .map_err(Exit::report)
        .and_then(|runtime| LocalSet::new().block_on(&runtime, run()));
    match served {
        Ok(()) => ExitCode::SUCCESS,
        Err(reported) => reported.into(),
    }
}

/// Install a stdout `tracing` subscriber. Verbosity follows `RUST_LOG`
/// (defaults to `info` for gabion crates, `warn` elsewhere).
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,gabion=info,gabion_server=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stdout)
        .with_target(true)
        .init();
}

/// Start gabiond and serve until a signal (`Ok`) or a failure, which `Err`
/// proves was logged.
async fn run() -> Result<(), Reported> {
    // Install signal handlers **before** any other work so a SIGTERM
    // arriving during config loading, gossip-runtime binding, or task
    // spawning gets caught and translated into a clean shutdown instead of
    // killing the process with the default disposition.
    let mut sigterm = signal(SignalKind::terminate()).map_err(startup(Stage::Signals))?;
    let mut sigint = signal(SignalKind::interrupt()).map_err(startup(Stage::Signals))?;

    let config = load_config().map_err(startup(Stage::Config))?;
    let gossip_bind = config
        .gossip
        .membership()
        .bind()
        .ok_or(ConfigError::MissingGossipBind)
        .map_err(startup(Stage::Config))?;
    let rule_table = Arc::new(config.rules);
    tracing::info!(
        code = "gabiond_started",
        gossip_bind = %gossip_bind,
        envoy_bind = ?config.settings.envoy_bind,
        admin_bind = ?config.settings.admin_bind,
        rules_loaded = rule_table.rules().len(),
        max_keys = config.settings.max_cells,
        max_rules = rule_table.dictionary_capacity(),
        max_peers = config.settings.peer_capacity,
        max_peer_instances = config.storage.node_dictionary_capacity(),
        zone_setting = config.settings.zone.as_ref().map(ZoneName::as_str),
        membership = config.gossip.membership().name(),
        "Starting gabion.",
    );

    let identity = NodeIdentity::new(
        identity::node_id(config.settings.node_id_seed.as_deref()),
        identity::incarnation(None),
    );
    tracing::info!(
        code = "identity_generated",
        node_id = %format!("{:032x}", identity.node_id.0),
        incarnation = identity.incarnation,
        "Generated this node's identity. The incarnation number changes on \
         every restart so peers can tell restarts apart from new nodes.",
    );

    let cell_store = CellStore::new(config.storage, identity, &rule_table);
    let counts = Arc::new(Aggregate::new(&config.storage));

    let (admin_tx, admin_rx) = admin::admin_channel();

    config.gossip.note_startup();
    let peer_events = config.gossip.membership().peer_events();
    let (gossip_rt, gossip_client) = GossipRuntime::bind(
        gossip_bind,
        config.gossip,
        cell_store,
        counts.clone(),
        admin_rx,
    )
    .await
    .map_err(|error| Exit::Bind(Listener::Gossip, gossip_bind, error.into()))?;
    let grpc_listener = bind(Server::Grpc, config.settings.envoy_bind).await?;
    let admin_listener = bind(Server::Admin, config.settings.admin_bind).await?;
    tracing::info!(
        code = "gossip_listening",
        listen_addr = %gossip_rt.local_addr().unwrap_or(gossip_bind),
        "Listening for gossip from other gabion nodes.",
    );

    let limiter = SharedLimiter::new(
        rule_table.clone(),
        gossip_client,
        counts.clone(),
        config.storage,
    );

    let gossip_task = tokio::task::spawn_local(gossip_rt.run(peer_events));

    // Shutdown broadcasts to every listener. `false` = keep serving,
    // `true` = drain.
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

    let mut grpc = grpc_listener.map(|(bind, listener)| {
        tracing::info!(
            code = "grpc_listening",
            listen_addr = %bind,
            health_service = "grpc.health.v1.Health",
            reflection_service = "grpc.reflection.v1.ServerReflection",
            rate_limit_service = RATE_LIMIT_SERVICE_NAME,
            "Accepting rate-limit decisions from Envoy. Health, reflection, \
             and the rate-limit service are mounted on this address.",
        );
        let drain = drain_requested(shutdown_rx.clone());
        Serving::spawn(
            Server::Grpc,
            bind,
            serve(listener, limiter.clone(), health_server.clone(), drain),
        )
    });

    let mut admin = admin_listener.map(|(bind, listener)| {
        tracing::info!(
            code = "admin_listening",
            listen_addr = %bind,
            "Admin HTTP endpoint is up. Hit /snapshot for a live view of \
             this node's rate-limit state.",
        );
        let state = AdminState::new(rule_table.clone(), counts.clone(), admin_tx.clone());
        let drain = drain_requested(shutdown_rx.clone());
        Serving::spawn(Server::Admin, bind, admin::serve(listener, state, drain))
    });

    // Serving ends at a signal, or at once when the gossip task or a server
    // stops.
    let served = tokio::select! {
        result = gossip_task => Err(Exit::GossipStopped(
            joined(result).err().unwrap_or(GossipError::RuntimeShutDown),
        )
        .report()),
        exit = stopped(&mut grpc) => Err(exit.report()),
        exit = stopped(&mut admin) => Err(exit.report()),
        _ = sigterm.recv() => {
            tracing::info!(
                code = "shutdown_signal",
                signal = "SIGTERM",
                "Received shutdown signal; draining in-flight requests and \
                 stopping cleanly.",
            );
            Ok(())
        }
        _ = sigint.recv() => {
            tracing::info!(
                code = "shutdown_signal",
                signal = "SIGINT",
                "Received shutdown signal (Ctrl-C); draining in-flight \
                 requests and stopping cleanly.",
            );
            Ok(())
        }
    };

    // Flip health to NOT_SERVING first so readiness probes (which run
    // independently of in-flight traffic) immediately fail. External load
    // balancers will stop sending new requests; each server keeps serving
    // its in-flight requests until they complete.
    health_reporter
        .set_service_status(
            RATE_LIMIT_SERVICE_NAME,
            tonic_health::ServingStatus::NotServing,
        )
        .await;
    tracing::info!(
        code = "not_serving",
        "Marked the rate-limit service as NOT_SERVING in the health protocol. Load balancers \
         should now route traffic elsewhere.",
    );

    // Trigger graceful shutdown of the gRPC and admin servers. Each task
    // observes the flip via `wait_for(|drain| *drain)` and drains in-flight
    // requests before returning.
    shutdown_tx.send_replace(true);
    let mut drained = Ok(());
    for serving in [grpc, admin].into_iter().flatten() {
        if let Err(exit) = serving.drained().await {
            drained = Err(exit.report());
        }
    }
    let status = served.and(drained);
    if status.is_ok() {
        tracing::info!(code = "clean_shutdown", "Gabion shut down cleanly.");
    }
    status
}

/// Bind `server`'s address when it is configured, before anything claims
/// to serve it, so a taken port fails startup. Returns the bound address, so
/// a port-0 bind reports the port the OS chose.
async fn bind(
    server: Server,
    addr: Option<SocketAddr>,
) -> Result<Option<(SocketAddr, TcpListener)>, Exit> {
    let Some(addr) = addr else {
        return Ok(None);
    };
    match TcpListener::bind(addr).await {
        Ok(listener) => Ok(Some((listener.local_addr().unwrap_or(addr), listener))),
        Err(error) => Err(Exit::Bind(server.into(), addr, error.into())),
    }
}

/// A server's task, with what it serves and where.
struct Serving {
    server: Server,
    addr: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Serving {
    fn spawn<E: Into<anyhow::Error>>(
        server: Server,
        addr: SocketAddr,
        serve: impl Future<Output = Result<(), E>> + 'static,
    ) -> Self {
        let task = tokio::task::spawn_local(async move { serve.await.map_err(Into::into) });
        Self { server, addr, task }
    }

    /// Wait for a server told to drain to finish.
    async fn drained(self) -> Result<(), Exit> {
        joined(self.task.await).map_err(|error| Exit::Stopped(self.server, self.addr, error))
    }
}

/// Resolves when `serving`'s server stops, which before the drain is always a
/// failure; never, when that server is not configured.
async fn stopped(serving: &mut Option<Serving>) -> Exit {
    let Some(running) = serving else {
        return std::future::pending().await;
    };
    let error = joined((&mut running.task).await)
        .err()
        .unwrap_or_else(|| anyhow::anyhow!("it stopped before gabiond told it to drain"));
    let exit = Exit::Stopped(running.server, running.addr, error);
    *serving = None;
    exit
}

/// Resolves once `run` tells the listeners to drain.
async fn drain_requested(mut drain: watch::Receiver<bool>) {
    // `run` holds the sender until every listener has drained, so a dropped
    // sender, the only error, cannot come first.
    let _ = drain.wait_for(|requested| *requested).await;
}

/// A task's output. A panic in the task is re-raised here, so gabiond exits
/// 101 for a bug as it would on the main thread; no task is ever aborted, so
/// every join error is a panic.
fn joined<T>(result: Result<T, JoinError>) -> T {
    result.unwrap_or_else(|error| std::panic::resume_unwind(error.into_panic()))
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
