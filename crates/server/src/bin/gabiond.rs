//! Production binary: ties the gossip runtime, gRPC service, and admin
//! endpoint together. Single-threaded `current_thread` runtime under a
//! `LocalSet` because the gossip runtime is `!Send`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tokio::task::{JoinError, JoinHandle, LocalSet};
use tracing_subscriber::EnvFilter;

use gabion::aggregate::Aggregate;
use gabion::crdt::{CellStore, NodeIdentity};
use gabion::gossip::{GossipError, GossipRuntime};
use gabion::identity;
use gabion::settings::Setting;
use gabion::zone::ZoneName;

use gabion_server::admin::{self, AdminState};
use gabion_server::config::{AppConfig, ConfigError};
use gabion_server::{RATE_LIMIT_SERVICE_NAME, SharedLimiter, serve};

fn main() -> ExitCode {
    init_tracing();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(startup(Stage::Runtime))
        .and_then(|runtime| LocalSet::new().block_on(&runtime, run()))
        .unwrap_or_else(Exit::report)
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

/// Start gabiond and serve until a signal or a failure. `Err` is a failure
/// to start; `Ok` is the status to exit with, any failure already logged.
async fn run() -> Result<ExitCode, Exit> {
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
    .map_err(|error| Exit::Listener(Listener::Gossip, gossip_bind, error.into()))?;
    tracing::info!(
        listen_addr = %gossip_bind,
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

    let mut grpc = config.settings.envoy_bind.map(|bind| {
        tracing::info!(
            listen_addr = %bind,
            health_service = "grpc.health.v1.Health",
            reflection_service = "grpc.reflection.v1.ServerReflection",
            rate_limit_service = RATE_LIMIT_SERVICE_NAME,
            "Accepting rate-limit decisions from Envoy. Health, reflection, \
             and the rate-limit service are mounted on this address.",
        );
        let drain = drain_requested(shutdown_rx.clone());
        Serving::spawn(
            Listener::Grpc,
            bind,
            serve(bind, limiter.clone(), health_server.clone(), drain),
        )
    });

    let mut admin = config.settings.admin_bind.map(|bind| {
        tracing::info!(
            listen_addr = %bind,
            "Admin HTTP endpoint is up. Hit /snapshot for a live view of \
             this node's rate-limit state.",
        );
        let state = AdminState::new(rule_table.clone(), counts.clone(), admin_tx.clone());
        let drain = drain_requested(shutdown_rx.clone());
        Serving::spawn(Listener::Admin, bind, admin::serve(bind, state, drain))
    });

    // Serving ends at a signal, or at once when the gossip task or a
    // listener stops: a listener that cannot bind stops at its first poll.
    let mut status = tokio::select! {
        result = gossip_task => Exit::GossipStopped(
            joined(result).err().unwrap_or(GossipError::RuntimeShutDown),
        )
        .report(),
        exit = stopped(&mut grpc) => exit.report(),
        exit = stopped(&mut admin) => exit.report(),
        _ = sigterm.recv() => {
            tracing::info!(
                signal = "SIGTERM",
                "Received shutdown signal; draining in-flight requests and \
                 stopping cleanly.",
            );
            ExitCode::SUCCESS
        }
        _ = sigint.recv() => {
            tracing::info!(
                signal = "SIGINT",
                "Received shutdown signal (Ctrl-C); draining in-flight \
                 requests and stopping cleanly.",
            );
            ExitCode::SUCCESS
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
    // observes the flip via `wait_for(|drain| *drain)` and drains in-flight
    // requests before returning.
    shutdown_tx.send_replace(true);
    for serving in [grpc, admin].into_iter().flatten() {
        if let Err(exit) = serving.drained().await {
            status = exit.report();
        }
    }
    if status == ExitCode::SUCCESS {
        tracing::info!("Gabion shut down cleanly.");
    }
    Ok(status)
}

/// Why gabiond exits with status 1; [`Exit::report`] logs it in one line.
enum Exit {
    /// A startup step failed (`gabiond_exit`).
    Startup(Stage, anyhow::Error),
    /// A listener could not serve, or stopped (`listener_failed`).
    Listener(Listener, SocketAddr, anyhow::Error),
    /// The gossip task stopped (`gossip_stopped`).
    GossipStopped(GossipError),
}

/// The exit for a failed startup step.
fn startup<E: Into<anyhow::Error>>(stage: Stage) -> impl FnOnce(E) -> Exit {
    move |error| Exit::Startup(stage, error.into())
}

impl Exit {
    /// Log why gabiond is exiting, once, and return status 1.
    fn report(self) -> ExitCode {
        match self {
            Self::Startup(stage, error) => tracing::error!(
                code = "gabiond_exit",
                stage = stage.as_str(),
                error = %format_args!("{error:#}"),
                "gabiond is exiting with status 1 because it could not {}. The cause chain is in \
                 the `error` field; the first cause is usually the one to fix. Fix it and restart.",
                stage.as_str(),
            ),
            Self::Listener(listener, addr, error) => {
                let setting = listener.setting();
                tracing::error!(
                    code = "listener_failed",
                    %addr,
                    error = %format_args!("{error:#}"),
                    config_key = setting.yaml_key(),
                    "gabiond could not serve {} on {addr}: {error:#}. Usually another process \
                     already uses the port. Change {setting} or stop the other process. gabiond \
                     is exiting with status 1.",
                    listener.name(),
                );
            }
            Self::GossipStopped(error) => tracing::error!(
                code = "gossip_stopped",
                %error,
                "The gossip background task stopped on its own. This node can no longer share \
                 counts, so gabiond is exiting with status 1 to be restarted rather than \
                 under-count silently. The `error` field says why.",
            ),
        }
        ExitCode::FAILURE
    }
}

/// A startup step, as `gabiond_exit` names it.
#[derive(Clone, Copy)]
enum Stage {
    Runtime,
    Signals,
    Config,
}

impl Stage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "start its async runtime",
            Self::Signals => "install its SIGTERM and SIGINT handlers",
            Self::Config => "load its configuration",
        }
    }
}

/// An address gabiond listens on, as `listener_failed` names it.
#[derive(Clone, Copy)]
enum Listener {
    Grpc,
    Admin,
    Gossip,
}

impl Listener {
    fn name(self) -> &'static str {
        match self {
            Self::Grpc => "Envoy's rate-limit gRPC calls",
            Self::Admin => "the admin HTTP endpoint",
            Self::Gossip => "gossip from other gabion nodes",
        }
    }

    fn setting(self) -> Setting {
        match self {
            Self::Grpc => Setting::EnvoyBind,
            Self::Admin => Setting::AdminBind,
            Self::Gossip => Setting::GossipBind,
        }
    }
}

/// A listener's task, with what it serves and where.
struct Serving {
    listener: Listener,
    addr: SocketAddr,
    task: JoinHandle<anyhow::Result<()>>,
}

impl Serving {
    fn spawn<E: Into<anyhow::Error>>(
        listener: Listener,
        addr: SocketAddr,
        serve: impl Future<Output = Result<(), E>> + 'static,
    ) -> Self {
        let task = tokio::task::spawn_local(async move { serve.await.map_err(Into::into) });
        Self {
            listener,
            addr,
            task,
        }
    }

    /// Wait for a listener told to drain to finish.
    async fn drained(self) -> Result<(), Exit> {
        joined(self.task.await).map_err(|error| Exit::Listener(self.listener, self.addr, error))
    }
}

/// Resolves when `serving`'s listener stops, which before the drain is always
/// a failure; never, when that listener is not configured.
async fn stopped(serving: &mut Option<Serving>) -> Exit {
    let Some(running) = serving else {
        return std::future::pending().await;
    };
    let error = joined((&mut running.task).await)
        .err()
        .unwrap_or_else(|| anyhow::anyhow!("it stopped before gabiond told it to drain"));
    let exit = Exit::Listener(running.listener, running.addr, error);
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
