//! Why gabiond exits with status 1, said once: a [`Reported`] exists only
//! once [`Exit::report`] has logged the reason, and status 1 needs one.

use std::net::SocketAddr;
use std::process::ExitCode;

use gabion::gossip::GossipError;
use gabion::settings::Setting;

/// Why gabiond exits with status 1; [`Exit::report`] logs it in one line.
#[must_use = "an exit says why only when reported"]
pub enum Exit {
    /// A startup step failed (`gabiond_exit`).
    Startup(Stage, anyhow::Error),
    /// A listener could not bind its address (`listener_failed`).
    Bind(Listener, SocketAddr, anyhow::Error),
    /// A server stopped after binding its address (`server_stopped`).
    Stopped(Server, SocketAddr, anyhow::Error),
    /// The gossip task stopped (`gossip_stopped`).
    GossipStopped(GossipError),
}

/// Proof that an exit's reason was logged. Only [`Exit::report`] makes one,
/// and only a `Reported` becomes status 1.
pub struct Reported(());

/// `?` on an [`Exit`] reports it.
impl From<Exit> for Reported {
    fn from(exit: Exit) -> Self {
        exit.report()
    }
}

impl From<Reported> for ExitCode {
    fn from(_: Reported) -> Self {
        Self::FAILURE
    }
}

/// The exit for a failed startup step.
pub fn startup<E: Into<anyhow::Error>>(stage: Stage) -> impl FnOnce(E) -> Exit {
    move |error| Exit::Startup(stage, error.into())
}

impl Exit {
    /// Log why gabiond is exiting, once.
    pub fn report(self) -> Reported {
        match self {
            Self::Startup(stage, error) => tracing::error!(
                code = "gabiond_exit",
                stage = stage.as_str(),
                error = %format_args!("{error:#}"),
                "gabiond is exiting with status 1 because it could not {}. The cause chain is in \
                 the `error` field; the first cause is usually the one to fix. Fix it and restart.",
                stage.as_str(),
            ),
            Self::Bind(listener, addr, error) => {
                let setting = listener.setting();
                tracing::error!(
                    code = "listener_failed",
                    %addr,
                    error = %format_args!("{error:#}"),
                    config_key = setting.yaml_key(),
                    "gabiond could not serve {} on {addr}; the `error` field says why. Usually \
                     another process already uses the port. Change {setting} or stop the other \
                     process. gabiond is exiting with status 1.",
                    listener.name(),
                );
            }
            Self::Stopped(server, addr, error) => tracing::error!(
                code = "server_stopped",
                %addr,
                error = %format_args!("{error:#}"),
                "gabiond stopped serving {} on {addr} after binding it, so this is not a port \
                 clash; the `error` field says why. A server that fails once bound is a gabion \
                 bug: report it with this line. gabiond is exiting with status 1 to be restarted.",
                Listener::from(server).name(),
            ),
            Self::GossipStopped(error) => tracing::error!(
                code = "gossip_stopped",
                %error,
                "The gossip background task stopped on its own. This node can no longer share \
                 counts, so gabiond is exiting with status 1 to be restarted rather than \
                 under-count silently. The `error` field says why.",
            ),
        }
        Reported(())
    }
}

/// A startup step, as `gabiond_exit` names it.
#[derive(Clone, Copy)]
pub enum Stage {
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
pub enum Listener {
    Server(Server),
    Gossip,
}

/// A TCP server gabiond spawns; the gossip socket is not one.
#[derive(Clone, Copy)]
pub enum Server {
    Grpc,
    Admin,
}

impl From<Server> for Listener {
    fn from(server: Server) -> Self {
        Self::Server(server)
    }
}

impl Listener {
    fn name(self) -> &'static str {
        match self {
            Self::Server(Server::Grpc) => "Envoy's rate-limit gRPC calls",
            Self::Server(Server::Admin) => "the admin HTTP endpoint",
            Self::Gossip => "gossip from other gabion nodes",
        }
    }

    fn setting(self) -> Setting {
        match self {
            Self::Server(Server::Grpc) => Setting::EnvoyBind,
            Self::Server(Server::Admin) => Setting::AdminBind,
            Self::Gossip => Setting::GossipBind,
        }
    }
}
