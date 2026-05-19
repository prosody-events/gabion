use std::path::PathBuf;

use serde::Deserialize;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let config = load_config()?;
    let envoy_bind = config.server.envoy_rls.bind;
    let envoy_enabled = config.server.envoy_rls.enabled;
    let admin_bind = config.server.admin.bind;
    let admin_enabled = config.server.admin.enabled;
    let cardinality_limits = config.runtime.cardinality_limits();
    let runtime = gabion::Runtime::new(config.runtime).map_err(|error| error.to_string())?;
    let gossip_enabled = runtime.gossip_enabled();

    let runtime_task = tokio::spawn({
        let runtime = runtime.clone();
        async move {
            runtime
                .run_until_shutdown()
                .await
                .map_err(|error| error.to_string())
        }
    });
    let envoy_task = envoy_enabled.then_some(envoy_bind).flatten().map(|bind| {
        tokio::spawn(gabion_server::serve_with_limits(
            bind,
            runtime.clone(),
            cardinality_limits,
        ))
    });
    let admin_task = admin_enabled
        .then_some(admin_bind)
        .flatten()
        .map(|bind| tokio::spawn(gabion::admin::serve_for_runtime(bind, runtime.clone())));

    match (envoy_task, admin_task, gossip_enabled) {
        (Some(envoy_task), Some(admin_task), _) => {
            tokio::select! {
                result = runtime_task => flatten_task(result)?,
                result = envoy_task => flatten_task(result).map_err(|error| error.to_string())?,
                result = admin_task => flatten_task(result).map_err(|error| error.to_string())?,
                result = tokio::signal::ctrl_c() => result.map_err(|error| error.to_string())?,
            }
        }
        (Some(envoy_task), None, _) => {
            tokio::select! {
                result = runtime_task => flatten_task(result)?,
                result = envoy_task => flatten_task(result).map_err(|error| error.to_string())?,
                result = tokio::signal::ctrl_c() => result.map_err(|error| error.to_string())?,
            }
        }
        (None, Some(admin_task), _) => {
            tokio::select! {
                result = runtime_task => flatten_task(result)?,
                result = admin_task => flatten_task(result).map_err(|error| error.to_string())?,
                result = tokio::signal::ctrl_c() => result.map_err(|error| error.to_string())?,
            }
        }
        (None, None, true) => {
            tokio::select! {
                result = runtime_task => flatten_task(result)?,
                result = tokio::signal::ctrl_c() => result.map_err(|error| error.to_string())?,
            }
        }
        (None, None, false) => return Err("at least one listener or gossip must be enabled".into()),
    }

    runtime.shutdown();
    Ok(())
}

fn load_config() -> Result<AppConfig, String> {
    let config_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| "usage: gabiond <config.yaml>".to_string())?;
    let text = std::fs::read_to_string(config_path).map_err(|error| error.to_string())?;
    let config: FileConfig = serde_yaml::from_str(&text).map_err(|error| error.to_string())?;
    config.into_app_config()
}

#[derive(Debug)]
struct AppConfig {
    runtime: gabion::Config,
    server: FileServerConfig,
}

#[derive(Debug, Deserialize)]
struct FileConfig {
    #[serde(flatten)]
    runtime: gabion::Config,
    #[serde(default)]
    server: FileServerConfig,
}

impl FileConfig {
    fn into_app_config(self) -> Result<AppConfig, String> {
        Ok(AppConfig {
            runtime: self.runtime,
            server: self.server,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
struct FileServerConfig {
    #[serde(default)]
    envoy_rls: FileListenerConfig,
    #[serde(default)]
    admin: FileListenerConfig,
}

#[derive(Debug, Default, Deserialize)]
struct FileListenerConfig {
    #[serde(default)]
    enabled: bool,
    bind: Option<std::net::SocketAddr>,
}

fn flatten_task<T, E: std::fmt::Display>(
    result: Result<Result<T, E>, tokio::task::JoinError>,
) -> Result<T, String> {
    result
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())
}
