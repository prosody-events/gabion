use std::path::PathBuf;
use std::sync::Arc;

use gabion_bin::{
    ConfigError, DiscoveryKind, LocalOnlyConfig, RuntimePeerHandler, admin,
    endpoint_slice_configs_from_discovery, gossip_runtime, peer_provider_from_config,
    shared_limiter,
};
use thiserror::Error;

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let config_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or(MainError::MissingConfigPath)?;
    let config = LocalOnlyConfig::from_yaml_str(&std::fs::read_to_string(config_path)?)?;
    let envoy_bind = config.server.envoy_rls.bind;
    let envoy_enabled = config.server.envoy_rls.enabled;
    let admin_bind = config.server.admin.bind;
    let admin_enabled = config.server.admin.enabled;
    let cardinality_limits = config.cardinality_limits();
    let gossip_enabled = config.gossip.enabled;
    let gossip_bind = config.gossip.bind;
    let gossip_config = config.gossip.clone();
    let gossip_admin = gossip_enabled.then(|| {
        std::sync::Arc::new(std::sync::Mutex::new(
            gossip_runtime::GossipAdminSnapshot::default(),
        ))
    });
    let peer_provider = if gossip_enabled {
        if gossip_bind.is_none() {
            return Err(MainError::Config(ConfigError::MissingGossipBind));
        }
        let provider = runtime_peer_provider(&config.discovery).await?;
        Some(provider)
    } else {
        None
    };
    let remote_cell_capacity = config.storage.max_cells.unwrap_or_else(|| {
        config
            .storage
            .max_keys
            .saturating_mul(config.storage.max_active_buckets.max(1))
            .max(1)
    });
    let limiter = shared_limiter(config.into_engine()?);
    let gossip_task = peer_provider.map(|peer_provider| {
        tokio::spawn(gossip_runtime::run_udp_runtime_with_admin(
            Arc::clone(&limiter),
            peer_provider,
            gossip_config,
            remote_cell_capacity,
            gossip_admin.clone(),
        ))
    });

    match (
        envoy_enabled,
        envoy_bind,
        admin_enabled,
        admin_bind,
        gossip_task,
    ) {
        (true, Some(envoy_bind), true, Some(admin_bind), Some(gossip_task)) => {
            tokio::select! {
                result = gabion_envoy::serve_with_limits(envoy_bind, Arc::clone(&limiter), cardinality_limits) => {
                    result?;
                }
                result = admin::serve_with_gossip(admin_bind, Arc::clone(&limiter), gossip_admin.clone()) => {
                    result?;
                }
                result = gossip_task => {
                    result??;
                }
            }
        }
        (true, Some(envoy_bind), true, Some(admin_bind), None) => {
            tokio::select! {
                result = gabion_envoy::serve_with_limits(envoy_bind, Arc::clone(&limiter), cardinality_limits) => {
                    result?;
                }
                result = admin::serve_with_gossip(admin_bind, limiter, gossip_admin.clone()) => {
                    result?;
                }
            }
        }
        (true, Some(envoy_bind), _, _, Some(gossip_task)) => {
            tokio::select! {
                result = gabion_envoy::serve_with_limits(envoy_bind, limiter, cardinality_limits) => {
                    result?;
                }
                result = gossip_task => {
                    result??;
                }
            }
        }
        (true, Some(envoy_bind), .., None) => {
            gabion_envoy::serve_with_limits(envoy_bind, limiter, cardinality_limits).await?;
        }
        (_, _, true, Some(admin_bind), Some(gossip_task)) => {
            tokio::select! {
                result = admin::serve_with_gossip(admin_bind, limiter, gossip_admin.clone()) => {
                    result?;
                }
                result = gossip_task => {
                    result??;
                }
            }
        }
        (_, _, true, Some(admin_bind), None) => {
            admin::serve_with_gossip(admin_bind, limiter, gossip_admin).await?;
        }
        (_, _, _, _, Some(gossip_task)) => {
            gossip_task.await??;
        }
        _ => return Err(MainError::NoEnabledListener),
    }

    Ok(())
}

async fn runtime_peer_provider(
    discovery: &gabion_bin::DiscoveryConfig,
) -> Result<RuntimePeerHandler, MainError> {
    if discovery.kind == DiscoveryKind::Auto {
        return match gabion_discovery::kubernetes::incluster_client() {
            Some(client) => {
                let provider = peer_provider_from_config(&gabion_bin::DiscoveryConfig {
                    kind: DiscoveryKind::KubernetesEndpointSlice,
                    ..discovery.clone()
                })?;
                let kube_configs =
                    endpoint_slice_configs_for_runtime(client.clone(), discovery).await?;
                start_kubernetes_discovery_with_client(provider.clone(), client, kube_configs)
                    .await?;
                Ok(provider)
            }
            None => peer_provider_from_config(&gabion_bin::DiscoveryConfig {
                kind: DiscoveryKind::None,
                ..discovery.clone()
            })
            .map_err(MainError::from),
        };
    }

    let provider = peer_provider_from_config(discovery)?;
    if let Some(file_handler) = provider.file_handler() {
        tokio::spawn(gabion_discovery::run_file_peer_events(
            file_handler,
            discovery.recent_peer_grace_millis.clamp(100, 1_000),
        ));
    }
    if discovery.kind == DiscoveryKind::KubernetesEndpointSlice {
        let client = kube::Client::try_default().await;
        let kube_configs = match &client {
            Ok(client) => endpoint_slice_configs_for_runtime(client.clone(), discovery).await?,
            Err(_) => endpoint_slice_configs_from_discovery(discovery)?,
        };
        start_kubernetes_discovery(provider.clone(), kube_configs).await?;
    }
    Ok(provider)
}

async fn endpoint_slice_configs_for_runtime(
    client: kube::Client,
    discovery: &gabion_bin::DiscoveryConfig,
) -> Result<Vec<gabion_discovery::kubernetes::EndpointSliceDiscoveryConfig>, MainError> {
    if !discovery.endpoint_slices.is_empty()
        || discovery.namespace.is_some()
        || discovery.service_name.is_some()
    {
        return endpoint_slice_configs_from_discovery(discovery).map_err(MainError::from);
    }

    infer_running_service_endpoint_slices(client, discovery.self_addr).await
}

async fn infer_running_service_endpoint_slices(
    client: kube::Client,
    self_addr: Option<std::net::SocketAddr>,
) -> Result<Vec<gabion_discovery::kubernetes::EndpointSliceDiscoveryConfig>, MainError> {
    gabion_discovery::kubernetes::running_service_endpoint_slice_configs(client, self_addr)
        .await
        .map_err(|_| ConfigError::MissingKubernetesEndpointSliceSelector.into())
}

async fn start_kubernetes_discovery(
    provider: RuntimePeerHandler,
    configs: Vec<gabion_discovery::kubernetes::EndpointSliceDiscoveryConfig>,
) -> Result<(), MainError> {
    let Ok(client) = kube::Client::try_default().await else {
        return Ok(());
    };
    start_kubernetes_discovery_with_client(provider, client, configs).await
}

async fn start_kubernetes_discovery_with_client(
    provider: RuntimePeerHandler,
    client: kube::Client,
    configs: Vec<gabion_discovery::kubernetes::EndpointSliceDiscoveryConfig>,
) -> Result<(), MainError> {
    let RuntimePeerHandler::Snapshot(snapshot_provider) = provider else {
        return Ok(());
    };

    if let Ok(peers) =
        gabion_discovery::kubernetes::initial_endpoint_slice_snapshots(client.clone(), &configs)
            .await
    {
        for peer in peers {
            snapshot_provider.peer_added(peer);
        }
    }

    tokio::spawn(gabion_discovery::kubernetes::run_endpoint_slice_watchers(
        client,
        configs,
        snapshot_provider,
    ));

    Ok(())
}

#[derive(Debug, Error)]
enum MainError {
    #[error("usage: gabiond <config.yaml>")]
    MissingConfigPath,
    #[error("at least one enabled listener with a bind address is required")]
    NoEnabledListener,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    ConfigParse(#[from] serde_yaml::Error),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Gossip(#[from] gossip_runtime::GossipRuntimeError),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Envoy(#[from] tonic::transport::Error),
}
