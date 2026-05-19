use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::{Pod, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::ListParams;
use kube::runtime::watcher::{Config as WatcherConfig, Event, watcher};
use kube::{Api, Client};

use super::publish_peer_snapshot;
use crate::discovery::{DEFAULT_GOSSIP_PORT_NAME, Peer, PeerHandler};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointSliceDiscoveryConfig {
    pub namespace: String,
    pub service_name: String,
    pub port_name: Option<String>,
    pub self_addr: Option<SocketAddr>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunningServiceDiscoveryError {
    Namespace,
    PodName,
    Pod,
    Services,
    NoSelectingService,
}

pub fn incluster_client() -> Option<Client> {
    kube::Config::incluster_env()
        .or_else(|_| kube::Config::incluster_dns())
        .ok()
        .and_then(|config| Client::try_from(config).ok())
}

pub async fn running_service_endpoint_slice_configs(
    client: Client,
    self_addr: Option<SocketAddr>,
) -> Result<Vec<EndpointSliceDiscoveryConfig>, RunningServiceDiscoveryError> {
    let namespace = fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        .map_err(|_| RunningServiceDiscoveryError::Namespace)?
        .trim()
        .to_string();
    let pod_name = env::var("HOSTNAME").map_err(|_| RunningServiceDiscoveryError::PodName)?;
    let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let services: Api<Service> = Api::namespaced(client, &namespace);
    let pod = pods
        .get(&pod_name)
        .await
        .map_err(|_| RunningServiceDiscoveryError::Pod)?;
    let labels = pod.metadata.labels.unwrap_or_default();
    let service_list = services
        .list(&Default::default())
        .await
        .map_err(|_| RunningServiceDiscoveryError::Services)?;
    let mut configs = Vec::new();

    for service in service_list {
        let Some(spec) = service.spec else {
            continue;
        };
        let Some(selector) = spec.selector else {
            continue;
        };
        if !selector
            .iter()
            .all(|(key, value)| labels.get(key) == Some(value))
        {
            continue;
        }
        let Some(name) = service.metadata.name else {
            continue;
        };
        let port_name = spec.ports.as_ref().and_then(|ports| {
            ports
                .iter()
                .find(|port| port.name.as_deref() == Some(DEFAULT_GOSSIP_PORT_NAME))
                .or_else(|| (ports.len() == 1).then(|| &ports[0]))
                .and_then(|port| port.name.clone())
        });
        configs.push(EndpointSliceDiscoveryConfig {
            namespace: namespace.clone(),
            service_name: name,
            port_name,
            self_addr,
        });
    }

    if configs.is_empty() {
        return Err(RunningServiceDiscoveryError::NoSelectingService);
    }
    configs.sort_by(|left, right| {
        left.namespace
            .cmp(&right.namespace)
            .then_with(|| left.service_name.cmp(&right.service_name))
            .then_with(|| left.port_name.cmp(&right.port_name))
    });
    Ok(configs)
}

#[derive(Clone, Debug, Default)]
pub struct EndpointSlicePeerSet {
    slices: BTreeMap<String, Vec<Peer>>,
    generation: u64,
}

impl EndpointSlicePeerSet {
    pub fn clear(&mut self) {
        self.slices.clear();
        self.generation = self.generation.saturating_add(1);
    }

    pub fn replace(&mut self, next: Self) {
        *self = next;
    }

    pub fn apply(&mut self, slice: &EndpointSlice, config: &EndpointSliceDiscoveryConfig) {
        let name = slice
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| format!("anonymous-{}", self.generation));
        let peers = peers_from_endpoint_slice(slice, config);
        self.slices.insert(name, peers);
        self.generation = self.generation.saturating_add(1);
    }

    pub fn delete(&mut self, slice: &EndpointSlice) {
        if let Some(name) = &slice.metadata.name {
            self.slices.remove(name);
            self.generation = self.generation.saturating_add(1);
        }
    }

    pub fn snapshot_peers(&self) -> Vec<Peer> {
        let mut peers = self.slices.values().flatten().copied().collect::<Vec<_>>();
        peers.sort();
        peers.dedup();
        peers
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

pub async fn run_endpoint_slice_watcher(
    client: Client,
    config: EndpointSliceDiscoveryConfig,
    provider: impl PeerHandler,
) -> Result<(), kube::runtime::watcher::Error> {
    let api: Api<EndpointSlice> = Api::namespaced(client, &config.namespace);
    let labels = format!("kubernetes.io/service-name={}", config.service_name);
    let watcher_config = WatcherConfig::default().labels(&labels);
    let mut events = watcher(api, watcher_config).boxed();
    let mut peer_set = EndpointSlicePeerSet::default();
    let mut init_peer_set = None;

    while let Some(event) = events.try_next().await? {
        match event {
            Event::Apply(slice) => {
                peer_set.apply(&slice, &config);
                publish_peer_snapshot(&provider, &peer_set.snapshot_peers());
            }
            Event::Init => {
                init_peer_set = Some(EndpointSlicePeerSet::default());
            }
            Event::InitApply(slice) => {
                init_peer_set
                    .get_or_insert_with(EndpointSlicePeerSet::default)
                    .apply(&slice, &config);
            }
            Event::InitDone => {
                if let Some(next) = init_peer_set.take() {
                    peer_set.replace(next);
                } else {
                    peer_set.clear();
                }
                publish_peer_snapshot(&provider, &peer_set.snapshot_peers());
            }
            Event::Delete(slice) => {
                peer_set.delete(&slice);
                publish_peer_snapshot(&provider, &peer_set.snapshot_peers());
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
struct MergedEndpointSliceState {
    peers_by_selector: Vec<Vec<Peer>>,
}

impl MergedEndpointSliceState {
    fn new(selector_count: usize) -> Self {
        Self {
            peers_by_selector: vec![Vec::new(); selector_count],
        }
    }

    fn update(&mut self, selector_index: usize, peers: Vec<Peer>) -> Vec<Peer> {
        if let Some(slot) = self.peers_by_selector.get_mut(selector_index) {
            *slot = peers;
        }
        self.snapshot()
    }

    fn snapshot(&self) -> Vec<Peer> {
        let mut peers = self
            .peers_by_selector
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        peers.sort();
        peers.dedup();
        peers
    }
}

pub async fn run_endpoint_slice_watchers(
    client: Client,
    configs: Vec<EndpointSliceDiscoveryConfig>,
    provider: impl PeerHandler + Clone + Send + Sync + 'static,
) {
    let state = Arc::new(Mutex::new(MergedEndpointSliceState::new(configs.len())));

    for (selector_index, config) in configs.into_iter().enumerate() {
        let client = client.clone();
        let provider = provider.clone();
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                let api: Api<EndpointSlice> = Api::namespaced(client.clone(), &config.namespace);
                let labels = format!("kubernetes.io/service-name={}", config.service_name);
                let watcher_config = WatcherConfig::default().labels(&labels);
                let mut events = watcher(api, watcher_config).boxed();
                let mut peer_set = EndpointSlicePeerSet::default();
                let mut init_peer_set = None;

                while let Ok(Some(event)) = events.try_next().await {
                    match event {
                        Event::Apply(slice) => {
                            peer_set.apply(&slice, &config);
                        }
                        Event::Init => {
                            init_peer_set = Some(EndpointSlicePeerSet::default());
                        }
                        Event::InitApply(slice) => {
                            init_peer_set
                                .get_or_insert_with(EndpointSlicePeerSet::default)
                                .apply(&slice, &config);
                        }
                        Event::InitDone => {
                            if let Some(next) = init_peer_set.take() {
                                peer_set.replace(next);
                            } else {
                                peer_set.clear();
                            }
                        }
                        Event::Delete(slice) => {
                            peer_set.delete(&slice);
                        }
                    }

                    if let Ok(mut state) = state.lock() {
                        publish_peer_snapshot(
                            &provider,
                            &state.update(selector_index, peer_set.snapshot_peers()),
                        );
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        });
    }
}

pub async fn initial_endpoint_slice_snapshots(
    client: Client,
    configs: &[EndpointSliceDiscoveryConfig],
) -> Result<Vec<Peer>, kube::Error> {
    let mut peers = Vec::new();
    for config in configs {
        peers.extend(initial_endpoint_slice_snapshot(client.clone(), config).await?);
    }
    peers.sort();
    peers.dedup();
    Ok(peers)
}

pub async fn initial_endpoint_slice_snapshot(
    client: Client,
    config: &EndpointSliceDiscoveryConfig,
) -> Result<Vec<Peer>, kube::Error> {
    let api: Api<EndpointSlice> = Api::namespaced(client, &config.namespace);
    let labels = format!("kubernetes.io/service-name={}", config.service_name);
    let list = api.list(&ListParams::default().labels(&labels)).await?;
    let mut peer_set = EndpointSlicePeerSet::default();
    for slice in &list.items {
        peer_set.apply(slice, config);
    }
    Ok(peer_set.snapshot_peers())
}

pub fn peers_from_endpoint_slice(
    slice: &EndpointSlice,
    config: &EndpointSliceDiscoveryConfig,
) -> Vec<Peer> {
    let Some(port) = select_port(slice, config.port_name.as_deref()) else {
        return Vec::new();
    };

    let mut peers = slice
        .endpoints
        .iter()
        .filter(|endpoint| {
            endpoint
                .conditions
                .as_ref()
                .and_then(|conditions| conditions.ready)
                .unwrap_or(true)
        })
        .flat_map(|endpoint| endpoint.addresses.iter())
        .filter_map(|address| address.parse::<IpAddr>().ok())
        .map(|ip| Peer::new(SocketAddr::new(ip, port)))
        .filter(|peer| Some(peer.addr) != config.self_addr)
        .collect::<Vec<_>>();

    peers.sort();
    peers.dedup();
    peers
}

fn select_port(slice: &EndpointSlice, port_name: Option<&str>) -> Option<u16> {
    let ports = slice.ports.as_ref()?;
    let selected = match port_name {
        Some(name) => ports
            .iter()
            .find(|port| port.name.as_deref() == Some(name))?,
        None => ports.iter().find(|port| port.port.is_some())?,
    };
    selected.port.and_then(|port| u16::try_from(port).ok())
}
