use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};

use async_stream::try_stream;
use futures::{Stream, StreamExt};
use k8s_openapi::api::core::v1::{Service, ServicePort};
use k8s_openapi::api::discovery::v1::{EndpointPort, EndpointSlice};
use kube::api::ListParams;
use kube::runtime::watcher::{Config as WatcherConfig, Error as WatcherError, Event, watcher};
use kube::{Api, Client};

use crate::discovery::{
    DEFAULT_GABION_SERVICE_NAME, DiscoveryConfig, Peer, PeerDiscovery, PeerEvent,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointSliceDiscoveryConfig {
    pub namespace: String,
    pub service_name: String,
    pub self_addr: Option<SocketAddr>,
}

#[derive(Clone)]
pub struct EndpointSliceDiscovery {
    client: Client,
    config: EndpointSliceDiscoveryConfig,
}

impl EndpointSliceDiscovery {
    pub fn new(client: Client, config: EndpointSliceDiscoveryConfig) -> Self {
        Self { client, config }
    }
}

impl PeerDiscovery for EndpointSliceDiscovery {
    type Error = WatcherError;

    fn peer_events(self) -> impl Stream<Item = Result<PeerEvent, Self::Error>> + Send {
        endpoint_slice_peer_events(self.client, self.config)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceDiscoveryError {
    Services,
    NoGabionServices,
}

pub async fn endpoint_slice_configs_from_services(
    client: Client,
    discovery: &DiscoveryConfig,
) -> Result<Vec<EndpointSliceDiscoveryConfig>, ServiceDiscoveryError> {
    let service_names = whitelist(&discovery.service_whitelist);
    let namespace_names = whitelist(&discovery.namespace_whitelist);
    let mut configs = Vec::new();

    if namespace_names.is_empty() {
        let services: Api<Service> = Api::all(client);
        let service_list = services
            .list(&ListParams::default())
            .await
            .map_err(|_| ServiceDiscoveryError::Services)?;
        configs.extend(service_list.into_iter().filter_map(|service| {
            endpoint_slice_config_from_service(service, &service_names, discovery.self_addr)
        }));
    } else {
        for namespace in namespace_names {
            let services: Api<Service> = Api::namespaced(client.clone(), namespace);
            let service_list = services
                .list(&ListParams::default())
                .await
                .map_err(|_| ServiceDiscoveryError::Services)?;
            configs.extend(service_list.into_iter().filter_map(|service| {
                endpoint_slice_config_from_service(service, &service_names, discovery.self_addr)
            }));
        }
    }

    if configs.is_empty() {
        return Err(ServiceDiscoveryError::NoGabionServices);
    }
    configs.sort_by(|left, right| {
        left.namespace
            .cmp(&right.namespace)
            .then_with(|| left.service_name.cmp(&right.service_name))
    });
    Ok(configs)
}

fn whitelist(values: &[String]) -> BTreeSet<&str> {
    values
        .iter()
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .collect()
}

fn endpoint_slice_config_from_service(
    service: Service,
    service_names: &BTreeSet<&str>,
    self_addr: Option<SocketAddr>,
) -> Option<EndpointSliceDiscoveryConfig> {
    let namespace = service.metadata.namespace?;
    let service_name = service.metadata.name?;
    if !service_names.is_empty() && !service_names.contains(service_name.as_str()) {
        return None;
    }
    let spec = service.spec?;
    if !service_exposes_gabion_udp(&spec.ports.unwrap_or_default()) {
        return None;
    }
    Some(EndpointSliceDiscoveryConfig {
        namespace,
        service_name,
        self_addr,
    })
}

fn service_exposes_gabion_udp(ports: &[ServicePort]) -> bool {
    ports.iter().any(is_gabion_udp_service_port)
}

fn is_gabion_udp_service_port(port: &ServicePort) -> bool {
    port.name.as_deref() == Some(DEFAULT_GABION_SERVICE_NAME)
        && port.protocol.as_deref().unwrap_or("TCP") == "UDP"
}

pub fn endpoint_slice_peer_events(
    client: Client,
    config: EndpointSliceDiscoveryConfig,
) -> impl Stream<Item = Result<PeerEvent, WatcherError>> + Send {
    let api: Api<EndpointSlice> = Api::namespaced(client, &config.namespace);
    let labels = format!("kubernetes.io/service-name={}", config.service_name);

    try_stream! {
        let events = watcher(api, WatcherConfig::default().labels(&labels));
        futures::pin_mut!(events);
        while let Some(event) = events.next().await {
            match event? {
                Event::Apply(slice) | Event::InitApply(slice) => {
                    for peer in peers_from_endpoint_slice(&slice, &config) {
                        yield PeerEvent::Added(peer);
                    }
                }
                Event::Delete(slice) => {
                    for peer in peers_from_endpoint_slice(&slice, &config) {
                        yield PeerEvent::Removed(peer);
                    }
                }
                Event::Init | Event::InitDone => {}
            }
        }
    }
}

pub fn peers_from_endpoint_slice<'a>(
    slice: &'a EndpointSlice,
    config: &'a EndpointSliceDiscoveryConfig,
) -> impl Iterator<Item = Peer> + 'a {
    select_gabion_udp_port(slice)
        .into_iter()
        .flat_map(move |port| {
            slice
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
                .map(move |ip| Peer::new(SocketAddr::new(ip, port)))
                .filter(|peer| Some(peer.addr) != config.self_addr)
        })
}

fn select_gabion_udp_port(slice: &EndpointSlice) -> Option<u16> {
    let selected = slice
        .ports
        .as_ref()?
        .iter()
        .find(|port| is_gabion_udp_endpoint_port(port))?;
    selected.port.and_then(|port| u16::try_from(port).ok())
}

fn is_gabion_udp_endpoint_port(port: &EndpointPort) -> bool {
    port.name.as_deref() == Some(DEFAULT_GABION_SERVICE_NAME)
        && port.protocol.as_deref().unwrap_or("TCP") == "UDP"
}
