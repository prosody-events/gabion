use crate::discovery::{Peer, PeerDiscovery, PeerEvent};
use ahash::{AHashMap, AHashSet};
use async_stream::stream;
use futures::stream::{SelectAll, Stream, StreamExt, select_all};
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::runtime::watcher::{Config as WatcherConfig, Error as WatcherError, Event, watcher};
use kube::{Api, Client};
use std::collections::hash_map::Entry;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use tokio::sync::oneshot;

const DEFAULT_GABION_SERVICE_NAME: &str = "gabion";

/// Watches every Service in the configured namespaces and, for each Service
/// exposing a UDP `gabion` port, watches its EndpointSlices. New services are
/// picked up live; deletions and disappearing ports emit `Removed` for every
/// peer the Service had contributed before its watcher is shut down.
///
/// The kube `Client` is built lazily when `peer_events` is first polled. If
/// the in-cluster/kubeconfig environment is missing, a warning is logged and
/// the stream completes empty.
#[derive(Clone)]
pub struct EndpointSliceDiscovery {
    self_addr: Option<SocketAddr>,
    namespace_whitelist: Vec<String>,
    service_whitelist: Vec<String>,
}

impl EndpointSliceDiscovery {
    pub fn new(
        self_addr: Option<SocketAddr>,
        namespace_whitelist: Vec<String>,
        service_whitelist: Vec<String>,
    ) -> Self {
        Self {
            self_addr,
            namespace_whitelist,
            service_whitelist,
        }
    }
}

impl PeerDiscovery for EndpointSliceDiscovery {
    type Error = DiscoveryError;

    fn peer_events(self) -> impl Stream<Item = Result<PeerEvent, Self::Error>> + Send {
        stream! {
            let client = match Client::try_default().await {
                Ok(client) => client,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "kubernetes client unavailable; peer discovery disabled",
                    );
                    return;
                }
            };
            let mut services = watch_services(&client, &self.namespace_whitelist);
            let mut endpoints = SelectAll::new();
            let mut watched: AHashMap<Target, oneshot::Sender<()>> = AHashMap::new();

            loop {
                tokio::select! {
                    Some(svc_event) = services.next() => match svc_event {
                        Err(err) => yield Err(err.into()),
                        Ok(event) => match service_change(event, &self.service_whitelist) {
                            ServiceChange::Track(target) => {
                                if let Entry::Vacant(slot) = watched.entry(target) {
                                    let (cancel_tx, cancel_rx) = oneshot::channel();
                                    endpoints.push(Box::pin(watch_target(
                                        client.clone(),
                                        slot.key().clone(),
                                        self.self_addr,
                                        cancel_rx,
                                    )));
                                    slot.insert(cancel_tx);
                                }
                            }
                            ServiceChange::Untrack(target) => { watched.remove(&target); }
                            ServiceChange::Ignore => {}
                        },
                    },
                    Some(peer_event) = endpoints.next() => yield peer_event,
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct DiscoveryError(WatcherError);

impl fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "kubernetes watch failed: {}", self.0)
    }
}

impl std::error::Error for DiscoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl From<WatcherError> for DiscoveryError {
    fn from(err: WatcherError) -> Self {
        Self(err)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Target {
    namespace: String,
    service_name: String,
}

impl Target {
    fn take_from(service: Service) -> Option<Self> {
        Some(Self {
            namespace: service.metadata.namespace?,
            service_name: service.metadata.name?,
        })
    }
}

enum ServiceChange {
    Track(Target),
    Untrack(Target),
    Ignore,
}

fn service_change(event: Event<Service>, whitelist: &[String]) -> ServiceChange {
    let (svc, present) = match event {
        Event::Apply(svc) | Event::InitApply(svc) => {
            let present = has_gabion_udp_port(&svc);
            (svc, present)
        }
        Event::Delete(svc) => (svc, false),
        Event::Init | Event::InitDone => return ServiceChange::Ignore,
    };
    let Some(target) = Target::take_from(svc) else {
        return ServiceChange::Ignore;
    };
    if !matches_whitelist(&target.service_name, whitelist) {
        return ServiceChange::Ignore;
    }
    if present {
        ServiceChange::Track(target)
    } else {
        ServiceChange::Untrack(target)
    }
}

fn has_gabion_udp_port(service: &Service) -> bool {
    service
        .spec
        .iter()
        .flat_map(|spec| spec.ports.iter().flatten())
        .any(|p| is_gabion_udp(p.name.as_deref(), p.protocol.as_deref()))
}

fn watch_services(
    client: &Client,
    namespace_whitelist: &[String],
) -> impl Stream<Item = Result<Event<Service>, WatcherError>> + Send + Unpin {
    let mut apis: Vec<Api<Service>> = namespace_whitelist
        .iter()
        .filter(|s| !s.is_empty())
        .map(|ns| Api::namespaced(client.clone(), ns))
        .collect();
    if apis.is_empty() {
        apis.push(Api::all(client.clone()));
    }
    select_all(
        apis.into_iter()
            .map(|api| Box::pin(watcher(api, WatcherConfig::default()))),
    )
}

/// True if `name` matches any non-empty entry in `whitelist`. A whitelist with
/// no non-empty entries matches everything.
fn matches_whitelist(name: &str, whitelist: &[String]) -> bool {
    let mut entries = whitelist.iter().filter(|s| !s.is_empty()).peekable();
    entries.peek().is_none() || entries.any(|s| s == name)
}

fn is_gabion_udp(name: Option<&str>, protocol: Option<&str>) -> bool {
    name == Some(DEFAULT_GABION_SERVICE_NAME) && protocol == Some("UDP")
}

fn watch_target(
    client: Client,
    target: Target,
    self_addr: Option<SocketAddr>,
    mut cancel: oneshot::Receiver<()>,
) -> impl Stream<Item = Result<PeerEvent, DiscoveryError>> + Send {
    stream! {
        let api: Api<EndpointSlice> = Api::namespaced(client, &target.namespace);
        let labels = format!("kubernetes.io/service-name={}", target.service_name);
        let events = watcher(api, WatcherConfig::default().labels(&labels));
        futures::pin_mut!(events);
        let mut by_slice: AHashMap<String, AHashSet<Peer>> = AHashMap::new();

        loop {
            tokio::select! {
                biased;
                _ = &mut cancel => {
                    for peer in by_slice.into_values().flatten() {
                        yield Ok(PeerEvent::Removed(peer));
                    }
                    return;
                }
                event = events.next() => match event {
                    None => return,
                    Some(Err(err)) => yield Err(err.into()),
                    Some(Ok(Event::Init | Event::InitDone)) => {}
                    Some(Ok(Event::Apply(slice) | Event::InitApply(slice))) => {
                        let new: AHashSet<Peer> = peers(&slice, self_addr).collect();
                        let Some(name) = slice.metadata.name else { continue };
                        let old = by_slice.remove(&name).unwrap_or_default();
                        for &peer in old.difference(&new) {
                            yield Ok(PeerEvent::Removed(peer));
                        }
                        for &peer in new.difference(&old) {
                            yield Ok(PeerEvent::Added(peer));
                        }
                        by_slice.insert(name, new);
                    }
                    Some(Ok(Event::Delete(slice))) => {
                        let Some(name) = slice.metadata.name.as_deref() else { continue };
                        if let Some(old) = by_slice.remove(name) {
                            for peer in old {
                                yield Ok(PeerEvent::Removed(peer));
                            }
                        }
                    }
                },
            }
        }
    }
}

fn peers(slice: &EndpointSlice, self_addr: Option<SocketAddr>) -> impl Iterator<Item = Peer> + '_ {
    select_gabion_udp_port(slice)
        .into_iter()
        .flat_map(move |port| {
            slice
                .endpoints
                .iter()
                .filter(|e| e.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true))
                .flat_map(|e| &e.addresses)
                .filter_map(move |addr| {
                    let sock = SocketAddr::new(addr.parse::<IpAddr>().ok()?, port);
                    (Some(sock) != self_addr).then_some(Peer::new(sock))
                })
        })
}

fn select_gabion_udp_port(slice: &EndpointSlice) -> Option<u16> {
    slice
        .ports
        .iter()
        .flatten()
        .find(|p| is_gabion_udp(p.name.as_deref(), p.protocol.as_deref()))?
        .port
        .and_then(|p| u16::try_from(p).ok())
}
