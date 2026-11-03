#[cfg(test)]
mod tests;

use crate::discovery::{DiscoveryConfig, Peer, PeerDiscovery, PeerEvent, SelfMatch};
use crate::zone::{ZoneHash, ZoneName};
use ahash::{AHashMap, AHashSet};
use async_stream::stream;
use futures::stream::{SelectAll, Stream, StreamExt, select_all};
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointSlice};
use kube::config::Config;
use kube::runtime::watcher::{Config as WatcherConfig, Error as WatcherError, Event, watcher};
use kube::{Api, Client};
use std::collections::hash_map::Entry;
use std::net::{IpAddr, SocketAddr};
use tokio::sync::oneshot;

const DEFAULT_GABION_SERVICE_NAME: &str = "gabion";

/// Watches every Service in the configured namespaces and, for each Service
/// exposing a UDP `gabion` port, watches its EndpointSlices. New services are
/// picked up live; deletions and disappearing ports emit `Removed` for every
/// peer only that Service listed. One peer set spans every watched Service,
/// so an address another Service or slice still lists is never removed. The
/// node's own entry, found by [`SelfMatch`], is never a peer: its zone is
/// reported as [`PeerEvent::OwnEntry`].
///
/// The kube `Client` is built lazily when `peer_events` is first polled. If
/// the in-cluster environment is missing, a warning is logged and
/// the stream completes empty.
#[derive(Clone)]
pub struct EndpointSliceDiscovery {
    config: DiscoveryConfig,
    /// Pre-built client; when `Some` we skip the in-cluster client
    /// construction entirely and use it as-is. Only the `#[ignore]`d
    /// live-cluster test in `tests.rs` sets it — production constructors
    /// leave it `None`.
    preconnected: Option<Client>,
}

impl EndpointSliceDiscovery {
    pub fn new(config: DiscoveryConfig) -> Self {
        Self {
            config,
            preconnected: None,
        }
    }
}

impl PeerDiscovery for EndpointSliceDiscovery {
    type Error = DiscoveryError;

    fn peer_events(self) -> impl Stream<Item = Result<PeerEvent, Self::Error>> + Send {
        stream! {
            let client = match self.preconnected.clone() {
                Some(c) => c,
                None => match build_incluster_client() {
                    Ok(client) => client,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "Could not connect to the Kubernetes API, so gabion \
                             cannot discover peers and this node's counts stay \
                             local to it. That is expected for a single node \
                             outside Kubernetes. In a pod, check that it has the \
                             auto-mounted \
                             ServiceAccount secrets at \
                             /var/run/secrets/kubernetes.io/serviceaccount/ \
                             and an RBAC binding granting watch on Services \
                             and EndpointSlices.",
                        );
                        return;
                    }
                },
            };
            let (mut services, namespace_count) = watch_services(&client, &self.config.namespace_allow);
            let mut endpoints = SelectAll::new();
            // Each watched Service's id and the sender whose drop ends its
            // EndpointSlice watch.
            let mut watched: AHashMap<Target, (TargetId, oneshot::Sender<()>)> = AHashMap::new();
            let mut next_target = 0_u64;
            let mut peer_set = EndpointSlicePeerSet::default();
            // True once every watched namespace has finished its initial
            // Service list without producing a single Track. The first time
            // that happens we emit a one-shot operator warning (see
            // CLAUDE.md's operator-facing-errors rules). Tracks observed
            // after the warning fires don't unset it — the operator already
            // has the information they need, and re-firing on every relist
            // would be noisy. `listed[i]` turns true at namespace `i`'s first
            // `InitDone`; a relist of one namespace says nothing of another.
            let mut zero_match_warned = false;
            let mut listed = vec![false; namespace_count].into_boxed_slice();
            let watched_namespaces = self.config.namespace_allow.clone();

            loop {
                tokio::select! {
                    Some((namespace, svc_event)) = services.next() => match svc_event {
                        Err(err) => {
                            // kube-rs's `watcher()` reconnects on every Err,
                            // even ones that will never heal (401/403, the
                            // requested resource doesn't exist). Spinning on
                            // those wastes CPU and floods error_log without
                            // making forward progress — the operator has to
                            // fix the SA/RBAC and restart the pod either way.
                            if let Some(status) = fatal_status(&err) {
                                tracing::error!(
                                    code = "kubernetes_forbidden",
                                    resource = "Service",
                                    status,
                                    error = %err,
                                    "The Kubernetes API rejected gabion's Service watch, so peer \
                                     discovery has stopped and the peer list stays as it was; \
                                     gossip continues with the peers already known. Usually the \
                                     pod's ServiceAccount lacks RBAC permission (`status` 401 or \
                                     403); `status` 404 means the cluster does not serve \
                                     `discovery.k8s.io/v1` EndpointSlices (Kubernetes 1.21 or \
                                     later is required). Grant the ServiceAccount get/list/watch \
                                     on Services and EndpointSlices and get on Pods (see \
                                     `deploy/kubernetes/gabion-discovery-rbac.yaml` and the \
                                     Kubernetes checklist in gabion's README), then restart the \
                                     pod.",
                                );
                                return;
                            }
                            yield Err(err.into());
                        }
                        Ok(event) => match service_change(event, &self.config.service_allow) {
                            ServiceChange::Track(target) => {
                                if let Entry::Vacant(slot) = watched.entry(target) {
                                    tracing::info!(
                                        namespace = %slot.key().namespace,
                                        service = %slot.key().service_name,
                                        "Found a gabion service in the cluster; \
                                         watching it for new peers.",
                                    );
                                    let (cancel_tx, cancel_rx) = oneshot::channel();
                                    let id = TargetId(next_target);
                                    next_target += 1;
                                    endpoints.push(Box::pin(watch_target(
                                        client.clone(),
                                        slot.key().clone(),
                                        id,
                                        cancel_rx,
                                    )));
                                    slot.insert((id, cancel_tx));
                                }
                            }
                            ServiceChange::Untrack(target) => {
                                // Dropping the sender ends the watch at its
                                // next poll, before it yields another event.
                                if let Some((id, _cancel)) = watched.remove(&target) {
                                    tracing::info!(
                                        namespace = %target.namespace,
                                        service = %target.service_name,
                                        "Gabion service is no longer reachable \
                                         (deleted or missing its UDP port); \
                                         stopping peer discovery for it.",
                                    );
                                    for peer in peer_set.forget(id) {
                                        yield Ok(PeerEvent::Removed(peer));
                                    }
                                }
                            }
                            ServiceChange::InitDone => {
                                if mark_listed(&mut listed, namespace)
                                    && !zero_match_warned
                                    && watched.is_empty()
                                {
                                    let ns_display = if watched_namespaces.is_empty() {
                                        "<pod's own namespace>".to_string()
                                    } else {
                                        watched_namespaces.join(",")
                                    };
                                    tracing::warn!(
                                        code = "no_gabion_service",
                                        watched_namespaces = %ns_display,
                                        gabion_port_name = DEFAULT_GABION_SERVICE_NAME,
                                        "Kubernetes auto-discovery is up but no \
                                         Service in the watched namespaces \
                                         exposes a UDP port named `gabion`. \
                                         Usually the manifest named the port \
                                         differently (`gossip`, `udp`, etc.) \
                                         or left the protocol as the default \
                                         TCP — discovery filters on both the \
                                         literal name `gabion` and \
                                         protocol `UDP`. Rename the port to \
                                         `name: gabion` with `protocol: UDP` \
                                         (see the Kubernetes checklist in gabion's \
                                         README, \
                                         https://github.com/prosody-events/gabion#kubernetes-checklist), \
                                         then re-apply the manifest.",
                                    );
                                    zero_match_warned = true;
                                }
                            }
                            ServiceChange::Ignore => {}
                        },
                    },
                    Some((id, event)) = endpoints.next() => match event {
                        Err(err) => yield Err(err.into()),
                        Ok(Event::Init) => peer_set.relist(id),
                        Ok(Event::InitDone) => {
                            for peer in peer_set.relisted(id) {
                                yield Ok(PeerEvent::Removed(peer));
                            }
                        }
                        Ok(Event::Apply(slice) | Event::InitApply(slice)) => {
                            let self_match = self.config.self_match.as_ref();
                            let diff = peer_set.apply(id, &slice, self_match);
                            // The own zone first, so the roles the peers
                            // below take already see it.
                            if let Some(own) = diff.own {
                                yield Ok(own.into());
                            }
                            for peer in diff.removed {
                                yield Ok(PeerEvent::Removed(peer));
                            }
                            for peer in diff.added {
                                yield Ok(PeerEvent::Added(peer));
                            }
                        }
                        Ok(Event::Delete(slice)) => {
                            for peer in peer_set.delete(id, &slice) {
                                yield Ok(PeerEvent::Removed(peer));
                            }
                        }
                    },
                }
            }
        }
    }
}

/// Build a Kubernetes client from in-cluster credentials. Tries the
/// canonical env-var path (`KUBERNETES_SERVICE_HOST`/`PORT`, what
/// client-go does) first, then falls back to DNS-based bootstrap against
/// `https://kubernetes.default.svc/`. The latter is what makes the same
/// code path work transparently inside an nginx worker, which scrubs
/// every env var except `TZ` on fork.
///
/// Both paths read the auto-mounted ServiceAccount triple at
/// `/var/run/secrets/kubernetes.io/serviceaccount/{token,ca.crt,namespace}`,
/// which kubelet injects into every pod regardless of which SA the pod
/// runs under. There is intentionally no kubeconfig probe — gabion only
/// runs in-pod, and the kubeconfig branch of `Config::infer` produces
/// misleading "No such file" error strings against the worker user's
/// HOME (e.g. `/var/cache/nginx`).
fn build_incluster_client() -> Result<Client, InClusterClientError> {
    let config = match Config::incluster_env() {
        Ok(cfg) => cfg,
        Err(env_err) => {
            tracing::debug!(
                error = %env_err,
                "in-cluster config via KUBERNETES_SERVICE_HOST/PORT \
                 unavailable; falling back to DNS-based bootstrap",
            );
            Config::incluster_dns()?
        }
    };
    Ok(Client::try_from(config)?)
}

/// Concrete error for [`build_incluster_client`]. Each variant captures
/// the underlying kube-rs error verbatim so the warning we log preserves
/// the same surface `Client::try_default` would have produced.
#[derive(Debug, thiserror::Error)]
enum InClusterClientError {
    #[error("failed to load in-cluster config: {0}")]
    Config(#[from] kube::config::InClusterError),
    #[error("failed to construct kube client: {0}")]
    Client(#[from] kube::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("kubernetes watch failed: {0}")]
pub struct DiscoveryError(#[from] WatcherError);

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
    /// One namespace's Service list (initial or a relist) has finished
    /// syncing. Once every watched namespace has listed with no tracks, the
    /// caller surfaces a one-shot operator warning: no Service in any
    /// watched namespace exposes a `gabion`-named UDP port.
    InitDone,
    Ignore,
}

fn service_change(event: Event<Service>, allow: &[Box<str>]) -> ServiceChange {
    let (svc, present) = match event {
        Event::Apply(svc) | Event::InitApply(svc) => {
            let present = has_gabion_udp_port(&svc);
            (svc, present)
        }
        Event::Delete(svc) => (svc, false),
        Event::Init => return ServiceChange::Ignore,
        Event::InitDone => return ServiceChange::InitDone,
    };
    let Some(target) = Target::take_from(svc) else {
        return ServiceChange::Ignore;
    };
    if !matches_allow(&target.service_name, allow) {
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

/// Mark namespace `index` as listed; true once every namespace has listed.
fn mark_listed(listed: &mut [bool], index: usize) -> bool {
    listed[index] = true;
    listed.iter().all(|&done| done)
}

/// One Service watcher per namespace, merged, each event tagged with its
/// namespace's index, plus the number of namespaces.
fn watch_services(
    client: &Client,
    namespace_allow: &[Box<str>],
) -> (
    impl Stream<Item = (usize, Result<Event<Service>, WatcherError>)> + Send + Unpin,
    usize,
) {
    let mut apis: Vec<Api<Service>> = namespace_allow
        .iter()
        .map(|ns| Api::namespaced(client.clone(), ns))
        .collect();
    if apis.is_empty() {
        // Default to the pod's own namespace. `Client::default_namespace()`
        // is populated from
        // `/var/run/secrets/kubernetes.io/serviceaccount/namespace`
        // by both `Config::incluster_env` and `Config::incluster_dns`, so it
        // tracks wherever the pod was actually scheduled — no kubeconfig
        // magic, no operator-supplied namespace directive needed for the
        // common single-namespace deployment. Cross-namespace discovery
        // (e.g. one HTTP namespace, separate gossip namespace) still works
        // by listing namespaces explicitly via the
        // `gabion_discovery_namespace_allow` directive.
        let ns = client.default_namespace().to_owned();
        apis.push(Api::namespaced(client.clone(), &ns));
    }
    let count = apis.len();
    let watchers = apis.into_iter().enumerate().map(|(index, api)| {
        Box::pin(watcher(api, WatcherConfig::default()).map(move |event| (index, event)))
    });
    (select_all(watchers), count)
}

/// The HTTP status of a kube-rs watcher error that is not worth retrying.
/// `watcher()` reconnects on every Err, but auth (401, 403) and "this
/// resource doesn't exist" (404) failures don't heal without an operator
/// change — we'd just spin forever in the meantime. Anything
/// transport-shaped (TLS, hyper, service errors, no resource version) is
/// `None` and left to retry.
fn fatal_status(err: &WatcherError) -> Option<u16> {
    let code = match err {
        WatcherError::InitialListFailed(e)
        | WatcherError::WatchStartFailed(e)
        | WatcherError::WatchFailed(e) => {
            let kube::Error::Api(api_err) = e else {
                return None;
            };
            api_err.code
        }
        // `WatchError` carries an `ErrorResponse` directly (apiserver sent
        // back a Status object mid-stream), so check its code too.
        WatcherError::WatchError(api_err) => api_err.code,
        WatcherError::NoResourceVersion => return None,
    };
    matches!(code, 401 | 403 | 404).then_some(code)
}

/// True if `name` is in `allow`. An empty list allows every name.
fn matches_allow(name: &str, allow: &[Box<str>]) -> bool {
    allow.is_empty() || allow.iter().any(|s| **s == *name)
}

fn is_gabion_udp(name: Option<&str>, protocol: Option<&str>) -> bool {
    name == Some(DEFAULT_GABION_SERVICE_NAME) && protocol == Some("UDP")
}

/// One watched Service's EndpointSlice events, tagged with its id, until
/// `cancel` fires or its sender drops; a fatal error is logged and ends the
/// watch.
fn watch_target(
    client: Client,
    target: Target,
    id: TargetId,
    mut cancel: oneshot::Receiver<()>,
) -> impl Stream<Item = (TargetId, Result<Event<EndpointSlice>, WatcherError>)> + Send {
    stream! {
        let api: Api<EndpointSlice> = Api::namespaced(client, &target.namespace);
        let labels = format!("kubernetes.io/service-name={}", target.service_name);
        let events = watcher(api, WatcherConfig::default().labels(&labels));
        futures::pin_mut!(events);

        loop {
            tokio::select! {
                biased;
                _ = &mut cancel => return,
                event = events.next() => match event {
                    None => return,
                    Some(Err(err)) => {
                        if let Some(status) = fatal_status(&err) {
                            tracing::error!(
                                code = "kubernetes_forbidden",
                                resource = "EndpointSlice",
                                namespace = %target.namespace,
                                service = %target.service_name,
                                status,
                                error = %err,
                                "The Kubernetes API rejected gabion's EndpointSlice watch for \
                                 this Service, so discovery of its peers has stopped and they \
                                 stay in the peer list as they were; gossip continues with the \
                                 peers already known. Usually the pod's ServiceAccount lacks \
                                 RBAC permission (`status` 401 or 403); `status` 404 means the \
                                 cluster does not serve `discovery.k8s.io/v1` EndpointSlices \
                                 (Kubernetes 1.21 or later is required). Grant the \
                                 ServiceAccount get/list/watch on Services and EndpointSlices \
                                 and get on Pods (see \
                                 `deploy/kubernetes/gabion-discovery-rbac.yaml` and the \
                                 Kubernetes checklist in gabion's README), then restart the \
                                 pod.",
                            );
                            return;
                        }
                        yield (id, Err(err));
                    }
                    Some(Ok(event)) => yield (id, Ok(event)),
                },
            }
        }
    }
}

/// One watched Service, numbered in the order discovery found it; a Service
/// found again gets a new id.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct TargetId(pub(super) u64);

/// Peer accounting across every watched Service, driven by the discovery
/// stream for the lifetime of its watches: each Service's slices keyed by
/// EndpointSlice `metadata.name`, so an `Apply` replaces that slice's set in
/// place, and each slice's peers by address, so a zone change is one `Added`.
/// An address any slice of any Service still lists is not removed, and a
/// relist forgets each slice it does not list, since the watcher reports no
/// `Delete` for a slice deleted while it was disconnected. Lifted out of the
/// stream so the diff semantics are unit-testable without a kube client.
#[derive(Default)]
pub(super) struct EndpointSlicePeerSet {
    /// Each watched Service's slices.
    targets: AHashMap<TargetId, Slices>,
    /// The own entry's zone as last reported; `None` until first seen. An
    /// entry that goes missing or not ready never clears it.
    own: Option<Option<ZoneHash>>,
}

/// One Service's cached slices.
#[derive(Default)]
struct Slices {
    by_slice: AHashMap<String, AHashMap<SocketAddr, Peer>>,
    /// The cached slices a relist in progress has not listed yet.
    unlisted: AHashSet<String>,
}

/// This node's own entry, as discovery reports it.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct OwnEntry {
    pub(super) zone: Option<ZoneName>,
    pub(super) node_name: Option<Box<str>>,
}

impl From<OwnEntry> for PeerEvent {
    fn from(own: OwnEntry) -> Self {
        PeerEvent::OwnEntry {
            zone: own.zone,
            node_name: own.node_name,
        }
    }
}

/// What `apply` changed relative to the previous snapshot for that
/// slice — separated so the caller can emit the own zone first and
/// `Removed` before `Added` (matching how the kube-rs watcher reports an
/// Apply that both adds and removes addresses within one slice).
#[derive(Debug, Default, Eq, PartialEq)]
pub(super) struct PeerSetDiff {
    /// The own entry, when this slice holds it and its zone is new.
    pub(super) own: Option<OwnEntry>,
    pub(super) added: Vec<Peer>,
    pub(super) removed: Vec<Peer>,
}

impl EndpointSlicePeerSet {
    /// Replace Service `id`'s cached peer set for `slice.metadata.name` with
    /// the parsed peers of `slice`. Returns the own entry when its zone is
    /// new, the addresses gone from every slice as `removed` and the peers
    /// new or changed as `added`. A slice with no name is silently dropped.
    pub(super) fn apply(
        &mut self,
        id: TargetId,
        slice: &EndpointSlice,
        self_match: Option<&SelfMatch>,
    ) -> PeerSetDiff {
        let Some(name) = slice.metadata.name.as_deref() else {
            return PeerSetDiff::default();
        };
        let (new, own) = peers_from_endpoint_slice(slice, self_match);
        let own = own.and_then(|endpoint| self.own_entry(endpoint));
        let slices = self.targets.entry(id).or_default();
        slices.unlisted.remove(name);
        let old = slices.by_slice.remove(name).unwrap_or_default();
        let added = (new.values())
            .filter(|&peer| old.get(&peer.addr) != Some(peer))
            .copied()
            .collect();
        slices.by_slice.insert(name.to_owned(), new);
        PeerSetDiff {
            own,
            added,
            removed: self.unlisted_peers(old),
        }
    }

    /// Forget Service `id`'s cached peer set for `slice`; returns the peers
    /// gone from every slice, which the caller now needs to emit `Removed`
    /// for. Unknown slice names are a no-op.
    pub(super) fn delete(&mut self, id: TargetId, slice: &EndpointSlice) -> Vec<Peer> {
        let peers = (slice.metadata.name.as_deref())
            .zip(self.targets.get_mut(&id))
            .and_then(|(name, slices)| slices.by_slice.remove(name));
        self.unlisted_peers(peers.unwrap_or_default())
    }

    /// A relist of Service `id` starts: each of its cached slices is unlisted
    /// until it lists it.
    pub(super) fn relist(&mut self, id: TargetId) {
        let slices = self.targets.entry(id).or_default();
        slices.unlisted = slices.by_slice.keys().cloned().collect();
    }

    /// A relist of Service `id` ends: forget each of its slices it did not
    /// list; returns the peers gone from every slice, which the caller emits
    /// `Removed` for.
    pub(super) fn relisted(&mut self, id: TargetId) -> Vec<Peer> {
        let mut gone = AHashMap::new();
        if let Some(slices) = self.targets.get_mut(&id) {
            for name in std::mem::take(&mut slices.unlisted) {
                gone.extend(slices.by_slice.remove(&name).into_iter().flatten());
            }
        }
        self.unlisted_peers(gone)
    }

    /// Discovery stopped watching Service `id`: forget its slices; returns
    /// the peers no other Service lists, which the caller emits `Removed`
    /// for.
    pub(super) fn forget(&mut self, id: TargetId) -> Vec<Peer> {
        let gone = (self.targets.remove(&id).into_iter())
            .flat_map(|slices| slices.by_slice.into_values().flatten())
            .collect();
        self.unlisted_peers(gone)
    }

    /// The peers of `gone` no cached slice of any Service lists.
    fn unlisted_peers(&self, gone: AHashMap<SocketAddr, Peer>) -> Vec<Peer> {
        gone.into_values()
            .filter(|peer| !self.lists(&peer.addr))
            .collect()
    }

    /// Whether a cached slice of any Service lists `addr`: Kubernetes may
    /// list an endpoint in two slices at once while it moves between them,
    /// and two Services may select the same pod.
    fn lists(&self, addr: &SocketAddr) -> bool {
        (self.targets.values())
            .flat_map(|slices| slices.by_slice.values())
            .any(|peers| peers.contains_key(addr))
    }

    /// The own entry to report for `endpoint`, if its zone changed since the
    /// last report. A name that is missing, empty or not a zone name is no
    /// zone.
    fn own_entry(&mut self, endpoint: &Endpoint) -> Option<OwnEntry> {
        let zone = (endpoint.zone.as_deref()).and_then(|zone| ZoneName::parse(zone).ok());
        let hash = zone.as_ref().map(ZoneName::hash);
        if self.own == Some(hash) {
            return None;
        }
        self.own = Some(hash);
        Some(OwnEntry {
            zone,
            node_name: endpoint.node_name.as_deref().map(Box::from),
        })
    }
}

/// Parse an EndpointSlice into the (deduplicated, ready-only) peer set, each
/// with the zone its endpoint names, and this node's own endpoint, which is
/// matched before the readiness filter (a starting pod is often not ready,
/// and its zone is valid anyway) and is never a peer. Exposed to `tests`
/// rather than kept private to the runtime so the parser invariants are
/// pinned by unit tests.
pub(super) fn peers_from_endpoint_slice<'s>(
    slice: &'s EndpointSlice,
    self_match: Option<&SelfMatch>,
) -> (AHashMap<SocketAddr, Peer>, Option<&'s Endpoint>) {
    let mut peers = AHashMap::new();
    let Some(port) = select_gabion_udp_port(slice) else {
        return (peers, None);
    };
    let mut own = None;
    for endpoint in &slice.endpoints {
        if self_match.is_some_and(|self_match| is_own(endpoint, port, self_match)) {
            own = Some(endpoint);
            continue;
        }
        if !(endpoint.conditions.as_ref())
            .and_then(|c| c.ready)
            .unwrap_or(true)
        {
            continue;
        }
        let zone = endpoint_zone(endpoint);
        for addr in &endpoint.addresses {
            if let Ok(ip) = addr.parse::<IpAddr>() {
                let addr = SocketAddr::new(ip, port);
                peers.insert(addr, Peer { addr, zone });
            }
        }
    }
    (peers, own)
}

/// Whether `endpoint` is this node's: one of its addresses with the gabion
/// `port` is `discovery.self_addr`, or its `targetRef` is the Pod named like
/// this host.
fn is_own(endpoint: &Endpoint, port: u16, self_match: &SelfMatch) -> bool {
    match self_match {
        SelfMatch::Addr(self_addr) => endpoint.addresses.iter().any(|addr| {
            addr.parse::<IpAddr>()
                .is_ok_and(|ip| SocketAddr::new(ip, port) == *self_addr)
        }),
        SelfMatch::PodName(pod) => endpoint.target_ref.as_ref().is_some_and(|target| {
            target.kind.as_deref() == Some("Pod") && target.name.as_deref() == Some(&**pod)
        }),
    }
}

/// The zone an endpoint names (`endpoints[].zone`, which Kubernetes copies
/// from its node's `topology.kubernetes.io/zone` label); one missing, empty
/// or not a zone name is none.
fn endpoint_zone(endpoint: &Endpoint) -> Option<ZoneHash> {
    ZoneHash::parse(endpoint.zone.as_deref()?).ok()
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
