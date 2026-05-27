#[cfg(test)]
mod tests;

use crate::discovery::{Peer, PeerDiscovery, PeerEvent};
use ahash::{AHashMap, AHashSet};
use async_stream::stream;
use futures::stream::{SelectAll, Stream, StreamExt, select_all};
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;
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
/// peer the Service had contributed before its watcher is shut down.
///
/// The kube `Client` is built lazily when `peer_events` is first polled. If
/// the in-cluster/kubeconfig environment is missing, a warning is logged and
/// the stream completes empty.
#[derive(Clone)]
pub struct EndpointSliceDiscovery {
    self_addr: Option<SocketAddr>,
    namespace_allow: Vec<String>,
    service_allow: Vec<String>,
    /// Pre-built client; when `Some` we skip the in-cluster client
    /// construction entirely and use it as-is. Only the
    /// kubeconfig-aware test constructor populates this — production
    /// constructors leave it `None` so the in-cluster credential
    /// loading path on the production hot start path is byte-identical.
    preconnected: Option<Client>,
}

impl EndpointSliceDiscovery {
    pub fn new(
        self_addr: Option<SocketAddr>,
        namespace_allow: Vec<String>,
        service_allow: Vec<String>,
    ) -> Self {
        Self {
            self_addr,
            namespace_allow,
            service_allow,
            preconnected: None,
        }
    }

    /// Build an `EndpointSliceDiscovery` from an already-constructed
    /// `kube::Client`. Used by the live-cluster integration test, which
    /// resolves credentials via `Config::infer()` (kubeconfig + env
    /// vars) so it can run against a kind cluster from the workstation.
    /// Production code paths never reach this — they construct the
    /// client lazily via [`build_incluster_client`].
    #[doc(hidden)]
    pub fn with_client(
        client: Client,
        self_addr: Option<SocketAddr>,
        namespace_allow: Vec<String>,
        service_allow: Vec<String>,
    ) -> Self {
        Self {
            self_addr,
            namespace_allow,
            service_allow,
            preconnected: Some(client),
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
                            "Could not connect to the Kubernetes API; gabion \
                             cannot auto-discover peers. This node will only \
                             talk to peers listed in `gossip.bootstrap_peers`. \
                             Check that the pod has the auto-mounted \
                             ServiceAccount secrets at \
                             /var/run/secrets/kubernetes.io/serviceaccount/ \
                             and an RBAC binding granting watch on Services \
                             and EndpointSlices.",
                        );
                        return;
                    }
                },
            };
            let mut services = watch_services(&client, &self.namespace_allow);
            let mut endpoints = SelectAll::new();
            let mut watched: AHashMap<Target, oneshot::Sender<()>> = AHashMap::new();
            // True once *any* namespace has finished its initial Service list
            // without producing a single Track. The first time that happens
            // we emit a one-shot operator warning (see CLAUDE.md's
            // operator-facing-errors rules). Tracks observed after the
            // warning fires don't unset it — the operator already has the
            // information they need, and re-firing on every relist would
            // be noisy.
            let mut zero_match_warned = false;
            let watched_namespaces = self.namespace_allow.clone();

            loop {
                tokio::select! {
                    Some(svc_event) = services.next() => match svc_event {
                        Err(err) => {
                            // kube-rs's `watcher()` reconnects on every Err,
                            // even ones that will never heal (401/403, the
                            // requested resource doesn't exist). Spinning on
                            // those wastes CPU and floods error_log without
                            // making forward progress — the operator has to
                            // fix the SA/RBAC and restart the pod either way.
                            let fatal = is_fatal_watcher_error(&err);
                            if fatal {
                                tracing::error!(
                                    error = %err,
                                    "Kubernetes API rejected our credentials \
                                     for service watches; stopping peer \
                                     discovery. Grant the pod's ServiceAccount \
                                     get/list/watch on Services and \
                                     EndpointSlices, then restart this pod.",
                                );
                            }
                            yield Err(err.into());
                            if fatal {
                                return;
                            }
                        }
                        Ok(event) => match service_change(event, &self.service_allow) {
                            ServiceChange::Track(target) => {
                                if let Entry::Vacant(slot) = watched.entry(target) {
                                    tracing::info!(
                                        namespace = %slot.key().namespace,
                                        service = %slot.key().service_name,
                                        "Found a gabion service in the cluster; \
                                         watching it for new peers.",
                                    );
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
                            ServiceChange::Untrack(target) => {
                                if watched.remove(&target).is_some() {
                                    tracing::info!(
                                        namespace = %target.namespace,
                                        service = %target.service_name,
                                        "Gabion service is no longer reachable \
                                         (deleted or missing its UDP port); \
                                         stopping peer discovery for it.",
                                    );
                                }
                            }
                            ServiceChange::InitDone => {
                                if !zero_match_warned && watched.is_empty() {
                                    let ns_display = if watched_namespaces.is_empty() {
                                        "<pod's own namespace>".to_string()
                                    } else {
                                        watched_namespaces.join(",")
                                    };
                                    tracing::warn!(
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
                                         (see \
                                         `deploy/kubernetes/nginx-scale-rate-limit.sh` \
                                         for a working example), then re-apply \
                                         the manifest.",
                                    );
                                    zero_match_warned = true;
                                }
                            }
                            ServiceChange::Ignore => {}
                        },
                    },
                    Some(peer_event) = endpoints.next() => yield peer_event,
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
    /// One namespace's initial Service list has finished syncing. The
    /// caller uses this boundary to detect a zero-match
    /// misconfiguration (no Service in any watched namespace exposes a
    /// `gabion`-named UDP port) and surface a one-shot operator
    /// warning. Distinct from `Ignore` so the warning path can fire
    /// exactly once at the first end-of-init-sync that still has no
    /// tracks.
    InitDone,
    Ignore,
}

fn service_change(event: Event<Service>, allow: &[String]) -> ServiceChange {
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

fn watch_services(
    client: &Client,
    namespace_allow: &[String],
) -> impl Stream<Item = Result<Event<Service>, WatcherError>> + Send + Unpin {
    let mut apis: Vec<Api<Service>> = namespace_allow
        .iter()
        .filter(|s| !s.is_empty())
        .map(|ns| Api::namespaced(client.clone(), ns))
        .collect();
    if apis.is_empty() {
        // Default to the pod's own namespace. `Client::default_namespace()`
        // is populated from `/var/run/secrets/kubernetes.io/serviceaccount/namespace`
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
    select_all(
        apis.into_iter()
            .map(|api| Box::pin(watcher(api, WatcherConfig::default()))),
    )
}

/// Decide whether a kube-rs watcher error is worth retrying. `watcher()`
/// reconnects on every Err, but auth and "this resource doesn't exist"
/// failures don't heal without an operator change — we'd just spin
/// forever in the meantime. Anything transport-shaped (TLS, hyper,
/// service errors, no resource version) is left to retry.
fn is_fatal_watcher_error(err: &WatcherError) -> bool {
    let client_err = match err {
        WatcherError::InitialListFailed(e)
        | WatcherError::WatchStartFailed(e)
        | WatcherError::WatchFailed(e) => e,
        // `WatchError` carries an `ErrorResponse` directly (apiserver sent
        // back a Status object mid-stream), so check its code too.
        WatcherError::WatchError(api_err) => return is_fatal_status_code(api_err.code),
        WatcherError::NoResourceVersion => return false,
    };
    if let kube::Error::Api(api_err) = client_err {
        is_fatal_status_code(api_err.code)
    } else {
        false
    }
}

fn is_fatal_status_code(code: u16) -> bool {
    matches!(code, 401 | 403 | 404)
}

/// True if `name` matches any non-empty entry in `allow`. An allow list
/// with no non-empty entries matches everything.
fn matches_allow(name: &str, allow: &[String]) -> bool {
    let mut entries = allow.iter().filter(|s| !s.is_empty()).peekable();
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
        let mut peer_set = EndpointSlicePeerSet::default();

        loop {
            tokio::select! {
                biased;
                _ = &mut cancel => {
                    for peer in peer_set.drain_peers() {
                        yield Ok(PeerEvent::Removed(peer));
                    }
                    return;
                }
                event = events.next() => match event {
                    None => return,
                    Some(Err(err)) => {
                        let fatal = is_fatal_watcher_error(&err);
                        if fatal {
                            tracing::error!(
                                namespace = %target.namespace,
                                service = %target.service_name,
                                error = %err,
                                "Kubernetes API rejected our credentials for \
                                 EndpointSlice watches on this service; \
                                 stopping discovery for it.",
                            );
                        }
                        yield Err(err.into());
                        if fatal {
                            return;
                        }
                    }
                    Some(Ok(Event::Init | Event::InitDone)) => {}
                    Some(Ok(Event::Apply(slice) | Event::InitApply(slice))) => {
                        let diff = peer_set.apply(&slice, self_addr);
                        for peer in diff.removed {
                            yield Ok(PeerEvent::Removed(peer));
                        }
                        for peer in diff.added {
                            yield Ok(PeerEvent::Added(peer));
                        }
                    }
                    Some(Ok(Event::Delete(slice))) => {
                        for peer in peer_set.delete(&slice) {
                            yield Ok(PeerEvent::Removed(peer));
                        }
                    }
                },
            }
        }
    }
}

/// Per-slice peer accounting for one `Service` target. `watch_target`
/// drives one of these for the lifetime of its EndpointSlice watch; the
/// inner map is keyed by EndpointSlice `metadata.name` so that
/// successive `Apply` events for the same slice replace the old peer
/// set in place. Lifted out of `watch_target` as its own type so the
/// snapshot/diff semantics are unit-testable without spinning up a kube
/// client.
#[derive(Default)]
pub(super) struct EndpointSlicePeerSet {
    by_slice: AHashMap<String, AHashSet<Peer>>,
}

/// What `apply` changed relative to the previous snapshot for that
/// slice — separated so the caller can emit `Removed` before `Added`
/// (matching how the kube-rs watcher reports an Apply that both adds
/// and removes addresses within one slice).
#[derive(Debug, Default, Eq, PartialEq)]
pub(super) struct PeerSetDiff {
    pub(super) added: Vec<Peer>,
    pub(super) removed: Vec<Peer>,
}

impl EndpointSlicePeerSet {
    /// Replace the cached peer set for `slice.metadata.name` with the
    /// parsed peers of `slice`. Returns the symmetric difference. A
    /// slice with no name is silently dropped (matches the in-tree
    /// `continue` in `watch_target`).
    pub(super) fn apply(
        &mut self,
        slice: &EndpointSlice,
        self_addr: Option<SocketAddr>,
    ) -> PeerSetDiff {
        let Some(name) = slice.metadata.name.as_deref() else {
            return PeerSetDiff::default();
        };
        let new: AHashSet<Peer> = peers_from_endpoint_slice(slice, self_addr);
        let old = self.by_slice.remove(name).unwrap_or_default();
        let mut diff = PeerSetDiff::default();
        for &peer in old.difference(&new) {
            diff.removed.push(peer);
        }
        for &peer in new.difference(&old) {
            diff.added.push(peer);
        }
        self.by_slice.insert(name.to_owned(), new);
        diff
    }

    /// Forget the cached peer set for `slice`; returns the peers the
    /// caller now needs to emit `Removed` for. Unknown slice names are
    /// a no-op.
    pub(super) fn delete(&mut self, slice: &EndpointSlice) -> Vec<Peer> {
        let Some(name) = slice.metadata.name.as_deref() else {
            return Vec::new();
        };
        self.by_slice
            .remove(name)
            .map(|set| set.into_iter().collect())
            .unwrap_or_default()
    }

    /// Replace the entire peer map with `other`'s. Used on
    /// relist/re-init to drop slices the watcher no longer reports.
    /// Returns nothing — callers handle the diff by comparing their
    /// own snapshots before and after.
    #[cfg(test)]
    pub(super) fn replace(&mut self, other: EndpointSlicePeerSet) {
        self.by_slice = other.by_slice;
    }

    /// Empty the map and yield every peer it contained — used at
    /// watcher shutdown to emit `Removed` for everything still live.
    pub(super) fn drain_peers(&mut self) -> impl Iterator<Item = Peer> + '_ {
        std::mem::take(&mut self.by_slice).into_values().flatten()
    }

    /// Sorted, deduplicated union of every cached slice's peer set.
    /// Used by tests to assert against a model snapshot.
    #[cfg(test)]
    pub(super) fn snapshot_peers(&self) -> Vec<Peer> {
        let mut peers: Vec<Peer> = self.by_slice.values().flatten().copied().collect();
        peers.sort();
        peers.dedup();
        peers
    }
}

/// Parse an EndpointSlice into the (deduplicated, self-filtered, ready-
/// only) peer set. Exposed to `tests` rather than kept private to the
/// runtime so the parser invariants are pinned by unit tests.
pub(super) fn peers_from_endpoint_slice(
    slice: &EndpointSlice,
    self_addr: Option<SocketAddr>,
) -> AHashSet<Peer> {
    let Some(port) = select_gabion_udp_port(slice) else {
        return AHashSet::new();
    };
    slice
        .endpoints
        .iter()
        .filter(|e| e.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true))
        .flat_map(|e| &e.addresses)
        .filter_map(|addr| {
            let sock = SocketAddr::new(addr.parse::<IpAddr>().ok()?, port);
            (Some(sock) != self_addr).then_some(Peer::new(sock))
        })
        .collect()
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
