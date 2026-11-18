//! Peer discovery: how a node finds its peers ([`Membership`]), and the
//! Kubernetes EndpointSlice discovery that also finds the node's own entry
//! and its zone.
//!
//! Discovery is intentionally a stream translation layer: it maps Kubernetes
//! watch events into typed peer events. It does not own peer state or know
//! how callers store peers.

use std::net::SocketAddr;

use futures::Stream;

use crate::zone::{ZoneHash, ZoneName};

#[cfg(feature = "discovery-kubernetes")]
pub mod kubernetes;

pub trait PeerDiscovery {
    type Error: std::error::Error + Send + Sync + 'static;

    fn peer_events(self) -> impl Stream<Item = Result<PeerEvent, Self::Error>> + Send;
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Peer {
    pub addr: SocketAddr,
    /// The zone discovery saw for the peer (a Kubernetes EndpointSlice
    /// endpoint's `zone`), if any. The runtime routes a peer by it until one
    /// of the peer's headers names a zone, which then wins; a header naming
    /// none moves nothing.
    pub zone: Option<ZoneHash>,
}

impl Peer {
    /// A peer whose zone discovery does not know, such as a static one.
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr, zone: None }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerEvent {
    /// A peer joined, or discovery now names a zone for one already known.
    Added(Peer),
    /// A peer left; its `zone` is ignored.
    Removed(Peer),
    /// This node's own entry.
    OwnEntry(OwnEntry),
}

/// This node's own entry, the first time discovery sees it and whenever its
/// zone changes: the zone it names, if any, and the Kubernetes node it runs
/// on. The runtime adopts the zone only while it awaits one, then recomputes
/// every peer's role and asks for a join sweep.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnEntry {
    pub zone: Option<ZoneName>,
    pub node_name: Option<Box<str>>,
}

/// How a node finds its peers, and where it listens for them. Every
/// `Membership` a runtime holds comes from
/// [`crate::settings::Settings::validate`], so a node with peers always has a
/// gossip bind and its static list is never empty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Membership {
    /// Kubernetes EndpointSlice discovery: `gossip.bootstrap_peers` is empty.
    Kubernetes {
        bind: SocketAddr,
        discovery: DiscoveryConfig,
    },
    /// `gossip.bootstrap_peers`: the runtime seeds its peer table with them
    /// at construction and removes none but the row this node's own packets
    /// come from, and the Kubernetes watcher never starts.
    Static {
        bind: SocketAddr,
        peers: Box<[SocketAddr]>,
    },
    /// No gossip bind (NGINX without `gabion_gossip_bind`): no peers.
    Alone,
}

impl Membership {
    /// The `membership` log field.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Kubernetes { .. } => "kubernetes",
            Self::Static { .. } => "static",
            Self::Alone => "alone",
        }
    }

    /// `gossip.bind`, which every node with peers has; `None` for a node
    /// alone.
    pub fn bind(&self) -> Option<SocketAddr> {
        match self {
            Self::Kubernetes { bind, .. } | Self::Static { bind, .. } => Some(*bind),
            Self::Alone => None,
        }
    }

    /// The peer events both adapters feed the runtime: Kubernetes
    /// discovery's, a failed update skipped and logged as `discovery_error`
    /// at powers of two; none for a static list, whose peers the runtime
    /// seeds from the config, or for a node alone.
    #[cfg(feature = "discovery-kubernetes")]
    pub fn peer_events(&self) -> impl Stream<Item = PeerEvent> + use<> {
        use futures::StreamExt;

        let Self::Kubernetes { discovery, .. } = self else {
            return futures::stream::empty().right_stream();
        };
        let mut errors_total = 0_u64;
        kubernetes::EndpointSliceDiscovery::new(discovery.clone())
            .peer_events()
            .filter_map(move |result| {
                let event = match result {
                    Ok(event) => Some(event),
                    Err(error) => {
                        if let Some(errors_total) = crate::throttle::bump(&mut errors_total) {
                            tracing::warn!(
                                code = "discovery_error",
                                error = %error,
                                errors_total,
                                "Peer discovery hit an error and skipped an update; gabion will \
                                 keep retrying. If this keeps happening, check API access \
                                 (Kubernetes RBAC for Services and EndpointSlices) and the \
                                 discovery settings (`discovery.*` in gabiond, \
                                 `gabion_discovery_*` in NGINX).",
                            );
                        }
                        None
                    }
                };
                std::future::ready(event)
            })
            .left_stream()
    }
}

/// How Kubernetes discovery finds this node's own EndpointSlice entry,
/// chosen once at startup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelfMatch {
    /// `discovery.self_addr`: the entry whose address and gabion port equal
    /// it, for a `hostNetwork` pod, whose host name is its node's.
    Addr(SocketAddr),
    /// The entry whose `targetRef` is the Pod named like this host:
    /// Kubernetes sets a pod's host name to its name.
    PodName(Box<str>),
}

impl SelfMatch {
    /// By `self_addr` when set, else by this host's name; `None` when neither
    /// is known.
    pub(crate) fn resolve(self_addr: Option<SocketAddr>) -> Option<Self> {
        self_addr
            .map(Self::Addr)
            .or_else(|| host_name().map(Self::PodName))
    }

    /// The `self_match` log field.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Addr(_) => "self_addr",
            Self::PodName(_) => "pod_name",
        }
    }
}

/// This host's name, which in a pod is its pod name.
#[cfg(feature = "discovery-kubernetes")]
fn host_name() -> Option<Box<str>> {
    whoami::hostname().ok().map(Box::from)
}

/// Without Kubernetes discovery there is no own entry to find.
#[cfg(not(feature = "discovery-kubernetes"))]
fn host_name() -> Option<Box<str>> {
    None
}

/// Kubernetes discovery's settings, filled from the settings table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryConfig {
    /// How the own entry is matched, which is then left out of the peers;
    /// `None` when neither `discovery.self_addr` nor the host name is known.
    pub self_match: Option<SelfMatch>,
    /// Kubernetes namespaces allowed for Service discovery; empty allows all
    /// namespaces.
    pub namespace_allow: Box<[Box<str>]>,
    /// Kubernetes Service names allowed for discovery; empty allows all
    /// Services.
    pub service_allow: Box<[Box<str>]>,
}
