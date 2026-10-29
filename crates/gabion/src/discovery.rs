//! Kubernetes EndpointSlice-driven peer discovery.
//!
//! Discovery is intentionally a stream translation layer: it maps Kubernetes
//! watch events into typed peer add/remove events. It does not own peer state
//! or know how callers store peers.

use std::net::SocketAddr;

use futures::Stream;

use crate::zone::ZoneHash;

#[cfg(feature = "discovery-kubernetes")]
pub mod kubernetes;

pub trait PeerDiscovery {
    type Error: std::error::Error + Send + Sync + 'static;

    fn peer_events(self) -> impl Stream<Item = Result<PeerEvent, Self::Error>> + Send;
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Peer {
    pub addr: SocketAddr,
}

impl Peer {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerEvent {
    Added(Peer),
    Removed(Peer),
    /// This node's own entry names its zone. The runtime adopts it only
    /// while it knows no zone, then recomputes every peer's role and asks for
    /// a join sweep.
    OwnEntry {
        zone: ZoneHash,
    },
}

/// Peer discovery settings used by gossip, filled from the settings table
/// ([`crate::settings::Settings::discovery`]).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DiscoveryConfig {
    /// Local gossip address to exclude from emitted peer events.
    pub self_addr: Option<SocketAddr>,
    /// Kubernetes namespaces allowed for Service discovery; empty allows all
    /// namespaces.
    pub namespace_allow: Box<[Box<str>]>,
    /// Kubernetes Service names allowed for discovery; empty allows all
    /// Services.
    pub service_allow: Box<[Box<str>]>,
}

/// The configured discovery's peer events, as both adapters consume them: a
/// failed update is skipped and logged as `discovery_error` at powers of two.
#[cfg(feature = "discovery-kubernetes")]
pub fn peer_events(config: DiscoveryConfig) -> impl Stream<Item = PeerEvent> {
    use futures::StreamExt;

    let mut errors_total = 0_u64;
    kubernetes::EndpointSliceDiscovery::new(config)
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
                            "Peer discovery hit an error and skipped an update; gabion will keep \
                             retrying. If this keeps happening, check API access (Kubernetes RBAC \
                             for Services and EndpointSlices) and the discovery settings \
                             (`discovery.*` in gabiond, `gabion_discovery_*` in NGINX).",
                        );
                    }
                    None
                }
            };
            std::future::ready(event)
        })
}
