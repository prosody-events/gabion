//! Kubernetes EndpointSlice-driven peer discovery.
//!
//! Discovery is intentionally a stream translation layer: it maps Kubernetes
//! watch events into typed peer add/remove events. It does not own peer state
//! or know how callers store peers.

use std::net::SocketAddr;

use bon::Builder;
use futures::Stream;
use serde::{Deserialize, Serialize};

pub mod kubernetes;

pub trait PeerDiscovery {
    type Error;

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
}

/// Peer discovery settings used by gossip.
#[derive(Clone, Debug, Default, Builder, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct DiscoveryConfig {
    /// Local gossip address to exclude from emitted peer events.
    pub self_addr: Option<SocketAddr>,
    /// Kubernetes namespaces allowed for Service discovery; empty allows all
    /// namespaces.
    pub namespace_whitelist: Vec<String>,
    /// Kubernetes Service names allowed for discovery; empty allows all
    /// Services.
    pub service_whitelist: Vec<String>,
}

/// Build the configured peer discovery.
pub fn from_config(config: DiscoveryConfig) -> impl PeerDiscovery {
    kubernetes::EndpointSliceDiscovery::new(
        config.self_addr,
        config.namespace_whitelist,
        config.service_whitelist,
    )
}
