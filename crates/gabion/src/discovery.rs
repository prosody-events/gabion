//! Kubernetes EndpointSlice-driven peer discovery.
//!
//! Discovery is intentionally a stream translation layer: it maps Kubernetes
//! watch events into typed peer add/remove events. It does not own peer state
//! or know how callers store peers.

use std::net::SocketAddr;
use std::time::Duration;

use bon::Builder;
use futures::Stream;
use serde::{Deserialize, Serialize};

pub mod kubernetes;

pub const DEFAULT_MAX_PEERS: usize = 128;
pub const DEFAULT_RECENT_PEER_GRACE_MILLIS: u64 = 30_000;
pub const DEFAULT_GABION_SERVICE_NAME: &str = "gabion";

pub trait PeerDiscovery {
    type Error;

    fn peer_events(self) -> impl Stream<Item = Result<PeerEvent, Self::Error>> + Send;
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
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

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryMode {
    /// Discover peers from Kubernetes EndpointSlice watch events.
    #[default]
    #[serde(rename = "kubernetes")]
    KubernetesEndpointSlice,
}

/// Peer discovery settings used by gossip.
#[derive(Clone, Debug, Builder, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct DiscoveryConfig {
    /// Local gossip address to exclude from emitted peer events.
    pub self_addr: Option<SocketAddr>,
    /// Kubernetes namespaces allowed for Service discovery; empty allows all namespaces.
    pub namespace_whitelist: Vec<String>,
    /// Kubernetes Service names allowed for discovery; empty allows all Services.
    pub service_whitelist: Vec<String>,
    /// Maximum peers the caller should retain.
    pub max_peers: usize,
    /// Duration removed peers remain authorized by the caller for in-flight frames.
    #[serde(with = "humantime_serde")]
    pub recent_peer_grace: Duration,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            self_addr: None,
            namespace_whitelist: Vec::new(),
            service_whitelist: Vec::new(),
            max_peers: DEFAULT_MAX_PEERS,
            recent_peer_grace: Duration::from_millis(DEFAULT_RECENT_PEER_GRACE_MILLIS),
        }
    }
}
