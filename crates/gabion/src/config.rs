//! Typed runtime configuration.
//!
//! This module is the only configuration surface for the core library. Adapters
//! that accept another configuration language should parse that language in the
//! adapter crate and map it into these typed structs before constructing a
//! runtime.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use bon::Builder;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::core::{
    CardinalityLimits, DescriptorMatcher, EnforcementMode, LocalEngine, NodeIdentity, OverflowPolicy,
    Rule, RuleId, RuleTable, SafetyMargin, WindowSpec, hash_domain,
};
use crate::discovery::{
    DEFAULT_GOSSIP_PORT_NAME, DEFAULT_MAX_PEERS, DEFAULT_RECENT_PEER_GRACE_MILLIS, FilePeerHandler,
    SnapshotPeerHandler, StaticPeerHandler,
};
use crate::{DiscoveryMode, RuntimePeerHandler};

const DEFAULT_GOSSIP_FANOUT: usize = 3;
const DEFAULT_GOSSIP_PAYLOAD_BYTES: usize = 256 * 1024;
const DEFAULT_GOSSIP_MAX_CELLS_PER_FRAME: usize = 4096;
const DEFAULT_GOSSIP_CLUSTER_ID_HASH: u128 = 1;

/// Complete runtime configuration for the Gabion library.
#[derive(Clone, Debug, Builder, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct RuntimeConfig {
    /// Bounded storage and request-cardinality limits.
    pub storage: StorageConfig,
    /// Rate-limit rules evaluated in declaration order.
    pub limits: Vec<LimitRuleConfig>,
    /// Local runtime batching settings.
    pub runtime: RuntimeTuningConfig,
    /// Peer discovery settings used by gossip.
    pub discovery: DiscoveryConfig,
    /// Gossip transport and propagation settings.
    pub gossip: GossipConfig,
}

impl RuntimeConfig {
    pub fn into_engine(self) -> Result<LocalEngine, ConfigError> {
        self.into_engine_with_identity(NodeIdentity::default())
    }

    pub fn into_engine_with_identity(
        self,
        identity: NodeIdentity,
    ) -> Result<LocalEngine, ConfigError> {
        let bucket_count = self
            .limits
            .iter()
            .map(|limit| limit.bucket_count())
            .max()
            .unwrap_or(1);
        if bucket_count > self.storage.max_active_buckets {
            return Err(ConfigError::TooManyBuckets {
                configured: bucket_count,
                max: self.storage.max_active_buckets,
            });
        }
        let rules = self
            .limits
            .iter()
            .enumerate()
            .map(|(index, limit)| limit.to_rule(index as RuleId + 1))
            .collect::<Result<Vec<_>, _>>()?;

        let max_cells = self.storage.max_cells.unwrap_or_else(|| {
            self.storage
                .max_keys
                .saturating_mul(bucket_count.max(1))
                .max(1)
        });
        let dirty_capacity = self.storage.dirty_ring_entries.unwrap_or(max_cells);

        Ok(LocalEngine::with_identity(
            RuleTable::new(rules),
            self.storage.max_keys,
            bucket_count,
            max_cells,
            dirty_capacity,
            identity,
        ))
    }

    pub fn cardinality_limits(&self) -> CardinalityLimits {
        CardinalityLimits {
            max_descriptor_count: self.storage.max_descriptor_count,
            max_descriptor_bytes: self.storage.max_descriptor_bytes,
            max_key_bytes: self.storage.max_key_bytes,
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            limits: Vec::new(),
            runtime: RuntimeTuningConfig::default(),
            discovery: DiscoveryConfig::default(),
            gossip: GossipConfig::default(),
        }
    }
}

/// Bounded storage and cardinality settings for local request accounting.
#[derive(Clone, Debug, Builder, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Maximum distinct request keys tracked in the local admission table.
    pub max_keys: usize,
    /// Maximum local CRDT cells retained for gossip; defaults to max_keys times bucket count.
    pub max_cells: Option<usize>,
    /// Maximum dirty-cell ring entries retained between gossip sends; defaults to max_cells.
    pub dirty_ring_entries: Option<usize>,
    /// Maximum descriptor entries accepted per request.
    pub max_descriptor_count: usize,
    /// Maximum aggregate domain, key, and value bytes accepted per request.
    pub max_descriptor_bytes: usize,
    /// Maximum bytes accepted for one descriptor key.
    pub max_key_bytes: usize,
    /// Maximum bucket count permitted for any configured rule.
    pub max_active_buckets: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_keys: 1024,
            max_cells: None,
            dirty_ring_entries: None,
            max_descriptor_count: 16,
            max_descriptor_bytes: 512,
            max_key_bytes: 128,
            max_active_buckets: 64,
        }
    }
}

/// Local runtime tuning independent of rule behavior.
#[derive(Clone, Copy, Debug, Builder, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct RuntimeTuningConfig {
    /// Maximum count updates passed to a count-update handler in one callback.
    pub count_update_batch_size: usize,
}

impl Default for RuntimeTuningConfig {
    fn default() -> Self {
        Self {
            count_update_batch_size: 64,
        }
    }
}

/// Peer discovery settings used by gossip.
#[derive(Clone, Debug, Builder, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct DiscoveryConfig {
    /// Discovery backend used to produce gossip peers.
    pub kind: DiscoveryMode,
    /// Static seed peers; also used as initial peers for file discovery.
    pub peers: Vec<SocketAddr>,
    /// Peer-file path for file discovery.
    pub path: Option<PathBuf>,
    /// Local gossip address to exclude from peer snapshots.
    pub self_addr: Option<SocketAddr>,
    /// Explicit Kubernetes EndpointSlice selectors.
    pub endpoint_slices: Vec<EndpointSliceSelectorConfig>,
    /// Shorthand Kubernetes namespace used when endpoint_slices is empty.
    pub namespace: Option<String>,
    /// Shorthand Kubernetes service name used when endpoint_slices is empty.
    pub service_name: Option<String>,
    /// Shorthand Kubernetes service port name; defaults to "gossip".
    pub port_name: Option<String>,
    /// Maximum peers retained by bounded discovery handlers.
    pub max_peers: usize,
    /// Duration removed peers remain authorized for in-flight gossip frames.
    #[serde(with = "humantime_serde")]
    pub recent_peer_grace: Duration,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            kind: DiscoveryMode::default(),
            peers: Vec::new(),
            path: None,
            self_addr: None,
            endpoint_slices: Vec::new(),
            namespace: None,
            service_name: None,
            port_name: Some(DEFAULT_GOSSIP_PORT_NAME.to_string()),
            max_peers: DEFAULT_MAX_PEERS,
            recent_peer_grace: Duration::from_millis(DEFAULT_RECENT_PEER_GRACE_MILLIS),
        }
    }
}

/// Kubernetes EndpointSlice selector used for gossip peer discovery.
#[derive(Clone, Debug, Builder, Deserialize, Eq, PartialEq, Serialize)]
pub struct EndpointSliceSelectorConfig {
    /// Kubernetes namespace containing the service.
    pub namespace: String,
    /// Kubernetes service name whose EndpointSlices advertise peers.
    pub service_name: String,
    /// Service port name to read from each endpoint; defaults to "gossip" when absent.
    #[serde(default)]
    pub port_name: Option<String>,
}

/// Gossip transport and propagation settings.
#[derive(Clone, Debug, Builder, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct GossipConfig {
    /// Whether the runtime should bind UDP gossip and exchange counter cells.
    pub enabled: bool,
    /// UDP socket address used by the gossip transport when enabled.
    pub bind: Option<SocketAddr>,
    /// Minimum duration between ordinary gossip sends.
    #[serde(with = "humantime_serde")]
    pub linger: Duration,
    /// Maximum peers sent to on each gossip tick.
    pub fanout: usize,
    /// Maximum encoded gossip frame size in bytes.
    pub max_payload_bytes: usize,
    /// Maximum counter cells included in one gossip frame.
    pub max_cells_per_frame: usize,
    /// Cluster discriminator; frames from a different cluster id are ignored.
    pub cluster_id_hash: u128,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: None,
            linger: Duration::from_millis(crate::gossip::DEFAULT_GOSSIP_LINGER_MS),
            fanout: DEFAULT_GOSSIP_FANOUT,
            max_payload_bytes: DEFAULT_GOSSIP_PAYLOAD_BYTES,
            max_cells_per_frame: DEFAULT_GOSSIP_MAX_CELLS_PER_FRAME,
            cluster_id_hash: DEFAULT_GOSSIP_CLUSTER_ID_HASH,
        }
    }
}

/// One rate-limit rule.
#[derive(Clone, Debug, Builder, Deserialize, Eq, PartialEq, Serialize)]
pub struct LimitRuleConfig {
    /// Human-readable rule name.
    pub name: String,
    /// Request domain this rule applies to.
    pub domain: String,
    /// Ordered descriptor pattern matched against incoming requests.
    pub descriptors: Vec<DescriptorConfig>,
    /// Global count limit for a fresh distributed estimate.
    pub limit: u64,
    /// Sliding-window size.
    #[serde(with = "humantime_serde")]
    pub window: Duration,
    /// Bucket granularity within the sliding window.
    #[serde(with = "humantime_serde")]
    pub bucket: Duration,
    /// Local cap used while distributed estimates are stale.
    pub local_fallback_limit: u64,
    /// Absolute local cap enforced even when distributed estimates are fresh.
    pub local_absolute_limit: u64,
    /// Maximum age of a distributed estimate before local fallback applies.
    #[serde(with = "humantime_serde")]
    pub stale_after: Duration,
    /// Count reserved below the global limit to absorb gossip lag.
    #[serde(default)]
    pub safety_margin: SafetyMarginConfig,
    /// Behavior when local key storage is exhausted.
    #[serde(default)]
    pub overflow_policy: OverflowPolicy,
    /// Whether this rule enforces or is ignored.
    #[serde(default)]
    pub mode: EnforcementMode,
}

impl LimitRuleConfig {
    pub(crate) fn bucket_count(&self) -> usize {
        duration_millis(self.window)
            .div_ceil(duration_millis(self.bucket).max(1))
            .max(1) as usize
    }

    pub(crate) fn to_rule(&self, id: RuleId) -> Result<Rule, ConfigError> {
        if self.descriptors.is_empty() {
            return Err(ConfigError::EmptyDescriptorSet(self.name.clone()));
        }
        let window_millis = duration_millis(self.window);
        let bucket_millis = duration_millis(self.bucket).max(1);
        let stale_after_millis = duration_millis(self.stale_after);

        Ok(Rule {
            id,
            domain_hash: hash_domain(&self.domain),
            descriptor_matcher: DescriptorMatcher::exact(
                self.descriptors
                    .iter()
                    .map(|descriptor| (descriptor.key.as_str(), descriptor.value.as_str())),
            ),
            limit: self.limit,
            window: WindowSpec {
                size_millis: window_millis,
                bucket_count: window_millis.div_ceil(bucket_millis).max(1) as usize,
            },
            local_fallback_limit: self.local_fallback_limit,
            local_absolute_limit: self.local_absolute_limit,
            stale_after_millis,
            safety_margin: SafetyMargin {
                hits: self.safety_margin.hits,
            },
            overflow_policy: self.overflow_policy,
            mode: self.mode,
        })
    }
}

/// One descriptor key/value pattern in a rule.
#[derive(Clone, Debug, Builder, Deserialize, Eq, PartialEq, Serialize)]
pub struct DescriptorConfig {
    /// Descriptor key name.
    pub key: String,
    /// Descriptor value to match; "*" matches any value.
    #[serde(default)]
    pub value: String,
}

/// Safety margin applied to fresh distributed estimates.
#[derive(Clone, Copy, Debug, Default, Builder, Deserialize, Eq, PartialEq, Serialize)]
pub struct SafetyMarginConfig {
    /// Hits reserved below the configured global limit.
    pub hits: u64,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConfigError {
    #[error("gossip.enabled requires gossip.bind")]
    MissingGossipBind,
    #[error("discovery.kind static requires at least one peer")]
    MissingStaticPeers,
    #[error("discovery.kind file requires discovery.path")]
    MissingPeerFile,
    #[error("discovery.kind kubernetes requires discovery.namespace")]
    MissingKubernetesNamespace,
    #[error("discovery.kind kubernetes requires discovery.service_name")]
    MissingKubernetesServiceName,
    #[error("discovery.kind kubernetes requires at least one EndpointSlice selector")]
    MissingKubernetesEndpointSliceSelector,
    #[error("rule {0} has no descriptors")]
    EmptyDescriptorSet(String),
    #[error("configured bucket count {configured} exceeds max_active_buckets {max}")]
    TooManyBuckets { configured: usize, max: usize },
}

pub fn peer_provider_from_config(
    discovery: &DiscoveryConfig,
) -> Result<RuntimePeerHandler, ConfigError> {
    match discovery.kind {
        DiscoveryMode::Auto => Ok(RuntimePeerHandler::Static(StaticPeerHandler::new(
            Vec::new(),
            discovery.self_addr,
        ))),
        DiscoveryMode::None => Ok(RuntimePeerHandler::Static(StaticPeerHandler::new(
            Vec::new(),
            discovery.self_addr,
        ))),
        DiscoveryMode::Static => {
            if discovery.peers.is_empty() {
                return Err(ConfigError::MissingStaticPeers);
            }
            Ok(RuntimePeerHandler::Static(StaticPeerHandler::new(
                discovery.peers.clone(),
                discovery.self_addr,
            )))
        }
        DiscoveryMode::File => {
            let Some(path) = &discovery.path else {
                return Err(ConfigError::MissingPeerFile);
            };
            Ok(RuntimePeerHandler::File(FilePeerHandler::with_capacity(
                path,
                discovery.self_addr,
                discovery.peers.clone(),
                discovery.max_peers,
            )))
        }
        DiscoveryMode::KubernetesEndpointSlice => Ok(RuntimePeerHandler::Snapshot(
            SnapshotPeerHandler::with_capacity(discovery.max_peers),
        )),
    }
}

pub fn endpoint_slice_config_from_discovery(
    discovery: &DiscoveryConfig,
) -> Result<crate::discovery::kubernetes::EndpointSliceDiscoveryConfig, ConfigError> {
    endpoint_slice_configs_from_discovery(discovery)?
        .into_iter()
        .next()
        .ok_or(ConfigError::MissingKubernetesEndpointSliceSelector)
}

pub fn endpoint_slice_configs_from_discovery(
    discovery: &DiscoveryConfig,
) -> Result<Vec<crate::discovery::kubernetes::EndpointSliceDiscoveryConfig>, ConfigError> {
    if !discovery.endpoint_slices.is_empty() {
        return Ok(discovery
            .endpoint_slices
            .iter()
            .map(
                |selector| crate::discovery::kubernetes::EndpointSliceDiscoveryConfig {
                    namespace: selector.namespace.clone(),
                    service_name: selector.service_name.clone(),
                    port_name: selector.port_name.clone(),
                    self_addr: discovery.self_addr,
                },
            )
            .collect());
    }

    let Some(namespace) = &discovery.namespace else {
        return Err(ConfigError::MissingKubernetesNamespace);
    };
    let Some(service_name) = &discovery.service_name else {
        return Err(ConfigError::MissingKubernetesServiceName);
    };

    Ok(vec![
        crate::discovery::kubernetes::EndpointSliceDiscoveryConfig {
            namespace: namespace.clone(),
            service_name: service_name.clone(),
            port_name: discovery.port_name.clone(),
            self_addr: discovery.self_addr,
        },
    ])
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX).max(1)
}
