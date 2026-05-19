use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

extern crate self as gabion_core;
extern crate self as gabion_discovery;
extern crate self as gabion_gossip;

pub use core::{
    CardinalityError, CardinalityLimits, Decision, Descriptor, EnforcementMode, HashedLimitRequest,
    HashedLimitRequestBuilder, KeyHash, LimitRequest, OverflowPolicy, RateLimitRecorder,
    RateLimitRuntime, RejectReason, RuleId, SafetyMargin, TimedHashedLimitRequest, WindowSpec,
};
pub use discovery::DiscoveryMode;

use crate::core::{
    DescriptorMatcher, LocalEngine, NodeId, NodeIdentity, Rule, RuleTable, hash_domain,
};
use crate::discovery::{
    DEFAULT_GOSSIP_PORT_NAME, DEFAULT_MAX_PEERS, DEFAULT_RECENT_PEER_GRACE_MILLIS, FilePeerHandler,
    Peer, PeerHandler, PeerSnapshot, SnapshotPeerHandler, StaticPeerHandler,
};
use serde::Deserialize;
use thiserror::Error;

pub mod admin;
pub(crate) mod core;
pub(crate) mod discovery;
pub(crate) mod gossip;
pub mod gossip_runtime;
#[cfg(test)]
mod tests;

type SharedLimiter = Arc<Mutex<LocalEngine>>;

fn shared_limiter(engine: LocalEngine) -> SharedLimiter {
    Arc::new(Mutex::new(engine))
}

pub type Config = LocalOnlyConfig;

#[derive(Clone, Debug)]
pub struct Runtime<H = NoOpCountUpdateHandler> {
    inner: Arc<RuntimeInner<H>>,
}

#[derive(Debug)]
struct RuntimeInner<H> {
    limiter: SharedLimiter,
    limits: CardinalityLimits,
    runtime: RuntimeConfig,
    discovery: DiscoveryConfig,
    gossip: GossipConfig,
    admin_snapshot: Option<crate::gossip_runtime::SharedGossipAdminSnapshot>,
    remote_cell_capacity: usize,
    count_update_handler: H,
    shutdown: AtomicBool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CountAggregate {
    pub rule_id: RuleId,
    pub key_hash: KeyHash,
    pub bucket_start_millis: u64,
    pub count: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ApplyBatchOutcome {
    pub applied: usize,
    pub dropped: usize,
}

impl ApplyBatchOutcome {
    pub fn all_applied(count: usize) -> Self {
        Self {
            applied: count,
            dropped: 0,
        }
    }
}

pub trait CountUpdateHandler: Send + Sync + 'static {
    fn apply_batch(&self, aggregates: &[CountAggregate]) -> ApplyBatchOutcome;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NoOpCountUpdateHandler;

impl CountUpdateHandler for NoOpCountUpdateHandler {
    fn apply_batch(&self, aggregates: &[CountAggregate]) -> ApplyBatchOutcome {
        ApplyBatchOutcome {
            applied: aggregates.len(),
            dropped: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct RuntimeConfig {
    #[serde(default = "default_count_update_batch_size")]
    pub count_update_batch_size: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            count_update_batch_size: default_count_update_batch_size(),
        }
    }
}

fn default_count_update_batch_size() -> usize {
    64
}

impl Runtime<NoOpCountUpdateHandler> {
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        Self::with_count_update_handler(config, NoOpCountUpdateHandler)
    }
}

impl<H: CountUpdateHandler> Runtime<H> {
    pub fn with_count_update_handler(
        config: Config,
        count_update_handler: H,
    ) -> Result<Self, ConfigError> {
        let limits = config.cardinality_limits();
        let runtime = config.runtime;
        let discovery = config.discovery.clone();
        let gossip = config.gossip.clone();
        let bucket_count = config
            .limits
            .iter()
            .map(|limit| limit.bucket_count())
            .max()
            .unwrap_or(1);
        let remote_cell_capacity = config.storage.max_cells.unwrap_or_else(|| {
            config
                .storage
                .max_keys
                .saturating_mul(bucket_count.max(1))
                .max(1)
        });
        let limiter = shared_limiter(config.into_engine_with_identity(runtime_node_identity())?);
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                limiter,
                limits,
                runtime,
                discovery,
                admin_snapshot: gossip.enabled.then(|| {
                    Arc::new(Mutex::new(
                        crate::gossip_runtime::GossipAdminSnapshot::default(),
                    ))
                }),
                gossip,
                remote_cell_capacity,
                count_update_handler,
                shutdown: AtomicBool::new(false),
            }),
        })
    }

    pub fn record_at(&self, request: LimitRequest<'_>, now_millis: u64) -> Decision {
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Decision::Allow;
        }
        if request.validate_cardinality(self.inner.limits).is_err() {
            return Decision::Reject(RejectReason::LocalFallbackLimit);
        }
        self.inner
            .limiter
            .lock()
            .map(|mut limiter| limiter.check_and_record(request, now_millis))
            .unwrap_or(Decision::Allow)
    }

    pub fn record_hashed_at(&self, request: HashedLimitRequest, now_millis: u64) -> Decision {
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Decision::Allow;
        }
        let Ok(mut limiter) = self.inner.limiter.lock() else {
            return Decision::Allow;
        };
        let (decision, aggregate) = record_hashed_one(&mut limiter, request, now_millis);
        drop(limiter);
        if let Some(aggregate) = aggregate {
            let _ = self
                .inner
                .count_update_handler
                .apply_batch(std::slice::from_ref(&aggregate));
        }
        decision
    }

    pub fn record_hashed_batch_at(
        &self,
        requests: &[HashedLimitRequest],
        aggregate_scratch: &mut [CountAggregate],
        now_millis: u64,
    ) -> usize {
        if self.inner.shutdown.load(Ordering::Acquire)
            || requests.is_empty()
            || aggregate_scratch.is_empty()
        {
            return 0;
        }

        let batch_size = self
            .inner
            .runtime
            .count_update_batch_size
            .max(1)
            .min(aggregate_scratch.len());
        let mut recorded = 0_usize;
        let mut buffered = 0_usize;
        let Ok(mut limiter) = self.inner.limiter.lock() else {
            return 0;
        };

        for request in requests {
            let (decision, aggregate) = record_hashed_one(&mut limiter, *request, now_millis);
            if decision == Decision::Allow {
                recorded = recorded.saturating_add(1);
            }
            let Some(aggregate) = aggregate else {
                continue;
            };
            aggregate_scratch[buffered] = aggregate;
            buffered += 1;
            if buffered == batch_size {
                let batch = &aggregate_scratch[..buffered];
                self.inner.count_update_handler.apply_batch(batch);
                buffered = 0;
            }
        }
        drop(limiter);
        if buffered != 0 {
            self.inner
                .count_update_handler
                .apply_batch(&aggregate_scratch[..buffered]);
        }
        recorded
    }

    pub fn record_timed_hashed_batch(
        &self,
        requests: &[TimedHashedLimitRequest],
        aggregate_scratch: &mut [CountAggregate],
    ) -> usize {
        if self.inner.shutdown.load(Ordering::Acquire)
            || requests.is_empty()
            || aggregate_scratch.is_empty()
        {
            return 0;
        }

        let batch_size = self
            .inner
            .runtime
            .count_update_batch_size
            .max(1)
            .min(aggregate_scratch.len());
        let mut recorded = 0_usize;
        let mut buffered = 0_usize;
        let Ok(mut limiter) = self.inner.limiter.lock() else {
            return 0;
        };

        for request in requests {
            let (decision, aggregate) =
                record_hashed_one(&mut limiter, request.request(), request.now_millis());
            if decision == Decision::Allow {
                recorded = recorded.saturating_add(1);
            }
            let Some(aggregate) = aggregate else {
                continue;
            };
            aggregate_scratch[buffered] = aggregate;
            buffered += 1;
            if buffered == batch_size {
                let batch = &aggregate_scratch[..buffered];
                self.inner.count_update_handler.apply_batch(batch);
                buffered = 0;
            }
        }
        drop(limiter);
        if buffered != 0 {
            self.inner
                .count_update_handler
                .apply_batch(&aggregate_scratch[..buffered]);
        }
        recorded
    }

    pub fn record_all_at(
        &self,
        requests: &[LimitRequest<'_>],
        decisions: &mut [Decision],
        now_millis: u64,
    ) -> usize {
        let count = requests.len().min(decisions.len());
        if self.inner.shutdown.load(Ordering::Acquire) {
            decisions[..count].fill(Decision::Allow);
            return count;
        }
        if requests[..count]
            .iter()
            .any(|request| request.validate_cardinality(self.inner.limits).is_err())
        {
            decisions[..count].fill(Decision::Reject(RejectReason::LocalFallbackLimit));
            return count;
        }
        let Ok(mut limiter) = self.inner.limiter.lock() else {
            decisions[..count].fill(Decision::Allow);
            return count;
        };
        limiter.check_and_record_all_into(&requests[..count], &mut decisions[..count], now_millis)
    }

    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
    }

    pub fn gossip_enabled(&self) -> bool {
        self.inner.gossip.enabled
    }

    pub async fn run_until_shutdown(
        &self,
    ) -> Result<(), crate::gossip_runtime::GossipRuntimeError> {
        if !self.inner.gossip.enabled {
            while !self.inner.shutdown.load(Ordering::Acquire) {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            return Ok(());
        }

        let peers = if self.inner.discovery.kind == DiscoveryMode::Auto
            && kubernetes_client().await.is_ok()
        {
            RuntimePeerHandler::Snapshot(SnapshotPeerHandler::with_capacity(
                self.inner.discovery.max_peers,
            ))
        } else {
            peer_provider_from_config(&self.inner.discovery)
                .map_err(crate::gossip_runtime::GossipRuntimeError::Config)?
        };
        let discovery_task = self.start_discovery_task(&peers).await?;
        let bind = self
            .inner
            .gossip
            .bind
            .ok_or(crate::gossip_runtime::GossipRuntimeError::MissingBind)?;
        let transport = crate::gossip_runtime::UdpGossipTransport::bind(bind)?;
        let identity = self
            .inner
            .limiter
            .lock()
            .map(|limiter| limiter.identity())
            .unwrap_or_default();
        let mut runtime_config = crate::gossip_runtime::StandaloneGossipConfig::from_config(
            &self.inner.gossip,
            self.inner.remote_cell_capacity,
        );
        runtime_config.sender_node_id = crate::gossip::NodeId::from(u128::from(identity.node_id));
        runtime_config.sender_incarnation = identity.incarnation;
        let mut runtime = crate::gossip_runtime::StandaloneGossipRuntime::new_with_admin(
            Arc::clone(&self.inner.limiter),
            peers,
            transport,
            runtime_config,
            self.inner.admin_snapshot.clone(),
        );
        let wake_millis = self.inner.gossip.linger_ms.max(1).div_ceil(4).max(1);
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(wake_millis));

        while !self.inner.shutdown.load(Ordering::Acquire) {
            interval.tick().await;
            runtime.tick(current_time_millis());
        }
        if let Some(task) = discovery_task {
            task.abort();
        }
        Ok(())
    }

    async fn start_discovery_task(
        &self,
        peers: &RuntimePeerHandler,
    ) -> Result<Option<tokio::task::JoinHandle<()>>, crate::gossip_runtime::GossipRuntimeError>
    {
        match peers {
            RuntimePeerHandler::File(handler) => {
                let handler = handler.clone();
                let poll_millis = self.inner.gossip.linger_ms.max(1);
                Ok(Some(tokio::spawn(async move {
                    crate::discovery::run_file_peer_events(handler, poll_millis).await;
                })))
            }
            RuntimePeerHandler::Snapshot(handler)
                if self.inner.discovery.kind == DiscoveryMode::KubernetesEndpointSlice
                    || self.inner.discovery.kind == DiscoveryMode::Auto =>
            {
                let client = kubernetes_client().await.map_err(|error| {
                    crate::gossip_runtime::GossipRuntimeError::Discovery(error.to_string())
                })?;
                let configs = if self.inner.discovery.endpoint_slices.is_empty()
                    && self.inner.discovery.namespace.is_none()
                    && self.inner.discovery.service_name.is_none()
                {
                    crate::discovery::kubernetes::running_service_endpoint_slice_configs(
                        client.clone(),
                        self.inner.discovery.self_addr,
                    )
                    .await
                    .map_err(|error| {
                        crate::gossip_runtime::GossipRuntimeError::Discovery(format!("{error:?}"))
                    })?
                } else {
                    endpoint_slice_configs_from_discovery(&self.inner.discovery)
                        .map_err(crate::gossip_runtime::GossipRuntimeError::Config)?
                };
                let handler = handler.clone();
                Ok(Some(tokio::spawn(async move {
                    crate::discovery::kubernetes::run_endpoint_slice_watchers(
                        client, configs, handler,
                    )
                    .await;
                })))
            }
            _ => Ok(None),
        }
    }
}

fn record_hashed_one(
    limiter: &mut LocalEngine,
    request: HashedLimitRequest,
    now_millis: u64,
) -> (Decision, Option<CountAggregate>) {
    let (decision, cell) = limiter.check_and_record_hashed_with_cell(request, now_millis);
    (decision, cell.map(count_aggregate_from_cell))
}

fn count_aggregate_from_cell(cell: crate::core::CounterCell) -> CountAggregate {
    CountAggregate {
        rule_id: cell.rule_id,
        key_hash: cell.key_hash,
        bucket_start_millis: cell.bucket_start_millis,
        count: cell.count,
    }
}

fn current_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

async fn kubernetes_client() -> Result<kube::Client, kube::Error> {
    match kube::Client::try_default().await {
        Ok(client) => Ok(client),
        Err(default_error) => match kube::Config::incluster_dns() {
            Ok(config) => kube::Client::try_from(config).map_err(|_| default_error),
            Err(_) => Err(default_error),
        },
    }
}

fn runtime_node_identity() -> NodeIdentity {
    let seed = std::env::var("GABION_NODE_ID")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("POD_NAME")
                .ok()
                .filter(|value| !value.is_empty())
        })
        .or_else(|| {
            std::env::var("HOSTNAME")
                .ok()
                .filter(|value| !value.is_empty())
        });
    let Some(seed) = seed else {
        return NodeIdentity::default();
    };

    let secret = twox_hash::xxhash3_128::SecretBuffer::new(
        0,
        [0x9d; twox_hash::xxhash3_128::DEFAULT_SECRET_LENGTH],
    )
    .expect("valid XXH3 secret length");
    let mut hasher = twox_hash::xxhash3_128::RawHasher::new(secret);
    hasher.write(seed.as_bytes());
    let node_id = hasher.finish_128().max(1);
    NodeIdentity {
        node_id: NodeId::from(node_id),
        incarnation: 1,
    }
}

impl<H: CountUpdateHandler> RateLimitRecorder<LimitRequest<'_>> for Runtime<H> {
    type Decision = Decision;

    fn record_at(&self, request: LimitRequest<'_>, now_millis: u64) -> Self::Decision {
        Runtime::record_at(self, request, now_millis)
    }
}

impl<H: CountUpdateHandler> RateLimitRecorder<HashedLimitRequest> for Runtime<H> {
    type Decision = Decision;

    fn record_at(&self, request: HashedLimitRequest, now_millis: u64) -> Self::Decision {
        Runtime::record_hashed_at(self, request, now_millis)
    }
}

impl<H: CountUpdateHandler> RateLimitRecorder<TimedHashedLimitRequest> for Runtime<H> {
    type Decision = Decision;

    fn record_at(&self, request: TimedHashedLimitRequest, _now_millis: u64) -> Self::Decision {
        Runtime::record_hashed_at(self, request.request(), request.now_millis())
    }
}

impl<H: CountUpdateHandler> RateLimitRuntime<LimitRequest<'_>> for Runtime<H> {
    fn shutdown(&self) {
        Runtime::shutdown(self);
    }
}

impl<H: CountUpdateHandler> RateLimitRuntime<HashedLimitRequest> for Runtime<H> {
    fn shutdown(&self) {
        Runtime::shutdown(self);
    }
}

impl<H: CountUpdateHandler> RateLimitRuntime<TimedHashedLimitRequest> for Runtime<H> {
    fn shutdown(&self) {
        Runtime::shutdown(self);
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct LocalOnlyConfig {
    pub storage: StorageConfig,
    pub limits: Vec<LimitRuleConfig>,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub discovery: DiscoveryConfig,
    #[serde(default)]
    pub gossip: GossipConfig,
}

impl LocalOnlyConfig {
    pub fn from_yaml_str(input: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(input)
    }

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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct StorageConfig {
    pub max_keys: usize,
    pub max_cells: Option<usize>,
    pub dirty_ring_entries: Option<usize>,
    #[serde(default = "default_max_descriptor_count")]
    pub max_descriptor_count: usize,
    #[serde(default = "default_max_descriptor_bytes")]
    pub max_descriptor_bytes: usize,
    #[serde(default = "default_max_key_bytes")]
    pub max_key_bytes: usize,
    #[serde(default = "default_max_active_buckets")]
    pub max_active_buckets: usize,
}

fn default_max_descriptor_count() -> usize {
    16
}

fn default_max_descriptor_bytes() -> usize {
    512
}

fn default_max_key_bytes() -> usize {
    128
}

fn default_max_active_buckets() -> usize {
    64
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct ServerConfig {
    #[serde(default)]
    pub envoy_rls: ListenerConfig,
    #[serde(default)]
    pub admin: ListenerConfig,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct ListenerConfig {
    #[serde(default)]
    pub enabled: bool,
    pub bind: Option<SocketAddr>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct DiscoveryConfig {
    #[serde(default)]
    pub kind: DiscoveryMode,
    #[serde(default)]
    pub peers: Vec<SocketAddr>,
    pub path: Option<std::path::PathBuf>,
    pub self_addr: Option<SocketAddr>,
    #[serde(default)]
    pub endpoint_slices: Vec<EndpointSliceSelectorConfig>,
    pub namespace: Option<String>,
    pub service_name: Option<String>,
    #[serde(default = "default_gossip_port_name")]
    pub port_name: Option<String>,
    #[serde(default = "default_max_peers")]
    pub max_peers: usize,
    #[serde(default = "default_recent_peer_grace_millis")]
    pub recent_peer_grace_millis: u64,
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
            port_name: default_gossip_port_name(),
            max_peers: DEFAULT_MAX_PEERS,
            recent_peer_grace_millis: DEFAULT_RECENT_PEER_GRACE_MILLIS,
        }
    }
}

fn default_max_peers() -> usize {
    DEFAULT_MAX_PEERS
}

fn default_recent_peer_grace_millis() -> u64 {
    DEFAULT_RECENT_PEER_GRACE_MILLIS
}

fn default_gossip_port_name() -> Option<String> {
    Some(DEFAULT_GOSSIP_PORT_NAME.to_string())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct EndpointSliceSelectorConfig {
    pub namespace: String,
    pub service_name: String,
    #[serde(default = "default_gossip_port_name")]
    pub port_name: Option<String>,
}

pub use crate::DiscoveryMode as DiscoveryKind;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct GossipConfig {
    #[serde(default)]
    pub enabled: bool,
    pub bind: Option<SocketAddr>,
    #[serde(default = "default_gossip_linger_ms")]
    pub linger_ms: u64,
    #[serde(default = "default_gossip_fanout")]
    pub fanout: usize,
    #[serde(default = "default_gossip_payload_bytes")]
    pub max_payload_bytes: usize,
    #[serde(default = "default_gossip_max_cells")]
    pub max_cells_per_frame: usize,
    #[serde(default = "default_gossip_cluster_id")]
    pub cluster_id_hash: u128,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: None,
            linger_ms: default_gossip_linger_ms(),
            fanout: default_gossip_fanout(),
            max_payload_bytes: default_gossip_payload_bytes(),
            max_cells_per_frame: default_gossip_max_cells(),
            cluster_id_hash: default_gossip_cluster_id(),
        }
    }
}

fn default_gossip_linger_ms() -> u64 {
    gossip::DEFAULT_GOSSIP_LINGER_MS
}

fn default_gossip_fanout() -> usize {
    3
}

fn default_gossip_payload_bytes() -> usize {
    256 * 1024
}

fn default_gossip_max_cells() -> usize {
    4096
}

fn default_gossip_cluster_id() -> u128 {
    1
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct LimitRuleConfig {
    pub name: String,
    pub domain: String,
    pub descriptors: Vec<DescriptorConfig>,
    pub limit: u64,
    pub window: String,
    pub bucket: String,
    pub local_fallback_limit: u64,
    pub local_absolute_limit: u64,
    pub stale_after: String,
    #[serde(default)]
    pub safety_margin: SafetyMarginConfig,
    #[serde(default = "default_overflow_policy")]
    pub overflow_policy: OverflowPolicy,
    #[serde(default = "default_enforcement_mode")]
    pub mode: EnforcementMode,
}

impl LimitRuleConfig {
    fn bucket_count(&self) -> usize {
        let window = parse_duration_millis(&self.window).unwrap_or(1);
        let bucket = parse_duration_millis(&self.bucket).unwrap_or(1);
        window.div_ceil(bucket).max(1) as usize
    }

    fn to_rule(&self, id: RuleId) -> Result<Rule, ConfigError> {
        if self.descriptors.is_empty() {
            return Err(ConfigError::EmptyDescriptorSet(self.name.clone()));
        }
        let window_millis = parse_duration_millis(&self.window)
            .ok_or_else(|| ConfigError::InvalidDuration(self.window.clone()))?;
        let bucket_millis = parse_duration_millis(&self.bucket)
            .ok_or_else(|| ConfigError::InvalidDuration(self.bucket.clone()))?;
        let stale_after_millis = parse_duration_millis(&self.stale_after)
            .ok_or_else(|| ConfigError::InvalidDuration(self.stale_after.clone()))?;

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

fn default_overflow_policy() -> OverflowPolicy {
    OverflowPolicy::UseOverflowKey
}

fn default_enforcement_mode() -> EnforcementMode {
    EnforcementMode::Enforce
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct DescriptorConfig {
    pub key: String,
    #[serde(default)]
    pub value: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SafetyMarginConfig {
    #[serde(default)]
    pub hits: u64,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConfigError {
    #[error("invalid duration: {0}")]
    InvalidDuration(String),
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

#[derive(Clone, Debug)]
pub enum RuntimePeerHandler {
    Static(StaticPeerHandler),
    File(FilePeerHandler),
    Snapshot(SnapshotPeerHandler),
}

impl RuntimePeerHandler {
    pub fn file_handler(&self) -> Option<FilePeerHandler> {
        match self {
            Self::File(handler) => Some(handler.clone()),
            _ => None,
        }
    }
}

impl PeerHandler for RuntimePeerHandler {
    fn snapshot(&self) -> PeerSnapshot {
        match self {
            Self::Static(provider) => provider.snapshot(),
            Self::File(provider) => provider.snapshot(),
            Self::Snapshot(provider) => provider.snapshot(),
        }
    }

    fn peer_added(&self, peer: Peer) {
        match self {
            Self::Static(_) => {}
            Self::File(provider) => provider.peer_added(peer),
            Self::Snapshot(provider) => provider.peer_added(peer),
        }
    }

    fn peer_removed(&self, peer: Peer) {
        match self {
            Self::Static(_) => {}
            Self::File(provider) => provider.peer_removed(peer),
            Self::Snapshot(provider) => provider.peer_removed(peer),
        }
    }
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

pub fn parse_duration_millis(input: &str) -> Option<u64> {
    let input = input.trim();
    let split_at = input.find(|ch: char| !ch.is_ascii_digit())?;
    let (number, unit) = input.split_at(split_at);
    let value = number.parse::<u64>().ok()?;
    match unit.trim() {
        "ms" => Some(value),
        "s" => value.checked_mul(1_000),
        "m" => value.checked_mul(60_000),
        "h" => value.checked_mul(3_600_000),
        _ => None,
    }
}
