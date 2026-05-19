use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

extern crate self as gabion_core;
extern crate self as gabion_discovery;
extern crate self as gabion_gossip;

pub(crate) mod core;
pub(crate) mod discovery;
pub(crate) mod gossip;

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

pub mod gossip_runtime {
    //! Standalone gossip runtime.
    //!
    //! Invariants:
    //! - Peer discovery is injected through `PeerHandler`.
    //! - Message communication is injected through `GossipTransport`; UDP is
    //!   optional.
    //! - Unknown senders are rejected before decode and merge.
    //! - Recently removed peers remain accepted only through the configured
    //!   grace window.
    //! - Dirty overflow forces a bounded resync rather than dropping
    //!   convergence forever.
    //! - Send and receive buffers are allocated at construction and reused per
    //!   tick.
    //! - Ticks are deterministic for a supplied timestamp.

    use std::net::{SocketAddr, UdpSocket};
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::SharedLimiter;
    use crate::discovery::{Peer, PeerHandler, PeerSnapshot};
    use crate::gossip::{
        CellTable, DecodeError, GossipHeader, GossipLimits, GossipMetrics, GossipSendPolicy,
        GossipSendReason, GossipSpaceUsage, HmacKey, ShardDigest,
        decode_authenticated_message_visit_checked, decode_message_visit_checked,
        encode_authenticated_message_parts, encode_message_parts,
    };
    use thiserror::Error;

    use crate::{GossipConfig, RuntimePeerHandler};

    #[derive(Clone, Debug, Default, serde::Serialize)]
    pub struct GossipAdminSnapshot {
        pub cluster_id_hash: u128,
        pub sender_node_id: crate::gossip::NodeId,
        pub sender_incarnation: u64,
        pub active_peers: Vec<SocketAddr>,
        pub recent_peers: Vec<RecentPeerSnapshot>,
        pub discovery_generation: u64,
        pub local_only: bool,
        pub discovery_stale: bool,
        pub remote_active_cells: usize,
        pub remote_cell_capacity: usize,
        pub remote_dirty_ring_len: usize,
        pub remote_dirty_overflow: bool,
        pub remote_cells_sample: Vec<crate::gossip::CounterCell>,
        pub metrics: GossipMetrics,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
    pub struct RecentPeerSnapshot {
        pub addr: SocketAddr,
        pub expires_millis: u64,
    }

    pub type SharedGossipAdminSnapshot = Arc<Mutex<GossipAdminSnapshot>>;

    pub trait GossipTransport {
        fn send_to(&mut self, peer: Peer, payload: &[u8]) -> bool;
        fn recv_into(&mut self, buffer: &mut [u8]) -> Option<(Peer, usize)>;
    }

    #[derive(Debug)]
    pub struct UdpGossipTransport {
        socket: UdpSocket,
    }

    impl UdpGossipTransport {
        pub fn bind(bind: SocketAddr) -> Result<Self, GossipRuntimeError> {
            let socket = UdpSocket::bind(bind).map_err(GossipRuntimeError::Bind)?;
            socket
                .set_nonblocking(true)
                .map_err(GossipRuntimeError::Configure)?;
            Ok(Self { socket })
        }

        pub fn local_addr(&self) -> Result<SocketAddr, GossipRuntimeError> {
            self.socket
                .local_addr()
                .map_err(GossipRuntimeError::LocalAddr)
        }
    }

    impl GossipTransport for UdpGossipTransport {
        fn send_to(&mut self, peer: Peer, payload: &[u8]) -> bool {
            matches!(self.socket.send_to(payload, peer.addr), Ok(sent) if sent == payload.len())
        }

        fn recv_into(&mut self, buffer: &mut [u8]) -> Option<(Peer, usize)> {
            match self.socket.recv_from(buffer) {
                Ok((len, addr)) => Some((Peer::new(addr), len)),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => None,
                Err(_) => None,
            }
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct StandaloneGossipConfig {
        pub cluster_id_hash: u128,
        pub sender_node_id: crate::gossip::NodeId,
        pub sender_incarnation: u64,
        pub fanout: usize,
        pub max_payload_bytes: usize,
        pub max_cells_per_frame: usize,
        pub remote_cell_capacity: usize,
        pub remote_dirty_capacity: usize,
        pub auth_key: Option<HmacKey>,
        pub max_peers: usize,
        pub recent_peer_grace_millis: u64,
        pub send_policy: GossipSendPolicy,
    }

    impl StandaloneGossipConfig {
        pub fn from_config(config: &GossipConfig, remote_cell_capacity: usize) -> Self {
            Self {
                cluster_id_hash: config.cluster_id_hash,
                sender_node_id: crate::gossip::NodeId::from(1_u128),
                sender_incarnation: 1,
                fanout: config.fanout.max(1),
                max_payload_bytes: config
                    .max_payload_bytes
                    .max(crate::gossip::GOSSIP_HEADER_LEN),
                max_cells_per_frame: config.max_cells_per_frame.max(1),
                remote_cell_capacity,
                remote_dirty_capacity: remote_cell_capacity,
                auth_key: None,
                max_peers: 128,
                recent_peer_grace_millis: 30_000,
                send_policy: GossipSendPolicy::with_linger_ms(config.linger_ms),
            }
        }
    }

    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub struct GossipTickSummary {
        pub peers_seen: usize,
        pub peers_sent: usize,
        pub send_failures: usize,
        pub cells_sent: usize,
        pub frames_received: usize,
        pub cells_merged: usize,
        pub peer_rejected: usize,
        pub local_only: bool,
        pub discovery_stale: bool,
        pub send_reason: Option<GossipSendReason>,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct RecentPeer {
        peer: Peer,
        expires_millis: u64,
    }

    #[derive(Clone, Debug)]
    struct PeerAuthorizer {
        current: Vec<Peer>,
        recent: Vec<RecentPeer>,
        grace_millis: u64,
    }

    impl PeerAuthorizer {
        fn with_capacity(max_peers: usize, grace_millis: u64) -> Self {
            Self {
                current: Vec::with_capacity(max_peers),
                recent: Vec::with_capacity(max_peers),
                grace_millis,
            }
        }

        fn update(&mut self, now_millis: u64, peers: &[Peer]) {
            self.expire(now_millis);

            for index in 0..self.current.len() {
                let peer = self.current[index];
                if !peers.contains(&peer) {
                    self.insert_recent(peer, now_millis.saturating_add(self.grace_millis));
                }
            }

            self.current.clear();
            for peer in peers.iter().copied().take(self.current.capacity()) {
                if !self.current.contains(&peer) {
                    self.current.push(peer);
                }
            }
        }

        fn accepts(&mut self, peer: Peer, now_millis: u64) -> bool {
            self.expire(now_millis);
            self.current.contains(&peer) || self.recent.iter().any(|entry| entry.peer == peer)
        }

        fn expire(&mut self, now_millis: u64) {
            self.recent
                .retain(|entry| entry.expires_millis > now_millis);
        }

        fn insert_recent(&mut self, peer: Peer, expires_millis: u64) {
            if self.recent.iter().any(|entry| entry.peer == peer) {
                return;
            }
            if self.recent.len() == self.recent.capacity() && !self.recent.is_empty() {
                self.recent.swap_remove(0);
            }
            if self.recent.len() < self.recent.capacity() {
                self.recent.push(RecentPeer {
                    peer,
                    expires_millis,
                });
            }
        }

        fn recent_snapshot(&self) -> Vec<RecentPeerSnapshot> {
            self.recent
                .iter()
                .map(|entry| RecentPeerSnapshot {
                    addr: entry.peer.addr,
                    expires_millis: entry.expires_millis,
                })
                .collect()
        }
    }

    pub struct StandaloneGossipRuntime<T: GossipTransport, P: PeerHandler = RuntimePeerHandler> {
        limiter: SharedLimiter,
        peers: P,
        transport: T,
        config: StandaloneGossipConfig,
        remote_cells: CellTable,
        send_buffer: Vec<u8>,
        recv_buffer: Vec<u8>,
        cell_buffer: Vec<crate::gossip::CounterCell>,
        digest_buffer: Vec<ShardDigest>,
        peer_authorizer: PeerAuthorizer,
        force_resync: bool,
        last_send_millis: u64,
        metrics: GossipMetrics,
        admin_snapshot: Option<SharedGossipAdminSnapshot>,
    }

    impl<T: GossipTransport, P: PeerHandler> StandaloneGossipRuntime<T, P> {
        pub fn new(
            limiter: SharedLimiter,
            peers: P,
            transport: T,
            config: StandaloneGossipConfig,
        ) -> Self {
            Self::new_with_admin(limiter, peers, transport, config, None)
        }

        pub fn new_with_admin(
            limiter: SharedLimiter,
            peers: P,
            transport: T,
            config: StandaloneGossipConfig,
            admin_snapshot: Option<SharedGossipAdminSnapshot>,
        ) -> Self {
            Self {
                limiter,
                peers,
                transport,
                config,
                remote_cells: CellTable::with_capacity(
                    config.remote_cell_capacity,
                    config.remote_dirty_capacity,
                ),
                send_buffer: Vec::with_capacity(config.max_payload_bytes),
                recv_buffer: vec![0; config.max_payload_bytes],
                cell_buffer: Vec::with_capacity(config.max_cells_per_frame),
                digest_buffer: Vec::with_capacity(1),
                peer_authorizer: PeerAuthorizer::with_capacity(
                    config.max_peers,
                    config.recent_peer_grace_millis,
                ),
                force_resync: false,
                last_send_millis: 0,
                metrics: GossipMetrics::default(),
                admin_snapshot,
            }
        }

        pub fn tick(&mut self, now_millis: u64) -> GossipTickSummary {
            let snapshot = self.peers.snapshot();
            self.peer_authorizer.update(now_millis, snapshot.peers());
            let mut summary = GossipTickSummary {
                peers_seen: snapshot.peers().len(),
                local_only: snapshot.local_only(),
                discovery_stale: snapshot.stale(),
                ..GossipTickSummary::default()
            };

            if !snapshot.local_only() {
                let send_reason = if self.force_resync {
                    Some(GossipSendReason::DirtyOverflow)
                } else {
                    self.local_usage().and_then(|usage| {
                        self.config.send_policy.should_send(
                            now_millis,
                            self.last_send_millis,
                            usage,
                        )
                    })
                };
                summary.send_reason = send_reason;

                if send_reason.is_some() {
                    self.collect_dirty_cells();
                    let header = GossipHeader {
                        cluster_id_hash: self.config.cluster_id_hash,
                        sender_node_id: self.config.sender_node_id,
                        sender_incarnation: self.config.sender_incarnation,
                        min_bucket: 0,
                        max_bucket: 0,
                        flags: 0,
                    };
                    let truncated = if let Some(key) = self.config.auth_key {
                        encode_authenticated_message_parts(
                            header,
                            self.digest_buffer.as_slice(),
                            self.cell_buffer.as_slice(),
                            false,
                            key,
                            &mut self.send_buffer,
                            GossipLimits {
                                max_payload_bytes: self.config.max_payload_bytes,
                                max_digests: 64,
                                max_cells: self.config.max_cells_per_frame,
                            },
                        )
                    } else {
                        encode_message_parts(
                            header,
                            self.digest_buffer.as_slice(),
                            self.cell_buffer.as_slice(),
                            false,
                            &mut self.send_buffer,
                            self.config.max_payload_bytes,
                        )
                    };
                    self.metrics.record_send(self.send_buffer.len(), truncated);
                    summary.cells_sent = self.cell_buffer.len();

                    for peer in snapshot.peers().iter().take(self.config.fanout) {
                        if self.transport.send_to(*peer, self.send_buffer.as_slice()) {
                            summary.peers_sent = summary.peers_sent.saturating_add(1);
                        } else {
                            summary.send_failures = summary.send_failures.saturating_add(1);
                        }
                    }
                    self.last_send_millis = now_millis;
                    tracing::debug!(
                        ?send_reason,
                        cells = summary.cells_sent,
                        peers = summary.peers_sent,
                        failures = summary.send_failures,
                        bytes = self.send_buffer.len(),
                        truncated,
                        "gossip frame sent"
                    );
                }
            }

            while let Some((peer, len)) = self.transport.recv_into(self.recv_buffer.as_mut_slice())
            {
                if !self.peer_authorizer.accepts(peer, now_millis) {
                    summary.peer_rejected = summary.peer_rejected.saturating_add(1);
                    continue;
                }
                self.metrics.record_recv(len);
                summary.frames_received = summary.frames_received.saturating_add(1);
                summary.cells_merged = summary
                    .cells_merged
                    .saturating_add(self.merge_frame(now_millis, len));
            }

            self.publish_admin_snapshot(&snapshot);
            summary
        }

        pub fn metrics(&self) -> GossipMetrics {
            self.metrics
        }

        pub fn limiter(&self) -> &SharedLimiter {
            &self.limiter
        }

        pub fn transport_mut(&mut self) -> &mut T {
            &mut self.transport
        }

        pub fn admin_snapshot(&self) -> GossipAdminSnapshot {
            let snapshot = self.peers.snapshot();
            self.build_admin_snapshot(&snapshot)
        }

        #[cfg(test)]
        pub(crate) fn buffer_capacities(&self) -> RuntimeBufferCapacities {
            RuntimeBufferCapacities {
                send: self.send_buffer.capacity(),
                recv: self.recv_buffer.capacity(),
                cells: self.cell_buffer.capacity(),
                digests: self.digest_buffer.capacity(),
            }
        }

        fn publish_admin_snapshot(&self, snapshot: &PeerSnapshot) {
            let Some(shared) = &self.admin_snapshot else {
                return;
            };
            if let Ok(mut admin_snapshot) = shared.lock() {
                *admin_snapshot = self.build_admin_snapshot(snapshot);
            }
        }

        fn build_admin_snapshot(&self, snapshot: &PeerSnapshot) -> GossipAdminSnapshot {
            GossipAdminSnapshot {
                cluster_id_hash: self.config.cluster_id_hash,
                sender_node_id: self.config.sender_node_id,
                sender_incarnation: self.config.sender_incarnation,
                active_peers: snapshot.peers().iter().map(|peer| peer.addr).collect(),
                recent_peers: self.peer_authorizer.recent_snapshot(),
                discovery_generation: snapshot.generation(),
                local_only: snapshot.local_only(),
                discovery_stale: snapshot.stale(),
                remote_active_cells: self.remote_cells.active_cell_count(),
                remote_cell_capacity: self.remote_cells.capacity(),
                remote_dirty_ring_len: self.remote_cells.dirty_len(),
                remote_dirty_overflow: self.remote_cells.dirty_overflowed(),
                remote_cells_sample: self
                    .remote_cells
                    .cells()
                    .take(self.config.max_cells_per_frame)
                    .map(|(_, cell)| cell)
                    .collect(),
                metrics: self.metrics,
            }
        }

        fn local_usage(&self) -> Option<GossipSpaceUsage> {
            let Ok(limiter) = self.limiter.lock() else {
                return None;
            };
            let summary = limiter.storage_summary();
            Some(GossipSpaceUsage {
                active_cells: summary.active_cells,
                max_cells: summary.max_cells,
                dirty_cells: summary.dirty_ring_len,
                dirty_capacity: summary.dirty_ring_capacity,
                dirty_overflowed: summary.dirty_overflow,
            })
        }

        fn collect_dirty_cells(&mut self) {
            self.cell_buffer.clear();
            self.digest_buffer.clear();
            let Ok(limiter) = self.limiter.lock() else {
                return;
            };

            self.digest_buffer.push(crate::gossip::digest_cells(
                limiter.cells().map(convert_core_cell),
                0,
                1,
            ));

            if limiter.dirty_overflowed() || self.force_resync {
                self.metrics.dirty_overflow = self.metrics.dirty_overflow.saturating_add(1);
                for cell in limiter.cells().take(self.config.max_cells_per_frame) {
                    self.cell_buffer.push(convert_core_cell(cell));
                }
                self.force_resync = false;
            } else {
                for cell in limiter.dirty_cells().take(self.config.max_cells_per_frame) {
                    self.cell_buffer.push(convert_core_cell(cell));
                }
            }
        }

        fn merge_frame(&mut self, now_millis: u64, len: usize) -> usize {
            let limits = GossipLimits {
                max_payload_bytes: self.config.max_payload_bytes,
                max_digests: 64,
                max_cells: self.config.max_cells_per_frame,
            };
            let mut merged = 0_usize;
            let mut decode_error = None;
            let accept_header = |header: GossipHeader| {
                header.cluster_id_hash == self.config.cluster_id_hash
                    && header.sender_node_id != self.config.sender_node_id
            };
            let mut received_digest = None;
            let on_digest = |digest: ShardDigest| {
                received_digest = Some(digest);
            };
            let mut on_cell = |cell| {
                let Ok(mut limiter) = self.limiter.lock() else {
                    return;
                };
                match self
                    .remote_cells
                    .merge_remote(cell, Some(&mut limiter), now_millis)
                {
                    Ok(outcome) => {
                        if outcome.changed {
                            merged = merged.saturating_add(1);
                            self.metrics.merge_cells = self.metrics.merge_cells.saturating_add(1);
                        }
                    }
                    Err(_) => {
                        decode_error = Some(DecodeError::CapacityExceeded);
                    }
                }
            };

            let result = if let Some(key) = self.config.auth_key {
                decode_authenticated_message_visit_checked(
                    &self.recv_buffer[..len],
                    key,
                    limits,
                    accept_header,
                    on_digest,
                    &mut on_cell,
                )
            } else {
                decode_message_visit_checked(
                    &self.recv_buffer[..len],
                    limits,
                    accept_header,
                    on_digest,
                    &mut on_cell,
                )
            };
            if matches!(result, Err(DecodeError::AuthenticationFailed)) {
                self.metrics.auth_failures = self.metrics.auth_failures.saturating_add(1);
            }
            if result.is_err() || decode_error.is_some() {
                self.metrics.decode_errors = self.metrics.decode_errors.saturating_add(1);
            }
            if let Some(digest) = received_digest
                && self.remote_cells.digest(digest.shard_id, 1) != digest
            {
                self.metrics.digest_mismatch = self.metrics.digest_mismatch.saturating_add(1);
                self.force_resync = true;
            }
            merged
        }
    }

    #[cfg(test)]
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) struct RuntimeBufferCapacities {
        pub send: usize,
        pub recv: usize,
        pub cells: usize,
        pub digests: usize,
    }

    pub async fn run_udp_runtime(
        limiter: SharedLimiter,
        peers: RuntimePeerHandler,
        config: GossipConfig,
        remote_cell_capacity: usize,
    ) -> Result<(), GossipRuntimeError> {
        run_udp_runtime_with_admin(limiter, peers, config, remote_cell_capacity, None).await
    }

    pub async fn run_udp_runtime_with_admin(
        limiter: SharedLimiter,
        peers: RuntimePeerHandler,
        config: GossipConfig,
        remote_cell_capacity: usize,
        admin_snapshot: Option<SharedGossipAdminSnapshot>,
    ) -> Result<(), GossipRuntimeError> {
        let bind = config.bind.ok_or(GossipRuntimeError::MissingBind)?;
        let transport = UdpGossipTransport::bind(bind)?;
        let runtime_config = StandaloneGossipConfig::from_config(&config, remote_cell_capacity);
        let runtime = StandaloneGossipRuntime::new_with_admin(
            limiter,
            peers,
            transport,
            runtime_config,
            admin_snapshot,
        );
        run_runtime(runtime, config.linger_ms).await
    }

    pub async fn run_runtime<T: GossipTransport, P: PeerHandler>(
        mut runtime: StandaloneGossipRuntime<T, P>,
        linger_ms: u64,
    ) -> Result<(), GossipRuntimeError> {
        let wake_millis = linger_ms.max(1).div_ceil(4).max(1);
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(wake_millis));

        loop {
            interval.tick().await;
            runtime.tick(now_millis());
        }
    }

    fn convert_core_cell(cell: crate::core::CounterCell) -> crate::gossip::CounterCell {
        crate::gossip::CounterCell {
            rule_id: cell.rule_id,
            key_hash: cell.key_hash,
            bucket_start_millis: cell.bucket_start_millis.min(i64::MAX as u64) as i64,
            origin_node_id: u128::from(cell.origin_node_id).into(),
            origin_incarnation: cell.origin_incarnation,
            count: cell.count,
            last_update_millis: cell.last_update_millis,
            sequence: cell.sequence,
        }
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    #[derive(Debug, Error)]
    pub enum GossipRuntimeError {
        #[error("gossip bind address is required")]
        MissingBind,
        #[error("invalid gossip runtime config: {0}")]
        Config(#[from] crate::ConfigError),
        #[error("gossip discovery failed: {0}")]
        Discovery(String),
        #[error("failed to bind gossip socket: {0}")]
        Bind(std::io::Error),
        #[error("failed to configure gossip socket: {0}")]
        Configure(std::io::Error),
        #[error("failed to read gossip socket address: {0}")]
        LocalAddr(std::io::Error),
    }
}

pub mod admin {
    use crate::RuleId;
    use crate::SharedLimiter;
    use crate::core::{CounterCell, Metrics, NodeIdentity, Rule, StorageSummary};
    use axum::extract::{Query, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use axum::{Json, Router};
    use serde::{Deserialize, Serialize};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::gossip_runtime::GossipAdminSnapshot;
    use crate::gossip_runtime::SharedGossipAdminSnapshot;

    const DEFAULT_DEBUG_LIMIT: usize = 64;

    #[derive(Clone)]
    pub struct AdminState {
        limiter: SharedLimiter,
        gossip: Option<SharedGossipAdminSnapshot>,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct AdminSnapshot {
        pub mode: &'static str,
        pub identity: NodeIdentity,
        pub storage: StorageSummary,
        pub metrics: Metrics,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct RulesSnapshot {
        pub rules: Vec<Rule>,
        pub truncated: bool,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct PeersSnapshot {
        pub active_peers: Vec<SocketAddr>,
        pub recent_peers: Vec<crate::gossip_runtime::RecentPeerSnapshot>,
        pub discovery_generation: u64,
        pub local_only: bool,
        pub discovery_stale: bool,
        pub truncated: bool,
    }

    #[derive(Clone, Debug, Serialize)]
    pub struct IntrospectionSnapshot {
        pub mode: &'static str,
        pub identity: NodeIdentity,
        pub cluster_id_hash: u128,
        pub active_rule_ids: Vec<RuleId>,
        pub storage: StorageSummary,
        pub local_cells: Vec<CounterCell>,
        pub remote_cells: Vec<crate::gossip::CounterCell>,
        pub peers: PeersSnapshot,
        pub gossip: Option<GossipAdminSnapshotSummary>,
        pub metrics: Metrics,
        pub truncated: bool,
    }

    #[derive(Clone, Copy, Debug, Serialize)]
    pub struct GossipAdminSnapshotSummary {
        pub send_bytes: u64,
        pub recv_bytes: u64,
        pub merge_cells: u64,
        pub digest_mismatch: u64,
        pub truncated_frames: u64,
        pub auth_failures: u64,
        pub decode_errors: u64,
        pub dirty_overflow: u64,
        pub remote_active_cells: usize,
        pub remote_cell_capacity: usize,
        pub remote_dirty_ring_len: usize,
        pub remote_dirty_overflow: bool,
    }

    #[derive(Clone, Copy, Debug, Deserialize)]
    pub struct DebugLimits {
        pub max_rules: Option<usize>,
        pub max_cells: Option<usize>,
        pub max_peers: Option<usize>,
    }

    impl DebugLimits {
        fn max_rules(self) -> usize {
            self.max_rules.unwrap_or(DEFAULT_DEBUG_LIMIT)
        }

        fn max_cells(self) -> usize {
            self.max_cells.unwrap_or(DEFAULT_DEBUG_LIMIT)
        }

        fn max_peers(self) -> usize {
            self.max_peers.unwrap_or(DEFAULT_DEBUG_LIMIT)
        }
    }

    impl AdminState {
        pub fn new(limiter: SharedLimiter, gossip: Option<SharedGossipAdminSnapshot>) -> Self {
            Self { limiter, gossip }
        }

        fn gossip_snapshot(&self) -> Option<GossipAdminSnapshot> {
            self.gossip
                .as_ref()
                .and_then(|snapshot| snapshot.lock().ok().map(|snapshot| snapshot.clone()))
        }
    }

    pub fn snapshot(limiter: &SharedLimiter) -> AdminSnapshot {
        match limiter.lock() {
            Ok(limiter) => AdminSnapshot {
                mode: "local_only",
                identity: limiter.identity(),
                storage: limiter.storage_summary(),
                metrics: limiter.metrics(),
            },
            Err(_) => AdminSnapshot {
                mode: "local_only",
                identity: NodeIdentity::default(),
                storage: StorageSummary::default(),
                metrics: Metrics::default(),
            },
        }
    }

    pub fn rules_snapshot(limiter: &SharedLimiter, limits: DebugLimits) -> RulesSnapshot {
        match limiter.lock() {
            Ok(limiter) => {
                let max_rules = limits.max_rules();
                let rules = limiter
                    .rules()
                    .iter()
                    .take(max_rules)
                    .cloned()
                    .collect::<Vec<_>>();
                RulesSnapshot {
                    truncated: limiter.rules().len() > rules.len(),
                    rules,
                }
            }
            Err(_) => RulesSnapshot {
                rules: Vec::new(),
                truncated: false,
            },
        }
    }

    pub fn peers_snapshot(
        gossip: Option<GossipAdminSnapshot>,
        limits: DebugLimits,
    ) -> PeersSnapshot {
        match gossip {
            Some(gossip) => {
                let max_peers = limits.max_peers();
                let active_peers = gossip
                    .active_peers
                    .iter()
                    .copied()
                    .take(max_peers)
                    .collect::<Vec<_>>();
                let recent_peers = gossip
                    .recent_peers
                    .iter()
                    .copied()
                    .take(max_peers)
                    .collect::<Vec<_>>();
                PeersSnapshot {
                    truncated: gossip.active_peers.len() > active_peers.len()
                        || gossip.recent_peers.len() > recent_peers.len(),
                    active_peers,
                    recent_peers,
                    discovery_generation: gossip.discovery_generation,
                    local_only: gossip.local_only,
                    discovery_stale: gossip.discovery_stale,
                }
            }
            None => PeersSnapshot {
                active_peers: Vec::new(),
                recent_peers: Vec::new(),
                discovery_generation: 0,
                local_only: true,
                discovery_stale: false,
                truncated: false,
            },
        }
    }

    pub fn introspection_snapshot(
        state: &AdminState,
        limits: DebugLimits,
    ) -> IntrospectionSnapshot {
        let gossip = state.gossip_snapshot();
        let peers = peers_snapshot(gossip.clone(), limits);
        let mut truncated = peers.truncated;
        let max_rules = limits.max_rules();
        let max_cells = limits.max_cells();

        let (identity, storage, metrics, active_rule_ids, local_cells) = match state.limiter.lock()
        {
            Ok(limiter) => {
                let active_rule_ids = limiter
                    .rules()
                    .iter()
                    .take(max_rules)
                    .map(|rule| rule.id)
                    .collect::<Vec<_>>();
                truncated |= limiter.rules().len() > active_rule_ids.len();
                let local_cells = limiter.cells().take(max_cells).collect::<Vec<_>>();
                truncated |= limiter.active_cells() > local_cells.len();
                (
                    limiter.identity(),
                    limiter.storage_summary(),
                    limiter.metrics(),
                    active_rule_ids,
                    local_cells,
                )
            }
            Err(_) => (
                NodeIdentity::default(),
                StorageSummary::default(),
                Metrics::default(),
                Vec::new(),
                Vec::new(),
            ),
        };

        let (cluster_id_hash, remote_cells, gossip_summary) = match gossip {
            Some(gossip) => {
                let remote_cells = gossip
                    .remote_cells_sample
                    .iter()
                    .copied()
                    .take(max_cells)
                    .collect::<Vec<_>>();
                truncated |= gossip.remote_active_cells > remote_cells.len();
                (
                    gossip.cluster_id_hash,
                    remote_cells,
                    Some(GossipAdminSnapshotSummary {
                        send_bytes: gossip.metrics.send_bytes,
                        recv_bytes: gossip.metrics.recv_bytes,
                        merge_cells: gossip.metrics.merge_cells,
                        digest_mismatch: gossip.metrics.digest_mismatch,
                        truncated_frames: gossip.metrics.truncated,
                        auth_failures: gossip.metrics.auth_failures,
                        decode_errors: gossip.metrics.decode_errors,
                        dirty_overflow: gossip.metrics.dirty_overflow,
                        remote_active_cells: gossip.remote_active_cells,
                        remote_cell_capacity: gossip.remote_cell_capacity,
                        remote_dirty_ring_len: gossip.remote_dirty_ring_len,
                        remote_dirty_overflow: gossip.remote_dirty_overflow,
                    }),
                )
            }
            None => (0, Vec::new(), None),
        };

        IntrospectionSnapshot {
            mode: "local_only",
            identity,
            cluster_id_hash,
            active_rule_ids,
            storage,
            local_cells,
            remote_cells,
            peers,
            gossip: gossip_summary,
            metrics,
            truncated,
        }
    }

    pub fn prometheus_metrics(state: &AdminState) -> String {
        let snapshot = snapshot(&state.limiter);
        let metrics = snapshot.metrics;
        let gossip = state.gossip_snapshot();
        let (local_only, discovery_stale, peers, gossip_metrics) = match &gossip {
            Some(gossip) => (
                gossip.local_only,
                gossip.discovery_stale,
                gossip.active_peers.len(),
                gossip.metrics,
            ),
            None => (true, false, 0, crate::gossip::GossipMetrics::default()),
        };
        format!(
            concat!(
                "limiter_mode{{mode=\"local_only\"}} 1\n",
                "limiter_local_only {}\n",
                "limiter_discovery_stale {}\n",
                "limiter_peers {}\n",
                "limiter_requests_total {}\n",
                "limiter_allowed_total {}\n",
                "limiter_rejected_total {}\n",
                "limiter_rejected_total{{reason=\"local_absolute\"}} {}\n",
                "limiter_rejected_total{{reason=\"global_estimate\"}} {}\n",
                "limiter_rejected_total{{reason=\"local_fallback\"}} {}\n",
                "limiter_overflow_key_total {}\n",
                "limiter_overflow_rejected_total {}\n",
                "limiter_active_keys {}\n",
                "limiter_active_cells {}\n",
                "limiter_dirty_ring_len {}\n",
                "limiter_dirty_overflow {}\n",
                "gossip_send_bytes_total {}\n",
                "gossip_recv_bytes_total {}\n",
                "gossip_merge_cells_total {}\n",
                "gossip_digest_mismatch_total {}\n",
                "gossip_auth_failures_total {}\n",
                "gossip_decode_errors_total {}\n"
            ),
            u8::from(local_only),
            u8::from(discovery_stale),
            peers,
            metrics.requests,
            metrics.allowed,
            metrics.rejected,
            metrics.local_absolute_rejected,
            metrics.global_estimate_rejected,
            metrics.local_fallback_rejected,
            metrics.overflow_key_uses,
            metrics.overflow_rejected,
            snapshot.storage.active_keys,
            snapshot.storage.active_cells,
            snapshot.storage.dirty_ring_len,
            u8::from(snapshot.storage.dirty_overflow),
            gossip_metrics.send_bytes,
            gossip_metrics.recv_bytes,
            gossip_metrics.merge_cells,
            gossip_metrics.digest_mismatch,
            gossip_metrics.auth_failures,
            gossip_metrics.decode_errors,
        )
    }

    pub fn router(limiter: SharedLimiter) -> Router {
        router_with_gossip(limiter, None)
    }

    pub fn router_for_runtime<H: crate::CountUpdateHandler>(runtime: crate::Runtime<H>) -> Router {
        router_with_gossip(
            Arc::clone(&runtime.inner.limiter),
            runtime.inner.admin_snapshot.clone(),
        )
    }

    pub fn router_with_gossip(
        limiter: SharedLimiter,
        gossip: Option<SharedGossipAdminSnapshot>,
    ) -> Router {
        let admin_state = Arc::new(AdminState::new(limiter, gossip));
        Router::new()
            .route("/healthz", get(healthz))
            .route("/readyz", get(readyz))
            .route("/metrics", get(metrics))
            .route("/debug/rules", get(debug_rules))
            .route("/debug/peers", get(debug_peers))
            .route("/debug/storage", get(debug_storage))
            .route("/debug/introspection", get(debug_introspection))
            .route("/state", get(state_endpoint))
            .with_state(admin_state)
    }

    pub async fn serve(bind: SocketAddr, limiter: SharedLimiter) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(bind).await?;
        axum::serve(listener, router(limiter)).await
    }

    pub async fn serve_for_runtime<H: crate::CountUpdateHandler>(
        bind: SocketAddr,
        runtime: crate::Runtime<H>,
    ) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(bind).await?;
        axum::serve(listener, router_for_runtime(runtime)).await
    }

    pub async fn serve_with_gossip(
        bind: SocketAddr,
        limiter: SharedLimiter,
        gossip: Option<SharedGossipAdminSnapshot>,
    ) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(bind).await?;
        axum::serve(listener, router_with_gossip(limiter, gossip)).await
    }

    async fn healthz() -> &'static str {
        "ok\n"
    }

    async fn readyz(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
        if state.limiter.lock().is_ok() {
            (StatusCode::OK, "ready\n")
        } else {
            (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
        }
    }

    async fn metrics(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
        prometheus_metrics(&state)
    }

    async fn debug_rules(
        State(state): State<Arc<AdminState>>,
        Query(limits): Query<DebugLimits>,
    ) -> impl IntoResponse {
        Json(rules_snapshot(&state.limiter, limits))
    }

    async fn debug_peers(
        State(state): State<Arc<AdminState>>,
        Query(limits): Query<DebugLimits>,
    ) -> impl IntoResponse {
        Json(peers_snapshot(state.gossip_snapshot(), limits))
    }

    async fn debug_storage(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
        Json(snapshot(&state.limiter).storage)
    }

    async fn debug_introspection(
        State(state): State<Arc<AdminState>>,
        Query(limits): Query<DebugLimits>,
    ) -> impl IntoResponse {
        Json(introspection_snapshot(&state, limits))
    }

    async fn state_endpoint(
        State(state): State<Arc<AdminState>>,
        Query(limits): Query<DebugLimits>,
    ) -> impl IntoResponse {
        Json(introspection_snapshot(&state, limits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{LocalEngine, NodeIdentity, Rule, RuleTable};
    use crate::discovery::{
        DEFAULT_GOSSIP_PORT_NAME, FilePeerHandler, Peer, PeerSnapshot, SnapshotPeerHandler,
        StaticPeerHandler,
    };
    use crate::gossip::GossipSendPolicy;
    use crate::gossip_runtime::{
        GossipTickSummary, GossipTransport, StandaloneGossipConfig, StandaloneGossipRuntime,
        UdpGossipTransport,
    };
    use crate::{Decision, Descriptor, LimitRequest, RejectReason};
    use quickcheck::{Arbitrary, Gen, TestResult};
    use quickcheck_macros::quickcheck;
    use std::collections::VecDeque;
    use std::net::SocketAddr;

    #[derive(Clone, Debug)]
    struct RuntimeRecentPeerCase {
        grace_millis: u8,
        elapsed_millis: u8,
    }

    impl Arbitrary for RuntimeRecentPeerCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                grace_millis: (u8::arbitrary(g) % 32).saturating_add(1),
                elapsed_millis: u8::arbitrary(g) % 64,
            }
        }
    }

    #[derive(Clone, Debug)]
    struct EndpointConfigCase {
        selector_count: u8,
        include_legacy_selector: bool,
    }

    impl Arbitrary for EndpointConfigCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                selector_count: u8::arbitrary(g) % 8,
                include_legacy_selector: bool::arbitrary(g),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct RuntimeUnknownPeerCase {
        known_octet: u8,
        unknown_octet: u8,
        sender: u8,
    }

    impl Arbitrary for RuntimeUnknownPeerCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                known_octet: (u8::arbitrary(g) % 64).saturating_add(1),
                unknown_octet: (u8::arbitrary(g) % 64).saturating_add(65),
                sender: (u8::arbitrary(g) % 64).saturating_add(2),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct RuntimeDirtyResyncCase {
        tenant_count: u8,
        max_cells_per_frame: u8,
    }

    impl Arbitrary for RuntimeDirtyResyncCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                tenant_count: (u8::arbitrary(g) % 8).saturating_add(2),
                max_cells_per_frame: (u8::arbitrary(g) % 8).saturating_add(1),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct RuntimeDeterministicTickCase {
        peer_count: u8,
        fanout: u8,
        now_millis: u16,
    }

    impl Arbitrary for RuntimeDeterministicTickCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                peer_count: u8::arbitrary(g) % 8,
                fanout: (u8::arbitrary(g) % 8).saturating_add(1),
                now_millis: u16::arbitrary(g),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct RuntimeBufferReuseCase {
        peer_count: u8,
        tick_count: u8,
    }

    impl Arbitrary for RuntimeBufferReuseCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                peer_count: (u8::arbitrary(g) % 8).saturating_add(1),
                tick_count: (u8::arbitrary(g) % 16).saturating_add(1),
            }
        }
    }

    #[derive(Default)]
    struct MemoryTransport {
        inbox: VecDeque<(Peer, Vec<u8>)>,
        outbox: VecDeque<(Peer, Vec<u8>)>,
    }

    impl MemoryTransport {
        fn push_inbox(&mut self, peer: Peer, payload: Vec<u8>) {
            self.inbox.push_back((peer, payload));
        }

        fn drain_outbox(&mut self) -> Vec<(Peer, Vec<u8>)> {
            self.outbox.drain(..).collect()
        }
    }

    impl GossipTransport for MemoryTransport {
        fn send_to(&mut self, peer: Peer, payload: &[u8]) -> bool {
            self.outbox.push_back((peer, payload.to_vec()));
            true
        }

        fn recv_into(&mut self, buffer: &mut [u8]) -> Option<(Peer, usize)> {
            let (peer, payload) = self.inbox.pop_front()?;
            if payload.len() > buffer.len() {
                return None;
            }
            buffer[..payload.len()].copy_from_slice(&payload);
            Some((peer, payload.len()))
        }
    }

    fn test_config() -> LocalOnlyConfig {
        LocalOnlyConfig::from_yaml_str(
            r#"
storage:
  max_keys: 16
  max_cells: 64
  dirty_ring_entries: 64
discovery:
  kind: none
gossip:
  enabled: false
limits:
  - name: tenant_api_minute
    domain: api
    descriptors:
      - key: tenant
        value: "*"
    limit: 1
    window: 60s
    bucket: 1s
    local_fallback_limit: 100
    local_absolute_limit: 100
    stale_after: 2s
    overflow_policy: aggregate
    mode: enforce
"#,
        )
        .expect("config parses")
    }

    #[derive(Clone, Debug, Default)]
    struct RecordingCountHandler {
        batch_sizes: Arc<Mutex<Vec<usize>>>,
    }

    impl CountUpdateHandler for RecordingCountHandler {
        fn apply_batch(&self, aggregates: &[CountAggregate]) -> ApplyBatchOutcome {
            self.batch_sizes
                .lock()
                .expect("batch sizes")
                .push(aggregates.len());
            ApplyBatchOutcome {
                applied: aggregates.len(),
                dropped: 0,
            }
        }
    }

    #[test]
    fn runtime_publishes_count_aggregates_in_configured_batches() {
        let mut config = test_config();
        config.runtime.count_update_batch_size = 2;
        let handler = RecordingCountHandler::default();
        let seen = Arc::clone(&handler.batch_sizes);
        let runtime =
            Runtime::with_count_update_handler(config, handler).expect("runtime with handler");
        let requests = [
            TimedHashedLimitRequest::new(HashedLimitRequest::new(1, 1_u128, 1), 0),
            TimedHashedLimitRequest::new(HashedLimitRequest::new(1, 2_u128, 1), 0),
            TimedHashedLimitRequest::new(HashedLimitRequest::new(1, 3_u128, 1), 0),
        ];
        let mut scratch = [CountAggregate::default(); 3];

        let recorded = runtime.record_timed_hashed_batch(&requests, &mut scratch);

        assert_eq!(recorded, 3);
        assert_eq!(*seen.lock().expect("batch sizes"), vec![2, 1]);
    }

    #[test]
    fn runtime_does_not_publish_count_aggregates_after_shutdown() {
        let mut config = test_config();
        config.runtime.count_update_batch_size = 1;
        let handler = RecordingCountHandler::default();
        let seen = Arc::clone(&handler.batch_sizes);
        let runtime =
            Runtime::with_count_update_handler(config, handler).expect("runtime with handler");
        runtime.shutdown();
        let requests = [TimedHashedLimitRequest::new(
            HashedLimitRequest::new(1, 1_u128, 1),
            0,
        )];
        let mut scratch = [CountAggregate::default(); 1];

        let recorded = runtime.record_timed_hashed_batch(&requests, &mut scratch);

        assert_eq!(recorded, 0);
        assert!(seen.lock().expect("batch sizes").is_empty());
    }

    fn engine_with_node(node: u64) -> LocalEngine {
        let config = test_config();
        let bucket_count = config
            .limits
            .iter()
            .map(|limit| limit.bucket_count())
            .max()
            .unwrap_or(1);
        let rules = config
            .limits
            .iter()
            .enumerate()
            .map(|(index, limit)| limit.to_rule(index as RuleId + 1))
            .collect::<Result<Vec<_>, _>>()
            .expect("rules");

        LocalEngine::with_identity(
            RuleTable::new(rules),
            config.storage.max_keys,
            bucket_count,
            64,
            64,
            NodeIdentity {
                node_id: u128::from(node).into(),
                incarnation: 1,
            },
        )
    }

    fn rule_config_for_runtime(id: RuleId) -> Rule {
        test_config().limits[0].to_rule(id).expect("rule")
    }

    fn runtime_config(node: u64) -> StandaloneGossipConfig {
        StandaloneGossipConfig {
            cluster_id_hash: 42,
            sender_node_id: u128::from(node).into(),
            sender_incarnation: 1,
            fanout: 3,
            max_payload_bytes: 4096,
            max_cells_per_frame: 16,
            remote_cell_capacity: 64,
            remote_dirty_capacity: 64,
            auth_key: None,
            max_peers: 16,
            recent_peer_grace_millis: 30_000,
            send_policy: GossipSendPolicy::with_linger_ms(1),
        }
    }

    fn empty_gossip_packet(sender: u64) -> Vec<u8> {
        let message = crate::gossip::GossipMessage {
            header: crate::gossip::GossipHeader {
                cluster_id_hash: 42,
                sender_node_id: u128::from(sender).into(),
                sender_incarnation: 1,
                min_bucket: 0,
                max_bucket: 0,
                flags: 0,
            },
            digests: Vec::new(),
            cells: Vec::new(),
            truncated: false,
        };
        let mut packet = Vec::with_capacity(128);
        assert!(!crate::gossip::encode_message(&message, &mut packet, 128));
        packet
    }

    fn peer_addr(octet: u8) -> SocketAddr {
        format!("127.0.0.{octet}:18080").parse().expect("addr")
    }

    fn runtime(node: u64, peer_addr: SocketAddr) -> StandaloneGossipRuntime<MemoryTransport> {
        let limiter = shared_limiter(engine_with_node(node));
        let peers = RuntimePeerHandler::Static(StaticPeerHandler::new(vec![peer_addr], None));
        let config = runtime_config(node);
        StandaloneGossipRuntime::new(limiter, peers, MemoryTransport::default(), config)
    }

    fn request<'a>(descriptors: &'a [Descriptor<'a>]) -> LimitRequest<'a> {
        LimitRequest {
            domain: "api",
            descriptors,
            hits: 1,
        }
    }

    fn record_tenant(limiter: &SharedLimiter, tenant: &str, now_millis: u64) {
        let descriptors = [Descriptor {
            key: "tenant",
            value: tenant,
        }];
        let mut limiter = limiter.lock().expect("limiter");
        assert_eq!(
            limiter.check_and_record(request(&descriptors), now_millis),
            Decision::Allow
        );
    }

    #[test]
    fn admin_introspection_is_bounded_by_query_limits() {
        let limiter = shared_limiter(engine_with_node(1));
        record_tenant(&limiter, "a", 0);
        record_tenant(&limiter, "b", 1);

        let gossip =
            std::sync::Arc::new(std::sync::Mutex::new(gossip_runtime::GossipAdminSnapshot {
                cluster_id_hash: 42,
                sender_node_id: 1_u128.into(),
                sender_incarnation: 1,
                active_peers: vec![
                    "127.0.0.2:9000".parse().expect("addr"),
                    "127.0.0.3:9000".parse().expect("addr"),
                ],
                recent_peers: vec![gossip_runtime::RecentPeerSnapshot {
                    addr: "127.0.0.4:9000".parse().expect("addr"),
                    expires_millis: 30_000,
                }],
                discovery_generation: 7,
                local_only: false,
                discovery_stale: true,
                remote_active_cells: 2,
                remote_cell_capacity: 8,
                remote_dirty_ring_len: 1,
                remote_dirty_overflow: false,
                remote_cells_sample: vec![
                    crate::gossip::CounterCell {
                        rule_id: 1,
                        key_hash: 2_u128.into(),
                        bucket_start_millis: 0,
                        origin_node_id: 2_u128.into(),
                        origin_incarnation: 1,
                        count: 1,
                        last_update_millis: 1,
                        sequence: 1,
                    },
                    crate::gossip::CounterCell {
                        rule_id: 1,
                        key_hash: 4_u128.into(),
                        bucket_start_millis: 0,
                        origin_node_id: 3_u128.into(),
                        origin_incarnation: 1,
                        count: 1,
                        last_update_millis: 1,
                        sequence: 1,
                    },
                ],
                metrics: crate::gossip::GossipMetrics {
                    send_bytes: 10,
                    recv_bytes: 20,
                    merge_cells: 2,
                    digest_mismatch: 1,
                    truncated: 0,
                    auth_failures: 0,
                    decode_errors: 0,
                    dirty_overflow: 0,
                },
            }));
        let state = admin::AdminState::new(limiter, Some(gossip));
        let snapshot = admin::introspection_snapshot(
            &state,
            admin::DebugLimits {
                max_rules: Some(1),
                max_cells: Some(1),
                max_peers: Some(1),
            },
        );

        assert_eq!(snapshot.cluster_id_hash, 42);
        assert_eq!(snapshot.active_rule_ids, vec![1]);
        assert_eq!(snapshot.local_cells.len(), 1);
        assert_eq!(snapshot.remote_cells.len(), 1);
        assert_eq!(snapshot.peers.active_peers.len(), 1);
        assert_eq!(snapshot.peers.recent_peers.len(), 1);
        assert!(snapshot.peers.discovery_stale);
        assert!(snapshot.truncated);
        assert_eq!(snapshot.gossip.expect("gossip").merge_cells, 2);
    }

    #[test]
    fn admin_prometheus_metrics_include_discovery_and_gossip_state() {
        let limiter = shared_limiter(engine_with_node(1));
        record_tenant(&limiter, "a", 0);
        let gossip =
            std::sync::Arc::new(std::sync::Mutex::new(gossip_runtime::GossipAdminSnapshot {
                local_only: false,
                discovery_stale: true,
                active_peers: vec!["127.0.0.2:9000".parse().expect("addr")],
                metrics: crate::gossip::GossipMetrics {
                    send_bytes: 11,
                    recv_bytes: 22,
                    merge_cells: 3,
                    ..Default::default()
                },
                ..Default::default()
            }));
        let state = admin::AdminState::new(limiter, Some(gossip));
        let metrics = admin::prometheus_metrics(&state);

        assert!(metrics.contains("limiter_local_only 0\n"));
        assert!(metrics.contains("limiter_discovery_stale 1\n"));
        assert!(metrics.contains("limiter_peers 1\n"));
        assert!(metrics.contains("gossip_send_bytes_total 11\n"));
        assert!(metrics.contains("gossip_recv_bytes_total 22\n"));
        assert!(metrics.contains("gossip_merge_cells_total 3\n"));
    }

    #[test]
    fn admin_storage_summary_reports_cells_and_dirty_ring() {
        let limiter = shared_limiter(engine_with_node(1));
        record_tenant(&limiter, "a", 0);

        let storage = admin::snapshot(&limiter).storage;

        assert_eq!(storage.active_keys, 1);
        assert_eq!(storage.active_cells, 1);
        assert_eq!(storage.max_keys, 16);
        assert_eq!(storage.max_cells, 64);
        assert_eq!(storage.dirty_ring_len, 1);
        assert!(!storage.dirty_overflow);
        assert!(storage.estimated_memory_bytes > 0);
    }

    #[test]
    fn parses_local_only_config_into_engine() {
        let config = LocalOnlyConfig::from_yaml_str(
            r#"
storage:
  max_keys: 16
discovery:
  kind: none
gossip:
  enabled: false
limits:
  - name: tenant_api_minute
    domain: api
    descriptors:
      - key: tenant
        value: "*"
    limit: 10
    window: 60s
    bucket: 1s
    local_fallback_limit: 3
    local_absolute_limit: 6
    stale_after: 2s
    overflow_policy: aggregate
    mode: enforce
"#,
        )
        .expect("config parses");

        let engine = config.into_engine().expect("local-only engine builds");

        assert_eq!(engine.rules().len(), 1);
        assert_eq!(engine.active_keys(), 0);
    }

    #[test]
    fn parses_static_peer_config_without_boxed_provider() {
        let config = LocalOnlyConfig::from_yaml_str(
            r#"
storage:
  max_keys: 16
discovery:
  kind: static
  self_addr: 127.0.0.1:18080
  peers:
    - 127.0.0.1:18080
    - 127.0.0.2:18080
gossip:
  enabled: true
  bind: 127.0.0.1:18080
limits:
  - name: tenant_api_minute
    domain: api
    descriptors:
      - key: tenant
        value: "*"
    limit: 10
    window: 60s
    bucket: 1s
    local_fallback_limit: 3
    local_absolute_limit: 6
    stale_after: 2s
"#,
        )
        .expect("config parses");
        let provider = peer_provider_from_config(&config.discovery).expect("provider");

        assert_eq!(provider.snapshot().peers().len(), 1);
    }

    #[test]
    fn discovery_defaults_to_auto_and_sync_provider_is_local_only() {
        let config = LocalOnlyConfig::from_yaml_str(
            r#"
storage:
  max_keys: 16
gossip:
  enabled: true
  bind: 0.0.0.0:18080
limits:
  - name: tenant_api_minute
    domain: api
    descriptors:
      - key: tenant
        value: "*"
    limit: 10
    window: 60s
    bucket: 1s
    local_fallback_limit: 3
    local_absolute_limit: 6
    stale_after: 2s
"#,
        )
        .expect("config parses");
        let provider = peer_provider_from_config(&config.discovery).expect("provider");

        assert_eq!(config.discovery.kind, DiscoveryKind::Auto);
        assert!(provider.snapshot().local_only());
    }

    #[test]
    fn discovery_section_without_kind_defaults_to_auto() {
        let config = LocalOnlyConfig::from_yaml_str(
            r#"
storage:
  max_keys: 16
discovery:
  self_addr: 10.0.0.1:18080
gossip:
  enabled: true
  bind: 0.0.0.0:18080
limits:
  - name: tenant_api_minute
    domain: api
    descriptors:
      - key: tenant
        value: "*"
    limit: 10
    window: 60s
    bucket: 1s
    local_fallback_limit: 3
    local_absolute_limit: 6
    stale_after: 2s
"#,
        )
        .expect("config parses");

        assert_eq!(config.discovery.kind, DiscoveryKind::Auto);
        assert_eq!(
            config.discovery.self_addr,
            Some("10.0.0.1:18080".parse().expect("addr"))
        );
    }

    #[test]
    fn gossip_tick_sends_dirty_cells_and_receiver_merges_estimate() {
        let addr_a = "127.0.0.1:10001".parse().expect("addr");
        let addr_b = "127.0.0.1:10002".parse().expect("addr");
        let mut runtime_a = runtime(1, addr_b);
        let mut runtime_b = runtime(2, addr_a);
        let descriptors = [Descriptor {
            key: "tenant",
            value: "a",
        }];

        {
            let mut limiter = runtime_a.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 0),
                Decision::Allow
            );
        }

        let sent = runtime_a.tick(1);
        for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
            runtime_b
                .transport_mut()
                .push_inbox(Peer::new(addr_a), payload);
        }
        let received = runtime_b.tick(2);

        assert_eq!(sent.cells_sent, 1);
        assert_eq!(received.frames_received, 1);
        assert_eq!(received.cells_merged, 1);
        assert_eq!(runtime_b.metrics().merge_cells, 1);
        {
            let mut limiter = runtime_b.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 3),
                Decision::Reject(RejectReason::GlobalLimit)
            );
        }
    }

    #[test]
    fn deterministic_gossip_uses_peer_and_transport_traits_without_udp() {
        let addr_a = "127.0.0.1:10101".parse().expect("addr");
        let addr_b = "127.0.0.1:10102".parse().expect("addr");
        let peers_a = SnapshotPeerHandler::with_capacity(4);
        let peers_b = SnapshotPeerHandler::with_capacity(4);
        peers_a.peer_added(Peer::new(addr_b));
        peers_b.peer_added(Peer::new(addr_a));
        let config_a = StandaloneGossipConfig {
            cluster_id_hash: 42,
            sender_node_id: 1_u128.into(),
            sender_incarnation: 1,
            fanout: 1,
            max_payload_bytes: 4096,
            max_cells_per_frame: 16,
            remote_cell_capacity: 64,
            remote_dirty_capacity: 64,
            auth_key: None,
            max_peers: 16,
            recent_peer_grace_millis: 30_000,
            send_policy: GossipSendPolicy::with_linger_ms(1),
        };
        let mut config_b = config_a;
        config_b.sender_node_id = 2_u128.into();
        let mut runtime_a = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(1)),
            peers_a.clone(),
            MemoryTransport::default(),
            config_a,
        );
        let mut runtime_b = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(2)),
            peers_b,
            MemoryTransport::default(),
            config_b,
        );
        let descriptors = [Descriptor {
            key: "tenant",
            value: "trait",
        }];

        {
            let mut limiter = runtime_a.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 0),
                Decision::Allow
            );
        }
        assert_eq!(runtime_a.tick(1).peers_sent, 1);
        for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
            runtime_b
                .transport_mut()
                .push_inbox(Peer::new(addr_a), payload);
        }
        assert_eq!(runtime_b.tick(2).cells_merged, 1);

        peers_a.peer_removed(Peer::new(addr_b));
        {
            let mut limiter = runtime_a.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 3),
                Decision::Allow
            );
        }
        let summary = runtime_a.tick(4);
        assert_eq!(summary.peers_seen, 0);
        assert_eq!(summary.peers_sent, 0);
    }

    #[test]
    fn gossip_tick_keeps_last_good_file_peers_when_read_fails() {
        let missing =
            std::env::temp_dir().join(format!("gabion-missing-peers-{}", std::process::id()));
        let provider = RuntimePeerHandler::File(FilePeerHandler::new(
            &missing,
            None,
            vec!["127.0.0.2:18080".parse().expect("addr")],
        ));
        let limiter = shared_limiter(engine_with_node(1));
        let config = StandaloneGossipConfig {
            cluster_id_hash: 42,
            sender_node_id: 1_u128.into(),
            sender_incarnation: 1,
            fanout: 1,
            max_payload_bytes: 4096,
            max_cells_per_frame: 16,
            remote_cell_capacity: 64,
            remote_dirty_capacity: 64,
            auth_key: None,
            max_peers: 16,
            recent_peer_grace_millis: 30_000,
            send_policy: GossipSendPolicy::with_linger_ms(1),
        };
        let mut runtime =
            StandaloneGossipRuntime::new(limiter, provider, MemoryTransport::default(), config);

        let summary: GossipTickSummary = runtime.tick(0);

        assert!(!summary.discovery_stale);
        assert!(!summary.local_only);
        assert_eq!(summary.peers_seen, 1);
    }

    #[test]
    fn kubernetes_endpoint_slice_config_uses_snapshot_provider() {
        let config = LocalOnlyConfig::from_yaml_str(
            r#"
storage:
  max_keys: 16
discovery:
  kind: kubernetes
  namespace: default
  service_name: gabion
  port_name: gossip
  self_addr: 10.0.0.1:18080
gossip:
  enabled: true
  bind: 0.0.0.0:18080
limits:
  - name: tenant_api_minute
    domain: api
    descriptors:
      - key: tenant
        value: "*"
    limit: 10
    window: 60s
    bucket: 1s
    local_fallback_limit: 3
    local_absolute_limit: 6
    stale_after: 2s
"#,
        )
        .expect("config parses");
        let endpoint_config =
            endpoint_slice_config_from_discovery(&config.discovery).expect("endpoint config");
        let provider = peer_provider_from_config(&config.discovery).expect("provider");

        assert_eq!(endpoint_config.namespace, "default");
        assert_eq!(endpoint_config.service_name, "gabion");
        assert_eq!(endpoint_config.port_name.as_deref(), Some("gossip"));
        assert!(provider.snapshot().local_only());
    }

    #[quickcheck]
    fn quickcheck_endpoint_slice_config_preserves_selector_shape(
        case: EndpointConfigCase,
    ) -> TestResult {
        let mut discovery = DiscoveryConfig {
            kind: DiscoveryMode::KubernetesEndpointSlice,
            self_addr: Some("10.0.0.1:18080".parse().expect("addr")),
            ..Default::default()
        };
        if case.include_legacy_selector {
            discovery.namespace = Some("default".to_string());
            discovery.service_name = Some("gabion".to_string());
            discovery.port_name = Some(DEFAULT_GOSSIP_PORT_NAME.to_string());
        }
        for index in 0..case.selector_count {
            discovery.endpoint_slices.push(EndpointSliceSelectorConfig {
                namespace: "default".to_string(),
                service_name: format!("gabion-{index}"),
                port_name: Some(DEFAULT_GOSSIP_PORT_NAME.to_string()),
            });
        }

        let configs = endpoint_slice_configs_from_discovery(&discovery);
        if case.selector_count == 0 && !case.include_legacy_selector {
            return if configs == Err(ConfigError::MissingKubernetesNamespace) {
                TestResult::passed()
            } else {
                TestResult::error("missing Kubernetes selector did not report missing namespace")
            };
        }

        let Ok(configs) = configs else {
            return TestResult::error("valid Kubernetes selector config was rejected");
        };
        let expected = usize::from(case.selector_count.max(1));
        if configs.len() != expected {
            return TestResult::error("EndpointSlice config count did not match selectors");
        }
        if configs
            .iter()
            .any(|config| config.port_name.as_deref() != Some(DEFAULT_GOSSIP_PORT_NAME))
        {
            return TestResult::error("EndpointSlice config lost default gossip port name");
        }
        TestResult::passed()
    }

    #[test]
    fn kubernetes_endpoint_slice_config_accepts_multiple_selectors() {
        let config = LocalOnlyConfig::from_yaml_str(
            r#"
storage:
  max_keys: 16
discovery:
  kind: kubernetes
  self_addr: 10.0.0.1:18080
  endpoint_slices:
    - namespace: default
      service_name: gabion-grpc
      port_name: gossip
    - namespace: default
      service_name: gabion-nginx
      port_name: gossip
gossip:
  enabled: true
  bind: 0.0.0.0:18080
limits:
  - name: tenant_api_minute
    domain: api
    descriptors:
      - key: tenant
        value: "*"
    limit: 10
    window: 60s
    bucket: 1s
    local_fallback_limit: 3
    local_absolute_limit: 6
    stale_after: 2s
"#,
        )
        .expect("config parses");

        let configs =
            endpoint_slice_configs_from_discovery(&config.discovery).expect("endpoint configs");

        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].service_name, "gabion-grpc");
        assert_eq!(configs[1].service_name, "gabion-nginx");
        assert_eq!(
            configs[0].self_addr,
            Some("10.0.0.1:18080".parse().expect("addr"))
        );
    }

    #[test]
    fn kubernetes_endpoint_slice_config_defaults_port_name_to_gossip() {
        let config = LocalOnlyConfig::from_yaml_str(
            r#"
storage:
  max_keys: 16
discovery:
  kind: kubernetes
  namespace: default
  service_name: gabion
gossip:
  enabled: true
  bind: 0.0.0.0:18080
limits:
  - name: tenant_api_minute
    domain: api
    descriptors:
      - key: tenant
        value: "*"
    limit: 10
    window: 60s
    bucket: 1s
    local_fallback_limit: 3
    local_absolute_limit: 6
    stale_after: 2s
"#,
        )
        .expect("config parses");
        let endpoint_config =
            endpoint_slice_config_from_discovery(&config.discovery).expect("endpoint config");

        assert_eq!(endpoint_config.port_name.as_deref(), Some("gossip"));
    }

    #[quickcheck]
    fn quickcheck_removed_peers_are_accepted_only_within_grace_window(
        case: RuntimeRecentPeerCase,
    ) -> TestResult {
        let addr_b = "127.0.0.1:10202".parse().expect("addr");
        let peers = SnapshotPeerHandler::with_capacity(4);
        peers.peer_added(Peer::new(addr_b));
        let mut config = runtime_config(1);
        config.recent_peer_grace_millis = u64::from(case.grace_millis);
        let mut runtime = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(1)),
            peers.clone(),
            MemoryTransport::default(),
            config,
        );

        runtime.tick(0);
        peers.peer_removed(Peer::new(addr_b));
        runtime.tick(1);
        runtime
            .transport_mut()
            .push_inbox(Peer::new(addr_b), empty_gossip_packet(2));
        let summary = runtime.tick(1 + u64::from(case.elapsed_millis));
        let should_accept = u64::from(case.elapsed_millis) < u64::from(case.grace_millis);

        if should_accept
            && summary.frames_received == 1
            && summary.peer_rejected == 0
            && runtime.metrics().recv_bytes > 0
        {
            return TestResult::passed();
        }
        if !should_accept
            && summary.frames_received == 0
            && summary.peer_rejected == 1
            && runtime.metrics().recv_bytes == 0
        {
            return TestResult::passed();
        }
        TestResult::error("recent peer grace window did not match acceptance behavior")
    }

    #[quickcheck]
    fn quickcheck_unknown_senders_are_rejected_before_decode(
        case: RuntimeUnknownPeerCase,
    ) -> TestResult {
        let known = peer_addr(case.known_octet);
        let unknown = peer_addr(case.unknown_octet);
        let mut runtime = runtime(1, known);
        runtime.transport_mut().push_inbox(
            Peer::new(unknown),
            empty_gossip_packet(u64::from(case.sender)),
        );

        let summary = runtime.tick(0);

        if summary.frames_received == 0
            && summary.cells_merged == 0
            && summary.peer_rejected == 1
            && runtime.metrics().recv_bytes == 0
        {
            TestResult::passed()
        } else {
            TestResult::error("unknown peer was decoded or merged before rejection")
        }
    }

    #[quickcheck]
    fn quickcheck_dirty_overflow_forces_bounded_resync(case: RuntimeDirtyResyncCase) -> TestResult {
        let mut config = runtime_config(7);
        config.fanout = 1;
        config.max_cells_per_frame = usize::from(case.max_cells_per_frame);
        let mut runtime = StandaloneGossipRuntime::new(
            shared_limiter(LocalEngine::with_identity(
                RuleTable::new(vec![rule_config_for_runtime(1)]),
                16,
                10,
                16,
                1,
                NodeIdentity {
                    node_id: 7_u128.into(),
                    incarnation: 1,
                },
            )),
            RuntimePeerHandler::Static(StaticPeerHandler::new(vec![peer_addr(10)], None)),
            MemoryTransport::default(),
            config,
        );

        for index in 0..case.tenant_count {
            let value = format!("tenant-{index}");
            let descriptors = [Descriptor {
                key: "tenant",
                value: value.as_str(),
            }];
            let mut limiter = runtime.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), u64::from(index)),
                Decision::Allow
            );
        }

        let summary = runtime.tick(10);
        let expected = usize::from(case.tenant_count).min(usize::from(case.max_cells_per_frame));

        if summary.cells_sent != expected {
            return TestResult::error("dirty overflow resync did not send bounded active-cell set");
        }
        if runtime.metrics().dirty_overflow != 1 {
            return TestResult::error("dirty overflow was not reported exactly once");
        }
        TestResult::passed()
    }

    #[quickcheck]
    fn quickcheck_ticks_are_deterministic_for_supplied_timestamp(
        case: RuntimeDeterministicTickCase,
    ) -> TestResult {
        let mut peers = Vec::with_capacity(usize::from(case.peer_count));
        for index in 0..case.peer_count {
            peers.push(peer_addr(index.saturating_add(1)));
        }
        let mut config = runtime_config(1);
        config.fanout = usize::from(case.fanout);
        let peer_handler_a =
            RuntimePeerHandler::Static(StaticPeerHandler::new(peers.clone(), None));
        let peer_handler_b = RuntimePeerHandler::Static(StaticPeerHandler::new(peers, None));
        let mut runtime_a = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(1)),
            peer_handler_a,
            MemoryTransport::default(),
            config,
        );
        let mut runtime_b = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(1)),
            peer_handler_b,
            MemoryTransport::default(),
            config,
        );

        let summary_a = runtime_a.tick(u64::from(case.now_millis));
        let summary_b = runtime_b.tick(u64::from(case.now_millis));
        let outbox_a = runtime_a.transport_mut().drain_outbox();
        let outbox_b = runtime_b.transport_mut().drain_outbox();

        if summary_a != summary_b {
            return TestResult::error(
                "same runtime state and timestamp produced different summaries",
            );
        }
        if outbox_a.len() != outbox_b.len()
            || outbox_a.iter().zip(outbox_b.iter()).any(
                |((peer_a, packet_a), (peer_b, packet_b))| {
                    peer_a != peer_b || packet_a.len() != packet_b.len()
                },
            )
        {
            return TestResult::error("same runtime state and timestamp produced different sends");
        }
        TestResult::passed()
    }

    #[quickcheck]
    fn quickcheck_runtime_reuses_construction_buffers(case: RuntimeBufferReuseCase) -> TestResult {
        let mut peers = Vec::with_capacity(usize::from(case.peer_count));
        for index in 0..case.peer_count {
            peers.push(peer_addr(index.saturating_add(1)));
        }
        let mut runtime = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(1)),
            RuntimePeerHandler::Static(StaticPeerHandler::new(peers, None)),
            MemoryTransport::default(),
            runtime_config(1),
        );
        let initial = runtime.buffer_capacities();

        for tick in 0..case.tick_count {
            let _ = runtime.tick(u64::from(tick));
            if runtime.buffer_capacities() != initial {
                return TestResult::error("runtime buffer capacity changed after a tick");
            }
            let _ = runtime.transport_mut().drain_outbox();
        }

        TestResult::passed()
    }

    #[test]
    fn endpoint_slice_selector_defaults_port_name_to_gossip() {
        let config = LocalOnlyConfig::from_yaml_str(
            r#"
storage:
  max_keys: 16
discovery:
  kind: kubernetes
  endpoint_slices:
    - namespace: default
      service_name: gabion
gossip:
  enabled: true
  bind: 0.0.0.0:18080
limits:
  - name: tenant_api_minute
    domain: api
    descriptors:
      - key: tenant
        value: "*"
    limit: 10
    window: 60s
    bucket: 1s
    local_fallback_limit: 3
    local_absolute_limit: 6
    stale_after: 2s
"#,
        )
        .expect("config parses");
        let configs =
            endpoint_slice_configs_from_discovery(&config.discovery).expect("endpoint configs");

        assert_eq!(configs[0].port_name.as_deref(), Some("gossip"));
    }

    #[test]
    fn gossip_runtime_rejects_unknown_peer_before_decode() {
        let known = "127.0.0.1:12001".parse().expect("addr");
        let unknown = "127.0.0.1:12002".parse().expect("addr");
        let mut runtime_a = runtime(1, known);
        let mut runtime_b = runtime(2, known);
        let descriptors = [Descriptor {
            key: "tenant",
            value: "a",
        }];

        {
            let mut limiter = runtime_a.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 0),
                Decision::Allow
            );
        }
        runtime_a.tick(1);
        for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
            runtime_b
                .transport_mut()
                .push_inbox(Peer::new(unknown), payload);
        }

        let summary = runtime_b.tick(2);

        assert_eq!(summary.frames_received, 0);
        assert_eq!(summary.cells_merged, 0);
        assert_eq!(summary.peer_rejected, 1);
    }

    #[test]
    fn gossip_runtime_accepts_recently_known_peer_during_grace_window() {
        let old_peer_addr = "127.0.0.1:13001".parse().expect("addr");
        let new_peer_addr = "127.0.0.1:13002".parse().expect("addr");
        let provider =
            SnapshotPeerHandler::new(PeerSnapshot::new(vec![Peer::new(old_peer_addr)], false, 0));
        let limiter = shared_limiter(engine_with_node(2));
        let config = StandaloneGossipConfig {
            cluster_id_hash: 42,
            sender_node_id: 2_u128.into(),
            sender_incarnation: 1,
            fanout: 1,
            max_payload_bytes: 4096,
            max_cells_per_frame: 16,
            remote_cell_capacity: 64,
            remote_dirty_capacity: 64,
            auth_key: None,
            max_peers: 16,
            recent_peer_grace_millis: 10,
            send_policy: GossipSendPolicy::with_linger_ms(1),
        };
        let mut receiver = StandaloneGossipRuntime::new(
            limiter,
            provider.clone(),
            MemoryTransport::default(),
            config,
        );
        let mut sender = runtime(1, old_peer_addr);
        let descriptors = [Descriptor {
            key: "tenant",
            value: "a",
        }];

        receiver.tick(0);
        provider.peer_removed(Peer::new(old_peer_addr));
        provider.peer_added(Peer::new(new_peer_addr));
        {
            let mut limiter = sender.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 1),
                Decision::Allow
            );
        }
        sender.tick(2);
        for (_peer, payload) in sender.transport_mut().drain_outbox() {
            receiver
                .transport_mut()
                .push_inbox(Peer::new(old_peer_addr), payload);
        }

        let summary = receiver.tick(3);

        assert_eq!(summary.peer_rejected, 0);
        assert_eq!(summary.cells_merged, 1);
    }

    #[test]
    fn packet_loss_does_not_drop_dirty_cell_retry() {
        let addr_a = "127.0.0.1:14001".parse().expect("addr");
        let addr_b = "127.0.0.1:14002".parse().expect("addr");
        let mut runtime_a = runtime(1, addr_b);
        let mut runtime_b = runtime(2, addr_a);
        let descriptors = [Descriptor {
            key: "tenant",
            value: "loss",
        }];

        {
            let mut limiter = runtime_a.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 0),
                Decision::Allow
            );
        }

        let dropped = runtime_a.tick(1);
        let lost_packets = runtime_a.transport_mut().drain_outbox();
        assert_eq!(dropped.cells_sent, 1);
        assert_eq!(lost_packets.len(), 1);
        assert_eq!(runtime_b.tick(2).cells_merged, 0);

        let retried = runtime_a.tick(3);
        for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
            runtime_b
                .transport_mut()
                .push_inbox(Peer::new(addr_a), payload);
        }
        let received = runtime_b.tick(4);

        assert_eq!(retried.cells_sent, 1);
        assert_eq!(received.cells_merged, 1);
    }

    #[test]
    fn authenticated_runtime_rejects_mismatched_hmac_frames() {
        let addr_a = "127.0.0.1:11001".parse().expect("addr");
        let addr_b = "127.0.0.1:11002".parse().expect("addr");
        let config_a = StandaloneGossipConfig {
            cluster_id_hash: 42,
            sender_node_id: 1_u128.into(),
            sender_incarnation: 1,
            fanout: 1,
            max_payload_bytes: 4096,
            max_cells_per_frame: 16,
            remote_cell_capacity: 64,
            remote_dirty_capacity: 64,
            auth_key: Some(crate::gossip::HmacKey::new([1; 32])),
            max_peers: 16,
            recent_peer_grace_millis: 30_000,
            send_policy: GossipSendPolicy::with_linger_ms(1),
        };
        let mut config_b = config_a;
        config_b.sender_node_id = 2_u128.into();
        config_b.auth_key = Some(crate::gossip::HmacKey::new([2; 32]));
        let mut runtime_a = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(1)),
            RuntimePeerHandler::Static(StaticPeerHandler::new(vec![addr_b], None)),
            MemoryTransport::default(),
            config_a,
        );
        let mut runtime_b = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(2)),
            RuntimePeerHandler::Static(StaticPeerHandler::new(vec![addr_a], None)),
            MemoryTransport::default(),
            config_b,
        );
        let descriptors = [Descriptor {
            key: "tenant",
            value: "a",
        }];

        {
            let mut limiter = runtime_a.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 0),
                Decision::Allow
            );
        }
        runtime_a.tick(1);
        for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
            runtime_b
                .transport_mut()
                .push_inbox(Peer::new(addr_a), payload);
        }

        let received = runtime_b.tick(2);

        assert_eq!(received.frames_received, 1);
        assert_eq!(received.cells_merged, 0);
        assert_eq!(runtime_b.metrics().decode_errors, 1);
        assert_eq!(runtime_b.metrics().auth_failures, 1);
        {
            let mut limiter = runtime_b.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 3),
                Decision::Allow
            );
        }
    }

    #[test]
    fn dirty_overflow_forces_bounded_resync_cells() {
        let mut runtime = StandaloneGossipRuntime::new(
            shared_limiter(LocalEngine::with_identity(
                RuleTable::new(vec![rule_config_for_runtime(1)]),
                8,
                10,
                8,
                1,
                NodeIdentity {
                    node_id: 7_u128.into(),
                    incarnation: 1,
                },
            )),
            RuntimePeerHandler::Static(StaticPeerHandler::new(
                vec!["127.0.0.1:19001".parse().expect("addr")],
                None,
            )),
            MemoryTransport::default(),
            StandaloneGossipConfig {
                cluster_id_hash: 42,
                sender_node_id: 7_u128.into(),
                sender_incarnation: 1,
                fanout: 1,
                max_payload_bytes: 4096,
                max_cells_per_frame: 16,
                remote_cell_capacity: 64,
                remote_dirty_capacity: 64,
                auth_key: None,
                max_peers: 16,
                recent_peer_grace_millis: 30_000,
                send_policy: GossipSendPolicy::with_linger_ms(1),
            },
        );

        for value in ["a", "b"] {
            let descriptors = [Descriptor {
                key: "tenant",
                value,
            }];
            let mut limiter = runtime.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 0),
                Decision::Allow
            );
        }

        let sent = runtime.tick(1);

        assert_eq!(sent.cells_sent, 2);
        assert_eq!(runtime.metrics().dirty_overflow, 1);
    }

    #[test]
    #[ignore = "requires localhost UDP sockets"]
    fn local_udp_gossip_converges_end_to_end() {
        let transport_a =
            UdpGossipTransport::bind("127.0.0.1:0".parse().expect("addr")).expect("udp a");
        let transport_b =
            UdpGossipTransport::bind("127.0.0.1:0".parse().expect("addr")).expect("udp b");
        let addr_a = transport_a.local_addr().expect("addr a");
        let addr_b = transport_b.local_addr().expect("addr b");
        let config_a = StandaloneGossipConfig {
            cluster_id_hash: 42,
            sender_node_id: 1_u128.into(),
            sender_incarnation: 1,
            fanout: 1,
            max_payload_bytes: 4096,
            max_cells_per_frame: 16,
            remote_cell_capacity: 64,
            remote_dirty_capacity: 64,
            auth_key: None,
            max_peers: 16,
            recent_peer_grace_millis: 30_000,
            send_policy: GossipSendPolicy::with_linger_ms(1),
        };
        let mut config_b = config_a;
        config_b.sender_node_id = 2_u128.into();
        let mut runtime_a = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(1)),
            RuntimePeerHandler::Static(StaticPeerHandler::new(vec![addr_b], None)),
            transport_a,
            config_a,
        );
        let mut runtime_b = StandaloneGossipRuntime::new(
            shared_limiter(engine_with_node(2)),
            RuntimePeerHandler::Static(StaticPeerHandler::new(vec![addr_a], None)),
            transport_b,
            config_b,
        );
        let descriptors = [Descriptor {
            key: "tenant",
            value: "udp",
        }];

        {
            let mut limiter = runtime_a.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 0),
                Decision::Allow
            );
        }
        assert_eq!(runtime_a.tick(1).peers_sent, 1);

        let mut received = GossipTickSummary::default();
        for attempt in 0..1_000 {
            received = runtime_b.tick(2 + attempt);
            if received.cells_merged == 1 {
                break;
            }
        }

        assert_eq!(received.cells_merged, 1);
        {
            let mut limiter = runtime_b.limiter().lock().expect("limiter");
            assert_eq!(
                limiter.check_and_record(request(&descriptors), 100),
                Decision::Reject(RejectReason::GlobalLimit)
            );
        }
    }

    #[tokio::test]
    #[ignore = "requires local Kubernetes API"]
    async fn local_kubernetes_endpoint_slice_watcher_drives_gossip_convergence() {
        use crate::discovery::kubernetes::{
            EndpointSliceDiscoveryConfig, run_endpoint_slice_watchers,
        };
        use k8s_openapi::api::core::v1::{Namespace, Service, ServicePort, ServiceSpec};
        use k8s_openapi::api::discovery::v1::{
            Endpoint, EndpointConditions, EndpointPort, EndpointSlice,
        };
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        use kube::api::{DeleteParams, PostParams};
        use kube::{Api, Client};
        use std::collections::BTreeMap;

        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            let client = Client::try_default().await.expect("local kube client");
            let namespace = format!("gabion-kube-e2e-{}", std::process::id());
            let grpc_service_name = "gabion-grpc";
            let nginx_service_name = "gabion-nginx";

            let namespaces: Api<Namespace> = Api::all(client.clone());
            let _ = namespaces
                .create(
                    &PostParams::default(),
                    &Namespace {
                        metadata: ObjectMeta {
                            name: Some(namespace.clone()),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .await
                .expect("create namespace");

            let services: Api<Service> = Api::namespaced(client.clone(), &namespace);
            for service_name in [grpc_service_name, nginx_service_name] {
                services
                    .create(
                        &PostParams::default(),
                        &Service {
                            metadata: ObjectMeta {
                                name: Some(service_name.to_string()),
                                ..Default::default()
                            },
                            spec: Some(ServiceSpec {
                                selector: Some(BTreeMap::from([(
                                    "app".to_string(),
                                    service_name.to_string(),
                                )])),
                                ports: Some(vec![ServicePort {
                                    name: Some("gossip".to_string()),
                                    port: 18080,
                                    target_port: None,
                                    ..Default::default()
                                }]),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("create service");
            }

            let advertised_ip: std::net::IpAddr = "10.0.0.1".parse().expect("ip");
            let advertised_a: SocketAddr = "10.0.0.1:30001".parse().expect("addr a");
            let advertised_b: SocketAddr = "10.0.0.1:30002".parse().expect("addr b");

            let provider = SnapshotPeerHandler::new(PeerSnapshot::new(Vec::new(), false, 0));
            let watcher_configs = vec![
                EndpointSliceDiscoveryConfig {
                    namespace: namespace.clone(),
                    service_name: grpc_service_name.to_string(),
                    port_name: Some("gossip".to_string()),
                    self_addr: Some(advertised_a),
                },
                EndpointSliceDiscoveryConfig {
                    namespace: namespace.clone(),
                    service_name: nginx_service_name.to_string(),
                    port_name: Some("gossip".to_string()),
                    self_addr: Some(advertised_a),
                },
            ];
            let watcher_provider = provider.clone();
            let watcher = tokio::spawn(run_endpoint_slice_watchers(
                client.clone(),
                watcher_configs,
                watcher_provider,
            ));

            let endpoint_slices: Api<EndpointSlice> = Api::namespaced(client.clone(), &namespace);
            for (slice_name, service_name, port) in [
                ("gabion-grpc-a", grpc_service_name, advertised_a.port()),
                ("gabion-nginx-b", nginx_service_name, advertised_b.port()),
            ] {
                endpoint_slices
                    .create(
                        &PostParams::default(),
                        &EndpointSlice {
                            address_type: "IPv4".to_string(),
                            metadata: ObjectMeta {
                                name: Some(slice_name.to_string()),
                                labels: Some(BTreeMap::from([(
                                    "kubernetes.io/service-name".to_string(),
                                    service_name.to_string(),
                                )])),
                                ..Default::default()
                            },
                            endpoints: vec![Endpoint {
                                addresses: vec![advertised_ip.to_string()],
                                conditions: Some(EndpointConditions {
                                    ready: Some(true),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }],
                            ports: Some(vec![EndpointPort {
                                name: Some("gossip".to_string()),
                                port: Some(i32::from(port)),
                                ..Default::default()
                            }]),
                        },
                    )
                    .await
                    .expect("create endpoint slice");
            }

            for _ in 0..50 {
                if provider
                    .snapshot()
                    .peers()
                    .contains(&Peer::new(advertised_b))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(provider.snapshot().peers(), &[Peer::new(advertised_b)]);

            let config_a = StandaloneGossipConfig {
                cluster_id_hash: 42,
                sender_node_id: 1_u128.into(),
                sender_incarnation: 1,
                fanout: 1,
                max_payload_bytes: 4096,
                max_cells_per_frame: 16,
                remote_cell_capacity: 64,
                remote_dirty_capacity: 64,
                auth_key: None,
                max_peers: 16,
                recent_peer_grace_millis: 30_000,
                send_policy: GossipSendPolicy::with_linger_ms(1),
            };
            let mut config_b = config_a;
            config_b.sender_node_id = 2_u128.into();
            let mut runtime_a = StandaloneGossipRuntime::new(
                shared_limiter(engine_with_node(1)),
                RuntimePeerHandler::Snapshot(provider.clone()),
                MemoryTransport::default(),
                config_a,
            );
            let mut runtime_b = StandaloneGossipRuntime::new(
                shared_limiter(engine_with_node(2)),
                RuntimePeerHandler::Static(StaticPeerHandler::new(vec![advertised_a], None)),
                MemoryTransport::default(),
                config_b,
            );
            let descriptors = [Descriptor {
                key: "tenant",
                value: "kube",
            }];

            {
                let mut limiter = runtime_a.limiter().lock().expect("limiter");
                assert_eq!(
                    limiter.check_and_record(request(&descriptors), 0),
                    Decision::Allow
                );
            }
            assert_eq!(runtime_a.tick(1).peers_sent, 1);
            for (_peer, payload) in runtime_a.transport_mut().drain_outbox() {
                runtime_b
                    .transport_mut()
                    .push_inbox(Peer::new(advertised_a), payload);
            }

            let mut received = GossipTickSummary::default();
            for attempt in 0..50 {
                received = runtime_b.tick(2 + attempt);
                if received.cells_merged == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(received.cells_merged, 1);

            {
                let mut limiter = runtime_b.limiter().lock().expect("limiter");
                assert_eq!(
                    limiter.check_and_record(request(&descriptors), 100),
                    Decision::Reject(RejectReason::GlobalLimit)
                );
            }

            endpoint_slices
                .delete("gabion-nginx-b", &DeleteParams::default())
                .await
                .expect("delete endpoint slice");
            for _ in 0..50 {
                if provider.snapshot().peers().is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(provider.snapshot().peers().is_empty());

            watcher.abort();
            let _ = namespaces
                .delete(&namespace, &DeleteParams::default())
                .await;
        })
        .await
        .expect("local Kubernetes convergence test timed out");
    }
}
