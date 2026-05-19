use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

extern crate self as gabion_core;
extern crate self as gabion_discovery;
extern crate self as gabion_gossip;

pub use config::{
    ConfigError, DescriptorConfig, GossipConfig, LimitRuleConfig, RuntimeConfig,
    RuntimeTuningConfig, SafetyMarginConfig, StorageConfig,
};
pub use core::{
    CardinalityError, CardinalityLimits, Decision, Descriptor, EnforcementMode, HashedLimitRequest,
    HashedLimitRequestBuilder, KeyHash, LimitRequest, OverflowPolicy, RateLimitRecorder,
    RateLimitRuntime, RejectReason, RuleId, SafetyMargin, TimedHashedLimitRequest, WindowSpec,
};
pub use discovery::{
    DiscoveryConfig, DiscoveryMode, EndpointSliceSelectorConfig,
    endpoint_slice_config_from_discovery, endpoint_slice_configs_from_discovery,
    peer_provider_from_config,
};

use crate::core::{LocalEngine, NodeId, NodeIdentity};
use crate::discovery::{
    FilePeerHandler, Peer, PeerHandler, PeerSnapshot, SnapshotPeerHandler, StaticPeerHandler,
};

pub mod admin;
pub mod config;
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

pub type Config = RuntimeConfig;

#[derive(Clone, Debug)]
pub struct Runtime<H = NoOpCountUpdateHandler> {
    inner: Arc<RuntimeInner<H>>,
}

#[derive(Debug)]
struct RuntimeInner<H> {
    limiter: SharedLimiter,
    limits: CardinalityLimits,
    runtime: RuntimeTuningConfig,
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
        let mut runtime_config = crate::gossip_runtime::StandaloneGossipConfig::from_runtime_config(
            &self.inner.gossip,
            &self.inner.discovery,
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
        let wake_millis = duration_millis(self.inner.gossip.linger)
            .max(1)
            .div_ceil(4)
            .max(1);
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
                let poll_millis = duration_millis(self.inner.gossip.linger).max(1);
                Ok(Some(tokio::spawn(async move {
                    crate::discovery::run_file_peer_events(handler, poll_millis).await;
                })))
            }
            RuntimePeerHandler::Snapshot(handler)
                if self.inner.discovery.kind == DiscoveryMode::Kubernetes
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

fn duration_millis(duration: std::time::Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX).max(1)
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
