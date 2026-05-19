//! NGINX request-path adapter and bounded configuration tables.
//!
//! Invariants:
//! - Request-path local limiting performs no network or Kubernetes I/O.
//! - Missing variables decline without allocating or tracking a key.
//! - Peer tables are sorted, deduplicated, bounded, and exclude self.
//! - Peer-file loading uses caller-provided scratch memory and rejects
//!   oversized files.
//! - Kubernetes selector config is bounded and defaults the gossip port name
//!   consistently.

use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};

use gabion::DiscoveryMode;
use gabion::{
    ApplyBatchOutcome, CountAggregate, CountUpdateHandler, Decision, EnforcementMode,
    HashedLimitRequest, HashedLimitRequestBuilder, RuleId, Runtime, TimedHashedLimitRequest,
};
use thiserror::Error;

#[cfg(feature = "ngx-module")]
mod module;
mod request_queue;

pub use request_queue::{
    RequestEvent, SharedRequestEventRecord, SharedRequestQueue, SharedRequestRingControl,
};

pub const MAX_NAME_BYTES: usize = 64;
pub const MAX_KEY_COMPONENTS: usize = 8;
pub const MAX_NGINX_PEERS: usize = 64;
pub const MAX_ENDPOINT_SLICE_SELECTORS: usize = 16;
pub const MAX_PEER_FILE_PATH_BYTES: usize = 256;
pub const MAX_NGINX_SHM_RULES: usize = 16;
pub const MAX_NGINX_SHM_BUCKETS: usize = DEFAULT_MAX_ACTIVE_BUCKETS;
pub const DEFAULT_MAX_ACTIVE_BUCKETS: usize = 64;
pub const DEFAULT_GOSSIP_PAYLOAD_BYTES: usize = 64 * 1024;
pub const DEFAULT_GOSSIP_MAX_CELLS: usize = 4096;
pub const DEFAULT_GOSSIP_FANOUT: usize = 3;
pub const DEFAULT_GOSSIP_CLUSTER_ID_HASH: u128 = 1;
pub const DEFAULT_GOSSIP_LINGER_MS: u64 = 250;
pub const DEFAULT_GOSSIP_PORT_NAME: &str = "gossip";
const SHM_MAGIC: u32 = 0x4742_4e58;
const SHM_VERSION: u16 = 1;
const SHM_LOCK_FREE: u16 = 0;
const SHM_LOCK_HELD: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NginxStatus {
    Declined,
    TooManyRequests,
}

impl NginxStatus {
    pub fn from_decision(decision: Decision) -> Self {
        match decision {
            Decision::Allow => Self::Declined,
            Decision::Reject(_) => Self::TooManyRequests,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FixedName<const N: usize> {
    bytes: [u8; N],
    len: u8,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NginxEndpointSliceSelector {
    pub namespace: FixedName<MAX_NAME_BYTES>,
    pub service_name: FixedName<MAX_NAME_BYTES>,
    pub port_name: FixedName<MAX_NAME_BYTES>,
}

impl NginxEndpointSliceSelector {
    pub fn new(
        namespace: &str,
        service_name: &str,
        port_name: &str,
    ) -> Result<Self, NginxConfigError> {
        let port_name = if port_name.is_empty() {
            DEFAULT_GOSSIP_PORT_NAME
        } else {
            port_name
        };
        Ok(Self {
            namespace: FixedName::new(namespace)?,
            service_name: FixedName::new(service_name)?,
            port_name: FixedName::new(port_name)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NginxEndpointSliceSelectors {
    selectors: [NginxEndpointSliceSelector; MAX_ENDPOINT_SLICE_SELECTORS],
    len: u8,
}

impl NginxEndpointSliceSelectors {
    pub const fn empty() -> Self {
        Self {
            selectors: [NginxEndpointSliceSelector {
                namespace: FixedName::empty(),
                service_name: FixedName::empty(),
                port_name: FixedName::empty(),
            }; MAX_ENDPOINT_SLICE_SELECTORS],
            len: 0,
        }
    }

    pub fn push(&mut self, selector: NginxEndpointSliceSelector) -> Result<(), NginxConfigError> {
        if self.len as usize == MAX_ENDPOINT_SLICE_SELECTORS {
            return Err(NginxConfigError::TooManyEndpointSliceSelectors);
        }
        self.selectors[self.len as usize] = selector;
        self.len += 1;
        Ok(())
    }

    pub fn as_slice(&self) -> &[NginxEndpointSliceSelector] {
        &self.selectors[..self.len as usize]
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for NginxEndpointSliceSelectors {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NginxDiscoveryConfig {
    pub kind: DiscoveryMode,
    pub bind_addr: Option<SocketAddr>,
    pub self_addr: Option<SocketAddr>,
    pub static_peers: NginxPeerTable,
    pub peer_file_path: FixedName<MAX_PEER_FILE_PATH_BYTES>,
    pub endpoint_slices: NginxEndpointSliceSelectors,
    pub linger_ms: u64,
    pub fanout: usize,
    pub max_payload_bytes: usize,
    pub max_cells_per_frame: usize,
    pub cluster_id_hash: u128,
}

impl NginxDiscoveryConfig {
    pub fn local_default() -> Self {
        Self {
            kind: DiscoveryMode::default(),
            bind_addr: None,
            self_addr: None,
            static_peers: NginxPeerTable::empty(),
            peer_file_path: FixedName::empty(),
            endpoint_slices: NginxEndpointSliceSelectors::empty(),
            linger_ms: DEFAULT_GOSSIP_LINGER_MS,
            fanout: DEFAULT_GOSSIP_FANOUT,
            max_payload_bytes: DEFAULT_GOSSIP_PAYLOAD_BYTES,
            max_cells_per_frame: DEFAULT_GOSSIP_MAX_CELLS,
            cluster_id_hash: DEFAULT_GOSSIP_CLUSTER_ID_HASH,
        }
    }

    pub fn set_kind(&mut self, kind: DiscoveryMode) {
        self.kind = kind;
    }

    pub fn set_self_addr(&mut self, self_addr: SocketAddr) {
        self.self_addr = Some(self_addr);
    }

    pub fn set_bind_addr(&mut self, bind_addr: SocketAddr) {
        self.bind_addr = Some(bind_addr);
    }

    pub fn add_static_peer(&mut self, peer: SocketAddr) -> Result<(), NginxPeerConfigError> {
        if Some(peer) == self.self_addr {
            return Ok(());
        }
        self.static_peers.insert(NginxPeer::new(peer))
    }

    pub fn set_peer_file_path(&mut self, path: &str) -> Result<(), NginxConfigError> {
        self.peer_file_path = FixedName::new(path)?;
        Ok(())
    }

    pub fn add_endpoint_slice(
        &mut self,
        namespace: &str,
        service_name: &str,
        port_name: &str,
    ) -> Result<(), NginxConfigError> {
        self.endpoint_slices.push(NginxEndpointSliceSelector::new(
            namespace,
            service_name,
            port_name,
        )?)
    }

    pub fn set_linger_ms(&mut self, millis: u64) {
        self.linger_ms = millis.max(1);
    }

    pub fn set_fanout(&mut self, fanout: usize) {
        self.fanout = fanout.max(1);
    }

    pub fn set_max_payload_bytes(&mut self, bytes: usize) {
        self.max_payload_bytes = bytes.max(68);
    }

    pub fn set_max_cells_per_frame(&mut self, cells: usize) {
        self.max_cells_per_frame = cells.max(1);
    }

    pub fn set_cluster_id_hash(&mut self, cluster_id_hash: u128) {
        self.cluster_id_hash = cluster_id_hash;
    }
}

impl Default for NginxDiscoveryConfig {
    fn default() -> Self {
        Self::local_default()
    }
}

impl<const N: usize> FixedName<N> {
    pub const fn empty() -> Self {
        Self {
            bytes: [0; N],
            len: 0,
        }
    }

    pub fn new(value: &str) -> Result<Self, NginxConfigError> {
        if value.len() > N || value.len() > u8::MAX as usize {
            return Err(NginxConfigError::NameTooLong);
        }

        let mut name = Self::empty();
        let len = value.len();
        name.bytes[..len].copy_from_slice(value.as_bytes());
        name.len = len as u8;
        Ok(name)
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len as usize]).unwrap_or_default()
    }
}

impl<const N: usize> Default for FixedName<N> {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NginxZoneConfig {
    pub name: FixedName<MAX_NAME_BYTES>,
    pub bytes: usize,
    pub max_keys: usize,
}

impl NginxZoneConfig {
    pub fn new(name: &str, bytes: usize, max_keys: usize) -> Result<Self, NginxConfigError> {
        if bytes == 0 || max_keys == 0 {
            return Err(NginxConfigError::InvalidCapacity);
        }

        Ok(Self {
            name: FixedName::new(name)?,
            bytes,
            max_keys,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyComponent {
    pub variable: FixedName<MAX_NAME_BYTES>,
}

impl KeyComponent {
    pub fn variable(name: &str) -> Result<Self, NginxConfigError> {
        let name = name.strip_prefix('$').unwrap_or(name);
        Ok(Self {
            variable: FixedName::new(name)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyComponentList {
    components: [KeyComponent; MAX_KEY_COMPONENTS],
    len: u8,
}

impl KeyComponentList {
    pub fn new(names: &[&str]) -> Result<Self, NginxConfigError> {
        if names.is_empty() {
            return Err(NginxConfigError::NoKeyComponents);
        }
        if names.len() > MAX_KEY_COMPONENTS {
            return Err(NginxConfigError::TooManyKeyComponents);
        }

        let mut components = [KeyComponent {
            variable: FixedName::empty(),
        }; MAX_KEY_COMPONENTS];
        for (index, name) in names.iter().enumerate() {
            components[index] = KeyComponent::variable(name)?;
        }

        Ok(Self {
            components,
            len: names.len() as u8,
        })
    }

    pub fn as_slice(&self) -> &[KeyComponent] {
        &self.components[..self.len as usize]
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NginxRuleConfig {
    pub id: RuleId,
    pub name: FixedName<MAX_NAME_BYTES>,
    pub domain: FixedName<MAX_NAME_BYTES>,
    pub key_components: KeyComponentList,
    pub limit: u64,
    pub window_millis: u64,
    pub bucket_millis: u64,
    pub local_fallback_limit: u64,
    pub local_absolute_limit: u64,
    pub stale_after_millis: u64,
    pub mode: EnforcementMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NginxRuleBuilder<'a> {
    pub id: RuleId,
    pub name: &'a str,
    pub domain: &'a str,
    pub key_components: &'a [&'a str],
    pub limit: &'a str,
    pub window: &'a str,
    pub bucket: &'a str,
    pub local_fallback: &'a str,
    pub local_absolute: &'a str,
    pub stale_after: &'a str,
    pub mode: EnforcementMode,
}

impl NginxRuleBuilder<'_> {
    pub fn build(self) -> Result<NginxRuleConfig, NginxConfigError> {
        let limit = parse_rate(self.limit)?;
        let local_fallback_limit = parse_rate(self.local_fallback)?;
        let local_absolute_limit = parse_rate(self.local_absolute)?;
        let window_millis = parse_duration_millis(self.window)?;
        let bucket_millis = parse_duration_millis(self.bucket)?;
        let stale_after_millis = parse_duration_millis(self.stale_after)?;
        if bucket_millis == 0 || window_millis == 0 {
            return Err(NginxConfigError::InvalidDuration);
        }
        let bucket_count = window_millis.div_ceil(bucket_millis);
        if bucket_count == 0 || bucket_count as usize > DEFAULT_MAX_ACTIVE_BUCKETS {
            return Err(NginxConfigError::TooManyBuckets);
        }

        Ok(NginxRuleConfig {
            id: self.id,
            name: FixedName::new(self.name)?,
            domain: FixedName::new(self.domain)?,
            key_components: KeyComponentList::new(self.key_components)?,
            limit,
            window_millis,
            bucket_millis,
            local_fallback_limit,
            local_absolute_limit,
            stale_after_millis,
            mode: self.mode,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NginxVariable<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NginxRequest<'a> {
    pub domain: &'a str,
    pub variables: &'a [NginxVariable<'a>],
    pub hits: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NginxShmLayout {
    pub max_rules: usize,
    pub max_keys: usize,
    pub bucket_count: usize,
    rules_offset: usize,
    request_ring_offset: usize,
    request_events_offset: usize,
    aggregates_offset: usize,
    leader_offset: usize,
    stats_offset: usize,
    total_bytes: usize,
}

impl NginxShmLayout {
    pub fn for_capacity(max_rules: usize, max_keys: usize, bucket_count: usize) -> Option<Self> {
        let max_rules = max_rules.clamp(1, MAX_NGINX_SHM_RULES);
        let max_keys = max_keys.max(1);
        let bucket_count = bucket_count.clamp(1, MAX_NGINX_SHM_BUCKETS);
        let rules_offset = align_up(std::mem::size_of::<StoreHeader>(), align_of_shm())?;
        let rules_bytes = std::mem::size_of::<RuleRuntimeRecord>().checked_mul(max_rules)?;
        let bucket_slots = max_keys.checked_mul(bucket_count)?;
        let request_ring_offset = align_up(rules_offset.checked_add(rules_bytes)?, align_of_shm())?;
        let request_events_offset = align_up(
            request_ring_offset.checked_add(std::mem::size_of::<SharedRequestRingControl>())?,
            align_of_shm(),
        )?;
        let event_bytes =
            std::mem::size_of::<SharedRequestEventRecord>().checked_mul(bucket_slots)?;
        let aggregates_offset = align_up(
            request_events_offset.checked_add(event_bytes)?,
            align_of_shm(),
        )?;
        let aggregates_bytes =
            std::mem::size_of::<SharedCountAggregateRecord>().checked_mul(bucket_slots)?;
        let leader_offset = align_up(
            aggregates_offset.checked_add(aggregates_bytes)?,
            align_of_shm(),
        )?;
        let stats_offset = align_up(
            leader_offset.checked_add(std::mem::size_of::<SharedLeaderLease>())?,
            align_of_shm(),
        )?;
        let total_bytes = align_up(
            stats_offset.checked_add(std::mem::size_of::<SharedStatsCounters>())?,
            align_of_shm(),
        )?;

        Some(Self {
            max_rules,
            max_keys,
            bucket_count,
            rules_offset,
            request_ring_offset,
            request_events_offset,
            aggregates_offset,
            leader_offset,
            stats_offset,
            total_bytes,
        })
    }

    pub fn total_bytes(self) -> usize {
        self.total_bytes
    }

    pub fn aggregate_capacity(self) -> usize {
        self.max_keys * self.bucket_count
    }

    pub fn request_event_capacity(self) -> usize {
        self.max_keys * self.bucket_count
    }
}

fn align_of_shm() -> usize {
    std::mem::align_of::<StoreHeader>()
        .max(std::mem::align_of::<RuleRuntimeRecord>())
        .max(std::mem::align_of::<SharedRequestRingControl>())
        .max(std::mem::align_of::<SharedRequestEventRecord>())
        .max(std::mem::align_of::<SharedCountAggregateRecord>())
        .max(std::mem::align_of::<SharedLeaderLease>())
        .max(std::mem::align_of::<SharedStatsCounters>())
}

fn align_up(value: usize, align: usize) -> Option<usize> {
    let mask = align.checked_sub(1)?;
    value.checked_add(mask).map(|value| value & !mask)
}

#[repr(C)]
#[derive(Debug)]
pub struct RuleRuntimeRecord {
    id: u32,
    limit: u64,
    window_millis: u64,
    bucket_millis: u64,
    key_component_count: u8,
    overflow_policy: u8,
    reserved: [u8; 6],
    key_components: [FixedName<MAX_NAME_BYTES>; MAX_KEY_COMPONENTS],
}

impl RuleRuntimeRecord {
    fn empty() -> Self {
        Self {
            id: 0,
            limit: 0,
            window_millis: 1,
            bucket_millis: 1,
            key_component_count: 0,
            overflow_policy: OverflowPolicyCode::Aggregate as u8,
            reserved: [0; 6],
            key_components: [FixedName::empty(); MAX_KEY_COMPONENTS],
        }
    }

    fn from_config(rule: NginxRuleConfig) -> Self {
        let mut record = Self::empty();
        record.id = rule.id;
        record.limit = rule.limit.max(1);
        record.window_millis = rule.window_millis.max(1);
        record.bucket_millis = rule.bucket_millis.max(1);
        record.key_component_count = rule.key_components.len() as u8;
        for (index, component) in rule.key_components.as_slice().iter().enumerate() {
            record.key_components[index] = component.variable;
        }
        record
    }

    fn key_components(&self) -> &[FixedName<MAX_NAME_BYTES>] {
        &self.key_components[..self.key_component_count as usize]
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SharedCountAggregateRecord {
    rule_id: u32,
    key_hash: u128,
    bucket_start_millis: u64,
    count: u64,
}

impl SharedCountAggregateRecord {
    fn empty() -> Self {
        Self::default()
    }

    fn is_empty(self) -> bool {
        self.count == 0
    }

    fn matches(self, aggregate: CountAggregate) -> bool {
        self.rule_id == aggregate.rule_id
            && self.key_hash == u128::from(aggregate.key_hash)
            && self.bucket_start_millis == aggregate.bucket_start_millis
    }

    fn from_aggregate(aggregate: CountAggregate) -> Self {
        Self {
            rule_id: aggregate.rule_id,
            key_hash: u128::from(aggregate.key_hash),
            bucket_start_millis: aggregate.bucket_start_millis,
            count: aggregate.count,
        }
    }

    pub fn as_aggregate(self) -> CountAggregate {
        CountAggregate {
            rule_id: self.rule_id,
            key_hash: self.key_hash.into(),
            bucket_start_millis: self.bucket_start_millis,
            count: self.count,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct SharedStatsCounters {
    requests: AtomicU64,
    allowed: AtomicU64,
    rejected: AtomicU64,
    overflow_keys: AtomicU64,
}

impl SharedStatsCounters {
    pub fn snapshot(&self) -> StatsCounters {
        StatsCounters {
            requests: self.requests.load(Ordering::Relaxed),
            allowed: self.allowed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            overflow_keys: self.overflow_keys.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverflowPolicyCode {
    Aggregate = 0,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NgxShmAccessError {
    MissingVariable,
    InvalidRule,
    StoreFull,
}

pub trait NginxVariableLookup {
    fn value<'a>(&'a self, name: &str) -> Option<&'a [u8]>;
}

pub trait NginxRequestEventSource {
    fn try_acquire_runtime_leader(&self, worker_id: u32, now_millis: u64, ttl_millis: u64) -> bool;

    fn drain_request_events(&mut self, out: &mut [RequestEvent]) -> usize;
}

pub fn drain_request_events_into_runtime<H: CountUpdateHandler>(
    source: &mut impl NginxRequestEventSource,
    runtime: &Runtime<H>,
    events: &mut [RequestEvent],
    requests: &mut [TimedHashedLimitRequest],
    aggregates: &mut [CountAggregate],
) -> usize {
    let capacity = events.len().min(requests.len());
    if capacity == 0 || aggregates.is_empty() {
        return 0;
    }

    let events = &mut events[..capacity];
    let mut recorded = 0_usize;
    loop {
        let drained = source.drain_request_events(events);
        if drained == 0 {
            break;
        }
        for index in 0..drained {
            requests[index] =
                TimedHashedLimitRequest::new(events[index].as_hashed(), events[index].now_millis);
        }
        recorded = recorded
            .saturating_add(runtime.record_timed_hashed_batch(&requests[..drained], aggregates));
        if drained < events.len() {
            break;
        }
    }
    recorded
}

#[derive(Debug)]
pub struct NgxShmStore {
    ptr: *mut u8,
    layout: NginxShmLayout,
}

impl NgxShmStore {
    pub fn required_bytes(max_rules: usize, max_keys: usize, bucket_count: usize) -> Option<usize> {
        NginxShmLayout::for_capacity(max_rules, max_keys, bucket_count)
            .map(|layout| layout.total_bytes().max(std::mem::size_of::<StoreHeader>()))
    }

    /// # Safety
    ///
    /// `ptr` must point to writable shared memory of at least `len` bytes for
    /// the whole lifetime of this handle. The memory must be shared between
    /// NGINX workers if cross-worker counters are required.
    pub unsafe fn initialize(
        ptr: *mut u8,
        len: usize,
        max_rules: usize,
        max_keys: usize,
        bucket_count: usize,
    ) -> Result<Self, NginxConfigError> {
        let Some(layout) = NginxShmLayout::for_capacity(max_rules, max_keys, bucket_count) else {
            return Err(NginxConfigError::InvalidCapacity);
        };
        if ptr.is_null() || len < layout.total_bytes() {
            return Err(NginxConfigError::InvalidCapacity);
        }
        let mut store = Self { ptr, layout };
        unsafe {
            std::ptr::write_bytes(ptr, 0, layout.total_bytes());
            std::ptr::write(
                store.header_mut(),
                StoreHeader {
                    magic: SHM_MAGIC,
                    version: SHM_VERSION,
                    flags: SHM_LOCK_FREE as u16,
                    zone_bytes: len as u64,
                    max_keys: layout.max_keys as u32,
                    max_rules: layout.max_rules as u32,
                },
            );
            for index in 0..layout.max_rules {
                std::ptr::write(store.rule_mut(index), RuleRuntimeRecord::empty());
            }
            let request_ring = &mut *store.request_ring_mut();
            let request_events = std::slice::from_raw_parts_mut(
                store
                    .ptr
                    .add(layout.request_events_offset)
                    .cast::<SharedRequestEventRecord>(),
                layout.request_event_capacity(),
            );
            SharedRequestQueue::initialize(request_ring, request_events);
            for index in 0..layout.aggregate_capacity() {
                std::ptr::write(
                    store.aggregate_mut(index),
                    SharedCountAggregateRecord::empty(),
                );
            }
            std::ptr::write(store.leader_mut(), SharedLeaderLease::default());
            std::ptr::write(
                store.stats_mut(),
                SharedStatsCounters {
                    requests: AtomicU64::new(0),
                    allowed: AtomicU64::new(0),
                    rejected: AtomicU64::new(0),
                    overflow_keys: AtomicU64::new(0),
                },
            );
        }
        Ok(store)
    }

    /// # Safety
    ///
    /// `ptr` must point to a store previously initialized with
    /// `NgxShmStore::initialize`.
    pub unsafe fn from_initialized(ptr: *mut u8, len: usize) -> Option<Self> {
        if ptr.is_null() || len < std::mem::size_of::<StoreHeader>() {
            return None;
        }
        let header = unsafe { &*(ptr as *const StoreHeader) };
        if header.magic != SHM_MAGIC || header.version != SHM_VERSION {
            return None;
        }
        let layout = NginxShmLayout::for_capacity(
            header.max_rules as usize,
            header.max_keys as usize,
            MAX_NGINX_SHM_BUCKETS,
        )?;
        if len < layout.total_bytes() {
            return None;
        }
        Some(Self { ptr, layout })
    }

    pub fn add_rule(
        &mut self,
        index: usize,
        rule: NginxRuleConfig,
    ) -> Result<(), NginxConfigError> {
        if index >= self.layout.max_rules {
            return Err(NginxConfigError::TooManyRules);
        }
        unsafe {
            std::ptr::write(self.rule_mut(index), RuleRuntimeRecord::from_config(rule));
        }
        Ok(())
    }

    pub fn access(
        &mut self,
        rule_index: usize,
        variables: &impl NginxVariableLookup,
        now_millis: u64,
    ) -> Result<NginxStatus, NgxShmAccessError> {
        if rule_index >= self.layout.max_rules {
            return Err(NgxShmAccessError::InvalidRule);
        }

        let _guard = self.lock();
        let rule = unsafe { &*self.rule_ptr(rule_index) };
        if rule.id == 0 || rule.key_component_count == 0 {
            return Err(NgxShmAccessError::InvalidRule);
        }
        let request = hashed_request_from_variables(rule, variables)?;
        let current = self.aggregate_window_total(rule, request, now_millis);
        self.stats().requests.fetch_add(1, Ordering::Relaxed);
        if current.saturating_add(1) > rule.limit {
            self.stats().rejected.fetch_add(1, Ordering::Relaxed);
            return Ok(NginxStatus::TooManyRequests);
        }

        let event = RequestEvent::from_hashed(request, now_millis);
        self.request_queue()
            .push(event)
            .map_err(|_| NgxShmAccessError::StoreFull)?;
        self.stats().allowed.fetch_add(1, Ordering::Relaxed);
        Ok(NginxStatus::Declined)
    }

    pub fn drain_request_events(&mut self, out: &mut [RequestEvent]) -> usize {
        if out.is_empty() {
            return 0;
        }

        let _guard = self.lock();
        self.request_queue().drain(out)
    }

    pub fn stats_snapshot(&self) -> StatsCounters {
        self.stats().snapshot()
    }

    pub fn apply_count_aggregates(&mut self, aggregates: &[CountAggregate]) -> ApplyBatchOutcome {
        if aggregates.is_empty() {
            return ApplyBatchOutcome::default();
        }

        let _guard = self.lock();
        let mut outcome = ApplyBatchOutcome::default();
        for aggregate in aggregates.iter().copied() {
            if aggregate.count == 0 || aggregate.rule_id == 0 {
                outcome.dropped = outcome.dropped.saturating_add(1);
                continue;
            }
            if self.upsert_count_aggregate(aggregate) {
                outcome.applied = outcome.applied.saturating_add(1);
            } else {
                outcome.dropped = outcome.dropped.saturating_add(1);
            }
        }
        outcome
    }

    pub fn try_acquire_leader(&self, worker_id: u32, now_millis: u64, ttl_millis: u64) -> bool {
        self.leader().try_acquire(worker_id, now_millis, ttl_millis)
    }

    pub fn layout(&self) -> NginxShmLayout {
        self.layout
    }

    fn upsert_count_aggregate(&mut self, aggregate: CountAggregate) -> bool {
        let mut vacant = None;
        for index in 0..self.layout.aggregate_capacity() {
            let stored = unsafe { *self.aggregate_ptr(index) };
            if !stored.is_empty() && stored.matches(aggregate) {
                unsafe {
                    (*self.aggregate_mut(index)).count = aggregate.count;
                }
                return true;
            }
            if stored.is_empty() && vacant.is_none() {
                vacant = Some(index);
            }
        }

        let Some(index) = vacant else {
            return false;
        };
        unsafe {
            std::ptr::write(
                self.aggregate_mut(index),
                SharedCountAggregateRecord::from_aggregate(aggregate),
            );
        }
        true
    }

    fn aggregate_window_total(
        &self,
        rule: &RuleRuntimeRecord,
        request: HashedLimitRequest,
        now_millis: u64,
    ) -> u64 {
        let mut total = 0_u64;
        for index in 0..self.layout.aggregate_capacity() {
            let aggregate = unsafe { *self.aggregate_ptr(index) };
            if aggregate.count == 0
                || aggregate.rule_id != rule.id
                || aggregate.key_hash != request.key_hash().into()
            {
                continue;
            }
            if aggregate
                .bucket_start_millis
                .saturating_add(rule.window_millis)
                > now_millis
            {
                total = total.saturating_add(aggregate.count);
            }
        }
        total
    }

    fn lock(&self) -> ShmLockGuard {
        let lock = self.lock_word() as *const AtomicU16;
        while unsafe { &*lock }
            .compare_exchange(
                SHM_LOCK_FREE,
                SHM_LOCK_HELD,
                Ordering::Acquire,
                Ordering::Relaxed,
            )
            .is_err()
        {
            std::hint::spin_loop();
        }
        ShmLockGuard { lock }
    }

    fn lock_word(&self) -> &AtomicU16 {
        unsafe { &*(std::ptr::addr_of!((*self.header()).flags).cast::<AtomicU16>()) }
    }

    fn header(&self) -> *const StoreHeader {
        self.ptr as *const StoreHeader
    }

    unsafe fn header_mut(&mut self) -> *mut StoreHeader {
        self.ptr as *mut StoreHeader
    }

    fn rule_ptr(&self, index: usize) -> *const RuleRuntimeRecord {
        unsafe {
            self.ptr
                .add(self.layout.rules_offset)
                .cast::<RuleRuntimeRecord>()
                .add(index)
        }
    }

    unsafe fn rule_mut(&mut self, index: usize) -> *mut RuleRuntimeRecord {
        unsafe {
            self.ptr
                .add(self.layout.rules_offset)
                .cast::<RuleRuntimeRecord>()
                .add(index)
        }
    }

    unsafe fn request_ring_mut(&mut self) -> *mut SharedRequestRingControl {
        unsafe {
            self.ptr
                .add(self.layout.request_ring_offset)
                .cast::<SharedRequestRingControl>()
        }
    }

    fn request_queue(&mut self) -> SharedRequestQueue<'_> {
        unsafe {
            let control = &*self
                .ptr
                .add(self.layout.request_ring_offset)
                .cast::<SharedRequestRingControl>();
            let events = std::slice::from_raw_parts_mut(
                self.ptr
                    .add(self.layout.request_events_offset)
                    .cast::<SharedRequestEventRecord>(),
                self.layout.request_event_capacity(),
            );
            SharedRequestQueue::new(control, events)
        }
    }

    fn aggregate_ptr(&self, index: usize) -> *const SharedCountAggregateRecord {
        unsafe {
            self.ptr
                .add(self.layout.aggregates_offset)
                .cast::<SharedCountAggregateRecord>()
                .add(index)
        }
    }

    unsafe fn aggregate_mut(&mut self, index: usize) -> *mut SharedCountAggregateRecord {
        unsafe {
            self.ptr
                .add(self.layout.aggregates_offset)
                .cast::<SharedCountAggregateRecord>()
                .add(index)
        }
    }

    fn leader(&self) -> &SharedLeaderLease {
        unsafe {
            &*self
                .ptr
                .add(self.layout.leader_offset)
                .cast::<SharedLeaderLease>()
        }
    }

    unsafe fn leader_mut(&mut self) -> *mut SharedLeaderLease {
        unsafe {
            self.ptr
                .add(self.layout.leader_offset)
                .cast::<SharedLeaderLease>()
        }
    }

    fn stats(&self) -> &SharedStatsCounters {
        unsafe {
            &*self
                .ptr
                .add(self.layout.stats_offset)
                .cast::<SharedStatsCounters>()
        }
    }

    unsafe fn stats_mut(&mut self) -> *mut SharedStatsCounters {
        unsafe {
            self.ptr
                .add(self.layout.stats_offset)
                .cast::<SharedStatsCounters>()
        }
    }
}

impl NginxRequestEventSource for NgxShmStore {
    fn try_acquire_runtime_leader(&self, worker_id: u32, now_millis: u64, ttl_millis: u64) -> bool {
        self.leader().try_acquire(worker_id, now_millis, ttl_millis)
    }

    fn drain_request_events(&mut self, out: &mut [RequestEvent]) -> usize {
        NgxShmStore::drain_request_events(self, out)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NginxSharedCountHandler {
    ptr: *mut u8,
    len: usize,
}

impl NginxSharedCountHandler {
    /// # Safety
    ///
    /// `ptr` must point to a live `NgxShmStore` mapping for the whole lifetime
    /// of the runtime using this handler.
    pub unsafe fn new(ptr: *mut u8, len: usize) -> Self {
        Self { ptr, len }
    }
}

unsafe impl Send for NginxSharedCountHandler {}
unsafe impl Sync for NginxSharedCountHandler {}

impl CountUpdateHandler for NginxSharedCountHandler {
    fn apply_batch(&self, aggregates: &[CountAggregate]) -> ApplyBatchOutcome {
        let Some(mut store) = (unsafe { NgxShmStore::from_initialized(self.ptr, self.len) }) else {
            return ApplyBatchOutcome {
                applied: 0,
                dropped: aggregates.len(),
            };
        };
        store.apply_count_aggregates(aggregates)
    }
}

fn hashed_request_from_variables(
    rule: &RuleRuntimeRecord,
    variables: &impl NginxVariableLookup,
) -> Result<HashedLimitRequest, NgxShmAccessError> {
    let mut request = HashedLimitRequestBuilder::new(rule.id, 1);
    for component in rule.key_components() {
        let Some(value) = variables.value(component.as_str()) else {
            return Err(NgxShmAccessError::MissingVariable);
        };
        request.push_component(component.as_str().as_bytes(), value);
    }
    Ok(request.finish())
}

struct ShmLockGuard {
    lock: *const AtomicU16,
}

impl Drop for ShmLockGuard {
    fn drop(&mut self) {
        unsafe { &*self.lock }.store(SHM_LOCK_FREE, Ordering::Release);
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreHeader {
    pub magic: u32,
    pub version: u16,
    pub flags: u16,
    pub zone_bytes: u64,
    pub max_keys: u32,
    pub max_rules: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatsCounters {
    pub requests: u64,
    pub allowed: u64,
    pub rejected: u64,
    pub overflow_keys: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LeaderLease {
    pub owner_worker: u32,
    pub expires_millis: u64,
    pub epoch: u64,
}

#[repr(C)]
#[derive(Debug, Default)]
pub struct SharedLeaderLease {
    owner_worker: AtomicU32,
    expires_millis: AtomicU64,
    epoch: AtomicU64,
}

impl SharedLeaderLease {
    pub fn snapshot(&self) -> LeaderLease {
        LeaderLease {
            owner_worker: self.owner_worker.load(Ordering::Acquire),
            expires_millis: self.expires_millis.load(Ordering::Acquire),
            epoch: self.epoch.load(Ordering::Acquire),
        }
    }

    pub fn try_acquire(&self, worker_id: u32, now_millis: u64, ttl_millis: u64) -> bool {
        if worker_id == 0 || ttl_millis == 0 {
            return false;
        }

        let owner = self.owner_worker.load(Ordering::Acquire);
        let expires = self.expires_millis.load(Ordering::Acquire);
        if owner == worker_id {
            self.expires_millis
                .store(now_millis.saturating_add(ttl_millis), Ordering::Release);
            return true;
        }
        if owner != 0 && expires > now_millis {
            return false;
        }

        if self
            .owner_worker
            .compare_exchange(owner, worker_id, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.expires_millis
                .store(now_millis.saturating_add(ttl_millis), Ordering::Release);
            self.epoch.fetch_add(1, Ordering::AcqRel);
            return true;
        }

        false
    }

    pub fn release(&self, worker_id: u32) -> bool {
        self.owner_worker
            .compare_exchange(worker_id, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct NginxPeer {
    family: u8,
    ip: [u8; 16],
    port: u16,
}

impl NginxPeer {
    pub fn new(addr: SocketAddr) -> Self {
        match addr.ip() {
            IpAddr::V4(ip) => {
                let mut bytes = [0_u8; 16];
                bytes[..4].copy_from_slice(&ip.octets());
                Self {
                    family: 4,
                    ip: bytes,
                    port: addr.port(),
                }
            }
            IpAddr::V6(ip) => Self {
                family: 6,
                ip: ip.octets(),
                port: addr.port(),
            },
        }
    }

    pub fn socket_addr(self) -> Option<SocketAddr> {
        match self.family {
            4 => Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(
                    self.ip[0], self.ip[1], self.ip[2], self.ip[3],
                )),
                self.port,
            )),
            6 => Some(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::from(self.ip)),
                self.port,
            )),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NginxPeerTable {
    peers: [NginxPeer; MAX_NGINX_PEERS],
    len: u8,
}

impl NginxPeerTable {
    pub const fn empty() -> Self {
        Self {
            peers: [NginxPeer {
                family: 0,
                ip: [0; 16],
                port: 0,
            }; MAX_NGINX_PEERS],
            len: 0,
        }
    }

    pub fn parse_lines(
        input: &str,
        self_addr: Option<SocketAddr>,
    ) -> Result<Self, NginxPeerConfigError> {
        let mut table = Self::empty();
        for line in input.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let addr = line
                .parse::<SocketAddr>()
                .map_err(|_| NginxPeerConfigError::InvalidPeer)?;
            if Some(addr) != self_addr {
                table.insert(NginxPeer::new(addr))?;
            }
        }
        Ok(table)
    }

    pub fn insert(&mut self, peer: NginxPeer) -> Result<(), NginxPeerConfigError> {
        if self.as_slice().contains(&peer) {
            return Ok(());
        }
        if self.len as usize == MAX_NGINX_PEERS {
            return Err(NginxPeerConfigError::PeerTableFull);
        }

        self.peers[self.len as usize] = peer;
        self.len += 1;
        self.peers[..self.len as usize].sort();
        Ok(())
    }

    pub fn remove(&mut self, peer: NginxPeer) {
        let Some(index) = self.as_slice().iter().position(|stored| *stored == peer) else {
            return;
        };
        let len = self.len as usize;
        for offset in index..len.saturating_sub(1) {
            self.peers[offset] = self.peers[offset + 1];
        }
        self.len = self.len.saturating_sub(1);
    }

    pub fn as_slice(&self) -> &[NginxPeer] {
        &self.peers[..self.len as usize]
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for NginxPeerTable {
    fn default() -> Self {
        Self::empty()
    }
}

pub fn load_peer_file(
    path: impl AsRef<Path>,
    scratch: &mut [u8],
    self_addr: Option<SocketAddr>,
) -> Result<NginxPeerTable, NginxPeerConfigError> {
    if scratch.is_empty() {
        return Err(NginxPeerConfigError::PeerFileTooLarge);
    }

    let mut file = std::fs::File::open(path).map_err(|_| NginxPeerConfigError::PeerFileRead)?;
    let mut len = 0;
    loop {
        if len == scratch.len() {
            let mut extra = [0_u8; 1];
            match file.read(&mut extra) {
                Ok(0) => break,
                Ok(_) => return Err(NginxPeerConfigError::PeerFileTooLarge),
                Err(_) => return Err(NginxPeerConfigError::PeerFileRead),
            }
        }

        match file.read(&mut scratch[len..]) {
            Ok(0) => break,
            Ok(read) => len += read,
            Err(_) => return Err(NginxPeerConfigError::PeerFileRead),
        }
    }

    let input = std::str::from_utf8(&scratch[..len])
        .map_err(|_| NginxPeerConfigError::InvalidPeerFileUtf8)?;
    NginxPeerTable::parse_lines(input, self_addr)
}
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NginxPeerConfigError {
    #[error("invalid peer")]
    InvalidPeer,
    #[error("peer table full")]
    PeerTableFull,
    #[error("peer file read failed")]
    PeerFileRead,
    #[error("peer file too large")]
    PeerFileTooLarge,
    #[error("invalid peer file utf-8")]
    InvalidPeerFileUtf8,
    #[error("invalid payload capacity")]
    InvalidPayloadCapacity,
    #[error("missing EndpointSlice selector")]
    MissingEndpointSliceSelector,
    #[error("kubernetes discovery failed")]
    KubernetesDiscovery,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NginxConfigError {
    #[error("name too long")]
    NameTooLong,
    #[error("invalid capacity")]
    InvalidCapacity,
    #[error("missing key components")]
    NoKeyComponents,
    #[error("too many key components")]
    TooManyKeyComponents,
    #[error("invalid rate")]
    InvalidRate,
    #[error("invalid duration")]
    InvalidDuration,
    #[error("too many buckets")]
    TooManyBuckets,
    #[error("too many EndpointSlice selectors")]
    TooManyEndpointSliceSelectors,
    #[error("too many rules")]
    TooManyRules,
    #[error("no rules")]
    NoRules,
    #[error("invalid runtime config")]
    RuntimeConfig,
}

pub fn parse_size_bytes(input: &str) -> Result<usize, NginxConfigError> {
    let input = input.trim();
    let split = input
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(input.len());
    let (number, unit) = input.split_at(split);
    let value = number
        .parse::<usize>()
        .map_err(|_| NginxConfigError::InvalidCapacity)?;
    match unit.trim().to_ascii_lowercase().as_str() {
        "" => Ok(value),
        "k" | "kb" => value
            .checked_mul(1024)
            .ok_or(NginxConfigError::InvalidCapacity),
        "m" | "mb" => value
            .checked_mul(1024 * 1024)
            .ok_or(NginxConfigError::InvalidCapacity),
        "g" | "gb" => value
            .checked_mul(1024 * 1024 * 1024)
            .ok_or(NginxConfigError::InvalidCapacity),
        _ => Err(NginxConfigError::InvalidCapacity),
    }
}

pub fn parse_duration_millis(input: &str) -> Result<u64, NginxConfigError> {
    let input = input.trim();
    let split = input
        .find(|ch: char| !ch.is_ascii_digit())
        .ok_or(NginxConfigError::InvalidDuration)?;
    let (number, unit) = input.split_at(split);
    let value = number
        .parse::<u64>()
        .map_err(|_| NginxConfigError::InvalidDuration)?;
    match unit.trim() {
        "ms" => Ok(value),
        "s" => value
            .checked_mul(1_000)
            .ok_or(NginxConfigError::InvalidDuration),
        "m" => value
            .checked_mul(60_000)
            .ok_or(NginxConfigError::InvalidDuration),
        "h" => value
            .checked_mul(3_600_000)
            .ok_or(NginxConfigError::InvalidDuration),
        _ => Err(NginxConfigError::InvalidDuration),
    }
}

pub fn parse_rate(input: &str) -> Result<u64, NginxConfigError> {
    let input = input.trim();
    let Some((number, _unit)) = input.split_once("r/") else {
        return Err(NginxConfigError::InvalidRate);
    };
    number
        .parse::<u64>()
        .map_err(|_| NginxConfigError::InvalidRate)
}

#[cfg(test)]
mod tests;
