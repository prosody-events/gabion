//! NGINX request-path adapter, bounded peer tables, and embedded gossip pieces.
//!
//! Invariants:
//! - Request-path local limiting performs no network or Kubernetes I/O.
//! - Missing variables decline without allocating or tracking a key.
//! - Peer tables are sorted, deduplicated, bounded, and exclude self.
//! - Peer-file loading uses caller-provided scratch memory and rejects
//!   oversized files.
//! - Embedded gossip sends only from the elected owner and uses caller-owned
//!   buffers.
//! - Embedded gossip receive decodes through visitor callbacks and rejects
//!   invalid frames before mutating cell state.
//! - Kubernetes selector config is bounded and defaults the gossip port name
//!   consistently.

use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::path::Path;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};

use gabion_core::{
    Decision, Descriptor, DescriptorMatcher, EnforcementMode, LimitRequest, LocalEngine,
    OverflowPolicy, Rule, RuleId, RuleTable, SafetyMargin, WindowSpec, hash_domain,
};
use gabion_discovery::{DEFAULT_GOSSIP_PORT_NAME, DiscoveryMode};
use gabion_gossip::{
    CellTable, CounterCell, DecodeError, GossipHeader, GossipLimits, GossipMessage, HmacKey,
    NodeId, decode_authenticated_message_visit_checked, decode_message_visit_checked,
    encode_authenticated_message, encode_message,
};
use thiserror::Error;

#[cfg(feature = "ngx-module")]
mod module;

pub const MAX_NAME_BYTES: usize = 64;
pub const MAX_KEY_COMPONENTS: usize = 8;
pub const MAX_NGINX_PEERS: usize = 64;
pub const MAX_ENDPOINT_SLICE_SELECTORS: usize = 16;
pub const MAX_NGINX_SHM_RULES: usize = 16;
pub const MAX_NGINX_SHM_BUCKETS: usize = DEFAULT_MAX_ACTIVE_BUCKETS;
pub const DEFAULT_MAX_ACTIVE_BUCKETS: usize = 64;
pub const DEFAULT_GOSSIP_PAYLOAD_BYTES: usize = 64 * 1024;
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
pub enum GossipMode {
    Off,
    FilePeers,
    Embedded,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NginxDiscoveryConfig {
    pub kind: DiscoveryMode,
    pub self_addr: Option<SocketAddr>,
    pub endpoint_slices: NginxEndpointSliceSelectors,
}

impl NginxDiscoveryConfig {
    pub fn local_default() -> Self {
        Self {
            kind: DiscoveryMode::default(),
            self_addr: None,
            endpoint_slices: NginxEndpointSliceSelectors::empty(),
        }
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

    pub fn auto_incluster_client(&self) -> Option<kube::Client> {
        if self.kind != DiscoveryMode::Auto {
            return None;
        }
        gabion_discovery::kubernetes::incluster_client()
    }
}

impl Default for NginxDiscoveryConfig {
    fn default() -> Self {
        Self::local_default()
    }
}

pub async fn load_kubernetes_peer_table(
    client: kube::Client,
    discovery: &NginxDiscoveryConfig,
) -> Result<NginxPeerTable, NginxGossipError> {
    let configs = if let Some(configs) = endpoint_slice_configs_from_nginx_discovery(discovery) {
        configs
    } else {
        gabion_discovery::kubernetes::running_service_endpoint_slice_configs(
            client.clone(),
            discovery.self_addr,
        )
        .await
        .map_err(|_| NginxGossipError::KubernetesDiscovery)?
    };
    let peers = gabion_discovery::kubernetes::initial_endpoint_slice_snapshots(client, &configs)
        .await
        .map_err(|_| NginxGossipError::KubernetesDiscovery)?;
    let mut table = NginxPeerTable::empty();

    for peer in peers {
        table.insert(NginxPeer::new(peer.addr))?;
    }

    Ok(table)
}

fn endpoint_slice_configs_from_nginx_discovery(
    discovery: &NginxDiscoveryConfig,
) -> Option<Vec<gabion_discovery::kubernetes::EndpointSliceDiscoveryConfig>> {
    if discovery.endpoint_slices.is_empty() {
        return None;
    }

    Some(
        discovery
            .endpoint_slices
            .as_slice()
            .iter()
            .map(
                |selector| gabion_discovery::kubernetes::EndpointSliceDiscoveryConfig {
                    namespace: selector.namespace.as_str().to_string(),
                    service_name: selector.service_name.as_str().to_string(),
                    port_name: Some(selector.port_name.as_str().to_string()),
                    self_addr: discovery.self_addr,
                },
            )
            .collect(),
    )
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
    pub gossip: GossipMode,
}

impl NginxRuleConfig {
    pub fn to_core_rule(&self) -> Rule {
        Rule {
            id: self.id,
            domain_hash: hash_domain(self.domain.as_str()),
            descriptor_matcher: DescriptorMatcher::exact_keys(
                self.key_components
                    .as_slice()
                    .iter()
                    .map(|component| component.variable.as_str()),
            ),
            limit: self.limit,
            window: WindowSpec {
                size_millis: self.window_millis,
                bucket_count: self
                    .window_millis
                    .div_ceil(self.bucket_millis.max(1))
                    .max(1) as usize,
            },
            local_fallback_limit: self.local_fallback_limit,
            local_absolute_limit: self.local_absolute_limit,
            stale_after_millis: self.stale_after_millis,
            safety_margin: SafetyMargin { hits: 0 },
            overflow_policy: OverflowPolicy::UseOverflowKey,
            mode: self.mode,
        }
    }
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
            gossip: GossipMode::Off,
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

impl NginxRequest<'_> {
    fn variable(&self, name: &str) -> Option<&str> {
        self.variables
            .iter()
            .find(|variable| variable.name == name)
            .map(|variable| variable.value)
    }
}

pub struct NginxLocalOnlyAdapter {
    engine: LocalEngine,
    rule: NginxRuleConfig,
}

impl NginxLocalOnlyAdapter {
    pub fn new(zone: NginxZoneConfig, rule: NginxRuleConfig) -> Self {
        let core_rule = rule.to_core_rule();
        let bucket_count = core_rule.window.bucket_count;
        Self {
            engine: LocalEngine::new(RuleTable::new(vec![core_rule]), zone.max_keys, bucket_count),
            rule,
        }
    }

    pub fn access_phase(&mut self, request: NginxRequest<'_>, now_millis: u64) -> NginxStatus {
        let mut descriptors = [Descriptor { key: "", value: "" }; MAX_KEY_COMPONENTS];
        let mut count = 0;

        for component in self.rule.key_components.as_slice() {
            let key = component.variable.as_str();
            let Some(value) = request.variable(key) else {
                return NginxStatus::Declined;
            };
            descriptors[count] = Descriptor { key, value };
            count += 1;
        }

        let limit_request = LimitRequest {
            domain: request.domain,
            descriptors: &descriptors[..count],
            hits: request.hits,
        };
        NginxStatus::from_decision(self.engine.check_and_record(limit_request, now_millis))
    }

    pub fn active_keys(&self) -> usize {
        self.engine.active_keys()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NginxShmLayout {
    pub max_rules: usize,
    pub max_keys: usize,
    pub bucket_count: usize,
    rules_offset: usize,
    keys_offset: usize,
    buckets_offset: usize,
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
        let keys_offset = align_up(rules_offset.checked_add(rules_bytes)?, align_of_shm())?;
        let keys_bytes = std::mem::size_of::<KeyRuntimeRecord>().checked_mul(max_keys)?;
        let buckets_offset = align_up(keys_offset.checked_add(keys_bytes)?, align_of_shm())?;
        let bucket_slots = max_keys.checked_mul(bucket_count)?;
        let buckets_bytes = std::mem::size_of::<BucketRuntimeRecord>().checked_mul(bucket_slots)?;
        let stats_offset = align_up(buckets_offset.checked_add(buckets_bytes)?, align_of_shm())?;
        let total_bytes = align_up(
            stats_offset.checked_add(std::mem::size_of::<SharedStatsCounters>())?,
            align_of_shm(),
        )?;

        Some(Self {
            max_rules,
            max_keys,
            bucket_count,
            rules_offset,
            keys_offset,
            buckets_offset,
            stats_offset,
            total_bytes,
        })
    }

    pub fn total_bytes(self) -> usize {
        self.total_bytes
    }
}

fn align_of_shm() -> usize {
    std::mem::align_of::<StoreHeader>()
        .max(std::mem::align_of::<RuleRuntimeRecord>())
        .max(std::mem::align_of::<KeyRuntimeRecord>())
        .max(std::mem::align_of::<BucketRuntimeRecord>())
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
#[derive(Debug)]
pub struct KeyRuntimeRecord {
    rule_id: u32,
    hash: u64,
    occupied: u8,
    overflow: u8,
    reserved: [u8; 6],
    local_window_total: u64,
}

impl KeyRuntimeRecord {
    fn empty() -> Self {
        Self {
            rule_id: 0,
            hash: 0,
            occupied: 0,
            overflow: 0,
            reserved: [0; 6],
            local_window_total: 0,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct BucketRuntimeRecord {
    start_millis: u64,
    count: u64,
}

impl BucketRuntimeRecord {
    fn empty() -> Self {
        Self {
            start_millis: 0,
            count: 0,
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
            for index in 0..layout.max_keys {
                std::ptr::write(store.key_mut(index), KeyRuntimeRecord::empty());
            }
            for index in 0..layout.max_keys * layout.bucket_count {
                std::ptr::write(store.bucket_mut(index), BucketRuntimeRecord::empty());
            }
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
        let mut hash = ShmHasher::new();
        for component in rule.key_components() {
            let Some(value) = variables.value(component.as_str()) else {
                return Err(NgxShmAccessError::MissingVariable);
            };
            hash.write(component.as_str().as_bytes());
            hash.write(&[0]);
            hash.write(value);
            hash.write(&[0xff]);
        }
        let hash = hash.finish();
        let key_index = self.find_or_insert_key(rule, hash)?;
        let bucket_start = bucket_start(now_millis, rule.bucket_millis);
        self.rotate_key_buckets(key_index, rule, now_millis);
        let current = unsafe { (*self.key_mut(key_index)).local_window_total };
        self.stats().requests.fetch_add(1, Ordering::Relaxed);
        if current.saturating_add(1) > rule.limit {
            self.stats().rejected.fetch_add(1, Ordering::Relaxed);
            return Ok(NginxStatus::TooManyRequests);
        }
        self.increment_bucket(key_index, bucket_start);
        unsafe {
            (*self.key_mut(key_index)).local_window_total = (*self.key_mut(key_index))
                .local_window_total
                .saturating_add(1);
        }
        self.stats().allowed.fetch_add(1, Ordering::Relaxed);
        Ok(NginxStatus::Declined)
    }

    pub fn stats_snapshot(&self) -> StatsCounters {
        self.stats().snapshot()
    }

    pub fn layout(&self) -> NginxShmLayout {
        self.layout
    }

    fn find_or_insert_key(
        &mut self,
        rule: &RuleRuntimeRecord,
        hash: u64,
    ) -> Result<usize, NgxShmAccessError> {
        let mut vacant = None;
        for index in 0..self.layout.max_keys {
            let key = unsafe { &*self.key_ptr(index) };
            if key.occupied != 0 && key.rule_id == rule.id && key.hash == hash {
                return Ok(index);
            }
            if key.occupied == 0 && vacant.is_none() {
                vacant = Some(index);
            }
        }

        if let Some(index) = vacant {
            unsafe {
                std::ptr::write(
                    self.key_mut(index),
                    KeyRuntimeRecord {
                        rule_id: rule.id,
                        hash,
                        occupied: 1,
                        overflow: 0,
                        reserved: [0; 6],
                        local_window_total: 0,
                    },
                );
                self.clear_buckets(index);
            }
            return Ok(index);
        }

        self.stats().overflow_keys.fetch_add(1, Ordering::Relaxed);
        Ok((rule.id as usize - 1) % self.layout.max_keys)
    }

    fn rotate_key_buckets(&mut self, key_index: usize, rule: &RuleRuntimeRecord, now_millis: u64) {
        let mut total = 0_u64;
        for offset in 0..self.layout.bucket_count {
            let bucket = unsafe { &mut *self.bucket_for_key_mut(key_index, offset) };
            if bucket.count != 0
                && bucket.start_millis.saturating_add(rule.window_millis) <= now_millis
            {
                *bucket = BucketRuntimeRecord::empty();
            }
            total = total.saturating_add(bucket.count);
        }
        unsafe {
            (*self.key_mut(key_index)).local_window_total = total;
        }
    }

    fn increment_bucket(&mut self, key_index: usize, bucket_start: u64) {
        let mut target = 0;
        for offset in 0..self.layout.bucket_count {
            let bucket = unsafe { &mut *self.bucket_for_key_mut(key_index, offset) };
            if bucket.count == 0 || bucket.start_millis == bucket_start {
                target = offset;
                break;
            }
            if bucket.start_millis
                < unsafe { (*self.bucket_for_key_mut(key_index, target)).start_millis }
            {
                target = offset;
            }
        }
        let bucket = unsafe { &mut *self.bucket_for_key_mut(key_index, target) };
        if bucket.start_millis != bucket_start {
            *bucket = BucketRuntimeRecord {
                start_millis: bucket_start,
                count: 0,
            };
        }
        bucket.count = bucket.count.saturating_add(1);
    }

    unsafe fn clear_buckets(&mut self, key_index: usize) {
        for offset in 0..self.layout.bucket_count {
            unsafe {
                std::ptr::write(
                    self.bucket_for_key_mut(key_index, offset),
                    BucketRuntimeRecord::empty(),
                );
            }
        }
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

    fn key_ptr(&self, index: usize) -> *const KeyRuntimeRecord {
        unsafe {
            self.ptr
                .add(self.layout.keys_offset)
                .cast::<KeyRuntimeRecord>()
                .add(index)
        }
    }

    unsafe fn key_mut(&mut self, index: usize) -> *mut KeyRuntimeRecord {
        unsafe {
            self.ptr
                .add(self.layout.keys_offset)
                .cast::<KeyRuntimeRecord>()
                .add(index)
        }
    }

    unsafe fn bucket_mut(&mut self, index: usize) -> *mut BucketRuntimeRecord {
        unsafe {
            self.ptr
                .add(self.layout.buckets_offset)
                .cast::<BucketRuntimeRecord>()
                .add(index)
        }
    }

    unsafe fn bucket_for_key_mut(
        &mut self,
        key_index: usize,
        bucket_offset: usize,
    ) -> *mut BucketRuntimeRecord {
        unsafe { self.bucket_mut(key_index * self.layout.bucket_count + bucket_offset) }
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

struct ShmLockGuard {
    lock: *const AtomicU16,
}

impl Drop for ShmLockGuard {
    fn drop(&mut self) {
        unsafe { &*self.lock }.store(SHM_LOCK_FREE, Ordering::Release);
    }
}

struct ShmHasher {
    state: u64,
}

impl ShmHasher {
    fn new() -> Self {
        Self {
            state: 0xcbf2_9ce4_8422_2325,
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish(self) -> u64 {
        self.state
    }
}

fn bucket_start(now_millis: u64, bucket_millis: u64) -> u64 {
    let bucket_millis = bucket_millis.max(1);
    now_millis / bucket_millis * bucket_millis
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
    ) -> Result<Self, NginxGossipError> {
        let mut table = Self::empty();
        for line in input.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let addr = line
                .parse::<SocketAddr>()
                .map_err(|_| NginxGossipError::InvalidPeer)?;
            if Some(addr) != self_addr {
                table.insert(NginxPeer::new(addr))?;
            }
        }
        Ok(table)
    }

    pub fn insert(&mut self, peer: NginxPeer) -> Result<(), NginxGossipError> {
        if self.as_slice().contains(&peer) {
            return Ok(());
        }
        if self.len as usize == MAX_NGINX_PEERS {
            return Err(NginxGossipError::PeerTableFull);
        }

        self.peers[self.len as usize] = peer;
        self.len += 1;
        self.peers[..self.len as usize].sort();
        Ok(())
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
) -> Result<NginxPeerTable, NginxGossipError> {
    if scratch.is_empty() {
        return Err(NginxGossipError::PeerFileTooLarge);
    }

    let mut file = std::fs::File::open(path).map_err(|_| NginxGossipError::PeerFileRead)?;
    let mut len = 0;
    loop {
        if len == scratch.len() {
            let mut extra = [0_u8; 1];
            match file.read(&mut extra) {
                Ok(0) => break,
                Ok(_) => return Err(NginxGossipError::PeerFileTooLarge),
                Err(_) => return Err(NginxGossipError::PeerFileRead),
            }
        }

        match file.read(&mut scratch[len..]) {
            Ok(0) => break,
            Ok(read) => len += read,
            Err(_) => return Err(NginxGossipError::PeerFileRead),
        }
    }

    let input =
        std::str::from_utf8(&scratch[..len]).map_err(|_| NginxGossipError::InvalidPeerFileUtf8)?;
    NginxPeerTable::parse_lines(input, self_addr)
}

#[derive(Debug)]
pub struct NginxGossipBuffers {
    send: Vec<u8>,
    recv: Vec<u8>,
    max_payload_bytes: usize,
}

impl NginxGossipBuffers {
    pub fn with_capacity(max_payload_bytes: usize) -> Result<Self, NginxGossipError> {
        if max_payload_bytes == 0 {
            return Err(NginxGossipError::InvalidPayloadCapacity);
        }
        Ok(Self {
            send: Vec::with_capacity(max_payload_bytes),
            recv: vec![0; max_payload_bytes],
            max_payload_bytes,
        })
    }

    pub fn send_buffer(&self) -> &[u8] {
        &self.send
    }

    pub fn recv_buffer_mut(&mut self) -> &mut [u8] {
        self.recv.as_mut_slice()
    }

    pub fn recv_capacity(&self) -> usize {
        self.recv.len()
    }
}

pub trait NginxGossipTransport {
    fn send(&mut self, peer: NginxPeer, payload: &[u8]) -> bool;

    fn recv(&mut self, _buffer: &mut [u8]) -> Option<(NginxPeer, usize)> {
        None
    }
}

#[derive(Debug)]
pub struct NginxUdpTransport {
    socket: UdpSocket,
}

impl NginxUdpTransport {
    pub fn bind(addr: SocketAddr) -> Result<Self, NginxUdpError> {
        let socket = UdpSocket::bind(addr).map_err(NginxUdpError::Bind)?;
        socket
            .set_nonblocking(true)
            .map_err(NginxUdpError::Configure)?;
        Ok(Self { socket })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, NginxUdpError> {
        self.socket.local_addr().map_err(NginxUdpError::LocalAddr)
    }
}

impl NginxGossipTransport for NginxUdpTransport {
    fn send(&mut self, peer: NginxPeer, payload: &[u8]) -> bool {
        let Some(addr) = peer.socket_addr() else {
            return false;
        };
        matches!(self.socket.send_to(payload, addr), Ok(sent) if sent == payload.len())
    }

    fn recv(&mut self, buffer: &mut [u8]) -> Option<(NginxPeer, usize)> {
        match self.socket.recv_from(buffer) {
            Ok((len, addr)) => Some((NginxPeer::new(addr), len)),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(_) => None,
        }
    }
}

#[derive(Debug, Error)]
pub enum NginxUdpError {
    #[error("failed to bind UDP socket: {0}")]
    Bind(std::io::Error),
    #[error("failed to configure UDP socket: {0}")]
    Configure(std::io::Error),
    #[error("failed to read local UDP address: {0}")]
    LocalAddr(std::io::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NginxEmbeddedGossip {
    pub cluster_id_hash: u64,
    pub node_id: NodeId,
    pub incarnation: u64,
    pub lease_ttl_millis: u64,
    pub auth_key: Option<HmacKey>,
}

impl NginxEmbeddedGossip {
    pub fn tick(
        self,
        worker_id: u32,
        now_millis: u64,
        peers: &NginxPeerTable,
        lease: &SharedLeaderLease,
        buffers: &mut NginxGossipBuffers,
        transport: &mut impl NginxGossipTransport,
    ) -> GossipTickOutcome {
        if peers.is_empty() {
            return GossipTickOutcome::LocalOnlyNoPeers;
        }
        if !lease.try_acquire(worker_id, now_millis, self.lease_ttl_millis) {
            return GossipTickOutcome::LocalOnlyNoLeader;
        }

        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: self.cluster_id_hash,
                sender_node_id: self.node_id,
                sender_incarnation: self.incarnation,
                min_bucket: 0,
                max_bucket: 0,
                flags: 0,
            },
            digests: Vec::new(),
            cells: Vec::new(),
            truncated: false,
        };

        let truncated = if let Some(key) = self.auth_key {
            encode_authenticated_message(
                &message,
                key,
                &mut buffers.send,
                GossipLimits {
                    max_payload_bytes: buffers.max_payload_bytes,
                    max_digests: 64,
                    max_cells: 0,
                },
            )
        } else {
            encode_message(&message, &mut buffers.send, buffers.max_payload_bytes)
        };
        let mut sent = 0_u16;
        let mut failed = 0_u16;
        for peer in peers.as_slice() {
            if transport.send(*peer, buffers.send.as_slice()) {
                sent = sent.saturating_add(1);
            } else {
                failed = failed.saturating_add(1);
            }
        }

        GossipTickOutcome::Sent {
            peers: sent,
            failed,
            bytes: buffers.send.len(),
            truncated,
        }
    }

    pub fn receive_one(
        self,
        now_millis: u64,
        cell_table: &mut CellTable,
        engine: Option<&mut LocalEngine>,
        buffers: &mut NginxGossipBuffers,
        transport: &mut impl NginxGossipTransport,
    ) -> GossipReceiveOutcome {
        let Some((_peer, len)) = transport.recv(buffers.recv_buffer_mut()) else {
            return GossipReceiveOutcome::NoPacket;
        };
        if len > buffers.max_payload_bytes || len > buffers.recv.len() {
            return GossipReceiveOutcome::DecodeError(DecodeError::PayloadTooLarge);
        }

        let mut merged = 0_u16;
        let mut stale = 0_u16;
        let mut full = false;
        let mut engine = engine;
        let mut header_outcome = HeaderOutcome::Accepted;
        let limits = GossipLimits {
            max_payload_bytes: buffers.max_payload_bytes,
            max_digests: 64,
            max_cells: cell_table.capacity(),
        };
        let accept_header = |header: GossipHeader| {
            if header.cluster_id_hash != self.cluster_id_hash {
                header_outcome = HeaderOutcome::WrongCluster;
                return false;
            }
            if header.sender_node_id == self.node_id {
                header_outcome = HeaderOutcome::SelfPacket;
                return false;
            }
            true
        };
        let on_digest = |_| {};
        let mut on_cell = |cell: CounterCell| {
            if full {
                return;
            }
            if cell.origin_node_id == self.node_id && cell.origin_incarnation == self.incarnation {
                stale = stale.saturating_add(1);
                return;
            }
            match cell_table.merge_remote(cell, engine.as_deref_mut(), now_millis) {
                Ok(outcome) => {
                    if outcome.changed {
                        merged = merged.saturating_add(1);
                    } else {
                        stale = stale.saturating_add(1);
                    }
                }
                Err(_) => full = true,
            }
        };
        let result = if let Some(key) = self.auth_key {
            decode_authenticated_message_visit_checked(
                &buffers.recv[..len],
                key,
                limits,
                accept_header,
                on_digest,
                &mut on_cell,
            )
        } else {
            decode_message_visit_checked(
                &buffers.recv[..len],
                limits,
                accept_header,
                on_digest,
                &mut on_cell,
            )
        };

        match result {
            Ok(_) if header_outcome == HeaderOutcome::WrongCluster => {
                GossipReceiveOutcome::WrongCluster
            }
            Ok(_) if header_outcome == HeaderOutcome::SelfPacket => {
                GossipReceiveOutcome::SelfPacket
            }
            Ok(_) if full => GossipReceiveOutcome::CellTableFull,
            Ok(_) => GossipReceiveOutcome::Merged { merged, stale },
            Err(error) => GossipReceiveOutcome::DecodeError(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeaderOutcome {
    Accepted,
    WrongCluster,
    SelfPacket,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GossipTickOutcome {
    LocalOnlyNoPeers,
    LocalOnlyNoLeader,
    Sent {
        peers: u16,
        failed: u16,
        bytes: usize,
        truncated: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GossipReceiveOutcome {
    NoPacket,
    WrongCluster,
    SelfPacket,
    CellTableFull,
    DecodeError(DecodeError),
    Merged { merged: u16, stale: u16 },
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NginxGossipError {
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
    #[error("invalid gossip payload capacity")]
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
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen, TestResult};
    use quickcheck_macros::quickcheck;

    #[derive(Clone, Debug)]
    struct NginxPeerTableCase {
        self_octet: u8,
        peers: Vec<u8>,
    }

    impl Arbitrary for NginxPeerTableCase {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut peers = Vec::<u8>::arbitrary(g);
            peers.truncate(MAX_NGINX_PEERS + 16);
            Self {
                self_octet: (u8::arbitrary(g) % 64).saturating_add(1),
                peers,
            }
        }
    }

    #[derive(Clone, Debug)]
    struct NginxAccessCase {
        attempts: u8,
        missing_variable: bool,
    }

    impl Arbitrary for NginxAccessCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                attempts: (u8::arbitrary(g) % 16).max(1),
                missing_variable: bool::arbitrary(g),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct NginxEmbeddedSendCase {
        peer_count: u8,
        leader_worker: u32,
        contender_worker: u32,
    }

    impl Arbitrary for NginxEmbeddedSendCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                peer_count: u8::arbitrary(g) % 8,
                leader_worker: (u32::arbitrary(g) % 16).saturating_add(1),
                contender_worker: (u32::arbitrary(g) % 16).saturating_add(1),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct NginxPeerFileCase {
        peers: Vec<u8>,
        scratch_extra: u8,
    }

    impl Arbitrary for NginxPeerFileCase {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut peers = Vec::<u8>::arbitrary(g);
            peers.truncate(12);
            Self {
                peers,
                scratch_extra: u8::arbitrary(g) % 8,
            }
        }
    }

    #[derive(Clone, Debug)]
    struct NginxReceiveRejectCase {
        wrong_cluster: bool,
        self_sender: bool,
        tamper_auth: bool,
        count: u8,
    }

    impl Arbitrary for NginxReceiveRejectCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                wrong_cluster: bool::arbitrary(g),
                self_sender: bool::arbitrary(g),
                tamper_auth: bool::arbitrary(g),
                count: (u8::arbitrary(g) % 8).max(1),
            }
        }
    }

    fn rule() -> NginxRuleConfig {
        NginxRuleBuilder {
            id: 1,
            name: "tenant_api",
            domain: "api",
            key_components: &["$tenant", "$uri"],
            limit: "10r/m",
            window: "60s",
            bucket: "1s",
            local_fallback: "3r/m",
            local_absolute: "6r/m",
            stale_after: "2s",
            mode: EnforcementMode::Enforce,
        }
        .build()
        .expect("rule")
    }

    #[test]
    fn parses_nginx_rule_config() {
        let rule = rule();

        assert_eq!(rule.name.as_str(), "tenant_api");
        assert_eq!(rule.key_components.len(), 2);
        assert_eq!(rule.limit, 10);
        assert_eq!(rule.window_millis, 60_000);
    }

    #[test]
    fn nginx_discovery_defaults_to_auto_and_stores_kubernetes_selectors_bounded() {
        let mut discovery = NginxDiscoveryConfig {
            kind: DiscoveryMode::KubernetesEndpointSlice,
            ..Default::default()
        };

        discovery
            .add_endpoint_slice("default", "gabion-grpc", "gossip")
            .expect("grpc selector");
        discovery
            .add_endpoint_slice("default", "gabion-nginx", "gossip")
            .expect("nginx selector");

        assert_eq!(NginxDiscoveryConfig::default().kind, DiscoveryMode::Auto);
        assert_eq!(discovery.endpoint_slices.len(), 2);
        assert_eq!(
            discovery.endpoint_slices.as_slice()[0]
                .service_name
                .as_str(),
            "gabion-grpc"
        );
        assert_eq!(
            discovery.endpoint_slices.as_slice()[1]
                .service_name
                .as_str(),
            "gabion-nginx"
        );
    }

    #[test]
    fn nginx_endpoint_slice_selector_defaults_empty_port_name_to_gossip() {
        let selector =
            NginxEndpointSliceSelector::new("default", "gabion-grpc", "").expect("selector");

        assert_eq!(selector.port_name.as_str(), "gossip");
    }

    #[test]
    fn nginx_discovery_rejects_too_many_endpoint_slice_selectors() {
        let mut selectors = NginxEndpointSliceSelectors::empty();
        for index in 0..MAX_ENDPOINT_SLICE_SELECTORS {
            selectors
                .push(
                    NginxEndpointSliceSelector::new(
                        "default",
                        "gabion-grpc",
                        &format!("gossip-{index}"),
                    )
                    .expect("selector"),
                )
                .expect("push selector");
        }

        let error = selectors.push(
            NginxEndpointSliceSelector::new("default", "gabion-nginx", "gossip").expect("selector"),
        );

        assert_eq!(error, Err(NginxConfigError::TooManyEndpointSliceSelectors));
    }

    #[test]
    fn nginx_kubernetes_discovery_uses_shared_inference_when_selectors_are_empty() {
        let empty = NginxDiscoveryConfig {
            kind: DiscoveryMode::KubernetesEndpointSlice,
            ..Default::default()
        };
        assert!(endpoint_slice_configs_from_nginx_discovery(&empty).is_none());

        let mut explicit = empty;
        explicit
            .add_endpoint_slice("default", "gabion-nginx", "gossip")
            .expect("selector");
        let configs = endpoint_slice_configs_from_nginx_discovery(&explicit).expect("configs");

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].namespace, "default");
        assert_eq!(configs[0].service_name, "gabion-nginx");
        assert_eq!(configs[0].port_name.as_deref(), Some("gossip"));
    }

    #[test]
    fn rejects_too_many_key_components() {
        let keys = ["a", "b", "c", "d", "e", "f", "g", "h", "i"];

        assert_eq!(
            KeyComponentList::new(&keys),
            Err(NginxConfigError::TooManyKeyComponents)
        );
    }

    #[test]
    fn local_only_access_phase_allows_then_rejects_without_request_allocation() {
        let zone = NginxZoneConfig::new("api", 128 * 1024 * 1024, 16).expect("zone");
        let mut adapter = NginxLocalOnlyAdapter::new(zone, rule());
        let variables = [
            NginxVariable {
                name: "tenant",
                value: "a",
            },
            NginxVariable {
                name: "uri",
                value: "/v1",
            },
        ];
        let request = NginxRequest {
            domain: "api",
            variables: &variables,
            hits: 1,
        };

        assert_eq!(adapter.access_phase(request, 0), NginxStatus::Declined);
        assert_eq!(adapter.access_phase(request, 1), NginxStatus::Declined);
        assert_eq!(adapter.access_phase(request, 2), NginxStatus::Declined);
        assert_eq!(
            adapter.access_phase(request, 3),
            NginxStatus::TooManyRequests
        );
        assert_eq!(adapter.active_keys(), 1);
    }

    #[test]
    fn missing_variable_declines_without_tracking() {
        let zone = NginxZoneConfig::new("api", 128 * 1024 * 1024, 16).expect("zone");
        let mut adapter = NginxLocalOnlyAdapter::new(zone, rule());
        let variables = [NginxVariable {
            name: "tenant",
            value: "a",
        }];
        let request = NginxRequest {
            domain: "api",
            variables: &variables,
            hits: 1,
        };

        assert_eq!(adapter.access_phase(request, 0), NginxStatus::Declined);
        assert_eq!(adapter.active_keys(), 0);
    }

    #[test]
    fn shared_memory_records_are_c_layout_copy_types() {
        assert_eq!(std::mem::size_of::<StoreHeader>(), 24);
        assert_eq!(std::mem::size_of::<StatsCounters>(), 32);
        assert_eq!(std::mem::size_of::<LeaderLease>(), 24);
    }

    fn shm_rule(name: &str, limit: &str, keys: &[&str]) -> NginxRuleConfig {
        NginxRuleBuilder {
            id: 1,
            name,
            domain: "nginx",
            key_components: keys,
            limit,
            window: "100ms",
            bucket: "10ms",
            local_fallback: limit,
            local_absolute: limit,
            stale_after: "2s",
            mode: EnforcementMode::Enforce,
        }
        .build()
        .expect("shared memory rule")
    }

    fn shm_store(bytes: &mut [u8], rule: NginxRuleConfig, max_keys: usize) -> NgxShmStore {
        let mut store = unsafe {
            NgxShmStore::initialize(
                bytes.as_mut_ptr(),
                bytes.len(),
                MAX_NGINX_SHM_RULES,
                max_keys,
                DEFAULT_MAX_ACTIVE_BUCKETS,
            )
        }
        .expect("initialize shared store");
        store.add_rule(0, rule).expect("add rule");
        store
    }

    #[derive(Clone, Copy)]
    struct TestVariables<'a> {
        values: &'a [(&'a str, &'a [u8])],
    }

    impl NginxVariableLookup for TestVariables<'_> {
        fn value<'a>(&'a self, name: &str) -> Option<&'a [u8]> {
            self.values
                .iter()
                .find_map(|(key, value)| (*key == name).then_some(*value))
        }
    }

    #[test]
    fn shm_store_limits_requests_without_allocating_runtime_records() {
        let bytes = NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 8, DEFAULT_MAX_ACTIVE_BUCKETS)
            .expect("required bytes");
        let mut memory = vec![0_u8; bytes];
        let mut store = shm_store(&mut memory, shm_rule("tenant_api", "2r/m", &["$uri"]), 8);
        let variables = TestVariables {
            values: &[("uri", b"/api/a")],
        };

        assert_eq!(store.access(0, &variables, 0), Ok(NginxStatus::Declined));
        assert_eq!(store.access(0, &variables, 1), Ok(NginxStatus::Declined));
        assert_eq!(
            store.access(0, &variables, 2),
            Ok(NginxStatus::TooManyRequests)
        );

        let stats = store.stats_snapshot();
        assert_eq!(stats.requests, 3);
        assert_eq!(stats.allowed, 2);
        assert_eq!(stats.rejected, 1);
    }

    #[test]
    fn shm_store_handles_share_counts_across_workers() {
        let bytes = NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 8, DEFAULT_MAX_ACTIVE_BUCKETS)
            .expect("required bytes");
        let mut memory = vec![0_u8; bytes];
        let mut first = shm_store(&mut memory, shm_rule("tenant_api", "1r/m", &["$uri"]), 8);
        let mut second =
            unsafe { NgxShmStore::from_initialized(memory.as_mut_ptr(), memory.len()) }
                .expect("second worker handle");
        let variables = TestVariables {
            values: &[("uri", b"/api/a")],
        };

        assert_eq!(first.access(0, &variables, 0), Ok(NginxStatus::Declined));
        assert_eq!(
            second.access(0, &variables, 1),
            Ok(NginxStatus::TooManyRequests)
        );
    }

    #[test]
    fn shm_store_uses_all_configured_key_components() {
        let bytes = NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 8, DEFAULT_MAX_ACTIVE_BUCKETS)
            .expect("required bytes");
        let mut memory = vec![0_u8; bytes];
        let mut store = shm_store(
            &mut memory,
            shm_rule("tenant_api", "1r/m", &["$arg_tenant", "$uri"]),
            8,
        );
        let tenant_a = TestVariables {
            values: &[("arg_tenant", b"a"), ("uri", b"/api")],
        };
        let tenant_b = TestVariables {
            values: &[("arg_tenant", b"b"), ("uri", b"/api")],
        };

        assert_eq!(store.access(0, &tenant_a, 0), Ok(NginxStatus::Declined));
        assert_eq!(store.access(0, &tenant_b, 1), Ok(NginxStatus::Declined));
        assert_eq!(
            store.access(0, &tenant_a, 2),
            Ok(NginxStatus::TooManyRequests)
        );
    }

    #[test]
    fn shm_store_supports_ip_based_rate_limiting() {
        let bytes = NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 8, DEFAULT_MAX_ACTIVE_BUCKETS)
            .expect("required bytes");
        let mut memory = vec![0_u8; bytes];
        let mut store = shm_store(
            &mut memory,
            shm_rule("ip_api", "1r/m", &["$remote_addr"]),
            8,
        );
        let first_ip = TestVariables {
            values: &[("remote_addr", b"192.0.2.1")],
        };
        let second_ip = TestVariables {
            values: &[("remote_addr", b"192.0.2.2")],
        };

        assert_eq!(store.access(0, &first_ip, 0), Ok(NginxStatus::Declined));
        assert_eq!(store.access(0, &second_ip, 1), Ok(NginxStatus::Declined));
        assert_eq!(
            store.access(0, &first_ip, 2),
            Ok(NginxStatus::TooManyRequests)
        );
    }

    #[test]
    fn shm_store_expires_window_and_aggregates_overflow() {
        let bytes = NgxShmStore::required_bytes(MAX_NGINX_SHM_RULES, 1, DEFAULT_MAX_ACTIVE_BUCKETS)
            .expect("required bytes");
        let mut memory = vec![0_u8; bytes];
        let mut store = shm_store(&mut memory, shm_rule("tenant_api", "1r/m", &["$uri"]), 1);
        let key_a = TestVariables {
            values: &[("uri", b"/api/a")],
        };
        let key_b = TestVariables {
            values: &[("uri", b"/api/b")],
        };

        assert_eq!(store.access(0, &key_a, 0), Ok(NginxStatus::Declined));
        assert_eq!(store.access(0, &key_b, 1), Ok(NginxStatus::TooManyRequests));
        assert_eq!(store.stats_snapshot().overflow_keys, 1);
        assert_eq!(store.access(0, &key_a, 101), Ok(NginxStatus::Declined));
    }

    #[test]
    fn peer_table_parses_deduplicates_and_ignores_self() {
        let self_addr = "127.0.0.1:9000".parse().expect("addr");
        let table = NginxPeerTable::parse_lines(
            "
            # comment
            127.0.0.1:9000
            127.0.0.2:9000
            127.0.0.2:9000
            [::1]:9001
            ",
            Some(self_addr),
        )
        .expect("peers");

        assert_eq!(table.len(), 2);
        assert_eq!(
            table.as_slice()[0].socket_addr(),
            Some("127.0.0.2:9000".parse().expect("addr"))
        );
        assert_eq!(
            table.as_slice()[1].socket_addr(),
            Some("[::1]:9001".parse().expect("addr"))
        );
    }

    #[test]
    fn peer_file_loads_through_caller_scratch_buffer() {
        let path =
            std::env::temp_dir().join(format!("gabion-nginx-peers-{}.txt", std::process::id()));
        std::fs::write(&path, "127.0.0.2:9000\n127.0.0.3:9000\n").expect("write peers");
        let mut scratch = [0_u8; 128];

        let table = load_peer_file(&path, &mut scratch, None).expect("load peers");
        let too_small = load_peer_file(&path, &mut scratch[..8], None);

        let _ = std::fs::remove_file(path);
        assert_eq!(table.len(), 2);
        assert_eq!(too_small, Err(NginxGossipError::PeerFileTooLarge));
    }

    #[test]
    fn leader_lease_allows_one_gossip_owner_and_expires() {
        let lease = SharedLeaderLease::default();

        assert!(lease.try_acquire(1, 100, 50));
        assert!(!lease.try_acquire(2, 110, 50));
        assert!(lease.try_acquire(1, 120, 50));
        assert_eq!(lease.snapshot().owner_worker, 1);
        assert!(lease.try_acquire(2, 171, 50));

        let snapshot = lease.snapshot();
        assert_eq!(snapshot.owner_worker, 2);
        assert_eq!(snapshot.epoch, 2);
    }

    #[derive(Default)]
    struct RecordingTransport {
        sends: Vec<(NginxPeer, usize)>,
    }

    impl NginxGossipTransport for RecordingTransport {
        fn send(&mut self, peer: NginxPeer, payload: &[u8]) -> bool {
            self.sends.push((peer, payload.len()));
            true
        }
    }

    struct PacketTransport {
        peer: NginxPeer,
        packet: Vec<u8>,
        delivered: bool,
    }

    impl PacketTransport {
        fn new(peer: NginxPeer, packet: Vec<u8>) -> Self {
            Self {
                peer,
                packet,
                delivered: false,
            }
        }
    }

    impl NginxGossipTransport for PacketTransport {
        fn send(&mut self, _peer: NginxPeer, _payload: &[u8]) -> bool {
            true
        }

        fn recv(&mut self, buffer: &mut [u8]) -> Option<(NginxPeer, usize)> {
            if self.delivered || self.packet.len() > buffer.len() {
                return None;
            }
            buffer[..self.packet.len()].copy_from_slice(&self.packet);
            self.delivered = true;
            Some((self.peer, self.packet.len()))
        }
    }

    #[test]
    fn embedded_gossip_sends_only_from_elected_owner_with_reused_buffer() {
        let peers =
            NginxPeerTable::parse_lines("127.0.0.2:9000\n127.0.0.3:9000\n", None).expect("peers");
        let lease = SharedLeaderLease::default();
        let mut buffers =
            NginxGossipBuffers::with_capacity(DEFAULT_GOSSIP_PAYLOAD_BYTES).expect("buffers");
        let mut transport = RecordingTransport::default();
        let gossip = NginxEmbeddedGossip {
            cluster_id_hash: 7,
            node_id: NodeId { hi: 1, lo: 2 },
            incarnation: 9,
            lease_ttl_millis: 1_000,
            auth_key: None,
        };

        let first = gossip.tick(1, 0, &peers, &lease, &mut buffers, &mut transport);
        let send_capacity = buffers.send.capacity();
        let second = gossip.tick(2, 1, &peers, &lease, &mut buffers, &mut transport);

        assert_eq!(
            first,
            GossipTickOutcome::Sent {
                peers: 2,
                failed: 0,
                bytes: 68,
                truncated: false,
            }
        );
        assert_eq!(second, GossipTickOutcome::LocalOnlyNoLeader);
        assert_eq!(transport.sends.len(), 2);
        assert_eq!(buffers.send.capacity(), send_capacity);
        assert_eq!(buffers.recv_capacity(), DEFAULT_GOSSIP_PAYLOAD_BYTES);
    }

    #[test]
    fn embedded_gossip_receives_and_merges_packet_without_message_allocation() {
        let peer = NginxPeer::new("127.0.0.2:9000".parse().expect("addr"));
        let remote_node = NodeId { hi: 9, lo: 9 };
        let mut packet = Vec::with_capacity(256);
        let cell = gabion_gossip::CounterCell {
            rule_id: 1,
            key_hash_hi: 10,
            key_hash_lo: 20,
            bucket_start_millis: 0,
            origin_node_id: remote_node,
            origin_incarnation: 1,
            count: 5,
            last_update_millis: 100,
            sequence: 0,
        };
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 7,
                sender_node_id: remote_node,
                sender_incarnation: 1,
                min_bucket: 0,
                max_bucket: 0,
                flags: 0,
            },
            digests: Vec::new(),
            cells: vec![cell],
            truncated: false,
        };
        let gossip = NginxEmbeddedGossip {
            cluster_id_hash: 7,
            node_id: NodeId { hi: 1, lo: 2 },
            incarnation: 9,
            lease_ttl_millis: 1_000,
            auth_key: None,
        };
        let mut buffers = NginxGossipBuffers::with_capacity(256).expect("buffers");
        let mut table = CellTable::with_capacity(4, 4);
        assert!(!encode_message(&message, &mut packet, 256));
        let mut transport = PacketTransport::new(peer, packet);

        let outcome = gossip.receive_one(123, &mut table, None, &mut buffers, &mut transport);

        assert_eq!(
            outcome,
            GossipReceiveOutcome::Merged {
                merged: 1,
                stale: 0,
            }
        );
        assert_eq!(table.active_cell_count(), 1);
    }

    #[test]
    fn embedded_gossip_rejects_wrong_cluster_before_merge() {
        let peer = NginxPeer::new("127.0.0.2:9000".parse().expect("addr"));
        let mut packet = Vec::with_capacity(128);
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 8,
                sender_node_id: NodeId { hi: 9, lo: 9 },
                sender_incarnation: 1,
                min_bucket: 0,
                max_bucket: 0,
                flags: 0,
            },
            digests: Vec::new(),
            cells: vec![gabion_gossip::CounterCell {
                rule_id: 1,
                key_hash_hi: 10,
                key_hash_lo: 20,
                bucket_start_millis: 0,
                origin_node_id: NodeId { hi: 9, lo: 9 },
                origin_incarnation: 1,
                count: 5,
                last_update_millis: 100,
                sequence: 0,
            }],
            truncated: false,
        };
        let gossip = NginxEmbeddedGossip {
            cluster_id_hash: 7,
            node_id: NodeId { hi: 1, lo: 2 },
            incarnation: 9,
            lease_ttl_millis: 1_000,
            auth_key: None,
        };
        let mut buffers = NginxGossipBuffers::with_capacity(256).expect("buffers");
        let mut table = CellTable::with_capacity(4, 4);
        assert!(!encode_message(&message, &mut packet, 256));
        let mut transport = PacketTransport::new(peer, packet);

        let outcome = gossip.receive_one(123, &mut table, None, &mut buffers, &mut transport);

        assert_eq!(outcome, GossipReceiveOutcome::WrongCluster);
        assert_eq!(table.active_cell_count(), 0);
    }

    #[test]
    fn embedded_gossip_rejects_tampered_authenticated_packet() {
        let key = HmacKey::new([7_u8; 32]);
        let peer = NginxPeer::new("127.0.0.2:9000".parse().expect("addr"));
        let remote_node = NodeId { hi: 9, lo: 9 };
        let mut packet = Vec::with_capacity(256);
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 7,
                sender_node_id: remote_node,
                sender_incarnation: 1,
                min_bucket: 0,
                max_bucket: 0,
                flags: 0,
            },
            digests: Vec::new(),
            cells: vec![gabion_gossip::CounterCell {
                rule_id: 1,
                key_hash_hi: 10,
                key_hash_lo: 20,
                bucket_start_millis: 0,
                origin_node_id: remote_node,
                origin_incarnation: 1,
                count: 5,
                last_update_millis: 100,
                sequence: 0,
            }],
            truncated: false,
        };
        let gossip = NginxEmbeddedGossip {
            cluster_id_hash: 7,
            node_id: NodeId { hi: 1, lo: 2 },
            incarnation: 9,
            lease_ttl_millis: 1_000,
            auth_key: Some(key),
        };
        let mut buffers = NginxGossipBuffers::with_capacity(256).expect("buffers");
        let mut table = CellTable::with_capacity(4, 4);
        assert!(!encode_authenticated_message(
            &message,
            key,
            &mut packet,
            GossipLimits {
                max_payload_bytes: 256,
                max_digests: 0,
                max_cells: 1,
            },
        ));
        let last = packet.len() - 1;
        packet[last] ^= 1;
        let mut transport = PacketTransport::new(peer, packet);

        let outcome = gossip.receive_one(123, &mut table, None, &mut buffers, &mut transport);

        assert_eq!(
            outcome,
            GossipReceiveOutcome::DecodeError(DecodeError::AuthenticationFailed)
        );
        assert_eq!(table.active_cell_count(), 0);
    }

    #[test]
    fn udp_transport_sends_and_receives_without_packet_allocation() {
        let Ok(mut first) = NginxUdpTransport::bind("127.0.0.1:0".parse().expect("addr")) else {
            return;
        };
        let Ok(mut second) = NginxUdpTransport::bind("127.0.0.1:0".parse().expect("addr")) else {
            return;
        };
        let second_addr = second.local_addr().expect("second addr");
        let payload = [1_u8, 2, 3, 4];
        let mut recv = [0_u8; 16];

        assert!(first.send(NginxPeer::new(second_addr), &payload));

        let mut received = None;
        for _ in 0..1_000 {
            received = second.recv(&mut recv);
            if received.is_some() {
                break;
            }
        }

        let (_peer, len) = received.expect("packet");
        assert_eq!(len, payload.len());
        assert_eq!(&recv[..len], &payload);
    }

    #[quickcheck]
    fn quickcheck_peer_file_uses_scratch_and_rejects_oversized_inputs(
        case: NginxPeerFileCase,
    ) -> TestResult {
        let mut input = String::new();
        for octet in &case.peers {
            input.push_str("127.0.0.");
            input.push_str(&(octet % 64).saturating_add(1).to_string());
            input.push_str(":9000\n");
        }
        let path = std::env::temp_dir().join(format!(
            "gabion-nginx-peer-file-{}-{}-{}.txt",
            std::process::id(),
            input.len(),
            case.scratch_extra
        ));
        if std::fs::write(&path, input.as_bytes()).is_err() {
            return TestResult::error("failed to write generated peer file");
        }

        let mut exact_scratch = vec![
            0_u8;
            input
                .len()
                .saturating_add(usize::from(case.scratch_extra))
                .max(1)
        ];
        let loaded = load_peer_file(&path, &mut exact_scratch, None);
        let mut short_scratch = vec![0_u8; input.len().saturating_sub(1)];
        let too_small = if input.is_empty() {
            Err(NginxGossipError::PeerFileTooLarge)
        } else {
            load_peer_file(&path, &mut short_scratch, None)
        };
        let _ = std::fs::remove_file(path);

        let Ok(table) = loaded else {
            return TestResult::error("peer file did not load with sufficient scratch");
        };
        if table.len() > case.peers.len().min(MAX_NGINX_PEERS) {
            return TestResult::error("peer file loaded more peers than generated");
        }
        if !input.is_empty() && too_small != Err(NginxGossipError::PeerFileTooLarge) {
            return TestResult::error("peer file did not reject undersized scratch");
        }
        TestResult::passed()
    }

    #[quickcheck]
    fn quickcheck_embedded_receive_rejects_invalid_frames_before_mutating_cells(
        case: NginxReceiveRejectCase,
    ) -> TestResult {
        if !case.wrong_cluster && !case.self_sender && !case.tamper_auth {
            return TestResult::discard();
        }

        let key = HmacKey::new([7_u8; 32]);
        let peer = NginxPeer::new("127.0.0.2:9000".parse().expect("addr"));
        let local_node = NodeId { hi: 1, lo: 2 };
        let remote_node = if case.self_sender {
            local_node
        } else {
            NodeId { hi: 9, lo: 9 }
        };
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: if case.wrong_cluster { 8 } else { 7 },
                sender_node_id: remote_node,
                sender_incarnation: 1,
                min_bucket: 0,
                max_bucket: 0,
                flags: 0,
            },
            digests: Vec::new(),
            cells: vec![gabion_gossip::CounterCell {
                rule_id: 1,
                key_hash_hi: 10,
                key_hash_lo: 20,
                bucket_start_millis: 0,
                origin_node_id: remote_node,
                origin_incarnation: 1,
                count: u64::from(case.count),
                last_update_millis: 100,
                sequence: 0,
            }],
            truncated: false,
        };
        let gossip = NginxEmbeddedGossip {
            cluster_id_hash: 7,
            node_id: local_node,
            incarnation: 9,
            lease_ttl_millis: 1_000,
            auth_key: case.tamper_auth.then_some(key),
        };
        let mut packet = Vec::with_capacity(256);
        let truncated = if case.tamper_auth {
            encode_authenticated_message(
                &message,
                key,
                &mut packet,
                GossipLimits {
                    max_payload_bytes: 256,
                    max_digests: 0,
                    max_cells: 1,
                },
            )
        } else {
            encode_message(&message, &mut packet, 256)
        };
        if truncated {
            return TestResult::error("generated invalid receive frame truncated");
        }
        if case.tamper_auth {
            let last = packet.len() - 1;
            packet[last] ^= 1;
        }
        let mut buffers = match NginxGossipBuffers::with_capacity(256) {
            Ok(buffers) => buffers,
            Err(_) => return TestResult::error("valid receive buffer capacity was rejected"),
        };
        let mut table = CellTable::with_capacity(4, 4);
        let mut transport = PacketTransport::new(peer, packet);

        let outcome = gossip.receive_one(123, &mut table, None, &mut buffers, &mut transport);

        if matches!(outcome, GossipReceiveOutcome::Merged { merged: 1, .. }) {
            return TestResult::error("invalid receive frame merged a cell");
        }
        if table.active_cell_count() != 0 {
            return TestResult::error("invalid receive frame mutated cell table");
        }
        TestResult::passed()
    }

    #[quickcheck]
    fn quickcheck_peer_table_is_sorted_deduped_bounded_and_selfless(
        case: NginxPeerTableCase,
    ) -> TestResult {
        let self_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, case.self_octet)), 9000);
        let mut input = String::new();
        for octet in case.peers {
            input.push_str("127.0.0.");
            input.push_str(&(octet % 64).saturating_add(1).to_string());
            input.push_str(":9000\n");
        }

        let Ok(table) = NginxPeerTable::parse_lines(&input, Some(self_addr)) else {
            return TestResult::error("generated peer table failed to parse");
        };
        let peers = table.as_slice();
        if peers.len() > MAX_NGINX_PEERS {
            return TestResult::error("peer table exceeded configured capacity");
        }
        if peers.windows(2).any(|window| window[0] >= window[1]) {
            return TestResult::error("peer table is not strictly sorted and deduplicated");
        }
        if peers
            .iter()
            .any(|peer| peer.socket_addr() == Some(self_addr))
        {
            return TestResult::error("peer table retained self address");
        }
        TestResult::passed()
    }

    #[quickcheck]
    fn quickcheck_access_phase_respects_missing_variables_and_fallback_cap(
        case: NginxAccessCase,
    ) -> TestResult {
        let zone = match NginxZoneConfig::new("api", 128 * 1024 * 1024, 16) {
            Ok(zone) => zone,
            Err(_) => return TestResult::error("valid generated zone was rejected"),
        };
        let mut adapter = NginxLocalOnlyAdapter::new(zone, rule());
        let complete = [
            NginxVariable {
                name: "tenant",
                value: "a",
            },
            NginxVariable {
                name: "uri",
                value: "/v1",
            },
        ];
        let missing = [NginxVariable {
            name: "tenant",
            value: "a",
        }];
        let variables = if case.missing_variable {
            missing.as_slice()
        } else {
            complete.as_slice()
        };
        let request = NginxRequest {
            domain: "api",
            variables,
            hits: 1,
        };
        let mut allowed = 0_u8;

        for now_millis in 0..u64::from(case.attempts) {
            match adapter.access_phase(request, now_millis) {
                NginxStatus::Declined => allowed = allowed.saturating_add(1),
                NginxStatus::TooManyRequests => break,
            }
        }

        if case.missing_variable {
            if adapter.active_keys() == 0 {
                TestResult::passed()
            } else {
                TestResult::error("missing variable path tracked a key")
            }
        } else if allowed <= rule().local_fallback_limit as u8 && adapter.active_keys() <= 1 {
            TestResult::passed()
        } else {
            TestResult::error("access phase exceeded fallback cap or tracked too many keys")
        }
    }

    #[quickcheck]
    fn quickcheck_embedded_gossip_sends_from_single_owner(
        case: NginxEmbeddedSendCase,
    ) -> TestResult {
        let mut peers = NginxPeerTable::empty();
        for index in 0..case.peer_count {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 1, index + 1)), 9000);
            if peers.insert(NginxPeer::new(addr)).is_err() {
                return TestResult::error("generated peer table unexpectedly filled");
            }
        }
        let lease = SharedLeaderLease::default();
        let mut buffers = match NginxGossipBuffers::with_capacity(DEFAULT_GOSSIP_PAYLOAD_BYTES) {
            Ok(buffers) => buffers,
            Err(_) => return TestResult::error("valid gossip buffer capacity was rejected"),
        };
        let mut transport = RecordingTransport::default();
        let gossip = NginxEmbeddedGossip {
            cluster_id_hash: 7,
            node_id: NodeId { hi: 1, lo: 2 },
            incarnation: 9,
            lease_ttl_millis: 1_000,
            auth_key: None,
        };

        let first = gossip.tick(
            case.leader_worker,
            0,
            &peers,
            &lease,
            &mut buffers,
            &mut transport,
        );
        let second = gossip.tick(
            case.contender_worker,
            1,
            &peers,
            &lease,
            &mut buffers,
            &mut transport,
        );

        if peers.is_empty() {
            return if first == GossipTickOutcome::LocalOnlyNoPeers
                && second == GossipTickOutcome::LocalOnlyNoPeers
                && transport.sends.is_empty()
            {
                TestResult::passed()
            } else {
                TestResult::error("empty peer table sent gossip")
            };
        }
        if !matches!(first, GossipTickOutcome::Sent { .. }) {
            return TestResult::error("leader did not send to non-empty peer table");
        }
        if case.contender_worker != case.leader_worker
            && second != GossipTickOutcome::LocalOnlyNoLeader
        {
            return TestResult::error("contender sent while lease owner was active");
        }
        if transport.sends.len() < peers.len() {
            return TestResult::error("leader did not send to every generated peer");
        }
        TestResult::passed()
    }
}
