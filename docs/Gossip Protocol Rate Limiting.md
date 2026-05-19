# Design Document: Availability-First Gossip Rate Limiter in Rust

## 1. Purpose

Build a **fast, in-memory, availability-first distributed rate limiter** in Rust.

The limiter makes every request-path decision from local memory. It uses asynchronous anti-entropy gossip to share request counts between nodes, but gossip is never required for request admission.

The same core engine supports:

| Mode | Description |
|---|---|
| Standalone service | Runs as its own deployment and serves Envoy Rate Limit Service v3 |
| Embedded Rust library | Used directly by Rust applications |
| NGINX module | Embedded into NGINX using `ngx`, with allocation and process-model constraints |
| Local-only mode | Runs without Kubernetes, without peers, and without gossip |

The system is intentionally **AP**, not CP. It favors availability and bounded inaccuracy over strict global consistency.

---

## 2. Primary Design Principle

> **No request waits for the network. No request depends on Redis. No request depends on Kubernetes. No request depends on gossip.**

The request path is:

```text
extract key
  ↓
read local in-memory counters
  ↓
make allow / reject decision
  ↓
increment local in-memory counter
  ↓
return
```

The background path is:

```text
discover peers
  ↓
gossip local and merged counter state
  ↓
merge remote CRDT cells
  ↓
update local estimates
  ↓
expire old buckets
```

If background work stops entirely, the limiter keeps running in local-only mode.

---

## 3. Non-Goals

This design does **not** provide strict global quotas.

It does not guarantee:

```text
exact global counts
zero overshoot
linearizable reads
durable accounting
quorum behavior
billing-grade enforcement
```

It is suitable for:

```text
service protection
abuse dampening
noisy-neighbor control
availability-first tenant limiting
rough global throttling
```

It is not sufficient by itself for:

```text
hard contractual quota enforcement
financial metering
security decisions requiring strong consistency
```

---

## 4. External Protocols and Integrations

### 4.1 Envoy Rate Limit Service

The standalone service implements Envoy’s `envoy.service.ratelimit.v3.RateLimitService`.

Envoy’s RLS request includes a `domain`, one or more descriptors, and optional `hits_addend`. When multiple descriptors are provided, the service should limit on all of them and return `OVER_LIMIT` if any descriptor is over limit. `hits_addend` allows a request to count as more than one hit.

Reference: <https://www.envoyproxy.io/docs/envoy/latest/api-v3/service/ratelimit/v3/rls.proto>

The standalone service returns `OK` or `OVER_LIMIT` from local memory only.

Envoy itself still makes a remote call to the RLS process, so Envoy should be configured availability-first. Envoy’s HTTP rate limit filter calls the external service when matching rate-limit config applies; if the RLS reports over-limit, Envoy returns 429. If the RLS call errors while `failure_mode_deny` is true, Envoy returns 500.

Reference: <https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/rate_limit_filter>

Recommended Envoy posture:

```yaml
failure_mode_deny: false
timeout: 10ms-50ms
```

The RLS service should almost always respond, but Envoy should not turn an RLS outage into a traffic outage.

### 4.2 Kubernetes EndpointSlice Discovery

In Kubernetes, peer discovery should use EndpointSlices.

EndpointSlices are the Kubernetes API used to scale Service backend endpoint tracking. They are stable since Kubernetes v1.21 and track backend endpoint IPs that normally represent Pods.

Reference: <https://kubernetes.io/docs/concepts/services-networking/endpoint-slices/>

The standalone service can use `kube-rs` to watch EndpointSlices. The `kube::runtime::watcher` continuously watches a Kubernetes resource and attempts to recover after errors.

Reference: <https://docs.rs/kube/latest/kube/runtime/fn.watcher.html>

Important EndpointSlice behavior:

```text
watch all EndpointSlices for a Service
deduplicate endpoints
ignore self
keep last known peer snapshot
degrade to local-only if discovery fails
```

Kubernetes RBAC:

```yaml
rules:
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
```

### 4.3 NGINX / `ngx`

The NGINX adapter is a constrained target, not a normal Tokio service.

The `ngx` crate provides Rust bindings for building NGINX dynamic modules. Its README says the project is still in active development, APIs are not stabilized, and breaking changes are expected. It also recommends building module binaries against the exact NGINX source and configuration used in production, because configure arguments and downstream distribution patches can affect visible APIs and symbols.

Reference: <https://github.com/nginx/ngx-rust>

The `ngx` crate has default `alloc` and `std` features, plus an `async` feature for a minimal runtime on top of the NGINX event loop. However, the NGINX module design should **not** depend on Tokio, Tonic, kube-rs, protobuf-heavy paths, or heap allocation in the hot path.

NGINX shared memory zones are the correct mechanism for sharing limiter state across worker processes. NGINX maps shared zones into all NGINX processes and provides a slab pool for allocating inside a shared zone.

Reference: <https://nginx.org/en/docs/dev/development_guide.html>

---

## 5. System Architecture

```text
                       ┌────────────────────────────┐
                       │ Peer Discovery              │
                       │ - EndpointSlice via kube-rs │
                       │ - static file               │
                       │ - static config             │
                       │ - none                      │
                       └──────────────┬─────────────┘
                                      │
                                      ▼
┌──────────────────┐        ┌───────────────────────────┐
│ Envoy RLS adapter │──────▶│                           │
│ tonic/prost       │        │                           │
└──────────────────┘        │                           │
                            │     limiter-core           │
┌──────────────────┐        │                           │
│ Rust library API  │──────▶│ - rule matching            │
└──────────────────┘        │ - key hashing              │
                            │ - local counters           │
┌──────────────────┐        │ - CRDT cell table          │
│ NGINX module      │──────▶│ - decision engine          │
│ ngx + shm store   │        │ - dirty delta log          │
└──────────────────┘        │ - memory budgets           │
                            │                           │
                            └─────────────┬─────────────┘
                                          │
                                          ▼
                               ┌────────────────────┐
                               │ Gossip backend      │
                               │ anti-entropy merge  │
                               └────────────────────┘
```

The core engine does not know about Envoy, NGINX, Kubernetes, or Tokio.

Adapters provide:

```text
clock
storage backend
key source
peer source
transport
metrics sink
```

---

## 6. Rust Workspace

```text
crates/
  limiter-core/
    no_std-compatible data model where practical
    rule model
    key hashing
    CRDT merge logic
    decision engine
    storage traits
    no networking

  limiter-store-heap/
    standalone/Rust-app storage backend
    preallocated shards
    fixed-capacity hash tables
    reusable buffers

  limiter-store-nginx/
    NGINX shared-memory storage backend
    ngx_slab_pool_t allocation
    offset-based records
    no Rust heap in request path

  limiter-gossip/
    anti-entropy protocol
    compact binary encoding
    delta log
    digest generation
    transport traits

  limiter-gossip-standalone/
    Tokio TCP or gRPC transport
    reusable BytesMut buffers
    standalone daemon only

  limiter-discovery/
    PeerHandler trait
    none/static/file providers
    kube-rs EndpointSlice watcher
    shared Kubernetes discovery defaults

  limiter-envoy/
    Envoy RLS v3 protobuf bindings
    tonic service
    descriptor mapping

  limiter-nginx/
    ngx dynamic module
    config directives
    access-phase hook
    shared-memory store integration

  limiter-bin/
    standalone daemon
    config loading
    metrics
    admin endpoints
```

The workspace keeps heavyweight dependencies isolated:

| Dependency | Allowed in |
|---|---|
| `tokio` | standalone service |
| `tonic` | Envoy RLS adapter, standalone gossip transport |
| `prost` | Envoy RLS adapter |
| `kube` | Kubernetes discovery crate |
| `ngx` | NGINX module only |
| `std` | standalone service; optional in NGINX module |
| `alloc` | config, startup, controlled storage setup |
| request-path heap allocation | avoided everywhere |

---

## 7. Allocation Strategy

The design prefers:

```text
few large allocations
fixed-capacity tables
arena/slab allocation
reusable buffers
bounded memory
no per-request heap allocation
```

It avoids:

```text
HashMap allocation per key
Vec allocation per request
String construction per request
protobuf allocation in gossip
unbounded descriptor storage
remote_counts HashMap inside every key
Tokio/kube/prost inside the NGINX hot path
```

### 7.1 Core Allocation Rules

The request path may allocate only when a brand-new key is admitted into the table.

Existing keys should require:

```text
zero heap allocation
bounded atomic reads
bounded atomic writes
no network I/O
no async await
no lock held across I/O
```

New-key allocation is bounded by rule-level memory budgets. If the budget is exhausted, new keys map to an overflow key or use another configured overflow policy.

### 7.2 `no_std`-Style Core

The core crate should be designed as if it might need to run in a constrained FFI environment:

```rust
#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;
```

The entire product does not need to be truly `no_std`.

Recommended split:

```text
limiter-core
  no networking
  no tokio
  no kube
  no tonic
  no ngx
  no OS assumptions
  allocator-aware traits

limiter-store-heap
  std-backed preallocated heap storage

limiter-store-nginx
  NGINX shared-memory storage
```

The goal is not ideological `no_std`; the goal is **allocation discipline and adapter portability**.

---

## 8. Core Data Structures

The core avoids nested dynamic maps.

Instead of this:

```rust
HashMap<Key, HashMap<Bucket, HashMap<NodeId, Count>>>
```

Use fixed-capacity, table-oriented storage:

```text
RuleTable
PeerTable
KeyTable
CellTable
DirtyDeltaRing
GossipBufferPool
```

### 8.1 Rule Table

Rules are loaded at configuration time.

```rust
pub struct Rule {
    pub id: RuleId,
    pub domain_hash: KeyHash,
    pub descriptor_matcher: DescriptorMatcher,
    pub limit: u64,
    pub window: WindowSpec,
    pub local_fallback_limit: u64,
    pub local_absolute_limit: u64,
    pub stale_after_millis: u64,
    pub safety_margin: SafetyMargin,
    pub overflow_policy: OverflowPolicy,
    pub mode: EnforcementMode,
}
```

Rules are immutable during request handling. Reload creates a new rule table and swaps it atomically where the adapter allows.

### 8.2 Key Table

A key is:

```text
rule_id + hashed descriptor values
```

Do not build a canonical key string on the hot path. Hash incrementally.

For Envoy:

```text
hash(domain)
hash(descriptor[0].key)
hash(descriptor[0].value)
hash(descriptor[1].key)
hash(descriptor[1].value)
...
```

For NGINX:

```text
hash(rule_id)
hash(configured variable component 1)
hash(separator byte)
hash(configured variable component 2)
...
```

The key table stores compact entries:

```rust
#[repr(C)]
pub struct KeyEntry {
    rule_id: RuleId,
    key_hash: u128,
    last_seen_millis: AtomicU64,

    local_window_total: AtomicU64,
    estimated_window_total: AtomicU64,

    bucket_base_millis: AtomicI64,
    buckets: BucketRing,
}
```

### 8.3 Bucket Ring

Each key has a fixed-size ring of buckets.

```rust
#[repr(C)]
pub struct BucketSlot {
    bucket_start_millis: AtomicI64,
    local_count: AtomicU64,
    estimated_total: AtomicU64,
}
```

`estimated_total` includes:

```text
local count from this node
+
merged remote counts for this bucket
```

The request path should normally read:

```text
local_window_total
estimated_window_total
```

instead of scanning all peer counters.

### 8.4 Cell Table

Remote CRDT state lives in a separate global cell table.

```rust
#[repr(C)]
pub struct CellEntry {
    rule_id: RuleId,
    key_hash: u128,
    bucket_start_millis: i64,
    origin_node: NodeIndex,
    origin_incarnation: u64,
    count: u64,
    last_update_millis: u64,
}
```

Cell key:

```text
(rule_id, key_hash, bucket_start, origin_node, origin_incarnation)
```

Merge rule:

```text
stored_count = max(stored_count, received_count)
```

If the received count is greater than the stored count:

```text
delta = received_count - stored_count
stored_count = received_count
bucket.estimated_total += delta
key.estimated_window_total += delta
mark_dirty(cell)
```

This avoids per-key remote maps and keeps merge-time allocation centralized and bounded.

### 8.5 Dirty Delta Ring

Local increments and remote merges mark cells dirty for gossip.

```rust
#[repr(C)]
pub struct DirtyEntry {
    cell_id: CellId,
    sequence: u64,
}
```

The dirty log is fixed capacity.

If it overflows:

```text
set shard_needs_resync = true
drop oldest dirty references
continue serving requests
```

Gossip can recover with a full shard digest/resync.

---

## 9. Request Path

### 9.1 Pseudocode

```rust
pub fn check_and_record(&self, req: LimitRequest<'_>) -> Decision {
    let now = self.clock.now_millis();
    let hits = req.hits_addend.unwrap_or(1);

    let matched_rules = self.rules.match_request(&req);

    for rule in matched_rules {
        let key_hash = self.hash_key(rule, &req);

        let key = self
            .store
            .get_or_insert_key(rule.id, key_hash, now)
            .unwrap_or_else(|| self.store.overflow_key(rule.id));

        self.store.rotate_if_needed(key, now);

        let local = self.store.local_window_total(key);
        let estimated = self.store.estimated_window_total(key);
        let fresh = self.store.global_estimate_is_fresh(rule.id, now);
        let margin = self.safety_margin(rule, now);

        let decision = decide(rule, local, estimated, fresh, margin, hits);

        if decision.is_reject() {
            return decision;
        }
    }

    for rule in matched_rules {
        let key_hash = self.hash_key(rule, &req);
        let key = self.store.get_existing_or_overflow_key(rule.id, key_hash);
        self.store.increment_local(key, now, hits);
    }

    Decision::Allow
}
```

### 9.2 Decision Logic

```text
if local_count + hits > local_absolute_limit:
    reject

if global_estimate_is_fresh:
    if estimated_global + hits + safety_margin <= global_limit:
        allow
    else:
        reject

if local_count + hits <= local_fallback_limit:
    allow

reject
```

This produces coherent availability-first behavior:

| State | Behavior |
|---|---|
| Gossip healthy | Use global estimate |
| Gossip stale | Use local fallback limit |
| No peers | Use local fallback/local limit |
| Partition | Each side remains available with bounded local behavior |
| Memory pressure | Use overflow policy; never allocate unboundedly |

---

## 10. Rate-Limit Semantics

Each rule has three important limits:

```text
global_limit
local_fallback_limit
local_absolute_limit
```

### 10.1 Global Limit

The desired approximate cluster-wide limit.

Example:

```text
10,000 requests / minute / tenant
```

### 10.2 Local Fallback Limit

The amount a node may serve when global gossip state is stale or absent.

Example:

```text
800 requests / minute / tenant / node
```

This is the availability budget.

### 10.3 Local Absolute Limit

The maximum a single node may serve even if global gossip appears healthy.

Example:

```text
3,000 requests / minute / tenant / node
```

This prevents one hot node from consuming the entire global limit based on stale or optimistic estimates.

---

## 11. CRDT Model

The backend is a **time-bucketed G-Counter CRDT**.

Each node owns only its own counter cells.

```text
(rule_id, key_hash, bucket_start, origin_node, origin_incarnation) -> count
```

Local node increments:

```text
count += hits
```

Remote merge:

```text
count = max(local_count, remote_count)
```

Properties:

```text
idempotent
commutative
associative
monotonic within a bucket
```

Old buckets expire by time. We do not decrement counters to implement sliding windows.

### 11.1 Node Identity

Node identity must not be reused accidentally.

In Kubernetes standalone mode:

```text
node_id = pod UID
incarnation = process start timestamp or random u64
```

Outside Kubernetes:

```text
node_id = configured stable ID
or
node_id = random UUID at startup
```

For NGINX:

```text
node_id = configured instance ID
incarnation = NGINX cycle generation + random u64
```

If multiple NGINX workers share one shared-memory state, they should share one limiter node identity.

---

## 12. Gossip Protocol

### 12.1 Protocol Shape

Use bounded push/pull anti-entropy.

Each gossip tick:

```text
choose random peers
send compact digest
send dirty deltas
receive peer deltas
merge cells
```

No gossip operation is required for request admission.

### 12.2 Wire Format

Do not use protobuf for the gossip hot path.

Use a compact binary format over reusable buffers:

```text
header
  magic
  version
  cluster_id_hash
  sender_node_id
  sender_incarnation
  min_bucket
  max_bucket
  flags

digest section
  shard_id
  cell_count
  checksum
  max_sequence

delta section
  repeated fixed-width CounterCell
```

A fixed-width `CounterCell` avoids allocation-heavy decoding:

```rust
#[repr(C)]
pub struct WireCounterCell {
    rule_id: u64,
    key_hash: u128,
    bucket_start_millis: i64,
    origin_node_id: u128,
    origin_incarnation: u64,
    count: u64,
}
```

### 12.3 Digest

A v1 digest should be simple and bounded:

```rust
pub struct ShardDigest {
    shard_id: u16,
    active_cell_count: u32,
    max_sequence: u64,
    checksum: u64,
}
```

Checksum input:

```text
rule_id
key_hash
bucket_start
origin_node
origin_incarnation
count
```

If digests mismatch, the peer sends cells from that shard up to `max_payload_bytes`.

This is deliberately simpler than a Merkle tree. Add Merkle-style prefix digests only if shard resyncs become too expensive.

### 12.4 Gossip Buffers

Allocate large buffers once:

```text
send_buffer: 64 KiB or 256 KiB
recv_buffer: 64 KiB or 256 KiB
decode_scratch: fixed capacity
```

Reuse them for every gossip tick.

If a peer has more deltas than fit:

```text
send truncated response
resume next tick
or request shard resync
```

Never allocate a larger buffer because one peer is behind.

### 12.5 Gossip Transport

Standalone service:

```text
Tokio TCP or gRPC transport
mTLS optional
short deadlines
buffer pool
```

NGINX module:

```text
no tonic
no kube
no protobuf
no tokio on request path
custom binary protocol only
optional dedicated gossip worker/thread or NGINX event-loop integration
```

---

## 13. Peer Discovery

### 13.1 PeerHandler Trait

```rust
pub trait PeerHandler {
    fn snapshot(&self) -> PeerSnapshot;
    fn peer_added(&self, peer: Peer);
    fn peer_removed(&self, peer: Peer);
}
```

The runtime owns a `PeerHandler`. Discovery sources push individual peer add/remove events into that handler; they do not hand vectors to the runtime and the runtime does not poll discovery with refresh calls. The runtime reads bounded snapshots from the same trait before each gossip tick.

Discovery failures are retry conditions, not state transitions. A file read failure or broken EndpointSlice watch keeps the last good peer set, continues serving, and keeps trying until a later event produces concrete peer additions or removals.

Message communication uses a separate transport trait. UDP is one implementation of that trait; deterministic tests can use an in-memory transport to exercise the gossip protocol without opening sockets.

### 13.2 Standalone Kubernetes Provider

The standalone daemon may run:

```text
kube-rs EndpointSlice watcher
  ↓
deduplicated peer list
  ↓
atomic peer snapshot swap
```

EndpointSlice discovery is not a health dependency. If it fails:

```text
keep last snapshot
continue serving
keep retrying the EndpointSlice watch
publish add/remove peer events only after real EndpointSlice changes
```

### 13.3 NGINX Peer Discovery

Do **not** put kube-rs inside the NGINX module initially.

Reasons:

```text
kube-rs brings async runtime and allocation behavior
EndpointSlice watches are not needed on the NGINX request path
NGINX module lifecycle and Tokio lifecycle are awkward together
NGINX worker processes complicate background runtime ownership
```

For NGINX, support:

```text
discovery none
discovery static
discovery file
```

A Kubernetes deployment that needs NGINX embedded gossip can update a peer file out-of-band. If that file stops updating, the module keeps the last peer snapshot or becomes local-only.

This keeps Kubernetes discovery out of the NGINX hot path and avoids coupling the NGINX module to kube-rs.

---

## 14. Standalone Service Design

The standalone service is the cleanest full-featured deployment.

```text
rate-gossipd
  :8081  Envoy RLS gRPC
  :18080 gossip transport
  :9090  metrics/admin
```

### 14.1 Request Flow

```text
Envoy
  ↓
ShouldRateLimit
  ↓
descriptor mapping
  ↓
limiter-core.check_and_record
  ↓
OK or OVER_LIMIT
```

The RLS handler must not call peers.

### 14.2 Standalone Storage

Use `HeapStore`.

Characteristics:

```text
large allocations at startup
fixed-capacity sharded key table
fixed-capacity cell table
fixed-size dirty rings
reusable gossip buffers
bounded key admission
```

Configuration example:

```yaml
storage:
  shards: 256
  max_keys: 1000000
  max_cells: 16000000
  dirty_ring_entries: 1048576
  gossip_buffer_bytes: 262144
  overflow_policy: aggregate
```

### 14.3 Standalone Discovery

```yaml
discovery:
  kind: kubernetes
  namespace: default
  service_name: rate-gossipd
  port_name: gossip
```

or:

```yaml
discovery:
  kind: static
  peers:
    - 10.0.0.10:18080
    - 10.0.0.11:18080
```

or:

```yaml
discovery:
  kind: none
```

---

## 15. NGINX Module Design

The NGINX module is a separate adapter over the same core logic.

It must not be treated like a normal async Rust service.

### 15.1 NGINX Constraints

The design assumes:

```text
multiple worker processes
ordinary Rust heap is per worker
shared limiter state requires NGINX shared memory
request handler must not block
request handler should not allocate
request handler must not await
Rust panics must not unwind across C ABI
```

The NGINX docs describe shared memory zones as the mechanism for sharing common data across processes, and expose a slab pool for allocations within those zones.

Reference: <https://nginx.org/en/docs/dev/development_guide.html>

### 15.2 NGINX Storage

Use `NgxShmStore`.

The shared memory zone contains:

```text
StoreHeader
RuleRuntimeTable
PeerTable
KeyTable
CellTable
DirtyDeltaRing
StatsCounters
LeaderLease
```

Do not store Rust-owned heap objects in shared memory.

Avoid:

```text
String
Vec
Box
Arc
HashMap
trait objects
ordinary Rust references
```

Use:

```text
#[repr(C)] records
integer IDs
offset pointers
fixed-capacity arrays
atomic counters
NGINX slab allocation for rare growth
```

Even though NGINX maps shared zones into all processes, offset-style references are safer and clearer than ordinary Rust references for shared-memory data.

### 15.3 NGINX Request Path

```text
access phase handler
  ↓
evaluate configured key components
  ↓
stream-hash key without String allocation
  ↓
check shared-memory counters
  ↓
increment local slot
  ↓
return NGX_DECLINED or HTTP 429
```

Example config shape:

```nginx
http {
    gabion_limit_zone api 128m;

    gabion_limit_rule tenant_api {
        key $http_x_tenant_id $uri;
        limit 10000r/m;
        window 60s;
        bucket 1s;
        local_fallback 1000r/m;
        local_absolute 3000r/m;
        mode enforce;
    }

    server {
        location /api/ {
            gabion_limit tenant_api;
            proxy_pass http://app;
        }
    }
}
```

### 15.4 NGINX Gossip Modes

The NGINX module should support three modes.

#### Mode 1: `gossip off`

```text
shared-memory local limiter only
no network activity
safest first implementation
```

#### Mode 2: `gossip file_peers`

```text
peer list loaded from static config or file
background gossip optional
no kube-rs in module
```

#### Mode 3: `gossip embedded`

```text
advanced mode
one elected worker owns gossip
leader elected via shared-memory lease
leader uses preallocated buffers
leader does not call NGINX APIs from non-NGINX threads
if leader disappears, another worker may acquire lease
if no leader, request path continues local-only
```

The embedded gossip path is the highest-risk part of the project. It should come after the standalone service and local-only NGINX module are stable.

### 15.5 NGINX Build Posture

Because `ngx` APIs are not stabilized and module binaries should be built against the exact NGINX source/configuration used in production, the NGINX adapter should have a dedicated build pipeline per NGINX version.

Reference: <https://github.com/nginx/ngx-rust>

Recommended:

```text
containerized NGINX build
exact NGINX source version pinned
exact configure flags pinned
module built in same image
integration tests load the built .so
```

### 15.6 Panic and FFI Policy

NGINX module build profile:

```toml
[profile.release]
lto = "fat"
codegen-units = 1
```

The request path should be panic-free by construction:

```text
no unwrap
no expect
no indexing without bounds checks
no allocation-dependent success
no Rust unwinding across NGINX C callbacks
```

---

## 16. Memory Management

### 16.1 Memory Budgets

Every rule must have hard budgets:

```yaml
max_keys: 1000000
max_cells: 16000000
max_nodes: 128
max_descriptor_bytes: 512
max_active_buckets: 64
overflow_policy: aggregate
```

Memory exhaustion must not crash the process.

### 16.2 Overflow Policy

When a new key cannot be admitted:

| Policy | Behavior |
|---|---|
| `aggregate` | Map new keys to reserved overflow key |
| `allow` | Allow without tracking |
| `reject` | Reject untracked new keys |
| `sample` | Track sampled subset |

Default:

```text
aggregate
```

This preserves availability and avoids unbounded allocation.

### 16.3 Garbage Collection

A background sweeper expires:

```text
old buckets
old CRDT cells
inactive keys
stale peers
dirty entries already superseded
```

Retention:

```text
window + gossip_horizon + clock_skew_allowance
```

Example:

```text
60s window
+ 30s gossip horizon
+ 5s skew allowance
= 95s retention
```

---

## 17. Local-Only Mode

Local-only mode is a first-class operating mode.

```yaml
discovery:
  kind: none

gossip:
  enabled: false
```

Behavior:

```text
no peer discovery
no gossip listener
no gossip sender
same rule engine
same storage engine
same request-path logic
```

A global rule becomes local to the process, pod, or NGINX shared-memory zone.

Metrics should expose this clearly:

```text
limiter_mode{mode="local_only"} 1
```

---

## 18. Configuration Examples

### 18.1 Standalone Kubernetes Envoy RLS

```yaml
node:
  id_source: kubernetes_pod_uid
  cluster_id: prod-api

server:
  envoy_rls:
    enabled: true
    bind: 0.0.0.0:8081

  gossip:
    enabled: true
    bind: 0.0.0.0:18080

  admin:
    bind: 0.0.0.0:9090

storage:
  shards: 256
  max_keys: 1000000
  max_cells: 16000000
  dirty_ring_entries: 1048576
  gossip_buffer_bytes: 262144

discovery:
  kind: kubernetes
  namespace: default
  service_name: rate-gossipd
  port_name: gossip

gossip:
  linger_ms: 250
  fanout: 3
  peer_timeout: 50ms
  max_payload_bytes: 262144
  full_resync_interval: 30s
  authentication:
    kind: mtls

limits:
  - name: tenant_api_minute
    domain: api
    descriptors:
      - key: tenant_id
        value: "*"
      - key: route
        value: "*"

    limit: 10000
    window: 60s
    bucket: 1s

    local_fallback_limit: 1000
    local_absolute_limit: 3000
    stale_after: 2s

    safety_margin:
      kind: dynamic
      max_peer_lag: 2s

    max_keys: 1000000
    overflow_policy: aggregate
    mode: enforce
```

### 18.2 Standalone Local-Only

```yaml
node:
  id_source: random
  cluster_id: local

server:
  envoy_rls:
    enabled: true
    bind: 0.0.0.0:8081

discovery:
  kind: none

gossip:
  enabled: false

storage:
  shards: 64
  max_keys: 100000
  max_cells: 0

limits:
  - name: local_tenant_minute
    domain: api
    descriptors:
      - key: tenant_id
        value: "*"

    limit: 1000
    window: 60s
    bucket: 1s

    local_fallback_limit: 1000
    local_absolute_limit: 1000
    mode: enforce
```

### 18.3 NGINX Local-Only

```nginx
http {
    gabion_limit_zone api 128m;

    gabion_limit_rule tenant_api {
        key $http_x_tenant_id $uri;
        limit 1000r/m;
        window 60s;
        bucket 1s;
        local_fallback 1000r/m;
        local_absolute 1000r/m;
        gabion off;
        overflow aggregate;
    }

    server {
        location /api/ {
            gabion_limit tenant_api;
            proxy_pass http://app;
        }
    }
}
```

### 18.4 NGINX With Static/File Peers

```nginx
http {
    gabion_limit_zone api 256m;

    gabion_peer_file /etc/gabion/peers.json;

    gabion_limit_rule tenant_api {
        key $http_x_tenant_id $uri;
        limit 10000r/m;
        window 60s;
        bucket 1s;
        local_fallback 1000r/m;
        local_absolute 3000r/m;
        gossip embedded;
        overflow aggregate;
    }

    server {
        location /api/ {
            gabion_limit tenant_api;
            proxy_pass http://app;
        }
    }
}
```

---

## 19. Security

### 19.1 Gossip Authentication

Unauthenticated gossip can be used for denial of service by injecting inflated counts.

Support:

```text
mTLS in standalone mode
HMAC-signed frames in simple binary mode
static peer allowlist
Kubernetes NetworkPolicy
```

Merge semantics prevent peers from lowering counters, but they do not prevent malicious peers from raising counters.

### 19.2 Peer Authorization

Kubernetes standalone mode:

```text
accept deltas only from current or recently known EndpointSlice peers
```

Static mode:

```text
accept deltas only from configured peers
```

Local-only mode:

```text
no gossip port
```

### 19.3 Cardinality Protection

Externally controlled dimensions are attacker-controlled:

```text
tenant headers
API keys
paths
query params
IP addresses
user agents
```

Enforce:

```text
max key length
max descriptor count
max descriptor bytes
max keys per rule
max cells per rule
max peers
max gossip payload
```

---

## 20. Metrics

Expose:

```text
limiter_requests_total{rule,decision}
limiter_allowed_total{rule}
limiter_rejected_total{rule,reason}
limiter_local_fallback_total{rule}
limiter_global_estimate_reject_total{rule}
limiter_overflow_key_total{rule}
limiter_fail_open_total{reason}

limiter_active_keys{rule}
limiter_active_cells{rule}
limiter_memory_bytes{rule}
limiter_evictions_total{rule}

limiter_gossip_peers
limiter_gossip_send_bytes_total
limiter_gossip_recv_bytes_total
limiter_gossip_merge_cells_total
limiter_gossip_digest_mismatch_total
limiter_gossip_truncated_total
limiter_gossip_peer_lag_seconds{peer}

limiter_discovery_peers
limiter_discovery_stale
limiter_discovery_errors_total
```

Standalone admin endpoints:

```text
GET /healthz
GET /readyz
GET /metrics
GET /debug/rules
GET /debug/peers
GET /debug/storage
```

Readiness should mean:

```text
can make local decisions
```

It should not depend on gossip health or Kubernetes API health.

---

## 21. Testing Strategy

### 21.1 Core Property Tests

Verify CRDT merge properties:

```text
idempotent
commutative
associative
monotonic
```

### 21.2 Allocation Tests

Add tests that assert:

```text
existing-key request path performs zero heap allocations
gossip encode reuses buffers
gossip decode respects fixed capacity
overflow policy triggers before unbounded allocation
```

Use a counting allocator in standalone tests.

### 21.3 Simulation Tests

Build a deterministic simulator with:

```text
N nodes
traffic skew
packet loss
network partitions
clock skew
node restarts
dirty ring overflow
memory pressure
high-cardinality attack
```

Measure:

```text
allowed traffic
rejected traffic
global overshoot
time to convergence
memory pressure
stale estimate behavior
```

### 21.4 Envoy Integration Tests

Test:

```text
single descriptor
multiple descriptors
hits_addend
OK response
OVER_LIMIT response
RLS timeout
Envoy fail-open behavior
```

### 21.5 Kubernetes Tests

Use kind or k3d.

Test:

```text
EndpointSlice watch
scale up
scale down
pod restart
Pod UID changes
API server outage
stale peer removal
```

### 21.6 NGINX Tests

Test:

```text
module loads
config parsing
single worker local-only
multi-worker shared memory
overflow behavior
reload behavior
panic-free request path
embedded gossip disabled
embedded gossip leader election, later
```

---

## 22. Implementation Phases

### Phase 1: Core Local Engine

Deliver:

```text
rule table
streaming key hash
fixed-capacity heap store
sliding bucket counters
decision engine
overflow key
metrics
zero-allocation existing-key path
```

### Phase 2: Standalone Envoy RLS, Local-Only

Deliver:

```text
tonic Envoy RLS service
descriptor mapping
hits_addend support
OK / OVER_LIMIT
admin endpoints
local-only config
```

### Phase 3: CRDT Cell Table and Gossip

Deliver:

```text
CounterCell model
cell table
dirty ring
digest generation
binary gossip protocol
static peer provider
simulation tests
```

### Phase 4: Kubernetes Discovery

Deliver:

```text
EndpointSlice watcher
peer snapshot updates
RBAC manifests
discovery stale behavior
local-only fallback
```

### Phase 5: Production Hardening

Deliver:

```text
gossip authentication
payload limits
fuzzed decoders
memory pressure tests
cardinality protections
metrics
benchmarks
```

### Phase 6: NGINX Local-Only Module

Deliver:

```text
ngx module skeleton
config directives
shared memory zone
NgxShmStore
access-phase hook
zero-allocation request path
multi-worker shared counters
```

### Phase 7: NGINX Embedded Gossip

Deliver only after Phase 6 is stable:

```text
peer file support
shared-memory leader lease
one gossip owner
preallocated gossip buffers
binary transport
local-only fallback if no gossip leader
```

---

## 23. Final Recommended Defaults

```text
availability_mode = always_local_decision
request_path_network = forbidden
request_path_heap_allocation = forbidden for existing keys
gossip_required = false
kubernetes_required = false
redis_required = false
envoy_failure_mode = fail_open
nginx_default_gossip = off
nginx_storage = shared_memory
nginx_kube_rs = no
overflow_policy = aggregate
global_stale_behavior = local_fallback
strict_quota_mode = unsupported
```

The product should describe itself as:

> **Locally enforced, globally informed, allocation-bounded, availability-first rate limiting.**
