# Gossip Protocol Gap Closure Plan

This plan turns the current local-only implementation and gossip scaffolding into the system described in `docs/Gossip Protocol Rate Limiting.md`.

The implementation priorities are:

```text
local decisions only
bounded memory
table-oriented state
no per-request network I/O
no per-request heap allocation for existing keys
simple code paths before clever ones
tests that prove behavior under pressure
```

The plan is intentionally phased. Each phase should leave the repository in a working state with focused tests and a small, understandable API surface.

---

## Ground Rules

### Data-Oriented Storage

Prefer contiguous tables and integer handles over object graphs:

```text
RuleTable
KeyTable
BucketTable or fixed bucket ring per key
CellTable
DirtyDeltaRing
PeerTable
GossipBufferPool
StatsTable
```

Avoid:

```text
HashMap per key
Vec per request
String construction on the request path
trait-object storage in shared memory
Arc/Box/String/Vec in NGINX shared memory
protobuf on the gossip path
locks held across I/O
```

Use plain handles:

```rust
struct RuleIndex(u32);
struct KeyIndex(u32);
struct CellIndex(u32);
struct PeerIndex(u16);
```

This keeps ownership simple and makes memory pressure explicit.

### Allocation Policy

Allowed:

```text
startup allocations
configuration parsing allocations
bounded table allocation
new-key allocation when capacity allows
preallocated gossip buffers
test-only allocation instrumentation
```

Forbidden:

```text
allocation on existing-key request path
allocation while decoding a bounded gossip frame
allocation to handle an unexpectedly large peer payload
unbounded descriptor/key growth
ordinary Rust heap objects in NGINX shared memory
```

### Simplicity Policy

Keep every phase small enough to reason about:

```text
one storage responsibility per type
one merge rule for CRDT cells
small public APIs
clear error enums
no hidden background work in core
adapter code owns runtimes and I/O
```

Prefer a direct implementation over a generic abstraction until two concrete adapters need the same interface.

---

## Phase 1: Core Store and CRDT Integration

Goal: local increments produce gossipable CRDT cells, and remote cells update local estimates without changing the request-path availability model.

### Work

1. Split core storage responsibilities.

   Keep `LocalEngine` as the request-facing API, but make its state explicit:

   ```text
   RuleTable
   HeapKeyTable
   LocalCellTable
   DirtyDeltaRing
   FreshnessTable
   Metrics
   ```

2. Move or mirror `CellTable` ownership so local increments can update both:

   ```text
   key bucket local_count
   key bucket estimated_total
   cell table local origin count
   dirty ring entry
   ```

3. Add node identity to engine construction:

   ```rust
   struct NodeIdentity {
       node_id: NodeId,
       incarnation: u64,
   }
   ```

4. Replace the single `last_gossip_update_millis` with rule-scoped freshness:

   ```text
   FreshnessTable[rule_index].last_update_millis
   ```

   If this later proves too coarse, extend to peer or shard freshness. Rule-level freshness is the simplest correction over a global timestamp.

5. Make descriptor matching honor configured values.

   Support:

   ```text
   exact key match
   exact value match
   wildcard value "*"
   ```

   Store matchers as compact config-time data. Do not build canonical strings on the request path.

6. Complete overflow policy enum.

   Implement in this order:

   ```text
   aggregate
   reject
   allow_untracked
   sample
   ```

   `sample` can initially be deterministic hash sampling so it does not need RNG on the request path.

### Acceptance

```text
local request increments create or update a CRDT cell
dirty ring contains local increment cells
remote stale cells do not lower estimates
freshness is rule-scoped
descriptor value wildcard and exact matching are tested
all overflow policies are tested
existing-key request path allocation test passes
```

### Tests

```text
core CRDT local increment test
remote merge idempotence/commutativity/associativity tests
rule-scoped freshness regression test
descriptor value matching tests
overflow policy tests
counting allocator test for existing-key hot path
```

---

## Phase 2: Standalone Gossip Runtime

Goal: `gabiond` can run as a distributed standalone service with static/file peers and local-only fallback.

### Work

1. Add standalone gossip runtime crate or module.

   Keep it outside `gabion-core`:

   ```text
   gabion-gossip-runtime
   or crates/bin/src/gossip_runtime.rs
   ```

2. Define transport traits around preallocated buffers:

   ```rust
   trait GossipTransport {
       fn send_to(&mut self, peer: PeerIndex, frame: &[u8]) -> SendResult;
       fn recv_into(&mut self, buffer: &mut [u8]) -> Option<RecvPacket>;
   }
   ```

3. Implement a simple Tokio UDP or TCP transport first.

   Prefer the simplest reliable implementation that preserves bounded buffers. If TCP is selected, use fixed frame size limits and read into reusable buffers.

4. Implement tick loop:

   ```text
   snapshot peers
   choose deterministic bounded fanout
   encode digest
   encode dirty cells up to max payload
   send
   receive bounded frames
   authenticate if configured
   merge cells into engine
   update freshness
   continue on every error
   ```

5. Implement digest mismatch handling.

   Start simple:

   ```text
   if peer digest differs, include shard cells up to max_payload_bytes on next tick
   if dirty ring overflowed, force shard resync
   ```

6. Wire static and file peer providers into `gabiond`.

   `discovery.kind` should support:

   ```text
   none
   static
   file
   ```

7. Expose gossip metrics.

   Include:

   ```text
   peers
   send bytes
   recv bytes
   merged cells
   decode errors
   auth failures
   truncated frames
   dirty overflow
   local-only state
   ```

### Acceptance

```text
two local gabiond processes converge through gossip
requests never wait on gossip
gossip disabled still runs local-only
peer file read failure keeps last snapshot and keeps retrying
payload limit is enforced before merge
dirty overflow triggers resync behavior
```

### Tests

```text
in-process two-node convergence test
packet loss simulation test
static peer config test
file peer retry test with paused Tokio time
bounded payload test
authenticated gossip test
metrics snapshot test
```

---

## Phase 3: Kubernetes Discovery Wiring

Goal: EndpointSlice discovery is usable by standalone `gabiond` without becoming a health dependency, and the shared discovery primitives needed by NGINX are in place.

### Work

1. Extend config:

   ```yaml
   discovery:
     kind: auto
     endpoint_slices:
       - namespace: default
         service_name: gabion-grpc
         port_name: gossip
       - namespace: default
         service_name: gabion-nginx
         port_name: gossip
   ```

   `auto` is the default. At startup, the runtime uses kube-rs in-cluster configuration inference. If kube-rs can build an in-cluster Kubernetes client, `auto` resolves to `kubernetes_endpoint_slice`; otherwise it resolves to `none` and remains local-only.

   Explicit modes still override auto:

   ```text
   kind: none = always local-only
   kind: static = use configured static peers
   kind: file = use configured peer file
   kind: kubernetes_endpoint_slice = require Kubernetes EndpointSlice config and keep retrying if the API cannot be reached
   kind: auto = Kubernetes EndpointSlice when kube-rs in-cluster config is available, local-only otherwise
   ```

   When `auto` resolves to in-cluster Kubernetes and no EndpointSlice selector is configured, default to the Services that select the running Pod:

   ```text
   read current namespace from the service-account namespace file
   read current Pod by HOSTNAME
   list Services in the namespace
   keep Services whose selector labels match the Pod labels
   prefer the Service port named gossip; if the Service has one port, use that port
   use the matched Services as EndpointSlice selectors
   ```

   If no Service selects the Pod, treat this as a configuration error in Kubernetes mode. This avoids silently starting a pod in local-only mode when it was deployed into Kubernetes without a usable Service.

   The single `namespace`/`service_name` form may remain as a compatibility shorthand, but the canonical Kubernetes form is a bounded list of EndpointSlice selectors. `port_name` is optional and defaults to the named gossip port `gossip`; if a Service has exactly one port, auto-inference may use that single port. If a Service has multiple ports and none is named `gossip`, require explicit `port_name`. This lets standalone Gabion gRPC rate-limit server pods and NGINX module pods bridge into one gossip cluster while still being exposed through separate Kubernetes Services. The gRPC server is Envoy-compatible, but it must not be modeled as running inside Envoy or requiring Envoy pods.

   Use the same logical shape for standalone `gabiond` and the NGINX module. Phase 3 wires it into `gabiond` and adds the shared NGINX runtime/config primitives; Phase 7 wires the directives, background watcher, shared peer table, and embedded gossip into the actual NGINX module.

   ```nginx
   gabion_gossip_discovery kubernetes_endpoint_slice;
   gabion_gossip_endpoint_slice default gabion-grpc gossip;
   gabion_gossip_endpoint_slice default gabion-nginx gossip;
   ```

   The parsed runtime representation should be data-oriented:

   ```text
   fixed-capacity selector array
   namespace/service/port interned or copied into bounded config storage
   one merged peer snapshot
   dedupe across all selected Services
   ignore this process's own advertised address
   ```

   Required discovery-mode parity for the shared runtime/config layer:

   ```text
   standalone Gabion gRPC server supports auto, none, static, file, Kubernetes EndpointSlice
   NGINX runtime/config primitives support auto, none, static, file, Kubernetes EndpointSlice
   both runtimes use kube-rs detection for auto Kubernetes mode
   both runtimes default to auto
   both runtimes accept multiple Kubernetes namespace/service/port selectors
   both runtimes keep the last good peer set and retry discovery failures
   both runtimes apply the same peer authorization and recent-peer grace policy
   both runtimes expose the same effective peer snapshot semantics
   ```

2. Wire `gabion-discovery` Kubernetes support into `gabiond`.

   The watcher pushes individual add/remove events into a `PeerHandler`. The gossip runtime only reads bounded snapshots from the same trait. For multi-service discovery, run one watcher per selector or one shared reflector per namespace when that remains simple; either way, merge results before pushing concrete peer additions and removals.

3. Add NGINX discovery primitives with the same discovery modes.

   Kubernetes-enabled gossip is a required NGINX capability, not a sidecar-only or future replacement path. Phase 3 adds direct Kubernetes EndpointSlice discovery helpers using the same selector list semantics as `gabiond`.

   Request-path code must not watch Kubernetes or allocate from Kubernetes data. Phase 7 must wire this into a background owner that updates a bounded peer table while workers read the latest table. If a helper process is ever used for tests or transitional packaging, it must be treated as an implementation detail and must not reduce the required NGINX config surface or behavior parity.

4. Preserve last known snapshot on watcher failure.

   Discovery failure is a retry condition. Do not mark peers stale and do not clear the peer set just because a watch failed:

   ```text
   keep last good peer set
   restart EndpointSlice watch
   push add/remove events only for observed EndpointSlice changes
   ```

5. Add peer authorization gate for received gossip:

   ```text
   accept current peers
   accept recently known peers for a short grace window
   reject unknown peers
   ```

6. Expand RBAC and deployment examples only as needed.

   RBAC must cover EndpointSlices for every configured namespace. For auto selector inference, it must also allow reading the current Pod and listing Services in the namespace. The local examples should include separate `gabion-grpc` and `gabion-nginx` Services that both expose the same named gossip port.

7. Add a guarded local Kubernetes end-to-end discovery test.

   This test must run only against an explicitly local context, such as OrbStack or kind. It must refuse to run if the current context is not allowlisted or if the Kubernetes API server is not localhost.

   Test flow:

   ```text
   create temporary namespace
   create gabion-grpc Service with named gossip port
   create gabion-nginx Service with named gossip port
   create EndpointSlices for local gossip endpoints behind both Services
   run EndpointSlice watchers against the local API server
   assert SnapshotPeerHandler receives add/update/delete changes
   assert peers from both Services are merged and deduped
   start two local gossip runtimes using the watched snapshot
   send traffic through one local limiter
   gossip dirty cells through the discovered peer snapshot
   assert the second limiter's admission changes from remote estimate
   delete one EndpointSlice
   assert peer snapshot removes that peer
   delete namespace in cleanup trap
   ```

   This is stronger than a manifest smoke test. It proves the live Kubernetes watcher, peer snapshot handoff, and local distributed rate-limiting behavior work together.

8. Defer real pod-scale convergence tests to later phases.

   Phase 3 intentionally stops at the guarded local EndpointSlice watcher plus two-node gossip convergence test. Real Deployment scale-up/scale-down tests require packaged pods and bounded introspection APIs, so they belong in Phase 5 and Phase 7. This keeps Phase 3 complete once Kubernetes discovery wiring is correct and locally exercised without pretending the stronger production-shape tests already exist.

### Acceptance

```text
EndpointSlice add/delete updates peer snapshot
multiple EndpointSlice selectors merge into one deduped peer snapshot
shared self endpoint and discovery semantics exist for the gRPC server and NGINX runtime/config layer
self endpoint is ignored
not-ready endpoints are ignored
watch failure does not stop request handling
unknown gossip sender is rejected
guarded local Kubernetes watcher test drives distributed convergence
gRPC server and NGINX runtime/config primitives both support auto/none/static/file/Kubernetes discovery modes
```

### Tests

```text
unit tests for peer set updates
mock watcher failure test
guarded OrbStack/kind EndpointSlice watcher integration test
local Kubernetes-discovered two-node gossip convergence test
multi-service EndpointSlice merge test
auto discovery mode tests
RBAC manifest validation
peer authorization test
```

---

## Phase 4: Admin, Metrics, and Operational Surface

Goal: expose enough state to operate and debug the limiter without leaking implementation complexity into core.

### Work

1. Add admin endpoints:

   ```text
   GET /healthz
   GET /readyz
   GET /metrics
   GET /debug/rules
   GET /debug/peers
   GET /debug/storage
   ```

2. Keep readiness local:

   ```text
   ready = can make local decisions
   not ready = config/store unavailable
   unrelated = Kubernetes stale, gossip stale, peer count zero
   ```

3. Add labeled Prometheus metrics.

   Prefer stable labels:

   ```text
   rule
   decision
   reason
   mode
   ```

   Avoid labels with raw descriptor values.

4. Add debug storage summaries:

   ```text
   active keys
   active cells
   dirty ring length
   dirty overflow flag
   table capacities
   estimated memory bytes
   ```

5. Add an internal introspection API for distributed test assertions.

   The standalone Gabion gRPC rate-limit server should expose this as a gRPC service on the admin listener or on a separately configured loopback/admin bind. This server is Envoy-compatible, but the API must be useful regardless of whether any pod is running Envoy. The NGINX module can expose equivalent data through an admin HTTP endpoint or a test-only helper process, but the response shape should match the standalone service closely enough for shared tests.

   Minimum response data:

   ```text
   node id and incarnation
   cluster id hash
   active peer addresses and discovery generation
   recent peer grace entries
   active rule ids
   active cell count
   bounded sample of local cells
   bounded sample of remote cells
   per-cell rule id, descriptor hash, bucket, node id, count, and last update time
   dirty ring length and overflow flag
   gossip send/receive/merge counters
   local-only and discovery-stale flags
   ```

   This API is for tests and operations, not for request-path behavior. It must be bounded by caller-supplied limits, must never allocate proportional to attacker-controlled descriptor cardinality, and must redact raw descriptor values unless an explicit debug build or config flag enables them.

   Use it in Kubernetes tests to assert convergence directly:

   ```text
   poll every pod's introspection API
   compare peer snapshots after scale events
   compare per-rule/per-descriptor-hash estimated totals
   assert convergence within configured tolerance and timeout
   assert removed peers age out after the grace window
   ```

### Acceptance

```text
admin endpoints match design document
metrics include local-only, discovery, and gossip state
debug endpoints do not allocate based on request cardinality
readiness ignores gossip/discovery health
introspection API exposes bounded peer and cell summaries for tests
introspection output can prove cross-pod rate-data convergence
```

### Tests

```text
admin route tests
metrics format snapshot tests
readyz behavior tests
storage summary tests
introspection API limit/redaction tests
introspection convergence assertion helper tests
```

---

## Phase 5: Standalone Hardening and Simulation

Goal: prove the distributed design behaves acceptably under skew, loss, restart, and memory pressure.

### Work

1. Add property-based tests for core invariants.

   Use `proptest` or an equivalent deterministic bounded property-style test framework. Keep generated cases bounded so failures are fast to reproduce and do not allocate in the hot path.

   Properties:

   ```text
   CRDT merge is idempotent
   CRDT merge is commutative
   CRDT merge is associative
   CRDT counts never decrease within a bucket
   stale remote cells never reduce local estimates
   bucket rotation never produces negative totals
   local_window_total equals the sum of live local buckets
   estimated_window_total equals live local plus live remote deltas
   global fresh decisions never exceed local_absolute_limit
   stale decisions never exceed local_fallback_limit
   overflow policies never allocate beyond configured capacity
   descriptor matching is deterministic for exact and wildcard values
   ```

2. Add property-based tests for codec and bounded decoding.

   Properties:

   ```text
   valid encoded messages round-trip
   payload limits are always enforced before callbacks
   capacity limits are always enforced before allocation-heavy decode
   visitor decode and allocating decode report the same digest/cell content
   malformed input returns an error and never panics
   authenticated frames reject any single-byte mutation
   truncated output never exceeds max_payload_bytes
   ```

3. Build deterministic simulator.

   Every external boundary must be trait-driven:

   ```text
   peer discovery pushes individual peer_added/peer_removed events through PeerHandler
   message communication uses GossipTransport, with UDP only as one implementation
   tests use in-memory transports and test PeerHandler implementations
   no test requires UDP to exercise gossip protocol semantics
   timer-driven tests use tokio paused time plus advance, or tick runtimes manually
   tests must not sleep
   ```

   Model:

   ```text
   N nodes
   skewed tenant traffic
   packet loss
   partitions
   node restart with new incarnation
   dirty ring overflow
   high-cardinality attack
   clock skew
   ```

4. Add counting allocator tests.

   Assert:

   ```text
   existing-key request path allocates zero bytes
   gossip visitor decode allocates zero bytes
   encode does not grow above configured capacity
   ```

5. Add fuzz-style decoder tests.

   Keep corpus small and deterministic initially. The important property is no panic and bounded work.

6. Add benchmarks for:

   ```text
   hot key check_and_record
   cold key admission
   cell merge
   gossip encode/decode
   ```

7. Add Kubernetes scale tests for standalone Gabion gRPC server pods.

   Once the standalone container/deployment and Phase 4 introspection API are available, the guarded local Kubernetes test should scale the deployment up and down and verify:

   ```text
   EndpointSlices reflect the new pod set
   every pod learns the expected peers
   request traffic through the gRPC rate-limit API creates local dirty cells
   gossip converges those cells across pods
   peer removal on scale-down does not stop local decisions
   newly added pods converge after joining
   removed peers age out after the grace window
   ```

### Acceptance

```text
simulation reports overshoot and convergence
memory pressure does not panic
decoders reject malformed input without panics
property failures shrink to actionable minimal cases
benchmarks run locally
standalone pods learn scaled peer sets and converge rate data
```

### Tests

```text
core proptest suite
gossip codec proptest suite
deterministic simulator tests
counting allocator tests
decoder fuzz smoke tests
benchmark targets
standalone pod scale-up/scale-down convergence test
```

---

## Phase 6: NGINX Local-Only Shared Memory

Goal: replace the current NGINX smoke-test counter with a real local-only module over shared memory.

### Work

1. Implement `NgxShmStore`.

   Shared zone contains:

   ```text
   StoreHeader
   RuleRuntimeTable
   KeyTable
   BucketTable
   StatsCounters
   ```

   Defer `CellTable`, `PeerTable`, and `LeaderLease` until embedded gossip.

2. Use fixed records and offsets only.

   Do not store:

   ```text
   Vec
   String
   Box
   Arc
   Rust references
   trait objects
   ```

3. Parse NGINX directives into compact runtime config.

   Support:

   ```nginx
   gossip_limit_zone
   gossip_limit_rule
   gossip_limit
   gossip off
   overflow aggregate
   ```

4. Implement access-phase key evaluation.

   ```text
   evaluate configured variables
   stream hash components
   lookup key slot
   rotate bucket
   decide
   increment
   return NGX_DECLINED or 429
   ```

5. Add panic boundary policy.

   Request handler must avoid `unwrap`, `expect`, and unchecked indexing. Convert errors into fail-open or 429 according to config.

6. Build real NGINX integration coverage.

   Use the pinned containerized NGINX build from `deploy/nginx`. Tests should load the compiled `.so` into actual NGINX, issue HTTP requests, and assert behavior through NGINX rather than only through Rust unit tests.

7. Test multiple configuration shapes.

   Minimum local-only matrix:

   ```text
   one worker, one rule, one key component
   one worker, one rule, multiple key components
   one worker, multiple locations sharing one rule
   one worker, multiple rules with different limits
   multiple workers sharing one zone
   multiple zones with independent counters
   small zone forcing overflow aggregate
   missing variable fail-open behavior
   reload with equivalent config
   reload with changed limit
   invalid config rejected at nginx -t
   ```

8. Add black-box assertions for actual NGINX behavior.

   Validate:

   ```text
   allowed requests pass to upstream
   rejected requests return 429
   counters are shared across workers
   counters are isolated across zones
   configured key components affect the selected counter
   window expiration allows traffic again
   reload does not corrupt shared state
   module unload/restart starts from empty or documented state
   ```

### Acceptance

```text
NGINX module uses shared memory counters
multiple workers share the same counts
existing-key request path does not allocate
module load smoke test passes
config parsing supports documented local-only shape
overflow aggregate works in NGINX
actual NGINX integration tests cover multiple configurations
invalid NGINX configs fail during nginx -t
```

### Tests

```text
module load integration test
single-worker local-only test
multi-worker shared counter test
multi-zone isolation test
multi-rule config test
multi-key-component config test
overflow behavior test
reload behavior test
invalid config nginx -t tests
Docker-backed actual NGINX request tests
panic-free request handler review/test
```

---

## Phase 7: NGINX Embedded Gossip

Goal: add optional NGINX gossip after shared-memory local limiting is stable.

### Work

1. Extend shared memory with:

   ```text
   PeerTable
   CellTable
   DirtyDeltaRing
   LeaderLease
   GossipStats
   ```

2. Connect local request increments to shared CRDT cells.

   The request path updates local cell counts and dirty entries without network I/O.

3. Make the leader gossip real data.

   Replace empty message send with:

   ```text
   digests from CellTable
   dirty cells from DirtyDeltaRing
   resync cells when needed
   ```

4. Keep peer discovery out of the request path, but support every discovery mode in the module.

   Support:

   ```text
   auto
   none
   static peers
   peer file
   direct Kubernetes EndpointSlice selectors matching standalone gabiond config
   keep last known peer snapshot
   retry peer file read failures without clearing peers
   retry Kubernetes watch failures without clearing peers
   ```

   Kubernetes discovery must accept a bounded list of namespace/service/port selectors so NGINX pods and standalone Gabion gRPC server pods can gossip in the same cluster. This is required behavior for the NGINX module. A sidecar or helper may be used only as a packaging detail if the module still owns the same config semantics, retry behavior, and effective peer snapshot behavior.

   Discovery-mode parity requirement:

   ```text
   gRPC server and NGINX support the same discovery modes
   gRPC server and NGINX both default to auto
   gRPC server and NGINX both use kube-rs detection for auto Kubernetes mode
   gRPC server and NGINX support the same multi-service Kubernetes selector model
   gRPC server and NGINX expose comparable peer snapshots through introspection
   gRPC server and NGINX converge with either side scaled independently
   ```

5. Keep gossip ownership explicit.

   One worker owns gossip through `LeaderLease`. If no worker owns it, request handling continues local-only.

6. Extend actual NGINX integration coverage for embedded gossip.

   Minimum embedded matrix:

   ```text
   two NGINX containers with static peers
   two NGINX containers with peer files
   peer file removed or malformed after startup
   HMAC enabled on both sides
   HMAC mismatch
   one gossip leader per NGINX instance
   leader worker exits or NGINX reloads
   one instance partitioned, then healed
   ```

7. Add Kubernetes scale tests for NGINX embedded gossip.

   After `NgxShmStore` and embedded gossip are implemented, the local Kubernetes test should deploy NGINX module pods and verify:

   ```text
   NGINX pods discover each other through static/file peers or EndpointSlice selectors
   NGINX pods discover standalone Gabion gRPC server pods from a second Service selector
   standalone Gabion gRPC server pods discover NGINX pods from the NGINX Service selector
   scaling NGINX pods up adds peers without request-path blocking
   scaling NGINX pods down removes peers after the grace window
   request traffic to one pod creates shared local cells
   embedded gossip converges rate data across pods
   NGINX and gRPC server pods converge on the same rate data while both Services are scaled
   every pod enforces the same converged estimated rate data within the AP tolerance
   local-only fallback continues if the gossip leader disappears
   ```

8. Add all-mode cross-runtime tests.

   ```text
   NGINX plus gRPC server in auto mode stays local-only outside Kubernetes
   NGINX plus gRPC server in auto mode discovers both Services in Kubernetes
   NGINX plus gRPC server in none mode stays local-only
   NGINX plus gRPC server in static mode converges
   NGINX plus gRPC server in file mode converges and handles stale files
   NGINX plus gRPC server in Kubernetes mode discovers both Services and converges
   all modes preserve request-path allocation bounds
   ```

### Acceptance

```text
one NGINX worker owns gossip at a time
embedded gossip exchanges real cells
remote cells update shared estimates
peer file failure does not stop request handling
wrong cluster/self packets are ignored
HMAC-authenticated frames are supported
actual multi-instance NGINX tests show convergence
NGINX keeps serving local-only during gossip failures
scaled NGINX pods learn peers and converge rate data
scaled NGINX and gRPC server pods bridge through multi-service discovery
NGINX and gRPC server support the same discovery modes and auto default
```

### Tests

```text
leader election test
leader failover test
peer file reload/stale test
two-module convergence test
two-container NGINX convergence test
partition/heal NGINX test
HMAC mismatch NGINX test
Kubernetes NGINX scale-up/scale-down convergence test
Kubernetes mixed NGINX plus gRPC server scale convergence test
cross-runtime all-discovery-mode convergence tests
authenticated packet test
shared-memory cell merge test
```

---

## Phase 8: Documentation and Production Readiness

Goal: make the implementation understandable, supportable, and difficult to misuse.

### Work

1. Update the design document where implementation choices differ.

2. Add deployment docs:

   ```text
   standalone local-only
   standalone static gossip
   standalone Kubernetes discovery
   NGINX local-only
   NGINX embedded gossip
   ```

3. Add operational guidance:

   ```text
   Envoy fail-open config
   memory sizing
   choosing local_fallback_limit
   choosing local_absolute_limit
   interpreting stale gossip
   expected overshoot
   unsupported strict quota use cases
   ```

4. Add a compatibility matrix:

   ```text
   crate
   feature
   dependencies
   runtime assumptions
   allocation posture
   ```

### Acceptance

```text
docs match shipped behavior
examples run in CI or documented smoke tests
strict-quota non-goal is visible
NGINX build posture is documented by version
```

---

## Recommended Sequence

Do not start with NGINX embedded gossip. The lowest-risk order is:

```text
1. Core store and CRDT integration
2. Standalone static/file gossip
3. Kubernetes discovery wiring
4. Admin and metrics
5. Simulation and hardening
6. NGINX shared-memory local-only
7. NGINX embedded gossip
8. Documentation polish
```

This sequence gets distributed standalone behavior working before carrying the same state model into NGINX shared memory.

---

## Near-Term First Pull Requests

### PR 1: Core Matcher and Freshness Cleanup

```text
add descriptor value/wildcard matching
replace global freshness timestamp with rule freshness table
add tests for both
```

### PR 2: Local Increment Cell Recording

```text
add node identity to LocalEngine
record local CRDT cells on successful increments
mark dirty entries
test local cells and dirty ring
```

### PR 3: Standalone Static Gossip Loop

```text
add static peer config
add bounded transport loop
send dirty cells
merge received cells
add two-node convergence test
```

### PR 4: Metrics and Debug Endpoints

```text
add gossip/discovery/storage metrics
add /debug/rules, /debug/peers, /debug/storage
add route tests
```

These first four PRs close the highest-value distributed gaps without touching NGINX module complexity.
