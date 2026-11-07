# gabiond — gabion's gRPC adapter

`gabiond` is gabion's standalone rate-limit service: an Envoy-compatible
gRPC server that any sidecar or proxy speaking
`envoy.service.ratelimit.v3` can call. It shares the CRDT and gossip
machinery with the nginx module, so the two adapters can coexist in a
single cluster and count a rule written the same way in both against one
total; see
[Writing the same rule for NGINX and gabiond](../gabion/README.md#writing-the-same-rule-for-nginx-and-gabiond).
If you are not running
Envoy — or you want enforcement inside nginx itself — see
[`../nginx/README.md`](../nginx/README.md). Every setting is in the
[Configuration reference](#configuration-reference) below; the gossip
protocol is explained in [`../gabion/GOSSIP.md`](../gabion/GOSSIP.md), and
the knobs shared with NGINX in
[`../gabion/README.md#operator-knobs`](../gabion/README.md#operator-knobs).

## Contents

- [Your first YAML](#your-first-yaml)
- [How configuration layers](#how-configuration-layers)
- [Configuration reference](#configuration-reference)
- [Deploying on Kubernetes](#deploying-on-kubernetes)
- [Admin endpoint](#admin-endpoint)
- [Logging](#logging)
- [Glossary](#glossary)
- [Common patterns](#common-patterns)
- [Running across a cluster](#running-across-a-cluster)
- [Exit codes and HTTP statuses](#exit-codes-and-http-statuses)
- [Troubleshooting](#troubleshooting)
- [Fail-open invariant](#fail-open-invariant)

## Your first YAML

A single-node `gabiond` configuration sets the Envoy gRPC bind, an
admin HTTP port, at least one rule, and a gossip socket. The gossip
socket is mandatory even with no peers, so a future peer can join
without restarting the process.

```yaml
# Where Envoy (or any v3 ratelimit client) reaches gabiond.
envoy_bind: 0.0.0.0:8081

# HTTP endpoint for /snapshot (peer + cell view).
admin_bind: 0.0.0.0:9090

# One rule: 100 requests/second per IP. Envoy emits a descriptor
# whose key is `remote_address`; gabion keys the counter on the value.
limits:
  - name: per_ip
    domain: envoy
    descriptors:
      - key: remote_address
    rate: 100r/s

# Gossip channel — required even on a single node.
gossip:
  bind: 0.0.0.0:9000
  cluster_id_hash: 0xc0ffee   # any non-zero u128 shared across peers
```

Run it:

```bash
gabiond /etc/gabion/config.yaml
```

Point Envoy's rate-limit filter at `gabiond.<namespace>.svc:8081` (or
wherever you bound `envoy_bind`). Envoy emits one RPC per request, and
gabiond evaluates every descriptor in that RPC against the rules its
`(domain, key)` matches, records the allowed ones for gossip to send to
its peers, and replies. The reply's `overall_code` is `OVER_LIMIT` if any
descriptor was rejected, while the per-descriptor codes ride in
`statuses[]` so a partial reject does not fail the whole batch.

## How configuration layers

Most operators ship one ConfigMap to every replica and override a
couple of values — typically the gossip bind and the pod IP — through
per-pod env vars, which is the shape gabiond is built for.

```
┌─────────────────────────────────────────────────────────────┐
│ Environment variables (last word)                           │
│   GABION_GOSSIP_BIND, GABION_DISCOVERY_SELF_ADDR, …         │
│   Scalars and comma-separated lists. Cannot express         │
│   `limits[]` or anything with nested duration fields.       │
├─────────────────────────────────────────────────────────────┤
│ YAML file (the shape)                                       │
│   /etc/gabion/config.yaml. Full schema including rules.     │
├─────────────────────────────────────────────────────────────┤
│ Built-in defaults                                           │
│   `Settings::default()` in `gabion::settings`.              │
└─────────────────────────────────────────────────────────────┘
```

Every setting has exactly one `GABION_*` env var, named in the one
settings table both adapters share (`Setting` in `gabion::settings`,
which also gives each setting's NGINX directive). Comma-separated values
feed list settings, as in `GABION_DISCOVERY_NAMESPACE_ALLOW=ns-a,ns-b`;
in YAML a list setting is a sequence (`namespace_allow: ["ns-a"]`) or
one name. A list variable that is set but empty fails to load rather
than silently replacing the YAML list with nothing. Structured lists such as the `limits:` block, along with
anything that contains nested durations, live in YAML only because flat
env vars cannot express them.

gabiond parses and validates every value once, at startup, before it
binds a socket: a zero count, a malformed value, an out-of-range value
or a YAML key the table and the `limits:` fields do not know (a typo, or
a key a release removed) stops startup with a message that names the
setting in both spellings. See [Troubleshooting](#troubleshooting).

## Configuration reference

Every setting `gabiond` reads, block by block. The YAML key is also the
`config_key` field of every message that names the setting; the environment
variable wins over the file. Durations are written in whole milliseconds or
larger units (`500ms`, `10s`); lists are YAML sequences, or comma-separated in
the environment variable. The rules themselves (`limits:`) are YAML only, see
[Rate, window, and bucket](#rate-window-and-bucket). The same settings in
NGINX, and which are common and which advanced, are in the
[library's knob tables](../gabion/README.md#operator-knobs); the memory each
one sizes is in its [Sizing](../gabion/README.md#sizing) section.

### Listeners

| Key | Environment variable | Default | Meaning | Change it when |
|---|---|---|---|---|
| `envoy_bind` | `GABION_ENVOY_BIND` | unset: no gRPC listener | `host:port` Envoy's rate-limit filter calls. | Always set it; without it `gabiond` serves no decisions. |
| `admin_bind` | `GABION_ADMIN_BIND` | unset: no admin endpoint | `host:port` of the [admin endpoint](#admin-endpoint), `/snapshot` and `/readyz`. | Set it for a readiness probe or to inspect a node. |

### `storage`

| Key | Environment variable | Default | Meaning | Change it when |
|---|---|---|---|---|
| `storage.max_cells` | `GABION_STORAGE_MAX_CELLS` | `131072` | Counter cells, one per key, bucket and origin node: about 96 bytes each, plus 128 in the aggregate table. | `cell_store_full` or `aggregate_full`. |
| `storage.max_descriptor_count` | `GABION_STORAGE_MAX_DESCRIPTOR_COUNT` | `16` | Descriptors one rule may declare, up to 65,535. | A rule fails to load with too many descriptors. |
| `storage.max_descriptor_bytes` | `GABION_STORAGE_MAX_DESCRIPTOR_BYTES` | `512` | Bytes of domain, keys and values in one descriptor. | `cardinality_rejected` with `limit = "bytes"` for legitimate traffic. |
| `storage.max_key_bytes` | `GABION_STORAGE_MAX_KEY_BYTES` | `128` | Bytes in one descriptor key. | A rule fails to load with a long key, or `cardinality_rejected` with `limit = "key_bytes"`. |
| `storage.max_request_descriptors` | `GABION_STORAGE_MAX_REQUEST_DESCRIPTORS` | `16` | Descriptors of one request `gabiond` evaluates. | `cardinality_rejected` with `limit = "request_descriptors"`. |
| `storage.max_matched_rules` | `GABION_STORAGE_MAX_MATCHED_RULES` | `16` | Rules one request is counted against; 32 bytes a rule per runtime thread. | `matched_rule_overflow` (advanced). |
| `storage.rule_dictionary_capacity` | `GABION_STORAGE_RULE_DICTIONARY_CAPACITY` | max(256, 2 × configured rules) | Distinct rules the counter store tracks, this node's and those peers gossip. | `rule_dictionary_full` (advanced). |
| `storage.node_dictionary_capacity` | `GABION_STORAGE_NODE_DICTIONARY_CAPACITY` | twice `gossip.peer_capacity` | Distinct peer instances (a restart is a new one) with live counts. | `node_dictionary_full` (advanced). |

### `gossip`

| Key | Environment variable | Default | Meaning | Change it when |
|---|---|---|---|---|
| `gossip.bind` | `GABION_GOSSIP_BIND` | unset; required | UDP `host:port` this node gossips on, even alone. | Always set it. |
| `gossip.cluster_id_hash` | `GABION_GOSSIP_CLUSTER_ID_HASH` | `1` | Non-zero id, decimal or `0x` hex, every node of one cluster shares. A label, not a secret. | Always set your own; `default_cluster_id` says when a node runs with the default. |
| `gossip.zone` | `GABION_GOSSIP_ZONE` | unset: from the pod's EndpointSlice entry | This node's availability zone, like a Kubernetes label value. | Outside Kubernetes, or when `zone_unknown` says the entry has none. |
| `gossip.bootstrap_peers` | `GABION_GOSSIP_BOOTSTRAP_PEERS` | unset: Kubernetes discovery | Static peers, `ip:port`; a non-empty list replaces discovery and may name this node. | Running outside Kubernetes. |
| `gossip.peer_capacity` | `GABION_GOSSIP_PEER_CAPACITY` | `512` | The most peers a node gossips with, about 330 bytes each. | `peer_table_full`. |
| `gossip.tick_interval` | `GABION_GOSSIP_TICK_INTERVAL` | `500ms` | The heartbeat, also the retransmission timeout. | Rarely; `setting_warning` warns below 20 ms. |
| `gossip.target_err_bps` | `GABION_GOSSIP_TARGET_ERR_BPS` | `100` | Per-rule error budget in basis points of the rule's limit, 1 to 10,000. | A hot rule needs a tighter bound, or gossip bytes matter more. |
| `gossip.limit_queue_capacity` | `GABION_GOSSIP_LIMIT_QUEUE_CAPACITY` | `8192` | Counted hits waiting for the gossip task. | `request_queue_full` (advanced). |
| `gossip.recv_buffer_bytes` | `GABION_GOSSIP_RECV_BUFFER_BYTES` | `4194304` | Receive buffer the gossip socket asks the kernel for. | `socket_buffer_capped` (advanced). |
| `gossip.send_buffer_bytes` | `GABION_GOSSIP_SEND_BUFFER_BYTES` | `1048576` | Send buffer the gossip socket asks the kernel for. | `socket_buffer_capped` or `send_blocked` (advanced). |
| `gossip.suspect_after` | `GABION_GOSSIP_SUSPECT_AFTER` | max(4 × `gossip.tick_interval`, 2s) | How long a packet to a peer may go unanswered before the peer is suspect and routed around; at least two heartbeats. | `gateway_failover` repeats (advanced). |
| `gossip.sweep_concurrency` | `GABION_GOSSIP_SWEEP_CONCURRENCY` | `4` | Join sweeps this node answers at once, 1 to 255. | `sweep_declined` outside rollouts (advanced). |
| `gossip.zone_grace` | `GABION_GOSSIP_ZONE_GRACE` | `10s` | How long after the first peer event the node waits for its own EndpointSlice entry before `zone_unknown`. | `zone_unknown` followed soon by `zone_known` (advanced). |
| `gossip.max_payload_bytes` | `GABION_GOSSIP_MAX_PAYLOAD_BYTES` | `1400` | Largest gossip packet, 158 to 65,507 bytes; the same on every node. | `decode_rejected` with `reason = "peer_limits"` (advanced). |
| `gossip.min_emit_interval` | `GABION_GOSSIP_MIN_EMIT_INTERVAL` | `5ms` | Floor between two threshold or prompt rounds, at least 1 ms and below `gossip.tick_interval`; it caps a node's packet rate. | `rounds_threshold` and `packets_sent` in `/snapshot` climb far above normal (advanced). |

### `discovery`

| Key | Environment variable | Default | Meaning | Change it when |
|---|---|---|---|---|
| `discovery.namespace_allow` | `GABION_DISCOVERY_NAMESPACE_ALLOW` | unset: the pod's own namespace | Namespaces whose Services discovery watches. | Peers run in other namespaces. |
| `discovery.service_allow` | `GABION_DISCOVERY_SERVICE_ALLOW` | unset: every Service with a UDP port named `gabion` | Services discovery reads peers from. | Several gabion clusters share a namespace. |
| `discovery.self_addr` | `GABION_DISCOVERY_SELF_ADDR` | unset: found by pod name | This pod's IP and gossip port. | Only for `hostNetwork` pods, as `$(POD_IP):9000`. |

### `runtime`

| Key | Environment variable | Default | Meaning | Change it when |
|---|---|---|---|---|
| `runtime.node_id_seed` | `GABION_RUNTIME_NODE_ID_SEED` | unset: the host name | The text this node's stable id is hashed from: the host name, else the outbound IP address. | Two nodes share a host name. |

## Deploying on Kubernetes

[`deploy/kubernetes/gabiond.yaml`](../../deploy/kubernetes/gabiond.yaml) is a
complete starting point: a ConfigMap holding the YAML, a Deployment, and a
Service whose UDP port is named `gabion`, which is how discovery finds the
peers. Apply it with the RBAC that discovery needs and the NetworkPolicy that
keeps the gossip port private:

```sh
kubectl apply -f deploy/kubernetes/gabion-discovery-rbac.yaml \
  -f deploy/kubernetes/gabiond.yaml \
  -f deploy/kubernetes/gabion-networkpolicy.yaml
```

- **Readiness.** The Deployment probes `GET /readyz` on `admin_bind`, which
  is unset by default, so the ConfigMap sets it. `/readyz` answers while the
  process runs; `gabiond` exits when gossip or a listener fails, so a static
  probe is enough (see [Admin endpoint](#admin-endpoint)).
- **Zones** need no setting: each pod reads its zone from its own
  EndpointSlice entry. `discovery.self_addr` is only for `hostNetwork` pods.
- **Rollouts.** Every pod of a cluster runs one release; roll a new one fast
  ([Upgrading](../gabion/README.md#upgrading)).

The root README's [Kubernetes checklist](../../README.md#kubernetes-checklist)
says what each object is for, and
[`deploy/kubernetes/README.md`](../../deploy/kubernetes/README.md) what to
edit in the manifests.

## Admin endpoint

`admin_bind` serves two routes, over plain HTTP; keep the port inside the
cluster.

| Route | Status | Body |
|---|---|---|
| `GET /snapshot` | 200 | The node's state as JSON (below). |
| `GET /snapshot` | 503 | Once the gossip task has stopped: the `admin_unavailable` text. |
| `GET /readyz` | 200 | `ready`. |

`/readyz` proves only that the admin server answers. It checks neither gossip
nor the gRPC listener, because `gabiond` exits with status 1 when either
fails, so the restarted pod is what a failure looks like.

`/snapshot` holds:

| Field | What it is |
|---|---|
| `zone` | This node's zone: `name` and `source` (`config` or `endpoint_slice`), or for an unknown zone a null `name`, `source` `none` and the `reason`. |
| `local_identity` | `node_id` and `incarnation`, which changes on every restart. |
| `peers` | One entry per peer: `addr`, `node_id` (null until heard), `zone` (`"same"`, `"unknown"` or the remote zone's hash; provisional until the peer's headers name one), `role` (`unheard`, `zone`, `gateway`, `remote` or `incompatible`), `answer` (`settled`, `owed`, `suspect` or `cannot_relay`), `unanswered_ms` (how long its oldest packet has waited for an answer), `own_unacked` and `relay_unacked`. |
| `peers_by_role` | How many peers hold each role; `incompatible` above 0 means a peer runs another wire version. |
| `store_stats` | The counter store: `active_cells` of `cell_capacity`, rule and node dictionary use, and the `*_full_rejects` counts behind each "full" warning. |
| `counters` | Since start: rounds by kind, packets and bytes sent and received, `bytes_sent_cross_zone`, cells merged, rewinds and gap reports, `gateway_failovers`, `relay_stalls`, sweeps, `send_blocked`, `send_errors` and the `decode_rejects_*` counts. |
| `rules` | The loaded rules, each with its `fingerprint`, `limit`, `window_millis`, `bucket_millis` and `live_buckets`. |
| `aggregate_rows` | Occupied slots of the aggregate table admission reads. |
| `last_error_budget` | The error budget, in hits, of the last rule recorded. |

### Verify convergence

```bash
curl -s "$ADMIN_HOST:9090/snapshot" | jq '.peers | length'
curl -s "$ADMIN_HOST:9090/snapshot" | jq '.store_stats.active_cells'
curl -s "$ADMIN_HOST:9090/snapshot" | jq '.peers_by_role'
```

A cluster has converged when every node's `peers | length` reflects
the expected topology and `active_cells` is the same order of
magnitude on each.

## Logging

`gabiond` writes `tracing`'s plain-text format to standard output, one line
per event: time, level, target, the message, then its fields as `key=value`.
`RUST_LOG` sets what is written (default `info,gabion=info,gabion_server=info`;
for example `RUST_LOG=warn` keeps warnings and errors only). Every line an
operator acts on carries a `code` field, the key of its entry in
[Troubleshooting](#troubleshooting) or in the
[library's catalogue](../gabion/TROUBLESHOOTING.md), written as
`code="gabiond_started"`; filter on it to find one kind of event.

## Glossary

The core vocabulary — rule, descriptor, rate, window, bucket,
cardinality, fail-open — is defined once in the
[root glossary](../../README.md#glossary). Server-specific terms:

| Term                 | Definition                                                                                                                  |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------|
| **Envoy domain**     | The `domain:` field on a rule. Matched against the `domain` field on the inbound `RateLimitRequest`; Envoy filters set this per-route. |
| **Descriptor**       | Envoy's term for the `(key, value)` pairs each request carries. Gabion's `descriptors:` patterns match against them.        |
| **Read-then-record** | Each descriptor is evaluated against the cluster aggregate first; only allowed descriptors record into gossip.              |
| **`/snapshot`**      | Admin HTTP endpoint returning the full peer + cell view. The fastest way to verify a cluster has converged.                 |

## Common patterns

### Limit per IP

```yaml
limits:
  - name: per_ip
    domain: envoy
    descriptors:
      - key: remote_address
    rate: 100r/s
```

Envoy's `remote_address` action emits `("remote_address", "1.2.3.4")`
per request, and gabion keys the counter on the descriptor's value.

### Limit per tenant header

```yaml
limits:
  - name: per_tenant
    domain: envoy
    descriptors:
      - key: tenant
    rate: 1000r/m
```

Configure Envoy to extract the header into a descriptor:

```yaml
# Envoy route_config snippet
rate_limits:
  - actions:
      - request_headers:
          header_name: x-tenant-id
          descriptor_key: tenant
```

Each tenant then gets an independent 1000-per-minute budget.

### Roll out a new limit safely

```yaml
limits:
  - name: new_route
    domain: envoy
    descriptors:
      - key: route
    rate: 50r/m
    mode: dry_run        # evaluate + record, never reject
```

In `dry_run` mode the rule is evaluated and its hits are recorded into
gossip, but no request is rejected. To see what would have happened,
look at the per-descriptor `code` field in the gRPC `statuses[]` array
(Envoy's access logs can surface this), and check `/snapshot` to
confirm the rule is accumulating counts. Once the numbers look right,
drop the `mode:` line to start enforcing.

### Stack per-IP and per-tenant

Each YAML rule is an independent gate. A request is evaluated against
every rule its descriptors match, and if any rule rejects then that
descriptor's status is `OVER_LIMIT` and `overall_code` is `OVER_LIMIT`
as well.

```yaml
limits:
  - name: per_ip
    domain: envoy
    descriptors: [{ key: remote_address }]
    rate: 100r/s
  - name: per_tenant
    domain: envoy
    descriptors: [{ key: tenant }]
    rate: 1000r/m
```

The Envoy filter emits two descriptors per request, one per action.

### Rate, window, and bucket

The concept is defined in the
[root glossary](../../README.md#glossary). The YAML surface:

* `rate:` (mandatory) — `Nr/<unit>`, e.g. `10r/s`, `100r/5m`,
  `1000r/h`.
* `window:` (optional) — the time horizon. Defaults to the rate's
  period. Set it longer to scale the limit up to
  `floor(rate_count * window / period)`.
* `bucket:` (optional) — granularity inside the window, no longer than
  the window. Defaults to the resolved window (one fixed-window bucket).
  Set smaller for sliding-window-style enforcement.

Worked example:

```yaml
limits:
  - name: per_tenant
    domain: envoy
    descriptors: [{ key: tenant }]
    rate: 10r/s
    window: 5h
    bucket: 1h
```

resolves to `limit = 10 * 5 * 3600 = 180_000`, `window = 5h`,
`bucket = 1h` (five live buckets). The same triple is equivalent to
`rate: 180000r/5h` with `bucket: 1h`.

> **Rule of thumb.** A `window:` longer than the rate's period with
> the default `bucket:` is a *burstable* budget — clients can fire the
> whole window's allowance instantly, then sit empty until the window
> rolls. For sustained-rate enforcement, set `bucket:` close to the
> rate's period. `rate: 10r/s, window: 5h, bucket: 1s` keeps the
> 180k-request 5-hour budget but smooths it to roughly 10 r/s.

A `window:` shorter than the rate's period is refused at config-load
time because it has nothing to scale the rate over. To enforce "100
in 500ms", write `rate: 100r/500ms`.

## Running across a cluster

The cross-cutting plumbing — binding a gossip socket, picking a
cluster id, telling peers how to find each other — is documented in
the root README's
[Running across a cluster](../../README.md#running-across-a-cluster).
The `gabiond` keys for each piece are in the
[Configuration reference](#configuration-reference): `gossip.bind` and
`gossip.cluster_id_hash`, then `discovery.*` on Kubernetes or
`gossip.bootstrap_peers` and `gossip.zone` outside it (see
[`../gabion/README.md#zones`](../gabion/README.md#zones)). Discovery needs
the Role in the root README's
[Kubernetes checklist](../../README.md#kubernetes-checklist): `get` on
Pods and `get`/`list`/`watch` on Services and EndpointSlices, bound to the
pod's ServiceAccount
([`deploy/kubernetes/gabion-discovery-rbac.yaml`](../../deploy/kubernetes/gabion-discovery-rbac.yaml)).
Without it, discovery logs `kubernetes_forbidden` and the peer set stays
empty.

The defaults need no tuning at production sizes: in the study they
converged with a 99th percentile of 337 ms at 80 pods and 301 ms at 183
([What we measured](../gabion/README.md#what-we-measured)). When you do tune,
the **adaptive emit rate** (threshold-triggered emissions between
heartbeats) is governed by `gossip.target_err_bps` and
`gossip.min_emit_interval`; the math and tradeoffs live in
[`../gabion/GOSSIP.md`](../gabion/GOSSIP.md#adaptive-emit-rate).

## Exit codes and HTTP statuses

| Surface | Value | Meaning |
|---|---|---|
| gabiond exit | 0 | Clean shutdown after SIGTERM or SIGINT. |
| gabiond exit | 1 | Startup or runtime failure, said once by one error line: `gabiond_exit` (a startup stage), `listener_failed` (the gRPC, admin or gossip address could not be bound), `server_stopped` (the gRPC or admin server failed after binding) or `gossip_stopped`. |
| gabiond exit | 101 | Panic, including one in a gabiond task. The panic message on stderr is a gabion bug report. |
| `/readyz` | 200 | Always `ready`: it shows only that the admin HTTP server answers. It checks neither gossip nor the gRPC listener; gabiond exits when either fails, so a static probe is enough. |
| `/snapshot` | 200 / 503 | 503 with the `admin_unavailable` body once the gossip task has stopped. |
| gRPC response | `OVER_LIMIT` | A request over a rule's limit; Envoy turns it into its configured status, 429 by default. No log line. |
| gRPC response, cardinality | `OVER_LIMIT` | A request over `storage.max_descriptor_*` or `storage.max_request_descriptors`; the `cardinality_rejected` line tells it apart. |

## Troubleshooting

Every operator-facing warning answers three questions: what happened,
why it is likely happening, and the next thing to try. Warnings that
can fire at request rate are throttled, so a misbehaving client at
high rate produces a handful of log lines rather than a continuous
flood. Messages from the gabion library (gossip, discovery, the counter
store) are in [`../gabion/TROUBLESHOOTING.md`](../gabion/TROUBLESHOOTING.md);
the entries below are `gabiond`'s own. A startup failure, including
every configuration error in the table below, reaches the log as the
`error` field of one `gabiond_exit` line, and `gabiond` exits with status 1.

| Symptom (verbatim from the error string)                                                                                       | What it means                                                                                          | Fix                                                                                                                          |
|--------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `` gabiond could not read its configuration: … missing field `name` ``                                                         | A `limits:` entry is missing a required field.                                                         | Supply `name`, `domain`, `descriptors`, and `rate`. `window` and `bucket` are optional.                                       |
| `` Two rules are named `X`. ``                                                                                                  | Two `limits:` entries share a `name:`.                                                                 | Pick distinct names.                                                                                                          |
| `` Rules `X` and `Y` count the same keys with the same domain, descriptors, window and bucket … ``                             | Two rules share an identity (they may differ only in `rate:` count or `mode:`), so their counters could not be told apart. | Give one a different descriptor, `window:` or `bucket:`, or merge them.                                                       |
| `` Rule `X` has an invalid rate: …. ``                                                                                         | The `rate:` string didn't parse, its count is zero, or its period is under 1 ms.                      | Write a count of at least 1 and a period of at least 1 ms: a unit (`s`, `m`, `h`, `d`) or a duration such as `500ms` or `5m`. A zero rate is refused because it would deny all traffic. |
| `` Rule `X`: the window is shorter than the rate's period, so the rate cannot be spread over it. … for example `100r/500ms` ``   | An explicit `window:` was shorter than the rate's period; a window shorter than the rate's period has nothing to scale the rate over. | Move the period into the rate itself (`rate: 100r/500ms`) instead of pairing a short `window:` with a longer period.          |
| `` Rule `X`: the window is longer than gabion tracks … ``                                                                      | The window is 49 days or more, or spans 2,147,483,648 buckets or more.                                 | Use a shorter `window:` or a larger `bucket:`.                                                                                |
| `` Rule `X`: the bucket is longer than the window … ``                                                                         | `bucket:` is longer than the resolved window, so a counted hit would outlive the window it counts toward. | Use a `bucket:` no longer than the window, or omit it for one bucket per window.                                              |
| `` Rule `X` has descriptor key `Y`, but keys must match `[A-Za-z_][A-Za-z0-9_.-]*` … ``                                         | A descriptor key uses an unsupported character.                                                        | Use identifier-like names. Underscore, dot, and dash are allowed inside; the first character must be a letter or underscore.  |
| `` Rule `X` has a domain … ``                                                                                                  | `domain:` is empty, longer than 255 bytes, or holds whitespace or a control character.                 | Use 1 to 255 bytes with no whitespace, the same domain an NGINX twin of the rule names in `domain=`.                          |
| `` Rule `X` has no descriptors … ``, `` Rule `X` has N descriptors, above … ``, `` Rule `X` has descriptor key `Y` of N bytes, above … `` | The rule has no `descriptors:`, more than `storage.max_descriptor_count`, or a key longer than `storage.max_key_bytes`. | Add a descriptor, or shorten the rule or raise the named setting.                                                             |
| `` N rate-limit rules are configured, but `storage.rule_dictionary_capacity` … is M. … ``                                      | `storage.rule_dictionary_capacity` is set below the number of rules.                                   | Remove it to use the default (twice the configured rules, at least 256), or raise it.                                         |
| `` The configuration file sets `X`, which gabiond does not know. … ``                                                            | The YAML sets a key that is not a setting or a field of a `limits:` entry: a typo, or a key this release removed or renamed. | Correct or delete the key; the release notes name each removed key's replacement.                                             |
| `` The configuration file sets `X` (…) to …, which gabiond cannot read as its value. … ``                                       | A table, a list where one value belongs, or an unquoted integer above 9223372036854775807, which YAML reads as a fraction and rounds. | Fix the indentation or quote the number, or remove the key.                                                                   |
| `` `X` is 0, but it must be at least 1: … ``                                                                                    | A count, capacity or duration was set to 0, which would disable what it sizes.                         | Remove the setting to use the default the message names, or set a positive value.                                             |
| `` `X` is `V`, which is not …, for example `E`. … ``                                                                            | The value does not parse as the setting's type (an integer in range, a duration in whole milliseconds such as `500ms`, a socket address, a Kubernetes name). | Write it like the example, or remove it to use the default.                                                                   |
| `` `X` is N; gabion supports MIN to MAX. … ``                                                                                   | A capacity is outside the range gabion can hold; the message gives the reason.                         | Set it inside the range, or remove it to use the default.                                                                     |
| `The gossip packet size is N bytes, but gabion needs between 158 and 65507 …`                                                  | `gossip.max_payload_bytes` is too small for one counter or larger than the largest UDP payload.        | Set it inside that range, and to the same value on every node.                                                                |
| `` `gossip.bootstrap_peers` (…) lists N peers, but the peer table holds M. … ``                                                | The static peer list is longer than the peer table.                                                    | Raise `gossip.peer_capacity` to at least N, or shorten the list.                                                              |
| `` `gossip.bootstrap_peers` (…) lists N peers, but `gossip.bind` (…) is not set. … ``                                          | A static list with no gossip bind.                                                                     | Set `gossip.bind`, or remove the list.                                                                                        |
| `` The zone setting `V` is not a valid zone name: … ``                                                                         | `gossip.zone` is empty, longer than 63 characters, or holds a character a Kubernetes label value cannot. | Use the cloud's zone identifier (for example `use1-az2`), or remove it to learn the zone from Kubernetes.                    |
| `The gossip error budget is N basis points, outside 1 to 10000 …`                                                               | `gossip.target_err_bps` is 0 or above 100 % of a rule's limit.                                         | Set it between 1 and 10000; the default, 100, is 1 % of each rule's limit.                                                    |
| `` `gossip.min_emit_interval` … must be at least 1ms and shorter than `gossip.tick_interval` … ``                               | The gap between two sends is at or above the heartbeat, so every change would wait for the heartbeat.  | Set it below `gossip.tick_interval`, or remove it to use the default of 5ms.                                                  |
| `gabiond cannot start: it has no gossip address to listen on …`                                                                | No bind address was supplied.                                                                          | Set `gossip.bind` in YAML or `GABION_GOSSIP_BIND` in env.                                                                     |
| `` `GABION_X` contains bytes that are not valid UTF-8 ``                                                                       | A non-UTF-8 byte appeared in `GABION_X`.                                                               | Re-export the env var with a valid UTF-8 value.                                                                               |
| `Config file not found: PATH`                                                                                                  | The path given as `gabiond`'s argument does not exist (reported inside `gabiond_exit`).                 | Pass the right path, or no argument at all: `gabiond` then reads only the environment and the defaults.                       |
| `OVER_LIMIT` for every descriptor                                                                                              | The configured `rate:` is below sustained load.                                                        | Raise the rate, widen the window, or split the rule. Re-run with `mode: dry_run` while you measure.                           |

### `admin_listening`

**Level** info, once at start · **Request allowed** not applicable · **Emitted by** `gabiond`

Message starts: "Admin HTTP endpoint is up."

**What happened.** `gabiond` bound `admin_bind` and serves `/snapshot` and `/readyz` on it.

**Why.** `admin_bind` is set.

**What to do next.** Nothing. Point the readiness probe at `/readyz` on this address ([Admin endpoint](#admin-endpoint)).

**Fields** `listen_addr` · **Knob** `admin_bind` (`GABION_ADMIN_BIND`)

### `admin_unavailable`

**Level** HTTP 503 body of `/snapshot`, not a log line · **Request allowed** not applicable · **Emitted by** `gabiond`

Message starts: "The gossip task has stopped, so this node has no live state to report."

**What happened.** `/snapshot` got no answer from the gossip task, so it returned 503 instead of a snapshot.

**Why.** The gossip task has stopped. The `gossip_stopped` line logged just before says why, and `gabiond` exits once its listeners drain.

**What to do next.** Find the `gossip_stopped` line, fix its cause, and let the pod restart.

**Fields** none (an HTTP body) · **Knob** none

### `cardinality_rejected`

**Level** warn, power of two · **Request allowed** no, rejected (`OVER_LIMIT`) · **Emitted by** `gabiond`

Message starts: "Rejecting requests that attach too many or too large rate-limit descriptors."

**What happened.** A request exceeded the cardinality envelope, so `gabiond` rejected that descriptor before touching any counter. The `limit` field says which part: `count` (too many entries in one descriptor), `bytes` (domain, keys and values too long in total), `key_bytes` (one key too long) or `request_descriptors` (too many descriptors in one request).

**Why.** Usually a misbehaving client or an attack trying to exhaust gabion's tracking memory; occasionally a legitimate client whose descriptors outgrew the defaults.

**What to do next.** If the traffic is legitimate, raise the setting in `config_key` and restart: `storage.max_descriptor_count`, `storage.max_descriptor_bytes`, `storage.max_key_bytes` or, for `request_descriptors`, `storage.max_request_descriptors` (each up to 65,535 where it is a count).

**Fields** `domain`, `limit`, `max`, `rejected_total`, `config_key` · **Knob** `storage.max_descriptor_count` (`GABION_STORAGE_MAX_DESCRIPTOR_COUNT`), `storage.max_descriptor_bytes` (`GABION_STORAGE_MAX_DESCRIPTOR_BYTES`), `storage.max_key_bytes` (`GABION_STORAGE_MAX_KEY_BYTES`), `storage.max_request_descriptors` (`GABION_STORAGE_MAX_REQUEST_DESCRIPTORS`, default 16)

### `clean_shutdown`

**Level** info, once at exit · **Request allowed** not applicable, `gabiond` has stopped · **Emitted by** `gabiond`

Message starts: "Gabion shut down cleanly."

**What happened.** After a signal, every listener drained its in-flight requests and `gabiond` exits with status 0.

**Why.** A `shutdown_signal` line came first: Kubernetes stopping the pod, or an operator.

**What to do next.** Nothing. A shutdown without this line exited with status 1; its error line says why.

**Fields** none · **Knob** none

### `gabiond_exit`

**Level** error, once · **Request allowed** not applicable, `gabiond` is not serving · **Emitted by** `gabiond`

Message starts: "gabiond is exiting with status 1 because it could not"

**What happened.** A startup stage failed, so `gabiond` exited with status 1 without serving. The `stage` field says which: starting its async runtime, installing its signal handlers, or loading its configuration.

**Why.** Almost always the configuration; see the symptom table above for the message in `error`.

**What to do next.** Fix what `error` names and restart.

**Fields** `stage`, `error` · **Knob** none

### `gabiond_started`

**Level** info, once at start · **Request allowed** not applicable · **Emitted by** `gabiond`

Message starts: "Starting gabion."

**What happened.** `gabiond` loaded and validated its configuration and is about to bind its sockets. The fields echo what it will run with.

**Why.** The process started.

**What to do next.** Check the fields against what you meant to configure: `rules_loaded`, the binds, `membership` (`kubernetes` or `static`) and the capacities.

**Fields** `gossip_bind`, `envoy_bind`, `admin_bind`, `rules_loaded`, `max_keys`, `max_rules`, `max_peers`, `max_peer_instances`, `zone_setting`, `membership` · **Knob** none

### `gossip_listening`

**Level** info, once at start · **Request allowed** not applicable · **Emitted by** `gabiond`

Message starts: "Listening for gossip from other gabion nodes."

**What happened.** `gabiond` bound `gossip.bind` and gossips on it.

**Why.** The process started.

**What to do next.** Nothing. Peers reach this node on `listen_addr`; on Kubernetes the Service's UDP port named `gabion` must point at it.

**Fields** `listen_addr` · **Knob** `gossip.bind` (`GABION_GOSSIP_BIND`)

### `gossip_record_failure`

**Level** warn, power of two · **Request allowed** yes, hits not recorded · **Emitted by** `gabiond`

Message starts: "This node can no longer share rate-limit counts with the rest of the cluster."

**What happened.** `gabiond` could not hand a count to the gossip task because the task is gone. The request was allowed and its hits were not recorded. The same task applies peers' counts, so this node's counts stop rising altogether: its limits under-count all traffic and, as windows pass, allow everything.

**Why.** The gossip background task has stopped; the `gossip_stopped` line says why, and `gabiond` exits once its listeners drain.

**What to do next.** Fix what `gossip_stopped` names; the pod restarts on its own.

**Fields** `failed_total` · **Knob** none

### `gossip_stopped`

**Level** error, once · **Request allowed** yes until `gabiond` exits, hits not counted (see `gossip_record_failure`) · **Emitted by** `gabiond`

Message starts: "The gossip background task stopped on its own."

**What happened.** The gossip task ended, so this node can no longer share counts. `gabiond` drains its listeners and exits with status 1 to be restarted rather than under-count silently.

**Why.** The gossip socket returned the error in `error`.

**What to do next.** Read `error` and check the pod's network. The restarted pod rebinds and relearns counts from its peers.

**Fields** `error` · **Knob** none

### `grpc_listening`

**Level** info, once at start · **Request allowed** not applicable · **Emitted by** `gabiond`

Message starts: "Accepting rate-limit decisions from Envoy."

**What happened.** `gabiond` bound `envoy_bind` and serves the rate-limit, health and reflection services on it.

**Why.** `envoy_bind` is set.

**What to do next.** Nothing. Point Envoy's rate-limit filter at `listen_addr`.

**Fields** `listen_addr`, `health_service`, `reflection_service`, `rate_limit_service` · **Knob** `envoy_bind` (`GABION_ENVOY_BIND`)

### `identity_generated`

**Level** info, once at start · **Request allowed** not applicable · **Emitted by** `gabiond`

Message starts: "Generated this node's identity."

**What happened.** `gabiond` derived its node id and drew a new incarnation; peers' `/snapshot` shows this node under that id.

**Why.** The node id is hashed from `runtime.node_id_seed`, else the host name, so it stays the same across restarts; the incarnation changes on every start.

**What to do next.** Nothing. If two nodes log the same `node_id`, give each a distinct `runtime.node_id_seed`.

**Fields** `node_id`, `incarnation` · **Knob** `runtime.node_id_seed` (`GABION_RUNTIME_NODE_ID_SEED`)

### `listener_failed`

**Level** error, once · **Request allowed** not applicable, `gabiond` exits · **Emitted by** `gabiond`

Message starts: "gabiond could not serve"

**What happened.** `envoy_bind`, `admin_bind` or `gossip.bind` could not be bound, so `gabiond` exited with status 1 before serving anything rather than run without it. Every address is bound before `gabiond` logs that it serves any of them.

**Why.** Usually another process holds the port (a second `gabiond` on the host, a sidecar); less often the address is not on this host, or the port is privileged.

**What to do next.** Change the setting named in `config_key` (`GABION_ENVOY_BIND`, `GABION_ADMIN_BIND` or `GABION_GOSSIP_BIND`) or stop the other process, then restart.

**Fields** `addr`, `error`, `config_key` · **Knob** `envoy_bind` (`GABION_ENVOY_BIND`), `admin_bind` (`GABION_ADMIN_BIND`), `gossip.bind` (`GABION_GOSSIP_BIND`)

### `matched_rule_overflow`

**Level** warn, power of two · **Request allowed** yes, only the first `cap` rules counted it · **Emitted by** `gabiond`

Message starts: "A request matched more than 16 rate-limit rules, the most this node counts per request." (the number is `storage.max_matched_rules`)

**What happened.** One request matched more rules than `gabiond` counts per request. It was allowed; the first `cap` matched rules counted it, and the rest under-count.

**Why.** Usually overlapping rule patterns, for example a wildcard rule alongside several specific ones.

**What to do next.** Merge or narrow the overlapping rules under `limits:`, split the rule space across `domain:` values, or raise `storage.max_matched_rules`. Each extra rule costs 32 bytes per runtime thread, reserved once on the thread's first request.

**Fields** `domain`, `cap`, `overflowed_total`, `config_key` · **Knob** `storage.max_matched_rules` (`GABION_STORAGE_MAX_MATCHED_RULES`, default 16)

### `not_serving`

**Level** info, once at shutdown · **Request allowed** yes, in-flight requests finish · **Emitted by** `gabiond`

Message starts: "Marked the rate-limit service as NOT_SERVING in the health protocol."

**What happened.** After a signal, or after a `gossip_stopped` or `server_stopped` error, `gabiond` set the gRPC health status to NOT_SERVING so load balancers route new requests elsewhere, then drains its listeners.

**Why.** A `shutdown_signal` line came first. If a `gossip_stopped` or `server_stopped` error came first instead, `gabiond` exits with status 1 once its listeners drain.

**What to do next.** Nothing after a signal. After an error, follow that error's entry.

**Fields** none · **Knob** none

### `request_queue_full`

**Level** warn, power of two · **Request allowed** yes, some hits not counted · **Emitted by** `gabiond`

Message starts: "The gossip request queue was full, so counted hits were dropped"

**What happened.** An allowed request's hits found the gossip task's request queue full, so they were dropped rather than making the request wait. The request was allowed and the cluster's counts are missing those hits, so limits briefly under-count.

**Why.** Usually a burst larger than the queue, or the gossip task starved of CPU.

**What to do next.** Raise `gossip.limit_queue_capacity` (`GABION_GOSSIP_LIMIT_QUEUE_CAPACITY`) or give the pod more CPU.

**Fields** `capacity`, `dropped_total`, `config_key` · **Knob** `gossip.limit_queue_capacity` (`GABION_GOSSIP_LIMIT_QUEUE_CAPACITY`)

### `server_stopped`

**Level** error, once · **Request allowed** not applicable, `gabiond` exits · **Emitted by** `gabiond`

Message starts: "gabiond stopped serving"

**What happened.** The gRPC or admin server stopped after its address was bound: on its own before a signal, or with an error while draining. `gabiond` drains the rest and exits with status 1 to be restarted.

**Why.** Not a port clash, since the address was already bound. A bound server that fails, including one whose gRPC reflection service could not be built, is a gabion bug.

**What to do next.** Report it with the whole line, `error` included; the pod restarts on its own.

**Fields** `addr`, `error` · **Knob** none

### `shutdown_signal`

**Level** info, once · **Request allowed** yes, in-flight requests finish · **Emitted by** `gabiond`

Message starts: "Received shutdown signal"

**What happened.** `gabiond` received SIGTERM or SIGINT (the `signal` field) and is stopping cleanly: `not_serving`, then `clean_shutdown`.

**Why.** Kubernetes is stopping the pod (a rollout, a scale-down, an eviction), or an operator pressed Ctrl-C.

**What to do next.** Nothing.

**Fields** `signal` · **Knob** none

## Fail-open invariant

`gabiond` returns `OVER_LIMIT` only on a measured limit overflow.
Every other condition results in `OK`: gossip queue saturation, a
rule-table inconsistency, and transient internal failures all allow
the request through. The deliberate exception is `OVER_LIMIT` from
the cardinality envelope (`storage.max_descriptor_count`,
`storage.max_descriptor_bytes`, `storage.max_key_bytes` and
`storage.max_request_descriptors`), which
exists to bound memory consumption against pathological client input.

See the [Fail-open invariant](../../README.md#fail-open-invariant)
section of the root README for the policy statement.
