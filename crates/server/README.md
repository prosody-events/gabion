# gabiond — gabion's gRPC adapter

`gabiond` is gabion's standalone rate-limit service: an Envoy-compatible
gRPC server that any sidecar or proxy speaking
`envoy.service.ratelimit.v3` can call. It shares the CRDT and gossip
machinery with the nginx module, so the two adapters can coexist in a
single cluster and count against the same totals. If you are not running
Envoy — or you want enforcement inside nginx itself — see
[`../nginx/README.md`](../nginx/README.md). For the gossip protocol
explainer and tuning knobs, see
[`../gabion/README.md#how-gossip-works`](../gabion/README.md#how-gossip-works)
and
[`../gabion/README.md#operator-knobs`](../gabion/README.md#operator-knobs).

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
`(domain, key)` matches, records the allowed ones into the gossip ring,
and replies. The reply's `overall_code` is `OVER_LIMIT` if any
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
one name. Structured lists such as the `limits:` block, along with
anything that contains nested durations, live in YAML only because flat
env vars cannot express them.

gabiond parses and validates every value once, at startup, before it
binds a socket: a zero count, a malformed value, an out-of-range value
or a YAML key the table and the `limits:` fields do not know (a typo, or
a key a release removed) stops startup with a message that names the
setting in both spellings. See [Troubleshooting](#troubleshooting).

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
* `bucket:` (optional) — granularity inside the window. Defaults to
  the resolved window (one fixed-window bucket). Set smaller for
  sliding-window-style enforcement.

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
The `gabiond` YAML keys are `gossip.bind` (UDP `host:port`),
`gossip.cluster_id_hash` (non-zero u128 shared across peers),
`discovery.namespace_allow` / `discovery.service_allow` (Kubernetes
EndpointSlice filters), and `discovery.self_addr` (this pod's own
`host:port` from `POD_IP`, excluded from the peer set). Discovery needs
the Role in the root README's
[Kubernetes checklist](../../README.md#kubernetes-checklist): `get` on
Pods and `get`/`list`/`watch` on Services and EndpointSlices, bound to the
pod's ServiceAccount
([`deploy/kubernetes/gabion-discovery-rbac.yaml`](../../deploy/kubernetes/gabion-discovery-rbac.yaml)).
Without it, discovery logs `kubernetes_forbidden` and the peer set stays
empty.

Tuning the gossip cadence is rarely necessary, since the defaults
converge in well under a second at production scale. When you do tune,
the two adaptive aspects of the protocol have their own knobs:
**coverage fanout** (per-tick peer count sized to `⌈ln(n)+c⌉`, scaled by
cluster size) has its floor at `gossip.fanout`; the **adaptive emit rate** (threshold-triggered
emissions between heartbeats) is governed by `gossip.target_err_bps`
(per-rule error budget in basis points of the limit) and
`gossip.min_emit_interval` (the floor between threshold-fire
emissions). The math and tradeoffs live in
[`../gabion/README.md#how-gossip-works`](../gabion/README.md#how-gossip-works).

### Verify convergence

`/snapshot` returns the peer list, CRDT stats (`store_stats`), gossip counters
(`counters`), the rule list and the aggregate row count:

```bash
curl -s "$ADMIN_HOST:9090/snapshot" | jq '.peers | length'
curl -s "$ADMIN_HOST:9090/snapshot" | jq '.store_stats.active_cells'
```

A cluster has converged when every node's `peers | length` reflects
the expected topology and `active_cells` is the same order of
magnitude on each.

## Troubleshooting

Every operator-facing warning answers three questions: what happened,
why it is likely happening, and the next thing to try. Warnings that
can fire at request rate are throttled, so a misbehaving client at
high rate produces a handful of log lines rather than a continuous
flood. Messages from the gabion library (gossip, discovery, the counter
store) are in [`../gabion/TROUBLESHOOTING.md`](../gabion/TROUBLESHOOTING.md);
the entries below are `gabiond`'s own.

| Symptom (verbatim from the error string)                                                                                       | What it means                                                                                          | Fix                                                                                                                          |
|--------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `` gabiond could not read its configuration: … missing field `name` ``                                                         | A `limits:` entry is missing a required field.                                                         | Supply `name`, `domain`, `descriptors`, and `rate`. `window` and `bucket` are optional.                                       |
| `` Two rules are named `X`. ``                                                                                                  | Two `limits:` entries share a `name:`.                                                                 | Pick distinct names.                                                                                                          |
| `` Rules `X` and `Y` count the same keys with the same domain, descriptors, window and bucket … ``                             | Two rules share an identity (they may differ only in `rate:` count or `mode:`), so their counters could not be told apart. | Give one a different descriptor, `window:` or `bucket:`, or merge them.                                                       |
| `` Rule `X` has an invalid rate: …. ``                                                                                         | The `rate:` string didn't parse, its count is zero, or its period is under 1 ms.                      | Write a count of at least 1 and a period of at least 1 ms: a unit (`s`, `m`, `h`, `d`) or a duration such as `500ms` or `5m`. A zero rate is refused because it would deny all traffic. |
| `` Rule `X`: the window is shorter than the rate's period, so the rate cannot be spread over it. … for example `100r/500ms` ``   | An explicit `window:` was shorter than the rate's period; a window shorter than the rate's period has nothing to scale the rate over. | Move the period into the rate itself (`rate: 100r/500ms`) instead of pairing a short `window:` with a longer period.          |
| `` Rule `X`: the window is longer than gabion tracks … ``                                                                      | The window is 49 days or more, or spans 2,147,483,648 buckets or more.                                 | Use a shorter `window:` or a larger `bucket:`.                                                                                |
| `` Rule `X` has descriptor key `Y`, but keys must match `[A-Za-z_][A-Za-z0-9_.-]*` … ``                                         | A descriptor key uses an unsupported character.                                                        | Use identifier-like names. Underscore, dot, and dash are allowed inside; the first character must be a letter or underscore.  |
| `` Rule `X` has a domain … ``                                                                                                  | `domain:` is empty, longer than 255 bytes, or holds whitespace or a control character.                 | Use 1 to 255 bytes with no whitespace, the same domain an NGINX twin of the rule names in `domain=`.                          |
| `` Rule `X` has no descriptors … ``, `` Rule `X` has N descriptors, above … ``, `` Rule `X` has descriptor key `Y` of N bytes, above … `` | The rule has no `descriptors:`, more than `storage.max_descriptor_count`, or a key longer than `storage.max_key_bytes`. | Add a descriptor, or shorten the rule or raise the named setting.                                                             |
| `` N rate-limit rules are configured, but `storage.rule_dictionary_capacity` … is M. … ``                                      | `storage.rule_dictionary_capacity` is set below the number of rules.                                   | Remove it to use the default (twice the configured rules, at least 256), or raise it.                                         |
| `` The configuration file sets `X`, which gabiond does not know. … ``                                                            | The YAML sets a key that is not a setting or a field of a `limits:` entry: a typo, or a key this release removed or renamed. | Correct or delete the key; the release notes name each removed key's replacement.                                             |
| `` The configuration file sets `X` (…) to …, which gabiond cannot read as its value. … ``                                       | A table, a list where one value belongs, or an unquoted integer above 9223372036854775807, which YAML reads as a fraction and rounds. | Fix the indentation or quote the number, or remove the key.                                                                   |
| `` `X` is 0, but it must be at least 1: … ``                                                                                    | A count, capacity or duration was set to 0, which would disable what it sizes.                         | Remove the setting to use the default the message names, or set a positive value.                                             |
| `` `X` is `V`, which is not …, for example `E`. … ``                                                                            | The value does not parse as the setting's type (an integer in range, a duration in whole milliseconds such as `500ms`, a socket address, a Kubernetes name). | Write it like the example, or remove it to use the default.                                                                   |
| `` `X` is N; gabion supports MIN to MAX. … ``                                                                                   | A capacity is outside the range gabion can hold; the message gives the reason.                         | Set it inside the range, or remove it to use the default.                                                                     |
| `The gossip packet size is N bytes, but gabion needs between 152 and 65507 …`                                                  | `gossip.max_payload_bytes` is too small for one counter or larger than the largest UDP payload.        | Set it inside that range, and to the same value on every node.                                                                |
| `The gossip error budget is N basis points, outside 1 to 10000 …`                                                               | `gossip.target_err_bps` is 0 or above 100 % of a rule's limit.                                         | Set it between 1 and 10000; the default, 100, is 1 % of each rule's limit.                                                    |
| `` `gossip.min_emit_interval` … must be at least 1ms and shorter than `gossip.tick_interval` … ``                               | The gap between two sends is at or above the heartbeat, so every change would wait for the heartbeat.  | Set it below `gossip.tick_interval`, or remove it to use the default of 5ms.                                                  |
| `gabiond cannot start: it has no gossip address to listen on …`                                                                | No bind address was supplied.                                                                          | Set `gossip.bind` in YAML or `GABION_GOSSIP_BIND` in env.                                                                     |
| `` `GABION_X` contains bytes that are not valid UTF-8 ``                                                                       | A non-UTF-8 byte appeared in `GABION_X`.                                                               | Re-export the env var with a valid UTF-8 value.                                                                               |
| `OVER_LIMIT` for every descriptor                                                                                              | The configured `rate:` is below sustained load.                                                        | Raise the rate, widen the window, or split the rule. Re-run with `mode: dry_run` while you measure.                           |

### `cardinality_rejected`

**Level** warn, power of two · **Request allowed** no, rejected (`OVER_LIMIT`) · **Emitted by** `gabiond`

Message starts: "Rejecting requests that attach too many or too large rate-limit descriptors."

**What happened.** A request exceeded the cardinality envelope, so `gabiond` rejected that descriptor before touching any counter. The `limit` field says which part: `count` (too many entries in one descriptor), `bytes` (domain, keys and values too long in total), `key_bytes` (one key too long) or `request_descriptors` (too many descriptors in one request).

**Why.** Usually a misbehaving client or an attack trying to exhaust gabion's tracking memory; occasionally a legitimate client whose descriptors outgrew the defaults.

**What to do next.** If the traffic is legitimate, raise the setting in `config_key` and restart: `storage.max_descriptor_count`, `storage.max_descriptor_bytes`, `storage.max_key_bytes` or, for `request_descriptors`, `storage.max_request_descriptors` (each up to 65,535 where it is a count).

**Fields** `domain`, `limit`, `max`, `rejected_total`, `config_key` · **Knob** `storage.max_descriptor_count` (`GABION_STORAGE_MAX_DESCRIPTOR_COUNT`), `storage.max_descriptor_bytes` (`GABION_STORAGE_MAX_DESCRIPTOR_BYTES`), `storage.max_key_bytes` (`GABION_STORAGE_MAX_KEY_BYTES`), `storage.max_request_descriptors` (`GABION_STORAGE_MAX_REQUEST_DESCRIPTORS`, default 16)

### `gossip_record_failure`

**Level** warn, power of two · **Request allowed** yes, hits not recorded · **Emitted by** `gabiond`

Message starts: "This node can no longer share rate-limit counts with the rest of the cluster."

**What happened.** `gabiond` could not hand a count to the gossip task because the task is gone. The request was allowed and its hits were not recorded. The same task applies peers' counts, so this node's counts stop rising altogether: its limits under-count all traffic and, as windows pass, allow everything.

**Why.** The gossip background task has stopped, which happens after it surfaces a typed error.

**What to do next.** Look for an earlier error log entry that says why gossip stopped, fix that, and restart the pod.

**Fields** `error`, `failed_total` · **Knob** none

### `matched_rule_overflow`

**Level** warn, power of two · **Request allowed** yes, only the first `cap` rules counted it · **Emitted by** `gabiond`

Message starts: "A request matched more than 16 rate-limit rules, the most this node counts per request." (the number is `storage.max_matched_rules`)

**What happened.** One request matched more rules than `gabiond` counts per request. It was allowed; the first `cap` matched rules counted it, and the rest under-count.

**Why.** Usually overlapping rule patterns, for example a wildcard rule alongside several specific ones.

**What to do next.** Merge or narrow the overlapping rules under `limits:`, split the rule space across `domain:` values, or raise `storage.max_matched_rules`. Each extra rule costs 32 bytes per runtime thread, reserved once on the thread's first request.

**Fields** `domain`, `cap`, `overflowed_total`, `config_key` · **Knob** `storage.max_matched_rules` (`GABION_STORAGE_MAX_MATCHED_RULES`, default 16)

### `request_queue_full`

**Level** warn, power of two · **Request allowed** yes, some hits not counted · **Emitted by** `gabiond`

Message starts: "The gossip request queue was full, so counted hits were dropped"

**What happened.** An allowed request's hits found the gossip task's request queue full, so they were dropped rather than making the request wait. The request was allowed and the cluster's counts are missing those hits, so limits briefly under-count.

**Why.** Usually a burst larger than the queue, or the gossip task starved of CPU.

**What to do next.** Raise `gossip.limit_queue_capacity` (`GABION_GOSSIP_LIMIT_QUEUE_CAPACITY`) or give the pod more CPU.

**Fields** `capacity`, `dropped_total`, `config_key` · **Knob** `gossip.limit_queue_capacity` (`GABION_GOSSIP_LIMIT_QUEUE_CAPACITY`)

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

## Migration from the pre-1.0 YAML

There is no deprecation cycle: the old `limit:` + `window:` pair has
been replaced by a mandatory `rate:` string together with optional
`window:` and `bucket:` durations.

| Before                                       | After                                                                                                            |
|----------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `limit: 100, window: 60s`                    | `rate: 100r/m`                                                                                                   |
| `limit: 10, window: 1s`                      | `rate: 10r/s`                                                                                                    |
| `limit: 180000, window: 5h`                  | `rate: 10r/s, window: 5h` (same resolved limit, original intent preserved)                                       |
| `limit: 100, window: 500ms` (sub-period)     | `rate: 100r/500ms`. A `window:` shorter than the rate's period is refused (it has nothing to scale the rate over). |

Read [Rate, window, and bucket](#rate-window-and-bucket) before
reaching for `window:`, because long windows with the default
`bucket:` give you a burstable budget rather than a paced one.
