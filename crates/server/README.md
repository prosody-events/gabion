# gabiond — gabion's gRPC adapter

`gabiond` is gabion's standalone rate-limit service: an Envoy-compatible
gRPC server that any sidecar or proxy speaking
`envoy.service.ratelimit.v3` can call. It shares the CRDT and gossip
machinery with the nginx module, so the two adapters can coexist in a
single cluster and count against the same totals. If you're not running
Envoy — or you want enforcement inside nginx itself — see
[`../nginx/README.md`](../nginx/README.md). For the gossip protocol
explainer and tuning knobs, see
[`../gabion/README.md#how-gossip-works`](../gabion/README.md#how-gossip-works)
and
[`../gabion/README.md#operator-knobs`](../gabion/README.md#operator-knobs).

## Your first YAML

A single-node `gabiond` needs four things: where Envoy reaches it, an
admin port, one rule, and a gossip socket. The gossip socket is
mandatory even with no peers so a future peer can join without a
restart.

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
wherever you bound `envoy_bind`). Envoy emits one RPC per request;
gabiond evaluates every descriptor in that RPC against the rules its
`(domain, key)` matches, records the allowed ones into the gossip
ring, and replies. `overall_code` is `OVER_LIMIT` if any descriptor
was rejected; per-descriptor codes ride in `statuses[]` so a partial
reject doesn't fail the whole batch.

## How configuration layers

Most operators ship one ConfigMap to every replica and poke a couple
of values — the gossip bind, the pod IP — through per-pod env vars.
That's the shape gabiond is built for.

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
│   `gabion::defaults` + per-struct `Default` impls.          │
└─────────────────────────────────────────────────────────────┘
```

Every overridable field has exactly one `GABION_*` env var; see
`ENV_BINDINGS` in `crates/server/src/config.rs` for the full table.
Comma-separated values feed list fields:
`GABION_DISCOVERY_NAMESPACE_ALLOW=ns-a,ns-b`. Structured lists (the
`limits:` block, anything with nested durations) live in YAML only —
flat env vars cannot express them.

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
per request; gabion keys the counter on the descriptor's value.

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

Each tenant gets an independent 1000-per-minute budget.

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

`dry_run` evaluates the rule and records hits into gossip but never
rejects. To see what would have happened, look at the per-descriptor
`code` field in the gRPC `statuses[]` array (Envoy's access logs can
surface this), and check `/snapshot` to confirm the rule is
accumulating counts. Once the numbers look right, drop the `mode:`
line to start enforcing.

### Stack per-IP and per-tenant

Each YAML rule is an independent gate. A request is evaluated against
every rule its descriptors match; if any rule rejects, that
descriptor's status is `OVER_LIMIT` and `overall_code` is
`OVER_LIMIT`.

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
time (it would resolve to `limit = 0`). To enforce "100 in 500ms",
write `rate: 100r/500ms`.

## Running across a cluster

The three plumbing steps — bind a gossip socket, pick a cluster id,
tell peers how to find each other — are cross-cutting and live in the
root README's
[Running across a cluster](../../README.md#running-across-a-cluster).
The `gabiond` YAML keys are `gossip.bind` (UDP `host:port`),
`gossip.cluster_id_hash` (non-zero u128 shared across peers),
`discovery.namespace_allow` / `discovery.service_allow` (Kubernetes
EndpointSlice filters), and `discovery.self_addr` (this pod's own
`host:port` from `POD_IP`, excluded from the peer set). Bind a
ServiceAccount with `endpointslices` `get`/`list`/`watch` on the
deployment; without it, discovery logs `403` and the peer set stays
empty.

Tuning the gossip cadence is rarely necessary — defaults converge in
well under a second at production scale. When you do tune, the two
adaptive aspects of the protocol have their own knobs: **adaptive
fanout** (per-tick peer count scaling with the dirty set) is
`gossip.fanout`; the **adaptive emit rate** (threshold-triggered
emissions between heartbeats) is `gossip.target_err_bps` (per-rule
error budget in basis points of the limit) and
`gossip.min_emit_interval` (the floor between threshold-fire
emissions). The math and tradeoffs live in
[`../gabion/README.md#how-gossip-works`](../gabion/README.md#how-gossip-works).

### Verify convergence

`/snapshot` returns peer list, rule list, and CRDT stats:

```bash
curl -s "$ADMIN_HOST:9090/snapshot" | jq '.peers | length'
curl -s "$ADMIN_HOST:9090/snapshot" | jq '.store.cell_store.active_cells'
```

A cluster has converged when every node's `peers | length` reflects
the expected topology and `active_cells` is the same order of
magnitude on each.

## Troubleshooting

Every operator-facing warning answers three questions in one breath:
what happened, why it's likely happening, and the next thing to try.
Warnings that can fire at request rate are throttled, so a misbehaving
client at high rate produces a handful of log lines, not a flood.

| Symptom (verbatim from the error string)                                                                                       | What it means                                                                                          | Fix                                                                                                                          |
|--------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `` config error: … missing field `name` ``                                                                                     | A `limits:` entry is missing a required field.                                                         | Supply `name`, `domain`, `descriptors`, and `rate`. `window` and `bucket` are optional.                                       |
| `` rule `X` is declared more than once; rule names must be unique ``                                                           | Two `limits:` entries share a `name:`.                                                                 | Pick distinct names.                                                                                                          |
| `` rule `X` has an invalid `rate:` value: …. Use e.g. `rate: 100r/s` or `rate: 10r/5m`. ``                                     | The `rate:` string didn't parse, or its count is zero.                                                 | Write a non-zero count and a unit (`s`, `ms`, `m`, `h`). A zero rate is refused because it would deny all traffic.            |
| `` rule `X`: `window=` must be at least as long as the rate's period; … e.g. `rate=100r/500ms` ``                              | An explicit `window:` was shorter than the rate's period; the resolved limit would be zero.            | Move the period into the rate itself (`rate: 100r/500ms`) instead of pairing a short `window:` with a longer period.          |
| `` rule `X` descriptor key `Y` must match `[A-Za-z_][A-Za-z0-9_.-]*` ``                                                        | A descriptor key uses an unsupported character.                                                        | Use identifier-like names. Underscore, dot, and dash are allowed inside; the first character must be a letter or underscore.  |
| `gossip.bind is required`                                                                                                      | No bind address was supplied.                                                                          | Set `gossip.bind` in YAML or `GABION_GOSSIP_BIND` in env.                                                                     |
| `environment variable GABION_X is not valid UTF-8`                                                                             | A non-UTF-8 byte appeared in `GABION_X`.                                                               | Re-export the env var with a valid UTF-8 value.                                                                               |
| `OVER_LIMIT` for every descriptor                                                                                              | The configured `rate:` is below sustained load.                                                        | Raise the rate, widen the window, or split the rule. Re-run with `mode: dry_run` while you measure.                           |
| *"Rejecting requests that attach too many rate-limit descriptors."*                                                            | A client sent more descriptors, larger descriptors, or larger keys than the configured envelope.       | Raise `storage.max_descriptor_count`, `storage.max_descriptor_bytes`, or `storage.max_key_bytes` in YAML; restart.            |
| *"This node can no longer share rate-limit counts with the rest of the cluster."*                                              | The gossip background task has stopped. Counters keep working off local traffic only.                  | Look for an earlier error log entry — gossip exits after surfacing a typed error. Fix that and restart.                       |
| *"A single request matched more rules than gabion's per-request cap allows"*                                                   | A request matched more than `STORAGE_MAX_MATCHED_RULES` (= 16) rules. Gabion **allowed** the request.  | Reduce overlapping rule patterns or split the rule space across multiple `domain:` values. The cap is a compile-time const.   |

## Fail-open invariant

`gabiond` returns `OVER_LIMIT` only on a measured limit overflow.
Every other condition — gossip queue saturation, a rule-table
inconsistency, a transient internal failure — results in `OK`. The
deliberate exception is `OVER_LIMIT` from the cardinality envelope
(`storage.max_descriptor_count`, `storage.max_descriptor_bytes`,
`storage.max_key_bytes`), which exists to bound memory consumption
against pathological client input.

See the [Fail-open invariant](../../README.md#fail-open-invariant)
section of the root README for the policy statement.

## Migration from the pre-1.0 YAML

No deprecation cycle: the old `limit:` + `window:` pair became a
mandatory `rate:` string plus optional `window:` / `bucket:`
durations.

| Before                                       | After                                                                                                            |
|----------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `limit: 100, window: 60s`                    | `rate: 100r/m`                                                                                                   |
| `limit: 10, window: 1s`                      | `rate: 10r/s`                                                                                                    |
| `limit: 180000, window: 5h`                  | `rate: 10r/s, window: 5h` (same resolved limit, original intent preserved)                                       |
| `limit: 100, window: 500ms` (sub-period)     | `rate: 100r/500ms`. A `window:` shorter than the rate's period is refused (would resolve to `limit = 0`).        |

Read [Rate, window, and bucket](#rate-window-and-bucket) before
reaching for `window:` — long windows with the default `bucket:`
give you a burstable budget, not a paced one.
