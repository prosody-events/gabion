# gabiond — gabion's gRPC adapter

## What gabiond is

`gabiond` is gabion's standalone rate-limit service: an Envoy-compatible
gRPC server that any sidecar or proxy speaking
`envoy.service.ratelimit.v3` can call. Internally it uses the same
CRDT and gossip plumbing as the nginx module, so the two can coexist in
a single cluster and share counters. If you're not running Envoy or you
want enforcement inside nginx itself, see the [main nginx README](../../README.md)
instead.

For the broader architecture see `CLAUDE.md`; for CRDT internals see
`docs/CRDT Module.md`.

## Your first YAML

A minimal `config.yaml` that runs `gabiond` on a single node:

```yaml
# Where Envoy (or any v3 ratelimit client) reaches gabiond.
envoy_bind: 0.0.0.0:8081

# Optional: HTTP endpoint for /snapshot and other admin reads.
admin_bind: 0.0.0.0:9090

# One rule: 100 requests/second per IP, keyed by an Envoy descriptor
# entry whose key is `remote_address`.
limits:
  - name: per_ip
    domain: envoy
    descriptors:
      - key: remote_address       # the Envoy descriptor key to match on
    rate: 100r/s                  # the rate's period IS the default window
    # `window:` (optional) widens the time horizon; the resolved limit
    # scales up to `floor(rate_count * window / period)`.
    # `bucket:` (optional) defaults to the resolved window — one fixed-
    # window bucket. Set explicitly for sliding-window-style enforcement.

# Gossip channel — required even for a single node so future peers
# can join without a restart.
gossip:
  bind: 0.0.0.0:9000
  cluster_id_hash: 0xc0ffee       # any non-zero u128 shared across peers
```

Run with:

```bash
gabiond /etc/gabion/config.yaml
```

Configure Envoy to point its rate-limit filter at
`gabiond.<namespace>.svc:8081` (or wherever you bound `envoy_bind`).
Each request Envoy emits one `RateLimit` RPC; gabiond evaluates the
matching rules against the current cluster-wide aggregate, records
allowed hits, and returns `OK` or `OVER_LIMIT`.

## How configuration layers

Configuration is built up in three stages, each overriding the
previous:

1. **Built-in defaults** from `gabion::defaults` and the per-struct
   `Default` impls.
2. **YAML file** passed on the command line.
3. **Environment variables** — every overridable field has one
   `GABION_*` env var bound explicitly. Useful for container deploys
   where most of the config is shared via ConfigMap but a few values
   (binds, seeds) come from per-pod env.

See `crates/server/src/config.rs::ENV_BINDINGS` for the full list of
env-var names. Comma-separated values feed list fields:
`GABION_DISCOVERY_NAMESPACE_ALLOW=ns-a,ns-b`.

Structured lists (notably `limits:`, where each entry has nested
fields and durations) come from the YAML file — they cannot be expressed
through flat env vars.

## Glossary

The gabion vocabulary is shared between adapters; see the [main
README's glossary](../../README.md#glossary) for the full set.
Server-specific terms:

| Term            | Definition                                                                                                                                  |
|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| **Envoy domain**| The `domain:` field in a YAML rule, matched against the `domain` field of the inbound `RateLimitRequest`. Envoy filters set this per-route. |
| **Descriptor**  | An Envoy term: a list of `(key, value)` pairs sent with each request. Gabion's rule descriptors match against these.                        |
| **Read-then-record** | Each descriptor is evaluated against the current aggregate; only allowed descriptors are recorded into gossip. Multi-descriptor requests are not all-or-nothing — over-limit descriptors return `OVER_LIMIT` for that descriptor alone. |
| **`/snapshot`** | Admin HTTP endpoint that returns the full peer + cell view. The fastest way to verify a cluster has converged.                              |

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

Envoy's `remote_address` action emits a descriptor of the form
`("remote_address", "1.2.3.4")` per request. Gabion keys the counter
on the descriptor's value.

### Limit per tenant header

```yaml
limits:
  - name: per_tenant
    domain: envoy
    descriptors:
      - key: tenant
    rate: 1000r/m
```

Configure Envoy's filter to extract a header into a descriptor:

```yaml
# Envoy route_config snippet
rate_limits:
  - actions:
      - request_headers:
          header_name: x-tenant-id
          descriptor_key: tenant
```

Each tenant sees an independent 1000/m budget.

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

Watch `gabiond`'s metrics (`gabion_admission_allowed`,
`gabion_admission_rejected`) for the new rule. Once the ratio looks
right, drop `mode: dry_run` to start enforcing.

### Stack per-IP + per-tenant

Each YAML rule is an independent gate; the gRPC service evaluates every
descriptor in the request and returns `OVER_LIMIT` if any rule rejects.

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

### Rate, window, and bucket

A rule resolves to three internal numbers — a `limit`, a `window`, and a
`bucket`. The YAML shape mirrors the nginx adapter:

* `rate:` (mandatory) — `N` requests per period, e.g. `10r/s`, `100r/5m`,
  `1000r/h`. Same syntax as the nginx `rate=` argument.
* `window:` (optional) — the time horizon the rate is enforced over.
  Defaults to the rate's period. When set, the resolved limit scales up
  to `floor(rate_count * window / period)`.
* `bucket:` (optional) — granularity inside the window. Defaults to the
  resolved window (one fixed-window bucket). Set smaller for
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

resolves to `limit = 10 * 5 * 3600 = 180000`, `window = 5h`, `bucket = 1h`
(five live buckets). The same triple can be written as
`rate: 180000r/5h\nbucket: 1h` — the operator-facing knob is identical.

> **Rule of thumb.** If you set `window:` larger than the rate's period
> and don't also set `bucket:`, you get a *burstable* budget — clients
> can fire the whole window's allowance instantly, then sit empty for
> the rest of the window. For sustained-rate enforcement, set `bucket:`
> close to the rate's period. `rate: 10r/s, window: 5h, bucket: 1s`
> keeps the 180k 5-hour budget but smooths it to roughly 10 r/s.

Other things worth knowing:

* **`OVER_LIMIT` reports the resolved budget.** A rule written as
  `rate: 10r/s window: 1h` returns the 36000-request budget, not 10.
* **Floor silently under-budgets non-multiples.** `rate: 10r/m window:
  85s` resolves to `limit = 14` (the leftover 0.16 period vanishes).
* **`window:` shorter than the rate's period is rejected.** To enforce
  "100 in 500ms" write `rate: 100r/500ms`, not `rate: 200r/s window:
  500ms` — the latter would resolve to `limit = 0` and is refused at
  config-load time.

The corresponding Envoy filter emits two descriptors per request, one
for each action.

### Scale beyond one node

Add discovery so peers can find each other under Kubernetes:

```yaml
gossip:
  bind: 0.0.0.0:9000
  cluster_id_hash: 0xc0ffee

discovery:
  namespace_allow: [my-app]
  service_allow: [gabiond]
  self_addr: ${POD_IP}:9000    # exclude this pod from discovered peers
```

`namespace_allow` and `service_allow` filter Kubernetes EndpointSlice
watches. `self_addr` is read from the pod's own IP (set via the
downward API) so each replica doesn't try to gossip to itself. Bind a
ServiceAccount with `endpointslices` `get`/`list`/`watch` to the
deployment; without it, discovery logs `403` and falls back to the
empty peer set.

## Running across a cluster

The three pieces of plumbing are identical to the nginx side:

1. **Gossip bind** (`gossip.bind`) — UDP socket every peer reaches.
2. **Cluster identifier** (`gossip.cluster_id_hash`) — non-zero u128
   shared by every peer; mismatches drop frames on the floor.
3. **Discovery** (`discovery.namespace_allow` / `discovery.service_allow`)
   — Kubernetes EndpointSlice filter that picks up peer pods as they
   come and go.

Verify convergence with `/snapshot`:

```bash
curl -s "$ADMIN_HOST:9090/snapshot" | jq '.peers | length'   # peer count
curl -s "$ADMIN_HOST:9090/snapshot" | jq '.cells | length'   # local cells
```

Tuning the gossip cadence is rarely necessary — defaults converge in
well under a second at production scale. See the main
[Running across a cluster](../../README.md#running-across-a-cluster)
section for the knobs and their tradeoffs.

## Troubleshooting

| Symptom                                                                          | What it means                                                                                                  | Fix                                                                                                                          |
|----------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `config error: ... missing field 'name' at limits[0]`                            | A YAML `limits:` entry is missing the required field.                                                          | Supply `name`, `domain`, `descriptors`, and `rate`. `window` / `bucket` are optional.                                        |
| `rule X is declared more than once`                                              | Two entries in `limits:` share a `name:`.                                                                      | Pick distinct names.                                                                                                         |
| `rule X has an invalid 'rate:' value: ...`                                       | The `rate:` string didn't parse, or its count is zero.                                                         | Use e.g. `rate: 100r/s`, `rate: 10r/5m`. Zero counts are refused — a zero rate would deny all traffic.                       |
| `rule X: 'window=' must be at least as long as the rate's period`                | An explicit `window:` was shorter than the rate's period; the resolved limit would be zero.                    | Move the period into the rate (`rate: 100r/500ms`) instead of pairing a short `window:` with a longer period.                |
| `rule X descriptor key 'with space' must match '[A-Za-z_][A-Za-z0-9_.-]*'`       | A descriptor key uses unsupported characters.                                                                  | Stick to identifier-like names (underscore + dot + dash OK).                                                                 |
| `gossip.bind is required`                                                        | No bind address was supplied.                                                                                  | Set `gossip.bind` in YAML or `GABION_GOSSIP_BIND` in env.                                                                    |
| `environment variable GABION_X is not valid UTF-8`                               | A non-UTF-8 byte in an env var.                                                                                | Re-export the env var with a valid value.                                                                                    |
| `OVER_LIMIT` responses for all descriptors                                       | The configured `rate:` is below sustained load.                                                                | Raise the rate, or extend the window, or split the rule. Run with `mode: dry_run` while you measure.                         |
| `gabiond` warns about gossip record failures                                     | The gossip queue is full; gabiond is **allowing** the request and **under-counting**.                          | Either tune `gossip.limit_queue_capacity` upward or reduce upstream traffic. Errors are rate-limited via a power-of-two pattern. |
| `gabiond` warns about matched-rule overflow                                      | A request matched more than `STORAGE_MAX_MATCHED_RULES` rules; the request was **allowed** (allow-by-default). | Reduce the number of rules matching a single descriptor, or split your rule space across multiple domains.                   |

Operator-facing log lines follow the three-question shape from
`CLAUDE.md`: *what happened*, *why it's likely happening*, *what to do
next*.

## Fail-open invariant

`gabiond` returns `OVER_LIMIT` only on a measured limit overflow.
Every other condition — gossip record failure, internal queue
saturation, rule-table miss — results in `OK`. The deliberate
exception is `OVER_LIMIT` from the cardinality envelope
(`max_descriptor_bytes`), which exists to bound memory consumption
against pathological client input.

This mirrors the nginx adapter's behaviour; see the
[Fail-open invariant](../../README.md#fail-open-invariant) section of
the main README for the full statement.
