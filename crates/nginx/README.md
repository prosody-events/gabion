# gabion-nginx — gabion's NGINX module

`gabion-nginx` is gabion's in-process NGINX module: a dynamic `.so`
that runs in every worker, enforces rate limits at the access phase,
and (on the elected leader worker) drives the gossip runtime that
keeps counters in sync across replicas. If you're running Envoy
sidecars instead of nginx, see [`crates/server/README.md`](../server/README.md)
for the gRPC adapter.

For the project-level overview, glossary, and cross-adapter concepts
(gossip, discovery, fail-open invariant), see the [top-level
README](../../README.md).

## Contents

- [Your first rule](#your-first-rule)
- [Common patterns](#common-patterns)
- [Directive reference](#directive-reference)
- [Composition: layering rules](#composition-layering-rules)
- [Predicates: `except_if=$variable`](#predicates-except_ifvariable)
- [Dry-run mode](#dry-run-mode)
- [Discovery directives](#discovery-directives)
- [Gossip directives](#gossip-directives)
- [Running across a cluster](#running-across-a-cluster)
- [Configuration error messages](#configuration-error-messages)
- [Troubleshooting](#troubleshooting)
- [Unknown variable detection](#unknown-variable-detection)
- [Migration from the previous DSL](#migration-from-the-previous-dsl)

## Your first rule

A minimal single-node nginx config that exercises gabion end-to-end:

```nginx
# Load the compiled module shared object. Required once, before `events`.
load_module /etc/nginx/modules/ngx_http_gabion_module.so;

worker_processes 2;
events {}

http {
    # Allocate the shared-memory zone all rules live in. One per `http {}`.
    gabion_limit_zone zone=api:64m;

    # Declare a rule: at most 100 requests per second per client IP.
    gabion_limit_rule per_ip $remote_addr rate=100r/s;

    server {
        listen 8080;
        location / {
            # Apply the rule at this location.
            gabion_limit per_ip;
            return 200 "ok\n";
        }
    }
}
```

What you'll see:

- `curl -i http://127.0.0.1:8080/` under budget → `HTTP/1.1 200 OK`.
- Hammer it past 100 requests in one second from the same IP →
  `HTTP/1.1 429 Too Many Requests` with `X-RateLimit-Limit`,
  `X-RateLimit-Remaining: 0`, `X-RateLimit-Reset`, and a `Retry-After`
  header pinned to the rule's window.

Before flipping a freshly-added rule to enforcing mode, run it in
**dry-run** first. Append `dry_run` to the directive; the rule
evaluates and records hits (so your metrics show the load) but never
rejects:

```nginx
gabion_limit_rule per_ip $remote_addr rate=100r/s dry_run;
```

Watch the rule's allow/reject counts in your logs or metrics for a
release window, then drop `dry_run` to start enforcing.

To scale beyond one node, add `gabion_gossip_bind` plus a cluster
identifier and (under Kubernetes) the namespace allowlist — see
[Running across a cluster](#running-across-a-cluster) below.

## Common patterns

### Limit per IP, exempt internal network

```nginx
geo $trusted_ip {
    default 0;
    10.0.0.0/8 1;     # internal range bypasses this rule
    127.0.0.1/32 1;
}
gabion_limit_rule per_ip $remote_addr rate=100r/s except_if=$trusted_ip;

server {
    location /api/ { gabion_limit per_ip; }
}
```

The `geo` block defines a variable that's `1` for internal addresses and
`0` elsewhere. `except_if=$trusted_ip` skips the rule whenever the
variable resolves truthy. Internal traffic is uncounted; external
traffic is gated. See [Predicates](#predicates-except_ifvariable) for
the precise truthy/falsy rules.

### Limit per tenant header

```nginx
gabion_limit_rule per_tenant tenant:$arg_tenant rate=1000r/m;

server {
    location /api/ { gabion_limit per_tenant; }
}
```

`$arg_tenant` reads the `?tenant=` query arg. `tenant:` is the
descriptor key — each distinct tenant gets its own counter. A request
without `?tenant=` produces an empty value; with allow-by-default the
rule simply doesn't apply (no counter is bumped).

### Roll out a new limit safely

```nginx
# Step 1: ship the rule in dry_run; observe metrics for a release window.
gabion_limit_rule new_route tenant:$arg_tenant path:$uri rate=50r/m dry_run;
gabion_limit new_route;

# Step 2: once the allow/reject ratio looks right, drop `dry_run`.
gabion_limit_rule new_route tenant:$arg_tenant path:$uri rate=50r/m;
```

Dry-run rules evaluate and record hits but never reject. The aggregate
in SHM and gossip both see real traffic, so capacity planning is
truthful before you flip the switch.

### Stack per-IP + per-tenant

```nginx
gabion_limit_rule per_ip     $remote_addr        rate=100r/s;
gabion_limit_rule per_tenant tenant:$arg_tenant  rate=1000r/m;

location /api/ {
    gabion_limit per_ip per_tenant;     # both must allow the request
}
```

Each rule is an independent gate. A reject from either rejects the
request; the `X-RateLimit-*` headers pin to the rule with the longest
window so the client doesn't immediately get re-rejected by the wider
rule. See [Composition: layering rules](#composition-layering-rules)
for the inheritance shape.

### Exclude `/healthz` from rate limiting

```nginx
location /healthz {
    gabion off;       # skip the access handler entirely; zero per-request cost
    return 200;
}
```

`gabion off` skips the access handler altogether (no rule lookup, no
SHM read). Compare with `gabion_limit off`, which keeps the handler
running but evaluates no rules — useful when you want a scoped opt-out
without disabling future gabion machinery for the location. See
[`gabion_limit off` vs `gabion off`](#gabion_limit-off-vs-gabion-off).

### Scale beyond one node

Add the cluster-side directives at the `http {}` level:

```nginx
gabion_gossip_bind 0.0.0.0:9000;
gabion_gossip_cluster 0xc0ffee;                       # any non-zero u128 shared across peers
gabion_discovery_namespace_allow my-app-namespace;    # Kubernetes EndpointSlice discovery
```

Every gabion process that shares `gabion_gossip_cluster` and can reach
each other's `gabion_gossip_bind` socket will exchange counters. See
[Running across a cluster](#running-across-a-cluster) for the full
checklist.

## Directive reference

### `gabion_limit_zone zone=NAME:SIZE` (http)

Allocates the shared-memory zone all rules live in. Required exactly once
per `http {}` block. Matches the shape of nginx core's `limit_req_zone
zone=name:size`.

```nginx
gabion_limit_zone zone=api:128m;
```

### `gabion_limit_rule NAME [$bindings...] rate=Nr/<unit> [...]` (http)

Declares a rate-limit rule. Positional arguments after the name are
descriptor bindings; everything else is a `keyword=value` named argument or
a bare flag.

```nginx
gabion_limit_rule per_ip    $remote_addr                  rate=100r/s;
gabion_limit_rule per_uri   $uri                          rate=10r/s;
gabion_limit_rule per_route tenant:$arg_tenant path:$uri  rate=5r/s;

gabion_limit_rule by_asn    $geoip2_asn_number   rate=50r/s except_if=$trusted_ip;
gabion_limit_rule by_bot    class:$bot_class     rate=10r/s;
gabion_limit_rule shadow    $uri                 rate=1r/s  dry_run;

# Non-round periods: any humantime-parsable duration after `r/`.
gabion_limit_rule slow_path $uri                 rate=5r/30s;
gabion_limit_rule daily_cap tenant:$arg_tenant   rate=10000r/d;

# Explicit window= widens the time horizon; the resolved limit scales
# from the rate up to fit. `rate=10r/s window=5h bucket=1h` enforces
# a 180k-over-5h budget across five 1h buckets.
gabion_limit_rule sustained tenant:$arg_tenant   rate=10r/s window=5h bucket=1h;
```

#### Descriptor bindings

A binding pairs a descriptor `key` with the variable expression evaluated
at request time:

| Form                              | Effect                                                         |
|-----------------------------------|----------------------------------------------------------------|
| `$identifier`                     | Auto-keyed by the variable name (`$uri` → key `uri`).          |
| `name:$identifier`                | Explicit key; single-variable.                                 |
| `name:"prefix-$foo-$bar"`         | Explicit key; template — compiled to a complex value.          |

Single-variable bindings (forms 1 and 2) dispatch through nginx's indexed
variable subsystem — O(1) array lookup, zero allocation per request. The
inline fast path (`$uri`, `$request_uri`, `$args`, `$remote_addr`,
`$arg_*`) is even cheaper — no FFI hop at all. Template bindings compile
via `ngx_http_compile_complex_value` at config phase and allocate ~tens of
bytes per evaluation against the request pool; they are fine for
operator-meaningful compositions but pay for what they use.

#### Named arguments

| Argument                | Meaning                                                                                                                                    |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `rate=Nr/<unit>`        | **Required.** `N` requests per the period named by `<unit>`. `<unit>` is `s\|m\|h\|d` (1 second / 1 minute / 1 hour / 1 day) or any humantime duration like `30s`, `5m`, `2h30m`. Must be a positive integer. |
| `window=DURATION`       | Time horizon the rate is enforced over (default: the rate's period). When set, the resolved limit scales up to `floor(rate_count * window / period)`. See "Rate, window, and bucket" below — and watch out for the burstable-budget gotcha. |
| `bucket=DURATION`       | Bucket granularity inside the window (default: the resolved window — one fixed-window bucket). Smaller buckets enforce more smoothly; larger buckets cost less memory and gossip traffic. |
| `mode=enforce`          | Default. Evaluate and reject on overflow.                                                                                                  |
| `mode=dry_run`          | Evaluate, record the hit, never reject. Lets operators observe before enforcing.                                                           |
| `mode=disabled`         | Skip the rule entirely.                                                                                                                    |
| `dry_run`               | Bare flag; alias for `mode=dry_run`.                                                                                                       |
| `except_if=$variable`   | Skip this rule for requests where `$variable` resolves truthy. See "Predicates" below.                                                     |
| `domain=NAME`           | Domain bucket for the rule (defaults to `nginx`). Must match `[A-Za-z_][A-Za-z0-9_.-]*`.                                                   |

#### Rate, window, and bucket

A rule resolves to three internal numbers — a `limit`, a `window`, and a
`bucket`. The directive surface gives you three knobs, evaluated in
this order:

* `rate=Nr/<unit>` (mandatory) — sustained allowance and its natural
  period.
* `window=DURATION` (optional) — widens the time horizon. The resolved
  internal limit is scaled up to fit:
  `limit = floor(rate_count * window_millis / period_millis)`.
  Omitted, the window equals the rate's period.
* `bucket=DURATION` (optional) — granularity inside the window. Omitted,
  the bucket equals the resolved window (one fixed-window bucket).

Worked example:

```nginx
gabion_limit_rule per_tenant tenant:$arg_tenant rate=10r/s window=5h bucket=1h;
```

resolves to `limit = 10 * 5 * 3600 = 180000`, `window = 5h`,
`bucket = 1h` (five live buckets). The same triple can be written as
`rate=180000r/5h bucket=1h` — the operator-facing knob is identical,
but `rate=10r/s window=5h bucket=1h` preserves the "10 r/s applied
over 5 hours" intent in the config text.

> **Rule of thumb.** If you set `window=` larger than the rate's period
> and don't also set `bucket=`, you get a *burstable* budget — clients
> can fire the whole window's allowance instantly, then sit empty for
> the rest of the window. For sustained-rate enforcement, set `bucket=`
> close to the rate's period. Example: `rate=10r/s window=5h bucket=1s`
> keeps the 180k 5-hour budget but smooths it to roughly 10 r/s.

Other things worth knowing:

* **`X-RateLimit-Limit` reports the *resolved* number.** A rule written
  as `rate=10r/s window=1h` returns `X-RateLimit-Limit: 36000`. If you
  surface that header to end users (dashboards, customer-facing error
  pages), be aware they will see 36000, not 10.
* **`Retry-After` scales with the resolved window.** With `window=5h`
  a rejected client may be told to wait up to 5 hours; without an
  explicit window, the worst case is the rate's period.
* **Floor silently under-budgets non-multiples.** `rate=10r/m
  window=85s` resolves to `limit=14` (the leftover 0.16 period
  vanishes). Pick `window=` values that are integer multiples of the
  rate's period when you care about every last request.
* **`window=` shorter than the rate's period is rejected.** To enforce
  "100 in 500ms" write `rate=100r/500ms`, not
  `rate=200r/s window=500ms` — the latter would resolve to `limit=0`
  and is refused at `nginx -t` time.

### `gabion_limit NAME [NAME ...]` (http, server, location)

Applies one or more rules at the current scope.

```nginx
gabion_limit per_ip per_tenant by_asn;
```

#### Inheritance and overriding

The inheritance shape mirrors nginx core's `limit_req`:

- **No `gabion_limit` at this level** → the location inherits its
  parent's set verbatim.
- **One or more `gabion_limit` directives at this level** → the child's
  set **replaces** the parent's entirely. The replacement is the *full
  set declared at the child level*, not the parent's set unioned with
  the child's additions. Multiple `gabion_limit` directives within the
  same level accumulate with each other before the merge.

This replace-on-redeclare semantics is the standard way to override
inherited limits at a sub-location. To drop a rule, simply omit it
from the child's list:

```nginx
server {
    gabion_limit per_ip per_tenant;          # baseline at server level

    location /api/ {
        # No gabion_limit here → inherits per_ip + per_tenant.
    }

    location /api/upload {
        # Replaces baseline. Only upload_throttle applies here;
        # per_ip and per_tenant do NOT.
        gabion_limit upload_throttle;
    }

    location /api/internal {
        # Replaces baseline with a narrower set. Only per_ip applies;
        # per_tenant is dropped.
        gabion_limit per_ip;
    }

    location /api/billing {
        # Replaces baseline with the union of the baseline plus an
        # extra rule. Operators who want "parent's set + one more"
        # must restate the parent's set explicitly.
        gabion_limit per_ip per_tenant billing_throttle;
    }
}
```

There is no "append to parent" form. If a child location wants
*everything the parent applied, plus one more*, it must restate the
full set explicitly (the `/api/billing` example). This is deliberate —
silent inheritance plus override would let a remote `gabion_limit` in
a parent change behaviour in an apparently scoped sub-location.

#### `gabion_limit off`

`gabion_limit off` (one-arg) locally suppresses all rules at this level
without disabling the module entirely. Use it when you want a scoped
opt-out from inherited rules while preserving any future gabion
machinery (metrics, headers) for the location. See
[`gabion_limit off` vs `gabion off`](#gabion_limit-off-vs-gabion-off)
for the comparison with the access-handler-skipping form.

### `gabion on | off` (http, server, location)

`gabion off` disables the access handler entirely for this scope — no
rules evaluated, no access-phase work. `gabion on` re-enables where a
parent had it off.

`gabion off` is the foolproof way to fully bypass a parent's rule stack
in a sub-location.

#### `gabion_limit off` vs `gabion off`

The two `off` modes are deliberately distinct:

- **`gabion_limit off`** keeps the access handler running and produces a
  clean access-phase decision, but evaluates no rules at this scope.
  Use this when you want a scoped opt-out from inherited rules while
  preserving any future gabion machinery (metrics, headers) for the
  location.
- **`gabion off`** skips the access handler entirely — no rule lookup,
  no SHM read. Use this when you want zero per-request cost (e.g.
  `/static/`, `/healthz`).

Both shapes are nginx-idiomatic (`limit_req off`, `auth_basic off`); see
the layering example below for them side by side.

## Composition: layering rules

Each rule is an independent gate. A request is allowed only if **every**
rule allows it. Rules evaluate in declaration order; the first enforcing
reject wins. Rejection from any enforcing rule rejects the request, with
`Retry-After` and the `X-RateLimit-*` triplet pinned to the rule with the
longest window so the client doesn't immediately get re-rejected by a
wider rule. See the [fail-open invariant](../../README.md#fail-open-invariant)
for what happens when something else goes wrong.

```nginx
http {
    gabion_limit_zone zone=api:128m;
    gabion_limit_rule per_ip     $remote_addr           rate=100r/s;
    gabion_limit_rule per_tenant tenant:$arg_tenant     rate=10r/s;

    server {
        gabion_limit per_ip per_tenant;     # baseline at server level
        location /api/      { /* inherits per_ip + per_tenant */ }
        location /api/upload {
            # Replaces the baseline: only upload_throttle applies here.
            # per_ip and per_tenant are NOT in effect — restate them
            # explicitly if you want them to keep applying.
            gabion_limit upload_throttle;
        }
        location /api/healthz { gabion_limit off; }   # local opt-out
        location /static/     { gabion off; }         # skip access handler entirely
    }
}
```

### First-class ASN / UA / IP-range limits and bypasses

The combination of generic variable lookup + `except_if=` predicates +
multi-rule stacking lets operators treat trusted crawlers (Google,
Microsoft, Cloudflare, etc.) specially in a single location.

```nginx
http {
    gabion_limit_zone zone=api:128m;

    geo $trusted_ip {
        default 0;
        127.0.0.1/32 1;
    }
    map $http_user_agent $bot_class {
        default      other;
        ~*Googlebot  google;
        ~*bingbot    ms;
        ~*facebook   fb;
    }

    gabion_limit_rule per_ip  ip:$remote_addr     rate=50r/s except_if=$trusted_ip;
    gabion_limit_rule per_bot class:$bot_class    rate=10r/s;
    gabion_limit_rule per_uri $uri                rate=10r/s;

    server {
        listen 8080;
        location /api/ {
            gabion_limit per_ip per_bot per_uri;
        }
    }
}
```

Trusted IPs bypass `per_ip` but still gate against `per_bot` and `per_uri`.
A misbehaving Googlebot still trips the `per_bot` 10r/s cap. The `per_uri`
floor catches anything else trying to hammer a single endpoint.

#### Cardinality safety

`$http_user_agent` raw will explode the descriptor space — every unique
UA string becomes a distinct counter. Always map UAs through a small
`map` block first (the `$bot_class` recipe above) so the descriptor
cardinality is bounded.

`$geoip2_asn_number` from `ngx_http_geoip2_module` works the same way:
the value is already bucketed to ASN numbers, but if you key on the
human-readable `$geoip2_asn` (organisation name) you'll want to bound
it. IPv6 ASN attribution at MaxMind is per-`/64`.

## Predicates: `except_if=$variable`

The `except_if=` argument names a single nginx variable. When that
variable resolves to a truthy value at request time, the rule is skipped.

Truthy ≡ non-empty AND **not** in the case-insensitive falsy set
`{ "0", "false", "off", "no" }`. Anything else (including `"1"`, `"true"`,
`"yes"`, or any arbitrary non-empty string) means "exempt this rule".

```nginx
geo $trusted_ip {
    default 0;
    10.0.0.0/8 1;   # internal network bypasses this rule
}
gabion_limit_rule public_traffic $remote_addr rate=100r/s except_if=$trusted_ip;
```

Semantics worth knowing:

- **One `except_if=` per rule.** The parser holds a single predicate
  slot. If a rule declaration repeats `except_if=`, the last one wins
  silently — no error, no accumulation. To compose multiple bypass
  conditions, pre-combine them in a `map` or `geo` block (see below).
- **Predicates never contribute to cardinality.** Evaluated before the
  byte-budget check so a truthy predicate exempts a request without
  billing the cardinality budget.
- **Predicate variables missing at request time fall through.** Per
  allow-by-default, a missing variable means "predicate did not fire" —
  the rule applies as usual. Operator-typo protection happens at
  startup, where `nginx -t` rejects predicates that name an undefined
  variable.
- **Exempted requests bump a distinct counter.** `Stats::exempted`
  separates "this request was allowed because the predicate fired" from
  the generic allow counter, so a misconfigured always-true predicate
  shows up in metrics.

### Combining multiple bypass conditions

To exempt a rule when **any of several conditions** fires (logical OR),
fold the source variables into one with `map`:

```nginx
# Truthy when EITHER the IP is trusted OR the request carries an admin token.
map $trusted_ip$is_admin $exempt {
    default 0;
    "~.*1.*"  1;   # any "1" anywhere in the concatenated string
}
gabion_limit_rule public_traffic $remote_addr rate=100r/s except_if=$exempt;
```

Adjust the regex (or use a literal `map` with explicit cases) to match
whatever truthy convention your source variables emit.

To exempt a rule only when **all** of several conditions fire (logical
AND), again fold them with `map`:

```nginx
# Truthy ONLY when the IP is trusted AND the request carries an admin token.
map $trusted_ip:$is_admin $exempt {
    default  0;
    "1:1"    1;
}
gabion_limit_rule sensitive_route $uri rate=10r/s except_if=$exempt;
```

The pattern generalises: build whatever boolean expression you need in a
`map` (or chain of `map`s, or a small block of `geo` + `map`) so that
the final variable is truthy exactly when you want the rule exempted,
then pass it to `except_if=`. Doing the composition in nginx core
keeps gabion's per-request work to a single variable lookup.

## Dry-run mode

```nginx
gabion_limit_rule canary $uri rate=10r/s dry_run;
gabion_limit per_ip canary;     # canary stacks with per_ip but never rejects
```

Dry-run rules evaluate the descriptor, look up the aggregate, and record
the hit (so metrics and gossip see real traffic) — but never produce a
reject verdict. Useful for sizing a new rule before flipping it to
`enforce`.

## Discovery directives

| Directive                                     | Description                                                                                  |
|-----------------------------------------------|----------------------------------------------------------------------------------------------|
| `gabion_discovery_namespace_allow NAMESPACE`  | Restrict Kubernetes EndpointSlice discovery to the listed namespaces. Empty = all.           |
| `gabion_discovery_service_allow SERVICE`      | Restrict discovery to the listed Service names. Empty = all.                                 |
| `gabion_discovery_self_addr ADDR`             | Local gossip address that should be excluded from discovered peers.                          |

## Gossip directives

The full list lives in `module.rs`; the common ones:

| Directive                                   | Description                                                                        |
|---------------------------------------------|------------------------------------------------------------------------------------|
| `gabion_gossip_bind ADDR:PORT`              | UDP bind for the gossip channel.                                                   |
| `gabion_gossip_cluster ID`                  | Cluster identifier (u128 hash); peers must share.                                  |
| `gabion_gossip_fanout N`                    | How many peers each tick selects (defaults to 6).                                  |
| `gabion_gossip_tick_interval DURATION`      | Gossip cycle period (default 100ms).                                               |
| `gabion_gossip_target_err_bps N`            | Per-rule unreplicated-error budget in bps of the rule's limit (default 100 = 1 %). |
| `gabion_gossip_min_emit_interval DURATION`  | Floor between threshold-fire emissions (default 5ms).                              |

## Running across a cluster

Beyond a single nginx box, gabion's value is shared counters. Three
pieces of plumbing make a cluster — the same three across both adapters
(see the [cross-adapter overview](../../README.md#running-across-a-cluster)):

1. **Bind a gossip socket** so peers can talk to each other.
   `gabion_gossip_bind ADDR:PORT` opens a UDP socket. UDP is intentional
   — gabion's wire codec is self-describing and loss-tolerant; one
   dropped frame just means counters re-converge on the next tick.

2. **Pick a cluster identifier.** Every gabion process that should share
   counters declares the same `gabion_gossip_cluster ID` (any non-zero
   u128). Frames from peers with a mismatched cluster ID are dropped on
   the floor — this is the cheap firewall against accidental
   cross-cluster contamination.

3. **Tell peers how to find each other.** The simplest production path
   is Kubernetes EndpointSlice discovery: declare which namespaces and
   service names to watch, and gabion picks up peer pods as they come
   and go. No static peer list to maintain.

   ```nginx
   gabion_discovery_namespace_allow my-app-namespace;
   gabion_discovery_service_allow   gabion-nginx;
   gabion_discovery_service_allow   gabiond;          # if running mixed
   ```

   Each directive takes one name; repeat to allow multiple. Without any
   `..._allow` directive, gabion falls back to the pod's own namespace.

A complete cluster-side `http {}` block looks like:

```nginx
http {
    gabion_limit_zone zone=api:128m;

    gabion_limit_rule per_ip $remote_addr rate=100r/s;

    gabion_gossip_bind 0.0.0.0:9000;
    gabion_gossip_cluster 0xc0ffee;
    gabion_gossip_fanout 6;                      # peers per tick; default 6

    gabion_discovery_namespace_allow my-app;
    gabion_discovery_service_allow   gabion-nginx;

    server { listen 8080; location / { gabion_limit per_ip; } }
}
```

Tuning the gossip cadence is rarely necessary — the defaults converge
in well under a second at production scale. The settings that matter:

| Directive                                   | When to touch it                                                                                                |
|---------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| `gabion_gossip_fanout N`                    | How many peers each tick selects. Increase only if convergence is too slow at high cluster sizes (>50 peers).   |
| `gabion_gossip_tick_interval DURATION`      | Cycle period (default `100ms`). Shorter = faster convergence, more UDP traffic. Lengthen at large fleet sizes.  |
| `gabion_gossip_target_err_bps N`            | Threshold-fire budget in basis points of the rule's limit (default `100` = 1%). Lower = tighter accuracy, more emissions. |
| `gabion_gossip_min_emit_interval DURATION`  | Floor between threshold-fires (default `5ms`). Raise when the gossip channel itself becomes the bottleneck.     |

See [`crates/gossip-bench/README.md`](../gossip-bench/README.md) for
the simulator that produces measured convergence curves at different
fanouts and cluster sizes.

### Verifying the cluster has converged

After deploy, check three things:

1. **Process logs.** Each process logs the gossip bind, its derived
   node identity, and the count of discovered peers. If a process logs
   `discovered 0 peers`, the discovery filter is wrong (namespace or
   service mismatch).
2. **Counter delta under load.** Send traffic to one replica only;
   counters on every other replica should rise within a tick or two.
   If they don't, the gossip channel is partitioned (UDP firewall,
   cluster-ID mismatch, or wrong `gabion_gossip_bind` reachability).
3. **`gabiond` `/snapshot`** (server adapter only) returns the full
   peer/cell view; a similar HTTP endpoint is on roadmap for the
   nginx module.

## Configuration error messages

Every `gabion_*` directive emits an operator-readable error at `nginx -t`
time when something is wrong, with the offending value quoted and the
fix named. Examples:

```
gabion: `gabion_limit_zone` argument must start with `zone=` (e.g. `zone=api:128m`)
gabion: `gabion_limit_rule` rule `per_ip` is missing the required `rate=Nr/s` argument
gabion: `gabion_limit_rule` argument `key=$uri` is invalid: expected `$variable`, `name:$variable`, or one of `rate=`, `bucket=`, `mode=`, `dry_run`, `except_if=`, `domain=`
gabion: `gabion_limit_rule` argument `rate=100r/fortnight` is invalid: rate period must be `s`, `m`, `h`, `d`, or a duration like `30s`, `5m`
gabion: `gabion_limit_rule` rule `zero_window`: `window=` must be greater than zero
gabion: `gabion_limit_rule` rule `inverted`: `window=` must be at least as long as the rate's period; a sub-period window would resolve to a zero limit. To enforce N requests in a shorter span, write the period into the rate itself (e.g. `rate=100r/500ms`).
gabion: `gabion_limit_rule` rule `per_ip` is declared more than once; rule names must be unique within an http {} block
gabion: `gabion_limit` references rule `tenant_api`, which is not declared via `gabion_limit_rule`
gabion: `gabion_gossip_tick_interval` rejected value `notaduration`: expected a duration like `100ms` or `1s`
```

## Troubleshooting

One-line "you'll see this when / what to do" for the messages
operators most commonly hit.

| Symptom                                                                                                   | What it means                                                                                                                              | Fix                                                                                                                              |
|-----------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `nginx -t` says `unknown 'foo' variable`                                                                  | A `gabion_limit_rule` references a variable no loaded module defines.                                                                      | Load the providing module (`geoip2`, `map`, `geo`) before the gabion directive that references it.                               |
| `gabion_limit references rule X, which is not declared`                                                   | A `gabion_limit X;` names a rule that has no `gabion_limit_rule X ...` declaration in the same `http {}` block.                            | Add the missing declaration or fix the name.                                                                                     |
| `gabion_limit_rule rule X is declared more than once`                                                     | Two `gabion_limit_rule X ...` directives with the same name.                                                                               | Pick distinct names. The grammar is unambiguous; this is almost always a copy-paste bug.                                         |
| `gabion_limit_rule argument 'rate=0r/s' is invalid`                                                       | Zero rate; would deny all traffic.                                                                                                         | Pick a non-zero positive integer. To temporarily disable a rule, use `mode=disabled` instead.                                    |
| `gabion_limit_rule rule X: window= must be at least as long as the rate's period`                         | `window=` was paired with a rate whose period is longer (e.g. `rate=200r/s window=500ms`); the resolved limit would be zero.               | Move the period into the rate itself (e.g. `rate=100r/500ms`) instead of pairing a short window with a longer-period rate.       |
| `gabion_gossip_cluster requires a non-zero cluster identifier`                                            | The cluster ID parses to `0`, which is almost certainly unintended.                                                                        | Pick any non-zero 128-bit value shared by every peer (`1`, `0xc0ffee`, a u128 literal).                                          |
| `gabion: gabion_gossip_tick_interval rejected value 'notaduration': expected a duration like '100ms'`     | A tuning directive received a value it couldn't parse.                                                                                     | The error message names the directive and the expected format; supply a humantime duration (`100ms`, `5s`).                      |
| Responses include `X-RateLimit-Remaining: 0` and `429 Too Many Requests`                                  | The client crossed a rule's limit.                                                                                                         | Expected behaviour. `Retry-After` says how long to back off.                                                                     |
| `400 Bad Request` from gabion                                                                             | Pathological request: client supplied more descriptor bytes than `gabion_storage_max_descriptor_bytes` permits.                            | Either tighten the upstream client or raise `gabion_storage_max_descriptor_bytes` after sanity-checking why it's that large.     |
| `gabion: ... matched rules cap exceeded` in nginx error log                                               | The location stacked more rules than `STORAGE_MAX_MATCHED_RULES` permits. **The request was allowed** (allow-by-default).                  | Reduce the number of rules applied at this location or split the location.                                                       |
| `gabion: ... gossip background task has stopped` in error log                                             | The leader thread exited. Cluster-wide convergence stops; admission still runs locally.                                                    | Check earlier log lines for the underlying error. Restart the worker (or the pod) to re-elect a leader.                          |

Operator-facing log lines all follow the three-question shape from
`CLAUDE.md`: *what happened*, *why it's likely happening*, *what to do
next*. Open an issue if you see one that doesn't end with a concrete
next step.

## Unknown variable detection

A typo in a `gabion_limit_rule` binding source — `except_if=$tursted_ip`
when the operator meant `$trusted_ip` — fails `nginx -t` *before* the
worker accepts a single request. The detection happens inside nginx core:
`ngx_http_get_variable_index` declares a dependency on the variable name
at config phase; nginx then runs `ngx_http_variables_init_vars` after
every module's `postconfiguration` callback, walks each declared
dependency, and emits a "unknown 'tursted_ip' variable" error and a
non-zero exit when no module provides a getter.

Gabion deliberately does **not** layer a second validator on top — the
core check is exhaustive and the error message is already operator-clear.
Make sure the module that provides the variable (e.g.
`ngx_http_geoip2_module`, the `map` directive that defines `$bot_class`,
the `geo` directive that defines `$trusted_ip`) is loaded before the
`gabion_limit_rule` directive that references it.

## Migration from the previous DSL

Pre-1.0: there's no deprecation cycle, just one-shot updates to operator
configs.

| Before                                                   | After                                                       |
|----------------------------------------------------------|-------------------------------------------------------------|
| `gabion_limit_zone NAME SIZE`                            | `gabion_limit_zone zone=NAME:SIZE`                          |
| `gabion_limit_rule NAME 2r/m key=$uri window=60s`        | `gabion_limit_rule NAME $uri rate=2r/m`                     |
| `gabion_limit_rule NAME $uri rate=10r/s window=1s`       | `gabion_limit_rule NAME $uri rate=10r/s` (rate's period is the default window) |
| `gabion_limit_rule NAME $uri rate=10r/m window=30s`      | `gabion_limit_rule NAME $uri rate=10r/30s` (duration after `r/`) — or `rate=10r/m window=30s` if you want to keep "10/min" in the text (resolves to limit=5 over 30s) |
| `bucket=` default of `1s`                                | `bucket=` defaults to the rate's window (single fixed-window bucket); set explicitly for sub-window granularity |
| `key=tenant:$arg_tenant`                                 | `tenant:$arg_tenant` (positional)                           |
| `gabion_limit foo` only                                  | `gabion_limit foo bar baz` / `gabion_limit off`             |
| `gabion_gossip_discovery_namespace NS`                   | `gabion_discovery_namespace_allow NS`                       |
| `gabion_discovery_namespace_whitelist NS`                | `gabion_discovery_namespace_allow NS`                       |
| `gabion_discovery_service_whitelist SVC`                 | `gabion_discovery_service_allow SVC`                        |

The directive surface also gained an explicit `window=` for operators
whose mental model is "N requests per second, applied over an H-hour
window". `rate=10r/s window=5h` resolves to a 180,000-over-5-hour
budget — equivalent to `rate=180000r/5h`, but the original "10 r/s"
intent survives in the config text. Read the new
[Rate, window, and bucket](#rate-window-and-bucket) section before
you reach for `window=` — long windows with the default `bucket=`
produce a *burstable* budget, not a paced one.
