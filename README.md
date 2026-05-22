# Gabion

Distributed rate limiter. Cluster members maintain per-origin counters in a
CRDT, exchange them over an anti-entropy UDP gossip protocol, and admit or
reject incoming requests against the cluster-wide aggregate. Two adapters
consume the same core: `gabiond`, an Envoy-compatible gRPC service, and an
in-process NGINX module.

Cluster-wide counts are eventually consistent. Admission is local and
allocation-free; under heavy load operators pay for one atomic read of SHM
(nginx) or one `DashMap` lookup (server), no syscalls.

This README covers the NGINX module's directive surface. For the broader
architecture see `CLAUDE.md`; for CRDT internals see `docs/CRDT Module.md`.

## NGINX directive reference

### `gabion_limit_zone zone=NAME:SIZE` (http)

Allocates the shared-memory zone all rules live in. Required exactly once
per `http {}` block. Matches the shape of nginx core's `limit_req_zone
zone=name:size`.

```nginx
gabion_limit_zone zone=api:128m;
```

### `gabion_limit_rule NAME [$bindings...] rate=Nr/{s,m,h} [...]` (http)

Declares a rate-limit rule. Positional arguments after the name are
descriptor bindings; everything else is a `keyword=value` named argument or
a bare flag.

```nginx
gabion_limit_rule per_ip    $remote_addr                  rate=100r/s window=1s;
gabion_limit_rule per_uri   $uri                          rate=10r/s  window=1s;
gabion_limit_rule per_route tenant:$arg_tenant path:$uri  rate=5r/s   window=1s;

gabion_limit_rule by_asn    $geoip2_asn_number   rate=50r/s window=1s except_if=$trusted_ip;
gabion_limit_rule by_bot    class:$bot_class     rate=10r/s window=1s;
gabion_limit_rule shadow    $uri                 rate=1r/s  window=1s dry_run;
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

| Argument                | Meaning                                                                                 |
|-------------------------|-----------------------------------------------------------------------------------------|
| `rate=Nr/{s,m,h}`       | **Required.** Rate; the unit-letter is informational, the actual window is `window=`.   |
| `window=DURATION`       | Sliding-window length (default `60s`).                                                  |
| `bucket=DURATION`       | Bucket granularity inside the window (default `1s`). Sub-second buckets are fine.       |
| `mode=enforce`          | Default. Evaluate and reject on overflow.                                               |
| `mode=dry_run`          | Evaluate, record the hit, never reject. Lets operators observe before enforcing.        |
| `mode=disabled`         | Skip the rule entirely.                                                                 |
| `dry_run`               | Bare flag; alias for `mode=dry_run`.                                                    |
| `except_if=$variable`   | Skip this rule for requests where `$variable` resolves truthy. See "Predicates" below.  |
| `domain=NAME`           | Domain bucket for the rule (defaults to `nginx`).                                       |

### `gabion_limit NAME [NAME ...]` (http, server, location)

Applies one or more rules at the current scope. Multiple `gabion_limit`
directives within the same level accumulate; declaring `gabion_limit` at a
child level **replaces** the parent's set rather than appending — exactly
the inheritance shape `limit_req` uses.

```nginx
gabion_limit per_ip per_tenant by_asn;
```

`gabion_limit off` (one-arg) locally suppresses all rules at this level
without disabling the module entirely.

### `gabion on | off` (http, server, location)

`gabion off` disables the access handler entirely for this scope — no
rules evaluated, no access-phase work. `gabion on` re-enables where a
parent had it off.

`gabion off` is the foolproof way to fully bypass a parent's rule stack
in a sub-location.

## Composition: layering rules

Each rule is an independent gate. A request is allowed only if **every**
rule allows it. Rejection from any enforcing rule rejects the request, with
`Retry-After` and the `X-RateLimit-*` triplet pinned to the rule with the
longest window so the client doesn't immediately get re-rejected by a
wider rule.

```nginx
http {
    gabion_limit_zone zone=api:128m;
    gabion_limit_rule per_ip     $remote_addr           rate=100r/s window=1s;
    gabion_limit_rule per_tenant tenant:$arg_tenant     rate=10r/s  window=1s;

    server {
        gabion_limit per_ip per_tenant;     # baseline at server level
        location /api/      { /* inherits per_ip + per_tenant */ }
        location /api/upload {
            gabion_limit per_ip per_tenant upload_throttle;  # REPLACES baseline
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

    gabion_limit_rule per_ip  ip:$remote_addr     rate=50r/s window=1s except_if=$trusted_ip;
    gabion_limit_rule per_bot class:$bot_class    rate=10r/s window=1s;
    gabion_limit_rule per_uri $uri                rate=10r/s window=1s;

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
gabion_limit_rule public_traffic $remote_addr rate=100r/s window=1s except_if=$trusted_ip;
```

Semantics worth knowing:

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

## Dry-run mode

```nginx
gabion_limit_rule canary $uri rate=10r/s window=1s dry_run;
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

| Directive                              | Description                                              |
|----------------------------------------|----------------------------------------------------------|
| `gabion_gossip_bind ADDR:PORT`         | UDP bind for the gossip channel.                         |
| `gabion_gossip_cluster ID`             | Cluster identifier (u128 hash); peers must share.        |
| `gabion_gossip_fanout N`               | How many peers each tick selects (defaults to 6).        |
| `gabion_gossip_tick_interval DURATION` | Gossip cycle period (default 100ms).                     |

## Configuration error messages

Every `gabion_*` directive emits an operator-readable error at `nginx -t`
time when something is wrong, with the offending value quoted and the
fix named. Examples:

```
gabion: `gabion_limit_zone` argument must start with `zone=` (e.g. `zone=api:128m`)
gabion: `gabion_limit_rule` rule `per_ip` is missing the required `rate=Nr/s` argument
gabion: `gabion_limit_rule` argument `key=$uri` is invalid: expected `$variable`, `name:$variable`, or one of `rate=`, `window=`, `bucket=`, `mode=`, `dry_run`, `except_if=`, `domain=`
gabion: `gabion_limit` references rule `tenant_api`, which is not declared via `gabion_limit_rule`
```

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

## Fail-open invariant

The only path that can return `429` is a successful, decisive determination
that a request crossed a configured limit. Every other condition — variable
missing, predicate unresolved, template allocation failure, queue full,
shared-memory accessor unavailable, anything unanticipated — results in
allow-through. The request counter only increments when we record an allow
into the queue; rejects, declines, cardinality skips, and queue-drops
never push a `QueueEvent`.

The single deliberate exception is **descriptor byte budget** (`max_descriptor_bytes`),
which returns `400 Bad Request` because the request itself is pathological
(client-supplied input over budget). All gabion-internal limits (matched
rules cap, rule-table lookup miss, …) decline rather than reject.

## Test runner & toolchain

`cargo nextest` is the only sanctioned test runner. `cargo +nightly fmt` is
the only sanctioned formatter. `make test` runs fmt-check, clippy, the
workspace nextest suite, the safety integration tests, and hygiene. `make
ci` adds Miri (Stacked Borrows) and the nginx smoke tests.

### Miri coverage of the SHM unsafe surface

Every `unsafe` block that touches the shared-memory zone, raw pointers into
Rust-managed memory, or atomic operations on shared state is exercised by
`crates/nginx/tests/safety.rs`, which runs under both Stacked Borrows
(`make miri-safety`) and Tree Borrows (`make miri-safety-tb`). The table
below maps each unsafe site to the test(s) that cover it.

| Unsafe site                                       | Test(s) in `safety.rs`                                                                                                            |
|---------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `ShmRegion::initialize` (`shm.rs`)                | `master_stamps_node_id_and_initializes_region`, `concurrent_leader_writer_and_worker_readers`, `end_to_end_workers_push_leader_drains_workers_read`, `decide_and_leader_apply_concurrent`, `decide_all_multi_rule_concurrent` |
| `ShmRegion::from_initialized`                     | `worker_view_via_from_initialized_sees_master_writes`, all concurrent tests                                                       |
| `ShmAggregateStore::new` + `apply`                | `leader_stamps_incarnation_and_applies_deltas`, `concurrent_leader_writer_and_worker_readers`, `end_to_end_workers_push_leader_drains_workers_read`, `decide_all_multi_rule_concurrent` |
| `AggregateTable::get` / seqlock read              | `access_path_allows_then_rejects_via_aggregate_seqlock`, all concurrent tests                                                     |
| `RequestQueue::push` / `pop` (MPSC)               | `end_to_end_workers_push_leader_drains_workers_read` (3 producers + 1 consumer)                                                   |
| `LeaderLease::try_acquire` / `release`            | `lease_takeover_under_contention`, `lease_concurrent_acquire_distinct_winners`                                                    |
| `unsafe impl Send + Sync for BindingLookup`       | Documented contract; not Miri-testable (FFI-pointer-typed; soundness rests on nginx cycle-pool semantics)                         |
| `unsafe impl Send + Sync for TestZone`            | Test-only; sound by construction (single owner, Box-backed)                                                                       |

**FFI unsafe is necessarily uncovered by Miri.** Three FFI calls live in
`NgxBindingCompiler::compile` (`ngx_http_get_variable_index`, `ngx_palloc`,
`ngx_http_compile_complex_value`); five more in `RequestVariables::lookup`
(`ngx_http_get_indexed_variable`, the complex-value accessor). The
remaining FFI sites are nginx directive handlers and lifecycle hooks. None
can run under Miri because Miri cannot execute nginx C code. They are
entirely gated behind the `ngx-module` Cargo feature, which the Miri test
build does not enable; soundness rests on the documented nginx contract
(single-threaded config phase, pool-owned token storage, request lifetime
tied to the access-phase handler), and every site carries a SAFETY block
spelling out the relevant precondition.

## Migration from the previous DSL

Pre-1.0: there's no deprecation cycle, just one-shot updates to operator
configs.

| Before                                                   | After                                                       |
|----------------------------------------------------------|-------------------------------------------------------------|
| `gabion_limit_zone NAME SIZE`                            | `gabion_limit_zone zone=NAME:SIZE`                          |
| `gabion_limit_rule NAME 2r/m key=$uri window=60s`        | `gabion_limit_rule NAME $uri rate=2r/m window=60s`          |
| `key=tenant:$arg_tenant`                                 | `tenant:$arg_tenant` (positional)                           |
| `gabion_limit foo` only                                  | `gabion_limit foo bar baz` / `gabion_limit off`             |
| `gabion_gossip_discovery_namespace NS`                   | `gabion_discovery_namespace_allow NS`                       |
| `gabion_discovery_namespace_whitelist NS`                | `gabion_discovery_namespace_allow NS`                       |
| `gabion_discovery_service_whitelist SVC`                 | `gabion_discovery_service_allow SVC`                        |
