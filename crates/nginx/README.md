# gabion-nginx — gabion's NGINX module

`gabion-nginx` is gabion's in-process NGINX module: a dynamic `.so` that
loads into every worker, enforces rate limits at the access phase, and
(on the elected leader worker) drives the gossip runtime that keeps
counters in sync across replicas. Admission is decided against a
cluster-wide aggregate without ever leaving the worker process, so
each request costs neither an out-of-process call, a syscall, nor a
heap allocation.

If you're running Envoy sidecars instead of nginx, see
[`../server/README.md`](../server/README.md) for the gRPC adapter
that speaks the same gossip protocol over the same wire codec.

For the project-level overview, glossary, and the cross-adapter
explainer of rate / window / bucket, gossip, discovery, and the
fail-open invariant, see the [top-level README](../../README.md).

## Contents

- [Your first rule](#your-first-rule)
- [Common patterns](#common-patterns)
- [Per-request access flow](#per-request-access-flow)
- [Directive reference](#directive-reference)
- [Composition: rule layering](#composition-rule-layering)
- [Predicates: `except_if=$variable`](#predicates-except_ifvariable)
- [Dry-run mode](#dry-run-mode)
- [Running across a cluster](#running-across-a-cluster)
- [Configuration error messages](#configuration-error-messages)
- [Troubleshooting](#troubleshooting)
- [Unknown variable detection](#unknown-variable-detection)

## Your first rule

A minimal single-node nginx config that exercises gabion end-to-end:

```nginx
load_module /etc/nginx/modules/ngx_http_gabion_module.so;

worker_processes 2;
events {}

http {
    gabion_limit_zone zone=api:64m;
    gabion_limit_rule per_ip $remote_addr rate=100r/s;

    server {
        listen 8080;
        location / {
            gabion_limit per_ip;
            root html;
        }
    }
}
```

gabion runs in nginx's access phase, so it never sees a request that a
`return` directive answers first: `return` runs in the earlier rewrite
phase.

A request under budget gets `200 OK`; hammering the same IP past 100
in one second gets `429 Too Many Requests` together with
`X-RateLimit-Limit`, `X-RateLimit-Remaining: 0`, `X-RateLimit-Reset`,
and `Retry-After`. Those four headers are pinned to the rule with
the longest window, so a client that just tripped a narrow rule is
not immediately re-rejected by a wider one.

Before flipping a new rule to enforcing, it is worth running it in
dry-run first by appending the `dry_run` flag. The rule evaluates
and records hits — metrics and gossip see real traffic — but never
rejects, which lets you watch the allow/reject ratio against
production load before you drop the flag.

To scale beyond one node, bind the gossip socket on an address peers
can reach with `gabion_gossip_bind 0.0.0.0:9000`, set a
`gabion_gossip_cluster` id shared by every peer, and, under Kubernetes,
set `gabion_discovery_namespace_allow`. See
[Running across a cluster](#running-across-a-cluster).

<a id="fail-open-invariant"></a>

> **Fail-open in one paragraph.** Internal limits — matched-rule
> overflow, descriptor decode failures, a missing predicate variable —
> decline the request rather than reject it, so a bug or saturation in
> gabion never returns a spurious 429. The full statement of the
> invariant lives in the [root README](../../README.md#fail-open-invariant).

## Common patterns

Each subsection asks an operator's question first and then names the
recipe. You can stack as many as you need at one location; rules
compose left-to-right, and the first enforcing reject wins.

### How do you exempt internal traffic without measuring it?

```nginx
geo $trusted_ip {
    default 0;
    10.0.0.0/8 1;
    127.0.0.1/32 1;
}
gabion_limit_rule per_ip $remote_addr rate=100r/s except_if=$trusted_ip;

server {
    location /api/ { gabion_limit per_ip; }
}
```

`except_if=$trusted_ip` skips the rule when the variable resolves
truthy. An exempted request neither bumps the counter nor reaches
the aggregate, and it produces no gossip cell. See
[Predicates](#predicates-except_ifvariable) for the truthy/falsy set.

### How do you key off a tenant header instead of an IP?

```nginx
gabion_limit_rule per_tenant tenant:$arg_tenant rate=1000r/m;

server {
    location /api/ { gabion_limit per_tenant; }
}
```

`$arg_tenant` reads the `?tenant=` query argument and `tenant:` is
the descriptor key, so each distinct tenant gets its own counter.
A request without `?tenant=` produces an empty value, at which point
the rule declines without bumping a counter and the request flows
through.

### How do you roll out a new limit safely?

```nginx
# Step 1: ship in dry-run; observe the allow/reject ratio for a release window.
gabion_limit_rule new_route tenant:$arg_tenant path:$uri rate=50r/m dry_run;
gabion_limit new_route;

# Step 2: once the ratio looks right, drop `dry_run`.
gabion_limit_rule new_route tenant:$arg_tenant path:$uri rate=50r/m;
```

A dry-run rule still records hits into the SHM aggregate and
gossips them to peers, so capacity planning is truthful before you
flip the switch — you are sizing against real load rather than a
synthetic projection.

### How do you stack per-IP and per-tenant limits?

```nginx
gabion_limit_rule per_ip     $remote_addr        rate=100r/s;
gabion_limit_rule per_tenant tenant:$arg_tenant  rate=1000r/m;

location /api/ { gabion_limit per_ip per_tenant; }
```

Each rule is an independent gate, and a reject from either rejects
the request. Headers pin to the rule with the longer window so that
a 1s per-IP reject does not tell the client to retry after 1s when
the 1-minute per-tenant budget would also reject them.

### How do you exclude `/healthz` from rate limiting?

```nginx
location /healthz {
    gabion off;
    return 200;
}
```

`gabion off` skips the access handler entirely, so the request pays
zero per-request cost. It is the right choice for liveness probes
and static asset paths where you would rather not pay even a SHM
atomic load.

### How do you scale beyond one node?

```nginx
gabion_gossip_bind 0.0.0.0:9000;
gabion_gossip_cluster 0xc0ffee;
gabion_discovery_namespace_allow my-app-namespace;
```

Every gabion process that shares the cluster identifier and can
reach the others' gossip sockets exchanges counters with them. The
[Running across a cluster](#running-across-a-cluster) section names
the directives by role.

## Per-request access flow

```mermaid
flowchart TD
    Req([HTTP request enters access phase])
    Off{gabion off?}
    Empty{gabion_limit set\nempty / unset?}
    Loop[For each rule index in scope]
    Pred{except_if=$var\ntruthy?}
    Build[Build descriptors\nfrom bindings]
    BudgetExceeded{byte budget\nexceeded?}
    UTF8{descriptor\nUTF-8 OK?}
    Mode{rule.mode}

    subgraph SHM[" SHM zone (cross-process) "]
        direction LR
        Read[(Read window_total\nfrom AggregateTable)]
        Push[(Push QueueEvent\nto RequestQueue)]
    end

    Decide{total + 1 > limit?}
    Reject([429 Too Many Requests<br/>X-RateLimit-* + Retry-After])
    Exempt([Allow; bump exempted counter])
    Cardinality([400 Bad Request])
    Decline([NGX_DECLINED — flow continues])
    Commit([Allow; flow continues])

    Req --> Off
    Off -- yes --> Decline
    Off -- no --> Empty
    Empty -- yes --> Decline
    Empty -- no --> Loop
    Loop --> Pred
    Pred -- yes --> Exempt
    Pred -- no --> Build
    Build --> BudgetExceeded
    BudgetExceeded -- yes --> Cardinality
    BudgetExceeded -- no --> UTF8
    UTF8 -- no --> Decline
    UTF8 -- yes --> Mode
    Mode -- disabled --> Decline
    Mode -- enforce --> Read
    Mode -- dry_run --> Read
    Read --> Decide
    Decide -- yes, enforce --> Reject
    Decide -- yes, dry_run --> Push
    Decide -- no --> Push
    Push --> Commit

    style SHM fill:#fafafa,stroke:#666,stroke-dasharray: 4 4
```

A few things are worth reading off the picture. Both `gabion off`
and an empty `gabion_limit` set decline early without ever touching
SHM, which is why those branches are essentially free. The byte-budget
check is the only cardinality gate evaluated at request time; the
descriptor count and key length are bounded earlier, at config phase.
Finally, a dry-run rule still pushes its event onto the SHM queue, so
counters and gossip continue to see real traffic — only the reject
verdict itself is suppressed. A request is decided once: internal
redirects (`index`, `try_files`, `error_page`, named locations) don't
count it again, though a pass that counted nothing leaves the next
limited location to decide, as with `limit_req`. So an inherited
http-level `gabion_limit` that counts a request keeps a stricter rule in
a location it is redirected to from applying. Subrequests never count,
and gabion reserves the `$gabion_decided` variable name for this mark.

## Directive reference

The full directive surface is below. Every directive starts with
`gabion_` and is registered as an http-block directive unless noted.
Errors at `nginx -t` time use the shape *what happened, why it
likely happened, what to do next*; if you see one that doesn't end
with a concrete next step, please open an issue.

### Core

#### `gabion_limit_zone zone=NAME:SIZE` *(http)*

Allocates the shared-memory zone that holds the local-aggregate
counters, the SHM queue, and the per-zone `Stats`. Required exactly
once per `http {}` block that declares rules; `nginx -t` refuses rules
without one. Mirrors nginx core's `limit_req_zone zone=name:size`.

Like nginx's own zones, a zone survives `nginx -s reload` when its
name and size are unchanged: the node identity and any queued hits
carry over, and the log line `gabion: zone ready` says `reused=true`.
The counts do not: whenever a worker takes over gossip (after a reload,
a crash, or a lost lease), it empties the count table and peers gossip
the counts back, usually within a gossip tick or two, longer when the
cluster tracks many thousands of keys. Until then this node under-counts
and may admit requests over the limit. A node with no peers has nothing
to rebuild from, so a reload or leader change resets its limits as if
every window had just started. Changing the name or size starts an
empty zone, and the master releases the old one once no configuration
uses it. A reload that changes only `gabion_gossip_limit_queue_capacity`
cannot resize a reused zone's queue, so nginx refuses it and keeps the
previous configuration (`limit_queue_resized_on_reload`); change SIZE
with it, or restart nginx.

```nginx
gabion_limit_zone zone=api:128m;
```

SIZE sets how many `(rule, key, bucket)` counters the zone holds: after
the request queue (`gabion_gossip_limit_queue_capacity` × 72 bytes,
rounded up to a power of two: 576 KiB at the default) and nginx's slab
bookkeeping (under 1%), the rest becomes a power-of-two table of 64-byte slots, so each doubling of
SIZE doubles the slots (`2m` holds 16,384, `64m` holds 524,288; the
`gabion: zone ready` log line reports `slots`). A rule needs about
*active keys × (window / bucket + 1)* slots, and lookups stay short
while the table is under half full. For example, 650 keys a second under
`rate=20r/s window=10s bucket=1s` keep about 650 × 11 ≈ 7,200 cells
live, so choose 16,384 slots: `zone=api:2m`. A zone too small for the
queue and 1,024 slots fails `nginx -t` naming the smallest SIZE that fits;
`1m` fits the default queue capacity. When the table runs out of room, gabion allows the request,
under-counts that key, increments `aggregate_dropped` and logs
`aggregate_full` ([library troubleshooting](../gabion/TROUBLESHOOTING.md#aggregate_full)).

#### `gabion_limit_rule NAME [$bindings...] rate=Nr/<unit> [...]` *(http)*

Declares a rate-limit rule. Positional arguments after the name are
descriptor bindings; named arguments and bare flags follow.

```nginx
gabion_limit_rule per_ip     $remote_addr                  rate=100r/s;
gabion_limit_rule per_uri    $uri                          rate=10r/s;
gabion_limit_rule per_route  tenant:$arg_tenant path:$uri  rate=5r/s;

gabion_limit_rule by_asn     $geoip2_asn_number   rate=50r/s except_if=$trusted_ip;
gabion_limit_rule by_bot     class:$bot_class     rate=10r/s;
gabion_limit_rule shadow     $uri                 rate=60r/m dry_run;

gabion_limit_rule slow_path  $uri                 rate=5r/30s;
gabion_limit_rule smooth     $uri                 rate=5r/30s bucket=5s;
gabion_limit_rule daily_cap  tenant:$arg_tenant   rate=10000r/d;
gabion_limit_rule sustained  tenant:$arg_tenant   rate=10r/s window=5h bucket=1h;
gabion_limit_rule acme_cap   tenant:$arg_tenant   rate=100r/s value=tenant:acme;
```

**A rule's identity** is its domain, its bindings' keys and values, its
window and its bucket; the limit and the mode stay out, so changing either
keeps the rule's counts. Two rules with one identity would share counters,
so `nginx -t` refuses them; above, `per_uri`, `shadow`, `slow_path` and
`smooth` all key on `$uri` but each has its own window or bucket. A
`gabiond` rule with the same identity shares counts with the NGINX one in a
mixed cluster, whichever order either configuration lists them in.
[Writing the same rule for NGINX and gabiond](../gabion/README.md#writing-the-same-rule-for-nginx-and-gabiond)
gives every field in both spellings.

**Descriptor bindings.** A binding pairs a descriptor key with a
variable expression evaluated at request time:

| Form                              | Effect                                                         |
|-----------------------------------|----------------------------------------------------------------|
| `$identifier`                     | Auto-keyed by the variable name (`$uri` → key `uri`).          |
| `name:$identifier`                | Explicit key; single-variable.                                 |
| `name:"prefix-$foo-$bar"`         | Explicit key; template — compiled to a complex value.          |

Single-variable bindings dispatch through nginx's indexed-variable
subsystem, which is an O(1) array lookup with no allocation per
request. The inline fast path for `$uri`, `$request_uri`, `$args`,
`$remote_addr`, and `$arg_*` skips even the FFI hop. Template
bindings compile via `ngx_http_compile_complex_value` once at config
phase and allocate a few tens of bytes per evaluation against the
request pool, which is fine for operator-meaningful compositions
since they only pay for what they use.

**Named arguments.**

| Argument                | Meaning                                                                                                                                    |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `rate=Nr/<unit>`        | **Required.** `N` requests per the period named by `<unit>`. `<unit>` is `s\|m\|h\|d` or any humantime duration like `30s`, `5m`, `2h30m`. |
| `window=DURATION`       | Time horizon the rate is enforced over (default: the rate's period). When set, the resolved limit scales to `floor(rate_count * window / period)`. |
| `bucket=DURATION`       | Bucket granularity inside the window, no longer than the window (default: the resolved window — one fixed-window bucket).                  |
| `mode=enforce`          | Default. Evaluate and reject on overflow.                                                                                                  |
| `mode=dry_run`          | Evaluate and record the hit; never reject.                                                                                                 |
| `mode=disabled`         | Skip the rule entirely.                                                                                                                    |
| `dry_run`               | Bare flag; alias for `mode=dry_run`.                                                                                                       |
| `except_if=$variable`   | Skip the rule when `$variable` resolves truthy. See [Predicates](#predicates-except_ifvariable).                                           |
| `domain=NAME`           | Domain for the rule (defaults to `nginx`): 1 to 255 bytes with no whitespace or control characters, as in `gabiond`. Keys hash in it.        |
| `value=KEY:LITERAL`     | The binding keyed `KEY` matches only `LITERAL` (default `*`, any value); any other value declines the rule for that request, counting nothing. `KEY` must name one of the rule's bindings. |

**Rate, window, and bucket.** A rule resolves internally to three
numbers — a `limit`, a `window`, and a `bucket` — controlled by
three directive-level knobs that are evaluated in order:

* `rate=Nr/<unit>` (mandatory) — sustained allowance and its natural
  period.
* `window=DURATION` (optional) — widens the time horizon. The resolved
  limit is `floor(rate_count * window_millis / period_millis)`.
  Omitted, the window equals the rate's period.
* `bucket=DURATION` (optional) — granularity inside the window, no
  longer than the window. Omitted, the bucket equals the resolved
  window, which gives a single fixed-window bucket.

Worked example:

```nginx
gabion_limit_rule per_tenant tenant:$arg_tenant rate=10r/s window=5h bucket=1h;
```

resolves to `limit = 10 * 5 * 3600 = 180000`, `window = 5h`, and
`bucket = 1h`, leaving five live buckets. The same triple can be
written as `rate=180000r/5h bucket=1h` and the resolved enforcement
will be identical, but the original phrasing preserves the "10 r/s
applied over 5 hours" intent in the config text.

> **Rule of thumb.** If you set `window=` larger than the rate's
> period and don't also set `bucket=`, you get a *burstable* budget:
> clients can fire the whole window's allowance instantly and then
> sit empty for the rest of the window. For sustained-rate
> enforcement, set `bucket=` close to the rate's period — for
> example, `rate=10r/s window=5h bucket=1s` keeps the 180k 5-hour
> budget but smooths it to roughly 10 r/s.

A few related subtleties are worth knowing. `X-RateLimit-Limit`
reports the resolved number, so a rule written as
`rate=10r/s window=1h` returns `X-RateLimit-Limit: 36000`; clients
that see the header will read 36000, not 10. `Retry-After` scales
with the resolved window in the same way, which means that under
`window=5h` a rejected client may be told to wait up to five hours,
whereas without an explicit window the worst case is just the
rate's period. The floor in the limit calculation silently
under-budgets non-multiples — `rate=10r/m window=85s` resolves to
`limit=14` because the leftover 0.16 of a period vanishes — so if
you care about every last request, pick `window=` values that are
integer multiples of the rate's period. Finally, a `window=` shorter
than the rate's period is refused at `nginx -t` time, because a
window shorter than the rate's period has nothing to scale the rate
over; to enforce "100 in 500ms", write
`rate=100r/500ms` rather than `rate=200r/s window=500ms`.

#### `gabion_limit NAME [NAME ...]` *(http, server, location)*

Applies one or more rules at the current scope. See
[Composition: rule layering](#composition-rule-layering) for the
inheritance shape.

```nginx
gabion_limit per_ip per_tenant by_asn;
```

`gabion_limit off` locally suppresses all rules at this level
without disabling the module entirely.

#### `gabion on | off` *(http, server, location)*

`gabion off` disables the access handler for this scope so that
the request never incurs a rule lookup or an SHM read, leaving its
per-request cost at zero. `gabion on` re-enables the handler where
a parent had it off.

The two `off` modes are deliberately distinct. Both reach the same
access-phase outcome (allow); the difference is which directive an
operator reads as the intent. `gabion_limit off` says "this location
is in scope for gabion but has no active rules at this level".
`gabion off` says "this location is not in scope for gabion at all".

### Discovery

Without discovery, every node has to be told the address of every
peer at startup, and a rolling deploy breaks the list. The
EndpointSlice watch makes membership self-healing: gabion picks up
peer pods as they come and go, and there is no static list to
maintain.

| Directive                                     | Description                                                                                  |
|-----------------------------------------------|----------------------------------------------------------------------------------------------|
| `gabion_discovery_namespace_allow NAMESPACE`  | Restrict Kubernetes EndpointSlice discovery to the listed namespace. Repeat for several. Empty = the pod's own namespace. |
| `gabion_discovery_service_allow SERVICE`      | Restrict discovery to the listed Service name. Repeat for several. Empty = all services in the allowed namespaces. |
| `gabion_discovery_self_addr ADDR`             | Local gossip address that should be excluded from discovered peers (avoid talking to yourself). |

Each name must be a Kubernetes DNS label (`[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?`).

### Gossip — streams and the adaptive emit rate

Each node sends its changed counts straight to every peer of its zone, and
to one gateway in each other zone that relays them into its zone, each as
a stream the peer acknowledges; a gap report or a heartbeat without an ack
brings a resend, a peer silent for `gabion_gossip_suspect_after` is routed
around, and a starting node asks two peers for a join sweep of their live
counts. The defaults are sized for production loads.

- **Adaptive emit rate** — the gossip cadence adapts to per-rule
  pressure. Heartbeats fire every `gabion_gossip_tick_interval`;
  between heartbeats, the runtime fires a synthetic "threshold" tick
  whenever a hot rule's unreplicated local hits would exceed its
  per-site error budget. `gabion_gossip_target_err_bps` sets the
  budget; `gabion_gossip_min_emit_interval` caps the emit rate.

The math behind it lives in
[`crates/gabion/README.md#how-gossip-works`](../gabion/README.md#how-gossip-works).
Defaults come from `gabion::defaults` and apply identically to both
adapters; the canonical knob table lives in
[`crates/gabion/README.md#operator-knobs`](../gabion/README.md#operator-knobs).

**Gossip directives controlling the adaptive emit rate.**

| Directive                                   | Default | Adapts                                                       |
|---------------------------------------------|---------|--------------------------------------------------------------|
| `gabion_gossip_target_err_bps N`            | `100`   | **Adaptive emit rate** — per-rule unreplicated-error budget in basis points of the rule's limit (`100` = 1%). Lower = tighter accuracy, more emissions. |
| `gabion_gossip_min_emit_interval DURATION`  | `5ms`   | **Adaptive emit rate** — floor between threshold-fire emissions. Raise when the gossip channel itself is the bottleneck. |

**Gossip directives — transport plumbing.** These are not adaptive;
they bound resource use.

| Directive                                       | Default | Effect                                                                                |
|-------------------------------------------------|---------|---------------------------------------------------------------------------------------|
| `gabion_gossip_bind ADDR:PORT`                  | *(none)* | UDP bind for the gossip channel. Required for clustering; without it the node counts alone and runs no peer discovery. |
| `gabion_gossip_cluster ID`                      | `1`     | Cluster identifier: any non-zero u128, in decimal or `0x`-prefixed hex. |
| `gabion_gossip_tick_interval DURATION`          | `500ms` | Heartbeat period. Shorter = faster convergence, more UDP traffic.                     |
| `gabion_gossip_max_payload_bytes N`             | `1400`  | Cap on UDP payload size per gossip packet: 158 to 65,507. 158 bytes hold the header and one counter; 65,507 is the largest UDP payload over IPv4. |
| `gabion_gossip_suspect_after DURATION`          | `max(4 × tick, 2s)` | How long a packet to a peer may go unanswered before the peer is suspect: routed around as a gateway and sent only probes. At least two heartbeats. |
| `gabion_gossip_sweep_concurrency N`             | `4`     | Join sweeps this node answers at once, 1 to 255; each pages its live counts to one newly started peer. Past it a request is declined (`sweep_declined`) and the peer asks another. |
| `gabion_gossip_peer_capacity N`                 | `512`   | The most peers this node gossips with, 1 to 32,767, sized once at start, about 330 bytes a peer. Past it, a new peer is declined (`peer_table_full`) and its counts still merged. Also sets the node dictionary's default, twice it. |
| `gabion_gossip_limit_queue_capacity N`          | `8192`  | Slots in the shared-memory request queue workers push counted hits onto for the leader, 64 to 1,048,576, rounded up to a power of two. Each slot is 72 bytes of the `gabion_limit_zone` SIZE, so raise SIZE with it. The default covers a 1.3 s leader handover at up to 6,300 counted hits a second per pod; raise it when the `queue_dropped` counter rises. A reload that changes it needs a new SIZE or a restart. |
| `gabion_gossip_recv_buffer_bytes N`            | `4194304` | Receive buffer the gossip socket asks the kernel for, 65,536 to 1,073,741,823 bytes. The host's `net.core.rmem_max` caps it; `socket_buffer_capped` says when. See [Socket buffers](../gabion/README.md#socket-buffers). |
| `gabion_gossip_send_buffer_bytes N`            | `1048576` | Send buffer the gossip socket asks the kernel for, 65,536 to 1,073,741,823 bytes. The host's `net.core.wmem_max` caps it; `send_blocked` warns when a round fills it. |

### Storage — CRDT capacities

The CRDT cell store and dictionaries allocate exactly once, at leader
startup. These knobs size that allocation, so raising
one also raises the worker's RSS by roughly the named slot count
times the per-slot byte cost. The defaults are sized for production
clusters and you should only need to touch them if your workload runs
into the cap.

| Directive                                       | Default     | Effect                                                                                |
|-------------------------------------------------|-------------|---------------------------------------------------------------------------------------|
| `gabion_storage_max_cells N`                    | `131072`    | Total cell capacity in the CRDT.                                                      |
| `gabion_storage_rule_dictionary_capacity N`     | twice the rules, at least `256` | Number of distinct rule slots (each unique `rule_fingerprint` interns into one slot). Set it only on `rule_dictionary_full`; below the number of rules fails `nginx -t`. |
| `gabion_storage_node_dictionary_capacity N`     | twice `gabion_gossip_peer_capacity` (`1024`) | Number of distinct (node_id, incarnation) slots; must exceed `gabion_gossip_peer_capacity`. |
| `gabion_storage_max_descriptor_count N`         | `16`        | Cap on descriptors a single rule may declare, up to 65,535.                           |
| `gabion_storage_max_descriptor_bytes N`         | `512`       | Per-request budget on `(key + value)` bytes summed across descriptors plus the domain. Exceeded ⇒ 400. |
| `gabion_storage_max_key_bytes N`                | `128`       | Cap on a single descriptor key's length (validated at config phase).                  |
| `gabion_storage_max_matched_rules N`            | `16`        | Most rules one request is counted against; past it the request is allowed and the rest under-count (`matched_rule_overflows`). Each worker reserves 64 bytes per rule once, on its first request. |

### Runtime / identity

| Directive                          | Effect                                                                                                                                              |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `gabion_node_id_seed STRING`       | Non-empty string used to derive this node's stable identity (typically the pod name). Omitted, the identity is drawn fresh on each process start.    |

## Composition: rule layering

Each `gabion_limit` is an independent gate, so a request is allowed
only if **every** rule allows it and the first enforcing reject
wins. Reject headers pin to the rule with the longest window, which
prevents a client from seeing a short retry that puts it right back
into a 429 against a wider rule. Internal errors such as
matched-rule overflow or descriptor decode failures fall through to
allow, as described under the
[fail-open invariant](../../README.md#fail-open-invariant).

The inheritance shape mirrors nginx core's `limit_req`: **redeclaring
`gabion_limit` at a child level replaces the parent's set entirely**.
There is no append form, so a child that wants the parent's set plus
one more rule has to restate the parent's set explicitly.

A location's effective config is two independent axes — whether the
access handler runs at all (`gabion on|off`) and which rule set it
evaluates (`gabion_limit`). Each axis inherits from the enclosing
level when the child declares nothing, and each is resolved at the
child without consulting the other:

```mermaid
flowchart LR
    subgraph axisA["Access handler (gabion on|off)"]
        direction TB
        A1{child<br/>declares?}
        A1 -- "no decl" --> Ai[inherit from parent]
        A1 -- "`gabion on`" --> Aon[run access handler]
        A1 -- "`gabion off`" --> Aoff[skip access handler]
    end
    subgraph axisB["Rule set (gabion_limit)"]
        direction TB
        B1{child<br/>declares?}
        B1 -- "no decl" --> Bi[inherit parent's set verbatim]
        B1 -- "named rules" --> Bnamed[REPLACE parent's set]
        B1 -- "`gabion_limit off`" --> Boff[empty set — allow]
    end
```

Both axes resolve independently. `gabion off` skips the access handler
regardless of any `gabion_limit` at the same level, and conversely
`gabion_limit off` empties the rule set without stopping the handler,
which is useful when a later directive (for example, a future header
writer) needs the handler to remain in the chain.

The picture in code:

```nginx
server {
    gabion_limit per_ip per_tenant;        # baseline at server level

    location /api/         { /* no decl → inherits per_ip + per_tenant */ }
    location /api/upload   { gabion_limit upload_throttle; }                # replaces baseline
    location /api/internal { gabion_limit per_ip; }                         # narrows baseline
    location /api/billing  { gabion_limit per_ip per_tenant billing_throttle; }  # parent + one more
    location /api/healthz  { gabion_limit off; }                            # local opt-out
    location /static/      { gabion off; }                                  # skip handler
}
```

Replace-rather-than-append is deliberate: if silent inheritance were
combined with override semantics, a remote `gabion_limit` in some
parent block could quietly change behaviour inside an apparently
self-contained sub-location, which is exactly the kind of action at a
distance the directive shape is designed to rule out.

### First-class ASN / UA / IP-range limits and bypasses

Variable lookup, `except_if=`, and multi-rule stacking together let
you treat trusted crawlers specially in a single location:

```nginx
geo $trusted_ip { default 0; 127.0.0.1/32 1; }
map $http_user_agent $bot_class {
    default      other;
    ~*Googlebot  google;
    ~*bingbot    ms;
    ~*facebook   fb;
}

gabion_limit_rule per_ip  ip:$remote_addr     rate=50r/s except_if=$trusted_ip;
gabion_limit_rule per_bot class:$bot_class    rate=10r/s;
gabion_limit_rule per_uri $uri                rate=10r/s;

server { location /api/ { gabion_limit per_ip per_bot per_uri; } }
```

Trusted IPs bypass `per_ip` but are still gated against `per_bot`
and `per_uri`, so a misbehaving Googlebot will trip the 10 r/s
`per_bot` cap, and anything else that hammers a single endpoint will
hit the `per_uri` floor.

**Cardinality safety.** Don't key directly on `$http_user_agent`,
because every unique UA string becomes a distinct counter and the
descriptor space explodes. Map UAs through a small `map` block first
(the `$bot_class` recipe above is the pattern) so that the
cardinality is bounded by the number of classes you care about.
`$geoip2_asn_number` is safe to bind directly because it is already
bucketed to ASN numbers; the human-readable `$geoip2_asn`
organisation name is not, and should be bound through a map before
it reaches a rule.

## Predicates: `except_if=$variable`

`except_if=` names a single nginx variable. When the variable
resolves to a truthy value at request time, the rule is skipped.

**Truthy ≡ non-empty AND not in the case-insensitive falsy set
`{ "0", "false", "off", "no" }`.** Anything else — `1`, `true`,
`yes`, or any arbitrary non-empty string — means "exempt this rule".

```nginx
geo $trusted_ip {
    default 0;
    10.0.0.0/8 1;
}
gabion_limit_rule public_traffic $remote_addr rate=100r/s except_if=$trusted_ip;
```

A few semantics are worth knowing. Only one `except_if=` is permitted
per rule, and a rule that repeats the argument quietly keeps the
last one — to compose multiple bypass conditions, pre-combine them
in a `map` or `geo` block. Predicates never bill the cardinality
budget, because a truthy predicate exempts the request before the
byte-budget check ever runs. A missing predicate variable falls
through, so the rule applies as if the predicate were not there,
which is the same fail-open shape as a missing descriptor variable;
operator-typo protection runs at startup, because `nginx -t` rejects
a predicate that names a variable no loaded module provides.
Finally, exempted requests bump a separate SHM counter, `exempted`, so a
misconfigured always-true predicate is visible against the allow
counter.

### Combining multiple bypass conditions

To exempt on **any** of several conditions (logical OR), fold the
source variables with `map`:

```nginx
map $trusted_ip$is_admin $exempt {
    default   0;
    "~.*1.*"  1;
}
gabion_limit_rule public_traffic $remote_addr rate=100r/s except_if=$exempt;
```

To exempt only when **all** conditions fire (logical AND):

```nginx
map $trusted_ip:$is_admin $exempt {
    default 0;
    "1:1"   1;
}
gabion_limit_rule sensitive_route $uri rate=10r/s except_if=$exempt;
```

The pattern generalises: build whatever boolean expression you need
in `map` or `geo`, then pass the final variable to `except_if=`.
Pushing the composition into nginx core keeps gabion's per-request
work to a single variable lookup.

## Dry-run mode

```nginx
gabion_limit_rule canary $uri rate=10r/s dry_run;
gabion_limit per_ip canary;     # canary stacks with per_ip but never rejects
```

A dry-run rule evaluates the descriptor, reads the aggregate, and
records the hit, but it never produces a reject verdict. Because
counters and gossip continue to see real traffic, capacity planning
remains truthful before you flip the rule to `enforce`.

## Running across a cluster

Once you move past a single nginx box, gabion's whole value is the
shared counters, and the three pieces of cluster plumbing involved
are the same across both adapters — the
[root README](../../README.md#running-across-a-cluster) carries the
narrative. The nginx directives that bind each piece are:

| Piece                  | Directive                                                    |
|------------------------|--------------------------------------------------------------|
| Gossip socket          | `gabion_gossip_bind ADDR:PORT`                               |
| Cluster identifier     | `gabion_gossip_cluster ID` (non-zero u128, decimal or `0x` hex) |
| Discovery filter       | `gabion_discovery_namespace_allow`, `gabion_discovery_service_allow`, `gabion_discovery_self_addr` |

On Kubernetes, discovery also needs a Service port named `gabion` and a Role;
see the root README's
[Kubernetes checklist](../../README.md#kubernetes-checklist).

A complete cluster-side `http {}` block:

```nginx
http {
    gabion_limit_zone zone=api:128m;
    gabion_limit_rule per_ip $remote_addr rate=100r/s;

    gabion_gossip_bind 0.0.0.0:9000;
    gabion_gossip_cluster 0xc0ffee;

    gabion_discovery_namespace_allow my-app;
    gabion_discovery_service_allow   gabion-nginx;

    server { listen 8080; location / { gabion_limit per_ip; } }
}
```

Tuning the gossip cadence is rarely necessary, because the defaults
converge in under a second at production scale. When you do need to
tune,
[`crates/gabion/README.md#operator-knobs`](../gabion/README.md#operator-knobs)
has the canonical knob discussion and a table of effects, and the
matching nginx directives are listed under
[Gossip — the two adaptive aspects](#gossip--the-two-adaptive-aspects).

### Verifying convergence

There are two things worth checking after a deploy. The first is the
process logs: each worker logs the gossip bind, its node identity, and
the discovered-peer count. A line that reads
`gabion: worker did not win leader lease` means you are looking at a
follower, which is expected because only one worker drives gossip per
node; the worker that does logs `gabion: worker won leader lease`.
Followers keep campaigning for the lease. A worker stops leading and
campaigning as soon as nginx starts shutting it down gracefully
(`nginx -s quit`, or the old generation on a reload), even while it
drains connections, so a worker that's staying up takes over gossip
within about 600ms. After a crash, takeover waits for the lease TTL
(1s) to run out. The absence of any peer-discovery logs at all means
the discovery filter doesn't match anything. The second check is the
counter delta under load: send traffic to one replica and watch the
counters on every other replica rise within a tick or two. If they
don't, the gossip channel is partitioned — typical causes are a UDP
firewall or `gabion_gossip_bind` being unreachable between pods.

The nginx adapter does not currently expose an admin HTTP endpoint; its
SHM `Stats` counters (`requests`, `allowed`, `rejected`,
`rejected_cardinality`, `declines_invalid_descriptor`,
`matched_rule_overflows`, `exempted`, `queue_pushed`, `queue_drained`,
`queue_dropped`, `aggregate_dropped`) are read by the module's tests, and
the counters with a warning surface in the log. The server adapter exposes
its state via `/snapshot` (see [`../server/README.md`](../server/README.md)).

## Configuration error messages

Every `gabion_*` directive emits an operator-readable error at
`nginx -t` time when something is wrong, naming the file and line, with
the offending value quoted and the fix named. Every setting directive
(`gabion_gossip_*`, `gabion_storage_*`, `gabion_discovery_*`,
`gabion_node_id_seed`) is parsed and
validated by the gabion library's settings table, the same one
`gabiond` uses, so its errors name the setting in both adapters'
spellings: a zero count or duration, a value that does not parse, or a
value out of range fails `nginx -t` rather than being clamped. A rule
between settings (`gabion_gossip_min_emit_interval` below
`gabion_gossip_tick_interval`, the packet size and error-budget ranges)
is checked once the whole `http {}` block is read, so its error points
at the block's end.

Examples (verbatim, all from the current source):

```
gabion: `gabion_limit_zone` argument must start with `zone=` (e.g. `zone=api:128m`)
gabion: `gabion_limit_zone` zone name `nope!` must match `[A-Za-z0-9_]+` (matches nginx core's `limit_req_zone` grammar)
gabion: `gabion_limit_rule` rule `per_ip` has no `rate=`, so it has no limit to enforce. Add one (for example `rate=10r/s`), then run `nginx -t`
gabion: `gabion_limit_rule` argument `key=$uri` is invalid: expected `$variable`, `name:$variable`, or one of `rate=`, `window=`, `bucket=`, `mode=`, `dry_run`, `except_if=`, `domain=`, `value=`
gabion: `gabion_limit_rule` argument `rate=100r/fortnight` is invalid: rate period must be `s`, `m`, `h`, `d`, or a duration like `30s`, `5m`
gabion: Rule `inverted`: the window is shorter than the rate's period, so the rate cannot be spread over it. To enforce N requests in a shorter span, put the period in the rate itself (for example `100r/500ms`).
gabion: Two rules are named `per_ip`. Rule names must be unique; rename one.
gabion: `gabion_limit` references rule `tenant_api`, which is not declared via `gabion_limit_rule`
gabion: `gossip.cluster_id_hash` is `0`, which is not a non-zero cluster id in decimal or `0x`-prefixed hex, for example `0xc0ffee`. Correct it (`GABION_GOSSIP_CLUSTER_ID_HASH`; NGINX `gabion_gossip_cluster`) or remove it to use the default, 1.
gabion: rules are declared but `gabion_limit_zone` is not set, so no request would ever be counted or limited: the zone is the shared memory that holds the counts. Add `gabion_limit_zone zone=gabion:64m;` to the http {} block.
gabion: `gossip.tick_interval` is `notaduration`, which is not a duration in whole milliseconds, for example `500ms`. Correct it (`GABION_GOSSIP_TICK_INTERVAL`; NGINX `gabion_gossip_tick_interval`) or remove it to use the default, 500ms.
gabion: `storage.max_cells` is 0, but it must be at least 1: gabion could not track any rate-limit key. Remove the setting to use the default, 131072, or set a positive value (`GABION_STORAGE_MAX_CELLS`; NGINX `gabion_storage_max_cells`).
```

## Troubleshooting

Messages from the gabion library (gossip, discovery, the counter store) are
in [`../gabion/TROUBLESHOOTING.md`](../gabion/TROUBLESHOOTING.md); the entries
below are the module's own.

| Symptom                                                                       | What it means                                                                                                                                       | Fix                                                                                                                              |
|-------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| `nginx -t` says `unknown 'foo' variable`                                      | A `gabion_limit_rule` references a variable no loaded module defines.                                                                               | Load the providing module (`geoip2`, `map`, `geo`) before the `gabion_limit_rule` that references it.                            |
| `gabion_limit references rule X, which is not declared`                       | A `gabion_limit X;` names a rule with no `gabion_limit_rule X` declaration in the same `http {}` block.                                             | Add the missing declaration or fix the name.                                                                                     |
| `Two rules are named X`                                                       | Two `gabion_limit_rule X ...` directives with the same name. The grammar is unambiguous; almost always a copy-paste bug.                            | Pick distinct names.                                                                                                             |
| `Rules X and Y count the same keys with the same domain, descriptors, window and bucket` | Two rules share an identity, so their counters could not be told apart; they may differ only in limit or mode.                         | Give one a different binding, `window=` or `bucket=`, or merge them.                                                             |
| `` `value=K:…` in rule X names no binding of that rule ``                     | `value=` names a key none of the rule's bindings has.                                                                                               | Add a binding keyed `K`, or correct the key.                                                                                     |
| `gabion_limit_rule argument 'rate=0r/s' is invalid`                           | Zero rate; would deny all traffic.                                                                                                                  | Pick a non-zero positive integer. To temporarily disable a rule, use `mode=disabled`.                                            |
| `Rule X: the window is shorter than the rate's period`                           | `window=` was paired with a rate whose period is longer (e.g. `rate=200r/s window=500ms`); a window shorter than the rate's period has nothing to scale the rate over. | Move the period into the rate itself (for example `rate=100r/500ms`) instead of pairing a short window with a longer-period rate.       |
| `Rule X has N descriptors, above …`, `Rule X has descriptor key K of N bytes, above …`, `Rule X has descriptor key K, but keys must match …`, `Rule X has a domain …`, or `rule X: nginx could not compile binding …` | The rule's bindings exceed `gabion_storage_max_descriptor_count` or `gabion_storage_max_key_bytes`, a key or the domain breaks the grammar both adapters share, or nginx rejected a binding (an invalid template, or it ran out of configuration memory); the message says which. | Fix the rule (or raise the named directive), then run `nginx -t` to re-check. |
| `N rate-limit rules are configured, but storage.rule_dictionary_capacity … is M` | `gabion_storage_rule_dictionary_capacity` is set below the number of rules.                                                                     | Remove it to use the default (twice the rules, at least 256), or raise it.                                                       |
| `` gabion: `X` is 0, but it must be at least 1: … ``                           | A count, capacity or duration directive was set to 0, which would disable what it sizes.                                                            | Remove the directive to use the default the message names, or set a positive value.                                              |
| `` gabion: `X` is `V`, which is not …, for example `E`. … ``                   | The directive's argument does not parse: an integer in range, a duration in whole milliseconds (`500ms`), a socket address, a Kubernetes name, or a non-zero cluster id. | Write it like the example, or remove the directive to use the default.                                                           |
| `` gabion: `X` is N; gabion supports MIN to MAX. … ``                          | A capacity directive is outside the range gabion can hold; the message gives the reason.                                                            | Set it inside the range, or remove it.                                                                                           |
| `gabion: The gossip packet size …`, `… error budget …`, or `` … `gossip.min_emit_interval` … must be at least 1ms and shorter than … `` | A settings rule failed once the `http {}` block was read: the packet size outside 158 to 65,507, the error budget outside 1 to 10,000, or the emit floor at or above the heartbeat. | Follow the message; it names the directive and the range.                                                                        |
| `` gabion: `gabion_limit_zone` size N bytes is too small to hold gabion's request queue … `` | SIZE cannot hold the request queue (`gabion_gossip_limit_queue_capacity` × 72 bytes) and 1,024 counter slots.                              | Raise SIZE to at least the size the message names, or lower `gabion_gossip_limit_queue_capacity`.                               |
| `rules are declared but gabion_limit_zone is not set`                         | The config declares rules but no zone, so there is nowhere to count hits and every request would be admitted uncounted.                             | Add `gabion_limit_zone zone=gabion:64m;` to the `http {}` block, or check that the include declaring it is loaded.               |
| Responses include `X-RateLimit-Remaining: 0` and `429 Too Many Requests`      | A client crossed a rule's limit.                                                                                                                    | Expected. `Retry-After` says how long to back off; the rule with the longest window owns the header.                             |
| `400 Bad Request` from gabion                                                 | Pathological request: client supplied more descriptor bytes than `gabion_storage_max_descriptor_bytes` permits (`512` by default).                  | Either tighten the upstream client or raise `gabion_storage_max_descriptor_bytes` after sanity-checking why it's that large.     |
| SHM `matched_rule_overflows` counter rising                                   | Requests matched more allowed rules than `gabion_storage_max_matched_rules` (`16` by default). **The request was allowed** (allow-by-default); the rules past the bound under-count. *No log line — observed only via the SHM stats counters.* | Reduce the number of rules applied at this location, split the location, or raise `gabion_storage_max_matched_rules`.            |
| `The count table had no free slot for a rate-limit key` (warn, `aggregate_full`) | The zone holds too few slots for the live `(rule, key, bucket)` cells. **The request was allowed** and that key under-counted. Logged at powers of two of `aggregate_dropped`; see [`aggregate_full`](../gabion/TROUBLESHOOTING.md#aggregate_full). | Raise SIZE in `gabion_limit_zone` (see its sizing notes); each doubling doubles the slots.                                         |
| `gabion: this worker stopped leading gossip` (error) in the error log         | The leader's gossip runtime or queue drain failed or panicked, or it lost the lease. The worker released the lease and campaigns again after `retry_in` (doubling, up to 30s); another worker may take over first. Admission keeps running against the shared aggregate. | Read the `error` field. A bind error means `gabion_gossip_bind` is already in use; a lost lease means the worker was starved long enough to miss its renewals. |
| `gabion: leader thread panicked` in the error log                             | Logged as a worker exits: its leader thread panicked outside a leadership term, so the worker stopped campaigning. Other workers still campaign and take over gossip. | The `panic` field holds the panic message. Report it as a gabion bug, with earlier log lines.                                                                                |
| `Peer discovery hit an error and skipped an update` (warn) in the error log   | `discovery_error`, a library message: see [`../gabion/TROUBLESHOOTING.md`](../gabion/TROUBLESHOOTING.md#discovery_error).                           | Usually transient. If it keeps repeating, check pod RBAC and network reachability to the API.                                    |

### `leader_takeover`

**Level** info, once per takeover · **Request allowed** yes, and may briefly over-admit · **Emitted by** NGINX module only

Message starts: "gabion: this worker took over gossip and emptied the previous leader's counts"

**What happened.** This worker became the gossip leader and emptied the shared count table the previous leader left behind, so peers' gossip can refill it without counting anything twice. Until then, usually a gossip tick or two, this node under-counts and may admit requests over their limits; a node with no peers instead under-counts until each rule's window has passed.

**Why.** The previous leader exited (a reload, `nginx -s quit`, or a crash) and this worker won the lease.

**What to do next.** Nothing, unless it lasts more than a few gossip ticks. If it does, check that this node receives gossip from its peers ("Verifying convergence" above).

**Fields** `cleared` · **Knob** none

### `limit_queue_resized_on_reload`

**Level** error, once per refused reload · **Request allowed** yes, the previous configuration keeps serving · **Emitted by** NGINX module only

Message starts: "gabion: this reload changes `gabion_gossip_limit_queue_capacity`"

**What happened.** nginx refused the reload and kept the previous configuration, including its rules. The reload changed the queue capacity, but the `gabion_limit_zone` name and size stayed the same, so nginx reused the running zone, whose queue the running workers still use and cannot resize.

**Why.** nginx maps a fresh shared-memory zone only when a zone's name or size changes.

**What to do next.** Change SIZE in `gabion_limit_zone` along with `gabion_gossip_limit_queue_capacity` (a larger queue needs a larger SIZE anyway, 72 bytes a slot), so nginx maps a fresh zone, or restart nginx.

**Fields** `running`, `configured` · **Knob** `gabion_gossip_limit_queue_capacity`, `gabion_limit_zone`

Operator-facing errors are written to answer three questions: *what
happened*, *why it likely happened*, and *what to do next*. If you
see one that doesn't end with a concrete next step, please open an
issue.

## Unknown variable detection

A typo in a `gabion_limit_rule` binding source — `except_if=$tursted_ip`
when you meant `$trusted_ip` — fails `nginx -t` *before* any worker
accepts a single request. The detection happens inside nginx core:
when gabion encounters `$identifier` at config phase, it calls
`ngx_http_get_variable_index`, which declares the dependency. After
every module's `postconfiguration` callback, nginx walks the
declared dependencies and rejects the config if any variable has no
provider.

Gabion deliberately does not layer a second validator on top, because
the core check is already exhaustive and the resulting error message
(`unknown 'tursted_ip' variable`) is operator-clear on its own. Make
sure the module that provides the variable — `ngx_http_geoip2_module`,
or the `map` directive that defines `$bot_class`, or the `geo`
directive that defines `$trusted_ip` — is loaded before the
`gabion_limit_rule` that references it.
