# gabion

The pure-Rust library at the heart of every gabion deployment. It owns
the CRDT, the gossip runtime, the wire codec, the rule machinery, and the
discovery interface, while knowing nothing about YAML, gRPC, or nginx.
Both adapters in the workspace (`gabion-server` and `gabion-nginx`) build
their configuration on top of the same library types and call the same
hot paths. This README is also the crate's rustdoc front page.

Read this README for what an operator sets and why: the knobs, the sizing,
zones, writing one rule for both adapters, and upgrading. The protocol
itself, two tiers of gossip and the wire format byte by byte, is in
[`GOSSIP.md`](GOSSIP.md); the counter store behind it in
[`CRDT.md`](CRDT.md); every log message the library writes, and what to do
about it, in [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md). For cross-adapter
vocabulary (rule, descriptor, bucket, zone, gateway, fail-open), see the
[repo root README](../../README.md#glossary).

## Contents

- [Module map](#module-map)
- [Embedding the runtime](#embedding-the-runtime)
- [How a change travels](#how-a-change-travels)
- [Writing the same rule for NGINX and gabiond](#writing-the-same-rule-for-nginx-and-gabiond)
- [Zones](#zones)
  - [Outside Kubernetes](#outside-kubernetes)
  - [Testing zones in kind](#testing-zones-in-kind)
- [Operator knobs](#operator-knobs)
  - [Common](#common)
  - [Advanced](#advanced)
  - [Peer table](#peer-table)
  - [Socket buffers](#socket-buffers)
- [Sizing](#sizing)
- [Upgrading](#upgrading)
- [What we measured](#what-we-measured)

## Module map

Every type that crosses an adapter boundary lives in exactly one of the
library's modules. The dependency graph is
a DAG, so no module imports from a module that imports it.

| Module       | Path                          | What it owns                                                                                                                              |
|--------------|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `crdt`       | `src/crdt/*`                  | The per-origin counter store. Robin Hood hash index, structure-of-arrays columns, the ordered own-stream list. Allocates only at construction. See [`CRDT.md`](CRDT.md). |
| `rules`      | `src/rules/*`                 | `RuleDef::build` and `RuleTable::new`, the one way both adapters build rules; `Descriptor`, fingerprint and key hashing. Two nodes with the same rules produce identical `rule_fingerprint` values and key hashes, whatever order they list them in, which is what makes the CRDT addressable across the cluster. |
| `aggregate`  | `src/aggregate.rs`            | The cluster-aggregate table both adapters read on admission: seqlocked slots, linear probing within 128 slots of home, backward-shift removal. NGINX lays it over shared memory; `gabiond` owns one as `Aggregate`, sized from `storage.max_cells`. |
| `window`     | `src/window.rs`               | The sliding-window header math both adapters share: the time until a request would be admitted, `Retry-After` and the reset seconds. |
| `gossip`     | `src/gossip/*`                | `GossipRuntime`, `GossipClient`, `GossipTransport`, `Clock`, the `sim::SimTransport` used in tests, the pre-sized peer table, and the `AdminCommand` / `AdminSnapshot` observability surface. See [`GOSSIP.md`](GOSSIP.md). |
| `wire`       | `src/wire/*`                  | The on-the-wire codec. One UDP packet = header + rule dictionary + node dictionary + cell rows. Each packet decodes independently. See [`GOSSIP.md`](GOSSIP.md#wire-format-version-2). |
| `discovery`  | `src/discovery/*`             | `Membership` (Kubernetes, a static list, or alone), whose `peer_events` both adapters feed the runtime; the `PeerDiscovery` trait and the Kubernetes EndpointSlice implementation, which finds the node's own entry by `SelfMatch` and reports its zone as `PeerEvent::OwnEntry`. The gossip runtime treats peer events as a normal `select!` arm. |
| `zone`       | `src/zone.rs`                 | `ZoneName` and the 4-byte `ZoneHash` every header carries, and `ZoneStatus`: the node's zone and its source, or why none is known. |
| `defaults`   | `src/defaults.rs`             | Constants shared by both adapters, each documented with its unit, the memory it sizes and the message that names it: tick interval, payload caps, the per-rule error budget, the min-emit floor, and the stream and sweep constants (gap reports, sweep sources, the backlog that sizes the per-round quantum). |
| `settings`   | `src/settings.rs`             | Every operator setting named once: `gabiond` YAML key, environment variable and NGINX directive. Messages interpolate from it so each names both adapters' spellings. |
| `throttle`   | `src/throttle.rs`             | The one power-of-two log throttle every repeating warning counts through. |
| `identity`   | `src/identity.rs`             | This host's stable node id, and an incarnation that differs on every start (wall-clock milliseconds, stepped past the previous one). Behind `transport-udp`. |

`gossip` sits at the top of that graph and depends on every other
module, whereas `wire` and `crdt` are independent of each other.
`defaults`, `settings` and `throttle` are leaves: the other modules import
them, and they import nothing else in this crate. Adapters consume the public surface of
`gossip`, `rules`, and `discovery`, together with the constants in
`defaults`.

## Embedding the runtime

Both adapters do what this example does: validate the settings once, build
the rules, the counter store, the aggregate table that admission reads and a
gossip runtime, then decide each request against the table and record what it
admits. The example runs two nodes on the in-memory `SimTransport`, as the
tests do; a production node binds UDP with `GossipRuntime::bind` instead.
`tests/readme.rs` runs this code, and keeps this copy equal to it.

```rust,ignore
let router = SimRouter::new();
let [a, b]: [SocketAddr; 2] =
    ["10.0.0.1:9000", "10.0.0.2:9000"].map(|addr| addr.parse().expect("an address"));
// One node: its settings, its rule, its counter store, the aggregate
// table admission reads, and its gossip runtime.
let start = |id: u128, me: SocketAddr, peer: SocketAddr| {
    let settings = Settings {
        gossip_bind: Some(me),
        bootstrap_peers: Box::new([peer]),
        ..Settings::default()
    };
    let (gossip, storage) = settings.validate().expect("valid settings");
    let rule = RuleDef {
        name: "per_tenant",
        domain: "api",
        patterns: &[("tenant", "*")],
        rate: "100r/m",
        window: None,
        bucket: None,
        mode: EnforcementMode::Enforce,
    }
    .build(&storage.cardinality)
    .expect("valid rule");
    let rules = RuleTable::new(Box::new([rule]), storage.rule_dictionary_capacity())
        .expect("the rules fit");
    let store = CellStore::new(storage, NodeIdentity::new(NodeId(id), 1), &rules);
    let counts = Rc::new(Aggregate::new(&storage));
    let (runtime, client) = GossipRuntime::from_parts(
        router.bind(me),
        TokioClock::from_millis(0),
        gossip,
        store,
        Rc::clone(&counts),
    );
    tokio::task::spawn_local(runtime.run(futures::stream::empty()));
    (rules, counts, client)
};
let (rules, a_counts, a_client) = start(1, a, b);
let (_, b_counts, _b_client) = start(2, b, a);

// Admission on node A: read the cluster total, decide, record the hit.
let spec = rules.rules()[0].spec();
let key = hash_key(
    "api",
    [Descriptor {
        key: "tenant",
        value: "acme",
    }],
);
let now = 0;
if a_counts.table().window_total(spec, key, now) < spec.limit {
    let bucket = spec.bucket_at(now);
    a_client
        .record(spec.fingerprint, key, bucket, 1, spec.limit, now)
        .await
        .expect("the runtime is running");
}
// Gossip carries the hit to node B within a heartbeat.
sim_advance_ticks(Duration::from_millis(10), 100).await;
assert_eq!(b_counts.table().window_total(spec, key, now), 1);
```

## How a change travels

A hit admitted on one node changes one counter cell of that node. Gossip
sends the change in two tiers: straight to every other node of the same
availability zone, and to one *gateway* in each other zone, which relays it to
the rest of its zone. So each change crosses each zone boundary once, while a
flat mesh would send it across once per remote node. Every stream is
acknowledged: a lost packet is resent on the next round, and a node that
stops answering is routed around within a few seconds. A node of no zone
gets every change directly, so counts stay correct without zones; only the
cross-zone saving is lost.

What that costs, measured in the study for the design this release ships
(95 % credible intervals; [What we measured](#what-we-measured)):

| Regime | Bytes sent per node | Of which cross-zone | Convergence p99 |
|---|---|---|---|
| `prod-typical`, 80 pods in 3 zones | 88.1 KB/s [87.7, 88.4] | 2.6 KB/s | 337 ms [307, 367] |
| `prod-peak`, 183 pods in 3 zones | 236.6 KB/s [236.0, 237.1] | 3.0 KB/s | 301 ms [292, 309] |

The previous design sent 790 KB/s per node across zones at `prod-peak`. How
gateways are chosen, how failures are detected and the wire format are in
[`GOSSIP.md`](GOSSIP.md).

## Writing the same rule for NGINX and gabiond

NGINX nodes and `gabiond` nodes in one cluster count a rule together when
both adapters write it the same way: the same domain, the same descriptor keys
and value patterns in the same order, and the same window and bucket. Those
fields are the rule's identity, hashed into the fingerprint every counter is
filed under, so neither the rule's name nor its position in either
configuration matters. The rate's count and the mode are not part of the
identity: changing either keeps a rule's counts. The rate's period is, when
`window` is omitted, because it sets the default window and through it the
default bucket: `rate=10r/s` and `rate=600r/m` without a window are two rules.
Write the rate and mode the same in both adapters too, because each adapter
enforces its own against the shared count.

| Field | `gabiond` YAML (`limits:` entry) | NGINX `gabion_limit_rule` argument | When omitted | Identity |
|---|---|---|---|---|
| name | `name: api_tenant` | first argument | required | no; unique within each configuration |
| domain | `domain: nginx` | `domain=nginx` | `gabiond`: required; NGINX: `nginx` | yes |
| descriptors, in order | `descriptors: [{key: uri}, {key: tenant}]` | one binding per descriptor: `$uri tenant:$arg_t` (a bare `$uri` is keyed `uri`; `key:$source` names the key) | required, at least one | yes: keys and their order |
| value pattern | `value: acme` on a descriptor | `value=tenant:acme` | `*`, any value | yes |
| rate | `rate: 100r/10s` | `rate=100r/10s` | required | count: no; period: yes when `window` is omitted |
| window | `window: 10s` | `window=10s` | the rate's period | yes |
| bucket | `bucket: 1s` | `bucket=1s` | the window | yes |
| mode | `mode: dry_run` (`enforce`, `dry_run`, `disabled`) | `mode=dry_run`, or the flag `dry_run` | `enforce` | no |

Both adapters compute the limit as ⌊rate count × window ÷ rate period⌋.
These two rules are one rule:

```yaml
# gabiond
limits:
  - name: api_tenant
    domain: nginx
    descriptors:
      - key: uri
      - key: tenant
    rate: 100r/10s
    bucket: 1s
```

```nginx
# NGINX
gabion_limit_rule api_tenant $uri tenant:$arg_t rate=100r/10s bucket=1s;
```

and so are these, which count only the tenant `acme`:

```yaml
  - name: acme_only
    domain: nginx
    descriptors:
      - key: tenant
        value: acme
    rate: 10r/s
```

```nginx
gabion_limit_rule acme_only tenant:$arg_t rate=10r/s value=tenant:acme;
```

Rules that differ in any identity field, even only in `bucket`, are
different rules: each adapter counts its own, so a mixed cluster admits up to
the limit through each. gabion cannot tell such a pair from two unrelated
rules, so if a mixed cluster admits more than a rule allows, compare the two
spellings field by field against the table.

**Which requests reach a rule is not part of it.** NGINX selects them with
the location's `gabion_limit`, `except_if=$var` and the variable each binding
reads; Envoy with the route's rate-limit actions, which decide the
descriptors a request carries. Requests share a counter only when both
adapters hand gabion the same bytes for each value: NGINX's `$uri` is the
normalized path without the query string, whereas an Envoy action that reads
the `:path` header sends the raw path with its query.

## Zones

A node learns its own zone from, in order:

1. **The zone setting**, `gossip.zone` (`GABION_GOSSIP_ZONE`; NGINX
   `gabion_gossip_zone`): 1 to 63 letters, digits, `-`, `_` or `.`,
   starting and ending with a letter or digit, like a Kubernetes label
   value. An empty value fails to load rather than meaning "unset".
2. **Its own Kubernetes EndpointSlice entry.** Kubernetes copies each
   node's `topology.kubernetes.io/zone` label into the `zone` of every
   endpoint on it. Discovery finds the node's own entry by the pod name,
   which Kubernetes makes the pod's host name, or by `discovery.self_addr`
   when that is set. A `hostNetwork: true` pod's host name is its node's,
   so set `discovery.self_addr` to the pod's IP and gossip port there (for
   gabiond, `GABION_DISCOVERY_SELF_ADDR: "$(POD_IP):9000"` with `POD_IP`
   from the downward API). The own entry is read before the readiness
   filter, since a starting pod is often not ready yet, and it is never a
   peer, so pods do not gossip with themselves.
3. **None.** The node treats every peer as a zone peer and sends every
   change directly: counts stay correct, but none of the cross-zone traffic
   the gateways save is saved.

Once known, the zone does not change for the life of the process; a later
entry naming another zone is ignored. A zone learned from the entry after
start makes peers recompute their roles and the node ask for a join sweep.
When the setting and the entry disagree, the setting wins and the node logs
`zone_conflict` once.

Each node says what it found: `zone_known` once its zone is known, with its
source, and `zone_unknown` when it is not. On Kubernetes `zone_unknown`
waits `gossip.zone_grace` (`GABION_GOSSIP_ZONE_GRACE`; NGINX
`gabion_gossip_zone_grace`, default 10 s) after the first peer event, since
the EndpointSlice controller usually writes a starting pod's entry a moment
after it starts; the kind smokes print that wait, and the default stays at
least five times it. With a static peer list and no zone setting it fires at
start; a node with no peers never logs it. The same grace bounds how long a
join sweep waits for unheard peers before a new zone's first node asks a
peer of another zone. Each code has an entry in
[`TROUBLESHOOTING.md`](TROUBLESHOOTING.md).

`gabiond`'s `/snapshot` serves the node's zone as
`"zone": {"name": "use1-az2", "source": "endpoint_slice"}` (`source` is
`config` or `endpoint_slice`), or for an unknown zone
`"zone": {"name": null, "source": "none", "reason": "own_entry_not_found"}`,
where `reason` is `not_configured`, `own_entry_not_found` or
`own_entry_has_no_zone`. To check a Kubernetes cluster's zones:

```sh
kubectl get nodes -L topology.kubernetes.io/zone
kubectl get endpointslices -n <namespace> -l kubernetes.io/service-name=<service> \
  -o jsonpath='{range .items[*].endpoints[*]}{.targetRef.name}{"\t"}{.zone}{"\n"}{end}'
```

### Outside Kubernetes

A cluster outside Kubernetes needs `gossip.bind`, `gossip.bootstrap_peers`
and `gossip.zone` on every node. `gossip.bootstrap_peers`
(`GABION_GOSSIP_BOOTSTRAP_PEERS`, comma-separated; a YAML list; NGINX
`gabion_gossip_bootstrap_peers`, repeated once per peer) lists IP addresses
and ports; host names are not resolved. A non-empty list replaces
Kubernetes discovery, so the watcher never starts. One list may name every
node, the node itself included: a node drops the entry its own first packet
comes back from. That relies on the kernel delivering the node's packet to
itself from the listed address; a list that names a node by another of its
addresses (behind NAT, say) leaves that entry unanswered. Static peers are
never removed: a listed node that is down is resent to on the usual
schedule, and the list may hold no more peers than `gossip.peer_capacity`.

```sh
export GABION_GOSSIP_BIND=0.0.0.0:9090
export GABION_GOSSIP_BOOTSTRAP_PEERS=192.0.2.11:9090,198.51.100.12:9090,203.0.113.13:9090
export GABION_GOSSIP_ZONE="$ZONE"   # from one of the recipes below
exec gabiond /etc/gabion/gabiond.yaml
```

NGINX cannot read environment variables in directives, so the container's
entrypoint writes a file the `http {}` block includes:

```sh
printf 'gabion_gossip_zone %s;\n' "$ZONE" > /etc/nginx/gabion-zone.conf
```

```nginx
http {
    gabion_gossip_bind 0.0.0.0:9000;
    gabion_gossip_bootstrap_peers 192.0.2.11:9000;
    gabion_gossip_bootstrap_peers 198.51.100.12:9000;
    gabion_gossip_bootstrap_peers 203.0.113.13:9000;
    include /etc/nginx/gabion-zone.conf;
    # ...
}
```

These commands read the zone from cloud metadata; gabion does not run them
itself. Each prints one zone identifier. Skip the export when it prints
nothing, because an empty zone fails to load.

| Platform | Command | Note |
|---|---|---|
| AWS EC2 (IMDSv2) | `T=$(curl -sX PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60'); curl -sH "X-aws-ec2-metadata-token: $T" http://169.254.169.254/latest/meta-data/placement/availability-zone-id` | Use the zone **ID** (`use1-az2`), not the zone name: AWS maps names to physical zones differently in each account. A container needs an IMDS hop limit of 2. |
| Google Cloud | `Z=$(curl -sH 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone); echo "${Z##*/}"` | The endpoint returns `projects/<n>/zones/<zone>`; keep the last segment. |
| Azure | `curl -sH Metadata:true 'http://169.254.169.254/metadata/instance/compute/zone?api-version=2021-02-01&format=text'` | Prints `1`, `2` or `3`, or nothing for a VM outside zones. Zone numbers are consistent within one subscription only. |
| AWS ECS | `curl -s "$ECS_CONTAINER_METADATA_URI_V4/task" \| jq -r .AvailabilityZone` | Prints the zone **name**, consistent within one account. On EC2 capacity, the IMDS recipe above gives the zone ID instead. |

### Testing zones in kind

`make kubernetes-zones-test` runs `deploy/kubernetes/zones-gossip.sh`
against a kind cluster of three zones, `kind-a`, `kind-b` and `kind-c`, one
worker each. Create it with a kubeconfig of its own, so your current
context is left alone, and point the target at it:

```sh
kind create cluster --name gabion-zones --config deploy/kubernetes/kind-zones.yaml \
  --kubeconfig /tmp/gabion-zones.kubeconfig
KUBECONFIG=/tmp/gabion-zones.kubeconfig make kubernetes-zones-test
```

It runs six `gabiond` replicas, two per zone, and checks through each pod's
`/snapshot` that every pod learned its zone from its EndpointSlice entry,
that each has one zone peer and one gateway in each other zone, that hits
through a `kind-a` pod reject at the shared limit through a `kind-c` pod,
and that both still hold after one pod per zone is replaced and after a
gateway is deleted. [`deploy/kubernetes/README.md`](../../deploy/kubernetes/README.md)
lists every smoke.

## Operator knobs

Every setting has a default that works at `prod-peak` (183 pods) with
headroom. The *common* settings are the ones most deployments set; the
*advanced* ones are each listed with the log message that calls for them, so
you reach one from a log line rather than by browsing. Each row gives the
setting in all three spellings: the `gabiond` YAML key (also the `config_key`
field of every message), its environment variable, and the NGINX directive
(`none` for a `gabiond`-only setting). Memory is per node.

`gabion::settings` is the one table both adapters name, parse and validate
every setting through, and `gabion::defaults` holds the defaults, so a
changed default or bound moves both adapters together, and a bad value fails
configuration load with the same message in both. The adapter references,
[`gabiond`'s](../server/README.md#configuration-reference) and
[NGINX's](../nginx/README.md#directive-reference), give each adapter's
syntax.

### Common

| Setting | Environment variable | NGINX directive | Default | Memory | What it sets; when to change it |
|---|---|---|---|---|---|
| `gossip.bind` | `GABION_GOSSIP_BIND` | `gabion_gossip_bind` | unset | — | The UDP address this node gossips on. `gabiond` needs it; an NGINX node without it counts alone. |
| `gossip.bootstrap_peers` | `GABION_GOSSIP_BOOTSTRAP_PEERS` | `gabion_gossip_bootstrap_peers` | unset: Kubernetes discovery | — | Static peers, `ip:port`, for a cluster outside Kubernetes; a non-empty list replaces discovery ([Outside Kubernetes](#outside-kubernetes)). |
| `gossip.zone` | `GABION_GOSSIP_ZONE` | `gabion_gossip_zone` | unset: from the node's EndpointSlice entry | — | This node's availability zone. Set it outside Kubernetes, or when `zone_unknown` says the entry has none ([Zones](#zones)). |
| `discovery.namespace_allow` | `GABION_DISCOVERY_NAMESPACE_ALLOW` | `gabion_discovery_namespace_allow` | unset: the pod's own namespace | — | The namespaces Kubernetes discovery watches. |
| `discovery.service_allow` | `GABION_DISCOVERY_SERVICE_ALLOW` | `gabion_discovery_service_allow` | unset: every Service with a UDP port named `gabion` | — | The Services discovery reads peers from. |
| `discovery.self_addr` | `GABION_DISCOVERY_SELF_ADDR` | `gabion_discovery_self_addr` | unset: the entry is found by pod name | — | This pod's IP and gossip port; set it only for `hostNetwork` pods, whose host name is not their pod name. |
| `envoy_bind` | `GABION_ENVOY_BIND` | none | unset: no gRPC listener | — | Where Envoy reaches `gabiond`'s rate-limit service. |
| `admin_bind` | `GABION_ADMIN_BIND` | none | unset: no admin endpoint | — | `gabiond`'s `/snapshot` and `/readyz`; set it for a readiness probe. |
| `gossip.cluster_id_hash` | `GABION_GOSSIP_CLUSTER_ID_HASH` | `gabion_gossip_cluster` | `1` | — | A non-zero id every node of one cluster shares; a node drops gossip carrying another. A label, not a secret. Set your own; `default_cluster_id` says when a node runs with the default. |
| `runtime.node_id_seed` | `GABION_RUNTIME_NODE_ID_SEED` | `gabion_node_id_seed` | unset: the host name | — | The text this node's stable id is hashed from; the host name, else the outbound IP address, else a per-process seed. |
| `gossip.peer_capacity` | `GABION_GOSSIP_PEER_CAPACITY` | `gabion_gossip_peer_capacity` | `512` | about 330 B a peer | The most peers a node gossips with, 1 to 32,767. Raise it past your largest pod count; `peer_table_full` names it. |
| `storage.max_cells` | `GABION_STORAGE_MAX_CELLS` | `gabion_storage_max_cells` | `131072` | about 96 B a cell; `gabiond` adds 128 B for its aggregate table | Counter cells the store holds ([Sizing](#sizing)). Raise it on `cell_store_full` or, in `gabiond`, `aggregate_full`. |
| `gossip.tick_interval` | `GABION_GOSSIP_TICK_INTERVAL` | `gabion_gossip_tick_interval` | `500ms` | — | The heartbeat, also the retransmission timeout. It sets the convergence tail of a quiet rule; `setting_warning` warns below 20 ms, near a cross-zone round trip. |
| `gossip.target_err_bps` | `GABION_GOSSIP_TARGET_ERR_BPS` | `gabion_gossip_target_err_bps` | `100` | — | Per-rule error budget in basis points of the rule's limit, 1 to 10,000: 100 bounds the cluster's unreplicated hits per rule at 1 % of its limit. Lower for tighter limits, higher to send less. |
| `storage.max_descriptor_count` | `GABION_STORAGE_MAX_DESCRIPTOR_COUNT` | `gabion_storage_max_descriptor_count` | `16` | — | Descriptors one rule may declare, up to 65,535; a longer rule fails to load. |
| `storage.max_descriptor_bytes` | `GABION_STORAGE_MAX_DESCRIPTOR_BYTES` | `gabion_storage_max_descriptor_bytes` | `512` | — | Bytes of domain, keys and values one descriptor may carry (in NGINX, one matched rule's bindings); past it that descriptor is rejected as `cardinality_rejected` (NGINX answers 400). |
| `storage.max_key_bytes` | `GABION_STORAGE_MAX_KEY_BYTES` | `gabion_storage_max_key_bytes` | `128` | — | Bytes in one descriptor key: a longer key fails to load, or rejects the request (`cardinality_rejected`). |
| `storage.max_request_descriptors` | `GABION_STORAGE_MAX_REQUEST_DESCRIPTORS` | none | `16` | — | Descriptors of one Envoy request `gabiond` evaluates; the rest are rejected as `cardinality_rejected`. |

### Advanced

| Setting | Environment variable | NGINX directive | Default | Memory | Change it when |
|---|---|---|---|---|---|
| `gossip.recv_buffer_bytes` | `GABION_GOSSIP_RECV_BUFFER_BYTES` | `gabion_gossip_recv_buffer_bytes` | `4194304` | kernel memory, up to twice the grant while full | `socket_buffer_capped` says the host granted less: raise `net.core.rmem_max`, or set this to the grant ([Socket buffers](#socket-buffers)). |
| `gossip.send_buffer_bytes` | `GABION_GOSSIP_SEND_BUFFER_BYTES` | `gabion_gossip_send_buffer_bytes` | `1048576` | kernel memory, up to twice the grant while full | `socket_buffer_capped`, or `send_blocked` repeats. |
| `gossip.limit_queue_capacity` | `GABION_GOSSIP_LIMIT_QUEUE_CAPACITY` | `gabion_gossip_limit_queue_capacity` | `8192` | NGINX: 72 B a slot of the limit zone, rounded up to a power of two | `queue_dropped` (NGINX) or `request_queue_full` (`gabiond`): counted hits were dropped. |
| `storage.node_dictionary_capacity` | `GABION_STORAGE_NODE_DICTIONARY_CAPACITY` | `gabion_storage_node_dictionary_capacity` | twice `gossip.peer_capacity` | — | `node_dictionary_full`: more peer instances live at once than it holds, as with long windows and frequent rollouts. |
| `storage.rule_dictionary_capacity` | `GABION_STORAGE_RULE_DICTIONARY_CAPACITY` | `gabion_storage_rule_dictionary_capacity` | max(256, 2 × configured rules) | — | `rule_dictionary_full`: more distinct rules live across the cluster than it holds. |
| `storage.max_matched_rules` | `GABION_STORAGE_MAX_MATCHED_RULES` | `gabion_storage_max_matched_rules` | `16` | 32 B a rule per `gabiond` runtime thread, 64 B per NGINX worker | `matched_rule_overflow`: a request matched more rules than are counted. |
| `gossip.suspect_after` | `GABION_GOSSIP_SUSPECT_AFTER` | `gabion_gossip_suspect_after` | `max(4 × tick_interval, 2s)` | — | `gateway_failover` repeats on a lossy network or with long pauses: raise it. At least two heartbeats. |
| `gossip.sweep_concurrency` | `GABION_GOSSIP_SWEEP_CONCURRENCY` | `gabion_gossip_sweep_concurrency` | `4` | — | `sweep_declined` outside rollouts. |
| `gossip.zone_grace` | `GABION_GOSSIP_ZONE_GRACE` | `gabion_gossip_zone_grace` | `10s` | — | `zone_unknown` on Kubernetes while the pod's EndpointSlice entry is merely slow to appear. |
| `gossip.max_payload_bytes` | `GABION_GOSSIP_MAX_PAYLOAD_BYTES` | `gabion_gossip_max_payload_bytes` | `1400` | — | `decode_rejected` with `reason = "peer_limits"`: nodes disagree on it. Set one value on every node, 158 to 65,507, below your path MTU less 28 bytes. |
| `gossip.min_emit_interval` | `GABION_GOSSIP_MIN_EMIT_INTERVAL` | `gabion_gossip_min_emit_interval` | `5ms` | — | `/snapshot`'s `rounds_threshold`, `rounds_prompt` and `packets_sent` climb far above normal under a flood of hits: this floor caps a node's packet rate ([Sizing](#sizing)); doubling it halves the ceiling's main terms. |

### Peer table

`gossip.peer_capacity` (`GABION_GOSSIP_PEER_CAPACITY`; NGINX
`gabion_gossip_peer_capacity`, default 512) sizes the table of peers a node
gossips with, once, at start. A peer discovery reports past it is declined,
logged as `peer_table_full`, and still has its counts merged. It also sets the
node dictionary's default, twice the peer table, which
`storage.node_dictionary_capacity` overrides.

### Socket buffers

The kernel's socket buffers are gossip's queues. `gossip.recv_buffer_bytes`
(`GABION_GOSSIP_RECV_BUFFER_BYTES`; NGINX `gabion_gossip_recv_buffer_bytes`,
default 4 MiB) absorbs bursts and pauses such as CPU throttling;
`gossip.send_buffer_bytes` (`GABION_GOSSIP_SEND_BUFFER_BYTES`; NGINX
`gabion_gossip_send_buffer_bytes`, default 1 MiB) holds one round's packets.
Linux grants at most `net.core.rmem_max` and `net.core.wmem_max` (212,992 bytes
by default) and reports double what it grants; gabion halves the report, so
every size it logs is the grant itself. When it grants less than asked,
gabion logs `socket_buffer_capped` once per buffer at start; an NGINX node with
no `gabion_gossip_bind` asks for the smallest buffers and never logs it. Raise
the host settings in the node's bootstrap; in Kubernetes they are node settings,
not pod sysctls:

```sh
sysctl -w net.core.rmem_max=4194304 net.core.wmem_max=1048576
```

A host that cannot change sets the two settings to the granted sizes, which
silences the warning. `send_blocked` means the send buffer filled during a
round: the round stopped there, and the next round sends the refused packet
first, then the rest.

## Sizing

These equations are the design's model of a node. The packet equation
predicts the study's measured packets and bytes within 1.3 % at 80, 183 and
300 pods; the rest size the defaults with the headroom shown. `N` is the
pods in the cluster, `Z` the zones, `T` the heartbeat (`gossip.tick_interval`)
and `Λ` the hits per second the whole cluster admits.

**Cells.** A cell counts one key's hits in one bucket from one origin node.
A rule with window `W` and bucket `b` keeps `W / b + 1` buckets live, so

```text
cells ≈ Σ over rules: keys × nodes that count each key × (W / b + 1)
      ≤ Λ · (W + b)              (every hit opens a new cell)
```

`prod-peak` keeps about 20,000 cells live; the default `storage.max_cells`,
131,072, is 6.5 times that. The NGINX limit zone's count table holds one slot
per live `(rule, key, bucket)`, origins merged (its sizing notes are under
[`gabion_limit_zone`](../nginx/README.md#gabion_limit_zone-zonenamesize-http)).

**Memory.** At the defaults a node holds:

| What | Size at the defaults | Scales with |
|---|---|---|
| Counter store | about 12 MiB (about 96 B a cell) | `storage.max_cells` |
| `gabiond`'s aggregate table | 16 MiB (64 B a slot, twice the cells rounded up to a power of two) | `storage.max_cells` |
| NGINX's count table and request queue | the limit zone's SIZE, in shared memory | `gabion_limit_zone` |
| Peer table | about 165 KiB (about 330 B a peer) | `gossip.peer_capacity` |
| Kernel socket buffers | up to 8 MiB receive and 2 MiB send on Linux, only while they hold data | the two buffer settings |

**Packets.** A node runs `R_own` own rounds a second: with the default error
budget a rule's budget is one hit for any limit below about 36,000 at 183
pods, so a node flushes at every second hit and the heartbeat flushes a lone
one. Each own round sends one packet to every other node, directly or through
a gateway's relay, so a node sends

```text
D = R_own · (N − 1) + a          packets a second, about Λ / 2
```

where `a` counts acknowledgements sent alone (15 to 29 a second at
production sizes). Packets follow the cluster's admitted rate, not the pod
count. `gossip.min_emit_interval` caps them under any load at

```text
D_max = (1/F + 1/T)(n_z − 1 + Z − 1) + min(1/F, g/F)(n_z − 1) + (N − 1)/T
```

with `F` the floor, `n_z` the pods of the node's zone and `g` the origins it
is a gateway for (about `Z − 1`): 24,900 packets a second per node at 183
pods and 40,800 at 300. A node reaches the ceiling only when it admits 400
hits a second, 40 times `prod-peak`; past it packets stop growing and carry
more cells each.

**Socket buffers.** The kernel's buffers are gossip's queues. For a pause of
`t_s` (100 ms, the default CFS period a CPU-throttled pod can lose) and the
kernel's charge `s_q` per queued datagram (about 2 KiB),

```text
B_rcv = h · D · t_s · s_q
B_snd = h · Q_round · s_q,       Q_round = 2 (n_z − 1) + (Z − 1) + g + sweep pages
```

with `h` the headroom. At `prod-peak` the need is about 210 KB to receive and
254 KB to send; the defaults (4 MiB and 1 MiB requested, twice that granted on
Linux) give 40 and 8 times that. A bigger cluster raises the settings and the
host's `net.core.rmem_max` / `net.core.wmem_max` together
([Socket buffers](#socket-buffers)).

## Upgrading

Run one gabion release on every node of a cluster, both adapters included:
they share the wire format, the rule identity and the settings table.

### To wire format version 2

This release changes the gossip wire format to version 2 (the header and
body are in [`GOSSIP.md`](GOSSIP.md#wire-format-version-2)). Version 1 and
version 2 nodes reject each other's packets and log `peer_version_mismatch`,
so during a rolling upgrade the cluster splits into two islands that do not
share counts, and each enforces its limits against its own share of the
traffic: up to about twice a key's limit per window until the rollout ends.
Rule fingerprints and key hashes changed in the same release, so no count
carries across the upgrade either way.

1. Before the rollout, give every node a zone (the Kubernetes
   `topology.kubernetes.io/zone` label, or `gossip.zone` outside Kubernetes)
   and raise the hosts' `net.core.rmem_max` and `net.core.wmem_max`
   ([Socket buffers](#socket-buffers)).
2. Roll fast: a high `maxSurge` and `maxUnavailable` keep the split short.
3. Watch `/snapshot`'s `peers_by_role.incompatible` (`gabiond`) or the
   `incompatible_peers` field of `peer_version_mismatch` fall to 0.

A rollback is the same split in reverse, and as short as you make it.

### Mixed NGINX and gabiond clusters

NGINX and `gabiond` nodes count together when they share:

- **the release**: upgrade both adapters in the same rollout, since they
  share the wire version;
- **the cluster id**: `gossip.cluster_id_hash` in `gabiond`, `gabion_gossip_cluster`
  in NGINX;
- **the packet size**: the same `gossip.max_payload_bytes` /
  `gabion_gossip_max_payload_bytes` everywhere;
- **each rule's identity**: the same domain, descriptor keys and value
  patterns in order, window and bucket, by the table in
  [Writing the same rule for NGINX and gabiond](#writing-the-same-rule-for-nginx-and-gabiond).
  NGINX's default domain is `nginx`, so a `gabiond` twin of an NGINX rule that
  sets no `domain=` names `domain: nginx`.

A rule written differently in the two adapters is two rules, each counted on
its own, and the cluster admits up to its limit through each.

### Removed settings

These settings are gone. A YAML file or `nginx.conf` that still sets one fails
to load naming it; delete the line. `gabiond` reads only the environment
variables of current settings, so it ignores a removed one without a word:
delete those from the Deployment too, or they will mislead the next reader.

| Removed (YAML key / environment variable / NGINX directive) | What replaces it |
|---|---|
| `gossip.send_queue_capacity` / `GABION_GOSSIP_SEND_QUEUE_CAPACITY` / `gabion_gossip_send_queue_capacity` | Nothing: the kernel send buffer queues packets, sized by `gossip.send_buffer_bytes`. |
| `storage.peer_capacity` / `GABION_STORAGE_PEER_CAPACITY` / `gabion_storage_peer_capacity` | `gossip.peer_capacity` (default 512). |
| `gossip.max_cells_per_frame` / `GABION_GOSSIP_MAX_CELLS_PER_FRAME` / `gabion_gossip_max_cells_per_frame` | Nothing: derived from `gossip.max_payload_bytes`. |
| `storage.local_dirty_capacity` / `GABION_STORAGE_LOCAL_DIRTY_CAPACITY` / `gabion_storage_local_dirty_capacity` | Nothing: each peer's stream tracks what it has been sent. |
| `storage.forwarded_dirty_capacity` / `GABION_STORAGE_FORWARDED_DIRTY_CAPACITY` / `gabion_storage_forwarded_dirty_capacity` | Nothing: the relay stream tracks what each zone peer has been sent. |
| `runtime.rng_seed` / `GABION_RUNTIME_RNG_SEED` / `gabion_runtime_rng_seed` | Nothing: gateways are chosen by rendezvous hashing, with no randomness. |
| `gossip.fanout` / `GABION_GOSSIP_FANOUT` / `gabion_gossip_fanout` | Nothing: every zone peer gets each change, and one gateway per remote zone. |
| `gossip.max_cells_per_tick` / `GABION_GOSSIP_MAX_CELLS_PER_TICK` / `gabion_gossip_max_cells_per_tick` | Nothing: the per-round quantum is derived from `storage.max_cells`. |

### `/snapshot` changes

For scrapers of `gabiond`'s `/snapshot`:

- `identity` is `local_identity`; `store.cell_store` is `store_stats`;
  `store.aggregate_rows` is `aggregate_rows`.
- `decode_reject_count` is replaced by `counters.decode_rejects_framing`,
  `decode_rejects_peer_limits`, `decode_rejects_version` and
  `decode_rejects_cluster`.
- Removed: `gossip.send_pending_depth`, `local_dirty_len`,
  `forwarded_dirty_len`, `ticks_total`, `threshold_fires`, `dirty_ticks`,
  `last_effective_fanout`, `peak_effective_fanout` and `peers[].peer_slot`.
- Added: `zone`, `peers_by_role`, the `counters` object (rounds, packets,
  bytes, cross-zone bytes, merges, rewinds, gap reports, failovers, stalls,
  sweeps), and on each peer `zone`, `role`, `answer`, `unanswered_ms`,
  `own_unacked` and `relay_unacked`.
- `/debug/introspection` is gone; `/snapshot` and `/readyz` remain
  ([admin endpoint](../server/README.md#admin-endpoint)).

## What we measured

The study ran each gossip design in a deterministic simulator of a gabion
cluster under production-shaped load, three seeds per regime, and scored
every node against the true count over a 60-second measurement window. The
design this release ships was measured as the study's `imp-az-hier-nopk`
arm; `main` is the design it replaces. Each figure is the posterior mean with
its 95 % credible interval; bytes are per node, the cluster's total divided
by pods and the window.

| Regime | Design | Convergence p50 | Convergence p99 | Never converged | Bytes sent per node | Cross-zone per node | Mean over-admission |
|---|---|---|---|---|---|---|---|
| `prod-typical`, 80 pods | this release | 25 ms [23, 28] | 337 ms [307, 367] | none in 3 seeds | 88.1 KB/s [87.7, 88.4] | 2.6 KB/s [2.6, 2.6] | 1.1 % [0.2, 1.9] |
| `prod-typical`, 80 pods | `main` | 4.8 s [4.73, 4.82] | 6.2 s [6.03, 6.44] | none in 3 seeds | 1,044 KB/s [1,037, 1,050] | 708 KB/s [697, 718] | 41 % [36, 45] |
| `prod-peak`, 183 pods | this release | 20 ms [15, 26] | 301 ms [292, 309] | none in 3 seeds | 236.6 KB/s [236.0, 237.1] | 3.0 KB/s [3.0, 3.0] | 1.0 % [0.6, 1.4] |
| `prod-peak`, 183 pods | `main` | 8.5 s [8.48, 8.56] | 9.7 s [9.63, 9.73] | 10.7 % [10.2, 11.3] | 1,180 KB/s [1,177, 1,182] | 790 KB/s [785, 795] | 86 % [75, 97] |

Convergence is how long a hit takes to reach every node's count, with hits
that never arrived counted at the end of the run; over-admission is how far
the cluster admits past a limit, on keys that reach it, as a share of the
limit. These were measured
on the measured branch's version 1 wire; this release's version 2 header and
cells are smaller (the design predicts about 7 % fewer bytes), which
`make sim-gate` measures for any checkout. The method, every regime and the
full results are in [`docs/study/README.md`](../../docs/study/README.md); the
harness that produced them is [`crates/gossip-bench`](../gossip-bench/README.md).
