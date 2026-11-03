# Troubleshooting gabion's library messages

Find an entry by the `code` field of the log line. These messages come from
the gabion library, so they appear under both adapters. Messages only one
adapter emits are in that adapter's own Troubleshooting section:
[`gabiond`](../server/README.md#troubleshooting) and
[NGINX](../nginx/README.md#troubleshooting). Each entry names its knob in both
adapters' spellings.

### `cell_store_full`

**Level** warn, power of two · **Request allowed** yes, not counted · **Emitted by** library (both adapters)

Message starts: "Too many distinct rate-limit keys are being tracked at once."

**What happened.** The counter store had no free cell for a new rate-limit key. The request was allowed and the key is not counted until older time buckets expire, so its limit under-counts.

**Why.** Usually many clients, an attack spraying keys, or a store smaller than the number of keys live within the rules' windows (keys × live buckets).

**What to do next.** Raise `storage.max_cells` (`GABION_STORAGE_MAX_CELLS`) in `gabiond`, or `gabion_storage_max_cells` in NGINX. If one rule's descriptors take unbounded client input, narrow what it keys on.

**Fields** `config_key`, `capacity`, `in_use`, `rejected_total` · **Knob** `storage.max_cells` (`GABION_STORAGE_MAX_CELLS`) / `gabion_storage_max_cells`

### `rule_dictionary_full`

**Level** warn, power of two · **Request allowed** yes, the rule does not enforce on this node · **Emitted by** library (both adapters)

Message starts: "A rate-limit rule this node does not configure, gossiped by a peer or recorded by a worker still on an earlier configuration, found the rule dictionary full."

**What happened.** A rate-limit rule this node does not configure, gossiped by a peer or recorded by a worker still on an earlier configuration, found the rule dictionary full, so this node drops its hits and that rule under-counts across the cluster.

**Why.** Usually more rules are live across the cluster than the dictionary holds, for example during a configuration rollout that briefly runs old and new rule sets together. A rule this node does not configure holds its slot until its last cell expires, 60 s after that cell's last update.

**What to do next.** The default is twice the configured rules, at least 256. Set `storage.rule_dictionary_capacity` (`GABION_STORAGE_RULE_DICTIONARY_CAPACITY`) in `gabiond`, or `gabion_storage_rule_dictionary_capacity` in NGINX, above the number of distinct rules alive in the cluster at once, and only above that default.

**Fields** `config_key`, `capacity`, `in_use`, `rule_fingerprint`, `rejected_total` · **Knob** `storage.rule_dictionary_capacity` (`GABION_STORAGE_RULE_DICTIONARY_CAPACITY`) / `gabion_storage_rule_dictionary_capacity`

### `aggregate_full`

**Level** warn, power of two · **Request allowed** yes, that key not counted · **Emitted by** library (both adapters)

Message starts: "The count table had no free slot for a rate-limit key"

**What happened.** The aggregate table both adapters read on admission had no free slot near a key's home, so that key's hits were not counted: its requests were allowed and it is under-counted.

**Why.** The table needs a slot per rule × active key × live bucket (window / bucket + 1), and it is too small for the current traffic; lookups stay short only while it is under half full.

**What to do next.** Raise `storage.max_cells` (`GABION_STORAGE_MAX_CELLS`) in `gabiond`, whose table holds twice that many slots rounded up to a power of two, or SIZE in `gabion_limit_zone zone=NAME:SIZE` in NGINX.

**Fields** `config_key`, `capacity`, `dropped_total` · **Knob** `storage.max_cells` (`GABION_STORAGE_MAX_CELLS`) / SIZE in `gabion_limit_zone`

### `node_dictionary_full`

**Level** warn, power of two · **Request allowed** yes, some peers' counts dropped · **Emitted by** library (both adapters)

Message starts: "Too many gabion peer instances are being tracked"

**What happened.** The node dictionary had no free slot for a peer instance. Counts from some peers are being dropped, so cluster-wide limits under-count the requests those peers handled.

**Why.** Every peer restart counts as a new instance until its data ages out, so a large cluster with frequent restarts can outgrow the dictionary.

**What to do next.** Raise `storage.node_dictionary_capacity` (`GABION_STORAGE_NODE_DICTIONARY_CAPACITY`) in `gabiond`, or `gabion_storage_node_dictionary_capacity` in NGINX. Size it above pods × (1 + restarts within your longest rule window). The default is twice `gossip.peer_capacity`.

**Fields** `config_key`, `capacity`, `in_use`, `peer_node_id`, `peer_incarnation`, `rejected_total` · **Knob** `storage.node_dictionary_capacity` (`GABION_STORAGE_NODE_DICTIONARY_CAPACITY`) / `gabion_storage_node_dictionary_capacity`

### `peer_table_full`

**Level** warn, power of two · **Request allowed** yes, under-counted at the declined peer · **Emitted by** library (both adapters)

Message starts: "This node's peer table is full"

**What happened.** Discovery reported a peer after the table of peers this node gossips with was full, so the node ignored it. It sends that peer none of its counts, so limits enforced at that peer under-count this node's traffic and allow requests over the limit. Counts that peer sends here are still merged.

**Why.** The cluster has more pods than `gossip.peer_capacity`.

**What to do next.** Raise `gossip.peer_capacity` (`GABION_GOSSIP_PEER_CAPACITY`) in `gabiond`, or `gabion_gossip_peer_capacity` in NGINX, above the cluster's pod count. Each slot costs about 330 bytes.

**Fields** `config_key`, `capacity`, `peer`, `declined_total` · **Knob** `gossip.peer_capacity` (`GABION_GOSSIP_PEER_CAPACITY`) / `gabion_gossip_peer_capacity`

### `peer_joined`

**Level** info, once per peer added · **Request allowed** yes, counts unaffected · **Emitted by** library (both adapters)

Message starts: "Peer joined the cluster."

**What happened.** Discovery, or the static peer list at start, reported a new peer, and this node added it to its peer table. It now gossips with it and shares its counts.

**Why.** A pod started, or discovery saw its EndpointSlice entry become ready. During a rollout every new pod logs one line on every node.

**What to do next.** Nothing. `cluster_size` is the peer table's size after the change; compare it with your pod count less one to check that discovery sees every pod.

**Fields** `peer`, `cluster_size` · **Knob** none

### `peer_left`

**Level** info, once per peer removed · **Request allowed** yes, counts unaffected · **Emitted by** library (both adapters)

Message starts: "Peer left the cluster."

**What happened.** Discovery reported that a peer is gone, and this node removed it from its peer table. Its counts already merged stay until their buckets expire.

**Why.** A pod stopped or was rescheduled, or its EndpointSlice entry stopped being ready. Normal during rollouts and scale-downs; static peers are never removed.

**What to do next.** Nothing, unless peers leave and rejoin while their pods keep running: then look for `discovery_error` lines and failing readiness probes.

**Fields** `peer`, `cluster_size` · **Knob** none

### `gap_abandoned`

**Level** warn, power of two · **Request allowed** yes, under-counted until the missing changes arrive · **Emitted by** library (both adapters)

Message starts: "Stopped asking"

**What happened.** A peer's own or relay stream (`stream`) to this node skipped some changes, and this node asked the peer to resend them `reports` times over `waited_ms` without receiving them, so it stopped asking. If the peer still sends the stream, its next packet makes this node ask again, and its retransmission timer resends the missing changes on its own; until they arrive, limits here under-count the traffic that peer handled. Counted in `/snapshot` as `counters.gaps_abandoned`.

**Why.** Usually UDP from that peer to this node is blocked or lossy in one direction, so the peer never hears the reports or its resends never arrive.

**What to do next.** If it repeats for a peer that is running, check the NetworkPolicies and security groups between the two nodes for UDP on the gossip port, in both directions.

**Fields** `peer`, `stream`, `missing`, `reports`, `waited_ms`, `abandoned_total`

### `sweep_declined`

**Level** warn, power of two · **Request allowed** yes, the new node under-counts until it catches up · **Emitted by** library (both adapters)

Message starts: "Declined a catch-up request from"

**What happened.** A newly started peer asked this node for its live counts (a join sweep) while this node was already sending them to `gossip.sweep_concurrency` other new nodes, so it declined. The peer asks again at its next heartbeat and then asks another peer, so it most likely still catches up. Counted in `/snapshot` as `counters.sweeps_refused`.

**Why.** Many pods started at once: a rollout or a scale-out.

**What to do next.** Nothing, unless new pods keep under-counting for longer than one rate-limit window; then slow the rollout. Raising `gossip.sweep_concurrency` (`GABION_GOSSIP_SWEEP_CONCURRENCY`) in `gabiond`, or `gabion_gossip_sweep_concurrency` in NGINX, reduces these refusals; each extra sweep adds its own burst of packets to every prompt round while it runs.

**Fields** `config_key`, `peer`, `active`, `limit`, `refused_total` · **Knob** `gossip.sweep_concurrency` (`GABION_GOSSIP_SWEEP_CONCURRENCY`) / `gabion_gossip_sweep_concurrency`

### `decode_rejected`

**Level** warn, power of two of each `reason` · **Request allowed** yes, the packet's counts are lost · **Emitted by** library (both adapters)

Each `reason` has its own counter in `/snapshot` (`counters.decode_rejects_framing`, `decode_rejects_peer_limits`, `decode_rejects_cluster`) and its own power-of-two throttle, so a flood of one cause cannot hide another. Another protocol version is [`peer_version_mismatch`](#peer_version_mismatch).

`reason = "framing"`. Message starts: "Dropped a packet from this peer that is not valid gabion gossip."

- **What happened.** A packet on the gossip port was not a valid gossip packet (the `error` field says why) and was dropped. Counts from healthy peers are not affected.
- **Why.** Usually another program is sending UDP to this node's gossip port, or something on the path truncates packets.
- **What to do next.** If `peer` is a gabion node, check the path MTU between the two: gossip packets are at most `max_payload_bytes` bytes.

`reason = "peer_limits"`. Message starts: "Dropped a packet from this peer larger than this node accepts."

- **What happened.** A packet of `length` bytes arrived, more than this node's `max_payload_bytes`, and was dropped, so limits here under-count the traffic that peer handled.
- **Why.** The peer has a higher `gossip.max_payload_bytes`, or something on the path changes packet sizes. A non-gabion sender of large UDP packets also lands here, since the size is checked first.
- **What to do next.** Set `gossip.max_payload_bytes` to the same value on every node.

`reason = "cluster"`. Message starts: "Ignored gossip from this peer: it belongs to another gabion cluster"

- **What happened.** The packet carried another cluster id (`peer_cluster`) than this node's (`local_cluster`), so this node dropped it before reading its counts.
- **Why.** Two gabion clusters share a network and gossip port.
- **What to do next.** If they are one cluster, set the same `gossip.cluster_id_hash` on every node. If they are separate, also give them different gossip ports or networks.

**Fields** `peer`, `error`, `reason`, `rejected_total` (per reason); `max_payload_bytes` and `config_key` (framing, peer_limits); `length` (peer_limits); `peer_cluster`, `local_cluster`, `config_key` (cluster) · **Knob** `gossip.max_payload_bytes` (`GABION_GOSSIP_MAX_PAYLOAD_BYTES`) / `gabion_gossip_max_payload_bytes`; `gossip.cluster_id_hash` (`GABION_GOSSIP_CLUSTER_ID_HASH`) / `gabion_gossip_cluster`

### `peer_version_mismatch`

**Level** warn, power of two · **Request allowed** yes, the two versions do not share counts · **Emitted by** library (both adapters)

Message starts: "Ignoring gossip from this peer: it speaks another gossip protocol version"

**What happened.** A peer sent gossip in another protocol version (`peer_version`) than this node's (`local_version`). Neither merges the other's counts, so each version enforces limits on its own share of the traffic, and the cluster can admit up to about twice a key's limit per window. Requests are allowed.

**Why.** During a rolling upgrade between those versions this is expected and stops once every pod runs the same release. If it continues after the rollout ends, a pod is still on the other release.

**What to do next.** Finish the rollout quickly, or roll back. Otherwise find the pod by the `peer` field and upgrade or restart it. `counters.decode_rejects_version` in `/snapshot` counts the dropped packets.

**Fields** `peer`, `peer_version`, `local_version`, `reason` (`version`), `rejected_total`, `incompatible_peers` (peers on another version now; it falls to zero as the rollout ends) · **Knob** none

### `default_cluster_id`

**Level** info, once at start (NGINX: when a worker starts leading gossip on `gabion_gossip_bind`) · **Request allowed** yes · **Emitted by** library (both adapters)

Message starts: "This node uses the default gossip cluster id, 1."

**What happened.** `gossip.cluster_id_hash` is left at its default. Nothing is wrong yet.

**Why.** Any other gabion cluster left at the default on the same network and gossip port would merge counts with this one.

**What to do next.** Set `gossip.cluster_id_hash` to a value unique to this cluster, the same on every node of it.

**Fields** `cluster_id_hash`, `config_key` · **Knob** `gossip.cluster_id_hash` (`GABION_GOSSIP_CLUSTER_ID_HASH`) / `gabion_gossip_cluster`

### `send_blocked`

**Level** warn, power of two · **Request allowed** yes, counts reach peers late · **Emitted by** library (both adapters)

Message starts: "The kernel's send buffer for the gossip socket was full"

**What happened.** The kernel refused a gossip packet (`EAGAIN`, or `ENOBUFS`), so the round stopped there. The next round sends that packet first, then the rest. Until then, limits on other nodes under-count and may allow requests over the limit.

**Why.** Usually the send buffer is smaller than gabion asked for (look for an earlier `socket_buffer_capped` naming `net.core.wmem_max`), or the host's network is saturated.

**What to do next.** Raise `net.core.wmem_max` on the host to at least `requested_send_buffer_bytes` (see "Socket buffers" in the [gabion README](README.md#socket-buffers)), and keep `gossip.send_buffer_bytes` at or above what one round sends, raising both together when `granted_send_buffer_bytes` already matches the request.

**Fields** `send_blocked_total`, `requested_send_buffer_bytes`, `granted_send_buffer_bytes`, `sysctl`, `config_key` · **Knob** `gossip.send_buffer_bytes` (`GABION_GOSSIP_SEND_BUFFER_BYTES`) / `gabion_gossip_send_buffer_bytes`

### `socket_buffer_capped`

**Level** warn, once per buffer at start · **Request allowed** yes · **Emitted by** library (both adapters)

Message starts: "The kernel gave the gossip socket a"

**What happened.** The kernel granted the gossip socket a smaller receive or send buffer than gabion asked for. gabion keeps running; if the node pauses or a burst arrives, packets that do not fit are dropped and their counts arrive a round or more late.

**Why.** The host setting named in `sysctl` (`net.core.rmem_max` or `net.core.wmem_max`, 212,992 bytes by default on Linux) caps every socket's buffer.

**What to do next.** Raise the sysctl on the host to at least `requested_bytes` (in Kubernetes a node setting, not a pod sysctl). If the host cannot change, set the setting named in `config_key` to `granted_bytes` to accept the smaller buffer and silence the warning.

**Fields** `buffer`, `requested_bytes`, `granted_bytes` (the grant itself; gabion halves the double Linux reports), `sysctl`, `config_key` · **Knob** `gossip.recv_buffer_bytes` (`GABION_GOSSIP_RECV_BUFFER_BYTES`) / `gabion_gossip_recv_buffer_bytes`; `gossip.send_buffer_bytes` (`GABION_GOSSIP_SEND_BUFFER_BYTES`) / `gabion_gossip_send_buffer_bytes`

### `send_error`

**Level** warn, power of two · **Request allowed** yes, that peer under-counts · **Emitted by** library (both adapters)

Message starts: "Could not send a gossip packet to this peer"

**What happened.** A send to one peer failed with an error other than a full buffer. gabion dropped that packet and kept gossiping with every other peer; the peer gets the same counts on a later round once sends succeed, and until then its limits under-count.

**Why.** Usually the peer has just left the cluster (a stale address), or a firewall or NetworkPolicy blocks UDP from this node to it.

**What to do next.** If it repeats for a peer that is running, check the policy and the route between the two nodes. If the peer is this node's gateway into its zone and sends to it keep failing, another member of that zone takes over after `gossip.suspect_after`, as for a peer that does not answer.

**Fields** `peer`, `error`, `send_errors_total` · **Knob** none

### `gateway_failover`

**Level** info for the first, then warn at powers of two · **Request allowed** yes, under-counted in that zone for a few seconds · **Emitted by** library (both adapters)

Message starts: "Switched this node's gateway for zone" (info), "This node has switched gateways" (warn)

**What happened.** This node sends its changes into each other zone through one member of it, its gateway there. That gateway answered none of this node's packets for `gossip.suspect_after` (`reason = "unanswered"`), or said it cannot reach most of its own zone (`reason = "cannot_relay"`), so the next member of that zone took over and was sent every live change this node holds; nothing is lost. Until it relays them, limits in that zone under-count this node's traffic, for up to `gossip.suspect_after` plus three heartbeats.

**Why.** The gateway restarted or left the cluster, or UDP between this node and that zone drops packets; with `cannot_relay`, the gateway's path into its own zone is broken. A pod that discovery has just placed in that zone can become the gateway before it has spoken; if it has not discovered this node within `gossip.suspect_after`, the next member takes over the same way. Repeated switches mean pods in that zone keep restarting or the network between the zones is lossy.

**What to do next.** Nothing for a single switch after a restart. If the warning repeats, check restarts in the zone named by `zone_hash`, then the network path and any NetworkPolicy or security group between the zones; with `cannot_relay`, read the old gateway's `relay_stalled` lines. If the pods are healthy and the network between zones loses packets, raise `gossip.suspect_after` to make switches rarer at the cost of slower failover.

**Fields** `zone_hash`, `old_gateway`, `new_gateway`, `reason`, `unanswered_ms`, `suspect_after_ms`, `failover_total`, `config_key` · **Knob** `gossip.suspect_after` (`GABION_GOSSIP_SUSPECT_AFTER`) / `gabion_gossip_suspect_after`

### `relay_stalled`

**Level** warn, power of two · **Request allowed** yes, under-counted at that peer · **Emitted by** library (both adapters)

Message starts: the peer's address, then "a peer in this node's zone, has not acknowledged the changes this node relays"

**What happened.** This node relays other zones' changes into its own zone, and one peer of its zone has acknowledged none of them for `stalled_ms`, so it is missing those counts and limits enforced there under-count and allow requests over the limit. gabion keeps resending (once the peer is suspect, as one small probe per heartbeat whose answer brings the resend), and the counts arrive as soon as the path works.

**Why.** Usually UDP from this node to that peer is blocked in one direction, or the peer is overloaded. Only a peer this node has heard from is reported: one that discovery placed in the zone and that has not answered yet may simply not have discovered this node. When most of the zone stalls, this node also tells the nodes it relays for that it cannot relay, and they switch gateways (`gateway_failover` with `reason = "cannot_relay"`).

**What to do next.** Check the NetworkPolicies and security groups between the two pods, and the peer's CPU.

**Fields** `peer`, `stalled_ms`, `relay_unacked`, `stall_total` · **Knob** none

### `setting_warning`

**Level** warn, once at start · **Request allowed** yes · **Emitted by** library (both adapters)

Message starts: "`gossip.tick_interval` (`GABION_GOSSIP_TICK_INTERVAL`; NGINX `gabion_gossip_tick_interval`) is"

**What happened.** A setting is legal but probably not what was meant. `cause = "tick_near_round_trip"`: the heartbeat (`tick_ms`) is under 20 ms, so a resend can leave before the answer to the first copy arrives. That costs bytes, not counts: suspicion waits for `gossip.suspect_after`, whose default stays at least 2 s however short the heartbeat.

**Why.** `gossip.tick_interval` was set very short, usually to speed convergence in a test.

**What to do next.** If `/snapshot` shows `unanswered_ms` regularly above `tick_ms` for healthy peers, raise `gossip.tick_interval` to at least four times the round trip between your zones.

**Fields** `cause`, `tick_ms`, `config_key` · **Knob** `gossip.tick_interval` (`GABION_GOSSIP_TICK_INTERVAL`) / `gabion_gossip_tick_interval`

### `kubernetes_forbidden`

**Level** error, once per watch · **Request allowed** yes, discovery stops · **Emitted by** library (both adapters)

Message starts: "The Kubernetes API rejected gabion's"

**What happened.** The Kubernetes API rejected gabion's Service or EndpointSlice watch (the `resource` field says which), so peer discovery has stopped and the peer list stays as it was. Gossip continues with the peers already known; no new peers are found.

**Why.** Usually the pod's ServiceAccount lacks RBAC permission (`status` 401 or 403); `status` 404 means the cluster does not serve `discovery.k8s.io/v1` EndpointSlices (Kubernetes 1.21 or later is required).

**What to do next.** Grant the ServiceAccount get/list/watch on Services and EndpointSlices and get on Pods, in every namespace discovery watches (see [`deploy/kubernetes/gabion-discovery-rbac.yaml`](../../deploy/kubernetes/gabion-discovery-rbac.yaml) and the [Kubernetes checklist](../../README.md#kubernetes-checklist)), then restart the pod.

**Fields** `resource`, `status`, `error`, `namespace` and `service` (EndpointSlice watch only) · **Knob** the Role in the Kubernetes checklist

### `no_gabion_service`

**Level** warn, once · **Request allowed** yes, no peers are found · **Emitted by** library (both adapters)

Message starts: "Kubernetes auto-discovery is up but no Service in the watched namespaces"

**What happened.** Discovery can reach the Kubernetes API, but no Service in the watched namespaces exposes a UDP port named `gabion`, so it found no peers and this node's counts stay local.

**Why.** Usually the manifest named the port differently (`gossip`, `udp`) or left the protocol as the default TCP. Discovery filters on both the name `gabion` and protocol `UDP`.

**What to do next.** Rename the Service port to `name: gabion` with `protocol: UDP` and re-apply the manifest (see the [Kubernetes checklist](../../README.md#kubernetes-checklist)). Also check `discovery.namespace_allow` and `discovery.service_allow` (NGINX `gabion_discovery_namespace_allow`, `gabion_discovery_service_allow`).

**Fields** `watched_namespaces`, `gabion_port_name` · **Knob** the Service's port `name` and `protocol`

### `discovery_error`

**Level** warn, power of two · **Request allowed** yes, the peer list may be stale · **Emitted by** library (both adapters)

Message starts: "Peer discovery hit an error and skipped an update"

**What happened.** The Kubernetes discovery stream returned an error, so gabion skipped that update and kept the peer list it had. It keeps retrying; until an update succeeds, peers that joined or left since are not reflected, and gossip to a missing peer under-counts what it handled.

**Why.** Usually transient: the API server restarted or a watch was reset. If it keeps repeating, the pod cannot reach the API or lacks RBAC access to Services and EndpointSlices.

**What to do next.** If it repeats, check API access (the Role in the [Kubernetes checklist](../../README.md#kubernetes-checklist)) and the discovery settings: `discovery.namespace_allow`, `discovery.service_allow`, `discovery.self_addr` in `gabiond`, `gabion_discovery_*` in NGINX.

**Fields** `error`, `errors_total` · **Knob** `discovery.*` / `gabion_discovery_*`

### `kubernetes_unreachable`

**Level** warn, once each time discovery starts (NGINX: once per leadership term) · **Request allowed** yes, this node's counts stay local · **Emitted by** library (both adapters)

Message starts: "Could not connect to the Kubernetes API, so gabion cannot discover peers"

**What happened.** Kubernetes discovery could not build its API client, so it stopped and this node found no peers: its limits count only its own traffic.

**Why.** Expected outside Kubernetes, where a node with `gossip.bind` and no `gossip.bootstrap_peers` tries Kubernetes discovery. In a pod, usually the ServiceAccount token is not mounted (`automountServiceAccountToken: false`) or the in-cluster environment variables are missing.

**What to do next.** Outside Kubernetes, list the peers in `gossip.bootstrap_peers` (`GABION_GOSSIP_BOOTSTRAP_PEERS`; NGINX `gabion_gossip_bootstrap_peers`), see [Outside Kubernetes](README.md#outside-kubernetes); a single node needs nothing. In a pod, mount the ServiceAccount token and grant the Role in the [Kubernetes checklist](../../README.md#kubernetes-checklist), then restart the pod.

**Fields** `error` · **Knob** `gossip.bootstrap_peers` (`GABION_GOSSIP_BOOTSTRAP_PEERS`) / `gabion_gossip_bootstrap_peers`

### `gabion_service_found`

**Level** info, once per Service · **Request allowed** yes, counts unaffected · **Emitted by** library (both adapters)

Message starts: "Found a gabion service in the cluster; watching it for new peers."

**What happened.** Discovery found a Service with a UDP port named `gabion` in a watched namespace and started watching its EndpointSlices; its ready pods become peers.

**Why.** The node started, or the Service was created or gained its `gabion` port.

**What to do next.** Nothing. If the Service is not the one you meant, narrow `discovery.service_allow` (`GABION_DISCOVERY_SERVICE_ALLOW`; NGINX `gabion_discovery_service_allow`).

**Fields** `namespace`, `service` · **Knob** `discovery.service_allow` / `gabion_discovery_service_allow`

### `gabion_service_gone`

**Level** info, once per Service · **Request allowed** yes, its peers leave this node's table · **Emitted by** library (both adapters)

Message starts: "Gabion service is no longer reachable"

**What happened.** A Service discovery was watching was deleted or lost its UDP port named `gabion`, so discovery stopped watching it and removed the peers only it listed (each logs `peer_left`).

**Why.** Usually a deployment was removed or its Service rewritten; also a port renamed or switched to TCP by mistake.

**What to do next.** Nothing if the Service was meant to go. Otherwise restore its port as `name: gabion`, `protocol: UDP` (the [Kubernetes checklist](../../README.md#kubernetes-checklist)).

**Fields** `namespace`, `service` · **Knob** the Service's port `name` and `protocol`

### `zone_known`

**Level** info, once when the zone becomes known (NGINX: each time a worker starts leading gossip) · **Request allowed** yes, counts unaffected · **Emitted by** library (both adapters)

Message starts: "This node is in zone"

**What happened.** This node knows its availability zone, from the zone setting (`zone_source = "config"`) or from its own Kubernetes EndpointSlice entry (`zone_source = "endpoint_slice"`). It now sends each change directly to the peers of its zone and through one gateway in each other zone. Nothing is wrong.

**Why.** Logged at start for a configured zone, or when discovery first finds the node's own entry with a zone. A `zone_unknown` line before it only meant discovery was slow. The first node of a new zone asks a peer of another zone for its catch-up sweep once every discovered peer has answered, or once it has waited `gossip.zone_grace`.

**What to do next.** Nothing. If the zone is wrong, set `gossip.zone`, or correct the node's `topology.kubernetes.io/zone` label and restart the pod. `/snapshot`'s `zone` object shows the same name and source.

**Fields** `zone`, `zone_hash`, `zone_source` · **Knob** `gossip.zone` (`GABION_GOSSIP_ZONE`) / `gabion_gossip_zone`

### `zone_unknown`

**Level** warn, once (NGINX: once per leadership term) · **Request allowed** yes, counts unaffected · **Emitted by** library (both adapters)

Message starts: "gabion does not know which availability zone this node runs in"

**What happened.** This node knows no zone, so it treats the whole cluster as one zone and sends every change to every peer directly. Counts stay correct, but none of the cross-zone traffic that zone-aware gossip saves is saved. `reason` says why.

**Why.** `not_configured`: the node uses a static peer list (`gossip.bootstrap_peers`) and has no zone setting, the only place it can learn one; logged at start. `own_entry_not_found`: Kubernetes discovery runs, but after `gossip.zone_grace` none of the EndpointSlice entries it watches is this pod's, usually because the pod's host name differs from its pod name (`hostNetwork: true`, `spec.hostname`) or the pod is outside the watched Services. `own_entry_has_no_zone`: the pod's entry has no zone, because its node (`node_name`) has no `topology.kubernetes.io/zone` label. A node with no peers never logs it.

**What to do next.** `not_configured`: set `gossip.zone` on each node, for example from one of the cloud-metadata commands in the [zones section](README.md#outside-kubernetes). `own_entry_not_found`: set `discovery.self_addr` (`GABION_DISCOVERY_SELF_ADDR`; NGINX `gabion_discovery_self_addr`) to the pod's IP and gossip port, or set `gossip.zone`; if a `zone_known` line follows soon after, discovery was only slow: raise `gossip.zone_grace` above that delay. `own_entry_has_no_zone`: `kubectl label node <node_name> topology.kubernetes.io/zone=<zone>` and restart the pod, or set `gossip.zone`. In a cluster that runs in one zone, set `gossip.zone` to any name to silence it.

**Fields** `zone_source` (`none`), `reason`, `membership`, `config_key`, and by reason `self_match` (`self_addr`, `pod_name` or `none`), `hostname` or `self_addr`, `node_name`, `grace_ms` · **Knob** `gossip.zone` (`GABION_GOSSIP_ZONE`) / `gabion_gossip_zone`; `gossip.zone_grace` (`GABION_GOSSIP_ZONE_GRACE`) / `gabion_gossip_zone_grace`

### `zone_conflict`

**Level** warn, once (NGINX: once per leadership term) · **Request allowed** yes, counts unaffected · **Emitted by** library (both adapters)

Message starts: "The zone setting says this node is in zone"

**What happened.** The zone setting (`configured_zone`) differs from the zone of this node's own Kubernetes EndpointSlice entry (`discovered_zone`). The setting takes precedence, and every peer sees that value, so counts stay correct.

**Why.** Usually a templated `gossip.zone` that does not match the node the pod landed on, or a mislabelled node. If the node really runs in `discovered_zone`, its traffic is grouped with the wrong zone and the cross-zone savings for this node are lost.

**What to do next.** Correct `gossip.zone`, or remove it so the node uses the Kubernetes value.

**Fields** `configured_zone`, `discovered_zone`, `zone_source` (`config`), `config_key` · **Knob** `gossip.zone` (`GABION_GOSSIP_ZONE`) / `gabion_gossip_zone`
