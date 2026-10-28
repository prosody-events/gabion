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

**Why.** Usually more rules are live across the cluster than the dictionary holds, for example during a configuration rollout that briefly runs old and new rule sets together.

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

**What to do next.** Raise `gossip.peer_capacity` (`GABION_GOSSIP_PEER_CAPACITY`) in `gabiond`, or `gabion_gossip_peer_capacity` in NGINX, above the cluster's pod count. Each slot costs about 100 bytes.

**Fields** `config_key`, `capacity`, `peer`, `declined_total` · **Knob** `gossip.peer_capacity` (`GABION_GOSSIP_PEER_CAPACITY`) / `gabion_gossip_peer_capacity`

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

**Fields** `peer`, `peer_version`, `local_version`, `reason` (`version`), `rejected_total` · **Knob** none

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

**What to do next.** If it repeats for a peer that is running, check the policy and the route between the two nodes.

**Fields** `peer`, `error`, `send_errors_total` · **Knob** none

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
