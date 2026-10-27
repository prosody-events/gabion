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

Message starts: "Too many distinct rate-limit rules are in flight."

**What happened.** The rule dictionary had no free slot for a rule. The rule cannot be registered on this node, so requests it matches are allowed and its limit under-counts here.

**Why.** Usually more rules are configured across the cluster than the dictionary holds, for example during a configuration rollout that briefly runs old and new rule sets together.

**What to do next.** Raise `storage.rule_dictionary_capacity` (`GABION_STORAGE_RULE_DICTIONARY_CAPACITY`) in `gabiond`, or `gabion_storage_rule_dictionary_capacity` in NGINX, above the number of distinct rules alive in the cluster at once.

**Fields** `config_key`, `capacity`, `in_use`, `rule_fingerprint`, `rejected_total` · **Knob** `storage.rule_dictionary_capacity` (`GABION_STORAGE_RULE_DICTIONARY_CAPACITY`) / `gabion_storage_rule_dictionary_capacity`

### `node_dictionary_full`

**Level** warn, power of two · **Request allowed** yes, some peers' counts dropped · **Emitted by** library (both adapters)

Message starts: "Too many gabion peer instances are being tracked"

**What happened.** The node dictionary had no free slot for a peer instance. Counts from some peers are being dropped, so cluster-wide limits under-count the requests those peers handled.

**Why.** Every peer restart counts as a new instance until its data ages out, so a large cluster with frequent restarts can outgrow the dictionary.

**What to do next.** Raise `storage.node_dictionary_capacity` (`GABION_STORAGE_NODE_DICTIONARY_CAPACITY`) in `gabiond`, or `gabion_storage_node_dictionary_capacity` in NGINX. Size it above pods × (1 + restarts within your longest rule window).

**Fields** `config_key`, `capacity`, `in_use`, `peer_node_id`, `peer_incarnation`, `rejected_total` · **Knob** `storage.node_dictionary_capacity` (`GABION_STORAGE_NODE_DICTIONARY_CAPACITY`) / `gabion_storage_node_dictionary_capacity`

### `peer_frontier_full`

**Level** warn, power of two · **Request allowed** yes, gossip to that peer uses more bandwidth · **Emitted by** library (both adapters)

Message starts: "Too many gabion peers to optimize gossip with."

**What happened.** The peer frontier, which remembers what each peer already has so gossip can skip it, had no free slot for a peer. Counts still converge; gossip with that peer resends rows it already holds.

**Why.** Usually the cluster has more peers than `storage.peer_capacity`, for example after scaling out.

**What to do next.** Raise `storage.peer_capacity` (`GABION_STORAGE_PEER_CAPACITY`) in `gabiond`, or `gabion_storage_peer_capacity` in NGINX, above the number of peers in the cluster.

**Fields** `config_key`, `capacity`, `peer_node_id`, `rejected_total` (packets from peers without a slot, not peers) · **Knob** `storage.peer_capacity` (`GABION_STORAGE_PEER_CAPACITY`) / `gabion_storage_peer_capacity`

### `decode_rejected`

**Level** warn, power of two · **Request allowed** yes, the peer's counts are lost · **Emitted by** library (both adapters)

Message starts: "Could not understand a gossip message from this peer, so it was dropped"

**What happened.** A gossip packet could not be decoded and was dropped, so limits here under-count the traffic that peer handled. The `error` field says what was wrong with the packet.

**Why.** Usually the peer runs a different gabion version, or its payload or cells-per-frame size is larger than this node's. Otherwise another program is sending UDP to this node's gossip port.

**What to do next.** Check the peer's gabion version, and set `gossip.max_payload_bytes` and `gossip.max_cells_per_frame` (NGINX `gabion_gossip_max_payload_bytes`, `gabion_gossip_max_cells_per_frame`) to the same values on every node.

**Fields** `peer`, `error`, `rejected_total` · **Knob** `gossip.max_payload_bytes` (`GABION_GOSSIP_MAX_PAYLOAD_BYTES`) / `gabion_gossip_max_payload_bytes`; `gossip.max_cells_per_frame` (`GABION_GOSSIP_MAX_CELLS_PER_FRAME`) / `gabion_gossip_max_cells_per_frame`

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
