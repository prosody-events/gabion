//! Production defaults shared by `gabiond` and the nginx adapter.
//!
//! The low-level library types may keep smaller construction defaults for
//! tests and examples. Server-facing binaries should use these values.
//! Each constant's doc gives its unit and, where it has them, the memory it
//! sizes and the message that names it.

#![warn(missing_docs)]

/// Default of `storage.max_cells`, in counter cells: about 96 bytes a cell in
/// the store and, in `gabiond`, 128 more in the aggregate table (twice the
/// cells, rounded up to a power of two, 64 bytes a slot). Full:
/// `cell_store_full`, `aggregate_full`.
pub const STORAGE_MAX_CELLS: usize = 131_072;

/// Floor of the rule dictionary's default capacity, `max(256, 2 × rules)`;
/// `storage.rule_dictionary_capacity` overrides it.
pub const STORAGE_RULE_DICTIONARY_FLOOR: u16 = 256;

/// Default of `storage.max_descriptor_count`, in descriptors per rule (at
/// most 65,535); a longer rule fails to load (`TooManyDescriptors`).
pub const STORAGE_MAX_DESCRIPTOR_COUNT: u16 = 16;

/// Default of `storage.max_descriptor_bytes`, in bytes per descriptor: the
/// domain plus the keys and values of one Envoy descriptor, or of one NGINX
/// rule's bindings; past it that descriptor is rejected
/// (`cardinality_rejected`).
pub const STORAGE_MAX_DESCRIPTOR_BYTES: usize = 512;

/// Default of `storage.max_key_bytes`, in bytes per descriptor key: a longer
/// key fails to load (`KeyTooLong`), or rejects the request that sends it
/// (`cardinality_rejected`).
pub const STORAGE_MAX_KEY_BYTES: usize = 128;

/// Default of `storage.max_matched_rules`, the most rules one request is
/// counted against. Past the bound a request is still checked against every
/// matched rule, and only the first this-many count it; the rest
/// under-count. Both adapters log `matched_rule_overflow`: gabiond on the
/// request, NGINX from each worker's leader thread. Each rule reserves 32
/// bytes per `gabiond` runtime thread and 64 per NGINX worker.
pub const STORAGE_MAX_MATCHED_RULES: u16 = 16;

/// Default of `storage.max_request_descriptors`, the most descriptors of one
/// Envoy request gabiond evaluates; the rest are rejected as
/// `cardinality_rejected`.
pub const STORAGE_MAX_REQUEST_DESCRIPTORS: u16 = 16;

/// Default of `gossip.tick_interval`, in milliseconds: the heartbeat period
/// `T`, also the retransmission timeout. Below 20 ms: `setting_warning`.
pub const GOSSIP_TICK_INTERVAL_MILLIS: u64 = 500;

/// In heartbeats: how many a peer may leave a packet unanswered before it is
/// suspect. `gossip.suspect_after` defaults to
/// `max(this × tick_interval, GOSSIP_SUSPECT_AFTER_FLOOR_MS)`, four answer
/// attempts; `gateway_failover` says when a suspect gateway is replaced.
pub const GOSSIP_SUSPECT_AFTER_TICKS: u64 = 4;

/// The floor of `gossip.suspect_after`'s default, in milliseconds, so a
/// shorter heartbeat buys more answer attempts, not a shorter suspicion time.
pub const GOSSIP_SUSPECT_AFTER_FLOOR_MS: u64 = 2_000;

/// Default of `gossip.peer_capacity`, the most peers one node gossips with:
/// 300 pods with 1.7x headroom, about 330 bytes a peer. Full:
/// `peer_table_full`. The node dictionary's default is twice it.
pub const GOSSIP_PEER_CAPACITY: u16 = 512;

/// Default of `gossip.max_payload_bytes`, in bytes per UDP payload: 1,400
/// plus 28 bytes of IPv4 and UDP header fit a 1,500-byte MTU. A peer that
/// sends larger packets is logged as `decode_rejected` (`peer_limits`).
pub const GOSSIP_MAX_PAYLOAD_BYTES: usize = 1400;

/// Default of `gossip.limit_queue_capacity`, in queued hits: NGINX's
/// shared-memory request queue (72 bytes a slot, rounded up to a power of two)
/// and `gabiond`'s request channel. Full: `queue_dropped` (NGINX),
/// `request_queue_full` (`gabiond`).
pub const GOSSIP_LIMIT_QUEUE_CAPACITY: usize = 8192;

/// Receive buffer, in bytes, the gossip socket asks the kernel for: 4 MiB.
/// `socket_buffer_capped` warns when `net.core.rmem_max` grants less.
pub const GOSSIP_RECV_BUFFER_BYTES: u32 = 4 << 20;

/// Send buffer, in bytes, the gossip socket asks the kernel for: 1 MiB.
/// `socket_buffer_capped` warns when `net.core.wmem_max` grants less;
/// `send_blocked` when a round fills it.
pub const GOSSIP_SEND_BUFFER_BYTES: u32 = 1 << 20;

/// In cells: the most expired cells one expiry call frees before the runtime
/// applies them and calls again; sizes the runtime's expiration sink.
pub const GOSSIP_EXPIRATION_BATCH: usize = 4_096;

/// Default of `gossip.cluster_id_hash`: a label, not a secret. A node drops
/// packets that carry another cluster's id; `default_cluster_id` says when a
/// node runs with this one.
pub const GOSSIP_CLUSTER_ID_HASH: u128 = 1;

/// Default of `gossip.target_err_bps`, in basis points of each rule's limit
/// (1 to 10,000): a node sends a rule's unsent hits once they pass `max(1,
/// limit × target_err_bps / (10,000 × peers))`, so the cluster's unreplicated
/// hits per rule stay under this share of its limit whatever the request rate
/// (`crates/gabion/GOSSIP.md`, Adaptive emit rate).
pub const GOSSIP_TARGET_ERR_BPS: u32 = 100;

/// Default of `gossip.min_emit_interval`, in milliseconds: the floor between
/// two threshold or prompt rounds, below `gossip.tick_interval`, which caps a
/// node's packet rate under a flood of hits; `/snapshot`'s `rounds_threshold`
/// and `packets_sent` show when it binds.
pub const GOSSIP_MIN_EMIT_INTERVAL_MS: u64 = 5;

/// In reports: gap reports a receiver sends for one gap without progress before
/// it abandons the gap and logs `gap_abandoned`; also the requests a join sweep
/// sends one source before it asks the next.
pub const GOSSIP_GAP_REPORTS: u8 = 4;

/// In peers: sources a starting node asks for a join sweep: its highest-weight
/// heard peers, fewer when it has heard fewer.
pub const GOSSIP_SWEEP_PEERS: usize = 2;

/// Default of `gossip.sweep_concurrency`, the most join sweeps one node
/// answers at once.
pub const GOSSIP_SWEEP_CONCURRENCY: u8 = 4;

/// Default of `gossip.zone_grace`, in milliseconds: how long after its first
/// peer event a Kubernetes node waits for its own EndpointSlice entry before
/// it logs `zone_unknown`, and how long a join sweep waits for unheard peers
/// before a lone node of a new zone asks a peer of another zone. It must stay
/// at least five times the own-entry wait the kind smokes print.
pub const GOSSIP_ZONE_GRACE_MS: u64 = 10_000;

/// In bytes: a lower bound on the receive-buffer bytes the kernel charges one
/// queued datagram. A round reads at most the granted receive bytes over this
/// many waiting datagrams, so an inbound flood cannot hold it.
pub const GOSSIP_MIN_DATAGRAM_CHARGE: usize = 512;

/// In rounds: the prompt rounds in which a full store's backlog drains to one
/// peer: the per-round quantum of cells per stream per peer is
/// `max(⌈storage.max_cells / this⌉, cells in one packet)`.
pub const GOSSIP_BACKLOG_ROUNDS: usize = 128;
