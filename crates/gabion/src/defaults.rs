//! Production defaults shared by `gabiond` and the nginx adapter.
//!
//! The low-level library types may keep smaller construction defaults for
//! tests and examples. Server-facing binaries should use these values.

pub const STORAGE_MAX_CELLS: usize = 131_072;
/// Floor of the rule dictionary's default capacity, `max(256, 2 × rules)`;
/// `storage.rule_dictionary_capacity` overrides it.
pub const STORAGE_RULE_DICTIONARY_FLOOR: u16 = 256;

pub const STORAGE_MAX_DESCRIPTOR_COUNT: u16 = 16;
pub const STORAGE_MAX_DESCRIPTOR_BYTES: usize = 512;
pub const STORAGE_MAX_KEY_BYTES: usize = 128;

/// Default of `storage.max_matched_rules`, the most rules one request is
/// counted against. Past the bound a request is still checked against every
/// matched rule, and only the first this-many count it; the rest
/// under-count. Both adapters log `matched_rule_overflow`: gabiond on the
/// request, NGINX from its gossip leader.
pub const STORAGE_MAX_MATCHED_RULES: u16 = 16;

/// Default of `storage.max_request_descriptors`, the most descriptors of one
/// Envoy request gabiond evaluates; the rest are rejected as
/// `cardinality_rejected`.
pub const STORAGE_MAX_REQUEST_DESCRIPTORS: u16 = 16;

/// The heartbeat period `T`, also the retransmission timeout.
pub const GOSSIP_TICK_INTERVAL_MILLIS: u64 = 500;

/// Heartbeats a peer may leave a packet unanswered before it is suspect:
/// `gossip.suspect_after` defaults to
/// `max(this × tick_interval, GOSSIP_SUSPECT_AFTER_FLOOR_MS)`, four answer
/// attempts.
pub const GOSSIP_SUSPECT_AFTER_TICKS: u64 = 4;

/// The floor of `gossip.suspect_after`'s default, so a shorter heartbeat buys
/// more answer attempts, not a shorter suspicion time.
pub const GOSSIP_SUSPECT_AFTER_FLOOR_MS: u64 = 2_000;

/// Default of `gossip.peer_capacity`, the most peers one node gossips with:
/// 300 pods with 1.7x headroom. The node dictionary's default is twice it.
pub const GOSSIP_PEER_CAPACITY: u16 = 512;

pub const GOSSIP_MAX_PAYLOAD_BYTES: usize = 1400;
pub const GOSSIP_LIMIT_QUEUE_CAPACITY: usize = 8192;

/// Receive buffer, in bytes, the gossip socket asks the kernel for: 4 MiB.
/// `socket_buffer_capped` warns when `net.core.rmem_max` grants less.
pub const GOSSIP_RECV_BUFFER_BYTES: u32 = 4 << 20;

/// Send buffer, in bytes, the gossip socket asks the kernel for: 1 MiB.
/// `socket_buffer_capped` warns when `net.core.wmem_max` grants less;
/// `send_blocked` when a round fills it.
pub const GOSSIP_SEND_BUFFER_BYTES: u32 = 1 << 20;

/// The most expired cells one expiry call frees before the runtime applies
/// them and calls again; sizes the runtime's expiration sink.
pub const GOSSIP_EXPIRATION_BATCH: usize = 4_096;
pub const GOSSIP_CLUSTER_ID_HASH: u128 = 1;

/// Per-rule error budget for threshold-triggered anti-entropy, expressed in
/// basis points of the rule's own limit. A node emits the moment its locally
/// unreplicated delta for some rule R would cross `target_err_bps / 10_000 ×
/// L_R / N` (per-site safe zone of Sharfman, Schuster, Keren, SIGMOD 2006,
/// calibrated by the Olston/Jiang/Widom SIGMOD 2003 error budget). The
/// cluster-wide unreplicated error per rule is then bounded by
/// `target_err_bps / 10_000 × L_R`, independent of request rate; default
/// 100 bps = 1 % of the rule's limit.
pub const GOSSIP_TARGET_ERR_BPS: u32 = 100;

/// Floor on the gap between two threshold-fire emissions, in milliseconds.
/// When the budget saturates to 1 hit under adversarial high-RPS traffic,
/// this clamps worst-case bandwidth so a bad client cannot pin the gossip
/// plane. Independent of the steady-state heartbeat (which still fires at
/// `GOSSIP_TICK_INTERVAL_MILLIS`).
pub const GOSSIP_MIN_EMIT_INTERVAL_MS: u64 = 5;

/// Gap reports a receiver sends for one gap without progress before it
/// abandons the gap and logs `gap_abandoned`; also the requests a join sweep
/// sends one source before it asks the next.
pub const GOSSIP_GAP_REPORTS: u8 = 4;

/// Sources a starting node asks for a join sweep: its highest-weight heard
/// peers, fewer when it has heard fewer.
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

/// A lower bound on the receive-buffer bytes the kernel charges one queued
/// datagram. A round reads at most the granted receive bytes over this many
/// waiting datagrams, so an inbound flood cannot hold it.
pub const GOSSIP_MIN_DATAGRAM_CHARGE: usize = 512;

/// The prompt rounds in which a full store's backlog drains to one peer: the
/// per-round quantum of cells per stream per peer is
/// `max(⌈storage.max_cells / this⌉, cells in one packet)`.
pub const GOSSIP_BACKLOG_ROUNDS: usize = 128;
