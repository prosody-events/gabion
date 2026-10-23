//! Production defaults shared by `gabiond` and the nginx adapter.
//!
//! The low-level library types may keep smaller construction defaults for
//! tests and examples. Server-facing binaries should use these values.

pub const STORAGE_MAX_CELLS: usize = 131_072;
pub const STORAGE_RULE_DICTIONARY_CAPACITY: u16 = 64;
pub const STORAGE_NODE_DICTIONARY_CAPACITY: u16 = 1024;
pub const STORAGE_LOCAL_DIRTY_CAPACITY: usize = 65_536;
pub const STORAGE_FORWARDED_DIRTY_CAPACITY: usize = 524_288;
pub const STORAGE_PEER_CAPACITY: u16 = 256;

pub const STORAGE_MAX_DESCRIPTOR_COUNT: usize = 16;
pub const STORAGE_MAX_DESCRIPTOR_BYTES: usize = 512;
pub const STORAGE_MAX_KEY_BYTES: usize = 128;

/// Maximum number of rules that can match a single request. Bounds the
/// per-request decision/record loop in both adapters; deployments with
/// fewer rules cost less, deployments above this cap reject the request
/// (rather than silently truncate).
pub const STORAGE_MAX_MATCHED_RULES: usize = 16;

pub const GOSSIP_TICK_INTERVAL_MILLIS: u64 = 500;

/// Branching factor `k` of the Snow broadcast tree (Tong, arXiv 2504.02676,
/// default `k = 4`): each node forwards a broadcast to at most `k` children and
/// a root waits for `k` acknowledgements. Each side of a node's range is split
/// into `k / 2` sub-ranges, so the runtime clamps `k` to `2..=8`.
pub const GOSSIP_FANOUT: usize = 4;

pub const GOSSIP_MAX_PAYLOAD_BYTES: usize = 1400;
pub const GOSSIP_MAX_CELLS_PER_FRAME: u32 = 4096;
pub const GOSSIP_MAX_CELLS_PER_TICK: usize = 4096;
pub const GOSSIP_SEND_QUEUE_CAPACITY: usize = 128;
pub const GOSSIP_LIMIT_QUEUE_CAPACITY: usize = 8192;
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

/// A root rebroadcasts after `GOSSIP_ACK_TIMEOUT_RTT_MULTIPLE` times the
/// expected tree round-trip time without a complete acknowledgement. The
/// expected round trip is the root's smoothed measurement of completed
/// broadcasts, seeded by `GOSSIP_TREE_INITIAL_HOP_RTT_MS` until the first
/// completes.
pub const GOSSIP_ACK_TIMEOUT_RTT_MULTIPLE: u32 = 2;

/// Rebroadcasts a root attempts before it leaves the broadcast to the
/// periodic repair pass.
pub const GOSSIP_ROOT_REBROADCASTS: u8 = 1;

/// Cells the periodic repair pass sends to one random peer per heartbeat.
/// The repair pass is the rotating anti-entropy slice of the cell store that
/// backs every dissemination design; it covers broadcasts the tree could not
/// complete and cells a newly joined node missed. The paper leaves the repair
/// lane unspecified; 256 is the heartbeat repair slice the study's other
/// push design uses (`flat-once`, section 3.4 of the survey), so the lanes
/// behind each dissemination design carry the same volume.
pub const GOSSIP_REPAIR_CELLS_PER_TICK: usize = 256;

/// Per-hop round-trip estimate used to seed the acknowledgement timeout
/// before the first broadcast completes: `tree height * this`.
pub const GOSSIP_TREE_INITIAL_HOP_RTT_MS: u64 = 4;

/// Granularity at which a root checks acknowledgement deadlines. Only
/// polled while a broadcast is awaiting acknowledgements.
pub const GOSSIP_TREE_TIMER_TICK_MS: u64 = 5;

/// Broadcasts a node can forward for other roots at once before the oldest
/// is forgotten (forgetting costs at most one redundant forward).
pub const GOSSIP_TREE_RELAY_CAPACITY: usize = 1024;

/// Broadcasts a node can have awaiting acknowledgement as a root.
pub const GOSSIP_TREE_ORIGIN_CAPACITY: usize = 64;

pub fn random_rng_seed() -> Result<u64, getrandom::Error> {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes)?;
    Ok(u64::from_ne_bytes(bytes))
}
