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

/// Unused by the bimodal design: the first phase sends each change to every
/// peer, so there is no per-tick fanout to size. Kept so configuration that
/// sets it still loads.
pub const GOSSIP_FANOUT: usize = 3;

/// Period between digest repair exchanges, in milliseconds. Each period a
/// node sends one digest to one random peer (Birman et al., "Bimodal
/// Multicast", ACM TOCS 1999: one repair round per gossip period, fanout 1).
pub const GOSSIP_REPAIR_PERIOD_MILLIS: u64 = 1000;

/// Linux's `net.core.rmem_default`: the UDP receive buffer a gossip socket
/// gets unless the operator enlarges it. Datagrams that arrive while it is
/// full are dropped silently.
pub const GOSSIP_RECEIVE_BUFFER_BYTES: usize = 212_992;

/// The repair phase may fill at most `1 / GOSSIP_REPAIR_BUFFER_SHARE` of one
/// receive buffer per repair period; the rest is left for first-phase
/// packets and digests that arrive in the same period.
pub const GOSSIP_REPAIR_BUFFER_SHARE: usize = 2;

/// Worst-case wire bytes one cell costs a repair packet: the fixed per-cell
/// columns (42), an 8-byte count, and its own rule and node dictionary
/// entries (2 x 16) in a packet that shares nothing with other cells.
pub const GOSSIP_REPAIR_CELL_WIRE_BYTES: usize = 82;

/// Cells a node will send in answer to digests during one repair period: the
/// paper's per-round retransmission limit, which it states as "a limit" with
/// no value. Derived from the receive buffer rather than tuned: half of
/// `rmem_default` (106,496 bytes) at the worst-case 82 bytes per cell is
/// 1,298 cells (about 76 full packets). Repair and churn results are bound by
/// this budget; it is a swept design parameter alongside
/// [`GOSSIP_REPAIR_PERIOD_MILLIS`].
pub const GOSSIP_REPAIR_MAX_CELLS: usize =
    GOSSIP_RECEIVE_BUFFER_BYTES / GOSSIP_REPAIR_BUFFER_SHARE / GOSSIP_REPAIR_CELL_WIRE_BYTES;

/// Words of the per-origin held-sequence bitmap in a digest entry, at most.
/// The bitmap covers up to `64 * words` sequences below the origin's maximum
/// (256 by default). An entry sends only the words it needs to reach its
/// oldest held sequence, so a word costs 8 bytes only in entries that span
/// past the words before it. When the held sequences span more than the
/// window, a gap further back is assumed held and never repaired: repair sees
/// every live cell while an origin's change rate times its rules' live span
/// (window plus one bucket) stays at or under `64 * words`, which a
/// long-window rule at a high per-origin hit rate can exceed.
pub const GOSSIP_DIGEST_WINDOW_WORDS: usize = 4;

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

pub fn random_rng_seed() -> Result<u64, getrandom::Error> {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes)?;
    Ok(u64::from_ne_bytes(bytes))
}
