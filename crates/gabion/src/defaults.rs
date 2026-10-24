//! Production defaults shared by `gabiond` and the nginx adapter.
//!
//! The low-level library types may keep smaller construction defaults for
//! tests and examples. Server-facing binaries should use these values.

pub const STORAGE_MAX_CELLS: usize = 131_072;
pub const STORAGE_RULE_DICTIONARY_CAPACITY: u16 = 64;
pub const STORAGE_NODE_DICTIONARY_CAPACITY: u16 = 1024;
pub const STORAGE_LOCAL_DIRTY_CAPACITY: usize = 65_536;
pub const STORAGE_FORWARDED_DIRTY_CAPACITY: usize = 524_288;
/// Has no effect: it sized the per-peer gossip frontier, which no longer
/// exists. Kept, like `CellStoreConfig::peer_capacity`, because the frozen
/// `gossip-bench` study harness and existing operator configs still name it.
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

/// Floor on the number of peers contacted per gossip tick. The runtime does
/// **not** treat this as the operating fanout — it scales the actual per-tick
/// fanout up to the coverage threshold `⌈ln(n) + GOSSIP_COVERAGE_MARGIN⌉`
/// (`n` = live peer count), capped at the peer count. This floor is a hard
/// minimum: it binds only if `GOSSIP_COVERAGE_MARGIN` is lowered far enough
/// that the coverage threshold drops below it. At the shipped margin it never
/// binds. See `handle_gossip_tick` in `gossip/runtime.rs`.
pub const GOSSIP_FANOUT: usize = 3;

/// Coverage margin `c` in the gossip fanout law `⌈ln(n) + c⌉` (`n` = live peer
/// count). Per Kermarrec, Massoulié & Ganesh (IEEE TPDS 2003, "Probabilistic
/// Reliable Dissemination in Large-Scale Systems", Theorem 1), a directed
/// gossip round with mean fanout `ln(n) + c` reaches every node with
/// probability → `e^(−e^(−c))`:
///
/// | `c`   | per-round coverage `e^(−e^(−c))` |
/// |-------|----------------------------------|
/// | **3** | **95.1 %**                       |
/// | 4     | 98.2 %                           |
/// | 5     | 99.3 %                           |
///
/// Each change is disseminated once, KMG's one-shot setting: every node
/// that first learns it forwards it in one round. At `c = 3` about 5 % of
/// changes miss some node; a later change to the same cell supersedes the
/// miss, and the repair lane (`GOSSIP_REPAIR_CELLS_PER_HEARTBEAT`) reaches
/// the rest. Each step of `c` adds about one peer per round (≈ 10 % more
/// gossip bandwidth) for about e× fewer misses. Kept a `const`; promote to a
/// `GossipConfig` field if an operator ever needs to trade bandwidth for
/// coverage at runtime.
pub const GOSSIP_COVERAGE_MARGIN: f64 = 3.0;

pub const GOSSIP_MAX_PAYLOAD_BYTES: usize = 1400;
/// Pace of relay rounds. A node that merged changes from a peer forwards
/// them in a round of their own, at most once per this interval, instead of
/// waiting for the heartbeat; the wait lets a few dozen changes share each
/// packet's header and dictionaries.
pub const GOSSIP_RELAY_INTERVAL_MILLIS: u64 = 20;
/// Cells of the rotating repair slice each heartbeat round adds to its frame,
/// after the dirty lanes. Dirty changes are sent in one round each, so the
/// repair lane is what reaches a peer the round's picks missed, or a packet
/// the network lost; a slice per heartbeat bounds what that costs.
pub const GOSSIP_REPAIR_CELLS_PER_HEARTBEAT: usize = 64;
/// Shortest time the repair lane takes to sweep a node's whole store. A
/// store smaller than this many heartbeats' slices is swept at this pace
/// instead of being resent in full on every heartbeat.
pub const GOSSIP_REPAIR_MIN_SWEEP_MILLIS: u64 = 2_000;
/// A peer heard from again after this long without a packet is welcomed
/// back like a newly discovered one: sent this node's own live cells, which
/// it likely missed while cut off. Silence never removes a peer; membership
/// comes from discovery alone.
pub const GOSSIP_REWELCOME_SILENCE_MILLIS: u64 = 10_000;
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
