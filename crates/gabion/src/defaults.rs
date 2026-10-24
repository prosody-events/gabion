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
/// **These are single-round figures, and the single round is the wrong unit
/// for gabion.** KMG's theorem bounds one *one-shot* dissemination; gabion
/// instead runs continuous anti-entropy — it re-gossips every dirty cell each
/// tick until the peer frontier shows it acked, with the repair lane behind
/// that. So a node missed in one round is overwhelmingly likely to be reached
/// by the next, and the per-round coverage *compounds*: at `c = 3` the 4.9 %
/// per-round miss falls to ≈ 0.24 % after two rounds and ≈ 0.012 % after three.
/// We therefore size to the leaner `c = 3` threshold and let anti-entropy close
/// the gap, rather than paying KMG's one-shot `c ≈ 4` (their validated sims —
/// fanout 13 @ 10 000 nodes, 15 @ 50 000 — measure single-shot reach, which a
/// re-gossiping system does not need to match). `c = 3` costs ≈ 1 fewer peer
/// per tick at every cluster size (≈ 10 % less gossip bandwidth). Kept a
/// `const`; promote to a `GossipConfig` field if an operator ever needs to
/// trade bandwidth for coverage at runtime.
pub const GOSSIP_COVERAGE_MARGIN: f64 = 3.0;

pub const GOSSIP_MAX_PAYLOAD_BYTES: usize = 1400;
pub const GOSSIP_MAX_CELLS_PER_FRAME: u32 = 4096;
pub const GOSSIP_MAX_CELLS_PER_TICK: usize = 4096;
pub const GOSSIP_SEND_QUEUE_CAPACITY: usize = 128;
pub const GOSSIP_LIMIT_QUEUE_CAPACITY: usize = 8192;
pub const GOSSIP_CLUSTER_ID_HASH: u128 = 1;

/// Per-rule error budget for threshold-triggered anti-entropy, expressed in
/// basis points of the rule's own limit. A node emits the moment its locally
/// unreplicated delta for one key of some rule R would cross `target_err_bps /
/// 10_000 × L_R / N` (per-site safe zone of Sharfman, Schuster, Keren, SIGMOD
/// 2006, calibrated by the Olston/Jiang/Widom SIGMOD 2003 error budget). The
/// cluster-wide unreplicated error per key is then bounded by
/// `target_err_bps / 10_000 × L_R`, independent of request rate; default
/// 100 bps = 1 % of the rule's limit.
pub const GOSSIP_TARGET_ERR_BPS: u32 = 100;

/// Floor on the gap between two threshold-fire emissions, in milliseconds.
/// When the budget saturates to 1 hit under adversarial high-RPS traffic,
/// this clamps worst-case bandwidth so a bad client cannot pin the gossip
/// plane. Independent of the steady-state heartbeat (which still fires at
/// `GOSSIP_TICK_INTERVAL_MILLIS`).
pub const GOSSIP_MIN_EMIT_INTERVAL_MS: u64 = 5;

// -- GossipSub mesh dissemination -------------------------------------------
//
// The values below are the GossipSub v1.1 specification defaults
// (https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md),
// except the two capacities, which bound the per-node memory the spec leaves
// open. See "Design notes" in `crates/gabion/README.md`.

/// Target mesh degree `D`: how many peers a node eagerly pushes full messages
/// to, and the size a mesh is pruned or grafted back to.
pub const GOSSIPSUB_D: usize = 6;
/// Lower mesh bound `D_lo`: below this a heartbeat grafts up to `D`.
pub const GOSSIPSUB_D_LO: usize = 4;
/// Upper mesh bound `D_hi`: above this a heartbeat prunes down to `D`, and a
/// GRAFT arriving at this size is refused.
pub const GOSSIPSUB_D_HI: usize = 12;
/// Minimum number of non-mesh peers sent an `IHAVE` per heartbeat (`D_lazy`).
pub const GOSSIPSUB_D_LAZY: usize = 6;
/// Fraction of non-mesh peers sent an `IHAVE` per heartbeat when that exceeds
/// `D_lazy` (`gossip_factor`).
pub const GOSSIPSUB_GOSSIP_FACTOR: f64 = 0.25;
/// Heartbeats a message stays in the message cache and can answer an `IWANT`
/// (`history_length`).
pub const GOSSIPSUB_MCACHE_LEN: u32 = 5;
/// Newest heartbeats of the message cache whose ids are advertised in
/// `IHAVE` (`history_gossip`).
pub const GOSSIPSUB_MCACHE_GOSSIP: u32 = 3;
/// Mesh heartbeat period, milliseconds.
pub const GOSSIPSUB_HEARTBEAT_MS: u64 = 1_000;
/// Seconds a pruned peer must not be grafted again (`prune_backoff`).
pub const GOSSIPSUB_PRUNE_BACKOFF_SECS: u16 = 60;
/// Seconds a message id is remembered for duplicate suppression
/// (`seen_ttl`).
pub const GOSSIPSUB_SEEN_TTL_SECS: u64 = 120;
/// Hard bound on messages the cache holds, whatever the heartbeat window.
pub const GOSSIPSUB_MCACHE_CAPACITY: usize = 4_096;
/// Bytes reserved per cached message. A message is one datagram, so this is
/// `GOSSIP_MAX_PAYLOAD_BYTES`; a larger datagram is forwarded but cannot be
/// served to an `IWANT`.
pub const GOSSIPSUB_MCACHE_SLOT_BYTES: usize = GOSSIP_MAX_PAYLOAD_BYTES;
/// Hard bound on message ids remembered for duplicate suppression.
pub const GOSSIPSUB_SEEN_CAPACITY: usize = 32_768;
/// Most message ids one node requests from one peer per heartbeat with
/// `IWANT` (`max_ihave_length`, which go-libp2p sets to 5,000 for a TCP
/// stream). Over UDP the answer is a burst of datagrams, and the advertiser
/// can queue no more of them than its send pool holds
/// (`GOSSIP_SEND_QUEUE_CAPACITY`), so the cap matches it; the asked ids are a
/// random sample of the unseen ones, as in go-libp2p.
pub const GOSSIPSUB_MAX_IWANT_IDS: usize = GOSSIP_SEND_QUEUE_CAPACITY;
/// Cells per answer of the catch-up sweep, the study's `pull-sweep-join`
/// component (not part of GossipSub; see the design notes). A node that
/// finds most of an `IHAVE` unseen pulls the advertiser's live cells this
/// many at a time, about 18 full datagrams per round trip, well inside a
/// receive buffer. `0` turns the sweep off.
pub const GOSSIPSUB_SWEEP_SLICE_CELLS: usize = 512;

pub fn random_rng_seed() -> Result<u64, getrandom::Error> {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes)?;
    Ok(u64::from_ne_bytes(bytes))
}
