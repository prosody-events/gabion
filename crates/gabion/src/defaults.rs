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
/// Reliable Dissemination in Large-Scale Systems", Theorem 1), spreading a
/// message by infect-and-forward-once (each node forwards it once, to
/// `fanout` random peers) with mean fanout `ln(n) + c` reaches every node with
/// probability → `e^(−e^(−c))`:
///
/// | `c`   | P(one change reaches every node) |
/// |-------|----------------------------------|
/// | **3** | **95.1 %**                       |
/// | 4     | 98.2 %                           |
/// | 5     | 99.3 %                           |
///
/// The runtime runs this model directly: each change goes out once, to one
/// round's picks, and each node that merges it forwards it once in its next
/// round. So at `c = 3` a change misses at least one node ≈ 4.9 % of the time,
/// and the expected number of nodes it misses is `e^(−c)` ≈ 0.05. Two things
/// close that gap without a wider fanout: the next change to the same cell
/// carries its whole count, and the heartbeat repair slice
/// ([`GOSSIP_REPAIR_CELLS_PER_TICK`]) resends every live cell once per
/// rotation. Packet loss and cut frames push the real miss rate above the
/// theoretical one, and repair covers those too. So we size to `c = 3` rather
/// than KMG's validated single-shot `c ≈ 4` (fanout 13 @ 10 000 nodes, 15 @
/// 50 000): it costs ≈ 1 fewer peer per round at every cluster size (≈ 10 %
/// less change traffic). Kept a `const`; promote to a `GossipConfig` field if
/// an operator ever needs to trade bandwidth for coverage at runtime.
pub const GOSSIP_COVERAGE_MARGIN: f64 = 3.0;

pub const GOSSIP_MAX_PAYLOAD_BYTES: usize = 1400;
pub const GOSSIP_MAX_CELLS_PER_FRAME: u32 = 4096;
pub const GOSSIP_MAX_CELLS_PER_TICK: usize = 4096;

/// Send slots, one pre-allocated packet buffer each (about 700 KiB at the
/// default payload). A round shares them evenly across its picks, so this
/// bounds the packets one round can queue: `⌈ln(n)+c⌉` picks (9 at 300
/// peers) times each frame's packets. Sized so a round's frames fit whole at
/// 300 peers and 1,800 changes a second across the cluster: about 1,000 cells
/// per frame, plus the repair slice on one pick, within a 56-packet share. A
/// smaller pool still reaches every pick but cuts each frame's tail: repair
/// first, then the oldest changes, with a warning when changes are cut.
pub const GOSSIP_SEND_QUEUE_CAPACITY: usize = 512;

/// Cells of the rotating repair slice a heartbeat sends, to one random pick.
/// Threshold rounds send none.
///
/// A round sends each change once, so repair is the only resend: for a
/// change every forwarder lost, one cut from a full frame or pruned by the
/// peer frontier, one dropped while a node had no peers, or state a new peer
/// missed. How much that matters depends on the regime:
///
/// - **Large cluster, short window** (300 peers, 10 s): each change reaches
///   each node through about `⌈ln(n)+c⌉` forwarders, and the next change to a
///   cell carries its whole count, so a lost packet rarely matters. A sweep of
///   ~15k live cells at this rate takes longer than the window, so repair is a
///   backstop costing ~5% of gossip bandwidth.
/// - **Small cluster or long window**: fewer forwarders, or cells that stop
///   changing long before they expire. A sweep of a few thousand cells takes
///   seconds, well inside the window, and the bandwidth is small.
///
/// A fixed budget therefore favours novelty where repair cannot pay off and
/// converges where it can. Kept a `const`; promote it to a `GossipConfig`
/// field if an operator needs a different trade.
pub const GOSSIP_REPAIR_CELLS_PER_TICK: usize = 256;
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
