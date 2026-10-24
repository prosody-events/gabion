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

// -- Plumtree dissemination ---------------------------------------------------
//
// Parameters of the Plumtree overlay (`gossip::runtime::plumtree`). The first
// seven are the fields of `gossip::PlumtreeConfig`; the rest bound retries,
// time the repair digest and size bounded buffers. See "Design notes" in
// `crates/gabion/README.md`.

/// How long a lazily announced change may stay undelivered before the
/// announcer is sent a GRAFT, in milliseconds (Leitao et al., SRDS 2007).
pub const PLUMTREE_MISSING_TIMEOUT_MS: u64 = 500;

/// Lazy peers sent an IHAVE digest per heartbeat. The paper announces to every
/// lazy peer; capping it bounds the announcement bytes at large views.
pub const PLUMTREE_LAZY_TARGETS: usize = 2;

/// Every this many heartbeats an IHAVE digest lists every origin the peer is
/// lazy for, not only those that advanced since the last digest, so a lost
/// IHAVE costs at most this many heartbeats. `0` never sends the full digest.
pub const PLUMTREE_FULL_IHAVE_EVERY: u32 = 8;

/// Delay between a change arriving and the relay round that forwards it,
/// in milliseconds. Batches the changes of one burst into one frame per edge.
pub const PLUMTREE_RELAY_FLOOR_MS: u64 = 5;

/// How long a NACKed edge packet may stay missing before it is requested
/// again, in milliseconds.
pub const PLUMTREE_NACK_RETRY_MS: u64 = 100;

/// Random peers outside the tree that also receive each round's changes (the
/// "hybrid" variant). `0` is plain Plumtree.
pub const PLUMTREE_EXTRA_RANDOM: usize = 0;

/// Ask the first peer to show us an origin for that origin's live cells, so a
/// node that joins or restarts catches up without waiting for the repair
/// slice to sweep every cell.
pub const PLUMTREE_FIRST_CONTACT_PULL: bool = true;

/// GRAFTs sent for one origin's missing announcements, each to the next
/// announcer, before the record is dropped and the repair lane is left to
/// deliver. The paper retries without bound; a bound keeps a dead announcer
/// set from costing a GRAFT every `missing_timeout` forever.
pub const PLUMTREE_MAX_GRAFT_TRIES: u8 = 3;

/// NACKs sent for the packets missing from an edge, one per `nack_retry`,
/// before giving up on a quiet edge. A busy edge gives up on a packet sooner:
/// it leaves the 64-packet receive window.
pub const PLUMTREE_MAX_NACK_TRIES: u8 = 10;

/// How long a bucket must have been closed before a repair digest counts its
/// cells, in milliseconds. A change still on its way through the tree is not a
/// hole; the tree delivers within a few hundred milliseconds.
pub const PLUMTREE_DIGEST_SETTLE_MS: u64 = 1_000;

/// Most cells one GRAFT or PULL reply carries, about 80 full packets: a
/// joiner or a healed partition asks for whole origins at once, and a reply
/// much larger would overflow the requester's receive buffer.
pub const PLUMTREE_REPLY_CELLS: usize = 2_048;

/// Eager packets remembered per node so a NACK can be answered, and the most
/// cells one remembered packet keeps (a fuller packet cannot be resent).
pub const PLUMTREE_RESEND_PACKETS: usize = 512;
pub const PLUMTREE_RESEND_CELLS_PER_PACKET: usize = 48;

pub fn random_rng_seed() -> Result<u64, getrandom::Error> {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes)?;
    Ok(u64::from_ne_bytes(bytes))
}
