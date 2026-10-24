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

/// Default of `GossipConfig::fanout`, which has no effect: changes travel down
/// the Thicket trees (see the `THICKET_*` constants), not to a number of random
/// peers per tick.
pub const GOSSIP_FANOUT: usize = 3;

/// Coverage margin `c` in the law `⌈ln(n) + c⌉` that sizes each node's backbone
/// view (`n` = its class-mates; the baseline sized its fanout by it over all
/// peers). Per Kermarrec, Massoulié & Ganesh (IEEE TPDS 2003, "Probabilistic
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

/// Send slots, one pre-allocated packet buffer each. Tree dissemination
/// sends a round's changes to each tree neighbour in its own packets: about
/// `⌈ln(n/T)+c⌉` backbone neighbours, their leaf children and `T − 1` parents.
/// Sized so those frames queue whole in a burst; a smaller pool cuts the
/// tail of a frame (with a warning) and leaves it to the lazy IHAVE path.
pub const GOSSIP_SEND_QUEUE_CAPACITY: usize = 512;
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

// -- Thicket dissemination (Ferreira, Leitão & Rodrigues, SRDS 2010) --------

/// Number of trees `T`. Every node is interior (forwards) in exactly one
/// tree, chosen by hashing its address, and a leaf in the other `T − 1`.
/// Matched to the three availability zones the deployment spans.
pub const THICKET_TREES: usize = 3;

/// How long a change announced by IHAVE may stay undelivered before the
/// announcer is asked for it with a GRAFT (Plumtree's `timeout1`).
pub const THICKET_MISSING_TIMEOUT_MS: u64 = 500;

/// Peers sent the lazy IHAVE announcement of recent changes on each
/// heartbeat.
pub const THICKET_LAZY_TARGETS: usize = 2;

/// Minimum gap between two relay rounds. Changes merged from a tree
/// neighbour are forwarded in the next relay round, so this is the most a
/// relay hop waits to batch the changes that arrive together.
pub const THICKET_RELAY_FLOOR_MS: u64 = 5;

/// How long a lost eager packet is waited for before it is asked for again.
pub const THICKET_NACK_RETRY_MS: u64 = 100;

/// Times a lost eager packet is asked for before it is left to the lazy path.
pub const THICKET_NACK_ATTEMPTS: u8 = 3;

/// Cells of the slice of the store a catching-up node asks a peer for in one
/// request.
pub const THICKET_REPAIR_CELLS: usize = 256;

/// Newest settled buckets of a rule a repair digest covers. A bucket is
/// settled once it closed at least one bucket ago, until it leaves the window.
/// Each digest names, per origin, the sum of the counts held in each of these
/// buckets; a longer window is compared on its newest buckets only.
pub const THICKET_DIGEST_BUCKETS: usize = 64;

/// Most cells a node pushes to one peer in answer to its digests between two
/// heartbeats. The rest of what the peer lacks is pushed in answer to its
/// next digest, which, while repair brings it news, comes at its next
/// heartbeat.
pub const THICKET_DIGEST_PUSH_CELLS: usize = 1024;

/// Most undelivered announced changes a node tracks at once. A change over
/// the limit is left to the repair slice.
pub const THICKET_MISSING_CAPACITY: usize = 4096;

/// Times a GRAFT for an announced change is sent, one per missing timeout,
/// before the change is left to the repair slice.
pub const THICKET_GRAFT_ATTEMPTS: u8 = 3;

/// Heartbeats a PROBE is sent after the last eager packet to a peer. One: a
/// tail hidden by a lost probe is still announced by IHAVE and, failing that,
/// found by the repair digest, and in a quiet cluster probes were a third of
/// the bytes.
pub const THICKET_PROBE_REPEATS: u8 = 1;

/// Requests, one per heartbeat of silence, a node that joined empty makes for
/// its first slice of the cluster's cells before it relies on the repair
/// slice alone.
pub const THICKET_CATCH_UP_ATTEMPTS: u8 = 6;

/// Most neighbours in the backbone view of the tree a node is interior in.
pub const THICKET_VIEW_CAPACITY: usize = 32;

/// Fewest leaf children an interior node accepts before it refuses an attach
/// and the leaf picks another parent. The limit rises to twice the even share
/// of leaves per interior node when the classes are lopsided, so a tree with a
/// single interior node still takes every leaf.
pub const THICKET_LEAF_CHILDREN_MIN: usize = 8;

/// Eager packets kept for retransmission, across all tree neighbours.
pub const THICKET_RETX_PACKETS: usize = 512;

/// Cells of one packet kept for retransmission. A packet carries about 25.
pub const THICKET_RETX_CELLS: usize = 48;

/// Lost eager packets tracked per neighbour at once.
pub const THICKET_EDGE_GAPS: usize = 16;

pub fn random_rng_seed() -> Result<u64, getrandom::Error> {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes)?;
    Ok(u64::from_ne_bytes(bytes))
}
