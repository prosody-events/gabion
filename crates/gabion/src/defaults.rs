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

/// Unused by the zone-hierarchical runtime, which sends to every zone peer and
/// one gateway per remote zone instead of sampling a fanout. Kept only so the
/// server config and the legacy bench scenarios still compile.
pub const GOSSIP_FANOUT: usize = 3;

/// Unused by the zone-hierarchical runtime (see [`GOSSIP_FANOUT`]). Coverage
/// margin `c` in the gossip fanout law `⌈ln(n) + c⌉` (`n` = live peer
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

/// Prefix length, in bits, of the peer-address block that names a zone (an
/// availability zone). Peers whose gossip addresses share this prefix are in
/// one zone. `16` assumes one subnet per zone, as with per-AZ VPC subnets; it
/// stands in for the EndpointSlice `endpoints[].zone` label, which
/// `PeerEvent` does not carry.
pub const GOSSIP_ZONE_PREFIX_LEN: u8 = 16;

/// Floor, in milliseconds, on a stream's retransmission timeout. The timeout
/// is `max(2 * RTT_est, GOSSIP_RTO_FLOOR_MS)`; the floor is one heartbeat
/// tick, since a peer with nothing to send acks at its next tick.
pub const GOSSIP_RTO_FLOOR_MS: u64 = 500;

/// Minimum gap, in milliseconds, between two relay rounds: a gateway that
/// receives cells from another zone forwards them into its own zone at the
/// next relay round rather than waiting for the heartbeat, and no more than
/// once per this interval so that arrivals within it share one packet per
/// local peer.
pub const GOSSIP_RELAY_FLOOR_MS: u64 = 5;

/// Members of its zone a node asks for their live window when it joins, or
/// when a zone member leaves before ever reaching it (`pull-sweep-join`).
pub const GOSSIP_SWEEP_PEERS: usize = 2;

/// Cells a node sends per prompt round while answering a sweep request, so a
/// window of tens of thousands of cells reaches the requester as a paced
/// stream (about eight packets every `GOSSIP_RELAY_FLOOR_MS`) rather than a
/// burst that would overflow its receive buffer.
pub const GOSSIP_SWEEP_CELLS_PER_ROUND: usize = 256;

/// Sweep requests a node answers at once; more are dropped, and their
/// requesters still hear from their other sweep peers.
pub const GOSSIP_SWEEP_CONCURRENCY: usize = 4;

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
