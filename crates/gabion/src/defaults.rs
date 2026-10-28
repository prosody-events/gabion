//! Production defaults shared by `gabiond` and the nginx adapter.
//!
//! The low-level library types may keep smaller construction defaults for
//! tests and examples. Server-facing binaries should use these values.

pub const STORAGE_MAX_CELLS: usize = 131_072;
/// Floor of the rule dictionary's default capacity, `max(256, 2 × rules)`;
/// `storage.rule_dictionary_capacity` overrides it.
pub const STORAGE_RULE_DICTIONARY_FLOOR: u16 = 256;
pub const STORAGE_NODE_DICTIONARY_CAPACITY: u16 = 1024;
pub const STORAGE_LOCAL_DIRTY_CAPACITY: usize = 65_536;
pub const STORAGE_FORWARDED_DIRTY_CAPACITY: usize = 524_288;
pub const STORAGE_PEER_CAPACITY: u16 = 256;

pub const STORAGE_MAX_DESCRIPTOR_COUNT: u16 = 16;
pub const STORAGE_MAX_DESCRIPTOR_BYTES: usize = 512;
pub const STORAGE_MAX_KEY_BYTES: usize = 128;

/// Default of `storage.max_matched_rules`, the most rules one request is
/// counted against. A request that matches more is **allowed** and the rest
/// under-count: gabiond checks one rule past the bound, stops, and logs
/// `matched_rule_overflow`; NGINX still checks every rule but queues counts
/// for only the first this-many and bumps the SHM `matched_rule_overflows`
/// counter.
pub const STORAGE_MAX_MATCHED_RULES: u16 = 16;

/// Default of `storage.max_request_descriptors`, the most descriptors of one
/// Envoy request gabiond evaluates; the rest are rejected as
/// `cardinality_rejected`.
pub const STORAGE_MAX_REQUEST_DESCRIPTORS: u16 = 16;

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
/// `const`; promote to a setting in `gabion::settings` if an operator ever
/// needs to trade bandwidth for coverage at runtime.
pub const GOSSIP_COVERAGE_MARGIN: f64 = 3.0;

pub const GOSSIP_MAX_PAYLOAD_BYTES: usize = 1400;
pub const GOSSIP_MAX_CELLS_PER_FRAME: u32 = 4096;
pub const GOSSIP_MAX_CELLS_PER_TICK: usize = 4096;
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

pub fn random_rng_seed() -> Result<u64, getrandom::Error> {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes)?;
    Ok(u64::from_ne_bytes(bytes))
}
