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

/// Unused by owner aggregation, which has no random fanout. Kept as the
/// default of the `fanout` setting.
pub const GOSSIP_FANOUT: usize = 3;

pub const GOSSIP_MAX_PAYLOAD_BYTES: usize = 1400;
pub const GOSSIP_MAX_CELLS_PER_FRAME: u32 = 4096;
pub const GOSSIP_MAX_CELLS_PER_TICK: usize = 4096;
pub const GOSSIP_SEND_QUEUE_CAPACITY: usize = 128;
pub const GOSSIP_LIMIT_QUEUE_CAPACITY: usize = 8192;
pub const GOSSIP_CLUSTER_ID_HASH: u128 = 1;

/// How often a node sends the hits it has recorded to the owner of their
/// keys, in milliseconds (`sync_wait`). Gubernator batches hits on a
/// sub-millisecond window to coalesce requests; that is not a staleness
/// bound, so the cadence here is chosen from the sub-second staleness target
/// instead. Unacknowledged hits are re-sent on the same cadence.
pub const GOSSIP_SYNC_WAIT_MS: u64 = 100;

/// How often a key's owner broadcasts the totals of the key buckets that
/// changed since its last broadcast to every peer, in milliseconds
/// (`broadcast_interval`). Chosen with [`GOSSIP_SYNC_WAIT_MS`]: a hit reaches
/// every peer within about one sync wait plus one broadcast interval.
pub const GOSSIP_BROADCAST_INTERVAL_MS: u64 = 100;

/// Most key buckets an owner puts in one broadcast (after Gubernator's
/// `GlobalBatchLimit`). Changed buckets beyond it wait for the next
/// interval.
pub const GOSSIP_BROADCAST_BATCH_LIMIT: usize = 1000;

/// Share of its limit, in percent, at which a key's hits are sent to its
/// owner, and its owner's totals broadcast, at once instead of at the next
/// `sync_wait` or `broadcast_interval`. A key far below its limit cannot be
/// over-admitted by a stale count, so it rides the batched cadence; a key
/// near its limit is where staleness turns into over-admission.
pub const GOSSIP_URGENT_PERCENT_OF_LIMIT: u64 = 50;

/// Broadcast packets an owner remembers the key buckets of, so it can
/// re-send one a receiver reports missing. A receiver that misses an older
/// one gets a snapshot of every key the owner owns instead. At 10 packets a
/// second this covers about 25 s.
pub const GOSSIP_BROADCAST_HISTORY: usize = 256;

/// Most owners a node asks for a snapshot at one sync. A joining node asks
/// every owner; pacing the requests keeps the replies from overflowing its
/// receive buffer (about 150 full packets at Linux's default).
pub const GOSSIP_SNAPSHOT_REQUESTS_PER_SYNC: usize = 32;

/// Unused by owner aggregation, which has no threshold-triggered emit. Kept
/// as the default of the `target_err_bps` setting.
pub const GOSSIP_TARGET_ERR_BPS: u32 = 100;

/// Unused by owner aggregation, which has no threshold-triggered emit. Kept
/// as the default of the `min_emit_interval` setting.
pub const GOSSIP_MIN_EMIT_INTERVAL_MS: u64 = 5;

pub fn random_rng_seed() -> Result<u64, getrandom::Error> {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes)?;
    Ok(u64::from_ne_bytes(bytes))
}
