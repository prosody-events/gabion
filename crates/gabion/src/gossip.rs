//! Gossip CRDT storage and wire codec.
//!
//! Invariants:
//! - Counter-cell identity is rule, key, bucket, origin node, and origin
//!   incarnation.
//! - Remote merges are monotonic per cell identity and never lower stored
//!   counts.
//! - CRDT merge is idempotent, commutative, and associative for equivalent cell
//!   sets.
//! - Dirty rings are bounded and report overflow instead of allocating.
//! - Encoders never write past the configured payload capacity.
//! - Decoders enforce payload and count limits before allocation-heavy
//!   decoding.
//! - Visitor decoding reports the same content as allocating decoding.
//! - Authenticated frames reject any payload or tag mutation before callbacks
//!   run.

use crate::RuleId;
use crate::core::{KeyHash, LocalEngine};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use thiserror::Error;
use twox_hash::xxhash3_128::{DEFAULT_SECRET_LENGTH, RawHasher as XxHash3RawHasher, SecretBuffer};

#[cfg(test)]
mod tests;

pub type CellId = usize;

const MAGIC: u32 = 0x4742_4731;
const VERSION: u16 = 1;
pub const GOSSIP_HEADER_LEN: usize = 76;
pub const GOSSIP_DIGEST_LEN: usize = 30;
pub const GOSSIP_CELL_LEN: usize = 72;
pub const GOSSIP_AUTH_TAG_LEN: usize = 32;
const HEADER_LEN: usize = GOSSIP_HEADER_LEN;
const DIGEST_LEN: usize = GOSSIP_DIGEST_LEN;
const CELL_LEN: usize = GOSSIP_CELL_LEN;
const AUTH_TAG_LEN: usize = GOSSIP_AUTH_TAG_LEN;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, serde::Serialize)]
pub struct NodeId(u128);

impl NodeId {
    pub fn value(self) -> u128 {
        self.0
    }
}

impl From<u128> for NodeId {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

impl From<NodeId> for u128 {
    fn from(value: NodeId) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize)]
pub struct CounterCell {
    pub rule_id: RuleId,
    pub key_hash: KeyHash,
    pub bucket_start_millis: i64,
    pub origin_node_id: NodeId,
    pub origin_incarnation: u64,
    pub count: u64,
    pub last_update_millis: u64,
    pub sequence: u64,
}

impl CounterCell {
    pub fn key_hash(self) -> KeyHash {
        self.key_hash
    }

    fn same_identity(self, other: Self) -> bool {
        self.rule_id == other.rule_id
            && self.key_hash == other.key_hash
            && self.bucket_start_millis == other.bucket_start_millis
            && self.origin_node_id == other.origin_node_id
            && self.origin_incarnation == other.origin_incarnation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirtyEntry {
    pub cell_id: CellId,
    pub sequence: u64,
}

#[derive(Clone, Debug)]
pub struct DirtyRing {
    entries: Vec<Option<DirtyEntry>>,
    next: usize,
    len: usize,
    overflowed: bool,
}

impl DirtyRing {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: vec![None; capacity],
            next: 0,
            len: 0,
            overflowed: false,
        }
    }

    pub fn push(&mut self, entry: DirtyEntry) {
        if self.entries.is_empty() {
            self.overflowed = true;
            return;
        }

        if self.len == self.entries.len() {
            self.overflowed = true;
        } else {
            self.len += 1;
        }

        self.entries[self.next] = Some(entry);
        self.next = (self.next + 1) % self.entries.len();
    }

    pub fn overflowed(&self) -> bool {
        self.overflowed
    }

    pub fn clear(&mut self) {
        for entry in self.entries.iter_mut() {
            *entry = None;
        }
        self.next = 0;
        self.len = 0;
        self.overflowed = false;
    }

    pub fn iter(&self) -> impl Iterator<Item = DirtyEntry> + '_ {
        let len = self.len;
        let start = if len == self.entries.len() {
            self.next
        } else {
            0
        };
        (0..len).filter_map(move |offset| self.entries[(start + offset) % self.entries.len()])
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[derive(Clone, Debug)]
pub struct CellTable {
    cells: Vec<Option<CounterCell>>,
    active: usize,
    next_sequence: u64,
    dirty: DirtyRing,
}

impl CellTable {
    pub fn with_capacity(max_cells: usize, dirty_capacity: usize) -> Self {
        Self {
            cells: vec![None; max_cells],
            active: 0,
            next_sequence: 0,
            dirty: DirtyRing::with_capacity(dirty_capacity),
        }
    }

    pub fn active_cell_count(&self) -> usize {
        self.active
    }

    pub fn dirty_overflowed(&self) -> bool {
        self.dirty.overflowed()
    }

    pub fn dirty_len(&self) -> usize {
        self.dirty.len()
    }

    pub fn dirty_capacity(&self) -> usize {
        self.dirty.capacity()
    }

    pub fn cells(&self) -> impl Iterator<Item = (CellId, CounterCell)> + '_ {
        self.cells
            .iter()
            .enumerate()
            .filter_map(|(id, cell)| cell.map(|cell| (id, cell)))
    }

    pub fn dirty_cells(&self) -> impl Iterator<Item = CounterCell> + '_ {
        self.dirty
            .iter()
            .filter_map(|dirty| self.cells.get(dirty.cell_id).and_then(|cell| *cell))
    }

    pub fn capacity(&self) -> usize {
        self.cells.len()
    }

    pub fn clear(&mut self) {
        for cell in self.cells.iter_mut() {
            *cell = None;
        }
        self.active = 0;
        self.next_sequence = 0;
        self.dirty.clear();
    }

    pub fn upsert_local(&mut self, cell: CounterCell) -> Result<CellId, CellTableFull> {
        self.upsert(cell, |stored, incoming| {
            stored.count = stored.count.saturating_add(incoming.count);
            stored.last_update_millis = incoming.last_update_millis;
            true
        })
    }

    pub fn merge_remote(
        &mut self,
        incoming: CounterCell,
        engine: Option<&mut LocalEngine>,
        now_millis: u64,
    ) -> Result<MergeOutcome, CellTableFull> {
        let existed = self.find_cell(incoming).is_some();
        let mut delta = if existed { 0 } else { incoming.count };
        let cell_id = self.upsert(incoming, |stored, incoming| {
            if incoming.count > stored.count {
                delta = incoming.count - stored.count;
                stored.count = incoming.count;
                stored.last_update_millis = incoming.last_update_millis;
                true
            } else {
                false
            }
        })?;

        if delta == 0 {
            return Ok(MergeOutcome {
                cell_id,
                delta: 0,
                changed: false,
            });
        }

        if let Some(engine) = engine {
            engine.add_remote_estimate(
                incoming.rule_id,
                incoming.key_hash(),
                incoming.bucket_start_millis.max(0) as u64,
                now_millis,
                delta,
            );
        }

        Ok(MergeOutcome {
            cell_id,
            delta,
            changed: true,
        })
    }

    fn upsert(
        &mut self,
        mut incoming: CounterCell,
        merge_existing: impl FnOnce(&mut CounterCell, CounterCell) -> bool,
    ) -> Result<CellId, CellTableFull> {
        if let Some(id) = self.find_cell(incoming) {
            let Some(stored) = self.cells.get_mut(id).and_then(Option::as_mut) else {
                return Err(CellTableFull);
            };
            let changed = merge_existing(stored, incoming);
            if changed {
                self.next_sequence = self.next_sequence.saturating_add(1);
                let sequence = self.next_sequence;
                if let Some(stored) = self.cells.get_mut(id).and_then(Option::as_mut) {
                    stored.sequence = sequence;
                }
                self.dirty.push(DirtyEntry {
                    cell_id: id,
                    sequence,
                });
            }
            return Ok(id);
        }

        let Some(id) = self.find_vacant() else {
            return Err(CellTableFull);
        };

        self.next_sequence = self.next_sequence.saturating_add(1);
        incoming.sequence = self.next_sequence;
        self.cells[id] = Some(incoming);
        self.active += 1;
        self.dirty.push(DirtyEntry {
            cell_id: id,
            sequence: incoming.sequence,
        });
        Ok(id)
    }

    fn find_cell(&self, incoming: CounterCell) -> Option<CellId> {
        self.cells().find_map(|(id, cell)| {
            if cell.same_identity(incoming) {
                Some(id)
            } else {
                None
            }
        })
    }

    fn find_vacant(&self) -> Option<CellId> {
        self.cells.iter().position(Option::is_none)
    }

    pub fn digest(&self, shard_id: u16, shard_count: u16) -> ShardDigest {
        digest_cells(self.cells().map(|(_id, cell)| cell), shard_id, shard_count)
    }
}

pub const DEFAULT_GOSSIP_LINGER_MS: u64 = 250;
pub fn max_cells_for_payload(
    max_payload_bytes: usize,
    digest_count: usize,
    authenticated: bool,
) -> usize {
    let auth_len = if authenticated {
        GOSSIP_AUTH_TAG_LEN
    } else {
        0
    };
    let Some(payload_without_auth) = max_payload_bytes.checked_sub(auth_len) else {
        return 0;
    };
    let Some(fixed_len) =
        GOSSIP_HEADER_LEN.checked_add(digest_count.saturating_mul(GOSSIP_DIGEST_LEN))
    else {
        return 0;
    };
    let Some(cell_bytes) = payload_without_auth.checked_sub(fixed_len) else {
        return 0;
    };
    cell_bytes / GOSSIP_CELL_LEN
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GossipSpaceUsage {
    pub active_cells: usize,
    pub max_cells: usize,
    pub dirty_cells: usize,
    pub dirty_capacity: usize,
    pub dirty_overflowed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GossipSendReason {
    DirtyOverflow,
    PacketFull,
    TimeElapsed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GossipSendPolicy {
    pub linger: std::time::Duration,
}

impl Default for GossipSendPolicy {
    fn default() -> Self {
        Self {
            linger: std::time::Duration::from_millis(DEFAULT_GOSSIP_LINGER_MS),
        }
    }
}

impl GossipSendPolicy {
    pub fn with_linger(linger: std::time::Duration) -> Self {
        Self {
            linger,
            ..Self::default()
        }
    }

    pub fn should_send(
        self,
        now_millis: u64,
        last_send_millis: u64,
        usage: GossipSpaceUsage,
    ) -> Option<GossipSendReason> {
        if usage.dirty_overflowed {
            return Some(GossipSendReason::DirtyOverflow);
        }
        if usage.max_cells != 0 && usage.active_cells >= usage.max_cells {
            return Some(GossipSendReason::PacketFull);
        }
        if now_millis.saturating_sub(last_send_millis) >= duration_millis(self.linger) {
            return Some(GossipSendReason::TimeElapsed);
        }
        None
    }
}

fn duration_millis(duration: std::time::Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX).max(1)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MergeOutcome {
    pub cell_id: CellId,
    pub delta: u64,
    pub changed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellTableFull;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShardDigest {
    pub shard_id: u16,
    pub active_cell_count: u32,
    pub max_sequence: u64,
    pub checksum: u128,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GossipHeader {
    pub cluster_id_hash: u128,
    pub sender_node_id: NodeId,
    pub sender_incarnation: u64,
    pub min_bucket: i64,
    pub max_bucket: i64,
    pub flags: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GossipMessage {
    pub header: GossipHeader,
    pub digests: Vec<ShardDigest>,
    pub cells: Vec<CounterCell>,
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedMessageSummary {
    pub header: GossipHeader,
    pub digest_count: usize,
    pub cell_count: usize,
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GossipLimits {
    pub max_payload_bytes: usize,
    pub max_digests: usize,
    pub max_cells: usize,
}

impl Default for GossipLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 256 * 1024,
            max_digests: 1024,
            max_cells: 4096,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct GossipMetrics {
    pub send_bytes: u64,
    pub recv_bytes: u64,
    pub merge_cells: u64,
    pub digest_mismatch: u64,
    pub truncated: u64,
    pub auth_failures: u64,
    pub decode_errors: u64,
    pub dirty_overflow: u64,
}

impl GossipMetrics {
    pub fn record_send(&mut self, bytes: usize, truncated: bool) {
        self.send_bytes = self.send_bytes.saturating_add(bytes as u64);
        if truncated {
            self.truncated = self.truncated.saturating_add(1);
        }
    }

    pub fn record_recv(&mut self, bytes: usize) {
        self.recv_bytes = self.recv_bytes.saturating_add(bytes as u64);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HmacKey {
    bytes: [u8; 32],
}

impl HmacKey {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    #[error("payload too short")]
    TooShort,
    #[error("bad magic")]
    BadMagic,
    #[error("bad version")]
    BadVersion,
    #[error("truncated payload")]
    Truncated,
    #[error("decode capacity exceeded")]
    CapacityExceeded,
    #[error("payload size exceeded")]
    PayloadTooLarge,
    #[error("authentication failed")]
    AuthenticationFailed,
}

pub fn encode_message(
    message: &GossipMessage,
    buffer: &mut Vec<u8>,
    max_payload_bytes: usize,
) -> bool {
    encode_message_parts(
        message.header,
        &message.digests,
        &message.cells,
        message.truncated,
        buffer,
        max_payload_bytes,
    )
}

pub fn encode_message_parts(
    header: GossipHeader,
    digests: &[ShardDigest],
    cells: &[CounterCell],
    truncated: bool,
    buffer: &mut Vec<u8>,
    max_payload_bytes: usize,
) -> bool {
    buffer.clear();
    let digest_count = digests.len().min(u16::MAX as usize);
    let max_cells_by_count = cells.len().min(u32::MAX as usize);
    let fixed_len = HEADER_LEN + digest_count * DIGEST_LEN;
    if fixed_len > max_payload_bytes {
        return true;
    }
    let max_cells_by_size = (max_payload_bytes - fixed_len) / CELL_LEN;
    let cell_count = max_cells_by_count.min(max_cells_by_size);
    let truncated = truncated || cell_count < cells.len();
    let flags = if truncated {
        header.flags | 1
    } else {
        header.flags
    };

    put_u32(buffer, MAGIC);
    put_u16(buffer, VERSION);
    put_u16(buffer, digest_count as u16);
    put_u32(buffer, cell_count as u32);
    put_u128(buffer, header.cluster_id_hash);
    put_u128(buffer, header.sender_node_id.into());
    put_u64(buffer, header.sender_incarnation);
    put_i64(buffer, header.min_bucket);
    put_i64(buffer, header.max_bucket);
    put_u32(buffer, flags);
    put_u32(buffer, 0);

    for digest in digests.iter().take(digest_count) {
        put_u16(buffer, digest.shard_id);
        put_u32(buffer, digest.active_cell_count);
        put_u64(buffer, digest.max_sequence);
        put_u128(buffer, digest.checksum);
    }

    for cell in cells.iter().take(cell_count) {
        put_u64(buffer, u64::from(cell.rule_id));
        put_u128(buffer, cell.key_hash.into());
        put_i64(buffer, cell.bucket_start_millis);
        put_u128(buffer, cell.origin_node_id.into());
        put_u64(buffer, cell.origin_incarnation);
        put_u64(buffer, cell.count);
        put_u64(buffer, cell.last_update_millis);
    }

    truncated
}

pub fn encode_authenticated_message(
    message: &GossipMessage,
    key: HmacKey,
    buffer: &mut Vec<u8>,
    limits: GossipLimits,
) -> bool {
    encode_authenticated_message_parts(
        message.header,
        &message.digests,
        &message.cells,
        message.truncated,
        key,
        buffer,
        limits,
    )
}

pub fn encode_authenticated_message_parts(
    header: GossipHeader,
    digests: &[ShardDigest],
    cells: &[CounterCell],
    truncated: bool,
    key: HmacKey,
    buffer: &mut Vec<u8>,
    limits: GossipLimits,
) -> bool {
    if limits.max_payload_bytes < AUTH_TAG_LEN {
        buffer.clear();
        return true;
    }

    let truncated = encode_message_parts(
        header,
        digests,
        cells,
        truncated,
        buffer,
        limits.max_payload_bytes - AUTH_TAG_LEN,
    );
    let tag = hmac_sha256(key, buffer);
    buffer.extend_from_slice(&tag);
    truncated
}

pub fn decode_message(
    bytes: &[u8],
    max_digests: usize,
    max_cells: usize,
) -> Result<GossipMessage, DecodeError> {
    decode_message_with_limits(
        bytes,
        GossipLimits {
            max_payload_bytes: usize::MAX,
            max_digests,
            max_cells,
        },
    )
}

pub fn decode_message_with_limits(
    bytes: &[u8],
    limits: GossipLimits,
) -> Result<GossipMessage, DecodeError> {
    if bytes.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }
    if bytes.len() < HEADER_LEN {
        return Err(DecodeError::TooShort);
    }

    let mut cursor = 0;
    let magic = take_u32(bytes, &mut cursor)?;
    if magic != MAGIC {
        return Err(DecodeError::BadMagic);
    }
    let version = take_u16(bytes, &mut cursor)?;
    if version != VERSION {
        return Err(DecodeError::BadVersion);
    }
    let digest_count = take_u16(bytes, &mut cursor)? as usize;
    let cell_count = take_u32(bytes, &mut cursor)? as usize;
    if digest_count > limits.max_digests || cell_count > limits.max_cells {
        return Err(DecodeError::CapacityExceeded);
    }

    let header = GossipHeader {
        cluster_id_hash: take_u128(bytes, &mut cursor)?,
        sender_node_id: take_u128(bytes, &mut cursor)?.into(),
        sender_incarnation: take_u64(bytes, &mut cursor)?,
        min_bucket: take_i64(bytes, &mut cursor)?,
        max_bucket: take_i64(bytes, &mut cursor)?,
        flags: take_u32(bytes, &mut cursor)?,
    };
    let _reserved = take_u32(bytes, &mut cursor)?;

    let mut digests = Vec::with_capacity(digest_count);
    for _ in 0..digest_count {
        digests.push(ShardDigest {
            shard_id: take_u16(bytes, &mut cursor)?,
            active_cell_count: take_u32(bytes, &mut cursor)?,
            max_sequence: take_u64(bytes, &mut cursor)?,
            checksum: take_u128(bytes, &mut cursor)?,
        });
    }

    let mut cells = Vec::with_capacity(cell_count);
    for _ in 0..cell_count {
        cells.push(CounterCell {
            rule_id: take_u64(bytes, &mut cursor)? as RuleId,
            key_hash: take_u128(bytes, &mut cursor)?.into(),
            bucket_start_millis: take_i64(bytes, &mut cursor)?,
            origin_node_id: take_u128(bytes, &mut cursor)?.into(),
            origin_incarnation: take_u64(bytes, &mut cursor)?,
            count: take_u64(bytes, &mut cursor)?,
            last_update_millis: take_u64(bytes, &mut cursor)?,
            sequence: 0,
        });
    }

    if cursor != bytes.len() {
        return Err(DecodeError::Truncated);
    }

    Ok(GossipMessage {
        truncated: header.flags & 1 == 1,
        header,
        digests,
        cells,
    })
}

pub fn decode_message_visit(
    bytes: &[u8],
    limits: GossipLimits,
    on_digest: impl FnMut(ShardDigest),
    on_cell: impl FnMut(CounterCell),
) -> Result<DecodedMessageSummary, DecodeError> {
    decode_message_visit_checked(bytes, limits, |_| true, on_digest, on_cell)
}

pub fn decode_message_visit_checked(
    bytes: &[u8],
    limits: GossipLimits,
    accept_header: impl FnOnce(GossipHeader) -> bool,
    mut on_digest: impl FnMut(ShardDigest),
    mut on_cell: impl FnMut(CounterCell),
) -> Result<DecodedMessageSummary, DecodeError> {
    if bytes.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }
    if bytes.len() < HEADER_LEN {
        return Err(DecodeError::TooShort);
    }

    let mut cursor = 0;
    let magic = take_u32(bytes, &mut cursor)?;
    if magic != MAGIC {
        return Err(DecodeError::BadMagic);
    }
    let version = take_u16(bytes, &mut cursor)?;
    if version != VERSION {
        return Err(DecodeError::BadVersion);
    }
    let digest_count = take_u16(bytes, &mut cursor)? as usize;
    let cell_count = take_u32(bytes, &mut cursor)? as usize;
    if digest_count > limits.max_digests || cell_count > limits.max_cells {
        return Err(DecodeError::CapacityExceeded);
    }

    let header = GossipHeader {
        cluster_id_hash: take_u128(bytes, &mut cursor)?,
        sender_node_id: take_u128(bytes, &mut cursor)?.into(),
        sender_incarnation: take_u64(bytes, &mut cursor)?,
        min_bucket: take_i64(bytes, &mut cursor)?,
        max_bucket: take_i64(bytes, &mut cursor)?,
        flags: take_u32(bytes, &mut cursor)?,
    };
    let _reserved = take_u32(bytes, &mut cursor)?;
    if !accept_header(header) {
        return Ok(DecodedMessageSummary {
            header,
            digest_count,
            cell_count,
            truncated: header.flags & 1 == 1,
        });
    }

    for _ in 0..digest_count {
        on_digest(ShardDigest {
            shard_id: take_u16(bytes, &mut cursor)?,
            active_cell_count: take_u32(bytes, &mut cursor)?,
            max_sequence: take_u64(bytes, &mut cursor)?,
            checksum: take_u128(bytes, &mut cursor)?,
        });
    }

    for _ in 0..cell_count {
        on_cell(CounterCell {
            rule_id: take_u64(bytes, &mut cursor)? as RuleId,
            key_hash: take_u128(bytes, &mut cursor)?.into(),
            bucket_start_millis: take_i64(bytes, &mut cursor)?,
            origin_node_id: take_u128(bytes, &mut cursor)?.into(),
            origin_incarnation: take_u64(bytes, &mut cursor)?,
            count: take_u64(bytes, &mut cursor)?,
            last_update_millis: take_u64(bytes, &mut cursor)?,
            sequence: 0,
        });
    }

    if cursor != bytes.len() {
        return Err(DecodeError::Truncated);
    }

    Ok(DecodedMessageSummary {
        header,
        digest_count,
        cell_count,
        truncated: header.flags & 1 == 1,
    })
}

pub fn decode_authenticated_message(
    bytes: &[u8],
    key: HmacKey,
    limits: GossipLimits,
) -> Result<GossipMessage, DecodeError> {
    if bytes.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }
    if bytes.len() < AUTH_TAG_LEN {
        return Err(DecodeError::TooShort);
    }

    let payload_len = bytes.len() - AUTH_TAG_LEN;
    let (payload, received_tag) = bytes.split_at(payload_len);
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.bytes)
        .map_err(|_| DecodeError::AuthenticationFailed)?;
    mac.update(payload);
    mac.verify_slice(received_tag)
        .map_err(|_| DecodeError::AuthenticationFailed)?;

    decode_message_with_limits(payload, limits)
}

pub fn decode_authenticated_message_visit_checked(
    bytes: &[u8],
    key: HmacKey,
    limits: GossipLimits,
    accept_header: impl FnOnce(GossipHeader) -> bool,
    on_digest: impl FnMut(ShardDigest),
    on_cell: impl FnMut(CounterCell),
) -> Result<DecodedMessageSummary, DecodeError> {
    if bytes.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }
    if bytes.len() < AUTH_TAG_LEN {
        return Err(DecodeError::TooShort);
    }

    let payload_len = bytes.len() - AUTH_TAG_LEN;
    let (payload, received_tag) = bytes.split_at(payload_len);
    let mut mac = Hmac::<Sha256>::new_from_slice(&key.bytes)
        .map_err(|_| DecodeError::AuthenticationFailed)?;
    mac.update(payload);
    mac.verify_slice(received_tag)
        .map_err(|_| DecodeError::AuthenticationFailed)?;

    decode_message_visit_checked(payload, limits, accept_header, on_digest, on_cell)
}

fn shard_for(cell: CounterCell, shard_count: u16) -> u16 {
    if shard_count == 0 {
        return 0;
    }
    cell.key_hash.value() as u16 % shard_count
}

fn cell_checksum(cell: CounterCell) -> u128 {
    let secret =
        SecretBuffer::new(0, [0x9d; DEFAULT_SECRET_LENGTH]).expect("valid XXH3 secret length");
    let mut hasher = XxHash3RawHasher::new(secret);
    hasher.write(&u64::from(cell.rule_id).to_le_bytes());
    hasher.write(&cell.key_hash.value().to_le_bytes());
    hasher.write(&cell.bucket_start_millis.to_le_bytes());
    hasher.write(&cell.origin_node_id.value().to_le_bytes());
    hasher.write(&cell.origin_incarnation.to_le_bytes());
    hasher.write(&cell.count.to_le_bytes());
    hasher.finish_128()
}

pub fn digest_cells(
    cells: impl IntoIterator<Item = CounterCell>,
    shard_id: u16,
    shard_count: u16,
) -> ShardDigest {
    let mut checksum = 0_u128;
    let mut active_cell_count = 0_u32;
    let mut max_sequence = 0_u64;

    for cell in cells {
        if shard_for(cell, shard_count) != shard_id {
            continue;
        }
        active_cell_count = active_cell_count.saturating_add(1);
        max_sequence = max_sequence.max(cell.sequence);
        checksum ^= cell_checksum(cell);
    }

    ShardDigest {
        shard_id,
        active_cell_count,
        max_sequence,
        checksum,
    }
}

fn put_u16(buffer: &mut Vec<u8>, value: u16) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(buffer: &mut Vec<u8>, value: u32) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(buffer: &mut Vec<u8>, value: u64) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn put_u128(buffer: &mut Vec<u8>, value: u128) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(buffer: &mut Vec<u8>, value: i64) {
    buffer.extend_from_slice(&value.to_le_bytes());
}

fn take_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, DecodeError> {
    let end = cursor.saturating_add(2);
    let Some(slice) = bytes.get(*cursor..end) else {
        return Err(DecodeError::Truncated);
    };
    *cursor = end;
    let mut value = [0_u8; 2];
    value.copy_from_slice(slice);
    Ok(u16::from_le_bytes(value))
}

fn take_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, DecodeError> {
    let end = cursor.saturating_add(4);
    let Some(slice) = bytes.get(*cursor..end) else {
        return Err(DecodeError::Truncated);
    };
    *cursor = end;
    let mut value = [0_u8; 4];
    value.copy_from_slice(slice);
    Ok(u32::from_le_bytes(value))
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, DecodeError> {
    let end = cursor.saturating_add(8);
    let Some(slice) = bytes.get(*cursor..end) else {
        return Err(DecodeError::Truncated);
    };
    *cursor = end;
    let mut value = [0_u8; 8];
    value.copy_from_slice(slice);
    Ok(u64::from_le_bytes(value))
}

fn take_u128(bytes: &[u8], cursor: &mut usize) -> Result<u128, DecodeError> {
    let end = cursor.saturating_add(16);
    let Some(slice) = bytes.get(*cursor..end) else {
        return Err(DecodeError::Truncated);
    };
    *cursor = end;
    let mut value = [0_u8; 16];
    value.copy_from_slice(slice);
    Ok(u128::from_le_bytes(value))
}

fn take_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64, DecodeError> {
    Ok(take_u64(bytes, cursor)? as i64)
}

fn hmac_sha256(key: HmacKey, payload: &[u8]) -> [u8; AUTH_TAG_LEN] {
    let mut mac = match Hmac::<Sha256>::new_from_slice(&key.bytes) {
        Ok(mac) => mac,
        Err(_) => return [0_u8; AUTH_TAG_LEN],
    };
    mac.update(payload);
    mac.finalize().into_bytes().into()
}
