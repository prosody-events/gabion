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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use gabion_core::{KeyHash, LocalEngine, RuleId};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use thiserror::Error;

pub type CellId = usize;

const MAGIC: u32 = 0x4742_4731;
const VERSION: u16 = 1;
const HEADER_LEN: usize = 68;
const DIGEST_LEN: usize = 18;
const CELL_LEN: usize = 72;
const AUTH_TAG_LEN: usize = 32;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, serde::Serialize)]
pub struct NodeId {
    pub hi: u64,
    pub lo: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize)]
pub struct CounterCell {
    pub rule_id: RuleId,
    pub key_hash_hi: u64,
    pub key_hash_lo: u64,
    pub bucket_start_millis: i64,
    pub origin_node_id: NodeId,
    pub origin_incarnation: u64,
    pub count: u64,
    pub last_update_millis: u64,
    pub sequence: u64,
}

impl CounterCell {
    pub fn key_hash(self) -> KeyHash {
        KeyHash::from_parts(self.key_hash_hi, self.key_hash_lo)
    }

    fn same_identity(self, other: Self) -> bool {
        self.rule_id == other.rule_id
            && self.key_hash_hi == other.key_hash_hi
            && self.key_hash_lo == other.key_hash_lo
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
    pub checksum: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GossipHeader {
    pub cluster_id_hash: u64,
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
    buffer.clear();
    let digest_count = message.digests.len().min(u16::MAX as usize);
    let max_cells_by_count = message.cells.len().min(u32::MAX as usize);
    let fixed_len = HEADER_LEN + digest_count * DIGEST_LEN;
    if fixed_len > max_payload_bytes {
        return true;
    }
    let max_cells_by_size = (max_payload_bytes - fixed_len) / CELL_LEN;
    let cell_count = max_cells_by_count.min(max_cells_by_size);
    let truncated = message.truncated || cell_count < message.cells.len();
    let flags = if truncated {
        message.header.flags | 1
    } else {
        message.header.flags
    };

    put_u32(buffer, MAGIC);
    put_u16(buffer, VERSION);
    put_u16(buffer, digest_count as u16);
    put_u32(buffer, cell_count as u32);
    put_u64(buffer, message.header.cluster_id_hash);
    put_u64(buffer, message.header.sender_node_id.hi);
    put_u64(buffer, message.header.sender_node_id.lo);
    put_u64(buffer, message.header.sender_incarnation);
    put_i64(buffer, message.header.min_bucket);
    put_i64(buffer, message.header.max_bucket);
    put_u32(buffer, flags);
    put_u32(buffer, 0);

    for digest in message.digests.iter().take(digest_count) {
        put_u16(buffer, digest.shard_id);
        put_u32(buffer, digest.active_cell_count);
        put_u64(buffer, digest.max_sequence);
        put_u64(buffer, digest.checksum);
    }

    for cell in message.cells.iter().take(cell_count) {
        put_u64(buffer, u64::from(cell.rule_id));
        put_u64(buffer, cell.key_hash_hi);
        put_u64(buffer, cell.key_hash_lo);
        put_i64(buffer, cell.bucket_start_millis);
        put_u64(buffer, cell.origin_node_id.hi);
        put_u64(buffer, cell.origin_node_id.lo);
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
    if limits.max_payload_bytes < AUTH_TAG_LEN {
        buffer.clear();
        return true;
    }

    let truncated = encode_message(message, buffer, limits.max_payload_bytes - AUTH_TAG_LEN);
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
        cluster_id_hash: take_u64(bytes, &mut cursor)?,
        sender_node_id: NodeId {
            hi: take_u64(bytes, &mut cursor)?,
            lo: take_u64(bytes, &mut cursor)?,
        },
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
            checksum: take_u64(bytes, &mut cursor)?,
        });
    }

    let mut cells = Vec::with_capacity(cell_count);
    for _ in 0..cell_count {
        cells.push(CounterCell {
            rule_id: take_u64(bytes, &mut cursor)? as RuleId,
            key_hash_hi: take_u64(bytes, &mut cursor)?,
            key_hash_lo: take_u64(bytes, &mut cursor)?,
            bucket_start_millis: take_i64(bytes, &mut cursor)?,
            origin_node_id: NodeId {
                hi: take_u64(bytes, &mut cursor)?,
                lo: take_u64(bytes, &mut cursor)?,
            },
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
        cluster_id_hash: take_u64(bytes, &mut cursor)?,
        sender_node_id: NodeId {
            hi: take_u64(bytes, &mut cursor)?,
            lo: take_u64(bytes, &mut cursor)?,
        },
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
            checksum: take_u64(bytes, &mut cursor)?,
        });
    }

    for _ in 0..cell_count {
        on_cell(CounterCell {
            rule_id: take_u64(bytes, &mut cursor)? as RuleId,
            key_hash_hi: take_u64(bytes, &mut cursor)?,
            key_hash_lo: take_u64(bytes, &mut cursor)?,
            bucket_start_millis: take_i64(bytes, &mut cursor)?,
            origin_node_id: NodeId {
                hi: take_u64(bytes, &mut cursor)?,
                lo: take_u64(bytes, &mut cursor)?,
            },
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
    (cell.key_hash_hi ^ cell.key_hash_lo) as u16 % shard_count
}

fn cell_checksum(cell: CounterCell) -> u64 {
    let mut hasher = DefaultHasher::new();
    cell.rule_id.hash(&mut hasher);
    cell.key_hash_hi.hash(&mut hasher);
    cell.key_hash_lo.hash(&mut hasher);
    cell.bucket_start_millis.hash(&mut hasher);
    cell.origin_node_id.hash(&mut hasher);
    cell.origin_incarnation.hash(&mut hasher);
    cell.count.hash(&mut hasher);
    hasher.finish()
}

pub fn digest_cells(
    cells: impl IntoIterator<Item = CounterCell>,
    shard_id: u16,
    shard_count: u16,
) -> ShardDigest {
    let mut checksum = 0_u64;
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

#[cfg(test)]
mod tests {
    use super::*;
    use gabion_core::{
        Decision, Descriptor, DescriptorMatcher, EnforcementMode, LimitRequest, LocalEngine,
        OverflowPolicy, Rule, RuleTable, SafetyMargin, WindowSpec, hash_domain, hash_key,
    };
    use quickcheck::{Arbitrary, Gen, TestResult};
    use quickcheck_macros::quickcheck;

    #[derive(Clone, Debug)]
    struct CodecRoundTripCase {
        digest_count: u8,
        cell_count: u8,
    }

    impl Arbitrary for CodecRoundTripCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                digest_count: u8::arbitrary(g) % 8,
                cell_count: u8::arbitrary(g) % 32,
            }
        }
    }

    #[derive(Clone, Debug)]
    struct AuthMutationCase {
        cell_count: u8,
        mutation_index: u16,
    }

    impl Arbitrary for AuthMutationCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                cell_count: (u8::arbitrary(g) % 8).max(1),
                mutation_index: u16::arbitrary(g),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct MergeLawCase {
        counts: Vec<u8>,
        origins: Vec<u8>,
    }

    impl Arbitrary for MergeLawCase {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut counts = Vec::<u8>::arbitrary(g);
            let mut origins = Vec::<u8>::arbitrary(g);
            counts.truncate(24);
            origins.truncate(24);
            Self { counts, origins }
        }
    }

    #[derive(Clone, Debug)]
    struct DirtyRingCase {
        dirty_capacity: u8,
        cell_count: u8,
    }

    impl Arbitrary for DirtyRingCase {
        fn arbitrary(g: &mut Gen) -> Self {
            Self {
                dirty_capacity: u8::arbitrary(g) % 8,
                cell_count: (u8::arbitrary(g) % 16).max(1),
            }
        }
    }

    fn cell(count: u64, origin: u64) -> CounterCell {
        CounterCell {
            rule_id: 1,
            key_hash_hi: 10,
            key_hash_lo: 20,
            bucket_start_millis: 0,
            origin_node_id: NodeId { hi: 0, lo: origin },
            origin_incarnation: 1,
            count,
            last_update_millis: count,
            sequence: 0,
        }
    }

    fn rule() -> Rule {
        Rule {
            id: 1,
            domain_hash: hash_domain("api"),
            descriptor_matcher: DescriptorMatcher::exact_keys(["tenant"]),
            limit: 10,
            window: WindowSpec {
                size_millis: 1_000,
                bucket_count: 10,
            },
            local_fallback_limit: 3,
            local_absolute_limit: 6,
            stale_after_millis: 500,
            safety_margin: SafetyMargin { hits: 0 },
            overflow_policy: OverflowPolicy::UseOverflowKey,
            mode: EnforcementMode::Enforce,
        }
    }

    #[test]
    fn merge_remote_is_monotonic() {
        let mut table = CellTable::with_capacity(4, 8);

        let first = table.merge_remote(cell(5, 1), None, 0).expect("insert");
        let stale = table.merge_remote(cell(3, 1), None, 0).expect("stale");
        let newer = table.merge_remote(cell(8, 1), None, 0).expect("newer");

        assert_eq!(first.delta, 5);
        assert_eq!(stale.delta, 0);
        assert!(!stale.changed);
        assert_eq!(newer.delta, 3);
        assert_eq!(table.active_cell_count(), 1);
    }

    #[test]
    fn merge_remote_is_idempotent_commutative_and_associative() {
        fn merged_counts(cells: &[CounterCell]) -> Vec<(u64, u64)> {
            let mut table = CellTable::with_capacity(8, 16);
            for cell in cells {
                table.merge_remote(*cell, None, 0).expect("merge");
            }
            let mut counts = table
                .cells()
                .map(|(_id, cell)| (cell.origin_node_id.lo, cell.count))
                .collect::<Vec<_>>();
            counts.sort();
            counts
        }

        let a1 = cell(3, 1);
        let a2 = cell(7, 1);
        let b1 = cell(5, 2);
        let b2 = cell(9, 2);

        assert_eq!(
            merged_counts(&[a1, a2, b1, b2]),
            merged_counts(&[a2, a2, b2, b2])
        );
        assert_eq!(
            merged_counts(&[a1, b1, a2, b2]),
            merged_counts(&[b1, b2, a1, a2])
        );
        assert_eq!(
            merged_counts(&[a1, b1, a2, b2]),
            merged_counts(&[a1, a2, b1, b2])
        );
    }

    #[test]
    fn dirty_ring_records_overflow() {
        let mut table = CellTable::with_capacity(4, 1);

        table.merge_remote(cell(1, 1), None, 0).expect("insert one");
        table.merge_remote(cell(1, 2), None, 0).expect("insert two");

        assert!(table.dirty_overflowed());
        assert_eq!(table.dirty_cells().count(), 1);
    }

    #[test]
    fn digest_changes_when_counts_change() {
        let mut table = CellTable::with_capacity(4, 8);
        table.merge_remote(cell(1, 1), None, 0).expect("insert");
        let before = table.digest(0, 1);
        table.merge_remote(cell(2, 1), None, 0).expect("update");
        let after = table.digest(0, 1);

        assert_eq!(before.active_cell_count, 1);
        assert_ne!(before.checksum, after.checksum);
        assert!(after.max_sequence > before.max_sequence);
    }

    #[test]
    fn binary_round_trip_reuses_buffer_and_respects_capacity() {
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 42,
                sender_node_id: NodeId { hi: 1, lo: 2 },
                sender_incarnation: 7,
                min_bucket: 0,
                max_bucket: 1000,
                flags: 0,
            },
            digests: vec![ShardDigest {
                shard_id: 0,
                active_cell_count: 1,
                max_sequence: 9,
                checksum: 11,
            }],
            cells: vec![cell(5, 1), cell(6, 2)],
            truncated: false,
        };
        let mut buffer = Vec::with_capacity(512);
        let capacity = buffer.capacity();

        let truncated = encode_message(&message, &mut buffer, 512);
        let decoded = decode_message(&buffer, 4, 4).expect("decode");

        assert!(!truncated);
        assert_eq!(buffer.capacity(), capacity);
        assert_eq!(decoded.cells.len(), 2);
        assert_eq!(decoded.digests, message.digests);
        assert_eq!(
            decode_message(&buffer, 0, 4),
            Err(DecodeError::CapacityExceeded)
        );
    }

    #[test]
    fn visitor_decode_reports_cells_without_allocating_message_vectors() {
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 42,
                sender_node_id: NodeId { hi: 1, lo: 2 },
                sender_incarnation: 7,
                min_bucket: 0,
                max_bucket: 1000,
                flags: 0,
            },
            digests: vec![ShardDigest {
                shard_id: 0,
                active_cell_count: 1,
                max_sequence: 9,
                checksum: 11,
            }],
            cells: vec![cell(5, 1), cell(6, 2)],
            truncated: false,
        };
        let mut buffer = Vec::with_capacity(512);
        let mut digest_count = 0;
        let mut cell_count = 0;
        let mut total = 0_u64;

        assert!(!encode_message(&message, &mut buffer, 512));
        let summary = decode_message_visit(
            &buffer,
            GossipLimits {
                max_payload_bytes: 512,
                max_digests: 4,
                max_cells: 4,
            },
            |_| digest_count += 1,
            |cell| {
                cell_count += 1;
                total = total.saturating_add(cell.count);
            },
        )
        .expect("decode");

        assert_eq!(summary.digest_count, 1);
        assert_eq!(summary.cell_count, 2);
        assert_eq!(digest_count, 1);
        assert_eq!(cell_count, 2);
        assert_eq!(total, 11);
    }

    #[test]
    fn binary_encode_truncates_to_max_payload() {
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 42,
                sender_node_id: NodeId { hi: 1, lo: 2 },
                sender_incarnation: 7,
                min_bucket: 0,
                max_bucket: 1000,
                flags: 0,
            },
            digests: Vec::new(),
            cells: vec![cell(5, 1), cell(6, 2)],
            truncated: false,
        };
        let mut buffer = Vec::with_capacity(HEADER_LEN + CELL_LEN);

        assert!(encode_message(&message, &mut buffer, HEADER_LEN + CELL_LEN));
        let decoded = decode_message(&buffer, 0, 2).expect("decode");

        assert!(decoded.truncated);
        assert_eq!(decoded.cells.len(), 1);
    }

    #[test]
    fn authenticated_message_rejects_tampering() {
        let key = HmacKey::new([7_u8; 32]);
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 42,
                sender_node_id: NodeId { hi: 1, lo: 2 },
                sender_incarnation: 7,
                min_bucket: 0,
                max_bucket: 1000,
                flags: 0,
            },
            digests: Vec::new(),
            cells: vec![cell(5, 1)],
            truncated: false,
        };
        let mut buffer = Vec::with_capacity(512);

        assert!(!encode_authenticated_message(
            &message,
            key,
            &mut buffer,
            GossipLimits::default()
        ));
        let decoded =
            decode_authenticated_message(&buffer, key, GossipLimits::default()).expect("decode");
        assert_eq!(decoded.cells.len(), 1);

        let last = buffer.len() - 1;
        buffer[last] ^= 1;
        assert_eq!(
            decode_authenticated_message(&buffer, key, GossipLimits::default()),
            Err(DecodeError::AuthenticationFailed)
        );
    }

    fn merged_counts(cells: &[CounterCell]) -> Vec<(u64, u64)> {
        let mut table = CellTable::with_capacity(32, 32);
        for cell in cells {
            table.merge_remote(*cell, None, 0).expect("merge");
        }
        let mut counts = table
            .cells()
            .map(|(_id, cell)| (cell.origin_node_id.lo, cell.count))
            .collect::<Vec<_>>();
        counts.sort();
        counts
    }

    // TODO(gap): keep CRDT merge laws covered with generated cell sequences.
    #[quickcheck]
    fn quickcheck_remote_merge_is_monotonic_idempotent_commutative_and_associative(
        case: MergeLawCase,
    ) -> TestResult {
        let len = case.counts.len().min(case.origins.len()).min(24);
        if len == 0 {
            return TestResult::discard();
        }
        let mut cells = Vec::with_capacity(len);
        for index in 0..len {
            cells.push(cell(
                u64::from(case.counts[index]) + 1,
                u64::from(case.origins[index] % 8) + 1,
            ));
        }
        let mut sorted_by_identity = cells.clone();
        sorted_by_identity.sort_by_key(|cell| (cell.origin_node_id.lo, cell.count));
        let mut reversed = cells.clone();
        reversed.reverse();
        let mut duplicated = cells.clone();
        duplicated.extend(cells.iter().copied());

        let merged = merged_counts(&cells);
        if merged != merged_counts(&reversed)
            || merged != merged_counts(&sorted_by_identity)
            || merged != merged_counts(&duplicated)
        {
            return TestResult::error("merge result changed under reorder or duplicate delivery");
        }

        for origin in 1..=8 {
            let max_input = cells
                .iter()
                .filter(|cell| cell.origin_node_id.lo == origin)
                .map(|cell| cell.count)
                .max()
                .unwrap_or(0);
            let merged_count = merged
                .iter()
                .find_map(|(merged_origin, count)| (*merged_origin == origin).then_some(*count))
                .unwrap_or(0);
            if merged_count != max_input {
                return TestResult::error(
                    "merged count was not monotonic maximum per cell identity",
                );
            }
        }

        TestResult::passed()
    }

    // TODO(gap): keep dirty-ring overflow bounded under generated write pressure.
    #[quickcheck]
    fn quickcheck_dirty_ring_is_bounded_and_reports_overflow(case: DirtyRingCase) -> TestResult {
        let dirty_capacity = usize::from(case.dirty_capacity);
        let cell_count = usize::from(case.cell_count);
        let mut table = CellTable::with_capacity(cell_count, dirty_capacity);

        for origin in 0..cell_count {
            if table
                .merge_remote(cell(1, origin as u64 + 1), None, 0)
                .is_err()
            {
                return TestResult::error("generated table filled before cell capacity");
            }
            if table.dirty_len() > dirty_capacity {
                return TestResult::error("dirty ring length exceeded configured capacity");
            }
        }

        let expected_overflow = dirty_capacity < cell_count;
        if table.dirty_overflowed() == expected_overflow {
            TestResult::passed()
        } else {
            TestResult::error("dirty ring overflow flag diverged from capacity model")
        }
    }

    #[quickcheck]
    fn quickcheck_codec_roundtrip_and_visitor_decode_match(case: CodecRoundTripCase) -> TestResult {
        let digest_count = usize::from(case.digest_count);
        let cell_count = usize::from(case.cell_count);
        let mut digests = Vec::with_capacity(digest_count);
        for index in 0..digest_count {
            digests.push(ShardDigest {
                shard_id: index as u16,
                active_cell_count: (index + 1) as u32,
                max_sequence: index as u64 + 10,
                checksum: index as u64 + 20,
            });
        }
        let mut cells = Vec::with_capacity(cell_count);
        for index in 0..cell_count {
            cells.push(CounterCell {
                rule_id: 1,
                key_hash_hi: index as u64,
                key_hash_lo: index as u64 + 1,
                bucket_start_millis: (index as i64) * 1_000,
                origin_node_id: NodeId {
                    hi: 0,
                    lo: index as u64 + 1,
                },
                origin_incarnation: 1,
                count: index as u64 + 1,
                last_update_millis: index as u64 + 2,
                sequence: 0,
            });
        }
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 42,
                sender_node_id: NodeId { hi: 1, lo: 2 },
                sender_incarnation: 7,
                min_bucket: 0,
                max_bucket: 8_000,
                flags: 0,
            },
            digests,
            cells,
            truncated: false,
        };
        let mut buffer = Vec::with_capacity(4096);
        if encode_message(&message, &mut buffer, 4096) {
            return TestResult::error("generated codec message unexpectedly truncated");
        }

        let Ok(decoded) = decode_message(&buffer, digest_count, cell_count) else {
            return TestResult::error("allocating decode failed for encoded message");
        };
        let mut visitor_digests = Vec::with_capacity(digest_count);
        let mut visitor_cells = Vec::with_capacity(cell_count);
        let Ok(summary) = decode_message_visit(
            &buffer,
            GossipLimits {
                max_payload_bytes: 4096,
                max_digests: digest_count,
                max_cells: cell_count,
            },
            |digest| visitor_digests.push(digest),
            |cell| visitor_cells.push(cell),
        ) else {
            return TestResult::error("visitor decode failed for encoded message");
        };

        if decoded != message {
            return TestResult::error("allocating decode did not round-trip encoded message");
        }
        if summary.digest_count != decoded.digests.len()
            || summary.cell_count != decoded.cells.len()
            || visitor_digests != decoded.digests
            || visitor_cells != decoded.cells
        {
            return TestResult::error("visitor decode content diverged from allocating decode");
        }
        TestResult::passed()
    }

    #[quickcheck]
    fn quickcheck_authenticated_frames_reject_single_byte_mutations(
        case: AuthMutationCase,
    ) -> TestResult {
        let key = HmacKey::new([7_u8; 32]);
        let cells = (0..case.cell_count)
            .map(|index| cell(u64::from(index) + 1, u64::from(index) + 1))
            .collect();
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 42,
                sender_node_id: NodeId { hi: 1, lo: 2 },
                sender_incarnation: 7,
                min_bucket: 0,
                max_bucket: 1000,
                flags: 0,
            },
            digests: vec![ShardDigest {
                shard_id: 0,
                active_cell_count: 1,
                max_sequence: 2,
                checksum: 3,
            }],
            cells,
            truncated: false,
        };
        let mut buffer = Vec::with_capacity(1024);

        if encode_authenticated_message(&message, key, &mut buffer, GossipLimits::default()) {
            return TestResult::error("generated authenticated message unexpectedly truncated");
        }
        let index = usize::from(case.mutation_index) % buffer.len();
        let mut mutated = buffer.clone();
        mutated[index] ^= 1;
        if decode_authenticated_message(&mutated, key, GossipLimits::default())
            == Err(DecodeError::AuthenticationFailed)
        {
            TestResult::passed()
        } else {
            TestResult::error("authenticated decode accepted a mutated frame")
        }
    }

    // TODO(gap): keep authenticated visitor decoding covered before callbacks
    // mutate state.
    #[quickcheck]
    fn quickcheck_authenticated_visitor_rejects_mutations_before_cell_callbacks(
        case: AuthMutationCase,
    ) -> TestResult {
        let key = HmacKey::new([7_u8; 32]);
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 42,
                sender_node_id: NodeId { hi: 1, lo: 2 },
                sender_incarnation: 7,
                min_bucket: 0,
                max_bucket: 1000,
                flags: 0,
            },
            digests: Vec::new(),
            cells: vec![cell(u64::from(case.cell_count), 1)],
            truncated: false,
        };
        let mut buffer = Vec::with_capacity(512);
        if encode_authenticated_message(&message, key, &mut buffer, GossipLimits::default()) {
            return TestResult::error("generated authenticated visitor message truncated");
        }
        let index = usize::from(case.mutation_index) % buffer.len();
        buffer[index] ^= 1;
        let mut callback_count = 0_u8;

        let result = decode_authenticated_message_visit_checked(
            &buffer,
            key,
            GossipLimits::default(),
            |_| true,
            |_| {},
            |_| callback_count = callback_count.saturating_add(1),
        );

        if result == Err(DecodeError::AuthenticationFailed) && callback_count == 0 {
            TestResult::passed()
        } else {
            TestResult::error("authenticated visitor decode ran callbacks for a mutated frame")
        }
    }

    #[test]
    fn authenticated_visitor_decode_rejects_tampering_without_cell_callbacks() {
        let key = HmacKey::new([7_u8; 32]);
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 42,
                sender_node_id: NodeId { hi: 1, lo: 2 },
                sender_incarnation: 7,
                min_bucket: 0,
                max_bucket: 1000,
                flags: 0,
            },
            digests: Vec::new(),
            cells: vec![cell(5, 1)],
            truncated: false,
        };
        let mut buffer = Vec::with_capacity(512);
        let mut cells = 0_u8;

        assert!(!encode_authenticated_message(
            &message,
            key,
            &mut buffer,
            GossipLimits::default()
        ));
        let summary = decode_authenticated_message_visit_checked(
            &buffer,
            key,
            GossipLimits::default(),
            |_| true,
            |_| {},
            |_| cells = cells.saturating_add(1),
        )
        .expect("decode");
        assert_eq!(summary.cell_count, 1);
        assert_eq!(cells, 1);

        cells = 0;
        let last = buffer.len() - 1;
        buffer[last] ^= 1;
        assert_eq!(
            decode_authenticated_message_visit_checked(
                &buffer,
                key,
                GossipLimits::default(),
                |_| true,
                |_| {},
                |_| cells = cells.saturating_add(1),
            ),
            Err(DecodeError::AuthenticationFailed)
        );
        assert_eq!(cells, 0);
    }

    #[test]
    fn payload_limit_is_enforced_before_decode() {
        let mut buffer = Vec::with_capacity(HEADER_LEN);
        let message = GossipMessage {
            header: GossipHeader {
                cluster_id_hash: 1,
                sender_node_id: NodeId { hi: 1, lo: 2 },
                sender_incarnation: 1,
                min_bucket: 0,
                max_bucket: 0,
                flags: 0,
            },
            digests: Vec::new(),
            cells: Vec::new(),
            truncated: false,
        };

        assert!(!encode_message(&message, &mut buffer, HEADER_LEN));
        assert_eq!(
            decode_message_with_limits(
                &buffer,
                GossipLimits {
                    max_payload_bytes: HEADER_LEN - 1,
                    max_digests: 0,
                    max_cells: 0,
                }
            ),
            Err(DecodeError::PayloadTooLarge)
        );
    }

    #[test]
    fn decoder_fuzz_smoke_returns_errors_without_panics() {
        let mut bytes = [0_u8; 96];
        let mut state = 0x1234_5678_9abc_def0_u64;

        for len in 0..bytes.len() {
            for byte in &mut bytes {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                *byte = (state >> 32) as u8;
            }
            let _ = decode_message_with_limits(
                &bytes[..len],
                GossipLimits {
                    max_payload_bytes: 128,
                    max_digests: 4,
                    max_cells: 4,
                },
            );
        }
    }

    #[test]
    fn cell_table_reports_memory_pressure_without_growth() {
        let mut table = CellTable::with_capacity(1, 2);

        assert!(table.merge_remote(cell(1, 1), None, 0).is_ok());
        assert_eq!(table.merge_remote(cell(1, 2), None, 0), Err(CellTableFull));
        assert_eq!(table.capacity(), 1);
        assert_eq!(table.active_cell_count(), 1);
    }

    #[test]
    fn simulation_merges_remote_counts_into_local_engine_estimates() {
        let descriptors = [Descriptor {
            key: "tenant",
            value: "a",
        }];
        let request = LimitRequest {
            domain: "api",
            descriptors: &descriptors,
            hits: 1,
        };
        let key_hash = hash_key(1, &request);
        let remote = CounterCell {
            rule_id: 1,
            key_hash_hi: key_hash.hi(),
            key_hash_lo: key_hash.lo(),
            bucket_start_millis: 0,
            origin_node_id: NodeId { hi: 0, lo: 2 },
            origin_incarnation: 1,
            count: 10,
            last_update_millis: 0,
            sequence: 0,
        };
        let mut table = CellTable::with_capacity(8, 8);
        let mut engine = LocalEngine::new(RuleTable::new(vec![rule()]), 16, 10);

        table
            .merge_remote(remote, Some(&mut engine), 0)
            .expect("merge");

        assert_eq!(
            engine.check_and_record(request, 1),
            Decision::Reject(gabion_core::RejectReason::GlobalLimit)
        );
    }
}
