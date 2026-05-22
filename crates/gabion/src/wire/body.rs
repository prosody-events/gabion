//! Body codec: SoA columns with per-frame u128 interning.
//!
//! Each packet is self-contained — its small `(rule_fp, origin_node_id)` mini
//! dictionary lives inside the same body, so a receiver can decode any packet
//! in isolation regardless of whether earlier or later packets in the same
//! batch arrived.
//!
//! Layout when `cell_count > 0`:
//!
//! ```text
//! [ rule_dict_len      : u8     ]    R, 1..=255
//! [ rule_dict          : u128×R ]    16R bytes, slot order
//! [ node_dict_len      : u8     ]    D, 1..=255
//! [ node_dict          : u128×D ]    16D bytes, slot order
//! [ rule_slot          : u8 ×N  ]    each < R
//! [ key_hash           : u128×N ]    16N
//! [ bucket             : u32 ×N ]     4N
//! [ node_slot          : u8 ×N  ]    each < D
//! [ origin_incarnation : u32 ×N ]     4N
//! [ last_update_millis : u64 ×N ]     8N
//! [ origin_sequence    : u64 ×N ]     8N
//! [ count              : C  ×N  ]    count_size · N
//! ```
//!
//! When `cell_count == 0` the body is empty (no dict section). All integers
//! are little-endian; no alignment requirement.

use crate::crdt::{BucketEpoch, CellHandle, CellStore, Count, Incarnation, KeyHash, NodeId};

use super::DecodeError;

/// Per-cell identity bytes once the slot indices are encoded.
/// `1 (rule_slot) + 16 (key_hash) + 4 (bucket) + 1 (node_slot) +
///  4 (origin_incarnation) + 8 (last_update_millis) + 8 (origin_sequence)`.
pub(crate) const PER_CELL_IDENT_BYTES: usize = 1 + 16 + 4 + 1 + 4 + 8 + 8;

/// Fixed body overhead when at least one cell is present: the two
/// `dict_len` bytes. Dictionary entries themselves cost `16 * (R + D)`.
pub(crate) const DICT_LEN_BYTES: usize = 2;

/// Maximum slots per packet for each mini-dictionary. The 255 cap is encoded
/// by the `u8` slot type at the call site; this constant is for boundary
/// arithmetic.
pub(crate) const MAX_DICT_SLOTS: usize = 255;

/// Decoded cell handed to visitor callbacks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireCell<C: Count> {
    pub rule_fingerprint: u128,
    pub key_hash: KeyHash,
    pub bucket: BucketEpoch,
    pub origin_node_id: NodeId,
    pub origin_incarnation: Incarnation,
    pub count: C,
    pub last_update_millis: u64,
    pub origin_sequence: u64,
}

/// Per-encode scratch. Re-fills `rule_remap` / `node_remap` with `u8::MAX` on
/// every `reset()`; the dictionary fingerprints are truncated to length zero.
///
/// Capacities are derived from the source `CellStore`'s dictionary
/// capacities — the only way to construct one is [`WireScratch::for_store`],
/// which threads dimensional correctness through the type system.
#[derive(Debug)]
pub struct WireScratch {
    /// Maps a [`crate::crdt::RuleSlot`] (u16) in the source store to a per-
    /// packet slot index `0..=254`, or `u8::MAX` if not yet interned this
    /// packet.
    pub(crate) rule_remap: Box<[u8]>,
    /// Same for [`crate::crdt::NodeSlot`].
    pub(crate) node_remap: Box<[u8]>,
    /// Per-packet rule-fingerprint dict (in slot order).
    pub(crate) rule_fps: Vec<u128>,
    /// Per-packet node-id dict (in slot order).
    pub(crate) node_ids: Vec<u128>,
}

impl WireScratch {
    /// Allocate scratch sized to a particular store's dictionary capacities.
    /// One allocation at startup; threaded through every packet.
    pub fn for_store<C: Count>(store: &CellStore<C>) -> Self {
        let rule_cap = store.rule_dictionary().capacity() as usize;
        let node_cap = store.node_dictionary().capacity() as usize;
        Self {
            rule_remap: vec![u8::MAX; rule_cap].into_boxed_slice(),
            node_remap: vec![u8::MAX; node_cap].into_boxed_slice(),
            rule_fps: Vec::with_capacity(MAX_DICT_SLOTS),
            node_ids: Vec::with_capacity(MAX_DICT_SLOTS),
        }
    }

    /// Reset the dictionaries and remaps. Called once per packet by the
    /// encoder.
    pub(crate) fn reset(&mut self) {
        for v in self.rule_remap.iter_mut() {
            *v = u8::MAX;
        }
        for v in self.node_remap.iter_mut() {
            *v = u8::MAX;
        }
        self.rule_fps.clear();
        self.node_ids.clear();
    }
}

/// Outcome of encoding one packet's body. The header patcher reads these.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct EncodedBody {
    /// Bytes written into the body region (exclusive of header / auth tag).
    pub body_len: usize,
    /// Cells emitted into this packet.
    pub cells_emitted: u32,
    /// Handles inspected for this packet that did not resolve to a stored
    /// row (stale generation, missing dict descriptor). They never advance
    /// to the next packet.
    pub cells_dropped: u32,
    /// Lowest `origin_sequence` among emitted cells, or `0` when empty.
    pub min_origin_sequence: u64,
    /// Highest `origin_sequence` among emitted cells.
    pub max_origin_sequence: u64,
    /// Number of handles consumed from `handles[start..]`, i.e.
    /// `cells_emitted + cells_dropped`.
    pub handles_consumed: usize,
}

/// Pre-flight pass: project how many handles fit in this packet without
/// writing anything. Returns the chosen handle range and the dictionary
/// sizes used.
struct Plan {
    /// Number of handles consumed: `cells_emitted + cells_dropped`.
    consumed: usize,
    cells_emitted: u32,
    cells_dropped: u32,
    /// `R` — distinct rule fingerprints required by the emitted cells.
    rule_count: u8,
    /// `D` — distinct origin node ids required by the emitted cells.
    node_count: u8,
    body_len: usize,
    min_seq: u64,
    max_seq: u64,
}

/// Encode the next packet's body into `out` starting at `out.len()`. Returns
/// `EncodedBody` describing what was written; the caller patches the header
/// using these values.
///
/// `body_budget` is the bytes available for the body (header + auth tag are
/// the caller's concern). The encoder will not exceed it.
///
/// `start_at` is the index into `handles` of the next handle to consider.
pub(crate) fn encode_packet_body<C: Count>(
    store: &CellStore<C>,
    handles: &[CellHandle],
    start_at: usize,
    body_budget: usize,
    scratch: &mut WireScratch,
    out: &mut Vec<u8>,
) -> EncodedBody {
    debug_assert!(start_at <= handles.len());
    scratch.reset();

    let count_size = std::mem::size_of::<C>();
    let plan = plan_packet(store, handles, start_at, body_budget, count_size, scratch);

    if plan.cells_emitted == 0 {
        return EncodedBody {
            body_len: 0,
            cells_emitted: 0,
            cells_dropped: plan.cells_dropped,
            min_origin_sequence: 0,
            max_origin_sequence: 0,
            handles_consumed: plan.consumed,
        };
    }

    write_body::<C>(store, handles, start_at, &plan, count_size, scratch, out);

    EncodedBody {
        body_len: plan.body_len,
        cells_emitted: plan.cells_emitted,
        cells_dropped: plan.cells_dropped,
        min_origin_sequence: plan.min_seq,
        max_origin_sequence: plan.max_seq,
        handles_consumed: plan.consumed,
    }
}

fn plan_packet<C: Count>(
    store: &CellStore<C>,
    handles: &[CellHandle],
    start_at: usize,
    body_budget: usize,
    count_size: usize,
    scratch: &mut WireScratch,
) -> Plan {
    let per_cell_bytes = PER_CELL_IDENT_BYTES + count_size;
    let mut cursor = start_at;
    let mut cells_emitted: u32 = 0;
    let mut cells_dropped: u32 = 0;
    let mut r: u8 = 0;
    let mut d: u8 = 0;
    let mut min_seq = u64::MAX;
    let mut max_seq = 0_u64;

    while cursor < handles.len() {
        let handle = handles[cursor];
        let Some(row) = store.get(handle) else {
            cursor += 1;
            cells_dropped = cells_dropped.saturating_add(1);
            continue;
        };
        let Some(rule_desc) = store.rule_dictionary().descriptor(row.key.rule) else {
            cursor += 1;
            cells_dropped = cells_dropped.saturating_add(1);
            continue;
        };
        let Some(node_desc) = store.node_dictionary().descriptor(row.key.origin) else {
            cursor += 1;
            cells_dropped = cells_dropped.saturating_add(1);
            continue;
        };

        let rule_unseen = scratch.rule_remap[row.key.rule as usize] == u8::MAX;
        let node_unseen = scratch.node_remap[row.key.origin as usize] == u8::MAX;
        let new_r = r as usize + if rule_unseen { 1 } else { 0 };
        let new_d = d as usize + if node_unseen { 1 } else { 0 };

        // Intern cap — encoded by the u8 slot indices.
        if new_r > MAX_DICT_SLOTS || new_d > MAX_DICT_SLOTS {
            break;
        }

        let projected_body = DICT_LEN_BYTES
            + 16 * new_r
            + 16 * new_d
            + (cells_emitted as usize + 1) * per_cell_bytes;
        if projected_body > body_budget {
            break;
        }

        // Admit: assign per-packet slots if unseen.
        if rule_unseen {
            scratch.rule_remap[row.key.rule as usize] = r;
            scratch.rule_fps.push(rule_desc.fingerprint);
            r += 1;
        }
        if node_unseen {
            scratch.node_remap[row.key.origin as usize] = d;
            scratch.node_ids.push(node_desc.node_id.0);
            d += 1;
        }

        cells_emitted = cells_emitted.saturating_add(1);
        if row.origin_sequence < min_seq {
            min_seq = row.origin_sequence;
        }
        if row.origin_sequence > max_seq {
            max_seq = row.origin_sequence;
        }
        cursor += 1;
    }

    let body_len = if cells_emitted == 0 {
        0
    } else {
        DICT_LEN_BYTES + 16 * (r as usize + d as usize) + cells_emitted as usize * per_cell_bytes
    };

    Plan {
        consumed: cursor - start_at,
        cells_emitted,
        cells_dropped,
        rule_count: r,
        node_count: d,
        body_len,
        min_seq: if cells_emitted == 0 { 0 } else { min_seq },
        max_seq,
    }
}

fn write_body<C: Count>(
    store: &CellStore<C>,
    handles: &[CellHandle],
    start_at: usize,
    plan: &Plan,
    count_size: usize,
    scratch: &WireScratch,
    out: &mut Vec<u8>,
) {
    let n = plan.cells_emitted as usize;
    let r = plan.rule_count as usize;
    let d = plan.node_count as usize;

    let base = out.len();
    // Grow once to the planned size; pass 2 writes via slice indexing.
    out.resize(base + plan.body_len, 0);
    let body = &mut out[base..base + plan.body_len];

    // Header: dict lengths + dict fingerprints, in declared layout order.
    body[0] = plan.rule_count;
    let mut cursor = 1;
    for &fp in &scratch.rule_fps[..r] {
        body[cursor..cursor + 16].copy_from_slice(&fp.to_le_bytes());
        cursor += 16;
    }
    body[cursor] = plan.node_count;
    cursor += 1;
    for &id in &scratch.node_ids[..d] {
        body[cursor..cursor + 16].copy_from_slice(&id.to_le_bytes());
        cursor += 16;
    }

    // Compute the eight column slab offsets. Splitting via slice indexing
    // keeps the borrow checker happy without `split_at_mut` ceremony.
    let rule_slot_off = cursor;
    let key_hash_off = rule_slot_off + n;
    let bucket_off = key_hash_off + 16 * n;
    let node_slot_off = bucket_off + 4 * n;
    let origin_inc_off = node_slot_off + n;
    let last_update_off = origin_inc_off + 4 * n;
    let origin_seq_off = last_update_off + 8 * n;
    let count_off = origin_seq_off + 8 * n;
    debug_assert_eq!(count_off + count_size * n, plan.body_len);

    // Pass 2 — walk the same handle range we planned over and fill slabs.
    // The staleness checks are identical to plan_packet so the two cell
    // counts agree.
    let mut i = 0_usize;
    let mut consumed = 0_usize;
    while consumed < plan.consumed && i < n {
        let handle = handles[start_at + consumed];
        consumed += 1;
        let Some(row) = store.get(handle) else {
            continue;
        };
        let Some(_rule_desc) = store.rule_dictionary().descriptor(row.key.rule) else {
            continue;
        };
        let Some(_node_desc) = store.node_dictionary().descriptor(row.key.origin) else {
            continue;
        };

        body[rule_slot_off + i] = scratch.rule_remap[row.key.rule as usize];
        body[key_hash_off + i * 16..key_hash_off + i * 16 + 16]
            .copy_from_slice(&row.key.key_hash.0.to_le_bytes());
        body[bucket_off + i * 4..bucket_off + i * 4 + 4]
            .copy_from_slice(&row.key.bucket.to_le_bytes());
        body[node_slot_off + i] = scratch.node_remap[row.key.origin as usize];
        body[origin_inc_off + i * 4..origin_inc_off + i * 4 + 4]
            .copy_from_slice(&row.key.incarnation.to_le_bytes());
        body[last_update_off + i * 8..last_update_off + i * 8 + 8]
            .copy_from_slice(&row.last_update_millis.to_le_bytes());
        body[origin_seq_off + i * 8..origin_seq_off + i * 8 + 8]
            .copy_from_slice(&row.origin_sequence.to_le_bytes());

        let count_u64: u64 = row.count.into();
        body[count_off + i * count_size..count_off + i * count_size + count_size]
            .copy_from_slice(&count_u64.to_le_bytes()[..count_size]);

        i += 1;
    }
    debug_assert_eq!(i, n, "pass 2 emitted a different cell count than pass 1");
}

fn read_count<C: Count>(src: &[u8]) -> C {
    let mut buf = [0u8; 8];
    let n = std::mem::size_of::<C>();
    buf[..n].copy_from_slice(&src[..n]);
    C::saturating_from_u64(u64::from_le_bytes(buf))
}

fn read_u32(src: &[u8]) -> u32 {
    u32::from_le_bytes(src[..4].try_into().expect("4 bytes"))
}

fn read_u64(src: &[u8]) -> u64 {
    u64::from_le_bytes(src[..8].try_into().expect("8 bytes"))
}

fn read_u128(src: &[u8]) -> u128 {
    u128::from_le_bytes(src[..16].try_into().expect("16 bytes"))
}

/// Decode one packet's body. Per-packet; no scratch needed because the
/// dictionaries are embedded.
pub(crate) fn decode_body_visit<C: Count>(
    bytes: &[u8],
    cell_count: u32,
    mut on_cell: impl FnMut(WireCell<C>),
) -> Result<(), DecodeError> {
    let count_size = std::mem::size_of::<C>();

    if cell_count == 0 {
        if !bytes.is_empty() {
            return Err(DecodeError::BodyLenMismatch);
        }
        return Ok(());
    }

    if bytes.is_empty() {
        return Err(DecodeError::BodyLenMismatch);
    }
    let rule_dict_len = bytes[0] as usize;
    if rule_dict_len == 0 {
        // Cells require at least one rule slot.
        return Err(DecodeError::BodyLenMismatch);
    }
    let rule_dict_start = 1;
    let rule_dict_end = rule_dict_start + 16 * rule_dict_len;
    if bytes.len() < rule_dict_end + 1 {
        return Err(DecodeError::BodyLenMismatch);
    }
    let node_dict_len = bytes[rule_dict_end] as usize;
    if node_dict_len == 0 {
        return Err(DecodeError::BodyLenMismatch);
    }
    let node_dict_start = rule_dict_end + 1;
    let node_dict_end = node_dict_start + 16 * node_dict_len;

    let n = cell_count as usize;
    let per_cell = PER_CELL_IDENT_BYTES + count_size;
    let expected_tail = n
        .checked_mul(per_cell)
        .ok_or(DecodeError::BodyLenMismatch)?;
    let expected_total = node_dict_end
        .checked_add(expected_tail)
        .ok_or(DecodeError::BodyLenMismatch)?;
    if bytes.len() != expected_total {
        return Err(DecodeError::BodyLenMismatch);
    }

    let rule_dict_bytes = &bytes[rule_dict_start..rule_dict_end];
    let node_dict_bytes = &bytes[node_dict_start..node_dict_end];

    // Column slab offsets, in layout order.
    let mut cursor = node_dict_end;
    let rule_slot = &bytes[cursor..cursor + n];
    cursor += n;
    let key_hash_slab = &bytes[cursor..cursor + 16 * n];
    cursor += 16 * n;
    let bucket_slab = &bytes[cursor..cursor + 4 * n];
    cursor += 4 * n;
    let node_slot = &bytes[cursor..cursor + n];
    cursor += n;
    let origin_inc_slab = &bytes[cursor..cursor + 4 * n];
    cursor += 4 * n;
    let last_update_slab = &bytes[cursor..cursor + 8 * n];
    cursor += 8 * n;
    let origin_seq_slab = &bytes[cursor..cursor + 8 * n];
    cursor += 8 * n;
    let count_slab = &bytes[cursor..cursor + count_size * n];
    debug_assert_eq!(cursor + count_size * n, bytes.len());

    for i in 0..n {
        let rs = rule_slot[i] as usize;
        if rs >= rule_dict_len {
            return Err(DecodeError::BadSlot);
        }
        let ns = node_slot[i] as usize;
        if ns >= node_dict_len {
            return Err(DecodeError::BadSlot);
        }
        let rule_fingerprint = read_u128(&rule_dict_bytes[rs * 16..]);
        let origin_node_id = read_u128(&node_dict_bytes[ns * 16..]);
        let key_hash = read_u128(&key_hash_slab[i * 16..]);
        let bucket = read_u32(&bucket_slab[i * 4..]);
        let origin_incarnation = read_u32(&origin_inc_slab[i * 4..]);
        let last_update_millis = read_u64(&last_update_slab[i * 8..]);
        let origin_sequence = read_u64(&origin_seq_slab[i * 8..]);
        let count = read_count::<C>(&count_slab[i * count_size..]);
        on_cell(WireCell {
            rule_fingerprint,
            key_hash: KeyHash(key_hash),
            bucket,
            origin_node_id: NodeId(origin_node_id),
            origin_incarnation,
            count,
            last_update_millis,
            origin_sequence,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests;
