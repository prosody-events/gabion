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
//! [ count              : u32 ×N ]     4N
//! ```
//!
//! When `cell_count == 0` the body is empty (no dict section). All integers
//! are little-endian; no alignment requirement.

use crate::crdt::{CellHandle, CellRow, CellStore, KeyHash, NodeId, Observation};

use super::{DecodeError, read_u32, read_u64, read_u128};

/// Per-cell identity bytes once the slot indices are encoded.
/// `1 (rule_slot) + 16 (key_hash) + 4 (bucket) + 1 (node_slot) +
///  4 (origin_incarnation) + 8 (last_update_millis)`.
const PER_CELL_IDENT_BYTES: usize = 1 + 16 + 4 + 1 + 4 + 8;

/// Bytes one cell costs: its identity and its `u32` count.
pub(crate) const CELL_BYTES: usize = PER_CELL_IDENT_BYTES + 4;

/// Fixed body overhead when at least one cell is present: the two
/// `dict_len` bytes. Dictionary entries themselves cost `16 * (R + D)`.
pub(crate) const DICT_LEN_BYTES: usize = 2;

/// Maximum slots per packet for each mini-dictionary. The 255 cap is encoded
/// by the `u8` slot type at the call site; this constant is for boundary
/// arithmetic.
pub(crate) const MAX_DICT_SLOTS: usize = 255;

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
    pub fn for_store(store: &CellStore) -> Self {
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

/// What one packet's body holds: the handles it consumes, the cells it
/// emits, and its dictionary sizes and length.
pub(crate) struct Plan {
    /// Handles consumed from `handles[start..]`: the cells emitted and the
    /// stale handles skipped, which never carry over to the next packet.
    pub(crate) handles_consumed: usize,
    pub(crate) cells_emitted: u16,
    /// `R` — distinct rule fingerprints required by the emitted cells.
    rule_count: u8,
    /// `D` — distinct origin node ids required by the emitted cells.
    node_count: u8,
    /// Body bytes.
    len: usize,
}

/// Encode the next packet's body into `out` starting at `out.len()`. Returns
/// the [`Plan`] it wrote; the caller patches the header from it.
///
/// `body_budget` is the bytes available for the body (the header is the
/// caller's concern). The encoder will not exceed it.
///
/// `start_at` is the index into `handles` of the next handle to consider.
pub(crate) fn encode_packet_body(
    store: &CellStore,
    handles: &[CellHandle],
    start_at: usize,
    body_budget: usize,
    scratch: &mut WireScratch,
    out: &mut Vec<u8>,
) -> Plan {
    debug_assert!(start_at <= handles.len());
    scratch.reset();

    let plan = plan_packet(store, handles, start_at, body_budget, scratch);
    if plan.cells_emitted > 0 {
        write_body(store, handles, start_at, &plan, scratch, out);
    }
    plan
}

/// The row behind `handle` with its rule fingerprint and origin node id,
/// or `None` for a stale handle, which both passes skip alike.
fn resolve(store: &CellStore, handle: CellHandle) -> Option<(CellRow, u128, u128)> {
    let row = store.get(handle)?;
    let rule = store.rule_dictionary().descriptor(row.key.rule)?;
    let node = store.node_dictionary().descriptor(row.key.origin)?;
    Some((row, rule.fingerprint, node.node_id.0))
}

fn plan_packet(
    store: &CellStore,
    handles: &[CellHandle],
    start_at: usize,
    body_budget: usize,
    scratch: &mut WireScratch,
) -> Plan {
    let mut cursor = start_at;
    let mut cells_emitted: u16 = 0;
    let mut r: u8 = 0;
    let mut d: u8 = 0;

    while cursor < handles.len() {
        let Some((row, rule_fp, node_id)) = resolve(store, handles[cursor]) else {
            cursor += 1;
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
            + (usize::from(cells_emitted) + 1) * CELL_BYTES;
        if projected_body > body_budget {
            break;
        }

        // Admit: assign per-packet slots if unseen.
        if rule_unseen {
            scratch.rule_remap[row.key.rule as usize] = r;
            scratch.rule_fps.push(rule_fp);
            r += 1;
        }
        if node_unseen {
            scratch.node_remap[row.key.origin as usize] = d;
            scratch.node_ids.push(node_id);
            d += 1;
        }

        cells_emitted += 1;
        cursor += 1;
    }

    let len = if cells_emitted == 0 {
        0
    } else {
        DICT_LEN_BYTES + 16 * (r as usize + d as usize) + usize::from(cells_emitted) * CELL_BYTES
    };

    Plan {
        handles_consumed: cursor - start_at,
        cells_emitted,
        rule_count: r,
        node_count: d,
        len,
    }
}

fn write_body(
    store: &CellStore,
    handles: &[CellHandle],
    start_at: usize,
    plan: &Plan,
    scratch: &WireScratch,
    out: &mut Vec<u8>,
) {
    let n = usize::from(plan.cells_emitted);
    let r = plan.rule_count as usize;
    let d = plan.node_count as usize;

    let base = out.len();
    // Grow once to the planned size; pass 2 writes via slice indexing.
    out.resize(base + plan.len, 0);
    let body = &mut out[base..base + plan.len];

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

    // Compute the seven column slab offsets. Splitting via slice indexing
    // keeps the borrow checker happy without `split_at_mut` ceremony.
    let rule_slot_off = cursor;
    let key_hash_off = rule_slot_off + n;
    let bucket_off = key_hash_off + 16 * n;
    let node_slot_off = bucket_off + 4 * n;
    let origin_inc_off = node_slot_off + n;
    let last_update_off = origin_inc_off + 4 * n;
    let count_off = last_update_off + 8 * n;
    debug_assert_eq!(count_off + 4 * n, plan.len);

    // Pass 2 — walk the same handle range we planned over and fill slabs.
    let mut i = 0_usize;
    let mut consumed = 0_usize;
    while consumed < plan.handles_consumed && i < n {
        let handle = handles[start_at + consumed];
        consumed += 1;
        let Some((row, ..)) = resolve(store, handle) else {
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
        body[count_off + i * 4..count_off + i * 4 + 4].copy_from_slice(&row.count.to_le_bytes());

        i += 1;
    }
    debug_assert_eq!(i, n, "pass 2 emitted a different cell count than pass 1");
}

/// Decode one packet's body. Per-packet; no scratch needed because the
/// dictionaries are embedded.
pub(crate) fn decode_body(
    bytes: &[u8],
    cell_count: u16,
    mut on_cell: impl FnMut(Observation),
) -> Result<(), DecodeError> {
    if cell_count == 0 {
        if !bytes.is_empty() {
            return Err(DecodeError::BodyLength);
        }
        return Ok(());
    }

    if bytes.is_empty() {
        return Err(DecodeError::BodyLength);
    }
    let rule_dict_len = bytes[0] as usize;
    let rule_dict_start = 1;
    let rule_dict_end = rule_dict_start + 16 * rule_dict_len;
    if bytes.len() < rule_dict_end + 1 {
        return Err(DecodeError::BodyLength);
    }
    let node_dict_len = bytes[rule_dict_end] as usize;
    let node_dict_start = rule_dict_end + 1;
    let node_dict_end = node_dict_start + 16 * node_dict_len;

    // The exact length is checked before any cell is visited, so a cell
    // count larger than the bytes hold never reaches the loop.
    let n = usize::from(cell_count);
    if bytes.len() != node_dict_end + n * CELL_BYTES {
        return Err(DecodeError::BodyLength);
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
    let count_slab = &bytes[cursor..cursor + 4 * n];
    debug_assert_eq!(cursor + 4 * n, bytes.len());

    // Every slot is checked before any cell is handed over, so a dropped
    // packet hands over none.
    if rule_slot.iter().any(|&s| usize::from(s) >= rule_dict_len)
        || node_slot.iter().any(|&s| usize::from(s) >= node_dict_len)
    {
        return Err(DecodeError::BadSlot);
    }

    for i in 0..n {
        let rs = usize::from(rule_slot[i]);
        let ns = usize::from(node_slot[i]);
        on_cell(Observation {
            rule_fingerprint: read_u128(&rule_dict_bytes[rs * 16..]),
            key_hash: KeyHash(read_u128(&key_hash_slab[i * 16..])),
            bucket: read_u32(&bucket_slab[i * 4..]),
            origin: NodeId(read_u128(&node_dict_bytes[ns * 16..])),
            incarnation: read_u32(&origin_inc_slab[i * 4..]),
            count: read_u32(&count_slab[i * 4..]),
            last_update_millis: read_u64(&last_update_slab[i * 8..]),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests;
