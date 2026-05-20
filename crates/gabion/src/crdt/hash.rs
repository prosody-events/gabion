//! Hash mixers shared by [`super::CellStore`] and the identity dictionaries.

use super::{CompactCellKey, Incarnation, NodeId};

#[inline(always)]
pub(super) fn mix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

#[inline(always)]
pub(super) fn hash_compact_cell_key(key: &CompactCellKey) -> u64 {
    let lo = key.key_hash.0 as u64;
    let hi = (key.key_hash.0 >> 64) as u64;
    let pack_a = (key.rule as u64) | ((key.origin as u64) << 16) | ((key.bucket as u64) << 32);
    let pack_b = key.incarnation as u64;
    mix64(lo ^ pack_a) ^ mix64(hi.wrapping_add(pack_b).wrapping_add(0x9E37_79B9_7F4A_7C15))
}

#[inline(always)]
pub(super) fn hash_fingerprint(fingerprint: u128) -> u64 {
    let lo = fingerprint as u64;
    let hi = (fingerprint >> 64) as u64;
    mix64(lo) ^ mix64(hi.wrapping_add(0x517C_C1B7_2722_0A95))
}

#[inline(always)]
pub(super) fn hash_node_identity(node_id: NodeId, incarnation: Incarnation) -> u64 {
    let lo = node_id.0 as u64;
    let hi = (node_id.0 >> 64) as u64;
    mix64(lo ^ incarnation as u64) ^ mix64(hi.wrapping_add(0xBF58_476D_1CE4_E5B9))
}
