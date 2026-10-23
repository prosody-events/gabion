//! Fixed-length frame header — hand-rolled little-endian, exactly 80 bytes.

use super::DecodeError;

pub const HEADER_LEN: usize = 80;
pub const MAGIC: u32 = 0x4742_4732;
pub const VERSION: u16 = 1;
/// Set on every packet but the last in a multi-packet batch. Each packet is
/// independently decodable — `FLAG_MORE` is only an advisory hint for
/// receivers that want to defer downstream work until the batch settles.
pub const FLAG_MORE: u16 = 0x01;
pub const FLAG_AUTHENTICATED: u16 = 0x02;
/// The packet is one of the sender's eager pushes to this receiver, numbered
/// in `edge_sequence` so the receiver can tell when one was lost.
pub const FLAG_EDGE: u16 = 0x04;
/// The packet carries no cells: its body is a control message (see
/// `wire::control`).
pub const FLAG_CONTROL: u16 = 0x08;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Header {
    pub cluster_id_hash: u128,
    pub sender_node_id: u128,
    pub sender_incarnation: u32,
    pub count_width: u32,
    pub cell_count: u32,
    pub body_len: u32,
    pub min_origin_sequence: u64,
    pub max_origin_sequence: u64,
    /// Position of this packet in the sender's eager stream to this receiver,
    /// counting from 1. Meaningful only with [`FLAG_EDGE`].
    pub edge_sequence: u64,
    pub flags: u16,
}

pub(crate) fn patch_header(buf: &mut [u8], hdr: &Header) {
    debug_assert!(buf.len() >= HEADER_LEN);
    let dst = &mut buf[..HEADER_LEN];
    dst[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    dst[4..6].copy_from_slice(&VERSION.to_le_bytes());
    dst[6..8].copy_from_slice(&hdr.flags.to_le_bytes());
    dst[8..24].copy_from_slice(&hdr.cluster_id_hash.to_le_bytes());
    dst[24..40].copy_from_slice(&hdr.sender_node_id.to_le_bytes());
    dst[40..44].copy_from_slice(&hdr.sender_incarnation.to_le_bytes());
    dst[44..48].copy_from_slice(&hdr.count_width.to_le_bytes());
    dst[48..52].copy_from_slice(&hdr.cell_count.to_le_bytes());
    dst[52..56].copy_from_slice(&hdr.body_len.to_le_bytes());
    dst[56..64].copy_from_slice(&hdr.min_origin_sequence.to_le_bytes());
    dst[64..72].copy_from_slice(&hdr.max_origin_sequence.to_le_bytes());
    dst[72..80].copy_from_slice(&hdr.edge_sequence.to_le_bytes());
}

pub(crate) fn read_header(bytes: &[u8]) -> Result<Header, DecodeError> {
    if bytes.len() < HEADER_LEN {
        return Err(DecodeError::TooShort);
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != MAGIC {
        return Err(DecodeError::BadMagic);
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    if version != VERSION {
        return Err(DecodeError::BadVersion);
    }
    let flags = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
    let cluster_id_hash = u128::from_le_bytes(bytes[8..24].try_into().unwrap());
    let sender_node_id = u128::from_le_bytes(bytes[24..40].try_into().unwrap());
    let sender_incarnation = u32::from_le_bytes(bytes[40..44].try_into().unwrap());
    let count_width = u32::from_le_bytes(bytes[44..48].try_into().unwrap());
    let cell_count = u32::from_le_bytes(bytes[48..52].try_into().unwrap());
    let body_len = u32::from_le_bytes(bytes[52..56].try_into().unwrap());
    let min_origin_sequence = u64::from_le_bytes(bytes[56..64].try_into().unwrap());
    let max_origin_sequence = u64::from_le_bytes(bytes[64..72].try_into().unwrap());
    let edge_sequence = u64::from_le_bytes(bytes[72..80].try_into().unwrap());
    Ok(Header {
        cluster_id_hash,
        sender_node_id,
        sender_incarnation,
        count_width,
        cell_count,
        body_len,
        min_origin_sequence,
        max_origin_sequence,
        edge_sequence,
        flags,
    })
}

#[cfg(test)]
mod tests;
