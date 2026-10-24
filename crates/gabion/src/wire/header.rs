//! Fixed-length frame header — hand-rolled little-endian, exactly 88 bytes.

use super::DecodeError;

pub const HEADER_LEN: usize = 88;
pub const MAGIC: u32 = 0x4742_4732;
pub const VERSION: u16 = 2;
/// Set on every packet but the last in a multi-packet batch. Each packet is
/// independently decodable — `FLAG_MORE` is only an advisory hint for
/// receivers that want to defer downstream work until the batch settles.
pub const FLAG_MORE: u16 = 0x01;
pub const FLAG_AUTHENTICATED: u16 = 0x02;
/// The packet's cells belong to the sender's *relay stream* (cells a gateway
/// forwards into its own zone) rather than to its own-origin stream. It
/// selects which sequence space `stream_lower`/`stream_upper` index.
pub const FLAG_RELAY: u16 = 0x04;
/// The sender holds cells of the receiver's own stream beyond `ack_own` but
/// is missing some at or below them (a packet was lost): resend from
/// `ack_own` now rather than at the retransmission timeout.
pub const FLAG_GAP_OWN: u16 = 0x08;
/// As [`FLAG_GAP_OWN`], for the receiver's relay stream and `ack_relay`.
pub const FLAG_GAP_RELAY: u16 = 0x10;
/// A header-only packet asking the receiver, a member of the sender's zone,
/// for every live cell it holds.
pub const FLAG_SWEEP_REQUEST: u16 = 0x20;
/// The packet's cells answer a [`FLAG_SWEEP_REQUEST`]: part of the sender's
/// live window, outside any stream.
pub const FLAG_SWEEP: u16 = 0x40;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Header {
    pub cluster_id_hash: u128,
    pub sender_node_id: u128,
    pub sender_incarnation: u32,
    pub count_width: u32,
    pub cell_count: u32,
    pub body_len: u32,
    /// Cumulative ack: the highest sequence of the *receiver's* own stream
    /// the sender holds contiguously (`0` = none).
    pub ack_own: u64,
    /// Cumulative ack of the *receiver's* relay stream.
    pub ack_relay: u64,
    /// Exclusive lower bound of the stream range this packet covers: it
    /// carries every live cell of the sender's stream with a sequence in
    /// `(stream_lower, stream_upper]`.
    pub stream_lower: u64,
    /// Inclusive upper bound of the stream range this packet covers.
    pub stream_upper: u64,
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
    dst[56..64].copy_from_slice(&hdr.ack_own.to_le_bytes());
    dst[64..72].copy_from_slice(&hdr.ack_relay.to_le_bytes());
    dst[72..80].copy_from_slice(&hdr.stream_lower.to_le_bytes());
    dst[80..88].copy_from_slice(&hdr.stream_upper.to_le_bytes());
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
    let ack_own = u64::from_le_bytes(bytes[56..64].try_into().unwrap());
    let ack_relay = u64::from_le_bytes(bytes[64..72].try_into().unwrap());
    let stream_lower = u64::from_le_bytes(bytes[72..80].try_into().unwrap());
    let stream_upper = u64::from_le_bytes(bytes[80..88].try_into().unwrap());
    Ok(Header {
        cluster_id_hash,
        sender_node_id,
        sender_incarnation,
        count_width,
        cell_count,
        body_len,
        ack_own,
        ack_relay,
        stream_lower,
        stream_upper,
        flags,
    })
}

#[cfg(test)]
mod tests;
