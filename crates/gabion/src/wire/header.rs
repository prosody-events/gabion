//! Fixed-length frame header — hand-rolled little-endian, exactly 72 bytes.

use super::DecodeError;

pub const HEADER_LEN: usize = 72;
pub const MAGIC: u32 = 0x4742_4732;
pub const VERSION: u16 = 1;
/// Set on every packet but the last in a multi-packet batch. Each packet is
/// independently decodable — `FLAG_MORE` is only an advisory hint for
/// receivers that want to defer downstream work until the batch settles.
pub const FLAG_MORE: u16 = 0x01;
pub const FLAG_AUTHENTICATED: u16 = 0x02;
const FRAME_KIND_SHIFT: u16 = 2;
const FRAME_KIND_MASK: u16 = 0b111 << FRAME_KIND_SHIFT;

/// What a frame is for, carried in three flag bits so a receiver knows how to
/// treat it before it reads a cell. Cells from every kind merge the same way;
/// the kind decides what the receiver does *about* them (see
/// `gossip::runtime::plumtree`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameKind {
    /// PULL replies, the catch-up that leaves the tree alone: merge, never
    /// forward.
    Plain = 0,
    /// Eager push along a root's tree, numbered per edge (the number rides in
    /// `min_origin_sequence`). A duplicate here is the PRUNE signal.
    Eager = 1,
    /// A lost eager packet sent again under its original edge number: merge
    /// and forward, but a duplicate says nothing about the tree.
    Resent = 2,
    /// The answer to a GRAFT: merge and forward, duplicates expected.
    Grafted = 3,
    /// Hybrid random push: merge, forward only once a tree edge delivers the
    /// same change.
    Random = 4,
    /// IHAVE, GRAFT, PRUNE, NACK, PULL or DIGEST: no cells, see
    /// [`super::ControlKind`].
    Control = 5,
}

impl FrameKind {
    /// The header flag bits that name this kind.
    pub const fn flags(self) -> u16 {
        (self as u16) << FRAME_KIND_SHIFT
    }

    fn from_flags(flags: u16) -> Option<Self> {
        match (flags & FRAME_KIND_MASK) >> FRAME_KIND_SHIFT {
            0 => Some(Self::Plain),
            1 => Some(Self::Eager),
            2 => Some(Self::Resent),
            3 => Some(Self::Grafted),
            4 => Some(Self::Random),
            5 => Some(Self::Control),
            _ => None,
        }
    }
}

/// The kind a packet claims, read without validating or authenticating the
/// rest of it. Only routes the packet to the right decoder; that decoder
/// still checks everything. `None` for a packet too short to carry flags or
/// naming an unknown kind.
pub fn frame_kind(bytes: &[u8]) -> Option<FrameKind> {
    let flags = u16::from_le_bytes(bytes.get(6..8)?.try_into().ok()?);
    FrameKind::from_flags(flags)
}

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
    Ok(Header {
        cluster_id_hash,
        sender_node_id,
        sender_incarnation,
        count_width,
        cell_count,
        body_len,
        min_origin_sequence,
        max_origin_sequence,
        flags,
    })
}

#[cfg(test)]
mod tests;
