//! Fixed-length frame header — hand-rolled little-endian, exactly 72 bytes.

use std::num::NonZeroU64;

use super::DecodeError;

pub const HEADER_LEN: usize = 72;
pub const MAGIC: u32 = 0x4742_4732;
pub const VERSION: u16 = 1;
/// Set on every packet but the last in a multi-packet batch. Each packet is
/// independently decodable — `FLAG_MORE` is only an advisory hint for
/// receivers that want to defer downstream work until the batch settles.
pub const FLAG_MORE: u16 = 0x01;
pub const FLAG_AUTHENTICATED: u16 = 0x02;
/// The packet carries a contributor's own cells to the owner of their keys.
pub const FLAG_SYNC: u16 = 0x04;
/// The packet echoes cells an owner has folded, acknowledging a sync.
pub const FLAG_ACK: u16 = 0x08;
/// The packet asks an owner to repair broadcasts this node missed.
pub const FLAG_NACK: u16 = 0x10;
/// The packet re-sends one broadcast an owner was asked to repair.
pub const FLAG_REPAIR: u16 = 0x20;
/// The packet is part of an owner's snapshot of every key it owns.
pub const FLAG_SNAPSHOT: u16 = 0x40;

const KIND_FLAGS: u16 = FLAG_SYNC | FLAG_ACK | FLAG_NACK | FLAG_REPAIR | FLAG_SNAPSHOT;

/// What a packet is for, carried in the kind bits ([`FLAG_SYNC`],
/// [`FLAG_ACK`], [`FLAG_NACK`], [`FLAG_REPAIR`], [`FLAG_SNAPSHOT`]; none is a
/// broadcast). A packet is exactly one kind; two kind bits set is not a kind
/// and fails to parse.
///
/// Broadcasts, repairs and snapshots carry key totals and number their
/// packets in the header's sequence fields:
///
/// - a **broadcast** packet's `max_origin_sequence` is its sequence number: an
///   owner numbers every broadcast packet it sends 1, 2, 3, ..., so a receiver
///   can tell which it missed;
/// - a **repair** packet's `max_origin_sequence` is the sequence number of the
///   broadcast it re-sends, with the keys' current totals;
/// - a **snapshot** packet's `max_origin_sequence` is the last broadcast the
///   snapshot covers and its `min_origin_sequence` is the packet's index in the
///   snapshot, [`FLAG_MORE`] marking every packet but the last.
///
/// A NACK carries no rows; see [`Nack`] for its fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameKind {
    /// An owner's per-bucket totals of its changed keys, sent to every peer.
    /// The body is in the totals layout, not the cell layout.
    Broadcast,
    /// A contributor's own cells, sent to the owner of their keys.
    Sync,
    /// An owner's acknowledgement of the cells in a [`FrameKind::Sync`].
    Ack,
    /// A receiver's request for the broadcasts it missed, or for a snapshot.
    Nack,
    /// An owner's re-send of one broadcast a receiver missed.
    Repair,
    /// Part of an owner's snapshot of every key it owns.
    Snapshot,
}

impl FrameKind {
    pub fn flags(self) -> u16 {
        match self {
            Self::Broadcast => 0,
            Self::Sync => FLAG_SYNC,
            Self::Ack => FLAG_ACK,
            Self::Nack => FLAG_NACK,
            Self::Repair => FLAG_REPAIR,
            Self::Snapshot => FLAG_SNAPSHOT,
        }
    }

    pub fn from_flags(flags: u16) -> Option<Self> {
        match flags & KIND_FLAGS {
            0 => Some(Self::Broadcast),
            FLAG_SYNC => Some(Self::Sync),
            FLAG_ACK => Some(Self::Ack),
            FLAG_NACK => Some(Self::Nack),
            FLAG_REPAIR => Some(Self::Repair),
            FLAG_SNAPSHOT => Some(Self::Snapshot),
            _ => None,
        }
    }

    /// Whether the body is in the totals layout rather than the cell layout.
    pub fn carries_totals(self) -> bool {
        matches!(self, Self::Broadcast | Self::Repair | Self::Snapshot)
    }
}

/// What a [`FrameKind::Nack`] asks of an owner, carried in the header's
/// sequence fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Nack {
    /// Re-send the broadcasts whose bits are set in `missing`: bit `i` is
    /// broadcast `latest - 1 - i`, `latest` being the highest broadcast the
    /// receiver has seen (`max_origin_sequence`; `missing` is
    /// `min_origin_sequence`).
    Missed { latest: u64, missing: NonZeroU64 },
    /// Send a snapshot of every key the owner owns (both fields zero).
    Snapshot,
}

impl Nack {
    /// `(min_origin_sequence, max_origin_sequence)` of the NACK's header.
    pub fn sequence_fields(self) -> (u64, u64) {
        match self {
            Self::Missed { latest, missing } => (missing.get(), latest),
            Self::Snapshot => (0, 0),
        }
    }

    pub fn from_header(header: &Header) -> Self {
        match NonZeroU64::new(header.min_origin_sequence) {
            Some(missing) => Self::Missed {
                latest: header.max_origin_sequence,
                missing,
            },
            None => Self::Snapshot,
        }
    }
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
