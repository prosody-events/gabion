//! Control frames: the tree protocol's messages that carry no cells.
//!
//! A control frame is a normal header with [`FLAG_CONTROL`] set and
//! `cell_count == 0`, followed by this body, and an HMAC tag when
//! authenticated. Like a data packet it is self-describing and independently
//! decodable.
//!
//! ```text
//! [ kind        : u8      ]
//! [ tree        : u8      ]    which tree the message is about
//! [ entry_count : u16     ]
//! [ entries     : 28 × N  ]    node_id u128, incarnation u32, value u64
//! ```
//!
//! What `value` means depends on `kind`; see [`ControlKind`].

use crate::crdt::{Incarnation, NodeId};

use super::auth::{self, AUTH_TAG_LEN};
use super::header::{patch_header, read_header};
use super::{
    DecodeError, EncodeError, FLAG_AUTHENTICATED, FLAG_CONTROL, FrameLimits, HEADER_LEN, Header,
    HmacKey, PacketBuf,
};

const BODY_PREFIX: usize = 4;
/// Bytes of one control entry on the wire.
pub const CONTROL_ENTRY_BYTES: usize = 16 + 4 + 8;

/// What a control frame asks of its receiver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlKind {
    /// "Origin `node_id` has changes up to sequence `value`." Lazy push: ids
    /// only, no cells.
    Ihave,
    /// "Send me origin `node_id`'s cells above sequence `value`, and make me
    /// a tree neighbour for `tree`." `tree == PULL_ONLY_TREE` asks for the
    /// cells alone.
    Graft,
    /// "Stop pushing origin `node_id`'s changes to me" for each entry, or,
    /// with no entries, "I am no longer your leaf in `tree`."
    Prune,
    /// "Edge packets numbered `value` never arrived."
    Nack,
    /// "My latest packet to you was numbered `value`." Lets a receiver notice
    /// that the end of a burst was lost.
    Probe,
}

/// The `tree` of a [`ControlKind::Graft`] that asks for cells only.
pub const PULL_ONLY_TREE: u8 = u8::MAX;

impl ControlKind {
    fn code(self) -> u8 {
        match self {
            Self::Ihave => 1,
            Self::Graft => 2,
            Self::Prune => 3,
            Self::Nack => 4,
            Self::Probe => 5,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            1 => Self::Ihave,
            2 => Self::Graft,
            3 => Self::Prune,
            4 => Self::Nack,
            5 => Self::Probe,
            _ => return None,
        })
    }
}

/// One entry of a control frame.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ControlEntry {
    pub node_id: NodeId,
    pub incarnation: Incarnation,
    pub value: u64,
}

/// The fixed part of a decoded control frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedControl {
    pub header: Header,
    pub kind: ControlKind,
    pub tree: u8,
}

/// Whether `bytes` is a control frame. Reads only the flags, so it says
/// nothing about whether the frame is well formed or authentic.
pub fn is_control(bytes: &[u8]) -> bool {
    bytes.len() >= HEADER_LEN && u16::from_le_bytes([bytes[6], bytes[7]]) & FLAG_CONTROL != 0
}

/// Encode as many of `entries` as fit one packet into `buf`. Returns how many
/// were written; the caller encodes the rest in further frames. An empty
/// `entries` writes a frame with none, as `Prune` uses to detach a leaf.
pub fn encode_control(
    header: Header,
    kind: ControlKind,
    tree: u8,
    entries: &[ControlEntry],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    buf: &mut PacketBuf,
) -> Result<usize, EncodeError> {
    let auth_overhead = if key.is_some() { AUTH_TAG_LEN } else { 0 };
    let room = limits
        .max_payload_bytes
        .saturating_sub(HEADER_LEN + BODY_PREFIX + auth_overhead);
    let fit = (room / CONTROL_ENTRY_BYTES).min(usize::from(u16::MAX));
    if fit == 0 && !entries.is_empty() {
        return Err(EncodeError::BudgetTooSmall);
    }
    let written = entries.len().min(fit);

    buf.inner.clear();
    buf.inner.resize(HEADER_LEN, 0);
    buf.inner.push(kind.code());
    buf.inner.push(tree);
    buf.inner.extend_from_slice(&(written as u16).to_le_bytes());
    for e in &entries[..written] {
        buf.inner.extend_from_slice(&e.node_id.0.to_le_bytes());
        buf.inner.extend_from_slice(&e.incarnation.to_le_bytes());
        buf.inner.extend_from_slice(&e.value.to_le_bytes());
    }

    let mut packet_header = header;
    packet_header.flags = FLAG_CONTROL;
    if key.is_some() {
        packet_header.flags |= FLAG_AUTHENTICATED;
    }
    packet_header.count_width = 0;
    packet_header.cell_count = 0;
    packet_header.body_len = (buf.inner.len() - HEADER_LEN) as u32;
    packet_header.min_origin_sequence = 0;
    packet_header.max_origin_sequence = 0;
    packet_header.edge_sequence = 0;
    patch_header(&mut buf.inner[..HEADER_LEN], &packet_header);

    if let Some(key) = key {
        let tag = auth::sign(key, &buf.inner);
        buf.inner.extend_from_slice(&tag);
    }
    Ok(written)
}

/// Decode a control frame, handing each entry to `on_entry`. Verifies the
/// tag first when `key` is set.
pub fn decode_control(
    bytes: &[u8],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    mut on_entry: impl FnMut(ControlEntry),
) -> Result<DecodedControl, DecodeError> {
    if bytes.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }
    let auth_overhead = if key.is_some() { AUTH_TAG_LEN } else { 0 };
    if bytes.len() < HEADER_LEN + BODY_PREFIX + auth_overhead {
        return Err(DecodeError::TooShort);
    }
    let frame = match key {
        Some(key) => {
            let (frame, tag) = bytes.split_at(bytes.len() - AUTH_TAG_LEN);
            auth::verify(key, frame, tag).map_err(|_| DecodeError::AuthenticationFailed)?;
            frame
        }
        None => bytes,
    };
    let header = read_header(frame)?;
    if header.flags & FLAG_CONTROL == 0 || header.body_len as usize != frame.len() - HEADER_LEN {
        return Err(DecodeError::BodyLenMismatch);
    }
    let body = &frame[HEADER_LEN..];
    let kind = ControlKind::from_code(body[0]).ok_or(DecodeError::BadControl)?;
    let tree = body[1];
    let count = usize::from(u16::from_le_bytes([body[2], body[3]]));
    let rows = &body[BODY_PREFIX..];
    if rows.len() != count * CONTROL_ENTRY_BYTES {
        return Err(DecodeError::BadControl);
    }
    let (rows, _) = rows.as_chunks::<CONTROL_ENTRY_BYTES>();
    for row in rows {
        on_entry(ControlEntry {
            node_id: NodeId(u128::from_le_bytes(std::array::from_fn(|i| row[i]))),
            incarnation: u32::from_le_bytes(std::array::from_fn(|i| row[16 + i])),
            value: u64::from_le_bytes(std::array::from_fn(|i| row[20 + i])),
        });
    }
    Ok(DecodedControl { header, kind, tree })
}

#[cfg(test)]
mod tests;
