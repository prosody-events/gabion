//! Control frames: the cell-less messages of the Plumtree overlay.
//!
//! A control frame is the usual 72-byte header followed by `cell_count`
//! fixed-size entries and, when authenticated, the HMAC tag. The header's
//! frame kind is [`FrameKind::Control`] and the control kind sits in the flag
//! byte above it.
//!
//! | kind    | entries                         | header `min`/`max_origin_sequence` |
//! |---------|---------------------------------|------------------------------------|
//! | `IHave` | `(origin, incarnation, highest sequence held)` | 0 |
//! | `Graft` | `(origin, incarnation, send me cells above this sequence)` | 0 |
//! | `Pull`  | as `Graft`, without joining the tree | 0 |
//! | `Prune` | `(origin, incarnation, 0)`      | 0 |
//! | `Nack`  | none                            | first number of a 64-packet window, and a mask of its missing numbers |
//! | `Digest` | `(origin, incarnation, count of its settled cells)` | the sender's clock, which fixes the settled buckets |

use super::auth::{self, AUTH_TAG_LEN};
use super::header::{
    FLAG_AUTHENTICATED, FLAG_MORE, FrameKind, HEADER_LEN, Header, patch_header, read_header,
};
use super::{DecodeError, FrameLimits, HmacKey, PacketBuf};
use crate::crdt::{Incarnation, NodeId};

/// Bytes per entry: node id (16), incarnation (4), sequence (8).
pub const CONTROL_ENTRY_BYTES: usize = 28;

const CONTROL_KIND_SHIFT: u16 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlKind {
    /// Lazy announcement: "I hold this origin up to this sequence".
    IHave = 1,
    /// "I never got what you announced: send it and make this link eager."
    Graft = 2,
    /// "You already sent me this origin by another path: make this link lazy."
    Prune = 3,
    /// "These eager packets never arrived: send them again." The header's
    /// `min_origin_sequence` is the first number of a 64-packet window and
    /// `max_origin_sequence` a mask whose bit `j` stands for number `min + j`.
    Nack = 4,
    /// A catch-up request for an origin's live cells that leaves the tree
    /// alone.
    Pull = 5,
    /// Repair: "this is how many hits I hold of each origin in buckets that
    /// should have settled". The header's `min_origin_sequence` is the
    /// sender's clock in milliseconds, which fixes which buckets count.
    Digest = 6,
}

impl ControlKind {
    fn from_flags(flags: u16) -> Option<Self> {
        match flags >> CONTROL_KIND_SHIFT {
            1 => Some(Self::IHave),
            2 => Some(Self::Graft),
            3 => Some(Self::Prune),
            4 => Some(Self::Nack),
            5 => Some(Self::Pull),
            6 => Some(Self::Digest),
            _ => None,
        }
    }
}

/// One `(origin, incarnation, sequence)` triple; what `sequence` means
/// depends on the [`ControlKind`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControlEntry {
    pub node_id: NodeId,
    pub incarnation: Incarnation,
    pub sequence: u64,
}

/// Encode as many of `entries` as fit one packet into `buf` and return how
/// many that was. `header` supplies the sender identity and, for `Nack`, the
/// window and mask in `min_origin_sequence` and `max_origin_sequence`; its
/// other fields are overwritten. A frame that leaves entries behind has
/// [`FLAG_MORE`] set, so the caller encodes the rest into the next buffer.
pub fn encode_control(
    buf: &mut PacketBuf,
    header: Header,
    kind: ControlKind,
    entries: &[ControlEntry],
    key: Option<&HmacKey>,
    limits: FrameLimits,
) -> usize {
    let auth_overhead = if key.is_some() { AUTH_TAG_LEN } else { 0 };
    let budget = limits
        .max_payload_bytes
        .saturating_sub(HEADER_LEN + auth_overhead);
    let fit = (budget / CONTROL_ENTRY_BYTES).min(limits.max_cells as usize);
    let n = entries.len().min(fit);

    buf.inner.clear();
    buf.inner.resize(HEADER_LEN, 0);
    for e in &entries[..n] {
        buf.inner.extend_from_slice(&e.node_id.0.to_le_bytes());
        buf.inner.extend_from_slice(&e.incarnation.to_le_bytes());
        buf.inner.extend_from_slice(&e.sequence.to_le_bytes());
    }

    let mut flags = FrameKind::Control.flags() | ((kind as u16) << CONTROL_KIND_SHIFT);
    if n < entries.len() {
        flags |= FLAG_MORE;
    }
    if key.is_some() {
        flags |= FLAG_AUTHENTICATED;
    }
    let header = Header {
        count_width: 0,
        cell_count: n as u32,
        body_len: (n * CONTROL_ENTRY_BYTES) as u32,
        flags,
        ..header
    };
    patch_header(&mut buf.inner[..HEADER_LEN], &header);
    if let Some(key) = key {
        let tag = auth::sign(key, &buf.inner);
        buf.inner.extend_from_slice(&tag);
    }
    n
}

pub fn decode_control_unauth(
    bytes: &[u8],
    limits: FrameLimits,
    visit: impl FnMut(ControlEntry),
) -> Result<(Header, ControlKind), DecodeError> {
    decode_control(bytes, None, limits, visit)
}

pub fn decode_control_auth(
    bytes: &[u8],
    key: &HmacKey,
    limits: FrameLimits,
    visit: impl FnMut(ControlEntry),
) -> Result<(Header, ControlKind), DecodeError> {
    decode_control(bytes, Some(key), limits, visit)
}

fn decode_control(
    bytes: &[u8],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    mut visit: impl FnMut(ControlEntry),
) -> Result<(Header, ControlKind), DecodeError> {
    if bytes.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }
    let frame = match key {
        Some(key) => {
            if bytes.len() < HEADER_LEN + AUTH_TAG_LEN {
                return Err(DecodeError::TooShort);
            }
            let (frame, tag) = bytes.split_at(bytes.len() - AUTH_TAG_LEN);
            auth::verify(key, frame, tag).map_err(|_| DecodeError::AuthenticationFailed)?;
            frame
        }
        None => bytes,
    };
    let header = read_header(frame)?;
    let kind = ControlKind::from_flags(header.flags).ok_or(DecodeError::BadControl)?;
    let entries = header.cell_count as usize;
    if entries > limits.max_cells as usize
        || header.body_len as usize != entries * CONTROL_ENTRY_BYTES
        || frame.len() != HEADER_LEN + header.body_len as usize
    {
        return Err(DecodeError::BadControl);
    }
    let (rows, _) = frame[HEADER_LEN..].as_chunks::<CONTROL_ENTRY_BYTES>();
    for row in rows {
        let (node_id, rest) = row.split_at(16);
        let (incarnation, sequence) = rest.split_at(4);
        visit(ControlEntry {
            node_id: NodeId(u128::from_le_bytes(node_id.try_into().unwrap_or_default())),
            incarnation: u32::from_le_bytes(incarnation.try_into().unwrap_or_default()),
            sequence: u64::from_le_bytes(sequence.try_into().unwrap_or_default()),
        });
    }
    Ok((header, kind))
}
