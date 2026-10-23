//! Control frames: the four GossipSub control messages (`GRAFT`, `PRUNE`,
//! `IHAVE`, `IWANT`), one per datagram.
//!
//! A control frame reuses the data frame's header and authentication tag so a
//! cluster key covers both; it is told apart by [`FLAG_CONTROL`], and carries
//! no cells (`cell_count == 0`). Message ids are 64-bit and the body is:
//!
//! ```text
//! [ kind : u8 ]                       1 GRAFT, 2 PRUNE, 3 IHAVE, 4 IWANT
//! GRAFT: nothing more
//! PRUNE: [ backoff_secs : u16 ]
//! IHAVE / IWANT: [ n : u16 ][ id : u64 × n ]
//! ```

use super::auth::{self, AUTH_TAG_LEN};
use super::header::{
    FLAG_AUTHENTICATED, FLAG_CONTROL, HEADER_LEN, Header, patch_header, read_header,
};
use super::{DecodeError, EncodeError, FrameLimits, HmacKey, PacketBuf};

const KIND_GRAFT: u8 = 1;
const KIND_PRUNE: u8 = 2;
const KIND_IHAVE: u8 = 3;
const KIND_IWANT: u8 = 4;

/// One control message. `IHAVE` and `IWANT` carry their ids beside it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Control {
    /// Ask the receiver to add the sender to its mesh.
    Graft,
    /// Tell the receiver it left the sender's mesh and must not GRAFT back
    /// for `backoff_secs`.
    Prune { backoff_secs: u16 },
    /// Advertise ids (only) of messages the sender holds.
    IHave,
    /// Request the full messages for ids the sender lacks.
    IWant,
}

/// Most ids one `IHAVE` / `IWANT` datagram can carry within `limits`.
pub fn max_ids_per_frame(limits: FrameLimits, authenticated: bool) -> usize {
    let auth = if authenticated { AUTH_TAG_LEN } else { 0 };
    limits
        .max_payload_bytes
        .saturating_sub(HEADER_LEN + auth + 3)
        / 8
}

/// Encode one control frame into `buf`. `ids` is read only for `IHave` and
/// `IWant` and must fit [`max_ids_per_frame`].
pub fn encode_control(
    mut header: Header,
    control: Control,
    ids: &[u64],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    buf: &mut PacketBuf,
) -> Result<(), EncodeError> {
    let id_count = match control {
        Control::IHave | Control::IWant => ids.len(),
        Control::Graft | Control::Prune { .. } => 0,
    };
    if id_count > max_ids_per_frame(limits, key.is_some()) {
        return Err(EncodeError::BudgetTooSmall);
    }
    buf.inner.clear();
    buf.inner.resize(HEADER_LEN, 0);
    match control {
        Control::Graft => buf.inner.push(KIND_GRAFT),
        Control::Prune { backoff_secs } => {
            buf.inner.push(KIND_PRUNE);
            buf.inner.extend_from_slice(&backoff_secs.to_le_bytes());
        }
        Control::IHave | Control::IWant => {
            buf.inner.push(if control == Control::IHave {
                KIND_IHAVE
            } else {
                KIND_IWANT
            });
            buf.inner
                .extend_from_slice(&(id_count as u16).to_le_bytes());
            for id in ids {
                buf.inner.extend_from_slice(&id.to_le_bytes());
            }
        }
    }
    header.flags = FLAG_CONTROL | if key.is_some() { FLAG_AUTHENTICATED } else { 0 };
    header.count_width = 0;
    header.cell_count = 0;
    header.body_len = (buf.inner.len() - HEADER_LEN) as u32;
    header.min_origin_sequence = 0;
    header.max_origin_sequence = 0;
    patch_header(&mut buf.inner[..HEADER_LEN], &header);
    if let Some(key) = key {
        let tag = auth::sign(key, &buf.inner);
        buf.inner.extend_from_slice(&tag);
    }
    debug_assert!(buf.inner.len() <= limits.max_payload_bytes);
    Ok(())
}

/// Decode one control frame. The ids of an `IHave` / `IWant` are appended to
/// `ids` after clearing it.
pub fn decode_control(
    bytes: &[u8],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    ids: &mut Vec<u64>,
) -> Result<(Header, Control), DecodeError> {
    if bytes.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }
    let frame = match key {
        Some(key) => {
            if bytes.len() < HEADER_LEN + AUTH_TAG_LEN {
                return Err(DecodeError::TooShort);
            }
            let (frame, tag) = bytes.split_at(bytes.len() - AUTH_TAG_LEN);
            auth::verify(key, frame, tag)?;
            frame
        }
        None => bytes,
    };
    let header = read_header(frame)?;
    let body = &frame[HEADER_LEN..];
    if header.flags & FLAG_CONTROL == 0 || header.body_len as usize != body.len() {
        return Err(DecodeError::BodyLenMismatch);
    }
    ids.clear();
    let (&kind, rest) = body.split_first().ok_or(DecodeError::TooShort)?;
    let control = match kind {
        KIND_GRAFT if rest.is_empty() => Control::Graft,
        KIND_PRUNE if rest.len() == 2 => Control::Prune {
            backoff_secs: u16::from_le_bytes([rest[0], rest[1]]),
        },
        KIND_IHAVE | KIND_IWANT if rest.len() >= 2 => {
            let n = u16::from_le_bytes([rest[0], rest[1]]) as usize;
            let list = &rest[2..];
            if list.len() != n * 8 {
                return Err(DecodeError::BodyLenMismatch);
            }
            ids.extend(
                list.as_chunks::<8>()
                    .0
                    .iter()
                    .map(|id| u64::from_le_bytes(*id)),
            );
            if kind == KIND_IHAVE {
                Control::IHave
            } else {
                Control::IWant
            }
        }
        _ => return Err(DecodeError::BodyLenMismatch),
    };
    Ok((header, control))
}

#[cfg(test)]
mod tests;
