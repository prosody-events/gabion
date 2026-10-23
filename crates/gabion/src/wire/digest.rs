//! Scuttlebutt digest frames.
//!
//! A digest frame carries no cells. Its body names a slice of the node-id
//! space and the sender's watermark for each origin in that slice:
//!
//! ```text
//! [ range_lo : u128 ]                      first node id the slice covers
//! [ range_hi : u128 ]                      last node id the slice covers
//! [ node_id     : u128 ×N ]               N = header.cell_count, ascending
//! [ incarnation : u32  ×N ]
//! [ version     : u64  ×N ]               the sender's watermark for the origin
//! ```
//!
//! An origin inside `[range_lo, range_hi]` that the frame does not list is one
//! the sender holds nothing for, so the receiver answers it from version `0`.
//! That makes every frame self-describing: a large digest is cut into several
//! frames whose ranges tile the node-id space, and the receiver needs no state
//! linking them, which is what keeps a lost frame harmless.

use crate::crdt::{Incarnation, NodeId};

use super::auth::{self, AUTH_TAG_LEN};
use super::header::{FLAG_AUTHENTICATED, FLAG_DIGEST, FLAG_SYN, HEADER_LEN, Header, patch_header};
use super::{DecodeError, EncodeError, FrameLimits, HmacKey, PacketBuf};

const RANGE_BYTES: usize = 32;
const ENTRY_BYTES: usize = 16 + 4 + 8;

/// One origin's advertised watermark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DigestEntry {
    pub node_id: NodeId,
    pub incarnation: Incarnation,
    pub version: u64,
}

/// The node-id slice a digest frame covers, both ends inclusive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DigestRange {
    pub lo: u128,
    pub hi: u128,
}

impl DigestRange {
    pub const ALL: Self = Self {
        lo: 0,
        hi: u128::MAX,
    };

    pub fn contains(&self, node_id: NodeId) -> bool {
        (self.lo..=self.hi).contains(&node_id.0)
    }
}

/// How many entries one frame can carry within `limits`.
pub fn max_digest_entries(limits: FrameLimits, authenticated: bool) -> usize {
    let overhead = HEADER_LEN + RANGE_BYTES + if authenticated { AUTH_TAG_LEN } else { 0 };
    let by_payload = limits.max_payload_bytes.saturating_sub(overhead) / ENTRY_BYTES;
    by_payload.min(limits.max_cells as usize)
}

/// Encode one digest frame into `buf`. `header` supplies the sender
/// identity; the codec sets every other field. `syn` marks the frame as the
/// start of an exchange.
pub fn encode_digest(
    header: Header,
    syn: bool,
    range: DigestRange,
    entries: &[DigestEntry],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    buf: &mut PacketBuf,
) -> Result<(), EncodeError> {
    if entries.len() > max_digest_entries(limits, key.is_some()) {
        return Err(EncodeError::BudgetTooSmall);
    }
    let body_len = RANGE_BYTES + entries.len() * ENTRY_BYTES;
    let out = &mut buf.inner;
    out.clear();
    out.resize(HEADER_LEN, 0);
    out.extend_from_slice(&range.lo.to_le_bytes());
    out.extend_from_slice(&range.hi.to_le_bytes());
    for e in entries {
        out.extend_from_slice(&e.node_id.0.to_le_bytes());
    }
    for e in entries {
        out.extend_from_slice(&e.incarnation.to_le_bytes());
    }
    for e in entries {
        out.extend_from_slice(&e.version.to_le_bytes());
    }

    let mut flags = FLAG_DIGEST;
    if syn {
        flags |= FLAG_SYN;
    }
    if key.is_some() {
        flags |= FLAG_AUTHENTICATED;
    }
    let mut digest_header = header;
    digest_header.flags = flags;
    digest_header.count_width = 0;
    digest_header.cell_count = entries.len() as u32;
    digest_header.body_len = body_len as u32;
    digest_header.min_origin_sequence = 0;
    digest_header.max_origin_sequence = 0;
    patch_header(&mut out[..HEADER_LEN], &digest_header);
    if let Some(key) = key {
        let tag = auth::sign(key, &out[..HEADER_LEN + body_len]);
        out.extend_from_slice(&tag);
    }
    debug_assert!(out.len() <= limits.max_payload_bytes);
    Ok(())
}

/// Decode a digest body, handing each entry to `on_entry`, and return the
/// range the frame covers.
pub(crate) fn decode_digest_body(
    body: &[u8],
    entry_count: u32,
    mut on_entry: impl FnMut(DigestEntry),
) -> Result<DigestRange, DecodeError> {
    let n = entry_count as usize;
    if body.len() != RANGE_BYTES + n * ENTRY_BYTES {
        return Err(DecodeError::BodyLenMismatch);
    }
    let range = DigestRange {
        lo: u128::from_le_bytes(body[..16].try_into().expect("16 bytes")),
        hi: u128::from_le_bytes(body[16..32].try_into().expect("16 bytes")),
    };
    let ids = &body[RANGE_BYTES..RANGE_BYTES + 16 * n];
    let incarnations = &body[RANGE_BYTES + 16 * n..RANGE_BYTES + 20 * n];
    let versions = &body[RANGE_BYTES + 20 * n..];
    for i in 0..n {
        on_entry(DigestEntry {
            node_id: NodeId(u128::from_le_bytes(
                ids[i * 16..i * 16 + 16].try_into().expect("16 bytes"),
            )),
            incarnation: u32::from_le_bytes(
                incarnations[i * 4..i * 4 + 4].try_into().expect("4 bytes"),
            ),
            version: u64::from_le_bytes(versions[i * 8..i * 8 + 8].try_into().expect("8 bytes")),
        });
    }
    Ok(range)
}
