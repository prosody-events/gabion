//! Digest frames: one node's summary of the settled cells it holds for one
//! rule, which the repair lane compares against a peer's.
//!
//! A digest is a control frame ([`FLAG_CONTROL`]) with this body, and an HMAC
//! tag when authenticated:
//!
//! ```text
//! [ kind         : u8     ]    7, beside the ControlKind codes
//! [ reserved     : u8     ]
//! [ group_count  : u16    ]    G
//! [ rule_fp      : u128   ]
//! [ first_bucket : u32    ]
//! [ buckets      : u8     ]    N, the settled buckets first_bucket ..
//! [ lo           : u128   ]    origins with node id in [lo, hi) are all
//! [ hi           : u128   ]    listed: one not listed holds nothing there
//! [ groups       : G × (node_id u128, incarnation u32, sums u32 × N) ]
//! ```
//!
//! A group names one origin and, per bucket, the sum of the counts of that
//! origin's cells of the rule the sender holds. Groups are in node id order,
//! and a digest that spans several frames gives each frame the slice of node
//! ids it covers, so each frame is decodable, and useful, on its own.

use crate::crdt::{BucketEpoch, Incarnation, NodeId};

use super::auth::{self, AUTH_TAG_LEN};
use super::control::{BODY_PREFIX, DIGEST_KIND};
use super::header::{patch_header, read_header};
use super::{
    DecodeError, EncodeError, FLAG_AUTHENTICATED, FLAG_CONTROL, FrameLimits, HEADER_LEN, Header,
    HmacKey, PacketBuf,
};

#[cfg(test)]
mod tests;

/// Bytes of a digest body before its groups.
const FIXED: usize = BODY_PREFIX + 16 + 4 + 1 + 16 + 16;
/// Bytes of a group before its sums.
const GROUP_IDENT: usize = 16 + 4;
const LO_AT: usize = HEADER_LEN + BODY_PREFIX + 16 + 4 + 1;
const HI_AT: usize = LO_AT + 16;

/// The fixed part of a decoded digest frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DigestFrame {
    pub header: Header,
    pub rule_fingerprint: u128,
    pub first_bucket: BucketEpoch,
    pub buckets: u8,
    /// Every origin whose node id is in `lo..hi` and that holds anything in
    /// these buckets is listed in this frame.
    pub lo: u128,
    pub hi: u128,
}

/// One group's per-bucket sums, as they lie in the frame.
#[derive(Clone, Copy)]
pub struct DigestSums<'a>(&'a [u8]);

impl DigestSums<'_> {
    pub fn len(&self) -> usize {
        self.0.len() / 4
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The sum of bucket `first_bucket + i`.
    pub fn get(&self, i: usize) -> u32 {
        u32::from_le_bytes(std::array::from_fn(|k| self.0[4 * i + k]))
    }
}

/// Writes one digest frame into a send buffer, group by group.
pub struct DigestWriter<'b> {
    buf: &'b mut PacketBuf,
    buckets: usize,
    groups: u16,
    /// Bytes left for groups.
    room: usize,
}

impl<'b> DigestWriter<'b> {
    /// Start a frame for `buckets` buckets of rule `rule_fingerprint` from
    /// `first_bucket`, covering origins from node id `lo`.
    pub fn begin(
        buf: &'b mut PacketBuf,
        rule_fingerprint: u128,
        first_bucket: BucketEpoch,
        buckets: u8,
        lo: u128,
        authenticated: bool,
        limits: FrameLimits,
    ) -> Result<Self, EncodeError> {
        let tag = if authenticated { AUTH_TAG_LEN } else { 0 };
        let group = GROUP_IDENT + 4 * usize::from(buckets);
        let room = limits
            .max_payload_bytes
            .checked_sub(HEADER_LEN + FIXED + tag)
            .filter(|&room| room >= group)
            .ok_or(EncodeError::BudgetTooSmall)?;
        buf.inner.clear();
        buf.inner.resize(HEADER_LEN, 0);
        buf.inner.push(DIGEST_KIND);
        buf.inner.push(0);
        buf.inner.extend_from_slice(&0u16.to_le_bytes());
        buf.inner.extend_from_slice(&rule_fingerprint.to_le_bytes());
        buf.inner.extend_from_slice(&first_bucket.to_le_bytes());
        buf.inner.push(buckets);
        buf.inner.extend_from_slice(&lo.to_le_bytes());
        buf.inner.extend_from_slice(&u128::MAX.to_le_bytes());
        Ok(Self {
            buf,
            buckets: usize::from(buckets),
            groups: 0,
            room,
        })
    }

    /// Whether one more group fits.
    pub fn has_room(&self) -> bool {
        self.room >= GROUP_IDENT + 4 * self.buckets && self.groups < u16::MAX
    }

    /// Append a group: `sums` yields exactly the frame's bucket count. The
    /// caller checks [`Self::has_room`] first.
    pub fn push(
        &mut self,
        node_id: NodeId,
        incarnation: Incarnation,
        sums: impl Iterator<Item = u32>,
    ) {
        debug_assert!(self.has_room());
        let start = self.buf.inner.len();
        self.buf.inner.extend_from_slice(&node_id.0.to_le_bytes());
        self.buf.inner.extend_from_slice(&incarnation.to_le_bytes());
        for sum in sums.take(self.buckets) {
            self.buf.inner.extend_from_slice(&sum.to_le_bytes());
        }
        debug_assert_eq!(self.buf.inner.len() - start, GROUP_IDENT + 4 * self.buckets);
        self.room -= GROUP_IDENT + 4 * self.buckets;
        self.groups += 1;
    }

    /// Close the frame: it covers origins up to node id `hi`, exclusive.
    pub fn finish(self, header: Header, hi: u128, key: Option<&HmacKey>) {
        let inner = &mut self.buf.inner;
        inner[HEADER_LEN + 2..HEADER_LEN + 4].copy_from_slice(&self.groups.to_le_bytes());
        inner[HI_AT..HI_AT + 16].copy_from_slice(&hi.to_le_bytes());
        let mut packet_header = header;
        packet_header.flags = FLAG_CONTROL;
        if key.is_some() {
            packet_header.flags |= FLAG_AUTHENTICATED;
        }
        packet_header.count_width = 0;
        packet_header.cell_count = 0;
        packet_header.body_len = (inner.len() - HEADER_LEN) as u32;
        packet_header.min_origin_sequence = 0;
        packet_header.max_origin_sequence = 0;
        packet_header.edge_sequence = 0;
        patch_header(&mut inner[..HEADER_LEN], &packet_header);
        if let Some(key) = key {
            let tag = auth::sign(key, inner);
            inner.extend_from_slice(&tag);
        }
    }
}

/// Decode a digest frame, handing each group to `on_group`. Verifies the tag
/// first when `key` is set.
pub fn decode_digest(
    bytes: &[u8],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    mut on_group: impl FnMut(NodeId, Incarnation, DigestSums<'_>),
) -> Result<DigestFrame, DecodeError> {
    if bytes.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }
    let tag = if key.is_some() { AUTH_TAG_LEN } else { 0 };
    if bytes.len() < HEADER_LEN + FIXED + tag {
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
    if frame[HEADER_LEN] != DIGEST_KIND {
        return Err(DecodeError::BadControl);
    }
    let at = |i: usize| HEADER_LEN + i;
    let groups = usize::from(u16::from_le_bytes([frame[at(2)], frame[at(3)]]));
    let u128_at = |i: usize| u128::from_le_bytes(std::array::from_fn(|k| frame[i + k]));
    let rule_fingerprint = u128_at(at(BODY_PREFIX));
    let first_bucket = u32::from_le_bytes(std::array::from_fn(|k| frame[at(BODY_PREFIX + 16) + k]));
    let buckets = frame[at(BODY_PREFIX + 20)];
    let lo = u128_at(LO_AT);
    let hi = u128_at(HI_AT);
    let width = GROUP_IDENT + 4 * usize::from(buckets);
    let rows = &frame[HEADER_LEN + FIXED..];
    if rows.len() != groups * width {
        return Err(DecodeError::BadControl);
    }
    for row in rows.chunks_exact(width) {
        let node_id = NodeId(u128::from_le_bytes(std::array::from_fn(|k| row[k])));
        let incarnation = u32::from_le_bytes(std::array::from_fn(|k| row[16 + k]));
        on_group(node_id, incarnation, DigestSums(&row[GROUP_IDENT..]));
    }
    Ok(DigestFrame {
        header,
        rule_fingerprint,
        first_bucket,
        buckets,
        lo,
        hi,
    })
}
