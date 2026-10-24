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
//! [ entries     : N × size ]
//! ```
//!
//! An entry is `node_id u128, incarnation u32, value u64` (28 bytes).
//! [`ControlKind::Graft`], which names cell versions, adds `rule_fingerprint
//! u128, key_hash u128, bucket u32` (64 bytes). What `value` means depends on
//! `kind`; see [`ControlKind`].
//!
//! [`ControlKind::Ihave`] names the same cell versions, many to a frame, so
//! its entries are grouped by origin and rule, and `entry_count` counts the
//! groups:
//!
//! ```text
//! group: [ node_id u128 ][ incarnation u32 ][ rule_fingerprint u128 ]
//!        [ bucket u32 ][ value u64 ][ n u16 ]                    50 bytes
//!        n × [ key_hash u128 ][ bucket − group bucket : u16 ]
//!            [ value − group value : u32 ]                       22 bytes
//! ```
//!
//! Entries sorted by origin, rule and value fill groups best; an entry that
//! is not of the group's origin and rule, or below or too far past its bucket
//! or value, starts a new group.

use crate::crdt::{BucketEpoch, Incarnation, KeyHash, NodeId};

use super::auth::{self, AUTH_TAG_LEN};
use super::header::{patch_header, read_header};
use super::{
    DecodeError, EncodeError, FLAG_AUTHENTICATED, FLAG_CONTROL, FrameLimits, HEADER_LEN, Header,
    HmacKey, PacketBuf,
};

pub(super) const BODY_PREFIX: usize = 4;
/// Bytes of one control entry that names an origin only.
pub const CONTROL_ENTRY_BYTES: usize = 16 + 4 + 8;
/// Bytes of one control entry that names a cell version.
pub const CHANGE_ENTRY_BYTES: usize = CONTROL_ENTRY_BYTES + 16 + 16 + 4;
/// Bytes of the fixed part of an IHAVE group.
const GROUP_HEAD: usize = 16 + 4 + 16 + 4 + 8 + 2;
/// Bytes of one IHAVE entry within its group.
const GROUP_ENTRY: usize = 16 + 2 + 4;

/// What a control frame asks of its receiver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlKind {
    /// "I hold this version of this cell": origin `node_id`'s change number
    /// `value` to the cell named by the rest of the entry. Lazy push: ids
    /// only, no cells.
    Ihave,
    /// "Send me the current cell for each entry whose `value` is not
    /// [`NO_CELLS`], and make me a tree neighbour for `tree`, for the roots
    /// the entries name." `tree == PULL_ONLY_TREE` asks for the cells alone.
    Graft,
    /// "Stop pushing origin `node_id`'s changes to me" for each entry, or,
    /// with no entries, "I am no longer your leaf in `tree`."
    Prune,
    /// "Edge packets numbered `value` never arrived."
    Nack,
    /// "My latest packet to you was numbered `value`." Lets a receiver notice
    /// that the end of a burst was lost.
    Probe,
    /// "Send me your next slice of live cells." No entries. How a node that
    /// joined with no state is brought up to date.
    Pull,
}

/// The kind byte of a digest frame: "here is what I hold of one rule's settled
/// buckets; push me what I lack." Its body is groups, not entries, so it is
/// not a [`ControlKind`]: [`super::decode_digest`] decodes it.
pub(super) const DIGEST_KIND: u8 = 7;

/// The `value` of a [`ControlKind::Graft`] entry that asks for no cells, only
/// a place in the tree.
pub const NO_CELLS: u64 = u64::MAX;

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
            Self::Pull => 6,
        }
    }

    /// Bytes of one entry of this kind; an IHAVE entry takes fewer within a
    /// group, and its group's share of the group's fixed part.
    pub fn entry_bytes(self) -> usize {
        match self {
            Self::Ihave | Self::Graft => CHANGE_ENTRY_BYTES,
            Self::Prune | Self::Nack | Self::Probe | Self::Pull => CONTROL_ENTRY_BYTES,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            1 => Self::Ihave,
            2 => Self::Graft,
            3 => Self::Prune,
            4 => Self::Nack,
            5 => Self::Probe,
            6 => Self::Pull,
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
    /// The cell a change is of. Zero in the kinds that do not carry it.
    pub rule_fingerprint: u128,
    pub key_hash: KeyHash,
    pub bucket: BucketEpoch,
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

/// Whether control frame `bytes` is a digest, which [`super::decode_digest`]
/// decodes. Reads only the kind, as [`is_control`] reads only the flags.
pub fn is_digest(bytes: &[u8]) -> bool {
    bytes.get(HEADER_LEN).copied() == Some(DIGEST_KIND)
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
    let smallest = match kind {
        ControlKind::Ihave => GROUP_HEAD + GROUP_ENTRY,
        _ => kind.entry_bytes(),
    };
    if room < smallest && !entries.is_empty() {
        return Err(EncodeError::BudgetTooSmall);
    }

    buf.inner.clear();
    buf.inner.resize(HEADER_LEN, 0);
    buf.inner.push(kind.code());
    buf.inner.push(tree);
    buf.inner.extend_from_slice(&0u16.to_le_bytes());
    let (written, count) = match kind {
        ControlKind::Ihave => encode_groups(&mut buf.inner, entries, room),
        _ => {
            let fit = (room / kind.entry_bytes()).min(usize::from(u16::MAX));
            let written = entries.len().min(fit);
            for e in &entries[..written] {
                buf.inner.extend_from_slice(&e.node_id.0.to_le_bytes());
                buf.inner.extend_from_slice(&e.incarnation.to_le_bytes());
                buf.inner.extend_from_slice(&e.value.to_le_bytes());
                if kind.entry_bytes() == CHANGE_ENTRY_BYTES {
                    buf.inner
                        .extend_from_slice(&e.rule_fingerprint.to_le_bytes());
                    buf.inner.extend_from_slice(&e.key_hash.0.to_le_bytes());
                    buf.inner.extend_from_slice(&e.bucket.to_le_bytes());
                }
            }
            (written, written as u16)
        }
    };
    buf.inner[HEADER_LEN + 2..HEADER_LEN + BODY_PREFIX].copy_from_slice(&count.to_le_bytes());

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
    if kind == ControlKind::Ihave {
        decode_groups(rows, count, on_entry)?;
        return Ok(DecodedControl { header, kind, tree });
    }
    let width = kind.entry_bytes();
    if rows.len() != count * width {
        return Err(DecodeError::BadControl);
    }
    for row in rows.chunks_exact(width) {
        let mut entry = ControlEntry {
            node_id: NodeId(u128::from_le_bytes(std::array::from_fn(|i| row[i]))),
            incarnation: u32::from_le_bytes(std::array::from_fn(|i| row[16 + i])),
            value: u64::from_le_bytes(std::array::from_fn(|i| row[20 + i])),
            ..ControlEntry::default()
        };
        if width == CHANGE_ENTRY_BYTES {
            entry.rule_fingerprint = u128::from_le_bytes(std::array::from_fn(|i| row[28 + i]));
            entry.key_hash = KeyHash(u128::from_le_bytes(std::array::from_fn(|i| row[44 + i])));
            entry.bucket = u32::from_le_bytes(std::array::from_fn(|i| row[60 + i]));
        }
        on_entry(entry);
    }
    Ok(DecodedControl { header, kind, tree })
}

/// Whether `e` can join the IHAVE group that starts with `first`.
fn joins(first: &ControlEntry, e: &ControlEntry) -> bool {
    (e.node_id, e.incarnation, e.rule_fingerprint)
        == (first.node_id, first.incarnation, first.rule_fingerprint)
        && e.bucket
            .checked_sub(first.bucket)
            .is_some_and(|d| d <= u32::from(u16::MAX))
        && e.value
            .checked_sub(first.value)
            .is_some_and(|d| d <= u64::from(u32::MAX))
}

/// Write as many of `entries` as fit `room` bytes as IHAVE groups. Returns
/// how many entries and how many groups were written.
fn encode_groups(out: &mut Vec<u8>, entries: &[ControlEntry], mut room: usize) -> (usize, u16) {
    let mut written = 0;
    let mut groups = 0u16;
    while let Some(first) = entries.get(written)
        && room >= GROUP_HEAD + GROUP_ENTRY
        && groups < u16::MAX
    {
        let fit = ((room - GROUP_HEAD) / GROUP_ENTRY).min(usize::from(u16::MAX));
        let n = entries[written..]
            .iter()
            .take(fit)
            .take_while(|e| joins(first, e))
            .count();
        out.extend_from_slice(&first.node_id.0.to_le_bytes());
        out.extend_from_slice(&first.incarnation.to_le_bytes());
        out.extend_from_slice(&first.rule_fingerprint.to_le_bytes());
        out.extend_from_slice(&first.bucket.to_le_bytes());
        out.extend_from_slice(&first.value.to_le_bytes());
        out.extend_from_slice(&(n as u16).to_le_bytes());
        for e in &entries[written..written + n] {
            out.extend_from_slice(&e.key_hash.0.to_le_bytes());
            out.extend_from_slice(&((e.bucket - first.bucket) as u16).to_le_bytes());
            out.extend_from_slice(&((e.value - first.value) as u32).to_le_bytes());
        }
        room -= GROUP_HEAD + n * GROUP_ENTRY;
        written += n;
        groups += 1;
    }
    (written, groups)
}

/// Read `groups` IHAVE groups that must fill `rows` exactly.
fn decode_groups(
    mut rows: &[u8],
    groups: usize,
    mut on_entry: impl FnMut(ControlEntry),
) -> Result<(), DecodeError> {
    for _ in 0..groups {
        let head = rows.get(..GROUP_HEAD).ok_or(DecodeError::BadControl)?;
        let node_id = NodeId(u128::from_le_bytes(std::array::from_fn(|i| head[i])));
        let incarnation = u32::from_le_bytes(std::array::from_fn(|i| head[16 + i]));
        let rule_fingerprint = u128::from_le_bytes(std::array::from_fn(|i| head[20 + i]));
        let bucket = u32::from_le_bytes(std::array::from_fn(|i| head[36 + i]));
        let value = u64::from_le_bytes(std::array::from_fn(|i| head[40 + i]));
        let n = usize::from(u16::from_le_bytes([head[48], head[49]]));
        let body = rows
            .get(GROUP_HEAD..GROUP_HEAD + n * GROUP_ENTRY)
            .ok_or(DecodeError::BadControl)?;
        for row in body.as_chunks::<GROUP_ENTRY>().0 {
            on_entry(ControlEntry {
                node_id,
                incarnation,
                value: value.saturating_add(u64::from(u32::from_le_bytes(std::array::from_fn(
                    |i| row[18 + i],
                )))),
                rule_fingerprint,
                key_hash: KeyHash(u128::from_le_bytes(std::array::from_fn(|i| row[i]))),
                bucket: bucket.saturating_add(u32::from(u16::from_le_bytes([row[16], row[17]]))),
            });
        }
        rows = &rows[GROUP_HEAD + n * GROUP_ENTRY..];
    }
    if rows.is_empty() {
        Ok(())
    } else {
        Err(DecodeError::BadControl)
    }
}

#[cfg(test)]
mod tests;
