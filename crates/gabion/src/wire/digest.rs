//! Digest frames: the repair half of the gossip protocol.
//!
//! A digest tells a peer which origin sequences the sender holds, per origin,
//! so the peer can answer with the cells the sender lacks (Demers et al.,
//! PODC 1987, anti-entropy: compare, then send only the difference). One entry
//! per origin carries the highest sequence held and a bitmap of which of the
//! [`DIGEST_WINDOW`] sequences up to it are held. Sequences further back are
//! assumed held: they have normally aged out of the live window by then.
//!
//! Every packet is independently decodable. It names the half-open range of
//! origin keys it speaks for (`scope`), so an origin that falls inside the
//! scope but has no entry is one the sender holds nothing for.
//!
//! Layout, little-endian, after the common header:
//!
//! ```text
//! [ scope.from : u128 node_id, u32 incarnation ]
//! [ scope.to   : u128 node_id, u32 incarnation ]   exclusive
//! [ entry_count : u16 ]
//! [ entry : u128 node_id, u32 incarnation, u64 max_sequence,
//!           u64 × DIGEST_WORDS held ] × entry_count
//! ```

use crate::crdt::{Incarnation, NodeId};
use crate::defaults::GOSSIP_DIGEST_WINDOW_WORDS;

use super::auth::{self, AUTH_TAG_LEN};
use super::header::{FLAG_AUTHENTICATED, FLAG_DIGEST, FLAG_MORE, patch_header};
use super::{
    DecodeError, EncodeError, FrameLimits, HEADER_LEN, Header, HmacKey, PacketBuf, frame_body,
    open_frame,
};

const DIGEST_WORDS: usize = GOSSIP_DIGEST_WINDOW_WORDS;

/// How many sequences below an origin's maximum a digest entry tracks.
pub const DIGEST_WINDOW: u64 = 64 * DIGEST_WORDS as u64;

const KEY_BYTES: usize = 16 + 4;
const SCOPE_BYTES: usize = 2 * KEY_BYTES;
const COUNT_BYTES: usize = 2;
const ENTRY_BYTES: usize = KEY_BYTES + 8 + 8 * DIGEST_WORDS;
const BODY_OVERHEAD: usize = SCOPE_BYTES + COUNT_BYTES;

/// One origin as the digest names it, ordered by node id then incarnation.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OriginKey {
    pub node_id: NodeId,
    pub incarnation: Incarnation,
}

impl OriginKey {
    /// Upper bound of the last packet's scope. No origin has this key.
    const END: Self = Self {
        node_id: NodeId(u128::MAX),
        incarnation: u32::MAX,
    };
    const MIN: Self = Self {
        node_id: NodeId(0),
        incarnation: 0,
    };
}

/// The origin keys a digest packet speaks for: `from <= key < to`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DigestScope {
    pub from: OriginKey,
    pub to: OriginKey,
}

impl DigestScope {
    pub fn contains(&self, key: OriginKey) -> bool {
        self.from <= key && key < self.to
    }
}

/// What the sender holds of one origin.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DigestEntry {
    pub origin: OriginKey,
    pub max_sequence: u64,
    /// Bit `d` is set when sequence `max_sequence - d` is held.
    held: [u64; DIGEST_WORDS],
}

impl DigestEntry {
    pub fn new(origin: OriginKey, max_sequence: u64) -> Self {
        Self {
            origin,
            max_sequence,
            held: [0; DIGEST_WORDS],
        }
    }

    /// Record that `sequence` is held. Sequences above the maximum or
    /// outside the window are not tracked.
    pub fn mark(&mut self, sequence: u64) {
        if let Some(back) = self.max_sequence.checked_sub(sequence)
            && back < DIGEST_WINDOW
        {
            self.held[(back / 64) as usize] |= 1 << (back % 64);
        }
    }

    /// Whether the sender holds `sequence`. Anything beyond the window is
    /// taken as held.
    pub fn holds(&self, sequence: u64) -> bool {
        match self.max_sequence.checked_sub(sequence) {
            None => false,
            Some(back) if back >= DIGEST_WINDOW => true,
            Some(back) => self.held[(back / 64) as usize] & (1 << (back % 64)) != 0,
        }
    }
}

/// Synchronous iterator over the packets of one digest. `entries` must be
/// sorted by origin; the packets tile the whole key space, so a digest of no
/// entries still says "I hold nothing" in a single packet.
pub struct DigestPackets<'a> {
    header: Header,
    entries: &'a [DigestEntry],
    key: Option<&'a HmacKey>,
    per_packet: usize,
    cursor: usize,
    done: bool,
}

impl<'a> DigestPackets<'a> {
    pub fn new(
        mut header: Header,
        entries: &'a [DigestEntry],
        key: Option<&'a HmacKey>,
        limits: FrameLimits,
    ) -> Result<Self, EncodeError> {
        debug_assert!(entries.is_sorted_by_key(|e| e.origin));
        let auth_overhead = if key.is_some() { AUTH_TAG_LEN } else { 0 };
        let room = limits
            .max_payload_bytes
            .saturating_sub(HEADER_LEN + auth_overhead + BODY_OVERHEAD);
        let per_packet = room / ENTRY_BYTES;
        if per_packet == 0 {
            return Err(EncodeError::BudgetTooSmall);
        }
        header.flags |= FLAG_DIGEST;
        Ok(Self {
            header,
            entries,
            key,
            per_packet,
            cursor: 0,
            done: false,
        })
    }

    /// Encode the next packet into `buf`, or return `false` once the digest
    /// is complete.
    pub fn next_into(&mut self, buf: &mut PacketBuf) -> bool {
        buf.inner.clear();
        if self.done {
            return false;
        }
        let end = (self.cursor + self.per_packet).min(self.entries.len());
        let chunk = &self.entries[self.cursor..end];
        let scope = DigestScope {
            from: match self.cursor {
                0 => OriginKey::MIN,
                _ => self.entries[self.cursor].origin,
            },
            to: self.entries.get(end).map_or(OriginKey::END, |e| e.origin),
        };
        let more = end < self.entries.len();

        buf.inner.resize(HEADER_LEN, 0);
        put_key(&mut buf.inner, scope.from);
        put_key(&mut buf.inner, scope.to);
        buf.inner
            .extend_from_slice(&(chunk.len() as u16).to_le_bytes());
        for entry in chunk {
            put_key(&mut buf.inner, entry.origin);
            buf.inner
                .extend_from_slice(&entry.max_sequence.to_le_bytes());
            for word in entry.held {
                buf.inner.extend_from_slice(&word.to_le_bytes());
            }
        }

        let mut header = self.header;
        header.flags |= if more { FLAG_MORE } else { 0 };
        if self.key.is_some() {
            header.flags |= FLAG_AUTHENTICATED;
        }
        header.body_len = (buf.inner.len() - HEADER_LEN) as u32;
        patch_header(&mut buf.inner[..HEADER_LEN], &header);
        if let Some(key) = self.key {
            let tag = auth::sign(key, &buf.inner);
            buf.inner.extend_from_slice(&tag);
        }
        debug_assert!(buf.inner.len() <= buf.capacity);

        self.cursor = end;
        self.done = !more;
        true
    }
}

fn put_key(out: &mut Vec<u8>, key: OriginKey) {
    out.extend_from_slice(&key.node_id.0.to_le_bytes());
    out.extend_from_slice(&key.incarnation.to_le_bytes());
}

fn read_key(bytes: &[u8]) -> OriginKey {
    OriginKey {
        node_id: NodeId(u128::from_le_bytes(
            bytes[..16].try_into().expect("16 bytes"),
        )),
        incarnation: u32::from_le_bytes(bytes[16..20].try_into().expect("4 bytes")),
    }
}

/// Decode one digest packet, handing each entry to `on_entry`. Returns the
/// header and the scope the packet speaks for.
pub fn decode_digest_visit(
    bytes: &[u8],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    mut on_entry: impl FnMut(DigestEntry),
) -> Result<(Header, DigestScope), DecodeError> {
    let (header, frame) = open_frame(bytes, key, limits)?;
    if header.flags & FLAG_DIGEST == 0 {
        return Err(DecodeError::NotADigest);
    }
    let body = frame_body(&header, frame)?;
    if body.len() < BODY_OVERHEAD {
        return Err(DecodeError::BodyLenMismatch);
    }
    let scope = DigestScope {
        from: read_key(&body[..KEY_BYTES]),
        to: read_key(&body[KEY_BYTES..SCOPE_BYTES]),
    };
    let count = u16::from_le_bytes([body[SCOPE_BYTES], body[SCOPE_BYTES + 1]]) as usize;
    let entries = &body[BODY_OVERHEAD..];
    if entries.len() != count * ENTRY_BYTES {
        return Err(DecodeError::BodyLenMismatch);
    }
    for raw in entries.as_chunks::<ENTRY_BYTES>().0 {
        let mut entry = DigestEntry::new(
            read_key(raw),
            u64::from_le_bytes(raw[KEY_BYTES..KEY_BYTES + 8].try_into().expect("8 bytes")),
        );
        for (held, word) in entry
            .held
            .iter_mut()
            .zip(raw[KEY_BYTES + 8..].as_chunks::<8>().0)
        {
            *held = u64::from_le_bytes(*word);
        }
        on_entry(entry);
    }
    Ok((header, scope))
}

#[cfg(test)]
mod tests;
