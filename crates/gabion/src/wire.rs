//! On-the-wire codec for per-origin counter CRDT gossip frames.
//!
//! Each UDP packet is self-describing: the body carries its own small
//! `(rule_fp, origin_node_id)` mini-dictionaries followed by per-cell slot
//! indices. A receiver can decode any packet in isolation regardless of
//! whether earlier or later packets in the same batch arrived — the only
//! cross-packet signal is the advisory [`FLAG_MORE`] hint, which receivers
//! may use to defer downstream work until a batch settles.
//!
//! Invariants:
//! - Frames are little-endian.
//! - Headers are exactly [`HEADER_LEN`] bytes.
//! - The encoder never writes past `FrameLimits::max_payload_bytes`.
//! - Decoders enforce `cell_count <= FrameLimits::max_cells` before any work.
//! - Authenticated frames reject any header / body / tag mutation before
//!   decoding the body.
//! - `count_width` is strict: the decoder's `C: Count` must match the sender's
//!   exactly.
//! - Each packet is independently useful — losing packet `k` of a batch does
//!   not invalidate packet `k+1` or any earlier packet.
//!
//! Socket I/O, peer selection, and tick scheduling live outside this module.

mod auth;
mod body;
mod header;

#[cfg(test)]
mod tests;

pub use auth::HmacKey;
pub use body::{WireCell, WireScratch};
pub use header::{
    FLAG_ACK, FLAG_AUTHENTICATED, FLAG_MORE, FLAG_NACK, FLAG_REPAIR, FLAG_SNAPSHOT, FLAG_SYNC,
    FrameKind, HEADER_LEN, Header, MAGIC, Nack, VERSION,
};

use auth::AUTH_TAG_LEN;
use body::{
    DICT_LEN_BYTES, PER_CELL_IDENT_BYTES, decode_body_visit, decode_totals_visit,
    encode_packet_body, encode_totals_body,
};
use header::{patch_header, read_header};

use crate::crdt::{CellHandle, CellStore, Count, KeyTotal, NodeId, Observation, ObservationBatch};

use thiserror::Error;

/// Limits applied at frame boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameLimits {
    pub max_payload_bytes: usize,
    pub max_cells: u32,
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 256 * 1024,
            max_cells: 4096,
        }
    }
}

/// Outcome metadata for one packet emitted by [`Packets::next_into`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PacketWritten {
    /// Cells encoded into this packet.
    pub cells_emitted: u32,
    /// Handles inspected for this packet that did not resolve (stale
    /// generation or missing dictionary descriptor). Stale handles are
    /// consumed in the current packet — never carried forward.
    pub cells_dropped: u32,
    /// Flags as written to the packet header.
    pub flags: u16,
}

/// Outcome of a successful decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedSummary {
    pub header: Header,
    pub cell_count: u32,
    /// Mirror of `header.flags`. Receivers may inspect [`FLAG_MORE`] to
    /// decide when a batch is fully delivered. Each packet is decoded and
    /// applied independently regardless of the bit's value.
    pub flags: u16,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum EncodeError {
    #[error("max_payload_bytes is smaller than the fixed framing overhead + one cell")]
    BudgetTooSmall,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    #[error("frame is shorter than the fixed framing overhead")]
    TooShort,
    #[error("magic mismatch")]
    BadMagic,
    #[error("unsupported version")]
    BadVersion,
    #[error("count_width does not match local Count width")]
    BadCountWidth,
    #[error("frame exceeds max_payload_bytes")]
    PayloadTooLarge,
    #[error("cell_count exceeds max_cells")]
    CapacityExceeded,
    #[error("body_len does not match frame size")]
    BodyLenMismatch,
    #[error("rule_slot or node_slot index out of range for the packet's dictionary")]
    BadSlot,
    #[error("HMAC verification failed")]
    AuthenticationFailed,
    #[error("trailing bytes after body")]
    Trailing,
    #[error("sync and ack flags are both set")]
    BadKind,
}

// -- send-side buffer -------------------------------------------------------

/// Send-side packet buffer with a fixed capacity of
/// `limits.max_payload_bytes`. The inner `Vec<u8>` is private so external
/// callers cannot grow it past capacity — the zero-allocation invariant
/// holds by construction.
#[derive(Debug)]
pub struct PacketBuf {
    inner: Vec<u8>,
    capacity: usize,
}

impl PacketBuf {
    /// Allocate one send buffer sized to `limits.max_payload_bytes`.
    pub fn for_limits(limits: FrameLimits) -> Self {
        Self {
            inner: Vec::with_capacity(limits.max_payload_bytes),
            capacity: limits.max_payload_bytes,
        }
    }

    /// The encoded packet's bytes. Pass this to `socket.send_to`.
    pub fn as_bytes(&self) -> &[u8] {
        &self.inner
    }

    /// Overwrite this buffer with `other`'s bytes. Both buffers share
    /// `limits`, so the copy never reallocates.
    pub fn copy_from(&mut self, other: &PacketBuf) {
        debug_assert!(other.inner.len() <= self.capacity);
        self.inner.clear();
        self.inner.extend_from_slice(&other.inner);
    }

    /// Maximum byte capacity (fixed at construction).
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl AsRef<[u8]> for PacketBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

// -- packet iterator --------------------------------------------------------

/// Compute the minimum bytes needed to hold one minimal cell. A minimal
/// packet has both dict lengths = 1, one rule fingerprint, one node id, and
/// one cell row. Used by the constructors to reject undersized budgets up
/// front rather than mid-stream.
fn min_payload_bytes(count_size: usize, auth_overhead: usize) -> usize {
    HEADER_LEN
        + DICT_LEN_BYTES        // rule_dict_len + node_dict_len
        + 16                    // one rule fingerprint
        + 16                    // one node id
        + PER_CELL_IDENT_BYTES  // one cell's identity columns
        + count_size            // its count
        + auth_overhead
}

/// Most key totals one packet holds, for sizing buffers that track them:
/// one rule fingerprint and `count_width`-wide totals in the budget left by
/// the header and, if `authenticated`, the tag.
pub fn max_totals_per_packet<C: Count>(limits: FrameLimits, authenticated: bool) -> usize {
    let auth_overhead = if authenticated { AUTH_TAG_LEN } else { 0 };
    limits
        .max_payload_bytes
        .saturating_sub(HEADER_LEN + auth_overhead + 1 + 16)
        / (body::PER_TOTAL_IDENT_BYTES + std::mem::size_of::<C>())
}

/// Synchronous iterator over outbound packets. Not `std::iter::Iterator` —
/// `next_into` takes a caller-owned buffer; `Iterator::next` cannot.
///
/// `#[must_use]` so undrained iterators warn at compile time. The `Drop`
/// impl additionally debug-asserts that the iterator was drained, so silent
/// data-loss bugs trip a panic in tests and debug builds.
#[must_use = "drain Packets to send every cell — undrained handles are lost"]
pub struct Packets<'a, C: Count> {
    header: Header,
    store: &'a CellStore<C>,
    rows: Rows<'a, C>,
    key: Option<&'a HmacKey>,
    scratch: &'a mut WireScratch,
    limits: FrameLimits,
    cursor: usize,
    /// Packets emitted so far; numbers a totals frame's packets.
    emitted: u64,
}

/// What a frame carries: stored cells (a sync or an acknowledgement) or key
/// totals (a broadcast, repair or snapshot, or none for a NACK).
#[derive(Clone, Copy)]
enum Rows<'a, C: Count> {
    Cells(&'a [CellHandle]),
    Totals(&'a [KeyTotal<C>]),
}

impl<C: Count> Rows<'_, C> {
    fn len(&self) -> usize {
        match self {
            Self::Cells(handles) => handles.len(),
            Self::Totals(totals) => totals.len(),
        }
    }
}

impl<C: Count> Drop for Packets<'_, C> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            debug_assert_eq!(
                self.remaining(),
                0,
                "Packets dropped with {} handles undrained",
                self.remaining(),
            );
        }
    }
}

impl<'a, C: Count> Packets<'a, C> {
    /// Build an unauthenticated packet iterator over stored cells, for a
    /// sync or an acknowledgement (the kind in `header.flags`). The
    /// `cluster_id_hash`, `sender_node_id`, `sender_incarnation` fields of
    /// `header` are copied into every packet; the codec overwrites
    /// `count_width`, `cell_count`, `body_len`, `flags`,
    /// `min/max_origin_sequence` on each packet.
    pub fn unauth(
        header: Header,
        store: &'a CellStore<C>,
        handles: &'a [CellHandle],
        scratch: &'a mut WireScratch,
        limits: FrameLimits,
    ) -> Result<Self, EncodeError> {
        Self::new(header, store, Rows::Cells(handles), None, scratch, limits)
    }

    /// A frame of key totals of the kind in `header.flags` (a broadcast,
    /// repair or snapshot; with no totals, also a NACK), authenticated when
    /// `key` is set. Unlike a frame of cells, it emits one packet even with
    /// no totals: an empty snapshot still says what it covers. Packets are
    /// numbered as [`FrameKind`] describes: packet `i` of a broadcast
    /// carries `max_origin_sequence + i`, packet `i` of a snapshot carries
    /// `min_origin_sequence = i`, and any other kind carries the header's
    /// sequence fields unchanged.
    pub fn totals(
        header: Header,
        store: &'a CellStore<C>,
        totals: &'a [KeyTotal<C>],
        key: Option<&'a HmacKey>,
        scratch: &'a mut WireScratch,
        limits: FrameLimits,
    ) -> Result<Self, EncodeError> {
        debug_assert!(
            FrameKind::from_flags(header.flags)
                .is_some_and(|kind| kind.carries_totals() || totals.is_empty()),
            "a frame of totals is a broadcast, repair or snapshot",
        );
        Self::new(header, store, Rows::Totals(totals), key, scratch, limits)
    }

    /// Authenticated variant — HMAC-SHA256 over each packet's
    /// `header || body`. [`FLAG_AUTHENTICATED`] is set on every emitted
    /// packet.
    pub fn auth(
        header: Header,
        store: &'a CellStore<C>,
        handles: &'a [CellHandle],
        key: &'a HmacKey,
        scratch: &'a mut WireScratch,
        limits: FrameLimits,
    ) -> Result<Self, EncodeError> {
        Self::new(
            header,
            store,
            Rows::Cells(handles),
            Some(key),
            scratch,
            limits,
        )
    }

    fn new(
        header: Header,
        store: &'a CellStore<C>,
        rows: Rows<'a, C>,
        key: Option<&'a HmacKey>,
        scratch: &'a mut WireScratch,
        limits: FrameLimits,
    ) -> Result<Self, EncodeError> {
        let count_size = std::mem::size_of::<C>();
        let auth_overhead = if key.is_some() { AUTH_TAG_LEN } else { 0 };
        if limits.max_payload_bytes < min_payload_bytes(count_size, auth_overhead) {
            return Err(EncodeError::BudgetTooSmall);
        }
        Ok(Self {
            header,
            store,
            rows,
            key,
            scratch,
            limits,
            cursor: 0,
            emitted: 0,
        })
    }

    /// Rows still to be encoded into a packet.
    pub fn remaining(&self) -> usize {
        self.rows.len() - self.cursor
    }

    /// Encode the next packet into `buf`. The encoder clears `buf` first.
    ///
    /// - `Ok(Some(p))` — one packet ready; send `buf.as_bytes()`.
    /// - `Ok(None)` — iterator exhausted.
    pub fn next_into(&mut self, buf: &mut PacketBuf) -> Result<Option<PacketWritten>, EncodeError> {
        buf.inner.clear();
        let first_of_totals = matches!(self.rows, Rows::Totals(_)) && self.emitted == 0;
        if self.cursor >= self.rows.len() && !first_of_totals {
            return Ok(None);
        }

        let auth_overhead = if self.key.is_some() { AUTH_TAG_LEN } else { 0 };
        let body_budget = self
            .limits
            .max_payload_bytes
            .saturating_sub(HEADER_LEN + auth_overhead);

        // Reserve the header slot; encode_packet_body appends the body.
        buf.inner.resize(HEADER_LEN, 0);
        let encoded = match self.rows {
            Rows::Cells(handles) => encode_packet_body::<C>(
                self.store,
                handles,
                self.cursor,
                body_budget,
                self.scratch,
                &mut buf.inner,
            ),
            Rows::Totals(totals) => encode_totals_body::<C>(
                self.store,
                totals,
                self.cursor,
                body_budget,
                self.scratch,
                &mut buf.inner,
            ),
        };

        // A stale-only run with no emitted cells advances the cursor and
        // emits an empty packet — but only if the dropped handles are real.
        // If we made zero progress (no drops, no cells), the budget is too
        // tight for even a minimal cell; constructor-time validation should
        // have caught this. Bail rather than spin.
        if encoded.handles_consumed == 0 && self.cursor < self.rows.len() {
            return Err(EncodeError::BudgetTooSmall);
        }

        let next_cursor = self.cursor + encoded.handles_consumed;
        let more = next_cursor < self.rows.len();

        let mut flags = self.header.flags;
        if more {
            flags |= FLAG_MORE;
        } else {
            flags &= !FLAG_MORE;
        }
        if self.key.is_some() {
            flags |= FLAG_AUTHENTICATED;
        } else {
            flags &= !FLAG_AUTHENTICATED;
        }

        let mut packet_header = self.header;
        packet_header.flags = flags;
        packet_header.count_width = std::mem::size_of::<C>() as u32;
        packet_header.cell_count = encoded.cells_emitted;
        packet_header.body_len = encoded.body_len as u32;
        match (self.rows, FrameKind::from_flags(self.header.flags)) {
            (Rows::Cells(_), _) => {
                packet_header.min_origin_sequence = encoded.min_origin_sequence;
                packet_header.max_origin_sequence = encoded.max_origin_sequence;
            }
            (Rows::Totals(_), Some(FrameKind::Broadcast)) => {
                packet_header.max_origin_sequence += self.emitted;
            }
            (Rows::Totals(_), Some(FrameKind::Snapshot)) => {
                packet_header.min_origin_sequence = self.emitted;
            }
            (Rows::Totals(_), _) => {}
        }

        patch_header(&mut buf.inner[..HEADER_LEN], &packet_header);

        if let Some(key) = self.key {
            // Sign header + body (with the final flags already in place) and
            // append the tag. Total length is guaranteed <= capacity by the
            // constructor-time check.
            let tag = auth::sign(key, &buf.inner[..HEADER_LEN + encoded.body_len]);
            buf.inner.extend_from_slice(&tag);
        }

        debug_assert!(buf.inner.len() <= self.limits.max_payload_bytes);

        self.cursor = next_cursor;
        self.emitted += 1;
        Ok(Some(PacketWritten {
            cells_emitted: encoded.cells_emitted,
            cells_dropped: encoded.cells_dropped,
            flags,
        }))
    }
}

// -- decode -----------------------------------------------------------------

pub fn decode_unauth<C: Count>(
    bytes: &[u8],
    limits: FrameLimits,
    obs: &mut ObservationBatch<C>,
) -> Result<DecodedSummary, DecodeError> {
    decode_inner(
        bytes,
        None,
        limits,
        |_| true,
        |cell| obs.push(observation_from_wire(cell)),
    )
}

pub fn decode_auth<C: Count>(
    bytes: &[u8],
    key: &HmacKey,
    limits: FrameLimits,
    obs: &mut ObservationBatch<C>,
) -> Result<DecodedSummary, DecodeError> {
    decode_inner(
        bytes,
        Some(key),
        limits,
        |_| true,
        |cell| obs.push(observation_from_wire(cell)),
    )
}

#[inline]
fn observation_from_wire<C: Count>(cell: WireCell<C>) -> Observation<C> {
    Observation {
        rule_fingerprint: cell.rule_fingerprint,
        key_hash: cell.key_hash,
        bucket: cell.bucket,
        origin: cell.origin_node_id,
        incarnation: cell.origin_incarnation,
        count: cell.count,
        last_update_millis: cell.last_update_millis,
    }
}

pub fn decode_unauth_visit<C: Count>(
    bytes: &[u8],
    limits: FrameLimits,
    accept_header: impl FnOnce(&Header) -> bool,
    on_cell: impl FnMut(WireCell<C>),
) -> Result<DecodedSummary, DecodeError> {
    decode_inner(bytes, None, limits, accept_header, on_cell)
}

pub fn decode_auth_visit<C: Count>(
    bytes: &[u8],
    key: &HmacKey,
    limits: FrameLimits,
    accept_header: impl FnOnce(&Header) -> bool,
    on_cell: impl FnMut(WireCell<C>),
) -> Result<DecodedSummary, DecodeError> {
    decode_inner(bytes, Some(key), limits, accept_header, on_cell)
}

fn decode_inner<C: Count>(
    bytes: &[u8],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    accept_header: impl FnOnce(&Header) -> bool,
    on_cell: impl FnMut(WireCell<C>),
) -> Result<DecodedSummary, DecodeError> {
    if bytes.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }
    let auth_overhead = if key.is_some() { AUTH_TAG_LEN } else { 0 };
    if bytes.len() < HEADER_LEN + auth_overhead {
        return Err(DecodeError::TooShort);
    }

    let (frame, _tag) = if let Some(key) = key {
        let split = bytes.len() - AUTH_TAG_LEN;
        let (frame, tag) = bytes.split_at(split);
        auth::verify(key, frame, tag).map_err(|_| DecodeError::AuthenticationFailed)?;
        (frame, Some(tag))
    } else {
        (bytes, None)
    };

    let header = read_header(frame)?;
    let kind = FrameKind::from_flags(header.flags).ok_or(DecodeError::BadKind)?;
    if kind == FrameKind::Nack && header.cell_count != 0 {
        // A NACK is a request; it carries no rows.
        return Err(DecodeError::BodyLenMismatch);
    }
    if header.count_width as usize != std::mem::size_of::<C>() {
        return Err(DecodeError::BadCountWidth);
    }
    if header.cell_count > limits.max_cells {
        return Err(DecodeError::CapacityExceeded);
    }
    let body_len = header.body_len as usize;
    let total_required = HEADER_LEN
        .checked_add(body_len)
        .ok_or(DecodeError::BodyLenMismatch)?;
    if total_required != frame.len() {
        if total_required > frame.len() {
            return Err(DecodeError::BodyLenMismatch);
        }
        return Err(DecodeError::Trailing);
    }

    let summary = DecodedSummary {
        header,
        cell_count: header.cell_count,
        flags: header.flags,
    };

    if !accept_header(&header) {
        return Ok(summary);
    }

    let body_bytes = &frame[HEADER_LEN..HEADER_LEN + body_len];
    match kind {
        FrameKind::Broadcast | FrameKind::Repair | FrameKind::Snapshot | FrameKind::Nack => {
            decode_totals_visit::<C>(
                body_bytes,
                header.cell_count,
                NodeId(header.sender_node_id),
                header.sender_incarnation,
                on_cell,
            )?
        }
        FrameKind::Sync | FrameKind::Ack => {
            decode_body_visit::<C>(body_bytes, header.cell_count, on_cell)?
        }
    }

    Ok(summary)
}
