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
//! - A packet may carry an opaque control extension ([`FLAG_EXT`]): the body
//!   then starts with a little-endian `u16` extension length and the extension
//!   bytes, followed by the cell body. The extension is covered by the HMAC
//!   like the rest of the frame; the codec does not interpret it.
//!
//! Socket I/O, peer selection, and tick scheduling live outside this module.

mod auth;
mod body;
mod header;

#[cfg(test)]
mod tests;

pub use auth::HmacKey;
pub use body::{WireCell, WireScratch};
pub use header::{FLAG_AUTHENTICATED, FLAG_EXT, FLAG_MORE, HEADER_LEN, Header, MAGIC, VERSION};

use auth::AUTH_TAG_LEN;
use body::{DICT_LEN_BYTES, PER_CELL_IDENT_BYTES, decode_body_visit, encode_packet_body};
use header::{patch_header, read_header};

use crate::crdt::{CellHandle, CellStore, Count, Observation, ObservationBatch};

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
pub struct DecodedSummary<'a> {
    pub header: Header,
    pub cell_count: u32,
    /// Mirror of `header.flags`. Receivers may inspect [`FLAG_MORE`] to
    /// decide when a batch is fully delivered. Each packet is decoded and
    /// applied independently regardless of the bit's value.
    pub flags: u16,
    /// The control extension, empty unless [`FLAG_EXT`] is set.
    pub ext: &'a [u8],
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum EncodeError {
    #[error("max_payload_bytes is smaller than the fixed framing overhead + one cell")]
    BudgetTooSmall,
    #[error("frame with its control extension would exceed max_payload_bytes")]
    PayloadTooLarge,
    #[error("source frame to re-frame is not a well-formed gabion frame")]
    MalformedSource,
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
    handles: &'a [CellHandle],
    key: Option<&'a HmacKey>,
    scratch: &'a mut WireScratch,
    limits: FrameLimits,
    cursor: usize,
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
    /// Build an unauthenticated packet iterator. The `cluster_id_hash`,
    /// `sender_node_id`, `sender_incarnation` fields of `header` are copied
    /// into every packet; the codec overwrites `count_width`, `cell_count`,
    /// `body_len`, `flags`, `min/max_origin_sequence` on each packet.
    pub fn unauth(
        header: Header,
        store: &'a CellStore<C>,
        handles: &'a [CellHandle],
        scratch: &'a mut WireScratch,
        limits: FrameLimits,
    ) -> Result<Self, EncodeError> {
        Self::new(header, store, handles, None, scratch, limits)
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
        Self::new(header, store, handles, Some(key), scratch, limits)
    }

    fn new(
        header: Header,
        store: &'a CellStore<C>,
        handles: &'a [CellHandle],
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
            handles,
            key,
            scratch,
            limits,
            cursor: 0,
        })
    }

    /// Handles still to be encoded into a packet.
    pub fn remaining(&self) -> usize {
        self.handles.len() - self.cursor
    }

    /// Stop after the packets already encoded and return how many handles
    /// they consumed (cells emitted plus stale handles dropped). The caller
    /// owns the rest, for callers that send one packet per message and carry
    /// the remainder into the next one.
    pub fn finish(mut self) -> usize {
        let consumed = self.cursor;
        self.cursor = self.handles.len();
        consumed
    }

    /// Encode the next packet into `buf`. The encoder clears `buf` first.
    ///
    /// - `Ok(Some(p))` — one packet ready; send `buf.as_bytes()`.
    /// - `Ok(None)` — iterator exhausted.
    pub fn next_into(&mut self, buf: &mut PacketBuf) -> Result<Option<PacketWritten>, EncodeError> {
        buf.inner.clear();
        if self.cursor >= self.handles.len() {
            return Ok(None);
        }

        let auth_overhead = if self.key.is_some() { AUTH_TAG_LEN } else { 0 };
        let body_budget = self
            .limits
            .max_payload_bytes
            .saturating_sub(HEADER_LEN + auth_overhead);

        // Reserve the header slot; encode_packet_body appends the body.
        buf.inner.resize(HEADER_LEN, 0);
        let encoded = encode_packet_body::<C>(
            self.store,
            self.handles,
            self.cursor,
            body_budget,
            self.scratch,
            &mut buf.inner,
        );

        // A stale-only run with no emitted cells advances the cursor and
        // emits an empty packet — but only if the dropped handles are real.
        // If we made zero progress (no drops, no cells), the budget is too
        // tight for even a minimal cell; constructor-time validation should
        // have caught this. Bail rather than spin.
        if encoded.cells_emitted == 0 && encoded.handles_consumed == 0 {
            return Err(EncodeError::BudgetTooSmall);
        }

        let next_cursor = self.cursor + encoded.handles_consumed;
        let more = next_cursor < self.handles.len();

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
        packet_header.min_origin_sequence = encoded.min_origin_sequence;
        packet_header.max_origin_sequence = encoded.max_origin_sequence;

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
        Ok(Some(PacketWritten {
            cells_emitted: encoded.cells_emitted,
            cells_dropped: encoded.cells_dropped,
            flags,
        }))
    }
}

// -- decode -----------------------------------------------------------------

pub fn decode_unauth<'a, C: Count>(
    bytes: &'a [u8],
    limits: FrameLimits,
    obs: &mut ObservationBatch<C>,
) -> Result<DecodedSummary<'a>, DecodeError> {
    decode_inner(
        bytes,
        None,
        limits,
        |_| true,
        |cell| obs.push(observation_from_wire(cell)),
    )
}

pub fn decode_auth<'a, C: Count>(
    bytes: &'a [u8],
    key: &HmacKey,
    limits: FrameLimits,
    obs: &mut ObservationBatch<C>,
) -> Result<DecodedSummary<'a>, DecodeError> {
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

pub fn decode_unauth_visit<'a, C: Count>(
    bytes: &'a [u8],
    limits: FrameLimits,
    accept_header: impl FnOnce(&Header) -> bool,
    on_cell: impl FnMut(WireCell<C>),
) -> Result<DecodedSummary<'a>, DecodeError> {
    decode_inner(bytes, None, limits, accept_header, on_cell)
}

pub fn decode_auth_visit<'a, C: Count>(
    bytes: &'a [u8],
    key: &HmacKey,
    limits: FrameLimits,
    accept_header: impl FnOnce(&Header) -> bool,
    on_cell: impl FnMut(WireCell<C>),
) -> Result<DecodedSummary<'a>, DecodeError> {
    decode_inner(bytes, Some(key), limits, accept_header, on_cell)
}

fn decode_inner<'a, C: Count>(
    bytes: &'a [u8],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    accept_header: impl FnOnce(&Header) -> bool,
    on_cell: impl FnMut(WireCell<C>),
) -> Result<DecodedSummary<'a>, DecodeError> {
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

    let (ext, cell_bytes) = split_ext(header.flags, &frame[HEADER_LEN..HEADER_LEN + body_len])?;
    let summary = DecodedSummary {
        header,
        cell_count: header.cell_count,
        flags: header.flags,
        ext,
    };

    if !accept_header(&header) {
        return Ok(summary);
    }

    decode_body_visit::<C>(cell_bytes, header.cell_count, on_cell)?;

    Ok(summary)
}

/// Bytes the control extension adds to a body: its `u16` length prefix plus
/// the extension itself.
pub const EXT_PREFIX_LEN: usize = 2;

/// Most extension bytes a frame without cells can carry within `limits`.
pub fn ext_capacity(limits: FrameLimits, authenticated: bool) -> usize {
    let auth_overhead = if authenticated { AUTH_TAG_LEN } else { 0 };
    limits
        .max_payload_bytes
        .saturating_sub(HEADER_LEN + EXT_PREFIX_LEN + auth_overhead)
        .min(usize::from(u16::MAX))
}

/// Split a body into its control extension and the cell body.
fn split_ext(flags: u16, body: &[u8]) -> Result<(&[u8], &[u8]), DecodeError> {
    if flags & FLAG_EXT == 0 {
        return Ok((&[], body));
    }
    let prefix = body
        .get(..EXT_PREFIX_LEN)
        .ok_or(DecodeError::BodyLenMismatch)?;
    let ext_len = usize::from(u16::from_le_bytes([prefix[0], prefix[1]]));
    let rest = &body[EXT_PREFIX_LEN..];
    if rest.len() < ext_len {
        return Err(DecodeError::BodyLenMismatch);
    }
    Ok(rest.split_at(ext_len))
}

// -- control extension ------------------------------------------------------

/// Write `header || [ext] || cell_body` (and the HMAC tag when `key` is set)
/// into `out`. `header` supplies the identity and cell fields; flags, body
/// length and the extension are set here.
fn write_frame(
    mut header: Header,
    ext: &[u8],
    cell_body: &[u8],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    out: &mut PacketBuf,
) -> Result<(), EncodeError> {
    let auth_overhead = if key.is_some() { AUTH_TAG_LEN } else { 0 };
    let ext_overhead = if ext.is_empty() {
        0
    } else {
        EXT_PREFIX_LEN + ext.len()
    };
    let body_len = ext_overhead + cell_body.len();
    if HEADER_LEN + body_len + auth_overhead > limits.max_payload_bytes {
        return Err(EncodeError::PayloadTooLarge);
    }
    header.flags &= !(FLAG_EXT | FLAG_AUTHENTICATED);
    if !ext.is_empty() {
        header.flags |= FLAG_EXT;
    }
    if key.is_some() {
        header.flags |= FLAG_AUTHENTICATED;
    }
    header.body_len = body_len as u32;

    out.inner.clear();
    out.inner.resize(HEADER_LEN, 0);
    if !ext.is_empty() {
        out.inner
            .extend_from_slice(&(ext.len() as u16).to_le_bytes());
        out.inner.extend_from_slice(ext);
    }
    out.inner.extend_from_slice(cell_body);
    patch_header(&mut out.inner[..HEADER_LEN], &header);
    if let Some(key) = key {
        let tag = auth::sign(key, &out.inner);
        out.inner.extend_from_slice(&tag);
    }
    Ok(())
}

/// Re-emit a gabion frame with a different control extension: the cell body
/// is copied byte for byte, the extension replaced (an empty `ext` removes
/// it), the sender fields taken from `sender`, and the frame re-signed when
/// `key` is set. `frame` must be a frame this node encoded or already
/// verified; it is not re-authenticated here. Interior nodes of a broadcast
/// tree use this to forward a received packet to a child without decoding
/// or re-encoding its cells.
pub fn reframe(
    frame: &[u8],
    sender: Header,
    ext: &[u8],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    out: &mut PacketBuf,
) -> Result<(), EncodeError> {
    let auth_overhead = if key.is_some() { AUTH_TAG_LEN } else { 0 };
    let unsigned = frame
        .len()
        .checked_sub(auth_overhead)
        .map(|end| &frame[..end])
        .ok_or(EncodeError::MalformedSource)?;
    let source = read_header(unsigned).map_err(|_| EncodeError::MalformedSource)?;
    let body = unsigned
        .get(HEADER_LEN..HEADER_LEN + source.body_len as usize)
        .ok_or(EncodeError::MalformedSource)?;
    let (_, cell_body) = split_ext(source.flags, body).map_err(|_| EncodeError::MalformedSource)?;
    let header = Header {
        cluster_id_hash: sender.cluster_id_hash,
        sender_node_id: sender.sender_node_id,
        sender_incarnation: sender.sender_incarnation,
        ..source
    };
    write_frame(header, ext, cell_body, key, limits, out)
}

/// Encode a frame that carries only a control extension and no cells.
pub fn encode_ext_only<C: Count>(
    sender: Header,
    ext: &[u8],
    key: Option<&HmacKey>,
    limits: FrameLimits,
    out: &mut PacketBuf,
) -> Result<(), EncodeError> {
    let header = Header {
        count_width: std::mem::size_of::<C>() as u32,
        cell_count: 0,
        min_origin_sequence: 0,
        max_origin_sequence: 0,
        ..sender
    };
    write_frame(header, ext, &[], key, limits, out)
}
