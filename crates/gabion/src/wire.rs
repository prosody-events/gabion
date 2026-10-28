//! On-the-wire codec for gossip packets, version 2.
//!
//! Each UDP packet is an 86-byte [`Header`] and a body of cells. The body
//! carries its own small `(rule_fp, origin_node_id)` dictionaries followed by
//! per-cell slot indices, so a receiver decodes any packet alone, whether or
//! not the packets around it arrived. The header's kind byte, flags and
//! stream range decode into one [`Payload`], so no decoded packet is two kinds
//! at once.
//!
//! Invariants:
//! - Frames are little-endian, with no padding and no authentication tag.
//! - The encoder never writes past `FrameLimits::max_payload_bytes`.
//! - [`decode`] checks the payload limit, then the header's length, magic,
//!   version, kind, flags and range, then the cluster id, and only then the
//!   body, so a packet from another version or cluster is dropped before any of
//!   its cells is read.
//! - No cell bound is configured: the body's exact length and every slot are
//!   checked before any cell is visited, so a packet within `max_payload_bytes`
//!   cannot deliver more cells than `FrameLimits::cell_bound`.
//!
//! Socket I/O, peer selection, and tick scheduling live outside this module.

mod body;
mod header;

#[cfg(test)]
mod tests;

pub use body::WireScratch;
pub use header::{
    AckInfo, CellPayload, HEADER_LEN, Header, MAGIC, OwnAck, Payload, StreamRange, VERSION,
};

pub(crate) use header::write_header;

use std::num::NonZeroU128;

use body::{CELL_BYTES, DICT_LEN_BYTES, decode_body, encode_packet_body};
use header::read_header;

use crate::crdt::{CellHandle, CellStore, Observation};

use thiserror::Error;

/// The smallest `max_payload_bytes`: the header, the two dictionary lengths
/// with one 16-byte entry each, and one cell. 158 bytes.
pub const MIN_PAYLOAD_BYTES: usize = HEADER_LEN + DICT_LEN_BYTES + 16 + 16 + CELL_BYTES;

/// The largest UDP payload over IPv4: 65,535 − 20 (IP header) − 8 (UDP header).
pub const MAX_PAYLOAD_BYTES: usize = 65_507;

// The `u16` cell count holds every cell the largest payload carries.
const _: () = assert!(MAX_PAYLOAD_BYTES / CELL_BYTES <= u16::MAX as usize);

/// A payload size within `MIN_PAYLOAD_BYTES..=MAX_PAYLOAD_BYTES`, so every
/// packet holds at least one cell and fits one UDP datagram.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameLimits {
    max_payload_bytes: usize,
}

impl FrameLimits {
    /// `None` outside `MIN_PAYLOAD_BYTES..=MAX_PAYLOAD_BYTES`.
    pub fn new(max_payload_bytes: usize) -> Option<Self> {
        (MIN_PAYLOAD_BYTES..=MAX_PAYLOAD_BYTES)
            .contains(&max_payload_bytes)
            .then_some(Self { max_payload_bytes })
    }

    pub(crate) fn max_payload_bytes(self) -> usize {
        self.max_payload_bytes
    }

    /// The most cells one packet within these limits holds: one rule, one
    /// origin, and every other byte spent on cells.
    pub(crate) fn cell_bound(self) -> usize {
        (self.max_payload_bytes - (MIN_PAYLOAD_BYTES - CELL_BYTES)) / CELL_BYTES
    }
}

impl Default for FrameLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: MAX_PAYLOAD_BYTES,
        }
    }
}

/// Why a packet was dropped, in the order [`decode`] checks.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    #[error("packet exceeds max_payload_bytes")]
    PayloadTooLarge,
    #[error("packet is shorter than the header")]
    TooShort,
    #[error("magic mismatch")]
    BadMagic,
    #[error("unsupported protocol version {found}")]
    BadVersion { found: u16 },
    #[error("unknown packet kind, or cells on a header-only kind")]
    BadKind,
    #[error("flag not allowed on this packet kind")]
    BadFlags,
    #[error("stream range does not fit the packet kind")]
    BadRange,
    #[error("cluster id {found:#x} is not this node's")]
    ForeignCluster { found: u128 },
    #[error("body length does not match its dictionaries and cell count")]
    BodyLength,
    #[error("rule_slot or node_slot index out of range for the packet's dictionary")]
    BadSlot,
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

    /// Encode a packet of `header` alone: an ack, a sweep request, a closing
    /// packet or an empty sweep page.
    pub(crate) fn header_only<P: Copy + Into<Payload>>(&mut self, header: &Header<P>) {
        self.inner.clear();
        self.inner.extend_from_slice(&write_header(header, 0));
    }
}

impl AsRef<[u8]> for PacketBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

// -- packet iterator --------------------------------------------------------

/// Synchronous iterator over outbound packets. Not `std::iter::Iterator` —
/// `next_into` takes a caller-owned buffer; `Iterator::next` cannot. A caller
/// may stop early: handles it never encodes are simply not sent.
///
/// A stream frame's header names the frame's range; its packets tile that
/// range, each closing the positions up to its last cell's sequence and the
/// last closing the rest, so a lost packet opens a gap no later one hides.
#[must_use = "Packets encodes nothing until next_into is called"]
pub struct Packets<'a> {
    header: Header<CellPayload>,
    store: &'a CellStore,
    handles: &'a [CellHandle],
    scratch: &'a mut WireScratch,
    limits: FrameLimits,
    cursor: usize,
    /// The stream position the next packet's range starts at.
    lower: u64,
}

impl<'a> Packets<'a> {
    /// Build a packet iterator; every packet carries `header` and its own
    /// cell count.
    pub fn new(
        header: Header<CellPayload>,
        store: &'a CellStore,
        handles: &'a [CellHandle],
        scratch: &'a mut WireScratch,
        limits: FrameLimits,
    ) -> Self {
        let lower = match header.payload {
            CellPayload::Own { range, .. } | CellPayload::Relay { range, .. } => range.lower(),
            CellPayload::Sweep => 0,
        };
        Self {
            header,
            store,
            handles,
            scratch,
            limits,
            cursor: 0,
            lower,
        }
    }

    /// The stream position the packets encoded so far close through.
    pub(crate) fn closed_through(&self) -> u64 {
        self.lower
    }

    /// Encode the next packet into `buf` and return its cell count, or
    /// return `None` once every handle is consumed. The encoder clears `buf`
    /// first.
    pub fn next_into(&mut self, buf: &mut PacketBuf) -> Option<u16> {
        buf.inner.clear();
        if self.cursor >= self.handles.len() {
            return None;
        }

        // Reserve the header slot; encode_packet_body appends the body.
        buf.inner.resize(HEADER_LEN, 0);
        let plan = encode_packet_body(
            self.store,
            self.handles,
            self.cursor,
            self.limits.max_payload_bytes - HEADER_LEN,
            self.scratch,
            &mut buf.inner,
        );

        // A stale-only run with no emitted cells advances the cursor and
        // emits an empty packet. The limits fit one cell in an empty packet,
        // so every call consumes a handle; stop rather than
        // spin if that ever breaks.
        debug_assert!(plan.handles_consumed > 0);
        if plan.handles_consumed == 0 {
            return None;
        }

        self.cursor += plan.handles_consumed;
        let header = Header {
            payload: self.packet_payload(),
            ..self.header
        };
        buf.inner[..HEADER_LEN].copy_from_slice(&write_header(&header, plan.cells_emitted));
        debug_assert!(buf.inner.len() <= self.limits.max_payload_bytes);
        Some(plan.cells_emitted)
    }

    /// The payload of the packet ending at `cursor`: a stream packet closes
    /// up to its last cell's sequence, or to the frame's upper end when it is
    /// the last. Advances `lower` past it.
    fn packet_payload(&mut self) -> CellPayload {
        let close = |range: StreamRange, lower: &mut u64| {
            let upper = match self.handles.get(self.cursor - 1) {
                Some(&last) if self.cursor < self.handles.len() => self
                    .store
                    .get(last)
                    .map_or(range.upper(), |row| row.origin_sequence),
                _ => range.upper(),
            };
            let packet = StreamRange::new(*lower, upper);
            debug_assert!(packet.is_some(), "a frame's packets tile its range");
            *lower = upper;
            packet.unwrap_or(range)
        };
        match self.header.payload {
            CellPayload::Own {
                range,
                relay_into_zone,
                probe,
            } => CellPayload::Own {
                range: close(range, &mut self.lower),
                relay_into_zone,
                probe,
            },
            CellPayload::Relay { range, probe } => CellPayload::Relay {
                range: close(range, &mut self.lower),
                probe,
            },
            CellPayload::Sweep => CellPayload::Sweep,
        }
    }
}

// -- decode -----------------------------------------------------------------

/// Decode one packet, handing each cell to `on_cell`, and return its header.
/// A packet from another cluster fails as [`DecodeError::ForeignCluster`]
/// before its body is read. On any error no cell was handed over.
pub fn decode(
    bytes: &[u8],
    limits: FrameLimits,
    cluster_id_hash: NonZeroU128,
    on_cell: impl FnMut(Observation),
) -> Result<Header, DecodeError> {
    if bytes.len() > limits.max_payload_bytes {
        return Err(DecodeError::PayloadTooLarge);
    }
    let (header, cell_count) = read_header(bytes, cluster_id_hash)?;
    decode_body(&bytes[HEADER_LEN..], cell_count, on_cell)?;
    Ok(header)
}

fn read_u16(src: &[u8]) -> u16 {
    u16::from_le_bytes(src[..2].try_into().expect("2 bytes"))
}

fn read_u32(src: &[u8]) -> u32 {
    u32::from_le_bytes(src[..4].try_into().expect("4 bytes"))
}

fn read_u64(src: &[u8]) -> u64 {
    u64::from_le_bytes(src[..8].try_into().expect("8 bytes"))
}

fn read_u128(src: &[u8]) -> u128 {
    u128::from_le_bytes(src[..16].try_into().expect("16 bytes"))
}
