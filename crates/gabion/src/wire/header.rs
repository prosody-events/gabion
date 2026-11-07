//! Fixed-length packet header: hand-rolled little-endian, exactly 86 bytes,
//! decoded into a typed [`Header`] whose kind, flags and range cannot
//! disagree.

use std::num::NonZeroU128;

use crate::crdt::{Incarnation, NodeId, NodeIdentity};
use crate::zone::ZoneHash;

use super::{DecodeError, read_u16, read_u32, read_u64, read_u128};

pub const HEADER_LEN: usize = 86;
pub const MAGIC: u32 = 0x4742_4732;
pub const VERSION: u16 = 2;

const RELAY_INTO_ZONE: u8 = 0x01;
const GAP_OWN: u8 = 0x02;
const GAP_RELAY: u8 = 0x04;
const CANNOT_RELAY: u8 = 0x08;
const ACK_AS_GATEWAY: u8 = 0x10;
const PROBE_REPLY: u8 = 0x20;
const PROBE: u8 = 0x40;
const RESERVED: u8 = 0x80;

/// The kind byte. [`Payload`] is the typed kind; this only maps the byte.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PacketKind {
    Ack = 0,
    Own = 1,
    Relay = 2,
    Sweep = 3,
    SweepRequest = 4,
}

/// Indexed by the kind byte.
const KINDS: [PacketKind; 5] = [
    PacketKind::Ack,
    PacketKind::Own,
    PacketKind::Relay,
    PacketKind::Sweep,
    PacketKind::SweepRequest,
];

/// The stream positions `(lower, upper]` an `Own` or `Relay` packet covers;
/// never empty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamRange {
    lower: u64,
    upper: u64,
}

impl StreamRange {
    /// `None` unless `lower < upper`.
    pub fn new(lower: u64, upper: u64) -> Option<Self> {
        (lower < upper).then_some(Self { lower, upper })
    }

    pub fn lower(self) -> u64 {
        self.lower
    }

    pub fn upper(self) -> u64 {
        self.upper
    }
}

/// What a packet carries. `PROBE` rides only on `Ack`, `Own` and `Relay`, and
/// `RELAY_INTO_ZONE` only on `Own`, so neither can be built on another kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Payload {
    /// Header only: acks and gap flags.
    Ack { probe: bool },
    /// A kind that carries cells.
    Cells(CellPayload),
    /// Header only: "send me your live cells".
    SweepRequest,
}

/// The kinds that carry cells, the only ones [`Packets`](super::Packets)
/// encodes, so no header-only kind is sent with cells.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CellPayload {
    /// Cells of the sender's own stream.
    Own {
        range: StreamRange,
        relay_into_zone: bool,
        probe: bool,
    },
    /// Cells of the sender's relay stream.
    Relay { range: StreamRange, probe: bool },
    /// Cells outside any stream.
    Sweep,
}

impl Payload {
    /// Whether the packet asks its addressee for an answer: stream data and
    /// probes do; standalone acks, gap reports, probe answers and sweep
    /// packets do not.
    pub fn asks_answer(self) -> bool {
        match self {
            Self::Ack { probe } => probe,
            Self::Cells(CellPayload::Own { .. } | CellPayload::Relay { .. }) => true,
            Self::Cells(CellPayload::Sweep) | Self::SweepRequest => false,
        }
    }
}

impl From<CellPayload> for Payload {
    fn from(cells: CellPayload) -> Self {
        Self::Cells(cells)
    }
}

/// The sender's mark for the addressee's own stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnAck {
    /// The mark of the addressee's ranges received directly.
    Direct(u64),
    /// The mark of the addressee's ranges received as its gateway
    /// (`ACK_AS_GATEWAY`).
    AsGateway(u64),
}

impl Default for OwnAck {
    fn default() -> Self {
        Self::Direct(0)
    }
}

/// The acks and signals every kind carries.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AckInfo {
    /// The addressee's incarnation as the sender knows it; 0 = not heard yet.
    /// The acks apply only when it equals the addressee's incarnation.
    pub incarnation: Incarnation,
    pub own: OwnAck,
    pub relay: u64,
    pub gap_own: bool,
    pub gap_relay: bool,
    pub cannot_relay: bool,
    pub probe_reply: bool,
}

/// A packet header. A decoded one carries any [`Payload`]; one sent with
/// cells carries a [`CellPayload`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Header<P = Payload> {
    pub cluster_id_hash: NonZeroU128,
    pub sender: NodeIdentity,
    /// The sender's zone; `None` is written as 0.
    pub sender_zone: Option<ZoneHash>,
    pub acks: AckInfo,
    pub payload: P,
}

/// Encode `header` for a packet of `cell_count` cells; a header-only kind
/// has none.
pub(crate) fn write_header<P: Copy + Into<Payload>>(
    header: &Header<P>,
    cell_count: u16,
) -> [u8; HEADER_LEN] {
    let (kind, range, relay_into_zone, probe) = match header.payload.into() {
        Payload::Ack { probe } => (PacketKind::Ack, None, false, probe),
        Payload::Cells(CellPayload::Own {
            range,
            relay_into_zone,
            probe,
        }) => (PacketKind::Own, Some(range), relay_into_zone, probe),
        Payload::Cells(CellPayload::Relay { range, probe }) => {
            (PacketKind::Relay, Some(range), false, probe)
        }
        Payload::Cells(CellPayload::Sweep) => (PacketKind::Sweep, None, false, false),
        Payload::SweepRequest => (PacketKind::SweepRequest, None, false, false),
    };
    let acks = header.acks;
    let (ack_own, ack_as_gateway) = match acks.own {
        OwnAck::Direct(mark) => (mark, false),
        OwnAck::AsGateway(mark) => (mark, true),
    };
    let flags = bit(relay_into_zone, RELAY_INTO_ZONE)
        | bit(acks.gap_own, GAP_OWN)
        | bit(acks.gap_relay, GAP_RELAY)
        | bit(acks.cannot_relay, CANNOT_RELAY)
        | bit(ack_as_gateway, ACK_AS_GATEWAY)
        | bit(acks.probe_reply, PROBE_REPLY)
        | bit(probe, PROBE);
    let (lower, upper) = range.map_or((0, 0), |r| (r.lower, r.upper));

    let mut dst = [0; HEADER_LEN];
    dst[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    dst[4..6].copy_from_slice(&VERSION.to_le_bytes());
    dst[6] = kind as u8;
    dst[7] = flags;
    dst[8..24].copy_from_slice(&header.cluster_id_hash.get().to_le_bytes());
    dst[24..40].copy_from_slice(&header.sender.node_id.0.to_le_bytes());
    dst[40..44].copy_from_slice(&header.sender.incarnation.to_le_bytes());
    dst[44..48].copy_from_slice(&header.sender_zone.map_or(0, ZoneHash::get).to_le_bytes());
    dst[48..50].copy_from_slice(&cell_count.to_le_bytes());
    dst[50..54].copy_from_slice(&acks.incarnation.to_le_bytes());
    dst[54..62].copy_from_slice(&ack_own.to_le_bytes());
    dst[62..70].copy_from_slice(&acks.relay.to_le_bytes());
    dst[70..78].copy_from_slice(&lower.to_le_bytes());
    dst[78..86].copy_from_slice(&upper.to_le_bytes());
    dst
}

fn bit(set: bool, flag: u8) -> u8 {
    if set { flag } else { 0 }
}

/// Decode a header and its cell count, checking the length, magic, version,
/// kind, flags and range, then the cluster id, in that order. Public so a
/// simulator can tag the packets it carries; the runtime decodes whole packets
/// through [`decode`](super::decode).
pub fn read_header(
    bytes: &[u8],
    cluster_id_hash: NonZeroU128,
) -> Result<(Header, u16), DecodeError> {
    let Some(bytes) = bytes.first_chunk::<HEADER_LEN>() else {
        return Err(DecodeError::TooShort);
    };
    if read_u32(&bytes[0..]) != MAGIC {
        return Err(DecodeError::BadMagic);
    }
    let version = read_u16(&bytes[4..]);
    if version != VERSION {
        return Err(DecodeError::BadVersion { found: version });
    }
    let flags = bytes[7];
    let cell_count = read_u16(&bytes[48..]);
    let payload = read_payload(
        bytes[6],
        flags,
        cell_count,
        read_u64(&bytes[70..]),
        read_u64(&bytes[78..]),
    )?;
    let found = read_u128(&bytes[8..]);
    if found != cluster_id_hash.get() {
        return Err(DecodeError::ForeignCluster { found });
    }
    let ack_own = read_u64(&bytes[54..]);
    let header = Header {
        cluster_id_hash,
        sender: NodeIdentity::new(NodeId(read_u128(&bytes[24..])), read_u32(&bytes[40..])),
        sender_zone: ZoneHash::from_wire(read_u32(&bytes[44..])),
        acks: AckInfo {
            incarnation: read_u32(&bytes[50..]),
            own: if flags & ACK_AS_GATEWAY == 0 {
                OwnAck::Direct(ack_own)
            } else {
                OwnAck::AsGateway(ack_own)
            },
            relay: read_u64(&bytes[62..]),
            gap_own: flags & GAP_OWN != 0,
            gap_relay: flags & GAP_RELAY != 0,
            cannot_relay: flags & CANNOT_RELAY != 0,
            probe_reply: flags & PROBE_REPLY != 0,
        },
        payload,
    };
    Ok((header, cell_count))
}

/// Apply the kind, flag and range rules, in the order the wire format lists
/// them.
fn read_payload(
    kind: u8,
    flags: u8,
    cell_count: u16,
    lower: u64,
    upper: u64,
) -> Result<Payload, DecodeError> {
    let kind = *KINDS.get(usize::from(kind)).ok_or(DecodeError::BadKind)?;
    let relay_into_zone = flags & RELAY_INTO_ZONE != 0;
    let probe = flags & PROBE != 0;
    if flags & RESERVED != 0
        || (relay_into_zone && kind != PacketKind::Own)
        || (probe && matches!(kind, PacketKind::Sweep | PacketKind::SweepRequest))
    {
        return Err(DecodeError::BadFlags);
    }
    if cell_count > 0 && matches!(kind, PacketKind::Ack | PacketKind::SweepRequest) {
        return Err(DecodeError::BadKind);
    }
    let range = StreamRange::new(lower, upper).ok_or(DecodeError::BadRange);
    match kind {
        PacketKind::Own => Ok(Payload::Cells(CellPayload::Own {
            range: range?,
            relay_into_zone,
            probe,
        })),
        PacketKind::Relay => Ok(Payload::Cells(CellPayload::Relay {
            range: range?,
            probe,
        })),
        _ if (lower, upper) != (0, 0) => Err(DecodeError::BadRange),
        PacketKind::Ack => Ok(Payload::Ack { probe }),
        PacketKind::Sweep => Ok(Payload::Cells(CellPayload::Sweep)),
        PacketKind::SweepRequest => Ok(Payload::SweepRequest),
    }
}

#[cfg(test)]
mod tests;
