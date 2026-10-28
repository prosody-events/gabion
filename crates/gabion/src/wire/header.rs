//! Fixed-length packet header: hand-rolled little-endian, exactly 86 bytes,
//! decoded into a typed [`Header`] whose kind, flags and range cannot
//! disagree.

use crate::crdt::{Incarnation, NodeId, NodeIdentity};

use super::{DecodeError, read_u16, read_u32, read_u64, read_u128};

pub const HEADER_LEN: usize = 86;
pub const MAGIC: u32 = 0x4742_4732;
pub const VERSION: u16 = 2;

const RELAY_INTO_ZONE: u8 = 0x01;
const GAP_OWN: u8 = 0x02;
const GAP_RELAY: u8 = 0x04;
const CANNOT_RELAY: u8 = 0x08;
const ACK_FLAGGED: u8 = 0x10;
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
}

/// What a packet carries. `PROBE` rides only on `Ack`, `Own` and `Relay`, and
/// `RELAY_INTO_ZONE` only on `Own`, so neither can be built on another kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Payload {
    /// Header only: acks and gap flags.
    Ack { probe: bool },
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
    /// Header only: "send me your live cells".
    SweepRequest,
}

/// The sender's mark for the addressee's own stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnAck {
    Plain(u64),
    /// The mark of the addressee's flagged ranges (`ACK_FLAGGED`).
    Flagged(u64),
}

impl Default for OwnAck {
    fn default() -> Self {
        Self::Plain(0)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Header {
    pub cluster_id_hash: u128,
    pub sender: NodeIdentity,
    /// The sender's zone hash; 0 = no zone.
    pub sender_zone: u32,
    pub acks: AckInfo,
    pub payload: Payload,
}

/// Encode `header` for a packet of `cell_count` cells.
pub(crate) fn write_header(header: &Header, cell_count: u16) -> [u8; HEADER_LEN] {
    let (kind, range, relay_into_zone, probe) = match header.payload {
        Payload::Ack { probe } => (PacketKind::Ack, None, false, probe),
        Payload::Own {
            range,
            relay_into_zone,
            probe,
        } => (PacketKind::Own, Some(range), relay_into_zone, probe),
        Payload::Relay { range, probe } => (PacketKind::Relay, Some(range), false, probe),
        Payload::Sweep => (PacketKind::Sweep, None, false, false),
        Payload::SweepRequest => (PacketKind::SweepRequest, None, false, false),
    };
    let acks = header.acks;
    let (ack_own, ack_flagged) = match acks.own {
        OwnAck::Plain(mark) => (mark, false),
        OwnAck::Flagged(mark) => (mark, true),
    };
    let flags = bit(relay_into_zone, RELAY_INTO_ZONE)
        | bit(acks.gap_own, GAP_OWN)
        | bit(acks.gap_relay, GAP_RELAY)
        | bit(acks.cannot_relay, CANNOT_RELAY)
        | bit(ack_flagged, ACK_FLAGGED)
        | bit(acks.probe_reply, PROBE_REPLY)
        | bit(probe, PROBE);
    let (lower, upper) = range.map_or((0, 0), |r| (r.lower, r.upper));

    let mut dst = [0; HEADER_LEN];
    dst[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    dst[4..6].copy_from_slice(&VERSION.to_le_bytes());
    dst[6] = kind as u8;
    dst[7] = flags;
    dst[8..24].copy_from_slice(&header.cluster_id_hash.to_le_bytes());
    dst[24..40].copy_from_slice(&header.sender.node_id.0.to_le_bytes());
    dst[40..44].copy_from_slice(&header.sender.incarnation.to_le_bytes());
    dst[44..48].copy_from_slice(&header.sender_zone.to_le_bytes());
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
/// kind, flags and range, then the cluster id, in that order.
pub(crate) fn read_header(
    bytes: &[u8],
    cluster_id_hash: u128,
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
    if found != cluster_id_hash {
        return Err(DecodeError::ForeignCluster { found });
    }
    let ack_own = read_u64(&bytes[54..]);
    let header = Header {
        cluster_id_hash,
        sender: NodeIdentity::new(NodeId(read_u128(&bytes[24..])), read_u32(&bytes[40..])),
        sender_zone: read_u32(&bytes[44..]),
        acks: AckInfo {
            incarnation: read_u32(&bytes[50..]),
            own: if flags & ACK_FLAGGED == 0 {
                OwnAck::Plain(ack_own)
            } else {
                OwnAck::Flagged(ack_own)
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
        PacketKind::Own => Ok(Payload::Own {
            range: range?,
            relay_into_zone,
            probe,
        }),
        PacketKind::Relay => Ok(Payload::Relay {
            range: range?,
            probe,
        }),
        _ if (lower, upper) != (0, 0) => Err(DecodeError::BadRange),
        PacketKind::Ack => Ok(Payload::Ack { probe }),
        PacketKind::Sweep => Ok(Payload::Sweep),
        PacketKind::SweepRequest => Ok(Payload::SweepRequest),
    }
}

#[cfg(test)]
mod tests;
