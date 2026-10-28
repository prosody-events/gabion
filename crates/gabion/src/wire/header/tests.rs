use std::num::NonZeroU128;

use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;

use super::*;
use crate::wire::{FrameLimits, decode};

const CLUSTER: NonZeroU128 = NonZeroU128::new(0xC1).unwrap();

#[derive(Clone, Debug)]
struct AnyHeader(Header);

impl Arbitrary for AnyHeader {
    fn arbitrary(g: &mut Gen) -> Self {
        let lower = u64::arbitrary(g).min(u64::MAX - 1);
        let upper = lower.saturating_add(1 + u64::from(u8::arbitrary(g)));
        let range = StreamRange::new(lower, upper).expect("lower < upper");
        let probe = bool::arbitrary(g);
        let payload = match u8::arbitrary(g) % 5 {
            0 => Payload::Ack { probe },
            1 => Payload::Cells(CellPayload::Own {
                range,
                relay_into_zone: bool::arbitrary(g),
                probe,
            }),
            2 => Payload::Cells(CellPayload::Relay { range, probe }),
            3 => Payload::Cells(CellPayload::Sweep),
            _ => Payload::SweepRequest,
        };
        let mark = u64::arbitrary(g);
        Self(Header {
            cluster_id_hash: NonZeroU128::new(u128::arbitrary(g)).unwrap_or(CLUSTER),
            sender: NodeIdentity::new(NodeId(u128::arbitrary(g)), u32::arbitrary(g)),
            sender_zone: u32::arbitrary(g),
            acks: AckInfo {
                incarnation: u32::arbitrary(g),
                own: if bool::arbitrary(g) {
                    OwnAck::Flagged(mark)
                } else {
                    OwnAck::Plain(mark)
                },
                relay: u64::arbitrary(g),
                gap_own: bool::arbitrary(g),
                gap_relay: bool::arbitrary(g),
                cannot_relay: bool::arbitrary(g),
                probe_reply: bool::arbitrary(g),
            },
            payload,
        })
    }
}

/// Every header, of every kind, survives the wire unchanged.
#[quickcheck]
fn header_v2_round_trips(header: AnyHeader) -> bool {
    let header = header.0;
    let bytes = write_header(&header, 0);
    decode(
        &bytes,
        FrameLimits::default(),
        header.cluster_id_hash,
        |_| panic!("a header-only packet has no cells"),
    ) == Ok(header)
}

/// Each field sits at the offset the wire format gives it.
#[test]
fn header_v2_layout_is_pinned() {
    let header = Header {
        cluster_id_hash: NonZeroU128::new(0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10).unwrap(),
        sender: NodeIdentity::new(
            NodeId(0x1112_1314_1516_1718_191A_1B1C_1D1E_1F20),
            0x2122_2324,
        ),
        sender_zone: 0x3132_3334,
        acks: AckInfo {
            incarnation: 0x4142_4344,
            own: OwnAck::Flagged(0x5152_5354_5556_5758),
            relay: 0x6162_6364_6566_6768,
            gap_own: true,
            gap_relay: true,
            cannot_relay: true,
            probe_reply: true,
        },
        payload: Payload::Cells(CellPayload::Own {
            range: StreamRange::new(0x7172_7374_7576_7778, 0x8182_8384_8586_8788).expect("range"),
            relay_into_zone: true,
            probe: true,
        }),
    };
    let buf = write_header(&header, 0);
    assert_eq!(HEADER_LEN, 86);
    assert_eq!(buf[0..4], MAGIC.to_le_bytes());
    assert_eq!(buf[4..6], 2_u16.to_le_bytes());
    assert_eq!(buf[6], 1);
    assert_eq!(buf[7], 0x7F);
    assert_eq!(buf[8..24], header.cluster_id_hash.get().to_le_bytes());
    assert_eq!(buf[24..40], header.sender.node_id.0.to_le_bytes());
    assert_eq!(buf[40..44], 0x2122_2324_u32.to_le_bytes());
    assert_eq!(buf[44..48], 0x3132_3334_u32.to_le_bytes());
    assert_eq!(buf[48..50], 0_u16.to_le_bytes());
    assert_eq!(buf[50..54], 0x4142_4344_u32.to_le_bytes());
    assert_eq!(buf[54..62], 0x5152_5354_5556_5758_u64.to_le_bytes());
    assert_eq!(buf[62..70], 0x6162_6364_6566_6768_u64.to_le_bytes());
    assert_eq!(buf[70..78], 0x7172_7374_7576_7778_u64.to_le_bytes());
    assert_eq!(buf[78..86], 0x8182_8384_8586_8788_u64.to_le_bytes());
    assert_eq!(
        read_header(&buf, header.cluster_id_hash),
        Ok((header, 0)),
        "a closing `Own` packet decodes"
    );

    // Each ack flag owns its bit.
    let sweep = Header {
        acks: AckInfo::default(),
        payload: Payload::Cells(CellPayload::Sweep),
        ..header
    };
    let flags = |set: fn(&mut AckInfo)| {
        let mut sweep = sweep;
        set(&mut sweep.acks);
        write_header(&sweep, 0)[7]
    };
    assert_eq!(flags(|acks| acks.gap_own = true), 0x02);
    assert_eq!(flags(|acks| acks.gap_relay = true), 0x04);
    assert_eq!(flags(|acks| acks.cannot_relay = true), 0x08);
    assert_eq!(flags(|acks| acks.own = OwnAck::Flagged(0)), 0x10);
    assert_eq!(flags(|acks| acks.probe_reply = true), 0x20);
}

/// The rules of the kind, flag and range table, transcribed in its order.
fn table_rule(kind: u8, flags: u8, cells: u16, lower: u64, upper: u64) -> Option<DecodeError> {
    if kind > 4 {
        Some(DecodeError::BadKind)
    } else if flags & 0x80 != 0
        || (flags & 0x01 != 0 && kind != 1)
        || (flags & 0x40 != 0 && (kind == 3 || kind == 4))
    {
        Some(DecodeError::BadFlags)
    } else if (kind == 0 || kind == 4) && cells > 0 {
        Some(DecodeError::BadKind)
    } else if ((kind == 1 || kind == 2) && lower >= upper)
        || ((kind == 0 || kind == 3 || kind == 4) && (lower, upper) != (0, 0))
    {
        Some(DecodeError::BadRange)
    } else {
        None
    }
}

fn raw_header(kind: u8, flags: u8, cells: u16, lower: u64, upper: u64) -> [u8; HEADER_LEN] {
    let mut bytes = write_header(
        &Header {
            cluster_id_hash: CLUSTER,
            sender: NodeIdentity::new(NodeId(0xA), 3),
            sender_zone: 0,
            acks: AckInfo::default(),
            payload: CellPayload::Sweep,
        },
        cells,
    );
    bytes[6] = kind;
    bytes[7] = flags;
    bytes[70..78].copy_from_slice(&lower.to_le_bytes());
    bytes[78..86].copy_from_slice(&upper.to_le_bytes());
    bytes
}

/// Every kind byte, flag byte, cell count and range shape: the decoder
/// rejects exactly what the table forbids, and an accepted header encodes
/// back to the same bytes.
#[test]
fn forbidden_kind_flag_range_combinations_are_rejected() {
    for kind in 0..=5_u8 {
        for flags in 0..=u8::MAX {
            for cells in [0, 1] {
                for (lower, upper) in [(0, 0), (0, 1), (1, 1), (2, 1)] {
                    let bytes = raw_header(kind, flags, cells, lower, upper);
                    let decoded = read_header(&bytes, CLUSTER);
                    let case = (kind, flags, cells, lower, upper);
                    assert_eq!(
                        decoded.err(),
                        table_rule(kind, flags, cells, lower, upper),
                        "{case:?}"
                    );
                    if let Ok((header, count)) = decoded {
                        assert_eq!(write_header(&header, count), bytes, "{case:?}");
                    }
                }
            }
        }
    }
    for kind in [0, 1, 2] {
        let range = if kind == 0 { 0 } else { 1 };
        assert!(
            read_header(&raw_header(kind, PROBE, 0, 0, range), CLUSTER).is_ok(),
            "PROBE rides on kind {kind}"
        );
    }
    for kind in [1, 2] {
        assert!(
            read_header(&raw_header(kind, 0, 0, 4, 9), CLUSTER).is_ok(),
            "a closing packet of kind {kind} is accepted"
        );
    }
}
