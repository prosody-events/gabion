use super::*;
use crate::wire::{
    ControlKind, DigestScope, decode_control, encode_control, is_control, is_digest,
};

const KEY: HmacKey = HmacKey([9; 32]);

fn header() -> Header {
    Header {
        cluster_id_hash: 0xC1,
        sender_node_id: 0xAA,
        sender_incarnation: 3,
        ..Header::default()
    }
}

fn limits() -> FrameLimits {
    FrameLimits {
        max_payload_bytes: 1400,
        max_cells: 4096,
    }
}

type Group = (NodeId, Incarnation, Vec<u32>);

fn scope(buckets: u8) -> DigestScope {
    DigestScope {
        rule_fingerprint: 0xF00D,
        first_bucket: 40,
        buckets,
        reply: buckets.is_multiple_of(2),
    }
}

/// Write as many of `groups` as fit one frame; returns the frame and how many
/// were written.
fn write(groups: &[Group], buckets: u8, key: Option<&HmacKey>) -> (PacketBuf, usize) {
    let mut buf = PacketBuf::for_limits(limits());
    let mut writer =
        DigestWriter::begin(&mut buf, scope(buckets), 5, key.is_some(), limits()).unwrap();
    let mut written = 0;
    for (node_id, incarnation, sums) in groups {
        if !writer.has_room() {
            break;
        }
        writer.push(*node_id, *incarnation, sums.iter().copied());
        written += 1;
    }
    let hi = groups.get(written).map_or(u128::MAX, |g| g.0.0);
    writer.finish(header(), hi, key);
    (buf, written)
}

fn read(buf: &PacketBuf, key: Option<&HmacKey>) -> Result<(DigestFrame, Vec<Group>), DecodeError> {
    let mut got = Vec::new();
    let frame = decode_digest(
        buf.as_bytes(),
        key,
        limits(),
        |node_id, incarnation, sums| {
            got.push((
                node_id,
                incarnation,
                (0..sums.len()).map(|i| sums.get(i)).collect(),
            ));
        },
    )?;
    Ok((frame, got))
}

fn groups(n: u32, buckets: u32) -> Vec<Group> {
    (0..n)
        .map(|i| {
            let sums = (0..buckets).map(|b| i * 100 + b).collect();
            (NodeId(0x1000 + u128::from(i)), i, sums)
        })
        .collect()
}

#[test]
fn a_digest_round_trips_with_and_without_auth() {
    let all = groups(3, 8);
    for key in [None, Some(&KEY)] {
        let (buf, written) = write(&all, 8, key);
        assert_eq!(written, 3);
        assert!(is_control(buf.as_bytes()) && is_digest(buf.as_bytes()));
        let (frame, got) = read(&buf, key).unwrap();
        assert_eq!(got, all);
        assert_eq!(frame.scope, scope(8));
        assert_eq!((frame.lo, frame.hi), (5, u128::MAX));
        assert_eq!(frame.header.sender_node_id, 0xAA);
    }
}

#[test]
fn a_full_frame_stops_taking_groups_and_names_where_it_ends() {
    let all = groups(100, 8);
    let (buf, written) = write(&all, 8, Some(&KEY));
    assert!(
        written > 10 && written < 100,
        "{written} groups in one frame"
    );
    assert!(buf.as_bytes().len() <= limits().max_payload_bytes);
    let (frame, got) = read(&buf, Some(&KEY)).unwrap();
    assert_eq!(got, all[..written]);
    assert_eq!(frame.hi, all[written].0.0);
}

#[test]
fn a_digest_with_no_groups_says_its_range_holds_nothing() {
    let (buf, written) = write(&[], 8, None);
    assert_eq!(written, 0);
    let (frame, got) = read(&buf, None).unwrap();
    assert!(got.is_empty());
    assert_eq!((frame.lo, frame.hi), (5, u128::MAX));
}

#[test]
fn digests_and_entry_frames_do_not_decode_as_each_other() {
    let (digest, _) = write(&groups(2, 4), 4, None);
    assert_eq!(
        decode_control(digest.as_bytes(), None, limits(), |_| {}),
        Err(DecodeError::BadControl)
    );
    let mut prune = PacketBuf::for_limits(limits());
    encode_control(
        header(),
        ControlKind::Prune,
        1,
        &[],
        None,
        limits(),
        &mut prune,
    )
    .unwrap();
    assert!(!is_digest(prune.as_bytes()));
    assert_eq!(
        decode_digest(prune.as_bytes(), None, limits(), |_, _, _| {}),
        Err(DecodeError::TooShort)
    );
}

#[test]
fn a_tampered_or_truncated_digest_is_rejected() {
    let (buf, _) = write(&groups(2, 4), 4, Some(&KEY));
    let mut bytes = buf.as_bytes().to_vec();
    let last = bytes.len() - AUTH_TAG_LEN - 1;
    bytes[last] ^= 1;
    assert_eq!(
        decode_digest(&bytes, Some(&KEY), limits(), |_, _, _| {}),
        Err(DecodeError::AuthenticationFailed)
    );
    let (buf, _) = write(&groups(2, 4), 4, None);
    let short = &buf.as_bytes()[..buf.as_bytes().len() - 4];
    assert!(decode_digest(short, None, limits(), |_, _, _| {}).is_err());
}

#[test]
fn a_budget_too_small_for_one_group_is_refused() {
    let tiny = FrameLimits {
        max_payload_bytes: HEADER_LEN + FIXED + GROUP_IDENT + 4,
        max_cells: 1,
    };
    let mut buf = PacketBuf::for_limits(tiny);
    assert!(DigestWriter::begin(&mut buf, scope(1), 0, false, tiny).is_ok());
    assert_eq!(
        DigestWriter::begin(&mut buf, scope(2), 0, false, tiny).err(),
        Some(EncodeError::BudgetTooSmall)
    );
}

#[test]
fn the_reply_mark_round_trips() {
    for buckets in [3, 4] {
        let (buf, _) = write(&groups(1, u32::from(buckets)), buckets, None);
        assert_eq!(
            read(&buf, None).unwrap().0.scope.reply,
            buckets.is_multiple_of(2)
        );
    }
}
