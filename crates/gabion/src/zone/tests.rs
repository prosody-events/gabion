use quickcheck::{Arbitrary, Gen};
use quickcheck_macros::quickcheck;

use super::*;

/// A string of 0 to 70 characters drawn from the zone alphabet and a few
/// characters outside it.
#[derive(Clone, Debug)]
struct Text(String);

impl Arbitrary for Text {
    fn arbitrary(g: &mut Gen) -> Self {
        let pool: Vec<char> = ('a'..='c')
            .chain('X'..='Z')
            .chain('0'..='2')
            .chain(['.', '_', '-', ' ', '/', 'é', '\0'])
            .collect();
        let len = usize::arbitrary(g) % 71;
        Self((0..len).map(|_| *g.choose(&pool).unwrap()).collect())
    }
}

/// `parse` accepts exactly the label syntax: 1 to 63 characters of
/// `[A-Za-z0-9._-]`, letters or digits at both ends.
#[quickcheck]
fn zone_name_parse_accepts_exactly_label_syntax(text: Text) {
    let text = text.0;
    let alnum = |c: Option<char>| c.is_some_and(|c| c.is_ascii_alphanumeric());
    let valid = (1..=63).contains(&text.chars().count())
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        && alnum(text.chars().next())
        && alnum(text.chars().last());
    let parsed = ZoneName::parse(&text);
    assert_eq!(parsed.is_ok(), valid, "{text:?}");
    if let Ok(zone) = parsed {
        assert_eq!(zone.as_str(), text);
    }
}

#[test]
fn zone_name_parse_names_each_reason() {
    assert_eq!(ZoneName::parse(""), Err(ZoneError::Empty));
    assert_eq!(
        ZoneName::parse(&"a".repeat(64)),
        Err(ZoneError::TooLong { len: 64 })
    );
    assert_eq!(ZoneName::parse("-a"), Err(ZoneError::BadEdge { char: '-' }));
    assert_eq!(
        ZoneName::parse("a b"),
        Err(ZoneError::BadChar { char: ' ' })
    );
    for name in [
        "use1-az2",
        "1",
        "us-east-1a",
        "europe-west1-b",
        &"a".repeat(63),
    ] {
        assert!(ZoneName::parse(name).is_ok(), "{name}");
    }
}

/// A name hashes alike on every parse, a different name differently, and
/// the hash is never 0, which the wire reserves for "no zone".
#[test]
fn zone_hash_is_stable_and_never_zero() {
    let parse = |name| ZoneName::parse(name).unwrap().hash();
    assert_eq!(parse("use1-az2"), parse("use1-az2"));
    assert_ne!(parse("use1-az2"), parse("use1-az1"));
    assert_eq!(from_low32(0).get(), 1);
    assert_eq!(from_low32(7).get(), 7);
    assert_eq!(ZoneHash::from_wire(0), None);
    assert_eq!(ZoneHash::from_wire(9).map(ZoneHash::get), Some(9));
    assert_eq!(ZoneHash(from_low32(0x9f3a_1c77)).to_string(), "0x9f3a1c77");
}
