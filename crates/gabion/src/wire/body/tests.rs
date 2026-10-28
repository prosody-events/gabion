use super::*;
use crate::crdt::{NodeId, NodeIdentity};
use crate::settings::mock::{rules, storage};

fn store() -> CellStore {
    CellStore::new(
        storage(256, 48, 64),
        NodeIdentity::new(NodeId(0xABCD), 1),
        &rules(32),
    )
}

#[test]
fn empty_body_round_trips() {
    decode_body(&[], 0, |_| panic!("no cells")).expect("ok");
}

#[test]
fn empty_cell_count_with_trailing_bytes_rejected() {
    let err = decode_body(&[0u8; 4], 0, |_| {}).unwrap_err();
    assert_eq!(err, DecodeError::BodyLength);
}

#[test]
fn truncated_body_rejected() {
    let err = decode_body(&[0u8; 0], 1, |_| {}).unwrap_err();
    assert_eq!(err, DecodeError::BodyLength);
}

#[test]
fn scratch_for_store_sizes_from_dict_capacity() {
    let s = store();
    let scratch = WireScratch::for_store(&s);
    assert_eq!(
        scratch.rule_remap.len(),
        s.rule_dictionary().capacity() as usize
    );
    assert_eq!(
        scratch.node_remap.len(),
        s.node_dictionary().capacity() as usize
    );
    assert!(scratch.rule_remap.iter().all(|&b| b == u8::MAX));
    assert!(scratch.node_remap.iter().all(|&b| b == u8::MAX));
}
