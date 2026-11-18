use super::{CellIndex, EMPTY_INDEX_SLOT};
use crate::crdt::hash::mix64;

/// Maximum probe distance currently in use. O(capacity).
fn max_probe_distance(index: &CellIndex) -> u32 {
    (0..index.capacity)
        .filter(|&slot| index.slot_indexes[slot as usize] != EMPTY_INDEX_SLOT)
        .map(|slot| index.probe_distance(slot, index.hashes[slot as usize]))
        .max()
        .unwrap_or(0)
}

#[test]
fn robin_hood_backshift_keeps_probe_bounded() {
    // Deterministic churn — insert and remove many entries and verify
    // the index keeps probe distance bounded relative to log2(capacity).
    let cap = 1024_u32;
    let mut index = CellIndex::with_capacity(cap);
    let mut occupants: Vec<(u64, u32)> = Vec::new();
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for _ in 0..(cap as usize / 2) {
        state = mix64(state);
        let slot = (state as u32) % (cap / 2);
        let h = mix64(state.wrapping_add(slot as u64));
        index.insert_unchecked(h, slot);
        occupants.push((h, slot));
    }
    // Churn: remove half, insert different.
    for _ in 0..(occupants.len() / 2) {
        let (h, slot) = occupants.pop().unwrap();
        index.remove(h, slot);
    }
    for _ in 0..(cap as usize / 4) {
        state = mix64(state);
        let slot = ((state >> 16) as u32) % (cap / 2) + (cap / 2);
        let h = mix64(state.wrapping_add(slot as u64));
        index.insert_unchecked(h, slot);
        occupants.push((h, slot));
    }
    // Backshift must keep every survivor reachable, not just shorten probes.
    assert_eq!(index.len() as usize, occupants.len());
    for &(h, slot) in &occupants {
        assert_eq!(index.find(h, |s| s == slot), Some(slot));
    }
    let max_dist = max_probe_distance(&index);
    // Bound is generous: should be <= load * small constant. Cap log2 = 10,
    // and load factor is below 50%. Empirical worst case ~ a few dozen.
    assert!(
        max_dist < 64,
        "max probe distance {max_dist} too large under churn"
    );
}
