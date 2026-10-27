use std::sync::atomic::AtomicU64;

use super::{bump, bump_shared};

#[test]
fn throttle_fires_at_powers_of_two_only() {
    let oracle: Vec<u64> = (0..13).map(|k| 1u64 << k).collect();
    let mut owned = 0;
    let shared = AtomicU64::new(0);
    for occurrence in 1..=4096u64 {
        let expected = oracle.contains(&occurrence).then_some(occurrence);
        assert_eq!(
            bump(&mut owned),
            expected,
            "owned counter, occurrence {occurrence}"
        );
        assert_eq!(
            bump_shared(&shared),
            expected,
            "shared counter, occurrence {occurrence}"
        );
    }
}
