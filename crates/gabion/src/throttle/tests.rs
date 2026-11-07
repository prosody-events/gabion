use std::sync::atomic::{AtomicU64, Ordering};

use quickcheck_macros::quickcheck;

use super::{bump, bump_shared, catch_up};

#[test]
fn throttle_fires_at_powers_of_two_only() {
    let oracle: Vec<u64> = (0..13).map(|k| 1u64 << k).collect();
    let mut owned = 0;
    let shared = AtomicU64::new(0);
    let seen = AtomicU64::new(0);
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
        assert_eq!(
            catch_up(&seen, occurrence),
            expected,
            "read count, occurrence {occurrence}"
        );
    }
}

/// A count read at any two totals logs exactly when a power of two lies
/// between them, and the mark ends at the larger, so a reader behind
/// another never moves it back.
#[quickcheck]
fn catch_up_logs_once_per_power_of_two_passed(seen: u16, total: u16) -> bool {
    let (seen, total) = (u64::from(seen), u64::from(total));
    let mark = AtomicU64::new(seen);
    let expected = (seen + 1..=total)
        .any(u64::is_power_of_two)
        .then_some(total);
    catch_up(&mark, total) == expected && mark.load(Ordering::Relaxed) == seen.max(total)
}
