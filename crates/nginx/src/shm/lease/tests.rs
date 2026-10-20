use super::*;

/// Build a lease whose init anchor is set so callers can feed in
/// either tiny test millis (the lease maps them through `init=0`) or
/// realistic unix epoch millis (set init to slightly before the first
/// call).
fn lease() -> LeaderLease {
    LeaderLease::default()
}

#[test]
fn first_acquire_wins_and_renew_succeeds() {
    let lease = lease();
    assert!(lease.try_acquire(11, 1_000, 500));
    assert!(lease.try_acquire(11, 1_200, 500));
    let snap = lease.snapshot();
    assert_eq!(snap.owner_worker, truncate_owner(11));
}

#[test]
fn other_worker_cannot_steal_active_lease() {
    let lease = lease();
    assert!(lease.try_acquire(11, 1_000, 500));
    assert!(!lease.try_acquire(22, 1_100, 500));
}

#[test]
fn expired_lease_can_be_taken_over() {
    let lease = lease();
    assert!(lease.try_acquire(11, 1_000, 500));
    // After the lease expires (now > 1_500), another worker grabs it.
    assert!(lease.try_acquire(22, 1_600, 500));
    let snap = lease.snapshot();
    assert_eq!(snap.owner_worker, truncate_owner(22));
}

#[test]
fn release_resets_owner() {
    let lease = lease();
    assert!(lease.try_acquire(11, 1_000, 500));
    assert!(lease.release(11));
    assert!(lease.try_acquire(22, 1_100, 500));
}

#[test]
fn release_only_succeeds_for_current_owner() {
    let lease = lease();
    assert!(lease.try_acquire(11, 1_000, 500));
    assert!(!lease.release(22));
}

#[test]
fn zero_worker_or_ttl_rejects() {
    let lease = lease();
    assert!(!lease.try_acquire(0, 0, 100));
    assert!(!lease.try_acquire(1, 0, 0));
}

#[test]
fn epoch_bumps_on_takeover_not_renew() {
    let lease = lease();
    assert!(lease.try_acquire(11, 1_000, 500));
    let snap1 = lease.snapshot();
    assert!(lease.try_acquire(11, 1_100, 500));
    let snap2 = lease.snapshot();
    assert_eq!(snap1.epoch, snap2.epoch);
    assert!(lease.try_acquire(22, 1_700, 500));
    let snap3 = lease.snapshot();
    assert_eq!(snap3.epoch, snap2.epoch + 1);
}

#[test]
fn ms_precision_takeover_just_after_expiry() {
    let lease = lease();
    assert!(lease.try_acquire(1, 0, 1_000));
    assert!(lease.try_acquire(2, 1_001, 1_000));
}
