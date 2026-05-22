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

#[test]
fn unix_epoch_millis_dont_break_lease() {
    // Reproduces the production bug: without init_millis, two workers
    // would both acquire because `now.min(2^40-1)` clamps every recent
    // unix-epoch value to the same number.
    let lease = lease();
    // Mirror the production startup: SHM init stamps the anchor, then
    // the first worker calls try_acquire shortly after.
    let init_now = 1_770_000_000_000_u64; // ~2026-02 unix epoch millis
    lease.set_init_millis(init_now);
    let now1 = init_now + 5;
    assert!(lease.try_acquire(30, now1, 1_000));
    // A second worker calling almost simultaneously must NOT win.
    let now2 = init_now + 6;
    assert!(!lease.try_acquire(31, now2, 1_000));
    // After ttl expires, the second worker takes over.
    let now3 = init_now + 1_500;
    assert!(lease.try_acquire(31, now3, 1_000));
}

#[test]
fn set_init_millis_is_idempotent() {
    let lease = lease();
    lease.set_init_millis(100);
    lease.set_init_millis(200);
    assert_eq!(lease.init_millis(), 100);
}
