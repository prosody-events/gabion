//! Cross-worker leader lease.
//!
//! Atomic CAS-controlled deadline electing one worker to run the gossip
//! runtime at a time. Owner + expiration are packed into a single
//! `AtomicU64` so they update atomically — without the pack, a successful
//! `owner` CAS followed by a separate `expires` store opens a brief window
//! where another worker can observe the new owner with a stale (zero)
//! expiry and steal the lease via a second CAS. This race is real (miri
//! catches it under multi-thread contention) so the lock-free protocol
//! must operate on a single 64-bit value.
//!
//! Pack layout: `owner_worker` in the high 24 bits, `expires_millis_rel`
//! in the low 40 bits. u24 holds 16M worker ids (well above any plausible
//! nginx PID); u40 holds ~34 years of milliseconds.
//!
//! Time anchoring: the stored `expires_millis_rel` is *relative to zone
//! init time*, not unix epoch. Unix epoch milliseconds in 2026 already
//! exceed 2^40 (~1.77e12 vs 1.10e12), so a naive `now.min(EXPIRES_MASK)`
//! clamp would make every fresh call observe `now == EXPIRES_MASK ==
//! expires`, breaking the "active lease" check and letting every worker
//! steal the lease at startup. Subtracting `init_millis` keeps the
//! relative value well inside u40 for any sane process lifetime.

use std::sync::atomic::{AtomicU64, Ordering};

const OWNER_SHIFT: u32 = 40;
const OWNER_MAX: u32 = (1 << 24) - 1;
const EXPIRES_MASK: u64 = (1 << OWNER_SHIFT) - 1;

#[repr(C)]
#[derive(Debug, Default)]
pub struct LeaderLease {
    /// Packed `(owner_worker: u24, expires_millis_rel: u40)` —
    /// expires_millis_rel is measured from `init_millis` below.
    state: AtomicU64,
    pub epoch: AtomicU64,
    /// Wall-clock millis at which the SHM zone was initialized. Stamped
    /// once by `ShmRegion::initialize`. All subsequent lease arithmetic
    /// happens in `now - init` space so the relative value stays inside
    /// u40 regardless of the absolute clock value.
    init_millis: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LeaseSnapshot {
    pub owner_worker: u32,
    /// Absolute wall-clock millis at which the lease expires.
    pub expires_millis: u64,
    pub epoch: u64,
}

#[inline]
const fn pack(owner: u32, expires_millis_rel: u64) -> u64 {
    let owner = owner & OWNER_MAX;
    let expires = expires_millis_rel & EXPIRES_MASK;
    ((owner as u64) << OWNER_SHIFT) | expires
}

#[inline]
const fn unpack(state: u64) -> (u32, u64) {
    let owner = (state >> OWNER_SHIFT) as u32;
    let expires = state & EXPIRES_MASK;
    (owner, expires)
}

#[inline]
fn truncate_owner(worker_id: u32) -> u32 {
    // Folds the high bits into u24 space so two `getpid()` values that only
    // differ above bit 23 still map to distinct slots in practice (the
    // birthday-paradox collision rate at 16M slots is negligible for the
    // handful of workers a single nginx process spawns).
    let lo = worker_id & OWNER_MAX;
    let hi = worker_id >> 24;
    let folded = lo ^ hi;
    if folded == 0 { 1 } else { folded }
}

impl LeaderLease {
    /// Stamp the wall-clock anchor used by all subsequent lease arithmetic.
    /// Idempotent on the first call: stores `now_millis` only if the field
    /// is still zero (default), so repeated initializations don't clobber
    /// a live anchor.
    pub fn set_init_millis(&self, now_millis: u64) {
        let _ = self.init_millis.compare_exchange(
            0,
            now_millis,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub fn init_millis(&self) -> u64 {
        self.init_millis.load(Ordering::Acquire)
    }

    pub fn snapshot(&self) -> LeaseSnapshot {
        let state = self.state.load(Ordering::Acquire);
        let (owner, expires_rel) = unpack(state);
        LeaseSnapshot {
            owner_worker: owner,
            expires_millis: expires_rel.saturating_add(self.init_millis()),
            epoch: self.epoch.load(Ordering::Acquire),
        }
    }

    /// Attempt to acquire (or renew) the lease for `worker_id`. Returns
    /// `true` if `worker_id` holds the lease through `now_millis +
    /// ttl_millis`.
    pub fn try_acquire(&self, worker_id: u32, now_millis: u64, ttl_millis: u64) -> bool {
        if worker_id == 0 || ttl_millis == 0 {
            return false;
        }
        let init = self.init_millis();
        // If the caller's clock predates the SHM init, treat it as time
        // zero — that only happens on truly broken clock skew and we want
        // the lease comparison to still terminate.
        let now_rel = now_millis.saturating_sub(init).min(EXPIRES_MASK);
        let new_expires_rel = now_rel.saturating_add(ttl_millis).min(EXPIRES_MASK);
        let owner_id = truncate_owner(worker_id);
        loop {
            let state = self.state.load(Ordering::Acquire);
            let (owner, expires_rel) = unpack(state);

            if owner == owner_id {
                // Renew: keep ownership, bump expiry.
                let new_state = pack(owner_id, new_expires_rel);
                match self.state.compare_exchange(
                    state,
                    new_state,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return true,
                    Err(_) => continue,
                }
            }
            if owner != 0 && expires_rel > now_rel {
                return false;
            }
            // Take over an idle/expired lease. Single CAS on the packed
            // state guarantees owner and expires update atomically.
            let new_state = pack(owner_id, new_expires_rel);
            match self.state.compare_exchange(
                state,
                new_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.epoch.fetch_add(1, Ordering::AcqRel);
                    return true;
                }
                Err(_) => continue,
            }
        }
    }

    pub fn release(&self, worker_id: u32) -> bool {
        let owner_id = truncate_owner(worker_id);
        loop {
            let state = self.state.load(Ordering::Acquire);
            let (owner, _) = unpack(state);
            if owner != owner_id {
                return false;
            }
            match self.state.compare_exchange(
                state,
                pack(0, 0),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
}
