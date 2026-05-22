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
//! Pack layout: `owner_worker` in the high 24 bits, `expires_millis` in the
//! low 40 bits. u24 holds 16M worker ids (well above any plausible nginx
//! PID); u40 holds ~34 years of milliseconds — comfortable runway for the
//! seconds-since-epoch timestamps we feed in.

use std::sync::atomic::{AtomicU64, Ordering};

const OWNER_SHIFT: u32 = 40;
const OWNER_MAX: u32 = (1 << 24) - 1;
const EXPIRES_MASK: u64 = (1 << OWNER_SHIFT) - 1;

#[repr(C)]
#[derive(Debug, Default)]
pub struct LeaderLease {
    /// Packed `(owner_worker: u24, expires_millis: u40)` — see module docs.
    state: AtomicU64,
    pub epoch: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LeaseSnapshot {
    pub owner_worker: u32,
    pub expires_millis: u64,
    pub epoch: u64,
}

#[inline]
const fn pack(owner: u32, expires_millis: u64) -> u64 {
    let owner = owner & OWNER_MAX;
    let expires = expires_millis & EXPIRES_MASK;
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
    pub fn snapshot(&self) -> LeaseSnapshot {
        let state = self.state.load(Ordering::Acquire);
        let (owner, expires_millis) = unpack(state);
        LeaseSnapshot {
            owner_worker: owner,
            expires_millis,
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
        let owner_id = truncate_owner(worker_id);
        let new_expires =
            now_millis.saturating_add(ttl_millis).min(EXPIRES_MASK);
        let now_clamped = now_millis.min(EXPIRES_MASK);
        loop {
            let state = self.state.load(Ordering::Acquire);
            let (owner, expires) = unpack(state);

            if owner == owner_id {
                // Renew.
                let new_state = pack(owner_id, new_expires);
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
            if owner != 0 && expires > now_clamped {
                return false;
            }
            // Take over an idle/expired lease. Single CAS on the packed
            // state guarantees owner and expires update atomically.
            let new_state = pack(owner_id, new_expires);
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

    #[test]
    fn first_acquire_wins_and_renew_succeeds() {
        let lease = LeaderLease::default();
        assert!(lease.try_acquire(11, 1_000, 500));
        assert!(lease.try_acquire(11, 1_200, 500));
        let snap = lease.snapshot();
        assert_eq!(snap.owner_worker, truncate_owner(11));
    }

    #[test]
    fn other_worker_cannot_steal_active_lease() {
        let lease = LeaderLease::default();
        assert!(lease.try_acquire(11, 1_000, 500));
        assert!(!lease.try_acquire(22, 1_100, 500));
    }

    #[test]
    fn expired_lease_can_be_taken_over() {
        let lease = LeaderLease::default();
        assert!(lease.try_acquire(11, 1_000, 500));
        // After the lease expires (now > 1_500), another worker grabs it.
        assert!(lease.try_acquire(22, 1_600, 500));
        let snap = lease.snapshot();
        assert_eq!(snap.owner_worker, truncate_owner(22));
    }

    #[test]
    fn release_resets_owner() {
        let lease = LeaderLease::default();
        assert!(lease.try_acquire(11, 1_000, 500));
        assert!(lease.release(11));
        assert!(lease.try_acquire(22, 1_100, 500));
    }

    #[test]
    fn release_only_succeeds_for_current_owner() {
        let lease = LeaderLease::default();
        assert!(lease.try_acquire(11, 1_000, 500));
        assert!(!lease.release(22));
    }

    #[test]
    fn zero_worker_or_ttl_rejects() {
        let lease = LeaderLease::default();
        assert!(!lease.try_acquire(0, 0, 100));
        assert!(!lease.try_acquire(1, 0, 0));
    }

    #[test]
    fn epoch_bumps_on_takeover_not_renew() {
        let lease = LeaderLease::default();
        assert!(lease.try_acquire(11, 1_000, 500));
        let snap1 = lease.snapshot();
        // Renewal must not bump epoch.
        assert!(lease.try_acquire(11, 1_100, 500));
        let snap2 = lease.snapshot();
        assert_eq!(snap1.epoch, snap2.epoch);
        // Takeover after expiry bumps epoch.
        assert!(lease.try_acquire(22, 1_700, 500));
        let snap3 = lease.snapshot();
        assert_eq!(snap3.epoch, snap2.epoch + 1);
    }

    #[test]
    fn ms_precision_takeover_just_after_expiry() {
        let lease = LeaderLease::default();
        // ttl = 1000ms, now = 0 → expires_millis = 1000.
        assert!(lease.try_acquire(1, 0, 1_000));
        // At now=1000ms, the lease is still considered active (expires>now
        // is false because 1000 > 1000 is false, so we'd be allowed).
        // At now=1001ms, we are past expiry and should take over.
        assert!(lease.try_acquire(2, 1_001, 1_000));
    }
}
