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
//! Pack layout: `owner_worker` in the high 24 bits, `expires_millis`
//! in the low 40 bits. u24 holds 16M worker ids (well above any plausible
//! nginx PID); u40 holds ~34 years of milliseconds.
//!
//! Clock: callers pass [`now_millis`], which every worker reads the same
//! way and which NTP steps don't move, so a wall-clock jump can't expire a
//! live leader's lease and let a campaigning follower take over while the
//! leader still runs.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const OWNER_SHIFT: u32 = 40;
const OWNER_MAX: u32 = (1 << 24) - 1;
const EXPIRES_MASK: u64 = (1 << OWNER_SHIFT) - 1;

/// Milliseconds since this process tree first called it. `Instant` reads a
/// system-wide monotonic clock, and the nginx master calls this before it
/// forks workers, so every worker inherits the same anchor and their
/// readings agree.
pub fn now_millis() -> u64 {
    static ANCHOR: OnceLock<Instant> = OnceLock::new();
    ANCHOR.get_or_init(Instant::now).elapsed().as_millis() as u64
}

#[repr(C)]
#[derive(Debug, Default)]
pub struct LeaderLease {
    /// Packed `(owner_worker: u24, expires_millis: u40)`.
    state: AtomicU64,
    pub epoch: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LeaseSnapshot {
    pub owner_worker: u32,
    /// [`now_millis`] at which the lease expires.
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
        let now = now_millis.min(EXPIRES_MASK);
        let new_expires = now.saturating_add(ttl_millis).min(EXPIRES_MASK);
        let owner_id = truncate_owner(worker_id);
        loop {
            let state = self.state.load(Ordering::Acquire);
            let (owner, expires) = unpack(state);

            if owner == owner_id {
                // Renew: keep ownership, bump expiry.
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
            if owner != 0 && expires > now {
                return false;
            }
            // Take over an idle/expired lease. Single CAS on the packed
            // state guarantees owner and expires update atomically.
            let new_state = pack(owner_id, new_expires);
            match self
                .state
                .compare_exchange(state, new_state, Ordering::AcqRel, Ordering::Acquire)
            {
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
mod tests;
