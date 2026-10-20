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
//! Clock: production callers pass [`now_millis`], which reads
//! `CLOCK_MONOTONIC`. Every process on the host shares that clock, and NTP
//! steps don't move it, so a wall-clock jump can't expire a live leader's
//! lease and let a campaigning follower take over while the leader still
//! runs.
//!
//! Time anchoring: the stored `expires_millis_rel` is *relative to zone
//! init time*. A raw clock reading can exceed 2^40 (unix epoch milliseconds
//! in 2026 are ~1.77e12 vs 1.10e12), so a naive `now.min(EXPIRES_MASK)`
//! clamp would make every fresh call observe `now == EXPIRES_MASK ==
//! expires`, breaking the "active lease" check and letting every worker
//! steal the lease at startup. Subtracting `init_millis` keeps the
//! relative value well inside u40 for any sane process lifetime.

use std::sync::atomic::{AtomicU64, Ordering};

const OWNER_SHIFT: u32 = 40;
const OWNER_MAX: u32 = (1 << 24) - 1;
const EXPIRES_MASK: u64 = (1 << OWNER_SHIFT) - 1;

/// Milliseconds on `CLOCK_MONOTONIC`, the clock all lease arithmetic uses.
pub fn now_millis() -> u64 {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `clock_gettime` only writes a `timespec` through its pointer,
    // and `now` is a live, writable, aligned `timespec`. `CLOCK_MONOTONIC` is
    // supported on every platform nginx runs on, so the call can't fail.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) };
    debug_assert_eq!(rc, 0, "clock_gettime(CLOCK_MONOTONIC) failed");
    now.tv_sec as u64 * 1_000 + now.tv_nsec as u64 / 1_000_000
}

#[repr(C)]
#[derive(Debug, Default)]
pub struct LeaderLease {
    /// Packed `(owner_worker: u24, expires_millis_rel: u40)` —
    /// expires_millis_rel is measured from `init_millis` below.
    state: AtomicU64,
    pub epoch: AtomicU64,
    /// [`now_millis`] at which the SHM zone was initialized. Stamped
    /// once by `ShmRegion::initialize`. All subsequent lease arithmetic
    /// happens in `now - init` space so the relative value stays inside
    /// u40 regardless of the absolute clock value.
    init_millis: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LeaseSnapshot {
    pub owner_worker: u32,
    /// [`now_millis`] at which the lease expires.
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
    /// Stamp the clock anchor used by all subsequent lease arithmetic.
    /// Idempotent on the first call: stores `now_millis` only if the field
    /// is still zero (default), so repeated initializations don't clobber
    /// a live anchor.
    pub fn set_init_millis(&self, now_millis: u64) {
        let _ =
            self.init_millis
                .compare_exchange(0, now_millis, Ordering::AcqRel, Ordering::Acquire);
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

    /// Whether `worker_id` owns the lease, expired or not. A leader whose
    /// lease expired still owns it until another worker takes it over.
    pub fn is_held_by(&self, worker_id: u32) -> bool {
        unpack(self.state.load(Ordering::Acquire)).0 == truncate_owner(worker_id)
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
