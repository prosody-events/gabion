//! Unified, injectable time source for the gossip runtime.
//!
//! The runtime reads `now_millis()` once per gossip tick to feed
//! [`crate::crdt::CellStore::expire_at`], and awaits a [`Ticker`] for the
//! heartbeat cadence. Both ride on one injected [`Clock`] so the tick
//! scheduler and the bucket clock always read the *same* time: a production
//! [`TokioClock`] drives both off `tokio::time` (a single
//! `tokio::time::advance(d)` moves them in lockstep), while a hand-driven
//! clock can advance virtual time and fire ticks itself. Nothing in the
//! runtime is tied to `tokio` — it only sees the [`Clock`] and [`Ticker`]
//! traits.

use std::cell::Cell;
use std::future::Future;
use std::time::Duration;

/// Monotonic, non-decreasing wall-clock source in unix-epoch milliseconds,
/// paired with the gossip-tick source it drives.
pub trait Clock {
    /// The heartbeat source this clock hands the runtime.
    type Ticker: Ticker;

    /// Current time in milliseconds since the unix epoch.
    fn now_millis(&self) -> u64;

    /// Build a ticker that resolves once per gossip tick of `interval`.
    fn ticker(&self, interval: Duration) -> Self::Ticker;
}

/// The gossip heartbeat source. Each [`Ticker::tick`] resolution is one tick.
///
/// Uses the same return-position-`impl Future` idiom as
/// [`crate::gossip::GossipTransport`], so the runtime monomorphizes over it
/// with no `dyn` and without tripping `async_fn_in_trait`.
pub trait Ticker {
    /// Resolves the next time a gossip tick is due.
    fn tick(&mut self) -> impl Future<Output = ()> + '_;
}

/// Production + native-simulator default. Bridges `tokio::time::Instant`
/// (monotonic, mockable via `tokio::time::pause()`/`advance()`) to unix-epoch
/// millis via a startup-captured offset, and ticks off `tokio::time::interval`
/// reading the same clock.
///
/// Construction requires a tokio runtime in scope so it can capture an
/// `Instant`; use [`FixedClock`] in unit tests that don't have a runtime.
#[derive(Clone, Debug)]
pub struct TokioClock {
    base_millis: u64,
    base_instant: tokio::time::Instant,
}

impl TokioClock {
    /// Capture `SystemTime::now()` as the base and the current
    /// `tokio::time::Instant` as the offset. Production entry point.
    pub fn new() -> Self {
        let base_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            base_millis,
            base_instant: tokio::time::Instant::now(),
        }
    }

    /// Capture an explicit base. Tests using `tokio::time::pause()` pass
    /// `0` (or any seed) so the bucket clock starts at a known value.
    pub fn from_millis(base_millis: u64) -> Self {
        Self {
            base_millis,
            base_instant: tokio::time::Instant::now(),
        }
    }
}

impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TokioClock {
    type Ticker = TokioTicker;

    #[inline]
    fn now_millis(&self) -> u64 {
        let elapsed = tokio::time::Instant::now()
            .saturating_duration_since(self.base_instant)
            .as_millis() as u64;
        self.base_millis.saturating_add(elapsed)
    }

    fn ticker(&self, interval: Duration) -> TokioTicker {
        let mut tick = tokio::time::interval(interval);
        // `Delay`: a tick consumed late shifts the cadence forward rather than
        // bursting to catch up — the heartbeat semantics the runtime relies on.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        TokioTicker(tick)
    }
}

/// Production ticker: a `tokio::time::interval`, mockable under
/// `tokio::time::pause()` / `advance()` exactly as the runtime relied on
/// before the tick source was made injectable.
pub struct TokioTicker(tokio::time::Interval);

impl Ticker for TokioTicker {
    // `async fn` in the impl satisfies the trait's `-> impl Future` signature.
    #[inline]
    async fn tick(&mut self) {
        self.0.tick().await;
    }
}

/// Hand-driven clock for CRDT bucket-arithmetic unit tests that don't have a
/// tokio runtime. Single-threaded `Cell` interior mutability keeps the trait
/// method `&self`.
#[derive(Debug, Default)]
pub struct FixedClock(Cell<u64>);

impl FixedClock {
    pub fn new(now_millis: u64) -> Self {
        Self(Cell::new(now_millis))
    }

    pub fn set(&self, now_millis: u64) {
        self.0.set(now_millis);
    }

    pub fn advance(&self, delta_millis: u64) {
        self.0.set(self.0.get().saturating_add(delta_millis));
    }
}

impl Clock for FixedClock {
    type Ticker = PendingTicker;

    #[inline]
    fn now_millis(&self) -> u64 {
        self.0.get()
    }

    /// `FixedClock` feeds CRDT bucket-arithmetic tests that never run the
    /// gossip loop, so its ticker never fires.
    fn ticker(&self, _interval: Duration) -> PendingTicker {
        PendingTicker
    }
}

/// A ticker that never resolves — see [`FixedClock::ticker`].
pub struct PendingTicker;

impl Ticker for PendingTicker {
    async fn tick(&mut self) {
        std::future::pending().await
    }
}
