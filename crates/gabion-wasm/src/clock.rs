//! A hand-driven [`Clock`] for the visualizer.
//!
//! The engine owns virtual time, so neither `std::time::Instant` (unimplemented
//! on `wasm32-unknown-unknown`) nor `tokio::time` is ever touched — that is the
//! whole point: the production [`gabion::gossip::TokioClock`] drives ticks off
//! `tokio::time::interval`, which panics on wasm, so the wasm engine injects
//! this instead.
//!
//! Time is one shared `Rc<Cell<u64>>` every node reads through
//! [`Clock::now_millis`]. Ticks are a **per-node bounded `mpsc`**: the engine
//! sets the clock then sends one `()` per node per gossip tick. An `mpsc`
//! queues (unlike a `watch`, which coalesces and would silently drop a tick a
//! busy runtime hadn't polled yet), so every fired tick is delivered exactly
//! once — the determinism the shareable-URL replay relies on.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use gabion::gossip::{Clock, Ticker};
use tokio::sync::mpsc;

/// Shared virtual time, in milliseconds. The engine holds one handle and gives
/// every node's [`ManualClock`] a clone, so one [`Cell::set`] moves the whole
/// cluster's bucket clock at once.
pub type SharedNow = Rc<Cell<u64>>;

/// One node's clock: shared time plus that node's own tick inbox. Built per
/// node by the engine; not `Clone` (the `mpsc::Receiver` is single-consumer).
pub struct ManualClock {
    now: SharedNow,
    /// Taken once by [`Clock::ticker`]; `RefCell<Option<_>>` so the `&self`
    /// trait method can move the receiver out.
    tick_rx: RefCell<Option<mpsc::Receiver<()>>>,
}

impl ManualClock {
    /// Pair this node's clock with the tick sender the engine keeps.
    pub fn new(now: SharedNow) -> (Self, mpsc::Sender<()>) {
        // Capacity well above the at-most-one-unconsumed-tick-per-drain the
        // engine produces; sized so a fire never blocks.
        let (tick_tx, tick_rx) = mpsc::channel(256);
        let clock = Self {
            now,
            tick_rx: RefCell::new(Some(tick_rx)),
        };
        (clock, tick_tx)
    }
}

impl Clock for ManualClock {
    type Ticker = ManualTicker;

    fn now_millis(&self) -> u64 {
        self.now.get()
    }

    fn ticker(&self, _interval: Duration) -> ManualTicker {
        let rx = self
            .tick_rx
            .borrow_mut()
            .take()
            .expect("ManualClock::ticker called more than once");
        ManualTicker(rx)
    }
}

/// Resolves once per queued tick. After the engine drops the sender, parks
/// forever rather than spinning the select loop on a closed channel.
pub struct ManualTicker(mpsc::Receiver<()>);

impl Ticker for ManualTicker {
    // `async fn` in the impl satisfies the trait's `-> impl Future` signature.
    async fn tick(&mut self) {
        if self.0.recv().await.is_none() {
            std::future::pending::<()>().await;
        }
    }
}
