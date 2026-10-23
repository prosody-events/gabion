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

/// Tickers a runtime asks its clock for: housekeeping, sync and broadcast.
const TICKERS: usize = 3;

/// One node's clock: shared time plus that node's tick inboxes. Built per
/// node by the engine; not `Clone` (the `mpsc::Receiver`s are single-consumer).
pub struct ManualClock {
    now: SharedNow,
    /// Handed out one per [`Clock::ticker`] call; `RefCell` so the `&self`
    /// trait method can move a receiver out.
    tick_rxs: RefCell<Vec<mpsc::Receiver<()>>>,
}

/// The engine's end of a node's tickers: one tick fires every ticker the
/// runtime created, so a visualizer step runs a full housekeeping, sync and
/// broadcast round.
pub struct TickSenders([mpsc::Sender<()>; TICKERS]);

impl TickSenders {
    pub async fn send(&self, tick: ()) -> Result<(), mpsc::error::SendError<()>> {
        for tx in &self.0 {
            tx.send(tick).await?;
        }
        Ok(())
    }
}

impl ManualClock {
    /// Pair this node's clock with the tick senders the engine keeps.
    pub fn new(now: SharedNow) -> (Self, TickSenders) {
        // Capacity well above the at-most-one-unconsumed-tick-per-drain the
        // engine produces; sized so a fire never blocks.
        let (txs, rxs): (Vec<_>, Vec<_>) = (0..TICKERS).map(|_| mpsc::channel(256)).unzip();
        let clock = Self {
            now,
            tick_rxs: RefCell::new(rxs),
        };
        let txs = txs
            .try_into()
            .unwrap_or_else(|_| unreachable!("TICKERS senders"));
        (clock, TickSenders(txs))
    }
}

impl Clock for ManualClock {
    type Ticker = ManualTicker;

    fn now_millis(&self) -> u64 {
        self.now.get()
    }

    fn ticker(&self, _interval: Duration) -> ManualTicker {
        let rx = self
            .tick_rxs
            .borrow_mut()
            .pop()
            .expect("ManualClock hands out one ticker per runtime ticker");
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
