//! A hand-driven [`Clock`] for the visualizer.
//!
//! The engine owns virtual time, so neither `std::time::Instant` (unimplemented
//! on `wasm32-unknown-unknown`) nor `tokio::time` is ever touched — that is the
//! whole point: the production [`gabion::gossip::TokioClock`] drives ticks off
//! `tokio::time::interval`, which panics on wasm, so the wasm engine injects
//! this instead.
//!
//! Time is one shared `Rc<Cell<u64>>` every node reads through
//! [`Clock::now_millis`]. The runtime asks for two tickers, the heartbeat and
//! the prompt-round pacer, and each gets its own **bounded `mpsc`** inbox of
//! the instants the engine fires it for. An `mpsc` queues (unlike a `watch`,
//! which coalesces and would silently drop a tick a busy runtime hadn't polled
//! yet), so every fired tick is delivered exactly once — the determinism the
//! shareable-URL replay relies on. The prompt ticker sets the node's `armed`
//! flag while the runtime awaits it, which the runtime does only while it has
//! prompt work, so the engine fires it only then.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use gabion::gossip::{Clock, Ticker};
use tokio::sync::mpsc;

/// Shared virtual time, in milliseconds. The engine holds one handle and gives
/// every node's [`ManualClock`] a clone, so one [`Cell::set`] moves the whole
/// cluster's bucket clock at once.
pub type SharedNow = Rc<Cell<u64>>;

/// Ticks one node's tickers can queue; the engine fires each at most once
/// per sub-step and the runtime drains them as it goes.
const INBOX: usize = 256;

/// The engine's ends of one node's tickers.
pub struct Tickers {
    pub heartbeat: mpsc::Sender<u64>,
    pub prompt: mpsc::Sender<u64>,
    /// Set while the runtime awaits its prompt ticker: it has prompt work.
    pub armed: Rc<Cell<bool>>,
}

/// One node's clock: shared time plus that node's two tick inboxes. Built per
/// node by the engine; not `Clone` (each `mpsc::Receiver` is single-consumer).
pub struct ManualClock {
    now: SharedNow,
    /// The heartbeat interval; a ticker of any other interval is the prompt
    /// pacer.
    heartbeat: Duration,
    /// Each taken once by [`Clock::ticker`]; `RefCell<Option<_>>` so the
    /// `&self` trait method can move the receiver out.
    heartbeat_rx: RefCell<Option<mpsc::Receiver<u64>>>,
    prompt_rx: RefCell<Option<mpsc::Receiver<u64>>>,
    armed: Rc<Cell<bool>>,
}

impl ManualClock {
    /// Pair this node's clock with the ticker senders the engine keeps.
    pub fn new(now: SharedNow, heartbeat: Duration) -> (Self, Tickers) {
        let (heartbeat_tx, heartbeat_rx) = mpsc::channel(INBOX);
        let (prompt_tx, prompt_rx) = mpsc::channel(INBOX);
        let armed = Rc::new(Cell::new(false));
        let clock = Self {
            now,
            heartbeat,
            heartbeat_rx: RefCell::new(Some(heartbeat_rx)),
            prompt_rx: RefCell::new(Some(prompt_rx)),
            armed: Rc::clone(&armed),
        };
        let tickers = Tickers {
            heartbeat: heartbeat_tx,
            prompt: prompt_tx,
            armed,
        };
        (clock, tickers)
    }
}

impl Clock for ManualClock {
    type Ticker = ManualTicker;

    fn now_millis(&self) -> u64 {
        self.now.get()
    }

    /// The heartbeat inbox for the heartbeat interval, else the prompt inbox
    /// with the `armed` flag. An inbox already taken yields a ticker without
    /// an inbox, which never fires.
    fn ticker(&self, interval: Duration) -> ManualTicker {
        let (inbox, armed) = if interval == self.heartbeat {
            (&self.heartbeat_rx, None)
        } else {
            (&self.prompt_rx, Some(Rc::clone(&self.armed)))
        };
        let rx = inbox.borrow_mut().take();
        debug_assert!(rx.is_some(), "the runtime asks for each ticker once");
        ManualTicker { rx, armed }
    }
}

/// Resolves once per queued tick, to the instant it was fired for. Without an
/// inbox, or after the engine drops the sender, parks forever rather than
/// spinning the select loop on a closed channel.
pub struct ManualTicker {
    rx: Option<mpsc::Receiver<u64>>,
    armed: Option<Rc<Cell<bool>>>,
}

/// Holds a ticker's `armed` flag set while its tick future is polled.
struct Armed<'a>(&'a Cell<bool>);

impl Drop for Armed<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

impl Ticker for ManualTicker {
    // `async fn` in the impl satisfies the trait's `-> impl Future` signature.
    async fn tick(&mut self) -> u64 {
        // Created in the body, so only a polled future arms the flag.
        let _armed = self.armed.as_deref().map(|armed| {
            armed.set(true);
            Armed(armed)
        });
        if let Some(rx) = self.rx.as_mut()
            && let Some(at) = rx.recv().await
        {
            return at;
        }
        std::future::pending().await
    }
}
