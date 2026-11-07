//! Captures the WARN and ERROR events gabion emits during a run.
//!
//! gabion reports saturation only through `tracing`, usually on power-of-two
//! thresholds of a running `*_total` counter. The run's subscriber is
//! `tracing_subscriber::fmt` writing JSON lines into [`Warnings`], which
//! tallies every line per `(level, target, message)` and per node, keeping
//! the largest `*_total` field each node reported, and counts any line it
//! cannot read as such an event. The driver names the node it is polling;
//! lines outside a node poll are attributed to [`NO_NODE`].

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::{Level, Subscriber};
use tracing_subscriber::fmt::MakeWriter;

pub const NO_NODE: usize = usize::MAX;

/// The levels the subscriber passes. `Error` sorts first, as the level
/// strings did.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Severity {
    Error,
    Warn,
}

/// One JSON line as `tracing_subscriber::fmt().json()` writes it.
#[derive(Deserialize)]
struct Event {
    level: Severity,
    target: String,
    fields: Fields,
}

#[derive(Deserialize)]
struct Fields {
    message: String,
    #[serde(flatten)]
    rest: Map<String, Value>,
}

type Kind = (Severity, String, String);

#[derive(Default)]
struct Tally {
    events: u64,
    /// Node → largest `*_total` field it reported (0 when none).
    nodes: BTreeMap<usize, u64>,
}

#[derive(Default)]
struct Tallies {
    kinds: BTreeMap<Kind, Tally>,
    /// Lines that did not parse as an [`Event`].
    unparsed: u64,
}

struct Inner {
    node: AtomicUsize,
    tallies: Mutex<Tallies>,
}

/// Handle shared by the driver (which names the polled node) and the
/// subscriber's writer.
#[derive(Clone)]
pub struct Warnings(Arc<Inner>);

impl Warnings {
    pub fn new() -> Self {
        Self(Arc::new(Inner {
            node: AtomicUsize::new(NO_NODE),
            tallies: Mutex::default(),
        }))
    }

    pub fn set_node(&self, node: usize) {
        self.0.node.store(node, Ordering::Relaxed);
    }

    /// The run-scoped subscriber: WARN and ERROR events, one JSON line each.
    pub fn subscriber(&self) -> impl Subscriber + Send + Sync + 'static {
        tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_max_level(Level::WARN)
            .with_writer(self.clone())
            .finish()
    }

    /// Tally one formatted event.
    fn tally(&self, line: &[u8]) {
        let event = serde_json::from_slice::<Event>(line);
        let node = self.0.node.load(Ordering::Relaxed);
        let mut tallies = self
            .0
            .tallies
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Ok(Event {
            level,
            target,
            fields: Fields { message, rest },
        }) = event
        else {
            tallies.unparsed += 1;
            return;
        };
        let total = rest
            .iter()
            .filter(|(name, _)| name.ends_with("_total"))
            .filter_map(|(_, v)| v.as_u64())
            .max()
            .unwrap_or(0);
        let tally = tallies.kinds.entry((level, target, message)).or_default();
        tally.events += 1;
        let max = tally.nodes.entry(node).or_default();
        *max = (*max).max(total);
    }

    pub fn report(&self) -> WarningReport {
        let tallies = self
            .0
            .tallies
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let kinds: Vec<WarningKind> = tallies
            .kinds
            .iter()
            .map(|((level, target, message), tally)| {
                let totals = tally.nodes.values().filter(|&&t| t > 0);
                WarningKind {
                    level: *level,
                    target: target.clone(),
                    message: message.clone(),
                    events: tally.events,
                    nodes: tally.nodes.keys().filter(|&&n| n != NO_NODE).count() as u64,
                    max_total: totals.clone().max().copied(),
                    sum_node_max_total: totals.sum(),
                }
            })
            .collect();
        let events = |level| {
            kinds
                .iter()
                .filter(|k| k.level == level)
                .map(|k| k.events)
                .sum()
        };
        WarningReport {
            warn_events: events(Severity::Warn),
            error_events: events(Severity::Error),
            unparsed_lines: tallies.unparsed,
            kinds,
        }
    }
}

impl<'a> MakeWriter<'a> for Warnings {
    type Writer = Line<'a>;

    fn make_writer(&'a self) -> Line<'a> {
        Line {
            warnings: self,
            bytes: Vec::new(),
        }
    }
}

/// One event's formatted line; tallied when the subscriber drops it.
pub struct Line<'a> {
    warnings: &'a Warnings,
    bytes: Vec<u8>,
}

impl io::Write for Line<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Line<'_> {
    fn drop(&mut self) {
        self.warnings.tally(&self.bytes);
    }
}

/// Summary of the captured events, part of the run's health.
#[derive(Debug, Default, Serialize)]
pub struct WarningReport {
    pub warn_events: u64,
    pub error_events: u64,
    /// Captured lines that did not read as a WARN or ERROR event with a
    /// target and a message, so they are in no kind below.
    pub unparsed_lines: u64,
    pub kinds: Vec<WarningKind>,
}

#[derive(Debug, Serialize)]
pub struct WarningKind {
    pub level: Severity,
    pub target: String,
    pub message: String,
    /// Events logged (gabion rate-limits most of these to power-of-two
    /// totals, so this is not the number of occurrences).
    pub events: u64,
    /// Polled nodes that logged it. Lines outside a poll ([`NO_NODE`]) count
    /// toward the totals below but not here.
    pub nodes: u64,
    /// Largest `*_total` field any node reported.
    pub max_total: Option<u64>,
    /// Per node the largest `*_total` field, summed over nodes.
    pub sum_node_max_total: u64,
}

#[cfg(test)]
mod tests;
