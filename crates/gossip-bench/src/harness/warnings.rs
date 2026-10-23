//! Captures the WARN and ERROR events gabion emits during a run.
//!
//! gabion reports saturation (dirty-ring overflow, full dictionaries, decode
//! failures) only through `tracing`, usually on power-of-two thresholds of a
//! running `*_total` counter. A run-scoped subscriber tallies every such
//! event per `(level, target, message)` and per node, keeping the largest
//! `*_total` field each node reported, so the result records saturation that
//! would otherwise vanish. The driver names the node it is polling; events
//! outside a node poll are attributed to [`NO_NODE`].

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use serde::Serialize;
use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber, span};

pub const NO_NODE: usize = usize::MAX;

/// Messages that report a dirty ring overwriting entries before gossip sent
/// them (`crates/gabion/src/crdt.rs`).
const DIRTY_OVERFLOW_MARKER: &str = "dirty ring overflowed";

type Kind = (&'static str, &'static str, String);

#[derive(Default)]
struct Tally {
    events: u64,
    /// Node → largest `*_total` field it reported (0 when none).
    nodes: BTreeMap<usize, u64>,
}

#[derive(Default)]
struct Inner {
    node: AtomicUsize,
    tallies: Mutex<BTreeMap<Kind, Tally>>,
}

/// Handle shared by the driver (which names the polled node) and the
/// subscriber.
#[derive(Clone, Default)]
pub struct Warnings(Arc<Inner>);

impl Warnings {
    pub fn new() -> Self {
        let warnings = Self::default();
        warnings.set_node(NO_NODE);
        warnings
    }

    pub fn set_node(&self, node: usize) {
        self.0.node.store(node, Ordering::Relaxed);
    }

    pub fn subscriber(&self) -> Capture {
        Capture(self.clone())
    }

    pub fn report(&self) -> WarningReport {
        let tallies = self
            .0
            .tallies
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let kinds: Vec<WarningKind> = tallies
            .iter()
            .map(|((level, target, message), tally)| {
                let totals = tally.nodes.values().filter(|&&t| t > 0);
                WarningKind {
                    level,
                    target,
                    message: message.clone(),
                    events: tally.events,
                    nodes: tally.nodes.keys().filter(|&&n| n != NO_NODE).count() as u64,
                    max_total: totals.clone().max().copied(),
                    sum_node_max_total: totals.sum(),
                }
            })
            .collect();
        let dirty = || {
            kinds
                .iter()
                .zip(tallies.values())
                .filter(|(k, _)| k.message.contains(DIRTY_OVERFLOW_MARKER))
        };
        let mut dirty_nodes: Vec<usize> =
            dirty().flat_map(|(_, t)| t.nodes.keys().copied()).collect();
        dirty_nodes.sort_unstable();
        dirty_nodes.dedup();
        WarningReport {
            warn_events: kinds
                .iter()
                .filter(|k| k.level == "WARN")
                .map(|k| k.events)
                .sum(),
            error_events: kinds
                .iter()
                .filter(|k| k.level == "ERROR")
                .map(|k| k.events)
                .sum(),
            dirty_overflow_nodes: dirty_nodes.len() as u64,
            dirty_overflow_lower_bound: dirty().map(|(k, _)| k.sum_node_max_total).sum(),
            kinds,
        }
    }
}

/// Summary of the captured events, part of the run's health.
#[derive(Debug, Default, Serialize)]
pub struct WarningReport {
    pub warn_events: u64,
    pub error_events: u64,
    /// Nodes that logged a dirty-ring overflow.
    pub dirty_overflow_nodes: u64,
    /// Sum over nodes and rings of the largest reported `overflow_total`:
    /// dirty entries overwritten before gossip sent them. A lower bound
    /// (within 2×), since gabion logs on power-of-two totals only.
    pub dirty_overflow_lower_bound: u64,
    pub kinds: Vec<WarningKind>,
}

#[derive(Debug, Serialize)]
pub struct WarningKind {
    pub level: &'static str,
    pub target: &'static str,
    pub message: String,
    /// Events logged (gabion rate-limits most of these to power-of-two
    /// totals, so this is not the number of occurrences).
    pub events: u64,
    pub nodes: u64,
    /// Largest `*_total` field any node reported.
    pub max_total: Option<u64>,
    /// Per node the largest `*_total` field, summed over nodes.
    pub sum_node_max_total: u64,
}

/// The run-scoped `tracing` subscriber. Interested in WARN and ERROR events
/// only; spans are disabled.
pub struct Capture(Warnings);

fn wanted(metadata: &Metadata<'_>) -> bool {
    metadata.is_event() && *metadata.level() <= Level::WARN
}

#[derive(Default)]
struct Fields {
    message: String,
    total: u64,
}

impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name().ends_with("_total") {
            self.total = self.total.max(value);
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_u64(field, value.max(0) as u64);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

impl Subscriber for Capture {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if wanted(metadata) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        wanted(metadata)
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::WARN)
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        let metadata = event.metadata();
        let mut fields = Fields::default();
        event.record(&mut fields);
        let level = if *metadata.level() == Level::ERROR {
            "ERROR"
        } else {
            "WARN"
        };
        let node = self.0.0.node.load(Ordering::Relaxed);
        let mut tallies = self
            .0
            .0
            .tallies
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let tally = tallies
            .entry((level, metadata.target(), fields.message))
            .or_default();
        tally.events += 1;
        let max = tally.nodes.entry(node).or_default();
        *max = (*max).max(fields.total);
    }

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

#[cfg(test)]
mod tests;
