//! Simulator-driven evaluation harness for the gabion gossip protocol.
//!
//! Scenarios live as plain data (`Scenario`) and produce a structured
//! `ScenarioResult`. The CLI (`gossip-bench run`) loads a scenario from
//! JSON, executes it on top of `gabion::gossip::sim::SimRouter` with
//! virtual time, and emits the result as JSON for downstream
//! analysis/plotting.
//!
//! The harness deliberately mirrors the methodologies of Demers et al.
//! (Xerox PARC 1987), Karp et al. (FOCS 2000), Das/Gupta/Motivala (SWIM,
//! DSN 2002), Van Renesse et al. (Astrolabe, TOCS 2003), and Birman et
//! al. (Bimodal Multicast, TOCS 1999):
//!
//! * **Convergence rounds**: how many gossip ticks until all nodes have
//!   absorbed a write (Demers, Karp). Reported per fanout / cluster size.
//! * **Network cost** (Bimodal Multicast): bytes-per-node-per-second and
//!   packets-per-node-per-second at the steady state.
//! * **Loss tolerance** (Bimodal, Astrolabe): convergence under per-link drop
//!   probability.
//! * **Partition + heal** (SWIM): elapsed virtual time to re-converge after a
//!   network split is repaired.
//! * **Steady-state staleness** (Astrolabe): median observed lag for a
//!   sustained write workload.
//!
//! All metrics are emitted in a stable JSON schema so the Python plot
//! harness can ingest them without re-implementing parsing.

pub mod metrics;
pub mod scenario;
pub mod scenarios;
pub mod transport;

pub use metrics::{NodeMetrics, ScenarioResult, TickSnapshot};
pub use scenario::{LinkModel, NetworkModel, Scenario, ScenarioKind, Workload};
pub use transport::CountingTransport;
