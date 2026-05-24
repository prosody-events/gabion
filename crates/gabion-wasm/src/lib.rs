//! Visualizer bridge: the real gabion gossip + CRDT core, compiled to
//! WebAssembly and driven from a TypeScript frontend.
//!
//! This crate adds **no new simulator**. [`engine::run_engine`] stands up N
//! [`gabion::gossip::GossipRuntime`]s on one shared
//! [`gabion::gossip::sim::SimRouter`] under paused virtual time — the exact
//! machinery `gossip-bench` already exercises — and exposes it over a command
//! channel so the page can inject requests, partition links, advance time, and
//! pull a per-node snapshot. Two thin observation shims
//! ([`shims::EventEmittingAggregateStore`], [`shims::LoggingSimTransport`])
//! turn the runtime's existing push surfaces into a typed [`event::Event`]
//! log.
//!
//! Nothing here is on a production hot path, so the library's
//! allocation/copy/no-panic-in-prod constraints don't bind this crate — but it
//! follows the repo's error and elegance norms.
//!
//! The `#[wasm_bindgen]` surface lives in the `wasm` module and is compiled
//! only for `wasm32`. Everything else is target-agnostic and tested natively
//! with `cargo nextest`, so the engine and shims are validated without any
//! wasm toolchain.

pub mod clock;
pub mod config;
pub mod engine;
pub mod event;
mod hex;
pub mod shims;

#[cfg(target_arch = "wasm32")]
mod wasm;
