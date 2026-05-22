//! NGINX request-path adapter built on `gabion::{rules, crdt, gossip,
//! discovery}` with all cross-process state in `mmap`'d shared memory.
//!
//! Two execution contexts share one SHM zone:
//!   - Every nginx worker handles requests on its event loop. The access phase
//!     reads the SHM aggregate table (no syscalls, no allocation), decides
//!     allow/reject, and pushes a record into the SHM queue.
//!   - One elected worker spawns a dedicated OS thread that owns a
//!     `current_thread` tokio runtime + `LocalSet`. The thread drains the SHM
//!     queue, drives the `GossipRuntime`, and writes deltas back into the SHM
//!     aggregate table via `ShmAggregateStore`.
//!
//! The library half (this file plus the `access`, `headers`, `identity`,
//! `leader`, `rules`, `shm` modules) builds and tests without the
//! `ngx-module` feature. The `module` submodule pulls in `ngx` for the FFI
//! glue when that feature is on.

pub mod access;
pub mod headers;
pub mod identity;
pub mod leader;
pub mod rules;
pub mod shm;

#[cfg(feature = "ngx-module")]
mod module;
