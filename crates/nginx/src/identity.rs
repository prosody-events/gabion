//! Build the gossip [`NodeIdentity`] at startup.
//!
//! Mirrors `crates/server/src/identity.rs` — the same seed chain (explicit
//! override → hostname → local IP → random fallback) so a node's identity
//! is stable across restarts.

use std::net::UdpSocket;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use gabion::crdt::{NodeId, NodeIdentity};
use twox_hash::xxhash3_128;

#[cfg(test)]
mod tests;

/// Derive a [`NodeIdentity`]. `seed_override` is used when the operator has
/// configured a stable identifier; otherwise we fall back to hostname → local
/// IP → random.
pub fn derive_identity(seed_override: Option<&str>) -> NodeIdentity {
    let seed = seed_override
        .map(str::to_owned)
        .or_else(|| whoami::hostname().ok())
        .or_else(local_ip_seed)
        .unwrap_or_else(random_seed);
    let node_id = NodeId(xxhash3_128::Hasher::oneshot(seed.as_bytes()));
    NodeIdentity::new(node_id, fresh_incarnation())
}

/// Stamp a new wall-clock-seconds incarnation. Called on leader takeover so
/// peers see fresh `(node_id, incarnation)` for the new leader. Sub-second
/// takeovers reuse the same incarnation — same caveat as the server.
pub fn fresh_incarnation() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u32::try_from(d.as_secs()).unwrap_or(u32::MAX))
        .unwrap_or(1)
        .max(1)
}

fn local_ip_seed() -> Option<String> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    if socket.connect("1.1.1.1:80").is_err() && socket.connect("[2606:4700:4700::1111]:80").is_err()
    {
        return None;
    }
    socket.local_addr().ok().map(|sa| sa.ip().to_string())
}

pub(crate) fn random_seed() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("gabion-{:x}-{:x}", nanos, process::id())
}
