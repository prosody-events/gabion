//! Build the gossip [`NodeIdentity`] at startup.
//!
//! `node_id` is a stable per-host identifier so `PeerFrontierTable` doesn't
//! burn a peer slot on every restart. `incarnation` bumps on every boot so
//! peers see a fresh per-origin sequence and don't silently prune our cells.

use std::net::UdpSocket;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use gabion::crdt::{NodeId, NodeIdentity};
use twox_hash::xxhash3_128;

#[cfg(test)]
mod tests;

/// Seed sources tried in order:
/// 1. Explicit `seed_override` (from config).
/// 2. OS hostname via [`whoami::hostname`].
/// 3. The local interface IP the kernel would use to reach the public internet
///    (no packet is actually sent — UDP `connect` is just an address
///    association).
/// 4. A last-resort random-ish seed from `SystemTime` × `process_id`; not
///    stable across restarts, but better than collapsing every restart into a
///    single bucketed identity.
///
/// `incarnation` is unix epoch seconds clamped to `u32`, with a minimum of 1.
/// Sub-second restarts reuse the same incarnation — peers catch up via the
/// repair lane.
pub fn derive_identity(seed_override: Option<&str>) -> NodeIdentity {
    let seed = seed_override
        .map(str::to_owned)
        .or_else(|| whoami::hostname().ok())
        .or_else(local_ip_seed)
        .unwrap_or_else(random_seed);
    let node_id = NodeId(xxhash3_128::Hasher::oneshot(seed.as_bytes()));

    let incarnation = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u32::try_from(d.as_secs()).unwrap_or(u32::MAX))
        .unwrap_or(1)
        .max(1);

    NodeIdentity::new(node_id, incarnation)
}

fn local_ip_seed() -> Option<String> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    // Associate the socket with a public-looking address. No packet is
    // sent; the kernel just picks the outgoing interface so `local_addr`
    // returns its bound IP. We try v4 first, then v6.
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
