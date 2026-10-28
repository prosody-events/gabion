//! This process's gossip identity: a node id stable across restarts of one
//! host, and an incarnation that differs on every start, so peers tell a
//! restart from a new node and acks written for the old process credit
//! nothing in the new one.

use std::net::UdpSocket;
use std::num::NonZeroU32;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use twox_hash::xxhash3_128;

use crate::crdt::{Incarnation, NodeId};

#[cfg(test)]
mod tests;

/// This host's node id: the hash of `seed_override` (from config), else the
/// hostname, else the IP of the interface the kernel would use to reach the
/// internet (no packet is sent), else a per-process seed of time and process
/// id, which is not stable across restarts but keeps restarts apart.
pub fn node_id(seed_override: Option<&str>) -> NodeId {
    let seed = seed_override
        .map(str::to_owned)
        .or_else(|| whoami::hostname().ok())
        .or_else(local_ip_seed)
        .unwrap_or_else(random_seed);
    NodeId(xxhash3_128::Hasher::oneshot(seed.as_bytes()))
}

/// A new incarnation for a process starting now: the low 32 bits of
/// wall-clock milliseconds, never 0, and stepped past `previous` (the NGINX
/// leader knows the last leader's) when it would repeat it.
pub fn incarnation(previous: Option<Incarnation>) -> Incarnation {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64);
    incarnation_at(now, previous)
}

/// [`incarnation`] at `now_millis`.
pub(crate) fn incarnation_at(now_millis: u64, previous: Option<Incarnation>) -> Incarnation {
    let step = |x: u32| if x == u32::MAX { 1 } else { x + 1 };
    let now = NonZeroU32::new(now_millis as u32).map_or(1, NonZeroU32::get);
    if previous == Some(now) {
        step(now)
    } else {
        now
    }
}

fn local_ip_seed() -> Option<String> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    if socket.connect("1.1.1.1:80").is_err() && socket.connect("[2606:4700:4700::1111]:80").is_err()
    {
        return None;
    }
    socket.local_addr().ok().map(|sa| sa.ip().to_string())
}

fn random_seed() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("gabion-{:x}-{:x}", nanos, process::id())
}
