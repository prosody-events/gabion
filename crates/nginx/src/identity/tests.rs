//! Tests for the parent [`crate::identity`] module.

use std::process;

use crate::identity::{derive_identity, fresh_incarnation, random_seed};

#[test]
fn explicit_override_is_stable_and_pins_hash() {
    // Pinning the xxhash output guards against an upstream algorithm
    // change silently re-bucketing every peer's identity.
    let a = derive_identity(Some("test-node"));
    let b = derive_identity(Some("test-node"));
    assert_eq!(a.node_id, b.node_id);
    assert_eq!(a.node_id.0, 0xa412461da00c7f583da3d8ea39d95ad2);
}

#[test]
fn override_seeds_differ_when_seeds_differ() {
    let a = derive_identity(Some("node-a"));
    let b = derive_identity(Some("node-b"));
    assert_ne!(a.node_id, b.node_id);
}

#[test]
fn falls_back_to_hostname_consistently() {
    // No explicit override — the chain falls through hostname (or IP,
    // or random). The exact value is machine-dependent, but two
    // back-to-back calls must walk the same chain and produce the
    // same node_id. Random fallback would break this.
    let a = derive_identity(None);
    let b = derive_identity(None);
    assert_eq!(a.node_id, b.node_id);
}

#[test]
fn fresh_incarnation_is_unix_seconds_with_floor() {
    let inc = fresh_incarnation();
    assert!(inc >= 1, "incarnation must be at least 1");
    // Sanity floor: any year >= 2023.
    assert!(inc > 1_700_000_000, "incarnation looks too small: {inc}");
}

#[test]
fn random_seed_includes_pid_and_nanos() {
    let seed = random_seed();
    let suffix = format!("-{:x}", process::id());
    assert!(
        seed.starts_with("gabion-"),
        "random seed must be namespaced: {seed:?}"
    );
    assert!(
        seed.ends_with(&suffix),
        "random seed must end with pid {suffix}: {seed:?}"
    );
    // gabion-<nanos>-<pid> → exactly two '-' beyond the prefix.
    assert_eq!(seed.matches('-').count(), 2, "unexpected shape: {seed:?}");
}
