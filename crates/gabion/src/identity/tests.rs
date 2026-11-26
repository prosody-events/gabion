use std::process;

use super::*;

#[test]
fn explicit_override_is_stable_and_pins_hash() {
    // Pinning the xxhash output guards against an upstream algorithm
    // change silently re-bucketing every peer's identity.
    assert_eq!(
        node_id(Some("test-node")).0,
        0xa412461da00c7f583da3d8ea39d95ad2
    );
}

#[test]
fn override_seeds_differ_when_seeds_differ() {
    assert_ne!(node_id(Some("node-a")), node_id(Some("node-b")));
}

#[test]
#[cfg_attr(
    miri,
    ignore = "whoami::hostname() calls libc::gethostname; miri has no shim for that foreign \
              function"
)]
fn falls_back_to_hostname_consistently() {
    // The chain falls through hostname (or IP, or random). The value is
    // machine-dependent, but two calls must walk the same chain to the same
    // id; the random fallback would break this.
    assert_eq!(node_id(None), node_id(None));
}

#[test]
fn random_seed_includes_pid_and_nanos() {
    let seed = random_seed();
    let suffix = format!("-{:x}", process::id());
    assert!(seed.starts_with("gabion-"), "{seed:?}");
    assert!(seed.ends_with(&suffix), "{seed:?}");
    assert_eq!(seed.matches('-').count(), 2, "{seed:?}");
}

/// Two starts one millisecond apart get different incarnations, a
/// start that would repeat the previous one steps past it, and 0 never
/// appears, even where the step wraps.
#[test]
fn incarnation_differs_on_every_start() {
    let t = 1_700_000_000_000;
    assert_ne!(incarnation_at(t, None), incarnation_at(t + 1, None));
    let first = incarnation_at(t, None);
    assert_eq!(incarnation_at(t, Some(first)), first + 1);
    assert_eq!(incarnation_at(t, Some(first + 7)), first);
    assert_eq!(incarnation_at(1 << 32, None), 1);
    assert_eq!(incarnation_at(u64::from(u32::MAX), Some(u32::MAX)), 1);
    assert_eq!(incarnation_at(1 << 32, Some(1)), 2);
}
