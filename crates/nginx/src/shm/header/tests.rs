//! Tests for the parent [`crate::shm::header`] module.

use std::sync::atomic::Ordering;

use crate::shm::header::{Header, SHM_MAGIC, SHM_VERSION};

fn header_with(magic: u32, version: u32) -> Header {
    let h = Header::default();
    h.magic.store(magic, Ordering::Release);
    h.version.store(version, Ordering::Release);
    h
}

#[test]
fn initialized_when_magic_and_version_match() {
    let h = Header::default();
    assert!(h.is_initialized());
}

#[test]
fn not_initialized_when_version_is_zero() {
    // Partial init: magic stamped, version still default. The
    // post-fork worker must wait, not crash on stale geometry.
    let h = header_with(SHM_MAGIC, 0);
    assert!(!h.is_initialized());
}

#[test]
fn not_initialized_when_magic_is_wrong() {
    let h = header_with(0xDEAD_BEEF, SHM_VERSION);
    assert!(!h.is_initialized());
}

#[test]
fn not_initialized_when_zeroed() {
    // Fresh zero-mapped page — both fields default to 0. Must not
    // pretend the zone is ready.
    let h = header_with(0, 0);
    assert!(!h.is_initialized());
}

#[test]
fn not_initialized_for_future_version() {
    // A live reader against a writer of a newer schema must refuse
    // rather than read incompatible geometry.
    let h = header_with(SHM_MAGIC, SHM_VERSION + 1);
    assert!(!h.is_initialized());
}
