//! SHM zone header: identity, aggregate write sequence.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Node identity fields stored cross-process. `node_id` is stamped by the
/// master on every config load, before fork; `incarnation` is updated
/// atomically by each leader takeover so peers see fresh `(node_id,
/// incarnation)` pairs after a flip.
///
/// `node_id` is logically a `u128` but stored as a paired
/// `(AtomicU64, AtomicU64)`: stable Rust has no 128-bit atomic.
/// `shm/queue.rs` and `gabion::aggregate` use the same paired-u64
/// convention for their own logical-u128 fields.
#[repr(C)]
#[derive(Debug, Default)]
pub struct NodeIdentityFields {
    pub node_id_lo: AtomicU64,
    pub node_id_hi: AtomicU64,
    pub incarnation: AtomicU32,
    _pad: u32,
}

impl NodeIdentityFields {
    pub fn store_node_id(&self, node_id: u128) {
        let lo = node_id as u64;
        let hi = (node_id >> 64) as u64;
        self.node_id_lo.store(lo, Ordering::Release);
        self.node_id_hi.store(hi, Ordering::Release);
    }

    pub fn load_node_id(&self) -> u128 {
        let lo = self.node_id_lo.load(Ordering::Acquire) as u128;
        let hi = self.node_id_hi.load(Ordering::Acquire) as u128;
        (hi << 64) | lo
    }

    pub fn store_incarnation(&self, incarnation: u32) {
        self.incarnation.store(incarnation, Ordering::Release);
    }

    pub fn load_incarnation(&self) -> u32 {
        self.incarnation.load(Ordering::Acquire)
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct Header {
    pub identity: NodeIdentityFields,
    /// The next sequence the aggregate writer stamps on a slot. Every leader
    /// continues it, so a reader holding a sequence from a slot that has
    /// since been freed or cleared cannot accept it again over another cell.
    pub aggregate_write_seq: AtomicU64,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            identity: NodeIdentityFields::default(),
            aggregate_write_seq: AtomicU64::new(2),
        }
    }
}
