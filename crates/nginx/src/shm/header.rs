//! SHM zone header: magic, version, identity, geometry summary, rule digest.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

pub const SHM_MAGIC: u32 = 0x4742_4e58; // "GBNX"
pub const SHM_VERSION: u32 = 2;

/// Node identity fields stored cross-process. `node_id` is stamped once by
/// the master process before fork; `incarnation` is updated atomically by
/// each leader takeover so peers see fresh `(node_id, incarnation)` pairs
/// after a flip.
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
    pub magic: AtomicU32,
    pub version: AtomicU32,
    pub identity: NodeIdentityFields,
    pub zone_bytes: AtomicU64,
    pub queue_capacity: AtomicU64,
    pub aggregate_capacity: AtomicU64,
    pub rule_table_digest: AtomicU64,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            magic: AtomicU32::new(SHM_MAGIC),
            version: AtomicU32::new(SHM_VERSION),
            identity: NodeIdentityFields::default(),
            zone_bytes: AtomicU64::new(0),
            queue_capacity: AtomicU64::new(0),
            aggregate_capacity: AtomicU64::new(0),
            rule_table_digest: AtomicU64::new(0),
        }
    }
}

impl Header {
    pub fn is_initialized(&self) -> bool {
        self.magic.load(Ordering::Acquire) == SHM_MAGIC
            && self.version.load(Ordering::Acquire) == SHM_VERSION
    }
}

#[cfg(test)]
mod tests;
