//! Test-only configs, built through [`Settings::validate`] as production
//! builds them.

use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};

use super::Settings;
use crate::crdt::ValidStorageConfig;

/// A cell store of the given capacities.
pub(crate) fn storage(
    cells: u32,
    rules: u16,
    nodes: u16,
    local_dirty: usize,
    forwarded_dirty: usize,
    peers: u16,
) -> ValidStorageConfig {
    let settings = Settings {
        max_cells: NonZeroU32::new(cells).expect("cells"),
        rule_dictionary_capacity: NonZeroU16::new(rules).expect("rules"),
        node_dictionary_capacity: NonZeroU16::new(nodes).expect("nodes"),
        local_dirty_capacity: NonZeroUsize::new(local_dirty).expect("local dirty"),
        forwarded_dirty_capacity: NonZeroUsize::new(forwarded_dirty).expect("forwarded dirty"),
        peer_capacity: NonZeroU16::new(peers).expect("peers"),
        ..Settings::default()
    };
    settings.validate().expect("test storage validates").1
}
