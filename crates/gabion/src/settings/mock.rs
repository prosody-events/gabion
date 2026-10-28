//! Test-only configs, built through [`Settings::validate`] as production
//! builds them.

use super::Settings;
use crate::crdt::ValidStorageConfig;

/// A storage config of the given capacities; `dirty` sizes both dirty rings.
pub(crate) fn storage(
    cells: u32,
    rules: u16,
    nodes: u16,
    dirty: u32,
    peers: u16,
) -> ValidStorageConfig {
    let dirty = dirty.try_into().expect("dirty");
    let settings = Settings {
        max_cells: cells.try_into().expect("cells"),
        rule_dictionary_capacity: rules.try_into().expect("rules"),
        node_dictionary_capacity: nodes.try_into().expect("nodes"),
        local_dirty_capacity: dirty,
        forwarded_dirty_capacity: dirty,
        peer_capacity: peers.try_into().expect("peers"),
        ..Settings::default()
    };
    settings.validate().expect("test storage validates").1
}
