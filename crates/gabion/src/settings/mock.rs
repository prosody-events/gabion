//! Test-only configs, built through [`Settings::validate`] as production
//! builds them.

use std::num::NonZeroU16;

use super::Settings;
use crate::crdt::ValidStorageConfig;
use crate::rules::RuleTable;

/// A storage config of the given capacities; `dirty` sizes both dirty rings.
pub(crate) fn storage(cells: u32, nodes: u16, dirty: u32, peers: u16) -> ValidStorageConfig {
    let dirty = dirty.try_into().expect("dirty");
    let settings = Settings {
        max_cells: cells.try_into().expect("cells"),
        node_dictionary_capacity: nodes.try_into().expect("nodes"),
        local_dirty_capacity: dirty,
        forwarded_dirty_capacity: dirty,
        peer_capacity: peers.try_into().expect("peers"),
        ..Settings::default()
    };
    settings.validate().expect("test storage validates").1
}

/// No configured rules, in a rule dictionary of `capacity` slots.
pub(crate) fn rules(capacity: u16) -> RuleTable {
    RuleTable::new(Box::default(), NonZeroU16::new(capacity)).expect("an empty table fits")
}
