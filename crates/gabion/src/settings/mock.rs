//! Test-only configs, built through [`Settings::validate`] as production
//! builds them.

use std::num::NonZeroU16;

use super::Settings;
use crate::crdt::ValidStorageConfig;
use crate::rules::RuleTable;

/// A storage config of the given capacities. The storage half ignores the
/// peer table, and the smallest one lets any dictionary of 2 or more validate.
pub(crate) fn storage(cells: u32, nodes: u16) -> ValidStorageConfig {
    let settings = Settings {
        max_cells: cells.try_into().expect("cells"),
        node_dictionary_capacity: Some(nodes.try_into().expect("nodes")),
        peer_capacity: NonZeroU16::MIN,
        ..Settings::default()
    };
    settings.validate().expect("test storage validates").1
}

/// No configured rules, in a rule dictionary of `capacity` slots.
pub(crate) fn rules(capacity: u16) -> RuleTable {
    RuleTable::new(Box::default(), NonZeroU16::new(capacity)).expect("an empty table fits")
}
