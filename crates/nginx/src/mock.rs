//! Test-only settings, built through `Settings::validate` as the module
//! builds them.

use gabion::crdt::ValidStorageConfig;
use gabion::gossip::ValidGossipConfig;
use gabion::settings::Settings;

/// A small node's settings, then `tune`.
pub(crate) fn validated(
    tune: impl FnOnce(&mut Settings),
) -> (ValidGossipConfig, ValidStorageConfig) {
    let mut settings = Settings {
        max_cells: 256.try_into().unwrap(),
        node_dictionary_capacity: 32.try_into().unwrap(),
        local_dirty_capacity: 64.try_into().unwrap(),
        forwarded_dirty_capacity: 64.try_into().unwrap(),
        peer_capacity: 16.try_into().unwrap(),
        ..Settings::default()
    };
    tune(&mut settings);
    settings.validate().expect("test settings validate")
}
