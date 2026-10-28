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
        ..Settings::default()
    };
    tune(&mut settings);
    settings.validate().expect("test settings validate")
}
