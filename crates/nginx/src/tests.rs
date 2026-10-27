use std::collections::BTreeSet;

use gabion::settings::Setting;

/// `include_str!` because module.rs only builds with the `ngx-module` feature.
#[test]
fn settings_table_matches_module_directives() {
    const NON_SETTINGS: [&str; 4] = [
        "gabion",
        "gabion_limit",
        "gabion_limit_rule",
        "gabion_limit_zone",
    ];
    let module: BTreeSet<&str> = include_str!("module.rs")
        .split("name: ngx_string!(\"")
        .skip(1)
        .filter_map(|rest| rest.split_once('"').map(|(name, _)| name))
        .filter(|name| !NON_SETTINGS.contains(name))
        .collect();
    let table: BTreeSet<&str> = Setting::ALL
        .iter()
        .filter_map(|setting| setting.nginx_directive())
        .collect();
    assert_eq!(module, table);
}
