use gabion::settings::Setting;

#[test]
fn setting_table_spellings_parse() {
    let module = include_str!("module.rs");
    for setting in Setting::ALL {
        let Some(directive) = setting.nginx_directive() else {
            continue;
        };
        assert!(
            module.contains(&format!("ngx_string!(\"{directive}\")")),
            "`{directive}` is in the settings table but module.rs has no such directive",
        );
    }
}
