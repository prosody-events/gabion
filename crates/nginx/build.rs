fn main() {
    println!("cargo::rerun-if-env-changed=DEP_NGINX_FEATURES_CHECK");
    println!(
        "cargo::rustc-check-cfg=cfg(ngx_feature, values({}))",
        std::env::var("DEP_NGINX_FEATURES_CHECK").unwrap_or_else(|_| "any()".to_string())
    );

    println!("cargo::rerun-if-env-changed=DEP_NGINX_FEATURES");
    if let Ok(features) = std::env::var("DEP_NGINX_FEATURES") {
        for feature in features.split(',').map(str::trim) {
            println!("cargo::rustc-cfg=ngx_feature=\"{feature}\"");
        }
    }

    println!("cargo::rerun-if-env-changed=DEP_NGINX_OS_CHECK");
    println!(
        "cargo::rustc-check-cfg=cfg(ngx_os, values({}))",
        std::env::var("DEP_NGINX_OS_CHECK").unwrap_or_else(|_| "any()".to_string())
    );

    println!("cargo::rerun-if-env-changed=DEP_NGINX_OS");
    if let Ok(os) = std::env::var("DEP_NGINX_OS") {
        println!("cargo::rustc-cfg=ngx_os=\"{os}\"");
    }

    const VERSION_CHECKS: &[(u64, &str)] = &[(1_025_001, "nginx1_25_1")];
    for (_, cfg_name) in VERSION_CHECKS {
        println!("cargo::rustc-check-cfg=cfg({cfg_name})");
    }

    println!("cargo::rerun-if-env-changed=DEP_NGINX_VERSION_NUMBER");
    if let Ok(version) = std::env::var("DEP_NGINX_VERSION_NUMBER")
        && let Ok(version) = version.parse::<u64>()
    {
        for (minimum, cfg_name) in VERSION_CHECKS {
            if version >= *minimum {
                println!("cargo::rustc-cfg={cfg_name}");
            }
        }
    }

    // `src/bin/nginx_layout.rs` compiles its C probe with the same include
    // path and defines nginx-sys ran bindgen with.
    for var in ["DEP_NGINX_INCLUDE", "DEP_NGINX_METADATA"] {
        println!("cargo::rerun-if-env-changed={var}");
        if let Ok(value) = std::env::var(var) {
            println!("cargo::rustc-env={var}={value}");
        }
    }

    if cfg!(target_os = "macos") {
        println!("cargo::rustc-link-arg=-undefined");
        println!("cargo::rustc-link-arg=dynamic_lookup");
    }
}
