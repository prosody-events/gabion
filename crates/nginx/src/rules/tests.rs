use super::*;

fn binding(key: &str, var: &str) -> DescriptorBinding {
    DescriptorBinding {
        key: key.to_string(),
        source: format!("${var}"),
    }
}

fn cfg(name: &str, bindings: Vec<DescriptorBinding>) -> RuleConfig {
    RuleConfig {
        name: name.to_string(),
        domain: DEFAULT_DOMAIN.to_string(),
        bindings,
        limit: 10,
        window: Duration::from_secs(60),
        bucket: Duration::from_secs(1),
        mode: EnforcementMode::Enforce,
        except_if: None,
    }
}

#[test]
fn compiles_simple_rules() {
    let rules = CompiledRules::compile(&[
        cfg("per_tenant", vec![binding("tenant", "http_x_tenant")]),
        cfg("per_uri", vec![binding("uri", "uri")]),
    ])
    .expect("compile");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules.rules()[0].rule.spec().limit, 10);
    assert_eq!(rules.rules()[0].rule.spec().live_buckets, 60);
    let table = rules.table();
    assert_eq!(table.len(), 2);
}

#[test]
fn empty_bindings_reject() {
    let err = CompiledRules::compile(&[cfg("bad", vec![])]).unwrap_err();
    assert!(matches!(err, RuleConfigError::EmptyBindings(_)));
}

#[test]
fn empty_set_rejects() {
    let err = CompiledRules::compile(&[]).unwrap_err();
    assert_eq!(err, RuleConfigError::Empty);
}

#[test]
fn key_too_long_at_compile() {
    let long_key = "k".repeat(200);
    let cardinality = CardinalitySettings::default();
    let err = CompiledRules::compile_with_cardinality(
        &[cfg("long", vec![binding(&long_key, "http_x")])],
        cardinality,
    )
    .unwrap_err();
    assert!(matches!(err, RuleConfigError::KeyTooLong { .. }));
}

#[test]
fn too_many_bindings_at_compile() {
    let bindings = (0..MAX_DESCRIPTORS + 1)
        .map(|i| binding(&format!("k{i}"), &format!("v{i}")))
        .collect();
    let err = CompiledRules::compile(&[cfg("wide", bindings)]).unwrap_err();
    assert!(matches!(err, RuleConfigError::TooManyBindings(_)));
}
