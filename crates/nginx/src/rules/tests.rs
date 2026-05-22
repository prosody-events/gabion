use super::*;

fn binding(key: &str, var: &str) -> DescriptorBinding {
    DescriptorBinding {
        key: key.into(),
        source: format!("${var}").into_boxed_str(),
    }
}

fn cfg(name: &str, bindings: Vec<DescriptorBinding>) -> RuleConfig {
    RuleConfig {
        name: name.into(),
        domain: DEFAULT_DOMAIN.into(),
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

#[test]
fn zero_limit_is_rejected_at_compile() {
    let mut rule = cfg("zero", vec![binding("uri", "uri")]);
    rule.limit = 0;
    let err = CompiledRules::compile(&[rule]).unwrap_err();
    assert!(matches!(err, RuleConfigError::ZeroLimit(_)));
}

#[test]
fn parse_binding_auto_keyed_single_variable() {
    let b = parse_binding("$remote_addr").unwrap();
    assert_eq!(b.key.as_ref(), "remote_addr");
    assert_eq!(b.source.as_ref(), "$remote_addr");
}

#[test]
fn parse_binding_explicit_key() {
    let b = parse_binding("tenant:$arg_tenant").unwrap();
    assert_eq!(b.key.as_ref(), "tenant");
    assert_eq!(b.source.as_ref(), "$arg_tenant");
}

#[test]
fn parse_binding_template() {
    let b = parse_binding("combo:prefix-$asn-$ua").unwrap();
    assert_eq!(b.key.as_ref(), "combo");
    assert_eq!(b.source.as_ref(), "prefix-$asn-$ua");
}

#[test]
fn parse_binding_rejects_invalid_key_characters() {
    assert!(parse_binding("bad key:$var").is_err());
    assert!(parse_binding("bad/key:$var").is_err());
    assert!(parse_binding("9leading:$var").is_err());
}

#[test]
fn parse_binding_accepts_kebab_and_dotted_keys() {
    assert!(parse_binding("tenant-id:$var").is_ok());
    assert!(parse_binding("app.tenant:$var").is_ok());
}

#[test]
fn parse_binding_rejects_empty_source() {
    assert!(parse_binding("tenant:").is_err());
}

#[test]
fn is_dns_label_accepts_valid_names() {
    assert!(is_dns_label("default"));
    assert!(is_dns_label("gabion-mixed-1234"));
    assert!(is_dns_label("a"));
    assert!(is_dns_label("a1"));
}

#[test]
fn is_dns_label_rejects_invalid_names() {
    assert!(!is_dns_label(""));
    assert!(!is_dns_label("-leading-dash"));
    assert!(!is_dns_label("trailing-dash-"));
    assert!(!is_dns_label("UPPER"));
    assert!(!is_dns_label("under_score"));
    assert!(!is_dns_label(&"a".repeat(64)));
}

#[test]
fn is_zone_name_matches_nginx_grammar() {
    assert!(is_zone_name("api"));
    assert!(is_zone_name("api_v2"));
    assert!(is_zone_name("123"));
    assert!(!is_zone_name(""));
    assert!(!is_zone_name("api.v2"));
    assert!(!is_zone_name("api-v2"));
}

#[test]
fn is_descriptor_key_matches_grammar() {
    assert!(is_descriptor_key("tenant"));
    assert!(is_descriptor_key("tenant_id"));
    assert!(is_descriptor_key("tenant.id"));
    assert!(is_descriptor_key("tenant-id"));
    assert!(is_descriptor_key("_internal"));
    assert!(!is_descriptor_key(""));
    assert!(!is_descriptor_key("9leading"));
    assert!(!is_descriptor_key(".dot-first"));
    assert!(!is_descriptor_key("with space"));
}
