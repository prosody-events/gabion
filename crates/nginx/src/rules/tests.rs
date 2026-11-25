use gabion::crdt::{CellStore, NodeId, NodeIdentity};

use super::mock::{compile, directive, rules};
use super::*;
use crate::mock::validated;

#[test]
fn compiles_simple_rules() {
    let rules = rules(&[
        "per_tenant tenant:$http_x_tenant rate=10r/m bucket=1s",
        "per_uri $uri rate=10r/m bucket=1s",
    ]);
    assert_eq!(rules.table().rules().len(), 2);
    let (rule, compiled) = rules.get(0).expect("rule 0");
    assert_eq!(rule.spec().limit, 10);
    assert_eq!(rule.spec().live_buckets.get(), 60);
    assert_eq!(compiled.bindings.len(), 1);
}

/// The configured `gabion_storage_max_key_bytes` is the only key-length
/// bound: a key over the default parses and compiles once the zone raises it.
#[test]
#[cfg_attr(
    all(miri, target_arch = "aarch64"),
    ignore = "hashing a key this long takes twox-hash's NEON path, whose inline assembly Miri \
              cannot run; the x86_64 Miri job covers it"
)]
fn raised_max_key_bytes_admits_long_key() {
    let long_key = "k".repeat(200);
    let config = directive(&format!("long ${long_key} {long_key}x:$http_x rate=10r/s"));
    let (_, storage) = validated(|s| s.max_key_bytes = 256.try_into().unwrap());
    let rules = CompiledRules::compile_with(&[config], &storage, &mut NopBindingCompiler)
        .expect("a 200-byte key fits a 256-byte budget");
    assert_eq!(rules.table().rules()[0].descriptors()[0].key.len(), 200);
}

/// The directive's argument errors, reported at its line.
#[test]
fn parse_rule_directive_reports_argument_errors() {
    let parse = |text: &str| {
        let args: Vec<&str> = text.split_whitespace().collect();
        parse_rule_directive(&args)
    };
    assert_eq!(parse(""), Err(RuleArgError::MissingName));
    assert_eq!(
        parse("r $uri"),
        Err(RuleArgError::MissingRate { rule: "r".into() })
    );
    for text in [
        "r $uri rate=10r/s mode=enforce dry_run",
        "r $uri rate=10r/s dry_run mode=enforce",
    ] {
        assert_eq!(
            parse(text),
            Err(RuleArgError::DryRunConflict { rule: "r".into() }),
            "{text}"
        );
    }
    for arg in [
        "rate=0r/s",
        "rate=10r/fortnight",
        "window=soon",
        "bucket=soon",
        "mode=loud",
        "except_if=$bad-ident",
        "value=:acme",
        "value=tenant",
        "tenant:",
    ] {
        assert!(
            matches!(parse(&format!("r tenant:$t rate=10r/s {arg}")), Err(RuleArgError::Invalid { arg: a, .. }) if &*a == arg),
            "{arg}"
        );
    }
}

#[test]
fn parse_binding_auto_keyed_single_variable() {
    let b = parse_binding("$remote_addr").unwrap();
    assert_eq!(b.key.as_ref(), "remote_addr");
    assert_eq!(b.source.as_ref(), "$remote_addr");
    assert_eq!(b.value.as_ref(), "*");
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
fn parse_binding_rejects_empty_source() {
    assert!(parse_binding("tenant:").is_err());
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

/// The README's example rules load together, and each keeps its own window
/// and bucket in the rule dictionary, so each expires on its own schedule.
#[test]
fn nginx_readme_rules_each_expire_on_their_own_schedule() {
    let readme = include_str!("../../README.md");
    let section = &readme[readme
        .find("#### `gabion_limit_rule ")
        .expect("the README documents `gabion_limit_rule`")..];
    let start = section.find("```nginx\n").expect("an nginx example") + "```nginx\n".len();
    let block = &section[start..start + section[start..].find("```").expect("closed")];
    let configs: Vec<RuleConfig> = block
        .lines()
        .filter_map(|line| line.trim().strip_prefix("gabion_limit_rule"))
        .map(|line| directive(line.trim_end_matches(';')))
        .collect();
    let rules = compile(&configs).expect("no two README rules share an identity");
    let store = CellStore::new(
        validated(|_| {}).1,
        NodeIdentity::new(NodeId(1), 1),
        rules.table(),
    );
    let table = rules.table().rules();
    for rule in table {
        let slot = store
            .rule_dictionary()
            .find(rule.fingerprint())
            .expect("interned");
        let interned = store.rule_dictionary().descriptor(slot).expect("pinned");
        assert_eq!(
            (
                u64::from(interned.window_millis),
                u64::from(interned.bucket_millis)
            ),
            (rule.spec().window_millis, rule.spec().bucket_millis.get()),
            "{}",
            rule.name()
        );
    }
    assert!(
        table
            .iter()
            .enumerate()
            .any(|(i, a)| table[..i].iter().any(|b| {
                a.descriptors() == b.descriptors()
                    && a.spec().window_millis == b.spec().window_millis
                    && a.spec().bucket_millis != b.spec().bucket_millis
            })),
        "the README keeps a pair of rules that differ only in bucket"
    );
}
