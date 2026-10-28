use std::num::NonZeroU16;

use gabion::crdt::{CellStore, NodeId, NodeIdentity};
use gabion::defaults::{STORAGE_MAX_DESCRIPTOR_COUNT, STORAGE_MAX_KEY_BYTES};
use gabion::rules::RateResolveError::{
    WindowShorterThanPeriod, WindowTooLong, ZeroBucket, ZeroWindow,
};
use gabion::settings::Settings;

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

type Tune = fn(&mut Settings);

/// Every rule gabiond or NGINX refused on `main` is refused by the one
/// library constructor, with the error the operator reads.
#[test]
fn rule_def_rejects_what_both_adapters_rejected() {
    let max_count = usize::from(STORAGE_MAX_DESCRIPTOR_COUNT);
    let many: String = (0..=max_count).map(|i| format!(" k{i}:$v{i}")).collect();
    let many = format!("r{many} rate=10r/s");
    let long = "k".repeat(STORAGE_MAX_KEY_BYTES + 1);
    let long_key = format!("r {long}:$t rate=10r/s");
    let defaults: Tune = |_| {};
    let rule = || Box::<str>::from("r");
    let window = |source| RuleError::Window {
        rule: rule(),
        source,
    };
    let rows: [(&[&str], Tune, RuleError); 13] = [
        (
            &["r rate=10r/s"],
            defaults,
            RuleError::NoDescriptors { rule: rule() },
        ),
        (
            &["r 9x:$t rate=10r/s"],
            defaults,
            RuleError::InvalidKey {
                rule: rule(),
                key: "9x".into(),
            },
        ),
        (&["r $t rate=10r/s window=0s"], defaults, window(ZeroWindow)),
        (&["r $t rate=10r/s bucket=0s"], defaults, window(ZeroBucket)),
        (
            &["r $t rate=10r/s bucket=500us"],
            defaults,
            window(ZeroBucket),
        ),
        (
            &["r $t rate=10r/s window=60d"],
            defaults,
            window(WindowTooLong),
        ),
        (
            &["r $t rate=10r/m window=500ms"],
            defaults,
            window(WindowShorterThanPeriod),
        ),
        (
            &[&many],
            defaults,
            RuleError::TooManyDescriptors {
                rule: rule(),
                count: max_count + 1,
                max: max_count,
            },
        ),
        (
            &[&long_key],
            defaults,
            RuleError::KeyTooLong {
                rule: rule(),
                key: long.as_str().into(),
                bytes: STORAGE_MAX_KEY_BYTES + 1,
                max: STORAGE_MAX_KEY_BYTES,
            },
        ),
        (
            &["r $t rate=10r/s domain=a\u{7}b"],
            defaults,
            RuleError::InvalidDomain {
                rule: rule(),
                reason: "with whitespace or a control character",
            },
        ),
        (
            &["r $t rate=10r/s", "r $u rate=10r/s"],
            defaults,
            RuleError::DuplicateRuleName { name: rule() },
        ),
        (
            &["r $t rate=10r/s", "s $t rate=20r/s dry_run"],
            defaults,
            RuleError::DuplicateRuleIdentity {
                first: rule(),
                second: "s".into(),
            },
        ),
        (
            &["r $t rate=10r/s", "s $u rate=10r/s"],
            |s| s.rule_dictionary_capacity = NonZeroU16::new(1),
            RuleError::TooManyRules {
                rules: 2,
                capacity: 1,
            },
        ),
    ];
    for (directives, tune, expected) in rows {
        let configs: Vec<RuleConfig> = directives.iter().map(|text| directive(text)).collect();
        assert_eq!(
            CompiledRules::compile_with(&configs, &validated(tune).1, &mut NopBindingCompiler)
                .map(drop),
            Err(RuleConfigError::Rule(expected)),
            "{directives:?}"
        );
    }
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
    let store = CellStore::<u32>::new(
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
