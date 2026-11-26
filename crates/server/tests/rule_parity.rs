//! A rule written for NGINX and the same rule written for `gabiond` are one
//! rule. Each adapter loads its rules through its production path, the two
//! configs listing them in opposite orders. A hit NGINX queues, folded into
//! the cluster aggregate, is one `gabiond` counts for the same request
//! exactly when the pair also agrees on its bucket, so each adapter's own key
//! hash is on the line.

use std::num::NonZeroU16;
use std::sync::Arc;

use gabion::aggregate::Aggregate;
use gabion::crdt::{CellIdentity, CellStore, DeltaSink, ExpirationSink, NodeId, NodeIdentity};
use gabion::defaults::{STORAGE_MAX_DESCRIPTOR_COUNT, STORAGE_MAX_KEY_BYTES};
use gabion::gossip::sim::SimRouter;
use gabion::gossip::{AggregateStore, FixedClock, GossipRuntime};
use gabion::rules::RateResolveError::{
    WindowShorterThanPeriod, WindowTooLong, ZeroBucket, ZeroWindow,
};
use gabion::rules::{RuleError, parse_rate};
use gabion::settings::Settings;
use gabion_nginx::access::{AccessCtx, AccessOutcome, VariableLookup, decide_all};
use gabion_nginx::rules::{
    BindingLookup, CompiledRules, NopBindingCompiler, RuleConfig, RuleConfigError,
    parse_rule_directive,
};
use gabion_nginx::shm::queue::QueueEvent;
use gabion_nginx::shm::{QueueControl, QueueSlot, RequestQueue, Stats};
use gabion_server::admission::Decision;
use gabion_server::config::{AppConfig, ConfigError};
use gabion_server::{RateLimitDescriptor, SharedLimiter, rate_limit_descriptor};

/// One rule in NGINX's spelling, a request it counts, and whether its
/// `gabiond` twin shares its counter.
type Case<'a> = (&'a str, &'a [(&'a str, &'a str)], bool);

/// `coarse` differs from its `gabiond` twin only in its bucket.
const CASES: [Case; 7] = [
    ("wild $uri rate=10r/s", &[("uri", "/a")], true),
    (
        "acme_only tenant:$tenant rate=10r/s value=tenant:acme",
        &[("tenant", "acme")],
        true,
    ),
    (
        "api_tenant $uri tenant:$tenant rate=100r/10s bucket=1s",
        &[("uri", "/a"), ("tenant", "t1")],
        true,
    ),
    (
        "coarse session:$session rate=10r/10s",
        &[("session", "s1")],
        false,
    ),
    (
        "api tenant:$tenant rate=10r/s domain=api",
        &[("tenant", "t1")],
        true,
    ),
    ("per_minute user:$user rate=60r/m", &[("user", "u1")], true),
    (
        "shadow ip:$ip rate=10r/s dry_run",
        &[("ip", "10.0.0.1")],
        true,
    ),
];

/// The same rules in `gabiond`'s spelling, in the opposite order.
/// `per_minute` leaves its window and bucket to the defaults in both.
const GABIOND: &str = r#"
limits:
  - {name: shadow, domain: nginx, descriptors: [{key: ip}], rate: 10r/s, mode: dry_run}
  - {name: per_minute, domain: nginx, descriptors: [{key: user}], rate: 60r/m}
  - {name: api, domain: api, descriptors: [{key: tenant}], rate: 10r/s}
  - {name: coarse, domain: nginx, descriptors: [{key: session}], rate: 10r/10s, bucket: 1s}
  - {name: api_tenant, domain: nginx, descriptors: [{key: uri, value: "*"}, {key: tenant}], rate: 100r/10s, bucket: 1s}
  - {name: acme_only, domain: nginx, descriptors: [{key: tenant, value: acme}], rate: 10r/s}
  - {name: wild, domain: nginx, descriptors: [{key: uri}], rate: 10r/s}
"#;

const NOW: u64 = 1_700_000_000_000;

/// A request's NGINX variables: each binding reads the variable its key
/// names.
struct Vars<'a>(&'a [(&'a str, &'a str)]);

impl VariableLookup for Vars<'_> {
    fn lookup(&self, binding: &BindingLookup) -> Option<&[u8]> {
        let name = match binding {
            BindingLookup::Uri => "uri",
            BindingLookup::IndexedVariable { name, .. } => name,
            _ => return None,
        };
        self.0
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.as_bytes())
    }
}

/// Fold one hit NGINX queued into the cluster aggregate, as its leader does.
fn fold(counts: &Aggregate, hit: QueueEvent) {
    let deltas = DeltaSink {
        keys: vec![CellIdentity {
            rule_fingerprint: hit.rule_fingerprint,
            key_hash: hit.key_hash,
            bucket: hit.bucket,
        }],
        previous: vec![0],
        current: vec![1],
        deltas: vec![1],
        applies_locally: vec![1],
    };
    counts.apply(&deltas, &ExpirationSink::default());
}

#[test]
fn equal_rules_have_equal_identity_in_both_adapters() {
    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("rule_parity.yaml");
    std::fs::write(&path, GABIOND).expect("write the gabiond config");
    let config = AppConfig::load(Some(&path)).expect("gabiond loads its rules");
    let storage = config.storage;
    let directives: Vec<RuleConfig> = CASES
        .iter()
        .map(|(text, ..)| {
            parse_rule_directive(&text.split_whitespace().collect::<Vec<_>>()).expect(text)
        })
        .collect();
    let nginx = CompiledRules::compile_with(&directives, &storage, &mut NopBindingCompiler)
        .expect("NGINX compiles its rules");

    // One aggregate for the cluster: NGINX admits against it, its hits are
    // folded into it, and gabiond admits against it.
    let counts = Arc::new(Aggregate::new(&storage));
    let gabiond = Arc::new(config.rules);
    // Built and never run: gabiond's admission only queues its own hits.
    let (_runtime, client) = GossipRuntime::from_parts(
        SimRouter::new().bind("127.0.0.1:7000".parse().expect("a socket address")),
        FixedClock::default(),
        config.gossip,
        CellStore::new(storage, NodeIdentity::new(NodeId(1), 1), &gabiond),
        counts.clone(),
    );
    let limiter = SharedLimiter::new(gabiond.clone(), client, counts.clone(), storage);
    let (control, slots) = (
        QueueControl::default(),
        [QueueSlot::empty(0), QueueSlot::empty(1)],
    );
    let queue = RequestQueue::from_parts(&control, &slots);
    let stats = Stats::default();
    let ctx = AccessCtx {
        rules: &nginx,
        aggregate: counts.table(),
        queue,
        stats: &stats,
        cardinality: storage.cardinality,
        max_matched_rules: storage.max_matched_rules(),
    };
    let mut events = Vec::new();

    for (i, &(_, request, shares)) in CASES.iter().enumerate() {
        let ours = &nginx.table().rules()[i];
        let theirs = &gabiond.rules()[CASES.len() - 1 - i];
        let name = ours.name();
        assert_eq!(
            theirs.name(),
            name,
            "gabiond lists the rules in the opposite order"
        );
        assert_eq!(
            (
                ours.domain(),
                ours.descriptors(),
                ours.mode(),
                ours.spec().limit,
                ours.spec().window_millis
            ),
            (
                theirs.domain(),
                theirs.descriptors(),
                theirs.mode(),
                theirs.spec().limit,
                theirs.spec().window_millis
            ),
            "{name}"
        );
        assert_eq!(
            (
                ours.fingerprint() == theirs.fingerprint(),
                ours.spec().bucket_millis == theirs.spec().bucket_millis,
            ),
            (shares, shares),
            "{name}"
        );

        let outcome = decide_all(ctx, &[i], &Vars(request), NOW, &mut events);
        assert!(
            matches!(outcome, AccessOutcome::Allow(Some(_))),
            "{name}: {outcome:?}"
        );
        let mut hit = [QueueEvent::default()];
        assert_eq!(queue.drain(&mut hit), 1, "{name}");
        fold(&counts, hit[0]);
        let descriptor = RateLimitDescriptor {
            entries: request
                .iter()
                .map(|&(key, value)| rate_limit_descriptor::Entry {
                    key: key.into(),
                    value: value.into(),
                })
                .collect(),
            ..Default::default()
        };
        let decision = limiter.decide(theirs.domain(), &descriptor, 1, NOW);
        let Decision::Allow(Some(quota)) = decision else {
            panic!("{name}: {decision:?}");
        };
        assert_eq!(
            quota.remaining,
            theirs.spec().limit - 1 - u64::from(shares),
            "{name}: gabiond counts NGINX's hit exactly when the two share a counter"
        );
    }
}

/// One `limits:` entry named `name`: domain `api`, descriptor `ip`, rate
/// `10r/s`, with `fields` added or replacing those.
fn limit(name: &str, fields: &[(&str, &str)]) -> String {
    let mut entry = vec![
        ("domain", "api"),
        ("descriptors", "[{ key: ip }]"),
        ("rate", "10r/s"),
    ];
    for &(field, value) in fields {
        match entry.iter_mut().find(|(f, _)| *f == field) {
            Some(slot) => slot.1 = value,
            None => entry.push((field, value)),
        }
    }
    let fields: String = entry
        .iter()
        .map(|(field, value)| format!("    {field}: {value}\n"))
        .collect();
    format!("  - name: {name}\n{fields}")
}

/// A rule both adapters refuse is refused by the one library constructor,
/// with the same error the operator reads in either.
#[test]
fn both_adapters_refuse_a_rule_with_the_same_error() {
    let max_count = usize::from(STORAGE_MAX_DESCRIPTOR_COUNT);
    let many_nginx: String = (0..=max_count).map(|i| format!(" k{i}:$v{i}")).collect();
    let many_nginx = format!("r{many_nginx} rate=10r/s");
    let many_yaml: String = (0..=max_count)
        .map(|i| format!("{{ key: k{i} }},"))
        .collect();
    let many_yaml = format!("[{many_yaml}]");
    let long = "k".repeat(STORAGE_MAX_KEY_BYTES + 1);
    let long_nginx = format!("r {long}:$t rate=10r/s");
    let long_yaml = format!("[{{ key: {long} }}]");
    let rule = || Box::<str>::from("r");
    let window = |source| RuleError::Window {
        rule: rule(),
        source,
    };
    let yaml = |entries: String| format!("limits:\n{entries}");
    let rows: [(&[&str], String, RuleError); 13] = [
        (
            &["r rate=10r/s"],
            yaml(limit("r", &[("descriptors", "[]")])),
            RuleError::NoDescriptors { rule: rule() },
        ),
        (
            &["r 9x:$t rate=10r/s"],
            yaml(limit("r", &[("descriptors", "[{ key: 9x }]")])),
            RuleError::InvalidKey {
                rule: rule(),
                key: "9x".into(),
            },
        ),
        (
            &["r $t rate=10r/s window=0s"],
            yaml(limit("r", &[("window", "0s")])),
            window(ZeroWindow),
        ),
        (
            &["r $t rate=10r/s bucket=0s"],
            yaml(limit("r", &[("bucket", "0s")])),
            window(ZeroBucket),
        ),
        (
            &["r $t rate=10r/s bucket=500us"],
            yaml(limit("r", &[("bucket", "500us")])),
            window(ZeroBucket),
        ),
        (
            &["r $t rate=10r/s window=60d"],
            yaml(limit("r", &[("window", "60d")])),
            window(WindowTooLong),
        ),
        (
            &["r $t rate=10r/m window=500ms"],
            yaml(limit("r", &[("rate", "10r/m"), ("window", "500ms")])),
            window(WindowShorterThanPeriod),
        ),
        (
            &[&many_nginx],
            yaml(limit("r", &[("descriptors", &many_yaml)])),
            RuleError::TooManyDescriptors {
                rule: rule(),
                count: max_count + 1,
                max: max_count,
            },
        ),
        (
            &[&long_nginx],
            yaml(limit("r", &[("descriptors", &long_yaml)])),
            RuleError::KeyTooLong {
                rule: rule(),
                key: long.as_str().into(),
                bytes: STORAGE_MAX_KEY_BYTES + 1,
                max: STORAGE_MAX_KEY_BYTES,
            },
        ),
        (
            &["r $t rate=10r/s domain=a\u{7}b"],
            yaml(limit("r", &[("domain", "\"a b\"")])),
            RuleError::InvalidDomain {
                rule: rule(),
                reason: "with whitespace or a control character",
            },
        ),
        (
            &["r $t rate=10r/s", "r $u rate=10r/s"],
            yaml(limit("r", &[]) + &limit("r", &[("rate", "5r/s")])),
            RuleError::DuplicateRuleName { name: rule() },
        ),
        (
            &["r $t rate=10r/s", "s $t rate=20r/s dry_run"],
            yaml(limit("r", &[]) + &limit("s", &[("rate", "20r/s")])),
            RuleError::DuplicateRuleIdentity {
                first: rule(),
                second: "s".into(),
            },
        ),
        (
            &["r $t rate=10r/s", "s $u rate=10r/s"],
            "storage:\n  rule_dictionary_capacity: 1\n".to_owned()
                + &yaml(limit("r", &[]) + &limit("s", &[("window", "2s")])),
            RuleError::TooManyRules {
                rules: 2,
                capacity: 1,
            },
        ),
    ];
    // NGINX's directive parser refuses these rates itself; only gabiond
    // reaches the library with them.
    let rate = |text| RuleError::Rate {
        rule: rule(),
        reason: parse_rate(text).unwrap_err(),
    };
    let gabiond_only = ["0r/s", "10r/500us", "10r/fortnight"]
        .map(|text| (yaml(limit("r", &[("rate", text)])), rate(text)));

    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("rule_refusals.yaml");
    let gabiond_refuses = |yaml: &str, expected: &RuleError| {
        std::fs::write(&path, yaml).expect("write the gabiond config");
        match AppConfig::load(Some(&path)) {
            Err(ConfigError::Rule(error)) => assert_eq!(&error, expected, "{yaml}"),
            other => panic!("{yaml}: {other:?}"),
        }
    };
    for (directives, yaml, expected) in &rows {
        let settings = match expected {
            RuleError::TooManyRules { .. } => Settings {
                rule_dictionary_capacity: NonZeroU16::new(1),
                ..Settings::default()
            },
            _ => Settings::default(),
        };
        let (_, storage) = settings.validate().expect("the settings validate");
        let configs: Vec<RuleConfig> = directives
            .iter()
            .map(|text| {
                parse_rule_directive(&text.split_whitespace().collect::<Vec<_>>()).expect(text)
            })
            .collect();
        match CompiledRules::compile_with(&configs, &storage, &mut NopBindingCompiler) {
            Err(RuleConfigError::Rule(error)) => assert_eq!(&error, expected, "{directives:?}"),
            Err(other) => panic!("{directives:?}: {other}"),
            Ok(_) => panic!("{directives:?}: NGINX compiled the rules"),
        }
        gabiond_refuses(yaml, expected);
    }
    for (yaml, expected) in &gabiond_only {
        gabiond_refuses(yaml, expected);
    }
}
