//! A rule written for NGINX and the same rule written for `gabiond` are one
//! rule. Each adapter loads its rules through its production path, the two
//! configs listing them in opposite orders. A hit NGINX queues, folded into
//! the cluster aggregate, is one `gabiond` counts for the same request
//! exactly when the pair also agrees on its bucket, so each adapter's own key
//! hash is on the line.

use std::sync::Arc;

use gabion::aggregate::Aggregate;
use gabion::crdt::{
    CellHandle, CellIdentity, CellStore, DeltaSink, ExpirationSink, NodeId, NodeIdentity,
};
use gabion::gossip::sim::SimRouter;
use gabion::gossip::{AggregateStore, FixedClock, GossipRuntime};
use gabion_nginx::access::{AccessCtx, AccessOutcome, VariableLookup, decide_all};
use gabion_nginx::rules::{
    BindingLookup, CompiledRules, NopBindingCompiler, RuleConfig, parse_rule_directive,
};
use gabion_nginx::shm::queue::QueueEvent;
use gabion_nginx::shm::{QueueControl, QueueSlot, RequestQueue, Stats};
use gabion_server::admission::Decision;
use gabion_server::config::AppConfig;
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
        handles: vec![CellHandle::default()],
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
