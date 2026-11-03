//! The README's example, run: `readme_example` is the code the README's
//! `rust,ignore` fence shows, and `readme_example_matches_source` keeps the
//! two the same.

use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use gabion::aggregate::Aggregate;
use gabion::crdt::{CellStore, NodeId, NodeIdentity};
use gabion::gossip::sim::{SimRouter, sim_advance_ticks};
use gabion::gossip::{GossipRuntime, TokioClock};
use gabion::rules::{Descriptor, EnforcementMode, RuleDef, RuleTable, hash_key};
use gabion::settings::{Setting, Settings};
use tokio::task::LocalSet;

const README: &str = include_str!("../README.md");

#[test]
fn readme_example() {
    let tokio = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("tokio runtime");
    tokio.block_on(LocalSet::new().run_until(async {
        let router = SimRouter::new();
        let [a, b]: [SocketAddr; 2] =
            ["10.0.0.1:9000", "10.0.0.2:9000"].map(|addr| addr.parse().expect("an address"));
        // One node: its settings, its rule, its counter store, the aggregate
        // table admission reads, and its gossip runtime.
        let start = |id: u128, me: SocketAddr, peer: SocketAddr| {
            let settings = Settings {
                gossip_bind: Some(me),
                bootstrap_peers: Box::new([peer]),
                ..Settings::default()
            };
            let (gossip, storage) = settings.validate().expect("valid settings");
            let rule = RuleDef {
                name: "per_tenant",
                domain: "api",
                patterns: &[("tenant", "*")],
                rate: "100r/m",
                window: None,
                bucket: None,
                mode: EnforcementMode::Enforce,
            }
            .build(&storage.cardinality)
            .expect("valid rule");
            let rules = RuleTable::new(Box::new([rule]), storage.rule_dictionary_capacity())
                .expect("the rules fit");
            let store = CellStore::new(storage, NodeIdentity::new(NodeId(id), 1), &rules);
            let counts = Rc::new(Aggregate::new(&storage));
            let (runtime, client) = GossipRuntime::from_parts(
                router.bind(me),
                TokioClock::from_millis(0),
                gossip,
                store,
                Rc::clone(&counts),
            );
            tokio::task::spawn_local(runtime.run(futures::stream::empty()));
            (rules, counts, client)
        };
        let (rules, a_counts, a_client) = start(1, a, b);
        let (_, b_counts, _b_client) = start(2, b, a);

        // Admission on node A: read the cluster total, decide, record the hit.
        let spec = rules.rules()[0].spec();
        let key = hash_key(
            "api",
            [Descriptor {
                key: "tenant",
                value: "acme",
            }],
        );
        let now = 0;
        if a_counts.table().window_total(spec, key, now) < spec.limit {
            let bucket = spec.bucket_at(now);
            a_client
                .record(spec.fingerprint, key, bucket, 1, spec.limit, now)
                .await
                .expect("the runtime is running");
        }
        // Gossip carries the hit to node B within a heartbeat.
        sim_advance_ticks(Duration::from_millis(10), 100).await;
        assert_eq!(b_counts.table().window_total(spec, key, now), 1);
    }));
}

/// Each non-blank line of the README's `rust,ignore` fence appears, in order,
/// in `readme_example`.
#[test]
fn readme_example_matches_source() {
    let source = include_str!("readme.rs");
    let body = &source[source.find("fn readme_example()").expect("the example")..];
    let body = &body[..body
        .find("fn readme_example_matches_source")
        .expect("this test")];
    let mut fences = README.split("```rust,ignore\n").skip(1);
    let fence = fences.next().expect("the README has a `rust,ignore` fence");
    assert!(
        fences.next().is_none(),
        "the README has one `rust,ignore` fence"
    );
    let fence = &fence[..fence.find("```").expect("the fence closes")];
    let mut lines = body.lines().map(str::trim);
    for line in fence.lines().map(str::trim).filter(|line| !line.is_empty()) {
        assert!(
            lines.any(|candidate| candidate == line),
            "README line `{line}` is not in readme_example, in order"
        );
    }
}

/// Every fence in the README names its language, so rustdoc compiles none.
#[test]
fn readme_fences_name_their_language() {
    let mut open = false;
    for (number, line) in README.lines().enumerate() {
        if let Some(info) = line.trim_start().strip_prefix("```") {
            assert!(
                open || !info.is_empty(),
                "README line {}: a bare fence",
                number + 1
            );
            open = !open;
        }
    }
}

/// Every setting has a row in the README's knob tables giving its three
/// spellings, and its default when it has one: the table both adapter
/// READMEs copy.
#[test]
fn readme_lists_every_setting() {
    for &setting in Setting::ALL {
        let key = format!("`{}`", setting.yaml_key());
        let env = format!("`{}`", setting.env_var());
        let directive = setting
            .nginx_directive()
            .map_or_else(|| "none".to_owned(), |name| format!("`{name}`"));
        let row: Vec<&str> = README
            .lines()
            .map(|line| line.split('|').map(str::trim).collect::<Vec<_>>())
            .find(|cells| cells.get(1) == Some(&key.as_str()))
            .unwrap_or_else(|| panic!("crates/gabion/README.md has no knob row for {key}"));
        assert_eq!(row.get(2).copied(), Some(env.as_str()), "{key}'s variable");
        assert_eq!(
            row.get(3).copied(),
            Some(directive.as_str()),
            "{key}'s directive"
        );
        if let Some(default) = setting.default_text() {
            let default = format!("`{default}`");
            assert_eq!(
                row.get(4).copied(),
                Some(default.as_str()),
                "{key}'s default"
            );
        }
    }
}
