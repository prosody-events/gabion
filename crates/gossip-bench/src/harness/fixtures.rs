//! Small regimes for the harness's own tests.

use super::regime::Regime;

/// A five-pod regime small enough to run in well under a second, with every
/// feature switched on: hot keys, a burst, loss, a rollout and a crash.
pub fn tiny() -> Regime {
    let regime: Regime = serde_json::from_value(serde_json::json!({
        "id": "tiny",
        "description": "test fixture",
        "duration_s": 30,
        "warmup_s": 5,
        "nodes": 5,
        "rule": { "limit": 20, "window_ms": 4000, "bucket_ms": 1000 },
        "traffic": {
            "site_rate_rps": 10,
            "site_period_ms": 2000,
            "site_jitter": 0.5,
            "hot_keys": [{ "limit_multiple": 2.0 }],
            "bursts": [{ "rate_rps": 30, "start_s": 10, "duration_s": 5 }]
        },
        "network": { "loss": 0.05 },
        "churn": {
            "rollouts": [{ "at_s": 12, "count": 2, "interval_ms": 1000, "overlap_ms": 2000 }],
            "crashes": [{ "at_s": 20 }]
        },
        "tracked_sample_keys": 10
    }))
    .expect("fixture regime parses");
    regime.validate().expect("fixture regime is valid");
    regime
}

/// `nodes` pods and nothing else: no traffic, no loss, no churn.
pub fn quiet(nodes: u32) -> Regime {
    let regime: Regime = serde_json::from_value(serde_json::json!({
        "id": "quiet",
        "description": "test fixture",
        "duration_s": 10,
        "warmup_s": 0,
        "nodes": nodes,
        "rule": { "limit": 10, "window_ms": 3000, "bucket_ms": 1000 },
        "traffic": { "site_rate_rps": 1, "site_period_ms": 1000, "site_jitter": 0.0 },
        "tracked_sample_keys": 1
    }))
    .expect("fixture regime parses");
    regime.validate().expect("fixture regime is valid");
    regime
}
