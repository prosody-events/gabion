# gossip-bench

The simulator-driven evaluation harness for the gabion gossip protocol.
The Rust binary runs scenarios against `gabion::gossip::sim::SimRouter`
under virtual time and emits a JSON result; a Python harness
(`bench/plot.py`) generates scenario matrices, drives the binary, and
produces SVG plots.

Read [`crates/gabion/README.md`](../gabion/README.md) for the gossip
explainer and the headline benchmark numbers. This README only covers
"how to run the suite."

## Running

```sh
# Every suite. Writes target/gossip-bench/<suite>/results.jsonl and
# target/gossip-bench/figures/<suite>.svg.
python3 crates/gossip-bench/bench/plot.py all

# Same, but also copy SVGs into crates/gabion/figures/ so the
# explainer's embedded figures reflect this run. Run --publish only
# after a clean bench; otherwise the README will commit stale plots.
python3 crates/gossip-bench/bench/plot.py all --publish

# Just one suite.
python3 crates/gossip-bench/bench/plot.py adaptive_fanout
```

Suite names: `convergence`, `fanout_sweep`, `loss`, `partition`,
`staleness`, `scale_n`, `adaptive_fanout`, `error_budget`,
`min_emit_clamp`, `heartbeat_threshold_mix`.

The first run builds `target/release/gossip-bench` once and reuses it
for every subsequent scenario.

## Ad-hoc scenarios

```sh
# Generate a starter spec, edit it, run it back through the bench.
cargo run -p gossip-bench --release -- example --kind partition > scn.json
$EDITOR scn.json
cargo run -p gossip-bench --release -- run --scenario scn.json | jq .

# Or stream JSONL.
for f in 1 2 3 5; do
    cargo run -p gossip-bench --release -- example --kind convergence \
        | jq -c ".fanout = $f | .name = \"f$f\""
done | cargo run -p gossip-bench --release -- batch > matrix.jsonl
```

Built-in example kinds: `convergence`, `loss`, `partition`,
`sustained`, `scale_n`, `fanout_sweep`, `adaptive_fanout`,
`error_budget`, `min_emit_clamp`, `heartbeat_threshold_mix`.

## What each suite measures

| Suite | Paper | What it measures |
| --- | --- | --- |
| `convergence` | Demers 1987, Karp 2000 | Rounds to converge vs `(N, fanout)` for a single write. |
| `fanout_sweep` | Demers 1987, Bimodal '99 | Convergence vs network cost (rounds vs bytes/node/s) at fixed N. |
| `loss` | Bimodal '99, SWIM '02 | Convergence under i.i.d. per-link drop probability. |
| `partition` | SWIM '02 | Time to re-converge after a network partition is healed. |
| `staleness` | Astrolabe '03 | Per-hit p50/p95 lag under sustained writes from k sources. |
| `scale_n` | Karp 2000, Astrolabe '03 | log-N curve: rounds-to-converge as cluster size grows. |
| `adaptive_fanout` | Verma & Ooi '05 | Rounds and effective fanout as the dirty set grows; at static `fanout=1`, with the adaptive bump in place, rounds stay nearly flat. |
| `error_budget` | Sharfman/Schuster/Keren '06, Olston/Jiang/Widom '03 | Bandwidth and max-lag as a function of `target_err_bps`; verifies the `N × ε_R` cluster-wide bound. |
| `min_emit_clamp` | gabion-specific | Adversarial saturating write rate; sweeps `min_emit_interval`. Confirms the floor caps worst-case emit rate while the cluster still converges. |
| `heartbeat_threshold_mix` | gabion-specific | A hot rule (saturating ε every tick) and a cold rule (slow trickle) replicate concurrently; both must converge. |

Each suite's headline numbers, methodology, and full figure are
documented in
[`crates/gabion/README.md#what-we-measured`](../gabion/README.md#what-we-measured).
[`REFERENCES.md`](REFERENCES.md) is the paper-by-paper survey behind
the methodology choices.

## Files

- `src/lib.rs` — public API.
- `src/scenario.rs` — scenario JSON schema (kinds, workloads, network
  policies).
- `src/metrics.rs` — result JSON schema.
- `src/scenarios.rs` — the runner: N runtimes on one `SimRouter`,
  workload driver, sampler, headline derivation.
- `src/transport.rs` — `CountingTransport` wrapper that counts
  bytes/packets per `try_send_to` / `recv_from`.
- `src/bin/gossip_bench.rs` — the CLI (`run`, `batch`, `example`).
- `bench/plot.py` — Python harness: matrix generator + matplotlib /
  seaborn plotting, SVG output.
- `REFERENCES.md` — paper-by-paper survey of the methodologies we
  borrowed.
