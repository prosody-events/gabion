# gossip-bench

The simulator-driven evaluation harness for the gabion gossip protocol.
A Rust binary runs scenarios against `gabion::gossip::sim::SimRouter`
under virtual time and emits a JSON result, while a Python harness
(`bench/plot.py`) generates scenario matrices, drives the binary, and
turns the resulting records into SVG plots.

For the gossip explainer itself and the headline numbers we publish,
see [`crates/gabion/README.md`](../gabion/README.md); the present
document is concerned only with the mechanics of running the suite.

## Running

```sh
# Every suite. Writes target/gossip-bench/<suite>/results.jsonl and
# target/gossip-bench/figures/<suite>.svg.
python3 crates/gossip-bench/bench/plot.py all

# Same, but also copy SVGs into crates/gabion/figures/ so the
# explainer's embedded figures reflect this run. Only --publish after
# a clean bench; otherwise the figures the README embeds will be stale.
python3 crates/gossip-bench/bench/plot.py all --publish

# Just one suite.
python3 crates/gossip-bench/bench/plot.py coverage_fanout
```

Suite names: `convergence`, `fanout_sweep`, `loss`, `partition`,
`staleness`, `scale_n`, `coverage_fanout`, `error_budget`,
`min_emit_clamp`, `heartbeat_threshold_mix`.

On the first invocation the harness builds `target/release/gossip-bench`
once and then reuses the same binary across every subsequent scenario,
so repeated runs pay only the cost of the simulator itself.

## Ad-hoc scenarios

```sh
# Generate a starter spec, edit it, run it back through the bench.
cargo run -p gossip-bench --release -- example --kind partition > scn.json
$EDITOR scn.json
cargo run -p gossip-bench --release -- run --scenario scn.json | jq .

# Or stream JSONL. Result records echo the scenario name back, so
# unique names are for your own readability.
for f in 1 2 3 5; do
    cargo run -p gossip-bench --release -- example --kind convergence \
        | jq -c ".fanout = $f | .name = \"f$f\""
done | cargo run -p gossip-bench --release -- batch > matrix.jsonl
```

The built-in example kinds available through `--kind` are `convergence`,
`loss`, `partition`, `sustained`, `scale_n`, `fanout_sweep`,
`coverage_fanout`, `error_budget`, `min_emit_clamp`, and
`heartbeat_threshold_mix`.

## What each suite measures

The suites fall into two groups. Three of them are the empirical
evidence for gabion's two adaptive mechanisms: `coverage_fanout`
exercises the runtime's *coverage fanout*, while `error_budget` and
`min_emit_clamp` together exercise its *adaptive emit rate*. The
remaining suites are classical anti-entropy measurements borrowed from
the literature surveyed in [`REFERENCES.md`](REFERENCES.md), and serve
mainly to establish that gabion behaves as the published theory
predicts before we layer the adaptive machinery on top.

| Suite | Paper | What it measures |
| --- | --- | --- |
| `convergence` | Demers 1987, Karp 2000 | Rounds to converge vs `(N, fanout)` for a single write. |
| `fanout_sweep` | Demers 1987, Bimodal '99 | Convergence vs network cost (rounds vs bytes/node/s) at fixed N. |
| `loss` | Bimodal '99, SWIM '02 | Convergence under i.i.d. per-link drop probability. |
| `partition` | SWIM '02 | Time to re-converge after a network partition is healed. |
| `staleness` | Astrolabe '03 | Per-hit p50/p95 lag under sustained writes from k sources. |
| `scale_n` | Karp 2000, Astrolabe '03 | log-N curve: rounds-to-converge as cluster size grows. |
| `coverage_fanout` | Kermarrec/Massoulié/Ganesh '03 | Evidence for **coverage fanout**: the per-tick `peak_effective_fanout` tracks the threshold `⌈ln(n)+c⌉` as the cluster grows, and stays flat as the dirty set grows (the burst rides one fat frame, so volume does not widen the pick). |
| `error_budget` | Sharfman/Schuster/Keren '06, Olston/Jiang/Widom '03 | Evidence for **adaptive emit rate**: bandwidth and max-lag as a function of `target_err_bps`; verifies the `N × ε_R` cluster-wide bound. |
| `min_emit_clamp` | gabion-specific | Evidence for **adaptive emit rate** (the floor): adversarial saturating write rate; sweeps `min_emit_interval`. Confirms the floor caps worst-case emit rate while the cluster still converges. |
| `heartbeat_threshold_mix` | gabion-specific | A hot rule (saturating ε every tick) and a cold rule (slow trickle) replicate concurrently; both must converge. |

The headline number from each suite, the methodology behind it, and the
full-resolution figure all live in
[`crates/gabion/README.md#what-we-measured`](../gabion/README.md#what-we-measured),
and [`REFERENCES.md`](REFERENCES.md) provides the paper-by-paper survey
that justifies the methodology choices made here.

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
- `bench/plot.py` — Python harness: matrix generator, matplotlib /
  seaborn plotting, SVG output.
- `REFERENCES.md` — paper-by-paper survey of the methodologies we
  borrowed.
