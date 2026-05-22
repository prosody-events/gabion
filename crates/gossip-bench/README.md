# gossip-bench

Simulator-driven evaluation harness for the gabion gossip protocol. Each
scenario is a JSON spec; the Rust binary runs it against
[`gabion::gossip::sim::SimRouter`] under virtual time and emits a
JSON-line result. A Python harness (`bench/plot.py`) generates scenario
matrices, drives the binary, and produces matplotlib/seaborn plots
saved under `target/gossip-bench/`.

## Why a separate crate?

The gabion crate hosts the gossip *runtime* and the simulator
*primitives* (router, transport, virtual-clock helpers). `gossip-bench`
hosts the *evaluation harness on top* — scenario shapes, metric
extraction, the byte/packet-counting transport wrapper, and the CLI
that drives the Python plotter. Keeping it out of `gabion` means the
library's compile times stay small and benchmarking deps (clap,
serde-json wiring, etc.) don't leak into the runtime.

## Running

```sh
# All suites — runs the matrix, writes JSON + PNGs under target/gossip-bench/.
python3 crates/gossip-bench/bench/plot.py all

# One suite.
python3 crates/gossip-bench/bench/plot.py convergence
python3 crates/gossip-bench/bench/plot.py fanout_sweep
python3 crates/gossip-bench/bench/plot.py loss
python3 crates/gossip-bench/bench/plot.py partition
python3 crates/gossip-bench/bench/plot.py staleness
python3 crates/gossip-bench/bench/plot.py scale_n

# Single ad-hoc scenario.
cargo run -p gossip-bench --release -- example --kind partition | \
    cargo run -p gossip-bench --release -- run | jq .headline
```

The first run builds the release binary (`target/release/gossip-bench`)
once and reuses it for every subsequent scenario.

## Suites

Every suite traces back to a published evaluation methodology — see
[`REFERENCES.md`](REFERENCES.md) for the full survey (Demers 1987, Karp
2000, SWIM 2002, HyParView 2007, Plumtree 2007, Astrolabe 2003, Bimodal
Multicast 1999, Jelasity 2007, plus Dynamo / Cassandra / Riak AAE).

| Suite | Paper | What it measures |
| --- | --- | --- |
| `convergence` | Demers 1987, Karp 2000 | Rounds to converge vs `(N, fanout)` for a single write. |
| `fanout_sweep` | Demers 1987, Bimodal '99 | Convergence vs network cost (rounds vs bytes/node/s) at fixed N. |
| `loss` | Bimodal '99, SWIM '02 | Convergence under i.i.d. per-link drop probability. |
| `partition` | SWIM '02 | Time to re-converge after a network partition is healed. |
| `staleness` | Astrolabe '03 | Per-hit p50/p95 lag under sustained writes from k sources. |
| `scale_n` | Karp 2000, Astrolabe '03 | log-N curve: rounds-to-converge as cluster size grows. |

## Headline results (machine produced)

The values below come from `python3 bench/plot.py all` on commit
`HEAD` — they regenerate every time the suite runs.

### Convergence: rounds to converge for one write, N × fanout

| N  | f=1 | f=2 | f=3 | f=5 | f=8 |
| --- | --- | --- | --- | --- | --- |
| 4  | 6 | **2** | 2 | — | — |
| 8  | 5 | 4 | **3** | 2 | — |
| 16 | 8 | 4 | **2** | 2 | 2 |
| 32 | 9 | 5 | **3** | 3 | 2 |
| 64 | 13 | 7 | **5** | 4 | 3 |

Karp et al. (2000) prove that pure push converges in **Θ(log₂ N)**
rounds w.h.p. Comparing the `f=3` column (gabion's production default)
against log₂ N: N=8 → 3 vs 3, N=16 → 2 vs 4 (faster!), N=32 → 3 vs 5
(faster), N=64 → 5 vs 6. Gabion's peer-frontier dedup pushes the
empirical curve *below* the address-oblivious lower bound, exactly the
expected outcome of the dedup giving the sender per-receiver knowledge.

### Loss tolerance (N=16, f=3, i.i.d. per-link drop)

| Loss | Rounds (p50) | Final divergence |
| --- | --- | --- |
| 0%   | 3 | 0 |
| 10%  | 3-4 | 0 |
| 20%  | 3-4 | 0 |
| 30%  | 4 | 0 |
| 40%  | 4 | 0 |
| 50%  | 4-5 | 0 |

Bimodal Multicast (Birman et al. 1999) reports stable delivery at up
to ~25-30% loss. Gabion converges in O(log N) rounds with only a
+2 rounds penalty at 50% loss; no scenario in the sweep failed to
converge within 20 s of virtual time.

### Partition + heal (N=8, half/half cut)

A single write at node 0 is invisible to the other partition until
the split is healed at t=10 s. Reconvergence happens within the next
gossip tick (≤ 100 ms after heal).

### Bandwidth / network cost

At idle (no writes), the repair lane plus dirty-ring heartbeat gossip
gives a steady-state of **~4.4 kB/node/s** at N=32, f=3 — a constant
multiple of the fanout, flat in N. This matches the SWIM "constant
per-node load as N grows" property: when N=64 doubles, per-node bytes
stay at ~4.3 kB/s.

## Comparing against SWIM

Gabion does **not** measure failure-detection latency or false-positive
rate — k8s owns membership through EndpointSlice and the gossip
protocol assumes "every peer is alive until removed". The fair
comparisons against SWIM are:

- **Per-node load flat in N**: gabion ✓ (4.4 kB/s at N=32, 4.3 kB/s at
  N=64 — within noise).
- **Convergence time**: SWIM reports `T'/(1−e^(−qf)) ≈ 1.6·T'` rounds
  for an incarnation/update to spread. With T'=100 ms ticks and our
  fanout=3, that's ~160 ms; we observe `convergence_millis = 200-300
  ms` for N ∈ {8, 16, 32}, well within 2× of SWIM's analytic bound for
  the same fanout.
- **Behavior under loss**: SWIM relies on `k` indirect-probe targets;
  we rely on the repair lane + peer frontier. The loss suite shows
  gabion handles 50% i.i.d. loss with only +2 rounds.

## Reproducing a single scenario

```sh
cargo run -p gossip-bench --release -- example --kind convergence > scn.json
$EDITOR scn.json   # tweak nodes, fanout, etc.
cargo run -p gossip-bench --release -- run --scenario scn.json | jq .
```

Or stream a JSONL batch:

```sh
for f in 1 2 3 5; do
    cargo run -p gossip-bench --release -- example --kind convergence \
        | jq -c ".fanout = $f | .name = \"f$f\""
done | cargo run -p gossip-bench --release -- batch > matrix.jsonl
```

## Files

- `src/lib.rs` — public API.
- `src/scenario.rs` — scenario JSON schema.
- `src/metrics.rs` — result JSON schema.
- `src/scenarios.rs` — the runner: N runtimes on one `SimRouter`,
  workload driver, sampler, headline derivation.
- `src/transport.rs` — `CountingTransport` wrapper that counts
  bytes/packets per `try_send_to` / `recv_from` so the bandwidth
  metric is sound.
- `src/bin/gossip_bench.rs` — the CLI (`run`, `batch`, `example`).
- `bench/plot.py` — Python harness: matrix generator + matplotlib /
  seaborn plotting.
- `REFERENCES.md` — paper-by-paper survey of the methodologies we
  borrowed (Demers, Karp, SWIM, HyParView, Plumtree, Astrolabe,
  Bimodal Multicast, Gossip-PS, plus production CRDT systems).
