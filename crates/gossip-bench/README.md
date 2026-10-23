# gossip-bench

Simulators for the gabion gossip protocol. Two live here:

- The **study harness** ([below](#study-harness)): a deterministic
  discrete-event simulation that runs any gossip design against identical
  production-shaped load, network and churn, and scores it against ground
  truth. Use it to compare designs.
- The **scenario suite** (the `legacy` cargo feature, on by default): a
  Rust binary runs scenarios against `gabion::gossip::sim::SimRouter`
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

## Study harness

The study harness answers one question: on identical traffic, network and
churn, what does a gossip design cost in bytes, and what does it buy in
accuracy? Every solution is a branch based on the harness branch that leaves
`crates/gossip-bench/` byte-identical, so the simulator, the workload and the
judge are the same code for every contender. The `main` baseline is the
harness branch itself.

### Running

```sh
# One run. Prints the result JSON, or writes it with --out.
cargo run --release -p gossip-bench --no-default-features -- \
    study run --regime crates/gossip-bench/regimes/prod-typical.json --seed 1

# A matrix for the checked-out solution: every regime, seeds 1..3, two at a
# time. Builds the binary, stamps each result, resumes after interruption.
python3 crates/gossip-bench/judge/matrix.py --solution main --seeds 3 \
    --jobs 2 --out ../study-results

# Compare solutions. Refuses mixed harness hashes and invalid runs.
python3 crates/gossip-bench/judge/judge.py ../study-results --baseline main \
    --out ../study-report
```

`matrix.py` writes `<out>/<solution>/<regime>/seed-<n>.json`, stamped with
the harness tree hash (`git rev-parse HEAD:crates/gossip-bench`), the
solution label, branch and commit, plus the process's wall time, CPU time
and peak RSS from `wait4` (host-dependent; everything else is not). It
refuses to run when `crates/gossip-bench/` has uncommitted changes. The
judge writes `summary.md` and `summary.csv` (every metric per regime and
solution: mean across seeds, 95 % Student-t confidence half-width, min–max,
ratio to the baseline), `pareto.md` (total bytes against limited-key p99
staleness and against worst-key over-admission), and `rejected.md`.

Memory is the constraint on `--jobs`: cell-store capacity is allocated up
front, so `prod-4x` needs several GB per run. The judge's `rss_mib` and
`cpu_s` rows report each regime's cost.

### Model

**Time.** Virtual time is a `u64` nanosecond count that advances only when
the event loop pops the next event (`src/harness/world.rs`). There is no
fixed step: datagram arrivals, request arrivals and timer deadlines keep
nanosecond precision. The runtime reads milliseconds from its `Clock`, as in
production, offset to a fixed epoch (2026-01-01T00:00Z) that is a multiple
of every bucket width. Each node's ticker mirrors `tokio::time::interval`
with `MissedTickBehavior::Delay`: the first tick fires at start, later ticks
every `tick_interval`. A node is a task on a small single-threaded executor,
polled when its waker fires, in wake order; no virtual time passes while it
runs.

**Determinism.** A run is a pure function of regime, seed and binary: the
same triple yields byte-identical JSON (tested). No map is iterated in an
order that depends on hashing. Randomness comes from SplitMix64 streams
separated per concern: each key's arrival process and load-balancer picks
(traffic), latency and loss (network), pod start offsets, discovery jitter
and crash victims (churn), the tracked-key sample (oracle), and per-node
gossip RNG seeds. A solution that sends more datagrams consumes more of the
network stream and nothing else, so the offered-load trace and membership
timeline are identical across solutions (tested).

**Network** (`src/harness/net.rs`). Pods are assigned round-robin to three
availability zones. One-way latency is `base + U(0, jitter_max)`: 200 µs +
U(0, 100 µs) within a zone (mean 0.25 ms), 800 µs + U(0, 400 µs) across
zones (mean 1 ms); datagrams may reorder. A datagram's fate is decided at
send time in order: partition, unknown destination, i.i.d. Bernoulli loss,
Gilbert-Elliott burst loss (state per directed link, advanced per
datagram). On arrival it is dropped if the destination process has stopped
or its receive buffer (default 212,992 payload bytes, Linux's
`rmem_default`) would overflow. The sender never sees backpressure:
`writable` always resolves and `try_send_to` always succeeds, as for a UDP
socket with send-buffer room.

**Byte accounting.** Every datagram is counted once, at send time, for the
whole network: UDP payload + 28 B IPv4/UDP header. Totals are reported for
the measurement window and for the whole run, with packets, cross-AZ bytes,
and bytes and packets dropped by cause (loss, partition, receive buffer, no
receiver). Derived figures divide the window total by the integral of the
running-pod count (bytes per node per second), by window length, and by
admitted and offered requests. A payload above 1,472 B (one 1,500 B MTU) is
counted as a violation and invalidates the run.

**Workload** (`src/harness/workload.rs`). One enforced rule (limit, window,
bucket) per regime. Site keys number `round(site_rate × period)`; each
requests every `period × U(1 − jitter, 1 + jitter)`, first at
`U(0, period)`. Hot keys are Poisson at `limit_multiple` × the sustainable
rate `limit / window`; burst keys are Poisson at `rate_rps` during their
interval. Every request lands on a uniformly random running pod, as a load
balancer would place it. Keys are hashed with `gabion::rules::hash_key` and
the rule fingerprint comes from `gabion::rules::Rule::new`, as in the
adapters.

**Admission** follows `SharedLimiter::evaluate`
(`crates/server/src/lib.rs`): read the pod's window total, reject iff
`total + hits > limit`, otherwise record the hit with
`GossipClient::record(fingerprint, key_hash, now_ms / bucket_ms, 1, limit,
now_ms)` and wait for the acknowledgement (the runtime has applied it to the
pod's aggregate). Only admitted hits are recorded. The offered load is
identical across solutions; the admitted load legitimately differs.

**Membership and churn** (`src/harness/plan.rs`), computed before any node
runs. Initial pods start at `U(0, 1 s)` so their ticks are not
phase-aligned, and each lists every initial pod at start. A pod that joins
later lists every current member at start; each other member learns of it
after `U(0, 2 s)`. A rollout wave starts replacement `i` (fresh identity and
address) at `at + i × interval` and stops the oldest surviving initial pod
gracefully `overlap` later; members learn of the departure after
`U(0, 2 s)`. A silent crash stops a seeded-random running pod; discovery
removes it 10 s later, plus `U(0, 2 s)`. A stopped pod leaves the load
balancer at once. Optional partitions drop all traffic between one zone and
the rest during an interval.

**Aggregate store.** Each pod's `AggregateStore` is a per-key ring of
per-bucket totals: deltas add, expirations subtract the origin's last count,
the window total sums the live buckets. This is the server adapter's
`DashMapStore` (`crates/server/src/store.rs`) laid out for a key population
known up front; within the live window the two are equivalent.

**Sizing.** Capacities are derived from the regime, identically for every
solution. `cell_capacity` is 1.5 × an upper bound on the cells one pod can
hold (each request creates at most one `(key, bucket, origin)` cell living
one window plus two buckets, and a key holds at most one cell per pod per
bucket), at least 16,384, rounded up to 1,024; any solution that expires
cells within two buckets of their leaving the window fits; dirty rings keep the production ratios to
the cell store (`STORAGE_LOCAL_DIRTY_CAPACITY` and
`STORAGE_FORWARDED_DIRTY_CAPACITY` over `STORAGE_MAX_CELLS`); node and peer
dictionaries are the production defaults or the regime's pod count + 16,
whichever is larger. Gossip settings are the production defaults
(`GossipSettings::default()` in `gabion-server`); solutions own them.

### Metrics

All metrics except the whole-run byte totals cover the measurement window
(after `warmup_s`). The oracle sees ground truth; nodes never do.

- **Truth** is every admitted hit, cluster-wide, per key and bucket.
- **Staleness error** of a pod for a tracked key is `truth window total −
  the pod's window total`; positive is an under-count, the error that lets a
  pod over-admit. It is recomputed on every event that can change it (an
  admission, an aggregate update on that pod, a bucket boundary), so its
  distribution (mean, p50, p99, max, min) is weighted by exact exposure time
  over every running (pod, key) pair, in hits and as a percentage of the
  limit. Tracked keys are every hot and burst key ("limited") plus a seeded
  sample of `tracked_sample_keys` sites ("sampled"), reported separately.
- **Residual error** is the same difference restricted to buckets that
  closed at least 1.5 s ago: error that is no longer in flight. Sampled
  every second at +500 ms.
- **Convergence time** of an admitted hit on a tracked key, the `j`-th in
  its bucket, is the time until every pod that was running when it was
  admitted, and still runs, has a count of at least `j` for that bucket.
  Hits whose bucket leaves the window first count as never converged;
  those still pending at the end are reported as censored.
- **Rate-limit accuracy** per limited key: at every bucket boundary the
  cluster-wide admitted total of the trailing window minus the limit
  (over-admission: max, mean, share of windows over), plus offered,
  admitted, rejected and **false rejects** (rejected although the true
  cluster total plus the request was within the limit) in the window.
- **Churn**: for every pod that joins during the window, the time until its
  summed tracked-key totals first reach 95 % and 99 % of truth, and its
  integrated summed under-count over the first window after joining
  (hit-seconds).
- **Health**: cell-store, rule- and node-dictionary full rejects, oversize
  datagrams, runtime exits, failed records, and aggregate rows for keys the
  regime never issued each make the run **invalid**. Also reported: peak
  active cells, decode rejects, maximum send-queue depth.

### Regimes

Regime files are in `regimes/`. Pods, request rate (site rate plus hot and
burst keys), rule, and the virtual duration (warmup + measurement):

| Regime | Pods | Load | Rule | Duration | Notes |
|---|---|---|---|---|---|
| `prod-typical` | 80 | 650 req/s sites | 100 / 10 s, 1 s buckets | 20 + 60 s | hot keys at 1.5× and 3× limit; 90 req/s burst for 20 s |
| `prod-peak` | 183 | 1,800 req/s | as above | 20 + 60 s | as above |
| `prod-4x` | 300 | 7,200 req/s, 23,040 sites | as above | 20 + 60 s | as above |
| `loss-5`, `loss-20` | 183 | as `prod-peak` | as above | 20 + 60 s | 5 % / 20 % i.i.d. loss |
| `loss-burst` | 183 | as `prod-peak` | as above | 20 + 60 s | Gilbert-Elliott, 5 % mean loss, mean burst 5.3 datagrams |
| `low-3`, `low-5` | 3 / 5 | 10 req/s sites | 20 / 10 s, 1 s buckets | 20 + 60 s | hot keys at 1.5× and 3× |
| `idle` | 80 | 1 req/s | 100 / 10 s | 20 + 60 s | protocol floor cost |
| `churn-rollout` | 80 (90 at most mid-wave; 79 after the crash) | as `prod-typical` | as above | 20 + 90 s | 45-pod rollout from 30 s, one pod every 500 ms, 5 s overlap; silent crash at 75 s |
| `long-window` | 60 | 30 req/s | 2,000 / 1 h, 1 min buckets | 3,600 + 1,200 s | hot keys; 90 req/s burst for 60 s; first window warmed by real traffic |

### Solution contract

A solution branch must keep this surface compiling and behaving as
documented; the harness uses nothing else (`src/harness/node.rs`,
`src/harness/world.rs`, `src/harness/driver.rs`):

- `gabion_server::config::GossipSettings::default().into_runtime_config(identity, rng_seed) -> GossipConfig`:
  the production gossip configuration. Tune a solution through it (and
  `gabion::defaults`), not through the harness.
- `gabion::crdt::{CellStore::<u32>::new(CellStoreConfig, NodeIdentity), CellStore::intern_rule(RuleDescriptor) -> Option<_>}`,
  `CellStoreConfig` with exactly the fields `cell_capacity`,
  `rule_dictionary_capacity`, `node_dictionary_capacity`,
  `local_dirty_capacity`, `forwarded_dirty_capacity`, `peer_capacity`, and
  `RuleDescriptor { fingerprint, window_millis, bucket_millis, limit, flags, local_rule_id }`.
- `gabion::gossip::GossipRuntime::from_parts_with_admin(transport, clock, config, cell_store, aggregates, Some(admin_rx)) -> (runtime, GossipClient<u32>)`
  and `runtime.run(peer_events) -> impl Future<Output = Result<(), GossipError>>`.
- `gabion::gossip::GossipTransport` (`local_addr`, `writable`,
  `try_send_to`, `recv_from`), `Clock` (`now_millis`, `ticker`) and `Ticker`
  (`tick`), implemented by the harness. **All timing must come from
  `Clock::now_millis` and tickers from `Clock::ticker`** (any number of
  them); the runtime must not use `tokio::time`, `tokio::spawn` or threads,
  because no Tokio runtime is driving it.
- `gabion::gossip::AggregateStore<u32>::apply(&DeltaSink<u32>, &ExpirationSink<u32>)`,
  reading `DeltaSink { keys, deltas, applies_locally }` and
  `ExpirationSink { keys, last_counts, applies_locally }` with
  `CellIdentity { rule_fingerprint, key_hash, bucket }`. Deltas must be
  per-bucket increments of the cluster aggregate and expirations the
  matching decrements.
- `gabion::gossip::GossipClient::record(rule_fingerprint, key_hash, bucket, hits, rule_limit, now_millis)`,
  resolving once the hit is visible in the node's own aggregate.
- `gabion::gossip::AdminCommand::Snapshot { reply }` answered with an
  `AdminSnapshot` carrying `store_stats.{active_cells,
  cell_store_full_rejects, rule_dictionary_full_rejects,
  node_dictionary_full_rejects}`, `decode_reject_count` and
  `max_send_pending_depth`.
- `gabion::discovery::{Peer::new, PeerEvent::{Added, Removed}}` delivered
  on the `peer_events` stream; `gabion::rules::{Rule::new, hash_key,
  Descriptor, DescriptorPattern, EnforcementMode}`; `gabion::defaults`
  `STORAGE_*` capacity constants.
- Determinism: given its `rng_seed`, a node must behave identically run to
  run (no `RandomState` iteration, no wall clock). The determinism test
  (`cargo nextest run -p gossip-bench --no-default-features`) must pass.

The study builds with `--no-default-features`, so a solution that reshapes
`gabion::gossip::sim` or the admin counters the legacy suite reads breaks
only the legacy suite.

### Limitations

- **Instantaneous processing.** Nodes take no virtual time to process a
  datagram, a request or a tick, so CPU saturation, scheduling delay and
  queueing inside a node are not modelled, and the receive buffer can only
  overflow when many datagrams arrive at the same instant. Process CPU time
  and peak RSS are reported as a noisy secondary signal.
- **Adapter.** Admission follows the gRPC server adapter. The NGINX adapter
  additionally batches hits through an SHM queue drained every ≤ 5 ms by a
  leader, adding up to 5 ms before a hit reaches the runtime.
- **No clock skew**: every node reads the same clock.
- **Network**: no bandwidth limit, no send-buffer limit, no MTU
  fragmentation, latency independent of load. Gilbert-Elliott state
  advances per datagram on a link, not per unit time.
- **Workload**: one rule, one request = one hit, load balancer uniform over
  running pods; a crashed pod is removed from rotation immediately rather
  than failing requests until a readiness probe notices.
- **Convergence** is defined on bucket counts, not origin cells: the
  aggregate store does not expose origins. The `j`-th hit of a bucket
  counts as converged at a pod once the pod's count for that bucket reaches
  `j`, even if a later hit from another origin got there first. It measures
  how long each pod's count lags the truth as of each admission.

### Deviations from the study specification

- **`long-window` runs 30 req/s, not 300 req/s, and measures 1,200 s, not
  four windows.** At 300 req/s a 1 h window holds about 1.1 M live cells per
  pod (66 M across 60 pods, about 170 times the reference deployment's peak
  live-cell count); a probe of `main` needed more than 7 GB and about half
  an hour per 600 virtual seconds, so one run would take hours. 30 req/s
  still holds about 110 k live cells per pod across 60 one-minute buckets
  per key. The first full window is warmed by real traffic, so the state is
  at steady size when measurement starts; four further windows would add
  four virtual hours per run for little new information, so the
  measurement covers 20 bucket boundaries instead.
- **Admission follows the server adapter**, not the NGINX one (see
  Limitations). The difference is the NGINX SHM drain delay of at most
  5 ms.
- **Production gossip settings come from `gabion-server`**
  (`GossipSettings::default().into_runtime_config`) rather than from fields
  set in the harness, so each solution runs with the configuration it would
  ship and tunes it in its own code.
- **The scenario suite moved behind the `legacy` feature** so the study
  does not depend on `gabion::gossip::sim`.

