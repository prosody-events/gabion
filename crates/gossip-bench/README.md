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

# Compare solutions. Refuses mixed harness hashes and dirty results.
python3 crates/gossip-bench/judge/judge.py ../study-results --baseline main \
    --out ../study-report
```

`matrix.py` writes `<out>/<solution>/<regime>/seed-<n>.json`, stamped with
the harness tree hash (`git rev-parse HEAD:crates/gossip-bench`), the
solution label, branch and commit, plus the process's wall time, CPU time
and peak RSS from `wait4` (host-dependent; everything else is not). It always
builds the release binary first (a no-op when current), takes the binary's
path from cargo's own report of that build, and runs a private copy under
`--out`, so a result can never carry numbers from a binary older than its
stamped commit and a matrix building in another worktree cannot replace the
binary under a running one. It
refuses to run when the worktree has uncommitted changes (`--allow-dirty`
overrides, and the judge then rejects the results), and resumes only over
results with an identical clean stamp. The judge refuses mixed harness
hashes (except hashes whose trees differ only in `judge/judge.py`,
`judge/test_*.py` or `README.md`, which it checks with `git diff-tree` and
lists in the `summary.md` header; `judge/matrix.py` builds and drives the runs,
so it is not exempt), results with an older `gabion-study/*` schema, and
several commits under one solution label, and writes
`summary.md` and `summary.csv` (every metric per regime and solution).
The seed values are modelled as normal with the prior 1/sigma on the spread,
cut at the metric's rounding noise: a count, or anything else reported on a
grid of step h (one hit of the limit for a percentage, the 100 ms probe for
the time to heal, and so on; `RESOLUTION` in `judge.py` lists them), is known
only to within h / 2, so sigma is at least h / sqrt(12). Cells show the mean
across seeds and the equal-tailed credible interval for it (95 % by default,
`--level` changes it); when the seeds spread well past the rounding noise this
is the Jeffreys posterior, Student t with n - 1 degrees of freedom. Seeds that
tie on a count therefore keep about one step of doubt (three seeds at p99
staleness 2 read `2 [1.25, 2.75]`, not `2 [2, 2]`). One seed, or seeds that
agree exactly on a metric without a grid, give no interval: the posterior is
improper. The model ignores bounds, so a count that ties at 0 gets an interval
reaching below 0. Against the baseline, paired over the seeds valid for both,
the judge models the log ratio when every value is positive and reports the
geometric-mean ratio with its credible interval and P, the posterior
probability that the solution is better (lower is better except for
`admitted`); with a zero or negative value it reports the mean difference
instead. The rounding noise carries over to the paired values: three tied
seeds one count apart give P near 0.97, not 1. Regimes that cut the network get a
second table of [partition phases](#metrics), whose time-to-heal cell also
shows how many seeds healed (`healed k/n`); `pareto.md` (total bytes
against limited-key p99 staleness, worst-key over-admission, and p99
convergence including never-converged hits, each point a mean over the seeds
valid for every solution in that table; when no seed is valid for every
solution the points are listed without a front, and a metric no solution has
in that regime, such as hot-key over-admission in `idle`, is marked not
applicable), `flags.md` (non-invalidating saturation and
the gabion warnings behind it), and `rejected.md`. A (regime, solution) with
INVALID seeds shows `INVALID k/n` in every cell rather than silently
averaging the survivors; one with no valid seed stays in the tables and is
excluded from the Pareto tables with a note. Judge tests:
`python3 -m unittest discover -s crates/gossip-bench/judge`.

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

**Receive path.** A node reads its buffer at a bounded rate: each datagram
read occupies the node's receive path for `rx_ns_per_datagram +
rx_ns_per_byte × payload` (defaults 1.5 µs + 3 ns/B, identical for every
solution and overridable per regime). The calibration: a 1,378 B datagram,
`main`'s mean, costs 5.6 µs, which is the whole-process CPU time per
delivered datagram measured for `main` at `prod-peak` on the reference host
(an overestimate of receive work, since it includes encoding and the
harness, but excluding the `recvmsg` syscall the simulation never makes).
Datagrams that arrive meanwhile wait in the receive buffer, so a burst
faster than about 170 k full frames per second into one node fills its
buffer (about 145 full frames) and drops. Everything else a node does
(ticks, sends, admissions) takes no virtual time.

**Byte accounting.** Every datagram is counted once, at send time, for the
whole network: UDP payload + 28 B IPv4/UDP header. Totals are reported for
the measurement window and for the whole run, with packets, cross-AZ bytes,
and bytes and packets dropped by cause (loss, partition, receive buffer, no
receiver). A datagram counts as delivered when its node reads it, so every
sent datagram is exactly one of delivered, dropped (by one cause), or still
in flight or queued when the run ends; datagrams queued at a pod that stops
count as dropped with no receiver. Derived figures divide the window total by the integral of the
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
address) at `at + i × interval + U(0, 1 s)` and stops the oldest surviving
initial pod gracefully `overlap` after its replacement starts. The offset
keeps joiners' ticks out of phase with each other, as for initial pods:
without it, a 500 ms rollout interval that equals a solution's tick
interval makes every joiner send its per-tick burst at the same instant,
filling receive buffers for reasons that depend on the solution's tick
length rather than its design; members learn of the departure after
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
cells within two buckets of their leaving the window fits; dirty rings are
the production sizes (`STORAGE_LOCAL_DIRTY_CAPACITY`,
`STORAGE_FORWARDED_DIRTY_CAPACITY`), or, where the cell store is above
`STORAGE_MAX_CELLS`, the production ratio to it, never less; node and peer
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
  limit. Because the signed mean lets an over-count offset an under-count,
  the under-count `max(0, error)` and the over-count `max(0, view − truth)`
  are also reported separately (`under_mean`, `over_mean`, `over_max`,
  `over_time_fraction`); a correct solution never over-counts. Tracked keys are every hot and burst key ("limited") plus a seeded
  sample of `tracked_sample_keys` sites ("sampled"), reported separately.
- **Residual error** is the same difference restricted to buckets that
  closed at least 1.5 s ago: error that is no longer in flight. Sampled
  every second at +500 ms.
- **Convergence time** of an admitted hit on a tracked key, the `j`-th in
  its bucket, is the time until every pod that was running when it was
  admitted, and still runs, has a count of at least `j` for that bucket.
  Nodes report counts, not origins, so this is count-based. With a
  partition, hits are ranked within the origin's side of the cut, and an
  observer on the other side needs `j` plus every hit its own side has
  admitted in that bucket so far, so a far pod's own hits are never taken
  for hits that crossed. Cross-cut convergence is therefore a conservative
  bound (it also waits for the pod to catch up on its own side), and
  same-side convergence after the heal can be credited early by hits that
  crossed. Hits whose bucket leaves the window first count as never converged;
  those still pending at the end are reported as censored. The plain
  quantiles cover converged hits only and so are right-censored; the
  `*_incl_never_ms` quantiles rank every hit, valuing a never-converged hit
  at the time from its admission until its bucket left the window (a lower
  bound), and are the ones to compare across solutions.
- **Rate-limit accuracy** per limited key: at every bucket boundary the
  cluster-wide admitted total of the trailing window minus the limit
  (over-admission: max, mean, share of windows over), plus offered,
  admitted, rejected and **false rejects** (rejected although the true
  cluster total plus the request was within the limit) in the window.
- **Churn**: for every pod that joins during the window, the time until its
  summed tracked-key totals first reach 95 % and 99 % of truth; the time
  until its summed under-count first falls to the mean over established
  pods (parity: catch-up net of the steady-state error every pod carries,
  checked whenever the joiner's error or the truth changes); and its
  integrated summed under-count over the first window after joining
  (hit-seconds).
- **Partition phases**, for a regime that schedules a partition
  (`churn.partitions`: one zone cut off from the rest during `[at_s,
  heal_s)`; the metrics follow that schedule, never the regime's name). The
  window splits into **before** the cut, **during** it, and **after** the
  heal, and pods into those inside the cut zone and the rest. Per phase:
  convergence of the hits admitted in it, as `same_side` (time until every
  pod on the origin's side covers the hit) and `cross_side` (every pod on the
  other side; a conservative bound, see above), with never-converged hits
  ranked as in the whole-run figures;
  and staleness (limited and sampled keys) for observers inside the zone and
  outside it, time-weighted over the part of each exposure inside the phase.
  A pod's view is not resolved by origin, so staleness can be split by the
  observer's side only. **Time to heal** runs from the heal instant until no
  pod that was running at the heal is missing a hit of any bucket that closed
  before it and is still in the window: every cross-cut count exact, with no
  tolerance. It is probed every 100 ms (so quantized to it). Expiry is never
  repair: a hit that leaves the window while some pod still lacks it makes the
  heal impossible, and the time is then `null`, as it is when the run ends
  first. `unrepaired_at_expiry_hits` is the mean hits per (pod, tracked key)
  lost that way (0 for a full repair), so `null` with 0 means the run ended
  mid-repair and `null` with more than 0 means repair did not finish in time.
  `deficit_at_heal_hits` is the mean number of hits missing per (pod, tracked
  key) at the heal instant, the repair the solution owes. Regimes with no
  partition report `partition: null`.
- **Health**: cell-store, rule- and node-dictionary full rejects, oversize
  datagrams, decode rejects, runtime exits, failed records, aggregate rows
  for keys the regime never issued, and admin snapshots that failed or a
  node that never answered one each make the run **invalid**. Also
  reported: peak active cells and maximum send-queue depth.
- **gabion warnings**: a run-scoped `tracing` subscriber
  (`src/harness/warnings.rs`) tallies every WARN and ERROR event gabion
  logs, per `(level, target, message)` and per node (the node being polled),
  with the largest `*_total` field each node reported. gabion logs most
  saturation only on power-of-two totals, so these are lower bounds
  (within 2×). Messages containing "dirty ring overflowed" (local and
  forwarded dirty rings overwriting entries before gossip sent them) and
  any ERROR event **flag** the run without invalidating it: overflow can be
  a genuine property of a design at a given load. The judge shows the
  counts and lists every flag in `flags.md`. A solution that keeps
  gabion's `CellStore` keeps these messages; one that replaces it should
  log the same phrase for the same condition.

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
| `churn-rollout` | 80 (~92 at most mid-wave; 79 after the crash) | as `prod-typical` | as above | 20 + 90 s | 45-pod rollout from 30 s, one pod every 500 ms + `U(0, 1 s)`, 5 s overlap; silent crash at 75 s |
| `hot-key` | 183 | 1,800 req/s sites | as above | 20 + 60 s | 20 hot keys, each offered 5x its limit (50 req/s) uniformly across nodes; no burst; stresses over-admission |
| `az-partition` | 80 | 650 req/s sites | as above | 20 + 90 s | zone 0 isolated from the other two zones for 30 s from 40 s, then healed (data only: `churn.partitions`) |
| `long-window` | 60 | 30 req/s | 2,000 / 1 h, 1 min buckets | 3,600 + 1,200 s | hot keys; 90 req/s burst for 60 s; first window warmed by real traffic |

### Validation

The simulator is checked against results known independently of gabion's
protocol (`src/harness/validation/`, run with `cargo nextest run --release -p
gossip-bench validation --no-capture` to print the figures below). Reference
nodes, a few lines each, run on the harness's own network model
(`net.rs`), event queue and single-threaded executor (`world.rs`) through the
same `SimSocket`, clock and ticker a gabion node uses; the byte-accounting
check runs a real gabion cluster under the full `Driver`. Seeds are fixed, so
every run is bit-identical; the whole suite takes about 11 s in release on the
reference host, 7 s of it the 300-node epidemic. Every measured proportion is
reported as a Bayesian posterior. A count of `k` events in `n` independent
trials has the Jeffreys posterior Beta(`k` + 1/2, `n` - `k` + 1/2); the tables give
its median and equal-tailed 95 % credible interval, and whether the closed-form
value lies inside that interval. A measured mean (latency, burst length) has the
Jeffreys posterior for a normal mean: Student t with `n` - 1 degrees of freedom,
centred on the sample mean, with scale `s/sqrt(n)` for sample standard
deviation `s`. The seeds are fixed, so each figure is one draw of the
simulator and the interval carries only that draw's sampling noise.

Separately, each test asserts a fixed acceptance band: the measured value must
lie within 4 binomial standard deviations of the closed-form value, where
the standard deviation is `sqrt(p (1 - p) / n)` for a proportion and the
model's own standard deviation divided by `sqrt(n)` for a mean (the
autocorrelated cases use the chain's asymptotic variance, below). The band is a
pass/fail rule about 2x wider than the 95 % interval, so it is looser than the
credible intervals reported here; it is not itself evidence. The epidemic
check's closed-form value is approximate (below), which the band's slack
absorbs.

**1. Epidemic reach.** An end-to-end delivery and event-loop sanity check,
not a test of the network model: the reference nodes choose their own targets
and the harness only carries their loss-free datagrams (loss and latency are
checked in 2 and 3). Infect-and-forward-once push gossip: the origin sends
the message to `f` distinct nodes chosen uniformly from the others; every
node does the same once, on first receipt, and then falls silent (non-integer
`f` by randomised rounding, so the mean is exact). Kermarrec, Massoulié and
Ganesh ("Probabilistic Reliable Dissemination in Large-Scale Systems", IEEE
Trans. Parallel and Distributed Systems 14(3), 2003) show that with `f = ln n
+ c` the probability that every node is reached tends to `exp(-exp(-c))`.
5,000 independent runs per cell.
The limit is asymptotic; the same Poisson argument at finite `n` with distinct
targets gives `exp(-(n-1)(1 - f/(n-1))^(n-1))`, the "finite-n" column. That
value is itself a Poisson approximation. An independent plain-Python
simulation of the same protocol (`n = 100`, 40,000 trials per cell, no harness
code; the script is not part of the repository) reached every node in 15,959,
29,363 and 36,004 trials for `c = 0, 1, 2`: posterior medians 0.3990 [0.3942,
0.4038], 0.7341 [0.7297, 0.7384] and 0.9001 [0.8971, 0.9030]. Its `c = 0`
interval excludes the finite-n value 0.4121, so that value is about 0.013
above the true probability there; at `c = 1, 2` the finite-n value lies
inside the interval. The independent run's median lies inside each of the
harness's three `n = 100` intervals. The harness
must land within 4 binomial standard deviations of the finite-n value and
within 0.05 of the limit.

| n | c | runs reaching all (of 5,000) | posterior median [95 % credible interval] | finite-n | finite-n inside? | KMG limit | limit inside? | acceptance band (±) |
|---|---|---|---|---|---|---|---|---|
| 100 | 0 | 2,019 | 0.4038 [0.3903, 0.4175] | 0.4121 | yes | 0.3679 | no | 0.0278 |
| 100 | 1 | 3,650 | 0.7300 [0.7176, 0.7422] | 0.7343 | yes | 0.6922 | no | 0.0250 |
| 100 | 2 | 4,463 | 0.8926 [0.8838, 0.9010] | 0.8991 | yes | 0.8734 | no | 0.0170 |
| 300 | 0 | 1,978 | 0.3956 [0.3821, 0.4092] | 0.3894 | yes | 0.3679 | no | 0.0276 |
| 300 | 1 | 3,543 | 0.7086 [0.6959, 0.7211] | 0.7120 | yes | 0.6922 | no | 0.0256 |
| 300 | 2 | 4,444 | 0.8888 [0.8799, 0.8973] | 0.8852 | yes | 0.8734 | no | 0.0180 |

All six intervals contain the finite-n value. The asymptotic limit lies
outside every interval, 0.015 to 0.038 below the posterior median, as expected
of a limit evaluated at `n` = 100 and 300; the harness only requires it to be
within 0.05.

**2. Loss.** One million probes, 1 ms apart, between two nodes, counted at
the receiving application; the network's own drop counter must equal the
probes missing at the receiver.

| model | quantity | posterior median [95 % credible interval] | closed form | inside? | acceptance band (±) |
|---|---|---|---|---|---|
| i.i.d., `p = 0.01` (9,933 lost) | loss rate | 0.00993 [0.00974, 0.01013] | 0.01 | yes | 0.00040 |
| i.i.d., `p = 0.05` (49,804 lost) | loss rate | 0.04980 [0.04938, 0.05023] | 0.05 | yes | 0.00087 |
| i.i.d., `p = 0.20` (199,983 lost) | loss rate | 0.19998 [0.19920, 0.20077] | 0.20 | yes | 0.00160 |
| Gilbert-Elliott, `p = 0.01, r = 0.19`, all lost while bad (the `loss-burst` chain) (50,425 lost) | loss rate | 0.05043 [0.04915, 0.05172] | `p/(p+r)` = 0.05 | yes | 0.00262 |
| same | mean burst length (9,459 bursts, sample sd 4.761) | 5.331 [5.235, 5.427] | `1/r` = 5.263 | yes | 0.195 |
| Gilbert-Elliott, `p = 0.02, r = 0.10`, half lost while bad (81,617 lost) | loss rate | 0.08162 [0.08014, 0.08311] | `q p/(p+r)` = 0.08333 | no | 0.00306 |

For the i.i.d. rows the Jeffreys Beta posterior applies directly, and the
acceptance band is 4 binomial standard deviations. Gilbert-Elliott losses are
autocorrelated (`rho = 1 - p - r`), so a million probes carry fewer than a
million independent trials: the loss-rate rows use the Beta posterior at the
effective sample size `n_eff = n e (1 - e) / v`, where `e` is the closed-form
rate and `v` the chain's asymptotic per-probe variance of the loss indicator
(111,111 for the first chain, 130,435 for the second), with the observed
fraction scaled to match; the acceptance band uses the same variance. The burst
mean uses the Student t posterior above, with the sample standard deviation
of the burst lengths; the geometric distribution's variance is `(1 - r)/r^2`.

The one interval that excludes its closed form is the second Gilbert-Elliott
loss rate: its posterior puts the rate 0.00171 below `q p/(p+r)` with
posterior probability 0.988 that the rate is below the closed form, and
its 99 % credible interval [0.07968, 0.08358] contains it. The test's seed is one draw; as a
one-off check outside the suite, seeds 2 to 21 of the same configuration lost 81,617
(seed 1) and 82,062 to 85,294 probes, and the posterior of the mean loss rate
across all 21 seeds (Student t, 20 degrees of freedom) is 0.08320 [0.08280, 0.08359], which contains 0.08333; for the first chain the
same 21-seed posterior is 0.05014 [0.04980, 0.05047], which contains 0.05. The
seed-1 draw of the test is the low one of the 21.

**3. Latency.** 200,000 probes per path with receive service time set to
zero, so the delay measured at the read is the network's alone, against the
model's `base + U(0, jitter)`: the default intra- and cross-zone latencies,
and a second configuration with very different bases and jitters. The mean's
Student t posterior uses the sample standard deviation (28.9 µs, 115.2 µs,
866.1 µs and 2,309.9 µs, close to `jitter/sqrt(12)`). The tests require the
mean to lie within 4 standard deviations of the mean (`jitter/sqrt(12)/sqrt(N)`)
of `base + jitter/2`, every delay inside `[base, base + jitter]`, and the
Kolmogorov-Smirnov distance `D` between the empirical and the uniform
distribution below the fixed bound `1.95/sqrt(N)` = 0.00436, the value the
Kolmogorov distribution gives for a uniform sample at the 0.1 % tail.

| configuration | path | mean (µs): posterior median [95 % credible interval] | expected (µs) | inside? | acceptance band (± µs) | observed range (ns) | configured range (ns) | KS D |
|---|---|---|---|---|---|---|---|---|
| default | intra-AZ | 250.10 [249.97, 250.22] | 250.0 | yes | 0.26 | 200,000..300,000 | 200,000..300,000 | 0.00225 |
| default | cross-AZ | 1,000.14 [999.63, 1,000.64] | 1,000.0 | yes | 1.03 | 800,001..1,200,000 | 800,000..1,200,000 | 0.00193 |
| slow | intra-AZ | 3,500.41 [3,496.62, 3,504.21] | 3,500.0 | yes | 7.75 | 2,000,003..4,999,989 | 2,000,000..5,000,000 | 0.00164 |
| slow | cross-AZ | 19,002.04 [18,991.92, 19,012.17] | 19,000.0 | yes | 20.66 | 15,000,012..22,999,988 | 15,000,000..23,000,000 | 0.00182 |

**4. Byte accounting.** The `tiny` regime (5 pods, rollout of 2, one crash,
5 % loss, 30 s) runs under the full driver with every node's socket wrapped
in a tally that adds `payload length + 28` for each datagram handed to it
and for each one read from it, independently of the network's counters. The
totals are equal exactly: 5,251 datagrams, 6,095,936 B over the run (tally
6,095,936), 5,687,704 B in the measurement window (tally 5,687,704), and
5,249,280 B read (tally 5,249,280); the run sent bytes equal payload bytes
plus 28 per datagram, and the run includes datagrams lost, dropped at
stopped pods and still in flight.

**5. Receive buffer.** A datagram is dropped exactly when the bytes already
queued plus its own exceed the buffer. (a) Against a queue-of-lengths model
over 300,000 random arrivals and reads into an 8,192 B buffer (164,947
arrivals, 32,678 dropped, 44 of them landing exactly on the capacity and
accepted), the drop decision and every read agree on every operation; the
boundary is also checked by hand (filling to the byte is accepted, one byte
over is dropped, a read frees exactly its length). (b) Through the event
loop, 50 probes of 1,000 B arrive within 50 ns of each other at a node that
needs 5 µs to read one, with a 10,000 B buffer: the first is read on arrival,
the next ten fill the buffer to the byte, the other 39 are dropped (11 read,
39 dropped, as predicted).

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
  `max_send_pending_depth`, before the node next goes idle (a snapshot
  that is refused or unanswered invalidates the run).
- Saturation is logged through `tracing` at WARN or ERROR, with a
  `*_total` field carrying the running count; dirty-ring overflow keeps the
  phrase "dirty ring overflowed".
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

- **Processing is instantaneous except the receive path.** Only reading a
  datagram costs virtual time (see Receive path), and its cost is a fixed
  estimate rather than a measurement of each solution's decoder. Ticks,
  sends and admissions are free, so CPU saturation and scheduling delay are
  not modelled, and a design that spends more CPU per received byte than
  the estimate is not charged for it. The receive buffer is charged payload
  bytes, not the kernel's larger per-datagram `truesize`, so it holds more
  small datagrams than Linux would. Process CPU time and peak RSS are
  reported as a noisy secondary signal.
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
- **The harness reads gabion's `tracing` output.** Saturation that gabion
  reports only as a log line is captured into the result. Its `Visit`
  implementation takes a trait object (`fmt::Debug`), so `warnings.rs` joins the
  NGINX logger in the Makefile's `dyn` hygiene exemption.

