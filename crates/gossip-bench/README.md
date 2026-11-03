# gossip-bench

The study harness: a deterministic simulator of a gabion cluster under a
production-shaped workload, scored against the true count. On identical
traffic, network and churn it answers one question about a gossip design:
what does it cost in bytes, and what does it buy in accuracy? A contributor
runs it as a dev tool; it is not shipped. The Python judge compares runs and
the regression gate applies pass rules to a change against the measured
baselines. The study that used it, with its method and results, is in
[`docs/study/README.md`](../../docs/study/README.md); the instrument as the
study froze it is `sim/harness` at `896ea20`.

Every arm (the `main` baseline, the measured `imp-az-hier-nopk` branch, the
checkout under test) runs the same simulator, workload and judge: the
measurement core under `src/harness/` is compared by tree hash and must be
identical (see [Reading the judge's output](#reading-the-judges-output)). Only
`src/harness/adapter.rs` names gabion's own types, so a change to the library
edits that one file (see [The adapter contract](#the-adapter-contract)).

## Running one regime

```sh
cargo run --release -p gossip-bench --     --regime crates/gossip-bench/regimes/prod-typical.json --seed 1 --out r.json
```

Without `--out` the result JSON goes to stdout. `--require-valid` writes the
result first, then exits non-zero when the run broke a health invariant (see
**Health** under [Metrics](#metrics)). A run is a pure function of regime, seed
and binary. Regimes are the files in `regimes/`.

## The CI smoke

`make sim-smoke` runs the judge's Python tests
(`python3 -m unittest discover -s crates/gossip-bench/judge`), then the
`low-3` regime (3 pods, 80 virtual seconds) with seed 1 and `--require-valid`,
and fails on an invalid run. CI runs it after `make unit`. Measured locally on
an Apple-silicon laptop, with the dev profile already built: about 8 s (the judge's tests about 5 s, the run about 2 s).

## The regression gate

A change is judged against two baselines, all with the same core:

| Arm | What it is |
|---|---|
| `main` | the tree that landed the harness (`H1`), measured with the current core |
| `imp-az-hier-nopk` | the study's measured branch `sim/imp-az-hier-nopk` (`328f7cf`) |
| `zone` | the checkout under test |

```sh
# Once per core: both baselines, 13 regimes x 3 seeds (about 11,100 CPU-seconds).
make sim-baselines H1=<sha> SIM_OUT=/abs/path/sim
# Per change: the checkout as the zone arm, then the pass rules.
make sim-gate SIM_OUT=/abs/path/sim
```

Give both the same absolute `SIM_OUT` (the default `target/sim` is per
checkout). `sim-baselines` builds each baseline in a worktree under
`SIM_OUT/worktrees/` from its own commit, overlaid with the checkout's current
core and with `H1`'s `adapter.rs` and `Cargo.toml`, so every arm runs the same
simulator on its own library code. Keep the worktrees: their overlay commits
back the judge's `git diff-tree`. The measured branch costs about 1,430
CPU-seconds per seed; `--jobs` (`SIM_JOBS`, default 2) is limited by memory,
since `prod-4x` needs several GB per run.

`gate.py` runs `judge.py` over the three arms once per comparator (`main`,
then `imp-az-hier-nopk`) and classifies each paired comparison with
`judge/verdict.py`:

- **better**: P(better) of at least 0.95 and the estimate below the band;
- **worse**: P(better) of at most 0.05 and the estimate above the band;
- **tie**: the whole credible interval inside the band;
- **credible <5 %**: P beyond a threshold but the estimate inside the band;
- otherwise **unsettled**.

The band is the ratio [0.95, 1.05] (or, on the difference scale, 5 % of the
comparator's mean). The rules, with the study's ten error metrics
(`conv_p50_incl_never_ms`, `conv_p99_incl_never_ms`, `conv_never_frac`,
`stale_limited_p99_pct`, `over_max_pct`, `over_mean_pct`,
`churn_parity_max_ms`, `part_deficit_at_heal`, `part_time_to_heal_ms`,
`part_unrepaired_at_expiry`) and the two cost metrics (`bytes_total`,
`cross_az_bytes`):

| Rule | Passes when |
|---|---|
| G-v | every arm has 3 valid seeds in every regime |
| G-a1 | no error metric is worse than `imp-az-hier-nopk` |
| G-a2 | `az-partition` heals on all 3 seeds |
| G-a3 | in `prod-typical`, `prod-peak` and `prod-4x` each cost metric is credibly lower than `imp-az-hier-nopk` (P of at least 0.95, ratio below 1), and no cost metric is worse in any regime |
| G-a4 | `drop_rcvbuf_pkts` is never worse than `imp-az-hier-nopk` |
| G-b1 | each cost metric is better than `main` in every regime |
| G-b2 | every error metric `imp-az-hier-nopk` beats `main` on is still better than `main` |
| G-b3 | no error metric is worse than `main` |

`gate.py` writes `gate.md` into its `--out`: the core hash, the broken rules
or "Every rule passes.", the measured cost ratios of G-a3 beside the 0.93 the
design predicted, and the `git diff` of `adapter.rs` and `Cargo.toml` from the
baselines (both carry H1's copy) to zone. Read that diff: the adapter is outside the core hash, so
the diff is where a change to what the adapter does shows. Between the cost
ratios and the diff sits the cost-model section below. It exits 1 when a rule
fails.

**The cost model (not gating).** gabion's gossip defaults are sized with
the equations below, for a steady cluster of `N`
nodes in `Z` zones at `Λ` admitted hits per second (`λ = Λ / N` per node),
heartbeat `T`, and a rule of limit `L`, window `W` and bucket `b`:

```
ε = max(1, ⌊L · β / (10⁴ · (N − 1))⌋)                   (3.2) per-rule error budget
R_own = λ / (ε + 1) + (1 − e^(−2λT)) / (4T)   (ε = 1)   (6.2) own rounds per node-second
D = R_own · (N − 1) + a                                 (6.3) packets per node-second
e^(−λt) · (1 − t/T) = 0.02                              (6.5) t: convergence p99
A ≈ Λ · (W + b)                                         (6.10) live cells, an upper bound
p_a^n_a, p_a = 1 − (1 − p)², n_a = ⌊T_u / T⌋            (6.5.8) false suspicion per late answer
```

`a` is the acknowledgements sent alone, for which the design gives no
equation; `β` is `gossip.target_err_bps`, `p` the packet loss and `T_u`
`gossip.suspect_after`. Under burst loss the false-suspicion chance is
`(1 − p_bad_to_good)^(n_a − 1)` per loss burst.

Per regime, `gate.md` sets the zone arm's measured mean and credible interval
beside what these predict: packets, bytes and cross-zone bytes per node per
second against (6.3), convergence p99 against (6.5) and peak live cells
against (6.10). Each prediction reads the regime's JSON, the zone arm's
measured admitted rate, and the defaults and wire sizes named at the top of
`gate.py`; `test_gate.py` checks each against its Rust source, except the
16-byte width of a u128. The predictions leave out `a`, so a measured packet
rate somewhat above (6.3)'s data term is expected; bytes count `R_own · (N − 1)`
data packets, each one header, one rule and one origin plus `λ / R_own` cells,
and cross-zone bytes the `R_own · (Z − 1)` of them sent to gateways. A metric
more than 5 % off its prediction, as printed to one decimal, is named under
its table with the setting the equation sizes (the quantum or
`gossip.tick_interval` for (6.3), the tick for (6.5), `STORAGE_MAX_CELLS` for
(6.10)), and the owner decides whether the default or the model is wrong.
(6.10) is an upper bound, so only a measurement above it misses. A regime with
loss, churn or a partition is outside the equations: its numbers are shown and
never counted as misses. Cross-zone bytes per node per second is the judge's
`cross_az_bytes_per_node_s`, the window's cross-AZ bytes over the running-pod
integral, as `bytes_per_node_s` divides. Peak live cells is the judge's
`peak_cells`, the most active cells any node reported in the core's health
snapshots, taken every 5 s. `gateway_failovers` reads `missing`: the result is
the core's and does not carry it. Beside it stands (6.5.8)'s chance that an
episode ends in a false suspicion, at `n_a` attempts and at its floor
`n_a − 1`, which `gossip.suspect_after` sizes. A missing value is never a
pass. The section never changes the exit code; `gate.py` prints how many
comparisons miss.

The core hash is `judge.core_hash(tree, repo)`: SHA-1 over the harness tree's
entries without the paths the judge exempts. It is unchanged by edits to
`README.md` and the other exempt files, so it can be recorded here. Recompute it
with:

```sh
python3 -c 'import sys; from pathlib import Path; sys.path.insert(0, "crates/gossip-bench/judge"); import judge; print(judge.core_hash(sys.argv[1], Path(".")))' "$(git rev-parse HEAD:crates/gossip-bench)"
```

Core hash: `1ed9ff8c988cbd895bff19992388818703495b41`. A new or
changed regime, or any other edit to the core, changes it, requires rerunning
`make sim-baselines`, and updates this line.

## Reading the judge's output

```sh
# A matrix for the checked-out arm: every regime, seeds 1..3, two at a time.
# Builds the binary, stamps each result, resumes after interruption.
python3 crates/gossip-bench/judge/matrix.py --solution zone --seeds 3     --jobs 2 --out ../results

# Compare arms. Refuses mixed harness hashes and dirty results.
python3 crates/gossip-bench/judge/judge.py ../results/main ../results/zone     --baseline main --out ../report
```

`matrix.py` writes `<out>/<solution>/<regime>/seed-<n>.json`, stamped with
the harness tree hash (`git rev-parse HEAD:crates/gossip-bench`), the
solution label, branch and commit, plus the process's wall time, CPU time
and peak RSS from `wait4` (host-dependent; everything else is not), and, beside
the stamp, `finished_at` (UTC, ISO 8601). It always builds the release binary
first (a no-op when current), takes the binary's path from cargo's own report
of that build, and runs a private copy under `--out`, so a result can never
carry numbers from a binary older than its stamped commit and a matrix
building in another worktree cannot replace the binary under a running one.
It refuses to run when the worktree has uncommitted changes
(`--allow-dirty` overrides, and the judge then rejects the results), and
resumes only over results with an identical clean stamp; `finished_at` is not
part of that comparison, so a rerun counts a finished run as done.

The judge refuses mixed harness hashes, except hashes whose trees differ only
in the `EXEMPT` paths (`README.md`, `Cargo.toml`, `src/harness/adapter.rs`,
`judge/judge.py`, `judge/verdict.py`, `judge/gate.py` and `judge/test_*.py`),
which it checks with `git diff-tree` and lists in the `summary.md` header;
`judge/matrix.py` builds and drives the runs, so it is not exempt. It also
refuses results with an older `gabion-study/*` schema and several commits
under one solution label, and writes `summary.md` and `summary.csv` (every
metric per regime and solution).
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
shows how many seeds healed (`healed k/n`). `flags.md` lists non-invalidating
saturation and the gabion warnings behind it, and `rejected.md` the results
the judge refused. A (regime, solution) with INVALID seeds shows
`INVALID k/n` in every cell rather than silently averaging the survivors; one
with no valid seed stays in the tables as INVALID.

The judge's `rss_mib` and `cpu_s` rows report each regime's cost.

## Adding a regime

Add `regimes/<id>.json`; the id is the file stem. `every_regime_file_parses`
loads every file in the directory and `Regime::validate` rejects zero or
inconsistent values, so a malformed file fails `cargo nextest run -p
gossip-bench`. Describe it in the [Regimes](#regimes) table. A regime is part
of the core: adding or changing one changes the core hash, so rerun `make
sim-baselines`.

## Model

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
gossip draws (the zone arm draws each node's id from it). A solution that sends more datagrams consumes more of the
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
balancer would place it. The adapter (`src/harness/adapter.rs`) hashes keys with
`gabion::rules::hash_key` and builds the rule through
`gabion::rules::RuleDef::build` and `RuleTable::new`, as the production adapters
do.

**Admission** follows `SharedLimiter::decide`
(`crates/server/src/lib.rs`): read the pod's window total, reject iff
`total + hits > limit`, otherwise record the hit with
`GossipClient::record(fingerprint, key_hash, now_ms / bucket_ms, 1, limit,
now_ms)` and wait for the acknowledgement (the runtime has applied it to the
pod's aggregate), where `gabiond` hands the hit over with `try_record` and
does not wait. Only admitted hits are recorded. The offered load is
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

**Aggregate store.** Each pod's `AggregateStore` (`NodeStore` in the adapter) is
the library's aggregate table (`gabion::aggregate::Aggregate`), sized from the
pod's `cell_capacity` as `gabiond` sizes it from `storage.max_cells`: deltas
add, expirations subtract the origin's last count, the window total sums the
live buckets, exactly as both adapters read it on admission.

**Sizing.** Capacities are derived from the regime, identically for every
arm. The core fixes `cell_capacity`: 1.5 × an upper bound on the cells one
pod can hold (each request creates at most one `(key, bucket, origin)` cell
living one window plus two buckets, and a key holds at most one cell per pod
per bucket), at least 16,384, rounded up to 1,024; any design that expires
cells within two buckets of their leaving the window fits. Because the
capacity decides whether a run is valid, it is part of the core. The adapter
derives the rest from the library's own defaults: node and peer
dictionaries at the production defaults or the regime's pod count + 16,
whichever is larger. Gossip settings are the production defaults
(`Settings::default()` in `gabion::settings`); the adapter validates them
once, as `gabiond` does.

## Metrics

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
  reported: peak active cells.
- **gabion warnings**: a run-scoped `tracing` subscriber
  (`src/harness/warnings.rs`: `tracing_subscriber::fmt` writing JSON lines
  into an in-memory writer) tallies every WARN and ERROR event gabion logs,
  per `(level, target, message)` and per node (the node being polled), with
  the largest `*_total` field each node reported. gabion logs most
  saturation only on power-of-two totals, so these are lower bounds (within
  2×). Any ERROR event **flags** the run without invalidating it, and so
  does any captured line that does not read as a WARN or ERROR event with a
  target and a message (`unparsed_lines`). The judge shows the counts and
  lists every flag in `flags.md`.

## Regimes

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

## Validation

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

## Limitations

- **Processing is instantaneous except the receive path.** Only reading a
  datagram costs virtual time (see Receive path), and its cost is a fixed
  estimate rather than a measurement of each solution's decoder. Ticks,
  sends and admissions are free, so CPU saturation and scheduling delay are
  not modelled, and a design that spends more CPU per received byte than
  the estimate is not charged for it. The receive buffer is charged payload
  bytes, not the kernel's larger per-datagram `truesize`, so it holds more
  small datagrams than Linux would. Process CPU time and peak RSS are
  reported as a noisy secondary signal.
- **Admission.** Admission follows the gRPC server adapter. The NGINX adapter
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

## The adapter contract

The core (everything under `src/harness/` except `adapter.rs`) names only the
stable trait surface of the library: `gabion::gossip::{Clock, GossipTransport,
Ticker}` and `gabion::discovery::{Peer, PeerEvent}`. It implements those traits
for the simulated network, clock and discovery stream, and keeps every other
line about gabion in `src/harness/adapter.rs`:

- `Launcher::new(regime, sizing, world, keys, seed)` (returning the
  `Launcher`, or since H1 a `Result` of it) and
  `Launcher::launch(idx) -> Launched`: build one node (identity, gossip
  configuration, `CellStore`, runtime) and its aggregate store;
- `Node::record(key, bucket, now_millis)` (a hit, resolving once it is visible
  in the node's aggregate) and `Node::ask_health()` then `HealthReply::take()`
  (the admin snapshot, as the core's `Counters`);
- `NodeStore`, the per-node `AggregateStore`, and `BucketEpoch`, re-exported for
  the driver and oracle;
- the `Ticker` impl for the core's `SimTicker`, whose inherent
  `next_tick` returns the instant each tick was scheduled for;
- the discovery stream: the core's `PeerStream` events carry addresses
  only, and the adapter maps each `Added` peer to carry the zone the plan
  places that node in (`plan::node_az`), as a Kubernetes EndpointSlice
  endpoint's `zone` does.

`make hygiene` (run by `make test` and CI) checks the boundary: outside
`adapter.rs`, every line that names the `gabion` crate must be one `use` of
those traits, so an alias (`use gabion as g`), an `extern crate`, an inline
path or a brace list with any other name fails it.

A change to a library API the adapter uses edits `adapter.rs` in the same
commit. The baselines are built with `H1`'s adapter and manifest, so the
adapter must map names only: what a node does with them is the library's
behaviour, which is what each arm measures. Beyond the names, the contract is:

- **All timing comes from `Clock::now_millis` and tickers from
  `Clock::ticker`**; the runtime must not use `tokio::time`, `tokio::spawn` or
  threads, because no Tokio runtime is driving it.
- Saturation is logged through `tracing` at WARN or ERROR with a `*_total`
  field carrying the running count.
- Given its configuration, a node behaves identically run to run (no
  `RandomState` iteration, no wall clock). `identical_inputs_give_bit_identical_results`
  checks it.

## Tests

```sh
cargo nextest run -p gossip-bench
python3 -m unittest discover -s crates/gossip-bench/judge
# Print the validation figures:
cargo nextest run --release -p gossip-bench validation --no-capture
```

The crate's tests build at `opt-level = 2` (the root `Cargo.toml`): the
300-node validation epidemic takes about 78 s unoptimised and about 8 s
optimised.
