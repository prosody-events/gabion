# Gabion Gossip Bench: Results Audit

This audit checks the JSONL results, plots, and literature comparison for the
gossip-bench report. Performed on the artifacts present at the time of writing;
no source files were modified.

## Inputs read

All paths under `target/gossip-bench/`. Byte counts as a sanity check:

| Suite          | Path                            | Bytes   | Trials |
| -------------- | ------------------------------- | ------- | -----: |
| `convergence`  | `convergence/results.jsonl`     | 393 746 |     22 |
| `fanout_sweep` | `fanout_sweep/results.jsonl`    | 237 361 |      8 |
| `scale_n`      | `scale_n/results.jsonl`         | 173 856 |      6 |
| `loss`         | `loss/results.jsonl`            | 719 567 |     18 |
| `partition`    | `partition/results.jsonl`       |  33 275 |      1 |
| `staleness`    | `staleness/results.jsonl`       |  88 736 |      4 |

The figures directory described in the task (`target/gossip-bench/figures/`)
did not exist at the start of the audit, only per-suite PNGs. The task spec
referenced SVGs produced by `bench/render.py`. I ran `python3 bench/render.py`
once to materialise `figures/*.svg` and the companion `data.typ`. `render.py`
only writes under `target/` (build output) and was not modified; no source
under `crates/gossip-bench/{src,bench}` was touched.

## Sanity-check findings

- **Convergence (rule: at f=3, rounds ≤ 2·log₂ N).** Pass. At f=3 the
  rounds-to-converge are `{N=4:2, N=8:3, N=16:2, N=32:3, N=64:5}` against
  thresholds `{4, 6, 8, 10, 12}`. The series is consistent with the canonical
  O(log N) shape, with a clear jump at N=64 (5 rounds vs 3 at N=32). Two
  technical outliers exist at **f=1**: `converge_n4_f1` (6 vs threshold 4) and
  `converge_n64_f1` (13 vs threshold 12). The threshold rule was framed for
  f=3; f=1 is a degenerate linear-chain regime where the O(log N) bound does
  not hold, so these are expected rather than regressions. They should be
  acknowledged in the report so a reviewer reading the f=1 row does not assume
  the bound was violated by an interesting case.
- **Convergence (rule: no `null` rounds at f≥2, duration ≥5 s).** Pass. All 22
  trials report a finite `convergence_rounds`.
- **Scale_n (rule: bytes/node/s flat in N within ~30%).** Issue. At fanout=3
  the values are `N=4:3377, N=8:3933, N=16:4217, N=32:4388, N=64:4310, N=128:3075`.
  Min–max spread is `(4388−3075)/mean ≈ 35.2 %`, slightly above the 30 %
  budget. Important: the trend is **not** linear in N — bandwidth peaks at
  N=32 and drops at N=128. The failure mode the rule was looking for
  (per-node bandwidth that grows with N, indicating a fan-in regression) is
  not present. The dip at N=128 with rounds=6 suggests a different effect —
  likely workload-finite — where the single 10-hit write at t=100 ms is fully
  delivered well before the 6 s run ends and the per-node steady-state is
  dominated by idle anti-entropy chatter that scales sub-linearly. Worth a
  longer-running variant before drawing a tight bound.
- **Loss (rule: final_divergence = 0 for every trial that converged).** Pass.
  All 18 trials at uniform_loss ∈ {0, 0.1, 0.2, 0.3, 0.4, 0.5} report
  `final_divergence = 0` and finite `convergence_rounds`. Median rounds
  progresses monotonically from 3 (at 0 % loss) to 4–5 (at 50 % loss).
- **Partition (rule: pre-heal split, post-heal converged).** Pass. The single
  N=8 trial has nodes 0–3 holding `7` and nodes 4–7 holding `0` continuously
  from t=300 ms through t=9 900 ms, then all 8 nodes report `7` from
  t=10 000 ms onwards. The schedule restores 32 directed links at t=10 s and
  `extras.reconvergence_millis_after_heal = 0` (reconvergence completes in the
  same sample tick as heal).
- **Staleness (rule: p50 ≤ p95; p50 a small multiple of tick).** Mostly pass.
  All four trials satisfy p50 ≤ p95. p50 is 0 or 100 ms (0× or 1× the 100 ms
  tick interval); p95 is uniformly 100 ms (1× tick). One genuine issue:
  `sustained_src8` reports `convergence_rounds = null` at fanout=3 with a 10 s
  duration. The literal rule "no null rounds for a 5+ second run at fanout
  ≥ 2" triggers. The workload here is a sustained write at 8 sources × 1 per
  tick, so the ground truth is monotonically rising and the divergence-based
  convergence definition never fires — this is a workload artefact, not an
  algorithm failure (the final per-node totals are 793–800 against gt=800,
  a 9-cell trailing lag, ~1.1 %). The report should distinguish "never
  converged" from "ground truth is always moving and the measure is
  inapplicable".

## Literature comparison

All numbers below come from the JSONL, not from prose recollection.

**Demers 1987 (epidemic algorithms, residue / traffic / delay).** Demers
predicts O(log N) anti-entropy convergence and provides residue, traffic and
delay as the canonical triple. At f=3 we measure rounds = {2, 3, 2, 3, 5} for
N ∈ {4, 8, 16, 32, 64}, which sits below log₂ N for every point — closer to
the upper-tail Demers gives for push-pull than to the pure-push bound.
First-delivery times computed per-origin from the convergence samples are
`t_avg ≈ {200, 213, 200, 216, 313} ms` and `t_last ≈ {200, 300, 200, 300, 500} ms`,
at a 100 ms tick. Residue, in Demers' rumor-mongering sense, is not
applicable — gabion is anti-entropy and never stops repairing — but
`final_divergence = 0` for every converged trial is the steady-state analogue
and it holds. **The comparison is fair on the round-count axis.** It is
**unfair on traffic** because Demers normalises traffic to messages-per-update
on a single rumor and gabion's bytes/node/s aggregates anti-entropy
maintenance traffic against the workload size; the units don't line up
without a per-cell denominator we are not measuring.

**Karp 2000 (randomized rumor spreading, Θ(log n) rounds, Θ(n log log n)
messages).** Karp's bound is asymptotic on K_n with random phone calls. At
f=3, N=64 gabion converges in 5 rounds against log₂(64)=6, so we sit *below*
the asymptotic floor on a small graph — typical for sparse fanout protocols on
small N where constants dominate. The message-complexity claim
(Θ(n log log n)) cannot be directly checked: we have packet counts but not a
"messages per rumor" denominator, since gabion packs many cells per frame and
has no notion of a single rumor. **Fair on rounds.** **Unfair on the
n log log n claim** because gabion's peer-frontier dedup is an
address-aware protocol — the sender knows the receiver's frontier — so the
address-oblivious lower bound does not apply, and the only honest comparison
would require us to instrument bytes-per-novel-cell, which we are not.

**SWIM 2002 (constant per-process bandwidth in N).** SWIM's headline is that
outgoing bandwidth at a single process is independent of group size. At
fanout=3 the bytes-per-node-per-second in the scale_n suite is
`{3377, 3933, 4217, 4388, 4310, 3075}` for N ∈ {4..128} — flat within ±20 % of
the mean, no growth trend. This is the closest gabion comes to one of the
classical headline plots. **Comparison fair on the bandwidth axis.** It is
**unfair on the SWIM failure-detection numbers** (≈1.6·T' detection, ≈0.1 %
false-positive rate at 95 % delivery) — gabion does not implement membership
at all, k8s EndpointSlice owns that, and we have nothing to compare against
those numbers. The report should keep that boundary explicit so no reviewer
expects a time-to-detect figure.

**HyParView 2007 (≈100 % delivery at 80 % simultaneous failure on N=10 000).**
HyParView measures broadcast reliability under massive churn. Gabion has no
HyParView analogue in the benches we ran: the partition suite is a single
4-vs-4 cut with a one-shot heal, not a churn sweep, and no trial runs at
N=10 000. The loss suite at 50 % uniform loss preserves
`final_divergence = 0` in all 3 trials and still converges in 4–5 rounds —
suggestive of resilience under packet loss, but **not the same experiment**
HyParView ran. **Comparison is unfair**: HyParView's stress is on node
disappearance and overlay healing, gabion's loss bench is on per-link
i.i.d. drops with the same membership set. The right comparison would require
the massive-pod-loss sweep that REFERENCES.md identifies as item 4 of the
target methodology.

**Plumtree 2007 (RMR ≪ fanout − 1, LDH ≈ flat-gossip).** Plumtree's headline
is Relative Message Redundancy: bytes shipped minus the novel-delta floor,
normalised by N−1. Gabion's peer-frontier dedup is the closest structural
analogue to Plumtree's IHAVE/PRUNE in the literature, but **we cannot compute
RMR from the present JSONL** — `bytes_sent_total` is recorded, but
"bytes that carried a cell the receiver had not yet seen" is not. The closest
proxy we have is "bytes/node/s under a fixed 10-hit single-write" which at
f=3 sits at ≈3.4–4.4 kB/s. **The comparison is honestly unfair without RMR
instrumentation.** This is the single most important gap; the report should
say so explicitly rather than imply by silence that gabion is on par with
Plumtree.

**Astrolabe 2003 (propagation delay ρ · log_b N, tens of seconds at scale).**
Astrolabe gossips every 5–10 s and reports propagation delays in the tens of
seconds at thousands of nodes. Gabion runs ρ = 100 ms (tick interval) and the
N=64, f=3 convergence is `convergence_millis = 500`. Plugged into Astrolabe's
floor, ρ · log_b N for ρ=100 ms, b=3, N=64 is ≈378 ms — the measured 500 ms is
within one round of that. **Fair as a back-of-envelope upper-bound check.**
**Unfair as a direct comparison** because Astrolabe is hierarchical and
aggregating, gabion is flat and replicating; the ρ · log N relationship is
the only shared structure, and the constants live in different regimes (a
500 ms convergence at N=64 vs Astrolabe's 10 s class).

**Bimodal Multicast 1999 (bimodal latency, 200 msg/s at 25 % perturbation).**
Bimodal Multicast reports a two-mode delivery distribution and throughput
stability under CPU-perturbation. Gabion's two lanes (direct push + repair)
should produce a bimodal latency too, but the staleness bench reports only
p50 and p95, which is too coarse to see a bimodal shape. The 50 % i.i.d. loss
result (median 4–5 rounds, `final_divergence = 0`) is in the same spirit as
the 20 %-loss-at-100-msg/s reliability number from Bimodal but for a
different stress (link drops vs CPU starvation). **Unfair on perturbation**
(we did not run any node-throttling experiment) and **unfair on the bimodal
distribution claim** (we don't ship the per-delivery latency histogram, only
p50/p95).

**Jelasity et al. 2007 (peer-sampling: in-degree CDF, partitioning under
churn).** Jelasity et al. measure the global in-degree distribution of the
sampled overlay. Gabion uses EndpointSlice as its peer source, so it inherits
whatever uniformity the EndpointSlice gives, and the per-round fanout
selection is uniform-random over that list. **None of the present benches
sample in-degree.** The JSONL gives `bytes_sent` per node — which at N=64, f=3
is `{17 936, 17 936, 17 936, 26 904}` for nodes 0–3 in the convergence trial
— but that reflects node 0 being the write source, not in-degree skew, and we
have no instrumentation to disentangle them. **The comparison is unfair**;
the in-degree CDF Jelasity emphasises is exactly the kind of plot
REFERENCES.md item 5 says we should produce and have not.

## Plot honesty

For each figure, the y-axis range that `render.py` chose versus the
actual data range in the JSONL.

- **`convergence.svg` (left, rounds-to-converge vs N).** Plot y-axis
  [1.56, 13.44]. Data range: rounds ∈ [2, 13] across all (N, f).
  matplotlib `tight_y` adds a small symmetric pad on both ends — honest.
- **`convergence.svg` (right, bytes/node/s vs N).** Plot y-axis
  [646, 12 091]. Data range: 1 070 (f=1, N=4) to 11 666 (f=8, N=32).
  Pad on both ends, no zero baseline, but the visible spread is on a
  scale where the f=1 line is plainly below the f=8 line — honest, though
  the absence of a zero baseline could be called out.
- **`fanout_sweep.svg` (rounds line).** Plot y-axis [1.72, 9.28]. Data
  range: 2 to 9. Honest with small pad.
- **`fanout_sweep.svg` (bytes/s twin axis).** Plot y-axis **[0, 18 553]**
  — forced to zero by `ax2.set_ylim(0, max(df["bytes_per_s"]) * 1.05)`.
  Data max is 17 670 at f=12. Honest; this axis correctly anchors at
  zero.
- **`scale_n.svg` (left, rounds vs N).** Plot y-axis [1.8, 7.2]. Data
  range: 2 to 6. Honest with small pad.
- **`scale_n.svg` (right, bytes/node/s vs N).** Plot y-axis
  **[3022, 4441]**. Data range: 3 075 (N=128) to 4 388 (N=32). **Issue:
  axis lies — visually.** The data varies by ~14 % around 3 700, but
  the axis is `tight_y`-padded on a non-zero baseline, so the dip at
  N=128 reads as a dramatic cliff. Compare against the fanout_sweep
  twin axis, where `set_ylim(0, …)` forces a zero baseline. The same
  data on a 0-anchored axis would look almost flat — which is the
  conclusion the report wants to draw ("per-node bandwidth is roughly
  flat in N"). The current axis suggests the opposite. This is the
  single most actionable plot-honesty finding.
- **`loss.svg` (rounds vs loss rate).** Plot y-axis [1.88, 5.12]. Data
  range: 2 to 5. Honest.
- **`partition.svg`.** Plot y-axis [-0.5, 7.6]. Data range: 0 to 7
  (ground truth ceiling). Honest, zero baseline preserved.
- **`staleness.svg` (per-hit lag vs sources).** Plot y-axis [-4, 104].
  Data range: 0 to 100 ms. Honest, near-zero baseline.

## What's missing

These are evaluation axes the literature treats as headline that the present
JSONL cannot answer.

- **RMR (Plumtree).** No counter for "novel cells per byte shipped". Without
  it, the gabion-vs-Plumtree comparison is qualitative only.
- **In-degree CDF (Jelasity).** `bytes_sent` per node is logged but not
  per-receiver, so we cannot count how many sends each peer was on the
  receiving end of.
- **Residue tail under churn (Demers).** No churn workload (pod joins/leaves)
  is exercised. The partition suite is one cut and one heal.
- **Massive simultaneous failure sweep (HyParView).** Only one partition
  topology (4-vs-4, N=8) is tested. The 10 / 50 / 80 / 95 % failure axis
  REFERENCES.md item 4 calls for is unrun.
- **Bimodal latency distribution (Bimodal Multicast).** Staleness reports
  only p50 and p95; no histogram, so the two-lane shape cannot be observed.
- **CPU-throttled perturbation (Bimodal).** No "slow a fraction of pods"
  workload exists.
- **Repair-lane catch-up after forced dirty-ring overflow.** REFERENCES.md
  item 6 — the gabion-specific worst-case staleness bound — is not measured.
- **Multi-seed variance for everything except `loss`.** Of the six suites
  only `loss` runs 3 seeds per (loss-rate); the other five suites are
  single-shot. Means and trends drawn from them are point estimates without
  error bars. Adding 3–5 seeds per configuration would let the bandwidth
  spread, round counts, and staleness numbers carry a confidence interval.
