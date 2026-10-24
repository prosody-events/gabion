#!/usr/bin/env python3
"""Aggregate stamped study results into per-regime comparison tables.

Usage:
    judge.py RESULTS_DIR [RESULTS_DIR ...] --out REPORT_DIR [--baseline main]
             [--level 0.95]

Reads every `*.json` result under the given directories (as written by
`matrix.py`). Refuses to proceed when results carry different harness tree
hashes or another `gabion-study/*` schema (stale results from an older
harness), or one solution label carries several commits: numbers from
different code are not comparable. Hashes that differ only in `judge/judge.py`,
`judge/test_*.py` or `README.md` (compared with `git diff-tree` in the checkout
holding this file) count as one harness: nothing that builds or drives a run
changed. `judge/matrix.py` is not exempt: it builds the binary and passes its
arguments. Results without a stamp or schema, or produced from a dirty harness
or solution worktree, are rejected and listed.

Runs the harness marked INVALID (a health invariant failed) stay visible:
every cell of a (regime, solution) with an invalid seed reads
`INVALID k/n`, and a solution whose every seed is invalid stays in the
tables as INVALID rather than disappearing. Statistics cover valid seeds
only; ratios to the baseline and paired differences use only the seeds valid
for both, so a solution cannot gain by losing its hardest seed.

For each regime x solution x metric it reports a Bayesian summary. The seed
values y_1..y_n are modelled as Normal(mu, sigma^2) with the prior
p(mu, sigma) proportional to 1/sigma on sigma >= sigma_min, where sigma_min is
the metric's rounding noise: a value reported on a grid of step h (`RESOLUTION`:
1 for counts and integer hit quantiles, one count per node-second or per
admitted request for rates, 100 / limit for a percentage of the limit,
1 / hits for a fraction of hits, the 100 ms probe for the time to heal) is
known only to within h / 2, which adds uniform noise of standard deviation
h / sqrt(12), so the reported values cannot vary less than that. Three seeds
that tie on an integer therefore say "within about one step", not "exactly".
For a metric without a declared grid sigma_min = 0 and the prior is the
Jeffreys prior. The model ignores bounds: a count that ties at 0 gets an
interval reaching below 0. The tables show the posterior median of mu (the
sample mean) and its equal-tailed credible interval (95 % by default,
`--level`; the posterior is symmetric and unimodal, so this is also the
highest-density interval). When
sigma_min is far below the sample spread s the posterior of mu is Student t
with n - 1 degrees of freedom and scale s / sqrt(n), as with the Jeffreys
prior; otherwise it is the same normal mixture over sigma cut at sigma_min,
integrated numerically. With one seed, or seeds that agree exactly on a metric
without a grid, the posterior is improper and no interval or P is shown.

The comparison with the baseline is paired over the seeds valid for both. When
every paired value is positive the model applies to d_i = ln(a_i) - ln(b_i)
(rounding noise h_a^2 / (12 a^2) + h_b^2 / (12 b^2) by the delta method): the
judge reports the geometric-mean ratio exp(mean d) with its credible
interval and P(better), the posterior probability that the mean log ratio
favours the solution. Otherwise it models d_i = a_i - b_i (rounding noise
(h_a^2 + h_b^2) / 12) and reports the mean difference the same way. Lower is
better except for `HIGHER_IS_BETTER`. Writes
`summary.md` (with a per-phase table for regimes that cut the network),
`summary.csv` (long format, with the paired comparison),
`pareto.md` (means over the seeds valid for every solution on the front),
`flags.md` (saturation the harness flagged without invalidating, e.g. dirty-ring
overflow) and `rejected.md`.

Python 3 standard library only.
"""

from __future__ import annotations

import argparse
import csv
import functools
import itertools
import json
import math
import re
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

SCHEMA = "gabion-study/2"
SCHEMA_FAMILY = "gabion-study/"



def betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the incomplete beta function (modified Lentz,
    as in Numerical Recipes `betacf`)."""
    tiny = 1e-300
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) > tiny else tiny)
    h = d
    for m in range(1, 1000):
        m2 = 2 * m
        for aa in (m * (b - m) * x / ((qam + m2) * (a + m2)),
                   -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))):
            d = 1.0 + aa * d
            d = 1.0 / (d if abs(d) > tiny else tiny)
            c = 1.0 + aa / c
            c = c if abs(c) > tiny else tiny
            step = d * c
            h *= step
        if abs(step - 1.0) < 3e-16:
            break
    return h


def betai(a: float, b: float, x: float, y: float | None = None) -> float:
    """Regularized incomplete beta function I_x(a, b). `y` is 1 - x, for a
    caller that has it without the rounding of 1 - x near x = 1."""
    y = 1.0 - x if y is None else y
    if x <= 0.0:
        return 0.0
    if y <= 0.0:
        return 1.0
    front = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
                     + a * math.log(x) + b * math.log(y))
    if x < (a + 1.0) / (a + b + 2.0):
        return front * betacf(a, b, x) / a
    return 1.0 - front * betacf(b, a, y) / b


def t_cdf(t: float, df: float) -> float:
    """P(T <= t) for Student t with `df` degrees of freedom."""
    if t == 0.0:
        return 0.5
    # x = df / (df + t^2) and y = 1 - x, each formed without cancellation,
    # overflow or division by zero at |t| near 0 or infinity.
    q = t * t / df
    x, y = (1.0 / (1.0 + q), q / (1.0 + q)) if q <= 1.0 else ((1.0 / q) / (1.0 + 1.0 / q),
                                                              1.0 / (1.0 + 1.0 / q))
    tail = 0.5 * betai(df / 2.0, 0.5, x, y)
    return 1.0 - tail if t > 0 else tail


@functools.lru_cache(maxsize=None)
def t_quantile(p: float, df: int) -> float:
    """The t with P(T <= t) = p, by bisection on `t_cdf`."""
    if p < 0.5:
        return -t_quantile(1.0 - p, df)
    lo, hi = 0.0, 1.0
    while t_cdf(hi, df) < p and hi < 1e300:
        lo, hi = hi, hi * 2.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        lo, hi = (mid, hi) if t_cdf(mid, df) < p else (lo, mid)
    return (lo + hi) / 2.0


def integrate(f, a: float, b: float, tol: float = 1e-12) -> float:
    """Adaptive Simpson's rule on [a, b], starting from 16 panels so a narrow
    feature cannot fall between the first samples."""
    edges = [a + (b - a) * i / 16 for i in range(17)]
    stack = []
    for lo, hi in zip(edges, edges[1:]):
        f_lo, f_mid, f_hi = f(lo), f((lo + hi) / 2.0), f(hi)
        stack.append((lo, hi, f_lo, f_mid, f_hi, (hi - lo) / 6.0 * (f_lo + 4.0 * f_mid + f_hi),
                      tol / 16, 0))
    total = 0.0
    while stack:
        lo, hi, f_lo, f_mid, f_hi, whole, eps, depth = stack.pop()
        mid = (lo + hi) / 2.0
        f_left, f_right = f((lo + mid) / 2.0), f((mid + hi) / 2.0)
        left = (mid - lo) / 6.0 * (f_lo + 4.0 * f_left + f_mid)
        right = (hi - mid) / 6.0 * (f_mid + 4.0 * f_right + f_hi)
        if depth >= 40 or abs(left + right - whole) <= 15.0 * eps:
            total += left + right + (left + right - whole) / 15.0
        else:
            stack.append((lo, mid, f_lo, f_left, f_mid, left, eps / 2.0, depth + 1))
            stack.append((mid, hi, f_mid, f_right, f_hi, right, eps / 2.0, depth + 1))
    return total


def floor_negligible(n: int, kappa: float) -> bool:
    """Whether the cut at sigma_min removes negligible posterior mass (below
    1e-12): the precision 1 / sigma^2 has a Gamma((n - 1) / 2) posterior whose
    mass past the cut is the upper incomplete gamma Q((n - 1) / 2, kappa)."""
    shape = (n - 1) / 2.0
    return kappa >= 30.0 + shape + 10.0 * math.sqrt(shape)


@functools.lru_cache(maxsize=None)
def floored_weight(n: int, kappa: float):
    """Posterior density of r = sigma_min / sigma on (0, 1], unnormalized and
    scaled to peak 1, and its integral."""
    peak = min(1.0, math.sqrt((n - 2) / (2.0 * kappa))) if kappa > 0 else 1.0
    top = peak ** (n - 2) * math.exp(-kappa * peak * peak)

    def weight(r: float) -> float:
        return r ** (n - 2) * math.exp(-kappa * r * r) / top

    return weight, integrate(weight, 0.0, 1.0)


def floored_cdf(z: float, n: int, kappa: float) -> float:
    """P(Z <= z) for Z = (mu - mean) sqrt(n) / sigma_min under the prior
    1/sigma on sigma >= sigma_min: mu | sigma is Normal(mean, sigma^2 / n) and
    r = sigma_min / sigma has density proportional to r^(n-2) exp(-kappa r^2)
    on (0, 1], kappa = (n - 1) s^2 / (2 sigma_min^2)."""
    if z < 0.0:
        return 1.0 - floored_cdf(-z, n, kappa)
    weight, norm = floored_weight(n, kappa)
    # Integrate the upper tail, and only where it lives: Phi(-z r) is below
    # 1e-15 past r = 8 / z, a sliver an integrator sampling all of (0, 1]
    # would step over.
    edge = min(1.0, 8.0 / z) if z > 0.0 else 1.0
    tail = integrate(lambda r: weight(r) * math.erfc(z * r / math.sqrt(2.0)), 0.0, edge) / 2.0
    return max(0.5, 1.0 - tail / norm)


@functools.lru_cache(maxsize=None)
def floored_quantile(p: float, n: int, kappa: float) -> float:
    """The z with `floored_cdf(z) = p`, by bracketing and bisection; the
    distribution is symmetric about 0."""
    if p < 0.5:
        return -floored_quantile(1.0 - p, n, kappa)
    lo, hi = 0.0, 1.0
    while floored_cdf(hi, n, kappa) < p and hi < 1e15:
        lo, hi = hi, hi * 2.0
    while hi - lo > 1e-10 * hi:
        mid = (lo + hi) / 2.0
        lo, hi = (mid, hi) if floored_cdf(mid, n, kappa) < p else (lo, mid)
    return (lo + hi) / 2.0


def dig(result: dict, path: str):
    node = result
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def over_admission(result: dict, field: str):
    values = [a[field] for a in result.get("accuracy", []) if a.get(field) is not None]
    return max(values) if values else None


def over_admission_mean(result: dict):
    values = [a["over_mean_pct_limit"] for a in result.get("accuracy", [])]
    return statistics.fmean(values) if values else None


def cpu_s(result: dict):
    p = result.get("process") or {}
    if "cpu_user_s" not in p:
        return None
    return p["cpu_user_s"] + p["cpu_sys_s"]


def rss_mib(result: dict):
    p = result.get("process") or {}
    return p["max_rss_bytes"] / 2**20 if "max_rss_bytes" in p else None


# (key, label, extractor). Lower is better for every metric except those in
# HIGHER_IS_BETTER; the Pareto view uses the starred ones.
METRICS = [
    ("bytes_total", "Total bytes sent, measurement window (whole network)*",
     lambda r: dig(r, "network.measure.bytes_sent")),
    ("bytes_total_run", "Total bytes sent, whole run",
     lambda r: dig(r, "network.run.bytes_sent")),
    ("bytes_per_node_s", "Bytes per node per second",
     lambda r: dig(r, "network.measure_bytes_per_node_per_s")),
    ("packets_per_node_s", "Packets per node per second",
     lambda r: dig(r, "network.measure_packets_per_node_per_s")),
    ("bytes_per_admitted", "Bytes per admitted request",
     lambda r: dig(r, "network.measure_bytes_per_admitted")),
    ("cross_az_bytes", "Cross-AZ bytes, measurement window",
     lambda r: dig(r, "network.measure.cross_az_bytes")),
    ("stale_limited_mean", "Staleness, limited keys: signed mean, truth - view (hits)",
     lambda r: dig(r, "staleness.limited.mean")),
    ("stale_limited_p99", "Staleness, limited keys: p99 (hits)*",
     lambda r: dig(r, "staleness.limited.p99")),
    ("stale_limited_max", "Staleness, limited keys: max (hits)",
     lambda r: dig(r, "staleness.limited.max")),
    ("stale_limited_p99_pct", "Staleness, limited keys: p99 (% of limit)",
     lambda r: dig(r, "staleness.limited.p99_pct_limit")),
    ("stale_sampled_mean", "Staleness, sampled sites: mean (hits)",
     lambda r: dig(r, "staleness.sampled.mean")),
    ("stale_sampled_p99", "Staleness, sampled sites: p99 (hits)",
     lambda r: dig(r, "staleness.sampled.p99")),
    ("residual_limited_mean", "Residual (closed >= 1.5 s), limited: mean (hits)",
     lambda r: dig(r, "staleness.residual_limited.mean")),
    ("residual_limited_p99", "Residual (closed >= 1.5 s), limited: p99 (hits)",
     lambda r: dig(r, "staleness.residual_limited.p99")),
    ("residual_sampled_p99", "Residual (closed >= 1.5 s), sampled: p99 (hits)",
     lambda r: dig(r, "staleness.residual_sampled.p99")),
    ("stale_limited_under_mean", "Staleness, limited keys: mean of max(0, error) (hits)",
     lambda r: dig(r, "staleness.limited.under_mean")),
    ("stale_limited_over_mean", "Over-count, limited keys: mean of max(0, view - truth) (hits)",
     lambda r: dig(r, "staleness.limited.over_mean")),
    ("stale_limited_over_max", "Over-count, limited keys: max (hits)",
     lambda r: dig(r, "staleness.limited.over_max")),
    ("stale_sampled_over_max", "Over-count, sampled sites: max (hits)",
     lambda r: dig(r, "staleness.sampled.over_max")),
    ("conv_never_frac", "Fraction never converged before expiry",
     lambda r: dig(r, "convergence.never_fraction")),
    ("conv_p50_incl_never_ms", "Convergence p50 incl. never-converged (ms, lower bound)",
     lambda r: dig(r, "convergence.p50_incl_never_ms")),
    ("conv_p99_incl_never_ms", "Convergence p99 incl. never-converged (ms, lower bound)*",
     lambda r: dig(r, "convergence.p99_incl_never_ms")),
    ("conv_mean_incl_never_ms", "Convergence mean incl. never-converged (ms, lower bound)",
     lambda r: dig(r, "convergence.mean_incl_never_ms")),
    ("conv_p50_ms", "Convergence p50, converged hits only (ms; read with never-converged)",
     lambda r: dig(r, "convergence.p50_ms")),
    ("conv_p99_ms", "Convergence p99, converged hits only (ms; read with never-converged)",
     lambda r: dig(r, "convergence.p99_ms")),
    ("conv_max_ms", "Convergence max, converged hits only (ms)",
     lambda r: dig(r, "convergence.max_ms")),
    ("over_max_pct", "Over-admission, worst limited key: max (% of limit)*",
     lambda r: over_admission(r, "over_max_pct_limit")),
    ("over_mean_pct", "Over-admission, limited keys: mean (% of limit)",
     over_admission_mean),
    ("false_rejects", "False rejects", lambda r: dig(r, "traffic.measure.false_rejects")),
    ("admitted", "Admitted requests (context, not a score)",
     lambda r: dig(r, "traffic.measure.admitted")),
    ("rejected", "Rejected requests", lambda r: dig(r, "traffic.measure.rejected")),
    ("churn_never95", "Joiners that never reached 95 % of truth",
     lambda r: dig(r, "churn.never_reached_95")),
    ("churn_parity_p50_ms", "Joiner time to parity with established pods, p50 (ms)",
     lambda r: dig(r, "churn.t_parity_p50_ms")),
    ("churn_parity_max_ms", "Joiner time to parity with established pods, max (ms)",
     lambda r: dig(r, "churn.t_parity_max_ms")),
    ("churn_never_parity", "Joiners that never reached parity",
     lambda r: dig(r, "churn.never_reached_parity")),
    ("churn_t95_p50_ms", "Joiner time to 95 % of truth, p50 (ms)",
     lambda r: dig(r, "churn.t95_p50_ms")),
    ("churn_t99_max_ms", "Joiner time to 99 % of truth, max (ms)",
     lambda r: dig(r, "churn.t99_max_ms")),
    ("churn_undercount", "Joiner under-count integral, mean (hit-s)",
     lambda r: dig(r, "churn.undercount_hit_s_mean")),
    ("drop_loss_pkts", "Datagrams lost (network loss)",
     lambda r: dig(r, "network.measure.dropped_loss_packets")),
    ("drop_rcvbuf_pkts", "Datagrams dropped at a full receive buffer",
     lambda r: dig(r, "network.measure.dropped_rcvbuf_packets")),
    ("peak_cells", "Peak active cells on any node", lambda r: dig(r, "health.peak_active_cells")),
    ("send_pending_max", "Max send-queue depth", lambda r: dig(r, "health.max_send_pending_depth")),
    ("dirty_overflow_nodes", "Nodes whose dirty ring overflowed (flag)",
     lambda r: dig(r, "health.gabion_warnings.dirty_overflow_nodes")),
    ("dirty_overflow_entries", "Dirty-ring entries overwritten, lower bound (flag)",
     lambda r: dig(r, "health.gabion_warnings.dirty_overflow_lower_bound")),
    ("gabion_warn_events", "gabion WARN events logged",
     lambda r: dig(r, "health.gabion_warnings.warn_events")),
    ("gabion_error_events", "gabion ERROR events logged",
     lambda r: dig(r, "health.gabion_warnings.error_events")),
    ("cpu_s", "Process CPU time (s), noisy", cpu_s),
    ("rss_mib", "Process peak RSS (MiB), noisy", rss_mib),
]

def partition_metrics() -> list:
    """Per-phase metrics of a regime with a network partition (`partition` in
    the result): convergence by the side of the cut the hit's origin and its
    observers sit on, staleness by the observer's side, and the heal."""
    rows = [
        ("part_deficit_at_heal", "Hits missing per (pod, key) at the heal (hits)",
         lambda r: dig(r, "partition.deficit_at_heal_hits")),
        ("part_time_to_heal_ms",
         "Time to heal: until every cross-cut count is exact (ms), seeds that healed only",
         lambda r: dig(r, "partition.time_to_heal_ms")),
        ("part_unrepaired_at_expiry", "Hits per (pod, key) that expired unrepaired (hits)",
         lambda r: dig(r, "partition.unrepaired_at_expiry_hits")),
    ]
    for phase in ("before", "during", "after"):
        for relation, side in (("same_side", "same side"), ("cross_side", "cross side")):
            rows.append((f"part_{phase}_conv_{relation}_p99_ms",
                         f"{phase}: convergence p99 incl. never, {side} (ms, lower bound)",
                         lambda r, p=phase, q=relation:
                         dig(r, f"partition.{p}.convergence.{q}.p99_incl_never_ms")))
            rows.append((f"part_{phase}_conv_{relation}_never",
                         f"{phase}: fraction never converged, {side}",
                         lambda r, p=phase, q=relation:
                         dig(r, f"partition.{p}.convergence.{q}.never_fraction")))
        for observers, who in (("in_zone", "cut-off zone"), ("rest", "rest of cluster")):
            for cls in ("limited", "sampled"):
                rows.append((f"part_{phase}_stale_{observers}_{cls}_p99",
                             f"{phase}: staleness p99, {cls} keys, pods in the {who} (hits)",
                             lambda r, p=phase, o=observers, c=cls:
                             dig(r, f"partition.{p}.staleness.{o}.{c}.p99")))
    return rows


PARTITION_METRICS = partition_metrics()
HIGHER_IS_BETTER = {"admitted"}
ALL_METRICS = METRICS + PARTITION_METRICS


def one_hit_of_limit(result: dict) -> float:
    """Grid of a percentage of the rule's limit computed from whole hits."""
    limit = dig(result, "regime.rule.limit")
    return 100.0 / limit if limit else 0.0


def one_count_per(path: str):
    """Grid of a count divided by the figure at `path` (node-seconds, admitted
    requests): one unit of the count."""
    def grid(result: dict) -> float:
        per = dig(result, path)
        return 1.0 / per if per else 0.0
    return grid


def one_hit_of(path: str):
    """Grid of a never-converged fraction: one hit of those scored at `path`."""
    def grid(result: dict) -> float:
        node = dig(result, path) or {}
        hits = (node.get("converged") or 0) + (node.get("never_converged") or 0)
        return 1.0 / hits if hits else 0.0
    return grid


# Step h of the grid each metric's value lies on, or a function of the result
# giving it: the value is known only to within h / 2, which puts the floor
# h / sqrt(12) under sigma (see the module docstring). Metrics not listed are
# treated as continuous (no floor). HEAL_PROBE_MS is `HEAL_PROBE_NS` in
# src/harness/phases.rs.
HEAL_PROBE_MS = 100.0
RESOLUTION = {
    **dict.fromkeys(
        ["bytes_total", "bytes_total_run", "cross_az_bytes", "stale_limited_p99",
         "stale_limited_max", "stale_sampled_p99", "residual_limited_p99",
         "residual_sampled_p99", "stale_limited_over_max", "stale_sampled_over_max",
         "false_rejects", "admitted", "rejected", "churn_never95", "churn_never_parity",
         "drop_loss_pkts", "drop_rcvbuf_pkts", "peak_cells", "send_pending_max",
         "dirty_overflow_nodes", "dirty_overflow_entries", "gabion_warn_events",
         "gabion_error_events"]
        + [k for k, _, _ in PARTITION_METRICS if "_stale_" in k],
        1.0),
    "bytes_per_node_s": one_count_per("windows.node_seconds_measure"),
    "packets_per_node_s": one_count_per("windows.node_seconds_measure"),
    "bytes_per_admitted": one_count_per("traffic.measure.admitted"),
    "stale_limited_p99_pct": one_hit_of_limit,
    "over_max_pct": one_hit_of_limit,
    "conv_never_frac": one_hit_of("convergence"),
    "part_time_to_heal_ms": HEAL_PROBE_MS,
    **{f"part_{phase}_conv_{relation}_never":
       one_hit_of(f"partition.{phase}.convergence.{relation}")
       for phase in ("before", "during", "after") for relation in ("same_side", "cross_side")},
}


def resolution(key: str, result: dict) -> float:
    grid = RESOLUTION.get(key, 0.0)
    return grid(result) if callable(grid) else grid


def rounding_sd(grid_squares: list[float]) -> float:
    """The floor under sigma: the standard deviation of rounding to a grid,
    sqrt(h^2 / 12), with h^2 averaged over the values."""
    return math.sqrt(statistics.fmean(grid_squares) / 12.0) if grid_squares else 0.0

PARETO = [("bytes_total", "stale_limited_p99"), ("bytes_total", "over_max_pct"),
          ("bytes_total", "conv_p99_incl_never_ms")]


def load(dirs: list[Path]):
    """Stamped results from clean trees, valid or INVALID, and the rejects."""
    accepted, rejected = [], []
    for directory in dirs:
        for path in sorted(directory.rglob("*.json")):
            try:
                result = json.loads(path.read_text())
            except ValueError as error:
                rejected.append((path, f"unreadable JSON: {error}"))
                continue
            schema = result.get("schema") if isinstance(result, dict) else None
            if schema != SCHEMA:
                if isinstance(schema, str) and schema.startswith(SCHEMA_FAMILY):
                    sys.exit(f"error: {path} has schema `{schema}`, this judge reads "
                             f"`{SCHEMA}`: it is a stale result from another harness. "
                             "Delete it or rerun that solution on the current harness.")
                rejected.append((path, f"not a study result (schema {schema!r})"))
                continue
            stamp = result.get("stamp")
            if not stamp:
                rejected.append((path, "no stamp (not produced by matrix.py)"))
            elif stamp.get("harness_dirty"):
                rejected.append((path, "harness had uncommitted changes"))
            elif stamp.get("solution_dirty", True):
                rejected.append((path, "solution worktree had uncommitted changes"))
            else:
                accepted.append((path, result))
    return accepted, rejected


def posterior(values: list[float], level: float, floor: float = 0.0) -> dict:
    """Posterior of the mean mu under the normal model with the prior 1/sigma
    on sigma >= `floor` (see the module docstring). `lo`/`hi` are the
    equal-tailed credible interval, absent when the posterior is improper."""
    n = len(values)
    mean = statistics.fmean(values)
    post = {"n": n, "mean": mean, "sd": statistics.stdev(values) if n > 1 else 0.0,
            "floor": floor, "lo": None, "hi": None, "min": min(values), "max": max(values)}
    if proper(post):
        half = offset_quantile(post, 0.5 + level / 2.0)
        post["lo"], post["hi"] = mean - half, mean + half
    return post


def proper(post: dict) -> bool:
    """Whether the posterior of the mean is proper: at least two values, and
    spread among them or a floor under sigma. Otherwise the 1/sigma prior
    piles unbounded mass at sigma = 0 and the data cannot say how close mu
    is to the mean."""
    return post["n"] > 1 and (post["sd"] > 0.0 or post["floor"] > 0.0)


def kappa(post: dict) -> float:
    """(n - 1) s^2 / (2 sigma_min^2): how far the sample spread sits above the
    floor, which decides whether the floor shapes the posterior at all."""
    if post["floor"] <= 0.0:
        return math.inf
    return (post["n"] - 1) * post["sd"] ** 2 / (2.0 * post["floor"] ** 2)


def offset_quantile(post: dict, p: float) -> float:
    """The p-quantile of mu - mean (`proper(post)` holds)."""
    n, k = post["n"], kappa(post)
    if floor_negligible(n, k):
        return t_quantile(p, n - 1) * post["sd"] / math.sqrt(n)
    return floored_quantile(p, n, k) * post["floor"] / math.sqrt(n)


def p_below(post: dict, x: float) -> float:
    """Posterior probability that the mean is below `x` (`proper(post)` holds)."""
    n, k = post["n"], kappa(post)
    if floor_negligible(n, k):
        return t_cdf((x - post["mean"]) * math.sqrt(n) / post["sd"], n - 1)
    return floored_cdf((x - post["mean"]) * math.sqrt(n) / post["floor"], n, k)


def fmt(x) -> str:
    if x is None:
        return "n/a"
    return f"{x:,.0f}" if abs(x) >= 1000 else f"{x:.3g}"


def cell(stats: dict | None, pair: dict | None, invalid: int, total: int,
         note: str = "") -> str:
    mark = f"INVALID {invalid}/{total}" if invalid else ""
    if stats is None:
        return "; ".join(filter(None, [mark, note])) or "n/a"
    text = fmt(stats["mean"])
    if stats["lo"] is not None:
        text += f" [{fmt(stats['lo'])}, {fmt(stats['hi'])}]"
    if pair is not None:
        if pair["scale"] == "log":
            parts = [f"×{pair['ratio']:.2f}"]
            if pair["ratio_lo"] is not None:
                parts[0] += f" [{pair['ratio_lo']:.2f}, {pair['ratio_hi']:.2f}]"
        else:
            parts = [f"Δ{fmt(pair['diff_mean'])}"]
            if pair["diff_lo"] is not None:
                parts[0] += f" [{fmt(pair['diff_lo'])}, {fmt(pair['diff_hi'])}]"
        if pair["p_better"] is not None:
            parts.append(f"P={pair['p_better']:.2f}")
        text += f" ({', '.join(parts)})"
    return "; ".join(filter(None, [mark, text, note]))


# Paths inside the harness tree that neither build nor drive a run: a change
# to them alone leaves the study's numbers unchanged. `judge/matrix.py` is not
# among them, since it builds the binary and passes its arguments.
EXEMPT = re.compile(r"README\.md|judge/judge\.py|judge/test_[^/]*\.py")
TREE_HASH = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")


def hash_equivalence(hashes: list[str], repo: Path) -> str | None:
    """None when every pair of harness tree hashes differs only in `EXEMPT`
    paths (relative to the harness tree), else the reason. Uses plumbing with
    renames off, so no user diff configuration (`diff.relative`,
    `diff.renames`) can hide a path, and a file moved into an exempt path still
    shows where it left."""
    for h in hashes:
        if not TREE_HASH.fullmatch(h):
            return f"`{h}` is not a git object id"
    for a, b in itertools.combinations(sorted(hashes), 2):
        try:
            proc = subprocess.run(["git", "-C", str(repo), "diff-tree", "-r", "-z",
                                   "--no-renames", "--name-only", f"{a}^{{tree}}",
                                   f"{b}^{{tree}}"],
                                  capture_output=True, text=True)
        except OSError as error:
            return f"git could not be run ({error})"
        if proc.returncode:
            return f"git in {repo} cannot compare {a} and {b} ({proc.stderr.strip()})"
        other = [p for p in proc.stdout.split("\0") if p and not EXEMPT.fullmatch(p)]
        if other:
            return f"{a} and {b} differ in {', '.join(other[:5])}"
    return None


def dominated(a: tuple, b: tuple) -> bool:
    """`a` is dominated by `b` when `b` is no worse on both and better on one."""
    return b[0] <= a[0] and b[1] <= a[1] and (b[0] < a[0] or b[1] < a[1])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("results", type=Path, nargs="+")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--baseline", default="main")
    parser.add_argument("--level", type=float, default=0.95,
                        help="credibility level of the intervals, in (0, 1)")
    args = parser.parse_args()
    if not 0.0 < args.level < 1.0:
        sys.exit(f"error: --level {args.level} is not a probability. Pass a credibility "
                 "level strictly between 0 and 1, e.g. 0.95.")
    percent = f"{args.level * 100:g} %"

    accepted, rejected = load(args.results)
    hashes = {r["stamp"]["harness_tree_hash"] for _, r in accepted}
    equivalent = ""
    if len(hashes) > 1:
        repo = Path(__file__).resolve().parent
        why = hash_equivalence(sorted(hashes), repo)
        if why:
            sys.exit("error: results come from different harnesses (tree hashes "
                     f"{', '.join(sorted(hashes))}): {why}. The judge accepts hashes that "
                     "differ only in `judge/judge.py`, `judge/test_*.py` or `README.md`. "
                     "Rerun every solution on "
                     "one harness commit; if git cannot resolve a hash, fetch that harness "
                     f"commit into the checkout containing {Path(__file__).name} and retry.")
        equivalent = (" Treated as one harness (they differ only in `judge/judge.py`, "
                      "`judge/test_*.py` or `README.md`): "
                      f"{', '.join(f'`{h}`' for h in sorted(hashes))}.")
    if not accepted:
        sys.exit("error: no stamped results from clean trees found.")

    runs: dict[tuple[str, str], dict[int, dict]] = defaultdict(dict)
    commits: dict[str, set] = defaultdict(set)
    for path, result in accepted:
        solution = result["stamp"]["solution"]
        key = (result["regime_id"], solution)
        if result["seed"] in runs[key]:
            sys.exit(f"error: duplicate result for {key} seed {result['seed']} ({path})")
        runs[key][result["seed"]] = result
        commits[solution].add(result["stamp"]["solution_commit"])
    for solution, shas in commits.items():
        if len(shas) > 1:
            sys.exit(f"error: solution `{solution}` has results from several commits: "
                     f"{', '.join(sorted(shas))}")

    def valid_seeds(regime: str, solution: str) -> dict[int, dict]:
        return {s: r for s, r in runs.get((regime, solution), {}).items() if r.get("valid")}

    def invalid_count(regime: str, solution: str) -> tuple[int, int]:
        by_seed = runs.get((regime, solution), {})
        return sum(not r.get("valid") for r in by_seed.values()), len(by_seed)

    regimes = sorted({r for r, _ in runs})
    partitioned = {r for (r, _), by_seed in runs.items()
                   if any(dig(result, "partition") for result in by_seed.values())}
    solutions = sorted({s for _, s in runs}, key=lambda s: (s != args.baseline, s))
    table: dict[tuple[str, str, str], dict | None] = {}
    for regime, solution in runs:
        valid = valid_seeds(regime, solution)
        for key, _, extract in ALL_METRICS:
            seen = [(float(v), resolution(key, r)) for r in valid.values()
                    if (v := extract(r)) is not None]
            table[(regime, solution, key)] = posterior(
                [v for v, _ in seen], args.level,
                rounding_sd([h * h for _, h in seen])) if seen else None

    def paired(regime: str, solution: str, key: str) -> dict | None:
        """Posterior of the mean paired comparison (solution against baseline)
        over the seeds valid for both with a value for `key`: the log ratio
        when every value is positive, else the difference."""
        extract = next(e for k, _, e in ALL_METRICS if k == key)
        mine, base = valid_seeds(regime, solution), valid_seeds(regime, args.baseline)
        pairs = [(float(a), float(b), resolution(key, mine[s]), resolution(key, base[s]))
                 for s in sorted(mine.keys() & base.keys())
                 if (a := extract(mine[s])) is not None and (b := extract(base[s])) is not None]
        if not pairs:
            return None
        log = all(a > 0 and b > 0 for a, b, _, _ in pairs)
        # Rounding variance of each paired value: on the log scale by the
        # delta method, var(ln a) = var(a) / a^2.
        noise = [(ha / a) ** 2 + (hb / b) ** 2 if log else ha * ha + hb * hb
                 for a, b, ha, hb in pairs]
        post = posterior([math.log(a) - math.log(b) if log else a - b for a, b, _, _ in pairs],
                         args.level, rounding_sd(noise))
        sign = -1.0 if key in HIGHER_IS_BETTER else 1.0
        better = None
        if proper(post):
            better = p_below({**post, "mean": sign * post["mean"]}, 0.0)
        convert = math.exp if log else (lambda x: x)
        lo, hi = (None, None) if post["lo"] is None else (convert(post["lo"]), convert(post["hi"]))
        est = convert(post["mean"])
        return {"n": post["n"], "scale": "log" if log else "diff", "p_better": better,
                "ratio": est if log else None, "ratio_lo": lo if log else None,
                "ratio_hi": hi if log else None,
                "diff_mean": None if log else est, "diff_lo": None if log else lo,
                "diff_hi": None if log else hi}

    def compare(regime: str, solution: str, key: str) -> dict | None:
        return None if solution == args.baseline else paired(regime, solution, key)

    args.out.mkdir(parents=True, exist_ok=True)
    harness = ", ".join(f"`{h}`" for h in sorted(hashes))
    lines = [
        "# Gossip study: per-regime comparison", "",
        f"Harness tree hash {harness}. Baseline `{args.baseline}`.{equivalent}",
        "",
        f"Cells show the mean across seeds [{percent} credible interval for it: normal "
        "model, prior 1/sigma with sigma no smaller than the metric's rounding noise, so "
        "seeds that tie on a count still leave about one step of doubt; Student t with "
        "n-1 df when the seeds spread well past that], then for solutions other than the "
        "baseline (×geometric-mean ratio to the baseline "
        f"[{percent} credible interval], P = posterior probability the solution is better), "
        "paired over the seeds valid for both. Where a paired value is zero or negative "
        "the comparison is the mean difference Δ instead of a ratio. One seed, or seeds "
        "that agree exactly on a metric without a known grid, give no interval and no P. "
        "`INVALID k/n` marks a "
        "solution with k of n seeds invalid (see `rejected.md`); its statistics cover the "
        "valid seeds only. Lower is better unless noted. Metrics marked (flag) report "
        "saturation that does not invalidate a run; see `flags.md`.", "",
        "| Solution | Branch | Commit |", "|---|---|---|",
    ]
    for solution in solutions:
        stamp = next(r for (_, s), by in runs.items() if s == solution
                     for r in by.values())["stamp"]
        lines.append(f"| {solution} | `{stamp['solution_branch']}` | "
                     f"`{stamp['solution_commit'][:12]}` |")
    for regime in regimes:
        present = [s for s in solutions if (regime, s) in runs]
        counts = {s: invalid_count(regime, s) for s in present}
        lines += ["", f"## {regime}", "",
                  "Valid seeds: " + ", ".join(f"{s} {n - bad}/{n}"
                                              for s, (bad, n) in counts.items()), "",
                  "| Metric | " + " | ".join(present) + " |",
                  "|---|" + "---|" * len(present)]
        def healed(regime: str, solution: str, key: str) -> str:
            if key != "part_time_to_heal_ms" or not valid_seeds(regime, solution):
                return ""
            stats = table.get((regime, solution, key))
            return f"healed {stats['n'] if stats else 0}/{len(valid_seeds(regime, solution))}"

        def rows(metrics):
            return [f"| {label} | "
                    + " | ".join(cell(table.get((regime, s, key)), compare(regime, s, key),
                                      *counts[s], healed(regime, s, key)) for s in present)
                    + " |"
                    for key, label, _ in metrics]

        lines += rows(METRICS)
        if regime in partitioned:
            lines += ["", f"### {regime}: partition phases", "",
                      "The cut isolates one availability zone, then heals. Convergence is "
                      "of hits admitted in the phase: `same side` waits for the pods on "
                      "the origin's side, `cross side` for the other side. Staleness is "
                      "by the observer's side. `cross side` is a conservative bound: the far pod "
                      "must also catch up on its own side's hits. The heal is probed every "
                      "100 ms; a hit that expires unrepaired is not a heal, so time to heal "
                      "averages only the seeds that healed (`healed k/n` counts them) and "
                      "the unrepaired row shows the rest.", "",
                      "| Metric | " + " | ".join(present) + " |",
                      "|---|" + "---|" * len(present)] + rows(PARTITION_METRICS)
    (args.out / "summary.md").write_text("\n".join(lines) + "\n")

    with (args.out / "summary.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["regime", "solution", "metric", "n_valid", "n_invalid", "n_total",
                         "mean", "cri_lo", "cri_hi", "min", "max", "ratio", "ratio_lo",
                         "ratio_hi", "diff_mean", "diff_lo", "diff_hi", "p_better", "scale",
                         "n"])
        for regime in regimes:
            for solution in solutions:
                if (regime, solution) not in runs:
                    continue
                bad, total = invalid_count(regime, solution)
                for key, _, _ in METRICS + (PARTITION_METRICS if regime in partitioned else []):
                    stats = table.get((regime, solution, key)) or {}
                    pair = compare(regime, solution, key) or {}
                    writer.writerow([regime, solution, key, stats.get("n", 0), bad, total,
                                     stats.get("mean"), stats.get("lo"), stats.get("hi"),
                                     stats.get("min"), stats.get("max"), pair.get("ratio"),
                                     pair.get("ratio_lo"), pair.get("ratio_hi"),
                                     pair.get("diff_mean"), pair.get("diff_lo"),
                                     pair.get("diff_hi"), pair.get("p_better"),
                                     pair.get("scale"), pair.get("n")])

    pareto = ["# Pareto view", "",
              "Per regime, each solution's mean on two objectives (both lower is "
              "better), over the seeds valid for every solution listed in that table, so "
              "all points rest on the same workloads. **Bold** solutions are not dominated "
              "by any other solution on that pair. A solution with invalid seeds is marked "
              "with its count; one with no valid seed, or no value for a metric, is "
              "excluded from the table. When no seed is valid for every solution the "
              "points rest on different seeds, so they are listed without a front.", ""]
    for regime in regimes:
        pareto += [f"## {regime}", ""]
        for x, y in PARETO:
            ex = next(e for k, _, e in METRICS if k == x)
            ey = next(e for k, _, e in METRICS if k == y)
            values, invalid, valueless, points = {}, [], [], {}
            for s in solutions:
                if (regime, s) not in runs:
                    continue
                bad, total = invalid_count(regime, s)
                if bad == total:
                    invalid.append(f"{s} (INVALID {bad}/{total})")
                    continue
                seeds = {seed: (float(a), float(b))
                         for seed, r in valid_seeds(regime, s).items()
                         if (a := ex(r)) is not None and (b := ey(r)) is not None}
                if seeds:
                    values[s] = seeds
                else:
                    valueless.append(s)
            common = set.intersection(*(set(v) for v in values.values())) if values else set()
            if not values:
                note = (f"{x} vs {y}: metric not applicable in this regime (no solution "
                        "has a value for both)." if valueless else
                        f"{x} vs {y}: no solution has a valid seed.")
            elif common:
                note = f"Seeds {', '.join(map(str, sorted(common)))}."
            else:
                note = ("No seed is valid for every solution: points are per-solution "
                        "means over different seeds and are not comparable, so no front "
                        "is marked.")
            pareto.append(note)
            if values:
                pareto += ["", f"| Solution | {x} | {y} |", "|---|---|---|"]
            for s, seeds in values.items():
                used = [v for k, v in seeds.items() if k in common] if common else list(seeds.values())
                point = (statistics.fmean(a for a, _ in used), statistics.fmean(b for _, b in used))
                points[s] = point
            for s, point in points.items():
                front = bool(common) and not any(
                    dominated(point, o) for t, o in points.items() if t != s)
                name = f"**{s}**" if front else s
                bad, total = invalid_count(regime, s)
                if bad:
                    name += f" (INVALID {bad}/{total})"
                pareto.append(f"| {name} | {fmt(point[0])} | {fmt(point[1])} |")
            if invalid:
                pareto += ["", "Excluded, every seed invalid: " + ", ".join(invalid) + "."]
            if valueless:
                pareto += ["", f"Excluded, no value for {x} or {y}: " + ", ".join(valueless) + "."]
            pareto.append("")
    (args.out / "pareto.md").write_text("\n".join(pareto) + "\n")

    flag_lines = ["# Flagged runs", "",
                  "Saturation the harness recorded without invalidating the run, and the "
                  "gabion WARN/ERROR messages behind it.", ""]
    for regime in regimes:
        for solution in solutions:
            for seed, result in sorted(runs.get((regime, solution), {}).items()):
                if not result.get("flags"):
                    continue
                flag_lines.append(f"- {regime} / {solution} / seed {seed}: "
                                  + "; ".join(result["flags"]))
                for kind in dig(result, "health.gabion_warnings.kinds") or []:
                    flag_lines.append(f"  - {kind['level']} {kind['target']} on "
                                      f"{kind['nodes']} nodes, max total "
                                      f"{kind['max_total']}: {kind['message']}")
    if len(flag_lines) == 4:
        flag_lines.append("None.")
    (args.out / "flags.md").write_text("\n".join(flag_lines) + "\n")

    for path, result in accepted:
        if not result.get("valid"):
            rejected.append((path, "INVALID (shown as INVALID k/n): "
                             + ", ".join(result.get("invalid_reasons", []))))
    rejected_lines = ["# Rejected results", ""]
    rejected_lines += [f"- `{p}`: {why}" for p, why in rejected] or ["None."]
    (args.out / "rejected.md").write_text("\n".join(rejected_lines) + "\n")
    invalid = sum(not r.get("valid") for _, r in accepted)
    print(f"{len(accepted) - invalid} valid and {invalid} INVALID results accepted, "
          f"{len(rejected) - invalid} rejected; wrote {args.out}/summary.md, summary.csv, "
          "pareto.md, flags.md, rejected.md")


if __name__ == "__main__":
    main()
