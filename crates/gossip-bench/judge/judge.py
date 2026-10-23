#!/usr/bin/env python3
"""Aggregate stamped study results into per-regime comparison tables.

Usage:
    judge.py RESULTS_DIR [RESULTS_DIR ...] --out REPORT_DIR [--baseline main]

Reads every `*.json` result under the given directories (as written by
`matrix.py`). Refuses to proceed when results carry different harness tree
hashes or another `gabion-study/*` schema (stale results from an older
harness), or one solution label carries several commits: numbers from
different code are not comparable. Results without a stamp or schema, or
produced from a dirty harness or solution worktree, are rejected and listed.

Runs the harness marked INVALID (a health invariant failed) stay visible:
every cell of a (regime, solution) with an invalid seed reads
`INVALID k/n`, and a solution whose every seed is invalid stays in the
tables as INVALID rather than disappearing. Statistics cover valid seeds
only; ratios to the baseline and paired differences use only the seeds valid
for both, so a solution cannot gain by losing its hardest seed.

For each regime x solution x metric it reports the mean across valid seeds
with a 95 % confidence half-width (Student t, n - 1 degrees of freedom), the
min and max, and the ratio to the baseline over common seeds. Writes
`summary.md` (with a per-phase table for regimes that cut the network),
`summary.csv` (long format, with paired differences),
`pareto.md` (means over the seeds valid for every solution on the front),
`flags.md` (saturation the harness flagged without invalidating, e.g. dirty-ring
overflow) and `rejected.md`.

Python 3 standard library only.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

SCHEMA = "gabion-study/2"
SCHEMA_FAMILY = "gabion-study/"

# Two-sided 95 % Student t quantiles t(0.975, df) for df = 1..30.
T975 = [12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
        2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086,
        2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042]


def t975(df: int) -> float:
    if df <= 30:
        return T975[df - 1]
    if df <= 40:
        return 2.021
    if df <= 60:
        return 2.000
    if df <= 120:
        return 1.980
    return 1.960


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


# (key, label, extractor). Lower is better for every metric except where
# noted in the label; the Pareto view uses the starred ones.
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
        ("part_time_to_heal_ms", "Time to heal: until every cross-cut count is exact (ms)",
         lambda r: dig(r, "partition.time_to_heal_ms")),
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
ALL_METRICS = METRICS + PARTITION_METRICS

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


def summarize(values: list[float]) -> dict:
    n = len(values)
    mean = statistics.fmean(values)
    ci = t975(n - 1) * statistics.stdev(values) / math.sqrt(n) if n > 1 else None
    return {"n": n, "mean": mean, "ci95": ci, "min": min(values), "max": max(values)}


def fmt(x) -> str:
    if x is None:
        return "n/a"
    return f"{x:,.0f}" if abs(x) >= 1000 else f"{x:.3g}"


def cell(stats: dict | None, ratio: float | None, invalid: int, total: int) -> str:
    mark = f"INVALID {invalid}/{total}" if invalid else ""
    if stats is None:
        return mark or "n/a"
    text = (mark + "; " if mark else "") + fmt(stats["mean"])
    if stats["ci95"] is not None:
        text += f" ± {fmt(stats['ci95'])}"
    if stats["n"] > 1:
        text += f" [{fmt(stats['min'])}–{fmt(stats['max'])}]"
    if ratio is not None:
        text += f" (×{ratio:.2f})"
    return text


def dominated(a: tuple, b: tuple) -> bool:
    """`a` is dominated by `b` when `b` is no worse on both and better on one."""
    return b[0] <= a[0] and b[1] <= a[1] and (b[0] < a[0] or b[1] < a[1])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("results", type=Path, nargs="+")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--baseline", default="main")
    args = parser.parse_args()

    accepted, rejected = load(args.results)
    hashes = {r["stamp"]["harness_tree_hash"] for _, r in accepted}
    if len(hashes) > 1:
        sys.exit("error: results come from different harnesses (tree hashes "
                 f"{', '.join(sorted(hashes))}); rerun every solution on one harness commit.")
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
            values = [v for r in valid.values() if (v := extract(r)) is not None]
            table[(regime, solution, key)] = summarize([float(v) for v in values]) if values else None

    def paired(regime: str, solution: str, key: str) -> dict | None:
        """Ratio of means and mean paired difference (solution - baseline)
        over the seeds valid for both with a value for `key`."""
        extract = next(e for k, _, e in ALL_METRICS if k == key)
        mine, base = valid_seeds(regime, solution), valid_seeds(regime, args.baseline)
        pairs = [(float(a), float(b)) for s in sorted(mine.keys() & base.keys())
                 if (a := extract(mine[s])) is not None and (b := extract(base[s])) is not None]
        if not pairs:
            return None
        base_mean = statistics.fmean(b for _, b in pairs)
        diff = summarize([a - b for a, b in pairs])
        return {
            "n": len(pairs),
            "ratio": statistics.fmean(a for a, _ in pairs) / base_mean if base_mean else None,
            "diff_mean": diff["mean"],
            "diff_ci95": diff["ci95"],
        }

    def ratio(regime: str, solution: str, key: str):
        if solution == args.baseline:
            return None
        pair = paired(regime, solution, key)
        return pair["ratio"] if pair else None

    args.out.mkdir(parents=True, exist_ok=True)
    harness = next(iter(hashes))
    lines = [
        "# Gossip study: per-regime comparison", "",
        f"Harness tree hash `{harness}`. Baseline `{args.baseline}`. Cells show the mean "
        "across valid seeds ± the 95 % CI half-width (Student t) [min–max] (×ratio of "
        "means to the baseline over the seeds valid for both). `INVALID k/n` marks a "
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
        def rows(metrics):
            return [f"| {label} | "
                    + " | ".join(cell(table.get((regime, s, key)), ratio(regime, s, key),
                                      *counts[s]) for s in present) + " |"
                    for key, label, _ in metrics]

        lines += rows(METRICS)
        if regime in partitioned:
            lines += ["", f"### {regime}: partition phases", "",
                      "The cut isolates one availability zone, then heals. Convergence is "
                      "of hits admitted in the phase: `same side` waits for the pods on "
                      "the origin's side, `cross side` for the other side. Staleness is "
                      "by the observer's side. The heal is probed every 100 ms.", "",
                      "| Metric | " + " | ".join(present) + " |",
                      "|---|" + "---|" * len(present)] + rows(PARTITION_METRICS)
    (args.out / "summary.md").write_text("\n".join(lines) + "\n")

    with (args.out / "summary.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["regime", "solution", "metric", "n_valid", "n_invalid", "n_total",
                         "mean", "ci95", "min", "max", "paired_n", "ratio_vs_baseline",
                         "paired_diff_mean", "paired_diff_ci95"])
        for regime in regimes:
            for solution in solutions:
                if (regime, solution) not in runs:
                    continue
                bad, total = invalid_count(regime, solution)
                for key, _, _ in METRICS + (PARTITION_METRICS if regime in partitioned else []):
                    stats = table.get((regime, solution, key)) or {}
                    pair = (paired(regime, solution, key)
                            if solution != args.baseline else None) or {}
                    writer.writerow([regime, solution, key, stats.get("n", 0), bad, total,
                                     stats.get("mean"), stats.get("ci95"), stats.get("min"),
                                     stats.get("max"), pair.get("n"), pair.get("ratio"),
                                     pair.get("diff_mean"), pair.get("diff_ci95")])

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
