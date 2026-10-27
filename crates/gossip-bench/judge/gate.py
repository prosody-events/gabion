#!/usr/bin/env python3
"""Regression gate: judge the `zone` arm against both baselines and apply the
pass rules (crate README, "The regression gate").

Usage:
    gate.py RESULTS --out OUT

RESULTS holds one directory per arm, as `matrix.py --solution <arm>` writes
them: `main` and `imp-az-hier-nopk` (`make sim-baselines`) and `zone` (the
checkout under test). Runs `judge.py` over the three once per comparator, into
OUT/<comparator>/, classifies every comparison with `verdict()`, writes
OUT/gate.md and exits 1 when a rule fails.

Python 3 standard library only.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path

from judge import core_hash
from verdict import verdict

HERE = Path(__file__).resolve().parent
MAIN, NOPK, ZONE = "main", "imp-az-hier-nopk", "zone"
SEEDS = 3
# The study's error metrics (study report §4.3): the 78 (regime, metric)
# comparisons every regime has, plus those only some regimes have.
ERROR_METRICS = {"conv_p50_incl_never_ms", "conv_p99_incl_never_ms", "conv_never_frac",
                 "stale_limited_p99_pct", "over_max_pct", "over_mean_pct",
                 "churn_parity_max_ms", "part_deficit_at_heal", "part_time_to_heal_ms",
                 "part_unrepaired_at_expiry"}
COST = ("bytes_total", "cross_az_bytes")
PROD = ("prod-typical", "prod-peak", "prod-4x")
PREDICTED_COST_RATIO = 0.93
NUMBERS = ("mean", "ratio", "ratio_lo", "ratio_hi", "diff_mean", "diff_lo", "diff_hi",
           "p_better")


def read(summary: Path) -> dict:
    """`(regime, solution, metric) -> row` of a judge `summary.csv`, numbers parsed."""
    with summary.open() as f:
        return {(r["regime"], r["solution"], r["metric"]):
                {**r, **{k: float(r[k]) if r[k] else None for k in NUMBERS},
                 "scale": r["scale"] or None, "n_valid": int(r["n_valid"])}
                for r in csv.DictReader(f)}


def verdicts(rows: dict, solution: str, comparator: str) -> dict:
    """`(regime, metric) -> (verdict, row)` for `solution` against `comparator`."""
    return {(regime, metric): (verdict(row, (rows.get((regime, comparator, metric)) or {})
                                       .get("mean")), row)
            for (regime, s, metric), row in rows.items() if s == solution}


def failures(regimes: list[str], rows: dict, vs_nopk: dict, vs_main: dict,
             nopk_vs_main: dict) -> list[str]:
    """Every broken rule, one line each. `rows` is a `read()` holding all arms."""
    fails = [f"G-v: {arm} has {(rows.get((r, arm, 'bytes_total')) or {}).get('n_valid', 0)} "
             f"valid seeds of {SEEDS} in {r}"
             for r in regimes for arm in (MAIN, NOPK, ZONE)
             if (rows.get((r, arm, "bytes_total")) or {}).get("n_valid", 0) < SEEDS]

    def worse(rule: str, comparisons: dict, metrics) -> None:
        fails.extend(f"{rule}: {m} in {r} is worse" for (r, m), (v, _) in
                     sorted(comparisons.items()) if m in metrics and v == "worse")

    worse("G-a1", vs_nopk, ERROR_METRICS)
    healed = (rows.get(("az-partition", ZONE, "part_time_to_heal_ms")) or {}).get("n_valid", 0)
    if healed < SEEDS:
        fails.append(f"G-a2: az-partition healed on {healed} of {SEEDS} seeds")
    for r in PROD:
        for m in COST:
            row = vs_nopk.get((r, m), (None, {}))[1]
            if not ((row.get("p_better") or 0) >= 0.95 and (row.get("ratio") or 1) < 1):
                fails.append(f"G-a3: {m} in {r} is not credibly lower "
                             f"(ratio {row.get('ratio')}, P {row.get('p_better')})")
    worse("G-a3", vs_nopk, COST)
    worse("G-a4", vs_nopk, {"drop_rcvbuf_pkts"})
    fails.extend(f"G-b1: {m} in {r} is {vs_main.get((r, m), (None,))[0]}, not better"
                 for r in regimes for m in COST if vs_main.get((r, m), (None,))[0] != "better")
    fails.extend(f"G-b2: {m} in {r} was better than main on {NOPK}, now "
                 f"{vs_main.get((r, m), (None,))[0]}" for (r, m), (v, _) in
                 sorted(nopk_vs_main.items()) if m in ERROR_METRICS and v == "better"
                 and vs_main.get((r, m), (None,))[0] != "better")
    worse("G-b3", vs_main, ERROR_METRICS)
    return fails


def git(*args: str) -> str:
    proc = subprocess.run(["git", "-C", str(HERE), *args], capture_output=True, text=True)
    return proc.stdout if proc.returncode == 0 else f"git {args[0]} failed: {proc.stderr}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("results", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    arms = (MAIN, NOPK, ZONE)
    for arm in arms:
        if not (args.results / arm).is_dir():
            sys.exit(f"gate: {args.results / arm} is missing: `make sim-baselines` writes "
                     f"{MAIN} and {NOPK}, `make sim-gate` writes {ZONE}")
    rows = {}
    for comparator in (MAIN, NOPK):
        subprocess.run([sys.executable, str(HERE / "judge.py"),
                        *(str(args.results / arm) for arm in arms), "--baseline", comparator,
                        "--out", str(args.out / comparator)], check=True)
        rows[comparator] = read(args.out / comparator / "summary.csv")
    regimes = sorted(p.stem for p in (HERE.parent / "regimes").glob("*.json"))
    vs_nopk = verdicts(rows[NOPK], ZONE, NOPK)
    fails = failures(regimes, rows[MAIN], vs_nopk, verdicts(rows[MAIN], ZONE, MAIN),
                     verdicts(rows[MAIN], NOPK, MAIN))
    stamps = {arm: json.loads(next((args.results / arm).rglob("seed-*.json")).read_text())
              ["stamp"] for arm in arms}
    lines = ["# Regression gate", "",
             f"Core hash `{core_hash(stamps[ZONE]['harness_tree_hash'], HERE)}`.", "",
             *(f"- {f}" for f in fails), "" if fails else "Every rule passes.", "",
             f"| Regime | Metric | Ratio to {NOPK} | P | Predicted |", "|---|---|---|---|---|"]
    for r in PROD:
        for m in COST:
            row = vs_nopk.get((r, m), (None, {}))[1]
            lines.append(f"| {r} | {m} | {row.get('ratio')} [{row.get('ratio_lo')}, "
                         f"{row.get('ratio_hi')}] | {row.get('p_better')} | "
                         f"{PREDICTED_COST_RATIO} |")
    for arm in (MAIN, NOPK):
        for path in ("src/harness/adapter.rs", "Cargo.toml"):
            path = f"crates/gossip-bench/{path}"
            lines += ["", f"## `{path}`: {arm} to {ZONE}", "", "```diff",
                      git("diff", f"{stamps[arm]['solution_commit']}:{path}",
                          f"{stamps[ZONE]['solution_commit']}:{path}").rstrip(), "```"]
    (args.out / "gate.md").write_text("\n".join(lines) + "\n")
    print("\n".join(fails) or "Every rule passes.", f"Wrote {args.out}/gate.md", sep="\n")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
