#!/usr/bin/env python3
"""Regression gate: judge the `zone` arm against both baselines and apply the
pass rules (crate README, "The regression gate").

Usage:
    gate.py RESULTS --out OUT

RESULTS holds one directory per arm, as `matrix.py --solution <arm>` writes
them: `main` and `imp-az-hier-nopk` (`make sim-baselines`) and `zone` (the
checkout under test). Runs `judge.py` over the three once per comparator, into
OUT/<comparator>/, classifies every comparison with `verdict()`, writes
OUT/gate.md and exits 1 when a rule fails. gate.md also checks §6.5's cost
model against the zone arm (crate README, "The regression gate"); that check
never changes the exit code.

Python 3 standard library only.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import sys
from pathlib import Path

from judge import core_hash, fmt
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
           "p_better", "cri_lo", "cri_hi")
# §6.5's cost model reads the defaults and wire sizes it was derived with;
# test_gate.py checks each against its Rust source.
TICK_MS = 500                                 # defaults::GOSSIP_TICK_INTERVAL_MILLIS, T
ERR_BPS = 100                                 # defaults::GOSSIP_TARGET_ERR_BPS, β
SUSPECT_TICKS, SUSPECT_FLOOR_MS = 4, 2_000    # defaults::GOSSIP_SUSPECT_AFTER_{TICKS,FLOOR_MS}
ZONES = 3                                     # harness plan::AVAILABILITY_ZONES, Z
IP_UDP_BYTES, HEADER_BYTES = 28, 86           # harness net::IP_UDP_HEADER_BYTES, wire HEADER_LEN
DICT_LEN_BYTES, CELL_BYTES = 2, 38            # wire body DICT_LEN_BYTES, CELL_BYTES
TICK_S = TICK_MS / 1000
ACK_BYTES = IP_UDP_BYTES + HEADER_BYTES             # a datagram with no cells
DATA_BYTES = ACK_BYTES + DICT_LEN_BYTES + 16 + 16   # plus one rule and one origin
MODEL_BAND = 0.05
STREAM_SIZES = "the quantum `q` (from `storage.max_cells`) or `gossip.tick_interval`"
# (summary.csv metric, equation, the setting the equation sizes)
MODEL = (("packets_per_node_s", "(6.3)", STREAM_SIZES),
         ("bytes_per_node_s", "(6.3)", STREAM_SIZES),
         ("cross_az_bytes_per_node_s", "(6.3)", STREAM_SIZES),
         ("conv_p99_incl_never_ms", "(6.5)", "`gossip.tick_interval`"),
         ("peak_cells", "(6.10)", "`STORAGE_MAX_CELLS` (`storage.max_cells`)"))
MODEL_INTRO = (
    "Never changes the exit code. Each prediction uses the zone arm's measured admitted "
    "rate, the regime and the defaults; measured is the mean over valid seeds [95 % "
    "credible interval]. A metric more than 5 % off its prediction is named below its "
    "table. (6.3)'s `a`, the acknowledgements sent alone, has no equation in §6.5: the "
    "predictions count only its cross-zone part, (Z − 1) / T · (1 − e^(−λT)), and put "
    "λ / R_own cells in each data packet. `missing` means no value: no valid seed, no "
    "admitted hits, a regime outside (6.2)'s ε = 1, or a metric the run's result does "
    "not carry.")


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


def failures(regimes: list[str], rows: dict, nopk_rows: dict) -> list[str]:
    """Every broken rule, one line each. `rows` is the judge table against `main`
    and `nopk_rows` the one against `imp-az-hier-nopk`, each a `read()` holding
    all arms."""
    vs_nopk, vs_main, nopk_vs_main = (verdicts(nopk_rows, ZONE, NOPK),
                                      verdicts(rows, ZONE, MAIN), verdicts(rows, NOPK, MAIN))
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


def p99_ms(rate: float) -> float:
    """The root of (6.5), e^(−λt)(1 − t/T) = 0.02, by bisection on [0, T],
    where the left side falls from 1 to 0."""
    lo, hi = 0.0, TICK_S
    for _ in range(60):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if math.exp(-rate * mid) * (1 - mid / TICK_S) > 0.02 else (lo, mid)
    return 1000 * lo


def predict(regime: dict, admitted: float | None) -> dict:
    """§6.5's prediction of each `MODEL` metric for `regime` at the zone arm's
    mean admitted hits in the measurement window. Empty, so every metric reads
    missing, without traffic or where (3.2) gives ε ≠ 1, outside (6.2)."""
    if not admitted:
        return {}
    n, rule = regime["nodes"], regime["rule"]
    total = admitted / (regime["duration_s"] - regime["warmup_s"])           # Λ
    rate = total / n                                                          # λ
    if max(1, rule["limit"] * ERR_BPS // (10_000 * (n - 1))) != 1:           # (3.2)
        return {}
    own = rate / 2 + (1 - math.exp(-2 * rate * TICK_S)) / (4 * TICK_S)       # (6.2), R_own
    data = own * (n - 1)                                                      # (6.3) data
    # (6.3)'s `a` has no equation; only its cross-zone part is predicted: each
    # node gateways Z − 1 remote origins and acks each at its heartbeat (§6.5.2)
    # when the origin admitted a hit in the interval.
    acks = (ZONES - 1) / TICK_S * (1 - math.exp(-rate * TICK_S))
    packet = DATA_BYTES + CELL_BYTES * rate / own                             # c_p = λ / R_own
    return {"packets_per_node_s": data + acks,
            "bytes_per_node_s": data * packet + acks * ACK_BYTES,
            "cross_az_bytes_per_node_s": own * (ZONES - 1) * packet + acks * ACK_BYTES,
            "conv_p99_incl_never_ms": p99_ms(rate),
            "peak_cells": total * (rule["window_ms"] + rule["bucket_ms"]) / 1000}  # (6.10)


def false_suspicion(regime: dict) -> str:
    """§6.5.8's chance that an episode ends in a false suspicion here, at
    n_a = ⌊T_u / T⌋ answer attempts and at its floor, one fewer."""
    attempts = max(SUSPECT_TICKS * TICK_MS, SUSPECT_FLOOR_MS) // TICK_MS     # n_a
    network = regime.get("network", {})
    if "burst_loss" in network:
        stay, unit = 1 - network["burst_loss"]["p_bad_to_good"], "loss burst"
        odds = [stay ** (k - 1) for k in (attempts, attempts - 1)]
    else:
        fail, unit = 1 - (1 - network.get("loss", 0.0)) ** 2, "late answer"   # p_a
        odds = [fail ** k for k in (attempts, attempts - 1)]
    return (f"{odds[0]:.2g} (n_a = {attempts}) to {odds[1]:.2g} (n_a = {attempts - 1}) "
            f"per {unit}")


def model_check(regimes: dict, rows: dict) -> tuple[list[str], int]:
    """gate.md's not-gating section: per regime, §6.5's predictions beside the
    zone arm's measured mean [credible interval], and how many miss by more
    than `MODEL_BAND`."""
    lines, misses = ["", "## §6.5's cost model against the zone arm (not gating)", "",
                     MODEL_INTRO], 0
    for r, regime in regimes.items():
        predicted = predict(regime, (rows.get((r, ZONE, "admitted")) or {}).get("mean"))
        lines += ["", f"### {r}", "", "| Metric | Equation | Predicted | Measured | Off by |",
                  "|---|---|---|---|---|"]
        notes = [f"- `gateway_failovers`: missing, the run's result does not carry it (that "
                 f"needs a harness-core field). §6.5.8 predicts a false suspicion in "
                 f"{false_suspicion(regime)}; `gossip.suspect_after` sizes it."]
        for metric, equation, sizes in MODEL:
            row = rows.get((r, ZONE, metric)) or {}
            p, m = predicted.get(metric), row.get("mean")
            off = "missing"
            if p is not None and m is not None:
                miss = (m - p) / p
                off = f"{100 * miss:+.1f} %"
                if abs(miss) > MODEL_BAND:
                    misses += 1
                    notes.append(f"- `{metric}`: §6's defaults rest on a model that "
                                 f"misses here by {100 * abs(miss):.1f}%; the owner decides. "
                                 f"{equation} sizes {sizes}.")
            shown = ("missing" if m is None
                     else f"{fmt(m)} [{fmt(row.get('cri_lo'))}, {fmt(row.get('cri_hi'))}]")
            lines.append(f"| `{metric}` | {equation} | {'missing' if p is None else fmt(p)} | "
                         f"{shown} | {off} |")
        lines += ["", *notes]
    return lines, misses


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
        if subprocess.run([sys.executable, str(HERE / "judge.py"),
                           *(str(args.results / arm) for arm in arms), "--baseline", comparator,
                           "--out", str(args.out / comparator)]).returncode:
            sys.exit(f"gate: judge.py refused the arms against {comparator} (its reason is "
                     "above). Regenerate the arm it names (`make sim-baselines` or "
                     "`make sim-gate`) and rerun.")
        rows[comparator] = read(args.out / comparator / "summary.csv")
    regimes = {p.stem: json.loads(p.read_text())
               for p in sorted((HERE.parent / "regimes").glob("*.json"))}
    fails = failures(list(regimes), rows[MAIN], rows[NOPK])
    vs_nopk = verdicts(rows[NOPK], ZONE, NOPK)
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
    # The zone arm's own mean and interval do not depend on the comparator.
    model, misses = model_check(regimes, rows[NOPK])
    lines += model
    for path in ("src/harness/adapter.rs", "Cargo.toml"):
        path = f"crates/gossip-bench/{path}"
        lines += ["", f"## `{path}`: baselines (H1's copy) to {ZONE}", "", "```diff",
                  git("diff", f"{stamps[MAIN]['solution_commit']}:{path}",
                      f"{stamps[ZONE]['solution_commit']}:{path}").rstrip(), "```"]
    (args.out / "gate.md").write_text("\n".join(lines) + "\n")
    print("\n".join(fails) or "Every rule passes.",
          f"Cost model (not gating): {misses} comparisons miss §6.5 by more than 5 %; gate.md "
          "names the setting each sizes, and the owner decides.",
          f"Wrote {args.out}/gate.md", sep="\n")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
