#!/usr/bin/env python3
"""Paired posteriors against comparators other than baseline main, using the
harness judge unchanged. Writes data/self_posterior.json, whose `comparison`
column is one of:

- vs_own_baseline: imp-<arm>[-nopk] against <arm> (imp-main[-nopk] against main);
- perkey_effect:   imp-<arm> against imp-<arm>-nopk;
- vs_reference:    every improved tip without the per-key fix, and imp-owner-agg,
                   against the --reference tip (default imp-az-hier-nopk).

Each comparison is one judge run over a scratch directory holding only the
solutions it compares, so `--baseline` names the comparator.

    python3 study/scripts/self_judge.py \
        --judge <harness>/crates/gossip-bench/judge/judge.py \
        --baseline-results <baseline-results> --improved-results <improved-results> \
        --arms study/data/arms.json --metrics study/data/metrics.json \
        --work <scratch-dir> --out study/data/self_posterior.json

Python standard library and rsync.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from export_posterior import read_summary  # noqa: E402


def judge(judge_py: Path, work: Path, keep: set[str], name: str, baseline: str,
          members: dict[str, Path]) -> list[dict]:
    d = work / name
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    for sol, src in members.items():
        if not (src / sol).exists():
            return []
        # Copies, not symlinks: the judge's glob does not descend into symlinked dirs.
        subprocess.run(["rsync", "-a", "--include=*/", "--include=seed-*.json", "--exclude=*",
                        f"{src / sol}/", str(d / sol)], check=True)
    out = work / f"{name}-judge"
    subprocess.run([sys.executable, str(judge_py), str(d), "--baseline", baseline, "--out", str(out)],
                   check=True, stdout=subprocess.DEVNULL)
    rows = [r for r in read_summary(out / "summary.csv", keep) if r["solution"] != baseline]
    for r in rows:
        r["against"] = baseline
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--judge", type=Path, required=True, help="crates/gossip-bench/judge/judge.py")
    ap.add_argument("--baseline-results", type=Path, required=True, help="results of the baseline arms")
    ap.add_argument("--improved-results", type=Path, required=True, help="results of the imp-* tips")
    ap.add_argument("--arms", type=Path, required=True, help="study/data/arms.json")
    ap.add_argument("--metrics", type=Path, required=True, help="study/data/metrics.json")
    ap.add_argument("--work", type=Path, required=True, help="scratch directory (overwritten)")
    ap.add_argument("--out", type=Path, required=True, help="self_posterior.json to write")
    ap.add_argument("--reference", default="imp-az-hier-nopk", help="comparator for vs_reference")
    args = ap.parse_args()

    keep = {m["metric"] for m in json.loads(args.metrics.read_text())}
    arms = json.loads(args.arms.read_text())
    base, imp = args.baseline_results, args.improved_results
    run = lambda name, baseline, members: judge(args.judge, args.work, keep, name, baseline, members)  # noqa: E731

    rows = []
    tips = []
    for a in arms:
        arm, improved, nopk = a["arm"], a.get("improved"), a.get("improved_nopk")
        if not improved:
            continue
        tips.append(nopk or improved)
        members = {arm: base, improved: imp}
        if nopk:
            members[nopk] = imp
        for r in run(f"own-{arm}", arm, members):
            r["comparison"] = "vs_own_baseline"
            rows.append(r)
        if nopk:
            for r in run(f"perkey-{arm}", nopk, {nopk: imp, improved: imp}):
                r["comparison"] = "perkey_effect"
                rows.append(r)
    for r in run("reference", args.reference, {t: imp for t in tips}):
        r["comparison"] = "vs_reference"
        rows.append(r)
    args.out.write_text(json.dumps(rows))
    print(len(rows), "self-posterior rows")


if __name__ == "__main__":
    main()
