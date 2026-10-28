#!/usr/bin/env python3
"""Write data/runs.json: one row per run (regime, solution, seed, validity,
solution commit, harness tree hash, and run_at, when the run finished), read from
matrix.py result directories.

    python3 docs/study/scripts/export_runs.py --out docs/study/data/runs.json \
        [--run-at-default <ISO 8601>] <results-dir> [<results-dir> ...]

Each results directory holds <solution>/<regime>/seed-<n>.json as written by
crates/gossip-bench/judge/matrix.py. Python standard library only.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True, help="runs.json to write")
    ap.add_argument("--run-at-default", help="run_at for a result without finished_at (written before matrix.py recorded one)")
    ap.add_argument("results", type=Path, nargs="+", help="matrix.py result directories")
    args = ap.parse_args()
    runs = []
    for src in args.results:
        for f in sorted(src.glob("*/*/seed-*.json")):
            r = json.loads(f.read_text())
            st = r["stamp"]
            runs.append({"regime": r["regime_id"], "arm": st["solution"], "seed": r["seed"],
                         "valid": bool(r.get("valid")), "commit": st["solution_commit"][:12],
                         "harness": st["harness_tree_hash"][:12], "run_at": r.get("finished_at", args.run_at_default)})
    runs.sort(key=lambda r: (r["arm"], r["regime"], r["seed"]))
    args.out.write_text(json.dumps(runs))
    print(len(runs), "runs")


if __name__ == "__main__":
    main()
