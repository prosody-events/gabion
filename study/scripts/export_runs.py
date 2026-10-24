#!/usr/bin/env python3
"""Write data/runs.json: one row per run (regime, solution, seed, validity,
solution commit, harness tree hash), read from matrix.py result directories.

    python3 study/scripts/export_runs.py --out study/data/runs.json <results-dir> [<results-dir> ...]

Each results directory holds <solution>/<regime>/seed-<n>.json as written by
crates/gossip-bench/judge/matrix.py. Python standard library only.
"""

from __future__ import annotations

import argparse
import datetime
import json
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True, help="runs.json to write")
    ap.add_argument("results", type=Path, nargs="+", help="matrix.py result directories")
    args = ap.parse_args()
    # When the snapshot was taken, so a reader can tell how old it is.
    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    runs = []
    for src in args.results:
        for f in sorted(src.glob("*/*/seed-*.json")):
            r = json.loads(f.read_text())
            st = r["stamp"]
            runs.append({"regime": r["regime_id"], "arm": st["solution"], "seed": r["seed"],
                         "valid": bool(r.get("valid")), "commit": st["solution_commit"][:12],
                         "harness": st["harness_tree_hash"][:12], "generated_at": generated_at})
    runs.sort(key=lambda r: (r["arm"], r["regime"], r["seed"]))
    args.out.write_text(json.dumps(runs))
    print(len(runs), "runs")


if __name__ == "__main__":
    main()
