#!/usr/bin/env python3
"""Write data/seed_metrics.json: one row per run and metric, the per-seed values
the dashboard's detail pane draws, read with the judge's own metric extractors.

    python3 docs/study/scripts/export_seeds.py --judge <harness>/crates/gossip-bench/judge/judge.py \
        --out docs/study/data/seed_metrics.json <results-dir> [<results-dir> ...]

Each results directory holds <solution>/<regime>/seed-<n>.json as written by
matrix.py. Python standard library only.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

METRICS = ("bytes_total", "cross_az_bytes", "conv_p50_incl_never_ms", "conv_p99_incl_never_ms", "conv_never_frac",
           "stale_limited_p99_pct", "over_max_pct", "over_mean_pct", "drop_rcvbuf_pkts", "send_pending_max",
           "churn_parity_max_ms", "part_deficit_at_heal", "part_time_to_heal_ms", "part_unrepaired_at_expiry")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--judge", type=Path, required=True, help="the harness judge.py")
    ap.add_argument("--out", type=Path, required=True, help="seed_metrics.json to write")
    ap.add_argument("results", type=Path, nargs="+", help="matrix.py result directories")
    args = ap.parse_args()
    spec = importlib.util.spec_from_file_location("judge", args.judge)
    judge = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(judge)
    extract = {k: e for k, _, e in judge.ALL_METRICS}
    files = sorted((f.relative_to(src), f) for src in args.results for f in src.glob("*/*/seed-*.json"))
    rows = []
    for _, f in files:
        r = json.loads(f.read_text())
        for k in METRICS:
            v = extract[k](r)
            if v is not None:
                rows.append({"regime": r["regime_id"], "arm": r["stamp"]["solution"], "seed": r["seed"],
                             "valid": bool(r.get("valid")), "metric": k, "value": float(v)})
    args.out.write_text(json.dumps(rows))
    print(len(rows), "seed rows")


if __name__ == "__main__":
    main()
