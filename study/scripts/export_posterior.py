#!/usr/bin/env python3
"""Write data/posterior.json from a judge summary.csv, keeping the metrics
listed in data/metrics.json. Numeric columns become floats, empty cells null.

    python3 study/scripts/export_posterior.py --summary <judge-out>/summary.csv \
        --metrics study/data/metrics.json --out study/data/posterior.json

Python standard library only.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

TEXT_COLUMNS = ("regime", "solution", "metric", "scale")


def read_summary(path: Path, keep: set[str]) -> list[dict]:
    rows = []
    with path.open(newline="") as fh:
        for r in csv.DictReader(fh):
            if r["metric"] not in keep:
                continue
            rows.append({k: (v or None) if k in TEXT_COLUMNS else (float(v) if v not in ("", None) else None)
                         for k, v in r.items()})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--summary", type=Path, required=True, help="the judge's summary.csv")
    ap.add_argument("--metrics", type=Path, required=True, help="study/data/metrics.json")
    ap.add_argument("--out", type=Path, required=True, help="posterior.json to write")
    args = ap.parse_args()
    keep = {m["metric"] for m in json.loads(args.metrics.read_text())}
    rows = read_summary(args.summary, keep)
    args.out.write_text(json.dumps(rows))
    print(len(rows), "posterior rows")


if __name__ == "__main__":
    main()
