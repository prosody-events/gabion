#!/usr/bin/env python3
"""Rewrite the datasets refresh.sh generates with compact separators and every
float rounded to 7 significant figures, which moves no value by more than
5e-7 of itself (docs/study/README.md, "Data"). Every row and key is kept.

    python3 docs/study/scripts/compact.py [<data-dir>]

Python standard library only.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

DATASETS = ("posterior.json", "self_posterior.json", "runs.json", "seed_metrics.json")


def compact(text: str) -> str:
    rows = json.loads(text)
    return json.dumps([{k: float(f"{v:.7g}") if isinstance(v, float) else v for k, v in r.items()} for r in rows],
                      separators=(",", ":"))


def main() -> None:
    data = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "data"
    for name in DATASETS:
        f = data / name
        f.write_text(compact(f.read_text()))


if __name__ == "__main__":
    main()
