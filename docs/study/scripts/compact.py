#!/usr/bin/env python3
"""Rewrite the datasets refresh.sh generates with compact separators. Every
row, key and value is kept exactly (docs/study/README.md, "Data").

    python3 docs/study/scripts/compact.py [<data-dir>]

Python standard library only.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

DATASETS = ("posterior.json", "self_posterior.json", "runs.json", "seed_metrics.json")


def compact(text: str) -> str:
    return json.dumps(json.loads(text), separators=(",", ":"))


def main() -> None:
    data = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "data"
    for name in DATASETS:
        f = data / name
        f.write_text(compact(f.read_text()))


if __name__ == "__main__":
    main()
