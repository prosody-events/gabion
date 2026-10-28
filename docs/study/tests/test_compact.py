#!/usr/bin/env python3
"""compact.py keeps every row and key and moves no float by more than 5e-7 of
itself. The judge's summary.csv holds the raw posterior, so the committed
posterior.json is checked against it; every generated dataset is checked to be
compact.py's own output.

    python3 docs/study/tests/test_compact.py
"""

import json
import math
import sys
import unittest
from pathlib import Path

STUDY = Path(__file__).resolve().parent.parent
DATA = STUDY / "data"
sys.path.insert(0, str(STUDY / "scripts"))
from compact import DATASETS, compact  # noqa: E402
from export_posterior import read_summary  # noqa: E402


class CompactTest(unittest.TestCase):
    def test_the_raw_posterior_compacts_to_the_committed_one(self):
        keep = {m["metric"] for m in json.loads((DATA / "metrics.json").read_text())}
        raw = read_summary(DATA / "judge" / "summary.csv", keep)
        compacted = compact(json.dumps(raw))
        self.assertEqual(compacted, (DATA / "posterior.json").read_text())
        for r, c in zip(raw, json.loads(compacted), strict=True):
            self.assertEqual(list(r), list(c))
            for k, v in r.items():
                if isinstance(v, float):
                    self.assertTrue(math.isclose(v, c[k], rel_tol=5e-7), f"{k}: {v} became {c[k]}")
                else:
                    self.assertEqual(v, c[k])

    def test_every_generated_dataset_is_compacted(self):
        for name in DATASETS:
            text = (DATA / name).read_text()
            self.assertEqual(compact(text), text, f"{name} is not compact.py's output; run compact.py")


if __name__ == "__main__":
    unittest.main()
