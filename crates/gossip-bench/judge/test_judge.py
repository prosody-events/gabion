#!/usr/bin/env python3
"""Tests for judge.py on synthetic results.

Run: python3 -m unittest discover -s crates/gossip-bench/judge
"""

from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

JUDGE = Path(__file__).with_name("judge.py")
HASH = "a" * 40


def result(solution: str, seed: int, bytes_sent: float, *, regime: str = "r",
           valid: bool = True, harness: str = HASH, dirty: bool = False,
           flags: list[str] | None = None) -> dict:
    return {
        "schema": "gabion-study/2",
        "regime_id": regime,
        "seed": seed,
        "valid": valid,
        "invalid_reasons": [] if valid else ["cell_store_full_rejects = 1"],
        "flags": flags or [],
        "network": {"measure": {"bytes_sent": bytes_sent}},
        "staleness": {"limited": {"p99": bytes_sent / 10}},
        "stamp": {
            "harness_tree_hash": harness,
            "harness_dirty": False,
            "solution": solution,
            "solution_branch": f"b/{solution}",
            "solution_commit": solution[0] * 40,
            "solution_dirty": dirty,
        },
    }


class JudgeTest(unittest.TestCase):
    def judge(self, results: list[dict]) -> tuple[subprocess.CompletedProcess, Path]:
        tmp = Path(tempfile.mkdtemp())
        for i, r in enumerate(results):
            (tmp / f"{i}.json").write_text(json.dumps(r))
        out = tmp / "report"
        proc = subprocess.run([sys.executable, str(JUDGE), str(tmp), "--out", str(out)],
                              capture_output=True, text=True)
        return proc, out

    def rows(self, out: Path) -> dict[tuple[str, str, str], dict]:
        with (out / "summary.csv").open() as f:
            return {(r["regime"], r["solution"], r["metric"]): r for r in csv.DictReader(f)}

    def test_invalid_seed_is_shown_and_ratio_uses_common_seeds(self):
        proc, out = self.judge([
            result("main", 1, 10), result("main", 2, 12), result("main", 3, 14),
            result("alt", 1, 20), result("alt", 2, 22), result("alt", 3, 99, valid=False),
        ])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        summary = (out / "summary.md").read_text()
        self.assertIn("INVALID 1/3; 21 ± 12.7 [20–22] (×1.91)", summary)
        self.assertIn("Valid seeds: main 3/3, alt 2/3", summary)
        row = self.rows(out)[("r", "alt", "bytes_total")]
        self.assertEqual((row["n_valid"], row["n_invalid"], row["paired_n"]), ("2", "1", "2"))
        self.assertAlmostEqual(float(row["ratio_vs_baseline"]), 21 / 11)
        self.assertAlmostEqual(float(row["paired_diff_mean"]), 10.0)
        self.assertIn("INVALID", (out / "rejected.md").read_text())
        self.assertIn("alt (INVALID 1/3)", (out / "pareto.md").read_text())

    def test_all_invalid_solution_stays_in_tables_and_leaves_the_front(self):
        proc, out = self.judge([
            result("main", 1, 10), result("bad", 1, 1, valid=False),
        ])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("| INVALID 1/1 |", (out / "summary.md").read_text())
        pareto = (out / "pareto.md").read_text()
        self.assertIn("Excluded, every seed invalid: bad (INVALID 1/1).", pareto)
        self.assertNotIn("| bad |", pareto)

    def test_dirty_solution_is_rejected(self):
        proc, out = self.judge([result("main", 1, 10), result("alt", 1, 5, dirty=True)])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertNotIn("alt", (out / "summary.md").read_text())
        self.assertIn("solution worktree had uncommitted changes",
                      (out / "rejected.md").read_text())

    def test_mixed_harness_hashes_are_refused(self):
        proc, _ = self.judge([result("main", 1, 10),
                              result("alt", 1, 5, harness="b" * 40, valid=False)])
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("different harnesses", proc.stderr)

    def test_flags_are_listed(self):
        proc, out = self.judge([result("main", 1, 10, flags=["dirty_ring_overflow: 3 nodes"])])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("r / main / seed 1: dirty_ring_overflow: 3 nodes",
                      (out / "flags.md").read_text())


if __name__ == "__main__":
    unittest.main()
