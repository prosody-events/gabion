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
           flags: list[str] | None = None, **extra) -> dict:
    return {
        **extra,
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

    def test_stale_schema_is_refused(self):
        stale = result("alt", 3, 5, harness="d" * 40)
        stale["schema"] = "gabion-study/1"
        proc, _ = self.judge([result("main", 1, 10), result("alt", 1, 5), stale])
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("gabion-study/1", proc.stderr)

    def test_foreign_json_is_listed_as_rejected(self):
        proc, out = self.judge([result("main", 1, 10), {"hello": "world"}])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("not a study result", (out / "rejected.md").read_text())

    def test_pareto_uses_seeds_common_to_every_solution(self):
        proc, out = self.judge([
            result("main", 1, 10), result("main", 2, 12), result("main", 3, 14),
            result("alt", 1, 20), result("alt", 2, 22), result("alt", 3, 99, valid=False),
        ])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        pareto = (out / "pareto.md").read_text()
        self.assertIn("Seeds 1, 2.", pareto)
        # main over seeds 1-2 only: bytes 11, staleness 1.1.
        self.assertIn("| **main** | 11 | 1.1 |", pareto)

    def test_flags_are_listed(self):
        proc, out = self.judge([result("main", 1, 10, flags=["dirty_ring_overflow: 3 nodes"])])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("r / main / seed 1: dirty_ring_overflow: 3 nodes",
                      (out / "flags.md").read_text())

    def test_pareto_without_a_common_seed_lists_points_without_a_front(self):
        proc, out = self.judge([result("main", 1, 10), result("alt", 2, 20)])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        pareto = (out / "pareto.md").read_text()
        self.assertIn("No seed is valid for every solution", pareto)
        self.assertIn("| main | 10 | 1 |", pareto)
        self.assertIn("| alt | 20 | 2 |", pareto)
        self.assertNotIn("**main**", pareto)

    def test_pareto_metric_absent_for_every_solution_is_not_applicable(self):
        proc, out = self.judge([result("main", 1, 10), result("alt", 1, 20)])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        pareto = (out / "pareto.md").read_text()
        self.assertIn("bytes_total vs over_max_pct: metric not applicable in this regime",
                      pareto)
        self.assertIn("Excluded, no value for bytes_total or over_max_pct: main, alt.", pareto)
        # The pair both solutions do have is unaffected.
        self.assertIn("| **main** | 10 | 1 |", pareto)
        self.assertNotIn("No seed is valid", pareto)

    def test_pareto_excludes_only_the_solution_lacking_the_metric(self):
        over = {"accuracy": [{"over_max_pct_limit": 5.0, "over_mean_pct_limit": 1.0}]}
        proc, out = self.judge([result("main", 1, 10, **over), result("alt", 1, 20)])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        pareto = (out / "pareto.md").read_text()
        self.assertIn("| **main** | 10 | 5 |", pareto)
        self.assertIn("Excluded, no value for bytes_total or over_max_pct: alt.", pareto)

    def test_partition_phases_get_their_own_table_only_where_the_regime_has_a_cut(self):
        phase = {"convergence": {"same_side": {"p99_incl_never_ms": 800.0},
                                 "cross_side": {"p99_incl_never_ms": 9_000.0,
                                                "never_fraction": 0.6}},
                 "staleness": {"in_zone": {"limited": {"p99": 40.0}},
                               "rest": {"limited": {"p99": 20.0}}}}
        cut = {"partition": {"time_to_heal_ms": 6_200.0, "deficit_at_heal_hits": 2.5,
                             "before": phase, "during": phase, "after": phase}}
        proc, out = self.judge([result("main", 1, 10, regime="az", **cut),
                                result("main", 1, 10, regime="plain", partition=None)])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        summary = (out / "summary.md").read_text()
        self.assertEqual(summary.count("partition phases"), 1)
        self.assertIn("| Time to heal: until every cross-cut count is exact (ms) | 6,200 |",
                      summary)
        self.assertIn("| during: fraction never converged, cross side | 0.6 |", summary)
        rows = self.rows(out)
        self.assertEqual(rows[("az", "main", "part_time_to_heal_ms")]["mean"], "6200.0")
        self.assertEqual(rows[("az", "main", "part_during_stale_in_zone_limited_p99")]["mean"],
                         "40.0")
        self.assertNotIn(("plain", "main", "part_time_to_heal_ms"), rows)


if __name__ == "__main__":
    unittest.main()
