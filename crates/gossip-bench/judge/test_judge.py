#!/usr/bin/env python3
"""Tests for judge.py on synthetic results.

Run: python3 -m unittest discover -s crates/gossip-bench/judge
"""

from __future__ import annotations

import csv
import importlib.util
import json
import math
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

JUDGE = Path(__file__).with_name("judge.py")
spec = importlib.util.spec_from_file_location("judge", JUDGE)
judge_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(judge_module)
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
    def judge(self, results: list[dict], *args: str) -> tuple[subprocess.CompletedProcess, Path]:
        tmp = Path(tempfile.mkdtemp())
        for i, r in enumerate(results):
            (tmp / f"{i}.json").write_text(json.dumps(r))
        out = tmp / "report"
        proc = subprocess.run([sys.executable, str(JUDGE), str(tmp), "--out", str(out), *args],
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
        # alt over seeds 1-2: mean 21, se 1, t_1 quantile 12.706 -> [8.29, 33.7];
        # paired log ratios ln 2 and ln(22/12).
        self.assertIn("INVALID 1/3; 21 [8.29, 33.7] (×1.91 [", summary)
        self.assertIn("Valid seeds: main 3/3, alt 2/3", summary)
        row = self.rows(out)[("r", "alt", "bytes_total")]
        self.assertEqual((row["n_valid"], row["n_invalid"], row["n"], row["scale"]),
                         ("2", "1", "2", "log"))
        expected = (math.log(2) + math.log(22 / 12)) / 2
        self.assertAlmostEqual(float(row["ratio"]), math.exp(expected))
        self.assertEqual(row["diff_mean"], "")
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
                             "unrepaired_at_expiry_hits": 0.0,
                             "before": phase, "during": phase, "after": phase}}
        proc, out = self.judge([result("main", 1, 10, regime="az", **cut),
                                result("main", 1, 10, regime="plain", partition=None)])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        summary = (out / "summary.md").read_text()
        self.assertEqual(summary.count("partition phases"), 1)
        self.assertIn("seeds that healed only | 6,200; healed 1/1 |", summary)
        self.assertIn("| Hits per (pod, key) that expired unrepaired (hits) | 0 |", summary)
        self.assertIn("| during: fraction never converged, cross side | 0.6 |", summary)
        rows = self.rows(out)
        self.assertEqual(rows[("az", "main", "part_time_to_heal_ms")]["mean"], "6200.0")
        self.assertEqual(rows[("az", "main", "part_during_stale_in_zone_limited_p99")]["mean"],
                         "40.0")
        self.assertNotIn(("plain", "main", "part_time_to_heal_ms"), rows)



class StatsTest(unittest.TestCase):
    def test_t_cdf_matches_closed_forms(self):
        for t in (-3.0, -0.5, 0.0, 0.7, 2.5, 40.0):
            self.assertAlmostEqual(judge_module.t_cdf(t, 1), 0.5 + math.atan(t) / math.pi, 10)
            self.assertAlmostEqual(judge_module.t_cdf(t, 2),
                                   0.5 + t / (2 * math.sqrt(t * t + 2)), 10)

    def test_t_quantile_matches_tables(self):
        for df, expected in ((2, 4.302653), (10, 2.228139), (30, 2.042272)):
            self.assertAlmostEqual(judge_module.t_quantile(0.975, df), expected, delta=1e-5)
        self.assertAlmostEqual(judge_module.t_quantile(0.025, 10), -2.228139, delta=1e-5)
        self.assertAlmostEqual(judge_module.t_quantile(0.5, 5), 0.0, delta=1e-9)
        # Any level works: t(0.9, 10) = 1.372184.
        self.assertAlmostEqual(judge_module.t_quantile(0.9, 10), 1.372184, delta=1e-5)

    def test_posterior_interval_and_edge_cases(self):
        post = judge_module.posterior([1.0, 2.0, 3.0], 0.95)
        half = 4.302653 * 1.0 / math.sqrt(3)
        self.assertAlmostEqual(post["lo"], 2.0 - half, delta=1e-5)
        self.assertAlmostEqual(post["hi"], 2.0 + half, delta=1e-5)
        single = judge_module.posterior([5.0], 0.95)
        self.assertEqual((single["mean"], single["lo"], single["hi"]), (5.0, None, None))
        point = judge_module.posterior([4.0, 4.0, 4.0], 0.95)
        self.assertEqual((point["lo"], point["hi"]), (4.0, 4.0))
        self.assertEqual(judge_module.p_below(point, 5.0), 1.0)
        self.assertEqual(judge_module.p_below(point, 4.0), 0.5)
        self.assertEqual(judge_module.p_below(point, 3.0), 0.0)


class ComparisonTest(unittest.TestCase):
    judge = JudgeTest.judge
    rows = JudgeTest.rows

    def pair(self, a_values, b_values, *args):
        runs = [result("main", i, b) for i, b in enumerate(b_values)]
        runs += [result("alt", i, a) for i, a in enumerate(a_values)]
        proc, out = self.judge(runs, *args)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return self.rows(out)[("r", "alt", "bytes_total")], out

    def test_log_ratio_posterior_by_hand(self):
        # d = ln 2, ln 4, ln 8 -> mean 2 ln 2, s = ln 2, se = ln 2 / sqrt(3).
        row, out = self.pair([2, 4, 8], [1, 1, 1])
        self.assertEqual(row["scale"], "log")
        ln2 = math.log(2)
        half = 4.302653 * ln2 / math.sqrt(3)
        self.assertAlmostEqual(float(row["ratio"]), 4.0)
        self.assertAlmostEqual(float(row["ratio_lo"]), math.exp(2 * ln2 - half), places=4)
        self.assertAlmostEqual(float(row["ratio_hi"]), math.exp(2 * ln2 + half), places=4)
        # P(mean log ratio < 0): t = -2 ln2 / se = -3.4641 with 2 df.
        t = -2 * ln2 / (ln2 / math.sqrt(3))
        self.assertAlmostEqual(float(row["p_better"]),
                               0.5 + t / (2 * math.sqrt(t * t + 2)), places=6)
        self.assertIn("(×4.00 [", (out / "summary.md").read_text())

    def test_p_better_follows_direction(self):
        lower, _ = self.pair([8, 9, 10], [10, 11, 12])
        higher, _ = self.pair([10, 11, 12], [8, 9, 10])
        self.assertGreater(float(lower["p_better"]), 0.9)
        self.assertLess(float(higher["p_better"]), 0.1)
        # `admitted` is higher-is-better: more admitted than the baseline is better.
        runs = [result("main", i, 1, traffic={"measure": {"admitted": a}})
                for i, a in enumerate((8, 9, 10))]
        runs += [result("alt", i, 1, traffic={"measure": {"admitted": a}})
                 for i, a in enumerate((10, 11, 12))]
        proc, out = self.judge(runs)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertGreater(float(self.rows(out)[("r", "alt", "admitted")]["p_better"]), 0.9)

    def test_non_positive_values_fall_back_to_the_difference(self):
        row, out = self.pair([0, 3, 6], [1, 1, 1])
        self.assertEqual(row["scale"], "diff")
        self.assertEqual(row["ratio"], "")
        # d = -1, 2, 5: mean 2, s = 3, se = sqrt(3).
        self.assertAlmostEqual(float(row["diff_mean"]), 2.0)
        half = 4.302653 * math.sqrt(3)
        self.assertAlmostEqual(float(row["diff_lo"]), 2 - half, places=4)
        self.assertAlmostEqual(float(row["diff_hi"]), 2 + half, places=4)
        self.assertIn("(Δ2 [", (out / "summary.md").read_text())

    def test_single_pair_has_no_interval_or_probability(self):
        row, _ = self.pair([20], [10])
        self.assertAlmostEqual(float(row["ratio"]), 2.0)
        self.assertEqual((row["ratio_lo"], row["ratio_hi"], row["p_better"], row["cri_lo"]),
                         ("", "", "", ""))

    def test_zero_spread_gives_a_point_mass(self):
        row, out = self.pair([20, 20, 20], [10, 10, 10])
        self.assertEqual((row["cri_lo"], row["cri_hi"]), ("20.0", "20.0"))
        self.assertAlmostEqual(float(row["ratio_lo"]), 2.0)
        self.assertEqual(float(row["p_better"]), 0.0)
        same, _ = self.pair([10, 10, 10], [10, 10, 10])
        self.assertEqual(float(same["p_better"]), 0.5)
        better, _ = self.pair([5, 5, 5], [10, 10, 10])
        self.assertEqual(float(better["p_better"]), 1.0)

    def test_level_widens_the_interval(self):
        narrow, _ = self.pair([1, 2, 3], [1, 1, 1], "--level", "0.5")
        wide, _ = self.pair([1, 2, 3], [1, 1, 1], "--level", "0.99")
        self.assertLess(float(wide["cri_lo"]), float(narrow["cri_lo"]))
        self.assertGreater(float(wide["cri_hi"]), float(narrow["cri_hi"]))

    def test_time_to_heal_shows_how_many_seeds_healed(self):
        def cut(heal):
            return {"partition": {"time_to_heal_ms": heal}}
        proc, out = self.judge([result("main", 1, 10, **cut(5_000.0)),
                                result("main", 2, 10, **cut(7_000.0)),
                                result("main", 3, 10, **cut(None))])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("6,000 [", (out / "summary.md").read_text())
        self.assertIn("; healed 2/3 |", (out / "summary.md").read_text())


class HarnessHashTest(unittest.TestCase):
    def setUp(self):
        self.repo = Path(tempfile.mkdtemp())
        self.git("init", "-q")

    def git(self, *args: str) -> str:
        return subprocess.run(["git", "-C", str(self.repo), "-c", "user.name=t",
                               "-c", "user.email=t@t", *args], check=True,
                              capture_output=True, text=True).stdout.strip()

    def tree(self, files: dict[str, str]) -> str:
        for path, text in files.items():
            (self.repo / path).parent.mkdir(parents=True, exist_ok=True)
            (self.repo / path).write_text(text)
        self.git("add", "-A")
        return self.git("write-tree")

    def test_judge_and_readme_changes_are_equivalent(self):
        a = self.tree({"scenario.json": "1", "judge/judge.py": "a", "README.md": "a"})
        b = self.tree({"judge/judge.py": "b", "README.md": "b", "judge/new.py": "x"})
        self.assertIsNone(judge_module.hash_equivalence([a, b], self.repo))

    def test_other_changes_are_refused(self):
        a = self.tree({"scenario.json": "1", "judge/judge.py": "a"})
        b = self.tree({"scenario.json": "2", "judge/judge.py": "b"})
        why = judge_module.hash_equivalence([a, b], self.repo)
        self.assertIn("scenario.json", why)

    def test_every_pair_must_be_equivalent(self):
        a = self.tree({"x.rs": "1", "judge/j.py": "a"})
        b = self.tree({"judge/j.py": "b"})
        c = self.tree({"x.rs": "2", "judge/j.py": "b"})
        self.assertIsNone(judge_module.hash_equivalence([a, b], self.repo))
        self.assertIn("x.rs", judge_module.hash_equivalence([a, b, c], self.repo))

    def test_unresolvable_tree_is_refused(self):
        a = self.tree({"x": "1"})
        why = judge_module.hash_equivalence([a, "f" * 40], self.repo)
        self.assertIn("cannot compare", why)


if __name__ == "__main__":
    unittest.main()
