#!/usr/bin/env python3
"""Tests for gate.py's pass rules and verdict.py.

Run: python3 -m unittest discover -s crates/gossip-bench/judge
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from gate import MAIN, NOPK, ZONE, failures, verdicts
from verdict import verdict

REGIMES = ["az-partition", "prod-4x", "prod-peak", "prod-typical"]


def log(p: float, ratio: float, lo: float | None = None, hi: float | None = None) -> dict:
    lo = ratio - 0.01 if lo is None else lo
    hi = ratio + 0.01 if hi is None else hi
    return {"scale": "log", "p_better": p, "ratio": ratio, "ratio_lo": lo, "ratio_hi": hi,
            "mean": 1.0, "n_valid": 3}


def passing() -> dict:
    """Judge rows (as `gate.read` returns them, every arm in one table) that
    break no rule."""
    rows = {}
    for r in REGIMES:
        for arm in (MAIN, NOPK, ZONE):
            rows[(r, arm, "bytes_total")] = log(0.99, 0.5)
            rows[(r, arm, "cross_az_bytes")] = log(0.99, 0.5)
    rows[("az-partition", ZONE, "part_time_to_heal_ms")] = log(0.5, 1.0, 0.9, 1.1)
    return rows


def rules(main: dict | None = None, nopk: dict | None = None) -> list[str]:
    """`failures` over the judge's tables with `main` and with nopk as the
    comparator (each defaults to `passing()`)."""
    main, nopk = main or passing(), nopk or passing()
    return failures(REGIMES, main, verdicts(nopk, ZONE, NOPK), verdicts(main, ZONE, MAIN),
                    verdicts(main, NOPK, MAIN))


class RuleTest(unittest.TestCase):
    def assertOnly(self, fails: list[str], rule: str) -> None:
        self.assertTrue(fails, f"{rule} did not fail")
        self.assertTrue(all(f.startswith(rule) for f in fails), fails)

    def test_the_fixture_passes(self):
        self.assertEqual(rules(passing()), [])

    def test_g_v_every_arm_has_three_valid_seeds(self):
        rows = passing()
        rows[("prod-peak", ZONE, "bytes_total")]["n_valid"] = 2
        self.assertOnly(rules(rows), "G-v")

    def test_g_a1_no_error_comparison_is_worse_than_nopk(self):
        nopk = passing()
        nopk[("prod-peak", ZONE, "over_max_pct")] = log(0.01, 1.2)
        self.assertOnly(rules(nopk=nopk), "G-a1")

    def test_g_a2_az_partition_heals_on_every_seed(self):
        rows = passing()
        rows[("az-partition", ZONE, "part_time_to_heal_ms")]["n_valid"] = 2
        self.assertOnly(rules(rows), "G-a2")

    def test_g_a3_cost_is_credibly_lower_than_nopk_in_production(self):
        nopk = passing()
        nopk[("prod-4x", ZONE, "cross_az_bytes")] = log(0.94, 0.9, 0.8, 1.01)
        self.assertOnly(rules(nopk=nopk), "G-a3")
        nopk[("prod-4x", ZONE, "cross_az_bytes")] = log(0.99, 1.02)
        self.assertOnly(rules(nopk=nopk), "G-a3")

    def test_g_a4_receive_buffer_drops_are_not_worse_than_nopk(self):
        nopk = passing()
        nopk[("az-partition", ZONE, "drop_rcvbuf_pkts")] = log(0.01, 1.5)
        self.assertOnly(rules(nopk=nopk), "G-a4")

    def test_g_b1_cost_is_better_than_main_everywhere(self):
        rows = passing()
        rows[("az-partition", ZONE, "bytes_total")] = log(0.6, 1.0, 0.96, 1.04)
        self.assertOnly(rules(rows), "G-b1")

    def test_g_b2_what_nopk_won_against_main_stays_won(self):
        rows = passing()
        rows[("prod-peak", NOPK, "stale_limited_p99_pct")] = log(0.99, 0.5)
        rows[("prod-peak", ZONE, "stale_limited_p99_pct")] = log(0.97, 0.97, 0.9, 0.99)
        self.assertOnly(rules(rows), "G-b2")

    def test_g_b3_no_error_comparison_is_worse_than_main(self):
        rows = passing()
        rows[("prod-typical", ZONE, "conv_never_frac")] = log(0.02, 1.3)
        self.assertOnly(rules(rows), "G-b3")


class MissingArmTest(unittest.TestCase):
    def test_a_missing_arm_is_named_without_a_traceback(self):
        with tempfile.TemporaryDirectory() as tmp:
            proc = subprocess.run([sys.executable, str(Path(__file__).with_name("gate.py")), tmp,
                                   "--out", tmp], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 1)
        self.assertIn("make sim-baselines", proc.stderr)
        self.assertNotIn("Traceback", proc.stderr)


class VerdictTest(unittest.TestCase):
    def test_classes_at_the_band_edges(self):
        cases = [
            (log(0.95, 0.94), None, "better"),
            (log(0.95, 0.95, 0.9, 0.99), None, "credible <5 %"),
            (log(0.05, 1.06), None, "worse"),
            (log(0.06, 1.06, 1.0, 1.1), None, "unsettled"),
            (log(0.5, 1.0, 0.95, 1.05), None, "tie"),
            (log(0.99, 0.5, None, None) | {"ratio_lo": None}, None, "unsettled"),
            ({"scale": "diff", "p_better": None, "diff_mean": 0.0, "diff_lo": None,
              "diff_hi": None}, 0.0, "tie"),
            ({"scale": "diff", "p_better": 0.96, "diff_mean": -5.1, "diff_lo": -6.0,
              "diff_hi": -4.0}, 100.0, "better"),
            ({"scale": "diff", "p_better": 0.96, "diff_mean": -4.9, "diff_lo": -6.0,
              "diff_hi": -4.0}, 100.0, "credible <5 %"),
            ({"scale": None}, None, None),
        ]
        for row, comparator, expected in cases:
            self.assertEqual(verdict(row, comparator), expected, row)


if __name__ == "__main__":
    unittest.main()
