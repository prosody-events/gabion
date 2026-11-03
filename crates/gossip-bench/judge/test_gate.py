#!/usr/bin/env python3
"""Tests for gate.py's pass rules, its cost-model report and verdict.py.

Run: python3 -m unittest discover -s crates/gossip-bench/judge
"""

from __future__ import annotations

import contextlib
import csv
import io
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import gate
from gate import MAIN, NOPK, ZONE, failures
from verdict import verdict

REGIMES = ["az-partition", "prod-4x", "prod-peak", "prod-typical"]
REGIME_DIR = Path(__file__).resolve().parent.parent / "regimes"


def regime(name: str) -> dict:
    return json.loads((REGIME_DIR / f"{name}.json").read_text())


def log(p: float, ratio: float, lo: float | None = None, hi: float | None = None) -> dict:
    lo = ratio - 0.01 if lo is None else lo
    hi = ratio + 0.01 if hi is None else hi
    return {"scale": "log", "p_better": p, "ratio": ratio, "ratio_lo": lo, "ratio_hi": hi,
            "mean": 1.0, "n_valid": 3}


def passing(regimes=REGIMES) -> dict:
    """Judge rows (as `gate.read` returns them, every arm in one table) that
    break no rule in `regimes`."""
    rows = {}
    for r in regimes:
        for arm in (MAIN, NOPK, ZONE):
            rows[(r, arm, "bytes_total")] = log(0.99, 0.5)
            rows[(r, arm, "cross_az_bytes")] = log(0.99, 0.5)
    rows[("az-partition", ZONE, "part_time_to_heal_ms")] = log(0.5, 1.0, 0.9, 1.1)
    return rows


class RuleTest(unittest.TestCase):
    def assertOnly(self, fails: list[str], rule: str) -> None:
        self.assertTrue(fails, f"{rule} did not fail")
        self.assertTrue(all(f.startswith(rule) for f in fails), fails)

    def test_the_fixture_passes(self):
        self.assertEqual(failures(REGIMES, passing(), passing()), [])

    def test_g_v_every_arm_has_three_valid_seeds(self):
        rows = passing()
        rows[("prod-peak", ZONE, "bytes_total")]["n_valid"] = 2
        self.assertOnly(failures(REGIMES, rows, passing()), "G-v")

    def test_g_a1_no_error_comparison_is_worse_than_nopk(self):
        nopk = passing()
        nopk[("prod-peak", ZONE, "over_max_pct")] = log(0.01, 1.2)
        self.assertOnly(failures(REGIMES, passing(), nopk), "G-a1")

    def test_g_a2_az_partition_heals_on_every_seed(self):
        rows = passing()
        rows[("az-partition", ZONE, "part_time_to_heal_ms")]["n_valid"] = 2
        self.assertOnly(failures(REGIMES, rows, passing()), "G-a2")

    def test_g_a3_cost_is_credibly_lower_than_nopk_in_production(self):
        nopk = passing()
        nopk[("prod-4x", ZONE, "cross_az_bytes")] = log(0.94, 0.9, 0.8, 1.01)
        self.assertOnly(failures(REGIMES, passing(), nopk), "G-a3")
        nopk[("prod-4x", ZONE, "cross_az_bytes")] = log(0.99, 1.02)
        self.assertOnly(failures(REGIMES, passing(), nopk), "G-a3")
        nopk = passing()
        nopk[("az-partition", ZONE, "bytes_total")] = log(0.01, 1.2)
        self.assertOnly(failures(REGIMES, passing(), nopk), "G-a3")

    def test_g_a4_receive_buffer_drops_are_not_worse_than_nopk(self):
        nopk = passing()
        nopk[("az-partition", ZONE, "drop_rcvbuf_pkts")] = log(0.01, 1.5)
        self.assertOnly(failures(REGIMES, passing(), nopk), "G-a4")

    def test_g_b1_cost_is_better_than_main_everywhere(self):
        rows = passing()
        rows[("az-partition", ZONE, "bytes_total")] = log(0.6, 1.0, 0.96, 1.04)
        self.assertOnly(failures(REGIMES, rows, passing()), "G-b1")

    def test_g_b2_what_nopk_won_against_main_stays_won(self):
        rows = passing()
        rows[("prod-peak", NOPK, "stale_limited_p99_pct")] = log(0.99, 0.5)
        rows[("prod-peak", ZONE, "stale_limited_p99_pct")] = log(0.97, 0.97, 0.9, 0.99)
        self.assertOnly(failures(REGIMES, rows, passing()), "G-b2")

    def test_g_b3_no_error_comparison_is_worse_than_main(self):
        rows = passing()
        rows[("prod-typical", ZONE, "conv_never_frac")] = log(0.02, 1.3)
        self.assertOnly(failures(REGIMES, rows, passing()), "G-b3")


class ExitTest(unittest.TestCase):
    def gate(self, *arms: str) -> str:
        """gate.py's stderr over empty directories for `arms`; it must exit 1 cleanly."""
        with tempfile.TemporaryDirectory() as tmp:
            for arm in arms:
                Path(tmp, arm).mkdir()
            proc = subprocess.run([sys.executable, str(Path(__file__).with_name("gate.py")), tmp,
                                   "--out", tmp], capture_output=True, text=True)
        self.assertEqual(proc.returncode, 1)
        self.assertNotIn("Traceback", proc.stderr)
        return proc.stderr

    def test_a_missing_arm_is_named(self):
        self.assertIn(f"{NOPK} is missing", self.gate(MAIN, ZONE))

    def test_a_judge_refusal_is_reported(self):
        self.assertIn("judge.py refused", self.gate(MAIN, NOPK, ZONE))

    def test_a_model_miss_never_changes_the_exit_code(self):
        # Red if a model miss feeds the exit code or the printed miss count is
        # wrong. Runs the real `main()` with
        # only judge.py, git and the core hash faked, every rule passing and
        # one miss far outside any band.
        rows = passing(sorted(p.stem for p in REGIME_DIR.glob("*.json")))
        rows[("prod-peak", ZONE, "admitted")] = {"mean": 1.0}
        rows[("prod-peak", ZONE, "peak_cells")] = {"mean": 1e6}  # predicted 0.18
        with tempfile.TemporaryDirectory() as tmp:
            results, out = Path(tmp, "results"), Path(tmp, "out")
            for arm in (MAIN, NOPK, ZONE):
                Path(results, arm, "r").mkdir(parents=True)
                Path(results, arm, "r", "seed-1.json").write_text(json.dumps(
                    {"stamp": {"harness_tree_hash": "0" * 40, "solution_commit": "0" * 40}}))

            def run(argv, **_):
                if argv[1].endswith("judge.py"):
                    report = Path(argv[argv.index("--out") + 1])
                    report.mkdir(parents=True, exist_ok=True)
                    with (report / "summary.csv").open("w", newline="") as f:
                        writer = csv.DictWriter(f, ["regime", "solution", "metric", "n_valid",
                                                    "scale", *gate.NUMBERS])
                        writer.writeheader()
                        writer.writerows({"regime": r, "solution": s, "metric": m,
                                          "n_valid": 3, **row}
                                         for (r, s, m), row in rows.items())
                return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

            stdout = io.StringIO()
            with (mock.patch.object(gate.subprocess, "run", run),
                  mock.patch.object(gate, "core_hash", lambda *_: "0" * 40),
                  mock.patch.object(sys, "argv", ["gate.py", str(results), "--out", str(out)]),
                  contextlib.redirect_stdout(stdout),
                  self.assertRaises(SystemExit) as exit_):
                gate.main()
            text = (out / "gate.md").read_text()
        self.assertEqual(exit_.exception.code, 0)
        self.assertIn("Cost model (not gating): 1 comparisons miss", stdout.getvalue())
        self.assertIn("Every rule passes.", text)
        self.assertIn("`peak_cells`: §6's defaults rest on a model that misses here", text)


class ModelTest(unittest.TestCase):
    ADMITTED = 109_474.0  # prod-peak's zone arm, about Λ = 1,825 over 60 s

    def check(self, scale: float, metric: str) -> str:
        """The model section for prod-peak with the zone arm measuring `scale`
        times the prediction of `metric` and exactly the prediction elsewhere."""
        rows = {("prod-peak", ZONE, "admitted"): {"mean": self.ADMITTED}}
        for m, p in gate.predict(regime("prod-peak"), self.ADMITTED).items():
            rows[("prod-peak", ZONE, m)] = {"mean": p * (scale if m == metric else 1.0)}
        return "\n".join(gate.model_check({"prod-peak": regime("prod-peak")}, rows)[0])

    def test_a_miss_is_flagged_past_five_percent_either_way(self):
        # Red if the band is not 5 %, is one-sided, measures the miss against
        # the measured value instead of the prediction, or names the wrong
        # setting.
        for scale, flagged in ((1.051, True), (0.949, True), (1.049, False), (0.951, False)):
            self.assertEqual("misses here by" in self.check(scale, "peak_cells"), flagged, scale)
        for metric, equation, sizes in gate.MODEL:
            self.assertIn(f"- `{metric}`: §6's defaults rest on a model that misses here by "
                          f"5.1%; the owner decides. {equation} sizes {sizes}.",
                          self.check(1.051, metric))
        self.assertEqual(dict((m, s) for m, _, s in gate.MODEL),
                         {**dict.fromkeys(("packets_per_node_s", "bytes_per_node_s",
                                           "cross_az_bytes_per_node_s"), gate.STREAM_SIZES),
                          "conv_p99_incl_never_ms": "`gossip.tick_interval`",
                          "peak_cells": "`STORAGE_MAX_CELLS` (`storage.max_cells`)"})
        self.assertIn("quantum", gate.STREAM_SIZES)
        self.assertIn("`gossip.tick_interval`", gate.STREAM_SIZES)

    def test_a_missing_metric_reads_missing_not_a_pass(self):
        # Red if a missing mean is read as 0 or as within the band, if
        # predictions are made without an admitted count or outside ε = 1, or
        # if `gateway_failovers` is left out, or shown without §6.5.8's odds or
        # `gossip.suspect_after`, instead of shown missing.
        peak = {"prod-peak": regime("prod-peak")}
        rows = {("prod-peak", ZONE, "admitted"): {"mean": self.ADMITTED},
                **{("prod-peak", ZONE, m): {"mean": p}
                   for m, p in gate.predict(peak["prod-peak"], self.ADMITTED).items()}}
        rows[("prod-peak", ZONE, "peak_cells")] = {"mean": None}
        lines, misses = gate.model_check(peak, rows)
        self.assertIn("| `peak_cells` | (6.10) | 20,070 | missing | missing |", lines)
        self.assertEqual(misses, 0)
        lines, misses = gate.model_check(peak, {})
        table = [line for line in lines if line.startswith("| `")]
        self.assertEqual(len(table), len(gate.MODEL))
        self.assertTrue(all(line.endswith("| missing | missing | missing |") for line in table),
                        table)
        self.assertEqual(misses, 0)
        self.assertIn(f"- `gateway_failovers`: missing. §6.5.8 predicts a false suspicion in "
                      f"{gate.false_suspicion(peak['prod-peak'])}; `gossip.suspect_after` "
                      "sizes it.", lines)
        wide = peak["prod-peak"] | {"rule": peak["prod-peak"]["rule"] | {"limit": 10**7}}
        self.assertEqual(gate.predict(wide, self.ADMITTED), {})  # ε = 549

    def test_the_model_gives_section_6_5s_numbers(self):
        # Red if (6.2), (6.3), (6.5), (6.10), the packet size or §6.5.8's
        # false-suspicion odds are mistyped: the expected values are §6.3's and
        # §6.5's own figures (R_own 5.49 and 12.54, p99 300 and 148 ms,
        # A = Λ · 11 s, the §6.5.8 tables), plus the cross-zone acks,
        # 4 · (1 − e^(−λT)).
        peak = gate.predict(regime("prod-peak"), 1_825 * 60)  # Λ = 1,825 (§6.3)
        x4 = gate.predict(regime("prod-4x"), 7_222 * 60)      # Λ = 7,222
        self.assertAlmostEqual(peak["packets_per_node_s"], 5.49 * 182 + 3.97, delta=2)
        self.assertAlmostEqual(x4["packets_per_node_s"], 12.54 * 299 + 4.0, delta=4)
        # 998 data packets of 1.82 cells at 148 + 38 c bytes, plus 4 acks of 114 B
        self.assertAlmostEqual(peak["bytes_per_node_s"],
                               998 * (148 + 38 * 1.82) + 3.97 * 114, delta=300)
        self.assertAlmostEqual(peak["cross_az_bytes_per_node_s"],
                               5.49 * 2 * (148 + 38 * 1.82) + 3.97 * 114, delta=10)
        self.assertAlmostEqual(peak["conv_p99_incl_never_ms"], 300, delta=1)
        self.assertAlmostEqual(x4["conv_p99_incl_never_ms"], 148, delta=1)
        self.assertAlmostEqual(peak["peak_cells"], 1_825 * 11, delta=1)
        self.assertAlmostEqual(x4["peak_cells"], 7_222 * 11, delta=1)
        for name, odds in (("loss-5", "9e-05 (n_a = 4) to 0.00093 (n_a = 3) per late answer"),
                           ("loss-20", "0.017 (n_a = 4) to 0.047 (n_a = 3) per late answer"),
                           ("loss-burst", "0.53 (n_a = 4) to 0.66 (n_a = 3) per loss burst"),
                           ("prod-peak", "0 (n_a = 4) to 0 (n_a = 3) per late answer")):
            self.assertEqual(gate.false_suspicion(regime(name)), odds)

    def test_the_model_constants_match_their_sources(self):
        # Red if a default or a wire size changes without gate.py following it
        # (the prediction would silently use the old value).
        root = Path(__file__).resolve().parents[3]

        def const(path: str, name: str) -> str:
            return re.search(rf"\b{name}: \w+ = ([^;]+);", (root / path).read_text()).group(1)

        defaults = "crates/gabion/src/defaults.rs"
        self.assertEqual(int(const(defaults, "GOSSIP_TICK_INTERVAL_MILLIS")), gate.TICK_MS)
        self.assertEqual(int(const(defaults, "GOSSIP_TARGET_ERR_BPS")), gate.ERR_BPS)
        self.assertEqual(int(const(defaults, "GOSSIP_SUSPECT_AFTER_TICKS")), gate.SUSPECT_TICKS)
        self.assertEqual(int(const(defaults, "GOSSIP_SUSPECT_AFTER_FLOOR_MS")),
                         gate.SUSPECT_FLOOR_MS)
        self.assertEqual(int(const("crates/gabion/src/wire/header.rs", "HEADER_LEN")),
                         gate.HEADER_BYTES)
        body = "crates/gabion/src/wire/body.rs"
        self.assertEqual(const(body, "CELL_BYTES"), "PER_CELL_IDENT_BYTES + 4")
        self.assertEqual(sum(int(t) for t in const(body, "PER_CELL_IDENT_BYTES").split("+")) + 4,
                         gate.CELL_BYTES)
        self.assertEqual(int(const(body, "DICT_LEN_BYTES")), gate.DICT_LEN_BYTES)
        harness = "crates/gossip-bench/src/harness"
        self.assertEqual(int(const(f"{harness}/net.rs", "IP_UDP_HEADER_BYTES")),
                         gate.IP_UDP_BYTES)
        self.assertEqual(int(const(f"{harness}/plan.rs", "AVAILABILITY_ZONES")), gate.ZONES)


class VerdictTest(unittest.TestCase):
    def test_classes_at_the_band_edges(self):
        cases = [
            (log(0.95, 0.94), None, "better"),
            (log(0.95, 0.95, 0.9, 0.99), None, "credible <5 %"),
            (log(0.05, 1.06), None, "worse"),
            (log(0.06, 1.06, 1.0, 1.1), None, "unsettled"),
            (log(0.5, 1.0, 0.95, 1.05), None, "tie"),
            (log(0.99, 0.5) | {"ratio_lo": None}, None, "unsettled"),
            ({"scale": "diff", "p_better": None, "diff_mean": 0.0, "diff_lo": None,
              "diff_hi": None}, 0.0, "tie"),
            ({"scale": "diff", "p_better": 0.96, "diff_mean": -5.1, "diff_lo": -6.0,
              "diff_hi": -4.0}, 100.0, "better"),
            ({"scale": "diff", "p_better": 0.96, "diff_mean": -4.9, "diff_lo": -6.0,
              "diff_hi": -4.0}, 100.0, "credible <5 %"),
            ({"scale": "diff", "p_better": 0.04, "diff_mean": 5.1, "diff_lo": 4.0,
              "diff_hi": 6.0}, 100.0, "worse"),
            ({"scale": "diff", "p_better": 0.5, "diff_mean": 0.0, "diff_lo": -4.9,
              "diff_hi": 4.9}, 100.0, "tie"),
            ({"scale": None}, None, None),
        ]
        for row, comparator, expected in cases:
            self.assertEqual(verdict(row, comparator), expected, row)


if __name__ == "__main__":
    unittest.main()
