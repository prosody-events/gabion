#!/usr/bin/env python3
"""Drive the `gossip-bench` Rust binary across a matrix of scenarios and
produce SVG plots from the JSON results.

Why Python: the bench binary is the source of truth — it emits stable
JSON. Python is for matrix orchestration + plotting only.

Usage:
    python3 bench/plot.py all                 # every suite
    python3 bench/plot.py convergence         # one suite
    python3 bench/plot.py all --publish       # also copy SVGs into
                                              # crates/gabion/figures/ so
                                              # the README renders on GitHub
    python3 bench/plot.py --help

Each suite writes:
    target/gossip-bench/<suite>/results.jsonl   — raw bench output
    target/gossip-bench/figures/<suite>.svg     — final plot

`--publish` additionally copies every SVG into `crates/gabion/figures/`
which IS checked into the repo (`target/` is gitignored). Run it after a
clean bench when you want the README's embedded figures to reflect
current code.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

# ----- bench binary plumbing ------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]
TARGET_ROOT = REPO_ROOT / "target" / "gossip-bench"
FIGURES_DIR = TARGET_ROOT / "figures"
PUBLISH_DIR = REPO_ROOT / "crates" / "gabion" / "figures"
RUSTUP_CARGO = os.environ.get(
    "GOSSIP_BENCH_CARGO",
    str(Path.home() / ".rustup" / "toolchains" / "stable-aarch64-apple-darwin" / "bin" / "cargo"),
)


def cargo_bin() -> str:
    """Resolve a cargo executable that works in any environment."""
    if Path(RUSTUP_CARGO).exists():
        return RUSTUP_CARGO
    for entry in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(entry) / "cargo"
        if candidate.exists():
            return str(candidate)
    raise SystemExit("could not find a cargo binary; set GOSSIP_BENCH_CARGO")


def build_bench() -> Path:
    """Build the release binary once and return its path."""
    subprocess.run(
        [cargo_bin(), "build", "--release", "-p", "gossip-bench"],
        cwd=REPO_ROOT,
        check=True,
    )
    return REPO_ROOT / "target" / "release" / "gossip-bench"


def run_bench(binary: Path, scenarios: Iterable[dict]) -> list[dict]:
    """Pipe a list of scenarios into `gossip-bench batch` and parse the
    resulting JSONL stream into a list of dicts."""
    payload = "\n".join(json.dumps(s) for s in scenarios) + "\n"
    proc = subprocess.run(
        [str(binary), "batch"],
        input=payload,
        text=True,
        capture_output=True,
        check=True,
    )
    return [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]


# ----- scenario builders ----------------------------------------------------


def _base(name: str, **overrides) -> dict:
    spec = {
        "name": name,
        "nodes": 8,
        "fanout": 3,
        "tick_interval": "100ms",
        "duration": "5s",
        "sample_interval": "100ms",
        "network": {"uniform_loss": 0.0, "links": [], "schedule": []},
        "workload": {
            "shape": "single_write",
            "node": 0,
            "hits": 10,
            "at": "100ms",
        },
        "kind": "convergence",
        "seed": 0xDEADBEEF,
        "cell_capacity": 256,
        "max_cells_per_tick": 256,
    }
    spec.update(overrides)
    return spec


def suite_convergence() -> list[dict]:
    """Demers/Karp: single-write convergence vs cluster size and fanout."""
    out = []
    for n in [4, 8, 16, 32, 64, 128, 256]:
        duration_s = max(8, 2 * (n.bit_length() - 1))
        for f in [1, 2, 3, 5, 8]:
            if f >= n:
                continue
            out.append(
                _base(
                    f"converge_n{n}_f{f}",
                    nodes=n,
                    fanout=f,
                    duration=f"{duration_s}s",
                    kind="convergence",
                )
            )
    return out


def suite_fanout_sweep() -> list[dict]:
    """Convergence time as the static fanout floor sweeps at fixed N=32."""
    return [
        _base(
            f"fanout_sweep_f{f}",
            nodes=32,
            fanout=f,
            duration="10s",
            kind="convergence",
        )
        for f in [1, 2, 3, 4, 5, 6, 8, 12]
    ]


def suite_loss() -> list[dict]:
    """Bimodal Multicast / SWIM: convergence under i.i.d. per-link drop."""
    out = []
    for loss in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]:
        for trial in range(3):
            out.append(
                _base(
                    f"loss_{int(loss * 100):02d}_t{trial}",
                    nodes=16,
                    fanout=3,
                    duration="20s",
                    kind="loss_tolerance",
                    network={
                        "uniform_loss": loss,
                        "links": [],
                        "schedule": [],
                    },
                    seed=0xDEADBEEF + trial,
                )
            )
    return out


def suite_partition() -> list[dict]:
    """SWIM-style partition + heal — re-convergence time after the heal."""
    nodes = 8
    half = nodes // 2
    blocks: list[dict] = []
    heals: list[dict] = []
    for a in range(half):
        for b in range(half, nodes):
            blocks.append({"from": a, "to": b, "action": "block"})
            blocks.append({"from": b, "to": a, "action": "block"})
            heals.append({"from": a, "to": b, "action": "pass"})
            heals.append({"from": b, "to": a, "action": "pass"})
    return [
        _base(
            "partition_heal_8",
            nodes=nodes,
            fanout=3,
            duration="20s",
            kind="partition",
            workload={
                "shape": "single_write",
                "node": 0,
                "hits": 7,
                "at": "200ms",
            },
            network={
                "uniform_loss": 0.0,
                "links": blocks,
                "schedule": [{"at": "10s", "apply": heals}],
            },
        )
    ]


def suite_staleness() -> list[dict]:
    """Astrolabe-style: per-hit lag under sustained writes."""
    return [
        _base(
            f"sustained_src{src}",
            nodes=16,
            fanout=3,
            duration="10s",
            kind="staleness",
            workload={
                "shape": "sustained",
                "sources": list(range(src)),
                "per_tick": 1,
            },
        )
        for src in [1, 2, 4, 8]
    ]


def suite_scale_n() -> list[dict]:
    """Convergence as cluster size scales — the classic log-N curve."""
    out = []
    for n in [4, 8, 16, 32, 64, 128, 256, 512, 1024]:
        log2_n = n.bit_length() - 1
        duration_s = max(10, log2_n * 5)
        out.append(
            _base(
                f"scale_n{n}",
                nodes=n,
                fanout=3,
                duration=f"{duration_s}s",
                kind="scale_n",
            )
        )
    return out


# gabion::defaults::GOSSIP_COVERAGE_MARGIN — keep in sync with the Rust const.
COVERAGE_MARGIN = 4.0


def _coverage_pick(n_nodes: int, floor: int = 1) -> int:
    """Predicted per-tick fanout `⌈ln(peers) + c⌉`, clamped to
    `[floor, peers]`. `peers = n_nodes - 1` because the runtime sizes the
    pick off `self.peers.len()`."""
    peers = max(n_nodes - 1, 1)
    coverage = math.ceil(math.log(peers) + COVERAGE_MARGIN)
    return min(max(floor, coverage), peers)


def suite_coverage_fanout() -> list[dict]:
    """Kermarrec, Massoulié & Ganesh (TPDS 2003, Thm 1): the per-round
    fanout that reaches every node is `⌈ln(n) + c⌉` — a function of cluster
    size, not data volume. Two sweeps in one grid:

      * cluster size `n` (16/64/256) shows the observed
        `peak_effective_fanout` tracking `⌈ln(n-1) + c⌉`;
      * dirty-set cardinality (distinct keys written at once) at fixed `n`
        shows the per-tick fanout is *flat* in volume — the burst rides one
        fat frame and does not widen the peer pick.

    `DistinctKeyBurst` makes each write land in its own CellStore slot so
    `local_dirty.len()` actually jumps to `cells`; the fanout staying put
    is the direct refutation of an `O(N/fanout)` volume premise.
    """
    out = []
    for n_nodes in [16, 64, 256]:
        for dirty in [16, 256, 1024]:
            out.append(
                _base(
                    f"coverage_n{n_nodes}_d{dirty}",
                    nodes=n_nodes,
                    fanout=1,
                    duration="5s",
                    kind="coverage_fanout",
                    workload={
                        "shape": "distinct_key_burst",
                        "node": 0,
                        "cells": dirty,
                        "at": "50ms",
                    },
                    seed=0xC0F0 + dirty,
                    cell_capacity=4096,
                    max_cells_per_tick=4096,
                )
            )
    return out


def suite_error_budget() -> list[dict]:
    """Sweep `target_err_bps` against a sustained workload at N=16.

    Lower budgets fire emits sooner and use more bandwidth; higher
    budgets let local error accumulate. The workload picks a small
    rule_limit so the per-site ε_R = max(1, L × bps / 10_000 / N) is
    a single-digit number that the per-sample hit count straddles —
    that way the budget actually fires at the tight end of the sweep
    and rides the heartbeat at the loose end.

    Headline numbers:
      - bandwidth (bytes / node / s) climbs as bps tightens
      - max_lag stays bounded by `N × ε_R` (reported in `extras` as
        `theoretical_max_lag`).
    """
    # rule_limit=10_000, N=16 → ε_R = bps · 0.0625 hits. Sustained at
    # per_tick=5 hits/sample/source means each node accumulates 5 hits
    # per 100 ms sample. With this scaling, ε crosses inside one sample
    # window for bps ≲ 1600 and starts riding the heartbeat above that.
    # The bps sweep deliberately spans several decades of ε so we can
    # see the crossing.
    return [
        _base(
            f"errbudget_{bps:05d}bps",
            nodes=16,
            fanout=3,
            duration="10s",
            kind="error_budget",
            target_err_bps=bps,
            workload={
                "shape": "sustained",
                "sources": list(range(16)),
                "per_tick": 5,
                "rule_limit": 10_000,
            },
        )
        for bps in [50, 100, 250, 500, 1000, 2500, 5000, 10000]
    ]


def suite_min_emit_clamp() -> list[dict]:
    """Adversarial: ε saturates to 1 and the request stream pins it.

    Sweep the `min_emit_interval` floor; with a tight floor (or none),
    the cluster emits a packet per crossed-budget request. The floor
    caps the per-second emit rate. We confirm the cluster still
    converges after the burst regardless of the floor.
    """
    out = []
    for floor_ms in [0, 1, 5, 10, 50]:
        out.append(
            _base(
                f"minemit_{floor_ms:02d}ms",
                nodes=8,
                fanout=3,
                duration="1s",
                kind="min_emit_clamp",
                min_emit_interval=f"{floor_ms}ms",
                workload={
                    "shape": "burst_compressed",
                    "node": 0,
                    "hits": 10_000,
                    "at": "0ms",
                    "burst_span": "50ms",
                },
            )
        )
    return out


def suite_heartbeat_threshold_mix() -> list[dict]:
    """One hot rule (saturates ε every tick → threshold fires) and one
    cold rule (well under ε → rides the heartbeat) running side by
    side. Both must converge; the bench reports per-rule convergence
    millis via `extras.hot_convergence_millis` /
    `extras.cold_convergence_millis`."""
    return [
        _base(
            "mix_hotcold_8",
            nodes=8,
            fanout=3,
            duration="5s",
            kind="heartbeat_threshold_mix",
            workload={
                "shape": "two_rule",
                "hot_node": 0,
                "hot_per_tick": 200,
                "hot_limit": 1_000,
                "cold_node": 1,
                "cold_per_interval": 1,
                "cold_interval": "1s",
                "cold_limit": 1_000_000,
            },
        )
    ]


SUITES = {
    "convergence": suite_convergence,
    "fanout_sweep": suite_fanout_sweep,
    "loss": suite_loss,
    "partition": suite_partition,
    "staleness": suite_staleness,
    "scale_n": suite_scale_n,
    "coverage_fanout": suite_coverage_fanout,
    "error_budget": suite_error_budget,
    "min_emit_clamp": suite_min_emit_clamp,
    "heartbeat_threshold_mix": suite_heartbeat_threshold_mix,
}


# ----- plotting -------------------------------------------------------------

try:
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    import seaborn as sns
except ImportError:
    mpl = None
    plt = None
    sns = None


@dataclass
class PlotResult:
    name: str
    path: Path
    notes: list[str] = field(default_factory=list)


# Small clean styling pulled forward from the retired Tufte renderer.
# Serif fonts, no top/right spines, sparse gridlines — kept minimal so
# the plots read on a README at 1× zoom without screaming for attention.
PALETTE = [
    "#202020",
    "#6e6e6e",
    "#8a1a1f",
    "#274060",
    "#7a6f4a",
    "#4f6457",
    "#a05828",
    "#5a3a5e",
]


def _ensure_plot_libs() -> None:
    if plt is None or sns is None:
        raise SystemExit(
            "matplotlib + seaborn are required. Install with:\n"
            "  pip install matplotlib seaborn pandas"
        )


@contextmanager
def style():
    """Lightweight matplotlib rc context — clean serif look, SVG-friendly."""
    rc = {
        "font.family": "serif",
        "font.serif": ["DejaVu Serif", "Times New Roman", "Times"],
        "font.size": 10,
        "axes.titlesize": 11,
        "axes.titleweight": "regular",
        "axes.labelsize": 9.5,
        "axes.edgecolor": "#202020",
        "axes.linewidth": 0.6,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.color": "#cccccc",
        "grid.linewidth": 0.4,
        "xtick.direction": "out",
        "ytick.direction": "out",
        "legend.frameon": False,
        "legend.fontsize": 8.5,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "axes.prop_cycle": mpl.cycler(color=PALETTE),
        "lines.linewidth": 1.4,
        "lines.markersize": 4,
        "savefig.facecolor": "white",
        # Embed text as text in the SVG so it remains selectable / searchable
        # and renders correctly without bundled fonts.
        "svg.fonttype": "none",
    }
    with mpl.rc_context(rc):
        sns.set_palette(PALETTE)
        yield


def _save(fig, out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, format="svg", bbox_inches="tight")
    plt.close(fig)
    return out


# ----- per-suite plotters ---------------------------------------------------


def plot_convergence(results: list[dict], out_dir: Path) -> list[PlotResult]:
    _ensure_plot_libs()
    import pandas as pd

    rows = []
    for r in results:
        s = r["scenario"]
        h = r["headline"]
        rows.append(
            {
                "nodes": s["nodes"],
                "fanout": s["fanout"],
                "convergence_rounds": h["convergence_rounds"],
                "bytes_per_node_per_s": h["bytes_per_node_per_second"],
            }
        )
    df = pd.DataFrame(rows)

    with style():
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))

        ax = axes[0]
        for f, group in df.groupby("fanout"):
            group = group.sort_values("nodes")
            ax.plot(group["nodes"], group["convergence_rounds"], marker="o", label=f"fanout={f}")
        n_grid = sorted(df["nodes"].unique())
        ax.plot(n_grid, [math.log2(n) for n in n_grid], linestyle="--", color="#888",
                label="log₂ N (Karp lower bound)")
        ax.set_xscale("log", base=2)
        ax.set_xlabel("cluster size N")
        ax.set_ylabel("convergence (gossip rounds)")
        ax.set_title("Single-write convergence vs N")
        ax.legend(loc="best", fontsize=8)

        ax = axes[1]
        sns.lineplot(data=df, x="nodes", y="bytes_per_node_per_s", hue="fanout", marker="o", ax=ax)
        ax.set_xscale("log", base=2)
        ax.set_xlabel("cluster size N")
        ax.set_ylabel("bytes / node / second")
        ax.set_title("Idle gossip bandwidth")

        fig.tight_layout()
        return [PlotResult(name="convergence", path=_save(fig, FIGURES_DIR / "convergence.svg"))]


def plot_fanout_sweep(results: list[dict], out_dir: Path) -> list[PlotResult]:
    _ensure_plot_libs()
    import pandas as pd

    rows = [
        {
            "fanout": r["scenario"]["fanout"],
            "convergence_rounds": r["headline"]["convergence_rounds"],
            "bytes_per_node_per_s": r["headline"]["bytes_per_node_per_second"],
        }
        for r in results
    ]
    df = pd.DataFrame(rows).sort_values("fanout")
    with style():
        fig, ax = plt.subplots(figsize=(7, 4.2))
        ax2 = ax.twinx()
        l1, = ax.plot(df["fanout"], df["convergence_rounds"], marker="o", label="convergence rounds")
        l2, = ax2.plot(df["fanout"], df["bytes_per_node_per_s"], marker="s",
                       color=PALETTE[2], label="bytes/node/s")
        ax.set_xlabel("static fanout floor f")
        ax.set_ylabel("convergence (gossip rounds)")
        ax2.set_ylabel("bytes / node / s")
        ax.set_title("Convergence vs network cost at N=32")
        ax.legend(handles=[l1, l2], loc="best")
        fig.tight_layout()
        return [PlotResult(name="fanout_sweep", path=_save(fig, FIGURES_DIR / "fanout_sweep.svg"))]


def plot_loss(results: list[dict], out_dir: Path) -> list[PlotResult]:
    _ensure_plot_libs()
    import pandas as pd

    rows = []
    for r in results:
        h = r["headline"]
        rows.append(
            {
                "loss": r["scenario"]["network"]["uniform_loss"],
                "convergence_rounds": h["convergence_rounds"],
                "final_divergence": h["final_divergence"],
                "converged": h["convergence_millis"] is not None,
            }
        )
    df = pd.DataFrame(rows)
    converged = df[df["converged"]]

    with style():
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))

        ax = axes[0]
        sns.boxplot(data=converged, x="loss", y="convergence_rounds", ax=ax, color="#e5e5e5",
                    linewidth=0.7)
        sns.stripplot(data=converged, x="loss", y="convergence_rounds", ax=ax, color=PALETTE[0],
                      size=3.5, alpha=0.85)
        ax.set_xlabel("per-link drop probability")
        ax.set_ylabel("convergence (gossip rounds, N=16, f=3)")
        ax.set_title("Convergence under i.i.d. loss")

        ax = axes[1]
        sns.boxplot(data=df, x="loss", y="final_divergence", ax=ax, color="#e5e5e5", linewidth=0.7)
        ax.set_xlabel("per-link drop probability")
        ax.set_ylabel("final divergence (max-min observed total)")
        ax.set_title("Final divergence at run end")

        fig.tight_layout()
        return [PlotResult(name="loss", path=_save(fig, FIGURES_DIR / "loss.svg"))]


def plot_partition(results: list[dict], out_dir: Path) -> list[PlotResult]:
    _ensure_plot_libs()
    out = []
    with style():
        for r in results:
            samples = r["samples"]
            n_nodes = r["scenario"]["nodes"]
            fig, ax = plt.subplots(figsize=(9, 4.2))
            t = [s["t_millis"] / 1000 for s in samples]
            for i in range(n_nodes):
                ax.plot(t, [s["per_node_total"][i] for s in samples], alpha=0.6, lw=1.0)
            ax.plot(
                t,
                [s["ground_truth_total"] for s in samples],
                color="#202020",
                lw=1.8,
                linestyle="--",
                label="ground truth",
            )
            for change in r["scenario"]["network"]["schedule"]:
                secs = _hms_to_seconds(change["at"])
                ax.axvline(secs, color="#4f7059", linestyle=":", label=f"heal at {secs:.1f}s")
            reconv = r["headline"]["extras"].get("reconvergence_millis_after_heal")
            title = "Partition + heal"
            if reconv is not None:
                title += f"  ·  reconverged {reconv} ms after heal"
            ax.set_title(title)
            ax.set_xlabel("virtual time (s)")
            ax.set_ylabel("per-node observed total")
            ax.legend(loc="best", fontsize=8)
            fig.tight_layout()
            out.append(
                PlotResult(name=r["scenario"]["name"], path=_save(fig, FIGURES_DIR / "partition.svg"))
            )
    return out


def plot_staleness(results: list[dict], out_dir: Path) -> list[PlotResult]:
    _ensure_plot_libs()
    import pandas as pd

    rows = []
    for r in results:
        rows.append(
            {
                "sources": len(r["scenario"]["workload"]["sources"]),
                "p50_staleness_ms": r["headline"]["p50_staleness_millis"],
                "p95_staleness_ms": r["headline"]["p95_staleness_millis"],
            }
        )
    df = pd.DataFrame(rows).sort_values("sources")
    with style():
        fig, ax = plt.subplots(figsize=(7, 4.2))
        sns.lineplot(data=df, x="sources", y="p50_staleness_ms", marker="o", label="p50", ax=ax)
        sns.lineplot(data=df, x="sources", y="p95_staleness_ms", marker="s", label="p95", ax=ax)
        ax.set_xlabel("concurrent write sources")
        ax.set_ylabel("per-hit lag (ms)")
        ax.set_title("Per-hit delivery delay under sustained traffic")
        fig.tight_layout()
        return [PlotResult(name="staleness", path=_save(fig, FIGURES_DIR / "staleness.svg"))]


def plot_scale_n(results: list[dict], out_dir: Path) -> list[PlotResult]:
    _ensure_plot_libs()
    import pandas as pd

    rows = [
        {
            "nodes": r["scenario"]["nodes"],
            "convergence_rounds": r["headline"]["convergence_rounds"],
            "bytes_per_node_per_s": r["headline"]["bytes_per_node_per_second"],
        }
        for r in results
    ]
    df = pd.DataFrame(rows).sort_values("nodes")
    with style():
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))

        ax = axes[0]
        ax.plot(df["nodes"], df["convergence_rounds"], marker="o", label="observed")
        n_grid = sorted(df["nodes"].unique())
        ax.plot(n_grid, [math.log2(n) for n in n_grid], linestyle="--", color="#888", label="log₂ N")
        ax.set_xscale("log", base=2)
        ax.set_xlabel("cluster size N")
        ax.set_ylabel("convergence (gossip rounds)")
        ax.set_title("Scaling: rounds-to-converge")
        ax.legend()

        ax = axes[1]
        ax.plot(df["nodes"], df["bytes_per_node_per_s"], marker="o")
        ax.set_xscale("log", base=2)
        ax.set_xlabel("cluster size N")
        ax.set_ylabel("bytes / node / s")
        ax.set_title("Scaling: per-node steady-state bandwidth")

        fig.tight_layout()
        return [PlotResult(name="scale_n", path=_save(fig, FIGURES_DIR / "scale_n.svg"))]


def plot_coverage_fanout(results: list[dict], out_dir: Path) -> list[PlotResult]:
    """Two claims, two panels: (left) the per-tick fanout tracks the
    coverage threshold `⌈ln(n)+c⌉` as the cluster grows; (right) it is flat
    in burst volume, because the dirty set rides one fat frame rather than
    widening the peer pick.
    """
    _ensure_plot_libs()
    import pandas as pd

    rows = []
    for r in results:
        rows.append(
            {
                "nodes": r["scenario"]["nodes"],
                "dirty": r["scenario"]["workload"]["cells"],
                "peak_fanout": r["headline"].get("peak_effective_fanout"),
                "effective_fanout_p50": r["headline"]["effective_fanout_p50"],
            }
        )
    df = pd.DataFrame(rows).sort_values(["nodes", "dirty"])
    with style():
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))

        # Left: peak per-tick fanout vs N, with the predicted ⌈ln(n)+c⌉
        # reference. Peak is invariant over the dirty sweep, so collapse to
        # the max per N.
        ax = axes[0]
        by_n = df.groupby("nodes")["peak_fanout"].max().reset_index()
        ax.plot(by_n["nodes"], by_n["peak_fanout"], marker="o",
                label="observed peak fanout")
        n_grid = sorted(df["nodes"].unique())
        ax.plot(n_grid, [_coverage_pick(n) for n in n_grid], linestyle="--", color="#888",
                label="⌈ln(n)+c⌉ (coverage threshold)")
        ax.set_xscale("log", base=2)
        ax.set_xlabel("cluster size N")
        ax.set_ylabel("peak per-tick fanout")
        ax.set_title("Coverage fanout tracks ln(N)+c")
        ax.legend(fontsize=8)

        # Right: effective fanout vs dirty-set size, one line per N. Flat
        # lines are the point — volume does not widen the pick.
        ax = axes[1]
        for n, group in df.groupby("nodes"):
            ax.plot(group["dirty"], group["effective_fanout_p50"], marker="o", label=f"N={n}")
        ax.set_xscale("log", base=2)
        ax.set_xlabel("burst-write cardinality (dirty cells)")
        ax.set_ylabel("effective per-tick fanout (packets / dirty-tick)")
        ax.set_title("Fanout is flat in burst volume")
        ax.legend(fontsize=8)

        fig.tight_layout()
        return [PlotResult(name="coverage_fanout",
                           path=_save(fig, FIGURES_DIR / "coverage_fanout.svg"))]


def plot_error_budget(results: list[dict], out_dir: Path) -> list[PlotResult]:
    """Bandwidth and max lag against the per-rule error budget."""
    _ensure_plot_libs()
    import pandas as pd

    rows = []
    for r in results:
        h = r["headline"]
        extras = h.get("extras", {})
        rows.append(
            {
                "bps": r["scenario"]["target_err_bps"],
                "bytes_per_node_per_s": h["bytes_per_node_per_second"],
                "max_lag": h["max_lag"],
                "theoretical_max_lag": extras.get("theoretical_max_lag"),
                "threshold_fires_per_node": h["threshold_fires_per_node"],
            }
        )
    df = pd.DataFrame(rows).sort_values("bps")
    with style():
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))

        ax = axes[0]
        ax.plot(df["bps"], df["bytes_per_node_per_s"], marker="o", label="bytes/node/s")
        ax2 = ax.twinx()
        ax2.plot(df["bps"], df["threshold_fires_per_node"], marker="s", color=PALETTE[2],
                 label="threshold fires / node")
        ax2.spines["right"].set_visible(True)
        ax.set_xscale("log")
        ax.set_xlabel("target_err_bps (per-rule budget, log scale)")
        ax.set_ylabel("bytes / node / second")
        ax2.set_ylabel("threshold fires / node")
        ax.set_title("Bandwidth vs error budget")
        lines, labels = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines + lines2, labels + labels2, loc="upper right")

        ax = axes[1]
        ax.plot(df["bps"], df["max_lag"], marker="o", label="empirical max lag")
        ax.plot(df["bps"], df["theoretical_max_lag"], marker="x", linestyle="--",
                color="#888", label="theoretical N·ε bound")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("target_err_bps")
        ax.set_ylabel("max lag (hits)")
        ax.set_title("Empirical max lag stays under N·ε")
        ax.legend()

        fig.tight_layout()
        return [PlotResult(name="error_budget",
                           path=_save(fig, FIGURES_DIR / "error_budget.svg"))]


def plot_min_emit_clamp(results: list[dict], out_dir: Path) -> list[PlotResult]:
    """Bandwidth and threshold-fires/node against the min_emit_interval floor."""
    _ensure_plot_libs()
    import pandas as pd

    rows = []
    for r in results:
        h = r["headline"]
        rows.append(
            {
                "floor_ms": r["headline"]["extras"].get("min_emit_interval_ms", 0),
                "bytes_per_node_per_s": h["bytes_per_node_per_second"],
                "packets_per_node_per_s": h["packets_per_node_per_second"],
                "threshold_fires_per_node": h["threshold_fires_per_node"],
                "final_divergence": h["final_divergence"],
            }
        )
    df = pd.DataFrame(rows).sort_values("floor_ms")
    with style():
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))

        ax = axes[0]
        ax.plot(df["floor_ms"], df["bytes_per_node_per_s"], marker="o", label="bytes/node/s")
        ax.plot(df["floor_ms"], df["packets_per_node_per_s"], marker="s", label="packets/node/s")
        ax.set_xlabel("min_emit_interval floor (ms)")
        ax.set_ylabel("rate / node / s")
        ax.set_title("Bandwidth shrinks as the floor tightens emit cadence")
        ax.legend()

        ax = axes[1]
        ax.plot(df["floor_ms"], df["threshold_fires_per_node"], marker="o",
                label="threshold fires / node")
        ax.plot(df["floor_ms"], df["final_divergence"], marker="s",
                label="final divergence (hits)")
        ax.set_xlabel("min_emit_interval floor (ms)")
        ax.set_ylabel("count")
        ax.set_title("Cluster still converges; divergence stays at zero")
        ax.legend()

        fig.tight_layout()
        return [PlotResult(name="min_emit_clamp",
                           path=_save(fig, FIGURES_DIR / "min_emit_clamp.svg"))]


def plot_heartbeat_threshold_mix(results: list[dict], out_dir: Path) -> list[PlotResult]:
    """Per-rule replication over time: hot rule vs cold rule."""
    _ensure_plot_libs()
    out: list[PlotResult] = []
    with style():
        for r in results:
            samples = r["samples"]
            n_nodes = r["scenario"]["nodes"]
            fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
            t = [s["t_millis"] / 1000 for s in samples]

            ax = axes[0]
            for i in range(n_nodes):
                ax.plot(t, [s["per_node_hot_total"][i] for s in samples], alpha=0.6, lw=1.0)
            ax.plot(t, [s["ground_truth_hot_total"] for s in samples], color="#202020",
                    lw=1.6, linestyle="--", label="ground truth")
            ax.set_xlabel("virtual time (s)")
            ax.set_ylabel("hot rule: per-node total")
            hot_conv = r["headline"]["extras"].get("hot_convergence_millis")
            title = "Hot rule — threshold fires"
            if hot_conv is not None:
                title += f"  ·  first converged at {hot_conv} ms"
            ax.set_title(title)
            ax.legend(fontsize=8)

            ax = axes[1]
            for i in range(n_nodes):
                ax.plot(t, [s["per_node_cold_total"][i] for s in samples], alpha=0.6, lw=1.0)
            ax.plot(t, [s["ground_truth_cold_total"] for s in samples], color="#202020",
                    lw=1.6, linestyle="--", label="ground truth")
            ax.set_xlabel("virtual time (s)")
            ax.set_ylabel("cold rule: per-node total")
            cold_conv = r["headline"]["extras"].get("cold_convergence_millis")
            title = "Cold rule — heartbeat"
            if cold_conv is not None:
                title += f"  ·  first converged at {cold_conv} ms"
            ax.set_title(title)
            ax.legend(fontsize=8)

            fig.tight_layout()
            out.append(
                PlotResult(
                    name="heartbeat_threshold_mix",
                    path=_save(fig, FIGURES_DIR / "heartbeat_threshold_mix.svg"),
                )
            )
    return out


def _hms_to_seconds(spec: str) -> float:
    spec = spec.strip()
    if spec.endswith("ms"):
        return float(spec[:-2]) / 1000.0
    if spec.endswith("s"):
        return float(spec[:-1])
    if spec.endswith("m"):
        return float(spec[:-1]) * 60.0
    return float(spec)


PLOTTERS = {
    "convergence": plot_convergence,
    "fanout_sweep": plot_fanout_sweep,
    "loss": plot_loss,
    "partition": plot_partition,
    "staleness": plot_staleness,
    "scale_n": plot_scale_n,
    "coverage_fanout": plot_coverage_fanout,
    "error_budget": plot_error_budget,
    "min_emit_clamp": plot_min_emit_clamp,
    "heartbeat_threshold_mix": plot_heartbeat_threshold_mix,
}


# ----- entry point ----------------------------------------------------------


def run_suite(name: str, binary: Path, out_root: Path) -> list[PlotResult]:
    print(f"\n=== suite: {name} ===", flush=True)
    scenarios = SUITES[name]()
    print(f"  scenarios: {len(scenarios)}", flush=True)
    out_dir = out_root / name
    out_dir.mkdir(parents=True, exist_ok=True)
    results = run_bench(binary, scenarios)
    with (out_dir / "results.jsonl").open("w") as f:
        for r in results:
            f.write(json.dumps(r))
            f.write("\n")
    return PLOTTERS[name](results, out_dir)


def publish_to_repo(svgs: list[Path]) -> None:
    PUBLISH_DIR.mkdir(parents=True, exist_ok=True)
    for src in svgs:
        dst = PUBLISH_DIR / src.name
        shutil.copy2(src, dst)
        print(f"  published {dst.relative_to(REPO_ROOT)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "suites",
        nargs="*",
        default=["all"],
        help=f"one of: {', '.join(SUITES)} or 'all'",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=TARGET_ROOT,
        help="output root (default: target/gossip-bench)",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help=(
            "after rendering, copy each SVG into crates/gabion/figures/ "
            "so the README renders without re-running the bench"
        ),
    )
    args = parser.parse_args()

    if "all" in args.suites:
        chosen = list(SUITES.keys())
    else:
        chosen = []
        for s in args.suites:
            if s not in SUITES:
                raise SystemExit(f"unknown suite: {s}")
            chosen.append(s)

    binary = build_bench()
    args.out.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    summary: list[PlotResult] = []
    for suite in chosen:
        summary.extend(run_suite(suite, binary, args.out))

    print("\n=== plots written ===")
    for plot in summary:
        print(f"  {plot.path.relative_to(REPO_ROOT)}")

    if args.publish:
        print("\n=== publishing to crates/gabion/figures/ ===")
        publish_to_repo([plot.path for plot in summary])


if __name__ == "__main__":
    main()
