#!/usr/bin/env python3
"""Drive the `gossip-bench` Rust binary across a matrix of scenarios and
produce matplotlib/seaborn plots from the JSON results.

Why Python: the bench binary is the source of truth — it emits stable
JSON. Python is for matrix orchestration + plotting only, which is what
matplotlib/seaborn are well-suited for. The simulator runs entirely in
Rust on virtual time, so there are no realtime / network reliability
concerns.

Usage:
    python3 bench/plot.py all            # full suite
    python3 bench/plot.py convergence    # one suite
    python3 bench/plot.py --help

Plots are written to `target/gossip-bench/<suite>/*.png` and the raw
JSON for each scenario is preserved alongside.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

# ----- bench binary plumbing ------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]
TARGET_ROOT = REPO_ROOT / "target" / "gossip-bench"
RUSTUP_CARGO = os.environ.get(
    "GOSSIP_BENCH_CARGO",
    str(Path.home() / ".rustup" / "toolchains" / "stable-aarch64-apple-darwin" / "bin" / "cargo"),
)


def cargo_bin() -> str:
    """Resolve a cargo executable that works in any environment."""
    if Path(RUSTUP_CARGO).exists():
        return RUSTUP_CARGO
    # Fall back to PATH lookup.
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
    """Demers/Karp: single-write convergence vs cluster size and fanout.
    N sweeps up to 256 here (the scale_n suite goes further). Bigger
    clusters take more virtual ticks to converge, so the duration grows
    with N to make sure we capture the convergence point even at
    fanout=1."""
    out = []
    for n in [4, 8, 16, 32, 64, 128, 256]:
        # ceil(2 * log2(n)) seconds is enough virtual time for fanout=1
        # to finish; smaller fanouts converge faster.
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
    """Convergence time as fanout increases at fixed N=32."""
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
    """Bimodal Multicast / SWIM: convergence under i.i.d. per-link drop
    probability. We sweep loss from 0% to 50% (Birman et al. report
    bimodal stability up to ~25-30%). N=16, fanout=3, 3 trials each.

    The simulator's `DropProb` policy uses a deterministic per-link
    splitmix seeded from the link's (src, dst) pair, so trials only
    diverge through the `rng_seed` we feed the gossip runtime — that
    selects a different peer-sampling order. The drop pattern itself is
    pinned per scenario seed."""
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
    """Convergence as cluster size scales — the classic log-N curve.
    Stretched all the way to N=1024 to demonstrate the protocol holds
    its log-N shape and its constant per-node bandwidth at scale.
    Duration grows with N so the runner captures the convergence point
    even when fanout=3 needs ~log_2(N) rounds at 100ms ticks."""
    out = []
    for n in [4, 8, 16, 32, 64, 128, 256, 512, 1024]:
        # 4 * log2(N) tick periods = 0.4 * log2(N) seconds of headroom.
        # Floor at 8 s for the small clusters.
        duration_s = max(8, 4 * (n.bit_length() - 1) // 10 + 1) * 4
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


SUITES = {
    "convergence": suite_convergence,
    "fanout_sweep": suite_fanout_sweep,
    "loss": suite_loss,
    "partition": suite_partition,
    "staleness": suite_staleness,
    "scale_n": suite_scale_n,
}


# ----- plotting -------------------------------------------------------------

import math

try:
    import matplotlib.pyplot as plt
    import seaborn as sns
except ImportError:
    plt = None
    sns = None


@dataclass
class PlotResult:
    name: str
    path: Path
    notes: list[str] = field(default_factory=list)


def _ensure_plot_libs() -> None:
    if plt is None or sns is None:
        raise SystemExit(
            "matplotlib + seaborn are required. Install with:\n"
            "  pip install matplotlib seaborn pandas"
        )
    sns.set_theme(context="paper", style="whitegrid", palette="deep")


def plot_convergence(results: list[dict], out_dir: Path) -> list[PlotResult]:
    """Convergence rounds vs cluster size, faceted by fanout."""
    _ensure_plot_libs()
    import pandas as pd

    rows = []
    for r in results:
        s = r["scenario"]
        h = r["headline"]
        rows.append(
            {
                "name": s["name"],
                "nodes": s["nodes"],
                "fanout": s["fanout"],
                "convergence_rounds": h["convergence_rounds"],
                "convergence_millis": h["convergence_millis"],
                "bytes_per_node_per_s": h["bytes_per_node_per_second"],
            }
        )
    df = pd.DataFrame(rows)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))

    # (a) Convergence rounds vs N, one line per fanout.
    ax = axes[0]
    for f, group in df.groupby("fanout"):
        group = group.sort_values("nodes")
        ax.plot(
            group["nodes"],
            group["convergence_rounds"],
            marker="o",
            label=f"fanout={f}",
        )
    # Theoretical lower bound: log_2(N) for pure push (Karp et al.).
    n_grid = sorted(df["nodes"].unique())
    ax.plot(
        n_grid,
        [math.log2(n) for n in n_grid],
        linestyle="--",
        color="black",
        label="log₂ N (Karp lower bound)",
    )
    ax.set_xscale("log", base=2)
    ax.set_xlabel("cluster size N")
    ax.set_ylabel("convergence (gossip rounds)")
    ax.set_title("Single-write convergence vs N (Demers/Karp)")
    ax.legend(loc="best", fontsize=8)

    # (b) Bandwidth at idle (after convergence): bytes/node/s.
    ax = axes[1]
    sns.lineplot(
        data=df,
        x="nodes",
        y="bytes_per_node_per_s",
        hue="fanout",
        marker="o",
        ax=ax,
    )
    ax.set_xscale("log", base=2)
    ax.set_xlabel("cluster size N")
    ax.set_ylabel("bytes / node / second")
    ax.set_title("Idle gossip bandwidth (repair lane + dirty heartbeat)")

    fig.tight_layout()
    out = out_dir / "convergence.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    return [PlotResult(name="convergence", path=out)]


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
    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax2 = ax.twinx()
    line1, = ax.plot(
        df["fanout"], df["convergence_rounds"], marker="o", label="convergence rounds"
    )
    line2, = ax2.plot(
        df["fanout"],
        df["bytes_per_node_per_s"],
        marker="s",
        color="crimson",
        label="bytes/node/s",
    )
    ax.set_xlabel("fanout f")
    ax.set_ylabel("convergence (gossip rounds)")
    ax2.set_ylabel("bytes / node / s")
    ax.set_title("Convergence vs network cost at N=32")
    ax.legend(handles=[line1, line2], loc="best")
    fig.tight_layout()
    out = out_dir / "fanout_sweep.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    return [PlotResult(name="fanout_sweep", path=out)]


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
                "convergence_millis": h["convergence_millis"],
                "final_divergence": h["final_divergence"],
                "converged": h["convergence_millis"] is not None,
            }
        )
    df = pd.DataFrame(rows)
    converged = df[df["converged"]]

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))

    ax = axes[0]
    sns.boxplot(data=converged, x="loss", y="convergence_rounds", ax=ax)
    sns.stripplot(
        data=converged,
        x="loss",
        y="convergence_rounds",
        ax=ax,
        color="black",
        alpha=0.6,
        size=3,
    )
    ax.set_xlabel("per-link drop probability")
    ax.set_ylabel("convergence (gossip rounds, N=16, f=3)")
    ax.set_title("Convergence under i.i.d. loss (Bimodal Multicast)")

    ax = axes[1]
    # Final divergence (max - min) — non-zero values mean the run did
    # NOT fully converge by the end of the scenario window.
    sns.boxplot(data=df, x="loss", y="final_divergence", ax=ax)
    ax.set_xlabel("per-link drop probability")
    ax.set_ylabel("final divergence (max-min observed total)")
    ax.set_title("Final divergence at run end")

    fig.tight_layout()
    out = out_dir / "loss.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    return [PlotResult(name="loss", path=out)]


def plot_partition(results: list[dict], out_dir: Path) -> list[PlotResult]:
    _ensure_plot_libs()
    plots: list[PlotResult] = []
    for r in results:
        samples = r["samples"]
        n_nodes = r["scenario"]["nodes"]
        fig, ax = plt.subplots(figsize=(9, 4.5))
        t = [s["t_millis"] / 1000 for s in samples]
        for i in range(n_nodes):
            ax.plot(t, [s["per_node_total"][i] for s in samples], alpha=0.6, lw=1.2)
        ax.plot(
            t,
            [s["ground_truth_total"] for s in samples],
            color="black",
            lw=2.0,
            linestyle="--",
            label="ground truth",
        )
        # Mark heal point.
        for change in r["scenario"]["network"]["schedule"]:
            secs = _hms_to_seconds(change["at"])
            ax.axvline(secs, color="green", linestyle=":", label=f"heal at {secs:.1f}s")
        reconv = r["headline"]["extras"].get("reconvergence_millis_after_heal")
        title = "Partition + heal (SWIM-style failure recovery)"
        if reconv is not None:
            title += f"  ·  reconvergence {reconv} ms ({reconv/1000:.2f}s) after heal"
        ax.set_title(title)
        ax.set_xlabel("virtual time (s)")
        ax.set_ylabel("per-node observed total")
        ax.legend(loc="best", fontsize=8)
        fig.tight_layout()
        out = out_dir / f"{r['scenario']['name']}.png"
        fig.savefig(out, dpi=140)
        plt.close(fig)
        plots.append(PlotResult(name=r["scenario"]["name"], path=out))
    return plots


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
    fig, ax = plt.subplots(figsize=(7, 4.5))
    sns.lineplot(data=df, x="sources", y="p50_staleness_ms", marker="o", label="p50", ax=ax)
    sns.lineplot(data=df, x="sources", y="p95_staleness_ms", marker="s", label="p95", ax=ax)
    ax.set_xlabel("concurrent write sources")
    ax.set_ylabel("per-hit lag (ms)")
    ax.set_title("Per-hit delivery delay under sustained traffic (Astrolabe)")
    fig.tight_layout()
    out = out_dir / "staleness.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    return [PlotResult(name="staleness", path=out)]


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
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))

    ax = axes[0]
    ax.plot(df["nodes"], df["convergence_rounds"], marker="o", label="observed")
    n_grid = sorted(df["nodes"].unique())
    ax.plot(
        n_grid,
        [math.log2(n) for n in n_grid],
        linestyle="--",
        color="black",
        label="log₂ N",
    )
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
    out = out_dir / "scale_n.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    return [PlotResult(name="scale_n", path=out)]


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
}


# ----- entry point ----------------------------------------------------------


def run_suite(name: str, binary: Path, out_root: Path) -> list[PlotResult]:
    print(f"\n=== suite: {name} ===", flush=True)
    scenarios = SUITES[name]()
    print(f"  scenarios: {len(scenarios)}", flush=True)
    out_dir = out_root / name
    out_dir.mkdir(parents=True, exist_ok=True)
    results = run_bench(binary, scenarios)
    # Persist raw JSON so the plot is reproducible.
    with (out_dir / "results.jsonl").open("w") as f:
        for r in results:
            f.write(json.dumps(r))
            f.write("\n")
    return PLOTTERS[name](results, out_dir)


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
    summary: list[PlotResult] = []
    for suite in chosen:
        summary.extend(run_suite(suite, binary, args.out))

    print("\n=== plots written ===")
    for plot in summary:
        print(f"  {plot.path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
