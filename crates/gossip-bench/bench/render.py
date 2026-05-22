#!/usr/bin/env python3
"""Render Tufte-style SVG plots and a Typst data fragment from the
`gossip-bench` JSONL results. The companion file `report.typ` consumes
both to produce the final typeset PDF.

Why split it this way:
- The plots come straight from matplotlib SVG output (vector, embedded
  in the PDF by Typst via `image("…svg")`).
- The narrative + tables come from Typst directly, so the typographic
  hierarchy (Bringhurst's rule: one type family at three sizes, never
  two families) stays inside the typesetting tool. Python writes the
  *data* (`data.typ` constants); Typst owns the layout.

Usage:
    python3 bench/render.py            # uses target/gossip-bench/
    python3 bench/render.py --regenerate
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Iterable

# Path / cargo plumbing — shared with plot.py.
REPO_ROOT = Path(__file__).resolve().parents[3]
TARGET_ROOT = REPO_ROOT / "target" / "gossip-bench"
FIG_DIR = TARGET_ROOT / "figures"
DATA_TYP = TARGET_ROOT / "data.typ"

SUITES = [
    "convergence",
    "fanout_sweep",
    "scale_n",
    "loss",
    "partition",
    "staleness",
]


def _ensure_plot_libs():
    try:
        import matplotlib  # noqa
        import seaborn  # noqa
        import pandas  # noqa
    except ImportError:
        raise SystemExit(
            "matplotlib + seaborn + pandas required.\n"
            "  pip install matplotlib seaborn pandas"
        )


# ----- figure builders ------------------------------------------------------


def fig_convergence(results: list[dict]) -> "matplotlib.figure.Figure":
    import matplotlib.pyplot as plt
    import pandas as pd

    from tufte import (
        INK,
        PALETTE,
        direct_label,
        offset_spines,
        tight_x,
        tight_y,
        title_only,
        tufte_rc,
    )

    rows = []
    for r in results:
        s, h = r["scenario"], r["headline"]
        rows.append(
            {
                "nodes": s["nodes"],
                "fanout": s["fanout"],
                "rounds": h["convergence_rounds"],
                "bytes_per_s": h["bytes_per_node_per_second"],
            }
        )
    df = pd.DataFrame(rows)

    with tufte_rc():
        fig, (left, right) = plt.subplots(1, 2, figsize=(6.6, 3.0))

        # --- Convergence vs N, one line per fanout ---
        fanouts = sorted(df["fanout"].unique())
        for i, f in enumerate(fanouts):
            sub = df[df["fanout"] == f].sort_values("nodes")
            color = PALETTE[i % len(PALETTE)]
            left.plot(sub["nodes"], sub["rounds"], marker="o", color=color)
            # Direct-label each line at its right-most point.
            last = sub.iloc[-1]
            direct_label(
                left,
                last["nodes"],
                last["rounds"],
                f"f={f}",
                color=color,
                fontsize=8,
            )

        ns = sorted(df["nodes"].unique())
        left.plot(
            ns,
            [math.log2(n) for n in ns],
            linestyle=":",
            color=INK,
            linewidth=0.8,
        )
        direct_label(
            left,
            ns[-1],
            math.log2(ns[-1]),
            "log₂ N",
            color=INK,
            fontsize=7.5,
            style="italic",
        )

        left.set_xscale("log", base=2)
        left.set_xlabel("cluster size N")
        left.set_ylabel("rounds to converge")
        title_only(left, "convergence")
        offset_spines(left)
        tight_x(left, df["nodes"])
        tight_y(left, list(df["rounds"]) + [math.log2(n) for n in ns])

        # --- Bandwidth vs N ---
        for i, f in enumerate(fanouts):
            sub = df[df["fanout"] == f].sort_values("nodes")
            color = PALETTE[i % len(PALETTE)]
            right.plot(sub["nodes"], sub["bytes_per_s"], marker="o", color=color)
            last = sub.iloc[-1]
            direct_label(
                right, last["nodes"], last["bytes_per_s"], f"f={f}", color=color
            )

        right.set_xscale("log", base=2)
        right.set_xlabel("cluster size N")
        right.set_ylabel("bytes per node, per second")
        title_only(right, "per-node bandwidth")
        offset_spines(right)
        tight_x(right, df["nodes"])
        tight_y(right, df["bytes_per_s"])

        fig.tight_layout()
        return fig


def fig_fanout_sweep(results: list[dict]):
    import matplotlib.pyplot as plt
    import pandas as pd

    from tufte import (
        INK,
        PALETTE,
        direct_label,
        offset_spines,
        tight_x,
        tight_y,
        title_only,
        tufte_rc,
    )

    df = pd.DataFrame(
        [
            {
                "fanout": r["scenario"]["fanout"],
                "rounds": r["headline"]["convergence_rounds"],
                "bytes_per_s": r["headline"]["bytes_per_node_per_second"],
            }
            for r in results
        ]
    ).sort_values("fanout")

    with tufte_rc():
        fig, ax = plt.subplots(figsize=(6.6, 3.0))
        ax2 = ax.twinx()
        ax2.spines["top"].set_visible(False)
        ax2.spines["left"].set_visible(False)
        ax2.tick_params(axis="y", colors=PALETTE[2])
        ax2.spines["right"].set_color(PALETTE[2])

        ax.plot(df["fanout"], df["rounds"], marker="o", color=PALETTE[0])
        ax2.plot(df["fanout"], df["bytes_per_s"], marker="s", color=PALETTE[2])

        last = df.iloc[-1]
        # Bigger right-edge offset so the labels clear the tick
        # numbers; pull them slightly off the data line vertically too.
        direct_label(
            ax,
            last["fanout"],
            last["rounds"],
            "rounds (left axis)",
            color=PALETTE[0],
            xytext=(10, 8),
        )
        direct_label(
            ax2,
            last["fanout"],
            last["bytes_per_s"],
            "bytes / s (right axis)",
            color=PALETTE[2],
            fontsize=8,
            xytext=(10, -8),
        )

        ax.set_xlabel("fanout f")
        ax.set_ylabel("rounds to converge")
        ax2.set_ylabel("bytes per node, per second")
        title_only(ax, "convergence vs network cost at N = 32")
        offset_spines(ax)
        # A little extra x-room on the right so the labels render.
        tight_x(ax, list(df["fanout"]) + [df["fanout"].max() + 1.5])
        tight_y(ax, df["rounds"])
        ax2.set_ylim(0, max(df["bytes_per_s"]) * 1.05)
        fig.tight_layout()
        return fig


def fig_scale_n(results: list[dict]):
    import matplotlib.pyplot as plt
    import pandas as pd

    from tufte import (
        INK,
        direct_label,
        offset_spines,
        tight_x,
        tight_y,
        title_only,
        tufte_rc,
    )

    df = pd.DataFrame(
        [
            {
                "nodes": r["scenario"]["nodes"],
                "rounds": r["headline"]["convergence_rounds"],
                "bytes_per_s": r["headline"]["bytes_per_node_per_second"],
            }
            for r in results
        ]
    ).sort_values("nodes")

    with tufte_rc():
        fig, (left, right) = plt.subplots(1, 2, figsize=(6.6, 3.0))

        left.plot(df["nodes"], df["rounds"], marker="o", color=INK)
        ns = list(df["nodes"])
        left.plot(
            ns,
            [math.log2(n) for n in ns],
            linestyle=":",
            color=INK,
            linewidth=0.8,
        )
        direct_label(left, ns[-1], math.log2(ns[-1]), "log₂ N", style="italic", fontsize=8)
        left.set_xscale("log", base=2)
        left.set_xlabel("cluster size N")
        left.set_ylabel("rounds to converge")
        title_only(left, "scaling: rounds-to-converge (f = 3)")
        offset_spines(left)
        tight_x(left, ns)
        tight_y(left, list(df["rounds"]) + [math.log2(n) for n in ns])

        right.plot(df["nodes"], df["bytes_per_s"], marker="o", color=INK)
        right.set_xscale("log", base=2)
        right.set_xlabel("cluster size N")
        right.set_ylabel("bytes per node, per second")
        title_only(right, "per-node bandwidth (the SWIM constant-load claim)")
        offset_spines(right)
        tight_x(right, ns)
        # The SWIM headline is "load is FLAT in N". A non-zero y-axis
        # would visually amplify a ~30% range into a steep curve, which
        # would lie about the claim. Anchor at 0.
        right.set_ylim(0, max(df["bytes_per_s"]) * 1.1)

        fig.tight_layout()
        return fig


def fig_loss(results: list[dict]):
    import matplotlib.pyplot as plt
    import pandas as pd

    from tufte import (
        INK,
        PALETTE,
        offset_spines,
        tight_y,
        title_only,
        tufte_rc,
    )

    rows = [
        {
            "loss": r["scenario"]["network"]["uniform_loss"],
            "rounds": r["headline"]["convergence_rounds"],
            "divergence": r["headline"]["final_divergence"],
        }
        for r in results
    ]
    df = pd.DataFrame(rows)

    with tufte_rc():
        fig, ax = plt.subplots(figsize=(6.6, 3.0))
        # Tufte-style strip plot: trials as jittered dots; tiny crossbar
        # for the median per loss level. No box, no whiskers.
        losses = sorted(df["loss"].unique())
        for i, loss in enumerate(losses):
            ys = df[df["loss"] == loss]["rounds"].dropna().tolist()
            if not ys:
                continue
            # Center-aligned strip at x = i with small horizontal jitter.
            xs = [i + (k - (len(ys) - 1) / 2) * 0.04 for k in range(len(ys))]
            ax.plot(xs, ys, "o", color=INK, markersize=3, alpha=0.7)
            median = statistics.median(ys)
            ax.plot([i - 0.18, i + 0.18], [median, median], color=PALETTE[2], linewidth=1.5)
        ax.set_xticks(range(len(losses)))
        ax.set_xticklabels([f"{int(l * 100)}%" for l in losses])
        ax.set_xlabel("per-link drop probability (i.i.d.)")
        ax.set_ylabel("rounds to converge")
        title_only(ax, "convergence under loss · N = 16, f = 3, 3 trials each")
        # Annotate the FIRST loss level so the arrow doesn't pile on
        # top of the rightmost data point. The label sits well above
        # the data; arrow head touches the median bar from outside.
        first_med = statistics.median(df[df["loss"] == losses[0]]["rounds"].dropna())
        ax.annotate(
            "median over 3 trials",
            xy=(0.18, first_med),
            xytext=(28, 16),
            textcoords="offset points",
            fontsize=8,
            color=PALETTE[2],
            arrowprops=dict(arrowstyle="->", color=PALETTE[2], lw=0.5),
            va="center",
        )
        offset_spines(ax)
        ax.set_xlim(-0.4, len(losses) - 0.6)
        tight_y(ax, list(df["rounds"]) + [max(df["rounds"]) + 1])
        fig.tight_layout()
        return fig


def fig_partition(results: list[dict]):
    import matplotlib.pyplot as plt

    from tufte import (
        INK,
        PALETTE,
        offset_spines,
        tight_x,
        tight_y,
        title_only,
        tufte_rc,
    )

    r = results[0]
    samples = r["samples"]
    n_nodes = r["scenario"]["nodes"]
    half = n_nodes // 2

    with tufte_rc():
        fig, ax = plt.subplots(figsize=(6.6, 3.0))
        t = [s["t_millis"] / 1000 for s in samples]

        # Group nodes by partition side so the two bands don't overlap
        # into a single illegible smear.
        left_side, right_side = PALETTE[0], PALETTE[3]
        for i in range(n_nodes):
            color = left_side if i < half else right_side
            ys = [s["per_node_total"][i] for s in samples]
            ax.plot(t, ys, color=color, alpha=0.45, lw=0.8)

        gt = [s["ground_truth_total"] for s in samples]
        ax.plot(t, gt, color=INK, lw=1.6, linestyle="--")

        # Heal marker, with a short arrow tying the label to the line
        # so the reader isn't left guessing which event it describes.
        for change in r["scenario"]["network"]["schedule"]:
            heal_at = _hms_to_seconds(change["at"])
            ax.axvline(heal_at, color=PALETTE[2], linestyle=":", lw=0.8)
            ax.annotate(
                f"heal at t = {heal_at:.0f} s",
                xy=(heal_at, max(gt) * 0.55),
                xytext=(10, 0),
                textcoords="offset points",
                fontsize=8,
                color=PALETTE[2],
                arrowprops=dict(arrowstyle="-", color=PALETTE[2], lw=0.5),
                va="center",
            )

        # Stagger the right-edge labels vertically so they don't pile
        # onto the same y-coordinate. "ground truth" rides slightly
        # above the line; "nodes 0..3" sits slightly below; "nodes
        # 4..7" goes above its baseline.
        ax.annotate(
            "ground truth",
            (t[-1], gt[-1]),
            xytext=(6, 6),
            textcoords="offset points",
            fontsize=8,
            color=INK,
            va="bottom",
        )
        ax.annotate(
            "nodes 0..3 (write side)",
            (t[-1], r["samples"][-1]["per_node_total"][0]),
            xytext=(6, -10),
            textcoords="offset points",
            fontsize=8,
            color=left_side,
            va="top",
        )
        ax.annotate(
            "nodes 4..7 (cut side, pre-heal)",
            (t[len(t) // 4], 0),
            xytext=(0, 10),
            textcoords="offset points",
            fontsize=8,
            color=right_side,
            va="bottom",
        )

        ax.set_xlabel("virtual time (s)")
        ax.set_ylabel("observed total")
        title_only(ax, "partition + heal · N = 8, two equal halves")
        offset_spines(ax)
        tight_x(ax, t)
        ax.set_ylim(-0.5, max(gt) + 0.6)
        fig.tight_layout()
        return fig


def fig_staleness(results: list[dict]):
    import matplotlib.pyplot as plt
    import pandas as pd

    from tufte import (
        INK,
        PALETTE,
        direct_label,
        offset_spines,
        tight_x,
        tight_y,
        title_only,
        tufte_rc,
    )

    df = pd.DataFrame(
        [
            {
                "sources": len(r["scenario"]["workload"]["sources"]),
                "p50": r["headline"]["p50_staleness_millis"] or 0,
                "p95": r["headline"]["p95_staleness_millis"] or 0,
            }
            for r in results
        ]
    ).sort_values("sources")

    with tufte_rc():
        fig, ax = plt.subplots(figsize=(6.6, 3.0))
        ax.plot(df["sources"], df["p50"], marker="o", color=INK)
        ax.plot(df["sources"], df["p95"], marker="s", color=PALETTE[2])
        last = df.iloc[-1]
        # At k = 8 the two lines often coincide (both 100 ms in our
        # current data), so labels at identical offsets would stack
        # into one illegible glyph. Place p95 ABOVE the marker (red,
        # higher percentile) and p50 BELOW it.
        direct_label(
            ax,
            last["sources"],
            last["p50"],
            "p50",
            color=INK,
            xytext=(14, -12),
        )
        direct_label(
            ax,
            last["sources"],
            last["p95"],
            "p95",
            color=PALETTE[2],
            xytext=(14, 12),
        )
        ax.set_xlabel("concurrent write sources")
        ax.set_ylabel("per-hit lag (ms)")
        title_only(ax, "per-hit delivery delay under sustained writes")
        offset_spines(ax)
        # Stretch x just enough that the labels at the right edge have
        # room to render without being clipped by the figure border.
        tight_x(ax, list(df["sources"]) + [df["sources"].max() + 0.5])
        tight_y(ax, list(df["p50"]) + list(df["p95"]))
        fig.tight_layout()
        return fig


PLOTTERS = {
    "convergence": fig_convergence,
    "fanout_sweep": fig_fanout_sweep,
    "scale_n": fig_scale_n,
    "loss": fig_loss,
    "partition": fig_partition,
    "staleness": fig_staleness,
}


# ----- data fragment for Typst ----------------------------------------------


def _hms_to_seconds(spec: str) -> float:
    spec = spec.strip()
    if spec.endswith("ms"):
        return float(spec[:-2]) / 1000.0
    if spec.endswith("s"):
        return float(spec[:-1])
    return float(spec)


def emit_typst_data(summary: dict[str, list[dict]]) -> str:
    """Write a small data fragment so the report Typst source can stay
    declarative."""
    lines: list[str] = ["// generated by bench/render.py — do not edit"]

    # Convergence table: rows are N, columns are fanout.
    conv_rows = []
    fanouts = sorted({r["scenario"]["fanout"] for r in summary.get("convergence", [])})
    nodes = sorted({r["scenario"]["nodes"] for r in summary.get("convergence", [])})
    by_nf = {
        (r["scenario"]["nodes"], r["scenario"]["fanout"]): r["headline"]["convergence_rounds"]
        for r in summary.get("convergence", [])
    }
    for n in nodes:
        row = [str(n)]
        for f in fanouts:
            v = by_nf.get((n, f))
            row.append(f"{v:.0f}" if v is not None else "—")
        conv_rows.append(row)
    lines.append(f"#let convergence_fanouts = ({', '.join(str(f) for f in fanouts)},)")
    lines.append(f"#let convergence_rows = ({_typst_array_of_arrays(conv_rows)})")

    # Loss table.
    loss_rows = []
    by_loss = {}
    for r in summary.get("loss", []):
        loss = r["scenario"]["network"]["uniform_loss"]
        h = r["headline"]
        by_loss.setdefault(loss, []).append((h["convergence_rounds"], h["final_divergence"]))
    for loss in sorted(by_loss):
        rounds = [v[0] for v in by_loss[loss] if v[0] is not None]
        rounds.sort()
        median = rounds[len(rounds) // 2] if rounds else None
        divs = sorted(v[1] for v in by_loss[loss])
        div_median = divs[len(divs) // 2]
        total = len(by_loss[loss])
        conv_count = sum(1 for v in by_loss[loss] if v[0] is not None)
        loss_rows.append(
            [
                f"{int(loss * 100)}%",
                f"{median:.0f}" if median is not None else "—",
                str(div_median),
                f"{conv_count}/{total}",
            ]
        )
    lines.append(f"#let loss_rows = ({_typst_array_of_arrays(loss_rows)})")

    # Bandwidth-vs-N (fanout=3) row used in the headline-numbers table.
    bw_rows = []
    for n in sorted({r["scenario"]["nodes"] for r in summary.get("convergence", [])}):
        for r in summary["convergence"]:
            s = r["scenario"]
            if s["nodes"] == n and s["fanout"] == 3:
                bw_rows.append([str(n), f"{r['headline']['bytes_per_node_per_second']:.0f}"])
                break
    lines.append(f"#let bandwidth_rows = ({_typst_array_of_arrays(bw_rows)})")

    # Partition headline.
    partition = summary.get("partition", [])
    if partition:
        reconv = partition[0]["headline"]["extras"].get(
            "reconvergence_millis_after_heal"
        )
        lines.append(
            f"#let partition_reconv_ms = {reconv if reconv is not None else 'none'}"
        )
    else:
        lines.append("#let partition_reconv_ms = none")

    # Scale_n table: N, rounds, wall-clock ms, bytes/node/s.
    scale_rows: list[list[str]] = []
    for r in sorted(
        summary.get("scale_n", []), key=lambda r: r["scenario"]["nodes"]
    ):
        s, h = r["scenario"], r["headline"]
        scale_rows.append(
            [
                str(s["nodes"]),
                f"{h['convergence_rounds']:.0f}" if h["convergence_rounds"] is not None else "—",
                f"{h['convergence_millis']}" if h["convergence_millis"] is not None else "—",
                f"{h['bytes_per_node_per_second']:.0f}",
            ]
        )
    lines.append(f"#let scale_rows = ({_typst_array_of_arrays(scale_rows)})")

    # Headline numbers at N = 1024 (referenced inline in the report).
    big = next(
        (r for r in summary.get("scale_n", []) if r["scenario"]["nodes"] == 1024),
        None,
    )
    if big is not None:
        h = big["headline"]
        rounds = h["convergence_rounds"]
        ms = h["convergence_millis"]
        bw = h["bytes_per_node_per_second"]
        lines.append(
            f'#let n1024_rounds = "{rounds:.0f}"' if rounds is not None else '#let n1024_rounds = "—"'
        )
        lines.append(
            f'#let n1024_ms = "{ms}"' if ms is not None else '#let n1024_ms = "—"'
        )
        lines.append(f'#let n1024_bytes_per_s = "{bw:.0f}"')
    else:
        lines.append('#let n1024_rounds = "—"')
        lines.append('#let n1024_ms = "—"')
        lines.append('#let n1024_bytes_per_s = "—"')

    return "\n".join(lines) + "\n"


def _typst_array_of_arrays(rows: list[list[str]]) -> str:
    out_rows = []
    for r in rows:
        out_rows.append("(" + ", ".join(f'"{c}"' for c in r) + ",)")
    return "(" + ", ".join(out_rows) + ",)"


# ----- pipeline -------------------------------------------------------------


def regenerate() -> None:
    plot_script = REPO_ROOT / "crates" / "gossip-bench" / "bench" / "plot.py"
    subprocess.run([sys.executable, str(plot_script), "all"], check=True)


def load_summary(source: Path) -> dict[str, list[dict]]:
    summary: dict[str, list[dict]] = {}
    for suite in SUITES:
        path = source / suite / "results.jsonl"
        if not path.exists():
            continue
        with path.open() as f:
            summary[suite] = [json.loads(l) for l in f if l.strip()]
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=TARGET_ROOT)
    parser.add_argument("--out", type=Path, default=FIG_DIR)
    parser.add_argument("--data", type=Path, default=DATA_TYP)
    parser.add_argument("--regenerate", action="store_true")
    args = parser.parse_args()

    _ensure_plot_libs()
    sys.path.insert(0, str(Path(__file__).parent))

    if args.regenerate:
        regenerate()

    summary = load_summary(args.source)
    args.out.mkdir(parents=True, exist_ok=True)

    import matplotlib.pyplot as plt

    for suite, builder in PLOTTERS.items():
        if suite not in summary:
            continue
        fig = builder(summary[suite])
        target = args.out / f"{suite}.svg"
        fig.savefig(target, format="svg", bbox_inches="tight")
        plt.close(fig)
        print(f"wrote {target.relative_to(REPO_ROOT)}")

    args.data.write_text(emit_typst_data(summary))
    print(f"wrote {args.data.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
