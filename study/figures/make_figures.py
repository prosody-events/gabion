#!/usr/bin/env python3
"""Figures and the verdict table for study/report.md.

Reads only the data snapshot in study/data/ (posterior.json, self_posterior.json,
metrics.json, regimes.json, arms.json) and writes PNGs plus verdicts.csv next to
this file. Every estimate is drawn with its 95 % equal-tailed credible interval
as reported by the harness judge (crates/gossip-bench/judge/judge.py).

    python3 study/figures/make_figures.py
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import FixedLocator, FuncFormatter, NullFormatter  # noqa: E402

HERE = Path(__file__).resolve().parent
DATA = HERE.parent / "data"

POST = json.loads((DATA / "posterior.json").read_text())
SELF = json.loads((DATA / "self_posterior.json").read_text())
METRICS = json.loads((DATA / "metrics.json").read_text())
ARMS = json.loads((DATA / "arms.json").read_text())

IDX = {(r["solution"], r["regime"], r["metric"]): r for r in POST}
SIDX = {(r["comparison"], r["solution"], r["regime"], r["metric"]): r for r in SELF}
ERROR_METRICS = [m["metric"] for m in METRICS if m["error"]]
HEALTH_METRICS = ["drop_rcvbuf_pkts", "send_pending_max"]
REGIMES = ["low-3", "low-5", "idle", "prod-typical", "prod-peak", "prod-4x", "loss-5",
           "loss-20", "loss-burst", "hot-key", "churn-rollout", "az-partition", "long-window"]
DESIGNS = [a["arm"] for a in ARMS if a["improved"]]  # main + 10 designs, in arms.json order
FAMILY = {a["arm"]: a["family"] for a in ARMS}

# Okabe-Ito hues, one per design family (validated for CVD separation with the
# dataviz palette checker); marker shape is the secondary encoding per design.
FAMILY_COLOUR = {
    "Baseline": "#333333",
    "Flat epidemic": "#E69F00",
    "Trees": "#0072B2",
    "Direct mail": "#009E73",
    "Reconciliation": "#D55E00",
    "Aggregation": "#CC79A7",
}
MARKER = {
    "main": "*", "flat-once": "o", "gossipsub": "s", "plumtree": "o", "thicket": "s",
    "snow": "^", "direct-ack": "o", "bimodal": "s", "az-hier": "^", "scuttlebutt": "D",
    "owner-agg": "P",
}
INK = "#222222"
MUTED = "#777777"

plt.rcParams.update({
    "font.size": 8,
    "axes.titlesize": 8.5,
    "axes.labelsize": 8,
    "legend.fontsize": 7,
    "xtick.labelsize": 7,
    "ytick.labelsize": 7,
    "axes.edgecolor": MUTED,
    "axes.linewidth": 0.6,
    "xtick.color": INK,
    "ytick.color": INK,
    "axes.labelcolor": INK,
    "text.color": INK,
    "axes.grid": True,
    "grid.color": "#e4e4e4",
    "grid.linewidth": 0.5,
    "figure.dpi": 200,
    "savefig.bbox": "tight",
})


def nopk(arm: str) -> str:
    a = next(x for x in ARMS if x["arm"] == arm)
    return a["improved_nopk"] or a["improved"]


# ---------------------------------------------------------------------------
# Verdict classification (report §4.6). P is the judge's posterior probability
# that the solution is better (mean log ratio, or mean difference, below 0).
# better: P >= 0.95 and ratio < 0.95; worse: P <= 0.05 and ratio > 1.05;
# tie: whole 95 % CrI inside [0.95, 1.05]; "credible <5 %": P beyond a threshold
# but the estimate inside the band; otherwise unsettled. On the difference scale
# the band is ±5 % of the comparator's posterior median; two all-zero samples tie.
# ---------------------------------------------------------------------------

def verdict(row: dict | None, comparator_mean: float | None) -> str | None:
    if row is None or row.get("scale") is None:
        return None
    p = row["p_better"]
    if row["scale"] == "log":
        est, lo, hi = row["ratio"], row["ratio_lo"], row["ratio_hi"]
        if lo is None:
            return "unsettled"
        if p is not None and p >= 0.95 and est < 0.95:
            return "better"
        if p is not None and p <= 0.05 and est > 1.05:
            return "worse"
        if lo >= 0.95 and hi <= 1.05:
            return "tie"
    else:
        est, lo, hi = row["diff_mean"], row["diff_lo"], row["diff_hi"]
        band = 0.05 * abs(comparator_mean) if comparator_mean else 0.0
        if lo is None:
            return "unsettled"
        if est == 0 and band == 0:
            return "tie"
        if p is not None and p >= 0.95 and est < -band:
            return "better"
        if p is not None and p <= 0.05 and est > band:
            return "worse"
        if band > 0 and lo >= -band and hi <= band:
            return "tie"
    if p is not None and (p >= 0.95 or p <= 0.05):
        return "credible <5 %"
    return "unsettled"


def write_verdicts() -> None:
    """verdicts.csv: per (comparison, solution, against, metric group) the count
    of cells in each verdict class over the 13 regimes."""
    jobs = []
    for sol in sorted({r["solution"] for r in POST}):
        if sol != "main":
            jobs.append(("vs_main", sol, "main"))
    for comp, sol, against in sorted({(r["comparison"], r["solution"], r["against"]) for r in SELF}):
        jobs.append((comp, sol, against))
    classes = ["better", "worse", "tie", "credible <5 %", "unsettled"]
    with open(HERE / "verdicts.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["comparison", "solution", "against", "group", *classes])
        for comp, sol, against in jobs:
            for group, mets in (("error", ERROR_METRICS), ("health", HEALTH_METRICS)):
                counts = dict.fromkeys(classes, 0)
                for reg in REGIMES:
                    for m in mets:
                        row = IDX.get((sol, reg, m)) if comp == "vs_main" else SIDX.get((comp, sol, reg, m))
                        base = IDX.get((against, reg, m))
                        v = verdict(row, base["mean"] if base else None)
                        if v:
                            counts[v] += 1
                w.writerow([comp, sol, against, group, *[counts[c] for c in classes]])


# ---------------------------------------------------------------------------
# Figure 1: bytes against limited-key staleness p99, per regime.
# ---------------------------------------------------------------------------

def err(lo, mid, hi, floor):
    lo = max(lo, floor)
    return [[max(mid - lo, 0.0)], [max(hi - mid, 0.0)]]


def fig_tradeoff() -> None:
    panels = [("prod-typical", "Scale"), ("prod-peak", "Scale"), ("prod-4x", "Scale"),
              ("loss-20", "Faults"), ("churn-rollout", "Dynamics"), ("az-partition", "Faults")]
    fig, axes = plt.subplots(2, 3, figsize=(7.2, 5.0))
    floor = 0.3
    for ax, (reg, group) in zip(axes.flat, panels):
        for arm in DESIGNS:
            colour = FAMILY_COLOUR[FAMILY[arm]]
            for sol, filled in ((arm, False), (nopk(arm), True)):
                if arm == "main" and filled:
                    sol = "imp-main-nopk"
                b = IDX[(sol, reg, "bytes_total")]
                s = IDX[(sol, reg, "stale_limited_p99_pct")]
                y = max(s["mean"], floor)
                ax.errorbar(
                    b["mean"], y,
                    xerr=err(b["cri_lo"], b["mean"], b["cri_hi"], 1.0),
                    yerr=err(s["cri_lo"], y, s["cri_hi"], floor),
                    fmt=MARKER[arm], ms=7 if MARKER[arm] in "*P" else 5,
                    mfc=colour if filled else "white", mec=colour, mew=1.1,
                    ecolor=colour, elinewidth=0.8, capsize=0, zorder=3 if filled else 2,
                )
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_title(f"{reg}  ({group})")
        ax.set_xlabel("bytes, measurement window")
        ax.set_ylabel("staleness p99, % of limit")
    handles = [Line2D([], [], ls="", marker=MARKER[a], mfc=FAMILY_COLOUR[FAMILY[a]],
                      mec=FAMILY_COLOUR[FAMILY[a]], ms=6, label=a) for a in DESIGNS]
    handles += [Line2D([], [], ls="", marker="o", mfc="white", mec=INK, label="baseline (hollow)"),
                Line2D([], [], ls="", marker="o", mfc=INK, mec=INK,
                       label="improved, no per-key (filled)")]
    fig.legend(handles=handles, loc="lower center", ncol=5, frameon=False,
               bbox_to_anchor=(0.5, -0.09))
    fig.suptitle("Bytes against limited-key staleness p99 (posterior median, 95 % CrI; lower whiskers clipped at 0.3 %; log-log). "
                 "Lower-left is better.", fontsize=8.5)
    fig.tight_layout()
    fig.savefig(HERE / "fig1_tradeoff.png")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Forest plots.
# ---------------------------------------------------------------------------

def ratio_ticks(ax) -> None:
    """Readable ticks on a log ratio axis: a few round values, no minor labels."""
    lo, hi = ax.get_xlim()
    nice = [0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20]
    if hi / lo > 50:
        nice = [0.001, 0.01, 0.1, 1, 10]
    inside = [v for v in nice if lo <= v <= hi]
    if len(inside) < 3:
        inside = [v for v in [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.2, 1.5] if lo <= v <= hi][::2] or inside
    ax.xaxis.set_major_locator(FixedLocator(inside))
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.xaxis.set_minor_formatter(NullFormatter())


def forest(ax, rows, title, xlim=None):
    """rows: list of (label, row-or-None, colour, marker, y). Ratio scale only."""
    ax.axvspan(0.95, 1.05, color="#d9d9d9", alpha=0.6, lw=0, zorder=0)
    ax.axvline(1.0, color=MUTED, lw=0.8, zorder=1)
    for label, r, colour, marker, y in rows:
        if r is None or r.get("scale") != "log" or r["ratio_lo"] is None:
            ax.text(1.0, y, "Δ scale", fontsize=6, color=MUTED, va="center", ha="center")
            continue
        ax.errorbar(r["ratio"], y, xerr=[[r["ratio"] - r["ratio_lo"]], [r["ratio_hi"] - r["ratio"]]],
                    fmt=marker, ms=4.5, mfc=colour, mec=colour, ecolor=colour,
                    elinewidth=1.0, capsize=0, zorder=3)
    ax.set_xscale("log")
    if xlim:
        ax.set_xlim(*xlim)
    ax.set_title(title)
    ax.grid(axis="y", visible=False)
    ratio_ticks(ax)


def fig_own_baseline() -> None:
    metrics = [("bytes_total", "bytes"), ("conv_p99_incl_never_ms", "convergence p99 (incl. never)"),
               ("stale_limited_p99_pct", "staleness p99")]
    regimes = [("prod-typical", "o", 0.22), ("loss-20", "s", 0.0), ("churn-rollout", "^", -0.22)]
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 4.4), sharey=True)
    for ax, (m, mlabel) in zip(axes, metrics):
        rows = []
        for i, arm in enumerate(DESIGNS):
            sol = nopk(arm)
            colour = FAMILY_COLOUR[FAMILY[arm]]
            for reg, marker, off in regimes:
                r = SIDX.get(("vs_own_baseline", sol, reg, m))
                rows.append((sol, r, colour, marker, len(DESIGNS) - 1 - i + off))
        forest(ax, rows, mlabel)
        ax.set_xlabel("ratio to own baseline (log)")
    axes[0].set_yticks(range(len(DESIGNS)))
    axes[0].set_yticklabels([nopk(a) for a in reversed(DESIGNS)])
    handles = [Line2D([], [], ls="", marker=mk, mfc=INK, mec=INK, label=reg) for reg, mk, _ in regimes]
    fig.legend(handles=handles, loc="lower center", ncol=3, frameon=False, bbox_to_anchor=(0.5, -0.05))
    fig.suptitle("Improved variant (no per-key fix) against its own baseline: geometric-mean ratio, "
                 "95 % CrI. Grey band: ±5 % tie band. Left of 1 is better.", fontsize=8.5)
    fig.tight_layout()
    fig.savefig(HERE / "fig2_own_baseline.png")
    plt.close(fig)


def fig_perkey() -> None:
    panels = [("bytes_total", "prod-typical", "bytes\nprod-typical"),
              ("bytes_total", "prod-peak", "bytes\nprod-peak"),
              ("conv_p50_incl_never_ms", "prod-typical", "convergence p50\nprod-typical"),
              ("stale_limited_p99_pct", "prod-typical", "staleness p99\nprod-typical"),
              ("over_max_pct", "prod-typical", "worst-key over-admission\nprod-typical"),
              ("conv_p50_incl_never_ms", "low-3", "convergence p50\nlow-3")]
    arms = [a for a in DESIGNS if next(x for x in ARMS if x["arm"] == a)["improved_nopk"]]
    fig, grid = plt.subplots(2, 3, figsize=(7.2, 5.6), sharey=True)
    axes = list(grid.flat)
    for ax, (m, reg, title) in zip(axes, panels):
        rows = []
        for i, arm in enumerate(arms):
            sol = next(x for x in ARMS if x["arm"] == arm)["improved"]
            r = SIDX.get(("perkey_effect", sol, reg, m))
            rows.append((sol, r, FAMILY_COLOUR[FAMILY[arm]], MARKER[arm] if MARKER[arm] != "*" else "o",
                         len(arms) - 1 - i))
        forest(ax, rows, title)
    for ax in (axes[0], axes[3]):
        ax.set_yticks(range(len(arms)))
        ax.set_yticklabels(list(reversed(arms)))
    fig.supxlabel("ratio, with per-key / without per-key (log)", fontsize=8)
    fig.suptitle("Effect of the per-key threshold fix alone (imp-<arm> against imp-<arm>-nopk): "
                 "geometric-mean ratio, 95 % CrI, log scale. Grey: ±5 % tie band.", fontsize=8.5)
    fig.tight_layout()
    fig.savefig(HERE / "fig3_perkey.png")
    plt.close(fig)


def fig_bytes_by_regime() -> None:
    """Bytes relative to baseline main for the no-per-key tips, every regime."""
    show = ["imp-main-nopk", "imp-flat-once-nopk", "imp-plumtree-nopk", "imp-direct-ack-nopk",
            "imp-bimodal-nopk", "imp-az-hier-nopk", "imp-scuttlebutt-nopk", "imp-owner-agg"]
    arm_of = {nopk(a): a for a in DESIGNS}
    arm_of["imp-main-nopk"] = "main"
    fig, ax = plt.subplots(figsize=(7.2, 3.4))
    n = len(show)
    for j, sol in enumerate(show):
        arm = arm_of[sol]
        colour = FAMILY_COLOUR[FAMILY[arm]]
        xs, ys, lo, hi = [], [], [], []
        for i, reg in enumerate(REGIMES):
            r = IDX[(sol, reg, "bytes_total")]
            xs.append(i + (j - (n - 1) / 2) * 0.09)
            ys.append(r["ratio"])
            lo.append(r["ratio"] - r["ratio_lo"])
            hi.append(r["ratio_hi"] - r["ratio"])
        ax.errorbar(xs, ys, yerr=[lo, hi], fmt=MARKER[arm] if arm != "main" else "*", ms=5,
                    mfc=colour, mec=colour, ecolor=colour, lw=0, elinewidth=0.8, label=sol)
    ax.axhline(1.0, color=MUTED, lw=0.8)
    ax.set_yscale("log")
    ax.set_xticks(range(len(REGIMES)))
    ax.set_xticklabels(REGIMES, rotation=35, ha="right")
    ax.set_ylabel("bytes / baseline main (log)")
    ax.legend(ncol=4, frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.32))
    ax.set_title("Bytes of the improved tips relative to baseline main, every regime "
                 "(geometric-mean ratio, 95 % CrI)")
    fig.tight_layout()
    fig.savefig(HERE / "fig4_bytes_by_regime.png")
    plt.close(fig)


def write_fronts() -> None:
    """fronts.csv: the non-dominated solutions on (bytes, error metric), by
    posterior median, within the baseline set and within the improved set (no
    per-key tips plus imp-owner-agg), per regime. Every seed is valid for every
    solution, so the means rest on the same three workloads."""
    sets = {
        "baseline": DESIGNS,
        "improved": [("imp-main-nopk" if a == "main" else nopk(a)) for a in DESIGNS],
    }
    with open(HERE / "fronts.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["set", "regime", "metric", "solution", "bytes_total", "value"])
        for name, sols in sets.items():
            for m in ("stale_limited_p99_pct", "over_max_pct", "conv_p99_incl_never_ms"):
                for reg in REGIMES:
                    pts = [(s, IDX[(s, reg, "bytes_total")]["mean"], IDX[(s, reg, m)]["mean"])
                           for s in sols if IDX.get((s, reg, m)) and IDX[(s, reg, m)]["mean"] is not None]
                    front = [p for p in pts if not any(
                        q[1] <= p[1] and q[2] <= p[2] and (q[1] < p[1] or q[2] < p[2]) for q in pts)]
                    for s, b, v in sorted(front, key=lambda p: p[1]):
                        w.writerow([name, reg, m, s, f"{b:.6g}", f"{v:.6g}"])


if __name__ == "__main__":
    write_verdicts()
    write_fronts()
    fig_tradeoff()
    fig_own_baseline()
    fig_perkey()
    fig_bytes_by_regime()
    print("wrote", ", ".join(sorted(p.name for p in HERE.iterdir() if p.suffix in (".png", ".csv"))))
