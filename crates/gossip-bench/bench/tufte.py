"""Tufte-leaning matplotlib styling.

The rules:
- No top/right spines; remaining spines pulled slightly off the data
  ("offset" style, à la `theme_tufte` in ggplot).
- Light, sparse gridlines only on the major axis values that matter.
- Direct labels on lines where feasible; legend boxes are visual noise.
- Range bracketed by data — no decorative padding on the axes.
- Serif typeface for labels; axes ticks point outward.

Use:
    fig, ax = plt.subplots(figsize=(...))
    apply_tufte_style(ax)
    # plot...
    finalise(ax)  # last-pass: tighten ranges, direct-label lines
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable

import matplotlib as mpl
import matplotlib.pyplot as plt

SERIF = ["New Computer Modern", "Linux Libertine", "Times New Roman", "Times"]

INK = "#202020"
SUBTLE = "#9a9a9a"
ACCENT = "#8a1a1f"  # Bringhurst-ish quiet red for emphasis
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


@contextmanager
def tufte_rc():
    """Enter a matplotlib rc context with Tufte defaults set."""
    rc = {
        "font.family": "serif",
        "font.serif": SERIF,
        "font.size": 10,
        "axes.titlesize": 10.5,
        "axes.titleweight": "regular",
        "axes.labelsize": 9.5,
        "axes.labelcolor": INK,
        "axes.edgecolor": INK,
        "axes.linewidth": 0.5,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": False,        # data-ink rule: gridlines off by default.
        "xtick.direction": "out",
        "ytick.direction": "out",
        "xtick.color": INK,
        "ytick.color": INK,
        "xtick.labelsize": 8.5,
        "ytick.labelsize": 8.5,
        "xtick.major.size": 3,
        "ytick.major.size": 3,
        "xtick.major.width": 0.5,
        "ytick.major.width": 0.5,
        "legend.frameon": False,
        "legend.fontsize": 8.5,
        "legend.handlelength": 1.6,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "axes.prop_cycle": mpl.cycler(color=PALETTE),
        "lines.linewidth": 1.1,
        "lines.markersize": 3.5,
        "savefig.facecolor": "white",
        "svg.fonttype": "none",
    }
    with mpl.rc_context(rc):
        yield


def range_frame(ax, x_values, y_values) -> None:
    """Tufte's range-frame: replace the spines with line segments that
    span only the data's actual range, not the full axis. After this
    runs, the axes look like two small scale bars at the left and
    bottom of the plot rather than a complete frame."""
    xs = [v for v in x_values if v is not None]
    ys = [v for v in y_values if v is not None]
    if not xs or not ys:
        return
    ax.spines["bottom"].set_bounds(min(xs), max(xs))
    ax.spines["left"].set_bounds(min(ys), max(ys))


def offset_spines(ax, offset: float = 6.0) -> None:
    """Pull the left and bottom spines slightly off the data so the axes
    read as scale bars, not a frame (per Tufte / Wilke)."""
    for side in ("left", "bottom"):
        ax.spines[side].set_position(("outward", offset))
        ax.spines[side].set_visible(True)


def tight_x(ax, x_values: Iterable[float], pad_frac: float = 0.02) -> None:
    x = [v for v in x_values if v is not None]
    if not x:
        return
    lo, hi = min(x), max(x)
    if lo == hi:
        return
    span = hi - lo
    ax.set_xlim(lo - span * pad_frac, hi + span * pad_frac)


def tight_y(ax, y_values: Iterable[float], pad_frac: float = 0.04) -> None:
    y = [v for v in y_values if v is not None]
    if not y:
        return
    lo, hi = min(y), max(y)
    if lo == hi:
        hi = lo + 1
    span = hi - lo
    ax.set_ylim(lo - span * pad_frac, hi + span * pad_frac)


def direct_label(ax, x: float, y: float, text: str, **kwargs) -> None:
    """Place a text label directly adjacent to a line / data point in
    place of a legend entry."""
    defaults = dict(
        fontsize=8.5,
        color=INK,
        ha="left",
        va="center",
        xytext=(4, 0),
        textcoords="offset points",
    )
    defaults.update(kwargs)
    ax.annotate(text, (x, y), **defaults)


def title_only(ax, text: str) -> None:
    ax.set_title(text, loc="left", color=INK, pad=8)
