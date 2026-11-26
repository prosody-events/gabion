"""The study's verdict on one paired comparison.

`row` is one `summary.csv` row of the judge with its numbers parsed: `scale`
(`log` or `diff`), `p_better` (the posterior probability the solution is
better), and the ratio or difference with its credible interval. better: P >=
0.95 and the estimate below the band; worse: P <= 0.05 and above it; tie: the
whole interval inside the band; "credible <5 %": P beyond a threshold but the
estimate inside the band; otherwise unsettled. The first rule that matches
wins, in that order. The band is ratio [0.95, 1.05], or on the difference scale
±5 % of the comparator's posterior median; two all-zero samples tie.
"""

from __future__ import annotations


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
        if est == 0 and band == 0:
            return "tie"
        if lo is None:
            return "unsettled"
        if p is not None and p >= 0.95 and est < -band:
            return "better"
        if p is not None and p <= 0.05 and est > band:
            return "worse"
        if band > 0 and lo >= -band and hi <= band:
            return "tie"
    if p is not None and (p >= 0.95 or p <= 0.05):
        return "credible <5 %"
    return "unsettled"
