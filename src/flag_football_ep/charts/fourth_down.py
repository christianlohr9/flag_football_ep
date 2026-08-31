"""4th-down conversion chart: conversion rate against distance-to-go, with attempt counts
and Clopper-Pearson intervals visible on the chart itself (REQ-S1-14).

Answers "wie oft geht 4th down auf welche Distanz durch?" (docs/plan-1-analytics-refresh.md
§Phase 1.4) using `flag_football_ep.features.mutations.estimate_fourth_down_rates`'s per-
distance-bucket rates. `render_fourth_down` sets the `Agg` backend before importing `pyplot`
so this module never requires a display, matching `charts/pat_breakeven.py`'s headless-figure
convention. The report pipeline embeds `render_fourth_down`'s returned Figure through
`reports.render.fig_to_data_uri` rather than calling `write_fourth_down`, which exists for the
standalone-PNG use case (a coach printing a single decision chart).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from flag_football_ep.features.mutations import FOURTH_DOWN_DISTANCE_BUCKETS, FourthDownRates

if TYPE_CHECKING:
    from matplotlib.figure import Figure

_NO_DATA_LABEL = "keine Daten"


def render_fourth_down(rates: FourthDownRates, title: str | None = None) -> "Figure":
    """Render the 4th-down conversion chart from `rates`, without showing or saving it.

    One bar per `FOURTH_DOWN_DISTANCE_BUCKETS` bucket, x tick labels the bucket labels, y the
    conversion rate on a fixed `0..1` axis. Each populated bucket's bar carries asymmetric
    error bars from its Clopper-Pearson `ci` (`rate - ci[0]`, `ci[1] - rate`) and is annotated
    above with its attempt count (`n=12`) -- the interval must be visible, that is the whole
    point of the honest small-sample estimate. A bucket with `attempts == 0` draws no bar and
    is annotated "keine Daten" at y=0, so an empty bucket reads as an explicit gap and never as
    a 0% conversion rate. A horizontal dashed reference line at `rates.overall_rate`, labelled
    with the overall rate and total attempts, is drawn only when `overall_rate` is not None.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = [label for label, _, _ in FOURTH_DOWN_DISTANCE_BUCKETS]
    x_positions = list(range(len(labels)))

    fig, ax = plt.subplots(figsize=(8, 5))

    for x, bucket in zip(x_positions, rates.buckets):
        if bucket.attempts == 0 or bucket.rate is None:
            ax.annotate(
                _NO_DATA_LABEL,
                xy=(x, 0),
                xytext=(x, 0.03),
                ha="center",
            )
            continue

        lower_err = bucket.rate - bucket.ci[0] if bucket.ci is not None else 0.0
        upper_err = bucket.ci[1] - bucket.rate if bucket.ci is not None else 0.0
        ax.bar(x, bucket.rate, color="tab:blue")
        ax.errorbar(
            x,
            bucket.rate,
            yerr=[[lower_err], [upper_err]],
            fmt="none",
            ecolor="black",
            capsize=4,
        )
        ax.annotate(
            f"n={bucket.attempts}",
            xy=(x, bucket.rate),
            xytext=(0, 8),
            textcoords="offset points",
            ha="center",
        )

    if rates.overall_rate is not None:
        ax.axhline(
            rates.overall_rate,
            color="gray",
            linestyle="--",
            label=(
                f"overall rate {rates.overall_rate:.3f} (n={rates.total_attempts})"
            ),
        )
        ax.legend()

    ax.set_xticks(x_positions)
    ax.set_xticklabels(labels)
    ax.set_ylim(0, 1)
    ax.set_xlabel("Distance (yards to go)")
    ax.set_ylabel("Conversion rate")
    ax.set_title(title or "4th down: Conversion rate nach Distance")
    fig.tight_layout()
    return fig


def write_fourth_down(rates: FourthDownRates, out_path: Path, overwrite: bool = False) -> Path:
    """Render and write the 4th-down conversion chart as a PNG at `out_path`.

    Refuses to overwrite an existing file unless `overwrite` is True (mirrors
    `write_pat_breakeven`'s write-once guard), so an accidental rerun cannot silently replace a
    chart already shared with the coaching staff.
    """
    out_path = Path(out_path)
    if out_path.exists() and not overwrite:
        raise FileExistsError(
            f"refusing to overwrite existing 4th-down chart: {out_path}"
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig = render_fourth_down(rates)
    try:
        fig.savefig(out_path)
    finally:
        import matplotlib.pyplot as plt

        plt.close(fig)
    return out_path
