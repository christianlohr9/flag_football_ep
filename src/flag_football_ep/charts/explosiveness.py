"""Headless chart renderers for the Explosiveness/Efficiency comparison (HC-04, Phase M3-3).

Rendering the report page is M3-4's job, not this phase's -- but the DATA these two
figures show (the cliff-zone table, the four-definition comparison) is measured HERE,
in `scripts/explosiveness_comparison.py`, from `flag_football_ep.features.explosiveness`.
Without this module, M3-4 would either recompute the same numbers a second time or
import this phase's script directly; neither is desirable. `docs/` holds zero image
files today (every German document in this repo is plain Markdown), so
`docs/explosiveness-vorschlag.md` shows the cliff-zone distribution as a Markdown table
with text bars instead of embedding a figure -- these renderers exist so the HTML
handout M3-4 builds can show the identical measured data as real matplotlib Figures,
through `reports.render.fig_to_data_uri`, the only sanctioned path from a Figure into a
report (`charts/fourth_down.py`'s convention, copied exactly here).

Both renderers follow `charts/fourth_down.py`'s headless discipline: `matplotlib.use
("Agg")` is selected INSIDE the function, before `pyplot` is imported, so this module
never requires a display; each function returns a `Figure` and never shows or saves it.
No `write_*` PNG helper exists here -- `fourth_down.py` has one for a standalone-print
use case this phase has no equivalent of, and the repo tracks zero PNGs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from flag_football_ep.features.explosiveness import DEFINITIONS, HC_EXPLOSIVE_YARDS_THRESHOLD

if TYPE_CHECKING:
    from matplotlib.figure import Figure

_ACCENT = "#1f4e79"
_MUTED = "#999999"

_NO_DATA_LABEL = "keine Daten"


def _empty_figure(title: str) -> "Figure":
    """A `keine Daten` placeholder Figure -- returned, never raised, on empty input
    (matches `charts/tendency.py::_empty_placeholder`'s survive-one-empty-section
    discipline).
    """
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 3))
    ax.text(0.5, 0.5, _NO_DATA_LABEL, ha="center", va="center", fontsize=14)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(title)
    fig.tight_layout()
    return fig


def render_cliff_zone(
    cliff_table: pl.DataFrame, *, threshold: int = HC_EXPLOSIVE_YARDS_THRESHOLD
) -> "Figure":
    """Render the per-yard cliff-zone distribution around `threshold`, without showing
    or saving it.

    One bar per `yards_gained` row in `cliff_table` (already sorted ascending), a
    dashed vertical rule drawn at the midpoint between `threshold` and `threshold + 1`
    -- the head coach's own cutoff, made visible as a line rather than merely implied
    by two adjacent bars -- and a German x-axis label naming Yards. The three bars at
    `threshold - 2`, `threshold - 1` and `threshold` (10, 11, 12 at the default
    threshold) each carry the SAME annotation: their combined share of all scrimmage
    plays, so a reader can see the cliff zone's total weight off any one of the three
    bars without having to sum bar heights themselves.

    An empty `cliff_table` (`height == 0`) returns a `keine Daten` placeholder Figure
    with no bars and raises nothing.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mtick

    title = "Klippen-Zone: Anteil der Plays je Yard"

    if cliff_table.height == 0:
        return _empty_figure(title)

    sorted_table = cliff_table.sort("yards_gained")
    yards = sorted_table["yards_gained"].to_list()
    shares = [s if s is not None else 0.0 for s in sorted_table["share"].to_list()]

    fig, ax = plt.subplots(figsize=(8, 5))
    x_positions = list(range(len(yards)))
    ax.bar(x_positions, shares, color=_ACCENT)
    ax.set_xticks(x_positions)
    ax.set_xticklabels([str(y) for y in yards])
    ax.set_xlabel("Yards (Raumgewinn)")
    ax.set_ylabel("Anteil an allen Scrimmage-Plays")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1))

    if threshold in yards and (threshold + 1) in yards:
        low_x = x_positions[yards.index(threshold)]
        high_x = x_positions[yards.index(threshold + 1)]
        ax.axvline((low_x + high_x) / 2, color="black", linestyle="--", linewidth=1.5)

    cliff_zone_yards = [threshold - 2, threshold - 1, threshold]
    cliff_indices = [yards.index(y) for y in cliff_zone_yards if y in yards]
    if cliff_indices:
        combined_share = sum(shares[i] for i in cliff_indices)
        annotation_text = (
            f"Klippen-Zone {threshold - 2}-{threshold}: {combined_share:.1%}"
        )
        for i in cliff_indices:
            ax.annotate(
                annotation_text,
                xy=(x_positions[i], shares[i]),
                xytext=(0, 8),
                textcoords="offset points",
                ha="center",
                fontsize=8,
            )

    ax.set_title(title)
    fig.tight_layout()
    return fig


def render_definition_comparison(comparison_frame: pl.DataFrame) -> "Figure":
    """Render one horizontal bar per metric definition, without showing or saving it.

    Rows are ordered by `DEFINITIONS` order (never by the input frame's own row order,
    which `definition_comparison`'s multi-group concatenation does not guarantee); a
    `definition` value absent from `DEFINITIONS` is dropped rather than plotted at an
    undefined position. Each bar is labelled with `label_de`, carries asymmetric error
    bars built from `ci_low`/`ci_high`, and is annotated with its `n`. Rows with
    `muted == True` are drawn in grey and still annotated with their `n` -- muted, never
    dropped, matching the project's standing convention (`charts/tendency.py`). A row
    with a null `rate` (zero in-scope plays for that definition) is plotted at 0 and
    still annotated with `n=0`, never silently omitted.

    An empty `comparison_frame` returns a `keine Daten` placeholder Figure and raises
    nothing.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mtick

    title = "Definitionsvergleich: Explosiveness / Efficiency"

    if comparison_frame.height == 0:
        return _empty_figure(title)

    order = {definition.key: i for i, definition in enumerate(DEFINITIONS)}
    ordered = (
        comparison_frame.filter(pl.col("definition").is_in(list(order)))
        .with_columns(
            _order=pl.col("definition").replace_strict(
                order, default=len(order), return_dtype=pl.Int64
            )
        )
        .sort("_order")
    )

    if ordered.height == 0:
        return _empty_figure(title)

    labels = ordered["label_de"].to_list()
    rates = ordered["rate"].to_list()
    ci_lows = ordered["ci_low"].to_list()
    ci_highs = ordered["ci_high"].to_list()
    ns = ordered["n"].to_list()
    muted = ordered["muted"].to_list()

    n_bars = len(labels)
    fig, ax = plt.subplots(figsize=(8, 0.6 * n_bars + 1.5))

    y_positions = list(range(n_bars))
    colors = [_MUTED if m else _ACCENT for m in muted]
    values = [r if r is not None else 0.0 for r in rates]
    bars = ax.barh(y_positions, values, color=colors)
    ax.set_yticks(y_positions)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1))

    for bar, rate, lo, hi, n in zip(bars, rates, ci_lows, ci_highs, ns):
        y = bar.get_y() + bar.get_height() / 2
        if rate is not None and lo is not None and hi is not None:
            lower_err = rate - lo
            upper_err = hi - rate
            ax.errorbar(
                rate,
                y,
                xerr=[[lower_err], [upper_err]],
                fmt="none",
                ecolor="black",
                capsize=4,
            )
        x = rate if rate is not None else 0.0
        ax.annotate(
            f"n={n}",
            xy=(x, y),
            xytext=(4, 0),
            textcoords="offset points",
            va="center",
            ha="left",
            fontsize=8,
        )

    ax.set_title(title)
    fig.tight_layout()
    return fig
