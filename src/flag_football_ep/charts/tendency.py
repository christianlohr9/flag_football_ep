"""Generic bar renderers shared by the opponent tendency report and the own-team efficiency
report (REQ-S1-12, REQ-S1-13).

`render_share_bars` and `render_epa_bars` take an aggregation frame produced by
`reports.aggregate` (a label column, a value column, an `n` column, and a `muted` column) and
know nothing about which report is calling them -- both the opponent and own-team report pages
embed the resulting Figures directly. Each render function sets the `Agg` backend inside
itself, before importing `pyplot`, matching `charts/pat_breakeven.py`'s headless convention;
neither function ever displays the figure interactively or saves a file -- the caller decides
whether/how to persist the returned Figure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

# Single report accent colour (matches the base template's one-accent "clean neutral" style).
_ACCENT = "#1f4e79"
# Contrasting second tone for negative EPA bars -- distinct from both the accent and muted grey.
_NEGATIVE = "#c1440e"
# Thin-sample rows below the report's muting threshold; muting outranks sign or magnitude.
_MUTED = "#999999"

_MUTED_NOTE = "grau = geringe Stichprobe (unter dem Muting-Schwellenwert)"


def _figsize(n_bars: int) -> tuple[float, float]:
    """Figure size for the report's full-width `.chart-img` styling: wide enough that a
    3-row chart is not stretched, tall enough that a `max_bars`-row chart stays legible."""
    return (8.0, 0.45 * max(n_bars, 1) + 1.5)


def _empty_placeholder(title: str) -> "Figure":
    """An empty aggregation frame renders a centred `keine Daten` placeholder and returns
    normally -- a batch run must survive one empty report section rather than raising."""
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 2))
    ax.text(0.5, 0.5, "keine Daten", ha="center", va="center", fontsize=14)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(title)
    fig.tight_layout()
    return fig


def _annotate_n(ax: "Axes", y: float, value: float, n: int) -> None:
    """Annotate a bar with its play count in the form `n=K`, placed just past the bar end --
    on the right for a non-negative value, on the left for a negative one."""
    if value >= 0:
        ax.annotate(
            f"n={n}",
            xy=(value, y),
            xytext=(4, 0),
            textcoords="offset points",
            va="center",
            ha="left",
            fontsize=8,
        )
    else:
        ax.annotate(
            f"n={n}",
            xy=(value, y),
            xytext=(-4, 0),
            textcoords="offset points",
            va="center",
            ha="right",
            fontsize=8,
        )


def _draw_muted_note(ax: "Axes") -> None:
    """Axes note explaining what grey means -- a reader must never have to guess."""
    ax.text(
        0.0, -0.12, _MUTED_NOTE, transform=ax.transAxes, ha="left", fontsize=8, color=_MUTED
    )


def _draw_omission_note(ax: "Axes", notes: list[str]) -> None:
    """Axes note naming any rows silently truncated from the chart (top-`max_bars` cutoff,
    or null-valued rows dropped rather than plotted as zero)."""
    if notes:
        ax.text(
            1.0,
            -0.12,
            "; ".join(notes),
            transform=ax.transAxes,
            ha="right",
            fontsize=8,
        )


def render_share_bars(
    table: pl.DataFrame,
    *,
    label_col: str,
    share_col: str = "share",
    n_col: str = "n",
    muted_col: str = "muted",
    title: str,
    max_bars: int = 12,
) -> "Figure":
    """Render a share distribution as a horizontal bar chart, without showing or saving it.

    Rows are sorted by `share_col` descending (largest share at the top); when the frame has
    more than `max_bars` rows only the top `max_bars` are kept and the omitted count is
    annotated on the axes. The x axis is fixed to `(0, 1)` and formatted as percentages. Rows
    whose `muted_col` is True are drawn in grey -- an axes note explains that grey means a
    thin sample below the report's muting threshold. Every bar is annotated with its `n_col`
    value. An empty `table` renders a `keine Daten` placeholder and returns normally.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mtick

    if table.height == 0:
        return _empty_placeholder(title)

    sorted_table = table.sort(share_col, descending=True)
    omitted = max(sorted_table.height - max_bars, 0)
    plotted = sorted_table.head(max_bars)

    labels = plotted[label_col].to_list()
    shares = plotted[share_col].to_list()
    ns = plotted[n_col].to_list()
    muted = plotted[muted_col].to_list()

    n_bars = len(labels)
    fig, ax = plt.subplots(figsize=_figsize(n_bars))

    y_positions = list(range(n_bars))
    colors = [_MUTED if m else _ACCENT for m in muted]
    bars = ax.barh(y_positions, shares, color=colors)
    ax.set_yticks(y_positions)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()  # largest share at the top
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1))

    for bar, n in zip(bars, ns):
        y = bar.get_y() + bar.get_height() / 2
        _annotate_n(ax, y, bar.get_width(), n)

    _draw_muted_note(ax)

    omission_notes = []
    if omitted:
        omission_notes.append(f"{omitted} weitere Kategorien nicht angezeigt")
    _draw_omission_note(ax, omission_notes)

    ax.set_title(title)
    fig.tight_layout()
    return fig


def render_epa_bars(
    table: pl.DataFrame,
    *,
    label_col: str,
    epa_col: str,
    n_col: str,
    muted_col: str = "muted",
    title: str,
    max_bars: int = 12,
) -> "Figure":
    """Render a signed EPA-per-play rollup as a horizontal bar chart around a zero baseline,
    without showing or saving it.

    Rows with a null `epa_col` are dropped from the chart (never plotted as zero) and their
    count is reported in an axes note. Remaining rows are sorted by `epa_col` descending
    (best at the top); when more than `max_bars` remain, only the top `max_bars` are kept and
    the omitted count is annotated. X limits are symmetric around zero from the data's maximum
    absolute value, with a solid reference line at `x = 0`. Positive bars use the report accent
    colour, negative bars a contrasting tone, and muted rows stay grey regardless of sign --
    muting outranks sign because a thin sample is the more important caveat. An empty `table`
    renders a `keine Daten` placeholder and returns normally.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if table.height == 0:
        return _empty_placeholder(title)

    null_dropped = table[epa_col].null_count()
    non_null = table.filter(pl.col(epa_col).is_not_null())

    if non_null.height == 0:
        fig = _empty_placeholder(title)
        ax = fig.axes[0]
        if null_dropped:
            ax.text(
                0.5,
                0.2,
                f"{null_dropped} Zeilen ohne EPA-Wert nicht angezeigt",
                ha="center",
                fontsize=8,
            )
        return fig

    sorted_table = non_null.sort(epa_col, descending=True)
    omitted = max(sorted_table.height - max_bars, 0)
    plotted = sorted_table.head(max_bars)

    labels = plotted[label_col].to_list()
    values = plotted[epa_col].to_list()
    ns = plotted[n_col].to_list()
    muted = plotted[muted_col].to_list()

    n_bars = len(labels)
    fig, ax = plt.subplots(figsize=_figsize(n_bars))

    y_positions = list(range(n_bars))
    colors = []
    for value, m in zip(values, muted):
        if m:
            colors.append(_MUTED)
        elif value >= 0:
            colors.append(_ACCENT)
        else:
            colors.append(_NEGATIVE)

    bars = ax.barh(y_positions, values, color=colors)
    ax.set_yticks(y_positions)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()  # best EPA at the top

    max_abs = max(abs(v) for v in values)
    margin = max_abs * 0.15 + 1e-6
    ax.set_xlim(-(max_abs + margin), max_abs + margin)
    ax.axvline(0, color="black", linewidth=1)

    for bar, n, value in zip(bars, ns, values):
        y = bar.get_y() + bar.get_height() / 2
        _annotate_n(ax, y, value, n)

    ax.set_xlabel("EPA/Play")
    _draw_muted_note(ax)

    omission_notes = []
    if omitted:
        omission_notes.append(f"{omitted} weitere Kategorien nicht angezeigt")
    if null_dropped:
        omission_notes.append(f"{null_dropped} Zeilen ohne EPA-Wert nicht angezeigt")
    _draw_omission_note(ax, omission_notes)

    ax.set_title(title)
    fig.tight_layout()
    return fig
