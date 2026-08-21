"""Per-game win-probability review chart (REQ-S1-15): the home-team WP curve across a game,
annotated with the plays that actually moved it, and an unmissable synthetic-clock
disclosure.

This is the film-session artifact -- "wo ist das Spiel gekippt?". `render_wp_review` sets
the `Agg` backend inside the render function, before importing `pyplot`, matching
`charts/pat_breakeven.py`'s headless-figure convention. CONTEXT's "top-k |EPA| swings"
discretion is resolved here concretely as top-k `|wpa|` plus every scoring play and every
turnover -- see `select_wp_annotations`.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from matplotlib.figure import Figure

# More than eight captions on a single WP curve stops being readable on a tablet during a
# film session -- this is a hard cap independent of how many scoring/turnover plays a game
# has.
WP_ANNOTATION_TOP_K = 5
WP_ANNOTATION_MAX = 8

_EVENT_COLUMNS = (
    "touchdown",
    "interception",
    "one_point_conv_success",
    "two_point_conv_success",
    "safety",
)

_EVENT_TAGS = {
    "touchdown": "TD",
    "interception": "INT",
    "one_point_conv_success": "PAT",
    "two_point_conv_success": "2PT",
    "safety": "SAFETY",
}


def _annotation_label(row: dict) -> str:
    """Build one annotation caption, e.g. `P23 TD GER +0.18`, kept under ~24 characters."""
    tag = "Swing"
    for col in _EVENT_COLUMNS:
        if row[col] == 1:
            tag = _EVENT_TAGS[col]
            break
    wpa = row["wpa"] if row["wpa"] is not None else 0.0
    return f"P{row['play_id']} {tag} {row['posteam']} {wpa:+.2f}"


def select_wp_annotations(
    game_plays: pl.DataFrame,
    *,
    top_k: int = WP_ANNOTATION_TOP_K,
    max_markers: int = WP_ANNOTATION_MAX,
) -> pl.DataFrame:
    """Select which plays get an annotation marker on the WP review curve.

    Candidate set: the `top_k` rows with the largest `|wpa|` (nulls excluded), unioned with
    every scoring play (`touchdown`, `one_point_conv_success`, `two_point_conv_success`) or
    turnover (`interception`, `safety`) row. De-duplicated on `play_id`. When the union
    exceeds `max_markers`, scoring/turnover rows are kept ahead of swing-only rows (ranked by
    `|wpa|` descending within each group), so a big pure swing can never bump a scoring play
    off the chart. Result is sorted by `play_id` ascending with an added `annotation_label`
    Utf8 column.

    Returns an empty frame with the same schema (plus `annotation_label`) when `game_plays`
    is empty or every `wpa` is null -- never raises.
    """
    if game_plays.height == 0:
        return game_plays.with_columns(annotation_label=pl.lit(None, dtype=pl.Utf8))

    is_event = pl.any_horizontal([pl.col(c) == 1 for c in _EVENT_COLUMNS])

    non_null = game_plays.filter(pl.col("wpa").is_not_null())
    if non_null.height == 0:
        return game_plays.with_columns(
            annotation_label=pl.lit(None, dtype=pl.Utf8)
        ).head(0)

    top_swing = (
        non_null.with_columns(_abs_wpa=pl.col("wpa").abs())
        .sort("_abs_wpa", descending=True)
        .head(top_k)
        .drop("_abs_wpa")
    )
    event_rows = game_plays.filter(is_event)

    candidates = pl.concat([top_swing, event_rows]).unique(
        subset="play_id", keep="first"
    )
    candidates = candidates.with_columns(
        _is_event=is_event,
        _abs_wpa=pl.col("wpa").abs().fill_null(0.0),
    )

    if candidates.height > max_markers:
        candidates = candidates.sort(
            ["_is_event", "_abs_wpa"], descending=[True, True]
        ).head(max_markers)

    candidates = candidates.sort("play_id").drop(["_is_event", "_abs_wpa"])

    labels = [
        _annotation_label(row)
        for row in candidates.select(
            "play_id", "posteam", "wpa", *_EVENT_COLUMNS
        ).iter_rows(named=True)
    ]
    return candidates.with_columns(annotation_label=pl.Series(labels, dtype=pl.Utf8))


def render_wp_review(
    game_plays: pl.DataFrame,
    *,
    game_label: str,
    synthetic_clock: bool = True,
    top_k: int = WP_ANNOTATION_TOP_K,
) -> "Figure":
    """Render REQ-S1-15's per-game WP review chart: the home-team WP curve across a game,
    annotated with the plays that actually moved it, and an unmissable synthetic-clock
    disclosure when `synthetic_clock` is True.

    `synthetic_clock` defaults to True because `features.mutations.prepare_wp_data`'s only
    production path is `real_clock=False` -- the per-play clock is interpolated, not real.
    When True, the disclosure appears in two independent places: the title suffix and an
    in-axes `Hinweis` note, so the flag survives being cropped or embedded under a separate
    heading.

    A game with fewer than two non-null `home_wp` rows renders an empty axes with a centred
    `keine WP-Daten für dieses Spiel` placeholder instead of raising.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 5))

    non_null_wp = game_plays.filter(pl.col("home_wp").is_not_null())
    if non_null_wp.height < 2:
        ax.text(
            0.5,
            0.5,
            "keine WP-Daten für dieses Spiel",
            ha="center",
            va="center",
            transform=ax.transAxes,
        )
        ax.set_axis_off()
        fig.tight_layout()
        return fig

    home_team = game_plays["home_team"][0]

    ax.plot(
        non_null_wp["play_id"], non_null_wp["home_wp"], color="tab:blue", linewidth=1.5
    )
    ax.axhline(0.5, color="gray", linestyle="--")
    ax.axhspan(0.5, 1.0, color="tab:blue", alpha=0.05)
    ax.set_ylim(0, 1)

    annotations = select_wp_annotations(game_plays, top_k=top_k)
    if annotations.height > 0:
        ax.plot(
            annotations["play_id"],
            annotations["home_wp"],
            marker="o",
            linestyle="none",
            color="tab:orange",
        )
        for i, row in enumerate(annotations.iter_rows(named=True)):
            offset = 15 if i % 2 == 0 else -20
            ax.annotate(
                row["annotation_label"],
                xy=(row["play_id"], row["home_wp"]),
                xytext=(0, offset),
                textcoords="offset points",
                ha="center",
                fontsize="x-small",
            )

    x_label = "Play # (synthetische Spielzeit)" if synthetic_clock else "Play #"
    ax.set_xlabel(x_label)
    ax.set_ylabel(f"Win Probability {home_team}")

    title = f"WP Review — {game_label}"
    if synthetic_clock:
        title += " (synthetische Spielzeit)"
    ax.set_title(title)

    if synthetic_clock:
        ax.text(
            0.02,
            0.02,
            "Hinweis: synthetische Spielzeit — Uhr pro Play interpoliert, "
            "keine echten Clock-Daten",
            transform=ax.transAxes,
            fontsize="x-small",
            ha="left",
            va="bottom",
        )

    fig.tight_layout()
    return fig


def write_wp_review(
    game_plays: pl.DataFrame,
    out_path: Path,
    *,
    game_label: str,
    synthetic_clock: bool = True,
    overwrite: bool = False,
) -> Path:
    """Render and write the WP review chart as a PNG at `out_path`.

    Refuses to overwrite an existing file unless `overwrite` is True (mirrors
    `write_pat_breakeven`'s write-once guard), so an accidental rerun cannot silently
    replace a chart already shared with the coaching staff.
    """
    out_path = Path(out_path)
    if out_path.exists() and not overwrite:
        raise FileExistsError(
            f"refusing to overwrite existing WP review chart: {out_path}"
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig = render_wp_review(
        game_plays, game_label=game_label, synthetic_clock=synthetic_clock
    )
    try:
        fig.savefig(out_path)
    finally:
        import matplotlib.pyplot as plt

        plt.close(fig)
    return out_path
