"""Shared aggregation vocabulary for Phase 1.4's coaching reports (REQ-S1-12).

This module owns the aggregation vocabulary shared by the opponent tendency report
(plan 08) and the own-team efficiency report (plan 09): field-zone and score-state
bucketing, honest rate cells with sample size and a Clopper-Pearson confidence
interval, share tables, the Hudl-charted-only filter, and each report section's data
basis. Every bucket boundary and every threshold defined here is quoted verbatim in
the rendered reports' footnotes, so changing one changes what the coaching staff
reads. Implementing this once here keeps plans 08 and 09 from each inventing their
own bucket boundaries and their own definition of "thin sample".
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

# CONTEXT ("Tendency report content"): locks "3 zones plus an explicit red zone" over
# yardline_50 (yards to the opponent goal line). The red zone (0-9) is the locked explicit
# bucket; the remaining [10, 50] range is split into three equal-width thirds (~14 yards each)
# -- CONTEXT delegates the exact thirds split to discretion, this is that discretion call.
# Bounds are inclusive; the tuple order is the display order every report renders in.
FIELD_ZONES: tuple[tuple[str, int, int], ...] = (
    ("Red Zone", 0, 9),
    ("Gegnerhälfte", 10, 22),
    ("Mittelfeld", 23, 36),
    ("Eigene Hälfte", 37, 50),
)

# CONTEXT ("Tendency report content"): locks "three score buckets by margin" from
# score_differential (posteam - defteam). ±6 is "within one score" in flag football (a
# touchdown is worth 6 points plus a 1- or 2-point PAT), which is the locked close-game
# definition -- a margin of exactly 6 is still "Ausgeglichen", not yet "Führung"/"Rückstand".
SCORE_STATES: tuple[str, ...] = ("Rückstand", "Ausgeglichen", "Führung")

# CONTEXT ("every rate a report prints carries its play count... a rate computed from a thin
# sample is marked as thin -- it is shown, never hidden"): CONTEXT delegates the exact
# threshold to discretion. 5 is the smallest n at which a Clopper-Pearson interval still says
# something more useful than "could be anything" while staying conservative for this corpus's
# generally small per-cell counts (tens-to-low-hundreds of plays per game).
MUTED_MIN_N = 5

# The same distance boundaries `estimate_fourth_down_rates` (features/mutations.py) uses via
# FOURTH_DOWN_DISTANCE_BUCKETS, so the tendency tables and the 4th-down decision chart speak
# the same distance language. Order matters: it is display order.
DISTANCE_BUCKETS: tuple[tuple[str, int, int | None], ...] = (
    ("Short 1-3", 1, 3),
    ("Medium 4-6", 4, 6),
    ("Long 7-10", 7, 10),
    ("XL 11+", 11, None),
)

# Columns only populated for Hudl-charted games; the graceful-degradation mechanism for
# CONTEXT's locked all-sources data scope. Formation/route/coverage sections restrict to these
# non-null rows via `charted_only`; everything else in a report uses the full corpus.
CHARTED_COLUMNS: tuple[str, ...] = (
    "off_form",
    "off_play",
    "target_route",
    "def_front",
    "coverage",
)


def _field_zone_expr() -> pl.Expr:
    """Chained `pl.when(...).then(...).otherwise(...)` mapping `yardline_50` to a
    `FIELD_ZONES` label. Null or out-of-range `yardline_50` matches none of the chained
    conditions and falls through to `otherwise(None)` -- never folded into a wrong bucket.
    """
    expr: pl.Expr | None = None
    for label, low, high in FIELD_ZONES:
        condition = (pl.col("yardline_50") >= low) & (pl.col("yardline_50") <= high)
        expr = (
            pl.when(condition).then(pl.lit(label))
            if expr is None
            else expr.when(condition).then(pl.lit(label))
        )
    assert expr is not None  # FIELD_ZONES is never empty
    return expr.otherwise(pl.lit(None, dtype=pl.Utf8))


def _score_state_expr() -> pl.Expr:
    """Chained bucketing of `score_differential` into `SCORE_STATES`. Null differential
    falls through to `otherwise(None)`.
    """
    return (
        pl.when(pl.col("score_differential").is_null())
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(pl.col("score_differential") < -6)
        .then(pl.lit("Rückstand"))
        .when(pl.col("score_differential") > 6)
        .then(pl.lit("Führung"))
        .otherwise(pl.lit("Ausgeglichen"))
    )


def _distance_bucket_expr() -> pl.Expr:
    """Chained `pl.when(...).then(...).otherwise(...)` mapping `yards_to_go` to a
    `DISTANCE_BUCKETS` label, built from the tuple itself so the boundaries have one source of
    truth (mirrors `features/mutations.py`'s `_fourth_down_distance_bucket_expr`).
    `yards_to_go < 1`, or null, falls through to `otherwise(None)`.
    """
    expr: pl.Expr | None = None
    for label, min_distance, max_distance in DISTANCE_BUCKETS:
        condition = pl.col("yards_to_go") >= min_distance
        if max_distance is not None:
            condition = condition & (pl.col("yards_to_go") <= max_distance)
        expr = (
            pl.when(condition).then(pl.lit(label))
            if expr is None
            else expr.when(condition).then(pl.lit(label))
        )
    assert expr is not None  # DISTANCE_BUCKETS is never empty
    return expr.otherwise(pl.lit(None, dtype=pl.Utf8))


def add_report_buckets(plays: pl.DataFrame) -> pl.DataFrame:
    """Add `field_zone`, `score_state` and `distance_bucket` Utf8 columns.

    Pure: returns a new frame, adds only these three columns, preserves row count and row
    order, and never mutates the input frame.

    - `field_zone` from `yardline_50` using `FIELD_ZONES`'s inclusive bounds; a null or
      out-of-range `yardline_50` yields null, never a wrong bucket.
    - `score_state` from `score_differential`: `< -6` -> `"Rückstand"`, `-6..6` inclusive ->
      `"Ausgeglichen"`, `> 6` -> `"Führung"`; null differential yields null. ±6 is "within one
      score" in flag football (TD 6 + PAT), the locked close-game definition.
    - `distance_bucket` from `yards_to_go` using `DISTANCE_BUCKETS`; `yards_to_go < 1` or null
      yields null.
    """
    return plays.with_columns(
        field_zone=_field_zone_expr(),
        score_state=_score_state_expr(),
        distance_bucket=_distance_bucket_expr(),
    )


@dataclass(frozen=True)
class SectionBasis:
    """A report section's data basis: which games, which sources, how many plays.

    `text` is the German one-liner a template prints verbatim, e.g.
    "Datenbasis: 4 Spiele (hudl, ifaf), 218 Plays".
    """

    games: tuple[str, ...]
    sources: tuple[str, ...]
    n_plays: int
    text: str


def section_basis(df: pl.DataFrame) -> SectionBasis:
    """Build a `SectionBasis` from a frame's `game_id`/`source` columns.

    Distinct sorted `game_id` values, distinct sorted `source` values, `df.height`, and a
    German one-line `text`. Uses singular `1 Spiel` when there is exactly one game. An empty
    frame returns `SectionBasis((), (), 0, "Datenbasis: keine Daten")` and raises nothing.
    """
    if df.height == 0:
        return SectionBasis((), (), 0, "Datenbasis: keine Daten")

    games = tuple(sorted(df["game_id"].drop_nulls().unique().to_list()))
    sources = tuple(sorted(df["source"].drop_nulls().unique().to_list()))
    n_plays = df.height

    game_word = "Spiel" if len(games) == 1 else "Spiele"
    sources_str = ", ".join(sources)
    text = f"Datenbasis: {len(games)} {game_word} ({sources_str}), {n_plays} Plays"

    return SectionBasis(games=games, sources=sources, n_plays=n_plays, text=text)


def charted_only(plays: pl.DataFrame, column: str) -> pl.DataFrame:
    """Filter to rows where `column` is non-null.

    The graceful-degradation mechanism for CONTEXT's locked all-sources data scope:
    formation/route/coverage sections restrict to Hudl-charted rows via this filter,
    everything else in a report uses the full corpus. Raises `ValueError` naming `column`
    when it is not one of `CHARTED_COLUMNS`, so a typo cannot silently return an empty
    section instead of failing loudly.
    """
    if column not in CHARTED_COLUMNS:
        raise ValueError(
            f"charted_only: {column!r} is not a charted column; expected one of "
            f"{CHARTED_COLUMNS}"
        )
    return plays.filter(pl.col(column).is_not_null())
