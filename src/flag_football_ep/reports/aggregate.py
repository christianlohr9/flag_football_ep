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
from typing import Sequence

import polars as pl
from scipy.stats import binomtest

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


@dataclass(frozen=True)
class ReportSection:
    """One rendered block of a tendency report: heading, table, data basis, empty notice.

    `empty_notice` is a German sentence set when `table` has nothing meaningful to show
    (CONTEXT's locked "explicit no-data block rather than failing" behaviour, resolved
    as a per-section notice) and `None` otherwise. Shared by the opponent and own-team
    report builders.
    """

    key: str
    heading: str
    table: pl.DataFrame
    basis: SectionBasis
    empty_notice: str | None


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


@dataclass(frozen=True)
class RateCell:
    """A single rate observation with its sample size and confidence interval.

    `muted` is `n < MUTED_MIN_N` (CONTEXT: "cells under a threshold are visually muted, never
    hidden"). The consumer's job is to render muted cells greyed, never to drop them.
    """

    label: str
    n: int
    successes: int
    rate: float | None
    ci: tuple[float, float] | None
    muted: bool

    @property
    def text(self) -> str:
        """German display string: rounded percentage plus the count in parentheses, e.g.
        `78% Pass (n=9)`. Returns `keine Daten` when `n == 0`.
        """
        if self.n == 0 or self.rate is None:
            return "keine Daten"
        pct = round(self.rate * 100)
        return f"{pct}% {self.label} (n={self.n})"


def rate_table(
    df: pl.DataFrame,
    group_cols: Sequence[str],
    success: pl.Expr,
    *,
    label_col: str = "label",
) -> pl.DataFrame:
    """One row per `group_cols` group with `n`, `successes`, `rate`, `ci_low`/`ci_high`
    (Clopper-Pearson) and `muted` (`n < MUTED_MIN_N`). Also adds a `label_col` column (default
    `"label"`) holding the group's display label -- `group_cols` values joined with `" / "` --
    so a caller can build `RateCell(label=row[label_col], ...)` directly from each output row.

    The aggregation stays in polars (`pl.len()` for `n`, the boolean `success` expression's
    true count for `successes`); the Clopper-Pearson interval is computed per row in a small
    Python loop over the already-tiny aggregated frame -- only the scipy call is per row.
    `rate` and both CI bounds are null when `n == 0`. Rows are sorted by `group_cols` so table
    output is deterministic across runs. Returns an empty frame with the full declared schema
    (not a bare empty frame) when `df` is empty, and raises nothing.
    """
    group_cols = list(group_cols)
    schema = {col: pl.Utf8 for col in group_cols}
    schema[label_col] = pl.Utf8
    schema["n"] = pl.Int64
    schema["successes"] = pl.Int64
    schema["rate"] = pl.Float64
    schema["ci_low"] = pl.Float64
    schema["ci_high"] = pl.Float64
    schema["muted"] = pl.Boolean

    if df.height == 0:
        return pl.DataFrame(schema=schema)

    grouped = (
        df.with_columns(success.cast(pl.Int32).alias("__success__"))
        .group_by(group_cols, maintain_order=True)
        .agg(
            n=pl.len(),
            successes=pl.col("__success__").sum(),
        )
        .sort(group_cols)
        .with_columns(
            n=pl.col("n").cast(pl.Int64),
            successes=pl.col("successes").cast(pl.Int64),
        )
    )

    rows = grouped.to_dicts()
    rates: list[float | None] = []
    ci_lows: list[float | None] = []
    ci_highs: list[float | None] = []
    muted: list[bool] = []
    for row in rows:
        n = int(row["n"])
        successes = int(row["successes"])
        if n == 0:
            rates.append(None)
            ci_lows.append(None)
            ci_highs.append(None)
        else:
            rates.append(successes / n)
            ci = binomtest(successes, n).proportion_ci()
            ci_lows.append(ci.low)
            ci_highs.append(ci.high)
        muted.append(n < MUTED_MIN_N)

    result = grouped.with_columns(
        pl.Series("rate", rates, dtype=pl.Float64),
        pl.Series("ci_low", ci_lows, dtype=pl.Float64),
        pl.Series("ci_high", ci_highs, dtype=pl.Float64),
        pl.Series("muted", muted, dtype=pl.Boolean),
        pl.concat_str(
            [pl.col(col).cast(pl.Utf8) for col in group_cols], separator=" / "
        ).alias(label_col),
    )
    return result


def share_table(
    df: pl.DataFrame,
    group_cols: Sequence[str],
    category_col: str,
) -> pl.DataFrame:
    """The distribution of `category_col`'s values within each `group_cols` group.

    One row per (group, category) pair with `n` (rows in that category), `group_n` (rows in
    the group), `share = n / group_n`, Clopper-Pearson `ci_low`/`ci_high` for that share, and
    `muted = group_n < MUTED_MIN_N`. Null `category_col` values are excluded from the
    numerator but reported as their own `"unbekannt"` category row rather than silently
    vanishing, so a distribution always sums to 1 within a group. Sorted by `group_cols` then
    `share` descending, so the dominant tendency is the first row a template renders. Returns
    an empty frame with the full declared schema when `df` is empty, and raises nothing.
    """
    group_cols = list(group_cols)
    schema = {col: pl.Utf8 for col in group_cols}
    schema[category_col] = pl.Utf8
    schema["n"] = pl.Int64
    schema["group_n"] = pl.Int64
    schema["share"] = pl.Float64
    schema["ci_low"] = pl.Float64
    schema["ci_high"] = pl.Float64
    schema["muted"] = pl.Boolean

    if df.height == 0:
        return pl.DataFrame(schema=schema)

    filled = df.with_columns(
        pl.col(category_col).cast(pl.Utf8).fill_null(pl.lit("unbekannt")).alias(category_col)
    )

    group_n_df = filled.group_by(group_cols, maintain_order=True).agg(group_n=pl.len())

    counts = (
        filled.group_by([*group_cols, category_col], maintain_order=True)
        .agg(n=pl.len())
        # join_nulls: real corpora carry null group keys (e.g. `down` on un-charted
        # rows); a default join would drop their group_n and crash the share math
        .join(group_n_df, on=group_cols, how="left", join_nulls=True)
        .with_columns(
            n=pl.col("n").cast(pl.Int64),
            group_n=pl.col("group_n").cast(pl.Int64),
        )
    )

    rows = counts.to_dicts()
    shares: list[float] = []
    ci_lows: list[float] = []
    ci_highs: list[float] = []
    muted: list[bool] = []
    for row in rows:
        n = int(row["n"])
        group_n = int(row["group_n"])
        shares.append(n / group_n)
        ci = binomtest(n, group_n).proportion_ci()
        ci_lows.append(ci.low)
        ci_highs.append(ci.high)
        muted.append(group_n < MUTED_MIN_N)

    result = counts.with_columns(
        pl.Series("share", shares, dtype=pl.Float64),
        pl.Series("ci_low", ci_lows, dtype=pl.Float64),
        pl.Series("ci_high", ci_highs, dtype=pl.Float64),
        pl.Series("muted", muted, dtype=pl.Boolean),
    ).sort([*group_cols, "share"], descending=[False] * len(group_cols) + [True])

    return result
