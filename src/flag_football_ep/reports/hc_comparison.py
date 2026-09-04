"""Puts the head coach's own "Scoring Probability by Situation" method and our model on one
axis so they can be read against each other (HC-03, EPA-D03, October 2026 sync).

This module reproduces his empirical `SP by D&D`/`EP by D&D` tables (M3-02-RESEARCH section
4.2) -- down x distance-to-go x field-half, with n beside every probability -- from HIS rows
now in our canonical corpus and from the rest of the corpus. It deliberately does NOT
reproduce his `Reg` tab (M3-02-RESEARCH section 4.4): `data_only=False` inspection of that
tab's formula cells found hardcoded polynomials of rising degree per down and field half --
the signature of hand-transcribed Excel chart-trendline equations, not a systematically fit
model. Reproducing that would be reproducing exactly the kind of ad-hoc, per-cell method his
own pitch ("professioneller und nachhaltiger gestalten", M3-02-CONTEXT) is asking to move
past. His `Reg` values are quoted verbatim from `data/reference/hc_sp_tables/reg_formulas.csv`
in the German write-up (M3-02-07); this module never recomputes them.

Every function here is pure: no file reads, no config, no MLflow. `scripts/epa_comparison.py`
owns loading `data/reference/hc_sp_tables/*.csv`, `data/processed/plays.parquet` and
`data/processed/oof_predictions_ep.parquet`, and wiring their output into the functions below.
"""

from __future__ import annotations

import polars as pl

from flag_football_ep.reports.aggregate import rate_table

# Distinct from MUTED_MIN_N (reports/aggregate.py, currently 5): MUTED_MIN_N says "too thin
# to trust at all"; THIN_MIN_N says "trustworthy but visibly thinner than its counterpart".
# This comparison's whole point is the gap between a cell at n=13 and one at n=324 -- his own
# own-half cells at distance >= 10 sit in the low teens next to opposite-half cells for the
# same (down, distance) in the hundreds (M3-02-RESEARCH section 4.2). A cell at n=20 is
# muted == False (>= MUTED_MIN_N) but thin == True (< THIN_MIN_N): confidently non-empty,
# still visibly thinner than a n=300 cell next to it.
THIN_MIN_N = 30

# yardline_50 is yards from the offense's OWN goal line, integer, 0..50 inclusive; midfield
# is 25 (docs/data-contract.md line 82, validation/checks.py::yardline_range). A play
# starting exactly at midfield is "opponent", not "own" -- an explicit boundary choice, not
# an accident of `<` vs `<=`.
MIDFIELD_YARDLINE = 25

# His own uncluttered SP/EP-by-D&D axis: distance-to-go 1..14 individually, then one open
# "15+" bin (M3-02-RESEARCH section 4.2; data/reference/hc_sp_tables/sp_by_dd.csv reads
# distance 1..34 verbatim, but his own axis groups everything past 14 into the open bin for
# display -- the clustered tabs make this explicit with their own separate bin edges).
MAX_INDIVIDUAL_DISTANCE = 14

# The join key every function in this module and `comparison_table` shares.
COMPARISON_KEYS: tuple[str, ...] = ("down", "distance_bin", "field_half")


class MissingComparisonColumns(ValueError):
    """Raised when a required column is missing from a comparison input frame.

    Mirrors `flag_football_ep.features.mutations.MissingFeatureColumns`: names every
    missing column instead of letting a downstream group_by/join fail with an opaque
    polars ColumnNotFound.
    """


def field_half_expr() -> pl.Expr:
    """`yardline_50 < MIDFIELD_YARDLINE` -> `"own"`; `>= MIDFIELD_YARDLINE` -> `"opponent"`.

    A play starting exactly at midfield (`yardline_50 == 25`) is `"opponent"` -- the
    boundary is inclusive on the opponent side, matching how the head coach's own
    own-half/opposite-half split reads (own half is strictly nearer his own goal line).
    Null `yardline_50` yields null, never a guessed default half.
    """
    return (
        pl.when(pl.col("yardline_50").is_null())
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(pl.col("yardline_50") < MIDFIELD_YARDLINE)
        .then(pl.lit("own"))
        .otherwise(pl.lit("opponent"))
    )


def distance_bin_expr(max_individual: int = MAX_INDIVIDUAL_DISTANCE) -> pl.Expr:
    """`yards_to_go` 1..`max_individual` -> that integer as a string; above it -> an open
    `"{max_individual + 1}+"` bin.

    `yards_to_go <= 0` (and null) yield null rather than falling into bin 1 by default --
    zero or negative yards-to-go is not a distance still to go, and silently bucketing it
    with a genuine 1-yard-to-go play would understate that cell's n dishonestly.
    """
    return (
        pl.when(pl.col("yards_to_go").is_null())
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(pl.col("yards_to_go") <= 0)
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(pl.col("yards_to_go") <= max_individual)
        .then(pl.col("yards_to_go").cast(pl.Utf8))
        .otherwise(pl.lit(f"{max_individual + 1}+"))
    )


_EMPIRICAL_SP_REQUIRED_COLUMNS: tuple[str, ...] = (
    "down",
    "yards_to_go",
    "yardline_50",
    "Next_Score_Half",
)


def empirical_sp(prepared: pl.DataFrame, *, success_label: str = "Touchdown") -> pl.DataFrame:
    """One row per `(down, distance_bin, field_half)` empirical scoring probability.

    `prepared` is a `flag_football_ep.features.mutations.prepare_ep_data` output (or any
    frame carrying its `Next_Score_Half` column) -- pass whichever subset of it the caller
    wants (his rows only, the rest of the corpus, or the pooled frame); this function does
    not filter by `source` itself.

    [ASSUMED] His "Scoring Probability" is read here as "the offense's next scoring event
    in this half is its own touchdown" (`Next_Score_Half == success_label`, default
    `"Touchdown"`) -- the closest canonical equivalent to a workbook that never states its
    own outcome definition in a machine-readable way. A differing definition on his side
    would show up as a systematic offset between `hc_published_sp` and `hc_recomputed_sp`
    in `comparison_table` rather than vanishing silently -- that is exactly why both columns
    get computed instead of trusting either one alone.

    Built through `flag_football_ep.reports.aggregate.rate_table` -- not a second
    Clopper-Pearson implementation. Adds `thin` (`n < THIN_MIN_N`) alongside `rate_table`'s
    own `muted` (`n < MUTED_MIN_N`): two separate, documented thresholds, because this
    comparison's argument depends on being able to tell "too thin to trust" apart from
    "trustworthy but noticeably thinner than its counterpart".

    `down == 0` (PAT) rows are excluded before grouping -- a PAT is not a down/distance
    situation on this axis. Returns the full declared schema with zero rows when `prepared`
    is empty (or empty after the down==0 filter), and raises nothing in that case. Raises
    `MissingComparisonColumns`, naming every missing column, when `down`, `yards_to_go`,
    `yardline_50` or `Next_Score_Half` is absent from `prepared`.
    """
    missing = [c for c in _EMPIRICAL_SP_REQUIRED_COLUMNS if c not in prepared.columns]
    if missing:
        raise MissingComparisonColumns(
            f"empirical_sp: missing required column(s): {', '.join(missing)}"
        )

    scoped = prepared.filter(pl.col("down") != 0).with_columns(
        distance_bin=distance_bin_expr(),
        field_half=field_half_expr(),
    )

    table = rate_table(
        scoped,
        list(COMPARISON_KEYS),
        pl.col("Next_Score_Half") == success_label,
    )
    return table.with_columns(thin=(pl.col("n") < THIN_MIN_N))
