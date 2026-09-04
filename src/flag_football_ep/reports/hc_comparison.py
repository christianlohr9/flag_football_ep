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


def _scope_to_axis(prepared: pl.DataFrame) -> pl.DataFrame:
    """Shared scoping for `empirical_sp` and `model_ep_per_cell`: exclude PAT rows
    (`down == 0`), add `distance_bin`/`field_half`, then drop any row where `down`,
    `distance_bin` or `field_half` is null.

    A row with a null axis component (null `down`, or `yards_to_go`/`yardline_50` null
    enough to make `distance_bin`/`field_half` null) has no defined position on the
    down/distance/field-half grid this comparison is built on -- it is not a "cell" with a
    missing value, it is not a cell at all. Keeping it would silently manufacture a
    `(null, ..., ...)` group that can never appear in the head coach's own table and would
    make `comparison_table`'s `field_half`/`down` columns fail their own domain (`{"own",
    "opponent"}` / `{1, 2, 3, 4}`) contract. This is the same exclusion reasoning as the
    `down == 0` PAT filter, just extended to every axis column instead of only `down`.
    """
    return (
        prepared.filter(pl.col("down") != 0)
        .with_columns(distance_bin=distance_bin_expr(), field_half=field_half_expr())
        .filter(
            pl.col("down").is_not_null()
            & pl.col("distance_bin").is_not_null()
            & pl.col("field_half").is_not_null()
        )
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
    situation on this axis -- and so is any row where `down`, `distance_bin` or
    `field_half` comes out null (`_scope_to_axis`): a null axis component means "no defined
    cell", not "a cell with a missing value". Returns the full declared schema with zero
    rows when `prepared` is empty (or empty after scoping), and raises nothing in that case.
    Raises
    `MissingComparisonColumns`, naming every missing column, when `down`, `yards_to_go`,
    `yardline_50` or `Next_Score_Half` is absent from `prepared`.
    """
    missing = [c for c in _EMPIRICAL_SP_REQUIRED_COLUMNS if c not in prepared.columns]
    if missing:
        raise MissingComparisonColumns(
            f"empirical_sp: missing required column(s): {', '.join(missing)}"
        )

    scoped = _scope_to_axis(prepared)

    table = rate_table(
        scoped,
        list(COMPARISON_KEYS),
        pl.col("Next_Score_Half") == success_label,
    )
    return table.with_columns(thin=(pl.col("n") < THIN_MIN_N))


_MODEL_PROB_COLUMNS: tuple[str, ...] = (
    "Touchdown_Prob",
    "Opp_Touchdown_Prob",
    "Safety_Prob",
    "Opp_Safety_Prob",
    "No_Score_Prob",
)


def _model_ep_expr() -> pl.Expr:
    """The pipeline's own expected-points weighting, copied verbatim from
    `features.mutations.add_ep_variables` (mutations.py:638-647) so the model column here
    and the pipeline's own `ep` mean exactly the same thing. Do not edit this independently
    of that function -- if the weighting ever changes there, it must change here too.
    """
    return (
        (0 * pl.col("No_Score_Prob"))
        + (2 * pl.col("Safety_Prob"))
        + (6 * pl.col("Touchdown_Prob"))
        + (-2 * pl.col("Opp_Safety_Prob"))
        + (-6 * pl.col("Opp_Touchdown_Prob"))
    )


def model_ep_per_cell(
    prepared: pl.DataFrame, oof: pl.DataFrame
) -> tuple[pl.DataFrame, int]:
    """One row per `(down, distance_bin, field_half)` mean out-of-fold expected points.

    Inner-joins `prepared` onto `oof` on `(game_id, play_id)` -- `oof` is
    `data/processed/oof_predictions_ep.parquet` (or an equivalent frame): each historical
    play's probability as predicted by a model that never saw that play's game
    (`model/evaluate.py::oof_frame`'s contract). A play present in `prepared` (after the
    same `down == 0` exclusion `empirical_sp` applies, so the two axes stay comparable) but
    absent from `oof` -- e.g. dropped at training by `drop_nulls()` -- is never silently
    ignored: it is counted in the returned `unscored_rows` integer instead.

    Per-row expected points use `_model_ep_expr()`, the exact weighting copied from
    `add_ep_variables`. Returns `(cells, unscored_rows)` where `cells` has columns
    `down, distance_bin, field_half, model_ep_mean, model_n`.

    Raises `MissingComparisonColumns`, naming every missing column, when any of the five
    `_MODEL_PROB_COLUMNS` is absent from `oof`.
    """
    missing = [c for c in _MODEL_PROB_COLUMNS if c not in oof.columns]
    if missing:
        raise MissingComparisonColumns(
            f"model_ep_per_cell: missing required column(s): {', '.join(missing)}"
        )

    scoped = _scope_to_axis(prepared)

    joined = scoped.join(
        oof.select(["game_id", "play_id", *_MODEL_PROB_COLUMNS]),
        on=["game_id", "play_id"],
        how="inner",
    )
    unscored_rows = scoped.height - joined.height

    schema = {
        "down": pl.Int32,
        "distance_bin": pl.Utf8,
        "field_half": pl.Utf8,
        "model_ep_mean": pl.Float64,
        "model_n": pl.Int64,
    }
    if joined.height == 0:
        return pl.DataFrame(schema=schema), unscored_rows

    cells = (
        joined.with_columns(model_ep=_model_ep_expr())
        .group_by(list(COMPARISON_KEYS), maintain_order=True)
        .agg(
            model_ep_mean=pl.col("model_ep").mean(),
            model_n=pl.len().cast(pl.Int64),
        )
        .sort(list(COMPARISON_KEYS))
    )
    return cells, unscored_rows


def _distance_sort_key_expr() -> pl.Expr:
    """The leading integer of `distance_bin` (`"2"` -> 2, `"10"` -> 10, `"15+"` -> 15,
    `"6-10"` -> 6, `"21-25"` -> 21) -- a natural-numeric sort key shared by every axis this
    module produces (uncluttered and clustered alike), so `"2"` sorts before `"10"` instead
    of after it as plain string comparison would.
    """
    return pl.col("distance_bin").str.extract(r"^(\d+)", 1).cast(pl.Int64, strict=False)


def comparison_table(
    hc_published: pl.DataFrame,
    hc_rows_ours: pl.DataFrame,
    corpus_rows_ours: pl.DataFrame,
    model_cells: pl.DataFrame,
) -> pl.DataFrame:
    """Outer-join all four comparison sources on `(down, distance_bin, field_half)`.

    Inputs:
      - `hc_published`: his own tables, tidy -- columns `down, distance_bin, field_half,
        hc_published_sp, hc_published_n, hc_published_ep`
        (`scripts/epa_comparison.py` merges `sp_by_dd.csv`/`ep_by_dd.csv`/
        `sample_size_by_dd.csv` into this shape before calling this function).
      - `hc_rows_ours`, `corpus_rows_ours`: `empirical_sp` output on his rows in our corpus
        and the rest of the corpus, respectively (columns `down, distance_bin, field_half,
        n, rate, thin`, plus whatever else `empirical_sp` returns -- only these three are
        used here).
      - `model_cells`: `model_ep_per_cell`'s first return value (columns `down,
        distance_bin, field_half, model_ep_mean, model_n`).

    Output columns (join keys plus): `hc_published_sp, hc_published_n, hc_published_ep,
    hc_recomputed_sp, hc_recomputed_n, hc_recomputed_thin, ours_sp, ours_n, ours_thin,
    model_ep, model_n, missing_in, abs_diff_hc_vs_model,
    abs_diff_hc_published_vs_hc_recomputed`.

    This is a genuine outer join -- no key present in any input is dropped, and no missing
    side is ever coalesced to 0. `missing_in` is `"ours"` when a key exists only in
    `hc_published` (both `*_ours` frames and `model_cells` have nothing for it), `"hc"` when
    a key exists in at least one of the three corpus-derived frames but not in
    `hc_published`, and null when the key is present on both the published side and the
    corpus-derived side.

    `hc_published_sp` and `hc_recomputed_sp` are the SAME quantity -- empirical scoring
    probability, same down/distance/field-half definition -- computed two ways over the same
    underlying head-coach plays. A systematic gap between them (`abs_diff_hc_published_vs_
    hc_recomputed`) is a definition mismatch worth raising with the head coach, not a data
    error, which is why both are carried rather than one replacing the other.

    `abs_diff_hc_vs_model` compares points to points: `hc_published_ep` against `model_ep`
    (not `hc_published_sp`, a [0, 1] probability, against `model_ep`, a points scale --
    those are not the same unit and a diff between them would not mean anything).

    No winner, rank or composite score column is computed here -- the table reports; the
    German document (M3-02-07) argues.
    """
    keys = list(COMPARISON_KEYS)

    hc = hc_published.select(
        [*keys, "hc_published_sp", "hc_published_n", "hc_published_ep"]
    ).with_columns(_in_hc_published=pl.lit(True))
    hc_rec = (
        hc_rows_ours.select(keys + ["n", "rate", "thin"])
        .rename({"n": "hc_recomputed_n", "rate": "hc_recomputed_sp", "thin": "hc_recomputed_thin"})
        .with_columns(_in_ours=pl.lit(True))
    )
    ours = (
        corpus_rows_ours.select(keys + ["n", "rate", "thin"])
        .rename({"n": "ours_n", "rate": "ours_sp", "thin": "ours_thin"})
        .with_columns(_in_ours=pl.lit(True))
    )
    model = (
        model_cells.select(keys + ["model_ep_mean", "model_n"])
        .rename({"model_ep_mean": "model_ep"})
        .with_columns(_in_ours=pl.lit(True))
    )

    joined = hc.join(hc_rec, on=keys, how="full", coalesce=True, suffix="_hc_rec")
    joined = joined.join(ours, on=keys, how="full", coalesce=True, suffix="_ours")
    joined = joined.join(model, on=keys, how="full", coalesce=True, suffix="_model")

    present_in_ours = (
        pl.col("_in_ours").fill_null(False)
        | pl.col("_in_ours_ours").fill_null(False)
        | pl.col("_in_ours_model").fill_null(False)
    )
    present_in_hc_published = pl.col("_in_hc_published").fill_null(False)

    result = (
        joined.with_columns(
            missing_in=pl.when(present_in_hc_published & ~present_in_ours)
            .then(pl.lit("ours"))
            .when(~present_in_hc_published & present_in_ours)
            .then(pl.lit("hc"))
            .otherwise(pl.lit(None, dtype=pl.Utf8)),
            abs_diff_hc_vs_model=(pl.col("hc_published_ep") - pl.col("model_ep")).abs(),
            abs_diff_hc_published_vs_hc_recomputed=(
                pl.col("hc_published_sp") - pl.col("hc_recomputed_sp")
            ).abs(),
        )
        .drop(["_in_hc_published", "_in_ours", "_in_ours_ours", "_in_ours_model"])
        .with_columns(_distance_sort_key=_distance_sort_key_expr())
        .sort(["field_half", "down", "_distance_sort_key"], nulls_last=True)
        .drop("_distance_sort_key")
    )
    return result


def coverage_table(comparison: pl.DataFrame) -> pl.DataFrame:
    """The subset of `comparison_table`'s output with a non-null `missing_in`, plus a blank
    `reason` column for the caller to fill in.

    A cell-specific reason (e.g. "distance bin absent from our corpus", "cell absent from
    his workbook") is only knowable at the call site (`scripts/epa_comparison.py`), not from
    the joined frame alone -- this function only isolates which rows need one.
    """
    return comparison.filter(pl.col("missing_in").is_not_null()).with_columns(
        reason=pl.lit(None, dtype=pl.Utf8)
    )
