"""Build the head-coach-vs-model comparison table for the October 2026 sync (EPA-D03).

Four columns per `(down, distance_bin, field_half)` cell:

1. His published number (`data/reference/hc_sp_tables/*.csv`, M3-02-03's read-only snapshot),
   with his own `Sample Size by D&D` count.
2. The same empirical scoring probability recomputed by us from HIS rows now in our canonical
   corpus, with our own n -- same data, same question, computed twice.
3. The same empirical number from the rest of the corpus (non-head-coach rows).
4. The model's mean out-of-fold expected points, from `data/processed/oof_predictions_ep.
   parquet` (M3-02-05's with-head-coach LOGO arm) -- never a champion re-score, so no cell is
   scored by a model that saw that game while fitting.

Two tables: `comparison_by_dd.csv` on his own uncluttered 1..14+ axis, `comparison_clustered.
csv` on his clustered bin labels (own-half and opposite-half edges kept separate, exactly as
snapshotted -- see `_OWN_CLUSTERED_BINS`/`_OPP_CLUSTERED_BINS` below). `comparison_coverage.
csv` is the union of both tables' `missing_in` rows with a filled-in `reason`, tagged by
`axis` -- a cell his workbook has and our corpus does not (or vice versa) is never a silent
zero, always a listed row.

All join/aggregation logic lives in `flag_football_ep.reports.hc_comparison`
(`empirical_sp`, `model_ep_per_cell`, `comparison_table`, `coverage_table`); this script only
loads the four input sources into the shapes those functions expect and writes the result.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from flag_football_ep.config import load_config  # noqa: E402
from flag_football_ep.features.mutations import (  # noqa: E402
    HC_SOURCE_PREFIX,
    prepare_ep_data,
)
from flag_football_ep.reports.aggregate import rate_table  # noqa: E402
from flag_football_ep.reports.hc_comparison import (  # noqa: E402
    _MODEL_PROB_COLUMNS,
    _model_ep_expr,
    COMPARISON_KEYS,
    MAX_INDIVIDUAL_DISTANCE,
    THIN_MIN_N,
    comparison_table,
    coverage_table,
    empirical_sp,
    field_half_expr,
    model_ep_per_cell,
)

# Her clustered bin edges as snapshotted (M3-02-03-SUMMARY, sp_by_dd_clustered.csv):
# own-half and opposite-half columns use slightly different edges -- never forced onto one
# shared set. Each entry is (label, low, high); high=None means an open upper bin.
_OWN_CLUSTERED_BINS: tuple[tuple[str, int, int | None], ...] = (
    ("1-5", 1, 5),
    ("6-10", 6, 10),
    ("11-15", 11, 15),
    ("16-19", 16, 19),
    ("20", 20, 20),
    ("20+", 21, None),
)
_OPP_CLUSTERED_BINS: tuple[tuple[str, int, int | None], ...] = (
    ("1-5", 1, 5),
    ("6-10", 6, 10),
    ("11-15", 11, 15),
    ("16-20", 16, 20),
    ("21-25", 21, 25),
    ("25+", 26, None),
)

# The two spot values verified against the real workbook (M3-02-RESEARCH section 4.2,
# M3-02-03-SUMMARY "Decisions Made"): down=1/distance=1/own-half SP=0.667 on n=21 is a
# single-down cell and checked directly. The RESEARCH/plan text's second anchor
# ("down=1/distance=10/own-half n=324") does not hold as a single-down cell -- M3-02-03's own
# investigation found the real n=324 is the *sum across all four downs* at distance=10/own
# (42 + 133 + 102 + 47 = 324), a workbook "Total" row, not a per-down cell. This script
# checks the reinterpreted (summed) form, consistent with that prior finding, and prints
# both readings so the discrepancy stays visible rather than silently reconciled.
_ANCHOR_1_DOWN = 1
_ANCHOR_1_DISTANCE = "1"
_ANCHOR_1_FIELD_HALF = "own"
_ANCHOR_1_SP = 0.667
_ANCHOR_1_N = 21
_ANCHOR_2_DISTANCE = "10"
_ANCHOR_2_FIELD_HALF = "own"
_ANCHOR_2_SUMMED_N = 324


def _load_hc_snapshot_csv(path: Path) -> pl.DataFrame:
    """Read one of the tidy uncluttered `hc_sp_tables` matrix CSVs (`sp_by_dd.csv`,
    `ep_by_dd.csv`, `sample_size_by_dd.csv`) with `distance_bin` forced to Utf8 --
    `pl.read_csv` otherwise infers it as Int64 for these tabs (every value is a plain digit
    string), which would silently break the join against our corpus's string distance-bin
    axis (`"1"`, `"15+"`, ...). Column names are left exactly as snapshotted (`value` for
    the SP/EP tabs, `n` for the sample-size tab).
    """
    return pl.read_csv(
        path,
        schema_overrides={"down": pl.Int64, "distance_bin": pl.Utf8},
    )


def _bucket_label_expr(max_individual: int) -> pl.Expr:
    return (
        pl.when(pl.col("distance_bin").cast(pl.Int64, strict=False) <= max_individual)
        .then(pl.col("distance_bin"))
        .otherwise(pl.lit(f"{max_individual + 1}+"))
    )


def build_hc_published_uncluttered(
    sp: pl.DataFrame, ep: pl.DataFrame, n: pl.DataFrame, *, max_individual: int = MAX_INDIVIDUAL_DISTANCE
) -> pl.DataFrame:
    """Re-bucket his own uncluttered `sp_by_dd`/`ep_by_dd`/`sample_size_by_dd` tabs (plain
    distance-to-go integers, 1..34/1..49) onto the same `(1..14, "15+")` axis
    `distance_bin_expr` builds from our corpus, so the two sides of the join describe the
    same cells.

    Distances `<= max_individual` are single-row groups (pass through unchanged). Distances
    above it are pooled into one `"{max_individual + 1}+"` bin per `(down, field_half)`,
    n-weighted for `hc_published_sp`/`hc_published_ep` (sample-size-weighted average, not a
    simple mean of ratios) and summed for `hc_published_n`. `hc_published_n` is built from
    the full `sample_size_by_dd` table regardless of whether SP/EP has a value for that cell
    (M3-02-03 found 83 cells where a sample size exists with no computed SP) -- a bucket
    that pools some n-with-no-SP cells alongside n-with-SP cells still reports the honest
    total n; only the SP/EP weighted average silently excludes the value-less rows (a
    weighted average has nothing to weight them by).
    """
    keys = ["down", "distance_bin", "field_half"]
    bucket = _bucket_label_expr(max_individual)

    n_bucketed = (
        n.with_columns(distance_bin=bucket)
        .group_by(keys, maintain_order=True)
        .agg(hc_published_n=pl.col("n").sum())
    )

    def _weighted(values: pl.DataFrame, out_col: str) -> pl.DataFrame:
        joined = values.select(["down", "distance_bin", "field_half", "value"]).join(
            n.select(["down", "distance_bin", "field_half", "n"]),
            on=keys,
            how="inner",  # sp/ep keys are a documented subset of n's keys (M3-02-03)
        )
        return (
            joined.with_columns(distance_bin=bucket)
            .group_by(keys, maintain_order=True)
            .agg(
                _num=(pl.col("value") * pl.col("n")).sum(),
                _den=pl.col("n").sum(),
            )
            .with_columns(**{out_col: pl.col("_num") / pl.col("_den")})
            .select([*keys, out_col])
        )

    sp_bucketed = _weighted(sp, "hc_published_sp")
    ep_bucketed = _weighted(ep, "hc_published_ep")

    return (
        n_bucketed.join(sp_bucketed, on=keys, how="left")
        .join(ep_bucketed, on=keys, how="left")
        .with_columns(down=pl.col("down").cast(pl.Int32))
        .select(["down", "distance_bin", "field_half", "hc_published_sp", "hc_published_n", "hc_published_ep"])
    )


def build_hc_published_clustered(sp: pl.DataFrame, ep: pl.DataFrame, n: pl.DataFrame) -> pl.DataFrame:
    """His clustered tabs are already at the target granularity -- no re-bucketing, just a
    three-way merge into the `hc_published` shape `comparison_table` expects.
    """
    keys = ["down", "distance_bin", "field_half"]
    return (
        n.rename({"n": "hc_published_n"})
        .select([*keys, "hc_published_n"])
        .join(
            sp.rename({"value": "hc_published_sp"}).select([*keys, "hc_published_sp"]),
            on=keys,
            how="left",
        )
        .join(
            ep.rename({"value": "hc_published_ep"}).select([*keys, "hc_published_ep"]),
            on=keys,
            how="left",
        )
        .with_columns(down=pl.col("down").cast(pl.Int32))
        .select(["down", "distance_bin", "field_half", "hc_published_sp", "hc_published_n", "hc_published_ep"])
    )


def _clustered_distance_bin_expr() -> pl.Expr:
    """Field-half-dependent clustered distance bin: own-half and opposite-half plays use
    `_OWN_CLUSTERED_BINS`/`_OPP_CLUSTERED_BINS` respectively, exactly as his clustered tabs
    snapshot them (own-half's `20`/`20+` split vs opposite-half's `21-25`/`25+` split are not
    the same edges). Requires `field_half` already present on the frame.
    """

    def _bins_expr(bins: tuple[tuple[str, int, int | None], ...]) -> pl.Expr:
        expr: pl.Expr | None = None
        for label, low, high in bins:
            cond = pl.col("yards_to_go") >= low
            if high is not None:
                cond = cond & (pl.col("yards_to_go") <= high)
            expr = pl.when(cond).then(pl.lit(label)) if expr is None else expr.when(cond).then(pl.lit(label))
        assert expr is not None
        return expr.otherwise(pl.lit(None, dtype=pl.Utf8))

    return (
        pl.when(pl.col("yards_to_go").is_null())
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(pl.col("field_half") == "own")
        .then(_bins_expr(_OWN_CLUSTERED_BINS))
        .when(pl.col("field_half") == "opponent")
        .then(_bins_expr(_OPP_CLUSTERED_BINS))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )


def empirical_sp_clustered(prepared: pl.DataFrame) -> pl.DataFrame:
    """The clustered-axis twin of `hc_comparison.empirical_sp` -- same PAT exclusion, same
    `rate_table` machinery, same `THIN_MIN_N` threshold, different (field-half-dependent)
    distance binning. Kept in this script rather than `hc_comparison.py` because
    `empirical_sp`'s public contract (M3-02-06 Task 1) is locked to the uncluttered
    `distance_bin_expr` axis only.
    """
    scoped = prepared.filter(pl.col("down") != 0).with_columns(
        field_half=field_half_expr()
    ).with_columns(distance_bin=_clustered_distance_bin_expr())

    table = rate_table(scoped, list(COMPARISON_KEYS), pl.col("Next_Score_Half") == "Touchdown")
    return table.with_columns(thin=(pl.col("n") < THIN_MIN_N))


def model_ep_per_cell_clustered(prepared: pl.DataFrame, oof: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """The clustered-axis twin of `hc_comparison.model_ep_per_cell`. Same inner join on
    `(game_id, play_id)`, same expected-points weighting (copied from `add_ep_variables`,
    matching `hc_comparison._model_ep_expr` verbatim), different distance binning.
    """
    scoped = prepared.filter(pl.col("down") != 0).with_columns(
        field_half=field_half_expr()
    ).with_columns(distance_bin=_clustered_distance_bin_expr())

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
        .agg(model_ep_mean=pl.col("model_ep").mean(), model_n=pl.len().cast(pl.Int64))
        .sort(list(COMPARISON_KEYS))
    )
    return cells, unscored_rows


def _fill_coverage_reasons(coverage: pl.DataFrame) -> pl.DataFrame:
    return coverage.with_columns(
        reason=pl.when(pl.col("missing_in") == "hc")
        .then(
            pl.lit(
                "cell absent from his workbook -- no row in the published SP/EP/"
                "sample-size tables for this down/distance-bin/field-half"
            )
        )
        .when(pl.col("missing_in") == "ours")
        .then(
            pl.lit(
                "down/distance-bin/field-half combination absent from our corpus "
                "(after the down==0 PAT exclusion)"
            )
        )
        .otherwise(pl.col("reason"))
    )


def _print_top_disagreements(comparison: pl.DataFrame, *, label: str) -> None:
    for col in ("abs_diff_hc_vs_model", "abs_diff_hc_published_vs_hc_recomputed"):
        top = (
            comparison.filter(pl.col(col).is_not_null())
            .sort(col, descending=True)
            .head(5)
            .select(["down", "distance_bin", "field_half", col])
        )
        print(f"[{label}] top 5 by {col}:")
        for row in top.to_dicts():
            print(f"  {row}")


def _check_anchors(comparison_by_dd: pl.DataFrame, sample_size: pl.DataFrame) -> None:
    row = comparison_by_dd.filter(
        (pl.col("down") == _ANCHOR_1_DOWN)
        & (pl.col("distance_bin") == _ANCHOR_1_DISTANCE)
        & (pl.col("field_half") == _ANCHOR_1_FIELD_HALF)
    ).to_dicts()
    if not row:
        raise SystemExit(
            f"anchor 1 missing: down={_ANCHOR_1_DOWN} distance={_ANCHOR_1_DISTANCE} "
            f"field_half={_ANCHOR_1_FIELD_HALF} -- snapshot join keys look misaligned, STOP"
        )
    sp = row[0]["hc_published_sp"]
    n = row[0]["hc_published_n"]
    # 1e-3 tolerance: the plan/RESEARCH text's anchor is quoted rounded to 3 decimals
    # (0.667); the snapshot's own value is the exact ratio (2/3 = 0.6666666667).
    if sp is None or abs(sp - _ANCHOR_1_SP) > 1e-3 or n != _ANCHOR_1_N:
        raise SystemExit(
            f"anchor 1 mismatch: expected sp={_ANCHOR_1_SP} n={_ANCHOR_1_N}, got sp={sp} "
            f"n={n} -- snapshot join keys look misaligned, STOP"
        )
    print(f"anchor 1 OK: down={_ANCHOR_1_DOWN} distance={_ANCHOR_1_DISTANCE} own -> sp={sp} n={n}")

    summed_n = (
        sample_size.filter(
            (pl.col("distance_bin") == _ANCHOR_2_DISTANCE)
            & (pl.col("field_half") == _ANCHOR_2_FIELD_HALF)
        )["n"].sum()
    )
    if summed_n != _ANCHOR_2_SUMMED_N:
        raise SystemExit(
            f"anchor 2 mismatch: expected summed n={_ANCHOR_2_SUMMED_N} across downs at "
            f"distance={_ANCHOR_2_DISTANCE} field_half={_ANCHOR_2_FIELD_HALF}, got "
            f"{summed_n} -- snapshot join keys look misaligned, STOP"
        )
    print(
        f"anchor 2 OK (reinterpreted as a downs-summed total, per M3-02-03): "
        f"distance={_ANCHOR_2_DISTANCE} own, summed across downs -> n={summed_n}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("ffep.toml"), help="Path to ffep.toml")
    parser.add_argument(
        "--out-dir", type=Path, default=Path("data/reference/epa_refinement"),
        help="Output directory for the comparison CSVs",
    )
    parser.add_argument("--dry-run", action="store_true", help="load and validate, write nothing")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    plays = pl.read_parquet(config.paths.processed / "plays.parquet")
    prepared = prepare_ep_data(plays)

    oof_path = config.paths.processed / "oof_predictions_ep.parquet"
    oof = pl.read_parquet(oof_path)
    hc_oof_rows = oof.filter(pl.col("source").str.starts_with(HC_SOURCE_PREFIX)).height
    if hc_oof_rows == 0:
        raise SystemExit(
            f"{oof_path} contains no {HC_SOURCE_PREFIX!r} rows -- this is not the "
            "with-head-coach LOGO arm's out-of-fold output. Re-run "
            "scripts/hc_corpus_ablation.py (M3-02-05) so the with_hc arm is the last one "
            "written, then retry. A comparison built on the wrong arm's out-of-fold "
            "predictions would silently flatter (or unfairly penalize) the model."
        )
    print(f"oof sanity check: {hc_oof_rows} head-coach row(s) present in {oof_path.name} -- OK")

    hc_dir = config.paths.reference / "hc_sp_tables"
    sp = _load_hc_snapshot_csv(hc_dir / "sp_by_dd.csv")
    ep = _load_hc_snapshot_csv(hc_dir / "ep_by_dd.csv")
    sample_size = _load_hc_snapshot_csv(hc_dir / "sample_size_by_dd.csv")
    sp_c = pl.read_csv(hc_dir / "sp_by_dd_clustered.csv")
    ep_c = pl.read_csv(hc_dir / "ep_by_dd_clustered.csv")
    n_c = pl.read_csv(hc_dir / "sample_size_by_dd_clustered.csv")

    is_hc = pl.col("source").str.starts_with(HC_SOURCE_PREFIX)
    hc_prepared = prepared.filter(is_hc)
    non_hc_prepared = prepared.filter(~is_hc)
    print(
        f"corpus split: {hc_prepared.height} head-coach row(s), "
        f"{non_hc_prepared.height} non-head-coach row(s)"
    )

    if args.dry_run:
        print("[dry-run] loaded inputs, wrote nothing")
        return 0

    args.out_dir.mkdir(parents=True, exist_ok=True)

    # --- Uncluttered (1..14+) axis -------------------------------------------------
    hc_published = build_hc_published_uncluttered(sp, ep, sample_size)
    hc_rows_ours = empirical_sp(hc_prepared)
    corpus_rows_ours = empirical_sp(non_hc_prepared)
    model_cells, unscored = model_ep_per_cell(prepared, oof)
    print(f"model column: {unscored} row(s) in the prepared corpus had no out-of-fold prediction")

    comparison = comparison_table(hc_published, hc_rows_ours, corpus_rows_ours, model_cells)
    comparison.write_csv(args.out_dir / "comparison_by_dd.csv")
    _check_anchors(comparison, sample_size)

    # --- Clustered axis --------------------------------------------------------------
    hc_published_c = build_hc_published_clustered(sp_c, ep_c, n_c)
    hc_rows_ours_c = empirical_sp_clustered(hc_prepared)
    corpus_rows_ours_c = empirical_sp_clustered(non_hc_prepared)
    model_cells_c, unscored_c = model_ep_per_cell_clustered(prepared, oof)
    comparison_clustered = comparison_table(
        hc_published_c, hc_rows_ours_c, corpus_rows_ours_c, model_cells_c
    )
    comparison_clustered.write_csv(args.out_dir / "comparison_clustered.csv")

    # --- Coverage --------------------------------------------------------------------
    coverage_uncluttered = _fill_coverage_reasons(coverage_table(comparison)).with_columns(
        axis=pl.lit("uncluttered")
    )
    coverage_clustered = _fill_coverage_reasons(coverage_table(comparison_clustered)).with_columns(
        axis=pl.lit("clustered")
    )
    coverage_all = pl.concat([coverage_uncluttered, coverage_clustered], how="vertical_relaxed")
    coverage_all.write_csv(args.out_dir / "comparison_coverage.csv")

    print(
        f"cells: comparison_by_dd={comparison.height} "
        f"comparison_clustered={comparison_clustered.height} "
        f"coverage={coverage_all.height}"
    )
    print(
        f"thin cells: hc_recomputed={comparison.filter(pl.col('hc_recomputed_thin')).height} "
        f"ours={comparison.filter(pl.col('ours_thin')).height} "
        f"(uncluttered axis)"
    )
    ep_vs_six_sp = comparison.filter(
        pl.col("hc_published_sp").is_not_null() & pl.col("hc_published_ep").is_not_null()
    ).select(
        (pl.col("hc_published_ep") - 6 * pl.col("hc_published_sp")).abs().alias("gap")
    )
    if ep_vs_six_sp.height:
        print(
            f"hc_published_ep vs 6*hc_published_sp: mean abs gap = "
            f"{ep_vs_six_sp['gap'].mean():.4f} over {ep_vs_six_sp.height} cell(s)"
        )
    _print_top_disagreements(comparison, label="uncluttered")

    print(
        "No source file under data/reference/hc_sp_tables/ was modified by this script -- "
        "read-only snapshot in, comparison tables out."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
