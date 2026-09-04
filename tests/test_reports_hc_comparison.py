"""Unit coverage for `flag_football_ep.reports.hc_comparison`.

Every test builds inputs with `flag_football_ep.testing.canonical_plays`/
`canonical_plays_with_scores` plus targeted `overrides`, matching the class-per-function
layout of `tests/test_features_mutations.py` and `tests/test_reports_aggregate.py`.
"""

from __future__ import annotations

import polars as pl
import pytest

from flag_football_ep.features.mutations import prepare_ep_data
from flag_football_ep.reports.hc_comparison import (
    MAX_INDIVIDUAL_DISTANCE,
    MIDFIELD_YARDLINE,
    THIN_MIN_N,
    MissingComparisonColumns,
    comparison_table,
    coverage_table,
    distance_bin_expr,
    empirical_sp,
    field_half_expr,
    model_ep_per_cell,
)
from flag_football_ep.testing import canonical_plays, canonical_plays_with_scores


class TestFieldHalfExpr:
    def test_below_midfield_is_own(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=3, overrides={"yardline_50": [0, 10, 24]}
        )
        result = df.with_columns(field_half=field_half_expr())
        assert result["field_half"].to_list() == ["own", "own", "own"]

    def test_exact_midfield_boundary_is_opponent(self) -> None:
        # Explicit boundary choice: yardline_50 == MIDFIELD_YARDLINE (25) is "opponent",
        # not "own" -- documented here, not left implicit.
        df = canonical_plays(
            n_games=1, plays_per_game=3, overrides={"yardline_50": [25, 26, 50]}
        )
        result = df.with_columns(field_half=field_half_expr())
        assert result["field_half"].to_list() == ["opponent", "opponent", "opponent"]
        assert MIDFIELD_YARDLINE == 25

    def test_null_yardline_yields_null_not_default_bucket(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=2, overrides={"yardline_50": [None, 10]}
        )
        result = df.with_columns(field_half=field_half_expr())
        assert result["field_half"].to_list() == [None, "own"]


class TestDistanceBinExpr:
    def test_individual_bins_one_through_fourteen(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=3, overrides={"yards_to_go": [1, 7, 14]}
        )
        result = df.with_columns(distance_bin=distance_bin_expr())
        assert result["distance_bin"].to_list() == ["1", "7", "14"]
        assert MAX_INDIVIDUAL_DISTANCE == 14

    def test_above_max_individual_falls_into_open_bin(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=2, overrides={"yards_to_go": [15, 30]}
        )
        result = df.with_columns(distance_bin=distance_bin_expr())
        assert result["distance_bin"].to_list() == ["15+", "15+"]

    def test_zero_or_negative_yields_null_not_bin_one(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=2, overrides={"yards_to_go": [0, -3]}
        )
        result = df.with_columns(distance_bin=distance_bin_expr())
        assert result["distance_bin"].to_list() == [None, None]

    def test_null_yards_to_go_yields_null(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=1, overrides={"yards_to_go": [None]}
        )
        result = df.with_columns(distance_bin=distance_bin_expr())
        assert result["distance_bin"].to_list() == [None]

    def test_custom_max_individual(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=2, overrides={"yards_to_go": [5, 6]}
        )
        result = df.with_columns(distance_bin=distance_bin_expr(max_individual=5))
        assert result["distance_bin"].to_list() == ["5", "6+"]


class TestEmpiricalSp:
    def test_one_row_per_cell_with_counts_and_muted_flag(self) -> None:
        # 4 rows in one (down, distance_bin, field_half) cell: 3 Touchdown, 1 Opp_Touchdown.
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=4,
            overrides={
                "half": [1, 1, 1, 1],
                "down": [1, 1, 1, 1],
                "yards_to_go": [5, 5, 5, 5],
                "yardline_50": [10, 10, 10, 10],
                "posteam": ["HOME", "AWAY", "HOME", "HOME"],
                "defteam": ["AWAY", "HOME", "AWAY", "AWAY"],
                "touchdown": [1, 0, 1, 1],
            },
        )
        prepared = prepare_ep_data(df)
        result = empirical_sp(prepared)

        row = result.filter(
            (pl.col("down") == 1)
            & (pl.col("distance_bin") == "5")
            & (pl.col("field_half") == "own")
        ).to_dicts()[0]
        assert row["n"] == 4
        assert row["successes"] == 3
        assert row["rate"] == pytest.approx(0.75)
        assert row["muted"] is True  # below MUTED_MIN_N (5)

    def test_thin_flag_distinct_from_muted_at_n_twenty(self) -> None:
        n = 20
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=n,
            overrides={
                "half": [1] * n,
                "down": [1] * n,
                "yards_to_go": [5] * n,
                "yardline_50": [10] * n,
            },
        )
        prepared = prepare_ep_data(df)
        result = empirical_sp(prepared)

        row = result.filter(
            (pl.col("down") == 1)
            & (pl.col("distance_bin") == "5")
            & (pl.col("field_half") == "own")
        ).to_dicts()[0]
        assert row["n"] == 20
        assert row["muted"] is False
        assert row["thin"] is True
        assert THIN_MIN_N == 30

    def test_pat_rows_excluded_from_every_cell(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=5,
            overrides={
                "half": [1, 1, 1, 1, 1],
                "down": [1, 1, 1, 1, 0],
                "yards_to_go": [5, 5, 5, 5, 5],
                "yardline_50": [10, 10, 10, 10, 10],
                "posteam": ["HOME", "HOME", "HOME", "HOME", "HOME"],
                "defteam": ["AWAY", "AWAY", "AWAY", "AWAY", "AWAY"],
                "touchdown": [1, 1, 1, 1, 0],
            },
        )
        prepared = prepare_ep_data(df)
        result = empirical_sp(prepared)

        assert result.filter(pl.col("down") == 0).height == 0
        row = result.filter(
            (pl.col("down") == 1)
            & (pl.col("distance_bin") == "5")
            & (pl.col("field_half") == "own")
        ).to_dicts()[0]
        assert row["n"] == 4

    def test_empty_input_returns_full_schema_and_raises_nothing(self) -> None:
        df = canonical_plays_with_scores(n_games=1, plays_per_game=2)
        prepared = prepare_ep_data(df).filter(pl.col("down") == 999)
        assert prepared.height == 0

        result = empirical_sp(prepared)
        assert result.height == 0
        for col in (
            "down",
            "distance_bin",
            "field_half",
            "n",
            "successes",
            "rate",
            "ci_low",
            "ci_high",
            "muted",
            "thin",
        ):
            assert col in result.columns

    def test_missing_required_columns_named_in_error(self) -> None:
        df = pl.DataFrame({"foo": [1, 2, 3]})
        with pytest.raises(MissingComparisonColumns) as excinfo:
            empirical_sp(df)
        msg = str(excinfo.value)
        for col in ("down", "yards_to_go", "yardline_50", "Next_Score_Half"):
            assert col in msg


_OOF_PROB_COLUMNS = (
    "Touchdown_Prob",
    "Opp_Touchdown_Prob",
    "Safety_Prob",
    "Opp_Safety_Prob",
    "No_Score_Prob",
)


class TestModelEpPerCell:
    def _prepared(self, **overrides) -> pl.DataFrame:
        df = canonical_plays_with_scores(n_games=1, plays_per_game=2, overrides=overrides)
        return prepare_ep_data(df)

    def test_pinned_weighting_touchdown_prob_one_gives_six(self) -> None:
        prepared = self._prepared(
            half=[1, 1],
            down=[1, 2],
            yards_to_go=[5, 6],
            yardline_50=[10, 10],
        )
        game_id = prepared["game_id"][0]
        oof = pl.DataFrame(
            {
                "game_id": [game_id, game_id],
                "play_id": [1, 2],
                "Touchdown_Prob": [1.0, 0.0],
                "Opp_Touchdown_Prob": [0.0, 0.0],
                "Safety_Prob": [0.0, 0.0],
                "Opp_Safety_Prob": [0.0, 0.0],
                "No_Score_Prob": [0.0, 1.0],
            }
        )
        cells, unscored = model_ep_per_cell(prepared, oof)

        row = cells.filter(
            (pl.col("down") == 1)
            & (pl.col("distance_bin") == "5")
            & (pl.col("field_half") == "own")
        ).to_dicts()[0]
        assert row["model_ep_mean"] == pytest.approx(6.0)
        assert row["model_n"] == 1
        assert unscored == 0

    def test_unscored_rows_counted_not_dropped(self) -> None:
        prepared = self._prepared(
            half=[1, 1],
            down=[1, 2],
            yards_to_go=[5, 6],
            yardline_50=[10, 10],
        )
        game_id = prepared["game_id"][0]
        oof = pl.DataFrame(
            {
                "game_id": [game_id],
                "play_id": [1],
                "Touchdown_Prob": [0.5],
                "Opp_Touchdown_Prob": [0.1],
                "Safety_Prob": [0.1],
                "Opp_Safety_Prob": [0.1],
                "No_Score_Prob": [0.2],
            }
        )
        cells, unscored = model_ep_per_cell(prepared, oof)
        assert unscored == 1
        assert cells.height == 1

    def test_missing_probability_columns_named_in_error(self) -> None:
        prepared = self._prepared(
            half=[1, 1], down=[1, 1], yards_to_go=[5, 5], yardline_50=[10, 10]
        )
        oof = pl.DataFrame({"game_id": ["x"], "play_id": [1], "Touchdown_Prob": [1.0]})
        with pytest.raises(MissingComparisonColumns) as excinfo:
            model_ep_per_cell(prepared, oof)
        msg = str(excinfo.value)
        for col in ("Opp_Touchdown_Prob", "Safety_Prob", "Opp_Safety_Prob", "No_Score_Prob"):
            assert col in msg


def _hc_published_fixture() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "down": [1, 1],
            "distance_bin": ["1", "2"],
            "field_half": ["own", "own"],
            "hc_published_sp": [0.667, 0.5],
            "hc_published_n": [21, 10],
            "hc_published_ep": [4.0, 3.0],
        }
    )


def _hc_rows_ours_fixture() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "down": [1],
            "distance_bin": ["1"],
            "field_half": ["own"],
            "n": [18],
            "rate": [0.6],
            "thin": [True],
        }
    )


def _corpus_rows_ours_fixture() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "down": [1, 2],
            "distance_bin": ["1", "10"],
            "field_half": ["own", "opponent"],
            "n": [200, 15],
            "rate": [0.55, 0.4],
            "thin": [False, True],
        }
    )


def _model_cells_fixture() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "down": [1, 2],
            "distance_bin": ["1", "10"],
            "field_half": ["own", "opponent"],
            "model_ep_mean": [4.1, 2.0],
            "model_n": [50, 12],
        }
    )


class TestComparisonTable:
    def _result(self) -> pl.DataFrame:
        return comparison_table(
            _hc_published_fixture(),
            _hc_rows_ours_fixture(),
            _corpus_rows_ours_fixture(),
            _model_cells_fixture(),
        )

    def test_outer_join_keeps_every_key(self) -> None:
        result = self._result()
        keys = set(
            zip(
                result["down"].to_list(),
                result["distance_bin"].to_list(),
                result["field_half"].to_list(),
            )
        )
        assert keys == {(1, "1", "own"), (1, "2", "own"), (2, "10", "opponent")}

    def test_missing_in_ours_when_only_published(self) -> None:
        result = self._result()
        row = result.filter(
            (pl.col("down") == 1) & (pl.col("distance_bin") == "2")
        ).to_dicts()[0]
        assert row["missing_in"] == "ours"
        assert row["hc_recomputed_sp"] is None
        assert row["ours_sp"] is None
        assert row["model_ep"] is None

    def test_missing_in_hc_when_only_ours(self) -> None:
        result = self._result()
        row = result.filter(
            (pl.col("down") == 2) & (pl.col("distance_bin") == "10")
        ).to_dicts()[0]
        assert row["missing_in"] == "hc"
        assert row["hc_published_sp"] is None
        assert row["hc_published_n"] is None

    def test_missing_in_null_when_present_everywhere(self) -> None:
        result = self._result()
        row = result.filter(
            (pl.col("down") == 1) & (pl.col("distance_bin") == "1")
        ).to_dicts()[0]
        assert row["missing_in"] is None

    def test_abs_diffs_computed_on_matching_units(self) -> None:
        result = self._result()
        row = result.filter(
            (pl.col("down") == 1) & (pl.col("distance_bin") == "1")
        ).to_dicts()[0]
        assert row["abs_diff_hc_published_vs_hc_recomputed"] == pytest.approx(
            abs(0.667 - 0.6)
        )
        assert row["abs_diff_hc_vs_model"] == pytest.approx(abs(4.0 - 4.1))

    def test_no_winner_or_rank_column(self) -> None:
        result = self._result()
        for forbidden in ("winner", "rank", "score", "best"):
            assert forbidden not in result.columns

    def test_distance_bin_sorts_naturally_not_lexically(self) -> None:
        keys = pl.DataFrame(
            {
                "down": [1, 1],
                "distance_bin": ["10", "2"],
                "field_half": ["own", "own"],
            }
        )
        hc_pub = keys.with_columns(
            hc_published_sp=pl.lit(0.5),
            hc_published_n=pl.lit(10),
            hc_published_ep=pl.lit(3.0),
        )
        hc_rec = keys.with_columns(n=pl.lit(10), rate=pl.lit(0.5), thin=pl.lit(False))
        ours = keys.with_columns(n=pl.lit(10), rate=pl.lit(0.5), thin=pl.lit(False))
        model = keys.with_columns(model_ep_mean=pl.lit(3.0), model_n=pl.lit(10))

        result = comparison_table(hc_pub, hc_rec, ours, model)
        assert result["distance_bin"].to_list() == ["2", "10"]


class TestCoverageTable:
    def test_returns_only_missing_rows_with_blank_reason_column(self) -> None:
        comparison = comparison_table(
            _hc_published_fixture(),
            _hc_rows_ours_fixture(),
            _corpus_rows_ours_fixture(),
            _model_cells_fixture(),
        )
        coverage = coverage_table(comparison)

        assert coverage.height == 2
        assert set(coverage["missing_in"].to_list()) == {"ours", "hc"}
        assert "reason" in coverage.columns
        assert coverage["reason"].null_count() == coverage.height
