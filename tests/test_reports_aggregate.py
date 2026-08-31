"""Unit coverage for `flag_football_ep.reports.aggregate`.

Every test builds inputs with `flag_football_ep.testing.canonical_plays` plus targeted
`overrides`/`extras`, matching the class-per-function layout of `tests/test_features_mutations.py`.
"""

from __future__ import annotations

import polars as pl
import pytest

from flag_football_ep.reports.aggregate import (
    CHARTED_COLUMNS,
    DISTANCE_BUCKETS,
    FIELD_ZONES,
    MUTED_MIN_N,
    SCORE_STATES,
    RateCell,
    SectionBasis,
    add_report_buckets,
    charted_only,
    rate_table,
    section_basis,
    share_table,
)
from flag_football_ep.testing import canonical_plays


class TestAddReportBuckets:
    def test_field_zone_boundary_assignment(self) -> None:
        yardlines = [0, 9, 10, 22, 23, 36, 37, 50]
        df = canonical_plays(
            n_games=1, plays_per_game=8, overrides={"yardline_50": yardlines}
        )
        result = add_report_buckets(df)
        expected = [
            "Red Zone",
            "Red Zone",
            "Gegnerhälfte",
            "Gegnerhälfte",
            "Mittelfeld",
            "Mittelfeld",
            "Eigene Hälfte",
            "Eigene Hälfte",
        ]
        assert result["field_zone"].to_list() == expected

    def test_null_yardline_50_yields_null_field_zone(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=2, overrides={"yardline_50": [None, 5]}
        )
        result = add_report_buckets(df)
        assert result["field_zone"].to_list() == [None, "Red Zone"]

    def test_out_of_range_yardline_50_yields_null_field_zone(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=2, overrides={"yardline_50": [-1, 51]}
        )
        result = add_report_buckets(df)
        assert result["field_zone"].to_list() == [None, None]

    def test_score_state_mapping(self) -> None:
        diffs = [-7, -6, 0, 6, 7]
        df = canonical_plays(
            n_games=1, plays_per_game=5, overrides={"score_differential": diffs}
        )
        result = add_report_buckets(df)
        expected = ["Rückstand", "Ausgeglichen", "Ausgeglichen", "Ausgeglichen", "Führung"]
        assert result["score_state"].to_list() == expected

    def test_null_score_differential_yields_null_score_state(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=2, overrides={"score_differential": [None, 0]}
        )
        result = add_report_buckets(df)
        assert result["score_state"].to_list() == [None, "Ausgeglichen"]

    def test_distance_bucket_boundaries(self) -> None:
        distances = [1, 3, 4, 6, 7, 10, 11]
        df = canonical_plays(
            n_games=1, plays_per_game=7, overrides={"yards_to_go": distances}
        )
        result = add_report_buckets(df)
        expected = [
            "Short 1-3",
            "Short 1-3",
            "Medium 4-6",
            "Medium 4-6",
            "Long 7-10",
            "Long 7-10",
            "XL 11+",
        ]
        assert result["distance_bucket"].to_list() == expected

    def test_distance_below_one_or_null_yields_null(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=2, overrides={"yards_to_go": [0, None]}
        )
        result = add_report_buckets(df)
        assert result["distance_bucket"].to_list() == [None, None]

    def test_row_count_and_order_preserved(self) -> None:
        df = canonical_plays(n_games=2, plays_per_game=5)
        result = add_report_buckets(df)
        assert result.height == df.height
        assert result["game_id"].to_list() == df["game_id"].to_list()
        assert result["play_id"].to_list() == df["play_id"].to_list()

    def test_source_frame_not_mutated(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=4)
        original_columns = list(df.columns)
        add_report_buckets(df)
        assert df.columns == original_columns


class TestSectionBasis:
    def test_multi_game_multi_source_text(self) -> None:
        df = canonical_plays(
            n_games=2,
            plays_per_game=2,
            overrides={"source": ["hudl", "hudl", "ifaf", "ifaf"]},
        )
        basis = section_basis(df)
        assert basis.n_plays == 4
        assert basis.games == ("test-0", "test-1")
        assert basis.sources == ("hudl", "ifaf")
        assert basis.text == "Datenbasis: 2 Spiele (hudl, ifaf), 4 Plays"

    def test_singular_one_spiel_form(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=3, source="hudl")
        basis = section_basis(df)
        assert basis.games == ("test-0",)
        assert basis.text == "Datenbasis: 1 Spiel (hudl), 3 Plays"

    def test_empty_frame(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=1).clear()
        basis = section_basis(df)
        assert basis == SectionBasis((), (), 0, "Datenbasis: keine Daten")


class TestChartedOnly:
    def test_filters_nulls(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=4,
            extras={"off_form": ["Trips", None, "Bunch", None]},
        )
        result = charted_only(df, "off_form")
        assert result.height == 2
        assert result["off_form"].to_list() == ["Trips", "Bunch"]

    def test_raises_on_unknown_column(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=2)
        with pytest.raises(ValueError, match="not_a_column"):
            charted_only(df, "not_a_column")

    def test_all_null_returns_empty_frame_not_error(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=3, extras={"off_form": [None, None, None]}
        )
        result = charted_only(df, "off_form")
        assert result.height == 0


class TestRateCell:
    def test_text_formats_percentage_and_count(self) -> None:
        cell = RateCell(
            label="Pass", n=9, successes=7, rate=7 / 9, ci=(0.4, 0.95), muted=False
        )
        assert cell.text == "78% Pass (n=9)"

    def test_text_keine_daten_at_zero_n(self) -> None:
        cell = RateCell(label="Pass", n=0, successes=0, rate=None, ci=None, muted=True)
        assert cell.text == "keine Daten"

    def test_muted_flips_at_threshold(self) -> None:
        muted_cell = RateCell(
            label="X", n=MUTED_MIN_N - 1, successes=1, rate=0.5, ci=(0.1, 0.9), muted=True
        )
        not_muted_cell = RateCell(
            label="X", n=MUTED_MIN_N, successes=1, rate=0.2, ci=(0.1, 0.9), muted=False
        )
        assert muted_cell.muted is True
        assert not_muted_cell.muted is False


class TestRateTable:
    def test_one_row_per_group_with_correct_counts(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=6,
            overrides={
                "posteam": ["A", "A", "A", "B", "B", "B"],
                "complete_pass": [1, 1, 0, 1, 1, 1],
            },
        )
        result = rate_table(df, ["posteam"], pl.col("complete_pass") == 1)
        assert result.height == 2
        rows = {row["posteam"]: row for row in result.to_dicts()}
        assert rows["A"]["n"] == 3
        assert rows["A"]["successes"] == 2
        assert rows["A"]["rate"] == pytest.approx(2 / 3)
        assert rows["B"]["n"] == 3
        assert rows["B"]["successes"] == 3

    def test_ci_bounds_contain_rate(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=6,
            overrides={
                "posteam": ["A", "A", "A", "B", "B", "B"],
                "complete_pass": [1, 1, 0, 1, 1, 1],
            },
        )
        result = rate_table(df, ["posteam"], pl.col("complete_pass") == 1)
        for row in result.to_dicts():
            assert row["ci_low"] <= row["rate"] <= row["ci_high"]

    def test_extreme_rate_ci_upper_bound_is_one(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=3,
            overrides={"posteam": ["A", "A", "A"], "complete_pass": [1, 1, 1]},
        )
        result = rate_table(df, ["posteam"], pl.col("complete_pass") == 1)
        row = result.to_dicts()[0]
        assert row["ci_high"] == 1.0
        assert row["ci_low"] < 1.0

    def test_sorted_by_group_cols(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=4,
            overrides={
                "posteam": ["B", "A", "B", "A"],
                "complete_pass": [1, 0, 1, 1],
            },
        )
        result = rate_table(df, ["posteam"], pl.col("complete_pass") == 1)
        assert result["posteam"].to_list() == ["A", "B"]

    def test_empty_input_returns_schema_correct_empty_frame(self) -> None:
        empty_df = pl.DataFrame()
        result = rate_table(empty_df, ["x"], pl.col("dummy") == 1)
        assert result.height == 0
        for col in ("n", "successes", "rate", "ci_low", "ci_high", "muted"):
            assert col in result.columns


class TestShareTable:
    def test_shares_sum_to_one_including_unbekannt(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=5,
            overrides={"posteam": ["A", "A", "A", "A", "A"]},
            extras={"off_play": ["Run", "Run", "Pass", None, None]},
        )
        result = share_table(df, ["posteam"], "off_play")
        total_share = result["share"].sum()
        assert total_share == pytest.approx(1.0)
        categories = result["off_play"].to_list()
        assert "unbekannt" in categories
        unbekannt_row = [r for r in result.to_dicts() if r["off_play"] == "unbekannt"][0]
        assert unbekannt_row["n"] == 2
        assert unbekannt_row["group_n"] == 5

    def test_orders_by_share_descending_within_group(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=6,
            overrides={"posteam": ["A"] * 6},
            extras={"off_play": ["Run", "Run", "Run", "Run", "Pass", None]},
        )
        result = share_table(df, ["posteam"], "off_play")
        shares = result["share"].to_list()
        assert shares == sorted(shares, reverse=True)
        assert result.to_dicts()[0]["off_play"] == "Run"

    def test_ci_bounds_contain_share(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=5,
            overrides={"posteam": ["A"] * 5},
            extras={"off_play": ["Run", "Run", "Pass", "Pass", "Pass"]},
        )
        result = share_table(df, ["posteam"], "off_play")
        for row in result.to_dicts():
            assert row["ci_low"] <= row["share"] <= row["ci_high"]

    def test_muted_flips_on_group_n(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=4,
            overrides={"posteam": ["A"] * 4},
            extras={"off_play": ["Run"] * 4},
        )
        result = share_table(df, ["posteam"], "off_play")
        row = result.to_dicts()[0]
        assert row["group_n"] == 4
        assert row["muted"] is True

    def test_empty_input_returns_schema_correct_empty_frame(self) -> None:
        empty_df = pl.DataFrame()
        result = share_table(empty_df, ["x"], "off_play")
        assert result.height == 0
        for col in ("n", "group_n", "share", "ci_low", "ci_high", "muted"):
            assert col in result.columns


class TestModuleConstants:
    def test_field_zones_cover_full_range_with_red_zone(self) -> None:
        assert FIELD_ZONES[0][0] == "Red Zone"
        assert FIELD_ZONES[0][1] == 0
        assert FIELD_ZONES[0][2] == 9
        assert FIELD_ZONES[-1][2] == 50

    def test_score_states_has_three_entries(self) -> None:
        assert len(SCORE_STATES) == 3

    def test_distance_buckets_last_bucket_is_open_ended(self) -> None:
        assert DISTANCE_BUCKETS[-1][2] is None

    def test_charted_columns_matches_canonical_extras(self) -> None:
        assert set(CHARTED_COLUMNS) == {
            "off_form",
            "off_play",
            "target_route",
            "def_front",
            "coverage",
        }
