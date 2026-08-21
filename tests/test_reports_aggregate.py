"""Unit coverage for `flag_football_ep.reports.aggregate`.

Every test builds inputs with `flag_football_ep.testing.canonical_plays` plus targeted
`overrides`/`extras`, matching the class-per-function layout of `tests/test_features_mutations.py`.
"""

from __future__ import annotations

import pytest

from flag_football_ep.reports.aggregate import (
    CHARTED_COLUMNS,
    DISTANCE_BUCKETS,
    FIELD_ZONES,
    SCORE_STATES,
    SectionBasis,
    add_report_buckets,
    charted_only,
    section_basis,
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
