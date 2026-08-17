"""Unit coverage for the canonical plays schema: conform, casting, game-id
rules and (later in this module) the score chain.
"""

from __future__ import annotations

import polars as pl
import pytest

from flag_football_ep.canonical import (
    CANONICAL_COLUMNS,
    CORE_COLUMNS,
    MissingCanonicalColumns,
    NULLABLE_EXTRAS,
    NON_NULL_COLUMNS,
    add_score_columns,
    add_scoring_play_team,
    conform_to_canonical,
    make_game_id,
)


def _full_core_frame(n: int = 2) -> pl.DataFrame:
    """A frame holding every CORE_COLUMNS column with schema-valid values."""
    data: dict[str, list] = {}
    for name, dtype in CORE_COLUMNS.items():
        if dtype == pl.Utf8:
            data[name] = [f"{name}_{i}" for i in range(n)]
        else:
            data[name] = [i for i in range(n)]
    return pl.DataFrame(data, schema={name: dtype for name, dtype in CORE_COLUMNS.items()})


class TestSchemaDefinition:
    def test_core_columns_contains_result_raw(self):
        assert "result_raw" in CORE_COLUMNS

    def test_nullable_extras_contains_game_clock_ms(self):
        assert "game_clock_ms" in NULLABLE_EXTRAS

    def test_canonical_columns_starts_with_source(self):
        assert CANONICAL_COLUMNS[0] == "source"

    def test_non_null_columns_subset_of_core(self):
        assert set(NON_NULL_COLUMNS) <= set(CORE_COLUMNS)

    def test_canonical_columns_has_at_least_60_entries(self):
        assert len(CANONICAL_COLUMNS) >= 60


class TestMakeGameId:
    def test_hudl_returns_stem_unchanged(self):
        stem = "2026-06-14_GER-vs-AUT_EM-QUALI"
        assert make_game_id("hudl", stem) == stem

    def test_legacy_returns_prefixed_key(self):
        assert make_game_id("legacy", 37) == "legacy-37"

    def test_sportapp_returns_prefixed_key(self):
        assert make_game_id("sportapp", 981) == "sportapp-981"


class TestConformToCanonical:
    def test_output_column_order_equals_canonical_columns(self):
        df = _full_core_frame()
        out, report = conform_to_canonical(df, "hudl")
        assert out.columns == list(CANONICAL_COLUMNS)
        assert report.missing_core == []

    def test_missing_extras_materialized_as_typed_nulls(self):
        df = _full_core_frame()
        out, report = conform_to_canonical(df, "hudl")
        assert set(report.materialized_extras) == set(NULLABLE_EXTRAS)
        for name in NULLABLE_EXTRAS:
            assert out[name].null_count() == out.height

    def test_missing_core_column_raises_with_all_names_in_message(self):
        df = _full_core_frame().drop(["down", "yards_to_go"])
        with pytest.raises(MissingCanonicalColumns) as excinfo:
            conform_to_canonical(df, "hudl")
        assert "down" in str(excinfo.value)
        assert "yards_to_go" in str(excinfo.value)

    def test_uncastable_string_becomes_null_and_is_recorded(self):
        df = _full_core_frame()
        # down is declared Int32; overwrite with a mix of a valid and an
        # uncastable string value.
        df = df.with_columns(pl.Series("down", ["1", "n/a"]))
        out, report = conform_to_canonical(df, "hudl")
        assert out["down"].null_count() == 1
        assert report.cast_failures.get("down") == 1

    def test_unknown_source_columns_are_dropped_and_reported(self):
        df = _full_core_frame().with_columns(pl.lit("x").alias("totally_unknown_column"))
        out, report = conform_to_canonical(df, "hudl")
        assert "totally_unknown_column" not in out.columns
        assert "totally_unknown_column" in report.dropped_unknown

    def test_source_is_stamped_on_every_row(self):
        df = _full_core_frame().with_columns(pl.lit("something-else").alias("source"))
        out, _ = conform_to_canonical(df, "sportapp")
        assert out["source"].unique().to_list() == ["sportapp"]

    def test_nulls_in_non_null_columns_do_not_raise(self):
        df = _full_core_frame()
        df = df.with_columns(pl.lit(None).cast(pl.Int32).alias("down"))
        out, _ = conform_to_canonical(df, "hudl")
        assert out["down"].null_count() == out.height


def _scoring_play_frame(**overrides) -> pl.DataFrame:
    """A single-row frame with every flag column defaulted to 0."""
    row = {
        "posteam": "A",
        "defteam": "B",
        "touchdown": 0,
        "def_touchdown": 0,
        "one_point_conv_success": 0,
        "two_point_conv_success": 0,
        "defensive_two_point_conv": 0,
        "safety": 0,
    }
    row.update(overrides)
    return pl.DataFrame({k: [v] for k, v in row.items()})


class TestAddScoringPlayTeam:
    def test_offense_touchdown_credits_posteam_both_modes(self):
        df = _scoring_play_frame(touchdown=1)
        for credit_defense in (False, True):
            out = add_scoring_play_team(df, credit_defense=credit_defense)
            assert out["scoring_play"][0] == 1
            assert out["scoring_play_team"][0] == "A"

    def test_safety_null_when_credit_defense_false(self):
        df = _scoring_play_frame(safety=1)
        out = add_scoring_play_team(df, credit_defense=False)
        assert out["scoring_play"][0] == 1
        assert out["scoring_play_team"][0] is None

    def test_safety_credits_defteam_when_credit_defense_true(self):
        df = _scoring_play_frame(safety=1)
        out = add_scoring_play_team(df, credit_defense=True)
        assert out["scoring_play_team"][0] == "B"

    def test_def_touchdown_and_defensive_two_point_conv_credit_defteam(self):
        for flag in ("def_touchdown", "defensive_two_point_conv"):
            df = _scoring_play_frame(**{flag: 1})
            out = add_scoring_play_team(df, credit_defense=True)
            assert out["scoring_play_team"][0] == "B", flag

    def test_no_scoring_flags_leaves_scoring_play_zero_and_team_null(self):
        df = _scoring_play_frame()
        out = add_scoring_play_team(df, credit_defense=True)
        assert out["scoring_play"][0] == 0
        assert out["scoring_play_team"][0] is None


def _score_chain_frame(rows: list[dict]) -> pl.DataFrame:
    base_cols = [
        "game_id",
        "play_id",
        "home_team",
        "away_team",
        "posteam",
        "defteam",
        "scoring_play_team",
        "touchdown",
        "def_touchdown",
        "one_point_conv_success",
        "two_point_conv_success",
        "defensive_two_point_conv",
        "safety",
    ]
    data = {col: [] for col in base_cols}
    for row in rows:
        for col in base_cols:
            default = 0 if col not in (
                "game_id", "play_id", "home_team", "away_team",
                "posteam", "defteam", "scoring_play_team",
            ) else None
            data[col].append(row.get(col, default))
    return pl.DataFrame(data)


class TestAddScoreColumns:
    def test_one_home_touchdown_ends_home_6_away_0(self):
        df = _score_chain_frame(
            [
                {"game_id": "g1", "play_id": 1, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": None},
                {"game_id": "g1", "play_id": 2, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": "H", "touchdown": 1},
            ]
        )
        out = add_score_columns(df)
        assert out["home_team_score"].to_list() == [0, 6]
        assert out["away_team_score"].to_list() == [0, 0]
        assert out["posteam_score"].to_list() == [0, 6]
        assert out["defteam_score"].to_list() == [0, 0]

    def test_forward_fill_carries_running_score_and_play_id_1_starts_at_zero(self):
        df = _score_chain_frame(
            [
                {"game_id": "g1", "play_id": 1, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": None},
                {"game_id": "g1", "play_id": 2, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": None},
                {"game_id": "g1", "play_id": 3, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": "H", "touchdown": 1},
            ]
        )
        out = add_score_columns(df)
        assert out["home_team_score"].to_list() == [0, 0, 6]

    def test_score_differential_equals_posteam_minus_defteam_every_row(self):
        df = _score_chain_frame(
            [
                {"game_id": "g1", "play_id": 1, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": None},
                {"game_id": "g1", "play_id": 2, "home_team": "H", "away_team": "A",
                 "posteam": "A", "defteam": "H", "scoring_play_team": "A",
                 "two_point_conv_success": 1},
                {"game_id": "g1", "play_id": 3, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": "H", "touchdown": 1},
            ]
        )
        out = add_score_columns(df)
        for row in out.iter_rows(named=True):
            assert row["score_differential"] == row["posteam_score"] - row["defteam_score"]

    def test_defensive_safety_adds_two_to_defending_team_only_with_credit_defense(self):
        df = _scoring_play_frame_for_score_chain = _score_chain_frame(
            [
                {"game_id": "g1", "play_id": 1, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": None},
                {"game_id": "g1", "play_id": 2, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": None, "safety": 1},
            ]
        )
        # credit_defense=False: add_scoring_play_team leaves scoring_play_team
        # null on the safety row, so add_score_columns credits nobody.
        no_credit = add_scoring_play_team(df.drop("scoring_play_team"), credit_defense=False)
        out_no_credit = add_score_columns(no_credit)
        assert out_no_credit["away_team_score"].to_list() == [0, 0]
        assert out_no_credit["defteam_score"].to_list() == [0, 0]

        # credit_defense=True: the safety row's scoring_play_team becomes the
        # defending team ("A" == away_team), which scores 2.
        credit = add_scoring_play_team(df.drop("scoring_play_team"), credit_defense=True)
        out_credit = add_score_columns(credit)
        assert out_credit["away_team_score"].to_list() == [0, 2]
        assert out_credit["defteam_score"].to_list() == [0, 2]

    def test_play_1_home_touchdown_is_credited_not_zeroed(self):
        df = _score_chain_frame(
            [
                {"game_id": "g1", "play_id": 1, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": "H", "touchdown": 1},
            ]
        )
        out = add_score_columns(df)
        assert out["home_team_points"].to_list() == [6]
        assert out["home_team_score"].to_list() == [6]
        assert out["away_team_points"].to_list() == [0]
        assert out["away_team_score"].to_list() == [0]

    def test_play_1_away_score_is_credited_and_home_stays_zero(self):
        df = _score_chain_frame(
            [
                {"game_id": "g1", "play_id": 1, "home_team": "H", "away_team": "A",
                 "posteam": "A", "defteam": "H", "scoring_play_team": "A",
                 "two_point_conv_success": 1},
            ]
        )
        out = add_score_columns(df)
        assert out["away_team_points"].to_list() == [2]
        assert out["away_team_score"].to_list() == [2]
        assert out["home_team_points"].to_list() == [0]
        assert out["home_team_score"].to_list() == [0]

    def test_play_1_with_null_scoring_play_team_still_seeds_both_teams_zero(self):
        df = _score_chain_frame(
            [
                {"game_id": "g1", "play_id": 1, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": None},
            ]
        )
        out = add_score_columns(df)
        assert out["home_team_points"].to_list() == [0]
        assert out["away_team_points"].to_list() == [0]
        assert out["home_team_score"].to_list() == [0]
        assert out["away_team_score"].to_list() == [0]

    def test_play_1_defensive_safety_credits_defending_team_when_credit_defense_true(self):
        df = _score_chain_frame(
            [
                {"game_id": "g1", "play_id": 1, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": None, "safety": 1},
            ]
        )
        credited = add_scoring_play_team(df.drop("scoring_play_team"), credit_defense=True)
        out = add_score_columns(credited)
        assert out["away_team_points"].to_list() == [2]
        assert out["away_team_score"].to_list() == [2]
        assert out["defteam_score"].to_list() == [2]

    def test_two_games_do_not_leak_cumulative_totals(self):
        df = _score_chain_frame(
            [
                {"game_id": "g1", "play_id": 1, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": None},
                {"game_id": "g1", "play_id": 2, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": "H", "touchdown": 1},
                {"game_id": "g2", "play_id": 1, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": None},
                {"game_id": "g2", "play_id": 2, "home_team": "H", "away_team": "A",
                 "posteam": "H", "defteam": "A", "scoring_play_team": None},
            ]
        )
        out = add_score_columns(df)
        g2 = out.filter(pl.col("game_id") == "g2")
        assert g2["home_team_score"].to_list() == [0, 0]
        g1 = out.filter(pl.col("game_id") == "g1")
        assert g1["home_team_score"].to_list() == [0, 6]


class TestCanonicalPlaysFactory:
    def test_default_frame_passes_conform_with_empty_report(self):
        from flag_football_ep.testing import canonical_plays

        df = canonical_plays()
        out, report = conform_to_canonical(df, "hudl")
        assert out.columns == list(CANONICAL_COLUMNS)
        assert report.missing_core == []
        assert report.materialized_extras == []
        assert report.cast_failures == {}

    def test_overrides_scalar_applies_to_every_row(self):
        from flag_football_ep.testing import canonical_plays

        df = canonical_plays(overrides={"down": 7})
        assert df["down"].to_list() == [7] * df.height

    def test_overrides_unknown_column_raises(self):
        from flag_football_ep.testing import canonical_plays

        with pytest.raises(ValueError):
            canonical_plays(overrides={"not_a_column": 1})

    def test_n_games_and_plays_per_game_produce_expected_height_and_contiguous_play_ids(self):
        from flag_football_ep.testing import canonical_plays

        df = canonical_plays(n_games=2, plays_per_game=6)
        assert df.height == 12
        for game_id in df["game_id"].unique().to_list():
            play_ids = df.filter(pl.col("game_id") == game_id)["play_id"].to_list()
            assert play_ids == list(range(1, 7))

    def test_canonical_plays_with_scores_imports_and_runs(self):
        from flag_football_ep.testing import canonical_plays_with_scores

        df = canonical_plays_with_scores(n_games=1, plays_per_game=4)
        assert "score_differential" in df.columns
        assert df.height == 4
