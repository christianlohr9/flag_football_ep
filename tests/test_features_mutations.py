"""Unit coverage for `flag_football_ep.features.mutations`.

Every test builds inputs with `flag_football_ep.testing.canonical_plays_with_scores` plus
targeted `overrides`/`extras`, matching the frozen notebook baseline behaviour documented in
`tests/fixtures/baseline_manifest.json`.
"""

from __future__ import annotations

import polars as pl
import pytest

from flag_football_ep.testing import canonical_plays_with_scores
from flag_football_ep.features.mutations import (
    EP_PROBABILITY_COLUMNS,
    PAT_BASELINE_ONE_POINT,
    PAT_BASELINE_TWO_POINT,
    MissingFeatureColumns,
    add_ep_variables,
    add_wp_variables,
    prepare_ep_data,
    prepare_wp_data,
)

_EP_PROBS = {
    "Touchdown_Prob": 0.2,
    "Opp_Touchdown_Prob": 0.2,
    "Safety_Prob": 0.2,
    "Opp_Safety_Prob": 0.2,
    "No_Score_Prob": 0.2,
}


def _with_ep_probs(df: pl.DataFrame, **overrides: float) -> pl.DataFrame:
    probs = {**_EP_PROBS, **overrides}
    return df.with_columns([pl.lit(value).alias(name) for name, value in probs.items()])


_MINIMAL_EP_ROW_DEFAULTS = {
    "game_id": "G1",
    "half": 1,
    "half_end": 0,
    "game_end": 0,
    "posteam": "HOME",
    "defteam": "AWAY",
    "home_team": "HOME",
    "away_team": "AWAY",
    "interception": 0,
    "touchdown": 0,
    "one_point_conv_success": 0,
    "two_point_conv_success": 0,
    "defensive_two_point_conv": 0,
    "safety": 0,
    "scoring_play_team": None,
    "scoring_play": 0,
    "play_type": "pass",
    "down": 1,
    "yards_to_go": 10,
    **_EP_PROBS,
}


def _minimal_ep_frame(rows: list[dict]) -> pl.DataFrame:
    """A hand-built frame with exactly the columns `add_ep_variables` needs.

    Used where the epa formula needs to be isolated from the rest of the
    `prepare_ep_data`/`add_scoring_play_team` machinery (e.g. proving the interception
    sign flip against an otherwise-identical row, or forcing `scoring_play_team` to differ
    from `posteam` on a touchdown row).
    """
    full_rows = [{**_MINIMAL_EP_ROW_DEFAULTS, **row} for row in rows]
    columns = list(_MINIMAL_EP_ROW_DEFAULTS)
    data = {col: [row[col] for row in full_rows] for col in columns}
    return pl.DataFrame(data)


class TestPrepareEpDataIndex:
    def test_creates_index_when_absent(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)
        assert "index" not in df.columns

        out = prepare_ep_data(df)

        assert "index" in out.columns
        assert out["index"].to_list() == list(range(1, 9))

    def test_reuses_existing_index_when_present(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8).with_row_index(
            name="index", offset=100
        )

        out = prepare_ep_data(df)

        assert out["index"].to_list() == list(range(100, 108))


class TestHalfEndFlags:
    def test_half_end_flags(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)

        out = prepare_ep_data(df)

        # Exactly one half_end==1 row per (game_id, half).
        counts = (
            out.filter(pl.col("half_end") == 1)
            .group_by(["game_id", "half"])
            .agg(pl.len().alias("n"))
        )
        assert set(counts["n"].to_list()) == {1}
        assert set(counts["half"].to_list()) == {1, 2}

        # It lands on the last play_id of each half.
        half1_end = out.filter((pl.col("half") == 1) & (pl.col("half_end") == 1))
        half2_end = out.filter((pl.col("half") == 2) & (pl.col("half_end") == 1))
        assert half1_end["play_id"].item() == 4
        assert half2_end["play_id"].item() == 8

    def test_game_end_flag(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)

        out = prepare_ep_data(df)

        game_end_rows = out.filter(pl.col("game_end") == 1)
        assert game_end_rows.height == 1
        assert game_end_rows["half"].item() == 2
        assert game_end_rows["half_end"].item() == 1

    def test_half_1_end_is_not_game_end(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)

        out = prepare_ep_data(df)

        half1_end_row = out.filter((pl.col("half") == 1) & (pl.col("half_end") == 1))
        assert half1_end_row["game_end"].item() == 0

    def test_two_game_no_leakage_half_end(self):
        df = canonical_plays_with_scores(n_games=2, plays_per_game=8)

        out = prepare_ep_data(df)

        for game_id in out["game_id"].unique().to_list():
            game_out = out.filter(pl.col("game_id") == game_id)
            counts = (
                game_out.filter(pl.col("half_end") == 1)
                .group_by("half")
                .agg(pl.len().alias("n"))
            )
            assert set(counts["n"].to_list()) == {1}
            assert set(counts["half"].to_list()) == {1, 2}


class TestScoringEventAndNextScoreHalf:
    def test_scoring_event_maps_touchdown(self):
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=8,
            overrides={"touchdown": [0, 0, 0, 1, 0, 0, 0, 0]},
        )

        out = prepare_ep_data(df)

        scoring_row = out.filter(pl.col("play_id") == 4)
        assert scoring_row["scoring_event"].item() == "Touchdown"

    def test_scoring_event_maps_def_touchdown_to_touchdown(self):
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=8,
            overrides={"def_touchdown": [0, 0, 0, 1, 0, 0, 0, 0]},
        )

        out = prepare_ep_data(df)

        scoring_row = out.filter(pl.col("play_id") == 4)
        assert scoring_row["scoring_event"].item() == "Touchdown"

    def test_scoring_event_maps_safety(self):
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=8,
            overrides={"safety": [0, 0, 0, 1, 0, 0, 0, 0]},
        )

        out = prepare_ep_data(df)

        scoring_row = out.filter(pl.col("play_id") == 4)
        assert scoring_row["scoring_event"].item() == "Safety"

    def test_scoring_event_maps_one_point_conv(self):
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=8,
            overrides={"one_point_conv_success": [0, 0, 0, 1, 0, 0, 0, 0]},
        )

        out = prepare_ep_data(df)

        scoring_row = out.filter(pl.col("play_id") == 4)
        assert scoring_row["scoring_event"].item() == "Extra_Point"

    def test_scoring_event_maps_two_point_conv(self):
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=8,
            overrides={"two_point_conv_success": [0, 0, 0, 1, 0, 0, 0, 0]},
        )

        out = prepare_ep_data(df)

        scoring_row = out.filter(pl.col("play_id") == 4)
        assert scoring_row["scoring_event"].item() == "Two_Point_Conversion"

    def test_no_score_marked_at_half_end_when_nothing_scored(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)

        out = prepare_ep_data(df)

        half1_end = out.filter((pl.col("half") == 1) & (pl.col("half_end") == 1))
        assert half1_end["scoring_event"].item() == "No_Score"
        assert half1_end["Next_Score_Half"].item() == "No_Score"

    def test_next_score_half_touchdown_when_posteam_scores(self):
        # play_id 4 has posteam=AWAY (play_idx % 2 == 1 -> HOME, else AWAY).
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=8,
            overrides={"touchdown": [0, 0, 0, 1, 0, 0, 0, 0]},
        )

        out = prepare_ep_data(df)

        away_rows = out.filter((pl.col("half") == 1) & (pl.col("posteam") == "AWAY"))
        assert set(away_rows["Next_Score_Half"].to_list()) == {"Touchdown"}

    def test_next_score_half_opp_touchdown_when_not_posteam(self):
        # play_id 4 (the scoring play) has posteam=AWAY; HOME rows in the same half
        # should see "Opp_Touchdown" since the backward-filled scoring team isn't theirs.
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=8,
            overrides={"touchdown": [0, 0, 0, 1, 0, 0, 0, 0]},
        )

        out = prepare_ep_data(df)

        home_rows = out.filter((pl.col("half") == 1) & (pl.col("posteam") == "HOME"))
        assert set(home_rows["Next_Score_Half"].to_list()) == {"Opp_Touchdown"}

    def test_drive_score_half_no_score_uses_own_drive_id(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)

        out = prepare_ep_data(df)

        no_score_rows = out.filter(pl.col("Next_Score_Half") == "No_Score")
        assert (no_score_rows["Drive_Score_Half"] == no_score_rows["drive_id"]).all()

    def test_max_play_id_per_half(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)

        out = prepare_ep_data(df)

        half1_rows = out.filter(pl.col("half") == 1)
        half2_rows = out.filter(pl.col("half") == 2)
        assert set(half1_rows["max_play_id"].to_list()) == {4}
        assert set(half2_rows["max_play_id"].to_list()) == {8}


class TestPrepareWpData:
    def test_half_seconds_remaining_starts_at_1200_and_is_non_increasing(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)

        out = prepare_wp_data(df)

        for half in (1, 2):
            half_rows = out.filter(pl.col("half") == half).sort("play_id")
            values = half_rows["half_seconds_remaining"].to_list()
            assert values[0] == 1200
            assert all(a >= b for a, b in zip(values, values[1:]))

    def test_game_seconds_remaining_starts_at_2400(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)

        out = prepare_wp_data(df)

        first_row = out.filter(pl.col("play_id") == 1)
        assert first_row["game_seconds_remaining"].item() == 2400

    def test_receive_2h_ko_zero_for_first_play_posteam(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)

        out = prepare_wp_data(df)

        first_play_posteam = out.filter(pl.col("play_id") == 1)["posteam"].item()
        matching_rows = out.filter(pl.col("posteam") == first_play_posteam)
        other_rows = out.filter(pl.col("posteam") != first_play_posteam)
        assert set(matching_rows["receive_2h_ko"].to_list()) == {0}
        assert set(other_rows["receive_2h_ko"].to_list()) == {1}

    def test_diff_time_ratio_formula(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)

        out = prepare_wp_data(df)

        expected = out["score_differential"] / (-4 * out["elapsed_share"]).exp()
        assert out["Diff_Time_Ratio"].to_list() == pytest.approx(expected.to_list())

    def test_two_game_no_leakage_clock(self):
        df = canonical_plays_with_scores(n_games=2, plays_per_game=8)

        out = prepare_wp_data(df)

        for game_id in out["game_id"].unique().to_list():
            game_rows = out.filter(pl.col("game_id") == game_id).sort("play_id")
            half1_first = game_rows.filter(pl.col("half") == 1).sort("play_id")[
                "half_seconds_remaining"
            ][0]
            half2_first = game_rows.filter(pl.col("half") == 2).sort("play_id")[
                "half_seconds_remaining"
            ][0]
            assert half1_first == 1200
            assert half2_first == 1200


class TestAddEpVariables:
    def test_exp_pts_matches_hand_computed_weighted_sum(self):
        df = _with_ep_probs(
            _minimal_ep_frame([{}]),
            Touchdown_Prob=0.5,
            Opp_Touchdown_Prob=0.1,
            Safety_Prob=0.1,
            Opp_Safety_Prob=0.1,
            No_Score_Prob=0.2,
        )

        out = add_ep_variables(df)

        expected = 0 * 0.2 + 2 * 0.1 + 6 * 0.5 + (-2 * 0.1) + (-6 * 0.1)
        assert out["ExpPts"].item() == pytest.approx(expected)

    def test_interception_flips_epa_sign_vs_identical_non_interception_row(self):
        row0 = {"posteam": "HOME"}
        row1 = {
            "posteam": "HOME",
            "half_end": 1,
            "Touchdown_Prob": 0.0,
            "No_Score_Prob": 1.0,
            "Opp_Touchdown_Prob": 0.0,
            "Safety_Prob": 0.0,
            "Opp_Safety_Prob": 0.0,
        }
        row0_probs = {
            "Touchdown_Prob": 1.0,
            "No_Score_Prob": 0.0,
            "Opp_Touchdown_Prob": 0.0,
            "Safety_Prob": 0.0,
            "Opp_Safety_Prob": 0.0,
        }

        non_intercept = _minimal_ep_frame(
            [{**row0, **row0_probs, "interception": 0}, row1]
        )
        intercept = _minimal_ep_frame([{**row0, **row0_probs, "interception": 1}, row1])

        epa_non_intercept = add_ep_variables(non_intercept)["epa"][0]
        epa_intercept = add_ep_variables(intercept)["epa"][0]

        assert epa_non_intercept != 0
        assert epa_intercept == pytest.approx(-epa_non_intercept)

    def test_touchdown_epa_when_scoring_team_is_posteam(self):
        df = _minimal_ep_frame(
            [
                {
                    "touchdown": 1,
                    "scoring_play_team": "HOME",
                    "posteam": "HOME",
                    "Touchdown_Prob": 1.0,
                    "No_Score_Prob": 0.0,
                }
            ]
        )

        out = add_ep_variables(df)

        assert out["epa"].item() == pytest.approx(6 - out["ep"].item())

    def test_touchdown_epa_when_scoring_team_is_not_posteam(self):
        df = _minimal_ep_frame(
            [
                {
                    "touchdown": 1,
                    "scoring_play_team": "AWAY",
                    "posteam": "HOME",
                    "Touchdown_Prob": 1.0,
                    "No_Score_Prob": 0.0,
                }
            ]
        )

        out = add_ep_variables(df)

        assert out["epa"].item() == pytest.approx(-6 - out["ep"].item())

    def test_pat_baselines_preserved(self):
        assert PAT_BASELINE_ONE_POINT == 0.5
        assert PAT_BASELINE_TWO_POINT == 0.92

    def test_missing_probability_column_raises_named(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)
        df = prepare_ep_data(df)
        df = _with_ep_probs(df)
        df = df.drop("Safety_Prob")

        with pytest.raises(MissingFeatureColumns) as excinfo:
            add_ep_variables(df)

        assert "Safety_Prob" in str(excinfo.value)

    def test_add_ep_variables_does_not_mutate_input(self):
        df = _with_ep_probs(_minimal_ep_frame([{}]))
        original_columns = list(df.columns)

        add_ep_variables(df)

        assert df.columns == original_columns
        assert "ExpPts" not in df.columns


class TestAddWpVariables:
    def test_home_wp_plus_away_wp_equals_one_every_row(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)
        df = prepare_ep_data(df)
        df = df.with_columns(wp=pl.lit(0.5))

        out = add_wp_variables(df)

        totals = (out["home_wp"] + out["away_wp"]).to_list()
        assert totals == pytest.approx([1.0] * out.height)

    def test_def_wp_is_one_minus_wp(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)
        df = prepare_ep_data(df)
        df = df.with_columns(wp=pl.lit(0.3))

        out = add_wp_variables(df)

        assert out["def_wp"].to_list() == pytest.approx([0.7] * out.height)

    def test_game_end_final_value_home_win(self):
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=8,
            overrides={"touchdown": [0, 0, 0, 0, 0, 0, 0, 1]},
        )
        df = prepare_ep_data(df)
        df = df.with_columns(wp=pl.lit(0.5))

        out = add_wp_variables(df)

        game_end_row = out.filter(pl.col("game_end") == 1)
        # play_id 8 has posteam=AWAY (even play_idx); the AWAY-scored touchdown makes
        # away_team_score > home_team_score, so home should be the loser (final_value 0).
        assert game_end_row["final_value"].item() == 0
        assert game_end_row["home_wp"].item() == 0

    def test_missing_wp_column_raises_named(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)
        df = prepare_ep_data(df)

        with pytest.raises(MissingFeatureColumns) as excinfo:
            add_wp_variables(df)

        assert "wp" in str(excinfo.value)

    def test_add_wp_variables_does_not_mutate_input(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)
        df = prepare_ep_data(df).with_columns(wp=pl.lit(0.5))
        original_columns = list(df.columns)

        add_wp_variables(df)

        assert df.columns == original_columns
        assert "home_wp" not in df.columns
