"""Unit coverage for `flag_football_ep.features.mutations`.

Every test builds inputs with `flag_football_ep.testing.canonical_plays_with_scores` plus
targeted `overrides`/`extras`, matching the frozen notebook baseline behaviour documented in
`tests/fixtures/baseline_manifest.json`.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.testing import canonical_plays, canonical_plays_with_scores
from flag_football_ep.features.mutations import (
    EP_HALF_UNKNOWN_SENTINEL,
    EP_PROBABILITY_COLUMNS,
    FOURTH_DOWN_DISTANCE_BUCKETS,
    HC_SOURCE_PREFIX,
    DegenerateWeightRange,
    InsufficientPatAttempts,
    InvalidGameClockValues,
    InvalidGameDateValues,
    MissingFeatureColumns,
    PatBaselines,
    add_competition_tier_features,
    add_ep_variables,
    add_recency_weight,
    add_wp_variables,
    estimate_fourth_down_rates,
    estimate_pat_baselines,
    make_ep_model_mutations,
    make_wp_model_mutations,
    prepare_ep_data,
    prepare_wp_data,
)
from flag_football_ep.model.hyperparams import (
    RECENCY_HALF_LIFE_DAYS_GRID,
    RECENCY_NULL_DATE_WEIGHT,
    WP_SELECTED_COLUMNS,
)
from flag_football_ep.reference import COMPETITION_TIERS, UnmappedCompetitionError

_EP_MODEL_COLUMNS = [
    "game_id",
    "play_id",
    "label",
    "down0",
    "down1",
    "down2",
    "down3",
    "down4",
    "Drive_Score_Dist",
    "Drive_Score_Dist_W",
    "ScoreDiff_W",
    "Total_W",
    "Total_W_Scaled",
]


def _multi_drive_ep_frame(n_games: int = 1, plays_per_game: int = 12) -> pl.DataFrame:
    """A frame with >1 drive per half and one mid-half touchdown, so Drive_Score_Dist and
    score_differential both vary (avoids the degenerate single-drive-per-half shape).
    """
    touchdown = [0] * plays_per_game
    touchdown[5] = 1  # play_id 6: mid-half, second drive of the half
    overrides = {"touchdown": touchdown * n_games}
    return prepare_ep_data(
        canonical_plays_with_scores(
            n_games=n_games, plays_per_game=plays_per_game, overrides=overrides
        )
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
    "play_id": 1,
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


_MINIMAL_WP_ROW_DEFAULTS = {
    "game_id": "G1",
    "play_id": 1,
    "posteam": "HOME",
    "defteam": "AWAY",
    "home_team": "HOME",
    "away_team": "AWAY",
    "home_team_score": 0,
    "away_team_score": 0,
    "game_end": 0,
    "wp": 0.5,
}


def _minimal_wp_frame(rows: list[dict]) -> pl.DataFrame:
    """A hand-built frame with exactly the columns `add_wp_variables` needs.

    Mirrors `_minimal_ep_frame`: used to isolate `add_wp_variables` from
    `prepare_ep_data`/`add_scoring_play_team` machinery, e.g. to build a two-game frame with
    an explicit `game_end=0` boundary row so the game_end null-out branch does not mask a
    cross-game-leakage assertion.
    """
    full_rows = [{**_MINIMAL_WP_ROW_DEFAULTS, **row} for row in rows]
    columns = list(_MINIMAL_WP_ROW_DEFAULTS)
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


class TestPrepareWpDataRealClock:
    """Coverage for `prepare_wp_data(..., real_clock=True)` (REQ-S1-09 IFAF sub-experiment,
    plan 01.3-08 Task 3)."""

    def _decreasing_clock(self, plays_per_game: int = 8) -> list[int]:
        """Monotonically-decreasing `game_clock_ms` per half, matching the plan's
        build-spec ("a small synthetic IFAF-sourced frame with a monotonically decreasing
        game_clock_ms")."""
        half_boundary = (plays_per_game + 1) // 2
        half1 = [1200000 - i * 150000 for i in range(half_boundary)]
        half2 = [1200000 - i * 150000 for i in range(plays_per_game - half_boundary)]
        return half1 + half2

    def _ifaf_frame(
        self, game_clock_ms: list | None = None, n_games: int = 1, plays_per_game: int = 8
    ) -> pl.DataFrame:
        extras = {} if game_clock_ms is None else {"game_clock_ms": game_clock_ms}
        return canonical_plays_with_scores(
            n_games=n_games, plays_per_game=plays_per_game, source="ifaf", extras=extras
        )

    def test_real_clock_default_parameter_is_false(self):
        import inspect

        assert inspect.signature(prepare_wp_data).parameters["real_clock"].default is False

    def test_no_kwarg_behavior_matches_explicit_real_clock_false(self):
        df = self._ifaf_frame(game_clock_ms=self._decreasing_clock())

        default_out = prepare_wp_data(df)
        explicit_false_out = prepare_wp_data(df, real_clock=False)

        assert (
            default_out["half_seconds_remaining"].to_list()
            == explicit_false_out["half_seconds_remaining"].to_list()
        )

    def test_real_clock_half_seconds_remaining_derived_from_game_clock_ms(self):
        clock = self._decreasing_clock()
        df = self._ifaf_frame(game_clock_ms=clock)

        out = prepare_wp_data(df, real_clock=True).sort("play_id")

        expected = [c / 1000.0 for c in clock]
        assert out["half_seconds_remaining"].to_list() == pytest.approx(expected)

    def test_real_clock_half_seconds_remaining_non_increasing_within_half(self):
        df = self._ifaf_frame(game_clock_ms=self._decreasing_clock())

        out = prepare_wp_data(df, real_clock=True)

        for half in (1, 2):
            values = (
                out.filter(pl.col("half") == half)
                .sort("play_id")["half_seconds_remaining"]
                .to_list()
            )
            assert all(a >= b for a, b in zip(values, values[1:]))

    def test_real_clock_game_seconds_remaining_matches_half_seconds_in_second_half(self):
        df = self._ifaf_frame(game_clock_ms=self._decreasing_clock())

        out = prepare_wp_data(df, real_clock=True)

        half2_rows = out.filter(pl.col("half") == 2)
        assert half2_rows["game_seconds_remaining"].to_list() == pytest.approx(
            half2_rows["half_seconds_remaining"].to_list()
        )

    def test_real_clock_game_seconds_remaining_is_1200_plus_half_seconds_in_first_half(self):
        df = self._ifaf_frame(game_clock_ms=self._decreasing_clock())

        out = prepare_wp_data(df, real_clock=True)

        half1_rows = out.filter(pl.col("half") == 1)
        expected = (half1_rows["half_seconds_remaining"] + 1200).to_list()
        assert half1_rows["game_seconds_remaining"].to_list() == pytest.approx(expected)

    def test_real_clock_diff_time_ratio_same_formula_as_synthetic_path(self):
        df = self._ifaf_frame(game_clock_ms=self._decreasing_clock())

        out = prepare_wp_data(df, real_clock=True)

        expected = out["score_differential"] / (-4 * out["elapsed_share"]).exp()
        assert out["Diff_Time_Ratio"].to_list() == pytest.approx(expected.to_list())

    def test_real_clock_missing_game_clock_ms_column_raises_named(self):
        df = self._ifaf_frame(game_clock_ms=self._decreasing_clock()).drop("game_clock_ms")

        with pytest.raises(MissingFeatureColumns, match="game_clock_ms"):
            prepare_wp_data(df, real_clock=True)

    def test_real_clock_null_game_clock_ms_raises_named_reporting_count(self):
        clock = self._decreasing_clock()
        clock[0] = None
        clock[1] = None
        df = self._ifaf_frame(game_clock_ms=clock)

        with pytest.raises(InvalidGameClockValues, match="2"):
            prepare_wp_data(df, real_clock=True)

    def test_real_clock_does_not_affect_default_synthetic_path(self):
        # Regression: adding the real_clock branch must not alter the untouched synthetic
        # path's output for the same input frame.
        df = self._ifaf_frame(game_clock_ms=self._decreasing_clock())

        synthetic_out = prepare_wp_data(df)

        assert synthetic_out["half_seconds_remaining"][0] == 1200


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

        out = add_ep_variables(df, pat_baselines=PatBaselines.legacy_notebook())

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

        epa_non_intercept = add_ep_variables(
            non_intercept, pat_baselines=PatBaselines.legacy_notebook()
        )["epa"][0]
        epa_intercept = add_ep_variables(
            intercept, pat_baselines=PatBaselines.legacy_notebook()
        )["epa"][0]

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

        out = add_ep_variables(df, pat_baselines=PatBaselines.legacy_notebook())

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

        out = add_ep_variables(df, pat_baselines=PatBaselines.legacy_notebook())

        assert out["epa"].item() == pytest.approx(-6 - out["ep"].item())

    def test_missing_probability_column_raises_named(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8)
        df = prepare_ep_data(df)
        df = _with_ep_probs(df)
        df = df.drop("Safety_Prob")

        with pytest.raises(MissingFeatureColumns) as excinfo:
            add_ep_variables(df, pat_baselines=PatBaselines.legacy_notebook())

        assert "Safety_Prob" in str(excinfo.value)

    def test_add_ep_variables_does_not_mutate_input(self):
        df = _with_ep_probs(_minimal_ep_frame([{}]))
        original_columns = list(df.columns)

        add_ep_variables(df, pat_baselines=PatBaselines.legacy_notebook())

        assert df.columns == original_columns
        assert "ExpPts" not in df.columns

    def test_add_ep_variables_requires_pat_baselines_keyword(self):
        df = _with_ep_probs(_minimal_ep_frame([{}]))

        with pytest.raises(TypeError):
            add_ep_variables(df)

    def test_add_ep_variables_with_legacy_notebook_baselines_reproduces_pre_change_epa(self):
        df = _minimal_ep_frame(
            [
                {
                    "down": 0,
                    "yards_to_go": 3,
                    "one_point_conv_success": 0,
                    "Touchdown_Prob": 0.0,
                    "No_Score_Prob": 1.0,
                }
            ]
        )

        out = add_ep_variables(df, pat_baselines=PatBaselines.legacy_notebook())

        assert out["epa"].item() == pytest.approx(0 - 0.5)

    def test_add_ep_variables_uses_estimated_rates_on_all_four_pat_branches(self):
        baselines = PatBaselines(
            one_point_rate=0.6,
            one_point_attempts=10,
            one_point_successes=6,
            one_point_ci=(0.4, 0.8),
            two_point_rate=0.3,
            two_point_attempts=10,
            two_point_successes=3,
            two_point_ci=(0.1, 0.5),
        )

        one_pt_success = _minimal_ep_frame(
            [
                {
                    "down": 0,
                    "yards_to_go": 3,
                    "one_point_conv_success": 1,
                    "Touchdown_Prob": 0.0,
                    "No_Score_Prob": 1.0,
                }
            ]
        )
        one_pt_fail = _minimal_ep_frame(
            [
                {
                    "down": 0,
                    "yards_to_go": 3,
                    "one_point_conv_success": 0,
                    "Touchdown_Prob": 0.0,
                    "No_Score_Prob": 1.0,
                }
            ]
        )
        two_pt_success = _minimal_ep_frame(
            [
                {
                    "down": 0,
                    "yards_to_go": 8,
                    "two_point_conv_success": 1,
                    "Touchdown_Prob": 0.0,
                    "No_Score_Prob": 1.0,
                }
            ]
        )
        two_pt_fail = _minimal_ep_frame(
            [
                {
                    "down": 0,
                    "yards_to_go": 8,
                    "two_point_conv_success": 0,
                    "Touchdown_Prob": 0.0,
                    "No_Score_Prob": 1.0,
                }
            ]
        )

        assert add_ep_variables(one_pt_success, pat_baselines=baselines)[
            "epa"
        ].item() == pytest.approx(1 - baselines.one_point_rate)
        assert add_ep_variables(one_pt_fail, pat_baselines=baselines)[
            "epa"
        ].item() == pytest.approx(0 - baselines.one_point_rate)
        assert add_ep_variables(two_pt_success, pat_baselines=baselines)[
            "epa"
        ].item() == pytest.approx(2 - baselines.two_point_rate)
        assert add_ep_variables(two_pt_fail, pat_baselines=baselines)[
            "epa"
        ].item() == pytest.approx(0 - baselines.two_point_rate)


class TestEstimatePatBaselines:
    def _pat_frame(self, one_point_rows: list[dict], two_point_rows: list[dict]) -> pl.DataFrame:
        one_point_defaults = {
            "down": 0,
            "yards_to_go": 3,
            "one_point_conv_success": 0,
            "two_point_conv_success": 0,
        }
        two_point_defaults = {
            "down": 0,
            "yards_to_go": 8,
            "one_point_conv_success": 0,
            "two_point_conv_success": 0,
        }
        rows = [{**one_point_defaults, **row} for row in one_point_rows] + [
            {**two_point_defaults, **row} for row in two_point_rows
        ]
        columns = ["down", "yards_to_go", "one_point_conv_success", "two_point_conv_success"]
        data = {col: [row[col] for row in rows] for col in columns}
        return pl.DataFrame(data)

    def test_counts_and_rate_match_hand_computed_values(self):
        df = self._pat_frame(
            one_point_rows=[
                {"one_point_conv_success": 1},
                {"one_point_conv_success": 1},
                {"one_point_conv_success": 0},
                {"one_point_conv_success": 0},
            ],
            two_point_rows=[
                {"two_point_conv_success": 1},
                {"two_point_conv_success": 0},
            ],
        )

        baselines = estimate_pat_baselines(df)

        assert baselines.one_point_attempts == 4
        assert baselines.one_point_successes == 2
        assert baselines.one_point_rate == pytest.approx(0.5)
        assert baselines.two_point_attempts == 2
        assert baselines.two_point_successes == 1
        assert baselines.two_point_rate == pytest.approx(0.5)

    def test_ci_brackets_the_rate(self):
        df = self._pat_frame(
            one_point_rows=[{"one_point_conv_success": 1}] * 6
            + [{"one_point_conv_success": 0}] * 4,
            two_point_rows=[{"two_point_conv_success": 1}] * 3
            + [{"two_point_conv_success": 0}] * 7,
        )

        baselines = estimate_pat_baselines(df)

        assert baselines.one_point_ci is not None
        assert baselines.one_point_ci[0] <= baselines.one_point_rate <= baselines.one_point_ci[1]
        assert baselines.two_point_ci is not None
        assert baselines.two_point_ci[0] <= baselines.two_point_rate <= baselines.two_point_ci[1]

    def test_zero_one_point_attempts_raises_insufficient_pat_attempts(self):
        df = self._pat_frame(
            one_point_rows=[],
            two_point_rows=[{"two_point_conv_success": 1}],
        )

        with pytest.raises(InsufficientPatAttempts) as excinfo:
            estimate_pat_baselines(df)

        assert "one-point" in str(excinfo.value)

    def test_zero_two_point_attempts_raises_insufficient_pat_attempts(self):
        df = self._pat_frame(
            one_point_rows=[{"one_point_conv_success": 1}],
            two_point_rows=[],
        )

        with pytest.raises(InsufficientPatAttempts) as excinfo:
            estimate_pat_baselines(df)

        assert "two-point" in str(excinfo.value)


class TestEstimateFourthDownRates:
    _DEFAULTS = {
        "down": 4,
        "yards_to_go": 3,
        "play_type": "run",
        "penalty": 0,
        "first_down": 0,
        "touchdown": 0,
    }

    def _fourth_down_frame(self, rows: list[dict]) -> pl.DataFrame:
        full_rows = [{**self._DEFAULTS, **row} for row in rows]
        columns = list(self._DEFAULTS)
        data = {col: [row[col] for row in full_rows] for col in columns}
        return pl.DataFrame(data)

    def test_no_fourth_down_rows_returns_zero_attempts_and_no_raise(self):
        df = self._fourth_down_frame([{"down": 1}, {"down": 2}, {"down": 3}])

        rates = estimate_fourth_down_rates(df)

        assert rates.total_attempts == 0
        assert rates.total_conversions == 0
        assert rates.overall_rate is None
        assert len(rates.buckets) == len(FOURTH_DOWN_DISTANCE_BUCKETS)
        assert all(bucket.rate is None and bucket.ci is None for bucket in rates.buckets)

    def test_bucket_assignment_at_every_boundary(self):
        boundary_distances = [1, 3, 4, 6, 7, 10, 11, 25]
        df = self._fourth_down_frame([{"yards_to_go": d} for d in boundary_distances])

        rates = estimate_fourth_down_rates(df)

        by_label = {bucket.label: bucket for bucket in rates.buckets}
        assert by_label["1-3"].attempts == 2  # 1, 3
        assert by_label["4-6"].attempts == 2  # 4, 6
        assert by_label["7-10"].attempts == 2  # 7, 10
        assert by_label["11+"].attempts == 2  # 11, 25
        assert rates.total_attempts == 8

    def test_touchdown_with_first_down_zero_counts_as_conversion(self):
        df = self._fourth_down_frame(
            [{"yards_to_go": 3, "touchdown": 1, "first_down": 0}]
        )

        rates = estimate_fourth_down_rates(df)

        by_label = {bucket.label: bucket for bucket in rates.buckets}
        assert by_label["1-3"].attempts == 1
        assert by_label["1-3"].conversions == 1

    def test_first_down_without_touchdown_counts_as_conversion(self):
        df = self._fourth_down_frame(
            [{"yards_to_go": 5, "first_down": 1, "touchdown": 0}]
        )

        rates = estimate_fourth_down_rates(df)

        by_label = {bucket.label: bucket for bucket in rates.buckets}
        assert by_label["4-6"].attempts == 1
        assert by_label["4-6"].conversions == 1

    def test_no_play_rows_excluded_from_attempts(self):
        df = self._fourth_down_frame(
            [{"play_type": "no_play"}, {"play_type": "run"}]
        )

        rates = estimate_fourth_down_rates(df)

        assert rates.total_attempts == 1

    def test_penalty_rows_excluded_from_attempts(self):
        df = self._fourth_down_frame([{"penalty": 1}, {"penalty": 0}])

        rates = estimate_fourth_down_rates(df)

        assert rates.total_attempts == 1

    def test_down_zero_pat_rows_excluded(self):
        df = self._fourth_down_frame(
            [{"down": 0, "play_type": "extra_point"}, {"down": 4}]
        )

        rates = estimate_fourth_down_rates(df)

        assert rates.total_attempts == 1

    def test_yards_to_go_below_one_or_null_excluded_from_every_bucket(self):
        df = self._fourth_down_frame(
            [{"yards_to_go": 0}, {"yards_to_go": None}, {"yards_to_go": 5}]
        )

        rates = estimate_fourth_down_rates(df)

        assert rates.total_attempts == 1
        by_label = {bucket.label: bucket for bucket in rates.buckets}
        assert by_label["1-3"].attempts == 0

    def test_empty_bucket_has_none_rate_and_ci_and_does_not_raise(self):
        df = self._fourth_down_frame([{"yards_to_go": 3}])

        rates = estimate_fourth_down_rates(df)

        by_label = {bucket.label: bucket for bucket in rates.buckets}
        assert by_label["11+"].attempts == 0
        assert by_label["11+"].rate is None
        assert by_label["11+"].ci is None

    def test_buckets_tuple_always_full_length(self):
        df = self._fourth_down_frame([{"yards_to_go": 3}])

        rates = estimate_fourth_down_rates(df)

        assert len(rates.buckets) == len(FOURTH_DOWN_DISTANCE_BUCKETS)
        assert [bucket.label for bucket in rates.buckets] == [
            label for label, _, _ in FOURTH_DOWN_DISTANCE_BUCKETS
        ]

    def test_ci_brackets_rate_for_populated_bucket(self):
        df = self._fourth_down_frame(
            [{"yards_to_go": 3, "first_down": 1}] * 6
            + [{"yards_to_go": 3, "first_down": 0}] * 4
        )

        rates = estimate_fourth_down_rates(df)

        by_label = {bucket.label: bucket for bucket in rates.buckets}
        bucket = by_label["1-3"]
        assert bucket.ci is not None
        assert bucket.ci[0] <= bucket.rate <= bucket.ci[1]


def _two_game_ep_leak_frame() -> pl.DataFrame:
    """Game A (G1) has 2 plays; game A's last play has null EP probability columns.
    Game B (G2) has 1 play with non-null, distinct probability columns and a different
    home_team/away_team so a leaked tmp_posteam is observable.

    `half_end`/`game_end` are left at the `_MINIMAL_EP_ROW_DEFAULTS` default (0) on every
    row, including game A's last play -- WR-04's `ep`/`epa` null-out-at-game_end branches
    are orthogonal to the backward_fill/shift(-1) scoping under test here, and leaving
    game_end=0 keeps them from masking whether the leak actually happened.
    """
    null_probs = {name: None for name in EP_PROBABILITY_COLUMNS}
    rows = [
        {  # game A, play 1: ordinary row with a real ep value to fill from within-game.
            "game_id": "G1",
            "play_id": 1,
            "posteam": "HOME",
            "home_team": "HOME",
            "away_team": "AWAY",
            **{**_EP_PROBS, "Touchdown_Prob": 1.0, "No_Score_Prob": 0.0},
        },
        {  # game A, play 2 (last play of game A): no probabilities -> ep starts null.
            "game_id": "G1",
            "play_id": 2,
            "posteam": "HOME",
            "home_team": "HOME",
            "away_team": "AWAY",
            **null_probs,
        },
        {  # game B, play 1: distinct team codes and a real, non-null ep value.
            "game_id": "G2",
            "play_id": 1,
            "posteam": "HOME2",
            "home_team": "HOME2",
            "away_team": "AWAY2",
            **{**_EP_PROBS, "Touchdown_Prob": 1.0, "No_Score_Prob": 0.0},
        },
    ]
    return _minimal_ep_frame(rows)


class TestAddEpVariablesCrossGameLeakage:
    def test_ep_does_not_leak_across_game_boundary(self):
        df = _two_game_ep_leak_frame()

        out = add_ep_variables(df, pat_baselines=PatBaselines.legacy_notebook())

        boundary_row = out.filter((pl.col("game_id") == "G1") & (pl.col("play_id") == 2))
        # Nothing later in game A to backward-fill from -> stays null, not game B's ExpPts.
        assert boundary_row["ep"].item() is None

    def test_home_ep_after_is_null_not_next_games_first_play(self):
        df = _two_game_ep_leak_frame()

        out = add_ep_variables(df, pat_baselines=PatBaselines.legacy_notebook())

        boundary_row = out.filter((pl.col("game_id") == "G1") & (pl.col("play_id") == 2))
        # shift(-1) has no next row within game A -> null, not game B's first-play home_ep.
        assert boundary_row["home_ep_after"].item() is None


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


def _two_game_wp_leak_frame() -> pl.DataFrame:
    """Game A (G1) has 2 plays; game A's last play has a null `wp`. Game B (G2) has 1 play
    with a non-null, distinct `wp` and a different home_team/away_team so a leaked
    tmp_posteam is observable.

    `game_end` is left at the `_MINIMAL_WP_ROW_DEFAULTS` default (0) on every row, including
    game A's last play, for the same reason as `_two_game_ep_leak_frame`: the game_end
    null-out-at-game_end branch for `wpa`/`home_wp` is orthogonal to the backward_fill/
    shift(-1) scoping under test and would otherwise mask whether the leak happened.
    """
    rows = [
        {  # game A, play 1: ordinary row with a real wp to fill from within-game.
            "game_id": "G1",
            "play_id": 1,
            "posteam": "HOME",
            "home_team": "HOME",
            "away_team": "AWAY",
            "wp": 0.4,
        },
        {  # game A, play 2 (last play of game A): null wp.
            "game_id": "G1",
            "play_id": 2,
            "posteam": "HOME",
            "home_team": "HOME",
            "away_team": "AWAY",
            "wp": None,
        },
        {  # game B, play 1: distinct team codes and a real, non-null wp.
            "game_id": "G2",
            "play_id": 1,
            "posteam": "HOME2",
            "home_team": "HOME2",
            "away_team": "AWAY2",
            "wp": 0.9,
        },
    ]
    return _minimal_wp_frame(rows)


class TestAddWpVariablesCrossGameLeakage:
    def test_wp_does_not_leak_across_game_boundary(self):
        df = _two_game_wp_leak_frame()

        out = add_wp_variables(df)

        boundary_row = out.filter((pl.col("game_id") == "G1") & (pl.col("play_id") == 2))
        # Nothing later in game A to backward-fill from -> stays null, not game B's wp.
        assert boundary_row["wp"].item() is None

    def test_home_wp_after_is_null_not_next_games_first_play(self):
        df = _two_game_wp_leak_frame()

        out = add_wp_variables(df)

        boundary_row = out.filter((pl.col("game_id") == "G1") & (pl.col("play_id") == 2))
        # shift(-1) has no next row within game A -> null, not game B's first-play home_wp.
        assert boundary_row["home_wp_after"].item() is None


class TestMakeEpModelMutations:
    def test_label_mapping(self):
        n = 12
        touchdown = [0] * n
        touchdown[5] = 1
        df = prepare_ep_data(
            canonical_plays_with_scores(
                n_games=1, plays_per_game=n, overrides={"touchdown": touchdown}
            )
        )

        out = make_ep_model_mutations(df, _EP_MODEL_COLUMNS)

        # Every label present must be one of the five mapped values (Extra_Point /
        # Two_Point_Conversion / Opp_Two_Point_Conversion stay unmapped -> null, as in the
        # notebook's commented-out branches).
        assert set(out["label"].drop_nulls().unique().to_list()) <= {0, 1, 2, 3, 4}

    def test_down_dummies_one_hot_in_range(self):
        df = _multi_drive_ep_frame()

        out = make_ep_model_mutations(df, _EP_MODEL_COLUMNS)

        down_sum = out["down0"] + out["down1"] + out["down2"] + out["down3"] + out["down4"]
        assert set(down_sum.to_list()) == {1}

    def test_drive_score_dist_equals_drive_score_half_minus_drive_id(self):
        df = _multi_drive_ep_frame()

        out = make_ep_model_mutations(df, _EP_MODEL_COLUMNS)

        assert set(out["Drive_Score_Dist"].to_list()) == {0, 1}

    def test_total_w_scaled_has_no_nan_no_inf_and_is_bounded(self):
        df = _multi_drive_ep_frame(n_games=3, plays_per_game=12)

        out = make_ep_model_mutations(df, _EP_MODEL_COLUMNS)

        assert out["Total_W_Scaled"].is_nan().sum() == 0
        assert out["Total_W_Scaled"].is_infinite().sum() == 0
        assert out["Total_W_Scaled"].null_count() == 0
        assert out["Total_W_Scaled"].min() >= 0
        assert out["Total_W_Scaled"].max() <= 1

    def test_degenerate_single_game_raises(self):
        # A single game with no scoring at all: every row is "No_Score" on its own drive,
        # so Drive_Score_Dist is 0 everywhere -> zero range.
        df = prepare_ep_data(canonical_plays_with_scores(n_games=1, plays_per_game=8))

        with pytest.raises(DegenerateWeightRange):
            make_ep_model_mutations(df, _EP_MODEL_COLUMNS)

    def test_null_yardline_50_rows_are_dropped(self):
        df = _multi_drive_ep_frame()
        df = df.with_columns(
            pl.when(pl.col("play_id") == 1)
            .then(None)
            .otherwise(pl.col("yardline_50"))
            .alias("yardline_50")
        )

        out = make_ep_model_mutations(df, _EP_MODEL_COLUMNS)

        assert 1 not in out["play_id"].to_list()

    def test_null_yards_to_go_rows_are_dropped(self):
        df = _multi_drive_ep_frame()
        df = df.with_columns(
            pl.when(pl.col("play_id") == 1)
            .then(None)
            .otherwise(pl.col("yards_to_go"))
            .alias("yards_to_go")
        )

        out = make_ep_model_mutations(df, _EP_MODEL_COLUMNS)

        assert 1 not in out["play_id"].to_list()

    def test_output_columns_equal_selected_columns_in_order(self):
        df = _multi_drive_ep_frame()
        columns = ["play_id", "down2", "label", "game_id"]

        out = make_ep_model_mutations(df, columns)

        assert out.columns == columns

    def test_recency_weight_column_multiplies_into_total_w_before_scaling(self):
        df = _multi_drive_ep_frame(n_games=3, plays_per_game=12)
        # A constant recency factor of 0.5 everywhere must not change Total_W_Scaled at all
        # -- the min-max rescale is invariant to a uniform scalar multiplier.
        df = df.with_columns(Recency_W=pl.lit(0.5))

        without = make_ep_model_mutations(df, _EP_MODEL_COLUMNS)
        with_recency = make_ep_model_mutations(
            df, _EP_MODEL_COLUMNS, recency_weight_column="Recency_W"
        )

        assert with_recency["Total_W_Scaled"].to_list() == pytest.approx(
            without["Total_W_Scaled"].to_list()
        )

    def test_recency_weight_column_none_default_matches_prior_behavior(self):
        df = _multi_drive_ep_frame(n_games=3, plays_per_game=12)

        explicit_none = make_ep_model_mutations(
            df, _EP_MODEL_COLUMNS, recency_weight_column=None
        )
        default = make_ep_model_mutations(df, _EP_MODEL_COLUMNS)

        assert explicit_none["Total_W_Scaled"].to_list() == default["Total_W_Scaled"].to_list()

    def test_recency_weight_column_total_w_scaled_stays_bounded_no_nan_no_inf(self):
        df = _multi_drive_ep_frame(n_games=3, plays_per_game=12)
        df = df.with_columns(
            Recency_W=(0.5 ** (pl.col("play_id").cast(pl.Float64) / 90.0))
        )

        out = make_ep_model_mutations(df, _EP_MODEL_COLUMNS, recency_weight_column="Recency_W")

        assert out["Total_W_Scaled"].is_nan().sum() == 0
        assert out["Total_W_Scaled"].is_infinite().sum() == 0
        assert out["Total_W_Scaled"].null_count() == 0
        assert out["Total_W_Scaled"].min() >= 0
        assert out["Total_W_Scaled"].max() <= 1

    def test_recency_weight_column_changes_total_w_when_non_uniform(self):
        df = _multi_drive_ep_frame(n_games=3, plays_per_game=12)
        df = df.with_columns(
            Recency_W=(0.5 ** (pl.col("play_id").cast(pl.Float64) / 90.0))
        )

        without = make_ep_model_mutations(df, _EP_MODEL_COLUMNS)
        with_recency = make_ep_model_mutations(
            df, _EP_MODEL_COLUMNS, recency_weight_column="Recency_W"
        )

        assert with_recency["Total_W_Scaled"].to_list() != without["Total_W_Scaled"].to_list()


class TestMakeEpModelMutationsHalfSentinel:
    """M3-02 task 2: `make_ep_model_mutations` overwrites `half` to
    `EP_HALF_UNKNOWN_SENTINEL` for hc_workbook-sourced rows, immediately before the final
    column selection -- decoupling the model's feature input from the `half=2` label-
    construction sentinel pinned in `TestHalfSentinelLabelConstruction` above.
    """

    _HC_SOURCE = "hc_workbook:scoring-probability-by-situation-2023-2026:data"

    def _ep_frame(
        self,
        *,
        source: str = "hudl",
        n_games: int = 1,
        plays_per_game: int = 12,
        half: int | None = None,
    ) -> pl.DataFrame:
        touchdown = [0] * plays_per_game
        touchdown[5] = 1
        overrides: dict = {"touchdown": touchdown * n_games}
        if half is not None:
            overrides["half"] = half
        return prepare_ep_data(
            canonical_plays_with_scores(
                n_games=n_games,
                plays_per_game=plays_per_game,
                source=source,
                overrides=overrides,
            )
        )

    def test_non_hc_source_half_is_unchanged_proven_no_op(self):
        df = self._ep_frame(source="hudl")

        out = make_ep_model_mutations(df, [*_EP_MODEL_COLUMNS, "half", "source"])

        assert out["half"].to_list() == df.filter(
            pl.col("yardline_50").is_not_null(), pl.col("yards_to_go").is_not_null()
        )["half"].to_list()

    def test_hc_source_constant_half_2_becomes_sentinel(self):
        df = self._ep_frame(source=self._HC_SOURCE, half=2)

        out = make_ep_model_mutations(df, [*_EP_MODEL_COLUMNS, "half", "source"])

        assert set(out["half"].to_list()) == {EP_HALF_UNKNOWN_SENTINEL}

    def test_mixed_source_frame_overwrite_is_row_scoped_not_frame_scoped(self):
        hudl_df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=12,
            source="hudl",
            overrides={"touchdown": [0] * 5 + [1] + [0] * 6},
        )
        hc_df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=12,
            source=self._HC_SOURCE,
            overrides={"touchdown": [0] * 3 + [1] + [0] * 8, "half": 2},
        )
        df = prepare_ep_data(pl.concat([hudl_df, hc_df]))

        out = make_ep_model_mutations(df, [*_EP_MODEL_COLUMNS, "half", "source"])

        hudl_rows = out.filter(pl.col("source") == "hudl")
        hc_rows = out.filter(pl.col("source") == self._HC_SOURCE)
        assert hc_rows.height > 0
        assert hudl_rows.height > 0
        assert set(hudl_rows["half"].to_list()) == {1, 2}
        assert set(hc_rows["half"].to_list()) == {EP_HALF_UNKNOWN_SENTINEL}

    def test_source_string_that_merely_contains_hc_workbook_is_not_rewritten(self):
        df = self._ep_frame(source="legacy-hc_workbook-import", half=2)
        assert not df["source"][0].startswith(HC_SOURCE_PREFIX)

        out = make_ep_model_mutations(df, [*_EP_MODEL_COLUMNS, "half", "source"])

        assert set(out["half"].to_list()) == {2}

    def test_other_feature_columns_unchanged_on_hc_source_frame(self):
        df = self._ep_frame(source=self._HC_SOURCE, half=2)

        out = make_ep_model_mutations(df, [*_EP_MODEL_COLUMNS, "half", "source"])

        assert out["Total_W_Scaled"].is_nan().sum() == 0
        assert out["Total_W_Scaled"].min() >= 0
        assert out["Total_W_Scaled"].max() <= 1
        down_sum = out["down0"] + out["down1"] + out["down2"] + out["down3"] + out["down4"]
        assert set(down_sum.to_list()) == {1}
        assert set(out["label"].drop_nulls().unique().to_list()) <= {0, 1, 2, 3, 4}

    def test_no_source_column_is_a_strict_no_op(self):
        df = self._ep_frame(source=self._HC_SOURCE, half=2).drop("source")

        out = make_ep_model_mutations(df, [*_EP_MODEL_COLUMNS, "half"])

        assert set(out["half"].to_list()) == {2}

    def test_make_wp_model_mutations_untouched_by_ep_half_sentinel(self):
        # hc_workbook source, constant half=2 (the label-construction sentinel) -- WP's
        # own model-mutation path must not gain a `half` overwrite of its own: `half` is
        # not in WP_FEATURES/WP_SELECTED_COLUMNS at all, so it never reaches WP's output
        # regardless of source. The frame's raw `half` column (still 2 everywhere, from
        # label construction) also stays exactly 2 -- the EP sentinel logic never touches
        # `prepare_wp_data`'s output.
        assert "half" not in WP_SELECTED_COLUMNS

        touchdown = [0] * 11 + [1]
        df = prepare_wp_data(
            canonical_plays_with_scores(
                n_games=1,
                plays_per_game=12,
                source=self._HC_SOURCE,
                overrides={"half": 2, "touchdown": touchdown},
            )
        )
        assert set(df["half"].to_list()) == {2}

        out = make_wp_model_mutations(df, ["posteam", "Winner", "label"])

        assert "half" not in out.columns
        assert set(df["half"].to_list()) == {2}


class TestAddRecencyWeight:
    """Coverage for `add_recency_weight` (REQ-S1-09 recency-weighting candidate, plan
    01.3-08 Task 1)."""

    def test_recency_half_life_grid_spans_at_least_one_order_of_magnitude(self):
        assert len(RECENCY_HALF_LIFE_DAYS_GRID) >= 3
        assert all(v > 0 for v in RECENCY_HALF_LIFE_DAYS_GRID)
        assert max(RECENCY_HALF_LIFE_DAYS_GRID) / min(RECENCY_HALF_LIFE_DAYS_GRID) >= 10

    def test_recency_null_date_weight_is_positive(self):
        assert RECENCY_NULL_DATE_WEIGHT > 0

    def test_missing_game_date_column_raises_named(self):
        df = canonical_plays(n_games=1, plays_per_game=2).drop("game_date")

        with pytest.raises(MissingFeatureColumns, match="game_date"):
            add_recency_weight(df, half_life_days=180.0)

    def test_most_recent_game_gets_weight_one(self):
        df = canonical_plays(
            n_games=2,
            plays_per_game=1,
            extras={"game_date": ["2024-01-01", "2024-07-01"]},
        )

        out, _ = add_recency_weight(df, half_life_days=182.0)

        most_recent = out.filter(pl.col("game_date") == "2024-07-01")
        assert most_recent["Recency_W"].item() == pytest.approx(1.0, abs=1e-9)

    def test_one_half_life_older_gets_half_within_tolerance(self):
        # 2024 is a leap year: Jan(31)+Feb(29)+Mar(31)+Apr(30)+May(31)+Jun(30) = 182 days
        # between 2024-01-01 and 2024-07-01.
        df = canonical_plays(
            n_games=2,
            plays_per_game=1,
            extras={"game_date": ["2024-01-01", "2024-07-01"]},
        )

        out, _ = add_recency_weight(df, half_life_days=182.0)

        older = out.filter(pl.col("game_date") == "2024-01-01")
        assert older["Recency_W"].item() == pytest.approx(0.5, abs=1e-9)

    def test_null_date_rows_get_null_date_weight_constant(self):
        df = canonical_plays(
            n_games=3,
            plays_per_game=1,
            extras={"game_date": ["2024-07-01", "2024-01-01", None]},
        )

        out, _ = add_recency_weight(df, half_life_days=182.0)

        null_row = out.filter(pl.col("game_date").is_null())
        assert null_row["Recency_W"].item() == pytest.approx(RECENCY_NULL_DATE_WEIGHT)

    def test_null_date_row_count_is_returned(self):
        df = canonical_plays(
            n_games=4,
            plays_per_game=1,
            extras={"game_date": ["2024-07-01", "2024-01-01", None, None]},
        )

        _, null_count = add_recency_weight(df, half_life_days=182.0)

        assert null_count == 2

    def test_recency_w_never_nan_or_infinite_including_null_dates(self):
        df = canonical_plays(
            n_games=4,
            plays_per_game=1,
            extras={"game_date": ["2024-07-01", "2024-01-01", None, "2023-01-01"]},
        )

        out, _ = add_recency_weight(df, half_life_days=180.0)

        assert out["Recency_W"].is_nan().sum() == 0
        assert out["Recency_W"].is_infinite().sum() == 0
        assert out["Recency_W"].null_count() == 0
        assert (out["Recency_W"] > 0).all()

    def test_all_null_game_date_frame_does_not_raise_and_weight_is_null_date_constant(self):
        # Matches the real corpus's current shape (01.3-DATA-PROFILE.md section 2: 100%
        # null across every source) -- recency weighting must not fail on this shape, it
        # degrades to a documented no-op.
        df = canonical_plays(n_games=3, plays_per_game=2)  # game_date defaults to null

        out, null_count = add_recency_weight(df, half_life_days=180.0)

        assert null_count == out.height
        assert out["Recency_W"].to_list() == pytest.approx(
            [RECENCY_NULL_DATE_WEIGHT] * out.height
        )

    def test_degenerate_single_distinct_date_raises(self):
        df = canonical_plays(
            n_games=2,
            plays_per_game=1,
            extras={"game_date": ["2024-01-01", "2024-01-01"]},
        )

        with pytest.raises(DegenerateWeightRange, match="2024-01-01"):
            add_recency_weight(df, half_life_days=180.0)

    def test_unparseable_game_date_value_raises_named(self):
        df = canonical_plays(
            n_games=2,
            plays_per_game=1,
            extras={"game_date": ["2024-01-01", "not-a-date"]},
        )

        with pytest.raises(InvalidGameDateValues, match="not-a-date"):
            add_recency_weight(df, half_life_days=180.0)

    def test_recency_w_is_float64(self):
        df = canonical_plays(
            n_games=2,
            plays_per_game=1,
            extras={"game_date": ["2024-01-01", "2024-07-01"]},
        )

        out, _ = add_recency_weight(df, half_life_days=182.0)

        assert out.schema["Recency_W"] == pl.Float64


class TestMakeWpModelMutations:
    def _wp_frame(self, n_games: int = 2, plays_per_game: int = 8) -> pl.DataFrame:
        touchdown = [0] * plays_per_game
        touchdown[-1] = 1  # last play of the game scores
        return prepare_ep_data(
            canonical_plays_with_scores(
                n_games=n_games,
                plays_per_game=plays_per_game,
                overrides={"touchdown": touchdown * n_games},
            )
        )

    def test_winner_matches_actual_team_not_literal_string(self):
        df = self._wp_frame()

        out = make_wp_model_mutations(
            df, ["game_id", "play_id", "posteam", "home_team", "away_team", "Winner", "label"]
        )

        assert set(out["Winner"].unique().to_list()) <= {"HOME", "AWAY"}

    def test_label_is_one_when_posteam_equals_winner(self):
        df = self._wp_frame()

        out = make_wp_model_mutations(
            df, ["game_id", "play_id", "posteam", "Winner", "label"]
        )

        winners = out.filter(pl.col("posteam") == pl.col("Winner"))
        losers = out.filter(pl.col("posteam") != pl.col("Winner"))
        assert set(winners["label"].to_list()) == {1}
        assert set(losers["label"].to_list()) == {0}

    def test_tie_game_labels_both_teams_zero(self):
        # No scoring at all -> home_team_score == away_team_score == 0 at game_end -> TIE.
        df = prepare_ep_data(canonical_plays_with_scores(n_games=1, plays_per_game=8))

        out = make_wp_model_mutations(df, ["posteam", "Winner", "label"])

        assert set(out["Winner"].unique().to_list()) == {"TIE"}
        assert set(out["label"].to_list()) == {0}

    def test_two_game_winner_does_not_leak_across_games(self):
        # Game 0 scores on the last play (AWAY wins); game 1 has no scoring (TIE).
        touchdown_game0 = [0] * 8
        touchdown_game0[-1] = 1
        touchdown_game1 = [0] * 8
        df = prepare_ep_data(
            canonical_plays_with_scores(
                n_games=2,
                plays_per_game=8,
                overrides={"touchdown": touchdown_game0 + touchdown_game1},
            )
        )

        out = make_wp_model_mutations(df, ["game_id", "posteam", "Winner", "label"])

        game_ids = sorted(out["game_id"].unique().to_list())
        winner_game0 = out.filter(pl.col("game_id") == game_ids[0])["Winner"].unique().to_list()
        winner_game1 = out.filter(pl.col("game_id") == game_ids[1])["Winner"].unique().to_list()
        assert winner_game0 == ["AWAY"]
        assert winner_game1 == ["TIE"]


class TestAddCompetitionTierFeatures:
    """Coverage for `add_competition_tier_features` (REQ-S1-09 competition-tier candidate,
    plan 01.3-07 Task 2). Mapping frames are built inline per test rather than reading the
    checked-in `data/reference/competition_tier.csv`, matching this module's
    `canonical_plays_with_scores`-plus-inline-overrides convention.
    """

    def _frame(self, source: str = "hudl", competition: str = "TEST", n: int = 4) -> pl.DataFrame:
        return canonical_plays_with_scores(
            n_games=1, plays_per_game=n, source=source, overrides={"competition": [competition] * n}
        )

    def _mapping(self, rows: list[tuple[str, str, str]]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "source": [r[0] for r in rows],
                "competition": [r[1] for r in rows],
                "tier": [r[2] for r in rows],
            },
            schema={"source": pl.Utf8, "competition": pl.Utf8, "tier": pl.Utf8},
        )

    def test_competition_tier_adds_one_int32_column_per_tier(self):
        df = self._frame()
        mapping = self._mapping([("hudl", "TEST", "womens-international")])

        out, cols = add_competition_tier_features(df, mapping)

        assert cols == [
            "tier_womens_international",
            "tier_womens_national",
            "tier_mixed_other",
            "tier_mens_international",
        ]
        for col in cols:
            assert out.schema[col] == pl.Int32

    def test_competition_tier_naming_rule_replaces_hyphen_with_underscore(self):
        df = self._frame()
        mapping = self._mapping([("hudl", "TEST", "womens-national")])

        _, cols = add_competition_tier_features(df, mapping)

        for tier, col in zip(COMPETITION_TIERS, cols):
            assert col == f"tier_{tier.replace('-', '_')}"

    def test_competition_tier_exactly_one_column_is_one_per_row(self):
        df = self._frame(n=6)
        mapping = self._mapping([("hudl", "TEST", "mixed-other")])

        out, cols = add_competition_tier_features(df, mapping)

        assert out.select(cols).sum_horizontal().min() == 1
        assert out.select(cols).sum_horizontal().max() == 1

    def test_competition_tier_row_count_and_order_preserved(self):
        df = self._frame(n=5)
        mapping = self._mapping([("hudl", "TEST", "womens-national")])

        out, _ = add_competition_tier_features(df, mapping)

        assert out.height == df.height
        assert out["play_id"].to_list() == df["play_id"].to_list()

    def test_competition_tier_returns_added_column_names(self):
        df = self._frame()
        mapping = self._mapping([("hudl", "TEST", "mixed-other")])

        _, cols = add_competition_tier_features(df, mapping)

        assert isinstance(cols, list)
        assert all(isinstance(c, str) for c in cols)

    def test_unmapped_competition_tier_pair_raises_naming_source_and_competition(self):
        df = self._frame(source="hudl", competition="UNKNOWN")
        mapping = self._mapping([])

        with pytest.raises(UnmappedCompetitionError, match="hudl"):
            add_competition_tier_features(df, mapping)

    def test_competition_tier_absent_from_corpus_still_produces_all_zero_column(self):
        # Only "mixed-other" is observed in this corpus; "womens-international" and
        # "womens-national" never appear, but their columns must still exist as all-zero.
        df = self._frame(competition="TEST")
        mapping = self._mapping([("hudl", "TEST", "mixed-other")])

        out, cols = add_competition_tier_features(df, mapping)

        assert out["tier_womens_international"].to_list() == [0] * df.height
        assert out["tier_womens_national"].to_list() == [0] * df.height
        assert out["tier_mixed_other"].to_list() == [1] * df.height

    def test_competition_tier_string_column_not_in_output(self):
        df = self._frame()
        mapping = self._mapping([("hudl", "TEST", "mixed-other")])

        out, _ = add_competition_tier_features(df, mapping)

        assert "competition_tier" not in out.columns

    def test_real_corpus_competition_tier_mapping_covers_every_pair(self, repo_root: Path):
        """The checked-in `data/reference/competition_tier.csv` covers every (source,
        competition) pair actually present in the real corpus, per
        01.3-DATA-PROFILE.md §3 -- guards against the reference file silently drifting out
        of sync with the ingested data.
        """
        plays_path = repo_root / "data" / "processed" / "plays.parquet"
        mapping_path = repo_root / "data" / "reference" / "competition_tier.csv"
        if not plays_path.exists() or not mapping_path.exists():
            pytest.skip("real corpus or competition_tier.csv not present in this worktree")

        from flag_football_ep.reference import load_competition_tier

        df = pl.read_parquet(plays_path)
        mapping = load_competition_tier(mapping_path)

        out, cols = add_competition_tier_features(df, mapping)

        assert out.height == df.height
        assert out.select(cols).sum_horizontal().min() == 1
        assert out.select(cols).sum_horizontal().max() == 1


# --- half sentinel: label construction ---
#
# Head-coach (`hc_workbook:*`) rows carry no half information from the source; M3-02-01's
# ingest reader stamps a constant `half = 2` for the whole game so that `_mark_half_end`'s
# `half_end`/`game_end` computation -- which both EP's (`prepare_ep_data`) and WP's
# (`prepare_wp_data`) label construction depend on -- resolves correctly (M3-02-RESEARCH
# section 2.2). These tests pin that decision table directly against the label-construction
# code, independent of the ingest reader (owned by M3-02-01, not built here) and independent
# of the model-feature sentinel added in `TestMakeEpModelMutationsHalfSentinel` below (M3-02
# task 2). Every frame is built with `flag_football_ep.testing.canonical_plays_with_scores`.
class TestHalfSentinelLabelConstruction:
    def test_constant_half_2_marks_half_end_and_game_end_on_same_row(self):
        """RESEARCH section 2.2 decision table, row `2`: the only sentinel that both passes
        `half_assigned` and makes `half_end`/`game_end` fire together, at the true last row.
        """
        df = canonical_plays_with_scores(
            n_games=1, plays_per_game=8, overrides={"half": 2}
        )

        out = prepare_ep_data(df)

        half_end_rows = out.filter(pl.col("half_end") == 1)
        game_end_rows = out.filter(pl.col("game_end") == 1)
        assert half_end_rows.height == 1
        assert game_end_rows.height == 1
        assert half_end_rows["play_id"].item() == game_end_rows["play_id"].item() == 8

    def test_constant_half_1_never_fires_game_end(self):
        """RESEARCH section 2.2 decision table, row `1`: `half_end` still fires once per
        game, but `game_end` (which requires `half == 2`) never fires -- the regression this
        sentinel choice exists to prevent. Assert it explicitly so nobody "simplifies" the
        sentinel to 1 later.
        """
        df = canonical_plays_with_scores(
            n_games=1, plays_per_game=8, overrides={"half": 1}
        )

        out = prepare_ep_data(df)

        assert out.filter(pl.col("half_end") == 1).height == 1
        assert out.filter(pl.col("game_end") == 1).height == 0

    def test_all_null_half_never_fires_game_end_and_wp_winner_stays_null(self):
        """RESEARCH section 2.2 decision table, row `null`: pins RESEARCH Pitfall 1's exact
        failure mode -- an all-null `half` never fires `game_end`, so WP's `Winner` never
        resolves for that game, independent of `half` ever being in `WP_FEATURES`.
        """
        touchdown = [0] * 7 + [1]
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=8,
            overrides={"half": None, "touchdown": touchdown},
        )

        ep_out = prepare_ep_data(df)
        assert ep_out.filter(pl.col("game_end") == 1).height == 0

        wp_out = prepare_wp_data(df)
        assert wp_out.filter(pl.col("game_end") == 1).height == 0
        winner_out = make_wp_model_mutations(wp_out, ["posteam", "Winner", "label"])
        assert winner_out["Winner"].null_count() == winner_out.height

    def test_prepare_ep_data_next_score_half_on_constant_half_2(self):
        """On the constant-half-2 sentinel, `prepare_ep_data` still produces a non-null
        `Next_Score_Half` for every row of the game (backward-filled from the single
        half_end row), and that terminal row is `"No_Score"` when it is not itself a
        scoring play -- RESEARCH section 2.2's `No_Score`-preserved-by-construction claim.
        """
        df = canonical_plays_with_scores(
            n_games=1, plays_per_game=8, overrides={"half": 2}
        )

        out = prepare_ep_data(df)

        assert out["Next_Score_Half"].null_count() == 0
        terminal_row = out.filter(pl.col("half_end") == 1)
        assert terminal_row["scoring_play"].item() == 0
        assert terminal_row["Next_Score_Half"].item() == "No_Score"

    def test_prepare_wp_data_resolves_winner_on_constant_half_2(self):
        """On the constant-half-2 sentinel, `prepare_wp_data`'s `game_end` flows through to
        a fully resolved (non-null) `Winner` via `make_wp_model_mutations`.
        """
        touchdown = [0] * 7 + [1]
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=8,
            overrides={"half": 2, "touchdown": touchdown},
        )

        wp_out = prepare_wp_data(df)
        winner_out = make_wp_model_mutations(wp_out, ["posteam", "Winner", "label"])

        assert winner_out["Winner"].null_count() == 0

    def test_two_game_frame_mixed_real_and_constant_half_does_not_leak_game_end(self):
        """A two-game frame where game A has real halves (1 then 2) and game B is constant
        half 2 produces exactly one `game_end` per game -- the `.over("game_id")` scoping
        holds even when one game's `half` values follow the sentinel and the other's don't.
        """
        half_game_a = [1, 1, 1, 1, 2, 2, 2, 2]
        half_game_b = [2] * 8

        df = canonical_plays_with_scores(
            n_games=2,
            plays_per_game=8,
            overrides={"half": half_game_a + half_game_b},
        )

        out = prepare_ep_data(df)

        counts = (
            out.filter(pl.col("game_end") == 1)
            .group_by("game_id")
            .agg(pl.len().alias("n"))
        )
        assert set(counts["n"].to_list()) == {1}
        assert counts.height == 2
