"""Unit coverage for `flag_football_ep.features.mutations`.

Every test builds inputs with `flag_football_ep.testing.canonical_plays_with_scores` plus
targeted `overrides`/`extras`, matching the frozen notebook baseline behaviour documented in
`tests/fixtures/baseline_manifest.json`.
"""

from __future__ import annotations

import polars as pl
import pytest

from flag_football_ep.testing import canonical_plays_with_scores
from flag_football_ep.features.mutations import prepare_ep_data, prepare_wp_data


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
