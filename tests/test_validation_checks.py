"""Unit coverage for the six per-game validation checks and the quarantine
partition (added in phase 01.2 plan 06).
"""

from __future__ import annotations

import polars as pl
import pytest

from flag_football_ep.testing import canonical_plays
from flag_football_ep.validation.checks import (
    CHECKS,
    CheckResult,
    GameResult,
    Status,
    downs_range,
    gapless_play_ids,
    half_assigned,
    monotonic_drive_ids,
    partition_games,
    run_checks,
    score_reconstruction,
    yardline_range,
)


def _final_scores(rows: list[dict]) -> pl.DataFrame:
    schema = {
        "game_id": pl.Utf8,
        "home_team": pl.Utf8,
        "away_team": pl.Utf8,
        "home_score": pl.Int32,
        "away_score": pl.Int32,
        "note": pl.Utf8,
    }
    data = {name: [row.get(name) for row in rows] for name in schema}
    return pl.DataFrame(data, schema=schema)


class TestDownsRange:
    def test_pass_when_all_downs_in_range(self):
        df = canonical_plays(n_games=1, plays_per_game=4)
        results = downs_range(df)
        assert len(results) == 1
        assert results[0].status == Status.PASS

    def test_fail_naming_offending_values_and_count(self):
        df = canonical_plays(n_games=1, plays_per_game=4, overrides={"down": [1, 7, -1, 2]})
        results = downs_range(df)
        assert len(results) == 1
        result = results[0]
        assert result.status == Status.FAIL
        assert "7" in result.detail
        assert "-1" in result.detail
        assert result.n_offending == 2

    def test_fail_when_down_is_null(self):
        df = canonical_plays(n_games=1, plays_per_game=4, overrides={"down": [1, None, 2, 3]})
        results = downs_range(df)
        result = results[0]
        assert result.status == Status.FAIL
        assert result.n_offending == 1


class TestYardlineRange:
    def test_pass_when_all_within_0_50(self):
        df = canonical_plays(n_games=1, plays_per_game=4, overrides={"yardline_50": [0, 25, 50, 10]})
        results = yardline_range(df)
        assert results[0].status == Status.PASS

    def test_fail_with_min_max_found(self):
        df = canonical_plays(n_games=1, plays_per_game=4, overrides={"yardline_50": [-5, 25, 60, 10]})
        results = yardline_range(df)
        result = results[0]
        assert result.status == Status.FAIL
        assert "-5" in result.detail
        assert "60" in result.detail


class TestHalfAssigned:
    def test_pass_when_all_half_1_or_2(self):
        df = canonical_plays(n_games=1, plays_per_game=4, overrides={"half": [1, 1, 2, 2]})
        results = half_assigned(df)
        assert results[0].status == Status.PASS

    def test_fail_no_half_boundary_when_null(self):
        df = canonical_plays(n_games=1, plays_per_game=4, overrides={"half": [1, None, 2, 2]})
        results = half_assigned(df)
        result = results[0]
        assert result.status == Status.FAIL
        assert "no half boundary" in result.detail

    def test_fail_naming_invalid_value(self):
        df = canonical_plays(n_games=1, plays_per_game=4, overrides={"half": [1, 3, 2, 2]})
        results = half_assigned(df)
        result = results[0]
        assert result.status == Status.FAIL
        assert "3" in result.detail


class TestMonotonicDriveIds:
    def test_pass_when_non_decreasing_and_starts_at_1(self):
        df = canonical_plays(n_games=1, plays_per_game=5, overrides={"drive_id": [1, 1, 2, 2, 3]})
        results = monotonic_drive_ids(df)
        assert results[0].status == Status.PASS

    def test_fail_on_decreasing_sequence_reports_first_offending_play_id(self):
        df = canonical_plays(n_games=1, plays_per_game=5, overrides={"drive_id": [1, 2, 2, 1, 3]})
        results = monotonic_drive_ids(df)
        result = results[0]
        assert result.status == Status.FAIL
        assert "4" in result.detail

    def test_fail_when_first_drive_id_not_1(self):
        df = canonical_plays(n_games=1, plays_per_game=5, overrides={"drive_id": [2, 2, 3, 3, 4]})
        results = monotonic_drive_ids(df)
        result = results[0]
        assert result.status == Status.FAIL
        assert "2" in result.detail


class TestGaplessPlayIds:
    def test_pass_when_contiguous_no_duplicates(self):
        df = canonical_plays(n_games=1, plays_per_game=5)
        results = gapless_play_ids(df)
        assert results[0].status == Status.PASS

    def test_fail_naming_missing_id(self):
        df = canonical_plays(n_games=1, plays_per_game=5, overrides={"play_id": [1, 2, 3, 4, 6]})
        results = gapless_play_ids(df)
        result = results[0]
        assert result.status == Status.FAIL
        assert "5" in result.detail

    def test_fail_naming_duplicated_id(self):
        df = canonical_plays(n_games=1, plays_per_game=5, overrides={"play_id": [1, 2, 3, 3, 5]})
        results = gapless_play_ids(df)
        result = results[0]
        assert result.status == Status.FAIL
        assert "3" in result.detail

    def test_fail_on_single_null_play_id_names_null_count_and_does_not_raise(self):
        df = canonical_plays(n_games=1, plays_per_game=4, overrides={"play_id": [1, 2, None, 4]})
        results = gapless_play_ids(df)
        result = results[0]
        assert result.status == Status.FAIL
        assert "null play_id" in result.detail
        assert "1" in result.detail

    def test_fail_on_duplicated_nulls_alongside_duplicated_ints_does_not_raise_typeerror(self):
        df = canonical_plays(n_games=1, plays_per_game=4, overrides={"play_id": [1, None, None, 2]})
        results = gapless_play_ids(df)
        result = results[0]
        assert result.status == Status.FAIL
        assert "null play_id" in result.detail

    def test_fail_on_all_null_play_id_game(self):
        df = canonical_plays(n_games=1, plays_per_game=3, overrides={"play_id": [None, None, None]})
        results = gapless_play_ids(df)
        result = results[0]
        assert result.status == Status.FAIL
        assert "null play_id" in result.detail
        assert "3" in result.detail


class TestScoreReconstruction:
    def test_pass_when_last_row_matches_reference(self):
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"home_team_score": [0, 6], "away_team_score": [0, 0]},
        )
        game_id = df["game_id"][0]
        final_scores = _final_scores(
            [{"game_id": game_id, "home_team": "HOME", "away_team": "AWAY", "home_score": 6, "away_score": 0}]
        )
        results = score_reconstruction(df, final_scores=final_scores)
        assert results[0].status == Status.PASS

    def test_fail_reports_reconstructed_vs_reference(self):
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"home_team_score": [0, 6], "away_team_score": [0, 0]},
        )
        game_id = df["game_id"][0]
        final_scores = _final_scores(
            [{"game_id": game_id, "home_team": "HOME", "away_team": "AWAY", "home_score": 12, "away_score": 0}]
        )
        results = score_reconstruction(df, final_scores=final_scores)
        result = results[0]
        assert result.status == Status.FAIL
        assert "6" in result.detail
        assert "12" in result.detail

    def test_fail_on_home_away_convention_mismatch(self):
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"home_team_score": [0, 6], "away_team_score": [0, 0]},
        )
        game_id = df["game_id"][0]
        final_scores = _final_scores(
            [{"game_id": game_id, "home_team": "OTHER", "away_team": "ANOTHER", "home_score": 6, "away_score": 0}]
        )
        results = score_reconstruction(df, final_scores=final_scores)
        result = results[0]
        assert result.status == Status.FAIL
        assert "mismatch" in result.detail.lower()

    def test_skipped_when_no_reference_entry(self):
        df = canonical_plays(n_games=1, plays_per_game=2)
        results = score_reconstruction(df, final_scores=_final_scores([]))
        result = results[0]
        assert result.status == Status.SKIPPED
        assert "no reference entry" in result.detail

    def test_skipped_when_final_scores_is_none(self):
        df = canonical_plays(n_games=1, plays_per_game=2)
        results = score_reconstruction(df, final_scores=None)
        assert results[0].status == Status.SKIPPED


class TestPerGameResults:
    def test_each_check_returns_one_result_per_game_id(self):
        df = canonical_plays(n_games=2, plays_per_game=4)
        for check_fn in (downs_range, yardline_range, half_assigned, monotonic_drive_ids, gapless_play_ids):
            results = check_fn(df)
            assert len(results) == 2
            assert {r.game_id for r in results} == set(df["game_id"].unique().to_list())


class TestRunChecks:
    def test_registry_order(self):
        assert list(CHECKS) == [
            "downs_range",
            "yardline_range",
            "half_assigned",
            "monotonic_drive_ids",
            "gapless_play_ids",
            "score_reconstruction",
        ]

    def test_two_game_frame_returns_twelve_results(self):
        df = canonical_plays(n_games=2, plays_per_game=4)
        results = run_checks(df, final_scores=None)
        assert len(results) == 12

    def test_empty_frame_returns_empty_list_without_raising(self):
        df = canonical_plays(n_games=1, plays_per_game=1).clear()
        results = run_checks(df, final_scores=None)
        assert results == []


class TestPartitionGames:
    def test_quarantine_on_failure(self):
        df = canonical_plays(n_games=2, plays_per_game=8)
        play_ids = df["play_id"].to_list()
        # second game occupies rows 8..15; introduce a gap (skip 5).
        play_ids[8:16] = [1, 2, 3, 4, 6, 7, 8, 9]
        df = df.with_columns(pl.Series("play_id", play_ids).cast(pl.Int32))

        results = run_checks(df, final_scores=None)
        accepted, quarantined, game_results = partition_games(df, results)

        game_ids = df["game_id"].unique(maintain_order=True).to_list()
        game_a, game_b = game_ids[0], game_ids[1]

        assert set(accepted["game_id"].unique().to_list()) == {game_a}
        assert set(quarantined["game_id"].unique().to_list()) == {game_b}

        b_result = next(g for g in game_results if g.game_id == game_b)
        assert b_result.quarantined is True
        assert any("gapless_play_ids" in reason for reason in b_result.reasons)

        a_result = next(g for g in game_results if g.game_id == game_a)
        assert a_result.quarantined is False

    def test_row_conservation(self):
        df = canonical_plays(n_games=2, plays_per_game=8)
        play_ids = df["play_id"].to_list()
        play_ids[8:16] = [1, 2, 3, 4, 6, 7, 8, 9]
        df = df.with_columns(pl.Series("play_id", play_ids).cast(pl.Int32))
        results = run_checks(df, final_scores=None)
        accepted, quarantined, _ = partition_games(df, results)
        assert accepted.height + quarantined.height == df.height

    def test_skipped_only_game_is_accepted(self):
        df = canonical_plays(n_games=1, plays_per_game=4)
        results = run_checks(df, final_scores=None)
        accepted, quarantined, game_results = partition_games(df, results)
        assert quarantined.height == 0
        assert accepted.height == df.height
        assert game_results[0].quarantined is False

    def test_warn_only_never_quarantines(self):
        df = canonical_plays(
            n_games=1,
            plays_per_game=4,
            source="legacy",
            overrides={"down": [7, 1, 2, 3], "half": [3, 1, 1, 2]},
        )
        results = run_checks(df, final_scores=None)
        accepted, quarantined, game_results = partition_games(df, results)

        assert quarantined.height == 0
        assert accepted.height == df.height

        g = game_results[0]
        assert g.quarantined is False
        warn_checks = {r.check for r in g.results if r.status == Status.WARN}
        assert "downs_range" in warn_checks
        assert "half_assigned" in warn_checks
        assert len(g.reasons) >= 2

    def test_warn_only_sources_is_keyword_param(self):
        import inspect

        sig = inspect.signature(partition_games)
        assert "warn_only_sources" in sig.parameters

    def test_empty_frame_returns_two_empty_frames_and_empty_result_list(self):
        df = canonical_plays(n_games=1, plays_per_game=1).clear()
        accepted, quarantined, game_results = partition_games(df, [])
        assert accepted.height == 0
        assert quarantined.height == 0
        assert accepted.columns == df.columns
        assert quarantined.columns == df.columns
        assert game_results == []

    def test_inconsistent_source_raises_value_error(self):
        df = canonical_plays(n_games=1, plays_per_game=2)
        df = df.with_columns(pl.Series("source", ["hudl", "legacy"]))
        with pytest.raises(ValueError):
            partition_games(df, [])

    def test_game_result_source_from_frame(self):
        df = canonical_plays(n_games=1, plays_per_game=2, source="legacy")
        results = run_checks(df, final_scores=None)
        _, _, game_results = partition_games(df, results)
        assert game_results[0].source == "legacy"


def test_check_result_is_a_dataclass_with_expected_fields():
    result = CheckResult(game_id="g1", check="downs_range", status=Status.PASS, detail="ok", n_offending=0)
    assert result.game_id == "g1"
    assert result.status == Status.PASS


def test_game_result_is_a_dataclass_with_expected_fields():
    result = GameResult(game_id="g1", source="hudl", results=[], quarantined=False, reasons=[])
    assert result.game_id == "g1"
    assert result.quarantined is False
