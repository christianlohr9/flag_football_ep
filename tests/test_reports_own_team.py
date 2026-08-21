"""Unit coverage for `flag_football_ep.reports.own_team`.

Every test builds inputs with `flag_football_ep.testing.canonical_plays_with_scores` plus
targeted `overrides`/`extras`, matching the class-per-function layout of
`tests/test_features_mutations.py` and `tests/test_reports_aggregate.py`.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from flag_football_ep.model.hyperparams import EP_PROB_LABELS
from flag_football_ep.reports.aggregate import MUTED_MIN_N
from flag_football_ep.reports.own_team import attach_epa, efficiency_by_call
from flag_football_ep.testing import canonical_plays_with_scores


def _pat_ready(df: pl.DataFrame) -> pl.DataFrame:
    """Give a `canonical_plays_with_scores` fixture at least one 1-pt and one 2-pt PAT
    attempt, so `estimate_pat_baselines` never raises `InsufficientPatAttempts`.
    """
    height = df.height
    down = [0, 0] + [1] * (height - 2)
    yards_to_go = [3, 10] + [10] * (height - 2)
    one_pt = [1, 0] + [0] * (height - 2)
    two_pt = [0, 1] + [0] * (height - 2)
    return df.with_columns(
        down=pl.Series(down, dtype=pl.Int32),
        yards_to_go=pl.Series(yards_to_go, dtype=pl.Int32),
        one_point_conv_success=pl.Series(one_pt, dtype=pl.Int32),
        two_point_conv_success=pl.Series(two_pt, dtype=pl.Int32),
    )


def _write_oof_ep(path: Path, game_id: str, play_ids: list[int], value: float = 0.2) -> None:
    frame = pl.DataFrame(
        {
            "game_id": [game_id] * len(play_ids),
            "play_id": pl.Series(play_ids, dtype=pl.Int32),
            "source": ["hudl"] * len(play_ids),
            **{label: [value] * len(play_ids) for label in EP_PROB_LABELS},
        }
    )
    frame.write_parquet(path)


def _write_oof_wp(path: Path, game_id: str, play_ids: list[int], value: float = 0.5) -> None:
    frame = pl.DataFrame(
        {
            "game_id": [game_id] * len(play_ids),
            "play_id": pl.Series(play_ids, dtype=pl.Int32),
            "source": ["hudl"] * len(play_ids),
            "wp": [value] * len(play_ids),
        }
    )
    frame.write_parquet(path)


class TestAttachEpa:
    def test_missing_oof_files_do_not_raise(self, tmp_path: Path) -> None:
        df = _pat_ready(canonical_plays_with_scores(n_games=1, plays_per_game=6))
        result = attach_epa(df, processed_dir=tmp_path, scored=None)
        assert result.height == df.height
        assert result["epa_source"].is_null().all()
        assert result["epa"].is_null().all()

    def test_height_and_order_preserved(self, tmp_path: Path) -> None:
        df = _pat_ready(canonical_plays_with_scores(n_games=2, plays_per_game=6))
        result = attach_epa(df, processed_dir=tmp_path, scored=None)
        assert result.height == df.height
        assert result["game_id"].to_list() == df["game_id"].to_list()
        assert result["play_id"].to_list() == df["play_id"].to_list()

    def test_oof_matched_play_gets_oof_source(self, tmp_path: Path) -> None:
        df = _pat_ready(canonical_plays_with_scores(n_games=1, plays_per_game=8))
        game_id = df["game_id"][0]
        play_ids = df["play_id"].to_list()
        _write_oof_ep(tmp_path / "oof_predictions_ep.parquet", game_id, play_ids)
        _write_oof_wp(tmp_path / "oof_predictions_wp.parquet", game_id, play_ids)

        result = attach_epa(df, processed_dir=tmp_path, scored=None)
        matched = result.filter(pl.col("epa_source").is_not_null())
        assert matched.height > 0
        assert set(matched["epa_source"].unique().to_list()) == {"oof"}

    def test_champion_only_play_gets_champion_source(self, tmp_path: Path) -> None:
        df = _pat_ready(canonical_plays_with_scores(n_games=1, plays_per_game=8))
        # No OOF files at all -- every play falls to the champion-scored frame.
        scored = df.with_columns(
            ep=pl.lit(1.0),
            epa=pl.lit(0.5),
            wp=pl.lit(0.6),
            home_wp=pl.lit(0.6),
            wpa=pl.lit(0.01),
        )
        result = attach_epa(df, processed_dir=tmp_path, scored=scored)
        assert result.height == df.height
        assert set(result["epa_source"].unique().to_list()) == {"champion"}
        assert result["epa"].to_list() == [0.5] * df.height

    def test_row_matched_by_neither_keeps_null_source(self, tmp_path: Path) -> None:
        df = _pat_ready(canonical_plays_with_scores(n_games=1, plays_per_game=4))
        result = attach_epa(df, processed_dir=tmp_path, scored=None)
        assert result["epa_source"].is_null().all()
        assert result["epa"].is_null().all()
        assert result["ep"].is_null().all()

    def test_oof_wins_over_conflicting_champion_value(self, tmp_path: Path) -> None:
        df = _pat_ready(canonical_plays_with_scores(n_games=1, plays_per_game=8))
        game_id = df["game_id"][0]
        play_ids = df["play_id"].to_list()
        _write_oof_ep(tmp_path / "oof_predictions_ep.parquet", game_id, play_ids, value=0.2)
        _write_oof_wp(tmp_path / "oof_predictions_wp.parquet", game_id, play_ids, value=0.5)

        # Deliberately conflicting champion values for every play.
        conflicting_scored = df.with_columns(
            ep=pl.lit(999.0),
            epa=pl.lit(999.0),
            wp=pl.lit(0.99),
            home_wp=pl.lit(0.99),
            wpa=pl.lit(0.0),
        )
        result = attach_epa(df, processed_dir=tmp_path, scored=conflicting_scored)
        oof_rows = result.filter(pl.col("epa_source") == "oof")
        assert oof_rows.height > 0
        assert (oof_rows["epa"] == 999.0).sum() == 0

    def test_partial_oof_coverage_falls_back_to_champion(self, tmp_path: Path) -> None:
        df = _pat_ready(canonical_plays_with_scores(n_games=1, plays_per_game=8))
        game_id = df["game_id"][0]
        # Only the first half of the plays are covered by OOF.
        covered_play_ids = df["play_id"].to_list()[:4]
        _write_oof_ep(tmp_path / "oof_predictions_ep.parquet", game_id, covered_play_ids)
        _write_oof_wp(tmp_path / "oof_predictions_wp.parquet", game_id, covered_play_ids)

        scored = df.with_columns(
            ep=pl.lit(1.0),
            epa=pl.lit(7.0),
            wp=pl.lit(0.6),
            home_wp=pl.lit(0.6),
            wpa=pl.lit(0.01),
        )
        result = attach_epa(df, processed_dir=tmp_path, scored=scored)
        sources = result.sort("play_id")["epa_source"].to_list()
        assert sources[:4] == ["oof"] * 4
        assert sources[4:] == ["champion"] * 4

    def test_empty_input_returns_empty_frame_without_raising(self, tmp_path: Path) -> None:
        df = _pat_ready(canonical_plays_with_scores(n_games=1, plays_per_game=4))
        empty = df.filter(pl.lit(False))
        result = attach_epa(empty, processed_dir=tmp_path, scored=None)
        assert result.height == 0
        assert "epa_source" in result.columns
        assert "epa" in result.columns


class TestEfficiencyByCall:
    def test_one_row_per_dimension_category(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=4,
            extras={
                "off_form": ["Trips", "Trips", "Bunch", "Bunch"],
                "off_play": ["Slant", "Slant", "Screen", "Screen"],
                "target_route": ["Go", "Go", "Out", "Out"],
            },
        )
        df = df.with_columns(epa=pl.Series([0.1, 0.2, -0.1, 0.3]))
        section = efficiency_by_call(df, cycle_start_season=2026)
        pairs = set(zip(section.table["dimension"].to_list(), section.table["wert"].to_list()))
        assert pairs == {
            ("off_form", "Trips"),
            ("off_form", "Bunch"),
            ("off_play", "Slant"),
            ("off_play", "Screen"),
            ("target_route", "Go"),
            ("target_route", "Out"),
        }

    def test_category_only_outside_cycle_has_null_cycle_epa(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=2,
            extras={"off_form": ["Trips", "Trips"]},
            overrides={"season": [2020, 2020]},
        )
        df = df.with_columns(epa=pl.Series([0.1, 0.2]))
        section = efficiency_by_call(df, cycle_start_season=2026)
        row = section.table.filter(
            (pl.col("dimension") == "off_form") & (pl.col("wert") == "Trips")
        )
        assert row["n_cycle"].item() == 0
        assert row["epa_play_cycle"].item() is None
        assert row["n_alltime"].item() == 2

    def test_muted_flips_at_muted_min_n(self) -> None:
        n = MUTED_MIN_N
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=n,
            extras={"off_form": ["Trips"] * n},
        )
        df = df.with_columns(epa=pl.Series([0.1] * n))
        section = efficiency_by_call(df, cycle_start_season=2026)
        row = section.table.filter(
            (pl.col("dimension") == "off_form") & (pl.col("wert") == "Trips")
        )
        assert row["muted"].item() is False

        below = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=n - 1,
            extras={"off_form": ["Trips"] * (n - 1)},
        )
        below = below.with_columns(epa=pl.Series([0.1] * (n - 1)))
        below_section = efficiency_by_call(below, cycle_start_season=2026)
        below_row = below_section.table.filter(
            (pl.col("dimension") == "off_form") & (pl.col("wert") == "Trips")
        )
        assert below_row["muted"].item() is True

    def test_null_epa_rows_excluded(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1, plays_per_game=2, extras={"off_form": ["Trips", "Trips"]}
        )
        df = df.with_columns(epa=pl.Series([None, 0.2]))
        section = efficiency_by_call(df, cycle_start_season=2026)
        row = section.table.filter(
            (pl.col("dimension") == "off_form") & (pl.col("wert") == "Trips")
        )
        assert row["n_alltime"].item() == 1

    def test_fully_uncharted_frame_yields_empty_section(self) -> None:
        df = canonical_plays_with_scores(n_games=1, plays_per_game=4)
        df = df.with_columns(epa=pl.Series([0.1, 0.2, -0.1, 0.3]))
        section = efficiency_by_call(df, cycle_start_season=2026)
        assert section.table.height == 0
        assert section.empty_notice is not None
        assert section.basis.n_plays == 0
