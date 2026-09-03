"""Unit coverage for `flag_football_ep.features.explosiveness` (HC-04, Phase M3-3).

Every test builds inputs with `flag_football_ep.testing.canonical_plays_with_scores` plus
targeted `overrides`/`extras`, matching `tests/test_features_mutations.py`'s conventions.
`epa` is NOT a canonical column (it is added downstream by
`features.mutations.add_ep_variables`), so every test that needs it appends
`.with_columns(epa=pl.Series([...]))` after building the frame -- mirroring the plan's own
verify script and never touching `tests/conftest.py` or inventing a new fixture style.

No real player names anywhere: every fixture uses synthetic labels (`QB A`, `QB B`, ...).
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.features import explosiveness
from flag_football_ep.testing import canonical_plays_with_scores


def _make_calibration(
    *, threshold: float = 2.0, iqr: float = 1.0, quantile: float = 0.80
) -> explosiveness.ExplosivenessCalibration:
    """A hand-built `ExplosivenessCalibration` for tests that only need the flag/score math,
    not a full `calibrate(...)` run.
    """
    return explosiveness.ExplosivenessCalibration(
        schema_version=explosiveness.CALIBRATION_SCHEMA_VERSION,
        epa_quantile=quantile,
        epa_threshold=threshold,
        epa_median_success=threshold * 0.5,
        epa_iqr_success=iqr,
        corpus_n=100,
        n_success=50,
        corpus_sources=("hudl",),
        corpus_fingerprint="deadbeef",
        calibrated_on="2026-09-03T00:00:00+00:00",
    )


# --- scrimmage_plays -------------------------------------------------------------------------


class TestScrimmagePlays:
    def test_excludes_pat_no_play_and_kickoff_rows(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=8,
            overrides={
                "play_type": [
                    "run",
                    "pass",
                    "extra_point",
                    "no_play",
                    "kickoff",
                    "run",
                    "pass",
                    "extra_point",
                ],
                "down": [1, 2, 0, 3, 0, 4, 1, 0],
            },
        )
        result = explosiveness.scrimmage_plays(df)
        assert result.height == 4
        assert set(result["play_type"].to_list()) == {"run", "pass"}
        assert (result["down"] == 0).sum() == 0

    def test_raises_on_missing_yards_gained(self) -> None:
        df = canonical_plays_with_scores(n_games=1, plays_per_game=4).drop("yards_gained")
        with pytest.raises(explosiveness.MissingExplosivenessColumns, match="yards_gained"):
            explosiveness.scrimmage_plays(df)

    def test_raises_on_missing_epa_when_required(self) -> None:
        df = canonical_plays_with_scores(n_games=1, plays_per_game=4)
        with pytest.raises(explosiveness.MissingExplosivenessColumns, match="epa"):
            explosiveness.scrimmage_plays(df, require_epa=True)

    def test_does_not_raise_when_epa_absent_and_not_required(self) -> None:
        df = canonical_plays_with_scores(n_games=1, plays_per_game=4)
        result = explosiveness.scrimmage_plays(df, require_epa=False)
        assert result.height == 4


# --- hc_workbook_explosive_rate --------------------------------------------------------------


class TestHcWorkbookExplosiveRate:
    def test_strictly_greater_than_twelve(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=4,
            overrides={"play_type": "pass", "yards_gained": [5, 12, 13, 40]},
            extras={"thrown_by": "QB A"},
        )
        result = explosiveness.hc_workbook_explosive_rate(df)
        row = result.row(0, named=True)
        assert row["n"] == 4
        assert row["exp_plays"] == 2
        assert row["explosive_pct"] == pytest.approx(0.5)

    def test_ignores_run_plays(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=5,
            overrides={
                "play_type": ["pass", "pass", "pass", "pass", "run"],
                "yards_gained": [5, 12, 13, 40, 30],
            },
            extras={"thrown_by": "QB A"},
        )
        result = explosiveness.hc_workbook_explosive_rate(df)
        row = result.row(0, named=True)
        assert row["n"] == 4
        assert row["exp_plays"] == 2

    def test_ignores_epa_column_entirely(self) -> None:
        base = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=4,
            overrides={"play_type": "pass", "yards_gained": [5, 12, 13, 40]},
            extras={"thrown_by": "QB A"},
        )
        without_epa = explosiveness.hc_workbook_explosive_rate(base)
        with_epa = explosiveness.hc_workbook_explosive_rate(
            base.with_columns(epa=pl.Series([-1.0, -2.0, -3.0, -4.0]))
        )
        assert without_epa.equals(with_epa)

    def test_empty_input_returns_schema_correct_empty_frame(self) -> None:
        df = canonical_plays_with_scores(n_games=0, plays_per_game=8)
        result = explosiveness.hc_workbook_explosive_rate(df)
        assert result.height == 0
        assert set(result.columns) == {"thrown_by", "n", "exp_plays", "explosive_pct"}


# --- hc_verbal_explosive_rate -----------------------------------------------------------------


class TestHcVerbalExplosiveRate:
    def test_or_predicate_combines_yards_and_epa(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=3,
            overrides={"play_type": "pass", "yards_gained": [5, 20, 3]},
            extras={"thrown_by": "QB A"},
        ).with_columns(epa=pl.Series([1.0, -0.5, -0.2]))
        result = explosiveness.hc_verbal_explosive_rate(df)
        row = result.row(0, named=True)
        assert row["n"] == 3
        assert row["successes"] == 2

    def test_empty_input_returns_schema_correct_empty_frame(self) -> None:
        df = canonical_plays_with_scores(n_games=0, plays_per_game=8).with_columns(
            epa=pl.Series([], dtype=pl.Float64)
        )
        result = explosiveness.hc_verbal_explosive_rate(df)
        assert result.height == 0
        assert set(result.columns) == {"thrown_by", "n", "successes", "rate"}


# --- hc_efficiency_table ----------------------------------------------------------------------


class TestHcEfficiencyTable:
    def test_basic_sum_and_denominator(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=4,
            overrides={"play_type": "pass"},
            extras={"thrown_by": "QB A", "efficiency": [1, 1, 0, None]},
        )
        result = explosiveness.hc_efficiency_table(df)
        row = result.row(0, named=True)
        assert row["efficiency_sum"] == 2
        assert row["attempts"] == 4
        assert row["denominator"] == 4
        assert row["efficiency"] == pytest.approx(0.5)

    def test_drops_flag_extends_denominator(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=4,
            overrides={
                "play_type": "pass",
                "result_raw": ["Complete", "Complete", "Complete", "Dropped"],
            },
            extras={"thrown_by": "QB A", "efficiency": [1, 1, 0, None]},
        )
        with_drops = explosiveness.hc_efficiency_table(
            df, drops_flag=pl.col("result_raw") == "Dropped"
        )
        without_drops = explosiveness.hc_efficiency_table(df)
        assert with_drops.row(0, named=True)["denominator"] == 5
        assert without_drops.row(0, named=True)["denominator"] == 4

    def test_raises_on_missing_efficiency_column(self) -> None:
        df = canonical_plays_with_scores(n_games=1, plays_per_game=4).drop("efficiency")
        with pytest.raises(
            explosiveness.MissingExplosivenessColumns, match="efficiency"
        ) as excinfo:
            explosiveness.hc_efficiency_table(df)
        assert "corpus" in str(excinfo.value)

    def test_out_of_domain_value_summed_and_counted(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1,
            plays_per_game=4,
            overrides={"play_type": "pass"},
            extras={"thrown_by": "QB A", "efficiency": [1, 1, 0, 9]},
        )
        result = explosiveness.hc_efficiency_table(df)
        row = result.row(0, named=True)
        assert row["efficiency_sum"] == 11
        assert row["out_of_domain"] == 1

    def test_empty_input_returns_schema_correct_empty_frame(self) -> None:
        df = canonical_plays_with_scores(n_games=0, plays_per_game=8)
        result = explosiveness.hc_efficiency_table(df)
        assert result.height == 0
        assert "efficiency_sum" in result.columns


# --- calibrate / ExplosivenessCalibration ------------------------------------------------------


class TestCalibrate:
    def test_epa_threshold_matches_q80_of_successes_only(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1, plays_per_game=10, overrides={"play_type": "pass"}
        ).with_columns(epa=pl.Series([float(i) for i in range(1, 11)]))
        calibration = explosiveness.calibrate(df)
        expected = df["epa"].quantile(0.80)
        assert calibration.epa_threshold == expected
        assert calibration.n_success == 10
        assert calibration.epa_quantile == 0.80

    def test_ignores_non_successful_plays(self) -> None:
        epa_values = [float(i) for i in range(1, 11)] + [-3.0] * 50
        df = canonical_plays_with_scores(
            n_games=1, plays_per_game=60, overrides={"play_type": "pass"}
        ).with_columns(epa=pl.Series(epa_values))
        with_failures = explosiveness.calibrate(df)

        only_successes = canonical_plays_with_scores(
            n_games=1, plays_per_game=10, overrides={"play_type": "pass"}
        ).with_columns(epa=pl.Series([float(i) for i in range(1, 11)]))
        baseline = explosiveness.calibrate(only_successes)

        assert with_failures.epa_threshold == baseline.epa_threshold

    def test_raises_when_fewer_than_minimum_successes(self) -> None:
        df = canonical_plays_with_scores(
            n_games=1, plays_per_game=5, overrides={"play_type": "pass"}
        ).with_columns(epa=pl.Series([1.0, 2.0, 3.0, -1.0, -2.0]))
        with pytest.raises(explosiveness.InsufficientCalibrationSample, match="3"):
            explosiveness.calibrate(df)

    def test_corpus_fingerprint_stable_and_changes_on_single_value(self) -> None:
        epa_values = [float(i) for i in range(1, 11)]
        df = canonical_plays_with_scores(
            n_games=1, plays_per_game=10, overrides={"play_type": "pass"}
        ).with_columns(epa=pl.Series(epa_values))
        cal1 = explosiveness.calibrate(df)
        cal2 = explosiveness.calibrate(df)
        assert cal1.corpus_fingerprint == cal2.corpus_fingerprint
        assert cal1.corpus_n == 10
        assert cal1.n_success == 10
        assert cal1.corpus_sources == ("hudl",)

        changed_values = list(epa_values)
        changed_values[0] = 1.5
        changed_df = canonical_plays_with_scores(
            n_games=1, plays_per_game=10, overrides={"play_type": "pass"}
        ).with_columns(epa=pl.Series(changed_values))
        cal3 = explosiveness.calibrate(changed_df)
        assert cal3.corpus_fingerprint != cal1.corpus_fingerprint


class TestCalibrationRoundTrip:
    def test_write_and_load_round_trips_byte_for_byte(self, tmp_path: Path) -> None:
        df = canonical_plays_with_scores(
            n_games=1, plays_per_game=10, overrides={"play_type": "pass"}
        ).with_columns(epa=pl.Series([float(i) for i in range(1, 11)]))
        calibration = explosiveness.calibrate(df)

        path = tmp_path / "calibration.json"
        explosiveness.write_calibration(calibration, path)
        loaded = explosiveness.load_calibration(path)
        assert loaded == calibration

        second_line = path.read_text().lstrip().splitlines()[1].strip()
        assert second_line.startswith('"schema_version"')

    def test_write_creates_parent_directories(self, tmp_path: Path) -> None:
        df = canonical_plays_with_scores(
            n_games=1, plays_per_game=10, overrides={"play_type": "pass"}
        ).with_columns(epa=pl.Series([float(i) for i in range(1, 11)]))
        calibration = explosiveness.calibrate(df)

        path = tmp_path / "nested" / "dir" / "calibration.json"
        explosiveness.write_calibration(calibration, path)
        assert path.exists()

    def test_load_raises_on_unknown_schema_version(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.json"
        path.write_text(json.dumps({"schema_version": 999}))
        with pytest.raises(explosiveness.UnknownCalibrationSchema, match="999"):
            explosiveness.load_calibration(path)


# --- success_flag / explosive_epa_flag / explosive_score ----------------------------------------


class TestSuccessFlag:
    def test_strictly_greater_than_zero(self) -> None:
        df = canonical_plays_with_scores(n_games=1, plays_per_game=3).with_columns(
            epa=pl.Series([0.5, 0.0, -0.5])
        )
        result = df.select(explosiveness.success_flag().alias("s"))["s"].to_list()
        assert result == [True, False, False]


class TestExplosiveEpaFlag:
    def test_threshold_inclusive_and_boundaries(self) -> None:
        calibration = _make_calibration(threshold=2.0)
        epa_values = [2.0, math.nextafter(2.0, 0.0), 1.0, -1.0]
        df = canonical_plays_with_scores(n_games=1, plays_per_game=4).with_columns(
            epa=pl.Series(epa_values)
        )
        flags = df.select(explosiveness.explosive_epa_flag(calibration).alias("f"))[
            "f"
        ].to_list()
        assert flags == [True, False, False, False]


class TestExplosiveScore:
    def test_bounds_monotone_and_half_at_threshold(self) -> None:
        calibration = _make_calibration(threshold=2.0, iqr=1.0)
        df = canonical_plays_with_scores(n_games=1, plays_per_game=5).with_columns(
            epa=pl.Series([-5.0, 0.0, 2.0, 5.0, 10.0])
        )
        scores = df.select(explosiveness.explosive_score(calibration).alias("s"))["s"].to_list()
        assert all(0.0 < s < 1.0 for s in scores)
        assert scores == sorted(scores)
        assert scores[2] == pytest.approx(0.5)

    def test_eleven_vs_twelve_yard_cliff_measurably_disappears(self) -> None:
        # Headline regression guard (RESEARCH/CONTEXT): encodes the user's objection --
        # "was ist, wenn eine Spielerin nur 11 Yards erzielt?" -- two plays with
        # near-identical EPA must score nearly identically, regardless of the yard-based
        # cliff the head coach's rule creates at 12.
        calibration = _make_calibration(threshold=2.0, iqr=1.0)
        df = canonical_plays_with_scores(
            n_games=1, plays_per_game=2, overrides={"yards_gained": [11, 12]}
        ).with_columns(epa=pl.Series([1.5, 1.55]))
        scores = df.select(explosiveness.explosive_score(calibration).alias("s"))["s"].to_list()
        assert abs(scores[1] - scores[0]) < 0.05
