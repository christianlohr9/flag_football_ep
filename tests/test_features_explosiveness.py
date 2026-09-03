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

import polars as pl
import pytest

from flag_football_ep.features import explosiveness
from flag_football_ep.testing import canonical_plays_with_scores


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
