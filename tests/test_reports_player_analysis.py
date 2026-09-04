"""Unit coverage for `flag_football_ep.reports.player_analysis`.

Every test builds inputs with `flag_football_ep.testing.canonical_plays` plus targeted
`overrides`/`extras`, matching `tests/test_reports_own_team.py`'s fixture style. Each test is
named after the `<behavior>` bullet it proves, cited to the formula cell it reproduces
(`M3-04-RESEARCH.md` Pattern 1).
"""

from __future__ import annotations

import polars as pl
import pytest

from flag_football_ep.reports.player_analysis import (
    HcColumnTable,
    _HC_COLUMN_SCHEMA,
    hc_columns_by_qb,
)
from flag_football_ep.testing import canonical_plays


def _row(table: pl.DataFrame, spieler: str) -> dict:
    matches = table.filter(pl.col("spieler") == spieler)
    assert matches.height == 1, f"expected exactly one row for {spieler!r}, got {matches.height}"
    return matches.to_dicts()[0]


# --- Task 1: counting/yardage columns, on his denominators ------------------


class TestCountingAndYardageColumns:
    def test_hc_attempts_excludes_sacks(self) -> None:
        """One QB, one completion, one incompletion, one interception, two sacks:
        comps==1, incs==1, ints==1, sacks==2, attempts==3 (Attempts = Comps+Incs+INTs,
        Sacks is a separate column, workbook cell D2)."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=5,
            overrides={
                "complete_pass": [1, 0, 0, 0, 0],
                "interception": [0, 0, 1, 0, 0],
                "sack": [0, 0, 0, 1, 1],
            },
            extras={"thrown_by": ["QB1"] * 5},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["comps"] == 1
        assert row["incs"] == 1
        assert row["ints"] == 1
        assert row["sacks"] == 2
        assert row["attempts"] == 3

    def test_comp_pct_matches_comps_over_attempts(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=4,
            overrides={"complete_pass": [1, 1, 0, 0], "interception": [0, 0, 0, 0]},
            extras={"thrown_by": ["QB1"] * 4},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["comp_pct"] == pytest.approx(row["comps"] / row["attempts"])

    def test_comp_pct_null_on_zero_attempts_and_muted_below_threshold(self) -> None:
        """A QB with only a sack (present via the sacks scope, zero Comps/Incs/INTs):
        attempts == 0 (a real number, not null -- they are known to have thrown), comp_pct
        is null (never his `iferror(...,0)` zero), and they are flagged muted."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=1,
            overrides={"sack": [1]},
            extras={"thrown_by": ["QB1"]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["attempts"] == 0
        assert row["comp_pct"] is None
        assert row["muted"] is True

    def test_rushing_only_qb_gets_null_pass_columns(self) -> None:
        """A QB who never appears in the pass/sack scope at all (rushing-only) gets NULL
        pass columns, not a fabricated zero (distinct from the sack-only case above)."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"play_type": ["run", "run"]},
            extras={"qb": ["QB1", "QB1"]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["attempts"] is None
        assert row["comp_pct"] is None
        assert row["carries"] == 2

    def test_pass_yards_only_counts_completions(self) -> None:
        """An incompletion with a non-zero yards_gained never contributes to Pass Yards."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={
                "complete_pass": [1, 0],
                "yards_gained": [10, 25],
            },
            extras={"thrown_by": ["QB1", "QB1"]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["pass_yards"] == 10.0

    def test_rush_columns_do_not_touch_pass_columns(self) -> None:
        """A 30-yard run for the same QB never changes attempts, ypa or any pass column."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=3,
            overrides={
                "play_type": ["pass", "pass", "run"],
                "complete_pass": [1, 0, 0],
                "yards_gained": [10, 0, 30],
                "touchdown": [0, 0, 1],
            },
            extras={"thrown_by": ["QB1", "QB1", None], "qb": [None, None, "QB1"]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["attempts"] == 2
        assert row["pass_yards"] == 10.0
        assert row["ypa"] == pytest.approx(5.0)
        assert row["carries"] == 1
        assert row["rush_yards"] == 30.0
        assert row["rush_tds"] == 1

    def test_player_identity_coalesces_thrown_by_and_qb(self) -> None:
        """A rushing QB (thrown_by null on the run row) resolves via `qb`."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"play_type": ["pass", "run"], "complete_pass": [1, 0]},
            extras={"thrown_by": ["QB1", None], "qb": ["QB1", "QB1"]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert result.table["spieler"].to_list() == ["QB1"]
        row = _row(result.table, "QB1")
        assert row["carries"] == 1

    def test_rows_with_null_identity_excluded(self) -> None:
        """A row with both thrown_by and qb null is excluded, not grouped under a null key."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 1]},
            extras={"thrown_by": ["QB1", None], "qb": [None, None]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert None not in result.table["spieler"].to_list()
        assert result.table["spieler"].to_list() == ["QB1"]

    def test_air_yards_null_when_column_absent(self) -> None:
        df = canonical_plays(
            n_games=1, plays_per_game=1, extras={"thrown_by": ["QB1"]}
        ).drop("air_yards")
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["air_yards"] is None

    def test_air_yards_summed_on_completions_when_present(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 0]},
            extras={"thrown_by": ["QB1", "QB1"], "air_yards": [8, 99]},
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["air_yards"] == 8.0

    def test_empty_frame_returns_schema_correct_table(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=1).filter(pl.lit(False))
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert result.table.height == 0
        assert result.table.schema == _HC_COLUMN_SCHEMA


# --- Task 2: delegated columns and named availability state -----------------


class TestDelegatedColumnsAndAvailability:
    def test_exp_plays_and_explosive_pct_match_explosiveness_module(self) -> None:
        from flag_football_ep.features.explosiveness import hc_workbook_explosive_rate

        df = canonical_plays(
            n_games=1,
            plays_per_game=3,
            overrides={"yards_gained": [20, 5, 3]},
            extras={"thrown_by": ["QB1", "QB1", "QB1"]},
        )
        expected = hc_workbook_explosive_rate(df, group_col="thrown_by")
        expected_row = expected.filter(pl.col("thrown_by") == "QB1").to_dicts()[0]

        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["exp_plays"] == expected_row["exp_plays"]
        assert row["explosive_pct"] == pytest.approx(expected_row["explosive_pct"])

    def test_efficiency_unavailable_when_column_has_no_real_signal(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=1, extras={"thrown_by": ["QB1"]})
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["efficiency"] is None
        assert "efficiency" in result.unavailable
        assert any("Efficiency" in n for n in result.notices)

    def test_efficiency_computed_when_synthetic_column_present(self) -> None:
        from flag_football_ep.features.explosiveness import hc_efficiency_table

        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 0]},
            extras={"thrown_by": ["QB1", "QB1"], "efficiency": [1, 0]},
        )
        expected = hc_efficiency_table(df, group_col="thrown_by")
        expected_row = expected.filter(pl.col("thrown_by") == "QB1").to_dicts()[0]

        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        assert row["efficiency"] == pytest.approx(expected_row["efficiency"])
        assert "efficiency" not in result.unavailable

    def test_adjusted_columns_unavailable_when_drop_column_absent(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 0]},
            extras={"thrown_by": ["QB1", "QB1"]},
        )
        assert df["drop"].null_count() == df.height  # sanity: no real drop signal

        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        for column in ("adj_comp_pct", "adj_pass_yards", "adj_ypa"):
            assert row[column] is None
            assert column in result.unavailable
        assert row["adj_comp_pct"] != row["comp_pct"]

    def test_adjusted_columns_computed_when_synthetic_drop_column_present(self) -> None:
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 0], "yards_gained": [10, 0]},
            extras={
                "thrown_by": ["QB1", "QB1"],
                "drop": [None, "X"],
                "air_yards": [None, 7],
            },
        )
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        for column in ("adj_comp_pct", "adj_pass_yards", "adj_ypa"):
            assert column not in result.unavailable
        assert row["adj_comp_pct"] == pytest.approx((1 + 1) / 2)
        assert row["adj_pass_yards"] == pytest.approx(17.0)

    def test_drop_flag_never_derived_from_other_columns(self) -> None:
        """The drop flag is `drop` non-null and non-empty after stripping -- never derived
        from any other column. A whitespace-only drop value is not flagged."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=2,
            overrides={"complete_pass": [1, 0]},
            extras={"thrown_by": ["QB1", "QB1"], "drop": ["X", "   "]},
        )
        # "X" is a real flag (row 1); the whitespace-only row 2 is not -- so drop IS
        # available overall (row 1 has real signal), but the whitespace row must not be
        # counted as a dropped incompletion.
        result = hc_columns_by_qb(df, group_col="thrown_by")
        row = _row(result.table, "QB1")
        # row 1 is a completion (not eligible as a dropped incompletion), row 2 is
        # whitespace-only (not a real flag) -- so no dropped incompletions at all.
        assert row["adj_comp_pct"] == pytest.approx(row["comp_pct"])

    def test_notices_include_air_yards_deviation_when_available(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=1, extras={"thrown_by": ["QB1"]})
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert any("Air Yards" in n for n in result.notices)

    def test_notices_include_hc_workbook_corpus_count(self) -> None:
        df = canonical_plays(n_games=1, plays_per_game=1, extras={"thrown_by": ["QB1"]})
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert any("hc_workbook" in n for n in result.notices)

    def test_unavailable_and_notices_nonempty_on_todays_real_column_set(self) -> None:
        """`canonical_plays()`'s default frame carries `drop`/`efficiency` as present but
        entirely null columns -- exactly today's real corpus state (zero HC rows ingested
        into `plays_scored.parquet` yet). Both must still be treated as unavailable."""
        df = canonical_plays(n_games=1, plays_per_game=2, extras={"thrown_by": ["QB1", "QB1"]})
        result = hc_columns_by_qb(df, group_col="thrown_by")
        assert result.unavailable
        assert result.notices
        assert {"adj_comp_pct", "adj_pass_yards", "adj_ypa", "efficiency"} <= set(
            result.unavailable
        )
