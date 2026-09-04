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
