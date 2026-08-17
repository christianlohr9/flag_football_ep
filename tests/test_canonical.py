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
