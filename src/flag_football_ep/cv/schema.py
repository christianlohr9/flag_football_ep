"""Canonical tracking-output schema: columns, dtypes, and Parquet write discipline.

Owns the single typed-schema definition every producer/consumer of tracking data
conforms to, the same "column-tuple + typed-schema-dict" pattern
`tests/test_capture_artifacts.py`'s `INVENTORY_COLUMNS`/`INVENTORY_SCHEMA` already
establishes. Must include a `session_id`/`clip_number` (the pilot's pseudo play key)
*and* nullable `game_id`/`play_id` columns, cast to the exact dtypes
`data/reference/video_sync.csv` and `plays.parquet` use (`pl.Utf8`/`pl.Int32`), so a
future join against `video_sync.csv` never re-introduces the dtype-mismatch silent-zero-
row-join risk `docs/sync-convention.md` and `tests/test_capture_artifacts.py` already
guard against for the charting pipeline.

`polars` is imported at module level here (unlike every other `cv/*` module) because
polars is a core project dependency, not one of the `cv` extras-group packages gated by
D-07/D-08.

`empty_tracking_frame`/`conform_tracking`/`write_tracking_parquet` are implemented by
plan 02.1-05; `TRACKING_COLUMNS`/`TRACKING_SCHEMA`'s final column set is filled in by
that same plan (the names are declared here so every later contract module can import
them without a circular dependency on the implementing plan).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

TRACKING_COLUMNS: tuple[str, ...] = ()
TRACKING_SCHEMA: dict[str, pl.DataType] = {}


def empty_tracking_frame() -> pl.DataFrame:
    """Return a zero-row `pl.DataFrame` typed to `TRACKING_SCHEMA`."""
    raise NotImplementedError("cv.schema.empty_tracking_frame is implemented by plan 02.1-05")


def conform_tracking(df: pl.DataFrame) -> pl.DataFrame:
    """Cast/reorder `df` to exactly `TRACKING_COLUMNS`/`TRACKING_SCHEMA`, raising loudly
    on a missing required column rather than silently dropping or nulling it.
    """
    raise NotImplementedError("cv.schema.conform_tracking is implemented by plan 02.1-05")


def write_tracking_parquet(df: pl.DataFrame, path: Path) -> Path:
    """Atomically write `df` (already conformed via `conform_tracking`) to `path`,
    matching `pipeline._atomic_write_parquet`'s `.tmp` sibling + `os.replace` discipline.
    """
    raise NotImplementedError(
        "cv.schema.write_tracking_parquet is implemented by plan 02.1-05"
    )
