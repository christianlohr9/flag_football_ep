"""Canonical tracking-output schema: columns, dtypes, and Parquet write discipline.

Owns the single typed-schema definition every producer/consumer of tracking data
conforms to, the same "column-tuple + typed-schema-dict" pattern
`tests/test_capture_artifacts.py`'s `INVENTORY_COLUMNS`/`INVENTORY_SCHEMA` already
establishes. Includes a `session_id`/`clip_number` (the pilot's pseudo play key, D-02)
*and* nullable `game_id`/`play_id` columns, cast to the exact dtypes
`canonical.CORE_COLUMNS` declares for the charting pipeline (`pl.Utf8`/`pl.Int32`), so a
future join against `video_sync.csv` -> `plays.parquet` never re-introduces the
dtype-mismatch silent-zero-row-join risk `docs/sync-convention.md` and
`tests/test_capture_artifacts.py` already guard against there.

`polars` is imported at module level here (unlike every other `cv/*` module) because
polars is a core project dependency, not one of the `cv` extras-group packages gated by
D-07/D-08.

Implemented by plan 02.1-05.
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl

from flag_football_ep import canonical

TRACKING_COLUMNS: tuple[str, ...] = (
    "session_id",
    "clip_number",
    "frame_index",
    "timestamp_s",
    "track_id",
    "class_name",
    "confidence",
    "bbox_x1",
    "bbox_y1",
    "bbox_x2",
    "bbox_y2",
    "foot_x_px",
    "foot_y_px",
    "team_id",
    "hover_position_id",
    "x_yards",
    "y_yards",
    "game_id",
    "play_id",
    "detector_run_id",
    "tracked_at",
)

TRACKING_SCHEMA: dict[str, pl.DataType] = {
    "session_id": pl.Utf8,
    "clip_number": pl.Int32,
    "frame_index": pl.Int32,
    "timestamp_s": pl.Float64,
    "track_id": pl.Int32,
    "class_name": pl.Utf8,
    "confidence": pl.Float64,
    "bbox_x1": pl.Float64,
    "bbox_y1": pl.Float64,
    "bbox_x2": pl.Float64,
    "bbox_y2": pl.Float64,
    "foot_x_px": pl.Float64,
    "foot_y_px": pl.Float64,
    # team_id/hover_position_id filled by plan 02.1-12; x_yards/y_yards by plan 02.1-13.
    "team_id": pl.Int32,
    "hover_position_id": pl.Utf8,
    "x_yards": pl.Float64,
    "y_yards": pl.Float64,
    # game_id/play_id MUST mirror canonical.CORE_COLUMNS's declared dtypes exactly --
    # this is the join-safety contract docs/sync-convention.md and
    # tests/test_capture_artifacts.py already establish for the charting pipeline.
    "game_id": canonical.CORE_COLUMNS["game_id"],
    "play_id": canonical.CORE_COLUMNS["play_id"],
    "detector_run_id": pl.Utf8,
    "tracked_at": pl.Utf8,
}

# Nullable per <interfaces>: null for the pilot friendly (D-02) until the join keys are
# filled, or until plans 02.1-12/02.1-13 backfill team/field-coordinate columns.
NULLABLE_COLUMNS: frozenset[str] = frozenset(
    {"team_id", "hover_position_id", "x_yards", "y_yards", "game_id", "play_id"}
)

# Every declared column outside NULLABLE_COLUMNS must never hold a null once
# conform_tracking has run.
NOT_NULL_COLUMNS: tuple[str, ...] = tuple(
    name for name in TRACKING_COLUMNS if name not in NULLABLE_COLUMNS
)

# The pilot detects no ball (project constraint C-12); a third class means an upstream
# detector/tracker mistake, not data to silently keep.
CLASS_VOCABULARY: frozenset[str] = frozenset({"player", "referee"})


class MissingTrackingColumns(Exception):
    """Raised when `conform_tracking`'s input is missing one or more required
    (not-null) tracking columns."""


class NullTrackingValues(Exception):
    """Raised when a not-null tracking column still holds a null after
    `conform_tracking` casts and reorders the frame."""


class InvalidTrackClass(Exception):
    """Raised when `class_name` holds a value outside `CLASS_VOCABULARY`
    ("player", "referee") -- the pilot detects no ball (C-12)."""


def empty_tracking_frame() -> pl.DataFrame:
    """Return a zero-row `pl.DataFrame` typed to `TRACKING_SCHEMA`."""
    return pl.DataFrame(schema=dict(TRACKING_SCHEMA)).select(list(TRACKING_COLUMNS))


def conform_tracking(df: pl.DataFrame) -> pl.DataFrame:
    """Cast/reorder `df` to exactly `TRACKING_COLUMNS`/`TRACKING_SCHEMA`.

    Raises loudly, never silently drops or nulls a required value: a required
    (not-null) column absent from `df` raises `MissingTrackingColumns` naming it; an
    absent nullable column is instead materialized as a typed null (this is what lets
    plan 02.1-12 write pixel-only rows and plan 02.1-13 fill field coordinates later
    with no schema change); a not-null column holding a null after casting raises
    `NullTrackingValues`; a `class_name` value outside `CLASS_VOCABULARY` raises
    `InvalidTrackClass` naming the offending values.
    """
    missing_required = [name for name in NOT_NULL_COLUMNS if name not in df.columns]
    if missing_required:
        raise MissingTrackingColumns(
            f"Missing required tracking columns: {', '.join(missing_required)}"
        )

    missing_nullable = [name for name in NULLABLE_COLUMNS if name not in df.columns]
    if missing_nullable:
        df = df.with_columns(
            [
                pl.lit(None).cast(TRACKING_SCHEMA[name]).alias(name)
                for name in missing_nullable
            ]
        )

    df = df.with_columns(
        [
            pl.col(name).cast(TRACKING_SCHEMA[name], strict=False).alias(name)
            for name in TRACKING_COLUMNS
        ]
    )
    df = df.select(list(TRACKING_COLUMNS))

    null_violations = [name for name in NOT_NULL_COLUMNS if df[name].null_count() > 0]
    if null_violations:
        raise NullTrackingValues(
            f"Null values found in not-null tracking columns: {', '.join(null_violations)}"
        )

    bad_classes = sorted(set(df["class_name"].to_list()) - CLASS_VOCABULARY)
    if bad_classes:
        raise InvalidTrackClass(
            f"class_name has values outside {sorted(CLASS_VOCABULARY)}: {bad_classes} "
            "-- the pilot detects no ball (C-12)"
        )

    return df


def write_tracking_parquet(df: pl.DataFrame, path: Path) -> Path:
    """Atomically write `df` (conformed via `conform_tracking` first) to `path`.

    Matches `pipeline._atomic_write_parquet`'s `.tmp` sibling + `os.replace`
    discipline verbatim, including the `finally` cleanup, so the canonical tracking
    artifact every downstream consumer trusts never exists in a half-written state.
    Because `conform_tracking` runs before any file operation, a conform failure
    leaves any pre-existing `path` untouched.
    """
    conformed = conform_tracking(df)

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        conformed.write_parquet(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return path


# --- Detection schema (Phase 2.2, REQ-S2-03) --------------------------------------
#
# The raw per-frame detector output, pre-tracking: one row per detected box, keyed
# by `det_index` (not `track_id` -- tracking has not run yet). Mirrors
# TRACKING_COLUMNS/TRACKING_SCHEMA column-for-column for every column both schemas
# share (session_id/clip_number/frame_index/timestamp_s/class_name/confidence/
# bbox_*/detector_run_id), so downstream code that already knows the tracking dtypes
# needs no new mental model to read a detections Parquet.
#
# Written by `cv.export.export_detections_parquet` (plan 02.2-08) and read by
# `cv.detect.evaluate_per_domain` (plan 02.2-15) and the active-learning uncertainty
# scorer (plan 02.2-09).

DETECTION_COLUMNS: tuple[str, ...] = (
    "session_id",
    "clip_number",
    "frame_index",
    "timestamp_s",
    "det_index",
    "class_name",
    "confidence",
    "bbox_x1",
    "bbox_y1",
    "bbox_x2",
    "bbox_y2",
    "detector_run_id",
    "detected_at",
)

DETECTION_SCHEMA: dict[str, pl.DataType] = {
    "session_id": pl.Utf8,
    "clip_number": pl.Int32,
    "frame_index": pl.Int32,
    "timestamp_s": pl.Float64,
    "det_index": pl.Int32,
    "class_name": pl.Utf8,
    "confidence": pl.Float64,
    "bbox_x1": pl.Float64,
    "bbox_y1": pl.Float64,
    "bbox_x2": pl.Float64,
    "bbox_y2": pl.Float64,
    "detector_run_id": pl.Utf8,
    "detected_at": pl.Utf8,
}

# Unlike TRACKING_COLUMNS (where team/coordinate columns are filled in by later
# stages), every detection row is complete the moment the detector runs -- no
# detection column is nullable.
DETECTION_NULLABLE_COLUMNS: frozenset[str] = frozenset()

DETECTION_NOT_NULL_COLUMNS: tuple[str, ...] = tuple(
    name for name in DETECTION_COLUMNS if name not in DETECTION_NULLABLE_COLUMNS
)


class MissingDetectionColumns(Exception):
    """Raised when `conform_detections`'s input is missing one or more required
    (not-null) detection columns."""


class NullDetectionValues(Exception):
    """Raised when a not-null detection column still holds a null after
    `conform_detections` casts and reorders the frame."""


class InvalidDetectionClass(Exception):
    """Raised when `class_name` holds a value outside `CLASS_VOCABULARY`
    ("player", "referee") -- the pilot detects no ball (C-12)."""


def empty_detection_frame() -> pl.DataFrame:
    """Return a zero-row `pl.DataFrame` typed to `DETECTION_SCHEMA`."""
    return pl.DataFrame(schema=dict(DETECTION_SCHEMA)).select(list(DETECTION_COLUMNS))


def conform_detections(df: pl.DataFrame) -> pl.DataFrame:
    """Cast/reorder `df` to exactly `DETECTION_COLUMNS`/`DETECTION_SCHEMA`.

    Mirrors `conform_tracking` exactly: raises `MissingDetectionColumns` naming any
    missing required column, `NullDetectionValues` naming any not-null column that
    still holds a null after casting, and `InvalidDetectionClass` naming any
    `class_name` value outside `CLASS_VOCABULARY`. No detection column is nullable
    (`DETECTION_NULLABLE_COLUMNS` is empty), so `conform_tracking`'s
    missing-nullable-materialization step has no equivalent here.
    """
    missing_required = [name for name in DETECTION_NOT_NULL_COLUMNS if name not in df.columns]
    if missing_required:
        raise MissingDetectionColumns(
            f"Missing required detection columns: {', '.join(missing_required)}"
        )

    df = df.with_columns(
        [
            pl.col(name).cast(DETECTION_SCHEMA[name], strict=False).alias(name)
            for name in DETECTION_COLUMNS
        ]
    )
    df = df.select(list(DETECTION_COLUMNS))

    null_violations = [name for name in DETECTION_NOT_NULL_COLUMNS if df[name].null_count() > 0]
    if null_violations:
        raise NullDetectionValues(
            f"Null values found in not-null detection columns: {', '.join(null_violations)}"
        )

    bad_classes = sorted(set(df["class_name"].to_list()) - CLASS_VOCABULARY)
    if bad_classes:
        raise InvalidDetectionClass(
            f"class_name has values outside {sorted(CLASS_VOCABULARY)}: {bad_classes} "
            "-- the pilot detects no ball (C-12)"
        )

    return df


def write_detections_parquet(df: pl.DataFrame, path: Path) -> Path:
    """Atomically write `df` (conformed via `conform_detections` first) to `path`.

    Mirrors `write_tracking_parquet`'s `.tmp` sibling + `os.replace` discipline
    exactly, including the `finally` cleanup.
    """
    conformed = conform_detections(df)

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        conformed.write_parquet(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return path
