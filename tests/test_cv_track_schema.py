"""Schema gate for the typed XY tracking table -- the Phase-2.3-facing contract.

Mirrors `tests/test_capture_artifacts.py`'s style: a declared column-tuple/typed-schema
pair, a conform function that raises loudly rather than silently dropping or nulling
required data, and a dtype-mismatch join regression against both a
`video_sync.csv`-shaped and a `plays.parquet`-shaped frame (RESEARCH Pitfall 1,
docs/sync-convention.md).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from flag_football_ep import canonical
from flag_football_ep.cv.schema import (
    CLASS_VOCABULARY,
    InvalidTrackClass,
    MissingTrackingColumns,
    NullTrackingValues,
    TRACKING_COLUMNS,
    TRACKING_SCHEMA,
    conform_tracking,
    empty_tracking_frame,
    write_tracking_parquet,
)
from flag_football_ep.testing import synthetic_tracks

REQUIRED_TRACKING_FIELDS = {
    "session_id": "s1",
    "clip_number": 1,
    "frame_index": 0,
    "timestamp_s": 0.0,
    "track_id": 0,
    "class_name": "player",
    "confidence": 0.9,
    "bbox_x1": 10.0,
    "bbox_y1": 20.0,
    "bbox_x2": 50.0,
    "bbox_y2": 90.0,
    "foot_x_px": 30.0,
    "foot_y_px": 90.0,
    "detector_run_id": "0" * 32,
    "tracked_at": "2026-08-24T00:00:00Z",
}
NULLABLE_TRACKING_FIELDS = {
    "team_id": None,
    "hover_position_id": None,
    "x_yards": None,
    "y_yards": None,
    "game_id": None,
    "play_id": None,
}


def _raw_frame(n_rows: int = 1, **overrides: object) -> pl.DataFrame:
    """One or more identical raw rows carrying every required tracking field,
    plus every nullable field, ready to be mutated via ``overrides``.
    """
    row = {**REQUIRED_TRACKING_FIELDS, **NULLABLE_TRACKING_FIELDS}
    row.update(overrides)
    return pl.DataFrame([dict(row) for _ in range(n_rows)])


def test_tracking_columns_and_schema_share_declared_order_and_key_set() -> None:
    assert set(TRACKING_COLUMNS) == set(TRACKING_SCHEMA)
    assert tuple(TRACKING_SCHEMA.keys()) == TRACKING_COLUMNS


def test_game_id_play_id_dtypes_mirror_canonical() -> None:
    assert TRACKING_SCHEMA["game_id"] == canonical.CORE_COLUMNS["game_id"]
    assert TRACKING_SCHEMA["play_id"] == canonical.CORE_COLUMNS["play_id"]


def test_empty_tracking_frame_has_exact_schema_and_zero_rows() -> None:
    df = empty_tracking_frame()
    assert df.height == 0
    assert df.columns == list(TRACKING_COLUMNS)
    assert dict(df.schema) == TRACKING_SCHEMA


def test_conform_tracking_fills_absent_nullable_columns_with_typed_nulls() -> None:
    raw = pl.DataFrame([dict(REQUIRED_TRACKING_FIELDS)])
    conformed = conform_tracking(raw)

    assert conformed.columns == list(TRACKING_COLUMNS)
    for name in NULLABLE_TRACKING_FIELDS:
        assert conformed[name].null_count() == 1
        assert conformed.schema[name] == TRACKING_SCHEMA[name]
    for name, value in REQUIRED_TRACKING_FIELDS.items():
        assert conformed[name].to_list() == [value]


def test_conform_tracking_missing_required_column_raises_named_exception() -> None:
    raw = _raw_frame().drop("track_id")
    with pytest.raises(MissingTrackingColumns, match="track_id"):
        conform_tracking(raw)


def test_conform_tracking_null_in_not_null_column_raises_named_exception() -> None:
    raw = _raw_frame(session_id=None)
    with pytest.raises(NullTrackingValues, match="session_id"):
        conform_tracking(raw)


def test_conform_tracking_rejects_ball_class() -> None:
    raw = _raw_frame(class_name="ball")
    with pytest.raises(InvalidTrackClass, match="ball"):
        conform_tracking(raw)
    assert "ball" not in CLASS_VOCABULARY


def test_conform_tracking_pilot_case_null_game_id_play_id_round_trips(
    tmp_path: Path,
) -> None:
    raw = _raw_frame()
    path = tmp_path / "tracks.parquet"
    written = write_tracking_parquet(raw, path)

    reread = pl.read_parquet(written)
    assert reread.schema["game_id"] == TRACKING_SCHEMA["game_id"]
    assert reread.schema["play_id"] == TRACKING_SCHEMA["play_id"]
    assert reread["game_id"].null_count() == 1
    assert reread["play_id"].null_count() == 1


def test_dtype_join_regression_against_sync_and_plays_shaped_frames() -> None:
    """A tracking frame with game_id/play_id populated must inner-join cleanly
    against both a video_sync.csv-shaped frame and a plays.parquet-shaped frame
    (built via flag_football_ep.testing's canonical factory) -- a dtype mismatch
    here silently yields a zero-row join (docs/sync-convention.md, RESEARCH Pitfall 1).
    """
    from flag_football_ep.testing import canonical_plays

    game_id = "2026-06-14_GER-vs-AUT_EM-QUALI"
    play_id = 3
    tracks = conform_tracking(_raw_frame(game_id=game_id, play_id=play_id))

    sync = pl.DataFrame(
        {
            "game_id": [game_id],
            "play_id": [play_id],
            "video_file": ["clip_01.mp4"],
            "snap_frame": [10],
            "snap_seconds": [0.33],
            "end_seconds": [None],
            "notes": [None],
        }
    ).cast(
        {
            "game_id": pl.Utf8,
            "play_id": pl.Int32,
            "video_file": pl.Utf8,
            "snap_frame": pl.Int64,
            "snap_seconds": pl.Float64,
            "end_seconds": pl.Float64,
            "notes": pl.Utf8,
        }
    )

    plays = canonical_plays(n_games=1, plays_per_game=1).with_columns(
        [
            pl.lit(game_id).alias("game_id"),
            pl.lit(play_id).cast(pl.Int32).alias("play_id"),
        ]
    )

    tracks_x_sync = tracks.join(sync, on=["game_id", "play_id"], how="inner")
    assert tracks_x_sync.height > 0, (
        "tracks x video_sync-shaped join produced 0 rows -- dtype mismatch against "
        "canonical game_id/play_id (docs/sync-convention.md)"
    )

    tracks_x_plays = tracks.join(plays, on=["game_id", "play_id"], how="inner")
    assert tracks_x_plays.height > 0, (
        "tracks x plays-shaped join produced 0 rows -- dtype mismatch against "
        "canonical game_id/play_id (docs/sync-convention.md)"
    )


def test_write_tracking_parquet_leaves_no_tmp_file(tmp_path: Path) -> None:
    raw = _raw_frame()
    path = tmp_path / "tracks.parquet"
    write_tracking_parquet(raw, path)

    assert path.exists()
    assert not path.with_suffix(path.suffix + ".tmp").exists()


def test_write_tracking_parquet_leaves_previous_file_intact_on_conform_failure(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tracks.parquet"
    write_tracking_parquet(_raw_frame(), path)
    before = path.read_bytes()

    bad = _raw_frame(class_name="ball")
    with pytest.raises(InvalidTrackClass):
        write_tracking_parquet(bad, path)

    assert path.read_bytes() == before
    assert not path.with_suffix(path.suffix + ".tmp").exists()


# -- Task 2: synthetic_tracks factory -----------------------------------------------


def test_synthetic_tracks_default_output_passes_conform_unchanged() -> None:
    df = synthetic_tracks()
    conformed = conform_tracking(df)
    assert conformed.equals(df.select(list(TRACKING_COLUMNS)))
    assert conformed.height > 0


def test_synthetic_tracks_is_deterministic() -> None:
    a = synthetic_tracks()
    b = synthetic_tracks()
    assert a.equals(b)


def test_synthetic_tracks_with_teams_fills_only_team_id() -> None:
    df = synthetic_tracks(with_teams=True)
    players = df.filter(pl.col("class_name") == "player")
    referees = df.filter(pl.col("class_name") == "referee")

    assert players["team_id"].null_count() == 0
    assert referees.height > 0
    assert referees["team_id"].null_count() == referees.height

    for name in ("hover_position_id", "x_yards", "y_yards", "game_id", "play_id"):
        assert df[name].null_count() == df.height


def test_synthetic_tracks_with_field_coords_fills_only_field_coords() -> None:
    df = synthetic_tracks(with_field_coords=True)

    assert df["x_yards"].null_count() == 0
    assert df["y_yards"].null_count() == 0
    for name in ("team_id", "hover_position_id", "game_id", "play_id"):
        assert df[name].null_count() == df.height


def test_synthetic_tracks_with_pbp_keys_fills_only_game_and_play_id() -> None:
    df = synthetic_tracks(with_pbp_keys=True)

    assert df["game_id"].null_count() == 0
    assert df["play_id"].null_count() == 0
    for name in ("team_id", "hover_position_id", "x_yards", "y_yards"):
        assert df[name].null_count() == df.height
