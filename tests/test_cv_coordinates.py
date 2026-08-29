"""Math-only unit tests for `flag_football_ep.cv.coordinates` -- no real video, no
model weights, no real calibration data. Every calibration used here is a small
synthetic, exactly-solvable pixel->yards mapping built in-memory.
"""

from __future__ import annotations

import warnings

import polars as pl
import pytest

pytest.importorskip("cv2", reason="requires the cv extras group (uv sync --extra cv)")

from flag_football_ep.config import load_config
from flag_football_ep.cv.homography import CalibrationError
from flag_football_ep.cv.coordinates import add_field_coordinates, foot_point
from flag_football_ep.testing import synthetic_tracks

CONFIG_PATH = "ffep.toml"


def _config():
    return load_config(CONFIG_PATH)


# A pure axis-aligned scale (no rotation, no perspective) from a 0..1000px square to
# the west half of the D-13 field-yard box -- cv2.findHomography reproduces an affine
# scale essentially exactly, so tests can assert tight tolerances without depending on
# a full DLT solve's numerical slack.
_SCALE_X = 0.05  # 1000px -> 50 yards
_SCALE_Y = 0.025  # 1000px -> 25 yards


def _scale_calibration_rows(hover_position_id: str) -> list[dict[str, object]]:
    corners_px = [(0.0, 0.0), (1000.0, 0.0), (1000.0, 1000.0), (0.0, 1000.0)]
    names = [
        "goalline_west_south",
        "goalline_east_south",
        "goalline_east_north",
        "goalline_west_north",
    ]
    rows = []
    for name, (sx, sy) in zip(names, corners_px):
        rows.append(
            {
                "hover_position_id": hover_position_id,
                "landmark": name,
                "source_x_px": sx,
                "source_y_px": sy,
                "target_x_yards": sx * _SCALE_X,
                "target_y_yards": sy * _SCALE_Y,
                "use_for_fit": True,
                "notes": "",
            }
        )
    return rows


def _calibration(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows)


# --- foot_point ------------------------------------------------------------------


def test_foot_point_returns_bottom_center_of_known_box() -> None:
    assert foot_point((10.0, 20.0, 50.0, 100.0)) == (30.0, 100.0)


def test_foot_point_handles_non_axis_ordered_floats() -> None:
    x, y = foot_point((100.5, 40.25, 140.5, 220.75))
    assert x == pytest.approx(120.5)
    assert y == pytest.approx(220.75)


# --- add_field_coordinates: happy path --------------------------------------------


def test_add_field_coordinates_projects_known_pixel_input_to_expected_yards() -> None:
    cfg = _config()
    calibration = _calibration(_scale_calibration_rows("hp-scale"))

    tracks = synthetic_tracks(n_clips=1, n_frames=1, n_tracks=1)
    tracks = tracks.with_columns(
        pl.lit("hp-scale").alias("hover_position_id"),
        pl.lit(200.0).alias("foot_x_px"),
        pl.lit(400.0).alias("foot_y_px"),
    )

    projected = add_field_coordinates(tracks, cfg, calibration)

    assert projected.height == tracks.height
    x_yards = projected["x_yards"].to_list()
    y_yards = projected["y_yards"].to_list()
    for x in x_yards:
        assert x == pytest.approx(200.0 * _SCALE_X, abs=1e-3)
    for y in y_yards:
        assert y == pytest.approx(400.0 * _SCALE_Y, abs=1e-3)


def test_add_field_coordinates_two_hover_positions_get_separate_transforms() -> None:
    cfg = _config()
    calibration = pl.concat(
        [
            _calibration(_scale_calibration_rows("hp-a")),
            # hp-b uses a different (larger) scale, so the same pixel input must
            # project to a different yard output than hp-a's.
            pl.DataFrame(
                [
                    {
                        "hover_position_id": "hp-b",
                        "landmark": name,
                        "source_x_px": sx,
                        "source_y_px": sy,
                        "target_x_yards": sx * _SCALE_X * 2.0,
                        "target_y_yards": sy * _SCALE_Y * 2.0,
                        "use_for_fit": True,
                        "notes": "",
                    }
                    for name, (sx, sy) in zip(
                        [
                            "goalline_west_south",
                            "goalline_east_south",
                            "goalline_east_north",
                            "goalline_west_north",
                        ],
                        [(0.0, 0.0), (1000.0, 0.0), (1000.0, 1000.0), (0.0, 1000.0)],
                    )
                ]
            ),
        ],
        how="vertical",
    )

    tracks = synthetic_tracks(n_clips=2, n_frames=1, n_tracks=1)
    tracks = tracks.with_columns(
        pl.when(pl.col("clip_number") == 1)
        .then(pl.lit("hp-a"))
        .otherwise(pl.lit("hp-b"))
        .alias("hover_position_id"),
        pl.lit(400.0).alias("foot_x_px"),
        pl.lit(400.0).alias("foot_y_px"),
    )

    projected = add_field_coordinates(tracks, cfg, calibration)

    hp_a_x = projected.filter(pl.col("hover_position_id") == "hp-a")["x_yards"].to_list()
    hp_b_x = projected.filter(pl.col("hover_position_id") == "hp-b")["x_yards"].to_list()

    assert hp_a_x
    assert hp_b_x
    for x in hp_a_x:
        assert x == pytest.approx(400.0 * _SCALE_X, abs=1e-3)
    for x in hp_b_x:
        assert x == pytest.approx(400.0 * _SCALE_X * 2.0, abs=1e-3)
    # the two hover positions must land on genuinely different values, not the same
    # transform applied twice.
    assert hp_a_x[0] != pytest.approx(hp_b_x[0])


# --- add_field_coordinates: missing calibration -----------------------------------


def test_add_field_coordinates_missing_calibration_raises_naming_hover_position() -> None:
    cfg = _config()
    calibration = _calibration(_scale_calibration_rows("hp-known"))

    tracks = synthetic_tracks(n_clips=1, n_frames=1, n_tracks=1)
    tracks = tracks.with_columns(pl.lit("hp-does-not-exist").alias("hover_position_id"))

    with pytest.raises(CalibrationError) as exc_info:
        add_field_coordinates(tracks, cfg, calibration)
    assert "hp-does-not-exist" in str(exc_info.value)


# --- add_field_coordinates: out-of-bounds notice ----------------------------------


def test_add_field_coordinates_out_of_bounds_rows_kept_and_counted_in_warning() -> None:
    cfg = _config()
    calibration = _calibration(_scale_calibration_rows("hp-scale"))

    tracks = synthetic_tracks(n_clips=1, n_frames=1, n_tracks=2)
    # Track 0 is the referee row synthetic_tracks always seeds; give it and the one
    # player row wildly out-of-frame pixel coordinates so both project well outside
    # the field-plus-margin box.
    tracks = tracks.with_columns(
        pl.lit("hp-scale").alias("hover_position_id"),
        pl.lit(50000.0).alias("foot_x_px"),
        pl.lit(50000.0).alias("foot_y_px"),
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        projected = add_field_coordinates(tracks, cfg, calibration)

    # Rows are kept, not dropped.
    assert projected.height == tracks.height
    assert projected["x_yards"].null_count() == 0
    assert projected["y_yards"].null_count() == 0

    user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
    assert user_warnings
    message = str(user_warnings[0].message)
    assert "hp-scale" in message
    assert str(tracks.height) in message


# --- add_field_coordinates: schema round trip -------------------------------------


def test_add_field_coordinates_returned_frame_passes_conform_tracking_and_keeps_pbp_null() -> None:
    cfg = _config()
    calibration = _calibration(_scale_calibration_rows("hp-scale"))

    tracks = synthetic_tracks(n_clips=1, n_frames=2, n_tracks=2)
    tracks = tracks.with_columns(pl.lit("hp-scale").alias("hover_position_id"))

    from flag_football_ep.cv.schema import TRACKING_COLUMNS, conform_tracking

    projected = add_field_coordinates(tracks, cfg, calibration)

    # conform_tracking is idempotent on an already-conformed frame -- calling it again
    # here proves the returned frame already satisfies the schema contract.
    reconformed = conform_tracking(projected)
    assert tuple(reconformed.columns) == TRACKING_COLUMNS
    assert reconformed.equals(projected)

    assert projected["game_id"].null_count() == projected.height
    assert projected["play_id"].null_count() == projected.height
