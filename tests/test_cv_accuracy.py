"""Math-only unit tests for `flag_football_ep.cv.accuracy` -- no real video, no model
weights, no real ground-truth labelling. Every calibration used here is a small
synthetic, exactly-solvable pixel->yards mapping (mirrors `test_cv_coordinates.py`'s
convention); every track position comes from `testing.synthetic_tracks`.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import polars as pl
import pytest

cv2 = pytest.importorskip("cv2", reason="requires the cv extras group (uv sync --extra cv)")

from flag_football_ep.config import load_config
from flag_football_ep.reference import MissingReferenceFile
from flag_football_ep.cv.accuracy import (
    GT_SCHEMA,
    GtValidationError,
    InsufficientGroundTruth,
    load_gt_positions,
    measure_position_error,
)
from flag_football_ep.testing import synthetic_tracks

CONFIG_PATH = Path("ffep.toml")


def _config():
    return load_config(CONFIG_PATH)


# A pure axis-aligned scale (no rotation, no perspective) from a 0..1000px square to
# a field-yard box -- cv2.findHomography reproduces an affine scale essentially
# exactly, so expected yard offsets from known pixel offsets are exact arithmetic
# (mirrors test_cv_coordinates.py's `_SCALE_X`/`_SCALE_Y` convention).
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
    return [
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
        for name, (sx, sy) in zip(names, corners_px)
    ]


def _config_with_synthetic_calibration(tmp_path: Path, hover_position_id: str = "hp-test"):
    """A `Config` whose `reference.homography_calibration` points at a freshly
    written synthetic (exactly-solvable) calibration CSV -- `measure_position_error`'s
    fixed 3-parameter contract (`gt, tracks, config`) resolves calibration only via
    `config.reference.homography_calibration`, so tests control it entirely through
    this override, never a fourth parameter.
    """
    cfg = _config()
    calib_path = tmp_path / "calibration.csv"
    pl.DataFrame(_scale_calibration_rows(hover_position_id)).write_csv(calib_path)
    return dataclasses.replace(
        cfg, reference=dataclasses.replace(cfg.reference, homography_calibration=calib_path)
    )


def _yards_to_px(x_yards: float, y_yards: float) -> tuple[float, float]:
    return x_yards / _SCALE_X, y_yards / _SCALE_Y


def _gt_row(**overrides) -> dict:
    row = {
        "clip_number": 1,
        "frame_index": 0,
        "gt_id": "c1f0p1",
        "class_name": "player",
        "team_hint": None,
        "foot_x_px": 100.0,
        "foot_y_px": 200.0,
        "hover_position_id": "hp-01",
        "field_zone": "midfield",
        "scale_pair_id": None,
        "scale_true_yards": None,
        "notes": None,
    }
    row.update(overrides)
    return row


def _bulk_gt_rows(n: int, *, prefix: str, **overrides) -> list[dict]:
    return [_gt_row(gt_id=f"{prefix}p{i}", **overrides) for i in range(1, n + 1)]


def _write_gt_csv(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows, schema=GT_SCHEMA).write_csv(path)
    return path


# --- load_gt_positions: validation rules ------------------------------------------


def test_load_gt_positions_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(MissingReferenceFile):
        load_gt_positions(tmp_path / "does-not-exist.csv")


def test_load_gt_positions_header_only_returns_empty_frame_with_warning(tmp_path: Path) -> None:
    path = tmp_path / "gt.csv"
    path.write_text(",".join(GT_SCHEMA) + "\n", encoding="utf-8")
    with pytest.warns(UserWarning):
        df = load_gt_positions(path)
    assert df.height == 0


def test_load_gt_positions_duplicate_gt_id_raises_naming_it(tmp_path: Path) -> None:
    path = _write_gt_csv(
        tmp_path / "gt.csv",
        [_gt_row(gt_id="c1f0p1"), _gt_row(gt_id="c1f0p1", foot_x_px=150.0)],
    )
    with pytest.raises(GtValidationError) as exc:
        load_gt_positions(path)
    assert "c1f0p1" in str(exc.value)


def test_load_gt_positions_bad_field_zone_raises_naming_gt_id(tmp_path: Path) -> None:
    path = _write_gt_csv(
        tmp_path / "gt.csv", [_gt_row(gt_id="c1f0p1", field_zone="not-a-zone")]
    )
    with pytest.raises(GtValidationError) as exc:
        load_gt_positions(path)
    assert "c1f0p1" in str(exc.value)


def test_load_gt_positions_bad_class_name_raises_naming_gt_id(tmp_path: Path) -> None:
    path = _write_gt_csv(tmp_path / "gt.csv", [_gt_row(gt_id="c1f0p1", class_name="ball")])
    with pytest.raises(GtValidationError) as exc:
        load_gt_positions(path)
    assert "c1f0p1" in str(exc.value)


def test_load_gt_positions_null_class_name_does_not_raise(tmp_path: Path) -> None:
    """A freshly `--prepare`-seeded row has no `class_name` yet -- load must not
    reject it just because it has not been labelled."""
    path = _write_gt_csv(
        tmp_path / "gt.csv",
        [_gt_row(gt_id="c1f0p1", class_name=None, foot_x_px=None, foot_y_px=None)],
    )
    df = load_gt_positions(path)
    assert df.height == 1


def test_load_gt_positions_out_of_bounds_pixel_raises_naming_gt_id(tmp_path: Path) -> None:
    # Clip 1 (data/reference/video_inventory.csv) is registered at 1920x1080.
    path = _write_gt_csv(
        tmp_path / "gt.csv",
        [_gt_row(gt_id="c1f0p1", clip_number=1, foot_x_px=99999.0, foot_y_px=200.0)],
    )
    with pytest.raises(GtValidationError) as exc:
        load_gt_positions(path)
    assert "c1f0p1" in str(exc.value)


def test_load_gt_positions_unknown_hover_position_raises_naming_gt_id(tmp_path: Path) -> None:
    path = _write_gt_csv(
        tmp_path / "gt.csv",
        [_gt_row(gt_id="c1f0p1", hover_position_id="hp-does-not-exist")],
    )
    with pytest.raises(GtValidationError) as exc:
        load_gt_positions(path)
    assert "c1f0p1" in str(exc.value)


def test_load_gt_positions_scale_pair_not_two_rows_raises(tmp_path: Path) -> None:
    path = _write_gt_csv(
        tmp_path / "gt.csv",
        [_gt_row(gt_id="c1f0p1", scale_pair_id="sp-1", scale_true_yards=10.0)],
    )
    with pytest.raises(GtValidationError) as exc:
        load_gt_positions(path)
    assert "sp-1" in str(exc.value)


def test_load_gt_positions_scale_pair_mismatched_true_yards_raises(tmp_path: Path) -> None:
    path = _write_gt_csv(
        tmp_path / "gt.csv",
        [
            _gt_row(gt_id="c1f0p1", scale_pair_id="sp-1", scale_true_yards=10.0),
            _gt_row(
                gt_id="c1f0p2", scale_pair_id="sp-1", scale_true_yards=12.0, foot_x_px=110.0
            ),
        ],
    )
    with pytest.raises(GtValidationError) as exc:
        load_gt_positions(path)
    assert "sp-1" in str(exc.value)


def test_load_gt_positions_valid_rows_load_without_error(tmp_path: Path) -> None:
    path = _write_gt_csv(
        tmp_path / "gt.csv",
        [
            _gt_row(gt_id="c1f0p1", scale_pair_id="sp-1", scale_true_yards=10.0),
            _gt_row(
                gt_id="c1f0p2", scale_pair_id="sp-1", scale_true_yards=10.0, foot_x_px=110.0
            ),
            _gt_row(gt_id="c1f0p3", class_name=None, foot_x_px=None, foot_y_px=None),
        ],
    )
    df = load_gt_positions(path)
    assert df.height == 3


# --- measure_position_error --------------------------------------------------------


def test_measure_position_error_exact_match_zero_median_full_match_rate(tmp_path: Path) -> None:
    cfg = _config_with_synthetic_calibration(tmp_path)

    tracks = synthetic_tracks(n_clips=1, n_frames=1, n_tracks=55)
    tracks = tracks.with_columns(pl.lit(25.0).alias("x_yards"), pl.lit(12.5).alias("y_yards"))

    px, py = _yards_to_px(25.0, 12.5)
    gt = pl.DataFrame(
        _bulk_gt_rows(
            50,
            prefix="c1f0",
            clip_number=1,
            frame_index=0,
            foot_x_px=px,
            foot_y_px=py,
            hover_position_id="hp-test",
            field_zone="midfield",
        ),
        schema=GT_SCHEMA,
    )

    result = measure_position_error(gt, tracks, cfg)

    assert result.n_points == 50
    assert result.match_rate == pytest.approx(1.0)
    assert result.median_yards == pytest.approx(0.0, abs=1e-6)
    assert result.p90_yards == pytest.approx(0.0, abs=1e-6)
    assert result.max_yards == pytest.approx(0.0, abs=1e-6)
    assert result.n_unmatched == 0


def test_measure_position_error_known_pixel_offset_yields_expected_yard_error(
    tmp_path: Path,
) -> None:
    cfg = _config_with_synthetic_calibration(tmp_path)

    tracks = synthetic_tracks(n_clips=1, n_frames=1, n_tracks=55)
    tracks = tracks.with_columns(pl.lit(25.0).alias("x_yards"), pl.lit(12.5).alias("y_yards"))

    # Every GT point sits exactly 1.0 yard away (x-only) from every track -- the
    # measured error must equal 1.0 yard exactly for a pure axis-aligned scale
    # calibration, under the project's own homography, not an approximation.
    expected_offset_yards = 1.0
    px, py = _yards_to_px(25.0 + expected_offset_yards, 12.5)
    gt = pl.DataFrame(
        _bulk_gt_rows(
            50,
            prefix="c1f0",
            clip_number=1,
            frame_index=0,
            foot_x_px=px,
            foot_y_px=py,
            hover_position_id="hp-test",
            field_zone="midfield",
        ),
        schema=GT_SCHEMA,
    )

    result = measure_position_error(gt, tracks, cfg)

    assert result.match_rate == pytest.approx(1.0)
    assert result.median_yards == pytest.approx(expected_offset_yards, abs=1e-6)
    assert result.max_yards == pytest.approx(expected_offset_yards, abs=1e-6)


def test_measure_position_error_unmatched_point_lowers_match_rate_and_excluded(
    tmp_path: Path,
) -> None:
    cfg = _config_with_synthetic_calibration(tmp_path)

    tracks = synthetic_tracks(n_clips=1, n_frames=1, n_tracks=49)
    tracks = tracks.with_columns(pl.lit(25.0).alias("x_yards"), pl.lit(12.5).alias("y_yards"))

    px_exact, py_exact = _yards_to_px(25.0, 12.5)
    rows = _bulk_gt_rows(
        49,
        prefix="c1f0",
        clip_number=1,
        frame_index=0,
        foot_x_px=px_exact,
        foot_y_px=py_exact,
        hover_position_id="hp-test",
        field_zone="midfield",
    )
    # 50th point sits 10 yards away -- well beyond the 3-yard match radius.
    px_far, py_far = _yards_to_px(35.0, 12.5)
    rows.append(
        _gt_row(
            gt_id="c1f0p50",
            clip_number=1,
            frame_index=0,
            foot_x_px=px_far,
            foot_y_px=py_far,
            hover_position_id="hp-test",
            field_zone="midfield",
        )
    )
    gt = pl.DataFrame(rows, schema=GT_SCHEMA)

    result = measure_position_error(gt, tracks, cfg)

    assert result.n_points == 50
    assert result.n_unmatched == 1
    assert result.match_rate == pytest.approx(49 / 50)
    # The unmatched 10-yard point must never enter the distance distribution.
    assert result.max_yards == pytest.approx(0.0, abs=1e-6)
    assert result.median_yards == pytest.approx(0.0, abs=1e-6)


def test_measure_position_error_per_zone_breaks_out_larger_offset_zone(tmp_path: Path) -> None:
    cfg = _config_with_synthetic_calibration(tmp_path)

    tracks = synthetic_tracks(n_clips=1, n_frames=2, n_tracks=30)
    tracks = tracks.with_columns(
        pl.when(pl.col("frame_index") == 0).then(pl.lit(5.0)).otherwise(pl.lit(45.0)).alias(
            "x_yards"
        ),
        pl.lit(12.5).alias("y_yards"),
    )

    small_offset = 0.5
    large_offset = 2.5
    px_small, py_small = _yards_to_px(5.0 + small_offset, 12.5)
    px_large, py_large = _yards_to_px(45.0 + large_offset, 12.5)

    rows = _bulk_gt_rows(
        25,
        prefix="west",
        clip_number=1,
        frame_index=0,
        foot_x_px=px_small,
        foot_y_px=py_small,
        hover_position_id="hp-test",
        field_zone="west-half",
    ) + _bulk_gt_rows(
        25,
        prefix="east",
        clip_number=1,
        frame_index=1,
        foot_x_px=px_large,
        foot_y_px=py_large,
        hover_position_id="hp-test",
        field_zone="east-half",
    )
    gt = pl.DataFrame(rows, schema=GT_SCHEMA)

    result = measure_position_error(gt, tracks, cfg)

    assert set(result.per_zone) == {"west-half", "east-half"}
    assert result.per_zone["west-half"]["median_yards"] == pytest.approx(small_offset)
    assert result.per_zone["east-half"]["median_yards"] == pytest.approx(large_offset)
    assert (
        result.per_zone["east-half"]["median_yards"]
        > result.per_zone["west-half"]["median_yards"]
    )


def test_measure_position_error_scale_pair_signed_error(tmp_path: Path) -> None:
    cfg = _config_with_synthetic_calibration(tmp_path)

    tracks = synthetic_tracks(n_clips=1, n_frames=1, n_tracks=48)
    tracks = tracks.with_columns(pl.lit(25.0).alias("x_yards"), pl.lit(12.5).alias("y_yards"))

    px_exact, py_exact = _yards_to_px(25.0, 12.5)
    rows = _bulk_gt_rows(
        48,
        prefix="c1f0",
        clip_number=1,
        frame_index=0,
        foot_x_px=px_exact,
        foot_y_px=py_exact,
        hover_position_id="hp-test",
        field_zone="midfield",
    )

    # A known-distance pair: true separation 10.0 yards, picked points measure 10.3
    # yards apart -- the check must surface a +0.3 yd signed error, not hide it.
    px_a, py_a = _yards_to_px(0.0, 0.0)
    px_b, py_b = _yards_to_px(10.3, 0.0)
    rows.append(
        _gt_row(
            gt_id="c1f0-sp-a",
            clip_number=1,
            frame_index=0,
            foot_x_px=px_a,
            foot_y_px=py_a,
            hover_position_id="hp-test",
            field_zone="midfield",
            scale_pair_id="sp-1",
            scale_true_yards=10.0,
        )
    )
    rows.append(
        _gt_row(
            gt_id="c1f0-sp-b",
            clip_number=1,
            frame_index=0,
            foot_x_px=px_b,
            foot_y_px=py_b,
            hover_position_id="hp-test",
            field_zone="midfield",
            scale_pair_id="sp-1",
            scale_true_yards=10.0,
        )
    )
    gt = pl.DataFrame(rows, schema=GT_SCHEMA)

    result = measure_position_error(gt, tracks, cfg)

    assert len(result.scale_pairs) == 1
    pair = result.scale_pairs[0]
    assert pair["scale_pair_id"] == "sp-1"
    assert pair["true_yards"] == pytest.approx(10.0)
    assert pair["measured_yards"] == pytest.approx(10.3, abs=1e-6)
    assert pair["signed_error_yards"] == pytest.approx(0.3, abs=1e-6)


def test_measure_position_error_below_min_points_raises(tmp_path: Path) -> None:
    cfg = _config_with_synthetic_calibration(tmp_path)

    tracks = synthetic_tracks(n_clips=1, n_frames=1, n_tracks=5)
    gt = pl.DataFrame(
        _bulk_gt_rows(
            10,
            prefix="c1f0",
            clip_number=1,
            frame_index=0,
            foot_x_px=100.0,
            foot_y_px=100.0,
            hover_position_id="hp-test",
            field_zone="midfield",
        ),
        schema=GT_SCHEMA,
    )

    with pytest.raises(InsufficientGroundTruth):
        measure_position_error(gt, tracks, cfg)
