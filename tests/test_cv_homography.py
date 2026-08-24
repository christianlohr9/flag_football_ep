"""Math-only unit tests for `flag_football_ep.cv.homography` -- no real video, no
model weights. Task 2's clip fixture is a tiny synthetically generated `.mp4`, never
real footage.
"""

from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np
import polars as pl
import pytest

from flag_football_ep.config import load_config
from flag_football_ep.reference import MissingReferenceFile
from flag_football_ep.cv.homography import (
    CALIBRATION_COLUMNS,
    FIELD_LANDMARKS,
    MIN_FIT_POINTS,
    CalibrationError,
    ViewTransformer,
    _append_calibration_rows,
    field_landmarks,
    load_calibration,
    pick_points,
    reprojection_error_yards,
    transformer_for,
)

CONFIG_PATH = Path("ffep.toml")


def _config():
    return load_config(CONFIG_PATH)


def _apply_projective_transform(matrix: np.ndarray, points: np.ndarray) -> np.ndarray:
    """Apply a known 3x3 projective matrix to Nx2 points -- an independent reference
    implementation, not `cv2`, used to build ground-truth synthetic correspondences.
    """
    pts = np.asarray(points, dtype=np.float64)
    homogeneous = np.hstack([pts, np.ones((pts.shape[0], 1))])
    transformed = homogeneous @ matrix.T
    transformed = transformed / transformed[:, [2]]
    return transformed[:, :2]


# A known, independently-defined pixel -> yards projective transform (not derived
# from cv2.findHomography) used to build synthetic ground-truth correspondences.
_KNOWN_H = np.array(
    [
        [0.05, 0.0, 0.0],
        [0.0, 0.05, 0.0],
        [0.0001, 0.0002, 1.0],
    ]
)

_SOURCE_PX = np.array(
    [
        [100.0, 100.0],
        [900.0, 100.0],
        [900.0, 700.0],
        [100.0, 700.0],
    ]
)


def _write_calibration_csv(path: Path, rows: list[dict[str, object]]) -> None:
    df = pl.DataFrame(rows) if rows else pl.DataFrame(schema=list(CALIBRATION_COLUMNS))
    df.write_csv(path)


def _make_synthetic_clip(path: Path, *, width: int = 200, height: int = 150, n_frames: int = 20, fps: float = 10.0) -> Path:
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(path), fourcc, fps, (width, height))
    for i in range(n_frames):
        frame = np.full((height, width, 3), (i * 5) % 256, dtype=np.uint8)
        writer.write(frame)
    writer.release()
    return path


# --- ViewTransformer ---------------------------------------------------------


def test_view_transformer_round_trips_known_homography() -> None:
    target = _apply_projective_transform(_KNOWN_H, _SOURCE_PX)

    vt = ViewTransformer(_SOURCE_PX, target)
    transformed = vt.transform_points(_SOURCE_PX.astype(np.float64))

    assert np.allclose(transformed, target, atol=1e-3)


def test_transform_points_empty_array_returns_unchanged() -> None:
    target = _apply_projective_transform(_KNOWN_H, _SOURCE_PX)
    vt = ViewTransformer(_SOURCE_PX, target)

    empty = np.empty((0, 2), dtype=np.float64)
    result = vt.transform_points(empty)

    assert result.size == 0


def test_view_transformer_raises_calibration_error_when_findhomography_fails() -> None:
    collinear_source = np.array([[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [3.0, 0.0]])
    collinear_target = np.array([[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [3.0, 0.0]])

    with pytest.raises(CalibrationError):
        ViewTransformer(collinear_source, collinear_target)


# --- field_landmarks ----------------------------------------------------------


def test_field_landmarks_matches_config_field_dimensions() -> None:
    cfg = _config()
    landmarks = field_landmarks(cfg)

    assert landmarks["goalline_east_north"] == (50.0, 25.0)
    assert landmarks["goalline_west_south"] == (0.0, 0.0)
    assert landmarks["endzone_west_back_south"] == (-10.0, 0.0)
    assert landmarks["endzone_east_back_north"] == (60.0, 25.0)
    assert landmarks["midfield_south"] == (25.0, 0.0)
    assert set(landmarks) == set(FIELD_LANDMARKS)


# --- load_calibration ----------------------------------------------------------


def test_load_calibration_missing_file_raises() -> None:
    missing = Path("/tmp/does-not-exist-ffep-homography-calibration.csv")
    with pytest.raises(MissingReferenceFile):
        load_calibration(missing)


def test_load_calibration_header_only_checked_in_file_loads_empty() -> None:
    cfg = _config()
    with pytest.warns(UserWarning):
        df = load_calibration(cfg.reference.homography_calibration)

    assert df.height == 0
    assert tuple(df.columns) == CALIBRATION_COLUMNS


def _valid_fit_rows(hover_position_id: str, landmarks: dict[str, tuple[float, float]]) -> list[dict[str, object]]:
    names = ["goalline_west_south", "goalline_east_south", "goalline_east_north", "goalline_west_north"]
    source = [(100.0, 700.0), (900.0, 700.0), (900.0, 100.0), (100.0, 100.0)]
    rows = []
    for name, (sx, sy) in zip(names, source):
        tx, ty = landmarks[name]
        rows.append(
            {
                "hover_position_id": hover_position_id,
                "landmark": name,
                "source_x_px": sx,
                "source_y_px": sy,
                "target_x_yards": tx,
                "target_y_yards": ty,
                "use_for_fit": True,
                "notes": "",
            }
        )
    return rows


def test_load_calibration_fewer_than_min_fit_points_raises(tmp_path: Path) -> None:
    cfg = _config()
    landmarks = field_landmarks(cfg)
    rows = _valid_fit_rows("hp-01", landmarks)[: MIN_FIT_POINTS - 1]

    path = tmp_path / "calibration.csv"
    _write_calibration_csv(path, rows)

    with pytest.raises(CalibrationError) as exc_info:
        load_calibration(path)
    assert "hp-01" in str(exc_info.value)


def test_load_calibration_collinear_fit_points_raises(tmp_path: Path) -> None:
    cfg = _config()
    landmarks = field_landmarks(cfg)
    rows = _valid_fit_rows("hp-02", landmarks)
    # Force every source pixel onto the same horizontal line -- degenerate fit set.
    for i, row in enumerate(rows):
        row["source_x_px"] = 100.0 + i * 50.0
        row["source_y_px"] = 400.0

    path = tmp_path / "calibration.csv"
    _write_calibration_csv(path, rows)

    with pytest.raises(CalibrationError) as exc_info:
        load_calibration(path)
    assert "hp-02" in str(exc_info.value)


def test_load_calibration_unknown_landmark_raises(tmp_path: Path) -> None:
    cfg = _config()
    landmarks = field_landmarks(cfg)
    rows = _valid_fit_rows("hp-03", landmarks)
    rows[0]["landmark"] = "not_a_real_landmark"

    path = tmp_path / "calibration.csv"
    _write_calibration_csv(path, rows)

    with pytest.raises(CalibrationError) as exc_info:
        load_calibration(path)
    assert "not_a_real_landmark" in str(exc_info.value)


def test_load_calibration_target_disagreeing_with_field_landmarks_raises(tmp_path: Path) -> None:
    cfg = _config()
    landmarks = field_landmarks(cfg)
    rows = _valid_fit_rows("hp-04", landmarks)
    rows[0]["target_x_yards"] = rows[0]["target_x_yards"] + 5.0  # well past the 0.01yd tolerance

    path = tmp_path / "calibration.csv"
    _write_calibration_csv(path, rows)

    with pytest.raises(CalibrationError) as exc_info:
        load_calibration(path)
    assert "hp-04" in str(exc_info.value)


def test_load_calibration_valid_rows_load_cleanly(tmp_path: Path) -> None:
    cfg = _config()
    landmarks = field_landmarks(cfg)
    rows = _valid_fit_rows("hp-05", landmarks)

    path = tmp_path / "calibration.csv"
    _write_calibration_csv(path, rows)

    df = load_calibration(path)
    assert df.height == MIN_FIT_POINTS


# --- transformer_for / reprojection_error_yards --------------------------------


def test_transformer_for_builds_transformer_from_calibration_frame(tmp_path: Path) -> None:
    cfg = _config()
    landmarks = field_landmarks(cfg)
    rows = _valid_fit_rows("hp-06", landmarks)
    path = tmp_path / "calibration.csv"
    _write_calibration_csv(path, rows)

    calibration = load_calibration(path)
    vt = transformer_for("hp-06", calibration)

    source = np.array([[r["source_x_px"], r["source_y_px"]] for r in rows])
    target = np.array([[r["target_x_yards"], r["target_y_yards"]] for r in rows])
    transformed = vt.transform_points(source)
    assert np.allclose(transformed, target, atol=1e-3)


def test_transformer_for_unknown_hover_position_raises(tmp_path: Path) -> None:
    cfg = _config()
    landmarks = field_landmarks(cfg)
    rows = _valid_fit_rows("hp-07", landmarks)
    path = tmp_path / "calibration.csv"
    _write_calibration_csv(path, rows)

    calibration = load_calibration(path)
    with pytest.raises(CalibrationError) as exc_info:
        transformer_for("hp-does-not-exist", calibration)
    assert "hp-does-not-exist" in str(exc_info.value)


def test_reprojection_error_yards_zero_for_exact_case() -> None:
    target = _apply_projective_transform(_KNOWN_H, _SOURCE_PX)
    vt = ViewTransformer(_SOURCE_PX, target)

    errors = reprojection_error_yards(vt, _SOURCE_PX, target)
    assert np.allclose(errors, 0.0, atol=1e-3)


def test_reprojection_error_yards_nonzero_for_known_perturbation() -> None:
    target = _apply_projective_transform(_KNOWN_H, _SOURCE_PX)
    vt = ViewTransformer(_SOURCE_PX, target)

    perturbed_target = target.copy()
    offset = np.array([0.3, 0.4])  # 3-4-5 triangle -> exact distance 0.5
    perturbed_target[0] = perturbed_target[0] + offset

    errors = reprojection_error_yards(vt, _SOURCE_PX, perturbed_target)
    assert np.isclose(errors[0], 0.5, atol=1e-3)
    assert np.allclose(errors[1:], 0.0, atol=1e-3)


# --- pick_points / _append_calibration_rows -------------------------------------


def test_pick_points_reference_frame_only_mode_writes_jpeg_and_leaves_csv_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("FFEP_CV_CALIBRATE_INTERACTIVE", raising=False)
    cfg = _config()

    clip = _make_synthetic_clip(tmp_path / "clip.mp4")
    out_csv = tmp_path / "calibration.csv"
    _write_calibration_csv(out_csv, [])
    before = out_csv.read_bytes()

    written = pick_points(clip, "hp-08", out_csv, at_second=0.5)

    assert written == out_csv
    assert out_csv.read_bytes() == before

    ref_path = cfg.paths.labels / "calibration" / "hp-08_ref.jpg"
    assert ref_path.exists()
    ref_path.unlink()


def test_pick_points_ref_jpeg_is_gitignored() -> None:
    cfg = _config()
    ref_dir = cfg.paths.labels / "calibration"
    ref_dir.mkdir(parents=True, exist_ok=True)
    probe = ref_dir / "hp-gitignore-probe_ref.jpg"
    probe.write_bytes(b"\xff\xd8\xff\xd9")  # minimal jpeg-ish bytes
    try:
        import subprocess

        result = subprocess.run(
            ["git", "check-ignore", "-q", str(probe)],
            cwd=Path.cwd(),
        )
        assert result.returncode == 0
    finally:
        probe.unlink()


def test_append_calibration_rows_replaces_not_duplicates(tmp_path: Path) -> None:
    cfg = _config()
    landmarks = field_landmarks(cfg)
    initial_rows = _valid_fit_rows("hp-09", landmarks)
    path = tmp_path / "calibration.csv"
    _write_calibration_csv(path, initial_rows)

    replacement_rows = _valid_fit_rows("hp-09", landmarks)
    for row in replacement_rows:
        row["notes"] = "re-picked"

    _append_calibration_rows(path, "hp-09", replacement_rows)

    df = load_calibration(path)
    hp09 = df.filter(pl.col("hover_position_id") == "hp-09")
    assert hp09.height == len(replacement_rows)
    assert set(hp09["notes"].to_list()) == {"re-picked"}


def test_append_calibration_rows_invalid_set_leaves_disk_content_byte_identical(
    tmp_path: Path,
) -> None:
    cfg = _config()
    landmarks = field_landmarks(cfg)
    existing_rows = _valid_fit_rows("hp-10-existing", landmarks)
    path = tmp_path / "calibration.csv"
    _write_calibration_csv(path, existing_rows)
    before = path.read_bytes()

    invalid_rows = _valid_fit_rows("hp-10-invalid", landmarks)[: MIN_FIT_POINTS - 1]

    with pytest.raises(CalibrationError):
        _append_calibration_rows(path, "hp-10-invalid", invalid_rows)

    assert path.read_bytes() == before
    assert not path.with_suffix(path.suffix + ".tmp").exists()
