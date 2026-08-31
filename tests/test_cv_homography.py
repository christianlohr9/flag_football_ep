"""Math-only unit tests for `flag_football_ep.cv.homography` -- no real video, no
model weights. Task 2's clip fixture is a tiny synthetically generated `.mp4`, never
real footage.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

cv2 = pytest.importorskip("cv2", reason="requires the cv extras group (uv sync --extra cv)")

from flag_football_ep.config import load_config
from flag_football_ep.reference import MissingReferenceFile
from flag_football_ep.cv.homography import (
    CALIBRATION_COLUMNS,
    CLIP_ALIGNMENT_REFERENCE_FRAMES,
    FIELD_LANDMARKS,
    MIN_FIT_POINTS,
    CalibrationError,
    ViewTransformer,
    _append_calibration_rows,
    clip_alignment,
    clip_alignment_matrix,
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


def _make_textured_image(width: int = 640, height: int = 480, seed: int = 0) -> np.ndarray:
    """A synthetic image rich in ORB-detectable corner features: a mid-gray background
    with many random-colored filled rectangles at random positions/sizes -- unlike flat
    noise, sharp rectangle corners survive ORB detection under a small perspective warp,
    the same way the real pitch's painted lines/lettering do (this module's "Per-clip
    homography refinement" docstring section).
    """
    rng = np.random.default_rng(seed)
    image = np.full((height, width, 3), 128, dtype=np.uint8)
    for _ in range(150):
        x1 = int(rng.integers(0, width - 20))
        y1 = int(rng.integers(0, height - 20))
        w = int(rng.integers(8, 40))
        h = int(rng.integers(8, 40))
        color = tuple(int(c) for c in rng.integers(0, 255, size=3))
        cv2.rectangle(image, (x1, y1), (min(width, x1 + w), min(height, y1 + h)), color, -1)
    return image


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


def test_load_calibration_checked_in_file_carries_both_pilot_hover_positions() -> None:
    """Since plan 02.1-13's user calibration the checked-in CSV is no longer the
    header-only anchor: it carries the real pilot points -- >=4 use_for_fit rows for
    each of the session's two hover positions.
    """
    cfg = _config()
    df = load_calibration(cfg.reference.homography_calibration)

    assert tuple(df.columns) == CALIBRATION_COLUMNS
    for hp in ("hp-01", "hp-02"):
        fit = df.filter((pl.col("hover_position_id") == hp) & pl.col("use_for_fit"))
        assert fit.height >= 4, f"{hp} has {fit.height} fit points, need >= 4"


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


# --- clip_alignment / clip_alignment_matrix ---------------------------------------


def test_clip_alignment_recovers_known_small_homography_from_synthetic_texture() -> None:
    """Warp a textured image by a known small homography (a real drone's between-clip
    drift is small, not a wild transform) and confirm `clip_alignment` recovers a
    matrix that undoes it within a few pixels -- the direct synthetic-recovery test the
    plan's <action> block calls for.
    """
    reference_frame = _make_textured_image(seed=1)
    height, width = reference_frame.shape[:2]

    # A small, known "reference pixel -> clip pixel" warp: ~12px translation plus a
    # ~3-degree rotation about the image center -- representative of between-clip
    # drone drift/rotation, not an extreme transform.
    center = (width / 2.0, height / 2.0)
    rotation = cv2.getRotationMatrix2D(center, 3.0, 1.0)
    known_w = np.vstack([rotation, [0.0, 0.0, 1.0]])
    known_w[0, 2] += 12.0
    known_w[1, 2] += 8.0

    clip_frame = cv2.warpPerspective(reference_frame, known_w, (width, height))

    recovered = clip_alignment(clip_frame, reference_frame)

    # recovered maps clip_frame pixels -> reference_frame pixels, i.e. it should undo
    # known_w: recovered @ known_w =~ identity. Check this on a handful of interior
    # sample points (corners near the frame edge are the most likely to fall outside
    # both images after warping and are not a fair check of the recovered matrix).
    sample_points = np.array(
        [[160.0, 120.0], [480.0, 120.0], [480.0, 360.0], [160.0, 360.0], [320.0, 240.0]]
    )
    warped_then_recovered = _apply_projective_transform(
        recovered, _apply_projective_transform(known_w, sample_points)
    )

    assert np.allclose(warped_then_recovered, sample_points, atol=5.0)


def test_clip_alignment_falls_back_to_identity_with_notice_for_blank_frames() -> None:
    """Two flat, featureless frames yield zero ORB keypoints -- `clip_alignment` must
    fall back to identity (never a garbage transform) and surface a `UserWarning`.
    """
    blank_clip = np.full((200, 300, 3), 100, dtype=np.uint8)
    blank_reference = np.full((200, 300, 3), 100, dtype=np.uint8)

    with pytest.warns(UserWarning, match="falling back to identity"):
        result = clip_alignment(blank_clip, blank_reference)

    assert np.array_equal(result, np.eye(3))


def test_clip_alignment_ecc_fallback_recovers_known_small_euclidean_warp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When SIFT/RANSAC's own acceptance gate can never be cleared -- monkeypatched here
    via `_MIN_ALIGNMENT_INLIERS` set unreachably high, simulating the real failure mode
    28/59 non-reference clips hit before this ECC follow-up (too few RANSAC-supported
    correspondences) -- `clip_alignment` still recovers a known small Euclidean warp
    (~12px translation, ~3deg rotation, the same between-clip-drift-representative
    transform `test_clip_alignment_recovers_known_small_homography_from_synthetic_texture`
    uses) via the `_ecc_align` second stage, on the SAME rich, SIFT-friendly synthetic
    texture -- proving the ECC path itself works correctly, independent of whether SIFT
    happens to succeed on any given real pair.
    """
    from flag_football_ep.cv import homography as homography_module

    monkeypatch.setattr(homography_module, "_MIN_ALIGNMENT_INLIERS", 10_000)

    reference_frame = _make_textured_image(seed=1)
    height, width = reference_frame.shape[:2]

    center = (width / 2.0, height / 2.0)
    rotation = cv2.getRotationMatrix2D(center, 3.0, 1.0)
    known_w = np.vstack([rotation, [0.0, 0.0, 1.0]])
    known_w[0, 2] += 12.0
    known_w[1, 2] += 8.0

    clip_frame = cv2.warpPerspective(reference_frame, known_w, (width, height))

    with pytest.warns(UserWarning, match="ECC second-stage fallback succeeded"):
        recovered = clip_alignment(clip_frame, reference_frame)

    assert not np.array_equal(recovered, np.eye(3))

    sample_points = np.array(
        [[160.0, 120.0], [480.0, 120.0], [480.0, 360.0], [160.0, 360.0], [320.0, 240.0]]
    )
    warped_then_recovered = _apply_projective_transform(
        recovered, _apply_projective_transform(known_w, sample_points)
    )

    assert np.allclose(warped_then_recovered, sample_points, atol=5.0)


def test_clip_alignment_ecc_fallback_rejected_below_correlation_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ECC candidate that converges but scores below `_ECC_MIN_CORRELATION` is
    rejected -- `clip_alignment` falls all the way back to identity (never trusts a
    low-confidence area-based fit), with a `UserWarning` naming both the SIFT failure
    and the ECC rejection. `_ecc_align` itself is monkeypatched to return a fixed,
    otherwise-plausible matrix at a correlation just below threshold, isolating the
    accept/reject GATE logic in `clip_alignment` from `_ecc_align`'s own (separately
    tested) convergence behavior.
    """
    from flag_football_ep.cv import homography as homography_module

    monkeypatch.setattr(homography_module, "_MIN_ALIGNMENT_INLIERS", 10_000)

    low_confidence_matrix = np.array(
        [[1.0, 0.0, 5.0], [0.0, 1.0, -5.0], [0.0, 0.0, 1.0]]
    )
    below_threshold_cc = homography_module._ECC_MIN_CORRELATION - 0.05
    monkeypatch.setattr(
        homography_module,
        "_ecc_align",
        lambda clip_frame, reference_frame: (low_confidence_matrix, below_threshold_cc),
    )

    reference_frame = _make_textured_image(seed=5)
    clip_frame = _make_textured_image(seed=6)

    with pytest.warns(UserWarning, match="falling back to identity"):
        result = clip_alignment(clip_frame, reference_frame)

    assert np.array_equal(result, np.eye(3))


def test_clip_alignment_matrix_identity_for_hover_position_without_reference_frame() -> None:
    """A hover position absent from `CLIP_ALIGNMENT_REFERENCE_FRAMES` (every synthetic
    test id, and any real hover position this mapping hasn't been extended to) gets
    identity with no video access attempted at all.
    """
    cfg = _config()
    assert "hp-does-not-have-a-reference-frame" not in CLIP_ALIGNMENT_REFERENCE_FRAMES

    result = clip_alignment_matrix("hp-does-not-have-a-reference-frame", 999, cfg)

    assert np.array_equal(result, np.eye(3))


def test_clip_alignment_matrix_identity_for_the_reference_clip_itself() -> None:
    """Aligning a hover position's reference clip against itself is a no-op -- short-
    circuited before any video access is attempted.
    """
    cfg = _config()
    reference_clip_number, _ = CLIP_ALIGNMENT_REFERENCE_FRAMES["hp-01"]

    result = clip_alignment_matrix("hp-01", reference_clip_number, cfg)

    assert np.array_equal(result, np.eye(3))


def test_clip_alignment_matrix_unresolvable_clip_falls_back_to_identity_with_notice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A registered hover position whose clip video cannot be resolved (e.g. no
    `video_inventory.csv` row) degrades to identity with a notice, never raises.
    """
    from flag_football_ep.cv import frames as frames_module

    def _raise_clip_not_found(*args, **kwargs):
        raise frames_module.ClipNotFound("synthetic: no clips registered")

    monkeypatch.setattr(frames_module, "clip_paths", _raise_clip_not_found)

    cfg = _config()
    with pytest.warns(UserWarning, match="falling back to identity"):
        result = clip_alignment_matrix("hp-01", 12345, cfg)

    assert np.array_equal(result, np.eye(3))


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
