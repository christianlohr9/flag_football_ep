"""Manual per-hover-position homography calibration and point projection.

Owns the D-05 manual 4-8-point calibration workflow: `pick_points` extracts a still
frame from a clip at `at_second` so an operator can hand-pick source/target point
correspondences into a CSV (`load_calibration` reads it back, following
`reference._read_reference_csv`'s typed-schema-loader pattern -- a small, hand-maintained
anchor CSV, one row per (hover_position, point), the same convention
`docs/sync-convention.md` establishes for `half_boundaries.csv`); `transformer_for`
builds a `ViewTransformer` for a given hover position from the loaded calibration;
`reprojection_error_yards` measures how well a fitted homography reproduces its own
input points, in yards (target coordinates are yards directly, D-13 -- no unit
conversion downstream).

`ViewTransformer` (`cv2.findHomography`/`perspectiveTransform`/`warpPerspective`) is
the `github.com/roboflow/sports` `sports/common/view.py` shape (MIT), ported verbatim.
Field-keypoint models for moving cameras are explicitly out of scope this phase (D-05)
-- calibration here is static, per fixed hover position only.

`field_landmarks(config)` computes the canonical target coordinates (in yards) for the
fixed `FIELD_LANDMARKS` name vocabulary from `config.cv.field_length_yards` (50.0),
`field_width_yards` (25.0) and `endzone_yards` (10.0) -- the field coordinate
convention (D-13) is x=0.0 at the west goal line, x=field_length_yards at the east
goal line (negative/>field_length_yards is inside an end zone), y=0.0 at the south
sideline and y=field_width_yards at the north sideline.

`load_calibration`'s fixed signature (`path` only, no `config` parameter -- see
`tests/test_cv_contracts.py`'s signature guard) resolves the project's default
`ffep.toml` internally via `flag_football_ep.config.load_config()` to get the field
dimensions needed for landmark-vocabulary and target-agreement validation. Callers
that already hold a `Config` still get to validate custom field dimensions by loading
the CSV against a non-default `ffep.toml` copy if ever needed; in practice this pilot
always validates against the one checked-in config.

`pick_points`'s out-of-frame pixel-bounds check is intentionally the light-weight
non-negative-coordinate guard rather than a full cross-reference against
`data/reference/hover_positions.csv`'s per-clip resolution: that reference file is
plan 02.1-03's output and this plan only depends on 02.1-02's contracts, so requiring
it here would make every calibration load fail before 02.1-03 lands. The full
resolution-bounds cross-check is left to whichever later plan wires
`hover_positions.csv` + `video_inventory.csv` together for this purpose.

`pick_points`'s interactive picking (mouse clicks on an `cv2.imshow` window) only runs
when the operator opts in via `FFEP_CV_CALIBRATE_INTERACTIVE=1` in the environment --
empirically, `cv2.imshow`/`cv2.waitKey` do not reliably raise `cv2.error` in every
headless/CI environment (verified during this plan's implementation: they can succeed
silently with a window that will never receive real clicks), so autodetecting "is a
window available" from exception behavior alone is not a safe way to guarantee the
T-2.1-12 "never blocks" mitigation. Defaulting to the safe reference-frame-export path
and gating the interactive attempt behind an explicit opt-in keeps every automated
invocation (tests, CI, a first-time `ffep cv calibrate` run) non-blocking by
construction; when opted in, the pick loop is additionally bounded by a hard
iteration/time cap so it still cannot hang even in a genuinely broken environment.
"""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import polars as pl

from flag_football_ep.cv import CvError
from flag_football_ep.reference import MissingReferenceFile

if TYPE_CHECKING:
    from flag_football_ep.config import Config


class CalibrationError(CvError, ValueError):
    """Raised when a calibration point set is degenerate/collinear, has fewer than
    four points, or cannot be resolved for a requested hover position.
    """


# Minimum number of `use_for_fit = true` points required to fit a homography.
MIN_FIT_POINTS = 4

# The field-coordinate agreement tolerance (yards) a calibration CSV's
# `target_*_yards` values must stay within of `field_landmarks(config)` for the
# same landmark name -- the CSV must not silently redefine the field (D-13).
_TARGET_AGREEMENT_TOLERANCE_YARDS = 0.01

# Fixed landmark-name vocabulary the calibration CSV's `landmark` column must use.
# Coordinates for these names are computed by `field_landmarks()` from the
# project's configured field dimensions.
FIELD_LANDMARKS: tuple[str, ...] = (
    "goalline_west_south",
    "goalline_west_north",
    "goalline_east_south",
    "goalline_east_north",
    "endzone_west_back_south",
    "endzone_west_back_north",
    "endzone_east_back_south",
    "endzone_east_back_north",
    "yardline_10_south",
    "yardline_10_north",
    "yardline_20_south",
    "yardline_20_north",
    "yardline_30_south",
    "yardline_30_north",
    "yardline_40_south",
    "yardline_40_north",
    "midfield_south",
    "midfield_north",
)

_CALIBRATION_SCHEMA: dict[str, pl.DataType] = {
    "hover_position_id": pl.Utf8,
    "landmark": pl.Utf8,
    "source_x_px": pl.Float64,
    "source_y_px": pl.Float64,
    "target_x_yards": pl.Float64,
    "target_y_yards": pl.Float64,
    "use_for_fit": pl.Boolean,
    "notes": pl.Utf8,
}

CALIBRATION_COLUMNS: tuple[str, ...] = tuple(_CALIBRATION_SCHEMA)


def field_landmarks(config: Config) -> dict[str, tuple[float, float]]:
    """Compute the canonical (x_yards, y_yards) target coordinate for every name in
    `FIELD_LANDMARKS`, from `config.cv.field_length_yards`/`field_width_yards`/
    `endzone_yards` (D-13 axis convention: x=0.0 west goal line, x=field_length_yards
    east goal line, negative/>field_length_yards inside an end zone; y=0.0 south
    sideline, y=field_width_yards north sideline).
    """
    length = config.cv.field_length_yards
    width = config.cv.field_width_yards
    endzone = config.cv.endzone_yards
    midfield_x = length / 2.0

    landmarks: dict[str, tuple[float, float]] = {
        "goalline_west_south": (0.0, 0.0),
        "goalline_west_north": (0.0, width),
        "goalline_east_south": (length, 0.0),
        "goalline_east_north": (length, width),
        "endzone_west_back_south": (-endzone, 0.0),
        "endzone_west_back_north": (-endzone, width),
        "endzone_east_back_south": (length + endzone, 0.0),
        "endzone_east_back_north": (length + endzone, width),
        "yardline_10_south": (10.0, 0.0),
        "yardline_10_north": (10.0, width),
        "yardline_20_south": (20.0, 0.0),
        "yardline_20_north": (20.0, width),
        "yardline_30_south": (30.0, 0.0),
        "yardline_30_north": (30.0, width),
        "yardline_40_south": (40.0, 0.0),
        "yardline_40_north": (40.0, width),
        "midfield_south": (midfield_x, 0.0),
        "midfield_north": (midfield_x, width),
    }

    assert set(landmarks) == set(FIELD_LANDMARKS), (
        "field_landmarks() name set drifted from the FIELD_LANDMARKS vocabulary"
    )
    return landmarks


class ViewTransformer:
    """Homography-based point/image projection between a clip's pixel space and the
    field's yard space, given a fitted source/target point correspondence.

    Source: `github.com/roboflow/sports` `sports/common/view.py` (MIT), ported
    verbatim per RESEARCH.md Pattern 3 -- never a hand-rolled DLT solver.
    """

    def __init__(self, source: np.ndarray, target: np.ndarray) -> None:
        import cv2

        source_arr = np.asarray(source, dtype=np.float32)
        target_arr = np.asarray(target, dtype=np.float32)

        matrix, _ = cv2.findHomography(source_arr, target_arr)
        if matrix is None:
            raise CalibrationError(
                "cv2.findHomography returned no solution for the given source/target "
                "point correspondences -- check for degenerate (collinear or "
                "duplicate) points"
            )
        self.m = matrix

    def transform_points(self, points: np.ndarray) -> np.ndarray:
        import cv2

        points_arr = np.asarray(points)
        if points_arr.size == 0:
            return points_arr

        reshaped = points_arr.reshape(-1, 1, 2).astype(np.float32)
        transformed = cv2.perspectiveTransform(reshaped, self.m)
        return transformed.reshape(-1, 2)

    def transform_image(self, image: np.ndarray, resolution_wh: tuple[int, int]) -> np.ndarray:
        import cv2

        return cv2.warpPerspective(image, self.m, resolution_wh)


def _read_calibration_csv(path: Path) -> pl.DataFrame:
    """`reference._read_reference_csv`'s typed-schema-loader pattern, applied to the
    calibration CSV's own schema (kept local to this module rather than importing the
    private helper cross-module).
    """
    if not path.exists():
        raise MissingReferenceFile(f"reference file not found: {path}")

    df = pl.read_csv(path, schema_overrides=_CALIBRATION_SCHEMA)

    if df.height == 0:
        warnings.warn(
            f"{path} is header-only; loading as an empty typed frame",
            stacklevel=3,
        )

    return df


def _is_degenerate(points: np.ndarray) -> bool:
    """True when `points` (an Nx2 array) do not span a plane -- all collinear, or all
    coincident -- the case where `cv2.findHomography` would otherwise silently return
    a garbage matrix instead of `None`.
    """
    centered = points - points.mean(axis=0)
    return np.linalg.matrix_rank(centered) < 2


def load_calibration(path: Path) -> pl.DataFrame:
    """Load the hand-maintained homography calibration CSV at `path`, rejecting
    degenerate/collinear point sets and naming the offending hover_position_id.
    """
    df = _read_calibration_csv(path)
    if df.height == 0:
        return df

    from flag_football_ep.config import load_config

    cfg = load_config()
    landmarks = field_landmarks(cfg)

    for hover_position_id in df["hover_position_id"].unique(maintain_order=True).to_list():
        group = df.filter(pl.col("hover_position_id") == hover_position_id)

        unknown = sorted(set(group["landmark"].to_list()) - set(landmarks))
        if unknown:
            raise CalibrationError(
                f"hover position {hover_position_id!r}: unknown landmark(s) {unknown} "
                "not in the field_landmarks vocabulary"
            )

        for row in group.iter_rows(named=True):
            landmark = row["landmark"]
            expected_x, expected_y = landmarks[landmark]
            if (
                abs(row["target_x_yards"] - expected_x) > _TARGET_AGREEMENT_TOLERANCE_YARDS
                or abs(row["target_y_yards"] - expected_y) > _TARGET_AGREEMENT_TOLERANCE_YARDS
            ):
                raise CalibrationError(
                    f"hover position {hover_position_id!r}: landmark {landmark!r} target "
                    f"({row['target_x_yards']}, {row['target_y_yards']}) disagrees with "
                    f"field_landmarks ({expected_x}, {expected_y}) by more than "
                    f"{_TARGET_AGREEMENT_TOLERANCE_YARDS} yards"
                )

            if row["source_x_px"] < 0 or row["source_y_px"] < 0:
                raise CalibrationError(
                    f"hover position {hover_position_id!r}: landmark {landmark!r} has an "
                    f"out-of-frame source pixel ({row['source_x_px']}, {row['source_y_px']})"
                )

        fit_group = group.filter(pl.col("use_for_fit"))
        if fit_group.height < MIN_FIT_POINTS:
            raise CalibrationError(
                f"hover position {hover_position_id!r}: only {fit_group.height} "
                f"use_for_fit point(s), need at least {MIN_FIT_POINTS}"
            )

        source_px = fit_group.select("source_x_px", "source_y_px").to_numpy()
        if _is_degenerate(source_px):
            raise CalibrationError(
                f"hover position {hover_position_id!r}: use_for_fit points are "
                "collinear/degenerate -- cv2.findHomography would return a garbage "
                "matrix rather than a usable one"
            )

    return df


def transformer_for(hover_position_id: str, calibration: pl.DataFrame) -> ViewTransformer:
    """Build a `ViewTransformer` for `hover_position_id` from the loaded `calibration`
    frame, raising `CalibrationError` if no rows match.
    """
    rows = calibration.filter(
        (pl.col("hover_position_id") == hover_position_id) & pl.col("use_for_fit")
    )
    if rows.height == 0:
        raise CalibrationError(
            f"hover position {hover_position_id!r} has no use_for_fit rows in the "
            "calibration frame"
        )

    source = rows.select("source_x_px", "source_y_px").to_numpy()
    target = rows.select("target_x_yards", "target_y_yards").to_numpy()
    return ViewTransformer(source, target)


def reprojection_error_yards(
    transformer: ViewTransformer, source: np.ndarray, target: np.ndarray
) -> np.ndarray:
    """Measure, in yards, how well `transformer` reproduces `target` from `source` --
    the calibration-quality diagnostic reported alongside the pilot gate's position
    error. Intended use is the held-out (`use_for_fit = false`) landmarks: transform
    their pixel coordinates and compare against their hand-recorded field-yard
    coordinates as an independent check the fit generalizes (D-10), not just the
    points it was fit on. `target` is already in yards (D-13; 1 m = 1.0936 yards), so
    the returned distances need no further unit conversion to read against the C-09
    position-error threshold.
    """
    projected = transformer.transform_points(np.asarray(source, dtype=np.float64))
    target_arr = np.asarray(target, dtype=np.float64)
    return np.linalg.norm(projected - target_arr, axis=1)


def _draw_reference_grid(frame: np.ndarray, landmarks: dict[str, tuple[float, float]]) -> np.ndarray:
    """Annotate `frame` with a 100px pixel grid (axis-labelled) so an operator can
    hand-read source pixel coordinates off the exported JPEG -- the documented
    fallback when interactive picking is unavailable or not opted into.
    """
    import cv2

    annotated = frame.copy()
    height, width = annotated.shape[:2]
    color = (0, 255, 0)

    for x in range(0, width, 100):
        cv2.line(annotated, (x, 0), (x, height), color, 1)
        cv2.putText(annotated, str(x), (x + 2, 15), cv2.FONT_HERSHEY_SIMPLEX, 0.4, color, 1)
    for y in range(0, height, 100):
        cv2.line(annotated, (0, y), (width, y), color, 1)
        cv2.putText(annotated, str(y), (2, y + 15), cv2.FONT_HERSHEY_SIMPLEX, 0.4, color, 1)

    for i, name in enumerate(sorted(landmarks)):
        cv2.putText(
            annotated,
            f"{i + 1}. {name}",
            (5, height - 10 - 14 * i),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.4,
            (0, 200, 255),
            1,
        )

    return annotated


def _pick_points_interactive(
    frame: np.ndarray, names: list[str], *, max_wait_iterations: int = 250, wait_ms: int = 20
) -> list[tuple[str, tuple[float, float]]]:
    """Open `frame` in a window and consume left-clicks in `names` order, ESC to
    finish early. Hard-bounded by `max_wait_iterations` * `wait_ms` (~5s by default)
    so this can never block indefinitely (T-2.1-12) regardless of whether a human is
    present to interact with it. Returns whatever was picked before the window closed
    (possibly nothing).
    """
    import cv2

    picked: list[tuple[str, tuple[float, float]]] = []
    window = "ffep calibration"

    def _on_click(event: int, x: int, y: int, flags: int, param: object) -> None:
        if event == cv2.EVENT_LBUTTONDOWN and len(picked) < len(names):
            picked.append((names[len(picked)], (float(x), float(y))))

    cv2.namedWindow(window)
    cv2.setMouseCallback(window, _on_click)
    try:
        for _ in range(max_wait_iterations):
            cv2.imshow(window, frame)
            key = cv2.waitKey(wait_ms) & 0xFF
            if key == 27 or len(picked) >= len(names):  # ESC
                break
    finally:
        cv2.destroyWindow(window)

    return picked


def _append_calibration_rows(
    out_csv: Path, hover_position_id: str, rows: list[dict[str, object]]
) -> Path:
    """Replace `hover_position_id`'s rows in `out_csv` with `rows` (re-picking
    replaces, never duplicates), sort by (hover_position_id, landmark), and write
    atomically -- validated via `load_calibration` before the write is committed, so
    an invalid set never overwrites a previously-valid on-disk CSV (T-2.1-11).
    """
    if out_csv.exists():
        existing = _read_calibration_csv(out_csv)
        existing = existing.filter(pl.col("hover_position_id") != hover_position_id)
    else:
        existing = pl.DataFrame(schema=_CALIBRATION_SCHEMA)

    new_rows = pl.DataFrame(rows, schema=_CALIBRATION_SCHEMA)
    combined = pl.concat([existing, new_rows], how="vertical").sort(
        ["hover_position_id", "landmark"]
    )

    tmp_path = out_csv.with_suffix(out_csv.suffix + ".tmp")
    combined.write_csv(tmp_path)

    try:
        load_calibration(tmp_path)
    except CalibrationError:
        tmp_path.unlink(missing_ok=True)
        raise

    os.replace(tmp_path, out_csv)
    return out_csv


def pick_points(clip: Path, hover_position_id: str, out_csv: Path, *, at_second: float) -> Path:
    """Extract a still frame from `clip` at `at_second`, always writing an annotated
    reference-frame JPEG (`data/labels/calibration/{hover_position_id}_ref.jpg`, the
    documented hand-edit fallback), and, only when `FFEP_CV_CALIBRATE_INTERACTIVE=1`
    is set, also attempt interactive point picking on that frame. Picked rows (if any)
    replace `hover_position_id`'s existing rows in `out_csv` (never duplicate),
    validated before the write is committed. Returns `out_csv`.
    """
    import cv2

    clip = Path(clip)
    out_csv = Path(out_csv)

    cap = cv2.VideoCapture(str(clip))
    try:
        fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
        if fps > 0:
            cap.set(cv2.CAP_PROP_POS_FRAMES, round(at_second * fps))
        else:
            cap.set(cv2.CAP_PROP_POS_MSEC, at_second * 1000.0)
        ok, frame = cap.read()
    finally:
        cap.release()

    if not ok or frame is None:
        raise CalibrationError(f"could not extract a frame at {at_second}s from {clip}")

    from flag_football_ep.config import load_config

    cfg = load_config()
    landmarks = field_landmarks(cfg)
    names = sorted(landmarks)

    annotated = _draw_reference_grid(frame, landmarks)

    ref_dir = cfg.paths.labels / "calibration"
    ref_dir.mkdir(parents=True, exist_ok=True)
    ref_path = ref_dir / f"{hover_position_id}_ref.jpg"
    cv2.imwrite(str(ref_path), annotated)

    print(f"reference frame: {ref_path}")
    print("landmark checklist (click in this order, or hand-edit the CSV using the "
          "pixel grid on the reference frame):")
    for i, name in enumerate(names):
        print(f"  {i + 1}. {name}")

    picked_rows: list[tuple[str, tuple[float, float]]] = []
    if os.environ.get("FFEP_CV_CALIBRATE_INTERACTIVE") == "1":
        try:
            picked_rows = _pick_points_interactive(frame, names)
        except Exception as exc:  # noqa: BLE001 - any GUI failure falls back safely
            print(
                f"interactive picking unavailable ({exc}); hand-edit points by reading "
                f"pixel coordinates off {ref_path}"
            )
            picked_rows = []

    if picked_rows:
        rows: list[dict[str, object]] = [
            {
                "hover_position_id": hover_position_id,
                "landmark": name,
                "source_x_px": x,
                "source_y_px": y,
                "target_x_yards": landmarks[name][0],
                "target_y_yards": landmarks[name][1],
                "use_for_fit": True,
                "notes": "",
            }
            for name, (x, y) in picked_rows
        ]
        _append_calibration_rows(out_csv, hover_position_id, rows)

    return out_csv
