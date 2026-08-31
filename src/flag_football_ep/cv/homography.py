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

## Per-clip homography refinement (drift correction)

A single manual calibration per hover position (above) is only exactly valid for the
one clip its points were picked on -- `docs/homography-calibration.md`'s "hp-01: Clip
028", "hp-02: Clip 044". Grid-overlay diagnostics confirmed the drone drifts and
rotates slightly between clips sharing the same hover position (a hand-flown/hovered
drone, not a locked-off tripod): the calibrated grid fits its own reference clip
pixel-perfect but sits tens of pixels off on other clips in the same group.

`clip_alignment(clip_frame, reference_frame)` registers one clip's representative
frame onto its hover position's calibration reference frame via SIFT features +
ratio-test matching + `cv2.findHomography(..., cv2.RANSAC)`, returning the 3x3
`H_align` that maps the clip's own pixel space onto the reference clip's pixel space.
The scene is dominated by the planar pitch (painted lines, large lettering) -- strong,
static features RANSAC keeps as inliers; moving players are outliers RANSAC rejects
by construction (a moving object's apparent pixel displacement between two frames of a
*static* camera scene does not follow the same planar homography as the pitch, so it
never accumulates enough consistent matches to out-vote the dominant, static-scene
homography). Guarded: too few SIFT features, too few ratio-test matches, or too low an
inlier count/ratio/implausible fit falls back to a SECOND registration stage rather
than straight to identity (see below).

### ECC second-stage fallback (2026-08-30 follow-up)

28 of 59 non-reference clips (47%) fell back to identity under SIFT/RANSAC alone --
including clip 11, confirmed by manual template matching to have a real, modest
transform (scale ~0.85, near-zero rotation) that SIFT/RANSAC's sparse-feature search
simply never found enough support for (clip 11 and its hp-01 reference clip 28 share
only a narrow overlap dominated by repetitive grass texture). `_ecc_align(clip_frame,
reference_frame)` is the second stage `clip_alignment` attempts whenever the SIFT/RANSAC
sweep above produces no plausible, well-supported candidate: `cv2.findTransformECC`, an
area-based (not sparse-feature) registration that directly maximizes intensity
correlation between the two frames, so it does not need a minimum count of discrete
matched keypoints the way SIFT/RANSAC does. Both frames are downscaled by
`_ECC_DOWNSCALE_FACTOR` first (cheaper, and a slightly softened image is if anything
easier for ECC's gradient-descent optimizer). `MOTION_EUCLIDEAN` (rotation+translation,
no independent scale/shear degrees of freedom) is tried first, initialized from
identity -- physically the right model for a hovering drone's between-clip drift, which
is dominated by position/orientation, not a material zoom change -- then optionally
refined by a second `MOTION_HOMOGRAPHY` pass seeded from the Euclidean result, kept only
if it also converges. The final warp is rescaled back to full resolution and accepted
only when its correlation coefficient clears `_ECC_MIN_CORRELATION` AND it passes the
SAME `_is_plausible_alignment` sanity guard SIFT/RANSAC candidates are held to.
`_ECC_MIN_CORRELATION` was tuned empirically (see `docs/homography-calibration.md`'s ECC
follow-up section and `_ECC_MIN_CORRELATION`'s own definition below for the full
calibration data) -- comparing max corner-pixel disagreement between ECC-only and SIFT
results turned out to be a misleading signal on this footage's extreme-perspective
homographies (tiny parameter differences blow up at the frame corners, far outside any
region real correspondences support); a direct visual check -- alpha-blending the
ECC-warped clip against its reference frame and looking for feature overlap -- is what
the threshold actually reflects. ECC failing to converge, or converging to a low-
correlation/implausible fit, still falls back to identity plus a `UserWarning` notice,
same guarantee as before: never a garbage transform.

`CLIP_ALIGNMENT_REFERENCE_FRAMES` hard-codes, per hover position, which
`(clip_number, at_second)` the calibration in `homography_calibration.csv` was
actually picked on -- the same two facts `docs/homography-calibration.md` already
records in prose. `clip_alignment_matrix(hover_position_id, clip_number, config)` is
the orchestration entry point `coordinates.composed_transformer_for` composes with the
per-hover-position calibrated homography: identity for hover positions with no
registered reference frame (keeps every pre-existing synthetic-hover-position test
byte-identical -- composing with identity is a no-op) and identity for the reference
clip itself (nothing to align against its own reference frame), computed once per
distinct clip encountered by a caller (never once per row) and never raising -- any
resolution/registration failure degrades to identity plus a notice rather than
aborting the whole coordinate projection over one bad clip.
"""

from __future__ import annotations

import math
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


class ClipAlignmentUnresolvable(CvError, ValueError):
    """Raised internally by `clip_alignment_matrix` when the clip or its hover
    position's reference clip cannot be located; always caught within
    `clip_alignment_matrix` itself and turned into an identity fallback plus a
    `UserWarning` notice, never propagated to callers.
    """


# Minimum number of `use_for_fit = true` points required to fit a homography.
MIN_FIT_POINTS = 4

# The field-coordinate agreement tolerance (yards) a calibration CSV's
# `target_*_yards` values must stay within of `field_landmarks(config)` for the
# same landmark name -- the CSV must not silently redefine the field (D-13).
_TARGET_AGREEMENT_TOLERANCE_YARDS = 0.01

ClipAlignmentReference = tuple[int, float]

# Per hover position, the `(clip_number, at_second)` its `homography_calibration.csv`
# rows were actually picked on -- see `docs/homography-calibration.md`'s "hp-01: Clip
# 028"/"hp-02: Clip 044" sections. Every other clip sharing that hover position is
# registered (`clip_alignment`) against a frame extracted fresh from THIS clip, never
# against the annotated `data/labels/calibration/{id}_ref.jpg` (grid lines drawn on it
# would corrupt feature matching). A hover position absent from this mapping (e.g. a
# synthetic test id) gets identity alignment -- see `clip_alignment_matrix`.
CLIP_ALIGNMENT_REFERENCE_FRAMES: dict[str, ClipAlignmentReference] = {
    "hp-01": (28, 3.0),
    "hp-02": (44, 3.0),
}

# `clip_alignment` uses SIFT (not ORB): empirically, on this session's real footage,
# SIFT's scale-space keypoint detection recovers far more genuine correspondences than
# ORB's fixed pyramid under the pitch's repetitive grass texture (verified during this
# fix's implementation -- ORB with default params found 4/15 inliers on a pair SIFT
# resolves with >20 plausible inliers). `cv2.SIFT_create` ships in stock
# `opencv-python`/`opencv-python-headless` (patent expired 2020), no `opencv-contrib`
# dependency needed.
_ALIGNMENT_SIFT_CONTRAST_THRESHOLD = 0.02
_ALIGNMENT_SIFT_EDGE_THRESHOLD = 10

# `clip_alignment` sweeps this small set of Lowe ratio-test thresholds (not just one)
# and keeps the best PLAUSIBLE candidate (see `_is_plausible_alignment` below) with the
# highest inlier count -- a single fixed ratio sometimes lands on a spurious, highly-
# self-consistent-looking cluster of matches at one threshold while a different
# threshold recovers the genuine correspondence set; trying a few and filtering by
# plausibility is far more robust on real footage than committing to one value.
_ALIGNMENT_RATIO_THRESHOLDS: tuple[float, ...] = (0.7, 0.75, 0.8)

# `cv2.findHomography(..., cv2.RANSAC, ...)`'s reprojection-error threshold (px) for a
# correspondence to count as an inlier.
_ALIGNMENT_RANSAC_REPROJ_THRESHOLD = 5.0

# Below either guard, a candidate homography is discarded rather than trusted as a fit
# on too few/too-agreeing-by-chance correspondences.
_MIN_ALIGNMENT_INLIERS = 10
_MIN_ALIGNMENT_INLIER_RATIO = 0.15

# Below this many SIFT keypoints per frame, matching is not attempted at all.
_MIN_ALIGNMENT_KEYPOINTS = 4

# `_is_plausible_alignment`'s sanity bounds on a candidate homography's linear (2x2)
# part, decomposed after normalizing by the matrix's [2, 2] entry: a real between-clip
# drone drift/rezoom is a MODEST rotation and a scale factor within roughly 0.4x-2.5x
# (`_MIN_ALIGNMENT_DETERMINANT`/`_MAX_ALIGNMENT_DETERMINANT` bound the determinant,
# i.e. squared scale, so the linear scale range is roughly sqrt(0.16)=0.4 to
# sqrt(6.25)=2.5), never a near-180-degree rotation or a near-singular/negative-
# determinant (mirrored) transform -- RANSAC can occasionally converge on exactly such
# a degenerate fit when the true inlier set is thin and the outlier pool (repetitive
# grass texture) is large, and a plausible-looking inlier COUNT alone does not rule
# that out (empirically observed during this fix's implementation on real footage).
_MIN_ALIGNMENT_DETERMINANT = 0.16
_MAX_ALIGNMENT_DETERMINANT = 6.25
_MAX_ALIGNMENT_ROTATION_DEG = 30.0

# `_ecc_align`'s downscale factor for both frames before `cv2.findTransformECC` -- area-
# based ECC is far more expensive per-pixel than SIFT's sparse features, and a slightly
# softened/downsampled image is if anything easier for ECC's gradient-descent optimizer
# to converge on, not harder (see this module's "ECC second-stage fallback" docstring
# section). The recovered warp is rescaled back to full resolution before being returned.
_ECC_DOWNSCALE_FACTOR = 0.5

# `cv2.findTransformECC`'s termination criteria: stop after `_ECC_MAX_ITERATIONS`
# iterations or once the correlation-coefficient increment between iterations drops
# below `_ECC_TERMINATION_EPS`, whichever comes first.
_ECC_MAX_ITERATIONS = 5000
_ECC_TERMINATION_EPS = 1e-6

# `cv2.findTransformECC`'s optional Gaussian-blur pre-filter size (odd, pixels) -- the
# library's own default, kept explicit here rather than relying on the function's
# default value.
_ECC_GAUSS_FILT_SIZE = 5

# The minimum ECC correlation coefficient (`cv2.findTransformECC`'s own return value, in
# [-1, 1], "1" being a perfect match) a candidate ECC warp must clear to be trusted --
# tuned empirically against real footage; see `docs/homography-calibration.md`'s ECC
# fallback section for the full calibration data. Two independent checks were used, not
# just one: (1) max corner-pixel disagreement between an ECC-only result and SIFT's own
# result on clips SIFT ALSO aligns -- this turned out to be a MISLEADING metric on this
# footage's extreme-perspective homographies (tiny parameter differences blow up into
# thousands of pixels when extrapolated to the frame corners, which sit far outside the
# region any correspondence actually supports) and was discarded as the tuning signal;
# (2) a direct visual check -- warp the clip through the candidate H_align and alpha-
# blend it against the reference frame (green=reference, red=warped clip; overlap reads
# yellow) -- which is what this threshold is actually tuned against. Confirmed BAD
# (double-vision features, no real overlap) at correlation 0.336/0.384; confirmed GOOD
# (features overlap closely) from 0.440 upward, including the clip 11 case this fallback
# exists for. A correlation below this is not distinguishable from two frames that
# simply don't share enough overlapping, stable content for area-based registration to
# lock onto.
_ECC_MIN_CORRELATION = 0.42

FIELD_LANDMARKS: tuple[str, ...] = (
    "goalline_west_south",
    "goalline_west_north",
    "goalline_east_south",
    "goalline_east_north",
    "endzone_west_back_south",
    "endzone_west_back_north",
    "endzone_east_back_south",
    "endzone_east_back_north",
    "yardline_5_south",
    "yardline_5_north",
    "yardline_45_south",
    "yardline_45_north",
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
        # The pilot field carries only the IFAF no-run-zone lines 5 yards off each
        # goal line -- intermediate 10/20/30/40 yardlines are NOT painted on this
        # field and were dropped from the vocabulary so nobody is tempted to guess
        # invisible lines during calibration.
        "yardline_5_south": (5.0, 0.0),
        "yardline_5_north": (5.0, width),
        "yardline_45_south": (length - 5.0, 0.0),
        "yardline_45_north": (length - 5.0, width),
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

    @classmethod
    def from_matrix(cls, matrix: np.ndarray) -> "ViewTransformer":
        """Build a `ViewTransformer` directly from a precomputed 3x3 projective
        matrix, bypassing `cv2.findHomography` -- used by
        `coordinates.composed_transformer_for` to wrap `M_calibration @ H_align`
        (see this module's "Per-clip homography refinement" docstring section) in the
        same `transform_points`/`transform_image` interface every other
        `ViewTransformer` consumer already relies on.
        """
        instance = cls.__new__(cls)
        instance.m = np.asarray(matrix, dtype=np.float64)
        return instance

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


def _read_frame_at(clip: Path, at_second: float) -> "np.ndarray":
    """Decode and return the single frame nearest `at_second` from `clip`, via
    `cv2.VideoCapture` -- factored out of `pick_points` so `clip_alignment_matrix` can
    extract a clean (unannotated) reference frame the exact same way, without seeking
    through the `_ref.jpg`'s grid-line annotations that would corrupt feature matching.
    """
    import cv2

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

    return frame


def _mid_clip_frame(clip: Path) -> "np.ndarray":
    """Decode and return the frame at `clip`'s midpoint (by frame count) -- the
    representative frame `clip_alignment_matrix` registers against a hover position's
    reference frame. A clip midpoint is a stable, content-agnostic choice: no
    assumption about where in the clip the field/players are best visible is needed,
    unlike `at_second`-style fixed timestamps tuned per calibration still.
    """
    import cv2

    cap = cv2.VideoCapture(str(clip))
    try:
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        mid_index = max(0, frame_count // 2)
        cap.set(cv2.CAP_PROP_POS_FRAMES, mid_index)
        ok, frame = cap.read()
    finally:
        cap.release()

    if not ok or frame is None:
        raise CalibrationError(f"could not extract a mid-clip frame from {clip}")

    return frame


def _is_plausible_alignment(matrix: np.ndarray) -> bool:
    """True when `matrix`'s linear (2x2) part, after normalizing by `matrix[2, 2]`,
    decomposes to a determinant and rotation within `_MIN_ALIGNMENT_DETERMINANT`/
    `_MAX_ALIGNMENT_DETERMINANT`/`_MAX_ALIGNMENT_ROTATION_DEG` -- see those constants'
    definitions for why a real between-clip drone drift/rezoom never produces a
    near-180-degree rotation or a near-singular/negative-determinant fit, even though
    RANSAC can occasionally converge on exactly such a degenerate solution when the
    true inlier set is thin (this module's "Per-clip homography refinement" section).
    """
    normalized = matrix / matrix[2, 2]
    linear = normalized[:2, :2]
    determinant = np.linalg.det(linear)
    if not (_MIN_ALIGNMENT_DETERMINANT <= determinant <= _MAX_ALIGNMENT_DETERMINANT):
        return False

    rotation_deg = math.degrees(
        math.atan2(linear[1, 0] - linear[0, 1], linear[0, 0] + linear[1, 1])
    )
    return abs(rotation_deg) <= _MAX_ALIGNMENT_ROTATION_DEG


def _to_grayscale(frame: "np.ndarray") -> "np.ndarray":
    """Single-channel view of `frame` for `cv2.findTransformECC`, which requires a
    single-channel image -- a no-op if `frame` is already single-channel (the tiny
    synthetic test frames this module's tests build sometimes are).
    """
    import cv2

    if frame.ndim == 3:
        return cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    return frame


def _ecc_align(
    clip_frame: "np.ndarray", reference_frame: "np.ndarray"
) -> tuple[np.ndarray, float] | None:
    """Second-stage registration attempted by `clip_alignment` when the SIFT/RANSAC
    sweep finds no plausible, well-supported fit (this module's "ECC second-stage
    fallback" docstring section). Returns `(H_align, correlation_coefficient)` with
    `H_align` already rescaled back to `clip_frame`'s/`reference_frame`'s full
    resolution (both were downscaled by `_ECC_DOWNSCALE_FACTOR` internally for speed),
    or `None` if `cv2.findTransformECC` fails to converge at every motion model
    attempted (raises `cv2.error` -- e.g. two frames with no shared gradient structure
    to lock onto at all).

    `MOTION_EUCLIDEAN` (rotation+translation only) is tried first, initialized from
    identity -- the physically appropriate model for a hovering drone's between-clip
    drift -- then a second `MOTION_HOMOGRAPHY` pass, seeded from the Euclidean result,
    attempts to additionally recover any residual perspective/zoom; kept only if it
    also converges, otherwise the Euclidean-only result is returned. The caller
    (`clip_alignment`) is responsible for the correlation-coefficient and plausibility
    acceptance gates -- this function reports what ECC found, it does not judge it.
    """
    import cv2

    clip_gray = _to_grayscale(clip_frame)
    reference_gray = _to_grayscale(reference_frame)

    clip_small = cv2.resize(
        clip_gray,
        None,
        fx=_ECC_DOWNSCALE_FACTOR,
        fy=_ECC_DOWNSCALE_FACTOR,
        interpolation=cv2.INTER_AREA,
    )
    reference_small = cv2.resize(
        reference_gray,
        None,
        fx=_ECC_DOWNSCALE_FACTOR,
        fy=_ECC_DOWNSCALE_FACTOR,
        interpolation=cv2.INTER_AREA,
    )

    criteria = (
        cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT,
        _ECC_MAX_ITERATIONS,
        _ECC_TERMINATION_EPS,
    )

    # `cv2.findTransformECC(templateImage, inputImage, ...)` returns a warpMatrix W
    # such that a templateImage point maps onto its corresponding inputImage point:
    # p_input = W @ p_template. Passing clip_small as the template and reference_small
    # as the input therefore yields W mapping clip pixels -> reference pixels directly
    # -- exactly `clip_alignment`'s own H_align contract, matching the SIFT/RANSAC path
    # above (`cv2.findHomography(src_pts=clip keypoints, dst_pts=reference keypoints)`).
    warp_euclidean = np.eye(2, 3, dtype=np.float32)
    try:
        cc, warp_euclidean = cv2.findTransformECC(
            clip_small,
            reference_small,
            warp_euclidean,
            cv2.MOTION_EUCLIDEAN,
            criteria,
            None,
            _ECC_GAUSS_FILT_SIZE,
        )
    except cv2.error:
        return None

    best_warp = np.vstack([warp_euclidean, [0.0, 0.0, 1.0]]).astype(np.float32)
    best_cc = float(cc)

    try:
        cc_h, warp_homography = cv2.findTransformECC(
            clip_small,
            reference_small,
            best_warp.copy(),
            cv2.MOTION_HOMOGRAPHY,
            criteria,
            None,
            _ECC_GAUSS_FILT_SIZE,
        )
        best_warp = np.asarray(warp_homography, dtype=np.float32)
        best_cc = float(cc_h)
    except cv2.error:
        pass  # keep the Euclidean-only result; the homography refinement didn't converge

    # Rescale the downscaled-space warp back to full resolution: a full-res point p is
    # first scaled down (S @ p), warped in downscaled space, then scaled back up
    # (S^-1 @ ...) -- H_full = S^-1 @ W_small @ S.
    scale = np.diag([_ECC_DOWNSCALE_FACTOR, _ECC_DOWNSCALE_FACTOR, 1.0])
    scale_inv = np.diag([1.0 / _ECC_DOWNSCALE_FACTOR, 1.0 / _ECC_DOWNSCALE_FACTOR, 1.0])
    h_full = scale_inv @ np.asarray(best_warp, dtype=np.float64) @ scale

    return h_full, best_cc


def clip_alignment(clip_frame: "np.ndarray", reference_frame: "np.ndarray") -> np.ndarray:
    """Register `clip_frame` onto `reference_frame`, returning the 3x3 `H_align` that
    maps a pixel in `clip_frame`'s space onto the corresponding pixel in
    `reference_frame`'s space (see this module's "Per-clip homography refinement"
    docstring section).

    SIFT features (`cv2.SIFT_create`) + Lowe's-ratio-test `knnMatch` (k=2, swept over
    `_ALIGNMENT_RATIO_THRESHOLDS`) + `cv2.findHomography(..., cv2.RANSAC,
    _ALIGNMENT_RANSAC_REPROJ_THRESHOLD)`. RANSAC's own inlier/outlier voting is the
    outlier rejection for moving players -- the planar, static pitch dominates the
    frame and produces far more mutually-consistent correspondences than any one
    moving person's apparent displacement, so no separate player mask is needed. Each
    ratio threshold's RANSAC result is additionally checked by
    `_is_plausible_alignment`; among the thresholds that produce a plausible fit with
    enough inliers, the one with the most inliers wins.

    Falls back to `np.eye(3)` (identity -- never a garbage transform) with a
    `UserWarning` notice when either frame yields fewer than
    `_MIN_ALIGNMENT_KEYPOINTS` SIFT keypoints (ECC is not attempted in this case either
    -- a frame with essentially no detectable structure at all is equally unpromising
    for area-based registration), or BOTH: no ratio threshold in
    `_ALIGNMENT_RATIO_THRESHOLDS` produces a `cv2.findHomography` solution that is BOTH
    plausible (`_is_plausible_alignment`) AND clears
    `_MIN_ALIGNMENT_INLIERS`/`_MIN_ALIGNMENT_INLIER_RATIO`, AND the `_ecc_align` second
    stage (this module's "ECC second-stage fallback" docstring section) also fails to
    converge, or converges to a fit below `_ECC_MIN_CORRELATION` or failing
    `_is_plausible_alignment`.
    """
    import cv2

    sift = cv2.SIFT_create(
        nfeatures=0,
        contrastThreshold=_ALIGNMENT_SIFT_CONTRAST_THRESHOLD,
        edgeThreshold=_ALIGNMENT_SIFT_EDGE_THRESHOLD,
    )
    keypoints_clip, descriptors_clip = sift.detectAndCompute(clip_frame, None)
    keypoints_ref, descriptors_ref = sift.detectAndCompute(reference_frame, None)

    if (
        descriptors_clip is None
        or descriptors_ref is None
        or len(keypoints_clip) < _MIN_ALIGNMENT_KEYPOINTS
        or len(keypoints_ref) < _MIN_ALIGNMENT_KEYPOINTS
    ):
        warnings.warn(
            "clip_alignment: too few SIFT keypoints detected "
            f"(clip={0 if keypoints_clip is None else len(keypoints_clip)}, "
            f"reference={0 if keypoints_ref is None else len(keypoints_ref)}); "
            "falling back to identity",
            stacklevel=2,
        )
        return np.eye(3)

    matcher = cv2.BFMatcher(cv2.NORM_L2)
    knn_matches = matcher.knnMatch(descriptors_clip, descriptors_ref, k=2)

    best: tuple[int, np.ndarray] | None = None
    best_report = ""
    best_report_inliers = -1
    for ratio_threshold in _ALIGNMENT_RATIO_THRESHOLDS:
        good_matches = [
            m
            for pair in knn_matches
            if len(pair) == 2
            for m, n in [pair]
            if m.distance < ratio_threshold * n.distance
        ]
        if len(good_matches) < 4:
            continue

        src_pts = np.float32(
            [keypoints_clip[m.queryIdx].pt for m in good_matches]
        ).reshape(-1, 1, 2)
        dst_pts = np.float32(
            [keypoints_ref[m.trainIdx].pt for m in good_matches]
        ).reshape(-1, 1, 2)

        matrix, mask = cv2.findHomography(
            src_pts, dst_pts, cv2.RANSAC, _ALIGNMENT_RANSAC_REPROJ_THRESHOLD
        )
        if matrix is None:
            continue

        inlier_count = int(mask.sum()) if mask is not None else 0
        inlier_ratio = inlier_count / len(good_matches) if good_matches else 0.0
        plausible = _is_plausible_alignment(matrix)
        if (
            inlier_count >= _MIN_ALIGNMENT_INLIERS
            and inlier_ratio >= _MIN_ALIGNMENT_INLIER_RATIO
            and plausible
            and (best is None or inlier_count > best[0])
        ):
            best = (inlier_count, np.asarray(matrix, dtype=np.float64))
        if inlier_count > best_report_inliers:
            best_report_inliers = inlier_count
            best_report = (
                f"ratio={ratio_threshold} inliers={inlier_count}/{len(good_matches)} "
                f"({inlier_ratio:.0%}) plausible={plausible}"
            )

    if best is None:
        sift_failure = (
            f"no ratio threshold in {_ALIGNMENT_RATIO_THRESHOLDS} produced a plausible "
            f"fit clearing >= {_MIN_ALIGNMENT_INLIERS} inliers and >= "
            f"{_MIN_ALIGNMENT_INLIER_RATIO:.0%} inlier ratio (best attempt: {best_report})"
        )

        ecc_result = _ecc_align(clip_frame, reference_frame)
        if ecc_result is not None:
            ecc_matrix, ecc_cc = ecc_result
            if ecc_cc >= _ECC_MIN_CORRELATION and _is_plausible_alignment(ecc_matrix):
                warnings.warn(
                    f"clip_alignment: {sift_failure}; ECC second-stage fallback "
                    f"succeeded (correlation={ecc_cc:.3f} >= {_ECC_MIN_CORRELATION})",
                    stacklevel=2,
                )
                return ecc_matrix

            warnings.warn(
                f"clip_alignment: {sift_failure}; ECC second-stage fallback also "
                f"insufficient (correlation={ecc_cc:.3f}, plausible="
                f"{_is_plausible_alignment(ecc_matrix)}); falling back to identity",
                stacklevel=2,
            )
            return np.eye(3)

        warnings.warn(
            f"clip_alignment: {sift_failure}; ECC second-stage fallback did not "
            "converge either; falling back to identity",
            stacklevel=2,
        )
        return np.eye(3)

    return best[1]


def clip_alignment_matrix(hover_position_id: str, clip_number: int, config: "Config") -> np.ndarray:
    """Return the 3x3 `H_align` mapping `clip_number`'s pixel space onto
    `hover_position_id`'s calibration reference clip's pixel space (see
    `CLIP_ALIGNMENT_REFERENCE_FRAMES`), computed via `clip_alignment` on a mid-clip
    frame (`_mid_clip_frame`) against a clean frame extracted fresh from the reference
    clip (`_read_frame_at`, never the annotated `_ref.jpg`).

    Returns identity, with no computation attempted, when `hover_position_id` has no
    registered reference frame (a synthetic/test hover position, or a real one this
    mapping hasn't been extended to yet) or when `clip_number` IS the reference clip
    itself. Never raises: any failure resolving clip paths or extracting/registering
    frames is caught and degrades to identity with a `UserWarning` notice naming
    `clip_number`/`hover_position_id` and the underlying error, so one bad clip can
    never abort a whole `add_field_coordinates`/`measure_position_error` run.
    """
    reference = CLIP_ALIGNMENT_REFERENCE_FRAMES.get(hover_position_id)
    if reference is None:
        return np.eye(3)

    reference_clip_number, reference_at_second = reference
    if clip_number == reference_clip_number:
        return np.eye(3)

    try:
        from flag_football_ep.cv.frames import clip_number as clip_number_of
        from flag_football_ep.cv.frames import clip_paths

        clips_by_number = {
            clip_number_of(path): path
            for path in clip_paths(config, config.cv.pilot_session_id)
        }
        clip_path = clips_by_number.get(clip_number)
        reference_clip_path = clips_by_number.get(reference_clip_number)
        if clip_path is None:
            raise ClipAlignmentUnresolvable(f"clip {clip_number} not found for registration")
        if reference_clip_path is None:
            raise ClipAlignmentUnresolvable(
                f"reference clip {reference_clip_number} not found for registration"
            )

        clip_frame = _mid_clip_frame(clip_path)
        reference_frame = _read_frame_at(reference_clip_path, reference_at_second)
    except Exception as exc:  # noqa: BLE001 - any resolution failure degrades to identity
        warnings.warn(
            f"clip_alignment_matrix: could not resolve/extract frames for clip "
            f"{clip_number} (hover position {hover_position_id!r}) against reference "
            f"clip {reference_clip_number}: {exc}; falling back to identity",
            stacklevel=2,
        )
        return np.eye(3)

    return clip_alignment(clip_frame, reference_frame)


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

    frame = _read_frame_at(clip, at_second)

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
