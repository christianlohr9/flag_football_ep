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
the `github.com/roboflow/sports` `sports/common/view.py` shape (MIT), ported verbatim by
the implementing plan. Field-keypoint models for moving cameras are explicitly out of
scope this phase (D-05) -- calibration here is static, per fixed hover position only.

Implemented by plan 02.1-04.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    import numpy as np
    import polars as pl


class CalibrationError(CvError, ValueError):
    """Raised when a calibration point set is degenerate/collinear, has fewer than
    four points, or cannot be resolved for a requested hover position.
    """


class ViewTransformer:
    """Homography-based point/image projection between a clip's pixel space and the
    field's yard space, given a fitted source/target point correspondence.
    """

    def __init__(self, source: np.ndarray, target: np.ndarray) -> None:
        raise NotImplementedError(
            "cv.homography.ViewTransformer.__init__ is implemented by plan 02.1-04"
        )

    def transform_points(self, points: np.ndarray) -> np.ndarray:
        raise NotImplementedError(
            "cv.homography.ViewTransformer.transform_points is implemented by plan 02.1-04"
        )

    def transform_image(self, image: np.ndarray, resolution_wh: tuple[int, int]) -> np.ndarray:
        raise NotImplementedError(
            "cv.homography.ViewTransformer.transform_image is implemented by plan 02.1-04"
        )


def load_calibration(path: Path) -> pl.DataFrame:
    """Load the hand-maintained homography calibration CSV at `path`, rejecting
    degenerate/collinear point sets and naming the offending hover_position_id.
    """
    raise NotImplementedError("cv.homography.load_calibration is implemented by plan 02.1-04")


def transformer_for(hover_position_id: str, calibration: pl.DataFrame) -> ViewTransformer:
    """Build a `ViewTransformer` for `hover_position_id` from the loaded `calibration`
    frame, raising `CalibrationError` if no rows match.
    """
    raise NotImplementedError("cv.homography.transformer_for is implemented by plan 02.1-04")


def reprojection_error_yards(
    transformer: ViewTransformer, source: np.ndarray, target: np.ndarray
) -> np.ndarray:
    """Measure, in yards, how well `transformer` reproduces `target` from `source` --
    the calibration-quality diagnostic reported alongside the pilot gate's position error.
    """
    raise NotImplementedError(
        "cv.homography.reprojection_error_yards is implemented by plan 02.1-04"
    )


def pick_points(clip: Path, hover_position_id: str, out_csv: Path, *, at_second: float) -> Path:
    """Extract a still frame from `clip` at `at_second` and seed `out_csv` with a
    row template for the operator to hand-pick point correspondences for
    `hover_position_id`.
    """
    raise NotImplementedError("cv.homography.pick_points is implemented by plan 02.1-04")
