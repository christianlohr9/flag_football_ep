"""Field-coordinate projection of tracked player boxes.

`foot_point` reduces a detection box to the single pixel point (bottom-center) that
represents where a player's feet touch the ground -- the correct point to project
through a homography (a box's center drifts with player height/pose; the foot point
does not). `add_field_coordinates` applies `homography.transformer_for` per hover
position to every track row's foot point, adding field x/y-in-yards columns to the
tracks frame -- the join key between raw pixel-space tracking output and everything
downstream (`accuracy.measure_position_error`, `radar.render_radar_frame`).

Implemented by plan 02.1-13, after `homography.py`'s calibration machinery (plan
02.1-04) and `track.py`'s tracking output (plan 02.1-12) both exist.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

    from flag_football_ep.config import Config


def foot_point(xyxy) -> tuple[float, float]:
    """Reduce a detection box (`x1, y1, x2, y2`) to its bottom-center pixel point."""
    raise NotImplementedError("cv.coordinates.foot_point is implemented by plan 02.1-13")


def add_field_coordinates(tracks: pl.DataFrame, config: Config, calibration: pl.DataFrame) -> pl.DataFrame:
    """Project every track row's foot point through its hover position's homography,
    adding field x/y-in-yards columns to `tracks`.
    """
    raise NotImplementedError(
        "cv.coordinates.add_field_coordinates is implemented by plan 02.1-13"
    )
