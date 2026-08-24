"""Position-error measurement against hand-labeled ground truth: the C-09 "~<=1m
position error" gate metric.

`load_gt_positions` loads the hand-labeled ground-truth CSV (`config.reference.gt_positions`,
following `reference._read_reference_csv`'s typed-schema-loader convention).
`prepare_gt_frames` exports a sample of `n_frames` tracked frames for an operator to
hand-label field positions on (seeding `GT_COLUMNS`-shaped rows for the operator to
fill in), the inverse of `measure_position_error`, which joins the filled-in ground
truth against `tracks` (via `coordinates.add_field_coordinates`'s field-yard columns)
and reports median/p90/max error in yards, plus a per-zone breakdown -- reported as
measured error distributions, never a bare pass/fail number, matching the "Richtwert,
kein Messprotokoll" statistical-honesty framing `docs/capture-protocol.md` already
establishes for this project's gate documentation.

Implemented by plan 02.1-15.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

    from flag_football_ep.config import Config

GT_COLUMNS: tuple[str, ...] = ()
GT_SCHEMA: dict[str, pl.DataType] = {}


def load_gt_positions(path: Path) -> pl.DataFrame:
    """Load the hand-labeled ground-truth position CSV at `path`."""
    raise NotImplementedError("cv.accuracy.load_gt_positions is implemented by plan 02.1-15")


def prepare_gt_frames(config: Config, tracks: pl.DataFrame, *, n_frames: int, out_dir: Path) -> Path:
    """Export `n_frames` tracked frames for hand ground-truth labeling, seeding
    `GT_COLUMNS`-shaped rows under `out_dir`.
    """
    raise NotImplementedError("cv.accuracy.prepare_gt_frames is implemented by plan 02.1-15")


@dataclass(frozen=True)
class AccuracyResult:
    """The measured position-error distribution against ground truth: point count,
    median/p90/max error in yards, and a per-zone breakdown.
    """

    n_points: int
    median_yards: float
    p90_yards: float
    max_yards: float
    per_zone: dict


def measure_position_error(gt: pl.DataFrame, tracks: pl.DataFrame, config: Config) -> AccuracyResult:
    """Join `gt` against `tracks`' field-yard coordinates and measure the position
    error distribution.
    """
    raise NotImplementedError(
        "cv.accuracy.measure_position_error is implemented by plan 02.1-15"
    )
