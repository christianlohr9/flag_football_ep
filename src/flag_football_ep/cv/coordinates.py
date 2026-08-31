"""Field-coordinate projection of tracked player boxes.

`foot_point` reduces a detection box (`x1, y1, x2, y2`) to the single pixel point
(bottom-center) that represents where a player's feet touch the ground -- the correct
point to project through a homography fitted on field-plane landmarks. A box's center
drifts with player height/pose (a taller player's box center sits further from the
field plane than a shorter one's), introducing a systematic error that scales with
exactly the ~1m error budget the C-09 gate measures against; the foot point does not.

`add_field_coordinates` applies `homography.transformer_for` per hover position to
every track row's foot point (already computed as `foot_x_px`/`foot_y_px` by
`track.track_session`), adding field x/y-in-yards columns to the tracks frame -- the
join key between raw pixel-space tracking output and everything downstream
(`accuracy.measure_position_error`, `radar.render_radar_frame`).

`composed_transformer_for(hover_position_id, clip_number, calibration, config)`
composes that per-hover-position calibrated homography with
`homography.clip_alignment_matrix`'s per-clip drift-correction homography
(`M_total = M_calibration @ H_align`): a clip's own pixel first maps onto its hover
position's calibration reference clip's pixel space, then through the calibrated
homography into field yards -- see `homography.py`'s "Per-clip homography refinement"
docstring section for why this composition exists. `add_field_coordinates` groups
every hover-position group further by `clip_number` and builds one composed
transformer per distinct clip present (never per row -- `clip_alignment_matrix` itself
is the expensive step, ORB feature extraction over two decoded video frames).
`accuracy._transform_gt_to_yards` composes the SAME way, so the pipeline's tracks and
the ground-truth points they are measured against are never projected through
different corrections.

`add_field_coordinates`'s return type is locked to a bare `pl.DataFrame` by
`02.1-02-PLAN.md`'s interfaces table and by `cv.commands.coords` (not touched by this
plan -- see its own docstring: "plans 03-16 ... never have to edit it"), which passes
the return value straight into `write_tracking_parquet(projected, ...)` and reads
`projected.height`. The per-hover-position out-of-bounds count the plan's action text
calls "a notice" is therefore surfaced via `warnings.warn` (one warning per affected
hover position, naming the position and the count), matching the `UserWarning`
convention `homography._read_calibration_csv` already established for
non-fatal-but-worth-surfacing conditions -- not a second return value, since the CLI
consumer's signature cannot change.

Implemented by plan 02.1-13, after `homography.py`'s calibration machinery (plan
02.1-04) and `track.py`'s tracking output (plan 02.1-12) both exist.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import polars as pl

from flag_football_ep.cv.homography import ViewTransformer, transformer_for
from flag_football_ep.cv.schema import conform_tracking

if TYPE_CHECKING:
    from flag_football_ep.config import Config

# A transformed coordinate landing further than this many yards outside the playing
# field (already including both end zones) is flagged in a notice, not dropped -- a
# large out-of-bounds share is itself the gate-relevant signal that a calibration is
# wrong (T-2.1-02), and silently discarding the row would hide exactly that evidence.
_OUT_OF_BOUNDS_MARGIN_YARDS = 5.0

# Internal-only column name for the row-order-preserving index used to reassemble the
# per-hover-position groups back into their original row order after transforming.
_ROW_INDEX_COLUMN = "_ffep_coordinates_row_index"


def composed_transformer_for(
    hover_position_id: str, clip_number: int, calibration: pl.DataFrame, config: "Config"
) -> ViewTransformer:
    """Compose `homography.transformer_for(hover_position_id, calibration)` with
    `homography.clip_alignment_matrix(hover_position_id, clip_number, config)`: `M_total
    = M_calibration @ H_align`. `clip_alignment_matrix` returns identity for hover
    positions with no registered reference frame and for the reference clip itself
    (see that function's docstring), so this composition is byte-identical to plain
    `transformer_for` in both of those cases -- calling it for every hover
    position/clip pair (real or synthetic/test) is always safe.
    """
    from flag_football_ep.cv.homography import clip_alignment_matrix

    transformer = transformer_for(hover_position_id, calibration)
    h_align = clip_alignment_matrix(hover_position_id, clip_number, config)
    return ViewTransformer.from_matrix(transformer.m @ h_align)


def foot_point(xyxy) -> tuple[float, float]:
    """Reduce a detection box (`x1, y1, x2, y2`) to its bottom-center pixel point."""
    x1, y1, x2, y2 = xyxy
    return ((x1 + x2) / 2.0, y2)


def add_field_coordinates(tracks: pl.DataFrame, config: Config, calibration: pl.DataFrame) -> pl.DataFrame:
    """Project every track row's foot point through its hover position's homography,
    adding field x/y-in-yards columns to `tracks`.

    Every distinct non-null `hover_position_id` present in `tracks` must have a
    calibration in `calibration` -- `homography.transformer_for` raises
    `CalibrationError` naming the offending hover position when it does not, and that
    exception is left to propagate rather than being caught here, so a missing
    calibration can never masquerade as an empty/silent region of the field (a null
    `x_yards`/`y_yards` column would look exactly like "no tracks there"). Rows whose
    `hover_position_id` is itself null are left with null `x_yards`/`y_yards` (already
    the nullable-column default `schema.conform_tracking` allows).

    Transformed coordinates landing more than `_OUT_OF_BOUNDS_MARGIN_YARDS` yards
    outside the field (including both end zones) are kept, not dropped; a
    `UserWarning` reports the count per affected hover position (see module docstring
    for why this is a warning, not a second return value).

    Returns `tracks` with `x_yards`/`y_yards` filled, passed back through
    `schema.conform_tracking`.
    """
    if tracks.height == 0:
        return conform_tracking(tracks)

    indexed = tracks.with_row_index(_ROW_INDEX_COLUMN)

    hover_position_ids = (
        indexed.filter(pl.col("hover_position_id").is_not_null())["hover_position_id"]
        .unique(maintain_order=True)
        .to_list()
    )

    length = config.cv.field_length_yards
    width = config.cv.field_width_yards
    endzone = config.cv.endzone_yards
    x_min = -endzone - _OUT_OF_BOUNDS_MARGIN_YARDS
    x_max = length + endzone + _OUT_OF_BOUNDS_MARGIN_YARDS
    y_min = -_OUT_OF_BOUNDS_MARGIN_YARDS
    y_max = width + _OUT_OF_BOUNDS_MARGIN_YARDS

    projected_groups: list[pl.DataFrame] = []
    for hover_position_id in hover_position_ids:
        group = indexed.filter(pl.col("hover_position_id") == hover_position_id)

        # Per-clip homography refinement (see module docstring): compose the
        # hover position's calibrated homography with one clip_alignment_matrix per
        # DISTINCT clip present in this group, never per row -- clip_alignment_matrix
        # is the expensive step (ORB feature extraction over two decoded video
        # frames), and every row of the same clip shares the exact same composed
        # transformer.
        clip_numbers_in_group = (
            group.filter(pl.col("clip_number").is_not_null())["clip_number"]
            .unique(maintain_order=True)
            .to_list()
        )

        clip_groups: list[pl.DataFrame] = []
        for clip_number_value in clip_numbers_in_group:
            composed = composed_transformer_for(
                hover_position_id, clip_number_value, calibration, config
            )
            clip_group = group.filter(pl.col("clip_number") == clip_number_value)
            source = clip_group.select("foot_x_px", "foot_y_px").to_numpy()
            projected = composed.transform_points(source)
            clip_groups.append(
                clip_group.with_columns(
                    [
                        pl.Series("x_yards", projected[:, 0]).cast(pl.Float64),
                        pl.Series("y_yards", projected[:, 1]).cast(pl.Float64),
                    ]
                )
            )

        null_clip_group = group.filter(pl.col("clip_number").is_null())
        if null_clip_group.height:
            clip_groups.append(null_clip_group)

        group = pl.concat(clip_groups, how="vertical").sort(_ROW_INDEX_COLUMN)

        out_of_bounds = group.filter(
            (pl.col("x_yards") < x_min)
            | (pl.col("x_yards") > x_max)
            | (pl.col("y_yards") < y_min)
            | (pl.col("y_yards") > y_max)
        ).height
        if out_of_bounds:
            warnings.warn(
                f"hover position {hover_position_id!r}: {out_of_bounds} of "
                f"{group.height} transformed row(s) fall outside the field plus "
                f"{_OUT_OF_BOUNDS_MARGIN_YARDS}-yard margin -- check the calibration",
                stacklevel=2,
            )

        projected_groups.append(group)

    null_hover_group = indexed.filter(pl.col("hover_position_id").is_null())
    if null_hover_group.height:
        projected_groups.append(null_hover_group)

    combined = (
        pl.concat(projected_groups, how="vertical")
        .sort(_ROW_INDEX_COLUMN)
        .drop(_ROW_INDEX_COLUMN)
    )

    return conform_tracking(combined)
