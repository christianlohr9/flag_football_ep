"""Top-down radar-view rendering of tracked/projected field positions.

`render_radar_frame` is the headless-render function (mirroring
`charts/pat_breakeven.py`'s render/write split -- a pure function returning pixel data,
separate from the path-writing wrapper) that draws one frame's field-yard positions as
a top-down radar image. `render_showcase_reel` composes a sequence of these frames
alongside the raw footage's own tracking overlay (`overlay.draw_frame`) across
`clip_numbers` into a single side-by-side output video -- the pilot's go/no-go
presentation artifact (D-16).

The pitch is drawn from `config.cv.field_length_yards`/`field_width_yards`/
`endzone_yards` using a single yards-to-pixels scale factor shared by both axes
(`_pitch_geometry`), so it is never stretched -- an aspect-distorted radar would
misrepresent player spacing to exactly the audience least equipped to notice. The
field coordinate convention is D-13: x=0 at the west goal line (drawn on the left),
x=`field_length_yards` at the east goal line, y=0 at the south sideline (drawn at the
bottom of the frame), y=`field_width_yards` at the north sideline.

Team/marker colours share `cv.palette`'s single definition with `overlay.py`, so the
two halves of the showcase reel read as the same event: `team_id` 0 draws red and
`team_id` 1 draws blue (matching `teams.assign_teams`'s jersey-colour anchoring), a
third colour+shape for `class_name == "referee"`, a fourth for a null `team_id`. Rows
with a null `x_yards`/`y_yards` are skipped without raising -- `render_showcase_reel`,
the caller that drives `render_radar_frame` frame by frame, counts and logs how many
rows it skipped per reel, so a silently thinner radar is reported rather than silently
misrepresenting the tracking.

Only ON-FIELD positions are drawn: sideline/bench people are deliberately tracked
(~25% of rows, by design -- see D-13) so continuity and accuracy metrics can account
for them, but plotting them on the radar clutters a view meant to read as "the play in
progress." `_is_on_field` (pitch + both end zones + a 1-yard margin) filters them out
of the radar draw only; the underlying tracking data and every non-radar consumer
(overlay video, accuracy metrics) is untouched.

No in-repo video-rendering precedent exists (existing charts render static PNGs via
matplotlib/Agg); this module's actual `cv2.VideoWriter`/compositing logic has no
in-repo analog and is built directly from scratch by this plan.

Implemented by plan 02.1-16.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

from flag_football_ep.cv import CvError
from flag_football_ep.cv.palette import NULL_TEAM_COLOR as _NULL_TEAM_COLOR
from flag_football_ep.cv.palette import REFEREE_COLOR as _REFEREE_COLOR
from flag_football_ep.cv.palette import TEAM_COLORS as _TEAM_COLORS

if TYPE_CHECKING:
    import polars as pl

    from flag_football_ep.config import Config

logger = logging.getLogger(__name__)


class NoFieldCoordinatesForClip(CvError, ValueError):
    """Raised when `render_showcase_reel` is asked to render a clip whose tracks carry
    no rows with non-null field coordinates -- an empty radar half would misrepresent
    the pipeline to exactly the audience that cannot tell the difference (T-2.1-40).
    """


# Team/referee/null-team colours live in `cv.palette` -- the same shared definition
# `overlay.py` imports, so the reel's two halves can never drift out of sync.

_PITCH_COLOR: tuple[int, int, int] = (40, 95, 40)  # dark turf green
_LINE_COLOR: tuple[int, int, int] = (235, 235, 235)
_YARD_LINE_EVERY = 5
_MARKER_RADIUS = 5
_MARKER_HALF_SIZE = 5
_FONT = 0  # cv2.FONT_HERSHEY_SIMPLEX, hardcoded to avoid an eager `import cv2` here
_FONT_SCALE = 0.4
_TEXT_THICKNESS = 1

_HEADER_HEIGHT_PX = 40
_HEADER_COLOR: tuple[int, int, int] = (0, 0, 0)
_HEADER_TEXT_COLOR: tuple[int, int, int] = (255, 255, 255)

# Fixed (not fps-scaled) frame count for the black separator burnt in between plays --
# deterministic so `render_showcase_reel`'s frame-count contract stays testable without
# reproducing a per-clip fps-rounding computation in the caller.
_SEPARATOR_FRAMES = 5

# Margin (in yards) added around the pitch + both end zones for the radar's on-field
# draw filter (`_is_on_field`) -- generous enough that a player legitimately near the
# boundary is never clipped, tight enough that sideline/bench people (tracked well off
# the pitch) still get filtered out.
_ON_FIELD_MARGIN_YARDS = 1.0


def _pitch_geometry(config: "Config", size_wh: tuple[int, int]) -> dict:
    """Compute the single yards-to-pixels scale (identical on both axes) and the pixel
    offsets that center the full pitch -- including both end zones -- inside `size_wh`.

    Never distorts the pitch: `scale` is the smaller of the two axis-fitting ratios, so
    the pitch is letterboxed (not stretched) to fill `size_wh`.
    """
    width_px, height_px = size_wh
    field_length = config.cv.field_length_yards
    field_width = config.cv.field_width_yards
    endzone = config.cv.endzone_yards

    total_x_yards = field_length + 2 * endzone
    total_y_yards = field_width

    scale = min(width_px / total_x_yards, height_px / total_y_yards)
    draw_width = total_x_yards * scale
    draw_height = total_y_yards * scale

    return {
        "scale": scale,
        "offset_x": (width_px - draw_width) / 2,
        "offset_y": (height_px - draw_height) / 2,
        "field_length_yards": field_length,
        "field_width_yards": field_width,
        "endzone_yards": endzone,
    }


def _yards_to_px(x_yards: float, y_yards: float, geometry: dict) -> tuple[int, int]:
    """Project a `(x_yards, y_yards)` field position (D-13 convention) into a pixel
    position under `geometry` (as returned by `_pitch_geometry`). y is flipped so the
    south sideline (y=0) draws at the bottom of the frame and the north sideline
    (y=field_width_yards) draws at the top -- a natural top-down read.
    """
    scale = geometry["scale"]
    px = geometry["offset_x"] + (x_yards + geometry["endzone_yards"]) * scale
    py = geometry["offset_y"] + (geometry["field_width_yards"] - y_yards) * scale
    return int(round(px)), int(round(py))


def _is_on_field(x_yards: float, y_yards: float, config: "Config") -> bool:
    """True if `(x_yards, y_yards)` lies within the pitch -- including both end zones
    -- plus a `_ON_FIELD_MARGIN_YARDS` margin (D-13 convention: x=0 at the west goal
    line, x=`field_length_yards` at the east goal line, y=0 at the south sideline,
    y=`field_width_yards` at the north sideline).

    Used by `render_radar_frame` to filter its draw to on-field positions only:
    sideline/bench people are deliberately tracked (by design, not a bug) but would
    otherwise clutter the radar with markers well outside the pitch.
    """
    endzone = config.cv.endzone_yards
    field_length = config.cv.field_length_yards
    field_width = config.cv.field_width_yards
    margin = _ON_FIELD_MARGIN_YARDS

    x_min = -endzone - margin
    x_max = field_length + endzone + margin
    y_min = -margin
    y_max = field_width + margin

    return x_min <= x_yards <= x_max and y_min <= y_yards <= y_max


def _draw_pitch(canvas: "np.ndarray", config: "Config", geometry: dict, cv2) -> None:
    field_length = geometry["field_length_yards"]
    field_width = geometry["field_width_yards"]
    endzone = geometry["endzone_yards"]

    west_goal_x, _ = _yards_to_px(0.0, 0.0, geometry)
    east_goal_x, _ = _yards_to_px(field_length, 0.0, geometry)
    west_back_x, _ = _yards_to_px(-endzone, 0.0, geometry)
    east_back_x, _ = _yards_to_px(field_length + endzone, 0.0, geometry)
    _, north_y = _yards_to_px(0.0, field_width, geometry)
    _, south_y = _yards_to_px(0.0, 0.0, geometry)

    # Sidelines (span the whole pitch, both end zones included).
    cv2.line(canvas, (west_back_x, north_y), (east_back_x, north_y), _LINE_COLOR, 2)
    cv2.line(canvas, (west_back_x, south_y), (east_back_x, south_y), _LINE_COLOR, 2)
    # Goal lines.
    cv2.line(canvas, (west_goal_x, north_y), (west_goal_x, south_y), _LINE_COLOR, 2)
    cv2.line(canvas, (east_goal_x, north_y), (east_goal_x, south_y), _LINE_COLOR, 2)
    # End-zone back lines.
    cv2.line(canvas, (west_back_x, north_y), (west_back_x, south_y), _LINE_COLOR, 2)
    cv2.line(canvas, (east_back_x, north_y), (east_back_x, south_y), _LINE_COLOR, 2)

    # Yard lines every 5 yards (from the west goal line), with their numbers.
    yard = _YARD_LINE_EVERY
    while yard < field_length:
        x_px, _ = _yards_to_px(float(yard), 0.0, geometry)
        cv2.line(canvas, (x_px, north_y), (x_px, south_y), _LINE_COLOR, 1)
        cv2.putText(
            canvas,
            str(yard),
            (x_px - 8, south_y + 14),
            _FONT,
            _FONT_SCALE,
            _LINE_COLOR,
            _TEXT_THICKNESS,
            cv2.LINE_AA,
        )
        yard += _YARD_LINE_EVERY


def _marker_color(row: dict) -> tuple[int, int, int]:
    if row.get("class_name") == "referee":
        return _REFEREE_COLOR
    team_id = row.get("team_id")
    if team_id is None:
        return _NULL_TEAM_COLOR
    return _TEAM_COLORS[int(team_id) % 2]


def _draw_marker_shape(canvas: "np.ndarray", row: dict, x_px: int, y_px: int, cv2) -> None:
    """Draw `row`'s marker SHAPE only (no track-id label) at `(x_px, y_px)`: an
    upward-pointing triangle for `class_name == "referee"`, a filled square for a
    null `team_id`, otherwise a filled circle.

    Split out from label drawing (`_draw_marker_label`) so `render_radar_frame` can
    draw every marker's shape in one pass and every marker's label in a second, later
    pass -- see that function's docstring for why (a dense cluster of markers used to
    let a later-drawn marker's filled shape silently paint over an earlier marker's
    already-drawn label).
    """
    color = _marker_color(row)
    if row.get("class_name") == "referee":
        # Upward-pointing triangle -- visually distinct from the player dot/square.
        points = np.array(
            [
                [x_px, y_px - _MARKER_HALF_SIZE],
                [x_px - _MARKER_HALF_SIZE, y_px + _MARKER_HALF_SIZE],
                [x_px + _MARKER_HALF_SIZE, y_px + _MARKER_HALF_SIZE],
            ],
            dtype=np.int32,
        )
        cv2.fillPoly(canvas, [points], color)
    elif row.get("team_id") is None:
        # Filled square -- a null-team track, distinct from both the referee triangle
        # and the two team dots.
        cv2.rectangle(
            canvas,
            (x_px - _MARKER_HALF_SIZE, y_px - _MARKER_HALF_SIZE),
            (x_px + _MARKER_HALF_SIZE, y_px + _MARKER_HALF_SIZE),
            color,
            -1,
        )
    else:
        cv2.circle(canvas, (x_px, y_px), _MARKER_RADIUS, color, -1)


def _draw_marker_label(canvas: "np.ndarray", row: dict, x_px: int, y_px: int, cv2) -> None:
    """Draw `row`'s `track_id` label only, at its fixed offset from `(x_px, y_px)` --
    the counterpart to `_draw_marker_shape`, always called in a later pass over the
    whole frame so no marker's shape can ever paint over another marker's label.
    """
    color = _marker_color(row)
    cv2.putText(
        canvas,
        str(row["track_id"]),
        (x_px + _MARKER_HALF_SIZE + 2, y_px - _MARKER_HALF_SIZE),
        _FONT,
        _FONT_SCALE,
        color,
        _TEXT_THICKNESS,
        cv2.LINE_AA,
    )


def render_radar_frame(tracks_at_frame: "pl.DataFrame", config: "Config", size_wh: tuple[int, int]) -> np.ndarray:
    """Render one frame's field-yard track positions as a top-down radar image of
    size `size_wh`.

    Draws the pitch (both goal lines, the sidelines, the end-zone back lines, and yard
    lines every 5 yards with their numbers) from `config.cv.field_length_yards`/
    `field_width_yards`/`endzone_yards`, then plots each row in `tracks_at_frame` at its
    `(x_yards, y_yards)` position with `cv.palette`'s shared team colour scheme, the
    `track_id` next to it, and a distinct marker shape for referees (triangle) and for
    null-`team_id` tracks (square). Rows with a null `x_yards`/`y_yards` are skipped
    without raising.

    ON-FIELD FILTER: rows whose `(x_yards, y_yards)` falls outside the pitch (incl.
    both end zones) plus a 1-yard margin are also skipped -- see `_is_on_field`.
    Sideline/bench people are deliberately tracked elsewhere in the pipeline; this
    filter only keeps them off the radar's draw, not out of the tracking data itself.

    Draws every row's marker SHAPE first, then every row's track-id LABEL in a
    second, later pass -- not shape-then-label-then-next-row. With real footage,
    players cluster closely enough that a later row's filled marker shape can sit
    right on top of an earlier row's already-drawn label; drawing every label only
    after every shape guarantees no marker can ever paint over another marker's
    number, regardless of which row happens to come first in `tracks_at_frame`.
    """
    import cv2

    width_px, height_px = size_wh
    canvas = np.full((height_px, width_px, 3), _PITCH_COLOR, dtype=np.uint8)

    geometry = _pitch_geometry(config, size_wh)
    _draw_pitch(canvas, config, geometry, cv2)

    positioned_rows: list[tuple[dict, int, int]] = []
    for row in tracks_at_frame.iter_rows(named=True):
        if row.get("x_yards") is None or row.get("y_yards") is None:
            continue
        if not _is_on_field(row["x_yards"], row["y_yards"], config):
            continue
        x_px, y_px = _yards_to_px(row["x_yards"], row["y_yards"], geometry)
        positioned_rows.append((row, x_px, y_px))

    for row, x_px, y_px in positioned_rows:
        _draw_marker_shape(canvas, row, x_px, y_px, cv2)
    for row, x_px, y_px in positioned_rows:
        _draw_marker_label(canvas, row, x_px, y_px, cv2)

    return canvas


def render_showcase_reel(
    config: "Config", clip_numbers: list[int], tracks: "pl.DataFrame", out_path: Path
) -> Path:
    """Render a showcase reel over `clip_numbers`, writing the composed video to
    `out_path`.

    For each requested clip (in the given order, one "play" of the reel), composes per
    frame a side-by-side canvas with `overlay.draw_frame`'s annotated video frame on
    the left and `render_radar_frame` for the same `frame_index` on the right, both at
    the source clip's own height, under a header strip carrying the clip number, the
    play index within the reel and a one-line caption. Writes one continuous mp4 at the
    source fps, with `_SEPARATOR_FRAMES` black frames between plays, and returns
    `out_path`.

    Raises `NoFieldCoordinatesForClip` naming a requested clip that has no rows with
    field coordinates -- rendering its radar half would silently show an empty pitch
    (T-2.1-40). Rows with a null `x_yards`/`y_yards` that a rendered clip does carry are
    skipped by `render_radar_frame`; this function counts them across the whole reel
    and reports the total via a `logging.warning` call, since a silently thinner radar
    would misrepresent the tracking to the reel's audience.
    """
    import cv2
    import polars as pl

    from flag_football_ep.cv import frames as frames_module
    from flag_football_ep.cv.detect import MissingClipError
    from flag_football_ep.cv.overlay import draw_frame

    if not clip_numbers:
        raise ValueError("render_showcase_reel requires at least one clip number")

    session_id = config.cv.pilot_session_id
    paths_by_number = {
        frames_module.clip_number(path): path
        for path in frames_module.clip_paths(config, session_id)
    }

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = None
    half_size: tuple[int, int] | None = None
    canvas_wh: tuple[int, int] | None = None
    total_null_skipped = 0

    try:
        for play_index, clip_number in enumerate(clip_numbers, start=1):
            clip_tracks = tracks.filter(pl.col("clip_number") == clip_number)
            if clip_tracks.filter(pl.col("x_yards").is_not_null()).height == 0:
                raise NoFieldCoordinatesForClip(
                    f"clip {clip_number} has no rows with field coordinates -- cannot "
                    "render its radar half of the showcase reel"
                )
            total_null_skipped += clip_tracks.filter(pl.col("x_yards").is_null()).height

            clip_path = paths_by_number.get(clip_number)
            if clip_path is None:
                raise MissingClipError(
                    f"clip {clip_number} not registered for session {session_id!r}"
                )

            capture = cv2.VideoCapture(str(clip_path))
            if not capture.isOpened():
                capture.release()
                raise MissingClipError(f"could not open clip for decoding: {clip_path}")

            fps = capture.get(cv2.CAP_PROP_FPS)
            fps = fps if fps and fps > 0 else 30.0
            src_width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
            src_height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))

            if half_size is None:
                half_size = (src_width, src_height)
                canvas_wh = (half_size[0] * 2, half_size[1] + _HEADER_HEIGHT_PX)
                writer = cv2.VideoWriter(str(out_path), fourcc, fps, canvas_wh)

            caption = (
                f"clip {clip_number} -- play {play_index}/{len(clip_numbers)} -- "
                "tracked footage (left) / radar (right)"
            )

            try:
                frame_index = 0
                while True:
                    ok, frame = capture.read()
                    if not ok:
                        break

                    frame_tracks = clip_tracks.filter(pl.col("frame_index") == frame_index)
                    frame_rows = frame_tracks.to_dicts()

                    left = draw_frame(
                        frame, frame_rows, config, clip_number=clip_number, frame_index=frame_index
                    )
                    right = render_radar_frame(frame_tracks, config, half_size)

                    header = np.full(
                        (_HEADER_HEIGHT_PX, canvas_wh[0], 3), _HEADER_COLOR, dtype=np.uint8
                    )
                    cv2.putText(
                        header,
                        caption,
                        (8, _HEADER_HEIGHT_PX - 12),
                        _FONT,
                        0.6,
                        _HEADER_TEXT_COLOR,
                        1,
                        cv2.LINE_AA,
                    )

                    combined = np.hstack([left, right])
                    canvas = np.vstack([header, combined])
                    writer.write(canvas)
                    frame_index += 1
            finally:
                capture.release()

            if play_index < len(clip_numbers):
                separator = np.zeros((canvas_wh[1], canvas_wh[0], 3), dtype=np.uint8)
                for _ in range(_SEPARATOR_FRAMES):
                    writer.write(separator)
    finally:
        if writer is not None:
            writer.release()

    if total_null_skipped:
        logger.warning(
            "showcase reel %s: skipped %d row(s) with null field coordinates across %d clip(s)",
            out_path,
            total_null_skipped,
            len(clip_numbers),
        )

    return out_path
