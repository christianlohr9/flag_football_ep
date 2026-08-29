"""Per-clip tracking overlay video rendering (boxes + track ids drawn over footage).

The visual QA counterpart to `continuity.measure_continuity`'s numeric ID-switch
metric: `render_track_overlay` draws every tracked box and its track id onto `clip`'s
frames and writes an annotated video to `out_path`, so a reviewer can visually confirm
what a `longest_track_frac`/`n_fragments` number actually looks like on screen before
the pilot gate decision is made.

Follows the repo's render/write split (`charts/pat_breakeven.py`): `draw_frame` is a
pure function returning an annotated `np.ndarray`, testable on a single frame without
producing a video file; `render_track_overlay` is the writer wrapper that decodes
`clip` with `cv2.VideoCapture` and writes through `cv2.VideoWriter`.

Output belongs under the gitignored label tree (`config.paths.labels`), never under
`reports/` -- rendered player footage is PII (T-2.1-01).

Implemented by plan 02.1-14, alongside `continuity.py`.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Sequence

import numpy as np

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    import polars as pl

    from flag_football_ep.config import Config


class NoTracksForClip(CvError, ValueError):
    """Raised when `tracks` contains no rows for the clip being rendered.

    Rendering an overlay with nothing drawn on it would look like a rendering bug
    rather than the tracking gap it actually is -- this exception makes that gap
    loud instead of silent.
    """


# BGR (cv2 convention) colours, chosen to be visually distinct from one another and
# from typical grass/turf green: two team colours, one referee colour, one
# null-team-id colour.
_TEAM_COLORS: dict[int, tuple[int, int, int]] = {
    0: (255, 80, 0),  # blue-ish
    1: (0, 60, 230),  # red-ish
}
_REFEREE_COLOR: tuple[int, int, int] = (0, 220, 255)  # yellow
_NULL_TEAM_COLOR: tuple[int, int, int] = (170, 170, 170)  # gray

_BOX_THICKNESS = 2
_DOT_RADIUS = 4
_FONT = 0  # cv2.FONT_HERSHEY_SIMPLEX, hardcoded to avoid an eager `import cv2` here
_FONT_SCALE = 0.5
_TEXT_THICKNESS = 1
_LEGEND_ENTRIES: tuple[tuple[str, tuple[int, int, int]], ...] = (
    ("team A", _TEAM_COLORS[0]),
    ("team B", _TEAM_COLORS[1]),
    ("referee", _REFEREE_COLOR),
    ("no team", _NULL_TEAM_COLOR),
)


def _row_color(row: dict) -> tuple[int, int, int]:
    if row["class_name"] == "referee":
        return _REFEREE_COLOR
    team_id = row.get("team_id")
    if team_id is None:
        return _NULL_TEAM_COLOR
    return _TEAM_COLORS[int(team_id) % 2]


def draw_frame(
    frame: "np.ndarray",
    rows: Sequence[dict],
    config: "Config",
    *,
    clip_number: int | None = None,
    frame_index: int | None = None,
) -> "np.ndarray":
    """Draw every row in `rows` (one tracked box per row, already filtered to a single
    frame) onto a copy of `frame`, returning the annotated array.

    Each row draws: its bounding box (`bbox_x1..y2`), its `track_id` as text above the
    box, and its ground-contact point (`foot_x_px`/`foot_y_px`) as a filled dot.
    Colour is chosen by `class_name`/`team_id`: two distinct colours for `team_id` 0
    and 1, a third for `class_name == "referee"`, a fourth for a null `team_id`. A
    small legend plus the clip number and frame index (when given) is burnt into the
    top-left corner so a reviewer always knows what they are looking at.

    Never mutates `frame` in place -- returns a new array, so `render_track_overlay`
    can decode into a reusable buffer without corrupting the source frame, and so this
    function stays testable on a single numpy array.
    """
    import cv2

    annotated = frame.copy()

    for row in rows:
        color = _row_color(row)
        x1, y1, x2, y2 = (
            int(round(row["bbox_x1"])),
            int(round(row["bbox_y1"])),
            int(round(row["bbox_x2"])),
            int(round(row["bbox_y2"])),
        )
        cv2.rectangle(annotated, (x1, y1), (x2, y2), color, _BOX_THICKNESS)
        cv2.putText(
            annotated,
            str(row["track_id"]),
            (x1, max(0, y1 - 5)),
            _FONT,
            _FONT_SCALE,
            color,
            _TEXT_THICKNESS,
            cv2.LINE_AA,
        )
        foot_x, foot_y = int(round(row["foot_x_px"])), int(round(row["foot_y_px"]))
        cv2.circle(annotated, (foot_x, foot_y), _DOT_RADIUS, color, -1)

    legend_y = 16
    header = f"clip {clip_number}" if clip_number is not None else "clip ?"
    header += f"  frame {frame_index}" if frame_index is not None else "  frame ?"
    cv2.putText(
        annotated, header, (6, legend_y), _FONT, _FONT_SCALE, (255, 255, 255), _TEXT_THICKNESS, cv2.LINE_AA
    )
    for label, color in _LEGEND_ENTRIES:
        legend_y += 16
        cv2.putText(
            annotated, label, (6, legend_y), _FONT, _FONT_SCALE, color, _TEXT_THICKNESS, cv2.LINE_AA
        )

    return annotated


def render_track_overlay(config: "Config", clip: Path, tracks: "pl.DataFrame", out_path: Path) -> Path:
    """Render `tracks`' boxes and track ids over `clip`'s frames, writing the
    annotated video to `out_path`.

    Opens `clip` with `cv2.VideoCapture` immediately, raising `MissingClipError`
    (from `cv.detect`) naming the path when it cannot be opened. Filters `tracks` down
    to the clip's own `clip_number` (parsed from `clip`'s filename via
    `frames.clip_number`), raising `NoTracksForClip` naming it when that filter yields
    zero rows -- an overlay with nothing on it would look like a rendering bug, not
    the tracking gap it actually is. Writes at the source clip's fps to `out_path`
    (parent directories created as needed) via `cv2.VideoWriter`, and returns
    `out_path`.
    """
    import cv2
    import polars as pl

    from flag_football_ep.cv import frames as frames_module
    from flag_football_ep.cv.detect import MissingClipError

    clip = Path(clip)
    clip_num = frames_module.clip_number(clip)

    capture = cv2.VideoCapture(str(clip))
    if not capture.isOpened():
        capture.release()
        raise MissingClipError(f"could not open clip for decoding: {clip}")

    clip_tracks = tracks.filter(pl.col("clip_number") == clip_num)
    if clip_tracks.height == 0:
        capture.release()
        raise NoTracksForClip(
            f"no tracks found for clip_number={clip_num} while rendering overlay for {clip}"
        )

    rows_by_frame: dict[int, list[dict]] = {}
    for row in clip_tracks.iter_rows(named=True):
        rows_by_frame.setdefault(int(row["frame_index"]), []).append(row)

    fps = capture.get(cv2.CAP_PROP_FPS)
    fps = fps if fps and fps > 0 else 30.0
    width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(out_path), fourcc, fps, (width, height))

    try:
        frame_index = 0
        while True:
            ok, frame = capture.read()
            if not ok:
                break
            frame_rows = rows_by_frame.get(frame_index, [])
            annotated = draw_frame(
                frame, frame_rows, config, clip_number=clip_num, frame_index=frame_index
            )
            writer.write(annotated)
            frame_index += 1
    finally:
        capture.release()
        writer.release()

    return out_path
