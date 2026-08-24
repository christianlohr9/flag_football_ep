"""Per-clip tracking overlay video rendering (boxes + track ids drawn over footage).

The visual QA counterpart to `continuity.measure_continuity`'s numeric ID-switch
metric: `render_track_overlay` draws every tracked box and its track id onto `clip`'s
frames and writes an annotated video to `out_path`, so a reviewer can visually confirm
what a `longest_track_frac`/`n_fragments` number actually looks like on screen before
the pilot gate decision is made.

Implemented by plan 02.1-14, alongside `continuity.py`.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

    from flag_football_ep.config import Config


def render_track_overlay(config: Config, clip: Path, tracks: pl.DataFrame, out_path: Path) -> Path:
    """Render `tracks`' boxes and track ids over `clip`'s frames, writing the
    annotated video to `out_path`.
    """
    raise NotImplementedError(
        "cv.overlay.render_track_overlay is implemented by plan 02.1-14"
    )
