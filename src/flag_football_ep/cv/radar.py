"""Top-down radar-view rendering of tracked/projected field positions.

`render_radar_frame` is the headless-render function (mirroring
`charts/pat_breakeven.py`'s render/write split -- a pure function returning pixel data,
separate from the path-writing wrapper) that draws one frame's field-yard positions as
a top-down radar image. `render_showcase_reel` composes a sequence of these frames
(alongside, or in place of, the raw footage) across `clip_numbers` into a single output
video -- the pilot's go/no-go presentation artifact.

No in-repo video-rendering precedent exists (existing charts render static PNGs via
matplotlib/Agg); this module's actual `cv2.VideoWriter`/compositing logic has no
in-repo analog and is built directly from scratch by the implementing plan.

Implemented by plan 02.1-16.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np
    import polars as pl

    from flag_football_ep.config import Config


def render_radar_frame(tracks_at_frame: pl.DataFrame, config: Config, size_wh: tuple[int, int]) -> np.ndarray:
    """Render one frame's field-yard track positions as a top-down radar image of
    size `size_wh`.
    """
    raise NotImplementedError("cv.radar.render_radar_frame is implemented by plan 02.1-16")


def render_showcase_reel(config: Config, clip_numbers: list[int], tracks: pl.DataFrame, out_path: Path) -> Path:
    """Render a showcase reel over `clip_numbers`, writing the composed video to
    `out_path`.
    """
    raise NotImplementedError(
        "cv.radar.render_showcase_reel is implemented by plan 02.1-16"
    )
