"""Per-clip apparent-player-size sighting pass: a fast manual/semi-manual review of
every clip that recommends inference settings before any detector training happens.

Owns the batch classification described in `docs/material-inventory.md`'s
clip-registration procedure: for each clip, record the hover position it was captured
from and the apparent on-screen player size (`apparent_player_px_p50`/`_p10`) that
determines whether the domain needs SAHI slicing or a higher inference resolution
(C-05: drone footage is its own detection regime, small objects, oblique > top-down --
pooled settings hide domain collapse). `sight_session` writes one `ClipSighting` row per
clip to a CSV; `recommend_inference_settings` turns those rows into a concrete
`InferenceRecommendation` (resolution + SAHI on/off) consumed by `cv/detect.py`'s
`detect_video` and `cv/track.py`'s `track_session`.

Implemented by plan 02.1-03.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flag_football_ep.config import Config


@dataclass(frozen=True)
class ClipSighting:
    """One clip's sighting-pass result: hover position, apparent player size in pixels
    (median and 10th percentile, the tail that drives the resolution/SAHI decision), a
    coarse tier label, and free-text notes.
    """

    clip_number: int
    clip_path: str
    hover_position_id: str
    apparent_player_px_p50: float
    apparent_player_px_p10: float
    tier: str
    notes: str


@dataclass(frozen=True)
class InferenceRecommendation:
    """The resolution/SAHI setting recommended for a domain, with the rationale that
    produced it (named apparent-size thresholds, not a bare number).
    """

    resolution: int
    sahi: bool
    rationale: str


@dataclass
class SightingResult:
    """The full sighting-pass output: every clip's `ClipSighting` row, any notices
    (e.g. a clip that could not be classified), and the CSV path the rows were written to.
    """

    rows: list[ClipSighting] = field(default_factory=list)
    notices: list[str] = field(default_factory=list)
    csv_path: Path = field(default_factory=Path)


def sight_session(config: Config, session_id: str, *, out_csv: Path | None = None) -> SightingResult:
    """Run the sighting pass over every clip registered for `session_id`, writing one
    `ClipSighting` row per clip to `out_csv` (defaulting to a config-derived path).
    """
    raise NotImplementedError("cv.sighting.sight_session is implemented by plan 02.1-03")


def recommend_inference_settings(
    rows: list[ClipSighting], config: Config
) -> InferenceRecommendation:
    """Turn a sighting pass's rows into a single resolution/SAHI recommendation for the
    domain they cover.
    """
    raise NotImplementedError(
        "cv.sighting.recommend_inference_settings is implemented by plan 02.1-03"
    )
