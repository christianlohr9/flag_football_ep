"""Clip discovery and frame extraction/sampling for the CV tracking pilot.

Owns two data flows: (1) locating a session's per-clip video files under
`config.paths.video` (`clip_paths`/`clip_number`) and extracting frames from
one clip at explicit timestamps (`extract_frames`) -- the primitive every
later stage (sighting, sampling, prelabeling) builds on; and (2) drawing the
stratified training-frame sample (`sample_training_frames`) that becomes the
labeling set, persisted as a `FrameSampleManifest` so the exact frames,
train/val split and random seed used for labeling are reproducible and never
re-derived silently (`write_manifest`/`read_manifest`).

`clip_paths`/`clip_number`/`extract_frames`/`write_manifest`/`read_manifest`
are implemented by plan 02.1-03. `sample_training_frames` is implemented by
plan 02.1-07, once `sighting.py`'s per-clip apparent-size classification
(plan 02.1-03) exists to inform the sampling strategy.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from flag_football_ep.config import Config


class ClipNotFound(CvError, ValueError):
    """Raised when a clip number or path cannot be resolved under `config.paths.video`."""


@dataclass(frozen=True)
class FrameSample:
    """One sampled training frame: which clip it came from, at which index/timestamp,
    and where the extracted image was written.
    """

    clip_number: int
    clip_path: str
    frame_index: int
    timestamp_s: float
    image_path: str


@dataclass(frozen=True)
class FrameSampleManifest:
    """The full record of a `sample_training_frames` run: every sampled frame, its
    train/val split assignment (`split`, keyed by `clip_number`), and the seed used --
    the labeling set this manifest describes is reproducible from these three fields alone.
    """

    frames: list[FrameSample]
    split: dict[int, str]
    seed: int


def clip_paths(config: Config, session_id: str) -> list[Path]:
    """List every clip video file registered for `session_id` under `config.paths.video`,
    in clip-number order.
    """
    raise NotImplementedError("cv.frames.clip_paths is implemented by plan 02.1-03")


def clip_number(path: Path) -> int:
    """Parse the clip number out of a clip filename, raising `ClipNotFound` when the
    filename does not follow the registered clip-naming convention.
    """
    raise NotImplementedError("cv.frames.clip_number is implemented by plan 02.1-03")


def extract_frames(clip: Path, out_dir: Path, at_seconds: list[float]) -> list[Path]:
    """Extract one still frame per timestamp in `at_seconds` from `clip` into `out_dir`."""
    raise NotImplementedError("cv.frames.extract_frames is implemented by plan 02.1-03")


def sample_training_frames(
    config: Config,
    session_id: str,
    *,
    target: int,
    seed: int,
    out_dir: Path,
) -> FrameSampleManifest:
    """Draw the stratified `target`-frame labeling sample for `session_id`, split
    train/val, extract the sampled frames into `out_dir`, and return the manifest.
    """
    raise NotImplementedError(
        "cv.frames.sample_training_frames is implemented by plan 02.1-07"
    )


def write_manifest(manifest: FrameSampleManifest, path: Path) -> Path:
    """Persist `manifest` to `path`."""
    raise NotImplementedError("cv.frames.write_manifest is implemented by plan 02.1-03")


def read_manifest(path: Path) -> FrameSampleManifest:
    """Load a `FrameSampleManifest` previously written by `write_manifest`."""
    raise NotImplementedError("cv.frames.read_manifest is implemented by plan 02.1-03")
