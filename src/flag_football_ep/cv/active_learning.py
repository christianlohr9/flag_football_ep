"""Active-learning frame selection for iterative dataset growth toward REQ-S2-03.

No upstream active-learning library targets RF-DETR's output shape, so this module is
"build, don't import" (RESEARCH Pattern 1): an uncertainty score per frame
(`frame_uncertainty_score`, over `cv.detect`'s existing `sv.Detections` output, never a
redefinition of the detection schema) combined with a diversity key
(`diversity_key`) so `select_al_frames` draws a stratified, non-redundant next-iteration
batch -- the same "group, allocate per group, then allocate within group" two-level
pattern `frames.py::sample_training_frames` already uses for its clip/hover-position
stratification. `write_selection_manifest`/`read_selection_manifest` persist an
`ALSelection` with the same atomic-JSON round trip as `frames.py::write_manifest`/
`read_manifest`, so a later iteration can read an earlier one's selection without
re-deriving it.

Every function below raises `NotImplementedError` naming the plan that implements it
-- this module is a contract freeze only (plan 02.2-05); the real scoring, selection,
and persistence logic is implemented by plan 02.2-09.

Implemented by plan 02.2-09.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from pathlib import Path

    from flag_football_ep.config import Config


class ActiveLearningError(CvError, ValueError):
    """Raised when a selection cannot be drawn (e.g. an empty candidate pool, or a
    malformed selection-manifest file).
    """


@dataclass(frozen=True)
class ALFrame:
    """One frame selected by an active-learning iteration: which session/clip/frame
    it is, its uncertainty score, and the diversity key it was drawn under.
    """

    session_id: str
    clip_number: int
    frame_index: int
    timestamp_s: float
    image_path: str
    uncertainty_score: float
    diversity_key: tuple


@dataclass(frozen=True)
class ALSelection:
    """The full record of a `select_al_frames` run: the session pool it drew from,
    the iteration number, target count, seed, and the selected frames -- mirrors
    `frames.FrameSampleManifest`'s reproducibility contract
    (`(session_ids, iteration, target, seed)` alone reproduces the same draw).
    """

    session_ids: list[str]
    iteration: int
    target: int
    seed: int
    frames: list[ALFrame]


def frame_uncertainty_score(detections) -> float:
    """Score one frame's detections by how uncertain the current detector is about
    them (higher = more valuable to label next).

    Implemented by plan 02.2-09.
    """
    raise NotImplementedError("implemented by plan 02.2-09")


def diversity_key(row) -> tuple:
    """Return the stratification key a candidate frame is grouped by (domain, clip,
    hover position/camera, field-zone bucket), mirroring
    `frames.py::sample_training_frames`'s grouping discipline.

    Implemented by plan 02.2-09.
    """
    raise NotImplementedError("implemented by plan 02.2-09")


def select_al_frames(
    config: Config,
    session_ids: list[str],
    iteration: int,
    target: int,
    seed: int,
    out_dir: Path,
) -> ALSelection:
    """Draw the next active-learning iteration's frame batch: run the current
    detector over `session_ids`' candidate pool, score by
    `frame_uncertainty_score`, stratify by `diversity_key`, and select `target`
    frames.

    Implemented by plan 02.2-09.
    """
    raise NotImplementedError("implemented by plan 02.2-09")


def write_selection_manifest(manifest: ALSelection, path: Path) -> Path:
    """Persist `manifest` to `path`, using the same `.tmp` + `os.replace` atomic
    write discipline as `frames.py::write_manifest`.

    Implemented by plan 02.2-09.
    """
    raise NotImplementedError("implemented by plan 02.2-09")


def read_selection_manifest(path: Path) -> ALSelection:
    """Load an `ALSelection` previously written by `write_selection_manifest`.

    Implemented by plan 02.2-09.
    """
    raise NotImplementedError("implemented by plan 02.2-09")
