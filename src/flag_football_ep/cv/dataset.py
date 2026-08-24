"""COCO dataset validation, content hashing, and CVAT task round-trip.

Owns two responsibilities. First, the CVAT round trip (`create_cvat_task`,
`export_cvat_task`): push a pre-labeled COCO package for human review, then pull the
reviewed/corrected annotations back down as a COCO export -- implemented by plan
02.1-08, once `docs/cv-setup.md`'s `## CVAT` section (reserved by plan 02.1-01) records
the instance connection details. Second, dataset acceptance (`validate_coco`,
`dataset_hash`): validate a COCO export's structural integrity against the sample
manifest that produced it (every manifest frame present, only `CLASS_NAMES` categories
used, no degenerate boxes) and compute a reproducible content hash of the labeled
dataset -- implemented by plan 02.1-09, the dataset-buildout gate for REQ-S2-03's
1,500-3,000 verified frame target.

`CLASS_NAMES` is the fixed two-class vocabulary for the whole pilot: no ball detection
in this phase (C-12 -- small, motion-blurred; play structure comes from snap detection
+ PBP join instead).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from flag_football_ep.cv.frames import FrameSampleManifest
    from flag_football_ep.config import Config

CLASS_NAMES: tuple[str, ...] = ("player", "referee")


class DatasetError(CvError, ValueError):
    """Raised when a COCO export fails structural validation against its sample
    manifest (missing frames, an out-of-vocabulary category, or a degenerate box).
    """


@dataclass(frozen=True)
class DatasetStats:
    """Summary statistics for a validated COCO dataset: image/box counts, the
    train/val split sizes, and the reproducible `content_sha256` used to pin the
    exact labeled dataset a training run consumed.
    """

    n_images: int
    n_boxes: dict[str, int]
    split_counts: dict[str, int]
    content_sha256: str


def validate_coco(coco_dir: Path, manifest: FrameSampleManifest) -> DatasetStats:
    """Validate `coco_dir` (a CVAT COCO export) against `manifest`: every sampled
    frame must be present, every category must be in `CLASS_NAMES`, and no box may be
    degenerate. Raises `DatasetError` naming the first violation found.
    """
    raise NotImplementedError("cv.dataset.validate_coco is implemented by plan 02.1-09")


def dataset_hash(root: Path) -> str:
    """Compute a reproducible content hash of every annotation/image file under
    `root`, used to pin the exact labeled dataset a training run consumed.
    """
    raise NotImplementedError("cv.dataset.dataset_hash is implemented by plan 02.1-09")


def create_cvat_task(config: Config, coco_dir: Path, *, name: str) -> int:
    """Push the COCO package at `coco_dir` to CVAT as a new task named `name`,
    returning the created task id.
    """
    raise NotImplementedError("cv.dataset.create_cvat_task is implemented by plan 02.1-08")


def export_cvat_task(config: Config, task_id: int, out_dir: Path) -> Path:
    """Pull the reviewed annotations for `task_id` back from CVAT as a COCO export
    written under `out_dir`.
    """
    raise NotImplementedError("cv.dataset.export_cvat_task is implemented by plan 02.1-08")
