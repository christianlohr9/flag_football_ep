"""RF-DETR player/referee detector: training and per-frame inference.

Owns the CRUD/request-response split the codebase already uses for EP/WP
(`model/train.py` + `model/score.py`): `train_detector` fine-tunes RF-DETR
(Apache-2.0, C-06 -- no AGPL Ultralytics/YOLO anywhere in this stack) on a validated
COCO dataset and, when `register=True`, registers the checkpoint via
`cv.registry.register_detector_model`; `load_detector`/`detect_video` resolve a trained
checkpoint (by `run_id`, defaulting to the `champion` alias exactly like
`model.score.resolve_run`) and run per-frame detection over a clip, optionally with
SAHI tiled-slicing inference for small/oblique domains (C-05).

Every MLflow-touching function in this module (`train_detector` when `register=True`,
`load_detector`) reuses the single shared tracking-store configuration in
`flag_football_ep.model.mlflow_store` -- it never points the ambient MLflow tracking
URI at an ad hoc local store, for the same reason documented in `model/registry.py`
(the plain file-backed store cannot back the model registry).

A clip/frame with zero detections still produces an (empty) `DetectionBatch`, never a
dropped clip -- matching `model.score`'s row-preserving, null-on-missing convention, and
required by D-09 (`the whole game is the denominator` for the pilot gate's coverage rate).

`train_detector` is implemented by plan 02.1-10 (remote-training-capable via
`--no-register`/`--from-artifacts`, C-10). `load_detector`/`detect_video` and the
inference-runtime benchmark harness are implemented by plan 02.1-11.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from collections.abc import Iterator

    import numpy as np

    from flag_football_ep.config import Config


class WeightsNotFound(CvError, ValueError):
    """Raised when a detector checkpoint cannot be resolved (a bad `run_id`, an
    unregistered/unpromoted model, or a missing local artifact path).
    """


class MissingClipError(CvError, ValueError):
    """Raised when a clip path passed to `detect_video` does not exist."""


@dataclass
class DetectorTrainResult:
    """The result of one `train_detector` call: the MLflow run id, the checkpoint
    path on disk, and the training metrics logged for that run.
    """

    run_id: str
    checkpoint: Path
    metrics: dict[str, float]


@dataclass
class DetectionBatch:
    """One frame's detections: bounding boxes (`xyxy`), per-box confidence, and
    per-box class id (indexing into `dataset.CLASS_NAMES`).
    """

    frame_index: int
    xyxy: np.ndarray
    confidence: np.ndarray
    class_id: np.ndarray


def train_detector(
    config: Config,
    dataset_dir: Path,
    *,
    epochs: int | None = None,
    batch_size: int | None = None,
    grad_accum: int | None = None,
    resolution: int | None = None,
    device: str | None = None,
    output_dir: Path | None = None,
    register: bool = True,
    from_artifacts: Path | None = None,
) -> DetectorTrainResult:
    """Fine-tune RF-DETR on the validated COCO dataset at `dataset_dir`.

    `None`-defaulting keyword overrides fall back to `config.cv.train_*`/`device`.
    `register=False` produces a checkpoint+metrics directory without touching MLflow
    (the remote-training-machine mode, C-10/D-05); `from_artifacts` registers a
    checkpoint+metrics directory produced by an earlier `register=False` run on another
    machine, without retraining.
    """
    raise NotImplementedError("cv.detect.train_detector is implemented by plan 02.1-10")


def load_detector(config: Config, run_id: str | None = None):
    """Load a trained RF-DETR checkpoint. `run_id=None` resolves the `champion` alias
    (`cv.registry.resolve_champion`) instead of the newest FINISHED run.
    """
    raise NotImplementedError("cv.detect.load_detector is implemented by plan 02.1-11")


def detect_video(
    config: Config, clip: Path, model, *, resolution: int, sahi: bool
) -> Iterator[DetectionBatch]:
    """Run per-frame detection over `clip` with a loaded detector, yielding one
    `DetectionBatch` per frame (empty, not skipped, when nothing is detected).
    `sahi=True` runs tiled-slicing inference for small/oblique domains (C-05).
    """
    raise NotImplementedError("cv.detect.detect_video is implemented by plan 02.1-11")
