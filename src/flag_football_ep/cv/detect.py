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

`train_detector`'s real API shape was verified against the actually-installed
`rfdetr==1.9.3` (not the plan's illustrative snippet, and not `cv/registry.py`'s
earlier RESEARCH-time spike, which predates a real training-stack rewrite in this
version): `RFDETRSmall.train()`/`.evaluate()` run on a PyTorch Lightning stack
(`rfdetr.training`) and require the package's `train` extras group
(`pytorch-lightning`, `torchmetrics[detection]`, `faster-coco-eval`, `pycocotools`,
`scipy`) -- absent from a bare `pip install rfdetr`. `pyproject.toml`'s `cv` extras
group installs `rfdetr[train]`, not bare `rfdetr`, for this reason (see that file's
comment). `train()` accepts `dataset_dir`/`epochs`/`batch_size`/`grad_accum_steps`/
`resolution`/`device`/`output_dir`/`class_names` as real `TrainConfig` fields (some,
like `resolution`/`device`, are absorbed and specially handled rather than passed
through verbatim); `evaluate(split="val", ...)` returns a flat metrics dict keyed
`"val/mAP_50_95"`, `"val/mAP_50"`, `"val/mAP_75"`, `"val/mAR"` and, when
`log_per_class_metrics=True` (the `TrainConfig` default) and class names are known,
one `"val/AP/<class_name>"` entry per class actually present in the validation split
-- RF-DETR's own `COCOEvalCallback` (`rfdetr.training.callbacks.coco_eval`) computes
this AP as the standard COCO-style average over IoU 0.5:0.95 per class, not a
separate per-class AP50; only the *overall* metric is split into an AP50 and an
AP50-95 figure. This module reports exactly what the trainer exposes (overall AP50 +
AP50-95, per-class AP50-95) rather than reaching for `supervision`'s
mean-average-precision metric to manufacture a per-class AP50 the upstream library
does not compute -- the plan's "if the trainer does not expose per-class AP" fallback
does not apply here because RF-DETR *does* expose it, just at a single IoU-averaged
granularity, and duplicating COCO-eval math with a second library would risk the two
disagreeing on doubtful edge cases (empty-image handling, box matching) for no
benefit. The end-to-end checkpoint filename (`checkpoint_best_total.pth`, written
unconditionally at `on_fit_end` by `rfdetr.training.callbacks.best_model.
BestModelCallback` once at least one improving validation epoch has run) is likewise
taken from the installed package's real behaviour, not assumed.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError
from flag_football_ep.cv.dataset import CLASS_NAMES

if TYPE_CHECKING:
    from collections.abc import Iterator

    import numpy as np

    from flag_football_ep.config import Config
    from flag_football_ep.cv.frames import FrameSampleManifest


class WeightsNotFound(CvError, ValueError):
    """Raised when a detector checkpoint cannot be resolved (a bad `run_id`, an
    unregistered/unpromoted model, or a missing local artifact path).
    """


class MissingClipError(CvError, ValueError):
    """Raised when a clip path passed to `detect_video` does not exist."""


class InvalidResolution(CvError, ValueError):
    """Raised when a requested training resolution is not a multiple of
    `_RESOLUTION_DIVISOR` (224 -- the lcm of RF-DETR-Small's documented 32/56
    divisibility rules, so any multiple satisfies both). Names the offending value.
    """


@dataclass
class DetectorTrainResult:
    """The result of one `train_detector` call: the MLflow run id, the checkpoint
    path on disk, and the training metrics logged for that run.

    `run_id` is the empty string for a `register=False` (remote-training-machine)
    call, which never opens an MLflow run.
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


# RF-DETR-Small requires `resolution % (patch_size * num_windows) == 0`; 224 is the
# lcm of the two documented divisibility rules (32/56), so any multiple of 224 is
# valid under either (RESEARCH.md Anti-Patterns to Avoid).
_RESOLUTION_DIVISOR = 224

# `rfdetr.training.callbacks.best_model.BestModelCallback.on_fit_end` writes this
# filename unconditionally (once at least one validation epoch improved on the
# initial 0.0 high-water mark) -- verified against the installed rfdetr==1.9.3, not
# assumed from the plan's illustrative snippet.
_CHECKPOINT_FILENAME = "checkpoint_best_total.pth"
_METRICS_FILENAME = "metrics.json"
_PARAMS_FILENAME = "params.json"
_DATASET_LAYOUT_DIRNAME = "dataset"


def _resolve_manifest_path(config: Config) -> Path:
    """The frame-sample manifest's default location: `ffep cv sample`'s own default
    output directory (`config.paths.labels / "frames"`), where `write_manifest`
    writes `manifest.json`. `train_detector`'s contract has no manifest parameter
    (plan 02.1-02's stub signature), so the manifest that defines the clip-level
    train/val split is always resolved from this one canonical location -- never
    re-derived, matching every other manifest consumer in this pipeline.
    """
    return config.paths.labels / "frames" / "manifest.json"


def _filter_manifest_to_dataset(
    manifest: FrameSampleManifest, coco_dir: Path
) -> FrameSampleManifest:
    """Restrict `manifest` to the frames actually present in `coco_dir`'s COCO export.

    `train_detector` has no manifest parameter (plan 02.1-02's stub signature), so it
    always resolves the *full* sampling manifest (`_resolve_manifest_path`) -- but a
    corrected export can legitimately be a strict subset of that manifest: plan
    02.1-09's own labelling session stopped at 304 of 404 sampled frames on a
    documented, sanctioned quality-over-quantity call (D-04's soft time-box), leaving
    the remaining frames unreviewed rather than absent from the manifest. Passing the
    unfiltered manifest straight to `validate_coco` would reject every such export as
    "missing" frames it never claims to cover. This filters down to exactly the
    manifest frames whose file name is present in `coco_dir/instances.json`'s image
    set -- a strict superset-to-exact-match reconciliation, not a validation bypass:
    `validate_coco` still raises on a genuine mismatch (an image in `coco_dir` with no
    matching manifest frame at all is not something this filter can produce, since it
    only ever removes manifest entries, never keeps foreign COCO image names).
    """
    from flag_football_ep.cv.frames import FrameSampleManifest as _FrameSampleManifest

    annotation_path = coco_dir / "instances.json"
    data = json.loads(annotation_path.read_text(encoding="utf-8"))
    present_names = {image["file_name"] for image in data.get("images", [])}

    filtered_frames = [
        frame for frame in manifest.frames if Path(frame.image_path).name in present_names
    ]
    remaining_clips = {frame.clip_number for frame in filtered_frames}
    filtered_split = {
        clip_number: split
        for clip_number, split in manifest.split.items()
        if clip_number in remaining_clips
    }

    return _FrameSampleManifest(
        session_id=manifest.session_id,
        seed=manifest.seed,
        target=manifest.target,
        frames=filtered_frames,
        split=filtered_split,
    )


# Mirrors `cv/dataset.py`'s own `_IMAGE_SUFFIXES` -- kept as a separate constant
# rather than importing the private module attribute across module boundaries.
_IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png")


def _index_images_by_name(coco_dir: Path) -> dict[str, Path]:
    """Map every image file's basename under `coco_dir` to its actual path.

    A real CVAT COCO export (`dataset.export_cvat_task`) nests images under
    `images/default/`, not directly beside `instances.json` -- `dataset_hash`
    already tolerates this via `rglob`, but this module's dataset-layout builder
    needs the same tolerance to actually find and copy each image `validate_coco`
    accepted by `file_name` alone. First match (in sorted path order) wins on a
    duplicate basename across subdirectories (not expected for any dataset this
    pipeline produces).
    """
    index: dict[str, Path] = {}
    for path in sorted(coco_dir.rglob("*")):
        if path.is_file() and path.suffix.lower() in _IMAGE_SUFFIXES:
            index.setdefault(path.name, path)
    return index


def _prepare_dataset_layout(
    coco_dir: Path, manifest: FrameSampleManifest, output_dir: Path
) -> tuple[Path, str]:
    """Validate `coco_dir` against `manifest` and build the sibling `train/`/`valid/`
    Roboflow-COCO layout `rfdetr` expects (RESEARCH.md Standard Stack -- each split
    directory holds its images plus an `_annotations.coco.json`) under `output_dir`,
    never under `data/labels/`. The clip-level split comes straight from
    `manifest.split`/`manifest.frames[*].split` -- read, never re-derived -- so
    training and every later metric use the exact same partition. `manifest` is
    first restricted to the frames actually present in `coco_dir` (see
    `_filter_manifest_to_dataset`), so a corrected export that legitimately covers
    fewer frames than the full sampling manifest still validates. Returns
    `(dataset_dir, content_sha256)`.

    Raises `dataset.DatasetError` (via `validate_coco`) before any trainer import or
    call when `coco_dir` fails structural validation.
    """
    from flag_football_ep.cv.dataset import DatasetError, validate_coco

    manifest = _filter_manifest_to_dataset(manifest, coco_dir)
    stats = validate_coco(coco_dir, manifest)

    annotation_path = coco_dir / "instances.json"
    data = json.loads(annotation_path.read_text(encoding="utf-8"))
    categories = data["categories"]
    images = data["images"]
    annotations = data["annotations"]

    manifest_by_file_name = {Path(frame.image_path).name: frame for frame in manifest.frames}
    image_path_by_name = _index_images_by_name(coco_dir)

    split_images: dict[str, list[dict]] = {"train": [], "valid": []}
    split_image_ids: dict[str, set[int]] = {"train": set(), "valid": set()}
    for image in images:
        frame = manifest_by_file_name.get(image["file_name"])
        target_split = "valid" if frame is not None and frame.split == "val" else "train"
        split_images[target_split].append(image)
        split_image_ids[target_split].add(image["id"])

    dataset_dir = output_dir / _DATASET_LAYOUT_DIRNAME
    for split_name in ("train", "valid"):
        split_dir = dataset_dir / split_name
        split_dir.mkdir(parents=True, exist_ok=True)
        for image in split_images[split_name]:
            source = image_path_by_name.get(image["file_name"])
            if source is None:
                raise DatasetError(
                    f"image file {image['file_name']!r} listed in "
                    f"{annotation_path} was not found anywhere under {coco_dir}"
                )
            shutil.copy2(source, split_dir / image["file_name"])
        split_annotations = [
            ann for ann in annotations if ann["image_id"] in split_image_ids[split_name]
        ]
        (split_dir / "_annotations.coco.json").write_text(
            json.dumps(
                {
                    "images": split_images[split_name],
                    "annotations": split_annotations,
                    "categories": categories,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    return dataset_dir, stats.content_sha256


def _extract_metrics(raw: dict[str, float]) -> dict[str, float]:
    """Flatten `RFDETR.evaluate()`'s `"val/mAP_50_95"`/`"val/AP/player"`-shaped keys
    into MLflow-safe metric names (`"mAP_50_95"`/`"AP_player"`): strip the `val/`
    namespace prefix and replace the remaining `/` with `_` (an MLflow metric key may
    not contain `/`).
    """
    metrics: dict[str, float] = {}
    for key, value in raw.items():
        name = key.split("/", 1)[1] if "/" in key else key
        metrics[name.replace("/", "_")] = float(value)
    return metrics


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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

    Three modes, selected by keyword argument:

    **`from_artifacts=<dir>` (registration-only).** Skips training entirely: reads
    `metrics.json`/`params.json`/the checkpoint from `<dir>` and registers them.
    Raises `WeightsNotFound` naming `<dir>` and the missing file when any of the
    three is absent.

    **`register=False` (remote-training mode, the default `train_detector` runs on
    the Dell box under, D-05/C-10).** Validates the dataset, builds the train/valid
    layout, trains, evaluates on `valid`, writes `metrics.json`/`params.json`/the
    checkpoint into `output_dir`, and returns with an empty `run_id` -- no MLflow
    call is made (the Dell box has the GPU but not the primary machine's MLflow
    store).

    **`register=True` (the default, primary-machine mode).** Same training pipeline
    as above, then opens `mlflow_store.ensure_experiment(config.cv.detector_experiment,
    config)` and an `mlflow.start_run()`, logs every `params.json` entry as a param
    and every `metrics.json` entry as a metric, and calls
    `registry.register_detector_model` inside the run. Never sets the champion alias
    -- that stays a separate, reviewed step (`ffep cv promote`), exactly as promotion
    already works for the EP/WP models.
    """
    if from_artifacts is not None:
        return _register_from_artifacts(Path(from_artifacts), config)

    resolved_epochs = epochs if epochs is not None else config.cv.train_epochs
    resolved_batch_size = batch_size if batch_size is not None else config.cv.train_batch_size
    resolved_grad_accum = grad_accum if grad_accum is not None else config.cv.train_grad_accum
    resolved_resolution = resolution if resolution is not None else config.cv.resolution
    resolved_device = device if device is not None else config.cv.device

    if resolved_resolution % _RESOLUTION_DIVISOR != 0:
        raise InvalidResolution(
            f"resolution {resolved_resolution} is not a multiple of "
            f"{_RESOLUTION_DIVISOR} (RF-DETR-Small requires a resolution divisible "
            "by patch_size * num_windows for the variant; "
            f"{_RESOLUTION_DIVISOR} satisfies every documented divisibility rule)"
        )

    resolved_output_dir = (
        Path(output_dir)
        if output_dir is not None
        else config.paths.processed / "cv" / "detector_runs" / _timestamp()
    )
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    from flag_football_ep.cv.frames import read_manifest

    manifest = read_manifest(_resolve_manifest_path(config))

    prepared_dir, content_sha256 = _prepare_dataset_layout(
        Path(dataset_dir), manifest, resolved_output_dir
    )

    # Function-local imports: rfdetr/torch are `cv`-extras dependencies, never a
    # module-level import (D-07/D-08).
    import torch
    from rfdetr import RFDETRSmall

    model = RFDETRSmall(resolution=resolved_resolution)
    model.train(
        dataset_dir=str(prepared_dir),
        epochs=resolved_epochs,
        batch_size=resolved_batch_size,
        grad_accum_steps=resolved_grad_accum,
        device=resolved_device,
        output_dir=str(resolved_output_dir),
        class_names=list(CLASS_NAMES),
        # This project's own MLflow store is the primary artifact record (per-run
        # params/metrics logged explicitly below); none of RF-DETR's built-in
        # loggers are needed and each pulls in its own optional dependency.
        tensorboard=False,
        wandb=False,
        mlflow=False,
        clearml=False,
    )

    raw_metrics = model.evaluate(
        split="val",
        dataset_dir=str(prepared_dir),
        resolution=resolved_resolution,
        device=resolved_device,
        batch_size=resolved_batch_size,
    )
    metrics = _extract_metrics(raw_metrics)

    checkpoint = resolved_output_dir / _CHECKPOINT_FILENAME
    if not checkpoint.exists():
        raise WeightsNotFound(
            f"expected checkpoint not found after training: {checkpoint}"
        )

    params = {
        "epochs": resolved_epochs,
        "batch_size": resolved_batch_size,
        "grad_accum_steps": resolved_grad_accum,
        "resolution": resolved_resolution,
        "device": resolved_device,
        "dataset_content_sha256": content_sha256,
        "machine": _machine_identifier(),
        "torch_version": torch.__version__,
        "cuda_available": torch.cuda.is_available(),
        "cuda_version": torch.version.cuda or "",
    }

    (resolved_output_dir / _METRICS_FILENAME).write_text(
        json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8"
    )
    (resolved_output_dir / _PARAMS_FILENAME).write_text(
        json.dumps(params, indent=2, sort_keys=True), encoding="utf-8"
    )

    if not register:
        return DetectorTrainResult(run_id="", checkpoint=checkpoint, metrics=metrics)

    return _register(checkpoint, metrics, params, config)


def _machine_identifier() -> str:
    """A human-readable machine identifier for `params.json`'s provenance record --
    never a secret, just `platform.node()` (the hostname), so a training record in
    `docs/cv-setup.md` can name which physical machine (Dell / Colab / primary) a
    run came from.
    """
    import platform

    return platform.node()


def _register(
    checkpoint: Path, metrics: dict[str, float], params: dict, config: Config
) -> DetectorTrainResult:
    """Log `params`/`metrics` and register `checkpoint` inside one new MLflow run.
    Never sets the champion alias -- promotion is `ffep cv promote`'s job.
    """
    import mlflow

    from flag_football_ep.cv import registry
    from flag_football_ep.model import mlflow_store

    mlflow_store.ensure_experiment(config.cv.detector_experiment, config)
    with mlflow.start_run() as run:
        mlflow.log_params(params)
        for metric_name, value in metrics.items():
            mlflow.log_metric(metric_name, value)
        registry.register_detector_model(
            checkpoint, registry.detector_model_name(config), config
        )
        return DetectorTrainResult(run_id=run.info.run_id, checkpoint=checkpoint, metrics=metrics)


def _register_from_artifacts(artifacts_dir: Path, config: Config) -> DetectorTrainResult:
    """Registration-only mode: read a checkpoint+metrics+params directory produced by
    an earlier `register=False` run (on the Dell box or Colab) and register it,
    without calling the trainer.
    """
    checkpoint = artifacts_dir / _CHECKPOINT_FILENAME
    metrics_path = artifacts_dir / _METRICS_FILENAME
    params_path = artifacts_dir / _PARAMS_FILENAME

    for path, label in (
        (checkpoint, _CHECKPOINT_FILENAME),
        (metrics_path, _METRICS_FILENAME),
        (params_path, _PARAMS_FILENAME),
    ):
        if not path.exists():
            raise WeightsNotFound(
                f"artifacts directory {artifacts_dir} is missing {label} -- expected {path}"
            )

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    params = json.loads(params_path.read_text(encoding="utf-8"))

    return _register(checkpoint, metrics, params, config)


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
