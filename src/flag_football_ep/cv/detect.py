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
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

from flag_football_ep.cv import CvError
from flag_football_ep.cv.dataset import CLASS_NAMES

if TYPE_CHECKING:
    from collections.abc import Iterator

    import supervision as sv

    from flag_football_ep.config import Config
    from flag_football_ep.cv.benchmark import StageTiming
    from flag_football_ep.cv.frames import FrameSampleManifest


class WeightsNotFound(CvError, ValueError):
    """Raised when a detector checkpoint cannot be resolved (a bad `run_id`, an
    unregistered/unpromoted model, or a missing local artifact path).
    """


class MissingClipError(CvError, ValueError):
    """Raised when a clip path passed to `detect_video` does not exist."""


class EvalGroundTruthMissing(CvError, ValueError):
    """Raised by `evaluate_per_domain` when a domain named in the frozen eval split
    has zero ground-truth-labeled frames available -- either no human-corrected COCO
    package exists for any session backing that domain's `frozen_eval` clips, or one
    exists but none of its frames fall on a `frozen_eval` clip number. Names the
    domain: reporting a metric over an empty ground-truth set would silently produce
    a meaningless number (C-05/D-04's pooled-acceptance-hides-collapse concern,
    applied to the eval side rather than the training side).
    """


class InvalidResolution(CvError, ValueError):
    """Raised when a requested training resolution is not a multiple of
    `_RESOLUTION_DIVISOR` (224 -- the lcm of RF-DETR-Small's documented 32/56
    divisibility rules, so any multiple satisfies both). Names the offending value.
    """


class InvalidDetectionClass(CvError, ValueError):
    """Raised when the detector emits a class id outside `dataset.CLASS_NAMES`'
    fixed two-class vocabulary. Names the offending id and the frame it came from --
    a silent out-of-vocabulary id would corrupt every downstream track/team/coordinate
    stage that assumes `class_id` always indexes `CLASS_NAMES`.
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
# filename only when at least one validation epoch improved the monitored metric
# (verified against the installed rfdetr==1.9.3 source, not assumed from the plan's
# illustrative snippet). A dataset with zero validation frames -- Phase 2.2's
# AL-iteration convention, where every merged frame carries `split: "train"` and
# evaluation happens separately against the frozen eval-clip split
# (`detect.evaluate_per_domain`), never `ffep cv train`'s internal val split -- never
# triggers an improving validation epoch, so `on_fit_end` never writes this file.
_CHECKPOINT_FILENAME = "checkpoint_best_total.pth"
# `on_fit_end` backfills this file unconditionally when EMA tracking is enabled (the
# rfdetr default) and `checkpoint_best_total.pth` was never written -- "a guaranteed
# checkpoint_best_ema.pth" per the callback's own docstring, verified against a real
# zero-validation-frame run (Phase 2.2 AL iteration 1, 2026-09-04): the log recorded
# "EMA metric never improved; saved final EMA weights as checkpoint_best_ema.pth" and
# `checkpoint_best_total.pth` was absent from `resolved_output_dir` afterward.
_CHECKPOINT_FALLBACK_FILENAME = "checkpoint_best_ema.pth"
_METRICS_FILENAME = "metrics.json"
_PARAMS_FILENAME = "params.json"
_DATASET_LAYOUT_DIRNAME = "dataset"

# RFDETRSmall.predict(threshold=0.5, ...)'s own documented default (rfdetr==1.9.3,
# verified via inspect.signature) -- detect_video filters to this floor explicitly
# rather than trusting every caller of the model to pass no override, so the floor is
# visible in this module rather than implicit in a library default nobody reads.
_MODEL_CONFIDENCE_THRESHOLD = 0.5

# IoU threshold `detect_video`'s SAHI path uses to de-duplicate the same real-world box
# detected in two overlapping tiles after each tile's boxes are shifted back into
# full-frame coordinates. Not RF-DETR/SAHI tuned (RESEARCH.md Pitfall 4's open tuning
# gap) -- this is cross-tile de-duplication, a separate, uncontroversial step from
# per-tile detection quality.
_SAHI_MERGE_IOU_THRESHOLD = 0.5

_TIMING_STAGES: tuple[str, ...] = ("decode", "detect", "postprocess")


def _resolve_manifest_path(config: Config, dataset_dir: Path) -> Path:
    """Resolve the frame-sample manifest that defines the clip-level train/val split
    for `dataset_dir`.

    Phase 2.2's growing multi-domain dataset (`data/labels/dataset/`) keeps its own
    `manifest.json` alongside `instances.json` -- the same convention `ffep cv
    dataset --manifest <coco_dir>/manifest.json` already validates against (plan
    02.2-13's merge). When `dataset_dir/manifest.json` exists, it is used directly:
    the growing dataset's manifest is the only one that actually describes which
    domain each merged image belongs to, and `_filter_manifest_to_dataset` below
    still restricts it to whatever subset of frames `dataset_dir` actually contains.

    Falls back to `ffep cv sample`'s own default output directory
    (`config.paths.labels / "frames" / "manifest.json"`) when `dataset_dir` carries
    no manifest of its own -- the original Phase 2.1 pilot contract (plan 02.1-02's
    stub signature predates the multi-domain dataset directory and never wrote a
    manifest next to its own COCO export), preserved unchanged for that flow.
    """
    dataset_local_manifest = Path(dataset_dir) / "manifest.json"
    if dataset_local_manifest.is_file():
        return dataset_local_manifest
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
    coco_dir: Path,
    manifest: FrameSampleManifest,
    output_dir: Path,
    *,
    eval_split_path: Path | None = None,
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
    call when `coco_dir` fails structural validation -- including, when
    `eval_split_path` is given, D-19's guard against a held-out frozen_eval clip
    having leaked into `manifest` (`dataset.assert_no_frozen_eval_clips`).
    """
    from flag_football_ep.cv.dataset import DatasetError, validate_coco

    manifest = _filter_manifest_to_dataset(manifest, coco_dir)
    stats = validate_coco(coco_dir, manifest, eval_split_path=eval_split_path)

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
    resume: Path | None = None,
) -> DetectorTrainResult:
    """Fine-tune RF-DETR on the validated COCO dataset at `dataset_dir`.

    `resume`, when given, is forwarded to `RFDETRSmall.train(resume=...)` as-is
    (rfdetr's own `TrainConfig.resume` field): a path to a full PyTorch Lightning
    `.ckpt` (optimizer/scheduler state included, e.g. `<output_dir>/last.ckpt`,
    written every epoch whenever `checkpoint_interval != 1`, the `TrainConfig`
    default). `epochs` stays the *total* target across the whole run -- Lightning
    restores `current_epoch` from the checkpoint and continues training up to
    `epochs`, it does not add `epochs` more on top. This is what makes a long run
    resumable across multiple bounded foreground calls (a single-machine wall-clock
    constraint, not a plan requirement) without discarding optimizer/LR-scheduler
    state between chunks.

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

    manifest = read_manifest(_resolve_manifest_path(config, Path(dataset_dir)))

    prepared_dir, content_sha256 = _prepare_dataset_layout(
        Path(dataset_dir),
        manifest,
        resolved_output_dir,
        eval_split_path=config.paths.reference / "frozen_eval_clips.csv",
    )

    # Function-local imports: rfdetr/torch are `cv`-extras dependencies, never a
    # module-level import (D-07/D-08).
    import torch
    from rfdetr import RFDETRSmall

    model = RFDETRSmall(resolution=resolved_resolution)
    train_kwargs: dict[str, object] = dict(
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
    if resume is not None:
        train_kwargs["resume"] = str(resume)
    model.train(**train_kwargs)

    raw_metrics = model.evaluate(
        split="val",
        dataset_dir=str(prepared_dir),
        resolution=resolved_resolution,
        device=resolved_device,
        batch_size=resolved_batch_size,
    )
    metrics = _extract_metrics(raw_metrics)

    checkpoint = resolved_output_dir / _CHECKPOINT_FILENAME
    checkpoint_source = "best_total"
    if not checkpoint.exists():
        fallback_checkpoint = resolved_output_dir / _CHECKPOINT_FALLBACK_FILENAME
        if not fallback_checkpoint.exists():
            raise WeightsNotFound(
                f"expected checkpoint not found after training: {checkpoint} "
                f"(fallback {fallback_checkpoint} also absent)"
            )
        checkpoint = fallback_checkpoint
        checkpoint_source = "best_ema_fallback_no_val_split"

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
        "checkpoint_source": checkpoint_source,
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

    Accepts either checkpoint filename `train_detector` itself may have written
    (`_CHECKPOINT_FILENAME` or, for a zero-validation-frame dataset,
    `_CHECKPOINT_FALLBACK_FILENAME`) -- the remote run went through the exact same
    fallback selection this module's own `train_detector` applies, so registration
    must recognize the same two names.
    """
    checkpoint = artifacts_dir / _CHECKPOINT_FILENAME
    if not checkpoint.exists():
        checkpoint = artifacts_dir / _CHECKPOINT_FALLBACK_FILENAME
    metrics_path = artifacts_dir / _METRICS_FILENAME
    params_path = artifacts_dir / _PARAMS_FILENAME

    for path, label in (
        (checkpoint, f"{_CHECKPOINT_FILENAME} or {_CHECKPOINT_FALLBACK_FILENAME}"),
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

    Accepts no filesystem path argument at all -- a weights path is not a supported
    input (T-2.1-15): every caller either names an existing MLflow run id or accepts
    the `champion` alias, never an arbitrary path this process would deserialize.
    Loads via `mlflow.pyfunc.load_model(f"runs:/{run_id}/model")` (the same
    `runs:/<id>/model` uri shape `model.score.load_model` uses for EP/WP), after
    `mlflow_store.configure(config)` -- never against an ambient tracking uri set
    elsewhere in the process (mirrors `cv.registry`'s own rule, RESEARCH.md Pitfall 5).
    Any resolution/load failure (an unregistered run, a corrupt/missing artifact) is
    wrapped in `WeightsNotFound` naming the run id and the tracking store uri, never
    left as a bare `MlflowException` for the CLI to print unexplained.
    """
    from mlflow.exceptions import MlflowException

    import mlflow.pyfunc

    from flag_football_ep.cv import registry
    from flag_football_ep.model import mlflow_store

    resolved_run_id = (
        run_id
        if run_id is not None
        else registry.resolve_champion(registry.detector_model_name(config), config)
    )

    mlflow_store.configure(config)
    try:
        return mlflow.pyfunc.load_model(f"runs:/{resolved_run_id}/model")
    except MlflowException as exc:
        raise WeightsNotFound(
            f"could not load detector weights for run {resolved_run_id!r} from "
            f"tracking store {mlflow_store.tracking_uri(config)!r}: {exc}"
        ) from exc


def _call_model(model, image: np.ndarray, *, resolution: int) -> sv.Detections:
    """Run `model.predict` on one image, requesting `resolution` via the pyfunc
    `params` channel (`RFDETRWrapper.predict` forwards `params` straight into the
    wrapped `RFDETRSmall.predict(**params)` call, so `params={"shape": (r, r)}`
    reaches `RFDETRSmall.predict(shape=(r, r))` unchanged when `params` survives to the
    wrapper). A fake test model with a `predict(image, params=None)` method satisfies
    the same contract with no MLflow or `rfdetr` involved.

    Verified against a real champion-loaded model (`ffep.toml`'s `cv_detector_model`,
    run `87a8a5222f7a472787875e974d089c44`): MLflow's outer `PyFuncModel.predict`
    silently drops `params` before they ever reach `RFDETRWrapper.predict` when the
    registered model carries no `ModelSignature` `params_schema` (a warning is logged,
    not an error) -- `register_detector_model` (`cv/registry.py`, plan 02.1-06) does not
    declare one. In that case the loaded model falls back to `RFDETRSmall.predict`'s own
    `shape=None -> (model.resolution, model.resolution)` default, which is the
    resolution the checkpoint was trained/loaded at -- for the current champion that is
    896, matching `ffep.toml`'s `[cv] resolution`, so this does not silently mis-run
    inference today. Declaring a `params_schema` at registration time (so `resolution`
    is enforceable per call, independent of what a checkpoint happened to train at) is
    a registration-time change to `cv/registry.py` outside this plan's scope, not a
    `detect_video` bug -- tracked as a follow-up, not fixed here.

    Strips `.metadata`/`.data` off the returned `Detections` before handing it back:
    RF-DETR's real `predict(..., include_source_image=True)` (the pyfunc wrapper's
    default) attaches the *entire input crop* under `metadata["source_image"]` plus a
    `data["source_shape"]` entry, verified against the real champion model -- every
    tile in `_detect_tiled` has a different source image/shape, and
    `sv.Detections.merge` raises `ValueError: Conflicting metadata for key:
    'source_image'` the moment two tiles' detections are merged (found running Task 3's
    real three-clip SAHI throughput sample). Neither field is used anywhere downstream
    of this module (`DetectionBatch` only carries `xyxy`/`confidence`/`class_id`), so
    dropping both is also a memory-hygiene win for the full-frame path, not just a
    SAHI-path bug fix.
    """
    detections = model.predict(image, params={"shape": (resolution, resolution)})
    detections.metadata = {}
    detections.data = {}
    return detections


def _confidence_filtered(detections: sv.Detections) -> sv.Detections:
    """Drop every detection below `_MODEL_CONFIDENCE_THRESHOLD` -- RF-DETR's own
    documented default, applied explicitly here rather than only trusted to already
    hold inside whatever produced `detections`.
    """
    return detections[detections.confidence >= _MODEL_CONFIDENCE_THRESHOLD]


def _empty_detections() -> sv.Detections:
    import supervision as sv

    return sv.Detections(
        xyxy=np.zeros((0, 4), dtype=np.float64),
        confidence=np.zeros((0,), dtype=np.float64),
        class_id=np.zeros((0,), dtype=np.int64),
    )


def _detect_full_frame(model, frame_rgb: np.ndarray, *, resolution: int) -> sv.Detections:
    """Full-frame inference path (`sahi=False`): one model call over the whole frame at
    `resolution`, confidence-floored.
    """
    detections = _call_model(model, frame_rgb, resolution=resolution)
    return _confidence_filtered(detections)


def _detect_tiled(
    config: Config, model, frame_rgb: np.ndarray, *, resolution: int
) -> sv.Detections:
    """SAHI tiled-slicing inference path (`sahi=True`, C-05): slice `frame_rgb` per
    `config.cv.sahi_slice`/`sahi_overlap` (RESEARCH.md Standard Stack -- `sahi` is
    model-agnostic via `supervision` loaders), run the model once per tile, shift each
    tile's boxes back into full-frame pixel coordinates by its slice offset, merge every
    tile's (confidence-floored) detections, and de-duplicate cross-tile double-counts of
    the same real-world box with `with_nms` (`_SAHI_MERGE_IOU_THRESHOLD`). Starts from
    `sahi`'s own documented slicing defaults (RESEARCH.md Pitfall 4's open RF-DETR
    tuning gap at high tile resolution is a measured, not assumed, follow-up -- Task 3
    is where that measurement happens).
    """
    import supervision as sv
    from sahi.slicing import slice_image

    slice_result = slice_image(
        frame_rgb,
        slice_height=config.cv.sahi_slice,
        slice_width=config.cv.sahi_slice,
        overlap_height_ratio=config.cv.sahi_overlap,
        overlap_width_ratio=config.cv.sahi_overlap,
    )

    tile_detections: list[sv.Detections] = []
    for tile_image, (offset_x, offset_y) in zip(
        slice_result.images, slice_result.starting_pixels
    ):
        detections = _confidence_filtered(
            _call_model(model, tile_image, resolution=resolution)
        )
        if len(detections) == 0:
            continue
        offset = np.array([offset_x, offset_y, offset_x, offset_y], dtype=np.float64)
        detections.xyxy = detections.xyxy.astype(np.float64) + offset
        tile_detections.append(detections)

    if not tile_detections:
        return _empty_detections()

    merged = sv.Detections.merge(tile_detections)
    return merged.with_nms(threshold=_SAHI_MERGE_IOU_THRESHOLD, class_agnostic=False)


def _to_detection_batch(frame_index: int, detections: sv.Detections) -> DetectionBatch:
    """Build one frame's `DetectionBatch`, validating every `class_id` indexes
    `dataset.CLASS_NAMES` -- an out-of-vocabulary id raises `InvalidDetectionClass`
    naming the id and the frame rather than silently corrupting downstream stages that
    assume `class_id` always maps to a `CLASS_NAMES` entry.
    """
    for class_id in detections.class_id:
        if not (0 <= int(class_id) < len(CLASS_NAMES)):
            raise InvalidDetectionClass(
                f"detector emitted class id {int(class_id)} outside the "
                f"{len(CLASS_NAMES)}-class vocabulary {list(CLASS_NAMES)} at frame "
                f"{frame_index}"
            )
    return DetectionBatch(
        frame_index=frame_index,
        xyxy=detections.xyxy,
        confidence=detections.confidence,
        class_id=detections.class_id,
    )


def _iter_detection_batches(
    config: Config,
    capture,
    model,
    *,
    resolution: int,
    sahi: bool,
    stage_totals: dict[str, dict[str, float]],
) -> Iterator[DetectionBatch]:
    """Decode `capture` frame by frame, run detection (full-frame or SAHI-tiled) on
    each, and yield one `DetectionBatch` per frame -- including frames with zero
    detections, never a skipped frame (D-09: the whole game is the denominator).
    Stops cleanly at end-of-stream (`capture.read()` returning `ok=False`) rather than
    retrying a failed read, and always releases `capture` on exit (including on an
    exception raised mid-stream, e.g. `InvalidDetectionClass`).
    """
    import cv2

    frame_index = 0
    try:
        while True:
            decode_start = time.perf_counter()
            ok, frame_bgr = capture.read()
            decode_elapsed = time.perf_counter() - decode_start
            if not ok:
                break
            stage_totals["decode"]["seconds"] += decode_elapsed
            stage_totals["decode"]["frames"] += 1

            frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)

            detect_start = time.perf_counter()
            if sahi:
                detections = _detect_tiled(config, model, frame_rgb, resolution=resolution)
            else:
                detections = _detect_full_frame(model, frame_rgb, resolution=resolution)
            detect_elapsed = time.perf_counter() - detect_start
            stage_totals["detect"]["seconds"] += detect_elapsed
            stage_totals["detect"]["frames"] += 1

            postprocess_start = time.perf_counter()
            batch = _to_detection_batch(frame_index, detections)
            postprocess_elapsed = time.perf_counter() - postprocess_start
            stage_totals["postprocess"]["seconds"] += postprocess_elapsed
            stage_totals["postprocess"]["frames"] += 1

            yield batch
            frame_index += 1
    finally:
        capture.release()


class DetectionRun:
    """An `Iterator[DetectionBatch]` returned by `detect_video`: iterating it decodes
    and detects one frame at a time (lazy, never loads a whole clip into memory), and
    `.timings()` -- read any time, most usefully after the iterator is exhausted --
    returns the `StageTiming` entries (`decode`/`detect`/`postprocess`) accumulated so
    far, so the C-09 inference-time gate criterion comes from instrumented code, not an
    estimate.
    """

    def __init__(
        self,
        frames: Iterator[DetectionBatch],
        stage_totals: dict[str, dict[str, float]],
    ) -> None:
        self._frames = frames
        self._stage_totals = stage_totals

    def __iter__(self) -> "DetectionRun":
        return self

    def __next__(self) -> DetectionBatch:
        return next(self._frames)

    def timings(self) -> tuple[StageTiming, ...]:
        from flag_football_ep.cv.benchmark import StageTiming

        return tuple(
            StageTiming(
                stage=stage,
                seconds=self._stage_totals[stage]["seconds"],
                frames=int(self._stage_totals[stage]["frames"]),
            )
            for stage in _TIMING_STAGES
        )


def detect_video(
    config: Config, clip: Path, model, *, resolution: int, sahi: bool
) -> Iterator[DetectionBatch]:
    """Run per-frame detection over `clip` with a loaded detector, yielding one
    `DetectionBatch` per frame (empty, not skipped, when nothing is detected).
    `sahi=True` runs tiled-slicing inference for small/oblique domains (C-05).

    Opens `clip` with `cv2.VideoCapture` immediately (not lazily on first iteration) so
    an unopenable clip raises `MissingClipError` naming the path as soon as
    `detect_video` is called, rather than on the caller's first `next()`. The returned
    `DetectionRun` accumulates per-stage timing (`decode`, `detect`, `postprocess`) as
    it is iterated, readable via `.timings()`.
    """
    import cv2

    clip = Path(clip)
    capture = cv2.VideoCapture(str(clip))
    if not capture.isOpened():
        capture.release()
        raise MissingClipError(f"could not open clip for decoding: {clip}")

    stage_totals: dict[str, dict[str, float]] = {
        stage: {"seconds": 0.0, "frames": 0} for stage in _TIMING_STAGES
    }
    frames = _iter_detection_batches(
        config, capture, model, resolution=resolution, sahi=sahi, stage_totals=stage_totals
    )
    return DetectionRun(frames, stage_totals)


_EVAL_GT_DIRNAME = "corrected"
_EVAL_GT_ROOT_DIRNAME = "eval"
_EVAL_CLIP_NUMBER_RE = re.compile(r"Clip[ _](\d+)_f\d+")


def _read_frozen_eval_session_ids(eval_split_path: Path) -> dict[str, dict[int, str]]:
    """`{domain: {clip_number: session_id}}` for every `role == "frozen_eval"` row in
    the eval-split CSV at `eval_split_path`.

    Reads the CSV directly rather than going through `frames.EvalSplit` a second
    time: `EvalSplit.clips_by_domain` (the `frames.read_eval_split` contract this
    module's `key_links` names) only carries clip numbers, not the `session_id` each
    clip belongs to -- both are needed here to locate a domain's ground-truth COCO
    package, which lives under `config.paths.labels / session_id / "corrected"`.
    """
    import polars as pl

    df = pl.read_csv(eval_split_path)
    eval_rows = df.filter(pl.col("role") == "frozen_eval")

    out: dict[str, dict[int, str]] = {}
    for row in eval_rows.iter_rows(named=True):
        out.setdefault(row["domain"], {})[int(row["clip_number"])] = row["session_id"]
    return out


def _eval_clip_number(file_name: str) -> int | None:
    """Parse the clip number out of an extracted frame's file name (e.g. `"drone__Wide
    - Clip 001_f00026.jpg"` or the pilot's unprefixed `"Wide - Clip 001_f00026.jpg"`),
    or `None` when the name does not match the project-wide `"Clip <n>_f<index>"`
    convention `frames.py`'s own frame-extraction naming already establishes.
    """
    match = _EVAL_CLIP_NUMBER_RE.search(file_name)
    return int(match.group(1)) if match else None


def _load_domain_ground_truth(
    config: Config, domain: str, clip_to_session: dict[int, str]
) -> tuple[list[dict], list[dict], list[dict], dict[str, Path]]:
    """Load and filter ground-truth COCO annotations for one domain's frozen eval
    clips.

    Two ground-truth sources exist, checked in this priority order, never merged
    together for the same domain:

    1. **`config.paths.labels / "eval" / <domain> / "corrected" / "instances.json"`**
       (the dedicated, domain-scoped held-out ground-truth convention this ad-hoc
       plan introduces, 2026-09-04): sampled exclusively from `frozen_eval` clips
       (`frames.sample_eval_gt_frames`), so every frame under it is guaranteed to be
       genuinely unseen by any detector trained on `data/labels/dataset/`. Used
       exclusively for `domain` when present -- never mixed with source 2, which can
       predate the eval-clip freeze and therefore overlap a model's own training
       data (the exact contamination the 2026-09-04 Koordinator-Korrektur in
       `docs/dataset-buildout.md` documents: 76 of the Phase-2.1 pilot's own
       corrected images sat inside the champion's own train/val split).
    2. **`config.paths.labels / <session_id> / "corrected" / "instances.json"`** for
       each distinct `session_id` in `clip_to_session` (the pre-existing, established
       convention this repo already used for the Phase-2.1 pilot's own corrected
       dataset, plan 02.1-09) -- the fallback when source 1 does not exist for
       `domain` yet. A session with no such package contributes nothing (not an
       error by itself -- a domain is only considered to have no ground truth if
       *no* source contributes any matching frame at all, checked by the caller).

    Returns `(images, annotations, categories, image_paths)`: only images whose
    parsed clip number is a `frozen_eval` clip for `domain` (per `clip_to_session`),
    their annotations, the package's category list (first package found, all
    packages share `dataset.CLASS_NAMES`), and a `file_name -> Path` index for
    locating each image's bytes on disk (mirrors `_index_images_by_name`).
    """
    images: list[dict] = []
    annotations: list[dict] = []
    categories: list[dict] = []
    image_paths: dict[str, Path] = {}

    next_image_id = 1
    next_ann_id = 1

    domain_eval_dir = config.paths.labels / _EVAL_GT_ROOT_DIRNAME / domain / _EVAL_GT_DIRNAME
    if (domain_eval_dir / "instances.json").is_file():
        # source 1: domain-scoped, guaranteed-held-out ground truth. `session_id`
        # is intentionally `None` here -- membership in `clip_to_session` (any
        # session backing this domain's frozen_eval clips) is enough; there is no
        # per-source session to additionally cross-check against, unlike source 2.
        sources: list[tuple[Path, str | None]] = [(domain_eval_dir, None)]
    else:
        sources = [
            (config.paths.labels / session_id / _EVAL_GT_DIRNAME, session_id)
            for session_id in sorted(set(clip_to_session.values()))
        ]

    for gt_dir, session_id in sources:
        annotation_path = gt_dir / "instances.json"
        if not annotation_path.is_file():
            continue

        data = json.loads(annotation_path.read_text(encoding="utf-8"))
        if not categories:
            categories = data.get("categories", [])
        session_image_paths = _index_images_by_name(gt_dir)

        old_to_new_id: dict[int, int] = {}
        for image in data.get("images", []):
            clip_num = _eval_clip_number(image["file_name"])
            if clip_num is None or clip_num not in clip_to_session:
                continue
            if session_id is not None and clip_to_session[clip_num] != session_id:
                continue
            source_path = session_image_paths.get(image["file_name"])
            if source_path is None:
                continue

            old_to_new_id[image["id"]] = next_image_id
            images.append({**image, "id": next_image_id})
            image_paths[image["file_name"]] = source_path
            next_image_id += 1

        for ann in data.get("annotations", []):
            new_image_id = old_to_new_id.get(ann["image_id"])
            if new_image_id is None:
                continue
            annotations.append({**ann, "id": next_ann_id, "image_id": new_image_id})
            next_ann_id += 1

    return images, annotations, categories, image_paths


def _write_json_atomic(path: Path, data: dict) -> None:
    """`.tmp` sibling + `os.replace`, matching `frames.write_manifest`'s discipline
    (T-2.1-10) -- never a half-written eval report on disk.
    """
    import os

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _evaluate_domain_frames(
    config: Config,
    model,
    images: list[dict],
    annotations: list[dict],
    categories: list[dict],
    image_paths: dict[str, Path],
    *,
    resolution: int,
    sahi: bool,
) -> dict:
    """Run `model` over every image in `images` and score the predictions against
    `annotations` with `torchmetrics.detection.MeanAveragePrecision` (the same
    `faster-coco-eval`/`torchmetrics` stack `train_detector`'s own
    `RFDETRSmall.evaluate()` call is built on, per this module's own docstring on
    RF-DETR's real metric shape) -- never a second, hand-rolled mAP implementation.

    Category ids are 1-indexed in COCO (`dataset.CLASS_NAMES` order: `player=1`,
    `referee=2`); `DetectionBatch.class_id` is 0-indexed into `CLASS_NAMES` directly
    (this module's own convention). Both sides are normalized to the same 0-indexed
    space before scoring.

    Mirrors RF-DETR's own reported granularity (see `train_detector`'s docstring):
    `mAP_50`/`mAP_50_95` are the *overall*, IoU-averaged-or-not pooled metrics;
    `AP_player`/`AP_referee` are per-class, IoU-averaged (0.5:0.95) only -- RF-DETR's
    trainer does not expose a separate per-class AP50, and this function does not
    manufacture one with a different library.
    """
    import cv2
    import torch
    from torchmetrics.detection.mean_ap import MeanAveragePrecision

    cat_id_to_class_idx = {c["id"]: idx for idx, c in enumerate(sorted(categories, key=lambda c: c["id"]))}

    anns_by_image_id: dict[int, list[dict]] = {}
    for ann in annotations:
        anns_by_image_id.setdefault(ann["image_id"], []).append(ann)

    metric = MeanAveragePrecision(box_format="xyxy", class_metrics=True)
    n_boxes = 0

    for image in images:
        source_path = image_paths[image["file_name"]]
        frame_bgr = cv2.imread(str(source_path))
        if frame_bgr is None:
            raise EvalGroundTruthMissing(
                f"ground-truth image {source_path} could not be decoded"
            )
        frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)

        if sahi:
            detections = _detect_tiled(config, model, frame_rgb, resolution=resolution)
        else:
            detections = _detect_full_frame(model, frame_rgb, resolution=resolution)

        pred_boxes = torch.as_tensor(detections.xyxy, dtype=torch.float32).reshape(-1, 4)
        pred_scores = torch.as_tensor(detections.confidence, dtype=torch.float32).reshape(-1)
        pred_labels = torch.as_tensor(detections.class_id, dtype=torch.int64).reshape(-1)

        gt_anns = anns_by_image_id.get(image["id"], [])
        gt_boxes_list: list[list[float]] = []
        gt_labels_list: list[int] = []
        for ann in gt_anns:
            x, y, w, h = ann["bbox"]
            gt_boxes_list.append([x, y, x + w, y + h])
            gt_labels_list.append(cat_id_to_class_idx[ann["category_id"]])
            n_boxes += 1

        gt_boxes = torch.tensor(gt_boxes_list, dtype=torch.float32).reshape(-1, 4)
        gt_labels = torch.tensor(gt_labels_list, dtype=torch.int64).reshape(-1)

        metric.update(
            [{"boxes": pred_boxes, "scores": pred_scores, "labels": pred_labels}],
            [{"boxes": gt_boxes, "labels": gt_labels}],
        )

    computed = metric.compute()

    ap_by_class_name = {name: 0.0 for name in CLASS_NAMES}
    classes_present = torch.atleast_1d(computed["classes"]).tolist()
    ap_per_class = torch.atleast_1d(computed["map_per_class"]).tolist()
    for class_idx, ap in zip(classes_present, ap_per_class):
        if 0 <= class_idx < len(CLASS_NAMES):
            ap_by_class_name[CLASS_NAMES[class_idx]] = float(ap)

    return {
        "mAP_50": float(computed["map_50"]),
        "mAP_50_95": float(computed["map"]),
        "AP_player": ap_by_class_name["player"],
        "AP_referee": ap_by_class_name["referee"],
        "n_images": len(images),
        "n_boxes": n_boxes,
    }


def evaluate_per_domain(config: Config, run_id: str, eval_split_path: Path, out_path: Path) -> dict:
    """Run the detector at `run_id` over every clip named in the frozen eval split at
    `eval_split_path` (`frames.EvalSplit`, written by `frames.freeze_eval_clips`),
    returning per-domain metrics --
    `{domain: {mAP_50, mAP_50_95, AP_player, AP_referee, n_images, n_boxes}}`, never a
    pooled number alone (C-05/D-04). Also writes the same result to `out_path`, and
    logs every metric into the MLflow run `run_id` as a `<domain>_<metric>` metric.

    Ground truth is sourced from each domain's human-corrected COCO package(s) at
    `data/labels/<session_id>/corrected/instances.json`, filtered to images whose
    parsed clip number is one of that domain's `frozen_eval` clips
    (`_load_domain_ground_truth`) -- `frozen_eval_clips.csv` itself only freezes
    *which clips* are held out, never labels them; a domain whose `frozen_eval`
    clips have no corresponding corrected package (or whose corrected package has no
    frame on a `frozen_eval` clip) raises `EvalGroundTruthMissing` naming that
    domain, rather than silently reporting a metric over an empty set (same
    C-05/D-04 discipline `dataset.validate_coco` already applies on the training
    side).

    A `"_pooled"` key is included as an additional, non-authoritative aggregate over
    every domain's boxes -- callers must never read it in place of the per-domain
    entries (C-05/D-04).
    """
    from flag_football_ep.cv.frames import read_eval_split

    split = read_eval_split(eval_split_path)
    session_by_clip = _read_frozen_eval_session_ids(eval_split_path)

    model = load_detector(config, run_id)

    resolution = config.cv.resolution
    sahi = config.cv.sahi

    results: dict[str, dict] = {}
    for domain in sorted(split.clips_by_domain):
        clip_to_session = session_by_clip.get(domain, {})
        images, annotations, categories, image_paths = _load_domain_ground_truth(
            config, domain, clip_to_session
        )
        if not images:
            raise EvalGroundTruthMissing(
                f"domain {domain!r} has zero ground-truth-labeled frames overlapping "
                f"its {len(clip_to_session)} frozen_eval clip(s) -- no corrected COCO "
                "package under data/labels/<session_id>/corrected/ covers any of them"
            )
        results[domain] = _evaluate_domain_frames(
            config, model, images, annotations, categories, image_paths,
            resolution=resolution, sahi=sahi,
        )

    total_images = sum(r["n_images"] for r in results.values())
    total_boxes = sum(r["n_boxes"] for r in results.values())
    if total_images:
        pooled_map = sum(r["mAP_50_95"] * r["n_images"] for r in results.values()) / total_images
        pooled_map_50 = sum(r["mAP_50"] * r["n_images"] for r in results.values()) / total_images
    else:
        pooled_map = 0.0
        pooled_map_50 = 0.0
    results["_pooled"] = {
        "mAP_50": pooled_map_50,
        "mAP_50_95": pooled_map,
        "n_images": total_images,
        "n_boxes": total_boxes,
    }

    _write_json_atomic(Path(out_path), results)

    import mlflow

    from flag_football_ep.model import mlflow_store

    mlflow_store.configure(config)
    with mlflow.start_run(run_id=run_id):
        for domain, metrics in results.items():
            for metric_name, value in metrics.items():
                if isinstance(value, (int, float)):
                    mlflow.log_metric(f"{domain}_{metric_name}", float(value))

    return results
