"""Zero-shot pre-labeling of sampled training frames via Grounding DINO.

Owns the bootstrap-labeling step between `frames.sample_training_frames` and the
CVAT round trip (`dataset.create_cvat_task`): runs the zero-shot open-vocabulary
detector (`transformers.GroundingDinoForObjectDetection`, `IDEA-Research/grounding-dino-tiny`
-- the binding backend decision recorded in `docs/cv-setup.md` and plan 02.1-01-SUMMARY.md;
`autodistill`/`autodistill-grounding-dino` are not used, their `CaptionOntology` is
unimportable due to an undeclared `roboflow` dependency) over every sampled frame in
`frames_dir` and writes a COCO-format pre-annotation package to `out_dir`, one box per
detected player/referee. These pre-annotations are a labeling *head start*, not ground
truth -- every one of them is reviewed and corrected in CVAT before `dataset.validate_coco`
ever sees the export.

`--force`/`force=False` mirrors `fetch.sportapp`'s cache-bypass convention: a frame
already pre-labeled on disk is skipped unless `force=True`.

Implemented by plan 02.1-07.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from flag_football_ep.config import Config


class PrelabelBackendUnavailable(CvError, RuntimeError):
    """Raised when the zero-shot pre-labeling backend cannot be loaded (e.g. the `cv`
    extras group is not installed, or the Grounding DINO weights cannot be fetched).
    """


@dataclass
class PrelabelResult:
    """The pre-labeling run's output: the COCO package path, and counts (frames
    processed, boxes produced) plus any per-frame notices (e.g. zero detections).
    """

    coco_path: Path
    n_frames: int
    n_boxes: int
    notices: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Detection:
    """One zero-shot detection: the mapped `cv.dataset.CLASS_NAMES` category, the
    backend's confidence score, and the box in `(x0, y0, x1, y1)` pixel form (converted
    to COCO `[x, y, w, h]` only when written to the output file).
    """

    category: str
    score: float
    bbox_xyxy: tuple[float, float, float, float]


# Zero-shot confidence/text thresholds applied to every backend -- recorded verbatim in
# `PrelabelResult.notices` so a reviewer knows what produced the boxes they are
# correcting.
_CONFIDENCE_THRESHOLD = 0.3
_TEXT_THRESHOLD = 0.25

# The open-vocabulary prompt/ontology every backend below is built from: `person` maps
# to the `player` class (`cv.dataset.CLASS_NAMES`), `referee` maps to itself. Kept as
# one module-level source of truth so the primary and fallback backends never drift.
_PROMPT_TO_CLASS = {"person": "player", "referee": "referee"}

# HuggingFace `transformers` fallback checkpoint -- the binding backend decision
# recorded in docs/cv-setup.md and plan 02.1-01-SUMMARY.md (autodistill's
# `CaptionOntology` is unimportable: an undeclared `roboflow` dependency breaks it on
# import, so `autodistill`/`autodistill-grounding-dino` are not in the `cv` extras
# group at all -- the primary path below always raises `ImportError` in this project's
# environment, and the fallback is what actually runs).
_TRANSFORMERS_CHECKPOINT = "IDEA-Research/grounding-dino-tiny"


def _map_label(text: str) -> str | None:
    """Map a backend's free-text phrase-grounding output back to a `CLASS_NAMES`
    category. Zero-shot phrase grounding does not always return the exact prompt
    substring verbatim (boundary/tokenization noise), so this matches by substring
    rather than exact equality -- `None` when neither `person` nor `referee` appears,
    which the caller treats as "not one of our classes" and drops.
    """
    normalized = text.strip().lower()
    if "referee" in normalized:
        return "referee"
    if "person" in normalized:
        return "player"
    return None


def _load_autodistill_backend(device: str) -> Callable[[Path], list[Detection]]:
    """Primary backend per this plan's `<action>` block: `autodistill_grounding_dino`
    with a `CaptionOntology`. Raises `ImportError` (caught by `_resolve_backend`) when
    `autodistill`/`autodistill-grounding-dino` are not installed -- which is always the
    case in this project's `cv` extras group (see the module-level comment above); kept
    as the first-attempted backend so re-adding the package later needs no code change.
    """
    from autodistill.detection import CaptionOntology
    from autodistill_grounding_dino import GroundingDINO

    ontology = CaptionOntology(dict(_PROMPT_TO_CLASS))
    model = GroundingDINO(ontology=ontology)
    class_names = ontology.classes()

    def detect(image_path: Path) -> list[Detection]:
        result = model.predict(str(image_path))
        detections: list[Detection] = []
        for xyxy, confidence, class_id in zip(result.xyxy, result.confidence, result.class_id):
            if class_id is None or not (0 <= int(class_id) < len(class_names)):
                continue
            x0, y0, x1, y1 = (float(v) for v in xyxy)
            detections.append(
                Detection(
                    category=class_names[int(class_id)],
                    score=float(confidence),
                    bbox_xyxy=(x0, y0, x1, y1),
                )
            )
        return detections

    return detect


def _load_transformers_backend(device: str) -> Callable[[Path], list[Detection]]:
    """Fallback backend per this plan's `<action>` block and the binding decision in
    docs/cv-setup.md: `transformers.GroundingDinoForObjectDetection` with checkpoint
    `IDEA-Research/grounding-dino-tiny` and the text prompt `"person. referee."` -- pure
    PyTorch, no custom CUDA op, unlike the primary backend's underlying implementation
    (RESEARCH.md "Alternatives Considered").
    """
    import torch
    from transformers import AutoProcessor, GroundingDinoForObjectDetection

    processor = AutoProcessor.from_pretrained(_TRANSFORMERS_CHECKPOINT)
    model = GroundingDinoForObjectDetection.from_pretrained(_TRANSFORMERS_CHECKPOINT)
    model.to(device)
    model.eval()

    text_prompt = ". ".join(_PROMPT_TO_CLASS) + "."

    def detect(image_path: Path) -> list[Detection]:
        from PIL import Image

        image = Image.open(image_path).convert("RGB")
        inputs = processor(images=image, text=text_prompt, return_tensors="pt").to(device)
        with torch.no_grad():
            outputs = model(**inputs)
        results = processor.post_process_grounded_object_detection(
            outputs,
            input_ids=inputs["input_ids"],
            threshold=_CONFIDENCE_THRESHOLD,
            text_threshold=_TEXT_THRESHOLD,
            target_sizes=[image.size[::-1]],
        )[0]

        detections: list[Detection] = []
        for label, score, box in zip(
            results["text_labels"], results["scores"].tolist(), results["boxes"].tolist()
        ):
            mapped = _map_label(label)
            if mapped is None:
                continue
            x0, y0, x1, y1 = box
            detections.append(
                Detection(category=mapped, score=float(score), bbox_xyxy=(x0, y0, x1, y1))
            )
        return detections

    return detect


# Explicit backend selection (this plan's `<action>` block): a name -> loader mapping
# and an attempt order, tried in sequence until one imports cleanly. Tests monkeypatch
# both module attributes with a fake loader so no weights are downloaded and no network
# is touched.
_BACKENDS: dict[str, Callable[[str], Callable[[Path], list[Detection]]]] = {
    "autodistill": _load_autodistill_backend,
    "transformers": _load_transformers_backend,
}
_BACKEND_ORDER: tuple[str, ...] = ("autodistill", "transformers")


def _resolve_device(config: Config) -> str:
    """CPU by default (RESEARCH Pitfall 3: a few hundred frames on CPU is minutes, not
    a hot loop -- not worth chasing acceleration here). When `config.cv.device` is
    `"mps"`, sets `PYTORCH_ENABLE_MPS_FALLBACK=1` before any backend below imports
    torch: Grounding DINO's Swin backbone uses `torch.roll`, which MPS does not
    implement without the fallback flag.
    """
    device = config.cv.device or "cpu"
    if device == "mps":
        os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")
    return device


def _resolve_backend(device: str) -> tuple[str, Callable[[Path], list[Detection]]]:
    """Try every backend in `_BACKEND_ORDER`, returning the first that imports
    cleanly. Raises `PrelabelBackendUnavailable` naming every attempted backend, its
    underlying import error, and the fallback install command, when none import --
    never returns an empty COCO file as if pre-labeling had succeeded.
    """
    errors: list[str] = []
    for name in _BACKEND_ORDER:
        loader = _BACKENDS[name]
        try:
            detect_fn = loader(device)
        except ImportError as exc:
            errors.append(f"{name} ({exc})")
            continue
        return name, detect_fn

    raise PrelabelBackendUnavailable(
        "no zero-shot pre-labeling backend is importable -- attempted "
        f"{', '.join(_BACKEND_ORDER)}: {'; '.join(errors)}. As a fallback, install the "
        "`cv` extras group (`uv sync --extra cv`), which provides the "
        f"transformers-based {_TRANSFORMERS_CHECKPOINT} backend this project binds to "
        "(see docs/cv-setup.md)."
    )


def prelabel_frames(
    config: Config, frames_dir: Path, out_dir: Path, *, force: bool = False
) -> PrelabelResult:
    """Run zero-shot pre-labeling over every frame in `frames_dir`, writing a COCO
    package to `out_dir`. Frames already pre-labeled on disk are skipped unless `force`.

    Loads the sample manifest belonging to `frames_dir` (`frames_dir / "manifest.json"`,
    written by `frames.sample_training_frames`/`write_manifest`) so the COCO `images`
    list is exactly the sampled frames in manifest order -- not a directory glob, which
    would silently include leftovers from an earlier run. Every sampled frame is
    hardlinked (falling back to a copy across filesystems) into `out_dir` so the
    directory is a self-contained CVAT import package, and every frame appears in
    `images` even with zero detections -- an honest empty result, not a dropped row.
    """
    from flag_football_ep.cv.dataset import CLASS_NAMES
    from flag_football_ep.cv.frames import read_manifest

    manifest_path = frames_dir / "manifest.json"
    manifest = read_manifest(manifest_path)

    coco_path = out_dir / "instances.json"
    if not force and coco_path.exists():
        data = json.loads(coco_path.read_text(encoding="utf-8"))
        return PrelabelResult(
            coco_path=coco_path,
            n_frames=len(data.get("images", [])),
            n_boxes=len(data.get("annotations", [])),
            notices=[
                f"skipped pre-labeling: {coco_path} already exists (pass force=True to re-run)"
            ],
        )

    device = _resolve_device(config)
    backend_name, detect = _resolve_backend(device)

    out_dir.mkdir(parents=True, exist_ok=True)

    category_ids = {name: index + 1 for index, name in enumerate(CLASS_NAMES)}
    categories = [{"id": cid, "name": name} for name, cid in category_ids.items()]

    from PIL import Image

    images: list[dict] = []
    annotations: list[dict] = []
    n_zero_detection_frames = 0
    next_annotation_id = 1
    started = time.monotonic()

    for image_id, frame in enumerate(manifest.frames, start=1):
        source_path = Path(frame.image_path)
        if not source_path.exists():
            raise PrelabelBackendUnavailable(
                f"sampled frame missing on disk, cannot pre-label: {source_path} "
                f"(re-run `ffep cv sample` for the manifest at {manifest_path})"
            )

        dest_path = out_dir / source_path.name
        if not dest_path.exists():
            try:
                os.link(source_path, dest_path)
            except OSError:
                shutil.copy2(source_path, dest_path)

        with Image.open(source_path) as im:
            width, height = im.size

        images.append({"id": image_id, "file_name": dest_path.name, "width": width, "height": height})

        detections = detect(source_path)
        if not detections:
            n_zero_detection_frames += 1

        for detection in detections:
            category_id = category_ids.get(detection.category)
            if category_id is None:
                continue
            x0, y0, x1, y1 = detection.bbox_xyxy
            w = max(0.0, x1 - x0)
            h = max(0.0, y1 - y0)
            annotations.append(
                {
                    "id": next_annotation_id,
                    "image_id": image_id,
                    "category_id": category_id,
                    "bbox": [x0, y0, w, h],
                    "area": w * h,
                    "iscrowd": 0,
                    "score": detection.score,
                }
            )
            next_annotation_id += 1

    elapsed_s = time.monotonic() - started

    coco = {"categories": categories, "images": images, "annotations": annotations}

    coco_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = coco_path.with_suffix(coco_path.suffix + ".tmp")
    try:
        tmp_path.write_text(json.dumps(coco, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp_path, coco_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    notices = [
        f"backend={backend_name} device={device} elapsed_s={elapsed_s:.1f} "
        f"confidence_threshold={_CONFIDENCE_THRESHOLD}",
        f"{n_zero_detection_frames}/{len(images)} frames had zero detections",
    ]

    return PrelabelResult(
        coco_path=coco_path, n_frames=len(images), n_boxes=len(annotations), notices=notices
    )
