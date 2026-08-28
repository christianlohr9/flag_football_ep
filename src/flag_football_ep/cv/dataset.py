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

import hashlib
import json
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from cvat_sdk.core.client import Client

    from flag_football_ep.cv.frames import FrameSampleManifest
    from flag_football_ep.config import Config

CLASS_NAMES: tuple[str, ...] = ("player", "referee")

# REQ-S2-02's ~300-500 training-frame target, widened to a hard [250, 600] acceptance
# band: below 250 the corrected set is too thin to trust a fine-tune on; above 600 is a
# D-06 violation -- the pilot answers a gate miss by going back to Phase 2.0 capture
# setup, never by quietly labeling more training frames. Evaluation labeling
# (plan 02.1-15, ground-truth positions) is a separate, unrelated budget.
_MIN_IMAGES = 250
_MAX_IMAGES = 600

# Explicit connect/read timeouts for every CVAT request, mirroring
# `fetch/sportapp.py`'s discipline of never issuing an unbounded network call.
_CVAT_CONNECT_TIMEOUT_S = 10.0
_CVAT_READ_TIMEOUT_S = 60.0

_IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png")


class DatasetError(CvError, ValueError):
    """Raised when a COCO export fails structural validation against its sample
    manifest (missing frames, an out-of-vocabulary category, or a degenerate box).
    """


@dataclass(frozen=True)
class DatasetStats:
    """Summary statistics for a validated COCO dataset: image/box counts, the
    train/val split sizes, and the reproducible `content_sha256` used to pin the
    exact labeled dataset a training run consumed.

    `n_boxes` is keyed by `CLASS_NAMES` plus a synthetic `"_empty_images"` entry
    counting images with zero annotations -- legal (a frame can genuinely show no
    visible player after correction), so counted rather than rejected.
    """

    n_images: int
    n_boxes: dict[str, int]
    split_counts: dict[str, int]
    content_sha256: str


def validate_coco(coco_dir: Path, manifest: FrameSampleManifest) -> DatasetStats:
    """Validate `coco_dir` (a CVAT COCO export) against `manifest`: every sampled
    frame must be present, every category must be exactly `CLASS_NAMES` in order, no
    annotation may reference an unknown image/category, no box may be degenerate or
    out of bounds, the image count must sit in `[_MIN_IMAGES, _MAX_IMAGES]`, and both
    the `train` and `val` splits (derived from `manifest.split`) must carry at least
    one `player` box. Raises `DatasetError` naming the first violation found, with the
    offending item(s) in the message. Zero-annotation images are legal and counted
    under the `"_empty_images"` key of `DatasetStats.n_boxes` rather than rejected --
    a frame can genuinely show no visible player after human correction.
    """
    coco_dir = Path(coco_dir)
    annotation_path = coco_dir / "instances.json"
    if not annotation_path.is_file():
        raise DatasetError(f"missing COCO annotation file: {annotation_path}")

    try:
        data = json.loads(annotation_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DatasetError(f"{annotation_path} is not valid JSON: {exc}") from None

    categories = data.get("categories", [])
    category_by_id: dict[int, str] = {c["id"]: c["name"] for c in categories}
    category_names_in_order = [c["name"] for c in sorted(categories, key=lambda c: c["id"])]
    if category_names_in_order != list(CLASS_NAMES):
        raise DatasetError(
            f"{annotation_path} categories {category_names_in_order} do not match the "
            f"required vocabulary {list(CLASS_NAMES)} in order -- an extra or missing "
            "category (e.g. a stray 'ball') violates C-12"
        )

    images = data.get("images", [])
    image_by_id: dict[int, dict] = {img["id"]: img for img in images}
    image_file_names = {img["file_name"] for img in images}
    manifest_by_file_name = {Path(frame.image_path).name: frame for frame in manifest.frames}
    manifest_file_names = set(manifest_by_file_name)

    missing = sorted(manifest_file_names - image_file_names)
    extra = sorted(image_file_names - manifest_file_names)
    if missing or extra:
        raise DatasetError(
            f"{annotation_path} image set does not match manifest {manifest.session_id!r}: "
            f"missing={missing} extra={extra}"
        )

    annotations = data.get("annotations", [])
    n_boxes: dict[str, int] = {name: 0 for name in CLASS_NAMES}
    split_box_counts: dict[str, dict[str, int]] = {
        split: {name: 0 for name in CLASS_NAMES} for split in set(manifest.split.values())
    }
    images_with_annotations: set[int] = set()

    for ann in annotations:
        ann_id = ann.get("id")
        image_id = ann.get("image_id")
        category_id = ann.get("category_id")

        image = image_by_id.get(image_id)
        if image is None:
            raise DatasetError(
                f"{annotation_path} annotation {ann_id} references unknown image_id {image_id}"
            )
        category_name = category_by_id.get(category_id)
        if category_name is None:
            raise DatasetError(
                f"{annotation_path} annotation {ann_id} references unknown category_id "
                f"{category_id}"
            )

        bbox = ann.get("bbox")
        if not isinstance(bbox, list) or len(bbox) != 4:
            raise DatasetError(
                f"{annotation_path} annotation {ann_id} has a malformed bbox {bbox!r}"
            )
        x, y, w, h = (float(v) for v in bbox)
        if w <= 0 or h <= 0:
            raise DatasetError(
                f"{annotation_path} annotation {ann_id} on {image['file_name']} has a "
                f"degenerate bbox {bbox!r} (w>0, h>0 required)"
            )
        img_w, img_h = image.get("width"), image.get("height")
        if x < 0 or y < 0 or x + w > img_w or y + h > img_h:
            raise DatasetError(
                f"{annotation_path} annotation {ann_id} bbox {bbox!r} on "
                f"{image['file_name']} falls outside the image bounds ({img_w}x{img_h})"
            )

        n_boxes[category_name] += 1
        images_with_annotations.add(image_id)

        frame = manifest_by_file_name.get(image["file_name"])
        split = frame.split if frame is not None else None
        if split is not None:
            split_box_counts.setdefault(split, {name: 0 for name in CLASS_NAMES})
            split_box_counts[split][category_name] += 1

    n_boxes["_empty_images"] = sum(
        1 for img in images if img["id"] not in images_with_annotations
    )

    n_images = len(images)
    if n_images < _MIN_IMAGES:
        raise DatasetError(
            f"{annotation_path} has {n_images} images, below the {_MIN_IMAGES}-image "
            f"floor (REQ-S2-02 targets ~300-500 human-corrected training frames)"
        )
    if n_images > _MAX_IMAGES:
        raise DatasetError(
            f"{annotation_path} has {n_images} images, above the {_MAX_IMAGES}-image "
            "ceiling -- D-06: the pilot does not answer a gate miss with more training "
            "labels; evaluation labeling (plan 02.1-15) is a separate, allowed budget"
        )

    # Per-image (not per-clip) split counts: `manifest.split` maps clip_number -> split,
    # but one clip can carry several sampled frames, so counting frames directly is the
    # image-level truth `DatasetStats.split_counts` documents.
    split_counts: dict[str, int] = {}
    for frame in manifest.frames:
        split_counts[frame.split] = split_counts.get(frame.split, 0) + 1

    for split, counts in split_box_counts.items():
        if split not in split_counts:
            continue
        if counts.get("player", 0) == 0:
            raise DatasetError(
                f"{annotation_path} split {split!r} has zero 'player' boxes -- a split "
                "with no player annotations cannot train or validate the detector"
            )

    content_sha256 = dataset_hash(coco_dir)

    return DatasetStats(
        n_images=n_images,
        n_boxes=n_boxes,
        split_counts=split_counts,
        content_sha256=content_sha256,
    )


def dataset_hash(root: Path) -> str:
    """Compute a reproducible content hash of the COCO package at `root`: a sha256
    over the sorted list of `(relative_file_name, sha256(file_bytes))` pairs for every
    image file under `root`, plus the canonical (sorted-key, separator-normalised) JSON
    of `root/instances.json`.

    Depends only on relative paths and byte content, never on `root`'s absolute
    location -- two byte-identical datasets copied to different directories hash
    identically. A single flipped bbox coordinate changes the annotations JSON and
    therefore the hash.
    """
    root = Path(root)

    image_entries: list[tuple[str, str]] = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.suffix.lower() in _IMAGE_SUFFIXES:
            rel_name = path.relative_to(root).as_posix()
            file_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
            image_entries.append((rel_name, file_sha256))
    image_entries.sort()

    annotations_path = root / "instances.json"
    annotations_data = (
        json.loads(annotations_path.read_text(encoding="utf-8"))
        if annotations_path.is_file()
        else {}
    )
    canonical_annotations = json.dumps(annotations_data, sort_keys=True, separators=(",", ":"))

    hasher = hashlib.sha256()
    for rel_name, file_sha256 in image_entries:
        hasher.update(rel_name.encode("utf-8"))
        hasher.update(b"\x00")
        hasher.update(file_sha256.encode("utf-8"))
        hasher.update(b"\n")
    hasher.update(canonical_annotations.encode("utf-8"))

    return hasher.hexdigest()


def _build_client(host: str) -> Client:
    """Construct a raw `cvat_sdk` client against `host` with an explicit connect/read
    timeout wired in before any request is made (including the SDK's own eager
    server-version handshake, which is skipped here and never performed at all --
    `client.login()` is the first real request either caller makes).

    This is the single seam every CVAT call in this module goes through, and the one
    function `tests/test_cv_cvat.py` monkeypatches to stub the network entirely.
    """
    import cvat_sdk
    import urllib3

    client = cvat_sdk.Client(url=host, check_server_version=False)
    client.api_client.rest_client.pool_manager.connection_pool_kw["timeout"] = urllib3.Timeout(
        connect=_CVAT_CONNECT_TIMEOUT_S, read=_CVAT_READ_TIMEOUT_S
    )
    return client


def _safe_extract_zip(archive_path: Path, extract_dir: Path, task_id: int) -> None:
    """Extract `archive_path` into `extract_dir`, rejecting any member whose resolved
    path would land outside `extract_dir` (zip-slip / path traversal, T-2.1-21).
    """
    extract_dir.mkdir(parents=True, exist_ok=True)
    resolved_root = extract_dir.resolve()
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            member_path = (extract_dir / member.filename).resolve()
            if not (member_path == resolved_root or member_path.is_relative_to(resolved_root)):
                raise DatasetError(
                    f"CVAT export archive for task {task_id} contains a path outside the "
                    f"extraction directory: {member.filename!r}"
                )
        archive.extractall(extract_dir)


def _find_coco_annotations(extract_dir: Path, task_id: int) -> Path:
    """Locate the extracted COCO annotation file and return it as `instances.json`
    under `extract_dir` -- CVAT's own COCO 1.0 exporter names it
    `annotations/instances_default.json`; this project's convention (plan 02.1-07's
    prelabel output) is the bare `instances.json` name, so the file is normalized here.
    """
    direct = extract_dir / "instances.json"
    if direct.is_file():
        return direct

    candidates = sorted(extract_dir.rglob("instances_default.json")) or sorted(
        extract_dir.rglob("*.json")
    )
    if not candidates:
        raise DatasetError(
            f"CVAT export for task {task_id} did not contain an instances.json annotation file"
        )

    source = candidates[0]
    direct.write_bytes(source.read_bytes())
    return direct


def create_cvat_task(config: Config, coco_dir: Path, *, name: str) -> int:
    """Push the COCO package at `coco_dir` to CVAT as a new task named `name`,
    returning the created task id.

    The client is built exclusively from `config.cv.cvat_host` and credentials
    resolved through `flag_football_ep.config.secret()` -- never a literal host or a
    literal credential, and never a credential surfaced in an exception message.
    """
    coco_dir = Path(coco_dir)
    annotation_path = coco_dir / "instances.json"
    if not annotation_path.is_file():
        raise DatasetError(f"missing COCO annotation file: {annotation_path}")

    image_files = sorted(
        p for p in coco_dir.iterdir() if p.is_file() and p.suffix.lower() in _IMAGE_SUFFIXES
    )
    if not image_files:
        raise DatasetError(f"no image files found in {coco_dir}")

    from flag_football_ep.config import secret

    username = secret(config.cv.cvat_username_env)
    password = secret(config.cv.cvat_password_env)

    client = None
    try:
        client = _build_client(config.cv.cvat_host)
        client.login((username, password))

        from cvat_sdk import models

        labels = [models.PatchedLabelRequest(name=class_name) for class_name in CLASS_NAMES]
        task_spec = models.TaskWriteRequest(name=name, labels=labels)
        task = client.tasks.create_from_data(
            spec=task_spec,
            resources=image_files,
            annotation_path=str(annotation_path),
            annotation_format="COCO 1.0",
        )
        return task.id
    except Exception as exc:
        status = getattr(exc, "status", "unknown")
        raise DatasetError(
            f"CVAT task creation failed against {config.cv.cvat_host} (HTTP {status})"
        ) from None
    finally:
        if client is not None:
            client.close()


def export_cvat_task(config: Config, task_id: int, out_dir: Path) -> Path:
    """Pull the reviewed annotations for `task_id` back from CVAT as a COCO export
    written under `out_dir`, unpacked, and returned as the path to `instances.json`.

    The client is built exclusively from `config.cv.cvat_host` and credentials
    resolved through `flag_football_ep.config.secret()` -- never a literal host or a
    literal credential, and never a credential surfaced in an exception message.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    archive_path = out_dir / f"task_{task_id}_export.zip"

    from flag_football_ep.config import secret

    username = secret(config.cv.cvat_username_env)
    password = secret(config.cv.cvat_password_env)

    client = None
    try:
        client = _build_client(config.cv.cvat_host)
        client.login((username, password))
        task = client.tasks.retrieve(task_id)
        task.export_dataset("COCO 1.0", archive_path, include_images=True)
    except Exception as exc:
        status = getattr(exc, "status", "unknown")
        raise DatasetError(
            f"CVAT export failed for task {task_id} against {config.cv.cvat_host} "
            f"(HTTP {status})"
        ) from None
    finally:
        if client is not None:
            client.close()

    if not archive_path.exists() or archive_path.stat().st_size == 0:
        raise DatasetError(f"CVAT export for task {task_id} produced an empty archive")

    # Extracted directly into out_dir (not a task_{id} subdirectory): plan 02.1-09's
    # dataset validation/recording step (`ffep cv dataset --coco <out_dir> ...`) expects
    # `instances.json` and the image files to sit directly under the directory the
    # operator names on the CLI, matching this project's other COCO-package convention
    # (cv/prelabel.py's `out_dir`).
    _safe_extract_zip(archive_path, out_dir, task_id)

    return _find_coco_annotations(out_dir, task_id)
