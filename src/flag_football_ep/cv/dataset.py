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

    extract_dir = out_dir / f"task_{task_id}"
    _safe_extract_zip(archive_path, extract_dir, task_id)

    return _find_coco_annotations(extract_dir, task_id)
