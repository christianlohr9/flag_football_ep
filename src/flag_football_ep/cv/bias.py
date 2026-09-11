"""Prelabel-bias measurement for the verified eval ground truth (2026-09-11 ad-hoc plan).

`docs/dataset-buildout.md`'s `### Nachtrag 2026-09-04 (abends)` and the Plan 02.2-18
three-way comparison both flag the same open caveat: the eval ground truth under
`data/labels/eval/<domain>/corrected/` (D-19-guaranteed held out from training, but
NOT held out from the *labeling* process) was prelabelled by the Phase-2.1 champion
`87a8a5222f7a472787875e974d089c44` and mostly confirmed rather than redrawn (95% of
drone boxes, 75% of GoPro/sideline boxes unchanged from the champion's own prelabel).
That inflates any champion-like run's measured accuracy relative to a run that never
saw those images, and correspondingly deflates AL-trained runs measured against the
same ground truth -- a labeling-process bias, not a training-data leak (D-19 already
rules out the leak; `select_bias_test_frames` draws exclusively from the same
`frozen_eval` clip set the eval GT itself is drawn from, so every bias-test frame is
covered by `dataset.assert_no_frozen_eval_clips` exactly like the rest of the eval GT
-- see `tests/test_cv_bias.py::test_bias_test_clips_are_covered_by_frozen_eval_guard`).

This module measures the SIZE of that bias directly rather than continuing to flag it
as an unquantified caveat: a 30-frame subset of the existing eval GT (20 drone + 10
sideline, `select_bias_test_frames`) gets a second, from-scratch label pass with NO
prelabels at all (`build_bias_test_coco_package` pushes a zero-annotation CVAT task),
and `evaluate_bias_test` scores the same detector runs against both label sets on the
identical images -- the difference between the two columns per model IS the bias.

Three functions, one pipeline:

1. `select_bias_test_frames` -- deterministic, seeded, per-clip-capped selection from
   the already-sampled/verified eval GT (never draws new frames from video -- these 30
   images already exist and already carry a prelabel-derived label).
2. `build_bias_test_coco_package` -- a zero-annotation COCO package for
   `ffep cv cvat-push` (reuses the existing push path unmodified; this module only
   builds the "no prelabels" package).
3. `evaluate_bias_test` -- after the user labels the pushed task from scratch and it
   is pulled back down, computes (a) box-level agreement between the two label sets
   per frame and (b) `evaluate_domain_frames`-based mAP for each of several detector
   runs against both label sets side by side, per domain.
"""

from __future__ import annotations

import json
import os
import random
import re
import shutil
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError
from flag_football_ep.cv.dataset import CLASS_NAMES

if TYPE_CHECKING:
    from flag_football_ep.config import Config

# Mirrors `dataset.py`/`detect.py`'s own `_IMAGE_SUFFIXES` -- kept as a separate
# constant per this codebase's small-private-helper-duplication precedent rather than
# importing a `_`-prefixed name across a module boundary.
_IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png")

# Matches `detect.py`'s `_EVAL_CLIP_NUMBER_RE`, extended to also capture the frame
# index -- this module needs both, that one only needs the clip number.
_CLIP_FRAME_RE = re.compile(r"Clip[ _](\d+)_f(\d+)")

_EVAL_GT_ROOT_DIRNAME = "eval"
_EVAL_GT_DIRNAME = "corrected"

# Domain-prefix separator for the bias-test push package: mirrors the "domain-prefixed
# file names" convention `docs/dataset-buildout.md`'s `### Merge & Validierung
# (Plan 02.2-13, Task 2)` section documents for merging multiple domains into one
# directory -- clip numbers are NOT unique across domains (e.g. clip 11/16/36/52 exist
# in both the drone and sideline frozen_eval sets), so pushing both domains' frames
# into a single flat CVAT task directory needs this to avoid a file name collision.
_DOMAIN_SEP = "__"

_BIAS_TEST_CSV_COLUMNS = ("domain", "session_id", "clip_number", "frame_index", "file_name")

# Detection-matching threshold for "the same box" (standard COCO-style matching
# threshold) and the stricter "essentially unchanged" threshold `docs/dataset-buildout.md`'s
# `### Nachtrag 2026-09-04 (abends)` already established for prelabel-vs-corrected
# diffing (IoU >= 0.95) -- reused here as the boundary between "two annotators drew
# compatible-but-not-identical boxes" and "two annotators drew essentially the same box".
_MATCH_IOU_THRESHOLD = 0.5
_NEAR_IDENTICAL_IOU = 0.95


class BiasTestError(CvError, ValueError):
    """Raised when the bias-test frame sample, push package, or analysis cannot be
    produced -- a missing verified eval GT source, a stale `bias_test_frames.csv`
    against the eval GT it was drawn from, or a domain with no bias-test ground truth
    on one side of the comparison.
    """


@dataclass(frozen=True)
class BiasTestFrame:
    """One frame drawn into the bias test: which domain/session/clip it came from and
    its file name under `data/labels/eval/<domain>/corrected/` -- no image bytes, no
    player-identifying data, just the same clip/frame identifiers the rest of this
    project's manifests already carry (D-19's own labeling convention).
    """

    domain: str
    session_id: str
    clip_number: int
    frame_index: int
    file_name: str


def _index_images_by_name(coco_dir: Path) -> dict[str, Path]:
    """Map every image file's basename under `coco_dir` to its actual path -- mirrors
    `detect.py`'s own `_index_images_by_name` (a real CVAT COCO export nests images
    under `images/default/`, not directly beside `instances.json`).
    """
    index: dict[str, Path] = {}
    for path in sorted(Path(coco_dir).rglob("*")):
        if path.is_file() and path.suffix.lower() in _IMAGE_SUFFIXES:
            index.setdefault(path.name, path)
    return index


def _read_frozen_eval_sessions(eval_split_path: Path) -> dict[tuple[str, int], str]:
    """`{(domain, clip_number): session_id}` for every `role == "frozen_eval"` row --
    mirrors `detect.py`'s `_read_frozen_eval_session_ids`, flattened to a single dict
    keyed by the pair this module actually looks up by.
    """
    import polars as pl

    df = pl.read_csv(eval_split_path)
    rows = df.filter(pl.col("role") == "frozen_eval")
    return {
        (row["domain"], int(row["clip_number"])): row["session_id"]
        for row in rows.iter_rows(named=True)
    }


def select_bias_test_frames(
    config: Config,
    *,
    n_by_domain: dict[str, int],
    max_per_clip: int,
    seed: int,
    eval_split_path: Path | None = None,
) -> list[BiasTestFrame]:
    """Draw a deterministic, seeded subset of `n_by_domain[domain]` frames per domain
    from the already-sampled, already-verified eval ground truth at
    `data/labels/eval/<domain>/corrected/instances.json` -- never a new extraction
    from video (these 30 frames already exist and already carry a prelabel-derived
    label; the bias test measures a second, from-scratch label pass on the SAME
    images, not a new sample of different images).

    Stratified over clips: allocation is round-robin over the domain's frozen_eval
    clips in a seeded-shuffled order, at most `max_per_clip` frames drawn from any one
    clip, so the 30-frame subset spreads across as many distinct clips as the target
    and cap allow rather than concentrating on a few. Frame choice within a clip is a
    second, independently seeded draw (`random.Random(f"{seed}:{domain}:{clip}")`),
    matching `frames.sample_eval_gt_frames`'s own per-clip-generator discipline (a
    later re-run with an unchanged seed/target reproduces byte-identical output; a
    domain with more clips added later never reshuffles another domain's draw).

    Raises `BiasTestError` naming the domain when its verified eval GT does not exist
    yet, when its frozen_eval clip capacity (`sum(min(max_per_clip, n_frames_in_clip))`
    over all its clips) is below the requested target, or when a clip present in the
    eval GT is not (or no longer) a `frozen_eval` clip in `eval_split_path` (the eval
    GT and the frozen split have diverged -- should never happen in practice, since
    `sample_eval_gt_frames` only ever draws from `frozen_eval` clips in the first
    place, but checked explicitly rather than assumed).
    """
    resolved_split_path = (
        Path(eval_split_path)
        if eval_split_path is not None
        else config.paths.reference / "frozen_eval_clips.csv"
    )
    session_by_domain_clip = _read_frozen_eval_sessions(resolved_split_path)

    frames: list[BiasTestFrame] = []
    for domain, n_target in n_by_domain.items():
        gt_path = (
            config.paths.labels / _EVAL_GT_ROOT_DIRNAME / domain / _EVAL_GT_DIRNAME / "instances.json"
        )
        if not gt_path.is_file():
            raise BiasTestError(
                f"no verified eval ground truth for domain {domain!r} at {gt_path} -- "
                "run ffep cv eval-gt-sample + prelabel + CVAT verification first"
            )
        data = json.loads(gt_path.read_text(encoding="utf-8"))

        frames_by_clip: dict[int, list[tuple[int, str]]] = {}
        for image in data.get("images", []):
            match = _CLIP_FRAME_RE.search(image["file_name"])
            if match is None:
                continue
            clip_num = int(match.group(1))
            frame_idx = int(match.group(2))
            frames_by_clip.setdefault(clip_num, []).append((frame_idx, image["file_name"]))

        clip_numbers = sorted(frames_by_clip)
        capacity = sum(min(max_per_clip, len(frames_by_clip[c])) for c in clip_numbers)
        if capacity < n_target:
            raise BiasTestError(
                f"domain {domain!r}: cannot draw {n_target} frame(s) at max "
                f"{max_per_clip}/clip from {len(clip_numbers)} clip(s) in {gt_path} "
                f"(capacity {capacity})"
            )

        order_rng = random.Random(f"{seed}:{domain}:clip-order")
        shuffled_clips = list(clip_numbers)
        order_rng.shuffle(shuffled_clips)

        counts = dict.fromkeys(clip_numbers, 0)
        remaining = n_target
        while remaining > 0:
            progressed = False
            for clip_num in shuffled_clips:
                if remaining == 0:
                    break
                if counts[clip_num] < max_per_clip and counts[clip_num] < len(frames_by_clip[clip_num]):
                    counts[clip_num] += 1
                    remaining -= 1
                    progressed = True
            if not progressed:
                raise BiasTestError(
                    f"domain {domain!r}: exhausted per-clip capacity before drawing "
                    f"{n_target} frame(s) (this should be unreachable given the "
                    "capacity check above)"
                )

        for clip_num in clip_numbers:
            count = counts[clip_num]
            if count == 0:
                continue
            session_id = session_by_domain_clip.get((domain, clip_num))
            if session_id is None:
                raise BiasTestError(
                    f"domain {domain!r} clip {clip_num} has verified eval GT but is "
                    f"not a frozen_eval clip in {resolved_split_path} -- the eval GT "
                    "and the frozen split have diverged"
                )
            frame_rng = random.Random(f"{seed}:{domain}:{clip_num}")
            chosen = frame_rng.sample(sorted(frames_by_clip[clip_num]), k=count)
            for frame_idx, file_name in chosen:
                frames.append(
                    BiasTestFrame(
                        domain=domain,
                        session_id=session_id,
                        clip_number=clip_num,
                        frame_index=frame_idx,
                        file_name=file_name,
                    )
                )

    frames.sort(key=lambda f: (f.domain, f.clip_number, f.frame_index))
    return frames


def write_bias_test_frames_csv(frames: list[BiasTestFrame], path: Path) -> Path:
    """Atomically write the bias-test frame selection to `path` (`.tmp` sibling +
    `os.replace`, T-2.1-10 discipline, matching `frames._write_eval_split_csv`) --
    game/clip/frame identifiers only, no image bytes, no player-identifying data.
    """
    import polars as pl

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df = pl.DataFrame(
        {
            "domain": [f.domain for f in frames],
            "session_id": [f.session_id for f in frames],
            "clip_number": [f.clip_number for f in frames],
            "frame_index": [f.frame_index for f in frames],
            "file_name": [f.file_name for f in frames],
        },
        schema={
            "domain": pl.Utf8,
            "session_id": pl.Utf8,
            "clip_number": pl.Int64,
            "frame_index": pl.Int64,
            "file_name": pl.Utf8,
        },
    )

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        df.write_csv(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return path


def read_bias_test_frames_csv(path: Path) -> list[BiasTestFrame]:
    """Load a bias-test frame selection previously written by
    `write_bias_test_frames_csv`. Raises `BiasTestError` naming `path` when a required
    column is missing.
    """
    import polars as pl

    path = Path(path)
    if not path.is_file():
        raise BiasTestError(f"bias-test frame selection not found: {path}")

    df = pl.read_csv(path)
    missing = set(_BIAS_TEST_CSV_COLUMNS) - set(df.columns)
    if missing:
        raise BiasTestError(f"{path} is missing column(s) {sorted(missing)}")

    return [
        BiasTestFrame(
            domain=row["domain"],
            session_id=row["session_id"],
            clip_number=int(row["clip_number"]),
            frame_index=int(row["frame_index"]),
            file_name=row["file_name"],
        )
        for row in df.iter_rows(named=True)
    ]


def build_bias_test_coco_package(config: Config, frames: list[BiasTestFrame], out_dir: Path) -> Path:
    """Build a zero-annotation COCO package for the bias-test CVAT push: one image per
    selected frame, hardlinked (falling back to a copy across filesystems, mirroring
    `prelabel.prelabel_frames`'s/`dataset.split_coco_for_task_upload`'s own hardlink
    discipline) from its domain's verified eval-GT `corrected/` directory into a
    single flat directory -- `dataset.create_cvat_task` only looks at direct children
    of `coco_dir`, never a nested search, so every image must land directly under
    `out_dir`, not in a per-domain subdirectory.

    File names are prefixed `<domain>__` (see `_DOMAIN_SEP`'s docstring above) to
    avoid a collision between domains that reuse the same clip number.

    Writes `"annotations": []` unconditionally -- this package must carry NO
    prelabels at all (the entire point of the bias test is a from-scratch label pass
    with nothing to anchor on), unlike every other `cvat-push` package in this
    project. Reuses the existing, unmodified `ffep cv cvat-push --coco <out_dir>
    --name eval-bias-test` command to actually push it -- this function only builds
    the package.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    domain_cache: dict[str, tuple[dict, dict[str, Path]]] = {}
    images: list[dict] = []

    for image_id, frame in enumerate(frames, start=1):
        if frame.domain not in domain_cache:
            gt_dir = config.paths.labels / _EVAL_GT_ROOT_DIRNAME / frame.domain / _EVAL_GT_DIRNAME
            data = json.loads((gt_dir / "instances.json").read_text(encoding="utf-8"))
            domain_cache[frame.domain] = (data, _index_images_by_name(gt_dir))
        data, source_index = domain_cache[frame.domain]

        image_meta = next((im for im in data["images"] if im["file_name"] == frame.file_name), None)
        if image_meta is None:
            raise BiasTestError(
                f"{frame.file_name!r} (domain {frame.domain!r}) not found in the "
                "verified eval ground truth -- bias_test_frames.csv is stale against "
                "data/labels/eval/<domain>/corrected/instances.json"
            )
        source_path = source_index.get(frame.file_name)
        if source_path is None:
            raise BiasTestError(
                f"{frame.file_name!r} (domain {frame.domain!r}) image bytes not found "
                f"under {config.paths.labels / _EVAL_GT_ROOT_DIRNAME / frame.domain / _EVAL_GT_DIRNAME}"
            )

        dest_name = f"{frame.domain}{_DOMAIN_SEP}{frame.file_name}"
        dest_path = out_dir / dest_name
        if not dest_path.exists():
            try:
                os.link(source_path, dest_path)
            except OSError:
                shutil.copy2(source_path, dest_path)

        images.append(
            {
                "id": image_id,
                "file_name": dest_name,
                "width": image_meta["width"],
                "height": image_meta["height"],
                "license": 0,
                "flickr_url": "",
                "coco_url": "",
                "date_captured": 0,
            }
        )

    categories = [{"id": i + 1, "name": name, "supercategory": ""} for i, name in enumerate(CLASS_NAMES)]
    package = {
        "licenses": [],
        "info": {},
        "categories": categories,
        "images": images,
        "annotations": [],
    }
    (out_dir / "instances.json").write_text(
        json.dumps(package, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return out_dir


def _load_existing_gt_for_domain(
    config: Config, domain: str, frames: list[BiasTestFrame]
) -> tuple[list[dict], list[dict], list[dict], dict[str, Path]]:
    """Load the existing (prelabel-derived) eval GT, restricted to `domain`'s
    selected bias-test frames -- same `(images, annotations, categories,
    image_paths)` shape `detect._load_domain_ground_truth` returns, so
    `detect.evaluate_domain_frames` accepts it unmodified.
    """
    gt_dir = config.paths.labels / _EVAL_GT_ROOT_DIRNAME / domain / _EVAL_GT_DIRNAME
    data = json.loads((gt_dir / "instances.json").read_text(encoding="utf-8"))
    wanted = {f.file_name for f in frames if f.domain == domain}
    image_index = _index_images_by_name(gt_dir)

    images: list[dict] = []
    image_paths: dict[str, Path] = {}
    old_to_new: dict[int, int] = {}
    next_image_id = 1
    for image in data.get("images", []):
        if image["file_name"] not in wanted:
            continue
        old_to_new[image["id"]] = next_image_id
        images.append({**image, "id": next_image_id})
        image_paths[image["file_name"]] = image_index[image["file_name"]]
        next_image_id += 1

    annotations: list[dict] = []
    next_ann_id = 1
    for ann in data.get("annotations", []):
        new_image_id = old_to_new.get(ann["image_id"])
        if new_image_id is None:
            continue
        annotations.append({**ann, "id": next_ann_id, "image_id": new_image_id})
        next_ann_id += 1

    return images, annotations, data.get("categories", []), image_paths


def _load_new_gt_for_domain(
    domain: str, frames: list[BiasTestFrame], bias_gt_dir: Path
) -> tuple[list[dict], list[dict], list[dict], dict[str, Path], dict[str, str]]:
    """Load the from-scratch bias-test GT (pulled CVAT export at `bias_gt_dir`),
    restricted to `domain`'s frames -- same shape as `_load_existing_gt_for_domain`,
    plus a `file_name -> original file_name` map (stripping the `<domain>__` push
    prefix) so `compute_agreement_for_domain` can align a frame against its existing-
    GT counterpart by the identifier both sides share.
    """
    bias_gt_dir = Path(bias_gt_dir)
    annotation_path = bias_gt_dir / "instances.json"
    if not annotation_path.is_file():
        raise BiasTestError(
            f"domain {domain!r}: no from-scratch bias-test ground truth at "
            f"{annotation_path} -- pull the eval-bias-test CVAT task first "
            "(ffep cv cvat-pull --task <id> --out <bias_gt_dir>)"
        )
    data = json.loads(annotation_path.read_text(encoding="utf-8"))
    image_index = _index_images_by_name(bias_gt_dir)

    prefix = f"{domain}{_DOMAIN_SEP}"
    wanted_prefixed = {f"{prefix}{f.file_name}" for f in frames if f.domain == domain}

    images: list[dict] = []
    image_paths: dict[str, Path] = {}
    prefixed_to_original: dict[str, str] = {}
    old_to_new: dict[int, int] = {}
    next_image_id = 1
    for image in data.get("images", []):
        if image["file_name"] not in wanted_prefixed:
            continue
        old_to_new[image["id"]] = next_image_id
        images.append({**image, "id": next_image_id})
        image_paths[image["file_name"]] = image_index[image["file_name"]]
        prefixed_to_original[image["file_name"]] = image["file_name"][len(prefix):]
        next_image_id += 1

    annotations: list[dict] = []
    next_ann_id = 1
    for ann in data.get("annotations", []):
        new_image_id = old_to_new.get(ann["image_id"])
        if new_image_id is None:
            continue
        annotations.append({**ann, "id": next_ann_id, "image_id": new_image_id})
        next_ann_id += 1

    return images, annotations, data.get("categories", []), image_paths, prefixed_to_original


def _boxes_by_frame(
    images: list[dict],
    annotations: list[dict],
    categories: list[dict],
    *,
    name_map: dict[str, str] | None = None,
) -> dict[str, list[tuple[str, tuple[float, float, float, float]]]]:
    """`{original_file_name: [(category_name, (x1, y1, x2, y2)), ...]}` -- `name_map`
    (when given) maps an image's own `file_name` to the original identifier to key by
    (used for the bias-test side, whose file names carry the `<domain>__` push
    prefix); identity mapping otherwise.
    """
    cat_name_by_id = {c["id"]: c["name"] for c in categories}
    anns_by_image: dict[int, list[dict]] = {}
    for ann in annotations:
        anns_by_image.setdefault(ann["image_id"], []).append(ann)

    out: dict[str, list[tuple[str, tuple[float, float, float, float]]]] = {}
    for image in images:
        original_name = name_map[image["file_name"]] if name_map is not None else image["file_name"]
        boxes: list[tuple[str, tuple[float, float, float, float]]] = []
        for ann in anns_by_image.get(image["id"], []):
            x, y, w, h = ann["bbox"]
            boxes.append((cat_name_by_id[ann["category_id"]], (x, y, x + w, y + h)))
        out[original_name] = boxes
    return out


def _iou(
    box_a: tuple[float, float, float, float], box_b: tuple[float, float, float, float]
) -> float:
    ax1, ay1, ax2, ay2 = box_a
    bx1, by1, bx2, by2 = box_b
    ix1, iy1 = max(ax1, bx1), max(ay1, by1)
    ix2, iy2 = min(ax2, bx2), min(ay2, by2)
    iw, ih = max(0.0, ix2 - ix1), max(0.0, iy2 - iy1)
    inter = iw * ih
    area_a = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    area_b = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)
    union = area_a + area_b - inter
    return inter / union if union > 0 else 0.0


def _match_boxes(
    existing_boxes: list[tuple[str, tuple[float, float, float, float]]],
    new_boxes: list[tuple[str, tuple[float, float, float, float]]],
    *,
    iou_threshold: float,
) -> tuple[list[tuple[int, int, float]], list[int], list[int]]:
    """Greedy same-category IoU matching (highest-IoU pair first, each box used at
    most once) between `existing_boxes` and `new_boxes`. Returns
    `(matches, only_existing_indices, only_new_indices)`, `matches` as
    `(existing_index, new_index, iou)`.
    """
    candidates: list[tuple[float, int, int]] = []
    for i, (cat_e, box_e) in enumerate(existing_boxes):
        for j, (cat_n, box_n) in enumerate(new_boxes):
            if cat_e != cat_n:
                continue
            iou = _iou(box_e, box_n)
            if iou >= iou_threshold:
                candidates.append((iou, i, j))
    candidates.sort(key=lambda c: -c[0])

    matched_existing: set[int] = set()
    matched_new: set[int] = set()
    matches: list[tuple[int, int, float]] = []
    for iou, i, j in candidates:
        if i in matched_existing or j in matched_new:
            continue
        matched_existing.add(i)
        matched_new.add(j)
        matches.append((i, j, iou))

    only_existing = [i for i in range(len(existing_boxes)) if i not in matched_existing]
    only_new = [j for j in range(len(new_boxes)) if j not in matched_new]
    return matches, only_existing, only_new


def compute_agreement_for_domain(
    existing_images: list[dict],
    existing_annotations: list[dict],
    existing_categories: list[dict],
    new_images: list[dict],
    new_annotations: list[dict],
    new_categories: list[dict],
    prefixed_to_original: dict[str, str],
    *,
    iou_threshold: float = _MATCH_IOU_THRESHOLD,
    near_identical_iou: float = _NEAR_IDENTICAL_IOU,
) -> dict:
    """Per-frame and aggregated box-level agreement between the existing
    (prelabel-derived) eval GT and the from-scratch bias-test GT for one domain: box
    count difference, greedy same-category IoU matching, and which boxes exist on
    only one side -- the direct measurement task 1's request asks for, independent of
    any detector.
    """
    existing_by_frame = _boxes_by_frame(existing_images, existing_annotations, existing_categories)
    new_by_frame = _boxes_by_frame(
        new_images, new_annotations, new_categories, name_map=prefixed_to_original
    )

    frame_names = sorted(set(existing_by_frame) | set(new_by_frame))
    per_frame: list[dict] = []
    total_existing = total_new = total_matched = total_only_existing = total_only_new = 0
    matched_ious: list[float] = []

    for name in frame_names:
        existing_boxes = existing_by_frame.get(name, [])
        new_boxes = new_by_frame.get(name, [])
        matches, only_existing, only_new = _match_boxes(
            existing_boxes, new_boxes, iou_threshold=iou_threshold
        )
        frame_ious = [m[2] for m in matches]
        per_frame.append(
            {
                "file_name": name,
                "n_existing": len(existing_boxes),
                "n_new": len(new_boxes),
                "box_count_diff": len(new_boxes) - len(existing_boxes),
                "n_matched": len(matches),
                "n_only_existing": len(only_existing),
                "n_only_new": len(only_new),
                "median_matched_iou": statistics.median(frame_ious) if frame_ious else None,
            }
        )
        total_existing += len(existing_boxes)
        total_new += len(new_boxes)
        total_matched += len(matches)
        total_only_existing += len(only_existing)
        total_only_new += len(only_new)
        matched_ious.extend(frame_ious)

    n_near_identical = sum(1 for iou in matched_ious if iou >= near_identical_iou)
    return {
        "n_frames": len(frame_names),
        "n_boxes_existing": total_existing,
        "n_boxes_new": total_new,
        "box_count_diff": total_new - total_existing,
        "n_matched": total_matched,
        "n_only_existing": total_only_existing,
        "n_only_new": total_only_new,
        "median_matched_iou": statistics.median(matched_ious) if matched_ious else None,
        "frac_matched_near_identical": (
            n_near_identical / total_matched if total_matched else None
        ),
        "iou_threshold": iou_threshold,
        "near_identical_iou": near_identical_iou,
        "per_frame": per_frame,
    }


def _write_json_atomic(path: Path, data: dict) -> None:
    """`.tmp` sibling + `os.replace`, matching `detect._write_json_atomic`/
    `frames.write_manifest`'s discipline (T-2.1-10).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def evaluate_bias_test(
    config: Config,
    *,
    frames_csv_path: Path,
    bias_gt_dir: Path,
    run_ids: dict[str, str],
    out_path: Path,
    resolution: int | None = None,
    sahi: bool | None = None,
) -> dict:
    """The full bias measurement: for every domain in the bias-test frame selection at
    `frames_csv_path`, (a) box-level agreement between the existing eval GT and the
    from-scratch GT at `bias_gt_dir` (`compute_agreement_for_domain`), and (b) for
    every named run in `run_ids` (e.g. `{"D": "a6d53662...", "iteration1":
    "be854a1a...", "iteration2": "682d62f9..."}`), `detect.evaluate_domain_frames`
    run twice on the identical images -- once against each label set -- so the
    per-model bias shows up directly as `new_gt` minus `existing_gt` (`delta_mAP_50`/
    `delta_mAP_50_95`), never as two separately-reported numbers a reader has to
    subtract by hand.

    `resolution`/`sahi` default to `config.cv.resolution`/`config.cv.sahi`, mirroring
    `evaluate_per_domain`'s own defaulting. Writes the full result to `out_path`
    (`_write_json_atomic`) and returns it. Raises `BiasTestError` naming the domain
    when either label set has zero images for it (a stale selection or a not-yet-
    pulled bias-test CVAT export).
    """
    frames = read_bias_test_frames_csv(frames_csv_path)
    domains = sorted({f.domain for f in frames})

    resolved_resolution = resolution if resolution is not None else config.cv.resolution
    resolved_sahi = sahi if sahi is not None else config.cv.sahi

    from flag_football_ep.cv import detect

    results: dict[str, dict] = {}
    for domain in domains:
        existing_images, existing_anns, existing_cats, existing_paths = _load_existing_gt_for_domain(
            config, domain, frames
        )
        new_images, new_anns, new_cats, new_paths, prefixed_to_original = _load_new_gt_for_domain(
            domain, frames, bias_gt_dir
        )
        if not existing_images or not new_images:
            raise BiasTestError(
                f"domain {domain!r}: missing existing ({len(existing_images)} images) "
                f"or from-scratch ({len(new_images)} images) ground truth for the "
                "bias-test frames -- has the bias-test CVAT task been pulled to "
                f"{bias_gt_dir}?"
            )

        agreement = compute_agreement_for_domain(
            existing_images, existing_anns, existing_cats,
            new_images, new_anns, new_cats, prefixed_to_original,
        )

        model_results: dict[str, dict] = {}
        for run_name, run_id in run_ids.items():
            model = detect.load_detector(config, run_id)
            existing_metrics = detect.evaluate_domain_frames(
                config, model, existing_images, existing_anns, existing_cats, existing_paths,
                resolution=resolved_resolution, sahi=resolved_sahi,
            )
            new_metrics = detect.evaluate_domain_frames(
                config, model, new_images, new_anns, new_cats, new_paths,
                resolution=resolved_resolution, sahi=resolved_sahi,
            )
            model_results[run_name] = {
                "run_id": run_id,
                "existing_gt": existing_metrics,
                "new_gt": new_metrics,
                "delta_mAP_50": new_metrics["mAP_50"] - existing_metrics["mAP_50"],
                "delta_mAP_50_95": new_metrics["mAP_50_95"] - existing_metrics["mAP_50_95"],
            }

        results[domain] = {"agreement": agreement, "models": model_results}

    _write_json_atomic(Path(out_path), results)
    return results
