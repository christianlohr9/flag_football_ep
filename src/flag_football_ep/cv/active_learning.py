"""Active-learning frame selection for iterative dataset growth toward REQ-S2-03.

No upstream active-learning library targets RF-DETR's output shape, so this module is
"build, don't import" (RESEARCH Pattern 1): an uncertainty score per frame
(`frame_uncertainty_score`, over `cv.detect`'s existing detection output, never a
redefinition of the detection schema) combined with a diversity key
(`diversity_key`) so `select_al_frames` draws a stratified, non-redundant next-iteration
batch -- the same "group, allocate per group, then allocate within group" two-level
pattern `frames.py::sample_training_frames` already uses for its clip/hover-position
stratification. `write_selection_manifest`/`read_selection_manifest` persist an
`ALSelection` with the same atomic-JSON round trip as `frames.py::write_manifest`/
`read_manifest`, so a later iteration can read an earlier one's selection without
re-deriving it.

`select_al_frames` reuses `detect.detect_video`'s existing decode+detect batching --
never a new seek-based inference path -- and only *keeps* a coarse, evenly-spaced grid
of the yielded `DetectionBatch`es as scoring candidates (RESEARCH Pattern 1). The
candidate pool is restricted to `role = pool` clips (`frames.read_eval_split`,
T-2.2-25): a clip held out as `frozen_eval` is never opened for candidate scoring at
all, so it can never be selected regardless of how uncertain a frame inside it would
have scored. Every AL prelabel/selection detector call always resolves through
`detect.load_detector`'s default `champion`-alias path -- the newest fine-tuned
detector, never the zero-shot fallback chain (RESEARCH Pitfall 1, T-2.2-26).

Implemented by plan 02.2-09.
"""

from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
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


# Mirrors `detect._MODEL_CONFIDENCE_THRESHOLD` (RF-DETR's own documented default):
# every `DetectionBatch` this module scores has already passed `detect_video`'s own
# confidence filter, so every observed confidence lies in
# `[_MODEL_CONFIDENCE_THRESHOLD, 1.0]`. Kept as a local constant (not a cross-module
# import of a private name) since `frame_uncertainty_score` must stay usable against
# any object carrying a bare `.confidence` array, not only a real `detect.py` call.
_MODEL_CONFIDENCE_THRESHOLD = 0.5
_MAX_UNCERTAINTY_MARGIN = 1.0 - _MODEL_CONFIDENCE_THRESHOLD
# A non-empty frame's score must always land strictly below the empty-frame score of
# 1.0 (the plan's own behaviour spec: "1.0 for an empty detection set, and a value in
# [0, 1) that decreases..."), even at zero margin (confidence sitting exactly on the
# threshold, the most uncertain a non-empty frame can be).
_NONEMPTY_UNCERTAINTY_EPSILON = 1e-6

# Per-clip candidate-count ceiling: reuses the spirit of `frames._MAX_FRAMES_PER_CLIP`
# (12) so no single clip's frames can dominate a selection -- a clip contributes at
# most this many *candidates* in the first place (the grid `_grid_indices` draws from
# its decoded frames), which in turn upper-bounds how many of its frames the final
# selection can ever include.
_MAX_CANDIDATES_PER_CLIP = 12

_MANIFEST_FILENAME = "selection_manifest.json"


def frame_uncertainty_score(detections) -> float:
    """Score one frame's detections by how uncertain the current detector is about
    them (higher = more valuable to label next).

    Zero detections in a frame scores exactly `1.0` -- a domain-shift miss (the
    detector expected players and found none) is the strongest available signal.
    Otherwise, the score is `1.0` minus the mean absolute distance of every
    detection's confidence from `_MODEL_CONFIDENCE_THRESHOLD`, normalized into
    `[0, 1)`: a detection whose confidence sits right at the threshold is the most
    uncertain a non-empty frame can be (score approaches, but never reaches, `1.0`);
    a detection the model is very sure about (confidence near `1.0`) scores near `0.0`.

    Accepts anything carrying a `.confidence` array (a real `DetectionBatch`/
    `sv.Detections`) or a bare iterable of confidence values directly, so tests can
    construct a lightweight fake without a real detection object.
    """
    confidence = getattr(detections, "confidence", detections)
    confidence = list(confidence)
    if not confidence:
        return 1.0

    mean_margin = sum(
        abs(float(c) - _MODEL_CONFIDENCE_THRESHOLD) for c in confidence
    ) / len(confidence)
    normalized_margin = (
        min(1.0, mean_margin / _MAX_UNCERTAINTY_MARGIN) if _MAX_UNCERTAINTY_MARGIN > 0 else 0.0
    )
    return max(0.0, (1.0 - normalized_margin) - _NONEMPTY_UNCERTAINTY_EPSILON)


def diversity_key(row) -> tuple:
    """Return the stratification key a candidate frame is grouped by: `(domain,
    session_id, stratum_id, field_zone_bucket)` -- the same "group, allocate per
    group" discipline `frames.py::sample_training_frames` uses for its clip/hover-
    position stratification, extended with a field-zone bucket so a selection cannot
    collapse onto one repeatedly-sampled scene (RESEARCH Anti-Pattern).

    `row` may be any object or mapping exposing `domain`/`session_id`/`stratum_id`/
    `field_zone_bucket` -- attribute access for dataclasses/namespaces, item access
    for dicts.
    """

    def _get(name: str):
        if isinstance(row, dict):
            return row[name]
        return getattr(row, name)

    return (
        _get("domain"),
        _get("session_id"),
        _get("stratum_id"),
        _get("field_zone_bucket"),
    )


@dataclass(frozen=True)
class _Candidate:
    """One scored grid frame considered for selection -- the internal `row` shape
    `diversity_key` groups by. Not part of the module's public contract.
    """

    domain: str
    session_id: str
    clip_number: int
    clip_path: Path
    frame_index: int
    timestamp_s: float
    uncertainty: float
    stratum_id: str
    field_zone_bucket: str


def _resolve_domain(config: Config, session_id: str) -> str:
    """The single domain `session_id` is registered under in `video_inventory.csv`.

    Raises `ActiveLearningError` naming `session_id` when no domain is registered for
    it, or naming every candidate domain when more than one row disagrees (today's
    inventory never registers the same `session_id` under two domains, but this stays
    defensive rather than silently picking one).
    """
    import polars as pl

    inventory_path = config.paths.reference / "video_inventory.csv"
    if not inventory_path.exists():
        raise ActiveLearningError(f"video inventory not found: {inventory_path}")

    df = pl.read_csv(inventory_path, columns=["domain", "session_id"])
    domains = sorted(
        df.filter(pl.col("session_id") == session_id).select("domain").unique().to_series().to_list()
    )
    if not domains:
        raise ActiveLearningError(
            f"no domain registered for session_id {session_id!r} in {inventory_path}"
        )
    if len(domains) > 1:
        raise ActiveLearningError(
            f"session_id {session_id!r} is registered under multiple domains "
            f"{domains} in {inventory_path} -- ambiguous, cannot select a single "
            "candidate pool"
        )
    return domains[0]


def _probe_clip_meta(clip: Path) -> tuple[float, float]:
    """`(fps, frame_width)` for `clip`, read straight from the container via cv2 --
    reused to convert a `DetectionBatch.frame_index` into a `timestamp_s`
    (`frame_index / fps`) and to bucket a frame's mean detection foot point into a
    thirds-of-frame-width zone.
    """
    import cv2

    capture = cv2.VideoCapture(str(clip))
    try:
        fps = capture.get(cv2.CAP_PROP_FPS)
        width = capture.get(cv2.CAP_PROP_FRAME_WIDTH)
    finally:
        capture.release()
    return (fps if fps > 0 else 1.0, width if width > 0 else 0.0)


def _grid_indices(n_frames: int, cap: int) -> list[int]:
    """Up to `cap` evenly spaced frame indices across `[0, n_frames - 1]` -- the
    "coarse frame grid" this module scores per clip, rather than every decoded frame
    (adjacent frames of a 30fps clip are near-duplicates; a dense scan would only
    inflate the candidate pool's redundancy, not its information).
    """
    if n_frames <= 0:
        return []
    count = min(cap, n_frames)
    if count <= 1:
        return [0]
    step = (n_frames - 1) / (count - 1)
    return sorted({round(i * step) for i in range(count)})


def _field_zone_bucket(xyxy, frame_width: float) -> str:
    """Bucket a frame's mean detection foot-point x-position into thirds of the frame
    width (`left`/`mid`/`right`) -- a coarse proxy for field zone (Line-of-Scrimmage
    congestion tends to cluster spatially within a frame, docs/dataset-plan.md `## 5`).
    Falls back to `"unknown"` when the frame carries no detections at all (a
    domain-shift miss has no foot point to bucket by) or `frame_width` is not known.
    """
    if len(xyxy) == 0 or frame_width <= 0:
        return "unknown"
    x_centers = [(float(box[0]) + float(box[2])) / 2.0 for box in xyxy]
    mean_x = sum(x_centers) / len(x_centers)
    third = frame_width / 3.0
    if mean_x < third:
        return "left"
    if mean_x < 2 * third:
        return "mid"
    return "right"


def select_al_frames(
    config: Config,
    session_ids: list[str],
    iteration: int,
    target: int,
    seed: int,
    out_dir: Path,
) -> ALSelection:
    """Draw the next active-learning iteration's frame batch: run the current
    (`champion`-aliased, fine-tuned) detector over `session_ids`' candidate pool,
    score by `frame_uncertainty_score`, stratify by `diversity_key`, and select
    `target` frames.

    The candidate pool for each session is every clip registered for that session's
    domain under `role = pool` in `data/reference/frozen_eval_clips.csv`
    (`frames.read_eval_split`) -- a clip held out as `role = frozen_eval` is never
    opened for candidate scoring at all (T-2.2-25), so it can never be selected no
    matter how uncertain a frame inside it would have scored. `iteration > 1` also
    excludes every `(session_id, clip_number, frame_index)` already present in
    `iteration - 1`'s manifest, read from `out_dir.parent /
    f"iteration-{iteration - 1}" / "selection_manifest.json"` -- the same directory
    convention `ffep cv active-learn` itself uses for `out_dir`.

    Candidates are scored via `detect.detect_video`'s existing decode+detect
    batching (never a new seek-based inference path): every clip is decoded in full,
    but only an evenly-spaced coarse grid of up to `_MAX_CANDIDATES_PER_CLIP` decoded
    frames is kept as a scoring candidate, capping how much any one clip can
    contribute to a selection. Candidates are grouped by `diversity_key` (`domain,
    session_id, stratum_id, field_zone_bucket`), the group weights allocated
    proportionally to `target` via `frames._allocate_proportional`, and within each
    group the highest-uncertainty candidates are taken (ties broken by a
    `random.Random(f"{seed}:{diversity_key}")`-seeded shuffle, so the same seed
    always reproduces the same tie-break order). Every stratum with at least one
    candidate receives at least one frame when `target` exceeds the stratum count,
    and no stratum receives more than its computed proportional share. Selected
    frames are extracted into `out_dir` via `frames.extract_frames` and the full
    selection is persisted to `out_dir / "selection_manifest.json"`.

    Raises `ActiveLearningError` naming the domain when a session's candidate pool is
    empty (every registered clip is `frozen_eval`, or none remain after excluding a
    prior iteration's selection).
    """
    from flag_football_ep.cv import detect
    from flag_football_ep.cv.frames import (
        _allocate_proportional,
        _read_stratum_ids,
        clip_number,
        clip_paths,
        extract_frames,
        read_eval_split,
    )

    out_dir = Path(out_dir)
    eval_split_path = config.paths.reference / "frozen_eval_clips.csv"
    eval_split = read_eval_split(eval_split_path)

    excluded_frame_keys: set[tuple[str, int, int]] = set()
    if iteration > 1:
        prior_manifest_path = out_dir.parent / f"iteration-{iteration - 1}" / _MANIFEST_FILENAME
        prior_selection = read_selection_manifest(prior_manifest_path)
        excluded_frame_keys = {
            (f.session_id, f.clip_number, f.frame_index) for f in prior_selection.frames
        }

    model = detect.load_detector(config)

    candidates: list[_Candidate] = []
    domains_seen: list[str] = []
    for session_id in session_ids:
        domain = _resolve_domain(config, session_id)
        domains_seen.append(domain)
        frozen_numbers = set(eval_split.clips_by_domain.get(domain, []))
        clips = clip_paths(config, session_id, domain=domain)
        pool_clips = [c for c in clips if clip_number(c) not in frozen_numbers]
        if not pool_clips:
            raise ActiveLearningError(
                f"empty candidate pool for domain {domain!r} (session {session_id!r}): "
                "every registered clip is held out as a frozen eval clip"
            )

        clip_numbers = {clip_number(c) for c in pool_clips}
        strata = _read_stratum_ids(config, domain, session_id, clip_numbers)

        for clip in pool_clips:
            n = clip_number(clip)
            fps, width = _probe_clip_meta(clip)
            batches = list(
                detect.detect_video(
                    config, clip, model, resolution=config.cv.resolution, sahi=config.cv.sahi
                )
            )
            for i in _grid_indices(len(batches), _MAX_CANDIDATES_PER_CLIP):
                batch = batches[i]
                frame_key = (session_id, n, batch.frame_index)
                if frame_key in excluded_frame_keys:
                    continue
                candidates.append(
                    _Candidate(
                        domain=domain,
                        session_id=session_id,
                        clip_number=n,
                        clip_path=clip,
                        frame_index=batch.frame_index,
                        timestamp_s=batch.frame_index / fps if fps > 0 else 0.0,
                        uncertainty=frame_uncertainty_score(batch),
                        stratum_id=strata[n],
                        field_zone_bucket=_field_zone_bucket(batch.xyxy, width),
                    )
                )

    if not candidates:
        raise ActiveLearningError(
            f"empty candidate pool for domain(s) {sorted(set(domains_seen))} after "
            "excluding frozen-eval clips and any prior-iteration selection"
        )

    groups: dict[tuple, list[_Candidate]] = {}
    for candidate in candidates:
        groups.setdefault(diversity_key(candidate), []).append(candidate)

    key_by_str = {str(key): key for key in groups}
    weights_by_str = {str(key): float(len(members)) for key, members in groups.items()}
    raw_alloc = _allocate_proportional(weights_by_str, target)

    selected: list[_Candidate] = []
    selected_keys: set[tuple[str, int, int]] = set()
    for key_str in sorted(key_by_str):
        key = key_by_str[key_str]
        group = groups[key]
        n_candidates = len(group)
        # Floor of 1 for every non-empty stratum once `target` exceeds the stratum
        # count -- mirrors `frames.sample_training_frames`'s own floor/ceiling clamp,
        # which likewise does not guarantee the total sums to exactly `target`.
        group_target = max(1 if target > 0 else 0, min(raw_alloc.get(key_str, 0), n_candidates))

        rng = random.Random(f"{seed}:{key_str}")
        shuffled = list(group)
        rng.shuffle(shuffled)
        ranked = sorted(shuffled, key=lambda c: -c.uncertainty)

        for candidate in ranked[:group_target]:
            frame_key = (candidate.session_id, candidate.clip_number, candidate.frame_index)
            if frame_key in selected_keys:
                continue
            selected.append(candidate)
            selected_keys.add(frame_key)

    if len(selected) < target:
        remaining = sorted(
            (
                c
                for c in candidates
                if (c.session_id, c.clip_number, c.frame_index) not in selected_keys
            ),
            key=lambda c: (-c.uncertainty, c.session_id, c.clip_number, c.frame_index),
        )
        for candidate in remaining:
            if len(selected) >= target:
                break
            selected.append(candidate)
            selected_keys.add((candidate.session_id, candidate.clip_number, candidate.frame_index))

    selected.sort(key=lambda c: (c.session_id, c.clip_number, c.frame_index))

    by_clip: dict[Path, list[_Candidate]] = {}
    for candidate in selected:
        by_clip.setdefault(candidate.clip_path, []).append(candidate)

    al_frames: list[ALFrame] = []
    for clip_path, clip_candidates in by_clip.items():
        ordered = sorted(clip_candidates, key=lambda c: c.timestamp_s)
        timestamps = [c.timestamp_s for c in ordered]
        written = extract_frames(clip_path, out_dir, timestamps)
        for candidate, image_path in zip(ordered, written):
            al_frames.append(
                ALFrame(
                    session_id=candidate.session_id,
                    clip_number=candidate.clip_number,
                    frame_index=candidate.frame_index,
                    timestamp_s=candidate.timestamp_s,
                    image_path=str(image_path),
                    uncertainty_score=candidate.uncertainty,
                    diversity_key=diversity_key(candidate),
                )
            )

    al_frames.sort(key=lambda f: (f.session_id, f.clip_number, f.frame_index))

    manifest = ALSelection(
        session_ids=list(session_ids),
        iteration=iteration,
        target=target,
        seed=seed,
        frames=al_frames,
    )
    write_selection_manifest(manifest, out_dir / _MANIFEST_FILENAME)
    return manifest


def write_selection_manifest(manifest: ALSelection, path: Path) -> Path:
    """Persist `manifest` to `path`, using the same `.tmp` + `os.replace` atomic
    write discipline as `frames.py::write_manifest`.
    """
    data = {
        "session_ids": list(manifest.session_ids),
        "iteration": manifest.iteration,
        "target": manifest.target,
        "seed": manifest.seed,
        "frames": [
            {
                "session_id": frame.session_id,
                "clip_number": frame.clip_number,
                "frame_index": frame.frame_index,
                "timestamp_s": frame.timestamp_s,
                "image_path": frame.image_path,
                "uncertainty_score": frame.uncertainty_score,
                "diversity_key": list(frame.diversity_key),
            }
            for frame in manifest.frames
        ],
    }

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return path


def read_selection_manifest(path: Path) -> ALSelection:
    """Load an `ALSelection` previously written by `write_selection_manifest`.

    Raises `ActiveLearningError` naming `path` when the file is absent, is not valid
    JSON, or is missing a required top-level key.
    """
    path = Path(path)
    if not path.exists():
        raise ActiveLearningError(f"selection manifest not found: {path}")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ActiveLearningError(
            f"selection manifest at {path} is not valid JSON: {exc}"
        ) from exc

    required_keys = {"session_ids", "iteration", "target", "seed", "frames"}
    missing_keys = required_keys - data.keys()
    if missing_keys:
        raise ActiveLearningError(
            f"selection manifest at {path} is missing key(s) {sorted(missing_keys)}"
        )

    frames = [
        ALFrame(
            session_id=row["session_id"],
            clip_number=row["clip_number"],
            frame_index=row["frame_index"],
            timestamp_s=row["timestamp_s"],
            image_path=row["image_path"],
            uncertainty_score=row["uncertainty_score"],
            diversity_key=tuple(row["diversity_key"]),
        )
        for row in data["frames"]
    ]

    return ALSelection(
        session_ids=list(data["session_ids"]),
        iteration=data["iteration"],
        target=data["target"],
        seed=data["seed"],
        frames=frames,
    )
