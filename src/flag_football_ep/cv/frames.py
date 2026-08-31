"""Clip discovery and frame extraction/sampling for the CV tracking pilot.

Owns two data flows: (1) locating a session's per-clip video files under
`config.paths.video` (`clip_paths`/`clip_number`) and extracting frames from
one clip at explicit timestamps (`extract_frames`) -- the primitive every
later stage (sighting, sampling, prelabeling) builds on; and (2) drawing the
stratified training-frame sample (`sample_training_frames`) that becomes the
labeling set, persisted as a `FrameSampleManifest` so the exact frames,
train/val split and random seed used for labeling are reproducible and never
re-derived silently (`write_manifest`/`read_manifest`).

`clip_paths`/`clip_number`/`extract_frames` were implemented by plan 02.1-03.
`sample_training_frames`/`write_manifest`/`read_manifest` were implemented by
plan 02.1-07, using `sighting.py`'s per-clip hover-position grouping
(plan 02.1-03, `data/reference/hover_positions.csv`) to stratify the sample.
"""

from __future__ import annotations

import json
import os
import random
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from flag_football_ep.config import Config


class ClipNotFound(CvError, ValueError):
    """Raised when a clip number or path cannot be resolved under `config.paths.video`."""


class FrameExtractionError(CvError, RuntimeError):
    """Raised when ffmpeg exits non-zero while extracting a frame from a clip."""


class ManifestError(CvError, ValueError):
    """Raised when a frame-sample manifest cannot be read: the path does not exist, or
    the JSON at that path does not match the `FrameSampleManifest` schema (missing
    keys, or a non-integer `seed`/`target`).
    """


# Per-clip sample-size bounds for `sample_training_frames`: a floor so no clip is
# unrepresented, a ceiling so no single long clip dominates the labeling set (this
# plan's <action> block).
_MIN_FRAMES_PER_CLIP = 3
_MAX_FRAMES_PER_CLIP = 12

# Grid-placement margin (seconds) kept clear at the start/end of every clip, and the
# jitter fraction of the grid-cell width applied on top of the evenly spaced grid --
# even spacing alone would systematically sample the same play phase in every clip;
# pure randomness would clump (this plan's <action> block).
_GRID_MARGIN_S = 0.5
_JITTER_FRACTION = 0.4

# Clip-level train/val split fraction and floor (this plan's <action> block): splitting
# by clip (not frame) is required because adjacent frames of a 30 fps clip are nearly
# identical images -- a frame-level split would leak a validation clip's neighbouring
# frames into training and make the detector metric meaningless.
_VAL_FRACTION = 0.2
_MIN_VAL_CLIPS = 6

_HOVER_POSITIONS_SCHEMA: dict[str, pl.DataType] = {
    "clip_number": pl.Int64,
    "hover_position_id": pl.Utf8,
}


# Mirrors tests/test_capture_artifacts.py::INVENTORY_SCHEMA -- the authoritative
# video_inventory.csv schema registered by Phase 2.0. video_inventory.csv is not one
# of Config.reference's declared ReferenceFiles (it predates the [cv] config surface),
# so it is addressed relative to config.paths.reference, matching every other
# reference CSV's directory.
_INVENTORY_SCHEMA: dict[str, pl.DataType] = {
    "domain": pl.Utf8,
    "session_id": pl.Utf8,
    "game_id": pl.Utf8,
    "capture_date": pl.Utf8,
    "resolution": pl.Utf8,
    "fps": pl.Float64,
    "duration_seconds": pl.Float64,
    "local_path": pl.Utf8,
    "content_sha256": pl.Utf8,
    "notes": pl.Utf8,
}

# Hudl re-encode filename pattern: "Wide - Clip 001.mp4" -> clip number 1. The clip
# number (not Hudl play_id) is the D-02 pseudo play key for this pilot.
_CLIP_NUMBER_RE = re.compile(r"(\d+)\.[^./]+$")


@dataclass(frozen=True)
class FrameSample:
    """One sampled training frame: which clip it came from, at which index/timestamp,
    where the extracted image was written, and which split (`train`/`val`) its parent
    clip belongs to.
    """

    clip_number: int
    clip_path: str
    frame_index: int
    timestamp_s: float
    image_path: str
    split: str


@dataclass(frozen=True)
class FrameSampleManifest:
    """The full record of a `sample_training_frames` run: the session it was drawn
    from, every sampled frame, the clip-level train/val split (`split`, keyed by
    `clip_number`), and the `seed`/`target` used to draw it -- the labeling set this
    manifest describes is reproducible from `(session_id, seed, target)` alone.
    """

    session_id: str
    seed: int
    target: int
    frames: list[FrameSample]
    split: dict[int, str]


def clip_paths(config: Config, session_id: str, *, domain: str = "drone") -> list[Path]:
    """List every clip video file registered for `session_id` and `domain` under
    `config.paths.video`, in clip-number order.

    Reads `data/reference/video_inventory.csv` (the Phase-2.0 source of truth, never a
    hard-coded glob) with the same typed `schema_overrides` discipline as
    `reference._read_reference_csv`, filters `domain == domain` and the requested
    `session_id`, and resolves every `local_path` against the repo root
    (`config.paths.data_root.parent`).

    Raises `ClipNotFound` naming `session_id` when the filter yields zero rows; naming
    the offending row when `local_path` is empty, absolute, home-relative (`~`), or
    escapes the repo root (T-2.1-02, hand-maintained CSV is untrusted input); and naming
    the missing file when a registered `local_path` does not exist on disk.
    """
    inventory_path = config.paths.reference / "video_inventory.csv"
    if not inventory_path.exists():
        raise ClipNotFound(f"video inventory not found: {inventory_path}")

    df = pl.read_csv(inventory_path, schema_overrides=_INVENTORY_SCHEMA)
    rows = df.filter((pl.col("domain") == domain) & (pl.col("session_id") == session_id))
    if rows.height == 0:
        raise ClipNotFound(
            f"no {domain} clips found for session_id {session_id!r} in {inventory_path}"
        )

    repo_root = config.paths.data_root.parent.resolve()
    paths: list[Path] = []
    for row in rows.iter_rows(named=True):
        local_path = row["local_path"]
        if not local_path:
            raise ClipNotFound(
                f"empty local_path for session_id {session_id!r} row: {row}"
            )
        if local_path.startswith("/") or local_path.startswith("~"):
            raise ClipNotFound(f"local_path is not repo-relative: {local_path!r}")

        candidate = Path(local_path)
        # Syntactic escape check (no ".." component), deliberately not a filesystem
        # `.resolve()` + `relative_to(repo_root)` check: `data/video/` legitimately
        # contains symlinks (footage synced in from elsewhere on disk, per
        # docs/material-inventory.md), and resolving through those would make a
        # perfectly repo-relative `local_path` look like it "escapes" the repo root.
        # A CSV row can still only reference paths that stay inside `data/video/` --
        # this check rejects `..` traversal the same way an absolute path is rejected
        # above (T-2.1-02).
        if ".." in candidate.parts:
            raise ClipNotFound(f"local_path escapes the repo root: {local_path!r}")

        resolved = repo_root / candidate
        if not resolved.exists():
            raise ClipNotFound(f"registered clip file does not exist: {resolved}")

        paths.append(resolved)

    return sorted(paths, key=clip_number)


def clip_number(path: Path) -> int:
    """Parse the clip number out of a clip filename, raising `ClipNotFound` when the
    filename does not follow the registered clip-naming convention.
    """
    match = _CLIP_NUMBER_RE.search(path.name)
    if not match:
        raise ClipNotFound(f"no clip number found in filename: {path.name}")
    return int(match.group(1))


def _probe_fps(clip: Path) -> float:
    """Read the container frame rate straight from `clip` via ffprobe -- the same value
    a stable, joinable `round(timestamp * fps)` frame index depends on.
    """
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=r_frame_rate",
            "-of",
            "csv=p=0",
            str(clip),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise FrameExtractionError(
            f"ffprobe exited {result.returncode} reading fps from {clip}: "
            f"{result.stderr.strip()[-2000:]}"
        )
    raw = result.stdout.strip()
    if not raw:
        raise FrameExtractionError(f"ffprobe returned no frame rate for {clip}")
    if "/" in raw:
        num, _, den = raw.partition("/")
        return float(num) / float(den)
    return float(raw)


def extract_frames(clip: Path, out_dir: Path, at_seconds: list[float]) -> list[Path]:
    """Extract one still frame per timestamp in `at_seconds` from `clip` into `out_dir`.

    Invokes ffmpeg via `subprocess.run` with an argument list, never a shell string
    (T-2.1-09), one accurate seek per requested timestamp. Each written file is named
    `{clip_stem}_f{frame_index:05d}.jpg` with `frame_index = round(timestamp * fps)`,
    `fps` probed straight from `clip`, so frame indices stay stable and joinable with
    tracking output later. `out_dir` is created if absent. A non-zero ffmpeg exit
    raises `FrameExtractionError` carrying ffmpeg's stderr tail.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    fps = _probe_fps(clip)

    written: list[Path] = []
    for timestamp in at_seconds:
        frame_index = round(timestamp * fps)
        out_path = out_dir / f"{clip.stem}_f{frame_index:05d}.jpg"
        result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                f"{timestamp}",
                "-i",
                str(clip),
                "-frames:v",
                "1",
                "-q:v",
                "2",
                str(out_path),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise FrameExtractionError(
                f"ffmpeg exited {result.returncode} extracting frame at {timestamp}s "
                f"from {clip}: {result.stderr.strip()[-2000:]}"
            )
        written.append(out_path)

    return written


def _read_clip_durations(
    config: Config, session_id: str, clip_numbers: set[int], *, domain: str = "drone"
) -> dict[int, float]:
    """Read `duration_seconds` for every clip in `clip_numbers` from
    `video_inventory.csv` (the same source `clip_paths` reads, filtered the same way).
    """
    inventory_path = config.paths.reference / "video_inventory.csv"
    df = pl.read_csv(inventory_path, schema_overrides=_INVENTORY_SCHEMA)
    rows = df.filter((pl.col("domain") == domain) & (pl.col("session_id") == session_id))

    durations: dict[int, float] = {}
    for row in rows.iter_rows(named=True):
        local_path = row["local_path"]
        if not local_path:
            continue
        n = clip_number(Path(local_path))
        if n in clip_numbers:
            durations[n] = float(row["duration_seconds"])

    missing = clip_numbers - set(durations)
    if missing:
        raise ClipNotFound(
            f"video_inventory.csv is missing duration_seconds for clip(s) "
            f"{sorted(missing)} in session {session_id!r}"
        )
    return durations


def _read_hover_position_ids(config: Config, clip_numbers: set[int]) -> dict[int, str]:
    """Read `hover_position_id` per clip from `config.reference.hover_positions`, when
    that file exists. Absent entirely (sighting pass not yet run) -> empty dict, which
    callers treat as "every clip shares one group".
    """
    path = config.reference.hover_positions
    if not path.exists():
        return {}

    df = pl.read_csv(
        path,
        schema_overrides=_HOVER_POSITIONS_SCHEMA,
        columns=["clip_number", "hover_position_id"],
    )
    return {
        int(row["clip_number"]): row["hover_position_id"]
        for row in df.iter_rows(named=True)
        if int(row["clip_number"]) in clip_numbers
    }


def _allocate_proportional(weights: dict[str, float], target: int) -> dict[str, int]:
    """Split `target` across `weights`' keys proportionally to their value.

    Falls back to an even split when every weight is zero (e.g. a group's clips all
    report zero duration) rather than dividing by zero.
    """
    total = sum(weights.values())
    if total <= 0:
        n = len(weights)
        if n == 0:
            return {}
        base, remainder = divmod(target, n)
        return {
            key: base + (1 if i < remainder else 0) for i, key in enumerate(sorted(weights))
        }
    return {key: max(0, round(target * weight / total)) for key, weight in weights.items()}


def sample_training_frames(
    config: Config,
    session_id: str,
    *,
    target: int,
    seed: int,
    out_dir: Path,
    domain: str = "drone",
) -> FrameSampleManifest:
    """Draw the stratified `target`-frame labeling sample for `session_id` in `domain`,
    split train/val, extract the sampled frames into `out_dir`, and return the manifest.

    The frame budget is allocated proportionally to clip duration, first across hover
    positions (when `data/reference/hover_positions.csv` names more than one for this
    session's clips -- every camera regime must be represented) and then within each
    hover position across its clips, with a floor of `_MIN_FRAMES_PER_CLIP` and a
    ceiling of `_MAX_FRAMES_PER_CLIP` per clip. For non-drone domains without hover
    positions registered, the grouping key falls back to the single bucket `"all"`
    (`hover_ids.get(n, "all")` below), exactly as it already does for drone clips that
    predate a sighting pass. Within a clip, timestamps sit on an evenly spaced grid over
    `[0.5s, duration - 0.5s]` (cell-centered, so no two clips' grids overlap at the
    boundary) and are jittered by up to `_JITTER_FRACTION` of the grid-cell width using
    a `random.Random(f"{seed}:{clip_number}")` generator -- deriving the per-clip
    generator from `(seed, clip_number)` keeps every clip's jitter sequence independent
    of how many other clips were sampled, so a later run that adds/removes an unrelated
    clip does not silently reshuffle every other clip's frames.

    The train/val split is decided at the CLIP level (never per frame): frame-level
    splitting would leak a validation clip's near-duplicate neighbouring frames into
    training, since adjacent frames of a 30 fps clip are nearly identical images.

    Frame indices are computed from the same ffprobe-measured fps `extract_frames`
    itself uses (not `video_inventory.csv`'s declared fps, which can round
    differently), so a manifest entry always matches the file `extract_frames` writes.
    """
    clips = clip_paths(config, session_id, domain=domain)
    clip_by_number = {clip_number(path): path for path in clips}
    all_numbers = set(clip_by_number)

    durations = _read_clip_durations(config, session_id, all_numbers, domain=domain)
    hover_ids = _read_hover_position_ids(config, all_numbers)

    groups: dict[str, list[int]] = {}
    for n in sorted(clip_by_number):
        groups.setdefault(hover_ids.get(n, "all"), []).append(n)

    group_footage = {gid: sum(durations[n] for n in members) for gid, members in groups.items()}
    group_targets = _allocate_proportional(group_footage, target)

    clip_targets: dict[int, int] = {}
    for gid, members in groups.items():
        group_duration = group_footage[gid]
        group_target = group_targets[gid]
        for n in members:
            if group_duration > 0:
                raw = group_target * (durations[n] / group_duration)
            else:
                raw = group_target / len(members)
            clip_targets[n] = min(_MAX_FRAMES_PER_CLIP, max(_MIN_FRAMES_PER_CLIP, round(raw)))

    clip_numbers = sorted(clip_by_number)
    shuffled = list(clip_numbers)
    random.Random(seed).shuffle(shuffled)
    n_val = min(len(clip_numbers), max(_MIN_VAL_CLIPS, round(_VAL_FRACTION * len(clip_numbers))))
    val_clips = set(shuffled[:n_val])
    split_by_clip = {n: ("val" if n in val_clips else "train") for n in clip_numbers}

    repo_root = config.paths.data_root.parent.resolve()

    frames: list[FrameSample] = []
    for n in clip_numbers:
        clip = clip_by_number[n]
        duration = durations[n]
        n_frames = clip_targets[n]

        start = _GRID_MARGIN_S
        end = max(start + 0.01, duration - _GRID_MARGIN_S)
        cell_width = (end - start) / n_frames

        # `random.Random` only accepts None/int/float/str/bytes/bytearray seeds (not a
        # tuple) -- a colon-joined string keeps the per-clip generator independent of
        # every other clip's seed the same way a tuple would.
        jitter_rng = random.Random(f"{seed}:{n}")
        timestamps: list[float] = []
        for i in range(n_frames):
            base = start + cell_width * (i + 0.5)
            jitter = jitter_rng.uniform(-_JITTER_FRACTION * cell_width, _JITTER_FRACTION * cell_width)
            timestamps.append(min(end, max(start, base + jitter)))

        fps = _probe_fps(clip)
        written = extract_frames(clip, out_dir, timestamps)
        clip_relative = clip.relative_to(repo_root).as_posix()
        clip_split = split_by_clip[n]

        for timestamp, image_path in zip(timestamps, written):
            frames.append(
                FrameSample(
                    clip_number=n,
                    clip_path=clip_relative,
                    frame_index=round(timestamp * fps),
                    timestamp_s=timestamp,
                    image_path=str(image_path),
                    split=clip_split,
                )
            )

    return FrameSampleManifest(
        session_id=session_id,
        seed=seed,
        target=target,
        frames=frames,
        split=split_by_clip,
    )


def write_manifest(manifest: FrameSampleManifest, path: Path) -> Path:
    """Persist `manifest` to `path`: a `.tmp` sibling written first, then `os.replace`
    (T-2.1-10 -- never a half-written manifest on disk), JSON with sorted keys and
    2-space indent so re-running `sample_training_frames` with the same `(target, seed)`
    produces a byte-identical file.
    """
    data = {
        "session_id": manifest.session_id,
        "seed": manifest.seed,
        "target": manifest.target,
        "split": {str(clip_num): split for clip_num, split in sorted(manifest.split.items())},
        "frames": [
            {
                "clip_number": frame.clip_number,
                "clip_path": frame.clip_path,
                "frame_index": frame.frame_index,
                "timestamp_s": frame.timestamp_s,
                "image_path": frame.image_path,
                "split": frame.split,
            }
            for frame in manifest.frames
        ],
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return path


def read_manifest(path: Path) -> FrameSampleManifest:
    """Load a `FrameSampleManifest` previously written by `write_manifest`.

    Raises `ManifestError` naming `path` when the file is absent, is not valid JSON, is
    missing a required top-level key, or carries a non-integer `seed`/`target` -- a
    malformed manifest must never be silently accepted, since later plans (dataset
    hashing, training) key on it.
    """
    if not path.exists():
        raise ManifestError(f"manifest not found: {path}")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ManifestError(f"manifest at {path} is not valid JSON: {exc}") from exc

    required_keys = {"session_id", "seed", "target", "split", "frames"}
    missing_keys = required_keys - data.keys()
    if missing_keys:
        raise ManifestError(f"manifest at {path} is missing key(s) {sorted(missing_keys)}")

    seed = data["seed"]
    target = data["target"]
    if not isinstance(seed, int) or not isinstance(target, int):
        raise ManifestError(
            f"manifest at {path} has non-integer seed/target: seed={seed!r} target={target!r}"
        )

    split = {int(clip_num): value for clip_num, value in data["split"].items()}
    frames = [
        FrameSample(
            clip_number=row["clip_number"],
            clip_path=row["clip_path"],
            frame_index=row["frame_index"],
            timestamp_s=row["timestamp_s"],
            image_path=row["image_path"],
            split=row["split"],
        )
        for row in data["frames"]
    ]

    return FrameSampleManifest(
        session_id=data["session_id"], seed=seed, target=target, frames=frames, split=split
    )
