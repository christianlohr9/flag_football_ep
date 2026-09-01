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
from datetime import datetime, timezone
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

    `domain` defaults to `"drone"` -- every `FrameSample` this module itself
    constructs (`sample_training_frames`) sets it explicitly from the caller's own
    `domain` argument; the default only matters for a manifest built by hand (tests,
    or a manifest written before this field existed) and preserves the single-domain
    Phase-2.1 convention those predate.
    """

    clip_number: int
    clip_path: str
    frame_index: int
    timestamp_s: float
    image_path: str
    split: str
    domain: str = "drone"


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
                    domain=domain,
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
                "domain": frame.domain,
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
            domain=row.get("domain", "drone"),
        )
        for row in data["frames"]
    ]

    return FrameSampleManifest(
        session_id=data["session_id"], seed=seed, target=target, frames=frames, split=split
    )


# --- Frozen eval-clip split (Phase 2.2, D-04/D-13) --------------------------------
#
# One level up from sample_training_frames's clip-level split discipline (this
# plan's <interfaces> block): freezes which clips, per domain, are held out for
# detector evaluation *before* any active-learning selection touches the remaining
# pool, so the per-domain mAP a training run reports (cv.detect.evaluate_per_domain,
# plan 02.2-15) is always measured against the identical held-out clips.


class EvalSplitError(CvError, ValueError):
    """Raised when a frozen eval-clip split cannot be written or read: a domain has
    fewer than `_MIN_VAL_CLIPS` clips to split, a domain already frozen in `out_csv`
    is asked to re-freeze under a different seed (the split is frozen by
    definition -- never silently overwritten), or the CSV at `path` does not exist,
    is not readable, or does not match the `EvalSplit` schema.
    """


@dataclass(frozen=True)
class EvalSplit:
    """The frozen per-domain evaluation-clip split: which clip numbers, grouped by
    domain, are held out for detector evaluation, the fraction/seed used to draw
    them, and when the freeze happened.

    `fraction` reflects the call that produced this value: `freeze_eval_clips`
    returns the fraction it was invoked with; `read_eval_split` returns the
    *achieved* fraction across every row in the file (n `frozen_eval` rows / n total
    rows) since a single manifest can accumulate domains frozen at different
    fractions across multiple `freeze_eval_clips` calls (only `seed` is enforced as
    a single file-wide invariant, not `fraction`).
    """

    clips_by_domain: dict[str, list[int]]
    fraction: float
    seed: int
    frozen_at: str


_EVAL_SPLIT_COLUMNS = (
    "domain",
    "session_id",
    "clip_number",
    "stratum_id",
    "role",
    "private_test",
    "frozen_at",
    "seed",
)

_EVAL_SPLIT_SCHEMA: dict[str, pl.DataType] = {
    "domain": pl.Utf8,
    "session_id": pl.Utf8,
    "clip_number": pl.Int64,
    "stratum_id": pl.Utf8,
    "role": pl.Utf8,
    "private_test": pl.Boolean,
    "frozen_at": pl.Utf8,
    "seed": pl.Int64,
}

# D-07: the drone domain's frozen eval clips double as the private hackathon test
# set -- the only domain whose `frozen_eval` rows carry `private_test = true`.
_PRIVATE_TEST_DOMAIN = "drone"


def _read_domain_session_ids(config: Config, domain: str) -> list[str]:
    """Every distinct `session_id` registered for `domain` in `video_inventory.csv`,
    sorted for determinism. Today every domain has exactly one session, but this
    stays general rather than assuming a singleton.
    """
    inventory_path = config.paths.reference / "video_inventory.csv"
    if not inventory_path.exists():
        raise EvalSplitError(f"video inventory not found: {inventory_path}")

    df = pl.read_csv(inventory_path, schema_overrides=_INVENTORY_SCHEMA)
    rows = df.filter(pl.col("domain") == domain)
    if rows.height == 0:
        raise EvalSplitError(f"no clips registered for domain {domain!r} in {inventory_path}")

    return sorted(rows.select("session_id").unique().to_series().to_list())


def _read_domain_clip_numbers(config: Config, domain: str, session_id: str) -> set[int]:
    """Clip numbers registered for `session_id`/`domain` in `video_inventory.csv`.

    Deliberately mirrors `_read_clip_durations`'s metadata-only read (filter the
    inventory, parse `clip_number` out of `local_path`), not `clip_paths`'s: freezing
    the eval split only needs which clip numbers exist and which stratum/session they
    belong to, never the video bytes themselves, so it must not require the actual
    clip file to be present on disk (`clip_paths` enforces that for frame-extraction
    callers; a stratification/bookkeeping caller like this one must not inherit that
    requirement).
    """
    inventory_path = config.paths.reference / "video_inventory.csv"
    df = pl.read_csv(inventory_path, schema_overrides=_INVENTORY_SCHEMA)
    rows = df.filter((pl.col("domain") == domain) & (pl.col("session_id") == session_id))

    numbers: set[int] = set()
    for row in rows.iter_rows(named=True):
        local_path = row["local_path"]
        if local_path:
            numbers.add(clip_number(Path(local_path)))
    return numbers


def _read_stratum_ids(
    config: Config, domain: str, session_id: str, clip_numbers: set[int]
) -> dict[int, str]:
    """Read `hover_position_id` per clip for `session_id`/`domain` -- the stratum
    `freeze_eval_clips` allocates the held-out fraction within. The drone domain's
    strata live in `config.reference.hover_positions` (Plan 02.1-03's file, keyed by
    `clip_number` alone); every other domain's strata live in the per-session
    sighting CSV `sighting.py::sight_session` already wrote (Plan 02.2-02), at the
    same default path `sight_session` itself resolves to for a non-drone domain:
    `config.paths.reference / f"sighting_{session_id}.csv"`.
    """
    if domain == _PRIVATE_TEST_DOMAIN:
        path = config.reference.hover_positions
    else:
        path = config.paths.reference / f"sighting_{session_id}.csv"

    if not path.exists():
        raise EvalSplitError(
            f"stratum source for domain {domain!r} session {session_id!r} not found: {path}"
        )

    df = pl.read_csv(
        path,
        schema_overrides=_HOVER_POSITIONS_SCHEMA,
        columns=["clip_number", "hover_position_id"],
    )
    ids = {int(row["clip_number"]): row["hover_position_id"] for row in df.iter_rows(named=True)}

    missing = clip_numbers - set(ids)
    if missing:
        raise EvalSplitError(
            f"{path} is missing hover_position_id for clip(s) {sorted(missing)} "
            f"(domain {domain!r}, session {session_id!r})"
        )
    return {n: ids[n] for n in clip_numbers}


def _write_eval_split_csv(rows: list[dict], path: Path) -> None:
    """Atomically write the frozen eval-clip split CSV: `.tmp` sibling + `os.replace`
    (T-2.1-10 discipline, matching `write_manifest`/`_write_hover_positions_csv`),
    sorted by `(domain, clip_number)` so re-running with unchanged inputs produces a
    byte-identical file.
    """
    rows_sorted = sorted(rows, key=lambda r: (r["domain"], r["clip_number"]))
    df = pl.DataFrame(
        {col: [row[col] for row in rows_sorted] for col in _EVAL_SPLIT_COLUMNS},
        schema=_EVAL_SPLIT_SCHEMA,
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        df.write_csv(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def freeze_eval_clips(
    config: Config, domains: list[str], fraction: float, seed: int, out_csv: Path
) -> EvalSplit:
    """Freeze the held-out per-domain evaluation-clip split before any
    active-learning selection touches the training pool (D-04/D-13): draws
    `fraction` of each domain's clips at `seed`, persists the split to `out_csv`,
    and returns it as an `EvalSplit` covering the domains passed in *this* call.

    Allocation is stratified: within each domain, clips are grouped by
    `hover_position_id` (the same stratum `sample_training_frames` groups by), and
    `round(fraction * len(stratum))` clips are drawn from each stratum via
    `random.Random(f"{seed}:{domain}:{stratum_id}")` -- independent per
    `(domain, stratum)` so adding a domain later never reshuffles an already-frozen
    one. A domain with fewer than `_MIN_VAL_CLIPS` clips total raises
    `EvalSplitError` naming the domain and its clip count -- too few clips to carve
    out a meaningful, reproducible eval split.

    `out_csv` accumulates domains across multiple calls (D-04's "one detector across
    domains" needs every domain's frozen split in one place, and different domains
    are legitimately frozen at different fractions/times -- e.g. the drone domain's
    D-07 private-test-set fraction differs from a later-admitted domain's). A domain
    already present in `out_csv` is treated as already frozen: if this call's `seed`
    matches the file's recorded seed for that domain, the call is a no-op for it
    (re-running with the same seed leaves the file byte-identical); if the seed
    differs, `EvalSplitError` is raised naming both seeds rather than silently
    overwriting the frozen split -- `seed` is a single file-wide invariant, exactly
    because the whole point of freezing is that it never moves once written.

    Only the drone domain's `frozen_eval` rows carry `private_test = true` (D-07:
    the drone eval clips double as the private hackathon test set).
    """
    if not (0.0 < fraction < 1.0):
        raise EvalSplitError(f"fraction must be strictly between 0 and 1: {fraction!r}")
    if not domains:
        raise EvalSplitError("domains must be a non-empty list")

    existing_rows: list[dict] = []
    if out_csv.exists():
        existing_df = pl.read_csv(out_csv, schema_overrides=_EVAL_SPLIT_SCHEMA)
        existing_rows = existing_df.to_dicts()

        existing_seeds = {row["seed"] for row in existing_rows}
        if existing_seeds and existing_seeds != {seed}:
            raise EvalSplitError(
                f"{out_csv} was already frozen with seed(s) {sorted(existing_seeds)}; "
                f"cannot freeze additional domains with a different seed {seed} "
                "(the split is frozen by definition)"
            )

    existing_domains = {row["domain"] for row in existing_rows}
    frozen_at = datetime.now(timezone.utc).isoformat()

    new_rows: list[dict] = []
    clips_by_domain: dict[str, list[int]] = {}

    for domain in domains:
        if domain in existing_domains:
            # Idempotent no-op: this domain is already frozen under the same seed
            # (the seed-mismatch case already raised above). Report its existing
            # frozen_eval clips without touching the file.
            clips_by_domain[domain] = sorted(
                row["clip_number"]
                for row in existing_rows
                if row["domain"] == domain and row["role"] == "frozen_eval"
            )
            continue

        session_ids = _read_domain_session_ids(config, domain)
        clip_session: dict[int, str] = {}
        for session_id in session_ids:
            for n in _read_domain_clip_numbers(config, domain, session_id):
                clip_session[n] = session_id

        all_clip_numbers = set(clip_session)
        if len(all_clip_numbers) < _MIN_VAL_CLIPS:
            raise EvalSplitError(
                f"domain {domain!r} has only {len(all_clip_numbers)} clip(s), fewer "
                f"than the minimum {_MIN_VAL_CLIPS} required for a frozen eval split"
            )

        strata: dict[int, str] = {}
        for session_id in session_ids:
            session_clips = {n for n, sid in clip_session.items() if sid == session_id}
            strata.update(_read_stratum_ids(config, domain, session_id, session_clips))

        groups: dict[str, list[int]] = {}
        for n in sorted(all_clip_numbers):
            groups.setdefault(strata[n], []).append(n)

        eval_clip_numbers: set[int] = set()
        for stratum_id, members in sorted(groups.items()):
            n_eval = min(len(members), max(0, round(fraction * len(members))))
            rng = random.Random(f"{seed}:{domain}:{stratum_id}")
            shuffled = list(members)
            rng.shuffle(shuffled)
            eval_clip_numbers.update(shuffled[:n_eval])

        is_private_test = domain == _PRIVATE_TEST_DOMAIN
        for n in sorted(all_clip_numbers):
            role = "frozen_eval" if n in eval_clip_numbers else "pool"
            new_rows.append(
                {
                    "domain": domain,
                    "session_id": clip_session[n],
                    "clip_number": n,
                    "stratum_id": strata[n],
                    "role": role,
                    "private_test": role == "frozen_eval" and is_private_test,
                    "frozen_at": frozen_at,
                    "seed": seed,
                }
            )
        clips_by_domain[domain] = sorted(eval_clip_numbers)

    if new_rows:
        _write_eval_split_csv(existing_rows + new_rows, out_csv)

    return EvalSplit(
        clips_by_domain=clips_by_domain, fraction=fraction, seed=seed, frozen_at=frozen_at
    )


def read_eval_split(path: Path) -> EvalSplit:
    """Load an `EvalSplit` previously written by `freeze_eval_clips`.

    Raises `EvalSplitError` naming `path` when the file is absent, is not readable
    as CSV, or is missing one of `_EVAL_SPLIT_COLUMNS`. `seed` must be identical
    across every row (a file-wide invariant `freeze_eval_clips` itself enforces on
    write) -- an inconsistent file raises rather than picking one value silently.
    `fraction` is the *achieved* fraction across the whole file (n `frozen_eval` rows
    / n total rows), since a file frozen by multiple calls can mix per-domain
    fractions (see `freeze_eval_clips`'s docstring). `frozen_at` is the latest
    freeze timestamp recorded in the file.
    """
    if not path.exists():
        raise EvalSplitError(f"eval split not found: {path}")

    try:
        df = pl.read_csv(path, schema_overrides=_EVAL_SPLIT_SCHEMA)
    except Exception as exc:  # noqa: BLE001 -- surfaced as a named EvalSplitError
        raise EvalSplitError(f"eval split at {path} could not be read: {exc}") from exc

    missing_columns = set(_EVAL_SPLIT_COLUMNS) - set(df.columns)
    if missing_columns:
        raise EvalSplitError(f"eval split at {path} is missing column(s) {sorted(missing_columns)}")

    if df.height == 0:
        raise EvalSplitError(f"eval split at {path} has no rows")

    seeds = df.select("seed").unique().to_series().to_list()
    if len(seeds) != 1:
        raise EvalSplitError(f"eval split at {path} carries inconsistent seeds: {sorted(seeds)}")

    eval_rows = df.filter(pl.col("role") == "frozen_eval")
    clips_by_domain: dict[str, list[int]] = {}
    for domain in sorted(eval_rows.select("domain").unique().to_series().to_list()):
        clips_by_domain[domain] = sorted(
            eval_rows.filter(pl.col("domain") == domain)
            .select("clip_number")
            .to_series()
            .to_list()
        )

    fraction = eval_rows.height / df.height if df.height else 0.0
    frozen_at = max(df.select("frozen_at").unique().to_series().to_list())

    return EvalSplit(
        clips_by_domain=clips_by_domain,
        fraction=fraction,
        seed=int(seeds[0]),
        frozen_at=frozen_at,
    )
