"""Clip discovery and frame extraction/sampling for the CV tracking pilot.

Owns two data flows: (1) locating a session's per-clip video files under
`config.paths.video` (`clip_paths`/`clip_number`) and extracting frames from
one clip at explicit timestamps (`extract_frames`) -- the primitive every
later stage (sighting, sampling, prelabeling) builds on; and (2) drawing the
stratified training-frame sample (`sample_training_frames`) that becomes the
labeling set, persisted as a `FrameSampleManifest` so the exact frames,
train/val split and random seed used for labeling are reproducible and never
re-derived silently (`write_manifest`/`read_manifest`).

`clip_paths`/`clip_number`/`extract_frames` are implemented by plan 02.1-03.
`sample_training_frames`/`write_manifest`/`read_manifest` are implemented by
plan 02.1-07, once `sighting.py`'s per-clip apparent-size classification
(plan 02.1-03) exists to inform the sampling strategy.
"""

from __future__ import annotations

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
    and where the extracted image was written.
    """

    clip_number: int
    clip_path: str
    frame_index: int
    timestamp_s: float
    image_path: str


@dataclass(frozen=True)
class FrameSampleManifest:
    """The full record of a `sample_training_frames` run: every sampled frame, its
    train/val split assignment (`split`, keyed by `clip_number`), and the seed used --
    the labeling set this manifest describes is reproducible from these three fields alone.
    """

    frames: list[FrameSample]
    split: dict[int, str]
    seed: int


def clip_paths(config: Config, session_id: str) -> list[Path]:
    """List every clip video file registered for `session_id` under `config.paths.video`,
    in clip-number order.

    Reads `data/reference/video_inventory.csv` (the Phase-2.0 source of truth, never a
    hard-coded glob) with the same typed `schema_overrides` discipline as
    `reference._read_reference_csv`, filters `domain == "drone"` and the requested
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
    rows = df.filter((pl.col("domain") == "drone") & (pl.col("session_id") == session_id))
    if rows.height == 0:
        raise ClipNotFound(
            f"no drone clips found for session_id {session_id!r} in {inventory_path}"
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


def sample_training_frames(
    config: Config,
    session_id: str,
    *,
    target: int,
    seed: int,
    out_dir: Path,
) -> FrameSampleManifest:
    """Draw the stratified `target`-frame labeling sample for `session_id`, split
    train/val, extract the sampled frames into `out_dir`, and return the manifest.
    """
    raise NotImplementedError(
        "cv.frames.sample_training_frames is implemented by plan 02.1-07"
    )


def write_manifest(manifest: FrameSampleManifest, path: Path) -> Path:
    """Persist `manifest` to `path`."""
    raise NotImplementedError("cv.frames.write_manifest is implemented by plan 02.1-07")


def read_manifest(path: Path) -> FrameSampleManifest:
    """Load a `FrameSampleManifest` previously written by `write_manifest`."""
    raise NotImplementedError("cv.frames.read_manifest is implemented by plan 02.1-07")
