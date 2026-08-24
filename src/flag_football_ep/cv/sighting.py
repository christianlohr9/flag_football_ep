"""Per-clip apparent-player-size sighting pass: a fast manual/semi-manual review of
every clip that recommends inference settings before any detector training happens.

Owns the batch classification described in `docs/material-inventory.md`'s
clip-registration procedure: for each clip, record the hover position it was captured
from and the apparent on-screen player size (`apparent_player_px_p50`/`_p10`) that
determines whether the domain needs SAHI slicing or a higher inference resolution
(C-05: drone footage is its own detection regime, small objects, oblique > top-down --
pooled settings hide domain collapse). `sight_session` writes one `ClipSighting` row per
clip to a CSV; `recommend_inference_settings` turns those rows into a concrete
`InferenceRecommendation` (resolution + SAHI on/off) consumed by `cv/detect.py`'s
`detect_video` and `cv/track.py`'s `track_session`.

Implemented by plan 02.1-03.
"""

from __future__ import annotations

import os
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import polars as pl

from flag_football_ep.cv import CvError

if TYPE_CHECKING:
    from flag_football_ep.config import Config


class SightingError(CvError, RuntimeError):
    """Raised when a clip cannot be opened or read for fingerprinting/measurement."""


# Grouping threshold for the framing-fingerprint normalized cross-correlation: clips
# whose fingerprints correlate at or above this value share a hover_position_id. Recorded
# in `SightingResult.notices` on every run (D-03: the grouping count drives the
# homography count, so the threshold used to get there must be traceable).
#
# 0.97 was the plan's literal starting point but proved far too strict against real
# footage: raw single-frame grayscale correlation between two clips visually confirmed
# to share the same static framing came out at 0.1-0.5 (compression noise, exposure
# flicker, player-position content dominating a two-frame sample), never near 0.97 --
# every one of the 2026-05-16 pilot session's 61 clips landed in its own singleton
# group. Re-validated empirically against the full pilot session (see
# docs/pilot-sighting.md): the multi-frame-averaged, blurred fingerprint below produces
# an identical, clean 2-group split (31/30 clips) for every threshold in [-0.1, 0.1], a
# wide, stable plateau -- 0.05 sits at its center. Re-run `ffep cv sight` after any
# threshold change; the value actually used is always recorded in `SightingResult.notices`.
_CORRELATION_THRESHOLD = 0.05
_FINGERPRINT_SIZE = (64, 36)  # (width, height)
_FINGERPRINT_SAMPLE_FRAMES = 8
_FINGERPRINT_BLUR_KERNEL = (61, 61)  # suppresses per-frame compression/exposure noise

# Apparent-size measurement: MOG2 background subtraction over (almost) every frame of
# the clip, discarding an initial warmup while the per-pixel model converges. Real
# compressed drone footage carries persistent edge/line compression flicker and sideline
# crowd motion that a naive small-area floor does not filter (validated against the
# 2026-05-16 pilot session -- an earlier, looser parameterization produced 100,000+
# spurious sub-5px "blobs" per clip from exactly this noise). The area floor, fill-ratio
# ("extent") and aspect-ratio filters below reject thin line segments and speckle noise
# while keeping solid player-shaped blobs; see docs/pilot-sighting.md for the validation.
_MOG2_WINDOW_FRAMES = 300  # ~10s at 30fps, i.e. effectively "the whole clip"
_MOG2_WARMUP_FRAMES = 60
_MOG2_VAR_THRESHOLD = 64
_MOG2_HISTORY = 300
_MOG2_BLUR_KERNEL = (15, 15)
_MOG2_OPEN_KERNEL_SIZE = (5, 5)
_MIN_BLOB_AREA_PX = 150
_MIN_BLOB_EXTENT = 0.35  # area / (w*h): rejects thin lines/streaks, keeps solid blobs
_MAX_BLOB_ASPECT_RATIO = 3.0

# docs/capture-protocol.md's verbatim tier vocabulary -- this module must reuse these
# words exactly, never invent a new vocabulary.
_TIER_IDEAL = "Ideal"
_TIER_BRAUCHBAR = "Brauchbar"
_TIER_UNBRAUCHBAR = "Unbrauchbar"

# Mirrors tests/test_capture_artifacts.py::INVENTORY_SCHEMA's `resolution` column --
# video_inventory.csv is not one of Config.reference's declared ReferenceFiles, so it is
# addressed relative to config.paths.reference, matching cv/frames.py::clip_paths.
_INVENTORY_SCHEMA: dict[str, pl.DataType] = {
    "domain": pl.Utf8,
    "session_id": pl.Utf8,
    "resolution": pl.Utf8,
    "local_path": pl.Utf8,
}

@dataclass(frozen=True)
class ClipSighting:
    """One clip's sighting-pass result: hover position, apparent player size in pixels
    (median and 10th percentile, the tail that drives the resolution/SAHI decision), a
    coarse tier label, and free-text notes.
    """

    clip_number: int
    clip_path: str
    hover_position_id: str
    apparent_player_px_p50: float
    apparent_player_px_p10: float
    tier: str
    notes: str


@dataclass(frozen=True)
class InferenceRecommendation:
    """The resolution/SAHI setting recommended for a domain, with the rationale that
    produced it (named apparent-size thresholds, not a bare number).
    """

    resolution: int
    sahi: bool
    rationale: str


@dataclass
class SightingResult:
    """The full sighting-pass output: every clip's `ClipSighting` row, any notices
    (e.g. a clip that could not be classified), and the CSV path the rows were written to.
    """

    rows: list[ClipSighting] = field(default_factory=list)
    notices: list[str] = field(default_factory=list)
    csv_path: Path = field(default_factory=Path)


def _read_inventory_rows(config: Config, session_id: str) -> pl.DataFrame:
    inventory_path = config.paths.reference / "video_inventory.csv"
    if not inventory_path.exists():
        raise SightingError(f"video inventory not found: {inventory_path}")
    df = pl.read_csv(inventory_path, schema_overrides=_INVENTORY_SCHEMA)
    return df.filter((pl.col("domain") == "drone") & (pl.col("session_id") == session_id))


def _inventory_resolutions(config: Config, session_id: str) -> dict[int, str]:
    from flag_football_ep.cv.frames import clip_number

    resolutions: dict[int, str] = {}
    for row in _read_inventory_rows(config, session_id).iter_rows(named=True):
        local_path = row["local_path"]
        if not local_path:
            continue
        n = clip_number(Path(local_path))
        resolutions[n] = row["resolution"] or ""
    return resolutions


def _cv2():
    import cv2

    return cv2


def _framing_fingerprint(clip: Path) -> np.ndarray:
    """A static-camera framing fingerprint: `_FINGERPRINT_SAMPLE_FRAMES` frames spread
    across the clip, each converted to grayscale, heavily Gaussian-blurred and
    downscaled to 64x36, then averaged into one array.

    A single start/end frame pair (the plan's literal starting point) turned out to be
    dominated by moving-player content and per-frame compression/exposure noise on real
    footage, not framing -- averaging several frames and blurring before the downscale
    suppresses both, leaving the static background/field layout as the dominant signal.
    A drone repositioning between drives still shows up as a distinctly different
    fingerprint (a new hover-position group); see the `_CORRELATION_THRESHOLD` docstring
    for the empirical validation.
    """
    cv2 = _cv2()
    cap = cv2.VideoCapture(str(clip))
    try:
        if not cap.isOpened():
            raise SightingError(f"could not open clip for fingerprinting: {clip}")

        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        if frame_count <= 0:
            raise SightingError(f"clip reports zero frames: {clip}")

        n = min(_FINGERPRINT_SAMPLE_FRAMES, frame_count)
        sample_indices = sorted({int(i * frame_count / n) for i in range(n)})

        accumulator = np.zeros(
            (_FINGERPRINT_SIZE[1], _FINGERPRINT_SIZE[0]), dtype=np.float64
        )
        n_read = 0
        for idx in sample_indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            blurred = cv2.GaussianBlur(gray, _FINGERPRINT_BLUR_KERNEL, 0)
            small = cv2.resize(blurred, _FINGERPRINT_SIZE, interpolation=cv2.INTER_AREA)
            accumulator += small.astype(np.float64)
            n_read += 1

        if n_read == 0:
            raise SightingError(f"could not read any frame for fingerprinting: {clip}")

        return (accumulator / n_read).ravel()
    finally:
        cap.release()


def _normalized_cross_correlation(a: np.ndarray, b: np.ndarray) -> float:
    a = a - a.mean()
    b = b - b.mean()
    denom = float(np.sqrt((a * a).sum() * (b * b).sum()))
    if denom == 0.0:
        return 1.0 if np.array_equal(a, b) else 0.0
    return float((a * b).sum() / denom)


def _group_by_framing(
    fingerprints: dict[int, np.ndarray], *, threshold: float
) -> dict[int, str]:
    """Assign deterministic `hp-01`/`hp-02`/... IDs.

    Processes clip numbers in ascending order (never insertion order, so the result is
    stable regardless of how `fingerprints` was built) and joins each clip to the
    existing group whose running centroid (mean of every member's fingerprint so far)
    correlates highest with it, provided that best correlation is at or above
    `threshold`; otherwise it starts a new group. Comparing against a group's centroid
    rather than only its first member is what makes this robust on real footage: real
    per-clip fingerprints correlate with each other only loosely (see
    `_CORRELATION_THRESHOLD`), and averaging across a growing group cancels much of that
    noise the same way `_framing_fingerprint`'s own multi-frame average does. Groups are
    renumbered by their lowest clip number, so re-running against the same clips
    reproduces identical IDs every time.
    """
    groups: list[list[int]] = []
    sums: list[np.ndarray] = []
    counts: list[int] = []

    for n in sorted(fingerprints):
        fp = fingerprints[n]
        best_gi: int | None = None
        best_score = threshold
        for gi in range(len(groups)):
            centroid = sums[gi] / counts[gi]
            score = _normalized_cross_correlation(fp, centroid)
            if score >= best_score:
                best_score = score
                best_gi = gi
        if best_gi is not None:
            groups[best_gi].append(n)
            sums[best_gi] = sums[best_gi] + fp
            counts[best_gi] += 1
        else:
            groups.append([n])
            sums.append(fp.copy())
            counts.append(1)

    groups.sort(key=min)
    return {n: f"hp-{i:02d}" for i, members in enumerate(groups, start=1) for n in members}


def _apparent_player_heights(clip: Path) -> tuple[float, float, int]:
    """Median (p50) and 10th-percentile (p10) foreground-blob pixel height, sampled via
    `cv2.createBackgroundSubtractorMOG2` over consecutive frames (not frames scattered
    across the clip -- MOG2 needs temporal continuity to build a stable per-pixel
    background model; jumping between widely separated frame indices made every frame
    look like a scene change and produced tens of thousands of spurious detections on
    real footage). The camera is static (D-03's hover-position premise), so surviving
    foreground blobs are moving players.

    An initial `_MOG2_WARMUP_FRAMES` are fed to the subtractor but never scored, giving
    the background model time to converge. Frames are blurred before `.apply()` and
    components are filtered by area, border-touching, height (scoreboard/stand
    artifacts), fill ratio ("extent" -- rejects thin field-line/compression-edge
    streaks) and aspect ratio (rejects further streak-shaped noise) -- all four are
    needed on real footage; see `_MIN_BLOB_AREA_PX`'s module-level comment for the
    validation against the 2026-05-16 pilot session.

    Returns `(0.0, 0.0, 0)` when no clean sample survives -- an honest empty result, not
    a fabricated measurement. The third element is the number of frames analyzed
    (post-warmup), not the number of blob observations -- matching the "sampled frames"
    phrasing `sight_session` writes into each row's `notes`.
    """
    cv2 = _cv2()
    cap = cv2.VideoCapture(str(clip))
    try:
        if not cap.isOpened():
            raise SightingError(f"could not open clip for size measurement: {clip}")

        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        if frame_count <= 0:
            return (0.0, 0.0, 0)

        window = min(_MOG2_WINDOW_FRAMES, frame_count)
        warmup = min(_MOG2_WARMUP_FRAMES, window // 3)

        subtractor = cv2.createBackgroundSubtractorMOG2(
            detectShadows=False,
            varThreshold=_MOG2_VAR_THRESHOLD,
            history=_MOG2_HISTORY,
        )
        open_kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, _MOG2_OPEN_KERNEL_SIZE)

        heights: list[float] = []
        frame_height: int | None = None
        frames_analyzed = 0

        for i in range(window):
            ok, frame = cap.read()
            if not ok or frame is None:
                break
            if frame_height is None:
                frame_height = frame.shape[0]

            blurred = cv2.GaussianBlur(frame, _MOG2_BLUR_KERNEL, 0)
            mask = subtractor.apply(blurred)
            if i < warmup:
                continue
            frames_analyzed += 1

            mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, open_kernel)
            n_components, _labels, stats, _centroids = cv2.connectedComponentsWithStats(
                mask, connectivity=8
            )
            for comp in range(1, n_components):
                x, y, w, h, area = stats[comp]
                if area < _MIN_BLOB_AREA_PX:
                    continue
                if x <= 0 or y <= 0 or (x + w) >= mask.shape[1] or (y + h) >= mask.shape[0]:
                    continue  # touches the frame border
                if frame_height and h > frame_height / 4:
                    continue  # scoreboard/stand artifact, not a player
                if area / float(w * h) < _MIN_BLOB_EXTENT:
                    continue  # thin line/streak, not a solid blob
                if w > _MAX_BLOB_ASPECT_RATIO * h or h > _MAX_BLOB_ASPECT_RATIO * w:
                    continue  # further streak-shaped-noise rejection
                heights.append(float(h))

        if not heights:
            # frames_analyzed is still honest here (frames were processed, just no
            # blob survived the filters) -- callers use it to distinguish "measured
            # zero" from "clip had zero frames".
            return (0.0, 0.0, frames_analyzed)

        arr = np.array(heights)
        p10 = float(np.percentile(arr, 10))
        p50 = float(np.percentile(arr, 50))
        return (p10, p50, frames_analyzed)
    finally:
        cap.release()


def _parse_resolution(resolution: str) -> tuple[int, int]:
    try:
        w_str, h_str = resolution.lower().split("x")
        return int(w_str), int(h_str)
    except (ValueError, AttributeError):
        return (0, 0)


def _classify_tier(resolution: str, apparent_player_px_p50: float) -> str:
    """Map a clip to docs/capture-protocol.md's `Ideal`/`Brauchbar`/`Unbrauchbar` tier
    vocabulary using the inventory `resolution` and the measured apparent player size as
    the altitude proxy (RESEARCH-facing altitude is not logged per clip; apparent size is
    what the capture-protocol table's own altitude rows describe measuring for).

    Resolution alone settles `Ideal` (>= 4K long edge) and `Brauchbar` (>= 2560x1440,
    per the table's explicit rows). At 1920x1080 or below, the table names `Unbrauchbar`
    only for altitudes over ~40 m; apparent player height under 20 px is this module's
    < 40 m proxy threshold (the same threshold `recommend_inference_settings` uses for
    the SAHI decision), so a small apparent size at 1080p-or-below is `Unbrauchbar` and
    everything else at that resolution is `Brauchbar`.
    """
    width, height = _parse_resolution(resolution)
    long_edge = max(width, height)

    if long_edge >= 3840:
        return _TIER_IDEAL
    if long_edge >= 2560:
        return _TIER_BRAUCHBAR
    if apparent_player_px_p50 and apparent_player_px_p50 < 20:
        return _TIER_UNBRAUCHBAR
    return _TIER_BRAUCHBAR


def _repo_relative(path: Path, repo_root: Path) -> str:
    # Deliberately not `path.resolve()`: `data/video/` legitimately contains symlinks
    # (footage synced in from elsewhere on disk), and resolving through one would
    # produce a path outside `repo_root` for an otherwise perfectly repo-relative
    # clip -- see the matching note in cv/frames.py::clip_paths.
    return path.relative_to(repo_root).as_posix()


def _write_hover_positions_csv(rows: list[ClipSighting], path: Path) -> None:
    """Write `data/reference/hover_positions.csv` atomically: a `.tmp` sibling, then
    `os.replace` (T-2.1-10), matching `pipeline._atomic_write_parquet`'s discipline.
    """
    df = pl.DataFrame(
        {
            "clip_number": [r.clip_number for r in rows],
            "clip_path": [r.clip_path for r in rows],
            "hover_position_id": [r.hover_position_id for r in rows],
            "apparent_player_px_p10": [r.apparent_player_px_p10 for r in rows],
            "apparent_player_px_p50": [r.apparent_player_px_p50 for r in rows],
            "tier": [r.tier for r in rows],
            "notes": [r.notes for r in rows],
        },
        schema={
            "clip_number": pl.Int64,
            "clip_path": pl.Utf8,
            "hover_position_id": pl.Utf8,
            "apparent_player_px_p10": pl.Float64,
            "apparent_player_px_p50": pl.Float64,
            "tier": pl.Utf8,
            "notes": pl.Utf8,
        },
    ).sort("clip_number")

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        df.write_csv(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def sight_session(config: Config, session_id: str, *, out_csv: Path | None = None) -> SightingResult:
    """Run the sighting pass over every clip registered for `session_id`, writing one
    `ClipSighting` row per clip to `out_csv` (defaulting to a config-derived path).
    """
    from flag_football_ep.cv.frames import clip_number, clip_paths

    clips = clip_paths(config, session_id)
    out_path = out_csv or config.reference.hover_positions
    repo_root = config.paths.data_root.parent.resolve()
    resolutions = _inventory_resolutions(config, session_id)

    notices: list[str] = [
        f"hover-position grouping used a normalized cross-correlation threshold of "
        f"{_CORRELATION_THRESHOLD}"
    ]

    clip_by_number: dict[int, Path] = {clip_number(clip): clip for clip in clips}
    fingerprints: dict[int, np.ndarray] = {
        n: _framing_fingerprint(clip) for n, clip in clip_by_number.items()
    }
    group_ids = _group_by_framing(fingerprints, threshold=_CORRELATION_THRESHOLD)

    rows: list[ClipSighting] = []
    for n in sorted(clip_by_number):
        clip = clip_by_number[n]
        p10, p50, n_samples = _apparent_player_heights(clip)
        if p10 == 0.0 and p50 == 0.0:
            notices.append(f"clip {n}: no moving-blob samples recovered by MOG2")

        resolution = resolutions.get(n, "")
        tier = _classify_tier(resolution, p50)

        rows.append(
            ClipSighting(
                clip_number=n,
                clip_path=_repo_relative(clip, repo_root),
                hover_position_id=group_ids[n],
                apparent_player_px_p50=p50,
                apparent_player_px_p10=p10,
                tier=tier,
                notes=(
                    f"MOG2 background-subtraction estimate over {n_samples} sampled "
                    "frames -- Richtwert, kein Messprotokoll"
                ),
            )
        )

    _write_hover_positions_csv(rows, out_path)

    return SightingResult(rows=rows, notices=notices, csv_path=out_path)


def recommend_inference_settings(
    rows: list[ClipSighting], config: Config
) -> InferenceRecommendation:
    """Turn a sighting pass's rows into a single resolution/SAHI recommendation for the
    domain they cover.

    Uses the median across every row's `apparent_player_px_p50`/`_p10` (never a single
    clip) as the domain-level measurement. `resolution` stays a multiple of 224 (=
    lcm(32, 56), valid under either documented RF-DETR divisibility rule) in every band.
    """
    p50_values = [r.apparent_player_px_p50 for r in rows if r.apparent_player_px_p50 > 0]
    p10_values = [r.apparent_player_px_p10 for r in rows if r.apparent_player_px_p10 > 0]
    p50 = statistics.median(p50_values) if p50_values else 0.0
    p10 = statistics.median(p10_values) if p10_values else 0.0

    if p50 >= 40:
        resolution, sahi, band = 672, False, ">= 40 px"
    elif p50 >= 20:
        resolution, sahi, band = 896, False, "20-40 px"
    else:
        resolution, sahi, band = 896, True, "< 20 px"

    rationale = (
        f"measured apparent player height p50={p50:.1f}px, p10={p10:.1f}px across "
        f"{len(rows)} clips; band {band} -> resolution={resolution}, sahi={sahi}"
    )
    if sahi:
        rationale += (
            "; tiling is enabled from this measurement, never by default, per "
            "RESEARCH Pitfall 4 -- it costs inference time against the < 1 h/game gate "
            "(C-09)"
        )

    return InferenceRecommendation(resolution=resolution, sahi=sahi, rationale=rationale)
