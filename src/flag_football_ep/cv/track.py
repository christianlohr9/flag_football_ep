"""Per-session tracking: detect + associate every clip into player tracks.

Owns the streaming, per-clip loop analogous to `pipeline.run_ingest`'s per-source
orchestration: `track_session` runs `detect.detect_video` + OC-SORT association
(`trackers`, Apache-2.0 -- the only tracking library this stack links; see C-06 in
PROJECT.md for the license policy an AGPL-licensed alternative would violate) over
every clip registered for `session_id`, one clip at a time, inside a per-clip
try/except so a single corrupt or zero-detection clip never aborts the whole session
(mirroring `run_ingest`'s "one broken source never aborts the run" discipline) -- this
is what makes the D-09 "whole game is the denominator" gate measurement possible
without one bad clip blocking the run. Anomalies are collected as `notices`, not
raised, exactly like `pipeline.run_ingest`'s `notices: list[str]` convention.

Resolves its detector exactly like `cv.detect.load_detector`: `run_id=None` goes
through `cv.registry.resolve_champion`, never "the newest FINISHED run." The single
shared MLflow tracking-store configuration in `flag_football_ep.model.mlflow_store` is
reused unchanged -- this module never constructs a second store or redirects the
ambient tracking URI itself.

Writes `data/processed/tracking/*.parquet` with the same atomic-write discipline
(`.tmp` sibling + `os.replace`) `pipeline._atomic_write_parquet` already uses for
`plays.parquet` (D-14).

Implemented by plan 02.1-12 (together with `teams.assign_teams`).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from flag_football_ep.config import Config

# Per-stage timing buckets accumulated across the whole session -- the shape
# `cv.benchmark.extrapolate_game_runtime` consumes (decode/detect are read straight off
# `detect.DetectionRun.timings()`; `track` covers both the OC-SORT `update()` call and
# `detect_video`'s own "postprocess" stage, since both turn raw model/tracker output
# into structured rows; `write` covers the final atomic Parquet write).
_STAGE_NAMES: tuple[str, ...] = ("decode", "detect", "track", "write")

# Task 1 action text: a clip whose decoded frame count disagrees with the
# inventory-declared duration by more than this many frames gets a notice -- D-09's
# whole-game denominator needs to know about a silently short clip, not just a crash.
_FRAME_COUNT_TOLERANCE = 2

# `trackers.OCSORTTracker.update` returns `tracker_id == -1` for a detection that has
# not yet been confirmed over `minimum_consecutive_frames` -- a provisional track, not
# a real one. Verified against the installed `trackers==2.6.0`: a fresh detection's
# first `update()` call always returns -1; the same track_id is assigned once confirmed.
_UNCONFIRMED_TRACK_ID = -1

# Mirrors `frames.py`'s own private `_INVENTORY_SCHEMA`/`_HOVER_POSITIONS_SCHEMA` --
# kept as separate constants rather than importing the private module attributes
# across module boundaries (same precedent as `detect.py`'s `_IMAGE_SUFFIXES`).
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

_HOVER_POSITIONS_SCHEMA: dict[str, pl.DataType] = {
    "clip_number": pl.Int64,
    "hover_position_id": pl.Utf8,
}


@dataclass
class TrackResult:
    """One `track_session` run's output: the written tracking-Parquet path, the
    number of clips/tracks produced, per-clip notices, and per-stage timing (the raw
    input to `benchmark.extrapolate_game_runtime`).
    """

    parquet_path: Path
    n_clips: int
    n_tracks: int
    notices: list[str] = field(default_factory=list)
    stage_seconds: dict[str, float] = field(default_factory=dict)


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_declared_durations(
    config: Config, session_id: str, clip_numbers: set[int]
) -> dict[int, float]:
    """Read `duration_seconds` per clip from `video_inventory.csv` (the same source
    `frames.clip_paths` reads, filtered the same way) -- used only for the frame-count
    sanity check, absent entirely -> empty dict (the check is simply skipped).
    """
    from flag_football_ep.cv.frames import clip_number as clip_number_of

    inventory_path = config.paths.reference / "video_inventory.csv"
    if not inventory_path.exists():
        return {}

    df = pl.read_csv(inventory_path, schema_overrides=_INVENTORY_SCHEMA)
    rows = df.filter((pl.col("domain") == "drone") & (pl.col("session_id") == session_id))

    durations: dict[int, float] = {}
    for row in rows.iter_rows(named=True):
        local_path = row["local_path"]
        if not local_path:
            continue
        n = clip_number_of(Path(local_path))
        if n in clip_numbers:
            durations[n] = float(row["duration_seconds"])
    return durations


def _read_hover_position_ids(config: Config, clip_numbers: set[int]) -> dict[int, str]:
    """Read `hover_position_id` per clip from `config.reference.hover_positions`, when
    that file exists. Absent entirely (sighting pass not yet run) -> empty dict, so
    every row's `hover_position_id` is simply null (a nullable column).
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


def _probe_fps(clip: Path) -> float:
    """The clip's frame rate, read straight off the same `cv2.VideoCapture` handle
    `detect.detect_video`'s decode loop uses -- `timestamp_s = frame_index / fps` needs
    a value that matches how frames were actually counted, not a declared inventory fps
    that can round differently.
    """
    import cv2

    capture = cv2.VideoCapture(str(clip))
    try:
        fps = capture.get(cv2.CAP_PROP_FPS)
    finally:
        capture.release()
    return fps if fps and fps > 0 else 30.0


def track_session(
    config: Config,
    session_id: str,
    *,
    run_id: str | None = None,
    resolution: int | None = None,
    sahi: bool | None = None,
    out_path: Path | None = None,
) -> TrackResult:
    """Detect + track every clip registered for `session_id`, writing the combined
    tracking Parquet to `out_path` (defaulting to a config-derived path under
    `config.paths.tracking`).

    `run_id=None` resolves the `champion` detector alias, exactly like
    `detect.load_detector`; the resolved run id is recorded on every output row so a
    later join always knows which detector weights produced it. `resolution`/`sahi=None`
    fall back to `config.cv.resolution`/`config.cv.sahi` (the ratified plan 02.1-11
    settings).

    Each clip is decoded and tracked inside its own try/except, mirroring
    `pipeline.run_ingest`'s per-source containment: a clip that raises, produces zero
    tracks, or decodes a frame count that disagrees with `video_inventory.csv`'s
    declared duration by more than `_FRAME_COUNT_TOLERANCE` frames appends a notice and
    the loop continues -- one broken clip never aborts the whole session (T-2.1-30), and
    every gap is named so D-09's "whole game is the denominator" measurement stays
    honest (T-2.1-31) rather than silently inflated.

    Each clip gets its own fresh `trackers.OCSORTTracker` instance (Apache-2.0, the only
    tracking library this stack links -- C-06, T-2.1-SC) -- track ids are per-clip, not
    global, because each clip is a separate play (D-02). A detection whose `tracker_id`
    is still -1 (OC-SORT's "not yet confirmed over `minimum_consecutive_frames`"
    sentinel) is dropped, not written as a row -- an unconfirmed track is not a track.
    """
    import supervision as sv
    from trackers import OCSORTTracker

    from flag_football_ep.cv import detect, frames, registry, schema
    from flag_football_ep.cv.dataset import CLASS_NAMES

    resolved_run_id = (
        run_id
        if run_id is not None
        else registry.resolve_champion(registry.detector_model_name(config), config)
    )
    model = detect.load_detector(config, resolved_run_id)

    resolved_resolution = resolution if resolution is not None else config.cv.resolution
    resolved_sahi = sahi if sahi is not None else config.cv.sahi

    clip_paths = frames.clip_paths(config, session_id)
    clip_numbers = {frames.clip_number(path) for path in clip_paths}
    declared_durations = _read_declared_durations(config, session_id, clip_numbers)
    hover_ids = _read_hover_position_ids(config, clip_numbers)

    stage_seconds: dict[str, float] = {name: 0.0 for name in _STAGE_NAMES}
    notices: list[str] = []
    rows: list[dict] = []
    tracked_at = _timestamp()

    for clip_path in clip_paths:
        clip_num = frames.clip_number(clip_path)
        try:
            tracker = OCSORTTracker()
            fps = _probe_fps(clip_path)
            detection_run = detect.detect_video(
                config, clip_path, model, resolution=resolved_resolution, sahi=resolved_sahi
            )

            clip_track_ids: set[int] = set()
            frame_count = 0
            for batch in detection_run:
                frame_count += 1
                frame_detections = sv.Detections(
                    xyxy=batch.xyxy,
                    confidence=batch.confidence,
                    class_id=batch.class_id,
                )

                track_start = time.perf_counter()
                tracked = tracker.update(frame_detections)
                stage_seconds["track"] += time.perf_counter() - track_start

                for i in range(len(tracked)):
                    track_id = int(tracked.tracker_id[i])
                    if track_id == _UNCONFIRMED_TRACK_ID:
                        continue
                    clip_track_ids.add(track_id)

                    x1, y1, x2, y2 = (float(v) for v in tracked.xyxy[i])
                    class_id = int(tracked.class_id[i])
                    rows.append(
                        {
                            "session_id": session_id,
                            "clip_number": clip_num,
                            "frame_index": batch.frame_index,
                            "timestamp_s": batch.frame_index / fps,
                            "track_id": track_id,
                            "class_name": CLASS_NAMES[class_id],
                            "confidence": float(tracked.confidence[i]),
                            "bbox_x1": x1,
                            "bbox_y1": y1,
                            "bbox_x2": x2,
                            "bbox_y2": y2,
                            "foot_x_px": (x1 + x2) / 2.0,
                            "foot_y_px": y2,
                            "hover_position_id": hover_ids.get(clip_num),
                            "detector_run_id": resolved_run_id,
                            "tracked_at": tracked_at,
                        }
                    )

            for timing in detection_run.timings():
                if timing.stage == "decode":
                    stage_seconds["decode"] += timing.seconds
                elif timing.stage == "detect":
                    stage_seconds["detect"] += timing.seconds
                elif timing.stage == "postprocess":
                    stage_seconds["track"] += timing.seconds

            if not clip_track_ids:
                notices.append(f"clip {clip_num}: produced zero tracks")

            declared = declared_durations.get(clip_num)
            if declared is not None:
                expected_frames = round(declared * fps)
                if abs(frame_count - expected_frames) > _FRAME_COUNT_TOLERANCE:
                    notices.append(
                        f"clip {clip_num}: decoded {frame_count} frames, expected "
                        f"~{expected_frames} from the inventory-declared duration "
                        f"{declared}s at {fps} fps (difference exceeds the "
                        f"{_FRAME_COUNT_TOLERANCE}-frame tolerance)"
                    )
        except Exception as exc:  # noqa: BLE001 -- per-clip containment (T-2.1-30)
            notices.append(f"clip {clip_num}: {type(exc).__name__}: {exc}")
            continue

    n_clips = len(clip_paths)
    distinct_tracks = len({(row["clip_number"], row["track_id"]) for row in rows})

    tracks_df = pl.DataFrame(rows) if rows else schema.empty_tracking_frame()

    resolved_out_path = (
        Path(out_path)
        if out_path is not None
        else config.paths.tracking / f"{session_id}_tracks.parquet"
    )
    write_start = time.perf_counter()
    written_path = schema.write_tracking_parquet(tracks_df, resolved_out_path)
    stage_seconds["write"] += time.perf_counter() - write_start

    return TrackResult(
        parquet_path=written_path,
        n_clips=n_clips,
        n_tracks=distinct_tracks,
        notices=notices,
        stage_seconds=stage_seconds,
    )
