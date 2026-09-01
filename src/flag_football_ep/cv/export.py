"""Tracking-Parquet -> CSV export for downstream/human consumption.

A thin transform-and-write step over `schema.conform_tracking`'s canonical frame,
mirroring `model/score.py`'s scored-output writer shape: read the tracking Parquet,
conform it to the canonical schema, write a plain CSV alongside it.

D-14: the Parquet is the only canonical tracking artifact. This export is one-way --
the CSV produced here is written for eyeballing only and is never read back into any
pipeline stage, and never becomes a second join surface: by default it is written
under the tracking directory next to its source Parquet, same stem plus `.csv`.

Implemented by plan 02.1-05, alongside `cv/schema.py`.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from flag_football_ep.cv.schema import TRACKING_COLUMNS, conform_tracking

if TYPE_CHECKING:
    from flag_football_ep.config import Config

_CSV_FLOAT_PRECISION = 4


class TrackingParquetNotFound(Exception):
    """Raised when `export_tracking_csv`'s input Parquet path does not exist."""


def export_tracking_csv(parquet_path: Path, csv_path: Path) -> Path:
    """Read the tracking Parquet at `parquet_path`, conform it to the canonical
    schema, and write it as a plain CSV to `csv_path`.

    Columns are written in `TRACKING_COLUMNS` order, floats formatted to
    `_CSV_FLOAT_PRECISION` decimal places (pixel/yard precision beyond that is noise
    and makes the file unreadable), and nulls render as an empty field, never the
    string `"null"`. Raises `TrackingParquetNotFound` naming `parquet_path` when it is
    missing.
    """
    parquet_path = Path(parquet_path)
    if not parquet_path.exists():
        raise TrackingParquetNotFound(f"Tracking Parquet not found: {parquet_path}")

    df = pl.read_parquet(parquet_path)
    df = conform_tracking(df)

    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(csv_path, float_precision=_CSV_FLOAT_PRECISION)

    assert df.columns == list(TRACKING_COLUMNS)
    return csv_path


# --- Detection/crop export (Phase 2.2, REQ-S2-03) ----------------------------------
#
# The dev-set bundle inputs Phase 2.1 never persisted: a per-frame detections Parquet
# (so teams never have to run a detector themselves) and torso crops (appearance-model
# training material), both pinned to the frozen detector run (T-2.2-23).


def export_detections_parquet(
    config: Config, session_id: str, domain: str, run_id: str | None, out_path: Path
) -> Path:
    """Run a detector over every clip registered for `session_id`/`domain` and write
    every detected box of every sampled frame as one row of a `schema.DETECTION_*`-
    conformed Parquet at `out_path`.

    `run_id=None` resolves the frozen hackathon pin (`freeze.read_freeze_pin` against
    `config.paths.reference / "hackathon_freeze.json"`) -- never `registry.
    resolve_champion` (T-2.2-24): an active-learning champion promotion must never
    silently change which detector produced the bundle's detections. `resolution`/
    `sahi` come from `config.cv` (`docs/dataset-plan.md`'s per-domain inference-settings
    table -- all three capture domains currently land in the same measured band and
    share one config value, so no per-domain override is needed here).

    Composes `detect.load_detector`/`detect.detect_video` -- every `DetectionBatch`
    (including an empty one) is flattened to zero-or-more rows; an empty frame
    contributes no rows but never aborts the export. `class_id` -> `class_name`
    validation already happened inside `detect._to_detection_batch` before a batch
    reaches this function, so an out-of-vocabulary id can never reach
    `schema.write_detections_parquet` from here. `detected_at` is stamped once, before
    any clip runs (not per row-batch), so a byte-for-byte re-export of the same run is
    diffable.
    """
    from datetime import datetime, timezone

    from flag_football_ep.cv import detect, frames, schema
    from flag_football_ep.cv.dataset import CLASS_NAMES
    from flag_football_ep.cv.freeze import read_freeze_pin

    resolved_run_id = (
        run_id
        if run_id is not None
        else read_freeze_pin(config.paths.reference / "hackathon_freeze.json").run_id
    )

    model = detect.load_detector(config, resolved_run_id)
    clip_paths = frames.clip_paths(config, session_id, domain=domain)
    detected_at = datetime.now(timezone.utc).isoformat()

    rows: list[dict] = []
    for clip_path in clip_paths:
        clip_num = frames.clip_number(clip_path)
        fps = _probe_fps(clip_path)

        detection_run = detect.detect_video(
            config, clip_path, model, resolution=config.cv.resolution, sahi=config.cv.sahi
        )
        for batch in detection_run:
            for det_index in range(len(batch.xyxy)):
                class_id = int(batch.class_id[det_index])
                x1, y1, x2, y2 = (float(v) for v in batch.xyxy[det_index])
                rows.append(
                    {
                        "session_id": session_id,
                        "clip_number": clip_num,
                        "frame_index": batch.frame_index,
                        "timestamp_s": batch.frame_index / fps,
                        "det_index": det_index,
                        "class_name": CLASS_NAMES[class_id],
                        "confidence": float(batch.confidence[det_index]),
                        "bbox_x1": x1,
                        "bbox_y1": y1,
                        "bbox_x2": x2,
                        "bbox_y2": y2,
                        "detector_run_id": resolved_run_id,
                        "detected_at": detected_at,
                    }
                )

    detections_df = pl.DataFrame(rows) if rows else schema.empty_detection_frame()
    return schema.write_detections_parquet(detections_df, Path(out_path))


def _probe_fps(clip: Path) -> float:
    """The clip's frame rate, read straight off a `cv2.VideoCapture` handle --
    mirrors `track.py::_probe_fps` exactly (same reasoning: `timestamp_s = frame_index
    / fps` needs a value that matches how `detect_video`'s own decode loop counts
    frames, not a declared inventory fps that can round differently).
    """
    import cv2

    capture = cv2.VideoCapture(str(clip))
    try:
        fps = capture.get(cv2.CAP_PROP_FPS)
    finally:
        capture.release()
    return fps if fps and fps > 0 else 30.0


# Reproduces the ~17,000-crop figure `docs/hackathon-challenge-reid.md` quotes for the
# pilot session's torso-region export: measured against the real v2 tracking Parquet
# (`2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet`, 1,508 player tracks),
# capping at 12 samples/track yields 17,638 player crops -- the closest round cap to the
# promised figure. Not a public parameter: `export_track_crops`'s signature is frozen by
# plan 02.2-05's contract guard (`tests/test_cv_contracts.py`), so the cap lives here as
# an internal constant rather than a caller-supplied keyword, mirroring plan 02.2-07's
# "force is CLI-only" precedent for the same constraint.
_EXPORT_MAX_CROPS_PER_TRACK = 12

_CROPS_INDEX_COLUMNS: tuple[str, ...] = (
    "session_id",
    "clip_number",
    "track_id",
    "frame_index",
    "team_id",
    "class_name",
    "file",
)


def export_track_crops(config: Config, session_id: str, tracks: pl.DataFrame, out_dir: Path) -> int:
    """Write one torso-region image crop per sampled tracked box in `tracks` to
    `out_dir/clip_XXX/track_YYYY/frame_ZZZZ.jpg`, plus an `index.csv` mapping every
    crop file to `(session_id, clip_number, track_id, frame_index, team_id,
    class_name)`. Returns the number of crops written.

    Reuses `teams._crop_row`/`teams._sample_frame_indices` -- the exact torso-region
    geometry and even-across-lifetime sampling `teams.extract_track_crops` itself
    uses -- rather than calling the public `extract_track_crops` directly: that
    function's return type (`dict[(clip, track) -> list[crop]]`) discards the
    per-crop `frame_index`/`team_id`/`class_name` this function's own `<behavior>`
    contract requires for `index.csv`, so composing the private geometry helpers
    (not inventing a second crop definition) is the only way to keep both the crop
    pixels and their provenance without duplicating `_crop_row`'s math.

    Only `class_name == "player"` rows are cropped. A track's first frame-sorted row
    (`docs/cv-setup.md`'s own team-assignment counting convention) screens out a
    wholly-referee track cheaply, but a handful of tracks flip class mid-track (known
    detector noise, ~55 tracks session-wide per `docs/cv-setup.md`) -- every sampled
    row is checked individually before it is queued for cropping, so a flip-noise
    track's referee-labeled frames are skipped too, never reaching `index.csv`.
    Re-running overwrites every crop file deterministically (same clip/track/frame
    always maps to the same path) and rewrites `index.csv` from scratch each call, so
    a rerun over the same `tracks` input is idempotent.
    """
    import cv2

    from flag_football_ep.cv import frames, teams
    from flag_football_ep.cv.detect import MissingClipError

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    clip_paths = {
        frames.clip_number(path): path for path in frames.clip_paths(config, session_id)
    }

    index_rows: list[dict] = []
    clip_numbers = sorted(tracks["clip_number"].unique().to_list())
    for clip_number in clip_numbers:
        clip_rows = tracks.filter(pl.col("clip_number") == clip_number)
        if clip_rows.height == 0:
            continue
        clip_path = clip_paths.get(int(clip_number))
        if clip_path is None:
            continue  # clip not registered for this session -- nothing to decode

        frame_to_rows: dict[int, list] = {}
        for (track_id,), group in clip_rows.group_by(["track_id"]):
            ordered = group.sort("frame_index")
            if ordered.row(0, named=True)["class_name"] != "player":
                continue  # referees skipped entirely
            for i in teams._sample_frame_indices(ordered.height, _EXPORT_MAX_CROPS_PER_TRACK):
                row = ordered.row(i, named=True)
                if row["class_name"] != "player":
                    # A track's class can flip mid-track (known detector noise,
                    # docs/cv-setup.md's ~55-track figure) -- the track-level check
                    # above only screens out wholly-referee tracks; this per-sample
                    # check is what actually guarantees no referee row ever reaches
                    # index.csv.
                    continue
                frame_to_rows.setdefault(int(row["frame_index"]), []).append(row)
        if not frame_to_rows:
            continue

        capture = cv2.VideoCapture(str(clip_path))
        if not capture.isOpened():
            capture.release()
            raise MissingClipError(f"could not open clip for crop extraction: {clip_path}")

        remaining = dict(frame_to_rows)
        frame_index = 0
        try:
            while remaining:
                read_ok, frame_bgr = capture.read()
                if not read_ok:
                    break
                if frame_index in remaining:
                    frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
                    for row in remaining.pop(frame_index):
                        crop = teams._crop_row(frame_rgb, row, torso=True)
                        if crop is None:
                            continue
                        track_id = int(row["track_id"])
                        crop_dir = (
                            out_dir
                            / f"clip_{int(clip_number):03d}"
                            / f"track_{track_id:04d}"
                        )
                        crop_dir.mkdir(parents=True, exist_ok=True)
                        crop_path = crop_dir / f"frame_{frame_index:05d}.jpg"
                        cv2.imwrite(str(crop_path), cv2.cvtColor(crop, cv2.COLOR_RGB2BGR))
                        index_rows.append(
                            {
                                "session_id": session_id,
                                "clip_number": int(clip_number),
                                "track_id": track_id,
                                "frame_index": frame_index,
                                "team_id": row["team_id"],
                                "class_name": row["class_name"],
                                "file": str(crop_path.relative_to(out_dir)),
                            }
                        )
                frame_index += 1
        finally:
            capture.release()

    _write_crops_index(out_dir / "index.csv", index_rows)

    detector_run_ids = (
        tracks["detector_run_id"].drop_nulls().unique().to_list() if tracks.height else []
    )
    _write_crops_meta(
        out_dir / "crops_meta.json",
        max_crops_per_track=_EXPORT_MAX_CROPS_PER_TRACK,
        n_crops=len(index_rows),
        detector_run_ids=sorted(detector_run_ids),
    )

    return len(index_rows)


def _write_crops_index(path: Path, index_rows: list[dict]) -> Path:
    """Write `index_rows` to `path` as a CSV, atomically (`.tmp` + `os.replace`,
    matching `schema.write_tracking_parquet`'s discipline), always in
    `_CROPS_INDEX_COLUMNS` order even when `index_rows` is empty.
    """
    import os

    schema_types = {
        "session_id": pl.Utf8,
        "clip_number": pl.Int32,
        "track_id": pl.Int32,
        "frame_index": pl.Int32,
        "team_id": pl.Int32,
        "class_name": pl.Utf8,
        "file": pl.Utf8,
    }
    df = (
        pl.DataFrame(index_rows, schema=schema_types)
        if index_rows
        else pl.DataFrame(schema=schema_types)
    )
    df = df.select(list(_CROPS_INDEX_COLUMNS))

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        df.write_csv(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return path


def _write_crops_meta(
    path: Path, *, max_crops_per_track: int, n_crops: int, detector_run_ids: list[str]
) -> Path:
    """Write the crop export's provenance (T-2.2-23): the effective per-track cap,
    the total crop count, the detector run id(s) the source tracks carry, and when
    this export ran. Atomic write, mirroring `track.py::_write_stage_timings`.
    """
    import json
    import os
    from datetime import datetime, timezone

    payload = {
        "max_crops_per_track": max_crops_per_track,
        "n_crops": n_crops,
        "detector_run_ids": detector_run_ids,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return path
