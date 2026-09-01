"""Shared measurement primitives for the M2-02 "ehrliche Baseline" plans.

Standalone module, same convention as `score_tracks.py` (not part of the installed
`flag_football_ep` package, English docstrings, `polars` for I/O). Imported by
`run_baseline_trackers.py` (motion-only methods: BoT-SORT re-score, ByteTrack, CBIoU)
and by `measure_gta.py` (plan M2-02-02) -- every function here is method-agnostic on
purpose, so a second measurement script only has to write its own adapter loop and
call the functions below for the schema, the scoring call, the split-aware
aggregation and the idempotent result append.

`score_tracks.py` stays the single unmodified scoring harness every team is measured
with: this module calls it via subprocess (`score_with_shared_harness`), never
reimplements or imports its scoring logic directly.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import polars as pl
import supervision as sv

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# `REQUIRED_TRACK_COLUMNS` is imported from `score_tracks.py` by path-based import,
# never re-typed here, so a change to the harness's schema cannot silently drift from
# what this module writes.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from score_tracks import REQUIRED_TRACK_COLUMNS, _fmt_rate  # noqa: E402

# The 8 required columns plus everything `ffep cv overlay --tracks <parquet>` reads
# for a visual spot-check (plan M2-02-03): it reads `team_id` via `row.get`, so a
# missing team column degrades to the null-team colour instead of crashing -- that is
# what makes a visual spot-check of a non-BoT-SORT method possible without adding
# team/homography work to this phase.
OUTPUT_TRACK_COLUMNS: tuple[str, ...] = (
    *REQUIRED_TRACK_COLUMNS,
    "timestamp_s",
    "class_name",
    "confidence",
    "detector_run_id",
    "tracked_at",
)

# The per-clip player-track count the challenge description already names as the
# ideal for 5v5 (`docs/hackathon-challenge-reid.md` Sec. Benchmark-Design, "Anzahl
# Spielerinnen-Tracks pro Clip (Ideal 10-14)"). This reuses an existing descriptive
# statistic -- it is NOT a new metric. The continuous metric is M2-4/METR-01 and must
# not be pre-empted here.
IDEAL_TRACK_BAND: tuple[int, int] = (10, 14)

_MIN_PLAUSIBLE_FPS = 20.0
_MAX_PLAUSIBLE_FPS = 61.0

SUMMARY_SCHEMA: dict[str, pl.PolarsDataType] = {
    "method": pl.Utf8,
    "config": pl.Utf8,
    "n_clips": pl.Int64,
    "auto_ok_k": pl.Int64,
    "auto_ok_n": pl.Int64,
    "dev_auto_ok_k": pl.Int64,
    "dev_auto_ok_n": pl.Int64,
    "median_n_tracks": pl.Float64,
    "median_n_player_tracks": pl.Float64,
    "ideal_band_k": pl.Int64,
    "ideal_band_n": pl.Int64,
    "median_n_fragments": pl.Float64,
    "mean_longest_track_frac": pl.Float64,
    "human_pass_k": pl.Int64,
    "human_pass_n": pl.Int64,
    "runtime_s": pl.Float64,
    "license": pl.Utf8,
    "start_command": pl.Utf8,
    "tracks_path": pl.Utf8,
    "notes": pl.Utf8,
}

PER_CLIP_SCHEMA: dict[str, pl.PolarsDataType] = {
    "method": pl.Utf8,
    "config": pl.Utf8,
    "clip_number": pl.Int64,
    "private_test": pl.Boolean,
    "n_tracks": pl.Int64,
    "n_player_tracks": pl.Int64,
    "longest_track_frac": pl.Float64,
    "n_fragments": pl.Int64,
    "auto_flag": pl.Utf8,
}

_SUMMARY_KEY: tuple[str, ...] = ("method", "config")
_PER_CLIP_KEY: tuple[str, ...] = ("method", "config", "clip_number")


def load_frozen_detections(detections_path: Path, freeze_json: Path) -> pl.DataFrame:
    """Read the frozen detections Parquet, asserting its `detector_run_id` matches
    `hackathon_freeze.json`'s `run_id` and that it covers all 61 clips -- every
    measured method must read the IDENTICAL frozen input (T-M2-02-01).
    """
    detections_path = Path(detections_path)
    freeze_json = Path(freeze_json)

    df = pl.read_parquet(detections_path)
    freeze = json.loads(freeze_json.read_text(encoding="utf-8"))
    expected_run_id = freeze["run_id"]

    run_ids = df["detector_run_id"].unique().to_list()
    if len(run_ids) != 1 or run_ids[0] != expected_run_id:
        print(
            "FEHLER: detector_run_id in den Detektionen "
            f"({run_ids}) stimmt nicht mit dem eingefrorenen run_id "
            f"({expected_run_id!r}) aus {freeze_json} ueberein.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    n_clips = df["clip_number"].n_unique()
    if n_clips != 61:
        print(
            f"FEHLER: Detektionen decken {n_clips} Clips ab, erwartet werden 61.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    print(f"Detektionen: {df.height} Zeilen, {n_clips} Clips, detector_run_id={expected_run_id}")
    return df


def clip_fps(clip_df: pl.DataFrame) -> tuple[float, str | None]:
    """Derive a clip's fps from `frame_index.max() / timestamp_s.max()`, falling
    back to the project default of 30.0 with an explicit notice when `timestamp_s`
    carries no usable maximum. Raises `ValueError` when the derived value falls
    outside a plausible frame-rate range -- a wrong fps silently changes
    `lost_track_buffer` semantics and would corrupt every downstream number.
    """
    ts_max = clip_df["timestamp_s"].max()
    frame_max = clip_df["frame_index"].max()

    if ts_max is not None and ts_max > 0:
        fps = round(float(frame_max) / float(ts_max), 3)
        notice = None
    else:
        fps = 30.0
        notice = (
            "timestamp_s hat kein positives Maximum (Einzelframe- oder Null-Clip) -- "
            "fps faellt auf den Projekt-Standard 30.0 zurueck"
        )

    if not (_MIN_PLAUSIBLE_FPS <= fps <= _MAX_PLAUSIBLE_FPS):
        raise ValueError(
            f"abgeleitete fps {fps} liegt ausserhalb des plausiblen Bereichs "
            f"[{_MIN_PLAUSIBLE_FPS}, {_MAX_PLAUSIBLE_FPS}] -- lost_track_buffer-"
            "Semantik wuerde sonst still verfaelscht."
        )

    return fps, notice


def class_id_codec(class_names: list[str]) -> tuple[dict[str, int], dict[int, str]]:
    """Build a bijection from the SORTED distinct `class_name` values found in the
    detections. Any bijection is safe because the same codec inverts the tracker
    output -- this avoids importing `cv.dataset.CLASS_NAMES` and coupling a
    standalone script to package internals.
    """
    distinct = sorted(set(class_names))
    name_to_id = {name: i for i, name in enumerate(distinct)}
    id_to_name = {i: name for name, i in name_to_id.items()}
    return name_to_id, id_to_name


def run_tracker_over_clip(
    tracker,
    clip_df: pl.DataFrame,
    *,
    fps: float,
    name_to_id: dict[str, int],
    id_to_name: dict[int, str],
    tracked_at: str,
) -> list[dict]:
    """Replay one clip's detections through `tracker`, shared by ByteTrack and
    CBIoU. Iterates `frame_index` from 0 to the clip's max INCLUSIVE, not only over
    frames that carry detections -- `cv/track.py` calls `update()` once per decoded
    frame, so skipping empty frames would age `lost_track_buffer` differently than
    the BoT-SORT baseline and make the comparison unfair.

    Never passes `frame=` (no video decode; CMC is BoT-SORT-only). Drops returned
    rows with `tracker_id == -1` (unconfirmed, same rule as `cv/track.py`).
    """
    clip_df = clip_df.sort("frame_index")
    session_id = clip_df["session_id"][0]
    clip_number = int(clip_df["clip_number"][0])
    detector_run_id = clip_df["detector_run_id"][0]
    max_frame = int(clip_df["frame_index"].max())

    by_frame: dict[int, pl.DataFrame] = {
        int(frame_index): frame_df
        for (frame_index,), frame_df in clip_df.group_by("frame_index", maintain_order=True)
    }

    rows: list[dict] = []
    for frame_index in range(max_frame + 1):
        frame_df = by_frame.get(frame_index)
        timestamp = frame_index / fps

        if frame_df is None or frame_df.height == 0:
            tracked = tracker.update(sv.Detections.empty(), timestamp=timestamp)
        else:
            xyxy = (
                frame_df.select("bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2")
                .to_numpy()
                .astype(np.float32)
            )
            confidence = frame_df["confidence"].to_numpy().astype(np.float32)
            class_id = np.array(
                [name_to_id[name] for name in frame_df["class_name"].to_list()], dtype=int
            )
            detections = sv.Detections(xyxy=xyxy, confidence=confidence, class_id=class_id)
            tracked = tracker.update(detections, timestamp=timestamp)

        for i in range(len(tracked)):
            track_id = int(tracked.tracker_id[i])
            if track_id == -1:
                continue
            x1, y1, x2, y2 = (float(v) for v in tracked.xyxy[i])
            class_id_val = int(tracked.class_id[i]) if tracked.class_id is not None else None
            confidence_val = float(tracked.confidence[i]) if tracked.confidence is not None else None
            rows.append(
                {
                    "session_id": session_id,
                    "clip_number": clip_number,
                    "frame_index": frame_index,
                    "track_id": track_id,
                    "bbox_x1": x1,
                    "bbox_y1": y1,
                    "bbox_x2": x2,
                    "bbox_y2": y2,
                    "timestamp_s": timestamp,
                    "class_name": id_to_name.get(class_id_val) if class_id_val is not None else None,
                    "confidence": confidence_val,
                    "detector_run_id": detector_run_id,
                    "tracked_at": tracked_at,
                }
            )

    return rows


def write_tracks(rows: list[dict], out_path: Path) -> pl.DataFrame:
    """Write `rows` as a Parquet with `OUTPUT_TRACK_COLUMNS` in that exact order.
    Raises if any required column is missing or if `rows` is empty -- an empty
    tracks file would silently fabricate a 0-track measurement.
    """
    if not rows:
        raise ValueError(f"keine Zeilen zu schreiben fuer {out_path} -- leere Tracks-Datei verweigert")

    df = pl.DataFrame(rows)
    missing = [c for c in OUTPUT_TRACK_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Zeilen fehlt Pflichtspalte(n): {missing}")
    df = df.select(list(OUTPUT_TRACK_COLUMNS))

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path)
    return df


def score_with_shared_harness(tracks_path: Path, review_csv: Path, report_path: Path) -> dict:
    """Run the unmodified `scripts/hackathon/score_tracks.py` as a subprocess (not
    an import) -- this keeps the harness exactly the artefact the teams run.
    Raises `SystemExit` on a non-zero return code with the captured stderr.
    """
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "hackathon" / "score_tracks.py"),
            "--tracks",
            str(tracks_path),
            "--review",
            str(review_csv),
            "--out",
            str(report_path),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"score_tracks.py ist fehlgeschlagen (exit {result.returncode}) fuer "
            f"{tracks_path}:\n{result.stdout}\n{result.stderr}"
        )
    return json.loads(report_path.read_text(encoding="utf-8"))


def private_test_by_clip(split_csv: Path) -> dict[int, bool]:
    """`clip_number -> private_test` for the drone domain, from
    `frozen_eval_clips.csv`. Used to build the dev-pool (private_test=false) view
    next to the full-61 view, and to populate `per_clip.csv`'s `private_test`
    column.
    """
    df = pl.read_csv(split_csv).filter(pl.col("domain") == "drone")
    return {int(row["clip_number"]): bool(row["private_test"]) for row in df.iter_rows(named=True)}


def player_track_counts(tracks_df: pl.DataFrame) -> dict[int, int]:
    """`clip_number -> distinct player track_id count`, computed HERE because the
    scoring harness's own `n_tracks` counts referees too; both numbers are kept,
    neither replaces the other.
    """
    if tracks_df.height == 0:
        return {}
    counts = (
        tracks_df.filter(pl.col("class_name") == "player")
        .group_by("clip_number")
        .agg(pl.col("track_id").n_unique().alias("n_player_tracks"))
    )
    return {int(row["clip_number"]): int(row["n_player_tracks"]) for row in counts.iter_rows(named=True)}


def summarise(report: dict, tracks_df: pl.DataFrame, split_csv: Path) -> dict:
    """Turn one `score_tracks.py` report plus its tracks into the summary row,
    computed over the full 61 clips AND over the dev pool (`domain == "drone" and
    private_test == false`, 43 clips) separately. Never contains a human pass-rate
    key -- `human_pass_k`/`human_pass_n` are set by the caller and only for a method
    that actually has a human review.
    """
    per_clip = report["per_clip"]
    private_by_clip = private_test_by_clip(split_csv)
    player_by_clip = player_track_counts(tracks_df)

    n_tracks_list: list[int] = []
    n_player_tracks_list: list[int] = []
    n_fragments_list: list[int] = []
    longest_frac_list: list[float] = []
    dev_ok = 0
    dev_n = 0
    ideal_band_k = 0

    for row in per_clip:
        clip_number = int(row["clip_number"])
        n_player = player_by_clip.get(clip_number, 0)

        n_tracks_list.append(int(row["n_tracks"]))
        n_player_tracks_list.append(n_player)
        n_fragments_list.append(int(row["n_fragments"]))
        longest_frac_list.append(float(row["longest_track_frac"]))

        if IDEAL_TRACK_BAND[0] <= n_player <= IDEAL_TRACK_BAND[1]:
            ideal_band_k += 1

        if private_by_clip.get(clip_number) is False:
            dev_n += 1
            if row["auto_flag"] == "ok":
                dev_ok += 1

    n_clips = int(report["n_clips"])

    def _median(values: list[float]) -> float:
        return float(np.median(values)) if values else 0.0

    def _mean(values: list[float]) -> float:
        return float(np.mean(values)) if values else 0.0

    return {
        "n_clips": n_clips,
        "auto_ok_k": int(report["auto"]["n_ok"]),
        "auto_ok_n": int(report["auto"]["n_clips"]),
        "dev_auto_ok_k": dev_ok,
        "dev_auto_ok_n": dev_n,
        "median_n_tracks": _median(n_tracks_list),
        "median_n_player_tracks": _median(n_player_tracks_list),
        "ideal_band_k": ideal_band_k,
        "ideal_band_n": n_clips,
        "median_n_fragments": _median(n_fragments_list),
        "mean_longest_track_frac": _mean(longest_frac_list),
    }


def fmt_rate(k: int, n: int) -> str:
    """Delegate to `score_tracks._fmt_rate` so the `k/n (p%)` convention has exactly
    one implementation in the repository.
    """
    return _fmt_rate(k, n)


def _append_csv(
    path: Path,
    schema: dict[str, pl.PolarsDataType],
    new_rows: list[dict],
    key: tuple[str, ...],
) -> None:
    columns = list(schema.keys())
    new_df = pl.DataFrame(new_rows, schema=schema).select(columns)

    if path.exists():
        existing_df = pl.read_csv(path, schema_overrides=schema).select(columns)
        key_values = new_df.select(list(key)).unique()
        # Replace (not duplicate) any existing rows sharing the same key -- a
        # re-run of the measurement is idempotent.
        existing_df = existing_df.join(key_values, on=list(key), how="anti")
        combined = pl.concat([existing_df, new_df], how="vertical")
    else:
        combined = new_df

    combined = combined.sort(list(key))

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        combined.write_csv(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def append_results(summary_row: dict, per_clip_rows: list[dict], results_dir: Path) -> None:
    """Append `summary_row` to `summary.csv` and `per_clip_rows` to `per_clip.csv`
    under `results_dir`, creating them with a header when absent, and replacing (not
    duplicating) any existing rows with the same `(method, config)` /
    `(method, config, clip_number)` key so a re-run is idempotent.
    """
    results_dir = Path(results_dir)
    _append_csv(results_dir / "summary.csv", SUMMARY_SCHEMA, [summary_row], _SUMMARY_KEY)
    if per_clip_rows:
        _append_csv(results_dir / "per_clip.csv", PER_CLIP_SCHEMA, per_clip_rows, _PER_CLIP_KEY)


__all__ = [
    "OUTPUT_TRACK_COLUMNS",
    "IDEAL_TRACK_BAND",
    "SUMMARY_SCHEMA",
    "PER_CLIP_SCHEMA",
    "load_frozen_detections",
    "clip_fps",
    "class_id_codec",
    "run_tracker_over_clip",
    "write_tracks",
    "score_with_shared_harness",
    "private_test_by_clip",
    "player_track_counts",
    "summarise",
    "fmt_rate",
    "append_results",
]
