#!/usr/bin/env python3
"""Measure BoT-SORT (re-scored, not re-run), ByteTrack and CBIoU on the frozen
detections, all through the SAME unmodified `score_tracks.py` harness, and append
one row per (method, config) to `data/reference/baseline-methods/{summary,per_clip}.csv`.

Every method reads the identical frozen detections
(`data/labels/.../bundle-inputs/detections.parquet`, `detector_run_id` asserted
against `data/reference/hackathon_freeze.json`) and is scored the identical way. The
human 15/61 pass-rate (`data/reference/continuity_review.csv`) is the verdict on the
BoT-SORT overlays -- it is filled ONLY for `--method botsort-existing`, never copied
onto ByteTrack/CBIoU rows, which have received no human review of their own.

BoT-SORT is NEVER re-run here: `--method botsort-existing` reads the existing tracks
Parquet as-is and only re-scores it with the shared harness, for comparability.

ByteTrack and CBIoU (`trackers==2.6.0`, Apache-2.0, already installed) are motion-only
in this script -- no video decode, no camera-motion compensation (CMC is
BoT-SORT-only) -- so no OpenCV import exists anywhere in this module.

Usage (one invocation reproduces exactly one result row):

    uv run python scripts/hackathon/run_baseline_trackers.py \\
        --method botsort-existing

    uv run python scripts/hackathon/run_baseline_trackers.py \\
        --method bytetrack --config baseline-matched

    uv run python scripts/hackathon/run_baseline_trackers.py \\
        --method cbiou --config defaults

Every invocation prints the current comparison table (all rows in `summary.csv` so
far) with every rate as `k/n (p%)`.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

sys.path.insert(0, str(Path(__file__).resolve().parent))
import baseline_common as bc  # noqa: E402

_LICENSE_NOTE = "Apache-2.0 (trackers 2.6.0)"

# The tuned BoT-SORT config the existing baseline Parquet was produced with
# (src/flag_football_ep/cv/track.py, gap-fix iteration, plan 02.1-12), applied to
# ByteTrack/CBIoU wherever the parameter exists on that class. Comparing a hand-tuned
# BoT-SORT against library-default competitors would be an unfair comparison in
# EITHER direction, so both `--config defaults` and `--config baseline-matched` are
# measured for both trackers.
_BASELINE_MATCHED_PARAMS: dict[str, dict[str, float | int]] = {
    "bytetrack": {
        "lost_track_buffer": 90,
        "minimum_consecutive_frames": 5,
        "minimum_iou_threshold": 0.1,
    },
    "cbiou": {
        "lost_track_buffer": 90,
        "minimum_consecutive_frames": 5,
        "minimum_iou_threshold_first_assoc": 0.1,
    },
}

_NO_HUMAN_REVIEW_NOTE = (
    "keine menschliche Review vorhanden -- Human-Urteile in continuity_review.csv "
    "beziehen sich auf die BoT-SORT-Overlays"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--method",
        required=True,
        choices=["botsort-existing", "bytetrack", "cbiou"],
    )
    parser.add_argument(
        "--config",
        choices=["baseline-matched", "defaults"],
        default="defaults",
        help="Ignored for --method botsort-existing.",
    )
    parser.add_argument(
        "--detections",
        type=Path,
        default=REPO_ROOT
        / "data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/detections.parquet",
        help="Frozen detections Parquet (ByteTrack/CBIoU input).",
    )
    parser.add_argument(
        "--tracks",
        type=Path,
        default=REPO_ROOT
        / "data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet",
        help="Existing BoT-SORT tracks Parquet (only used for --method botsort-existing).",
    )
    parser.add_argument(
        "--review", type=Path, default=REPO_ROOT / "data/reference/continuity_review.csv"
    )
    parser.add_argument(
        "--split", type=Path, default=REPO_ROOT / "data/reference/frozen_eval_clips.csv"
    )
    parser.add_argument(
        "--freeze", type=Path, default=REPO_ROOT / "data/reference/hackathon_freeze.json"
    )
    parser.add_argument(
        "--out-dir", type=Path, default=REPO_ROOT / "data/processed/baseline-methods"
    )
    parser.add_argument(
        "--results-dir", type=Path, default=REPO_ROOT / "data/reference/baseline-methods"
    )
    return parser


def _relative(path: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _build_per_clip_rows(
    method: str, config: str, report: dict, tracks_df: pl.DataFrame, split_csv: Path
) -> list[dict]:
    private_by_clip = bc.private_test_by_clip(split_csv)
    player_by_clip = bc.player_track_counts(tracks_df)

    rows = []
    for row in report["per_clip"]:
        clip_number = int(row["clip_number"])
        rows.append(
            {
                "method": method,
                "config": config,
                "clip_number": clip_number,
                "private_test": private_by_clip.get(clip_number),
                "n_tracks": int(row["n_tracks"]),
                "n_player_tracks": player_by_clip.get(clip_number, 0),
                "longest_track_frac": float(row["longest_track_frac"]),
                "n_fragments": int(row["n_fragments"]),
                "auto_flag": row["auto_flag"],
            }
        )
    return rows


def _print_comparison_table(results_dir: Path) -> None:
    summary_path = results_dir / "summary.csv"
    if not summary_path.exists():
        return

    df = pl.read_csv(summary_path, schema_overrides=bc.SUMMARY_SCHEMA)
    print("\nVergleichstabelle (automatische Kontinuitaet, Human-Referenz nur BoT-SORT):")
    for row in df.sort(["method", "config"]).iter_rows(named=True):
        label = row["method"] if not row["config"] else f"{row['method']}/{row['config']}"
        auto_rate = bc.fmt_rate(row["auto_ok_k"], row["auto_ok_n"])
        dev_rate = bc.fmt_rate(row["dev_auto_ok_k"], row["dev_auto_ok_n"])
        human = (
            bc.fmt_rate(row["human_pass_k"], row["human_pass_n"])
            if row["human_pass_k"] is not None
            else "n/a (keine Review)"
        )
        print(f"  {label:<28} auto={auto_rate:<20} dev={dev_rate:<20} human={human}")


def _measure_botsort_existing(args: argparse.Namespace) -> None:
    freeze = json.loads(args.freeze.read_text(encoding="utf-8"))
    tracks_df = pl.read_parquet(args.tracks)
    run_ids = tracks_df["detector_run_id"].unique().to_list()
    if len(run_ids) != 1 or run_ids[0] != freeze["run_id"]:
        raise SystemExit(
            "FEHLER: bestehende Tracks-Datei hat detector_run_id "
            f"{run_ids}, erwartet wird {freeze['run_id']!r} laut {args.freeze}."
        )
    print("BoT-SORT: bestehende Tracks wiederverwendet, kein erneuter Lauf")

    report_path = args.out_dir / "botsort-existing" / "report.json"
    start = time.perf_counter()
    report = bc.score_with_shared_harness(args.tracks, args.review, report_path)
    runtime_s = time.perf_counter() - start

    summary = bc.summarise(report, tracks_df, args.split)

    start_command = (
        "uv run python scripts/hackathon/run_baseline_trackers.py --method botsort-existing "
        f"--tracks {_relative(args.tracks)} --review {_relative(args.review)} "
        f"--split {_relative(args.split)} --freeze {_relative(args.freeze)}"
    )
    reproduce_score_command = (
        f"uv run python scripts/hackathon/score_tracks.py --tracks {_relative(args.tracks)} "
        f"--review {_relative(args.review)}"
    )
    notes = (
        "human_pass (Dev-Pool, private_test=false): 10/43. "
        f"Direkter score_tracks.py-Aufruf: {reproduce_score_command}"
    )

    summary_row = {
        "method": "botsort-existing",
        "config": "",
        **summary,
        "human_pass_k": 15,
        "human_pass_n": 61,
        "runtime_s": round(runtime_s, 3),
        "license": _LICENSE_NOTE,
        "start_command": start_command,
        "tracks_path": _relative(args.tracks),
        "notes": notes,
    }
    per_clip_rows = _build_per_clip_rows("botsort-existing", "", report, tracks_df, args.split)

    bc.append_results(summary_row, per_clip_rows, args.results_dir)
    print(
        f"BoT-SORT (Re-Scoring): auto={bc.fmt_rate(summary['auto_ok_k'], summary['auto_ok_n'])} "
        f"human={bc.fmt_rate(15, 61)} (Referenzwert, unveraendert von der urspruenglichen Review)"
    )
    _print_comparison_table(args.results_dir)


def _build_tracker(method: str, config: str, fps: float):
    from trackers import ByteTrackTracker, CBIoUTracker

    cls = ByteTrackTracker if method == "bytetrack" else CBIoUTracker
    kwargs: dict = {"frame_rate": fps}
    if config == "baseline-matched":
        kwargs.update(_BASELINE_MATCHED_PARAMS[method])
    return cls(**kwargs)


def _measure_tracker_method(method: str, config: str, args: argparse.Namespace) -> None:
    detections_df = bc.load_frozen_detections(args.detections, args.freeze)
    name_to_id, id_to_name = bc.class_id_codec(detections_df["class_name"].unique().to_list())
    tracked_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows: list[dict] = []
    start = time.perf_counter()
    for clip_number in sorted(detections_df["clip_number"].unique().to_list()):
        clip_df = detections_df.filter(pl.col("clip_number") == clip_number)
        fps, notice = bc.clip_fps(clip_df)
        if notice:
            print(f"Clip {clip_number}: {notice}")
        tracker = _build_tracker(method, config, fps)
        rows.extend(
            bc.run_tracker_over_clip(
                tracker,
                clip_df,
                fps=fps,
                name_to_id=name_to_id,
                id_to_name=id_to_name,
                tracked_at=tracked_at,
            )
        )
    runtime_s = time.perf_counter() - start
    print(f"{method}/{config}: {len(rows)} bestaetigte Track-Zeilen ueber 61 Clips, {runtime_s:.2f}s")

    out_path = args.out_dir / f"{method}_{config}" / "tracks.parquet"
    tracks_df = bc.write_tracks(rows, out_path)

    report_path = args.out_dir / f"{method}_{config}" / "report.json"
    report = bc.score_with_shared_harness(out_path, args.review, report_path)

    summary = bc.summarise(report, tracks_df, args.split)

    start_command = (
        f"uv run python scripts/hackathon/run_baseline_trackers.py --method {method} "
        f"--config {config} --detections {_relative(args.detections)} "
        f"--review {_relative(args.review)} --split {_relative(args.split)} "
        f"--freeze {_relative(args.freeze)} --out-dir {_relative(args.out_dir)} "
        f"--results-dir {_relative(args.results_dir)}"
    )
    params = _BASELINE_MATCHED_PARAMS.get(method, {}) if config == "baseline-matched" else {}
    notes = _NO_HUMAN_REVIEW_NOTE
    if params:
        notes += f"; baseline-matched Parameter: {params}"

    summary_row = {
        "method": method,
        "config": config,
        **summary,
        "human_pass_k": None,
        "human_pass_n": None,
        "runtime_s": round(runtime_s, 3),
        "license": _LICENSE_NOTE,
        "start_command": start_command,
        "tracks_path": _relative(out_path),
        "notes": notes,
    }
    per_clip_rows = _build_per_clip_rows(method, config, report, tracks_df, args.split)

    bc.append_results(summary_row, per_clip_rows, args.results_dir)
    _print_comparison_table(args.results_dir)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.method == "botsort-existing":
        _measure_botsort_existing(args)
    else:
        _measure_tracker_method(args.method, args.config, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
