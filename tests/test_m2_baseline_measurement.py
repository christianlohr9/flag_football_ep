"""Tests for `scripts/hackathon/baseline_common.py`: schema conformance, the
empty-frame fairness invariant (buffer aging must advance on every frame, not just
frames that carry a detection), the no-video-decode guarantee for motion-only
trackers, fps-derivation sanity, and the append-results idempotency contract that
lets the M2-02 baseline measurement plans re-run without duplicating rows.

Synthetic, tiny (3 clips, 12 frames, 4 boxes) -- no network, no real 61-clip data,
runtime well under ~10s.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import supervision as sv

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hackathon"))

import baseline_common as bc  # noqa: E402
from score_tracks import REQUIRED_TRACK_COLUMNS  # noqa: E402

CLIP_NUMBERS = (1, 2, 3)
N_FRAMES = 12
EMPTY_FRAME_CLIP = 1
EMPTY_FRAME_INDEX = 5


def _synthetic_detections() -> pl.DataFrame:
    """3 clips x 12 frames x 4 linearly-moving boxes (3 players, 1 referee). Clip 1
    frame 5 deliberately carries zero detections -- the buffer-aging fairness case.
    """
    rows = []
    for clip_number in CLIP_NUMBERS:
        for frame_index in range(N_FRAMES):
            if clip_number == EMPTY_FRAME_CLIP and frame_index == EMPTY_FRAME_INDEX:
                continue
            for box_index in range(4):
                x0 = 10.0 * box_index + frame_index * 2.0
                rows.append(
                    {
                        "session_id": "test-session",
                        "clip_number": clip_number,
                        "frame_index": frame_index,
                        "timestamp_s": frame_index / 30.0,
                        "det_index": box_index,
                        "class_name": "player" if box_index < 3 else "referee",
                        "confidence": 0.9,
                        "bbox_x1": x0,
                        "bbox_y1": 10.0,
                        "bbox_x2": x0 + 8.0,
                        "bbox_y2": 20.0,
                        "detector_run_id": "test-run",
                        "detected_at": "2026-01-01T00:00:00Z",
                    }
                )
    return pl.DataFrame(rows)


class _CountingStubTracker:
    """Records every `update()` call's timestamp; returns no confirmed tracks --
    only used to prove the replay loop advances the tracker on empty frames too.
    """

    def __init__(self) -> None:
        self.calls: list[float | None] = []

    def update(self, detections: sv.Detections, frame=None, timestamp: float | None = None) -> sv.Detections:
        self.calls.append(timestamp)
        result = sv.Detections.empty()
        result.tracker_id = np.array([], dtype=int)
        return result


def test_output_schema_superset_of_required() -> None:
    assert len(bc.OUTPUT_TRACK_COLUMNS) == 13
    assert bc.OUTPUT_TRACK_COLUMNS[:8] == REQUIRED_TRACK_COLUMNS
    assert set(REQUIRED_TRACK_COLUMNS) <= set(bc.OUTPUT_TRACK_COLUMNS)


def test_bytetrack_adapter_schema(tmp_path: Path) -> None:
    from trackers import ByteTrackTracker

    detections_df = _synthetic_detections()
    clip_df = detections_df.filter(pl.col("clip_number") == EMPTY_FRAME_CLIP)
    name_to_id, id_to_name = bc.class_id_codec(detections_df["class_name"].unique().to_list())
    tracker = ByteTrackTracker()

    rows = bc.run_tracker_over_clip(
        tracker,
        clip_df,
        fps=30.0,
        name_to_id=name_to_id,
        id_to_name=id_to_name,
        tracked_at="2026-01-01T00:00:00Z",
    )
    assert rows

    out_path = tmp_path / "bytetrack_tracks.parquet"
    written_df = bc.write_tracks(rows, out_path)

    assert out_path.exists()
    assert list(written_df.columns) == list(bc.OUTPUT_TRACK_COLUMNS)
    assert written_df.height > 0
    assert (written_df["track_id"] == -1).sum() == 0
    assert set(written_df["class_name"].unique().to_list()) <= {"player", "referee"}


def test_empty_frames_still_advance_the_tracker() -> None:
    detections_df = _synthetic_detections()
    clip_df = detections_df.filter(pl.col("clip_number") == EMPTY_FRAME_CLIP)
    name_to_id, id_to_name = bc.class_id_codec(detections_df["class_name"].unique().to_list())
    stub = _CountingStubTracker()

    bc.run_tracker_over_clip(
        stub,
        clip_df,
        fps=30.0,
        name_to_id=name_to_id,
        id_to_name=id_to_name,
        tracked_at="2026-01-01T00:00:00Z",
    )

    max_frame = int(clip_df["frame_index"].max())
    assert len(stub.calls) == max_frame + 1
    # Strictly increasing timestamps, including the frame with zero detections.
    assert stub.calls == sorted(stub.calls)


def test_no_video_decode() -> None:
    for name in ("baseline_common.py", "run_baseline_trackers.py"):
        path = REPO_ROOT / "scripts" / "hackathon" / name
        if not path.exists():
            pytest.skip(f"{name} does not exist yet")
        assert "cv2" not in path.read_text(encoding="utf-8"), name


def test_clip_fps_rejects_implausible_rate() -> None:
    too_slow = pl.DataFrame({"frame_index": [0, 10], "timestamp_s": [0.0, 2.0]})  # 5 fps
    with pytest.raises(ValueError):
        bc.clip_fps(too_slow)

    too_fast = pl.DataFrame({"frame_index": [0, 300], "timestamp_s": [0.0, 1.0]})  # 300 fps
    with pytest.raises(ValueError):
        bc.clip_fps(too_fast)


def test_summary_never_invents_a_human_rate(tmp_path: Path) -> None:
    report = {
        "n_clips": 2,
        "per_clip": [
            {"clip_number": 1, "n_tracks": 4, "longest_track_frac": 1.0, "n_fragments": 0, "auto_flag": "ok"},
            {
                "clip_number": 2,
                "n_tracks": 2,
                "longest_track_frac": 0.5,
                "n_fragments": 2,
                "auto_flag": "fragmented",
            },
        ],
        "auto": {"n_ok": 1, "n_clips": 2, "rate": 0.5},
    }
    tracks_df = pl.DataFrame(
        [
            {"clip_number": 1, "track_id": 1, "class_name": "player"},
            {"clip_number": 1, "track_id": 2, "class_name": "player"},
            {"clip_number": 2, "track_id": 3, "class_name": "player"},
        ]
    )
    split_csv = tmp_path / "split.csv"
    pl.DataFrame(
        [
            {
                "domain": "drone",
                "session_id": "s",
                "clip_number": 1,
                "stratum_id": "x",
                "role": "pool",
                "private_test": False,
                "frozen_at": "t",
                "seed": 1,
            },
            {
                "domain": "drone",
                "session_id": "s",
                "clip_number": 2,
                "stratum_id": "x",
                "role": "pool",
                "private_test": True,
                "frozen_at": "t",
                "seed": 1,
            },
        ]
    ).write_csv(split_csv)

    summary = bc.summarise(report, tracks_df, split_csv)

    assert "human_pass_k" not in summary
    assert "human_pass_n" not in summary
    assert summary["dev_auto_ok_n"] == 1  # only clip 1 is private_test=false
    assert summary["dev_auto_ok_k"] == 1


def test_append_results_is_idempotent(tmp_path: Path) -> None:
    summary_row = {
        "method": "bytetrack",
        "config": "defaults",
        "n_clips": 61,
        "auto_ok_k": 50,
        "auto_ok_n": 61,
        "dev_auto_ok_k": 35,
        "dev_auto_ok_n": 43,
        "median_n_tracks": 20.0,
        "median_n_player_tracks": 12.0,
        "ideal_band_k": 30,
        "ideal_band_n": 61,
        "median_n_fragments": 3.0,
        "mean_longest_track_frac": 0.9,
        "human_pass_k": None,
        "human_pass_n": None,
        "runtime_s": 12.3,
        "license": "Apache-2.0 (trackers 2.6.0)",
        "start_command": "uv run python scripts/hackathon/run_baseline_trackers.py --method bytetrack",
        "tracks_path": "data/processed/baseline-methods/bytetrack_defaults/tracks.parquet",
        "notes": "",
    }
    per_clip_rows = [
        {
            "method": "bytetrack",
            "config": "defaults",
            "clip_number": 1,
            "private_test": False,
            "n_tracks": 20,
            "n_player_tracks": 12,
            "longest_track_frac": 1.0,
            "n_fragments": 3,
            "auto_flag": "ok",
        }
    ]

    results_dir = tmp_path / "results"
    bc.append_results(summary_row, per_clip_rows, results_dir)
    bc.append_results(summary_row, per_clip_rows, results_dir)

    summary_df = pl.read_csv(results_dir / "summary.csv")
    per_clip_df = pl.read_csv(results_dir / "per_clip.csv")

    assert summary_df.height == 1
    assert per_clip_df.height == 1


def test_score_tracks_is_untouched() -> None:
    script = REPO_ROOT / "scripts" / "hackathon" / "score_tracks.py"
    try:
        result = subprocess.run(
            ["git", "show", "HEAD:scripts/hackathon/score_tracks.py"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        pytest.skip("git is not available in this environment")

    if result.returncode != 0:
        pytest.skip(f"git show failed, cannot compare against HEAD: {result.stderr}")

    assert script.read_text(encoding="utf-8") == result.stdout
