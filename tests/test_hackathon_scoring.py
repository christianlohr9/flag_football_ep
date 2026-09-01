"""Subprocess coverage for `scripts/hackathon/score_tracks.py`: schema validation,
denominator discipline (every printed rate carries `k/n`), the perfect-vs-degraded
aggregate comparison, and the flag-pull bonus precision/recall path.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "hackathon" / "score_tracks.py"

TRACK_COLUMNS = (
    "session_id",
    "clip_number",
    "frame_index",
    "track_id",
    "bbox_x1",
    "bbox_y1",
    "bbox_x2",
    "bbox_y2",
)

CLIP_NUMBERS = (1, 2, 3)
N_FRAMES = 20

RATE_RE = re.compile(r"\d+/\d+ \(\d+\.\d+%\)")


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def _write_review_csv(path: Path, verdicts: dict[int, str]) -> None:
    rows = [
        {
            "clip_number": n,
            "n_tracks": 4,
            "longest_track_frac": 1.0,
            "n_fragments": 0,
            "auto_flag": "ok",
            "verdict": verdicts[n],
            "id_switches": 0,
            "reviewer_note": "",
        }
        for n in CLIP_NUMBERS
    ]
    pl.DataFrame(rows).write_csv(path)


def _write_perfect_tracks(path: Path) -> None:
    """One track per player, each spanning every frame of every clip (no
    fragmentation, `n_tracks=4 >= _EXPECTED_MIN_TRACKS`) -- `auto_flag` should read
    `ok` for every clip.
    """
    rows = []
    for n in CLIP_NUMBERS:
        for track_id in range(4):
            for frame_index in range(N_FRAMES):
                rows.append(
                    {
                        "session_id": "test-session",
                        "clip_number": n,
                        "frame_index": frame_index,
                        "track_id": track_id,
                        "bbox_x1": 0.0,
                        "bbox_y1": 0.0,
                        "bbox_x2": 10.0,
                        "bbox_y2": 10.0,
                    }
                )
    pl.DataFrame(rows).select(list(TRACK_COLUMNS)).write_csv(path)


def _write_degraded_tracks(path: Path) -> None:
    """Eight short-lived tracks per clip, each covering only a quarter of the clip's
    frames -- every track falls under the fragment-coverage threshold, so
    `n_fragments > n_tracks/2` and `auto_flag` should read `fragmented`.
    """
    rows = []
    for n in CLIP_NUMBERS:
        for track_id in range(8):
            start = (track_id % 4) * 5
            for frame_index in range(start, start + 5):
                rows.append(
                    {
                        "session_id": "test-session",
                        "clip_number": n,
                        "frame_index": frame_index,
                        "track_id": track_id,
                        "bbox_x1": 0.0,
                        "bbox_y1": 0.0,
                        "bbox_x2": 10.0,
                        "bbox_y2": 10.0,
                    }
                )
    pl.DataFrame(rows).select(list(TRACK_COLUMNS)).write_csv(path)


def test_help_documents_all_four_flags() -> None:
    result = _run(["--help"])
    assert result.returncode == 0
    for flag in ("--tracks", "--review", "--flag-pulls", "--out"):
        assert flag in result.stdout, result.stdout


def test_missing_track_id_column_fails_naming_the_column(tmp_path: Path) -> None:
    tracks_path = tmp_path / "tracks.csv"
    df = pl.DataFrame(
        [
            {
                "session_id": "test-session",
                "clip_number": 1,
                "frame_index": 0,
                "bbox_x1": 0.0,
                "bbox_y1": 0.0,
                "bbox_x2": 10.0,
                "bbox_y2": 10.0,
            }
        ]
    )
    df.write_csv(tracks_path)

    result = _run(["--tracks", str(tracks_path)])

    assert result.returncode != 0
    assert "track_id" in result.stderr, result.stderr


def test_perfect_submission_every_rate_carries_denominator(tmp_path: Path) -> None:
    tracks_path = tmp_path / "tracks.csv"
    review_path = tmp_path / "continuity_review.csv"
    _write_perfect_tracks(tracks_path)
    _write_review_csv(review_path, {1: "pass", 2: "pass", 3: "fail"})

    result = _run(["--tracks", str(tracks_path), "--review", str(review_path)])

    assert result.returncode == 0, result.stderr
    rate_lines = [line for line in result.stdout.splitlines() if "Automatische Kontinuitaet" in line]
    assert rate_lines and RATE_RE.search(rate_lines[0]), result.stdout
    reference_lines = [line for line in result.stdout.splitlines() if "Referenz-Baseline" in line]
    assert reference_lines and RATE_RE.search(reference_lines[0]), result.stdout


def test_degraded_submission_scores_worse_than_perfect_on_aggregate(tmp_path: Path) -> None:
    review_path = tmp_path / "continuity_review.csv"
    _write_review_csv(review_path, {1: "pass", 2: "pass", 3: "fail"})

    perfect_tracks = tmp_path / "perfect.csv"
    degraded_tracks = tmp_path / "degraded.csv"
    _write_perfect_tracks(perfect_tracks)
    _write_degraded_tracks(degraded_tracks)

    perfect_out = tmp_path / "perfect_report.json"
    degraded_out = tmp_path / "degraded_report.json"

    perfect_result = _run(
        ["--tracks", str(perfect_tracks), "--review", str(review_path), "--out", str(perfect_out)]
    )
    degraded_result = _run(
        ["--tracks", str(degraded_tracks), "--review", str(review_path), "--out", str(degraded_out)]
    )

    assert perfect_result.returncode == 0, perfect_result.stderr
    assert degraded_result.returncode == 0, degraded_result.stderr

    perfect_report = json.loads(perfect_out.read_text(encoding="utf-8"))
    degraded_report = json.loads(degraded_out.read_text(encoding="utf-8"))

    assert perfect_report["auto"]["n_ok"] == 3
    assert degraded_report["auto"]["n_ok"] == 0
    assert degraded_report["auto"]["rate"] < perfect_report["auto"]["rate"]


def test_flag_pull_precision_recall_path(tmp_path: Path) -> None:
    review_path = tmp_path / "continuity_review.csv"
    _write_review_csv(review_path, {1: "pass", 2: "pass", 3: "fail"})

    ground_truth_path = tmp_path / "flag_pull_events.csv"
    pl.DataFrame(
        [
            {
                "clip_number": 1,
                "outcome": "pull",
                "pull_time_s": 5.0,
                "carrier_track_id": 1,
                "puller_track_id": "2",
                "notes": "",
            },
            {
                "clip_number": 2,
                "outcome": "pull",
                "pull_time_s": 7.2,
                "carrier_track_id": 3,
                "puller_track_id": "0",
                "notes": "",
            },
            {
                "clip_number": 3,
                "outcome": "incomplete",
                "pull_time_s": None,
                "carrier_track_id": None,
                "puller_track_id": "",
                "notes": "",
            },
        ]
    ).write_csv(ground_truth_path)

    predicted_path = tmp_path / "predicted_pulls.csv"
    pl.DataFrame(
        [
            # within +-0.5s of clip 1's ground truth pull -> hit
            {
                "clip_number": 1,
                "outcome": "pull",
                "pull_time_s": 5.2,
                "carrier_track_id": 1,
                "puller_track_id": "2",
                "notes": "",
            },
            # no matching ground-truth pull anywhere near this time -> false positive
            {
                "clip_number": 2,
                "outcome": "pull",
                "pull_time_s": 1.0,
                "carrier_track_id": 3,
                "puller_track_id": "0",
                "notes": "",
            },
        ]
    ).write_csv(predicted_path)

    tracks_path = tmp_path / "tracks.csv"
    _write_perfect_tracks(tracks_path)

    result = _run(
        [
            "--tracks",
            str(tracks_path),
            "--review",
            str(review_path),
            "--flag-pulls",
            str(predicted_path),
        ]
    )

    assert result.returncode == 0, result.stderr
    precision_lines = [line for line in result.stdout.splitlines() if "Flag-Pull Precision" in line]
    recall_lines = [line for line in result.stdout.splitlines() if "Flag-Pull Recall" in line]
    assert precision_lines and RATE_RE.search(precision_lines[0]), result.stdout
    assert recall_lines and RATE_RE.search(recall_lines[0]), result.stdout
    # tp=1 (clip 1), fp=1 (clip 2's prediction has no time-matching truth), fn=1 (clip 2's truth unmatched)
    assert "1/2" in precision_lines[0]
    assert "1/2" in recall_lines[0]
