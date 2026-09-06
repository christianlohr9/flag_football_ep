"""Tests for flag_football_ep.ingest.ifaf_video_marks."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from flag_football_ep.ingest.ifaf_video_marks import build_video_marks_table


def _write_plays_doc(tmp_path: Path, game_id: str, plays: list[dict], doc_video_url: str | None = None) -> None:
    payload = {"plays": plays, "videoUrl": doc_video_url}
    (tmp_path / f"plays_{game_id}.json").write_text(json.dumps(payload), encoding="utf-8")


def test_one_row_per_play_with_canonical_game_id(tmp_path):
    _write_plays_doc(
        tmp_path,
        "g1",
        [
            {
                "sequence": 10,
                "half": 1,
                "offenseTeamId": "w-pan",
                "down": 1,
                "ballOn": 5,
                "nullified": False,
                "events": [{"action": "PASS"}, {"action": "COMPLETE"}],
                "videoMark": {"videoUrl": "https://example.test/v.mp4", "videoTimeSec": 12.3},
                "videoTimeSource": "marked",
            }
        ],
    )
    df = build_video_marks_table(tmp_path)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["game_id"] == "ifaf-g1"
    assert row["source_game_id"] == "g1"
    assert row["sequence"] == 10
    assert row["outcome"] == "PASS+COMPLETE"
    assert row["video_url"] == "https://example.test/v.mp4"
    assert row["video_time_sec"] == 12.3
    assert row["video_time_source"] == "marked"


def test_falls_back_to_document_video_url_when_no_video_mark(tmp_path):
    _write_plays_doc(
        tmp_path,
        "g1",
        [
            {
                "sequence": 20,
                "half": 1,
                "offenseTeamId": "w-pan",
                "down": 2,
                "ballOn": 22,
                "nullified": False,
                "events": [{"action": "RUSH"}],
                "videoTimeSec": 87.5,
                "videoTimeSource": "derived",
            }
        ],
        doc_video_url="https://example.test/game-cam.mp4",
    )
    df = build_video_marks_table(tmp_path)
    row = df.row(0, named=True)
    assert row["video_url"] == "https://example.test/game-cam.mp4"
    assert row["video_time_sec"] == 87.5
    assert row["video_time_source"] == "derived"


def test_no_video_time_and_no_video_mark_gives_null_video_fields(tmp_path):
    _write_plays_doc(
        tmp_path,
        "g1",
        [{"sequence": 30, "half": 1, "offenseTeamId": "w-pan", "events": []}],
        doc_video_url="https://example.test/game-cam.mp4",
    )
    df = build_video_marks_table(tmp_path)
    row = df.row(0, named=True)
    assert row["video_url"] is None
    assert row["video_time_sec"] is None
    assert row["outcome"] is None


def test_penalty_event_outcome_label():
    from flag_football_ep.ingest.ifaf_video_marks import _outcome_label

    events = [{"action": "PENALTY", "offendingTeamId": "w-can", "penaltyType": "ILLEGAL_CONTACT"}]
    assert _outcome_label(events) == "PENALTY"


def test_nullified_flag_preserved(tmp_path):
    _write_plays_doc(
        tmp_path,
        "g1",
        [{"sequence": 40, "half": 1, "nullified": True, "events": []}],
    )
    df = build_video_marks_table(tmp_path)
    assert df.row(0, named=True)["nullified"] is True


def test_multiple_games_and_missing_or_empty_snapshot_skipped(tmp_path):
    _write_plays_doc(tmp_path, "g1", [{"sequence": 1, "half": 1, "events": []}])
    _write_plays_doc(tmp_path, "g2", [])  # empty plays list, e.g. reconciliation gap
    (tmp_path / "plays_bad.json").write_text("not json", encoding="utf-8")

    df = build_video_marks_table(tmp_path)
    assert df["game_id"].to_list() == ["ifaf-g1"]


def test_no_plays_files_returns_empty_typed_frame(tmp_path):
    df = build_video_marks_table(tmp_path)
    assert df.height == 0
    assert df.schema["game_id"] == pl.Utf8
    assert df.schema["video_time_sec"] == pl.Float64
