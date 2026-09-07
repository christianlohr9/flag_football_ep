"""Tests for `flag_football_ep.ingest.ifaf_spot_fill_worksheets`.

Every fixture below is small and entirely synthetic (fabricated `w-xxx`/`m-xxx`
team ids and fabricated player names like "Player One"/"Spielerin Eins",
mirroring `tests/test_ingest_ifaf.py`'s own PII policy) -- no real player
names appear anywhere in this module or its tests.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import polars as pl

from flag_football_ep.ingest.ifaf_spot_fill_worksheets import (
    WORKSHEET_COLUMNS,
    build_worksheet_rows,
    find_partially_spotted_women_games,
    generate_worksheets,
)


def _play_record(sequence, half=1, down=1, ball_on=5, offense="w-usa", events=None, video_time_sec=100.0):
    return {
        "gameId": "g1",
        "sequence": sequence,
        "half": half,
        "offenseTeamId": offense,
        "down": down,
        "nullified": False,
        "events": events or [],
        "ballOn": ball_on,
        "officialScore": None,
        "videoTimeSec": video_time_sec,
        "videoTimeSource": "derived",
    }


def _ev(action: str, **kwargs) -> dict:
    return {"action": action, **kwargs}


def _write_raw_dir(
    tmp_path: Path,
    reviewer_plays_by_game: dict[str, list],
    gender_by_tournament: dict[str, str] | None = None,
    doc_video_url: str | None = "https://videos.example.test/game.mp4",
) -> Path:
    """Minimal `data/raw/ifaf/`-shaped directory: one women's tournament
    (`t-women`, `divisions: ["Women"]`) by default; `gender_by_tournament`
    lets a test also register a men's tournament to check exclusion."""
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir(parents=True, exist_ok=True)

    gender_by_tournament = gender_by_tournament or {"t-women": "Women"}

    games_meta = []
    for gid in reviewer_plays_by_game:
        games_meta.append(
            {
                "id": gid,
                "tournamentId": "t-women",
                "homeTeam": {"id": "w-usa"},
                "awayTeam": {"id": "w-ger"},
            }
        )
    (raw_dir / "games.json").write_text(json.dumps(games_meta), encoding="utf-8")

    for tid, division in gender_by_tournament.items():
        (raw_dir / f"tournament_{tid}.json").write_text(
            json.dumps(
                {
                    "id": tid,
                    "name": "Synthetic Tournament",
                    "startDate": "2026-08-13T08:00:00.000+02:00",
                    "divisions": [division],
                }
            ),
            encoding="utf-8",
        )

    for gid, plays in reviewer_plays_by_game.items():
        payload = {"plays": plays}
        if doc_video_url is not None:
            payload["videoUrl"] = doc_video_url
        (raw_dir / f"plays_{gid}.json").write_text(json.dumps(payload), encoding="utf-8")

    return raw_dir


def _team_mapping() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "source": ["ifaf", "ifaf"],
            "source_team": ["w-usa", "w-ger"],
            "canonical_team": ["USA", "GER"],
        }
    )


def test_find_partially_spotted_women_games_identifies_null_ballon(tmp_path):
    reviewer_plays = {
        "g1": [
            _play_record(10, down=1, ball_on=5),
            _play_record(20, down=2, ball_on=None),
        ]
    }
    raw_dir = _write_raw_dir(tmp_path, reviewer_plays)

    games = find_partially_spotted_women_games(raw_dir)

    assert list(games.keys()) == ["g1"]
    assert games["g1"]["canonical_game_id"] == "ifaf-g1"
    assert games["g1"]["null_ballon_count"] == 1
    assert games["g1"]["total_records"] == 2
    assert games["g1"]["with_video_url"] == 1


def test_find_partially_spotted_women_games_excludes_fully_spotted_game(tmp_path):
    reviewer_plays = {"g1": [_play_record(10, ball_on=5), _play_record(20, ball_on=11)]}
    raw_dir = _write_raw_dir(tmp_path, reviewer_plays)

    games = find_partially_spotted_women_games(raw_dir)

    assert games == {}


def test_find_partially_spotted_women_games_excludes_mens_tournament(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir(parents=True, exist_ok=True)
    games_meta = [
        {
            "id": "g1",
            "tournamentId": "t-men",
            "homeTeam": {"id": "m-usa"},
            "awayTeam": {"id": "m-ger"},
        }
    ]
    (raw_dir / "games.json").write_text(json.dumps(games_meta), encoding="utf-8")
    (raw_dir / "tournament_t-men.json").write_text(
        json.dumps(
            {
                "id": "t-men",
                "name": "Synthetic Men's Tournament",
                "startDate": "2026-08-13T08:00:00.000+02:00",
                "divisions": ["Men"],
            }
        ),
        encoding="utf-8",
    )
    (raw_dir / "plays_g1.json").write_text(
        json.dumps({"plays": [_play_record(10, ball_on=None, offense="m-usa")]}), encoding="utf-8"
    )

    games = find_partially_spotted_women_games(raw_dir)

    assert games == {}


def test_build_worksheet_rows_includes_missing_and_neighbour_real_rows(tmp_path):
    reviewer_plays = {
        "g1": [
            _play_record(10, down=1, ball_on=5),  # real, becomes a neighbour row
            _play_record(20, down=2, ball_on=None),  # missing
            _play_record(30, down=3, ball_on=None),  # missing
            _play_record(40, down=1, ball_on=31),  # real, becomes a neighbour row
            _play_record(50, down=2, ball_on=35),  # real, far from any gap -- excluded
        ]
    }
    raw_dir = _write_raw_dir(tmp_path, reviewer_plays)

    rows = build_worksheet_rows(raw_dir, "g1", {"w-usa": "USA"}, {})

    sequences = [r["sequence"] for r in rows]
    assert sequences == ["10", "20", "30", "40"]

    statuses = {r["sequence"]: r["spot_status"] for r in rows}
    assert statuses == {"10": "real", "20": "missing", "30": "missing", "40": "real"}

    missing_rows = [r for r in rows if r["spot_status"] == "missing"]
    assert all(r["ballOn"] == "" for r in missing_rows)
    assert all(r["prev_ballOn"] == 5 for r in missing_rows)

    real_rows = {r["sequence"]: r for r in rows if r["spot_status"] == "real"}
    assert real_rows["10"]["ballOn"] == 5
    assert real_rows["40"]["ballOn"] == 31

    assert rows[0]["offense_team"] == "USA"
    assert set(rows[0].keys()) == set(WORKSHEET_COLUMNS)


def test_build_worksheet_rows_play_id_matches_canonical_numbering(tmp_path):
    reviewer_plays = {
        "g1": [
            _play_record(10, ball_on=5),
            _play_record(20, ball_on=None),
        ]
    }
    raw_dir = _write_raw_dir(tmp_path, reviewer_plays)

    rows = build_worksheet_rows(raw_dir, "g1", {}, {})

    assert [r["play_id"] for r in rows] == [1, 2]


def test_build_worksheet_rows_no_pii_when_player_names_empty(tmp_path):
    reviewer_plays = {
        "g1": [
            _play_record(
                10,
                ball_on=None,
                events=[
                    _ev("PASS", playerId="w-usa-p1", intendedReceiverId="w-usa-p2"),
                    _ev("COMPLETE", playerId="w-usa-p2"),
                ],
            )
        ]
    }
    raw_dir = _write_raw_dir(tmp_path, reviewer_plays)

    rows = build_worksheet_rows(raw_dir, "g1", {}, {})

    assert rows[0]["passer"] is None
    assert rows[0]["receiver"] is None


def test_generate_worksheets_is_idempotent_and_preserves_typed_values(tmp_path):
    reviewer_plays = {"g1": [_play_record(10, ball_on=5), _play_record(20, ball_on=None)]}
    raw_dir = _write_raw_dir(tmp_path, reviewer_plays)
    worksheet_dir = tmp_path / "worksheets"

    report = generate_worksheets(raw_dir, worksheet_dir, _team_mapping())
    assert report["g1"]["null_ballon_count"] == 1
    worksheet_path = Path(report["g1"]["worksheet_path"])
    assert worksheet_path.exists()

    # Simulate the owner typing a value in.
    rows = list(csv.DictReader(worksheet_path.open(encoding="utf-8")))
    for row in rows:
        if row["sequence"] == "20":
            row["ballOn"] = "11"
            row["note"] = "read off video"
    with worksheet_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(WORKSHEET_COLUMNS), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    # Regenerate -- must not clobber the typed-in value.
    generate_worksheets(raw_dir, worksheet_dir, _team_mapping())

    rows_after = list(csv.DictReader(worksheet_path.open(encoding="utf-8")))
    filled = next(r for r in rows_after if r["sequence"] == "20")
    assert filled["ballOn"] == "11"
    assert filled["note"] == "read off video"


def test_generate_worksheets_csv_is_comma_lf_no_semicolons(tmp_path):
    reviewer_plays = {"g1": [_play_record(10, ball_on=5), _play_record(20, ball_on=None)]}
    raw_dir = _write_raw_dir(tmp_path, reviewer_plays)
    worksheet_dir = tmp_path / "worksheets"

    report = generate_worksheets(raw_dir, worksheet_dir, _team_mapping())
    raw_bytes = Path(report["g1"]["worksheet_path"]).read_bytes()

    assert b"\r\n" not in raw_bytes
    header = raw_bytes.split(b"\n", 1)[0]
    assert b";" not in header
