"""Tests for flag_football_ep.ingest.sportapp.

`tests/fixtures/sportapp/match-drives_TEST001.json` + `match-v1_TEST001.json` is a
hand-built snapshot pair: two drives (four offensive plays ending in a touchdown, then a
PAT drive with the PAT itself plus a no-play kickoff placeholder), six plays total. All
I/O is against the fixture directory on disk -- no network call is made anywhere in this
module, matching `ingest/sportapp.py`'s "no requests import" invariant.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.canonical import CANONICAL_COLUMNS
from flag_football_ep.ingest.sportapp import (
    MissingDrivesArray,
    _extract_players_from_summary,
    add_event_columns,
    clean_play_ids,
    clean_yardage,
    correct_posteam,
    flatten_plays,
    ingest_snapshots,
    load_snapshot,
)
from flag_football_ep.reference import UnmappedTeamError

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sportapp"

TEAM_MAPPING = pl.DataFrame(
    {
        "source": ["sportapp", "sportapp"],
        "source_team": ["101", "102"],
        "canonical_team": ["HOM", "AWY"],
    }
)


def _load_test001() -> tuple[dict, dict]:
    return load_snapshot(
        FIXTURE_DIR / "match-drives_TEST001.json",
        FIXTURE_DIR / "match-v1_TEST001.json",
    )


def _ingest_test001() -> pl.DataFrame:
    results = ingest_snapshots(FIXTURE_DIR, TEAM_MAPPING, game_ids=["TEST001"])
    game_id, df, notices = results[0]
    assert notices == []
    return df


# --- load_snapshot -----------------------------------------------------------


def test_load_snapshot_reads_both_files():
    drives_data, match_data = _load_test001()
    assert isinstance(drives_data, dict)
    assert isinstance(match_data, dict)
    assert match_data["home"]["id"] == "101"


def test_load_snapshot_raises_naming_missing_drives_path(tmp_path):
    match_path = tmp_path / "match-v1_X.json"
    match_path.write_text("{}")
    with pytest.raises(FileNotFoundError) as excinfo:
        load_snapshot(tmp_path / "match-drives_X.json", match_path)
    assert "match-drives_X.json" in str(excinfo.value)


def test_load_snapshot_raises_naming_missing_match_path(tmp_path):
    drives_path = tmp_path / "match-drives_X.json"
    drives_path.write_text('{"drives": []}')
    with pytest.raises(FileNotFoundError) as excinfo:
        load_snapshot(drives_path, tmp_path / "match-v1_X.json")
    assert "match-v1_X.json" in str(excinfo.value)


# --- flatten_plays -------------------------------------------------------------


def test_flatten_plays_returns_one_row_per_play():
    drives_data, match_data = _load_test001()
    df = flatten_plays(drives_data, match_data, "TEST001")
    assert df.height == 6


def test_flatten_plays_derives_half_drive_play_ids_from_nesting():
    drives_data, match_data = _load_test001()
    df = flatten_plays(drives_data, match_data, "TEST001")
    assert df["half"].to_list() == [1, 1, 1, 1, 1, 1]
    assert df["drive_id_half"].to_list() == [1, 1, 1, 1, 2, 2]
    assert df["play_id_drive"].to_list() == [1, 2, 3, 4, 1, 2]


def test_flatten_plays_carries_match_and_play_metadata():
    drives_data, match_data = _load_test001()
    df = flatten_plays(drives_data, match_data, "TEST001")
    row = df.row(0, named=True)
    assert row["season"] == "2026"
    assert row["competition_id"] == 42
    assert row["competition_name"] == "TestCup"
    assert row["competition_league"] == "Group"
    assert row["gender"] == 42
    assert row["home_team"] == "101"
    assert row["away_team"] == "102"
    assert row["down"] == 1
    assert row["down_desc"] == "1st"
    assert row["start_yard_line"] == 20
    assert row["end_yard_line"] == 25


def test_flatten_plays_raises_missing_drives_array_on_dict_without_key():
    with pytest.raises(MissingDrivesArray):
        flatten_plays({"error": "not found"}, {"home": {"id": "1"}, "away": {"id": "2"}}, "X")


def test_flatten_plays_raises_missing_drives_array_on_non_list_non_dict():
    with pytest.raises(MissingDrivesArray):
        flatten_plays(None, {}, "X")


def test_flatten_plays_accepts_bare_list_top_level():
    bare_list = [{"num": 0, "drives": []}]
    df = flatten_plays(bare_list, {"home": {"id": "1"}, "away": {"id": "2"}}, "X")
    assert df.height == 0


# --- clean_play_ids / add_event_columns / correct_posteam / clean_yardage -----


def test_clean_play_ids_assigns_contiguous_play_ids_and_drive_ids():
    drives_data, match_data = _load_test001()
    df = flatten_plays(drives_data, match_data, "TEST001")
    df = clean_play_ids(df)
    assert df["play_id"].to_list() == [1, 2, 3, 4, 5, 6]
    assert df["drive_id"].to_list() == [1, 1, 1, 1, 2, 2]


def test_add_event_columns_zeroes_down_on_pat_play():
    drives_data, match_data = _load_test001()
    df = flatten_plays(drives_data, match_data, "TEST001")
    df = _extract_players_from_summary(df)
    df = clean_play_ids(df)
    df = add_event_columns(df)
    pat_row = df.filter(pl.col("down_desc") == "PAT 5 yards").row(0, named=True)
    assert pat_row["down"] == 0
    assert pat_row["point_after"] == 1
    assert pat_row["one_point_conv_success"] == 1


def test_add_event_columns_marks_touchdown_from_action_title():
    drives_data, match_data = _load_test001()
    df = flatten_plays(drives_data, match_data, "TEST001")
    df = _extract_players_from_summary(df)
    df = clean_play_ids(df)
    df = add_event_columns(df)
    td_row = df.filter(pl.col("action_title") == "Touchdown").row(0, named=True)
    assert td_row["touchdown"] == 1


def test_correct_posteam_keeps_offense_when_no_defensive_touchdown():
    drives_data, match_data = _load_test001()
    df = flatten_plays(drives_data, match_data, "TEST001")
    df = _extract_players_from_summary(df)
    df = clean_play_ids(df)
    df = add_event_columns(df)
    df = correct_posteam(df)
    assert set(df["posteam"].to_list()) == {"101"}
    assert set(df["defteam"].to_list()) == {"102"}


def test_clean_yardage_stays_within_0_50():
    drives_data, match_data = _load_test001()
    df = flatten_plays(drives_data, match_data, "TEST001")
    df = _extract_players_from_summary(df)
    df = clean_play_ids(df)
    df = add_event_columns(df)
    df = correct_posteam(df)
    df = clean_yardage(df)
    for value in df["yardline_50"].to_list():
        assert 0 <= value <= 50
    for value in df["yardline_50_after"].to_list():
        assert 0 <= value <= 50


# --- ingest_snapshots ----------------------------------------------------------


def test_ingest_snapshots_output_equals_canonical_columns():
    df = _ingest_test001()
    assert df.columns == list(CANONICAL_COLUMNS)


def test_canonical_merge_schema():
    """REQ-S1-06 test-map name: sportapp rows share the canonical column set."""
    from flag_football_ep.testing import canonical_plays

    sportapp_df = _ingest_test001()
    hudl_df = canonical_plays(source="hudl")
    assert sportapp_df.columns == hudl_df.columns
    for name in sportapp_df.columns:
        assert sportapp_df.schema[name] == hudl_df.schema[name]


def test_ingest_snapshots_source_is_sportapp_and_game_id_prefixed():
    df = _ingest_test001()
    assert set(df["source"].to_list()) == {"sportapp"}
    assert set(df["game_id"].to_list()) == {"sportapp-TEST001"}
    assert set(df["source_game_id"].to_list()) == {"TEST001"}


def test_ingest_snapshots_pat_play_has_down_zero():
    df = _ingest_test001()
    pat_row = df.filter(pl.col("one_point_conv_success") == 1).row(0, named=True)
    assert pat_row["down"] == 0


def test_ingest_snapshots_touchdown_is_scored_and_credited():
    df = _ingest_test001()
    td_row = df.filter(pl.col("touchdown") == 1).row(0, named=True)
    assert td_row["scoring_play"] == 1
    assert td_row["scoring_play_team"] == "HOM"
    assert td_row["home_team_score"] == 6


def test_ingest_snapshots_yardline_50_within_range():
    df = _ingest_test001()
    for value in df["yardline_50"].to_list():
        assert 0 <= value <= 50


def test_ingest_snapshots_maps_team_labels_via_map_teams():
    df = _ingest_test001()
    assert set(df["home_team"].to_list()) == {"HOM"}
    assert set(df["away_team"].to_list()) == {"AWY"}


def test_ingest_snapshots_unmapped_team_raises():
    incomplete_mapping = pl.DataFrame(
        {"source": ["sportapp"], "source_team": ["101"], "canonical_team": ["HOM"]}
    )
    with pytest.raises(UnmappedTeamError):
        ingest_snapshots(FIXTURE_DIR, incomplete_mapping, game_ids=["TEST001"])


def test_ingest_snapshots_skips_missing_snapshot_with_notice_not_exception():
    results = ingest_snapshots(FIXTURE_DIR, TEAM_MAPPING, game_ids=["DOES-NOT-EXIST"])
    game_id, df, notices = results[0]
    assert game_id == "DOES-NOT-EXIST"
    assert df.height == 0
    assert df.columns == list(CANONICAL_COLUMNS)
    assert len(notices) == 1
    assert "DOES-NOT-EXIST" in notices[0]


def test_ingest_snapshots_missing_drives_array_skips_with_notice(tmp_path):
    (tmp_path / "match-drives_NODRIVES.json").write_text('{"error": "gone"}')
    (tmp_path / "match-v1_NODRIVES.json").write_text(
        '{"home": {"id": "101"}, "away": {"id": "102"}}'
    )

    results = ingest_snapshots(tmp_path, TEAM_MAPPING, game_ids=["NODRIVES"])
    game_id, df, notices = results[0]
    assert df.height == 0
    assert len(notices) == 1
    assert "drives" in notices[0]


def test_ingest_snapshots_one_game_missing_does_not_abort_the_rest():
    results = ingest_snapshots(
        FIXTURE_DIR, TEAM_MAPPING, game_ids=["DOES-NOT-EXIST", "TEST001"]
    )
    by_id = {game_id: (df, notices) for game_id, df, notices in results}
    assert by_id["DOES-NOT-EXIST"][0].height == 0
    assert by_id["DOES-NOT-EXIST"][1] != []
    assert by_id["TEST001"][0].height == 6
    assert by_id["TEST001"][1] == []


def test_ingest_snapshots_discovers_game_ids_from_raw_dir_when_none_given():
    results = ingest_snapshots(FIXTURE_DIR, TEAM_MAPPING, game_ids=None)
    game_ids = {game_id for game_id, _df, _notices in results}
    assert "TEST001" in game_ids


def test_ingest_snapshots_no_import_requests():
    import flag_football_ep.ingest.sportapp as mod

    assert "requests" not in dir(mod)
