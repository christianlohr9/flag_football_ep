"""Tests for flag_football_ep.ingest.sportapp.

`tests/fixtures/sportapp/match-drives_TEST001.json` + `match-v1_TEST001.json` is a
hand-built snapshot pair: two drives (four offensive plays ending in a touchdown, then a
PAT drive with the PAT itself plus a no-play kickoff placeholder), six plays total. All
I/O is against the fixture directory on disk -- no network call is made anywhere in this
module, matching `ingest/sportapp.py`'s "no requests import" invariant.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.canonical import CANONICAL_COLUMNS, make_game_id
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
    read_mutated_sportapp_snapshot,
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


def test_ingest_snapshots_rush_play_emits_canonical_run_not_rush():
    """WR-11: sportapp's own "rush" outcome literal must not leak into the
    canonical play_type column -- it must be normalized to "run" like every
    other source."""
    df = _ingest_test001()
    assert "run" in df["play_type"].to_list()
    assert "rush" not in df["play_type"].to_list()


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


# --- Task 2 (plan 20): non-strict yardage casts and per-game containment ------------


def _write_bad_target_snapshot(tmp_path: Path, game_id: str) -> None:
    """Copy the TEST001 fixture pair into tmp_path under a new game id, with the
    first play's `target` replaced by a non-numeric value that isn't the "G"
    sentinel, an empty string, or null -- the case `clean_yardage`'s non-strict
    cast must turn into a null instead of raising.
    """
    drives_path = FIXTURE_DIR / "match-drives_TEST001.json"
    match_path = FIXTURE_DIR / "match-v1_TEST001.json"

    drives_data = json.loads(drives_path.read_text())
    drives_data["drives"][0]["drives"][0]["eventGroups"][0]["target"] = "not-a-number"

    (tmp_path / f"match-drives_{game_id}.json").write_text(json.dumps(drives_data))
    shutil.copy(match_path, tmp_path / f"match-v1_{game_id}.json")


def test_non_numeric_target_ingests_with_null_yards_to_go_and_no_exception(tmp_path):
    _write_bad_target_snapshot(tmp_path, "TESTBADTARGET")

    results = ingest_snapshots(tmp_path, TEAM_MAPPING, game_ids=["TESTBADTARGET"])

    game_id, df, notices = results[0]
    assert notices == []
    assert df.height == 6
    assert df["yards_to_go"][0] is None


def test_polars_error_in_one_game_mutation_chain_skips_only_that_game(tmp_path, monkeypatch):
    import flag_football_ep.ingest.sportapp as mod

    # A second game id whose snapshot pair is a byte-identical copy of TEST001's --
    # only its request order (first) determines which call fails below.
    shutil.copy(FIXTURE_DIR / "match-drives_TEST001.json", tmp_path / "match-drives_TESTFAIL.json")
    shutil.copy(FIXTURE_DIR / "match-v1_TEST001.json", tmp_path / "match-v1_TESTFAIL.json")
    shutil.copy(FIXTURE_DIR / "match-drives_TEST001.json", tmp_path / "match-drives_TEST001.json")
    shutil.copy(FIXTURE_DIR / "match-v1_TEST001.json", tmp_path / "match-v1_TEST001.json")

    real_clean_yardage = mod.clean_yardage
    calls: list[int] = []

    def _flaky_clean_yardage(df):
        calls.append(1)
        if len(calls) == 1:
            raise pl.exceptions.ComputeError("simulated clean_yardage failure")
        return real_clean_yardage(df)

    monkeypatch.setattr(mod, "clean_yardage", _flaky_clean_yardage)

    results = mod.ingest_snapshots(tmp_path, TEAM_MAPPING, game_ids=["TESTFAIL", "TEST001"])
    by_id = {game_id: (df, notices) for game_id, df, notices in results}

    fail_df, fail_notices = by_id["TESTFAIL"]
    ok_df, ok_notices = by_id["TEST001"]

    assert fail_df.height == 0
    assert fail_df.columns == list(CANONICAL_COLUMNS)
    assert len(fail_notices) == 1
    assert "ComputeError" in fail_notices[0]

    assert ok_df.height == 6
    assert ok_notices == []


def test_ingest_snapshots_unmapped_team_raises_still_passes_with_containment():
    """Re-assert test_ingest_snapshots_unmapped_team_raises' invariant holds with the
    per-game try/except now wrapping the mutation chain -- UnmappedTeamError must
    still propagate, not be swallowed as a notice.
    """
    incomplete_mapping = pl.DataFrame(
        {"source": ["sportapp"], "source_team": ["101"], "canonical_team": ["HOM"]}
    )
    with pytest.raises(UnmappedTeamError):
        ingest_snapshots(FIXTURE_DIR, incomplete_mapping, game_ids=["TEST001"])


# --- read_mutated_sportapp_snapshot (WC24 grandfather path, plan 11 task 1) ----

# A minimal fixture mirroring data/raw/legacy/wc24_pbp.csv's real 65-column header,
# with team-identity columns holding numeric-looking sportapp.fi ids (the exact
# situation the schema_overrides in read_mutated_sportapp_snapshot guards against --
# without it, polars infers these as integers, not the canonical Utf8).
_WC24_FIXTURE_HEADER = (
    "index,season,competition_id,competition_name,competition_league,gender,game_id,"
    "game_type,game_group_id,game_group,half,summary,action_title,down,down_desc,"
    "down_after,down_after_desc,yards_to_go,yards_to_go_after,possession_time,"
    "home_team,away_team,home_score,away_score,stream_url,passer,receiver,rusher,"
    "tackle_player,interception_player,sack_player,play_result,penalty,safety,"
    "drive_id,play_id,half_end,play_type,complete_pass,interception,touchdown,"
    "point_after,point_after_success,def_touchdown,one_point_conv_success,"
    "two_point_conv_success,defensive_two_point_conv,scoring_play,posteam,defteam,"
    "posteam_after,defteam_after,yardline_50,yardline_50_after,yards_gained,"
    "yards_to_go_simple,first_down,scoring_play_team,home_team_points,"
    "away_team_points,home_team_score,away_team_score,posteam_score,defteam_score,"
    "score_differential"
)


def _wc24_fixture_row(**overrides) -> str:
    base = {
        "index": "1", "season": "2024", "competition_id": "2", "competition_name": "FlagWC",
        "competition_league": "World cup", "gender": "2", "game_id": "956",
        "game_type": "Group stage", "game_group_id": "47", "game_group": "Group B",
        "half": "1", "summary": "", "action_title": "Rush", "down": "1", "down_desc": "1st",
        "down_after": "2", "down_after_desc": "2nd", "yards_to_go": "10",
        "yards_to_go_after": "10", "possession_time": "", "home_team": "13", "away_team": "11",
        "home_score": "6", "away_score": "45", "stream_url": "", "passer": "", "receiver": "",
        "rusher": "", "tackle_player": "", "interception_player": "", "sack_player": "",
        "play_result": "rush", "penalty": "0", "safety": "0", "drive_id": "1", "play_id": "1",
        "half_end": "0", "play_type": "rush", "complete_pass": "0", "interception": "0",
        "touchdown": "0", "point_after": "0", "point_after_success": "0", "def_touchdown": "0",
        "one_point_conv_success": "0", "two_point_conv_success": "0",
        "defensive_two_point_conv": "0", "scoring_play": "0", "posteam": "11", "defteam": "13",
        "posteam_after": "11", "defteam_after": "13", "yardline_50": "5",
        "yardline_50_after": "5", "yards_gained": "0", "yards_to_go_simple": "2",
        "first_down": "0", "scoring_play_team": "", "home_team_points": "0",
        "away_team_points": "0", "home_team_score": "0", "away_team_score": "0",
        "posteam_score": "0", "defteam_score": "0", "score_differential": "0",
    }
    base.update(overrides)
    return ",".join(base[col] for col in _WC24_FIXTURE_HEADER.split(","))


def _write_wc24_fixture(path: Path, rows: list[str]) -> Path:
    path.write_text(_WC24_FIXTURE_HEADER + "\n" + "\n".join(rows) + "\n")
    return path


def test_read_mutated_sportapp_snapshot_conforms_to_canonical_columns(tmp_path):
    path = _write_wc24_fixture(tmp_path / "wc24_fixture.csv", [_wc24_fixture_row()])
    df = read_mutated_sportapp_snapshot(path)
    assert df.columns == list(CANONICAL_COLUMNS)


def test_read_mutated_sportapp_snapshot_source_is_legacy_sportapp(tmp_path):
    path = _write_wc24_fixture(tmp_path / "wc24_fixture.csv", [_wc24_fixture_row()])
    df = read_mutated_sportapp_snapshot(path)
    assert set(df["source"].to_list()) == {"legacy-sportapp"}


def test_read_mutated_sportapp_snapshot_game_id_matches_make_game_id(tmp_path):
    path = _write_wc24_fixture(tmp_path / "wc24_fixture.csv", [_wc24_fixture_row(game_id="956")])
    df = read_mutated_sportapp_snapshot(path)
    assert df["game_id"][0] == make_game_id("legacy-sportapp", "956")
    assert df["source_game_id"][0] == "956"


def test_read_mutated_sportapp_snapshot_team_columns_stay_utf8_not_int(tmp_path):
    path = _write_wc24_fixture(tmp_path / "wc24_fixture.csv", [_wc24_fixture_row()])
    df = read_mutated_sportapp_snapshot(path)
    for name in ("posteam", "defteam", "home_team", "away_team"):
        assert df.schema[name] == pl.Utf8


def test_read_mutated_sportapp_snapshot_normalizes_rush_play_type_to_run(tmp_path):
    """WR-11: the grandfathered WC24 CSV's pre-computed play_type column carries
    the raw sportapp "rush" value -- it must be normalized to the canonical "run"
    the same way the fresh-ingest path is, while other values pass through
    unchanged."""
    path = _write_wc24_fixture(
        tmp_path / "wc24_fixture.csv",
        [
            _wc24_fixture_row(index="1", play_type="rush"),
            _wc24_fixture_row(index="2", play_type="pass"),
        ],
    )
    df = read_mutated_sportapp_snapshot(path)
    assert df["play_type"].to_list() == ["run", "pass"]


def test_read_mutated_sportapp_snapshot_derives_sack_and_yardline_50_simple(tmp_path):
    path = _write_wc24_fixture(
        tmp_path / "wc24_fixture.csv",
        [
            _wc24_fixture_row(index="1", play_result="sack", yardline_50="10"),
            _wc24_fixture_row(index="2", play_result="rush", yardline_50="30"),
        ],
    )
    df = read_mutated_sportapp_snapshot(path)
    assert df["sack"].to_list() == [1, 0]
    assert df["yardline_50_simple"].to_list() == [0, 1]


class TestWc24LegacyFile:
    """Against the real grandfathered file (data/raw/legacy/wc24_pbp.csv), if present."""

    @pytest.fixture(scope="class")
    def wc24_path(self, repo_root: Path) -> Path:
        path = repo_root / "data" / "raw" / "legacy" / "wc24_pbp.csv"
        if not path.exists():
            pytest.skip("data/raw/legacy/wc24_pbp.csv not present")
        return path

    def test_row_count_matches_the_verified_corpus_size(self, wc24_path):
        df = read_mutated_sportapp_snapshot(wc24_path)
        # The plan's "14,546" included the header row; the verified data-row count
        # (csv.DictReader / polars height, both checked) is 14,545 -- see the plan 11
        # SUMMARY's "WC24 disposition" section.
        assert df.height == 14545

    def test_conforms_to_canonical_columns(self, wc24_path):
        df = read_mutated_sportapp_snapshot(wc24_path)
        assert df.columns == list(CANONICAL_COLUMNS)

    def test_source_is_legacy_sportapp(self, wc24_path):
        df = read_mutated_sportapp_snapshot(wc24_path)
        assert set(df["source"].to_list()) == {"legacy-sportapp"}


def test_no_ingest_module_references_the_old_wc24_root_path(repo_root: Path):
    """T-1.2-27 gate: the old `data/wc24_pbp.csv` path must not appear anywhere under
    `src/` outside a `raw/legacy` mention -- it was `git mv`-ed and is a comparison/
    grandfather fixture only, never an ingest input by its old root-level path.
    """
    src_dir = repo_root / "src"
    offending: list[str] = []
    for path in src_dir.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            if "wc24_pbp.csv" in line and "raw/legacy" not in line:
                offending.append(f"{path}: {line.strip()}")
    assert offending == [], f"old wc24_pbp.csv root path referenced outside raw/legacy: {offending}"
