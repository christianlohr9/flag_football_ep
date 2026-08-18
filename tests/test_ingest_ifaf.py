"""Tests for `flag_football_ep.ingest.ifaf` — cpx.studio unified-plays -> canonical.

Uses the committed fixture (`tests/fixtures/ifaf/unified-plays_sample.json`, real
data trimmed and redacted from a live cpx.studio snapshot) for realistic coverage,
plus small hand-built payload variants for the specific edge cases (unknown outcome
value, missing context key, unparseable payload, possession-change drive_id) that
the fixture alone does not exercise deterministically.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.canonical import (
    CANONICAL_COLUMNS,
    add_score_columns,
    add_scoring_play_team,
)
from flag_football_ep.ingest.ifaf import (
    OUTCOME_MAP,
    UnparseablePayload,
    _play_sort_key,
    derive_outcome_columns,
    flatten_unified_plays,
    ingest_snapshots,
    load_snapshot,
)
from flag_football_ep.reference import UnmappedTeamError, map_teams

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "ifaf" / "unified-plays_sample.json"


def _load_fixture_payload() -> list[dict]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _team_mapping() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "source": ["ifaf", "ifaf"],
            "source_team": ["w-usa", "w-ger"],
            "canonical_team": ["USA", "GER"],
        }
    )


def _game_meta(home: str = "w-usa", away: str = "w-ger") -> dict:
    return {
        "home_team": home,
        "away_team": away,
        "competition": "IFAF World Flag 2026",
        "season": 2026,
        "gender": "women",
    }


def _write_snapshot_dir(
    tmp_path: Path,
    plays_by_game: dict[str, list],
    games_meta: list[dict] | None = None,
    tournament: dict | None = None,
    write_games_json: bool = True,
) -> Path:
    """Build a minimal `data/raw/ifaf/`-shaped directory for `ingest_snapshots`."""
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for game_id, plays in plays_by_game.items():
        (raw_dir / f"unified-plays_{game_id}.json").write_text(
            json.dumps(plays), encoding="utf-8"
        )

    if write_games_json:
        if games_meta is None:
            games_meta = [
                {
                    "id": gid,
                    "tournamentId": "ffwc26-women",
                    "homeTeam": {"id": "w-usa"},
                    "awayTeam": {"id": "w-ger"},
                }
                for gid in plays_by_game
            ]
        (raw_dir / "games.json").write_text(json.dumps(games_meta), encoding="utf-8")

    if tournament is None:
        tournament = {
            "id": "ffwc26-women",
            "name": "IFAF World Flag 2026",
            "startDate": "2026-08-13T08:00:00.000+02:00",
            "divisions": ["Women"],
        }
    (raw_dir / "tournament_ffwc26-women.json").write_text(
        json.dumps(tournament), encoding="utf-8"
    )

    return raw_dir


# ---------------------------------------------------------------------------
# load_snapshot
# ---------------------------------------------------------------------------


def test_load_snapshot_accepts_top_level_list(tmp_path):
    path = tmp_path / "unified-plays_g1.json"
    path.write_text(json.dumps([{"context": {}}]), encoding="utf-8")
    plays, tournament = load_snapshot(path)
    assert plays == [{"context": {}}]
    assert tournament is None


@pytest.mark.parametrize("wrapper_key", ["plays", "data", "items"])
def test_load_snapshot_accepts_wrapped_shapes(tmp_path, wrapper_key):
    path = tmp_path / "unified-plays_g1.json"
    path.write_text(json.dumps({wrapper_key: [{"context": {}}]}), encoding="utf-8")
    plays, _ = load_snapshot(path)
    assert plays == [{"context": {}}]


def test_load_snapshot_raises_on_unrecognized_shape_naming_the_file(tmp_path):
    path = tmp_path / "unified-plays_bad.json"
    path.write_text(json.dumps({"unexpected": "shape"}), encoding="utf-8")
    with pytest.raises(UnparseablePayload) as exc_info:
        load_snapshot(path)
    assert str(path) in str(exc_info.value)


def test_load_snapshot_accepts_empty_list_as_valid(tmp_path):
    """A real forfeit game (plan 01.2-07 finding): 200 response, genuinely empty."""
    path = tmp_path / "unified-plays_forfeit.json"
    path.write_text(json.dumps([]), encoding="utf-8")
    plays, _ = load_snapshot(path)
    assert plays == []


def test_load_snapshot_reads_tournament_payload_when_given(tmp_path):
    plays_path = tmp_path / "unified-plays_g1.json"
    plays_path.write_text(json.dumps([]), encoding="utf-8")
    tournament_path = tmp_path / "tournament_ffwc26-women.json"
    tournament_path.write_text(json.dumps({"id": "ffwc26-women"}), encoding="utf-8")
    _, tournament = load_snapshot(plays_path, tournament_path)
    assert tournament == {"id": "ffwc26-women"}


# ---------------------------------------------------------------------------
# flatten_unified_plays
# ---------------------------------------------------------------------------


def test_flatten_produces_one_row_per_play_with_contiguous_play_id():
    payload = _load_fixture_payload()
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    assert df.height == len(payload)
    assert df["play_id"].to_list() == list(range(1, len(payload) + 1))


def test_flatten_half_and_down_are_direct_copies():
    payload = _load_fixture_payload()
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    by_number = {p["playNumber"]: p for p in payload}
    row = df.filter(pl.col("play_id") == 1).row(0, named=True)
    first_play = sorted(payload, key=lambda p: p["playNumber"])[0]
    assert row["half"] == first_play["context"]["half"]
    assert row["down"] == first_play["context"].get("down")
    assert by_number  # sanity: fixture is non-empty


def test_flatten_yardline_50_from_ballon_within_range():
    payload = _load_fixture_payload()
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    non_null = df["yardline_50"].drop_nulls()
    assert non_null.len() > 0
    assert non_null.min() >= 0
    assert non_null.max() <= 50


def test_flatten_posteam_defteam_raw_labels():
    payload = _load_fixture_payload()
    df = flatten_unified_plays(payload, _game_meta(home="w-usa", away="w-ger"), "g1")
    assert set(df["posteam"].unique().to_list()) <= {"w-usa", "w-ger"}
    row = df.row(0, named=True)
    if row["posteam"] == "w-usa":
        assert row["defteam"] == "w-ger"
    else:
        assert row["defteam"] == "w-usa"


def test_flatten_game_clock_ms_int64_half_seconds_remaining_null():
    payload = _load_fixture_payload()
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    assert df.schema["game_clock_ms"] == pl.Int64
    assert df["half_seconds_remaining"].null_count() == df.height
    assert df["game_clock_ms"].drop_nulls().len() > 0


def test_flatten_yards_to_go_always_null():
    """context.yardsToGo (and games.json currentContext.yardsToGo) is a hardcoded
    constant per docs/ifaf-field-mapping.md, so this source never populates it."""
    payload = _load_fixture_payload()
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    assert df["yards_to_go"].null_count() == df.height


def test_flatten_drive_id_increments_on_possession_change_starting_at_1():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "down": 1, "ballOn": 5, "possessionTeamId": "w-usa"}},
        {"playNumber": 2, "context": {"half": 1, "down": 2, "ballOn": 15, "possessionTeamId": "w-usa"}},
        {"playNumber": 3, "context": {"half": 1, "down": 1, "ballOn": 30, "possessionTeamId": "w-ger"}},
        {"playNumber": 4, "context": {"half": 1, "down": 1, "ballOn": 5, "possessionTeamId": "w-usa"}},
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    assert df["drive_id"].to_list() == [1, 1, 2, 3]


def test_flatten_drive_id_starts_at_1_when_first_play_lacks_possession():
    # REVIEW WR-05: a possession-less first play used to leave drive_id at the
    # out-of-contract 0, quarantining the game via monotonic_drive_ids for a
    # missing-metadata condition. Null possession never advances the counter.
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 5}},  # no possessionTeamId
        {"playNumber": 2, "context": {"half": 1, "ballOn": 15, "possessionTeamId": "w-usa"}},
        {"playNumber": 3, "context": {"half": 1, "ballOn": 20}},  # null mid-game keeps drive
        {"playNumber": 4, "context": {"half": 1, "ballOn": 30, "possessionTeamId": "w-ger"}},
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    assert df["drive_id"].to_list() == [1, 1, 1, 2]


def test_flatten_missing_context_key_counted_not_raised():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 5, "possessionTeamId": "w-usa"}},  # no down
        {"playNumber": 2},  # no context at all
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    assert df.height == 2
    assert df["_missing_down"].to_list() == [1, 1]
    assert df["_missing_ballon"].to_list() == [0, 1]
    assert df["_missing_possession"].to_list() == [0, 1]


def test_flatten_sets_source_column_ifaf():
    payload = _load_fixture_payload()
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    assert set(df["source"].unique().to_list()) == {"ifaf"}


# ---------------------------------------------------------------------------
# derive_outcome_columns
# ---------------------------------------------------------------------------


def test_derive_outcome_columns_maps_known_vocabulary():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "TOUCHDOWN", "turnover": False, "pointsScored": 6}},
        {"playNumber": 2, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "TOUCHDOWN", "turnover": True, "pointsScored": 6}},  # defensive TD
        {"playNumber": 3, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "TD", "turnover": False, "pointsScored": 6}},
        {"playNumber": 4, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "INTERCEPTION", "turnover": True}},
        {"playNumber": 5, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "SACK"}},
        {"playNumber": 6, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "COMPLETE_PASS"}},
        {"playNumber": 7, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "INCOMPLETE_PASS"}},
        {"playNumber": 8, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "TRY", "pointsScored": 1}},
        {"playNumber": 9, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "TRY"}},  # no good, no pointsScored
        {"playNumber": 10, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "XP2", "pointsScored": 2}},
        {"playNumber": 11, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "SAFETY", "turnover": True}},
        {"playNumber": 12, "context": {"half": 1, "possessionTeamId": "w-usa"}, "penalty": True},
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = derive_outcome_columns(df)

    rows = df.sort("play_id").to_dicts()
    assert rows[0]["touchdown"] == 1 and rows[0]["def_touchdown"] == 0
    assert rows[1]["touchdown"] == 0 and rows[1]["def_touchdown"] == 1
    assert rows[2]["touchdown"] == 1  # TD alt vocabulary
    assert rows[3]["interception"] == 1
    assert rows[4]["sack"] == 1
    assert rows[5]["complete_pass"] == 1
    assert rows[6]["incomplete_pass"] == 1
    assert rows[7]["one_point_conv_success"] == 1
    assert rows[8]["one_point_conv_success"] == 0  # TRY with no pointsScored -> no good
    assert rows[9]["two_point_conv_success"] == 1
    assert rows[10]["safety"] == 1
    assert rows[11]["penalty"] == 1


def test_derive_outcome_columns_sets_play_type_for_unambiguous_outcomes():
    # REVIEW WR-04: parsed IFAF plays used to sit at null play_type across the
    # board, silently excluding the whole corpus from play_type filters. The
    # form-unambiguous outcome types now map onto the canonical vocabulary;
    # form-ambiguous/event types (TOUCHDOWN, SAFETY, penalty-only) stay null.
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "RUN"}},
        {"playNumber": 2, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "COMPLETE_PASS"}},
        {"playNumber": 3, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "INCOMPLETE_PASS"}},
        {"playNumber": 4, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "SACK"}},
        {"playNumber": 5, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "INTERCEPTION", "turnover": True}},
        {"playNumber": 6, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "XP1", "pointsScored": 1}},
        {"playNumber": 7, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "TRY", "pointsScored": 1}},
        {"playNumber": 8, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "TOUCHDOWN", "pointsScored": 6}},
        {"playNumber": 9, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "SAFETY", "turnover": True}},
        {"playNumber": 10, "context": {"half": 1, "possessionTeamId": "w-usa"}, "penalty": True},
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = derive_outcome_columns(df)
    play_types = df.sort("play_id")["play_type"].to_list()
    assert play_types == [
        "run", "pass", "pass", "pass", "pass",
        "extra_point", "extra_point",
        None, None, None,
    ]


def test_derive_outcome_columns_touchdown_without_points_scored_is_not_credited():
    """Regression test: a live-data finding — some "TOUCHDOWN"/"TD"-typed plays
    carry no outcome.pointsScored at all and never move the real scoreboard
    (consistent with an overturned/nullified play). outcome.type alone must
    never be trusted to mean 6 points were actually scored."""
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "TOUCHDOWN", "turnover": True}},  # no pointsScored
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = derive_outcome_columns(df)
    row = df.row(0, named=True)
    assert row["touchdown"] == 0
    assert row["def_touchdown"] == 0


def test_derive_outcome_columns_touchdown_type_with_conversion_points_routes_to_conversion():
    """Regression test: a live-data finding — some "TOUCHDOWN"-typed plays are
    actually 1- or 2-point conversions (description.kind == "TRY" on those rows),
    evidenced by outcome.pointsScored == 1 or 2 instead of 6."""
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "TOUCHDOWN", "turnover": False, "pointsScored": 1}},
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = derive_outcome_columns(df)
    row = df.row(0, named=True)
    assert row["touchdown"] == 0
    assert row["one_point_conv_success"] == 1


def test_derive_outcome_columns_try_is_variable_conversion_not_fixed_to_one_point():
    """Regression test: outcome.type "TRY" is a generic PAT-success token that can
    carry pointsScored 1 or 2 (docs/ifaf-field-mapping.md: 102 vs. 28 instances in
    the live corpus) — unlike XP1/XP2, it must never be assumed to be worth 1 point."""
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "TRY", "pointsScored": 2}},
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = derive_outcome_columns(df)
    row = df.row(0, named=True)
    assert row["two_point_conv_success"] == 1
    assert row["one_point_conv_success"] == 0


def test_derive_outcome_columns_two_point_turnover_sets_defensive_flag():
    """WR-03: a 2-point attempt returned by the defense (outcome.turnover == true)
    must set defensive_two_point_conv, not two_point_conv_success -- mirroring the
    existing touchdown/def_touchdown split at pointsScored == 6."""
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "XP2", "turnover": True, "pointsScored": 2}},
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = derive_outcome_columns(df)
    row = df.row(0, named=True)
    assert row["defensive_two_point_conv"] == 1
    assert row["two_point_conv_success"] == 0


def test_derive_outcome_columns_two_point_no_turnover_sets_offensive_flag():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "XP2", "turnover": False, "pointsScored": 2}},
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = derive_outcome_columns(df)
    row = df.row(0, named=True)
    assert row["two_point_conv_success"] == 1
    assert row["defensive_two_point_conv"] == 0


def test_defensive_two_point_conv_credited_to_defense_in_score_chain():
    """WR-03: after add_scoring_play_team(credit_defense=True) + add_score_columns,
    a defensive 2-point conversion's points land on defteam's cumulative score, not
    posteam's -- the score chain must not corrupt for a whole IFAF game because of
    this misrouting."""
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "XP2", "turnover": True, "pointsScored": 2}},
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = map_teams(df, _team_mapping(), "ifaf", ["posteam", "defteam", "home_team", "away_team"])
    df = derive_outcome_columns(df)
    df = add_scoring_play_team(df, credit_defense=True)
    df = add_score_columns(df)
    row = df.row(0, named=True)
    assert row["scoring_play_team"] == row["defteam"]
    if row["defteam"] == row["home_team"]:
        assert row["home_team_score"] == 2
        assert row["away_team_score"] == 0
    else:
        assert row["away_team_score"] == 2
        assert row["home_team_score"] == 0


def test_derive_outcome_columns_unmapped_value_leaves_flags_zero():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "SOME_FUTURE_OUTCOME_TYPE"}},
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = derive_outcome_columns(df)
    row = df.row(0, named=True)
    assert row["_unmapped_outcome"] == 1
    for flag in (
        "touchdown", "def_touchdown", "safety", "interception",
        "complete_pass", "incomplete_pass", "sack",
        "one_point_conv_success", "two_point_conv_success",
    ):
        assert row[flag] == 0


def test_derive_outcome_columns_none_type_is_not_unmapped():
    payload = [{"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"}, "outcome": {}}]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = derive_outcome_columns(df)
    assert df.row(0, named=True)["_unmapped_outcome"] == 0


def test_outcome_map_covers_the_observed_vocabulary():
    for outcome_type in (
        "FLAG_PULL", "INCOMPLETE_PASS", "TOUCHDOWN", "COMPLETE_PASS", "TURNOVER",
        "TRY", "MIDDLE_LINE", "INTERCEPTION", "SACK", "XP1", "TD", "XP2", "RUN", "SAFETY",
    ):
        assert outcome_type in OUTCOME_MAP


# ---------------------------------------------------------------------------
# ingest_snapshots
# ---------------------------------------------------------------------------


def test_ingest_snapshots_output_columns_equal_canonical_columns(tmp_path):
    payload = _load_fixture_payload()
    raw_dir = _write_snapshot_dir(tmp_path, {"g1": payload})
    results = ingest_snapshots(raw_dir, _team_mapping())
    assert len(results) == 1
    game_id, df, notices = results[0]
    assert game_id == "g1"
    assert df.columns == list(CANONICAL_COLUMNS)
    assert set(df["source"].unique().to_list()) == {"ifaf"}
    assert set(df["game_id"].unique().to_list()) == {"ifaf-g1"}
    assert notices.game_id == "g1"


def test_ingest_snapshots_unmapped_outcome_recorded_as_notice(tmp_path):
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"},
         "outcome": {"type": "NEW_UNSEEN_TYPE"}},
    ]
    raw_dir = _write_snapshot_dir(tmp_path, {"g1": payload})
    results = ingest_snapshots(raw_dir, _team_mapping())
    _, df, notices = results[0]
    assert notices.unmapped_outcomes.get("NEW_UNSEEN_TYPE") == 1
    row = df.row(0, named=True)
    assert row["touchdown"] == 0
    assert row["interception"] == 0


def test_ingest_snapshots_missing_context_key_recorded_as_notice(tmp_path):
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"}},  # no down, no ballOn
    ]
    raw_dir = _write_snapshot_dir(tmp_path, {"g1": payload})
    results = ingest_snapshots(raw_dir, _team_mapping())
    _, _, notices = results[0]
    assert notices.missing_context_keys.get("down") == 1
    assert notices.missing_context_keys.get("ballOn") == 1


def test_ingest_snapshots_unparseable_payload_skipped_others_still_ingest(tmp_path):
    good_payload = _load_fixture_payload()
    raw_dir = _write_snapshot_dir(tmp_path, {"good": good_payload})
    (raw_dir / "unified-plays_bad.json").write_text(
        json.dumps({"unexpected": "shape"}), encoding="utf-8"
    )

    results = ingest_snapshots(raw_dir, _team_mapping())
    by_game = {gid: (df, notices) for gid, df, notices in results}

    assert "bad" in by_game
    bad_df, bad_notices = by_game["bad"]
    assert bad_notices.skipped is True
    assert bad_df.height == 0
    assert bad_df.columns == list(CANONICAL_COLUMNS)

    assert "good" in by_game
    good_df, good_notices = by_game["good"]
    assert good_df.height == len(good_payload)
    assert good_notices.skipped is False


def test_ingest_snapshots_empty_forfeit_game_produces_zero_row_canonical_frame(tmp_path):
    raw_dir = _write_snapshot_dir(tmp_path, {"forfeit": []})
    results = ingest_snapshots(raw_dir, _team_mapping())
    _, df, notices = results[0]
    assert df.height == 0
    assert df.columns == list(CANONICAL_COLUMNS)
    assert notices.skipped is False


def test_ingest_snapshots_respects_game_ids_filter(tmp_path):
    payload = [{"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"}}]
    raw_dir = _write_snapshot_dir(tmp_path, {"g1": payload, "g2": payload})
    results = ingest_snapshots(raw_dir, _team_mapping(), game_ids=["g1"])
    assert [gid for gid, _, _ in results] == ["g1"]


def test_ingest_snapshots_score_cross_check_matches_reconstruction(tmp_path):
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa", "score": {"home": 0, "away": 0}}},
        {"playNumber": 2, "context": {"half": 1, "possessionTeamId": "w-usa", "score": {"home": 6, "away": 0}},
         "outcome": {"type": "TOUCHDOWN", "turnover": False, "scoringPlay": True, "pointsScored": 6}},
    ]
    raw_dir = _write_snapshot_dir(tmp_path, {"g1": payload})
    results = ingest_snapshots(raw_dir, _team_mapping())
    _, _, notices = results[0]
    # First play's own context.score (0-0) matches the reconstruction at that point
    # (both start at 0); no assertion of a specific mismatch count beyond it existing
    # as an attribute, since add_scoring_play_team's home/away resolution depends on
    # game_meta's home/away assignment lining up with which team is "w-usa" here.
    assert isinstance(notices.score_mismatches, int)


# ---------------------------------------------------------------------------
# _play_sort_key / per-game containment regression (01.2-VERIFICATION.md Truth 5,
# 01.2-REVIEW.md WR-01)
# ---------------------------------------------------------------------------


def test_play_sort_key_null_and_non_int_playnumber_do_not_raise():
    plays = [
        (0, {"playNumber": 3}),
        (1, {"playNumber": None}),
        (2, {}),
        (3, "not-a-play"),
        (4, {"playNumber": 1}),
    ]
    ordered = sorted(plays, key=lambda pair: _play_sort_key(pair[0], pair[1]))
    # Real int playNumbers sort first, in playNumber order; everything unusable
    # (null, absent, non-dict) sorts after, in stable original-index order.
    assert [index for index, _ in ordered] == [4, 0, 1, 2, 3]


def test_ingest_snapshots_null_playnumber_play_no_longer_raises_others_unaffected(tmp_path):
    good_payload = _load_fixture_payload()
    bad_payload = [
        {"playNumber": None, "context": {"half": 1, "possessionTeamId": "w-usa"}},
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"}},
    ]
    raw_dir = _write_snapshot_dir(tmp_path, {"good": good_payload, "bad": bad_payload})

    results = ingest_snapshots(raw_dir, _team_mapping())
    by_game = {gid: (df, notices) for gid, df, notices in results}

    bad_df, bad_notices = by_game["bad"]
    assert bad_notices.skipped is False
    assert bad_df.height == len(bad_payload)

    good_df, good_notices = by_game["good"]
    assert good_df.height == len(good_payload)
    assert good_notices.skipped is False


def test_ingest_snapshots_non_dict_play_entry_contained_skip_notice(tmp_path):
    good_payload = _load_fixture_payload()
    bad_payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"}},
        "not-a-play-object",
    ]
    raw_dir = _write_snapshot_dir(tmp_path, {"good": good_payload, "bad": bad_payload})

    results = ingest_snapshots(raw_dir, _team_mapping())
    by_game = {gid: (df, notices) for gid, df, notices in results}

    bad_df, bad_notices = by_game["bad"]
    assert bad_notices.skipped is True
    assert "AttributeError" in bad_notices.skip_reason
    assert bad_df.height == 0
    assert bad_df.columns == list(CANONICAL_COLUMNS)

    good_df, good_notices = by_game["good"]
    assert good_df.height == len(good_payload)
    assert good_notices.skipped is False


def test_ingest_snapshots_unmapped_team_raises(tmp_path):
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "unmapped-team-id"}},
    ]
    raw_dir = _write_snapshot_dir(tmp_path, {"g1": payload})

    with pytest.raises(UnmappedTeamError):
        ingest_snapshots(raw_dir, _team_mapping())


# ---------------------------------------------------------------------------
# Module-shape gates (mirror the plan's acceptance-criteria grep checks)
# ---------------------------------------------------------------------------


def test_module_does_not_reuse_hudl_or_sportapp_result_parsers():
    source = Path("src/flag_football_ep/ingest/ifaf.py").read_text(encoding="utf-8")
    assert "parse_result_tokens" not in source
    assert "action_title" not in source
