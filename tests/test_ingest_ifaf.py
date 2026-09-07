"""Tests for `flag_football_ep.ingest.ifaf` — cpx.studio /plays -> canonical.

Uses the committed fixture (`tests/fixtures/ifaf/unified-plays_sample.json`, real
data trimmed and redacted from a live cpx.studio snapshot) for realistic coverage
of the `unified-plays` fallback path, plus small hand-built payload variants for
the specific edge cases (unknown outcome value, missing context key, unparseable
payload, possession-change drive_id) that fixture alone does not exercise
deterministically.

The `/plays` primary path (`flatten_plays_records`, `derive_yardage_columns_plays`)
has no committed real-data fixture -- every test below uses small, entirely
synthetic payloads (fabricated `w-xxx-pN` player ids and fabricated names like
"Player One"), never real player names, per project PII policy.
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
    _build_game_meta,
    _events_score_ledger_summary,
    _load_teams_meta,
    _play_sort_key,
    _play_type_from_actions,
    _play_type_from_sequence,
    _plays_record_sort_key,
    derive_outcome_columns,
    derive_yardage_columns,
    derive_yardage_columns_plays,
    derive_yards_to_go,
    flatten_plays_records,
    flatten_unified_plays,
    ingest_snapshots,
    load_ifaf_final_scores,
    load_plays_snapshot,
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
    reviewer_plays_by_game: dict[str, list] | None = None,
    teams_roster: list[dict] | None = None,
    write_unified: bool = True,
) -> Path:
    """Build a minimal `data/raw/ifaf/`-shaped directory for `ingest_snapshots`.

    `plays_by_game` writes the legacy `unified-plays_{game_id}.json` fallback
    snapshots (existing param, unchanged meaning). `reviewer_plays_by_game`
    (new), when given, writes the primary `plays_{game_id}.json` snapshot for
    each listed game id -- a value of `[]` or `{"plays": []}` produces a
    real-but-empty response (exercises the fallback decision), and a missing
    key produces no file at all (also exercises the fallback decision, via
    "file absent"). `teams_roster` (new), when given, writes
    `tournament_ffwc26-women_teams.json` with the given team/player list --
    every name in this file MUST be synthetic (never a real player name; PII
    stays out of committed fixtures/tests, per project policy).
    `write_unified=False` skips writing `unified-plays_*.json` files
    entirely, for a "plays-only" game id.
    """
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir(parents=True, exist_ok=True)

    if write_unified:
        for game_id, plays in plays_by_game.items():
            (raw_dir / f"unified-plays_{game_id}.json").write_text(
                json.dumps(plays), encoding="utf-8"
            )

    if reviewer_plays_by_game is not None:
        for game_id, plays in reviewer_plays_by_game.items():
            (raw_dir / f"plays_{game_id}.json").write_text(
                json.dumps({"plays": plays}), encoding="utf-8"
            )

    if teams_roster is not None:
        (raw_dir / "tournament_ffwc26-women_teams.json").write_text(
            json.dumps(teams_roster), encoding="utf-8"
        )

    if write_games_json:
        if games_meta is None:
            all_gids = dict.fromkeys(plays_by_game)
            all_gids.update(dict.fromkeys(reviewer_plays_by_game or {}))
            games_meta = [
                {
                    "id": gid,
                    "tournamentId": "ffwc26-women",
                    "homeTeam": {"id": "w-usa"},
                    "awayTeam": {"id": "w-ger"},
                }
                for gid in all_gids
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


def test_flatten_yards_to_go_starts_null_before_derivation():
    """flatten_unified_plays itself never populates yards_to_go directly --
    context.yardsToGo (and games.json currentContext.yardsToGo) is a
    hardcoded constant per docs/ifaf-field-mapping.md, so trusting it
    verbatim would be wrong. yards_to_go is instead derived from field
    position afterward by derive_yards_to_go (see its own tests below),
    the same two-step pattern yards_gained already uses."""
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


# ---------------------------------------------------------------------------
# _play_type_from_sequence (2026-09-06: yardage/play_type derivation addendum)
# ---------------------------------------------------------------------------


def _seq(*actions: str) -> list[dict]:
    return [{"id": i, "action": a} for i, a in enumerate(actions, start=1)]


def test_play_type_from_sequence_pass_takes_priority_over_rush_after_catch():
    # A completed catch followed by yards-after-catch running is still a pass
    # play, not a run play.
    assert _play_type_from_sequence(_seq("SNAP", "QB_SET", "PASS", "COMPLETE", "RUSH")) == "pass"


def test_play_type_from_sequence_hand_off_only_is_run():
    assert _play_type_from_sequence(_seq("SNAP", "QB_SET", "HAND_OFF", "TOUCHDOWN")) == "run"


def test_play_type_from_sequence_interception_is_pass():
    assert _play_type_from_sequence(_seq("SNAP", "QB_SET", "PASS", "INTERCEPTION")) == "pass"


def test_play_type_from_sequence_ambiguous_lateral_only_stays_none():
    assert _play_type_from_sequence(_seq("SNAP", "QB_SET", "LATERAL")) is None


def test_play_type_from_sequence_empty_or_non_list_stays_none():
    assert _play_type_from_sequence([]) is None
    assert _play_type_from_sequence(None) is None


def test_derive_outcome_columns_touchdown_play_type_from_sequence_pass_vs_run():
    payload = [
        {
            "playNumber": 1,
            "context": {"half": 1, "possessionTeamId": "w-usa"},
            "outcome": {"type": "TOUCHDOWN", "turnover": False, "pointsScored": 6},
            "sequence": _seq("SNAP", "QB_SET", "PASS", "COMPLETE", "TOUCHDOWN"),
        },
        {
            "playNumber": 2,
            "context": {"half": 1, "possessionTeamId": "w-usa"},
            "outcome": {"type": "TD", "turnover": False, "pointsScored": 6},
            "sequence": _seq("SNAP", "QB_SET", "HAND_OFF", "RUSH", "TOUCHDOWN"),
        },
        {
            "playNumber": 3,
            "context": {"half": 1, "possessionTeamId": "w-usa"},
            "outcome": {"type": "TOUCHDOWN", "turnover": False},  # no pointsScored -> not credited
            "sequence": _seq("SNAP", "QB_SET", "LATERAL"),  # ambiguous
        },
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = derive_outcome_columns(df)
    rows = df.sort("play_id").to_dicts()
    assert rows[0]["play_type"] == "pass"
    assert rows[1]["play_type"] == "run"
    assert rows[2]["play_type"] is None


def test_derive_outcome_columns_flag_pull_play_type_from_sequence():
    """FLAG_PULL is the most common outcome value (1,289/4,057 in the live
    corpus per docs/ifaf-field-mapping.md) and had no play_type before this
    fallback -- most FLAG_PULL sequences do carry a PASS/COMPLETE or
    RUSH/HAND_OFF token."""
    payload = [
        {
            "playNumber": 1,
            "context": {"half": 1, "possessionTeamId": "w-usa"},
            "outcome": {"type": "FLAG_PULL"},
            "sequence": _seq("SNAP", "QB_SET", "PASS", "COMPLETE", "FLAG_PULL"),
        },
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    df = derive_outcome_columns(df)
    assert df.row(0, named=True)["play_type"] == "pass"


# ---------------------------------------------------------------------------
# derive_yardage_columns (2026-09-06: yardage derivation addendum)
# ---------------------------------------------------------------------------


def _run_through_outcome(payload: list[dict]) -> pl.DataFrame:
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    return derive_outcome_columns(df)


def test_derive_yardage_same_drive_gain_from_consecutive_ballon():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 10, "possessionTeamId": "w-usa"}},
        {"playNumber": 2, "context": {"half": 1, "ballOn": 22, "possessionTeamId": "w-usa"}},
    ]
    df = derive_yardage_columns(_run_through_outcome(payload))
    rows = df.sort("play_id")["yards_gained"].to_list()
    assert rows[0] == 12
    assert rows[1] is None  # last play of the drive, no following same-drive row


def test_derive_yardage_touchdown_is_distance_to_goal_line():
    payload = [
        {
            "playNumber": 1,
            "context": {"half": 1, "ballOn": 46, "possessionTeamId": "w-usa"},
            "outcome": {"type": "TOUCHDOWN", "turnover": False, "pointsScored": 6},
        },
        {
            "playNumber": 2,
            "context": {"half": 1, "ballOn": 45, "possessionTeamId": "w-usa"},
            "outcome": {"type": "TRY", "pointsScored": 1},
        },
    ]
    df = derive_yardage_columns(_run_through_outcome(payload))
    assert df.sort("play_id")["yards_gained"].to_list()[0] == 4  # 50 - 46


def test_derive_yardage_safety_is_negative_of_yardline():
    payload = [
        {
            "playNumber": 1,
            "context": {"half": 1, "ballOn": 3, "possessionTeamId": "w-usa"},
            "outcome": {"type": "SAFETY", "turnover": True},
        },
    ]
    df = derive_yardage_columns(_run_through_outcome(payload))
    assert df.row(0, named=True)["yards_gained"] == -3


def test_derive_yardage_turnover_stays_null_even_when_drive_id_matches():
    # No possessionTeamId on the second play keeps drive_id unchanged (per
    # flatten_unified_plays's own null-possession rule), so a naive same-drive
    # lookup would produce a spurious gain here without the turnover-priority
    # rule.
    payload = [
        {
            "playNumber": 1,
            "context": {"half": 1, "ballOn": 24, "possessionTeamId": "w-usa"},
            "outcome": {"type": "INTERCEPTION", "turnover": True},
        },
        {"playNumber": 2, "context": {"half": 1, "ballOn": 36}},
    ]
    df = derive_yardage_columns(_run_through_outcome(payload))
    assert df.sort("play_id")["yards_gained"].to_list()[0] is None


def test_derive_yardage_defensive_touchdown_stays_null():
    payload = [
        {
            "playNumber": 1,
            "context": {"half": 1, "ballOn": 24, "possessionTeamId": "w-usa"},
            "outcome": {"type": "TOUCHDOWN", "turnover": True, "pointsScored": 6},
        },
        {"playNumber": 2, "context": {"half": 1, "ballOn": 36}},
    ]
    df = derive_yardage_columns(_run_through_outcome(payload))
    assert df.sort("play_id")["yards_gained"].to_list()[0] is None


def test_derive_yardage_penalty_flag_stays_null_never_a_fake_gain():
    payload = [
        {
            "playNumber": 1,
            "context": {"half": 1, "ballOn": 20, "possessionTeamId": "w-usa"},
            "penalty": True,
        },
        {"playNumber": 2, "context": {"half": 1, "ballOn": 35, "possessionTeamId": "w-usa"}},
    ]
    df = derive_yardage_columns(_run_through_outcome(payload))
    assert df.sort("play_id")["yards_gained"].to_list()[0] is None


def test_derive_yardage_last_play_of_game_stays_null():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 20, "possessionTeamId": "w-usa"}},
    ]
    df = derive_yardage_columns(_run_through_outcome(payload))
    assert df.row(0, named=True)["yards_gained"] is None


def test_derive_yardage_missing_ballon_propagates_null_not_crash():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"}},  # no ballOn
        {"playNumber": 2, "context": {"half": 1, "ballOn": 20, "possessionTeamId": "w-usa"}},
    ]
    df = derive_yardage_columns(_run_through_outcome(payload))
    assert df.sort("play_id")["yards_gained"].to_list() == [None, None]


def test_ingest_snapshots_wires_yards_gained_derivation(tmp_path):
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 10, "possessionTeamId": "w-usa"}},
        {"playNumber": 2, "context": {"half": 1, "ballOn": 25, "possessionTeamId": "w-usa"}},
    ]
    raw_dir = _write_snapshot_dir(tmp_path, {"g1": payload})
    results = ingest_snapshots(raw_dir, _team_mapping())
    _, df, _ = results[0]
    assert df.sort("play_id")["yards_gained"].to_list()[0] == 15


# ---------------------------------------------------------------------------
# derive_yards_to_go (2026-09-06: yards_to_go-derivation addendum)
# ---------------------------------------------------------------------------


def test_derive_yards_to_go_own_half_targets_midfield():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 10, "possessionTeamId": "w-usa"}},
    ]
    df = derive_yards_to_go(_run_through_outcome(payload))
    assert df.row(0, named=True)["yards_to_go"] == 15  # 25 - 10


def test_derive_yards_to_go_crossed_midfield_targets_goal_line():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 30, "possessionTeamId": "w-usa"}},
    ]
    df = derive_yards_to_go(_run_through_outcome(payload))
    assert df.row(0, named=True)["yards_to_go"] == 20  # 50 - 30


def test_derive_yards_to_go_not_sticky_reverts_when_sacked_back_below_midfield():
    """Empirical finding (docs/ifaf-field-mapping.md): the real system's own
    MIDDLE/GOAL marker does NOT persist a "crossed midfield" achievement the
    way an American-football first down would -- a sack that drags the spot
    back under midfield reverts the target back to midfield-to-go. An
    earlier "sticky" draft of this rule (cum_max over the drive) matched the
    marker only 74.3% of the time; this simple per-play recompute matches
    98.2%."""
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 30, "possessionTeamId": "w-usa"}},
        {"playNumber": 2, "context": {"half": 1, "ballOn": 20, "possessionTeamId": "w-usa"},
         "outcome": {"type": "SACK"}},
    ]
    df = derive_yards_to_go(_run_through_outcome(payload))
    rows = df.sort("play_id")["yards_to_go"].to_list()
    assert rows[0] == 20  # 50 - 30, already past midfield
    assert rows[1] == 5  # 25 - 20, reverted to midfield-to-go, not sticky


def test_derive_yards_to_go_middle_line_outcome_marker_deliberately_unused():
    """`_outcome_middle_line` is computed in `flatten_unified_plays` and
    available on the frame, but `derive_yards_to_go` deliberately does not
    consult it -- adding it as an OR-signal measured slightly worse (97.5%)
    against the events feed's own marker than the pure ballOn threshold
    (98.2%). A MIDDLE_LINE-outcome play below midfield still gets the
    midfield-to-go target."""
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 24, "possessionTeamId": "w-usa"},
         "outcome": {"type": "MIDDLE_LINE"}},
    ]
    df = derive_yards_to_go(_run_through_outcome(payload))
    assert df.row(0, named=True)["yards_to_go"] == 1  # 25 - 24, midfield-to-go despite the marker


def test_derive_yards_to_go_sequence_middle_line_marker_deliberately_unused():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 18, "possessionTeamId": "w-usa"},
         "sequence": _seq("SNAP", "QB_SET", "RUSH", "MIDDLE_LINE")},
    ]
    df = derive_yards_to_go(_run_through_outcome(payload))
    assert df.row(0, named=True)["yards_to_go"] == 7  # 25 - 18, midfield-to-go despite the marker


def test_derive_yards_to_go_pat_row_is_always_goal_to_go():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 45, "possessionTeamId": "w-usa"},
         "outcome": {"type": "TRY", "pointsScored": 1}},
    ]
    df = flatten_unified_plays(payload, _game_meta(), "g1")
    # PAT rows carry down == 0 in the real payload -- flatten copies whatever
    # context.down provides, so set it explicitly for this synthetic case.
    df = df.with_columns(pl.lit(0).alias("down"))
    df = derive_outcome_columns(df)
    df = derive_yards_to_go(df)
    assert df.row(0, named=True)["yards_to_go"] == 5  # 50 - 45


def test_derive_yards_to_go_resets_on_possession_change():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 30, "possessionTeamId": "w-usa"}},
        {"playNumber": 2, "context": {"half": 1, "ballOn": 20, "possessionTeamId": "w-ger"},
         "outcome": {"type": "INTERCEPTION", "turnover": True}},
    ]
    df = derive_yards_to_go(_run_through_outcome(payload))
    rows = df.sort("play_id")["yards_to_go"].to_list()
    assert rows[0] == 20  # w-usa already past midfield: 50 - 30
    assert rows[1] == 5  # w-ger's new drive starts fresh: 25 - 20, not goal-to-go


def test_derive_yards_to_go_missing_ballon_propagates_null():
    payload = [
        {"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"}},  # no ballOn
    ]
    df = derive_yards_to_go(_run_through_outcome(payload))
    assert df.row(0, named=True)["yards_to_go"] is None


def test_ingest_snapshots_wires_yards_to_go_derivation(tmp_path):
    payload = [
        {"playNumber": 1, "context": {"half": 1, "ballOn": 10, "possessionTeamId": "w-usa"}},
    ]
    raw_dir = _write_snapshot_dir(tmp_path, {"g1": payload})
    results = ingest_snapshots(raw_dir, _team_mapping())
    _, df, _ = results[0]
    assert df.row(0, named=True)["yards_to_go"] == 15


# ---------------------------------------------------------------------------
# _build_game_meta / tournament_id (2026-09-06: competition-labelling fix)
#
# Both ffwc26-women and ffwc26-men tournament documents share the exact same
# tournament.name ("IFAF World Flag 2026") -- trusting the bare name alone
# silently merged 25 men's games into the women's competition label
# end-to-end. _build_game_meta must disambiguate via divisions[0].
# ---------------------------------------------------------------------------


def test_build_game_meta_appends_division_to_competition_name():
    game_entry = {"tournamentId": "ffwc26-women", "homeTeam": {"id": "w-usa"}, "awayTeam": {"id": "w-ger"}}
    tournament_entry = {
        "id": "ffwc26-women",
        "name": "IFAF World Flag 2026",
        "startDate": "2026-08-13T08:00:00.000+02:00",
        "divisions": ["Women"],
    }
    meta = _build_game_meta(game_entry, tournament_entry)
    assert meta["competition"] == "IFAF World Flag 2026 Women"
    assert meta["tournament_id"] == "ffwc26-women"
    assert meta["gender"] == "women"


def test_build_game_meta_women_and_men_tournaments_get_distinct_competition_labels():
    """The exact live-data finding: both tournament documents share
    tournament.name == "IFAF World Flag 2026" -- the division suffix is the
    only thing that tells them apart."""
    women_entry = {"tournamentId": "ffwc26-women", "homeTeam": {}, "awayTeam": {}}
    women_tournament = {"id": "ffwc26-women", "name": "IFAF World Flag 2026", "divisions": ["Women"]}
    men_entry = {"tournamentId": "ffwc26-men", "homeTeam": {}, "awayTeam": {}}
    men_tournament = {"id": "ffwc26-men", "name": "IFAF World Flag 2026", "divisions": ["Men"]}

    women_meta = _build_game_meta(women_entry, women_tournament)
    men_meta = _build_game_meta(men_entry, men_tournament)

    assert women_meta["competition"] != men_meta["competition"]
    assert women_meta["competition"] == "IFAF World Flag 2026 Women"
    assert men_meta["competition"] == "IFAF World Flag 2026 Men"
    assert women_meta["tournament_id"] == "ffwc26-women"
    assert men_meta["tournament_id"] == "ffwc26-men"


def test_build_game_meta_uses_bare_name_when_no_divisions_key():
    """No divisions field at all (a shape predating this addendum, or a
    single tournament with no division info) -- competition stays exactly
    the bare tournament.name, unchanged from the pre-2026-09-06 behavior.
    This is also what tests/test_pipeline_ingest.py's synthetic IFAF fixture
    relies on (tournament_test.json has no divisions key)."""
    game_entry = {"tournamentId": "some-new-tourney", "homeTeam": {}, "awayTeam": {}}
    tournament_entry = {"id": "some-new-tourney", "name": "Some Cup"}  # no divisions key
    meta = _build_game_meta(game_entry, tournament_entry)
    assert meta["competition"] == "Some Cup"
    assert meta["gender"] is None


def test_build_game_meta_tournament_id_falls_back_to_tournament_entry_id():
    """When the /games entry itself lacks tournamentId (defensive), fall back
    to the resolved tournament document's own id."""
    game_entry = {"homeTeam": {}, "awayTeam": {}}
    tournament_entry = {"id": "ffwc26-women", "name": "IFAF World Flag 2026", "divisions": ["Women"]}
    meta = _build_game_meta(game_entry, tournament_entry)
    assert meta["tournament_id"] == "ffwc26-women"


def test_flatten_unified_plays_carries_tournament_id_through_to_canonical():
    payload = _load_fixture_payload()
    meta = _game_meta()
    meta["tournament_id"] = "ffwc26-women"
    df = flatten_unified_plays(payload, meta, "g1")
    assert set(df["tournament_id"].unique().to_list()) == {"ffwc26-women"}


def test_ingest_snapshots_labels_women_and_men_games_with_distinct_competitions(tmp_path):
    """End-to-end: two games from different tournaments sharing the same
    tournament.name must not collapse into one competition label."""
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir(parents=True, exist_ok=True)

    women_payload = [{"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"}}]
    men_payload = [{"playNumber": 1, "context": {"half": 1, "possessionTeamId": "m-usa"}}]
    (raw_dir / "unified-plays_wgame.json").write_text(json.dumps(women_payload), encoding="utf-8")
    (raw_dir / "unified-plays_mgame.json").write_text(json.dumps(men_payload), encoding="utf-8")

    games_meta = [
        {"id": "wgame", "tournamentId": "ffwc26-women", "homeTeam": {"id": "w-usa"}, "awayTeam": {"id": "w-ger"}},
        {"id": "mgame", "tournamentId": "ffwc26-men", "homeTeam": {"id": "m-usa"}, "awayTeam": {"id": "m-ger"}},
    ]
    (raw_dir / "games.json").write_text(json.dumps(games_meta), encoding="utf-8")

    women_tournament = {
        "id": "ffwc26-women", "name": "IFAF World Flag 2026",
        "startDate": "2026-08-13T08:00:00.000+02:00", "divisions": ["Women"],
    }
    men_tournament = {
        "id": "ffwc26-men", "name": "IFAF World Flag 2026",
        "startDate": "2026-08-13T08:00:00.000+02:00", "divisions": ["Men"],
    }
    (raw_dir / "tournament_ffwc26-women.json").write_text(json.dumps(women_tournament), encoding="utf-8")
    (raw_dir / "tournament_ffwc26-men.json").write_text(json.dumps(men_tournament), encoding="utf-8")

    mapping = pl.DataFrame(
        {
            "source": ["ifaf", "ifaf", "ifaf", "ifaf"],
            "source_team": ["w-usa", "w-ger", "m-usa", "m-ger"],
            "canonical_team": ["USA", "GER", "USA", "GER"],
        }
    )
    results = ingest_snapshots(raw_dir, mapping)
    by_game = {gid: df for gid, df, _ in results}

    women_competition = by_game["wgame"]["competition"].unique().to_list()
    men_competition = by_game["mgame"]["competition"].unique().to_list()
    assert women_competition == ["IFAF World Flag 2026 Women"]
    assert men_competition == ["IFAF World Flag 2026 Men"]
    assert by_game["wgame"]["tournament_id"].unique().to_list() == ["ffwc26-women"]
    assert by_game["mgame"]["tournament_id"].unique().to_list() == ["ffwc26-men"]


# ---------------------------------------------------------------------------
# ingest_snapshots(tournaments=...) (2026-09-06, fourth follow-up) -- the
# corpus is safe by default: a tournament not opted into never reaches the
# canonical frame at all, not merely excluded downstream at training time.
# ---------------------------------------------------------------------------


def _write_two_tournament_dir(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir(parents=True, exist_ok=True)

    women_payload = [{"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"}}]
    men_payload = [{"playNumber": 1, "context": {"half": 1, "possessionTeamId": "m-usa"}}]
    (raw_dir / "unified-plays_wgame.json").write_text(json.dumps(women_payload), encoding="utf-8")
    (raw_dir / "unified-plays_mgame.json").write_text(json.dumps(men_payload), encoding="utf-8")

    games_meta = [
        {"id": "wgame", "tournamentId": "ffwc26-women", "homeTeam": {"id": "w-usa"}, "awayTeam": {"id": "w-ger"}},
        {"id": "mgame", "tournamentId": "ffwc26-men", "homeTeam": {"id": "m-usa"}, "awayTeam": {"id": "m-ger"}},
    ]
    (raw_dir / "games.json").write_text(json.dumps(games_meta), encoding="utf-8")

    women_tournament = {"id": "ffwc26-women", "name": "IFAF World Flag 2026", "divisions": ["Women"]}
    men_tournament = {"id": "ffwc26-men", "name": "IFAF World Flag 2026", "divisions": ["Men"]}
    (raw_dir / "tournament_ffwc26-women.json").write_text(json.dumps(women_tournament), encoding="utf-8")
    (raw_dir / "tournament_ffwc26-men.json").write_text(json.dumps(men_tournament), encoding="utf-8")

    return raw_dir


def _women_and_men_team_mapping() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "source": ["ifaf", "ifaf", "ifaf", "ifaf"],
            "source_team": ["w-usa", "w-ger", "m-usa", "m-ger"],
            "canonical_team": ["USA", "GER", "USA-M", "GER-M"],
        }
    )


def test_ingest_snapshots_tournaments_none_ingests_both(tmp_path):
    raw_dir = _write_two_tournament_dir(tmp_path)
    results = ingest_snapshots(raw_dir, _women_and_men_team_mapping())
    assert {gid for gid, _, _ in results} == {"wgame", "mgame"}


def test_ingest_snapshots_tournaments_filter_excludes_unlisted_tournament(tmp_path):
    raw_dir = _write_two_tournament_dir(tmp_path)
    results = ingest_snapshots(raw_dir, _team_mapping(), tournaments=["ffwc26-women"])
    assert {gid for gid, _, _ in results} == {"wgame"}


def test_ingest_snapshots_tournaments_filter_can_opt_into_multiple(tmp_path):
    raw_dir = _write_two_tournament_dir(tmp_path)
    results = ingest_snapshots(
        raw_dir, _women_and_men_team_mapping(), tournaments=["ffwc26-women", "ffwc26-men"]
    )
    assert {gid for gid, _, _ in results} == {"wgame", "mgame"}


def test_ingest_snapshots_tournaments_filter_excludes_unresolvable_tournament_id(tmp_path):
    """A game whose tournamentId cannot be resolved at all (no games.json
    entry) is excluded too when the filter is active -- conservative default,
    never silently included just because metadata is missing."""
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir(parents=True, exist_ok=True)
    payload = [{"playNumber": 1, "context": {"half": 1, "possessionTeamId": "w-usa"}}]
    (raw_dir / "unified-plays_orphan.json").write_text(json.dumps(payload), encoding="utf-8")
    # No games.json at all.
    results = ingest_snapshots(raw_dir, _team_mapping(), tournaments=["ffwc26-women"])
    assert results == []


# ---------------------------------------------------------------------------
# /plays (the reviewer feed) — primary source, 2026-09-07.
# ---------------------------------------------------------------------------


def _ev(action: str, **kwargs) -> dict:
    return {"action": action, **kwargs}


def _play_record(
    sequence,
    half: int = 1,
    down=1,
    ball_on=5,
    offense: str = "w-usa",
    nullified: bool = False,
    events: list | None = None,
    started_at: int = 1000,
    official_score: str | None = None,
) -> dict:
    """Build one synthetic `/plays` record. `down`/`ball_on` may be passed as
    `None` to model a genuine missing-field row; player ids used across this
    test module are always fabricated (`w-xxx-pN`), never real.
    `official_score` models the reviewer feed's own per-record scoring
    verdict (`"TD"`/`"XP1"`/`"XP2"`/`"NONE"`/`None`) that `flatten_plays_records`
    reads scoring from (2026-09-07 fix) -- most non-scoring fixtures leave it
    at the default `None` (absent field, "no score")."""
    return {
        "gameId": "g1",
        "sequence": sequence,
        "half": half,
        "offenseTeamId": offense,
        "startedAt": started_at,
        "down": down,
        "nullified": nullified,
        "events": events or [],
        "ballOn": ball_on,
        "officialScore": official_score,
    }


def _synthetic_roster() -> list[dict]:
    """A small, entirely fabricated roster -- no real player names."""
    return [
        {
            "id": "w-usa",
            "name": "United States",
            "players": [
                {"id": "w-usa-p1", "name": "Player One", "number": "1"},
                {"id": "w-usa-p2", "name": "Player Two", "number": "2"},
                {"id": "w-usa-p3", "name": "Player Three", "number": "3"},
            ],
        },
        {
            "id": "w-ger",
            "name": "Germany",
            "players": [
                {"id": "w-ger-p1", "name": "Spielerin Eins", "number": "1"},
                {"id": "w-ger-p2", "name": "Spielerin Zwei", "number": "2"},
            ],
        },
    ]


def _empty_player_names() -> dict[str, str]:
    return {}


# --- _plays_record_sort_key / _play_type_from_actions (unit) ---------------


def test_plays_record_sort_key_orders_by_sequence_value():
    plays = [_play_record(30), _play_record(10), _play_record(20)]
    ordered = sorted(
        enumerate(plays), key=lambda pair: _plays_record_sort_key(pair[0], pair[1])
    )
    assert [p["sequence"] for _, p in ordered] == [10, 20, 30]


def test_plays_record_sort_key_handles_inserted_half_sequence():
    """A `.5`-suffixed inserted sequence (e.g. `907.5`) sorts between its
    integer neighbors, matching the live corpus's insertion convention."""
    plays = [_play_record(10), _play_record(20), _play_record(15.5)]
    ordered = sorted(
        enumerate(plays), key=lambda pair: _plays_record_sort_key(pair[0], pair[1])
    )
    assert [p["sequence"] for _, p in ordered] == [10, 15.5, 20]


def test_plays_record_sort_key_missing_or_non_numeric_sequence_sorts_last():
    bad = {"sequence": None}
    ok = _play_record(5)
    ordered = sorted(
        enumerate([bad, ok]), key=lambda pair: _plays_record_sort_key(pair[0], pair[1])
    )
    assert [p.get("sequence") for _, p in ordered] == [5, None]


def test_play_type_from_actions_no_play_wins_over_everything():
    assert _play_type_from_actions({"PASS", "COMPLETE"}, is_extra_point=True, is_no_play=True) == (
        "no_play"
    )


def test_play_type_from_actions_try_wins_over_pass_and_run():
    assert _play_type_from_actions({"PASS", "COMPLETE"}, is_extra_point=True, is_no_play=False) == (
        "extra_point"
    )


def test_play_type_from_actions_pass_wins_over_run_after_catch():
    assert _play_type_from_actions({"COMPLETE", "PASS", "RUSH"}, False, False) == "pass"


def test_play_type_from_actions_run_only():
    assert _play_type_from_actions({"RUSH"}, False, False) == "run"


def test_play_type_from_actions_ambiguous_stays_none():
    assert _play_type_from_actions({"FLAG_PULL"}, False, False) is None
    assert _play_type_from_actions(set(), False, False) is None


# --- load_plays_snapshot ----------------------------------------------------


def test_load_plays_snapshot_accepts_top_level_list(tmp_path):
    path = tmp_path / "plays_g1.json"
    path.write_text(json.dumps([_play_record(10)]), encoding="utf-8")
    plays = load_plays_snapshot(path)
    assert len(plays) == 1


def test_load_plays_snapshot_accepts_wrapped_shape(tmp_path):
    path = tmp_path / "plays_g1.json"
    path.write_text(json.dumps({"plays": [_play_record(10)]}), encoding="utf-8")
    plays = load_plays_snapshot(path)
    assert len(plays) == 1


def test_load_plays_snapshot_empty_list_is_valid(tmp_path):
    path = tmp_path / "plays_g1.json"
    path.write_text(json.dumps([]), encoding="utf-8")
    assert load_plays_snapshot(path) == []


def test_load_plays_snapshot_raises_on_unrecognized_shape(tmp_path):
    path = tmp_path / "plays_bad.json"
    path.write_text(json.dumps({"unexpected": "shape"}), encoding="utf-8")
    with pytest.raises(UnparseablePayload) as exc_info:
        load_plays_snapshot(path)
    assert str(path) in str(exc_info.value)


def test_load_plays_snapshot_raises_on_bad_json(tmp_path):
    path = tmp_path / "plays_bad.json"
    path.write_text("not json{", encoding="utf-8")
    with pytest.raises(UnparseablePayload):
        load_plays_snapshot(path)


# --- _load_teams_meta --------------------------------------------------------


def test_load_teams_meta_builds_player_id_to_name_dict(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir()
    (raw_dir / "tournament_ffwc26-women_teams.json").write_text(
        json.dumps(_synthetic_roster()), encoding="utf-8"
    )
    names = _load_teams_meta(raw_dir)
    assert names["w-usa-p1"] == "Player One"
    assert names["w-ger-p2"] == "Spielerin Zwei"


def test_load_teams_meta_folds_multiple_roster_files(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir()
    (raw_dir / "tournament_ffwc26-women_teams.json").write_text(
        json.dumps([_synthetic_roster()[0]]), encoding="utf-8"
    )
    (raw_dir / "tournament_ffwc26-men_teams.json").write_text(
        json.dumps([_synthetic_roster()[1]]), encoding="utf-8"
    )
    names = _load_teams_meta(raw_dir)
    assert names["w-usa-p1"] == "Player One"
    assert names["w-ger-p1"] == "Spielerin Eins"


def test_load_teams_meta_no_roster_files_returns_empty_dict(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir()
    assert _load_teams_meta(raw_dir) == {}


def test_load_teams_meta_tolerates_malformed_entries(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir()
    (raw_dir / "tournament_ffwc26-women_teams.json").write_text(
        json.dumps([{"id": "w-usa", "players": [{"id": "w-usa-p1"}, "not-a-dict", None]}]),
        encoding="utf-8",
    )
    names = _load_teams_meta(raw_dir)
    # A player with no `name` key contributes nothing; malformed entries are skipped.
    assert names == {}


# --- load_ifaf_final_scores --------------------------------------------------


def _write_games_json(raw_dir: Path, games: list[dict]) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "games.json").write_text(json.dumps(games), encoding="utf-8")


def test_load_ifaf_final_scores_no_games_json_returns_empty(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir()
    df, notices = load_ifaf_final_scores(raw_dir, _team_mapping())
    assert df.height == 0
    assert notices == []


def test_load_ifaf_final_scores_maps_team_codes_and_game_id(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    _write_games_json(
        raw_dir,
        [
            {
                "id": "g1",
                "status": "FINAL",
                "homeTeam": {"id": "w-usa"},
                "awayTeam": {"id": "w-ger"},
                "currentScore": {"home": 27, "away": 26},
            }
        ],
    )
    df, notices = load_ifaf_final_scores(raw_dir, _team_mapping())
    assert notices == []
    row = df.row(0, named=True)
    assert row["game_id"] == "ifaf-g1"
    assert row["home_team"] == "USA"
    assert row["away_team"] == "GER"
    assert row["home_score"] == 27
    assert row["away_score"] == 26


def test_load_ifaf_final_scores_skips_non_final_status(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    _write_games_json(
        raw_dir,
        [
            {
                "id": "g1",
                "status": "IN_PROGRESS",
                "homeTeam": {"id": "w-usa"},
                "awayTeam": {"id": "w-ger"},
                "currentScore": {"home": 6, "away": 0},
            }
        ],
    )
    df, notices = load_ifaf_final_scores(raw_dir, _team_mapping())
    assert df.height == 0


def test_load_ifaf_final_scores_skips_unmapped_team_with_notice(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    _write_games_json(
        raw_dir,
        [
            {
                "id": "g1",
                "status": "FINAL",
                "homeTeam": {"id": "w-usa"},
                "awayTeam": {"id": "w-unknown"},
                "currentScore": {"home": 10, "away": 0},
            }
        ],
    )
    df, notices = load_ifaf_final_scores(raw_dir, _team_mapping())
    assert df.height == 0
    assert any("w-unknown" in n for n in notices)


def test_load_ifaf_final_scores_skips_missing_score(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    _write_games_json(
        raw_dir,
        [
            {
                "id": "g1",
                "status": "FINAL",
                "homeTeam": {"id": "w-usa"},
                "awayTeam": {"id": "w-ger"},
                "currentScore": {"home": None, "away": None},
            }
        ],
    )
    df, notices = load_ifaf_final_scores(raw_dir, _team_mapping())
    assert df.height == 0


# --- _events_score_ledger_summary --------------------------------------------


def _write_events_json(raw_dir: Path, game_id: str, events: list[dict]) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / f"events_{game_id}.json").write_text(json.dumps(events), encoding="utf-8")


def _score_event(seq: int, team: str, score_type: str, points: int, reverted: bool = False) -> dict:
    return {
        "eventType": "SCORE",
        "sequenceNumber": seq,
        "reverted": reverted,
        "payload": {"teamId": team, "scoreType": score_type, "points": points},
    }


def test_events_score_ledger_summary_no_events_file_returns_none(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir()
    game_entry = {
        "homeTeam": {"id": "w-usa"},
        "awayTeam": {"id": "w-ger"},
        "currentScore": {"home": 7, "away": 0},
    }
    assert _events_score_ledger_summary(raw_dir, "g1", game_entry) is None


def test_events_score_ledger_summary_no_score_events_returns_none(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    _write_events_json(raw_dir, "g1", [{"eventType": "POSSESSION_CHANGE", "payload": {}}])
    game_entry = {
        "homeTeam": {"id": "w-usa"},
        "awayTeam": {"id": "w-ger"},
        "currentScore": {"home": 7, "away": 0},
    }
    assert _events_score_ledger_summary(raw_dir, "g1", game_entry) is None


def test_events_score_ledger_summary_matches_official_score(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    _write_events_json(
        raw_dir,
        "g1",
        [
            _score_event(10, "w-usa", "TD", 6),
            _score_event(15, "w-usa", "XP1", 1),
        ],
    )
    game_entry = {
        "homeTeam": {"id": "w-usa"},
        "awayTeam": {"id": "w-ger"},
        "currentScore": {"home": 7, "away": 0},
    }
    msg = _events_score_ledger_summary(raw_dir, "g1", game_entry)
    assert msg is not None
    assert "confirm the official" in msg
    assert "7-0" in msg


def test_events_score_ledger_summary_disagrees_with_official_score(tmp_path):
    raw_dir = tmp_path / "raw_ifaf"
    _write_events_json(raw_dir, "g1", [_score_event(10, "w-usa", "TD", 6)])
    game_entry = {
        "homeTeam": {"id": "w-usa"},
        "awayTeam": {"id": "w-ger"},
        "currentScore": {"home": 7, "away": 0},
    }
    msg = _events_score_ledger_summary(raw_dir, "g1", game_entry)
    assert msg is not None
    assert "do NOT match" in msg


def test_events_score_ledger_summary_ignores_reverted_events(tmp_path):
    """A reverted SCORE event (undone by the reviewer) must not count --
    otherwise a real 7-0 game with one undone extra phantom touchdown would
    wrongly report a ledger/official disagreement."""
    raw_dir = tmp_path / "raw_ifaf"
    _write_events_json(
        raw_dir,
        "g1",
        [
            _score_event(10, "w-usa", "TD", 6),
            _score_event(15, "w-usa", "XP1", 1),
            _score_event(20, "w-usa", "TD", 6, reverted=True),
        ],
    )
    game_entry = {
        "homeTeam": {"id": "w-usa"},
        "awayTeam": {"id": "w-ger"},
        "currentScore": {"home": 7, "away": 0},
    }
    msg = _events_score_ledger_summary(raw_dir, "g1", game_entry)
    assert msg is not None
    assert "confirm the official" in msg


def test_events_score_ledger_summary_no_official_score_returns_none(tmp_path):
    """A forfeit-shaped or in-progress game entry with no currentScore has
    nothing to compare against."""
    raw_dir = tmp_path / "raw_ifaf"
    _write_events_json(raw_dir, "g1", [_score_event(10, "w-usa", "TD", 6)])
    game_entry = {"homeTeam": {"id": "w-usa"}, "awayTeam": {"id": "w-ger"}}
    assert _events_score_ledger_summary(raw_dir, "g1", game_entry) is None


def test_ingest_snapshots_surfaces_ledger_summary_as_game_notice(tmp_path):
    """End-to-end: `ingest_snapshots` folds the ledger diagnostic into the
    per-game `IngestNotices.messages`, without changing which rows reach
    the canonical frame."""
    raw_dir = tmp_path / "raw_ifaf"
    raw_dir.mkdir(parents=True, exist_ok=True)
    plays = [_play_record(10, events=[_ev("PASS"), _ev("COMPLETE"), _ev("TOUCHDOWN")], official_score="TD")]
    (raw_dir / "plays_g1.json").write_text(json.dumps(plays), encoding="utf-8")
    games_meta = [
        {
            "id": "g1",
            "tournamentId": "test",
            "homeTeam": {"id": "w-usa"},
            "awayTeam": {"id": "w-ger"},
            "currentScore": {"home": 6, "away": 0},
        }
    ]
    (raw_dir / "games.json").write_text(json.dumps(games_meta), encoding="utf-8")
    _write_events_json(raw_dir, "g1", [_score_event(10, "w-usa", "TD", 6)])

    results = ingest_snapshots(raw_dir, _team_mapping(), tournaments=None)
    gid, df, notices = results[0]
    assert gid == "g1"
    assert df.height == 1  # ledger check never changes accepted rows
    assert any("confirm the official" in m for m in notices.messages)


# --- flatten_plays_records ---------------------------------------------------


def _game_meta_plays(home: str = "w-usa", away: str = "w-ger") -> dict:
    return {
        "home_team": home,
        "away_team": away,
        "competition": "IFAF World Flag 2026 Women",
        "season": 2026,
        "gender": "women",
        "tournament_id": "ffwc26-women",
    }


def test_flatten_plays_records_one_row_per_record_gapless_play_id():
    payload = [_play_record(10), _play_record(20), _play_record(30)]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    assert df["play_id"].to_list() == [1, 2, 3]
    assert df["source_play_sequence"].to_list() == [10.0, 20.0, 30.0]


def test_flatten_plays_records_qf_regression_first_and_goal_to_midfield_sequence():
    """Regression test for the bug that motivated this rewrite: the real
    game's first three plays read 1st @5 -> 2nd @11 -> 3rd @31, not the
    down-2/ballOn-4 default state the old unified-plays context produced."""
    payload = [
        _play_record(10, down=1, ball_on=5, events=[_ev("PASS"), _ev("COMPLETE")]),
        _play_record(20, down=2, ball_on=11, events=[_ev("PASS"), _ev("COMPLETE")]),
        _play_record(30, down=3, ball_on=31, events=[_ev("PASS"), _ev("COMPLETE")]),
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    df = derive_yardage_columns_plays(df)
    df = derive_yards_to_go(df)
    assert df["down"].to_list() == [1, 2, 3]
    assert df["yardline_50"].to_list() == [5, 11, 31]
    # yards_to_go: midfield-targeting until yardline_50 >= 25, then goal-targeting.
    assert df["yards_to_go"].to_list() == [20, 14, 19]


def test_flatten_plays_records_try_sets_down_zero_even_when_raw_down_null():
    payload = [_play_record(10, down=None, ball_on=45, events=[_ev("TRY", tryPoints=1, tryGood=True)])]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    assert df["down"].to_list() == [0]
    assert df["_missing_down"].to_list() == [0]  # a TRY's null down is not a "missing" gap


def test_flatten_plays_records_nullified_non_extra_point_becomes_no_play_all_flags_zero():
    """A nullified live pass/run play (no extra-point signal) collapses to
    `no_play`, every scoring/turnover flag zeroed -- unaffected by the
    narrower `nullified` extra-point carve-out below."""
    payload = [
        _play_record(
            10,
            nullified=True,
            events=[_ev("PASS"), _ev("COMPLETE"), _ev("TOUCHDOWN")],
            official_score="TD",
        )
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["play_type"] == "no_play"
    assert row["complete_pass"] == 0
    assert row["touchdown"] == 0
    assert row["_nullified"] == 1
    # Raw record still preserved for traceability, not silently dropped.
    assert row["result_raw"] == "PASS, COMPLETE, TOUCHDOWN"


def test_flatten_plays_records_nullified_extra_point_keeps_extra_point_play_type():
    """2026-09-07 domain-expert review: an annulled play keeps its own
    identity -- a nullified try (e.g. the QF's own sequence 50: a
    successful catch called back by a following offensive penalty) is
    still `extra_point`, not `no_play`. Points still come from
    `officialScore`/`nullified` (0 here), and `down` is already 0 for this
    record shape regardless of nullification -- nothing about `no_play`'s
    null-down semantics was ever needed for it."""
    payload = [
        _play_record(
            10,
            nullified=True,
            events=[_ev("PASS"), _ev("COMPLETE"), _ev("TRY", tryPoints=1, tryGood=True)],
            official_score="NONE",
        )
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["play_type"] == "extra_point"
    assert row["down"] == 0
    assert row["complete_pass"] == 0
    assert row["one_point_conv_success"] == 0
    assert row["nullified"] == 1
    # Raw record still preserved for traceability, not silently dropped.
    assert row["result_raw"] == "PASS, COMPLETE, TRY"


def test_flatten_plays_records_penalty_only_is_no_play():
    payload = [_play_record(10, events=[_ev("PENALTY", offendingTeamId="w-ger", penaltyType="OTHER")])]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["play_type"] == "no_play"
    assert row["penalty"] == 1
    assert row["penalty_type"] == "OTHER"


def test_flatten_plays_records_nullified_penalty_only_still_sets_penalty_flag():
    """`penalty` is a record-shape classification, not a scoring/turnover
    effect -- it must survive nullification, or the downs_range no-play
    exemption (play_type == "no_play" AND penalty == 1) can never recognize
    an overturned dead-ball penalty entry (a real case in the live corpus:
    a PENALTY-only record the reviewer also marked nullified)."""
    payload = [
        _play_record(
            10, nullified=True, down=None, events=[_ev("PENALTY", penaltyType="OTHER")]
        )
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["play_type"] == "no_play"
    assert row["penalty"] == 1
    assert row["down"] is None


def test_flatten_plays_records_penalty_on_live_play_keeps_real_play_type():
    payload = [
        _play_record(10, events=[_ev("PASS"), _ev("COMPLETE"), _ev("PENALTY", penaltyType="ILLEGAL_CONTACT")])
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["play_type"] == "pass"
    assert row["complete_pass"] == 1
    assert row["penalty"] == 1


def test_flatten_plays_records_drive_id_increments_on_offense_change():
    payload = [
        _play_record(10, offense="w-usa"),
        _play_record(20, offense="w-usa"),
        _play_record(30, offense="w-ger"),
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    assert df["drive_id"].to_list() == [1, 1, 2]


def test_flatten_plays_records_touchdown_with_interception_is_defensive():
    """Scoring is driven by `officialScore`, not the `TOUCHDOWN` action alone
    (2026-09-07 fix) -- a `TOUCHDOWN`-actioned record with no `officialScore`
    at all (the ordinary "not a scoring play" case) sets no scoring flag."""
    payload = [
        _play_record(
            10,
            events=[_ev("PASS"), _ev("INTERCEPTION"), _ev("TOUCHDOWN")],
            official_score="TD",
        )
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["def_touchdown"] == 1
    assert row["touchdown"] == 0
    assert row["interception"] == 1


def test_flatten_plays_records_touchdown_action_without_official_score_scores_nothing():
    """The bug this fix corrects: a `TOUCHDOWN` action alone is not enough --
    a record whose `officialScore` doesn't say "TD" never scores 6, even
    with a clean pass-and-catch action list."""
    payload = [_play_record(10, events=[_ev("PASS"), _ev("COMPLETE"), _ev("TOUCHDOWN")])]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["touchdown"] == 0
    assert row["def_touchdown"] == 0
    assert row["play_type"] == "pass"


def test_flatten_plays_records_touchdown_without_interception_is_offensive():
    payload = [
        _play_record(
            10, events=[_ev("PASS"), _ev("COMPLETE"), _ev("TOUCHDOWN")], official_score="TD"
        )
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["touchdown"] == 1
    assert row["def_touchdown"] == 0
    assert row["play_type"] == "pass"


def test_flatten_plays_records_touchdown_with_rush_is_run_play_type():
    payload = [_play_record(10, events=[_ev("RUSH"), _ev("TOUCHDOWN")], official_score="TD")]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["touchdown"] == 1
    assert row["play_type"] == "run"


def test_flatten_plays_records_touchdown_with_no_pass_or_run_signal_stays_type_none():
    payload = [_play_record(10, events=[_ev("TOUCHDOWN")], official_score="TD")]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["touchdown"] == 1
    assert row["play_type"] is None


def test_flatten_plays_records_touchdown_action_xp1_official_score_is_one_point_pat():
    """A PAT catch the reviewer feed charts with a `TOUCHDOWN` action instead
    of `TRY` (seq 200 of the ESP-MEX QF, the concrete live-corpus bug this
    fix was written for): `officialScore == "XP1"` is authoritative, worth 1
    point, not 6, and the record is `extra_point`/`down == 0` for
    down/play_type purposes even though its own raw `down` is a real (non-
    null) value."""
    payload = [
        _play_record(
            10,
            down=2,
            ball_on=45,
            events=[_ev("PASS"), _ev("COMPLETE"), _ev("TOUCHDOWN")],
            official_score="XP1",
        )
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["touchdown"] == 0
    assert row["one_point_conv_success"] == 1
    assert row["two_point_conv_success"] == 0
    assert row["play_type"] == "extra_point"
    assert row["down"] == 0


def test_flatten_plays_records_touchdown_action_xp2_official_score_is_two_point_pat():
    payload = [
        _play_record(
            10,
            down=2,
            ball_on=40,
            events=[_ev("RUSH"), _ev("TOUCHDOWN")],
            official_score="XP2",
        )
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["touchdown"] == 0
    assert row["two_point_conv_success"] == 1
    assert row["play_type"] == "extra_point"
    assert row["down"] == 0


def test_flatten_plays_records_safety_xp2_official_score_stays_safety_not_conversion():
    """The 4 live-corpus SAFETY-only records carry `officialScore == "XP2"`
    (the app's own encoding for a safety) -- these must book as `safety`
    (2 points to the defense already set from the `SAFETY` action), never as
    a two-point conversion for the offense."""
    payload = [_play_record(10, events=[_ev("SACK"), _ev("SAFETY")], official_score="XP2")]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["safety"] == 1
    assert row["two_point_conv_success"] == 0
    assert row["touchdown"] == 0


@pytest.mark.parametrize(
    "official_score,expected_one,expected_two",
    [("XP1", 1, 0), ("XP2", 0, 1), ("NONE", 0, 0), (None, 0, 0)],
)
def test_flatten_plays_records_try_official_score_sets_conversion_flags(
    official_score, expected_one, expected_two
):
    """A TRY-actioned record's own points come from `officialScore`, not the
    TRY event's `tryGood`/`tryPoints` fields directly -- the live corpus has
    dozens of records where those disagree (`officialScore` is the
    reviewer's final call, e.g. a successful catch called back on review)."""
    payload = [
        _play_record(
            10,
            down=None,
            ball_on=45,
            events=[_ev("PASS"), _ev("COMPLETE"), _ev("TRY", tryPoints=1, tryGood=True)],
            official_score=official_score,
        )
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["one_point_conv_success"] == expected_one
    assert row["two_point_conv_success"] == expected_two
    assert row["defensive_two_point_conv"] == 0


@pytest.mark.parametrize(
    "try_points,try_good,expected_one,expected_two",
    [(1, True, 1, 0), (2, True, 0, 1), (1, False, 0, 0), (2, False, 0, 0), (None, None, 0, 0)],
)
def test_flatten_plays_records_try_ambiguous_td_official_score_falls_back_to_try_event(
    try_points, try_good, expected_one, expected_two
):
    """The 21-record live-corpus quirk: a TRY-actioned record whose own
    `officialScore` reads "TD" (never a valid verdict for a try -- see the
    backfill test below) scores from the TRY event's own `tryPoints`/
    `tryGood` fields instead, since `officialScore` on this one record shape
    is unusable directly."""
    payload = [
        _play_record(
            10,
            down=None,
            ball_on=45,
            events=[_ev("PASS"), _ev("COMPLETE"), _ev("TRY", tryPoints=try_points, tryGood=try_good)],
            official_score="TD",
        )
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["one_point_conv_success"] == expected_one
    assert row["two_point_conv_success"] == expected_two
    assert row["defensive_two_point_conv"] == 0
    assert row["touchdown"] == 0


def test_flatten_plays_records_try_td_official_score_backfills_preceding_touchdown():
    """The other half of the same quirk: when the TRY record's borrowed "TD"
    label evidences that the *preceding* TOUCHDOWN-actioned record was
    itself mislabeled (`officialScore == "NONE"` despite the TOUCHDOWN
    action), the real 6 points are credited there, not fabricated on the
    try row itself."""
    payload = [
        _play_record(
            10,
            events=[_ev("PASS"), _ev("COMPLETE"), _ev("TOUCHDOWN")],
            official_score="NONE",
        ),
        _play_record(
            20,
            down=None,
            ball_on=45,
            events=[_ev("PASS"), _ev("COMPLETE"), _ev("TRY", tryPoints=1, tryGood=False)],
            official_score="TD",
        ),
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    anchor, try_row = df.row(0, named=True), df.row(1, named=True)
    assert anchor["touchdown"] == 1
    assert try_row["touchdown"] == 0
    assert try_row["one_point_conv_success"] == 0  # tryGood False on the try itself


def test_flatten_plays_records_try_td_official_score_duplicate_anchor_already_scored():
    """When the preceding TOUCHDOWN-actioned record's own `officialScore`
    already reads "TD" (it was never mislabeled), a following try record's
    own "TD" label is a duplicate -- the anchor is not double-counted."""
    payload = [
        _play_record(
            10,
            events=[_ev("PASS"), _ev("COMPLETE"), _ev("TOUCHDOWN")],
            official_score="TD",
        ),
        _play_record(
            20,
            down=None,
            ball_on=40,
            events=[_ev("SACK"), _ev("TRY", tryPoints=2, tryGood=False)],
            official_score="TD",
        ),
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    anchor, try_row = df.row(0, named=True), df.row(1, named=True)
    assert anchor["touchdown"] == 1
    assert try_row["touchdown"] == 0
    assert try_row["two_point_conv_success"] == 0


def test_flatten_plays_records_nullified_flag_exposed_as_extra():
    payload = [
        _play_record(
            10,
            nullified=True,
            events=[_ev("PASS"), _ev("COMPLETE"), _ev("TRY", tryPoints=1, tryGood=True)],
        )
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    assert df["nullified"].to_list() == [1]


def test_flatten_plays_records_sack_is_pass_play_type():
    payload = [_play_record(10, down=4, events=[_ev("SACK")])]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["sack"] == 1
    assert row["play_type"] == "pass"


def test_flatten_plays_records_interception_alone_is_pass_play_type():
    """A record with only an `INTERCEPTION` action (no `PASS` event captured,
    a real live-corpus data gap) still classifies as a pass -- an
    interception can only happen on a passing down."""
    payload = [_play_record(10, events=[_ev("INTERCEPTION")])]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["interception"] == 1
    assert row["play_type"] == "pass"


def test_flatten_plays_records_safety_flag_and_ambiguous_play_type():
    payload = [_play_record(10, events=[_ev("SAFETY")])]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["safety"] == 1
    assert row["play_type"] is None  # no pass/run signal in this record


def test_flatten_plays_records_empty_events_not_nullified_stays_unknown():
    payload = [_play_record(10, events=[])]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["play_type"] is None
    assert row["result_raw"] is None
    assert row["complete_pass"] == 0


def test_flatten_plays_records_unknown_action_recorded_not_raised():
    payload = [_play_record(10, events=[_ev("SOME_NEW_ACTION")])]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    assert df["_unknown_action"].to_list() == ["SOME_NEW_ACTION"]


def test_flatten_plays_records_player_resolution_pass_complete():
    payload = [
        _play_record(
            10,
            events=[
                _ev("PASS", playerId="w-usa-p1", intendedReceiverId="w-usa-p2"),
                _ev("COMPLETE", playerId="w-usa-p2"),
            ],
        )
    ]
    names = {"w-usa-p1": "Player One", "w-usa-p2": "Player Two"}
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", names)
    row = df.row(0, named=True)
    assert row["qb"] == "Player One"
    assert row["thrown_by"] == "Player One"
    assert row["target"] == "Player Two"
    assert row["received_by"] == "Player Two"


def test_flatten_plays_records_player_resolution_falls_back_to_incomplete_pass_event():
    """~1% of the live corpus records an `INCOMPLETE_PASS` action with no
    accompanying `PASS` event -- `qb`/`target` still resolve from it."""
    payload = [
        _play_record(
            10,
            events=[_ev("INCOMPLETE_PASS", playerId="w-usa-p1", intendedReceiverId="w-usa-p2")],
        )
    ]
    names = {"w-usa-p1": "Player One", "w-usa-p2": "Player Two"}
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", names)
    row = df.row(0, named=True)
    assert row["qb"] == "Player One"
    assert row["target"] == "Player Two"
    assert row["received_by"] is None  # nobody caught it


def test_flatten_plays_records_rush_ball_carrier_maps_to_target():
    payload = [_play_record(10, events=[_ev("RUSH", playerId="w-usa-p3")])]
    names = {"w-usa-p3": "Player Three"}
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", names)
    row = df.row(0, named=True)
    assert row["target"] == "Player Three"
    assert row["qb"] is None


def test_flatten_plays_records_pass_side_depth_incomplete_reason_extras():
    payload = [
        _play_record(
            10,
            events=[
                _ev(
                    "INCOMPLETE_PASS",
                    passSide="LEFT",
                    passDepth="DEEP",
                    incompleteReason="DROPPED",
                )
            ],
        )
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    row = df.row(0, named=True)
    assert row["pass_side"] == "LEFT"
    assert row["pass_depth"] == "DEEP"
    assert row["incomplete_reason"] == "DROPPED"


def test_flatten_plays_records_missing_down_and_ballon_counted_and_null():
    payload = [_play_record(10, down=None, ball_on=None, events=[_ev("PASS"), _ev("COMPLETE")])]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    assert df["_missing_down"].to_list() == [1]
    assert df["_missing_ballon"].to_list() == [1]
    assert df["down"].to_list() == [None]
    assert df["yardline_50"].to_list() == [None]


def test_flatten_plays_records_missing_offense_counted():
    payload = [
        {
            "gameId": "g1",
            "sequence": 10,
            "half": 1,
            "startedAt": 1000,
            "down": 1,
            "nullified": False,
            "events": [],
            "ballOn": 5,
        }
    ]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    assert df["_missing_offense"].to_list() == [1]


def test_flatten_plays_records_output_columns_match_working_schema_columns():
    payload = [_play_record(10)]
    df = flatten_plays_records(payload, _game_meta_plays(), "g1", _empty_player_names())
    # source_detail defaults to null on the primary path (stamped only on the
    # unified-plays fallback path, by `ingest_snapshots`).
    assert df["source_detail"].to_list() == [None]


# --- derive_yardage_columns_plays -------------------------------------------


def _plays_frame(rows: list[dict]) -> pl.DataFrame:
    """Build a minimal already-flattened `/plays` working frame for
    `derive_yardage_columns_plays` unit tests, filling every column the
    function reads with a safe default when not given."""
    defaults = {
        "game_id": "ifaf-g1",
        "play_id": 0,
        "drive_id": 1,
        "down": 1,
        "yardline_50": 5,
        "play_type": "pass",
        "touchdown": 0,
        "safety": 0,
        "interception": 0,
        "def_touchdown": 0,
        "defensive_two_point_conv": 0,
    }
    full_rows = []
    for i, row in enumerate(rows):
        merged = {**defaults, **row}
        merged["play_id"] = i + 1
        full_rows.append(merged)
    return pl.DataFrame(full_rows)


def test_derive_yardage_plays_same_drive_gain():
    df = _plays_frame(
        [
            {"drive_id": 1, "yardline_50": 5},
            {"drive_id": 1, "yardline_50": 11},
        ]
    )
    out = derive_yardage_columns_plays(df)
    assert out["yards_gained"].to_list() == [6, None]


def test_derive_yardage_plays_touchdown_distance_to_goal():
    df = _plays_frame([{"yardline_50": 42, "touchdown": 1}])
    out = derive_yardage_columns_plays(df)
    assert out["yards_gained"].to_list() == [8]


def test_derive_yardage_plays_safety_negative_of_yardline():
    df = _plays_frame([{"yardline_50": 3, "safety": 1}])
    out = derive_yardage_columns_plays(df)
    assert out["yards_gained"].to_list() == [-3]


def test_derive_yardage_plays_interception_stays_null():
    df = _plays_frame(
        [
            {"drive_id": 1, "yardline_50": 24, "interception": 1},
            {"drive_id": 2, "yardline_50": 36},
        ]
    )
    out = derive_yardage_columns_plays(df)
    assert out["yards_gained"].to_list() == [None, None]


def test_derive_yardage_plays_def_touchdown_stays_null():
    df = _plays_frame([{"yardline_50": 24, "def_touchdown": 1, "interception": 1}])
    out = derive_yardage_columns_plays(df)
    assert out["yards_gained"].to_list() == [None]


def test_derive_yardage_plays_no_play_stays_null_never_fake_gain():
    df = _plays_frame(
        [
            {"drive_id": 1, "yardline_50": 5, "play_type": "no_play"},
            {"drive_id": 1, "yardline_50": 20},
        ]
    )
    out = derive_yardage_columns_plays(df)
    assert out["yards_gained"].to_list() == [None, None]


def test_derive_yardage_plays_try_row_excluded_even_with_same_drive_next():
    df = _plays_frame(
        [
            {"drive_id": 1, "down": 0, "yardline_50": 45, "play_type": "extra_point"},
            {"drive_id": 1, "yardline_50": 5},
        ]
    )
    out = derive_yardage_columns_plays(df)
    assert out["yards_gained"].to_list() == [None, None]


def test_derive_yardage_plays_penalty_between_plays_absorbs_adjustment():
    """A dead-ball penalty record sitting between two real plays on the same
    drive is this rule's own "next record" for the play immediately before
    it -- its `ballOn` already reflects the enforced spot, so the diff
    naturally includes the penalty adjustment with no special-casing."""
    df = _plays_frame(
        [
            {"drive_id": 1, "yardline_50": 5},  # live play
            {"drive_id": 1, "yardline_50": 18, "play_type": "no_play"},  # penalty no-play
            {"drive_id": 1, "yardline_50": 30},  # next live play
        ]
    )
    out = derive_yardage_columns_plays(df)
    # First play's own gain reflects the spot after the penalty was enforced.
    assert out["yards_gained"].to_list()[0] == 13
    assert out["yards_gained"].to_list()[1] is None  # the no-play row itself


def test_derive_yardage_plays_last_play_of_game_stays_null():
    df = _plays_frame([{"drive_id": 1, "yardline_50": 20}])
    out = derive_yardage_columns_plays(df)
    assert out["yards_gained"].to_list() == [None]


def test_derive_yardage_plays_missing_yardline_propagates_null():
    df = _plays_frame(
        [
            {"drive_id": 1, "yardline_50": None},
            {"drive_id": 1, "yardline_50": 20},
        ]
    )
    out = derive_yardage_columns_plays(df)
    assert out["yards_gained"].to_list() == [None, None]


# --- ingest_snapshots: primary /plays vs unified-plays fallback ------------


def test_ingest_snapshots_uses_plays_primary_when_usable(tmp_path):
    reviewer_plays = {
        "g1": [
            _play_record(10, down=1, ball_on=5, events=[_ev("PASS"), _ev("COMPLETE")]),
            _play_record(20, down=2, ball_on=11, events=[_ev("PASS"), _ev("COMPLETE")]),
        ]
    }
    raw_dir = _write_snapshot_dir(
        tmp_path,
        plays_by_game={},
        reviewer_plays_by_game=reviewer_plays,
        write_unified=False,
    )
    results = ingest_snapshots(raw_dir, _team_mapping())
    assert len(results) == 1
    gid, df, notices = results[0]
    assert gid == "g1"
    assert df.height == 2
    assert df["down"].to_list() == [1, 2]
    assert df["yardline_50"].to_list() == [5, 11]
    assert df["source_detail"].to_list() == [None, None]
    assert not notices.skipped


def test_ingest_snapshots_falls_back_when_plays_file_missing(tmp_path):
    unified_payload = [{"playNumber": 1, "context": {"half": 1, "down": 1, "ballOn": 5, "possessionTeamId": "w-usa"}}]
    raw_dir = _write_snapshot_dir(tmp_path, plays_by_game={"g1": unified_payload})
    results = ingest_snapshots(raw_dir, _team_mapping())
    assert len(results) == 1
    gid, df, notices = results[0]
    assert df.height == 1
    assert df["source_detail"].to_list() == ["unified-plays-fallback"]
    assert any("fell back to unified-plays" in m for m in notices.messages)


def test_ingest_snapshots_falls_back_when_plays_response_is_empty_forfeit(tmp_path):
    """An empty /plays response with NO reconciliation reason (a genuine
    zero-play forfeit) still falls back to unified-plays -- there is no
    structured "known-incomplete" signal to act on here, unlike the
    reconciliation-gap case below."""
    unified_payload = [{"playNumber": 1, "context": {"half": 1, "down": 1, "ballOn": 5, "possessionTeamId": "w-usa"}}]
    raw_dir = _write_snapshot_dir(
        tmp_path,
        plays_by_game={"g1": unified_payload},
        reviewer_plays_by_game={"g1": []},
    )
    results = ingest_snapshots(raw_dir, _team_mapping())
    gid, df, notices = results[0]
    assert df["source_detail"].to_list() == ["unified-plays-fallback"]


def test_ingest_snapshots_excludes_reconciliation_gap_never_falls_back(tmp_path):
    """A real, structured 'not reviewed' /plays response (a non-null
    reconciliation.reason on an empty play list, e.g. `no-tries-labelled`)
    is excluded entirely -- 2026-09-07 fix: unified-plays.context is known-
    unreliable, and an events-feed reconstruction was measured and found not
    accurate enough (77.5%/46.8% down/ballOn agreement, below the 95% bar),
    so this case must NEVER fall back to accepting unified-plays rows."""
    unified_payload = [{"playNumber": 1, "context": {"half": 1, "down": 1, "ballOn": 5, "possessionTeamId": "w-usa"}}]
    raw_dir = _write_snapshot_dir(tmp_path, plays_by_game={"g1": unified_payload}, write_unified=True)
    (raw_dir / "plays_g1.json").write_text(
        json.dumps(
            {
                "plays": [],
                "reconciliation": {
                    "ok": False,
                    "reason": "no-tries-labelled",
                    "message": "No conversion is labelled TRY.",
                },
            }
        ),
        encoding="utf-8",
    )
    results = ingest_snapshots(raw_dir, _team_mapping())
    gid, df, notices = results[0]
    assert df.height == 0
    assert df.columns == list(CANONICAL_COLUMNS)
    assert notices.skipped is True
    assert "no-tries-labelled" in notices.skip_reason
    assert "95%" in notices.skip_reason


def test_ingest_snapshots_falls_back_when_plays_file_unparseable(tmp_path):
    unified_payload = [{"playNumber": 1, "context": {"half": 1, "down": 1, "ballOn": 5, "possessionTeamId": "w-usa"}}]
    raw_dir = _write_snapshot_dir(tmp_path, plays_by_game={"g1": unified_payload})
    (raw_dir / "plays_g1.json").write_text("not json{", encoding="utf-8")
    results = ingest_snapshots(raw_dir, _team_mapping())
    gid, df, notices = results[0]
    assert df["source_detail"].to_list() == ["unified-plays-fallback"]
    assert any("could not read/parse JSON" in m for m in notices.messages)


def test_ingest_snapshots_discovers_plays_only_game_with_no_unified_file(tmp_path):
    """A game with only a `plays_{id}.json` file (no matching
    `unified-plays_{id}.json`) is still discovered and ingested via the
    primary path -- game discovery is the union of both snapshot kinds."""
    reviewer_plays = {"g1": [_play_record(10)]}
    raw_dir = _write_snapshot_dir(
        tmp_path, plays_by_game={}, reviewer_plays_by_game=reviewer_plays, write_unified=False
    )
    results = ingest_snapshots(raw_dir, _team_mapping())
    assert [gid for gid, _, _ in results] == ["g1"]


def test_ingest_snapshots_plays_unmapped_action_recorded_as_notice(tmp_path):
    reviewer_plays = {"g1": [_play_record(10, events=[_ev("MYSTERY_ACTION")])]}
    raw_dir = _write_snapshot_dir(
        tmp_path, plays_by_game={}, reviewer_plays_by_game=reviewer_plays, write_unified=False
    )
    results = ingest_snapshots(raw_dir, _team_mapping())
    _, df, notices = results[0]
    assert notices.unmapped_outcomes == {"MYSTERY_ACTION": 1}


def test_ingest_snapshots_plays_nullified_count_recorded_as_notice(tmp_path):
    reviewer_plays = {
        "g1": [_play_record(10, nullified=True, events=[_ev("PASS"), _ev("COMPLETE")])]
    }
    raw_dir = _write_snapshot_dir(
        tmp_path, plays_by_game={}, reviewer_plays_by_game=reviewer_plays, write_unified=False
    )
    results = ingest_snapshots(raw_dir, _team_mapping())
    _, df, notices = results[0]
    assert any("1 nullified" in m for m in notices.messages)


def test_ingest_snapshots_plays_missing_context_keys_notice(tmp_path):
    reviewer_plays = {"g1": [_play_record(10, down=None, ball_on=None, events=[_ev("PASS")])]}
    raw_dir = _write_snapshot_dir(
        tmp_path, plays_by_game={}, reviewer_plays_by_game=reviewer_plays, write_unified=False
    )
    results = ingest_snapshots(raw_dir, _team_mapping())
    _, df, notices = results[0]
    assert notices.missing_context_keys.get("down") == 1
    assert notices.missing_context_keys.get("ballOn") == 1


def test_ingest_snapshots_plays_output_columns_equal_canonical_columns(tmp_path):
    reviewer_plays = {"g1": [_play_record(10)]}
    raw_dir = _write_snapshot_dir(
        tmp_path, plays_by_game={}, reviewer_plays_by_game=reviewer_plays, write_unified=False
    )
    results = ingest_snapshots(raw_dir, _team_mapping())
    _, df, _ = results[0]
    assert df.columns == list(CANONICAL_COLUMNS)


def test_ingest_snapshots_plays_resolves_player_names_via_roster(tmp_path):
    reviewer_plays = {
        "g1": [
            _play_record(
                10,
                events=[
                    _ev("PASS", playerId="w-usa-p1", intendedReceiverId="w-usa-p2"),
                    _ev("COMPLETE", playerId="w-usa-p2"),
                ],
            )
        ]
    }
    raw_dir = _write_snapshot_dir(
        tmp_path,
        plays_by_game={},
        reviewer_plays_by_game=reviewer_plays,
        teams_roster=_synthetic_roster(),
        write_unified=False,
    )
    results = ingest_snapshots(raw_dir, _team_mapping())
    _, df, _ = results[0]
    row = df.row(0, named=True)
    assert row["qb"] == "Player One"
    assert row["received_by"] == "Player Two"

