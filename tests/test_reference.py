"""Tests for reference-data loaders and the team mapping gate in `reference`."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.reference import (
    COMPETITION_TIERS,
    MissingReferenceFile,
    PlayerMappingResult,
    UnmappedCompetitionError,
    UnmappedTeamError,
    load_competition_tier,
    load_final_scores,
    load_group_opponents,
    load_half_boundaries,
    load_hc_games,
    load_player_mapping,
    load_sportapp_games,
    load_team_mapping,
    map_competition_tier,
    map_players,
    map_teams,
)


def _write_csv(tmp_path: Path, name: str, lines: list[str]) -> Path:
    path = tmp_path / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


# --- MissingReferenceFile ------------------------------------------------------


@pytest.mark.parametrize(
    "loader",
    [load_half_boundaries, load_final_scores, load_team_mapping, load_sportapp_games],
)
def test_loader_raises_missing_reference_file(tmp_path: Path, loader) -> None:
    missing = tmp_path / "does-not-exist.csv"
    with pytest.raises(MissingReferenceFile) as exc_info:
        loader(missing)
    assert str(missing) in str(exc_info.value)


# --- header-only files ------------------------------------------------------


def test_header_only_half_boundaries_loads_empty_with_warning(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, "half_boundaries.csv", ["filename,half2_first_play"])

    with pytest.warns(UserWarning):
        df = load_half_boundaries(path)

    assert df.height == 0
    assert df.schema["filename"] == pl.Utf8
    assert df.schema["half2_first_play"] == pl.Int32


def test_header_only_final_scores_loads_empty_with_warning(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path, "final_scores.csv", ["game_id,home_team,away_team,home_score,away_score,note"]
    )

    with pytest.warns(UserWarning):
        df = load_final_scores(path)

    assert df.height == 0


# --- load_final_scores ------------------------------------------------------


def test_load_final_scores_dtypes(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "final_scores.csv",
        [
            "game_id,home_team,away_team,home_score,away_score,note",
            "2026-06-14_GER-vs-AUT_EM,GER,AUT,20,12,IFAF match report",
        ],
    )

    df = load_final_scores(path)

    assert df.schema["home_score"] == pl.Int32
    assert df.schema["away_score"] == pl.Int32
    assert df.schema["game_id"] == pl.Utf8
    assert df.schema["home_team"] == pl.Utf8
    assert df.schema["away_team"] == pl.Utf8


def test_load_final_scores_duplicate_game_id_raises(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "final_scores.csv",
        [
            "game_id,home_team,away_team,home_score,away_score,note",
            "G1,GER,AUT,20,12,note1",
            "G1,GER,AUT,20,12,note2",
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        load_final_scores(path)

    assert "G1" in str(exc_info.value)


# --- load_half_boundaries ------------------------------------------------------


def test_load_half_boundaries_dtypes(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "half_boundaries.csv",
        ["filename,half2_first_play", "2026-06-14_GER-vs-AUT_EM.csv,42"],
    )

    df = load_half_boundaries(path)

    assert df.schema["filename"] == pl.Utf8
    assert df.schema["half2_first_play"] == pl.Int32


def test_load_half_boundaries_rejects_second_half_at_play_one(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "half_boundaries.csv",
        ["filename,half2_first_play", "game.csv,1"],
    )

    with pytest.raises(ValueError):
        load_half_boundaries(path)


# --- load_team_mapping ------------------------------------------------------


def test_load_team_mapping_duplicate_pair_raises(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "team_mapping.csv",
        [
            "source,source_team,canonical_team",
            "hudl,GER,GER",
            "hudl,GER,GER",
        ],
    )

    with pytest.raises(ValueError):
        load_team_mapping(path)


# --- map_teams ------------------------------------------------------


def _mapping_frame(rows: list[tuple[str, str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows, schema=["source", "source_team", "canonical_team"], orient="row"
    )


def test_map_teams_replaces_listed_columns(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "posteam": ["AFVD", "FRA"],
            "defteam": ["FRA", "AFVD"],
            "home_team": ["AFVD", "FRA"],
            "away_team": ["FRA", "AFVD"],
        }
    )
    mapping = _mapping_frame([("hudl", "AFVD", "GER"), ("hudl", "FRA", "FRA")])

    result = map_teams(df, mapping, "hudl", ["posteam", "defteam", "home_team", "away_team"])

    assert result["posteam"].to_list() == ["GER", "FRA"]
    assert result["defteam"].to_list() == ["FRA", "GER"]
    assert result["home_team"].to_list() == ["GER", "FRA"]
    assert result["away_team"].to_list() == ["FRA", "GER"]


def test_map_teams_raises_unmapped_team_error_listing_all_labels(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "posteam": ["AFVD", "UNKNOWN1"],
            "home_team": ["AFVD", "UNKNOWN2"],
        }
    )
    mapping = _mapping_frame([("hudl", "AFVD", "GER")])

    with pytest.raises(UnmappedTeamError) as exc_info:
        map_teams(df, mapping, "hudl", ["posteam", "home_team"])

    message = str(exc_info.value)
    assert "UNKNOWN1" in message
    assert "UNKNOWN2" in message
    assert "hudl" in message


def test_map_teams_empty_mapping_never_passes_through(tmp_path: Path) -> None:
    df = pl.DataFrame({"posteam": ["GER", "AUT"]})
    mapping = pl.DataFrame(
        {"source": [], "source_team": [], "canonical_team": []},
        schema=["source", "source_team", "canonical_team"],
    )

    with pytest.raises(UnmappedTeamError):
        map_teams(df, mapping, "hudl", ["posteam"])


# --- load_competition_tier ------------------------------------------------------


def test_load_competition_tier_returns_typed_frame(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "competition_tier.csv",
        [
            "source,competition,tier",
            "ifaf,IFAF World Flag 2026,womens-international",
            "legacy,legacy,mixed-other",
        ],
    )

    df = load_competition_tier(path)

    assert df.schema["source"] == pl.Utf8
    assert df.schema["competition"] == pl.Utf8
    assert df.schema["tier"] == pl.Utf8
    assert df.height == 2


def test_load_competition_tier_duplicate_pair_raises(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "competition_tier.csv",
        [
            "source,competition,tier",
            "ifaf,IFAF World Flag 2026,womens-international",
            "ifaf,IFAF World Flag 2026,mixed-other",
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        load_competition_tier(path)

    message = str(exc_info.value)
    assert "ifaf" in message
    assert "IFAF World Flag 2026" in message


def test_load_competition_tier_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.csv"

    with pytest.raises(MissingReferenceFile) as exc_info:
        load_competition_tier(missing)

    assert str(missing) in str(exc_info.value)


def test_load_competition_tier_header_only_loads_empty_with_warning(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, "competition_tier.csv", ["source,competition,tier"])

    with pytest.warns(UserWarning):
        df = load_competition_tier(path)

    assert df.height == 0
    assert df.schema["source"] == pl.Utf8
    assert df.schema["competition"] == pl.Utf8
    assert df.schema["tier"] == pl.Utf8


def test_load_competition_tier_invalid_tier_value_raises(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "competition_tier.csv",
        [
            "source,competition,tier",
            "ifaf,IFAF World Flag 2026,not-a-real-tier",
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        load_competition_tier(path)

    assert "not-a-real-tier" in str(exc_info.value)


# --- map_competition_tier ------------------------------------------------------


def _tier_frame(rows: list[tuple[str, str, str]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=["source", "competition", "tier"], orient="row")


def test_map_competition_tier_adds_column_preserving_order(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "source": ["ifaf", "legacy", "ifaf"],
            "competition": ["IFAF World Flag 2026", "legacy", "IFAF World Flag 2026"],
        }
    )
    mapping = _tier_frame(
        [
            ("ifaf", "IFAF World Flag 2026", "womens-international"),
            ("legacy", "legacy", "mixed-other"),
        ]
    )

    result = map_competition_tier(df, mapping)

    assert result.height == df.height
    assert result["competition_tier"].to_list() == [
        "womens-international",
        "mixed-other",
        "womens-international",
    ]
    assert result["source"].to_list() == df["source"].to_list()


def test_map_competition_tier_unmapped_pair_raises(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "source": ["ifaf", "unknown-source"],
            "competition": ["IFAF World Flag 2026", "unknown-competition"],
        }
    )
    mapping = _tier_frame([("ifaf", "IFAF World Flag 2026", "womens-international")])

    with pytest.raises(UnmappedCompetitionError) as exc_info:
        map_competition_tier(df, mapping)

    message = str(exc_info.value)
    assert "unknown-source" in message
    assert "unknown-competition" in message


def test_map_competition_tier_empty_mapping_raises(tmp_path: Path) -> None:
    df = pl.DataFrame({"source": ["ifaf"], "competition": ["IFAF World Flag 2026"]})
    mapping = pl.DataFrame(
        {"source": [], "competition": [], "tier": []},
        schema=["source", "competition", "tier"],
    )

    with pytest.raises(UnmappedCompetitionError):
        map_competition_tier(df, mapping)


def test_competition_tiers_is_fixed_vocabulary() -> None:
    assert COMPETITION_TIERS == ("womens-international", "womens-national", "mixed-other")


# --- load_player_mapping ------------------------------------------------------


def test_load_player_mapping_duplicate_pair_raises(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "player_mapping.csv",
        [
            "source,source_player,canonical_player",
            "hudl,S Nuhse,Saskia Nühse",
            "hudl,S Nuhse,Saskia Nühse",
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        load_player_mapping(path)

    assert "S Nuhse" in str(exc_info.value)


def test_load_player_mapping_header_only_loads_empty_with_warning(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, "player_mapping.csv", ["source,source_player,canonical_player"])

    with pytest.warns(UserWarning):
        df = load_player_mapping(path)

    assert df.height == 0
    assert df.schema["source"] == pl.Utf8
    assert df.schema["source_player"] == pl.Utf8
    assert df.schema["canonical_player"] == pl.Utf8


def test_load_player_mapping_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.csv"

    with pytest.raises(MissingReferenceFile) as exc_info:
        load_player_mapping(missing)

    assert str(missing) in str(exc_info.value)


# --- load_group_opponents ------------------------------------------------------


def test_load_group_opponents_duplicate_canonical_team_raises(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "group_opponents.csv",
        [
            "canonical_team,team_name",
            "AUT,Austria",
            "AUT,Austria",
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        load_group_opponents(path)

    assert "AUT" in str(exc_info.value)


def test_load_group_opponents_header_only_loads_empty_with_warning(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, "group_opponents.csv", ["canonical_team,team_name"])

    with pytest.warns(UserWarning):
        df = load_group_opponents(path)

    assert df.height == 0
    assert df.schema["canonical_team"] == pl.Utf8
    assert df.schema["team_name"] == pl.Utf8


def test_load_group_opponents_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.csv"

    with pytest.raises(MissingReferenceFile) as exc_info:
        load_group_opponents(missing)

    assert str(missing) in str(exc_info.value)


# --- map_players ------------------------------------------------------


def _player_mapping_frame(rows: list[tuple[str, str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows, schema=["source", "source_player", "canonical_player"], orient="row"
    )


def test_map_players_replaces_mapped_labels() -> None:
    df = pl.DataFrame({"thrown_by": ["S Nuhse", "K Fischer"]})
    mapping = _player_mapping_frame(
        [("hudl", "S Nuhse", "Saskia Nühse"), ("hudl", "K Fischer", "Kyra Fischer")]
    )

    result = map_players(df, mapping, "hudl", ["thrown_by"])

    assert isinstance(result, PlayerMappingResult)
    assert result.frame["thrown_by"].to_list() == ["Saskia Nühse", "Kyra Fischer"]
    assert result.unmapped == []


def test_map_players_leaves_unmapped_label_verbatim_and_lists_it() -> None:
    df = pl.DataFrame({"thrown_by": ["S Nuhse", "UNKNOWN"]})
    mapping = _player_mapping_frame([("hudl", "S Nuhse", "Saskia Nühse")])

    result = map_players(df, mapping, "hudl", ["thrown_by"])

    assert result.frame["thrown_by"].to_list() == ["Saskia Nühse", "UNKNOWN"]
    assert result.unmapped == ["UNKNOWN"]


def test_map_players_empty_mapping_returns_every_label_as_unmapped_and_raises_nothing() -> None:
    df = pl.DataFrame({"thrown_by": ["S Nuhse", "K Fischer", "S Nuhse"]})
    mapping = pl.DataFrame(
        {"source": [], "source_player": [], "canonical_player": []},
        schema=["source", "source_player", "canonical_player"],
    )

    result = map_players(df, mapping, "hudl", ["thrown_by"])

    assert result.frame["thrown_by"].to_list() == df["thrown_by"].to_list()
    assert result.unmapped == sorted(set(df["thrown_by"].to_list()))


def test_map_players_preserves_height_and_order() -> None:
    df = pl.DataFrame({"thrown_by": ["C", "A", "B", "A"]})
    mapping = _player_mapping_frame(
        [("hudl", "A", "Player A"), ("hudl", "B", "Player B"), ("hudl", "C", "Player C")]
    )

    result = map_players(df, mapping, "hudl", ["thrown_by"])

    assert result.frame.height == df.height
    assert result.frame["thrown_by"].to_list() == ["Player C", "Player A", "Player B", "Player A"]


def test_map_players_skips_column_not_in_frame() -> None:
    df = pl.DataFrame({"thrown_by": ["S Nuhse"]})
    mapping = _player_mapping_frame([("hudl", "S Nuhse", "Saskia Nühse")])

    result = map_players(df, mapping, "hudl", ["thrown_by", "received_by"])

    assert result.frame["thrown_by"].to_list() == ["Saskia Nühse"]
    assert result.unmapped == []


def test_map_players_never_raises_on_unmapped_labels() -> None:
    df = pl.DataFrame({"thrown_by": ["UNKNOWN1", "UNKNOWN2"]})
    mapping = pl.DataFrame(
        {"source": [], "source_player": [], "canonical_player": []},
        schema=["source", "source_player", "canonical_player"],
    )

    result = map_players(df, mapping, "hudl", ["thrown_by"])  # must not raise

    assert result.unmapped == ["UNKNOWN1", "UNKNOWN2"]


# --- load_hc_games ------------------------------------------------------

_HC_GAMES_HEADER = (
    "workbook,sheet,block_key,source_team1,source_team2,game_id,home_team,"
    "away_team,competition,season,game_date,tier,corpus_game_id,note"
)


def _hc_games_row(
    workbook: str = "offense-analytics-2026",
    sheet: str = "data",
    block_key: str = "b00-g00",
    source_team1: str = "",
    source_team2: str = "",
    game_id: str = "hc-2026-01-alphaland-betaland",
    home_team: str = "ALP",
    away_team: str = "BET",
    competition: str = "Camp",
    season: str = "2026",
    game_date: str = "2026-01-10",
    tier: str = "womens-national",
    corpus_game_id: str = "",
    note: str = "",
) -> str:
    return ",".join(
        [
            workbook,
            sheet,
            block_key,
            source_team1,
            source_team2,
            game_id,
            home_team,
            away_team,
            competition,
            season,
            game_date,
            tier,
            corpus_game_id,
            note,
        ]
    )


def test_load_hc_games_returns_typed_frame(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, "hc_games.csv", [_HC_GAMES_HEADER, _hc_games_row()])

    df = load_hc_games(path)

    assert df.schema["season"] == pl.Int32
    for col in (
        "workbook",
        "sheet",
        "block_key",
        "source_team1",
        "source_team2",
        "game_id",
        "home_team",
        "away_team",
        "competition",
        "game_date",
        "tier",
        "corpus_game_id",
        "note",
    ):
        assert df.schema[col] == pl.Utf8
    assert df.height == 1


def test_load_hc_games_duplicate_key_triple_raises(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "hc_games.csv",
        [
            _HC_GAMES_HEADER,
            _hc_games_row(game_id="hc-a"),
            _hc_games_row(game_id="hc-b"),
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        load_hc_games(path)

    message = str(exc_info.value)
    assert "offense-analytics-2026" in message
    assert "data" in message
    assert "b00-g00" in message


def test_load_hc_games_duplicate_game_id_raises(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        "hc_games.csv",
        [
            _HC_GAMES_HEADER,
            _hc_games_row(block_key="b00-g00", game_id="hc-dupe"),
            _hc_games_row(block_key="b00-g01", game_id="hc-dupe"),
        ],
    )

    with pytest.raises(ValueError) as exc_info:
        load_hc_games(path)

    assert "hc-dupe" in str(exc_info.value)


def test_load_hc_games_bad_tier_raises(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path, "hc_games.csv", [_HC_GAMES_HEADER, _hc_games_row(tier="not-a-real-tier")]
    )

    with pytest.raises(ValueError) as exc_info:
        load_hc_games(path)

    message = str(exc_info.value)
    assert "not-a-real-tier" in message
    for tier in COMPETITION_TIERS:
        assert tier in message


def test_load_hc_games_non_hc_prefixed_game_id_raises(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path, "hc_games.csv", [_HC_GAMES_HEADER, _hc_games_row(game_id="hudl-2026-01-10")]
    )

    with pytest.raises(ValueError) as exc_info:
        load_hc_games(path)

    assert "hudl-2026-01-10" in str(exc_info.value)


def test_load_hc_games_header_only_loads_empty_with_warning(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, "hc_games.csv", [_HC_GAMES_HEADER])

    with pytest.warns(UserWarning):
        df = load_hc_games(path)

    assert df.height == 0
    assert df.schema["season"] == pl.Int32


def test_load_hc_games_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.csv"

    with pytest.raises(MissingReferenceFile) as exc_info:
        load_hc_games(missing)

    assert str(missing) in str(exc_info.value)
