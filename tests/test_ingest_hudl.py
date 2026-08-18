"""Tests for new-format Hudl ingest: filename parsing, header validation, exact-token
RESULT grammar, and the full derivation chain into a conformed canonical frame.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.canonical import CANONICAL_COLUMNS, MissingCanonicalColumns
from flag_football_ep.ingest.hudl import (
    FilenameError,
    WrongDelimiterError,
    derive_outcome_columns,
    ingest_dir,
    ingest_file,
    parse_filename,
    parse_result_tokens,
    read_export,
)
from flag_football_ep.reference import UnmappedTeamError
from flag_football_ep.validation.schema import MissingCoreColumnsError, load_contract


@pytest.fixture
def contract(contract_path: Path):
    return load_contract(contract_path)


# --- parse_filename -----------------------------------------------------------


def test_parse_filename_primary_pattern() -> None:
    meta = parse_filename(Path("2026-06-14_GER-vs-AUT_EM-QUALI.csv"))
    assert meta.game_id == "2026-06-14_GER-vs-AUT_EM-QUALI"
    assert meta.season == 2026
    assert meta.game_date == "2026-06-14"
    assert meta.team1 == "GER"
    assert meta.team2 == "AUT"
    assert meta.competition == "EM-QUALI"


def test_parse_filename_fallback_pattern_preserves_ordinal() -> None:
    meta = parse_filename(Path("2024_GER-vs-SLO_WC_2.csv"))
    assert meta.season == 2024
    assert meta.game_date is None
    assert meta.team1 == "GER"
    assert meta.team2 == "SLO"
    assert meta.competition == "WC"
    assert meta.game_id == "2024_GER-vs-SLO_WC_2"


def test_parse_filename_fallback_pattern_no_competition() -> None:
    meta = parse_filename(Path("2024_GER-vs-SLO.csv"))
    assert meta.competition == ""
    assert meta.game_id == "2024_GER-vs-SLO"


def test_parse_filename_invalid_raises_naming_both_patterns() -> None:
    with pytest.raises(FilenameError) as exc_info:
        parse_filename(Path("GERvsPAN.csv"))
    message = str(exc_info.value)
    assert "YYYY-MM-DD" in message
    assert "YYYY_" in message


# --- read_export -----------------------------------------------------------


def test_read_export_semicolon_utf8sig(tmp_path: Path, make_hudl_csv) -> None:
    path = make_hudl_csv(
        tmp_path,
        "game.csv",
        [
            {
                "PLAY #": "1",
                "ODK": "O",
                "DN": "1",
                "DIST": "10",
                "YARD LN": "25",
                "PLAY TYPE": "Rush",
                "RESULT": "Complete",
                "GN/LS": "5",
            }
        ],
    )
    df = read_export(path)
    assert df.columns[0] == "PLAY #"
    assert all(dtype == pl.Utf8 for dtype in df.schema.values())
    assert df.height == 1


def test_read_export_wrong_delimiter_raises(tmp_path: Path) -> None:
    path = tmp_path / "comma.csv"
    path.write_text("PLAY #,ODK,DN\n1,O,1\n", encoding="utf-8")
    with pytest.raises(WrongDelimiterError):
        read_export(path)


# --- ingest_file: header validation and domain checks (Task 1 scope) -----------------------------------------------------------


def test_ingest_file_missing_result_raises(tmp_path: Path, contract, make_hudl_csv) -> None:
    path = make_hudl_csv(
        tmp_path,
        "2026-06-14_GER-vs-AUT_EM.csv",
        [
            {
                "PLAY #": "1",
                "ODK": "O",
                "DN": "1",
                "DIST": "10",
                "YARD LN": "25",
                "PLAY TYPE": "Rush",
                "GN/LS": "5",
            }
        ],
        columns=["PLAY #", "ODK", "DN", "DIST", "YARD LN", "PLAY TYPE", "GN/LS"],
    )
    with pytest.raises(MissingCoreColumnsError):
        ingest_file(path, contract)


def test_ingest_file_12_column_export_materializes_optional_as_null(
    tmp_path: Path, contract, make_hudl_csv
) -> None:
    cols = [
        "PLAY #", "ODK", "DN", "DIST", "YARD LN", "PLAY TYPE", "RESULT", "GN/LS",
        "HASH", "OFF FORM", "OFF PLAY", "QTR",
    ]
    rows = [
        {"PLAY #": "1", "ODK": "O", "DN": "1", "DIST": "10", "YARD LN": "25",
         "PLAY TYPE": "Rush", "RESULT": "Complete", "GN/LS": "5"},
        {"PLAY #": "2", "ODK": "O", "DN": "2", "DIST": "10", "YARD LN": "30",
         "PLAY TYPE": "Rush", "RESULT": "Rush", "GN/LS": "5"},
    ]
    path = make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", rows, columns=cols)

    df, notices = ingest_file(path, contract)

    assert df.height == 2
    assert "COVERAGE" in notices.header.materialized_optional


def test_ingest_file_unknown_columns_recorded_not_fatal(
    tmp_path: Path, contract, make_hudl_csv
) -> None:
    cols = [
        "PLAY #", "ODK", "DN", "DIST", "YARD LN", "PLAY TYPE", "RESULT", "GN/LS",
        "TARGET ROUTE", "MOTION", "WRISTCOACH #",
    ]
    rows = [
        {"PLAY #": "1", "ODK": "O", "DN": "1", "DIST": "10", "YARD LN": "25",
         "PLAY TYPE": "Rush", "RESULT": "Complete", "GN/LS": "5"}
    ]
    path = make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", rows, columns=cols)

    df, notices = ingest_file(path, contract)

    assert set(notices.header.unknown) >= {"TARGET ROUTE", "MOTION", "WRISTCOACH #"}


def test_ingest_file_domain_violation_recorded_not_raised(
    tmp_path: Path, contract, make_hudl_csv
) -> None:
    rows = [
        {"PLAY #": "1", "ODK": "O", "DN": "7", "DIST": "10", "YARD LN": "25",
         "PLAY TYPE": "Rush", "RESULT": "Complete", "GN/LS": "5"}
    ]
    path = make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", rows)

    df, notices = ingest_file(path, contract)

    dn_violations = [v for v in notices.domain if v.column == "DN"]
    assert len(dn_violations) == 1


def test_ingest_file_real_samples_do_not_raise(hudl_sample_paths, contract) -> None:
    for path in hudl_sample_paths:
        ingest_file(path, contract)


# --- RESULT grammar and outcome derivation (Task 2 scope) -----------------------------------------------------------


def _result_frame(result: str, down: int = 1, yardline_50: int = 25) -> pl.DataFrame:
    return pl.DataFrame(
        {"RESULT": [result], "down": [down], "yardline_50": [yardline_50]},
        schema={"RESULT": pl.Utf8, "down": pl.Int32, "yardline_50": pl.Int32},
    )


def _outcome_row(result: str, down: int = 1, yardline_50: int = 25) -> dict:
    df = parse_result_tokens(_result_frame(result, down=down, yardline_50=yardline_50))
    df, _messages = derive_outcome_columns(df)
    return df.row(0, named=True)


@pytest.mark.parametrize(
    "result,expected",
    [
        ("Complete", {"complete_pass": 1, "incomplete_pass": 0, "play_type": "pass"}),
        ("Incomplete", {"complete_pass": 0, "incomplete_pass": 1, "play_type": "pass"}),
        ("Complete, TD", {"complete_pass": 1, "touchdown": 1}),
        ("Rush, TD", {"play_type": "run", "touchdown": 1, "complete_pass": 0}),
        ("Def TD", {"touchdown": 0, "def_touchdown": 1}),
        ("No Good", {"no_good": 1}),
        ("Good", {"one_point_conv_success": 0, "two_point_conv_success": 0}),
        ("Penalty", {"play_type": "no_play", "penalty": 1}),
        # Penalty nullifies regardless of base token (REVIEW WR-03): both flagged
        # variants are no_play, with the base outcome kept in the flag columns.
        ("Complete, Penalty", {"play_type": "no_play", "penalty": 1, "complete_pass": 1}),
        ("Rush, Penalty", {"play_type": "no_play", "penalty": 1}),
        ("KNEEL", {"play_type": "qb_kneel"}),
        ("Sack", {"sack": 1, "play_type": "pass"}),
        ("Interception", {"interception": 1}),
        ("Fumble", {"fumble": 1}),
        ("", {"play_type": None}),
    ],
)
def test_result_grammar_table(result: str, expected: dict) -> None:
    row = _outcome_row(result)
    for key, value in expected.items():
        assert row[key] == value, f"{result!r}: {key} expected {value}, got {row[key]}"


def test_incomplete_leaves_complete_pass_zero() -> None:
    assert _outcome_row("Incomplete")["complete_pass"] == 0


def test_def_td_leaves_touchdown_zero() -> None:
    assert _outcome_row("Def TD")["touchdown"] == 0


def test_unknown_token_recorded_not_raised() -> None:
    df = parse_result_tokens(_result_frame("Blorp"))
    df, messages = derive_outcome_columns(df)
    assert any("Blorp" in m for m in messages)


def test_empty_result_non_pat_yields_null_play_type_and_notice() -> None:
    df = parse_result_tokens(_result_frame("", down=2))
    df, messages = derive_outcome_columns(df)
    assert df["play_type"][0] is None
    assert any("empty RESULT" in m for m in messages)


def test_empty_result_pat_yields_extra_point() -> None:
    assert _outcome_row("", down=0)["play_type"] == "extra_point"


def test_one_point_conv_success_requires_good_down0_yardline45() -> None:
    row = _outcome_row("Good", down=0, yardline_50=45)
    assert row["one_point_conv_success"] == 1
    assert row["two_point_conv_success"] == 0


def test_two_point_conv_success_requires_good_down0_yardline40() -> None:
    row = _outcome_row("Good", down=0, yardline_50=40)
    assert row["two_point_conv_success"] == 1
    assert row["one_point_conv_success"] == 0


def test_defensive_two_point_conv_requires_def_td_down0() -> None:
    assert _outcome_row("Def TD", down=0)["defensive_two_point_conv"] == 1


# --- identity, sequence and score derivation to a conformed canonical frame (Task 3 scope) --------------------------


def _play_row(play_num: int, odk: str, result: str = "Rush", **overrides: str) -> dict:
    row = {
        "PLAY #": str(play_num),
        "ODK": odk,
        "DN": "1",
        "DIST": "10",
        "YARD LN": "25",
        "PLAY TYPE": "Rush",
        "RESULT": result,
        "GN/LS": "5",
    }
    row.update(overrides)
    return row


def test_posteam_team1_rule(tmp_path: Path, contract, make_hudl_csv) -> None:
    rows = [_play_row(1, "O"), _play_row(2, "D")]
    path = make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", rows)

    df, _notices = ingest_file(path, contract)

    assert df["posteam"].to_list() == ["GER", "AUT"]
    assert df["defteam"].to_list() == ["AUT", "GER"]
    assert df["home_team"].to_list() == ["GER", "GER"]
    assert df["away_team"].to_list() == ["AUT", "AUT"]


def test_odk_k_rows_get_kickoff_play_type(tmp_path: Path, contract, make_hudl_csv) -> None:
    rows = [_play_row(1, "K", result="")]
    path = make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", rows)

    df, _notices = ingest_file(path, contract)

    assert df["play_type"][0] == "kickoff"
    assert df["posteam"][0] == "AUT"


def test_unmapped_team_raises(tmp_path: Path, contract, make_hudl_csv) -> None:
    rows = [_play_row(1, "O")]
    path = make_hudl_csv(tmp_path, "2026-06-14_XXX-vs-AUT_EM.csv", rows)
    mapping = pl.DataFrame(
        {"source": ["hudl"], "source_team": ["AUT"], "canonical_team": ["AUT"]}
    )

    with pytest.raises(UnmappedTeamError):
        ingest_file(path, contract, team_mapping=mapping)


def test_drive_id_from_odk_flips(tmp_path: Path, contract, make_hudl_csv) -> None:
    rows = [
        _play_row(1, "O"),
        _play_row(2, "O"),
        _play_row(3, "D"),
        _play_row(4, "O"),
    ]
    path = make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", rows)

    df, _notices = ingest_file(path, contract)

    assert df["drive_id"].to_list() == [1, 1, 2, 3]


def test_drive_closing_result_increments_drive_id_without_odk_flip(
    tmp_path: Path, contract, make_hudl_csv
) -> None:
    rows = [_play_row(1, "O", result="Rush, TD"), _play_row(2, "O")]
    path = make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", rows)

    df, _notices = ingest_file(path, contract)

    assert df["drive_id"].to_list() == [1, 2]


def test_half_boundary_missing_yields_null_and_notice(
    tmp_path: Path, contract, make_hudl_csv
) -> None:
    rows = [_play_row(1, "O")]
    path = make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", rows)
    half_boundaries = pl.DataFrame(
        {"filename": [], "half2_first_play": []},
        schema={"filename": pl.Utf8, "half2_first_play": pl.Int32},
    )

    df, notices = ingest_file(path, contract, half_boundaries=half_boundaries)

    assert df["half"][0] is None
    assert any("no half boundary" in m for m in notices.messages)


def test_ingest_file_output_columns_equal_canonical(
    tmp_path: Path, contract, make_hudl_csv
) -> None:
    rows = [_play_row(1, "O")]
    path = make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", rows)

    df, _notices = ingest_file(path, contract)

    assert tuple(df.columns) == CANONICAL_COLUMNS


def test_defensive_safety_credits_defense_in_scoring_play_team(
    tmp_path: Path, contract, make_hudl_csv
) -> None:
    rows = [_play_row(1, "O", result="Sack, Safety", **{"YARD LN": "-45", "GN/LS": "-5"})]
    path = make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", rows)

    df, _notices = ingest_file(path, contract)

    assert df["scoring_play_team"][0] == "AUT"


def test_ingest_dir_reports_bad_filename_and_returns_good_one(
    tmp_path: Path, contract, make_hudl_csv
) -> None:
    good_rows = [_play_row(1, "O")]
    make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", good_rows)
    make_hudl_csv(tmp_path, "not-a-valid-name.csv", good_rows)

    results = ingest_dir(tmp_path, contract)

    assert len(results) == 2
    good = [r for r in results if r[1] is not None]
    bad = [r for r in results if r[1] is None]
    assert len(good) == 1
    assert len(bad) == 1
    assert any("skipped" in m for m in bad[0][2].messages)


# --- Task 1 (plan 20): non-strict identity casts and complete per-file containment ----


def test_non_numeric_dn_cell_ingests_with_null_down_and_domain_notice(
    tmp_path: Path, contract, make_hudl_csv
) -> None:
    rows = [_play_row(1, "O", **{"DN": "not-a-number"})]
    path = make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", rows)

    df, notices = ingest_file(path, contract)

    assert df["down"][0] is None
    dn_violations = [v for v in notices.domain if v.column == "DN"]
    assert len(dn_violations) == 1


def test_ingest_dir_comma_delimited_file_does_not_block_sibling_valid_export(
    tmp_path: Path, contract, make_hudl_csv
) -> None:
    good_rows = [_play_row(1, "O")]
    make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", good_rows)
    comma_path = tmp_path / "2027_GER-vs-SLO.csv"
    comma_path.write_text("PLAY #,ODK,DN\n1,O,1\n", encoding="utf-8")

    results = ingest_dir(tmp_path, contract)

    assert len(results) == 2
    good = [r for r in results if r[1] is not None]
    bad = [r for r in results if r[1] is None]
    assert len(good) == 1
    assert good[0][1].height == 1
    assert len(bad) == 1
    assert any("WrongDelimiterError" in m for m in bad[0][2].messages)


def test_ingest_dir_missing_canonical_columns_skips_that_file_only(
    tmp_path: Path, contract, make_hudl_csv, monkeypatch
) -> None:
    # Constructing a real MissingCanonicalColumns case from raw CSV would require
    # deleting a core column, which validate_header already turns into
    # MissingCoreColumnsError before conform_to_canonical ever runs -- monkeypatching
    # conform_to_canonical directly is the only way to exercise this specific except
    # branch in isolation.
    good_rows = [_play_row(1, "O")]
    make_hudl_csv(tmp_path, "2026-06-14_GER-vs-AUT_EM.csv", good_rows)
    make_hudl_csv(tmp_path, "2026-06-15_GER-vs-SLO_EM.csv", good_rows)

    import flag_football_ep.ingest.hudl as hudl_mod

    real_conform = hudl_mod.conform_to_canonical

    def _flaky_conform(df, source):
        if df["game_id"][0] == "2026-06-15_GER-vs-SLO_EM":
            raise MissingCanonicalColumns("simulated missing core column")
        return real_conform(df, source)

    monkeypatch.setattr(hudl_mod, "conform_to_canonical", _flaky_conform)

    results = ingest_dir(tmp_path, contract)

    by_game_id = {meta.game_id: (df, notices) for meta, df, notices in results}
    assert by_game_id["2026-06-15_GER-vs-SLO_EM"][0] is None
    assert any(
        "MissingCanonicalColumns" in m
        for m in by_game_id["2026-06-15_GER-vs-SLO_EM"][1].messages
    )
    assert by_game_id["2026-06-14_GER-vs-AUT_EM"][0] is not None
    assert by_game_id["2026-06-14_GER-vs-AUT_EM"][0].height == 1


def test_ingest_dir_still_propagates_unmapped_team_error(
    tmp_path: Path, contract, make_hudl_csv
) -> None:
    rows = [_play_row(1, "O")]
    make_hudl_csv(tmp_path, "2026-06-14_XXX-vs-AUT_EM.csv", rows)
    mapping = pl.DataFrame(
        {"source": ["hudl"], "source_team": ["AUT"], "canonical_team": ["AUT"]}
    )

    with pytest.raises(UnmappedTeamError):
        ingest_dir(tmp_path, contract, team_mapping=mapping)
