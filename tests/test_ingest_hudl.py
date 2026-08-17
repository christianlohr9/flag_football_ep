"""Tests for new-format Hudl ingest: filename parsing, header validation, exact-token
RESULT grammar, and the full derivation chain into a conformed canonical frame.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.ingest.hudl import (
    FilenameError,
    WrongDelimiterError,
    derive_outcome_columns,
    ingest_file,
    parse_filename,
    parse_result_tokens,
    read_export,
)
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
        ("Complete, Penalty", {"penalty": 1, "complete_pass": 1}),
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
