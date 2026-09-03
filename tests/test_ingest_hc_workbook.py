"""Tests for HC workbook ingest: sheet reading, dtype-based block segmentation
(Task 2), and per-block contract mapping (Task 3).

Every fixture is built in-process with `openpyxl.Workbook()` using synthetic
team labels (`Alphaland`, `Betaland`) and synthetic player labels
(`Spieler A`, jersey `7`) -- no test may open a real gitignored HC workbook
or reference any real player/team name.
"""

from __future__ import annotations

from pathlib import Path

import openpyxl
import pytest

from flag_football_ep.ingest.hc_workbook import (
    HcBlock,
    SheetNotFoundError,
    hc_source_label,
    map_block_to_frame,
    read_sheet_rows,
    segment_blocks,
    slugify,
)
from flag_football_ep.validation.schema import HeaderReport, load_contract


def _make_workbook(tmp_path: Path, sheets: dict[str, list[list]]) -> Path:
    """Write a real .xlsx with one sheet per (name, rows) pair; rows[0] is the header."""
    wb = openpyxl.Workbook()
    default = wb.active
    first = True
    for name, rows in sheets.items():
        ws = default if first else wb.create_sheet(name)
        if first:
            ws.title = name
            first = False
        for row in rows:
            ws.append(row)
    path = tmp_path / "hc_test_workbook.xlsx"
    wb.save(path)
    return path


# --- slugify / hc_source_label ---------------------------------------------


def test_slugify_lowercases_and_collapses_non_alnum_runs() -> None:
    assert slugify("Offense Analytics 2026 Camps and Competitions") == (
        "offense-analytics-2026-camps-and-competitions"
    )
    assert slugify("Copy of Data") == "copy-of-data"


def test_hc_source_label_data_sheet() -> None:
    path = Path("Offense Analytics 2026 Camps and Competitions.xlsx")
    assert hc_source_label(path, "Data") == (
        "hc_workbook:offense-analytics-2026-camps-and-competitions:data"
    )


def test_hc_source_label_copy_of_data_sheet() -> None:
    path = Path("Offense Analytics 2026 Camps and Competitions.xlsx")
    assert hc_source_label(path, "Copy of Data") == (
        "hc_workbook:offense-analytics-2026-camps-and-competitions:copy-of-data"
    )


# --- read_sheet_rows ---------------------------------------------------------


def test_read_sheet_rows_skips_blank_rows_and_counts_them(tmp_path: Path) -> None:
    header = ["PLAY #", "ODK", "RESULT"]
    rows = [
        header,
        [1, "O", "Complete"],
        [None, None, None],
        [2, "O", "Incomplete"],
    ]
    path = _make_workbook(tmp_path, {"Data": rows})

    header_out, kept_rows, messages = read_sheet_rows(path, "Data")

    assert list(header_out) == header
    assert len(kept_rows) == 2
    assert kept_rows[0][0] == 2  # physical row number
    assert kept_rows[1][0] == 4
    joined = " ".join(messages)
    assert "1" in joined
    assert "übersprungen" in joined or "blank" in joined


def test_read_sheet_rows_empty_sheet_reports_leer_with_counts(tmp_path: Path) -> None:
    header = ["PLAY #", "ODK", "RESULT"]
    rows = [header] + [[None, None, None] for _ in range(200)]
    path = _make_workbook(tmp_path, {"Data": rows})

    _, kept_rows, messages = read_sheet_rows(path, "Data")

    assert kept_rows == []
    joined = " ".join(messages)
    assert "leer" in joined or "empty" in joined
    assert "200" in joined
    assert "0 Datenzeile" in joined


def test_read_sheet_rows_counts_na_cells(tmp_path: Path) -> None:
    header = ["PLAY #", "ODK", "RESULT"]
    rows = [
        header,
        [1, "O", "#N/A"],
        [2, "O", "Complete"],
    ]
    path = _make_workbook(tmp_path, {"Data": rows})

    _, _, messages = read_sheet_rows(path, "Data")

    assert any("#N/A" in m for m in messages)


def test_read_sheet_rows_missing_sheet_raises_with_available_names(tmp_path: Path) -> None:
    path = _make_workbook(tmp_path, {"Data": [["PLAY #", "ODK"], [1, "O"]]})

    with pytest.raises(SheetNotFoundError) as exc_info:
        read_sheet_rows(path, "Copy of Data")

    message = str(exc_info.value)
    assert "Copy of Data" in message
    assert "Data" in message


# --- segment_blocks -----------------------------------------------------------


def test_block_segmentation_pair_then_numeric(tmp_path: Path) -> None:
    header = ["PLAY #", "ODK", "OFF FORM"]
    pair_rows = [["Alphaland", "Betaland", "DOG"] for _ in range(5)]
    numeric_rows = [[i, "O", "DOG"] for i in range(1, 8)]
    rows = [header] + pair_rows + numeric_rows
    path = _make_workbook(tmp_path, {"Data": rows})

    header_out, kept_rows, _ = read_sheet_rows(path, "Data")
    blocks, messages = segment_blocks(header_out, kept_rows)

    assert len(blocks) == 2
    assert isinstance(blocks[0], HcBlock)
    assert blocks[0].kind == "pair"
    assert blocks[0].index == 0
    assert len(blocks[0].rows) == 5
    assert blocks[1].kind == "numeric"
    assert blocks[1].index == 1
    assert len(blocks[1].rows) == 7
    assert blocks[0].last_row < blocks[1].first_row
    assert any("Block 0" in m for m in messages)
    assert any("Block 1" in m for m in messages)


def test_block_segmentation_float_first_cell_is_numeric(tmp_path: Path) -> None:
    header = ["PLAY #", "ODK"]
    rows = [header, [1.0, "O"], [2.0, "O"]]
    path = _make_workbook(tmp_path, {"Data": rows})

    header_out, kept_rows, _ = read_sheet_rows(path, "Data")
    blocks, _ = segment_blocks(header_out, kept_rows)

    assert len(blocks) == 1
    assert blocks[0].kind == "numeric"


def test_block_segmentation_boolean_first_cell_not_numeric(tmp_path: Path) -> None:
    header = ["PLAY #", "ODK"]
    rows = [header, [True, "O"]]
    path = _make_workbook(tmp_path, {"Data": rows})

    header_out, kept_rows, _ = read_sheet_rows(path, "Data")
    blocks, _ = segment_blocks(header_out, kept_rows)

    assert len(blocks) == 0


def test_block_segmentation_skips_empty_first_cell_without_new_block(tmp_path: Path) -> None:
    header = ["PLAY #", "ODK", "RESULT"]
    rows = [
        header,
        [1, "O", "Complete"],
        ["", None, "orphan note"],
        [2, "O", "Incomplete"],
    ]
    path = _make_workbook(tmp_path, {"Data": rows})

    header_out, kept_rows, _ = read_sheet_rows(path, "Data")
    blocks, _ = segment_blocks(header_out, kept_rows)

    assert len(blocks) == 1
    assert blocks[0].kind == "numeric"
    assert len(blocks[0].rows) == 2


def test_block_segmentation_twenty_consecutive_numeric_rows_is_one_block(tmp_path: Path) -> None:
    header = ["PLAY #", "ODK"]
    rows = [header] + [[i, "O"] for i in range(1, 21)]
    path = _make_workbook(tmp_path, {"Data": rows})

    header_out, kept_rows, _ = read_sheet_rows(path, "Data")
    blocks, _ = segment_blocks(header_out, kept_rows)

    assert len(blocks) == 1
    assert len(blocks[0].rows) == 20


# --- map_block_to_frame (Task 3) --------------------------------------------

# Offense Analytics 2026 Camps and Competitions.xlsx :: Data (M3-01-RESEARCH.md
# "Verified workbook facts") -- one clean numeric block, no pair rows at all.
OFFENSE_ANALYTICS_HEADER = [
    "PLAY #", "ODK", "OFF FORM", "OFF STR", "OFF PLAY", "DN", "DIST", "YARD LN",
    "RESULT", "GN/LS", "TARGET ROUTE", "RECEIVED BY", "AIR YARDS", "Hand",
    "Efficiency", "Thrown By", "Target", "X", "S", "C", "Q", "Y", "Drop", "B",
]

# Scoring Probability by Situation 2023-2026.xlsx :: Data -- the workbook
# with team-name-pair rows; note GN/LS sits AFTER RECEIVED BY here (unlike
# Offense Analytics), matching M3-01-RESEARCH.md Pitfall 2's finding that
# GN/LS is itself part of the pair block's unresolved tail in this workbook.
SCORING_PROBABILITY_HEADER = [
    "PLAY #", "ODK", "OFF FORM", "OFF STR", "OFF PLAY", "DN", "DIST", "YARD LN",
    "RESULT", "Drive Success", "TARGET ROUTE", "RECEIVED BY", "GN/LS", "Thrown By",
    "YAC", "QB", "C", "X/H", "Y/CAT", "Z", "Target", "Drop", "B",
]


@pytest.fixture
def contract(contract_path: Path):
    return load_contract(contract_path)


def _numeric_block(header: list, data_rows: list[list]) -> HcBlock:
    rows = [(i + 2, tuple(row)) for i, row in enumerate(data_rows)]
    return HcBlock(
        index=0, kind="numeric", header=header, rows=rows,
        first_row=rows[0][0], last_row=rows[-1][0],
    )


def _pair_block(header: list, data_rows: list[list]) -> HcBlock:
    rows = [(i + 2, tuple(row)) for i, row in enumerate(data_rows)]
    return HcBlock(
        index=0, kind="pair", header=header, rows=rows,
        first_row=rows[0][0], last_row=rows[-1][0],
    )


_OFFENSE_ROW = [
    1.0, "O", "DOG", "RIGHT", "SWEEP", 1.0, 10.0, -20.0, "Rush", 5.0,
    "SLANT", "Spieler A", 3.0, "R", 1.0, "Spieler B", "7", "", "", "", "", "", "", "",
]


def test_header_and_block_mapping_numeric_block_materializes_play_type(contract) -> None:
    block = _numeric_block(OFFENSE_ANALYTICS_HEADER, [_OFFENSE_ROW])

    df, _, _, messages = map_block_to_frame(block, contract)

    for name in ("PLAY #", "ODK", "DN", "DIST", "YARD LN", "RESULT", "GN/LS"):
        assert name in df.columns
    assert "PLAY TYPE" in df.columns
    assert df["PLAY TYPE"].null_count() == df.height
    assert any("PLAY TYPE" in m for m in messages)


def test_header_and_block_mapping_validate_header_never_raises_numeric(contract) -> None:
    block = _numeric_block(OFFENSE_ANALYTICS_HEADER, [_OFFENSE_ROW])

    df, header_report, _, _ = map_block_to_frame(block, contract)  # must not raise

    assert isinstance(header_report, HeaderReport)
    assert header_report.missing_core == []


def test_header_and_block_mapping_validate_header_never_raises_pair(contract) -> None:
    pair_row = [
        "Alphaland", "Betaland", "DOG", "RIGHT", "SWEEP", 1.0, 10.0, -20.0,
        "Rush", 1.0, "SLANT", "Spieler A", 5.0, "Spieler B", 3.0, "7",
        "", "", "", "", "", "", "",
    ]
    block = _pair_block(SCORING_PROBABILITY_HEADER, [pair_row])

    df, header_report, _, _ = map_block_to_frame(block, contract)  # must not raise

    assert header_report.missing_core == []


def test_pair_block_tail_nulled_with_notice(contract) -> None:
    pair_row = [
        "Alphaland", "Betaland", "DOG", "RIGHT", "SWEEP", 1.0, 10.0, -20.0,
        "Rush", 1.0, "SLANT", "Spieler A", 5.0, "Spieler B", 3.0, "7",
        "", "", "", "", "", "", "",
    ]
    block = _pair_block(SCORING_PROBABILITY_HEADER, [pair_row])

    df, _, _, messages = map_block_to_frame(block, contract)

    # columns through TARGET ROUTE carry real values
    assert df["target_route"][0] == "SLANT"
    assert df["OFF FORM"][0] == "DOG"
    # RECEIVED BY onward is null (renamed to received_by); GN/LS sits after
    # RECEIVED BY in this workbook's real header, so it is null here too
    assert df["received_by"].null_count() == 1
    assert df["GN/LS"].null_count() == 1
    # PLAY #/ODK are null for pair-block rows, but the raw team-pair values
    # survive under dedicated names for plan M3-01-03's game-identity key
    assert df["PLAY #"].null_count() == 1
    assert df["ODK"].null_count() == 1
    assert df["hc_pair_team1"][0] == "Alphaland"
    assert df["hc_pair_team2"][0] == "Betaland"
    joined = " ".join(messages)
    assert "RECEIVED BY" in joined
    assert "Frage 2" in joined or "offen" in joined


def test_jersey_string_integral_float_becomes_plain_integer_string(contract) -> None:
    block = _numeric_block(OFFENSE_ANALYTICS_HEADER, [_OFFENSE_ROW])

    df, _, _, _ = map_block_to_frame(block, contract)

    assert df["air_yards"][0] == "3"
    assert df["PLAY #"][0] == "1"


def test_result_negative_float_preserved_dn_out_of_range_flagged(contract) -> None:
    row = list(_OFFENSE_ROW)
    row[5] = 7.0  # DN out of the contract's [0, 4] range
    row[8] = -5.0  # RESULT: the real data-entry error from the corpus
    block = _numeric_block(OFFENSE_ANALYTICS_HEADER, [row])

    df, _, domain_violations, _ = map_block_to_frame(block, contract)

    assert df["RESULT"][0] == "-5.0"
    dn_violations = [v for v in domain_violations if v.column == "DN" and v.rule == "range"]
    assert dn_violations, f"expected a DN range violation, got {domain_violations}"


def test_unknown_headers_named_not_silently_dropped(contract) -> None:
    block = _numeric_block(OFFENSE_ANALYTICS_HEADER, [_OFFENSE_ROW])

    _, _, _, messages = map_block_to_frame(block, contract)

    joined = " ".join(messages)
    for unmapped_header in ("X", "S", "C", "Q", "Y", "Drop", "B"):
        assert unmapped_header in joined


def test_unknown_headers_charting_extras_are_renamed_not_listed(contract) -> None:
    block = _numeric_block(OFFENSE_ANALYTICS_HEADER, [_OFFENSE_ROW])

    df, _, _, _ = map_block_to_frame(block, contract)

    for extra in ("air_yards", "hand", "efficiency", "target_route", "received_by", "thrown_by", "target"):
        assert extra in df.columns


def test_header_dedup_appends_suffix_and_names_it(contract) -> None:
    header = list(OFFENSE_ANALYTICS_HEADER)
    header[18] = "C"  # duplicate of the existing "C" at index 19 -> "C" / "C_2"
    block = _numeric_block(header, [_OFFENSE_ROW])

    df, _, _, messages = map_block_to_frame(block, contract)

    assert "C" in df.columns
    assert "C_2" in df.columns
    assert any("C_2" in m for m in messages)
