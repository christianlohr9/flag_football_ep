"""Tests for HC workbook ingest: sheet reading, dtype-based block segmentation
(Task 2), and per-block contract mapping (Task 3).

Every fixture is built in-process with `openpyxl.Workbook()` using synthetic
team labels (`Alphaland`, `Betaland`) and synthetic player labels
(`Spieler A`, jersey `7`) -- no test may reference `data/raw/hc_files/` or
any real player/team name.
"""

from __future__ import annotations

from pathlib import Path

import openpyxl
import pytest

from flag_football_ep.ingest.hc_workbook import (
    HcBlock,
    SheetNotFoundError,
    hc_source_label,
    read_sheet_rows,
    segment_blocks,
    slugify,
)


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
