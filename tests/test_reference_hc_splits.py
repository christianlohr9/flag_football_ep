"""Tests for `resolve_hc_game_splits` and `load_hc_splits`'s validation rules.

Every `resolve_hc_game_splits` test below runs on a synthetic games frame and
a synthetic splits frame built in this module -- `data/reference/hc_games.csv`
is owned and being rewritten by a concurrent plan (M3-02-04), so this module
never reads it. `data/reference/hc_splits.csv` (the real, committed
reference file) is read exactly once, through `load_hc_splits`, to exercise
its validation rules against real data.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from flag_football_ep.reference import load_hc_splits, resolve_hc_game_splits

REPO_ROOT = Path(__file__).resolve().parents[1]
HC_SPLITS_CSV = REPO_ROOT / "data" / "reference" / "hc_splits.csv"

_WORKBOOK = "offense-analytics-2026-camps-and-competitions"
_SHEET = "data"


def _splits_frame() -> pl.DataFrame:
    """A small synthetic splits frame mirroring hc_splits.csv's real windows."""
    return pl.DataFrame(
        {
            "workbook": [_WORKBOOK] * 5,
            "sheet": [_SHEET] * 5,
            "first_row": [2, 1001, 2001, 3001, 4001],
            "last_row": [1000, 2000, 3000, 4000, 5000],
            "split_key": ["camp-i", "mexico", "camp-iii", "camp-iv-vi", "camp-v"],
            "label_de": [
                "Camp I (March Camp)",
                "Mexico",
                "Camp III (vs Switzerland)",
                "Camp IV/VI (unklar benannt)",
                "Camp V",
            ],
            "label_status": ["verified", "verified", "verified", "conflict", "verified"],
        },
        schema_overrides={"first_row": pl.Int32, "last_row": pl.Int32},
    )


def _game(
    game_id: str,
    note: str,
    workbook: str = _WORKBOOK,
    sheet: str = _SHEET,
) -> dict:
    return {"game_id": game_id, "workbook": workbook, "sheet": sheet, "note": note}


def _games_frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows)


# --- resolve_hc_game_splits behaviour ------------------------------------------------------


def test_matched_game_resolves_to_camp_iii() -> None:
    games = _games_frame(
        [_game("hc-x", "refill M3-02-04: numeric block, rows 2145-2189, 45 plays")]
    )

    result = resolve_hc_game_splits(games, _splits_frame())

    assert result.height == 1
    row = result.row(0, named=True)
    assert row["split_key"] == "camp-iii"
    assert row["split_match"] == "matched"


def test_contained_game_resolves_to_camp_i() -> None:
    games = _games_frame(
        [_game("hc-y", "refill M3-02-04: numeric block, rows 100-150, 50 plays")]
    )

    result = resolve_hc_game_splits(games, _splits_frame())

    row = result.row(0, named=True)
    assert row["split_key"] == "camp-i"
    assert row["split_match"] == "matched"


def test_boundary_crossing_game_spans_multiple() -> None:
    games = _games_frame(
        [_game("hc-z", "refill M3-02-04: numeric block, rows 950-1050, 100 plays")]
    )

    result = resolve_hc_game_splits(games, _splits_frame())

    row = result.row(0, named=True)
    assert row["split_key"] is None
    assert row["split_match"] == "spans-multiple"


def test_outside_every_window_names_the_case() -> None:
    games = _games_frame(
        [_game("hc-w", "refill M3-02-04: numeric block, rows 9000-9100, 100 plays")]
    )

    result = resolve_hc_game_splits(games, _splits_frame())

    row = result.row(0, named=True)
    assert row["split_key"] is None
    assert row["split_match"] == "outside-known-windows"


def test_note_without_row_range_names_the_case_and_does_not_raise() -> None:
    games = _games_frame(
        [_game("hc-dup", "fingerprint match 87/90 rows (96.7%) against legacy-39")]
    )

    result = resolve_hc_game_splits(games, _splits_frame())  # must not raise

    row = result.row(0, named=True)
    assert row["split_key"] is None
    assert row["split_match"] == "no-row-range"


def test_unknown_workbook_sheet_names_the_case() -> None:
    games = _games_frame(
        [
            _game(
                "hc-other",
                "refill M3-02-04: numeric block, rows 10-20, 10 plays",
                workbook="some-other-workbook",
                sheet="data",
            )
        ]
    )

    result = resolve_hc_game_splits(games, _splits_frame())

    row = result.row(0, named=True)
    assert row["split_key"] is None
    assert row["split_match"] == "no-window-for-source"


def test_output_has_one_row_per_game_in_input_order() -> None:
    games = _games_frame(
        [
            _game("hc-1", "refill M3-02-04: numeric block, rows 100-150, 50 plays"),
            _game("hc-2", "fingerprint match, no row range here"),
            _game("hc-3", "refill M3-02-04: numeric block, rows 9000-9100, 100 plays"),
        ]
    )

    result = resolve_hc_game_splits(games, _splits_frame())

    assert result.height == 3
    assert result["game_id"].to_list() == ["hc-1", "hc-2", "hc-3"]
    assert result["split_match"].to_list() == [
        "matched",
        "no-row-range",
        "outside-known-windows",
    ]


def test_empty_games_frame_returns_schema_correct_empty_frame() -> None:
    empty_games = pl.DataFrame(
        schema={"game_id": pl.Utf8, "workbook": pl.Utf8, "sheet": pl.Utf8, "note": pl.Utf8}
    )

    result = resolve_hc_game_splits(empty_games, _splits_frame())

    assert result.height == 0
    assert "split_match" in result.schema


# --- load_hc_splits against the real committed reference file ------------------------------


def test_load_hc_splits_real_file_has_five_windows_and_flags_conflict() -> None:
    df = load_hc_splits(HC_SPLITS_CSV)

    assert df.height == 5
    assert set(df["split_key"]) == {"camp-i", "mexico", "camp-iii", "camp-iv-vi", "camp-v"}
    conflict_row = df.filter(pl.col("split_key") == "camp-iv-vi").row(0, named=True)
    assert conflict_row["label_status"] == "conflict"
