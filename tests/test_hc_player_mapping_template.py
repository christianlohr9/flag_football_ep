"""Tests for `scripts/hc_player_mapping_template.py`.

Every fixture uses synthetic labels/roster rows, never the real (gitignored,
PII-carrying) `data/raw/hc_files/` corpus.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import hc_player_mapping_template as tmpl  # noqa: E402


def _roster(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def _women_roster() -> pl.DataFrame:
    return _roster(
        [
            {"team_id": 17, "team_name": "Germany", "player_id": 1, "player_name": "Ada Beispiel", "player_jersey": 7, "position": "QB", "club": ""},
            {"team_id": 17, "team_name": "Germany", "player_id": 2, "player_name": "Bea Vergleich", "player_jersey": 9, "position": "DB", "club": ""},
            {"team_id": 17, "team_name": "Germany", "player_id": 3, "player_name": "Cara Zwoelfmann", "player_jersey": 12, "position": "WR", "club": ""},
            {"team_id": 17, "team_name": "Germany", "player_id": 4, "player_name": "Dana Zwoelfschmidt", "player_jersey": 21, "position": "WR", "club": ""},
        ]
    )


# --- _fold ------------------------------------------------------------------


def test_fold_normalises_uppercase_umlauts() -> None:
    """Regression: the original one-off script replaced umlauts BEFORE
    lowercasing, so an all-caps label with an uppercase umlaut (as the HC
    workbook charts many surnames) never folded to the same key as its
    lowercase roster counterpart."""
    assert tmpl._fold("ZWOELFMANN") == tmpl._fold("Zwoelfmann")
    assert tmpl._fold("MUELLER") == tmpl._fold("Mueller")


def test_fold_strips_eszett_and_non_letters() -> None:
    assert tmpl._fold("Großmann") == "grossmann"
    assert tmpl._fold("O'Brien-Smith 2") == "obriensmith"


# --- build_template_rows -----------------------------------------------------


def test_numeric_label_resolves_uniquely_by_jersey() -> None:
    rows = tmpl.build_template_rows(["7"], _women_roster())
    assert rows == [
        {
            "source": "hc_workbook",
            "source_player": "7",
            "canonical_player": "Ada Beispiel",
            "match_basis": "jersey",
            "candidates": "",
        }
    ]


def test_numeric_label_with_no_jersey_match_is_unresolved() -> None:
    rows = tmpl.build_template_rows(["99"], _women_roster())
    row = rows[0]
    assert row["canonical_player"] == ""
    assert row["candidates"] == ""


def test_surname_label_resolves_uniquely() -> None:
    rows = tmpl.build_template_rows(["Beispiel"], _women_roster())
    assert rows[0]["canonical_player"] == "Ada Beispiel"
    assert rows[0]["match_basis"] == "surname"


def test_surname_label_resolves_uniquely_even_all_caps() -> None:
    """`_fold` folds the whole (space-stripped) name, so `"ZWOELFMANN"` only
    matches the roster row whose folded name actually ends with that suffix
    -- `"Dana Zwoelfschmidt"` does not, so this stays a unique hit."""
    rows = tmpl.build_template_rows(["ZWOELFMANN"], _women_roster())
    row = rows[0]
    assert row["canonical_player"] == "Cara Zwoelfmann"
    assert row["candidates"] == ""


def test_surname_suffix_shared_by_two_players_is_ambiguous() -> None:
    roster = _roster(
        [
            {"team_id": 17, "team_name": "Germany", "player_id": 1, "player_name": "Ada Macharia", "player_jersey": 9, "position": "DB", "club": ""},
            {"team_id": 17, "team_name": "Germany", "player_id": 2, "player_name": "Bea Macharia", "player_jersey": 11, "position": "DB", "club": ""},
        ]
    )
    rows = tmpl.build_template_rows(["MACHARIA"], roster)
    row = rows[0]
    assert row["canonical_player"] == ""
    assert "Ada Macharia" in row["candidates"]
    assert "Bea Macharia" in row["candidates"]


# --- already_mapped_labels / apply_unique ------------------------------------


def test_already_mapped_labels_filters_only_hc_workbook_rows(tmp_path: Path) -> None:
    path = tmp_path / "player_mapping.csv"
    path.write_text(
        "source,source_player,canonical_player\n"
        "hudl,S Foo,Foo Bar\n"
        "hc_workbook,7,Ada Beispiel\n",
        encoding="utf-8",
    )
    assert tmpl.already_mapped_labels(path) == {"7"}


def test_already_mapped_labels_missing_file_returns_empty_set(tmp_path: Path) -> None:
    assert tmpl.already_mapped_labels(tmp_path / "does-not-exist.csv") == set()


def test_apply_unique_appends_only_unambiguous_new_labels(tmp_path: Path) -> None:
    path = tmp_path / "player_mapping.csv"
    path.write_text(
        "source,source_player,canonical_player\n"
        "hc_workbook,7,Someone Else\n",  # pre-existing, must never be overwritten
        encoding="utf-8",
    )
    rows = [
        {"source": "hc_workbook", "source_player": "7", "canonical_player": "Ada Beispiel", "match_basis": "jersey", "candidates": ""},
        {"source": "hc_workbook", "source_player": "9", "canonical_player": "Bea Vergleich", "match_basis": "jersey", "candidates": ""},
        {"source": "hc_workbook", "source_player": "MACHARIA", "canonical_player": "", "match_basis": "", "candidates": "Ada Macharia | Bea Macharia"},
        {"source": "hc_workbook", "source_player": "99", "canonical_player": "", "match_basis": "", "candidates": ""},
    ]

    added = tmpl.apply_unique(rows, path)

    assert added == 1  # only "9" is new, unambiguous and not already present
    written = list(csv.DictReader(path.open(encoding="utf-8")))
    assert written[0] == {"source": "hc_workbook", "source_player": "7", "canonical_player": "Someone Else"}
    assert written[1] == {"source": "hc_workbook", "source_player": "9", "canonical_player": "Bea Vergleich"}
    assert len(written) == 2


def test_apply_unique_returns_zero_when_nothing_to_add(tmp_path: Path) -> None:
    path = tmp_path / "player_mapping.csv"
    path.write_text("source,source_player,canonical_player\n", encoding="utf-8")
    added = tmpl.apply_unique([], path)
    assert added == 0
    assert path.read_text(encoding="utf-8") == "source,source_player,canonical_player\n"


# --- write_template -----------------------------------------------------------


def test_write_template_round_trips_through_csv(tmp_path: Path) -> None:
    out_path = tmp_path / "sub" / "template.csv"
    rows = [
        {"source": "hc_workbook", "source_player": "7", "canonical_player": "Ada Beispiel", "match_basis": "jersey", "candidates": ""}
    ]
    tmpl.write_template(rows, out_path)
    written = list(csv.DictReader(out_path.open(encoding="utf-8")))
    assert written == rows
