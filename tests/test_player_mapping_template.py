"""Tests for `scripts/player_mapping_template.py`.

Every fixture uses synthetic labels/roster rows, never the real (gitignored,
PII-carrying) `data/raw/hc_files/` corpus or a real player name.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import player_mapping_template as tmpl  # noqa: E402

from flag_football_ep.testing import canonical_plays_with_scores  # noqa: E402


def _roster(rows: list[dict]) -> pl.DataFrame:
    defaults = {"team_id": 17, "team_name": "Germany", "player_id": 0, "club": "", "season": None}
    return pl.DataFrame([{**defaults, **row} for row in rows])


def _women_roster() -> pl.DataFrame:
    return _roster(
        [
            {"player_id": 1, "player_name": "Ada Beispiel", "player_jersey": 7, "position": "QB"},
            {"player_id": 2, "player_name": "Bea Vergleich", "player_jersey": 9, "position": "DB"},
            {"player_id": 3, "player_name": "Cara Zwoelfmann", "player_jersey": 12, "position": "WR"},
            {"player_id": 4, "player_name": "Dana Zwoelfschmidt", "player_jersey": 21, "position": "WR"},
        ]
    )


# --- _fold --------------------------------------------------------------------------------


def test_fold_normalises_uppercase_umlauts() -> None:
    assert tmpl._fold("ZWOELFMANN") == tmpl._fold("Zwoelfmann")
    assert tmpl._fold("MUELLER") == tmpl._fold("Mueller")


def test_fold_strips_eszett_non_letters_and_whitespace() -> None:
    assert tmpl._fold("Großmann") == "grossmann"
    assert tmpl._fold("O'Brien-Smith 2") == "obriensmith"
    assert tmpl._fold("Erika Musterfrau") == tmpl._fold("Erika  Musterfrau")


# --- match_label: hc_workbook -------------------------------------------------------------


def test_hc_workbook_numeric_label_resolves_uniquely_by_jersey() -> None:
    hits, basis = tmpl.match_label("7", "hc_workbook", _women_roster())
    assert hits == ["Ada Beispiel"]
    assert basis == "jersey"


def test_hc_workbook_numeric_label_with_no_jersey_match_is_unresolved() -> None:
    hits, _basis = tmpl.match_label("99", "hc_workbook", _women_roster())
    assert hits == []


def test_hc_workbook_surname_label_resolves_uniquely() -> None:
    hits, basis = tmpl.match_label("Beispiel", "hc_workbook", _women_roster())
    assert hits == ["Ada Beispiel"]
    assert basis == "surname"


def test_hc_workbook_surname_suffix_shared_by_two_players_is_ambiguous() -> None:
    roster = _roster(
        [
            {"player_id": 1, "player_name": "Ada Musterfrau", "player_jersey": 9, "position": "DB"},
            {"player_id": 2, "player_name": "Bea Musterfrau", "player_jersey": 11, "position": "DB"},
        ]
    )
    hits, _basis = tmpl.match_label("MUSTERFRAU", "hc_workbook", roster)
    assert set(hits) == {"Ada Musterfrau", "Bea Musterfrau"}


def test_hc_workbook_jersey_matches_regardless_of_season() -> None:
    """`hc_workbook` (unlike `legacy`) does not restrict jersey matching to a season --
    a 2026-only row is still a legitimate hc_workbook match."""
    roster = _roster(
        [{"player_id": 1, "player_name": "Erika Beispiel", "player_jersey": 5, "season": "2026"}]
    )
    hits, basis = tmpl.match_label("5", "hc_workbook", roster)
    assert hits == ["Erika Beispiel"]
    assert basis == "jersey"


# --- match_label: legacy -------------------------------------------------------------------


def test_legacy_numeric_label_matches_pre_2026_row_only() -> None:
    roster = _roster(
        [
            {"player_id": 1, "player_name": "Alte Spielerin", "player_jersey": 5, "season": None},
            {"player_id": 2, "player_name": "Neue Spielerin", "player_jersey": 5, "season": "2026"},
        ]
    )
    hits, basis = tmpl.match_label("5", "legacy", roster)
    assert hits == ["Alte Spielerin"]
    assert basis == "jersey"


def test_legacy_numeric_label_ignores_2026_only_jersey() -> None:
    roster = _roster(
        [{"player_id": 1, "player_name": "Neue Spielerin", "player_jersey": 5, "season": "2026"}]
    )
    hits, _basis = tmpl.match_label("5", "legacy", roster)
    assert hits == []


def test_legacy_surname_label_matches_across_seasons() -> None:
    """A legacy-era surname can belong to a player still on the 2026 roster (possibly under
    a longer 2026 name) -- surname matching is not season-restricted."""
    roster = _roster(
        [{"player_id": 1, "player_name": "Erika Beispielfrau", "player_jersey": 21, "season": "2026"}]
    )
    hits, basis = tmpl.match_label("BEISPIELFRAU", "legacy", roster)
    assert hits == ["Erika Beispielfrau"]
    assert basis == "surname"


# --- match_label: ifaf ----------------------------------------------------------------------


def test_ifaf_full_name_matches_regardless_of_team() -> None:
    roster = pl.DataFrame(
        [
            {
                "team_id": 99,
                "team_name": "Otherland",
                "player_id": 1,
                "player_name": "Foreign Playerina",
                "player_jersey": 3,
                "position": "DB",
                "club": "",
                "season": "2026",
            }
        ]
    )
    hits, basis = tmpl.match_label("Foreign Playerina", "ifaf", roster)
    assert hits == ["Foreign Playerina"]
    assert basis == "full_name"


def test_ifaf_full_name_tolerates_spacing_and_case_difference() -> None:
    roster = _roster([{"player_id": 1, "player_name": "Anna Musterexample", "player_jersey": 8}])
    hits, _basis = tmpl.match_label("anna musterexample", "ifaf", roster)
    assert hits == ["Anna Musterexample"]


def test_ifaf_full_name_does_not_match_reordered_tokens() -> None:
    """Folding strips whitespace but never reorders tokens -- `"Musterexample Anna"` (a
    different source's surname-first convention) must not collide with `"Anna
    Musterexample"`."""
    roster = _roster([{"player_id": 1, "player_name": "Musterexample Anna", "player_jersey": 8}])
    hits, _basis = tmpl.match_label("Anna Musterexample", "ifaf", roster)
    assert hits == []


def test_ifaf_full_name_no_surname_fallback() -> None:
    """`ifaf` matches the FULL name only -- a bare surname label must not resolve even if it
    would under `hc_workbook`'s/`legacy`'s surname-suffix rule."""
    roster = _roster([{"player_id": 1, "player_name": "Anna Musterexample", "player_jersey": 8}])
    hits, _basis = tmpl.match_label("Musterexample", "ifaf", roster)
    assert hits == []


def test_match_label_unknown_source_key_raises() -> None:
    import pytest

    with pytest.raises(ValueError):
        tmpl.match_label("x", "hudl", _women_roster())


# --- unmapped_labels_by_source ---------------------------------------------------------------


def _empty_mapping() -> pl.DataFrame:
    return pl.DataFrame(schema={"source": pl.Utf8, "source_player": pl.Utf8, "canonical_player": pl.Utf8})


def test_unmapped_labels_by_source_splits_per_coarse_source() -> None:
    hc_offense = canonical_plays_with_scores(
        n_games=1,
        plays_per_game=2,
        source="hc_workbook:some-file:some-sheet",
        extras={"thrown_by": ["7", None]},
    )
    legacy_offense = canonical_plays_with_scores(
        n_games=1, plays_per_game=2, source="legacy", extras={"thrown_by": ["9", None]}
    )
    offense = pl.concat([hc_offense, legacy_offense], how="vertical")

    result = tmpl.unmapped_labels_by_source(offense, _empty_mapping())

    assert result["hc_workbook"].unmapped == ("7",)
    assert result["legacy"].unmapped == ("9",)


def test_unmapped_labels_by_source_excludes_legacy_sentinels() -> None:
    offense = canonical_plays_with_scores(
        n_games=1,
        plays_per_game=3,
        source="legacy",
        extras={"thrown_by": ["-1", "0", "9"]},
    )
    result = tmpl.unmapped_labels_by_source(offense, _empty_mapping())
    assert result["legacy"].unmapped == ("9",)
    assert set(result["legacy"].sentinels_excluded) == {"-1", "0"}


def test_unmapped_labels_by_source_does_not_strip_sentinels_for_other_sources() -> None:
    """`"0"` is a plausible real jersey number for a source with no sentinel entry -- the
    allowlist must never apply outside the source key(s) it is registered for."""
    offense = canonical_plays_with_scores(
        n_games=1, plays_per_game=2, source="hc_workbook:f:s", extras={"thrown_by": ["0", None]}
    )
    result = tmpl.unmapped_labels_by_source(offense, _empty_mapping())
    assert result["hc_workbook"].unmapped == ("0",)
    assert result["hc_workbook"].sentinels_excluded == ()


def test_unmapped_labels_by_source_already_mapped_label_is_excluded() -> None:
    mapping = pl.DataFrame(
        {"source": ["legacy"], "source_player": ["9"], "canonical_player": ["Someone Example"]}
    )
    offense = canonical_plays_with_scores(
        n_games=1, plays_per_game=2, source="legacy", extras={"thrown_by": ["9", None]}
    )
    result = tmpl.unmapped_labels_by_source(offense, mapping)
    assert result == {}


# --- build_template_rows ----------------------------------------------------------------------


def test_build_template_rows_uses_the_right_source_key_and_basis() -> None:
    labels_by_source = {
        "legacy": tmpl.SourceLabels(unmapped=("7",), sentinels_excluded=("-1", "0")),
    }
    rows = tmpl.build_template_rows(labels_by_source, _women_roster())
    assert rows == [
        {
            "source": "legacy",
            "source_player": "7",
            "canonical_player": "Ada Beispiel",
            "match_basis": "jersey",
            "candidates": "",
        }
    ]


# --- apply_unique --------------------------------------------------------------------------


def test_apply_unique_appends_only_unambiguous_new_labels_per_source(tmp_path: Path) -> None:
    path = tmp_path / "player_mapping.csv"
    path.write_text(
        "source,source_player,canonical_player\n"
        "hc_workbook,7,Someone Else\n",  # pre-existing, must never be overwritten
        encoding="utf-8",
    )
    rows = [
        {"source": "hc_workbook", "source_player": "7", "canonical_player": "Ada Beispiel", "match_basis": "jersey", "candidates": ""},
        {"source": "legacy", "source_player": "9", "canonical_player": "Bea Vergleich", "match_basis": "jersey", "candidates": ""},
        {"source": "ifaf", "source_player": "MEHRDEUTIG", "canonical_player": "", "match_basis": "", "candidates": "A | B"},
        {"source": "legacy", "source_player": "99", "canonical_player": "", "match_basis": "", "candidates": ""},
    ]

    added = tmpl.apply_unique(rows, path)

    assert added == 1  # only legacy/"9" is new, unambiguous and not already present
    written = list(csv.DictReader(path.open(encoding="utf-8")))
    assert written[0] == {"source": "hc_workbook", "source_player": "7", "canonical_player": "Someone Else"}
    assert written[1] == {"source": "legacy", "source_player": "9", "canonical_player": "Bea Vergleich"}
    assert len(written) == 2


def test_apply_unique_returns_zero_when_nothing_to_add(tmp_path: Path) -> None:
    path = tmp_path / "player_mapping.csv"
    path.write_text("source,source_player,canonical_player\n", encoding="utf-8")
    added = tmpl.apply_unique([], path)
    assert added == 0
    assert path.read_text(encoding="utf-8") == "source,source_player,canonical_player\n"


def test_apply_unique_then_recompute_is_idempotent(tmp_path: Path) -> None:
    """Applying once resolves the label; recomputing `unmapped_labels_by_source` against the
    now-updated mapping must never see it again (never a duplicate row on a second apply)."""
    path = tmp_path / "player_mapping.csv"
    path.write_text("source,source_player,canonical_player\n", encoding="utf-8")

    rows = [
        {"source": "legacy", "source_player": "9", "canonical_player": "Bea Vergleich", "match_basis": "jersey", "candidates": ""},
    ]
    added_first = tmpl.apply_unique(rows, path)
    assert added_first == 1

    added_second = tmpl.apply_unique(rows, path)
    assert added_second == 0

    offense = canonical_plays_with_scores(
        n_games=1, plays_per_game=2, source="legacy", extras={"thrown_by": ["9", None]}
    )
    from flag_football_ep.reference import load_player_mapping

    mapping = load_player_mapping(path)
    result = tmpl.unmapped_labels_by_source(offense, mapping)
    assert result == {}


# --- write_template -----------------------------------------------------------------------


def test_write_template_round_trips_through_csv(tmp_path: Path) -> None:
    out_path = tmp_path / "sub" / "template.csv"
    rows = [
        {"source": "legacy", "source_player": "7", "canonical_player": "Ada Beispiel", "match_basis": "jersey", "candidates": ""}
    ]
    tmpl.write_template(rows, out_path)
    written = list(csv.DictReader(out_path.open(encoding="utf-8")))
    assert written == rows
