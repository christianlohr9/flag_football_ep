"""PII gate for Phase M3-01 (HC-workbook ingest): no `roster.csv` player name
may appear in any committed artefact this phase touches.

`data/reference/player_mapping.csv` and `data/reference/roster.csv` are
deliberately NOT in `_CHECKED_ARTEFACTS` -- they are the two sanctioned
places a real player name may live (HC-D02: "the maintained CSVs are the
mapping"). Every other artefact below must never carry a name; this test
turns that rule into a failing assertion rather than a promise.

Stdlib + pytest only, no network, no `cv` extra required, runtime well under
a second.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ROSTER = REPO_ROOT / "data" / "reference" / "roster.csv"

_CHECKED_ARTEFACTS: tuple[Path, ...] = (
    REPO_ROOT / "docs" / "hc-workbook-ingest.md",
    REPO_ROOT / "docs" / "hc-rueckfragen-2026-09.md",
    REPO_ROOT / "data" / "reference" / "hc_games.csv",
    REPO_ROOT / "src" / "flag_football_ep" / "ingest" / "hc_workbook.py",
    REPO_ROOT / "src" / "flag_football_ep" / "ingest" / "hc_dedupe.py",
    REPO_ROOT / "tests" / "test_ingest_hc_workbook.py",
    REPO_ROOT / "tests" / "test_ingest_hc_dedupe.py",
    # M3-03-02: explosiveness/efficiency comparison artefacts (HC-04).
    REPO_ROOT / "docs" / "explosiveness-vorschlag.md",
    REPO_ROOT / "docs" / "explosiveness-recherche.md",
    REPO_ROOT / "data" / "reference" / "explosiveness" / "comparison_overall.csv",
    REPO_ROOT / "data" / "reference" / "explosiveness" / "comparison_by_player.csv",
    REPO_ROOT / "data" / "reference" / "explosiveness" / "cliff_zone.csv",
    REPO_ROOT / "scripts" / "explosiveness_comparison.py",
)

_MIN_SURNAME_LEN = 6

# A surname that is also an ordinary German or English word (a coincidental
# false positive on prose, not a real player-name leak) goes here, one entry
# per line, with a one-line justification comment -- so weakening this gate
# is always a visible, deliberate decision, never a silent exception.
_ALLOWED_TOKENS: tuple[str, ...] = ()


def _load_roster_names() -> tuple[set[str], set[str]]:
    """Return (full_names, long_surnames) from `roster.csv`.

    `full_names` is every non-empty `player_name`, verbatim. `long_surnames`
    is the last whitespace-separated token of each full name, kept only when
    it is at least `_MIN_SURNAME_LEN` characters (a short surname like "Ott"
    or "Ali" would false-positive on ordinary prose far too often to be a
    useful gate on its own).
    """
    assert ROSTER.exists(), f"{ROSTER} does not exist"
    with ROSTER.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    full_names = {row["player_name"].strip() for row in rows if row.get("player_name")}
    assert full_names, "roster.csv produced zero player names -- loader broken?"

    surnames: set[str] = set()
    for name in full_names:
        parts = name.split()
        if not parts:
            continue
        surname = parts[-1]
        if len(surname) >= _MIN_SURNAME_LEN:
            surnames.add(surname)

    surnames -= set(_ALLOWED_TOKENS)
    return full_names, surnames


def test_no_roster_player_name_or_long_surname_in_any_committed_artefact() -> None:
    full_names, surnames = _load_roster_names()

    for artefact in _CHECKED_ARTEFACTS:
        if not artefact.exists():
            continue
        text = artefact.read_text(encoding="utf-8")
        lower = text.lower()

        for name in full_names:
            assert name.lower() not in lower, f"player name {name!r} found in {artefact}"

        for surname in surnames:
            pattern = re.compile(rf"\b{re.escape(surname)}\b", re.IGNORECASE)
            assert not pattern.search(text), f"surname {surname!r} found in {artefact}"


def test_no_artefact_pastes_a_label_list_next_to_the_unmapped_dump_reference() -> None:
    """HC-D02: the unmapped-label dump is referenced by path only. A pasted
    Python-list-shaped label dump (`['Name', ...]`) anywhere near the phrase
    `unmapped_players_` would mean someone copied
    `HcIngestNotices.unmapped_players` verbatim into a committed artefact
    instead of leaving it in the gitignored dump file.
    """
    label_list_pattern = re.compile(r"unmapped_players_[^\n]*\[[^\]]*'")
    for artefact in _CHECKED_ARTEFACTS:
        if not artefact.exists():
            continue
        text = artefact.read_text(encoding="utf-8")
        assert not label_list_pattern.search(text), (
            f"{artefact} appears to paste a label list next to an "
            "unmapped_players_ dump reference"
        )
