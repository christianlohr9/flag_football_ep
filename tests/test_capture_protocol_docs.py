"""Mechanical gates for the Phase 2.0 capture protocol wish list and legal note.

These gates keep `docs/capture-protocol.md` complete (all eight drone parameters,
all three tiers per parameter) and tonally on-register (no mandate language, D-03),
and keep `docs/capture-legal.md` on policy level (D-12/D-13: responsibility split,
no operator checklist, no per-person consent tracking). Both documents leave the
repo and go to Staff/the federation, so a roster-name leak check applies to both.
"""

from __future__ import annotations

import csv
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTOCOL_DOC = REPO_ROOT / "docs" / "capture-protocol.md"
LEGAL_DOC = REPO_ROOT / "docs" / "capture-legal.md"

TIERS: tuple[str, ...] = ("Ideal", "Brauchbar", "Unbrauchbar")

DRONE_PARAMETER_HEADINGS: tuple[str, ...] = (
    "### Hover-Position & Winkel",
    "### Flughöhe",
    "### Auflösung",
    "### Bildrate",
    "### Belichtung & Weißabgleich",
    "### Feldabdeckung",
    "### Aufnahmedauer & Akku-Wechsel",
    "### Sync-Signal (reiner Wunsch)",
)

PROTOCOL_SECTIONS: tuple[str, ...] = (
    "## Zweck & Ton — Wunschliste, kein Pflichtenheft",
    "## Wie die Stufen zu lesen sind",
    "## Domäne 1 — Drohne (Primärdomäne)",
    "## Domäne 2 — Erhöhte Seitenkamera (Zweitdomäne)",
    "## Material, das keine Stufe trifft",
    "## Ratifizierungs-Block",
)

LEGAL_SECTIONS: tuple[str, ...] = (
    "## Zweck & Abgrenzung",
    "## EU-Drohnenverordnung — Zuständigkeit liegt beim Betreiber",
    "## DSGVO — Einverständnis liegt beim Verband",
    "## Nicht-Ziele",
)

MANDATE_PHRASES: tuple[str, ...] = (
    "ihr müsst",
    "muss zwingend",
    "ist Pflicht",
    "verpflichtend",
    "Vorgabe",
    "Anforderung an",
)


def _lines(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]


def _roster_names() -> list[str]:
    roster_path = REPO_ROOT / "data" / "reference" / "roster.csv"
    with roster_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        names = {row["player_name"].strip() for row in reader if row.get("player_name")}
    return [name for name in names if name]


def test_protocol_doc_has_required_sections() -> None:
    lines = _lines(PROTOCOL_DOC)
    missing = [heading for heading in PROTOCOL_SECTIONS if heading not in lines]
    assert not missing, f"docs/capture-protocol.md missing sections: {missing}"


def test_protocol_doc_covers_all_eight_drone_parameters() -> None:
    lines = _lines(PROTOCOL_DOC)
    missing = [heading for heading in DRONE_PARAMETER_HEADINGS if heading not in lines]
    assert not missing, f"docs/capture-protocol.md missing drone parameters: {missing}"


def test_protocol_doc_states_all_three_tiers_per_parameter() -> None:
    text = PROTOCOL_DOC.read_text(encoding="utf-8")
    for tier in TIERS:
        count = text.count(tier)
        assert count >= 9, (
            f"docs/capture-protocol.md mentions {tier!r} only {count} times, "
            "expected at least 9 (eight drone parameters + second-domain table)"
        )


def test_protocol_doc_uses_no_mandate_language() -> None:
    text = PROTOCOL_DOC.read_text(encoding="utf-8")
    for line in text.splitlines():
        if "Pflichtenheft" in line:
            continue
        hits = [phrase for phrase in MANDATE_PHRASES if phrase.lower() in line.lower()]
        assert not hits, f"mandate language {hits} found in line: {line.strip()!r}"


def test_protocol_doc_marks_estimated_values_as_assumed() -> None:
    text = PROTOCOL_DOC.read_text(encoding="utf-8")
    assert "[ASSUMED]" in text, "docs/capture-protocol.md is missing an [ASSUMED] tag"
    assert "Richtwert" in text, "docs/capture-protocol.md is missing a 'Richtwert' tag"


def test_protocol_doc_keeps_sync_signal_optional() -> None:
    lines = _lines(PROTOCOL_DOC)
    assert "### Sync-Signal (reiner Wunsch)" in lines
    text = PROTOCOL_DOC.read_text(encoding="utf-8")
    assert "docs/sync-convention.md" in text, (
        "docs/capture-protocol.md does not reference docs/sync-convention.md"
    )


def test_legal_doc_has_required_sections() -> None:
    lines = _lines(LEGAL_DOC)
    missing = [heading for heading in LEGAL_SECTIONS if heading not in lines]
    assert not missing, f"docs/capture-legal.md missing sections: {missing}"


def test_legal_doc_records_responsibility_split() -> None:
    text = LEGAL_DOC.read_text(encoding="utf-8")
    for token in ("Betreiber", "DSGVO", "Verband", "data/video/"):
        assert token in text, f"docs/capture-legal.md is missing required string {token!r}"


def test_legal_doc_stays_policy_level() -> None:
    text = LEGAL_DOC.read_text(encoding="utf-8")
    assert "- [ ]" not in text, (
        "docs/capture-legal.md contains checkbox syntax — the note must stay on "
        "policy level (D-12), not become an operator checklist"
    )
    lines = _lines(LEGAL_DOC)
    assert len(lines) < 60, f"docs/capture-legal.md has {len(lines)} lines, expected < 60"


def test_protocol_and_legal_docs_contain_no_roster_names() -> None:
    names = _roster_names()
    for doc in (PROTOCOL_DOC, LEGAL_DOC):
        text = doc.read_text(encoding="utf-8").lower()
        leaked = sorted(name for name in names if name.lower() in text)
        assert not leaked, f"{doc} contains roster name(s): {leaked}"
