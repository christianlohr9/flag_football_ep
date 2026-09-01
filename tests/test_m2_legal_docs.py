"""Consistency and PII guard over the M2-01 release documents (RECHT-01..03).

Keeps `docs/freigabe-vorlage.md`, `docs/capture-legal.md` and
`docs/hackathon-challenge-reid.md` from drifting apart: the signature date
lives in exactly one machine-readable marker in the one-pager, and every
other document must quote it verbatim. Before the signature exists all
three carry the literal token `SIGNATUR-DATUM-TBD`; after it exists none of
them does. Also guards the one-pager's required structure and a minimal PII
guard (no clip filenames, no private-data paths).

Stdlib + pytest only, no network, no `cv` extra required, runtime under a
second.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
FREIGABE_VORLAGE = REPO_ROOT / "docs" / "freigabe-vorlage.md"
CAPTURE_LEGAL = REPO_ROOT / "docs" / "capture-legal.md"
HACKATHON_REID = REPO_ROOT / "docs" / "hackathon-challenge-reid.md"

TBD_TOKEN = "SIGNATUR-DATUM-TBD"
MARKER_RE = re.compile(r"^<!-- signatur-datum: (.+) -->$", re.MULTILINE)

REQUIRED_HEADINGS = (
    "## Parteien",
    "## Gegenstand der Freigabe",
    "## Zweckbindung",
    "## Löschweg und Löschfrist",
    "## Bestätigung der Löschung",
    "## Geltungsdauer",
    "## Unterschrift",
)


def _read(path: Path) -> str:
    assert path.exists(), f"{path} does not exist"
    return path.read_text(encoding="utf-8")


def _signatur_datum() -> str:
    text = _read(FREIGABE_VORLAGE)
    matches = MARKER_RE.findall(text)
    assert len(matches) == 1, (
        "docs/freigabe-vorlage.md must contain exactly one "
        f"'<!-- signatur-datum: ... -->' marker line, found {len(matches)}"
    )
    return matches[0]


def test_signature_marker_present_exactly_once() -> None:
    _signatur_datum()  # raises with a speaking message if not exactly one


def test_one_pager_has_required_headings_in_order() -> None:
    text = _read(FREIGABE_VORLAGE)
    positions = []
    for heading in REQUIRED_HEADINGS:
        # Line-anchored match only — a heading name can legitimately appear
        # as a cross-reference inside earlier prose (e.g. "siehe `## X`"),
        # which must not be mistaken for the heading itself.
        match = re.search(rf"^{re.escape(heading)}$", text, re.MULTILINE)
        assert match, f"docs/freigabe-vorlage.md is missing heading {heading!r}"
        positions.append(match.start())
    assert positions == sorted(positions), (
        "docs/freigabe-vorlage.md headings are not in the required order: "
        f"{REQUIRED_HEADINGS}"
    )


def test_one_pager_names_all_three_material_classes() -> None:
    text = _read(FREIGABE_VORLAGE)
    section_start = text.index("## Gegenstand der Freigabe")
    next_heading = re.search(r"^## ", text[section_start + 1 :], re.MULTILINE)
    section_end = (
        section_start + 1 + next_heading.start() if next_heading else len(text)
    )
    section = text[section_start:section_end]
    for cls in ("Dev-Set", "Test-Set", "Transfer-Set"):
        assert cls in section, (
            f"docs/freigabe-vorlage.md ## Gegenstand der Freigabe is missing {cls!r}"
        )


def test_one_pager_states_the_deletion_deadline() -> None:
    text = _read(FREIGABE_VORLAGE)
    section_start = text.index("## Löschweg und Löschfrist")
    next_heading = re.search(r"^## ", text[section_start + 1 :], re.MULTILINE)
    section_end = (
        section_start + 1 + next_heading.start() if next_heading else len(text)
    )
    section = text[section_start:section_end]
    assert "2026-12-11" in section, (
        "docs/freigabe-vorlage.md ## Löschweg und Löschfrist is missing the "
        "literal deletion deadline 2026-12-11"
    )


def test_signature_date_consistency_across_documents() -> None:
    signatur_datum = _signatur_datum()
    capture_legal = _read(CAPTURE_LEGAL)
    hackathon_reid = _read(HACKATHON_REID)

    if signatur_datum == TBD_TOKEN:
        assert TBD_TOKEN in capture_legal, (
            f"pre-signature: {TBD_TOKEN} must appear in docs/capture-legal.md"
        )
        assert TBD_TOKEN in hackathon_reid, (
            f"pre-signature: {TBD_TOKEN} must appear in docs/hackathon-challenge-reid.md"
        )
    else:
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", signatur_datum), (
            f"post-signature signatur-datum {signatur_datum!r} must be an ISO date"
        )
        assert signatur_datum in capture_legal, (
            f"post-signature: {signatur_datum} must appear literally in "
            "docs/capture-legal.md"
        )
        assert signatur_datum in hackathon_reid, (
            f"post-signature: {signatur_datum} must appear literally in "
            "docs/hackathon-challenge-reid.md"
        )
        text_one_pager = _read(FREIGABE_VORLAGE)
        for label, text in (
            ("docs/freigabe-vorlage.md", text_one_pager),
            ("docs/capture-legal.md", capture_legal),
            ("docs/hackathon-challenge-reid.md", hackathon_reid),
        ):
            assert TBD_TOKEN not in text, (
                f"post-signature: {TBD_TOKEN} must no longer appear in {label}"
            )


def test_documents_cross_reference_the_one_pager() -> None:
    capture_legal = _read(CAPTURE_LEGAL)
    hackathon_reid = _read(HACKATHON_REID)
    assert "docs/freigabe-vorlage.md" in capture_legal, (
        "docs/capture-legal.md must reference docs/freigabe-vorlage.md"
    )
    assert "docs/freigabe-vorlage.md" in hackathon_reid, (
        "docs/hackathon-challenge-reid.md must reference docs/freigabe-vorlage.md"
    )


def test_no_pii_in_one_pager() -> None:
    text = _read(FREIGABE_VORLAGE)
    assert "data/private/" not in text, "one-pager references a private data path"
    assert ".mp4" not in text, "one-pager references a raw clip filename"
