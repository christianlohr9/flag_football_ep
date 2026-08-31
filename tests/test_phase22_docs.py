"""Structural gate over the Phase 2.2 plan 01 documentation reconciliation.

Guards T-2.2-03: keeps the gate-verdict reconciliation (D-01), the capture-protocol
Wunschzettel update (D-03), and the federation-approval documentation (D-06) from
silently drifting apart across `docs/pilot-gate-decision.md`, `docs/capture-protocol.md`,
`docs/capture-legal.md`, `docs/hackathon-challenge-reid.md`,
`docs/hackathon-challenge-prep.md`, `.planning/ROADMAP.md` and `.planning/REQUIREMENTS.md`.
A later edit to any of these six documents cannot silently restore the "only after
passed pilot" claim or drop one of the three Nachtrag sections without this test file
failing loudly.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

GATE_DOC = REPO_ROOT / "docs" / "pilot-gate-decision.md"
CAPTURE_PROTOCOL = REPO_ROOT / "docs" / "capture-protocol.md"
CAPTURE_LEGAL = REPO_ROOT / "docs" / "capture-legal.md"
HACKATHON_REID = REPO_ROOT / "docs" / "hackathon-challenge-reid.md"
HACKATHON_PREP = REPO_ROOT / "docs" / "hackathon-challenge-prep.md"
ROADMAP_MD = REPO_ROOT / ".planning" / "ROADMAP.md"
REQUIREMENTS_MD = REPO_ROOT / ".planning" / "REQUIREMENTS.md"

NACHTRAG_HEADING = "## Nachtrag 2026-08-31"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _section(text: str, heading: str) -> str:
    """Extract the body of a `##`/`###` `Heading` section, stopping at the next
    top-level `## ` heading (line-anchored) -- mirrors the helper in
    tests/test_cv_gate_artifacts.py so both doc-gate tests share one extraction
    convention."""
    start = text.index(heading) + len(heading)
    next_heading = re.search(r"^## ", text[start:], re.MULTILINE)
    end = start + next_heading.start() if next_heading else len(text)
    return text[start:end]


def test_gate_doc_has_nachtrag_2026_08_31() -> None:
    text = _read(GATE_DOC)
    assert NACHTRAG_HEADING in text, (
        "docs/pilot-gate-decision.md is missing the Nachtrag 2026-08-31 section (D-01)"
    )


def test_capture_protocol_has_nachtrag_2026_08_31() -> None:
    text = _read(CAPTURE_PROTOCOL)
    assert NACHTRAG_HEADING in text, (
        "docs/capture-protocol.md is missing the Nachtrag 2026-08-31 section (D-03)"
    )


def test_capture_legal_has_nachtrag_2026_08_31() -> None:
    text = _read(CAPTURE_LEGAL)
    assert NACHTRAG_HEADING in text, (
        "docs/capture-legal.md is missing the Nachtrag 2026-08-31 section (D-06)"
    )


def test_capture_protocol_nachtrag_names_all_three_drone_wishes() -> None:
    text = _read(CAPTURE_PROTOCOL)
    section = _section(text, NACHTRAG_HEADING)
    for term in ("Hover-Winkel", "Endzone", "Kameraschnitt"):
        assert term in section, (
            f"docs/capture-protocol.md Nachtrag section is missing {term!r}"
        )


def test_capture_protocol_nachtrag_adds_no_new_side_camera_request() -> None:
    text = _read(CAPTURE_PROTOCOL)
    section = _section(text, NACHTRAG_HEADING)
    assert "seitenkamera" not in section.lower(), (
        "docs/capture-protocol.md Nachtrag mentions a side camera -- the drone-only wish "
        "list must not add a side-camera request (user decision, CONTEXT.md Deferred Ideas)"
    )


def test_roadmap_no_longer_claims_passed_pilot_precondition() -> None:
    text = _read(ROADMAP_MD)
    assert "only after passed pilot" not in text, (
        ".planning/ROADMAP.md still claims Phase 2.2 needs a passed pilot"
    )
    assert "gate PASSED" not in text, (
        ".planning/ROADMAP.md still claims Phase 2.1's gate PASSED (it is TEILWEISE)"
    )


def test_requirements_no_longer_claims_passed_pilot_precondition() -> None:
    text = _read(REQUIREMENTS_MD)
    assert "Only after passed pilot" not in text, (
        ".planning/REQUIREMENTS.md REQ-S2-03 still claims it needs a passed pilot"
    )


def test_hackathon_reid_datenschutz_records_dated_approval() -> None:
    text = _read(HACKATHON_REID)
    section = _section(text, "### Datenschutz")
    assert "2026-08-31" in section, (
        "docs/hackathon-challenge-reid.md Datenschutz section does not record the "
        "dated federation approval"
    )


def test_hackathon_reid_status_header_no_longer_pending() -> None:
    text = _read(HACKATHON_REID)
    assert "Voraussetzung vor Einreichung: Freigabe" not in text, (
        "docs/hackathon-challenge-reid.md status header still states the approval as "
        "an open precondition"
    )


def test_capture_legal_records_federation_approval_quote() -> None:
    text = _read(CAPTURE_LEGAL)
    assert "2026-08-31" in text, "docs/capture-legal.md is missing the approval date"
    assert "alle Befugnisse" in text, (
        "docs/capture-legal.md is missing the quoted federation approval statement"
    )


def test_hackathon_prep_verbandsfreigabe_checked() -> None:
    text = _read(HACKATHON_PREP)
    lines = [line for line in text.splitlines() if "Verbands-Freigabe" in line]
    assert lines, "docs/hackathon-challenge-prep.md has no Verbands-Freigabe line"
    assert lines[0].startswith("- [x]"), (
        f"Verbands-Freigabe checklist item is not checked off: {lines[0]!r}"
    )
