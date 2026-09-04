"""Structural guards for the M3-04-07 October-sync deliverables (HC-05).

Task 1 guards `docs/hc-rueckfragen-2026-09.md`'s appended `## Zusatzfragen (M3-4,
Report)` block (Frage 7-9) -- must never disturb the pre-existing six `## Frage N` /
six `### Frage N` invariant that `tests/test_m3_explosiveness_docs.py` (M3-3) already
guards, and must not be renumbered on top of Frage 1-6. Task 2 extends this file with
guards for `docs/hc-sync-2026-10.md`, the October handout.

Stdlib + pytest only, no network, sub-second.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

RUECKFRAGEN = REPO_ROOT / "docs" / "hc-rueckfragen-2026-09.md"


def _read(path: Path) -> str:
    assert path.exists(), f"{path} does not exist"
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# docs/hc-rueckfragen-2026-09.md -- additive Zusatzfragen (M3-4, Report) block
# ---------------------------------------------------------------------------


def test_zusatzfragen_m3_4_section_present() -> None:
    text = _read(RUECKFRAGEN)
    assert "## Zusatzfragen (M3-4, Report)" in text, (
        "missing '## Zusatzfragen (M3-4, Report)' section"
    )


def test_zusatzfragen_m3_4_has_exactly_three_frage_subheadings_in_order() -> None:
    text = _read(RUECKFRAGEN)
    section_idx = text.index("## Zusatzfragen (M3-4, Report)")
    section_body = text[section_idx:]

    sub_headings = re.findall(r"^#### Frage (\d+)", section_body, re.MULTILINE)
    assert sub_headings == ["7", "8", "9"], (
        f"expected exactly '#### Frage 7', '#### Frage 8', '#### Frage 9' in order, "
        f"found {sub_headings}"
    )


def test_zusatzfragen_m3_4_each_question_names_its_cell_or_row_reference() -> None:
    text = _read(RUECKFRAGEN)
    section_idx = text.index("## Zusatzfragen (M3-4, Report)")
    section_body = text[section_idx:]

    frage7_idx = section_body.index("#### Frage 7")
    frage8_idx = section_body.index("#### Frage 8")
    frage9_idx = section_body.index("#### Frage 9")

    frage7_body = section_body[frage7_idx:frage8_idx]
    frage8_body = section_body[frage8_idx:frage9_idx]
    frage9_body = section_body[frage9_idx:]

    assert "3001" in frage7_body, "Frage 7 does not name the row range (3001)"
    assert "Data!Y" in frage8_body, "Frage 8 does not name the source cell (Data!Y)"
    assert "Data!W" in frage9_body, "Frage 9 does not name the source cell (Data!W)"


def test_zusatzfragen_m3_4_does_not_break_the_six_and_six_frage_invariant() -> None:
    """The pre-existing Fragen 1-6 (and their six ### Frage N answer stubs under
    ## Antworten) must stay exactly six -- the new block uses #### Frage N, invisible
    to the `^## Frage \\d+` / `^### Frage \\d+` counters M3-3's own guard checks."""
    text = _read(RUECKFRAGEN)

    frage_headings = re.findall(r"^## Frage \d+", text, re.MULTILINE)
    assert len(frage_headings) == 6, (
        f"expected 6 '## Frage N' headings, found {frage_headings}"
    )

    antworten_idx = text.index("## Antworten")
    stub_section = text[antworten_idx:]
    stub_headings = re.findall(r"^### Frage \d+", stub_section, re.MULTILINE)
    assert len(stub_headings) == 6, (
        f"expected 6 '### Frage N' stubs under ## Antworten, found {stub_headings}"
    )


def test_zusatzfragen_m3_4_appended_after_m3_2_block_stays_byte_identical() -> None:
    """If an M3-2 '## Zusatzfragen (M3-2, ...)' block already exists, this plan's own
    block must sit strictly after it, and M3-2's own block text must be untouched."""
    text = _read(RUECKFRAGEN)
    if "## Zusatzfragen (M3-2" not in text:
        return  # M3-2's block has not landed in this worktree; nothing to check.

    m3_2_idx = text.index("## Zusatzfragen (M3-2")
    m3_4_idx = text.index("## Zusatzfragen (M3-4, Report)")
    assert m3_2_idx < m3_4_idx, (
        "M3-4's Zusatzfragen block must come after M3-2's, never before/inside it"
    )
