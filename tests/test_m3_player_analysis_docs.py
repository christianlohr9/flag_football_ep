"""Structural guards for the M3-04-07 October-sync deliverables (HC-05).

Task 1 guards `docs/hc-rueckfragen-2026-09.md`'s appended `## Zusatzfragen (M3-4,
Report)` block (Frage 7-9) -- must never disturb the pre-existing six `## Frage N` /
six `### Frage N` invariant that `tests/test_m3_explosiveness_docs.py` (M3-3) already
guards, and must not be renumbered on top of Frage 1-6.

Task 2 guards `docs/hc-sync-2026-10.md`, the October handout -- required section
headings in order, every relative Markdown link target resolves on disk (with the one
documented exception for a not-yet-landed EPA document), no roster player name or
long surname anywhere in the file, and every standalone rate ("NN,N %") carries a
`k/n` or `n=` denominator nearby (bare column-name mentions like `"Comp %"` are not
rates and are exempt).

Stdlib + pytest only, no network, sub-second.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

RUECKFRAGEN = REPO_ROOT / "docs" / "hc-rueckfragen-2026-09.md"
HANDOUT = REPO_ROOT / "docs" / "hc-sync-2026-10.md"
EPA_DOC = REPO_ROOT / "docs" / "epa-refinement-2026-10.md"
ROSTER = REPO_ROOT / "data" / "reference" / "roster.csv"

_MIN_SURNAME_LEN = 6

# EPA-document pending marker (see this plan's <precondition>) -- if the EPA document
# has not landed yet, the handout must carry this exact marker instead of a dead link.
_EPA_PENDING_MARKER = "EPA-Update steht noch aus (M3-2)"

REQUIRED_HANDOUT_HEADINGS = (
    "## Worum es geht",
    "## Was du bekommst",
    "## Dein Tab, automatisiert",
    "## Was heute noch fehlt",
    "## Offene Fragen",
    "## Quellen",
)


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


# ---------------------------------------------------------------------------
# docs/hc-sync-2026-10.md -- the October handout
# ---------------------------------------------------------------------------


def test_handout_required_headings_present_in_order() -> None:
    text = _read(HANDOUT)
    positions = []
    for heading in REQUIRED_HANDOUT_HEADINGS:
        assert heading in text, f"missing required heading: {heading!r}"
        positions.append(text.index(heading))
    assert positions == sorted(positions), (
        f"required headings are out of order: {REQUIRED_HANDOUT_HEADINGS}"
    )


def test_handout_has_a_stand_line() -> None:
    text = _read(HANDOUT)
    assert "**Stand:**" in text, "handout is missing a '**Stand:**' status line"


_MD_LINK_RE = re.compile(r"\]\(([^)]+)\)")


def test_handout_relative_links_resolve_or_carry_the_epa_pending_marker() -> None:
    text = _read(HANDOUT)
    docs_dir = HANDOUT.parent

    for target in _MD_LINK_RE.findall(text):
        if target.startswith(("http://", "https://")):
            continue
        resolved = (docs_dir / target).resolve()
        if resolved == EPA_DOC.resolve() and not EPA_DOC.exists():
            assert _EPA_PENDING_MARKER in text, (
                "epa-refinement-2026-10.md is linked but absent, and the handout does "
                f"not carry the pending marker {_EPA_PENDING_MARKER!r}"
            )
            continue
        assert resolved.exists(), f"dead relative link target: {target!r} -> {resolved}"


def _load_roster_names() -> tuple[set[str], set[str]]:
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
    return full_names, surnames


def test_handout_has_no_roster_player_name_or_long_surname() -> None:
    full_names, surnames = _load_roster_names()
    text = _read(HANDOUT)
    lower = text.lower()

    for name in full_names:
        assert name.lower() not in lower, f"player name {name!r} found in {HANDOUT}"

    for surname in surnames:
        pattern = re.compile(rf"\b{re.escape(surname)}\b", re.IGNORECASE)
        assert not pattern.search(text), f"surname {surname!r} found in {HANDOUT}"


# Bare column-name mentions ("Comp %", "Adj Comp %", "Explosive %", ...) are not rates
# and carry no denominator by construction -- exempt them the same way
# test_m3_explosiveness_docs.py exempts `"Explosive %"`.
_COLUMN_NAME_PERCENT = re.compile(r"\b(?:Adj )?(?:Comp|Explosive) ?%")


def test_handout_every_standalone_rate_has_a_denominator() -> None:
    text = _read(HANDOUT)
    fraction_before_or_after = re.compile(
        r"\d+[.,]?\d*\s*/\s*\d+[.,]?\d*|\d+[.,]?\d*\s+von\s+\d+[.,]?\d*|n\s*=\s*\d"
    )
    table_row = re.compile(r"^\s*\|")
    numeric_percent = re.compile(r"\d[\d.,]*\s?%")

    for raw_line in text.splitlines():
        line = _COLUMN_NAME_PERCENT.sub("", raw_line)
        if not numeric_percent.search(line):
            continue
        if table_row.match(line):
            assert re.search(r"\d", line), f"table row has '%' but no digits at all: {line!r}"
            continue
        assert fraction_before_or_after.search(line), (
            f"line has a numeric '%' with no k/n or n= denominator nearby: {raw_line!r}"
        )
