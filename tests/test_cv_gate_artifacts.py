"""Structural gate on `docs/pilot-gate-decision.md`, the Milestone-1 Strand-2 closing
artifact (REQ-S2-02, plan 02.1-17).

These tests guard the closing artifact from decaying into an unparseable narrative over
time (T-2.1-43): the required section set, the verdict vocabulary, and the existence of
every evidence file the document cites all fail loudly here rather than silently rotting.

Continuity-evidence note (T-2.1-41): the human continuity review
(`data/reference/continuity_review.csv`) deliberately stopped at 20/61 clips once the >= 90%
target became mathematically unreachable (`02.1-14-SUMMARY.md`) — even the most generous
possible reading (every unreviewed clip counted as a pass) yields 47/61 = 77.0% < 90%.
`summarise_review()` correctly reports `pass_rate: None` for this partial review (D-09,
T-2.1-31: never manufacture a headline rate from a shrinking denominator). This module does
not require a *complete* review (41 rows legitimately carry an empty `verdict`); instead it
recomputes the upper-bound argument the gate document relies on and asserts it independently
proves the criterion missed before the decision was recorded — the actual guard against a
verdict resting on insufficient evidence, not a blind completeness check that would falsify
the honestly-documented stopping point.
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.config import load_config
from flag_football_ep.cv.continuity import summarise_review

REPO_ROOT = Path(__file__).resolve().parent.parent
GATE_DOC = REPO_ROOT / "docs" / "pilot-gate-decision.md"
REQUIREMENTS_MD = REPO_ROOT / ".planning" / "REQUIREMENTS.md"
CONTINUITY_REVIEW_CSV = REPO_ROOT / "data" / "reference" / "continuity_review.csv"

REQUIRED_SECTIONS: tuple[str, ...] = (
    "## Zweck & Abgrenzung",
    "## Ausgangslage",
    "## Gate-Kriterien und Messung",
    "## Extrapolationsformel",
    "## Fehlerzerlegung und Vorbehalte",
    "## Entscheidung",
    "## Konsequenzen",
    "## Demo",
)

VERDICT_VOCABULARY: tuple[str, ...] = ("GO", "NO-GO", "TEILWEISE")

_STATUS_LINE_RE = re.compile(r"^\*\*Status: Entscheidung (GO|NO-GO|TEILWEISE) vom \S+\*\*$")
_CRITERION_ROW_RE = re.compile(r"^\| \d\. .+ \| .+ \| .+ \| .+ \| .+ \|$", re.MULTILINE)
_CITED_FILE_RE = re.compile(r"`((?:docs|data)/[A-Za-z0-9_./-]+\.(?:md|csv|py))`")


def _gate_doc_text() -> str:
    return GATE_DOC.read_text(encoding="utf-8")


def _section(text: str, heading: str) -> str:
    """Extract the body of a `## Heading` section, stopping at the next top-level
    `## ` heading (line-anchored) -- never at a `###`/`####` sub-heading that may appear
    embedded inside a table cell or citation elsewhere in the section body."""
    start = text.index(heading) + len(heading)
    next_heading = re.search(r"^## ", text[start:], re.MULTILINE)
    end = start + next_heading.start() if next_heading else len(text)
    return text[start:end]


def test_gate_doc_exists_and_status_header_is_dated() -> None:
    assert GATE_DOC.exists(), f"{GATE_DOC} does not exist"
    lines = _gate_doc_text().splitlines()
    assert len(lines) >= 3, "gate doc has fewer than 3 lines"
    status_line = lines[2]
    assert status_line.startswith("**Status: Entscheidung "), (
        f"line 3 does not start with '**Status: Entscheidung ': {status_line!r}"
    )
    assert _STATUS_LINE_RE.match(status_line), (
        f"line 3 does not match '**Status: Entscheidung <GO|NO-GO|TEILWEISE> vom <Datum>**': "
        f"{status_line!r}"
    )


def test_gate_doc_no_longer_marked_as_draft() -> None:
    text = _gate_doc_text()
    assert "Entscheidung offen" not in text, (
        "gate doc still contains 'Entscheidung offen' — the draft marker must be replaced "
        "with the recorded verdict"
    )
    assert "AWAITING" not in text, "gate doc still contains an unfilled AWAITING placeholder"


def test_gate_doc_has_all_required_sections() -> None:
    text = _gate_doc_text()
    missing = [section for section in REQUIRED_SECTIONS if section not in text]
    assert not missing, f"gate doc is missing required sections: {missing}"


def test_criteria_table_has_three_rows_with_no_empty_cells() -> None:
    text = _gate_doc_text()
    criteria_section = _section(text, "## Gate-Kriterien und Messung")
    rows = _CRITERION_ROW_RE.findall(criteria_section)
    assert len(rows) == 3, f"expected exactly 3 criterion rows, found {len(rows)}: {rows}"

    for row in rows:
        cells = [cell.strip() for cell in row.strip("|").split("|")]
        assert len(cells) == 5, f"criterion row does not have 5 cells: {row!r}"
        _kriterium, _zielwert, gemessen, _datenbasis, erfuellt = cells
        assert gemessen, f"empty 'Gemessen' cell in row: {row!r}"
        assert erfuellt, f"empty 'Erfuellt?' cell in row: {row!r}"


def test_verdict_is_in_vocabulary() -> None:
    text = _gate_doc_text()
    status_line = text.splitlines()[2]
    match = _STATUS_LINE_RE.match(status_line)
    assert match is not None, f"could not extract verdict from status line: {status_line!r}"
    assert match.group(1) in VERDICT_VOCABULARY

    entscheidung_section = _section(text, "## Entscheidung")
    assert f"Verdikt: {match.group(1)}" in entscheidung_section, (
        "## Entscheidung section does not restate the status-header verdict"
    )


def test_every_cited_data_basis_file_exists() -> None:
    text = _gate_doc_text()
    cited = sorted(set(_CITED_FILE_RE.findall(text)))
    assert cited, "no cited data-basis files found in the gate doc"
    missing = [path for path in cited if not (REPO_ROOT / path).exists()]
    assert not missing, f"gate doc cites files that do not exist in the repo: {missing}"


def test_continuity_review_evidence_supports_the_recorded_verdict() -> None:
    """Guards T-2.1-41: the verdict must never rest on insufficient evidence.

    The recorded verdict is TEILWEISE because continuity criterion 1 is missed even under
    the most generous possible reading of the partial review. This test independently
    recomputes that upper bound from the raw CSV and asserts it actually proves the
    criterion missed -- if a future re-run of this review ever produces different numbers,
    this test must fail loudly rather than silently accept a stale verdict.
    """
    config = load_config(REPO_ROOT / "ffep.toml")
    summary = summarise_review(config.reference.continuity_review)
    assert summary["n_clips"] == 61, f"expected 61 clips, found {summary['n_clips']}"

    if summary["pass_rate"] is not None:
        # A completed review: the ordinary D-09/T-2.1-31 contract applies directly.
        assert summary["pass_rate"] < 0.90 or "GO" in _gate_doc_text().splitlines()[2], (
            "review is complete and pass_rate >= 90% but the gate doc does not record GO"
        )
        return

    # Partial review: recompute the documented upper-bound argument independently of the
    # gate doc's prose and assert it actually decides the criterion (T-2.1-31: counting
    # every unreviewed clip as a pass is the only direction of bias that is safe to apply
    # without reviewing further).
    n_unreviewed = len(summary["unreviewed_clips"])
    upper_bound = (summary["n_pass"] + n_unreviewed) / summary["n_clips"]
    assert upper_bound < 0.90, (
        f"partial review's upper bound ({upper_bound:.3f}) no longer proves the >= 90% "
        "criterion missed -- the review must be continued before a verdict can rest on it"
    )

    text = _gate_doc_text()
    assert "77" in text and "47/61" in text, (
        "gate doc does not state the recomputed upper-bound percentage/fraction"
    )


def test_requirements_md_shows_req_s2_02_complete() -> None:
    text = REQUIREMENTS_MD.read_text(encoding="utf-8")
    assert text.count("REQ-S2-02") >= 2, "REQ-S2-02 should appear in both the list and traceability table"
    assert "- [x] **REQ-S2-02**" in text, "REQ-S2-02 checkbox is not marked complete"

    traceability_rows = [line for line in text.splitlines() if line.startswith("| REQ-S2-02 ")]
    assert traceability_rows, "no REQ-S2-02 row found in the Traceability table"
    assert "Complete (Gate:" in traceability_rows[0], (
        f"REQ-S2-02 traceability row does not read 'Complete (Gate: ...)': {traceability_rows[0]!r}"
    )


def test_continuity_review_csv_is_readable() -> None:
    """Sanity check that the underlying evidence CSV parses cleanly (dialect, schema)."""
    df = pl.read_csv(CONTINUITY_REVIEW_CSV)
    assert df.height == 61
    assert "verdict" in df.columns
