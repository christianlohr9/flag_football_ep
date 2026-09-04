"""Doc-versus-CSV agreement guard for the M3-02 German EPA deliverable
(`docs/epa-refinement-2026-10.md`).

This guard exists so that a number in the coach-facing document can never
drift away from the measured CSV it claims to report -- if a figure in
`docs/epa-refinement-2026-10.md` is wrong, the fix is a re-measurement
(re-run the relevant `scripts/*.py`), never a prose edit. Every MLflow run
id and every log-loss figure quoted in a structured table in the document
is checked against `data/reference/epa_refinement/*.csv` in both
directions: the document may not cite a run the CSV does not have, and the
CSV may not have a run the document silently omits.

Only the document's STRUCTURED tables (the ones with an explicit "Run-ID"
or per-source "Verbesserung" header) are figure-checked -- free prose
(e.g. the Phase-1.3 historical comparison numbers, which are legitimately
NOT in this phase's `data/reference/epa_refinement/*.csv`) is out of
scope for the figure guard, matching this plan's own instruction not to
assert on wording/tone/ordering beyond what Task 1's own verify already
gates. `tests/test_m3_epa_snapshot.py` (M3-02-03) already guards
`data/reference/hc_sp_tables/*.csv` itself; this file does not duplicate
those assertions.

Stdlib + pytest only, no network, sub-second.
"""

from __future__ import annotations

import csv
import re
from datetime import date
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

DOC = REPO_ROOT / "docs" / "epa-refinement-2026-10.md"
RUECKFRAGEN = REPO_ROOT / "docs" / "hc-rueckfragen-2026-09.md"
ROSTER = REPO_ROOT / "data" / "reference" / "roster.csv"
EPA_DIR = REPO_ROOT / "data" / "reference" / "epa_refinement"
ABLATION_CSV = EPA_DIR / "ablation_summary.csv"
PER_SOURCE_EP_CSV = EPA_DIR / "per_source_metrics_ep.csv"
PER_SOURCE_WP_CSV = EPA_DIR / "per_source_metrics_wp.csv"

_RUN_ID_RE = re.compile(r"\b[0-9a-f]{32}\b")
_MIN_SURNAME_LEN = 6


def _read(path: Path) -> str:
    if not path.exists():
        pytest.skip(f"{path} does not exist")
    return path.read_text(encoding="utf-8")


def _csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        pytest.skip(f"{path} does not exist -- run the M3-02 measurement scripts first")
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _run_ids_in_text(text: str) -> set[str]:
    return set(_RUN_ID_RE.findall(text))


def _find_table(text: str, header_marker: str) -> list[list[str]]:
    """Return every row (header first) of the first Markdown table whose
    header line contains `header_marker`, as a list of stripped cells."""
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("|") and header_marker in stripped:
            start = i
            break
    assert start is not None, f"no table found with header marker {header_marker!r}"

    rows: list[list[str]] = []
    for line in lines[start:]:
        stripped = line.strip()
        if not stripped.startswith("|"):
            break
        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if all(re.fullmatch(r":?-+:?", c) for c in cells):
            continue  # markdown header-separator row (incl. alignment colons)
        rows.append(cells)
    return rows


def _to_float(cell: str) -> float:
    """Parse a written figure, tolerant of German decimal comma AND point.
    Strips surrounding backticks/whitespace first."""
    cleaned = cell.strip().strip("`")
    return float(cleaned.replace(",", "."))


def _written_decimals(cell: str) -> int:
    cleaned = cell.strip().strip("`")
    sep = "," if "," in cleaned else ("." if "." in cleaned else None)
    if sep is None:
        return 0
    return len(cleaned.split(sep)[-1])


def _assert_figure_matches(written: str, measured: float, label: str) -> None:
    """Tolerance is derived from the precision the document actually wrote,
    not a fixed epsilon -- a figure written as `1,03` (2 decimals) must
    match a measured `1.027657` (tolerance 0.005), while a figure written
    at full precision (`0,957593`) must match to within half a
    millionth."""
    written_value = _to_float(written)
    decimals = _written_decimals(written)
    tolerance = 0.5 * (10 ** (-decimals)) if decimals else 0.5
    assert abs(written_value - measured) <= tolerance, (
        f"{label}: document says {written!r} ({written_value}), measured CSV value is "
        f"{measured} -- outside tolerance {tolerance} for {decimals} written decimals"
    )


def _load_roster_names() -> tuple[set[str], set[str]]:
    """`(full_names, long_surnames)` from `roster.csv`, same shape/skip
    behaviour as `tests/test_m3_epa_snapshot.py::_load_roster_names`."""
    if not ROSTER.exists():
        pytest.skip(f"{ROSTER} does not exist -- cannot run the PII gate")
    with ROSTER.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    full_names = {row["player_name"].strip() for row in rows if row.get("player_name")}
    if not full_names:
        pytest.skip("roster.csv produced zero player names -- loader broken?")

    surnames: set[str] = set()
    for name in full_names:
        parts = name.split()
        if not parts:
            continue
        surname = parts[-1]
        if len(surname) >= _MIN_SURNAME_LEN:
            surnames.add(surname)
    return full_names, surnames


# ---------------------------------------------------------------------------
# Run-id agreement (bidirectional)
# ---------------------------------------------------------------------------


def test_run_ids_match_ablation_summary_bidirectionally() -> None:
    doc_text = _read(DOC)
    ablation_rows = _csv_rows(ABLATION_CSV)

    doc_run_ids = _run_ids_in_text(doc_text)
    csv_run_ids = {row["run_id"] for row in ablation_rows}

    missing_from_doc = csv_run_ids - doc_run_ids
    extra_in_doc = doc_run_ids - csv_run_ids

    assert not missing_from_doc, (
        f"ablation_summary.csv has run id(s) never quoted in {DOC.name}: {missing_from_doc}"
    )
    assert not extra_in_doc, (
        f"{DOC.name} quotes run id(s) not present in ablation_summary.csv: {extra_in_doc}"
    )


# ---------------------------------------------------------------------------
# Log-loss figure agreement (structured tables only)
# ---------------------------------------------------------------------------


def test_ablation_table_figures_match_ablation_summary_csv() -> None:
    doc_text = _read(DOC)
    ablation_rows = _csv_rows(ABLATION_CSV)
    by_run_id = {row["run_id"]: row for row in ablation_rows}

    rows = _find_table(doc_text, "Naive Grundrate")
    header, data_rows = rows[0], rows[1:]
    assert len(header) == 8, f"unexpected ablation table header shape: {header}"

    checked = 0
    for cells in data_rows:
        _, _, _, _, metric_cell, naive_cell, impr_cell, run_id_cell = cells
        run_id = run_id_cell.strip("`")
        assert run_id in by_run_id, f"run id {run_id!r} in doc table has no ablation_summary.csv row"
        csv_row = by_run_id[run_id]

        _assert_figure_matches(metric_cell, float(csv_row["metric_value"]), f"{run_id} metric_value")
        _assert_figure_matches(naive_cell, float(csv_row["naive_value"]), f"{run_id} naive_value")
        _assert_figure_matches(
            impr_cell, float(csv_row["logloss_improvement"]), f"{run_id} logloss_improvement"
        )
        checked += 1

    assert checked == len(ablation_rows), (
        f"expected {len(ablation_rows)} ablation rows checked, got {checked}"
    )


def test_per_source_table_figures_match_per_source_csvs() -> None:
    doc_text = _read(DOC)
    ep_rows = {
        row["source"]: row for row in _csv_rows(PER_SOURCE_EP_CSV) if row["arm"] == "with_hc"
    }
    wp_rows = {
        row["source"]: row for row in _csv_rows(PER_SOURCE_WP_CSV) if row["arm"] == "with_hc"
    }

    rows = _find_table(doc_text, "EP Verbesserung")
    header, data_rows = rows[0], rows[1:]
    assert len(header) == 9, f"unexpected per-source table header shape: {header}"

    assert data_rows, "per-source comparison table has no data rows"
    for cells in data_rows:
        source_cell, _, ep_ll, ep_naive, ep_impr, _, wp_ll, wp_naive, wp_impr = cells
        source = source_cell.strip("`")

        assert source in ep_rows, f"source {source!r} in doc table has no per_source_metrics_ep.csv row"
        assert source in wp_rows, f"source {source!r} in doc table has no per_source_metrics_wp.csv row"

        ep_row, wp_row = ep_rows[source], wp_rows[source]
        _assert_figure_matches(ep_ll, float(ep_row["logloss"]), f"{source} EP logloss")
        _assert_figure_matches(ep_naive, float(ep_row["naive_logloss"]), f"{source} EP naive")
        _assert_figure_matches(ep_impr, float(ep_row["improvement"]), f"{source} EP improvement")
        _assert_figure_matches(wp_ll, float(wp_row["logloss"]), f"{source} WP logloss")
        _assert_figure_matches(wp_naive, float(wp_row["naive_logloss"]), f"{source} WP naive")
        _assert_figure_matches(wp_impr, float(wp_row["improvement"]), f"{source} WP improvement")


# ---------------------------------------------------------------------------
# Every measured CSV under data/reference/epa_refinement/ is referenced
# ---------------------------------------------------------------------------


def test_document_references_every_epa_refinement_csv() -> None:
    doc_text = _read(DOC)
    if not EPA_DIR.exists():
        pytest.skip(f"{EPA_DIR} does not exist")
    csv_files = sorted(EPA_DIR.glob("*.csv"))
    assert csv_files, f"{EPA_DIR} has no committed CSVs"

    missing = [p.name for p in csv_files if p.name not in doc_text]
    assert not missing, f"{DOC.name} never names these committed CSVs: {missing}"


# ---------------------------------------------------------------------------
# PII gate
# ---------------------------------------------------------------------------


def test_no_roster_player_name_in_committed_docs() -> None:
    full_names, surnames = _load_roster_names()

    for path in (DOC, RUECKFRAGEN):
        text = _read(path)
        lower = text.lower()
        for name in full_names:
            assert name.lower() not in lower, f"player name {name!r} found in {path}"
        for surname in surnames:
            pattern = re.compile(rf"\b{re.escape(surname)}\b", re.IGNORECASE)
            assert not pattern.search(text), f"surname {surname!r} found in {path}"


# ---------------------------------------------------------------------------
# docs/hc-rueckfragen-2026-09.md structural invariant (M3-3 compatibility)
# ---------------------------------------------------------------------------


def test_rueckfragen_frage_headings_stay_balanced() -> None:
    text = _read(RUECKFRAGEN)
    h2_frage = len(re.findall(r"^## Frage", text, re.MULTILINE))
    h3_frage = len(re.findall(r"^### Frage", text, re.MULTILINE))
    assert h2_frage == h3_frage, (
        f"unbalanced Frage headings: {h2_frage} '## Frage' vs {h3_frage} '### Frage' "
        "-- this must never change as a side effect of the Zusatzfragen addition"
    )


def test_rueckfragen_zusatzfrage_stub_count_matches_section() -> None:
    text = _read(RUECKFRAGEN)

    section_headings = list(re.finditer(r"^## Zusatzfragen.*$", text, re.MULTILINE))
    assert len(section_headings) == 1, (
        f"expected exactly one '## Zusatzfragen' section, found {len(section_headings)}"
    )
    heading = section_headings[0]
    body_start = heading.end()
    next_h2 = re.search(r"^## ", text[body_start:], re.MULTILINE)
    body_end = body_start + next_h2.start() if next_h2 else len(text)
    section_body = text[body_start:body_end]

    sub_questions_in_section = set(re.findall(r"^### Zusatzfrage (\S+)", section_body, re.MULTILINE))
    assert sub_questions_in_section, "## Zusatzfragen section has no ### Zusatzfrage sub-questions"

    total_zusatzfrage_headings = len(re.findall(r"^### Zusatzfrage", text, re.MULTILINE))
    assert total_zusatzfrage_headings == 2 * len(sub_questions_in_section), (
        f"expected {2 * len(sub_questions_in_section)} total '### Zusatzfrage' headings "
        f"(one in the question section, one stub under ## Antworten, per sub-question), "
        f"found {total_zusatzfrage_headings}"
    )


# ---------------------------------------------------------------------------
# Stand: line
# ---------------------------------------------------------------------------


def test_stand_line_is_a_valid_date() -> None:
    text = _read(DOC)
    match = re.search(r"Stand:\s*(\d{4}-\d{2}-\d{2})", text)
    assert match, f"{DOC.name} has no 'Stand: YYYY-MM-DD' line"
    date.fromisoformat(match.group(1))  # raises ValueError if not a real date
