"""Doc-versus-CSV agreement guard for the M3-3 explosiveness/efficiency proposal (HC-04).

Mirrors `tests/test_m2_baseline_docs.py`'s pattern: keeps every rate quoted in
`docs/explosiveness-vorschlag.md` from silently drifting apart from the measured
`data/reference/explosiveness/*.csv`, keeps the calibrated threshold/quantile honest
against `calibration.json`, and guards `docs/hc-rueckfragen-2026-09.md`'s structure
(six `## Frage` headings, six matching `### Frage N` answer stubs).

Stdlib + pytest only, no network, sub-second.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

VORSCHLAG = REPO_ROOT / "docs" / "explosiveness-vorschlag.md"
RUECKFRAGEN = REPO_ROOT / "docs" / "hc-rueckfragen-2026-09.md"
RECHERCHE = REPO_ROOT / "docs" / "explosiveness-recherche.md"

OVERALL_CSV = REPO_ROOT / "data" / "reference" / "explosiveness" / "comparison_overall.csv"
CLIFF_CSV = REPO_ROOT / "data" / "reference" / "explosiveness" / "cliff_zone.csv"
CALIBRATION_JSON = REPO_ROOT / "data" / "reference" / "explosiveness" / "calibration.json"

REQUIRED_HEADINGS = (
    "## Worum es geht",
    "## Datengrundlage",
    "## Deine heutige Zahl, wörtlich reproduziert",
    "## Der Befund: zwei Fragen in einer Kennzahl",
    "## Die Klippe, in Zahlen",
    "## Vorschlag",
    "## Kleine Stichproben",
    "## Was das im Report bedeutet (Übergabe an M3-4)",
    "## Offene Fragen",
    "## Quellen",
)


def _read(path: Path) -> str:
    assert path.exists(), f"{path} does not exist"
    return path.read_text(encoding="utf-8")


def _german_decimal_percent(text: str) -> float:
    """Parse the first `XX,X %` (or `XX,X%`) German-comma percentage in `text`."""
    match = re.search(r"(\d+,\d+)\s?%", text)
    assert match, f"no German-comma percentage found in {text!r}"
    return float(match.group(1).replace(",", ".")) / 100


def _overall_rows() -> list[dict]:
    with OVERALL_CSV.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _overall_row(definition: str) -> dict:
    rows = [r for r in _overall_rows() if r["definition"] == definition]
    assert len(rows) == 1, f"expected exactly one comparison_overall.csv row for {definition!r}"
    return rows[0]


def _cliff_rows() -> list[dict]:
    with CLIFF_CSV.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _calibration() -> dict:
    return json.loads(CALIBRATION_JSON.read_text(encoding="utf-8"))


def _de_thousands(value: str) -> str:
    """Format an integer string with German thousand-separator dots (`14991` ->
    `14.991`), matching the document's number style."""
    return f"{int(value):,}".replace(",", ".")


def test_all_required_headings_present_in_order() -> None:
    text = _read(VORSCHLAG)
    positions = []
    for heading in REQUIRED_HEADINGS:
        idx = text.find(heading)
        assert idx != -1, f"missing heading: {heading!r}"
        positions.append(idx)
    assert positions == sorted(positions), "required headings are present but out of order"


def test_document_names_recherche_as_source() -> None:
    text = _read(VORSCHLAG)
    assert "explosiveness-recherche.md" in text


def test_baseline_table_matches_comparison_overall_csv() -> None:
    text = _read(VORSCHLAG)
    workbook_row = _overall_row("baseline_hc_workbook")
    verbal_row = _overall_row("baseline_hc_verbal")

    for row in (workbook_row, verbal_row):
        n = _de_thousands(row["n"])
        successes = _de_thousands(row["successes"])
        expected_rate = float(row["rate"])

        fraction_pattern = re.escape(f"{successes}/{n}")
        match = re.search(
            rf"(\d+,\d+)\s?%\s?\({fraction_pattern}\)", text
        )
        assert match, (
            f"no line in {VORSCHLAG} matches '<rate> % ({successes}/{n})' for "
            f"definition {row['definition']!r}"
        )
        actual_rate = float(match.group(1).replace(",", ".")) / 100
        assert abs(actual_rate - round(expected_rate, 3)) < 5e-4, (
            f"{row['definition']}: document rate {actual_rate} != "
            f"comparison_overall.csv rate {expected_rate}"
        )


def test_verbal_only_yards_clause_matches_csv() -> None:
    text = _read(VORSCHLAG)
    row = _overall_row("verbal_only_yards_clause")
    n = _de_thousands(row["n"])
    successes = _de_thousands(row["successes"])

    assert f"{successes} von {n}" in text or f"{successes}/{n}" in text, (
        f"document does not quote the verbal_only_yards_clause finding "
        f"({successes}/{n}) from comparison_overall.csv"
    )


def test_cliff_zone_section_matches_csv() -> None:
    text = _read(VORSCHLAG)
    section_start = text.index("## Die Klippe, in Zahlen")
    section_end = text.index("## Vorschlag")
    section = text[section_start:section_end]

    rows = _cliff_rows()
    assert rows, "cliff_zone.csv has no rows"

    for row in rows:
        yards = row["yards_gained"]
        n = row["n"]
        share = float(row["share"])
        expected_pct = round(share * 100, 1)

        line_match = re.search(
            rf"\|\s*{yards}\s*\|\s*{n}\s*\|\s*(\d+,\d+)\s?%\s*\|", section
        )
        assert line_match, (
            f"no cliff-zone table row found for yards_gained={yards}, n={n} "
            f"in {VORSCHLAG}"
        )
        actual_pct = float(line_match.group(1).replace(",", "."))
        assert abs(actual_pct - expected_pct) < 0.05, (
            f"yards_gained={yards}: document share {actual_pct} != "
            f"cliff_zone.csv share {expected_pct}"
        )


def test_calibrated_threshold_and_quantile_match_json() -> None:
    text = _read(VORSCHLAG)
    cal = _calibration()

    quantile_pct = int(round(cal["epa_quantile"] * 100))
    assert f"{quantile_pct}. Perzentile" in text, (
        f"document does not quote the calibrated quantile ({quantile_pct}. Perzentile)"
    )

    threshold_str = f"{cal['epa_threshold']:.2f}".replace(".", ",")
    assert threshold_str in text, (
        f"document does not quote the calibrated EPA threshold ({threshold_str})"
    )


_METRIC_NAME_LABEL = re.compile(r'"Explosive\s?%"')

# A previously-established rate (already cited with its full k/n denominator earlier in
# the document) may be referenced again by value alone -- a callback, not a new,
# undenominated claim. One entry per line/context that does this, so weakening this list
# is always a visible, deliberate decision (mirrors _ALLOWED_TOKENS in test_m3_hc_pii.py).
# "49,7 %" is the M3-03-03-Nachtrag-recalibrated baseline_hc_verbal rate (was "49,4 %"
# post-M3-04-01-correction, "48,6 %" pre-correction) -- same Comps+Incs+INTs Attempts
# scope, now on the corpus including the head coach's own rows (2026-09-04 recalibration,
# see the "Nachtrag" section under "## Datengrundlage").
_ALLOWED_CALLBACK_PERCENTAGES = ("49,7 %",)


def test_every_percentage_has_a_denominator() -> None:
    text = _read(VORSCHLAG)
    fraction_before_or_after = re.compile(
        r"\d+[.,]?\d*\s*/\s*\d+[.,]?\d*|\d+[.,]?\d*\s+von\s+\d+[.,]?\d*"
    )
    table_row = re.compile(r"^\s*\|")
    for raw_line in text.splitlines():
        line = _METRIC_NAME_LABEL.sub("", raw_line)
        if "%" not in line:
            continue
        if table_row.match(line):
            # Markdown table rows (baseline/cliff-zone tables) carry n in an
            # adjacent column on the same line -- a numeric column is enough.
            assert re.search(r"\d", line), f"table row has '%' but no digits at all: {line!r}"
            continue
        if fraction_before_or_after.search(line):
            continue
        if re.search(r"\(\s*\d[\d.]*\s*Plays?\s*\)", line):
            continue
        if any(callback in line for callback in _ALLOWED_CALLBACK_PERCENTAGES):
            continue
        raise AssertionError(f"line has a '%' with no k/n denominator nearby: {raw_line!r}")


def test_korrektur_section_present_with_both_corrected_denominators() -> None:
    """M3-04-01: the Attempts and Efficiency denominator corrections must be visible and
    dated in the document, not applied silently to the numbers alone."""
    text = _read(VORSCHLAG)
    assert "### Korrektur 2026-09-04 (Nenner)" in text, (
        "missing dated 'Korrektur 2026-09-04 (Nenner)' heading"
    )
    assert "Comps + Incs + INTs" in text, (
        "Korrektur section does not name the corrected Attempts formula (Comps+Incs+INTs)"
    )
    assert "Attempts + Carries" in text, (
        "Korrektur section does not name the corrected Efficiency denominator "
        "(Attempts + Carries)"
    )


def test_rueckfragen_has_six_frage_headings_with_matching_stubs() -> None:
    text = _read(RUECKFRAGEN)
    frage_headings = re.findall(r"^## Frage \d+", text, re.MULTILINE)
    assert len(frage_headings) == 6, f"expected 6 '## Frage N' headings, found {frage_headings}"

    antworten_idx = text.index("## Antworten")
    stub_section = text[antworten_idx:]
    stub_headings = re.findall(r"^### Frage \d+", stub_section, re.MULTILINE)
    assert len(stub_headings) == 6, (
        f"expected 6 '### Frage N' stubs under ## Antworten, found {stub_headings}"
    )


def test_rueckfragen_new_questions_come_after_frage_3_before_no_answer_section() -> None:
    text = _read(RUECKFRAGEN)
    frage3_idx = text.index("## Frage 3")
    frage4_idx = text.index("## Frage 4")
    frage5_idx = text.index("## Frage 5")
    frage6_idx = text.index("## Frage 6")
    no_answer_idx = text.index("## Was wir ohne Antwort liefern")

    assert frage3_idx < frage4_idx < frage5_idx < frage6_idx < no_answer_idx
