"""Doc-versus-CSV agreement guard for the M2-2 baseline documentation (BASE-02/03/04).

Keeps `docs/baseline-messung.md`'s `## Verfahren und Messwerte` table from silently
drifting apart from the measured `data/reference/baseline-methods/summary.csv`, keeps
Deep-EIoU documented as an explicit, reasoned skip rather than a silent omission, keeps
the human 15/61 = 24,59 % reference from being copied onto any other method's row, and
guards both challenge-description documents against the obsolete 77 %-baseline claim
resurfacing as a current value.

Stdlib + pytest only, no network, sub-second.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

BASELINE_MESSUNG = REPO_ROOT / "docs" / "baseline-messung.md"
CHALLENGE_REID = REPO_ROOT / "docs" / "hackathon-challenge-reid.md"
CHALLENGE_FORMULAR = REPO_ROOT / "docs" / "hackathon-challenge-reid-formular.md"
SUMMARY_CSV = REPO_ROOT / "data" / "reference" / "baseline-methods" / "summary.csv"

HISTORY_MARKERS = ("vormalig", "obere Schranke", "ersetzt", "Hochrechnung")
OTHER_METHOD_NAMES = ("ByteTrack", "CBIoU", "GTA", "Deep-EIoU")

# Table column -> CSV method name (the table's own "Verfahren" cell)
METHOD_TO_CSV = {
    "BoT-SORT": "botsort-existing",
    "ByteTrack": "bytetrack",
    "CBIoU": "cbiou",
    "GTA": "gta",
}

RATE_RE = re.compile(r"(\d+)/(\d+)\s*\(([\d,]+)\s*%\)")


def _read(path: Path) -> str:
    assert path.exists(), f"{path} does not exist"
    return path.read_text(encoding="utf-8")


def _section(text: str, heading: str) -> str:
    start = text.index(heading) + len(heading)
    next_heading = re.search(r"^## ", text[start:], re.MULTILINE)
    end = start + next_heading.start() if next_heading else len(text)
    return text[start:end]


def _table_rows(section: str) -> list[list[str]]:
    rows = []
    for line in section.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if all(re.fullmatch(r"-+", c) for c in cells):
            continue  # markdown separator row
        if cells and cells[0] == "Verfahren":
            continue  # header row
        rows.append(cells)
    return rows


def _csv_rows() -> list[dict]:
    with SUMMARY_CSV.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _table_section() -> str:
    text = _read(BASELINE_MESSUNG)
    return _section(text, "## Verfahren und Messwerte")


def _csv_row_for(verfahren: str, konfiguration: str) -> dict:
    method = METHOD_TO_CSV[verfahren]
    csv_rows = _csv_rows()
    if verfahren == "BoT-SORT":
        matches = [r for r in csv_rows if r["method"] == method]
    elif verfahren == "GTA":
        matches = [r for r in csv_rows if r["method"] == method]
    else:
        matches = [
            r
            for r in csv_rows
            if r["method"] == method and r["config"] == konfiguration
        ]
    assert len(matches) == 1, (
        f"expected exactly one summary.csv row for ({verfahren!r}, {konfiguration!r}), "
        f"found {len(matches)}"
    )
    return matches[0]


def test_table_matches_summary_csv() -> None:
    rows = _table_rows(_table_section())
    measured_rows = [r for r in rows if r[0] != "Deep-EIoU"]
    assert measured_rows, "no measured rows found in ## Verfahren und Messwerte"

    for cells in measured_rows:
        verfahren, konfiguration, auto_full, auto_dev = cells[0], cells[1], cells[2], cells[3]
        csv_row = _csv_row_for(verfahren, konfiguration)

        full_match = RATE_RE.search(auto_full)
        assert full_match, f"row {verfahren}/{konfiguration}: no k/n (%) in {auto_full!r}"
        k, n, _ = full_match.groups()
        assert (k, n) == (csv_row["auto_ok_k"], csv_row["auto_ok_n"]), (
            f"row {verfahren}/{konfiguration}: full-61 auto k/n {k}/{n} != "
            f"summary.csv {csv_row['auto_ok_k']}/{csv_row['auto_ok_n']}"
        )

        dev_match = RATE_RE.search(auto_dev)
        assert dev_match, f"row {verfahren}/{konfiguration}: no k/n (%) in {auto_dev!r}"
        dk, dn, _ = dev_match.groups()
        assert (dk, dn) == (csv_row["dev_auto_ok_k"], csv_row["dev_auto_ok_n"]), (
            f"row {verfahren}/{konfiguration}: dev-43 auto k/n {dk}/{dn} != "
            f"summary.csv {csv_row['dev_auto_ok_k']}/{csv_row['dev_auto_ok_n']}"
        )


def test_every_measured_method_has_a_row() -> None:
    rows = _table_rows(_table_section())
    table_keys = set()
    for cells in rows:
        if cells[0] == "Deep-EIoU":
            continue
        method = METHOD_TO_CSV[cells[0]]
        config = "" if cells[0] == "BoT-SORT" else cells[1]
        table_keys.add((method, config))

    for csv_row in _csv_rows():
        key = (csv_row["method"], csv_row["config"])
        assert key in table_keys, (
            f"summary.csv row {key} has no matching row in "
            "docs/baseline-messung.md's ## Verfahren und Messwerte"
        )

    assert any(cells[0] == "Deep-EIoU" for cells in rows), (
        "## Verfahren und Messwerte has no Deep-EIoU row"
    )


def test_deep_eiou_documented_as_skipped() -> None:
    rows = _table_rows(_table_section())
    deep_eiou_rows = [cells for cells in rows if cells[0] == "Deep-EIoU"]
    assert len(deep_eiou_rows) == 1, "expected exactly one Deep-EIoU row"
    row_text = " ".join(deep_eiou_rows[0])
    assert "nicht gemessen" in row_text, "Deep-EIoU row must say 'nicht gemessen'"
    assert "LICENSE" in row_text, "Deep-EIoU row must name the LICENSE-file reason"


def test_no_invented_human_rate() -> None:
    text = _read(BASELINE_MESSUNG)
    for line in text.splitlines():
        if "15/61" in line:
            assert "BoT-SORT" in line or "Referenz" in line, (
                f"line contains 15/61 without BoT-SORT/Referenz context: {line!r}"
            )
        if "24,59" in line:
            for other in OTHER_METHOD_NAMES:
                assert other not in line, (
                    f"line contains 24,59 alongside other method name {other!r}: {line!r}"
                )


def test_start_commands_exist() -> None:
    text = _read(BASELINE_MESSUNG)
    section = _section(text, "## Startbefehle")
    paths = set(re.findall(r"scripts/hackathon/[\w_]+\.py", section))
    assert paths, "## Startbefehle names no scripts/hackathon/*.py path"
    for rel_path in paths:
        assert (REPO_ROOT / rel_path).exists(), f"{rel_path} does not exist on disk"


def test_no_stale_77_percent() -> None:
    for path in (CHALLENGE_REID, CHALLENGE_FORMULAR):
        text = _read(path)
        for line in text.splitlines():
            if "77 %" in line or "77%" in line:
                assert any(marker in line for marker in HISTORY_MARKERS), (
                    f"{path}: line with '77 %'/'77%' has no history marker "
                    f"({HISTORY_MARKERS}): {line!r}"
                )


def test_challenge_doc_links_protocol() -> None:
    text = _read(CHALLENGE_REID)
    assert "baseline-messung.md" in text, (
        "docs/hackathon-challenge-reid.md must reference docs/baseline-messung.md"
    )


def test_every_rate_has_a_denominator() -> None:
    text = _read(BASELINE_MESSUNG)
    kn_before_percent = re.compile(r"\d+/\d+[^%\n]*%")
    allowed_exceptions = ("90 %", "~85 %")
    for line in text.splitlines():
        if "%" not in line:
            continue
        if kn_before_percent.search(line):
            continue
        if any(exc in line for exc in allowed_exceptions):
            continue
        raise AssertionError(f"line has a '%' with no k/n denominator: {line!r}")


def test_no_pii_in_docs() -> None:
    for path in (BASELINE_MESSUNG, CHALLENGE_REID, CHALLENGE_FORMULAR):
        text = _read(path)
        assert "data/private/" not in text, f"{path} references a private data path"
        assert "data/video/" not in text, f"{path} references a raw video path"
        assert ".mp4" not in text, f"{path} references a raw clip filename"
