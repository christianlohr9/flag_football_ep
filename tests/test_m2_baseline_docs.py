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
PER_CLIP_CSV = REPO_ROOT / "data" / "reference" / "baseline-methods" / "per_clip.csv"

CONTINUOUS_SECTION_HEADING = "## Stetige Kennzahl neben der Schwelle (M2-4, METR-01/METR-04)"

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


def test_challenge_reid_states_metr03_framing() -> None:
    text = _read(CHALLENGE_REID)
    section = _section(text, "### Benchmark-Design")
    assert "Abnahmekriterium" in section, (
        "### Benchmark-Design must name the acceptance criterion (Abnahmekriterium)"
    )
    assert "Zielrichtung" in section, (
        "### Benchmark-Design must name the direction metric (Zielrichtung)"
    )
    assert "90 %" in section, (
        "### Benchmark-Design must still state the 90 % acceptance target"
    )
    assert "Design offen" not in section, (
        "### Benchmark-Design must no longer defer METR-03 wording as 'Design offen'"
    )


def test_challenge_reid_teil3_scoring_bullet_resolved() -> None:
    text = _read(CHALLENGE_REID)
    section = _section(text, "## Teil 3")
    assert "~~Scoring-Skript" in section, (
        "## Teil 3's dangling scoring bullet must be struck through, matching the "
        "project's resolved-bullet convention"
    )
    assert "geklärt" in section, (
        "## Teil 3's resolved scoring bullet must say 'geklärt' like the existing "
        "resolved trackers-version bullet"
    )


def test_challenge_formular_names_direction_metric() -> None:
    text = _read(CHALLENGE_FORMULAR)
    section = text.split("## Beschreibung")[1].split("\n## ")[0]
    assert "Zielrichtung" in section, (
        "docs/hackathon-challenge-reid-formular.md ## Beschreibung must name the "
        "direction metric (Zielrichtung)"
    )


def test_challenge_formular_beschreibung_word_count() -> None:
    text = _read(CHALLENGE_FORMULAR)
    section = text.split("## Beschreibung")[1].split("\n## ")[0]
    word_count = len(section.split())
    assert 150 <= word_count <= 300, (
        f"## Beschreibung word count {word_count} outside the 150-300 budget"
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


def _continuous_section() -> str:
    text = _read(BASELINE_MESSUNG)
    return _section(text, CONTINUOUS_SECTION_HEADING)


def _per_clip_rows() -> list[dict]:
    with PER_CLIP_CSV.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _german_decimal(cell: str) -> float:
    match = re.search(r"(\d+,\d+)", cell)
    assert match, f"no German-comma decimal found in {cell!r}"
    return float(match.group(1).replace(",", "."))


def test_continuous_section_has_one_row_per_measured_method() -> None:
    rows = _table_rows(_continuous_section())
    assert rows, "no rows found in the METR-04 comparison table"
    csv_keys = {(r["method"], r["config"]) for r in _csv_rows()}
    assert len(rows) == len(csv_keys), (
        f"expected {len(csv_keys)} rows in the METR-04 table (one per summary.csv "
        f"method/config), found {len(rows)}"
    )


def test_continuous_values_match_per_clip_csv() -> None:
    rows = _table_rows(_continuous_section())
    per_clip = _per_clip_rows()
    assert rows, "no rows found in the METR-04 comparison table"
    for cells in rows:
        verfahren, konfiguration = cells[0], cells[1]
        csv_row = _csv_row_for(verfahren, konfiguration)
        method, config = csv_row["method"], csv_row["config"]
        fragments = [
            float(r["n_fragments"])
            for r in per_clip
            if r["method"] == method and r["config"] == config
        ]
        assert fragments, f"no per_clip.csv rows for ({method!r}, {config!r})"
        expected = round(sum(fragments) / len(fragments) / 10, 4)
        actual = _german_decimal(cells[4])
        assert abs(actual - expected) < 1e-9, (
            f"row {verfahren}/{konfiguration}: continuous value {actual} != "
            f"recomputed {expected} from per_clip.csv"
        )


def test_continuous_human_column_matches_summary_csv() -> None:
    rows = _table_rows(_continuous_section())
    assert rows, "no rows found in the METR-04 comparison table"
    for cells in rows:
        verfahren, konfiguration, human_cell = cells[0], cells[1], cells[2]
        csv_row = _csv_row_for(verfahren, konfiguration)
        if csv_row["human_pass_k"] == "":
            assert human_cell == "keine Review", (
                f"row {verfahren}/{konfiguration}: expected 'keine Review' "
                f"(summary.csv human_pass_k is empty), got {human_cell!r}"
            )
        else:
            assert "15/61" in human_cell, (
                f"row {verfahren}/{konfiguration}: expected the 15/61 BoT-SORT "
                f"reference rate, got {human_cell!r}"
            )


def test_continuous_section_documents_guard_gta_caveat_and_blind_spot() -> None:
    section = _continuous_section()
    assert "diagnostisch" in section, (
        "METR-04 section must name the guard metric as diagnostic"
    )
    assert "364" in section, "METR-04 section is missing the GTA merge-count caveat"
    caveat_vocabulary = ("Merge-Operation", "zusammengef", "Over-Merge", "over-merge")
    assert any(word in section for word in caveat_vocabulary), (
        "METR-04 section is missing GTA merge-caveat vocabulary"
    )
    assert "39" in section, "METR-04 section is missing the blind-spot fail count"
    assert "Identitätswechsel" in section or "Identitaetswechsel" in section, (
        "METR-04 section is missing the blind-spot identity-switch statement"
    )


def test_no_pii_in_docs() -> None:
    for path in (BASELINE_MESSUNG, CHALLENGE_REID, CHALLENGE_FORMULAR):
        text = _read(path)
        assert "data/private/" not in text, f"{path} references a private data path"
        assert "data/video/" not in text, f"{path} references a raw video path"
        assert ".mp4" not in text, f"{path} references a raw clip filename"
