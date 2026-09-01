"""Coverage gate for `docs/lizenz-inventur.md` (RECHT-04, plan M2-01-02).

Keeps the license inventory from drifting silently out of sync with
`pyproject.toml`: a new distribution added to any dependency group must gain
a matching row in the `## Komponenten` table, or this module fails. Also
locks in the inventory's headline claims (own package is GPL-3.0, no AGPL
anywhere in the delivered chain, the three permissive anchors `ABGLEICH.md`
names explicitly) and a minimal PII guard.

Stdlib + pytest only, no network, no `cv` extra required, runtime under a
second.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / "pyproject.toml"
INVENTORY_DOC = REPO_ROOT / "docs" / "lizenz-inventur.md"

_KNOWN_SOURCES = ("importlib.metadata", "PyPI", "LICENSE-Datei", "GitHub")


def _normalize(name: str) -> str:
    return name.strip().strip("`").lower().replace("_", "-")


def _distribution_name(entry: str) -> str:
    match = re.match(r"^[A-Za-z0-9._-]+", entry)
    assert match, f"could not parse a distribution name out of {entry!r}"
    return match.group(0)


def _parse_komponenten_table() -> dict[str, dict[str, str]]:
    assert INVENTORY_DOC.exists(), f"{INVENTORY_DOC} does not exist"
    lines = INVENTORY_DOC.read_text(encoding="utf-8").splitlines()

    try:
        start = lines.index("## Komponenten")
    except ValueError as exc:
        raise AssertionError("docs/lizenz-inventur.md has no '## Komponenten' section") from exc

    end = len(lines)
    for idx in range(start + 1, len(lines)):
        if lines[idx].startswith("## "):
            end = idx
            break

    rows: dict[str, dict[str, str]] = {}
    header_seen = False
    for line in lines[start:end]:
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if not header_seen:
            # First pipe row is the column header; the next (all-dashes) row
            # is the separator. Skip both.
            if cells and cells[0].lower() == "komponente":
                header_seen = True
            continue
        if all(set(c) <= {"-"} for c in cells if c):
            continue
        # Group-label rows (e.g. "**Eigenes Paket**" with four empty cells)
        # do not carry five populated cells — skip them.
        if len(cells) != 5 or not all(cells):
            continue
        komponente, version, lizenz, rolle, quelle = cells
        key = _normalize(komponente)
        rows[key] = {
            "komponente": komponente,
            "version": version,
            "lizenz": lizenz,
            "rolle": rolle,
            "quelle": quelle,
        }
    return rows


def _declared_distribution_names() -> list[str]:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    names: list[str] = []
    names += [_distribution_name(e) for e in data["project"]["dependencies"]]
    names += [
        _distribution_name(e)
        for e in data["project"]["optional-dependencies"]["cv"]
    ]
    names += [
        _distribution_name(e)
        for e in data["project"]["optional-dependencies"]["versioning"]
    ]
    names += [_distribution_name(e) for e in data["dependency-groups"]["dev"]]
    return names


def test_every_declared_distribution_has_a_row() -> None:
    rows = _parse_komponenten_table()
    declared = _declared_distribution_names()
    missing = [n for n in declared if _normalize(n) not in rows]
    assert not missing, (
        "docs/lizenz-inventur.md ## Komponenten is missing a row for: "
        f"{missing} — every distribution declared in pyproject.toml needs a "
        "license row (RECHT-04)"
    )


def test_own_package_row_present() -> None:
    rows = _parse_komponenten_table()
    assert "flag-football-ep" in rows, (
        "## Komponenten has no row for the delivered repository itself "
        "(flag-football-ep)"
    )
    assert "GPL-3.0" in rows["flag-football-ep"]["lizenz"], (
        "the flag-football-ep row must state GPL-3.0 (the repository's own "
        "LICENSE)"
    )


def test_no_agpl_in_delivered_chain() -> None:
    rows = _parse_komponenten_table()
    offenders = [
        key for key, row in rows.items() if "agpl" in row["lizenz"].lower()
    ]
    assert not offenders, (
        f"AGPL found in the delivered chain: {offenders} — violates the "
        "project's no-AGPL policy (D-02/C-06)"
    )


def test_every_row_has_license_and_source() -> None:
    rows = _parse_komponenten_table()
    assert rows, "## Komponenten parsed to zero rows"
    for key, row in rows.items():
        assert row["lizenz"], f"{key}: empty Lizenz cell"
        assert row["quelle"].startswith(_KNOWN_SOURCES), (
            f"{key}: Quelle {row['quelle']!r} is not one of {_KNOWN_SOURCES}"
        )


def test_known_permissive_anchors() -> None:
    rows = _parse_komponenten_table()
    assert "apache" in rows["rfdetr"]["lizenz"].lower(), (
        "rfdetr must carry an Apache license per ABGLEICH.md"
    )
    assert "apache" in rows["trackers"]["lizenz"].lower(), (
        "trackers must carry an Apache license per ABGLEICH.md"
    )
    assert "mit" in rows["supervision"]["lizenz"].lower(), (
        "supervision must carry the MIT license per ABGLEICH.md"
    )


def test_no_pii_in_inventory() -> None:
    text = INVENTORY_DOC.read_text(encoding="utf-8")
    assert "data/private/" not in text, "inventory references a private data path"
    assert ".mp4" not in text, "inventory references a raw clip filename"
