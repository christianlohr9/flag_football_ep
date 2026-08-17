"""Shared pytest fixtures for the flag_football_ep test suite.

This file is owned by phase 01.2 plan 01. Later plans add their own test
modules and use `flag_football_ep.testing` for canonical-frame factories —
they must not edit this conftest.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

# docs/data-contract.schema.json file_format.filename_pattern:
#   YYYY-MM-DD_{TEAM1}-vs-{TEAM2}_{COMP}.csv
_PRIMARY_FILENAME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}_[A-Z]{3}-vs-[A-Z]{3}_.+\.csv$"
)
# docs/data-contract.schema.json file_format.filename_fallback_pattern:
#   YYYY_{TEAM1}-vs-{TEAM2}[_{COMP}][_n].csv
_FALLBACK_FILENAME_RE = re.compile(
    r"^\d{4}_[A-Z]{3}-vs-[A-Z]{3}(_.+)?\.csv$"
)

CORE_COLUMNS = ["PLAY #", "ODK", "DN", "DIST", "YARD LN", "PLAY TYPE", "RESULT", "GN/LS"]


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """The repository root, resolved from this file's location."""
    return Path(__file__).resolve().parent.parent


@pytest.fixture
def tmp_data_root(tmp_path: Path) -> Path:
    """A tmp_path subtree pre-created with the standard raw/processed/reference layout."""
    for sub in ("raw/hudl", "raw/sportapp", "raw/ifaf", "raw/legacy", "processed", "reference"):
        (tmp_path / sub).mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture(scope="session")
def contract_path(repo_root: Path) -> Path:
    """Path to the data contract schema; skips if the contract file is missing."""
    path = repo_root / "docs" / "data-contract.schema.json"
    if not path.exists():
        pytest.skip("docs/data-contract.schema.json not present")
    return path


@pytest.fixture(scope="session")
def hudl_sample_paths(repo_root: Path) -> list[Path]:
    """Contract-named Hudl exports under data/raw/hudl/, sorted.

    These files carry PII (player names) and are git-ignored, so tests must
    degrade to skip rather than fail when none are present locally.
    """
    hudl_dir = repo_root / "data" / "raw" / "hudl"
    if not hudl_dir.exists():
        pytest.skip("no contract-named Hudl exports present")

    paths = sorted(
        p
        for p in hudl_dir.glob("*.csv")
        if _PRIMARY_FILENAME_RE.match(p.name) or _FALLBACK_FILENAME_RE.match(p.name)
    )
    if not paths:
        pytest.skip("no contract-named Hudl exports present")
    return paths


@pytest.fixture(scope="session")
def legacy_csv_path(repo_root: Path) -> Path:
    """First existing legacy CSV: data/raw/legacy/data_raw.csv, else data_raw.csv."""
    for candidate in (
        repo_root / "data" / "raw" / "legacy" / "data_raw.csv",
        repo_root / "data_raw.csv",
    ):
        if candidate.exists():
            return candidate
    pytest.skip("no legacy data_raw.csv present")


@pytest.fixture
def make_hudl_csv():
    """Factory: write a `;`-delimited, utf-8-sig Hudl-shaped CSV.

    make_hudl_csv(dir, filename, rows, columns=None) -> Path

    `columns` defaults to the eight contract core columns in export order.
    `rows` is a list of dicts; keys missing from a row are written as "".
    """

    def _make(dir: Path, filename: str, rows: list[dict], columns: list[str] | None = None) -> Path:
        cols = columns if columns is not None else CORE_COLUMNS
        dir.mkdir(parents=True, exist_ok=True)
        path = dir / filename
        lines = [";".join(cols)]
        for row in rows:
            lines.append(";".join(str(row.get(c, "")) for c in cols))
        path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
        return path

    return _make
