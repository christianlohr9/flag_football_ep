"""Section- and drift-gates for the two Phase 2.0 reference documents.

These gates guard `docs/material-inventory.md` and `docs/sync-convention.md`
against two independent failure modes: silent loss of a required section (an
edit that drops a heading nobody notices) and drift between what a document
claims about a committed reference CSV and what that CSV's header actually
says (RESEARCH Pitfall 1 — an undocumented column is exactly how a later
consumer misreads the contract). Both documents also carry a DEFERRED-ANALYST
ratification block; this module checks it stays present and owned by the
user, and that neither document leaks a roster player name (T-2.0-02).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs"
INVENTORY_DOC = DOCS_DIR / "material-inventory.md"
SYNC_DOC = DOCS_DIR / "sync-convention.md"
INVENTORY_CSV = REPO_ROOT / "data" / "reference" / "video_inventory.csv"
SYNC_CSV = REPO_ROOT / "data" / "reference" / "video_sync.csv"

INVENTORY_DOC_SECTIONS: tuple[str, ...] = (
    "## Zweck & Abgrenzung",
    "## Domänen & Bestand",
    "## Spaltenmodell von data/reference/video_inventory.csv",
    "## Clip registrieren — Schritt für Schritt",
    "## Wo die Clips liegen",
    "## Was nicht in die CSV kommt",
    "## Ratifizierungs-Block",
)
SYNC_DOC_SECTIONS: tuple[str, ...] = (
    "## Zweck & Abgrenzung",
    "## Referenzseite: was in Hudl schon existiert",
    "## Was von Staff verlangt wird: nichts",
    "## Variante A — Upload nach Hudl (Wunsch, DEFERRED-ANALYST)",
    "## Variante B — manuelle Snap-Anker (immer verfügbar)",
    "## Spaltenmodell von data/reference/video_sync.csv",
    "## Join gegen data/processed/plays.parquet",
    "## Kompatibilität mit automatischer Snap-Erkennung (Phase 2.3)",
    "## Ratifizierungs-Block",
)

_DOC_CSV_PAIRS: tuple[tuple[Path, Path], ...] = (
    (INVENTORY_DOC, INVENTORY_CSV),
    (SYNC_DOC, SYNC_CSV),
)


def test_inventory_doc_has_required_sections() -> None:
    assert INVENTORY_DOC.exists(), f"{INVENTORY_DOC} does not exist"
    lines = {line.strip() for line in INVENTORY_DOC.read_text(encoding="utf-8").splitlines()}
    missing = [section for section in INVENTORY_DOC_SECTIONS if section not in lines]
    assert not missing, f"{INVENTORY_DOC} is missing sections: {missing}"


def test_sync_doc_has_required_sections() -> None:
    assert SYNC_DOC.exists(), f"{SYNC_DOC} does not exist"
    lines = {line.strip() for line in SYNC_DOC.read_text(encoding="utf-8").splitlines()}
    missing = [section for section in SYNC_DOC_SECTIONS if section not in lines]
    assert not missing, f"{SYNC_DOC} is missing sections: {missing}"


@pytest.mark.parametrize("doc_path,csv_path", _DOC_CSV_PAIRS)
def test_docs_document_every_committed_csv_column(doc_path: Path, csv_path: Path) -> None:
    text = doc_path.read_text(encoding="utf-8")
    header = csv_path.read_text(encoding="utf-8").splitlines()[0]
    columns = header.split(",")
    undocumented = [column for column in columns if column not in text]
    assert not undocumented, (
        f"{doc_path} does not mention CSV columns from {csv_path}: {undocumented}"
    )


def test_deferred_analyst_blocks_present_and_owned() -> None:
    for doc_path in (INVENTORY_DOC, SYNC_DOC):
        text = doc_path.read_text(encoding="utf-8")
        assert "DEFERRED-ANALYST" in text, f"{doc_path} is missing a DEFERRED-ANALYST block"
        assert "Owner: Nutzer" in text, f"{doc_path} is missing 'Owner: Nutzer'"


def test_sync_doc_states_join_dtypes() -> None:
    text = SYNC_DOC.read_text(encoding="utf-8")
    for token in ("Utf8", "Int32", "plays.parquet"):
        assert token in text, f"{SYNC_DOC} is missing required token: {token}"
    for variant_marker in ("Variante A", "Variante B"):
        assert variant_marker in text, f"{SYNC_DOC} is missing marker: {variant_marker}"


def test_inventory_doc_documents_registration_commands() -> None:
    text = INVENTORY_DOC.read_text(encoding="utf-8")
    for token in ("ffprobe", "shasum -a 256", "sha256sum", "data/video/*"):
        assert token in text, f"{INVENTORY_DOC} is missing required token: {token}"
    for domain in ("drone", "sideline", "broadcast"):
        assert domain in text, f"{INVENTORY_DOC} is missing domain value: {domain}"


def test_reference_docs_contain_no_roster_names() -> None:
    roster = pl.read_csv(REPO_ROOT / "data" / "reference" / "roster.csv")
    names = [name for name in roster["player_name"].unique().to_list() if name]

    for doc_path in (INVENTORY_DOC, SYNC_DOC):
        text = doc_path.read_text(encoding="utf-8").lower()
        for name in names:
            assert name.lower() not in text, f"{doc_path} contains roster name {name!r}"
