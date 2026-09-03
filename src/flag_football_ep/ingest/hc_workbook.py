"""Head-coach workbook ingest: cell-level `.xlsx` reading, dtype-based block
segmentation, and per-block contract mapping for the three hand-maintained
Excel workbooks under `data/raw/hc_files/` (gitignored, PII -- player names).

Why cell-level reading instead of `pl.read_excel`: one sheet is not one
table. `Scoring Probability by Situation 2023-2026.xlsx`'s `Data` and
`Copy of Data` tabs carry two incompatible row layouts under a single header
row -- roughly one row in six holds a team-name pair where the header claims
`PLAY #`/`ODK`, the rest hold a real numeric `PLAY #`. `Germany Analytics
Stats EC 2025 vs WC Nations.xlsx`'s `Data` tab carries a correct header over
thousands of completely empty rows. A bulk rectangular reader (calamine/
`fastexcel`) assumes one consistent layout per sheet and cannot represent
either finding; both must come out of this module as named, counted
findings instead of a silently corrupted frame.

Order of operations this module implements: `read_sheet_rows` (openpyxl,
`data_only=True` formula resolution + `read_only=True` streaming) ->
`segment_blocks` (classify every row by its first cell's dtype, before any
column mapping is trusted -- M3-01-RESEARCH.md Pattern 1) ->
`map_block_to_frame` (per block: normalize the header, cast every cell to
Utf8, null out the pair block's unresolved tail, materialize absent
contract core columns, `validate_header`/`check_column_domains`, rename
charting columns onto their canonical extras -- Pattern 2). Nothing in this
module raises on a data-quality finding; `SheetNotFoundError` is the only
exception it defines, for a structurally absent sheet. Everything else is
folded into the caller's `HcIngestNotices.messages`.

This module reads, it does not derive: RESULT-token parsing, drive/scoring
derivation and the final `canonical.conform_to_canonical` convergence are
plan M3-01-03/04's job, reusing `ingest.hudl`'s existing functions rather
than forking them (HC-D01).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import openpyxl
import polars as pl

from flag_football_ep.canonical import ConformReport
from flag_football_ep.ingest import hudl
from flag_football_ep.validation.schema import (
    Contract,
    DomainViolation,
    HeaderReport,
    check_column_domains,
    validate_header,
)

__all__ = [
    "SHEET_NAMES",
    "PAIR_BLOCK_TAIL_ANCHOR",
    "SheetNotFoundError",
    "slugify",
    "hc_source_label",
    "HcBlock",
    "HcIngestNotices",
    "read_sheet_rows",
    "segment_blocks",
    "map_block_to_frame",
]

# The two tab names every HC workbook charts plays under. `Copy of Data`
# (Scoring Probability workbook) is very likely an Excel-generated snapshot
# of `Data`, not an independent charting session (M3-01-RESEARCH.md
# Pitfall 4) -- dedupe between the two is a later plan's job, not this one's.
SHEET_NAMES = ("Data", "Copy of Data")

# By header position (not by name -- the header names past this point are
# exactly what is unknown for a pair block), the column at which a pair
# block's semantics become unresolved. See map_block_to_frame.
PAIR_BLOCK_TAIL_ANCHOR = "RECEIVED BY"


class SheetNotFoundError(Exception):
    """Raised when the requested sheet is absent from the workbook."""


@dataclass(frozen=True)
class HcBlock:
    """One contiguous run of same-kind rows within a sheet (block
    segmentation by column-1 dtype, not by header text -- see module
    docstring Pattern 1).

    `index` is the 0-based order of appearance within the sheet. `header` is
    the sheet's single header row, shared by every block (both block kinds
    are read under the same physical header row -- that mismatch for `pair`
    blocks is exactly the finding this module exists to report, not hide).
    `rows` is `(physical_row_number, cell_values)` per row, in sheet order.
    """

    index: int
    kind: str  # "pair" | "numeric"
    header: list[Any]
    rows: list[tuple[int, tuple]]
    first_row: int
    last_row: int


@dataclass
class HcIngestNotices:
    """Everything found for one (workbook, sheet): never raised, always
    returned. Keyed on the source label rather than a game id (mirrors
    `hudl.IngestNotices`), because one HC sheet holds many games across
    possibly several blocks -- there is no single game id to key on until
    plan M3-01-03's game-identity resolution runs.
    """

    source_label: str
    sheet: str
    messages: list[str] = field(default_factory=list)
    header: HeaderReport | None = None
    domain: list[DomainViolation] = field(default_factory=list)
    conform: ConformReport | None = None


def slugify(value: str) -> str:
    """Lowercase; every run of non-alphanumeric characters becomes a single
    `-`; strip leading/trailing `-`."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower())
    return slug.strip("-")


def hc_source_label(path: Path, sheet: str) -> str:
    """The value that lands in the canonical `source` column (HC-D01):
    `hc_workbook:<slugified file stem>:<slugified sheet name>`.
    """
    return f"hc_workbook:{slugify(Path(path).stem)}:{slugify(sheet)}"


def read_sheet_rows(path: Path, sheet: str) -> tuple[list[Any], list[tuple[int, tuple]], list[str]]:
    """Read one sheet's header and data rows with formula resolution.

    `openpyxl.load_workbook(path, data_only=True, read_only=True)`; always
    closes the workbook in a `finally` (read-only workbooks otherwise keep a
    file handle open). Header comes from row 1; data rows from
    `ws.iter_rows(min_row=2, values_only=True)`, physical row number kept
    alongside each kept row.

    Rows where every cell is `None` or an empty string are skipped and
    counted (a blank row inside the sheet's used range is not data). Cells
    holding the literal string `"#N/A"` (unresolved formula residue) are
    counted. A sheet whose header is intact but whose data rows are all
    blank is reported via a message naming both the physical row count and
    the (zero) kept count -- callers must be able to tell "empty tab" from
    "no such sheet" (`SheetNotFoundError`, raised only when `sheet` is
    absent from `wb.sheetnames`).
    """
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    try:
        if sheet not in wb.sheetnames:
            raise SheetNotFoundError(
                f"sheet {sheet!r} not found in {Path(path).name!r}; "
                f"available sheets: {wb.sheetnames}"
            )
        ws = wb[sheet]

        header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), ())
        header = list(header_row)

        rows: list[tuple[int, tuple]] = []
        physical = 0
        blank = 0
        na_count = 0
        for row_num, values in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            physical += 1
            if all(v is None or v == "" for v in values):
                blank += 1
                continue
            na_count += sum(1 for v in values if v == "#N/A")
            rows.append((row_num, values))

        messages: list[str] = []
        kept = len(rows)
        if kept == 0:
            messages.append(
                f"Sheet {sheet!r} ist leer/empty: {physical} physische Zeile(n) gesehen, "
                f"{kept} Datenzeile(n) übernommen -- als leere Quelle melden, nicht als "
                "Quelle mit null Spielen behandeln"
            )
        if blank:
            messages.append(f"{blank} leere Zeile(n) innerhalb des Bereichs übersprungen")
        if na_count:
            messages.append(f"{na_count} Zelle(n) mit '#N/A'-Formelresten gefunden")

        return header, rows, messages
    finally:
        wb.close()


def segment_blocks(
    header: list[Any], rows: list[tuple[int, tuple]]
) -> tuple[list[HcBlock], list[str]]:
    """Classify every row by its first cell and group consecutive same-kind
    rows into blocks, in sheet order (Pattern 1: block segmentation by
    column-1 dtype, not by header text).

    `isinstance(v, (int, float)) and not isinstance(v, bool)` is `numeric`
    (openpyxl always delivers Excel numbers as `1.0`, never `1` -- Pitfall
    5; `bool` is an `int` subclass in Python, excluded explicitly so a
    charted `True`/`False` cell never misclassifies as numeric). A non-empty
    `str` first cell is `pair`. Anything else (`None`, `""`, or any other
    type) is a skipped row -- counted, but never treated as a block
    boundary, so a stray blank first cell in the middle of a run does not
    fracture one real block into two.
    """
    blocks: list[HcBlock] = []
    current_kind: str | None = None
    current_rows: list[tuple[int, tuple]] = []
    skipped = 0

    def _flush() -> None:
        if current_kind is None or not current_rows:
            return
        blocks.append(
            HcBlock(
                index=len(blocks),
                kind=current_kind,
                header=header,
                rows=list(current_rows),
                first_row=current_rows[0][0],
                last_row=current_rows[-1][0],
            )
        )

    for row_num, values in rows:
        first_cell = values[0] if values else None
        if isinstance(first_cell, bool):
            kind: str | None = None
        elif isinstance(first_cell, (int, float)):
            kind = "numeric"
        elif isinstance(first_cell, str) and first_cell != "":
            kind = "pair"
        else:
            kind = None

        if kind is None:
            skipped += 1
            continue

        if kind != current_kind:
            _flush()
            current_kind = kind
            current_rows = []
        current_rows.append((row_num, values))

    _flush()

    messages: list[str] = []
    if skipped:
        messages.append(
            f"{skipped} Zeile(n) bei der Blockerkennung übersprungen "
            "(erste Zelle weder Zahl noch Text)"
        )
    for block in blocks:
        messages.append(
            f"Block {block.index} ({block.kind}): rows {block.first_row}-{block.last_row}, "
            f"{len(block.rows)} Zeilen"
        )
    if blocks:
        n_pair = sum(len(b.rows) for b in blocks if b.kind == "pair")
        n_numeric = sum(len(b.rows) for b in blocks if b.kind == "numeric")
        total = n_pair + n_numeric
        messages.append(f"Block-Split: {n_pair}/{total} pair, {n_numeric}/{total} numeric")

    return blocks, messages
