"""Shared tolerant reader/writer for CSV files the project owner edits in Microsoft Excel.

The project owner (not a developer) routinely opens a committed or generated CSV directly in
Excel, types into it, and saves. Two Excel behaviours matter here:

1. **Reading a plain UTF-8 CSV (no byte-order mark) on Excel for macOS**: Excel guesses the
   encoding from the byte content rather than asking, and for a file with German umlauts
   guesses wrong far too often -- an "Nühse" in the source file renders as "NÃ¼hse" on screen. A
   leading UTF-8 BOM (`EF BB BF`) is Excel's own signal to trust UTF-8, so every CSV this
   project hands the owner to open directly must be written WITH one (`utf-8-sig`).
2. **Saving that file back**: Excel then typically re-writes it semicolon-delimited (a
   German-locale decimal-comma reassigns the CSV field separator), non-UTF-8 (`mac_roman` on
   macOS, `cp1252` on Windows), and CRLF-terminated -- not the shape it was handed in. Every
   reader of an owner-edited file must tolerate that shape back, silently (a notice, never an
   error) -- this module is the one place that tolerance lives, so every caller shares the exact
   same decode/delimiter/line-ending rules instead of re-implementing them per file.

Originally lived as private helpers inside `ingest.ifaf` (`_decode_spot_fill_bytes`/
`_sniff_csv_delimiter`, 2026-09-08 Nachtrag) for the IFAF spot-fill/corrections fill files;
promoted to a shared module (2026-09-10) once the same tolerance was needed for the player
mapping template (`scripts/player_mapping_template.py --apply-filled`) and the worksheet CSVs
the owner opens directly (`ingest.ifaf_spot_fill_worksheets`).

`write_owner_csv` (also added 2026-09-10) is the writer-side counterpart for a `polars`
DataFrame -- `pl.DataFrame.write_csv` has no BOM option of its own, so every owner-facing
export (`scripts/export_pbp_csv.py`) writes through this one helper instead of hand-rolling
the BOM-prefix-then-write dance per call site.

Committed reference CSVs under `data/reference/*.csv` are the deliberate exception: they are
read by pipeline code, never opened directly by the owner in Excel, so they stay plain UTF-8
with no BOM -- do not route their writers through `write_owner_csv`.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

# The writer encoding for every CSV the project owner opens directly in Excel: a UTF-8 byte-
# order mark so Excel (macOS or Windows) detects UTF-8 instead of guessing wrong on umlauts.
# NEVER used for a committed `data/reference/*.csv` file that is read by pipeline code, not by
# the owner in Excel -- those stay plain UTF-8, no BOM (see module docstring point 1).
OWNER_CSV_WRITE_ENCODING = "utf-8-sig"

_UTF8_BOM = b"\xef\xbb\xbf"

# A cp1252 decode landing on any of this block's own typographic characters ("smart quotes",
# an em dash, ...) is treated as a `mac_roman` file mislabeled `cp1252` and re-decoded as such --
# see `decode_owner_csv_bytes` below for the full disambiguation rationale (moved verbatim from
# `ingest.ifaf._CP1252_MOJIBAKE_MARKERS`, 2026-09-08).
_CP1252_MOJIBAKE_MARKERS: frozenset[str] = frozenset("ƒˆ‰Š‹ŒŽ‘’“”•–—˜™š›œžŸ")


def decode_owner_csv_bytes(data: bytes) -> tuple[str, str | None]:
    """Decode one owner-edited file's raw bytes, tolerating a leading UTF-8 BOM and a
    non-UTF-8 Excel export.

    Returns `(text, fallback_encoding)` -- `fallback_encoding` is `None` for the silent,
    expected case (UTF-8, with or without a BOM -- the BOM itself is stripped from `text`,
    never left as a literal `﻿` prefix on the first field name), else the codec name
    actually used (`"cp1252"` or `"mac_roman"`), for the caller's own notice.

    Encoding fallback order: a leading `EF BB BF` byte sequence is always UTF-8-with-BOM (the
    BOM is not valid at the start of any of the fallback single-byte encodings' own text, so
    checking for it first is unambiguous) -- stripped, decoded as UTF-8, no notice. Otherwise
    UTF-8 (the expected, silent-success case for a file already normalised or written by a text
    editor). On a decode failure, `cp1252` (the far more common single-byte Windows export
    encoding) is tried next -- but every single byte value has *some* cp1252 mapping, so a
    wrong guess never raises. `_CP1252_MOJIBAKE_MARKERS` is cp1252's own "smart punctuation"
    block (`\x80`-`\x9f`); a cp1252 decode landing on any of them is treated as a `mac_roman`
    file (an Excel-for-Mac export) mislabeled `cp1252` and re-decoded as such -- a heuristic,
    not a certainty (a genuine cp1252 file that legitimately uses an em dash or a smart quote
    would be mis-detected), but matches every byte pattern observed in this project's real
    Excel exports. Never raises -- every fallback codec here accepts every byte value.
    """
    if data.startswith(_UTF8_BOM):
        return data[len(_UTF8_BOM) :].decode("utf-8"), None
    try:
        return data.decode("utf-8"), None
    except UnicodeDecodeError:
        pass
    cp1252_text = data.decode("cp1252", errors="replace")
    if any(ch in _CP1252_MOJIBAKE_MARKERS for ch in cp1252_text):
        return data.decode("mac_roman", errors="replace"), "mac_roman"
    return cp1252_text, "cp1252"


def sniff_owner_csv_delimiter(text: str) -> str:
    """`;` when the first line looks like a semicolon-delimited Excel export (contains at
    least one `;` and no `,` at all), else the standard `,`."""
    first_line = text.splitlines()[0] if text else ""
    if ";" in first_line and "," not in first_line:
        return ";"
    return ","


def read_owner_csv(path: Path) -> tuple[list[dict[str, str | None]], list[str]]:
    """Read one owner-edited CSV file into `csv.DictReader`-shaped row dicts, tolerating a
    BOM, `;` delimiter, `mac_roman`/`cp1252` encoding and CRLF line endings -- see the module
    docstring for why a file the owner has opened and saved in Excel routinely comes back in
    exactly this shape.

    Returns `([], [])` for a missing file -- the normal not-yet-created case, not an error.

    Returns `(rows, notices)` -- `notices`, each prefixed with the file's own name, name any
    fallback actually used (encoding and/or delimiter); a plain UTF-8 (with or without a BOM),
    comma-delimited, LF file produces no notices. This is a generic, untyped reader (every
    cell is `str | None`, no per-column parsing/validation) for callers that only need the raw
    rows -- `ingest.ifaf.load_spot_fill`/`load_corrections` build on the lower-level
    `decode_owner_csv_bytes`/`sniff_owner_csv_delimiter` primitives directly instead, since
    they need bespoke per-row typed parsing (an unparseable `sequence`/`ballOn` cell, a
    trailing unnamed column folded into `note`) this generic reader does not perform.
    """
    path = Path(path)
    if not path.exists():
        return [], []

    notices: list[str] = []
    text, fallback_encoding = decode_owner_csv_bytes(path.read_bytes())
    if fallback_encoding is not None:
        notices.append(f"{path.name}: not valid UTF-8, decoded as {fallback_encoding}")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    delimiter = sniff_owner_csv_delimiter(text)
    if delimiter != ",":
        notices.append(f"{path.name}: semicolon-delimited (Excel export), auto-detected")

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows = [dict(row) for row in reader]
    return rows, notices


def write_owner_csv(df: "pl.DataFrame", path: Path, **write_csv_kwargs: object) -> None:
    """Write a polars DataFrame to `path` as a CSV the project owner can open directly in
    Excel -- `utf-8-sig` (a leading BOM), so Excel (macOS or Windows) detects UTF-8 instead of
    guessing wrong on umlauts (see module docstring). `pl.DataFrame.write_csv` has no BOM
    option of its own; this writes the BOM once, then the frame's own CSV text, in a single
    call, so every export shares the exact same writer instead of a per-call-site BOM dance.

    `**write_csv_kwargs` is passed through to `pl.DataFrame.write_csv` verbatim (e.g.
    `float_precision`) -- `file` must not be passed, this function always writes in-memory
    then to `path` itself.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = df.write_csv(**write_csv_kwargs)
    with path.open("w", encoding=OWNER_CSV_WRITE_ENCODING, newline="") as fh:
        fh.write(text)
