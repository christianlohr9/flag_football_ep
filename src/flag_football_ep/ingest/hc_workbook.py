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

Game segmentation and identity: `segment_games` splits a block into
per-game slices (numeric: `PLAY #` reset; pair: team-pair change);
`resolve_game_identity` resolves a slice against the maintained
`data/reference/hc_games.csv`, degrading to a provisional id plus a notice
on a miss rather than raising (HC-D04).

`ingest_workbook` is this module's convergence point: it reuses
`ingest.hudl`'s derivation chain unchanged (`derive_identity_columns`,
`parse_result_tokens`, `derive_outcome_columns`, `derive_drive_id`,
`derive_yards_gained_first_down`) rather than forking a second
RESULT-parsing implementation (HC-D01), and calls
`canonical.conform_to_canonical` to converge onto the canonical schema with
`source = hc_workbook:<file>:<sheet>`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import openpyxl
import polars as pl

from flag_football_ep.canonical import (
    CORE_COLUMNS,
    NULLABLE_EXTRAS,
    ConformReport,
    add_score_columns,
    add_scoring_play_team,
    conform_to_canonical,
)
from flag_football_ep.ingest import hudl
from flag_football_ep.reference import map_players
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
    "HcGameSlice",
    "HcGameIdentity",
    "segment_games",
    "resolve_game_identity",
    "count_result_tokens",
    "ingest_workbook",
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

# Extends hudl._CHARTING_RENAME with the HC-only charting columns that have
# no equivalent in any Hudl export (HC-D01: reuse, don't fork). Several of
# these (OFF STR, THROWN BY, YAC) already exist in hudl._CHARTING_RENAME --
# re-declared here for documentation purposes; the dict-merge below is a
# harmless no-op overwrite for those keys. Matched case-insensitively and
# whitespace-trimmed against the sheet header (see _rename_target).
_HC_ONLY_RENAME: dict[str, str] = {
    "AIR YARDS": "air_yards",
    "BF ACTION": "bf_action",
    "HAND": "hand",
    "EFFICIENCY": "efficiency",
    "DRIVE SUCCESS": "drive_success",
    "OFF STR": "off_str",
    "THROWN BY": "thrown_by",
    "YAC": "yac",
}
_HC_RENAME: dict[str, str] = {**hudl._CHARTING_RENAME, **_HC_ONLY_RENAME}
# case-insensitive/whitespace-tolerant lookup: normalized header -> canonical extra
_HC_RENAME_UPPER: dict[str, str] = {k.strip().upper(): v for k, v in _HC_RENAME.items()}

# Columns this module invents for the pair block's team-name-pair cells
# (plan M3-01-03 uses them as the game-identity key) -- never flagged as an
# unmapped header, since they are not sheet headers at all.
_SYNTHETIC_COLUMNS = frozenset({"hc_pair_team1", "hc_pair_team2"})


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
    game-identity resolution (`resolve_game_identity`) runs.

    `unmapped_players` is PII (raw player names/jersey labels left unmapped
    by `reference.map_players`) -- it exists so a caller can decide what to
    do with the labels themselves (e.g. extend `player_mapping.csv`), but it
    must NEVER be rendered into a report, a console line, a doc or a commit
    message; only its length (`len(notices.unmapped_players)`) may ever
    appear in human-readable output. `result_token_counts` is not PII: every
    token observed in the sheet's `RESULT` column (contract vocabulary or
    not), counted by `count_result_tokens`.
    """

    source_label: str
    sheet: str
    messages: list[str] = field(default_factory=list)
    header: HeaderReport | None = None
    domain: list[DomainViolation] = field(default_factory=list)
    conform: ConformReport | None = None
    unmapped_players: list[str] = field(default_factory=list)
    result_token_counts: dict[str, int] = field(default_factory=dict)


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


def _normalize_header(header: list[Any]) -> tuple[list[str], list[int], list[str]]:
    """Trim header names, drop `None`/empty ones, de-duplicate the rest.

    Returns `(clean_names, kept_indices, messages)`: `clean_names[i]` is the
    (possibly de-duplicated) name for the original column at `kept_indices[i]`.
    A repeated name gets `_2`, `_3`, ... appended for its 2nd, 3rd, ...
    occurrence; each rename is named in a message so a duplicate charting
    column is never silently merged into the first one under the same name.
    """
    messages: list[str] = []
    seen: dict[str, int] = {}
    clean_names: list[str] = []
    kept_indices: list[int] = []

    for i, raw_name in enumerate(header):
        if raw_name is None:
            continue
        name = str(raw_name).strip()
        if name == "":
            continue

        seen[name] = seen.get(name, 0) + 1
        if seen[name] > 1:
            deduped = f"{name}_{seen[name]}"
            messages.append(
                f"doppelte Spalte {name!r} an Position {i} als {deduped!r} umbenannt"
            )
            name = deduped

        clean_names.append(name)
        kept_indices.append(i)

    return clean_names, kept_indices, messages


def _cell_to_utf8(value: Any, *, strip_integral: bool) -> str | None:
    """Cast one cell to its Utf8 representation.

    `None` stays null. A `bool` (checked before the numeric branch -- `bool`
    is an `int` subclass) becomes its Python str form. When `strip_integral`
    is true, a `float` with no fractional part becomes its plain integer
    string (`25.0` -> `"25"`) -- required both so jersey-number-shaped
    columns are usable as `player_mapping.csv` lookup keys, and so a
    genuinely numeric contract column (DN/DIST/YARD LN/PLAY #) stays
    castable downstream (a trailing ".0" would fail polars' non-strict
    str->int cast). `RESULT` is the one column this module calls with
    `strip_integral=False`: it is a free-text contract column, so a numeric
    charting error landing there (the real `-5.0` found in the corpus) is
    preserved verbatim as evidence of the error rather than normalized away.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, float) and strip_integral and value.is_integer():
        return str(int(value))
    return str(value)


def _rename_target(header_name: str) -> str | None:
    """The canonical extras name for `header_name`, or `None` if this module
    has no rename for it (case-insensitive, whitespace-trimmed lookup)."""
    return _HC_RENAME_UPPER.get(header_name.strip().upper())


def _null_pair_block_tail(df: pl.DataFrame, clean_names: list[str]) -> tuple[pl.DataFrame, list[str]]:
    """Pair-block handling (M3-01-RESEARCH.md Pitfall 2 / Open Question #2).

    The first two columns hold a team-name pair, not `PLAY #`/`ODK` --
    their raw values survive under dedicated names (`hc_pair_team1`/
    `hc_pair_team2`, plan M3-01-03's game-identity key) before `PLAY #`/
    `ODK` themselves are nulled. Columns from `PAIR_BLOCK_TAIL_ANCHOR`
    onward (by position, not by name -- the header names past that point
    are exactly what is unknown for this block) are nulled with one notice
    naming the reason: guessing the column shift would swap passer,
    receiver and gain. Columns before the anchor (through `TARGET ROUTE`)
    are left untouched -- they line up with the header even in a pair block
    (M3-01-RESEARCH.md Pitfall 2).
    """
    messages: list[str] = []
    n_rows = df.height

    if len(clean_names) >= 1:
        df = df.with_columns(pl.col(clean_names[0]).alias("hc_pair_team1"))
    if len(clean_names) >= 2:
        df = df.with_columns(pl.col(clean_names[1]).alias("hc_pair_team2"))

    for core_name in ("PLAY #", "ODK"):
        if core_name in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(core_name))
    messages.append(
        f"Pair-Block: PLAY #/ODK für {n_rows} Zeile(n) auf null gesetzt "
        "(Spalten enthalten stattdessen ein Team-Namenspaar)"
    )

    anchor_idx = next(
        (
            i
            for i, name in enumerate(clean_names)
            if name.strip().upper() == PAIR_BLOCK_TAIL_ANCHOR.upper()
        ),
        None,
    )
    if anchor_idx is not None:
        tail_names = clean_names[anchor_idx:]
        if tail_names:
            df = df.with_columns(
                [pl.lit(None, dtype=pl.Utf8).alias(name) for name in tail_names]
            )
            messages.append(
                f"Pair-Block: Spalten ab {PAIR_BLOCK_TAIL_ANCHOR!r} ({len(tail_names)} "
                f"Spalte(n), {n_rows} Zeile(n)) auf null gesetzt -- Frage 2 offen "
                "(Spaltenversatz ungeklärt); ein geratener Spaltenversatz würde "
                "Passgeber, Empfänger und Raumgewinn vertauschen"
            )

    return df, messages


def map_block_to_frame(
    block: HcBlock, contract: Contract
) -> tuple[pl.DataFrame, HeaderReport, list[DomainViolation], list[str]]:
    """Map one `HcBlock` onto the contract's raw column names, dtype-validated.

    Order of operations (Pattern 2: dtype-validated mapping, never
    header-text-only):
    1. Normalize the header (`_normalize_header`).
    2. Build the frame with every cell cast to Utf8 (`_cell_to_utf8`).
    3. For a `pair` block, null out the unresolved tail (`_null_pair_block_tail`).
    4. Materialize every absent contract core column as all-null Utf8 (every
       HC sheet lacks `PLAY TYPE`; a pair block also lacks a real `PLAY #`/
       `ODK`, but those are already present as columns by this point --
       nulled, not absent). Then `validate_header` (which now cannot raise,
       since every core column already exists) and `check_column_domains`.
    5. Rename charting columns onto their canonical extras via `_HC_RENAME`;
       every header with no contract slot and no rename target is collected
       into one notice, never silently dropped.

    Never raises on a data-quality finding; a block whose header carries no
    usable column names returns an empty frame plus a message.
    """
    messages: list[str] = []

    clean_names, kept_indices, dedup_messages = _normalize_header(block.header)
    messages.extend(dedup_messages)

    if not clean_names:
        messages.append(
            f"Block {block.index} ({block.kind}): keine verwertbaren Spalten im Header, "
            "leerer Frame zurückgegeben"
        )
        return (
            pl.DataFrame(),
            HeaderReport(missing_core=[], materialized_optional=[], unknown=[]),
            [],
            messages,
        )

    columns_data: dict[str, list[str | None]] = {name: [] for name in clean_names}
    for _row_num, values in block.rows:
        for name, idx in zip(clean_names, kept_indices):
            value = values[idx] if idx < len(values) else None
            strip_integral = name != "RESULT"
            columns_data[name].append(_cell_to_utf8(value, strip_integral=strip_integral))

    df = pl.DataFrame(columns_data, schema={name: pl.Utf8 for name in clean_names})

    if block.kind == "pair":
        df, pair_messages = _null_pair_block_tail(df, clean_names)
        messages.extend(pair_messages)

    materialized_core = [c for c in contract.core_columns if c not in df.columns]
    if materialized_core:
        df = df.with_columns(
            [pl.lit(None, dtype=pl.Utf8).alias(c) for c in materialized_core]
        )
        messages.append(
            f"Kernspalte(n) im Sheet nicht vorhanden, als null angelegt: {materialized_core}"
        )

    df, header_report = validate_header(df, contract)
    domain_violations = check_column_domains(df, contract)

    known_contract = set(contract.core_columns) | set(contract.optional_columns)
    rename_map: dict[str, str] = {}
    unmapped: list[str] = []
    for name in df.columns:
        if name in _SYNTHETIC_COLUMNS or name in known_contract:
            continue
        target = _rename_target(name)
        if target is not None:
            rename_map[name] = target
        else:
            unmapped.append(name)

    if unmapped:
        messages.append(
            f"Ohne kanonisches Ziel, nicht stillschweigend verworfen: {unmapped}"
        )
    if rename_map:
        df = df.rename(rename_map)

    return df, header_report, domain_violations, messages


@dataclass(frozen=True)
class HcGameSlice:
    """One game's worth of rows inside a single `HcBlock` (game segmentation
    within a block -- see `segment_games`).

    `block_index`/`game_index` are 0-based; `block_key` is the stable,
    block-scoped identifier `b{block_index:02d}-g{game_index:02d}` that
    `data/reference/hc_games.csv` keys on. `rows` is the game's row slice in
    the same `(physical_row_number, cell_values)` shape as `HcBlock.rows`.
    `source_team1`/`source_team2` are the raw team-pair labels for a `pair`
    slice (from the slice's first row), `None` for a `numeric` slice.
    """

    block_index: int
    game_index: int
    block_key: str
    kind: str  # "pair" | "numeric"
    rows: list[tuple[int, tuple]]
    first_row: int
    last_row: int
    source_team1: str | None
    source_team2: str | None


def _coerce_play_number(value: Any) -> float | None:
    """Best-effort numeric coercion of a `PLAY #` cell (Pitfall 5: openpyxl
    delivers Excel numbers as `float`). `None`/non-numeric/unparseable
    strings become `None` -- treated by `segment_games` as "does not
    increase", i.e. a game boundary, never silently merged into the
    previous game.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _normalize_pair_label(value: Any) -> str:
    """Case-insensitive, whitespace-trimmed comparison key for a team-pair
    label. The raw value (not this normalized form) is what survives into
    `HcGameSlice.source_team1`/`source_team2`.
    """
    if value is None:
        return ""
    return str(value).strip().casefold()


def _split_numeric_block(
    rows: list[tuple[int, tuple]], messages: list[str]
) -> list[list[tuple[int, tuple]]]:
    """Split a numeric block's rows into games wherever `PLAY #` (column 0)
    does not increase relative to the previous row -- a reset to 1, any
    decrease, or a null/unparseable cell, which is itself always a boundary
    (never compared against a stale reference value, so one corrupt cell
    cannot make two real games silently look like one)."""
    groups: list[list[tuple[int, tuple]]] = []
    current: list[tuple[int, tuple]] = []
    prev_play_num: float | None = None
    unparseable = 0

    for row_num, values in rows:
        raw = values[0] if values else None
        play_num = _coerce_play_number(raw)
        if play_num is None:
            unparseable += 1

        is_boundary = (
            not current or play_num is None or prev_play_num is None or play_num <= prev_play_num
        )
        if is_boundary and current:
            groups.append(current)
            current = []
        current.append((row_num, values))
        prev_play_num = play_num

    if current:
        groups.append(current)

    if unparseable:
        messages.append(
            f"{unparseable} Zeile(n) mit nicht auswertbarer PLAY # (null oder nicht "
            "parsebar) -- als Spielgrenze behandelt statt zwei Spiele stillschweigend "
            "zusammenzuführen"
        )

    return groups


def _split_pair_block(rows: list[tuple[int, tuple]]) -> list[list[tuple[int, tuple]]]:
    """Split a pair block's rows into games wherever the (team1, team2) pair
    (columns 0-1) changes, compared case-insensitively and whitespace-trimmed."""
    groups: list[list[tuple[int, tuple]]] = []
    current: list[tuple[int, tuple]] = []
    prev_pair: tuple[str, str] | None = None

    for row_num, values in rows:
        t1 = values[0] if len(values) >= 1 else None
        t2 = values[1] if len(values) >= 2 else None
        pair = (_normalize_pair_label(t1), _normalize_pair_label(t2))
        is_boundary = not current or pair != prev_pair
        if is_boundary and current:
            groups.append(current)
            current = []
        current.append((row_num, values))
        prev_pair = pair

    if current:
        groups.append(current)

    return groups


def segment_games(block: HcBlock) -> tuple[list[HcGameSlice], list[str]]:
    """Split one `HcBlock` into per-game `HcGameSlice`s.

    A numeric block splits wherever `PLAY #` does not increase relative to
    the previous row (reset to 1, any decrease, or an unparseable cell -- see
    `_split_numeric_block`). A pair block splits wherever the
    `(source_team1, source_team2)` pair changes (`_split_pair_block`).
    `block_key` is `b{block.index:02d}-g{game_index:02d}`, zero-padded and
    stable across runs for an unchanged sheet -- two blocks in the same sheet
    never collide (distinct `block_index`) and the same `block_key` string in
    two different sheets is disambiguated by the caller's
    `(workbook, sheet, block_key)` lookup, not by this function.
    """
    messages: list[str] = []
    slices: list[HcGameSlice] = []

    if not block.rows:
        return slices, messages

    if block.kind == "numeric":
        groups = _split_numeric_block(block.rows, messages)
    else:
        groups = _split_pair_block(block.rows)

    for game_index, group_rows in enumerate(groups):
        first_row = group_rows[0][0]
        last_row = group_rows[-1][0]
        block_key = f"b{block.index:02d}-g{game_index:02d}"

        source_team1: str | None = None
        source_team2: str | None = None
        if block.kind == "pair":
            first_values = group_rows[0][1]
            t1_raw = first_values[0] if len(first_values) >= 1 else None
            t2_raw = first_values[1] if len(first_values) >= 2 else None
            source_team1 = str(t1_raw) if t1_raw is not None else None
            source_team2 = str(t2_raw) if t2_raw is not None else None

        slices.append(
            HcGameSlice(
                block_index=block.index,
                game_index=game_index,
                block_key=block_key,
                kind=block.kind,
                rows=group_rows,
                first_row=first_row,
                last_row=last_row,
                source_team1=source_team1,
                source_team2=source_team2,
            )
        )

    return slices, messages


@dataclass(frozen=True)
class HcGameIdentity:
    """The resolved identity of one `HcGameSlice`: either a mapped row from
    `data/reference/hc_games.csv` (`provisional=False`) or a degraded
    placeholder (`provisional=True`) built when no row matches. Nothing is
    invented for a provisional identity -- `home_team`/`away_team`/`tier`/
    `season`/`game_date`/`corpus_game_id` all stay `None`.
    """

    game_id: str
    home_team: str | None
    away_team: str | None
    competition: str | None
    season: int | None
    game_date: str | None
    tier: str | None
    corpus_game_id: str | None
    provisional: bool


# HC-D04: `reference.map_teams` is deliberately NOT used for game identity --
# it raises `UnmappedTeamError` on any label with no mapping row, but an HC
# game the maintainer has not yet transcribed into `hc_games.csv` must
# degrade into a provisional id plus a loud notice, not abort the whole
# ingest run. Game identity for this source therefore comes from a direct,
# never-raising filter against the maintained `hc_games` frame below.
def resolve_game_identity(
    slice_: HcGameSlice,
    workbook_slug: str,
    sheet_slug: str,
    hc_games: pl.DataFrame,
) -> tuple[HcGameIdentity, list[str]]:
    """Resolve `slice_`'s game identity against the maintained `hc_games` frame.

    Filters on `(workbook, sheet, block_key)`. On a hit, returns the mapped
    identity built from that row (`provisional=False`), no message. On a
    miss, returns a provisional identity (`game_id =
    "hc-{workbook}-{sheet}-{block_key}"`, every other field `None`) plus
    exactly one notice naming the block key, the physical Excel row range,
    the play count and -- for a pair block -- the raw team labels, so a
    maintainer can add the `hc_games.csv` row without opening Excel. Never
    raises.
    """
    messages: list[str] = []

    match = hc_games.filter(
        (pl.col("workbook") == workbook_slug)
        & (pl.col("sheet") == sheet_slug)
        & (pl.col("block_key") == slice_.block_key)
    )

    if match.height:
        row = match.row(0, named=True)
        identity = HcGameIdentity(
            game_id=row["game_id"],
            home_team=row["home_team"],
            away_team=row["away_team"],
            competition=row["competition"],
            season=row["season"],
            game_date=row["game_date"],
            tier=row["tier"],
            corpus_game_id=row["corpus_game_id"],
            provisional=False,
        )
        return identity, messages

    provisional_id = f"hc-{workbook_slug}-{sheet_slug}-{slice_.block_key}"
    identity = HcGameIdentity(
        game_id=provisional_id,
        home_team=None,
        away_team=None,
        competition=None,
        season=None,
        game_date=None,
        tier=None,
        corpus_game_id=None,
        provisional=True,
    )

    n_plays = len(slice_.rows)
    team_note = ""
    if slice_.kind == "pair":
        team_note = f", Teams laut Sheet: {slice_.source_team1!r} vs. {slice_.source_team2!r}"
    messages.append(
        f"Unbekanntes Spiel {slice_.block_key!r} in {workbook_slug}/{sheet_slug}: "
        f"Zeilen {slice_.first_row}-{slice_.last_row} ({n_plays} Plays){team_note} -- "
        f"provisorische game_id {provisional_id!r} vergeben; data/reference/hc_games.csv "
        "um diese Zeile ergänzen"
    )

    return identity, messages


def _fill_synthesized_play_ids(game_df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Fill a null `PLAY #` with the row's 1-based position within the game
    (pair-block rows always lack a real `PLAY #`; a numeric block may have a
    stray blank cell too). Returns `(df, n_filled)` -- the imported
    `hudl.derive_identity_columns` can then cast `PLAY #` -> `play_id`
    unchanged, real or synthesized.
    """
    n_missing = int(game_df["PLAY #"].null_count())
    if not n_missing:
        return game_df, 0

    game_df = game_df.with_row_index(name="_hc_pos", offset=1)
    game_df = game_df.with_columns(
        pl.when(pl.col("PLAY #").is_null())
        .then(pl.col("_hc_pos").cast(pl.Utf8))
        .otherwise(pl.col("PLAY #"))
        .alias("PLAY #")
    )
    game_df = game_df.drop("_hc_pos")
    return game_df, n_missing


def _stamp_posteam_defteam(
    game_df: pl.DataFrame, kind: str, home_team: str | None, away_team: str | None
) -> pl.DataFrame:
    """Derive `posteam`/`defteam` for one game's rows.

    A `pair` block has no `ODK` (nulled by `_null_pair_block_tail`) -- the
    offence side is undetermined until Frage 2 (the pair block's column
    shift) is answered, so both columns stay null rather than guessed. A
    `numeric` block follows hudl's own convention: `posteam = home_team`
    when `ODK == "O"`, `away_team` for every other non-null `ODK` (`"D"` or
    `"K"` alike -- the `"K"` kickoff override only changes `play_type`, not
    `posteam`), null when `ODK` itself is null.
    """
    if kind == "pair":
        return game_df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("posteam"),
            pl.lit(None, dtype=pl.Utf8).alias("defteam"),
        )

    game_df = game_df.with_columns(
        pl.when(pl.col("ODK") == "O")
        .then(pl.lit(home_team, dtype=pl.Utf8))
        .when(pl.col("ODK").is_not_null())
        .then(pl.lit(away_team, dtype=pl.Utf8))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("posteam")
    )
    game_df = game_df.with_columns(
        pl.when(pl.col("posteam") == pl.lit(home_team, dtype=pl.Utf8))
        .then(pl.lit(away_team, dtype=pl.Utf8))
        .when(pl.col("posteam").is_not_null())
        .then(pl.lit(home_team, dtype=pl.Utf8))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("defteam")
    )
    return game_df


def count_result_tokens(df: pl.DataFrame) -> dict[str, int]:
    """Count every token observed in `RESULT` (split on `", "`), independent
    of the contract's known vocabulary -- so a caller can report per-token
    counts (including any not-yet-ratified token, e.g. an HC-only one, see
    docs/hc-rueckfragen-2026-09.md Frage 3) without re-parsing RESULT itself.
    """
    if df.height == 0 or "RESULT" not in df.columns:
        return {}

    tokens = (
        df["RESULT"]
        .fill_null("")
        .str.split(", ")
        .list.eval(pl.element().filter(pl.element() != ""))
        .explode()
        .drop_nulls()
        .to_list()
    )
    counts: dict[str, int] = {}
    for token in tokens:
        counts[token] = counts.get(token, 0) + 1
    return counts


def ingest_workbook(
    path: Path,
    sheet: str,
    contract: Contract,
    hc_games: pl.DataFrame,
    player_mapping: pl.DataFrame,
) -> tuple[pl.DataFrame, HcIngestNotices]:
    """Turn one (workbook, sheet)'s charted rows into canonical plays.

    Order of operations, mirroring `hudl.ingest_file` and reusing its
    derivation functions unchanged (HC-D01): `read_sheet_rows` ->
    `segment_blocks` -> per block `map_block_to_frame` -> `segment_games` ->
    per game slice `resolve_game_identity`, constant-stamping (`source`,
    `competition`, `season`, `game_id`, `game_date`, `home_team`,
    `away_team`, `result_raw`), `PLAY #` synthesis, `posteam`/`defteam` ->
    concatenate the sheet's game frames (`how="vertical"`; they share a
    schema by construction -- a mismatch is a bug, left to raise rather than
    silently switched to `diagonal`) -> `hudl.derive_identity_columns` ->
    `hudl.parse_result_tokens` -> `hudl.derive_outcome_columns` -> ODK `"K"`
    kickoff override (identical to hudl's) -> `hudl.derive_drive_id` ->
    `half` = typed null (no half-boundary data exists for HC workbooks) ->
    `add_scoring_play_team(credit_defense=True)` -> `add_score_columns` ->
    `hudl.derive_yards_gained_first_down` -> `reference.map_players` (source
    key `"hc_workbook"`, deliberately coarse -- one `player_mapping.csv` row
    serves all three workbooks, not a per-sheet key) -> `count_result_tokens`
    -> `conform_to_canonical`.

    Never raises on a data-quality finding -- everything folds into the
    returned `HcIngestNotices`. A sheet with no usable blocks at all (e.g.
    entirely empty) returns a zero-row `CANONICAL_COLUMNS` frame plus a
    notice, not an error. Structural problems (an absent sheet) still
    propagate from `read_sheet_rows` (`SheetNotFoundError`).
    """
    workbook_slug = slugify(Path(path).stem)
    sheet_slug = slugify(sheet)
    source_label = hc_source_label(path, sheet)

    header, rows, messages = read_sheet_rows(path, sheet)
    blocks, block_messages = segment_blocks(header, rows)
    messages.extend(block_messages)

    game_frames: list[pl.DataFrame] = []
    header_reports: list[HeaderReport] = []
    domain_violations: list[DomainViolation] = []
    pair_row_total = 0
    synthesized_play_ids = 0

    for block in blocks:
        block_df, header_report, block_domain, block_map_messages = map_block_to_frame(
            block, contract
        )
        messages.extend(block_map_messages)
        header_reports.append(header_report)
        domain_violations.extend(block_domain)

        slices, segment_messages = segment_games(block)
        messages.extend(segment_messages)

        offset = 0
        for slice_ in slices:
            n = len(slice_.rows)
            game_df = block_df.slice(offset, n)
            offset += n

            # A pair block's frame carries the two synthetic hc_pair_team1/
            # hc_pair_team2 columns (_null_pair_block_tail); a numeric
            # block's frame never does. Drop them here -- the raw labels
            # already live in slice_.source_team1/source_team2 for identity
            # resolution, and keeping them would break the "every game frame
            # in this sheet shares one schema" concat invariant below.
            present_synthetic = [c for c in _SYNTHETIC_COLUMNS if c in game_df.columns]
            if present_synthetic:
                game_df = game_df.drop(present_synthetic)

            identity, identity_messages = resolve_game_identity(
                slice_, workbook_slug, sheet_slug, hc_games
            )
            messages.extend(identity_messages)

            game_df = game_df.with_columns(
                pl.lit(source_label, dtype=pl.Utf8).alias("source"),
                pl.lit(identity.competition, dtype=pl.Utf8).alias("competition"),
                pl.lit(identity.season, dtype=pl.Int32).alias("season"),
                pl.lit(identity.game_id, dtype=pl.Utf8).alias("game_id"),
                pl.lit(identity.game_date, dtype=pl.Utf8).alias("game_date"),
                pl.lit(identity.home_team, dtype=pl.Utf8).alias("home_team"),
                pl.lit(identity.away_team, dtype=pl.Utf8).alias("away_team"),
                pl.col("RESULT").alias("result_raw"),
            )

            game_df, n_filled = _fill_synthesized_play_ids(game_df)
            synthesized_play_ids += n_filled

            game_df = _stamp_posteam_defteam(
                game_df, slice_.kind, identity.home_team, identity.away_team
            )
            if slice_.kind == "pair":
                pair_row_total += n

            game_frames.append(game_df)

    if pair_row_total:
        messages.append(
            f"{pair_row_total} Pair-Block-Zeile(n): posteam/defteam/ODK unbestimmt bis "
            "Frage 2 (Spaltenversatz) beantwortet ist"
        )
    if synthesized_play_ids:
        messages.append(
            f"PLAY # für {synthesized_play_ids} Zeile(n) synthetisiert (1-basierte "
            "Position innerhalb des Spiels, keine PLAY # im Sheet vorhanden)"
        )

    if not game_frames:
        df = pl.DataFrame(schema={**CORE_COLUMNS, **NULLABLE_EXTRAS})
        messages.append(
            f"Sheet {sheet!r} enthält keine auswertbaren Blöcke: leerer kanonischer "
            "Frame zurückgegeben"
        )
    else:
        df = pl.concat(game_frames, how="vertical")

        df = hudl.derive_identity_columns(df)
        df = hudl.parse_result_tokens(df)
        df, outcome_messages = hudl.derive_outcome_columns(df)
        messages.extend(outcome_messages)

        # ODK == 'K' overrides play_type to "kickoff" regardless of RESULT
        # tokens, identical to hudl's own override -- harmless when no 'K'
        # row exists in this sheet.
        df = df.with_columns(
            pl.when(pl.col("ODK") == "K")
            .then(pl.lit("kickoff"))
            .otherwise(pl.col("play_type"))
            .alias("play_type")
        )

        df = hudl.derive_drive_id(df)

        df = df.with_columns(pl.lit(None, dtype=pl.Int32).alias("half"))
        messages.append(
            f"HC-Workbooks kennen keine Halbzeitgrenzen: half für {df.height} Zeile(n) "
            "auf null gesetzt"
        )

        df = add_scoring_play_team(df, credit_defense=True)
        df = add_score_columns(df)
        df = hudl.derive_yards_gained_first_down(df)

    # Coarse "hc_workbook" source key (not the per-sheet source_label): one
    # player_mapping.csv row then serves all three HC workbooks, since the
    # HC's own player-label vocabulary does not vary per workbook/sheet.
    player_columns = ["qb", "thrown_by", "received_by", "target", "tackle"]
    map_result = map_players(df, player_mapping, source="hc_workbook", columns=player_columns)
    df = map_result.frame
    unmapped_players = map_result.unmapped
    if unmapped_players:
        # PII discipline: only the count, never the labels themselves.
        messages.append(
            f"{len(unmapped_players)} nicht zugeordnete Spieler-Label in "
            f"{player_columns}"
        )

    result_token_counts = count_result_tokens(df)

    df, conform_report = conform_to_canonical(df, source_label)

    notices = HcIngestNotices(
        source_label=source_label,
        sheet=sheet,
        messages=messages,
        header=header_reports[0] if header_reports else None,
        domain=domain_violations,
        conform=conform_report,
        unmapped_players=unmapped_players,
        result_token_counts=result_token_counts,
    )
    return df, notices
