"""New-format Hudl export ingest: filename -> GameMeta, contract-validated header,
exact-token RESULT parsing, contract derivations, and a conformed canonical frame.

Implements the phase 1.1 contract rules (docs/data-contract.md,
docs/data-contract.schema.json) that supersede the fragile notebook heuristics
kept only in `ingest/legacy.py`: substring RESULT matching and the
"first-drive-is-home" heuristic. New-format ingest instead derives `posteam`
from the filename's TEAM1/ODK rule and parses RESULT via exact token match
after splitting on ", ".

`ingest_file` order of operations: parse_filename -> read_export ->
validate_header -> check_column_domains (never raise on data-quality findings,
only on structural ones) -> constant/extras columns -> posteam/defteam ->
team mapping -> identity casts + yardline_50 -> RESULT tokens -> outcome
derivation -> drive_id -> half -> scoring chain -> yards_gained/first_down ->
conform_to_canonical. Everything data-quality related is collected into
`IngestNotices`, never raised.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from flag_football_ep.canonical import ConformReport
from flag_football_ep.validation.schema import (
    Contract,
    DomainViolation,
    HeaderReport,
    check_column_domains,
    validate_header,
)

__all__ = [
    "GameMeta",
    "FilenameError",
    "WrongDelimiterError",
    "IngestNotices",
    "parse_filename",
    "read_export",
    "ingest_file",
]


class FilenameError(Exception):
    """Raised when an export filename matches neither accepted pattern."""


class WrongDelimiterError(Exception):
    """Raised when an export does not parse as a ';'-delimited file."""


# docs/data-contract.schema.json file_format.filename_pattern:
#   YYYY-MM-DD_{TEAM1}-vs-{TEAM2}_{COMP}.csv
_PRIMARY_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})_"
    r"(?P<team1>[A-Z]{3})-vs-(?P<team2>[A-Z]{3})_(?P<comp>.+)$"
)
# docs/data-contract.schema.json file_format.filename_fallback_pattern:
#   YYYY_{TEAM1}-vs-{TEAM2}[_{COMP}][_n].csv
_FALLBACK_RE = re.compile(
    r"^(?P<year>\d{4})_(?P<team1>[A-Z]{3})-vs-(?P<team2>[A-Z]{3})(?P<rest>(?:_.+)?)$"
)
_PRIMARY_PATTERN_DESC = "YYYY-MM-DD_{TEAM1}-vs-{TEAM2}_{COMP}.csv"
_FALLBACK_PATTERN_DESC = "YYYY_{TEAM1}-vs-{TEAM2}[_{COMP}][_n].csv"


@dataclass(frozen=True)
class GameMeta:
    """Metadata parsed from a Hudl export filename."""

    game_id: str
    filename: str
    season: int
    game_date: str | None
    team1: str
    team2: str
    competition: str


@dataclass
class IngestNotices:
    """Everything `ingest_file` found for one game: never raised, always returned."""

    game_id: str
    header: HeaderReport
    domain: list[DomainViolation]
    conform: ConformReport
    messages: list[str] = field(default_factory=list)


def parse_filename(path: Path) -> GameMeta:
    """Parse a Hudl export filename into `GameMeta`.

    Tries the primary ISO-date pattern first, then the year-only fallback,
    both anchored regexes. `game_id` is always the filename stem verbatim
    (matches `canonical.make_game_id("hudl", stem)`), so the fallback
    pattern's `_n` collision ordinal is preserved in `game_id` even though it
    is stripped from `competition`.
    """
    stem = path.stem
    filename = path.name

    m = _PRIMARY_RE.match(stem)
    if m:
        return GameMeta(
            game_id=stem,
            filename=filename,
            season=int(m.group("year")),
            game_date=f"{m.group('year')}-{m.group('month')}-{m.group('day')}",
            team1=m.group("team1"),
            team2=m.group("team2"),
            competition=m.group("comp"),
        )

    m = _FALLBACK_RE.match(stem)
    if m:
        rest = m.group("rest")
        parts = [p for p in rest.split("_") if p]
        if parts and parts[-1].isdigit():
            parts = parts[:-1]
        return GameMeta(
            game_id=stem,
            filename=filename,
            season=int(m.group("year")),
            game_date=None,
            team1=m.group("team1"),
            team2=m.group("team2"),
            competition="_".join(parts),
        )

    raise FilenameError(
        f"filename {filename!r} matches neither accepted pattern: "
        f"primary {_PRIMARY_PATTERN_DESC!r} nor fallback {_FALLBACK_PATTERN_DESC!r}"
    )


def read_export(path: Path) -> pl.DataFrame:
    """Read a ';'-delimited, utf-8-sig Hudl export with every column as Utf8.

    Mirrors the notebook's `infer_schema_length=0` choice so every column
    arrives untyped. Strips a leading BOM from the first column name. Raises
    `WrongDelimiterError` when the parsed frame has one column or fewer --
    every real Hudl export has at least eight columns, so a single-column
    parse means the file was not actually ';'-delimited (e.g. a comma
    export).
    """
    df = pl.read_csv(path, separator=";", infer_schema_length=0, encoding="utf8-lossy")

    if df.columns:
        first = df.columns[0]
        stripped = first.lstrip("﻿")
        if stripped != first:
            df = df.rename({first: stripped})

    if df.width <= 1:
        raise WrongDelimiterError(
            f"{path}: parsed only {df.width} column(s) using ';' as separator -- "
            "the export is likely not ';'-delimited; check the file's delimiter"
        )

    return df


def ingest_file(
    path: Path,
    contract: Contract,
    half_boundaries: pl.DataFrame | None = None,
    team_mapping: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, IngestNotices]:
    """Parse, validate and (eventually) conform one Hudl export to canonical.

    Task 1 scope: parse_filename -> read_export -> validate_header ->
    check_column_domains. Structural problems (bad filename, missing core
    column) raise; data-quality findings (domain violations, unknown
    columns) are only ever recorded in the returned `IngestNotices`. Tasks 2
    and 3 extend this function with RESULT-token parsing, the outcome/
    identity/scoring derivations and the final `conform_to_canonical` call.
    """
    meta = parse_filename(path)
    df = read_export(path)
    df, header_report = validate_header(df, contract)
    domain_violations = check_column_domains(df, contract)

    notices = IngestNotices(
        game_id=meta.game_id,
        header=header_report,
        domain=domain_violations,
        conform=ConformReport(),
        messages=[],
    )
    return df, notices
