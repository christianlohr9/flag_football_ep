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
    "parse_result_tokens",
    "derive_outcome_columns",
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


# RESULT grammar (docs/data-contract.schema.json, column RESULT; docs/data-contract.md
# section "RESULT-Vokabular & Grammatik"): a value is a base token plus optional
# modifiers, joined by ", ". Matching is exact token membership after splitting on
# that separator, case-sensitive -- no substring-based matching anywhere in this
# module. This replaces the legacy pipeline's fragile substring semantics,
# which only worked by accident (Incomplete not matching contains("Complete") only
# because polars compares case-sensitively; TD vs Def TD needed an explicit guard).
_BASE_TOKENS = (
    "Rush", "KNEEL", "Sack", "Interception", "Complete", "Incomplete",
    "Good", "No Good", "Fumble", "Penalty",
)
_MODIFIER_TOKENS = ("TD", "Def TD", "Safety", "Penalty")
_ALL_TOKENS = sorted(set(_BASE_TOKENS) | set(_MODIFIER_TOKENS))

# token -> tok_* boolean column name
_TOKEN_COLUMN = {
    "Rush": "tok_rush",
    "KNEEL": "tok_kneel",
    "Sack": "tok_sack",
    "Interception": "tok_interception",
    "Complete": "tok_complete",
    "Incomplete": "tok_incomplete",
    "Good": "tok_good",
    "No Good": "tok_no_good",
    "Fumble": "tok_fumble",
    "Penalty": "tok_penalty",
    "TD": "tok_td",
    "Def TD": "tok_def_td",
    "Safety": "tok_safety",
}


def parse_result_tokens(df: pl.DataFrame) -> pl.DataFrame:
    """Split RESULT on ", " and derive one boolean tok_* column per contract token.

    Exact, case-sensitive membership only. A null or empty RESULT yields an
    empty token list (every tok_* column False, tok_unknown empty), not a
    list containing a single empty string. `tok_unknown` lists tokens outside
    the 13-token contract vocabulary, for the caller to fold into notices.
    """
    df = df.with_columns(
        pl.col("RESULT")
        .fill_null("")
        .str.split(", ")
        .list.eval(pl.element().filter(pl.element() != ""))
        .alias("_result_tokens")
    )

    flag_exprs = [
        pl.col("_result_tokens").list.contains(token).alias(col)
        for token, col in _TOKEN_COLUMN.items()
    ]
    unknown_expr = (
        pl.col("_result_tokens")
        .list.eval(pl.element().filter(~pl.element().is_in(_ALL_TOKENS)))
        .alias("tok_unknown")
    )

    df = df.with_columns(flag_exprs + [unknown_expr])
    df = df.drop("_result_tokens")
    return df


def derive_outcome_columns(df: pl.DataFrame) -> tuple[pl.DataFrame, list[str]]:
    """Derive the canonical play-outcome columns from the RESULT token flags.

    Requires `parse_result_tokens` to have already run (needs the tok_*
    columns) and a `down` column (needed for PAT semantics) and a
    `yardline_50` column (needed for the 1pt/2pt conversion spots) to already
    be present. Keeps the notebook's column names and point semantics from
    `Python/helper_add_hudl_mutations.py`; only the RESULT-matching predicate
    changes, from substring to exact-token.

    Returns `(df, messages)` -- messages record unknown RESULT tokens and
    empty-RESULT-on-non-PAT rows, never raised.
    """
    messages: list[str] = []

    unknown_tokens = sorted(
        {
            token
            for token in df["tok_unknown"].explode().drop_nulls().to_list()
            if token
        }
    )
    if unknown_tokens:
        messages.append(f"unknown RESULT token(s) recorded and ignored: {unknown_tokens}")

    empty_non_pat = df.filter(
        (pl.col("RESULT").fill_null("") == "") & (pl.col("down") != 0)
    ).height
    if empty_non_pat:
        messages.append(
            f"empty RESULT on {empty_non_pat} non-PAT play(s) (down != 0): "
            "play_type set to null instead of the legacy silent 'pass' default"
        )

    df = df.with_columns(
        pl.when(pl.col("tok_rush")).then(pl.lit("run"))
        .when(pl.col("tok_penalty")).then(pl.lit("no_play"))
        .when(pl.col("tok_kneel")).then(pl.lit("qb_kneel"))
        .when(pl.col("down") == 0).then(pl.lit("extra_point"))
        .when(pl.col("RESULT").fill_null("") == "").then(pl.lit(None, dtype=pl.Utf8))
        .otherwise(pl.lit("pass"))
        .alias("play_type")
    )

    df = df.with_columns(
        pl.col("tok_sack").cast(pl.Int32).alias("sack"),
        pl.col("tok_interception").cast(pl.Int32).alias("interception"),
        pl.col("tok_complete").cast(pl.Int32).alias("complete_pass"),
        pl.col("tok_incomplete").cast(pl.Int32).alias("incomplete_pass"),
        (pl.col("tok_td") & ~pl.col("tok_def_td")).cast(pl.Int32).alias("touchdown"),
        pl.col("tok_def_td").cast(pl.Int32).alias("def_touchdown"),
        pl.col("tok_penalty").cast(pl.Int32).alias("penalty"),
        pl.col("tok_safety").cast(pl.Int32).alias("safety"),
        pl.col("tok_no_good").cast(pl.Int32).alias("no_good"),
        # Fumble working semantics: charted ball-loss tag only. Possession-change
        # semantics is a DEFERRED-ANALYST ratification item (see
        # docs/data-contract.md deferred_ratification / docs/data-contract.schema.json
        # deferred_ratification.items) -- drive_id's turnover-closing logic treats
        # `fumble` as a drive-closer but this column itself makes no possession claim.
        pl.col("tok_fumble").cast(pl.Int32).alias("fumble"),
    )

    df = df.with_columns(
        (pl.col("tok_good") & (pl.col("down") == 0) & (pl.col("yardline_50") == 45))
        .cast(pl.Int32)
        .alias("one_point_conv_success"),
        (pl.col("tok_good") & (pl.col("down") == 0) & (pl.col("yardline_50") == 40))
        .cast(pl.Int32)
        .alias("two_point_conv_success"),
        (pl.col("tok_def_td") & (pl.col("down") == 0))
        .cast(pl.Int32)
        .alias("defensive_two_point_conv"),
    )

    return df, messages


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
