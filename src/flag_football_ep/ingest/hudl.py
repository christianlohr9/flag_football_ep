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

from flag_football_ep.canonical import (
    ConformReport,
    MissingCanonicalColumns,
    add_score_columns,
    add_scoring_play_team,
    conform_to_canonical,
)
from flag_football_ep.reference import map_teams
from flag_football_ep.validation.schema import (
    Contract,
    DomainViolation,
    HeaderReport,
    MissingCoreColumnsError,
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
    "derive_posteam_defteam",
    "derive_identity_columns",
    "derive_drive_id",
    "derive_half",
    "derive_extras",
    "derive_yards_gained_first_down",
    "ingest_file",
    "ingest_dir",
]

# Rich charting columns mapped onto the canonical extras (canonical.NULLABLE_EXTRAS)
# when the source column is present. QB/THROWN BY/RECEIVED BY/TARGET/TACKLE/
# TARGET ROUTE/YAC are not in the contract's core/optional list -- they are
# "unknown" per the contract's unknown_rule (ignored with a log notice, not a
# violation) but still carry real charting value, so ingest renames them onto
# extras when present rather than dropping them.
_CHARTING_RENAME = {
    "OFF FORM": "off_form",
    "OFF PLAY": "off_play",
    "OFF STR": "off_str",
    "PLAY DIR": "play_dir",
    "GAP": "gap",
    "PASS ZONE": "pass_zone",
    "TARGET ROUTE": "target_route",
    "HASH": "hash_mark",
    "DEF FRONT": "def_front",
    "COVERAGE": "coverage",
    "BLITZ": "blitz",
    "QB": "qb",
    "THROWN BY": "thrown_by",
    "RECEIVED BY": "received_by",
    "TARGET": "target",
    "TACKLE": "tackle",
    "YAC": "yac",
}


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
#
# v1.2 amendment (2026-09-03): six base tokens added -- Block, Blocked, Batted
# Down, Dropped, Timeout, Offsetting Penalties. Source: the head coach's three
# charting workbooks (docs/hc-notes-2026-09-03.md), never seen in a Hudl
# export. User approval 2026-09-03 ("Jona ist HC, er schlägt alles"); the
# semantics implemented below are our proposal, confirmation still pending
# (docs/hc-rueckfragen-2026-09.md Frage 3). `Blocked` only ever occurs inside
# "Blocked, Def TD" -- it is a spelling variant of `Block`, not a distinct
# outcome.
_BASE_TOKENS = (
    "Rush", "KNEEL", "Sack", "Interception", "Complete", "Incomplete",
    "Good", "No Good", "Fumble", "Penalty",
    "Block", "Blocked", "Batted Down", "Dropped", "Timeout", "Offsetting Penalties",
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
    "Block": "tok_block",
    "Blocked": "tok_blocked",
    "Batted Down": "tok_batted_down",
    "Dropped": "tok_dropped",
    "Timeout": "tok_timeout",
    "Offsetting Penalties": "tok_offsetting_penalties",
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

    # v1.2 amendment locals (docs/hc-rueckfragen-2026-09.md Frage 3, semantics
    # pending head-coach confirmation): Timeout and Offsetting Penalties are
    # non-plays like Penalty, so they never carry EP/WP training weight as if
    # they were plays; Block/Blocked/Batted Down/Dropped are pass attempts
    # without a completion (deflected or dropped, readable from result_raw).
    no_play_token = pl.col("tok_penalty") | pl.col("tok_timeout") | pl.col("tok_offsetting_penalties")
    deflected_or_dropped = (
        pl.col("tok_block") | pl.col("tok_blocked") | pl.col("tok_batted_down") | pl.col("tok_dropped")
    )

    df = df.with_columns(
        # tok_penalty (now no_play_token, which also covers Timeout and
        # Offsetting Penalties) is checked FIRST: a flagged play is a no-play
        # regardless of its base token, so "Rush, Penalty" and "Complete,
        # Penalty" both map to no_play (previously "Rush, Penalty" leaked
        # through as run — REVIEW WR-03). The base outcome survives in the
        # penalty/complete_pass/... flag columns derived below.
        pl.when(no_play_token).then(pl.lit("no_play"))
        .when(pl.col("tok_rush")).then(pl.lit("run"))
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
        (pl.col("tok_incomplete") | deflected_or_dropped).cast(pl.Int32).alias("incomplete_pass"),
        (pl.col("tok_td") & ~pl.col("tok_def_td")).cast(pl.Int32).alias("touchdown"),
        pl.col("tok_def_td").cast(pl.Int32).alias("def_touchdown"),
        (pl.col("tok_penalty") | pl.col("tok_offsetting_penalties")).cast(pl.Int32).alias("penalty"),
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


def derive_extras(df: pl.DataFrame, meta: GameMeta) -> pl.DataFrame:
    """Stamp constant per-game columns and rename rich charting columns onto
    their canonical extras (canonical.NULLABLE_EXTRAS) when present.
    """
    df = df.with_columns(
        pl.lit("hudl").alias("source"),
        pl.lit(meta.competition).alias("competition"),
        pl.lit(meta.season).cast(pl.Int32).alias("season"),
        pl.lit(meta.game_id).alias("game_id"),
        pl.lit(meta.game_date).alias("game_date"),
        pl.col("RESULT").alias("result_raw"),
    )

    present = {src: dst for src, dst in _CHARTING_RENAME.items() if src in df.columns}
    if present:
        df = df.rename(present)

    return df


def derive_posteam_defteam(df: pl.DataFrame, meta: GameMeta) -> pl.DataFrame:
    """posteam = TEAM1 when ODK == 'O', else TEAM2 (docs/data-contract.schema.json
    file_format.filename_semantics); defteam is the complement. ODK 'K' rows
    follow the same else-branch (posteam = TEAM2) and get `play_type` "kickoff"
    (applied by the caller once outcome derivation has set a baseline play_type).

    home_team = TEAM1, away_team = TEAM2 (filename order). This replaces the
    legacy helper's "team with the first drive is home" heuristic, which
    stays only in the legacy path (`ingest/legacy.py`) where TEAM1/TEAM2
    aren't available from a filename. Home/away here only affects score
    bookkeeping -- `data/reference/final_scores.csv` also carries home/away
    team codes, so a convention mismatch is caught there, not silently.
    """
    df = df.with_columns(
        pl.when(pl.col("ODK") == "O").then(pl.lit(meta.team1)).otherwise(pl.lit(meta.team2)).alias("posteam"),
        pl.lit(meta.team1).alias("home_team"),
        pl.lit(meta.team2).alias("away_team"),
    )
    df = df.with_columns(
        pl.when(pl.col("posteam") == pl.lit(meta.team1))
        .then(pl.lit(meta.team2))
        .otherwise(pl.lit(meta.team1))
        .alias("defteam"),
    )
    return df


def derive_identity_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Cast core numeric columns, derive yardline_50 and its simplified bins,
    and the within-game next-play shifts (yardline_50_after, posteam_after).

    Ported from `Python/helper_add_hudl_mutations.py` lines 28-47 verbatim
    except the yardline_50 rule, which now follows the contract exactly
    (`-x if x < 0 else 50 - x`) instead of relying on a pre-computed column.
    Requires `game_id` (derive_extras) and `posteam` (derive_posteam_defteam)
    to already be present.

    All four casts are `strict=False`: a non-numeric DN/DIST/YARD LN/PLAY #
    cell becomes a null in the derived column instead of raising, matching
    the module docstring's "everything data-quality related is collected
    into `IngestNotices`, never raised" contract -- `check_column_domains`
    already documents and reports exactly these cells as notice-worthy, so a
    strict cast here would crash on a finding this module promises to
    downgrade to a notice.
    """
    df = df.with_columns(
        pl.col("DN").cast(pl.Int32, strict=False).alias("down"),
        pl.col("DIST").cast(pl.Int32, strict=False).alias("yards_to_go"),
        pl.col("YARD LN").cast(pl.Int32, strict=False).alias("yardline"),
        pl.col("PLAY #").cast(pl.Int32, strict=False).alias("play_id"),
    )
    df = df.with_columns(
        pl.when(pl.col("yardline") < 0)
        .then(-pl.col("yardline"))
        .otherwise(50 - pl.col("yardline"))
        .alias("yardline_50")
    )
    df = df.with_columns(
        pl.when(pl.col("yardline_50") < 25).then(pl.lit(0)).otherwise(pl.lit(1)).alias(
            "yardline_50_simple"
        ),
        pl.when(pl.col("yards_to_go") <= 5).then(pl.lit(1))
        .when((pl.col("yards_to_go") > 5) & (pl.col("yards_to_go") <= 10)).then(pl.lit(2))
        .when((pl.col("yards_to_go") > 10) & (pl.col("yards_to_go") <= 15)).then(pl.lit(3))
        .when((pl.col("yards_to_go") > 15) & (pl.col("yards_to_go") <= 20)).then(pl.lit(4))
        .when(pl.col("yards_to_go") > 20).then(pl.lit(5))
        .otherwise(pl.lit(0))
        .alias("yards_to_go_simple"),
    )
    df = df.sort(["game_id", "play_id"])
    df = df.with_columns(
        pl.col("yardline_50").shift(-1).over("game_id").alias("yardline_50_after"),
        pl.col("posteam").shift(-1).over("game_id").alias("posteam_after"),
    )
    return df


def derive_drive_id(df: pl.DataFrame) -> pl.DataFrame:
    """drive_id starts at 1 and increments on an ODK O/D possession flip, or
    after a drive-closing RESULT (tok_td, tok_def_td, tok_safety,
    tok_interception, tok_fumble) even without an ODK flip.

    Requires `parse_result_tokens` to have already run. Sorts by
    (game_id, play_id) before the cumulative window.
    """
    df = df.sort(["game_id", "play_id"])

    closing = (
        pl.col("tok_td")
        | pl.col("tok_def_td")
        | pl.col("tok_safety")
        | pl.col("tok_interception")
        | pl.col("tok_fumble")
    )
    df = df.with_columns(
        (pl.col("ODK") != pl.col("ODK").shift(1)).over("game_id").fill_null(False).alias(
            "_odk_flip"
        ),
        closing.shift(1).over("game_id").fill_null(False).alias("_closed_prev"),
    )
    df = df.with_columns((pl.col("_odk_flip") | pl.col("_closed_prev")).alias("_drive_boundary"))
    df = df.with_columns(
        (pl.col("_drive_boundary").cum_sum().over("game_id") + 1).cast(pl.Int32).alias("drive_id")
    )
    # Defensive: force the first play of every game to drive_id 1 (already
    # guaranteed by the null-filled flip/closing flags above, but explicit
    # per the contract's derivation rule).
    df = df.with_columns(
        pl.when(pl.col("play_id") == pl.col("play_id").min().over("game_id"))
        .then(pl.lit(1).cast(pl.Int32))
        .otherwise(pl.col("drive_id"))
        .alias("drive_id")
    )
    df = df.drop(["_odk_flip", "_closed_prev", "_drive_boundary"])
    return df


def derive_half(
    df: pl.DataFrame, meta: GameMeta, half_boundaries: pl.DataFrame
) -> tuple[pl.DataFrame, list[str]]:
    """half = 1 if play_id < half2_first_play else 2, keyed on the export
    filename (data/half_boundaries.csv style: `filename,half2_first_play`).

    A game with no matching row gets a null `half` and the notice
    "no half boundary" instead of raising.
    """
    messages: list[str] = []

    boundary_row = half_boundaries.filter(pl.col("filename") == meta.filename)
    if boundary_row.height == 0:
        df = df.with_columns(pl.lit(None, dtype=pl.Int32).alias("half"))
        messages.append(f"no half boundary for {meta.filename!r}: half set to null")
        return df, messages

    boundary = boundary_row["half2_first_play"][0]
    df = df.with_columns(
        pl.when(pl.col("play_id") < boundary)
        .then(pl.lit(1))
        .otherwise(pl.lit(2))
        .cast(pl.Int32)
        .alias("half")
    )
    return df, messages


def derive_yards_gained_first_down(df: pl.DataFrame) -> pl.DataFrame:
    """Port yards_gained/first_down verbatim from
    `Python/helper_add_hudl_mutations.py` lines 167-179.
    """
    df = df.with_columns(
        yards_gained=(
            pl.when(down=0).then(pl.lit(0))
            .when(down=4, complete_pass=0).then(pl.lit(0))
            .when(down=4, safety=1).then(pl.lit(0))
            .otherwise(pl.col("yardline_50_after") - pl.col("yardline_50"))
        )
    )
    df = df.with_columns(
        pl.when((pl.col("yardline_50") < 25) & (pl.col("yards_gained") > pl.col("yards_to_go")))
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("first_down")
    )
    return df


def ingest_file(
    path: Path,
    contract: Contract,
    half_boundaries: pl.DataFrame | None = None,
    team_mapping: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, IngestNotices]:
    """Parse, validate and conform one Hudl export to the canonical schema.

    Order of operations: parse_filename -> read_export -> validate_header ->
    check_column_domains -> constant/extras columns -> posteam/defteam ->
    team mapping -> identity casts + yardline_50 -> RESULT tokens -> outcome
    derivation -> ODK 'K' kickoff override -> drive_id -> half -> scoring
    chain -> yards_gained/first_down -> conform_to_canonical. Structural
    problems (bad filename, missing core column, unmapped team) raise;
    everything else is only ever recorded in the returned `IngestNotices`.

    `no_good`, `incomplete_pass` and `fumble` are computed but have no
    canonical column (core or extra) -- `conform_to_canonical` drops them as
    unknown columns; see `ConformReport.dropped_unknown` and the plan
    SUMMARY for the schema-owner follow-up.
    """
    meta = parse_filename(path)
    df = read_export(path)
    df, header_report = validate_header(df, contract)
    domain_violations = check_column_domains(df, contract)

    messages: list[str] = []

    df = derive_extras(df, meta)
    df = derive_posteam_defteam(df, meta)

    if team_mapping is not None:
        df = map_teams(df, team_mapping, "hudl", ["posteam", "defteam", "home_team", "away_team"])

    df = derive_identity_columns(df)

    df = parse_result_tokens(df)
    df, outcome_messages = derive_outcome_columns(df)
    messages.extend(outcome_messages)

    # ODK == 'K' overrides play_type to "kickoff" regardless of RESULT tokens.
    df = df.with_columns(
        pl.when(pl.col("ODK") == "K").then(pl.lit("kickoff")).otherwise(pl.col("play_type")).alias(
            "play_type"
        )
    )

    df = derive_drive_id(df)

    if half_boundaries is not None:
        df, half_messages = derive_half(df, meta, half_boundaries)
        messages.extend(half_messages)
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Int32).alias("half"))
        messages.append("no half_boundaries provided: half set to null")

    df = add_scoring_play_team(df, credit_defense=True)
    df = add_score_columns(df)

    df = derive_yards_gained_first_down(df)

    df, conform_report = conform_to_canonical(df, "hudl")

    notices = IngestNotices(
        game_id=meta.game_id,
        header=header_report,
        domain=domain_violations,
        conform=conform_report,
        messages=messages,
    )
    return df, notices


def ingest_dir(
    directory: Path,
    contract: Contract,
    half_boundaries: pl.DataFrame | None = None,
    team_mapping: pl.DataFrame | None = None,
) -> list[tuple[GameMeta, pl.DataFrame | None, IngestNotices]]:
    """Ingest every `*.csv` in `directory` (sorted), one game per entry.

    Catches per-file structural errors -- bad filename (`FilenameError`),
    missing core columns (`MissingCoreColumnsError`), wrong delimiter
    (`WrongDelimiterError`), a canonical-conform failure
    (`MissingCanonicalColumns`), or any other polars error surfaced by the
    derivation chain (`pl.exceptions.PolarsError`) -- and reports them via
    `messages` with `df=None` rather than aborting the whole directory; one
    broken export must not block the rest.
    """
    results: list[tuple[GameMeta, pl.DataFrame | None, IngestNotices]] = []

    for path in sorted(directory.glob("*.csv")):
        try:
            meta = parse_filename(path)
        except FilenameError as exc:
            placeholder_meta = GameMeta(
                game_id=path.stem,
                filename=path.name,
                season=0,
                game_date=None,
                team1="",
                team2="",
                competition="",
            )
            notices = IngestNotices(
                game_id=path.stem,
                header=HeaderReport([], [], []),
                domain=[],
                conform=ConformReport(),
                messages=[f"skipped {path.name!r}: {exc}"],
            )
            results.append((placeholder_meta, None, notices))
            continue

        try:
            df, notices = ingest_file(path, contract, half_boundaries, team_mapping)
        # UnmappedTeamError is deliberately NOT caught here: CONTEXT.md's team-identity
        # decision (T-1.2-15) requires an unmapped team to abort loudly, not degrade
        # into a per-file notice -- it signals a reference-data gap needing a human
        # fix, not a per-file data-quality issue. PolarsError is included as defense
        # in depth for any remaining strict cast or schema error surfaced by the
        # derivation chain; UnmappedTeamError is a plain Exception, not a PolarsError,
        # so this catch never touches it.
        except (
            MissingCoreColumnsError,
            WrongDelimiterError,
            MissingCanonicalColumns,
            pl.exceptions.PolarsError,
        ) as exc:
            notices = IngestNotices(
                game_id=meta.game_id,
                header=HeaderReport([], [], []),
                domain=[],
                conform=ConformReport(),
                messages=[f"skipped {path.name!r}: {type(exc).__name__}: {exc}"],
            )
            results.append((meta, None, notices))
            continue

        results.append((meta, df, notices))

    return results
