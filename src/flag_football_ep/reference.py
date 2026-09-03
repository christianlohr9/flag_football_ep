"""Reference-data loaders and cross-source team-identity mapping.

Every reference file is a plain, minimal-column CSV under `data/reference/`,
following `half_boundaries.csv`'s style: comma-delimited, no metadata rows.
Each loader declares an explicit `schema_overrides` so a hand-edit that types
a score as text still lands as the declared dtype or fails loudly, and a
header-only file loads as an empty typed frame with a warning rather than
raising.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import polars as pl


class MissingReferenceFile(Exception):
    """Raised when a reference CSV does not exist at the given path."""


class UnmappedTeamError(Exception):
    """Raised when one or more team labels have no entry in the team mapping."""


class UnmappedCompetitionError(Exception):
    """Raised when one or more (source, competition) pairs have no entry in the
    competition-tier mapping.
    """


COMPETITION_TIERS: tuple[str, ...] = ("womens-international", "womens-national", "mixed-other")


_HALF_BOUNDARIES_SCHEMA: dict[str, pl.DataType] = {
    "filename": pl.Utf8,
    "half2_first_play": pl.Int32,
}
_FINAL_SCORES_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "home_score": pl.Int32,
    "away_score": pl.Int32,
    "note": pl.Utf8,
}
_TEAM_MAPPING_SCHEMA: dict[str, pl.DataType] = {
    "source": pl.Utf8,
    "source_team": pl.Utf8,
    "canonical_team": pl.Utf8,
}
_SPORTAPP_GAMES_SCHEMA: dict[str, pl.DataType] = {
    "source_game_id": pl.Utf8,
    "competition": pl.Utf8,
    "season": pl.Utf8,
    "note": pl.Utf8,
}
_COMPETITION_TIER_SCHEMA: dict[str, pl.DataType] = {
    "source": pl.Utf8,
    "competition": pl.Utf8,
    "tier": pl.Utf8,
}
_PLAYER_MAPPING_SCHEMA: dict[str, pl.DataType] = {
    "source": pl.Utf8,
    "source_player": pl.Utf8,
    "canonical_player": pl.Utf8,
}
_GROUP_OPPONENTS_SCHEMA: dict[str, pl.DataType] = {
    "canonical_team": pl.Utf8,
    "team_name": pl.Utf8,
}
_HC_GAMES_SCHEMA: dict[str, pl.DataType] = {
    "workbook": pl.Utf8,
    "sheet": pl.Utf8,
    "block_key": pl.Utf8,
    "source_team1": pl.Utf8,
    "source_team2": pl.Utf8,
    "game_id": pl.Utf8,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "competition": pl.Utf8,
    "season": pl.Int32,
    "game_date": pl.Utf8,
    "tier": pl.Utf8,
    "corpus_game_id": pl.Utf8,
    "note": pl.Utf8,
}


def _read_reference_csv(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    if not path.exists():
        raise MissingReferenceFile(f"reference file not found: {path}")

    df = pl.read_csv(path, schema_overrides=schema)

    if df.height == 0:
        warnings.warn(
            f"{path} is header-only; loading as an empty typed frame",
            stacklevel=3,
        )

    return df


def load_half_boundaries(path: Path) -> pl.DataFrame:
    """Load `filename,half2_first_play`. Rejects `half2_first_play < 2`."""
    df = _read_reference_csv(path, _HALF_BOUNDARIES_SCHEMA)

    if df.height:
        bad = df.filter(pl.col("half2_first_play") < 2)
        if bad.height:
            raise ValueError(
                "half2_first_play < 2 (play 1 cannot start the second half) for: "
                f"{bad['filename'].to_list()} in {path}"
            )

    return df


def load_final_scores(path: Path) -> pl.DataFrame:
    """Load `game_id,home_team,away_team,home_score,away_score,note`."""
    df = _read_reference_csv(path, _FINAL_SCORES_SCHEMA)

    if df.height:
        dupes = (
            df.group_by("game_id")
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") > 1)["game_id"]
            .to_list()
        )
        if dupes:
            raise ValueError(f"duplicate game_id in {path}: {dupes}")

    return df


def load_team_mapping(path: Path) -> pl.DataFrame:
    """Load `source,source_team,canonical_team`. Rejects duplicate (source, source_team)."""
    df = _read_reference_csv(path, _TEAM_MAPPING_SCHEMA)

    if df.height:
        dupes = (
            df.group_by(["source", "source_team"])
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") > 1)
            .select(["source", "source_team"])
            .rows()
        )
        if dupes:
            raise ValueError(f"duplicate (source, source_team) pair(s) in {path}: {dupes}")

    return df


def load_player_mapping(path: Path) -> pl.DataFrame:
    """Load `source,source_player,canonical_player`. Rejects duplicate (source, source_player).

    Deliberate divergence from a `source,source_name_or_jersey,canonical_player_id` schema:
    `canonical_player` is the display name the report prints, not an opaque id into
    `data/reference/roster.csv` (which has no `source` column and is an unverified
    single-source dump).
    """
    df = _read_reference_csv(path, _PLAYER_MAPPING_SCHEMA)

    if df.height:
        dupes = (
            df.group_by(["source", "source_player"])
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") > 1)
            .select(["source", "source_player"])
            .rows()
        )
        if dupes:
            raise ValueError(f"duplicate (source, source_player) pair(s) in {path}: {dupes}")

    return df


def load_group_opponents(path: Path) -> pl.DataFrame:
    """Load `canonical_team,team_name`. Rejects duplicate `canonical_team` values."""
    df = _read_reference_csv(path, _GROUP_OPPONENTS_SCHEMA)

    if df.height:
        dupes = (
            df.group_by("canonical_team")
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") > 1)["canonical_team"]
            .to_list()
        )
        if dupes:
            raise ValueError(f"duplicate canonical_team in {path}: {dupes}")

    return df


def load_hc_games(path: Path) -> pl.DataFrame:
    """Load the maintained head-coach game-identity mapping.

    `workbook,sheet,block_key,source_team1,source_team2,game_id,home_team,
    away_team,competition,season,game_date,tier,corpus_game_id,note`.
    `workbook`/`sheet` are the slugified file stem/sheet name
    (`hc_workbook.slugify`); `block_key` is `b{block_index:02d}-g{game_index:02d}`
    as produced by `hc_workbook.segment_games`, so `(workbook, sheet, block_key)`
    is the lookup key a fresh ingest run resolves against. `source_team1`/
    `source_team2` are the raw labels as charted (empty for a numeric block) --
    present only so a maintainer can recognise the row, never used for
    matching. `game_id` is the canonical id, always prefixed `hc-` to stay
    distinguishable from `hudl`/`ifaf`/`legacy` ids in `plays.parquet`.
    `corpus_game_id` names an already-ingested (or another HC) game this block
    duplicates -- empty when the block is a genuinely new game; plan
    M3-01-04's dedupe stage reads it. `note` is free text for the maintainer.

    Rejects, each naming the offending value(s):
    - a duplicate `(workbook, sheet, block_key)` triple,
    - a duplicate `game_id`,
    - a `tier` outside `COMPETITION_TIERS`,
    - a `game_id` that does not start with `hc-`.
    """
    df = _read_reference_csv(path, _HC_GAMES_SCHEMA)

    if df.height:
        dupe_keys = (
            df.group_by(["workbook", "sheet", "block_key"])
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") > 1)
            .select(["workbook", "sheet", "block_key"])
            .rows()
        )
        if dupe_keys:
            raise ValueError(
                f"duplicate (workbook, sheet, block_key) triple(s) in {path}: {dupe_keys}"
            )

        dupe_ids = (
            df.group_by("game_id")
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") > 1)["game_id"]
            .to_list()
        )
        if dupe_ids:
            raise ValueError(f"duplicate game_id in {path}: {dupe_ids}")

        bad_tiers = (
            df.filter(~pl.col("tier").is_in(COMPETITION_TIERS))["tier"].unique().to_list()
        )
        if bad_tiers:
            raise ValueError(
                f"tier value(s) not in COMPETITION_TIERS {COMPETITION_TIERS} in {path}: "
                f"{sorted(bad_tiers)}"
            )

        bad_ids = (
            df.filter(~pl.col("game_id").str.starts_with("hc-"))["game_id"].to_list()
        )
        if bad_ids:
            raise ValueError(f"game_id value(s) not prefixed 'hc-' in {path}: {bad_ids}")

    return df


def load_sportapp_games(path: Path) -> pl.DataFrame:
    """Load `source_game_id,competition,season,note`."""
    return _read_reference_csv(path, _SPORTAPP_GAMES_SCHEMA)


def load_competition_tier(path: Path) -> pl.DataFrame:
    """Load `source,competition,tier`.

    Rejects duplicate (source, competition) pairs and any `tier` value outside
    `COMPETITION_TIERS`, naming the offending pairs/values in either case.
    """
    df = _read_reference_csv(path, _COMPETITION_TIER_SCHEMA)

    if df.height:
        dupes = (
            df.group_by(["source", "competition"])
            .agg(pl.len().alias("n"))
            .filter(pl.col("n") > 1)
            .select(["source", "competition"])
            .rows()
        )
        if dupes:
            raise ValueError(f"duplicate (source, competition) pair(s) in {path}: {dupes}")

        bad_tiers = (
            df.filter(~pl.col("tier").is_in(COMPETITION_TIERS))["tier"].unique().to_list()
        )
        if bad_tiers:
            raise ValueError(
                f"tier value(s) not in COMPETITION_TIERS {COMPETITION_TIERS} in {path}: "
                f"{sorted(bad_tiers)}"
            )

    return df


def map_teams(
    df: pl.DataFrame, mapping: pl.DataFrame, source: str, columns: Sequence[str]
) -> pl.DataFrame:
    """Replace every listed column's values with the canonical team code.

    Raises `UnmappedTeamError` listing every distinct unmapped label, its
    source and the affected column names, rather than ever letting an
    unmapped label pass through silently (including when `mapping` is empty).
    """
    source_map = mapping.filter(pl.col("source") == source)
    lookup = dict(
        zip(source_map["source_team"].to_list(), source_map["canonical_team"].to_list())
    )

    unmapped: dict[str, set[str]] = {}
    for col in columns:
        if col not in df.columns:
            continue
        for label in df[col].drop_nulls().unique().to_list():
            if label not in lookup:
                unmapped.setdefault(label, set()).add(col)

    if unmapped:
        details = "; ".join(
            f"{label!r} (source={source!r}, columns={sorted(cols)})"
            for label, cols in sorted(unmapped.items())
        )
        raise UnmappedTeamError(f"unmapped team label(s): {details}")

    result = df
    for col in columns:
        if col not in result.columns:
            continue
        result = result.with_columns(pl.col(col).replace(lookup).alias(col))

    return result


@dataclass(frozen=True)
class PlayerMappingResult:
    """Result of `map_players`: the mapped frame plus every label left unmapped."""

    frame: pl.DataFrame
    unmapped: list[str]


def map_players(
    df: pl.DataFrame, mapping: pl.DataFrame, source: str, columns: Sequence[str]
) -> PlayerMappingResult:
    """Replace every listed column's values with the canonical player name.

    Deliberate divergence from `map_teams`/`map_competition_tier`'s raise-on-unmapped
    precedent: CONTEXT.md locks "the report never breaks on an unmapped name" -- an
    unmapped label is left as-is in the output column (never raised, never dropped, never
    nulled) and the caller renders `unmapped` as a prominent warning block.
    """
    source_map = mapping.filter(pl.col("source") == source)
    lookup = dict(
        zip(source_map["source_player"].to_list(), source_map["canonical_player"].to_list())
    )

    unmapped: set[str] = set()
    for col in columns:
        if col not in df.columns:
            continue
        for label in df[col].drop_nulls().unique().to_list():
            if label not in lookup:
                unmapped.add(label)

    result = df
    for col in columns:
        if col not in result.columns:
            continue
        result = result.with_columns(pl.col(col).replace(lookup).alias(col))

    return PlayerMappingResult(frame=result, unmapped=sorted(unmapped))


def map_competition_tier(df: pl.DataFrame, mapping: pl.DataFrame) -> pl.DataFrame:
    """Add a `competition_tier` column from the (source, competition) pair.

    Left-joins `mapping` onto `df` on `["source", "competition"]`, preserving
    row count and row order (join, never filter or sort). Raises
    `UnmappedCompetitionError` listing every distinct unmapped (source,
    competition) pair rather than ever letting a null `competition_tier` pass
    through silently (including when `mapping` is empty).
    """
    typed_mapping = mapping.select(
        pl.col("source").cast(pl.Utf8),
        pl.col("competition").cast(pl.Utf8),
        pl.col("tier").cast(pl.Utf8),
    )
    joined = df.with_row_index("__row_index__").join(
        typed_mapping,
        on=["source", "competition"],
        how="left",
    )

    unmapped = (
        joined.filter(pl.col("tier").is_null())
        .select(["source", "competition"])
        .unique()
        .rows()
    )
    if unmapped:
        details = ", ".join(
            f"(source={source!r}, competition={competition!r})"
            for source, competition in sorted(unmapped)
        )
        raise UnmappedCompetitionError(f"unmapped (source, competition) pair(s): {details}")

    result = (
        joined.sort("__row_index__")
        .drop("__row_index__")
        .rename({"tier": "competition_tier"})
    )
    return result
