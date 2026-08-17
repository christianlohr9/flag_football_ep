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
from pathlib import Path
from typing import Sequence

import polars as pl


class MissingReferenceFile(Exception):
    """Raised when a reference CSV does not exist at the given path."""


class UnmappedTeamError(Exception):
    """Raised when one or more team labels have no entry in the team mapping."""


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


def load_sportapp_games(path: Path) -> pl.DataFrame:
    """Load `source_game_id,competition,season,note`."""
    return _read_reference_csv(path, _SPORTAPP_GAMES_SCHEMA)


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
