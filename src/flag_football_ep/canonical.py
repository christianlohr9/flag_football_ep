"""The canonical plays schema and the shared derivations all ingest paths converge on.

Every ingest module (hudl, legacy, sportapp, ifaf) parses its own source-specific
result vocabulary, then calls `conform_to_canonical` as the single convergence
point onto this schema. Column names and dtypes here are the existing project
vocabulary from `Python/helper_add_hudl_mutations.py` and `data_raw.csv` — do not
rename them without updating every ingest module and `docs/data-contract.schema.json`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

# Required in every source frame after conform_to_canonical.
CORE_COLUMNS: dict[str, pl.DataType] = {
    # Utf8
    "source": pl.Utf8,
    "competition": pl.Utf8,
    "game_id": pl.Utf8,
    "posteam": pl.Utf8,
    "defteam": pl.Utf8,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "posteam_after": pl.Utf8,
    "play_type": pl.Utf8,
    "result_raw": pl.Utf8,
    "scoring_play_team": pl.Utf8,
    # Int32
    "season": pl.Int32,
    "play_id": pl.Int32,
    "drive_id": pl.Int32,
    "half": pl.Int32,
    "down": pl.Int32,
    "yards_to_go": pl.Int32,
    "yardline": pl.Int32,
    "yardline_50": pl.Int32,
    "yardline_50_after": pl.Int32,
    "yardline_50_simple": pl.Int32,
    "yards_to_go_simple": pl.Int32,
    "sack": pl.Int32,
    "interception": pl.Int32,
    "complete_pass": pl.Int32,
    "touchdown": pl.Int32,
    "def_touchdown": pl.Int32,
    "penalty": pl.Int32,
    "safety": pl.Int32,
    "one_point_conv_success": pl.Int32,
    "two_point_conv_success": pl.Int32,
    "defensive_two_point_conv": pl.Int32,
    "scoring_play": pl.Int32,
    "home_team_points": pl.Int32,
    "away_team_points": pl.Int32,
    "home_team_score": pl.Int32,
    "away_team_score": pl.Int32,
    "posteam_score": pl.Int32,
    "defteam_score": pl.Int32,
    "score_differential": pl.Int32,
    "yards_gained": pl.Int32,
    "first_down": pl.Int32,
}

# Materialized as typed nulls when a source lacks them.
NULLABLE_EXTRAS: dict[str, pl.DataType] = {
    # Utf8
    "gender": pl.Utf8,
    "source_game_id": pl.Utf8,
    "game_date": pl.Utf8,
    "off_form": pl.Utf8,
    "off_play": pl.Utf8,
    "off_str": pl.Utf8,
    "play_dir": pl.Utf8,
    "gap": pl.Utf8,
    "pass_zone": pl.Utf8,
    "target_route": pl.Utf8,
    "hash_mark": pl.Utf8,
    "def_front": pl.Utf8,
    "coverage": pl.Utf8,
    "blitz": pl.Utf8,
    "qb": pl.Utf8,
    "thrown_by": pl.Utf8,
    "received_by": pl.Utf8,
    "target": pl.Utf8,
    "tackle": pl.Utf8,
    "description": pl.Utf8,
    "jersey_number": pl.Utf8,
    "player_id": pl.Utf8,
    # Int32
    "yac": pl.Int32,
    "drive_success": pl.Int32,
    # Float64
    "half_seconds_remaining": pl.Float64,
    # Int64
    "game_clock_ms": pl.Int64,
}

# Spec only — enforced by validation in plan 06, never raised inside conform.
NON_NULL_COLUMNS: tuple[str, ...] = (
    "source",
    "competition",
    "season",
    "game_id",
    "play_id",
    "drive_id",
    "half",
    "posteam",
    "defteam",
    "home_team",
    "away_team",
    "down",
    "yards_to_go",
    "yardline_50",
)

# Parquet column order: core columns first, then nullable extras.
CANONICAL_COLUMNS: tuple[str, ...] = tuple(CORE_COLUMNS) + tuple(NULLABLE_EXTRAS)

# All canonical columns (core + extras) mapped to their declared dtype.
_ALL_DTYPES: dict[str, pl.DataType] = {**CORE_COLUMNS, **NULLABLE_EXTRAS}


class MissingCanonicalColumns(Exception):
    """Raised when a source frame is missing one or more CORE_COLUMNS."""


@dataclass
class ConformReport:
    """Machine-readable record of what `conform_to_canonical` had to do."""

    missing_core: list[str] = field(default_factory=list)
    materialized_extras: list[str] = field(default_factory=list)
    dropped_unknown: list[str] = field(default_factory=list)
    cast_failures: dict[str, int] = field(default_factory=dict)


def make_game_id(source: str, key: str | int) -> str:
    """Build a canonical game_id.

    hudl -> the export filename stem verbatim (key is already the stem).
    every other source -> f"{source}-{key}".
    """
    if source == "hudl":
        return str(key)
    return f"{source}-{key}"


def conform_to_canonical(df: pl.DataFrame, source: str) -> tuple[pl.DataFrame, ConformReport]:
    """Conform a source frame onto CANONICAL_COLUMNS, reporting what changed.

    Steps: (1) fail hard if any core column is missing, (2) materialize absent
    extras as typed nulls, (3) cast every canonical column non-strictly,
    counting newly introduced nulls per column, (4) stamp `source`, (5) select
    down to CANONICAL_COLUMNS in order, recording dropped unknown columns.
    """
    report = ConformReport()

    missing_core = [name for name in CORE_COLUMNS if name not in df.columns]
    report.missing_core = missing_core
    if missing_core:
        raise MissingCanonicalColumns(
            f"Missing required core columns: {', '.join(missing_core)}"
        )

    missing_extras = [name for name in NULLABLE_EXTRAS if name not in df.columns]
    if missing_extras:
        df = df.with_columns(
            [
                pl.lit(None).cast(NULLABLE_EXTRAS[name]).alias(name)
                for name in missing_extras
            ]
        )
    report.materialized_extras = missing_extras

    before_null_counts = {name: df[name].null_count() for name in _ALL_DTYPES}
    df = df.with_columns(
        [
            pl.col(name).cast(dtype, strict=False).alias(name)
            for name, dtype in _ALL_DTYPES.items()
        ]
    )
    cast_failures: dict[str, int] = {}
    for name in _ALL_DTYPES:
        introduced = df[name].null_count() - before_null_counts[name]
        if introduced > 0:
            cast_failures[name] = introduced
    report.cast_failures = cast_failures

    df = df.with_columns(pl.lit(source).alias("source"))

    dropped_unknown = [name for name in df.columns if name not in CANONICAL_COLUMNS]
    report.dropped_unknown = dropped_unknown
    df = df.select(list(CANONICAL_COLUMNS))

    return df, report


def add_scoring_play_team(df: pl.DataFrame, credit_defense: bool) -> pl.DataFrame:
    """Derive `scoring_play` and `scoring_play_team`.

    The original notebook chained `flag_a | flag_b | ... == 1` inside a single
    `pl.when`, which Python/polars operator precedence evaluates as
    `flag_a | flag_b | ... | (flag_f == 1)` — only the *last* flag is actually
    compared to 1, so `scoring_play` silently mis-fires whenever an earlier
    flag alone is set. Here every flag is compared to 1 explicitly and the
    booleans are combined with `|`, so `scoring_play` is correct for every
    flag, not only the last one.
    """
    any_scoring_flag = (
        (pl.col("touchdown") == 1)
        | (pl.col("def_touchdown") == 1)
        | (pl.col("one_point_conv_success") == 1)
        | (pl.col("two_point_conv_success") == 1)
        | (pl.col("defensive_two_point_conv") == 1)
        | (pl.col("safety") == 1)
    )
    df = df.with_columns(
        pl.when(any_scoring_flag)
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .cast(pl.Int32)
        .alias("scoring_play")
    )

    offense_scoring = (
        (pl.col("touchdown") == 1)
        | (pl.col("one_point_conv_success") == 1)
        | (pl.col("two_point_conv_success") == 1)
    )

    if credit_defense:
        defense_scoring = (
            (pl.col("def_touchdown") == 1)
            | (pl.col("defensive_two_point_conv") == 1)
            | (pl.col("safety") == 1)
        )
        scoring_team_expr = (
            pl.when((pl.col("scoring_play") == 1) & offense_scoring)
            .then(pl.col("posteam"))
            .when((pl.col("scoring_play") == 1) & defense_scoring)
            .then(pl.col("defteam"))
            .otherwise(pl.lit(None))
        )
    else:
        scoring_team_expr = (
            pl.when((pl.col("scoring_play") == 1) & offense_scoring)
            .then(pl.col("posteam"))
            .otherwise(pl.lit(None))
        )

    df = df.with_columns(scoring_team_expr.alias("scoring_play_team"))
    return df


def add_score_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Port the home/away points -> cum_sum -> forward_fill -> differential chain.

    Sorted explicitly by (game_id, play_id) before the cumulative window so
    ordering does not depend on read order. `home_team_points`/
    `away_team_points` are 0 on play_id 1, the scoring event's point value
    when that team equals `scoring_play_team`, and null otherwise; the
    subsequent `cum_sum().over(["game_id", team])` therefore also emits null
    on non-scoring, non-first rows, and `forward_fill()` carries the last
    known cumulative score forward across them.
    """
    df = df.sort(["game_id", "play_id"])

    points_value = (
        pl.when(pl.col("touchdown") == 1)
        .then(pl.lit(6))
        .when(pl.col("def_touchdown") == 1)
        .then(pl.lit(6))
        .when(pl.col("one_point_conv_success") == 1)
        .then(pl.lit(1))
        .when(pl.col("two_point_conv_success") == 1)
        .then(pl.lit(2))
        .when(pl.col("defensive_two_point_conv") == 1)
        .then(pl.lit(2))
        .when(pl.col("safety") == 1)
        .then(pl.lit(2))
        .otherwise(pl.lit(None))
        .cast(pl.Int32)
    )

    df = df.with_columns(
        [
            pl.when(pl.col("play_id") == 1)
            .then(pl.lit(0))
            .when(pl.col("home_team") == pl.col("scoring_play_team"))
            .then(points_value)
            .otherwise(pl.lit(None))
            .cast(pl.Int32)
            .alias("home_team_points"),
            pl.when(pl.col("play_id") == 1)
            .then(pl.lit(0))
            .when(pl.col("away_team") == pl.col("scoring_play_team"))
            .then(points_value)
            .otherwise(pl.lit(None))
            .cast(pl.Int32)
            .alias("away_team_points"),
        ]
    )

    df = df.with_columns(
        [
            pl.col("home_team_points")
            .cum_sum()
            .over(["game_id", "home_team"])
            .alias("home_team_score"),
            pl.col("away_team_points")
            .cum_sum()
            .over(["game_id", "away_team"])
            .alias("away_team_score"),
        ]
    )

    df = df.with_columns(
        [
            pl.col("home_team_score").forward_fill().alias("home_team_score"),
            pl.col("away_team_score").forward_fill().alias("away_team_score"),
        ]
    )

    df = df.with_columns(
        [
            pl.when(pl.col("posteam") == pl.col("home_team"))
            .then(pl.col("home_team_score"))
            .when(pl.col("posteam") == pl.col("away_team"))
            .then(pl.col("away_team_score"))
            .otherwise(pl.lit(None))
            .cast(pl.Int32)
            .alias("posteam_score"),
            pl.when(pl.col("defteam") == pl.col("home_team"))
            .then(pl.col("home_team_score"))
            .when(pl.col("defteam") == pl.col("away_team"))
            .then(pl.col("away_team_score"))
            .otherwise(pl.lit(None))
            .cast(pl.Int32)
            .alias("defteam_score"),
        ]
    )

    df = df.with_columns(
        (pl.col("posteam_score") - pl.col("defteam_score"))
        .cast(pl.Int32)
        .alias("score_differential")
    )

    return df
