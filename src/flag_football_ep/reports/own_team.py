"""Own-team efficiency data for the configured own team (REQ-S1-13).

This module computes REQ-S1-13 for the configured own team, performs no rendering and no
writes, and honours the locked 1.3 EPA contract: historical plays use persisted out-of-fold
predictions, new plays use the promoted champion's scores, and a single play never draws
from both. `attach_epa` is the single place this join happens; every other function in this
module consumes its output.

`ReportSection` here intentionally mirrors `reports.opponent.ReportSection` (plan 08)
field-for-field (`key`, `heading`, `table`, `basis`, `empty_notice`). Plan 08 is a same-wave
sibling plan (wave 3) that this plan does not declare a dependency on, and
`reports/opponent.py` had not landed in this worktree at the time this plan executed, so a
local copy is declared here rather than importing one that does not exist yet. A follow-up
plan should deduplicate onto one shared definition (e.g. move `ReportSection` into
`reports/aggregate.py`) once both plans are merged.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import polars as pl

from flag_football_ep.features.mutations import (
    add_ep_variables,
    add_wp_variables,
    estimate_pat_baselines,
    prepare_ep_data,
    prepare_wp_data,
)
from flag_football_ep.model.hyperparams import EP_PROB_LABELS
from flag_football_ep.reference import map_players
from flag_football_ep.reports.aggregate import (
    MUTED_MIN_N,
    SectionBasis,
    charted_only,
    rate_table,
    section_basis,
)

EpaSource = Literal["oof", "champion"]

_ATTACH_ROW_ID = "_attach_epa_row_id"


@dataclass(frozen=True)
class ReportSection:
    """One report section: heading, table, data basis, and an explicit empty-data notice.

    `empty_notice` is a German sentence set when `table` is empty, `None` otherwise --
    CONTEXT's locked "explicit no-data block rather than failing" behaviour.
    """

    key: str
    heading: str
    table: pl.DataFrame
    basis: SectionBasis
    empty_notice: str | None


# --- attach_epa ---------------------------------------------------------------------------


def attach_epa(
    plays: pl.DataFrame, *, processed_dir: Path, scored: pl.DataFrame | None
) -> pl.DataFrame:
    """Add `epa`, `ep`, `wp`, `home_wp`, `wpa` and an `epa_source` (`EpaSource` | null) column.

    Deliberately operates on `plays` as a whole (not pre-filtered to one team): `add_ep_variables`
    /`add_wp_variables` derive `epa`/`wpa` via `shift(-1)`/`backward_fill()` over `game_id`, which
    requires true play-by-play adjacency across BOTH teams' snaps. Filtering to one team's plays
    before calling this function would silently corrupt every EPA value by skipping over the
    opponent's intervening snaps -- callers must filter the OUTPUT of `attach_epa`, never its
    input, to a team subset.

    For historical plays, reads `oof_predictions_ep.parquet` and `oof_predictions_wp.parquet`
    from `processed_dir` (a missing file is not an error -- it just means no play qualifies as
    historical via that source; the caller is responsible for surfacing that as a notice). The
    OOF frames carry `EP_PROB_LABELS` probability columns and `wp`, not `epa`/`ep`, so `ep`/`epa`
    are derived from the joined probabilities via `add_ep_variables` (with
    `estimate_pat_baselines(plays)` as the baseline argument) exactly as `model.score.score_plays`
    does, and `home_wp`/`wpa` via `add_wp_variables` -- never hand-rolled.

    A play is "oof" exactly when its `(game_id, play_id)` matches a row in the EP OOF file (the
    WP OOF file is expected to cover the same historical corpus; a play's `epa_source` is decided
    by the EP match alone, documented here rather than silently assumed). For rows with no OOF
    match, the corresponding `ep`/`epa`/`wp`/`home_wp`/`wpa` are taken from `scored` (the
    champion-scored frame), joined on the same key, and `epa_source` is set to `"champion"`. The
    champion join is applied ONLY to the OOF-unmatched subset -- by construction, never as a
    coalesce -- so a single play can never draw a non-null EPA value from both sources. A row
    matched by neither keeps null EPA columns and a null `epa_source`; it is excluded from every
    EPA rollup downstream (callers filter on `epa.is_not_null()`) and should be counted in a
    caller-level notice.

    Row count and row order are preserved. Assumes the input is already in chronological
    (game_id, play_id) order, matching `plays.parquet`'s persisted order and the same assumption
    `model.score.score_plays` makes.
    """
    schema_cols = ["ep", "epa", "wp", "home_wp", "wpa"]

    if plays.height == 0:
        return plays.with_columns(
            [pl.lit(None, dtype=pl.Float64).alias(c) for c in schema_cols]
            + [pl.lit(None, dtype=pl.Utf8).alias("epa_source")]
        )

    plays = plays.with_row_index(name=_ATTACH_ROW_ID, offset=0)
    pat_baselines = estimate_pat_baselines(plays)

    ep_path = processed_dir / "oof_predictions_ep.parquet"
    wp_path = processed_dir / "oof_predictions_wp.parquet"

    if ep_path.exists():
        oof_ep = pl.read_parquet(ep_path).with_columns(
            pl.col("game_id").cast(pl.Utf8), pl.col("play_id").cast(pl.Int32)
        )
        ep_prepared = prepare_ep_data(plays)
        ep_joined = ep_prepared.join(
            oof_ep.select(["game_id", "play_id", *EP_PROB_LABELS]),
            on=["game_id", "play_id"],
            how="left",
        )
        matched_ep = ep_joined[EP_PROB_LABELS[0]].is_not_null()
        ep_scored = add_ep_variables(ep_joined, pat_baselines=pat_baselines)
        oof_ep_values = ep_scored.select([_ATTACH_ROW_ID, "ep", "epa"]).with_columns(
            pl.Series("_oof_matched", matched_ep)
        )
    else:
        oof_ep_values = plays.select([_ATTACH_ROW_ID]).with_columns(
            ep=pl.lit(None, dtype=pl.Float64),
            epa=pl.lit(None, dtype=pl.Float64),
            _oof_matched=pl.lit(False),
        )

    if wp_path.exists():
        oof_wp = pl.read_parquet(wp_path).with_columns(
            pl.col("game_id").cast(pl.Utf8), pl.col("play_id").cast(pl.Int32)
        )
        wp_prepared = prepare_wp_data(plays)
        wp_joined = wp_prepared.join(
            oof_wp.select(["game_id", "play_id", "wp"]),
            on=["game_id", "play_id"],
            how="left",
        )
        wp_scored = add_wp_variables(wp_joined)
        oof_wp_values = wp_scored.select([_ATTACH_ROW_ID, "wp", "home_wp", "wpa"])
    else:
        oof_wp_values = plays.select([_ATTACH_ROW_ID]).with_columns(
            wp=pl.lit(None, dtype=pl.Float64),
            home_wp=pl.lit(None, dtype=pl.Float64),
            wpa=pl.lit(None, dtype=pl.Float64),
        )

    oof_values = oof_ep_values.join(oof_wp_values, on=_ATTACH_ROW_ID, how="left")
    base = plays.join(oof_values, on=_ATTACH_ROW_ID, how="left")

    oof_rows = base.filter(pl.col("_oof_matched")).with_columns(epa_source=pl.lit("oof"))
    remainder = base.filter(~pl.col("_oof_matched"))

    if scored is not None and remainder.height > 0:
        champion_cols = (
            scored.select(["game_id", "play_id", *schema_cols])
            .with_columns(
                pl.col("game_id").cast(pl.Utf8), pl.col("play_id").cast(pl.Int32)
            )
            .with_columns(_champion_matched=pl.lit(True))
        )
        remainder = (
            remainder.drop(schema_cols)
            .join(champion_cols, on=["game_id", "play_id"], how="left")
            .with_columns(_champion_matched=pl.col("_champion_matched").fill_null(False))
        )
        remainder = remainder.with_columns(
            epa_source=pl.when(pl.col("_champion_matched"))
            .then(pl.lit("champion"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
        ).drop("_champion_matched")
    else:
        remainder = remainder.with_columns(epa_source=pl.lit(None, dtype=pl.Utf8))

    result = (
        pl.concat([oof_rows, remainder.select(oof_rows.columns)], how="vertical")
        .sort(_ATTACH_ROW_ID)
        .drop([_ATTACH_ROW_ID, "_oof_matched"])
    )
    return result


# --- efficiency_by_call --------------------------------------------------------------------

_EPA_ROLLUP_SCHEMA: dict[str, pl.DataType] = {
    "n_cycle": pl.Int64,
    "epa_play_cycle": pl.Float64,
    "n_alltime": pl.Int64,
    "epa_play_alltime": pl.Float64,
    "muted": pl.Boolean,
}


def _epa_rollup_by(
    df: pl.DataFrame, group_col: str, *, cycle_start_season: int
) -> pl.DataFrame:
    """Per-`group_col` value: `n_cycle`/`epa_play_cycle` (season >= cycle_start_season),
    `n_alltime`/`epa_play_alltime` (all rows), and `muted` (`n_cycle < MUTED_MIN_N`).

    Rows with a null `group_col` are excluded. A category present only outside the cycle keeps
    `n_cycle == 0` and a null `epa_play_cycle` while retaining its all-time values. Returns a
    schema-correct empty frame (with `group_col`) when `df` is empty or every value is null.
    """
    working = df.filter(pl.col(group_col).is_not_null())
    schema = {group_col: pl.Utf8, **_EPA_ROLLUP_SCHEMA}
    if working.height == 0:
        return pl.DataFrame(schema=schema)

    all_time = working.group_by(group_col, maintain_order=True).agg(
        n_alltime=pl.len().cast(pl.Int64),
        epa_play_alltime=pl.col("epa").mean(),
    )
    cycle = (
        working.filter(pl.col("season") >= cycle_start_season)
        .group_by(group_col, maintain_order=True)
        .agg(
            n_cycle=pl.len().cast(pl.Int64),
            epa_play_cycle=pl.col("epa").mean(),
        )
    )
    merged = (
        all_time.join(cycle, on=group_col, how="left")
        .with_columns(n_cycle=pl.col("n_cycle").fill_null(0))
        .with_columns(muted=pl.col("n_cycle") < MUTED_MIN_N)
    )
    return merged


_CALL_DIMENSIONS: tuple[str, ...] = ("off_form", "off_play", "target_route")

_CALL_TABLE_SCHEMA: dict[str, pl.DataType] = {
    "dimension": pl.Utf8,
    "wert": pl.Utf8,
    **_EPA_ROLLUP_SCHEMA,
}


def efficiency_by_call(plays: pl.DataFrame, *, cycle_start_season: int) -> ReportSection:
    """EPA/play by `off_form`, `off_play` and `target_route`, current cycle vs all-time.

    After restricting to rows with a non-null `epa`, builds one long-format table with a
    `dimension` column over `off_form`/`off_play`/`target_route` and a `wert` column holding
    the category. Each dimension is Hudl-only, so each is restricted with `charted_only`
    individually; the section's `basis` reports the union of rows charted on at least one of
    the three dimensions.
    """
    heading = "EPA/Play nach Formation, Play-Call und Route"
    key = "efficiency_by_call"

    df = plays.filter(pl.col("epa").is_not_null())

    tables = []
    for dim in _CALL_DIMENSIONS:
        rolled = _epa_rollup_by(
            charted_only(df, dim), dim, cycle_start_season=cycle_start_season
        )
        if rolled.height == 0:
            continue
        rolled = rolled.rename({dim: "wert"}).with_columns(dimension=pl.lit(dim)).select(
            ["dimension", "wert", *_EPA_ROLLUP_SCHEMA]
        )
        tables.append(rolled)

    table = (
        pl.concat(tables, how="vertical")
        if tables
        else pl.DataFrame(schema=_CALL_TABLE_SCHEMA)
    )

    charted_mask = pl.lit(False)
    for dim in _CALL_DIMENSIONS:
        charted_mask = charted_mask | pl.col(dim).is_not_null()
    basis_df = df.filter(charted_mask) if df.height else df
    basis = section_basis(basis_df)

    empty_notice = (
        None if table.height > 0 else "Kein Charting-Material für diese Auswertung vorhanden."
    )
    return ReportSection(key=key, heading=heading, table=table, basis=basis, empty_notice=empty_notice)


# --- player_efficiency ----------------------------------------------------------------------

_PLAYER_SOURCE_COLUMNS: tuple[str, ...] = ("qb", "thrown_by", "received_by", "target")

_PLAYER_TABLE_SCHEMA: dict[str, pl.DataType] = {
    "rolle": pl.Utf8,
    "spieler": pl.Utf8,
    "n_cycle": pl.Int64,
    "epa_play_cycle": pl.Float64,
    "n_alltime": pl.Int64,
    "epa_play_alltime": pl.Float64,
    "completion_n": pl.Int64,
    "completion_rate": pl.Float64,
    "completion_ci_low": pl.Float64,
    "completion_ci_high": pl.Float64,
    "yac_summe": pl.Int64,
    "yac_schnitt": pl.Float64,
    "yac_anteil": pl.Float64,
    "muted": pl.Boolean,
}


def _canonicalise_players(
    df: pl.DataFrame, mapping: pl.DataFrame
) -> tuple[pl.DataFrame, list[str]]:
    """Canonicalise `_PLAYER_SOURCE_COLUMNS` in `df` via `map_players`, called once per
    `source` present in the frame (a mapping row is keyed by `(source, source_player)`).
    Returns the canonicalised frame (row count and order preserved) and the sorted union of
    every label left unmapped across sources.
    """
    columns = [c for c in _PLAYER_SOURCE_COLUMNS if c in df.columns]
    if df.height == 0 or not columns:
        return df, []

    row_id = "_canon_row_id"
    indexed = df.with_row_index(name=row_id, offset=0)

    unmapped: set[str] = set()
    parts: list[pl.DataFrame] = []
    for src in sorted(indexed["source"].drop_nulls().unique().to_list()):
        subset = indexed.filter(pl.col("source") == src)
        result = map_players(subset, mapping, src, columns)
        unmapped.update(result.unmapped)
        parts.append(result.frame)

    no_source = indexed.filter(pl.col("source").is_null())
    if no_source.height:
        parts.append(no_source)

    combined = pl.concat(parts, how="vertical").sort(row_id).drop(row_id)
    return combined, sorted(unmapped)


def player_efficiency(
    plays: pl.DataFrame, mapping: pl.DataFrame, *, cycle_start_season: int
) -> tuple[ReportSection, tuple[str, ...]]:
    """Per-QB and per-receiver EPA, YAC shares, and the unmapped-player warning.

    Canonicalises identities before any rollup via `_canonicalise_players` -- `map_players`
    never raises, so an unmapped spelling still appears in the table verbatim; the returned
    unmapped tuple is what a page turns into a prominent warning block. Rows whose player
    column is null are excluded from the player rollups (an unattributed play is not a
    player's play). Sorted per role by `epa_play_cycle` descending with `n_cycle` as
    tiebreaker.
    """
    heading = "EPA pro QB und Receiver, YAC-Anteile"
    key = "player_efficiency"

    canon, unmapped = _canonicalise_players(plays, mapping)

    if canon.height == 0 or not any(c in canon.columns for c in _PLAYER_SOURCE_COLUMNS):
        table = pl.DataFrame(schema=_PLAYER_TABLE_SCHEMA)
        section = ReportSection(
            key=key,
            heading=heading,
            table=table,
            basis=section_basis(canon),
            empty_notice="Kein Charting-Material für diese Auswertung vorhanden.",
        )
        return section, tuple(unmapped)

    canon = canon.with_columns(
        _qb_player=pl.coalesce([pl.col("thrown_by"), pl.col("qb")])
    )

    qb_rows = canon.filter(pl.col("_qb_player").is_not_null())
    qb_epa = _epa_rollup_by(qb_rows, "_qb_player", cycle_start_season=cycle_start_season)
    if qb_epa.height:
        completion = rate_table(
            qb_rows, ["_qb_player"], pl.col("complete_pass") == 1, label_col="_label"
        ).select(
            [
                pl.col("_qb_player"),
                pl.col("n").alias("completion_n"),
                pl.col("rate").alias("completion_rate"),
                pl.col("ci_low").alias("completion_ci_low"),
                pl.col("ci_high").alias("completion_ci_high"),
            ]
        )
        qb_table = (
            qb_epa.join(completion, on="_qb_player", how="left")
            .rename({"_qb_player": "spieler"})
            .with_columns(
                rolle=pl.lit("QB"),
                yac_summe=pl.lit(None, dtype=pl.Int64),
                yac_schnitt=pl.lit(None, dtype=pl.Float64),
                yac_anteil=pl.lit(None, dtype=pl.Float64),
            )
            .sort(["epa_play_cycle", "n_cycle"], descending=[True, True], nulls_last=True)
            .select(list(_PLAYER_TABLE_SCHEMA))
        )
    else:
        qb_table = pl.DataFrame(schema=_PLAYER_TABLE_SCHEMA)

    recv_rows = canon.filter(pl.col("received_by").is_not_null())
    recv_epa = _epa_rollup_by(recv_rows, "received_by", cycle_start_season=cycle_start_season)
    if recv_epa.height:
        yac_rows = recv_rows.filter(pl.col("yac").is_not_null())
        total_yac = yac_rows["yac"].sum() if yac_rows.height else None
        yac_agg = recv_rows.group_by("received_by", maintain_order=True).agg(
            yac_summe=pl.col("yac").sum().cast(pl.Int64),
            yac_schnitt=pl.col("yac").mean(),
        )
        if total_yac:
            yac_agg = yac_agg.with_columns(
                yac_anteil=pl.col("yac_summe").fill_null(0) / total_yac
            )
        else:
            yac_agg = yac_agg.with_columns(yac_anteil=pl.lit(None, dtype=pl.Float64))
        recv_table = (
            recv_epa.join(yac_agg, on="received_by", how="left")
            .rename({"received_by": "spieler"})
            .with_columns(
                rolle=pl.lit("Receiver"),
                completion_n=pl.lit(None, dtype=pl.Int64),
                completion_rate=pl.lit(None, dtype=pl.Float64),
                completion_ci_low=pl.lit(None, dtype=pl.Float64),
                completion_ci_high=pl.lit(None, dtype=pl.Float64),
            )
            .sort(["epa_play_cycle", "n_cycle"], descending=[True, True], nulls_last=True)
            .select(list(_PLAYER_TABLE_SCHEMA))
        )
    else:
        recv_table = pl.DataFrame(schema=_PLAYER_TABLE_SCHEMA)

    table = pl.concat([qb_table, recv_table], how="vertical")

    empty_notice = (
        None if table.height > 0 else "Kein Charting-Material für diese Auswertung vorhanden."
    )
    basis = section_basis(canon.filter(pl.col("_qb_player").is_not_null() | pl.col("received_by").is_not_null()))
    section = ReportSection(key=key, heading=heading, table=table, basis=basis, empty_notice=empty_notice)
    return section, tuple(unmapped)
