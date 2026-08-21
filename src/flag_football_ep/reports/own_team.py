"""Own-team efficiency data for the configured own team (REQ-S1-13).

This module computes REQ-S1-13 for the configured own team, performs no rendering and no
writes, and honours the locked 1.3 EPA contract: historical plays use persisted out-of-fold
predictions, new plays use the promoted champion's scores, and a single play never draws
from both. `attach_epa` is the single place this join happens; every other function in this
module consumes its output.

`ReportSection` is the shared per-section container from `reports.aggregate`, used by both
this module and `reports.opponent`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import polars as pl

from flag_football_ep.config import Config
from flag_football_ep.features.mutations import (
    add_ep_variables,
    add_wp_variables,
    estimate_pat_baselines,
    prepare_ep_data,
    prepare_wp_data,
)
from flag_football_ep.model.hyperparams import EP_PROB_LABELS
from flag_football_ep.reference import MissingReferenceFile, load_player_mapping, map_players
from flag_football_ep.reports.aggregate import (
    MUTED_MIN_N,
    ReportSection,
    SectionBasis,
    charted_only,
    rate_table,
    section_basis,
    share_table,
)

EpaSource = Literal["oof", "champion"]

_ATTACH_ROW_ID = "_attach_epa_row_id"

_EMPTY_MAPPING_SCHEMA: dict[str, pl.DataType] = {
    "source": pl.Utf8,
    "source_player": pl.Utf8,
    "canonical_player": pl.Utf8,
}


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


# --- drive_outcomes -------------------------------------------------------------------------

_DRIVE_TABLE_SCHEMA: dict[str, pl.DataType] = {
    "drive_outcome": pl.Utf8,
    "n": pl.Int64,
    "group_n": pl.Int64,
    "share": pl.Float64,
    "ci_low": pl.Float64,
    "ci_high": pl.Float64,
    "muted": pl.Boolean,
    "punkte_pro_drive": pl.Float64,
    "drive_count": pl.Int64,
}


def drive_outcomes(plays: pl.DataFrame) -> ReportSection:
    """Drive-outcome breakdown (TD / Turnover / Downs / Halbzeitende / Sonstiges) plus a
    `punkte_pro_drive` headline, broadcast onto every table row alongside `drive_count`.

    Derives the breakdown FRESH per `(game_id, drive_id)`. Does NOT read the canonical
    `drive_success` column: it is populated only for the `legacy` source with different,
    binary semantics, so aggregating it would null out the majority of the corpus (RESEARCH
    Pitfall 4). Do not "fix" this back to reading `drive_success`.

    Classification precedence per drive: `TD` when any play has `touchdown == 1`; `Turnover`
    when any play has `interception == 1` or the drive's last play changes possession
    (`posteam_after != posteam`) on a non-scoring play; `Downs` when the last play is a 4th
    down that did not convert (`down == 4` and `first_down == 0`); `Halbzeitende` when the
    last play is the last play of its `(game_id, half)`; otherwise `Sonstiges`.
    """
    heading = "Drive Success: Punkte pro Drive und Drive-Ausgänge"
    key = "drive_outcomes"

    if plays.height == 0:
        table = pl.DataFrame(schema=_DRIVE_TABLE_SCHEMA)
        return ReportSection(
            key=key,
            heading=heading,
            table=table,
            basis=section_basis(plays),
            empty_notice="Keine Drives vorhanden.",
        )

    ordered = plays.sort(["game_id", "drive_id", "play_id"])

    half_ends = ordered.group_by(["game_id", "half"], maintain_order=True).agg(
        _half_max_play_id=pl.col("play_id").max()
    )

    per_drive = (
        ordered.group_by(["game_id", "drive_id"], maintain_order=True)
        .agg(
            any_touchdown=(pl.col("touchdown") == 1).any(),
            any_interception=(pl.col("interception") == 1).any(),
            last_down=pl.col("down").last(),
            last_first_down=pl.col("first_down").last(),
            last_play_id=pl.col("play_id").last(),
            last_half=pl.col("half").last(),
            last_posteam=pl.col("posteam").last(),
            last_posteam_after=pl.col("posteam_after").last(),
        )
        .join(
            half_ends,
            left_on=["game_id", "last_half"],
            right_on=["game_id", "half"],
            how="left",
        )
    )

    outcome_expr = (
        pl.when(pl.col("any_touchdown"))
        .then(pl.lit("TD"))
        .when(pl.col("any_interception"))
        .then(pl.lit("Turnover"))
        .when(
            pl.col("last_posteam_after").is_not_null()
            & (pl.col("last_posteam_after") != pl.col("last_posteam"))
        )
        .then(pl.lit("Turnover"))
        .when((pl.col("last_down") == 4) & (pl.col("last_first_down") == 0))
        .then(pl.lit("Downs"))
        .when(pl.col("last_play_id") == pl.col("_half_max_play_id"))
        .then(pl.lit("Halbzeitende"))
        .otherwise(pl.lit("Sonstiges"))
    )
    per_drive = per_drive.with_columns(drive_outcome=outcome_expr)

    n_drives = per_drive.height
    # share_table requires at least one group column; "_all" is a constant so every drive
    # lands in the same single group -- the group column is dropped from the output below.
    per_drive = per_drive.with_columns(_all=pl.lit("all"))
    table = share_table(per_drive, group_cols=["_all"], category_col="drive_outcome").drop(
        "_all"
    )

    total_points = plays.select(
        (
            pl.col("touchdown").fill_null(0) * 6
            + pl.col("one_point_conv_success").fill_null(0) * 1
            + pl.col("two_point_conv_success").fill_null(0) * 2
        ).sum()
    ).item()
    punkte_pro_drive = (total_points / n_drives) if n_drives else None

    table = table.with_columns(
        punkte_pro_drive=pl.lit(punkte_pro_drive, dtype=pl.Float64),
        drive_count=pl.lit(n_drives, dtype=pl.Int64),
    ).select(list(_DRIVE_TABLE_SCHEMA))

    basis = section_basis(plays)
    empty_notice = None if table.height > 0 else "Keine Drives vorhanden."
    return ReportSection(key=key, heading=heading, table=table, basis=basis, empty_notice=empty_notice)


# --- defense_section -------------------------------------------------------------------------

_DEFENSE_TABLE_SCHEMA: dict[str, pl.DataType] = {
    "dimension": pl.Utf8,
    "wert": pl.Utf8,
    "n": pl.Int64,
    "epa_play_allowed": pl.Float64,
    "muted": pl.Boolean,
    "interceptions_n": pl.Int64,
    "def_touchdowns_n": pl.Int64,
}

_DEFENSE_DIMENSIONS: tuple[str, ...] = ("def_front", "coverage")


def defense_section(plays: pl.DataFrame, *, team: str) -> ReportSection:
    """EPA/play allowed by `def_front` and `coverage`, plus takeaway counts.

    Filters to `defteam == team` (the own team on defense). `epa_play_allowed` is the mean
    EPA of the OPPOSING offense on those plays -- a lower value is better for the defense.
    Both `def_front` and `coverage` are Hudl-only, restricted individually with
    `charted_only`; the basis reports the union of rows charted on at least one of the two.
    This extends REQ-S1-13, it does not redefine it -- no per-defender attribution here
    (explicitly deferred in CONTEXT); no player mapping is needed since both fields are
    formation-level.
    """
    heading = "Defense: EPA/Play erlaubt nach DEF FRONT und COVERAGE (niedriger ist besser)"
    key = "defense_section"

    defense_all = plays.filter(pl.col("defteam") == team)
    df = defense_all.filter(pl.col("epa").is_not_null())

    interceptions_n = int(defense_all.filter(pl.col("interception") == 1).height)
    def_touchdowns_n = int(defense_all.filter(pl.col("def_touchdown") == 1).height)

    tables = []
    for dim in _DEFENSE_DIMENSIONS:
        charted = charted_only(df, dim)
        if charted.height == 0:
            continue
        grouped = (
            charted.group_by(dim, maintain_order=True)
            .agg(n=pl.len().cast(pl.Int64), epa_play_allowed=pl.col("epa").mean())
            .rename({dim: "wert"})
            .with_columns(dimension=pl.lit(dim), muted=pl.col("n") < MUTED_MIN_N)
            .select(["dimension", "wert", "n", "epa_play_allowed", "muted"])
        )
        tables.append(grouped)

    table = pl.concat(tables, how="vertical") if tables else pl.DataFrame(
        schema={k: v for k, v in _DEFENSE_TABLE_SCHEMA.items() if k not in ("interceptions_n", "def_touchdowns_n")}
    )
    table = table.with_columns(
        interceptions_n=pl.lit(interceptions_n, dtype=pl.Int64),
        def_touchdowns_n=pl.lit(def_touchdowns_n, dtype=pl.Int64),
    ).select(list(_DEFENSE_TABLE_SCHEMA))

    charted_mask = pl.lit(False)
    for dim in _DEFENSE_DIMENSIONS:
        charted_mask = charted_mask | pl.col(dim).is_not_null()
    basis_df = df.filter(charted_mask) if df.height else df
    basis = section_basis(basis_df)

    empty_notice = (
        None if table.height > 0 else "Kein Charting-Material für diese Auswertung vorhanden."
    )
    return ReportSection(key=key, heading=heading, table=table, basis=basis, empty_notice=empty_notice)


# --- build_own_team_data ---------------------------------------------------------------------


@dataclass(frozen=True)
class OwnTeamReportData:
    """The complete own-team efficiency data object (REQ-S1-13), render-ready."""

    team: str
    cycle_start_season: int
    sections: tuple[ReportSection, ...]
    unmapped_players: tuple[str, ...]
    overall_basis: SectionBasis
    notices: tuple[str, ...]


def build_own_team_data(
    plays: pl.DataFrame, *, config: Config, scored: pl.DataFrame | None = None
) -> OwnTeamReportData:
    """Assemble the full own-team `OwnTeamReportData` for `config.report.own_team`.

    `attach_epa` runs on the FULL, unfiltered `plays` corpus first (see `attach_epa`'s
    docstring for why), and only the resulting EPA-attached frame is filtered to
    `posteam == team` (offensive sections) / `defteam == team` (defense section).
    `notices` collects a German string per degraded condition: a missing OOF predictions
    file (either EP or WP), plays with no EPA source at all, a missing player-mapping file,
    unmapped player names, any empty section, and an absent own team. Never raises.
    """
    team = config.report.own_team
    cycle_start_season = config.report.cycle_start_season
    notices: list[str] = []

    ep_oof_path = config.paths.processed / "oof_predictions_ep.parquet"
    wp_oof_path = config.paths.processed / "oof_predictions_wp.parquet"
    if not ep_oof_path.exists():
        notices.append(
            f"Keine Out-of-fold EP-Predictions gefunden ({ep_oof_path}); historische Plays "
            "bleiben ohne EPA, sofern kein Champion-Scoring vorliegt."
        )
    if not wp_oof_path.exists():
        notices.append(
            f"Keine Out-of-fold WP-Predictions gefunden ({wp_oof_path}); historische Plays "
            "bleiben ohne WP, sofern kein Champion-Scoring vorliegt."
        )

    scored_plays = attach_epa(plays, processed_dir=config.paths.processed, scored=scored)

    if scored_plays.height:
        n_no_source = scored_plays.filter(pl.col("epa_source").is_null()).height
        if n_no_source:
            notices.append(
                f"{n_no_source} Play(s) ohne EPA-Quelle (weder OOF noch Champion-Scoring) "
                "wurden von den EPA-Auswertungen ausgeschlossen."
            )

    offense = scored_plays.filter(pl.col("posteam") == team)
    defense = scored_plays.filter(pl.col("defteam") == team)

    try:
        mapping = load_player_mapping(config.reference.player_mapping)
    except MissingReferenceFile:
        notices.append(
            f"Spieler-Mapping-Datei fehlt ({config.reference.player_mapping}); nicht "
            "zugeordnete Spielernamen können nicht aufgelöst werden."
        )
        mapping = pl.DataFrame(schema=_EMPTY_MAPPING_SCHEMA)

    call_section = efficiency_by_call(offense, cycle_start_season=cycle_start_season)
    player_section, unmapped = player_efficiency(
        offense, mapping, cycle_start_season=cycle_start_season
    )
    drive_section = drive_outcomes(offense)
    def_section = defense_section(defense, team=team)

    if unmapped:
        notices.append(f"Nicht zugeordnete Spielernamen: {', '.join(unmapped)}")

    sections = (call_section, player_section, drive_section, def_section)
    for section in sections:
        if section.table.height == 0:
            notices.append(f"Abschnitt '{section.heading}' enthält keine Daten.")

    if offense.height == 0:
        notices.append(f"Keine Offense-Plays für {team!r} im Korpus gefunden.")

    return OwnTeamReportData(
        team=team,
        cycle_start_season=cycle_start_season,
        sections=sections,
        unmapped_players=tuple(unmapped),
        overall_basis=section_basis(offense),
        notices=tuple(notices),
    )
