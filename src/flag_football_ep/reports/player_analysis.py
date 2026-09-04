"""Reproduces the head coach's `Player Analysis All Camps` tab (HC-05) from canonical plays.

Every column below is pinned verbatim to its workbook formula cell (`M3-04-RESEARCH.md`
Pattern 1, sheet `Player Analysis All Camps`, row 2 as the representative formula row), not
re-derived from what a column name suggests. Two denominator corrections landed in M3-04-01
before this module was written (`Attempts` excludes Sacks; `Efficiency`'s denominator is
`Attempts + Carries`, not `Attempts + Drops`) -- both will be consumed here via
`features.explosiveness`'s corrected public API in a follow-up task, never recomputed locally.

The one rule that governs this module: a column that cannot be computed from the corpus today
is named, never approximated -- never a silent zero, a dash, or a copy of a neighbouring column
(REP-D01: differences are shown, not hidden). This first task builds the thirteen
counting/yardage columns directly from canonical plays; a following task fills the six
delegated/blocked columns (`adj_comp_pct`, `adj_pass_yards`, `adj_ypa`, `exp_plays`,
`explosive_pct`, `efficiency`, `efficiency_drops`) -- they are typed nulls here.

There is no WR/receiver table on this tab (verified in M3-04-RESEARCH: columns Z onward and the
rows below the last QB row are empty) -- this module is QB-row-only, matching his own sheet.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from flag_football_ep.features.explosiveness import HC_PASS_ATTEMPT_SCOPE, scrimmage_plays
from flag_football_ep.reports.aggregate import MUTED_MIN_N, SectionBasis, section_basis

PLAYER_ANALYSIS_FILENAME: str = "player-analysis.html"

# His tab's own column order (interfaces block, M3-04-03-PLAN.md). Integers are Int64, rates
# and yard sums are Float64, `muted` is Boolean.
_HC_COLUMN_SCHEMA: dict[str, pl.DataType] = {
    "spieler": pl.Utf8,
    "comps": pl.Int64,
    "incs": pl.Int64,
    "attempts": pl.Int64,
    "tds": pl.Int64,
    "comp_pct": pl.Float64,
    "adj_comp_pct": pl.Float64,
    "ints": pl.Int64,
    "sacks": pl.Int64,
    "pass_yards": pl.Float64,
    "air_yards": pl.Float64,
    "ypa": pl.Float64,
    "adj_pass_yards": pl.Float64,
    "adj_ypa": pl.Float64,
    "exp_plays": pl.Int64,
    "explosive_pct": pl.Float64,
    "efficiency": pl.Float64,
    "efficiency_drops": pl.Float64,
    "carries": pl.Int64,
    "rush_yards": pl.Float64,
    "rush_tds": pl.Int64,
    "muted": pl.Boolean,
}

# Delegated/blocked columns -- typed nulls in this task, filled by a following task.
_DELEGATED_COLUMNS: tuple[str, ...] = (
    "adj_comp_pct",
    "adj_pass_yards",
    "adj_ypa",
    "exp_plays",
    "explosive_pct",
    "efficiency",
    "efficiency_drops",
)

# Internal row-key column name used while building the per-identity tables below; renamed to
# `spieler` (the schema's own key column) before any table leaves this module.
_IDENTITY = "_hc_identity"


def _identity_expr(group_col: str) -> pl.Expr:
    """`coalesce(thrown_by, qb)` for the default `group_col == "thrown_by"` -- the same
    QB-identity fallback `features/explosiveness.py::_with_group_key` and
    `reports/own_team.py::player_efficiency` both already use, so a rushing QB (no `thrown_by`
    on a run row) still resolves to their `qb` value. For any other `group_col`, that column is
    used as-is, mirroring `_with_group_key`'s own behaviour. A row where the resolved value is
    null is excluded by every caller below, never grouped under a null key.
    """
    if group_col == "thrown_by":
        return pl.coalesce([pl.col("thrown_by"), pl.col("qb")])
    return pl.col(group_col)


def _pass_and_sack_table(plays: pl.DataFrame, *, group_col: str) -> pl.DataFrame:
    """Comps, Incs, Attempts, TDs, Comp %, INTs, Sacks, Pass Yards, Air Yards, YPA -- one row
    per resolved identity.

    Every pass-attempt column routes through the imported `HC_PASS_ATTEMPT_SCOPE` on top of
    `scrimmage_plays` -- never a local `play_type == "pass"` filter (this module's own
    `<verification>` grep enforces it). `Sacks` is its own scope (`sack == 1`, workbook cell
    `I2`), never summed into `Attempts` (`D2 = B2 + C2 + H2`, no Sacks term, M3-04-01
    correction). A QB who only ever appears via a sack row (never a Comp/Inc/INT) still gets a
    real `0` for those three columns, not a null -- they are known to have thrown, just never
    completed/attempted anything else; a QB who never appears in either scope at all is simply
    absent from this table (the caller's outer join with the run-side table is what turns that
    absence into an explicit null, not this function).
    """
    base = scrimmage_plays(plays)
    has_air_yards = "air_yards" in plays.columns

    pass_scope = (
        base.filter(HC_PASS_ATTEMPT_SCOPE)
        .with_columns(_identity_expr(group_col).alias(_IDENTITY))
        .filter(pl.col(_IDENTITY).is_not_null())
    )

    pass_schema: dict[str, pl.DataType] = {
        _IDENTITY: pl.Utf8,
        "comps": pl.Int64,
        "incs": pl.Int64,
        "tds": pl.Int64,
        "ints": pl.Int64,
        "pass_yards": pl.Float64,
    }
    if has_air_yards:
        pass_schema["air_yards"] = pl.Float64

    if pass_scope.height == 0:
        pass_agg = pl.DataFrame(schema=pass_schema)
    else:
        agg_exprs = {
            "comps": (pl.col("complete_pass") == 1).sum().cast(pl.Int64),
            "incs": ((pl.col("complete_pass") == 0) & (pl.col("interception") == 0))
            .sum()
            .cast(pl.Int64),
            "tds": ((pl.col("touchdown") == 1) & (pl.col("complete_pass") == 1))
            .sum()
            .cast(pl.Int64),
            "ints": (pl.col("interception") == 1).sum().cast(pl.Int64),
            "pass_yards": pl.when(pl.col("complete_pass") == 1)
            .then(pl.col("yards_gained"))
            .otherwise(0)
            .sum()
            .cast(pl.Float64),
        }
        if has_air_yards:
            agg_exprs["air_yards"] = (
                pl.when(pl.col("complete_pass") == 1)
                .then(pl.col("air_yards"))
                .otherwise(0)
                .sum()
                .cast(pl.Float64)
            )
        pass_agg = pass_scope.group_by(_IDENTITY, maintain_order=True).agg(**agg_exprs)

    sack_scope = (
        base.filter(pl.col("sack") == 1)
        .with_columns(_identity_expr(group_col).alias(_IDENTITY))
        .filter(pl.col(_IDENTITY).is_not_null())
    )
    if sack_scope.height == 0:
        sack_agg = pl.DataFrame(schema={_IDENTITY: pl.Utf8, "sacks": pl.Int64})
    else:
        sack_agg = sack_scope.group_by(_IDENTITY, maintain_order=True).agg(
            sacks=pl.len().cast(pl.Int64)
        )

    combined = pass_agg.join(sack_agg, on=_IDENTITY, how="full", coalesce=True)

    fill_zero = ["comps", "incs", "tds", "ints", "pass_yards", "sacks"]
    if has_air_yards:
        fill_zero.append("air_yards")
    combined = combined.with_columns([pl.col(c).fill_null(0) for c in fill_zero])
    if not has_air_yards:
        combined = combined.with_columns(air_yards=pl.lit(None, dtype=pl.Float64))

    combined = combined.with_columns(
        attempts=pl.col("comps") + pl.col("incs") + pl.col("ints")
    ).with_columns(
        comp_pct=pl.when(pl.col("attempts") > 0)
        .then(pl.col("comps") / pl.col("attempts"))
        .otherwise(None),
        ypa=pl.when(pl.col("attempts") > 0)
        .then(pl.col("pass_yards") / pl.col("attempts"))
        .otherwise(None),
        muted=pl.col("attempts") < MUTED_MIN_N,
    )
    return combined


def _run_table(plays: pl.DataFrame, *, group_col: str) -> pl.DataFrame:
    """Carries, Rush Yards, Rush TDs -- `play_type == "run"` only, same resolved identity.
    Never touches any pass column: a 30-yard run for a QB who also passed never changes their
    `attempts`/`ypa`/any other pass-side value (they are computed entirely separately here and
    only combined via the caller's join on identity).
    """
    run_scope = (
        scrimmage_plays(plays)
        .filter(pl.col("play_type") == "run")
        .with_columns(_identity_expr(group_col).alias(_IDENTITY))
        .filter(pl.col(_IDENTITY).is_not_null())
    )
    schema: dict[str, pl.DataType] = {
        _IDENTITY: pl.Utf8,
        "carries": pl.Int64,
        "rush_yards": pl.Float64,
        "rush_tds": pl.Int64,
    }
    if run_scope.height == 0:
        return pl.DataFrame(schema=schema)

    return run_scope.group_by(_IDENTITY, maintain_order=True).agg(
        carries=pl.len().cast(pl.Int64),
        rush_yards=pl.col("yards_gained").sum().cast(pl.Float64),
        rush_tds=(pl.col("touchdown") == 1).sum().cast(pl.Int64),
    )


def _base_table(plays: pl.DataFrame, *, group_col: str) -> pl.DataFrame:
    """The thirteen counting/yardage columns plus `spieler`/`muted` (fifteen columns total),
    outer-joined so a rushing-only QB still appears with null pass columns (their identity never
    occurs in `_pass_and_sack_table`'s scope at all) while a passing-only QB gets real `0`
    carries/rush yards/rush TDs (they are known to have zero rushing plays, not "unknown"),
    mirroring `hc_efficiency_table`'s own `carries` `fill_null(0)` convention.
    """
    pass_and_sack = _pass_and_sack_table(plays, group_col=group_col)
    run = _run_table(plays, group_col=group_col)

    combined = pass_and_sack.join(run, on=_IDENTITY, how="full", coalesce=True)
    combined = combined.with_columns(
        carries=pl.col("carries").fill_null(0),
        rush_yards=pl.col("rush_yards").fill_null(0.0),
        rush_tds=pl.col("rush_tds").fill_null(0),
    )
    return combined.rename({_IDENTITY: "spieler"}).sort("spieler")


@dataclass(frozen=True)
class HcColumnTable:
    """The reproduced `Player Analysis All Camps` table plus its named availability state.

    `unavailable` lists the schema column keys that cannot be computed from the corpus today --
    their values in `table` are null, never a zero or a copy of a neighbouring column.
    `notices` are German sentences naming why. `basis` is the shared
    `reports.aggregate.SectionBasis` for the whole table, not a new per-column convention.

    In this task, the seven delegated columns (`_DELEGATED_COLUMNS`) are unconditionally typed
    null and NOT yet listed in `unavailable`/`notices` -- a following task wires the real
    availability computation (`features.explosiveness` delegation, the `drop`-column gate).
    """

    table: pl.DataFrame
    unavailable: tuple[str, ...]
    notices: tuple[str, ...]
    basis: SectionBasis


def hc_columns_by_qb(plays: pl.DataFrame, *, group_col: str = "thrown_by") -> HcColumnTable:
    """His `Player Analysis All Camps` tab, per QB, from canonical plays.

    Thirteen columns (Comps, Incs, Attempts, TDs, Comp %, INTs, Sacks, Pass Yards, Air Yards,
    YPA, Carries, Rush Yards, Rush TDs) plus `muted` are computed directly from `plays` in this
    task. The seven delegated columns are typed null placeholders here -- a following task wires
    `features.explosiveness` delegation and the `drop`-column availability gate.
    """
    basis = section_basis(scrimmage_plays(plays))

    table = _base_table(plays, group_col=group_col)
    table = table.with_columns(
        [pl.lit(None, dtype=_HC_COLUMN_SCHEMA[c]).alias(c) for c in _DELEGATED_COLUMNS]
    )
    table = table.select(list(_HC_COLUMN_SCHEMA)).sort("spieler")

    return HcColumnTable(table=table, unavailable=(), notices=(), basis=basis)
