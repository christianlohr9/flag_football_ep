"""Reproduces the head coach's `Player Analysis All Camps` tab (HC-05) from canonical plays.

Every column below is pinned verbatim to its workbook formula cell (`M3-04-RESEARCH.md`
Pattern 1, sheet `Player Analysis All Camps`, row 2 as the representative formula row), not
re-derived from what a column name suggests. Two denominator corrections landed in M3-04-01
before this module was written (`Attempts` excludes Sacks; `Efficiency`'s denominator is
`Attempts + Carries`, not `Attempts + Drops`) -- both are consumed here via
`features.explosiveness`'s corrected public API, never recomputed locally.

The one rule that governs this module: a column that cannot be computed from the corpus today
is named, never approximated. Three of his columns need a canonical `drop` column that may not
carry real signal yet (`Adj Comp %`, `adj Pass Yards`, `adj YPA`); one needs a hand-charted
`efficiency` extra (`Efficiency`); one drops a subtraction term whose source column's meaning is
undocumented (`Air Yards`, `Data!Y`, header literally `"B"` -- Frage 8). Every one of these is
either computed honestly or named in `HcColumnTable.unavailable` with a German sentence in
`HcColumnTable.notices` -- never a silent zero, a dash, or a copy of a neighbouring column
(REP-D01: differences are shown, not hidden).

Availability is judged on REAL SIGNAL, not merely on column presence: `plays_scored.parquet`
already carries `drop`/`efficiency` as typed-null NULLABLE_EXTRAS columns even with zero
head-coach rows in the corpus (`conform_to_canonical` materialises every extra unconditionally).
Treating "column present" as "available" would silently compute `Adj Comp % == Comp %` and
`Efficiency == 0.0` for every player today -- exactly the "copy of a neighbouring column"/
silent-zero failure this module exists to prevent. `_drop_available`/`_efficiency_available`
therefore check for at least one real (non-null, and for `drop`, non-blank) value before ever
delegating, which is a small, deliberate extension beyond a plain
`"column" in plays.columns` check.

There is no WR/receiver table on this tab (verified in M3-04-RESEARCH: columns Z onward and the
rows below the last QB row are empty) -- this module is QB-row-only, matching his own sheet.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from flag_football_ep.features.explosiveness import (
    HC_PASS_ATTEMPT_SCOPE,
    MissingExplosivenessColumns,
    hc_efficiency_table,
    hc_workbook_explosive_rate,
    scrimmage_plays,
)
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

# Internal row-key column name used while building the per-identity tables below; renamed to
# `spieler` (the schema's own key column) before any table leaves this module.
_IDENTITY = "_hc_identity"

_DROP_UNAVAILABLE_NOTICE = (
    "Adj Comp % / adj Pass Yards / adj YPA nicht verfügbar: die Drop-Spalte (Data!W) ist noch "
    "nicht kanonisch."
)
_EFFICIENCY_UNAVAILABLE_NOTICE = (
    "Efficiency nicht verfügbar: die handgechartete Spalte Data!O ist noch nicht im Korpus."
)
_AIR_YARDS_DEVIATION_NOTICE = (
    'Air Yards ohne den Abzugsterm aus Data!Y (Spaltenkopf "B", Bedeutung ungeklärt — Frage 8); '
    "unsere Zahl kann daher leicht über seiner liegen."
)


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


def _drop_flag_expr() -> pl.Expr:
    """His `COUNTIFS(..., Data!W, "*")` non-blank-text wildcard, reproduced as: `drop`
    non-null and non-empty after whitespace stripping. Never derived from any other column
    (RESEARCH Pitfall 2) -- an `Adj Comp %` numerically identical to `Comp %` is the exact
    failure this guards against. Assumes `"drop"` is present in the frame; callers check
    `_drop_available` first.
    """
    return pl.col("drop").is_not_null() & (pl.col("drop").cast(pl.Utf8).str.strip_chars() != "")


def _drop_available(plays: pl.DataFrame) -> bool:
    """True only when `drop` is present with at least one real (non-blank) flagged row -- see
    the module docstring's "Availability is judged on REAL SIGNAL" note.
    """
    if "drop" not in plays.columns:
        return False
    flagged = plays.select(_drop_flag_expr().any()).item()
    return bool(flagged)


def _efficiency_available(plays: pl.DataFrame) -> bool:
    """True only when `efficiency` is present with at least one non-null value -- same
    present-but-all-null trap as `_drop_available`. `hc_efficiency_table` itself only checks
    column presence (`MissingExplosivenessColumns` fires on a literally absent column), so this
    module adds the real-signal check before ever delegating rather than trusting a
    present-but-empty column to mean "computed".
    """
    if "efficiency" not in plays.columns:
        return False
    present = plays.select(pl.col("efficiency").is_not_null().any()).item()
    return bool(present)


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


def _dropped_aggregates(plays: pl.DataFrame, *, group_col: str) -> pl.DataFrame:
    """Per-identity dropped-incompletion count (`Adj Comp %` cell `G2`'s own numerator term:
    completions plus incompletions with a non-blank Drop) and dropped-row air-yards sum
    (`adj Pass Yards` cell `N2`'s own literal formula sums `air_yards` on every row flagged
    dropped, with NO completion-status restriction at all -- reproduced exactly, not narrowed to
    incompletions only). Only called once `_drop_available` is true.
    """
    base = (
        scrimmage_plays(plays)
        .filter(HC_PASS_ATTEMPT_SCOPE)
        .with_columns(_identity_expr(group_col).alias(_IDENTITY))
        .filter(pl.col(_IDENTITY).is_not_null())
    )
    has_air_yards = "air_yards" in plays.columns
    flag = _drop_flag_expr()

    schema: dict[str, pl.DataType] = {
        _IDENTITY: pl.Utf8,
        "dropped_incompletions": pl.Int64,
        "dropped_air_yards": pl.Float64,
    }
    if base.height == 0:
        return pl.DataFrame(schema=schema).rename({_IDENTITY: "spieler"})

    agg_exprs = {
        "dropped_incompletions": (
            (pl.col("complete_pass") == 0) & (pl.col("interception") == 0) & flag
        )
        .sum()
        .cast(pl.Int64),
    }
    if has_air_yards:
        agg_exprs["dropped_air_yards"] = (
            pl.when(flag).then(pl.col("air_yards")).otherwise(0).sum().cast(pl.Float64)
        )

    result = base.group_by(_IDENTITY, maintain_order=True).agg(**agg_exprs)
    if not has_air_yards:
        result = result.with_columns(dropped_air_yards=pl.lit(0.0, dtype=pl.Float64))
    return result.rename({_IDENTITY: "spieler"})


@dataclass(frozen=True)
class HcColumnTable:
    """The reproduced `Player Analysis All Camps` table plus its named availability state.

    `unavailable` lists the schema column keys (e.g. `"efficiency"`, `"adj_comp_pct"`) that
    cannot be computed from the corpus today -- their values in `table` are null, never a zero
    or a copy of a neighbouring column. `notices` are German sentences: one per unavailable
    group, plus the Air-Yards subtraction-term deviation and the head-coach-row corpus count,
    always present regardless of availability (a corpus notice naming zero HC rows is itself a
    legitimate, expressible answer). `basis` is the shared `reports.aggregate.SectionBasis` for
    the whole table, not a new per-column convention.
    """

    table: pl.DataFrame
    unavailable: tuple[str, ...]
    notices: tuple[str, ...]
    basis: SectionBasis


def hc_columns_by_qb(plays: pl.DataFrame, *, group_col: str = "thrown_by") -> HcColumnTable:
    """His `Player Analysis All Camps` tab, per QB, from canonical plays.

    Thirteen columns (Comps, Incs, Attempts, TDs, Comp %, INTs, Sacks, Pass Yards, Air Yards,
    YPA, Carries, Rush Yards, Rush TDs) plus `muted` are always computed directly from `plays`.
    `exp_plays`/`explosive_pct` are joined from `features.explosiveness.hc_workbook_explosive_rate`
    verbatim -- never a second implementation of "explosive" in this module.
    `efficiency`/`efficiency_drops` are joined from `hc_efficiency_table` inside a
    `MissingExplosivenessColumns` guard (mirrors `reports/build.py`'s per-product isolation,
    applied here per column), gated additionally on `_efficiency_available`'s real-signal check.
    `adj_comp_pct`/`adj_pass_yards`/`adj_ypa` are computed only when `_drop_available` is true.
    Every column that cannot be computed is null in `table` and named in `.unavailable`, never
    a silent zero or a copy of an unadjusted neighbour.

    Raises nothing: `plays` missing `sack`/`play_type`/`down`/`yards_gained` propagates
    `MissingExplosivenessColumns` from `scrimmage_plays` (a genuinely malformed input, not a
    "data not here yet" case this module's own availability handling covers).
    """
    unavailable: list[str] = []
    notices: list[str] = []

    basis = section_basis(scrimmage_plays(plays))

    table = _base_table(plays, group_col=group_col)

    has_air_yards = "air_yards" in plays.columns
    if has_air_yards:
        notices.append(_AIR_YARDS_DEVIATION_NOTICE)

    hc_rows = (
        plays.filter(pl.col("source") == "hc_workbook").height if "source" in plays.columns else 0
    )
    notices.append(
        f"{hc_rows} Zeile(n) im Korpus stammen aus der Quelle 'hc_workbook' (0 ist eine "
        "gültige Antwort)."
    )

    # Exp Plays / Explosive % -- delegated verbatim, never re-derived (M3-3 handoff).
    exp = hc_workbook_explosive_rate(plays, group_col=group_col).rename({group_col: "spieler"})
    table = table.join(
        exp.select(["spieler", "exp_plays", "explosive_pct"]), on="spieler", how="left"
    )

    # Efficiency / efficiency_drops.
    drop_available = _drop_available(plays)
    drops_flag = _drop_flag_expr() if drop_available else None

    if _efficiency_available(plays):
        try:
            eff = hc_efficiency_table(plays, group_col=group_col, drops_flag=drops_flag)
            eff = eff.rename({group_col: "spieler"}).select(
                ["spieler", "efficiency", "efficiency_drops"]
            )
            table = table.join(eff, on="spieler", how="left")
            if not drop_available:
                unavailable.append("efficiency_drops")
        except MissingExplosivenessColumns:
            table = table.with_columns(
                efficiency=pl.lit(None, dtype=pl.Float64),
                efficiency_drops=pl.lit(None, dtype=pl.Float64),
            )
            unavailable.extend(["efficiency", "efficiency_drops"])
            notices.append(_EFFICIENCY_UNAVAILABLE_NOTICE)
    else:
        table = table.with_columns(
            efficiency=pl.lit(None, dtype=pl.Float64),
            efficiency_drops=pl.lit(None, dtype=pl.Float64),
        )
        unavailable.extend(["efficiency", "efficiency_drops"])
        notices.append(_EFFICIENCY_UNAVAILABLE_NOTICE)

    # Adj Comp % / adj Pass Yards / adj YPA.
    if drop_available:
        dropped = _dropped_aggregates(plays, group_col=group_col)
        table = (
            table.join(dropped, on="spieler", how="left")
            .with_columns(
                dropped_incompletions=pl.col("dropped_incompletions").fill_null(0),
                dropped_air_yards=pl.col("dropped_air_yards").fill_null(0.0),
            )
            .with_columns(
                adj_comp_pct=pl.when(
                    pl.col("attempts").is_not_null() & (pl.col("attempts") > 0)
                )
                .then((pl.col("comps") + pl.col("dropped_incompletions")) / pl.col("attempts"))
                .otherwise(None),
                adj_pass_yards=pl.when(pl.col("pass_yards").is_not_null())
                .then(pl.col("pass_yards") + pl.col("dropped_air_yards"))
                .otherwise(None),
            )
        )
        table = table.with_columns(
            adj_ypa=pl.when(pl.col("attempts").is_not_null() & (pl.col("attempts") > 0))
            .then(pl.col("adj_pass_yards") / pl.col("attempts"))
            .otherwise(None)
        ).drop(["dropped_incompletions", "dropped_air_yards"])
    else:
        table = table.with_columns(
            adj_comp_pct=pl.lit(None, dtype=pl.Float64),
            adj_pass_yards=pl.lit(None, dtype=pl.Float64),
            adj_ypa=pl.lit(None, dtype=pl.Float64),
        )
        unavailable.extend(["adj_comp_pct", "adj_pass_yards", "adj_ypa"])
        notices.append(_DROP_UNAVAILABLE_NOTICE)

    table = table.select(list(_HC_COLUMN_SCHEMA)).sort("spieler")

    return HcColumnTable(
        table=table,
        unavailable=tuple(sorted(set(unavailable))),
        notices=tuple(notices),
        basis=basis,
    )
