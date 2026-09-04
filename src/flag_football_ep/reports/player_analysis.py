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

`m3_columns_by_qb` (M3-04-04) puts our own numbers next to his: Success Rate, calibrated
Explosiveness and the continuous explosiveness score, each with `n`/CI/muted/`shrunk_rate` --
every value read live from `features.explosiveness.definition_comparison`/`explosive_score`
(M3-3's frozen public API), never a second implementation of "explosive" or "success" in this
module. `build_player_analysis_data` is the render-ready assembly: one section per head-coach
camp/competition window (resolved through `reference.resolve_hc_game_splits`, never a row-number
guess), plus the corpus-wide and head-coach-total sections, mirroring `reports/own_team.py`'s
never-raise, name-every-degraded-condition discipline.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from flag_football_ep.config import Config
from flag_football_ep.features.mutations import HC_SOURCE_PREFIX
from flag_football_ep.features.explosiveness import (
    DEFINITIONS,
    HC_PASS_ATTEMPT_SCOPE,
    ExplosivenessCalibration,
    MissingExplosivenessColumns,
    UnknownCalibrationSchema,
    definition_comparison,
    explosive_score,
    hc_efficiency_table,
    hc_workbook_explosive_rate,
    load_calibration,
    scrimmage_plays,
)
from flag_football_ep.reference import (
    MissingReferenceFile,
    load_hc_games,
    load_hc_splits,
    load_player_mapping,
    resolve_hc_game_splits,
)
from flag_football_ep.reports.aggregate import MUTED_MIN_N, SectionBasis, section_basis

# `own_team.py` is READ-ONLY under this plan's file-collision guard (M3-04-04-PLAN.md): these
# two names are imported, never copied or refactored, so player-identity canonicalisation and
# the "missing mapping file" empty-frame shape can never drift between the two reports.
from flag_football_ep.reports.own_team import (
    _EMPTY_MAPPING_SCHEMA,
    _canonicalise_players,
    attach_epa,
)

PLAYER_ANALYSIS_FILENAME: str = "player-analysis.html"


def _is_hc_source() -> pl.Expr:
    """Rows from the head coach's workbooks. The ingest labels them
    `hc_workbook:{file}:{sheet}` (see `mutations.HC_SOURCE_PREFIX`), so an exact
    `== "hc_workbook"` comparison silently matches nothing on the real corpus."""
    return pl.col("source").str.starts_with(HC_SOURCE_PREFIX.rstrip(":"))

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
        plays.filter(_is_hc_source()).height if "source" in plays.columns else 0
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


# --- M3-3 columns beside his (M3-04-04, Task 1) ----------------------------------------------

_M3_DEFINITION_FIELDS: tuple[str, ...] = ("rate", "n", "ci_low", "ci_high", "muted", "shrunk_rate")

_M3_FIELD_DTYPES: dict[str, pl.DataType] = {
    "rate": pl.Float64,
    "n": pl.Int64,
    "ci_low": pl.Float64,
    "ci_high": pl.Float64,
    "muted": pl.Boolean,
    "shrunk_rate": pl.Float64,
}


def _m3_column_schema() -> dict[str, pl.DataType]:
    """`spieler` plus, for every `DEFINITIONS` key, its six fields
    (`{key}_rate`, `{key}_n`, `{key}_ci_low`, `{key}_ci_high`, `{key}_muted`,
    `{key}_shrunk_rate}`), plus `explosive_score_mean` -- the exact column-name shape
    `<interfaces>` specifies (`success_rate_epa_rate`, `success_rate_epa_n`, ...).
    """
    schema: dict[str, pl.DataType] = {"spieler": pl.Utf8}
    for definition in DEFINITIONS:
        for field in _M3_DEFINITION_FIELDS:
            schema[f"{definition.key}_{field}"] = _M3_FIELD_DTYPES[field]
    schema["explosive_score_mean"] = pl.Float64
    return schema


_M3_COLUMN_SCHEMA: dict[str, pl.DataType] = _m3_column_schema()

_MISSING_CALIBRATION_NOTICE_TEMPLATE = (
    "Explosiveness-Kalibrierung nicht gefunden ({detail}); die kalibrierten Spalten bleiben "
    "leer."
)


def load_report_calibration(config: Config) -> tuple[ExplosivenessCalibration | None, tuple[str, ...]]:
    """Resolve `config.paths.reference / "explosiveness" / "calibration.json"` (M3-3's frozen
    artefact).

    Returns `(calibration, ())` on success. A missing file or an `UnknownCalibrationSchema`
    (schema-version drift) both degrade to `(None, (notice,))` -- this function never raises.
    The caller is responsible for gating every M3-3 column on the returned calibration; this
    function only resolves the artefact.
    """
    path = config.paths.reference / "explosiveness" / "calibration.json"
    if not path.exists():
        return None, (_MISSING_CALIBRATION_NOTICE_TEMPLATE.format(detail=str(path)),)
    try:
        return load_calibration(path), ()
    except UnknownCalibrationSchema as exc:
        return None, (_MISSING_CALIBRATION_NOTICE_TEMPLATE.format(detail=str(exc)),)


def m3_columns_by_qb(
    plays: pl.DataFrame,
    *,
    calibration: ExplosivenessCalibration | None,
    group_col: str = "thrown_by",
) -> tuple[pl.DataFrame, tuple[str, ...]]:
    """Our numbers beside his: Success Rate, calibrated Explosiveness and the continuous
    explosiveness score, each with `n`/CI/`muted`/`shrunk_rate`, one row per resolved player
    identity (same `_identity_expr` fallback `hc_columns_by_qb` uses, so the two tables' player
    sets line up for a caller that joins them).

    Every value is read from a single live `definition_comparison(..., calibration=calibration)`
    call over `DEFINITIONS` -- never a second implementation of "explosive" or "success" here
    (M3-3 handoff, `docs/explosiveness-vorschlag.md`). `explosive_score_mean` is the mean of
    `explosive_score(calibration)` over the same scrimmage frame, per player; null for a player
    with no play carrying a real `epa` value (a mean over an all-null series is null, no special
    casing needed). A player whose plays never enter a given definition's scope still gets a row
    for that definition with `n == 0` (`definition_comparison`'s own `group_universe`
    guarantee) -- never a missing row.

    `calibration is None` (see `load_report_calibration`) returns a schema-correct table -- one
    row per identity present in `plays`, every M3-3 column null -- and an empty notice tuple
    (the notice itself is `load_report_calibration`'s job, not repeated here per call).

    Raises nothing beyond what `scrimmage_plays`/`definition_comparison` themselves raise for a
    genuinely malformed frame (missing `play_type`/`down`/`yards_gained`/`epa` entirely) -- a
    frame with zero head-coach rows or zero plays for this split is not malformed, it is handled
    via the empty-table path above.
    """
    base = scrimmage_plays(plays).with_columns(_identity_expr(group_col).alias(_IDENTITY))
    base = base.filter(pl.col(_IDENTITY).is_not_null())

    identities = (
        base.select(_IDENTITY)
        .unique()
        .sort(_IDENTITY)
        .rename({_IDENTITY: "spieler"})
    )

    if calibration is None:
        null_columns = [
            pl.lit(None, dtype=_M3_COLUMN_SCHEMA[col]).alias(col)
            for col in _M3_COLUMN_SCHEMA
            if col != "spieler"
        ]
        table = identities.with_columns(null_columns).select(list(_M3_COLUMN_SCHEMA))
        return table, ()

    comparison = definition_comparison(base, [_IDENTITY], calibration=calibration).rename(
        {_IDENTITY: "spieler"}
    )

    wide = identities
    for definition in DEFINITIONS:
        subset = (
            comparison.filter(pl.col("definition") == definition.key)
            .select(["spieler", *_M3_DEFINITION_FIELDS])
            .rename({field: f"{definition.key}_{field}" for field in _M3_DEFINITION_FIELDS})
        )
        wide = wide.join(subset, on="spieler", how="left")

    scored = base.with_columns(_explosive_score=explosive_score(calibration))
    exp_score = (
        scored.group_by(_IDENTITY, maintain_order=True)
        .agg(explosive_score_mean=pl.col("_explosive_score").mean())
        .rename({_IDENTITY: "spieler"})
    )
    wide = wide.join(exp_score, on="spieler", how="left")

    table = wide.select(list(_M3_COLUMN_SCHEMA)).sort("spieler")
    return table, ()


# --- Split sections and the render-ready report object (M3-04-04, Task 2) --------------------

_KORPUS_SPLIT_KEY = "korpus"
_HC_GESAMT_SPLIT_KEY = "hc-gesamt"
_NA_LABEL_STATUS = "n/a"

_STANDING_OPP_NOTICE = (
    'Pro-Gegner-Splits sind fuer Camp-Spiele nicht moeglich: die Ingest-Schicht setzt fuer '
    'jedes Camp-Spiel ein konstantes away_team = "OPP"; die tatsaechliche Gegner-Identitaet '
    "ist nur ueber das Camp-Label selbst erkennbar (z. B. \"Camp III (vs Switzerland)\")."
)

_SPLIT_REFERENCE_MISSING_NOTICE_TEMPLATE = (
    "Referenzdatei fuer Camp-Splits fehlt ({detail}); nur der Korpus-Gesamt-Abschnitt wird "
    "angezeigt."
)

_SPLIT_CONFLICT_NOTICE_TEMPLATE = (
    "Konflikt bei Split(s) {keys}: derselbe Zeilenbereich traegt im Workbook zwei "
    "unterschiedliche Tab-Namen (Frage 7, M3-04-07) -- noch nicht vom Head Coach entschieden."
)

_MISSING_PLAYER_MAPPING_NOTICE_TEMPLATE = (
    "Spieler-Mapping-Datei fehlt ({path}); nicht zugeordnete Spielernamen koennen nicht "
    "aufgeloest werden."
)


def _empty_split_notice(heading: str) -> str:
    return f"Abschnitt '{heading}': keine Daten im Korpus fuer diesen Abschnitt."


@dataclass(frozen=True)
class PlayerAnalysisSplit:
    """One rendered section: `korpus` (the whole own-team corpus, his "All Camps" tab's
    analogue), `hc-gesamt` (every head-coach-sourced row together, a cross-check subtotal) or
    one `split_key` from `hc_splits.csv` (a named camp/competition window).

    `columns`/`m3_table` are built from the SAME filtered frame via `hc_columns_by_qb`/
    `m3_columns_by_qb` -- never two independently filtered inputs that could silently diverge.
    `empty_notice` is set (and `columns.table`/`m3_table` are empty, schema-correct frames)
    when this section's filtered frame has zero rows -- the section still appears, because
    "this camp has no data in our corpus yet" is information the head coach needs.
    """

    key: str
    heading: str
    label_status: str
    columns: HcColumnTable
    m3_table: pl.DataFrame
    basis: SectionBasis
    empty_notice: str | None


@dataclass(frozen=True)
class PlayerAnalysisReportData:
    """The complete, render-ready `Player Analysis` data object (HC-05), one `PlayerAnalysisSplit`
    per section: `korpus`, `hc-gesamt`, plus one per declared `hc_splits.csv` window.

    `unresolved_games` names every declared head-coach game whose `resolve_hc_game_splits`
    outcome was not `"matched"` (spoofing mitigation T-M3-04-13) -- their plays are counted in
    `hc-gesamt` but in no camp section. `notices` collects every degraded condition (missing
    reference file, missing/unreadable calibration, missing player mapping, unmapped names, the
    standing per-opponent limitation, the Camp IV/VI conflict) as a German sentence, in first-
    occurrence order, deduplicated. Never raises: this object always builds, on today's
    zero-head-coach-row corpus and on tomorrow's, with no code change (must_haves).
    """

    team: str
    splits: tuple[PlayerAnalysisSplit, ...]
    unresolved_games: tuple[tuple[str, str], ...]
    unmapped_players: tuple[str, ...]
    notices: tuple[str, ...]
    n_hc_rows: int
    overall_basis: SectionBasis


def _build_split(
    plays: pl.DataFrame,
    *,
    key: str,
    heading: str,
    label_status: str,
    calibration: ExplosivenessCalibration | None,
    group_col: str,
) -> tuple[PlayerAnalysisSplit, tuple[str, ...]]:
    """Build one `PlayerAnalysisSplit` from `plays` already filtered to this section's scope,
    delegating to `hc_columns_by_qb`/`m3_columns_by_qb` on the identical frame. Returns the
    split plus every notice its two builders produced (the caller aggregates/deduplicates
    across sections).
    """
    columns = hc_columns_by_qb(plays, group_col=group_col)
    m3_table, m3_notices = m3_columns_by_qb(plays, calibration=calibration, group_col=group_col)
    empty_notice = _empty_split_notice(heading) if columns.table.height == 0 else None

    split = PlayerAnalysisSplit(
        key=key,
        heading=heading,
        label_status=label_status,
        columns=columns,
        m3_table=m3_table,
        basis=columns.basis,
        empty_notice=empty_notice,
    )
    return split, tuple(columns.notices) + tuple(m3_notices)


def build_player_analysis_data(
    plays: pl.DataFrame, *, config: Config, scored: pl.DataFrame | None = None
) -> PlayerAnalysisReportData:
    """Assemble the full `PlayerAnalysisReportData` for `config.report.own_team` (HC-05).

    `attach_epa` runs on the FULL, unfiltered `plays` corpus first (same reason
    `build_own_team_data` does -- EPA provenance needs true play-by-play adjacency across both
    teams' snaps), and only the resulting frame is filtered to `posteam == team`. Player
    identities are canonicalised through the maintained mapping the same way `own_team.py`
    does, via its own `_canonicalise_players` (imported, not refactored -- this plan's file
    ownership boundary keeps `own_team.py` read-only).

    Splits: `korpus` (the whole filtered frame, always built), `hc-gesamt` (every
    `source == "hc_workbook"` row, built only when both `hc_games.csv` and `hc_splits.csv`
    load), and one section per `split_key` in `hc_splits.csv`, each filtered to the `game_id`s
    `resolve_hc_game_splits` marked `"matched"` for that key. A missing `hc_games.csv` or
    `hc_splits.csv` degrades to a German notice and a report with only the `korpus` section --
    it never raises and never guesses a split.

    Never raises: a missing reference file, a missing/unreadable calibration, an empty section,
    an unresolved game and an absent own team all become a German notice in `notices`, exactly
    as `build_own_team_data` handles its own degraded conditions.
    """
    team = config.report.own_team
    group_col = "thrown_by"
    notices: list[str] = []

    scored_plays = attach_epa(plays, processed_dir=config.paths.processed, scored=scored)
    offense = scored_plays.filter(pl.col("posteam") == team)

    try:
        mapping = load_player_mapping(config.reference.player_mapping)
    except MissingReferenceFile:
        notices.append(
            _MISSING_PLAYER_MAPPING_NOTICE_TEMPLATE.format(path=config.reference.player_mapping)
        )
        mapping = pl.DataFrame(schema=_EMPTY_MAPPING_SCHEMA)

    canon, unmapped = _canonicalise_players(offense, mapping)
    if unmapped:
        notices.append(f"Nicht zugeordnete Spielernamen: {', '.join(unmapped)}")

    calibration, calibration_notices = load_report_calibration(config)
    notices.extend(calibration_notices)

    n_hc_rows = (
        canon.filter(_is_hc_source()).height if "source" in canon.columns else 0
    )
    overall_basis = section_basis(canon)

    korpus_split, korpus_notices = _build_split(
        canon,
        key=_KORPUS_SPLIT_KEY,
        heading="Alle Camps (Korpus gesamt)",
        label_status=_NA_LABEL_STATUS,
        calibration=calibration,
        group_col=group_col,
    )
    notices.extend(korpus_notices)
    splits: list[PlayerAnalysisSplit] = [korpus_split]

    unresolved_games: tuple[tuple[str, str], ...] = ()

    try:
        games = load_hc_games(config.reference.hc_games)
        hc_splits_ref = load_hc_splits(config.reference.hc_splits)
    except MissingReferenceFile as exc:
        notices.append(_SPLIT_REFERENCE_MISSING_NOTICE_TEMPLATE.format(detail=str(exc)))
    else:
        resolved = resolve_hc_game_splits(games, hc_splits_ref)
        unresolved_games = tuple(
            (row["game_id"], row["split_match"])
            for row in resolved.filter(pl.col("split_match") != "matched").iter_rows(named=True)
        )

        hc_gesamt_frame = (
            canon.filter(_is_hc_source())
            if "source" in canon.columns
            else canon.filter(pl.lit(False))
        )
        hc_gesamt_split, hc_gesamt_notices = _build_split(
            hc_gesamt_frame,
            key=_HC_GESAMT_SPLIT_KEY,
            heading="Head Coach Workbook gesamt (alle Camps)",
            label_status=_NA_LABEL_STATUS,
            calibration=calibration,
            group_col=group_col,
        )
        notices.extend(hc_gesamt_notices)
        splits.append(hc_gesamt_split)

        conflict_keys = sorted(
            hc_splits_ref.filter(pl.col("label_status") == "conflict")["split_key"].to_list()
        )
        if conflict_keys:
            notices.append(
                _SPLIT_CONFLICT_NOTICE_TEMPLATE.format(keys=", ".join(conflict_keys))
            )

        for split_row in hc_splits_ref.sort("first_row").iter_rows(named=True):
            split_key = split_row["split_key"]
            matched_game_ids = (
                resolved.filter(
                    (pl.col("split_key") == split_key) & (pl.col("split_match") == "matched")
                )["game_id"].to_list()
            )
            camp_frame = canon.filter(pl.col("game_id").is_in(matched_game_ids))
            camp_split, camp_notices = _build_split(
                camp_frame,
                key=split_key,
                heading=split_row["label_de"],
                label_status=split_row["label_status"],
                calibration=calibration,
                group_col=group_col,
            )
            notices.extend(camp_notices)
            splits.append(camp_split)

    notices.append(_STANDING_OPP_NOTICE)
    if offense.height == 0:
        notices.append(f"Keine Offense-Plays fuer {team!r} im Korpus gefunden.")

    return PlayerAnalysisReportData(
        team=team,
        splits=tuple(splits),
        unresolved_games=unresolved_games,
        unmapped_players=tuple(unmapped),
        notices=tuple(dict.fromkeys(notices)),
        n_hc_rows=n_hc_rows,
        overall_basis=overall_basis,
    )
