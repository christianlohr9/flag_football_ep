"""Explosiveness and Efficiency metrics over canonical plays (HC-04, Phase M3-3).

This module's one job: reproduce the head coach's own numbers FIRST, exactly as his workbook
and his spoken rule compute them, then offer researched alternatives on the same plays --
never the other way around. Every formula pinned here is cited verbatim from
`docs/explosiveness-recherche.md` (the German research artifact, itself read from the
workbook's formula cells via `openpyxl(data_only=False)`), not re-derived or guessed.

Two discrepancies are computed here on purpose, never silently resolved:

1. The head coach's workbook formula (`Player Analysis All Camps!R2:S2`,
   `hc_workbook_explosive_rate`) checks ONLY `yards_gained > 12` on pass attempts -- no EPA
   term anywhere in the formula chain. His spoken rule to the user (`docs/hc-notes-2026-09-03.md`,
   "mehr als 12 Yards und/oder positive EPA") is a DIFFERENT rule
   (`hc_verbal_explosive_rate`), which his workbook does not implement. Both are computed and
   reported side by side (RESEARCH Pitfall 1) -- neither silently stands in for the other.
2. Efficiency (success, `epa > 0`) and Explosiveness (EPA-magnitude on successful plays) are
   two different numbers with two different statistical properties (Connelly's Success
   Rate/IsoPPP split, RESEARCH Summary) -- never one conflated metric.

Every rate this module produces carries its `n`, a Clopper-Pearson confidence interval and a
`muted` flag, reusing `flag_football_ep.reports.aggregate.rate_table` rather than a second
convention (RESEARCH `Don't Hand-Roll`). `down == 0` (PAT) rows can never enter any rate here
(RESEARCH Pitfall 5) -- every function routes through `scrimmage_plays` first.

`MissingExplosivenessColumns` is this module's ASVS-V5 fail-loud control (RESEARCH Security
Domain): a required column is never allowed to arrive as a silent null and quietly deflate a
rate. See `.planning/phases/M3-03-explosiveness-efficiency/M3-03-RESEARCH.md` for the full
literature review and `.planning/phases/M3-03-explosiveness-efficiency/M3-03-CONTEXT.md` for
the locked EXP-D01..EXP-D05 decisions this module implements.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from flag_football_ep.reports.aggregate import MUTED_MIN_N, rate_table

# --- Shared scope filter -------------------------------------------------------------------

# The run/pass tuple that defines a real scrimmage snap -- everything that is not a PAT
# (down == 0), a no_play, a kickoff or any other special-teams row.
SCRIMMAGE_PLAY_TYPES: tuple[str, ...] = ("run", "pass")

# Literal workbook value from the `COUNTIFS(Data!J, ">12")` cell (Player Analysis All Camps!
# R2:S2, docs/explosiveness-recherche.md). Strictly greater -- 12 itself is NOT explosive.
HC_EXPLOSIVE_YARDS_THRESHOLD = 12

# The head coach's own Attempts scope, read directly from the formula cell (M3-04-01
# correction, 2026-09-04; M3-04-RESEARCH Pattern 1, "Attempts" row): workbook cell
# `Player Analysis All Camps!D2` = `B2 + C2 + H2` (Comps + Incs + INTs). Sacks live in a
# separate column (`I`) and are NEVER summed into `D2`. Canonical `play_type == "pass"`
# includes sack rows (a sack maps to `play_type == "pass"` in `ingest/hudl.py`), so this
# scope subtracts them explicitly rather than reusing the plain pass filter. `sack` is
# validated (never a silent null) by `_hc_pass_attempts` before this expression runs.
HC_PASS_ATTEMPT_SCOPE: pl.Expr = (pl.col("play_type") == "pass") & (
    pl.col("sack").fill_null(0) != 1
)

# Prose form of `HC_PASS_ATTEMPT_SCOPE` for docstrings/scope notes. Corrects this module's
# own prior "Comps+Incs+Sacks" reading (pre-M3-04-01): that claim was never checked against
# the workbook's actual `D2` formula cell and is wrong -- Sacks (`I2`) are a separate column,
# never added into `D2`.
HC_PASS_ATTEMPT_FILTER = (
    "Workbook cell `Player Analysis All Camps!D2` = B2+C2+H2 (Comps+Incs+INTs). Sacks (`I2`) "
    "are a SEPARATE column and are NEVER added into D2 -- read directly from the formula cell "
    "(M3-04-RESEARCH Pattern 1). Canonical play_type == 'pass' includes sack rows (a sack "
    "maps to play_type == 'pass' in ingest/hudl.py), so HC_PASS_ATTEMPT_SCOPE subtracts them "
    "explicitly. Corrects this module's own prior 'Comps+Incs+Sacks' reading, in force before "
    "M3-04-01's correction (2026-09-04), which was never checked against the workbook's "
    "actual D2 formula cell."
)


class MissingExplosivenessColumns(ValueError):
    """Raised when a required column is absent from the input frame.

    Mirrors `MissingFeatureColumns` in `features/mutations.py`: this is the module's ASVS-V5
    fail-loud control -- a required column must never be allowed to arrive as a silent null
    and quietly deflate a downstream rate. The message names every missing column (sorted)
    plus the frame's actual columns.
    """


def _require_columns(
    df: pl.DataFrame, required: Sequence[str], *, context: str, note: str | None = None
) -> None:
    """Raise `MissingExplosivenessColumns` naming every column in `required` absent from
    `df`, sorted, plus the frame's actual columns (also sorted). `note`, when given, is
    appended as extra context (e.g. why the column is currently absent). Raises nothing when
    every required column is present.
    """
    missing = sorted(c for c in required if c not in df.columns)
    if missing:
        suffix = f" ({note})" if note else ""
        raise MissingExplosivenessColumns(
            f"{context}: missing required column(s): {', '.join(missing)} "
            f"(frame columns: {', '.join(sorted(df.columns))}){suffix}"
        )


def _with_group_key(df: pl.DataFrame, group_col: str) -> tuple[pl.DataFrame, str]:
    """Return `(frame, key)` where `key` is the column name to `group_by` on.

    For the default `group_col == "thrown_by"`, adds a `_hc_group` column coalescing
    `thrown_by` onto `qb` -- the same QB-identity fallback
    `reports/own_team.py::player_efficiency` uses -- and returns that name. For any other
    `group_col`, the frame is returned unchanged and `group_col` itself is the key.
    """
    if group_col == "thrown_by":
        return (
            df.with_columns(_hc_group=pl.coalesce([pl.col("thrown_by"), pl.col("qb")])),
            "_hc_group",
        )
    return df, group_col


def scrimmage_plays(plays: pl.DataFrame, *, require_epa: bool = False) -> pl.DataFrame:
    """Restrict to real scrimmage snaps: `play_type` in `{"run", "pass"}` and `down` 1-4.

    This is the module's one shared entry filter (RESEARCH Pitfall 5): `down == 0` is a
    legitimate numeric value (a PAT attempt), not a null, so a plain
    `pl.col("down").is_not_null()` filter does NOT exclude it. Every rate function in this
    module routes through here first -- none re-derives its own scrimmage filter.

    Validates required columns *before* filtering: `play_type`, `down`, `yards_gained`
    always, plus `epa` when `require_epa` is True. Raises `MissingExplosivenessColumns`
    naming every missing column rather than letting a required column arrive as a silent
    null that would quietly deflate a downstream rate.
    """
    required = ["play_type", "down", "yards_gained"]
    if require_epa:
        required.append("epa")
    _require_columns(plays, required, context="scrimmage_plays")

    return plays.filter(
        pl.col("play_type").is_in(SCRIMMAGE_PLAY_TYPES) & pl.col("down").is_between(1, 4)
    )


def _hc_pass_attempts(plays: pl.DataFrame, *, require_epa: bool = False) -> pl.DataFrame:
    """Restrict to the head coach's own Attempts scope (`HC_PASS_ATTEMPT_SCOPE`): pass plays
    minus sack rows, per workbook cell `Player Analysis All Camps!D2` (M3-04-01 correction).

    Every HC-scoped function in this module (`hc_workbook_explosive_rate`,
    `hc_verbal_explosive_rate`, `hc_efficiency_table`, the `baseline_hc_*` `DEFINITIONS`
    entries) routes through this one helper rather than keeping a local
    `play_type == "pass"` filter, so a future scope correction only needs to change one
    place. Validates `sack` up front via `_require_columns` -- a missing `sack` column fails
    loud rather than silently letting sack rows count as Attempts. Applies `scrimmage_plays`
    first (so `down`/`yards_gained`/`play_type` are validated too), then
    `HC_PASS_ATTEMPT_SCOPE`.
    """
    _require_columns(plays, ["sack"], context="_hc_pass_attempts")
    working = scrimmage_plays(plays, require_epa=require_epa)
    return working.filter(HC_PASS_ATTEMPT_SCOPE)


# --- Head-coach baselines -------------------------------------------------------------------


def hc_workbook_explosive_rate(
    plays: pl.DataFrame, *, group_col: str = "thrown_by"
) -> pl.DataFrame:
    """Literal reproduction of the workbook's `Explosive %` formula (RESEARCH Pattern 1,
    CONTEXT EXP-D01).

    Source (`docs/explosiveness-recherche.md` sec. "Die HC's eigenen Formeln",
    `Player Analysis All Camps!R2:S2`, read via `openpyxl(data_only=False)`):

        ExpPlays   = COUNTIFS(Data!P, <QB>, Data!J, ">12")
        Explosive% = ExpPlays / Attempts     # Attempts (D2) = Comps+Incs+INTs, no Sacks
                                              # (M3-04-01 correction, 2026-09-04)

    Filters via `_hc_pass_attempts` (`HC_PASS_ATTEMPT_SCOPE`) on top of `scrimmage_plays` --
    `HC_PASS_ATTEMPT_FILTER` documents why this scope, not the plain `play_type == "pass"`
    filter, reproduces the workbook's own `D2` Attempts denominator (sack rows excluded). A
    30-yard run for the same QB never changes `n` or `exp_plays` here (RESEARCH Pitfall 3):
    the workbook's per-QB Attempts denominator never counted rushing plays, and this function
    reproduces that scope exactly, not a team-wide blend of run and pass.

    Deliberately touches no EPA column anywhere in this function -- an EPA term here would be
    a re-interpretation of the head coach's spoken rule (see `hc_verbal_explosive_rate`), not
    a reproduction of what his workbook actually computes. The workbook rule and the spoken
    rule are two different, separately labelled numbers on purpose (RESEARCH Pitfall 1); this
    function is the workbook one. Whether the missing EPA term is a deliberate choice or a
    spreadsheet gap is RESEARCH Open Question 1 for the head coach -- this code does not
    resolve that silently.

    `group_col` defaults to `"thrown_by"`, coalesced onto `qb` when `thrown_by` is null --
    the same QB-identity fallback `reports/own_team.py::player_efficiency` uses. Rows with a
    null resolved group value are excluded. Returns one row per group with `n`, `exp_plays`,
    `explosive_pct`, sorted by `group_col`; a schema-correct empty frame on empty input.
    """
    working = _hc_pass_attempts(plays)
    working, key = _with_group_key(working, group_col)
    working = working.filter(pl.col(key).is_not_null())

    schema = {
        group_col: pl.Utf8,
        "n": pl.Int64,
        "exp_plays": pl.Int64,
        "explosive_pct": pl.Float64,
    }
    if working.height == 0:
        return pl.DataFrame(schema=schema)

    result = (
        working.group_by(key, maintain_order=True)
        .agg(
            n=pl.len().cast(pl.Int64),
            exp_plays=(pl.col("yards_gained") > HC_EXPLOSIVE_YARDS_THRESHOLD)
            .sum()
            .cast(pl.Int64),
        )
        .with_columns(explosive_pct=pl.col("exp_plays") / pl.col("n"))
        .rename({key: group_col})
        .sort(group_col)
    )
    return result.select([group_col, "n", "exp_plays", "explosive_pct"])


def hc_verbal_explosive_rate(
    plays: pl.DataFrame, *, group_col: str = "thrown_by"
) -> pl.DataFrame:
    """Reproduces the head coach's *spoken* rule from `docs/hc-notes-2026-09-03.md`
    ("mehr als 12 Yards und/oder positive EPA"), which -- unlike `hc_workbook_explosive_rate`
    -- his workbook formula does not implement (RESEARCH Pitfall 1, Open Question 1). The two
    are computed here as separate, separately labelled functions and are always reported side
    by side, never merged into one silently-chosen number.

    Same `_hc_pass_attempts` scope and group-identity fallback as `hc_workbook_explosive_rate`.
    A play is a success under `(yards_gained > HC_EXPLOSIVE_YARDS_THRESHOLD) | (epa > 0)`.
    Requires `epa` (`_hc_pass_attempts(..., require_epa=True)`). Returns one row per group
    with `n`, `successes`, `rate`; a schema-correct empty frame on empty input.
    """
    working = _hc_pass_attempts(plays, require_epa=True)
    working, key = _with_group_key(working, group_col)
    working = working.filter(pl.col(key).is_not_null())

    schema = {group_col: pl.Utf8, "n": pl.Int64, "successes": pl.Int64, "rate": pl.Float64}
    if working.height == 0:
        return pl.DataFrame(schema=schema)

    result = (
        working.group_by(key, maintain_order=True)
        .agg(
            n=pl.len().cast(pl.Int64),
            successes=(
                (pl.col("yards_gained") > HC_EXPLOSIVE_YARDS_THRESHOLD) | (pl.col("epa") > 0)
            )
            .sum()
            .cast(pl.Int64),
        )
        .with_columns(rate=pl.col("successes") / pl.col("n"))
        .rename({key: group_col})
        .sort(group_col)
    )
    return result.select([group_col, "n", "successes", "rate"])


def hc_efficiency_table(
    plays: pl.DataFrame,
    *,
    group_col: str = "thrown_by",
    drops_flag: pl.Expr | None = None,
) -> pl.DataFrame:
    """Literal reproduction of the workbook's `Efficiency` formula (RESEARCH Pattern 1,
    M3-04-01 correction, 2026-09-04):

        Efficiency = SUMIF(Data!P, <QB>, Data!O) / (D2 + W2)
        # D2 = Attempts (Comps+Incs+INTs, no Sacks), W2 = Carries (same sheet, play_type=="run")

    Cell `U2`'s own denominator is Attempts **plus Carries** -- both same-sheet cells -- NOT
    Attempts plus Drops, which was this module's prior (pre-M3-04-01) assumption. `carries` is
    the same group's rushing-play count, `play_type == "run"`, from the same scrimmage frame
    (workbook cell `W2`), computed with the same `group_col` identity fallback as `attempts`.
    `denominator = attempts + carries` and `efficiency = efficiency_sum / denominator` are the
    PRIMARY reading -- the one that matches the workbook's own `U2` formula.

    `Data!O` (the canonical `efficiency` extra) is treated as an opaque, manually-charted
    per-play input, never re-derived: three plausible down/distance/yards formulas were
    tested against the real charted values and none reached 80% agreement (RESEARCH Pitfall
    2, Assumption A1) -- shipping a guessed formula here is forbidden. Charted values outside
    `{0, 1}` (e.g. an observed outlier `9`) are summed as-is into `efficiency_sum` and
    separately counted in `out_of_domain`, so the anomaly is visible rather than silently
    clipped. A null charted value counts in the denominator (via `attempts`) and contributes 0
    to the numerator.

    `drops_flag` is an optional `pl.Expr`, kept as the clearly-labelled SECOND reading: when
    given, it is counted into `drops`, and `denominator_drops = attempts + drops` /
    `efficiency_drops = efficiency_sum / denominator_drops` are emitted alongside the primary
    `denominator`/`efficiency`. `drops`/`denominator_drops`/`efficiency_drops` are null when
    `drops_flag is None`. This second reading exists because the earlier "Attempts + Drops"
    wording came from a different sheet's formula, not from this tab's own `U2` cell -- whether
    the head coach intended the rushing denominator this tab's formula actually computes, or
    whether the tab's own formula is itself inconsistent with a stated Drops-based intent, is
    an open question for the head coach (M3-04-07 turns it into a written question); this
    function does not resolve it, it only keeps both readings computable and separately named.

    Raises `MissingExplosivenessColumns` naming `efficiency` when the column is absent --
    HC-charted rows are not in the corpus yet -- and (via `_hc_pass_attempts`) naming `sack`
    when that column is absent.
    """
    _require_columns(
        plays,
        ["efficiency"],
        context="hc_efficiency_table",
        note="HC-charted rows are not in the corpus yet",
    )

    working = _hc_pass_attempts(plays)
    working, key = _with_group_key(working, group_col)
    working = working.filter(pl.col(key).is_not_null())

    schema = {
        group_col: pl.Utf8,
        "efficiency_sum": pl.Int64,
        "attempts": pl.Int64,
        "carries": pl.Int64,
        "denominator": pl.Int64,
        "efficiency": pl.Float64,
        "out_of_domain": pl.Int64,
        "drops": pl.Int64,
        "denominator_drops": pl.Int64,
        "efficiency_drops": pl.Float64,
    }
    if working.height == 0:
        return pl.DataFrame(schema=schema)

    working = working.with_columns(
        _eff_numerator=pl.col("efficiency").fill_null(0),
        _out_of_domain=(
            pl.col("efficiency").is_not_null() & ~pl.col("efficiency").is_in([0, 1])
        ),
    )
    if drops_flag is not None:
        working = working.with_columns(_drop=drops_flag.cast(pl.Int32))

    agg_exprs = {
        "efficiency_sum": pl.col("_eff_numerator").sum().cast(pl.Int64),
        "attempts": pl.len().cast(pl.Int64),
        "out_of_domain": pl.col("_out_of_domain").sum().cast(pl.Int64),
    }
    if drops_flag is not None:
        agg_exprs["drops"] = pl.col("_drop").sum().cast(pl.Int64)

    attempts_agg = working.group_by(key, maintain_order=True).agg(**agg_exprs)
    if drops_flag is None:
        attempts_agg = attempts_agg.with_columns(drops=pl.lit(None, dtype=pl.Int64))

    carries_working = scrimmage_plays(plays).filter(pl.col("play_type") == "run")
    carries_working, carries_key = _with_group_key(carries_working, group_col)
    carries_working = carries_working.filter(pl.col(carries_key).is_not_null())
    carries_agg = (
        carries_working.group_by(carries_key, maintain_order=True)
        .agg(carries=pl.len().cast(pl.Int64))
        .rename({carries_key: key})
    )

    result = (
        attempts_agg.join(carries_agg, on=key, how="left")
        .with_columns(carries=pl.col("carries").fill_null(0))
        .with_columns(denominator=pl.col("attempts") + pl.col("carries"))
        .with_columns(efficiency=pl.col("efficiency_sum") / pl.col("denominator"))
    )
    if drops_flag is not None:
        result = result.with_columns(
            denominator_drops=pl.col("attempts") + pl.col("drops")
        ).with_columns(efficiency_drops=pl.col("efficiency_sum") / pl.col("denominator_drops"))
    else:
        result = result.with_columns(
            denominator_drops=pl.lit(None, dtype=pl.Int64),
            efficiency_drops=pl.lit(None, dtype=pl.Float64),
        )

    result = result.rename({key: group_col}).sort(group_col)
    return result.select(
        [
            group_col,
            "efficiency_sum",
            "attempts",
            "carries",
            "denominator",
            "efficiency",
            "out_of_domain",
            "drops",
            "denominator_drops",
            "efficiency_drops",
        ]
    )


# --- Corpus-calibrated explosiveness ---------------------------------------------------------

DEFAULT_EPA_QUANTILE = 0.80
# RESEARCH Assumption A2: q80 of successful-play EPA (~+2.3 on our corpus) is a documented
# design choice calibrated on OUR corpus, not a borrowed external standard -- Sam Hoppen's own
# EPA>1.0 NFL cutoff is likewise self-described as landing "around the 80th percentile", not
# derived from first principles. CONTEXT explicitly delegates the exact level to discretion.

MIN_CALIBRATION_PLAYS = 10
# Below ten successful plays a q80 estimate is dominated by one or two extreme observations;
# ten is the working floor at which the quantile is determined by interpolation between order
# statistics rather than collapsing onto the single largest value -- a documented design
# choice (RESEARCH ties the exact bound to discretion), not an external standard.

CALIBRATION_SCHEMA_VERSION = 1


class InsufficientCalibrationSample(ValueError):
    """Raised by `calibrate` when fewer than `MIN_CALIBRATION_PLAYS` successful plays are
    available -- a threshold nobody should trust must never be returned silently.
    """


class UnknownCalibrationSchema(ValueError):
    """Raised by `load_calibration` when the JSON's `schema_version` does not match
    `CALIBRATION_SCHEMA_VERSION` -- fields may have moved or changed meaning between schema
    versions, so an unknown version is never read as if it were current.
    """


@dataclass(frozen=True)
class ExplosivenessCalibration:
    """A versioned, corpus-fingerprinted explosiveness threshold (RESEARCH Pattern 3).

    The threshold is derived from data, so the data it was derived from has to travel with
    it: a report that renders "explosive" must be able to name the corpus that defined the
    word. `corpus_fingerprint` is a sha256 over the sorted `(game_id, play_id, epa,
    yards_gained)` tuples of the corpus `calibrate` ran on; `corpus_sources` names the
    canonical `source` values included. Mirrors `data/reference/hackathon_freeze.json`'s
    plainness: a small versioned JSON artifact with a fingerprint and a timestamp.
    """

    schema_version: int
    epa_quantile: float
    epa_threshold: float
    epa_median_success: float
    epa_iqr_success: float
    corpus_n: int
    n_success: int
    corpus_sources: tuple[str, ...]
    corpus_fingerprint: str
    calibrated_on: str


def _corpus_fingerprint(working: pl.DataFrame) -> str:
    """sha256 over the sorted `(game_id, play_id, epa, yards_gained)` tuples of `working` --
    changes when a single value changes, stable across repeated calls on the same frame.
    """
    rows = (
        working.select(["game_id", "play_id", "epa", "yards_gained"])
        .sort(["game_id", "play_id", "epa", "yards_gained"])
        .to_dicts()
    )
    payload = "|".join(
        f"{row['game_id']},{row['play_id']},{row['epa']},{row['yards_gained']}" for row in rows
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def calibrate(
    plays: pl.DataFrame, *, epa_quantile: float = DEFAULT_EPA_QUANTILE
) -> ExplosivenessCalibration:
    """Calibrate an `ExplosivenessCalibration` from the corpus (RESEARCH Pattern 3,
    IsoPPP-style).

    The quantile is computed over successful plays only (`epa > 0`) via `pl.Expr.quantile`
    (native polars, never a manual sort-and-index -- RESEARCH `Don't Hand-Roll`); appending
    any number of non-successful plays never changes `epa_threshold`. Raises
    `InsufficientCalibrationSample` below `MIN_CALIBRATION_PLAYS` successes rather than
    returning a threshold nobody should trust.
    """
    working = scrimmage_plays(plays, require_epa=True)
    corpus_n = working.height
    success = working.filter(pl.col("epa") > 0)
    n_success = success.height

    if n_success < MIN_CALIBRATION_PLAYS:
        raise InsufficientCalibrationSample(
            f"calibrate: only {n_success} successful play(s) available, need at least "
            f"{MIN_CALIBRATION_PLAYS} to trust a q{epa_quantile:.2f} threshold"
        )

    stats = success.select(
        epa_threshold=pl.col("epa").quantile(epa_quantile),
        epa_median_success=pl.col("epa").median(),
        _q25=pl.col("epa").quantile(0.25),
        _q75=pl.col("epa").quantile(0.75),
    ).row(0, named=True)

    sources: tuple[str, ...] = ()
    if "source" in working.columns:
        sources = tuple(sorted(working["source"].drop_nulls().unique().to_list()))

    return ExplosivenessCalibration(
        schema_version=CALIBRATION_SCHEMA_VERSION,
        epa_quantile=epa_quantile,
        epa_threshold=float(stats["epa_threshold"]),
        epa_median_success=float(stats["epa_median_success"]),
        epa_iqr_success=float(stats["_q75"] - stats["_q25"]),
        corpus_n=corpus_n,
        n_success=n_success,
        corpus_sources=sources,
        corpus_fingerprint=_corpus_fingerprint(working),
        calibrated_on=datetime.now(timezone.utc).isoformat(),
    )


def write_calibration(calibration: ExplosivenessCalibration, path: Path | str) -> None:
    """Write `calibration` as `json.dumps(..., indent=2)` with `schema_version` first --
    mirrors `data/reference/hackathon_freeze.json`'s plainness. Creates parent directories.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(calibration), indent=2, sort_keys=False) + "\n")


def load_calibration(path: Path | str) -> ExplosivenessCalibration:
    """Round-trip the JSON `write_calibration` produces. Raises `UnknownCalibrationSchema`
    when `schema_version` does not match `CALIBRATION_SCHEMA_VERSION`, before touching any
    other field -- an unknown version's fields may have moved.
    """
    payload = json.loads(Path(path).read_text())
    if payload.get("schema_version") != CALIBRATION_SCHEMA_VERSION:
        raise UnknownCalibrationSchema(
            f"load_calibration: unknown schema_version {payload.get('schema_version')!r} in "
            f"{path} (expected {CALIBRATION_SCHEMA_VERSION})"
        )
    return ExplosivenessCalibration(
        schema_version=payload["schema_version"],
        epa_quantile=payload["epa_quantile"],
        epa_threshold=payload["epa_threshold"],
        epa_median_success=payload["epa_median_success"],
        epa_iqr_success=payload["epa_iqr_success"],
        corpus_n=payload["corpus_n"],
        n_success=payload["n_success"],
        corpus_sources=tuple(payload["corpus_sources"]),
        corpus_fingerprint=payload["corpus_fingerprint"],
        calibrated_on=payload["calibrated_on"],
    )


def success_flag() -> pl.Expr:
    """`epa > 0` (nflverse convention, RESEARCH Pattern 2) -- `epa == 0` is NOT a success."""
    return pl.col("epa") > 0


def explosive_epa_flag(calibration: ExplosivenessCalibration) -> pl.Expr:
    """`(epa > 0) & (epa >= calibration.epa_threshold)` -- the Kandidat-B rate (RESEARCH
    Pattern 3). Inclusive at the threshold. The cutoff moves from a yard boundary (the head
    coach's objection) to an EPA boundary that already carries down, distance, field position
    and score context -- answering the objection instead of relocating it.
    """
    return (pl.col("epa") > 0) & (pl.col("epa") >= calibration.epa_threshold)


_MIN_EXPLOSIVE_SCORE_SCALE = 0.1
# Guards a zero-IQR calibration (a corpus whose successful plays happen to share an identical
# inter-quartile EPA) from a divide-by-zero; 0.1 EPA is small enough that the resulting score
# stays steep around the threshold rather than becoming artificially flat.


def explosive_score(calibration: ExplosivenessCalibration) -> pl.Expr:
    """Continuous score in (0, 1): a standardised-EPA logistic, `1 / (1 + exp(-z))` with
    `z = (epa - epa_threshold) / iqr_success` (RESEARCH `Don't Hand-Roll` -- a plain polars
    expression rather than importing `scipy.stats.logistic`). Monotone increasing in `epa`;
    `epa == epa_threshold` maps to exactly 0.5, so "explosive" is the same statement in both
    the binary and the continuous world.

    This is what makes the 11-vs-12-yard cliff the user objected to ("was ist, wenn eine
    Spielerin nur 11 Yards erzielt?") measurably disappear: two plays with near-identical
    `epa` get near-identical scores here, regardless of yardage.
    """
    scale = (
        calibration.epa_iqr_success
        if calibration.epa_iqr_success > 0
        else _MIN_EXPLOSIVE_SCORE_SCALE
    )
    z = (pl.col("epa") - calibration.epa_threshold) / scale
    return 1 / (1 + (-z).exp())


# --- Definition comparison rollup ------------------------------------------------------------


@dataclass(frozen=True)
class MetricDefinition:
    """One row of `DEFINITIONS`: a named, data-driven metric definition rather than a
    hand-written call site, so the German proposal document and the comparison table cannot
    drift apart (RESEARCH `Don't Hand-Roll`, Pattern 1).
    """

    key: str
    label_de: str
    scope: pl.Expr
    flag_factory: Callable[[ExplosivenessCalibration], pl.Expr]
    scope_note: str
    requires: tuple[str, ...]


DEFINITIONS: tuple[MetricDefinition, ...] = (
    MetricDefinition(
        key="baseline_hc_workbook",
        label_de="HC-Workbook Explosive % (Yards > 12, nur Pass)",
        scope=HC_PASS_ATTEMPT_SCOPE,
        flag_factory=lambda _cal: pl.col("yards_gained") > HC_EXPLOSIVE_YARDS_THRESHOLD,
        scope_note=(
            "Nenner: Pass-Attempts nach Workbook-Formel D2 = Comps + Incs + INTs "
            "(ohne Sacks)."
        ),
        requires=("play_type", "yards_gained", "sack"),
    ),
    MetricDefinition(
        key="baseline_hc_verbal",
        label_de="HC mündliche Regel (Yards > 12 oder EPA > 0, nur Pass)",
        scope=HC_PASS_ATTEMPT_SCOPE,
        flag_factory=lambda _cal: (
            (pl.col("yards_gained") > HC_EXPLOSIVE_YARDS_THRESHOLD) | (pl.col("epa") > 0)
        ),
        scope_note=(
            "Nenner: Pass-Attempts nach Workbook-Formel D2 = Comps + Incs + INTs "
            "(ohne Sacks), wie im HC-Workbook (mündliche Regel)."
        ),
        requires=("play_type", "yards_gained", "epa", "sack"),
    ),
    MetricDefinition(
        key="success_rate_epa",
        label_de="Success Rate (EPA > 0)",
        scope=pl.lit(True),
        flag_factory=lambda _cal: pl.col("epa") > 0,
        scope_note="Nenner: alle Scrimmage-Plays (Lauf + Pass).",
        requires=("epa",),
    ),
    MetricDefinition(
        key="explosive_epa_magnitude",
        label_de="Explosiveness (EPA-Magnitude auf Erfolgen)",
        scope=pl.lit(True),
        flag_factory=explosive_epa_flag,
        scope_note="Nenner: alle Scrimmage-Plays (Lauf + Pass).",
        requires=("epa",),
    ),
)


PRIOR_STRENGTH = 10.0
# Beta-binomial shrinkage prior strength (CONTEXT EXP-D05): the number of attempts at which a
# player's own observed rate and the pooled corpus rate for that definition carry equal
# weight in `shrink_rate`. Ten mirrors the imported MUTED_MIN_N's order of magnitude -- a
# player with roughly twice that many attempts is already about half-weighted toward their
# own number.


def shrink_rate(successes, n, prior_rate: float, prior_strength: float):
    """Beta-binomial posterior mean: `(successes + prior_strength * prior_rate) / (n +
    prior_strength)`. `successes`/`n` may be Python numbers, a `pl.Expr` or a `pl.Series` --
    the arithmetic is polymorphic over all three. A proposal offered ALONGSIDE the
    established `rate`/`ci_low`/`ci_high`/`muted` convention (CONTEXT EXP-D05) -- those remain
    the authoritative honest-reporting mechanism; no report may print a shrunk rate without
    `n`.
    """
    return (successes + prior_strength * prior_rate) / (n + prior_strength)


_DEFINITION_COMPARISON_SCHEMA_EXTRA: dict[str, pl.DataType] = {
    "label": pl.Utf8,
    "definition": pl.Utf8,
    "label_de": pl.Utf8,
    "scope_note": pl.Utf8,
    "n": pl.Int64,
    "successes": pl.Int64,
    "rate": pl.Float64,
    "ci_low": pl.Float64,
    "ci_high": pl.Float64,
    "muted": pl.Boolean,
    "shrunk_rate": pl.Float64,
}


def definition_comparison(
    plays: pl.DataFrame,
    group_cols: Sequence[str],
    *,
    calibration: ExplosivenessCalibration,
    definitions: tuple[MetricDefinition, ...] = DEFINITIONS,
    prior_strength: float = PRIOR_STRENGTH,
) -> pl.DataFrame:
    """One row per (group, definition), reusing `rate_table` (never a second Clopper-Pearson
    call site, RESEARCH `Don't Hand-Roll`): the `rate_table` columns (`n`, `successes`,
    `rate`, `ci_low`/`ci_high`, `muted`) unchanged, plus `definition`, `label_de`,
    `scope_note` and `shrunk_rate`.

    Every known group (any non-null `group_cols` combination present anywhere in the
    scrimmage corpus) is represented for every definition, even when that definition's scope
    excludes the group entirely -- the row still appears with `n == 0` rather than being
    dropped. `shrunk_rate` is null when a definition's pooled rate cannot be computed (zero
    in-scope plays for that definition across the whole corpus). Returns a schema-correct
    empty frame on empty input.
    """
    group_cols = list(group_cols)
    output_columns = [
        *group_cols,
        *_DEFINITION_COMPARISON_SCHEMA_EXTRA,
    ]

    if plays.height == 0:
        schema = {col: pl.Utf8 for col in group_cols}
        schema.update(_DEFINITION_COMPARISON_SCHEMA_EXTRA)
        return pl.DataFrame(schema=schema)

    group_universe = (
        scrimmage_plays(plays, require_epa=True)
        .filter(pl.all_horizontal([pl.col(c).is_not_null() for c in group_cols]))
        .select(group_cols)
        .unique()
        .sort(group_cols)
    )

    tables = []
    for definition in definitions:
        scoped = scrimmage_plays(plays, require_epa="epa" in definition.requires).filter(
            definition.scope
        )
        flag = definition.flag_factory(calibration)
        table = rate_table(scoped, group_cols, flag)

        joined = (
            group_universe.join(table, on=group_cols, how="left")
            .with_columns(
                n=pl.col("n").fill_null(0),
                successes=pl.col("successes").fill_null(0),
                label=pl.concat_str(
                    [pl.col(c).cast(pl.Utf8) for c in group_cols], separator=" / "
                ),
            )
            .with_columns(muted=pl.col("n") < MUTED_MIN_N)
        )

        pooled_rate = (
            int(scoped.select(flag.cast(pl.Int32).sum()).item()) / scoped.height
            if scoped.height > 0
            else None
        )
        joined = joined.with_columns(
            definition=pl.lit(definition.key),
            label_de=pl.lit(definition.label_de),
            scope_note=pl.lit(definition.scope_note),
        )
        joined = joined.with_columns(
            shrunk_rate=(
                shrink_rate(pl.col("successes"), pl.col("n"), pooled_rate, prior_strength)
                if pooled_rate is not None
                else pl.lit(None, dtype=pl.Float64)
            )
        )
        tables.append(joined.select(output_columns))

    return pl.concat(tables, how="vertical")


def cliff_zone_table(plays: pl.DataFrame, *, window: tuple[int, int] = (8, 16)) -> pl.DataFrame:
    """Per-`yards_gained` counts and shares around the head-coach cutoff, plus a boolean
    `hc_explosive` column that flips exactly between 12 and 13 -- the table the German
    proposal renders to make the 10-12 "cliff zone" visible (RESEARCH Summary: 11.5% of all
    plays sit in this window) instead of merely asserting it.

    One row per integer `yards_gained` value in `window` (inclusive), always the full range
    regardless of gaps in the data -- a yard value with zero observed plays still appears
    with `n == 0` rather than a missing row. `share` is `n / total_scrimmage_n`; null when
    the corpus has zero scrimmage plays.
    """
    low, high = window
    working = scrimmage_plays(plays)
    total_n = working.height

    counts = (
        working.filter(pl.col("yards_gained").is_between(low, high))
        .group_by("yards_gained", maintain_order=True)
        .agg(n=pl.len().cast(pl.Int64))
    )
    full = pl.DataFrame(
        {"yards_gained": list(range(low, high + 1))}, schema={"yards_gained": pl.Int32}
    )
    merged = (
        full.join(counts, on="yards_gained", how="left")
        .with_columns(n=pl.col("n").fill_null(0))
        .with_columns(
            share=(pl.col("n") / total_n if total_n > 0 else pl.lit(None, dtype=pl.Float64)),
            hc_explosive=pl.col("yards_gained") > HC_EXPLOSIVE_YARDS_THRESHOLD,
        )
        .sort("yards_gained")
    )
    return merged.select(["yards_gained", "n", "share", "hc_explosive"])
