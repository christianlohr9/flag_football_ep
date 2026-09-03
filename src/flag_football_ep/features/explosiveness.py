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
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from flag_football_ep.reports.aggregate import MUTED_MIN_N, rate_table  # noqa: F401

# --- Shared scope filter -------------------------------------------------------------------

# The run/pass tuple that defines a real scrimmage snap -- everything that is not a PAT
# (down == 0), a no_play, a kickoff or any other special-teams row.
SCRIMMAGE_PLAY_TYPES: tuple[str, ...] = ("run", "pass")

# Literal workbook value from the `COUNTIFS(Data!J, ">12")` cell (Player Analysis All Camps!
# R2:S2, docs/explosiveness-recherche.md). Strictly greater -- 12 itself is NOT explosive.
HC_EXPLOSIVE_YARDS_THRESHOLD = 12

# Documents why canonical `play_type == "pass"` equals the workbook's per-QB `Attempts`
# denominator (`Comps+Incs+Sacks`): verified directly from `ingest/hudl.py` -- a sack maps to
# `play_type == "pass"`, and a `Dropped` / `Batted Down` / `Block` RESULT token also maps to
# `play_type == "pass"` and sets `incomplete_pass`. So filtering canonical `play_type ==
# "pass"` reproduces the workbook's pass-attempt denominator without re-deriving it from
# `complete_pass`/`sack` flags individually.
HC_PASS_ATTEMPT_FILTER = (
    "canonical play_type == 'pass' == workbook Attempts (Comps+Incs+Sacks): sacks and "
    "Dropped/Batted Down/Block RESULT tokens all map to play_type == 'pass' in "
    "ingest/hudl.py, matching the workbook's per-QB Attempts definition verbatim."
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


# --- Head-coach baselines -------------------------------------------------------------------


def hc_workbook_explosive_rate(
    plays: pl.DataFrame, *, group_col: str = "thrown_by"
) -> pl.DataFrame:
    """Literal reproduction of the workbook's `Explosive %` formula (RESEARCH Pattern 1,
    CONTEXT EXP-D01).

    Source (`docs/explosiveness-recherche.md` sec. "Die HC's eigenen Formeln",
    `Player Analysis All Camps!R2:S2`, read via `openpyxl(data_only=False)`):

        ExpPlays   = COUNTIFS(Data!P, <QB>, Data!J, ">12")
        Explosive% = ExpPlays / Attempts          # Attempts = Comps+Incs+Sacks, pass-only

    Filters to `play_type == "pass"` on top of `scrimmage_plays` -- `HC_PASS_ATTEMPT_FILTER`
    documents why canonical `play_type == "pass"` equals the workbook's Attempts denominator.
    A 30-yard run for the same QB never changes `n` or `exp_plays` here (RESEARCH Pitfall 3):
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
    working = scrimmage_plays(plays).filter(pl.col("play_type") == "pass")
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

    Same pass-only scope and group-identity fallback as `hc_workbook_explosive_rate`. A play
    is a success under `(yards_gained > HC_EXPLOSIVE_YARDS_THRESHOLD) | (epa > 0)`. Requires
    `epa` (`scrimmage_plays(..., require_epa=True)`). Returns one row per group with `n`,
    `successes`, `rate`; a schema-correct empty frame on empty input.
    """
    working = scrimmage_plays(plays, require_epa=True).filter(pl.col("play_type") == "pass")
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
    CONTEXT EXP-D04):

        Efficiency = SUMIF(Data!P, <QB>, Data!O) / (Attempts + Drops)

    `Data!O` (the canonical `efficiency` extra) is treated as an opaque, manually-charted
    per-play input, never re-derived: three plausible down/distance/yards formulas were
    tested against the real charted values and none reached 80% agreement (RESEARCH Pitfall
    2, Assumption A1) -- shipping a guessed formula here is forbidden. Charted values outside
    `{0, 1}` (e.g. an observed outlier `9`) are summed as-is into `efficiency_sum` and
    separately counted in `out_of_domain`, so the anomaly is visible rather than silently
    clipped.

    `attempts` is the pass-attempt count from the same scope as `hc_workbook_explosive_rate`.
    A null charted value counts in the denominator (via `attempts`) and contributes 0 to the
    numerator. `drops_flag` is an optional `pl.Expr` counted into `drops` and added to
    `attempts` to form `denominator` -- whether the workbook's `Incs` already includes
    `Drops` is RESEARCH Open Question 2 for the head coach (the Attempts-plus-Drops
    ambiguity), so this is an explicit argument, never a resolved default. Raises
    `MissingExplosivenessColumns` naming `efficiency` when the column is absent -- HC-charted
    rows are not in the corpus yet.
    """
    _require_columns(
        plays,
        ["efficiency"],
        context="hc_efficiency_table",
        note="HC-charted rows are not in the corpus yet",
    )

    working = scrimmage_plays(plays).filter(pl.col("play_type") == "pass")
    working, key = _with_group_key(working, group_col)
    working = working.filter(pl.col(key).is_not_null())

    schema = {
        group_col: pl.Utf8,
        "efficiency_sum": pl.Int64,
        "attempts": pl.Int64,
        "drops": pl.Int64,
        "denominator": pl.Int64,
        "efficiency": pl.Float64,
        "out_of_domain": pl.Int64,
    }
    if working.height == 0:
        return pl.DataFrame(schema=schema)

    working = working.with_columns(
        _eff_numerator=pl.col("efficiency").fill_null(0),
        _out_of_domain=(
            pl.col("efficiency").is_not_null() & ~pl.col("efficiency").is_in([0, 1])
        ),
        _drop=(drops_flag.cast(pl.Int32) if drops_flag is not None else pl.lit(0)),
    )

    result = (
        working.group_by(key, maintain_order=True)
        .agg(
            efficiency_sum=pl.col("_eff_numerator").sum().cast(pl.Int64),
            attempts=pl.len().cast(pl.Int64),
            drops=pl.col("_drop").sum().cast(pl.Int64),
            out_of_domain=pl.col("_out_of_domain").sum().cast(pl.Int64),
        )
        .with_columns(denominator=pl.col("attempts") + pl.col("drops"))
        .with_columns(efficiency=pl.col("efficiency_sum") / pl.col("denominator"))
        .rename({key: group_col})
        .sort(group_col)
    )
    return result.select(
        [
            group_col,
            "efficiency_sum",
            "attempts",
            "drops",
            "denominator",
            "efficiency",
            "out_of_domain",
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
