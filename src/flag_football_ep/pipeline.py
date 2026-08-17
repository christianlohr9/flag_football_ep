"""Ingest orchestration: four sources -> one validated canonical frame (REQ-S1-05, REQ-S1-06).

`run_ingest` is the single convergence point for every raw source dispatched by
`ffep ingest`: it loads the contract and reference data exactly once, dispatches
each requested source through its own `ingest/*` entry point inside a per-source
try/except (so one broken source never aborts the run), concatenates the already-
conformed per-source frames with a strict vertical concat (a schema mismatch here
is a bug, not something to paper over with a diagonal concat mode), and runs the shared
validation gate (`validation.checks.run_checks` + `partition_games`) to split
accepted from quarantined rows.

The "sportapp" source folds in two independent inputs, matching the
re-derive-with-fallback decision recorded at the 01.2-11 plan checkpoint: fresh
snapshots under `config.paths.raw_sportapp` (re-derived once `SPORTAPP_API_KEY`
is rotated and `ffep fetch-sportapp` has run) plus the grandfathered WC24 CSV at
`data/raw/legacy/wc24_pbp.csv` (`config.paths.raw_legacy / "wc24_pbp.csv"`,
read via `ingest.sportapp.read_mutated_sportapp_snapshot`, stamped
`source="legacy-sportapp"`). Both branches are attempted independently
whenever "sportapp" is requested; today only the grandfathered branch has data
on disk (the sportapp.fi API key rotation is deferred per STATE.md).

Artifact writes (the atomic Parquet pair plus the Markdown report) are added by
a later task of this same plan.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Sequence

import polars as pl

from flag_football_ep.canonical import CANONICAL_COLUMNS, CORE_COLUMNS, NULLABLE_EXTRAS, make_game_id
from flag_football_ep.config import Config
from flag_football_ep.ingest.hudl import ingest_dir
from flag_football_ep.ingest.ifaf import ingest_snapshots as ingest_ifaf_snapshots
from flag_football_ep.ingest.legacy import ingest_legacy
from flag_football_ep.ingest.sportapp import read_mutated_sportapp_snapshot
from flag_football_ep.ingest.sportapp import ingest_snapshots as ingest_sportapp_snapshots
from flag_football_ep.reference import load_final_scores, load_half_boundaries, load_team_mapping
from flag_football_ep.validation.checks import GameResult, partition_games, run_checks
from flag_football_ep.validation.schema import Contract, load_contract

__all__ = ["IngestResult", "run_ingest"]

# Fixed dispatch order (per <interfaces> in the plan): hudl, legacy, sportapp, ifaf.
_KNOWN_SOURCES: tuple[str, ...] = ("hudl", "legacy", "sportapp", "ifaf")

# "legacy" (data_raw.csv, 47 hand-charted games) and "legacy-sportapp" (the
# grandfathered WC24 CSV) both ran through pre-port/legacy mutation code that
# cannot be re-validated at source -- FAILs are downgraded to WARN, never
# quarantined. See 01.2-11-SUMMARY.md's "Pipeline wiring note for plan 14".
_WARN_ONLY_SOURCES: frozenset[str] = frozenset({"legacy", "legacy-sportapp"})

_ALL_CANONICAL_DTYPES: dict[str, pl.DataType] = {**CORE_COLUMNS, **NULLABLE_EXTRAS}


@dataclass
class IngestResult:
    """The outcome of one `run_ingest` call.

    `notices` carries source-level messages that are not tied to any single
    game_id (an empty/missing source directory, a source-level exception) --
    an addition beyond the `<interfaces>` shape declared by `cli.py`, additive
    only, so callers that only use the documented fields are unaffected.
    """

    run_id: str
    plays_path: Path
    games_path: Path
    report_path: Path
    n_plays: int
    n_games: int
    n_quarantined: int
    game_results: list[GameResult] = field(default_factory=list)
    notices: list[str] = field(default_factory=list)


def _empty_canonical_frame() -> pl.DataFrame:
    """A zero-row frame already conforming to `CANONICAL_COLUMNS`."""
    return pl.DataFrame(schema=dict(_ALL_CANONICAL_DTYPES)).select(list(CANONICAL_COLUMNS))


def _ingest_hudl(
    hudl_dir: Path,
    contract: Contract,
    half_boundaries: pl.DataFrame,
    team_mapping: pl.DataFrame,
) -> tuple[list[pl.DataFrame], list[str], dict[str, list[str]]]:
    """Dispatch the Hudl source.

    Per-file structural errors (bad filename, missing core column) are already
    caught inside `ingest_dir` and returned as a `df=None` entry with a notice;
    this wrapper only guards against an unexpected exception from the dispatch
    call itself, so a single broken source can never abort the whole run.
    """
    frames: list[pl.DataFrame] = []
    source_notices: list[str] = []
    game_notices: dict[str, list[str]] = {}

    if not hudl_dir.exists() or not any(hudl_dir.glob("*.csv")):
        source_notices.append(f"hudl: source directory {hudl_dir} is empty or missing, skipping")
        return frames, source_notices, game_notices

    try:
        results = ingest_dir(hudl_dir, contract, half_boundaries, team_mapping)
    except Exception as exc:  # noqa: BLE001 -- recorded as a notice, never swallowed
        source_notices.append(f"hudl: {type(exc).__name__}: {exc}")
        return frames, source_notices, game_notices

    for meta, df, notices in results:
        if df is not None:
            frames.append(df)
        if notices.messages:
            game_notices[meta.game_id] = list(notices.messages)

    return frames, source_notices, game_notices


def _ingest_legacy(
    legacy_path: Path, team_mapping: pl.DataFrame
) -> tuple[list[pl.DataFrame], list[str], dict[str, list[str]]]:
    """Dispatch the grandfathered `data_raw.csv` source (47 games, one file)."""
    frames: list[pl.DataFrame] = []
    source_notices: list[str] = []
    game_notices: dict[str, list[str]] = {}

    if not legacy_path.exists():
        source_notices.append(f"legacy: {legacy_path} not found, skipping")
        return frames, source_notices, game_notices

    try:
        df, notices = ingest_legacy(legacy_path, team_mapping)
    except Exception as exc:  # noqa: BLE001
        source_notices.append(f"legacy: {type(exc).__name__}: {exc}")
        return frames, source_notices, game_notices

    frames.append(df)
    if notices.messages:
        source_notices.extend(f"legacy: {m}" for m in notices.messages)

    return frames, source_notices, game_notices


def _ingest_sportapp(
    sportapp_dir: Path, wc24_path: Path, team_mapping: pl.DataFrame
) -> tuple[list[pl.DataFrame], list[str], dict[str, list[str]]]:
    """Dispatch the sportapp.fi source: fresh snapshots plus the grandfathered
    WC24 CSV fallback (re-derive-with-fallback, 01.2-11 checkpoint decision).

    Both branches are attempted independently -- a failure in one does not
    block the other, and both can contribute rows in the same run.
    """
    frames: list[pl.DataFrame] = []
    source_notices: list[str] = []
    game_notices: dict[str, list[str]] = {}

    has_fresh = sportapp_dir.exists() and any(sportapp_dir.glob("match-drives_*.json"))
    has_grandfathered = wc24_path.exists()

    if not has_fresh and not has_grandfathered:
        source_notices.append(
            f"sportapp: no fresh snapshots in {sportapp_dir} and no grandfathered "
            f"{wc24_path}, skipping"
        )
        return frames, source_notices, game_notices

    if has_fresh:
        try:
            results = ingest_sportapp_snapshots(sportapp_dir, team_mapping)
        except Exception as exc:  # noqa: BLE001
            source_notices.append(f"sportapp: {type(exc).__name__}: {exc}")
            results = []
        for game_id, df, notices in results:
            canonical_id = make_game_id("sportapp", game_id)
            if df.height:
                frames.append(df)
            if notices:
                game_notices[canonical_id] = list(notices)
    else:
        source_notices.append(f"sportapp: no fresh snapshots found in {sportapp_dir}")

    if has_grandfathered:
        try:
            legacy_df = read_mutated_sportapp_snapshot(wc24_path)
        except Exception as exc:  # noqa: BLE001
            source_notices.append(f"legacy-sportapp: {type(exc).__name__}: {exc}")
        else:
            frames.append(legacy_df)
            source_notices.append(
                f"legacy-sportapp: loaded grandfathered {wc24_path} ({legacy_df.height} rows)"
            )

    return frames, source_notices, game_notices


def _ingest_ifaf(
    ifaf_dir: Path, team_mapping: pl.DataFrame
) -> tuple[list[pl.DataFrame], list[str], dict[str, list[str]]]:
    """Dispatch the IFAF/cpx.studio source."""
    frames: list[pl.DataFrame] = []
    source_notices: list[str] = []
    game_notices: dict[str, list[str]] = {}

    if not ifaf_dir.exists() or not any(ifaf_dir.glob("unified-plays_*.json")):
        source_notices.append(f"ifaf: source directory {ifaf_dir} is empty or missing, skipping")
        return frames, source_notices, game_notices

    try:
        results = ingest_ifaf_snapshots(ifaf_dir, team_mapping)
    except Exception as exc:  # noqa: BLE001
        source_notices.append(f"ifaf: {type(exc).__name__}: {exc}")
        return frames, source_notices, game_notices

    for gid, df, notices in results:
        canonical_id = make_game_id("ifaf", gid)
        if df.height:
            frames.append(df)
        if notices.skipped:
            source_notices.append(f"ifaf/{canonical_id}: skipped - {notices.skip_reason}")
        if notices.messages:
            game_notices[canonical_id] = list(notices.messages)

    return frames, source_notices, game_notices


def run_ingest(
    config: Config,
    sources: Sequence[str],
    out_dir: Path | None = None,
    strict: bool = False,
) -> IngestResult:
    """Ingest every requested source into one validated canonical frame.

    Order of operations: validate `sources` against the four known names ->
    generate `run_id` once -> load the contract and reference data once ->
    dispatch each requested source in fixed order (hudl, legacy, sportapp,
    ifaf), each inside its own try/except -> strict vertical concat (a schema
    mismatch across already-conformed frames is a bug, never papered over with
    a diagonal concat mode) -> `run_checks` + `partition_games` -> return
    `IngestResult`.

    `strict` only affects the caller's exit-code decision (`cli.ingest`,
    wired in a later task); it never changes what gets computed here.
    """
    unknown = [s for s in sources if s not in _KNOWN_SOURCES]
    if unknown:
        raise ValueError(
            f"unknown source name(s) {unknown}; valid sources are {list(_KNOWN_SOURCES)}"
        )

    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")

    contract = load_contract(config.paths.contract)
    half_boundaries = load_half_boundaries(config.reference.half_boundaries)
    final_scores = load_final_scores(config.reference.final_scores)
    team_mapping = load_team_mapping(config.reference.team_mapping)

    frames: list[pl.DataFrame] = []
    notices: list[str] = []
    game_notices: dict[str, list[str]] = {}

    if "hudl" in sources:
        f, n, gn = _ingest_hudl(config.paths.raw_hudl, contract, half_boundaries, team_mapping)
        frames.extend(f)
        notices.extend(n)
        game_notices.update(gn)

    if "legacy" in sources:
        f, n, gn = _ingest_legacy(config.paths.raw_legacy / "data_raw.csv", team_mapping)
        frames.extend(f)
        notices.extend(n)
        game_notices.update(gn)

    if "sportapp" in sources:
        # `data/raw/legacy/wc24_pbp.csv` -- the grandfathered WC24 fallback.
        wc24_path = config.paths.raw_legacy / "wc24_pbp.csv"  # data/raw/legacy/
        f, n, gn = _ingest_sportapp(config.paths.raw_sportapp, wc24_path, team_mapping)
        frames.extend(f)
        notices.extend(n)
        game_notices.update(gn)

    if "ifaf" in sources:
        f, n, gn = _ingest_ifaf(config.paths.raw_ifaf, team_mapping)
        frames.extend(f)
        notices.extend(n)
        game_notices.update(gn)

    non_empty = [frame for frame in frames if frame.height > 0]
    combined = pl.concat(non_empty, how="vertical") if non_empty else _empty_canonical_frame()

    check_results = run_checks(combined, final_scores=final_scores)
    accepted, _quarantined_df, game_results = partition_games(
        combined, check_results, warn_only_sources=_WARN_ONLY_SOURCES
    )
    n_quarantined = sum(1 for g in game_results if g.quarantined)

    effective_out_dir = out_dir if out_dir is not None else config.paths.processed
    plays_path = effective_out_dir / "plays.parquet"
    games_path = effective_out_dir / "games.parquet"
    report_path = effective_out_dir / f"validation-report-{run_id}.md"

    return IngestResult(
        run_id=run_id,
        plays_path=plays_path,
        games_path=games_path,
        report_path=report_path,
        n_plays=accepted.height,
        n_games=len(game_results),
        n_quarantined=n_quarantined,
        game_results=game_results,
        notices=notices,
    )
