"""Ingest orchestration: five sources -> one validated canonical frame (REQ-S1-05, REQ-S1-06).

`run_ingest` is the single convergence point for every raw source dispatched by
`ffep ingest`: it loads the contract and reference data exactly once, dispatches
each requested source through its own `ingest/*` entry point inside a per-source
try/except (so one broken source never aborts the run), concatenates the already-
conformed per-source frames with a strict vertical concat (a schema mismatch here
is a bug, not something to paper over with a diagonal concat mode), and runs the shared
validation gate (`validation.checks.run_checks` + `partition_games`) to split
accepted from quarantined rows.

The "hc_workbook" source (HC-01/HC-02, plan M3-01-04) dispatches last, after
hudl/legacy/sportapp/ifaf are already concatenated into `corpus_combined`:
`ingest.hc_dedupe.dedupe_hc_rows` excludes head-coach rows that duplicate a
declared partner game (`data/reference/hc_games.csv`'s `corpus_game_id`
column, HC-D03) before the deduped HC rows join the rest. `hc_workbook` is
deliberately not in `_WARN_ONLY_SOURCES` (HC-D05): a failing HC game is
quarantined with reasons like any other new-format source.

The "sportapp" source folds in two independent inputs, matching the
re-derive-with-fallback decision recorded at the 01.2-11 plan checkpoint: fresh
snapshots under `config.paths.raw_sportapp` (re-derived once `SPORTAPP_API_KEY`
is rotated and `ffep fetch-sportapp` has run) plus the grandfathered WC24 CSV at
`data/raw/legacy/wc24_pbp.csv` (`config.paths.raw_legacy / "wc24_pbp.csv"`,
read via `ingest.sportapp.read_mutated_sportapp_snapshot`, stamped
`source="legacy-sportapp"`). Both branches are attempted independently
whenever "sportapp" is requested; today only the grandfathered branch has data
on disk (the sportapp.fi API key rotation is deferred per STATE.md).

Artifact writes: `plays.parquet` and `games.parquet` are each written through
`_atomic_write_parquet` (a `.tmp` sibling, then `os.replace`), games written
before plays so a games-write failure never touches (or even attempts) the
plays write -- a prior successful run's `plays.parquet` is left completely
untouched. The Markdown report is rendered and written last via
`validation.report`, passing the collected source-level `notices` through as
`render_report`'s `source_notices` argument so a dropped or degraded source
is always named in the report; a console summary is printed, then every
source notice is echoed to the console as well (`typer.echo`, prefixed
`notice: `) -- `console_summary` alone prints nothing when `game_results` is
empty (every source dropped), so a zero-game run would otherwise look silent.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Sequence

import polars as pl
import typer

from flag_football_ep.canonical import CANONICAL_COLUMNS, CORE_COLUMNS, NULLABLE_EXTRAS, make_game_id
from flag_football_ep.config import Config
from flag_football_ep.ingest.hc_dedupe import dedupe_hc_rows
from flag_football_ep.ingest.hc_workbook import SHEET_NAMES, SheetNotFoundError, ingest_workbook
from flag_football_ep.ingest.hudl import ingest_dir
from flag_football_ep.ingest.ifaf import ingest_snapshots as ingest_ifaf_snapshots
from flag_football_ep.ingest.legacy import ingest_legacy
from flag_football_ep.ingest.sportapp import read_mutated_sportapp_snapshot
from flag_football_ep.ingest.sportapp import ingest_snapshots as ingest_sportapp_snapshots
from flag_football_ep.reference import (
    _HC_GAMES_SCHEMA,
    _PLAYER_MAPPING_SCHEMA,
    load_final_scores,
    load_half_boundaries,
    load_hc_games,
    load_player_mapping,
    load_team_mapping,
)
from flag_football_ep.validation.checks import GameResult, partition_games, run_checks
from flag_football_ep.validation.report import console_summary, render_report, write_report
from flag_football_ep.validation.schema import Contract, load_contract

__all__ = ["IngestResult", "run_ingest", "RunAllResult", "run_all", "EmptyIngestResultError"]

# Fixed dispatch order (per <interfaces> in the plan): hudl, legacy, sportapp,
# ifaf, hc_workbook. hc_workbook dispatches last so `dedupe_hc_rows` has the
# fully-assembled corpus of every other source to dedupe against (M3-01-04).
_KNOWN_SOURCES: tuple[str, ...] = ("hudl", "legacy", "sportapp", "ifaf", "hc_workbook")

# "legacy" (data_raw.csv, 47 hand-charted games) and "legacy-sportapp" (the
# grandfathered WC24 CSV) both ran through pre-port/legacy mutation code that
# cannot be re-validated at source -- FAILs are downgraded to WARN, never
# quarantined. See 01.2-11-SUMMARY.md's "Pipeline wiring note for plan 14".
# `hc_workbook` is deliberately NOT here (HC-D05): a head-coach camp/scrimmage
# game that fails a check is quarantined with reasons like any other new-format
# source, never waved through -- the head coach's own charting can be wrong
# too, and this project has no independent way to re-validate it either.
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


_GAMES_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "source": pl.Utf8,
    "competition": pl.Utf8,
    "season": pl.Int32,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "n_plays": pl.Int64,
    "n_drives": pl.Int64,
    "status": pl.Utf8,
    "quarantine_reasons": pl.Utf8,
    "ingested_at": pl.Utf8,
}


def _atomic_write_parquet(df: pl.DataFrame, path: Path) -> None:
    """Write `df` to `path` atomically: a `.tmp` sibling, then `os.replace`.

    Cleans up the `.tmp` file in a `finally` regardless of outcome, so a mid-
    write exception (from `write_parquet` or `os.replace` itself) leaves the
    previous `path` content intact and no stray `.tmp` file behind. The
    canonical dataset every downstream stage trusts must never exist in a
    half-written state.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        df.write_parquet(tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _build_games_table(
    combined: pl.DataFrame, game_results: list[GameResult], run_id: str
) -> pl.DataFrame:
    """One row per game_id seen in `combined` (accepted and quarantined alike).

    `status` is "quarantined" (blocking FAIL, non-warn-only source),
    "accepted-with-warnings" (every FAIL downgraded to WARN, warn-only
    source) or "accepted" (no FAIL at all). `quarantine_reasons` is the
    joined FAIL detail strings recorded by `partition_games` regardless of
    whether they were downgraded, or null when there are none.
    """
    if combined.height == 0:
        return pl.DataFrame(schema=_GAMES_SCHEMA)

    agg = combined.group_by("game_id", maintain_order=True).agg(
        source=pl.col("source").first(),
        competition=pl.col("competition").first(),
        season=pl.col("season").first(),
        home_team=pl.col("home_team").first(),
        away_team=pl.col("away_team").first(),
        n_plays=pl.len(),
        n_drives=pl.col("drive_id").n_unique(),
    )

    result_by_game = {g.game_id: g for g in game_results}
    rows: list[dict] = []
    for row in agg.to_dicts():
        game_id = row["game_id"]
        g = result_by_game.get(game_id)
        if g is None:
            status, reasons = "accepted", None
        elif g.quarantined:
            status = "quarantined"
            reasons = "; ".join(g.reasons) if g.reasons else None
        elif g.reasons:
            status = "accepted-with-warnings"
            reasons = "; ".join(g.reasons)
        else:
            status, reasons = "accepted", None

        rows.append(
            {
                "game_id": game_id,
                "source": row["source"],
                "competition": row["competition"],
                "season": row["season"],
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "n_plays": row["n_plays"],
                "n_drives": row["n_drives"],
                "status": status,
                "quarantine_reasons": reasons,
                "ingested_at": run_id,
            }
        )

    return pl.DataFrame(rows, schema=_GAMES_SCHEMA)


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
    """Dispatch the IFAF/cpx.studio source.

    Per-game failure containment now lives inside `ifaf.ingest_snapshots`
    (a malformed play, or any other failure in one game's flatten-through-
    conform chain, skips only that game and arrives here as `notices.skipped`,
    surfaced below via the existing `ifaf/{canonical_id}: skipped - ...` source
    notice). The `except Exception` below is therefore a last-resort net for a
    genuine whole-source problem -- `_load_games_meta`/`_load_tournaments_meta`/
    directory access failing outright -- not for an individual game's data
    anomaly, and its notice says so explicitly.
    """
    frames: list[pl.DataFrame] = []
    source_notices: list[str] = []
    game_notices: dict[str, list[str]] = {}

    if not ifaf_dir.exists() or not any(ifaf_dir.glob("unified-plays_*.json")):
        source_notices.append(f"ifaf: source directory {ifaf_dir} is empty or missing, skipping")
        return frames, source_notices, game_notices

    try:
        results = ingest_ifaf_snapshots(ifaf_dir, team_mapping)
    except Exception as exc:  # noqa: BLE001
        source_notices.append(
            f"ifaf: source-level failure, all ifaf games dropped from this run: "
            f"{type(exc).__name__}: {exc}"
        )
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


def _ingest_hc_workbook(
    hc_dir: Path,
    contract: Contract,
    hc_games: pl.DataFrame,
    player_mapping: pl.DataFrame,
    run_id: str,
) -> tuple[list[pl.DataFrame], list[str], dict[str, list[str]]]:
    """Dispatch the head-coach workbook source (HC-01/HC-02).

    Iterates every `*.xlsx` file under `hc_dir` (sorted, for a deterministic
    run) and, per file, every sheet named in `SHEET_NAMES` that actually
    exists in that workbook (`SheetNotFoundError` from `ingest_workbook` is
    caught and silently skipped -- most workbooks only have `Data`, not
    `Copy of Data`). Each sheet is ingested inside its own try/except, so one
    broken sheet never drops the whole source, mirroring `_ingest_ifaf`'s
    per-game containment.

    A sheet's `HcIngestNotices.messages` (one "Unbekanntes Spiel ..." entry
    per provisional game, block-segmentation findings, the pair-block-tail
    notice, ...) are sheet-scoped, not per-game -- `HcIngestNotices` has no
    per-game key to split them by. They are folded into `source_notices`
    (one line per message, prefixed with the file/sheet), never into
    `game_notices`: a sheet can resolve to hundreds or (Scoring Probability's
    heavily-fragmented `Copy of Data` tab, real run) low thousands of game
    ids, and attaching the full message list to *every one* of them multiplies
    into a validation report of several million lines -- a real, measured
    Rule 1 finding from the M3-01-04 real run, not a hypothetical. Every
    message still reaches the report and the console exactly once, just
    under "Source notices" rather than duplicated under every game section.

    PII discipline (HC-D02): every sheet's `unmapped_players` (raw labels)
    are accumulated and written *once*, at the end, to
    `hc_dir / f"unmapped_players_{run_id}.txt"` -- inside the gitignored raw
    directory, never rendered, never committed. Every notice about unmapped
    players names only a count, never a label.
    """
    frames: list[pl.DataFrame] = []
    source_notices: list[str] = []
    game_notices: dict[str, list[str]] = {}

    if not hc_dir.exists() or not any(hc_dir.glob("*.xlsx")):
        source_notices.append(
            f"hc_workbook: source directory {hc_dir} is empty or missing, skipping"
        )
        return frames, source_notices, game_notices

    unmapped_labels: set[str] = set()

    for path in sorted(hc_dir.glob("*.xlsx")):
        for sheet in SHEET_NAMES:
            try:
                df, notices = ingest_workbook(path, sheet, contract, hc_games, player_mapping)
            except SheetNotFoundError:
                continue
            except Exception as exc:  # noqa: BLE001 -- one sheet's failure never drops the source
                source_notices.append(
                    f"hc_workbook/{path.name}:{sheet}: {type(exc).__name__}: {exc}"
                )
                continue

            if df.height:
                frames.append(df)

            game_ids = df["game_id"].unique().to_list() if df.height else []
            n_games = len(game_ids)
            n_provisional = sum(
                1 for m in notices.messages if "provisorische game_id" in m
            )
            unmapped_labels.update(notices.unmapped_players)

            source_notices.append(
                f"hc_workbook:{path.name}:{sheet}: {df.height} Zeile(n), {n_games} "
                f"Spiel(e) ({n_provisional} davon provisorisch), "
                f"{len(notices.unmapped_players)} nicht zugeordnete Spieler-Label"
            )

            source_notices.extend(
                f"hc_workbook/{path.name}:{sheet}: {m}" for m in notices.messages
            )

    if unmapped_labels:
        outfile = hc_dir / f"unmapped_players_{run_id}.txt"
        outfile.write_text("\n".join(sorted(unmapped_labels)) + "\n", encoding="utf-8")
        source_notices.append(
            f"hc_workbook: {len(unmapped_labels)} nicht zugeordnete Spieler-Label "
            f"(über alle Sheets) nach {outfile} geschrieben -- gitignored, nie committen"
        )

    return frames, source_notices, game_notices


def run_ingest(
    config: Config,
    sources: Sequence[str],
    out_dir: Path | None = None,
    strict: bool = False,
) -> IngestResult:
    """Ingest every requested source into one validated canonical frame.

    Order of operations: validate `sources` against the five known names ->
    generate `run_id` once -> load the contract and reference data once ->
    dispatch each requested source in fixed order (hudl, legacy, sportapp,
    ifaf, hc_workbook), each inside its own try/except -> strict vertical
    concat of the four non-HC sources into `corpus_combined` (a schema
    mismatch across already-conformed frames is a bug, never papered over
    with a diagonal concat mode) -> if `hc_workbook` was requested,
    `dedupe_hc_rows` excludes HC rows that duplicate a declared partner game
    in `corpus_combined` (HC-D03) before the deduped HC rows join the rest ->
    `run_checks` + `partition_games` -> atomic writes of `games.parquet` then
    `plays.parquet` -> render and write the Markdown report (source notices
    passed through as `render_report`'s `source_notices` argument) -> print
    the per-game console summary, then echo every source notice
    (`notice: {text}`) -> return `IngestResult`.

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

    # hc_workbook dispatches last (fixed order) so it dedupes against the
    # fully-assembled corpus of every other requested source.
    corpus_non_empty = [frame for frame in frames if frame.height > 0]
    corpus_combined = (
        pl.concat(corpus_non_empty, how="vertical") if corpus_non_empty else _empty_canonical_frame()
    )

    if "hc_workbook" in sources:
        try:
            hc_games = load_hc_games(config.reference.hc_games)
        except Exception as exc:  # noqa: BLE001
            notices.append(
                f"hc_workbook: {config.reference.hc_games} unavailable "
                f"({type(exc).__name__}: {exc}), every HC game resolves to a provisional id"
            )
            hc_games = pl.DataFrame(schema=_HC_GAMES_SCHEMA)

        try:
            player_mapping = load_player_mapping(config.reference.player_mapping)
        except Exception as exc:  # noqa: BLE001
            notices.append(
                f"hc_workbook: {config.reference.player_mapping} unavailable "
                f"({type(exc).__name__}: {exc}), no HC player label resolves this run"
            )
            player_mapping = pl.DataFrame(schema=_PLAYER_MAPPING_SCHEMA)

        f, n, gn = _ingest_hc_workbook(
            config.paths.raw_hc_files, contract, hc_games, player_mapping, run_id
        )
        notices.extend(n)
        game_notices.update(gn)

        hc_non_empty = [frame for frame in f if frame.height > 0]
        hc_combined = (
            pl.concat(hc_non_empty, how="vertical") if hc_non_empty else _empty_canonical_frame()
        )
        hc_kept, dedupe_report = dedupe_hc_rows(hc_combined, corpus_combined, hc_games)
        notices.extend(dedupe_report.summary_lines())
        if hc_kept.height:
            frames.append(hc_kept)

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

    games_table = _build_games_table(combined, game_results, run_id)

    # Games written before plays: if the games write fails (or is
    # monkeypatched to fail), execution never reaches the plays write, so a
    # prior successful run's plays.parquet is left completely untouched.
    _atomic_write_parquet(games_table, games_path)
    _atomic_write_parquet(accepted.select(list(CANONICAL_COLUMNS)), plays_path)

    markdown = render_report(
        game_results, game_notices, contract.version, run_id, source_notices=notices
    )
    report_path = write_report(markdown, effective_out_dir, run_id)
    console_summary(game_results)
    for notice in notices:
        typer.echo(f"notice: {notice}")

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


class EmptyIngestResultError(RuntimeError):
    """Raised by `run_all` when ingest produced zero accepted plays.

    Training on an empty frame would fail deep inside `make_ep_model_mutations`/
    `make_wp_model_mutations` (a `DegenerateWeightRange` with a much less actionable
    message, or a bare polars error) -- this is the mitigation for T-1.2-18: fail fast,
    at the ingest boundary, with a message that names the report to check.
    """


@dataclass
class RunAllResult:
    """The outcome of one `run_all` call: every stage's artifact reference plus timings."""

    ingest: IngestResult
    ep_run: str
    wp_run: str
    scored_path: Path
    durations: dict[str, float] = field(default_factory=dict)


def run_all(config: Config, tune: bool = False) -> RunAllResult:
    """Chain ingest -> EP training -> WP training -> scoring, timing each stage.

    Rehearses the phase-1.4 ten-minute end-to-end goal: `durations` records a wall-clock
    float (seconds) per stage under the keys "ingest", "train_ep", "train_wp", "score", so
    later phases can measure against the target without re-instrumenting.

    Reads the freshly written `plays.parquet` back from disk for the training/scoring
    stages rather than passing `run_ingest`'s in-memory frame -- so `ffep run` exercises
    exactly the same on-disk path a separate `ffep train`/`ffep score` invocation would.

    Network fetch is intentionally not this function's job: the snapshot-first design
    means ingest reads whatever is already on disk, and the CLI's `--skip-fetch` handling
    (calling `fetch_games`/`fetch_tournament` before this function, or not at all) lives in
    `cli.run` so this module never needs live network credentials to be importable or
    testable.

    Raises `EmptyIngestResultError` when ingest produced zero accepted plays, instead of
    training on an empty frame.
    """
    # Imported lazily: `model.train`/`model.score` pull in mlflow/xgboost/hyperopt, which
    # `run_ingest`-only callers (e.g. `cli.ingest`) should never have to pay for just by
    # importing this module.
    from flag_football_ep.model.score import score_plays
    from flag_football_ep.model.train import train_ep, train_wp

    durations: dict[str, float] = {}

    start = time.perf_counter()
    ingest_result = run_ingest(config, list(_KNOWN_SOURCES))
    durations["ingest"] = time.perf_counter() - start

    if ingest_result.n_plays == 0:
        raise EmptyIngestResultError(
            "run_all: ingest produced zero accepted plays; aborting before training -- "
            f"see {ingest_result.report_path} for per-game details"
        )

    plays = pl.read_parquet(ingest_result.plays_path)

    start = time.perf_counter()
    ep_run = train_ep(plays, config, tune=tune)
    durations["train_ep"] = time.perf_counter() - start

    start = time.perf_counter()
    wp_run = train_wp(plays, config, tune=tune)
    durations["train_wp"] = time.perf_counter() - start

    start = time.perf_counter()
    scored = score_plays(plays, config, ep_run, wp_run)
    scored_path = config.paths.processed / "plays_scored.parquet"
    scored.write_parquet(scored_path)
    durations["score"] = time.perf_counter() - start

    return RunAllResult(
        ingest=ingest_result,
        ep_run=ep_run,
        wp_run=wp_run,
        scored_path=scored_path,
        durations=durations,
    )
