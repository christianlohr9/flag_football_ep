"""Batch report orchestration for REQ-S1-16: ingest -> score(champion) -> report, timed.

This module deliberately does NOT reuse or extend `pipeline`'s existing full end-to-end
orchestrator, because that orchestrator's job is to exercise the whole ingest-through-scoring
chain by fitting a brand-new pair of models on every single call and then scoring with the
run ids those fresh fits just produced -- exactly right for rehearsing the pipeline, and
exactly wrong for a report run. Fitting two fresh models inside the REQ-S1-16 ten-minute clock
would very likely blow the budget by itself, and worse, it would silently hand a
never-reviewed, never-promoted model the job of producing the numbers the coaching staff is
about to read. The timed path here is ingest -> score(champion) -> report; fitting new models
and deciding to promote one of them stays a separate, deliberate, human-invoked step entirely
outside this module and entirely outside the ten-minute clock, so a report run can never
silently change which model produced its numbers.

`build_reports` performs no filesystem writes and no scoring -- it only assembles already
computed/scored data into `{filename: html}` via each product's existing page-builder
(`reports.opponent`, `reports.own_team`, `reports.decisions`, `reports.wp_review`). The one
filesystem touch inside it is reading `oof_predictions_wp.parquet` under
`config.paths.processed` through `attach_wp_provenance`, so the WP review pages are built from
the provenance-resolved frame (out-of-fold for historical plays, the champion frame only for
plays the out-of-fold set does not cover) and never straight off the champion-scored frame --
for a game inside the champion's training set that would put an in-sample curve in front of
the staff, exactly what the locked 1.3 EPA/WP contract rules out.

`run_report_pipeline` is the actual REQ-S1-16 ten-minute path: it chains ingest, scoring
(resolved to the promoted `champion` alias only -- never an explicit run id), report assembly
and the single write point (`reports.render.write_report_run`), timing each stage the same way
`pipeline`'s own multi-stage orchestrator does, so later phases can measure against the target
without re-instrumenting.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import polars as pl

from flag_football_ep.config import Config
from flag_football_ep.pipeline import run_ingest
from flag_football_ep.reference import load_group_opponents
from flag_football_ep.reports.decisions import DECISIONS_FILENAME, build_decisions_page
from flag_football_ep.reports.opponent import (
    build_opponent_data,
    build_opponent_page,
    opponent_filename,
)
from flag_football_ep.reports.own_team import (
    OWN_TEAM_FILENAME,
    build_own_team_data,
    build_own_team_page,
)
from flag_football_ep.reports.player_analysis import (
    PLAYER_ANALYSIS_FILENAME,
    build_player_analysis_data,
    build_player_analysis_page,
)
from flag_football_ep.reports.render import write_report_run
from flag_football_ep.reports.wp_review import (
    attach_wp_provenance,
    build_wp_review_page,
    wp_review_filename,
)

PRODUCTS: tuple[str, ...] = (
    "opponents",
    "own-team",
    "decisions",
    "wp-review",
    "player-analysis",
)
"""Every product `build_reports`/`run_report_pipeline` know how to build, in dispatch order."""

_INGEST_SOURCES: tuple[str, ...] = ("hudl", "legacy", "sportapp", "ifaf")


@dataclass(frozen=True)
class ReportRunResult:
    """The outcome of one `run_report_pipeline` call: where it landed, what it wrote, and how
    long each stage took."""

    run_dir: Path
    latest_dir: Path
    filenames: tuple[str, ...]
    durations: dict[str, float]
    notices: tuple[str, ...]


def build_reports(
    plays: pl.DataFrame,
    scored: pl.DataFrame,
    *,
    config: Config,
    opponents: Sequence[str] | None = None,
    products: Sequence[str] | None = None,
) -> tuple[dict[str, str], tuple[str, ...]]:
    """Render every requested product into `{filename: html}`, plus the notices collected
    along the way. Performs no filesystem writes and no scoring.

    `opponents=None` builds every `canonical_team` in the group-opponents reference file;
    an explicit sequence is validated against that same closed set first -- an opponent value
    outside it raises `ValueError` naming the rejected value and listing the valid codes,
    before any path is ever constructed from it (T-1.4-56; `reports.opponent.team_slug` and
    `reports.render.write_report_run`'s filename-key rejection are the second and third
    layers). `products=None` builds every entry in `PRODUCTS`; an unknown product name raises
    `ValueError` naming it. Both checks run before any per-product dispatch -- they are caller
    errors and must fail fast, unlike a single product's builder raising mid-batch.

    A per-product (or per-opponent, or per-game) page-build failure is recorded as a German
    notice naming the product and the exception text, and does not abort the rest of the
    batch (T-1.4-59) -- a run that produces four of five products the evening before a
    tournament is worth far more than a run that produces none.
    """
    group_opponents = load_group_opponents(config.reference.group_opponents)
    valid_codes = group_opponents["canonical_team"].to_list()
    team_names = dict(
        zip(
            group_opponents["canonical_team"].to_list(),
            group_opponents["team_name"].to_list(),
        )
    )

    if opponents is None:
        resolved_opponents = list(valid_codes)
    else:
        invalid = [o for o in opponents if o not in valid_codes]
        if invalid:
            raise ValueError(
                f"--opponent value(s) {invalid!r} not present in the group-opponents "
                f"reference file ({config.reference.group_opponents}); valid codes are "
                f"{valid_codes!r}"
            )
        resolved_opponents = list(opponents)

    resolved_products = list(products) if products is not None else list(PRODUCTS)
    unknown_products = [p for p in resolved_products if p not in PRODUCTS]
    if unknown_products:
        raise ValueError(
            f"unknown product name(s) {unknown_products!r}; valid products are {list(PRODUCTS)!r}"
        )

    rendered: dict[str, str] = {}
    notices: list[str] = []

    if "opponents" in resolved_products:
        for team in resolved_opponents:
            try:
                data = build_opponent_data(plays, team=team, team_name=team_names[team])
                rendered[opponent_filename(team)] = build_opponent_page(data)
            except Exception as exc:  # noqa: BLE001 - per-product isolation, see docstring
                notices.append(
                    f"Gegner-Auswertung für {team_names.get(team, team)!r} ({team!r}) "
                    f"fehlgeschlagen: {exc}"
                )

    if "own-team" in resolved_products:
        try:
            own_team_data = build_own_team_data(plays, config=config, scored=scored)
            rendered[OWN_TEAM_FILENAME] = build_own_team_page(own_team_data)
        except Exception as exc:  # noqa: BLE001 - per-product isolation, see docstring
            notices.append(f"Eigenteam-Auswertung fehlgeschlagen: {exc}")

    if "decisions" in resolved_products:
        try:
            rendered[DECISIONS_FILENAME] = build_decisions_page(plays)
        except Exception as exc:  # noqa: BLE001 - per-product isolation, see docstring
            notices.append(f"Entscheidungs-Cheatsheet fehlgeschlagen: {exc}")

    if "wp-review" in resolved_products:
        wp_frame = attach_wp_provenance(
            plays, processed_dir=config.paths.processed, scored=scored
        )
        for game_id in sorted(wp_frame["game_id"].unique().to_list()):
            try:
                game_plays = wp_frame.filter(pl.col("game_id") == game_id)
                rendered[wp_review_filename(game_id)] = build_wp_review_page(
                    game_plays, game_id=game_id
                )
            except Exception as exc:  # noqa: BLE001 - per-product isolation, see docstring
                notices.append(
                    f"WP-Review-Seite für Spiel {game_id!r} fehlgeschlagen: {exc}"
                )

    if "player-analysis" in resolved_products:
        try:
            player_analysis_data = build_player_analysis_data(
                plays, config=config, scored=scored
            )
            rendered[PLAYER_ANALYSIS_FILENAME] = build_player_analysis_page(
                player_analysis_data
            )
        except Exception as exc:  # noqa: BLE001 - per-product isolation, see docstring
            notices.append(f"Player-Analysis-Auswertung fehlgeschlagen: {exc}")

    return rendered, tuple(notices)


def run_report_pipeline(
    config: Config,
    *,
    opponents: Sequence[str] | None = None,
    products: Sequence[str] | None = None,
    skip_ingest: bool = False,
) -> ReportRunResult:
    """Chain ingest -> score(champion) -> report, timing each stage. Never fits a new model:
    scoring is called with no explicit run ids, so resolution falls through to the promoted
    `champion` alias only -- the only way this module's numbers ever change is a deliberate,
    human-invoked promotion, never a side effect of running a report.

    Reads `plays.parquet` back from disk for the scoring stage (matching the on-disk-first
    discipline `pipeline`'s own orchestrator already follows), unless `skip_ingest` is set, in
    which case the ingest stage is skipped entirely, its duration recorded as zero, and a
    notice records that re-rendering used the existing `plays.parquet` unchanged.

    Lets `flag_football_ep.model.registry.RegistryError` propagate unchanged when no
    `champion` alias has been set -- its own message already tells the operator to run the
    promotion command, which is the correct fix for a report built from an unpromoted model
    (T-1.4-58).
    """
    from flag_football_ep.model.score import score_plays

    durations: dict[str, float] = {}
    notices: list[str] = []

    if skip_ingest:
        durations["ingest"] = 0.0
        notices.append(
            "Ingest übersprungen (--skip-ingest); vorhandene plays.parquet wird verwendet."
        )
    else:
        start = time.perf_counter()
        run_ingest(config, list(_INGEST_SOURCES))
        durations["ingest"] = time.perf_counter() - start

    plays = pl.read_parquet(config.paths.processed / "plays.parquet")

    start = time.perf_counter()
    scored = score_plays(plays, config)
    scored.write_parquet(config.paths.processed / "plays_scored.parquet")
    durations["score"] = time.perf_counter() - start

    start = time.perf_counter()
    rendered, build_notices = build_reports(
        plays, scored, config=config, opponents=opponents, products=products
    )
    notices.extend(build_notices)
    run_dir, latest_dir = write_report_run(rendered, config.paths.reports)
    durations["report"] = time.perf_counter() - start

    return ReportRunResult(
        run_dir=run_dir,
        latest_dir=latest_dir,
        filenames=tuple(sorted(rendered)),
        durations=durations,
        notices=tuple(notices),
    )
