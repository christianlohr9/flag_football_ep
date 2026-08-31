"""Grandfathered ``data_raw.csv`` ingest path — notebook-identical semantics.

This module ports ``get_games``/``make_hudl_mutations`` from
``Python/helper_add_hudl_mutations.py`` for the 47 hand-charted legacy games
(~3,700 plays). Its behaviour is intentionally FROZEN, not modernized:

- The fragile "team with the first drive is home" heuristic lives ONLY here.
  New-format ingest (``ingest/hudl.py``) uses the contract's TEAM1/ODK rule
  instead; this file exists precisely because that rule is unavailable for
  ``data_raw.csv`` (its ``ODK`` column is polluted with formation text, not
  O/D/K codes).
- ``RESULT`` keeps its ``str.contains`` substring semantics (e.g. "Complete
  Penalty" without a comma still counts as a complete pass; "Penalty
  (declined)" still counts as no_play). These are documented as invalid
  going forward but are still present in the 47 legacy games and must not be
  silently reclassified.
- Scoring credit stays offense-only (``credit_defense=False``), matching the
  notebook's original ``scoring_play_team`` formula exactly.

Any semantic change to this path belongs to phase 1.3's methodological
review — changing it here would change the trained model without that
review having happened.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from flag_football_ep.canonical import (
    ConformReport,
    add_score_columns,
    add_scoring_play_team,
    conform_to_canonical,
    make_game_id,
)
from flag_football_ep.reference import map_teams
from flag_football_ep.validation.checks import CheckResult, run_checks

# Legacy charting columns mapped onto canonical NULLABLE_EXTRAS. Columns not
# listed here (e.g. C, X/H, Y/CAT, Z, Drop, B) have no canonical home and are
# dropped by conform_to_canonical's final select, reported as dropped_unknown.
_CHARTING_COLUMN_MAP: dict[str, str] = {
    "OFF FORM": "off_form",
    "Off Str": "off_str",
    "OFF PLAY": "off_play",
    "TARGET ROUTE": "target_route",
    "RECEIVED BY": "received_by",
    "Thrown By": "thrown_by",
    "YAC": "yac",
    "QB": "qb",
    "Target": "target",
    "Drive Success": "drive_success",
}


@dataclass
class IngestNotices:
    """Notices produced while ingesting the grandfathered legacy file.

    Unlike a per-game ingest path, this covers the entire multi-game
    ``data_raw.csv`` in one shot — the legacy file holds all 47 games
    together, not one file per game.

    ``check_results`` is the raw, undowngraded ``CheckResult`` list from
    ``validation.checks.run_checks``: this module never partitions games
    into accepted/quarantined or decides quarantine — that is
    ``pipeline.run_ingest``'s job (plan 14), which downgrades every FAIL to
    WARN for warn-only sources without dropping any row. ``messages`` is
    the same information rendered as human-readable strings for the
    ingest report.
    """

    conform: ConformReport
    check_results: list[CheckResult] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)


def read_legacy(path: Path) -> pl.DataFrame:
    """Read ``data_raw.csv`` exactly as the notebook did.

    ``infer_schema_length=0`` mirrors the original: every column arrives as
    Utf8, so numeric casts happen explicitly downstream in
    ``legacy_mutations``/``conform_to_canonical`` instead of relying on
    polars' schema inference.
    """
    return pl.read_csv(path, separator=";", infer_schema_length=0)


def get_games(df: pl.DataFrame) -> pl.DataFrame:
    """Derive ``home_team``/``away_team`` per game from first-drive order.

    Assumption preserved verbatim from the notebook: the team credited with
    a game's first drive is "home". This only matters for score bookkeeping
    and the score differential later — it is NOT a real home/away
    convention. This heuristic is deliberately quarantined to this module;
    ``ingest/hudl.py`` uses the contract's TEAM1/ODK rule instead.

    Implemented without polars' reshape-then-pivot API (RESEARCH Pitfall 5:
    API-compatibility risk on the pinned polars floor):
    sort by play order, keep the first-seen row per (game_id, posteam),
    group into a list per game, then take the first two list elements as
    home/away.

    Raises ``ValueError`` naming the game when more than two distinct
    ``posteam`` values are observed for it (the original silently produced
    a struct of unexpected width in this case).
    """
    ordered = df.with_columns(
        pl.col("play_id").cast(pl.Int64, strict=False).alias("_play_id_sort")
    ).sort(["game_id", "_play_id_sort"])

    first_seen = ordered.unique(subset=["game_id", "posteam"], keep="first", maintain_order=True)

    grouped = first_seen.group_by("game_id", maintain_order=True).agg(pl.col("posteam"))

    game_ids: list = []
    home_teams: list = []
    away_teams: list = []
    for row in grouped.iter_rows(named=True):
        posteams = row["posteam"]
        if len(posteams) > 2:
            raise ValueError(
                f"game {row['game_id']!r} has {len(posteams)} distinct posteam values "
                f"(expected at most 2): {posteams}"
            )
        game_ids.append(row["game_id"])
        home_teams.append(posteams[0] if len(posteams) > 0 else None)
        away_teams.append(posteams[1] if len(posteams) > 1 else None)

    return pl.DataFrame({"game_id": game_ids, "home_team": home_teams, "away_team": away_teams})


def legacy_mutations(df: pl.DataFrame) -> pl.DataFrame:
    """Faithful port of ``make_hudl_mutations`` for the ``data_raw.csv`` shape.

    Ports intact: the down/distance/yardline casts, the
    ``yardline_50_simple``/``yards_to_go_simple`` bins, the ``shift(-1)``
    lookahead columns (including the notebook's cross-game-boundary quirk —
    no ``.over("game_id")`` guard, exactly as written), and every RESULT
    ``str.contains`` derivation (play_type, sack, interception,
    complete_pass, touchdown, def_touchdown, penalty, safety,
    one_point_conv_success, two_point_conv_success,
    defensive_two_point_conv).

    Two deliberate replacements (per plan action, not a silent drift):
    - The keyword-comparison form ``pl.when(down = 0)`` becomes an explicit
      ``pl.col("down") == 0`` predicate in the ``yards_gained`` derivation —
      identical semantics, avoids a deprecated call style.
    - The score chain (``scoring_play``/``scoring_play_team``/points/
      cum_sum/differential) is replaced by
      ``add_scoring_play_team(credit_defense=False)`` plus
      ``add_score_columns`` from ``canonical.py``. ``credit_defense=False``
      preserves the notebook's offense-only scoring credit exactly — the
      original formula never credited defensive scores to
      ``scoring_play_team`` either.
    """
    games = get_games(df)
    output = (
        df.join(games, on="game_id")
        .with_columns(
            defteam=pl.when(pl.col("posteam") == pl.col("home_team"))
            .then(pl.col("away_team"))
            .when(pl.col("posteam") == pl.col("away_team"))
            .then(pl.col("home_team"))
            .otherwise(pl.lit(None))
        )
        .with_columns(
            [
                pl.col("DN").cast(pl.Int32(), strict=False).alias("down"),
                pl.col("DIST").cast(pl.Int32(), strict=False).alias("yards_to_go"),
                pl.col("YARD LN").cast(pl.Int32(), strict=False).alias("yardline"),
                pl.col(["yardline_50", "game_id", "play_id", "drive_id", "half"]).cast(
                    pl.Int32(), strict=False
                ),
            ]
        )
        .with_columns(
            yardline_50_simple=pl.when(pl.col("yardline_50") < 25).then(pl.lit(0)).otherwise(pl.lit(1)),
            yards_to_go_simple=pl.when(pl.col("yards_to_go") <= 5)
            .then(pl.lit(1))
            .when((pl.col("yards_to_go") > 5) & (pl.col("yards_to_go") <= 10))
            .then(pl.lit(2))
            .when((pl.col("yards_to_go") > 10) & (pl.col("yards_to_go") <= 15))
            .then(pl.lit(3))
            .when((pl.col("yards_to_go") > 15) & (pl.col("yards_to_go") <= 20))
            .then(pl.lit(4))
            .when(pl.col("yards_to_go") > 20)
            .then(pl.lit(5))
            .otherwise(pl.lit(0)),
        )
        .with_columns(
            yardline_50_after=pl.col("yardline_50").shift(-1),
            posteam_after=pl.col("posteam").shift(-1),
        )
        .with_columns(
            pl.when(pl.col("RESULT").str.contains("Rush"))
            .then(pl.lit("run"))
            .when(pl.col("RESULT").str.contains("Penalty"))
            .then(pl.lit("no_play"))
            .when(pl.col("RESULT").str.contains("KNEEL"))
            .then(pl.lit("qb_kneel"))
            .when(pl.col("down") == 0)
            .then(pl.lit("extra_point"))
            .otherwise(pl.lit("pass"))
            .alias("play_type")
        )
        .with_columns(
            pl.when(pl.col("RESULT").str.contains("Sack")).then(pl.lit(1)).otherwise(pl.lit(0)).alias("sack")
        )
        .with_columns(
            pl.when(pl.col("RESULT").str.contains("Interception"))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("interception")
        )
        .with_columns(
            complete_pass=pl.when(
                (pl.col("play_type").is_in(["pass", "extra_point", "no_play"]))
                & (pl.col("RESULT").str.contains("Complete"))
            )
            .then(pl.lit(1))
            .when(
                (pl.col("play_type").is_in(["pass", "extra_point", "no_play"]))
                & (pl.col("RESULT") == "Incomplete")
            )
            .then(pl.lit(0))
            .when(
                (pl.col("yardline_50") != pl.col("yardline_50_after"))
                & (pl.col("posteam") == pl.col("posteam_after"))
            )
            .then(pl.lit(1))
            .when((pl.col("down") == 4) & (pl.col("posteam") != pl.col("posteam_after")))
            .then(pl.lit(0))
            .otherwise(pl.lit(0)),
        )
        .with_columns(
            pl.when(
                (pl.col("RESULT").str.contains("TD")) & (pl.col("RESULT").str.contains("Def").not_())
            )
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("touchdown")
        )
        .with_columns(
            pl.when(pl.col("RESULT").str.contains("Def TD"))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("def_touchdown")
        )
        .with_columns(
            pl.when(pl.col("RESULT").str.contains("Penalty"))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("penalty")
        )
        .with_columns(
            pl.when(pl.col("RESULT").str.contains("Safety"))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("safety")
        )
        .with_columns(
            pl.when((pl.col("RESULT") == "Good") & (pl.col("down") == 0) & (pl.col("yardline_50") == 45))
            .then(pl.lit(1))
            .when((pl.col("down") == 0) & (pl.col("yardline_50") != 45))
            .then(pl.lit(0))
            .when(pl.col("down") != 0)
            .then(pl.lit(0))
            .otherwise(pl.lit(0))
            .alias("one_point_conv_success")
        )
        .with_columns(
            pl.when((pl.col("RESULT") == "Good") & (pl.col("down") == 0) & (pl.col("yardline_50") == 40))
            .then(pl.lit(1))
            .when((pl.col("down") == 0) & (pl.col("yardline_50") != 40))
            .then(pl.lit(0))
            .when(pl.col("down") != 0)
            .then(pl.lit(0))
            .otherwise(pl.lit(0))
            .alias("two_point_conv_success")
        )
        .with_columns(
            pl.when((pl.col("RESULT").str.contains("Def TD")) & (pl.col("down") == 0))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("defensive_two_point_conv")
        )
    )

    # Shared, per-game-safe score chain (canonical.py). credit_defense=False
    # preserves the notebook's offense-only scoring_play_team formula.
    output = add_scoring_play_team(output, credit_defense=False)
    output = add_score_columns(output)

    output = output.with_columns(
        yards_gained=pl.when(pl.col("down") == 0)
        .then(pl.lit(0))
        .when((pl.col("down") == 4) & (pl.col("complete_pass") == 0))
        .then(pl.lit(0))
        .when((pl.col("down") == 4) & (pl.col("safety") == 1))
        .then(pl.lit(0))
        .otherwise(pl.col("yardline_50_after") - pl.col("yardline_50")),
    ).with_columns(
        pl.when((pl.col("yardline_50") < 25) & (pl.col("yards_gained") > pl.col("yards_to_go")))
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("first_down")
    )

    return output


def ingest_legacy(
    path: Path, team_mapping: pl.DataFrame | None = None
) -> tuple[pl.DataFrame, IngestNotices]:
    """Ingest the grandfathered ``data_raw.csv`` into the canonical schema.

    ``game_id`` becomes ``legacy-{n}`` via ``make_game_id("legacy", n)``;
    the original integer game id is preserved as the ``source_game_id``
    extra. When ``team_mapping`` is ``None``, team labels are left
    untouched (legacy labels may not be mapped yet); a supplied mapping
    maps them via ``reference.map_teams`` and raises ``UnmappedTeamError``
    on any gap.

    Runs ``validation.checks.run_checks`` on the conformed output and
    records the results in the returned ``IngestNotices`` — this function
    never quarantines; that decision belongs to ``pipeline.run_ingest``.
    """
    raw = read_legacy(path)
    mutated = legacy_mutations(raw)

    mutated = mutated.with_columns(pl.col("game_id").cast(pl.Utf8).alias("source_game_id"))
    mutated = mutated.with_columns(
        pl.col("source_game_id")
        .map_elements(lambda key: make_game_id("legacy", key), return_dtype=pl.Utf8)
        .alias("game_id")
    )

    mutated = mutated.with_columns(
        pl.lit("legacy").alias("source"),
        pl.lit("legacy").alias("competition"),
        # data_raw.csv carries no season column; phase 1.3 may backfill this
        # from a reference file once the grandfathered games are dated.
        pl.lit(0).cast(pl.Int32).alias("season"),
        pl.col("RESULT").alias("result_raw"),
    )

    rename_exprs = [
        pl.col(src).alias(dst) for src, dst in _CHARTING_COLUMN_MAP.items() if src in mutated.columns
    ]
    if rename_exprs:
        mutated = mutated.with_columns(rename_exprs)

    if team_mapping is not None:
        mutated = map_teams(
            mutated, team_mapping, "legacy", ["posteam", "defteam", "home_team", "away_team"]
        )

    conformed, conform_report = conform_to_canonical(mutated, "legacy")

    check_results = run_checks(conformed)
    messages = [f"{r.game_id} {r.check}: {r.status.value} - {r.detail}" for r in check_results]

    notices = IngestNotices(conform=conform_report, check_results=check_results, messages=messages)
    return conformed, notices
