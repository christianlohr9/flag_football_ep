"""Run the Explosiveness/Efficiency comparison (HC-04, Phase M3-3) on the real corpus.

This is a standalone script, NOT part of the installed `flag_football_ep` package --
mirrors `scripts/capture_notebook_baseline.py`'s framing. Its only job is orchestration:
every computation (the head-coach baselines, the corpus-calibrated explosiveness
threshold, the four-definition comparison rollup, the cliff-zone table) lives in
`flag_football_ep.features.explosiveness` (M3-03-01) and is imported here, never
re-derived. This script decides WHAT to run and WHERE the output lands; the module
decides HOW each number is computed.

Four artifacts land under `data/reference/explosiveness/` (committed, public, git-tracked):
`calibration.json` (the versioned, corpus-fingerprinted explosiveness threshold),
`comparison_overall.csv` (one row per metric definition at the team level, plus two
named findings that would otherwise only exist as stdout output), `comparison_by_player.csv`
(the same comparison by QB identity and by receiver identity, pseudonymised), and
`cliff_zone.csv` (the 8-16 yard window around the head coach's cutoff, the direct answer
to "was ist, wenn eine Spielerin nur 11 Yards erzielt?").

A fifth artifact, the pseudonym-to-source-label key, is written to
`data/processed/m3-03/pseudonym_key.csv` -- gitignored (`data/processed/*` is ignored
except `.gitkeep`, `docs/` and `data/reference/` are not). This script asserts that path
is outside both `config.paths.reference` and `docs/` before writing it, so a future config
change can never silently make it public. No player label reaches stdout, a docstring, a
commit message, or any file under `data/reference/`/`docs/` -- only the pseudonym.

Rerun discipline: without `--recalibrate`, an existing `calibration.json` is loaded and
reused rather than recomputed, so a rerun of this script cannot silently move the
threshold under an already-published `docs/explosiveness-vorschlag.md`. Pass
`--recalibrate` to derive a fresh threshold from the current corpus.

Corpus scope (2026-09-06 addendum): rows whose `competition_tier` resolves to
`EXCLUDED_TIERS` (currently just `mens-international` -- the IFAF ffwc26-men
tournament, docs/ifaf-field-mapping.md) are dropped before any calibration or
comparison computation runs, mirroring the training-time
`exclude_games_ep`/`exclude_games_wp` exclusion in `ffep.toml` -- this corpus
is scoped to the German women's national team program, and the men's
tournament sharing team codes with the women's (`m-ger`/`w-ger` both map to
canonical `GER`) means it would otherwise silently contaminate the own-team
comparison the moment it entered `plays_scored.parquet`.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import polars as pl
from scipy.stats import binomtest

from flag_football_ep.config import load_config
from flag_football_ep.reference import load_competition_tier, map_competition_tier
from flag_football_ep.features.explosiveness import (
    DEFAULT_EPA_QUANTILE,
    DEFINITIONS,
    HC_EXPLOSIVE_YARDS_THRESHOLD,
    HC_PASS_ATTEMPT_SCOPE,
    MissingExplosivenessColumns,
    calibrate,
    cliff_zone_table,
    definition_comparison,
    hc_efficiency_table,
    load_calibration,
    scrimmage_plays,
    write_calibration,
)
from flag_football_ep.reports.aggregate import MUTED_MIN_N

# Design choice (documented, not an external standard): the bar for naming an individual
# player by pseudonym rather than folding them into the shared "Sonstige" bucket. Lower
# than this and a very thin-sample player becomes identifiable to teammates by process of
# elimination even behind a pseudonym; higher and too many real contributors disappear
# into the bucket. Overridable via --min-attempts.
DEFAULT_MIN_ATTEMPTS = 15

# 2026-09-06 addendum: competition_tier values excluded from this corpus by
# default. "mens-international" is the IFAF ffwc26-men tournament -- see the
# module docstring's "Corpus scope" section for why this must be filtered
# here rather than relying on team-code filtering downstream (m-ger/w-ger
# collide onto the same canonical "GER" team code).
EXCLUDED_TIERS: tuple[str, ...] = ("mens-international",)


def _exclude_tiers(plays: pl.DataFrame, config) -> pl.DataFrame:
    """Drop every row whose `competition_tier` (resolved fresh from
    `config.reference.competition_tier`, not trusted from any pre-existing
    column) is in `EXCLUDED_TIERS`. Reports how many rows/games were dropped
    so a silent scope change is never invisible in the script's own output.
    """
    if "source" not in plays.columns or "competition" not in plays.columns:
        return plays

    mapping = load_competition_tier(config.reference.competition_tier)
    tiered = map_competition_tier(plays, mapping)

    excluded = tiered.filter(pl.col("competition_tier").is_in(EXCLUDED_TIERS))
    if excluded.height:
        excluded_games = (
            excluded["game_id"].n_unique() if "game_id" in excluded.columns else None
        )
        print(
            f"finding: excluding {excluded.height} row(s) across {excluded_games} game(s) "
            f"with tier in {EXCLUDED_TIERS} from the explosiveness/comparison corpus"
        )

    return tiered.filter(~pl.col("competition_tier").is_in(EXCLUDED_TIERS)).drop("competition_tier")

_OVERALL_COLUMNS = (
    "definition",
    "label_de",
    "scope_note",
    "n",
    "successes",
    "rate",
    "ci_low",
    "ci_high",
    "muted",
)

_BY_PLAYER_COLUMNS = (
    "rolle",
    "spieler",
    "definition",
    "label_de",
    "scope_note",
    "n",
    "successes",
    "rate",
    "ci_low",
    "ci_high",
    "muted",
    "shrunk_rate",
)


def _print_corpus_census(plays: pl.DataFrame) -> None:
    """Print the corpus census the German document quotes: row counts, source mix,
    season range, and the two named findings (HC-workbook rows present? `efficiency`
    column present?) that must be reported rather than silently absent.
    """
    print("=== Corpus census ===")
    print(f"total_rows={plays.height}")

    scrimmage = scrimmage_plays(plays)
    print(f"scrimmage_rows={scrimmage.height}")

    epa_rows = scrimmage.filter(pl.col("epa").is_not_null()).height
    print(f"scrimmage_rows_with_epa={epa_rows}")

    print("source_counts:")
    source_counts = (
        plays.group_by("source").agg(n=pl.len()).sort("source").to_dicts()
        if "source" in plays.columns
        else []
    )
    for row in source_counts:
        print(f"  {row['source']}: {row['n']}")

    if "season" in plays.columns:
        season_stats = plays.select(
            season_min=pl.col("season").min(), season_max=pl.col("season").max()
        ).row(0, named=True)
        print(f"season_min={season_stats['season_min']} season_max={season_stats['season_max']}")

    has_hc_workbook_rows = (
        "source" in plays.columns
        and plays.filter(pl.col("source").str.starts_with("hc_workbook")).height > 0
    )
    print(f"finding: hc_workbook_rows_present={has_hc_workbook_rows}")

    has_efficiency_column = "efficiency" in plays.columns
    print(f"finding: efficiency_column_present={has_efficiency_column}")
    print("======================")


def _resolve_or_load_calibration(
    plays: pl.DataFrame, *, path: Path, epa_quantile: float, recalibrate: bool
):
    """Load an existing calibration unless `recalibrate` is set or none exists yet --
    the `--recalibrate` opt-in semantics that keep a rerun from silently moving the
    threshold under an already-published document.
    """
    if path.exists() and not recalibrate:
        calibration = load_calibration(path)
        print(
            f"loaded existing calibration ({path}): schema_version={calibration.schema_version} "
            f"epa_threshold={calibration.epa_threshold:.4f} corpus_n={calibration.corpus_n}"
        )
        return calibration

    scored = scrimmage_plays(plays, require_epa=True)
    calibration = calibrate(scored, epa_quantile=epa_quantile)
    write_calibration(calibration, path)
    print(
        f"calibrated fresh threshold and wrote {path}: "
        f"epa_threshold={calibration.epa_threshold:.4f} corpus_n={calibration.corpus_n} "
        f"n_success={calibration.n_success}"
    )
    return calibration


def _binomial_row(*, n: int, successes: int) -> dict[str, float | int | bool | None]:
    """Clopper-Pearson rate/CI/muted for a one-off finding row, reusing
    `scipy.stats.binomtest` -- the same primitive `reports.aggregate.rate_table` uses --
    rather than hand-rolling a second interval calculation for a single row.
    """
    if n == 0:
        return {"rate": None, "ci_low": None, "ci_high": None, "muted": True}
    ci = binomtest(successes, n).proportion_ci()
    return {
        "rate": successes / n,
        "ci_low": ci.low,
        "ci_high": ci.high,
        "muted": n < MUTED_MIN_N,
    }


def _build_overall_table(
    plays: pl.DataFrame, *, calibration, efficiency_status: str
) -> pl.DataFrame:
    """`comparison_overall.csv`: `definition_comparison` grouped by a constant
    team-level key (one row per `DEFINITIONS` entry), plus two named-finding rows that
    would otherwise only exist as stdout output: the yards-only-clause share of the
    head coach's verbal rule (the "89 plays" finding) and the `efficiency` column's
    absence from the corpus.
    """
    team_key = plays.with_columns(_team=pl.lit("Team"))
    team_comparison = definition_comparison(team_key, ["_team"], calibration=calibration)
    definitions_rows = team_comparison.select(list(_OVERALL_COLUMNS))

    # M3-04-01 correction: HC_PASS_ATTEMPT_SCOPE (imported, never re-derived) excludes sack
    # rows from the workbook's own Attempts denominator (D2 = Comps+Incs+INTs) -- this used
    # to be a local `play_type == "pass"` filter that silently counted sacks in.
    pass_epa = scrimmage_plays(plays, require_epa=True).filter(HC_PASS_ATTEMPT_SCOPE)
    yards_only_flag = (pl.col("yards_gained") > HC_EXPLOSIVE_YARDS_THRESHOLD) & ~(
        pl.col("epa") > 0
    )
    pass_epa_n = pass_epa.height
    yards_only_n = (
        int(pass_epa.select(yards_only_flag.sum()).item()) if pass_epa_n > 0 else 0
    )
    yards_only_stats = _binomial_row(n=pass_epa_n, successes=yards_only_n)
    print(
        f"finding: verbal_only_yards_clause={yards_only_n}/{pass_epa_n} "
        f"(plays triggering the verbal rule ONLY through yards>12, EPA<=0)"
    )

    yards_only_row = pl.DataFrame(
        {
            "definition": ["verbal_only_yards_clause"],
            "label_de": ["Nur durch Yards-Klausel ausgeloest (Yards > 12, EPA <= 0)"],
            "scope_note": [
                "Anteil an Pass-Attempts mit EPA, die NUR ueber die Yards-Klausel der "
                "muendlichen Regel explosive werden (EPA <= 0 auf demselben Play)."
            ],
            "n": [pass_epa_n],
            "successes": [yards_only_n],
            "rate": [yards_only_stats["rate"]],
            "ci_low": [yards_only_stats["ci_low"]],
            "ci_high": [yards_only_stats["ci_high"]],
            "muted": [yards_only_stats["muted"]],
        },
        schema={
            "definition": pl.Utf8,
            "label_de": pl.Utf8,
            "scope_note": pl.Utf8,
            "n": pl.Int64,
            "successes": pl.Int64,
            "rate": pl.Float64,
            "ci_low": pl.Float64,
            "ci_high": pl.Float64,
            "muted": pl.Boolean,
        },
    )

    # n = every scrimmage row scanned for the `efficiency` column, successes = 0 (the
    # column carries zero values corpus-wide today) -- honest denominator, not a bare
    # "n=0" placeholder, so this row still reads as "0 of N", never as "no data checked".
    efficiency_scanned_n = pass_epa.height if pass_epa_n > 0 else plays.height
    efficiency_row = pl.DataFrame(
        {
            "definition": ["hc_efficiency_status"],
            "label_de": ["HC Efficiency-Spalte (Data!O) im Korpus vorhanden?"],
            "scope_note": [efficiency_status],
            "n": [efficiency_scanned_n],
            "successes": [0],
            "rate": [0.0],
            "ci_low": [None],
            "ci_high": [None],
            "muted": [True],
        },
        schema=yards_only_row.schema,
    )

    workbook_rate = definitions_rows.filter(pl.col("definition") == "baseline_hc_workbook")
    verbal_rate = definitions_rows.filter(pl.col("definition") == "baseline_hc_verbal")
    if workbook_rate.height and verbal_rate.height:
        wb = workbook_rate.row(0, named=True)
        vb = verbal_rate.row(0, named=True)
        print(
            "finding: correlation-free baseline comparison -- "
            f"workbook(yards-only)={wb['rate']:.4f} (n={wb['n']}) vs. "
            f"verbal(yards-or-epa)={vb['rate']:.4f} (n={vb['n']})"
        )

    return pl.concat([definitions_rows, yards_only_row, efficiency_row], how="vertical")


def _assign_pseudonyms(
    counts: pl.DataFrame, *, id_col: str, prefix: str, min_attempts: int
) -> tuple[dict[str, str], list[dict[str, object]]]:
    """Deterministic pseudonym assignment: descending in-scope attempt count, ties
    broken by the SHA-256 of the source label so a rerun is reproducible without
    embedding the label anywhere. Players below `min_attempts` all map to the same
    `"Sonstige (n<X)"` bucket value rather than being dropped or numbered individually.
    """
    rows = counts.to_dicts()
    rows.sort(
        key=lambda r: (-r["n"], hashlib.sha256(r[id_col].encode("utf-8")).hexdigest())
    )

    mapping: dict[str, str] = {}
    key_rows: list[dict[str, object]] = []
    eligible_index = 0
    for row in rows:
        label = row[id_col]
        n = row["n"]
        if n >= min_attempts:
            eligible_index += 1
            pseudonym = f"{prefix}-{eligible_index:02d}"
        else:
            pseudonym = f"Sonstige (n<{min_attempts})"
        mapping[label] = pseudonym
        key_rows.append(
            {"source_label": label, "pseudonym": pseudonym, "n_attempts": n}
        )

    return mapping, key_rows


def _build_by_player_table(
    plays: pl.DataFrame,
    *,
    calibration,
    min_attempts: int,
    pseudonym_key_rows: list[dict[str, object]],
) -> pl.DataFrame:
    """`comparison_by_player.csv`: the same four-definition comparison by QB identity
    and by receiver identity, pseudonymised before `definition_comparison` ever sees
    a real label -- players below `min_attempts` share one pseudonymised identity value
    so `definition_comparison`'s own aggregation (n/successes/CI/shrunk_rate) handles
    the "Sonstige" bucket exactly like any other group, no separate rollup needed.

    QB identity mirrors `reports/own_team.py::player_efficiency`'s fallback
    (`thrown_by` coalesced onto `qb`); receiver identity uses `received_by` directly,
    matching the same module -- neither is re-derived here, both are copied exactly.
    """
    qb_frame = plays.with_columns(_qb_player=pl.coalesce([pl.col("thrown_by"), pl.col("qb")]))
    pass_epa = scrimmage_plays(qb_frame, require_epa=True).filter(pl.col("play_type") == "pass")

    qb_counts = (
        pass_epa.filter(pl.col("_qb_player").is_not_null())
        .group_by("_qb_player")
        .agg(n=pl.len())
    )
    qb_mapping, qb_key_rows = _assign_pseudonyms(
        qb_counts, id_col="_qb_player", prefix="QB", min_attempts=min_attempts
    )
    for row in qb_key_rows:
        row["rolle"] = "QB"
    pseudonym_key_rows.extend(qb_key_rows)

    recv_counts = (
        pass_epa.filter(pl.col("received_by").is_not_null())
        .group_by("received_by")
        .agg(n=pl.len())
    )
    recv_mapping, recv_key_rows = _assign_pseudonyms(
        recv_counts, id_col="received_by", prefix="WR", min_attempts=min_attempts
    )
    for row in recv_key_rows:
        row["rolle"] = "Receiver"
    pseudonym_key_rows.extend(recv_key_rows)

    qb_pseudo_frame = qb_frame.with_columns(
        spieler=pl.col("_qb_player").replace_strict(qb_mapping, default=None, return_dtype=pl.Utf8)
    ).filter(pl.col("spieler").is_not_null())
    qb_table = definition_comparison(qb_pseudo_frame, ["spieler"], calibration=calibration)
    qb_table = qb_table.with_columns(rolle=pl.lit("QB"))

    recv_pseudo_frame = plays.with_columns(
        spieler=pl.col("received_by").replace_strict(
            recv_mapping, default=None, return_dtype=pl.Utf8
        )
    ).filter(pl.col("spieler").is_not_null())
    recv_table = definition_comparison(recv_pseudo_frame, ["spieler"], calibration=calibration)
    recv_table = recv_table.with_columns(rolle=pl.lit("Receiver"))

    combined = pl.concat([qb_table, recv_table], how="vertical")
    return combined.select(list(_BY_PLAYER_COLUMNS)).sort(["rolle", "spieler", "definition"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path, default=Path("ffep.toml"), help="path to ffep.toml (default: ffep.toml)"
    )
    parser.add_argument(
        "--plays",
        type=Path,
        default=None,
        help="path to the scored plays parquet (default: <paths.processed>/plays_scored.parquet)",
    )
    parser.add_argument(
        "--epa-quantile",
        type=float,
        default=DEFAULT_EPA_QUANTILE,
        help=f"successful-play EPA quantile for a fresh calibration (default: {DEFAULT_EPA_QUANTILE})",
    )
    parser.add_argument(
        "--min-attempts",
        type=int,
        default=DEFAULT_MIN_ATTEMPTS,
        help=f"per-player pseudonym threshold below which players share a 'Sonstige' "
        f"bucket (default: {DEFAULT_MIN_ATTEMPTS})",
    )
    parser.add_argument(
        "--recalibrate",
        action="store_true",
        help="derive a fresh calibration threshold instead of loading an existing one",
    )
    args = parser.parse_args(argv)

    config = load_config(args.config)
    plays_path = args.plays or (config.paths.processed / "plays_scored.parquet")
    if not plays_path.exists():
        print(f"scored plays parquet not found: {plays_path}", file=sys.stderr)
        return 1

    plays = pl.read_parquet(plays_path)
    plays = _exclude_tiers(plays, config)
    _print_corpus_census(plays)

    explo_dir = config.paths.reference / "explosiveness"
    explo_dir.mkdir(parents=True, exist_ok=True)
    calibration_path = explo_dir / "calibration.json"
    assert explo_dir.is_relative_to(config.paths.reference), (
        "calibration/comparison artifacts must live under config.paths.reference"
    )

    calibration = _resolve_or_load_calibration(
        plays,
        path=calibration_path,
        epa_quantile=args.epa_quantile,
        recalibrate=args.recalibrate,
    )

    try:
        hc_efficiency_table(plays, group_col="thrown_by")
    except MissingExplosivenessColumns as exc:
        efficiency_status = (
            f"efficiency column absent from corpus -- HC-charted rows not yet ingested "
            f"(M3-1 ingests them into plays.parquet, M3-2 rescoring puts them into "
            f"plays_scored.parquet): {exc}"
        )
        print(f"finding: hc_efficiency_table skipped -- {exc}")
    else:
        efficiency_status = "efficiency column present, hc_efficiency_table computed"
        print("finding: hc_efficiency_table computed successfully")

    overall_table = _build_overall_table(
        plays, calibration=calibration, efficiency_status=efficiency_status
    )
    overall_path = explo_dir / "comparison_overall.csv"
    overall_table.write_csv(overall_path)
    print(f"wrote {overall_path} ({overall_table.height} rows)")

    pseudonym_key_rows: list[dict[str, object]] = []
    by_player_table = _build_by_player_table(
        plays,
        calibration=calibration,
        min_attempts=args.min_attempts,
        pseudonym_key_rows=pseudonym_key_rows,
    )
    by_player_path = explo_dir / "comparison_by_player.csv"
    by_player_table.write_csv(by_player_path)
    print(f"wrote {by_player_path} ({by_player_table.height} rows)")

    cliff_table = cliff_zone_table(plays)
    cliff_path = explo_dir / "cliff_zone.csv"
    cliff_table.write_csv(cliff_path)
    print(f"wrote {cliff_path} ({cliff_table.height} rows)")
    cliff_zone_rows = cliff_table.filter(pl.col("yards_gained").is_between(10, 12))
    cliff_zone_share = cliff_zone_rows["share"].sum()
    if cliff_zone_share is not None:
        print(f"finding: cliff_zone_share_10_12={cliff_zone_share:.4f}")

    pseudonym_key_path = Path("data/processed/m3-03/pseudonym_key.csv")
    assert not pseudonym_key_path.is_relative_to(config.paths.reference), (
        "pseudonym key must never be written under config.paths.reference (it is public)"
    )
    assert not pseudonym_key_path.is_relative_to(Path("docs")), (
        "pseudonym key must never be written under docs/ (it is public)"
    )
    pseudonym_key_path.parent.mkdir(parents=True, exist_ok=True)
    pseudonym_key_df = pl.DataFrame(
        pseudonym_key_rows,
        schema={"source_label": pl.Utf8, "pseudonym": pl.Utf8, "n_attempts": pl.Int64, "rolle": pl.Utf8},
    ).select(["rolle", "pseudonym", "source_label", "n_attempts"]).sort(["rolle", "pseudonym"])
    pseudonym_key_df.write_csv(pseudonym_key_path)
    print(f"wrote {pseudonym_key_path} ({pseudonym_key_df.height} rows, gitignored)")

    print("done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
