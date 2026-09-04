"""Two-arm with/without-head-coach corpus ablation driver for EP/WP training.

This is a driver, not a new training path (M3-02-RESEARCH.md section 5.2, EPA-D04): it
calls the existing `flag_football_ep.model.train.train_ep`/`train_wp` twice each, with two
different input frames (`without_hc` = every non-`hc_workbook:` row, `with_hc` = every row),
tags each resulting MLflow run with the arm it came from, and writes the measured comparison
down as CSVs under `data/reference/epa_refinement/`. No new fold scheme, no new metrics
implementation, and no call anywhere in this module touches the MLflow model registry's
`champion` alias -- promotion stays an explicit human decision on a later plan's checkpoint
(`docs/model-training.md` section 3).

"Frozen folds" under leave-one-game-out (D-07) means: the set of `game_id`s in each arm is
pinned and written down (`corpus_arms.csv`), plus the already-logged `training_data_sha256`
per run -- LOGO is exhaustive over whatever games are in the input frame, so a shared split
object across arms would be meaningless.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from mlflow.tracking import MlflowClient

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from flag_football_ep import reference  # noqa: E402
from flag_football_ep.config import Config, load_config  # noqa: E402
from flag_football_ep.features.mutations import (  # noqa: E402
    WP_PROBABILITY_COLUMN,
    add_competition_tier_features,
    make_ep_model_mutations,
    make_wp_model_mutations,
    prepare_ep_data,
    prepare_wp_data,
)
from flag_football_ep.model import mlflow_store  # noqa: E402
from flag_football_ep.model.evaluate import per_source_metrics  # noqa: E402
from flag_football_ep.model.hyperparams import (  # noqa: E402
    EP_PARAMS,
    EP_PROB_LABELS,
    EP_TRAINING_COLUMNS,
    WP_TRAINING_COLUMNS,
)
from flag_football_ep.model.train import train_ep, train_wp  # noqa: E402

import polars as pl  # noqa: E402

HC_SOURCE_PREFIX = "hc_workbook:"
ARMS: tuple[str, ...] = ("without_hc", "with_hc")

# Timeout / Offsetting Penalties / Penalty -- the three no-play RESULT tokens contract v1.2
# maps to play_type="no_play" (ingest.hudl._RESULT_TOKEN_MAP, no_play_token). Matched as a
# substring of the raw, comma-joined `result_raw` column -- "Penalty" is never a substring
# of "Offsetting Penalties" (the plural "Penalties" diverges from singular "Penalty" at its
# 7th character), so the three counts below never double-count a row.
_NO_PLAY_TOKENS: tuple[str, ...] = ("Timeout", "Offsetting Penalties", "Penalty")

# Per-model prep recipe -- everything `_reconstruct_labeled_frame`/`run_arm` need that
# `model/train.py::train_ep`/`train_wp` already own; kept here only as a lookup table, never
# as a second training implementation.
_MODEL_PREP: dict[str, dict] = {
    "ep": {
        "train_fn": train_ep,
        "prepare_fn": prepare_ep_data,
        "mutate_fn": make_ep_model_mutations,
        "selected_columns": EP_TRAINING_COLUMNS,
        "exclude_key": "exclude_games_ep",
        "experiment_key": "ep_experiment",
        "metric_name": "logo_mlogloss",
        "naive_metric_name": "naive_mlogloss",
        "prob_labels": EP_PROB_LABELS,
        "num_class": EP_PARAMS.get("num_class", 1),
    },
    "wp": {
        "train_fn": train_wp,
        "prepare_fn": prepare_wp_data,
        "mutate_fn": make_wp_model_mutations,
        "selected_columns": WP_TRAINING_COLUMNS,
        "exclude_key": "exclude_games_wp",
        "experiment_key": "wp_experiment",
        "metric_name": "logo_logloss",
        "naive_metric_name": "naive_logloss",
        "prob_labels": [WP_PROBABILITY_COLUMN],
        "num_class": 1,
    },
}


class NoHeadCoachRowsError(ValueError):
    """Raised by `build_arms` when the `with_hc` frame carries zero head-coach rows -- a
    with-vs-without comparison of two identical frames would be meaningless and must not be
    written down as a result."""


@dataclass(frozen=True)
class ArmResult:
    """One `run_arm` call's bookkeeping -- everything `main` needs to build the CSVs
    without re-querying MLflow."""

    model: str
    arm: str
    run_id: str
    experiment: str
    n_plays: int
    n_folds: int
    logo_wall_seconds: float
    training_data_sha256: str
    metric_name: str
    metric_value: float
    naive_metric_name: str
    naive_value: float
    logloss_improvement: float
    per_source_metrics_path: str
    oof_snapshot_path: str


def build_arms(plays: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """`without_hc`/`with_hc` input frames (RESEARCH section 5.2, verbatim shape).

    Raises `NoHeadCoachRowsError` when `with_hc` (== `plays`) has no `hc_workbook:` rows.
    """
    without_hc = plays.filter(~pl.col("source").str.starts_with(HC_SOURCE_PREFIX))
    with_hc = plays
    n_hc = with_hc.height - without_hc.height
    if n_hc <= 0:
        raise NoHeadCoachRowsError(
            "build_arms: the with_hc frame has zero head-coach rows -- a with-vs-without "
            "comparison of two identical frames would be meaningless"
        )
    return {"without_hc": without_hc, "with_hc": with_hc}


def _reconstruct_labeled_frame(frame: pl.DataFrame, config: Config, model: str) -> pl.DataFrame:
    """Rebuild the `(game_id, play_id, source, label)` frame `model/train.py::_train` fit on
    for this arm/model -- the same filter -> `_build_competition_tier` -> `prepare_fn` ->
    `mutate_fn` -> `drop_nulls()` chain `_train` uses internally. `write_oof_predictions`
    never persists a label column (Phase-1.4-facing contract, `model/evaluate.py`), so this
    is how `build_tier_metrics`/`report_no_play_rows` recover the true label for a join on
    `(game_id, play_id, source)` -- not a second metrics implementation, only the existing
    data-prep functions the training run already called.
    """
    spec = _MODEL_PREP[model]
    exclude_ids = list(getattr(config.train, spec["exclude_key"]))
    filtered = frame.filter(~pl.col("game_id").is_in(exclude_ids)) if exclude_ids else frame
    tier_mapping = reference.load_competition_tier(config.reference.competition_tier)
    augmented, _tier_features = add_competition_tier_features(filtered, tier_mapping)
    prepared = spec["prepare_fn"](augmented)
    model_data = spec["mutate_fn"](prepared, spec["selected_columns"]).drop_nulls()
    return model_data.select("game_id", "play_id", "source", "label")


def run_arm(
    model: str, arm_name: str, frame: pl.DataFrame, config: Config, snapshot_dir: Path
) -> ArmResult:
    """Fit one arm via the matching `train_ep`/`train_wp`, tag the run, and snapshot its
    out-of-fold predictions before the next arm's run overwrites the shared
    `oof_predictions_{model}.parquet` path (`model/evaluate.py::write_oof_predictions`
    always writes the same filename -- the with-head-coach arm's file is the one left on
    disk for M3-02-06, per this plan's ordering, so a caller needing the without-arm's
    out-of-fold predictions later must read this snapshot, not the live path).

    Never calls anything in `flag_football_ep.model.registry` -- no alias is ever moved.
    """
    spec = _MODEL_PREP[model]
    run_id = spec["train_fn"](frame, config)

    mlflow_store.configure(config)
    client = MlflowClient()
    client.set_tag(run_id, "corpus_arm", arm_name)
    client.set_tag(run_id, "gsd_phase", "M3-02")
    client.set_tag(run_id, "plan", "M3-02-05")

    run = client.get_run(run_id)
    params = run.data.params
    metrics = run.data.metrics

    per_source_path = client.download_artifacts(run_id, "per_source_metrics.md")

    oof_live = config.paths.processed / f"oof_predictions_{model}.parquet"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    oof_snapshot = snapshot_dir / f"oof_{model}_{arm_name}.parquet"
    oof_snapshot.write_bytes(oof_live.read_bytes())

    metric_name = spec["metric_name"]
    naive_metric_name = spec["naive_metric_name"]
    return ArmResult(
        model=model,
        arm=arm_name,
        run_id=run_id,
        experiment=getattr(config.train, spec["experiment_key"]),
        n_plays=int(params["n_plays"]),
        n_folds=int(params["n_folds"]),
        logo_wall_seconds=float(params["logo_wall_seconds"]),
        training_data_sha256=params["training_data_sha256"],
        metric_name=metric_name,
        metric_value=float(metrics[metric_name]),
        naive_metric_name=naive_metric_name,
        naive_value=float(metrics[naive_metric_name]),
        logloss_improvement=float(metrics["logloss_improvement"]),
        per_source_metrics_path=str(per_source_path),
        oof_snapshot_path=str(oof_snapshot),
    )


def _parse_markdown_table(text: str) -> pl.DataFrame:
    """Parse `model/train.py::_render_markdown_table`'s GitHub-flavoured Markdown back into
    a Polars frame -- reading the run's own logged artifact rather than recomputing
    `per_source_metrics`."""
    lines = [ln for ln in text.strip().splitlines() if ln.strip()]
    if len(lines) < 2:
        return pl.DataFrame()
    header = [c.strip() for c in lines[0].strip().strip("|").split("|")]
    rows = []
    for line in lines[2:]:
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        rows.append(dict(zip(header, cells)))
    if not rows:
        return pl.DataFrame(schema={name: pl.Utf8 for name in header})
    df = pl.DataFrame(rows)
    if "n_plays" in df.columns:
        df = df.with_columns(pl.col("n_plays").cast(pl.Int64))
    for col in ("logloss", "naive_logloss", "improvement"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64))
    return df


def build_source_metrics(model: str, arm_results: list[ArmResult]) -> pl.DataFrame:
    """`per_source_metrics_{model}.csv`: `arm` plus the artifact's own column names
    (`source, n_plays, logloss, naive_logloss, improvement`) -- one block of rows per arm,
    the `__pooled__` row included."""
    frames = []
    for res in arm_results:
        text = Path(res.per_source_metrics_path).read_text()
        table = _parse_markdown_table(text)
        table = table.with_columns(pl.lit(res.arm).alias("arm"))
        ordered = ["arm"] + [c for c in table.columns if c != "arm"]
        frames.append(table.select(ordered))
    return pl.concat(frames, how="vertical")


def build_tier_metrics(
    model: str,
    arm_results: list[ArmResult],
    arm_frames: dict[str, pl.DataFrame],
    config: Config,
) -> pl.DataFrame:
    """`per_tier_metrics_{model}.csv`: `arm, competition_tier, n, logloss, naive_logloss,
    improvement` -- the HC-03 Tier-Eval. Joins each arm's snapshotted out-of-fold
    predictions to the reconstructed label frame (by `(game_id, play_id, source)`) and to
    `plays.parquet`'s per-game `competition` (by `game_id`) and `competition_tier.csv`'s
    `(source, competition) -> tier` map, then calls the EXISTING
    `model.evaluate.per_source_metrics` with the tier array standing in for the source
    array -- the same fair, own-group-naive-baseline computation, just grouped by tier
    instead of by source.
    """
    spec = _MODEL_PREP[model]
    tier_mapping = reference.load_competition_tier(config.reference.competition_tier).rename(
        {"tier": "competition_tier"}
    )

    frames = []
    for res in arm_results:
        oof = pl.read_parquet(res.oof_snapshot_path)
        labeled = _reconstruct_labeled_frame(arm_frames[res.arm], config, model)
        joined = oof.join(labeled, on=["game_id", "play_id", "source"], how="inner")

        comp_lookup = arm_frames[res.arm].select("game_id", "competition").unique()
        joined = joined.join(comp_lookup, on="game_id", how="left")
        joined = joined.join(tier_mapping, on=["source", "competition"], how="left")
        joined = joined.with_columns(
            pl.col("competition_tier").fill_null("__unmapped__").alias("competition_tier")
        )

        oof_pred = joined.select(spec["prob_labels"]).to_numpy()
        oof_label = joined["label"].to_numpy()
        tier_arr = joined["competition_tier"].to_numpy()

        tier_table = per_source_metrics(oof_pred, oof_label, tier_arr, spec["num_class"])
        tier_table = tier_table.rename({"source": "competition_tier", "n_plays": "n"})
        tier_table = tier_table.with_columns(pl.lit(res.arm).alias("arm"))
        frames.append(
            tier_table.select(["arm", "competition_tier", "n", "logloss", "naive_logloss", "improvement"])
        )
    return pl.concat(frames, how="vertical")


def report_no_play_rows(plays: pl.DataFrame, config: Config) -> pl.DataFrame:
    """`no_play_rows.csv`: `source, token, rows, share_of_source_rows,
    rows_surviving_to_ep_training` -- the measured evidence behind the Timeout/Offsetting-
    Penalties/Penalty keep-or-filter decision (ROADMAP Milestone 3 M3-2 note,
    `docs/hc-rueckfragen-2026-09.md` Frage 3). One row per `(source, token)` for the three
    no-play tokens plus one `__any__` total row per source (a play can carry more than one
    token, e.g. "Rush, Penalty" -- `__any__` counts a row once regardless of how many of the
    three tokens it carries).
    """
    total_by_source = {
        row["source"]: row["n"]
        for row in plays.group_by("source").agg(pl.len().alias("n")).iter_rows(named=True)
    }

    labeled = _reconstruct_labeled_frame(plays, config, "ep")
    surviving = set(
        zip(labeled["game_id"].to_list(), labeled["play_id"].to_list(), labeled["source"].to_list())
    )

    def _rows_for(mask_expr: pl.Expr, token: str) -> list[dict]:
        matched = plays.filter(mask_expr)
        by_source = matched.group_by("source").agg(
            pl.len().alias("rows"), game_id=pl.col("game_id"), play_id=pl.col("play_id")
        )
        out = []
        for row in by_source.iter_rows(named=True):
            source = row["source"]
            n_rows = row["rows"]
            n_survive = sum(
                1
                for g, p in zip(row["game_id"], row["play_id"])
                if (g, p, source) in surviving
            )
            out.append(
                {
                    "source": source,
                    "token": token,
                    "rows": n_rows,
                    "share_of_source_rows": n_rows / total_by_source.get(source, 1),
                    "rows_surviving_to_ep_training": n_survive,
                }
            )
        return out

    rows: list[dict] = []
    token_exprs = []
    for token in _NO_PLAY_TOKENS:
        expr = pl.col("result_raw").fill_null("").str.contains(token, literal=True)
        token_exprs.append(expr)
        rows.extend(_rows_for(expr, token))

    any_expr = token_exprs[0]
    for expr in token_exprs[1:]:
        any_expr = any_expr | expr
    rows.extend(_rows_for(any_expr, "__any__"))

    if not rows:
        return pl.DataFrame(
            schema={
                "source": pl.Utf8,
                "token": pl.Utf8,
                "rows": pl.Int64,
                "share_of_source_rows": pl.Float64,
                "rows_surviving_to_ep_training": pl.Int64,
            }
        )
    return pl.DataFrame(rows).sort(["source", "token"])


def _build_corpus_arms_table(plays: pl.DataFrame) -> pl.DataFrame:
    """`corpus_arms.csv`: every `game_id` with a boolean per arm -- the pinned, reproducible
    game-id set behind "frozen folds" under leave-one-game-out."""
    hc_games = set(
        plays.filter(pl.col("source").str.starts_with(HC_SOURCE_PREFIX))["game_id"]
        .unique()
        .to_list()
    )
    all_games = plays.select("game_id").unique().sort("game_id")
    return all_games.with_columns(
        pl.lit(True).alias("with_hc"),
        (~pl.col("game_id").is_in(sorted(hc_games))).alias("without_hc"),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("ffep.toml"), help="Path to ffep.toml")
    parser.add_argument("--model", choices=("ep", "wp", "both"), default="both")
    parser.add_argument("--dry-run", action="store_true", help="print arm game/row counts, fit nothing")
    parser.add_argument(
        "--out-dir", type=Path, default=Path("data/reference/epa_refinement"),
        help="Output directory for the ablation CSVs",
    )
    parser.add_argument(
        "--no-play-report", action="store_true",
        help="write only no_play_rows.csv (no training)",
    )
    args = parser.parse_args(argv)

    config = load_config(args.config)
    plays = pl.read_parquet(config.paths.processed / "plays.parquet")
    arms = build_arms(plays)

    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.no_play_report:
        no_play = report_no_play_rows(plays, config)
        out_path = args.out_dir / "no_play_rows.csv"
        no_play.write_csv(out_path)
        print(f"wrote {out_path} ({no_play.height} row(s))")
        return 0

    if args.dry_run:
        for arm_name in ARMS:
            frame = arms[arm_name]
            print(
                f"{arm_name}: {frame['game_id'].n_unique()} game(s), {frame.height} row(s)"
            )
        print("[dry-run] nothing fitted")
        return 0

    models = ["ep", "wp"] if args.model == "both" else [args.model]

    snapshot_dir = Path(tempfile.mkdtemp(prefix="hc_corpus_ablation_"))
    all_results: list[ArmResult] = []
    for model in models:
        for arm_name in ARMS:  # without_hc then with_hc: the with_hc oof file is the one
            frame = arms[arm_name]  # left on disk for M3-02-06 to consume.
            result = run_arm(model, arm_name, frame, config, snapshot_dir)
            all_results.append(result)
            print(
                f"{model}/{arm_name}: run={result.run_id} n_plays={result.n_plays} "
                f"n_folds={result.n_folds} {result.metric_name}={result.metric_value:.6f} "
                f"{result.naive_metric_name}={result.naive_value:.6f} "
                f"improvement={result.logloss_improvement:.6f} "
                f"logo_wall_seconds={result.logo_wall_seconds:.1f}"
            )

    summary_rows = [
        {
            "model": r.model,
            "arm": r.arm,
            "run_id": r.run_id,
            "experiment": r.experiment,
            "n_plays": r.n_plays,
            "n_folds": r.n_folds,
            "logo_wall_seconds": r.logo_wall_seconds,
            "training_data_sha256": r.training_data_sha256,
            "metric_name": r.metric_name,
            "metric_value": r.metric_value,
            "naive_metric_name": r.naive_metric_name,
            "naive_value": r.naive_value,
            "logloss_improvement": r.logloss_improvement,
        }
        for r in all_results
    ]
    pl.DataFrame(summary_rows).write_csv(args.out_dir / "ablation_summary.csv")
    _build_corpus_arms_table(plays).write_csv(args.out_dir / "corpus_arms.csv")

    for model in models:
        model_results = [r for r in all_results if r.model == model]
        build_source_metrics(model, model_results).write_csv(
            args.out_dir / f"per_source_metrics_{model}.csv"
        )
        build_tier_metrics(model, model_results, arms, config).write_csv(
            args.out_dir / f"per_tier_metrics_{model}.csv"
        )

    report_no_play_rows(plays, config).write_csv(args.out_dir / "no_play_rows.csv")

    print(
        "No champion alias was moved by this run -- every model above was registered as a "
        "new version only; moving the champion alias stays a separate, explicit human "
        "decision (docs/model-training.md section 3)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
