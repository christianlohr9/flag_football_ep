"""EP and WP model training, with MLflow as the tracking/artifact backend.

Ports `models/ep_model.ipynb` (cells 2, 6, 7, 9-14, 16) and `models/wp_model.ipynb`
(cells 2, 5, 6, 8-10, 12) behaviour-for-behaviour on features and fixed hyperparameters (see
`flag_football_ep.model.hyperparams`). The evaluation protocol itself is not ported: the
notebooks' play-level 80/20 random split (test_size 0.2, fixed seed) let plays from the
same game land in both train and test, optimistically biasing every reported metric.
REQ-S1-07 / D-07 replace it with leave-one-game-out (LOGO) measurement over `game_id`
(`flag_football_ep.model.evaluate.run_logo`) -- every game is held out exactly once, and the
reported metric is the pooled out-of-fold log-loss. The shipped model is a single refit on
every game with the tuned params; the fold models `run_logo` fits are measurement-only and
are never exported or registered. Also, every run gets its own MLflow run (params, metrics,
model artifact) instead of the notebook's `pickle`-based dump into a fixed `"ep_model.pkl"`
filename, which silently overwrote the same file on every run (T-1.2-07, threat register in
the plan).

`train_ep` and `train_wp` are thin, model-specific wrappers around the shared `_train`
pipeline (filter -> prepare -> mutate -> drop_nulls -> tune (grouped inner CV) -> LOGO
measurement -> refit once on all games -> log) and the shared `_log_run` MLflow helper, so
the two paths cannot drift from each other on the params/metrics/artifact contract.

Hyperopt tuning (`tune=True`) is preserved behind an explicit flag rather than run on every
`ffep train`: RESEARCH Assumption A1 / Open Question 1 record that the notebook's tuned
hyperparameters were never persisted, so the fixed cell-7/cell-6 values are the only
recoverable defaults, and a 100-trial search on every train would make the phase-1.4
ten-minute `ffep run` goal unreachable. `tune=False` (the default) never imports or calls
`hyperopt.fmin`; passing `--tune` runs the ported search (`model_objective` below, from
ep_model.ipynb cell 10 / wp_model.ipynb cell 9) and refits on the full frame with the best
trial's hyperparameters (ep_model.ipynb cell 14 / wp_model.ipynb cell 12).

`export_pkl=True` additionally writes a dated, hash-suffixed `.pkl` under `config.paths.
models` for compatibility with existing consumers of the notebook's `{ep|wp}_model.pkl`
convention -- MLflow (via `_log_run`) is the primary artifact store; REQ-S1-11 (phase 1.3)
owns the full versioning scheme this export is a placeholder for.

The MLflow tracking store is SQLite-backed (`flag_football_ep.model.mlflow_store`), not the
deprecated local `file:` store -- the MLflow model registry (REQ-S1-11, "MLflow model
registry is primary") requires a database-backed store to support `register_model`/alias
APIs, which the `file:` FileStore does not implement.
"""

from __future__ import annotations

import hashlib
import io
import pickle
import re
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import mlflow.xgboost
import numpy as np
import polars as pl
import xgboost as xgb
from hyperopt import STATUS_OK, Trials, fmin, tpe
from sklearn.metrics import log_loss
from sklearn.model_selection import GroupKFold

from flag_football_ep.config import Config
from flag_football_ep.features.mutations import (
    WP_PROBABILITY_COLUMN,
    make_ep_model_mutations,
    make_wp_model_mutations,
    prepare_ep_data,
    prepare_wp_data,
)
from flag_football_ep.model import mlflow_store, registry
from flag_football_ep.model.evaluate import (
    naive_baseline_logloss,
    per_source_metrics,
    reliability_curves,
    render_reliability_figure,
    run_logo,
    write_oof_predictions,
)
from flag_football_ep.model.hyperparams import (
    EP_FEATURES,
    EP_PARAMS,
    EP_PROB_LABELS,
    EP_TRAINING_COLUMNS,
    HYPEROPT_QUNIFORM_KEYS,
    HYPEROPT_SPACE,
    INNER_CV_FOLDS,
    LOGO_GROUP_COLUMN,
    TUNE_EARLY_STOPPING_ROUNDS,
    WP_FEATURES,
    WP_PARAMS,
    WP_TRAINING_COLUMNS,
)

# T-1.3-16: MLflow metric keys must stay alphanumeric/underscore -- a `source` value from
# the canonical Parquet is interpolated into a `logo_logloss_by_source_<source>` metric key,
# so it is sanitised the same way T-1.2-08 validates identifiers before URI interpolation.
_METRIC_KEY_UNSAFE = re.compile(r"[^A-Za-z0-9_]")

# ep_model.ipynb cell 14 / wp_model.ipynb cell 12: the tuned refit only carries these
# structural settings forward from the fixed params -- `subsample` and any other fixed-only
# key are intentionally dropped, matching the notebooks' refit cells.
_TUNE_STRUCTURAL_KEYS = ("booster", "objective", "eval_metric", "num_class")


class MissingTrainingColumns(ValueError):
    """Raised when the input frame lacks a column `train_ep`/`train_wp` needs."""


def train_ep(
    plays: pl.DataFrame,
    config: Config,
    tune: bool = False,
    max_evals: int = 100,
    export_pkl: bool = False,
) -> str:
    """Fit the EP model on `plays` and log the run to MLflow. Returns the MLflow run id."""
    return _train(
        plays=plays,
        config=config,
        experiment=config.train.ep_experiment,
        exclude_ids=list(config.train.exclude_games_ep),
        prepare_fn=prepare_ep_data,
        mutate_fn=make_ep_model_mutations,
        selected_columns=EP_TRAINING_COLUMNS,
        features=EP_FEATURES,
        fixed_params=EP_PARAMS,
        metric_name="logo_mlogloss",
        weight_column="Total_W_Scaled",
        model_prefix="ep",
        prob_labels=EP_PROB_LABELS,
        tune=tune,
        max_evals=max_evals,
        export_pkl=export_pkl,
    )


def train_wp(
    plays: pl.DataFrame,
    config: Config,
    tune: bool = False,
    max_evals: int = 100,
    export_pkl: bool = False,
) -> str:
    """Fit the WP model on `plays` and log the run to MLflow. Returns the MLflow run id.

    Unlike `train_ep`, the WP fit uses no sample weights -- `wp_model.ipynb` cell 6 (and its
    tuned refit, cell 12) fit without one.
    """
    return _train(
        plays=plays,
        config=config,
        experiment=config.train.wp_experiment,
        exclude_ids=list(config.train.exclude_games_wp),
        prepare_fn=prepare_wp_data,
        mutate_fn=make_wp_model_mutations,
        selected_columns=WP_TRAINING_COLUMNS,
        features=WP_FEATURES,
        fixed_params=WP_PARAMS,
        metric_name="logo_logloss",
        weight_column=None,
        model_prefix="wp",
        prob_labels=[WP_PROBABILITY_COLUMN],
        tune=tune,
        max_evals=max_evals,
        export_pkl=export_pkl,
    )


def _train(
    *,
    plays: pl.DataFrame,
    config: Config,
    experiment: str,
    exclude_ids: list[str],
    prepare_fn: Callable[[pl.DataFrame], pl.DataFrame],
    mutate_fn: Callable[[pl.DataFrame, Sequence[str]], pl.DataFrame],
    selected_columns: Sequence[str],
    features: Sequence[str],
    fixed_params: dict,
    metric_name: str,
    weight_column: str | None,
    model_prefix: str,
    prob_labels: Sequence[str],
    tune: bool,
    max_evals: int,
    export_pkl: bool,
) -> str:
    """Shared EP/WP fit-and-log pipeline.

    filter excluded games -> `prepare_fn` -> `mutate_fn` -> `drop_nulls()` -> tune (grouped
    inner CV) -> LOGO measurement -> calibration report + out-of-fold persistence -> refit
    once on all games -> `_log_run` (registers the production refit) -> optional dated `.pkl`
    export. REQ-S1-07 / D-07: no game's plays ever appear in both a fold's train and test
    partition, and the model that ships is a single refit on every game -- the LOGO fold
    models are measurement-only. `train_ep`/`train_wp` only supply what differs between the
    two paths; this is the one place the pipeline shape itself lives.
    """
    try:
        filtered = (
            plays.filter(~pl.col("game_id").is_in(exclude_ids)) if exclude_ids else plays
        )
        prepared = prepare_fn(filtered)
        model_data = mutate_fn(prepared, selected_columns).drop_nulls()
    except pl.exceptions.ColumnNotFoundError as exc:
        raise MissingTrainingColumns(
            f"{model_prefix}: input frame is missing a required column: {exc}"
        ) from exc

    missing = [c for c in features if c not in model_data.columns]
    if missing:
        raise MissingTrainingColumns(
            f"{model_prefix}: missing required feature column(s): {', '.join(missing)}"
        )

    training_data_sha256 = _hash_frame(model_data)

    if tune:
        fit_params, trial_losses, best_params = _tune(
            model_data, features, weight_column, fixed_params, max_evals
        )
    else:
        fit_params = dict(fixed_params)
        trial_losses = []
        best_params = None

    # Measurement: leave-one-game-out over LOGO_GROUP_COLUMN, using the (possibly tuned)
    # fit_params -- never the play-level split the notebooks used.
    logo = run_logo(
        model_data=model_data,
        features=features,
        fixed_params=fit_params,
        weight_column=weight_column,
    )
    num_class = fixed_params.get("num_class")
    labels = list(range(num_class)) if num_class else None
    oof_pred_for_metric = logo.oof_pred if num_class else logo.oof_pred.ravel()
    metric_value = float(log_loss(logo.oof_label, oof_pred_for_metric, labels=labels))

    # REQ-S1-08 calibration report: naive class-frequency baseline, per-class reliability
    # curves and the per-source breakdown, all computed from the LOGO out-of-fold arrays
    # (report only -- no calibration correction). `num_class or 1` matches
    # `naive_baseline_logloss`/`reliability_curves`/`per_source_metrics`'s binary convention
    # (WP's `fixed_params` carries no `num_class` key).
    report_num_class = num_class or 1
    naive = naive_baseline_logloss(logo.oof_label, report_num_class)
    curves = reliability_curves(logo.oof_pred, logo.oof_label, prob_labels)
    source_table = per_source_metrics(logo.oof_pred, logo.oof_label, logo.oof_source, report_num_class)

    # CONTEXT §"Grouped-CV protocol" > Out-of-fold EPA: persisted for Phase 1.4's own-team
    # EPA reporting so it is never flattered by in-sample predictions.
    oof_path = write_oof_predictions(logo, prob_labels, config, model_prefix)

    # Production refit: one fresh model fit on the *whole* frame with the tuned params --
    # this is the artifact `_log_run`/`_export_pickle` ship. Separate from the LOGO fold
    # models above, which are measurement-only and are never returned or logged.
    fit_X = model_data.select(list(features)).to_numpy()
    fit_y = model_data.select("label").to_numpy()
    fit_weight = (
        model_data.select(weight_column).to_numpy().ravel()
        if weight_column is not None
        else None
    )
    model = xgb.XGBRegressor(**fit_params)
    model.fit(fit_X, fit_y, sample_weight=fit_weight)

    params = {
        **{str(key): value for key, value in fixed_params.items()},
        "n_plays": model_data.height,
        "n_features": len(features),
        "cv_scheme": "leave-one-game-out",
        "group_column": LOGO_GROUP_COLUMN,
        "n_folds": logo.n_folds,
        "logo_wall_seconds": round(logo.wall_seconds, 3),
        "training_data_sha256": training_data_sha256,
        "excluded_game_ids": ",".join(exclude_ids),
        "tuned": tune,
        "max_evals": max_evals,
        "oof_predictions_path": str(oof_path),
    }
    if best_params is not None:
        params.update({f"best_{key}": value for key, value in best_params.items()})

    naive_metric_name = f"naive_{metric_name.removeprefix('logo_')}"
    metrics = {
        metric_name: metric_value,
        naive_metric_name: naive,
        "logloss_improvement": naive - metric_value,
    }
    sanitized_notes = []
    for row in source_table.iter_rows(named=True):
        if row["source"] == "__pooled__":
            continue
        safe_source = _METRIC_KEY_UNSAFE.sub("_", row["source"])
        if safe_source != row["source"]:
            sanitized_notes.append(f"{row['source']}->{safe_source}")
        metrics[f"logo_logloss_by_source_{safe_source}"] = row["logloss"]
    if sanitized_notes:
        params["sanitized_source_metric_keys"] = ",".join(sanitized_notes)

    figure = render_reliability_figure(
        curves, f"{model_prefix.upper()} reliability (out-of-fold, leave-one-game-out)"
    )
    try:
        run_id = _log_run(
            experiment=experiment,
            params=params,
            metrics=metrics,
            model=model,
            feature_names=list(features),
            config=config,
            trial_losses=trial_losses,
            figures={f"reliability_{model_prefix}.png": figure},
            texts={"per_source_metrics.md": _render_markdown_table(source_table)},
            register_as=registry.registered_model_name(model_prefix),
        )
    finally:
        import matplotlib.pyplot as plt

        plt.close(figure)

    if export_pkl:
        _export_pickle(model, config, model_prefix, training_data_sha256)

    return run_id


def _render_markdown_table(df: pl.DataFrame) -> str:
    """Render `df` as a GitHub-flavoured Markdown table -- used for the per-source metrics
    artifact logged alongside every training run."""
    columns = df.columns
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    rows = [
        "| " + " | ".join(str(row[col]) for col in columns) + " |"
        for row in df.iter_rows(named=True)
    ]
    return "\n".join([header, separator, *rows])


def _tune(
    model_data: pl.DataFrame,
    features: Sequence[str],
    weight_column: str | None,
    fixed_params: dict,
    max_evals: int,
) -> tuple[dict, list[float], dict]:
    """Run the ported hyperopt search (ep_model.ipynb cells 9-11 / wp_model.ipynb cells 8-10)
    over a grouped inner CV, and build the tuned-refit params (cell 14 / cell 12).

    Each trial is scored on `GroupKFold(n_splits=min(INNER_CV_FOLDS, n_distinct_games))` over
    `LOGO_GROUP_COLUMN`, not a single fixed play-level split -- REQ-S1-07/D-07: no game's
    plays may leak between a trial's train and eval partitions. This inner CV is deliberately
    not LOGO -- scoring every trial with a full leave-one-game-out pass would multiply the
    already-expensive LOGO fit count by `max_evals` (RESEARCH "Anti-Patterns to Avoid": the
    LOGO x trials fit-count explosion). LOGO itself runs exactly once, after tuning, in
    `_train`.

    Returns `(fit_params, trial_losses, best_params)`: `fit_params` is what the final model
    is fit with (structural keys from `fixed_params` plus the tuned numeric keys, no
    `subsample` -- exactly what the notebooks' refit cells construct); `trial_losses` is
    every trial's mean inner-fold log_loss in trial order (for the stepped `tune_logloss`
    metric); `best_params` is every searched key (int-cast where `HYPEROPT_QUNIFORM_KEYS`
    says to), logged as `best_`-prefixed run params.
    """
    X_np = model_data.select(list(features)).to_numpy()
    y_np = model_data.select("label").to_numpy().ravel()
    groups_np = model_data.select(LOGO_GROUP_COLUMN).to_numpy().ravel()
    weight_np = (
        model_data.select(weight_column).to_numpy().ravel() if weight_column is not None else None
    )
    # Computed once from the full frame so an inner fold missing a rare class cannot make
    # log_loss raise (mirrors _train's `labels=list(range(num_class))` pre-flight).
    class_labels = sorted(set(np.unique(y_np)))
    n_distinct_games = len(set(groups_np))
    n_splits = min(INNER_CV_FOLDS, n_distinct_games)
    group_kfold = GroupKFold(n_splits=n_splits)

    def _objective(space: dict) -> dict:
        # ep_model.ipynb cell 10 / wp_model.ipynb cell 9 ("model_objective"), ported: an
        # XGBClassifier per inner fold with early stopping against that fold's held-out
        # partition. Scored by sklearn log_loss on predict_proba, mean over inner folds.
        # `reg_alpha`/`colsample_bytree` stay float here (see HYPEROPT_QUNIFORM_KEYS
        # docstring for why the notebooks' int-cast is not ported).
        fold_losses = []
        for train_idx, eval_idx in group_kfold.split(X_np, y_np, groups=groups_np):
            clf = xgb.XGBClassifier(
                n_estimators=int(space["n_estimators"]),
                eta=space["eta"],
                max_depth=int(space["max_depth"]),
                gamma=space["gamma"],
                reg_alpha=space["reg_alpha"],
                min_child_weight=int(space["min_child_weight"]),
                colsample_bytree=space["colsample_bytree"],
                early_stopping_rounds=TUNE_EARLY_STOPPING_ROUNDS,
            )
            fold_weight = weight_np[train_idx] if weight_np is not None else None
            clf.fit(
                X_np[train_idx],
                y_np[train_idx],
                sample_weight=fold_weight,
                eval_set=[(X_np[train_idx], y_np[train_idx]), (X_np[eval_idx], y_np[eval_idx])],
                verbose=False,
            )
            pred_proba = clf.predict_proba(X_np[eval_idx])
            fold_losses.append(log_loss(y_np[eval_idx], pred_proba, labels=class_labels))
        loss = float(np.mean(fold_losses))
        return {"loss": loss, "status": STATUS_OK}

    trials = Trials()
    best = fmin(
        fn=_objective,
        space=HYPEROPT_SPACE,
        algo=tpe.suggest,
        max_evals=max_evals,
        trials=trials,
    )

    trial_losses = [trial["result"]["loss"] for trial in trials.trials]

    best_params = dict(best)
    for key in HYPEROPT_QUNIFORM_KEYS:
        if key in best_params:
            best_params[key] = int(best_params[key])

    fit_params = {key: fixed_params[key] for key in _TUNE_STRUCTURAL_KEYS if key in fixed_params}
    fit_params.update(
        {
            "n_estimators": best_params["n_estimators"],
            "eta": best_params["eta"],
            "gamma": best_params["gamma"],
            "colsample_bytree": best_params["colsample_bytree"],
            "max_depth": best_params["max_depth"],
            "min_child_weight": best_params["min_child_weight"],
            "reg_alpha": best["reg_alpha"],
            "reg_lambda": best["reg_lambda"],
        }
    )

    return fit_params, trial_losses, best_params


def _log_run(
    *,
    experiment: str,
    params: dict,
    metrics: dict,
    model: xgb.XGBRegressor,
    feature_names: list[str],
    config: Config,
    trial_losses: list[float] | None = None,
    figures: dict[str, "Figure"] | None = None,  # noqa: F821
    texts: dict[str, str] | None = None,
    register_as: str | None = None,
) -> str:
    """Log one training run to the configured local MLflow store. Returns the run id.

    Params, metrics, a `phase=01.3` tag and the model artifact are logged the same way for
    every training path -- the one place this contract is defined, so `train_ep`/`train_wp`
    cannot drift from each other. `trial_losses`, when given, is logged as the stepped
    `tune_logloss` metric so a hyperopt search stays inspectable in the MLflow UI. `figures`
    and `texts` are logged as MLflow artifacts (`mlflow.log_figure`/`mlflow.log_text`) -- the
    REQ-S1-08 calibration report form: MLflow run artifacts, browsable in the MLflow UI, no
    separate standalone report file. When `register_as` is given, the model is registered as
    a new version of that name (REQ-S1-11) instead of a bare unregistered model log --
    becoming `champion` still requires an explicit `ffep promote` (CONTEXT §Promotion).
    """
    model.get_booster().feature_names = feature_names

    mlflow_store.ensure_experiment(experiment, config)
    with mlflow.start_run() as run:
        mlflow.log_params(params)
        for metric_name, value in metrics.items():
            mlflow.log_metric(metric_name, value)
        for step, loss in enumerate(trial_losses or []):
            mlflow.log_metric("tune_logloss", loss, step=step)
        for artifact_file, fig in (figures or {}).items():
            mlflow.log_figure(fig, artifact_file)
        for artifact_file, text in (texts or {}).items():
            mlflow.log_text(text, artifact_file)
        mlflow.set_tag("phase", "01.3")
        if register_as is not None:
            registry.register_production_model(model, register_as, config)
        else:
            mlflow.xgboost.log_model(model, name="model")
        return run.info.run_id


def _export_pickle(
    model: xgb.XGBRegressor, config: Config, model_prefix: str, training_data_sha256: str
) -> Path:
    """Write a dated, hash-suffixed `.pkl` export -- never a fixed filename (T-1.2-07).

    MLflow (`_log_run`'s `mlflow.xgboost.log_model`) is the primary artifact store; this
    export exists only for compatibility with existing consumers of the notebook's
    `{ep|wp}_model.pkl` convention. REQ-S1-11 (phase 1.3) owns the full versioning scheme.
    """
    config.paths.models.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    hash_suffix = training_data_sha256[:8]
    out_path = config.paths.models / f"{model_prefix}_model_{date_str}_{hash_suffix}.pkl"
    if out_path.exists():
        raise FileExistsError(f"refusing to overwrite existing model export: {out_path}")
    with out_path.open("wb") as f:
        pickle.dump(model, f)
    return out_path


def _hash_frame(df: pl.DataFrame) -> str:
    """SHA-256 over the training frame's Parquet bytes -- deterministic given row order."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    return hashlib.sha256(buf.getvalue()).hexdigest()
