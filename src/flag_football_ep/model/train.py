"""EP and WP model training, with MLflow as the tracking/artifact backend.

Ports `models/ep_model.ipynb` (cells 2, 6, 7, 9-14, 16) and `models/wp_model.ipynb`
(cells 2, 5, 6, 8-10, 12) behaviour-for-behaviour: same features, same
`train_test_split(test_size=0.2, random_state=42)`, same fixed hyperparameters (see
`flag_football_ep.model.hyperparams`), same metric. The one intentional behaviour change is
the artifact sink -- every run gets its own MLflow run (params, metrics, model artifact)
instead of the notebook's `pickle`-based dump into a fixed `"ep_model.pkl"` filename, which
silently overwrote the same file on every run (T-1.2-07, threat register in the plan).

`train_ep` and `train_wp` are thin, model-specific wrappers around the shared `_train`
pipeline (filter -> prepare -> mutate -> drop_nulls -> split -> fit or tune -> evaluate) and
the shared `_log_run` MLflow helper, so the two paths cannot drift from each other on the
params/metrics/artifact contract.

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
from sklearn.model_selection import train_test_split

from flag_football_ep.config import Config
from flag_football_ep.features.mutations import (
    make_ep_model_mutations,
    make_wp_model_mutations,
    prepare_ep_data,
    prepare_wp_data,
)
from flag_football_ep.model import mlflow_store
from flag_football_ep.model.hyperparams import (
    EP_FEATURES,
    EP_PARAMS,
    EP_SELECTED_COLUMNS,
    HYPEROPT_QUNIFORM_KEYS,
    HYPEROPT_SPACE,
    TUNE_EARLY_STOPPING_ROUNDS,
    WP_FEATURES,
    WP_PARAMS,
    WP_SELECTED_COLUMNS,
)

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
        selected_columns=EP_SELECTED_COLUMNS,
        features=EP_FEATURES,
        fixed_params=EP_PARAMS,
        metric_name="test_mlogloss",
        weight_column="Total_W_Scaled",
        model_prefix="ep",
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
        selected_columns=WP_SELECTED_COLUMNS,
        features=WP_FEATURES,
        fixed_params=WP_PARAMS,
        metric_name="test_logloss",
        weight_column=None,
        model_prefix="wp",
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
    tune: bool,
    max_evals: int,
    export_pkl: bool,
) -> str:
    """Shared EP/WP fit-and-log pipeline.

    filter excluded games -> `prepare_fn` -> `mutate_fn` -> `drop_nulls()` -> 80/20 split ->
    fit (fixed params on the train split) or tune (hyperopt search on the split, then refit
    `xgb.XGBRegressor` on the *full* frame with the best trial's params -- matching the
    notebooks' tuned-refit cells) -> evaluate `metric_name` on the held-out test split ->
    `_log_run` -> optional dated `.pkl` export. `train_ep`/`train_wp` only supply what
    differs between the two paths; this is the one place the pipeline shape itself lives.
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

    X = model_data.select(list(features))
    y = model_data.select("label")
    train_X, test_X, train_y, test_y = train_test_split(X, y, test_size=0.2, random_state=42)

    if weight_column is not None:
        # Same split settings applied to the weight column separately, exactly as the
        # notebook does (ep_model.ipynb cell 6) -- row order is identical so the split
        # lines up with X/y.
        weight_train, _weight_test = train_test_split(
            model_data.select(weight_column), test_size=0.2, random_state=42
        )
    else:
        weight_train = None

    training_data_sha256 = _hash_frame(model_data)

    if tune:
        fit_params, trial_losses, best_params = _tune(
            train_X, train_y, test_X, test_y, fixed_params, max_evals
        )
        fit_X = model_data.select(list(features)).to_numpy()
        fit_y = model_data.select("label").to_numpy()
        fit_weight = (
            model_data.select(weight_column).to_numpy().ravel()
            if weight_column is not None
            else None
        )
    else:
        fit_params = dict(fixed_params)
        trial_losses = []
        best_params = None
        fit_X = train_X.to_numpy()
        fit_y = train_y.select("label").to_numpy()
        fit_weight = weight_train.to_numpy().ravel() if weight_train is not None else None

    model = xgb.XGBRegressor(**fit_params)
    model.fit(fit_X, fit_y, sample_weight=fit_weight)

    y_pred = model.predict(test_X.to_numpy())
    # Explicit `labels` covering every class the model can output: a small held-out split
    # can easily miss a rare class, and log_loss raises rather than silently comparing a
    # subset of columns against y_pred's full width.
    num_class = fixed_params.get("num_class")
    labels = list(range(num_class)) if num_class else None
    metric_value = float(log_loss(test_y.to_numpy().ravel(), y_pred, labels=labels))

    params = {
        **{str(key): value for key, value in fixed_params.items()},
        "n_plays": model_data.height,
        "n_features": len(features),
        "test_size": 0.2,
        "random_state": 42,
        "training_data_sha256": training_data_sha256,
        "excluded_game_ids": ",".join(exclude_ids),
        "tuned": tune,
        "max_evals": max_evals,
    }
    if best_params is not None:
        params.update({f"best_{key}": value for key, value in best_params.items()})

    run_id = _log_run(
        experiment=experiment,
        params=params,
        metrics={metric_name: metric_value},
        model=model,
        feature_names=list(features),
        config=config,
        trial_losses=trial_losses,
    )

    if export_pkl:
        _export_pickle(model, config, model_prefix, training_data_sha256)

    return run_id


def _tune(
    train_X: pl.DataFrame,
    train_y: pl.DataFrame,
    test_X: pl.DataFrame,
    test_y: pl.DataFrame,
    fixed_params: dict,
    max_evals: int,
) -> tuple[dict, list[float], dict]:
    """Run the ported hyperopt search (ep_model.ipynb cells 9-11 / wp_model.ipynb cells 8-10)
    and build the tuned-refit params (cell 14 / cell 12).

    Returns `(fit_params, trial_losses, best_params)`: `fit_params` is what the final model
    is fit with (structural keys from `fixed_params` plus the tuned numeric keys, no
    `subsample` -- exactly what the notebooks' refit cells construct); `trial_losses` is
    every trial's held-out log_loss in trial order (for the stepped `tune_logloss` metric);
    `best_params` is every searched key (int-cast where `HYPEROPT_QUNIFORM_KEYS` says to),
    logged as `best_`-prefixed run params.
    """
    train_X_np = train_X.to_numpy()
    train_y_np = train_y.to_numpy().ravel()
    test_X_np = test_X.to_numpy()
    test_y_np = test_y.to_numpy().ravel()
    class_labels = sorted(set(np.unique(train_y_np)) | set(np.unique(test_y_np)))

    def _objective(space: dict) -> dict:
        # ep_model.ipynb cell 10 / wp_model.ipynb cell 9 ("model_objective"), ported: an
        # XGBClassifier per trial with early stopping against a held-out eval set, scored by
        # sklearn log_loss on predict_proba. `reg_alpha`/`colsample_bytree` stay float here
        # (see HYPEROPT_QUNIFORM_KEYS docstring for why the notebooks' int-cast is not
        # ported).
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
        clf.fit(
            train_X_np,
            train_y_np,
            eval_set=[(train_X_np, train_y_np), (test_X_np, test_y_np)],
            verbose=False,
        )
        pred_proba = clf.predict_proba(test_X_np)
        loss = log_loss(test_y_np, pred_proba, labels=class_labels)
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
) -> str:
    """Log one training run to the configured local MLflow store. Returns the run id.

    Params, metrics, a `phase=01.2` tag and the model artifact are logged the same way for
    every training path -- the one place this contract is defined, so `train_ep`/`train_wp`
    cannot drift from each other. `trial_losses`, when given, is logged as the stepped
    `tune_logloss` metric so a hyperopt search stays inspectable in the MLflow UI.
    """
    model.get_booster().feature_names = feature_names

    mlflow_store.ensure_experiment(experiment, config)
    with mlflow.start_run() as run:
        mlflow.log_params(params)
        for metric_name, value in metrics.items():
            mlflow.log_metric(metric_name, value)
        for step, loss in enumerate(trial_losses or []):
            mlflow.log_metric("tune_logloss", loss, step=step)
        mlflow.set_tag("phase", "01.2")
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
