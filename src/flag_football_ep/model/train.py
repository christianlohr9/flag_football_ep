"""EP and WP model training, with MLflow as the tracking/artifact backend.

Ports `models/ep_model.ipynb` (cells 2, 6, 7, 9-14, 16) and `models/wp_model.ipynb`
(cells 2, 5, 6, 8-10, 12) behaviour-for-behaviour: same features, same
`train_test_split(test_size=0.2, random_state=42)`, same fixed hyperparameters (see
`flag_football_ep.model.hyperparams`), same metric. The one intentional behaviour change is
the artifact sink -- every run gets its own MLflow run (params, metrics, model artifact)
instead of the notebook's `pickle`-based dump into a fixed `"ep_model.pkl"` filename, which
silently overwrote the same file on every run (T-1.2-07, threat register in the plan).

This module is built incrementally across this plan's three tasks: task 1 lands `train_ep`
with the fixed-hyperparameter path only, task 2 adds `train_wp` and factors the MLflow
bookkeeping into the shared `_log_run` helper, task 3 adds `--tune` hyperopt search and the
optional dated `.pkl` export.

MLflow >=3.15's local `file:` tracking store is deprecated ("maintenance mode") unless
`MLFLOW_ALLOW_FILE_STORE=true` is set -- CONTEXT.md's "local mlruns/" foundation decision
(and this task's behaviour requirement that a run directory with params/metrics/artifact
files exists under `config.paths.mlruns`) both depend on the file store, not the sqlite/
server backend MLflow now recommends, so this module opts back in rather than switching
tracking backends underneath the CONTEXT decision.
"""

from __future__ import annotations

import hashlib
import io
import os

os.environ.setdefault("MLFLOW_ALLOW_FILE_STORE", "true")

import mlflow  # noqa: E402
import mlflow.xgboost  # noqa: E402
import polars as pl  # noqa: E402
import xgboost as xgb  # noqa: E402
from sklearn.metrics import log_loss  # noqa: E402
from sklearn.model_selection import train_test_split  # noqa: E402

from flag_football_ep.config import Config  # noqa: E402
from flag_football_ep.features.mutations import make_ep_model_mutations, prepare_ep_data  # noqa: E402
from flag_football_ep.model.hyperparams import EP_FEATURES, EP_PARAMS, EP_SELECTED_COLUMNS  # noqa: E402


class MissingTrainingColumns(ValueError):
    """Raised when the input frame lacks a column `train_ep`/`train_wp` needs."""


def train_ep(
    plays: pl.DataFrame,
    config: Config,
    tune: bool = False,
    max_evals: int = 100,
    export_pkl: bool = False,
) -> str:
    """Fit the EP model on `plays` and log the run to MLflow. Returns the MLflow run id.

    `tune`/`export_pkl` are accepted here for interface stability with `train_wp` and the
    CLI (plan 01's `ffep train`), but are wired up in task 3 of this plan; passing
    `tune=True` or `export_pkl=True` before then raises `NotImplementedError`.
    """
    if tune:
        raise NotImplementedError("hyperopt tuning lands in task 3 of this plan")

    exclude_ids = list(config.train.exclude_games_ep)
    try:
        filtered = (
            plays.filter(~pl.col("game_id").is_in(exclude_ids)) if exclude_ids else plays
        )
        prepared = prepare_ep_data(filtered)
        model_data = make_ep_model_mutations(prepared, EP_SELECTED_COLUMNS).drop_nulls()
    except pl.exceptions.ColumnNotFoundError as exc:
        raise MissingTrainingColumns(
            f"train_ep: input frame is missing a required column: {exc}"
        ) from exc

    missing = [c for c in EP_FEATURES if c not in model_data.columns]
    if missing:
        raise MissingTrainingColumns(
            f"train_ep: missing required feature column(s): {', '.join(missing)}"
        )

    X = model_data.select(list(EP_FEATURES))
    y = model_data.select("label")
    train_X, test_X, train_y, test_y = train_test_split(X, y, test_size=0.2, random_state=42)
    # Same split settings applied to the weight column separately, exactly as the notebook
    # does (ep_model.ipynb cell 6) -- row order is identical so the split lines up with X/y.
    weight_train, _weight_test = train_test_split(
        model_data.select("Total_W_Scaled"), test_size=0.2, random_state=42
    )

    training_data_sha256 = _hash_frame(model_data)

    model = xgb.XGBRegressor(**EP_PARAMS)
    model.fit(
        train_X.to_numpy(),
        train_y.select("label").to_numpy(),
        sample_weight=weight_train.to_numpy().ravel(),
    )
    model.get_booster().feature_names = list(EP_FEATURES)

    y_pred = model.predict(test_X.to_numpy())
    # Explicit `labels` covering every EP class: a small held-out split can easily miss a
    # rare class (e.g. Safety), and log_loss raises rather than silently comparing a subset
    # of columns against `y_pred`'s full num_class width.
    test_mlogloss = float(
        log_loss(test_y.to_numpy().ravel(), y_pred, labels=list(range(EP_PARAMS["num_class"])))
    )

    params = {
        **{str(key): value for key, value in EP_PARAMS.items()},
        "n_plays": model_data.height,
        "n_features": len(EP_FEATURES),
        "test_size": 0.2,
        "random_state": 42,
        "training_data_sha256": training_data_sha256,
        "excluded_game_ids": ",".join(exclude_ids),
    }

    mlflow.set_tracking_uri("file:" + str(config.paths.mlruns))
    mlflow.set_experiment(config.train.ep_experiment)
    with mlflow.start_run() as run:
        mlflow.log_params(params)
        mlflow.log_metric("test_mlogloss", test_mlogloss)
        mlflow.set_tag("phase", "01.2")
        mlflow.xgboost.log_model(model, name="model")
        run_id = run.info.run_id

    if export_pkl:
        raise NotImplementedError("dated .pkl export lands in task 3 of this plan")

    return run_id


def _hash_frame(df: pl.DataFrame) -> str:
    """SHA-256 over the training frame's Parquet bytes -- deterministic given row order."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    return hashlib.sha256(buf.getvalue()).hexdigest()
