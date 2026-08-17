"""EP and WP model training, with MLflow as the tracking/artifact backend.

Ports `models/ep_model.ipynb` (cells 2, 6, 7, 9-14, 16) and `models/wp_model.ipynb`
(cells 2, 5, 6, 8-10, 12) behaviour-for-behaviour: same features, same
`train_test_split(test_size=0.2, random_state=42)`, same fixed hyperparameters (see
`flag_football_ep.model.hyperparams`), same metric. The one intentional behaviour change is
the artifact sink -- every run gets its own MLflow run (params, metrics, model artifact)
instead of the notebook's `pickle`-based dump into a fixed `"ep_model.pkl"` filename, which
silently overwrote the same file on every run (T-1.2-07, threat register in the plan).

`train_ep` and `train_wp` are thin, model-specific wrappers around the shared `_train`
pipeline (filter -> prepare -> mutate -> drop_nulls -> split -> fit -> evaluate) and the
shared `_log_run` MLflow helper, so the two paths cannot drift from each other on the
params/metrics/artifact contract.

This module is built incrementally across this plan's three tasks: task 1 landed `train_ep`
with the fixed-hyperparameter path only, task 2 (this state) adds `train_wp` and the shared
helpers, task 3 adds `--tune` hyperopt search and the optional dated `.pkl` export.

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
from collections.abc import Callable, Sequence

os.environ.setdefault("MLFLOW_ALLOW_FILE_STORE", "true")

import mlflow  # noqa: E402
import mlflow.xgboost  # noqa: E402
import polars as pl  # noqa: E402
import xgboost as xgb  # noqa: E402
from sklearn.metrics import log_loss  # noqa: E402
from sklearn.model_selection import train_test_split  # noqa: E402

from flag_football_ep.config import Config  # noqa: E402
from flag_football_ep.features.mutations import (  # noqa: E402
    make_ep_model_mutations,
    make_wp_model_mutations,
    prepare_ep_data,
    prepare_wp_data,
)
from flag_football_ep.model.hyperparams import (  # noqa: E402
    EP_FEATURES,
    EP_PARAMS,
    EP_SELECTED_COLUMNS,
    WP_FEATURES,
    WP_PARAMS,
    WP_SELECTED_COLUMNS,
)


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
    if export_pkl:
        raise NotImplementedError("dated .pkl export lands in task 3 of this plan")

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
        model_prefix="train_ep",
    )


def train_wp(
    plays: pl.DataFrame,
    config: Config,
    tune: bool = False,
    max_evals: int = 100,
    export_pkl: bool = False,
) -> str:
    """Fit the WP model on `plays` and log the run to MLflow. Returns the MLflow run id.

    `tune`/`export_pkl` are accepted here for interface stability with `train_ep` and the
    CLI (plan 01's `ffep train`), but are wired up in task 3 of this plan; passing
    `tune=True` or `export_pkl=True` before then raises `NotImplementedError`. Unlike
    `train_ep`, the WP fit uses no sample weights -- `wp_model.ipynb` cell 6 fits without
    one.
    """
    if tune:
        raise NotImplementedError("hyperopt tuning lands in task 3 of this plan")
    if export_pkl:
        raise NotImplementedError("dated .pkl export lands in task 3 of this plan")

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
        model_prefix="train_wp",
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
) -> str:
    """Shared EP/WP fit-and-log pipeline.

    filter excluded games -> `prepare_fn` -> `mutate_fn` -> `drop_nulls()` -> 80/20 split ->
    fit `xgb.XGBRegressor(**fixed_params)` -> evaluate `metric_name` on the held-out split ->
    `_log_run`. `train_ep`/`train_wp` only supply what differs between the two paths; this
    is the one place the pipeline shape itself lives, so it cannot drift between them.
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
        sample_weight = weight_train.to_numpy().ravel()
    else:
        sample_weight = None

    training_data_sha256 = _hash_frame(model_data)

    model = xgb.XGBRegressor(**fixed_params)
    model.fit(
        train_X.to_numpy(),
        train_y.select("label").to_numpy(),
        sample_weight=sample_weight,
    )

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
    }

    return _log_run(
        experiment=experiment,
        params=params,
        metrics={metric_name: metric_value},
        model=model,
        feature_names=list(features),
        config=config,
    )


def _log_run(
    *,
    experiment: str,
    params: dict,
    metrics: dict,
    model: xgb.XGBRegressor,
    feature_names: list[str],
    config: Config,
) -> str:
    """Log one training run to the configured local MLflow store. Returns the run id.

    Params, metrics, a `phase=01.2` tag and the model artifact are logged the same way for
    every training path -- the one place this contract is defined, so `train_ep`/`train_wp`
    cannot drift from each other.
    """
    model.get_booster().feature_names = feature_names

    mlflow.set_tracking_uri("file:" + str(config.paths.mlruns))
    mlflow.set_experiment(experiment)
    with mlflow.start_run() as run:
        mlflow.log_params(params)
        for metric_name, value in metrics.items():
            mlflow.log_metric(metric_name, value)
        mlflow.set_tag("phase", "01.2")
        mlflow.xgboost.log_model(model, name="model")
        return run.info.run_id


def _hash_frame(df: pl.DataFrame) -> str:
    """SHA-256 over the training frame's Parquet bytes -- deterministic given row order."""
    buf = io.BytesIO()
    df.write_parquet(buf)
    return hashlib.sha256(buf.getvalue()).hexdigest()
