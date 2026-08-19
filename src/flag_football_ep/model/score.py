"""Model scoring: resolve MLflow runs, load logged models, and score canonical plays.

Ports `ep_model.ipynb`'s load-predict-`add_ep_variables`/`add_wp_variables` pattern
(cells 10, 16) onto models resolved from the MLflow tracking store instead of an
arbitrary caller-chosen pickle path (T-1.2-08, threat register in the plan). `resolve_run` and
`load_model` always scope themselves to `config.paths.mlruns`, and any caller-supplied run
id is validated against a plain-hex pattern before it is interpolated into a `runs:/` uri --
the only artifacts reachable through this module are runs inside the configured tracking
store. The default resolution (no explicit run id) goes through the MLflow model registry's
`champion` alias (`flag_football_ep.model.registry`, REQ-S1-11): an unpromoted registered
model is an error, not a fallback to the newest FINISHED run -- promotion is an explicit,
human-invoked `ffep promote` step (CONTEXT.md "PAT baselines & model versioning" >
Promotion).

`score_plays` reuses the ported feature functions (`flag_football_ep.features.mutations`)
for both EP and WP -- it does not reimplement `ExpPts`/`ep`/`epa`/`wp`/`wpa` derivation.
Row count is preserved: a play missing an EP or WP feature is scored with null
probabilities rather than dropped (T-1.2-18), so the output always lines up 1:1 with the
input `plays` frame.

The MLflow tracking store is SQLite-backed (`flag_football_ep.model.mlflow_store`),
independently of `model.train` (plan 01.2-13) -- `ffep score` can be invoked without ever
importing the training module. `resolve_run`/`load_model` always call
`mlflow_store.configure` before touching the store, so an ambient tracking URI set elsewhere
in the process can never leak into this module's reads.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

import mlflow
import mlflow.xgboost
import numpy as np
import polars as pl
from mlflow.exceptions import MlflowException

from flag_football_ep.config import Config
from flag_football_ep.features.mutations import (
    WP_PROBABILITY_COLUMN,
    add_ep_variables,
    add_wp_variables,
    prepare_ep_data,
    prepare_wp_data,
)
from flag_football_ep.model import mlflow_store, registry
from flag_football_ep.model.hyperparams import EP_FEATURES, EP_PROB_LABELS, WP_FEATURES

# Plain hex only -- the mitigation for T-1.2-08: no path separator, `.`, or other fragment
# can ever reach the `runs:/<id>/model` uri interpolation below.
_RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{8,}$")

_ROW_ID_COLUMN = "_score_row_id"


class RunNotFound(ValueError):
    """Raised when a run id or experiment cannot be resolved in the configured MLflow store."""


class MissingScoreColumns(ValueError):
    """Raised when the input frame to `score_plays` lacks a required EP/WP feature column."""


def _validate_run_id(run_id: str) -> None:
    if not _RUN_ID_PATTERN.match(run_id):
        raise ValueError(
            f"run id {run_id!r} is not a plain hex identifier; refusing to build a runs:/ uri "
            "(this is the mitigation for T-1.2-08 -- no path fragment may reach the artifact "
            "loader)"
        )


def resolve_run(
    experiment: str, config: Config, run_id: str | None = None, *, model_prefix: str
) -> str:
    """Resolve a run id for `experiment` against the MLflow store at `config.paths.mlruns`.

    An explicit `run_id` wins after being validated as a plain hex identifier and confirmed
    to exist, raising `RunNotFound` if it does not. Otherwise resolution goes through the
    MLflow registry's `champion` alias for `model_prefix` (`registry.resolve_champion`) --
    an unpromoted registered model raises `registry.RegistryError` naming the model and
    telling the operator to run `ffep promote`, rather than silently falling back to the
    newest FINISHED run.
    """
    mlflow_store.configure(config)

    if run_id is not None:
        _validate_run_id(run_id)
        try:
            mlflow.get_run(run_id)
        except MlflowException as exc:
            raise RunNotFound(
                f"run {run_id!r} does not exist in the MLflow store at {config.paths.mlruns}"
            ) from exc
        return run_id

    name = registry.registered_model_name(model_prefix)
    return registry.resolve_champion(name, config)


def load_model(run_id: str, config: Config):
    """Load the XGBoost model logged under `run_id`, scoped to `config.paths.mlruns`.

    Always sets the tracking uri from `config.paths.mlruns` before loading, so a
    caller-supplied run id can never resolve against a different, uncontrolled store.
    `run_id` is validated as a plain hex identifier before being interpolated into the
    `runs:/<id>/model` uri (T-1.2-08) -- no deserialization of a caller-supplied file path
    happens anywhere in this module.
    """
    _validate_run_id(run_id)
    mlflow_store.configure(config)
    return mlflow.xgboost.load_model(f"runs:/{run_id}/model")


def _prepare_ep_features(df: pl.DataFrame) -> pl.DataFrame:
    """`prepare_ep_data` plus the down one-hot columns `EP_FEATURES` needs.

    Mirrors `make_ep_model_mutations`'s down-onehot derivation (features/mutations.py)
    without its sample-weight/label/drop_nulls machinery -- scoring needs the feature
    columns only and must preserve row count.
    """
    prepared = prepare_ep_data(df)
    return prepared.with_columns(
        down0=(pl.col("down") == 0).cast(pl.Int32),
        down1=(pl.col("down") == 1).cast(pl.Int32),
        down2=(pl.col("down") == 2).cast(pl.Int32),
        down3=(pl.col("down") == 3).cast(pl.Int32),
        down4=(pl.col("down") == 4).cast(pl.Int32),
    )


def _score_probabilities(
    df: pl.DataFrame, model, features: Sequence[str], prob_labels: Sequence[str]
) -> pl.DataFrame:
    """Select `features` in order, predict on rows with no null feature, and hstack the
    resulting `prob_labels` columns onto `df`.

    Rows missing any feature are scored with null probabilities instead of being dropped,
    so the returned frame's row count always equals `df`'s (T-1.2-18). Raises
    `MissingScoreColumns` naming any `features` entry absent from `df` entirely, or naming
    `_score_row_id` when that column itself is absent.

    The returned frame is always in `_ROW_ID_COLUMN` order, because `add_ep_variables`/
    `add_wp_variables` derive `home_ep_after`/`home_wp_after` via `shift(-1)` and fill
    `ep`/`wp`/`tmp_posteam` via `backward_fill()` -- computing those on a frame where
    null-feature rows have been relocated to the tail silently corrupts every row adjacent
    to one (CR-01).
    """
    missing = [c for c in features if c not in df.columns]
    if missing:
        raise MissingScoreColumns(
            f"score_plays: input frame is missing required feature column(s): "
            f"{', '.join(missing)}"
        )

    if _ROW_ID_COLUMN not in df.columns:
        raise MissingScoreColumns(
            f"score_plays: input frame is missing {_ROW_ID_COLUMN!r} -- the caller must add "
            "the row index (score_plays does this via with_row_index before calling "
            "_score_probabilities) so this function can restore input row order before "
            "splitting into complete/incomplete subframes."
        )

    null_mask = pl.any_horizontal([pl.col(f).is_null() for f in features])
    complete = df.filter(~null_mask)
    incomplete = df.filter(null_mask)

    if complete.height > 0:
        X = complete.select(list(features)).to_numpy()
        preds = np.asarray(model.predict(X), dtype=np.float64)
        if preds.ndim == 1:
            preds = preds.reshape(-1, 1)
        prob_df = pl.DataFrame(preds, schema=list(prob_labels))
        complete = complete.hstack(prob_df)
    else:
        complete = complete.with_columns(
            [pl.lit(None).cast(pl.Float64).alias(label) for label in prob_labels]
        )

    incomplete = incomplete.with_columns(
        [pl.lit(None).cast(pl.Float64).alias(label) for label in prob_labels]
    )

    return pl.concat([complete, incomplete], how="vertical").sort(_ROW_ID_COLUMN)


def score_plays(
    plays: pl.DataFrame,
    config: Config,
    ep_run: str | None = None,
    wp_run: str | None = None,
) -> pl.DataFrame:
    """Score `plays` with the trained EP and WP models resolved from the MLflow store.

    EP path (ep_model.ipynb cells 10, 16): `prepare_ep_data` -> down one-hot -> select
    `EP_FEATURES` -> predict -> the five `EP_PROB_LABELS` columns -> `add_ep_variables`
    (`ExpPts`, `ep`, `epa`). WP path: `prepare_wp_data` -> select `WP_FEATURES` -> predict
    -> `wp` -> `add_wp_variables` (`home_wp`, `away_wp`, `wpa`). Both paths run against the
    same input `plays` frame; results are joined back on an internal row id so the returned
    frame's row count always equals `plays`'s, in `plays`'s original order.

    `score_plays` performs no I/O beyond loading the two models -- the caller (`ffep score`)
    writes the returned frame to Parquet.
    """
    plays = plays.with_row_index(name=_ROW_ID_COLUMN, offset=0)

    ep_run_id = resolve_run(config.train.ep_experiment, config, ep_run, model_prefix="ep")
    wp_run_id = resolve_run(config.train.wp_experiment, config, wp_run, model_prefix="wp")
    ep_model = load_model(ep_run_id, config)
    wp_model = load_model(wp_run_id, config)

    try:
        ep_prepared = _prepare_ep_features(plays)
        wp_prepared = prepare_wp_data(plays)
    except pl.exceptions.ColumnNotFoundError as exc:
        raise MissingScoreColumns(
            f"score_plays: input frame is missing a required column: {exc}"
        ) from exc

    # add_ep_variables requires chronological order (shift(-1)/backward_fill()) --
    # _score_probabilities guarantees ep_scored is sorted on _ROW_ID_COLUMN (CR-01).
    ep_scored = _score_probabilities(ep_prepared, ep_model, EP_FEATURES, EP_PROB_LABELS)
    ep_result = add_ep_variables(ep_scored)

    # add_wp_variables requires chronological order (shift(-1)/backward_fill()) --
    # _score_probabilities guarantees wp_scored is sorted on _ROW_ID_COLUMN (CR-01).
    wp_scored = _score_probabilities(
        wp_prepared, wp_model, WP_FEATURES, [WP_PROBABILITY_COLUMN]
    )
    wp_result = add_wp_variables(wp_scored)

    ep_score_cols = ep_result.select(
        [_ROW_ID_COLUMN, *EP_PROB_LABELS, "ExpPts", "ep", "epa"]
    )
    wp_score_cols = wp_result.select(
        [_ROW_ID_COLUMN, WP_PROBABILITY_COLUMN, "home_wp", "away_wp", "wpa"]
    )

    result = (
        plays.join(ep_score_cols, on=_ROW_ID_COLUMN, how="left")
        .join(wp_score_cols, on=_ROW_ID_COLUMN, how="left")
        .sort(_ROW_ID_COLUMN)
        .drop(_ROW_ID_COLUMN)
    )
    return result
