"""Leave-one-game-out (LOGO) evaluation loop -- the REQ-S1-07 / D-07 measurement protocol.

CONTEXT §"Grouped-CV protocol" locks leave-one-game-out over `game_id` as the evaluation
scheme every reported EP/WP metric must come from: every game is held out exactly once,
fit on every other game, and the held-out game's predictions are accumulated into an
out-of-fold prediction matrix. This is the maximal form of D-07 (GroupKFold over `game_id`)
-- the user chose it over a cheaper K=5 grouped CV knowing the ~50+ fits cost (RESEARCH,
CONTEXT "Claude's Discretion").

`run_logo` consumes an already-mutated training frame -- the EP mutation stage's sample
weights (`Total_W_Scaled`, built from `Drive_Score_Dist_W`/`ScoreDiff_W`) are computed once
on the full training corpus and raise `DegenerateWeightRange` on a subset with no variation
(see `features/mutations.py`'s model-mutation docstring: "must be computed on the full
training corpus, never per game or on any subset with no variation"). This module therefore
never re-invokes that mutation stage inside the fold loop -- it only fits models on the
pre-computed weight column, exactly as `_train`'s refit-once production model does (RESEARCH
"Anti-Patterns to Avoid").

The models fit inside the LOGO loop are measurement-only: they exist to produce out-of-fold
predictions for the reported metric and are never exported, registered, or returned to a
caller. The shipped model is a separate single refit on all games (`model/train.py`'s
`_train`), built from the same tuned `fixed_params` this loop measures.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import polars as pl
import xgboost as xgb
from sklearn.model_selection import LeaveOneGroupOut

from flag_football_ep.model.hyperparams import LOGO_GROUP_COLUMN


class EvaluationError(ValueError):
    """Raised when `run_logo`'s input frame is malformed or LOGO is undefined for it."""


@dataclass(frozen=True)
class LogoResult:
    """Out-of-fold predictions and bookkeeping from one `run_logo` call.

    `oof_pred` has shape `(n_rows, n_outputs)` -- `n_outputs` is the multiclass `num_class`
    (EP: 5) or 1 for a binary objective (WP). `oof_label`, `oof_game_id`, `oof_play_id` and
    `oof_source` are aligned row-for-row with `oof_pred`, in the input frame's row order.
    """

    oof_pred: np.ndarray
    oof_label: np.ndarray
    oof_game_id: np.ndarray
    oof_play_id: np.ndarray
    oof_source: np.ndarray
    n_folds: int
    wall_seconds: float


def run_logo(
    *,
    model_data: pl.DataFrame,
    features: Sequence[str],
    fixed_params: dict,
    weight_column: str | None,
    group_column: str = LOGO_GROUP_COLUMN,
) -> LogoResult:
    """Fit one XGBoost model per held-out `group_column` value, measurement-only.

    For each unique value of `group_column` (default `game_id`), fits `xgb.XGBRegressor(
    **fixed_params)` -- the same estimator class `_train` uses for the shipped model, so the
    measured model family is the model family that ships -- on every other group's rows, and
    predicts on the held-out group's rows. Every input row is predicted exactly once, by a
    model that never saw that row's group during fitting.

    Raises `EvaluationError` naming every missing column when `group_column`, `"label"`,
    `"play_id"`, `"source"`, `weight_column` (if given), or any entry of `features` is absent
    from `model_data`, and naming the group count when `model_data` has fewer than 2 distinct
    `group_column` values (LOGO is undefined for one group).
    """
    required = [group_column, "label", "play_id", "source", *features]
    if weight_column is not None:
        required.append(weight_column)
    missing = [c for c in required if c not in model_data.columns]
    if missing:
        raise EvaluationError(
            f"run_logo: input frame is missing required column(s): {', '.join(missing)}"
        )

    X = model_data.select(list(features)).to_numpy()
    y = model_data.select("label").to_numpy().ravel()
    groups = model_data.select(group_column).to_numpy().ravel()
    weight = (
        model_data.select(weight_column).to_numpy().ravel() if weight_column is not None else None
    )

    logo = LeaveOneGroupOut()
    n_folds = logo.get_n_splits(groups=groups)
    if n_folds < 2:
        raise EvaluationError(
            f"run_logo: {group_column!r} has {n_folds} distinct value(s) in the input frame; "
            "leave-one-group-out requires at least 2 groups"
        )

    n_rows = model_data.height
    num_class = fixed_params.get("num_class")
    n_outputs = num_class if num_class else 1
    oof_pred = np.full((n_rows, n_outputs), np.nan)

    start = time.perf_counter()
    for train_idx, test_idx in logo.split(X, y, groups=groups):
        model = xgb.XGBRegressor(**fixed_params)
        fold_weight = weight[train_idx] if weight is not None else None
        model.fit(X[train_idx], y[train_idx], sample_weight=fold_weight)

        pred = np.asarray(model.predict(X[test_idx]))
        if pred.ndim == 1:
            pred = pred.reshape(-1, 1)
        oof_pred[test_idx] = pred
    wall_seconds = time.perf_counter() - start

    if np.isnan(oof_pred).any():
        raise EvaluationError(
            "run_logo: out-of-fold prediction matrix contains NaN after the LOGO loop -- "
            "at least one row was never predicted"
        )

    return LogoResult(
        oof_pred=oof_pred,
        oof_label=y,
        oof_game_id=groups,
        oof_play_id=model_data.select("play_id").to_numpy().ravel(),
        oof_source=model_data.select("source").to_numpy().ravel(),
        n_folds=n_folds,
        wall_seconds=wall_seconds,
    )
