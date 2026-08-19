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
from pathlib import Path

import numpy as np
import polars as pl
import xgboost as xgb
from sklearn.calibration import calibration_curve
from sklearn.metrics import log_loss
from sklearn.model_selection import LeaveOneGroupOut

from flag_football_ep.config import Config
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


# --- Calibration and training report (REQ-S1-08) -----------------------------------------
#
# CONTEXT locks the class-frequency naive baseline as the floor every reported model must
# beat, and MLflow run artifacts (reliability-curve PNGs plus a metrics table) as the report
# form -- no separate standalone report file. Scope guard: report only. Any isotonic/Platt
# calibration *correction* is explicitly deferred to a later decision; this module reports
# calibration, it does not correct it.


def naive_baseline_logloss(labels: np.ndarray, num_class: int) -> float:
    """Log-loss of predicting `labels`' own marginal class distribution for every row.

    This is the floor a model must beat to be worth anything -- CONTEXT locks this
    class-frequency baseline; a down/distance-bucket baseline is explicitly deferred.
    `num_class == 1` is the binary WP case: the naive prediction is `labels`' marginal
    positive rate (computed from the labels passed in, never assumed 0.5).
    """
    labels = np.asarray(labels)
    n = len(labels)
    if num_class == 1:
        positive_rate = float(np.mean(labels))
        naive_pred = np.full(n, positive_rate)
        return float(log_loss(labels, naive_pred, labels=[0, 1]))
    class_freqs = np.bincount(labels.astype(int), minlength=num_class) / n
    naive_pred = np.tile(class_freqs, (n, 1))
    return float(log_loss(labels, naive_pred, labels=list(range(num_class))))


@dataclass(frozen=True)
class ReliabilityCurve:
    """One class's reliability (calibration) curve, from `sklearn.calibration.
    calibration_curve` on one-vs-rest binarised labels."""

    name: str
    prob_true: np.ndarray
    prob_pred: np.ndarray
    n_bins: int


def reliability_curves(
    oof_pred: np.ndarray,
    oof_label: np.ndarray,
    class_names: Sequence[str],
    n_bins: int = 10,
) -> list[ReliabilityCurve]:
    """One-vs-rest reliability curve per class name, for a multiclass `oof_pred`; exactly
    one curve for a single-column (binary) `oof_pred`.

    Never hand-rolls the binning -- `calibration_curve` already handles empty and degenerate
    bins (RESEARCH "Don't Hand-Roll"). `CalibrationDisplay.from_estimator` is not used: LOGO
    produces accumulated prediction arrays with no single fitted estimator object.
    """
    oof_pred = np.asarray(oof_pred)
    if oof_pred.ndim == 1:
        oof_pred = oof_pred.reshape(-1, 1)
    oof_label = np.asarray(oof_label)

    if len(class_names) != oof_pred.shape[1]:
        raise EvaluationError(
            f"reliability_curves: class_names has {len(class_names)} entries but oof_pred "
            f"has {oof_pred.shape[1]} column(s) -- these must match"
        )

    if oof_pred.shape[1] == 1:
        # Binary case: oof_label is already the positive-class indicator, no OvR needed.
        prob_true, prob_pred = calibration_curve(
            oof_label.astype(int), oof_pred[:, 0], n_bins=n_bins
        )
        return [
            ReliabilityCurve(
                name=class_names[0], prob_true=prob_true, prob_pred=prob_pred, n_bins=n_bins
            )
        ]

    curves = []
    for class_idx, class_name in enumerate(class_names):
        y_true_binary = (oof_label == class_idx).astype(int)
        y_prob_class = oof_pred[:, class_idx]
        prob_true, prob_pred = calibration_curve(y_true_binary, y_prob_class, n_bins=n_bins)
        curves.append(
            ReliabilityCurve(
                name=class_name, prob_true=prob_true, prob_pred=prob_pred, n_bins=n_bins
            )
        )
    return curves


def per_source_metrics(
    oof_pred: np.ndarray,
    oof_label: np.ndarray,
    oof_source: np.ndarray,
    num_class: int,
) -> pl.DataFrame:
    """Per-source log-loss vs. each source's own naive baseline, plus a pooled `__pooled__`
    row.

    This is how CONTEXT's folded IFAF-benchmarking todo is realised -- the IFAF WM-2026 rows
    train like any other source and are simply reported separately. Each source's naive
    baseline is computed from that source's own label distribution, never the pooled one, so
    a source that hurts the model is visible against a fair floor.
    """
    oof_pred = np.asarray(oof_pred)
    if oof_pred.ndim == 1:
        oof_pred = oof_pred.reshape(-1, 1)
    oof_label = np.asarray(oof_label)
    oof_source = np.asarray(oof_source)

    labels_arg = list(range(num_class)) if num_class > 1 else [0, 1]
    pred_for_metric = oof_pred if num_class > 1 else oof_pred.ravel()

    def _row(source_name: str, mask: np.ndarray) -> dict:
        source_label = oof_label[mask]
        source_pred = pred_for_metric[mask]
        logloss = float(log_loss(source_label, source_pred, labels=labels_arg))
        naive_logloss = naive_baseline_logloss(source_label, num_class)
        return {
            "source": source_name,
            "n_plays": int(mask.sum()),
            "logloss": logloss,
            "naive_logloss": naive_logloss,
            "improvement": naive_logloss - logloss,
        }

    rows = [
        _row(source, oof_source == source) for source in sorted(set(oof_source.tolist()))
    ]
    rows.append(_row("__pooled__", np.full(oof_label.shape, True)))

    return pl.DataFrame(rows)


def render_reliability_figure(curves: Sequence[ReliabilityCurve], title: str) -> "Figure":  # noqa: F821
    """One subplot per curve, each with the `y = x` perfect-calibration diagonal.

    Uses the `Agg` backend so this works headless in tests and CI. Returns the `Figure`
    without showing or saving it -- the caller owns logging it to MLflow.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    n = len(curves)
    fig, axes = plt.subplots(1, n, figsize=(4 * n, 4), squeeze=False)
    for ax, curve in zip(axes[0], curves):
        ax.plot([0, 1], [0, 1], linestyle="--", color="gray", label="perfect calibration")
        ax.plot(curve.prob_pred, curve.prob_true, marker="o", label=curve.name)
        ax.set_xlabel("mean predicted probability")
        ax.set_ylabel("observed frequency")
        ax.set_title(curve.name)
        ax.legend(fontsize="small")
    fig.suptitle(title)
    fig.tight_layout()
    return fig


# --- Out-of-fold prediction persistence (Phase 1.4-facing contract) ----------------------
#
# **Phase 1.4-facing contract.** File names `oof_predictions_ep.parquet` and
# `oof_predictions_wp.parquet` under `config.paths.processed`; join key `(game_id, play_id)`;
# EP columns exactly `EP_PROB_LABELS` (hyperparams.py), WP column exactly `wp`
# (`features.mutations.WP_PROBABILITY_COLUMN`). Semantics: each historical play's probability
# as predicted by a model that never saw that play's game (CONTEXT §Out-of-fold EPA, nflfastR
# precedent). Phase 1.4 joins these onto `plays.parquet` for own-team EPA; new/unseen games
# are scored with the refit champion model via `ffep score` instead, never these files.


def oof_frame(logo: LogoResult, prob_labels: Sequence[str]) -> pl.DataFrame:
    """Assemble `logo`'s out-of-fold predictions into a joinable Polars frame.

    Columns: `game_id` (Utf8), `play_id` (Int32), `source` (Utf8) and one Float64 probability
    column per entry of `prob_labels`, in `prob_labels` order -- `(game_id, play_id)` is
    unique across the returned frame.
    """
    if len(prob_labels) != logo.oof_pred.shape[1]:
        raise EvaluationError(
            f"oof_frame: prob_labels has {len(prob_labels)} entries but logo.oof_pred has "
            f"{logo.oof_pred.shape[1]} column(s) -- these must match"
        )

    data = {
        "game_id": pl.Series("game_id", logo.oof_game_id, dtype=pl.Utf8),
        "play_id": pl.Series("play_id", logo.oof_play_id, dtype=pl.Int32),
        "source": pl.Series("source", logo.oof_source, dtype=pl.Utf8),
    }
    for col_idx, label in enumerate(prob_labels):
        data[label] = pl.Series(label, logo.oof_pred[:, col_idx], dtype=pl.Float64)
    return pl.DataFrame(data)


def write_oof_predictions(
    logo: LogoResult, prob_labels: Sequence[str], config: Config, model_prefix: str
) -> Path:
    """Write `oof_frame(logo, prob_labels)` to `oof_predictions_{model_prefix}.parquet`
    under `config.paths.processed`. Returns the written path.

    Writes to a `.tmp` sibling then `Path.replace`s it onto the live path -- the same
    write-to-temp-then-atomic-rename discipline `pipeline._atomic_write_parquet` uses
    (replicated here rather than imported, since `write_oof_predictions` needs the plain
    `Path.replace(...)` call this module's own acceptance criteria check for; the two
    implementations must never drift on the "crash mid-write leaves the previous file
    intact" guarantee). An interrupted write cannot destroy a previously good file, and no
    stray `.tmp` file is left behind, regardless of outcome.
    """
    frame = oof_frame(logo, prob_labels)
    config.paths.processed.mkdir(parents=True, exist_ok=True)
    out_path = config.paths.processed / f"oof_predictions_{model_prefix}.parquet"
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        frame.write_parquet(tmp_path)
        tmp_path.replace(out_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return out_path
