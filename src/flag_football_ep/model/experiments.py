"""Feature-candidate experiments -- REQ-S1-09's documented control-vs-candidate outcomes.

CONTEXT §"Feature re-tests & training-data mix" requires every feature candidate to be
re-tested on the same grouped-CV protocol the production model reports on, with the
adoption criterion "mean pooled LOGO log-loss improves over the current feature set", and
every candidate experiment logged in MLflow with its verdict. This module owns that
harness: every arm (control and candidate) is measured with `model.evaluate.run_logo`, the
identical protocol `model.train._train` reports production numbers with, so a candidate's
numbers are directly comparable to a production run's numbers.

This module never registers or promotes a model. Candidate runs land in a dedicated
`{experiment}_candidates` experiment (`candidate_experiment_name`) and never call the
registry module's model-logging or alias-setting APIs -- experiments here are measurement
only, and adopting a candidate into the production feature set (editing `hyperparams.py`'s
`EP_FEATURES`/`WP_FEATURES`) is a decision a later plan makes once every candidate has a
verdict.

Ordering note: `CandidateSpec.build` runs on the filtered canonical frame *before*
`prepare_fn`/`mutate_fn` -- some candidates (the competition-tier one-hot, plan 01.3-07
Task 2) need raw canonical columns like `competition` that `make_*_model_mutations`'s
final `.select(...)` would otherwise have already dropped.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import mlflow
import polars as pl
from sklearn.metrics import log_loss

from flag_football_ep import reference
from flag_football_ep.config import Config
from flag_football_ep.features.mutations import (
    add_competition_tier_features,
    make_ep_model_mutations,
    make_wp_model_mutations,
    prepare_ep_data,
    prepare_wp_data,
)
from flag_football_ep.model import mlflow_store
from flag_football_ep.model.evaluate import LogoResult, run_logo
from flag_football_ep.model.hyperparams import (
    CANDIDATE_ADOPTION_MIN_DELTA,
    CANDIDATE_EXPERIMENT_SUFFIX,
    EP_FEATURES,
    EP_PARAMS,
    EP_TRAINING_COLUMNS,
    WP_FEATURES,
    WP_PARAMS,
    WP_TRAINING_COLUMNS,
)


class ExperimentError(ValueError):
    """Raised for every failure mode in this module -- an unknown model prefix, a candidate
    that does not apply to the requested model, a candidate feature already present in the
    control feature list or absent from the built frame, or a control/candidate arm
    row-count mismatch. Follows the codebase's no-silent-failure, named-exception
    convention (see `model.evaluate.EvaluationError`).
    """


def candidate_experiment_name(model_prefix: str, config: Config) -> str:
    """The dedicated MLflow experiment name candidate runs for `model_prefix` land in.

    `"ep"` -> `config.train.ep_experiment + CANDIDATE_EXPERIMENT_SUFFIX`; `"wp"` -> the `wp`
    equivalent. Never the production `ep_model`/`wp_model` experiment name itself, so a
    candidate run can never be mistaken for -- or picked up as -- a production run.
    """
    if model_prefix == "ep":
        return config.train.ep_experiment + CANDIDATE_EXPERIMENT_SUFFIX
    if model_prefix == "wp":
        return config.train.wp_experiment + CANDIDATE_EXPERIMENT_SUFFIX
    raise ExperimentError(
        f"candidate_experiment_name: model_prefix must be 'ep' or 'wp', got {model_prefix!r}"
    )


@dataclass(frozen=True)
class CandidateResult:
    """One candidate's control-vs-candidate outcome for one model prefix.

    `delta` is `control_logloss - candidate_logloss`; `adopted` is `True` exactly when
    `delta > CANDIDATE_ADOPTION_MIN_DELTA` -- CONTEXT's "pooled LOGO log-loss improves over
    the current feature set" criterion.
    """

    name: str
    model_prefix: str
    control_logloss: float
    candidate_logloss: float
    delta: float
    adopted: bool
    n_folds: int
    n_plays: int
    control_features: list[str]
    candidate_features: list[str]


@dataclass(frozen=True)
class CandidateSpec:
    """One feature candidate's definition.

    `build` takes the raw canonical `plays` frame (post exclude-game filter, pre
    `prepare_fn`) plus the config, and returns the frame with any extra columns added
    together with the ordered list of extra feature column names the candidate
    contributes. `applies_to` names which model prefixes (`"ep"`/`"wp"`) the candidate is
    defined for.
    """

    name: str
    build: Callable[[pl.DataFrame, Config], tuple[pl.DataFrame, list[str]]]
    applies_to: tuple[str, ...]


def _half_build(df: pl.DataFrame, config: Config) -> tuple[pl.DataFrame, list[str]]:
    """Passthrough: `half` is already a `CORE_COLUMNS` Int32 column, no construction needed."""
    return df, ["half"]


def _competition_tier_build(df: pl.DataFrame, config: Config) -> tuple[pl.DataFrame, list[str]]:
    """One-hot competition-tier covariate from the maintained tier vocabulary
    (`data/reference/competition_tier.csv`) -- CONTEXT §"Competition covariate" requires
    this categorical tier, not the raw ingest-source label standing in for it. Runs before
    `prepare_fn`/`mutate_fn` (see module docstring's ordering note) because the join needs
    both the ingest-source column and `competition`, and `competition` is not in
    `GROUP_COLUMNS`, so it would already be gone from the frame after `mutate_fn`'s final
    `.select(...)`.
    """
    mapping = reference.load_competition_tier(config.reference.competition_tier)
    return add_competition_tier_features(df, mapping)


CANDIDATES: dict[str, CandidateSpec] = {
    "half": CandidateSpec(name="half", build=_half_build, applies_to=("ep", "wp")),
    "competition_tier": CandidateSpec(
        name="competition_tier", build=_competition_tier_build, applies_to=("ep", "wp")
    ),
}


def _model_prefix_config(model_prefix: str) -> tuple[
    Callable[[pl.DataFrame], pl.DataFrame],
    Callable[[pl.DataFrame, Sequence[str]], pl.DataFrame],
    Sequence[str],
    Sequence[str],
    dict,
    str | None,
]:
    """`(prepare_fn, mutate_fn, selected_columns, base_features, fixed_params,
    weight_column)` for `model_prefix` -- mirrors `train.train_ep`/`train_wp`'s per-model
    wiring so a candidate arm and the production arm can never drift from each other on
    which prepare/mutate function or feature list they use.
    """
    if model_prefix == "ep":
        return (
            prepare_ep_data,
            make_ep_model_mutations,
            EP_TRAINING_COLUMNS,
            EP_FEATURES,
            EP_PARAMS,
            "Total_W_Scaled",
        )
    if model_prefix == "wp":
        return (
            prepare_wp_data,
            make_wp_model_mutations,
            WP_TRAINING_COLUMNS,
            WP_FEATURES,
            WP_PARAMS,
            None,
        )
    raise ExperimentError(
        f"run_candidate: model_prefix must be 'ep' or 'wp', got {model_prefix!r}"
    )


def _exclude_ids(model_prefix: str, config: Config) -> list[str]:
    if model_prefix == "ep":
        return list(config.train.exclude_games_ep)
    return list(config.train.exclude_games_wp)


def _pooled_logloss(logo: LogoResult, fixed_params: dict) -> float:
    """Pooled out-of-fold log-loss, computed the same way `train._train` does."""
    num_class = fixed_params.get("num_class")
    labels = list(range(num_class)) if num_class else None
    oof_pred_for_metric = logo.oof_pred if num_class else logo.oof_pred.ravel()
    return float(log_loss(logo.oof_label, oof_pred_for_metric, labels=labels))


def run_candidate(
    *,
    plays: pl.DataFrame,
    config: Config,
    model_prefix: str,
    spec: CandidateSpec,
    tune: bool = False,
    max_evals: int = 100,
) -> CandidateResult:
    """Measure `spec` against the current feature set for `model_prefix` and log the
    verdict to a dedicated MLflow experiment. Returns the `CandidateResult`.

    `tune`/`max_evals` are accepted for interface symmetry with `train.train_ep`/`train_wp`
    but unused: candidate experiments always measure against the fixed, previously-tuned
    production hyperparameters (`EP_PARAMS`/`WP_PARAMS`) -- re-tuning per candidate would
    conflate "does this feature help" with "did we get luckier hyperparameters this time",
    which is not the question REQ-S1-09 asks.
    """
    del tune, max_evals

    prepare_fn, mutate_fn, selected_columns, base_features, fixed_params, weight_column = (
        _model_prefix_config(model_prefix)
    )

    if model_prefix not in spec.applies_to:
        raise ExperimentError(
            f"run_candidate: candidate {spec.name!r} does not apply to model_prefix "
            f"{model_prefix!r} (applies_to={spec.applies_to})"
        )

    exclude_ids = _exclude_ids(model_prefix, config)
    filtered = plays.filter(~pl.col("game_id").is_in(exclude_ids)) if exclude_ids else plays

    augmented, extra_features = spec.build(filtered, config)

    overlap = [c for c in extra_features if c in base_features]
    if overlap:
        raise ExperimentError(
            f"run_candidate: candidate {spec.name!r} feature(s) already present in the "
            f"control feature list: {', '.join(overlap)}"
        )
    missing = [c for c in extra_features if c not in augmented.columns]
    if missing:
        raise ExperimentError(
            f"run_candidate: candidate {spec.name!r} feature(s) not present in the frame "
            f"after spec.build: {', '.join(missing)}"
        )

    prepared = prepare_fn(augmented)

    # Both arms are built from the *same* `prepared` frame, differing only in which columns
    # `mutate_fn`'s `.select(...)` keeps before `.drop_nulls()`. If `extra_features`
    # introduces a null a control-only row set would not have dropped, the two arms'
    # heights diverge -- caught explicitly below rather than silently measured on
    # different row sets (CONTEXT: "a candidate that changes the row set is not
    # comparable").
    control_frame = mutate_fn(prepared, list(selected_columns)).drop_nulls()
    candidate_frame = mutate_fn(prepared, [*selected_columns, *extra_features]).drop_nulls()

    if control_frame.height != candidate_frame.height:
        raise ExperimentError(
            f"run_candidate: candidate {spec.name!r} arms have different row counts -- "
            f"control has {control_frame.height} rows, candidate has "
            f"{candidate_frame.height} rows; both arms must be measured on the identical "
            "row set"
        )

    candidate_features = [*base_features, *extra_features]

    control_logo = run_logo(
        model_data=control_frame,
        features=base_features,
        fixed_params=fixed_params,
        weight_column=weight_column,
    )
    candidate_logo = run_logo(
        model_data=candidate_frame,
        features=candidate_features,
        fixed_params=fixed_params,
        weight_column=weight_column,
    )

    control_logloss = _pooled_logloss(control_logo, fixed_params)
    candidate_logloss = _pooled_logloss(candidate_logo, fixed_params)
    delta = control_logloss - candidate_logloss
    adopted = delta > CANDIDATE_ADOPTION_MIN_DELTA
    verdict = "adopted" if adopted else "rejected"

    experiment_name = candidate_experiment_name(model_prefix, config)
    mlflow_store.ensure_experiment(experiment_name, config)
    with mlflow.start_run():
        mlflow.set_tag("candidate", spec.name)
        mlflow.set_tag("phase", "01.3")
        mlflow.log_param("verdict", verdict)
        mlflow.log_param("model_prefix", model_prefix)
        mlflow.log_param("n_folds", candidate_logo.n_folds)
        mlflow.log_param("n_plays", candidate_frame.height)
        mlflow.log_param("control_features", ",".join(base_features))
        mlflow.log_param("candidate_features", ",".join(candidate_features))
        mlflow.log_metric("control_logloss", control_logloss)
        mlflow.log_metric("candidate_logloss", candidate_logloss)
        mlflow.log_metric("delta", delta)

    return CandidateResult(
        name=spec.name,
        model_prefix=model_prefix,
        control_logloss=control_logloss,
        candidate_logloss=candidate_logloss,
        delta=delta,
        adopted=adopted,
        n_folds=candidate_logo.n_folds,
        n_plays=candidate_frame.height,
        control_features=list(base_features),
        candidate_features=candidate_features,
    )
