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

Plan 01.3-09 adoption note: `half` and `competition_tier` were adopted into
`EP_FEATURES`/`WP_FEATURES` (REQ-S1-09's combined-confirmation decision) and are no longer
registered in `CANDIDATES` below -- a candidate already in the production feature set has
nothing left to test (`run_candidate`'s overlap check would always reject it). Because
`competition_tier`'s one-hot columns are now part of the *control* arm too (not just a
candidate's extra columns), `_build_current_production_base_features` builds them
unconditionally at the top of `run_candidate`/`run_recency_candidate`/
`run_real_clock_experiment`, before `spec.build`/`weight_build` runs -- mirrors
`model/train.py::_build_competition_tier`'s production build step.
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
    add_recency_weight,
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
    RECENCY_HALF_LIFE_DAYS_GRID,
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

    `weight_build` is the REQ-S1-09 recency-weighting candidate's hook (plan 01.3-08):
    `None` for every feature candidate (`half`, `competition_tier`). When set, the
    candidate varies the *sample weight* rather than the feature list -- `build` alone
    cannot express that, so `run_recency_candidate` (not the generic `run_candidate`) reads
    `weight_build(df, config, half_life_days) -> (df_with_weight_column, weight_column_name)`
    once per grid entry in `RECENCY_HALF_LIFE_DAYS_GRID`.
    """

    name: str
    build: Callable[[pl.DataFrame, Config], tuple[pl.DataFrame, list[str]]]
    applies_to: tuple[str, ...]
    weight_build: Callable[[pl.DataFrame, Config, float], tuple[pl.DataFrame, str]] | None = None


def _competition_tier_build(df: pl.DataFrame, config: Config) -> tuple[pl.DataFrame, list[str]]:
    """One-hot competition-tier covariate from the maintained tier vocabulary
    (`data/reference/competition_tier.csv`) -- CONTEXT §"Competition covariate" requires
    this categorical tier, not the raw ingest-source label standing in for it. Runs before
    `prepare_fn`/`mutate_fn` (see module docstring's ordering note) because the join needs
    both the ingest-source column and `competition`, and `competition` is not in
    `GROUP_COLUMNS`, so it would already be gone from the frame after `mutate_fn`'s final
    `.select(...)`.

    Adopted into production for both EP and WP (plan 01.3-09) -- no longer registered as a
    `CANDIDATES` entry (nothing left to test), but reused directly by
    `_build_current_production_base_features` below, which every harness entry point calls
    unconditionally so the *control* arm also carries these columns.
    """
    mapping = reference.load_competition_tier(config.reference.competition_tier)
    return add_competition_tier_features(df, mapping)


def _build_current_production_base_features(df: pl.DataFrame, config: Config) -> pl.DataFrame:
    """Build every derived column the *current* `EP_FEATURES`/`WP_FEATURES` need beyond the
    raw canonical/prepared frame (REQ-S1-09 adoption, plan 01.3-09): today, just the
    competition-tier one-hot columns (`_competition_tier_build`). Called unconditionally at
    the top of `run_candidate`/`run_recency_candidate`/`run_real_clock_experiment`, before
    `spec.build`/`weight_build` runs -- both the control arm and any future candidate's arm
    need these columns present, since `_model_prefix_config`'s `base_features`
    (`EP_FEATURES`/`WP_FEATURES`) now includes them. Mirrors
    `model/train.py::_build_competition_tier`'s production build step exactly, so the
    harness's control number always matches what `ffep train` would log.
    """
    augmented, _tier_features = _competition_tier_build(df, config)
    return augmented


def _recency_build(df: pl.DataFrame, config: Config) -> tuple[pl.DataFrame, list[str]]:
    """`recency` is a sample-weight candidate (CONTEXT: multiplies into `Total_W_Scaled`),
    not a feature candidate -- it must be measured via `run_recency_candidate`, never the
    generic `run_candidate`, which only varies the feature list and has no half-life to
    sweep. This `build` exists solely so `CandidateSpec`'s required field is satisfied;
    calling it (i.e. calling `run_candidate` with `CANDIDATES["recency"]`) is a programming
    error and raises rather than silently producing a no-op arm.
    """
    del df, config
    raise ExperimentError(
        "recency: use run_recency_candidate, not run_candidate -- recency varies the "
        "sample weight across a half-life grid, not the feature list"
    )


def _recency_weight_build(
    df: pl.DataFrame, config: Config, half_life_days: float
) -> tuple[pl.DataFrame, str]:
    """`CandidateSpec.weight_build` for `recency`: adds `Recency_W` via `add_recency_weight`
    and reports its column name. `run_recency_candidate` passes this straight through to
    `make_ep_model_mutations(..., recency_weight_column=...)`.
    """
    del config
    weighted, _null_date_count = add_recency_weight(df, half_life_days)
    return weighted, "Recency_W"


def _real_clock_build(df: pl.DataFrame, config: Config) -> tuple[pl.DataFrame, list[str]]:
    """`real_clock` compares two `prepare_wp_data` derivations (synthetic vs. IFAF
    `game_clock_ms`) on the same IFAF-only subset -- it is neither a feature addition nor a
    sample-weight variation, so it must be measured via `run_real_clock_experiment`, never
    the generic `run_candidate`. This `build` exists solely so `CandidateSpec`'s required
    field is satisfied; calling it directly is a programming error.
    """
    del df, config
    raise ExperimentError(
        "real_clock: use run_real_clock_experiment, not run_candidate -- real_clock "
        "compares two prepare_wp_data derivations on an IFAF-only subset, not a feature "
        "addition"
    )


CANDIDATES: dict[str, CandidateSpec] = {
    # `half` and `competition_tier` were adopted into production (plan 01.3-09) and are
    # intentionally absent here -- see module docstring's "plan 01.3-09 adoption note".
    "recency": CandidateSpec(
        name="recency",
        build=_recency_build,
        applies_to=("ep",),
        weight_build=_recency_weight_build,
    ),
    "real_clock": CandidateSpec(
        name="real_clock", build=_real_clock_build, applies_to=("wp",)
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
    filtered = _build_current_production_base_features(filtered, config)

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


def run_recency_candidate(
    *, plays: pl.DataFrame, config: Config, model_prefix: str
) -> tuple[CandidateResult, list[CandidateResult]]:
    """Measure the REQ-S1-09 recency-weighting candidate across
    `RECENCY_HALF_LIFE_DAYS_GRID` plus an unweighted control, all on the same EP frame and
    the same LOGO folds, and log every arm to the EP candidate experiment tagged
    `candidate=recency`.

    Recency multiplies into the existing `Total_W_Scaled` sample weight
    (`make_ep_model_mutations(..., recency_weight_column="Recency_W")`), not a feature
    column, so `EP_FEATURES` is identical across every arm -- this cannot go through the
    generic `run_candidate`, which only varies the feature list (see `_recency_build`).

    Raises `ExperimentError` naming `model_prefix` for anything other than `"ep"`:
    `train_wp` fits without sample weights (`weight_column=None` for `"wp"` in
    `_model_prefix_config`), so a WP recency arm has nothing to multiply the weight into.
    `CANDIDATES["recency"].applies_to == ("ep",)` is how `ffep experiment --model both`
    skips this candidate for WP without calling this function at all.

    Returns `(best_half_life_result, half_life_results)`: `best_half_life_result` is the
    grid entry with the lowest pooled log-loss (its `name` identifies the winning half-life,
    e.g. `"recency_half_life_180.0"`), `adopted` is `True` only when it beats the control by
    more than `CANDIDATE_ADOPTION_MIN_DELTA`. `half_life_results` has exactly
    `len(RECENCY_HALF_LIFE_DAYS_GRID)` entries, each carrying the same `control_logloss` so
    the control's number is always available alongside every arm's delta.
    """
    if model_prefix != "ep":
        raise ExperimentError(
            "run_recency_candidate: candidate 'recency' only applies to model_prefix 'ep' "
            f"(train_wp fits without sample weights, weight_column=None), got {model_prefix!r}"
        )

    spec = CANDIDATES["recency"]
    exclude_ids = _exclude_ids(model_prefix, config)
    filtered = plays.filter(~pl.col("game_id").is_in(exclude_ids)) if exclude_ids else plays
    filtered = _build_current_production_base_features(filtered, config)

    control_prepared = prepare_ep_data(filtered)
    control_frame = make_ep_model_mutations(control_prepared, list(EP_TRAINING_COLUMNS)).drop_nulls()

    control_logo = run_logo(
        model_data=control_frame,
        features=EP_FEATURES,
        fixed_params=EP_PARAMS,
        weight_column="Total_W_Scaled",
    )
    control_logloss = _pooled_logloss(control_logo, EP_PARAMS)

    experiment_name = candidate_experiment_name(model_prefix, config)
    mlflow_store.ensure_experiment(experiment_name, config)

    def _log_arm(
        *,
        half_life_label: str,
        verdict: str,
        candidate_logloss: float,
        delta: float,
        n_folds: int,
        n_plays: int,
    ) -> None:
        with mlflow.start_run():
            mlflow.set_tag("candidate", "recency")
            mlflow.set_tag("phase", "01.3")
            mlflow.log_param("model_prefix", model_prefix)
            mlflow.log_param("half_life_days", half_life_label)
            mlflow.log_param("verdict", verdict)
            mlflow.log_param("n_folds", n_folds)
            mlflow.log_param("n_plays", n_plays)
            mlflow.log_metric("control_logloss", control_logloss)
            mlflow.log_metric("candidate_logloss", candidate_logloss)
            mlflow.log_metric("delta", delta)

    _log_arm(
        half_life_label="control",
        verdict="control",
        candidate_logloss=control_logloss,
        delta=0.0,
        n_folds=control_logo.n_folds,
        n_plays=control_frame.height,
    )

    half_life_results: list[CandidateResult] = []
    for half_life_days in RECENCY_HALF_LIFE_DAYS_GRID:
        weighted, weight_column_name = spec.weight_build(filtered, config, half_life_days)
        candidate_prepared = prepare_ep_data(weighted)
        candidate_frame = make_ep_model_mutations(
            candidate_prepared,
            list(EP_TRAINING_COLUMNS),
            recency_weight_column=weight_column_name,
        ).drop_nulls()

        if candidate_frame.height != control_frame.height:
            raise ExperimentError(
                f"run_recency_candidate: half_life_days={half_life_days!r} arm has "
                f"{candidate_frame.height} rows, control has {control_frame.height} rows -- "
                "both arms must be measured on the identical row set"
            )

        candidate_logo = run_logo(
            model_data=candidate_frame,
            features=EP_FEATURES,
            fixed_params=EP_PARAMS,
            weight_column="Total_W_Scaled",
        )
        candidate_logloss = _pooled_logloss(candidate_logo, EP_PARAMS)
        delta = control_logloss - candidate_logloss
        adopted = delta > CANDIDATE_ADOPTION_MIN_DELTA
        verdict = "adopted" if adopted else "rejected"

        _log_arm(
            half_life_label=str(half_life_days),
            verdict=verdict,
            candidate_logloss=candidate_logloss,
            delta=delta,
            n_folds=candidate_logo.n_folds,
            n_plays=candidate_frame.height,
        )

        half_life_results.append(
            CandidateResult(
                name=f"recency_half_life_{half_life_days}",
                model_prefix=model_prefix,
                control_logloss=control_logloss,
                candidate_logloss=candidate_logloss,
                delta=delta,
                adopted=adopted,
                n_folds=candidate_logo.n_folds,
                n_plays=candidate_frame.height,
                control_features=list(EP_FEATURES),
                candidate_features=list(EP_FEATURES),
            )
        )

    best = min(half_life_results, key=lambda r: r.candidate_logloss)
    return best, half_life_results


def run_real_clock_experiment(
    *, plays: pl.DataFrame, config: Config
) -> tuple[CandidateResult, CandidateResult]:
    """Measure the REQ-S1-09 IFAF-only real-clock WP sub-experiment: synthetic
    `half_seconds_remaining` versus the real IFAF `game_clock_ms`-derived clock, on the same
    IFAF games and the same LOGO folds. Logs both arms to the WP candidate experiment tagged
    `candidate=real_clock` with an `arm` param of `synthetic` or `real_clock`.

    Restricts `plays` to `source == "ifaf"` before either arm runs (CONTEXT: "comparing real
    game_clock_ms-derived time vs the synthetic construction on the same games" -- both arms
    must see the identical game population). Raises `ExperimentError` naming the source and
    the row/game counts when that subset is empty or has fewer than 2 distinct `game_id`
    values (LOGO is undefined for one game), when a non-time `WP_TRAINING_COLUMNS` entry
    (e.g. `yards_to_go`) is null for every row of the subset -- `drop_nulls()` would
    otherwise silently produce a 0-row frame that fails deep inside `run_logo`'s
    `LeaveOneGroupOut` with an unnamed sklearn error instead of a diagnosable one -- and when
    the two arms' built frames end up with different row counts (a partially-null derivation
    would otherwise make them silently incomparable).

    Returns `(synthetic_result, real_clock_result)`. Both carry the same
    `control_logloss`/`candidate_logloss`/`delta`/`adopted` (control = synthetic, candidate
    = real_clock) so either object alone documents the full verdict; `name` distinguishes
    which arm each describes. `WP_FEATURES` is never edited by this function -- production
    WP stays on the synthetic path (`prepare_wp_data`'s default) regardless of this
    experiment's outcome, per CONTEXT: "the production feature set stays synthetic until
    Hudl delivers real time".
    """
    ifaf_plays = plays.filter(pl.col("source") == "ifaf")
    n_games = ifaf_plays["game_id"].n_unique() if ifaf_plays.height > 0 else 0
    if ifaf_plays.height == 0 or n_games < 2:
        raise ExperimentError(
            f"run_real_clock_experiment: source='ifaf' subset has {ifaf_plays.height} "
            f"row(s) across {n_games} distinct game_id(s) -- leave-one-game-out requires "
            "at least 2 games"
        )

    exclude_ids = _exclude_ids("wp", config)
    filtered = (
        ifaf_plays.filter(~pl.col("game_id").is_in(exclude_ids)) if exclude_ids else ifaf_plays
    )
    filtered = _build_current_production_base_features(filtered, config)

    synthetic_prepared = prepare_wp_data(filtered)
    synthetic_full = make_wp_model_mutations(synthetic_prepared, list(WP_TRAINING_COLUMNS))
    synthetic_frame = synthetic_full.drop_nulls()

    real_clock_prepared = prepare_wp_data(filtered, real_clock=True)
    real_clock_full = make_wp_model_mutations(real_clock_prepared, list(WP_TRAINING_COLUMNS))
    real_clock_frame = real_clock_full.drop_nulls()

    if synthetic_frame.height == 0 or real_clock_frame.height == 0:
        fully_null_columns = sorted(
            {
                column
                for frame in (synthetic_full, real_clock_full)
                for column in WP_TRAINING_COLUMNS
                if frame.height > 0 and frame[column].null_count() == frame.height
            }
        )
        raise ExperimentError(
            "run_real_clock_experiment: not viable on this corpus -- the IFAF subset has "
            f"{ifaf_plays.height} row(s) across {n_games} game(s), but drop_nulls() removes "
            "every row because the following WP_TRAINING_COLUMNS entry/entries are null for "
            f"every row in this subset: {', '.join(fully_null_columns) or 'unknown'}. This is "
            "not a game_clock_ms/time-feature problem -- it is a pre-existing IFAF ingest "
            "data-coverage gap unrelated to this candidate."
        )

    if synthetic_frame.height != real_clock_frame.height:
        raise ExperimentError(
            f"run_real_clock_experiment: synthetic arm has {synthetic_frame.height} rows, "
            f"real_clock arm has {real_clock_frame.height} rows -- both arms must be "
            "measured on the identical row set"
        )

    synthetic_logo = run_logo(
        model_data=synthetic_frame,
        features=WP_FEATURES,
        fixed_params=WP_PARAMS,
        weight_column=None,
    )
    real_clock_logo = run_logo(
        model_data=real_clock_frame,
        features=WP_FEATURES,
        fixed_params=WP_PARAMS,
        weight_column=None,
    )

    synthetic_logloss = _pooled_logloss(synthetic_logo, WP_PARAMS)
    real_clock_logloss = _pooled_logloss(real_clock_logo, WP_PARAMS)
    delta = synthetic_logloss - real_clock_logloss
    adopted = delta > CANDIDATE_ADOPTION_MIN_DELTA
    verdict = "adopted" if adopted else "rejected"

    experiment_name = candidate_experiment_name("wp", config)
    mlflow_store.ensure_experiment(experiment_name, config)

    def _log_arm(*, arm: str, n_folds: int, n_plays: int) -> None:
        with mlflow.start_run():
            mlflow.set_tag("candidate", "real_clock")
            mlflow.set_tag("phase", "01.3")
            mlflow.log_param("model_prefix", "wp")
            mlflow.log_param("arm", arm)
            mlflow.log_param("verdict", verdict)
            mlflow.log_param("n_folds", n_folds)
            mlflow.log_param("n_plays", n_plays)
            mlflow.log_metric("synthetic_logloss", synthetic_logloss)
            mlflow.log_metric("real_clock_logloss", real_clock_logloss)
            mlflow.log_metric("delta", delta)

    _log_arm(arm="synthetic", n_folds=synthetic_logo.n_folds, n_plays=synthetic_frame.height)
    _log_arm(arm="real_clock", n_folds=real_clock_logo.n_folds, n_plays=real_clock_frame.height)

    synthetic_result = CandidateResult(
        name="real_clock_synthetic",
        model_prefix="wp",
        control_logloss=synthetic_logloss,
        candidate_logloss=real_clock_logloss,
        delta=delta,
        adopted=adopted,
        n_folds=synthetic_logo.n_folds,
        n_plays=synthetic_frame.height,
        control_features=list(WP_FEATURES),
        candidate_features=list(WP_FEATURES),
    )
    real_clock_result = CandidateResult(
        name="real_clock_real_clock",
        model_prefix="wp",
        control_logloss=synthetic_logloss,
        candidate_logloss=real_clock_logloss,
        delta=delta,
        adopted=adopted,
        n_folds=real_clock_logo.n_folds,
        n_plays=real_clock_frame.height,
        control_features=list(WP_FEATURES),
        candidate_features=list(WP_FEATURES),
    )

    return synthetic_result, real_clock_result
