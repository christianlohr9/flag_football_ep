"""Promotion gate: the mechanical checks a candidate MLflow run must pass before it can
become the `champion` alias -- replacing "judgement by hand reading the MLflow UI" as the
DEFAULT path for `ffep promote` (docs/model-training.md section 3), not removing judgement:
`ffep promote --force --reason "..."` is the escape hatch, and the escape hatch is itself
logged (see `cli.py::promote`, T-M3-05-15).

Every check reads exclusively from the candidate run's own MLflow metrics -- never a CSV
(RESEARCH "Anti-Patterns to Avoid": `data/reference/epa_refinement/*.csv` is a phase-specific,
dated, ad-hoc output shape that would break a general gate the moment its naming convention
changes). Metric-key resolution is per model prefix (RESEARCH Pitfall 2, T-M3-05-14): EP logs
`logo_mlogloss`/`naive_mlogloss`, WP logs `logo_logloss`/`naive_logloss` -- a gate that hard-codes
one name for both models silently passes or fails the wrong model.
"""

from __future__ import annotations

from dataclasses import dataclass

from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from flag_football_ep.config import Config, GateThresholds
from flag_football_ep.model import mlflow_store, registry
from flag_football_ep.model.registry import RegistryError
from flag_football_ep.model.score import _validate_run_id

# RESEARCH Pitfall 2 / T-M3-05-14: never hard-code one metric name for both models -- EP/WP
# log distinct LOGO log-loss metric keys (train.py::train_ep/train_wp's own `metric_name`
# argument to the shared `_train` pipeline).
_METRIC_KEY_BY_PREFIX = {"ep": "logo_mlogloss", "wp": "logo_logloss"}

_CALIBRATION_METRIC_PREFIX = "calibration_max_deviation_"


class GateError(ValueError):
    """Raised when the candidate run itself cannot be resolved against the configured
    MLflow store -- distinct from a normal gate failure (`GateResult.passed=False`), which
    is a legitimate refuse-to-promote outcome, not an error.
    """


@dataclass(frozen=True)
class GateResult:
    """The outcome of `evaluate_gate`.

    `passed` is the AND of every check that actually ran -- a skipped check (its required
    metric was absent on this run, e.g. a run that predates M3-05-03) never blocks a
    promotion by itself and never counts as a silent pass either (T-M3-05-16).

    `checks` maps a check name to `(status, reason)`: `status` is `True` (passed), `False`
    (failed), or `None` (skipped -- metric absent, not evaluated); `reason` is a
    human-readable explanation, always present regardless of `status`.
    """

    passed: bool
    checks: dict[str, tuple[bool | None, str]]


def _metric_key_for_prefix(model_prefix: str) -> str:
    try:
        return _METRIC_KEY_BY_PREFIX[model_prefix]
    except KeyError:
        raise ValueError(
            f"model_prefix {model_prefix!r} is not one of "
            f"{sorted(_METRIC_KEY_BY_PREFIX)} -- the gate cannot resolve which LOGO "
            "log-loss metric key belongs to it"
        ) from None


def evaluate_gate(
    model_prefix: str,
    run_id: str,
    config: Config,
    thresholds: GateThresholds | None = None,
) -> GateResult:
    """Evaluate the four promotion-gate checks for `run_id` against `model_prefix`'s
    currently-registered champion (if any).

    Every number is read from `run_id`'s own MLflow metrics via
    `MlflowClient().get_run(run_id).data.metrics` -- never a CSV. `thresholds` defaults to
    `config.promotion_gate` (the `[promotion_gate]` TOML table, or its safe defaults).

    Checks:
    1. Beats the naive baseline: `logloss_improvement > 0` (already logged by `_train`).
    2. Beats the current champion: resolved via the existing, unmodified
       `registry.resolve_champion` -- if no champion alias is set yet, this check passes
       automatically (there is nothing to beat).
    3. Calibration within tolerance: every `calibration_max_deviation_*` metric present on
       the run is `<= thresholds.max_calibration_deviation`.
    4. No-play share under threshold: `no_play_share <= thresholds.max_no_play_share`, if
       that metric is present on the run.

    A check whose required metric is absent (e.g. a run logged before M3-05-03) is recorded
    as skipped (`status=None`), never silently treated as a pass.

    Raises `GateError` if `run_id` itself cannot be resolved against the configured store --
    a distinct failure mode from a normal (non-erroring) gate refusal.
    """
    thresholds = thresholds if thresholds is not None else config.promotion_gate
    metric_key = _metric_key_for_prefix(model_prefix)

    _validate_run_id(run_id)
    mlflow_store.configure(config)
    client = MlflowClient()
    try:
        run = client.get_run(run_id)
    except MlflowException as exc:
        raise GateError(
            f"candidate run {run_id!r} does not exist in the MLflow store at "
            f"{mlflow_store.tracking_uri(config)!r}: {exc}"
        ) from exc
    metrics = run.data.metrics

    checks: dict[str, tuple[bool | None, str]] = {}

    # 1. Beats the naive baseline -- already logged by _train under a model-agnostic key.
    if "logloss_improvement" in metrics:
        improvement = metrics["logloss_improvement"]
        beats_naive = improvement > 0
        checks["beats_naive"] = (
            beats_naive,
            f"logloss_improvement={improvement:.6f} "
            f"({'beats' if beats_naive else 'does not beat'} the naive baseline)",
        )
    else:
        checks["beats_naive"] = (
            None,
            "logloss_improvement metric absent on this run -- skipped, not failed",
        )

    # 2. Beats the current champion, if one is set yet.
    name = registry.registered_model_name(model_prefix)
    try:
        champion_run_id = registry.resolve_champion(name, config)
    except RegistryError:
        checks["beats_champion"] = (
            True,
            f"no {name!r} champion set yet -- nothing to beat, check passes automatically",
        )
    else:
        champion_metrics = client.get_run(champion_run_id).data.metrics
        candidate_metric = metrics.get(metric_key)
        champion_metric = champion_metrics.get(metric_key)
        if candidate_metric is None or champion_metric is None:
            missing = (
                f"candidate's {metric_key}"
                if candidate_metric is None
                else f"champion {champion_run_id!r}'s {metric_key}"
            )
            checks["beats_champion"] = (
                None,
                f"{missing} metric absent -- cannot compare, skipped",
            )
        else:
            beats_champion = candidate_metric <= champion_metric + thresholds.champion_epsilon
            checks["beats_champion"] = (
                beats_champion,
                f"candidate {metric_key}={candidate_metric:.6f} vs. champion "
                f"{champion_run_id!r} {metric_key}={champion_metric:.6f} "
                f"(epsilon={thresholds.champion_epsilon})",
            )

    # 3. Calibration within tolerance -- every calibration_max_deviation_* metric present.
    calibration_metrics = {
        key: value for key, value in metrics.items() if key.startswith(_CALIBRATION_METRIC_PREFIX)
    }
    if not calibration_metrics:
        checks["calibration"] = (
            None,
            "no calibration_max_deviation_* metrics on this run (predates M3-05-03) -- skipped",
        )
    else:
        failing = {
            key: value
            for key, value in calibration_metrics.items()
            if value > thresholds.max_calibration_deviation
        }
        checks["calibration"] = (
            not failing,
            (
                f"all calibration_max_deviation_* metrics within tolerance "
                f"({thresholds.max_calibration_deviation})"
                if not failing
                else f"exceeds tolerance ({thresholds.max_calibration_deviation}): {failing}"
            ),
        )

    # 4. No-play share under threshold, if logged.
    if "no_play_share" in metrics:
        no_play_share = metrics["no_play_share"]
        under_threshold = no_play_share <= thresholds.max_no_play_share
        checks["no_play_share"] = (
            under_threshold,
            f"no_play_share={no_play_share:.6f} (threshold {thresholds.max_no_play_share})",
        )
    else:
        checks["no_play_share"] = (
            None,
            "no_play_share metric absent on this run (predates M3-05-03) -- skipped",
        )

    passed = all(status for status, _reason in checks.values() if status is not None)
    return GateResult(passed=passed, checks=checks)
