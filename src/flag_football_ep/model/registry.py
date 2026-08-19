"""MLflow model registry helpers: registration, champion-alias promotion and resolution.

This module is the REQ-S1-11 versioning source of truth: an MLflow-registered model version
per model prefix (`ep`/`wp`), with an explicit `champion` alias moved only by `promote`
(the CLI wires this to `ffep promote`). `ffep score` resolves the champion alias through
`resolve_champion` instead of picking up the newest FINISHED run of an experiment.

The registry requires the SQLite-backed tracking store from `mlflow_store` -- MLflow's
model-registry API (`register_model`/alias calls) does not work against the deprecated local
`file:` FileStore (RESEARCH.md Pitfall 1). Every public function here calls
`mlflow_store.configure(config)` first, so a differently-set ambient tracking URI cannot
redirect a lookup to an uncontrolled store.

This module extends the T-1.2-08 identifier-validation mitigation from `score.py`
(`_validate_run_id`, plain-hex-only run ids) to registered-model names and aliases:
`_validate_model_name`/`_validate_alias` reject anything outside a plain
`[A-Za-z0-9_-]{1,64}` identifier before it is interpolated into a `models:/{name}@{alias}`
URI (T-1.3-07).
"""

from __future__ import annotations

import re

import mlflow
import mlflow.xgboost
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from flag_football_ep.config import Config
from flag_football_ep.model import mlflow_store

CHAMPION_ALIAS: str = "champion"

# Plain identifier only -- the mitigation for T-1.3-07: no path separator, `.`, whitespace,
# or other fragment can ever reach a `models:/{name}@{alias}` uri interpolation.
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class RegistryError(ValueError):
    """Raised for every failure mode in this module -- invalid identifiers, a registration
    or promotion that cannot be completed, or a champion-alias lookup that fails. Follows the
    codebase's no-silent-failure, named-exception convention (see `score.RunNotFound`).
    """


def _validate_model_name(name: str) -> None:
    if not _IDENTIFIER_PATTERN.match(name):
        raise ValueError(
            f"registered model name {name!r} is not a plain identifier; refusing to build a "
            "models:/ uri from it (this extends the T-1.2-08 run-id-validation mitigation to "
            "registered-model names, T-1.3-07)"
        )


def _validate_alias(alias: str) -> None:
    if not _IDENTIFIER_PATTERN.match(alias):
        raise ValueError(
            f"registry alias {alias!r} is not a plain identifier; refusing to build a "
            "models:/ uri from it (this extends the T-1.2-08 run-id-validation mitigation to "
            "registry aliases, T-1.3-07)"
        )


def registered_model_name(model_prefix: str) -> str:
    """`"ep"` -> `"ep_model"`, `"wp"` -> `"wp_model"` -- the registered-model name for a
    model prefix, validated before being returned.
    """
    name = f"{model_prefix}_model"
    _validate_model_name(name)
    return name


def register_production_model(model, name: str, config: Config) -> str:
    """Log `model` into the active MLflow run and register it as a new version of `name`.

    Must be called from inside an active `mlflow.start_run()` block -- this function does
    not start one itself, matching `train.py`'s existing `_log_run` shape where the caller
    owns the run context. Returns the newest version string for `name` (parseable as an
    int); a second call under the same `name` always produces a strictly greater version --
    no version is ever overwritten.
    """
    _validate_model_name(name)
    mlflow_store.configure(config)
    try:
        mlflow.xgboost.log_model(model, name="model", registered_model_name=name)
        versions = MlflowClient().search_model_versions(f"name='{name}'")
    except MlflowException as exc:
        raise RegistryError(
            f"failed to register model version for {name!r} against tracking store "
            f"{mlflow_store.tracking_uri(config)!r}: {exc}"
        ) from exc
    if not versions:
        raise RegistryError(
            f"registered model {name!r} has no versions after log_model against tracking "
            f"store {mlflow_store.tracking_uri(config)!r}"
        )
    newest = max(versions, key=lambda mv: int(mv.version))
    return newest.version


def promote(name: str, run_id: str, config: Config) -> str:
    """Set the `champion` alias on `name` to the version produced by `run_id`.

    A second `promote` call with a different run moves the alias and leaves the previous
    version present in the registry (aliases are reassigned, never delete a version).
    Raises `RegistryError` naming both `run_id` and `name` when no registered version of
    `name` was produced by `run_id`.
    """
    _validate_model_name(name)
    # Function-local import: score.py will import resolve_champion from this module, so this
    # import must stay inside promote to avoid a circular import at module load time.
    from flag_football_ep.model.score import _validate_run_id

    _validate_run_id(run_id)
    mlflow_store.configure(config)
    try:
        versions = MlflowClient().search_model_versions(f"run_id='{run_id}'")
    except MlflowException as exc:
        raise RegistryError(
            f"failed to look up model versions for run {run_id!r} against tracking store "
            f"{mlflow_store.tracking_uri(config)!r}: {exc}"
        ) from exc
    matches = [mv for mv in versions if mv.name == name]
    if not matches:
        raise RegistryError(
            f"run {run_id!r} has no registered version under model name {name!r} -- "
            f"register it first (tracking store {mlflow_store.tracking_uri(config)!r})"
        )
    version = matches[0].version
    try:
        MlflowClient().set_registered_model_alias(name, CHAMPION_ALIAS, version)
    except MlflowException as exc:
        raise RegistryError(
            f"failed to set the {CHAMPION_ALIAS!r} alias on {name!r} version {version!r}: {exc}"
        ) from exc
    return version


def resolve_champion(name: str, config: Config) -> str:
    """Return the run id of the version currently aliased `champion` under `name`.

    Raises `RegistryError` naming `name` and telling the operator to run `ffep promote`
    when no `champion` alias has been set (including when `name` was never registered).
    """
    _validate_model_name(name)
    mlflow_store.configure(config)
    try:
        mv = MlflowClient().get_model_version_by_alias(name, CHAMPION_ALIAS)
    except MlflowException as exc:
        raise RegistryError(
            f"no {CHAMPION_ALIAS!r} alias set for registered model {name!r} in tracking "
            f"store {mlflow_store.tracking_uri(config)!r} -- run `ffep promote` after "
            "reviewing a training run"
        ) from exc
    return mv.run_id
