"""MLflow model registry helpers for the RF-DETR player/referee detector.

Mirrors `flag_football_ep.model.registry` exactly (REQ-S1-11's versioning discipline
extended to the CV detector): an MLflow-registered model version per detector name, a
`champion` alias moved only by `promote`, and `resolve_champion` -- never "the newest
FINISHED run" -- as `cv.detect.load_detector`'s default resolution. This is the second
place in the codebase that registers pyfunc-style models; `RFDETRWrapper` wraps an
RF-DETR checkpoint as an `mlflow.pyfunc.PythonModel` (the checkpoint is not a bare
xgboost model, so `register_detector_model` logs it through the generic pyfunc flavour,
not the xgboost-specific one `model/registry.py` uses).

Every function here calls `flag_football_ep.model.mlflow_store.configure(config)`
first, exactly like `model.registry` does -- this is the single shared MLflow
tracking-store definition for the whole project (there is exactly one store, D-15); this
module never constructs a second one or points the ambient tracking URI anywhere else,
for the same reason `model/registry.py` documents (the plain file-backed store cannot
back the model registry).

Identifier validation is reused, not reinvented (T-2.1-03): `_validate_model_name`,
`_validate_alias` and `CHAMPION_ALIAS` are imported directly from
`flag_football_ep.model.registry` rather than a second, looser regex living here.
`_validate_run_id` is imported from `flag_football_ep.model.score` function-locally
inside `promote`, mirroring the existing circular-import-avoidance comment in
`model/registry.py::promote`.

RF-DETR `.predict()` signature verified against the installed `rfdetr==1.9.3` (RESEARCH
assumption A4): `RFDETR.__init__(self, *, trust_checkpoint=False, **kwargs)` passes
`**kwargs` straight to the variant's pydantic `ModelConfig` (`pretrain_weights` is a
documented `ModelConfig` field, so `RFDETRSmall(pretrain_weights=<path>)` is valid), and
`RFDETRSmall.predict(self, images, threshold=0.5, shape=None, patch_size=None,
include_source_image=True, **kwargs) -> Detections | KeyPoints | list[...]` accepts the
same `model_input` shapes (`str | Image.Image | np.ndarray | torch.Tensor | list[...]`) a
pyfunc caller would pass through `model_input`.

Implemented by plan 02.1-06.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import mlflow.pyfunc
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from flag_football_ep.model import mlflow_store
from flag_football_ep.model.registry import (
    CHAMPION_ALIAS,
    RegistryError,
    _validate_alias,
    _validate_model_name,
)

if TYPE_CHECKING:
    from pathlib import Path

    from flag_football_ep.config import Config

__all__ = [
    "CHAMPION_ALIAS",
    "CheckpointNotFound",
    "RegistryError",
    "RFDETRWrapper",
    "detector_model_name",
    "promote",
    "register_detector_model",
    "resolve_champion",
]


def detector_model_name(config: Config) -> str:
    """The validated registered-model name for the pilot detector
    (`config.cv.detector_model`).
    """
    name = config.cv.detector_model
    _validate_model_name(name)
    return name


class RFDETRWrapper(mlflow.pyfunc.PythonModel):
    """`mlflow.pyfunc.PythonModel` wrapper around a trained RF-DETR checkpoint, so the
    detector can be registered/versioned/promoted through the same MLflow model-registry
    API the EP/WP xgboost models already use.
    """

    def load_context(self, context) -> None:
        # Function-local import: rfdetr is a `cv`-extras dependency, never a module-level
        # import (D-07/D-08 -- `import flag_football_ep.cv.registry` must stay clean of
        # the heavy CV stack when the extras group is not installed).
        from rfdetr import RFDETRSmall

        self.model = RFDETRSmall(pretrain_weights=context.artifacts["weights"])

    def predict(self, context, model_input, params=None):
        # `params` is the standard MLflow pyfunc side-channel (e.g. `{"shape": (r, r)}`
        # from `cv.detect._call_model`) -- forwarded straight into the wrapped
        # `RFDETRSmall.predict(**params)` call so a caller of the loaded pyfunc model
        # can still control per-call inference options (resolution, threshold) the
        # same way it could against the raw `RFDETRSmall` instance. `None`/`{}` is a
        # no-op, matching the previous unconditional `self.model.predict(model_input)`.
        return self.model.predict(model_input, **(params or {}))


class CheckpointNotFound(RegistryError):
    """Raised when `register_detector_model` is called with a checkpoint path that does
    not exist on disk -- caught before any MLflow call is made.
    """


def register_detector_model(checkpoint: Path, name: str, config: Config) -> str:
    """Log the RF-DETR checkpoint at `checkpoint` (wrapped in `RFDETRWrapper`) into the
    active MLflow run and register it as a new version of `name`. Returns the newest
    version string, matching `model.registry.register_production_model`'s contract.

    Must be called from inside an active `mlflow.start_run()` block -- this function does
    not start one itself, matching `model.registry.register_production_model`'s contract.
    A second call under the same `name` always produces a strictly greater version -- no
    version is ever overwritten.
    """
    _validate_model_name(name)
    if not checkpoint.exists():
        raise CheckpointNotFound(
            f"RF-DETR checkpoint {checkpoint!r} does not exist -- refusing to log a model "
            "with no weights on disk"
        )
    mlflow_store.configure(config)
    try:
        mlflow.pyfunc.log_model(
            name="model",
            python_model=RFDETRWrapper(),
            artifacts={"weights": str(checkpoint)},
            registered_model_name=name,
        )
        versions = MlflowClient().search_model_versions(f"name='{name}'")
    except MlflowException as exc:
        raise RegistryError(
            f"failed to register detector model version for {name!r} against tracking "
            f"store {mlflow_store.tracking_uri(config)!r}: {exc}"
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
    # Function-local import: score.py will import resolve_champion from this module, so
    # this import must stay inside promote to avoid a circular import at module load time
    # (mirrors model/registry.py::promote's own comment).
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

    Raises `RegistryError` naming `name` and telling the operator to run `ffep cv
    promote` after reviewing a training run when no `champion` alias has been set
    (including when `name` was never registered).
    """
    _validate_model_name(name)
    mlflow_store.configure(config)
    try:
        mv = MlflowClient().get_model_version_by_alias(name, CHAMPION_ALIAS)
    except MlflowException as exc:
        raise RegistryError(
            f"no {CHAMPION_ALIAS!r} alias set for registered detector model {name!r} in "
            f"tracking store {mlflow_store.tracking_uri(config)!r} -- run `ffep cv "
            "promote` after reviewing a training run"
        ) from exc
    return mv.run_id
