"""MLflow model registry helpers for the RF-DETR player/referee detector.

Mirrors `flag_football_ep.model.registry` exactly (REQ-S1-11's versioning discipline
extended to the CV detector): an MLflow-registered model version per detector name, a
`champion` alias moved only by `promote`, and `resolve_champion` -- never "the newest
FINISHED run" -- as `cv.detect.load_detector`'s default resolution. This is the second
place in the codebase that registers pyfunc-style models; `RFDETRWrapper` wraps an
RF-DETR checkpoint as an `mlflow.pyfunc.PythonModel` (the checkpoint is not an xgboost
model, so `register_detector_model` calls `mlflow.pyfunc.log_model`, not
`mlflow.xgboost.log_model`).

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

Implemented by plan 02.1-06.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import mlflow.pyfunc

from flag_football_ep.model.registry import CHAMPION_ALIAS, _validate_alias, _validate_model_name

if TYPE_CHECKING:
    from pathlib import Path

    from flag_football_ep.config import Config

__all__ = [
    "CHAMPION_ALIAS",
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
    raise NotImplementedError("cv.registry.detector_model_name is implemented by plan 02.1-06")


class RFDETRWrapper(mlflow.pyfunc.PythonModel):
    """`mlflow.pyfunc.PythonModel` wrapper around a trained RF-DETR checkpoint, so the
    detector can be registered/versioned/promoted through the same MLflow model-registry
    API the EP/WP xgboost models already use.
    """

    def predict(self, context, model_input, params=None):
        raise NotImplementedError(
            "cv.registry.RFDETRWrapper.predict is implemented by plan 02.1-06"
        )


def register_detector_model(checkpoint: Path, name: str, config: Config) -> str:
    """Log the RF-DETR checkpoint at `checkpoint` (wrapped in `RFDETRWrapper`) into the
    active MLflow run and register it as a new version of `name`. Returns the newest
    version string, matching `model.registry.register_production_model`'s contract.
    """
    raise NotImplementedError(
        "cv.registry.register_detector_model is implemented by plan 02.1-06"
    )


def promote(name: str, run_id: str, config: Config) -> str:
    """Set the `champion` alias on `name` to the version produced by `run_id`."""
    raise NotImplementedError("cv.registry.promote is implemented by plan 02.1-06")


def resolve_champion(name: str, config: Config) -> str:
    """Return the run id of the version currently aliased `champion` under `name`."""
    raise NotImplementedError("cv.registry.resolve_champion is implemented by plan 02.1-06")
