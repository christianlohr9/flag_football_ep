"""Freeze a detector run as a distinct, never-reused alias for hackathon delivery.

Sibling of `flag_football_ep.model.registry`'s `champion` alias mechanism
(`cv.registry.py::promote`/`resolve_champion`), but pointed at a second alias
(`FROZEN_ALIAS`) so active-learning retraining after the freeze point never
silently moves the bundle a hackathon deliverable is built from (RESEARCH
Pitfall 5). `freeze`/`resolve_frozen` mirror `registry.py::promote`/
`resolve_champion` exactly, reusing `flag_football_ep.model.registry.RegistryError`
as the base of this module's own `FreezeError` rather than inventing a parallel
error family. `write_freeze_pin`/`read_freeze_pin` persist the explicit artifact
(`FreezePin`: `run_id`, `dataset_hash`, `frozen_at`, `model_version`) the bundle
builder (`cv.bundle.build_bundle`) reads, distinct from the MLflow alias itself.

`write_freeze_pin`'s `model_version` is resolved internally via the same
`search_model_versions(run_id=...)` lookup `freeze` performs -- the contracted
signature (`config, run_id, dataset_hash, path`) leaves no room for a caller to
pass it explicitly, and the CLI (`ffep cv freeze`) calls `write_freeze_pin` as a
separate step after `freeze` without threading the resolved version through.

Implements the contract frozen by plan 02.2-05.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from flag_football_ep.cv.registry import detector_model_name
from flag_football_ep.model import mlflow_store
from flag_football_ep.model.registry import RegistryError, _validate_model_name

if TYPE_CHECKING:
    from pathlib import Path

    from flag_football_ep.config import Config

# Distinct from flag_football_ep.model.registry.CHAMPION_ALIAS ("champion") and
# flag_football_ep.cv.registry.CHAMPION_ALIAS (the same re-exported constant) --
# never reused for the rolling champion alias (T-2.2-14).
FROZEN_ALIAS = "hackathon-frozen"

_REQUIRED_PIN_KEYS = {"run_id", "dataset_hash", "frozen_at", "model_version"}


class FreezeError(RegistryError):
    """Raised when a detector run cannot be frozen, or a freeze-pin file cannot be
    written/read: no registered version for the given run id, an MLflow failure, or
    a malformed/missing pin file.
    """


@dataclass(frozen=True)
class FreezePin:
    """The explicit, git/DVC-independent record of what a hackathon bundle was built
    from: the frozen detector's MLflow run id, the dataset content hash it was
    trained on, when the freeze happened, and the resulting registered model version.
    """

    run_id: str
    dataset_hash: str
    frozen_at: str
    model_version: str


def _resolve_model_version(name: str, run_id: str, config: Config) -> str:
    """Look up the registered model version produced by `run_id` under `name`.

    Shared by `freeze` and `write_freeze_pin` -- both need the version, but neither
    can pass it to the other through the contracted signatures.
    """
    mlflow_store.configure(config)
    try:
        versions = MlflowClient().search_model_versions(f"run_id='{run_id}'")
    except MlflowException as exc:
        raise FreezeError(
            f"failed to look up model versions for run {run_id!r} against tracking store "
            f"{mlflow_store.tracking_uri(config)!r}: {exc}"
        ) from exc
    matches = [mv for mv in versions if mv.name == name]
    if not matches:
        raise FreezeError(
            f"run {run_id!r} has no registered version under model name {name!r} -- "
            f"register it first (tracking store {mlflow_store.tracking_uri(config)!r})"
        )
    return matches[0].version


def freeze(name: str, run_id: str, config: Config) -> str:
    """Set the `FROZEN_ALIAS` alias on `name` to the version produced by `run_id`.

    Mirrors `registry.py::promote` exactly, but on the sibling `FROZEN_ALIAS` so a
    later `champion` promotion from active-learning retraining never moves this
    alias. Returns the model version, matching `promote`'s contract.
    """
    _validate_model_name(name)
    # Function-local import: mirrors registry.py::promote's own circular-import
    # avoidance comment.
    from flag_football_ep.model.score import _validate_run_id

    _validate_run_id(run_id)
    version = _resolve_model_version(name, run_id, config)
    try:
        MlflowClient().set_registered_model_alias(name, FROZEN_ALIAS, version)
    except MlflowException as exc:
        raise FreezeError(
            f"failed to set the {FROZEN_ALIAS!r} alias on {name!r} version {version!r}: {exc}"
        ) from exc
    return version


def resolve_frozen(name: str, config: Config) -> str:
    """Return the run id of the version currently aliased `FROZEN_ALIAS` under
    `name`. Mirrors `registry.py::resolve_champion` exactly.
    """
    _validate_model_name(name)
    mlflow_store.configure(config)
    try:
        mv = MlflowClient().get_model_version_by_alias(name, FROZEN_ALIAS)
    except MlflowException as exc:
        raise FreezeError(
            f"no {FROZEN_ALIAS!r} alias set for registered detector model {name!r} in "
            f"tracking store {mlflow_store.tracking_uri(config)!r} -- run `ffep cv "
            "freeze` after reviewing a training run"
        ) from exc
    return mv.run_id


def write_freeze_pin(
    config: Config, run_id: str, dataset_hash: str, path: Path
) -> Path:
    """Persist `{run_id, dataset_hash, frozen_at, model_version}` to `path`, using
    the same `.tmp` + `os.replace` atomic-write discipline as
    `frames.py::write_manifest`.

    Re-freezing is a decision, not a retry: if `path` already exists and its
    `run_id` differs from the one being written, this raises `FreezeError` naming
    both run ids. A caller that wants to deliberately re-pin (the CLI's `--force`
    path) removes the existing file first rather than this function silently
    overwriting it.
    """
    if path.exists():
        try:
            existing = read_freeze_pin(path)
        except FreezeError:
            existing = None
        if existing is not None and existing.run_id != run_id:
            raise FreezeError(
                f"refusing to overwrite existing freeze pin at {path} (run_id="
                f"{existing.run_id!r}) with a different run_id {run_id!r} -- a re-freeze is "
                "a decision, not a retry; remove the existing pin first to force it"
            )

    name = detector_model_name(config)
    model_version = _resolve_model_version(name, run_id, config)
    pin = FreezePin(
        run_id=run_id,
        dataset_hash=dataset_hash,
        frozen_at=datetime.now(UTC).isoformat(),
        model_version=model_version,
    )
    data = asdict(pin)

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_text(
            json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return path


def read_freeze_pin(path: Path) -> FreezePin:
    """Load a `FreezePin` previously written by `write_freeze_pin`.

    Raises `FreezeError` naming `path` when the file is absent, is not valid JSON,
    or is missing a required top-level key -- a malformed pin must never be
    silently accepted, since bundle builds key on it.
    """
    if not path.exists():
        raise FreezeError(f"freeze pin not found: {path}")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise FreezeError(f"freeze pin at {path} is not valid JSON: {exc}") from exc

    missing_keys = _REQUIRED_PIN_KEYS - data.keys()
    if missing_keys:
        raise FreezeError(
            f"freeze pin at {path} is missing key(s) {sorted(missing_keys)}"
        )

    return FreezePin(
        run_id=data["run_id"],
        dataset_hash=data["dataset_hash"],
        frozen_at=data["frozen_at"],
        model_version=data["model_version"],
    )
