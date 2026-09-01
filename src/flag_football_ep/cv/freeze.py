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

Every function below raises `NotImplementedError` naming the plan that implements
it -- this module is a contract freeze only (plan 02.2-05); the real MLflow calls
and atomic-JSON round trip are implemented by plan 02.2-07.

Implemented by plan 02.2-07.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from flag_football_ep.model.registry import RegistryError

if TYPE_CHECKING:
    from pathlib import Path

    from flag_football_ep.config import Config

# Distinct from flag_football_ep.model.registry.CHAMPION_ALIAS ("champion") and
# flag_football_ep.cv.registry.CHAMPION_ALIAS (the same re-exported constant) --
# never reused for the rolling champion alias (T-2.2-14).
FROZEN_ALIAS = "hackathon-frozen"


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


def freeze(name: str, run_id: str, config: Config) -> str:
    """Set the `FROZEN_ALIAS` alias on `name` to the version produced by `run_id`.

    Mirrors `registry.py::promote` exactly, but on the sibling `FROZEN_ALIAS` so a
    later `champion` promotion from active-learning retraining never moves this
    alias. Returns the model version, matching `promote`'s contract.

    Implemented by plan 02.2-07.
    """
    raise NotImplementedError("implemented by plan 02.2-07")


def resolve_frozen(name: str, config: Config) -> str:
    """Return the run id of the version currently aliased `FROZEN_ALIAS` under
    `name`. Mirrors `registry.py::resolve_champion` exactly.

    Implemented by plan 02.2-07.
    """
    raise NotImplementedError("implemented by plan 02.2-07")


def write_freeze_pin(config: Config, run_id: str, dataset_hash: str, path: Path) -> Path:
    """Persist `{run_id, dataset_hash, frozen_at, model_version}` to `path`, using
    the same `.tmp` + `os.replace` atomic-write discipline as
    `frames.py::write_manifest`.

    Implemented by plan 02.2-07.
    """
    raise NotImplementedError("implemented by plan 02.2-07")


def read_freeze_pin(path: Path) -> FreezePin:
    """Load a `FreezePin` previously written by `write_freeze_pin`.

    Implemented by plan 02.2-07.
    """
    raise NotImplementedError("implemented by plan 02.2-07")
