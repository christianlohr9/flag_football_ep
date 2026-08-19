"""Single definition of the MLflow tracking store location for this pipeline.

The MLflow model registry (REQ-S1-11, phase 1.3: "MLflow model registry is primary") does
not work against a local `file:` tracking store -- `mlflow.register_model`/
`MlflowClient().create_registered_model(...)` raise or partially succeed in confusing ways,
because the registry is a metadata feature that requires SQL query capabilities FileStore
does not implement (RESEARCH.md Pitfall 1). This module is therefore the only place that
constructs the tracking URI and artifact root: every training/scoring/registry module routes
through `configure`/`ensure_experiment` instead of calling `mlflow.set_tracking_uri` with a
`file:` URI directly.
"""

from __future__ import annotations

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from flag_football_ep.config import Config


class MlflowStoreError(RuntimeError):
    """Raised when the configured MLflow store rejects an experiment-creation call."""


def tracking_uri(config: Config) -> str:
    """The SQLite-backed tracking URI for `config.paths.mlruns`.

    `config.paths.mlruns` is already an absolute path (resolved relative to `ffep.toml`'s
    directory by `load_config`), so the resulting URI has four leading slashes:
    `sqlite:///` + an absolute path that itself starts with `/`.
    """
    return "sqlite:///" + str(config.paths.mlruns / "mlflow.db")


def artifact_location(config: Config) -> str:
    """The `file://` artifact root under `config.paths.mlruns`.

    Passed explicitly to `mlflow.create_experiment` so run/model artifacts keep living on
    disk under `config.paths.mlruns` even though run *metadata* now lives in `mlflow.db`.
    """
    return "file://" + str(config.paths.mlruns / "artifacts")


def configure(config: Config) -> None:
    """Point the ambient MLflow tracking URI at the SQLite store for `config.paths.mlruns`.

    Creates `config.paths.mlruns` first -- SQLAlchemy will not create the parent directory
    for the `.db` file on its own.
    """
    config.paths.mlruns.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(tracking_uri(config))


def ensure_experiment(name: str, config: Config) -> str:
    """Return the id of experiment `name`, creating it against the SQLite store if needed.

    Idempotent: a second call with the same `name` returns the same id without raising. In
    both branches the fluent API's active experiment is set to `name` via
    `mlflow.set_experiment` so subsequent `mlflow.start_run()` calls land in the right place.
    """
    configure(config)
    client = MlflowClient()
    experiment = client.get_experiment_by_name(name)
    if experiment is None:
        try:
            experiment_id = mlflow.create_experiment(
                name, artifact_location=artifact_location(config)
            )
        except MlflowException as exc:
            raise MlflowStoreError(
                f"failed to create MLflow experiment {name!r} against tracking store "
                f"{tracking_uri(config)!r}: {exc}"
            ) from exc
    else:
        experiment_id = experiment.experiment_id
    mlflow.set_experiment(name)
    return experiment_id
