"""Single definition of the MLflow tracking store location for this pipeline.

The MLflow model registry (REQ-S1-11, phase 1.3: "MLflow model registry is primary") does
not work against a local `file:` tracking store -- `mlflow.register_model`/
`MlflowClient().create_registered_model(...)` raise or partially succeed in confusing ways,
because the registry is a metadata feature that requires SQL query capabilities FileStore
does not implement (RESEARCH.md Pitfall 1). This module is therefore the only place that
constructs the tracking URI and artifact root: every training/scoring/registry module routes
through `configure`/`ensure_experiment` instead of calling `mlflow.set_tracking_uri` with a
`file:` URI directly.

**M3-05-09 addendum (ADR `docs/adr/0001-modell-plattform.md`, Option (ii) gestaffelt, Punkt
1):** setting the `MLFLOW_TRACKING_URI` environment variable overrides the local sqlite store
everywhere in this module -- `tracking_uri()` returns it verbatim, `configure()` skips
creating `config.paths.mlruns` (a remote store has no local directory to create), and
`ensure_experiment()` omits `artifact_location=` for a remote (`http(s)://`) target, since the
containerised MLflow server resolves its own artifact root server-side
(`--serve-artifacts --artifacts-destination s3://...`, proxied -- see
`docker-compose.mlflow.yml`). Unsetting the variable reverts every call site to the unchanged
local sqlite/file behavior -- this IS the rollback path, not a separate code branch to
maintain. Set it via `export MLFLOW_TRACKING_URI=http://127.0.0.1:5000` or in a local,
gitignored `.env` (never `.env.mlflow` -- that file is docker-compose's own and is never read
by `config.load_dotenv()`). See `docs/mlflow-container-platform.md` for the full runbook.
"""

from __future__ import annotations

import os

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from flag_football_ep.config import Config

TRACKING_URI_ENV_VAR = "MLFLOW_TRACKING_URI"


class MlflowStoreError(RuntimeError):
    """Raised when the configured MLflow store rejects an experiment-creation call."""


def _is_remote_tracking_uri(uri: str) -> bool:
    """True for a containerised/hosted MLflow server (`http://`/`https://`), False for the
    local sqlite store. Used only to decide whether `ensure_experiment` may pass an explicit
    `artifact_location` -- a remote server resolves its own artifact root server-side and
    rejects (or ignores, depending on version) a client-supplied `file://` location.
    """
    return uri.startswith("http://") or uri.startswith("https://")


def tracking_uri(config: Config) -> str:
    """The MLflow tracking URI: `MLFLOW_TRACKING_URI` if set to a remote (`http://`/`https://`)
    address, else the SQLite-backed URI for `config.paths.mlruns`.

    `config.paths.mlruns` is already an absolute path (resolved relative to `ffep.toml`'s
    directory by `load_config`), so the resulting local URI has four leading slashes:
    `sqlite:///` + an absolute path that itself starts with `/`.

    When `MLFLOW_TRACKING_URI` is set (e.g. `http://127.0.0.1:5000` for the docker-compose
    platform, see the module-level M3-05-09 addendum above), it is returned verbatim and
    `config.paths.mlruns` is ignored entirely.

    **Deliberately restricted to `http(s)://` values, a verified correction session-tested
    against the installed `mlflow` 3.15.1:** `mlflow.set_tracking_uri()` itself writes
    `MLFLOW_TRACKING_URI` into `os.environ` as a side effect ("so that subprocess can inherit
    it", `mlflow/tracking/_tracking_service/utils.py`) -- for ANY tracking URI, not just this
    module's own local-sqlite one. This project already has a pre-existing regression contract
    (`tests/test_model_registry.py::test_resolve_champion_ignores_a_differently_set_ambient_tracking_uri`
    and its `promote`/`load_model` siblings) that a *different* piece of code calling
    `mlflow.set_tracking_uri(...)` directly (simulating unrelated ambient state) must NOT
    redirect `registry.py`/`score.py` functions away from `config`'s own store -- but that
    ambient call's side effect lands in the exact same `os.environ[MLFLOW_TRACKING_URI]` slot
    this override reads. Restricting recognition to `http(s)://` values only (the only shape
    this feature's real use case ever produces -- every documented example points at the
    containerised MLflow *server*, never a `sqlite:///` path) means an ambient
    `sqlite:///...` echo, from this module's own prior local-store `configure()` call or from
    any unrelated direct `mlflow.set_tracking_uri()` call elsewhere, can never be
    misinterpreted as an intentional override -- the two cases are structurally
    indistinguishable by presence alone, but never share a URI scheme.
    """
    override = os.environ.get(TRACKING_URI_ENV_VAR)
    if override and _is_remote_tracking_uri(override):
        return override
    return "sqlite:///" + str(config.paths.mlruns / "mlflow.db")


def artifact_location(config: Config) -> str:
    """The `file://` artifact root under `config.paths.mlruns`.

    Passed explicitly to `mlflow.create_experiment` so run/model artifacts keep living on
    disk under `config.paths.mlruns` even though run *metadata* now lives in `mlflow.db`.
    Only meaningful for the local sqlite store -- a remote tracking URI never reaches this
    function (see `ensure_experiment`'s remote guard).
    """
    return "file://" + str(config.paths.mlruns / "artifacts")


def configure(config: Config) -> None:
    """Point the ambient MLflow tracking URI at the configured store.

    Creates `config.paths.mlruns` first when using the local sqlite store -- SQLAlchemy will
    not create the parent directory for the `.db` file on its own. Skipped when
    `MLFLOW_TRACKING_URI` is set to a remote store: it has no local `mlruns` directory to
    create.

    **Secondary hygiene, defense-in-depth alongside `tracking_uri()`'s own `http(s)://`-only
    override restriction (see its docstring for the primary fix and the full explanation):**
    `mlflow.set_tracking_uri()` writes `MLFLOW_TRACKING_URI` into `os.environ` as a side
    effect for every call, local or remote. `os.environ`'s state is captured here before
    calling `mlflow.set_tracking_uri`; if no override was present beforehand, the resulting
    local-sqlite echo is popped back off immediately after, so `os.environ` never carries a
    stray `sqlite:///...` value between calls in the same process (harmless after the
    `http(s)://`-only restriction below, but kept clean regardless -- some other, unrelated
    tool inheriting this process's environment should not see an ffep-internal local path).
    When a real override *was* present (the normal `MLFLOW_TRACKING_URI=http://...` case this
    addendum exists for), nothing is touched -- the env var already holds the correct value.
    """
    override_present_before = os.environ.get(TRACKING_URI_ENV_VAR) is not None
    resolved_uri = tracking_uri(config)
    if not _is_remote_tracking_uri(resolved_uri):
        config.paths.mlruns.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(resolved_uri)
    if not override_present_before:
        os.environ.pop(TRACKING_URI_ENV_VAR, None)


def ensure_experiment(name: str, config: Config) -> str:
    """Return the id of experiment `name`, creating it against the configured store if needed.

    Idempotent: a second call with the same `name` returns the same id without raising. In
    both branches the fluent API's active experiment is set to `name` via
    `mlflow.set_experiment` so subsequent `mlflow.start_run()` calls land in the right place.

    `artifact_location=` is only passed to `mlflow.create_experiment` for the local sqlite
    store -- a remote (`http://`/`https://`) tracking URI resolves its own artifact root
    server-side (`--artifacts-destination`, proxied), so passing a local `file://` location
    there would be both meaningless and wrong.
    """
    configure(config)
    client = MlflowClient()
    experiment = client.get_experiment_by_name(name)
    if experiment is None:
        create_kwargs = {}
        if not _is_remote_tracking_uri(tracking_uri(config)):
            create_kwargs["artifact_location"] = artifact_location(config)
        try:
            experiment_id = mlflow.create_experiment(name, **create_kwargs)
        except MlflowException as exc:
            raise MlflowStoreError(
                f"failed to create MLflow experiment {name!r} against tracking store "
                f"{tracking_uri(config)!r}: {exc}"
            ) from exc
    else:
        experiment_id = experiment.experiment_id
    mlflow.set_experiment(name)
    return experiment_id
