"""Coverage for `flag_football_ep.model.mlflow_store`'s `MLFLOW_TRACKING_URI` override
(M3-05-09, ADR `docs/adr/0001-modell-plattform.md` Option (ii) gestaffelt, Punkt 1).

Every test builds a config pointing `mlruns` at `tmp_path` (never the real repo `mlruns/`).
No real MLflow server is required for these tests: `mlflow.set_tracking_uri` never validates
connectivity, and `mlflow.create_experiment` is monkeypatched where its kwargs matter, so a
placeholder `http://` URI is safe to use without a live container.
"""

from __future__ import annotations

import os
from pathlib import Path

import mlflow
import pytest

from flag_football_ep.config import (
    Config,
    CvSettings,
    IfafSource,
    Paths,
    ReferenceFiles,
    ReportSettings,
    Sources,
    SportappSource,
    TrainSettings,
)
from flag_football_ep.model import mlflow_store

_REMOTE_URI = "http://127.0.0.1:5000"


def _make_config(tmp_path: Path) -> Config:
    """A fully-populated Config pointing every path at `tmp_path` -- never the real repo."""
    paths = Paths(
        data_root=tmp_path / "data",
        raw_hudl=tmp_path / "data" / "raw" / "hudl",
        raw_sportapp=tmp_path / "data" / "raw" / "sportapp",
        raw_ifaf=tmp_path / "data" / "raw" / "ifaf",
        raw_legacy=tmp_path / "data" / "raw" / "legacy",
        processed=tmp_path / "data" / "processed",
        reference=tmp_path / "data" / "reference",
        models=tmp_path / "models",
        mlruns=tmp_path / "mlruns",
        contract=tmp_path / "docs" / "data-contract.schema.json",
        reports=tmp_path / "reports",
        video=tmp_path / "data" / "video",
        labels=tmp_path / "data" / "labels",
        tracking=tmp_path / "data" / "processed" / "tracking",
    )
    reference = ReferenceFiles(
        half_boundaries=tmp_path / "data" / "reference" / "half_boundaries.csv",
        final_scores=tmp_path / "data" / "reference" / "final_scores.csv",
        team_mapping=tmp_path / "data" / "reference" / "team_mapping.csv",
        sportapp_games=tmp_path / "data" / "reference" / "sportapp_games.csv",
        competition_tier=tmp_path / "data" / "reference" / "competition_tier.csv",
        player_mapping=tmp_path / "data" / "reference" / "player_mapping.csv",
        group_opponents=tmp_path / "data" / "reference" / "group_opponents.csv",
        hover_positions=tmp_path / "data" / "reference" / "hover_positions.csv",
        homography_calibration=tmp_path / "data" / "reference" / "homography_calibration.csv",
        gt_positions=tmp_path / "data" / "reference" / "gt_positions.csv",
        continuity_review=tmp_path / "data" / "reference" / "continuity_review.csv",
    )
    sources = Sources(
        sportapp=SportappSource(
            base_url="https://example.invalid/api/v1/public", api_key_env="SPORTAPP_API_KEY"
        ),
        ifaf=IfafSource(
            base_url="https://example.invalid/v1",
            tournament="test-tournament",
            api_key_env="CPX_API_KEY",
        ),
    )
    train = TrainSettings(
        ep_experiment="ep_model_test",
        wp_experiment="wp_model_test",
        exclude_games_ep=[],
        exclude_games_wp=[],
    )
    report = ReportSettings(own_team="HOME", cycle_start_season=2026)
    cv = CvSettings(
        pilot_session_id="test-session",
        detector_model="cv_detector_model_test",
        detector_experiment="cv_detector_test",
        resolution=672,
        sahi=False,
        sahi_slice=640,
        sahi_overlap=0.2,
        train_epochs=1,
        train_batch_size=4,
        train_grad_accum=4,
        device="cpu",
        label_frame_target=10,
        cvat_host="http://localhost:8080",
        cvat_username_env="CVAT_USERNAME",
        cvat_password_env="CVAT_PASSWORD",
        field_length_yards=50.0,
        field_width_yards=25.0,
        endzone_yards=10.0,
        dvc_remote_name="otc-obs",
        dvc_remote_url="s3://test-bucket/flag-football-datasets",
        dvc_remote_endpoint="https://obs.eu-de.otc.t-systems.com",
        otc_obs_access_key_env="OTC_OBS_ACCESS_KEY_ID",
        otc_obs_secret_key_env="OTC_OBS_SECRET_ACCESS_KEY",
    )
    return Config(
        paths=paths, reference=reference, sources=sources, train=train, report=report, cv=cv
    )


@pytest.fixture
def config(tmp_path: Path) -> Config:
    return _make_config(tmp_path)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test starts with the override unset -- regardless of the real shell/`.env`."""
    monkeypatch.delenv(mlflow_store.TRACKING_URI_ENV_VAR, raising=False)


# --- tracking_uri --------------------------------------------------------------------------


def test_tracking_uri_unchanged_when_override_unset(config: Config) -> None:
    """Regression guard: every existing caller/test stays correct with no env var set."""
    expected = "sqlite:///" + str(config.paths.mlruns / "mlflow.db")
    assert mlflow_store.tracking_uri(config) == expected


def test_tracking_uri_returns_override_verbatim_when_set(
    config: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(mlflow_store.TRACKING_URI_ENV_VAR, _REMOTE_URI)
    assert mlflow_store.tracking_uri(config) == _REMOTE_URI


def test_tracking_uri_override_ignores_config_paths_mlruns(
    config: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(mlflow_store.TRACKING_URI_ENV_VAR, _REMOTE_URI)
    # A completely different mlruns path must not leak into the returned URI.
    assert str(config.paths.mlruns) not in mlflow_store.tracking_uri(config)
    assert mlflow_store.tracking_uri(config) == _REMOTE_URI


# --- configure ------------------------------------------------------------------------------


def test_configure_creates_mlruns_when_override_unset(config: Config) -> None:
    assert not config.paths.mlruns.exists()
    mlflow_store.configure(config)
    assert config.paths.mlruns.exists()


def test_configure_does_not_create_mlruns_when_override_set(
    config: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(mlflow_store.TRACKING_URI_ENV_VAR, _REMOTE_URI)
    assert not config.paths.mlruns.exists()
    mlflow_store.configure(config)
    assert not config.paths.mlruns.exists()


# --- configure() must not leak state across configs via mlflow's own env-var side effect ---


def test_configure_does_not_leak_local_uri_into_env_for_next_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression guard for a verified `mlflow` 3.15.1 behavior: `mlflow.set_tracking_uri()`
    itself writes `MLFLOW_TRACKING_URI` into `os.environ` as a subprocess-inheritance side
    effect. Without the guard in `configure()`, the first local-sqlite `configure` call in a
    process leaks its resolved path into `os.environ`, and a second, different `Config`'s
    `tracking_uri()` call would misread that leaked value as a real override and get stuck
    on the first config's store. Caught in this session via `tests/test_model_train.py`
    cross-test state bleed (registered-model version counts accumulating across tests).
    """
    config_a = _make_config(tmp_path / "a")
    config_b = _make_config(tmp_path / "b")

    mlflow_store.configure(config_a)
    assert os.environ.get(mlflow_store.TRACKING_URI_ENV_VAR) is None

    assert mlflow_store.tracking_uri(config_b) == mlflow_store.tracking_uri(config_a).replace(
        str(config_a.paths.mlruns), str(config_b.paths.mlruns)
    )
    mlflow_store.configure(config_b)
    assert mlflow.get_tracking_uri() == mlflow_store.tracking_uri(config_b)
    assert mlflow.get_tracking_uri() != mlflow_store.tracking_uri(config_a)


def test_configure_preserves_a_real_override_across_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guard above must not clear a *real* user-set override -- only the echo `mlflow`
    itself writes back for the local-sqlite case.
    """
    monkeypatch.setenv(mlflow_store.TRACKING_URI_ENV_VAR, _REMOTE_URI)
    config = _make_config(tmp_path)

    mlflow_store.configure(config)

    assert os.environ.get(mlflow_store.TRACKING_URI_ENV_VAR) == _REMOTE_URI
    assert mlflow_store.tracking_uri(config) == _REMOTE_URI


# --- ensure_experiment / artifact_location kwarg --------------------------------------------


def test_ensure_experiment_passes_artifact_location_for_local_store(
    config: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict = {}

    def _fake_create_experiment(name: str, **kwargs):
        captured["name"] = name
        captured["kwargs"] = kwargs
        return "0"

    monkeypatch.setattr(mlflow_store.mlflow, "create_experiment", _fake_create_experiment)
    mlflow_store.ensure_experiment("ep_model_test", config)

    assert "artifact_location" in captured["kwargs"]
    assert captured["kwargs"]["artifact_location"] == mlflow_store.artifact_location(config)


def test_ensure_experiment_omits_artifact_location_for_remote_store(
    config: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(mlflow_store.TRACKING_URI_ENV_VAR, _REMOTE_URI)
    captured: dict = {}

    def _fake_create_experiment(name: str, **kwargs):
        captured["name"] = name
        captured["kwargs"] = kwargs
        return "0"

    monkeypatch.setattr(mlflow_store.mlflow, "create_experiment", _fake_create_experiment)
    # get_experiment_by_name against a placeholder http:// URI with no live server would raise
    # a connection error before reaching create_experiment -- also monkeypatch the client
    # lookup to simulate "experiment does not exist yet" without a real server.
    monkeypatch.setattr(
        mlflow_store.MlflowClient,
        "get_experiment_by_name",
        lambda self, name: None,
    )
    monkeypatch.setattr(mlflow_store.mlflow, "set_experiment", lambda name: None)
    mlflow_store.ensure_experiment("ep_model_test", config)

    assert captured["kwargs"] == {}


# --- _is_remote_tracking_uri -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("http://127.0.0.1:5000", True),
        ("https://mlflow.example.invalid", True),
        ("sqlite:////tmp/mlruns/mlflow.db", False),
        ("file:///tmp/mlruns/artifacts", False),
    ],
)
def test_is_remote_tracking_uri(uri: str, expected: bool) -> None:
    assert mlflow_store._is_remote_tracking_uri(uri) is expected
