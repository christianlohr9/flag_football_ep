"""Coverage for `flag_football_ep.cv.registry`: champion-alias round trip against a
`tmp_path` SQLite MLflow store, mirroring `tests/test_model_registry.py`.

Every test builds a config pointing `mlruns` (and every other path) at `tmp_path`, never
the real repo `mlruns/`. `RFDETRWrapper.load_context` is monkeypatched to load a stub
object with a `.predict()` -- no real RF-DETR weights, no network, no `rfdetr` import at
test time. These tests never assert on detector quality.
"""

from __future__ import annotations

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
from flag_football_ep.cv import registry
from flag_football_ep.cv.registry import CHAMPION_ALIAS, RegistryError, RFDETRWrapper
from flag_football_ep.model import mlflow_store

# --- shared test config helper -------------------------------------------------------------


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


class _StubDetector:
    """No real RF-DETR weights, no network -- just enough to satisfy `RFDETRWrapper`."""

    def predict(self, model_input):
        return {"boxes": [], "input": model_input}


def _register_trivial_version(
    config: Config, name: str, checkpoint: Path, experiment: str = "cv_detector_test"
) -> tuple[str, str]:
    """Start a run against `experiment`, register a stub `RFDETRWrapper` under `name`.

    Returns `(run_id, version)`.
    """
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_bytes(b"not-a-real-checkpoint")
    mlflow_store.ensure_experiment(experiment, config)
    with mlflow.start_run() as run:
        version = registry.register_detector_model(checkpoint, name, config)
    return run.info.run_id, version


@pytest.fixture(autouse=True)
def _stub_load_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test in this module monkeypatches `RFDETRWrapper.load_context` so no real
    `rfdetr` import or weights download ever happens.
    """

    def _load_context(self, context) -> None:
        self.model = _StubDetector()

    monkeypatch.setattr(RFDETRWrapper, "load_context", _load_context)


# --- detector_model_name -----------------------------------------------------------------


def test_detector_model_name_returns_validated_config_value(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    assert registry.detector_model_name(config) == "cv_detector_model_test"


# --- RFDETRWrapper -------------------------------------------------------------------------


def test_rfdetr_wrapper_is_pyfunc_python_model() -> None:
    import mlflow.pyfunc

    assert issubclass(RFDETRWrapper, mlflow.pyfunc.PythonModel)


# --- register_detector_model ---------------------------------------------------------------


def test_register_detector_model_returns_parseable_version(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"

    _run_id, version = _register_trivial_version(config, "cv_detector_model_test", checkpoint)

    assert int(version) >= 1


def test_register_detector_model_second_call_yields_strictly_greater_version(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"

    _first_run_id, first_version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )
    _second_run_id, second_version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )

    assert int(second_version) > int(first_version)

    from mlflow import MlflowClient

    versions = MlflowClient().search_model_versions("name='cv_detector_model_test'")
    version_numbers = {mv.version for mv in versions}
    assert first_version in version_numbers
    assert second_version in version_numbers


def test_register_detector_model_missing_checkpoint_raises_before_mlflow_call(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    mlflow_store.ensure_experiment("cv_detector_test", config)
    missing = tmp_path / "does-not-exist.pth"

    with mlflow.start_run():
        with pytest.raises(RegistryError) as exc_info:
            registry.register_detector_model(missing, "cv_detector_model_test", config)

    assert str(missing) in str(exc_info.value)

    from mlflow import MlflowClient

    versions = MlflowClient().search_registered_models()
    assert not any(rm.name == "cv_detector_model_test" for rm in versions)


# --- promote ---------------------------------------------------------------------------


def test_promote_sets_champion_alias_to_run_version(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    run_id, version = _register_trivial_version(config, "cv_detector_model_test", checkpoint)

    returned_version = registry.promote("cv_detector_model_test", run_id, config)

    assert returned_version == version
    assert registry.resolve_champion("cv_detector_model_test", config) == run_id


def test_promote_second_call_moves_alias_and_leaves_previous_version_present(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    first_run_id, first_version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )
    second_run_id, _second_version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )

    registry.promote("cv_detector_model_test", first_run_id, config)
    assert registry.resolve_champion("cv_detector_model_test", config) == first_run_id

    registry.promote("cv_detector_model_test", second_run_id, config)
    assert registry.resolve_champion("cv_detector_model_test", config) == second_run_id

    from mlflow import MlflowClient

    versions = MlflowClient().search_model_versions("name='cv_detector_model_test'")
    version_numbers = {mv.version for mv in versions}
    assert first_version in version_numbers


def test_promote_with_run_id_lacking_registered_version_raises_registry_error(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    mlflow_store.ensure_experiment("cv_detector_test", config)
    with mlflow.start_run() as run:
        pass  # a run with no model version registered under "cv_detector_model_test"

    with pytest.raises(RegistryError) as exc_info:
        registry.promote("cv_detector_model_test", run.info.run_id, config)

    message = str(exc_info.value)
    assert run.info.run_id in message
    assert "cv_detector_model_test" in message


def test_promote_rejects_non_hex_run_id(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    _register_trivial_version(config, "cv_detector_model_test", checkpoint)

    with pytest.raises(ValueError):
        registry.promote("cv_detector_model_test", "../../etc", config)


# --- resolve_champion ------------------------------------------------------------------


def test_resolve_champion_returns_run_id(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    run_id, _version = _register_trivial_version(config, "cv_detector_model_test", checkpoint)
    registry.promote("cv_detector_model_test", run_id, config)

    assert registry.resolve_champion("cv_detector_model_test", config) == run_id


def test_resolve_champion_no_alias_raises_registry_error_naming_model_and_ffep_cv_promote(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    _register_trivial_version(config, "cv_detector_model_test", checkpoint)  # never promoted

    with pytest.raises(RegistryError) as exc_info:
        registry.resolve_champion("cv_detector_model_test", config)

    message = str(exc_info.value)
    assert "cv_detector_model_test" in message
    assert "ffep cv promote" in message


# --- store isolation: every public function reconfigures the tracking uri from config -------


def test_resolve_champion_ignores_a_differently_set_ambient_tracking_uri(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    run_id, _version = _register_trivial_version(config, "cv_detector_model_test", checkpoint)
    registry.promote("cv_detector_model_test", run_id, config)

    mlflow.set_tracking_uri("sqlite:///" + str(tmp_path / "somewhere-else" / "mlflow.db"))

    assert registry.resolve_champion("cv_detector_model_test", config) == run_id
    assert mlflow.get_tracking_uri() == mlflow_store.tracking_uri(config)


def test_promote_ignores_a_differently_set_ambient_tracking_uri(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    run_id, version = _register_trivial_version(config, "cv_detector_model_test", checkpoint)

    mlflow.set_tracking_uri("sqlite:///" + str(tmp_path / "somewhere-else" / "mlflow.db"))

    returned_version = registry.promote("cv_detector_model_test", run_id, config)

    assert returned_version == version
    assert mlflow.get_tracking_uri() == mlflow_store.tracking_uri(config)


# --- import direction: cv.registry does not import cv.detect at module level ----------------


def test_registry_module_does_not_import_cv_detect_at_module_level() -> None:
    source = Path("src/flag_football_ep/cv/registry.py").read_text(encoding="utf-8")
    assert "from flag_football_ep.cv.detect import" not in source
    assert "from flag_football_ep.cv import detect" not in source
    assert "import flag_football_ep.cv.detect" not in source
