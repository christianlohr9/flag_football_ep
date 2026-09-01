"""Coverage for `flag_football_ep.cv.freeze`: the `hackathon-frozen` alias round trip
and the freeze-pin file round trip, both against a `tmp_path` SQLite MLflow store
(mirroring `tests/test_cv_registry.py`), plus the anti-drift guards (T-2.2-19) that
stop a later edit from silently re-pointing the bundle builder at the rolling
`champion` alias.

Every round-trip test builds a config pointing `mlruns` (and every other path) at
`tmp_path`, never the real repo `mlruns/`. `RFDETRWrapper.load_context` is
monkeypatched to load a stub object with a `.predict()` -- no real RF-DETR weights,
no network, no `rfdetr` import at test time. These tests never assert on detector
quality. The anti-drift guards at the bottom of this module are source/file gates
that do not need the monkeypatch.
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
    load_config,
)
from flag_football_ep.cv import registry
from flag_football_ep.cv.freeze import (
    FROZEN_ALIAS,
    FreezeError,
    FreezePin,
    freeze,
    read_freeze_pin,
    resolve_frozen,
    write_freeze_pin,
)
from flag_football_ep.cv.registry import CHAMPION_ALIAS, RFDETRWrapper
from flag_football_ep.model import mlflow_store
from flag_football_ep.model.registry import RegistryError

REPO_ROOT = Path(__file__).resolve().parent.parent
BUNDLE_PATH = REPO_ROOT / "src" / "flag_football_ep" / "cv" / "bundle.py"
FREEZE_PIN_PATH = REPO_ROOT / "data" / "reference" / "hackathon_freeze.json"

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
        homography_calibration=tmp_path
        / "data"
        / "reference"
        / "homography_calibration.csv",
        gt_positions=tmp_path / "data" / "reference" / "gt_positions.csv",
        continuity_review=tmp_path / "data" / "reference" / "continuity_review.csv",
    )
    sources = Sources(
        sportapp=SportappSource(
            base_url="https://example.invalid/api/v1/public",
            api_key_env="SPORTAPP_API_KEY",
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
        paths=paths,
        reference=reference,
        sources=sources,
        train=train,
        report=report,
        cv=cv,
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


# --- freeze / resolve_frozen: distinct from champion --------------------------------------


def test_resolve_frozen_returns_frozen_run_id_after_freeze(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    run_id, version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )

    returned_version = freeze("cv_detector_model_test", run_id, config)

    assert returned_version == version
    assert resolve_frozen("cv_detector_model_test", config) == run_id


def test_resolve_frozen_unaffected_by_later_promotion(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    first_run_id, _first_version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )
    second_run_id, _second_version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )

    freeze("cv_detector_model_test", first_run_id, config)
    registry.promote("cv_detector_model_test", second_run_id, config)

    assert resolve_frozen("cv_detector_model_test", config) == first_run_id
    assert registry.resolve_champion("cv_detector_model_test", config) == second_run_id
    assert resolve_frozen(
        "cv_detector_model_test", config
    ) != registry.resolve_champion("cv_detector_model_test", config)


def test_freeze_unknown_run_id_raises_freeze_error_naming_run_id(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    mlflow_store.ensure_experiment("cv_detector_test", config)
    with mlflow.start_run() as run:
        pass  # a run with no model version registered under "cv_detector_model_test"

    with pytest.raises(FreezeError) as exc_info:
        freeze("cv_detector_model_test", run.info.run_id, config)

    message = str(exc_info.value)
    assert run.info.run_id in message
    assert "cv_detector_model_test" in message
    # FreezeError is a RegistryError -- never a bare MlflowException escaping.
    assert isinstance(exc_info.value, RegistryError)


def test_freeze_rejects_non_hex_run_id(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    _register_trivial_version(config, "cv_detector_model_test", checkpoint)

    with pytest.raises(ValueError):
        freeze("cv_detector_model_test", "../../etc", config)


def test_resolve_frozen_no_alias_raises_freeze_error(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )  # never frozen

    with pytest.raises(FreezeError) as exc_info:
        resolve_frozen("cv_detector_model_test", config)

    assert "cv_detector_model_test" in str(exc_info.value)


# --- write_freeze_pin / read_freeze_pin round trip -----------------------------------------


def test_write_and_read_freeze_pin_round_trip(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    run_id, version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )
    freeze("cv_detector_model_test", run_id, config)

    pin_path = tmp_path / "pins" / "hackathon_freeze.json"
    written_path = write_freeze_pin(config, run_id, "deadbeef" * 8, pin_path)

    assert written_path == pin_path
    assert pin_path.exists()

    pin = read_freeze_pin(pin_path)
    assert isinstance(pin, FreezePin)
    assert pin.run_id == run_id
    assert pin.dataset_hash == "deadbeef" * 8
    assert pin.model_version == version
    assert pin.frozen_at  # non-empty timestamp


def test_write_freeze_pin_atomic_no_tmp_file_left_behind(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    run_id, _version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )
    freeze("cv_detector_model_test", run_id, config)

    pin_path = tmp_path / "hackathon_freeze.json"
    write_freeze_pin(config, run_id, "abc123", pin_path)

    assert not pin_path.with_suffix(pin_path.suffix + ".tmp").exists()


def test_write_freeze_pin_existing_different_run_id_raises_naming_both(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    first_run_id, _first_version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )
    freeze("cv_detector_model_test", first_run_id, config)

    pin_path = tmp_path / "hackathon_freeze.json"
    write_freeze_pin(config, first_run_id, "abc123", pin_path)

    second_run_id, _second_version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )

    with pytest.raises(FreezeError) as exc_info:
        write_freeze_pin(config, second_run_id, "def456", pin_path)

    message = str(exc_info.value)
    assert first_run_id in message
    assert second_run_id in message


def test_write_freeze_pin_same_run_id_twice_does_not_raise(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    run_id, _version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )
    freeze("cv_detector_model_test", run_id, config)

    pin_path = tmp_path / "hackathon_freeze.json"
    write_freeze_pin(config, run_id, "abc123", pin_path)
    # Re-running the exact same freeze is idempotent, not a conflict.
    write_freeze_pin(config, run_id, "abc123", pin_path)

    pin = read_freeze_pin(pin_path)
    assert pin.run_id == run_id


def test_read_freeze_pin_missing_file_raises_freeze_error(tmp_path: Path) -> None:
    with pytest.raises(FreezeError):
        read_freeze_pin(tmp_path / "does-not-exist.json")


def test_read_freeze_pin_invalid_json_raises_freeze_error(tmp_path: Path) -> None:
    path = tmp_path / "hackathon_freeze.json"
    path.write_text("{not valid json", encoding="utf-8")

    with pytest.raises(FreezeError):
        read_freeze_pin(path)


def test_read_freeze_pin_missing_dataset_hash_raises_named_error_not_key_error(
    tmp_path: Path,
) -> None:
    import json

    path = tmp_path / "hackathon_freeze.json"
    path.write_text(
        json.dumps(
            {
                "run_id": "abc123",
                "frozen_at": "2026-01-01T00:00:00Z",
                "model_version": "1",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(FreezeError) as exc_info:
        read_freeze_pin(path)

    assert not isinstance(exc_info.value, KeyError)
    assert "dataset_hash" in str(exc_info.value)


# --- store isolation: write_freeze_pin/resolve_frozen reconfigure the tracking uri ----------


def test_resolve_frozen_ignores_a_differently_set_ambient_tracking_uri(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    checkpoint = tmp_path / "checkpoint.pth"
    run_id, _version = _register_trivial_version(
        config, "cv_detector_model_test", checkpoint
    )
    freeze("cv_detector_model_test", run_id, config)

    mlflow.set_tracking_uri(
        "sqlite:///" + str(tmp_path / "somewhere-else" / "mlflow.db")
    )

    assert resolve_frozen("cv_detector_model_test", config) == run_id
    assert mlflow.get_tracking_uri() == mlflow_store.tracking_uri(config)


# --- anti-drift guards (T-2.2-19): a later edit cannot silently move the bundle -------------
# builder from the frozen detector back onto the rolling `champion` alias.


def test_bundle_module_never_references_resolve_champion() -> None:
    """RESEARCH Pitfall 5 (02.2-RESEARCH.md): `cv/bundle.py` must resolve the frozen
    detector via `read_freeze_pin`/`resolve_frozen`, never `cv.registry.resolve_champion`
    directly -- a `resolve_champion` call here would silently drift the hackathon
    deliverable onto whatever active-learning retraining most recently promoted.

    Comment lines and docstring blocks are stripped before counting, so this module's
    own design-intent documentation (which names `resolve_champion` explaining why it
    must never be called) cannot trip its own guard.
    """
    offenders: list[str] = []
    in_docstring = False
    for lineno, raw_line in enumerate(
        BUNDLE_PATH.read_text(encoding="utf-8").splitlines(), start=1
    ):
        stripped = raw_line.strip()
        if stripped.startswith('"""') or stripped.startswith("'''"):
            quote = stripped[:3]
            if stripped.count(quote) % 2 == 1:
                in_docstring = not in_docstring
            continue
        if in_docstring or stripped.startswith("#"):
            continue
        code_part = raw_line.split("#", 1)[0]
        if "resolve_champion" in code_part:
            offenders.append(f"{BUNDLE_PATH.name}:{lineno}: {raw_line.strip()}")

    assert not offenders, (
        "cv/bundle.py references resolve_champion -- forbidden (RESEARCH Pitfall 5, "
        "T-2.2-19): the bundle builder must resolve the frozen detector via "
        f"read_freeze_pin/resolve_frozen, never the rolling champion alias: {offenders}"
    )


def test_tracked_pin_file_run_id_matches_resolve_frozen_when_store_available() -> None:
    """The tracked pin (`data/reference/hackathon_freeze.json`) must parse and its
    `run_id` must match what `resolve_frozen` resolves against the real MLflow store
    -- skipped cleanly (not failed) when that store does not have the alias set
    (e.g. a fresh worktree checkout without the persistent `mlruns/` store, T-2.2-19).
    """
    if not FREEZE_PIN_PATH.exists():
        pytest.skip(f"tracked pin file not present: {FREEZE_PIN_PATH}")

    pin = read_freeze_pin(FREEZE_PIN_PATH)

    config = load_config(REPO_ROOT / "ffep.toml")
    name = registry.detector_model_name(config)
    try:
        resolved_run_id = resolve_frozen(name, config)
    except FreezeError as exc:
        pytest.skip(f"no MLflow store with {FROZEN_ALIAS!r} alias available: {exc}")

    assert resolved_run_id == pin.run_id


def test_frozen_alias_is_distinct_from_champion_alias() -> None:
    assert FROZEN_ALIAS != CHAMPION_ALIAS
    assert FROZEN_ALIAS == "hackathon-frozen"
