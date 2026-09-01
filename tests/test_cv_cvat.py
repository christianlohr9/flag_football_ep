"""Coverage for `flag_football_ep.cv.dataset`'s CVAT round trip: `create_cvat_task` /
`export_cvat_task`.

Every test monkeypatches `dataset._build_client` (the single seam both functions go
through to obtain a `cvat_sdk` client) with an in-process fake -- no real `cvat_sdk`
client is constructed, no socket is opened, no CVAT instance needs to be running. This
mirrors `tests/test_cv_registry.py`'s pattern of monkeypatching the one network seam a
module owns rather than mocking deep into a third-party SDK.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

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
from flag_football_ep.cv import dataset
from flag_football_ep.cv.dataset import CLASS_NAMES, DatasetError, create_cvat_task, export_cvat_task

_TEST_HOST = "http://cvat-test-host.invalid:9000"
_SECRET_PASSWORD = "s3cr3t-do-not-leak-me"

# --- shared test config helper (mirrors tests/test_cv_registry.py) -------------------------


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
        cvat_host=_TEST_HOST,
        cvat_username_env="TEST_CVAT_USERNAME",
        cvat_password_env="TEST_CVAT_PASSWORD",
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


@pytest.fixture(autouse=True)
def _cvat_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test resolves credentials through real env vars -- never a hard-coded value
    read directly by production code."""
    monkeypatch.setenv("TEST_CVAT_USERNAME", "test-user")
    monkeypatch.setenv("TEST_CVAT_PASSWORD", _SECRET_PASSWORD)


def _make_coco_dir(tmp_path: Path, *, with_annotations: bool = True) -> Path:
    coco_dir = tmp_path / "prelabels"
    coco_dir.mkdir(parents=True, exist_ok=True)
    if with_annotations:
        (coco_dir / "instances.json").write_text("{}", encoding="utf-8")
    (coco_dir / "frame_0001.jpg").write_bytes(b"fake-jpeg-bytes")
    (coco_dir / "frame_0002.jpg").write_bytes(b"fake-jpeg-bytes")
    return coco_dir


# --- fakes ------------------------------------------------------------------------------


class _FakeApiError(Exception):
    """Stands in for `cvat_sdk`'s `ApiException`: carries a `.status` and, crucially,
    a message that itself contains the password -- exactly what a real CVAT 401 body
    could echo back. The password must never reach the raised `DatasetError`."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class _FakeTasksRepo:
    def __init__(self, *, created_task=None, retrieve_task=None, create_error=None):
        self.created_task = created_task
        self.retrieve_task = retrieve_task
        self.create_error = create_error
        self.create_calls: list[dict] = []
        self.retrieve_calls: list[int] = []

    def create_from_data(self, *, spec, resources, annotation_path, annotation_format):
        self.create_calls.append(
            {
                "spec": spec,
                "resources": resources,
                "annotation_path": annotation_path,
                "annotation_format": annotation_format,
            }
        )
        if self.create_error is not None:
            raise self.create_error
        return self.created_task

    def retrieve(self, task_id):
        self.retrieve_calls.append(task_id)
        return self.retrieve_task


class _FakeTask:
    def __init__(self, task_id: int, *, export_writer=None):
        self.id = task_id
        self._export_writer = export_writer

    def export_dataset(self, format_name, filename, *, include_images=True):
        if self._export_writer is not None:
            self._export_writer(Path(filename))


class _FakeClient:
    def __init__(self, *, tasks_repo: _FakeTasksRepo | None = None, login_error=None):
        self.tasks = tasks_repo or _FakeTasksRepo()
        self.login_error = login_error
        self.login_calls: list[tuple[str, str]] = []
        self.closed = False

    def login(self, credentials):
        self.login_calls.append(credentials)
        if self.login_error is not None:
            raise self.login_error

    def close(self):
        self.closed = True


def _write_valid_export_zip(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("annotations/instances_default.json", '{"images": [], "annotations": []}')
        archive.writestr("images/frame_0001.jpg", "fake-jpeg-bytes")


def _write_empty_export_zip(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("README.txt", "no annotations in this export")


def _write_malicious_export_zip(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("../../evil.txt", "path traversal payload")


# --- create_cvat_task ---------------------------------------------------------------------


def test_create_cvat_task_sends_labels_in_class_names_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    coco_dir = _make_coco_dir(tmp_path)
    config = _make_config(tmp_path)

    tasks_repo = _FakeTasksRepo(created_task=_FakeTask(42))
    fake_client = _FakeClient(tasks_repo=tasks_repo)
    monkeypatch.setattr(dataset, "_build_client", lambda host: fake_client)

    task_id = create_cvat_task(config, coco_dir, name="pilot-task")

    assert task_id == 42
    assert len(tasks_repo.create_calls) == 1
    sent_labels = tasks_repo.create_calls[0]["spec"]["labels"]
    assert [label["name"] for label in sent_labels] == list(CLASS_NAMES)
    assert fake_client.closed


def test_create_cvat_task_reads_host_from_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    coco_dir = _make_coco_dir(tmp_path)
    config = _make_config(tmp_path)

    captured_hosts: list[str] = []

    def _spy_build_client(host: str):
        captured_hosts.append(host)
        return _FakeClient(tasks_repo=_FakeTasksRepo(created_task=_FakeTask(1)))

    monkeypatch.setattr(dataset, "_build_client", _spy_build_client)

    create_cvat_task(config, coco_dir, name="pilot-task")

    assert captured_hosts == [_TEST_HOST]
    assert "localhost:8080" not in _TEST_HOST  # sanity: the test host is not the real default


def test_create_cvat_task_credential_absent_from_error_on_401(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    coco_dir = _make_coco_dir(tmp_path)
    config = _make_config(tmp_path)

    login_error = _FakeApiError(401, f"Unauthorized: bad credentials for password={_SECRET_PASSWORD}")
    fake_client = _FakeClient(login_error=login_error)
    monkeypatch.setattr(dataset, "_build_client", lambda host: fake_client)

    with pytest.raises(DatasetError) as exc_info:
        create_cvat_task(config, coco_dir, name="pilot-task")

    assert _SECRET_PASSWORD not in str(exc_info.value)
    assert "401" in str(exc_info.value)
    # The exception TYPE name is diagnostic (auth vs DNS vs SDK bug) and carries no
    # credential -- it must survive the sanitization.
    assert "_FakeApiError" in str(exc_info.value)
    assert fake_client.closed


def test_create_cvat_task_names_exception_type_for_a_statusless_connection_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A CVAT container that is simply not running raises a `ConnectionError` with no
    `.status` -- the most common local-Docker failure. The operator must be able to
    tell it apart from a bad password: the error names the exception class instead of
    collapsing everything to 'HTTP unknown' alone.
    """
    coco_dir = _make_coco_dir(tmp_path)
    config = _make_config(tmp_path)

    fake_client = _FakeClient(login_error=ConnectionError("connection refused"))
    monkeypatch.setattr(dataset, "_build_client", lambda host: fake_client)

    with pytest.raises(DatasetError) as exc_info:
        create_cvat_task(config, coco_dir, name="pilot-task")

    message = str(exc_info.value)
    assert "ConnectionError" in message
    assert "HTTP unknown" in message  # no .status on the exception -- honestly unknown
    assert fake_client.closed


def test_export_cvat_task_names_exception_type_and_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out_dir = tmp_path / "export"
    config = _make_config(tmp_path)

    login_error = _FakeApiError(401, f"Unauthorized: password={_SECRET_PASSWORD}")
    fake_client = _FakeClient(login_error=login_error)
    monkeypatch.setattr(dataset, "_build_client", lambda host: fake_client)

    with pytest.raises(DatasetError) as exc_info:
        export_cvat_task(config, 7, out_dir)

    message = str(exc_info.value)
    assert _SECRET_PASSWORD not in message
    assert "_FakeApiError" in message
    assert "401" in message


def test_create_cvat_task_missing_instances_json_raises_named_path(tmp_path: Path) -> None:
    coco_dir = _make_coco_dir(tmp_path, with_annotations=False)
    config = _make_config(tmp_path)

    with pytest.raises(DatasetError) as exc_info:
        create_cvat_task(config, coco_dir, name="pilot-task")

    assert str(coco_dir / "instances.json") in str(exc_info.value)


# --- export_cvat_task ----------------------------------------------------------------------


def test_export_cvat_task_reads_host_from_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out_dir = tmp_path / "export"
    config = _make_config(tmp_path)

    captured_hosts: list[str] = []

    def _spy_build_client(host: str):
        captured_hosts.append(host)
        task = _FakeTask(7, export_writer=_write_valid_export_zip)
        return _FakeClient(tasks_repo=_FakeTasksRepo(retrieve_task=task))

    monkeypatch.setattr(dataset, "_build_client", _spy_build_client)

    export_cvat_task(config, 7, out_dir)

    assert captured_hosts == [_TEST_HOST]


def test_export_cvat_task_round_trips_to_instances_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out_dir = tmp_path / "export"
    config = _make_config(tmp_path)

    task = _FakeTask(7, export_writer=_write_valid_export_zip)
    fake_client = _FakeClient(tasks_repo=_FakeTasksRepo(retrieve_task=task))
    monkeypatch.setattr(dataset, "_build_client", lambda host: fake_client)

    result = export_cvat_task(config, 7, out_dir)

    assert result.name == "instances.json"
    assert result.is_file()
    assert fake_client.closed


def test_export_cvat_task_raises_named_task_id_when_no_instances_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out_dir = tmp_path / "export"
    config = _make_config(tmp_path)

    task = _FakeTask(99, export_writer=_write_empty_export_zip)
    fake_client = _FakeClient(tasks_repo=_FakeTasksRepo(retrieve_task=task))
    monkeypatch.setattr(dataset, "_build_client", lambda host: fake_client)

    with pytest.raises(DatasetError) as exc_info:
        export_cvat_task(config, 99, out_dir)

    assert "99" in str(exc_info.value)


def test_export_cvat_task_rejects_path_traversal_in_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out_dir = tmp_path / "export"
    config = _make_config(tmp_path)

    task = _FakeTask(13, export_writer=_write_malicious_export_zip)
    fake_client = _FakeClient(tasks_repo=_FakeTasksRepo(retrieve_task=task))
    monkeypatch.setattr(dataset, "_build_client", lambda host: fake_client)

    with pytest.raises(DatasetError) as exc_info:
        export_cvat_task(config, 13, out_dir)

    assert "13" in str(exc_info.value)
    # the archive must never be extracted outside out_dir
    assert not (out_dir.parent / "evil.txt").exists()
    assert not (tmp_path.parent / "evil.txt").exists()
