"""Coverage for `flag_football_ep.cv.detect.train_detector`: the three-mode training
entry point (train+register, remote train-only, registration-only), offline, against a
`tmp_path` MLflow store, with a monkeypatched fake `rfdetr.RFDETRSmall` -- no real
weights download, no GPU, no network.

Dataset structural validation (`cv.dataset.validate_coco`) is monkeypatched to a stub
`DatasetStats` for every test except the "invalid dataset aborts" test, which uses the
real function against a deliberately under-floor (`< 250` image) dataset -- exercising
the real `DatasetError` this module must propagate before ever importing `rfdetr`.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
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
from flag_football_ep.cv import detect, registry
from flag_football_ep.cv.dataset import DatasetError, DatasetStats
from flag_football_ep.cv.frames import FrameSample, FrameSampleManifest, write_manifest
from flag_football_ep.cv.registry import RegistryError, RFDETRWrapper, resolve_champion
from flag_football_ep.model import mlflow_store

# --- shared test config helper (mirrors tests/test_cv_registry.py::_make_config) -----------


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
    )
    return Config(
        paths=paths, reference=reference, sources=sources, train=train, report=report, cv=cv
    )


# --- synthetic dataset + manifest fixture ---------------------------------------------------

_FRAME_SPECS: list[tuple[int, str, str]] = [
    (1, "train", "clip001_f00010.jpg"),
    (1, "train", "clip001_f00020.jpg"),
    (2, "val", "clip002_f00010.jpg"),
    (2, "val", "clip002_f00020.jpg"),
]


def _build_dataset(tmp_path: Path) -> tuple[Path, FrameSampleManifest]:
    """A tiny (4-image) synthetic COCO export + its manifest: clip 1 -> train, clip 2 ->
    val. Image "bytes" are arbitrary (never decoded by `_prepare_dataset_layout`, which
    only copies file bytes and reads the COCO JSON).
    """
    coco_dir = tmp_path / "coco"
    coco_dir.mkdir()

    images = []
    annotations = []
    for idx, (_clip_number, _split, file_name) in enumerate(_FRAME_SPECS, start=1):
        (coco_dir / file_name).write_bytes(f"fake-image-bytes-{file_name}".encode())
        images.append({"id": idx, "file_name": file_name, "width": 100, "height": 100})
        annotations.append(
            {"id": idx, "image_id": idx, "category_id": 1, "bbox": [10.0, 10.0, 20.0, 20.0]}
        )
    categories = [{"id": 1, "name": "player"}, {"id": 2, "name": "referee"}]
    (coco_dir / "instances.json").write_text(
        json.dumps({"images": images, "annotations": annotations, "categories": categories})
    )

    frames = [
        FrameSample(
            clip_number=clip_number,
            clip_path=f"data/video/session/clip{clip_number:03d}.mp4",
            frame_index=idx * 10,
            timestamp_s=float(idx),
            image_path=file_name,
            split=split,
        )
        for idx, (clip_number, split, file_name) in enumerate(_FRAME_SPECS, start=1)
    ]
    manifest = FrameSampleManifest(
        session_id="test-session", seed=1, target=len(frames), frames=frames,
        split={1: "train", 2: "val"},
    )
    return coco_dir, manifest


_STUB_DATASET_STATS = DatasetStats(
    n_images=len(_FRAME_SPECS),
    n_boxes={"player": len(_FRAME_SPECS), "referee": 0, "_empty_images": 0},
    split_counts={"train": 2, "val": 2},
    content_sha256="deadbeef" * 8,
)


@pytest.fixture
def _stub_validate_coco(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bypass `validate_coco`'s real `[250, 600]`-image floor/ceiling for every test that
    is not itself testing the abort-on-invalid-dataset behaviour -- the layout/training
    logic under test here does not depend on real-scale label counts.
    """
    monkeypatch.setattr(
        "flag_football_ep.cv.dataset.validate_coco",
        lambda _coco_dir, _manifest: _STUB_DATASET_STATS,
    )


@pytest.fixture(autouse=True)
def _stub_load_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every test monkeypatches `RFDETRWrapper.load_context` so `register_detector_model`
    never imports real `rfdetr` or downloads weights during MLflow's pyfunc log/validate
    step (mirrors `tests/test_cv_registry.py`).
    """

    class _StubDetector:
        def predict(self, model_input):
            return {"boxes": [], "input": model_input}

    def _load_context(self, _context) -> None:
        self.model = _StubDetector()

    monkeypatch.setattr(RFDETRWrapper, "load_context", _load_context)


# --- fake rfdetr.RFDETRSmall trainer --------------------------------------------------------


@dataclass
class _FakeTrainerState:
    init_calls: list[dict] = field(default_factory=list)
    train_calls: list[dict] = field(default_factory=list)
    evaluate_calls: list[dict] = field(default_factory=list)


def _make_fake_rfdetr_small(state: _FakeTrainerState, eval_metrics: dict[str, float]):
    """A fake `RFDETRSmall`: `train()` writes a dummy `checkpoint_best_total.pth` into
    `output_dir` (mirroring the real trainer's `on_fit_end` behaviour), `evaluate()`
    returns `eval_metrics` unchanged -- no real training, no GPU, no network.
    """

    class _FakeRFDETRSmall:
        def __init__(self, **kwargs):
            state.init_calls.append(kwargs)

        def train(self, **kwargs):
            state.train_calls.append(kwargs)
            output_dir = Path(kwargs["output_dir"])
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "checkpoint_best_total.pth").write_bytes(b"fake-checkpoint")

        def evaluate(self, **kwargs):
            state.evaluate_calls.append(kwargs)
            return dict(eval_metrics)

    return _FakeRFDETRSmall


_EVAL_METRICS = {
    "val/mAP_50_95": 0.42,
    "val/mAP_50": 0.65,
    "val/mAP_75": 0.40,
    "val/mAR": 0.55,
    "val/AP/player": 0.60,
    "val/AP/referee": 0.20,
}


# --- _prepare_dataset_layout -----------------------------------------------------------------


def test_prepare_dataset_layout_splits_train_and_valid_with_no_overlap(
    tmp_path: Path, _stub_validate_coco: None
) -> None:
    coco_dir, manifest = _build_dataset(tmp_path)

    dataset_dir, content_sha256 = detect._prepare_dataset_layout(
        coco_dir, manifest, tmp_path / "out"
    )

    assert content_sha256 == _STUB_DATASET_STATS.content_sha256
    train_files = {p.name for p in (dataset_dir / "train").glob("*.jpg")}
    valid_files = {p.name for p in (dataset_dir / "valid").glob("*.jpg")}

    assert train_files == {"clip001_f00010.jpg", "clip001_f00020.jpg"}
    assert valid_files == {"clip002_f00010.jpg", "clip002_f00020.jpg"}
    assert train_files.isdisjoint(valid_files)

    train_ann = json.loads((dataset_dir / "train" / "_annotations.coco.json").read_text())
    valid_ann = json.loads((dataset_dir / "valid" / "_annotations.coco.json").read_text())
    assert {img["file_name"] for img in train_ann["images"]} == train_files
    assert {img["file_name"] for img in valid_ann["images"]} == valid_files


def test_prepare_dataset_layout_finds_images_nested_under_images_default(
    tmp_path: Path, _stub_validate_coco: None
) -> None:
    """A real CVAT COCO export (`dataset.export_cvat_task`) nests image files under
    `images/default/`, not directly beside `instances.json` -- `_prepare_dataset_layout`
    must still locate and copy them by `file_name` alone (`_index_images_by_name`).
    """
    coco_dir, manifest = _build_dataset(tmp_path)
    nested_dir = coco_dir / "images" / "default"
    nested_dir.mkdir(parents=True)
    for _clip_number, _split, file_name in _FRAME_SPECS:
        (coco_dir / file_name).rename(nested_dir / file_name)

    dataset_dir, _content_sha256 = detect._prepare_dataset_layout(
        coco_dir, manifest, tmp_path / "out"
    )

    train_files = {p.name for p in (dataset_dir / "train").glob("*.jpg")}
    valid_files = {p.name for p in (dataset_dir / "valid").glob("*.jpg")}
    assert train_files == {"clip001_f00010.jpg", "clip001_f00020.jpg"}
    assert valid_files == {"clip002_f00010.jpg", "clip002_f00020.jpg"}


def test_prepare_dataset_layout_tolerates_a_manifest_that_oversamples_the_dataset(
    tmp_path: Path, _stub_validate_coco: None
) -> None:
    """A corrected COCO export can legitimately cover fewer frames than the full
    sampling manifest (plan 02.1-09's documented, sanctioned early-stop labelling
    session) -- `_prepare_dataset_layout` must filter the manifest down to what the
    export actually contains rather than treating the extra manifest frames as
    "missing".
    """
    coco_dir, manifest = _build_dataset(tmp_path)
    # Drop clip 2 (both its frames) from the COCO export -- the manifest still
    # references them, mirroring the real 404-sampled/304-corrected split.
    dropped = {"clip002_f00010.jpg", "clip002_f00020.jpg"}
    for name in dropped:
        (coco_dir / name).unlink()
    data = json.loads((coco_dir / "instances.json").read_text())
    data["images"] = [img for img in data["images"] if img["file_name"] not in dropped]
    data["annotations"] = [
        ann for ann in data["annotations"] if ann["image_id"] in {i["id"] for i in data["images"]}
    ]
    (coco_dir / "instances.json").write_text(json.dumps(data))

    dataset_dir, _content_sha256 = detect._prepare_dataset_layout(
        coco_dir, manifest, tmp_path / "out"
    )

    train_files = {p.name for p in (dataset_dir / "train").glob("*.jpg")}
    valid_files = {p.name for p in (dataset_dir / "valid").glob("*.jpg")}
    assert train_files == {"clip001_f00010.jpg", "clip001_f00020.jpg"}
    assert valid_files == set()  # clip 2 (the only val clip) was entirely dropped


def test_resume_is_forwarded_to_the_trainer_as_a_bare_string(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _stub_validate_coco: None
) -> None:
    config = _make_config(tmp_path)
    coco_dir, manifest = _build_dataset(tmp_path)
    write_manifest(manifest, config.paths.labels / "frames" / "manifest.json")

    state = _FakeTrainerState()
    monkeypatch.setattr("rfdetr.RFDETRSmall", _make_fake_rfdetr_small(state, _EVAL_METRICS))

    resume_ckpt = tmp_path / "out" / "last.ckpt"
    detect.train_detector(
        config, coco_dir, register=False, output_dir=tmp_path / "out", resume=resume_ckpt
    )

    assert state.train_calls[0]["resume"] == str(resume_ckpt)


# --- abort-before-trainer --------------------------------------------------------------------


def test_invalid_dataset_aborts_before_trainer_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    coco_dir, manifest = _build_dataset(tmp_path)  # only 4 images, below the 250-image floor
    write_manifest(manifest, config.paths.labels / "frames" / "manifest.json")

    class _ExplodingRFDETRSmall:
        def __init__(self, **_kwargs):
            raise AssertionError("trainer must not be constructed for an invalid dataset")

    monkeypatch.setattr("rfdetr.RFDETRSmall", _ExplodingRFDETRSmall)

    with pytest.raises(DatasetError):
        detect.train_detector(config, coco_dir, register=False, output_dir=tmp_path / "out")


# --- resolution validation ---------------------------------------------------------------


def test_invalid_resolution_raises_named_exception_with_value(tmp_path: Path) -> None:
    config = _make_config(tmp_path)

    with pytest.raises(detect.InvalidResolution, match="900"):
        detect.train_detector(
            config, tmp_path / "unused-dataset", resolution=900, register=False
        )


# --- register=True ---------------------------------------------------------------------------


def test_register_true_creates_one_run_with_dataset_hash_and_per_class_ap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _stub_validate_coco: None
) -> None:
    config = _make_config(tmp_path)
    coco_dir, manifest = _build_dataset(tmp_path)
    write_manifest(manifest, config.paths.labels / "frames" / "manifest.json")

    state = _FakeTrainerState()
    monkeypatch.setattr("rfdetr.RFDETRSmall", _make_fake_rfdetr_small(state, _EVAL_METRICS))

    result = detect.train_detector(config, coco_dir, register=True, output_dir=tmp_path / "out")

    assert len(state.train_calls) == 1
    assert result.run_id
    assert result.checkpoint.exists()
    assert "AP_player" in result.metrics
    assert "AP_referee" in result.metrics

    mlflow_store.configure(config)
    runs = mlflow.search_runs(
        experiment_names=[config.cv.detector_experiment], output_format="list"
    )
    assert len(runs) == 1
    assert runs[0].data.params.get("dataset_content_sha256") == _STUB_DATASET_STATS.content_sha256

    from mlflow import MlflowClient

    versions = MlflowClient().search_model_versions(
        f"name='{registry.detector_model_name(config)}'"
    )
    assert len(versions) == 1


def test_register_true_does_not_set_champion_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _stub_validate_coco: None
) -> None:
    config = _make_config(tmp_path)
    coco_dir, manifest = _build_dataset(tmp_path)
    write_manifest(manifest, config.paths.labels / "frames" / "manifest.json")

    state = _FakeTrainerState()
    monkeypatch.setattr("rfdetr.RFDETRSmall", _make_fake_rfdetr_small(state, _EVAL_METRICS))

    detect.train_detector(config, coco_dir, register=True, output_dir=tmp_path / "out")

    with pytest.raises(RegistryError):
        resolve_champion(registry.detector_model_name(config), config)


# --- register=False (remote-training mode) ----------------------------------------------


def test_register_false_writes_artifacts_without_mlflow_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _stub_validate_coco: None
) -> None:
    config = _make_config(tmp_path)
    coco_dir, manifest = _build_dataset(tmp_path)
    write_manifest(manifest, config.paths.labels / "frames" / "manifest.json")

    state = _FakeTrainerState()
    monkeypatch.setattr("rfdetr.RFDETRSmall", _make_fake_rfdetr_small(state, _EVAL_METRICS))

    out_dir = tmp_path / "out"
    result = detect.train_detector(config, coco_dir, register=False, output_dir=out_dir)

    assert result.run_id == ""
    assert (out_dir / "checkpoint_best_total.pth").exists()
    assert (out_dir / "metrics.json").exists()
    assert (out_dir / "params.json").exists()
    # The tmp MLflow store was never touched -- no db file created.
    assert not (config.paths.mlruns / "mlflow.db").exists()


# --- from_artifacts (registration-only mode) ----------------------------------------------


def test_from_artifacts_registers_without_calling_trainer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    (artifacts_dir / "checkpoint_best_total.pth").write_bytes(b"fake-checkpoint")
    (artifacts_dir / "metrics.json").write_text(
        json.dumps({"mAP_50_95": 0.5, "AP_player": 0.6, "AP_referee": 0.1})
    )
    (artifacts_dir / "params.json").write_text(
        json.dumps({"dataset_content_sha256": "abc123", "resolution": 672})
    )

    state = _FakeTrainerState()
    monkeypatch.setattr("rfdetr.RFDETRSmall", _make_fake_rfdetr_small(state, {}))

    result = detect.train_detector(
        config, tmp_path / "unused-dataset", from_artifacts=artifacts_dir
    )

    assert result.run_id
    assert result.checkpoint == artifacts_dir / "checkpoint_best_total.pth"
    assert result.metrics["AP_player"] == 0.6
    assert len(state.init_calls) == 0
    assert len(state.train_calls) == 0


def test_from_artifacts_missing_checkpoint_raises_named_error(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    (artifacts_dir / "metrics.json").write_text("{}")
    (artifacts_dir / "params.json").write_text("{}")

    with pytest.raises(detect.WeightsNotFound, match="checkpoint_best_total.pth"):
        detect.train_detector(config, tmp_path / "unused-dataset", from_artifacts=artifacts_dir)


# --- CLI: --dataset is required only when --from-artifacts is absent -----------------------


def test_train_cli_requires_dataset_unless_from_artifacts_given() -> None:
    """`ffep cv train --from-artifacts <dir>` (Task 3's own registration-only command,
    matching the real primary-machine round trip) must not demand `--dataset` -- it is
    unused in that mode. Omitting both is still a clean, named usage error.
    """
    from typer.testing import CliRunner

    from flag_football_ep.cli import app

    runner = CliRunner()
    # --config satisfies the suite-hygiene guard (T-1.2-32); the BadParameter for the
    # missing --dataset/--from-artifacts fires before the config file is ever loaded.
    result = runner.invoke(app, ["cv", "train", "--config", "does-not-exist.toml"])

    assert result.exit_code != 0
    # Rich's error panel wraps flag names in ANSI colour codes that split literal
    # substrings like `--dataset`/`--from-artifacts` across escape sequences --
    # strip them before asserting on the message text.
    plain_output = re.sub(r"\x1b\[[0-9;]*m", "", result.output)
    assert "--dataset" in plain_output
    assert "--from-artifacts" in plain_output
    assert "required unless" in plain_output
