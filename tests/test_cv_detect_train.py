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

import cv2
import mlflow
import numpy as np
import pytest
import supervision as sv

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
        dvc_remote_name="otc-obs",
        dvc_remote_url="s3://test-bucket/flag-football-datasets",
        dvc_remote_endpoint="https://obs.eu-de.otc.t-systems.com",
        otc_obs_access_key_env="OTC_OBS_ACCESS_KEY_ID",
        otc_obs_secret_key_env="OTC_OBS_SECRET_ACCESS_KEY",
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
        lambda _coco_dir, _manifest, **_kwargs: _STUB_DATASET_STATS,
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


def _make_fake_rfdetr_small_no_val_split(
    state: _FakeTrainerState, eval_metrics: dict[str, float]
):
    """A fake `RFDETRSmall` mirroring the real `BestModelCallback.on_fit_end` behaviour
    on a dataset with zero validation frames (Phase 2.2's AL-iteration convention):
    `train()` writes only `checkpoint_best_ema.pth`, never `checkpoint_best_total.pth`
    -- verified against a real zero-val-split run (rfdetr==1.9.3, Phase 2.2 AL
    iteration 1, 2026-09-04): `on_fit_end`'s `checkpoint_best_total.pth` copy only
    fires when a validation epoch actually improved the monitored metric, which never
    happens with an empty validation dataloader.
    """

    class _FakeRFDETRSmallNoValSplit:
        def __init__(self, **kwargs):
            state.init_calls.append(kwargs)

        def train(self, **kwargs):
            state.train_calls.append(kwargs)
            output_dir = Path(kwargs["output_dir"])
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "checkpoint_best_ema.pth").write_bytes(b"fake-ema-checkpoint")

        def evaluate(self, **kwargs):
            state.evaluate_calls.append(kwargs)
            return dict(eval_metrics)

    return _FakeRFDETRSmallNoValSplit


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


def test_missing_val_split_falls_back_to_ema_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _stub_validate_coco: None
) -> None:
    """Phase 2.2's AL-iteration datasets carry no `val`-split frames at all (every
    merged frame is `split: "train"` -- evaluation runs separately against the
    frozen eval-clip split, `detect.evaluate_per_domain`). The real
    `BestModelCallback.on_fit_end` never writes `checkpoint_best_total.pth` in that
    case (no validation epoch ever improves), only backfills
    `checkpoint_best_ema.pth` -- `train_detector` must fall back to it rather than
    raising `WeightsNotFound`, and record the fallback in `params["checkpoint_source"]`
    for provenance.
    """
    config = _make_config(tmp_path)
    coco_dir, manifest = _build_dataset(tmp_path)
    write_manifest(manifest, config.paths.labels / "frames" / "manifest.json")

    state = _FakeTrainerState()
    monkeypatch.setattr(
        "rfdetr.RFDETRSmall", _make_fake_rfdetr_small_no_val_split(state, _EVAL_METRICS)
    )

    result = detect.train_detector(config, coco_dir, register=True, output_dir=tmp_path / "out")

    assert result.checkpoint.name == "checkpoint_best_ema.pth"
    assert result.checkpoint.exists()

    mlflow_store.configure(config)
    runs = mlflow.search_runs(
        experiment_names=[config.cv.detector_experiment], output_format="list"
    )
    assert runs[0].data.params.get("checkpoint_source") == "best_ema_fallback_no_val_split"


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


# --- evaluate_per_domain (plan 02.2-15) -----------------------------------------------------
#
# Ground truth is sourced from data/labels/<session_id>/corrected/instances.json,
# filtered to images whose parsed clip number is one of that domain's frozen_eval
# clips (see detect.py::_load_domain_ground_truth's docstring) -- mirroring the real
# Phase-2.1 pilot corrected-dataset convention these tests build synthetically.


def _write_frozen_eval_csv(path: Path, rows: list[dict]) -> None:
    columns = [
        "domain", "session_id", "clip_number", "stratum_id", "role",
        "private_test", "frozen_at", "seed",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(columns)]
    for row in rows:
        lines.append(",".join(str(row[c]) for c in columns))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_corrected_coco(
    gt_dir: Path, frames: list[tuple[int, int, list[tuple[int, list[float]]]]]
) -> None:
    """`frames`: list of `(clip_number, frame_index, boxes)`, `boxes`:
    `[(category_id, [x, y, w, h]), ...]`. Writes a tiny real JPEG per frame (decoded
    by `_evaluate_domain_frames` via `cv2.imread`) plus `instances.json` under
    `gt_dir/images/default/` + `gt_dir/instances.json`, matching the real corrected/
    CVAT-export layout `_index_images_by_name` already tolerates.
    """
    images_dir = gt_dir / "images" / "default"
    images_dir.mkdir(parents=True, exist_ok=True)

    images = []
    annotations = []
    ann_id = 1
    for image_id, (clip_number, frame_index, boxes) in enumerate(frames, start=1):
        file_name = f"Wide - Clip {clip_number:03d}_f{frame_index:05d}.jpg"
        frame = np.zeros((64, 64, 3), dtype=np.uint8)
        cv2.imwrite(str(images_dir / file_name), frame)
        images.append(
            {"id": image_id, "file_name": file_name, "width": 64, "height": 64}
        )
        for category_id, bbox in boxes:
            annotations.append(
                {
                    "id": ann_id,
                    "image_id": image_id,
                    "category_id": category_id,
                    "bbox": bbox,
                }
            )
            ann_id += 1

    categories = [{"id": 1, "name": "player"}, {"id": 2, "name": "referee"}]
    (gt_dir / "instances.json").write_text(
        json.dumps({"images": images, "annotations": annotations, "categories": categories})
    )


class _FakeEvalModel:
    """A fake detector for `evaluate_per_domain`: `.predict(image, params=None)`
    mirrors the loaded pyfunc model's contract (`cv.detect._call_model`).
    `boxes_by_call[i]` is returned on the i-th call, in call order -- lets a test
    control exactly which prediction each ground-truth frame receives.
    """

    def __init__(self, boxes_by_call: list[sv.Detections]) -> None:
        self._boxes_by_call = boxes_by_call
        self.calls: list[dict] = []

    def predict(self, image, params=None) -> sv.Detections:
        call_index = len(self.calls)
        self.calls.append({"image": image, "params": params})
        return self._boxes_by_call[call_index]


def _detections(xyxy, confidence, class_id) -> sv.Detections:
    return sv.Detections(
        xyxy=np.array(xyxy, dtype=np.float64).reshape(-1, 4),
        confidence=np.array(confidence, dtype=np.float64),
        class_id=np.array(class_id, dtype=np.int64),
    )


def test_evaluate_per_domain_returns_per_domain_metrics_with_n_images_and_n_boxes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)

    split_path = tmp_path / "data" / "reference" / "frozen_eval_clips.csv"
    _write_frozen_eval_csv(
        split_path,
        [
            {
                "domain": "drone", "session_id": "sess-drone", "clip_number": 5,
                "stratum_id": "hp-01", "role": "frozen_eval", "private_test": "true",
                "frozen_at": "2026-09-01T00:00:00Z", "seed": 1,
            },
            {
                "domain": "drone", "session_id": "sess-drone", "clip_number": 1,
                "stratum_id": "hp-01", "role": "pool", "private_test": "false",
                "frozen_at": "2026-09-01T00:00:00Z", "seed": 1,
            },
        ],
    )

    gt_dir = config.paths.labels / "sess-drone" / "corrected"
    _write_corrected_coco(
        gt_dir,
        [
            # clip 5 is frozen_eval -> included; clip 1 is pool -> excluded even
            # though a corrected frame exists for it.
            (5, 0, [(1, [10.0, 10.0, 20.0, 20.0])]),
            (1, 0, [(1, [10.0, 10.0, 20.0, 20.0])]),
        ],
    )

    model = _FakeEvalModel([_detections([[10.0, 10.0, 30.0, 30.0]], [0.9], [0])])
    monkeypatch.setattr(detect, "load_detector", lambda _config, _run_id: model)

    mlflow_store.configure(config)
    mlflow_store.ensure_experiment(config.cv.detector_experiment, config)
    with mlflow.start_run() as run:
        run_id = run.info.run_id

    out_path = tmp_path / "eval_report.json"
    results = detect.evaluate_per_domain(config, run_id, split_path, out_path)

    assert "drone" in results
    assert results["drone"]["n_images"] == 1
    assert results["drone"]["n_boxes"] == 1
    assert len(model.calls) == 1  # only the frozen_eval frame was inferred over
    assert "AP_player" in results["drone"]
    assert "AP_referee" in results["drone"]

    assert out_path.exists()
    written = json.loads(out_path.read_text())
    assert written["drone"]["n_images"] == 1


def test_evaluate_per_domain_never_reports_pooled_metric_instead_of_per_domain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pooled aggregate may be present as an *additional* key, but every domain
    named in the split must still carry its own entry (C-05/D-04) -- a caller
    reading only `results["_pooled"]` would silently hide a domain collapsing.
    """
    config = _make_config(tmp_path)

    split_path = tmp_path / "data" / "reference" / "frozen_eval_clips.csv"
    _write_frozen_eval_csv(
        split_path,
        [
            {
                "domain": "drone", "session_id": "sess-drone", "clip_number": 5,
                "stratum_id": "hp-01", "role": "frozen_eval", "private_test": "true",
                "frozen_at": "2026-09-01T00:00:00Z", "seed": 1,
            },
        ],
    )
    _write_corrected_coco(
        config.paths.labels / "sess-drone" / "corrected",
        [(5, 0, [(1, [10.0, 10.0, 20.0, 20.0])])],
    )

    model = _FakeEvalModel([_detections([[10.0, 10.0, 30.0, 30.0]], [0.9], [0])])
    monkeypatch.setattr(detect, "load_detector", lambda _config, _run_id: model)

    mlflow_store.configure(config)
    mlflow_store.ensure_experiment(config.cv.detector_experiment, config)
    with mlflow.start_run() as run:
        run_id = run.info.run_id

    results = detect.evaluate_per_domain(config, run_id, split_path, tmp_path / "out.json")

    assert "drone" in results
    assert "_pooled" in results
    assert set(results) != {"_pooled"}


def test_evaluate_per_domain_raises_named_error_for_domain_with_no_ground_truth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`sideline`'s frozen_eval clips exist in the split, but no corrected/ package
    has ever been written for its session -- must raise naming `sideline`, never
    report a metric over an empty ground-truth set.
    """
    config = _make_config(tmp_path)

    split_path = tmp_path / "data" / "reference" / "frozen_eval_clips.csv"
    _write_frozen_eval_csv(
        split_path,
        [
            {
                "domain": "drone", "session_id": "sess-drone", "clip_number": 5,
                "stratum_id": "hp-01", "role": "frozen_eval", "private_test": "true",
                "frozen_at": "2026-09-01T00:00:00Z", "seed": 1,
            },
            {
                "domain": "sideline", "session_id": "sess-sideline", "clip_number": 8,
                "stratum_id": "hp-01", "role": "frozen_eval", "private_test": "false",
                "frozen_at": "2026-09-01T00:00:00Z", "seed": 1,
            },
        ],
    )
    _write_corrected_coco(
        config.paths.labels / "sess-drone" / "corrected",
        [(5, 0, [(1, [10.0, 10.0, 20.0, 20.0])])],
    )
    # sess-sideline/corrected/ deliberately not written.

    model = _FakeEvalModel([_detections([[10.0, 10.0, 30.0, 30.0]], [0.9], [0])])
    monkeypatch.setattr(detect, "load_detector", lambda _config, _run_id: model)

    mlflow_store.configure(config)
    mlflow_store.ensure_experiment(config.cv.detector_experiment, config)
    with mlflow.start_run() as run:
        run_id = run.info.run_id

    with pytest.raises(detect.EvalGroundTruthMissing, match="sideline"):
        detect.evaluate_per_domain(config, run_id, split_path, tmp_path / "out.json")


def test_evaluate_per_domain_logs_mlflow_metrics_prefixed_per_domain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)

    split_path = tmp_path / "data" / "reference" / "frozen_eval_clips.csv"
    _write_frozen_eval_csv(
        split_path,
        [
            {
                "domain": "drone", "session_id": "sess-drone", "clip_number": 5,
                "stratum_id": "hp-01", "role": "frozen_eval", "private_test": "true",
                "frozen_at": "2026-09-01T00:00:00Z", "seed": 1,
            },
        ],
    )
    _write_corrected_coco(
        config.paths.labels / "sess-drone" / "corrected",
        [(5, 0, [(1, [10.0, 10.0, 20.0, 20.0])])],
    )

    model = _FakeEvalModel([_detections([[10.0, 10.0, 30.0, 30.0]], [0.9], [0])])
    monkeypatch.setattr(detect, "load_detector", lambda _config, _run_id: model)

    mlflow_store.configure(config)
    mlflow_store.ensure_experiment(config.cv.detector_experiment, config)
    with mlflow.start_run() as run:
        run_id = run.info.run_id

    detect.evaluate_per_domain(config, run_id, split_path, tmp_path / "out.json")

    from mlflow import MlflowClient

    logged = MlflowClient().get_run(run_id).data.metrics
    assert "drone_mAP_50" in logged
    assert "drone_mAP_50_95" in logged
    assert "drone_AP_player" in logged
    assert "drone_n_images" in logged


def test_evaluate_per_domain_prefers_eval_gt_directory_over_session_corrected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`data/labels/eval/<domain>/corrected/instances.json` (the dedicated,
    guaranteed-held-out ground-truth convention this ad-hoc plan introduces,
    2026-09-04) takes priority over the legacy `data/labels/<session_id>/corrected/`
    convention -- the two are never merged for the same domain, since the legacy
    convention can predate the eval-clip freeze and overlap a model's own training
    data (exactly the contamination the 2026-09-04 Koordinator-Korrektur in
    docs/dataset-buildout.md documents for the drone domain's pilot-derived GT).
    """
    config = _make_config(tmp_path)

    split_path = tmp_path / "data" / "reference" / "frozen_eval_clips.csv"
    _write_frozen_eval_csv(
        split_path,
        [
            {
                "domain": "drone", "session_id": "sess-drone", "clip_number": 5,
                "stratum_id": "hp-01", "role": "frozen_eval", "private_test": "true",
                "frozen_at": "2026-09-01T00:00:00Z", "seed": 1,
            },
        ],
    )
    # legacy, potentially-contaminated session-scoped GT: 1 frame
    _write_corrected_coco(
        config.paths.labels / "sess-drone" / "corrected",
        [(5, 0, [(1, [1.0, 1.0, 2.0, 2.0])])],
    )
    # new, guaranteed-held-out eval-GT directory: 2 frames -- a distinct count from
    # the legacy source lets this test prove which source was actually read (1 would
    # mean the legacy source leaked in, 3 would mean both got merged).
    _write_corrected_coco(
        config.paths.labels / "eval" / "drone" / "corrected",
        [
            (5, 1, [(1, [10.0, 10.0, 20.0, 20.0])]),
            (5, 2, [(1, [10.0, 10.0, 20.0, 20.0])]),
        ],
    )

    model = _FakeEvalModel(
        [
            _detections([[10.0, 10.0, 30.0, 30.0]], [0.9], [0]),
            _detections([[10.0, 10.0, 30.0, 30.0]], [0.9], [0]),
        ]
    )
    monkeypatch.setattr(detect, "load_detector", lambda _config, _run_id: model)

    mlflow_store.configure(config)
    mlflow_store.ensure_experiment(config.cv.detector_experiment, config)
    with mlflow.start_run() as run:
        run_id = run.info.run_id

    results = detect.evaluate_per_domain(config, run_id, split_path, tmp_path / "out.json")

    assert results["drone"]["n_images"] == 2
    assert len(model.calls) == 2
