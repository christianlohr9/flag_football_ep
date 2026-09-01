"""Tests for `flag_football_ep.cv.prelabel`: zero-shot pre-labeling with an injected
fake backend (plan 02.1-07 Task 2), and the fine-tuned active-learning backend
(plan 02.2-09 Task 2).

No weights are downloaded and no network is touched -- every test monkeypatches
`prelabel._BACKEND_ORDER`/`prelabel._BACKENDS` with a fake detector before calling
`prelabel_frames`, or (for the `finetuned` backend) monkeypatches
`flag_football_ep.cv.detect.load_detector` with a fake model whose `.predict()`
mirrors the real pyfunc model's contract (`cv.detect._call_model`).
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import supervision as sv
from PIL import Image

from flag_football_ep.config import Config, load_config
from flag_football_ep.cv import detect, prelabel
from flag_football_ep.cv.frames import FrameSample, FrameSampleManifest, write_manifest
from flag_football_ep.cv.prelabel import Detection, PrelabelBackendUnavailable, prelabel_frames
from test_config import MINIMAL_TOML


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(MINIMAL_TOML, encoding="utf-8")
    return load_config(config_path)


def _make_frame_image(path: Path, *, size: tuple[int, int] = (64, 36)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color=(0, 0, 0)).save(path)


def _make_manifest(frames_dir: Path, n_frames: int = 3) -> FrameSampleManifest:
    frames = []
    for i in range(n_frames):
        image_path = frames_dir / f"clip_f{i:05d}.jpg"
        _make_frame_image(image_path)
        frames.append(
            FrameSample(
                clip_number=1,
                clip_path="data/video/sess-1/Wide - Clip 001.mp4",
                frame_index=i,
                timestamp_s=float(i),
                image_path=str(image_path),
                split="train",
            )
        )
    manifest = FrameSampleManifest(
        session_id="sess-1", seed=1, target=n_frames, frames=frames, split={1: "train"}
    )
    write_manifest(manifest, frames_dir / "manifest.json")
    return manifest


def _install_fake_backend(monkeypatch: pytest.MonkeyPatch, *, detections_by_index=None):
    """Replace the real backend resolution with a fake, callable-returning-fixed-boxes
    backend so no weights are downloaded and no network is touched. Returns a dict of
    call counters (`loader`, `detect`) the test can assert against.
    """
    detections_by_index = detections_by_index or {}
    calls = {"loader": 0, "detect": 0}

    def loader(device: str):
        calls["loader"] += 1

        def detect(image_path: Path) -> list[Detection]:
            calls["detect"] += 1
            idx = int(image_path.stem.rsplit("_f", 1)[-1])
            if idx in detections_by_index:
                return detections_by_index[idx]
            return [Detection(category="player", score=0.9, bbox_xyxy=(10.0, 20.0, 30.0, 50.0))]

        return detect

    monkeypatch.setattr(prelabel, "_BACKEND_ORDER", ("fake",))
    monkeypatch.setattr(prelabel, "_BACKENDS", {"fake": loader})
    return calls


def test_prelabel_frames_writes_coco_with_two_categories_in_class_names_order(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames_dir = tmp_path / "frames"
    _make_manifest(frames_dir, n_frames=2)
    _install_fake_backend(monkeypatch)
    out_dir = tmp_path / "prelabels"

    result = prelabel_frames(cfg, frames_dir, out_dir)

    data = json.loads(result.coco_path.read_text(encoding="utf-8"))
    assert [c["name"] for c in data["categories"]] == ["player", "referee"]


def test_prelabel_frames_images_match_manifest_one_to_one_in_order(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames_dir = tmp_path / "frames"
    manifest = _make_manifest(frames_dir, n_frames=3)
    _install_fake_backend(monkeypatch)
    out_dir = tmp_path / "prelabels"

    result = prelabel_frames(cfg, frames_dir, out_dir)

    data = json.loads(result.coco_path.read_text(encoding="utf-8"))
    assert len(data["images"]) == len(manifest.frames)
    expected_names = [Path(f.image_path).name for f in manifest.frames]
    assert [row["file_name"] for row in data["images"]] == expected_names
    for row in data["images"]:
        assert not row["file_name"].startswith("/")


def test_prelabel_frames_bbox_is_xywh_not_xyxy(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames_dir = tmp_path / "frames"
    _make_manifest(frames_dir, n_frames=1)
    _install_fake_backend(
        monkeypatch, detections_by_index={0: [Detection("player", 0.9, (10.0, 20.0, 30.0, 50.0))]}
    )
    out_dir = tmp_path / "prelabels"

    result = prelabel_frames(cfg, frames_dir, out_dir)

    data = json.loads(result.coco_path.read_text(encoding="utf-8"))
    assert data["annotations"][0]["bbox"] == [10.0, 20.0, 20.0, 30.0]


def test_prelabel_frames_zero_detection_frame_stays_in_images_with_no_annotations(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames_dir = tmp_path / "frames"
    _make_manifest(frames_dir, n_frames=2)
    _install_fake_backend(monkeypatch, detections_by_index={0: []})
    out_dir = tmp_path / "prelabels"

    result = prelabel_frames(cfg, frames_dir, out_dir)

    data = json.loads(result.coco_path.read_text(encoding="utf-8"))
    assert len(data["images"]) == 2
    image_ids_with_annotations = {row["image_id"] for row in data["annotations"]}
    assert data["images"][0]["id"] not in image_ids_with_annotations
    assert any("zero detections" in notice for notice in result.notices)


def test_prelabel_frames_skips_backend_on_second_call_without_force(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames_dir = tmp_path / "frames"
    _make_manifest(frames_dir, n_frames=1)
    calls = _install_fake_backend(monkeypatch)
    out_dir = tmp_path / "prelabels"

    prelabel_frames(cfg, frames_dir, out_dir)
    assert calls["loader"] == 1

    result = prelabel_frames(cfg, frames_dir, out_dir)
    assert calls["loader"] == 1
    assert any("skipped" in notice for notice in result.notices)


def test_prelabel_frames_force_reruns_backend(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames_dir = tmp_path / "frames"
    _make_manifest(frames_dir, n_frames=1)
    calls = _install_fake_backend(monkeypatch)
    out_dir = tmp_path / "prelabels"

    prelabel_frames(cfg, frames_dir, out_dir)
    assert calls["loader"] == 1

    prelabel_frames(cfg, frames_dir, out_dir, force=True)
    assert calls["loader"] == 2


def test_prelabel_frames_raises_prelabel_backend_unavailable_naming_fallback(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames_dir = tmp_path / "frames"
    _make_manifest(frames_dir, n_frames=1)

    def failing_loader(device: str):
        raise ImportError("no module named 'fake_missing_pkg'")

    monkeypatch.setattr(prelabel, "_BACKEND_ORDER", ("fake",))
    monkeypatch.setattr(prelabel, "_BACKENDS", {"fake": failing_loader})
    out_dir = tmp_path / "prelabels"

    with pytest.raises(PrelabelBackendUnavailable, match="fallback"):
        prelabel_frames(cfg, frames_dir, out_dir)


# --- finetuned backend (plan 02.2-09 Task 2, RESEARCH Pitfall 1 / T-2.2-26) ----------------


class _FakeDetectorModel:
    """A fake `mlflow.pyfunc`-loaded RF-DETR model: `.predict(image, params=None)`
    mirrors `cv.detect._call_model`'s real contract. `boxes_fn(image)` decides what
    `sv.Detections` each call returns, so a test can vary output per image without a
    real detector.
    """

    def __init__(self, boxes_fn) -> None:
        self._boxes_fn = boxes_fn
        self.calls: list[dict] = []

    def predict(self, image, params=None) -> sv.Detections:
        self.calls.append({"image": image, "params": params})
        return self._boxes_fn(image)


def test_resolve_backend_never_returns_finetuned() -> None:
    """The finetuned backend is registered but never reachable through
    `_resolve_backend`'s ordered fallback chain (plan 02.2-05's frozen contract) --
    a static attribute check, not a live call, since exercising `_resolve_backend`
    for real would attempt to import the real `autodistill`/`transformers` backends.
    """
    assert "finetuned" not in prelabel._BACKEND_ORDER
    assert "finetuned" in prelabel._BACKENDS


def test_finetuned_backend_produces_coco_categories_exactly_player_and_referee(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames_dir = tmp_path / "frames"
    _make_manifest(frames_dir, n_frames=1)

    model = _FakeDetectorModel(
        lambda _image: sv.Detections(
            xyxy=np.array([[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]]),
            confidence=np.array([0.9, 0.8]),
            class_id=np.array([0, 1]),  # 0 -> player, 1 -> referee (dataset.CLASS_NAMES order)
        )
    )

    def fake_load_detector(config, run_id):
        assert run_id is None  # AL always resolves the champion alias, never a pin
        return model

    monkeypatch.setattr(detect, "load_detector", fake_load_detector)
    out_dir = tmp_path / "prelabels"

    result = prelabel_frames(cfg, frames_dir, out_dir, backend="finetuned")

    data = json.loads(result.coco_path.read_text(encoding="utf-8"))
    assert [c["name"] for c in data["categories"]] == ["player", "referee"]
    category_ids = sorted(a["category_id"] for a in data["annotations"])
    assert category_ids == [1, 2]  # player=1, referee=2
    assert model.calls  # the loaded model was actually invoked, not bypassed


def test_finetuned_backend_confidence_filters_like_full_frame_path(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detections below `detect._MODEL_CONFIDENCE_THRESHOLD` are dropped -- the
    finetuned backend reuses `detect._detect_full_frame`'s own confidence filter
    rather than re-implementing (or omitting) it.
    """
    frames_dir = tmp_path / "frames"
    _make_manifest(frames_dir, n_frames=1)

    model = _FakeDetectorModel(
        lambda _image: sv.Detections(
            xyxy=np.array([[1.0, 2.0, 3.0, 4.0], [5.0, 6.0, 7.0, 8.0]]),
            confidence=np.array([0.9, 0.1]),  # second detection below the 0.5 floor
            class_id=np.array([0, 0]),
        )
    )
    monkeypatch.setattr(detect, "load_detector", lambda config, run_id: model)
    out_dir = tmp_path / "prelabels"

    result = prelabel_frames(cfg, frames_dir, out_dir, backend="finetuned")

    data = json.loads(result.coco_path.read_text(encoding="utf-8"))
    assert len(data["annotations"]) == 1


def test_finetuned_backend_raises_prelabel_backend_unavailable_not_bare_weights_not_found(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames_dir = tmp_path / "frames"
    _make_manifest(frames_dir, n_frames=1)

    def fake_load_detector(config, run_id):
        raise detect.WeightsNotFound(
            f"no 'champion' alias set for run_id={run_id!r}"
        )

    monkeypatch.setattr(detect, "load_detector", fake_load_detector)
    out_dir = tmp_path / "prelabels"

    with pytest.raises(PrelabelBackendUnavailable, match="champion"):
        prelabel_frames(cfg, frames_dir, out_dir, backend="finetuned")


def test_load_finetuned_backend_docstring_explains_champion_vs_freeze_pin() -> None:
    doc = prelabel._load_finetuned_backend.__doc__ or ""
    assert "champion" in doc
    assert "freeze" in doc.lower() or "pin" in doc.lower()
