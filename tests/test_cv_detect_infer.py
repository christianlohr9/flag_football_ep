"""Coverage for `flag_football_ep.cv.detect.load_detector`/`detect_video`: champion-
resolved weight loading and per-frame inference (full-frame and SAHI-tiled), offline
against a `tmp_path` MLflow store, a synthetic OpenCV-written clip, and a fake detector
object -- no real RF-DETR weights, no network, no GPU.

`load_detector`'s success path (an actually-loadable pyfunc model) is intentionally not
covered here -- it would require a real registered checkpoint, which is
`tests/test_cv_registry.py`'s job. This file covers `load_detector`'s *contract*: it
resolves the champion alias by default, accepts no filesystem path, and wraps a
resolution failure in `WeightsNotFound` naming the run id.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import cv2
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
from flag_football_ep.cv.detect import (
    DetectionBatch,
    InvalidDetectionClass,
    MissingClipError,
    WeightsNotFound,
    detect_video,
    load_detector,
)

# --- shared test config helper (mirrors tests/test_cv_detect_train.py::_make_config) ------


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
        resolution=224,
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


# --- synthetic clip helper ------------------------------------------------------------------


def _write_synthetic_clip(path: Path, n_frames: int, *, width: int = 64, height: int = 48) -> Path:
    """Write a tiny, real, decodable video file -- each frame a distinct solid color so
    a bug that dropped/reordered frames would be visible if inspected manually.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(path), fourcc, 10.0, (width, height))
    try:
        for i in range(n_frames):
            frame = np.full((height, width, 3), (i * 17) % 256, dtype=np.uint8)
            writer.write(frame)
    finally:
        writer.release()
    return path


# --- fake model -------------------------------------------------------------------------


class _FakeModel:
    """A fake detector: `.predict(image, params=None)` mirrors the loaded pyfunc
    model's contract (`RFDETRWrapper.predict` forwards `params` straight into the
    wrapped `RFDETRSmall.predict(**params)` call -- see `cv.detect._call_model`).
    `boxes_fn(call_index)` decides what `sv.Detections` each successive call returns,
    so a test can vary output per frame/tile without a real detector.
    """

    def __init__(self, boxes_fn) -> None:
        self._boxes_fn = boxes_fn
        self.calls: list[dict] = []

    def predict(self, image, params=None) -> sv.Detections:
        call_index = len(self.calls)
        self.calls.append({"image": image, "params": params})
        return self._boxes_fn(call_index)


def _detections(xyxy, confidence, class_id) -> sv.Detections:
    return sv.Detections(
        xyxy=np.array(xyxy, dtype=np.float64).reshape(-1, 4),
        confidence=np.array(confidence, dtype=np.float64),
        class_id=np.array(class_id, dtype=np.int64),
    )


def _empty() -> sv.Detections:
    return _detections(np.zeros((0, 4)), [], [])


# --- load_detector ------------------------------------------------------------------------


def test_load_detector_signature_has_no_weights_path_parameter() -> None:
    params = inspect.signature(load_detector).parameters
    assert set(params) == {"config", "run_id"}


def test_load_detector_resolves_champion_alias_when_run_id_is_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    calls: list[tuple[str, Config]] = []

    def _fake_resolve_champion(name: str, cfg: Config) -> str:
        calls.append((name, cfg))
        return "deadbeef00"

    monkeypatch.setattr(registry, "resolve_champion", _fake_resolve_champion)

    with pytest.raises(WeightsNotFound) as excinfo:
        load_detector(config)

    assert calls == [(registry.detector_model_name(config), config)]
    assert "deadbeef00" in str(excinfo.value)


def test_load_detector_raises_weights_not_found_naming_run_id_when_run_missing(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)

    with pytest.raises(WeightsNotFound) as excinfo:
        load_detector(config, run_id="cafefeed01")

    assert "cafefeed01" in str(excinfo.value)


# --- detect_video: unopenable clip ----------------------------------------------------------


def test_detect_video_unopenable_clip_raises_missing_clip_error_naming_path(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    missing_clip = tmp_path / "does-not-exist.mp4"
    model = _FakeModel(lambda _i: _empty())

    with pytest.raises(MissingClipError) as excinfo:
        detect_video(config, missing_clip, model, resolution=224, sahi=False)

    assert str(missing_clip) in str(excinfo.value)


# --- detect_video: full-frame path -----------------------------------------------------------


def test_detect_video_yields_one_batch_per_frame_in_order_including_empty(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    clip = _write_synthetic_clip(tmp_path / "clip.mp4", n_frames=4)

    per_call = [
        _detections([[1, 2, 3, 4]], [0.9], [0]),
        _empty(),
        _detections([[5, 6, 7, 8], [9, 10, 11, 12]], [0.7, 0.6], [1, 0]),
        _empty(),
    ]
    model = _FakeModel(lambda i: per_call[i])

    batches = list(detect_video(config, clip, model, resolution=224, sahi=False))

    assert len(batches) == 4
    assert [b.frame_index for b in batches] == [0, 1, 2, 3]
    assert all(isinstance(b, DetectionBatch) for b in batches)
    assert batches[0].xyxy.shape == (1, 4)
    assert batches[1].xyxy.shape == (0, 4)
    assert batches[2].xyxy.shape == (2, 4)
    assert batches[3].xyxy.shape == (0, 4)
    # every model call requested the same resolution via the pyfunc `params` channel
    assert all(call["params"] == {"shape": (224, 224)} for call in model.calls)


def test_detect_video_out_of_vocabulary_class_id_raises_invalid_detection_class(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    clip = _write_synthetic_clip(tmp_path / "clip.mp4", n_frames=2)

    model = _FakeModel(lambda _i: _detections([[0, 0, 1, 1]], [0.9], [7]))

    with pytest.raises(InvalidDetectionClass) as excinfo:
        list(detect_video(config, clip, model, resolution=224, sahi=False))

    assert "7" in str(excinfo.value)


def test_detect_video_filters_below_model_confidence_floor(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    clip = _write_synthetic_clip(tmp_path / "clip.mp4", n_frames=1)

    model = _FakeModel(
        lambda _i: _detections(
            [[0, 0, 1, 1], [2, 2, 3, 3]], [0.9, 0.1], [0, 0]
        )
    )

    batches = list(detect_video(config, clip, model, resolution=224, sahi=False))

    assert batches[0].xyxy.shape == (1, 4)
    assert batches[0].confidence[0] == pytest.approx(0.9)


# --- detect_video: timing accessor -----------------------------------------------------------


def test_detect_video_timing_accessor_reports_nonzero_decode_and_detect(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    clip = _write_synthetic_clip(tmp_path / "clip.mp4", n_frames=3)
    model = _FakeModel(lambda _i: _empty())

    run = detect_video(config, clip, model, resolution=224, sahi=False)
    list(run)

    timings = {t.stage: t for t in run.timings()}
    assert set(timings) == {"decode", "detect", "postprocess"}
    assert timings["decode"].seconds > 0
    assert timings["detect"].seconds > 0
    assert timings["decode"].frames == 3
    assert timings["detect"].frames == 3
    assert timings["postprocess"].frames == 3


# --- detect_video: SAHI tiled path ------------------------------------------------------------


class _StubSliceResult:
    def __init__(self, images: list[np.ndarray], starting_pixels: list[list[int]]) -> None:
        self.images = images
        self.starting_pixels = starting_pixels


def test_detect_video_sahi_path_merges_two_tiles_of_the_same_object(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    clip = _write_synthetic_clip(tmp_path / "clip.mp4", n_frames=1)

    # Tile A covers world x in [0, 100), offset (0, 0); tile B covers world x in
    # [50, 150), offset (50, 0). A single real-world box at world xyxy=[60,10,90,30]
    # sits inside both tiles' overlap region -- tile A sees it at its own (unshifted)
    # world coordinates, tile B sees it shifted left by its own offset.
    tile_a = np.zeros((10, 10, 3), dtype=np.uint8)
    tile_b = np.zeros((10, 10, 3), dtype=np.uint8)

    def _fake_slice_image(image, **kwargs):
        return _StubSliceResult(images=[tile_a, tile_b], starting_pixels=[[0, 0], [50, 0]])

    monkeypatch.setattr("sahi.slicing.slice_image", _fake_slice_image)

    per_tile = [
        _detections([[60, 10, 90, 30]], [0.9], [0]),  # tile A, world-aligned already
        _detections([[10, 10, 40, 30]], [0.85], [0]),  # tile B, local coords
    ]
    model = _FakeModel(lambda i: per_tile[i])

    batches = list(detect_video(config, clip, model, resolution=224, sahi=True))

    assert len(batches) == 1
    batch = batches[0]
    assert batch.xyxy.shape == (1, 4)
    np.testing.assert_allclose(batch.xyxy[0], [60.0, 10.0, 90.0, 30.0])
    assert batch.confidence[0] == pytest.approx(0.9)
    assert batch.class_id[0] == 0


def test_detect_video_sahi_records_slice_settings_used(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    clip = _write_synthetic_clip(tmp_path / "clip.mp4", n_frames=1)

    captured_kwargs: dict = {}

    def _fake_slice_image(image, **kwargs):
        captured_kwargs.update(kwargs)
        return _StubSliceResult(images=[], starting_pixels=[])

    monkeypatch.setattr("sahi.slicing.slice_image", _fake_slice_image)

    model = _FakeModel(lambda _i: _empty())
    batches = list(detect_video(config, clip, model, resolution=224, sahi=True))

    assert batches[0].xyxy.shape == (0, 4)
    assert captured_kwargs["slice_height"] == config.cv.sahi_slice
    assert captured_kwargs["slice_width"] == config.cv.sahi_slice
    assert captured_kwargs["overlap_height_ratio"] == config.cv.sahi_overlap
    assert captured_kwargs["overlap_width_ratio"] == config.cv.sahi_overlap
