"""Round-trip and format guards for the one-way tracking Parquet -> CSV export
(D-14: the Parquet stays canonical, the CSV is never a pipeline input), plus
coverage for the two dev-set bundle inputs Phase 2.1 never persisted:
`export_detections_parquet` (raw per-frame detections, pinned to a detector run) and
`export_track_crops` (torso-region crops with a machine-readable index).
"""

from __future__ import annotations

import json
from pathlib import Path

import cv2
import numpy as np
import polars as pl
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
from flag_football_ep.cv import detect, frames, registry, schema, teams
from flag_football_ep.cv.export import (
    TrackingParquetNotFound,
    export_detections_parquet,
    export_track_crops,
    export_tracking_csv,
)
from flag_football_ep.cv.schema import TRACKING_COLUMNS, write_tracking_parquet
from flag_football_ep.testing import synthetic_tracks


def _write_tracks(tmp_path: Path) -> Path:
    tracks = synthetic_tracks(with_teams=True, with_field_coords=True)
    parquet_path = tmp_path / "tracks.parquet"
    write_tracking_parquet(tracks, parquet_path)
    return parquet_path


def test_export_round_trip_header_matches_tracking_columns(tmp_path: Path) -> None:
    parquet_path = _write_tracks(tmp_path)
    csv_path = tmp_path / "tracks.csv"

    written = export_tracking_csv(parquet_path, csv_path)

    assert written == csv_path
    first_line = csv_path.read_text(encoding="utf-8").splitlines()[0]
    assert first_line == ",".join(TRACKING_COLUMNS)


def test_export_round_trip_row_count_matches(tmp_path: Path) -> None:
    parquet_path = _write_tracks(tmp_path)
    csv_path = tmp_path / "tracks.csv"
    export_tracking_csv(parquet_path, csv_path)

    lines = csv_path.read_text(encoding="utf-8").splitlines()
    tracks = synthetic_tracks(with_teams=True, with_field_coords=True)
    assert len(lines) - 1 == tracks.height


def test_export_null_team_id_renders_as_empty_field(tmp_path: Path) -> None:
    parquet_path = _write_tracks(tmp_path)
    csv_path = tmp_path / "tracks.csv"
    export_tracking_csv(parquet_path, csv_path)

    header = TRACKING_COLUMNS
    team_id_idx = header.index("team_id")
    lines = csv_path.read_text(encoding="utf-8").splitlines()[1:]

    referee_lines = [
        line for line in lines if line.split(",")[header.index("class_name")] == "referee"
    ]
    assert referee_lines, "expected at least one referee row (null team_id) in the fixture"
    for line in referee_lines:
        field = line.split(",")[team_id_idx]
        assert field == ""
        assert field != "null"


def test_export_floats_carry_at_most_four_decimals(tmp_path: Path) -> None:
    parquet_path = _write_tracks(tmp_path)
    csv_path = tmp_path / "tracks.csv"
    export_tracking_csv(parquet_path, csv_path)

    header = TRACKING_COLUMNS
    float_columns = {"confidence", "bbox_x1", "bbox_y1", "bbox_x2", "bbox_y2",
                      "foot_x_px", "foot_y_px", "x_yards", "y_yards"}
    float_indices = [header.index(name) for name in float_columns]

    lines = csv_path.read_text(encoding="utf-8").splitlines()[1:]
    for line in lines:
        fields = line.split(",")
        for idx in float_indices:
            value = fields[idx]
            if value == "":
                continue
            if "." in value:
                decimals = value.split(".")[1]
                assert len(decimals) <= 4, f"{value!r} has more than 4 decimals"


def test_export_missing_input_parquet_raises_named_exception(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.parquet"
    csv_path = tmp_path / "out.csv"

    with pytest.raises(TrackingParquetNotFound, match=str(missing)):
        export_tracking_csv(missing, csv_path)


# --- shared config helper (mirrors tests/test_cv_detect_infer.py::_make_config) -------------


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
        dvc_remote_name="otc-obs",
        dvc_remote_url="s3://test-bucket/flag-football-datasets",
        dvc_remote_endpoint="https://obs.eu-de.otc.t-systems.com",
        otc_obs_access_key_env="OTC_OBS_ACCESS_KEY_ID",
        otc_obs_secret_key_env="OTC_OBS_SECRET_ACCESS_KEY",
    )
    return Config(
        paths=paths, reference=reference, sources=sources, train=train, report=report, cv=cv
    )


# --- synthetic clip helper (mirrors tests/test_cv_detect_infer.py) --------------------------


def _write_synthetic_clip(path: Path, n_frames: int, *, width: int = 800, height: int = 600) -> Path:
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


# --- fake model (mirrors tests/test_cv_detect_infer.py) -------------------------------------


class _FakeModel:
    """A fake detector: `.predict(image, params=None)` mirrors the loaded pyfunc
    model's contract. `boxes_fn(call_index)` decides what `sv.Detections` each
    successive call returns.
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


# --- export_detections_parquet ---------------------------------------------------------------


def test_export_detections_parquet_writes_one_row_per_detected_box(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    clip = _write_synthetic_clip(tmp_path / "video" / "clip_001.mp4", n_frames=3)

    per_call = [
        _detections([[1, 2, 3, 4], [5, 6, 7, 8]], [0.9, 0.8], [0, 1]),
        _empty(),
        _detections([[9, 10, 11, 12]], [0.7], [0]),
    ]
    model = _FakeModel(lambda i: per_call[i])

    monkeypatch.setattr(frames, "clip_paths", lambda *a, **k: [clip])
    monkeypatch.setattr(detect, "load_detector", lambda cfg, run_id: model)

    out_path = tmp_path / "out" / "detections.parquet"
    written = export_detections_parquet(config, "test-session", "drone", "run123", out_path)

    df = pl.read_parquet(written)
    assert df.height == 3  # 2 + 0 + 1 boxes, empty frame contributes zero rows
    assert sorted(df["frame_index"].to_list()) == [0, 0, 2]
    assert sorted(df["class_name"].to_list()) == ["player", "player", "referee"]
    assert set(df["detector_run_id"].to_list()) == {"run123"}


def test_export_detections_parquet_empty_frame_does_not_abort_export(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    clip = _write_synthetic_clip(tmp_path / "video" / "clip_001.mp4", n_frames=4)

    per_call = [_empty(), _empty(), _empty(), _detections([[1, 2, 3, 4]], [0.9], [0])]
    model = _FakeModel(lambda i: per_call[i])

    monkeypatch.setattr(frames, "clip_paths", lambda *a, **k: [clip])
    monkeypatch.setattr(detect, "load_detector", lambda cfg, run_id: model)

    out_path = tmp_path / "out" / "detections.parquet"
    written = export_detections_parquet(config, "test-session", "drone", "run123", out_path)

    df = pl.read_parquet(written)
    # Every frame is decoded (4 model calls happened) but only the last one has a box.
    assert len(model.calls) == 4
    assert df.height == 1
    assert df["frame_index"].to_list() == [3]


def test_export_detections_parquet_stamps_detected_at_once_per_export(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    clip_a = _write_synthetic_clip(tmp_path / "video" / "clip_001.mp4", n_frames=2)
    clip_b = _write_synthetic_clip(tmp_path / "video" / "clip_002.mp4", n_frames=2)

    model = _FakeModel(lambda i: _detections([[1, 2, 3, 4]], [0.9], [0]))

    monkeypatch.setattr(frames, "clip_paths", lambda *a, **k: [clip_a, clip_b])
    monkeypatch.setattr(detect, "load_detector", lambda cfg, run_id: model)

    out_path = tmp_path / "out" / "detections.parquet"
    written = export_detections_parquet(config, "test-session", "drone", "run123", out_path)

    df = pl.read_parquet(written)
    assert df.height == 4  # 2 clips x 2 frames x 1 box
    assert df["detected_at"].n_unique() == 1


def test_export_detections_parquet_round_trip_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    clip = _write_synthetic_clip(tmp_path / "video" / "clip_001.mp4", n_frames=2)

    per_call = [
        _detections([[1, 2, 3, 4], [5, 6, 7, 8]], [0.9, 0.8], [0, 1]),
        _detections([[9, 10, 11, 12]], [0.7], [0]),
    ]
    model = _FakeModel(lambda i: per_call[i])

    monkeypatch.setattr(frames, "clip_paths", lambda *a, **k: [clip])
    monkeypatch.setattr(detect, "load_detector", lambda cfg, run_id: model)

    out_path = tmp_path / "out" / "detections.parquet"
    written = export_detections_parquet(config, "test-session", "drone", "run123", out_path)

    written_frame = pl.read_parquet(written)
    reloaded = schema.conform_detections(pl.read_parquet(written))
    assert reloaded.equals(written_frame)
    assert reloaded.columns == list(schema.DETECTION_COLUMNS)


def test_export_detections_parquet_run_id_none_reads_freeze_pin_never_resolve_champion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    clip = _write_synthetic_clip(tmp_path / "video" / "clip_001.mp4", n_frames=1)
    model = _FakeModel(lambda i: _empty())

    pin_path = config.paths.reference / "hackathon_freeze.json"
    pin_path.parent.mkdir(parents=True, exist_ok=True)
    pin_path.write_text(
        json.dumps(
            {
                "run_id": "frozen-run-abc",
                "dataset_hash": "deadbeef",
                "frozen_at": "2026-01-01T00:00:00+00:00",
                "model_version": "1",
            }
        ),
        encoding="utf-8",
    )

    def _raise_if_called(name: str, cfg: Config) -> str:
        raise AssertionError("resolve_champion must never be called for run_id=None")

    monkeypatch.setattr(registry, "resolve_champion", _raise_if_called)
    monkeypatch.setattr(frames, "clip_paths", lambda *a, **k: [clip])

    seen_run_ids: list[str] = []

    def _fake_load_detector(cfg, run_id):
        seen_run_ids.append(run_id)
        return model

    monkeypatch.setattr(detect, "load_detector", _fake_load_detector)

    out_path = tmp_path / "out" / "detections.parquet"
    export_detections_parquet(config, "test-session", "drone", None, out_path)

    assert seen_run_ids == ["frozen-run-abc"]


def test_conform_detections_rejects_class_name_outside_vocabulary(tmp_path: Path) -> None:
    row = {
        "session_id": "test-session",
        "clip_number": 1,
        "frame_index": 0,
        "timestamp_s": 0.0,
        "det_index": 0,
        "class_name": "ball",
        "confidence": 0.9,
        "bbox_x1": 0.0,
        "bbox_y1": 0.0,
        "bbox_x2": 1.0,
        "bbox_y2": 1.0,
        "detector_run_id": "run123",
        "detected_at": "2026-01-01T00:00:00+00:00",
    }
    df = pl.DataFrame([row])

    with pytest.raises(schema.InvalidDetectionClass):
        schema.write_detections_parquet(df, tmp_path / "out.parquet")


# --- export_track_crops ------------------------------------------------------------------


def test_export_track_crops_writes_expected_directory_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    tracks = synthetic_tracks(
        n_clips=2, n_frames=20, n_tracks=3, session_id="test-session", with_teams=True
    )
    clips = {
        c: _write_synthetic_clip(tmp_path / "video" / f"clip_{c:03d}.mp4", n_frames=20)
        for c in (1, 2)
    }
    monkeypatch.setattr(frames, "clip_paths", lambda *a, **k: list(clips.values()))

    out_dir = tmp_path / "crops"
    n_crops = export_track_crops(config, "test-session", tracks, out_dir)

    assert n_crops > 0
    # track 0 is the referee in every clip (flag_football_ep.testing convention) -- never cropped
    assert not (out_dir / "clip_001" / "track_0000").exists()
    assert not (out_dir / "clip_002" / "track_0000").exists()
    # tracks 1 and 2 are players -- both cropped in both clips
    for clip_dir in ("clip_001", "clip_002"):
        for track_dir in ("track_0001", "track_0002"):
            jpgs = list((out_dir / clip_dir / track_dir).glob("*.jpg"))
            assert jpgs, f"expected crops under {clip_dir}/{track_dir}"
    assert (out_dir / "index.csv").exists()
    assert (out_dir / "crops_meta.json").exists()


def test_export_track_crops_index_has_one_row_per_written_jpeg(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    tracks = synthetic_tracks(
        n_clips=1, n_frames=20, n_tracks=3, session_id="test-session", with_teams=True
    )
    clip = _write_synthetic_clip(tmp_path / "video" / "clip_001.mp4", n_frames=20)
    monkeypatch.setattr(frames, "clip_paths", lambda *a, **k: [clip])

    out_dir = tmp_path / "crops"
    n_crops = export_track_crops(config, "test-session", tracks, out_dir)

    jpgs = list(out_dir.rglob("*.jpg"))
    index = pl.read_csv(out_dir / "index.csv")
    assert len(jpgs) == n_crops
    assert index.height == n_crops


def test_export_track_crops_excludes_referees(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    tracks = synthetic_tracks(
        n_clips=1, n_frames=20, n_tracks=3, session_id="test-session", with_teams=True
    )
    clip = _write_synthetic_clip(tmp_path / "video" / "clip_001.mp4", n_frames=20)
    monkeypatch.setattr(frames, "clip_paths", lambda *a, **k: [clip])

    out_dir = tmp_path / "crops"
    export_track_crops(config, "test-session", tracks, out_dir)

    index = pl.read_csv(out_dir / "index.csv")
    assert "referee" not in index["class_name"].to_list()
    assert not (out_dir / "clip_001" / "track_0000").exists()


def test_export_track_crops_rerun_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    tracks = synthetic_tracks(
        n_clips=1, n_frames=20, n_tracks=2, session_id="test-session", with_teams=True
    )
    clip = _write_synthetic_clip(tmp_path / "video" / "clip_001.mp4", n_frames=20)
    monkeypatch.setattr(frames, "clip_paths", lambda *a, **k: [clip])

    out_dir = tmp_path / "crops"
    n1 = export_track_crops(config, "test-session", tracks, out_dir)
    first_index = (out_dir / "index.csv").read_text(encoding="utf-8")

    n2 = export_track_crops(config, "test-session", tracks, out_dir)
    second_index = (out_dir / "index.csv").read_text(encoding="utf-8")

    assert n1 == n2
    assert first_index == second_index


def test_export_track_crops_skips_referee_labeled_rows_within_a_flip_noise_player_track(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression test (found running Task 3's real export against the pilot session
    tracking Parquet): a track whose FIRST row is `class_name="player"` still needs
    every individual sampled row checked, because a handful of tracks flip class
    mid-track (known detector noise, ~55 tracks session-wide per docs/cv-setup.md).
    The track-level check alone let 115 referee-labeled rows reach the real index.csv
    before this per-row guard was added.
    """
    config = _make_config(tmp_path)
    tracks = synthetic_tracks(
        n_clips=1, n_frames=20, n_tracks=2, session_id="test-session", with_teams=True
    )
    sampled_positions = teams._sample_frame_indices(20, 12)
    # Exclude position 0 -- track 1's first row must stay "player" so the track-level
    # first-row screen still passes and this test actually exercises the per-row check.
    flipped_frames = set(sampled_positions[1:3])
    assert flipped_frames, "expected at least two non-zero sampled positions for n_rows=20"

    tracks = tracks.with_columns(
        pl.when(
            (pl.col("track_id") == 1) & (pl.col("frame_index").is_in(list(flipped_frames)))
        )
        .then(pl.lit("referee"))
        .otherwise(pl.col("class_name"))
        .alias("class_name")
    )

    clip = _write_synthetic_clip(tmp_path / "video" / "clip_001.mp4", n_frames=20)
    monkeypatch.setattr(frames, "clip_paths", lambda *a, **k: [clip])

    out_dir = tmp_path / "crops"
    export_track_crops(config, "test-session", tracks, out_dir)

    index = pl.read_csv(out_dir / "index.csv")
    assert "referee" not in index["class_name"].to_list()

    track_1_frames = set(
        index.filter(pl.col("track_id") == 1)["frame_index"].to_list()
    )
    assert not (track_1_frames & flipped_frames)
