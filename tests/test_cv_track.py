"""Coverage for `flag_football_ep.cv.track.track_session`: per-clip BoT-SORT tracking,
per-clip containment, stage timing, and schema-conforming pixel-space output --
offline against a fake detector and tiny synthetic clips (`cv2.VideoWriter`), no real
RF-DETR weights, no network, no GPU.

`load_detector` itself is monkeypatched to return a fake model directly (its own
contract -- champion resolution, `WeightsNotFound` wrapping -- is covered by
`tests/test_cv_detect_infer.py`); this file's job is `track_session`'s own orchestration:
the per-clip try/except, notices, stage timing, and the BoT-SORT association loop
(tracker swapped from OC-SORT in the 02.1-12/02.1-14 gap-fix iteration -- see
`cv/track.py`'s module docstring for the measured rationale).
"""

from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np
import polars as pl
import pytest
import supervision as sv

pytest.importorskip("trackers", reason="requires the cv extras group (uv sync --extra cv)")

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
from flag_football_ep.cv import detect as detect_module
from flag_football_ep.cv.schema import conform_tracking
from flag_football_ep.cv.track import track_session

_CLIP_FPS = 10.0

# --- shared test config helper (mirrors tests/test_cv_detect_infer.py::_make_config) -------


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


# --- fixtures: inventory + synthetic clips --------------------------------------------------

_INVENTORY_HEADER = (
    "domain,session_id,game_id,capture_date,resolution,fps,duration_seconds,"
    "local_path,content_sha256,notes"
)


def _write_inventory(config: Config, rows: list[dict[str, str]]) -> Path:
    lines = [_INVENTORY_HEADER]
    fields = (
        "domain",
        "session_id",
        "game_id",
        "capture_date",
        "resolution",
        "fps",
        "duration_seconds",
        "local_path",
        "content_sha256",
        "notes",
    )
    for row in rows:
        lines.append(",".join(row.get(field, "") for field in fields))
    inventory_path = config.paths.reference / "video_inventory.csv"
    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    inventory_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return inventory_path


def _inventory_row(
    local_path: str, *, duration_seconds: float, session_id: str = "test-session"
) -> dict[str, str]:
    return {
        "domain": "drone",
        "session_id": session_id,
        "game_id": "",
        "capture_date": "2026-05-16",
        "resolution": "1920x1080",
        "fps": str(_CLIP_FPS),
        "duration_seconds": str(duration_seconds),
        "local_path": local_path,
        "content_sha256": "",
        "notes": "",
    }


def _write_synthetic_clip(path: Path, n_frames: int, *, width: int = 64, height: int = 48) -> Path:
    """Write a tiny, real, decodable video file -- each frame a distinct solid color so
    a bug that dropped/reordered frames would be visible if inspected manually.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(path), fourcc, _CLIP_FPS, (width, height))
    try:
        for i in range(n_frames):
            frame = np.full((height, width, 3), (i * 17) % 256, dtype=np.uint8)
            writer.write(frame)
    finally:
        writer.release()
    return path


# --- fake model (mirrors tests/test_cv_detect_infer.py::_FakeModel) -------------------------


class _FakeModel:
    """`.predict(image, params=None)` mirrors the loaded pyfunc model's contract.
    `boxes_fn(call_index)` decides what `sv.Detections` each successive call returns, a
    single global counter across every clip's frames (the same model instance is reused
    for the whole session, exactly like the real `load_detector` path).
    """

    def __init__(self, boxes_fn) -> None:
        self._boxes_fn = boxes_fn
        self.calls = 0

    def predict(self, image, params=None) -> sv.Detections:
        call_index = self.calls
        self.calls += 1
        return self._boxes_fn(call_index)


def _detections(xyxy, confidence, class_id) -> sv.Detections:
    return sv.Detections(
        xyxy=np.array(xyxy, dtype=np.float64).reshape(-1, 4),
        confidence=np.array(confidence, dtype=np.float64),
        class_id=np.array(class_id, dtype=np.int64),
    )


def _empty() -> sv.Detections:
    return _detections(np.zeros((0, 4)), [], [])


def _moving_player_box(call_index: int) -> sv.Detections:
    """A single "player" (class_id 0) detection, sliding one pixel per call so the
    tracker's motion model has something non-degenerate to associate on.
    """
    x1 = 10.0 + call_index
    return _detections([[x1, 20.0, x1 + 20.0, 60.0]], [0.9], [0])


def _install_fake_model(monkeypatch: pytest.MonkeyPatch, model: _FakeModel) -> None:
    def _fake_load_detector(config: Config, run_id: str | None = None):
        return model

    monkeypatch.setattr(detect_module, "load_detector", _fake_load_detector)


# --- Task 1 tests -----------------------------------------------------------------------------


def test_two_clip_session_produces_rows_for_both_clips_with_per_clip_track_id_spaces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    n_frames = 4
    clip1 = _write_synthetic_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    clip2 = _write_synthetic_clip(config.paths.video / "Wide - Clip 002.mp4", n_frames)
    _write_inventory(
        config,
        [
            _inventory_row("data/video/Wide - Clip 001.mp4", duration_seconds=n_frames / _CLIP_FPS),
            _inventory_row("data/video/Wide - Clip 002.mp4", duration_seconds=n_frames / _CLIP_FPS),
        ],
    )
    assert clip1.exists() and clip2.exists()

    model = _FakeModel(_moving_player_box)
    _install_fake_model(monkeypatch, model)

    result = track_session(config, "test-session", run_id="deadbeef00")

    tracks_df = pl.read_parquet(result.parquet_path)
    clip1_ids = set(tracks_df.filter(pl.col("clip_number") == 1)["track_id"].to_list())
    clip2_ids = set(tracks_df.filter(pl.col("clip_number") == 2)["track_id"].to_list())

    assert tracks_df.filter(pl.col("clip_number") == 1).height > 0
    assert tracks_df.filter(pl.col("clip_number") == 2).height > 0
    # Both clips independently confirm a track id 0 -- proof that ids are per-clip, not
    # a single incrementing counter across the whole session.
    assert 0 in clip1_ids
    assert 0 in clip2_ids
    assert result.n_clips == 2


def test_a_raising_clip_produces_a_notice_and_does_not_abort_the_other_clip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    n_frames = 4
    good_clip = _write_synthetic_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    # Clip 2 is a registered, existing file that is not a real video -- cv2 cannot open
    # it, so `detect.detect_video` raises `MissingClipError` synchronously, exercising
    # the per-clip try/except exactly like a corrupt real clip would.
    bad_clip = config.paths.video / "Wide - Clip 002.mp4"
    bad_clip.parent.mkdir(parents=True, exist_ok=True)
    bad_clip.write_bytes(b"not a real video file")

    _write_inventory(
        config,
        [
            _inventory_row("data/video/Wide - Clip 001.mp4", duration_seconds=n_frames / _CLIP_FPS),
            _inventory_row("data/video/Wide - Clip 002.mp4", duration_seconds=1.0),
        ],
    )
    assert good_clip.exists() and bad_clip.exists()

    model = _FakeModel(_moving_player_box)
    _install_fake_model(monkeypatch, model)

    result = track_session(config, "test-session", run_id="deadbeef00")

    assert any("2" in notice for notice in result.notices), result.notices
    tracks_df = pl.read_parquet(result.parquet_path)
    assert tracks_df.filter(pl.col("clip_number") == 1).height > 0
    assert tracks_df.filter(pl.col("clip_number") == 2).height == 0
    # The denominator stays whole: both clips are still counted even though one failed.
    assert result.n_clips == 2


def test_a_clip_with_zero_detections_produces_a_notice_and_no_rows_but_still_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    n_frames = 3
    _write_synthetic_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    _write_inventory(
        config,
        [_inventory_row("data/video/Wide - Clip 001.mp4", duration_seconds=n_frames / _CLIP_FPS)],
    )

    model = _FakeModel(lambda _i: _empty())
    _install_fake_model(monkeypatch, model)

    result = track_session(config, "test-session", run_id="deadbeef00")

    assert result.n_clips == 1
    assert any("zero tracks" in notice and "1" in notice for notice in result.notices), (
        result.notices
    )
    tracks_df = pl.read_parquet(result.parquet_path)
    assert tracks_df.height == 0


def test_foot_point_equals_bottom_centre_of_a_known_box(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    n_frames = 3
    _write_synthetic_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    _write_inventory(
        config,
        [_inventory_row("data/video/Wide - Clip 001.mp4", duration_seconds=n_frames / _CLIP_FPS)],
    )

    # A stationary box: x1=10, y1=20, x2=30, y2=60 -> bottom-centre is (20, 60).
    model = _FakeModel(lambda _i: _detections([[10.0, 20.0, 30.0, 60.0]], [0.9], [0]))
    _install_fake_model(monkeypatch, model)

    result = track_session(config, "test-session", run_id="deadbeef00")

    tracks_df = pl.read_parquet(result.parquet_path)
    assert tracks_df.height > 0
    for row in tracks_df.iter_rows(named=True):
        assert row["foot_x_px"] == pytest.approx(20.0)
        assert row["foot_y_px"] == pytest.approx(60.0)


def test_output_passes_conform_tracking_with_unfilled_columns_null(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    n_frames = 3
    _write_synthetic_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    _write_inventory(
        config,
        [_inventory_row("data/video/Wide - Clip 001.mp4", duration_seconds=n_frames / _CLIP_FPS)],
    )

    model = _FakeModel(_moving_player_box)
    _install_fake_model(monkeypatch, model)

    result = track_session(config, "test-session", run_id="deadbeef00")

    tracks_df = pl.read_parquet(result.parquet_path)
    conformed = conform_tracking(tracks_df)
    assert conformed.height == tracks_df.height
    for column in ("team_id", "x_yards", "y_yards", "game_id", "play_id"):
        assert conformed[column].null_count() == conformed.height


def test_detector_run_id_identical_on_every_row_and_equals_resolved_run_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    n_frames = 3
    _write_synthetic_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    _write_inventory(
        config,
        [_inventory_row("data/video/Wide - Clip 001.mp4", duration_seconds=n_frames / _CLIP_FPS)],
    )

    model = _FakeModel(_moving_player_box)
    _install_fake_model(monkeypatch, model)

    result = track_session(config, "test-session", run_id="cafefeed01")

    tracks_df = pl.read_parquet(result.parquet_path)
    assert tracks_df.height > 0
    assert set(tracks_df["detector_run_id"].to_list()) == {"cafefeed01"}


def test_stage_seconds_contains_all_four_stages_with_nonnegative_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    n_frames = 3
    _write_synthetic_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    _write_inventory(
        config,
        [_inventory_row("data/video/Wide - Clip 001.mp4", duration_seconds=n_frames / _CLIP_FPS)],
    )

    model = _FakeModel(_moving_player_box)
    _install_fake_model(monkeypatch, model)

    result = track_session(config, "test-session", run_id="deadbeef00")

    assert set(result.stage_seconds) == {"decode", "detect", "track", "write"}
    for value in result.stage_seconds.values():
        assert value >= 0.0


def test_track_session_persists_stage_timings_json_sibling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The C-09 runtime gate metric is measured by `ffep cv benchmark` from a persisted
    stage-timings artifact -- `track_session` must actually write it (before this
    coverage existed, `stage_seconds` was computed and dropped, leaving the benchmark
    command with no input any code path ever produced).
    """
    import json

    config = _make_config(tmp_path)
    n_frames = 3
    _write_synthetic_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    _write_inventory(
        config,
        [_inventory_row("data/video/Wide - Clip 001.mp4", duration_seconds=n_frames / _CLIP_FPS)],
    )

    model = _FakeModel(_moving_player_box)
    _install_fake_model(monkeypatch, model)

    result = track_session(config, "test-session", run_id="deadbeef00")

    assert result.timings_path is not None
    assert result.timings_path.is_file()
    assert result.timings_path.parent == result.parquet_path.parent
    assert result.timings_path.name == "test-session_stage_timings.json"

    payload = json.loads(result.timings_path.read_text(encoding="utf-8"))
    assert payload["session_id"] == "test-session"
    by_stage = {entry["stage"]: entry for entry in payload["stages"]}
    assert set(by_stage) == {"decode", "detect", "track", "write"}
    for stage, entry in by_stage.items():
        assert entry["seconds"] >= 0.0
        assert entry["seconds"] == pytest.approx(result.stage_seconds[stage])
    # Every stage covers the SAME decoded frames exactly once -- never a per-stage sum
    # that would multiply the real frame count.
    for stage in ("decode", "detect", "track", "write"):
        assert by_stage[stage]["frames"] == n_frames
    # footage_seconds is the inventory-declared clip duration, not frames / 30.0.
    assert payload["footage_seconds"] == pytest.approx(n_frames / _CLIP_FPS)
    assert result.footage_seconds == pytest.approx(n_frames / _CLIP_FPS)


def test_footage_seconds_falls_back_to_measured_fps_with_a_notice_when_duration_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A clip registered without an inventory `duration_seconds` still contributes real
    footage time -- decoded frames over the clip's MEASURED fps, never a hardcoded
    30.0 -- and the fallback is named in a notice rather than silently applied.
    """
    config = _make_config(tmp_path)
    n_frames = 4
    _write_synthetic_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    row = _inventory_row("data/video/Wide - Clip 001.mp4", duration_seconds=1.0)
    row["duration_seconds"] = ""  # inventory row with no declared duration
    _write_inventory(config, [row])

    model = _FakeModel(_moving_player_box)
    _install_fake_model(monkeypatch, model)

    result = track_session(config, "test-session", run_id="deadbeef00")

    # Fallback is decoded frames / measured fps (the clip was written at _CLIP_FPS).
    assert result.footage_seconds == pytest.approx(n_frames / _CLIP_FPS)
    assert any("footage" in notice and "fall back" in notice for notice in result.notices), (
        result.notices
    )


def test_late_spawning_track_needs_the_tuned_confirmation_window_before_appearing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression coverage for the BoT-SORT tuning (gap-fix iteration, post plan
    02.1-12): a track that spawns after the clip's very first tracked frame does not
    get BoT-SORT's `instant_first_frame_activation` shortcut, so it needs
    `_TRACKER_MINIMUM_CONSECUTIVE_FRAMES` (5) consecutive updates before its rows carry
    a real track id -- proof the tuned confirmation window, not BoT-SORT's own default
    (2) or OC-SORT's prior default (3), is actually wired through `track_session`.
    """
    config = _make_config(tmp_path)
    n_frames = 8
    _write_synthetic_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    _write_inventory(
        config,
        [_inventory_row("data/video/Wide - Clip 001.mp4", duration_seconds=n_frames / _CLIP_FPS)],
    )

    def _boxes(call_index: int) -> sv.Detections:
        # No detections at all for the first two frames, so the player box's first
        # appearance (call_index 2) is not the tracker's very first update() call --
        # it does not qualify for instant first-frame activation.
        if call_index < 2:
            return _empty()
        return _moving_player_box(call_index)

    model = _FakeModel(_boxes)
    _install_fake_model(monkeypatch, model)

    result = track_session(config, "test-session", run_id="deadbeef00")

    tracks_df = pl.read_parquet(result.parquet_path)
    seen_frames = sorted(tracks_df["frame_index"].to_list())
    assert seen_frames, "expected at least one confirmed row once the box stabilises"
    # frames 0-1: no detections; frames 2-5: unconfirmed (tracker_id -1, dropped);
    # frame 6 is the 5th consecutive confirmed update -- the earliest real track id.
    assert min(seen_frames) == 6


def test_boxmot_is_absent_from_the_source_tree() -> None:
    import subprocess

    src_dir = Path(__file__).resolve().parent.parent / "src" / "flag_football_ep"
    result = subprocess.run(
        ["grep", "-rl", "boxmot", str(src_dir)],
        capture_output=True,
        text=True,
        check=False,
    )
    # grep exits 1 when nothing matches -- that is the success case here.
    assert result.returncode == 1, result.stdout
