"""Coverage for `flag_football_ep.cv.overlay`: the pure `draw_frame` drawing function
and the `render_track_overlay` decode/write wrapper -- offline against tiny synthetic
clips (`cv2.VideoWriter`), no real footage, no network, no GPU.
"""

from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np
import polars as pl
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
from flag_football_ep.cv.detect import MissingClipError
from flag_football_ep.cv.overlay import NoTracksForClip, draw_frame, render_track_overlay
from flag_football_ep.cv.palette import TEAM_COLORS
from flag_football_ep.testing import synthetic_tracks

_CLIP_FPS = 10.0


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


def _write_synthetic_clip(path: Path, n_frames: int, *, width: int = 64, height: int = 48) -> Path:
    """Write a tiny, real, decodable video file."""
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


def _row(
    *,
    track_id: int,
    class_name: str,
    team_id: int | None,
    x1: float,
    y1: float,
    x2: float,
    y2: float,
) -> dict:
    return {
        "track_id": track_id,
        "class_name": class_name,
        "team_id": team_id,
        "bbox_x1": x1,
        "bbox_y1": y1,
        "bbox_x2": x2,
        "bbox_y2": y2,
        "foot_x_px": (x1 + x2) / 2,
        "foot_y_px": y2,
    }


# --- Task 1 tests -----------------------------------------------------------------------------


def test_draw_frame_returns_same_shape_and_dtype_and_differs_from_input(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    frame = np.zeros((200, 300, 3), dtype=np.uint8)
    rows = [_row(track_id=1, class_name="player", team_id=0, x1=10, y1=10, x2=40, y2=60)]

    annotated = draw_frame(frame, rows, config, clip_number=1, frame_index=0)

    assert annotated.shape == frame.shape
    assert annotated.dtype == frame.dtype
    assert not np.array_equal(annotated, frame)
    # original frame must not be mutated in place
    assert np.array_equal(frame, np.zeros((200, 300, 3), dtype=np.uint8))


def test_two_team_ids_produce_two_different_colours_at_box_borders(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    frame = np.zeros((200, 300, 3), dtype=np.uint8)
    rows = [
        _row(track_id=1, class_name="player", team_id=0, x1=10, y1=10, x2=40, y2=60),
        _row(track_id=2, class_name="player", team_id=1, x1=100, y1=10, x2=130, y2=60),
    ]

    annotated = draw_frame(frame, rows, config, clip_number=1, frame_index=0)

    pixel_team0 = annotated[10, 10]
    pixel_team1 = annotated[10, 100]
    assert not np.array_equal(pixel_team0, [0, 0, 0])
    assert not np.array_equal(pixel_team1, [0, 0, 0])
    assert not np.array_equal(pixel_team0, pixel_team1)


def test_team_zero_draws_red_and_team_one_draws_blue(tmp_path: Path) -> None:
    """Regression test for the display-colour inversion bug: team_id 0 must always
    draw as the red-dominant colour (jersey-colour anchoring contract, `cv.teams`),
    team_id 1 as the blue-dominant colour -- not merely "two distinct colours"
    (already covered by `test_two_team_ids_produce_two_different_colours_at_box_borders`).
    """
    config = _make_config(tmp_path)
    frame = np.zeros((200, 300, 3), dtype=np.uint8)
    # y1=110 keeps the box border well clear of the legend text (burnt into roughly
    # y=[6, 86] of the top-left corner) so the sampled pixel is pure box-border colour,
    # not anti-aliased text blended into it.
    rows = [
        _row(track_id=1, class_name="player", team_id=0, x1=10, y1=110, x2=40, y2=150),
        _row(track_id=2, class_name="player", team_id=1, x1=100, y1=110, x2=130, y2=150),
    ]

    annotated = draw_frame(frame, rows, config, clip_number=1, frame_index=0)

    team0_pixel = annotated[110, 10]  # BGR
    team1_pixel = annotated[110, 100]
    assert int(team0_pixel[2]) > int(team0_pixel[0]), "team_id 0 must be red-dominant"
    assert int(team1_pixel[0]) > int(team1_pixel[2]), "team_id 1 must be blue-dominant"
    assert tuple(int(c) for c in team0_pixel) == TEAM_COLORS[0]
    assert tuple(int(c) for c in team1_pixel) == TEAM_COLORS[1]


def test_referee_and_null_team_rows_each_get_their_own_colour(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    frame = np.zeros((200, 300, 3), dtype=np.uint8)
    rows = [
        _row(track_id=0, class_name="referee", team_id=None, x1=10, y1=10, x2=40, y2=60),
        _row(track_id=3, class_name="player", team_id=None, x1=100, y1=10, x2=130, y2=60),
        _row(track_id=1, class_name="player", team_id=0, x1=190, y1=10, x2=220, y2=60),
        _row(track_id=2, class_name="player", team_id=1, x1=250, y1=10, x2=280, y2=60),
    ]

    annotated = draw_frame(frame, rows, config, clip_number=1, frame_index=0)

    referee_pixel = annotated[10, 10]
    null_team_pixel = annotated[10, 100]
    team0_pixel = annotated[10, 190]
    team1_pixel = annotated[10, 250]

    colors = [tuple(p) for p in (referee_pixel, null_team_pixel, team0_pixel, team1_pixel)]
    assert len(set(colors)) == 4, f"expected 4 distinct colours, got {colors}"


def test_render_track_overlay_writes_a_file_with_matching_frame_count(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    n_frames = 6
    clip_path = _write_synthetic_clip(tmp_path / "clip_001.mp4", n_frames)
    tracks = synthetic_tracks(n_clips=1, n_frames=n_frames, n_tracks=2, with_teams=True)
    out_path = tmp_path / "out" / "clip_001_overlay.mp4"

    written = render_track_overlay(config, clip_path, tracks, out_path)

    assert written == out_path
    assert out_path.exists()

    capture = cv2.VideoCapture(str(out_path))
    try:
        written_frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
    finally:
        capture.release()
    assert written_frame_count == n_frames


def test_render_track_overlay_raises_missing_clip_error_naming_the_path(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    missing_clip = tmp_path / "clip_099.mp4"
    tracks = synthetic_tracks(n_clips=1, n_frames=3, n_tracks=2)

    with pytest.raises(MissingClipError, match=str(missing_clip)):
        render_track_overlay(config, missing_clip, tracks, tmp_path / "out.mp4")


def test_render_track_overlay_raises_no_tracks_for_clip_with_no_matching_rows(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    clip_path = _write_synthetic_clip(tmp_path / "clip_002.mp4", 3)
    # tracks only cover clip_number=1, the clip file parses to clip_number=2
    tracks = synthetic_tracks(n_clips=1, n_frames=3, n_tracks=2)

    with pytest.raises(NoTracksForClip, match="clip_number=2"):
        render_track_overlay(config, clip_path, tracks, tmp_path / "out.mp4")
