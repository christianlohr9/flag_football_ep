"""Coverage for `flag_football_ep.cv.radar`: the pure `render_radar_frame` pitch/marker
drawing function and the `render_showcase_reel` decode/compose/write wrapper --
offline against tiny synthetic clips (`cv2.VideoWriter`), no real footage, no network,
no GPU.
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
from flag_football_ep.cv.palette import TEAM_COLORS
from flag_football_ep.cv.radar import (
    _HEADER_HEIGHT_PX,
    _SEPARATOR_FRAMES,
    NoFieldCoordinatesForClip,
    _pitch_geometry,
    _yards_to_px,
    render_radar_frame,
    render_showcase_reel,
)
from flag_football_ep.testing import synthetic_tracks

_CLIP_FPS = 10.0
_SESSION_ID = "test-session"

_INVENTORY_HEADER = (
    "domain,session_id,game_id,capture_date,resolution,fps,duration_seconds,"
    "local_path,content_sha256,notes"
)
_INVENTORY_FIELDS = (
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
        pilot_session_id=_SESSION_ID,
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


def _write_inventory(tmp_path: Path, rows: list[dict[str, str]]) -> Path:
    lines = [_INVENTORY_HEADER]
    for row in rows:
        lines.append(",".join(row.get(field, "") for field in _INVENTORY_FIELDS))
    inventory_path = tmp_path / "data" / "reference" / "video_inventory.csv"
    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    inventory_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return inventory_path


def _inventory_row(local_path: str) -> dict[str, str]:
    return {
        "domain": "drone",
        "session_id": _SESSION_ID,
        "game_id": "",
        "capture_date": "2026-05-16",
        "resolution": "64x48",
        "fps": str(_CLIP_FPS),
        "duration_seconds": "1.0",
        "local_path": local_path,
        "content_sha256": "",
        "notes": "",
    }


def _seed_clip(tmp_path: Path, clip_number: int, n_frames: int) -> None:
    local_path = f"data/video/{_SESSION_ID}/Wide - Clip {clip_number:03d}.mp4"
    _write_synthetic_clip(tmp_path / local_path, n_frames)
    existing_rows = []
    inventory_path = tmp_path / "data" / "reference" / "video_inventory.csv"
    if inventory_path.exists():
        existing_rows = [
            {"local_path": r["local_path"]}
            for r in pl.read_csv(inventory_path, infer_schema_length=0).to_dicts()
        ]
    rows = [_inventory_row(r["local_path"]) for r in existing_rows]
    rows.append(_inventory_row(local_path))
    _write_inventory(tmp_path, rows)


# --- Task 1: render_radar_frame ----------------------------------------------------------------


def test_render_radar_frame_returns_requested_size(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    empty_tracks = synthetic_tracks(
        n_clips=1, n_frames=1, n_tracks=1, with_field_coords=True
    ).filter(pl.col("frame_index") == 999)

    frame = render_radar_frame(empty_tracks, config, (320, 200))

    assert frame.shape == (200, 320, 3)


def test_player_at_origin_lands_near_west_goal_south_sideline_corner(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    tracks_at_frame = pl.DataFrame(
        {
            "track_id": [7],
            "class_name": ["player"],
            "team_id": [0],
            "x_yards": [0.0],
            "y_yards": [0.0],
        }
    )
    size_wh = (400, 200)

    frame = render_radar_frame(tracks_at_frame, config, size_wh)

    geometry = _pitch_geometry(config, size_wh)
    expected_x, expected_y = _yards_to_px(0.0, 0.0, geometry)
    pixel = tuple(int(c) for c in frame[expected_y, expected_x])
    background = tuple(int(c) for c in frame[0, 0])
    assert pixel != background


def test_pitch_scale_is_identical_on_both_axes(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    geometry = _pitch_geometry(config, (500, 150))

    x0, _ = _yards_to_px(0.0, 0.0, geometry)
    x10, _ = _yards_to_px(10.0, 0.0, geometry)
    _, y0 = _yards_to_px(0.0, 0.0, geometry)
    _, y10 = _yards_to_px(0.0, 10.0, geometry)

    x_distance = abs(x10 - x0)
    y_distance = abs(y10 - y0)
    assert abs(x_distance - y_distance) <= 1


def test_two_team_ids_and_referee_and_null_team_produce_four_distinct_marker_colours(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    tracks_at_frame = pl.DataFrame(
        {
            "track_id": [1, 2, 3, 4],
            "class_name": ["player", "player", "referee", "player"],
            "team_id": [0, 1, None, None],
            "x_yards": [5.0, 15.0, 25.0, 35.0],
            "y_yards": [5.0, 5.0, 5.0, 5.0],
        }
    )
    size_wh = (600, 200)

    frame = render_radar_frame(tracks_at_frame, config, size_wh)

    geometry = _pitch_geometry(config, size_wh)
    pixels = []
    for x_yards in (5.0, 15.0, 25.0, 35.0):
        x_px, y_px = _yards_to_px(x_yards, 5.0, geometry)
        pixels.append(tuple(int(c) for c in frame[y_px, x_px]))
    assert len(set(pixels)) == 4


def test_team_zero_marker_is_red_and_team_one_marker_is_blue(tmp_path: Path) -> None:
    """Regression test for the display-colour inversion bug, mirroring
    `test_cv_overlay.test_team_zero_draws_red_and_team_one_draws_blue`: the radar
    half of the showcase reel must draw the same team_id in the same colour as the
    tracked-footage half (both import `cv.palette`'s single shared definition).
    """
    config = _make_config(tmp_path)
    tracks_at_frame = pl.DataFrame(
        {
            "track_id": [1, 2],
            "class_name": ["player", "player"],
            "team_id": [0, 1],
            "x_yards": [5.0, 15.0],
            "y_yards": [5.0, 5.0],
        }
    )
    size_wh = (400, 200)

    frame = render_radar_frame(tracks_at_frame, config, size_wh)

    geometry = _pitch_geometry(config, size_wh)
    x0_px, y0_px = _yards_to_px(5.0, 5.0, geometry)
    x1_px, y1_px = _yards_to_px(15.0, 5.0, geometry)
    team0_pixel = tuple(int(c) for c in frame[y0_px, x0_px])  # BGR
    team1_pixel = tuple(int(c) for c in frame[y1_px, x1_px])

    assert team0_pixel[2] > team0_pixel[0], "team_id 0 must be red-dominant"
    assert team1_pixel[0] > team1_pixel[2], "team_id 1 must be blue-dominant"
    assert team0_pixel == TEAM_COLORS[0]
    assert team1_pixel == TEAM_COLORS[1]


def test_render_radar_frame_draws_every_marker_shape_before_any_marker_label(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression test for the "one team's numbers are missing" display bug:
    `render_radar_frame` must draw every row's marker SHAPE before drawing ANY row's
    track-id LABEL. A single-pass "shape then label, one row at a time" order would
    let a later row's filled marker shape paint directly over an earlier row's
    already-drawn label whenever the two positions sit close together -- normal for
    real footage, where players cluster.

    Verified by recording the actual `cv2` draw-call order rather than by pixel
    inspection: at `_FONT_SCALE`'s size, `cv2.LINE_AA` anti-aliases every text pixel,
    so no pixel ever exactly equals the marker's own colour -- pixel-equality would
    not reliably detect the label at all, let alone whether it got painted over.
    """
    import cv2 as real_cv2

    config = _make_config(tmp_path)
    tracks_at_frame = pl.DataFrame(
        {
            "track_id": [1, 2, 3],
            "class_name": ["player", "player", "referee"],
            "team_id": [0, 1, None],
            "x_yards": [5.0, 15.0, 25.0],
            "y_yards": [5.0, 5.0, 5.0],
        }
    )

    call_order: list[str] = []
    for shape_name in ("rectangle", "circle", "fillPoly", "polylines"):
        original = getattr(real_cv2, shape_name)

        def _shape_spy(*args, _name=shape_name, _original=original, **kwargs):
            call_order.append(f"shape:{_name}")
            return _original(*args, **kwargs)

        monkeypatch.setattr(real_cv2, shape_name, _shape_spy)

    original_put_text = real_cv2.putText

    def _put_text_spy(*args, **kwargs):
        call_order.append("label:putText")
        return original_put_text(*args, **kwargs)

    monkeypatch.setattr(real_cv2, "putText", _put_text_spy)

    render_radar_frame(tracks_at_frame, config, (400, 200))

    # `_draw_pitch` also calls `putText` (the yard-line numbers), always before any
    # marker is drawn -- start from the first marker shape call (guaranteed to exist:
    # track 1 is a player with a known team_id, so it always takes the circle branch)
    # to drop those unrelated pitch-label calls from the ordering check below.
    first_shape_index = call_order.index("shape:circle")
    marker_calls = call_order[first_shape_index:]
    shape_indices = [i for i, c in enumerate(marker_calls) if c.startswith("shape:")]
    label_indices = [i for i, c in enumerate(marker_calls) if c == "label:putText"]

    # Every marker is a filled shape plus its 1px dark outline ring (two draw calls).
    assert len(shape_indices) == 6, "expected fill + outline draw per of the 3 markers"
    assert len(label_indices) == 3, "expected one label draw per of the 3 markers"
    assert max(shape_indices) < min(label_indices), (
        "every marker shape must be drawn before any marker label, so a later "
        "marker's shape can never paint over an earlier marker's own label"
    )


def test_on_field_marker_is_drawn_and_off_field_marker_is_skipped(tmp_path: Path) -> None:
    """Regression test for radar clutter from deliberately-tracked sideline/bench
    people: a marker inside the pitch + margin must be drawn, a marker well outside it
    (e.g. someone standing on the bench, off the sideline) must not be.
    """
    config = _make_config(tmp_path)
    tracks_at_frame = pl.DataFrame(
        {
            "track_id": [1, 2],
            "class_name": ["player", "player"],
            "team_id": [0, 1],
            "x_yards": [25.0, 25.0],
            "y_yards": [12.5, 40.0],  # second row is far off the 25-yard-wide pitch
        }
    )
    size_wh = (600, 300)

    frame = render_radar_frame(tracks_at_frame, config, size_wh)

    geometry = _pitch_geometry(config, size_wh)
    on_field_x, on_field_y = _yards_to_px(25.0, 12.5, geometry)
    on_field_pixel = tuple(int(c) for c in frame[on_field_y, on_field_x])
    background = tuple(int(c) for c in frame[0, 0])
    assert on_field_pixel != background, "on-field marker must be drawn"

    off_field_x, off_field_y = _yards_to_px(25.0, 40.0, geometry)
    if 0 <= off_field_y < size_wh[1] and 0 <= off_field_x < size_wh[0]:
        off_field_pixel = tuple(int(c) for c in frame[off_field_y, off_field_x])
        assert off_field_pixel == background, "off-field marker must not be drawn"


def test_rows_with_null_field_coordinates_are_skipped_without_raising(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    tracks_at_frame = pl.DataFrame(
        {
            "track_id": [1, 2],
            "class_name": ["player", "player"],
            "team_id": [0, 1],
            "x_yards": [10.0, None],
            "y_yards": [10.0, None],
        }
    )

    frame = render_radar_frame(tracks_at_frame, config, (400, 200))

    assert frame.shape == (200, 400, 3)


# --- Task 1/2: render_showcase_reel ------------------------------------------------------------


def test_render_showcase_reel_writes_frame_count_and_width_matching_the_composition(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    n_frames = 4
    _seed_clip(tmp_path, 1, n_frames)
    _seed_clip(tmp_path, 2, n_frames)
    tracks = synthetic_tracks(
        n_clips=2, n_frames=n_frames, n_tracks=2, with_field_coords=True, with_teams=True
    )
    out_path = tmp_path / "out" / "showcase.mp4"

    written = render_showcase_reel(config, [1, 2], tracks, out_path)

    assert written == out_path
    assert out_path.exists()

    capture = cv2.VideoCapture(str(out_path))
    try:
        frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
        width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
    finally:
        capture.release()

    expected_frames = n_frames * 2 + _SEPARATOR_FRAMES
    assert frame_count == expected_frames
    assert width == 64 * 2
    assert height == 48 + _HEADER_HEIGHT_PX


def test_render_showcase_reel_raises_named_exception_for_clip_without_field_coordinates(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    _seed_clip(tmp_path, 1, 3)
    tracks = synthetic_tracks(n_clips=1, n_frames=3, n_tracks=2, with_teams=True)

    with pytest.raises(NoFieldCoordinatesForClip, match="clip 1"):
        render_showcase_reel(config, [1], tracks, tmp_path / "out.mp4")


# --- `ffep cv radar` CLI default output path (PII placement, T-2.1-01) ----------------------


def test_radar_command_default_out_path_is_under_the_gitignored_label_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reel's left half is rendered player footage (PII) -- the default output
    must live under `data/labels/<session>/showcase/`, like the overlay videos, never
    under `data/processed/` (gitignored as regenerable output, not as PII).
    """
    from typer.testing import CliRunner

    from test_config import MINIMAL_TOML

    from flag_football_ep.cli import app
    from flag_football_ep.config import load_config
    from flag_football_ep.cv import radar as radar_module

    config_path = tmp_path / "ffep.toml"
    config_path.write_text(MINIMAL_TOML, encoding="utf-8")
    cfg = load_config(config_path)

    tracks_path = tmp_path / "tracks.parquet"
    synthetic_tracks(n_clips=1, n_frames=3, n_tracks=2).write_parquet(tracks_path)

    captured: dict = {}

    def _fake_render_showcase_reel(config, clip_numbers, tracks, out_path):
        captured["out_path"] = Path(out_path)
        return Path(out_path)

    monkeypatch.setattr(radar_module, "render_showcase_reel", _fake_render_showcase_reel)

    runner = CliRunner()
    result = runner.invoke(
        app, ["cv", "radar", "--config", str(config_path), "--tracks", str(tracks_path)]
    )

    assert result.exit_code == 0, result.output
    expected = cfg.paths.labels / cfg.cv.pilot_session_id / "showcase" / "showcase.mp4"
    assert captured["out_path"] == expected
    assert cfg.paths.processed not in captured["out_path"].parents
