"""Tests for `flag_football_ep.cv.sighting`: hover-position grouping, apparent-size
measurement, tier classification and inference-setting recommendation (plan 02.1-03
Task 2).

Every clip fixture is synthetic, generated with numpy/cv2 (`cv2.VideoWriter`) -- never
real footage, per RESEARCH's Validation Architecture rule. Skips whole-module if the
`cv` extras group is not installed (`uv sync --extra cv`).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

cv2 = pytest.importorskip("cv2")

from flag_football_ep.config import Config, load_config
from flag_football_ep.cv.sighting import (
    ClipSighting,
    InferenceRecommendation,
    SightingResult,
    _apparent_player_heights,
    _classify_tier,
    _framing_fingerprint,
    _group_by_framing,
    _normalized_cross_correlation,
    recommend_inference_settings,
    sight_session,
)
from test_config import MINIMAL_TOML

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


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(MINIMAL_TOML, encoding="utf-8")
    return load_config(config_path)


def _row(local_path: str, *, session_id: str = "sess-1", resolution: str = "1920x1080") -> dict[str, str]:
    return {
        "domain": "drone",
        "session_id": session_id,
        "game_id": "",
        "capture_date": "2026-05-16",
        "resolution": resolution,
        "fps": "20.0",
        "duration_seconds": "2.0",
        "local_path": local_path,
        "content_sha256": "",
        "notes": "",
    }


def _write_inventory(tmp_path: Path, rows: list[dict[str, str]]) -> Path:
    lines = [_INVENTORY_HEADER]
    for row in rows:
        lines.append(",".join(row.get(field, "") for field in _INVENTORY_FIELDS))
    inventory_path = tmp_path / "data" / "reference" / "video_inventory.csv"
    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    inventory_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return inventory_path


def _write_static_clip(
    path: Path, *, seed: int, size: tuple[int, int] = (128, 72), n_frames: int = 6, fps: float = 10.0
) -> None:
    """A clip whose every frame is the identical seeded-noise pattern -- fixed framing,
    no motion. Same `seed` -> (near-)identical fingerprint; different `seed` -> a
    distinctly different, uncorrelated fingerprint.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    pattern = rng.integers(0, 255, size=(size[1], size[0], 3), dtype=np.uint8)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(path), fourcc, fps, size)
    for _ in range(n_frames):
        writer.write(pattern)
    writer.release()


def _write_moving_rectangle_clip(
    path: Path,
    *,
    rect_height: int,
    size: tuple[int, int] = (160, 120),
    n_frames: int = 40,
    fps: float = 20.0,
    background: int = 60,
) -> None:
    """A clip with a fixed-height rectangle sweeping horizontally across a static
    background -- the moving foreground blob `_apparent_player_heights` must recover.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    width, height = size
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(path), fourcc, fps, size)
    y = height // 2 - rect_height // 2
    rect_width = 14
    for i in range(n_frames):
        frame = np.full((height, width, 3), background, dtype=np.uint8)
        x = 20 + (i * 7) % (width - 40)
        cv2.rectangle(frame, (x, y), (x + rect_width, y + rect_height), (230, 230, 230), -1)
        writer.write(frame)
    writer.release()


# --- hover-position grouping -----------------------------------------------------


def test_group_by_framing_same_background_yields_one_group() -> None:
    fp = np.random.default_rng(1).random(100)
    fingerprints = {1: fp.copy(), 2: fp.copy(), 3: fp.copy()}

    groups = _group_by_framing(fingerprints, threshold=0.97)

    assert len(set(groups.values())) == 1
    assert set(groups) == {1, 2, 3}


def test_group_by_framing_distinct_backgrounds_yield_two_groups() -> None:
    fp_a = np.random.default_rng(1).random(100)
    fp_b = np.random.default_rng(2).random(100) + 50.0

    fingerprints = {1: fp_a.copy(), 2: fp_b.copy(), 3: fp_a.copy()}

    groups = _group_by_framing(fingerprints, threshold=0.97)

    assert groups[1] == groups[3]
    assert groups[1] != groups[2]
    assert len(set(groups.values())) == 2


def test_group_by_framing_ids_ordered_by_lowest_clip_number() -> None:
    fp_a = np.random.default_rng(1).random(100)
    fp_b = np.random.default_rng(2).random(100) + 50.0
    # Clip 5 (pattern B) appears before clip 1 (pattern A) is even considered as a
    # dict entry, but group numbering must still follow ascending clip number.
    fingerprints = {5: fp_b.copy(), 1: fp_a.copy(), 3: fp_a.copy()}

    groups = _group_by_framing(fingerprints, threshold=0.97)

    assert groups[1] == "hp-01"
    assert groups[3] == "hp-01"
    assert groups[5] == "hp-02"


def test_group_by_framing_stable_across_input_ordering_and_reruns() -> None:
    fp_a = np.random.default_rng(1).random(100)
    fp_b = np.random.default_rng(2).random(100) + 50.0

    order_1 = {1: fp_a.copy(), 2: fp_b.copy(), 3: fp_a.copy()}
    order_2 = {3: fp_a.copy(), 1: fp_a.copy(), 2: fp_b.copy()}

    groups_1 = _group_by_framing(order_1, threshold=0.97)
    groups_2 = _group_by_framing(order_2, threshold=0.97)
    groups_1_rerun = _group_by_framing(order_1, threshold=0.97)

    assert groups_1 == groups_2
    assert groups_1 == groups_1_rerun


def test_normalized_cross_correlation_identical_is_one() -> None:
    a = np.random.default_rng(3).random(50)
    assert _normalized_cross_correlation(a, a.copy()) == pytest.approx(1.0)


def test_framing_fingerprint_same_pattern_correlates_above_threshold(tmp_path: Path) -> None:
    clip_a1 = tmp_path / "a1.mp4"
    clip_a2 = tmp_path / "a2.mp4"
    _write_static_clip(clip_a1, seed=7)
    _write_static_clip(clip_a2, seed=7)

    fp1 = _framing_fingerprint(clip_a1)
    fp2 = _framing_fingerprint(clip_a2)

    assert _normalized_cross_correlation(fp1, fp2) >= 0.97


def test_framing_fingerprint_distinct_patterns_correlate_below_threshold(tmp_path: Path) -> None:
    clip_a = tmp_path / "a.mp4"
    clip_b = tmp_path / "b.mp4"
    _write_static_clip(clip_a, seed=7)
    _write_static_clip(clip_b, seed=99)

    fp_a = _framing_fingerprint(clip_a)
    fp_b = _framing_fingerprint(clip_b)

    assert _normalized_cross_correlation(fp_a, fp_b) < 0.97


# --- apparent-size measurement ----------------------------------------------------


def test_apparent_player_heights_recovers_known_height_within_tolerance(tmp_path: Path) -> None:
    clip = tmp_path / "moving.mp4"
    known_height = 24
    _write_moving_rectangle_clip(clip, rect_height=known_height)

    p10, p50, n_samples = _apparent_player_heights(clip)

    assert n_samples > 0
    assert p50 == pytest.approx(known_height, rel=0.15)


def test_apparent_player_heights_returns_empty_for_static_clip(tmp_path: Path) -> None:
    clip = tmp_path / "static.mp4"
    _write_static_clip(clip, seed=1, n_frames=10)

    p10, p50, n_samples = _apparent_player_heights(clip)

    assert n_samples == 0
    assert p10 == 0.0
    assert p50 == 0.0


# --- tier classification -----------------------------------------------------------


def test_classify_tier_uses_capture_protocol_vocabulary() -> None:
    assert _classify_tier("3840x2160", 10.0) == "Ideal"
    assert _classify_tier("2560x1440", 10.0) == "Brauchbar"
    assert _classify_tier("1920x1080", 15.0) == "Unbrauchbar"
    assert _classify_tier("1920x1080", 45.0) == "Brauchbar"


# --- recommend_inference_settings --------------------------------------------------


@pytest.mark.parametrize(
    "p50,p10,expected_resolution,expected_sahi",
    [
        (45.0, 40.0, 672, False),
        (40.0, 35.0, 672, False),
        (30.0, 25.0, 896, False),
        (20.0, 15.0, 896, False),
        (15.0, 10.0, 896, True),
    ],
)
def test_recommend_inference_settings_bands(
    p50: float, p10: float, expected_resolution: int, expected_sahi: bool, cfg: Config
) -> None:
    rows = [ClipSighting(1, "clip.mp4", "hp-01", p50, p10, "Brauchbar", "")]

    rec = recommend_inference_settings(rows, cfg)

    assert rec.resolution == expected_resolution
    assert rec.sahi is expected_sahi
    assert rec.resolution % 224 == 0
    assert isinstance(rec, InferenceRecommendation)


def test_recommend_inference_settings_sahi_rationale_cites_measurement() -> None:
    rows = [ClipSighting(1, "clip.mp4", "hp-01", 15.0, 10.0, "Unbrauchbar", "")]
    cfg_stub = None  # config is unused by the recommendation logic itself

    rec = recommend_inference_settings(rows, cfg_stub)  # type: ignore[arg-type]

    assert "15.0" in rec.rationale
    assert "10.0" in rec.rationale


# --- sight_session end-to-end -------------------------------------------------------


def test_sight_session_writes_exact_header_and_no_absolute_paths(tmp_path: Path, cfg: Config) -> None:
    clip1 = tmp_path / "data" / "video" / "sess-1" / "Wide - Clip 001.mp4"
    clip2 = tmp_path / "data" / "video" / "sess-1" / "Wide - Clip 002.mp4"
    _write_static_clip(clip1, seed=11, n_frames=6)
    _write_static_clip(clip2, seed=12, n_frames=6)
    _write_inventory(
        tmp_path,
        [
            _row("data/video/sess-1/Wide - Clip 001.mp4"),
            _row("data/video/sess-1/Wide - Clip 002.mp4"),
        ],
    )

    result = sight_session(cfg, "sess-1")

    assert isinstance(result, SightingResult)
    content = result.csv_path.read_text(encoding="utf-8")
    lines = content.splitlines()
    assert lines[0] == (
        "clip_number,clip_path,hover_position_id,apparent_player_px_p10,"
        "apparent_player_px_p50,tier,notes"
    )
    assert "/Users/" not in content
    assert not any(line.startswith("/") for line in lines[1:] if line)
    assert len(result.rows) == 2


def test_sight_session_is_deterministic_across_reruns(tmp_path: Path, cfg: Config) -> None:
    clip1 = tmp_path / "data" / "video" / "sess-1" / "Wide - Clip 001.mp4"
    clip2 = tmp_path / "data" / "video" / "sess-1" / "Wide - Clip 002.mp4"
    _write_static_clip(clip1, seed=21, n_frames=6)
    _write_static_clip(clip2, seed=21, n_frames=6)
    _write_inventory(
        tmp_path,
        [
            _row("data/video/sess-1/Wide - Clip 001.mp4"),
            _row("data/video/sess-1/Wide - Clip 002.mp4"),
        ],
    )

    first = sight_session(cfg, "sess-1")
    first_bytes = first.csv_path.read_bytes()
    second = sight_session(cfg, "sess-1")
    second_bytes = second.csv_path.read_bytes()

    assert first_bytes == second_bytes


def test_sight_session_groups_clips_by_framing(tmp_path: Path, cfg: Config) -> None:
    clip1 = tmp_path / "data" / "video" / "sess-1" / "Wide - Clip 001.mp4"
    clip2 = tmp_path / "data" / "video" / "sess-1" / "Wide - Clip 002.mp4"
    clip3 = tmp_path / "data" / "video" / "sess-1" / "Wide - Clip 003.mp4"
    _write_static_clip(clip1, seed=31, n_frames=6)
    _write_static_clip(clip2, seed=31, n_frames=6)  # same framing as clip 1
    _write_static_clip(clip3, seed=32, n_frames=6)  # distinct framing
    _write_inventory(
        tmp_path,
        [
            _row("data/video/sess-1/Wide - Clip 001.mp4"),
            _row("data/video/sess-1/Wide - Clip 002.mp4"),
            _row("data/video/sess-1/Wide - Clip 003.mp4"),
        ],
    )

    result = sight_session(cfg, "sess-1")

    by_number = {r.clip_number: r for r in result.rows}
    assert by_number[1].hover_position_id == by_number[2].hover_position_id
    assert by_number[1].hover_position_id != by_number[3].hover_position_id
