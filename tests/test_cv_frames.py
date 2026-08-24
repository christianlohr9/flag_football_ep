"""Tests for `flag_football_ep.cv.frames`: clip discovery, clip-number parsing, and
ffmpeg frame extraction (plan 02.1-03 Task 1).

`clip_paths`/`extract_frames` are exercised against a tmp_path config whose
`data/reference/video_inventory.csv` mirrors `tests/test_capture_artifacts.py`'s
`INVENTORY_COLUMNS` header exactly. `extract_frames` is exercised against tiny
synthetic clips generated with ffmpeg's `lavfi`/`color` source -- never real
footage, per RESEARCH's Validation Architecture rule. `clip_paths` fixtures that
never reach ffmpeg use empty placeholder files, since only `Path.exists()` matters
for discovery.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from flag_football_ep.config import Config, load_config
from flag_football_ep.cv.frames import ClipNotFound, clip_number, clip_paths, extract_frames
from test_config import MINIMAL_TOML

FFMPEG_AVAILABLE = shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None

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


def _write_inventory(tmp_path: Path, rows: list[dict[str, str]]) -> Path:
    lines = [_INVENTORY_HEADER]
    for row in rows:
        lines.append(",".join(row.get(field, "") for field in _INVENTORY_FIELDS))
    inventory_path = tmp_path / "data" / "reference" / "video_inventory.csv"
    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    inventory_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return inventory_path


def _row(local_path: str, *, session_id: str = "sess-1", domain: str = "drone") -> dict[str, str]:
    return {
        "domain": domain,
        "session_id": session_id,
        "game_id": "",
        "capture_date": "2026-05-16",
        "resolution": "1920x1080",
        "fps": "30.0",
        "duration_seconds": "10.0",
        "local_path": local_path,
        "content_sha256": "",
        "notes": "",
    }


def _touch(tmp_path: Path, relative: str) -> None:
    path = tmp_path / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"fake-clip")


def _make_synthetic_clip(path: Path, *, duration: float = 2.0, fps: int = 24) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"color=c=blue:s=64x36:d={duration}:r={fps}",
            "-pix_fmt",
            "yuv420p",
            str(path),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_clip_paths_orders_by_clip_number(tmp_path: Path, cfg: Config) -> None:
    _touch(tmp_path, "data/video/sess-1/Wide - Clip 003.mp4")
    _touch(tmp_path, "data/video/sess-1/Wide - Clip 001.mp4")
    _touch(tmp_path, "data/video/sess-1/Wide - Clip 002.mp4")
    _write_inventory(
        tmp_path,
        [
            _row("data/video/sess-1/Wide - Clip 003.mp4"),
            _row("data/video/sess-1/Wide - Clip 001.mp4"),
            _row("data/video/sess-1/Wide - Clip 002.mp4"),
        ],
    )

    paths = clip_paths(cfg, "sess-1")

    assert [p.name for p in paths] == [
        "Wide - Clip 001.mp4",
        "Wide - Clip 002.mp4",
        "Wide - Clip 003.mp4",
    ]


def test_clip_paths_ignores_non_drone_domain(tmp_path: Path, cfg: Config) -> None:
    _touch(tmp_path, "data/video/sess-1/Wide - Clip 001.mp4")
    _touch(tmp_path, "data/video/sess-1/Sideline - Clip 001.mp4")
    _write_inventory(
        tmp_path,
        [
            _row("data/video/sess-1/Wide - Clip 001.mp4"),
            _row("data/video/sess-1/Sideline - Clip 001.mp4", domain="sideline"),
        ],
    )

    paths = clip_paths(cfg, "sess-1")

    assert [p.name for p in paths] == ["Wide - Clip 001.mp4"]


def test_clip_paths_raises_for_unknown_session(tmp_path: Path, cfg: Config) -> None:
    _touch(tmp_path, "data/video/sess-1/Wide - Clip 001.mp4")
    _write_inventory(tmp_path, [_row("data/video/sess-1/Wide - Clip 001.mp4")])

    with pytest.raises(ClipNotFound, match="unknown-session"):
        clip_paths(cfg, "unknown-session")


def test_clip_paths_rejects_absolute_local_path(tmp_path: Path, cfg: Config) -> None:
    _write_inventory(tmp_path, [_row("/Users/x/clip.mp4")])

    with pytest.raises(ClipNotFound, match=r"/Users/x/clip\.mp4"):
        clip_paths(cfg, "sess-1")


def test_clip_paths_rejects_path_escaping_repo_root(tmp_path: Path, cfg: Config) -> None:
    _write_inventory(tmp_path, [_row("../outside/clip.mp4")])

    with pytest.raises(ClipNotFound, match="escapes the repo root"):
        clip_paths(cfg, "sess-1")


def test_clip_paths_raises_when_registered_file_missing(tmp_path: Path, cfg: Config) -> None:
    _write_inventory(tmp_path, [_row("data/video/sess-1/Wide - Clip 001.mp4")])

    with pytest.raises(ClipNotFound, match="does not exist"):
        clip_paths(cfg, "sess-1")


def test_clip_number_round_trips_hudl_pattern() -> None:
    assert clip_number(Path("Wide - Clip 001.mp4")) == 1
    assert clip_number(Path("Wide - Clip 061.mp4")) == 61


def test_clip_number_raises_without_trailing_digits() -> None:
    with pytest.raises(ClipNotFound, match="Wide - Clip.mp4"):
        clip_number(Path("Wide - Clip.mp4"))


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_extract_frames_writes_one_file_per_timestamp(tmp_path: Path) -> None:
    clip = tmp_path / "clip.mp4"
    _make_synthetic_clip(clip, duration=2.0, fps=24)
    out_dir = tmp_path / "frames"

    written = extract_frames(clip, out_dir, [0.1, 1.0])

    assert len(written) == 2
    for path in written:
        assert path.exists()
        assert path.parent == out_dir
        assert path.name.startswith("clip_f")
        assert path.suffix == ".jpg"


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_extract_frames_creates_out_dir_if_absent(tmp_path: Path) -> None:
    clip = tmp_path / "clip.mp4"
    _make_synthetic_clip(clip, duration=1.0, fps=24)
    out_dir = tmp_path / "does" / "not" / "exist"
    assert not out_dir.exists()

    written = extract_frames(clip, out_dir, [0.0])

    assert out_dir.exists()
    assert written[0].exists()
