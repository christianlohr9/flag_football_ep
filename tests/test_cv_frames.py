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
from collections import Counter
from pathlib import Path

import pytest

from flag_football_ep.config import Config, load_config
from flag_football_ep.cv.frames import (
    ClipNotFound,
    EvalSplitError,
    ManifestError,
    clip_number,
    clip_paths,
    extract_frames,
    freeze_eval_clips,
    read_eval_split,
    read_manifest,
    sample_training_frames,
    write_manifest,
)
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


# --- domain parameterisation (plan 02.2-02 Task 1) --------------------------------


def test_clip_paths_resolves_sideline_domain(tmp_path: Path, cfg: Config) -> None:
    _touch(tmp_path, "data/video/sess-1/Wide - Clip 001.mp4")
    _touch(tmp_path, "data/video/sess-1/Side - Clip 001.mp4")
    _write_inventory(
        tmp_path,
        [
            _row("data/video/sess-1/Wide - Clip 001.mp4", domain="drone"),
            _row("data/video/sess-1/Side - Clip 001.mp4", domain="sideline"),
        ],
    )

    paths = clip_paths(cfg, "sess-1", domain="sideline")

    assert [p.name for p in paths] == ["Side - Clip 001.mp4"]


def test_clip_paths_resolves_broadcast_domain(tmp_path: Path, cfg: Config) -> None:
    _touch(tmp_path, "data/video/sess-1/TV - Clip 001.mp4")
    _write_inventory(
        tmp_path, [_row("data/video/sess-1/TV - Clip 001.mp4", domain="broadcast")]
    )

    paths = clip_paths(cfg, "sess-1", domain="broadcast")

    assert [p.name for p in paths] == ["TV - Clip 001.mp4"]


def test_clip_paths_default_domain_still_drone(tmp_path: Path, cfg: Config) -> None:
    """Regression: leaving `domain` at its default must keep resolving only drone
    clips, byte-for-byte the plan 02.1-03 behaviour.
    """
    _touch(tmp_path, "data/video/sess-1/Wide - Clip 001.mp4")
    _touch(tmp_path, "data/video/sess-1/Side - Clip 001.mp4")
    _write_inventory(
        tmp_path,
        [
            _row("data/video/sess-1/Wide - Clip 001.mp4", domain="drone"),
            _row("data/video/sess-1/Side - Clip 001.mp4", domain="sideline"),
        ],
    )

    paths = clip_paths(cfg, "sess-1")

    assert [p.name for p in paths] == ["Wide - Clip 001.mp4"]


def test_clip_paths_raises_for_unknown_domain(tmp_path: Path, cfg: Config) -> None:
    _touch(tmp_path, "data/video/sess-1/Wide - Clip 001.mp4")
    _write_inventory(tmp_path, [_row("data/video/sess-1/Wide - Clip 001.mp4")])

    with pytest.raises(ClipNotFound, match="tv"):
        clip_paths(cfg, "sess-1", domain="tv")


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


# --- sample_training_frames / write_manifest / read_manifest (plan 02.1-07 Task 1) ---

_SAMPLE_SESSION_ID = "sess-1"
_SAMPLE_N_CLIPS = 8


def _make_sample_session(
    tmp_path: Path,
    session_id: str,
    n_clips: int,
    *,
    duration: float = 2.0,
    fps: int = 24,
    domain: str = "drone",
) -> None:
    rows = []
    for i in range(1, n_clips + 1):
        rel_path = f"data/video/{session_id}/Wide - Clip {i:03d}.mp4"
        _make_synthetic_clip(tmp_path / rel_path, duration=duration, fps=fps)
        row = _row(rel_path, session_id=session_id, domain=domain)
        row["duration_seconds"] = str(duration)
        row["fps"] = str(float(fps))
        rows.append(row)
    _write_inventory(tmp_path, rows)


def _write_hover_positions(tmp_path: Path, mapping: dict[int, str]) -> None:
    lines = [
        "clip_number,clip_path,hover_position_id,apparent_player_px_p10,"
        "apparent_player_px_p50,tier,notes"
    ]
    for n, hp in sorted(mapping.items()):
        lines.append(f"{n},,{hp},0.0,0.0,Brauchbar,")
    path = tmp_path / "data" / "reference" / "hover_positions.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture
def sample_session(tmp_path: Path) -> str:
    _make_sample_session(tmp_path, _SAMPLE_SESSION_ID, _SAMPLE_N_CLIPS)
    mapping = {i: ("hp-01" if i <= 4 else "hp-02") for i in range(1, _SAMPLE_N_CLIPS + 1)}
    _write_hover_positions(tmp_path, mapping)
    return _SAMPLE_SESSION_ID


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_sample_training_frames_is_deterministic(
    tmp_path: Path, cfg: Config, sample_session: str
) -> None:
    out_dir = tmp_path / "out"
    manifest_path = out_dir / "manifest.json"

    first = sample_training_frames(cfg, sample_session, target=24, seed=20260516, out_dir=out_dir)
    write_manifest(first, manifest_path)
    first_bytes = manifest_path.read_bytes()

    second = sample_training_frames(cfg, sample_session, target=24, seed=20260516, out_dir=out_dir)
    write_manifest(second, manifest_path)
    second_bytes = manifest_path.read_bytes()

    assert first_bytes == second_bytes


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_sample_training_frames_respects_target_and_per_clip_bounds(
    tmp_path: Path, cfg: Config, sample_session: str
) -> None:
    out_dir = tmp_path / "out"
    manifest = sample_training_frames(cfg, sample_session, target=24, seed=1, out_dir=out_dir)

    assert abs(len(manifest.frames) - 24) <= max(1, round(24 * 0.10))
    counts = Counter(frame.clip_number for frame in manifest.frames)
    assert set(counts) == set(range(1, _SAMPLE_N_CLIPS + 1))
    for count in counts.values():
        assert 3 <= count <= 12


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_sample_training_frames_split_is_clip_level_no_leak(
    tmp_path: Path, cfg: Config, sample_session: str
) -> None:
    out_dir = tmp_path / "out"
    manifest = sample_training_frames(cfg, sample_session, target=24, seed=1, out_dir=out_dir)

    val_clips = {n for n, split in manifest.split.items() if split == "val"}
    train_clips = {n for n, split in manifest.split.items() if split == "train"}

    assert val_clips.isdisjoint(train_clips)
    assert len(val_clips) >= 6
    for frame in manifest.frames:
        assert frame.split == manifest.split[frame.clip_number]


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_sample_training_frames_stratifies_by_hover_position(tmp_path: Path, cfg: Config) -> None:
    session_id = "sess-2"
    durations = {1: 6.0, 2: 6.0, 3: 2.0, 4: 2.0}
    rows = []
    for n, duration in durations.items():
        rel_path = f"data/video/{session_id}/Wide - Clip {n:03d}.mp4"
        _make_synthetic_clip(tmp_path / rel_path, duration=duration, fps=24)
        row = _row(rel_path, session_id=session_id)
        row["duration_seconds"] = str(duration)
        row["fps"] = "24.0"
        rows.append(row)
    _write_inventory(tmp_path, rows)
    _write_hover_positions(tmp_path, {1: "hp-01", 2: "hp-01", 3: "hp-02", 4: "hp-02"})

    out_dir = tmp_path / "out"
    manifest = sample_training_frames(cfg, session_id, target=32, seed=1, out_dir=out_dir)

    counts = Counter(frame.clip_number for frame in manifest.frames)
    hp01_total = counts[1] + counts[2]
    hp02_total = counts[3] + counts[4]
    assert hp01_total > hp02_total


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_sample_training_frames_frame_index_matches_written_filename(
    tmp_path: Path, cfg: Config, sample_session: str
) -> None:
    out_dir = tmp_path / "out"
    manifest = sample_training_frames(cfg, sample_session, target=24, seed=1, out_dir=out_dir)

    assert manifest.frames
    for frame in manifest.frames:
        image_path = Path(frame.image_path)
        assert image_path.exists()
        assert image_path.name.endswith(f"_f{frame.frame_index:05d}.jpg")


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_sample_training_frames_different_seed_changes_sample(
    tmp_path: Path, cfg: Config, sample_session: str
) -> None:
    out_dir_a = tmp_path / "out-a"
    out_dir_b = tmp_path / "out-b"

    manifest_a = sample_training_frames(cfg, sample_session, target=24, seed=1, out_dir=out_dir_a)
    manifest_b = sample_training_frames(cfg, sample_session, target=24, seed=2, out_dir=out_dir_b)

    timestamps_a = sorted(frame.timestamp_s for frame in manifest_a.frames)
    timestamps_b = sorted(frame.timestamp_s for frame in manifest_b.frames)
    assert timestamps_a != timestamps_b


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_write_manifest_then_read_manifest_round_trips(
    tmp_path: Path, cfg: Config, sample_session: str
) -> None:
    out_dir = tmp_path / "out"
    manifest = sample_training_frames(cfg, sample_session, target=24, seed=1, out_dir=out_dir)
    path = write_manifest(manifest, out_dir / "manifest.json")

    loaded = read_manifest(path)

    assert loaded.session_id == manifest.session_id
    assert loaded.seed == manifest.seed
    assert loaded.target == manifest.target
    assert loaded.split == manifest.split
    assert [f.image_path for f in loaded.frames] == [f.image_path for f in manifest.frames]
    assert [f.frame_index for f in loaded.frames] == [f.frame_index for f in manifest.frames]


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_write_manifest_is_atomic_no_leftover_tmp_file(
    tmp_path: Path, cfg: Config, sample_session: str
) -> None:
    out_dir = tmp_path / "out"
    manifest = sample_training_frames(cfg, sample_session, target=24, seed=1, out_dir=out_dir)
    manifest_path = out_dir / "manifest.json"

    written_path = write_manifest(manifest, manifest_path)

    assert written_path == manifest_path
    assert manifest_path.exists()
    assert not manifest_path.with_suffix(manifest_path.suffix + ".tmp").exists()


def test_read_manifest_raises_for_missing_path(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.json"

    with pytest.raises(ManifestError, match="does-not-exist.json"):
        read_manifest(missing)


def test_read_manifest_raises_for_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text("{not valid json", encoding="utf-8")

    with pytest.raises(ManifestError):
        read_manifest(path)


def test_read_manifest_raises_for_non_integer_seed(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        '{"session_id": "s", "seed": "not-an-int", "target": 4, "split": {}, "frames": []}',
        encoding="utf-8",
    )

    with pytest.raises(ManifestError, match="non-integer"):
        read_manifest(path)


# --- domain parameterisation (plan 02.2-02 Task 1) --------------------------------


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_sample_training_frames_resolves_sideline_domain(tmp_path: Path, cfg: Config) -> None:
    session_id = "sess-sideline"
    _make_sample_session(tmp_path, session_id, 4, domain="sideline")
    out_dir = tmp_path / "out"

    manifest = sample_training_frames(
        cfg, session_id, target=12, seed=1, out_dir=out_dir, domain="sideline"
    )

    assert manifest.frames
    counts = Counter(frame.clip_number for frame in manifest.frames)
    assert set(counts) == {1, 2, 3, 4}


@pytest.mark.skipif(not FFMPEG_AVAILABLE, reason="ffmpeg/ffprobe not on PATH")
def test_sample_training_frames_no_hover_positions_falls_back_to_single_bucket(
    tmp_path: Path, cfg: Config
) -> None:
    """Non-drone domains without a `hover_positions.csv` entry group under the single
    `"all"` bucket -- the existing `hover_ids.get(n, "all")` fallback, not a new code
    path.
    """
    session_id = "sess-broadcast"
    _make_sample_session(tmp_path, session_id, 4, domain="broadcast")
    out_dir = tmp_path / "out"
    assert not (tmp_path / "data" / "reference" / "hover_positions.csv").exists()

    manifest = sample_training_frames(
        cfg, session_id, target=12, seed=1, out_dir=out_dir, domain="broadcast"
    )

    assert manifest.frames
    counts = Counter(frame.clip_number for frame in manifest.frames)
    assert set(counts) == {1, 2, 3, 4}


# --- freeze_eval_clips / read_eval_split (plan 02.2-06 Task 1, D-04/D-07/D-13) -----


def _write_sighting_csv(tmp_path: Path, session_id: str, mapping: dict[int, str]) -> None:
    """Write a `sighting_{session_id}.csv` (the non-drone stratum source
    `_read_stratum_ids` reads) with the same header `_write_hover_positions`
    writes -- both files share the `hover_position_id` column name and schema.
    """
    lines = [
        "clip_number,clip_path,hover_position_id,apparent_player_px_p10,"
        "apparent_player_px_p50,tier,notes"
    ]
    for n, hp in sorted(mapping.items()):
        lines.append(f"{n},,{hp},0.0,0.0,Brauchbar,")
    path = tmp_path / "data" / "reference" / f"sighting_{session_id}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _make_eval_session(
    tmp_path: Path,
    session_id: str,
    n_clips: int,
    *,
    domain: str = "drone",
    existing_rows: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    """Register `n_clips` clip files (no ffmpeg needed -- `freeze_eval_clips` never
    opens the video, only `Path.exists()` via `clip_paths`) for `session_id`/`domain`
    and write `video_inventory.csv`. Returns the accumulated row list so a test can
    combine two domains' clips into one inventory file across two calls.
    """
    rows = list(existing_rows or [])
    for i in range(1, n_clips + 1):
        rel_path = f"data/video/{session_id}/Wide - Clip {i:03d}.mp4"
        _touch(tmp_path, rel_path)
        rows.append(_row(rel_path, session_id=session_id, domain=domain))
    _write_inventory(tmp_path, rows)
    return rows


def test_freeze_eval_clips_stratifies_by_hover_position(tmp_path: Path, cfg: Config) -> None:
    session_id = "sess-drone"
    _make_eval_session(tmp_path, session_id, 20, domain="drone")
    _write_hover_positions(tmp_path, {n: ("hp-01" if n <= 10 else "hp-02") for n in range(1, 21)})
    out_csv = tmp_path / "frozen_eval_clips.csv"

    split = freeze_eval_clips(cfg, ["drone"], 0.3, 20260516, out_csv)

    assert split.clips_by_domain["drone"]
    assert len(split.clips_by_domain["drone"]) == 6  # round(10*0.3)*2 strata

    df_rows = out_csv.read_text(encoding="utf-8").splitlines()[1:]
    eval_rows = [r for r in df_rows if r.split(",")[4] == "frozen_eval"]
    hp01_eval = sum(1 for r in eval_rows if int(r.split(",")[2]) <= 10)
    hp02_eval = sum(1 for r in eval_rows if int(r.split(",")[2]) > 10)
    assert hp01_eval == 3
    assert hp02_eval == 3


def test_freeze_eval_clips_is_deterministic_for_same_seed(tmp_path: Path, cfg: Config) -> None:
    session_id = "sess-drone"
    _make_eval_session(tmp_path, session_id, 12, domain="drone")
    _write_hover_positions(tmp_path, {n: "hp-01" for n in range(1, 13)})
    out_csv = tmp_path / "frozen_eval_clips.csv"

    freeze_eval_clips(cfg, ["drone"], 0.25, 1, out_csv)
    first_bytes = out_csv.read_bytes()

    freeze_eval_clips(cfg, ["drone"], 0.25, 1, out_csv)
    second_bytes = out_csv.read_bytes()

    assert first_bytes == second_bytes


def test_freeze_eval_clips_raises_for_seed_mismatch(tmp_path: Path, cfg: Config) -> None:
    rows = _make_eval_session(tmp_path, "sess-drone", 12, domain="drone")
    _write_hover_positions(tmp_path, {n: "hp-01" for n in range(1, 13)})
    _make_eval_session(tmp_path, "sess-sideline", 8, domain="sideline", existing_rows=rows)
    _write_sighting_csv(tmp_path, "sess-sideline", {n: "hp-01" for n in range(1, 9)})
    out_csv = tmp_path / "frozen_eval_clips.csv"

    freeze_eval_clips(cfg, ["drone"], 0.3, 1, out_csv)

    with pytest.raises(EvalSplitError, match="different seed"):
        freeze_eval_clips(cfg, ["sideline"], 0.2, 2, out_csv)


def test_freeze_eval_clips_raises_for_too_few_clips(tmp_path: Path, cfg: Config) -> None:
    _make_eval_session(tmp_path, "sess-tiny", 4, domain="drone")
    _write_hover_positions(tmp_path, {n: "hp-01" for n in range(1, 5)})
    out_csv = tmp_path / "frozen_eval_clips.csv"

    with pytest.raises(EvalSplitError, match="drone.*4"):
        freeze_eval_clips(cfg, ["drone"], 0.3, 1, out_csv)


def test_freeze_eval_clips_pool_and_frozen_eval_partition_domain(
    tmp_path: Path, cfg: Config
) -> None:
    _make_eval_session(tmp_path, "sess-drone", 15, domain="drone")
    _write_hover_positions(tmp_path, {n: "hp-01" for n in range(1, 16)})
    out_csv = tmp_path / "frozen_eval_clips.csv"

    freeze_eval_clips(cfg, ["drone"], 0.3, 1, out_csv)

    lines = out_csv.read_text(encoding="utf-8").splitlines()[1:]
    clip_numbers = [int(line.split(",")[2]) for line in lines]
    roles = [line.split(",")[4] for line in lines]

    assert sorted(clip_numbers) == list(range(1, 16))
    assert len(clip_numbers) == len(set(clip_numbers))  # no clip listed twice
    assert set(roles) <= {"frozen_eval", "pool"}


def test_freeze_eval_clips_second_domain_appends_without_touching_first(
    tmp_path: Path, cfg: Config
) -> None:
    rows = _make_eval_session(tmp_path, "sess-drone", 12, domain="drone")
    _write_hover_positions(tmp_path, {n: "hp-01" for n in range(1, 13)})
    _make_eval_session(tmp_path, "sess-sideline", 10, domain="sideline", existing_rows=rows)
    _write_sighting_csv(tmp_path, "sess-sideline", {n: "hp-01" for n in range(1, 11)})
    out_csv = tmp_path / "frozen_eval_clips.csv"

    freeze_eval_clips(cfg, ["drone"], 0.3, 20260516, out_csv)
    drone_bytes_before = out_csv.read_bytes()
    drone_lines_before = [
        line for line in drone_bytes_before.decode("utf-8").splitlines() if line.startswith("drone,")
    ]

    split = freeze_eval_clips(cfg, ["sideline"], 0.2, 20260516, out_csv)

    all_lines = out_csv.read_text(encoding="utf-8").splitlines()
    drone_lines_after = [line for line in all_lines if line.startswith("drone,")]
    sideline_lines_after = [line for line in all_lines if line.startswith("sideline,")]

    assert drone_lines_after == drone_lines_before  # first domain's rows untouched
    assert len(sideline_lines_after) == 10
    assert split.clips_by_domain == {"sideline": sorted(
        int(line.split(",")[2])
        for line in sideline_lines_after
        if line.split(",")[4] == "frozen_eval"
    )}
    assert len(split.clips_by_domain["sideline"]) == 2  # round(10 * 0.2)


def test_freeze_eval_clips_private_test_only_on_drone(tmp_path: Path, cfg: Config) -> None:
    rows = _make_eval_session(tmp_path, "sess-drone", 12, domain="drone")
    _write_hover_positions(tmp_path, {n: "hp-01" for n in range(1, 13)})
    _make_eval_session(tmp_path, "sess-sideline", 8, domain="sideline", existing_rows=rows)
    _write_sighting_csv(tmp_path, "sess-sideline", {n: "hp-01" for n in range(1, 9)})
    out_csv = tmp_path / "frozen_eval_clips.csv"

    freeze_eval_clips(cfg, ["drone"], 0.3, 1, out_csv)
    freeze_eval_clips(cfg, ["sideline"], 0.2, 1, out_csv)

    lines = out_csv.read_text(encoding="utf-8").splitlines()[1:]
    for line in lines:
        fields = line.split(",")
        domain, role, private_test = fields[0], fields[4], fields[5]
        if role == "frozen_eval" and domain == "drone":
            assert private_test == "true"
        else:
            assert private_test == "false"


def test_read_eval_split_round_trips(tmp_path: Path, cfg: Config) -> None:
    _make_eval_session(tmp_path, "sess-drone", 12, domain="drone")
    _write_hover_positions(tmp_path, {n: "hp-01" for n in range(1, 13)})
    out_csv = tmp_path / "frozen_eval_clips.csv"

    written = freeze_eval_clips(cfg, ["drone"], 0.25, 1, out_csv)
    loaded = read_eval_split(out_csv)

    assert loaded.clips_by_domain == written.clips_by_domain
    assert loaded.seed == written.seed


def test_read_eval_split_raises_for_missing_path(tmp_path: Path) -> None:
    with pytest.raises(EvalSplitError, match="not found"):
        read_eval_split(tmp_path / "does-not-exist.csv")
