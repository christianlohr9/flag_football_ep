"""Mechanical gates for the hand-maintained Phase 2.0 capture artifacts.

These tests guard the two hand-edited reference CSVs (`video_inventory.csv`,
`video_sync.csv`), the `.gitignore` block for `data/video/`, and the
"documentation, no pipeline code" tier boundary of Phase 2.0. Header drift on
either CSV, or a dtype drift between `video_sync.csv` and the canonical
`game_id`/`play_id` types `plays.parquet` uses, must fail loudly here rather
than surfacing later as a silent zero-row join (RESEARCH Pitfall 1). PII
leaks (tracked video binaries, local user paths, roster names) and an
accidental video/CV dependency creeping into `pyproject.toml` are guarded the
same way.
"""

from __future__ import annotations

import re
import subprocess
import tomllib
from pathlib import Path

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
REFERENCE_DIR = REPO_ROOT / "data" / "reference"
INVENTORY_CSV = REFERENCE_DIR / "video_inventory.csv"
SYNC_CSV = REFERENCE_DIR / "video_sync.csv"

INVENTORY_COLUMNS: tuple[str, ...] = (
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
SYNC_COLUMNS: tuple[str, ...] = (
    "game_id",
    "play_id",
    "video_file",
    "snap_frame",
    "snap_seconds",
    "end_seconds",
    "notes",
)

INVENTORY_SCHEMA: dict[str, pl.DataType] = {
    "domain": pl.Utf8,
    "session_id": pl.Utf8,
    "game_id": pl.Utf8,
    "capture_date": pl.Utf8,
    "resolution": pl.Utf8,
    "fps": pl.Float64,
    "duration_seconds": pl.Float64,
    "local_path": pl.Utf8,
    "content_sha256": pl.Utf8,
    "notes": pl.Utf8,
}
SYNC_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "play_id": pl.Int32,
    "video_file": pl.Utf8,
    "snap_frame": pl.Int64,
    "snap_seconds": pl.Float64,
    "end_seconds": pl.Float64,
    "notes": pl.Utf8,
}

DOMAIN_VOCABULARY = frozenset({"drone", "sideline", "broadcast"})
VIDEO_SUFFIXES: tuple[str, ...] = (
    ".mp4",
    ".mov",
    ".mkv",
    ".avi",
    ".m4v",
    ".mts",
    ".insv",
    ".lrf",
    ".braw",
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

_CAPTURE_CSVS: tuple[Path, ...] = (INVENTORY_CSV, SYNC_CSV)
_EXPECTED_HEADERS: dict[Path, tuple[str, ...]] = {
    INVENTORY_CSV: INVENTORY_COLUMNS,
    SYNC_CSV: SYNC_COLUMNS,
}


def _write_csv(tmp_path: Path, name: str, lines: list[str]) -> Path:
    path = tmp_path / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("path", _CAPTURE_CSVS)
def test_reference_csv_headers_are_exact(path: Path) -> None:
    raw = path.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf"), f"{path} starts with a BOM"
    first_line = raw.decode("utf-8").splitlines()[0]
    actual = tuple(first_line.split(","))
    expected = _EXPECTED_HEADERS[path]
    assert actual == expected, f"{path} header is {actual!r}, expected {expected!r}"


@pytest.mark.parametrize("path", _CAPTURE_CSVS)
def test_reference_csvs_use_comma_dialect_not_hudl_dialect(path: Path) -> None:
    raw = path.read_text(encoding="utf-8")
    assert ";" not in raw, f"{path} contains ';' — Hudl-export dialect must not leak here"


def test_video_inventory_rows_are_verifiable() -> None:
    df = pl.read_csv(INVENTORY_CSV, schema_overrides=INVENTORY_SCHEMA)
    for row in df.iter_rows(named=True):
        assert row["domain"] in DOMAIN_VOCABULARY, (
            f"domain {row['domain']!r} not in {DOMAIN_VOCABULARY}"
        )
        assert row["session_id"], f"empty session_id in row: {row}"
        local_path = row["local_path"]
        if local_path:
            content_sha256 = row["content_sha256"]
            assert content_sha256 and _SHA256_RE.match(content_sha256), (
                f"content_sha256 {content_sha256!r} is not 64 lowercase hex chars "
                f"for local_path {local_path!r}"
            )


def test_video_inventory_paths_stay_repo_relative() -> None:
    df = pl.read_csv(INVENTORY_CSV, schema_overrides=INVENTORY_SCHEMA)
    for row in df.iter_rows(named=True):
        local_path = row["local_path"]
        if not local_path:
            continue
        assert not local_path.startswith("/"), f"local_path is absolute: {local_path!r}"
        assert not local_path.startswith("~"), f"local_path uses home shorthand: {local_path!r}"
        assert "/Users/" not in local_path, f"local_path leaks a macOS user path: {local_path!r}"
        assert "/home/" not in local_path, f"local_path leaks a Linux user path: {local_path!r}"


def test_video_sync_rows_are_anchored() -> None:
    df = pl.read_csv(SYNC_CSV, schema_overrides=SYNC_SCHEMA)
    assert df.schema["game_id"] == pl.Utf8, df.schema["game_id"]
    assert df.schema["play_id"] == pl.Int32, df.schema["play_id"]
    for row in df.iter_rows(named=True):
        assert row["game_id"] is not None, f"null game_id in row: {row}"
        assert row["play_id"] is not None, f"null play_id in row: {row}"
        assert row["video_file"], f"empty video_file in row: {row}"
        assert row["snap_frame"] is not None or row["snap_seconds"] is not None, (
            f"row has neither snap_frame nor snap_seconds: {row}"
        )


def test_video_sync_schema_joins_plays_shaped_frame(tmp_path: Path) -> None:
    sync_path = _write_csv(
        tmp_path,
        "video_sync.csv",
        [
            ",".join(SYNC_COLUMNS),
            "2026-06-14_GER-vs-AUT_EM-QUALI,3,"
            "data/video/2026-06-14_GER-vs-AUT_drone_01.mp4,1450,48.3,,",
        ],
    )
    sync = pl.read_csv(sync_path, schema_overrides=SYNC_SCHEMA)

    plays = pl.DataFrame(
        {"game_id": ["2026-06-14_GER-vs-AUT_EM-QUALI"], "play_id": [3]}
    ).cast({"game_id": pl.Utf8, "play_id": pl.Int32})

    joined = sync.join(plays, on=["game_id", "play_id"], how="inner")
    assert joined.height == 1, (
        "sync x plays-shaped-fixture join produced 0 rows — dtype mismatch "
        "against canonical game_id/play_id (RESEARCH Pitfall 1)"
    )


def test_gitignore_covers_video_directory() -> None:
    ignored = subprocess.run(
        ["git", "check-ignore", "-q", "data/video/probe.mp4"],
        cwd=REPO_ROOT,
    )
    assert ignored.returncode == 0, "data/video/*.mp4 is not covered by .gitignore"

    kept = subprocess.run(
        ["git", "check-ignore", "-q", "data/video/.gitkeep"],
        cwd=REPO_ROOT,
    )
    assert kept.returncode == 1, "data/video/.gitkeep must not be ignored"


def test_no_video_binaries_tracked_by_git() -> None:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    tracked = result.stdout.splitlines()
    offenders = [
        path
        for path in tracked
        if path.lower().endswith(VIDEO_SUFFIXES)
    ]
    assert not offenders, f"video binaries tracked by git: {offenders}"


def test_capture_artifacts_contain_no_roster_names() -> None:
    roster = pl.read_csv(REPO_ROOT / "data" / "reference" / "roster.csv")
    names = [name for name in roster["player_name"].unique().to_list() if name]

    for path in _CAPTURE_CSVS:
        text = path.read_text(encoding="utf-8").lower()
        for name in names:
            assert name.lower() not in text, f"{path} contains roster name {name!r}"


def test_core_dependencies_stay_free_of_cv_libraries() -> None:
    """D-07: core `uv sync` (no extras) stays lean, with no CV/video library.

    Phase 2.1 legitimately introduces CV/video dependencies, but only inside the
    optional `[project.optional-dependencies.cv]` group — never in core
    `[project.dependencies]`.
    """
    pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    data = tomllib.loads(pyproject_text)
    core_dependencies = data["project"]["dependencies"]
    forbidden = (
        "opencv",
        "decord",
        "ffmpeg-python",
        "imageio-ffmpeg",
        "moviepy",
        "rfdetr",
        "torch",
        "transformers",
    )
    for entry in core_dependencies:
        for token in forbidden:
            assert token not in entry, (
                f"project.dependencies entry {entry!r} mentions {token!r} — "
                "D-07: core install must stay free of CV/video libraries; "
                "add it under [project.optional-dependencies.cv] instead"
            )

    cv_group = data["project"]["optional-dependencies"]["cv"]
    assert any("trackers" in entry for entry in cv_group), (
        "the `cv` optional-dependencies group must list `trackers`"
    )
