"""Tests for `flag_football_ep.cv.testset`: the hackathon dev/private_test role split
(`write_hackathon_split`/`read_hackathon_split`) and the session-level active-learning
exclusion table (`write_al_exclusion`/`read_al_excluded_sessions`) -- plan 02.2-21 Task 1.

Fixtures reuse `test_cv_frames.py`'s `video_inventory.csv` helpers (`import
test_cv_frames as tcf`), the same cross-test-module-import precedent
`test_cv_active_learning.py` already established for this module. Every function here
is a metadata-only inventory read (never opens a clip file), so `tcf._touch` placeholder
files are enough -- no real decodable video is needed anywhere in this file.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import test_cv_frames as tcf
from flag_football_ep.config import Config, load_config
from flag_football_ep.cv.testset import (
    HackathonSplit,
    TestsetError,
    read_al_excluded_sessions,
    read_hackathon_split,
    write_al_exclusion,
    write_hackathon_split,
)
from test_config import MINIMAL_TOML

DEV_SESSION = "2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE"
TEST_SESSION = "2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE"


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(MINIMAL_TOML, encoding="utf-8")
    return load_config(config_path)


def _write_two_session_inventory(
    tmp_path: Path,
    *,
    dev_clip_numbers: list[int] = list(range(1, 6)),
    test_clip_numbers: list[int] | None = None,
) -> None:
    """A synthetic two-session drone inventory: `DEV_SESSION` with `dev_clip_numbers`
    and `TEST_SESSION` with `test_clip_numbers` (default: a non-contiguous set
    mirroring the real Puerto Rico session's 1-56+59-63 gap, at a much smaller scale:
    1-3 + 6-7).
    """
    if test_clip_numbers is None:
        test_clip_numbers = [1, 2, 3, 6, 7]

    rows = []
    for n in dev_clip_numbers:
        local_path = f"data/video/{DEV_SESSION}/Wide - Clip {n:03d}.mp4"
        tcf._touch(tmp_path, local_path)
        rows.append(tcf._row(local_path, session_id=DEV_SESSION, domain="drone"))
    for n in test_clip_numbers:
        local_path = f"data/video/{TEST_SESSION}/Wide - Clip {n:03d}.mp4"
        tcf._touch(tmp_path, local_path)
        rows.append(tcf._row(local_path, session_id=TEST_SESSION, domain="drone"))

    tcf._write_inventory(tmp_path, rows)


# --- write_hackathon_split / read_hackathon_split -----------------------------------


def test_write_hackathon_split_writes_dev_and_test_rows(tmp_path: Path, cfg: Config) -> None:
    _write_two_session_inventory(tmp_path)
    out_csv = tmp_path / "data" / "reference" / "hackathon_split.csv"

    split = write_hackathon_split(cfg, DEV_SESSION, TEST_SESSION, out_csv)

    assert isinstance(split, HackathonSplit)
    assert split.dev_session_id == DEV_SESSION
    assert split.test_session_id == TEST_SESSION
    assert split.dev_clips == [1, 2, 3, 4, 5]
    assert split.test_clips == [1, 2, 3, 6, 7]

    df = pl.read_csv(out_csv)
    dev_rows = df.filter(pl.col("hackathon_role") == "dev")
    test_rows = df.filter(pl.col("hackathon_role") == "private_test")
    assert dev_rows.height == 5
    assert test_rows.height == 5
    assert set(dev_rows["session_id"].to_list()) == {DEV_SESSION}
    assert set(test_rows["session_id"].to_list()) == {TEST_SESSION}
    assert set(df["domain"].to_list()) == {"drone"}
    assert all(note.startswith("plan 02.2-21") for note in df["note"].to_list())


def test_write_hackathon_split_second_write_is_byte_identical(
    tmp_path: Path, cfg: Config
) -> None:
    _write_two_session_inventory(tmp_path)
    out_csv = tmp_path / "data" / "reference" / "hackathon_split.csv"

    write_hackathon_split(cfg, DEV_SESSION, TEST_SESSION, out_csv)
    first_bytes = out_csv.read_bytes()

    write_hackathon_split(cfg, DEV_SESSION, TEST_SESSION, out_csv)
    second_bytes = out_csv.read_bytes()

    assert first_bytes == second_bytes


def test_read_hackathon_split_round_trips_write(tmp_path: Path, cfg: Config) -> None:
    _write_two_session_inventory(tmp_path)
    out_csv = tmp_path / "data" / "reference" / "hackathon_split.csv"

    written = write_hackathon_split(cfg, DEV_SESSION, TEST_SESSION, out_csv)
    read_back = read_hackathon_split(out_csv)

    assert read_back == written


def test_read_hackathon_split_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(TestsetError, match="not found"):
        read_hackathon_split(tmp_path / "does-not-exist.csv")


def test_read_hackathon_split_bad_role_raises(tmp_path: Path) -> None:
    path = tmp_path / "hackathon_split.csv"
    pl.DataFrame(
        {
            "domain": ["drone"],
            "session_id": [DEV_SESSION],
            "clip_number": [1],
            "hackathon_role": ["bogus"],
            "frozen_at": ["2026-09-02T00:00:00+00:00"],
            "note": [""],
        }
    ).write_csv(path)

    with pytest.raises(TestsetError, match="hackathon_role"):
        read_hackathon_split(path)


def test_read_hackathon_split_multiple_private_test_sessions_raises(tmp_path: Path) -> None:
    path = tmp_path / "hackathon_split.csv"
    pl.DataFrame(
        {
            "domain": ["drone", "drone", "drone"],
            "session_id": [DEV_SESSION, TEST_SESSION, "some-other-session"],
            "clip_number": [1, 1, 1],
            "hackathon_role": ["dev", "private_test", "private_test"],
            "frozen_at": ["2026-09-02T00:00:00+00:00"] * 3,
            "note": [""] * 3,
        }
    ).write_csv(path)

    with pytest.raises(TestsetError, match="private_test"):
        read_hackathon_split(path)


def test_read_hackathon_split_zero_private_test_sessions_raises(tmp_path: Path) -> None:
    path = tmp_path / "hackathon_split.csv"
    pl.DataFrame(
        {
            "domain": ["drone"],
            "session_id": [DEV_SESSION],
            "clip_number": [1],
            "hackathon_role": ["dev"],
            "frozen_at": ["2026-09-02T00:00:00+00:00"],
            "note": [""],
        }
    ).write_csv(path)

    with pytest.raises(TestsetError, match="private_test"):
        read_hackathon_split(path)


def test_write_hackathon_split_unknown_session_raises(tmp_path: Path, cfg: Config) -> None:
    _write_two_session_inventory(tmp_path)
    out_csv = tmp_path / "data" / "reference" / "hackathon_split.csv"

    with pytest.raises(TestsetError, match="no clips registered"):
        write_hackathon_split(cfg, DEV_SESSION, "session-not-in-inventory", out_csv)


# --- write_al_exclusion / read_al_excluded_sessions ----------------------------------


def test_write_al_exclusion_then_read_round_trips(tmp_path: Path, cfg: Config) -> None:
    _write_two_session_inventory(tmp_path)
    out_csv = tmp_path / "data" / "reference" / "al_excluded_sessions.csv"

    write_al_exclusion(
        cfg, TEST_SESSION, reason="private hackathon test game", requirement="DATA-04", out_csv=out_csv
    )

    excluded = read_al_excluded_sessions(out_csv)
    assert excluded == {TEST_SESSION: "private hackathon test game"}

    df = pl.read_csv(out_csv)
    assert df.height == 1
    assert df["domain"].to_list() == ["drone"]
    assert df["requirement"].to_list() == ["DATA-04"]


def test_read_al_excluded_sessions_missing_file_returns_empty_dict(tmp_path: Path) -> None:
    assert read_al_excluded_sessions(tmp_path / "does-not-exist.csv") == {}


def test_write_al_exclusion_preserves_other_sessions(tmp_path: Path, cfg: Config) -> None:
    _write_two_session_inventory(tmp_path)
    out_csv = tmp_path / "data" / "reference" / "al_excluded_sessions.csv"

    write_al_exclusion(
        cfg, DEV_SESSION, reason="unrelated exclusion", requirement="TEST-ONLY", out_csv=out_csv
    )
    write_al_exclusion(
        cfg, TEST_SESSION, reason="private hackathon test game", requirement="DATA-04", out_csv=out_csv
    )

    excluded = read_al_excluded_sessions(out_csv)
    assert excluded == {
        DEV_SESSION: "unrelated exclusion",
        TEST_SESSION: "private hackathon test game",
    }


def test_write_al_exclusion_refreshes_same_session_row(tmp_path: Path, cfg: Config) -> None:
    _write_two_session_inventory(tmp_path)
    out_csv = tmp_path / "data" / "reference" / "al_excluded_sessions.csv"

    write_al_exclusion(
        cfg, TEST_SESSION, reason="first reason", requirement="DATA-04", out_csv=out_csv
    )
    write_al_exclusion(
        cfg, TEST_SESSION, reason="second reason", requirement="DATA-04", out_csv=out_csv
    )

    df = pl.read_csv(out_csv)
    assert df.height == 1
    assert df["reason"].to_list() == ["second reason"]
