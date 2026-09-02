"""Tests for `flag_football_ep.cv.testset`: the hackathon dev/private_test role split
(`write_hackathon_split`/`read_hackathon_split`), the session-level active-learning
exclusion table (`write_al_exclusion`/`read_al_excluded_sessions`, plan 02.2-21 Task 1),
and the private test-set ground-truth vault tooling (`write_continuity_skeleton`/
`write_flag_pull_skeleton`/`validate_test_labels`, plan 02.2-21 Task 2).

Fixtures reuse `test_cv_frames.py`'s `video_inventory.csv` helpers (`import
test_cv_frames as tcf`), the same cross-test-module-import precedent
`test_cv_active_learning.py` already established for this module. Every function in
this module is a metadata-only inventory read or a pure-CSV operation (never opens or
decodes a clip file), so `tcf._touch` placeholder files are enough -- no real
decodable video is needed anywhere in this file.
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
    validate_test_labels,
    write_al_exclusion,
    write_continuity_skeleton,
    write_flag_pull_skeleton,
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


# --- write_continuity_skeleton / write_flag_pull_skeleton / validate_test_labels ----

_SKELETON_SESSION = "sess-skeleton"
_SKELETON_CLIPS = (1, 2, 3)


def _write_skeleton_session_inventory(tmp_path: Path, clip_numbers: tuple[int, ...] = _SKELETON_CLIPS) -> None:
    rows = []
    for n in clip_numbers:
        local_path = f"data/video/{_SKELETON_SESSION}/Wide - Clip {n:03d}.mp4"
        tcf._touch(tmp_path, local_path)
        rows.append(tcf._row(local_path, session_id=_SKELETON_SESSION, domain="drone"))
    tcf._write_inventory(tmp_path, rows)


def _skeleton_tracks(clip_numbers: tuple[int, ...] = _SKELETON_CLIPS) -> pl.DataFrame:
    """Minimal tracks frame: `write_continuity_skeleton`'s composed
    `continuity._measure_clip` only reads `frame_index`/`track_id` per clip.
    """
    rows = []
    for n in clip_numbers:
        for track_id in (0, 1):
            for frame_index in range(5):
                rows.append({"clip_number": n, "track_id": track_id, "frame_index": frame_index})
    return pl.DataFrame(rows)


def test_write_continuity_skeleton_one_row_per_registered_clip(tmp_path: Path, cfg: Config) -> None:
    _write_skeleton_session_inventory(tmp_path)
    out_path = tmp_path / "vault" / "continuity_review.csv"

    written = write_continuity_skeleton(cfg, _SKELETON_SESSION, _skeleton_tracks(), out_path)

    assert written == out_path
    df = pl.read_csv(out_path)
    assert set(df["clip_number"].to_list()) == set(_SKELETON_CLIPS)
    assert df["verdict"].fill_null("").to_list() == ["", "", ""]
    assert (df["n_tracks"] == 2).all()


def test_write_continuity_skeleton_preserves_human_columns_on_rerun(
    tmp_path: Path, cfg: Config
) -> None:
    _write_skeleton_session_inventory(tmp_path)
    out_path = tmp_path / "vault" / "continuity_review.csv"

    write_continuity_skeleton(cfg, _SKELETON_SESSION, _skeleton_tracks(), out_path)

    # Simulate the user filling in clip 1's verdict.
    df = pl.read_csv(out_path)
    df = df.with_columns(
        pl.when(pl.col("clip_number") == 1).then(pl.lit("pass")).otherwise(pl.col("verdict")).alias(
            "verdict"
        )
    )
    df.write_csv(out_path)

    write_continuity_skeleton(cfg, _SKELETON_SESSION, _skeleton_tracks(), out_path)

    reread = pl.read_csv(out_path)
    clip1 = reread.filter(pl.col("clip_number") == 1)
    assert clip1["verdict"].to_list() == ["pass"]


def test_write_flag_pull_skeleton_exact_header_and_empty_rows(tmp_path: Path, cfg: Config) -> None:
    _write_skeleton_session_inventory(tmp_path)
    out_path = tmp_path / "vault" / "flag_pull_events.csv"

    written = write_flag_pull_skeleton(cfg, _SKELETON_SESSION, out_path)

    assert written == out_path
    first_line = out_path.read_text(encoding="utf-8").splitlines()[0]
    assert first_line == "clip_number,outcome,pull_time_s,carrier_track_id,puller_track_id,notes"
    df = pl.read_csv(out_path)
    assert set(df["clip_number"].to_list()) == set(_SKELETON_CLIPS)
    assert df["outcome"].fill_null("").to_list() == ["", "", ""]


def test_write_flag_pull_skeleton_preserves_human_columns_on_rerun(
    tmp_path: Path, cfg: Config
) -> None:
    _write_skeleton_session_inventory(tmp_path)
    out_path = tmp_path / "vault" / "flag_pull_events.csv"

    write_flag_pull_skeleton(cfg, _SKELETON_SESSION, out_path)

    df = pl.read_csv(out_path)
    df = df.with_columns(
        pl.when(pl.col("clip_number") == 2)
        .then(pl.lit("touchdown"))
        .otherwise(pl.col("outcome"))
        .alias("outcome")
    )
    df.write_csv(out_path)

    write_flag_pull_skeleton(cfg, _SKELETON_SESSION, out_path)

    reread = pl.read_csv(out_path)
    clip2 = reread.filter(pl.col("clip_number") == 2)
    assert clip2["outcome"].to_list() == ["touchdown"]


def _write_valid_labels(tmp_path: Path) -> tuple[Path, Path]:
    continuity_path = tmp_path / "continuity_review.csv"
    flag_pull_path = tmp_path / "flag_pull_events.csv"

    continuity_rows = [
        {
            "clip_number": n,
            "n_tracks": 2,
            "longest_track_frac": 1.0,
            "n_fragments": 0,
            "auto_flag": "ok",
            "verdict": "pass" if n % 2 == 0 else "fail",
            "id_switches": 0,
            "reviewer_note": "" if n % 2 == 0 else "note",
        }
        for n in _SKELETON_CLIPS
    ]
    pl.DataFrame(continuity_rows).write_csv(continuity_path)

    flag_pull_rows = [
        {
            "clip_number": n,
            "outcome": "pull" if n == 1 else "incomplete",
            "pull_time_s": 3.5 if n == 1 else None,
            "carrier_track_id": 1 if n == 1 else None,
            "puller_track_id": "2/3" if n == 1 else "",
            "notes": "",
        }
        for n in _SKELETON_CLIPS
    ]
    pl.DataFrame(flag_pull_rows).write_csv(flag_pull_path)
    return continuity_path, flag_pull_path


def test_validate_test_labels_valid_vault_returns_summary(tmp_path: Path) -> None:
    continuity_path, flag_pull_path = _write_valid_labels(tmp_path)

    summary = validate_test_labels(continuity_path, flag_pull_path, list(_SKELETON_CLIPS))

    assert summary["n_clips"] == 3
    assert summary["n_pass"] == 1  # only clip 2
    assert summary["n_fail"] == 2
    assert summary["pass_rate"] == pytest.approx(1 / 3)
    assert summary["n_outcomes"] == 3


def test_validate_test_labels_missing_verdict_raises(tmp_path: Path) -> None:
    continuity_path, flag_pull_path = _write_valid_labels(tmp_path)
    df = pl.read_csv(continuity_path)
    df = df.with_columns(
        pl.when(pl.col("clip_number") == 1).then(pl.lit("")).otherwise(pl.col("verdict")).alias(
            "verdict"
        )
    )
    df.write_csv(continuity_path)

    with pytest.raises(TestsetError, match="missing verdict"):
        validate_test_labels(continuity_path, flag_pull_path, list(_SKELETON_CLIPS))


def test_validate_test_labels_bad_outcome_raises(tmp_path: Path) -> None:
    continuity_path, flag_pull_path = _write_valid_labels(tmp_path)
    df = pl.read_csv(flag_pull_path)
    df = df.with_columns(
        pl.when(pl.col("clip_number") == 3)
        .then(pl.lit("bogus-outcome"))
        .otherwise(pl.col("outcome"))
        .alias("outcome")
    )
    df.write_csv(flag_pull_path)

    with pytest.raises(TestsetError, match="invalid outcome"):
        validate_test_labels(continuity_path, flag_pull_path, list(_SKELETON_CLIPS))


def test_validate_test_labels_clip_set_mismatch_raises(tmp_path: Path) -> None:
    continuity_path, flag_pull_path = _write_valid_labels(tmp_path)

    with pytest.raises(TestsetError, match="clip set"):
        validate_test_labels(continuity_path, flag_pull_path, [1, 2, 3, 4])


def test_validate_test_labels_semicolon_dialect_raises(tmp_path: Path) -> None:
    continuity_path, flag_pull_path = _write_valid_labels(tmp_path)
    continuity_path.write_text(
        continuity_path.read_text(encoding="utf-8").replace(",", ";"), encoding="utf-8"
    )

    with pytest.raises(TestsetError, match="Hudl-export dialect"):
        validate_test_labels(continuity_path, flag_pull_path, list(_SKELETON_CLIPS))


def test_validate_test_labels_pull_without_pull_time_raises(tmp_path: Path) -> None:
    continuity_path, flag_pull_path = _write_valid_labels(tmp_path)
    df = pl.read_csv(flag_pull_path)
    df = df.with_columns(
        pl.when(pl.col("clip_number") == 1).then(None).otherwise(pl.col("pull_time_s")).alias(
            "pull_time_s"
        )
    )
    df.write_csv(flag_pull_path)

    with pytest.raises(TestsetError, match="pull_time_s"):
        validate_test_labels(continuity_path, flag_pull_path, list(_SKELETON_CLIPS))


def test_validate_test_labels_bad_puller_track_id_raises(tmp_path: Path) -> None:
    continuity_path, flag_pull_path = _write_valid_labels(tmp_path)
    df = pl.read_csv(flag_pull_path)
    # "notanumber" (not ';'-delimited) -- must fail the puller_track_id pattern gate,
    # not the comma-dialect gate.
    df = df.with_columns(
        pl.when(pl.col("clip_number") == 1)
        .then(pl.lit("notanumber"))
        .otherwise(pl.col("puller_track_id"))
        .alias("puller_track_id")
    )
    df.write_csv(flag_pull_path)

    with pytest.raises(TestsetError, match="puller_track_id"):
        validate_test_labels(continuity_path, flag_pull_path, list(_SKELETON_CLIPS))


def test_validate_test_labels_missing_file_raises(tmp_path: Path) -> None:
    continuity_path, flag_pull_path = _write_valid_labels(tmp_path)
    flag_pull_path.unlink()

    with pytest.raises(TestsetError, match="not found"):
        validate_test_labels(continuity_path, flag_pull_path, list(_SKELETON_CLIPS))
