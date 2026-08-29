"""Coverage for `flag_football_ep.cv.continuity`: `measure_continuity`'s per-clip
auto-flags and review-CSV contract, `summarise_review`'s refuse-on-partial-review
rule, and a `tests/test_capture_artifacts.py`-style schema gate over the committed
`data/reference/continuity_review.csv` artifact.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

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
from flag_football_ep.cv.continuity import REVIEW_COLUMNS, measure_continuity, summarise_review
from flag_football_ep.testing import synthetic_tracks

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTINUITY_CSV = REPO_ROOT / "data" / "reference" / "continuity_review.csv"

_REVIEW_SCHEMA: dict[str, pl.DataType] = {
    "clip_number": pl.Int32,
    "n_tracks": pl.Int32,
    "longest_track_frac": pl.Float64,
    "n_fragments": pl.Int32,
    "auto_flag": pl.Utf8,
    "verdict": pl.Utf8,
    "id_switches": pl.Int32,
    "reviewer_note": pl.Utf8,
}


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
    )
    return Config(
        paths=paths, reference=reference, sources=sources, train=train, report=report, cv=cv
    )


_INVENTORY_HEADER = (
    "domain,session_id,game_id,capture_date,resolution,fps,duration_seconds,"
    "local_path,content_sha256,notes"
)


def _write_inventory(config: Config, clip_numbers: list[int], *, session_id: str) -> Path:
    """Register `clip_numbers` as drone clips for `session_id`, creating a tiny
    placeholder file per clip (clip_paths only checks existence, never decodes).
    """
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
    lines = [_INVENTORY_HEADER]
    repo_root = config.paths.data_root.parent
    for n in clip_numbers:
        local_path = f"data/video/{session_id}/clip_{n:03d}.mp4"
        clip_file = repo_root / local_path
        clip_file.parent.mkdir(parents=True, exist_ok=True)
        clip_file.write_bytes(b"")
        row = {
            "domain": "drone",
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
        lines.append(",".join(row.get(field, "") for field in fields))

    inventory_path = config.paths.reference / "video_inventory.csv"
    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    inventory_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return inventory_path


def _split_track(
    tracks: pl.DataFrame, *, track_id: int, new_id_a: int, new_id_b: int, split_at: int
) -> pl.DataFrame:
    """Replace `track_id`'s rows with two half-tracks: frames `[0, split_at)` under
    `new_id_a`, frames `[split_at + 2, ...)` under `new_id_b` (a 2-frame gap between
    them, mirroring an OC-SORT re-identification after a brief occlusion).
    """
    track_rows = tracks.filter(pl.col("track_id") == track_id)
    max_frame = int(track_rows["frame_index"].max())

    first_half = track_rows.filter(pl.col("frame_index") < split_at).with_columns(
        pl.lit(new_id_a).alias("track_id")
    )
    second_half = track_rows.filter(pl.col("frame_index") >= split_at + 2).with_columns(
        pl.lit(new_id_b).alias("track_id")
    )
    _ = max_frame  # documents the intent; not otherwise used

    remainder = tracks.filter(pl.col("track_id") != track_id)
    return pl.concat([remainder, first_half, second_half]).sort(["track_id", "frame_index"])


# --- Task 2 tests -----------------------------------------------------------------------------


def test_single_full_length_track_yields_full_frac_and_ok_flag(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    session_id = config.cv.pilot_session_id
    _write_inventory(config, [1], session_id=session_id)

    # n_frames=10, n_tracks=4 (1 referee + 3 players), every track full-length.
    tracks = synthetic_tracks(session_id=session_id, n_clips=1, n_frames=10, n_tracks=4)

    result = measure_continuity(tracks, config)

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.clip_number == 1
    assert row.n_tracks == 4
    assert row.longest_track_frac == 1.0
    assert row.n_fragments == 0
    assert row.auto_flag == "ok"


def test_track_split_into_two_halves_yields_two_fragments_and_fragmented_flag(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    session_id = config.cv.pilot_session_id
    _write_inventory(config, [1], session_id=session_id)

    # referee (track 0, full 10 frames) + one player (track 1, full 10 frames) --
    # then split track 1 into two 4-frame halves (ids 1 and 2), each well under 50%
    # of the clip's 10-frame span.
    tracks = synthetic_tracks(session_id=session_id, n_clips=1, n_frames=10, n_tracks=2)
    tracks = _split_track(tracks, track_id=1, new_id_a=1, new_id_b=2, split_at=4)

    result = measure_continuity(tracks, config)

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.n_tracks == 3  # referee + 2 fragment halves
    assert row.n_fragments == 2
    assert row.auto_flag == "fragmented"


def test_clip_with_no_rows_yields_zero_tracks_and_no_tracks_flag(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    session_id = config.cv.pilot_session_id
    _write_inventory(config, [1, 2], session_id=session_id)

    # tracks only cover clip 1 -- clip 2 is registered but has zero rows.
    tracks = synthetic_tracks(session_id=session_id, n_clips=1, n_frames=10, n_tracks=4)

    result = measure_continuity(tracks, config)

    rows_by_clip = {row.clip_number: row for row in result.rows}
    assert set(rows_by_clip) == {1, 2}
    assert rows_by_clip[2].n_tracks == 0
    assert rows_by_clip[2].auto_flag == "no-tracks"


def test_rerun_preserves_existing_verdicts_and_notes(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    session_id = config.cv.pilot_session_id
    _write_inventory(config, [1], session_id=session_id)
    tracks = synthetic_tracks(session_id=session_id, n_clips=1, n_frames=10, n_tracks=4)

    first = measure_continuity(tracks, config)
    assert first.rows[0].auto_flag == "ok"

    # a human fills in the verdict by hand-editing the review CSV
    df = pl.read_csv(first.review_csv, schema_overrides=_REVIEW_SCHEMA)
    df = df.with_columns(
        pl.lit("pass").alias("verdict"),
        pl.lit(1).cast(pl.Int32).alias("id_switches"),
        pl.lit("one switch at the snap").alias("reviewer_note"),
    )
    df.write_csv(first.review_csv)

    # a re-run with a DIFFERENT tracks frame must still preserve the human columns
    # while refreshing the auto columns
    changed_tracks = synthetic_tracks(session_id=session_id, n_clips=1, n_frames=10, n_tracks=8)
    second = measure_continuity(changed_tracks, config)

    assert second.rows[0].n_tracks == 8  # auto columns refreshed

    reloaded = pl.read_csv(second.review_csv, schema_overrides=_REVIEW_SCHEMA)
    row = reloaded.row(0, named=True)
    assert row["verdict"] == "pass"
    assert row["id_switches"] == 1
    assert row["reviewer_note"] == "one switch at the snap"


def test_summarise_review_refuses_a_headline_number_when_partially_reviewed(
    tmp_path: Path,
) -> None:
    review_csv = tmp_path / "continuity_review.csv"
    review_csv.write_text(
        "clip_number,n_tracks,longest_track_frac,n_fragments,auto_flag,verdict,id_switches,reviewer_note\n"
        "1,4,1.0,0,ok,pass,0,\n"
        "2,4,1.0,0,ok,,,\n"
        "3,4,1.0,0,ok,fail,2,switch at the snap\n",
        encoding="utf-8",
    )

    summary = summarise_review(review_csv)

    assert summary["n_clips"] == 3
    assert summary["n_reviewed"] == 2
    assert summary["pass_rate"] is None
    assert summary["unreviewed_clips"] == [2]


def test_summarise_review_returns_exact_ratio_when_fully_reviewed(tmp_path: Path) -> None:
    review_csv = tmp_path / "continuity_review.csv"
    review_csv.write_text(
        "clip_number,n_tracks,longest_track_frac,n_fragments,auto_flag,verdict,id_switches,reviewer_note\n"
        "1,4,1.0,0,ok,pass,0,\n"
        "2,4,1.0,0,ok,pass,0,\n"
        "3,4,1.0,0,ok,fail,2,switch at the snap\n"
        "4,4,1.0,0,ok,fail,1,\n",
        encoding="utf-8",
    )

    summary = summarise_review(review_csv)

    assert summary["n_clips"] == 4
    assert summary["n_reviewed"] == 4
    assert summary["n_pass"] == 2
    assert summary["n_fail"] == 2
    assert summary["pass_rate"] == 0.5
    assert summary["unreviewed_clips"] == []


# --- schema gate over the committed data/reference/continuity_review.csv artifact -----------
# mirrors tests/test_capture_artifacts.py's style for video_inventory.csv/video_sync.csv.


def test_continuity_review_csv_header_is_exact() -> None:
    raw = CONTINUITY_CSV.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf"), f"{CONTINUITY_CSV} starts with a BOM"
    first_line = raw.decode("utf-8").splitlines()[0]
    actual = tuple(first_line.split(","))
    assert actual == REVIEW_COLUMNS, f"{CONTINUITY_CSV} header is {actual!r}"


def test_continuity_review_csv_uses_comma_dialect() -> None:
    raw = CONTINUITY_CSV.read_text(encoding="utf-8")
    assert ";" not in raw, f"{CONTINUITY_CSV} contains ';' -- Hudl-export dialect must not leak here"


def test_continuity_review_csv_verdict_vocabulary_and_unique_clip_numbers() -> None:
    df = pl.read_csv(CONTINUITY_CSV, schema_overrides=_REVIEW_SCHEMA)
    if df.height == 0:
        pytest.skip(f"{CONTINUITY_CSV} is header-only (no review recorded yet)")

    verdicts = set(df["verdict"].fill_null("").to_list())
    assert verdicts <= {"pass", "fail", ""}, f"unexpected verdict values: {verdicts}"

    dupes = (
        df.group_by("clip_number").agg(pl.len().alias("n")).filter(pl.col("n") > 1)["clip_number"].to_list()
    )
    assert not dupes, f"duplicate clip_number in {CONTINUITY_CSV}: {dupes}"


def test_gitignore_does_not_cover_continuity_review_csv() -> None:
    """The review CSV carries clip numbers and verdicts only (no PII) and must stay
    tracked -- unlike the overlay videos it is reviewed against.
    """
    result = subprocess.run(
        ["git", "check-ignore", "-q", "data/reference/continuity_review.csv"],
        cwd=REPO_ROOT,
    )
    assert result.returncode == 1, "data/reference/continuity_review.csv must not be gitignored"
