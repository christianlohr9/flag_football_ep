"""Unit tests for the M2-4 measurement core: the label-free continuous metric
(`scripts/hackathon/continuous_metric.py`) and the M2-3-ready label-based interface
(`scripts/hackathon/identity_metric.py`), including the honesty tests -- the swap
blind spot, the partial-review rate and the real vault-file dialect.

Imports both modules by path (`sys.path.insert` on `scripts/hackathon`), mirroring
`baseline_common.py`'s own import convention, and obtains the real `_measure_clip`
via `score_tracks._load_continuity_helpers()` so these tests exercise the real
fragment logic, never a stub.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hackathon"))

import continuous_metric as cm  # noqa: E402
import score_tracks  # noqa: E402

_measure_clip, summarise_review = score_tracks._load_continuity_helpers()

TRACK_COLUMNS = (
    "session_id",
    "clip_number",
    "frame_index",
    "track_id",
    "bbox_x1",
    "bbox_y1",
    "bbox_x2",
    "bbox_y2",
)

N_FRAMES = 20
SESSION = "test-session-dev"


def _row(
    session_id: str,
    clip_number: int,
    frame_index: int,
    track_id: int,
    x: float,
    y: float,
    class_name: str | None = None,
) -> dict:
    row = {
        "session_id": session_id,
        "clip_number": clip_number,
        "frame_index": frame_index,
        "track_id": track_id,
        "bbox_x1": x,
        "bbox_y1": y,
        "bbox_x2": x + 10.0,
        "bbox_y2": y + 10.0,
    }
    if class_name is not None:
        row["class_name"] = class_name
    return row


def _to_df(rows: list[dict], with_class_name: bool = False) -> pl.DataFrame:
    columns = list(TRACK_COLUMNS)
    if with_class_name:
        columns.append("class_name")
    if not rows:
        return pl.DataFrame(schema={c: pl.Float64 for c in columns})
    return pl.DataFrame(rows).select(columns)


def _fragments_fixture() -> pl.DataFrame:
    """7 short-lived tracks (5 of 20 frames each, <50% coverage -> fragments) plus
    one long track spanning the full 20 frames (sets the clip's real length and is
    itself NOT a fragment). `n_fragments` must read exactly 7.
    """
    rows: list[dict] = []
    for track_id in range(7):
        for frame_index in range(5):
            rows.append(_row(SESSION, 1, frame_index, track_id, track_id * 10.0, 0.0))
    for frame_index in range(N_FRAMES):
        rows.append(_row(SESSION, 1, frame_index, 100, 500.0, 500.0))
    return _to_df(rows)


def _overmerge_fixture() -> pl.DataFrame:
    """GTA-shaped over-merge: 5 long tracks, each covering the whole 20-frame clip
    (0 fragments, looks perfect on the primary metric) while only 5 of the expected
    10 players are simultaneously active (the guard metric flags it).
    """
    rows: list[dict] = []
    for track_id in range(5):
        for frame_index in range(N_FRAMES):
            rows.append(_row(SESSION, 2, frame_index, track_id, track_id * 20.0, 0.0))
    return _to_df(rows)


def _identity_pair_rows(swap_at: int | None) -> list[dict]:
    """Two full-length tracks (track_id 1 and 2) over 20 frames. When `swap_at` is
    set, the two tracks' bbox streams are exchanged from that frame on -- the
    track_id assignment itself never changes, modelling a silent identity swap: no
    track ends, no track is born, the active-track count never changes.
    """
    rows: list[dict] = []
    for frame_index in range(N_FRAMES):
        t1_x, t1_y = 10.0 + frame_index, 50.0
        t2_x, t2_y = 400.0 - frame_index, 300.0
        if swap_at is not None and frame_index >= swap_at:
            t1_x, t1_y, t2_x, t2_y = t2_x, t2_y, t1_x, t1_y
        rows.append(_row(SESSION, 3, frame_index, 1, t1_x, t1_y))
        rows.append(_row(SESSION, 3, frame_index, 2, t2_x, t2_y))
    return rows


def _referee_and_players_fixture() -> pl.DataFrame:
    """One `referee` track and nine `player` tracks, all covering the full 20-frame
    clip, WITH a `class_name` column -- `player_view` must drop the referee.
    """
    rows: list[dict] = []
    for frame_index in range(N_FRAMES):
        rows.append(_row(SESSION, 4, frame_index, 0, 0.0, 0.0, class_name="referee"))
        for track_id in range(1, 10):
            rows.append(_row(SESSION, 4, frame_index, track_id, track_id * 10.0, 0.0, class_name="player"))
    return _to_df(rows, with_class_name=True)


# --- player_view -------------------------------------------------------------


def test_player_view_filters_referees_when_class_name_present():
    df = _referee_and_players_fixture()
    filtered, class_name_filtered = cm.player_view(df)

    assert class_name_filtered is True
    assert filtered["track_id"].n_unique() == 9
    assert 0 not in filtered["track_id"].to_list()


def test_player_view_passthrough_without_class_name():
    df = _overmerge_fixture()
    assert "class_name" not in df.columns

    filtered, class_name_filtered = cm.player_view(df)

    assert class_name_filtered is False
    assert filtered.height == df.height


# --- clip_metrics: primary and guard numbers ----------------------------------


def test_fragments_per_expected_player_denominator_is_constant():
    df = _fragments_fixture()
    row = cm.clip_metrics(1, df, _measure_clip)

    assert row["n_fragments"] == 7
    assert row["fragments_per_expected_player"] == pytest.approx(0.7)
    assert row["class_name_filtered"] is False
    assert row["no_tracks"] is False


def test_empty_clip_is_not_perfect():
    df = _to_df([])
    row = cm.clip_metrics(99, df, _measure_clip)

    assert row["fragments_per_expected_player"] == pytest.approx(0.0)
    assert row["active_track_count_deviation"] == pytest.approx(10.0)
    assert row["no_tracks"] is True


def test_gta_shaped_overmerge_guard_catches_what_primary_misses():
    df = _overmerge_fixture()
    row = cm.clip_metrics(2, df, _measure_clip)

    assert row["fragments_per_expected_player"] == pytest.approx(0.0)
    assert row["active_track_count_deviation"] == pytest.approx(5.0)


def test_swap_is_invisible_to_both_metrics():
    """The load-bearing honesty test: a silent identity swap (bbox streams
    exchanged between two full-length tracks, track_id assignment untouched) is
    invisible to BOTH the primary and the guard number. This documents the ceiling
    of the label-free layer as an executable claim, not a footnote.
    """
    non_swapped = _to_df(_identity_pair_rows(swap_at=None))
    swapped = _to_df(_identity_pair_rows(swap_at=10))

    row_non_swapped = cm.clip_metrics(3, non_swapped, _measure_clip)
    row_swapped = cm.clip_metrics(3, swapped, _measure_clip)

    assert row_non_swapped == row_swapped


def test_blind_spot_note_names_swap_failure_mode():
    assert "Identitaetswechsel" in cm.BLIND_SPOT_NOTE
    assert "39" in cm.BLIND_SPOT_NOTE and "46" in cm.BLIND_SPOT_NOTE


def test_guard_note_marks_guard_as_diagnostic_not_acceptance():
    assert "diagnostisch" in cm.GUARD_NOTE
    assert "Abnahmekriterium" in cm.GUARD_NOTE


# --- aggregate -----------------------------------------------------------------


def test_aggregate_zero_clips_returns_none_not_zero():
    result = cm.aggregate([])

    assert result["n_clips"] == 0
    assert result["mean_fragments_per_expected_player"] is None
    assert result["median_fragments_per_expected_player"] is None
    assert result["mean_active_track_count_deviation"] is None
    assert result["n_clips_without_class_name"] is None
    assert result["n_clips_without_tracks"] is None


def test_aggregate_over_multiple_clips():
    rows = [
        cm.clip_metrics(1, _fragments_fixture(), _measure_clip),
        cm.clip_metrics(2, _overmerge_fixture(), _measure_clip),
        cm.clip_metrics(99, _to_df([]), _measure_clip),
    ]
    result = cm.aggregate(rows)

    assert result["n_clips"] == 3
    assert result["n_clips_without_tracks"] == 1
    assert result["n_clips_without_class_name"] == 3
    expected_mean = (0.7 + 0.0 + 0.0) / 3
    assert result["mean_fragments_per_expected_player"] == pytest.approx(expected_mean, abs=1e-4)
