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
import tempfile
from pathlib import Path

import numpy as np
import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "hackathon"))

import continuous_metric as cm  # noqa: E402
import identity_metric as im  # noqa: E402
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


# --- Split handling --------------------------------------------------------------

SPLIT_ROWS = [
    {
        "domain": "drone",
        "session_id": "test-session-dev",
        "clip_number": n,
        "hackathon_role": "dev",
        "frozen_at": "2026-09-02T00:00:00+00:00",
        "note": "synthetic",
    }
    for n in (1, 2, 3)
] + [
    {
        "domain": "drone",
        "session_id": "test-session-pt",
        "clip_number": n,
        "hackathon_role": "private_test",
        "frozen_at": "2026-09-02T00:00:00+00:00",
        "note": "synthetic",
    }
    for n in (1, 2, 3)
]


def _write_split_csv(path: Path, rows: list[dict]) -> None:
    pl.DataFrame(rows).select(list(cm.SPLIT_COLUMNS)).write_csv(path)


def test_read_split_maps_session_clip_to_role(tmp_path):
    path = tmp_path / "hackathon_split.csv"
    _write_split_csv(path, SPLIT_ROWS)

    split_map = cm.read_split(path)

    assert split_map[("test-session-dev", 1)] == "dev"
    assert split_map[("test-session-pt", 1)] == "private_test"
    assert len(split_map) == 6


def test_read_split_raises_named_error_on_missing_column(tmp_path):
    path = tmp_path / "hackathon_split.csv"
    rows = [{k: v for k, v in row.items() if k != "note"} for row in SPLIT_ROWS]
    pl.DataFrame(rows).write_csv(path)

    with pytest.raises(cm.SplitSchemaError, match="note"):
        cm.read_split(path)


def test_read_split_raises_named_error_on_invalid_role(tmp_path):
    path = tmp_path / "hackathon_split.csv"
    bad_rows = [dict(row) for row in SPLIT_ROWS]
    bad_rows[0]["hackathon_role"] = "public_test"
    _write_split_csv(path, bad_rows)

    with pytest.raises(cm.SplitSchemaError, match="public_test"):
        cm.read_split(path)


def test_role_violations_empty_when_roles_match(tmp_path):
    path = tmp_path / "hackathon_split.csv"
    _write_split_csv(path, SPLIT_ROWS)
    split_map = cm.read_split(path)

    dev_tracks = _to_df(
        [_row("test-session-dev", n, 0, 0, 0.0, 0.0) for n in (1, 2, 3)]
    )

    assert cm.role_violations(dev_tracks, split_map, expected_role="dev") == []


def test_role_violations_flags_wrong_role_and_unknown_pair(tmp_path):
    path = tmp_path / "hackathon_split.csv"
    _write_split_csv(path, SPLIT_ROWS)
    split_map = cm.read_split(path)

    # Clip 1 from the private_test session passed as --tracks-dev (wrong role);
    # clip 99 is not in the split file at all (unknown pair).
    mixed_tracks = _to_df(
        [
            _row("test-session-pt", 1, 0, 0, 0.0, 0.0),
            _row("test-session-dev", 99, 0, 0, 0.0, 0.0),
        ]
    )

    violations = cm.role_violations(mixed_tracks, split_map, expected_role="dev")

    assert len(violations) == 2
    assert any("private_test" in v and "erwartet 'dev'" in v for v in violations)
    assert any("unbekannt" in v for v in violations)
    assert violations == sorted(violations)


# --- Vault-dialect review reading --------------------------------------------------

_DIALECT_ROWS = [
    {
        "clip_number": 1,
        "n_tracks": 24,
        "longest_track_frac": 1.0,
        "n_fragments": 5,
        "auto_flag": "ok",
        "verdict": "pass",
        "id_switches": None,
        "reviewer_note": "",
    },
    {
        "clip_number": 2,
        "n_tracks": 21,
        "longest_track_frac": 1.0,
        "n_fragments": 6,
        "auto_flag": "ok",
        "verdict": "fail",
        "id_switches": 3,
        "reviewer_note": "Wechsel bei Ueberlappung, Spielerin fiel, 1>6, 6>20",
    },
    {
        "clip_number": 3,
        "n_tracks": 18,
        "longest_track_frac": 0.6,
        "n_fragments": 2,
        "auto_flag": "fragmented",
        "verdict": "",
        "id_switches": None,
        "reviewer_note": "",
    },
]

# A raw umlaut used inside the semicolon/cp1252 fixture's reviewer_note, matching
# the real vault file's shape (German reviewer prose with umlauts and embedded
# commas, never quoted because the delimiter is ';' not ','). The umlauts are real
# non-ASCII characters (not the "ue"/"ae" transliteration used elsewhere in this
# file) so the cp1252 fixture genuinely fails a UTF-8 decode, same as the real
# vault file.
_UMLAUT_NOTE = "Überlappung nahe Torraum, Spielerin für 3 Frames verdeckt"


def _write_semicolon_cp1252_fixture(path: Path, rows: list[dict]) -> None:
    header = ";".join(cm.REVIEW_COLUMNS)
    lines = [header]
    for row in rows:
        note = row["reviewer_note"]
        if row["clip_number"] == 2:
            note = _UMLAUT_NOTE
        id_switches = "" if row["id_switches"] is None else str(row["id_switches"])
        lines.append(
            ";".join(
                [
                    str(row["clip_number"]),
                    str(row["n_tracks"]),
                    str(row["longest_track_frac"]),
                    str(row["n_fragments"]),
                    row["auto_flag"],
                    row["verdict"],
                    id_switches,
                    note,
                ]
            )
        )
    text = "\r\n".join(lines) + "\r\n"
    path.write_bytes(text.encode("cp1252"))


def _write_comma_utf8_fixture(path: Path, rows: list[dict]) -> None:
    fixed_rows = []
    for row in rows:
        row = dict(row)
        if row["clip_number"] == 2:
            row["reviewer_note"] = _UMLAUT_NOTE
        fixed_rows.append(row)
    pl.DataFrame(fixed_rows).select(list(cm.REVIEW_COLUMNS)).write_csv(path)


def test_sniff_review_dialect_detects_semicolon_cp1252(tmp_path):
    path = tmp_path / "continuity_review.csv"
    _write_semicolon_cp1252_fixture(path, _DIALECT_ROWS)

    delimiter, encoding = cm.sniff_review_dialect(path)

    assert delimiter == ";"
    assert encoding == "cp1252"


def test_sniff_review_dialect_detects_comma_utf8(tmp_path):
    path = tmp_path / "continuity_review.csv"
    _write_comma_utf8_fixture(path, _DIALECT_ROWS)

    delimiter, encoding = cm.sniff_review_dialect(path)

    assert delimiter == ","
    assert encoding == "utf-8"


def test_read_review_table_tolerates_crlf_and_strips_no_stray_cr(tmp_path):
    path = tmp_path / "continuity_review.csv"
    _write_semicolon_cp1252_fixture(path, _DIALECT_ROWS)

    df = cm.read_review_table(path)

    assert df.height == 3
    assert list(df.columns) == list(cm.REVIEW_COLUMNS)
    assert not any("\r" in (v or "") for v in df["reviewer_note"].to_list())
    assert _UMLAUT_NOTE in df["reviewer_note"].to_list()


def test_semicolon_cp1252_crlf_matches_comma_utf8_summary(tmp_path):
    semicolon_path = tmp_path / "vault_review.csv"
    comma_path = tmp_path / "normal_review.csv"
    _write_semicolon_cp1252_fixture(semicolon_path, _DIALECT_ROWS)
    _write_comma_utf8_fixture(comma_path, _DIALECT_ROWS)

    before = semicolon_path.read_bytes()
    normalized_summary = cm.summarise_review_normalized(semicolon_path, summarise_review)
    after = semicolon_path.read_bytes()
    plain_summary = summarise_review(comma_path)

    assert before == after, "vault-shaped source file must stay byte-identical"
    assert normalized_summary == plain_summary


def test_summarise_review_normalized_short_circuits_for_comma_utf8(tmp_path, monkeypatch):
    comma_path = tmp_path / "normal_review.csv"
    _write_comma_utf8_fixture(comma_path, _DIALECT_ROWS)

    called_with = {}

    def _spy(path):
        called_with["path"] = Path(path)
        return summarise_review(path)

    result = cm.summarise_review_normalized(comma_path, _spy)

    assert called_with["path"] == comma_path
    assert result == summarise_review(comma_path)


def test_summarise_review_normalized_never_writes_inside_repo(tmp_path, monkeypatch):
    semicolon_path = tmp_path / "vault_review.csv"
    _write_semicolon_cp1252_fixture(semicolon_path, _DIALECT_ROWS)

    original_temp_dir_cls = tempfile.TemporaryDirectory
    captured: dict[str, Path] = {}

    class _CapturingTemporaryDirectory(original_temp_dir_cls):
        def __enter__(self):
            entered = super().__enter__()
            captured["path"] = Path(entered)
            return entered

    monkeypatch.setattr(cm.tempfile, "TemporaryDirectory", _CapturingTemporaryDirectory)

    cm.summarise_review_normalized(semicolon_path, summarise_review)

    assert "path" in captured
    captured_path = captured["path"]
    assert REPO_ROOT != captured_path
    assert REPO_ROOT not in captured_path.parents
    assert not captured_path.exists()


# --- Partial-review honesty layer --------------------------------------------------


def test_partial_review_keeps_pass_rate_none_and_reports_reviewed_only_rate(tmp_path):
    rows = []
    for n in range(1, 62):
        if n <= 8:
            verdict = "fail"
        elif n <= 10:
            verdict = "pass"
        else:
            verdict = ""
        rows.append(
            {
                "clip_number": n,
                "n_tracks": 20,
                "longest_track_frac": 1.0,
                "n_fragments": 2,
                "auto_flag": "ok",
                "verdict": verdict,
                "id_switches": 1 if verdict == "fail" else None,
                "reviewer_note": "",
            }
        )
    path = tmp_path / "partial_review.csv"
    _write_comma_utf8_fixture(path, rows)

    summary = summarise_review(path)
    assert summary["pass_rate"] is None
    assert summary["n_clips"] == 61
    assert summary["n_reviewed"] == 10

    rate = cm.reviewed_only_rate(summary)
    assert rate == {
        "k": 2,
        "n": 10,
        "complete": False,
        "note": "unvollstaendig (10/61 geprueft)",
    }


def test_reviewed_only_rate_complete_when_fully_reviewed():
    summary = {
        "n_clips": 3,
        "n_reviewed": 3,
        "n_pass": 1,
        "n_fail": 2,
        "pass_rate": 1 / 3,
        "unreviewed_clips": [],
    }

    rate = cm.reviewed_only_rate(summary)

    assert rate == {"k": 1, "n": 3, "complete": True, "note": None}


# --- Rendering ---------------------------------------------------------------


def test_render_markdown_emits_rates_guard_and_blind_spot():
    report = {
        "splits": {
            "dev": {
                "n": 61,
                "human_rate": {"k": 15, "n": 61},
                "mean_fragments_per_expected_player": 0.7,
                "mean_active_track_count_deviation": 1.2,
                "reviewed_only": None,
            },
            "private_test": {
                "n": 61,
                "human_rate": None,
                "mean_fragments_per_expected_player": 0.9,
                "mean_active_track_count_deviation": 1.5,
                "reviewed_only": {
                    "k": 2,
                    "n": 10,
                    "complete": False,
                    "note": "unvollstaendig (10/61 geprueft)",
                },
            },
        }
    }

    markdown = cm.render_markdown(report)

    assert "| dev | 61 | 15/61 (24.59%) | 0.7000 | 1.2000 |" in markdown
    assert "| private_test | 61 | n/a | 0.9000 | 1.5000 |" in markdown
    assert cm.BLIND_SPOT_NOTE in markdown
    assert cm.PARTIAL_REVIEW_LABEL in markdown
    assert "2/10 (20.00%)" in markdown
    assert "unvollstaendig (10/61 geprueft)" in markdown


# --- identity_metric.py: M2-3-ready label-based association interface -------------

IDENTITY_COLUMNS = (
    "session_id",
    "clip_number",
    "frame_index",
    "track_id",
    "bbox_x1",
    "bbox_y1",
    "bbox_x2",
    "bbox_y2",
)


def _identity_row(track_id: int, frame_index: int, x: float, y: float) -> dict:
    return {
        "session_id": "test-session-dev",
        "clip_number": 1,
        "frame_index": frame_index,
        "track_id": track_id,
        "bbox_x1": x,
        "bbox_y1": y,
        "bbox_x2": x + 10.0,
        "bbox_y2": y + 10.0,
    }


def _empty_identity_df() -> pl.DataFrame:
    schema = {
        "session_id": pl.Utf8,
        "clip_number": pl.Int64,
        "frame_index": pl.Int64,
        "track_id": pl.Int64,
        "bbox_x1": pl.Float64,
        "bbox_y1": pl.Float64,
        "bbox_x2": pl.Float64,
        "bbox_y2": pl.Float64,
    }
    return pl.DataFrame(schema=schema)


def test_frame_events_covers_frames_present_in_either_input():
    gt = pl.DataFrame([_identity_row(1, f, 0.0, 0.0) for f in range(5)]).select(list(IDENTITY_COLUMNS))
    hyp = pl.DataFrame([_identity_row(1, f, 0.0, 0.0) for f in range(3, 8)]).select(list(IDENTITY_COLUMNS))

    events = im.frame_events(gt, hyp, max_distance_px=10.0)

    assert [e[0] for e in events] == list(range(8))


def test_frame_events_matrix_shape_and_nan_threshold():
    # gt bbox centre (5, 5); hyp bbox centre (8, 5) -- a 3px offset.
    gt = pl.DataFrame([_identity_row(1, 0, 0.0, 0.0)]).select(list(IDENTITY_COLUMNS))
    hyp = pl.DataFrame([_identity_row(1, 0, 3.0, 0.0)]).select(list(IDENTITY_COLUMNS))

    events_wide = im.frame_events(gt, hyp, max_distance_px=10.0)
    events_narrow = im.frame_events(gt, hyp, max_distance_px=2.0)

    _, gt_ids, hyp_ids, wide_matrix = events_wide[0]
    assert gt_ids == [1]
    assert hyp_ids == [1]
    assert wide_matrix.shape == (1, 1)
    assert not np.isnan(wide_matrix[0, 0])
    assert wide_matrix[0, 0] == pytest.approx(3.0)

    _, _, _, narrow_matrix = events_narrow[0]
    assert np.isnan(narrow_matrix[0, 0])


def test_frame_events_one_sided_frame_yields_empty_list_on_other_side():
    gt = pl.DataFrame([_identity_row(1, 0, 0.0, 0.0)]).select(list(IDENTITY_COLUMNS))
    hyp = _empty_identity_df()

    events = im.frame_events(gt, hyp, max_distance_px=10.0)

    assert len(events) == 1
    frame_index, gt_ids, hyp_ids, distance_matrix = events[0]
    assert frame_index == 0
    assert gt_ids == [1]
    assert hyp_ids == []
    assert distance_matrix.shape == (1, 0)


def test_compute_identity_metrics_raises_actionable_error_when_motmetrics_missing(monkeypatch):
    monkeypatch.setitem(sys.modules, "motmetrics", None)
    gt = pl.DataFrame([_identity_row(1, 0, 0.0, 0.0)]).select(list(IDENTITY_COLUMNS))
    hyp = gt.clone()

    with pytest.raises(RuntimeError, match="motmetrics"):
        im.compute_identity_metrics(gt, hyp)


def test_identity_report_keys_are_the_documented_future_cli_surface():
    assert im.IDENTITY_REPORT_KEYS == ("idf1", "mota", "num_switches", "n_frames", "max_distance_px")


def test_compute_identity_metrics_perfect_hypothesis_scores_idf1_one():
    pytest.importorskip("motmetrics")

    rows = [_identity_row(track_id, f, track_id * 100.0, 0.0) for track_id in (1, 2) for f in range(N_FRAMES)]
    gt = pl.DataFrame(rows).select(list(IDENTITY_COLUMNS))
    hyp = gt.clone()

    result = im.compute_identity_metrics(gt, hyp, max_distance_px=10.0, name="perfect")

    assert result["idf1"] == pytest.approx(1.0)
    assert result["num_switches"] == 0
    assert result["n_frames"] == N_FRAMES


def test_compute_identity_metrics_swapped_hypothesis_scores_below_one():
    pytest.importorskip("motmetrics")

    gt_rows, hyp_rows = [], []
    for frame_index in range(N_FRAMES):
        t1_x, t2_x = 0.0, 100.0
        gt_rows.append(_identity_row(1, frame_index, t1_x, 0.0))
        gt_rows.append(_identity_row(2, frame_index, t2_x, 0.0))
        if frame_index >= N_FRAMES // 2:
            t1_x, t2_x = t2_x, t1_x
        hyp_rows.append(_identity_row(1, frame_index, t1_x, 0.0))
        hyp_rows.append(_identity_row(2, frame_index, t2_x, 0.0))

    gt = pl.DataFrame(gt_rows).select(list(IDENTITY_COLUMNS))
    hyp = pl.DataFrame(hyp_rows).select(list(IDENTITY_COLUMNS))

    result = im.compute_identity_metrics(gt, hyp, max_distance_px=10.0, name="swapped")

    assert result["idf1"] < 1.0
    assert result["num_switches"] >= 1
