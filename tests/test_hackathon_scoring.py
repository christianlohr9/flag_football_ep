"""Subprocess coverage for `scripts/hackathon/score_tracks.py`: schema validation,
denominator discipline (every printed rate carries `k/n`), the perfect-vs-degraded
aggregate comparison, the flag-pull bonus precision/recall path, Split-Modus (one run,
both splits, threshold rate AND continuous metric), the wrong-split guard, the vault
review dialect (semicolon/cp1252/CRLF), the Markdown report, the legacy report
contract regression and one real-data smoke test over the vaulted Puerto Rico review.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "hackathon" / "score_tracks.py"

sys.path.insert(0, str(REPO_ROOT / "scripts" / "hackathon"))
import continuous_metric as cm  # noqa: E402

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

CLIP_NUMBERS = (1, 2, 3)
N_FRAMES = 20

RATE_RE = re.compile(r"\d+/\d+ \(\d+\.\d+%\)")

# Synthetic session ids for the split-mode fixtures (pii_discipline: never a real
# session/player name).
DEV_SESSION = "test-session-dev"
TEST_SESSION = "test-session-pt"


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def _write_review_csv(path: Path, verdicts: dict[int, str]) -> None:
    rows = [
        {
            "clip_number": n,
            "n_tracks": 4,
            "longest_track_frac": 1.0,
            "n_fragments": 0,
            "auto_flag": "ok",
            "verdict": verdicts[n],
            "id_switches": 0,
            "reviewer_note": "",
        }
        for n in CLIP_NUMBERS
    ]
    pl.DataFrame(rows).write_csv(path)


def _write_perfect_tracks(path: Path) -> None:
    """One track per player, each spanning every frame of every clip (no
    fragmentation, `n_tracks=4 >= _EXPECTED_MIN_TRACKS`) -- `auto_flag` should read
    `ok` for every clip.
    """
    rows = []
    for n in CLIP_NUMBERS:
        for track_id in range(4):
            for frame_index in range(N_FRAMES):
                rows.append(
                    {
                        "session_id": "test-session",
                        "clip_number": n,
                        "frame_index": frame_index,
                        "track_id": track_id,
                        "bbox_x1": 0.0,
                        "bbox_y1": 0.0,
                        "bbox_x2": 10.0,
                        "bbox_y2": 10.0,
                    }
                )
    pl.DataFrame(rows).select(list(TRACK_COLUMNS)).write_csv(path)


def _write_degraded_tracks(path: Path) -> None:
    """Eight short-lived tracks per clip, each covering only a quarter of the clip's
    frames -- every track falls under the fragment-coverage threshold, so
    `n_fragments > n_tracks/2` and `auto_flag` should read `fragmented`.
    """
    rows = []
    for n in CLIP_NUMBERS:
        for track_id in range(8):
            start = (track_id % 4) * 5
            for frame_index in range(start, start + 5):
                rows.append(
                    {
                        "session_id": "test-session",
                        "clip_number": n,
                        "frame_index": frame_index,
                        "track_id": track_id,
                        "bbox_x1": 0.0,
                        "bbox_y1": 0.0,
                        "bbox_x2": 10.0,
                        "bbox_y2": 10.0,
                    }
                )
    pl.DataFrame(rows).select(list(TRACK_COLUMNS)).write_csv(path)


def _write_split_tracks_csv(
    path: Path, session_id: str, clip_numbers: tuple[int, ...] = CLIP_NUMBERS
) -> None:
    """Perfect-shaped tracks (no fragmentation) for a given `session_id`, used by the
    split-mode fixtures. Clip numbers OVERLAP with the other session's on purpose
    (both use 1-3) -- the role guard, not the clip number, is what must separate the
    two splits.
    """
    rows = []
    for n in clip_numbers:
        for track_id in range(4):
            for frame_index in range(N_FRAMES):
                rows.append(
                    {
                        "session_id": session_id,
                        "clip_number": n,
                        "frame_index": frame_index,
                        "track_id": track_id,
                        "bbox_x1": 0.0,
                        "bbox_y1": 0.0,
                        "bbox_x2": 10.0,
                        "bbox_y2": 10.0,
                    }
                )
    pl.DataFrame(rows).select(list(TRACK_COLUMNS)).write_csv(path)


def _write_split_csv(
    path: Path,
    dev_session: str = DEV_SESSION,
    test_session: str = TEST_SESSION,
    dev_clips: tuple[int, ...] = CLIP_NUMBERS,
    test_clips: tuple[int, ...] = CLIP_NUMBERS,
) -> None:
    rows = []
    for n in dev_clips:
        rows.append(
            {
                "domain": "drone",
                "session_id": dev_session,
                "clip_number": n,
                "hackathon_role": "dev",
                "frozen_at": "2026-01-01",
                "note": "",
            }
        )
    for n in test_clips:
        rows.append(
            {
                "domain": "drone",
                "session_id": test_session,
                "clip_number": n,
                "hackathon_role": "private_test",
                "frozen_at": "2026-01-01",
                "note": "",
            }
        )
    pl.DataFrame(rows).write_csv(path)


def _write_vault_dialect_review_csv(path: Path, verdicts: dict[int, str | None]) -> None:
    """Semicolon-delimited, cp1252-encoded, CRLF-terminated review rows -- mirrors the
    real vault file's dialect. `verdicts[n] = None` means unreviewed (empty verdict
    cell, the partial-review case).
    """
    header = "clip_number;n_tracks;longest_track_frac;n_fragments;auto_flag;verdict;id_switches;reviewer_note"
    lines = [header]
    for n, verdict in verdicts.items():
        verdict_str = verdict or ""
        id_switches = "0" if verdict is not None else ""
        lines.append(f"{n};4;1.0;0;ok;{verdict_str};{id_switches};")
    text = "\r\n".join(lines) + "\r\n"
    path.write_bytes(text.encode("cp1252"))


def _assert_no_bare_percentage(text: str) -> None:
    """Every `\\d+\\.\\d+%` in `text` must be part of a full `k/n (p%)` match -- strip
    every RATE_RE match out and assert nothing percentage-shaped survives.
    """
    stripped = RATE_RE.sub("", text)
    bare = re.search(r"\d+\.\d+%", stripped)
    assert bare is None, f"bare percentage without denominator: {bare.group(0)!r} near {stripped[:300]!r}"


# --- Legacy Einzel-Modus tests (unchanged behaviour) -----------------------------


def test_help_documents_all_four_flags() -> None:
    result = _run(["--help"])
    assert result.returncode == 0
    for flag in (
        "--tracks",
        "--review",
        "--flag-pulls",
        "--out",
        "--out-md",
        "--tracks-dev",
        "--review-dev",
        "--tracks-test",
        "--review-test",
        "--split",
    ):
        assert flag in result.stdout, result.stdout


def test_missing_track_id_column_fails_naming_the_column(tmp_path: Path) -> None:
    tracks_path = tmp_path / "tracks.csv"
    df = pl.DataFrame(
        [
            {
                "session_id": "test-session",
                "clip_number": 1,
                "frame_index": 0,
                "bbox_x1": 0.0,
                "bbox_y1": 0.0,
                "bbox_x2": 10.0,
                "bbox_y2": 10.0,
            }
        ]
    )
    df.write_csv(tracks_path)

    result = _run(["--tracks", str(tracks_path)])

    assert result.returncode != 0
    assert "track_id" in result.stderr, result.stderr


def test_perfect_submission_every_rate_carries_denominator(tmp_path: Path) -> None:
    tracks_path = tmp_path / "tracks.csv"
    review_path = tmp_path / "continuity_review.csv"
    _write_perfect_tracks(tracks_path)
    _write_review_csv(review_path, {1: "pass", 2: "pass", 3: "fail"})

    result = _run(["--tracks", str(tracks_path), "--review", str(review_path)])

    assert result.returncode == 0, result.stderr
    rate_lines = [line for line in result.stdout.splitlines() if "Automatische Kontinuitaet" in line]
    assert rate_lines and RATE_RE.search(rate_lines[0]), result.stdout
    reference_lines = [line for line in result.stdout.splitlines() if "Referenz-Baseline" in line]
    assert reference_lines and RATE_RE.search(reference_lines[0]), result.stdout


def test_degraded_submission_scores_worse_than_perfect_on_aggregate(tmp_path: Path) -> None:
    review_path = tmp_path / "continuity_review.csv"
    _write_review_csv(review_path, {1: "pass", 2: "pass", 3: "fail"})

    perfect_tracks = tmp_path / "perfect.csv"
    degraded_tracks = tmp_path / "degraded.csv"
    _write_perfect_tracks(perfect_tracks)
    _write_degraded_tracks(degraded_tracks)

    perfect_out = tmp_path / "perfect_report.json"
    degraded_out = tmp_path / "degraded_report.json"

    perfect_result = _run(
        ["--tracks", str(perfect_tracks), "--review", str(review_path), "--out", str(perfect_out)]
    )
    degraded_result = _run(
        ["--tracks", str(degraded_tracks), "--review", str(review_path), "--out", str(degraded_out)]
    )

    assert perfect_result.returncode == 0, perfect_result.stderr
    assert degraded_result.returncode == 0, degraded_result.stderr

    perfect_report = json.loads(perfect_out.read_text(encoding="utf-8"))
    degraded_report = json.loads(degraded_out.read_text(encoding="utf-8"))

    assert perfect_report["auto"]["n_ok"] == 3
    assert degraded_report["auto"]["n_ok"] == 0
    assert degraded_report["auto"]["rate"] < perfect_report["auto"]["rate"]


def test_flag_pull_precision_recall_path(tmp_path: Path) -> None:
    review_path = tmp_path / "continuity_review.csv"
    _write_review_csv(review_path, {1: "pass", 2: "pass", 3: "fail"})

    ground_truth_path = tmp_path / "flag_pull_events.csv"
    pl.DataFrame(
        [
            {
                "clip_number": 1,
                "outcome": "pull",
                "pull_time_s": 5.0,
                "carrier_track_id": 1,
                "puller_track_id": "2",
                "notes": "",
            },
            {
                "clip_number": 2,
                "outcome": "pull",
                "pull_time_s": 7.2,
                "carrier_track_id": 3,
                "puller_track_id": "0",
                "notes": "",
            },
            {
                "clip_number": 3,
                "outcome": "incomplete",
                "pull_time_s": None,
                "carrier_track_id": None,
                "puller_track_id": "",
                "notes": "",
            },
        ]
    ).write_csv(ground_truth_path)

    predicted_path = tmp_path / "predicted_pulls.csv"
    pl.DataFrame(
        [
            # within +-0.5s of clip 1's ground truth pull -> hit
            {
                "clip_number": 1,
                "outcome": "pull",
                "pull_time_s": 5.2,
                "carrier_track_id": 1,
                "puller_track_id": "2",
                "notes": "",
            },
            # no matching ground-truth pull anywhere near this time -> false positive
            {
                "clip_number": 2,
                "outcome": "pull",
                "pull_time_s": 1.0,
                "carrier_track_id": 3,
                "puller_track_id": "0",
                "notes": "",
            },
        ]
    ).write_csv(predicted_path)

    tracks_path = tmp_path / "tracks.csv"
    _write_perfect_tracks(tracks_path)

    result = _run(
        [
            "--tracks",
            str(tracks_path),
            "--review",
            str(review_path),
            "--flag-pulls",
            str(predicted_path),
        ]
    )

    assert result.returncode == 0, result.stderr
    precision_lines = [line for line in result.stdout.splitlines() if "Flag-Pull Precision" in line]
    recall_lines = [line for line in result.stdout.splitlines() if "Flag-Pull Recall" in line]
    assert precision_lines and RATE_RE.search(precision_lines[0]), result.stdout
    assert recall_lines and RATE_RE.search(recall_lines[0]), result.stdout
    # tp=1 (clip 1), fp=1 (clip 2's prediction has no time-matching truth), fn=1 (clip 2's truth unmatched)
    assert "1/2" in precision_lines[0]
    assert "1/2" in recall_lines[0]


def test_legacy_single_mode_report_contract_regression(tmp_path: Path) -> None:
    """Pins the exact legacy report keys/values `baseline_common.summarise()` reads
    (`n_clips`, `per_clip[]`, `auto{n_ok,n_clips,rate}`, `human_reference`,
    `flag_pulls`) for a fixed fixture -- the guard for the six committed M2-2 rows.
    The report may ADD `continuous`/`guard`/`blind_spot`, never move or drop a legacy
    key or change a legacy value.
    """
    tracks_path = tmp_path / "tracks.csv"
    review_path = tmp_path / "continuity_review.csv"
    out_path = tmp_path / "report.json"
    _write_perfect_tracks(tracks_path)
    _write_review_csv(review_path, {1: "pass", 2: "pass", 3: "fail"})

    result = _run(
        ["--tracks", str(tracks_path), "--review", str(review_path), "--out", str(out_path)]
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(out_path.read_text(encoding="utf-8"))

    assert report["n_clips"] == 3
    assert report["auto"] == {"n_ok": 3, "n_clips": 3, "rate": 1.0}
    assert report["flag_pulls"] is None
    assert report["human_reference"]["n_pass"] == 2
    assert report["human_reference"]["n_clips"] == 3
    assert report["human_reference"]["pass_rate"] == pytest.approx(2 / 3)

    assert len(report["per_clip"]) == 3
    for row in report["per_clip"]:
        assert row["n_tracks"] == 4
        assert row["longest_track_frac"] == pytest.approx(1.0)
        assert row["n_fragments"] == 0
        assert row["auto_flag"] == "ok"
        # Legacy per_clip rows must NOT gain continuous-metric keys -- that would be
        # an undocumented shape change even though the values above are unchanged.
        assert "fragments_per_expected_player" not in row

    # Additive-only: the new keys exist, the legacy keys above are untouched by them.
    assert report["continuous"]["mean_fragments_per_expected_player"] == pytest.approx(0.0)
    assert report["guard"]["mean_active_track_count_deviation"] is not None
    assert report["blind_spot"] == cm.BLIND_SPOT_NOTE
    # Legacy single-run mode does not promote human_reference_reviewed_only to the
    # top level (Split-Modus-only key, per plan M2-04-02).
    assert "human_reference_reviewed_only" not in report


# --- Split-Modus tests -----------------------------------------------------------


def test_split_mode_reports_threshold_and_continuous_for_both_splits(tmp_path: Path) -> None:
    dev_tracks = tmp_path / "dev_tracks.csv"
    test_tracks = tmp_path / "test_tracks.csv"
    dev_review = tmp_path / "dev_review.csv"
    test_review = tmp_path / "test_review.csv"
    split_csv = tmp_path / "hackathon_split.csv"

    _write_split_tracks_csv(dev_tracks, DEV_SESSION)
    _write_split_tracks_csv(test_tracks, TEST_SESSION)
    _write_review_csv(dev_review, {1: "pass", 2: "pass", 3: "fail"})
    _write_vault_dialect_review_csv(test_review, {1: "pass", 2: None, 3: None})
    _write_split_csv(split_csv)

    result = _run(
        [
            "--tracks-dev",
            str(dev_tracks),
            "--review-dev",
            str(dev_review),
            "--tracks-test",
            str(test_tracks),
            "--review-test",
            str(test_review),
            "--split",
            str(split_csv),
        ]
    )

    assert result.returncode == 0, result.stderr
    assert "=== Split: dev (n=3) ===" in result.stdout
    assert "=== Split: private_test (n=3) ===" in result.stdout

    dev_block = result.stdout.split("=== Split: dev")[1].split("=== Split: private_test")[0]
    test_block = result.stdout.split("=== Split: private_test")[1]

    for block in (dev_block, test_block):
        auto_lines = [line for line in block.splitlines() if "Automatische Kontinuitaet" in line]
        assert auto_lines and RATE_RE.search(auto_lines[0]), block
        continuous_lines = [line for line in block.splitlines() if "Stetige Kennzahl" in line]
        assert continuous_lines, block
        assert cm.BLIND_SPOT_NOTE in block

    assert "Referenz-Baseline" in dev_block
    assert "nicht auswertbar" in test_block
    assert cm.PARTIAL_REVIEW_LABEL in test_block


def test_split_mode_json_report_shape(tmp_path: Path) -> None:
    dev_tracks = tmp_path / "dev_tracks.csv"
    test_tracks = tmp_path / "test_tracks.csv"
    dev_review = tmp_path / "dev_review.csv"
    test_review = tmp_path / "test_review.csv"
    split_csv = tmp_path / "hackathon_split.csv"
    out_path = tmp_path / "report.json"

    _write_split_tracks_csv(dev_tracks, DEV_SESSION)
    _write_split_tracks_csv(test_tracks, TEST_SESSION)
    _write_review_csv(dev_review, {1: "pass", 2: "pass", 3: "fail"})
    _write_vault_dialect_review_csv(test_review, {1: "pass", 2: None, 3: None})
    _write_split_csv(split_csv)

    result = _run(
        [
            "--tracks-dev",
            str(dev_tracks),
            "--review-dev",
            str(dev_review),
            "--tracks-test",
            str(test_tracks),
            "--review-test",
            str(test_review),
            "--split",
            str(split_csv),
            "--out",
            str(out_path),
        ]
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(out_path.read_text(encoding="utf-8"))

    assert report["mode"] == "split"
    assert report["blind_spot"] == cm.BLIND_SPOT_NOTE
    assert report["guard_note"] == cm.GUARD_NOTE
    assert set(report["splits"].keys()) == {"dev", "private_test"}

    expected_keys = {
        "n_clips",
        "per_clip",
        "auto",
        "human_reference",
        "human_reference_reviewed_only",
        "continuous",
        "guard",
    }
    for split in report["splits"].values():
        assert expected_keys <= set(split.keys())
        assert split["n_clips"] == 3


def test_partial_test_review_keeps_pass_rate_null_and_adds_reviewed_only(tmp_path: Path) -> None:
    dev_tracks = tmp_path / "dev_tracks.csv"
    test_tracks = tmp_path / "test_tracks.csv"
    dev_review = tmp_path / "dev_review.csv"
    test_review = tmp_path / "test_review.csv"
    split_csv = tmp_path / "hackathon_split.csv"
    out_path = tmp_path / "report.json"

    _write_split_tracks_csv(dev_tracks, DEV_SESSION)
    _write_split_tracks_csv(test_tracks, TEST_SESSION)
    _write_review_csv(dev_review, {1: "pass", 2: "pass", 3: "fail"})
    _write_vault_dialect_review_csv(test_review, {1: "pass", 2: None, 3: None})
    _write_split_csv(split_csv)

    result = _run(
        [
            "--tracks-dev",
            str(dev_tracks),
            "--review-dev",
            str(dev_review),
            "--tracks-test",
            str(test_tracks),
            "--review-test",
            str(test_review),
            "--split",
            str(split_csv),
            "--out",
            str(out_path),
        ]
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(out_path.read_text(encoding="utf-8"))
    test_split = report["splits"]["private_test"]

    assert test_split["human_reference"]["pass_rate"] is None
    reviewed_only = test_split["human_reference_reviewed_only"]
    assert reviewed_only["k"] == 1
    assert reviewed_only["n"] == 1
    assert reviewed_only["complete"] is False
    assert "unvollstaendig" in reviewed_only["note"]

    # Never manufactured: the continuous metric needs no verdicts at all.
    assert test_split["continuous"]["mean_fragments_per_expected_player"] is not None
    assert test_split["guard"]["mean_active_track_count_deviation"] is not None


def test_role_mismatch_fails_naming_the_offending_session_and_clip(tmp_path: Path) -> None:
    dev_tracks = tmp_path / "dev_tracks.csv"
    test_tracks = tmp_path / "test_tracks.csv"
    split_csv = tmp_path / "hackathon_split.csv"

    # Both track files use the DEV session id -- the file passed under
    # --tracks-test is really dev-role tracks, which must fail loudly.
    _write_split_tracks_csv(dev_tracks, DEV_SESSION)
    _write_split_tracks_csv(test_tracks, DEV_SESSION)
    _write_split_csv(split_csv)

    result = _run(
        [
            "--tracks-dev",
            str(dev_tracks),
            "--tracks-test",
            str(test_tracks),
            "--split",
            str(split_csv),
        ]
    )

    assert result.returncode != 0
    assert DEV_SESSION in result.stderr
    assert any(str(n) in result.stderr for n in CLIP_NUMBERS)
    assert "hackathon_role" in result.stderr


def test_role_mismatch_unknown_pair_fails(tmp_path: Path) -> None:
    dev_tracks = tmp_path / "dev_tracks.csv"
    test_tracks = tmp_path / "test_tracks.csv"
    split_csv = tmp_path / "hackathon_split.csv"

    _write_split_tracks_csv(dev_tracks, DEV_SESSION)
    _write_split_tracks_csv(test_tracks, "unknown-session-not-in-split")
    _write_split_csv(split_csv)

    result = _run(
        [
            "--tracks-dev",
            str(dev_tracks),
            "--tracks-test",
            str(test_tracks),
            "--split",
            str(split_csv),
        ]
    )

    assert result.returncode != 0
    assert "unknown-session-not-in-split" in result.stderr


def test_split_mode_without_split_flag_fails_naming_it(tmp_path: Path) -> None:
    dev_tracks = tmp_path / "dev_tracks.csv"
    _write_split_tracks_csv(dev_tracks, DEV_SESSION)

    result = _run(["--tracks-dev", str(dev_tracks)])

    assert result.returncode != 0
    assert "--split" in result.stderr


def test_legacy_and_split_flags_are_mutually_exclusive(tmp_path: Path) -> None:
    tracks_path = tmp_path / "tracks.csv"
    dev_tracks = tmp_path / "dev_tracks.csv"
    split_csv = tmp_path / "hackathon_split.csv"
    _write_perfect_tracks(tracks_path)
    _write_split_tracks_csv(dev_tracks, DEV_SESSION)
    _write_split_csv(split_csv)

    result = _run(
        [
            "--tracks",
            str(tracks_path),
            "--tracks-dev",
            str(dev_tracks),
            "--split",
            str(split_csv),
        ]
    )

    assert result.returncode != 0
    assert "--tracks-dev" in result.stderr or "--tracks" in result.stderr


def test_neither_tracks_nor_split_mode_given_fails(tmp_path: Path) -> None:
    result = _run([])
    assert result.returncode != 0


def test_flag_pulls_rejected_in_split_mode(tmp_path: Path) -> None:
    dev_tracks = tmp_path / "dev_tracks.csv"
    test_tracks = tmp_path / "test_tracks.csv"
    split_csv = tmp_path / "hackathon_split.csv"
    predicted_path = tmp_path / "predicted_pulls.csv"

    _write_split_tracks_csv(dev_tracks, DEV_SESSION)
    _write_split_tracks_csv(test_tracks, TEST_SESSION)
    _write_split_csv(split_csv)
    pl.DataFrame(
        [{"clip_number": 1, "outcome": "pull", "pull_time_s": 1.0, "carrier_track_id": 1, "puller_track_id": "2", "notes": ""}]
    ).write_csv(predicted_path)

    result = _run(
        [
            "--tracks-dev",
            str(dev_tracks),
            "--tracks-test",
            str(test_tracks),
            "--split",
            str(split_csv),
            "--flag-pulls",
            str(predicted_path),
        ]
    )

    assert result.returncode != 0
    assert "--flag-pulls" in result.stderr


# --- Markdown / blind-spot / denominator discipline tests (Task 2) --------------


def test_split_mode_out_md_contains_both_labels_and_table(tmp_path: Path) -> None:
    dev_tracks = tmp_path / "dev_tracks.csv"
    test_tracks = tmp_path / "test_tracks.csv"
    dev_review = tmp_path / "dev_review.csv"
    test_review = tmp_path / "test_review.csv"
    split_csv = tmp_path / "hackathon_split.csv"
    out_path = tmp_path / "report.json"
    out_md_path = tmp_path / "report.md"

    _write_split_tracks_csv(dev_tracks, DEV_SESSION)
    _write_split_tracks_csv(test_tracks, TEST_SESSION)
    _write_review_csv(dev_review, {1: "pass", 2: "pass", 3: "fail"})
    _write_vault_dialect_review_csv(test_review, {1: "pass", 2: None, 3: None})
    _write_split_csv(split_csv)

    result = _run(
        [
            "--tracks-dev",
            str(dev_tracks),
            "--review-dev",
            str(dev_review),
            "--tracks-test",
            str(test_tracks),
            "--review-test",
            str(test_review),
            "--split",
            str(split_csv),
            "--out",
            str(out_path),
            "--out-md",
            str(out_md_path),
        ]
    )
    assert result.returncode == 0, result.stderr
    assert out_md_path.exists()
    markdown = out_md_path.read_text(encoding="utf-8")

    assert "dev" in markdown
    assert "private_test" in markdown
    assert "|" in markdown
    assert RATE_RE.search(markdown), markdown
    assert cm.BLIND_SPOT_NOTE in markdown
    assert cm.PARTIAL_REVIEW_LABEL in markdown
    _assert_no_bare_percentage(markdown)


def test_single_mode_out_md_uses_gesamt_label(tmp_path: Path) -> None:
    tracks_path = tmp_path / "tracks.csv"
    review_path = tmp_path / "continuity_review.csv"
    out_md_path = tmp_path / "report.md"
    _write_perfect_tracks(tracks_path)
    _write_review_csv(review_path, {1: "pass", 2: "pass", 3: "fail"})

    result = _run(
        [
            "--tracks",
            str(tracks_path),
            "--review",
            str(review_path),
            "--out-md",
            str(out_md_path),
        ]
    )
    assert result.returncode == 0, result.stderr
    markdown = out_md_path.read_text(encoding="utf-8")
    assert "gesamt" in markdown
    assert "|" in markdown
    assert cm.BLIND_SPOT_NOTE in markdown
    _assert_no_bare_percentage(markdown)


def test_json_blind_spot_equals_module_constant(tmp_path: Path) -> None:
    tracks_path = tmp_path / "tracks.csv"
    review_path = tmp_path / "continuity_review.csv"
    out_path = tmp_path / "report.json"
    _write_perfect_tracks(tracks_path)
    _write_review_csv(review_path, {1: "pass", 2: "pass", 3: "fail"})

    result = _run(
        ["--tracks", str(tracks_path), "--review", str(review_path), "--out", str(out_path)]
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(out_path.read_text(encoding="utf-8"))
    assert report["blind_spot"] == cm.BLIND_SPOT_NOTE


def test_no_bare_percentage_in_stdout_json_or_markdown(tmp_path: Path) -> None:
    dev_tracks = tmp_path / "dev_tracks.csv"
    test_tracks = tmp_path / "test_tracks.csv"
    dev_review = tmp_path / "dev_review.csv"
    test_review = tmp_path / "test_review.csv"
    split_csv = tmp_path / "hackathon_split.csv"
    out_path = tmp_path / "report.json"
    out_md_path = tmp_path / "report.md"

    _write_split_tracks_csv(dev_tracks, DEV_SESSION)
    _write_split_tracks_csv(test_tracks, TEST_SESSION)
    _write_review_csv(dev_review, {1: "pass", 2: "pass", 3: "fail"})
    _write_vault_dialect_review_csv(test_review, {1: "pass", 2: None, 3: None})
    _write_split_csv(split_csv)

    result = _run(
        [
            "--tracks-dev",
            str(dev_tracks),
            "--review-dev",
            str(dev_review),
            "--tracks-test",
            str(test_tracks),
            "--review-test",
            str(test_review),
            "--split",
            str(split_csv),
            "--out",
            str(out_path),
            "--out-md",
            str(out_md_path),
        ]
    )
    assert result.returncode == 0, result.stderr
    _assert_no_bare_percentage(result.stdout)
    _assert_no_bare_percentage(out_path.read_text(encoding="utf-8"))
    _assert_no_bare_percentage(out_md_path.read_text(encoding="utf-8"))


# --- Real-data regression (Task 3) -----------------------------------------------

VAULT_REVIEW = (
    REPO_ROOT
    / "data"
    / "private"
    / "test-labels"
    / "2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE"
    / "continuity_review.csv"
)
DEV_REVIEW = REPO_ROOT / "data" / "reference" / "continuity_review.csv"
DEV_TRACKS_FULL = (
    REPO_ROOT / "data" / "processed" / "tracking" / "2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet"
)
TEST_TRACKS_FULL = (
    REPO_ROOT
    / "data"
    / "processed"
    / "tracking"
    / "2026-05-16_FRIENDLY-GER-vs-PUERTORICO-DRONE-WIDE_tracks.parquet"
)
SPLIT_CSV_REAL = REPO_ROOT / "data" / "reference" / "hackathon_split.csv"


def _real_inputs_available() -> bool:
    return all(
        p.exists()
        for p in (VAULT_REVIEW, DEV_REVIEW, DEV_TRACKS_FULL, TEST_TRACKS_FULL, SPLIT_CSV_REAL)
    )


def test_real_split_mode_run_reports_honesty_flags_for_both_splits(tmp_path: Path) -> None:
    if not _real_inputs_available():
        pytest.skip("real dev/test tracking + vault review inputs not present in this environment")

    # Keep the test under 30s: filter the (large) real tracks parquets to a 3-clip
    # subset before scoring. Reviews stay full (61 rows each) -- the honesty flags
    # this test asserts (reviewed-only rate, "unvollstaendig") depend on the full
    # review, not on which clips happen to have tracks.
    dev_subset = tmp_path / "dev_tracks_subset.parquet"
    test_subset = tmp_path / "test_tracks_subset.parquet"
    pl.read_parquet(DEV_TRACKS_FULL).filter(pl.col("clip_number").is_in([1, 2, 3])).write_parquet(dev_subset)
    pl.read_parquet(TEST_TRACKS_FULL).filter(pl.col("clip_number").is_in([1, 2, 3])).write_parquet(test_subset)

    out_path = tmp_path / "report.json"
    result = _run(
        [
            "--tracks-dev",
            str(dev_subset),
            "--review-dev",
            str(DEV_REVIEW),
            "--tracks-test",
            str(test_subset),
            "--review-test",
            str(VAULT_REVIEW),
            "--split",
            str(SPLIT_CSV_REAL),
            "--out",
            str(out_path),
        ]
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(out_path.read_text(encoding="utf-8"))

    dev = report["splits"]["dev"]
    test = report["splits"]["private_test"]

    assert dev["n_clips"] == 61
    assert test["n_clips"] == 61
    assert dev["human_reference"]["pass_rate"] is not None
    assert dev["human_reference"]["n_pass"] == 15

    # The private-test review is human work in progress: its row count moves as the
    # user labels, so assert the honesty contract for whichever state the vault is in.
    reviewed_only = test["human_reference_reviewed_only"]
    if reviewed_only["complete"]:
        assert reviewed_only["n"] == 61
        assert test["human_reference"]["pass_rate"] is not None
    else:
        assert test["human_reference"]["pass_rate"] is None
        assert 0 < reviewed_only["n"] < 61
        assert "unvollstaendig" in reviewed_only["note"]

    for split in (dev, test):
        assert split["continuous"]["mean_fragments_per_expected_player"] is not None
        assert split["guard"]["mean_active_track_count_deviation"] is not None

    # A constant metric would be useless as a direction (METR-01's whole point).
    fpp_values = [row["fragments_per_expected_player"] for row in dev["continuous"]["per_clip"]]
    assert len(set(fpp_values)) > 1, fpp_values
