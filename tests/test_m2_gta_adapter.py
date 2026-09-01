"""Tests for `scripts/hackathon/measure_gta.py`: the checkpoint-integrity guard,
the pass-through guarantee for tracks without embedded crops, the no-row-lost
invariant across a synthetic split-like and merge-like refinement, output schema
conformance, and the two forbidden-artifact guards (no local DBSCAN
reimplementation, no mention of the forbidden checkpoint/PyPI package).

Synthetic, no network, no real checkpoint download. Tests that need the real
vendored `Tracklet`/`refine_tracklets` classes are skipped with a reason when
`vendor/gta-link` is absent (fresh clone before Task 1 has run), so the suite stays
green either way.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import numpy as np
import polars as pl
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
VENDOR_DIR = REPO_ROOT / "vendor" / "gta-link"

sys.path.insert(0, str(REPO_ROOT / "scripts" / "hackathon"))

import measure_gta as mg  # noqa: E402
import baseline_common as bc  # noqa: E402

_VENDOR_MISSING_REASON = "vendor/gta-link nicht vorhanden (frischer Klon vor Task 1) -- Test uebersprungen"


def _require_vendor():
    if not VENDOR_DIR.exists():
        pytest.skip(_VENDOR_MISSING_REASON)
    return mg.load_vendored_refine_module(VENDOR_DIR)


def test_checkpoint_sha_mismatch_aborts(tmp_path: Path) -> None:
    """`load_embedder` must raise `SystemExit` when the checkpoint file's digest
    differs from the caller-supplied SHA-256 -- a silently swapped checkpoint would
    invalidate the whole GTA measurement. Runs before any vendor/model loading, so
    this test needs neither `vendor/gta-link` nor a real OSNet checkpoint.
    """
    fake_checkpoint = tmp_path / "not_the_real_checkpoint.pth"
    fake_checkpoint.write_bytes(b"not actually a pytorch checkpoint")
    wrong_sha256 = "0" * 64
    actual_sha256 = hashlib.sha256(fake_checkpoint.read_bytes()).hexdigest()
    assert actual_sha256 != wrong_sha256

    with pytest.raises(SystemExit):
        mg.load_embedder(
            vendor_dir=tmp_path / "nonexistent-vendor-dir",
            checkpoint=fake_checkpoint,
            checkpoint_sha256=wrong_sha256,
            device="cpu",
        )


def test_tracks_without_crops_pass_through() -> None:
    """A synthetic clip with two embedded tracks and one track without any crop
    keeps all three original track ids represented in the final output -- a track
    without crops must not silently disappear.
    """
    tracklet_cls, refine_module = _require_vendor()

    rows = []
    for track_id, base_x in ((1, 0.0), (2, 500.0)):
        for frame_index in range(3):
            rows.append(
                {
                    "session_id": "synthetic",
                    "clip_number": 1,
                    "frame_index": frame_index,
                    "track_id": track_id,
                    "bbox_x1": base_x,
                    "bbox_y1": 0.0,
                    "bbox_x2": base_x + 10.0,
                    "bbox_y2": 10.0,
                    "confidence": 0.9,
                    "class_name": "player",
                    "detector_run_id": "synthetic-run",
                }
            )
    # Track 3: no crops at all (e.g. a referee never sampled into the crop tree).
    for frame_index in range(3):
        rows.append(
            {
                "session_id": "synthetic",
                "clip_number": 1,
                "frame_index": frame_index,
                "track_id": 3,
                "bbox_x1": 900.0,
                "bbox_y1": 900.0,
                "bbox_x2": 910.0,
                "bbox_y2": 910.0,
                "confidence": 0.5,
                "class_name": "referee",
                "detector_run_id": "synthetic-run",
            }
        )
    clip_df = pl.DataFrame(rows)

    rng = np.random.default_rng(0)
    embeddings: dict[tuple[int, int], dict[int, np.ndarray]] = {}
    for track_id in (1, 2):
        embeddings[(1, track_id)] = {
            frame_index: rng.normal(size=8).astype(np.float32) for frame_index in range(3)
        }
    # Deliberately no entry for (1, 3) -- track 3 has zero embedded crops.

    tracklets, metadata_by_key, all_rows_by_track, passthrough_ids = mg.build_tracklets(
        clip_df, embeddings, clip_number=1, tracklet_cls=tracklet_cls
    )

    assert set(tracklets.keys()) == {1, 2}
    assert passthrough_ids == {3}

    refined, n_split_ops, n_merge_ops = mg.refine(tracklets, mg.GTA_PARAMS, refine_module)
    # Tracks 1 and 2 fully overlap in time (both cover frames 0-2) -- get_distance's
    # doesOverlap guard forces them to stay separate regardless of feature content,
    # so this scenario is deterministic without depending on embedding values.
    assert n_merge_ops == 0

    output_rows = mg.apply_refinement(
        clip_number=1,
        refined=refined,
        metadata_by_key=metadata_by_key,
        all_rows_by_track=all_rows_by_track,
        passthrough_track_ids=passthrough_ids,
        fps=30.0,
        tracked_at="2026-09-01T00:00:00Z",
    )

    assert len(output_rows) == len(rows)
    original_track_signature = {(r["frame_index"], r["class_name"]) for r in rows}
    output_signature = {(r["frame_index"], r["class_name"]) for r in output_rows}
    assert original_track_signature == output_signature
    # Three distinct original identities in -> three distinct final track ids out
    # (no merge possible here, no split triggered -- min_len=100 >> 3 frames).
    assert len({r["track_id"] for r in output_rows}) == 3


def test_no_rows_lost() -> None:
    """`apply_refinement`'s output row count equals the input row count for both a
    synthetic split-like scenario (one original track's frames end up spread across
    two refined tracklets, as `split_tracklets` would produce) and a synthetic
    merge-like scenario (two refined tracklets' frames both trace back correctly).
    """
    tracklet_cls, _refine_module = _require_vendor()

    # Split-like: track 10's 4 frames are represented by two refined tracklets
    # (as if DBSCAN had detected two clusters) -- apply_refinement must still emit
    # exactly 4 rows, all traceable to metadata built for track 10.
    metadata_by_key = {}
    all_rows_by_track = {10: []}
    for frame_index, bbox in enumerate([(0.0, 0.0, 5.0, 5.0), (1.0, 1.0, 5.0, 5.0), (50.0, 50.0, 5.0, 5.0), (51.0, 51.0, 5.0, 5.0)]):
        l, t, w, h = bbox
        row = {
            "session_id": "synthetic",
            "clip_number": 7,
            "frame_index": frame_index,
            "track_id": 10,
            "bbox_x1": l,
            "bbox_y1": t,
            "bbox_x2": l + w,
            "bbox_y2": t + h,
            "confidence": 0.8,
            "class_name": "player",
            "detector_run_id": "synthetic-run",
        }
        all_rows_by_track[10].append(row)
        metadata_by_key[(frame_index, l, t, w, h)] = {
            "session_id": "synthetic",
            "class_name": "player",
            "confidence": 0.8,
            "detector_run_id": "synthetic-run",
            "original_track_id": 10,
        }

    subtracklet_a = tracklet_cls(101, [0, 1], [0.8, 0.8], [[0.0, 0.0, 5.0, 5.0], [1.0, 1.0, 5.0, 5.0]], feats=[np.zeros(8), np.zeros(8)])
    subtracklet_b = tracklet_cls(102, [2, 3], [0.8, 0.8], [[50.0, 50.0, 5.0, 5.0], [51.0, 51.0, 5.0, 5.0]], feats=[np.zeros(8), np.zeros(8)])
    refined_split_like = {101: subtracklet_a, 102: subtracklet_b}

    output_rows = mg.apply_refinement(
        clip_number=7,
        refined=refined_split_like,
        metadata_by_key=metadata_by_key,
        all_rows_by_track=all_rows_by_track,
        passthrough_track_ids=set(),
        fps=30.0,
        tracked_at="2026-09-01T00:00:00Z",
    )
    assert len(output_rows) == len(all_rows_by_track[10])
    assert len({r["track_id"] for r in output_rows}) == 2  # two split outputs, both present

    # Merge-like: two ORIGINAL tracks (20 and 21, disjoint in time) end up combined
    # into a single refined tracklet (as merge_tracklets would produce) -- all 4
    # rows across both original tracks must still appear exactly once.
    metadata_by_key_2 = {}
    all_rows_by_track_2: dict[int, list[dict]] = {20: [], 21: []}
    frames_bboxes = [(20, 0, (0.0, 0.0, 5.0, 5.0)), (20, 1, (1.0, 1.0, 5.0, 5.0)), (21, 10, (2.0, 2.0, 5.0, 5.0)), (21, 11, (3.0, 3.0, 5.0, 5.0))]
    for orig_tid, frame_index, bbox in frames_bboxes:
        l, t, w, h = bbox
        row = {
            "session_id": "synthetic",
            "clip_number": 8,
            "frame_index": frame_index,
            "track_id": orig_tid,
            "bbox_x1": l,
            "bbox_y1": t,
            "bbox_x2": l + w,
            "bbox_y2": t + h,
            "confidence": 0.7,
            "class_name": "player",
            "detector_run_id": "synthetic-run",
        }
        all_rows_by_track_2[orig_tid].append(row)
        metadata_by_key_2[(frame_index, l, t, w, h)] = {
            "session_id": "synthetic",
            "class_name": "player",
            "confidence": 0.7,
            "detector_run_id": "synthetic-run",
            "original_track_id": orig_tid,
        }

    merged_tracklet = tracklet_cls(
        20,
        [0, 1, 10, 11],
        [0.7, 0.7, 0.7, 0.7],
        [[0.0, 0.0, 5.0, 5.0], [1.0, 1.0, 5.0, 5.0], [2.0, 2.0, 5.0, 5.0], [3.0, 3.0, 5.0, 5.0]],
        feats=[np.zeros(8)] * 4,
    )
    refined_merge_like = {20: merged_tracklet}

    output_rows_2 = mg.apply_refinement(
        clip_number=8,
        refined=refined_merge_like,
        metadata_by_key=metadata_by_key_2,
        all_rows_by_track=all_rows_by_track_2,
        passthrough_track_ids=set(),
        fps=30.0,
        tracked_at="2026-09-01T00:00:00Z",
    )
    assert len(output_rows_2) == 4
    assert len({r["track_id"] for r in output_rows_2}) == 1  # merged into one final id


def test_output_schema(tmp_path: Path) -> None:
    """The Parquet written by `baseline_common.write_tracks` (the same writer every
    method uses) carries `OUTPUT_TRACK_COLUMNS` for GTA's own synthetic rows too.
    """
    rows = [
        {
            "session_id": "synthetic",
            "clip_number": 1,
            "frame_index": 0,
            "track_id": 1,
            "bbox_x1": 0.0,
            "bbox_y1": 0.0,
            "bbox_x2": 10.0,
            "bbox_y2": 10.0,
            "timestamp_s": 0.0,
            "class_name": "player",
            "confidence": 0.9,
            "detector_run_id": "synthetic-run",
            "tracked_at": "2026-09-01T00:00:00Z",
        }
    ]
    out_path = tmp_path / "gta_tracks.parquet"
    df = bc.write_tracks(rows, out_path)
    assert out_path.exists()
    assert tuple(df.columns) == tuple(bc.OUTPUT_TRACK_COLUMNS)

    reread = pl.read_parquet(out_path)
    assert tuple(reread.columns) == tuple(bc.OUTPUT_TRACK_COLUMNS)


def test_refuses_reimplementation() -> None:
    """`measure_gta.py` never calls `sklearn`'s DBSCAN directly -- the clustering
    that detects identity switches comes ONLY from the vendored `gta-link` source
    (`refine_tracklets.split_tracklets`), never a local reimplementation.
    """
    source = (REPO_ROOT / "scripts" / "hackathon" / "measure_gta.py").read_text(encoding="utf-8")
    non_comment_lines = [line for line in source.splitlines() if not line.strip().startswith("#")]
    non_comment_source = "\n".join(non_comment_lines)
    assert non_comment_source.count("DBSCAN") == 0
    assert "vendor" in source


def test_forbidden_artifacts_absent() -> None:
    """`measure_gta.py` never mentions the untraceable sports-finetuned checkpoint
    or an install of the third-party (maintainer-mismatched) PyPI `torchreid`
    package.
    """
    source = (REPO_ROOT / "scripts" / "hackathon" / "measure_gta.py").read_text(encoding="utf-8")
    assert "sports_model" not in source
    assert "pip install torchreid" not in source
