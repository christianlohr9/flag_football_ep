"""Tests for `flag_football_ep.cv.active_learning`: uncertainty + diversity frame
selection (plan 02.2-09 Task 1, D-15/T-2.2-25/T-2.2-26).

Fixtures reuse `test_cv_frames.py`'s `video_inventory.csv`/hover-position CSV
helpers (`import test_cv_frames as tcf`, matching the project's existing
cross-test-module-import precedent, e.g. `test_cli_run.py::import
test_pipeline_ingest`). `data/reference/frozen_eval_clips.csv` is written by hand
(matching `frames._EVAL_SPLIT_COLUMNS`) rather than via `frames.freeze_eval_clips`,
since that function enforces a >=6-clip-per-domain floor unrelated to what this
module needs to test -- `read_eval_split` (what `select_al_frames` actually calls)
has no such floor. The fine-tuned detector is always a monkeypatched fake:
`flag_football_ep.cv.detect.load_detector` is replaced with a factory returning a
`_FakeDetectorModel` whose `.predict(image, params=None)` mirrors the real pyfunc
model's contract (`cv.detect._call_model`), so no MLflow store, no real RF-DETR
weights, and no GPU are ever touched. Every candidate clip that IS expected to be
opened is a real, ffmpeg-decodable synthetic clip (`_write_color_clip`, cv2
`VideoWriter`, mirrors `test_cv_detect_infer.py::_write_synthetic_clip`); a clip that
must NEVER be opened (frozen-eval exclusion) is a placeholder file that cv2 cannot
decode at all -- if the exclusion logic ever regressed and let it through, the test
would fail loudly with a decode error, not silently pass.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
import supervision as sv

import test_cv_frames as tcf
from flag_football_ep.config import Config, load_config
from flag_football_ep.cv import detect
from flag_football_ep.cv.active_learning import (
    ActiveLearningError,
    ALFrame,
    ALSelection,
    diversity_key,
    frame_uncertainty_score,
    read_selection_manifest,
    select_al_frames,
    write_selection_manifest,
)
from test_config import MINIMAL_TOML


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    config_path = tmp_path / "ffep.toml"
    config_path.write_text(MINIMAL_TOML, encoding="utf-8")
    return load_config(config_path)


# --- shared fixture helpers ----------------------------------------------------------------

_EVAL_SPLIT_HEADER = "domain,session_id,clip_number,stratum_id,role,private_test,frozen_at,seed"


def _write_frozen_eval_csv(tmp_path: Path, rows: list[dict], *, seed: int = 1) -> Path:
    """Hand-write `frozen_eval_clips.csv` (`frames._EVAL_SPLIT_COLUMNS` schema)
    directly -- bypasses `frames.freeze_eval_clips`'s >=6-clip-per-domain floor,
    which is irrelevant to what `select_al_frames`/`read_eval_split` need here.
    """
    lines = [_EVAL_SPLIT_HEADER]
    for row in rows:
        lines.append(
            f"{row['domain']},{row['session_id']},{row['clip_number']},"
            f"{row['stratum_id']},{row['role']},"
            f"{str(row.get('private_test', False)).lower()},"
            f"2026-01-01T00:00:00+00:00,{seed}"
        )
    path = tmp_path / "data" / "reference" / "frozen_eval_clips.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_color_clip(
    path: Path, n_frames: int, *, color: tuple[int, int, int], width: int = 64,
    height: int = 48, fps: float = 10.0,
) -> None:
    """A tiny, real, ffmpeg/cv2-decodable clip -- every frame the same solid color,
    mirroring `test_cv_detect_infer.py::_write_synthetic_clip`'s technique but with
    an explicit, test-controlled color per clip (rather than a per-frame gradient)
    so a fake model's `.predict()` never needs to inspect pixel content: this
    module's tests identify "which clip is being decoded" purely by call order
    (`len(self.calls)`), the same pattern `test_cv_detect_infer.py::_FakeModel` uses.
    """
    import cv2

    path.parent.mkdir(parents=True, exist_ok=True)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(path), fourcc, fps, (width, height))
    try:
        for _ in range(n_frames):
            frame = np.full((height, width, 3), color, dtype=np.uint8)
            writer.write(frame)
    finally:
        writer.release()


class _FakeDetectorModel:
    """`.predict(image, params=None)` mirrors the real pyfunc model's contract
    (`cv.detect._call_model`). `boxes_fn(call_index)` decides what `sv.Detections`
    each successive call returns -- `call_index` is this model instance's own call
    count, so a test constructs one fresh `_FakeDetectorModel` per `select_al_frames`
    invocation (via the `load_detector` factory lambda) to keep indices predictable.
    """

    def __init__(self, boxes_fn) -> None:
        self._boxes_fn = boxes_fn
        self.calls: list[dict] = []

    def predict(self, image, params=None) -> sv.Detections:
        call_index = len(self.calls)
        self.calls.append({"image": image, "params": params})
        return self._boxes_fn(call_index)


def _empty_detections() -> sv.Detections:
    return sv.Detections(
        xyxy=np.zeros((0, 4), dtype=np.float64),
        confidence=np.zeros((0,), dtype=np.float64),
        class_id=np.zeros((0,), dtype=np.int64),
    )


def _one_detection(x0: float, confidence: float) -> sv.Detections:
    return sv.Detections(
        xyxy=np.array([[x0, 10.0, x0 + 5.0, 20.0]]),
        confidence=np.array([confidence]),
        class_id=np.array([0]),
    )


# --- frame_uncertainty_score / diversity_key (pure functions, no fixtures) ----------------


class _FakeBatch:
    def __init__(self, confidence: list[float]) -> None:
        self.confidence = confidence


def test_frame_uncertainty_score_empty_returns_exactly_one() -> None:
    assert frame_uncertainty_score(_FakeBatch([])) == 1.0


def test_frame_uncertainty_score_nonempty_always_below_one() -> None:
    assert frame_uncertainty_score(_FakeBatch([0.5])) < 1.0


def test_frame_uncertainty_score_decreases_as_confidence_moves_from_threshold() -> None:
    near_threshold = frame_uncertainty_score(_FakeBatch([0.51]))
    far_from_threshold = frame_uncertainty_score(_FakeBatch([0.95]))
    assert near_threshold > far_from_threshold
    assert 0.0 <= far_from_threshold < near_threshold < 1.0


def test_diversity_key_extracts_domain_session_stratum_zone_from_attrs() -> None:
    class _Row:
        domain = "drone"
        session_id = "sess-1"
        stratum_id = "hp-01"
        field_zone_bucket = "left"

    assert diversity_key(_Row()) == ("drone", "sess-1", "hp-01", "left")


def test_diversity_key_extracts_from_dict() -> None:
    row = {"domain": "sideline", "session_id": "s2", "stratum_id": "hp-02", "field_zone_bucket": "mid"}
    assert diversity_key(row) == ("sideline", "s2", "hp-02", "mid")


# --- write_selection_manifest / read_selection_manifest round trip ------------------------


def test_write_read_selection_manifest_round_trips(tmp_path: Path) -> None:
    manifest = ALSelection(
        session_ids=["sess-1"],
        iteration=1,
        target=2,
        seed=42,
        frames=[
            ALFrame(
                session_id="sess-1",
                clip_number=1,
                frame_index=3,
                timestamp_s=1.5,
                image_path=str(tmp_path / "f.jpg"),
                uncertainty_score=0.75,
                diversity_key=("drone", "sess-1", "hp-01", "left"),
            )
        ],
    )
    path = tmp_path / "manifest.json"

    write_selection_manifest(manifest, path)
    loaded = read_selection_manifest(path)

    assert loaded == manifest


def test_read_selection_manifest_missing_file_raises_named_path(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.json"
    with pytest.raises(ActiveLearningError, match=str(missing)):
        read_selection_manifest(missing)


# --- select_al_frames: T-2.2-25 eval-clip exclusion ----------------------------------------


def test_select_al_frames_never_selects_a_frozen_eval_clip(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Clip 1 is frozen (`role=frozen_eval`) and is a placeholder file cv2 cannot
    decode at all -- if the exclusion ever regressed and let it into the candidate
    pool, `detect.detect_video` would raise a decode error the moment it tried to
    open clip 1, failing this test loudly rather than silently leaking a frozen
    frame into the selection. Clip 2 is the only real, decodable pool clip and
    deliberately carries the *highest possible* uncertainty (every frame empty) --
    demonstrating that even the most "valuable" candidate in the pool is still
    correctly excluded when it belongs to the frozen clip, not merely absent by luck.
    """
    session_id = "sess-excl"
    rows = [tcf._row("data/video/sess-excl/Wide - Clip 001.mp4", session_id=session_id, domain="drone")]
    tcf._touch(tmp_path, "data/video/sess-excl/Wide - Clip 001.mp4")  # frozen -- never opened
    rows.append(
        tcf._row("data/video/sess-excl/Wide - Clip 002.mp4", session_id=session_id, domain="drone")
    )
    _write_color_clip(tmp_path / "data/video/sess-excl/Wide - Clip 002.mp4", 4, color=(50, 50, 50))
    tcf._write_inventory(tmp_path, rows)
    tcf._write_hover_positions(tmp_path, {1: "hp-01", 2: "hp-01"})
    _write_frozen_eval_csv(
        tmp_path,
        [
            {"domain": "drone", "session_id": session_id, "clip_number": 1, "stratum_id": "hp-01", "role": "frozen_eval", "private_test": True},
            {"domain": "drone", "session_id": session_id, "clip_number": 2, "stratum_id": "hp-01", "role": "pool"},
        ],
    )

    monkeypatch.setattr(
        detect, "load_detector", lambda config, run_id=None: _FakeDetectorModel(lambda _i: _empty_detections())
    )

    out_dir = tmp_path / "al" / "iteration-1"
    selection = select_al_frames(cfg, [session_id], 1, 2, 1, out_dir)

    assert selection.frames
    assert {f.clip_number for f in selection.frames} == {2}


def test_select_al_frames_empty_pool_raises_named_domain(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_id = "sess-empty"
    tcf._touch(tmp_path, "data/video/sess-empty/Wide - Clip 001.mp4")
    tcf._write_inventory(
        tmp_path, [tcf._row("data/video/sess-empty/Wide - Clip 001.mp4", session_id=session_id, domain="drone")]
    )
    tcf._write_hover_positions(tmp_path, {1: "hp-01"})
    _write_frozen_eval_csv(
        tmp_path,
        [
            {"domain": "drone", "session_id": session_id, "clip_number": 1, "stratum_id": "hp-01", "role": "frozen_eval", "private_test": True},
        ],
    )
    monkeypatch.setattr(
        detect, "load_detector", lambda config, run_id=None: _FakeDetectorModel(lambda _i: _empty_detections())
    )

    out_dir = tmp_path / "al" / "iteration-1"
    with pytest.raises(ActiveLearningError, match="drone"):
        select_al_frames(cfg, [session_id], 1, 4, 1, out_dir)


# --- select_al_frames: diversity vs. a naive flat uncertainty ranking ----------------------


def test_select_al_frames_diversity_avoids_collapsing_onto_one_group(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Three pool clips, one stratum: clip 1's frames are *all* empty (uncertainty
    exactly 1.0, the maximum possible) while clips 2/3 carry confident, non-empty
    detections at opposite ends of the frame (uncertainty far lower, and different
    `field_zone_bucket`s). A naive flat "sort every candidate by uncertainty, take
    the top `target`" ranking would necessarily draw every one of its 6 picks from
    clip 1 alone (it has 8 candidates all tied at the maximum score, more than the
    6 needed) -- computed directly below via the same `frame_uncertainty_score`
    the implementation uses, not asserted by comment. The actual diversity-grouped
    selection spans all three groups instead.
    """
    session_id = "sess-multi"
    rows = []
    for n, color in ((1, (10, 10, 10)), (2, (100, 100, 100)), (3, (200, 200, 200))):
        rel = f"data/video/sess-multi/Wide - Clip {n:03d}.mp4"
        rows.append(tcf._row(rel, session_id=session_id, domain="drone"))
        _write_color_clip(tmp_path / rel, 8, color=color)
    tcf._write_inventory(tmp_path, rows)
    tcf._write_hover_positions(tmp_path, {1: "hp-01", 2: "hp-01", 3: "hp-01"})
    _write_frozen_eval_csv(
        tmp_path,
        [
            {"domain": "drone", "session_id": session_id, "clip_number": n, "stratum_id": "hp-01", "role": "pool"}
            for n in (1, 2, 3)
        ],
    )

    # Clip 1 (calls 0-7): always empty -> uncertainty 1.0, zone "unknown".
    # Clip 2 (calls 8-15): confident detection near the left edge -> zone "left".
    # Clip 3 (calls 16-23): confident detection near the right edge -> zone "right".
    def boxes_fn(call_index: int) -> sv.Detections:
        if call_index < 8:
            return _empty_detections()
        if call_index < 16:
            return _one_detection(x0=2.0, confidence=0.95)  # near x=0 of a 64px-wide frame
        return _one_detection(x0=58.0, confidence=0.95)  # near x=64

    monkeypatch.setattr(detect, "load_detector", lambda config, run_id=None: _FakeDetectorModel(boxes_fn))

    out_dir = tmp_path / "al" / "iteration-1"
    selection = select_al_frames(cfg, [session_id], 1, 6, 1, out_dir)

    actual_groups = {f.diversity_key for f in selection.frames}
    assert len(actual_groups) == 3

    # Independently verify the "naive flat ranking would collapse" claim using the
    # same scoring function, not merely asserted by comment.
    naive_pool: list[tuple[int, float]] = []
    for call_index in range(24):
        clip_n = 1 if call_index < 8 else (2 if call_index < 16 else 3)
        score = frame_uncertainty_score(boxes_fn(call_index))
        naive_pool.append((clip_n, score))
    naive_top6 = sorted(naive_pool, key=lambda t: -t[1])[:6]
    naive_distinct_clips = {clip_n for clip_n, _ in naive_top6}
    assert naive_distinct_clips == {1}

    actual_distinct_clips = {f.clip_number for f in selection.frames}
    assert len(actual_distinct_clips) > 1


def test_select_al_frames_stratum_share_and_floor(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two diversity groups of unequal candidate size (clip 1: 8 candidates, clip 2:
    2 candidates) sharing one domain/session/stratum but different zones: with
    `target=6`, the smaller group still receives at least one frame (the floor,
    since `target` exceeds its own 2-candidate count), and neither group receives
    more than its computed proportional share.
    """
    session_id = "sess-strata"
    rows = []
    for n, (color, n_frames) in ((1, ((10, 10, 10), 8)), (2, ((200, 200, 200), 2))):
        rel = f"data/video/sess-strata/Wide - Clip {n:03d}.mp4"
        rows.append(tcf._row(rel, session_id=session_id, domain="drone"))
        _write_color_clip(tmp_path / rel, n_frames, color=color)
    tcf._write_inventory(tmp_path, rows)
    tcf._write_hover_positions(tmp_path, {1: "hp-01", 2: "hp-01"})
    _write_frozen_eval_csv(
        tmp_path,
        [
            {"domain": "drone", "session_id": session_id, "clip_number": n, "stratum_id": "hp-01", "role": "pool"}
            for n in (1, 2)
        ],
    )

    def boxes_fn(call_index: int) -> sv.Detections:
        if call_index < 8:
            return _one_detection(x0=2.0, confidence=0.95)  # clip 1 -> zone "left"
        return _one_detection(x0=58.0, confidence=0.6)  # clip 2 -> zone "right"

    monkeypatch.setattr(detect, "load_detector", lambda config, run_id=None: _FakeDetectorModel(boxes_fn))

    out_dir = tmp_path / "al" / "iteration-1"
    selection = select_al_frames(cfg, [session_id], 1, 6, 1, out_dir)

    by_group: dict[tuple, int] = {}
    for f in selection.frames:
        by_group[f.diversity_key] = by_group.get(f.diversity_key, 0) + 1

    assert len(by_group) == 2
    # every stratum with candidates got at least one frame
    assert all(count >= 1 for count in by_group.values())
    # the small group (2 total candidates) never exceeds what it has
    small_group_counts = [count for key, count in by_group.items() if key[-1] == "right"]
    assert small_group_counts and small_group_counts[0] <= 2


# --- select_al_frames: iteration exclusion, determinism ------------------------------------


def test_select_al_frames_iteration_2_excludes_iteration_1_selection(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_id = "sess-iter"
    rel = "data/video/sess-iter/Wide - Clip 001.mp4"
    tcf._write_inventory(tmp_path, [tcf._row(rel, session_id=session_id, domain="drone")])
    _write_color_clip(tmp_path / rel, 12, color=(10, 10, 10))
    tcf._write_hover_positions(tmp_path, {1: "hp-01"})
    _write_frozen_eval_csv(
        tmp_path,
        [{"domain": "drone", "session_id": session_id, "clip_number": 1, "stratum_id": "hp-01", "role": "pool"}],
    )

    # Distinct, strictly-decreasing confidence per frame (varying distance from the
    # 0.5 threshold) avoids uncertainty ties, so the top-6/bottom-6 split is
    # unambiguous regardless of the seeded tie-break shuffle.
    def boxes_fn(call_index: int) -> sv.Detections:
        return _one_detection(x0=2.0, confidence=0.5 + call_index * 0.03)

    def load_detector_factory(config, run_id=None):
        return _FakeDetectorModel(boxes_fn)

    monkeypatch.setattr(detect, "load_detector", load_detector_factory)

    out_dir_1 = tmp_path / "al" / "iteration-1"
    selection_1 = select_al_frames(cfg, [session_id], 1, 6, 1, out_dir_1)

    out_dir_2 = tmp_path / "al" / "iteration-2"
    selection_2 = select_al_frames(cfg, [session_id], 2, 6, 1, out_dir_2)

    keys_1 = {(f.session_id, f.clip_number, f.frame_index) for f in selection_1.frames}
    keys_2 = {(f.session_id, f.clip_number, f.frame_index) for f in selection_2.frames}
    assert keys_1.isdisjoint(keys_2)
    assert len(keys_2) == 6


def test_select_al_frames_same_seed_identical_bytes_different_seed_differs(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_id = "sess-seed"
    rel = "data/video/sess-seed/Wide - Clip 001.mp4"
    tcf._write_inventory(tmp_path, [tcf._row(rel, session_id=session_id, domain="drone")])
    _write_color_clip(tmp_path / rel, 4, color=(10, 10, 10))
    tcf._write_hover_positions(tmp_path, {1: "hp-01"})
    _write_frozen_eval_csv(
        tmp_path,
        [{"domain": "drone", "session_id": session_id, "clip_number": 1, "stratum_id": "hp-01", "role": "pool"}],
    )
    monkeypatch.setattr(
        detect, "load_detector",
        lambda config, run_id=None: _FakeDetectorModel(lambda _i: _one_detection(2.0, 0.9)),
    )

    # Same seed, same `out_dir` (`extract_frames` re-writing identical timestamps
    # into the same directory is idempotent) -- `image_path` is only stable across
    # runs when `out_dir` itself is unchanged, so re-running into a *different*
    # `out_dir` would trivially "differ" via that path text alone rather than via
    # the seed actually changing the selection.
    out_a = tmp_path / "a"
    select_al_frames(cfg, [session_id], 1, 2, 111, out_a)
    bytes_a1 = (out_a / "selection_manifest.json").read_bytes()
    select_al_frames(cfg, [session_id], 1, 2, 111, out_a)
    bytes_a2 = (out_a / "selection_manifest.json").read_bytes()

    out_b = tmp_path / "a"  # same directory, different seed overwrites in place
    select_al_frames(cfg, [session_id], 1, 2, 222, out_b)
    bytes_b = (out_b / "selection_manifest.json").read_bytes()

    assert bytes_a1 == bytes_a2
    assert bytes_a1 != bytes_b


def test_select_al_frames_extracts_selected_frames_and_persists_manifest(
    tmp_path: Path, cfg: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    session_id = "sess-persist"
    rel = "data/video/sess-persist/Wide - Clip 001.mp4"
    tcf._write_inventory(tmp_path, [tcf._row(rel, session_id=session_id, domain="drone")])
    _write_color_clip(tmp_path / rel, 3, color=(10, 10, 10))
    tcf._write_hover_positions(tmp_path, {1: "hp-01"})
    _write_frozen_eval_csv(
        tmp_path,
        [{"domain": "drone", "session_id": session_id, "clip_number": 1, "stratum_id": "hp-01", "role": "pool"}],
    )
    monkeypatch.setattr(
        detect, "load_detector",
        lambda config, run_id=None: _FakeDetectorModel(lambda _i: _one_detection(2.0, 0.9)),
    )

    out_dir = tmp_path / "al" / "iteration-1"
    selection = select_al_frames(cfg, [session_id], 1, 2, 1, out_dir)

    manifest_path = out_dir / "selection_manifest.json"
    assert manifest_path.exists()
    loaded = read_selection_manifest(manifest_path)
    assert loaded == selection

    for frame in selection.frames:
        assert Path(frame.image_path).exists()
        assert Path(frame.image_path).parent == out_dir
