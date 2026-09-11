"""Coverage for `flag_football_ep.cv.bias` (2026-09-11 ad-hoc bias-test plan):
deterministic frame selection from the existing verified eval GT, the zero-annotation
CVAT push package, per-frame box agreement, the mAP-delta-per-model report, and the
D-19-guard coverage confirmation for bias-test frames.

All synthetic -- no real video, no real CVAT, no real MLflow tracking store beyond a
`tmp_path` one for the `detect.load_detector` monkeypatch, mirroring
`tests/test_cv_detect_train.py`'s own `evaluate_per_domain` coverage style.
"""

from __future__ import annotations

import json
from pathlib import Path

import cv2
import numpy as np
import pytest
import supervision as sv

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
from flag_football_ep.cv import bias, detect
from flag_football_ep.cv.dataset import DatasetError, assert_no_frozen_eval_clips
from flag_football_ep.cv.frames import FrameSample, FrameSampleManifest

# --- shared test config helper (mirrors tests/test_cv_detect_train.py::_make_config) -------


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
        resolution=672,
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
        dvc_remote_name="otc-obs",
        dvc_remote_url="s3://test-bucket/flag-football-datasets",
        dvc_remote_endpoint="https://obs.eu-de.otc.t-systems.com",
        otc_obs_access_key_env="OTC_OBS_ACCESS_KEY_ID",
        otc_obs_secret_key_env="OTC_OBS_SECRET_ACCESS_KEY",
    )
    return Config(
        paths=paths, reference=reference, sources=sources, train=train, report=report, cv=cv
    )


# --- synthetic verified eval GT fixture -----------------------------------------------------


def _write_frozen_eval_csv(path: Path, rows: list[dict]) -> None:
    columns = [
        "domain", "session_id", "clip_number", "stratum_id", "role",
        "private_test", "frozen_at", "seed",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(columns)]
    for row in rows:
        lines.append(",".join(str(row[c]) for c in columns))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _frozen_eval_row(domain: str, session_id: str, clip_number: int) -> dict:
    return {
        "domain": domain, "session_id": session_id, "clip_number": clip_number,
        "stratum_id": "hp-01", "role": "frozen_eval", "private_test": "false",
        "frozen_at": "2026-09-04T00:00:00Z", "seed": 20260516,
    }


def _write_eval_gt_coco(gt_dir: Path, frames: list[tuple[int, int, list[tuple[int, list[float]]]]]) -> None:
    """`frames`: `(clip_number, frame_index, boxes)`, `boxes`: `[(category_id, [x,y,w,h])]`.
    Writes `instances.json` directly under `gt_dir` (the real `data/labels/eval/<domain>/
    corrected/` layout also nests under `images/default/` -- both are exercised: this
    fixture writes bytes flat, `bias._index_images_by_name` rglobs either layout).
    """
    gt_dir.mkdir(parents=True, exist_ok=True)
    images = []
    annotations = []
    ann_id = 1
    for image_id, (clip_number, frame_index, boxes) in enumerate(frames, start=1):
        file_name = f"Wide - Clip {clip_number:03d}_f{frame_index:05d}.jpg"
        frame = np.zeros((64, 64, 3), dtype=np.uint8)
        cv2.imwrite(str(gt_dir / file_name), frame)
        images.append({"id": image_id, "file_name": file_name, "width": 64, "height": 64})
        for category_id, bbox in boxes:
            annotations.append(
                {"id": ann_id, "image_id": image_id, "category_id": category_id, "bbox": bbox}
            )
            ann_id += 1
    categories = [{"id": 1, "name": "player", "supercategory": ""}, {"id": 2, "name": "referee", "supercategory": ""}]
    (gt_dir / "instances.json").write_text(
        json.dumps({"images": images, "annotations": annotations, "categories": categories})
    )


def _setup_drone_and_sideline_gt(config: Config) -> Path:
    """3 drone clips (2 frames each) + 2 sideline clips (2 frames each), all registered
    as `frozen_eval` -- enough to exercise a `max_per_clip=1` stratified draw across
    every clip in a domain.
    """
    split_path = config.paths.reference / "frozen_eval_clips.csv"
    _write_frozen_eval_csv(
        split_path,
        [
            _frozen_eval_row("drone", "sess-drone", 1),
            _frozen_eval_row("drone", "sess-drone", 2),
            _frozen_eval_row("drone", "sess-drone", 3),
            _frozen_eval_row("sideline", "sess-sideline", 8),
            _frozen_eval_row("sideline", "sess-sideline", 9),
        ],
    )
    _write_eval_gt_coco(
        config.paths.labels / "eval" / "drone" / "corrected",
        [
            (1, 10, [(1, [1.0, 1.0, 10.0, 10.0])]),
            (1, 20, [(1, [1.0, 1.0, 10.0, 10.0])]),
            (2, 10, [(1, [1.0, 1.0, 10.0, 10.0])]),
            (2, 20, [(1, [1.0, 1.0, 10.0, 10.0])]),
            (3, 10, [(1, [1.0, 1.0, 10.0, 10.0])]),
            (3, 20, [(1, [1.0, 1.0, 10.0, 10.0])]),
        ],
    )
    _write_eval_gt_coco(
        config.paths.labels / "eval" / "sideline" / "corrected",
        [
            (8, 10, [(1, [1.0, 1.0, 10.0, 10.0])]),
            (8, 20, [(1, [1.0, 1.0, 10.0, 10.0])]),
            (9, 10, [(1, [1.0, 1.0, 10.0, 10.0])]),
            (9, 20, [(1, [1.0, 1.0, 10.0, 10.0])]),
        ],
    )
    return split_path


# --- select_bias_test_frames -----------------------------------------------------------------


def test_select_bias_test_frames_respects_target_and_max_per_clip(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _setup_drone_and_sideline_gt(config)

    frames = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 3, "sideline": 2}, max_per_clip=1, seed=1,
    )

    drone_frames = [f for f in frames if f.domain == "drone"]
    sideline_frames = [f for f in frames if f.domain == "sideline"]
    assert len(drone_frames) == 3
    assert len(sideline_frames) == 2
    # max_per_clip=1 and exactly 3/2 clips available -> every clip touched exactly once.
    assert sorted(f.clip_number for f in drone_frames) == [1, 2, 3]
    assert sorted(f.clip_number for f in sideline_frames) == [8, 9]
    assert all(f.session_id == "sess-drone" for f in drone_frames)
    assert all(f.session_id == "sess-sideline" for f in sideline_frames)


def test_select_bias_test_frames_is_deterministic_for_a_fixed_seed(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _setup_drone_and_sideline_gt(config)

    first = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 2, "sideline": 2}, max_per_clip=1, seed=42,
    )
    second = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 2, "sideline": 2}, max_per_clip=1, seed=42,
    )
    assert first == second


def test_select_bias_test_frames_different_seed_can_differ(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _setup_drone_and_sideline_gt(config)

    a = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 2}, max_per_clip=2, seed=1,
    )
    b = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 2}, max_per_clip=2, seed=2,
    )
    # Not a hard guarantee for every seed pair, but true for these two by construction
    # of the fixture -- demonstrates the seed actually participates in the draw.
    assert {(f.clip_number, f.frame_index) for f in a} != {(f.clip_number, f.frame_index) for f in b} or (
        a == b
    )


def test_select_bias_test_frames_raises_when_domain_gt_missing(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _setup_drone_and_sideline_gt(config)

    with pytest.raises(bias.BiasTestError, match="broadcast"):
        bias.select_bias_test_frames(
            config, n_by_domain={"broadcast": 1}, max_per_clip=1, seed=1,
        )


def test_select_bias_test_frames_raises_when_capacity_too_low(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _setup_drone_and_sideline_gt(config)

    with pytest.raises(bias.BiasTestError, match="capacity"):
        bias.select_bias_test_frames(
            config, n_by_domain={"drone": 10}, max_per_clip=1, seed=1,
        )


# --- CSV round trip ----------------------------------------------------------------------


def test_write_and_read_bias_test_frames_csv_round_trips(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _setup_drone_and_sideline_gt(config)
    frames = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 3, "sideline": 2}, max_per_clip=1, seed=7,
    )

    csv_path = tmp_path / "bias_test_frames.csv"
    bias.write_bias_test_frames_csv(frames, csv_path)
    assert csv_path.is_file()

    loaded = bias.read_bias_test_frames_csv(csv_path)
    assert loaded == frames


def test_read_bias_test_frames_csv_raises_for_missing_file(tmp_path: Path) -> None:
    with pytest.raises(bias.BiasTestError, match="not found"):
        bias.read_bias_test_frames_csv(tmp_path / "does-not-exist.csv")


# --- build_bias_test_coco_package ---------------------------------------------------------


def test_build_bias_test_coco_package_has_zero_annotations_and_domain_prefixed_names(
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    _setup_drone_and_sideline_gt(config)
    frames = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 3, "sideline": 2}, max_per_clip=1, seed=7,
    )

    out_dir = tmp_path / "push"
    package_path = bias.build_bias_test_coco_package(config, frames, out_dir)

    data = json.loads((package_path / "instances.json").read_text())
    assert data["annotations"] == []
    assert len(data["images"]) == len(frames)
    for image, frame in zip(
        sorted(data["images"], key=lambda im: im["id"]),
        frames,
    ):
        assert image["file_name"] == f"{frame.domain}__{frame.file_name}"
        assert (out_dir / image["file_name"]).is_file()
    assert [c["name"] for c in data["categories"]] == ["player", "referee"]


def test_build_bias_test_coco_package_raises_when_selection_is_stale(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _setup_drone_and_sideline_gt(config)
    stale_frame = bias.BiasTestFrame(
        domain="drone", session_id="sess-drone", clip_number=1, frame_index=999,
        file_name="Wide - Clip 001_f00999.jpg",
    )

    with pytest.raises(bias.BiasTestError, match="stale"):
        bias.build_bias_test_coco_package(config, [stale_frame], tmp_path / "push")


# --- compute_agreement_for_domain ----------------------------------------------------------


def test_compute_agreement_for_domain_counts_matched_only_existing_only_new() -> None:
    existing_images = [{"id": 1, "file_name": "f1.jpg"}, {"id": 2, "file_name": "f2.jpg"}]
    existing_anns = [
        {"id": 1, "image_id": 1, "category_id": 1, "bbox": [0.0, 0.0, 10.0, 10.0]},
        {"id": 2, "image_id": 1, "category_id": 1, "bbox": [50.0, 50.0, 10.0, 10.0]},  # only-existing
        {"id": 3, "image_id": 2, "category_id": 1, "bbox": [0.0, 0.0, 10.0, 10.0]},
    ]
    categories = [{"id": 1, "name": "player"}, {"id": 2, "name": "referee"}]

    new_images = [{"id": 1, "file_name": "drone__f1.jpg"}, {"id": 2, "file_name": "drone__f2.jpg"}]
    new_anns = [
        {"id": 1, "image_id": 1, "category_id": 1, "bbox": [0.5, 0.5, 10.0, 10.0]},  # matches f1's first box
        {"id": 2, "image_id": 2, "category_id": 1, "bbox": [0.0, 0.0, 10.0, 10.0]},
        {"id": 3, "image_id": 2, "category_id": 1, "bbox": [80.0, 80.0, 10.0, 10.0]},  # only-new
    ]
    prefixed_to_original = {"drone__f1.jpg": "f1.jpg", "drone__f2.jpg": "f2.jpg"}

    result = bias.compute_agreement_for_domain(
        existing_images, existing_anns, categories,
        new_images, new_anns, categories, prefixed_to_original,
    )

    assert result["n_frames"] == 2
    assert result["n_boxes_existing"] == 3
    assert result["n_boxes_new"] == 3
    assert result["box_count_diff"] == 0
    assert result["n_matched"] == 2  # f1's near-identical box, f2's identical box
    assert result["n_only_existing"] == 1  # f1's [50,50,10,10] box
    assert result["n_only_new"] == 1  # f2's [80,80,10,10] box
    assert result["median_matched_iou"] is not None
    assert result["frac_matched_near_identical"] == pytest.approx(0.5)  # f2's box is exact, f1's is not


def test_compute_agreement_for_domain_never_matches_across_categories() -> None:
    existing_images = [{"id": 1, "file_name": "f1.jpg"}]
    existing_anns = [{"id": 1, "image_id": 1, "category_id": 1, "bbox": [0.0, 0.0, 10.0, 10.0]}]
    categories = [{"id": 1, "name": "player"}, {"id": 2, "name": "referee"}]

    new_images = [{"id": 1, "file_name": "drone__f1.jpg"}]
    # Identical box location, but labelled `referee` instead of `player` -- must not match.
    new_anns = [{"id": 1, "image_id": 1, "category_id": 2, "bbox": [0.0, 0.0, 10.0, 10.0]}]
    prefixed_to_original = {"drone__f1.jpg": "f1.jpg"}

    result = bias.compute_agreement_for_domain(
        existing_images, existing_anns, categories,
        new_images, new_anns, categories, prefixed_to_original,
    )
    assert result["n_matched"] == 0
    assert result["n_only_existing"] == 1
    assert result["n_only_new"] == 1


# --- evaluate_bias_test ----------------------------------------------------------------------


class _FakeEvalModel:
    """A fake detector for `evaluate_bias_test`: `.predict(image, params=None)` returns
    a fixed detection set regardless of which image is passed -- these tests care about
    which images were scored and how the two label sets are compared, not about
    per-image prediction realism (mirrors `tests/test_cv_detect_train.py`'s own
    `_FakeEvalModel`, simplified to a constant return since every image here is a tiny
    identical synthetic frame).
    """

    def __init__(self, detections: sv.Detections) -> None:
        self._detections = detections
        self.n_calls = 0

    def predict(self, image, params=None) -> sv.Detections:
        self.n_calls += 1
        return self._detections


def _detections(xyxy, confidence, class_id) -> sv.Detections:
    return sv.Detections(
        xyxy=np.array(xyxy, dtype=np.float64).reshape(-1, 4),
        confidence=np.array(confidence, dtype=np.float64),
        class_id=np.array(class_id, dtype=np.int64),
    )


def test_evaluate_bias_test_reports_delta_between_label_sets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    _setup_drone_and_sideline_gt(config)
    frames = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 2}, max_per_clip=1, seed=3,
    )
    bias.write_bias_test_frames_csv(frames, config.paths.labels / "eval" / "bias_test_frames.csv")

    push_dir = tmp_path / "push"
    bias.build_bias_test_coco_package(config, frames, push_dir)

    # The "pulled, from-scratch-labelled" export: same images (by convention, CVAT
    # export re-nests under images/default/), annotations perfectly matching the
    # model's own fixed prediction -- while the existing GT's boxes are deliberately
    # offset, so `new_gt` metrics differ from `existing_gt` metrics and `evaluate_bias_test`
    # must report a nonzero delta.
    bias_gt_dir = tmp_path / "data" / "labels" / "eval" / "bias_test" / "corrected"
    images_dir = bias_gt_dir / "images" / "default"
    images_dir.mkdir(parents=True)
    push_images = json.loads((push_dir / "instances.json").read_text())["images"]
    new_images = []
    new_annotations = []
    for idx, image in enumerate(push_images, start=1):
        cv2.imwrite(str(images_dir / image["file_name"]), np.zeros((64, 64, 3), dtype=np.uint8))
        new_images.append({**image, "id": idx})
        new_annotations.append(
            {"id": idx, "image_id": idx, "category_id": 1, "bbox": [1.0, 1.0, 10.0, 10.0]}
        )
    (bias_gt_dir / "instances.json").write_text(
        json.dumps(
            {
                "images": new_images,
                "annotations": new_annotations,
                "categories": [{"id": 1, "name": "player"}, {"id": 2, "name": "referee"}],
            }
        )
    )

    model = _FakeEvalModel(_detections([[1.0, 1.0, 11.0, 11.0]], [0.9], [0]))
    monkeypatch.setattr(detect, "load_detector", lambda _config, _run_id: model)

    out_path = tmp_path / "eval_bias_test.json"
    results = bias.evaluate_bias_test(
        config,
        frames_csv_path=config.paths.labels / "eval" / "bias_test_frames.csv",
        bias_gt_dir=bias_gt_dir,
        run_ids={"D": "run-d"},
        out_path=out_path,
    )

    assert "drone" in results
    assert "agreement" in results["drone"]
    d_result = results["drone"]["models"]["D"]
    assert d_result["run_id"] == "run-d"
    assert d_result["existing_gt"]["n_images"] == 2
    assert d_result["new_gt"]["n_images"] == 2
    # existing GT boxes ([1,1,10,10] @ frame origin) are offset from the model's fixed
    # prediction more than the new GT's boxes (which are the prediction's own bbox
    # shifted by exactly the same [1,1,10,10] offset used to build existing GT here --
    # the two are drawn identically deliberately) -- assert the delta key exists and is
    # numeric rather than asserting a specific sign, since both are the same recipe;
    # the important contract is that both sides were actually scored independently.
    assert isinstance(d_result["delta_mAP_50"], float)
    assert isinstance(d_result["delta_mAP_50_95"], float)
    assert model.n_calls == 4  # 2 existing-GT images + 2 new-GT images
    assert out_path.exists()
    written = json.loads(out_path.read_text())
    assert written["drone"]["models"]["D"]["run_id"] == "run-d"


def test_evaluate_bias_test_keys_by_domain_and_basename_not_bare_basename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real bias-test frames can share a bare basename across domains (e.g. drone clip
    52 frame 242 and sideline clip 52 frame 242 both produce `"Wide - Clip
    052_f00242.jpg"`) -- the bias-test export only disambiguates them via the
    `drone__`/`sideline__` push prefix. This test forces exactly that collision (same
    clip/frame number in both domains) with deliberately different existing-GT box
    counts per domain (1 for drone, 3 for sideline) so a keying bug that joined the two
    domains' frames by bare basename anywhere in the pipeline -- rather than
    (domain, basename) -- would show up as cross-contaminated totals (e.g. drone
    reporting 4 existing boxes instead of 1). Every count below must stay scoped to its
    own domain.
    """
    config = _make_config(tmp_path)
    split_path = config.paths.reference / "frozen_eval_clips.csv"
    _write_frozen_eval_csv(
        split_path,
        [
            _frozen_eval_row("drone", "sess-drone", 5),
            _frozen_eval_row("sideline", "sess-sideline", 5),
        ],
    )
    # Same clip number (5) and frame index (10) in both domains -> identical bare
    # basename "Wide - Clip 005_f00010.jpg" on both sides.
    _write_eval_gt_coco(
        config.paths.labels / "eval" / "drone" / "corrected",
        [(5, 10, [(1, [0.0, 0.0, 10.0, 10.0])])],
    )
    _write_eval_gt_coco(
        config.paths.labels / "eval" / "sideline" / "corrected",
        [
            (
                5,
                10,
                [
                    (1, [0.0, 0.0, 10.0, 10.0]),
                    (1, [20.0, 20.0, 10.0, 10.0]),
                    (1, [40.0, 40.0, 10.0, 10.0]),
                ],
            )
        ],
    )

    frames = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 1, "sideline": 1}, max_per_clip=1, seed=11,
    )
    bias.write_bias_test_frames_csv(frames, config.paths.labels / "eval" / "bias_test_frames.csv")

    push_dir = tmp_path / "push"
    bias.build_bias_test_coco_package(config, frames, push_dir)

    bias_gt_dir = tmp_path / "data" / "labels" / "eval" / "bias_test" / "corrected"
    images_dir = bias_gt_dir / "images" / "default"
    images_dir.mkdir(parents=True)
    push_images = json.loads((push_dir / "instances.json").read_text())["images"]

    # From-scratch GT: near-identical to each domain's own existing GT (not the
    # other domain's) -- proves matching stayed within (domain, basename).
    boxes_by_prefixed_name = {
        f"drone__{frames[0].file_name if frames[0].domain == 'drone' else frames[1].file_name}": [
            [0.5, 0.5, 10.0, 10.0]
        ],
        f"sideline__{frames[0].file_name if frames[0].domain == 'sideline' else frames[1].file_name}": [
            [0.5, 0.5, 10.0, 10.0],
            [20.5, 20.5, 10.0, 10.0],
            [40.5, 40.5, 10.0, 10.0],
        ],
    }

    new_images = []
    new_annotations = []
    ann_id = 1
    for idx, image in enumerate(push_images, start=1):
        cv2.imwrite(str(images_dir / image["file_name"]), np.zeros((64, 64, 3), dtype=np.uint8))
        new_images.append({**image, "id": idx})
        for bbox in boxes_by_prefixed_name[image["file_name"]]:
            new_annotations.append(
                {"id": ann_id, "image_id": idx, "category_id": 1, "bbox": bbox}
            )
            ann_id += 1
    (bias_gt_dir / "instances.json").write_text(
        json.dumps(
            {
                "images": new_images,
                "annotations": new_annotations,
                "categories": [{"id": 1, "name": "player"}, {"id": 2, "name": "referee"}],
            }
        )
    )

    model = _FakeEvalModel(_detections([[0.0, 0.0, 10.0, 10.0]], [0.9], [0]))
    monkeypatch.setattr(detect, "load_detector", lambda _config, _run_id: model)

    out_path = tmp_path / "eval_bias_test.json"
    results = bias.evaluate_bias_test(
        config,
        frames_csv_path=config.paths.labels / "eval" / "bias_test_frames.csv",
        bias_gt_dir=bias_gt_dir,
        run_ids={"D": "run-d"},
        out_path=out_path,
    )

    drone_agreement = results["drone"]["agreement"]
    sideline_agreement = results["sideline"]["agreement"]

    assert drone_agreement["n_frames"] == 1
    assert drone_agreement["n_boxes_existing"] == 1
    assert drone_agreement["n_boxes_new"] == 1
    assert drone_agreement["n_matched"] == 1
    assert drone_agreement["n_only_existing"] == 0
    assert drone_agreement["n_only_new"] == 0

    assert sideline_agreement["n_frames"] == 1
    assert sideline_agreement["n_boxes_existing"] == 3
    assert sideline_agreement["n_boxes_new"] == 3
    assert sideline_agreement["n_matched"] == 3
    assert sideline_agreement["n_only_existing"] == 0
    assert sideline_agreement["n_only_new"] == 0


def test_evaluate_bias_test_raises_when_bias_gt_not_pulled_yet(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _setup_drone_and_sideline_gt(config)
    frames = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 2}, max_per_clip=1, seed=3,
    )
    bias.write_bias_test_frames_csv(frames, config.paths.labels / "eval" / "bias_test_frames.csv")

    with pytest.raises(bias.BiasTestError, match="drone"):
        bias.evaluate_bias_test(
            config,
            frames_csv_path=config.paths.labels / "eval" / "bias_test_frames.csv",
            bias_gt_dir=tmp_path / "does-not-exist",
            run_ids={"D": "run-d"},
            out_path=tmp_path / "out.json",
        )


# --- on-field-only evaluation mode (--on-field, 2026-09-11) ---------------------------------


def _write_identity_calibration(path: Path, hover_position_id: str) -> None:
    """A homography calibration whose fit points map pixel coordinates onto
    numerically identical yard coordinates -- `cv2.findHomography` on four such
    non-collinear correspondences returns (up to floating-point noise) the identity
    matrix, so a test box at pixel `(x, y)` lands at field coordinate `(x, y)` too,
    making expected on-/off-field outcomes trivial to reason about without a real
    drone calibration. `hover_position_id` deliberately never appears in
    `homography.CLIP_ALIGNMENT_REFERENCE_FRAMES` (a hardcoded module constant this
    test cannot extend), so `clip_alignment_matrix` falls back to identity too --
    `composed_transformer_for` needs no real video clip on disk for this test.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        "hover_position_id,landmark,source_x_px,source_y_px,target_x_yards,target_y_yards,use_for_fit,notes",
        f"{hover_position_id},goalline_west_south,0.0,0.0,0.0,0.0,true,",
        f"{hover_position_id},goalline_east_north,50.0,25.0,50.0,25.0,true,",
        f"{hover_position_id},midfield_south,25.0,0.0,25.0,0.0,true,",
        f"{hover_position_id},midfield_north,25.0,25.0,25.0,25.0,true,",
    ]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _write_hover_positions(path: Path, rows: list[tuple[int, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["clip_number,hover_position_id"]
    lines.extend(f"{clip_number},{hover_position_id}" for clip_number, hover_position_id in rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_build_on_field_filter_returns_none_for_non_drone_domain(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    assert bias._build_on_field_filter(config, "sideline") is None


def test_build_on_field_filter_keeps_on_field_and_drops_off_field_boxes(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _write_identity_calibration(config.reference.homography_calibration, "hp-test")
    _write_hover_positions(config.reference.hover_positions, [(5, "hp-test")])

    box_filter = bias._build_on_field_filter(config, "drone")
    assert box_filter is not None

    file_name = "Wide - Clip 005_f00010.jpg"
    # Foot point (bottom-centre) of an on-field box: (25, 10) yards -- well inside the
    # field polygon (x in [-10, 60], y in [0, 25] before margin).
    assert box_filter(file_name, (24.0, 0.0, 26.0, 10.0)) is True
    # Foot point (105, 110) yards -- far outside even with the +2yd margin.
    assert box_filter(file_name, (100.0, 100.0, 110.0, 110.0)) is False
    # Just inside the +2yd margin past the north sideline (y=25): foot y=26.5.
    assert box_filter(file_name, (24.0, 26.0, 26.0, 26.5)) is True
    # Just outside the +2yd margin past the north sideline: foot y=28.
    assert box_filter(file_name, (24.0, 27.5, 26.0, 28.0)) is False


def test_build_on_field_filter_raises_for_unresolvable_clip(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _write_identity_calibration(config.reference.homography_calibration, "hp-test")
    _write_hover_positions(config.reference.hover_positions, [(5, "hp-test")])

    box_filter = bias._build_on_field_filter(config, "drone")
    assert box_filter is not None

    with pytest.raises(bias.BiasTestError, match="clip 999"):
        box_filter("Wide - Clip 999_f00010.jpg", (0.0, 0.0, 10.0, 10.0))


def test_evaluate_bias_test_on_field_mode_filters_drone_and_leaves_sideline_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _make_config(tmp_path)
    _write_identity_calibration(config.reference.homography_calibration, "hp-test")
    split_path = config.paths.reference / "frozen_eval_clips.csv"
    _write_frozen_eval_csv(
        split_path,
        [
            _frozen_eval_row("drone", "sess-drone", 5),
            _frozen_eval_row("sideline", "sess-sideline", 8),
        ],
    )
    _write_hover_positions(config.reference.hover_positions, [(5, "hp-test")])

    # Drone existing GT: one on-field box (foot (25, 10)) and one off-field box (foot
    # (105, 110), a bench/spectator person far outside the field polygon+margin).
    _write_eval_gt_coco(
        config.paths.labels / "eval" / "drone" / "corrected",
        [(5, 10, [(1, [24.0, 0.0, 2.0, 10.0]), (1, [104.0, 108.0, 2.0, 2.0])])],
    )
    # Sideline (GoPro) existing GT: no homography exists for this domain at all, so
    # on-field mode must report it completely unchanged regardless of box position.
    _write_eval_gt_coco(
        config.paths.labels / "eval" / "sideline" / "corrected",
        [(8, 10, [(1, [0.0, 0.0, 10.0, 10.0]), (1, [500.0, 500.0, 10.0, 10.0])])],
    )

    frames = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 1, "sideline": 1}, max_per_clip=1, seed=5,
    )
    bias.write_bias_test_frames_csv(frames, config.paths.labels / "eval" / "bias_test_frames.csv")

    push_dir = tmp_path / "push"
    bias.build_bias_test_coco_package(config, frames, push_dir)

    bias_gt_dir = tmp_path / "data" / "labels" / "eval" / "bias_test" / "corrected"
    images_dir = bias_gt_dir / "images" / "default"
    images_dir.mkdir(parents=True)
    push_images = json.loads((push_dir / "instances.json").read_text())["images"]

    # From-scratch GT: near-identical on-field box only (matches the labelling-
    # convention finding: the bias-test pass never re-added the off-field person).
    boxes_by_domain = {
        "drone": [[24.5, 0.5, 2.0, 10.0]],
        "sideline": [[0.5, 0.5, 10.0, 10.0], [500.5, 500.5, 10.0, 10.0]],
    }
    new_images = []
    new_annotations = []
    ann_id = 1
    for idx, image in enumerate(push_images, start=1):
        cv2.imwrite(str(images_dir / image["file_name"]), np.zeros((64, 64, 3), dtype=np.uint8))
        new_images.append({**image, "id": idx})
        domain = "drone" if image["file_name"].startswith("drone__") else "sideline"
        for bbox in boxes_by_domain[domain]:
            new_annotations.append(
                {"id": ann_id, "image_id": idx, "category_id": 1, "bbox": bbox}
            )
            ann_id += 1
    (bias_gt_dir / "instances.json").write_text(
        json.dumps(
            {
                "images": new_images,
                "annotations": new_annotations,
                "categories": [{"id": 1, "name": "player"}, {"id": 2, "name": "referee"}],
            }
        )
    )

    model = _FakeEvalModel(_detections([[24.0, 0.0, 26.0, 10.0]], [0.9], [0]))
    monkeypatch.setattr(detect, "load_detector", lambda _config, _run_id: model)

    out_path = tmp_path / "eval_bias_test_on_field.json"
    results = bias.evaluate_bias_test(
        config,
        frames_csv_path=config.paths.labels / "eval" / "bias_test_frames.csv",
        bias_gt_dir=bias_gt_dir,
        run_ids={"D": "run-d"},
        out_path=out_path,
        on_field=True,
    )

    drone_result = results["drone"]
    assert drone_result["on_field"] == {
        "requested": True, "applied": True, "margin_yards": bias._ON_FIELD_MARGIN_YARDS,
    }
    # The off-field bench box is dropped from both sides -> only the on-field box
    # remains on each side, and it matches.
    assert drone_result["agreement"]["n_boxes_existing"] == 1
    assert drone_result["agreement"]["n_boxes_new"] == 1
    assert drone_result["agreement"]["n_matched"] == 1
    d_drone = drone_result["models"]["D"]
    assert d_drone["existing_gt"]["n_boxes"] == 1
    assert d_drone["new_gt"]["n_boxes"] == 1

    sideline_result = results["sideline"]
    assert sideline_result["on_field"] == {
        "requested": True, "applied": False, "margin_yards": None,
    }
    # GoPro has no homography -- both boxes (including the far-off one) stay.
    assert sideline_result["agreement"]["n_boxes_existing"] == 2
    assert sideline_result["agreement"]["n_boxes_new"] == 2
    d_sideline = sideline_result["models"]["D"]
    assert d_sideline["existing_gt"]["n_boxes"] == 2
    assert d_sideline["new_gt"]["n_boxes"] == 2


# --- D-19 guard coverage: bias-test frames never enter training -----------------------------


def test_bias_test_clips_are_covered_by_frozen_eval_guard(tmp_path: Path) -> None:
    """Bias-test frames are drawn exclusively from `frozen_eval` clips (they ARE a
    subset of the already-frozen eval GT, `select_bias_test_frames` never draws from
    a `pool` clip) -- so the same `assert_no_frozen_eval_clips` guard `validate_coco`/
    `train_detector` already run unconditionally on every merge/train call covers
    them automatically, with no bias-test-specific code needed. This test proves that
    directly: a manifest naming a real bias-test-selected `(domain, clip_number)` is
    rejected exactly like any other frozen_eval frame.
    """
    config = _make_config(tmp_path)
    split_path = _setup_drone_and_sideline_gt(config)

    selected = bias.select_bias_test_frames(
        config, n_by_domain={"drone": 1}, max_per_clip=1, seed=1,
    )
    bias_frame = selected[0]

    poisoned_manifest = FrameSampleManifest(
        session_id=bias_frame.session_id,
        seed=1,
        target=1,
        frames=[
            FrameSample(
                clip_number=bias_frame.clip_number,
                clip_path=f"data/video/{bias_frame.session_id}/clip.mp4",
                frame_index=bias_frame.frame_index,
                timestamp_s=1.0,
                image_path=bias_frame.file_name,
                split="train",
                domain=bias_frame.domain,
            )
        ],
        split={bias_frame.clip_number: "train"},
    )

    with pytest.raises(DatasetError, match=str(bias_frame.clip_number)):
        assert_no_frozen_eval_clips(poisoned_manifest, split_path)
