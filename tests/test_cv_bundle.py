"""Coverage for `cv.bundle.build_bundle`/`bundle_manifest`: content-hash determinism,
the `role = pool` leak guard (D-07/T-2.2-29), the test-kind label-refusal guard
(T-2.2-28), and manifest/README structure -- all against a synthetic tmp_path fixture
that mirrors the real bundle-inputs layout `export.py`/`freeze.py` produce.
"""

from __future__ import annotations

import json
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
from flag_football_ep.cv.bundle import (
    BundleError,
    BundleResult,
    build_bundle,
    bundle_manifest,
)
from flag_football_ep.cv.freeze import FreezePin
from flag_football_ep.cv.schema import (
    DETECTION_COLUMNS,
    TRACKING_COLUMNS,
    write_detections_parquet,
    write_tracking_parquet,
)

SESSION_ID = "test-session"
RUN_ID = "abc123def456abc123def456abc123d"
DATASET_HASH = "f" * 64
POOL_CLIPS = (1, 2, 4)
PRIVATE_TEST_CLIPS = (3,)
ALL_CLIPS = tuple(sorted(POOL_CLIPS + PRIVATE_TEST_CLIPS))


# --- shared config helper (mirrors tests/test_cv_export.py::_make_config) -----------


def _make_config(tmp_path: Path) -> Config:
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
        pilot_session_id=SESSION_ID,
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
        dvc_remote_name="otc-obs",
        dvc_remote_url="s3://test-bucket/flag-football-datasets",
        dvc_remote_endpoint="https://obs.eu-de.otc.t-systems.com",
        otc_obs_access_key_env="OTC_OBS_ACCESS_KEY_ID",
        otc_obs_secret_key_env="OTC_OBS_SECRET_ACCESS_KEY",
    )
    return Config(paths=paths, reference=reference, sources=sources, train=train, report=report, cv=cv)


def _populate_fixture(tmp_path: Path) -> Config:
    """Write every input `build_bundle` reads: video inventory + clip files, the
    frozen eval-clip split, bundle-input detections/crops, baseline tracks, overlays,
    and the three reference label/GT CSVs -- for `ALL_CLIPS`, split into
    `POOL_CLIPS`/`PRIVATE_TEST_CLIPS` exactly like the real `frozen_eval_clips.csv`.
    """
    config = _make_config(tmp_path)

    video_dir = tmp_path / "data" / "video" / SESSION_ID
    video_dir.mkdir(parents=True)
    inventory_rows = []
    for n in ALL_CLIPS:
        clip_path = video_dir / f"Wide - Clip {n:03d}.mp4"
        clip_path.write_bytes(f"fake-mp4-bytes-clip-{n}".encode("utf-8"))
        inventory_rows.append(
            {
                "domain": "drone",
                "session_id": SESSION_ID,
                "game_id": "",
                "capture_date": "2026-05-16",
                "resolution": "1920x1080",
                "fps": 30.0,
                "duration_seconds": 9.0,
                "local_path": f"data/video/{SESSION_ID}/Wide - Clip {n:03d}.mp4",
                "content_sha256": "0" * 64,
                "notes": "",
            }
        )
    config.paths.reference.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(inventory_rows).write_csv(config.paths.reference / "video_inventory.csv")

    eval_split_rows = []
    for n in ALL_CLIPS:
        is_private = n in PRIVATE_TEST_CLIPS
        eval_split_rows.append(
            {
                "domain": "drone",
                "session_id": SESSION_ID,
                "clip_number": n,
                "stratum_id": "hp-01",
                "role": "frozen_eval" if is_private else "pool",
                "private_test": is_private,
                "frozen_at": "2026-09-01T00:00:00+00:00",
                "seed": 20260516,
            }
        )
    pl.DataFrame(eval_split_rows).write_csv(config.paths.reference / "frozen_eval_clips.csv")

    freeze_pin_data = {
        "run_id": RUN_ID,
        "dataset_hash": DATASET_HASH,
        "frozen_at": "2026-09-01T00:00:00+00:00",
        "model_version": "1",
    }
    (config.paths.reference / "hackathon_freeze.json").write_text(
        json.dumps(freeze_pin_data, indent=2) + "\n", encoding="utf-8"
    )

    detections_rows = []
    for n in ALL_CLIPS:
        detections_rows.append(
            {
                "session_id": SESSION_ID,
                "clip_number": n,
                "frame_index": 0,
                "timestamp_s": 0.0,
                "det_index": 0,
                "class_name": "player",
                "confidence": 0.9,
                "bbox_x1": 10.0,
                "bbox_y1": 20.0,
                "bbox_x2": 30.0,
                "bbox_y2": 90.0,
                "detector_run_id": RUN_ID,
                "detected_at": "2026-09-01T00:00:00+00:00",
            }
        )
    bundle_inputs_dir = config.paths.labels / SESSION_ID / "bundle-inputs"
    bundle_inputs_dir.mkdir(parents=True, exist_ok=True)
    write_detections_parquet(
        pl.DataFrame(detections_rows).select(list(DETECTION_COLUMNS)),
        bundle_inputs_dir / "detections.parquet",
    )

    crops_dir = bundle_inputs_dir / "crops"
    crops_index_rows = []
    for n in ALL_CLIPS:
        rel_file = f"clip_{n:03d}/track_0000/frame_00000.jpg"
        crop_path = crops_dir / rel_file
        crop_path.parent.mkdir(parents=True, exist_ok=True)
        crop_path.write_bytes(f"fake-jpeg-bytes-clip-{n}".encode("utf-8"))
        crops_index_rows.append(
            {
                "session_id": SESSION_ID,
                "clip_number": n,
                "track_id": 0,
                "frame_index": 0,
                "team_id": 0,
                "class_name": "player",
                "file": rel_file,
            }
        )
    pl.DataFrame(crops_index_rows).write_csv(crops_dir / "index.csv")
    (crops_dir / "crops_meta.json").write_text(
        json.dumps(
            {
                "max_crops_per_track": 12,
                "n_crops": len(crops_index_rows),
                "detector_run_ids": [RUN_ID],
                "generated_at": "2026-09-01T00:00:00+00:00",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    tracks_df = pl.DataFrame(
        [
            {
                "session_id": SESSION_ID,
                "clip_number": n,
                "frame_index": 0,
                "timestamp_s": 0.0,
                "track_id": 0,
                "class_name": "player",
                "confidence": 0.9,
                "bbox_x1": 10.0,
                "bbox_y1": 20.0,
                "bbox_x2": 30.0,
                "bbox_y2": 90.0,
                "foot_x_px": 20.0,
                "foot_y_px": 90.0,
                "team_id": 0,
                "hover_position_id": "hp-01",
                "x_yards": 5.0,
                "y_yards": 5.0,
                "game_id": None,
                "play_id": None,
                "detector_run_id": RUN_ID,
                "tracked_at": "2026-09-01T00:00:00+00:00",
            }
            for n in ALL_CLIPS
        ]
    ).select(list(TRACKING_COLUMNS))
    write_tracking_parquet(tracks_df, config.paths.tracking / f"{SESSION_ID}_tracks.parquet")

    overlays_dir = config.paths.labels / SESSION_ID / "overlays"
    overlays_dir.mkdir(parents=True, exist_ok=True)
    for n in ALL_CLIPS:
        (overlays_dir / f"clip_{n:03d}.mp4").write_bytes(f"fake-overlay-{n}".encode("utf-8"))

    continuity_rows = [
        {
            "clip_number": n,
            "n_tracks": 4,
            "longest_track_frac": 1.0,
            "n_fragments": 0,
            "auto_flag": "ok",
            "verdict": "pass" if n % 2 == 0 else "fail",
            "id_switches": 0,
            "reviewer_note": "",
        }
        for n in ALL_CLIPS
    ]
    pl.DataFrame(continuity_rows).write_csv(config.paths.reference / "continuity_review.csv")

    flag_pull_rows = [
        {
            "clip_number": n,
            "outcome": "incomplete",
            "pull_time_s": None,
            "carrier_track_id": None,
            "puller_track_id": "",
            "notes": "",
        }
        for n in ALL_CLIPS
    ]
    pl.DataFrame(flag_pull_rows).write_csv(config.paths.reference / "flag_pull_events.csv")

    gt_rows = [
        {
            "clip_number": n,
            "frame_index": 0,
            "gt_id": f"c{n}f0p1",
            "class_name": "player",
            "team_hint": "red",
            "foot_x_px": 100.0,
            "foot_y_px": 200.0,
            "hover_position_id": "hp-01",
            "field_zone": "west-half",
            "scale_pair_id": "sp-1",
            "scale_true_yards": 4,
            "notes": "",
        }
        for n in ALL_CLIPS
    ]
    pl.DataFrame(gt_rows).write_csv(config.paths.reference / "gt_positions.csv")

    homography_rows = [
        {
            "hover_position_id": "hp-01",
            "landmark": "goalline_west_south",
            "source_x_px": 1740.9,
            "source_y_px": 748.3,
            "target_x_yards": 0.0,
            "target_y_yards": 0.0,
            "use_for_fit": True,
            "notes": "",
        }
    ]
    pl.DataFrame(homography_rows).write_csv(config.paths.reference / "homography_calibration.csv")

    return config


def _pin() -> FreezePin:
    return FreezePin(
        run_id=RUN_ID, dataset_hash=DATASET_HASH, frozen_at="2026-09-01T00:00:00+00:00", model_version="1"
    )


# --- tests ----------------------------------------------------------------------


def test_build_bundle_dev_creates_archive_and_manifest(tmp_path: Path) -> None:
    config = _populate_fixture(tmp_path)
    out_dir = tmp_path / "out"

    result = build_bundle(config, "dev", _pin(), out_dir)

    assert isinstance(result, BundleResult)
    assert result.archive_path.exists()
    assert result.archive_path.suffix == ".zip"
    assert result.manifest_path.exists()
    assert result.n_files > 0
    assert len(result.content_sha256) == 64

    staging_root = out_dir / "dev-set"
    assert (staging_root / "README.md").exists()
    assert (staging_root / "data" / "detections.parquet").exists()
    assert (staging_root / "data" / "tracks.parquet").exists()
    assert (staging_root / "data" / "continuity_review.csv").exists()


def test_build_bundle_dev_excludes_private_test_clips(tmp_path: Path) -> None:
    config = _populate_fixture(tmp_path)
    out_dir = tmp_path / "out"

    build_bundle(config, "dev", _pin(), out_dir)

    staging_root = out_dir / "dev-set"
    shipped_clip_files = {p.name for p in (staging_root / "data" / "clips").glob("*.mp4")}
    private_test_files = {f"clip_{n:03d}.mp4" for n in PRIVATE_TEST_CLIPS}

    assert shipped_clip_files & private_test_files == set()
    assert shipped_clip_files == {f"clip_{n:03d}.mp4" for n in POOL_CLIPS}

    detections = pl.read_parquet(staging_root / "data" / "detections.parquet")
    assert set(detections["clip_number"].to_list()) & set(PRIVATE_TEST_CLIPS) == set()

    continuity = pl.read_csv(staging_root / "data" / "continuity_review.csv")
    assert set(continuity["clip_number"].to_list()) == set(POOL_CLIPS)


def test_build_bundle_content_hash_deterministic_across_two_builds(tmp_path: Path) -> None:
    config = _populate_fixture(tmp_path)

    result_a = build_bundle(config, "dev", _pin(), tmp_path / "out-a")
    result_b = build_bundle(config, "dev", _pin(), tmp_path / "out-b")

    assert result_a.content_sha256 == result_b.content_sha256
    assert result_a.n_files == result_b.n_files


def test_build_bundle_rebuild_into_same_out_dir_is_idempotent(tmp_path: Path) -> None:
    config = _populate_fixture(tmp_path)
    out_dir = tmp_path / "out"

    result_1 = build_bundle(config, "dev", _pin(), out_dir)
    result_2 = build_bundle(config, "dev", _pin(), out_dir)

    assert result_1.content_sha256 == result_2.content_sha256


def test_build_bundle_missing_freeze_pin_raises_named_error(tmp_path: Path) -> None:
    config = _populate_fixture(tmp_path)
    (config.paths.reference / "hackathon_freeze.json").unlink()

    with pytest.raises(BundleError, match="ffep cv freeze"):
        build_bundle(config, "dev", _pin(), tmp_path / "out")


def test_build_bundle_stale_pin_object_raises(tmp_path: Path) -> None:
    config = _populate_fixture(tmp_path)
    stale_pin = FreezePin(
        run_id="stale-run-id-not-on-disk",
        dataset_hash=DATASET_HASH,
        frozen_at="2020-01-01T00:00:00+00:00",
        model_version="0",
    )

    with pytest.raises(BundleError, match="stale-run-id-not-on-disk"):
        build_bundle(config, "dev", stale_pin, tmp_path / "out")


def test_build_bundle_test_kind_refuses_label_bearing_file(tmp_path: Path) -> None:
    config = _populate_fixture(tmp_path)

    with pytest.raises(BundleError, match="continuity_review.csv"):
        build_bundle(config, "test", _pin(), tmp_path / "out")


def test_build_bundle_transfer_kind_not_implemented(tmp_path: Path) -> None:
    config = _populate_fixture(tmp_path)

    with pytest.raises(BundleError, match="02.2-12"):
        build_bundle(config, "transfer", _pin(), tmp_path / "out")


def test_build_bundle_unknown_kind_raises(tmp_path: Path) -> None:
    config = _populate_fixture(tmp_path)

    with pytest.raises(BundleError, match="unknown bundle kind"):
        build_bundle(config, "bogus", _pin(), tmp_path / "out")


def test_build_bundle_readme_is_german_with_english_column_names(tmp_path: Path) -> None:
    config = _populate_fixture(tmp_path)
    out_dir = tmp_path / "out"

    build_bundle(config, "dev", _pin(), out_dir)

    readme_text = (out_dir / "dev-set" / "README.md").read_text(encoding="utf-8")
    assert "Zweck" in readme_text
    assert "bbox_x1" in readme_text


def test_bundle_manifest_reads_back_written_manifest(tmp_path: Path) -> None:
    config = _populate_fixture(tmp_path)
    out_dir = tmp_path / "out"

    result = build_bundle(config, "dev", _pin(), out_dir)
    manifest = bundle_manifest(out_dir / "dev-set")

    assert manifest["kind"] == "dev"
    assert manifest["detector_run_id"] == RUN_ID
    assert manifest["dataset_hash"] == DATASET_HASH
    assert manifest["content_sha256"] == result.content_sha256
    assert len(manifest["files"]) == result.n_files - 1  # manifest.json itself excluded


def test_bundle_manifest_missing_raises(tmp_path: Path) -> None:
    with pytest.raises(BundleError, match="bundle manifest not found"):
        bundle_manifest(tmp_path / "does-not-exist")


def test_build_bundle_archive_contains_no_private_test_clip(tmp_path: Path) -> None:
    import zipfile

    config = _populate_fixture(tmp_path)
    result = build_bundle(config, "dev", _pin(), tmp_path / "out")

    with zipfile.ZipFile(result.archive_path) as archive:
        names = archive.namelist()

    for n in PRIVATE_TEST_CLIPS:
        assert not any(f"clip_{n:03d}.mp4" in name for name in names), names
    for n in POOL_CLIPS:
        assert any(f"data/clips/clip_{n:03d}.mp4" in name for name in names), names
