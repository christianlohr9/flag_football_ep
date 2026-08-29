"""Coverage for `flag_football_ep.cv.teams.TeamClassifier`: deterministic two-cluster
splitting from an injected embedder, sample-size guards, and offline-only imports.

Every test injects a synthetic `embedder` (an identity function over pre-computed
feature vectors) so no SigLIP weights are ever downloaded and no network round trip
happens -- per RESEARCH's Validation Architecture rule ("no CV test should require
downloading model weights or real footage").

`extract_track_crops` coverage (gap-fix iteration, added alongside the torso-crop
default) uses tiny, real, decodable synthetic clips (`cv2.VideoWriter`, mirroring
`tests/test_cv_track.py`'s convention) -- no network, no real footage, but real
decode/crop arithmetic against actual pixel data.
"""

from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np
import polars as pl
import pytest

pytest.importorskip("umap", reason="requires the cv extras group (uv sync --extra cv)")

from flag_football_ep.cv import teams as teams_module
from flag_football_ep.cv.detect import MissingClipError
from flag_football_ep.cv.schema import conform_tracking
from flag_football_ep.cv.teams import (
    ClassifierNotFitted,
    InsufficientCrops,
    TeamAssignmentResult,
    TeamClassifier,
    assign_teams,
    extract_track_crops,
)
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


def _identity_embedder(crops) -> np.ndarray:
    """The injected `embedder`: `crops` here are already feature vectors, so this is a
    pure pass-through -- no SigLIP model, no network.
    """
    return np.asarray(crops, dtype=float)


def _two_gaussian_blobs(
    n_per_blob: int = 40, dim: int = 8, seed: int = 1
) -> tuple[np.ndarray, np.ndarray]:
    """Two well-separated Gaussian blobs in `dim`-dimensional space.

    Returns `(vectors, blob_id)` where `blob_id[i]` is 0 or 1 indicating which blob
    `vectors[i]` was drawn from (the ground-truth partition, not a predicted label).
    """
    rng = np.random.default_rng(seed)
    mean_a = np.zeros(dim)
    mean_b = np.full(dim, 20.0)
    blob_a = rng.normal(loc=mean_a, scale=0.5, size=(n_per_blob, dim))
    blob_b = rng.normal(loc=mean_b, scale=0.5, size=(n_per_blob, dim))
    vectors = np.concatenate([blob_a, blob_b], axis=0)
    blob_id = np.concatenate([np.zeros(n_per_blob), np.ones(n_per_blob)])
    return vectors, blob_id


def _as_partition(labels: np.ndarray) -> frozenset[frozenset[int]]:
    """Convert a label array into a set-partition of indices, so cluster *label value*
    (0 vs 1, arbitrary per TeamClassifier's own contract) never matters -- only which
    indices ended up grouped together does.
    """
    groups: dict = {}
    for i, label in enumerate(labels):
        groups.setdefault(int(label), set()).add(i)
    return frozenset(frozenset(group) for group in groups.values())


# --- fit/predict on well-separated blobs ----------------------------------------------------


def test_fit_splits_two_well_separated_blobs_into_correct_partition() -> None:
    vectors, blob_id = _two_gaussian_blobs()
    classifier = TeamClassifier(embedder=_identity_embedder, seed=20260516)

    classifier.fit(vectors)
    predicted = classifier.predict(vectors)

    assert _as_partition(predicted) == _as_partition(blob_id)


def test_fit_is_deterministic_for_the_same_seed() -> None:
    vectors, _blob_id = _two_gaussian_blobs()

    first = TeamClassifier(embedder=_identity_embedder, seed=20260516)
    first.fit(vectors)
    first_labels = first.predict(vectors)

    second = TeamClassifier(embedder=_identity_embedder, seed=20260516)
    second.fit(vectors)
    second_labels = second.predict(vectors)

    assert _as_partition(first_labels) == _as_partition(second_labels)


def test_predict_on_unseen_vectors_from_same_blobs_assigns_matching_clusters() -> None:
    fit_vectors, fit_blob_id = _two_gaussian_blobs(seed=1)
    unseen_vectors, unseen_blob_id = _two_gaussian_blobs(seed=2)

    classifier = TeamClassifier(embedder=_identity_embedder, seed=20260516)
    classifier.fit(fit_vectors)

    fit_labels = classifier.predict(fit_vectors)
    unseen_labels = classifier.predict(unseen_vectors)

    # Map each ground-truth blob id to the cluster label the classifier assigned to it
    # on the *fit* data, then check the unseen predictions follow the same mapping --
    # this avoids assuming cluster label 0/1 is stable, only that the partition is.
    label_for_blob = {}
    for blob, label in zip(fit_blob_id, fit_labels, strict=True):
        label_for_blob.setdefault(int(blob), int(label))

    expected = np.array([label_for_blob[int(b)] for b in unseen_blob_id])
    assert np.array_equal(unseen_labels, expected)


# --- sample-size and fit-order guards --------------------------------------------------------


def test_fit_with_too_few_crops_raises_insufficient_crops_naming_count_and_minimum() -> None:
    too_few = np.zeros((19, 4))
    classifier = TeamClassifier(embedder=_identity_embedder, seed=20260516)

    with pytest.raises(InsufficientCrops) as exc_info:
        classifier.fit(too_few)

    message = str(exc_info.value)
    assert "19" in message
    assert "20" in message


def test_predict_before_fit_raises_classifier_not_fitted() -> None:
    classifier = TeamClassifier(embedder=_identity_embedder, seed=20260516)

    with pytest.raises(ClassifierNotFitted):
        classifier.predict(np.zeros((5, 4)))


# --- import hygiene: no network-heavy dependency at module level ----------------------------


def test_import_teams_module_pulls_in_neither_transformers_nor_torch() -> None:
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, flag_football_ep.cv.teams as t; "
            "assert 'transformers' not in sys.modules; "
            "assert 'torch' not in sys.modules; "
            "print('ok')",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout


# --- assign_teams coverage (plan 02.1-12 Task 2) ---------------------------------------------


def _make_config() -> Config:
    """A minimal but fully-populated Config -- `assign_teams` only reads `config.cv.device`."""
    paths = Paths(
        data_root=Path("data"),
        raw_hudl=Path("data/raw/hudl"),
        raw_sportapp=Path("data/raw/sportapp"),
        raw_ifaf=Path("data/raw/ifaf"),
        raw_legacy=Path("data/raw/legacy"),
        processed=Path("data/processed"),
        reference=Path("data/reference"),
        models=Path("models"),
        mlruns=Path("mlruns"),
        contract=Path("docs/data-contract.schema.json"),
        reports=Path("reports"),
        video=Path("data/video"),
        labels=Path("data/labels"),
        tracking=Path("data/processed/tracking"),
    )
    reference = ReferenceFiles(
        half_boundaries=Path("data/reference/half_boundaries.csv"),
        final_scores=Path("data/reference/final_scores.csv"),
        team_mapping=Path("data/reference/team_mapping.csv"),
        sportapp_games=Path("data/reference/sportapp_games.csv"),
        competition_tier=Path("data/reference/competition_tier.csv"),
        player_mapping=Path("data/reference/player_mapping.csv"),
        group_opponents=Path("data/reference/group_opponents.csv"),
        hover_positions=Path("data/reference/hover_positions.csv"),
        homography_calibration=Path("data/reference/homography_calibration.csv"),
        gt_positions=Path("data/reference/gt_positions.csv"),
        continuity_review=Path("data/reference/continuity_review.csv"),
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


def _tracks_row(*, clip_number: int, track_id: int, class_name: str, frame_index: int) -> dict:
    return {
        "session_id": "test-session",
        "clip_number": clip_number,
        "frame_index": frame_index,
        "timestamp_s": frame_index / 30.0,
        "track_id": track_id,
        "class_name": class_name,
        "confidence": 0.9,
        "bbox_x1": 0.0,
        "bbox_y1": 0.0,
        "bbox_x2": 10.0,
        "bbox_y2": 10.0,
        "foot_x_px": 5.0,
        "foot_y_px": 10.0,
        "detector_run_id": "0" * 32,
        "tracked_at": "2026-08-29T00:00:00Z",
    }


def _make_tracks(spec: list[tuple[int, int, str]], n_frames: int = 2) -> pl.DataFrame:
    """`spec` is a list of `(clip_number, track_id, class_name)`; each gets `n_frames` rows."""
    rows = [
        _tracks_row(clip_number=clip, track_id=track, class_name=class_name, frame_index=i)
        for clip, track, class_name in spec
        for i in range(n_frames)
    ]
    return pl.DataFrame(rows)


def _blob(mean: float, n: int, *, dim: int = 6, seed: int) -> list[np.ndarray]:
    rng = np.random.default_rng(seed)
    return [rng.normal(loc=mean, scale=0.5, size=dim) for _ in range(n)]


def _install_identity_classifier(monkeypatch: pytest.MonkeyPatch, seen_crops: list) -> None:
    """Monkeypatch `teams.TeamClassifier` (the name `assign_teams` looks up at call
    time) to a factory that ignores `device` and uses a spy identity embedder --
    real UMAP/KMeans clustering, zero SigLIP/network.
    """

    def _spy_embedder(crops) -> np.ndarray:
        seen_crops.extend(crops)
        return _identity_embedder(crops)

    def _factory(device: str = "cpu") -> TeamClassifier:
        return TeamClassifier(embedder=_spy_embedder, seed=20260516)

    monkeypatch.setattr(teams_module, "TeamClassifier", _factory)


def _two_team_dataset() -> tuple[pl.DataFrame, dict[tuple[int, int], list[np.ndarray]]]:
    spec = [
        (1, 0, "referee"),
        (1, 1, "player"),
        (1, 2, "player"),
        (2, 0, "referee"),
        (2, 1, "player"),
        (2, 2, "player"),
    ]
    tracks = _make_tracks(spec)
    # Referee crops are wildly out of range: if they ever reached the fit they would
    # visibly corrupt the two-blob clustering below.
    crops_by_track = {
        (1, 0): _blob(500.0, 6, seed=101),
        (1, 1): _blob(0.0, 25, seed=1),
        (1, 2): _blob(20.0, 25, seed=2),
        (2, 0): _blob(500.0, 6, seed=102),
        (2, 1): _blob(0.0, 25, seed=3),
        (2, 2): _blob(20.0, 25, seed=4),
    }
    return tracks, crops_by_track


def test_two_clearly_separated_groups_yield_two_team_ids_consistent_per_track(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracks, crops_by_track = _two_team_dataset()

    seen_crops: list = []
    _install_identity_classifier(monkeypatch, seen_crops)

    result = assign_teams(tracks, _make_config(), crops_by_track=crops_by_track)

    assert isinstance(result, TeamAssignmentResult)
    df = result.tracks

    # Every row of a given track shares exactly one team_id.
    for clip, track in ((1, 1), (1, 2), (2, 1), (2, 2)):
        team_ids = set(
            df.filter((pl.col("clip_number") == clip) & (pl.col("track_id") == track))[
                "team_id"
            ].to_list()
        )
        assert len(team_ids) == 1, (clip, track, team_ids)

    # Exactly two distinct team ids across every player row.
    player_team_ids = set(df.filter(pl.col("class_name") == "player")["team_id"].to_list())
    assert len(player_team_ids) == 2


def test_referee_rows_are_null_and_referee_crops_never_reach_the_fit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracks, crops_by_track = _two_team_dataset()

    seen_crops: list = []
    _install_identity_classifier(monkeypatch, seen_crops)

    result = assign_teams(tracks, _make_config(), crops_by_track=crops_by_track)

    referee_team_ids = set(
        result.tracks.filter(pl.col("class_name") == "referee")["team_id"].to_list()
    )
    assert referee_team_ids == {None}

    # Referee crops never reached the embedder, in the fit call or any predict call.
    referee_crop_values = {tuple(v) for v in crops_by_track[(1, 0)]} | {
        tuple(v) for v in crops_by_track[(2, 0)]
    }
    seen_values = {tuple(v) for v in seen_crops}
    assert not (referee_crop_values & seen_values)


def test_ambiguous_track_gets_null_team_id_and_a_notice_naming_the_track_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = [
        (1, 1, "player"),
        (1, 2, "player"),
        (1, 3, "player"),
    ]
    tracks = _make_tracks(spec)

    # Track 3's crops split exactly 50/50 between the two blobs the classifier learns
    # from tracks 1/2 -- a coin flip, not a real signal.
    crops_by_track = {
        (1, 1): _blob(0.0, 25, seed=1),
        (1, 2): _blob(20.0, 25, seed=2),
        (1, 3): _blob(0.0, 5, seed=3) + _blob(20.0, 5, seed=4),
    }

    seen_crops: list = []
    _install_identity_classifier(monkeypatch, seen_crops)

    result = assign_teams(tracks, _make_config(), crops_by_track=crops_by_track)

    ambiguous_team_ids = set(
        result.tracks.filter(pl.col("track_id") == 3)["team_id"].to_list()
    )
    assert ambiguous_team_ids == {None}
    assert any("3" in notice for notice in result.notices), result.notices


def test_session_wide_fit_is_independent_of_crops_by_track_iteration_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = [
        (1, 1, "player"),
        (1, 2, "player"),
        (2, 1, "player"),
        (2, 2, "player"),
    ]
    tracks = _make_tracks(spec)
    crops_by_track = {
        (1, 1): _blob(0.0, 25, seed=1),
        (1, 2): _blob(20.0, 25, seed=2),
        (2, 1): _blob(0.0, 25, seed=3),
        (2, 2): _blob(20.0, 25, seed=4),
    }
    reversed_crops_by_track = dict(reversed(list(crops_by_track.items())))

    seen_a: list = []
    _install_identity_classifier(monkeypatch, seen_a)
    result_a = assign_teams(tracks, _make_config(), crops_by_track=crops_by_track)

    seen_b: list = []
    _install_identity_classifier(monkeypatch, seen_b)
    result_b = assign_teams(tracks, _make_config(), crops_by_track=reversed_crops_by_track)

    sort_cols = ["clip_number", "track_id", "frame_index"]
    left = result_a.tracks.sort(sort_cols)
    right = result_b.tracks.sort(sort_cols)
    assert left["team_id"].to_list() == right["team_id"].to_list()


def test_assign_teams_output_passes_conform_tracking(monkeypatch: pytest.MonkeyPatch) -> None:
    spec = [
        (1, 0, "referee"),
        (1, 1, "player"),
        (1, 2, "player"),
    ]
    tracks = _make_tracks(spec)
    crops_by_track = {
        (1, 0): _blob(500.0, 6, seed=201),
        (1, 1): _blob(0.0, 25, seed=1),
        (1, 2): _blob(20.0, 25, seed=2),
    }

    seen_crops: list = []
    _install_identity_classifier(monkeypatch, seen_crops)

    result = assign_teams(tracks, _make_config(), crops_by_track=crops_by_track)

    reconformed = conform_tracking(result.tracks)
    assert reconformed.height == result.tracks.height


# --- extract_track_crops coverage (gap-fix iteration) -----------------------------------------


def _make_config_tmp(tmp_path: Path, *, session_id: str = "test-session") -> Config:
    """Mirrors `tests/test_cv_track.py::_make_config` -- every path under `tmp_path`,
    never the real repo, so `frames.clip_paths`' inventory resolution has somewhere
    real to decode from.
    """
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
        pilot_session_id=session_id,
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


_CROP_TEST_FPS = 10.0
_INVENTORY_HEADER = (
    "domain,session_id,game_id,capture_date,resolution,fps,duration_seconds,"
    "local_path,content_sha256,notes"
)


def _write_inventory_tmp(config: Config, rows: list[dict[str, str]]) -> Path:
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
    lines = [_INVENTORY_HEADER] + [
        ",".join(row.get(field, "") for field in fields) for row in rows
    ]
    inventory_path = config.paths.reference / "video_inventory.csv"
    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    inventory_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return inventory_path


def _inventory_row_tmp(
    local_path: str, *, duration_seconds: float, session_id: str = "test-session"
) -> dict[str, str]:
    return {
        "domain": "drone",
        "session_id": session_id,
        "game_id": "",
        "capture_date": "2026-05-16",
        "resolution": "1920x1080",
        "fps": str(_CROP_TEST_FPS),
        "duration_seconds": str(duration_seconds),
        "local_path": local_path,
        "content_sha256": "",
        "notes": "",
    }


def _write_two_tone_clip(
    path: Path, n_frames: int, *, width: int = 100, height: int = 100
) -> Path:
    """A tiny, real, decodable clip: every frame's top half is a uniform dark value
    (10) and bottom half a uniform light value (200), the same on every channel so
    BGR/RGB conversion never changes the recorded value -- lets a test assert exactly
    which half of the frame a crop came from without caring about channel order.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(path), fourcc, _CROP_TEST_FPS, (width, height))
    try:
        for _ in range(n_frames):
            frame = np.full((height, width, 3), 10, dtype=np.uint8)
            frame[height // 2 :, :, :] = 200
            writer.write(frame)
    finally:
        writer.release()
    return path


def _crop_tracks_row(
    *, clip_number: int, track_id: int, frame_index: int, class_name: str = "player"
) -> dict:
    # bbox spans y=[10, 90] of a 100-tall frame: top half of the box (y<50) lands in
    # the clip's dark (10) region, bottom half in the light (200) region.
    return {
        "session_id": "test-session",
        "clip_number": clip_number,
        "frame_index": frame_index,
        "timestamp_s": frame_index / _CROP_TEST_FPS,
        "track_id": track_id,
        "class_name": class_name,
        "confidence": 0.9,
        "bbox_x1": 20.0,
        "bbox_y1": 10.0,
        "bbox_x2": 60.0,
        "bbox_y2": 90.0,
        "foot_x_px": 40.0,
        "foot_y_px": 90.0,
        "detector_run_id": "0" * 32,
        "tracked_at": "2026-08-29T00:00:00Z",
    }


def test_extract_track_crops_torso_default_only_covers_the_dark_top_half(
    tmp_path: Path,
) -> None:
    config = _make_config_tmp(tmp_path)
    n_frames = 4
    _write_two_tone_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    _write_inventory_tmp(
        config, [_inventory_row_tmp("data/video/Wide - Clip 001.mp4", duration_seconds=0.4)]
    )
    tracks = pl.DataFrame(
        [_crop_tracks_row(clip_number=1, track_id=0, frame_index=i) for i in range(n_frames)]
    )

    crops = extract_track_crops(config, "test-session", tracks)

    assert (1, 0) in crops
    for crop in crops[(1, 0)]:
        # bbox height is 80px (10..90); torso keeps the upper 50% -> y in [10, 50),
        # entirely inside the clip's dark (10) top half.
        assert crop.max() <= 30, "torso crop leaked into the light bottom half"


def test_extract_track_crops_full_box_spans_both_tones_when_torso_false(
    tmp_path: Path,
) -> None:
    config = _make_config_tmp(tmp_path)
    n_frames = 4
    _write_two_tone_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    _write_inventory_tmp(
        config, [_inventory_row_tmp("data/video/Wide - Clip 001.mp4", duration_seconds=0.4)]
    )
    tracks = pl.DataFrame(
        [_crop_tracks_row(clip_number=1, track_id=0, frame_index=i) for i in range(n_frames)]
    )

    crops = extract_track_crops(config, "test-session", tracks, torso=False)

    for crop in crops[(1, 0)]:
        assert crop.min() <= 30
        assert crop.max() >= 180, "full-box crop should still reach the light bottom half"


def test_extract_track_crops_caps_and_spreads_samples_across_the_track_lifetime(
    tmp_path: Path,
) -> None:
    config = _make_config_tmp(tmp_path)
    n_frames = 40
    _write_two_tone_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    _write_inventory_tmp(
        config, [_inventory_row_tmp("data/video/Wide - Clip 001.mp4", duration_seconds=4.0)]
    )
    tracks = pl.DataFrame(
        [_crop_tracks_row(clip_number=1, track_id=0, frame_index=i) for i in range(n_frames)]
    )

    crops = extract_track_crops(config, "test-session", tracks, max_crops_per_track=5)

    assert len(crops[(1, 0)]) == 5


def test_extract_track_crops_keys_by_clip_number_and_track_id_across_clips(
    tmp_path: Path,
) -> None:
    config = _make_config_tmp(tmp_path)
    n_frames = 3
    _write_two_tone_clip(config.paths.video / "Wide - Clip 001.mp4", n_frames)
    _write_two_tone_clip(config.paths.video / "Wide - Clip 002.mp4", n_frames)
    _write_inventory_tmp(
        config,
        [
            _inventory_row_tmp("data/video/Wide - Clip 001.mp4", duration_seconds=0.3),
            _inventory_row_tmp("data/video/Wide - Clip 002.mp4", duration_seconds=0.3),
        ],
    )
    tracks = pl.DataFrame(
        [_crop_tracks_row(clip_number=1, track_id=0, frame_index=i) for i in range(n_frames)]
        + [_crop_tracks_row(clip_number=2, track_id=0, frame_index=i) for i in range(n_frames)]
        + [_crop_tracks_row(clip_number=2, track_id=1, frame_index=i) for i in range(n_frames)]
    )

    crops = extract_track_crops(config, "test-session", tracks)

    assert set(crops.keys()) == {(1, 0), (2, 0), (2, 1)}


def test_extract_track_crops_raises_missing_clip_error_naming_the_path(
    tmp_path: Path,
) -> None:
    config = _make_config_tmp(tmp_path)
    bad_clip = config.paths.video / "Wide - Clip 001.mp4"
    bad_clip.parent.mkdir(parents=True, exist_ok=True)
    bad_clip.write_bytes(b"not a real video file")
    _write_inventory_tmp(
        config, [_inventory_row_tmp("data/video/Wide - Clip 001.mp4", duration_seconds=0.3)]
    )
    tracks = pl.DataFrame([_crop_tracks_row(clip_number=1, track_id=0, frame_index=0)])

    with pytest.raises(MissingClipError) as exc_info:
        extract_track_crops(config, "test-session", tracks)

    assert str(bad_clip) in str(exc_info.value)
