"""Coverage for `flag_football_ep.cv.dataset`'s dataset-acceptance gate:
`validate_coco` and `dataset_hash`.

Every test builds a synthetic COCO package directly in `tmp_path` (a hand-written
`instances.json` plus tiny fake image files) and a matching `FrameSampleManifest` --
no real frames, no CVAT instance, no network. `small_bounds` patches the real
[250, 600]-image acceptance band down to a size a fast unit test can construct
directly for every rule *except* the floor/ceiling rules themselves, which get their
own dedicated tests against the real REQ-S2-02/D-06 thresholds.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Sequence

import pytest

from flag_football_ep.cv import dataset
from flag_football_ep.cv.dataset import CLASS_NAMES, DatasetError, DatasetStats
from flag_football_ep.cv.frames import FrameSample, FrameSampleManifest

# --- shared helpers --------------------------------------------------------------------


def _manifest(specs: Sequence[tuple[str, str]], session_id: str = "test-session") -> FrameSampleManifest:
    """Build a `FrameSampleManifest` whose frames are exactly `specs`
    (`(file_name, split)` pairs) -- `clip_path`/`frame_index`/`timestamp_s` are
    irrelevant to `validate_coco`/`dataset_hash` and filled with placeholder values.
    """
    frames = [
        FrameSample(
            clip_number=i,
            clip_path=f"data/video/test/clip_{i:03d}.mp4",
            frame_index=i,
            timestamp_s=float(i),
            image_path=file_name,
            split=split,
        )
        for i, (file_name, split) in enumerate(specs)
    ]
    split_by_clip = {i: split for i, (_, split) in enumerate(specs)}
    return FrameSampleManifest(
        session_id=session_id, seed=1, target=len(specs), frames=frames, split=split_by_clip
    )


def _default_categories() -> list[dict]:
    return [{"id": index + 1, "name": name} for index, name in enumerate(CLASS_NAMES)]


def _write_coco(
    coco_dir: Path, *, categories: list[dict], images: list[dict], annotations: list[dict]
) -> Path:
    coco_dir.mkdir(parents=True, exist_ok=True)
    annotation_path = coco_dir / "instances.json"
    annotation_path.write_text(
        json.dumps({"categories": categories, "images": images, "annotations": annotations}),
        encoding="utf-8",
    )
    return annotation_path


def _write_fake_image(coco_dir: Path, file_name: str, content: bytes = b"fake-jpeg-bytes") -> None:
    (coco_dir / file_name).write_bytes(content)


def _bulk_specs(n: int) -> list[tuple[str, str]]:
    return [(f"frame_{i:04d}.jpg", "train" if i % 5 else "val") for i in range(n)]


@pytest.fixture
def small_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patches `_MIN_IMAGES`/`_MAX_IMAGES` down to [2, 50] -- isolates tests of
    `validate_coco`'s other rules (vocabulary, image-set equality, bbox bounds, split
    coverage, empty-image counting) from the real 250/600 REQ-S2-02/D-06 thresholds.
    """
    monkeypatch.setattr(dataset, "_MIN_IMAGES", 2)
    monkeypatch.setattr(dataset, "_MAX_IMAGES", 50)


# --- validate_coco: happy path ----------------------------------------------------------


def test_validate_coco_valid_dataset_returns_stats(tmp_path: Path, small_bounds: None) -> None:
    coco_dir = tmp_path / "coco"
    specs = [
        ("frame_train_1.jpg", "train"),
        ("frame_train_2.jpg", "train"),
        ("frame_val_1.jpg", "val"),
        ("frame_val_2.jpg", "val"),
    ]
    manifest = _manifest(specs)

    images = [
        {"id": i + 1, "file_name": name, "width": 640, "height": 480}
        for i, (name, _) in enumerate(specs)
    ]
    annotations = [
        {"id": 1, "image_id": 1, "category_id": 1, "bbox": [10, 10, 50, 100]},  # train player
        {"id": 2, "image_id": 3, "category_id": 1, "bbox": [20, 20, 40, 90]},  # val player
        {"id": 3, "image_id": 3, "category_id": 2, "bbox": [200, 20, 30, 80]},  # val referee
        # image_id 2 and 4 carry zero annotations -- legal, counted as _empty_images.
    ]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)
    for name, _ in specs:
        _write_fake_image(coco_dir, name, content=f"bytes-for-{name}".encode())

    stats = dataset.validate_coco(coco_dir, manifest)

    assert isinstance(stats, DatasetStats)
    assert stats.n_images == 4
    assert stats.n_boxes["player"] == 2
    assert stats.n_boxes["referee"] == 1
    assert stats.n_boxes["_empty_images"] == 2
    assert stats.split_counts == {"train": 2, "val": 2}
    assert len(stats.content_sha256) == 64
    int(stats.content_sha256, 16)  # valid hex


def test_validate_coco_zero_annotation_images_counted_not_rejected(
    tmp_path: Path, small_bounds: None
) -> None:
    coco_dir = tmp_path / "coco"
    specs = [("frame_a.jpg", "train"), ("frame_b.jpg", "val")]
    manifest = _manifest(specs)

    images = [
        {"id": 1, "file_name": "frame_a.jpg", "width": 640, "height": 480},
        {"id": 2, "file_name": "frame_b.jpg", "width": 640, "height": 480},
    ]
    annotations = [
        {"id": 1, "image_id": 1, "category_id": 1, "bbox": [0, 0, 10, 10]},
        {"id": 2, "image_id": 2, "category_id": 1, "bbox": [0, 0, 10, 10]},
    ]
    # Add a third, zero-annotation image that is NOT part of the manifest split
    # requirement check (both splits already have a player box above) but must still
    # be counted, not rejected, when it carries no annotations at all.
    images.append({"id": 3, "file_name": "frame_c.jpg", "width": 640, "height": 480})
    manifest = _manifest(specs + [("frame_c.jpg", "train")])

    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)
    for name in ("frame_a.jpg", "frame_b.jpg", "frame_c.jpg"):
        _write_fake_image(coco_dir, name)

    stats = dataset.validate_coco(coco_dir, manifest)

    assert stats.n_images == 3
    assert stats.n_boxes["_empty_images"] == 1


# --- validate_coco: category vocabulary --------------------------------------------------


def test_validate_coco_extra_category_raises_named(tmp_path: Path, small_bounds: None) -> None:
    coco_dir = tmp_path / "coco"
    manifest = _manifest([])
    categories = _default_categories() + [{"id": 3, "name": "ball"}]
    _write_coco(coco_dir, categories=categories, images=[], annotations=[])

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(coco_dir, manifest)

    assert "ball" in str(exc_info.value)


def test_validate_coco_wrong_category_order_raises(tmp_path: Path, small_bounds: None) -> None:
    coco_dir = tmp_path / "coco"
    manifest = _manifest([])
    categories = [{"id": 1, "name": "referee"}, {"id": 2, "name": "player"}]
    _write_coco(coco_dir, categories=categories, images=[], annotations=[])

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(coco_dir, manifest)

    assert "referee" in str(exc_info.value) and "player" in str(exc_info.value)


# --- validate_coco: image set vs. manifest ------------------------------------------------


def test_validate_coco_missing_and_extra_images_reported_separately(
    tmp_path: Path, small_bounds: None
) -> None:
    coco_dir = tmp_path / "coco"
    manifest = _manifest(
        [("frame_a.jpg", "train"), ("frame_b.jpg", "train"), ("frame_c.jpg", "val")]
    )
    images = [
        {"id": 1, "file_name": "frame_a.jpg", "width": 640, "height": 480},
        {"id": 2, "file_name": "frame_d.jpg", "width": 640, "height": 480},
    ]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=[])

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(coco_dir, manifest)

    message = str(exc_info.value)
    assert "frame_b.jpg" in message and "frame_c.jpg" in message  # missing
    assert "frame_d.jpg" in message  # extra


# --- validate_coco: annotation integrity --------------------------------------------------


def test_validate_coco_annotation_unknown_image_id_raises(
    tmp_path: Path, small_bounds: None
) -> None:
    coco_dir = tmp_path / "coco"
    manifest = _manifest([("frame_a.jpg", "train")])
    images = [{"id": 1, "file_name": "frame_a.jpg", "width": 640, "height": 480}]
    annotations = [{"id": 1, "image_id": 999, "category_id": 1, "bbox": [0, 0, 10, 10]}]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(coco_dir, manifest)

    assert "999" in str(exc_info.value)


def test_validate_coco_annotation_unknown_category_id_raises(
    tmp_path: Path, small_bounds: None
) -> None:
    coco_dir = tmp_path / "coco"
    manifest = _manifest([("frame_a.jpg", "train")])
    images = [{"id": 1, "file_name": "frame_a.jpg", "width": 640, "height": 480}]
    annotations = [{"id": 1, "image_id": 1, "category_id": 77, "bbox": [0, 0, 10, 10]}]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(coco_dir, manifest)

    assert "77" in str(exc_info.value)


def test_validate_coco_degenerate_bbox_raises(tmp_path: Path, small_bounds: None) -> None:
    coco_dir = tmp_path / "coco"
    manifest = _manifest([("frame_a.jpg", "train")])
    images = [{"id": 1, "file_name": "frame_a.jpg", "width": 640, "height": 480}]
    annotations = [{"id": 1, "image_id": 1, "category_id": 1, "bbox": [0, 0, 0, 10]}]  # w=0
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(coco_dir, manifest)

    assert "frame_a.jpg" in str(exc_info.value)


def test_validate_coco_out_of_bounds_bbox_raises(tmp_path: Path, small_bounds: None) -> None:
    coco_dir = tmp_path / "coco"
    manifest = _manifest([("frame_a.jpg", "train")])
    images = [{"id": 1, "file_name": "frame_a.jpg", "width": 640, "height": 480}]
    # x + w = 630 + 50 = 680 > image width 640 -- box spills outside the frame.
    annotations = [{"id": 1, "image_id": 1, "category_id": 1, "bbox": [630, 0, 50, 10]}]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(coco_dir, manifest)

    assert "frame_a.jpg" in str(exc_info.value) and "bounds" in str(exc_info.value)


def test_validate_coco_tolerates_subpixel_bbox_overflow(tmp_path: Path, small_bounds: None) -> None:
    """CVAT derives a bbox from a corrected polygon by taking its coordinate extrema,
    which can land a fraction of a pixel outside the frame (observed up to ~0.26px on
    real corrected data) -- a floating-point artifact, not a labeling error, and must
    not fail the whole dataset build."""
    coco_dir = tmp_path / "coco"
    specs = [("frame_a.jpg", "train"), ("frame_b.jpg", "train")]
    manifest = _manifest(specs)
    images = [
        {"id": 1, "file_name": "frame_a.jpg", "width": 640, "height": 480},
        {"id": 2, "file_name": "frame_b.jpg", "width": 640, "height": 480},
    ]
    annotations = [{"id": 1, "image_id": 1, "category_id": 1, "bbox": [10.0, -0.26, 20.0, 30.0]}]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)
    _write_fake_image(coco_dir, "frame_a.jpg")
    _write_fake_image(coco_dir, "frame_b.jpg")

    stats = dataset.validate_coco(coco_dir, manifest)

    assert stats.n_boxes["player"] == 1


# --- validate_coco: split coverage ---------------------------------------------------------


def test_validate_coco_split_with_zero_player_boxes_raises_named_split(
    tmp_path: Path, small_bounds: None
) -> None:
    coco_dir = tmp_path / "coco"
    specs = [("frame_train.jpg", "train"), ("frame_val.jpg", "val")]
    manifest = _manifest(specs)
    images = [
        {"id": 1, "file_name": "frame_train.jpg", "width": 640, "height": 480},
        {"id": 2, "file_name": "frame_val.jpg", "width": 640, "height": 480},
    ]
    # Only the train split gets a player box; val gets a referee box only.
    annotations = [
        {"id": 1, "image_id": 1, "category_id": 1, "bbox": [0, 0, 10, 10]},
        {"id": 2, "image_id": 2, "category_id": 2, "bbox": [0, 0, 10, 10]},
    ]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(coco_dir, manifest)

    assert "val" in str(exc_info.value)


# --- validate_coco: image-count floor/ceiling (real REQ-S2-02/D-06 thresholds) -------------


def test_validate_coco_below_floor_raises_named_count(tmp_path: Path) -> None:
    coco_dir = tmp_path / "coco"
    specs = _bulk_specs(dataset._MIN_IMAGES - 1)
    manifest = _manifest(specs)
    images = [
        {"id": i + 1, "file_name": name, "width": 640, "height": 480}
        for i, (name, _) in enumerate(specs)
    ]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=[])

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(coco_dir, manifest)

    message = str(exc_info.value)
    assert str(dataset._MIN_IMAGES - 1) in message
    assert "300-500" in message or "floor" in message


def test_validate_coco_above_ceiling_raises_d06(tmp_path: Path) -> None:
    coco_dir = tmp_path / "coco"
    specs = _bulk_specs(dataset._MAX_IMAGES + 1)
    manifest = _manifest(specs)
    images = [
        {"id": i + 1, "file_name": name, "width": 640, "height": 480}
        for i, (name, _) in enumerate(specs)
    ]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=[])

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(coco_dir, manifest)

    assert "D-06" in str(exc_info.value)


# --- dataset_hash ----------------------------------------------------------------------


def test_dataset_hash_stable_across_directories_and_changes_on_bbox_flip(tmp_path: Path) -> None:
    coco_dir = tmp_path / "coco_a"
    images = [
        {"id": 1, "file_name": "frame_a.jpg", "width": 640, "height": 480},
        {"id": 2, "file_name": "frame_b.jpg", "width": 640, "height": 480},
    ]
    annotations = [{"id": 1, "image_id": 1, "category_id": 1, "bbox": [10, 10, 50, 60]}]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)
    _write_fake_image(coco_dir, "frame_a.jpg", content=b"content-a")
    _write_fake_image(coco_dir, "frame_b.jpg", content=b"content-b")

    # Same dataset copied to a second, differently-named tmp directory.
    coco_dir_copy = tmp_path / "coco_b_copy"
    shutil.copytree(coco_dir, coco_dir_copy)

    hash_original = dataset.dataset_hash(coco_dir)
    hash_copy = dataset.dataset_hash(coco_dir_copy)
    assert hash_original == hash_copy

    # Flip one bbox coordinate by a single pixel -- the hash must change.
    flipped_annotations = [{"id": 1, "image_id": 1, "category_id": 1, "bbox": [11, 10, 50, 60]}]
    _write_coco(
        coco_dir, categories=_default_categories(), images=images, annotations=flipped_annotations
    )
    hash_after_flip = dataset.dataset_hash(coco_dir)
    assert hash_after_flip != hash_original
