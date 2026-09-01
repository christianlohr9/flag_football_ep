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


def _manifest(
    specs: Sequence[tuple[str, str]],
    session_id: str = "test-session",
    *,
    domains: Sequence[str] | None = None,
) -> FrameSampleManifest:
    """Build a `FrameSampleManifest` whose frames are exactly `specs`
    (`(file_name, split)` pairs) -- `clip_path`/`frame_index`/`timestamp_s` are
    irrelevant to `validate_coco`/`dataset_hash` and filled with placeholder values.

    `domains`, when given, assigns `FrameSample.domain` positionally (one entry per
    spec); omitted entirely, every frame keeps `FrameSample`'s own `"drone"` default
    -- the single-domain Phase-2.1 shape every pre-existing test in this file targets.
    """
    frames = [
        FrameSample(
            clip_number=i,
            clip_path=f"data/video/test/clip_{i:03d}.mp4",
            frame_index=i,
            timestamp_s=float(i),
            image_path=file_name,
            split=split,
            **({"domain": domains[i]} if domains is not None else {}),
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


# --- validate_coco: multi-domain (plan 02.2-09 Task 3, C-05/D-04) -------------------------


def test_validate_coco_two_domains_both_with_player_boxes_passes(
    tmp_path: Path, small_bounds: None
) -> None:
    coco_dir = tmp_path / "coco"
    specs = [
        ("frame_drone_1.jpg", "train"),
        ("frame_drone_2.jpg", "val"),
        ("frame_sideline_1.jpg", "train"),
        ("frame_sideline_2.jpg", "val"),
    ]
    manifest = _manifest(specs, domains=["drone", "drone", "sideline", "sideline"])
    images = [
        {"id": i + 1, "file_name": name, "width": 640, "height": 480}
        for i, (name, _) in enumerate(specs)
    ]
    annotations = [
        {"id": 1, "image_id": 1, "category_id": 1, "bbox": [10, 10, 50, 100]},  # drone train
        {"id": 2, "image_id": 2, "category_id": 1, "bbox": [10, 10, 50, 100]},  # drone val
        {"id": 3, "image_id": 3, "category_id": 1, "bbox": [10, 10, 50, 100]},  # sideline train
        {"id": 4, "image_id": 4, "category_id": 1, "bbox": [10, 10, 50, 100]},  # sideline val
    ]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)

    stats = dataset.validate_coco(coco_dir, manifest)

    assert set(stats.boxes_by_domain) == {"drone", "sideline"}
    assert stats.boxes_by_domain["drone"]["player"] == 2
    assert stats.boxes_by_domain["sideline"]["player"] == 2


def test_validate_coco_domain_with_only_referee_boxes_raises_named_domain(
    tmp_path: Path, small_bounds: None
) -> None:
    coco_dir = tmp_path / "coco"
    specs = [
        ("frame_drone_1.jpg", "train"),
        ("frame_drone_2.jpg", "val"),
        ("frame_sideline_1.jpg", "train"),
        ("frame_sideline_2.jpg", "val"),
    ]
    manifest = _manifest(specs, domains=["drone", "drone", "sideline", "sideline"])
    images = [
        {"id": i + 1, "file_name": name, "width": 640, "height": 480}
        for i, (name, _) in enumerate(specs)
    ]
    annotations = [
        {"id": 1, "image_id": 1, "category_id": 1, "bbox": [10, 10, 50, 100]},  # drone player
        {"id": 2, "image_id": 2, "category_id": 1, "bbox": [10, 10, 50, 100]},  # drone player
        # sideline gets referee boxes only -- the domain has collapsed for "player".
        {"id": 3, "image_id": 3, "category_id": 2, "bbox": [10, 10, 50, 100]},
        {"id": 4, "image_id": 4, "category_id": 2, "bbox": [10, 10, 50, 100]},
    ]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(coco_dir, manifest)

    assert "sideline" in str(exc_info.value)


def test_validate_coco_multidomain_band_accepts_1500_rejects_120(tmp_path: Path) -> None:
    coco_dir = tmp_path / "coco"
    specs = _bulk_specs(1500)
    manifest = _manifest(specs, domains=["drone"] * 1500)
    images = [
        {"id": i + 1, "file_name": name, "width": 640, "height": 480}
        for i, (name, _) in enumerate(specs)
    ]
    # image_id 1 (index 0) is "val", image_id 2 (index 1) is "train" per _bulk_specs'
    # own `"train" if i % 5 else "val"` alternation -- one player box in each split
    # keeps both the split-coverage check and the domain-collapse check satisfied.
    annotations = [
        {"id": 1, "image_id": 1, "category_id": 1, "bbox": [0, 0, 10, 10]},
        {"id": 2, "image_id": 2, "category_id": 1, "bbox": [0, 0, 10, 10]},
    ]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)

    stats = dataset.validate_coco(
        coco_dir,
        manifest,
        min_images=dataset._MIN_IMAGES_MULTIDOMAIN,
        max_images=dataset._MAX_IMAGES_MULTIDOMAIN,
    )
    assert stats.n_images == 1500

    small_specs = _bulk_specs(120)
    small_manifest = _manifest(small_specs, domains=["drone"] * 120)
    small_images = [
        {"id": i + 1, "file_name": name, "width": 640, "height": 480}
        for i, (name, _) in enumerate(small_specs)
    ]
    small_coco_dir = tmp_path / "coco_small"
    _write_coco(
        small_coco_dir,
        categories=_default_categories(),
        images=small_images,
        annotations=[{"id": 1, "image_id": 1, "category_id": 1, "bbox": [0, 0, 10, 10]}],
    )

    with pytest.raises(DatasetError) as exc_info:
        dataset.validate_coco(
            small_coco_dir,
            small_manifest,
            min_images=dataset._MIN_IMAGES_MULTIDOMAIN,
            max_images=dataset._MAX_IMAGES_MULTIDOMAIN,
        )
    assert "1500" in str(exc_info.value)


def test_validate_coco_pilot_single_domain_still_validates_under_default_band(
    tmp_path: Path, small_bounds: None
) -> None:
    """The pilot's single-domain manifest (every frame defaulting to `domain="drone"`,
    predating this field) still validates under the default `[_MIN_IMAGES,
    _MAX_IMAGES]` band with no `min_images`/`max_images` passed at all.
    """
    coco_dir = tmp_path / "coco"
    specs = [("frame_a.jpg", "train"), ("frame_b.jpg", "val")]
    manifest = _manifest(specs)  # no domains= -> every frame defaults to "drone"
    images = [
        {"id": 1, "file_name": "frame_a.jpg", "width": 640, "height": 480},
        {"id": 2, "file_name": "frame_b.jpg", "width": 640, "height": 480},
    ]
    annotations = [
        {"id": 1, "image_id": 1, "category_id": 1, "bbox": [0, 0, 10, 10]},
        {"id": 2, "image_id": 2, "category_id": 1, "bbox": [0, 0, 10, 10]},
    ]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)

    stats = dataset.validate_coco(coco_dir, manifest)

    assert stats.boxes_by_domain == {"drone": {"player": 2, "referee": 0, "_empty_images": 0}}


def test_grep_min_images_constant_still_present_unmodified() -> None:
    """`_MIN_IMAGES = 250` (the 2.1 band) was parameterised via keyword arguments,
    not overwritten -- this greps the source file the same way the plan's own
    acceptance criterion does.
    """
    import subprocess

    result = subprocess.run(
        ["grep", "-c", "_MIN_IMAGES = 250", "src/flag_football_ep/cv/dataset.py"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parent.parent,
    )
    assert result.stdout.strip() == "1"


# --- split_coco_for_task_upload (plan 02.2-11: weekend-sized CVAT tasks) ---------------


def _write_split_source(coco_dir: Path, n_images: int) -> None:
    images = [{"id": i, "file_name": f"frame_{i:04d}.jpg", "width": 4, "height": 4} for i in range(n_images)]
    annotations = [
        {"id": i, "image_id": i, "category_id": 1, "bbox": [0, 0, 1, 1]} for i in range(n_images)
    ]
    _write_coco(coco_dir, categories=_default_categories(), images=images, annotations=annotations)
    for image in images:
        _write_fake_image(coco_dir, image["file_name"])


def test_split_coco_for_task_upload_caps_every_chunk_at_max_images(tmp_path: Path) -> None:
    coco_dir = tmp_path / "coco"
    _write_split_source(coco_dir, n_images=7)

    chunks = dataset.split_coco_for_task_upload(coco_dir, tmp_path / "out", max_images=3)

    assert len(chunks) == 3  # ceil(7 / 3)
    total_images = 0
    for chunk_dir in chunks:
        data = json.loads((chunk_dir / "instances.json").read_text(encoding="utf-8"))
        assert len(data["images"]) <= 3
        total_images += len(data["images"])
        for image in data["images"]:
            assert (chunk_dir / image["file_name"]).exists()
        image_ids = {image["id"] for image in data["images"]}
        assert all(ann["image_id"] in image_ids for ann in data["annotations"])
    assert total_images == 7


def test_split_coco_for_task_upload_single_chunk_when_already_under_max(tmp_path: Path) -> None:
    coco_dir = tmp_path / "coco"
    _write_split_source(coco_dir, n_images=2)

    chunks = dataset.split_coco_for_task_upload(coco_dir, tmp_path / "out", max_images=300)

    assert len(chunks) == 1
    data = json.loads((chunks[0] / "instances.json").read_text(encoding="utf-8"))
    assert len(data["images"]) == 2
    assert len(data["annotations"]) == 2


def test_split_coco_for_task_upload_preserves_categories(tmp_path: Path) -> None:
    coco_dir = tmp_path / "coco"
    _write_split_source(coco_dir, n_images=4)

    chunks = dataset.split_coco_for_task_upload(coco_dir, tmp_path / "out", max_images=2)

    for chunk_dir in chunks:
        data = json.loads((chunk_dir / "instances.json").read_text(encoding="utf-8"))
        assert data["categories"] == _default_categories()
