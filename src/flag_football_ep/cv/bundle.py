"""Package a frozen detector + frozen eval split into a dev/test/transfer deliverable.

Sibling of `cv.dataset`'s content-hashing and `cv.frames`'s atomic-manifest-write
conventions applied to a new artifact kind: a self-contained, checksummed archive a
hackathon participant can pull without touching the DVC-tracked training set. Depends
on `cv.freeze.read_freeze_pin`/the freeze-pin file -- never `cv.registry.resolve_champion`
directly, so a bundle always names the frozen (not the rolling-champion) detector.

`build_bundle`/`bundle_manifest` were implemented by plan 02.2-10 for the "dev" kind.
Plan 02.2-12 (this plan) fills the two remaining content tables:

- "test": the 18 `private_test = true` drone clips (D-07 fallback -- no second drone
  game had materialised by build time). Clips/detections/baseline tracks/overlays/
  homography only -- never `continuity_review.csv`/`flag_pull_events.csv`/
  `gt_positions.csv`. The withheld continuity/flag-pull rows for exactly these clips
  are written to a local, gitignored vault (`data/private/test-labels/`) instead, for
  post-event grading -- outside every bundle, outside git.
- "transfer": the sideline (GoPro) and broadcast (TV) material, each with detections
  produced under its own per-domain inference settings (`docs/dataset-plan.md ## 4`).

The "test"-kind label-leak guard (T-2.2-28) was implemented by plan 02.2-10 and is
exercised here by a real build for the first time: a declarative, name-based
pre-assembly check (`_assert_no_test_kind_label_leak`) plus a column-level,
post-assembly guard over the actual staged files (`_assert_no_label_leak_in_tree`) --
a name-only check alone is too weak, since a renamed label file would slip through.
`deliver_bundle` (OTC OBS upload) remains a stub -- implemented by plan 02.2-14.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from flag_football_ep.cv import CvError, frames, schema
from flag_football_ep.cv.freeze import FreezeError, FreezePin, read_freeze_pin

if TYPE_CHECKING:
    from flag_football_ep.config import Config

# The three deliverable kinds this phase produces (D-09): a small development sample,
# a held-out evaluation subset, and a transfer-only subset for domains ratified as
# transfer-only under D-11 (e.g. broadcast/TV if it fails the per-domain mAP ablation).
BUNDLE_KINDS: tuple[str, ...] = ("dev", "test", "transfer")


class BundleError(CvError, RuntimeError):
    """Raised when a bundle cannot be assembled: a missing frozen-detector pin, a
    missing eval-clip manifest, an unknown `kind`, or an archive write failure.
    """


@dataclass(frozen=True)
class BundleResult:
    """The output of a `build_bundle` run: where the archive and its manifest were
    written, the archive's reproducible content hash, and how many files it contains.
    """

    archive_path: Path
    manifest_path: Path
    content_sha256: str
    n_files: int


# --- Eval-clip split (D-07/D-13): the `role = pool` filter every dev-facing bundle
# must apply. Mirrors `continuity.py`'s own precedent (a separate private schema
# constant rather than importing another module's private attribute across module
# boundaries) -- `frames.py`'s own `_EVAL_SPLIT_SCHEMA` stays private to that module.
_EVAL_SPLIT_CSV_SCHEMA: dict[str, pl.DataType] = {
    "domain": pl.Utf8,
    "session_id": pl.Utf8,
    "clip_number": pl.Int64,
    "stratum_id": pl.Utf8,
    "role": pl.Utf8,
    "private_test": pl.Boolean,
    "frozen_at": pl.Utf8,
    "seed": pl.Int64,
}

# Mirrors `tests/test_cv_benchmark_labels.py`'s `_REVIEW_SCHEMA`/`_FLAG_PULL_SCHEMA`
# (the authoritative dtypes for these two hand-labelled CSVs).
_CONTINUITY_REVIEW_SCHEMA: dict[str, pl.DataType] = {
    "clip_number": pl.Int32,
    "n_tracks": pl.Int32,
    "longest_track_frac": pl.Float64,
    "n_fragments": pl.Int32,
    "auto_flag": pl.Utf8,
    "verdict": pl.Utf8,
    "id_switches": pl.Int32,
    "reviewer_note": pl.Utf8,
}

_FLAG_PULL_EVENTS_SCHEMA: dict[str, pl.DataType] = {
    "clip_number": pl.Int32,
    "outcome": pl.Utf8,
    "pull_time_s": pl.Float64,
    "carrier_track_id": pl.Int32,
    "puller_track_id": pl.Utf8,
    "notes": pl.Utf8,
}

# Mirrors `export.py::_CROPS_INDEX_COLUMNS` (kept as a local constant rather than an
# import -- that tuple is private to `export.py`).
_CROPS_INDEX_SCHEMA: dict[str, pl.DataType] = {
    "session_id": pl.Utf8,
    "clip_number": pl.Int64,
    "track_id": pl.Int64,
    "frame_index": pl.Int64,
    "team_id": pl.Int64,
    "class_name": pl.Utf8,
    "file": pl.Utf8,
}

# The withheld-set label files a "test"-kind bundle must never contain (T-2.2-28):
# exactly the two files plan 02.2-03's labelling work produced. `gt_positions.csv` is
# legitimately shipped in the dev set (participants need it to self-evaluate position
# accuracy) but is NOT part of the "test" content table at all (plan 02.2-12's own
# withholding decision, see `_test_items`): ground-truth foot positions for the
# withheld clips are exactly what a position-accuracy score would need, so they never
# ship in any bundle, dev-set-style filename or not.
_LABEL_BEARING_BASENAMES = frozenset({"continuity_review.csv", "flag_pull_events.csv"})

# Column-level leak guard (T-2.2-28): a "test" bundle's staged tree must never contain
# a file carrying any of these columns, regardless of the file's name -- a name-only
# check alone is too weak, since a label file renamed to e.g. `notes.csv` would slip
# through a basename-only check. Continuity-review columns (`verdict`, `id_switches`,
# `reviewer_note`) and flag-pull columns (`pull_time_s`, `carrier_track_id`,
# `puller_track_id`) mirror `_CONTINUITY_REVIEW_SCHEMA`/`_FLAG_PULL_EVENTS_SCHEMA`
# above exactly (`clip_number`/`n_tracks`/etc. are deliberately excluded -- they are
# generic enough to appear in non-label files too and would make the guard useless).
_LEAK_COLUMN_NAMES = frozenset(
    {
        "verdict",
        "id_switches",
        "reviewer_note",
        "pull_time_s",
        "carrier_track_id",
        "puller_track_id",
    }
)

# The local, gitignored vault a "test" build writes its withheld continuity/flag-pull
# rows to -- outside every bundle tree, outside git (T-2.2-28).
_TEST_LABEL_VAULT_DIRNAME = "test-labels"

# The transfer-set domains (D-11/interfaces): sideline (GoPro) and broadcast (TV)
# material, each its own session. Hardcoded here (not config-driven) the same way
# `frames.py::_PRIVATE_TEST_DOMAIN` hardcodes "drone" -- these are fixed facts about
# which two capture sessions this phase's transfer material comes from, not a runtime
# choice a caller should be able to vary.
_TRANSFER_DOMAINS: tuple[tuple[str, str], ...] = (
    ("sideline", "2026-08-14_WC-GER-vs-MEX-GOPRO"),
    ("broadcast", "2026-08-14_WC-USA-vs-AUS-TV"),
)


@dataclass(frozen=True)
class _BundleItem:
    """One declared piece of bundle content: where it lands inside the `<kind>-set`
    staging tree, which real file/directory it is derived from (used for the
    required-input existence check and the test-kind label-leak guard), and the
    writer that actually produces `dest` (a filtered CSV/Parquet rewrite, a crop-tree
    copy, or a plain file copy).
    """

    dest: str
    source: Path
    category: str  # "label" | "data"
    writer: Callable[[Path], None]


def _assert_exists(path: Path, what: str) -> None:
    if not path.exists():
        raise BundleError(f"{what} not found: {path}")


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp_path.write_bytes(data)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _copy_writer(src: Path) -> Callable[[Path], None]:
    def _write(dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)

    return _write


def _filtered_csv_writer(
    src: Path, pool_clips: list[int], schema_overrides: dict[str, pl.DataType]
) -> Callable[[Path], None]:
    def _write(dest: Path) -> None:
        df = pl.read_csv(src, schema_overrides=schema_overrides)
        filtered = df.filter(pl.col("clip_number").is_in(pool_clips))
        _atomic_write_bytes(dest, filtered.write_csv().encode("utf-8"))

    return _write


def _filtered_detections_writer(src: Path, pool_clips: list[int]) -> Callable[[Path], None]:
    def _write(dest: Path) -> None:
        df = pl.read_parquet(src)
        filtered = df.filter(pl.col("clip_number").is_in(pool_clips))
        schema.write_detections_parquet(filtered, dest)

    return _write


def _filtered_tracks_writer(src: Path, pool_clips: list[int]) -> Callable[[Path], None]:
    def _write(dest: Path) -> None:
        df = pl.read_parquet(src)
        filtered = df.filter(pl.col("clip_number").is_in(pool_clips))
        schema.write_tracking_parquet(filtered, dest)

    return _write


def _crops_writer(crops_src_dir: Path, pool_clips: list[int]) -> Callable[[Path], None]:
    def _write(dest_dir: Path) -> None:
        index_src = crops_src_dir / "index.csv"
        meta_src = crops_src_dir / "crops_meta.json"
        _assert_exists(index_src, "crops index.csv")
        _assert_exists(meta_src, "crops crops_meta.json")

        df = pl.read_csv(index_src, schema_overrides=_CROPS_INDEX_SCHEMA)
        filtered = df.filter(pl.col("clip_number").is_in(pool_clips))

        dest_dir.mkdir(parents=True, exist_ok=True)
        for row in filtered.iter_rows(named=True):
            src_file = crops_src_dir / row["file"]
            dest_file = dest_dir / row["file"]
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dest_file)

        _atomic_write_bytes(dest_dir / "index.csv", filtered.write_csv().encode("utf-8"))

        meta = json.loads(meta_src.read_text(encoding="utf-8"))
        meta["n_crops"] = filtered.height
        _atomic_write_bytes(
            dest_dir / "crops_meta.json",
            (json.dumps(meta, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        )

    return _write


def _pool_clip_numbers(config: Config, domain: str, session_id: str) -> list[int]:
    """Every clip number registered `role = pool` for `domain`/`session_id` in the
    frozen eval-clip split (D-07/D-13) -- the ONLY clip set any dev-facing bundle may
    ship. Never `role = frozen_eval` (18 of the drone domain's clips carry
    `private_test = true`: D-07's private hackathon test set).
    """
    path = config.paths.reference / "frozen_eval_clips.csv"
    if not path.exists():
        raise BundleError(
            f"frozen eval-clip split not found: {path} -- run `ffep cv eval-split` "
            "first (a dev bundle must never ship the private_test clips, D-07)"
        )
    df = pl.read_csv(path, schema_overrides=_EVAL_SPLIT_CSV_SCHEMA)
    rows = df.filter(
        (pl.col("domain") == domain)
        & (pl.col("session_id") == session_id)
        & (pl.col("role") == "pool")
    )
    if rows.height == 0:
        raise BundleError(
            f"no role=pool clips found for domain={domain!r} session_id={session_id!r} "
            f"in {path}"
        )
    return sorted(rows["clip_number"].to_list())


def _private_test_clip_numbers(config: Config, domain: str, session_id: str) -> list[int]:
    """Every clip number registered `private_test = true` for `domain`/`session_id` in
    the frozen eval-clip split (D-07) -- the withheld private hackathon test set. A
    "test" bundle's clip list is ALWAYS resolved from here, never from a hard-coded
    clip-number list (this plan's own `<behavior>` contract): if a second drone game
    ever replaces the withheld-clip fallback, re-freezing the eval split for that
    session is the only change a real test-set build needs -- this module does not
    have to change.
    """
    path = config.paths.reference / "frozen_eval_clips.csv"
    if not path.exists():
        raise BundleError(
            f"frozen eval-clip split not found: {path} -- run `ffep cv eval-split` first"
        )
    df = pl.read_csv(path, schema_overrides=_EVAL_SPLIT_CSV_SCHEMA)
    rows = df.filter(
        (pl.col("domain") == domain)
        & (pl.col("session_id") == session_id)
        & (pl.col("private_test"))
    )
    if rows.height == 0:
        raise BundleError(
            f"no private_test=true clips found for domain={domain!r} session_id={session_id!r} "
            f"in {path}"
        )
    return sorted(rows["clip_number"].to_list())


def _dev_items(config: Config, session_id: str, pool_clips: list[int]) -> list[_BundleItem]:
    items: list[_BundleItem] = []

    all_clips = frames.clip_paths(config, session_id, domain="drone")
    pool_set = set(pool_clips)
    pool_video_paths = [p for p in all_clips if frames.clip_number(p) in pool_set]
    if not pool_video_paths:
        raise BundleError(
            f"none of the role=pool clip numbers {pool_clips} matched a registered "
            f"video file for session_id {session_id!r}"
        )
    for video_path in pool_video_paths:
        n = frames.clip_number(video_path)
        items.append(
            _BundleItem(
                dest=f"data/clips/clip_{n:03d}.mp4",
                source=video_path,
                category="data",
                writer=_copy_writer(video_path),
            )
        )

    detections_src = config.paths.labels / session_id / "bundle-inputs" / "detections.parquet"
    _assert_exists(detections_src, "bundle-inputs detections Parquet")
    items.append(
        _BundleItem(
            dest="data/detections.parquet",
            source=detections_src,
            category="data",
            writer=_filtered_detections_writer(detections_src, pool_clips),
        )
    )

    tracks_src = config.paths.tracking / f"{session_id}_tracks.parquet"
    _assert_exists(tracks_src, "baseline tracking Parquet")
    items.append(
        _BundleItem(
            dest="data/tracks.parquet",
            source=tracks_src,
            category="data",
            writer=_filtered_tracks_writer(tracks_src, pool_clips),
        )
    )

    crops_src_dir = config.paths.labels / session_id / "bundle-inputs" / "crops"
    _assert_exists(crops_src_dir, "bundle-inputs crops directory")
    items.append(
        _BundleItem(
            dest="data/crops",
            source=crops_src_dir,
            category="data",
            writer=_crops_writer(crops_src_dir, pool_clips),
        )
    )

    overlays_src_dir = config.paths.labels / session_id / "overlays"
    _assert_exists(overlays_src_dir, "overlay videos directory")
    for n in pool_clips:
        overlay_src = overlays_src_dir / f"clip_{n:03d}.mp4"
        _assert_exists(overlay_src, f"overlay video for clip {n}")
        items.append(
            _BundleItem(
                dest=f"data/overlays/clip_{n:03d}.mp4",
                source=overlay_src,
                category="data",
                writer=_copy_writer(overlay_src),
            )
        )

    continuity_src = config.paths.reference / "continuity_review.csv"
    _assert_exists(continuity_src, "continuity review CSV")
    items.append(
        _BundleItem(
            dest="data/continuity_review.csv",
            source=continuity_src,
            category="label",
            writer=_filtered_csv_writer(continuity_src, pool_clips, _CONTINUITY_REVIEW_SCHEMA),
        )
    )

    flag_pull_src = config.paths.reference / "flag_pull_events.csv"
    _assert_exists(flag_pull_src, "flag-pull events CSV")
    items.append(
        _BundleItem(
            dest="data/flag_pull_events.csv",
            source=flag_pull_src,
            category="label",
            writer=_filtered_csv_writer(flag_pull_src, pool_clips, _FLAG_PULL_EVENTS_SCHEMA),
        )
    )

    gt_positions_src = config.paths.reference / "gt_positions.csv"
    _assert_exists(gt_positions_src, "ground-truth positions CSV")
    items.append(
        _BundleItem(
            dest="data/gt_positions.csv",
            source=gt_positions_src,
            category="data",
            writer=_filtered_csv_writer(gt_positions_src, pool_clips, {}),
        )
    )

    homography_src = config.paths.reference / "homography_calibration.csv"
    _assert_exists(homography_src, "homography calibration CSV")
    items.append(
        _BundleItem(
            dest="data/homography_calibration.csv",
            source=homography_src,
            category="data",
            writer=_copy_writer(homography_src),
        )
    )

    return items


def _test_items(config: Config, session_id: str, private_clips: list[int]) -> list[_BundleItem]:
    """The "test" content table (T-2.2-28/D-07): clips, detections, baseline tracks,
    overlays and homography calibration for the withheld `private_clips` ONLY --
    deliberately narrower than `_dev_items`. Never `continuity_review.csv`, never
    `flag_pull_events.csv` (the withheld labels this build vaults instead, see
    `_vault_withheld_labels`), and never `gt_positions.csv` (ground-truth foot
    positions for these clips are exactly what a position-accuracy score would need --
    they stay withheld everywhere, unlike the dev set's pool-only `gt_positions.csv`).
    """
    items: list[_BundleItem] = []

    all_clips = frames.clip_paths(config, session_id, domain="drone")
    private_set = set(private_clips)
    private_video_paths = [p for p in all_clips if frames.clip_number(p) in private_set]
    if not private_video_paths:
        raise BundleError(
            f"none of the private_test clip numbers {private_clips} matched a registered "
            f"video file for session_id {session_id!r}"
        )
    for video_path in private_video_paths:
        n = frames.clip_number(video_path)
        items.append(
            _BundleItem(
                dest=f"data/clips/clip_{n:03d}.mp4",
                source=video_path,
                category="data",
                writer=_copy_writer(video_path),
            )
        )

    detections_src = config.paths.labels / session_id / "bundle-inputs" / "detections.parquet"
    _assert_exists(detections_src, "bundle-inputs detections Parquet")
    items.append(
        _BundleItem(
            dest="data/detections.parquet",
            source=detections_src,
            category="data",
            writer=_filtered_detections_writer(detections_src, private_clips),
        )
    )

    tracks_src = config.paths.tracking / f"{session_id}_tracks.parquet"
    _assert_exists(tracks_src, "baseline tracking Parquet")
    items.append(
        _BundleItem(
            dest="data/tracks.parquet",
            source=tracks_src,
            category="data",
            writer=_filtered_tracks_writer(tracks_src, private_clips),
        )
    )

    overlays_src_dir = config.paths.labels / session_id / "overlays"
    _assert_exists(overlays_src_dir, "overlay videos directory")
    for n in private_clips:
        overlay_src = overlays_src_dir / f"clip_{n:03d}.mp4"
        _assert_exists(overlay_src, f"overlay video for clip {n}")
        items.append(
            _BundleItem(
                dest=f"data/overlays/clip_{n:03d}.mp4",
                source=overlay_src,
                category="data",
                writer=_copy_writer(overlay_src),
            )
        )

    homography_src = config.paths.reference / "homography_calibration.csv"
    _assert_exists(homography_src, "homography calibration CSV")
    items.append(
        _BundleItem(
            dest="data/homography_calibration.csv",
            source=homography_src,
            category="data",
            writer=_copy_writer(homography_src),
        )
    )

    return items


def _transfer_items(config: Config) -> list[_BundleItem]:
    """The "transfer" content table (D-11/interfaces): every clip and its per-domain
    detections Parquet for the sideline (GoPro) and broadcast (TV) material -- no
    tracks, no overlays, no labels. `docs/hackathon-challenge-reid.md`'s own
    Benchmark-Design table promises only "Detektionen" for this set; continuity
    judgments on a transfer sample are explicitly "optional, falls Zeit" future work,
    not part of this bundle. Each domain's detections come from a detector run under
    that domain's own per-domain inference settings (`docs/dataset-plan.md ## 4`,
    produced by `ffep cv detections --domain <domain>`), pinned to the same frozen
    detector as dev/test (T-2.2-24, verified by `build_bundle`'s own pin re-read).
    """
    items: list[_BundleItem] = []
    for domain, session_id in _TRANSFER_DOMAINS:
        try:
            clip_video_paths = frames.clip_paths(config, session_id, domain=domain)
        except frames.ClipNotFound as exc:
            raise BundleError(
                f"transfer domain {domain!r} (session_id={session_id!r}) has no "
                f"registered clips: {exc}"
            ) from exc
        if not clip_video_paths:
            raise BundleError(f"no {domain} clips found for session_id {session_id!r}")
        for video_path in clip_video_paths:
            n = frames.clip_number(video_path)
            items.append(
                _BundleItem(
                    dest=f"data/{domain}/clips/clip_{n:03d}.mp4",
                    source=video_path,
                    category="data",
                    writer=_copy_writer(video_path),
                )
            )

        detections_src = config.paths.labels / session_id / "bundle-inputs" / "detections.parquet"
        _assert_exists(
            detections_src, f"{domain} bundle-inputs detections Parquet ({session_id})"
        )
        domain_clip_numbers = sorted(frames.clip_number(p) for p in clip_video_paths)
        items.append(
            _BundleItem(
                dest=f"data/{domain}/detections.parquet",
                source=detections_src,
                category="data",
                writer=_filtered_detections_writer(detections_src, domain_clip_numbers),
            )
        )

    return items


def _vault_withheld_labels(config: Config, private_clips: list[int]) -> Path:
    """Write the withheld continuity/flag-pull rows for `private_clips` to the local,
    gitignored label vault (`data/private/test-labels/`) -- OUTSIDE every bundle tree,
    so a "test" build's own withheld labels survive for post-event grading without
    ever entering the archive `_test_items` assembles. Atomic write, same discipline
    as the rest of this module (`_atomic_write_bytes`).
    """
    vault_dir = config.paths.data_root / "private" / _TEST_LABEL_VAULT_DIRNAME

    continuity_src = config.paths.reference / "continuity_review.csv"
    _assert_exists(continuity_src, "continuity review CSV")
    continuity_df = pl.read_csv(continuity_src, schema_overrides=_CONTINUITY_REVIEW_SCHEMA)
    continuity_withheld = continuity_df.filter(pl.col("clip_number").is_in(private_clips))
    _atomic_write_bytes(
        vault_dir / "continuity_review.csv", continuity_withheld.write_csv().encode("utf-8")
    )

    flag_pull_src = config.paths.reference / "flag_pull_events.csv"
    _assert_exists(flag_pull_src, "flag-pull events CSV")
    flag_pull_df = pl.read_csv(flag_pull_src, schema_overrides=_FLAG_PULL_EVENTS_SCHEMA)
    flag_pull_withheld = flag_pull_df.filter(pl.col("clip_number").is_in(private_clips))
    _atomic_write_bytes(
        vault_dir / "flag_pull_events.csv", flag_pull_withheld.write_csv().encode("utf-8")
    )

    return vault_dir


def _assert_no_label_leak_in_tree(staging_root: Path) -> None:
    """Post-assembly, column-level leak guard for a "test" staging tree (T-2.2-28):
    walk every file actually written to disk and fail on either a known label-table
    basename OR any CSV/Parquet column that carries a label/verdict-shaped name
    (`_LEAK_COLUMN_NAMES`). A name-only check alone is too weak -- a label file
    renamed to e.g. `notes.csv` would slip past a basename-only check -- so this reads
    each file's own header/schema off disk rather than trusting its name, in addition
    to (not instead of) the basename check `_assert_no_test_kind_label_leak` already
    runs pre-assembly against the declared content table.
    """
    for path in sorted(staging_root.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue

        columns: list[str] = []
        if path.suffix == ".csv":
            try:
                with path.open("r", encoding="utf-8", newline="") as handle:
                    columns = next(csv.reader(handle), [])
            except (OSError, UnicodeDecodeError):
                columns = []
        elif path.suffix == ".parquet":
            try:
                columns = list(pl.read_parquet_schema(path).keys())
            except Exception:  # noqa: BLE001 -- unreadable parquet is not this guard's job
                columns = []

        leaking_columns = sorted(_LEAK_COLUMN_NAMES.intersection(columns))
        basename_leak = path.name in _LABEL_BEARING_BASENAMES
        if leaking_columns or basename_leak:
            reasons = []
            if basename_leak:
                reasons.append("basename matches a withheld label table")
            if leaking_columns:
                reasons.append(f"columns {leaking_columns} are label-bearing")
            raise BundleError(
                f"refusing to ship {path.relative_to(staging_root)} in a 'test' bundle: "
                f"{'; '.join(reasons)} (T-2.2-28)"
            )


def _assert_no_test_kind_label_leak(kind: str, items: list[_BundleItem]) -> None:
    if kind != "test":
        return
    for item in items:
        if item.source.name in _LABEL_BEARING_BASENAMES:
            raise BundleError(
                f"refusing to build a 'test' bundle containing label-bearing file "
                f"{item.source} -- test-kind bundles must never ship withheld labels "
                "(T-2.2-28)"
            )


def _hash_tree(root: Path, *, exclude: frozenset[str]) -> tuple[str, list[tuple[str, str, int]]]:
    """Sha256 over sorted `(relative_path, sha256(file_bytes))` pairs for every file
    under `root`, plus the same per-file `(path, sha256, bytes)` list -- the same
    hashing discipline `dataset.dataset_hash` uses for a COCO package (sorted
    relative-path/content-hash pairs, hashed together), generalised from "images plus
    one instances.json" to "every file in the tree" since a hackathon bundle ships
    Parquet/CSV/MP4/JPEG/JSON/Markdown, not only images.
    """
    entries: list[tuple[str, str, int]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if rel in exclude:
            continue
        data = path.read_bytes()
        entries.append((rel, hashlib.sha256(data).hexdigest(), len(data)))
    entries.sort(key=lambda entry: entry[0])

    hasher = hashlib.sha256()
    for rel, file_sha256, _bytes in entries:
        hasher.update(rel.encode("utf-8"))
        hasher.update(b"\x00")
        hasher.update(file_sha256.encode("utf-8"))
        hasher.update(b"\n")
    return hasher.hexdigest(), entries


def _pool_baseline_stats(continuity_review_path: Path) -> dict:
    df = pl.read_csv(continuity_review_path, schema_overrides=_CONTINUITY_REVIEW_SCHEMA)
    n = df.height
    n_pass = int((df["verdict"] == "pass").sum())
    return {"n": n, "n_pass": n_pass, "pass_rate": (n_pass / n) if n else None}


def _render_readme(
    kind: str, pin: FreezePin, session_id: str, pool_clips: list[int], baseline: dict
) -> str:
    n_clips = len(pool_clips)
    pass_rate_pct = f"{baseline['pass_rate'] * 100:.2f}" if baseline["pass_rate"] is not None else "?"
    detection_cols = ", ".join(schema.DETECTION_COLUMNS)
    tracking_cols = ", ".join(schema.TRACKING_COLUMNS)
    crops_index_cols = ", ".join(_CROPS_INDEX_SCHEMA)

    return f"""# Hackathon-{kind}-Set — {session_id}

**Status: automatisch erzeugt von `ffep cv bundle --kind {kind}`. Eingefrorener Detektor-Lauf:
`{pin.run_id}` (Dataset-Hash `{pin.dataset_hash}`, eingefroren am {pin.frozen_at}).**

## Zweck

Dieses Paket ist das öffentliche Dev-Set der Hackathon-Challenge „Wer ist wer nach der
Verdeckung?" (`docs/hackathon-challenge-reid.md`): {n_clips} Spielzug-Clips des Pilotspiels
GER vs. Panama Rojo (16.05.2026), auf die `role = pool` in
`data/reference/frozen_eval_clips.csv` eingeschränkt. Die restlichen 18 Drohnen-Clips dieses
Spiels sind das private Testset (D-07) und erscheinen in keinem öffentlichen Bundle.

## Verzeichnisstruktur

```
data/
  clips/clip_NNN.mp4          Rohes Drohnenmaterial, {n_clips} Clips (nur role=pool)
  overlays/clip_NNN.mp4       Boxen + Track-Nummern zur Sichtprüfung
  detections.parquet          Pro-Frame-Detektionen des eingefrorenen Detektors
  tracks.parquet               Baseline-Tracks (BoT-SORT), Team-Zuordnung, Feldkoordinaten
  crops/                       Oberkörper-Crops je Track (Trainingsmaterial für Erscheinungsmodelle)
    index.csv                  eine Zeile je Crop-Datei
    crops_meta.json             Cap/Zähler/Detektor-Run-Provenienz
  continuity_review.csv        Human-Urteile pass/fail je Clip (Kern-Metrik-Referenz)
  flag_pull_events.csv         Flag-Pull-Ereignisse je Clip (Bonus-Metrik-Referenz)
  gt_positions.csv              Hand-markierte Fußpositionen (Positions-Genauigkeit)
  homography_calibration.csv    Landmarken je Hover-Position (Pixel -> Yards)
manifest.json                  Datei-für-Datei-Hashes + Gesamt-Content-Hash
README.md                      diese Datei
```

## Schemas

- **`detections.parquet`** (Spalten): {detection_cols}
- **`tracks.parquet`** (Spalten): {tracking_cols}
- **`crops/index.csv`** (Spalten): {crops_index_cols}
- **`continuity_review.csv`**/**`flag_pull_events.csv`**: siehe
  `docs/hackathon-benchmark-labels.md` für das vollständige Vokabular.

## Scoring

```
uv run python scripts/hackathon/score_tracks.py \\
  --tracks <eure_tracks.csv> \\
  --review data/continuity_review.csv \\
  --out report.json
```

Bonus (Flag-Pull, optional): `--flag-pulls <eure_pull_events.csv>` zusätzlich angeben --
die Referenzdatei `data/flag_pull_events.csv` muss im selben Verzeichnis wie `--review`
liegen.

## Baseline-Zahlen (dieses Bundle, nur die {n_clips} Pool-Clips)

**Kontinuität (BoT-SORT-Baseline, menschlich bewertet): {baseline["n_pass"]}/{baseline["n"]} =
{pass_rate_pct} %.** Reproduzierbar über `data/tracks.parquet` +
`data/continuity_review.csv` und das obige Scoring-Kommando. Diese Zahl ist NICHT die
Vollspiel-Zahl aus `docs/hackathon-challenge-reid.md` (dort über alle 61 Clips inkl. der
privaten Testset-Clips) -- sie ist die auf dieses Bundle beschränkte, mit den
mitgelieferten Daten reproduzierbare Zahl.

## Nutzungsregeln

- Keine Cloud-Uploads dieses Materials. Arbeit ausschließlich auf bereitgestellter
  Infrastruktur/Laptops.
- Zweckgebundene, interne Nutzung im Rahmen der Hackathon-Challenge (Verbandsfreigabe
  vom 2026-08-31, siehe `docs/capture-legal.md`).
- Löschung/Rückgabe nach dem Event.
- Ausschließlich mit `scripts/hackathon/score_tracks.py` bewerten, damit alle Teams
  dieselbe Zahl messen.

## Bekannte Lücken

Radar-Renderings (Top-Down-Feldansicht) sind noch nicht Teil dieses Bundles -- nur die
Overlay-Videos (Boxen + Track-Nummern über dem Rohmaterial). Grund: es existiert noch
kein Pro-Clip-Radar-Rendering-Lauf für die {n_clips} Pool-Clips; `cv/radar.py` kann das
technisch, aber ein solcher Lauf ist nicht Teil dieses Plans. Nachgereicht, sobald ein
späterer Plan die Pro-Clip-Radar-Renderings erzeugt.
"""


def _render_test_readme(pin: FreezePin, session_id: str, private_clips: list[int]) -> str:
    n_clips = len(private_clips)
    detection_cols = ", ".join(schema.DETECTION_COLUMNS)
    tracking_cols = ", ".join(schema.TRACKING_COLUMNS)

    return f"""# Hackathon-test-Set — {session_id}

**Status: automatisch erzeugt von `ffep cv bundle --kind test`. Eingefrorener Detektor-Lauf:
`{pin.run_id}` (Dataset-Hash `{pin.dataset_hash}`, eingefroren am {pin.frozen_at}).**

## Zweck

Das private Testset der Hackathon-Challenge „Wer ist wer nach der Verdeckung?"
(`docs/hackathon-challenge-reid.md`): {n_clips} zurückgehaltene Drohnen-Clips des
Pilotspiels GER vs. Panama Rojo (16.05.2026), `private_test = true` in
`data/reference/frozen_eval_clips.csv` (D-07-Fallback -- kein zweites Drohnenspiel lag
zum Build-Zeitpunkt vor, siehe `docs/hackathon-bundles.md`). Dient ausschließlich der
Endwertung nach dem Event -- niemals zur Entwicklung oder zum Tuning verwenden.

## Verzeichnisstruktur

```
data/
  clips/clip_NNN.mp4          Rohes Drohnenmaterial, {n_clips} Clips
  overlays/clip_NNN.mp4       Boxen + Track-Nummern zur Sichtprüfung
  detections.parquet          Pro-Frame-Detektionen des eingefrorenen Detektors
  tracks.parquet               Baseline-Tracks (BoT-SORT), Team-Zuordnung, Feldkoordinaten
  homography_calibration.csv   Landmarken je Hover-Position (Pixel -> Yards)
manifest.json                  Datei-für-Datei-Hashes + Gesamt-Content-Hash
README.md                      diese Datei
```

## Was bewusst fehlt

Keine `continuity_review.csv`, keine `flag_pull_events.csv`, keine `gt_positions.csv`.
Diese drei Referenzen bleiben zurückgehalten (T-2.2-28): die Kontinuitäts- und
Flag-Pull-Urteile für genau diese {n_clips} Clips liegen ausschließlich im lokalen,
nicht versionierten Label-Tresor (`data/private/test-labels/`) für die Endwertung nach
dem Event. Kein öffentliches Bundle enthält sie, und keine Ground-Truth-Fußpositionen
für diese Clips existieren in irgendeinem Bundle.

## Schemas

- **`detections.parquet`** (Spalten): {detection_cols}
- **`tracks.parquet`** (Spalten): {tracking_cols}

## Nutzungsregeln

- Nur für die Endwertung -- nicht zum Tuning verwenden.
- Keine Cloud-Uploads dieses Materials. Arbeit ausschließlich auf bereitgestellter
  Infrastruktur/Laptops.
- Zweckgebundene, interne Nutzung im Rahmen der Hackathon-Challenge (Verbandsfreigabe
  vom 2026-08-31, siehe `docs/capture-legal.md`).
- Löschung/Rückgabe nach dem Event.
- Bewertung ausschließlich über `scripts/hackathon/score_tracks.py`, gegen die
  vertraulich gehaltenen Urteile im Label-Tresor -- niemals gegen ein team-eigenes
  Urteil über diese Clips.
"""


def _render_transfer_readme(config: Config, pin: FreezePin) -> str:
    detection_cols = ", ".join(schema.DETECTION_COLUMNS)
    lines = []
    total = 0
    for domain, session_id in _TRANSFER_DOMAINS:
        n = len(frames.clip_paths(config, session_id, domain=domain))
        total += n
        lines.append(f"- **{domain}** (`{session_id}`): {n} Clips")
    domain_list = "\n".join(lines)

    return f"""# Hackathon-transfer-Set

**Status: automatisch erzeugt von `ffep cv bundle --kind transfer`. Eingefrorener
Detektor-Lauf: `{pin.run_id}` (Dataset-Hash `{pin.dataset_hash}`, eingefroren am
{pin.frozen_at}).**

## Zweck

Die Transfer-Wertung der Hackathon-Challenge (`docs/hackathon-challenge-reid.md`):
{total} Clips aus zwei Domänen, mit denen gemessen wird, wie viel einer Verbesserung
gegenüber der Drohnen-Baseline den Kamerawechsel übersteht.

{domain_list}

## Domänen-Details (gemessen, aus `docs/material-sighting.md`/`docs/dataset-plan.md`)

| Domäne | p50 (px) | p10 (px) | Stufe | `resolution` | `sahi` |
|---|---:|---:|---|---:|---|
| Seitenkamera (`sideline`, GoPro, WM GER-MEX) | 27,0 | 16,5 | Brauchbar | 896 | false |
| Broadcast (`broadcast`, TV, WM USA-AUS) | 23,0 | 14,0 | Brauchbar | 896 | false |

Beide Domänen landen im selben 20-40-px-Band wie die Piloten-Drohnensession (p50 =
30,0 px) und teilen sich dieselbe `resolution`/`sahi`-Einstellung -- eine gemessene
Koinzidenz, keine Vereinfachung (`docs/dataset-plan.md ## 4`).

## Verzeichnisstruktur

```
data/
  sideline/clips/clip_NNN.mp4       GoPro-Seitenlinien-Rohmaterial (WM GER-MEX)
  sideline/detections.parquet        Pro-Frame-Detektionen, sideline-Einstellungen
  broadcast/clips/clip_NNN.mp4      TV-Ausschnitte (WM USA-AUS)
  broadcast/detections.parquet       Pro-Frame-Detektionen, broadcast-Einstellungen
manifest.json                        Datei-für-Datei-Hashes + Gesamt-Content-Hash
README.md                            diese Datei
```

## Was bewusst fehlt

Keine Baseline-Tracks, keine Overlays, keine Kontinuitäts-Urteile -- nur Detektionen.
`docs/hackathon-challenge-reid.md` §Benchmark-Design nennt für dieses Set ausdrücklich
nur "Kontinuitäts-Urteile auf einer Stichprobe (optional, falls Zeit)"; das ist nicht
Teil dieses Bundles.

## Schemas

- **`detections.parquet`** (beide Domänen, identisches Schema): {detection_cols}

## Nutzungsregeln

- Keine Cloud-Uploads dieses Materials. Arbeit ausschließlich auf bereitgestellter
  Infrastruktur/Laptops.
- Zweckgebundene, interne Nutzung im Rahmen der Hackathon-Challenge (Verbandsfreigabe
  vom 2026-08-31, siehe `docs/capture-legal.md`).
- Löschung/Rückgabe nach dem Event.
"""


def _write_archive(staging_root: Path, out_dir: Path, kind: str, content_sha256: str) -> Path:
    today = datetime.now(UTC).date().isoformat()
    short_hash = content_sha256[:12]
    archive_path = out_dir / f"{kind}-set_{today}_{short_hash}.zip"
    tmp_path = archive_path.with_suffix(archive_path.suffix + ".tmp")
    try:
        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(staging_root.rglob("*")):
                if path.is_file():
                    arcname = (Path(f"{kind}-set") / path.relative_to(staging_root)).as_posix()
                    archive.write(path, arcname)
        os.replace(tmp_path, archive_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return archive_path


def build_bundle(config: Config, kind: str, pin: FreezePin, out_dir: Path) -> BundleResult:
    """Assemble a `kind` (`BUNDLE_KINDS`) deliverable archive for the detector pinned
    by `pin`, written under `out_dir`.

    Resolves the detector run id exclusively via `freeze.read_freeze_pin` against
    `config.paths.reference / "hackathon_freeze.json"` -- never trusts `pin.run_id`
    blindly: a freshly re-read pin must exist and must match `pin.run_id`, or this
    raises `BundleError` (T-2.2-24; a stale/synthetic `pin` object must never silently
    drive a real build). The "dev" content table is the pilot session's `role = pool`
    clips only (D-07/D-13); a "test" build ships only the `private_test = true` clips
    (never a hard-coded list) and always refuses any label-bearing file, both by
    basename and by column (T-2.2-28); "transfer" ships the sideline/broadcast
    material with per-domain detections (D-11).
    """
    if kind not in BUNDLE_KINDS:
        raise BundleError(f"unknown bundle kind {kind!r} (expected one of {BUNDLE_KINDS})")

    pin_path = config.paths.reference / "hackathon_freeze.json"
    try:
        on_disk_pin = read_freeze_pin(pin_path)
    except FreezeError as exc:
        raise BundleError(
            f"no freeze pin at {pin_path} -- run `ffep cv freeze` first: {exc}"
        ) from exc
    if on_disk_pin.run_id != pin.run_id:
        raise BundleError(
            f"freeze pin at {pin_path} (run_id={on_disk_pin.run_id!r}) no longer matches "
            f"the pin passed to build_bundle (run_id={pin.run_id!r}) -- re-read the pin "
            "with `freeze.read_freeze_pin` before building"
        )

    session_id = config.cv.pilot_session_id
    private_clips: list[int] = []
    pool_clips: list[int] = []
    if kind == "dev":
        pool_clips = _pool_clip_numbers(config, "drone", session_id)
        items = _dev_items(config, session_id, pool_clips)
    elif kind == "test":
        private_clips = _private_test_clip_numbers(config, "drone", session_id)
        items = _test_items(config, session_id, private_clips)
    else:  # "transfer"
        items = _transfer_items(config)

    _assert_no_test_kind_label_leak(kind, items)

    out_dir = Path(out_dir)
    staging_root = out_dir / f"{kind}-set"
    if staging_root.exists():
        shutil.rmtree(staging_root)
    staging_root.mkdir(parents=True, exist_ok=True)

    for item in items:
        item.writer(staging_root / item.dest)

    if kind == "test":
        _vault_withheld_labels(config, private_clips)
        _assert_no_label_leak_in_tree(staging_root)

    readme_path = staging_root / "README.md"
    if kind == "dev":
        baseline = _pool_baseline_stats(staging_root / "data" / "continuity_review.csv")
        readme_text = _render_readme(kind, pin, session_id, pool_clips, baseline)
    elif kind == "test":
        readme_text = _render_test_readme(pin, session_id, private_clips)
    else:  # "transfer"
        readme_text = _render_transfer_readme(config, pin)
    readme_path.write_text(readme_text, encoding="utf-8")

    content_sha256, file_entries = _hash_tree(staging_root, exclude=frozenset({"manifest.json"}))

    manifest_path = staging_root / "manifest.json"
    manifest_data = {
        "kind": kind,
        "built_at": datetime.now(UTC).isoformat(),
        "detector_run_id": pin.run_id,
        "dataset_hash": pin.dataset_hash,
        "content_sha256": content_sha256,
        "files": [
            {"path": rel, "sha256": file_sha256, "bytes": n_bytes}
            for rel, file_sha256, n_bytes in file_entries
        ],
    }
    _atomic_write_bytes(
        manifest_path, (json.dumps(manifest_data, indent=2, sort_keys=True) + "\n").encode("utf-8")
    )

    archive_path = _write_archive(staging_root, out_dir, kind, content_sha256)

    return BundleResult(
        archive_path=archive_path,
        manifest_path=manifest_path,
        content_sha256=content_sha256,
        n_files=len(file_entries) + 1,
    )


def bundle_manifest(root: Path) -> dict:
    """Read back the manifest of a bundle previously extracted/built at `root`."""
    root = Path(root)
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        raise BundleError(f"bundle manifest not found: {manifest_path}")

    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BundleError(f"bundle manifest at {manifest_path} is not valid JSON: {exc}") from exc

    required_keys = {"kind", "built_at", "detector_run_id", "dataset_hash", "content_sha256", "files"}
    missing_keys = required_keys - data.keys()
    if missing_keys:
        raise BundleError(f"bundle manifest at {manifest_path} is missing key(s) {sorted(missing_keys)}")

    return data


def deliver_bundle(config: Config, archive: Path, remote: str) -> str:
    """Upload `archive` to `remote` (an OTC OBS URI), returning the remote URI it was
    written to. Never echoes a credential value (T-2.2-13).

    Implemented by plan 02.2-14.
    """
    raise NotImplementedError("implemented by plan 02.2-14")
