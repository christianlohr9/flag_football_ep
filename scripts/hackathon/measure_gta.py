#!/usr/bin/env python3
"""GTA (Global Tracklet Association) measurement adapter for the M2-02 "ehrliche
Baseline" plans.

GTA is not a tracker -- it is post-processing over an ALREADY-TRACKED tracklet set
(here: the existing BoT-SORT tracks Parquet). It embeds player crops with an
appearance model (OSNet), then splits tracklets that contain more than one identity
and merges tracklets that belong to the same identity, using the vendored MIT
implementation at `vendor/gta-link` (pinned commit, see `vendor/README.md`). This
script is the adapter: it converts this project's tracks/crops schema into the
vendored `Tracklet` objects, calls the vendored split/merge functions UNMODIFIED, and
converts the result back into `baseline_common.OUTPUT_TRACK_COLUMNS`, scored by the
same unmodified `scripts/hackathon/score_tracks.py` as every other method.

Standalone script, same convention as `baseline_common.py`/`run_baseline_trackers.py`
(not part of the installed `flag_football_ep` package). No local density-based
clustering or split/merge reimplementation exists anywhere in this file -- the
clustering logic is imported from the vendored source, never rebuilt from scratch.

Usage (one invocation measures the requested clips end to end):

    uv run python scripts/hackathon/measure_gta.py \\
        --tracks data/processed/tracking/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE_tracks.parquet \\
        --crops data/labels/2026-05-16_FRIENDLY-GER-vs-PANAMA-ROJO-DRONE/bundle-inputs/crops \\
        --vendor vendor/gta-link \\
        --checkpoint data/processed/baseline-methods/checkpoints/osnet_x1_0_market1501.pth \\
        --checkpoint-sha256 2809d3227f7d078f6045f7feb874a34d0684f0e0057b264b99adccf7d4519154 \\
        --review data/reference/continuity_review.csv \\
        --split data/reference/frozen_eval_clips.csv \\
        --out-dir data/processed/baseline-methods \\
        --results-dir data/reference/baseline-methods

`--dry-run` performs the embed+split+merge steps on a single requested clip and
prints statistics without writing to `data/reference/`.

Known vendored-code caveat (documented, not "fixed" -- the vendored functions are
called exactly as written): `refine_tracklets.merge_tracklets` concatenates
`.times`/`.bboxes`/`.features` on merge but NOT `.scores`. This adapter therefore
never reads `.scores` back off a refined Tracklet for output -- confidence/class_name
are looked up from a side-channel metadata table keyed by the exact
`(frame_index, bbox)` tuple built before any vendored function runs, which IS
preserved value-for-value through both split and merge (never numerically
transformed, only filtered/concatenated as Python list operations).
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import sys
import time
import types
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
import torch
import torchvision.transforms as T
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

sys.path.insert(0, str(Path(__file__).resolve().parent))
import baseline_common as bc  # noqa: E402

_LICENSE_NOTE = "MIT (gta-link) + MIT (deep-person-reid/osnet_x1_0)"

# README-documented defaults (Task 2's read_first instruction: use the SOURCE's
# defaults if they differ). `refine_tracklets.py`'s own `argparse` default for `--eps`
# is 0.7, not the README's 0.6 -- the plan's exact values (README-documented) are used
# here regardless, and this deviation between README and argparse default is recorded
# in the SUMMARY, not silently picked one way or the other.
GTA_PARAMS: dict[str, float | int] = {
    "min_len": 100,
    "eps": 0.6,
    "min_samples": 10,
    "max_k": 3,
    "spatial_factor": 1.0,
    "merge_dist_thres": 0.4,
}

# Preprocessing exactly as `vendor/gta-link/generate_tracklets.py`'s `val_transforms`
# (the vendored repo's OWN embedding entry point) applies it -- not re-derived, read
# directly off that file (Task 2 read_first).
_CROP_RESIZE = (256, 128)  # (H, W), torchvision.transforms.Resize convention
_IMAGENET_MEAN = (0.485, 0.456, 0.406)
_IMAGENET_STD = (0.229, 0.224, 0.225)


def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stub_loguru_if_missing() -> bool:
    """`vendor/gta-link/refine_tracklets.py` does `from loguru import logger` at
    module level, used only for progress logging inside the split/merge functions.
    `loguru` is not part of this project's dependency tree and installing it would
    violate the plan's zero-permanent-install constraint. A no-op stub module is
    registered in `sys.modules` (never on disk, never touching `vendor/gta-link`
    itself) so the import succeeds without installing anything or modifying the
    vendored file. Returns True if a stub was installed (recorded in the SUMMARY).
    """
    try:
        import loguru  # noqa: F401

        return False
    except ImportError:
        pass

    class _NullLogger:
        def __getattr__(self, _name):
            def _noop(*_args, **_kwargs):
                return None

            return _noop

    stub = types.ModuleType("loguru")
    stub.logger = _NullLogger()  # type: ignore[attr-defined]
    sys.modules["loguru"] = stub
    return True


def load_vendored_refine_module(vendor_dir: Path):
    """Import `Tracklet` and `refine_tracklets` from the vendored `gta-link` source
    directly (adds `vendor_dir` to `sys.path`), stubbing `loguru` if absent. Returns
    `(Tracklet_class, refine_tracklets_module)`. Never reimplements anything in
    these modules -- only makes their import succeed in an environment without
    `loguru` installed.
    """
    vendor_dir = Path(vendor_dir)
    vendor_str = str(vendor_dir)
    if vendor_str not in sys.path:
        sys.path.insert(0, vendor_str)
    stubbed = _stub_loguru_if_missing()
    if stubbed:
        print("Hinweis: loguru nicht installiert -- No-Op-Stub fuer den Import von refine_tracklets.py registriert (kein permanenter Install).")

    import Tracklet as tracklet_module  # noqa: E402
    import refine_tracklets as refine_module  # noqa: E402

    return tracklet_module.Tracklet, refine_module


def load_embedder(vendor_dir: Path, checkpoint: Path, checkpoint_sha256: str, device: str):
    """Verify the checkpoint's SHA-256 against `checkpoint_sha256` (SystemExit on
    mismatch -- a silently swapped checkpoint would invalidate the whole
    measurement), then build `osnet_x1_0` and load the checkpoint's state dict.

    Imports the OSNet MODULE directly via `importlib.util`, not the `torchreid`
    package umbrella: `vendor/gta-link/reid/torchreid/__init__.py` imports
    `data, optim, utils, engine, losses, models, metrics`, which transitively require
    `yacs`/`h5py`/`gdown` -- none of which are installed in this project's
    environment (verified: a plain package import fails on those). `osnet.py` itself
    only needs `torch`/`torchvision`, confirmed by reading the file directly.
    """
    checkpoint = Path(checkpoint)
    if not checkpoint.exists():
        print(f"FEHLER: Checkpoint nicht gefunden: {checkpoint}", file=sys.stderr)
        raise SystemExit(1)
    actual_sha256 = _sha256_of(checkpoint)
    if actual_sha256 != checkpoint_sha256:
        print(
            "FEHLER: Checkpoint-SHA-256 stimmt nicht mit dem erwarteten Wert ueberein "
            f"-- erwartet {checkpoint_sha256}, tatsaechlich {actual_sha256}. "
            "Ein stillschweigend ausgetauschter Checkpoint wuerde die Messung entwerten.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    vendor_dir = Path(vendor_dir)
    reid_dir = vendor_dir / "reid"
    import_path_used = "unknown"
    try:
        for p in (str(vendor_dir), str(reid_dir)):
            if p not in sys.path:
                sys.path.insert(0, p)
        from torchreid.models.osnet import osnet_x1_0  # type: ignore  # noqa: PLC0415

        import_path_used = "torchreid.models.osnet (Paket-Import erfolgreich)"
    except ImportError:
        osnet_path = reid_dir / "torchreid" / "models" / "osnet.py"
        spec = importlib.util.spec_from_file_location("gta_osnet", osnet_path)
        if spec is None or spec.loader is None:
            raise SystemExit(f"FEHLER: konnte {osnet_path} nicht als Modul laden.")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        osnet_x1_0 = module.osnet_x1_0  # type: ignore[attr-defined]
        import_path_used = (
            "importlib.util.spec_from_file_location auf reid/torchreid/models/osnet.py "
            "(Paket-Import scheitert an fehlenden yacs/h5py/gdown -- erwartet, siehe RESEARCH.md)"
        )

    model = osnet_x1_0(num_classes=1000, pretrained=False)
    raw_state = torch.load(checkpoint, map_location="cpu", weights_only=False)
    state_dict = raw_state.get("state_dict", raw_state) if isinstance(raw_state, dict) else raw_state
    state_dict = {(k[len("module.") :] if k.startswith("module.") else k): v for k, v in state_dict.items()}
    model_dict = model.state_dict()
    matched = {k: v for k, v in state_dict.items() if k in model_dict and model_dict[k].shape == v.shape}
    discarded = sorted(set(state_dict) - set(matched))
    model_dict.update(matched)
    model.load_state_dict(model_dict)
    print(
        f"OSNet-Checkpoint geladen ueber [{import_path_used}]: "
        f"{len(matched)}/{len(state_dict)} Layer passten; verworfen (i.d.R. Klassifikator, "
        f"andere Klassenzahl): {discarded}"
    )

    resolved_device = device
    if device == "auto":
        resolved_device = "mps" if torch.backends.mps.is_available() else "cpu"
    try:
        model = model.to(resolved_device)
    except Exception as exc:  # pragma: no cover -- depends on local MPS state
        print(f"Hinweis: Geraet {resolved_device} fehlgeschlagen ({exc}), Rueckfall auf CPU.")
        resolved_device = "cpu"
        model = model.to(resolved_device)
    model.eval()
    return model, resolved_device, import_path_used


def _crop_transform() -> T.Compose:
    return T.Compose(
        [
            T.Resize(_CROP_RESIZE),
            T.ToTensor(),
            T.Normalize(mean=_IMAGENET_MEAN, std=_IMAGENET_STD),
        ]
    )


def embed_crops(
    index_df: pl.DataFrame, crops_dir: Path, model, device: str, batch_size: int = 256
) -> tuple[dict[tuple[int, int], dict[int, np.ndarray]], float]:
    """RGB, resize to 256x128, ImageNet normalisation, `torch.no_grad()`, L2-normalise
    the output. Returns `{(clip_number, track_id): {frame_index: np.ndarray}}` and the
    wall-clock seconds spent embedding. Prints embedding time and crops/second.
    """
    crops_dir = Path(crops_dir)
    transform = _crop_transform()
    rows = index_df.to_dicts()
    result: dict[tuple[int, int], dict[int, np.ndarray]] = {}

    start = time.perf_counter()
    n = len(rows)
    for batch_start in range(0, n, batch_size):
        batch = rows[batch_start : batch_start + batch_size]
        tensors = []
        keys: list[tuple[int, int, int]] = []
        for row in batch:
            img_path = crops_dir / row["file"]
            with Image.open(img_path) as im:
                im = im.convert("RGB")
                tensors.append(transform(im))
            keys.append((int(row["clip_number"]), int(row["track_id"]), int(row["frame_index"])))
        if not tensors:
            continue
        batch_tensor = torch.stack(tensors).to(device)
        with torch.no_grad():
            feats = model(batch_tensor)
        feats = torch.nn.functional.normalize(feats, p=2, dim=1)
        feats_np = feats.detach().cpu().numpy()
        for (clip_number, track_id, frame_index), feat in zip(keys, feats_np):
            result.setdefault((clip_number, track_id), {})[frame_index] = feat

    elapsed = time.perf_counter() - start
    crops_per_s = (n / elapsed) if elapsed > 0 else float("nan")
    print(f"Embedding: {n} Crops in {elapsed:.2f}s ({crops_per_s:.1f} Crops/s), Geraet={device}")
    return result, elapsed


def build_tracklets(
    clip_df: pl.DataFrame,
    embeddings: dict[tuple[int, int], dict[int, np.ndarray]],
    clip_number: int,
    tracklet_cls,
):
    """Build one vendored `Tracklet` per original `track_id` that has at least one
    embedded crop, from ONLY that track's embedded frames. Returns:
    - `tracklets`: `{original_track_id: Tracklet}` for embedded tracks
    - `metadata_by_key`: `{(frame_index, l, t, w, h): {session_id, class_name,
      confidence, detector_run_id, original_track_id}}` for every embedded row --
      values are looked up by the EXACT bbox tuple fed into the Tracklet, which is
      never numerically transformed by the vendored split/merge code (only
      filtered/concatenated), so this lookup is robust through both operations.
    - `all_rows_by_track`: every row of the clip, grouped by original `track_id`
      (used to reattach non-embedded frames of partially-embedded tracks and to
      emit fully-unembedded tracks unchanged).
    - `passthrough_track_ids`: original `track_id`s with ZERO embedded crops (all of
      their rows are emitted unchanged, per Task 2's requirement that a track
      without crops must not disappear from the output).
    """
    tracklets: dict[int, object] = {}
    metadata_by_key: dict[tuple[int, float, float, float, float], dict] = {}
    all_rows_by_track: dict[int, list[dict]] = {}
    passthrough_track_ids: set[int] = set()

    for (track_id,), track_df in clip_df.group_by("track_id", maintain_order=True):
        track_id = int(track_id)
        rows = track_df.sort("frame_index").to_dicts()
        all_rows_by_track[track_id] = rows

        emb_for_track = embeddings.get((clip_number, track_id), {})
        embedded_rows = [r for r in rows if int(r["frame_index"]) in emb_for_track]
        if not embedded_rows:
            passthrough_track_ids.add(track_id)
            continue

        frames: list[int] = []
        scores: list[float] = []
        bboxes: list[list[float]] = []
        feats: list[np.ndarray] = []
        for r in embedded_rows:
            frame_index = int(r["frame_index"])
            l = float(r["bbox_x1"])
            t = float(r["bbox_y1"])
            w = float(r["bbox_x2"]) - l
            h = float(r["bbox_y2"]) - t
            frames.append(frame_index)
            scores.append(float(r["confidence"]))
            bboxes.append([l, t, w, h])
            feats.append(emb_for_track[frame_index])
            metadata_by_key[(frame_index, l, t, w, h)] = {
                "session_id": r["session_id"],
                "class_name": r["class_name"],
                "confidence": float(r["confidence"]),
                "detector_run_id": r["detector_run_id"],
                "original_track_id": track_id,
            }
        tracklets[track_id] = tracklet_cls(track_id, frames, scores, bboxes, feats=feats)

    return tracklets, metadata_by_key, all_rows_by_track, passthrough_track_ids


def refine(tracklets: dict, params: dict, refine_module):
    """Call the vendored split and merge functions exactly as written -- never
    reimplemented here. Returns `(refined, n_split_ops, n_merge_ops)`:
    - `n_split_ops`: tracklets ADDED by the split step (new identities carved out)
    - `n_merge_ops`: tracklets REMOVED by the merge step (pairs joined into one)
    """
    if not tracklets:
        return {}, 0, 0

    n_before = len(tracklets)
    split_result = refine_module.split_tracklets(
        dict(tracklets),
        eps=params["eps"],
        max_k=params["max_k"],
        min_samples=params["min_samples"],
        len_thres=params["min_len"],
    )
    n_after_split = len(split_result)
    n_split_ops = n_after_split - n_before

    max_x_range, max_y_range = refine_module.get_spatial_constraints(split_result, params["spatial_factor"])
    dist = refine_module.get_distance_matrix(split_result)
    seq2dist: dict = {}
    merged = refine_module.merge_tracklets(
        split_result,
        seq2dist,
        dist,
        seq_name=None,
        max_x_range=max_x_range,
        max_y_range=max_y_range,
        merge_dist_thres=params["merge_dist_thres"],
    )
    n_after_merge = len(merged)
    n_merge_ops = n_after_split - n_after_merge

    return merged, n_split_ops, n_merge_ops


def apply_refinement(
    clip_number: int,
    refined: dict,
    metadata_by_key: dict,
    all_rows_by_track: dict[int, list[dict]],
    passthrough_track_ids: set[int],
    fps: float,
    tracked_at: str,
) -> list[dict]:
    """Rewrite `track_id` per the split/merge result and re-attach every row that
    never entered GTA's input (non-embedded frames of partially-embedded tracks;
    fully unembedded tracks). Fresh sequential ids are assigned to every final
    entity (refined tracklets AND passthrough tracks) rather than reusing the
    vendored module's own new-id scheme, because that scheme (`max(embedded track
    ids) + 1`) is only guaranteed not to collide with OTHER EMBEDDED track ids, not
    with passthrough track ids that were excluded from its input entirely. Asserts
    no row is lost: every row of every original track in the clip is emitted exactly
    once.
    """
    output_rows: list[dict] = []
    next_id_counter = [0]

    def _alloc_id() -> int:
        nid = next_id_counter[0]
        next_id_counter[0] += 1
        return nid

    def _emit(row_track_id: int, frame_index: int, l: float, t: float, w: float, h: float, meta: dict) -> None:
        output_rows.append(
            {
                "session_id": meta["session_id"],
                "clip_number": clip_number,
                "frame_index": frame_index,
                "track_id": row_track_id,
                "bbox_x1": l,
                "bbox_y1": t,
                "bbox_x2": l + w,
                "bbox_y2": t + h,
                "timestamp_s": frame_index / fps,
                "class_name": meta["class_name"],
                "confidence": meta["confidence"],
                "detector_run_id": meta["detector_run_id"],
                "tracked_at": tracked_at,
            }
        )

    frame_to_final_id_by_orig: dict[int, list[tuple[int, int]]] = {}
    emitted_keys: set[tuple[int, int]] = set()  # (original_track_id, frame_index)

    for tracklet in refined.values():
        final_id = _alloc_id()
        for frame_index_raw, bbox in zip(tracklet.times, tracklet.bboxes):
            frame_index = int(frame_index_raw)
            l, t, w, h = (float(v) for v in bbox[:4])
            key = (frame_index, l, t, w, h)
            meta = metadata_by_key.get(key)
            if meta is None:
                # Should not happen: bboxes are carried through split/merge
                # unmodified. Fail loudly rather than silently drop a row.
                raise ValueError(
                    f"Clip {clip_number}: keine Metadaten fuer refined bbox-Schluessel {key} gefunden."
                )
            _emit(final_id, frame_index, l, t, w, h, meta)
            orig_tid = meta["original_track_id"]
            frame_to_final_id_by_orig.setdefault(orig_tid, []).append((frame_index, final_id))
            emitted_keys.add((orig_tid, frame_index))

    for pairs in frame_to_final_id_by_orig.values():
        pairs.sort()

    # Non-embedded frames of PARTIALLY embedded tracks: nearest-embedded-frame
    # assignment (GTA never saw these frames; they follow whichever refined
    # sub-tracklet of the SAME original track is temporally closest).
    for orig_tid, rows in all_rows_by_track.items():
        if orig_tid in passthrough_track_ids:
            continue
        pairs = frame_to_final_id_by_orig.get(orig_tid)
        if not pairs:
            continue
        for r in rows:
            frame_index = int(r["frame_index"])
            if (orig_tid, frame_index) in emitted_keys:
                continue
            nearest_final_id = min(pairs, key=lambda p: abs(p[0] - frame_index))[1]
            l = float(r["bbox_x1"])
            t = float(r["bbox_y1"])
            w = float(r["bbox_x2"]) - l
            h = float(r["bbox_y2"]) - t
            meta = {
                "session_id": r["session_id"],
                "class_name": r["class_name"],
                "confidence": float(r["confidence"]),
                "detector_run_id": r["detector_run_id"],
            }
            _emit(nearest_final_id, frame_index, l, t, w, h, meta)
            emitted_keys.add((orig_tid, frame_index))

    # Fully unembedded tracks: unchanged, fresh id.
    for orig_tid in passthrough_track_ids:
        final_id = _alloc_id()
        for r in all_rows_by_track[orig_tid]:
            frame_index = int(r["frame_index"])
            l = float(r["bbox_x1"])
            t = float(r["bbox_y1"])
            w = float(r["bbox_x2"]) - l
            h = float(r["bbox_y2"]) - t
            meta = {
                "session_id": r["session_id"],
                "class_name": r["class_name"],
                "confidence": float(r["confidence"]),
                "detector_run_id": r["detector_run_id"],
            }
            _emit(final_id, frame_index, l, t, w, h, meta)

    return output_rows


def process_clip(
    tracks_df: pl.DataFrame,
    index_df: pl.DataFrame,
    embeddings: dict[tuple[int, int], dict[int, np.ndarray]],
    clip_number: int,
    tracklet_cls,
    refine_module,
    tracked_at: str,
) -> tuple[list[dict], dict]:
    """Run build_tracklets -> refine -> apply_refinement for one clip. Returns
    `(output_rows, stats)` where `stats` carries the per-clip counters Task 3 needs
    (crops per track, split/merge op counts, unchanged flag).
    """
    clip_df = tracks_df.filter(pl.col("clip_number") == clip_number)
    n_input_rows = clip_df.height
    fps, notice = bc.clip_fps(clip_df)
    if notice:
        print(f"Clip {clip_number}: {notice}")

    tracklets, metadata_by_key, all_rows_by_track, passthrough_ids = build_tracklets(
        clip_df, embeddings, clip_number, tracklet_cls
    )
    n_embedded_tracks = len(tracklets)
    crops_per_track = [len(t.times) for t in tracklets.values()]

    refined, n_split_ops, n_merge_ops = refine(tracklets, GTA_PARAMS, refine_module)

    output_rows = apply_refinement(
        clip_number, refined, metadata_by_key, all_rows_by_track, passthrough_ids, fps, tracked_at
    )

    if len(output_rows) != n_input_rows:
        raise ValueError(
            f"Clip {clip_number}: Zeilenanzahl veraendert ({n_input_rows} -> {len(output_rows)}) "
            "-- GTA darf keine Zeilen verlieren oder erfinden."
        )

    stats = {
        "clip_number": clip_number,
        "n_embedded_tracks": n_embedded_tracks,
        "n_passthrough_tracks": len(passthrough_ids),
        "crops_per_track": crops_per_track,
        "n_split_ops": n_split_ops,
        "n_merge_ops": n_merge_ops,
        "partition_unchanged": (n_split_ops == 0 and n_merge_ops == 0),
    }
    return output_rows, stats


def _relative(path: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tracks", type=Path, required=True, help="Existing BoT-SORT tracks Parquet (input, read-only).")
    parser.add_argument("--crops", type=Path, required=True, help="bundle-inputs/crops directory (index.csv + clip_NNN/track_YYYY/*.jpg).")
    parser.add_argument("--vendor", type=Path, default=REPO_ROOT / "vendor" / "gta-link")
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--checkpoint-sha256", type=str, required=True)
    parser.add_argument("--review", type=Path, default=REPO_ROOT / "data/reference/continuity_review.csv")
    parser.add_argument("--split", type=Path, default=REPO_ROOT / "data/reference/frozen_eval_clips.csv")
    parser.add_argument("--out-dir", type=Path, default=REPO_ROOT / "data/processed/baseline-methods")
    parser.add_argument("--results-dir", type=Path, default=REPO_ROOT / "data/reference/baseline-methods")
    parser.add_argument("--clips", type=str, default=None, help="Comma-separated clip numbers; default: all clips present in --tracks.")
    parser.add_argument("--device", choices=["auto", "mps", "cpu"], default="auto")
    parser.add_argument("--dry-run", action="store_true", help="Run steps 1-4 on the requested clip(s) and print statistics without writing results.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    model, resolved_device, import_path_used = load_embedder(
        args.vendor, args.checkpoint, args.checkpoint_sha256, args.device
    )
    tracklet_cls, refine_module = load_vendored_refine_module(args.vendor)

    tracks_df = pl.read_parquet(args.tracks)
    index_df = pl.read_csv(args.crops / "index.csv")

    if args.clips:
        requested = [int(c.strip()) for c in args.clips.split(",") if c.strip()]
        tracks_df = tracks_df.filter(pl.col("clip_number").is_in(requested))
        index_df = index_df.filter(pl.col("clip_number").is_in(requested))
    clip_numbers = sorted(tracks_df["clip_number"].unique().to_list())
    if not clip_numbers:
        print("FEHLER: keine Clips im gefilterten --tracks -- pruefe --clips.", file=sys.stderr)
        return 1

    embeddings, embed_elapsed_s = embed_crops(index_df, args.crops, model, resolved_device)

    tracked_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    all_rows: list[dict] = []
    all_stats: list[dict] = []
    refine_start = time.perf_counter()
    for clip_number in clip_numbers:
        rows, stats = process_clip(
            tracks_df, index_df, embeddings, clip_number, tracklet_cls, refine_module, tracked_at
        )
        all_rows.extend(rows)
        all_stats.append(stats)
        print(
            f"Clip {clip_number:>3}: {stats['n_embedded_tracks']} embedded, "
            f"{stats['n_passthrough_tracks']} passthrough, split_ops={stats['n_split_ops']}, "
            f"merge_ops={stats['n_merge_ops']}, unveraendert={stats['partition_unchanged']}"
        )
    refine_elapsed_s = time.perf_counter() - refine_start

    all_crops_per_track = [c for s in all_stats for c in s["crops_per_track"]]
    median_crops_per_track = float(np.median(all_crops_per_track)) if all_crops_per_track else 0.0
    total_split_ops = sum(s["n_split_ops"] for s in all_stats)
    total_merge_ops = sum(s["n_merge_ops"] for s in all_stats)
    n_unchanged_clips = sum(1 for s in all_stats if s["partition_unchanged"])

    print(
        f"\nZusammenfassung ueber {len(clip_numbers)} Clip(s): "
        f"median Crops/Track={median_crops_per_track:.1f}, "
        f"Split-Operationen={total_split_ops}, Merge-Operationen={total_merge_ops}, "
        f"unveraenderte Partition in {n_unchanged_clips}/{len(clip_numbers)} Clips. "
        f"Embedding-Laufzeit={embed_elapsed_s:.2f}s, Split/Merge-Laufzeit={refine_elapsed_s:.2f}s."
    )

    if args.dry_run:
        print("Dry-Run: keine Ergebnisse geschrieben.")
        return 0

    out_path = args.out_dir / "gta" / "tracks.parquet"
    tracks_out_df = bc.write_tracks(all_rows, out_path)

    report_path = args.out_dir / "gta" / "report.json"
    start = time.perf_counter()
    report = bc.score_with_shared_harness(out_path, args.review, report_path)
    score_elapsed_s = time.perf_counter() - start
    runtime_s = embed_elapsed_s + refine_elapsed_s + score_elapsed_s

    summary = bc.summarise(report, tracks_out_df, args.split)

    checkpoint_name = Path(args.checkpoint).name
    try:
        gta_head = _run_git(["-C", str(args.vendor), "rev-parse", "HEAD"]).strip()
    except Exception:
        gta_head = "unbekannt"
    config = f"gta-link@{gta_head[:8]}+osnet_x1_0-generic"

    start_command = (
        "uv run python scripts/hackathon/measure_gta.py "
        f"--tracks {_relative(args.tracks)} --crops {_relative(args.crops)} "
        f"--vendor {_relative(args.vendor)} --checkpoint {_relative(args.checkpoint)} "
        f"--checkpoint-sha256 {args.checkpoint_sha256} --review {_relative(args.review)} "
        f"--split {_relative(args.split)} --out-dir {_relative(args.out_dir)} "
        f"--results-dir {_relative(args.results_dir)}"
    )

    notes = (
        "generisches OSNet-Checkpoint (kein sportspezifisches Finetuning) -- Grund: "
        "Lizenz/Provenienz (der sport-feingetunte Checkpoint hat keine nachvollziehbare "
        "Herkunft, siehe vendor/README.md ## Checkpoint). "
        f"median Crops/Track={median_crops_per_track:.1f} bei max_crops_per_track=12 "
        "(gesampelte Crop-Menge, nicht jeder Frame eingebettet -- Referenzimplementierung embettet jeden Frame). "
        f"Split-Operationen={total_split_ops}, Merge-Operationen={total_merge_ops}, "
        f"unveraenderte Partition in {n_unchanged_clips}/{len(clip_numbers)} Clips."
    )

    summary_row = {
        "method": "gta",
        "config": config,
        **summary,
        "human_pass_k": None,
        "human_pass_n": None,
        "runtime_s": round(runtime_s, 3),
        "license": _LICENSE_NOTE,
        "start_command": start_command,
        "tracks_path": _relative(out_path),
        "notes": notes,
    }
    per_clip_rows = _build_per_clip_rows("gta", config, report, tracks_out_df, args.split)
    bc.append_results(summary_row, per_clip_rows, args.results_dir)

    print(
        f"GTA: auto={bc.fmt_rate(summary['auto_ok_k'], summary['auto_ok_n'])} "
        f"(Checkpoint: {checkpoint_name}, Laufzeit gesamt={runtime_s:.2f}s -- "
        f"embed={embed_elapsed_s:.2f}s, refine={refine_elapsed_s:.2f}s, score={score_elapsed_s:.2f}s)"
    )
    return 0


def _build_per_clip_rows(method: str, config: str, report: dict, tracks_df: pl.DataFrame, split_csv: Path) -> list[dict]:
    private_by_clip = bc.private_test_by_clip(split_csv)
    player_by_clip = bc.player_track_counts(tracks_df)
    rows = []
    for row in report["per_clip"]:
        clip_number = int(row["clip_number"])
        rows.append(
            {
                "method": method,
                "config": config,
                "clip_number": clip_number,
                "private_test": private_by_clip.get(clip_number),
                "n_tracks": int(row["n_tracks"]),
                "n_player_tracks": player_by_clip.get(clip_number, 0),
                "longest_track_frac": float(row["longest_track_frac"]),
                "n_fragments": int(row["n_fragments"]),
                "auto_flag": row["auto_flag"],
            }
        )
    return rows


def _run_git(args: list[str]) -> str:
    import subprocess

    result = subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=True)
    return result.stdout


if __name__ == "__main__":
    raise SystemExit(main())
