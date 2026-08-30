"""Validation/reporting tooling for `homography.clip_alignment` (per-clip drift
correction, see `docs/homography-calibration.md`'s "Per-Clip-Homographie-Verfeinerung"
section). Standalone script, not part of the installed `flag_football_ep` package --
its outputs (a drift-distribution CSV/report and per-clip grid-overlay JPEGs) are
one-off validation artifacts for this fix, not something the pipeline needs at
runtime.

Three subcommands:

`drift` -- computes `homography.clip_alignment_matrix` for every clip registered
against its hover position's reference clip in `data/reference/hover_positions.csv`
(skipping the two reference clips themselves), decomposes each `H_align` into a
translation magnitude (px, measured at the frame center -- more representative of
"how far did the visible pitch move" than the raw matrix translation term, which is
the displacement at pixel (0,0)) and a rotation angle (degrees, from the linear part of
the matrix), and writes the full per-clip table plus summary statistics.

`intra-clip` -- for a handful of sample clips, registers that SAME clip's early frame
against its own late frame (not against the hover position's reference clip) to
measure drift WITHIN a single clip -- the plan's <action> block asks this be measured
and reported, not acted on unless it exceeds ~15px.

`grid` -- projects the calibrated + per-clip-aligned field-yard grid onto a
representative frame of each requested clip and writes
`data/processed/experiments/grid_check_clip{N}.jpg`, the visual proof the fix lands the
grid on the painted lines for clips OTHER than the two calibration reference clips.
"""

from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path

import cv2
import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load():
    import sys

    sys.path.insert(0, str(REPO_ROOT / "src"))
    from flag_football_ep.config import load_config
    from flag_football_ep.cv.coordinates import composed_transformer_for
    from flag_football_ep.cv.frames import clip_number as clip_number_of
    from flag_football_ep.cv.frames import clip_paths
    from flag_football_ep.cv.homography import (
        CLIP_ALIGNMENT_REFERENCE_FRAMES,
        clip_alignment,
        clip_alignment_matrix,
        load_calibration,
    )

    return {
        "load_config": load_config,
        "composed_transformer_for": composed_transformer_for,
        "clip_number_of": clip_number_of,
        "clip_paths": clip_paths,
        "CLIP_ALIGNMENT_REFERENCE_FRAMES": CLIP_ALIGNMENT_REFERENCE_FRAMES,
        "clip_alignment": clip_alignment,
        "clip_alignment_matrix": clip_alignment_matrix,
        "load_calibration": load_calibration,
    }


def _decompose(matrix: np.ndarray, frame_wh: tuple[int, int]) -> tuple[float, float]:
    """`(translation_px, rotation_deg)` for a drift-correction homography `matrix`.

    Translation is measured as the displacement of the FRAME CENTER under `matrix`
    (not the raw `matrix[:2, 2]` term, which is the displacement at pixel (0, 0) --
    for a matrix that also carries rotation, that is not representative of how far the
    visibly-tracked pitch actually moved). Rotation is read off the linear (2x2) part
    of the normalized matrix via the standard rotation+scale/shear decomposition
    formula -- an approximation (a general homography's 2x2 block also carries some
    perspective-induced shear), adequate for this diagnostic's purpose.
    """
    normalized = matrix / matrix[2, 2]
    width, height = frame_wh
    center = np.array([[width / 2.0, height / 2.0]], dtype=np.float64)
    homogeneous = np.hstack([center, np.ones((1, 1))])
    mapped = (homogeneous @ normalized.T)
    mapped = mapped[:, :2] / mapped[:, [2]]
    translation_px = float(np.linalg.norm(mapped[0] - center[0]))

    a = normalized[:2, :2]
    rotation_rad = math.atan2(a[1, 0] - a[0, 1], a[0, 0] + a[1, 1])
    rotation_deg = math.degrees(rotation_rad)

    return translation_px, rotation_deg


def cmd_drift(args: argparse.Namespace) -> None:
    mods = _load()
    cfg = mods["load_config"](REPO_ROOT / "ffep.toml")

    hover_path = cfg.paths.reference / "hover_positions.csv"
    hover_df = pl.read_csv(
        hover_path, schema_overrides={"clip_number": pl.Int64, "hover_position_id": pl.Utf8}
    )

    clips_by_number = {
        mods["clip_number_of"](p): p for p in mods["clip_paths"](cfg, cfg.cv.pilot_session_id)
    }

    reference_clip_numbers = {ref[0] for ref in mods["CLIP_ALIGNMENT_REFERENCE_FRAMES"].values()}

    rows: list[dict[str, object]] = []
    for row in hover_df.iter_rows(named=True):
        clip_num = int(row["clip_number"])
        hover_id = row["hover_position_id"]
        if clip_num in reference_clip_numbers:
            continue
        if hover_id not in mods["CLIP_ALIGNMENT_REFERENCE_FRAMES"]:
            continue
        clip_path = clips_by_number.get(clip_num)
        if clip_path is None:
            continue

        cap = cv2.VideoCapture(str(clip_path))
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        cap.release()

        h_align = mods["clip_alignment_matrix"](hover_id, clip_num, cfg)
        is_identity = np.array_equal(h_align, np.eye(3))
        translation_px, rotation_deg = _decompose(h_align, (width, height))

        rows.append(
            {
                "clip_number": clip_num,
                "hover_position_id": hover_id,
                "translation_px": round(translation_px, 2),
                "rotation_deg": round(rotation_deg, 3),
                "fell_back_to_identity": is_identity,
            }
        )
        print(
            f"clip {clip_num:>3} ({hover_id}): translation={translation_px:6.2f}px "
            f"rotation={rotation_deg:6.3f}deg"
            + (" [IDENTITY FALLBACK]" if is_identity else "")
        )

    out_csv = REPO_ROOT / "data/processed/experiments/clip_alignment_drift.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "clip_number",
                "hover_position_id",
                "translation_px",
                "rotation_deg",
                "fell_back_to_identity",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    translations = [r["translation_px"] for r in rows if not r["fell_back_to_identity"]]
    rotations = [abs(r["rotation_deg"]) for r in rows if not r["fell_back_to_identity"]]
    n_fallback = sum(1 for r in rows if r["fell_back_to_identity"])

    if translations:
        arr = np.array(translations)
        rot_arr = np.array(rotations)
        print()
        print(f"n_clips={len(rows)} n_identity_fallback={n_fallback}")
        print(
            f"translation_px: median={np.median(arr):.2f} p90={np.percentile(arr, 90):.2f} "
            f"max={np.max(arr):.2f}"
        )
        print(
            f"rotation_deg (abs): median={np.median(rot_arr):.3f} "
            f"p90={np.percentile(rot_arr, 90):.3f} max={np.max(rot_arr):.3f}"
        )
    print(f"wrote {out_csv}")


def cmd_intra_clip(args: argparse.Namespace) -> None:
    mods = _load()
    cfg = mods["load_config"](REPO_ROOT / "ffep.toml")
    clips_by_number = {
        mods["clip_number_of"](p): p for p in mods["clip_paths"](cfg, cfg.cv.pilot_session_id)
    }

    for clip_num in args.clip:
        clip_path = clips_by_number[clip_num]
        cap = cv2.VideoCapture(str(clip_path))
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        duration = frame_count / fps

        early_t = min(1.0, duration * 0.1)
        late_t = max(early_t + 0.1, duration - 1.0)

        cap.set(cv2.CAP_PROP_POS_MSEC, early_t * 1000.0)
        ok1, early_frame = cap.read()
        cap.set(cv2.CAP_PROP_POS_MSEC, late_t * 1000.0)
        ok2, late_frame = cap.read()
        cap.release()

        if not ok1 or not ok2:
            print(f"clip {clip_num}: could not extract both frames, skipping")
            continue

        h_align = mods["clip_alignment"](late_frame, early_frame)
        translation_px, rotation_deg = _decompose(h_align, (width, height))
        print(
            f"clip {clip_num}: intra-clip drift over {late_t - early_t:.1f}s "
            f"({early_t:.1f}s -> {late_t:.1f}s): translation={translation_px:.2f}px "
            f"rotation={rotation_deg:.3f}deg"
        )


def cmd_grid(args: argparse.Namespace) -> None:
    mods = _load()
    cfg = mods["load_config"](REPO_ROOT / "ffep.toml")

    hover_path = cfg.paths.reference / "hover_positions.csv"
    hover_df = pl.read_csv(
        hover_path, schema_overrides={"clip_number": pl.Int64, "hover_position_id": pl.Utf8}
    )
    hover_by_clip = {
        int(row["clip_number"]): row["hover_position_id"] for row in hover_df.iter_rows(named=True)
    }

    calibration = mods["load_calibration"](cfg.reference.homography_calibration)
    clips_by_number = {
        mods["clip_number_of"](p): p for p in mods["clip_paths"](cfg, cfg.cv.pilot_session_id)
    }

    length = cfg.cv.field_length_yards
    width_yards = cfg.cv.field_width_yards
    endzone = cfg.cv.endzone_yards

    out_dir = REPO_ROOT / "data/processed/experiments"
    out_dir.mkdir(parents=True, exist_ok=True)

    for clip_num in args.clip:
        hover_id = hover_by_clip.get(clip_num)
        if hover_id is None:
            print(f"clip {clip_num}: no hover_position_id, skipping")
            continue
        clip_path = clips_by_number.get(clip_num)
        if clip_path is None:
            print(f"clip {clip_num}: no video file, skipping")
            continue

        cap = cv2.VideoCapture(str(clip_path))
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        cap.set(cv2.CAP_PROP_POS_FRAMES, max(0, frame_count // 2))
        ok, frame = cap.read()
        cap.release()
        if not ok:
            print(f"clip {clip_num}: could not extract frame, skipping")
            continue

        transformer = mods["composed_transformer_for"](hover_id, clip_num, calibration, cfg)
        inverse_matrix = np.linalg.inv(transformer.m)

        annotated = frame.copy()

        def yards_to_px(x_yd: float, y_yd: float) -> tuple[int, int]:
            pt = np.array([[[x_yd, y_yd]]], dtype=np.float32)
            projected = cv2.perspectiveTransform(pt, inverse_matrix.astype(np.float32))
            return int(round(projected[0, 0, 0])), int(round(projected[0, 0, 1]))

        color = (0, 255, 255)
        # Sidelines, goal lines, end-zone back lines.
        for x_yd in (-endzone, 0.0, length, length + endzone):
            p1 = yards_to_px(x_yd, 0.0)
            p2 = yards_to_px(x_yd, width_yards)
            cv2.line(annotated, p1, p2, color, 2)
        for y_yd in (0.0, width_yards):
            p1 = yards_to_px(-endzone, y_yd)
            p2 = yards_to_px(length + endzone, y_yd)
            cv2.line(annotated, p1, p2, color, 2)
        # Yard lines every 5 yards.
        yard = 5.0
        while yard < length:
            p1 = yards_to_px(yard, 0.0)
            p2 = yards_to_px(yard, width_yards)
            cv2.line(annotated, p1, p2, (0, 200, 200), 1)
            yard += 5.0

        cv2.putText(
            annotated,
            f"clip {clip_num} (hover {hover_id}) -- composed grid",
            (10, 30),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.8,
            (0, 0, 0),
            3,
            cv2.LINE_AA,
        )
        cv2.putText(
            annotated,
            f"clip {clip_num} (hover {hover_id}) -- composed grid",
            (10, 30),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.8,
            (0, 255, 255),
            1,
            cv2.LINE_AA,
        )

        out_path = out_dir / f"grid_check_clip{clip_num}.jpg"
        cv2.imwrite(str(out_path), annotated)
        print(f"wrote {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    drift_parser = subparsers.add_parser("drift", help="per-clip H_align drift distribution")
    drift_parser.set_defaults(func=cmd_drift)

    intra_parser = subparsers.add_parser("intra-clip", help="within-clip drift for sample clips")
    intra_parser.add_argument("--clip", type=int, nargs="+", required=True)
    intra_parser.set_defaults(func=cmd_intra_clip)

    grid_parser = subparsers.add_parser("grid", help="render composed-grid overlay JPEGs")
    grid_parser.add_argument("--clip", type=int, nargs="+", required=True)
    grid_parser.set_defaults(func=cmd_grid)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
