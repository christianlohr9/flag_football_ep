"""M2-3-ready label-based association interface (IDF1/MOTA via `motmetrics`).

(a) This is the M2-3/DATA-03 activation point: once per-frame identity ground
    truth exists, `compute_identity_metrics` is the callable that plugs into
    `score_tracks.py`'s future `--identity-gt-dev`/`--identity-gt-test` flags
    (named below, not yet wired into any CLI in this phase).

(b) It is the ONLY layer that can see identity swaps. `continuous_metric.py`'s
    label-free layer measures track coverage and fragmentation and is
    structurally blind to a silent identity swap during an overlap (see its own
    `BLIND_SPOT_NOTE`); this module's `frame_events`/`compute_identity_metrics`
    is the layer that can actually catch it, once ground truth exists to compare
    hypotheses against.

(c) `motmetrics` (MIT, verified 2026-09-02 via `gh api repos/cheind/py-motmetrics`
    and `slopcheck`) is a lazy, optional, TEST-time dependency. It is imported
    ONLY inside `compute_identity_metrics`'s function body and must never become
    a runtime dependency of the scoring path -- the label-free path in
    `continuous_metric.py` stays installable and runnable in seconds regardless
    of whether `motmetrics` is installed. This plan does NOT run `uv add`,
    `uv sync` or `pip install`; `motmetrics` stays uninstalled (it belongs in the
    `dev` dependency group, to be added as M2-3 follow-up work, not here).

(d) Future CLI surface, named now so M2-3's wiring is mechanical: flags
    `--identity-gt-dev` / `--identity-gt-test` carrying `REQUIRED_TRACK_COLUMNS`-
    shaped ground truth (the same 8-column contract `score_tracks.py` already
    enforces for `--tracks-dev`/`--tracks-test`), and a new `identity` block per
    split in the JSON report with the keys named in `IDENTITY_REPORT_KEYS`.

Standalone module, same convention as `continuous_metric.py` and `score_tracks.py`:
not part of the installed `flag_football_ep` package, English docstrings. `numpy`
and `polars` are imported at module level (both already project dependencies, used
by `baseline_common.py`); `motmetrics` is imported lazily, inside
`compute_identity_metrics` only -- importing this module never imports
`motmetrics`.
"""

from __future__ import annotations

import numpy as np
import polars as pl

# The future JSON report's `identity` block keys, one source of truth for plan
# M2-04-03's documentation and any future CLI wiring.
IDENTITY_REPORT_KEYS: tuple[str, ...] = (
    "idf1",
    "mota",
    "num_switches",
    "n_frames",
    "max_distance_px",
)

_MOTMETRICS_MISSING_MESSAGE = (
    "motmetrics ist nicht installiert (MIT-Lizenz, github.com/cheind/py-motmetrics, "
    "verifiziert 2026-09-02). Es ist eine TEST-Time-Abhaengigkeit der "
    "label-basierten Schicht, kein Laufzeit-Requirement des Scoring-Pfads. "
    "Installation (M2-3-Folgearbeit, nicht Teil dieses Plans): "
    "`uv add --group dev motmetrics`."
)


def _bbox_centres(frame_df: pl.DataFrame) -> np.ndarray:
    """`(n, 2)` array of bbox centre `(x, y)` coordinates, one row per detection
    in `frame_df`, in row order. Returns an `(0, 2)` array for an empty frame.
    """
    if frame_df.height == 0:
        return np.zeros((0, 2))
    xs = ((frame_df["bbox_x1"] + frame_df["bbox_x2"]) / 2).to_numpy()
    ys = ((frame_df["bbox_y1"] + frame_df["bbox_y2"]) / 2).to_numpy()
    return np.column_stack([xs, ys])


def frame_events(
    gt_df: pl.DataFrame, hyp_df: pl.DataFrame, *, max_distance_px: float
) -> list[tuple[int, list[int], list[int], np.ndarray]]:
    """Build one event per `frame_index` present in either `gt_df` or `hyp_df`:
    `(frame_index, gt_ids, hyp_ids, distance_matrix)`, where `distance_matrix` has
    shape `(len(gt_ids), len(hyp_ids))`. Distances are Euclidean between bbox
    centres; entries strictly above `max_distance_px` become `numpy.nan` (no
    match). A frame present in only one input yields an empty id list -- and a
    correspondingly empty distance-matrix axis -- on the other side. Pure
    numpy/polars; no `motmetrics` import here.
    """
    frame_indices = sorted(
        set(gt_df["frame_index"].to_list()) | set(hyp_df["frame_index"].to_list())
    )

    events: list[tuple[int, list[int], list[int], np.ndarray]] = []
    for frame_index in frame_indices:
        gt_frame = gt_df.filter(pl.col("frame_index") == frame_index)
        hyp_frame = hyp_df.filter(pl.col("frame_index") == frame_index)

        gt_ids = gt_frame["track_id"].to_list()
        hyp_ids = hyp_frame["track_id"].to_list()

        if len(gt_ids) == 0 or len(hyp_ids) == 0:
            distance_matrix = np.full((len(gt_ids), len(hyp_ids)), np.nan)
        else:
            gt_centres = _bbox_centres(gt_frame)
            hyp_centres = _bbox_centres(hyp_frame)
            diff = gt_centres[:, None, :] - hyp_centres[None, :, :]
            raw_distances = np.sqrt((diff**2).sum(axis=2))
            distance_matrix = np.where(raw_distances <= max_distance_px, raw_distances, np.nan)

        events.append((frame_index, gt_ids, hyp_ids, distance_matrix))

    return events


def compute_identity_metrics(
    gt_df: pl.DataFrame,
    hyp_df: pl.DataFrame,
    *,
    max_distance_px: float = 20.0,
    name: str = "clip",
) -> dict:
    """Compute IDF1/MOTA/num_switches for `hyp_df` against `gt_df` via
    `motmetrics`, lazily imported here so the label-free scoring path never
    depends on it.

    Raises `RuntimeError` (naming `motmetrics`, its MIT licence and the dev-group
    install instruction) when the library is absent -- never a bare
    `ImportError`, never a silently fabricated score.
    """
    try:
        import motmetrics as mm
    except ImportError as exc:
        raise RuntimeError(_MOTMETRICS_MISSING_MESSAGE) from exc

    events = frame_events(gt_df, hyp_df, max_distance_px=max_distance_px)

    accumulator = mm.MOTAccumulator(auto_id=False)
    for frame_index, gt_ids, hyp_ids, distance_matrix in events:
        accumulator.update(gt_ids, hyp_ids, distance_matrix, frameid=frame_index)

    metrics_host = mm.metrics.create()
    summary = metrics_host.compute(
        accumulator, metrics=["idf1", "mota", "num_switches"], name=name
    )

    return {
        "idf1": float(summary["idf1"].iloc[0]),
        "mota": float(summary["mota"].iloc[0]),
        "num_switches": int(summary["num_switches"].iloc[0]),
        "n_frames": len(events),
        "max_distance_px": max_distance_px,
    }
