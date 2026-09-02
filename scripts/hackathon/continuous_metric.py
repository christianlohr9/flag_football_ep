"""Label-free continuous metric layer for the hackathon scoring script (METR-01).

Standalone module, same convention as `score_tracks.py` and `baseline_common.py`:
not part of the installed `flag_football_ep` package, English docstrings, `polars`
for I/O, no deep-learning/GPU training framework import anywhere in this module (the
scoring path stays installable and runnable in seconds). The continuity helpers this
module normalises (`_measure_clip`, `summarise_review`) arrive by injection -- every
function that needs one takes it as a parameter -- so this module stays importable
and unit-testable without ever touching `sys.path` for `flag_football_ep` itself.

Two numbers, two very different jobs:

- `fragments_per_expected_player` (`n_fragments / EXPECTED_PLAYERS`) is the ONE
  officially reported continuous number (METR-01). It moves when tracking gets
  better inside a play the pass/fail threshold still marks failed.
- `active_track_count_deviation` is a DIAGNOSTIC guard column, never an acceptance
  criterion -- see `GUARD_NOTE`. It partially catches the over-merge failure mode
  the primary number rewards (fewer simultaneous tracks than expected).

See `BLIND_SPOT_NOTE` for what neither number can see: a silent identity swap during
an overlap, the dominant real failure mode in this dataset (39/46 pilot fails).
`tests/test_m2_metric.py::test_swap_is_invisible_to_both_metrics` makes that ceiling
an executable claim, not a footnote.

Deliberately NOT implemented (RESEARCH.md rejects it as uncalibrated): a
switch-event proximity heuristic (`track A ends at frame f near position p` ->
`track B starts within r px within n frames`) that would flag the
occlusion-then-reacquire failure mode. It needs two tuned parameters with no ground
truth in this phase to calibrate them against, and it cannot see the crossing-swap
failure mode at all (no track ever ends in that case). Left as a documented,
exploratory option only -- not shipped as an official metric.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# 5v5, the lower bound of `baseline_common.IDEAL_TRACK_BAND` -- the same constant the
# challenge description already publishes as the ideal player-track count per clip.
EXPECTED_PLAYERS = 10

BLIND_SPOT_NOTE = (
    "Diese Kennzahl misst Track-Abdeckung und Fragmentierung, NICHT Identitaets-"
    "Korrektheit. Ein stiller Identitaetswechsel waehrend einer Ueberlappung -- der "
    "dominante Fehlerfall in diesem Datensatz, 39 von 46 Pilot-Fails -- hinterlaesst "
    "darin keine Spur: kein Track endet, kein Track wird neu geboren, und die Anzahl "
    "gleichzeitig aktiver Tracks aendert sich nicht. Ein Over-Merge (zwei "
    "Spielerinnen unter einer ID) VERBESSERT die Zahl, waehrend die Identitaet "
    "schlechter wird. Die Kennzahl zeigt Fortschritt innerhalb eines gescheiterten "
    "Plays und ersetzt weder das menschliche Urteil noch eine Assoziationsmetrik mit "
    "Identitaets-Labels."
)

GUARD_NOTE = (
    "active_track_count_deviation ist eine diagnostische Kennzahl -- kein "
    "Abnahmekriterium."
)


def player_view(clip_tracks: pl.DataFrame) -> tuple[pl.DataFrame, bool]:
    """Filter `clip_tracks` to `class_name == "player"` rows when the column
    exists (referees must not inflate the count), otherwise return the frame
    unchanged. Returns `(view, class_name_filtered)` -- the bool says which
    happened, since `REQUIRED_TRACK_COLUMNS` does not guarantee `class_name`.
    """
    if "class_name" in clip_tracks.columns:
        return clip_tracks.filter(pl.col("class_name") == "player"), True
    return clip_tracks, False


def active_track_count_deviation(clip_tracks: pl.DataFrame) -> float:
    """Mean over frames of `abs(distinct active track_ids in the frame - EXPECTED_PLAYERS)`.

    An empty clip (no rows at all) returns the full deviation `float(EXPECTED_PLAYERS)`
    -- zero active tracks is not perfect, it is the worst possible reading.
    """
    if clip_tracks.height == 0:
        return float(EXPECTED_PLAYERS)

    counts = clip_tracks.group_by("frame_index").agg(
        pl.col("track_id").n_unique().cast(pl.Int64).alias("n_active")
    )
    deviations = (counts["n_active"] - EXPECTED_PLAYERS).abs()
    return float(deviations.mean())


def clip_metrics(clip_number: int, clip_tracks: pl.DataFrame, measure_clip_fn) -> dict:
    """Apply `player_view`, call `measure_clip_fn` (injected, e.g.
    `cv.continuity._measure_clip`) on the filtered view, and return one clip's row
    for both the primary and the guard metric.
    """
    filtered, class_name_filtered = player_view(clip_tracks)
    result = measure_clip_fn(clip_number, filtered)
    guard = active_track_count_deviation(filtered)

    return {
        "clip_number": result.clip_number,
        "n_tracks": result.n_tracks,
        "longest_track_frac": result.longest_track_frac,
        "n_fragments": result.n_fragments,
        "auto_flag": result.auto_flag,
        "fragments_per_expected_player": round(result.n_fragments / EXPECTED_PLAYERS, 4),
        "active_track_count_deviation": round(guard, 4),
        "class_name_filtered": class_name_filtered,
        "no_tracks": result.n_tracks == 0,
    }


def aggregate(clip_rows: list[dict]) -> dict:
    """Roll `clip_metrics` rows up into session-level means/medians. Every
    aggregate is `None` when `clip_rows` is empty -- never `0.0` posing as a
    measurement over zero clips.
    """
    n_clips = len(clip_rows)
    if n_clips == 0:
        return {
            "n_clips": 0,
            "mean_fragments_per_expected_player": None,
            "median_fragments_per_expected_player": None,
            "mean_active_track_count_deviation": None,
            "n_clips_without_class_name": None,
            "n_clips_without_tracks": None,
        }

    fpp = [row["fragments_per_expected_player"] for row in clip_rows]
    guard = [row["active_track_count_deviation"] for row in clip_rows]
    n_clips_without_class_name = sum(1 for row in clip_rows if not row["class_name_filtered"])
    n_clips_without_tracks = sum(1 for row in clip_rows if row["no_tracks"])

    return {
        "n_clips": n_clips,
        "mean_fragments_per_expected_player": round(float(np.mean(fpp)), 4),
        "median_fragments_per_expected_player": round(float(np.median(fpp)), 4),
        "mean_active_track_count_deviation": round(float(np.mean(guard)), 4),
        "n_clips_without_class_name": n_clips_without_class_name,
        "n_clips_without_tracks": n_clips_without_tracks,
    }
