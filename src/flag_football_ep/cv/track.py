"""Per-session tracking: detect + associate every clip into player tracks.

Owns the streaming, per-clip loop analogous to `pipeline.run_ingest`'s per-source
orchestration: `track_session` runs `detect.detect_video` + OC-SORT association
(`trackers`, Apache-2.0 -- never `boxmot`, AGPL-3.0, C-06) over every clip registered
for `session_id`, one clip at a time, inside a per-clip try/except so a single corrupt
or zero-detection clip never aborts the whole session (mirroring `run_ingest`'s
"one broken source never aborts the run" discipline) -- this is what makes the D-09
"whole game is the denominator" gate measurement possible without one bad clip blocking
the run. Anomalies are collected as `notices`, not raised, exactly like
`pipeline.run_ingest`'s `notices: list[str]` convention.

Resolves its detector exactly like `cv.detect.load_detector`: `run_id=None` goes
through `cv.registry.resolve_champion`, never "the newest FINISHED run." The single
shared MLflow tracking-store configuration in `flag_football_ep.model.mlflow_store` is
reused unchanged -- this module never constructs a second store or redirects the
ambient tracking URI itself.

Writes `data/processed/tracking/*.parquet` with the same atomic-write discipline
(`.tmp` sibling + `os.replace`) `pipeline._atomic_write_parquet` already uses for
`plays.parquet` (D-14).

Implemented by plan 02.1-12 (together with `teams.assign_teams`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flag_football_ep.config import Config


@dataclass
class TrackResult:
    """One `track_session` run's output: the written tracking-Parquet path, the
    number of clips/tracks produced, per-clip notices, and per-stage timing (the raw
    input to `benchmark.extrapolate_game_runtime`).
    """

    parquet_path: Path
    n_clips: int
    n_tracks: int
    notices: list[str] = field(default_factory=list)
    stage_seconds: dict[str, float] = field(default_factory=dict)


def track_session(
    config: Config,
    session_id: str,
    *,
    run_id: str | None = None,
    resolution: int | None = None,
    sahi: bool | None = None,
    out_path: Path | None = None,
) -> TrackResult:
    """Detect + track every clip registered for `session_id`, writing the combined
    tracking Parquet to `out_path` (defaulting to a config-derived path under
    `config.paths.tracking`).

    `run_id=None` resolves the `champion` detector alias. `resolution`/`sahi=None`
    fall back to `sighting.recommend_inference_settings`'s stored recommendation for
    the session's domain.
    """
    raise NotImplementedError("cv.track.track_session is implemented by plan 02.1-12")
