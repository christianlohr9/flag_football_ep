"""Test-support module: canonical-frame factories for every later test module.

Ships inside the package (not `tests/conftest.py`) so any test file can
`from flag_football_ep.testing import canonical_plays` with no conftest edits
or import-path hacks. Frames are derived from `CORE_COLUMNS`/`NULLABLE_EXTRAS`
so the factory cannot drift from the canonical schema it builds against.

`testing.py` is a core module: it may import `flag_football_ep.cv.schema` (polars
only, no `cv` extras dependency -- see D-07/D-08) but must never pull an actual `cv`
extras package (rfdetr/trackers/supervision/sahi/transformers/umap/torch/cv2),
guarded by `tests/test_cv_contracts.py`'s lazy-import check.
"""

from __future__ import annotations

import random

import polars as pl

from flag_football_ep.canonical import (
    CORE_COLUMNS,
    NULLABLE_EXTRAS,
    add_score_columns,
    add_scoring_play_team,
    make_game_id,
)
from flag_football_ep.cv.schema import TRACKING_COLUMNS, TRACKING_SCHEMA, conform_tracking

_HOME_TEAM = "HOME"
_AWAY_TEAM = "AWAY"

_INT_CORE_COLUMNS = [name for name, dtype in CORE_COLUMNS.items() if dtype == pl.Int32]


def canonical_plays(
    n_games: int = 1,
    plays_per_game: int = 8,
    source: str = "hudl",
    overrides: dict | None = None,
    extras: dict | None = None,
) -> pl.DataFrame:
    """Build a schema-valid canonical frame for tests.

    `game_id` comes from `make_game_id`; `play_id` is contiguous 1..N per
    game; `drive_id` is non-decreasing, incrementing every 4 plays starting
    at 1; `half` is 1 for the first half of the plays and 2 for the rest;
    `posteam`/`defteam` alternate between two team codes; `down` cycles
    1..4; `yards_to_go` is between 5 and 20; `yardline_50` is between 0 and
    50; all other flag/count columns default to 0; every extra defaults to
    null.

    `overrides` maps a column name to a scalar (broadcast to every row) or a
    list (length must equal the frame height). `extras` does the same for
    NULLABLE_EXTRAS columns. Both raise `ValueError` on an unknown column
    name or a length mismatch.
    """
    rows: list[dict] = []
    for game_idx in range(n_games):
        game_id = make_game_id(source, f"test-{game_idx}")
        drive_id = 1
        half_boundary = (plays_per_game + 1) // 2
        for play_idx in range(1, plays_per_game + 1):
            if play_idx > 1 and (play_idx - 1) % 4 == 0:
                drive_id += 1
            half = 1 if play_idx <= half_boundary else 2
            posteam = _HOME_TEAM if play_idx % 2 == 1 else _AWAY_TEAM
            defteam = _AWAY_TEAM if posteam == _HOME_TEAM else _HOME_TEAM
            down = ((play_idx - 1) % 4) + 1
            yards_to_go = 5 + ((play_idx - 1) % 16)
            yardline_50 = (play_idx * 7) % 51

            row = {name: 0 for name in _INT_CORE_COLUMNS}
            row.update(
                {
                    "source": source,
                    "competition": "TEST",
                    "game_id": game_id,
                    "posteam": posteam,
                    "defteam": defteam,
                    "home_team": _HOME_TEAM,
                    "away_team": _AWAY_TEAM,
                    "posteam_after": posteam,
                    "play_type": "pass",
                    "result_raw": "Complete",
                    "scoring_play_team": None,
                    "season": 2026,
                    "play_id": play_idx,
                    "drive_id": drive_id,
                    "half": half,
                    "down": down,
                    "yards_to_go": yards_to_go,
                    "yardline": yardline_50,
                    "yardline_50": yardline_50,
                    "yardline_50_after": yardline_50,
                    "yardline_50_simple": 0 if yardline_50 < 25 else 1,
                }
            )
            rows.append(row)

    height = len(rows)
    data: dict[str, list] = {name: [row.get(name) for row in rows] for name in CORE_COLUMNS}
    for name in NULLABLE_EXTRAS:
        data[name] = [None] * height

    df = pl.DataFrame(data, schema={**CORE_COLUMNS, **NULLABLE_EXTRAS})

    df = _apply_column_values(df, overrides, height)
    df = _apply_column_values(df, extras, height)
    return df


def canonical_plays_with_scores(
    n_games: int = 1,
    plays_per_game: int = 8,
    source: str = "hudl",
    overrides: dict | None = None,
    extras: dict | None = None,
) -> pl.DataFrame:
    """`canonical_plays(...)` with `scoring_play_team` and the score chain filled in."""
    df = canonical_plays(
        n_games=n_games,
        plays_per_game=plays_per_game,
        source=source,
        overrides=overrides,
        extras=extras,
    )
    df = add_scoring_play_team(df, credit_defense=True)
    df = add_score_columns(df)
    return df


def _apply_column_values(df: pl.DataFrame, values: dict | None, height: int) -> pl.DataFrame:
    """Apply an overrides/extras mapping, raising on unknown names or bad lengths."""
    if not values:
        return df

    columns = []
    for name, value in values.items():
        if name not in df.columns:
            raise ValueError(f"Unknown column: {name!r}")
        dtype = df.schema[name]
        if isinstance(value, list):
            if len(value) != height:
                raise ValueError(
                    f"Override for {name!r} has length {len(value)}, expected {height}"
                )
            columns.append(pl.Series(name, value).cast(dtype, strict=False))
        else:
            columns.append(pl.lit(value).cast(dtype, strict=False).alias(name))

    return df.with_columns(columns)


# Pixel-space frame the synthetic detector "sees"; field-yard bounds the synthetic
# homography projects into. Both are internal to this factory -- callers never need
# to know them, only that x_yards/y_yards stay inside a real flag-football field.
_FRAME_WIDTH_PX = 1920.0
_FRAME_HEIGHT_PX = 1080.0
_BOX_WIDTH_PX = 40.0
_BOX_HEIGHT_PX = 90.0
_FIELD_LENGTH_YARDS = 50.0
_FIELD_WIDTH_YARDS = 25.0


def synthetic_tracks(
    *,
    n_clips: int = 2,
    n_frames: int = 10,
    n_tracks: int = 4,
    session_id: str = "test-session",
    with_teams: bool = False,
    with_field_coords: bool = False,
    with_pbp_keys: bool = False,
    detector_run_id: str = "0" * 32,
    seed: int = 20260516,
) -> pl.DataFrame:
    """Build a schema-valid `cv.schema` tracking frame for tests.

    One row per (clip, frame, track), `n_clips` x `n_frames` x `n_tracks` rows total.
    Every value is deterministic from the arguments -- two calls with identical
    arguments produce an identical frame (`seed` only drives `confidence` jitter via a
    freshly-seeded `random.Random`, never wall-clock/global state). Track 0 in every
    clip is `class_name="referee"`; every other track is `class_name="player"`. Each
    track moves linearly across the frame (constant per-frame pixel velocity), so
    downstream continuity/homography tests get monotone, checkable geometry.

    `with_teams` fills `team_id` for `player` rows (alternating by track index) and
    leaves it null for the one `referee` track per clip. `with_field_coords` fills
    `x_yards`/`y_yards` by linearly projecting pixel position into a fixed field-yard
    box. `with_pbp_keys` fills `game_id`/`play_id` with canonical-dtype values (one
    play per clip), so the dtype-mismatch join regression test can build both sides of
    a join from this same factory. Every flag fills exactly the columns it names; the
    others stay null. The returned frame already passes `cv.schema.conform_tracking`
    unchanged.
    """
    rng = random.Random(seed)
    rows: list[dict] = []

    for clip in range(1, n_clips + 1):
        for track in range(n_tracks):
            is_referee = track == 0
            class_name = "referee" if is_referee else "player"
            start_x = 80.0 + track * 120.0
            start_y = 200.0 + track * 60.0
            velocity_x = 15.0
            velocity_y = 4.0

            for frame_index in range(n_frames):
                foot_x = start_x + velocity_x * frame_index
                foot_y = start_y + velocity_y * frame_index

                row = {
                    "session_id": session_id,
                    "clip_number": clip,
                    "frame_index": frame_index,
                    "timestamp_s": round(frame_index / 30.0, 4),
                    "track_id": track,
                    "class_name": class_name,
                    "confidence": round(0.75 + rng.random() * 0.2, 4),
                    "bbox_x1": foot_x - _BOX_WIDTH_PX / 2,
                    "bbox_y1": foot_y - _BOX_HEIGHT_PX,
                    "bbox_x2": foot_x + _BOX_WIDTH_PX / 2,
                    "bbox_y2": foot_y,
                    "foot_x_px": foot_x,
                    "foot_y_px": foot_y,
                    "team_id": (track % 2) if (with_teams and not is_referee) else None,
                    "hover_position_id": None,
                    "x_yards": None,
                    "y_yards": None,
                    "game_id": f"{session_id}-game" if with_pbp_keys else None,
                    "play_id": clip if with_pbp_keys else None,
                    "detector_run_id": detector_run_id,
                    "tracked_at": "2026-08-24T00:00:00Z",
                }

                if with_field_coords:
                    row["x_yards"] = round(
                        (foot_x / _FRAME_WIDTH_PX) * _FIELD_LENGTH_YARDS, 4
                    )
                    row["y_yards"] = round(
                        (foot_y / _FRAME_HEIGHT_PX) * _FIELD_WIDTH_YARDS, 4
                    )

                rows.append(row)

    df = pl.DataFrame(rows, schema=dict(TRACKING_SCHEMA)).select(list(TRACKING_COLUMNS))
    return conform_tracking(df)
