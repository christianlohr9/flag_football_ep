"""Test-support module: canonical-frame factories for every later test module.

Ships inside the package (not `tests/conftest.py`) so any test file can
`from flag_football_ep.testing import canonical_plays` with no conftest edits
or sys.path tricks. Frames are derived from `CORE_COLUMNS`/`NULLABLE_EXTRAS`
so the factory cannot drift from the canonical schema it builds against.
"""

from __future__ import annotations

import polars as pl

from flag_football_ep.canonical import (
    CORE_COLUMNS,
    NULLABLE_EXTRAS,
    add_score_columns,
    add_scoring_play_team,
    make_game_id,
)

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
