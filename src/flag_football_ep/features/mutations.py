"""Feature engineering: EP/WP data preparation, EP/WP variables, and model training frames.

Ported from `Python/helper_add_hudl_mutations.py` (`prepare_ep_data`, `prepare_wp_data`),
`Python/helper_add_ep_wp.py` (`add_ep_variables`, `add_wp_variables`) and
`Python/helper_add_model_mutations.py` (`make_ep_model_mutations`, `make_wp_model_mutations`).

This is a behaviour-preserving port: every derived column, formula and constant matches the
frozen notebook baseline (see `tests/fixtures/baseline_manifest.json`). No evaluation
methodology changes (GroupKFold, calibration, empirical PAT baselines, feature re-tests) land
here -- those are Phase 1.3 (REQ-S1-07..REQ-S1-10).

`half_seconds_remaining` (`prepare_wp_data`) is SYNTHETIC: it is derived as
`1200 / max(play_id_half)` per half because real Hudl clock data has not been delivered
(REQ-S1-02 pending). Phase 1.4 gates the WP charts on this flag -- never treat it as a real
game clock, and do not source it from `game_clock_ms` here.
"""

from __future__ import annotations

import polars as pl


def _mark_half_end(df: pl.DataFrame) -> pl.DataFrame:
    """Add `index` (if absent), `half_end` and `game_end`.

    `half_end` is 1 on exactly the last row of each (game_id, half) group -- reproducing the
    notebook's `unique(subset=["game_id", "half"], keep="last")` selection via the per-group
    max index, since `index` increases monotonically with row order.
    """
    if "index" not in df.columns:
        df = df.with_row_index(name="index", offset=1)

    return df.with_columns(
        half_end=(pl.col("index") == pl.col("index").max().over(["game_id", "half"]))
        .cast(pl.Int32)
    ).with_columns(
        game_end=pl.when((pl.col("half_end") == 1) & (pl.col("half") == 2))
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
    )


def prepare_ep_data(df: pl.DataFrame) -> pl.DataFrame:
    """Port of `helper_add_hudl_mutations.prepare_ep_data`.

    Adds `index` (if absent), `half_end`, `game_end`, `scoring_event`, `score_drive`,
    `start_posteam`, `Next_Score_Half`, `Drive_Score_Half`, `max_play_id`.
    """
    output = (
        _mark_half_end(df)
        .with_columns(
            scoring_event=pl.when(pl.col("touchdown") == 1)
            .then(pl.lit("Touchdown"))
            .when(pl.col("def_touchdown") == 1)
            .then(pl.lit("Touchdown"))
            .when(pl.col("safety") == 1)
            .then(pl.lit("Safety"))
            .when(pl.col("one_point_conv_success") == 1)
            .then(pl.lit("Extra_Point"))
            .when(pl.col("two_point_conv_success") == 1)
            .then(pl.lit("Two_Point_Conversion"))
            .when((pl.col("half_end") == 1) & (pl.col("scoring_play") == 0))
            .then(pl.lit("No_Score"))
            .otherwise(pl.lit(None))
        )
        .with_columns(
            score_drive=pl.when(pl.col("scoring_play") == 1)
            .then(pl.col("drive_id"))
            .otherwise(pl.lit(None))
        )
        .with_columns(
            start_posteam=pl.when(pl.col("play_id") == 1)
            .then(pl.col("posteam"))
            .otherwise(pl.lit(None))
        )
        # Every fill below is scoped by game_id so a multi-game frame cannot leak a scoring
        # event, drive id or running score from one game into the next.
        .with_columns(
            scoring_play_team=pl.col("scoring_play_team").backward_fill().over("game_id"),
            scoring_event=pl.col("scoring_event").backward_fill().over("game_id"),
            score_drive=pl.col("score_drive").backward_fill().over("game_id"),
            posteam_score=pl.col("posteam_score").forward_fill().over("game_id"),
            defteam_score=pl.col("defteam_score").forward_fill().over("game_id"),
            start_posteam=pl.col("start_posteam").forward_fill().over("game_id"),
        )
        .with_columns(
            Next_Score_Half=pl.when(
                (pl.col("scoring_event") == "Touchdown")
                & (pl.col("posteam") == pl.col("scoring_play_team"))
            )
            .then(pl.lit("Touchdown"))
            .when(
                (pl.col("scoring_event") == "Touchdown")
                & (pl.col("posteam") != pl.col("scoring_play_team"))
            )
            .then(pl.lit("Opp_Touchdown"))
            .when(
                (pl.col("scoring_event") == "Safety")
                & (pl.col("posteam") == pl.col("scoring_play_team"))
            )
            .then(pl.lit("Safety"))
            .when(
                (pl.col("scoring_event") == "Safety")
                & (pl.col("posteam") != pl.col("scoring_play_team"))
            )
            .then(pl.lit("Opp_Safety"))
            .when(
                (pl.col("scoring_event") == "Extra_Point")
                & (pl.col("posteam") == pl.col("scoring_play_team"))
            )
            .then(pl.lit("Extra_Point"))
            .when(
                (pl.col("scoring_event") == "Extra_Point")
                & (pl.col("posteam") != pl.col("scoring_play_team"))
            )
            .then(pl.lit("Opp_Two_Point_Conversion"))
            .when(
                (pl.col("scoring_event") == "Two_Point_Conversion")
                & (pl.col("posteam") == pl.col("scoring_play_team"))
            )
            .then(pl.lit("Two_Point_Conversion"))
            .when(
                (pl.col("scoring_event") == "Two_Point_Conversion")
                & (pl.col("posteam") != pl.col("scoring_play_team"))
            )
            .then(pl.lit("Opp_Two_Point_Conversion"))
            .when(pl.col("scoring_event") == "No_Score")
            .then(pl.lit("No_Score"))
            .otherwise(pl.lit(None))
        )
        # Put the play's own drive_id in for No_Score rows so a non-scoring drive isn't
        # attributed to whichever drive eventually scores.
        .with_columns(
            Drive_Score_Half=pl.when(pl.col("Next_Score_Half") == "No_Score")
            .then(pl.col("drive_id"))
            .otherwise(pl.col("score_drive"))
        )
        .with_columns(max_play_id=pl.col("play_id").max().over(["game_id", "half"]))
    )
    return output


def prepare_wp_data(df: pl.DataFrame) -> pl.DataFrame:
    """Port of `helper_add_hudl_mutations.prepare_wp_data`.

    Adds `index` (if absent), `half_end`, `game_end`, `helper_one`, `play_id_half`,
    `play_time`, `half_seconds_remaining` (SYNTHETIC), `game_seconds_remaining`,
    `elapsed_share`, `Diff_Time_Ratio`, `start_posteam`, `receive_2h_ko`.

    `half_seconds_remaining` is SYNTHETIC: 1200 seconds per half, decremented per play by
    `1200 / max(play_id_half)`, because real Hudl clock data has not been delivered
    (REQ-S1-02 pending; see module docstring). `game_seconds_remaining` mirrors the same
    synthetic assumption starting from 2400 seconds per game.
    """
    output = (
        _mark_half_end(df)
        .with_columns(helper_one=pl.lit(1))
        .with_columns(play_id_half=pl.col("helper_one").cum_sum().over(["game_id", "half"]))
        .with_columns(
            play_time=pl.when(pl.col("play_id_half") == 1)
            .then(pl.lit(0))
            .otherwise(1200 / pl.col("play_id_half").max())
            .over(["game_id", "half"])
        )
        .with_columns(
            half_seconds_remaining=(
                1200 - pl.col("play_time").cum_sum().over(["game_id", "half"])
            )
        )
        .with_columns(
            game_seconds_remaining=pl.when(pl.col("half") == 2)
            .then(pl.col("half_seconds_remaining"))
            .otherwise(2400 - pl.col("play_time").cum_sum().over("game_id"))
        )
        .with_columns(elapsed_share=(2400 - pl.col("game_seconds_remaining")) / 2400)
        .with_columns(
            Diff_Time_Ratio=pl.col("score_differential")
            / (-4 * pl.col("elapsed_share")).exp()
        )
        # Forward-filled across the whole game (not reset per half) so `start_posteam`
        # always names the team that had the ball on the game's first play, matching the
        # module-wide no-cross-game-leakage / .over("game_id") invariant.
        .with_columns(
            start_posteam=pl.when(pl.col("play_id") == 1)
            .then(pl.col("posteam"))
            .otherwise(pl.lit(None))
            .forward_fill()
            .over("game_id")
        )
        .with_columns(
            receive_2h_ko=pl.when(pl.col("start_posteam") == pl.col("posteam"))
            .then(pl.lit(0))
            .otherwise(pl.lit(1))
        )
    )
    return output
