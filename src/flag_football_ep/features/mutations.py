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

# Hard-coded PAT success-rate assumptions from the notebook (1-pt try from the 5 yard line,
# 2-pt try from the 10). REQ-S1-10 (phase 1.3) replaces these with empirical break-even
# estimates computed from the data; until then this is the single place to change them.
PAT_BASELINE_ONE_POINT = 0.5
PAT_BASELINE_TWO_POINT = 0.92

# Order matters: it is the order the EP model's predict() output columns must land in.
EP_PROBABILITY_COLUMNS = (
    "Touchdown_Prob",
    "Opp_Touchdown_Prob",
    "Safety_Prob",
    "Opp_Safety_Prob",
    "No_Score_Prob",
)
WP_PROBABILITY_COLUMN = "wp"


class MissingFeatureColumns(ValueError):
    """Raised when a required EP/WP probability column is missing from the input frame.

    The notebook let this surface as an opaque polars ColumnNotFound; this names every
    missing column instead.
    """


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


def add_ep_variables(df: pl.DataFrame) -> pl.DataFrame:
    """Port of `helper_add_ep_wp.add_ep_variables`.

    Requires the five EP probability columns (`EP_PROBABILITY_COLUMNS`) already present --
    typically the model's `.predict()` output hstacked onto the `prepare_ep_data` frame.
    Computes `ExpPts`, `ep`, `epa` and the home/away cumulative EPA totals. Pure: returns a
    new frame, does not mutate `df`.
    """
    missing = [c for c in EP_PROBABILITY_COLUMNS if c not in df.columns]
    if missing:
        raise MissingFeatureColumns(
            f"add_ep_variables: missing required probability column(s): {', '.join(missing)}"
        )

    df = (
        df.with_columns(
            ExpPts=(
                (0 * pl.col("No_Score_Prob"))
                + (2 * pl.col("Safety_Prob"))
                + (6 * pl.col("Touchdown_Prob"))
                + (-2 * pl.col("Opp_Safety_Prob"))
                + (-6 * pl.col("Opp_Touchdown_Prob"))
            )
        )
        .with_columns(
            ep=pl.col("ExpPts"),
            tmp_posteam=pl.col("posteam"),
        )
        .with_columns(
            ep=pl.col("ep").backward_fill(),
            tmp_posteam=pl.col("tmp_posteam").backward_fill(),
        )
        # Non-scoring plays: home-perspective forward difference in ep.
        .with_columns(
            home_ep=pl.when(pl.col("tmp_posteam") == pl.col("home_team"))
            .then(pl.col("ep"))
            .otherwise(-pl.col("ep"))
        )
        .with_columns(home_ep_after=pl.col("home_ep").shift(-1))
        .with_columns(
            home_epa=pl.when(pl.col("interception") == 1)
            .then(-(pl.col("home_ep_after") - pl.col("home_ep")))
            .otherwise(pl.col("home_ep_after") - pl.col("home_ep"))
        )
        .with_columns(
            epa=pl.when(pl.col("tmp_posteam") == pl.col("home_team"))
            .then(pl.col("home_epa"))
            .otherwise(-pl.col("home_epa"))
        )
        # Touchdown: real 6 points, not ep_after.
        .with_columns(
            epa=pl.when(
                (pl.col("scoring_play_team").is_not_null()) & (pl.col("touchdown") == 1)
            )
            .then(
                pl.when(pl.col("scoring_play_team") == pl.col("posteam"))
                .then(6 - pl.col("ep"))
                .otherwise(-6 - pl.col("ep"))
            )
            .otherwise(pl.col("epa"))
        )
        # Offense extra-point. PAT_BASELINE_ONE_POINT is the notebook's hard-coded
        # assumption (try from the 5); REQ-S1-10 (phase 1.3) replaces it with an empirical
        # break-even estimate.
        .with_columns(
            epa=pl.when(pl.col("one_point_conv_success") == 1)
            .then(1 - pl.lit(PAT_BASELINE_ONE_POINT))
            .otherwise(pl.col("epa"))
        )
        # Offense two-point conversion. PAT_BASELINE_TWO_POINT is the notebook's hard-coded
        # assumption (try from the 10); REQ-S1-10 (phase 1.3) replaces it.
        .with_columns(
            epa=pl.when(pl.col("two_point_conv_success") == 1)
            .then(2 - pl.lit(PAT_BASELINE_TWO_POINT))
            .otherwise(pl.col("epa"))
        )
        # Failed PAT (1 point; assumption: yards_to_go <= 5 is a 1-pt try).
        .with_columns(
            epa=pl.when(
                (pl.col("down") == 0)
                & (pl.col("yards_to_go") <= 5)
                & (pl.col("one_point_conv_success") == 0)
            )
            .then(0 - pl.lit(PAT_BASELINE_ONE_POINT))
            .otherwise(pl.col("epa"))
        )
        # Failed PAT (2 points; assumption: yards_to_go > 5 is a 2-pt try).
        .with_columns(
            epa=pl.when(
                (pl.col("down") == 0)
                & (pl.col("yards_to_go") > 5)
                & (pl.col("two_point_conv_success") == 0)
            )
            .then(0 - pl.lit(PAT_BASELINE_TWO_POINT))
            .otherwise(pl.col("epa"))
        )
        # Opponent scores defensive two-point conversion.
        .with_columns(
            epa=pl.when(pl.col("defensive_two_point_conv") == 1)
            .then(-2 - pl.col("ep"))
            .otherwise(pl.col("epa"))
        )
        # Safety.
        .with_columns(
            epa=pl.when(
                (pl.col("scoring_play_team").is_not_null())
                & (pl.col("scoring_play_team") == pl.col("posteam"))
                & (pl.col("safety") == 1)
            )
            .then(2 - pl.col("ep"))
            .when(
                (pl.col("scoring_play_team").is_not_null())
                & (pl.col("scoring_play_team") == pl.col("defteam"))
                & (pl.col("safety") == 1)
            )
            .then(-2 - pl.col("ep"))
            .otherwise(pl.col("epa"))
        )
        # End-of-half play with no scoring: epa is just 0 minus starting ep.
        .with_columns(
            epa=pl.when(
                (pl.col("half_end") == 1)
                & (pl.col("scoring_play") == 0)
                & (pl.col("play_type").is_not_null())
            )
            .then(0 - pl.col("ep"))
            .otherwise(pl.col("epa"))
        )
        # Half-end epa/ep are undefined for the first half (game-end handled next).
        .with_columns(
            epa=pl.when((pl.col("half_end") == 1) & (pl.col("half") == 1))
            .then(pl.lit(None))
            .otherwise(pl.col("epa"))
        )
        .with_columns(
            epa=pl.when(pl.col("game_end") == 1).then(pl.lit(None)).otherwise(pl.col("epa"))
        )
        .with_columns(
            ep=pl.when((pl.col("half_end") == 1) & (pl.col("half") == 1))
            .then(pl.lit(None))
            .otherwise(pl.col("ep"))
        )
        .with_columns(
            ep=pl.when(pl.col("game_end") == 1).then(pl.lit(None)).otherwise(pl.col("ep"))
        )
        .with_columns(
            home_team_epa=pl.when(pl.col("home_team") == pl.col("posteam"))
            .then(pl.col("epa"))
            .otherwise(-pl.col("epa")),
            away_team_epa=pl.when(pl.col("away_team") == pl.col("posteam"))
            .then(pl.col("epa"))
            .otherwise(-pl.col("epa")),
        )
        .with_columns(
            home_team_epa=pl.when(pl.col("home_team_epa").is_null())
            .then(pl.lit(0))
            .otherwise(pl.col("home_team_epa")),
            away_team_epa=pl.when(pl.col("away_team_epa").is_null())
            .then(pl.lit(0))
            .otherwise(pl.col("away_team_epa")),
        )
        .with_columns(
            total_home_epa=pl.col("home_team_epa").cum_sum().over("game_id"),
            total_away_epa=pl.col("away_team_epa").cum_sum().over("game_id"),
        )
    )
    return df


def add_wp_variables(df: pl.DataFrame) -> pl.DataFrame:
    """Port of `helper_add_ep_wp.add_wp_variables`.

    Requires `WP_PROBABILITY_COLUMN` ("wp") already present -- typically the model's
    `.predict()` output. Computes `home_wp`, `away_wp`, `def_wp`, `wpa`,
    `home_wp_post`/`away_wp_post` and the home/away cumulative WP/WPA totals. Pure: returns
    a new frame, does not mutate `df`.
    """
    if WP_PROBABILITY_COLUMN not in df.columns:
        raise MissingFeatureColumns(
            f"add_wp_variables: missing required probability column: {WP_PROBABILITY_COLUMN!r}"
        )

    df = (
        df.with_columns(tmp_posteam=pl.col("posteam"))
        .with_columns(
            wp=pl.col("wp").backward_fill(),
            tmp_posteam=pl.col("tmp_posteam").backward_fill(),
        )
        .with_columns(
            home_wp=pl.when(pl.col("tmp_posteam") == pl.col("home_team"))
            .then(pl.col("wp"))
            .otherwise(1 - pl.col("wp"))
        )
        .with_columns(
            final_value=pl.when(pl.col("home_team_score") > pl.col("away_team_score"))
            .then(pl.lit(1))
            .when(pl.col("away_team_score") > pl.col("home_team_score"))
            .then(pl.lit(0))
            .when(pl.col("home_team_score") == pl.col("away_team_score"))
            .then(pl.lit(0.5))
        )
        .with_columns(
            home_wp=pl.when(pl.col("game_end") == 1)
            .then(pl.col("final_value"))
            .otherwise(pl.col("home_wp"))
        )
        .with_columns(away_wp=1 - pl.col("home_wp"))
        .with_columns(def_wp=1 - pl.col("wp"))
        .with_columns(home_wp_after=pl.col("home_wp").shift(-1))
        .with_columns(home_wpa=pl.col("home_wp_after") - pl.col("home_wp"))
        .with_columns(
            wpa=pl.when(pl.col("tmp_posteam") == pl.col("home_team"))
            .then(pl.col("home_wpa"))
            .otherwise(-pl.col("home_wpa"))
        )
        .with_columns(
            wpa=pl.when(pl.col("game_end") == 1).then(pl.lit(None)).otherwise(pl.col("wpa"))
        )
        .with_columns(
            home_wp_post=pl.when(pl.col("posteam") == pl.col("home_team"))
            .then(pl.col("home_wp") + pl.col("wpa"))
            .otherwise(pl.col("home_wp") - pl.col("wpa")),
            away_wp_post=pl.when(pl.col("posteam") == pl.col("away_team"))
            .then(pl.col("away_wp") + pl.col("wpa"))
            .otherwise(pl.col("away_wp") - pl.col("wpa")),
        )
        .with_columns(
            home_team_wpa=pl.when(pl.col("posteam") == pl.col("home_team"))
            .then(pl.col("wpa"))
            .otherwise(-pl.col("wpa")),
            away_team_wpa=pl.when(pl.col("posteam") == pl.col("away_team"))
            .then(pl.col("wpa"))
            .otherwise(-pl.col("wpa")),
        )
        .with_columns(
            home_team_wpa=pl.when(pl.col("home_team_wpa").is_null())
            .then(pl.lit(0))
            .otherwise(pl.col("home_team_wpa")),
            away_team_wpa=pl.when(pl.col("away_team_wpa").is_null())
            .then(pl.lit(0))
            .otherwise(pl.col("away_team_wpa")),
        )
        .with_columns(
            total_home_wp=pl.col("home_wp_post").cum_sum().over("game_id"),
            total_away_wp=pl.col("away_wp_post").cum_sum().over("game_id"),
            total_home_wpa=pl.col("home_team_wpa").cum_sum().over("game_id"),
            total_away_wpa=pl.col("away_team_wpa").cum_sum().over("game_id"),
        )
    )
    return df
