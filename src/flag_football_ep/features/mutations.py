"""Feature engineering: EP/WP data preparation, EP/WP variables, and model training frames.

Ported from `Python/helper_add_hudl_mutations.py` (`prepare_ep_data`, `prepare_wp_data`),
`Python/helper_add_ep_wp.py` (`add_ep_variables`, `add_wp_variables`) and
`Python/helper_add_model_mutations.py` (`make_ep_model_mutations`, `make_wp_model_mutations`).

This is a behaviour-preserving port: every derived column, formula and constant matches the
frozen notebook baseline (see `tests/fixtures/baseline_manifest.json`), with one exception:
REQ-S1-10 has landed -- PAT baselines are now estimated from the corpus via
`estimate_pat_baselines` below, instead of the notebook's fixed 50%/92% assumptions.
GroupKFold/LOGO evaluation lives in `model/evaluate.py` (REQ-S1-07) and calibration in
`model/train.py` (REQ-S1-08).

`half_seconds_remaining` (`prepare_wp_data`) is SYNTHETIC: it is derived as
`1200 / max(play_id_half)` per half because real Hudl clock data has not been delivered
(REQ-S1-02 pending). Phase 1.4 gates the WP charts on this flag -- never treat it as a real
game clock, and do not source it from `game_clock_ms` here.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import polars as pl
from scipy.stats import binomtest

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


class DegenerateWeightRange(ValueError):
    """Raised when Drive_Score_Dist_W or ScoreDiff_W would divide by a zero range.

    These sample weights (RESEARCH Pitfall 4) must be computed on the full training corpus,
    never per game or on any subset with no variation in Drive_Score_Dist / score_differential
    -- a zero range silently produced NaN/inf weights in the notebook.
    """


class InsufficientPatAttempts(ValueError):
    """Raised when a PAT baseline would be estimated from zero attempts.

    A PAT success rate must never be estimated from zero attempts -- silently returning a NaN
    or 0.0 rate would distort every PAT `epa` in the corpus. `estimate_pat_baselines` is
    intended to run on the pooled full-corpus frame, never on a per-game or per-source subset
    that might have zero PAT-shaped rows.
    """


@dataclass(frozen=True)
class PatBaselines:
    """Empirical PAT success rates with attempt counts and confidence intervals (REQ-S1-10).

    Every PAT `epa` adjustment in `add_ep_variables` uses these rates instead of a fixed
    constant, so a rate estimated from a handful of tries is never quoted as if it were
    precise -- see `one_point_ci`/`two_point_ci`.
    """

    one_point_rate: float
    one_point_attempts: int
    one_point_successes: int
    one_point_ci: tuple[float, float] | None
    two_point_rate: float
    two_point_attempts: int
    two_point_successes: int
    two_point_ci: tuple[float, float] | None

    @classmethod
    def legacy_notebook(cls) -> "PatBaselines":
        """The pre-REQ-S1-10 hard-coded notebook assumptions (1-pt 0.5, 2-pt 0.92), retained
        only so regression tests can pin historical `epa` arithmetic -- production code must
        never call this.
        """
        return cls(
            one_point_rate=0.5,
            one_point_attempts=0,
            one_point_successes=0,
            one_point_ci=None,
            two_point_rate=0.92,
            two_point_attempts=0,
            two_point_successes=0,
            two_point_ci=None,
        )


def estimate_pat_baselines(plays: pl.DataFrame) -> PatBaselines:
    """Estimate pooled PAT success rates from the full canonical corpus (REQ-S1-10).

    A 1-pt attempt is a row with `down == 0` and `yards_to_go <= 5`; a 2-pt attempt is a row
    with `down == 0` and `yards_to_go > 5` -- the same predicates `add_ep_variables` uses to
    identify a failed try. Returns the *pooled overall* success rate per PAT type across the
    whole input frame -- CONTEXT locks the estimator as pooled, not per-source or per-season;
    per-source counts are reported by the chart and the data profile, not baked into this
    baseline. Each rate's confidence interval comes from
    `scipy.stats.binomtest(...).proportion_ci()` (Clopper-Pearson), not a hand-rolled normal
    approximation -- RESEARCH flags that this corpus's PAT attempt counts are small
    (tens-to-low-hundreds) and its rates are often extreme (>90%), exactly where a normal
    approximation breaks down. Raises `InsufficientPatAttempts` naming the PAT type and the
    observed count when a type has zero attempts.
    """
    one_point_attempts_df = plays.filter(
        (pl.col("down") == 0) & (pl.col("yards_to_go") <= 5)
    )
    two_point_attempts_df = plays.filter(
        (pl.col("down") == 0) & (pl.col("yards_to_go") > 5)
    )

    one_point_attempts = one_point_attempts_df.height
    two_point_attempts = two_point_attempts_df.height

    if one_point_attempts == 0:
        raise InsufficientPatAttempts(
            "estimate_pat_baselines: 0 one-point PAT attempts observed (down == 0 and "
            "yards_to_go <= 5) -- a PAT baseline must never be estimated from zero attempts"
        )
    if two_point_attempts == 0:
        raise InsufficientPatAttempts(
            "estimate_pat_baselines: 0 two-point PAT attempts observed (down == 0 and "
            "yards_to_go > 5) -- a PAT baseline must never be estimated from zero attempts"
        )

    one_point_successes = int(
        one_point_attempts_df.filter(pl.col("one_point_conv_success") == 1).height
    )
    two_point_successes = int(
        two_point_attempts_df.filter(pl.col("two_point_conv_success") == 1).height
    )

    one_point_rate = one_point_successes / one_point_attempts
    two_point_rate = two_point_successes / two_point_attempts

    one_point_ci = binomtest(one_point_successes, one_point_attempts).proportion_ci()
    two_point_ci = binomtest(two_point_successes, two_point_attempts).proportion_ci()

    return PatBaselines(
        one_point_rate=one_point_rate,
        one_point_attempts=one_point_attempts,
        one_point_successes=one_point_successes,
        one_point_ci=(one_point_ci.low, one_point_ci.high),
        two_point_rate=two_point_rate,
        two_point_attempts=two_point_attempts,
        two_point_successes=two_point_successes,
        two_point_ci=(two_point_ci.low, two_point_ci.high),
    )


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


def add_ep_variables(df: pl.DataFrame, *, pat_baselines: PatBaselines) -> pl.DataFrame:
    """Port of `helper_add_ep_wp.add_ep_variables`.

    Requires the five EP probability columns (`EP_PROBABILITY_COLUMNS`) already present --
    typically the model's `.predict()` output hstacked onto the `prepare_ep_data` frame.
    Computes `ExpPts`, `ep`, `epa` and the home/away cumulative EPA totals. Pure: returns a
    new frame, does not mutate `df`.

    `pat_baselines` is a required keyword argument (REQ-S1-10) -- every PAT `epa` branch uses
    its empirically estimated rates; there is no default and no production fallback to a
    hard-coded constant. Pass `PatBaselines.legacy_notebook()` only in tests that pin
    pre-REQ-S1-10 historical `epa` arithmetic.
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
        # Scoped by game_id so a game's trailing null ep/tmp_posteam rows do not inherit the
        # next game's values (a multi-game frame concatenates games back-to-back).
        .with_columns(
            ep=pl.col("ep").backward_fill().over("game_id"),
            tmp_posteam=pl.col("tmp_posteam").backward_fill().over("game_id"),
        )
        # Non-scoring plays: home-perspective forward difference in ep.
        .with_columns(
            home_ep=pl.when(pl.col("tmp_posteam") == pl.col("home_team"))
            .then(pl.col("ep"))
            .otherwise(-pl.col("ep"))
        )
        # Scoped by game_id so shift(-1) never reads the next game's first play.
        .with_columns(home_ep_after=pl.col("home_ep").shift(-1).over("game_id"))
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
        # Offense extra-point. Uses the empirical 1-pt success rate estimated from the corpus
        # (REQ-S1-10, `estimate_pat_baselines`) instead of the notebook's hard-coded 0.5.
        .with_columns(
            epa=pl.when(pl.col("one_point_conv_success") == 1)
            .then(1 - pl.lit(pat_baselines.one_point_rate))
            .otherwise(pl.col("epa"))
        )
        # Offense two-point conversion. Uses the empirical 2-pt success rate estimated from
        # the corpus (REQ-S1-10, `estimate_pat_baselines`) instead of the notebook's
        # hard-coded 0.92.
        .with_columns(
            epa=pl.when(pl.col("two_point_conv_success") == 1)
            .then(2 - pl.lit(pat_baselines.two_point_rate))
            .otherwise(pl.col("epa"))
        )
        # Failed PAT (1 point; assumption: yards_to_go <= 5 is a 1-pt try). Uses the same
        # empirical 1-pt success rate as the success branch above (REQ-S1-10).
        .with_columns(
            epa=pl.when(
                (pl.col("down") == 0)
                & (pl.col("yards_to_go") <= 5)
                & (pl.col("one_point_conv_success") == 0)
            )
            .then(0 - pl.lit(pat_baselines.one_point_rate))
            .otherwise(pl.col("epa"))
        )
        # Failed PAT (2 points; assumption: yards_to_go > 5 is a 2-pt try). Uses the same
        # empirical 2-pt success rate as the success branch above (REQ-S1-10).
        .with_columns(
            epa=pl.when(
                (pl.col("down") == 0)
                & (pl.col("yards_to_go") > 5)
                & (pl.col("two_point_conv_success") == 0)
            )
            .then(0 - pl.lit(pat_baselines.two_point_rate))
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
        # Scoped by game_id so a game's trailing null wp/tmp_posteam rows do not inherit the
        # next game's values (a multi-game frame concatenates games back-to-back).
        .with_columns(
            wp=pl.col("wp").backward_fill().over("game_id"),
            tmp_posteam=pl.col("tmp_posteam").backward_fill().over("game_id"),
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
        # Scoped by game_id so shift(-1) never reads the next game's first play.
        .with_columns(home_wp_after=pl.col("home_wp").shift(-1).over("game_id"))
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


def make_ep_model_mutations(df: pl.DataFrame, selected_columns: Sequence[str]) -> pl.DataFrame:
    """Port of `helper_add_model_mutations.make_ep_model_mutations`.

    Adds `label` (0..4 from `Next_Score_Half`), `down0`..`down4`, `Drive_Score_Dist`,
    `Drive_Score_Dist_W`, `ScoreDiff_W`, `Total_W`, `Total_W_Scaled`; filters null
    `yardline_50`/`yards_to_go`; selects `selected_columns` in that exact order.

    Must be called on the full training corpus, never per-game or on any subset with no
    variation in `Drive_Score_Dist` / `score_differential` -- `DegenerateWeightRange` is
    raised instead of silently emitting NaN/inf sample weights (RESEARCH Pitfall 4).
    """
    df = (
        df.with_columns(
            label=pl.when(pl.col("Next_Score_Half") == "Touchdown")
            .then(pl.lit(0))
            .when(pl.col("Next_Score_Half") == "Opp_Touchdown")
            .then(pl.lit(1))
            # .when(pl.col("Next_Score_Half") == "Extra_Point").then(pl.lit(2))
            # .when(pl.col("Next_Score_Half") == "Two_Point_Conversion").then(pl.lit(3))
            # .when(pl.col("Next_Score_Half") == "Opp_Two_Point_Conversion").then(pl.lit(4))
            .when(pl.col("Next_Score_Half") == "Safety")
            .then(pl.lit(2))
            .when(pl.col("Next_Score_Half") == "Opp_Safety")
            .then(pl.lit(3))
            .when(pl.col("Next_Score_Half") == "No_Score")
            .then(pl.lit(4))
            .otherwise(pl.lit(None))
        )
        .with_columns(
            down0=pl.when(pl.col("down") == 0).then(pl.lit(1)).otherwise(pl.lit(0)),
            down1=pl.when(pl.col("down") == 1).then(pl.lit(1)).otherwise(pl.lit(0)),
            down2=pl.when(pl.col("down") == 2).then(pl.lit(1)).otherwise(pl.lit(0)),
            down3=pl.when(pl.col("down") == 3).then(pl.lit(1)).otherwise(pl.lit(0)),
            down4=pl.when(pl.col("down") == 4).then(pl.lit(1)).otherwise(pl.lit(0)),
        )
        .with_columns(Drive_Score_Dist=pl.col("Drive_Score_Half") - pl.col("drive_id"))
    )

    drive_score_dist_max = df["Drive_Score_Dist"].max()
    drive_score_dist_min = df["Drive_Score_Dist"].min()
    if drive_score_dist_max == drive_score_dist_min:
        raise DegenerateWeightRange(
            "Drive_Score_Dist has zero range across the input frame (max == min == "
            f"{drive_score_dist_max!r}). Drive_Score_Dist_W must be computed on the full "
            "training corpus, never per game or on a subset with no variation."
        )

    score_diff_abs_max = df["score_differential"].abs().max()
    score_diff_abs_min = df["score_differential"].abs().min()
    if score_diff_abs_max == score_diff_abs_min:
        raise DegenerateWeightRange(
            "score_differential has zero absolute range across the input frame (max == min "
            f"== {score_diff_abs_max!r}). ScoreDiff_W must be computed on the full training "
            "corpus, never per game or on a subset with no variation."
        )

    model_data = (
        df.with_columns(
            Drive_Score_Dist_W=(
                pl.col("Drive_Score_Dist").max() - pl.col("Drive_Score_Dist")
            )
            / (pl.col("Drive_Score_Dist").max() - pl.col("Drive_Score_Dist").min())
        )
        .with_columns(
            ScoreDiff_W=(
                pl.col("score_differential").abs().max() - pl.col("score_differential").abs()
            )
            / (
                pl.col("score_differential").abs().max()
                - pl.col("score_differential").abs().min()
            )
        )
        .with_columns(Total_W=pl.col("Drive_Score_Dist_W") + pl.col("ScoreDiff_W"))
        .with_columns(
            Total_W_Scaled=(pl.col("Total_W") - pl.col("Total_W").min())
            / (pl.col("Total_W").max() - pl.col("Total_W").min())
        )
        .filter(
            pl.col("yardline_50").is_not_null(),
            pl.col("yards_to_go").is_not_null(),
        )
        .select(list(selected_columns))
    )
    return model_data


def make_wp_model_mutations(df: pl.DataFrame, selected_columns: Sequence[str]) -> pl.DataFrame:
    """Port of `helper_add_model_mutations.make_wp_model_mutations`.

    Adds `Winner` (backward-filled from the game_end scores) and `label` (1 when `posteam`
    equals `Winner`, else 0 -- a tie yields label 0 for both teams since `Winner` is "TIE"
    for every posteam); selects `selected_columns` in that exact order.

    Fixed vs. the notebook: `Winner` is set to the actual team name (`pl.col("home_team")` /
    `pl.col("away_team")`) instead of the literal strings `"home_team"`/`"away_team"`, which
    could never match `posteam` and would have made every label 0. `Winner`'s backward_fill
    is scoped `.over("game_id")` so a multi-game frame does not leak one game's winner into
    another's rows.
    """
    model_data = (
        df.with_columns(
            Winner=pl.when(
                (pl.col("game_end") == 1)
                & (pl.col("home_team_score") > pl.col("away_team_score"))
            )
            .then(pl.col("home_team"))
            .when(
                (pl.col("game_end") == 1)
                & (pl.col("home_team_score") < pl.col("away_team_score"))
            )
            .then(pl.col("away_team"))
            .when(
                (pl.col("game_end") == 1)
                & (pl.col("home_team_score") == pl.col("away_team_score"))
            )
            .then(pl.lit("TIE"))
            .otherwise(pl.lit(None))
        )
        .with_columns(Winner=pl.col("Winner").backward_fill().over("game_id"))
        .with_columns(
            label=pl.when(pl.col("posteam") == pl.col("Winner"))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
        )
        .select(list(selected_columns))
    )
    return model_data
