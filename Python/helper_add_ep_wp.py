import polars as pl

def add_ep_variables(df: pl.DataFrame):
    """This function adds all needed variables for Expected Points to calculate them in a correct way (e.g. TD not substracted by ep_after, but real TD points). 

    Args:
        df (pl.DataFrame): Expects a Polars DataFrame with all raw datas and the corresponding probabilities from the ep-Model.

    Returns:
        _type_: Polars DataFrame with Expected Points Columns
    """
    df = (df.with_columns(
                  ExpPts = 
                  (0*pl.col("No_Score_Prob")) +
                #   (1*pl.col("Extra_Point_Prob")) +
                #   (2*pl.col("Two_Point_Conversion_Prob")) +
                  (2*pl.col("Safety_Prob")) +
                  (6*pl.col("Touchdown_Prob")) +
                #   (-2*pl.col("Opp_Two_Point_Conversion_Prob")) +
                  (-2*pl.col("Opp_Safety_Prob")) +
                  (-6*pl.col("Opp_Touchdown_Prob"))
                  )
                #   .select(pl.exclude("^.*Prob$"))
                #   .with_columns(epa = pl.col("ep") - pl.col("ep_before"))
                  .with_columns(# Now conditionally assign the EPA, first for possession team
                        # touchdowns:
                        ep = pl.col("ExpPts"),
                        tmp_posteam = pl.col("posteam")                      
                  )
                  .with_columns(
                        ep = pl.col("ep").backward_fill(),
                        tmp_posteam = pl.col("tmp_posteam").backward_fill()
                  )
                  # get epa for non-scoring plays
                  .with_columns(home_ep = pl.when(pl.col("tmp_posteam") == pl.col("home_team")).then(pl.col("ep")).otherwise(-pl.col("ep")))
                  .with_columns(home_ep_after = pl.col("home_ep").shift(-1))
                  .with_columns(home_epa = 
                                pl.when(pl.col("interception") == 1)
                                .then(- (pl.col("home_ep_after") - pl.col("home_ep")))
                                .otherwise(pl.col("home_ep_after") - pl.col("home_ep"))
                  )
                  .with_columns(epa = pl.when(pl.col("tmp_posteam") == pl.col("home_team")).then(pl.col("home_epa")).otherwise(-pl.col("home_epa")))
                  # td
                  .with_columns(
                        epa = 
                        pl.when((pl.col("scoring_play_team").is_not_null()) & (pl.col("touchdown") == 1))
                        .then(
                              pl.when(pl.col("scoring_play_team") == pl.col("posteam"))
                              .then(6 - pl.col("ep"))
                              .otherwise(-6 - pl.col("ep"))
                              )
                              .otherwise(pl.col("epa")))
                  # Offense extra-point:
                  .with_columns(
                        epa = 
                        pl.when(pl.col("one_point_conv_success") == 1)
                        .then(1 - pl.lit(0.5)) # Tries von der 5 Yard Linie sind zu 50% gut (1 * 0.5 = 0.5).
                        .otherwise(pl.col("epa"))
                  )
                  # Offense two-point conversion:
                  .with_columns(
                        epa = 
                        pl.when(pl.col("two_point_conv_success") == 1)
                        .then(2 - pl.lit(0.92)) # Tries von der 10 Yard Linie sind zu 46% gut (2 * 0.46 = 0.92). 
                        .otherwise(pl.col("epa"))
                  )
                  # Failed PAT (1):
                  .with_columns(
                        epa = 
                        pl.when((pl.col("down") == 0) & (pl.col("yards_to_go") <= 5) & ((pl.col("one_point_conv_success") == 0))) # Annahme: Unter 5 ist 1Pt.
                        .then(0 - pl.lit(0.5)) # Tries von der 5 Yard Linie sind zu 50% gut (1 * 0.5 = 0.5).
                        .otherwise(pl.col("epa"))
                  )
                # Failed PAT (2):
                  .with_columns(
                        epa = 
                        pl.when((pl.col("down") == 0) & (pl.col("yards_to_go") > 5) & ((pl.col("two_point_conv_success") == 0))) # Annahme: Über 5 ist 2Pt.
                        .then(0 - pl.lit(0.92)) # Tries von der 10 Yard Linie sind zu 46% gut (2 * 0.46 = 0.92). 
                        .otherwise(pl.col("epa"))
                  )
                  # Opponent scores defensive 2 point:
                  .with_columns(
                        epa = 
                        pl.when(pl.col("defensive_two_point_conv") == 1)
                        .then(-2 - pl.col("ep"))
                        .otherwise(pl.col("epa"))
                  )
                  # Safety:
                  .with_columns(
                        epa = 
                        pl.when((pl.col("scoring_play_team").is_not_null()) & (pl.col("scoring_play_team") == pl.col("posteam")) & (pl.col("safety") == 1))
                        .then(2 - pl.col("ep"))
                        .when((pl.col("scoring_play_team").is_not_null()) & (pl.col("scoring_play_team") == pl.col("defteam")) & (pl.col("safety") == 1))
                        .then(-2 - pl.col("ep"))
                        .otherwise(pl.col("epa"))
                  )
                  # Create columns with cumulative epa totals for both teams:
                  .with_columns(
                      # Change epa for plays occurring at end of half with no scoring
                      # plays to be just the difference between 0 and starting ep:
                      epa = pl.when((pl.col("half_end") == 1) & (pl.col("scoring_play") == 0) & (pl.col("play_type").is_not_null()))
                      .then(0 - pl.col("ep"))
                      .otherwise(pl.col("epa"))
                  )
                  .with_columns(
                      # half end epa
                      epa =
                      pl.when((pl.col("half_end") == 1) & (pl.col("half") == 1))
                      .then(pl.lit(None))
                      .otherwise(pl.col("epa"))
                  )
                  .with_columns(
                      # game end epa
                      epa =
                      pl.when(pl.col("game_end") == 1)
                      .then(pl.lit(None))
                      .otherwise(pl.col("epa"))
                  )
                  .with_columns(
                      # half end ep
                      ep =
                      pl.when((pl.col("half_end") == 1) & (pl.col("half") == 1))
                      .then(pl.lit(None))
                      .otherwise(pl.col("ep"))
                  )
                  .with_columns(
                      # game end ep
                      ep =
                      pl.when(pl.col("game_end") == 1)
                      .then(pl.lit(None))
                      .otherwise(pl.col("ep"))
                  )
                  .with_columns(
                      home_team_epa = 
                      pl.when(pl.col("home_team") == pl.col("posteam"))
                      .then(pl.col("epa"))
                      .otherwise(-pl.col("epa")),
                      away_team_epa = 
                      pl.when(pl.col("away_team") == pl.col("posteam"))
                      .then(pl.col("epa"))
                      .otherwise(-pl.col("epa"))
                  )
                  .with_columns(
                      home_team_epa = 
                      pl.when(pl.col("home_team_epa").is_null())
                      .then(pl.lit(0))
                      .otherwise(pl.col("home_team_epa")),
                      away_team_epa = 
                      pl.when(pl.col("away_team_epa").is_null())
                      .then(pl.lit(0))
                      .otherwise(pl.col("away_team_epa")),
                  )
                  .with_columns(
                      total_home_epa = pl.col('home_team_epa').cum_sum().over(["game_id"]),
                      total_away_epa = pl.col('away_team_epa').cum_sum().over(["game_id"])
                  )
                #   # Same thing but separating passing and rushing:
                #   # Rushing
                #   .with_columns(
                #       home_team_rush_epa = pl.when(pl.col("play_type") == "run").then(pl.col('home_team_epa')).otherwise(0),
                #       away_team_rush_epa = pl.when(pl.col("play_type") == "run").then(pl.col('away_team_epa')).otherwise(0)
                #   )
                #   .with_columns(
                #       home_team_rush_epa = pl.when(pl.col("home_team_rush_epa").is_not_null()).then(pl.col('home_team_rush_epa')).otherwise(0),
                #       away_team_rush_epa = pl.when(pl.col("away_team_rush_epa").is_not_null()).then(pl.col('away_team_rush_epa')).otherwise(0)
                #   )
                #   .with_columns(
                #       total_home_rush_epa = pl.col('home_team_rush_epa').cum_sum().over(["game_id"]),
                #       total_away_rush_epa = pl.col('away_team_rush_epa').cum_sum().over(["game_id"])
                #   )
                #   # Passing
                #   .with_columns(
                #       home_team_pass_epa = pl.when(pl.col("play_type") == "pass").then(pl.col('home_team_epa')).otherwise(0),
                #       away_team_pass_epa = pl.when(pl.col("play_type") == "pass").then(pl.col('away_team_epa')).otherwise(0)
                #   )
                #   .with_columns(
                #       home_team_pass_epa = pl.when(pl.col("home_team_pass_epa").is_not_null()).then(pl.col('home_team_pass_epa')).otherwise(0),
                #       away_team_pass_epa = pl.when(pl.col("away_team_pass_epa").is_not_null()).then(pl.col('away_team_pass_epa')).otherwise(0)
                #   )
                #   .with_columns(
                #       total_home_pass_epa = pl.col('home_team_pass_epa').cum_sum().over(["game_id"]),
                #       total_away_pass_epa = pl.col('away_team_pass_epa').cum_sum().over(["game_id"])
                #   )
    )
    return df

def add_wp_variables(df: pl.DataFrame):
    """This function adds all needed variables for Win Probability to calculate them in a correct way. 

    Args:
        df (pl.DataFrame): Expects a Polars DataFrame with all raw datas and the corresponding probabilities from the wp-Model.

    Returns:
        _type_: Polars DataFrame with Win Probability Columns
    """
    df = (
        df
        .with_columns(tmp_posteam = pl.col("posteam"))
        .with_columns(
            wp = pl.col("wp").backward_fill(),
            tmp_posteam = pl.col("tmp_posteam").backward_fill()
        )
        .with_columns(home_wp = pl.when(pl.col("tmp_posteam") == pl.col("home_team")).then(pl.col("wp")).otherwise(1 - pl.col("wp")))
        # convenience for marking home win prob on last line
        .with_columns(
            final_value = 
            pl.when(pl.col("home_team_score") > pl.col("away_team_score"))
            .then(pl.lit(1))
            .when(pl.col("away_team_score") > pl.col("home_team_score"))
            .then(pl.lit(0))
            .when(pl.col("home_team_score") == pl.col("away_team_score"))
            .then(pl.lit(0.5))
            )
        # can we make this and the above into a function? feels like a lot of repitition
        .with_columns(home_wp = pl.when(game_end = 1).then(pl.col("final_value")).otherwise(pl.col("home_wp")))
        .with_columns(away_wp = 1 - pl.col("home_wp"))
        .with_columns(def_wp = 1 - pl.col("wp"))
        # home wpa isn't saved but needed for next line
        .with_columns(home_wp_after = pl.col("home_wp").shift(-1))
        .with_columns(home_wpa = pl.col("home_wp_after") - pl.col("home_wp"))
        .with_columns(wpa = pl.when(pl.col("tmp_posteam") == pl.col("home_team")).then(pl.col("home_wpa")).otherwise(-pl.col("home_wpa")))
        .with_columns(wpa = pl.when(game_end = 1).then(pl.lit(None)).otherwise(pl.col("wpa")))
        # Home and Away post:
        .with_columns(
            home_wp_post = pl.when(pl.col("posteam") == pl.col("home_team")).then(pl.col("home_wp") + pl.col("wpa")).otherwise(pl.col("home_wp") - pl.col("wpa")),
            away_wp_post = pl.when(pl.col("posteam") == pl.col("away_team")).then(pl.col("away_wp") + pl.col("wpa")).otherwise(pl.col("away_wp") - pl.col("wpa"))
        )
        # Generate columns to keep track of cumulative WPA values
        .with_columns(
            home_team_wpa = pl.when(pl.col("posteam") == pl.col("home_team")).then(pl.col("wpa")).otherwise(-pl.col("wpa")),
            away_team_wpa = pl.when(pl.col("posteam") == pl.col("away_team")).then(pl.col("wpa")).otherwise(-pl.col("wpa"))
        )
        .with_columns(
            home_team_wpa = pl.when(pl.col("home_team_wpa").is_null()).then(pl.lit(0)).otherwise(pl.col("home_team_wpa")),
            away_team_wpa = pl.when(pl.col("away_team_wpa").is_null()).then(pl.lit(0)).otherwise(pl.col("away_team_wpa"))
        )
        .with_columns(
            total_home_wp = pl.col('home_wp_post').cum_sum().over(["game_id"]),
            total_away_wp = pl.col('away_wp_post').cum_sum().over(["game_id"]),
            total_home_wpa = pl.col('home_team_wpa').cum_sum().over(["game_id"]),
            total_away_wpa = pl.col('away_team_wpa').cum_sum().over(["game_id"])
        )
        )
    return(df)
                  