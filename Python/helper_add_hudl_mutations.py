import polars as pl 
import numpy as np

def get_games(df: pl.DataFrame):
    games = (
        df.melt(id_vars="game_id", value_vars="posteam")
          .unique(maintain_order=True)
          .pivot(values="value",index="game_id", columns="variable", aggregate_function=pl.element())
          .with_columns(pl.col("posteam").list.to_struct()).unnest("posteam")
          .rename({"field_0":"home_team", "field_1":"away_team"}) # Annahme: Das Team, welches den ersten Drive hat ist "home_team". Nur wichtig für die Scores und das Differential später.
          )
    return(games)

def make_hudl_mutations(df: pl.DataFrame):
    """_summary_

    Args:
        df (pl.DataFrame): _description_
        games (pl.DataFrame): _description_
    """
    output = (
        df.join(get_games(df), on="game_id")
        .with_columns(defteam =
                    pl.when(pl.col("posteam") == pl.col("home_team")).then(pl.col("away_team"))
                    .when(pl.col("posteam") == pl.col("away_team")).then(pl.col("home_team"))
                    .otherwise(pl.lit(None))
                    )
        .with_columns([
        pl.col("DN").cast(pl.Int32()).alias("down"),
        pl.col("DIST").cast(pl.Int32()).alias("yards_to_go"),
        pl.col("YARD LN").cast(pl.Int32()).alias("yardline"),
        pl.col(["yardline_50","game_id","play_id","drive_id","half"]).cast(pl.Int32())
        ])
        .with_columns(
            yardline_50_simple = pl.when(pl.col("yardline_50") < 25).then(pl.lit(0)).otherwise(pl.lit(1)),
            yards_to_go_simple = 
            pl.when(pl.col("yards_to_go") <= 5).then(pl.lit(1))
            .when((pl.col("yards_to_go") > 5) & (pl.col("yards_to_go") <= 10)).then(pl.lit(2))
            .when((pl.col("yards_to_go") > 10) & (pl.col("yards_to_go") <= 15)).then(pl.lit(3))
            .when((pl.col("yards_to_go") > 15) & (pl.col("yards_to_go") <= 20)).then(pl.lit(4))
            .when(pl.col("yards_to_go") > 20).then(pl.lit(5))
            .otherwise(pl.lit(0))
        )
        .with_columns(
            yardline_50_after = pl.col("yardline_50").shift(-1),
            posteam_after = pl.col("posteam").shift(-1)
        )
        .with_columns(
            pl.when(pl.col('RESULT').str.contains("Rush")).then(pl.lit("run"))
            .when(pl.col('RESULT').str.contains("Penalty")).then(pl.lit("no_play"))
            .when(pl.col('RESULT').str.contains("KNEEL")).then(pl.lit("qb_kneel"))
            .when(pl.col('down') == 0).then(pl.lit("extra_point"))
            .otherwise(pl.lit("pass"))
            .alias('play_type')
        )
        .with_columns(
            pl.when(pl.col('RESULT').str.contains("Sack")).then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias('sack')
        )
        .with_columns(
            pl.when(pl.col('RESULT').str.contains("Interception")).then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias('interception')
        )
        .with_columns(
            complete_pass = 
            pl.when((pl.col("play_type").is_in(["pass","extra_point","no_play"])) & (pl.col("RESULT").str.contains("Complete"))).then(pl.lit(1))
            .when((pl.col("play_type").is_in(["pass","extra_point","no_play"])) & (pl.col("RESULT") == "Incomplete")).then(pl.lit(0))
            .when((pl.col("yardline_50") != pl.col("yardline_50_after")) & (pl.col("posteam") == pl.col("posteam_after"))).then(pl.lit(1))
            .when((pl.col("down")==4) & (pl.col("posteam") != pl.col("posteam_after"))).then(pl.lit(0))
            .otherwise(pl.lit(0))        
        )
        .with_columns(
            pl.when((pl.col('RESULT').str.contains("TD")) & (pl.col('RESULT').str.contains("Def").not_())).then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias('touchdown')
        )
        .with_columns(
            pl.when(pl.col('RESULT').str.contains("Def TD")).then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias('def_touchdown')
        )
        .with_columns(
            pl.when(pl.col('RESULT').str.contains("Penalty")).then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias('penalty')
        )
        .with_columns(
            pl.when(pl.col('RESULT').str.contains("Safety")).then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias('safety')
        )
        .with_columns(
            pl.when((pl.col('RESULT') == "Good") & (pl.col("down") == 0) & (pl.col("yardline_50") == 45)).then(pl.lit(1))
            .when((pl.col("down") == 0) & (pl.col("yardline_50") != 45)).then(pl.lit(0))
            .when(pl.col("down") != 0).then(pl.lit(0))
            .otherwise(pl.lit(0))
            .alias('one_point_conv_success')
        )
        .with_columns(
            pl.when((pl.col('RESULT') == "Good") & (pl.col("down") == 0) & (pl.col("yardline_50") == 40)).then(pl.lit(1))
            .when((pl.col("down") == 0) & (pl.col("yardline_50") != 40)).then(pl.lit(0))
            .when(pl.col("down") != 0).then(pl.lit(0))
            .otherwise(pl.lit(0))
            .alias('two_point_conv_success')
        )
        .with_columns(
            pl.when((pl.col('RESULT').str.contains("Def TD")) & (pl.col("down") == 0)).then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias('defensive_two_point_conv')
        )
        .with_columns(
            pl.when(
                pl.col('touchdown') |
                pl.col('def_touchdown') |
                pl.col('one_point_conv_success') |
                pl.col('two_point_conv_success') |
                pl.col('defensive_two_point_conv') |
                pl.col('safety') 
                == 1).then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias('scoring_play')
        )
        .with_columns(
            pl.when((pl.col('scoring_play') == 1) & (pl.col("touchdown") | pl.col("one_point_conv_success") | pl.col("two_point_conv_success") == 1)).then(pl.col("posteam"))
            .otherwise(pl.lit(None))
            .alias('scoring_play_team')
        )
        .with_columns(
            home_team_points = pl.when(pl.col("play_id")==1).then(pl.lit(0))
                .when(pl.col('home_team')==pl.col("scoring_play_team")).then(
                    pl.when(touchdown=1).then(pl.lit(6))
                    .when(def_touchdown=1).then(pl.lit(6))
                    .when(one_point_conv_success=1).then(pl.lit(1))
                    .when(two_point_conv_success=1).then(pl.lit(2))
                    .when(defensive_two_point_conv=1).then(pl.lit(2))
                    .when(safety=1).then(pl.lit(2))
                .otherwise(pl.lit(None))
            ),
            away_team_points = pl.when(pl.col("play_id")==1).then(pl.lit(0))
                .when(pl.col('away_team')==pl.col("scoring_play_team")).then(
                    pl.when(touchdown=1).then(pl.lit(6))
                    .when(def_touchdown=1).then(pl.lit(6))
                    .when(one_point_conv_success=1).then(pl.lit(1))
                    .when(two_point_conv_success=1).then(pl.lit(2))
                    .when(defensive_two_point_conv=1).then(pl.lit(2))
                    .when(safety=1).then(pl.lit(2))
                .otherwise(pl.lit(None))
            )
        )
        .with_columns(
            home_team_score = pl.col('home_team_points').cum_sum().over(["game_id","home_team"]),
            away_team_score = pl.col('away_team_points').cum_sum().over(["game_id","away_team"])
        )
        .with_columns(
                home_team_score = pl.col("home_team_score").forward_fill(),
                away_team_score = pl.col("away_team_score").forward_fill()
            )
        .with_columns(
            posteam_score = pl.when(pl.col("posteam") == pl.col("home_team")).then(pl.col('home_team_score'))
                .when(pl.col("posteam") == pl.col("away_team")).then(pl.col('away_team_score')),
            defteam_score = pl.when(pl.col("defteam") == pl.col("home_team")).then(pl.col('home_team_score'))
                .when(pl.col("defteam") == pl.col("away_team")).then(pl.col('away_team_score'))
            )
        .with_columns(score_differential = pl.col("posteam_score") - pl.col("defteam_score"))
        .with_columns(
            yards_gained = 
            pl.when(down = 0).then(pl.lit(0))
            .when(down=4,complete_pass=0).then(pl.lit(0))
            # .when((pl.col("down") == 4) & (pl.col("interception") == 0) & (pl.col("sack") == 0) & (pl.col("def_touchdown") == 0) & (pl.col("penalty") == 0) & (pl.col("safety") == 0)).then(pl.lit(0))
            .when(down = 4, safety= 1).then(pl.lit(0))
            .otherwise(pl.col("yardline_50_after") - pl.col("yardline_50"))
        )
        .with_columns(
            pl.when((pl.col('yardline_50') < 25) & (pl.col("yards_gained") > pl.col("yards_to_go"))).then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias('first_down')
        )
        .with_row_index(offset=1)
    )
    return(output)

def make_dsfootball_mutations(df: pl.DataFrame):
    output = (df
                .join(get_games(df), on="game_id")
                .with_columns(defteam =
                    pl.when(pl.col("posteam") == pl.col("home_team")).then(pl.col("away_team"))
                    .when(pl.col("posteam") == pl.col("away_team")).then(pl.col("home_team"))
                    .otherwise(pl.lit(None))
                    )
                .with_columns([
                    pl.col("play_id").cast(pl.Int32()),
                    pl.col("complete_pass").cast(pl.Int32()),
                    pl.col("interception").cast(pl.Int32()),
                    pl.col("touchdown").cast(pl.Int32()),
                    pl.col("point_after").cast(pl.Int32()),
                    pl.col("Down").cast(pl.Int32()).alias("down"),
                    pl.col("Distance").cast(pl.Int32()).alias("yards_to_go"),
                    pl.col("Spot").cast(pl.Int32()).alias("yardline_50"),
                    pl.col("Drive").cast(pl.Int32()).alias("drive_id"),
                    pl.col("Quarter").cast(pl.Int32()).alias("half")
                    ])
                .with_columns(pl.col("point_after_success").str.replace("NA", "0"))
                .with_columns(pl.col("point_after_success").cast(pl.Int32()))
                .with_columns(
                    yardline_50_simple = pl.when(pl.col("yardline_50") < 25).then(pl.lit(0)).otherwise(pl.lit(1)),
                    yards_to_go_simple = 
                    pl.when(pl.col("yards_to_go") <= 5).then(pl.lit(1))
                    .when((pl.col("yards_to_go") > 5) & (pl.col("yards_to_go") <= 10)).then(pl.lit(2))
                    .when((pl.col("yards_to_go") > 10) & (pl.col("yards_to_go") <= 15)).then(pl.lit(3))
                    .when((pl.col("yards_to_go") > 15) & (pl.col("yards_to_go") <= 20)).then(pl.lit(4))
                    .when(pl.col("yards_to_go") > 20).then(pl.lit(5))
                    .otherwise(pl.lit(0))
                )
                .with_columns(
                    yardline_50_after = pl.col("yardline_50").shift(-1),
                    posteam_after = pl.col("posteam").shift(-1)
                )
                .with_columns(
                    pl.when((pl.col('interception') == 1) & (pl.col("touchdown")==1)).then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias('def_touchdown')
                )
                .with_columns(
                    pl.when(pl.col('IsSafety') == True).then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias('safety')
                )
                .with_columns(
                    pl.when((pl.col('point_after_success') == 1) & (pl.col('point_after') == 1) & (pl.col("yardline_50") == 45)).then(pl.lit(1))
                    .when((pl.col("down") == 0) & (pl.col("yardline_50") != 45)).then(pl.lit(0))
                    .when(pl.col("down") != 0).then(pl.lit(0))
                    .otherwise(pl.lit(0))
                    .alias('one_point_conv_success')
                )
                .with_columns(
                    pl.when((pl.col('point_after_success') == 1) & (pl.col('point_after') == 1) & (pl.col("yardline_50") == 40)).then(pl.lit(1))
                    .when((pl.col("down") == 0) & (pl.col("yardline_50") != 40)).then(pl.lit(0))
                    .when(pl.col("down") != 0).then(pl.lit(0))
                    .otherwise(pl.lit(0))
                    .alias('two_point_conv_success')
                )
                .with_columns(
                    pl.when((pl.col('point_after') == 1) & (pl.col("interception") == 1) & (pl.col("touchdown") == 1)).then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias('defensive_two_point_conv')
                )
                .with_columns(
                    pl.when(
                        pl.col('touchdown') |
                        pl.col('def_touchdown') |
                        pl.col('one_point_conv_success') |
                        pl.col('two_point_conv_success') |
                        pl.col('defensive_two_point_conv') |
                        pl.col('safety') 
                        == 1).then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias('scoring_play')
                )
                .with_columns(
                    pl.when((pl.col('scoring_play') == 1) & (pl.col("touchdown") | pl.col("one_point_conv_success") | pl.col("two_point_conv_success") == 1)).then(pl.col("posteam"))
                    .when((pl.col('scoring_play') == 1) & (pl.col("def_touchdown") | pl.col("defensive_two_point_conv") | pl.col("safety") == 1)).then(pl.col("defteam"))
                    .otherwise(pl.lit(None))
                    .alias('scoring_play_team')
                )
                .with_columns(
                    home_team_points = pl.when(pl.col("play_id")==1).then(pl.lit(0))
                        .when(pl.col('home_team')==pl.col("scoring_play_team")).then(
                            pl.when(touchdown=1).then(pl.lit(6))
                            .when(def_touchdown=1).then(pl.lit(6))
                            .when(one_point_conv_success=1).then(pl.lit(1))
                            .when(two_point_conv_success=1).then(pl.lit(2))
                            .when(defensive_two_point_conv=1).then(pl.lit(2))
                            .when(safety=1).then(pl.lit(2))
                        .otherwise(pl.lit(None))
                    ),
                    away_team_points = pl.when(pl.col("play_id")==1).then(pl.lit(0))
                        .when(pl.col('away_team')==pl.col("scoring_play_team")).then(
                            pl.when(touchdown=1).then(pl.lit(6))
                            .when(def_touchdown=1).then(pl.lit(6))
                            .when(one_point_conv_success=1).then(pl.lit(1))
                            .when(two_point_conv_success=1).then(pl.lit(2))
                            .when(defensive_two_point_conv=1).then(pl.lit(2))
                            .when(safety=1).then(pl.lit(2))
                        .otherwise(pl.lit(None))
                    )
                )
                .with_columns(
                    home_team_score = pl.col('home_team_points').cum_sum().over(["game_id","home_team"]),
                    away_team_score = pl.col('away_team_points').cum_sum().over(["game_id","away_team"])
                )
                .with_columns(
                        home_team_score = pl.col("home_team_score").forward_fill(),
                        away_team_score = pl.col("away_team_score").forward_fill()
                    )
                .with_columns(
                    posteam_score = pl.when(pl.col("posteam") == pl.col("home_team")).then(pl.col('home_team_score'))
                        .when(pl.col("posteam") == pl.col("away_team")).then(pl.col('away_team_score')),
                    defteam_score = pl.when(pl.col("defteam") == pl.col("home_team")).then(pl.col('home_team_score'))
                        .when(pl.col("defteam") == pl.col("away_team")).then(pl.col('away_team_score'))
                    )
                .with_columns(score_differential = pl.col("posteam_score") - pl.col("defteam_score"))
                .with_columns(
                    yards_gained = 
                    pl.when(down = 0).then(pl.lit(0))
                    .when(down=4,complete_pass=0).then(pl.lit(0))
                    # .when((pl.col("down") == 4) & (pl.col("interception") == 0) & (pl.col("sack") == 0) & (pl.col("def_touchdown") == 0) & (pl.col("penalty") == 0) & (pl.col("safety") == 0)).then(pl.lit(0))
                    .when(down = 4, safety= 1).then(pl.lit(0))
                    .otherwise(pl.col("yardline_50_after") - pl.col("yardline_50"))
                )
                .with_columns(
                    pl.when((pl.col('yardline_50') < 25) & (pl.col("yards_gained") > pl.col("yards_to_go"))).then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias('first_down')
                )
                .with_row_index(offset=1)
            )
    return(output)

def prepare_ep_data(df: pl.DataFrame):
    output = (df
              .with_columns(
                  half_end = 
                  pl.when(pl.col("index")
                          .is_in(df
                                 .select(["index", "game_id", "half"])
                                 .unique(subset=["game_id","half"], keep="last")
                                 .select("index")
                                 .to_dicts())).then(pl.lit(1))
                                 .otherwise(pl.lit(0))
                )
                .with_columns(game_end = pl.when((pl.col("half_end") == 1 ) & (pl.col("half") == 2)).then(1).otherwise(0))
                .with_columns(
                    pl.when(pl.col("touchdown") == 1).then(pl.lit("Touchdown"))
                    .when(pl.col("def_touchdown") == 1).then(pl.lit("Touchdown"))
                    .when(pl.col("safety") == 1).then(pl.lit("Safety"))
                    .when(pl.col("one_point_conv_success") == 1).then(pl.lit("Extra_Point"))
                    .when(pl.col("two_point_conv_success") == 1).then(pl.lit("Two_Point_Conversion"))
                    .when((pl.col("half_end") == 1 ) & (pl.col("scoring_play") == 0)).then(pl.lit("No_Score"))
                    .otherwise(pl.lit(None))
                    .alias('scoring_event')
                )
                .with_columns(
                    pl.when(pl.col("scoring_play") == 1).then(pl.col("drive_id"))
                    .otherwise(pl.lit(None))
                    .alias('score_drive')
                )
                .with_columns(start_posteam = pl.when(pl.col("play_id") == 1).then(pl.col("posteam")).otherwise(pl.lit(None)))
                .with_columns(
                    scoring_play_team = pl.col("scoring_play_team").backward_fill(),
                    scoring_event = pl.col("scoring_event").backward_fill(),
                    score_drive = pl.col("score_drive").backward_fill(),
                    posteam_score = pl.col("posteam_score").forward_fill(),
                    defteam_score = pl.col("defteam_score").forward_fill(),
                    start_posteam = pl.col("start_posteam").forward_fill()
                )
                .with_columns(
                    pl.when((pl.col("scoring_event") == "Touchdown") & (pl.col("posteam")==pl.col("scoring_play_team"))).then(pl.lit("Touchdown"))
                    .when((pl.col("scoring_event") == "Touchdown") & (pl.col("posteam")==pl.col("scoring_play_team"))).then(pl.lit("Touchdown"))
                    .when((pl.col("scoring_event") == "Touchdown") & (pl.col("posteam")!=pl.col("scoring_play_team"))).then(pl.lit("Opp_Touchdown"))
                    .when((pl.col("scoring_event") == "Safety") & (pl.col("posteam")==pl.col("scoring_play_team"))).then(pl.lit("Safety"))
                    .when((pl.col("scoring_event") == "Safety") & (pl.col("posteam")!=pl.col("scoring_play_team"))).then(pl.lit("Opp_Safety"))
                    .when((pl.col("scoring_event") == "Extra_Point") & (pl.col("posteam")==pl.col("scoring_play_team"))).then(pl.lit("Extra_Point"))
                    .when((pl.col("scoring_event") == "Extra_Point") & (pl.col("posteam")!=pl.col("scoring_play_team"))).then(pl.lit("Opp_Two_Point_Conversion"))
                    .when((pl.col("scoring_event") == "Two_Point_Conversion") & (pl.col("posteam")==pl.col("scoring_play_team"))).then(pl.lit("Two_Point_Conversion"))
                    .when((pl.col("scoring_event") == "Two_Point_Conversion") & (pl.col("posteam")!=pl.col("scoring_play_team"))).then(pl.lit("Opp_Two_Point_Conversion"))
                    .when((pl.col("scoring_event") == "No_Score")).then(pl.lit("No_Score"))
                    .otherwise(pl.lit(None))
                    .alias('Next_Score_Half')
                )
                # To make 'No_Scores' no scoring drive put in actual drive_id for No Score Drives.
                .with_columns(
                    pl.when(pl.col("Next_Score_Half")=="No_Score").then(pl.col("drive_id"))
                    .otherwise(pl.col("score_drive"))
                    .alias("Drive_Score_Half")
                )
                .with_columns(
                    pl.col("play_id").max().over(["game_id","half"]).alias("max_play_id")
                )
            )
    return(output)

def prepare_wp_data(df: pl.DataFrame):
    output = (df
              .with_columns(
                  half_end = 
                  pl.when(pl.col("index")
                          .is_in(df
                                 .select(["index", "game_id", "half"])
                                 .unique(subset=["game_id","half"], keep="last")
                                 .select("index")
                                 .to_dicts())).then(pl.lit(1))
                                 .otherwise(pl.lit(0))
                )
                .with_columns(game_end = pl.when((pl.col("half_end") == 1 ) & (pl.col("half") == 2)).then(1).otherwise(0))
                .with_columns(helper_one = pl.lit(1))
                .with_columns(play_id_half = pl.col("helper_one").cum_sum().over(["game_id","half"]))
                .with_columns(play_time = pl.when(play_id_half=1).then(0).otherwise(1200 / pl.col('play_id_half').max()).over(['game_id', 'half']))
                .with_columns(half_seconds_remaining = (1200 - pl.col("play_time").cum_sum().over(["game_id","half"])))
                .with_columns(game_seconds_remaining = pl.when(pl.col("half") == 2).then(pl.col("half_seconds_remaining")).otherwise(2400 - pl.col("play_time").cum_sum().over("game_id")))
                .with_columns(elapsed_share = (2400 - pl.col("game_seconds_remaining")) / 2400)
                .with_columns(Diff_Time_Ratio = (pl.col("score_differential")) / (np.exp(-4 * pl.col("elapsed_share"))))
                .with_columns(start_posteam = pl.when(pl.col("play_id") == 1).then(pl.col("posteam")).otherwise(pl.lit(None)))
                .with_columns(receive_2h_ko = pl.when(pl.col("start_posteam") == pl.col("posteam")).then(0).otherwise(1))
              ) 
    return(output)
