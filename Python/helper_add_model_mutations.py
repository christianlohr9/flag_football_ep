import polars as pl 
import xgboost as xgb

def make_ep_model_mutations(df: pl.DataFrame, selected_columns):
    """Adds the needed 'label' column for the model and select only needed ones.


    Args:
        df (pl.DataFrame): Polars DataFrame
        selected_columns (_type_): features for the model in a 'list=[]' form.

    Returns:
        _type_: Polars DataFrame for model training
    """
    model_data = (
        df.with_columns(label = 
                        pl.when(Next_Score_Half="Touchdown").then(pl.lit(0))
                        .when(Next_Score_Half="Opp_Touchdown").then(pl.lit(1))
                        # .when(Next_Score_Half="Extra_Point").then(pl.lit(2))
                        # .when(Next_Score_Half="Two_Point_Conversion").then(pl.lit(3))
                        # .when(Next_Score_Half="Opp_Two_Point_Conversion").then(pl.lit(4))
                        .when(Next_Score_Half="Safety").then(pl.lit(2))
                        .when(Next_Score_Half="Opp_Safety").then(pl.lit(3))
                        .when(Next_Score_Half="No_Score").then(pl.lit(4))
                        )
                        .with_columns(
                            down0 = pl.when(down=0).then(pl.lit(1)).otherwise(0),
                            down1 = pl.when(down=1).then(pl.lit(1)).otherwise(0),
                            down2 = pl.when(down=2).then(pl.lit(1)).otherwise(0),
                            down3 = pl.when(down=3).then(pl.lit(1)).otherwise(0),
                            down4 = pl.when(down=4).then(pl.lit(1)).otherwise(0)
                        )
                        # Calculate the drive difference between the next score drive and the current play drive:
                        .with_columns(Drive_Score_Dist = pl.col("Drive_Score_Half") - pl.col("drive_id"))
                        # Create a weight column based on difference in drives between play and next score:
                        .with_columns(Drive_Score_Dist_W = (pl.max("Drive_Score_Dist") - pl.col("Drive_Score_Dist")) / (pl.max("Drive_Score_Dist") - pl.min("Drive_Score_Dist")))
                        # Create a weight column based on score differential:
                        .with_columns(ScoreDiff_W = (pl.col("score_differential").abs().max() - pl.col("score_differential").abs()) / (pl.col("score_differential").abs().max() - pl.col("score_differential").abs().min()))
                        # Add these weights together and scale again:
                        .with_columns(Total_W = pl.col("Drive_Score_Dist_W") + pl.col("ScoreDiff_W"))
                        .with_columns(Total_W_Scaled = (pl.col("Total_W") - pl.min("Total_W")) / (pl.max("Total_W") - pl.min("Total_W")))
                        .filter(
                            pl.col("yardline_50").is_not_null(),
                            pl.col("yards_to_go").is_not_null()
                        )
                        .select(selected_columns)
                        )
    return model_data

def make_wp_model_mutations(df: pl.DataFrame, selected_columns):
    """Adds the needed 'label' column for the model and select only needed ones.


    Args:
        df (pl.DataFrame): Polars DataFrame
        selected_columns (_type_): features for the model in a 'list=[]' form.

    Returns:
        _type_: Polars DataFrame for model training
    """
    model_data = (
        df
        .with_columns(
               Winner = 
               pl.when((pl.col("game_end") == 1) & (pl.col("home_team_score") > pl.col("away_team_score")))
               .then("home_team")
               .when((pl.col("game_end") == 1) & (pl.col("home_team_score") < pl.col("away_team_score")))
               .then("away_team")
               .when((pl.col("game_end") == 1) & (pl.col("home_team_score") == pl.col("away_team_score")))
               .then(pl.lit("TIE"))
               .otherwise(pl.lit(None))
        )
        .with_columns(Winner = pl.col("Winner").backward_fill())
        .with_columns(label = pl.when(pl.col("posteam") == pl.col("Winner")).then(pl.lit(1)).otherwise(pl.lit(0)))
        .select(selected_columns)
        )
    return model_data
