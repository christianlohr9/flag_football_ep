import requests
import pandas as pd
import polars as pl

# Funktion zum Abrufen und Verarbeiten der Spieldaten
def process_game(game_id, api_key):
    # Endpunkte definieren
    drives_url = f"https://main-api-1.sportapp.fi/api/v1/public/match-drives?id={game_id}&apikey={api_key}"
    match_url = f"https://main-api-1.sportapp.fi/api/v1/public/match-v1?id={game_id}&apikey={api_key}"
    
    # Drives-Daten abrufen
    drives_response = requests.get(drives_url)
    if drives_response.status_code != 200:
        print(f"Fehler beim Abrufen der Drives-Daten für Spiel {game_id}")
        return None
    
    # Match-Daten abrufen
    match_response = requests.get(match_url)
    if match_response.status_code != 200:
        print(f"Fehler beim Abrufen der Match-Daten für Spiel {game_id}")
        return None
    
    drives_data = drives_response.json()
    match_data = match_response.json()
    
    # Home- und Away-Team-Informationen extrahieren
    series_id = match_data.get("series", {}).get("id", 0)
    series_region = match_data.get("series", {}).get("region", 0)
    series_level = match_data.get("series", {}).get("level", 0)
    season = match_data.get("series", {}).get("seasonName", 0)
    gender = match_data.get("series", {}).get("id", 0)
    game_type = match_data.get("series", {}).get("phase", 0)
    game_group_id = match_data.get("series", {}).get("groupId", 0)
    game_group = match_data.get("series", {}).get("groupName", 0)
    game_type = match_data.get("series", {}).get("phase", 0)
    stream_url = match_data.get("streams", [{}])[0].get("url", "Unknown") if match_data.get("streams") else "Unknown"
    home_team = match_data.get("home", {}).get("id", "Unknown")
    away_team = match_data.get("away", {}).get("id", "Unknown")
    
    # Ergebnisdaten überprüfen und abrufen
    result_data = match_data.get("result")
    if result_data is None:
        print(f"Keine Ergebnisdaten verfügbar für Spiel {game_id}.")
        home_score = 0
        away_score = 0
    else:
        home_score = result_data.get("details", {}).get("points_total_home", 0)
        away_score = result_data.get("details", {}).get("points_total_away", 0)
    
    plays_data = []
    
    # Verarbeitung der Drives und Plays
    for drive in drives_data:
        half = drive.get("num") + 1
        for event_group_index, event_group in enumerate(drive.get("drives", [])):
            drive_id = event_group_index + 1
            team_id = event_group.get("team", {}).get("id", "Unknown")
            team_abb = event_group.get("team", {}).get("threeLetters", "Unknown")
            for play_index, play in enumerate(event_group.get("eventGroups", [])):
                play_id = play_index + 1
                plays_data.append({
                    "season": season,
                    "competition_id": series_id,
                    "competition_name": series_region,
                    "competition_league": series_level,
                    "gender": gender,
                    "game_id": game_id,
                    "game_type": game_type,
                    "game_group_id": game_group_id,
                    "game_group": game_group,
                    "half": half,
                    "drive_id_half": drive_id,
                    "play_id_drive": play_id,
                    "posteam_id": team_id,
                    "posteam_abb": team_abb,
                    "summary": play.get("summary"),
                    "action_title": play.get("actionTitle"),
                    "down": play.get("down"),
                    "down_desc": play.get("downLabel"),
                    "down_after": play.get("nextDown"),
                    "down_after_desc": play.get("nextDownLabel"),
                    "yards_to_go": play.get("target"),
                    "yards_to_go_after": play.get("nextTarget"),
                    "yards": event_group.get("yards"),
                    "start_yard_line": play.get("startYardLine", {}).get("yardLine", 0),
                    "start_yard_line_team_half_id": play.get("startYardLine", {}).get("team", 0),
                    "end_yard_line": play.get("endYardLine", {}).get("yardLine", 0),
                    "end_yard_line_team_half_id": play.get("endYardLine", {}).get("team", 0),
                    "possession_time": event_group.get("timeOfPossession"),
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_score": home_score,
                    "away_score": away_score,
                    "stream_url": stream_url
                })
    
    return plays_data

def fetch_team_players(team_ids, api_key):
    all_players_data = []
    
    for team_id in team_ids:
        api_url = f"https://main-api-1.sportapp.fi/api/v1/public/teams-players?team={team_id}&apikey={api_key}"
        print(f"Fetching data for team ID: {team_id}")

        try:
            response = requests.get(api_url)
            response.raise_for_status()  # Check for HTTP request errors
            data = response.json()  # Attempt to parse JSON

            if isinstance(data, list) and len(data) > 0 and "players" in data[0]:  # Ensure 'players' key exists in response
                team_id = data[0]["id"]
                team_name = data[0]["name"]
                players = data[0]["players"]
                
                for player in players:
                    player_info = {
                        "team_id": team_id,
                        "team_name": team_name,
                        "player_id": player["id"],
                        "player_name": player["name"],
                        "player_jersey": player["uniform_number"],
                        "position": player["position"],
                        "club": player["club"]
                    }
                    all_players_data.append(player_info)
                print(f"Successfully fetched data for team ID {team_id} ({team_name})")
            else:
                print(f"No players found for team ID: {team_id} or invalid response format.")

        except requests.JSONDecodeError:
            print(f"Skipping team ID: {team_id} - Response is not in JSON format.")
        except requests.RequestException as e:
            print(f"Request failed for team ID: {team_id} with error: {e}")
    
    players_df = pl.DataFrame(all_players_data)
    return players_df

# Mehrere Spiele abrufen
def process_games(game_ids, api_key):
    all_plays = []
    for game_id in game_ids:
        print(f"Verarbeite Spiel ID: {game_id}")
        game_plays = process_game(game_id, api_key)
        if game_plays:
            all_plays.extend(game_plays)
        print(f"Fertig mit Spiel ID: {game_id}")
    
    # DataFrame erstellen
    return pd.DataFrame(all_plays)

### utils

def convert_to_polars(df: pd.DataFrame) -> pl.DataFrame:
    df['yards_to_go'] = df['yards_to_go'].astype(str)
    df['yards_to_go_after'] = df['yards_to_go_after'].astype(str)
    
    df = pl.from_pandas(df)

    return df

def extract_players_from_summary(df: pd.DataFrame) -> pd.DataFrame:
    # Regular Expressions für die verschiedenen Fälle
    passer_pattern = r'#(\d+)\s+(\w+\s\w+)\s+pass'  # Identifikation des Passers
    receiver_pattern = r'pass complete to #(\d+)\s+(\w+\s\w+)'  # Identifikation des Receivers
    rusher_pattern = r'#(\d+)\s+(\w+\s\w+)\s+rush'  # Identifikation des Rushers
    tackle_pattern = r'tackled by #(\d+)\s+(\w+\s\w+)'  # Identifikation des Tacklers
    interception_pattern = r'pass intercepted to #(\d+)\s+(\w+\s\w+)'  # Identifikation des Interceptionsspielers
    sack_pattern = r'sacked by #(\d+)\s+(\w+\s\w+)'
    play_result_pattern = r'(rush|complete|incomplete|touchdown|first down|intercepted|sack|fumble|good|miss|timeout)'
    penalty_pattern = r'(penalty)'
    safety_pattern = r'(safety)'

    # Funktionen zur Extraktion der Spieler
    def get_passer(summary):
        match = re.search(passer_pattern, summary)
        return match.group(1) if match else None
    
    def get_receiver(summary):
        match = re.search(receiver_pattern, summary)
        return match.group(1) if match else None

    def get_rusher(summary):
        match = re.search(rusher_pattern, summary)
        return match.group(1) if match else None

    def get_tackle_player(summary):
        match = re.search(tackle_pattern, summary)
        return match.group(1) if match else None

    def get_interception_player(summary):
        match = re.search(interception_pattern, summary)
        return match.group(1) if match else None
    
    def get_sack_player(summary):
        match = re.search(sack_pattern, summary)
        return match.group(1) if match else None

    def get_play_result(summary):
        match = re.search(play_result_pattern, summary)
        return match.group(1) if match else None

    def get_penalty(summary):
        match = re.search(penalty_pattern, summary)
        return match.group(1) if match else None

    def get_safety(summary):
        match = re.search(safety_pattern, summary)
        return match.group(1) if match else None

    # Die neuen Spalten hinzufügen
    df['passer'] = df['summary'].apply(get_passer)
    df['receiver'] = df['summary'].apply(get_receiver)
    df['rusher'] = df['summary'].apply(get_rusher)
    df['tackle_player'] = df['summary'].apply(get_tackle_player)
    df['interception_player'] = df['summary'].apply(get_interception_player)
    df['sack_player'] = df['summary'].apply(get_sack_player)
    df['play_result'] = df['summary'].apply(get_play_result)
    df['penalty'] = df['summary'].apply(get_penalty)
    df['safety'] = df['summary'].apply(get_safety)

    return df

def clean_sort(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts the DataFrame based on the specified columns with the given order.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to be sorted.
    
    Returns:
    pd.DataFrame: The sorted DataFrame.
    """
    df = df.sort_values(by=['game_id','half', 'drive_id_half', 'play_id_drive'], ascending=[True, True, False, False])
    return df

def clean_yardage(df: pl.DataFrame) -> pl.DataFrame:
    """
    Computes the 'yardline_50' column based on the 'start_yard_line_team_half_desc' and 'start_yard_line' columns and the yards_to_go column based on original column.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to be processed.
    
    Returns:
    pl.DataFrame: The DataFrame with the 'yardline_50' column added.
    """ 
    df = (
        df
        .with_columns(
            yardline_50 = pl.when(pl.col("start_yard_line_team_half_id")==pl.col("away_team")).then(50-pl.col("start_yard_line")).otherwise(pl.col("start_yard_line")),
            yardline_50_after = pl.when(pl.col("end_yard_line_team_half_id")==pl.col("away_team")).then(50-pl.col("end_yard_line")).otherwise(pl.col("end_yard_line"))
        )
        .with_columns(
            yardline_50 = pl.when(pl.col("posteam")==pl.col("home_team"))
            .then(pl.col("yardline_50"))
            .when(pl.col("posteam")==pl.col("away_team"))
            .then(50-pl.col("yardline_50"))
            .otherwise(pl.lit(0)),
            yardline_50_after = pl.when(pl.col("posteam")==pl.col("home_team"))
            .then(pl.col("yardline_50_after"))
            .when(pl.col("posteam")==pl.col("away_team"))
            .then(50-pl.col("yardline_50_after"))
            .otherwise(pl.lit(0))
        )
        .with_columns(yards_gained = pl.col("yardline_50_after") - pl.col("yardline_50"))
        .with_columns(
            yards_to_go = pl.when(pl.col("yards_to_go")=="G").then(pl.lit(999))
            .when(pl.col("yards_to_go").is_null()).then(pl.lit(None))
            .when(pl.col("yards_to_go")=="").then(pl.lit(None))
            .when(pl.col("down_desc")=="PAT 5 yards").then(pl.lit(5))
            .when(pl.col("down_desc")=="PAT 10 yards").then(pl.lit(10))
            .otherwise(pl.col("yards_to_go")),
            yards_to_go_after = pl.when(pl.col("yards_to_go_after")=="G").then(pl.lit(999))
            .when(pl.col("yards_to_go_after").is_null()).then(pl.lit(None))
            .when(pl.col("yards_to_go_after")=="").then(pl.lit(None))
            .otherwise(pl.col("yards_to_go_after"))
        )
        .with_columns(
            pl.col("yards_to_go").cast(pl.Int32),
            pl.col("yards_to_go_after").cast(pl.Int32)
            )
        .with_columns(
            yards_to_go = pl.when(pl.col("yards_to_go")==999)
            .then(50-pl.col("yardline_50"))
            .otherwise(pl.col("yards_to_go")),
            yards_to_go_after = pl.when(pl.col("yards_to_go_after")==999)
            .then(50-pl.col("yardline_50_after"))
            .otherwise(pl.col("yards_to_go_after"))
        )
        .with_columns(
            yards_to_go_simple = 
                    pl.when(pl.col("yards_to_go") <= 5).then(pl.lit(1))
                    .when((pl.col("yards_to_go") > 5) & (pl.col("yards_to_go") <= 10)).then(pl.lit(2))
                    .when((pl.col("yards_to_go") > 10) & (pl.col("yards_to_go") <= 15)).then(pl.lit(3))
                    .when((pl.col("yards_to_go") > 15) & (pl.col("yards_to_go") <= 20)).then(pl.lit(4))
                    .when(pl.col("yards_to_go") > 20).then(pl.lit(5))
                    .otherwise(pl.lit(0))
        )
        .with_columns(
            first_down = 
                pl.when((pl.col('yardline_50') < 25) & (pl.col("yards_gained") > pl.col("yards_to_go"))).then(pl.lit(1))
                .otherwise(pl.lit(0))
        )
    )

    return df

def correct_posteam(df: pl.DataFrame) -> pl.DataFrame:

    df = df.with_columns(posteam_helper = pl.concat_str(pl.col("game_id"),pl.lit("_"),pl.col("drive_id")))

    filter_drive = df.select(pl.col('posteam_helper').filter(pl.col('def_touchdown')==1)).unique()
    
    df = (df
          .with_columns(
              defteam_id =
                pl.when(pl.col("posteam_id") == pl.col("home_team")).then(pl.col("away_team"))
                .when(pl.col("posteam_id") == pl.col("away_team")).then(pl.col("home_team"))
                .otherwise(pl.lit(None))
              )
          .with_columns(posteam_helper_2 = pl.when((pl.col("posteam_helper")).is_in(filter_drive)).then(pl.col("play_id")).otherwise(pl.lit(None)))
          .with_columns(posteam_helper_3 = pl.when((pl.col("posteam_helper")).is_in(filter_drive)).then(pl.col("posteam_helper")).otherwise(pl.lit(None)))
          .with_columns(posteam_helper_max = pl.when(((pl.col("play_id"))==(pl.col("posteam_helper_2")))&pl.col("def_touchdown")==1).then(pl.col("posteam_helper")).otherwise(pl.lit(None)))
          .with_columns(posteam_helper_max = pl.col("posteam_helper_max").backward_fill())
          .with_columns(posteam = pl.when((pl.col("posteam_helper_3"))==(pl.col("posteam_helper_max"))).then(pl.col("defteam_id")).otherwise(pl.col("posteam_id")))
          .with_columns(
              defteam =
                pl.when(pl.col("posteam") == pl.col("home_team")).then(pl.col("away_team"))
                .when(pl.col("posteam") == pl.col("away_team")).then(pl.col("home_team"))
                .otherwise(pl.lit(None))
              )
          .drop(["posteam_helper","posteam_helper_2","posteam_helper_3","posteam_helper_max","posteam_id","defteam_id"])
          .with_columns(posteam_after = pl.col("posteam").shift(-1))
          .with_columns(defteam_after = pl.col("defteam").shift(-1))
          )
    
    return df

def clean_play_ids(df: pl.DataFrame) -> pl.DataFrame:
    """
    Computes the 'play_id', 'drive_id' and 'half_end' columns based on the sorted index outputs of the clean_sort function.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to be processed.
    
    Returns:
    pl.DataFrame: The DataFrame with the 'drive_id', 'play_id' and 'half_end' column added.
    """ 
    df = (
        df
        .with_row_index(offset=1)
        .with_columns(
            drive_id = (pl.col("drive_id_half").diff() != 0).cum_sum().over("game_id"),
            play_id = (pl.col("index").diff() != 0).cum_sum().over("game_id")
            )
        .with_columns(
            drive_id = (pl.col("drive_id")+1).backward_fill(),
            play_id = (pl.col("play_id")+1),
            )
        .with_columns(
            play_id = pl.when(pl.col("play_id").is_null()).then(pl.lit(1)).otherwise(pl.col("play_id")),
            play_id_max_half = pl.col("play_id").max().over(["game_id","half"]),
            play_id_max = pl.col("play_id").max().over("game_id")
        )
        .with_columns(
            half_end = 
            pl.when(pl.col("play_id_max_half")==pl.col("play_id"))
            .then(pl.lit(1))
            .when(pl.col("play_id_max")==pl.col("play_id"))            
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            )
        .drop(["play_id_max_half", "play_id_max"])
    )
    return df

def add_event_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Computes the event columns mainly based on the 'summary', 'play_result' and 'penalty' columns.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to be processed.
    
    Returns:
    pl.DataFrame: The DataFrame with the event columns like 'touchdown' or 'interception' added.
    """ 
    df = (
        df
        .with_columns(
            down = pl.when(pl.col("down_desc").str.contains("PAT")).then(pl.lit(0)).otherwise(pl.col("down")),
            play_type = 
                pl.when(pl.col("play_result") == "rush").then(pl.lit("rush"))
                .when(pl.col("play_result").is_in(["complete","incomplete","sack","intercepted","good","miss"])).then(pl.lit("pass"))
                .when(pl.col("play_result").is_in(["timeout"])).then(pl.lit("no_play"))
                .when(pl.col("play_result").is_null()).then(pl.lit("no_play"))
                .otherwise(pl.lit(None)), 
            complete_pass = pl.when(pl.col("play_result") == "complete").then(pl.lit(1)).otherwise(pl.lit(0)),
            interception = pl.when(pl.col("play_result") == "intercepted").then(pl.lit(1)).otherwise(pl.lit(0)),
            touchdown = pl.when(pl.col("action_title") == "Touchdown").then(pl.lit(1)).otherwise(pl.lit(0)),
            point_after = pl.when(pl.col("down_desc").str.contains("PAT")).then(pl.lit(1)).otherwise(pl.lit(0)),
            point_after_success = pl.when(pl.col("play_result") == "good").then(pl.lit(1)).otherwise(pl.lit(0)),
            safety = pl.when(pl.col("safety") == "safety").then(pl.lit(1)).otherwise(pl.lit(0)),
            penalty = pl.when(pl.col("penalty") == "penalty").then(pl.lit(1)).otherwise(pl.lit(0))
        )
        .with_columns(
            def_touchdown = 
                pl.when((pl.col('interception') == 1) & (pl.col("touchdown")==1)).then(pl.lit(1))
                .otherwise(pl.lit(0)),
            one_point_conv_success =
                pl.when((pl.col('point_after_success') == 1) & (pl.col('point_after') == 1) & (pl.col("down_desc") == "PAT 5 yards")).then(pl.lit(1))
                .otherwise(pl.lit(0)),
            two_point_conv_success =
                pl.when((pl.col('point_after_success') == 1) & (pl.col('point_after') == 1) & (pl.col("down_desc") == "PAT 10 yards")).then(pl.lit(1))
                .otherwise(pl.lit(0)),
            defensive_two_point_conv =
                pl.when((pl.col('point_after') == 1) & (pl.col("interception") == 1) & (pl.col("touchdown") == 1))
                .then(pl.lit(1))
                .otherwise(pl.lit(0))
        )
        .with_columns(touchdown = pl.when((pl.col("def_touchdown")==1)|(pl.col("point_after")==1)).then(pl.lit(0)).otherwise(pl.col("touchdown")))
        .with_columns(
            scoring_play = 
                pl.when(
                    pl.col('touchdown') |
                    pl.col('def_touchdown') |
                    pl.col('one_point_conv_success') |
                    pl.col('two_point_conv_success') |
                    pl.col('defensive_two_point_conv') |
                    pl.col('safety') 
                    == 1)
                .then(pl.lit(1))
                .otherwise(pl.lit(0))
        )
    )
    return df

def add_scoring_play_team(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        scoring_play_team =
            pl.when((pl.col('scoring_play') == 1) & (pl.col("touchdown") | pl.col("one_point_conv_success") | pl.col("two_point_conv_success") == 1)).then(pl.col("posteam"))
            .when((pl.col('scoring_play') == 1) & (pl.col("def_touchdown") | pl.col("defensive_two_point_conv") | pl.col("safety") == 1)).then(pl.col("defteam"))
            .otherwise(pl.lit(None))
            .alias('scoring_play_team')
        )
    
    return df

def add_team_points(df: pl.DataFrame) -> pl.DataFrame:
    df = (df
          .with_columns(
              home_team_points = pl.when(pl.col("play_id")==1).then(pl.lit(0))
              .when(pl.col('home_team')==pl.col("scoring_play_team"))
              .then(
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
          )
    
    return df

def drop_cols(df: pl.DataFrame) -> pl.DataFrame:
    cols_to_drop = ["drive_id_half","play_id_drive","posteam_abb","yards","start_yard_line","start_yard_line_team_half_id","end_yard_line","end_yard_line_team_half_id"]

    df = df.drop(cols_to_drop)

    return df

