"""sportapp.fi raw JSON snapshot -> canonical plays.

Ports the parsing and mutation helpers of `Python/fetch_sportappfi_api.py` to read
already-fetched snapshots from disk (`data/raw/sportapp/match-drives_{game_id}.json`,
`data/raw/sportapp/match-v1_{game_id}.json`, written by `fetch/sportapp.py`) instead of
calling the network directly. Nothing in this module imports `requests` -- that
responsibility stays entirely in `fetch/sportapp.py` (per the phase research's
snapshot-first decision).

ANTI-PATTERN WARNING -- two independent mutation chains, one canonical output:
sportapp.fi derives `play_type`/`touchdown`/`complete_pass`/etc. from `action_title` and
regex-extracted `summary` text (its own vocabulary, no outcome-token-string equivalent).
Hudl's `ingest/hudl.py` derives the same *named* canonical columns from its own
outcome-token grammar (parsed from an exact-token-match, comma-split field -- a
completely different source vocabulary). These are NOT the same function with different
inputs -- do not attempt to unify sportapp's parsing with `ingest/hudl.py`'s
mutation/token-parsing logic (or vice versa). Convergence between sources happens only at
`canonical.conform_to_canonical` (schema-level) and at the small shared score/points tail
(`canonical.add_scoring_play_team`, `canonical.add_score_columns`), never at shared
outcome-parsing code (phase research's Architecture Pattern 3 / Pattern Map
anti-unification note).

Structural changes from `Python/fetch_sportappfi_api.py`, no semantic ones:

1. No network: `load_snapshot` reads two JSON files from disk. `flatten_plays` operates
   on one already-parsed game's payload at a time (the original's `process_games` fetched
   and concatenated many games before any mutation ran).
2. No unnecessary pandas round-trip: the original went `pd.DataFrame` -> (sort) ->
   `pl.from_pandas` for the *whole* multi-game frame. Here, only the regex-based player/
   play-result extraction (`extract_players_from_summary` in the original) still needs a
   pandas round-trip, done internally by a private helper; every public function in this
   module takes and returns polars frames.

One further structural consequence of moving from "fetch+concat all games, then sort by
(game_id, half, drive_id_half, play_id_drive) descending, then derive ids" to "one game
snapshot at a time": the original's `clean_sort` step is not ported. It existed only to
re-establish per-game/per-half order across a *multi-game concatenated* frame before
`clean_play_ids`' index-diff derivation; a single game's `flatten_plays` output is already
in ascending chronological order straight from the JSON's own drive/eventGroup nesting, so
there is nothing to re-sort. (The original's descending sort was, on inspection, backwards
for a single game's chronology; not re-deriving it here is a bug-fix by omission, not a
behavior change worth preserving.)
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Sequence

import pandas as pd
import polars as pl

from flag_football_ep.canonical import (
    CANONICAL_COLUMNS,
    CORE_COLUMNS,
    NULLABLE_EXTRAS,
    add_score_columns,
    add_scoring_play_team,
    conform_to_canonical,
    make_game_id,
)
from flag_football_ep.reference import map_teams

# One notice string per anomaly encountered for a given game_id (missing snapshot file,
# missing drives array, ...). Empty for a game that ingested cleanly.
IngestNotices = list[str]

_ALL_CANONICAL_DTYPES: dict[str, pl.DataType] = {**CORE_COLUMNS, **NULLABLE_EXTRAS}

_TEAM_MAPPING_COLUMNS = ("posteam", "defteam", "home_team", "away_team", "posteam_after", "defteam_after")


class MissingDrivesArray(Exception):
    """Raised by `flatten_plays` when the match-drives payload has no drives list to walk."""


def _empty_canonical_frame() -> pl.DataFrame:
    """A zero-row frame already conforming to `CANONICAL_COLUMNS`, for skipped games."""
    return pl.DataFrame(schema=dict(_ALL_CANONICAL_DTYPES)).select(list(CANONICAL_COLUMNS))


def load_snapshot(drives_path: Path, match_path: Path) -> tuple[dict, dict]:
    """Read the match-drives and match-v1 JSON snapshots for one game.

    Raises `FileNotFoundError` naming the specific missing path when either file is
    absent, rather than a generic "file not found" with no path.
    """
    drives_path = Path(drives_path)
    match_path = Path(match_path)

    if not drives_path.exists():
        raise FileNotFoundError(f"sportapp snapshot file not found: {drives_path}")
    if not match_path.exists():
        raise FileNotFoundError(f"sportapp snapshot file not found: {match_path}")

    with drives_path.open(encoding="utf-8") as f:
        drives_data = json.load(f)
    with match_path.open(encoding="utf-8") as f:
        match_data = json.load(f)

    return drives_data, match_data


def _extract_drives_list(drives_data: dict) -> list:
    """Pull the list of half-containers out of a match-drives payload.

    Accepts either `{"drives": [...]}` (the on-disk snapshot shape) or a bare top-level
    list (the shape the original in-memory `process_game` assumed the live API response
    to be). Raises `MissingDrivesArray` for anything else -- an error payload, a null
    body, a dict without a "drives" key, etc.
    """
    if isinstance(drives_data, list):
        return drives_data
    if isinstance(drives_data, dict) and isinstance(drives_data.get("drives"), list):
        return drives_data["drives"]
    raise MissingDrivesArray(
        "match-drives payload has no drives array to iterate "
        f"(got {type(drives_data).__name__})"
    )


def flatten_plays(drives_data: dict, match_data: dict, game_id: str) -> pl.DataFrame:
    """One row per play, ported from `Python/fetch_sportappfi_api.py`'s `process_game`.

    `half` comes from the drive container's `num` + 1; `drive_id_half` from the
    event-group's index + 1 (within its half); `play_id_drive` from the play's index + 1
    (within its drive) -- the original's naming and derivation, kept verbatim so
    `clean_play_ids` (which consumes `drive_id_half`) needs no changes. Carries season,
    competition_id/name/league, gender, home_team, away_team, home_score, away_score,
    summary, action_title, down, down_desc, yards_to_go and the yard-line fields through
    to the frame, plus the columns `clean_play_ids`/`correct_posteam`/`clean_yardage`
    need (`posteam_id`, `posteam_abb`, `start_yard_line*`, `end_yard_line*`).

    Raises `MissingDrivesArray` (not caught here -- `ingest_snapshots` is the layer that
    turns this into a per-game skip-with-notice) when `drives_data` has no drives list.
    """
    drives_list = _extract_drives_list(drives_data)

    series = match_data.get("series", {}) or {}
    # `gender` ported verbatim from the original (`series.get("id", 0)`) -- the live API's
    # actual gender field is unverified (RESEARCH: sportapp.fi network shape not
    # re-verified live in this phase); not fixed without evidence it is wrong.
    season = series.get("seasonName", 0)
    competition_id = series.get("id", 0)
    competition_name = series.get("region", 0)
    competition_league = series.get("level", 0)
    gender = series.get("id", 0)
    game_type = series.get("phase", 0)
    game_group_id = series.get("groupId", 0)
    game_group = series.get("groupName", 0)
    streams = match_data.get("streams") or []
    stream_url = streams[0].get("url", "Unknown") if streams else "Unknown"
    home_team = match_data.get("home", {}).get("id", "Unknown")
    away_team = match_data.get("away", {}).get("id", "Unknown")

    result_data = match_data.get("result")
    if result_data is None:
        home_score = 0
        away_score = 0
    else:
        details = result_data.get("details", {}) or {}
        home_score = details.get("points_total_home", 0)
        away_score = details.get("points_total_away", 0)

    rows: list[dict] = []
    for drive in drives_list:
        half = drive.get("num", 0) + 1
        for event_group_index, event_group in enumerate(drive.get("drives", [])):
            drive_id_half = event_group_index + 1
            team = event_group.get("team", {}) or {}
            team_id = team.get("id", "Unknown")
            team_abb = team.get("threeLetters", "Unknown")
            for play_index, play in enumerate(event_group.get("eventGroups", [])):
                play_id_drive = play_index + 1
                start_yard_line = play.get("startYardLine", {}) or {}
                end_yard_line = play.get("endYardLine", {}) or {}
                rows.append(
                    {
                        "season": season,
                        "competition_id": competition_id,
                        "competition_name": competition_name,
                        "competition_league": competition_league,
                        "gender": gender,
                        "game_id": str(game_id),
                        "game_type": game_type,
                        "game_group_id": game_group_id,
                        "game_group": game_group,
                        "half": half,
                        "drive_id_half": drive_id_half,
                        "play_id_drive": play_id_drive,
                        "posteam_id": team_id,
                        "posteam_abb": team_abb,
                        "summary": play.get("summary"),
                        "action_title": play.get("actionTitle"),
                        "down": play.get("down"),
                        "down_desc": play.get("downLabel"),
                        "down_after": play.get("nextDown"),
                        "down_after_desc": play.get("nextDownLabel"),
                        # Stringified up front: raw `target`/`nextTarget` can be an int
                        # or the "G" (goal-to-go) sentinel in the same column, which
                        # polars' schema inference cannot unify across rows. Ported from
                        # the original `convert_to_polars`'s `.astype(str)`, but a real
                        # None stays None here (the original's pandas `.astype(str)`
                        # silently turned every null into the literal string "None",
                        # which would have made `clean_yardage`'s `.is_null()` branch
                        # dead code -- not preserved, since nothing depends on that bug).
                        "yards_to_go": None if play.get("target") is None else str(play.get("target")),
                        "yards_to_go_after": None if play.get("nextTarget") is None else str(play.get("nextTarget")),
                        "yards": event_group.get("yards"),
                        "start_yard_line": start_yard_line.get("yardLine", 0),
                        "start_yard_line_team_half_id": start_yard_line.get("team", 0),
                        "end_yard_line": end_yard_line.get("yardLine", 0),
                        "end_yard_line_team_half_id": end_yard_line.get("team", 0),
                        "possession_time": event_group.get("timeOfPossession"),
                        "home_team": home_team,
                        "away_team": away_team,
                        "home_score": home_score,
                        "away_score": away_score,
                        "stream_url": stream_url,
                    }
                )

    if not rows:
        return pl.DataFrame(
            schema={
                "season": pl.Utf8, "competition_id": pl.Utf8, "competition_name": pl.Utf8,
                "competition_league": pl.Utf8, "gender": pl.Utf8, "game_id": pl.Utf8,
                "game_type": pl.Utf8, "game_group_id": pl.Utf8, "game_group": pl.Utf8,
                "half": pl.Int64, "drive_id_half": pl.Int64, "play_id_drive": pl.Int64,
                "posteam_id": pl.Utf8, "posteam_abb": pl.Utf8, "summary": pl.Utf8,
                "action_title": pl.Utf8, "down": pl.Int64, "down_desc": pl.Utf8,
                "down_after": pl.Int64, "down_after_desc": pl.Utf8, "yards_to_go": pl.Utf8,
                "yards_to_go_after": pl.Utf8, "yards": pl.Int64, "start_yard_line": pl.Int64,
                "start_yard_line_team_half_id": pl.Utf8, "end_yard_line": pl.Int64,
                "end_yard_line_team_half_id": pl.Utf8, "possession_time": pl.Utf8,
                "home_team": pl.Utf8, "away_team": pl.Utf8, "home_score": pl.Int64,
                "away_score": pl.Int64, "stream_url": pl.Utf8,
            }
        )

    return pl.DataFrame(rows, infer_schema_length=len(rows))


# summary regex vocabulary, ported verbatim from `extract_players_from_summary`
# (`Python/fetch_sportappfi_api.py`) -- source-specific text parsing, not shared with
# Hudl's own outcome-token chain (phase research's Architecture Pattern 3 /
# anti-unification note above).
_PASSER_RE = r"#(\d+)\s+(\w+\s\w+)\s+pass"
_RECEIVER_RE = r"pass complete to #(\d+)\s+(\w+\s\w+)"
_RUSHER_RE = r"#(\d+)\s+(\w+\s\w+)\s+rush"
_TACKLE_RE = r"tackled by #(\d+)\s+(\w+\s\w+)"
_INTERCEPTION_RE = r"pass intercepted to #(\d+)\s+(\w+\s\w+)"
_SACK_RE = r"sacked by #(\d+)\s+(\w+\s\w+)"
_PLAY_OUTCOME_RE = (
    r"(rush|complete|incomplete|touchdown|first down|intercepted|sack|fumble|good|miss|timeout)"
)
_PENALTY_RE = r"(penalty)"
_SAFETY_RE = r"(safety)"


def _first_group(pattern: str, text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(pattern, text)
    return match.group(1) if match else None


def _extract_players_from_summary(df: pl.DataFrame) -> pl.DataFrame:
    """Derive passer/receiver/.../play_result/penalty/safety from `summary` text.

    Ported verbatim (regex patterns and precedence unchanged) from
    `extract_players_from_summary`. The original ran this on a pandas `.apply`; kept as a
    pandas round-trip here (acceptable per the module's structural-changes-only scope)
    because the regex helpers are trivially expressed as `Series.apply`, but the public
    function stays polars in, polars out.
    """
    summary = df["summary"].to_pandas()
    extracted = pd.DataFrame(
        {
            "passer": summary.apply(lambda s: _first_group(_PASSER_RE, s)),
            "receiver": summary.apply(lambda s: _first_group(_RECEIVER_RE, s)),
            "rusher": summary.apply(lambda s: _first_group(_RUSHER_RE, s)),
            "tackle_player": summary.apply(lambda s: _first_group(_TACKLE_RE, s)),
            "interception_player": summary.apply(lambda s: _first_group(_INTERCEPTION_RE, s)),
            "sack_player": summary.apply(lambda s: _first_group(_SACK_RE, s)),
            "play_result": summary.apply(lambda s: _first_group(_PLAY_OUTCOME_RE, s)),
            "penalty": summary.apply(lambda s: _first_group(_PENALTY_RE, s)),
            "safety": summary.apply(lambda s: _first_group(_SAFETY_RE, s)),
        }
    )
    return df.hstack(pl.from_pandas(extracted))


def clean_play_ids(df: pl.DataFrame) -> pl.DataFrame:
    """Port of `clean_play_ids`: derives `drive_id`, `play_id`, `half_end` from the
    diff-cumsum chain over `drive_id_half` / row order, scoped `.over("game_id")`.
    """
    df = (
        df.with_row_index(name="index", offset=1)
        .with_columns(
            drive_id=(pl.col("drive_id_half").diff() != 0).cum_sum().over("game_id"),
            play_id=(pl.col("index").diff() != 0).cum_sum().over("game_id"),
        )
        .with_columns(
            drive_id=(pl.col("drive_id") + 1).backward_fill(),
            play_id=(pl.col("play_id") + 1),
        )
        .with_columns(
            play_id=pl.when(pl.col("play_id").is_null()).then(pl.lit(1)).otherwise(pl.col("play_id")),
            play_id_max_half=pl.col("play_id").max().over(["game_id", "half"]),
            play_id_max=pl.col("play_id").max().over("game_id"),
        )
        .with_columns(
            half_end=pl.when(pl.col("play_id_max_half") == pl.col("play_id"))
            .then(pl.lit(1))
            .when(pl.col("play_id_max") == pl.col("play_id"))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
        )
        .drop(["play_id_max_half", "play_id_max"])
    )
    return df


def add_event_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Port of `add_event_columns`: down/PAT and play_type/complete_pass/interception/
    touchdown/safety/penalty derivations from `action_title`, `down_desc` and the
    regex-extracted `play_result`/`penalty`/`safety` text columns -- never from an
    outcome token of Hudl's grammar (that vocabulary belongs to `ingest/hudl.py` only).

    Adds `sack` (not present in the original: `Python/fetch_sportappfi_api.py` never
    computed a boolean sack flag, only a `sack_player` text extraction) so the canonical
    core column of the same name is satisfiable -- `play_result == "sack"`.
    """
    df = (
        df.with_columns(
            down=pl.when(pl.col("down_desc").str.contains("PAT")).then(pl.lit(0)).otherwise(pl.col("down")),
            # WR-11: the raw sportapp play_result value stays "rush" (the predicate
            # below), but the canonical play_type emitted for it is "run", matching
            # canonical.PLAY_TYPE_VOCABULARY -- every other source calls this "run",
            # and a per-source synonym here silently drops sportapp from any
            # downstream play_type == "run" filter.
            play_type=pl.when(pl.col("play_result") == "rush")
            .then(pl.lit("run"))
            .when(pl.col("play_result").is_in(["complete", "incomplete", "sack", "intercepted", "good", "miss"]))
            .then(pl.lit("pass"))
            .when(pl.col("play_result").is_in(["timeout"]))
            .then(pl.lit("no_play"))
            .when(pl.col("play_result").is_null())
            .then(pl.lit("no_play"))
            .otherwise(pl.lit(None)),
            complete_pass=pl.when(pl.col("play_result") == "complete").then(pl.lit(1)).otherwise(pl.lit(0)),
            interception=pl.when(pl.col("play_result") == "intercepted").then(pl.lit(1)).otherwise(pl.lit(0)),
            sack=pl.when(pl.col("play_result") == "sack").then(pl.lit(1)).otherwise(pl.lit(0)),
            touchdown=pl.when(pl.col("action_title") == "Touchdown").then(pl.lit(1)).otherwise(pl.lit(0)),
            point_after=pl.when(pl.col("down_desc").str.contains("PAT")).then(pl.lit(1)).otherwise(pl.lit(0)),
            point_after_success=pl.when(pl.col("play_result") == "good").then(pl.lit(1)).otherwise(pl.lit(0)),
            safety=pl.when(pl.col("safety") == "safety").then(pl.lit(1)).otherwise(pl.lit(0)),
            penalty=pl.when(pl.col("penalty") == "penalty").then(pl.lit(1)).otherwise(pl.lit(0)),
        )
        .with_columns(
            def_touchdown=pl.when((pl.col("interception") == 1) & (pl.col("touchdown") == 1))
            .then(pl.lit(1))
            .otherwise(pl.lit(0)),
            one_point_conv_success=pl.when(
                (pl.col("point_after_success") == 1)
                & (pl.col("point_after") == 1)
                & (pl.col("down_desc") == "PAT 5 yards")
            )
            .then(pl.lit(1))
            .otherwise(pl.lit(0)),
            two_point_conv_success=pl.when(
                (pl.col("point_after_success") == 1)
                & (pl.col("point_after") == 1)
                & (pl.col("down_desc") == "PAT 10 yards")
            )
            .then(pl.lit(1))
            .otherwise(pl.lit(0)),
            defensive_two_point_conv=pl.when(
                (pl.col("point_after") == 1) & (pl.col("interception") == 1) & (pl.col("touchdown") == 1)
            )
            .then(pl.lit(1))
            .otherwise(pl.lit(0)),
        )
        .with_columns(
            touchdown=pl.when((pl.col("def_touchdown") == 1) | (pl.col("point_after") == 1))
            .then(pl.lit(0))
            .otherwise(pl.col("touchdown"))
        )
    )
    return df


def correct_posteam(df: pl.DataFrame) -> pl.DataFrame:
    """Port of `correct_posteam`: reassigns `posteam`/`defteam` to the defense on any
    drive containing a defensive touchdown (interception return), since `posteam_id`
    (the raw event-group team) still names the team that had the ball *before* the
    turnover, not the team that scored.
    """
    df = df.with_columns(posteam_helper=pl.concat_str(pl.col("game_id"), pl.lit("_"), pl.col("drive_id")))

    filter_drive = df.select(pl.col("posteam_helper").filter(pl.col("def_touchdown") == 1)).unique()

    df = (
        df.with_columns(
            defteam_id=pl.when(pl.col("posteam_id") == pl.col("home_team"))
            .then(pl.col("away_team"))
            .when(pl.col("posteam_id") == pl.col("away_team"))
            .then(pl.col("home_team"))
            .otherwise(pl.lit(None))
        )
        .with_columns(
            posteam_helper_2=pl.when(pl.col("posteam_helper").is_in(filter_drive))
            .then(pl.col("play_id"))
            .otherwise(pl.lit(None))
        )
        .with_columns(
            posteam_helper_3=pl.when(pl.col("posteam_helper").is_in(filter_drive))
            .then(pl.col("posteam_helper"))
            .otherwise(pl.lit(None))
        )
        .with_columns(
            posteam_helper_max=pl.when(
                # Explicit parentheses: `&` binds tighter than `==` in Python, so the
                # unparenthesized form parses as `(a == b & c) == 1` — the same
                # precedence trap that corrupted scoring_play in the notebook (see
                # canonical.add_scoring_play_team). Equivalent today only because
                # def_touchdown is strictly 0/1.
                (pl.col("play_id") == pl.col("posteam_helper_2")) & (pl.col("def_touchdown") == 1)
            )
            .then(pl.col("posteam_helper"))
            .otherwise(pl.lit(None))
        )
        .with_columns(posteam_helper_max=pl.col("posteam_helper_max").backward_fill())
        .with_columns(
            posteam=pl.when((pl.col("posteam_helper_3")) == (pl.col("posteam_helper_max")))
            .then(pl.col("defteam_id"))
            .otherwise(pl.col("posteam_id"))
        )
        .with_columns(
            defteam=pl.when(pl.col("posteam") == pl.col("home_team"))
            .then(pl.col("away_team"))
            .when(pl.col("posteam") == pl.col("away_team"))
            .then(pl.col("home_team"))
            .otherwise(pl.lit(None))
        )
        .drop(["posteam_helper", "posteam_helper_2", "posteam_helper_3", "posteam_helper_max", "posteam_id", "defteam_id"])
        .with_columns(posteam_after=pl.col("posteam").shift(-1))
        .with_columns(defteam_after=pl.col("defteam").shift(-1))
    )

    return df


def clean_yardage(df: pl.DataFrame) -> pl.DataFrame:
    """Port of `clean_yardage`: derives `yardline_50`/`yardline_50_after` from the raw
    start/end yard line plus its team-half id (flipped onto a 0..50 home-relative scale,
    then flipped again onto the possessing team's perspective), `yards_gained`,
    `yards_to_go`/`yards_to_go_after` (handling the "G" goal-to-go sentinel and the PAT
    5/10-yard conventions) and `yards_to_go_simple`/`first_down`.

    Requires `posteam`/`home_team`/`away_team` already present -- must run after
    `correct_posteam`, not before (the original module-level function order in
    `Python/fetch_sportappfi_api.py`'s real call site, verified against `api_call.ipynb`,
    is `clean_play_ids -> add_event_columns -> correct_posteam -> clean_yardage`), and
    must run *before* `map_teams` -- it compares the still-raw
    `start_yard_line_team_half_id`/`end_yard_line_team_half_id` against `home_team`/
    `away_team`, which only matches while those are still raw sportapp team ids.

    The "G" (goal-to-go) sentinel and the empty-string/null cases for
    `yards_to_go`/`yards_to_go_after` are already resolved by the preceding `when`
    chain before the cast below runs; the final `cast(pl.Int32, strict=False)` covers
    everything else the API can put in that field (e.g. an unexpected non-numeric
    `target`/`nextTarget` value), turning it into a null instead of raising.
    """
    df = (
        df.with_columns(
            yardline_50=pl.when(pl.col("start_yard_line_team_half_id") == pl.col("away_team"))
            .then(50 - pl.col("start_yard_line"))
            .otherwise(pl.col("start_yard_line")),
            yardline_50_after=pl.when(pl.col("end_yard_line_team_half_id") == pl.col("away_team"))
            .then(50 - pl.col("end_yard_line"))
            .otherwise(pl.col("end_yard_line")),
        )
        .with_columns(
            yardline_50=pl.when(pl.col("posteam") == pl.col("home_team"))
            .then(pl.col("yardline_50"))
            .when(pl.col("posteam") == pl.col("away_team"))
            .then(50 - pl.col("yardline_50"))
            .otherwise(pl.lit(0)),
            yardline_50_after=pl.when(pl.col("posteam") == pl.col("home_team"))
            .then(pl.col("yardline_50_after"))
            .when(pl.col("posteam") == pl.col("away_team"))
            .then(50 - pl.col("yardline_50_after"))
            .otherwise(pl.lit(0)),
        )
        .with_columns(yards_gained=pl.col("yardline_50_after") - pl.col("yardline_50"))
        .with_columns(
            yards_to_go=pl.when(pl.col("yards_to_go") == "G")
            .then(pl.lit(999))
            .when(pl.col("yards_to_go").is_null())
            .then(pl.lit(None))
            .when(pl.col("yards_to_go") == "")
            .then(pl.lit(None))
            .when(pl.col("down_desc") == "PAT 5 yards")
            .then(pl.lit(5))
            .when(pl.col("down_desc") == "PAT 10 yards")
            .then(pl.lit(10))
            .otherwise(pl.col("yards_to_go")),
            yards_to_go_after=pl.when(pl.col("yards_to_go_after") == "G")
            .then(pl.lit(999))
            .when(pl.col("yards_to_go_after").is_null())
            .then(pl.lit(None))
            .when(pl.col("yards_to_go_after") == "")
            .then(pl.lit(None))
            .otherwise(pl.col("yards_to_go_after")),
        )
        .with_columns(
            pl.col("yards_to_go").cast(pl.Int32, strict=False),
            pl.col("yards_to_go_after").cast(pl.Int32, strict=False),
        )
        .with_columns(
            yards_to_go=pl.when(pl.col("yards_to_go") == 999)
            .then(50 - pl.col("yardline_50"))
            .otherwise(pl.col("yards_to_go")),
            yards_to_go_after=pl.when(pl.col("yards_to_go_after") == 999)
            .then(50 - pl.col("yardline_50_after"))
            .otherwise(pl.col("yards_to_go_after")),
        )
        .with_columns(
            yards_to_go_simple=pl.when(pl.col("yards_to_go") <= 5)
            .then(pl.lit(1))
            .when((pl.col("yards_to_go") > 5) & (pl.col("yards_to_go") <= 10))
            .then(pl.lit(2))
            .when((pl.col("yards_to_go") > 10) & (pl.col("yards_to_go") <= 15))
            .then(pl.lit(3))
            .when((pl.col("yards_to_go") > 15) & (pl.col("yards_to_go") <= 20))
            .then(pl.lit(4))
            .when(pl.col("yards_to_go") > 20)
            .then(pl.lit(5))
            .otherwise(pl.lit(0))
        )
        .with_columns(
            first_down=pl.when(
                (pl.col("yardline_50") < 25) & (pl.col("yards_gained") > pl.col("yards_to_go"))
            )
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
        )
        # Hudl-chain-consistent binning (`Python/helper_add_hudl_mutations.py`): not
        # computed anywhere in the original sportapp.fi pipeline, but required by
        # `canonical.CORE_COLUMNS`. Same 0/1 split at the 25-yard line as Hudl's own
        # `yardline_50_simple`, so this feature means the same thing across sources.
        .with_columns(
            yardline_50_simple=pl.when(pl.col("yardline_50") < 25).then(pl.lit(0)).otherwise(pl.lit(1))
        )
    )

    return df


def add_team_points_chain(df: pl.DataFrame) -> pl.DataFrame:
    """Delegates to `canonical.add_score_columns` for the points/cumsum/differential
    tail, replacing the original `add_team_points` (RESEARCH: this tail is structurally
    identical across sources once the scoring booleans exist, so it is the one piece of
    the mutation chain safe to share as literal code, not just as a schema contract).
    """
    return add_score_columns(df)


def _finalize_canonical_columns(df: pl.DataFrame, source_game_id: str) -> pl.DataFrame:
    """Rename/derive the remaining source-specific fields onto their canonical names.

    `game_id` becomes the canonical `make_game_id("sportapp", source_game_id)`; the raw
    per-row sportapp game id is preserved as `source_game_id`. Extras: `summary`->
    `description`, `passer`->`qb`, `receiver`->`received_by`, `rusher`->`target`,
    `competition_name`->`competition`, `play_result`->`result_raw`, plus `gender` and
    `yardline` (best-effort: sportapp has no separate raw-yardline concept distinct from
    the normalized `yardline_50`, so `yardline` mirrors it).
    """
    canonical_game_id = make_game_id("sportapp", str(source_game_id))

    df = df.rename({"game_id": "source_game_id"})
    df = df.with_columns(
        pl.col("source_game_id").cast(pl.Utf8),
        # A placeholder value: `conform_to_canonical` requires "source" to already be a
        # column (it checks CORE_COLUMNS presence before stamping the real value), then
        # overwrites every row with the `source=` argument it is called with.
        source=pl.lit("sportapp"),
        game_id=pl.lit(canonical_game_id),
        competition=pl.col("competition_name").cast(pl.Utf8),
        result_raw=pl.col("play_result"),
        yardline=pl.col("yardline_50"),
        description=pl.col("summary"),
        qb=pl.col("passer"),
        received_by=pl.col("receiver"),
        target=pl.col("rusher"),
    )
    return df


def ingest_snapshots(
    raw_dir: Path,
    team_mapping: pl.DataFrame,
    game_ids: Sequence[str] | None = None,
) -> list[tuple[str, pl.DataFrame, IngestNotices]]:
    """Run the full sportapp.fi snapshot -> canonical chain for each game id.

    `game_ids` defaults to every id discoverable from `match-drives_*.json` filenames
    under `raw_dir`. For each game id, returns `(game_id, canonical_frame, notices)`:
    on success, `notices` is empty and `canonical_frame` has exactly
    `CANONICAL_COLUMNS` with `source == "sportapp"`; when a snapshot file is missing or
    its payload has no drives array, `canonical_frame` is an empty (0-row)
    canonical-schema frame and `notices` names the reason -- the game is skipped, not
    the whole run (T-1.2-03). The per-game mutation chain (`_extract_players_from_summary`
    through `conform_to_canonical`) is also wrapped: a `pl.exceptions.PolarsError`
    raised anywhere in that chain (e.g. a strict-cast/schema error surfaced despite
    `clean_yardage`'s own non-strict casts) is caught, recorded as a notice naming the
    exception class, and skips only that game, the same containment already applied to
    a missing snapshot file or a missing drives array. An unmapped team label raises
    `UnmappedTeamError` (T-1.2-15) rather than being folded into a notice -- it is not a
    `PolarsError`, so the chain's narrow catch does not touch it; it signals a
    reference-data gap that needs a human fix, not a per-game data anomaly (see
    `test_ingest_snapshots_unmapped_team_raises`).
    """
    raw_dir = Path(raw_dir)

    if game_ids is None:
        game_ids = sorted(
            p.name[len("match-drives_"):-len(".json")]
            for p in raw_dir.glob("match-drives_*.json")
        )

    results: list[tuple[str, pl.DataFrame, IngestNotices]] = []

    for game_id in game_ids:
        notices: IngestNotices = []
        drives_path = raw_dir / f"match-drives_{game_id}.json"
        match_path = raw_dir / f"match-v1_{game_id}.json"

        try:
            drives_data, match_data = load_snapshot(drives_path, match_path)
        except FileNotFoundError as exc:
            notices.append(f"game {game_id}: {exc}")
            results.append((game_id, _empty_canonical_frame(), notices))
            continue

        try:
            df = flatten_plays(drives_data, match_data, game_id)
        except MissingDrivesArray as exc:
            notices.append(f"game {game_id}: {exc}")
            results.append((game_id, _empty_canonical_frame(), notices))
            continue

        if df.height == 0:
            notices.append(f"game {game_id}: snapshot has a drives array but zero plays")
            results.append((game_id, _empty_canonical_frame(), notices))
            continue

        # UnmappedTeamError (raised by map_teams below) is deliberately NOT caught
        # here: it is a plain Exception, not a pl.exceptions.PolarsError, so this
        # narrow catch already excludes it -- an unmapped team must keep aborting
        # loudly per CONTEXT.md's team-identity decision (T-1.2-15); see
        # test_ingest_snapshots_unmapped_team_raises, which is unaffected by this
        # containment and must keep passing unchanged.
        try:
            df = _extract_players_from_summary(df)
            df = clean_play_ids(df)
            df = add_event_columns(df)
            df = correct_posteam(df)
            # clean_yardage compares start/end yard-line team-half ids (still raw
            # sportapp ids) against home_team/away_team -- map_teams must run after,
            # not before, or that comparison silently never matches once
            # home_team/away_team are canonical codes.
            df = clean_yardage(df)
            df = map_teams(df, team_mapping, "sportapp", list(_TEAM_MAPPING_COLUMNS))
            df = add_scoring_play_team(df, credit_defense=True)
            df = add_team_points_chain(df)
            df = _finalize_canonical_columns(df, game_id)
            df, _report = conform_to_canonical(df, source="sportapp")
        except pl.exceptions.PolarsError as exc:
            notices.append(f"game {game_id}: {type(exc).__name__}: {exc}")
            results.append((game_id, _empty_canonical_frame(), notices))
            continue

        results.append((game_id, df, notices))

    return results


# --- Grandfathered WC24 2024 CSV (plan 11 task 1 decision: re-derive-with-fallback) -----
#
# `data/raw/legacy/wc24_pbp.csv` (14,545 rows) is NOT raw sportapp.fi JSON -- RESEARCH
# verified it is the fully-mutated output of the pre-port `Python/fetch_sportappfi_api.py`
# pipeline for the WC24 2024 tournament (it already carries `complete_pass`, `drive_id`,
# `home_team_score`, etc.). The plan 11 task 1 checkpoint recorded a
# `re-derive-with-fallback` decision: attempt a live re-fetch through
# `ingest_snapshots`/`fetch/sportapp.py` when `SPORTAPP_API_KEY` resolves, otherwise
# grandfather this file exactly like `data_raw.csv`'s "legacy" source -- warn-only, never
# re-validated at source. Today `SPORTAPP_API_KEY` does not resolve (no `.env`, key
# rotation deferred per STATE.md's recorded blocker) and this branch is the one actually
# exercised; see the plan 11 SUMMARY for the numbers and rationale.
_MUTATED_SPORTAPP_SCHEMA_OVERRIDES: dict[str, pl.DataType] = {
    "posteam": pl.Utf8,
    "posteam_after": pl.Utf8,
    "defteam": pl.Utf8,
    "defteam_after": pl.Utf8,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "scoring_play_team": pl.Utf8,
    "penalty": pl.Int32,
}


def read_mutated_sportapp_snapshot(path: Path) -> pl.DataFrame:
    """Read the grandfathered, already-mutated WC24 CSV into the canonical schema.

    Forces the team-identity columns to Utf8 (otherwise polars infers them as integers,
    since sportapp.fi team ids look numeric, breaking canonical's Utf8 contract and any
    string equality downstream) and `penalty` to Int32 (matches the canonical dtype; the
    original notebook's CSV round-trip can leave it Int64). Every other Int64 column is
    then cast down to Int32 to match `canonical.CORE_COLUMNS`.

    Column names already match the canonical vocabulary almost 1:1 (this file predates
    the canonical schema, but the schema was named to match it -- see
    `Python/fetch_sportappfi_api.py`); this only renames/derives the handful that don't:
    `game_id` -> `source_game_id` (+ a new composite `game_id`), `competition_name` ->
    `competition`, `play_result` -> `result_raw`, `summary` -> `description`, `passer` ->
    `qb`, `receiver` -> `received_by`, `rusher` -> `target`, `tackle_player` -> `tackle`
    (mirroring `_finalize_canonical_columns`'s fresh-ingest mapping so both sportapp paths
    populate the same extras the same way), plus deriving `sack` and `yardline_50_simple`
    (never computed by the original pipeline, but required core columns -- same additions
    as the fresh-ingest path, for the same reason).

    WR-11: this pre-port CSV predates `canonical.PLAY_TYPE_VOCABULARY` and carries its
    own pre-computed `play_type` column with the raw sportapp value `rush` where the
    canonical vocabulary says `run`; that value is normalized here the same way the
    fresh-ingest path's `add_event_columns` normalizes it, leaving every other value
    (including null) untouched.

    Stamps `source="legacy-sportapp"` -- not `"sportapp"` -- so the validation/ingest
    pipeline can treat these rows warn-only (like `data_raw.csv`'s `"legacy"` source):
    they ran through pre-port mutation code that predates every fix and derivation this
    module makes, and cannot be re-validated at source.
    """
    path = Path(path)
    df = pl.read_csv(path, schema_overrides=_MUTATED_SPORTAPP_SCHEMA_OVERRIDES)

    int64_columns = [name for name, dtype in df.schema.items() if dtype == pl.Int64]
    if int64_columns:
        df = df.with_columns([pl.col(name).cast(pl.Int32) for name in int64_columns])

    df = df.rename({"game_id": "source_game_id"})
    df = df.with_columns(
        pl.col("source_game_id").cast(pl.Utf8),
        source=pl.lit("legacy-sportapp"),
        # Mirrors `canonical.make_game_id`'s non-"hudl" branch (`f"{source}-{key}"`)
        # without a per-row Python call over 14k+ rows.
        game_id=pl.concat_str([pl.lit("legacy-sportapp-"), pl.col("source_game_id")]),
        competition=pl.col("competition_name").cast(pl.Utf8),
        result_raw=pl.col("play_result"),
        yardline=pl.col("yardline_50"),
        description=pl.col("summary"),
        qb=pl.col("passer"),
        received_by=pl.col("receiver"),
        target=pl.col("rusher"),
        tackle=pl.col("tackle_player"),
        sack=pl.when(pl.col("play_result") == "sack").then(pl.lit(1)).otherwise(pl.lit(0)),
        yardline_50_simple=pl.when(pl.col("yardline_50") < 25).then(pl.lit(0)).otherwise(pl.lit(1)),
        play_type=pl.when(pl.col("play_type") == "rush")
        .then(pl.lit("run"))
        .otherwise(pl.col("play_type")),
    )

    df, _report = conform_to_canonical(df, source="legacy-sportapp")
    return df
