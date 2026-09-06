"""cpx.studio (IFAF WM-2026) snapshot parser: `unified-plays` JSON -> canonical plays.

Reads the raw snapshots `fetch/ifaf.py` already wrote to disk (`data/raw/ifaf/`);
no network access happens here. Implements `docs/ifaf-field-mapping.md` exactly —
see that document for the per-field evidence (`observed`/`documented`/`absent`)
this parser is built against, including why `context.ballOn` maps onto
`yardline_50` with an identity transform, and why `yards_to_go` is derived from
field position (`derive_yards_to_go`) rather than trusted from the payload's own
`context.yardsToGo` (a hardcoded constant, not real per-play distance data).

Convergence with the other ingest sources (hudl, legacy, sportapp) happens only at
`canonical.conform_to_canonical` — this module never reuses the Hudl `RESULT`
token parser or the sportapp free-text summary parser. `OUTCOME_MAP` is this
source's own, from-scratch vocabulary, driven entirely by the `outcome.type`
values documented in the mapping doc's outcome-vocabulary section.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

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

_ALL_CANONICAL_DTYPES: dict[str, pl.DataType] = {**CORE_COLUMNS, **NULLABLE_EXTRAS}

_PLAYS_LIST_KEYS = ("plays", "data", "items")

# outcome.type -> the single canonical flag it sets when matched (None = no flag,
# just a recognized value that leaves every flag at 0). Driven entirely from
# docs/ifaf-field-mapping.md's outcome-vocabulary section; adding a newly observed
# value is a one-line change here. The five scoring-shaped types below all route
# through "scoring" instead of a fixed flag name — see _SCORING_OUTCOME_TYPES.
OUTCOME_MAP: dict[str, str | None] = {
    "FLAG_PULL": None,
    "INCOMPLETE_PASS": "incomplete_pass",
    "TOUCHDOWN": "scoring",
    "COMPLETE_PASS": "complete_pass",
    "TURNOVER": None,
    "TRY": "scoring",
    "MIDDLE_LINE": None,
    "INTERCEPTION": "interception",
    "SACK": "sack",
    "XP1": "scoring",
    "TD": "scoring",
    "XP2": "scoring",
    "RUN": None,
    "SAFETY": "safety",
}

# Canonical `play_type` for the outcome types where the play form is unambiguous
# (REVIEW WR-04: parsed IFAF plays must not sit at null, or every downstream
# `play_type == "run"` / `== "pass"` filter silently excludes the whole corpus).
# Values must stay inside `canonical.PLAY_TYPE_VOCABULARY`. Deliberately absent:
# TOUCHDOWN/TD (could be a run or a pass — the type string carries no play form),
# FLAG_PULL, TURNOVER, MIDDLE_LINE, SAFETY (events, not play forms) — those stay
# null, with `result_raw` as the record.
_PLAY_TYPE_FROM_OUTCOME: dict[str, str] = {
    "RUN": "run",
    "COMPLETE_PASS": "pass",
    "INCOMPLETE_PASS": "pass",
    "SACK": "pass",
    "INTERCEPTION": "pass",
    "XP1": "extra_point",
    "XP2": "extra_point",
    "TRY": "extra_point",
}

# Fallback classifier for the outcome types _PLAY_TYPE_FROM_OUTCOME leaves at
# null (TOUCHDOWN/TD, FLAG_PULL, TURNOVER, MIDDLE_LINE, SAFETY, penalty-only,
# and the `outcome` key absent entirely) -- these still carry the play's own
# `sequence` action list, which names the play form directly (2026-09-06
# live-data finding, docs/ifaf-field-mapping.md yardage-derivation addendum).
# e.g. a TOUCHDOWN whose sequence is `SNAP, QB_SET, PASS, COMPLETE,
# TOUCHDOWN` is a pass play; `SNAP, QB_SET, HAND_OFF, RUSH, TOUCHDOWN` is a
# run. Pass-shaped actions are checked before rush-shaped ones so a completed
# catch followed by a RUSH token (yards after catch, not a designed running
# play) still classifies as "pass". Only tokens already in
# `canonical.PLAY_TYPE_VOCABULARY` are ever produced here -- no new contract
# token is introduced. A sequence with none of these tokens (LATERAL-only,
# SNAP/QB_SET-only, or empty) stays None, same as before this fallback existed.
_SEQUENCE_PASS_ACTIONS = frozenset(
    {"PASS", "COMPLETE", "INCOMPLETE_PASS", "INTERCEPTION", "SACK"}
)
_SEQUENCE_RUN_ACTIONS = frozenset({"RUSH", "HAND_OFF"})


def _play_type_from_sequence(sequence: Any) -> str | None:
    if not isinstance(sequence, list):
        return None
    actions = {
        step.get("action") for step in sequence if isinstance(step, dict)
    }
    if actions & _SEQUENCE_PASS_ACTIONS:
        return "pass"
    if actions & _SEQUENCE_RUN_ACTIONS:
        return "run"
    return None


def _sequence_has_middle_line(sequence: Any) -> bool:
    """True when the play's own `sequence` action list names `MIDDLE_LINE`
    directly (10 occurrences in the live corpus, distinct from -- but
    consistent with -- `outcome.type == "MIDDLE_LINE"`, 145 occurrences).
    Used by `derive_yards_to_go` as a second, independent crossing signal
    alongside `yardline_50 >= MIDFIELD_YARDLINE`."""
    if not isinstance(sequence, list):
        return False
    return any(
        isinstance(step, dict) and step.get("action") == "MIDDLE_LINE" for step in sequence
    )

# Live-data finding (docs/ifaf-field-mapping.md): `outcome.type` alone is not a
# reliable scoring signal for these five types. "TOUCHDOWN" plays are sometimes
# actually 1- or 2-point conversions (description.kind == "TRY" on those rows,
# outcome.pointsScored == 1 or 2 instead of 6), and some carry no pointsScored
# at all despite the "TOUCHDOWN"/"TD" label — those never move the real
# scoreboard (context.score is unchanged across them), consistent with an
# overturned/nullified play. `outcome.pointsScored` — not the type string — is
# therefore the authoritative signal for what actually scored, for all five of
# these types uniformly.
_SCORING_OUTCOME_TYPES = {"TOUCHDOWN", "TD", "TRY", "XP1", "XP2"}
_POINTS_TO_FLAG: dict[int, str] = {
    6: "touchdown",  # def_touchdown instead when outcome.turnover is True
    2: "two_point_conv_success",  # defensive_two_point_conv instead when outcome.turnover is True
    1: "one_point_conv_success",  # a 1-point conversion cannot be returned by the
    # defense in this vocabulary, so this case is not split on turnover
}

_UNCONDITIONAL_FLAG_NAMES = (
    "touchdown",
    "def_touchdown",
    "safety",
    "interception",
    "complete_pass",
    "incomplete_pass",
    "sack",
    "one_point_conv_success",
    "two_point_conv_success",
    "defensive_two_point_conv",
)

# Working frame schema: every canonical CORE column this module can populate
# directly, plus a handful of `_`-prefixed working columns that later stages
# (map_teams, derive_outcome_columns, add_scoring_play_team/add_score_columns)
# consume. The `_`-prefixed columns are not in CANONICAL_COLUMNS, so
# `conform_to_canonical`'s final `select` drops them automatically.
_WORKING_SCHEMA: dict[str, pl.DataType] = {
    "source": pl.Utf8,
    "source_game_id": pl.Utf8,
    "game_id": pl.Utf8,
    "play_id": pl.Int32,
    "drive_id": pl.Int32,
    "half": pl.Int32,
    "down": pl.Int32,
    "yards_to_go": pl.Int32,
    "yardline": pl.Int32,
    "yardline_50": pl.Int32,
    "yardline_50_after": pl.Int32,
    "yardline_50_simple": pl.Int32,
    "yards_to_go_simple": pl.Int32,
    "yards_gained": pl.Int32,
    "first_down": pl.Int32,
    "game_clock_ms": pl.Int64,
    "half_seconds_remaining": pl.Float64,
    "posteam": pl.Utf8,
    "posteam_after": pl.Utf8,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "defteam": pl.Utf8,
    "play_type": pl.Utf8,
    "result_raw": pl.Utf8,
    "description": pl.Utf8,
    "competition": pl.Utf8,
    "season": pl.Int32,
    "gender": pl.Utf8,
    "tournament_id": pl.Utf8,
    "_outcome_turnover": pl.Boolean,
    "_outcome_points_scored": pl.Int32,
    "_penalty": pl.Boolean,
    "_context_score_home": pl.Int32,
    "_context_score_away": pl.Int32,
    "_missing_down": pl.Int32,
    "_missing_ballon": pl.Int32,
    "_missing_possession": pl.Int32,
    "_sequence_play_type": pl.Utf8,
    "_sequence_middle_line": pl.Boolean,
    "_outcome_middle_line": pl.Boolean,
}


class UnparseablePayload(Exception):
    """Raised when a snapshot file is neither a top-level list nor an object
    wrapping the play array under `plays`/`data`/`items` — or is not valid JSON."""


@dataclass
class IngestNotices:
    """Machine-readable record of what `ingest_snapshots` had to work around for
    one game, folded from the working-column markers `flatten_unified_plays` and
    `derive_outcome_columns` leave behind."""

    game_id: str
    missing_context_keys: dict[str, int] = field(default_factory=dict)
    unmapped_outcomes: dict[str, int] = field(default_factory=dict)
    score_mismatches: int = 0
    skipped: bool = False
    skip_reason: str | None = None
    messages: list[str] = field(default_factory=list)


def _empty_canonical_frame() -> pl.DataFrame:
    """A zero-row frame already conforming to `CANONICAL_COLUMNS`, for skipped games."""
    return pl.DataFrame(schema=dict(_ALL_CANONICAL_DTYPES)).select(list(CANONICAL_COLUMNS))


def _extract_plays_list(payload: Any) -> list | None:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in _PLAYS_LIST_KEYS:
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return None


def load_snapshot(
    plays_path: Path, tournament_path: Path | None = None
) -> tuple[list, dict | None]:
    """Read one `unified-plays_{game_id}.json` snapshot from disk.

    Accepts either a top-level list payload or an object wrapping the play array
    under `plays`, `data` or `items`. Raises `UnparseablePayload` naming the file
    when neither shape is present, or when the file is not valid JSON. An empty
    list (`[]`) is a valid, real payload (a genuinely empty game, e.g. a forfeit),
    not an error.

    `tournament_path`, if given and present on disk, is read and returned
    verbatim as the second tuple element (`None` otherwise).
    """
    plays_path = Path(plays_path)
    try:
        with plays_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        raise UnparseablePayload(f"{plays_path}: could not read/parse JSON ({exc})") from exc

    plays = _extract_plays_list(payload)
    if plays is None:
        raise UnparseablePayload(
            f"{plays_path}: unrecognized unified-plays payload shape "
            "(expected a top-level list or an object with a 'plays'/'data'/'items' list)"
        )

    tournament_payload: dict | None = None
    if tournament_path is not None:
        tournament_path = Path(tournament_path)
        if tournament_path.exists():
            try:
                with tournament_path.open("r", encoding="utf-8") as f:
                    tournament_payload = json.load(f)
            except (OSError, json.JSONDecodeError):
                tournament_payload = None

    return plays, tournament_payload


def _play_sort_key(index: int, play: Any) -> tuple[int, int, int]:
    """Sort key for one payload entry, resilient to malformed input.

    A play carrying a usable integer `playNumber` (a real `int`, not a `bool` --
    `bool` is an `int` subclass in Python) sorts first, in `playNumber` order.
    Everything else -- a missing key, a null `playNumber`, a non-int value, or a
    non-dict entry altogether -- sorts after all of those, in stable payload
    order. This deliberately replaces the previous `play.get("playNumber",
    index)` fallback, which mixed index values into the same ordering space as
    real play numbers and raised `TypeError` the moment a real `playNumber` (an
    `int`) was compared against a fallback `None`.
    """
    number = play.get("playNumber") if isinstance(play, dict) else None
    if isinstance(number, int) and not isinstance(number, bool):
        return (0, number, index)
    return (1, 0, index)


def _other_team(posteam: str | None, home: str | None, away: str | None) -> str | None:
    if posteam is None:
        return None
    if posteam == home:
        return away
    if posteam == away:
        return home
    return None


def flatten_unified_plays(payload: list, game_meta: dict, game_id: str) -> pl.DataFrame:
    """Turn one game's `unified-plays` array into one canonical-shaped row per play.

    `play_id` is assigned 1..N by sorting on the payload's own `playNumber`, not by
    trusting it verbatim — gaps exist in the real data. Plays carrying a usable
    integer `playNumber` sort first, in `playNumber` order; everything else (an
    absent key, a null `playNumber`, a non-int value, or a non-dict entry) sorts
    after all of those, in stable payload order (see `_play_sort_key`). `drive_id`
    starts at 1 and increments only when `context.possessionTeamId` changes
    between two plays where it is known; a null possession id (missing metadata)
    keeps the current drive instead of advancing it or starting the game at an
    out-of-contract 0.
    `yards_to_go` is always left null (see the module docstring). `posteam`/
    `defteam`/`home_team`/`away_team` carry raw cpx.studio team labels here;
    `ingest_snapshots` maps them onto canonical team codes afterward.
    """
    home_raw = game_meta.get("home_team")
    away_raw = game_meta.get("away_team")
    competition = game_meta.get("competition")
    season = game_meta.get("season")
    gender = game_meta.get("gender")
    tournament_id = game_meta.get("tournament_id")

    ordered = sorted(enumerate(payload), key=lambda pair: _play_sort_key(pair[0], pair[1]))

    rows: list[dict] = []
    # drive_id seeds at 1 so a game whose first play lacks possessionTeamId still
    # starts in-contract (monotonic_drive_ids expects the first drive to be 1, not
    # 0). A null possessionTeamId never advances the drive counter — only a change
    # between two known possession ids does — so missing metadata degrades to
    # "same drive" instead of quarantining the game (REVIEW WR-05).
    prev_posteam_raw: str | None = None
    drive_id = 1

    for play_id, (_, play) in enumerate(ordered, start=1):
        context = play.get("context") or {}
        outcome = play.get("outcome") or {}
        description_obj = play.get("description") or {}
        score = context.get("score") or {}

        posteam_raw = context.get("possessionTeamId")
        if posteam_raw is not None:
            if prev_posteam_raw is not None and posteam_raw != prev_posteam_raw:
                drive_id += 1
            prev_posteam_raw = posteam_raw

        description_text = (
            description_obj.get("text")
            or description_obj.get("detail")
            or description_obj.get("label")
        )

        rows.append(
            {
                "source": "ifaf",
                "source_game_id": str(game_id),
                "game_id": make_game_id("ifaf", game_id),
                "play_id": play_id,
                "drive_id": drive_id,
                "half": context.get("half"),
                "down": context.get("down"),
                "yards_to_go": None,
                "yardline": None,
                "yardline_50": context.get("ballOn"),
                "yardline_50_after": None,
                "yardline_50_simple": None,
                "yards_to_go_simple": None,
                "yards_gained": None,
                "first_down": None,
                "game_clock_ms": context.get("gameClockMs"),
                "half_seconds_remaining": None,
                "posteam": posteam_raw,
                "posteam_after": None,
                "home_team": home_raw,
                "away_team": away_raw,
                "defteam": _other_team(posteam_raw, home_raw, away_raw),
                "play_type": None,
                "result_raw": outcome.get("type"),
                "description": description_text,
                "competition": competition,
                "season": season,
                "gender": gender,
                "tournament_id": tournament_id,
                "_outcome_turnover": bool(outcome.get("turnover")),
                "_outcome_points_scored": outcome.get("pointsScored"),
                "_penalty": bool(play.get("penalty")),
                "_context_score_home": score.get("home"),
                "_context_score_away": score.get("away"),
                "_missing_down": 0 if "down" in context else 1,
                "_missing_ballon": 0 if "ballOn" in context else 1,
                "_missing_possession": 0 if "possessionTeamId" in context else 1,
                "_sequence_play_type": _play_type_from_sequence(play.get("sequence")),
                "_sequence_middle_line": _sequence_has_middle_line(play.get("sequence")),
                "_outcome_middle_line": outcome.get("type") == "MIDDLE_LINE",
            }
        )

    return pl.DataFrame(rows, schema=_WORKING_SCHEMA)


def derive_outcome_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Map `result_raw` (the observed `outcome.type` vocabulary) onto canonical flags.

    Driven entirely by `OUTCOME_MAP`. For the five scoring-shaped types
    (`TOUCHDOWN`, `TD`, `TRY`, `XP1`, `XP2`), the flag is chosen by
    `_outcome_points_scored` alone, not the type string — the live data shows
    `outcome.type` is not reliable on its own (some "TOUCHDOWN" rows are really
    1- or 2-point conversions, and some carry no `pointsScored` at all despite a
    scoring-shaped type, matching zero actual scoreboard movement — see
    `_SCORING_OUTCOME_TYPES`'s docstring). A points value of 6 sets `touchdown`
    (or `def_touchdown` instead when `_outcome_turnover` is true — a defensive
    touchdown), 2 sets `two_point_conv_success` (or `defensive_two_point_conv`
    instead when `_outcome_turnover` is true — a 2-point attempt returned by the
    defense, mirroring the 6-point split), 1 sets `one_point_conv_success`
    unconditionally (a 1-point conversion cannot be returned by the defense in
    this vocabulary), and any other/missing value sets no flag, leaving
    `result_raw` as the only record of the attempt. `penalty` is copied directly
    from the payload's own
    top-level `penalty` boolean, not inferred from `outcome.type`. A `result_raw`
    value that is non-null and not a key of `OUTCOME_MAP` sets `_unmapped_outcome`
    instead of any flag; `ingest_snapshots` folds that marker into `IngestNotices`.
    `play_type` is set from `_PLAY_TYPE_FROM_OUTCOME` for the form-unambiguous
    outcome types (RUN -> run; the pass-shaped types -> pass; XP1/XP2/TRY ->
    extra_point); where that leaves it null, `_sequence_play_type` (already
    computed per-play in `flatten_unified_plays` from the play's own
    `sequence` action list) fills in "run"/"pass" for TOUCHDOWN/TD,
    FLAG_PULL, TURNOVER, MIDDLE_LINE, SAFETY and penalty-only plays where the
    sequence names an unambiguous play form; stays null only when neither
    source can determine one (see `_play_type_from_sequence`'s docstring).
    """
    result_raw = pl.col("result_raw")
    turnover = pl.col("_outcome_turnover")
    points = pl.col("_outcome_points_scored")

    flag_exprs: dict[str, pl.Expr] = {name: pl.lit(False) for name in _UNCONDITIONAL_FLAG_NAMES}

    for outcome_type, flag_name in OUTCOME_MAP.items():
        if flag_name is None:
            continue
        matches = result_raw == outcome_type

        if outcome_type in _SCORING_OUTCOME_TYPES:
            for pts_value, target_flag in _POINTS_TO_FLAG.items():
                # Kleene three-valued logic: comparing a null `points` to an int
                # yields null, which would otherwise poison the OR-chain below
                # with null instead of False (a missing pointsScored must mean
                # "this attempt did not score").
                pts_matches = matches & (points == pts_value).fill_null(False)
                if target_flag == "touchdown":
                    flag_exprs["touchdown"] = flag_exprs["touchdown"] | (pts_matches & (~turnover))
                    flag_exprs["def_touchdown"] = (
                        flag_exprs["def_touchdown"] | (pts_matches & turnover)
                    )
                elif target_flag == "two_point_conv_success":
                    flag_exprs["two_point_conv_success"] = (
                        flag_exprs["two_point_conv_success"] | (pts_matches & (~turnover))
                    )
                    flag_exprs["defensive_two_point_conv"] = (
                        flag_exprs["defensive_two_point_conv"] | (pts_matches & turnover)
                    )
                else:
                    flag_exprs[target_flag] = flag_exprs[target_flag] | pts_matches
            continue

        flag_exprs[flag_name] = flag_exprs[flag_name] | matches

    df = df.with_columns(
        [expr.fill_null(False).cast(pl.Int32).alias(name) for name, expr in flag_exprs.items()]
    )

    known_types = list(OUTCOME_MAP.keys())
    df = df.with_columns(
        [
            pl.col("_penalty").cast(pl.Int32).alias("penalty"),
            (result_raw.is_not_null() & (~result_raw.is_in(known_types)))
            .cast(pl.Int32)
            .alias("_unmapped_outcome"),
            # Unambiguous outcome types get their canonical play_type first;
            # where that is null, fall back to the sequence-derived classification
            # (TOUCHDOWN/TD/FLAG_PULL/etc. with an unambiguous action list).
            # Genuinely form-ambiguous plays (empty/LATERAL-only sequence, no
            # outcome match) stay null per the null-is-for-unparsed contract in
            # docs/pipeline.md.
            pl.coalesce(
                [
                    result_raw.replace_strict(
                        _PLAY_TYPE_FROM_OUTCOME, default=None, return_dtype=pl.Utf8
                    ),
                    pl.col("_sequence_play_type"),
                ]
            ).alias("play_type"),
        ]
    )
    return df


def derive_yardage_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Derive `yards_gained` from consecutive plays' `yardline_50` (`ballOn`)
    within one game, ordered by `play_id` (already contiguous 1..N — see
    `flatten_unified_plays`). Must run after `derive_outcome_columns` (needs
    the `touchdown`/`safety`/`interception`/`def_touchdown`/
    `defensive_two_point_conv` flags) on a single game's frame — this is not
    grouped `.over("game_id")`, matching how `ingest_snapshots` already calls
    every per-game derivation one game at a time.

    Rules (docs/ifaf-field-mapping.md yardage-derivation addendum), in
    priority order:

    1. A play carrying the top-level `penalty` flag (`_penalty`) stays null —
       a penalty can move the spot by rule, not by a real play result, and
       must never produce a fabricated gain.
    2. An offensive touchdown (`touchdown == 1`): `yards_gained = 50 -
       yardline_50` (distance from the snap spot to the opponent goal line;
       `yardline_50` is already "yards from own goal", so 50 is the opponent
       goal in this project's convention — see the ballOn-semantics section).
       This overrides the next-row lookup because the next row is a TRY/
       kickoff at a reset spot, not a continuation of this drive. Checked
       before the turnover-shaped rule below because `touchdown` and
       `_outcome_turnover` are never both true for the same row (an
       offensive touchdown is, by construction, not a turnover).
    3. A safety (`safety == 1`): `yards_gained = -yardline_50` (tackled at the
       offense's own goal line, spot 0). Checked before the turnover-shaped
       rule below because a safety's own `outcome.turnover == True` (see
       docs/ifaf-field-mapping.md's outcome-vocabulary section) would
       otherwise be caught by the broader `_outcome_turnover` clause and
       wrongly nulled.
    4. A turnover-shaped play (`interception`, `def_touchdown`,
       `defensive_two_point_conv`, `result_raw == "TURNOVER"`, or
       `_outcome_turnover`) stays null — the next row's `yardline_50` belongs
       to the new possession, not a gain by this play's own offense.
    5. Otherwise, if the next row shares this row's `drive_id`: `yards_gained
       = next.yardline_50 - yardline_50` (same team keeps possession; ballOn
       already increases toward the opponent goal within one team's drive).
    6. Otherwise (last play of a drive with no following same-drive row —
       end of half/game, or a possession change with none of the flags
       above, e.g. a plain `TURNOVER`/extra-point attempt): null.

    A null `yardline_50` on this row or the next (a missing-`ballOn` context,
    `_missing_ballon`) propagates to a null `yards_gained` automatically —
    Polars arithmetic on a null operand yields null, no special-casing
    needed.
    """
    turnover_like = (
        (pl.col("interception") == 1)
        | (pl.col("def_touchdown") == 1)
        | (pl.col("defensive_two_point_conv") == 1)
        | (pl.col("result_raw") == "TURNOVER")
        | pl.col("_outcome_turnover").fill_null(False)
    )

    next_drive = pl.col("drive_id").shift(-1)
    next_yardline = pl.col("yardline_50").shift(-1)
    same_drive_next = next_drive == pl.col("drive_id")

    gain = (
        pl.when(pl.col("_penalty"))
        .then(None)
        .when(pl.col("touchdown") == 1)
        .then(50 - pl.col("yardline_50"))
        .when(pl.col("safety") == 1)
        .then(-pl.col("yardline_50"))
        .when(turnover_like)
        .then(None)
        .when(same_drive_next)
        .then(next_yardline - pl.col("yardline_50"))
        .otherwise(None)
        .cast(pl.Int32)
    )

    return df.with_columns(gain.alias("yards_gained"))


# IFAF 5v5 flag rules (docs/ifaf-field-mapping.md yards_to_go-derivation
# addendum, 2026-09-06): the offense gets four downs to advance the ball past
# midfield, then a fresh four downs to score. The "line to gain" is therefore
# not a fixed +10 like American football -- it is always one of two field
# landmarks: midfield (not yet crossed this possession) or the opponent's
# goal line (already crossed). Given the already-verified `yardline_50`
# convention (own-goal-line origin, 0..50, midfield == 25 -- see the
# ballOn-semantics section), yards_to_go is fully determined by field
# position, needing no down-count arithmetic at all.
MIDFIELD_YARDLINE = 25
GOAL_YARDLINE = 50


def derive_yards_to_go(df: pl.DataFrame) -> pl.DataFrame:
    """Derive `yards_to_go` from field position alone, recomputed fresh per
    play. Must run after `flatten_unified_plays` on a single game's frame,
    already sorted by `play_id` -- same per-game, already-sorted contract
    every other per-game derivation in this module relies on.

    Rule:

    1. A down-0 (PAT/TRY) row: `yards_to_go = GOAL_YARDLINE - yardline_50` --
       every PAT attempt is inherently in the goal-to-go phase (a team only
       reaches a PAT by having already scored a touchdown, deep in opponent
       territory).
    2. Otherwise: `yards_to_go = GOAL_YARDLINE - yardline_50` when this row's
       own `yardline_50 >= MIDFIELD_YARDLINE` (already past midfield --
       goal-to-go), else `yards_to_go = MIDFIELD_YARDLINE - yardline_50`
       (still trying to reach midfield).

    A null `yardline_50` (a missing-`ballOn` context, `_missing_ballon`)
    propagates to a null `yards_to_go` automatically, same as
    `derive_yardage_columns`.

    **Not sticky across a drive -- an earlier draft of this rule persisted a
    "crossed midfield at some point in this drive" flag via
    `cum_max().over("drive_id")` (mirroring American-football down
    persistence: once a first down is earned, a subsequent loss doesn't
    revert the line to gain). That version was empirically WORSE, not
    better: cross-checked against the `events` feed's own
    `DISTANCE_CHANGE.payload.marker` (which carries exactly two real values,
    `MIDDLE`/`GOAL` -- the payload's `yardsToGo` number itself is the
    already-documented hardcoded `10` and carries no information), the
    sticky version agreed on only 74.3% of 3,527 comparable (game, ballOn)
    pairs, while this simple, non-sticky, per-play recompute agrees on
    98.2% -- adding the `MIDDLE_LINE` outcome/sequence marker as an
    additional OR-signal (`_outcome_middle_line`/`_sequence_middle_line`,
    computed in `flatten_unified_plays` but deliberately unused here) made
    it slightly worse still (97.5%). This is a live-data finding, not an
    assumption: IFAF's own engine does not appear to persist a "crossed
    midfield" achievement the way an American-football first down would --
    the MIDDLE/GOAL phase is just a function of the current spot. See
    docs/ifaf-field-mapping.md's Nachtrag for the full comparison and the
    residual ~2% disagreement (concentrated on a couple of specific ballOn
    values, most likely asynchronous referee-console state updates across
    the separate DOWN_UPDATE/LOS_UPDATE/DISTANCE_CHANGE event types, not a
    semantic gap in this rule).
    """
    yardline = pl.col("yardline_50")
    yards_to_go = (
        pl.when(pl.col("down") == 0)
        .then(GOAL_YARDLINE - yardline)
        .when(yardline >= MIDFIELD_YARDLINE)
        .then(GOAL_YARDLINE - yardline)
        .otherwise(MIDFIELD_YARDLINE - yardline)
        .cast(pl.Int32)
    )

    return df.with_columns(yards_to_go.alias("yards_to_go"))


def _read_json_or_empty(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _load_games_meta(raw_dir: Path) -> dict[str, dict]:
    games_path = raw_dir / "games.json"
    if not games_path.exists():
        return {}
    payload = _read_json_or_empty(games_path)
    games_list = _extract_plays_list(payload) if not isinstance(payload, list) else payload
    if games_list is None:
        return {}
    meta: dict[str, dict] = {}
    for entry in games_list:
        if not isinstance(entry, dict):
            continue
        gid = entry.get("id") or entry.get("gameId")
        if gid:
            meta[str(gid)] = entry
    return meta


def _load_tournaments_meta(raw_dir: Path) -> dict[str, dict]:
    meta: dict[str, dict] = {}
    for path in sorted(raw_dir.glob("tournament_*.json")):
        if path.name.endswith("_teams.json"):
            continue
        payload = _read_json_or_empty(path)
        if isinstance(payload, dict) and payload.get("id"):
            meta[str(payload["id"])] = payload
    return meta


def _build_game_meta(game_entry: dict, tournament_entry: dict) -> dict:
    """Build per-game metadata, keyed off both the `/games` entry and its
    resolved `/tournaments/{id}` document.

    `competition` is tournament-*and-division* specific, not just the bare
    tournament name -- a 2026-09-06 finding (docs/ifaf-field-mapping.md)
    that both `ffwc26-women` and `ffwc26-men` share the exact same
    `tournament.name` ("IFAF World Flag 2026"), so trusting the name alone
    silently merged 25 men's games into the women's competition label
    end-to-end (competition_tier lookup, reporting, everything keyed on
    `competition`). `divisions[0]` ("Women"/"Men") is the reliable
    disambiguator -- appended to the base name when present
    ("IFAF World Flag 2026 Women"); falls back to suffixing the raw
    `tournamentId` slug in parentheses if a future tournament document ever
    lacks `divisions` (defensive, not observed live).
    `tournament_id` is kept as its own field (not just folded into
    `competition`) so downstream code can key on the stable machine
    identifier rather than parsing the human-readable competition string.
    """
    home = (game_entry.get("homeTeam") or {}).get("id")
    away = (game_entry.get("awayTeam") or {}).get("id")
    tournament_id = game_entry.get("tournamentId") or tournament_entry.get("id")

    base_name = tournament_entry.get("name") or tournament_entry.get("id")

    season = None
    start_date = tournament_entry.get("startDate") or ""
    if len(start_date) >= 4 and start_date[:4].isdigit():
        season = int(start_date[:4])

    divisions = tournament_entry.get("divisions") or []
    division = divisions[0] if divisions and isinstance(divisions[0], str) else None
    gender = division.lower() if division else None

    if base_name and division:
        competition = f"{base_name} {division}"
    elif base_name and tournament_id:
        competition = f"{base_name} ({tournament_id})"
    else:
        competition = base_name

    return {
        "home_team": home,
        "away_team": away,
        "competition": competition,
        "season": season,
        "gender": gender,
        "tournament_id": tournament_id,
    }


def ingest_snapshots(
    raw_dir: Path,
    team_mapping: pl.DataFrame,
    game_ids: Sequence[str] | None = None,
    tournaments: Sequence[str] | None = None,
) -> list[tuple[str, pl.DataFrame, IngestNotices]]:
    """Parse every `unified-plays_{game_id}.json` snapshot under `raw_dir` into a
    canonical frame.

    `games.json` and `tournament_*.json` (if present in `raw_dir`) supply
    home/away team labels and competition/season/gender per game; their absence
    degrades gracefully to null metadata rather than raising. A snapshot whose
    payload is unparseable is recorded as a skipped, zero-row, still-canonical-
    shaped result with a notice — it never aborts the remaining games. A snapshot
    with a real but empty play array (a forfeit) is not a skip; it is a genuine
    zero-row game. Any failure in the per-game chain from `flatten_unified_plays`
    through `conform_to_canonical` -- not just an unparseable payload -- likewise
    skips exactly that game with a notice naming the exception class, and never
    the whole run (T-1.2-44 / T-1.2-45). An unmapped team label still raises
    `UnmappedTeamError` (T-1.2-15) rather than being folded into a notice, since
    it signals a reference-data gap that needs a human fix, not a per-game data
    anomaly.

    `tournaments` (2026-09-06 addendum, `docs/ifaf-field-mapping.md`), when
    given, restricts ingestion to games whose `games.json`-resolved
    `tournamentId` is in the set -- a game whose tournamentId cannot be
    resolved at all (missing `games.json` entry, or the entry lacks the key)
    is silently excluded too when this filter is active, the same
    conservative default as an unrecognized tournament (this corpus is meant
    to be "safe by default": a tournament not explicitly opted into never
    reaches the canonical frame, full stop, rather than merely being
    excluded downstream). `None` (the default) ingests every snapshot
    regardless of tournament, preserving the original no-filter contract for
    any caller that doesn't pass it (existing tests, `game_ids`-scoped
    single-game calls).
    """
    raw_dir = Path(raw_dir)
    games_meta = _load_games_meta(raw_dir)
    tournaments_meta = _load_tournaments_meta(raw_dir)

    unified_paths = sorted(raw_dir.glob("unified-plays_*.json"))
    wanted = set(game_ids) if game_ids is not None else None
    wanted_tournaments = set(tournaments) if tournaments is not None else None

    results: list[tuple[str, pl.DataFrame, IngestNotices]] = []

    for path in unified_paths:
        gid = path.stem.removeprefix("unified-plays_")
        if wanted is not None and gid not in wanted:
            continue
        if wanted_tournaments is not None:
            game_tournament_id = games_meta.get(gid, {}).get("tournamentId")
            if game_tournament_id not in wanted_tournaments:
                continue

        notices = IngestNotices(game_id=gid)

        try:
            payload, _ = load_snapshot(path)
        except UnparseablePayload as exc:
            payload = []
            notices.skipped = True
            notices.skip_reason = str(exc)
            notices.messages.append(str(exc))

        # Everything from here through `conform_to_canonical` runs per game, inside
        # one try/except: any TypeError/AttributeError/ValueError/KeyError/
        # PolarsError raised anywhere in this chain (e.g. a malformed `playNumber`
        # reaching `flatten_unified_plays`'s `sorted(...)` call, or a schema/cast
        # error surfaced despite `conform_to_canonical`'s own non-strict casts) is
        # caught, recorded as a notice naming the exception class, and skips only
        # this game -- the same containment `sportapp.ingest_snapshots` applies
        # (T-1.2-44 / T-1.2-45). UnmappedTeamError (raised by map_teams below) is
        # deliberately NOT caught here: it is a plain Exception, not one of the
        # types in this tuple, so the catch already excludes it -- an unmapped team
        # must keep aborting loudly per CONTEXT.md's team-identity decision
        # (T-1.2-15); see test_ingest_snapshots_unmapped_team_raises.
        try:
            game_entry = games_meta.get(gid, {})
            tournament_entry = tournaments_meta.get(game_entry.get("tournamentId"), {})
            game_meta = _build_game_meta(game_entry, tournament_entry)

            df = flatten_unified_plays(payload, game_meta, gid)

            notices.missing_context_keys = {
                "down": int(df["_missing_down"].sum()) if df.height else 0,
                "ballOn": int(df["_missing_ballon"].sum()) if df.height else 0,
                "possessionTeamId": int(df["_missing_possession"].sum()) if df.height else 0,
            }
            notices.missing_context_keys = {
                k: v for k, v in notices.missing_context_keys.items() if v
            }

            df = map_teams(df, team_mapping, "ifaf", ["posteam", "defteam", "home_team", "away_team"])
            df = derive_outcome_columns(df)
            df = derive_yardage_columns(df)
            df = derive_yards_to_go(df)

            if df.height:
                unmapped = (
                    df.filter(pl.col("_unmapped_outcome") == 1)
                    .group_by("result_raw")
                    .agg(pl.len().alias("count"))
                )
                if unmapped.height:
                    notices.unmapped_outcomes = dict(
                        zip(unmapped["result_raw"].to_list(), unmapped["count"].to_list())
                    )
                    notices.messages.append(
                        f"{unmapped.height} unmapped outcome value(s): {notices.unmapped_outcomes}"
                    )

            df = add_scoring_play_team(df, credit_defense=True)
            df = add_score_columns(df)

            if df.height:
                # context.score is the score entering this play (pre-play), while
                # home_team_score/away_team_score already include this row's own
                # scoring event (add_score_columns credits the scoring row itself).
                # Compare context.score against the *previous* row's reconstructed
                # score within the same game, not the current row's, or every play
                # following any score would show a spurious one-play-lag mismatch.
                expected = df.with_columns(
                    [
                        pl.col("home_team_score")
                        .shift(1)
                        .over("game_id")
                        .fill_null(0)
                        .alias("_expected_home_score"),
                        pl.col("away_team_score")
                        .shift(1)
                        .over("game_id")
                        .fill_null(0)
                        .alias("_expected_away_score"),
                    ]
                )
                mismatches = expected.filter(
                    (
                        pl.col("_context_score_home").is_not_null()
                        & (pl.col("_context_score_home") != pl.col("_expected_home_score"))
                    )
                    | (
                        pl.col("_context_score_away").is_not_null()
                        & (pl.col("_context_score_away") != pl.col("_expected_away_score"))
                    )
                )
                notices.score_mismatches = mismatches.height
                if mismatches.height:
                    notices.messages.append(
                        f"{mismatches.height} play(s) where the reconstructed score "
                        "diverges from context.score"
                    )

            df, conform_report = conform_to_canonical(df, "ifaf")
            if conform_report.cast_failures:
                notices.messages.append(f"cast failures: {conform_report.cast_failures}")
        except (TypeError, AttributeError, ValueError, KeyError, pl.exceptions.PolarsError) as exc:
            notices.skipped = True
            notices.skip_reason = f"{type(exc).__name__}: {exc}"
            notices.messages.append(f"game {gid}: {type(exc).__name__}: {exc}")
            results.append((gid, _empty_canonical_frame(), notices))
            continue

        results.append((gid, df, notices))

    return results
