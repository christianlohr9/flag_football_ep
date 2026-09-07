"""cpx.studio (IFAF WM-2026) snapshot parser: `/plays` JSON -> canonical plays.

Reads the raw snapshots `fetch/ifaf.py` already wrote to disk (`data/raw/ifaf/`);
no network access happens here. Implements `docs/ifaf-field-mapping.md` exactly —
see that document (Nachtrag 2026-09-07) for the per-field evidence this parser is
built against.

**Primary source, as of 2026-09-07: `/games/{id}/plays` (`plays_{id}.json`), not
`unified-plays`.** The unified-plays `context` block (down/ballOn/yardsToGo) was
found to not be a reliable pre-snap state — it alternates between pre- and
post-play spots and a large share of rows sit on the endpoint's own literal
default state. `/plays` is the reviewer-facing, per-play-reviewed feed: real
pre-snap `down`/`ballOn`/`half`/`offenseTeamId`, a `nullified` flag, an
`officialScore` verdict (`"TD"`/`"XP1"`/`"XP2"`/`"NONE"`/absent -- the sole
source of scoring flags, 2026-09-07 second same-day fix; never the
`TOUCHDOWN`/`TRY` action names, which are frequently misleading for point
value), and an `events[]` action list this module derives every other
outcome flag and `play_type` from directly (`flatten_plays_records`).
`flatten_unified_plays`/
`derive_outcome_columns` (the pre-2026-09-07 primary path) are kept unchanged
and now serve only as the fallback for a game with no usable `/plays` snapshot
(`ingest_snapshots` picks per game, stamping `source_detail` on the fallback
rows only) — every downstream derivation
(`derive_yardage_columns`/`derive_yards_to_go`) still applies to that path
exactly as before.

Convergence with the other ingest sources (hudl, legacy, sportapp) happens only at
`canonical.conform_to_canonical` — this module never reuses the Hudl `RESULT`
token parser or the sportapp free-text summary parser. `OUTCOME_MAP` is the
unified-plays fallback path's own, from-scratch vocabulary, driven entirely by
the `outcome.type` values documented in the mapping doc's outcome-vocabulary
section; the `/plays` primary path has its own action-list vocabulary
(`_PLAYS_PASS_ACTIONS`/`_PLAYS_RUN_ACTIONS`/`_TRY_ACTION`), from-scratch too.
"""

from __future__ import annotations

import json
from collections import Counter
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


# Schema for `load_ifaf_final_scores`'s returned frame -- matches
# `reference._FINAL_SCORES_SCHEMA` column-for-column (that constant is
# private to `reference.py`, so this module keeps its own equal copy rather
# than importing a private name; `pipeline.run_ingest` concatenates the two
# frames, which requires the schemas to line up exactly).
_IFAF_FINAL_SCORES_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "home_score": pl.Int32,
    "away_score": pl.Int32,
    "note": pl.Utf8,
}


def _empty_final_scores_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=dict(_IFAF_FINAL_SCORES_SCHEMA))


def load_ifaf_final_scores(
    raw_dir: Path, team_mapping: pl.DataFrame
) -> tuple[pl.DataFrame, list[str]]:
    """Build a final-score reference frame from `games.json`'s own
    `currentScore.home`/`currentScore.away`, for `validation.checks.
    score_reconstruction` to check every IFAF game's reconstructed score
    against the official result -- `data/reference/final_scores.csv` carries
    zero `ifaf-*` rows (2026-09-07 finding), so without this every IFAF game
    was silently SKIPPED by that check rather than actually validated.

    Only `status == "FINAL"` entries are used (every one of the 96 games in
    the live `games.json` snapshot is `"FINAL"`, but a future snapshot could
    carry an in-progress game whose `currentScore` is not yet the real
    result). Team ids are resolved through `team_mapping` (source="ifaf"),
    the same mapping `map_teams` uses for `posteam`/`defteam`/`home_team`/
    `away_team` on the play-level frame -- but unlike `map_teams`, an
    unmapped team id here is *skipped* (with a notice naming it), not raised:
    a missing mapping for a scores-only reference should never abort the
    whole ingest run the way `map_teams`'s hard-fail contract is meant to for
    the play-level path, where an unmapped team would otherwise pass through
    silently into the canonical corpus.
    """
    notices: list[str] = []
    games_meta = _load_games_meta(raw_dir)
    if not games_meta:
        return _empty_final_scores_frame(), notices

    ifaf_map = team_mapping.filter(pl.col("source") == "ifaf")
    lookup = dict(zip(ifaf_map["source_team"].to_list(), ifaf_map["canonical_team"].to_list()))

    rows: list[dict] = []
    unmapped: set[str] = set()
    for gid, entry in games_meta.items():
        if entry.get("status") != "FINAL":
            continue
        score = entry.get("currentScore") or {}
        home_score, away_score = score.get("home"), score.get("away")
        if not isinstance(home_score, int) or isinstance(home_score, bool):
            continue
        if not isinstance(away_score, int) or isinstance(away_score, bool):
            continue
        home_raw = (entry.get("homeTeam") or {}).get("id")
        away_raw = (entry.get("awayTeam") or {}).get("id")
        if not home_raw or not away_raw:
            continue
        home_code = lookup.get(home_raw)
        away_code = lookup.get(away_raw)
        if home_code is None or away_code is None:
            unmapped.update(x for x in (home_raw, away_raw) if lookup.get(x) is None)
            continue
        rows.append(
            {
                "game_id": make_game_id("ifaf", gid),
                "home_team": home_code,
                "away_team": away_code,
                "home_score": home_score,
                "away_score": away_score,
                "note": "games.json currentScore",
            }
        )

    if unmapped:
        notices.append(
            "ifaf: final-score reference skipped for unmapped team id(s) "
            f"{sorted(unmapped)}"
        )

    if not rows:
        return _empty_final_scores_frame(), notices

    return pl.DataFrame(rows, schema=_IFAF_FINAL_SCORES_SCHEMA), notices


def _load_tournaments_meta(raw_dir: Path) -> dict[str, dict]:
    meta: dict[str, dict] = {}
    for path in sorted(raw_dir.glob("tournament_*.json")):
        if path.name.endswith("_teams.json"):
            continue
        payload = _read_json_or_empty(path)
        if isinstance(payload, dict) and payload.get("id"):
            meta[str(payload["id"])] = payload
    return meta


def _load_events_list(raw_dir: Path, game_id: str) -> list | None:
    """Read one `events_{id}.json` snapshot's top-level event array, or
    `None` when the file is absent, unparseable, or not the expected shape
    (a top-level list, or an object wrapping the array under `events`) --
    shared by `_events_score_ledger_summary` (diagnostic report) and
    `apply_events_ledger` (the authorized synthetic-row fill)."""
    events_path = raw_dir / f"events_{game_id}.json"
    if not events_path.exists():
        return None
    payload = _read_json_or_empty(events_path)
    events = payload if isinstance(payload, list) else (
        payload.get("events") if isinstance(payload, dict) else None
    )
    return events if isinstance(events, list) else None


def _events_score_ledger_summary(raw_dir: Path, game_id: str, game_entry: dict) -> str | None:
    """Diagnostic report line for a `score_reconstruction` mismatch: sum
    the game's own `events_{id}.json` `SCORE`-type events (non-reverted
    only) per team and compare against `games.json`'s own `currentScore`.

    2026-09-07 (fifth follow-up, same day): the events feed carries an
    explicit scoring ledger (`eventType == "SCORE"`, `payload.teamId`/
    `scoreType`/`points` -- `scoreType` vocabulary observed corpus-wide:
    `TD`/`XP1`/`XP2` only, no distinct `SAFETY` type; a safety is logged as
    a `SCORE` event with `scoreType: "XP2"`, `points: 2`, same encoding
    `officialScore` already uses for it). Summed per team, non-reverted
    events reproduce `games.json`'s final score exactly for 41 of the 42
    women's games that have any `SCORE` events at all (verified
    2026-09-07; a further 6 games are zero-event forfeits, structurally
    outside this check's scope, not a ledger failure) -- 97.6% on the
    denominator that actually applies, clearing this project's own 95%
    per-game exact-match bar. Following that finding, the user (project
    owner, domain expert) explicitly authorized promoting the ledger from
    a diagnostic-only signal to the authoritative scoring source for
    ledger-consistent games, including inserting a synthetic row for a
    ledger-confirmed conversion `/plays` never recorded -- see
    `apply_events_ledger` (sixth follow-up) for that implementation. This
    function stays a pure diagnostic report line (used for every game,
    including the one -- `ffwc26-wd4` -- whose ledger itself disagrees
    with `games.json`, which `apply_events_ledger` never touches).

    Returns `None` when there is nothing to report (no events snapshot, no
    parseable `SCORE` events, or no official score to compare against --
    e.g. a forfeit). Otherwise returns one human-readable line: either the
    ledger CONFIRMS the official score (meaning a `score_reconstruction`
    mismatch on this game means `/plays` is missing or misattributing a
    scoring record, not the reference itself), or the ledger itself
    DISAGREES with the official score (meaning the ledger cannot be
    trusted to diagnose or fill this specific game's mismatch,
    `ffwc26-wd4`-style).
    """
    events = _load_events_list(raw_dir, game_id)
    if events is None:
        return None

    score_events = [
        e
        for e in events
        if isinstance(e, dict) and e.get("eventType") == "SCORE" and not e.get("reverted")
    ]
    if not score_events:
        return None

    home_raw = (game_entry.get("homeTeam") or {}).get("id")
    away_raw = (game_entry.get("awayTeam") or {}).get("id")
    official = game_entry.get("currentScore") or {}
    official_home, official_away = official.get("home"), official.get("away")
    if (
        home_raw is None
        or away_raw is None
        or not isinstance(official_home, int)
        or isinstance(official_home, bool)
        or not isinstance(official_away, int)
        or isinstance(official_away, bool)
    ):
        return None

    totals: dict[str, int] = {}
    for e in score_events:
        event_payload = e.get("payload") or {}
        team = event_payload.get("teamId")
        points = event_payload.get("points")
        if team is None or not isinstance(points, int) or isinstance(points, bool):
            continue
        totals[team] = totals.get(team, 0) + points

    ledger_home = totals.get(home_raw, 0)
    ledger_away = totals.get(away_raw, 0)

    if ledger_home == official_home and ledger_away == official_away:
        return (
            f"events-ledger SCORE-event totals ({ledger_home}-{ledger_away}) confirm the "
            "official games.json score -- a score_reconstruction mismatch on this game means "
            "/plays is missing or misattributing a scoring record, not the reference itself"
        )
    return (
        f"events-ledger SCORE-event totals ({ledger_home}-{ledger_away}) do NOT match the "
        f"official games.json score ({official_home}-{official_away}) -- the ledger itself is "
        "unreliable for this game; do not use it to diagnose a score_reconstruction mismatch here"
    )


_LEDGER_SCORE_TYPES = frozenset({"TD", "XP1", "XP2"})


def apply_events_ledger(
    df: pl.DataFrame,
    events: list,
    home_raw: str | None,
    away_raw: str | None,
    official_home: int | None,
    official_away: int | None,
) -> tuple[pl.DataFrame, list[str]]:
    """User-authorized design decision (2026-09-07, sixth follow-up,
    `docs/ifaf-field-mapping.md`): for a game whose events feed's own
    `SCORE` ledger (summed per team, non-reverted events only) reproduces
    `games.json`'s official final score exactly, the ledger becomes the
    SOLE source of `touchdown`/`def_touchdown`/`one_point_conv_success`/
    `two_point_conv_success` for that game -- overriding `officialScore`
    entirely (kept only as the `official_score` audit extra). A ledger
    score with no matching `/plays` record (a conversion the ledger
    confirms happened but the reviewer feed never logged at all -- a
    different problem than an *existing* record's `officialScore` being
    wrong) is inserted as a synthetic row, `score_source =
    "events-ledger-synthetic"`; a matched real row is stamped
    `score_source = "events-ledger"`.

    A game whose ledger does NOT reproduce the official score (or has no
    events at all -- forfeits, or a `/plays`-fallback game with no
    `events_{id}.json`) is returned **unchanged**: this function is a
    strict no-op for it, every row keeps whatever `officialScore`-driven
    scoring `flatten_plays_records` already computed, and `ffwc26-wd4`
    (the one live game whose ledger itself disagrees with `games.json`) is
    never patched by hand.

    Alignment walks the ledger's own `SCORE` events in `sequenceNumber`
    order, maintaining a single `used` mask over `/plays` rows (never
    matching the same row to two ledger events) and one "current TD
    anchor" row index per team: a `TD` event finds the next unused,
    non-nullified touchdown-shaped row (`TOUCHDOWN` action or
    `officialScore == "TD"`) whose credited team (offense, or defense when
    `INTERCEPTION` is also on the row -- a pick-six) matches the ledger
    event's `teamId`; an `XP1`/`XP2` event first looks for the next
    unused, non-nullified TRY-actioned row for that team *after* its own
    most recent TD anchor (consuming the anchor either way, so a second XP
    for the same team without an intervening new TD never reuses a stale
    one); an `XP2` event that finds no TRY candidate instead checks for an
    unused SAFETY-actioned row where that team is the defense (a safety,
    not a conversion -- the app's own confirmed `officialScore` encoding
    for a safety is the same `XP2` value) -- if found, `score_source` is
    stamped but nothing else changes, since `safety`/`add_scoring_play_team`
    already credit the defense unconditionally from the `SAFETY` action,
    independent of `officialScore`/the ledger. Only when neither a TRY nor
    a SAFETY candidate exists is a synthetic row inserted.

    **2026-09-07, eighth follow-up -- overrides the seventh follow-up below.**
    A `TD` event with no candidate at all now ALSO gets a synthetic row,
    exactly like a missing conversion: `touchdown = 1`, `play_type = null`
    (unlike a conversion row, there is no `"extra_point"` action to claim),
    `posteam` the scoring team, `defteam` the other team, `down`/
    `yardline_50`/`yards_to_go`/`yards_gained` all `null`, `half` copied from
    the nearest preceding real `/plays` row (see the placement note below),
    `result_raw = "SYNTHETIC TD (events ledger)"`, `score_source =
    "events-ledger-synthetic"`. The ledger's own immediate follow-up
    `XP1`/`XP2` for that team (if the ledger has one, and it too has no
    `/plays` candidate -- the QF-style common case) becomes a second
    synthetic row right after it, via the exact same mechanism the sixth
    follow-up already established for a missing conversion with a REAL TD
    anchor. Every affected game also gets `plays_incomplete = 1` on every
    one of its rows (`canonical.NULLABLE_EXTRAS`, real and synthetic alike)
    and a per-game aggregate notice naming the synthetic TD/conversion
    counts.

    This is a direct, explicit, user-level reversal of the seventh
    follow-up's decline, immediately below -- **read that section's
    reasoning first; it is not repeated or re-litigated here.** The project
    owner (a flag-football domain expert) was shown that exact trade-off
    again on 2026-09-07 and decided the other way: a correct running score
    for every later real play's `score_differential`/WP features (point 3
    of the seventh follow-up's own objection) is judged more valuable than
    leaving these 9 games' `score_reconstruction` failing outright over 9
    single-drive gaps, given the row is flagged on every axis available
    (`score_source`, `plays_incomplete`, and structural exclusion from
    EP/WP training -- see `docs/ifaf-field-mapping.md`'s Nachtrag for the
    full discussion) rather than silently blended in. `score_reconstruction`
    passing for one of these games is therefore no longer proof every real
    play in it was independently reviewed end to end -- it is proof the
    real reviewed plays plus a small number of ledger-only, clearly-marked
    score adjustments reproduce the official total. `ffwc26-wd4` (ledger
    disagrees with `games.json`) and `01a00140-b679-...` (this game's own
    ledger is internally malformed -- duplicate `XP2` events with no
    touchdown anywhere near them -- so even its orphaned conversions have no
    TD to anchor a synthetic row to) are each untouched by this change for
    reasons that predate it and still apply unchanged.

    **Placement of a synthetic touchdown row is honestly approximate.**
    There is no shared key between the events feed's own `sequenceNumber`
    and `/plays`' own row order: the events feed's `DOWN_UPDATE`/
    `LOS_UPDATE`/`POSSESSION_CHANGE` stream carries no `playId` at all
    (verified against the live corpus), and a prior attempt at cross-feed
    placement via `clientTimestamp`/`startedAt` proximity was independently
    measured and rejected elsewhere in this module as unreliable (a
    ~51-hour epoch offset was found on one game -- see the module's own
    Nachtrag). The exact spot is therefore always ambiguous by this
    project's own already-established bar, so the rule falls back to
    inserting at the END of that team's own possession segment, computed as
    "immediately before the next `/plays` row this same alignment walk
    matched to ANY later ledger event, in `sequenceNumber` order" -- i.e.,
    as late as possible while never sorting after a `/plays` row a
    known-real, later ledger event already precedes. When no later match
    exists at all for either team (the common case in the live corpus --
    every one of the 9 affected games' missing touchdowns is that team's
    own last recorded score), this collapses to the very end of the
    `/plays` feed. `half` is copied from that same preceding row, honestly
    satisfying "nearest preceding record in feed order" precisely because
    that is exactly what the resolved anchor row is.

    Every synthetic row (conversion or touchdown): `posteam` the scoring
    team, `half`/`drive_id`/game-level metadata copied from the anchor row,
    `yardline_50 = null` (no real spot to report), `nullified = null` (not
    `0` -- nullification is not applicable to a row that was never a real
    reviewed play), `source_play_sequence = null`, `result_raw` a
    clearly-labelled synthetic marker (never mistaken for a real action
    list). A conversion row keeps the sixth follow-up's original shape
    (`play_type = "extra_point"`, `down = 0`); a touchdown row uses
    `play_type = null`, `down = null` (spec'd as such -- there is no real
    play at all behind it, not even a resolved-by-convention down number).
    `play_id` is renumbered gapless 1..N across the whole game after every
    insertion.

    ---

    ## Nachtrag 2026-09-07 (seventh follow-up, same day) -- declined,
    then overridden the same day (eighth follow-up, above)

    A further request came in with the same framing as the sixth
    follow-up's authorization (an explicit user decision, trade-offs
    shown): for the games where the ledger names a touchdown with no
    matching `/plays` record at all, insert a synthetic touchdown row too.

    **This fix declined the request** at the time and kept the sixth
    follow-up's boundary exactly where it was drawn: `apply_events_ledger`
    logged a TD with no candidate and left it unscored, never fabricating a
    row for it. The reasoning, preserved verbatim for the record (now
    superseded by the user's own eighth-follow-up decision above, not by
    this fix reversing its own judgment):

    1. **A missing conversion and a missing touchdown are not the same
       class of gap.** Every conversion the sixth follow-up fills has a
       real, reviewed `/plays` record right next to it -- the touchdown
       itself is charted, actioned, spotted; only its one-line PAT
       follow-up is missing. A "synthetic touchdown" has none of that: no
       down, no field position, no charted action, not even a confirmed row
       position -- only an approximate placement, honestly disclosed above.
    2. **It makes `score_reconstruction` pass by construction for exactly
       the games where it matters most**, rather than by every play being
       independently verified. This is now an accepted, disclosed trade-off
       (see the `score_source`/`plays_incomplete` flags above), not an
       unstated one.
    3. **"Right" running-score continuity for later real plays means
       "consistent with a ledger-confirmed event," not "consistent with
       what was independently reviewed and charted."** The user was shown
       this exact framing directly and chose it anyway, given the row is
       clearly marked, not silently blended in.

    The per-game aggregate notice this decline originally shipped instead
    (naming the missing-points total without fabricating anything) is
    superseded by the new synthetic-row-count notice described above for
    every game that gets a synthetic touchdown; it is unchanged for a game
    whose only gap is an orphaned conversion with no TD anchor at all (a
    different failure shape this fix still does not fabricate a row for).
    """
    notices: list[str] = []
    if df.height == 0:
        return df, notices

    score_events = sorted(
        (
            e
            for e in events
            if isinstance(e, dict) and e.get("eventType") == "SCORE" and not e.get("reverted")
        ),
        key=lambda e: (
            e.get("sequenceNumber")
            if isinstance(e.get("sequenceNumber"), (int, float))
            and not isinstance(e.get("sequenceNumber"), bool)
            else float("inf")
        ),
    )
    if not score_events:
        return df, notices

    totals: dict[str, int] = {}
    for e in score_events:
        payload = e.get("payload") or {}
        team = payload.get("teamId")
        points = payload.get("points")
        score_type = payload.get("scoreType")
        if (
            team is None
            or score_type not in _LEDGER_SCORE_TYPES
            or not isinstance(points, int)
            or isinstance(points, bool)
        ):
            continue
        totals[team] = totals.get(team, 0) + points

    if (
        home_raw is None
        or away_raw is None
        or not isinstance(official_home, int)
        or isinstance(official_home, bool)
        or not isinstance(official_away, int)
        or isinstance(official_away, bool)
        or totals.get(home_raw, 0) != official_home
        or totals.get(away_raw, 0) != official_away
    ):
        notices.append(
            "events-ledger not used for scoring (does not confirm the official games.json "
            "score, or no official score to check against) -- officialScore-driven scoring "
            "left unchanged"
        )
        return df, notices

    rows = df.to_dicts()
    n = len(rows)

    def actions_of(row: dict) -> set[str]:
        raw = row.get("result_raw")
        return set(raw.split(", ")) if raw else set()

    # Reset every ledger-affected flag; `safety` is deliberately untouched
    # (already unconditional on the SAFETY action, independent of
    # officialScore/the ledger).
    for row in rows:
        row["touchdown"] = 0
        row["def_touchdown"] = 0
        row["one_point_conv_success"] = 0
        row["two_point_conv_success"] = 0

    used = [False] * n

    def find_td(team: str, start: int) -> int | None:
        for i in range(start, n):
            if used[i] or rows[i]["nullified"]:
                continue
            official = rows[i].get("official_score")
            acts = actions_of(rows[i])
            if official in ("XP1", "XP2") or "TRY" in acts:
                # A PAT catch the reviewer feed charted with a TOUCHDOWN
                # action instead of TRY (docs/ifaf-field-mapping.md Nachtrag
                # 2026-09-07, fourth follow-up) is try-shaped despite the
                # action, not a touchdown candidate. The converse also
                # happens (the same Nachtrag's 21-record quirk): a
                # TRY-actioned record whose own `officialScore` reads "TD"
                # is never really a 6-point touchdown either -- a TRY
                # action always means try-shaped, regardless of what
                # officialScore claims on that one record shape.
                continue
            is_td_shaped = "TOUCHDOWN" in acts or official == "TD"
            if not is_td_shaped:
                continue
            scoring_team = rows[i]["defteam"] if "INTERCEPTION" in acts else rows[i]["posteam"]
            if scoring_team == team:
                return i
        return None

    def find_try(team: str, start: int) -> int | None:
        # Bounded to strictly before this team's own NEXT touchdown-shaped
        # row (if any) -- without this bound, a genuinely missing PAT (the
        # QF's own sequence 110: no try record at all before the team's
        # *next* touchdown) would incorrectly steal that next touchdown's
        # own, real, later PAT record instead of correctly falling through
        # to a synthetic insertion. `find_td` is read-only (never mutates
        # `used`), so calling it again here just to find the boundary is
        # safe and free of side effects.
        boundary = find_td(team, start)
        end = boundary if boundary is not None else n
        for i in range(start, end):
            if used[i] or rows[i]["nullified"]:
                continue
            acts = actions_of(rows[i])
            official = rows[i].get("official_score")
            # Same TOUCHDOWN-actioned-PAT quirk as `find_td` above, from the
            # other side: officialScore XP1/XP2 makes a row try-shaped even
            # without a literal TRY action.
            is_try_shaped = "TRY" in acts or official in ("XP1", "XP2")
            if is_try_shaped and rows[i]["posteam"] == team:
                return i
        return None

    def find_safety(team: str, start: int) -> int | None:
        for i in range(start, n):
            if used[i] or rows[i]["nullified"]:
                continue
            if "SAFETY" in actions_of(rows[i]) and rows[i]["defteam"] == team:
                return i
        return None

    team_floor: dict[str, int] = {}
    # Per-team open anchor: an `int` is a real, matched `/plays` row index
    # (the sixth follow-up's original conversion-fill anchor); a `dict` is a
    # pending synthetic-touchdown group (eighth follow-up, this team's own
    # touchdown had no `/plays` candidate at all, so there is no real row to
    # anchor a follow-up conversion to yet -- resolved to a real insertion
    # point only after the whole walk finishes, see below); `None` means no
    # open anchor for this team right now.
    last_td_idx: dict[str, int | dict | None] = {}
    # (anchor_row_index, team, score_type) for a ledger conversion with a
    # REAL matched touchdown anchor but no /plays record of its own --
    # inserted only after the whole ledger walk finishes, so this loop's own
    # row indices never shift underneath it.
    pending_synthetic: list[tuple[int, str, str]] = []
    # Missing-touchdown groups (2026-09-07, eighth follow-up): each entry
    # anchors one ledger TD event with no /plays candidate, plus its own
    # immediate ledger follow-up XP1/XP2 (if any, and it too has no /plays
    # candidate). `anchor_idx` is resolved once, after the whole walk
    # finishes -- see the placement-rule docstring above.
    pending_td_groups: list[dict] = []
    # (sequenceNumber, row_index) for every event this walk actually matched
    # to a real /plays row (TD, try, or safety) -- ascending sequenceNumber
    # order by construction, since score_events is walked in that order.
    # Used only to resolve `pending_td_groups`' placement below.
    matched_seq_idx: list[tuple[float, int]] = []
    orphaned_conversion_count = 0
    orphaned_conversion_points = 0

    for ev in score_events:
        payload = ev.get("payload") or {}
        team = payload.get("teamId")
        score_type = payload.get("scoreType")
        seq = ev.get("sequenceNumber")
        if team is None or score_type not in _LEDGER_SCORE_TYPES:
            continue

        if score_type == "TD":
            idx = find_td(team, team_floor.get(team, 0))
            if idx is None:
                # User-authorized (2026-09-07, eighth follow-up, overriding
                # the seventh follow-up's decline -- see the docstring
                # above): a whole touchdown the reviewer feed never charted
                # is now inserted as a synthetic row too, not just logged
                # and left unscored. `anchor_idx` can only be resolved once
                # the rest of this walk (and its real matches) is known, so
                # this just opens a pending group for now.
                group: dict = {"sequence": seq, "team": team, "score_types": ["TD"]}
                pending_td_groups.append(group)
                last_td_idx[team] = group
                continue
            used[idx] = True
            team_floor[team] = max(team_floor.get(team, 0), idx)
            matched_seq_idx.append((seq, idx))
            if "INTERCEPTION" in actions_of(rows[idx]):
                rows[idx]["def_touchdown"] = 1
            else:
                rows[idx]["touchdown"] = 1
            rows[idx]["score_source"] = "events-ledger"
            last_td_idx[team] = idx
            continue

        anchor_idx = last_td_idx.get(team)
        idx = find_try(team, anchor_idx + 1) if isinstance(anchor_idx, int) else None
        if idx is not None:
            used[idx] = True
            team_floor[team] = max(team_floor.get(team, 0), idx)
            matched_seq_idx.append((seq, idx))
            if score_type == "XP1":
                rows[idx]["one_point_conv_success"] = 1
            else:
                rows[idx]["two_point_conv_success"] = 1
            rows[idx]["score_source"] = "events-ledger"
            last_td_idx[team] = None
            continue

        if score_type == "XP2":
            safety_idx = find_safety(team, team_floor.get(team, 0))
            if safety_idx is not None:
                used[safety_idx] = True
                team_floor[team] = max(team_floor.get(team, 0), safety_idx)
                matched_seq_idx.append((seq, safety_idx))
                rows[safety_idx]["score_source"] = "events-ledger"
                last_td_idx[team] = None
                continue

        if isinstance(anchor_idx, dict):
            # This team's own immediately-preceding touchdown was itself a
            # missing-/plays-record synthetic insertion -- its conversion
            # joins the same group, placed right after the synthetic
            # touchdown once the group's anchor is resolved below.
            anchor_idx["score_types"].append(score_type)
            last_td_idx[team] = None
            continue

        if anchor_idx is None:
            notices.append(
                f"events-ledger: {score_type} for {team!r} (sequenceNumber "
                f"{ev.get('sequenceNumber')}) has no TD anchor and no /plays candidate -- "
                "left unscored, not fabricated"
            )
            orphaned_conversion_count += 1
            orphaned_conversion_points += 2 if score_type == "XP2" else 1
            last_td_idx[team] = None
            continue

        pending_synthetic.append((anchor_idx, team, score_type))
        last_td_idx[team] = None

    if orphaned_conversion_count:
        notices.append(
            f"events-ledger: {orphaned_conversion_count} conversion(s) "
            f"({orphaned_conversion_points} points) confirmed by the ledger have no TD "
            "anchor and no /plays record at all -- left unscored, not fabricated (a "
            "standalone conversion event with no preceding touchdown of its own -- distinct "
            "from a missing touchdown's own immediate follow-up conversion, which IS now "
            "fabricated as part of that touchdown's synthetic group, see below)"
        )

    if not pending_synthetic and not pending_td_groups:
        return pl.DataFrame(rows, schema=_PLAYS_WORKING_SCHEMA), notices

    # Resolve each pending group's real insertion point -- see the
    # docstring's placement-rule paragraph for the full reasoning. In short:
    # insert immediately before the next /plays row this same walk matched
    # to ANY later ledger event (by sequenceNumber), or at the very end of
    # the game's /plays rows when no later match exists at all.
    for group in pending_td_groups:
        next_real_idx = None
        for seq, ridx in matched_seq_idx:
            if seq > group["sequence"]:
                next_real_idx = ridx
                break
        group["anchor_idx"] = (next_real_idx - 1) if next_real_idx is not None else (n - 1)

    def _synthetic_row(anchor: dict, team: str, score_type: str) -> dict:
        is_td = score_type == "TD"
        return {
            "source": anchor["source"],
            "source_game_id": anchor["source_game_id"],
            "game_id": anchor["game_id"],
            "play_id": None,  # renumbered below, once every insertion is known
            "drive_id": anchor["drive_id"],
            "half": anchor["half"],
            "down": None if is_td else 0,
            "yards_to_go": None,
            "yardline": None,
            "yardline_50": None,
            "yardline_50_after": None,
            "yardline_50_simple": None,
            "yards_to_go_simple": None,
            "yards_gained": None,
            "first_down": None,
            "game_clock_ms": None,
            "half_seconds_remaining": None,
            "posteam": team,
            "posteam_after": None,
            "home_team": anchor["home_team"],
            "away_team": anchor["away_team"],
            "defteam": _other_team(team, anchor["home_team"], anchor["away_team"]),
            "play_type": None if is_td else "extra_point",
            "result_raw": (
                "SYNTHETIC TD (events ledger)"
                if is_td
                else f"SYNTHETIC (events-ledger {score_type})"
            ),
            "description": None,
            "competition": anchor["competition"],
            "season": anchor["season"],
            "gender": anchor["gender"],
            "tournament_id": anchor["tournament_id"],
            "complete_pass": 0,
            "sack": 0,
            "interception": 0,
            "safety": 0,
            "touchdown": 1 if is_td else 0,
            "def_touchdown": 0,
            "one_point_conv_success": 1 if score_type == "XP1" else 0,
            "two_point_conv_success": 1 if score_type == "XP2" else 0,
            "defensive_two_point_conv": 0,
            "penalty": 0,
            "qb": None,
            "thrown_by": None,
            "received_by": None,
            "target": None,
            "pass_side": None,
            "pass_depth": None,
            "incomplete_reason": None,
            "penalty_type": None,
            "source_detail": None,
            "source_play_sequence": None,
            "nullified": None,
            "official_score": None,
            "score_source": "events-ledger-synthetic",
            # `None` by default here -- only stamped 1 below, on every row
            # (real and synthetic) of a game that actually got a synthetic
            # *touchdown*. A pure missing-conversion synthetic row (the
            # original sixth follow-up shape, a REAL touchdown anchor) does
            # not, by itself, warrant the game-level completeness flag this
            # column exists for.
            "plays_incomplete": None,
            "_missing_down": 1 if is_td else 0,
            "_missing_ballon": 1,
            "_missing_offense": 0,
            "_nullified": 0,
            "_unknown_action": None,
        }

    by_anchor: dict[int, list[dict]] = {}
    n_synthetic_td = 0
    n_synthetic_conv = 0

    for anchor_idx, team, score_type in pending_synthetic:
        anchor = rows[anchor_idx]
        synthetic = _synthetic_row(anchor, team, score_type)
        by_anchor.setdefault(anchor_idx, []).append(synthetic)
        n_synthetic_conv += 1
        notices.append(
            f"events-ledger: {score_type} for {team!r} confirmed by the ledger but absent "
            f"from /plays -- inserted as a synthetic extra_point row after play_id "
            f"{anchor['play_id']}"
        )

    for group in pending_td_groups:
        anchor_idx = group["anchor_idx"]
        # `anchor_idx` can be -1 (this group's touchdown precedes every real
        # match this walk found for either team -- not observed in the live
        # corpus, see the docstring, but handled honestly rather than
        # assumed away): metadata falls back to the game's own first row,
        # the nearest available context when there truly is no preceding one.
        anchor = rows[max(anchor_idx, 0)]
        for score_type in group["score_types"]:
            synthetic = _synthetic_row(anchor, group["team"], score_type)
            synthetic["plays_incomplete"] = 1
            by_anchor.setdefault(anchor_idx, []).append(synthetic)
            if score_type == "TD":
                n_synthetic_td += 1
            else:
                n_synthetic_conv += 1
        notices.append(
            f"events-ledger: TD for {group['team']!r} (sequenceNumber {group['sequence']}) "
            "confirmed by the ledger but absent from /plays -- inserted as a synthetic "
            f"touchdown row after play_id {anchor['play_id']}"
        )

    if pending_td_groups:
        for row in rows:
            row["plays_incomplete"] = 1
        notices.append(
            f"events-ledger: {n_synthetic_td} synthetic TD row(s) and {n_synthetic_conv} "
            'synthetic conversion row(s) inserted from the events ledger -- flagged '
            'score_source="events-ledger-synthetic"/plays_incomplete=1 and excluded from '
            "EP/WP training, but do feed the reconstructed running score so later real "
            "plays' score_differential/WP features reflect it correctly"
        )

    leading = by_anchor.pop(-1, [])
    new_rows: list[dict] = list(leading)
    for i, row in enumerate(rows):
        new_rows.append(row)
        new_rows.extend(by_anchor.get(i, []))

    for play_id, row in enumerate(new_rows, start=1):
        row["play_id"] = play_id

    return pl.DataFrame(new_rows, schema=_PLAYS_WORKING_SCHEMA), notices


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
    ("IFAF World Flag 2026 Women"). When `divisions` is absent entirely
    (a tournament document shape that predates this addendum, or a future
    one that never sets it), `competition` stays exactly the bare
    `tournament.name` -- unchanged from the pre-2026-09-06 behavior, since
    there is no ambiguity to resolve for a single tournament with no
    division info at all.
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


# ---------------------------------------------------------------------------
# `/games/{id}/plays` (the reviewer-feed) — primary source, 2026-09-07.
# ---------------------------------------------------------------------------

# `events[].action` vocabulary observed on `/plays` records (docs/ifaf-field-
# mapping.md Nachtrag 2026-09-07, full 5,522-record corpus, both tournaments):
# PASS, COMPLETE, FLAG_PULL, INCOMPLETE_PASS, TOUCHDOWN, TRY, RUSH, PENALTY,
# PASS_BREAK_UP, INTERCEPTION, SACK, SAFETY. `HAND_OFF` was never observed in
# this corpus but is kept in `_PLAYS_RUN_ACTIONS` as a documented, harmless
# synonym for `RUSH` (both would classify identically) in case a future
# refresh's charting uses it.
_TRY_ACTION = "TRY"
_PLAYS_PASS_ACTIONS = frozenset(
    {"PASS", "COMPLETE", "INCOMPLETE_PASS", "INTERCEPTION", "SACK", "PASS_BREAK_UP"}
)
_PLAYS_RUN_ACTIONS = frozenset({"RUSH", "HAND_OFF"})
_KNOWN_PLAYS_ACTIONS = _PLAYS_PASS_ACTIONS | _PLAYS_RUN_ACTIONS | {
    "TOUCHDOWN",
    _TRY_ACTION,
    "PENALTY",
    "FLAG_PULL",
    "SAFETY",
}

# Working frame schema for the `/plays`-derived primary path. Distinct from
# `_WORKING_SCHEMA` (the unified-plays fallback path's frame) because the two
# sources compute outcome flags directly at flatten time here (no separate
# `derive_outcome_columns`-equivalent pass is needed — the `events[]` action
# list is already fully resolved per play, unlike unified-plays' single
# `outcome.type` string). `_`-prefixed columns are working-only and dropped by
# `conform_to_canonical`'s final `select`, same convention as `_WORKING_SCHEMA`.
_PLAYS_WORKING_SCHEMA: dict[str, pl.DataType] = {
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
    "complete_pass": pl.Int32,
    "sack": pl.Int32,
    "interception": pl.Int32,
    "safety": pl.Int32,
    "touchdown": pl.Int32,
    "def_touchdown": pl.Int32,
    "one_point_conv_success": pl.Int32,
    "two_point_conv_success": pl.Int32,
    "defensive_two_point_conv": pl.Int32,
    "penalty": pl.Int32,
    "qb": pl.Utf8,
    "thrown_by": pl.Utf8,
    "received_by": pl.Utf8,
    "target": pl.Utf8,
    "pass_side": pl.Utf8,
    "pass_depth": pl.Utf8,
    "incomplete_reason": pl.Utf8,
    "penalty_type": pl.Utf8,
    "source_detail": pl.Utf8,
    "source_play_sequence": pl.Float64,
    "nullified": pl.Int32,
    "official_score": pl.Utf8,
    "score_source": pl.Utf8,
    "plays_incomplete": pl.Int32,
    "_missing_down": pl.Int32,
    "_missing_ballon": pl.Int32,
    "_missing_offense": pl.Int32,
    "_nullified": pl.Int32,
    "_unknown_action": pl.Utf8,
}


def _extract_plays_records(payload: Any) -> list | None:
    """Same tolerance as `_extract_plays_list`, but for the `/plays` response
    shape: a top-level list, or an object wrapping the play array under
    `plays` (the only wrapper key observed live for this endpoint)."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        value = payload.get("plays")
        if isinstance(value, list):
            return value
    return None


def load_plays_snapshot(path: Path) -> list:
    """Read one `plays_{game_id}.json` snapshot (the `/games/{id}/plays`
    reviewer feed) from disk.

    Accepts a top-level list or an object wrapping the play array under
    `plays` (the observed shape). Raises `UnparseablePayload` naming the file
    when neither shape is present or the file is not valid JSON. An empty list
    is a valid, real payload (a reconciliation gap -- `reconciliation.reason
    == "no-tries-labelled"` -- or a genuine forfeit), not an error; deciding
    whether an empty/missing/unparseable `/plays` snapshot should fall back to
    `unified-plays` is `ingest_snapshots`' job, not this function's.
    """
    path = Path(path)
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        raise UnparseablePayload(f"{path}: could not read/parse JSON ({exc})") from exc

    plays = _extract_plays_records(payload)
    if plays is None:
        raise UnparseablePayload(
            f"{path}: unrecognized /plays payload shape "
            "(expected a top-level list or an object with a 'plays' list)"
        )
    return plays


def _load_teams_meta(raw_dir: Path) -> dict[str, str]:
    """`playerId` -> player name, folded from every `tournament_*_teams.json`
    roster file under `raw_dir`.

    These roster files are gitignored (never committed) and carry real player
    names (PII) -- this function only ever runs against a local `raw_dir` at
    ingest time, never against a test fixture or anything that reaches a
    commit. Player ids are team-code-prefixed (`w-esp-p16`) and observed
    globally unique across every roster file in the live corpus, so every
    file's players are folded into one flat dict with no collision handling
    needed.
    """
    names: dict[str, str] = {}
    for path in sorted(raw_dir.glob("tournament_*_teams.json")):
        payload = _read_json_or_empty(path)
        if not isinstance(payload, list):
            continue
        for team in payload:
            if not isinstance(team, dict):
                continue
            for player in team.get("players") or []:
                if not isinstance(player, dict):
                    continue
                pid = player.get("id")
                name = player.get("name")
                if pid and name:
                    names[str(pid)] = str(name)
    return names


def _plays_record_sort_key(index: int, play: Any) -> tuple[float, int]:
    """Sort key for one `/plays` record, resilient to malformed input --
    mirrors `_play_sort_key`'s resilience contract but keys on `sequence`
    (a float: inserted rows use a `.5` suffix, e.g. `907.5`) instead of
    `playNumber`. A record with a usable numeric `sequence` sorts by that
    value; everything else (missing/null/non-numeric `sequence`, or a
    non-dict entry) sorts after all of those, in stable payload order.
    """
    seq = play.get("sequence") if isinstance(play, dict) else None
    if isinstance(seq, (int, float)) and not isinstance(seq, bool):
        return (float(seq), index)
    return (float("inf"), index)


def _play_type_from_actions(
    actions: set[str], is_extra_point: bool, is_no_play: bool
) -> str | None:
    """Classify one `/plays` record's `play_type` from its `events[].action`
    set. Priority: a no-play (nullified or penalty-only) record is always
    `"no_play"`; an extra-point-shaped record (`is_extra_point` -- a TRY
    action, or `officialScore in {"XP1", "XP2"}` on its own; see
    `flatten_plays_records`) is always `"extra_point"` regardless of whether
    the attempt itself was thrown or run (matches the unified-plays fallback
    path's `_PLAY_TYPE_FROM_OUTCOME` convention for `XP1`/`XP2`/`TRY`);
    otherwise a pass-shaped action wins over a run-shaped one (yards-after-
    catch running on a completed pass must not misclassify it as a run --
    same precedence `_play_type_from_sequence` already uses for the fallback
    path); a record with neither signal (an empty/ambiguous action list)
    stays null, per the null-is-for-unparsed contract convention.
    """
    if is_no_play:
        return "no_play"
    if is_extra_point:
        return "extra_point"
    if actions & _PLAYS_PASS_ACTIONS:
        return "pass"
    if actions & _PLAYS_RUN_ACTIONS:
        return "run"
    return None


def flatten_plays_records(
    payload: list, game_meta: dict, game_id: str, player_names: dict[str, str]
) -> pl.DataFrame:
    """Turn one game's `/plays` array into one canonical-shaped row per play
    record.

    `play_id` is assigned 1..N by sorting on the record's own `sequence`
    (`_plays_record_sort_key`), not by trusting play-record order in the
    payload. The raw `sequence` value is preserved verbatim as the
    `source_play_sequence` extra. `drive_id` starts at 1 and increments only
    when `offenseTeamId` changes between two records where it is known (same
    convention `flatten_unified_plays` uses for `possessionTeamId`).

    Every play record becomes exactly one canonical row -- never silently
    dropped. A `nullified` record (the reviewer overturned it) or a
    penalty-only record (its only `events[].action` is `PENALTY`, a dead-ball
    foul with no live-play result) becomes a `play_type == "no_play"` row with
    every scoring/turnover flag (`complete_pass`/`sack`/`interception`/
    `safety`/`touchdown`/`def_touchdown`/`one_point_conv_success`/
    `two_point_conv_success`) forced to 0 and `yards_gained` left for
    `derive_yardage_columns_plays` to null explicitly -- the record's own raw
    `result_raw` (a comma-joined action list) is still preserved for
    traceability, but nothing it implies (a score, a turnover, a gain) is
    ever trusted for a record the reviewer marked overturned or a record that
    is pure penalty bookkeeping. `penalty` is the one exception to "every
    flag forced to 0": it is a classification of the record shape itself
    (was this entry a foul call, at all), not an effect, so it is set from
    `events[].action` unconditionally, including on a nullified record -- a
    nullified PENALTY-only record still IS a penalty call with no down of
    its own, and `validation.checks.downs_range`'s no-play exemption
    (`play_type == "no_play"` AND `penalty == 1`) depends on this flag
    surviving nullification to recognize it.

    `complete_pass`/`sack`/`interception`/`safety`/`penalty` are read
    directly off the record's own `events[].action` set (no separate
    `outcome.type`-style single field exists on this endpoint for those).
    **Scoring is different: `touchdown`/`def_touchdown`/
    `one_point_conv_success`/`two_point_conv_success` are read from the
    record's own `officialScore` field (`"TD"`/`"XP1"`/`"XP2"`/`"NONE"`/
    absent), the reviewer's per-record scoring verdict -- never from the
    `TOUCHDOWN`/`TRY` action names alone** (2026-09-07 fix, see
    docs/ifaf-field-mapping.md's same-day Nachtrag: the live corpus has
    hundreds of `TOUCHDOWN`-actioned records whose `officialScore` is
    `"XP1"`/`"XP2"`/`"NONE"`, not 6 points -- e.g. a PAT catch the reviewer
    tool sometimes charts as `PASS, COMPLETE, TOUCHDOWN` instead of using a
    `TRY` action, distinguishable only by `officialScore`). `officialScore ==
    "TD"` on a non-TRY record sets `touchdown`, except when `INTERCEPTION`
    also appears on the same record (a pick-six), which sets `def_touchdown`
    instead. `officialScore == "XP1"`/`"XP2"` sets `one_point_conv_success`/
    `two_point_conv_success` respectively, on either a TRY-actioned record or
    a TOUCHDOWN-actioned one. A TRY-actioned record whose own `officialScore`
    reads `"TD"` (21 in the live corpus) is itself a data-entry quirk, not a
    6-point try -- see the backfill pass after the main per-record loop for
    how it is resolved (using the TRY event's own `tryPoints`/`tryGood`
    fields as the fallback signal for that record's own points, and crediting
    the real 6 points to the touchdown record it evidences). `officialScore
    == "NONE"`/absent sets nothing (a failed try, an overturned/no-score
    touchdown, or an ordinary non-scoring play). `defensive_two_point_conv`
    is always 0 -- no record combining a failed `TRY`'s defensive return
    with a score was observed live, so this flag stays a documented-absent
    case for this source (see the field-mapping doc), not a silently-wrong
    guess.

    `qb`/`thrown_by` both resolve to the `PASS` event's own `playerId` (this
    source carries exactly one passer identity per play, unlike Hudl's two
    separately-charted `QB`/`THROWN BY` columns, so both extras get the same
    value rather than leaving one perpetually null) -- falling back to an
    `INCOMPLETE_PASS` event's `playerId` when no `PASS` event exists on the
    record (a charting gap observed on ~1% of the live corpus: an incompletion
    recorded with no separate `PASS` action, even though `INCOMPLETE_PASS`
    itself carries the same `playerId`/`intendedReceiverId` shape). `target`
    resolves the same way (`intendedReceiverId` off whichever of those two
    events is present) when a pass was thrown, else to a `RUSH`/`HAND_OFF`
    event's own `playerId` (the ball carrier) -- mirroring
    `ingest/sportapp.py`'s existing `rusher -> target` convention, the
    closest established precedent for a structured (non-charted-free-text)
    source. `received_by` resolves to the `COMPLETE` event's `playerId` only
    (the offense's own receiver; stays null on an incompletion or an
    interception, where the offense never received the ball). All four are
    resolved through `player_names` (`_load_teams_meta`'s local, gitignored
    roster lookup) to a plain name string -- never left as a raw
    `w-esp-p16`-style id.

    A play record's own `down` is copied through as-is, except an
    extra-point-shaped record always gets `down = 0` (this project's existing
    PAT convention -- see `docs/data-contract.md`'s "DN = 0 markiert einen
    PAT-Play" and `derive_yards_to_go`'s own `down == 0` branch) even when
    the record's raw `down` field is itself non-null (a TOUCHDOWN-actioned
    PAT catch, see above) or null (observed on every TRY-actioned record in
    the live corpus) -- this lets `derive_yards_to_go` run unchanged on this
    working frame. "Extra-point-shaped" is a TRY action, or `officialScore in
    {"XP1", "XP2"}` on its own (same broadened signal `play_type` uses -- see
    `is_extra_point` below). A genuinely null `down` on a non-extra-point
    record (a real data gap, a handful of live plays in the corpus) stays
    null, per the null-is-for-unparsed contract convention, and is counted in
    `_missing_down` for `IngestNotices`.

    `nullified` (a new nullable extra, distinct from the `no_play`
    `play_type` it also drives) copies the record's own `nullified` flag
    through verbatim -- kept visible downstream so a genuinely overturned
    record stays distinguishable from an ordinary no-play penalty entry.
    """
    home_raw = game_meta.get("home_team")
    away_raw = game_meta.get("away_team")
    competition = game_meta.get("competition")
    season = game_meta.get("season")
    gender = game_meta.get("gender")
    tournament_id = game_meta.get("tournament_id")

    ordered = sorted(
        enumerate(payload), key=lambda pair: _plays_record_sort_key(pair[0], pair[1])
    )

    rows: list[dict] = []
    prev_offense_raw: str | None = None
    drive_id = 1

    # Per-row bookkeeping the officialScore backfill pass below needs but that
    # never reaches the canonical frame -- kept as a parallel list (same index
    # as `rows`) rather than folded into the row dicts themselves.
    score_meta: list[dict] = []

    for play_id, (_, play) in enumerate(ordered, start=1):
        if not isinstance(play, dict):
            play = {}

        events = play.get("events") or []
        if not isinstance(events, list):
            events = []
        dict_events = [e for e in events if isinstance(e, dict)]
        actions_seen = list(
            dict.fromkeys(e.get("action") for e in dict_events if e.get("action"))
        )
        actions_set = set(actions_seen)

        nullified = bool(play.get("nullified"))
        is_penalty_only = actions_set == {"PENALTY"}
        has_try = _TRY_ACTION in actions_set
        has_td_action = "TOUCHDOWN" in actions_set
        official_score = play.get("officialScore")
        # A try-shaped record for play_type/down purposes is either a TRY
        # action, or an officialScore of XP1/XP2 on its own -- the reviewer
        # feed sometimes charts a PAT catch with a TOUCHDOWN action instead
        # of TRY (docs/ifaf-field-mapping.md Nachtrag 2026-09-07), and
        # officialScore is the only reliable signal telling those apart from
        # a real 6-point touchdown.
        is_extra_point = has_try or official_score in ("XP1", "XP2")
        # A play is charted as it happened; a penalty is a separate, later
        # record (2026-09-07 domain-expert review, corpus-wide confirmed:
        # every PENALTY-only record has a preceding play, 30.8% of the time
        # annulling it). An annulled record keeps its own identity -- at
        # minimum, a nullified extra-point-shaped record (e.g. a successful
        # try called back by a following offensive foul, the QF's own
        # sequence 50) is still `"extra_point"`, not `"no_play"`: the attempt
        # happened, `officialScore`/`nullified` already correctly zero its
        # points, and `down` is already forced to 0 for it below -- nothing
        # about `no_play`'s null-down semantics was ever needed for this
        # record shape. A nullified non-extra-point record (a live pass/run
        # play overturned on review) is unaffected -- still `no_play`, since
        # this fix is scoped to the one record shape the review explicitly
        # named, not a blanket "nullified keeps play_type" rule (a broader
        # change to every play_type would need its own review of every
        # dependent no-play convention across this module).
        is_no_play = is_penalty_only or (nullified and not is_extra_point)

        offense_raw = play.get("offenseTeamId")
        if offense_raw is not None:
            if prev_offense_raw is not None and offense_raw != prev_offense_raw:
                drive_id += 1
            prev_offense_raw = offense_raw

        down_raw = play.get("down")
        down_working = 0 if is_extra_point else down_raw
        ball_on = play.get("ballOn")

        play_type = _play_type_from_actions(actions_set, is_extra_point, is_no_play)
        result_raw = ", ".join(actions_seen) if actions_seen else None

        unknown_action = next(
            (a for a in actions_seen if a not in _KNOWN_PLAYS_ACTIONS), None
        )

        pass_event = next((e for e in dict_events if e.get("action") == "PASS"), None)
        complete_event = next((e for e in dict_events if e.get("action") == "COMPLETE"), None)
        rush_event = next(
            (e for e in dict_events if e.get("action") in _PLAYS_RUN_ACTIONS), None
        )
        incomplete_event = next(
            (e for e in dict_events if e.get("action") == "INCOMPLETE_PASS"), None
        )
        penalty_event = next((e for e in dict_events if e.get("action") == "PENALTY"), None)
        try_event = next((e for e in dict_events if e.get("action") == _TRY_ACTION), None)

        complete_pass = sack = interception = safety = touchdown = def_touchdown = 0
        one_point = two_point = 0
        # `penalty` is a classification of what kind of dead-ball record this
        # is, not a scoring/turnover effect -- unlike every other flag below,
        # it is set unconditionally, including on a nullified record. A
        # nullified PENALTY-only record (the reviewer overturned the whole
        # entry, not just its outcome) still IS a penalty call with no down
        # of its own; suppressing this flag on nullified rows would silently
        # break `validation.checks.downs_range`'s no-play exemption (which
        # keys on `play_type == "no_play"` AND `penalty == 1`) for exactly
        # the record shape that exemption exists to cover.
        penalty = 1 if "PENALTY" in actions_set else 0
        if not nullified:
            complete_pass = 1 if "COMPLETE" in actions_set else 0
            sack = 1 if "SACK" in actions_set else 0
            interception = 1 if "INTERCEPTION" in actions_set else 0
            safety = 1 if "SAFETY" in actions_set else 0

            # Scoring comes from the reviewer's own per-record verdict
            # (`officialScore` in {"TD", "XP1", "XP2", "NONE"}, or absent/
            # null for an ordinary non-scoring play) -- never from the
            # `TOUCHDOWN`/`TRY` action names alone (docs/ifaf-field-mapping.md
            # Nachtrag 2026-09-07: those actions appear on plenty of records
            # `officialScore` marks as worth 1, 2, or 0 points, not 6 -- e.g.
            # a `PASS, COMPLETE, TOUCHDOWN` record can be the try attempt
            # right after the real touchdown, `officialScore: XP1`).
            if has_try:
                if official_score == "TD":
                    # Ambiguous: a TRY-actioned record's own officialScore
                    # can never really mean "this try is worth 6" -- it is
                    # either a duplicate of the preceding real touchdown's
                    # own officialScore (already handled there) or evidence
                    # that record was mislabeled and the real 6 points
                    # belong to it instead (the backfill pass below). This
                    # row's own points, if any, come from the TRY event's
                    # own tryGood/tryPoints fields, which the live corpus
                    # shows agree with the *other* try records' officialScore
                    # closely enough to trust as the fallback signal here.
                    if try_event is not None:
                        try_points = try_event.get("tryPoints")
                        try_good = try_event.get("tryGood")
                        if try_good is True and try_points == 1:
                            one_point = 1
                        elif try_good is True and try_points == 2:
                            two_point = 1
                elif official_score == "XP1":
                    one_point = 1
                elif official_score == "XP2":
                    two_point = 1
                # officialScore NONE/null -> failed/overturned try, 0 points.
            elif has_td_action:
                if official_score == "TD":
                    if "INTERCEPTION" in actions_set:
                        def_touchdown = 1
                    else:
                        touchdown = 1
                elif official_score == "XP1":
                    # A PAT catch the reviewer feed charted with a TOUCHDOWN
                    # action instead of TRY (seen live, e.g. sequence 200 of
                    # the ESP-MEX QF) -- officialScore is authoritative.
                    one_point = 1
                elif official_score == "XP2":
                    two_point = 1
                # officialScore NONE/null on a TOUCHDOWN-actioned record: no
                # points, unless a later TRY record's officialScore == "TD"
                # names this exact record as the real touchdown (the
                # backfill pass below).
            # officialScore XP1/XP2 with neither a TRY nor a TOUCHDOWN action
            # (4 live-corpus SAFETY-only records) is not a conversion -- the
            # `safety` flag above already books the 2 points to the defense.

        score_meta.append(
            {
                "has_try": has_try,
                "has_td": has_td_action,
                "official_score": official_score,
                "nullified": nullified,
                "actions_set": actions_set,
            }
        )

        # A `PASS` event is the normal source for the passer/intended-receiver
        # ids, but ~1% of the live corpus records an `INCOMPLETE_PASS` action
        # with no accompanying `PASS` event at all (a charting gap, not a run
        # play) -- `INCOMPLETE_PASS` events carry the same `playerId`/
        # `intendedReceiverId` shape, so it is used as the fallback throw
        # event rather than leaving `qb`/`target` null for a play that was,
        # in fact, a pass attempt.
        throw_event = pass_event if pass_event is not None else incomplete_event
        qb_name = (
            player_names.get(throw_event.get("playerId")) if throw_event is not None else None
        )
        target_id = None
        if throw_event is not None:
            target_id = throw_event.get("intendedReceiverId")
        elif rush_event is not None:
            target_id = rush_event.get("playerId")
        target_name = player_names.get(target_id) if target_id else None
        received_by_name = (
            player_names.get(complete_event.get("playerId"))
            if complete_event is not None
            else None
        )

        pass_side = pass_depth = None
        for e in dict_events:
            if pass_side is None and e.get("passSide") is not None:
                pass_side = e.get("passSide")
            if pass_depth is None and e.get("passDepth") is not None:
                pass_depth = e.get("passDepth")

        incomplete_reason = (
            incomplete_event.get("incompleteReason") if incomplete_event is not None else None
        )
        penalty_type = penalty_event.get("penaltyType") if penalty_event is not None else None

        raw_sequence = play.get("sequence")
        source_play_sequence = (
            float(raw_sequence)
            if isinstance(raw_sequence, (int, float)) and not isinstance(raw_sequence, bool)
            else None
        )

        rows.append(
            {
                "source": "ifaf",
                "source_game_id": str(game_id),
                "game_id": make_game_id("ifaf", game_id),
                "play_id": play_id,
                "drive_id": drive_id,
                "half": play.get("half"),
                "down": down_working,
                "yards_to_go": None,
                "yardline": None,
                "yardline_50": ball_on,
                "yardline_50_after": None,
                "yardline_50_simple": None,
                "yards_to_go_simple": None,
                "yards_gained": None,
                "first_down": None,
                "game_clock_ms": None,
                "half_seconds_remaining": None,
                "posteam": offense_raw,
                "posteam_after": None,
                "home_team": home_raw,
                "away_team": away_raw,
                "defteam": _other_team(offense_raw, home_raw, away_raw),
                "play_type": play_type,
                "result_raw": result_raw,
                "description": None,
                "competition": competition,
                "season": season,
                "gender": gender,
                "tournament_id": tournament_id,
                "complete_pass": complete_pass,
                "sack": sack,
                "interception": interception,
                "safety": safety,
                "touchdown": touchdown,
                "def_touchdown": def_touchdown,
                "one_point_conv_success": one_point,
                "two_point_conv_success": two_point,
                "defensive_two_point_conv": 0,
                "penalty": penalty,
                "qb": qb_name,
                "thrown_by": qb_name,
                "received_by": received_by_name,
                "target": target_name,
                "pass_side": pass_side,
                "pass_depth": pass_depth,
                "incomplete_reason": incomplete_reason,
                "penalty_type": penalty_type,
                "source_detail": None,
                "source_play_sequence": source_play_sequence,
                # Counted against `down_working`, not `down_raw`: a TRY row's
                # raw `down` is always null in the live corpus, but
                # `down_working` resolves it to 0 by convention above -- that
                # is a resolved value, not a missing-data gap, so it must not
                # inflate this notice.
                "nullified": 1 if nullified else 0,
                # Raw `officialScore` verbatim (kept as an audit extra even on
                # rows the events-ledger alignment below overrides or leaves
                # unscored -- 2026-09-07 sixth follow-up). `score_source`
                # starts null here; `apply_events_ledger` is the only place
                # that ever sets it to `"events-ledger"`/`"events-ledger-synthetic"`.
                "official_score": official_score,
                "score_source": None,
                # `apply_events_ledger` is the only place that ever sets this
                # to 1 (a whole synthetic-touchdown insertion happened
                # somewhere in this game); null everywhere else, including
                # every non-ifaf row (canonical.NULLABLE_EXTRAS default).
                "plays_incomplete": None,
                "_missing_down": 0 if down_working is not None else 1,
                "_missing_ballon": 0 if ball_on is not None else 1,
                "_missing_offense": 0 if offense_raw is not None else 1,
                "_nullified": 1 if nullified else 0,
                "_unknown_action": unknown_action,
            }
        )

    # Backfill: a TRY record whose own officialScore reads "TD" names the
    # nearest preceding TOUCHDOWN-actioned record as the real touchdown --
    # either confirming it (that record's own officialScore is already
    # "TD", a duplicate, nothing to do) or correcting it (that record's own
    # officialScore reads "NONE"/null despite the TOUCHDOWN action, and the
    # real 6 points belong there). See the scoring block above for the full
    # rationale; this is the second half of that same data-entry-quirk fix.
    for idx, meta in enumerate(score_meta):
        if not (meta["has_try"] and meta["official_score"] == "TD" and not meta["nullified"]):
            continue
        anchor_idx = next(
            (
                j
                for j in range(idx - 1, -1, -1)
                if score_meta[j]["has_td"]
                # Excludes a TOUCHDOWN-actioned record that is itself
                # try-shaped (officialScore XP1/XP2, or -- belt and braces
                # -- a TRY action too): the seq-200-style PAT-catch-charted-
                # as-touchdown quirk is not a valid backfill anchor either,
                # same exclusion `apply_events_ledger.find_td` already
                # applies for the ledger-driven path (2026-09-07, sixth
                # follow-up finding: this search had the identical gap).
                and score_meta[j]["official_score"] not in ("XP1", "XP2")
                and "TRY" not in score_meta[j]["actions_set"]
            ),
            None,
        )
        if anchor_idx is None:
            continue
        anchor_meta = score_meta[anchor_idx]
        if anchor_meta["nullified"] or anchor_meta["official_score"] == "TD":
            continue
        anchor_row = rows[anchor_idx]
        if anchor_row["touchdown"] == 0 and anchor_row["def_touchdown"] == 0:
            if "INTERCEPTION" in anchor_meta["actions_set"]:
                anchor_row["def_touchdown"] = 1
            else:
                anchor_row["touchdown"] = 1

    return pl.DataFrame(rows, schema=_PLAYS_WORKING_SCHEMA)


def derive_yardage_columns_plays(df: pl.DataFrame) -> pl.DataFrame:
    """Derive `yards_gained` for the `/plays`-primary working frame, from
    consecutive plays' `yardline_50` (`ballOn`) within one game, ordered by
    `play_id`. Must run after `flatten_plays_records` (needs the
    `play_type`/`touchdown`/`safety`/`interception`/`def_touchdown`/
    `defensive_two_point_conv` columns it already sets) and before
    `derive_yards_to_go`, on a single game's frame (not grouped
    `.over("game_id")` -- matches `ingest_snapshots`' one-game-at-a-time call
    pattern for every other per-game derivation in this module).

    Rules, in priority order (parallel to `derive_yardage_columns`'s rules for
    the unified-plays fallback path, adapted for the columns this path
    actually has):

    1. `play_type == "no_play"` (nullified or penalty-only): null -- never a
       fabricated gain for a record the reviewer overturned or a dead-ball
       foul call.
    2. `down == 0` (a TRY/PAT record, per `flatten_plays_records`'s down
       convention): null, explicitly excluded -- a conversion attempt is not
       a down-progression gain, and the next record's `ballOn` (the
       following kickoff-equivalent possession) is not this attempt's own
       result.
    3. `touchdown == 1`: `50 - yardline_50` (distance from the snap spot to
       the opponent goal line).
    4. `safety == 1`: `-yardline_50` (tackled at the offense's own goal
       line).
    5. A turnover-shaped record (`interception`, `def_touchdown`, or
       `defensive_two_point_conv`): null -- the next record's `ballOn`
       belongs to the new possession, not this offense's own gain.
    6. Otherwise, if the next record (by `play_id`) shares this record's
       `drive_id`: `next.yardline_50 - yardline_50`. This also correctly
       absorbs a live-ball or dead-ball penalty's yardage adjustment with no
       special-casing: when a no-play penalty record sits between two real
       plays on the same drive, it is *this* rule's own "next record" for
       the live play immediately before it -- the penalty record's own
       `ballOn` already reflects the server-tracked, enforced spot, so the
       diff naturally includes whatever the penalty moved.
    7. Otherwise (last play of a drive/game, or a possession change with
       none of the flags above): null.

    A null `yardline_50` on this row or the next (a missing-`ballOn` record --
    `_missing_ballon`) propagates to a null `yards_gained` automatically, same
    as `derive_yardage_columns`.
    """
    turnover_like = (
        (pl.col("interception") == 1)
        | (pl.col("def_touchdown") == 1)
        | (pl.col("defensive_two_point_conv") == 1)
    )

    next_drive = pl.col("drive_id").shift(-1)
    next_yardline = pl.col("yardline_50").shift(-1)
    same_drive_next = next_drive == pl.col("drive_id")

    gain = (
        pl.when(pl.col("play_type") == "no_play")
        .then(None)
        .when(pl.col("down") == 0)
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


# 2026-09-07 (events-feed reconstruction proof, see docs/ifaf-field-mapping.md's
# same-day Nachtrag): an events-feed-based reconstruction of pre-snap state
# (replaying POSSESSION_CHANGE/DOWN_UPDATE/LOS_UPDATE/TRY_DOWN/STATUS_CHANGE/
# MANUAL_EDIT in `sequenceNumber` order) was built and measured against the 29
# women's games that have real `/plays` data to check against: **77.5% down
# agreement, 46.8% ballOn agreement** (nearest-preceding-event match against
# each play's own `startedAt`) -- both well under the 95% bar required before
# a reconstructed source may feed the canonical corpus. At least one game
# (`ffwc26-wb1`) showed a multi-hour `clientTimestamp`/`startedAt` epoch
# offset that alone explains a large share of its mismatches, and the
# remaining noise matches the already-documented finding that `LOS_UPDATE`
# fires far more often than there are real plays. The reconstruction is
# therefore NOT wired into this module: a game whose `/plays` response is a
# real, structured "not reviewed yet" signal (a non-null `reconciliation.reason`
# on an empty response, e.g. `no-tries-labelled`) is excluded from the
# canonical corpus entirely (`notices.skipped = True`, zero rows) rather than
# accepted on `unified-plays.context`, which round 1 of this same fix already
# proved unreliable. A `/plays` snapshot that is simply missing, unparseable,
# or empty with no reconciliation reason at all (a genuine zero-play forfeit)
# still falls back to `unified-plays` exactly as before -- there is no
# structured "this game's data is known-incomplete" signal in those cases,
# unlike a named reconciliation gap.
_RECONSTRUCTION_EXCLUSION_REASON = (
    "no usable /plays snapshot ({reconciliation_reason!r}) -- unified-plays.context "
    "is known-unreliable (2026-09-07 finding) and an events-feed reconstruction was "
    "measured at only 77.5% down / 46.8% ballOn agreement against 29 verified /plays "
    "games (below the 95% bar), so this game is excluded rather than accepted on "
    "unreliable pre-snap state"
)


# 2026-09-07 (tenth follow-up, same day) -- a THIRD, structurally-driven
# reconstruction attempt, distinct from both the timestamp-based replay above
# (77.5%/46.8%) and the third follow-up's own two positional-alignment
# variants (`docs/ifaf-field-mapping.md`: whole-drive 58.8%, down-cycle
# 31.4%). Rather than matching by `clientTimestamp`/`startedAt` proximity or
# by a fixed positional index within a drive/cycle, this attempt replays the
# events feed as an explicit pre-snap state machine and aligns the emitted
# state sequence to `/plays`' own real records by *structure* -- team and
# down, walked in parallel, tolerating a bounded (`lookahead=1`) count
# mismatch on either side, never crossing a `POSSESSION_CHANGE` boundary.
#
# **Measured, not adopted.** Validated by blinding the real `ballOn` on every
# women's `/plays`-primary game with zero null `ballOn` on its own real plays
# (21 such games in the live corpus -- more than the 16 anticipated when this
# attempt was scoped, since some games the 2026-09-07 third follow-up counted
# among the 24 "accepted" games are quarantined for unrelated reasons
# (`score_reconstruction`/`downs_range`) that don't affect `ballOn`
# completeness; using all 21 is strictly more validation power, not a
# methodology change) and re-deriving each one's `ballOn` from its own events
# feed: **16.8% exact agreement (292/1,735 comparable rows)**, `down`
# agreement 41.8% (this state machine's own down-tracking, not `/plays`' own
# down field, which is copied through unchanged and never recomputed), `|Δ| ≤
# 2` agreement 18.9% -- all three well under the 95% bar, and the exact-match
# figure alone is *worse* than every one of the three previously-measured
# reconstructions above, not an improvement. See `validate_events_los_fill`'s
# own docstring for the root cause (found live, not assumed): the events
# feed's `LOS_UPDATE` stream fires at a finer granularity than `/plays`' own
# review-level down structure, and a genuinely dead spot (no yardage change
# between two consecutive downs -- an incomplete pass, a sack at the line)
# produces no `LOS_UPDATE` at all for that down, silently misattributing the
# *next* `LOS_UPDATE`'s spot to the wrong down once one gap opens. **No
# `ballOn` value is fabricated from this reconstruction.** The state machine,
# the alignment, and the validation gate below are kept as tested,
# permanently-available diagnostic tooling (this module's own established
# precedent for a measured-and-declined reconstruction, e.g. the fallback
# path exclusion above) -- never wired into `flatten_plays_records`/
# `ingest_snapshots`, and no `spot_source` extra was added, since the fill
# was never adopted.
_LOS_FILL_GATE_THRESHOLD = 0.95


def replay_events_los_states(events: list) -> tuple[list[list[dict]], Counter]:
    """Replay one game's `events_{id}.json` array as an explicit pre-snap
    state machine, walked in `sequenceNumber` order (`reverted == true`
    events skipped -- none were observed in the live corpus, but this is a
    correctness requirement, not an assumption).

    Returns `(segments, manual_edit_keys)`: `segments` is one list per
    `POSSESSION_CHANGE` event (in event order), each entry a `{"team",
    "down", "ballOn"}` state in the order it was emitted; `manual_edit_keys`
    is a `Counter` over every key seen under a `MANUAL_EDIT` event's own
    `payload.edits` dict, across the whole game -- see the "MANUAL_EDIT" case
    below for what this event type was found to actually carry.

    **Event semantics, verified against the live corpus (not the literal
    reading originally hypothesized -- see the docstring's own correction
    note at the end):**

    - `POSSESSION_CHANGE`: starts a new segment. Resets every piece of
      per-possession state (`team`, `down`, `ballOn`, and the "first down
      still pending" flag) -- a possession never inherits state from the one
      before it.
    - `DOWN_UPDATE` (`payload.down`): the FIRST `DOWN_UPDATE` of a possession
      emits its state immediately, using whatever `ballOn` is already known
      (the "5" default, or an earlier `LOS_UPDATE` that fired before this
      possession's first down -- the literal reading of "ballOn defaults to
      5 unless a LOS_UPDATE follows before the first DOWN_UPDATE"). Every
      SUBSEQUENT `DOWN_UPDATE` in the same possession does NOT emit
      immediately -- it only records the new down number and waits for the
      `LOS_UPDATE` that finalizes it (see below). If a down was already
      pending (a prior `DOWN_UPDATE` fired but no `LOS_UPDATE` closed it
      yet) when *another* `DOWN_UPDATE` arrives, the still-open down is
      flushed first, using its own last-known `ballOn` (unchanged from the
      previous down) -- this is required for a real, corpus-wide play
      shape: a down with literally no yardage change (an incomplete pass, a
      sack at the line) gets no `LOS_UPDATE` of its own at all, only the
      next `DOWN_UPDATE`.
    - `LOS_UPDATE` (`payload.ballOn`): if a down is currently open/pending
      (its own `DOWN_UPDATE` already fired, not yet flushed), this closes
      and emits it with the given `ballOn`. If no down is pending yet at all
      this possession (a `LOS_UPDATE` firing before the very first
      `DOWN_UPDATE`), it only overrides the "5" default the first down will
      use. Otherwise (a down was already closed/emitted, and a further
      `LOS_UPDATE` fires with no intervening `DOWN_UPDATE`) it is treated as
      an implicit down increment -- `down + 1` -- and emits immediately;
      this recovers the corpus's own "1, 2, 3, 2" reviewer-down-sequence
      inconsistency case (`docs/ifaf-field-mapping.md`'s first Nachtrag),
      where a genuine down was recorded via `LOS_UPDATE` alone with no
      matching `DOWN_UPDATE` at all.
    - `TRY_DOWN` (`payload.ballOn`): emits a `down = 0` try-state directly
      from the event's own `ballOn` field. Corpus-wide, every `TRY_DOWN`
      event carries a non-null `ballOn` (707/707 checked) -- the "defaults to
      45/40 by `tryPoints`" fallback this function's own initial design
      anticipated is dead code in the live corpus and intentionally not
      implemented; `payload.ballOn` is used directly.
    - `SCORE`: a boundary only -- closes out the possession's scoring, but
      emits no state of its own (the scoring play's own down/spot was
      already emitted by the `DOWN_UPDATE`/`LOS_UPDATE` pair that preceded
      it).
    - `MANUAL_EDIT`: inspected directly against the live corpus (required by
      this fix's own method, not assumed) -- every `payload.edits` key
      observed corpus-wide is UI/bookkeeping state with no down/ballOn/team
      information at all: `penaltyFlag` (a flag-on-the-field toggle),
      `tryDownPending`/`tryPointValue`/`currentContext.tryType` (clearing the
      try-attempt UI state once a try resolves), and a handful of
      end-of-game `halfTimeScore.*`/`periodScores.*` corrections. None of
      these carry a spot, a down, or a team -- `MANUAL_EDIT` is therefore a
      verified no-op for this state machine, not an unhandled case.
    - `CLOCK_START`/`CLOCK_ADJUST`/`CLOCK_STOP`/`TIMEOUT`/`DISTANCE_CHANGE`/
      `STATUS_CHANGE`: also ignored for state purposes -- none of them carry
      down/ballOn/team information relevant to this replay (`STATUS_CHANGE`
      carries `HALF_TIME`/`IN_PROGRESS`/`FINAL`/`OVERTIME` only, useful for
      half-tracking but not needed by the alignment this function feeds).

    **Correction from this fix's own original design.** The method this fix
    was scoped against hypothesized that `DOWN_UPDATE` itself emits a state
    immediately in every case, with `LOS_UPDATE` only ever setting up the
    *next* state. Checked against the live QF game's own event stream, this
    is wrong for every down after the first one in a possession: real
    `DOWN_UPDATE`s fire with a stale, not-yet-updated `ballOn` (the previous
    down's own spot), and the correct spot for that down only arrives via
    the `LOS_UPDATE` that follows it -- confirmed by manually tracing the
    QF's own opening ESP/MEX drives against `/plays`' own already-known
    (non-null) `ballOn` values for those exact plays. This function
    implements the corrected, verified semantics above, not the original
    hypothesis (Rule 1 -- the original design was a bug, not a valid
    alternative reading).
    """
    segments: list[list[dict]] = []
    manual_edit_keys: Counter = Counter()

    cur_seg: list[dict] | None = None
    team: str | None = None
    first_down_pending = True
    cur_down: int | None = None
    cur_ballon: float | int | None = None
    down_emitted = True

    for e in sorted(
        (e for e in events if isinstance(e, dict) and not e.get("reverted")),
        key=lambda e: (
            e.get("sequenceNumber")
            if isinstance(e.get("sequenceNumber"), (int, float))
            and not isinstance(e.get("sequenceNumber"), bool)
            else float("inf")
        ),
    ):
        et = e.get("eventType")
        payload = e.get("payload") or {}

        if et == "POSSESSION_CHANGE":
            cur_seg = []
            segments.append(cur_seg)
            team = payload.get("teamId")
            first_down_pending = True
            cur_down = None
            cur_ballon = None
            down_emitted = True
            continue

        if cur_seg is None:
            # No possession opened yet -- not observed live, but a state
            # emitted with no team is worse than dropping it.
            continue

        if et == "DOWN_UPDATE":
            k = payload.get("down")
            if first_down_pending:
                ballon = cur_ballon if cur_ballon is not None else 5
                cur_seg.append({"down": k, "ballOn": ballon, "team": team})
                cur_down, cur_ballon, down_emitted = k, ballon, True
                first_down_pending = False
            else:
                if not down_emitted:
                    cur_seg.append({"down": cur_down, "ballOn": cur_ballon, "team": team})
                cur_down = k
                down_emitted = False
            continue

        if et == "LOS_UPDATE":
            v = payload.get("ballOn")
            if first_down_pending:
                cur_ballon = v
            else:
                cur_ballon = v
                if not down_emitted:
                    cur_seg.append({"down": cur_down, "ballOn": v, "team": team})
                    down_emitted = True
                else:
                    cur_down = (cur_down or 0) + 1
                    cur_seg.append({"down": cur_down, "ballOn": v, "team": team})
                    down_emitted = True
            continue

        if et == "TRY_DOWN":
            cur_seg.append({"down": 0, "ballOn": payload.get("ballOn"), "team": team})
            continue

        if et == "SCORE":
            continue

        if et == "MANUAL_EDIT":
            for k in payload.get("edits") or {}:
                manual_edit_keys[k] += 1
            continue

        # CLOCK_START/CLOCK_ADJUST/CLOCK_STOP/TIMEOUT/DISTANCE_CHANGE/
        # STATUS_CHANGE -- verified no-op for this state machine, see
        # docstring above.

    return segments, manual_edit_keys


def _extract_real_down_records(plays_records: list) -> list[dict]:
    """Extract the real (non-`no_play`) down-having records from one game's
    raw `/plays` payload, sorted by `sequence`, for structural alignment
    against `replay_events_los_states`' own emitted states.

    Mirrors `flatten_plays_records`' own `no_play`/`is_extra_point`
    classification exactly (a `nullified` non-extra-point record, or a
    penalty-only record, is excluded -- it never had a real down of its
    own), but is intentionally a separate, read-only helper: this function
    feeds a diagnostic/validation path only (`validate_events_los_fill`/
    `diagnose_partial_los_fill`), never the canonical ingest, so it must
    not risk a behavior change in `flatten_plays_records` itself by sharing
    mutable state or being imported into that function's own hot path.
    """
    out: list[dict] = []
    for play in sorted(
        (p for p in plays_records if isinstance(p, dict)),
        key=lambda p: (
            p.get("sequence") if isinstance(p.get("sequence"), (int, float)) else float("inf")
        ),
    ):
        events_on_play = [e for e in (play.get("events") or []) if isinstance(e, dict)]
        actions = {e.get("action") for e in events_on_play if e.get("action")}
        nullified = bool(play.get("nullified"))
        is_penalty_only = actions == {"PENALTY"}
        has_try = _TRY_ACTION in actions
        official = play.get("officialScore")
        is_extra_point = has_try or official in ("XP1", "XP2")
        if is_penalty_only or (nullified and not is_extra_point):
            continue
        down = 0 if is_extra_point else play.get("down")
        out.append(
            {
                "sequence": play.get("sequence"),
                "team": play.get("offenseTeamId"),
                "down": down,
                "ballOn": play.get("ballOn"),
            }
        )
    return out


def _segment_records_by_team(records: list[dict]) -> list[list[dict]]:
    """Split a game's real down-having records into possession-shaped
    segments at every offense-team change, mirroring `flatten_plays_records`'
    own `drive_id` convention (increments only when `offenseTeamId`
    changes) -- the natural counterpart to `replay_events_los_states`' own
    `POSSESSION_CHANGE`-bounded segments."""
    segments: list[list[dict]] = []
    cur: list[dict] | None = None
    prev_team: str | None = None
    for r in records:
        if r["team"] != prev_team:
            cur = []
            segments.append(cur)
            prev_team = r["team"]
        cur.append(r)
    return segments


def align_events_los_states(
    real_segments: list[list[dict]],
    emitted_segments: list[list[dict]],
    lookahead: int = 1,
) -> list[tuple[dict, dict | None]]:
    """Align real `/plays` down-having records to `replay_events_los_states`'
    own emitted state sequence, segment-by-segment (never matching across a
    possession boundary -- each real segment is only checked against the
    emitted segment at the same position).

    Within one segment, walks both sides in parallel: for the current real
    record, checks the emitted state at the current position and up to
    `lookahead` states ahead for a `down` match (tolerating an emitted state
    the real feed has no counterpart for -- e.g. a duplicate `TRY_DOWN` from
    an operator retry, or the implicit-down-increment case's own occasional
    off-by-one). A match consumes every emitted state up to and including
    the matched one; no match leaves the real record's own aligned state
    `None` (a diagnostic miss, never filled with a wrong guess) and does not
    consume any emitted state, so the next real record gets a fresh look at
    the same position.

    Returns a list of `(real_record, matched_state_or_None)` pairs, in the
    same order as the concatenated real segments.
    """
    pairs: list[tuple[dict, dict | None]] = []
    for seg_idx, real_seg in enumerate(real_segments):
        emitted_seg = emitted_segments[seg_idx] if seg_idx < len(emitted_segments) else []
        ei = 0
        for r in real_seg:
            matched = None
            for skip in range(lookahead + 1):
                if ei + skip < len(emitted_seg) and emitted_seg[ei + skip]["down"] == r["down"]:
                    matched = emitted_seg[ei + skip]
                    ei = ei + skip + 1
                    break
            pairs.append((r, matched))
    return pairs


def validate_events_los_fill(raw_dir: Path, game_ids: Sequence[str]) -> dict:
    """Gate check for `replay_events_los_states`/`align_events_los_states`:
    for every game in `game_ids` (expected to already have zero null
    `ballOn` on its own real plays -- the caller's job to select those),
    blind the real `ballOn` (never touch it, only compare against it) and
    measure how often the aligned events-feed state would have reproduced
    it exactly, within `|Δ| <= 2`, and how often `down` alone agrees (a
    sanity check of the alignment independent of the flip/granularity
    issues that affect `ballOn` specifically).

    Returns `{"per_game": {game_id: {...}}, "overall": {...}, "adopted":
    bool}` -- `adopted` is `True` only when the overall exact-agreement rate
    is `>= _LOS_FILL_GATE_THRESHOLD` (0.95). A game with zero comparable
    (non-`None`-matched) rows contributes zeros to its own per-game rates
    without dividing by zero.

    **Root cause of the measured failure (16.8% exact -- 292/1,735 -- run
    2026-09-07 against 21 zero-null-ballOn women's games) --
    found live, not assumed:** the events feed's `LOS_UPDATE` stream is
    strictly finer-grained than `/plays`' own reviewed down structure. A
    real down with literally no yardage change (an incomplete pass, a sack
    at the line) fires no `LOS_UPDATE` of its own at all -- `/plays`' next
    real down still gets its own correct spot from ITS `LOS_UPDATE`, but the
    state machine has no way to know a down was "skipped" for LOS_UPDATE
    purposes until the down changes again, and one such gap immediately
    misaligns every state for the rest of that possession (`down`
    agreement collapses right along with `ballOn` on the same rows, ruling
    out a `ballOn`-only granularity story). Games recorded under the
    short-slug `ffwc26-w*` id shape measure consistently worse (4.7%-24.5%
    exact) than the UUID-shaped ids (18.8%-71.2%) -- checked directly
    against a hypothesized fixed-scale `ballOn` flip (`50 - ballOn`, found
    on ~21% of `TRY_DOWN` events corpus-wide, but only ever on 1 game's own
    `/plays` `ballOn` field, i.e. not a real ambiguity in the trusted
    source) as a possible explanation: applying the flip to every game and
    keeping whichever direction scores higher (an oracle upper bound no
    real caller could compute without already knowing the answer) barely
    moves the overall figure at all (16.8% -> 16.8%, per-game deltas all
    zero) -- the flip is not the cause of the low agreement in these games;
    the gap-then-cascade failure mode above is.
    """
    per_game: dict[str, dict] = {}
    totals = {"total": 0, "down_match": 0, "exact": 0, "delta2": 0}

    for gid in game_ids:
        events = _load_events_list(raw_dir, gid)
        plays_path = raw_dir / f"plays_{gid}.json"
        if events is None or not plays_path.exists():
            continue
        plays_payload = _read_json_or_empty(plays_path)
        plays_records = _extract_plays_records(plays_payload)
        if not plays_records:
            continue

        emitted_segments, _ = replay_events_los_states(events)
        real_records = _extract_real_down_records(plays_records)
        real_segments = _segment_records_by_team(real_records)
        pairs = align_events_los_states(real_segments, emitted_segments)

        g_total = g_down = g_exact = g_delta2 = 0
        for r, m in pairs:
            if r.get("ballOn") is None:
                # This validation only means something on rows where the
                # real value is known -- a caller passing a partially-spotted
                # game here would silently deflate its own denominator.
                continue
            g_total += 1
            if m is None:
                continue
            g_down += 1
            if m["ballOn"] == r["ballOn"]:
                g_exact += 1
            if isinstance(m["ballOn"], (int, float)) and abs(m["ballOn"] - r["ballOn"]) <= 2:
                g_delta2 += 1

        per_game[gid] = {
            "total": g_total,
            "down_match_rate": (g_down / g_total) if g_total else 0.0,
            "exact_rate": (g_exact / g_total) if g_total else 0.0,
            "delta2_rate": (g_delta2 / g_total) if g_total else 0.0,
        }
        totals["total"] += g_total
        totals["down_match"] += g_down
        totals["exact"] += g_exact
        totals["delta2"] += g_delta2

    overall_total = totals["total"]
    overall = {
        "total": overall_total,
        "down_match_rate": (totals["down_match"] / overall_total) if overall_total else 0.0,
        "exact_rate": (totals["exact"] / overall_total) if overall_total else 0.0,
        "delta2_rate": (totals["delta2"] / overall_total) if overall_total else 0.0,
    }
    return {
        "per_game": per_game,
        "overall": overall,
        "adopted": overall["exact_rate"] >= _LOS_FILL_GATE_THRESHOLD,
    }


def diagnose_partial_los_fill(raw_dir: Path, game_ids: Sequence[str]) -> dict:
    """Per-game diagnostic for a partially-spotted game (real plays with a
    null `ballOn` in `/plays`), for the provider question this fix's method
    calls for when the reconstruction gate fails: counts of null-`ballOn`
    real records, `LOS_UPDATE` events available in the game's own events
    feed, and how many of those null records this module's own structural
    alignment could even find a same-down candidate state for (a count of
    *candidates*, never a count of *correct fills* -- `validate_events_los_fill`'s
    own 16.8% exact-agreement rate on games where the truth is known applies
    identically here; a "matched" null record below is NOT more likely to be
    right than a "matched" row was on the validation set).

    Returns `{game_id: {"n_real_records", "n_null_ballon", "n_los_update_events",
    "n_null_structurally_matched", "n_null_unmatched"}}`. No `ballOn` value is
    ever written back anywhere by this function -- it is read-only reporting.
    """
    report: dict[str, dict] = {}
    for gid in game_ids:
        events = _load_events_list(raw_dir, gid)
        plays_path = raw_dir / f"plays_{gid}.json"
        if events is None or not plays_path.exists():
            continue
        plays_payload = _read_json_or_empty(plays_path)
        plays_records = _extract_plays_records(plays_payload)
        if not plays_records:
            continue

        n_los_events = sum(
            1
            for e in events
            if isinstance(e, dict) and e.get("eventType") == "LOS_UPDATE" and not e.get("reverted")
        )

        emitted_segments, _ = replay_events_los_states(events)
        real_records = _extract_real_down_records(plays_records)
        real_segments = _segment_records_by_team(real_records)
        pairs = align_events_los_states(real_segments, emitted_segments)

        null_pairs = [(r, m) for r, m in pairs if r.get("ballOn") is None]
        report[gid] = {
            "n_real_records": len(real_records),
            "n_null_ballon": len(null_pairs),
            "n_los_update_events": n_los_events,
            "n_null_structurally_matched": sum(1 for _, m in null_pairs if m is not None),
            "n_null_unmatched": sum(1 for _, m in null_pairs if m is None),
        }
    return report


def _load_usable_plays_records(
    raw_dir: Path, game_id: str, notices: IngestNotices
) -> tuple[list | None, str | None]:
    """Classify this game's `/plays` snapshot, returning `(records, exclude_reason)`.

    Three outcomes:

    1. Usable (file exists, parses, non-empty play list): `(records, None)` --
       the primary path.
    2. A real, structured "not reviewed" signal -- the file parses to an empty
       play list AND carries a non-null `reconciliation.reason` (e.g.
       `no-tries-labelled`): `(None, exclude_reason)` -- `ingest_snapshots`
       excludes this game entirely rather than falling back to
       `unified-plays` (see `_RECONSTRUCTION_EXCLUSION_REASON`'s docstring
       for why the fallback is no longer trusted for this case).
    3. Anything else unusable (missing file, unparseable file, or an empty
       play list with no reconciliation reason at all -- a genuine zero-play
       forfeit): `(None, None)` -- `ingest_snapshots` falls back to
       `unified-plays` exactly as before; there is no structured signal here
       that the game's data is specifically known-incomplete, only that this
       particular endpoint has nothing for it.

    Any of the non-"missing file" unusable cases appends an explanatory
    message to `notices`.
    """
    path = raw_dir / f"plays_{game_id}.json"
    if not path.exists():
        return None, None
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        notices.messages.append(f"{path}: could not read/parse JSON ({exc})")
        return None, None

    records = _extract_plays_records(payload)
    if records is None:
        notices.messages.append(
            f"{path}: unrecognized /plays payload shape "
            "(expected a top-level list or an object with a 'plays' list)"
        )
        return None, None
    if records:
        return records, None

    reconciliation_reason = None
    if isinstance(payload, dict):
        reconciliation_reason = (payload.get("reconciliation") or {}).get("reason")

    if reconciliation_reason:
        exclude_reason = _RECONSTRUCTION_EXCLUSION_REASON.format(
            reconciliation_reason=reconciliation_reason
        )
        notices.messages.append(f"{path}: {exclude_reason}")
        return None, exclude_reason

    notices.messages.append(
        f"{path}: /plays snapshot present but empty, no reconciliation reason given "
        "(a genuine zero-play forfeit) -- falling back to unified-plays"
    )
    return None, None


def ingest_snapshots(
    raw_dir: Path,
    team_mapping: pl.DataFrame,
    game_ids: Sequence[str] | None = None,
    tournaments: Sequence[str] | None = None,
) -> list[tuple[str, pl.DataFrame, IngestNotices]]:
    """Parse every IFAF snapshot under `raw_dir` into a canonical frame, one
    game at a time.

    **Primary source, per game: `plays_{game_id}.json`** (the `/games/{id}/plays`
    reviewer feed — `flatten_plays_records`). When that snapshot is unusable,
    `_load_usable_plays_records` classifies why and `ingest_snapshots` picks
    one of two different responses (2026-09-07, see that function's own
    docstring and `_RECONSTRUCTION_EXCLUSION_REASON`'s for the full reasoning):

    - **A real, structured "not reviewed" signal** (the response parses to an
      empty play list AND carries a non-null `reconciliation.reason`, e.g.
      `no-tries-labelled`): the game is **excluded entirely** -- zero rows,
      `notices.skipped = True`, `skip_reason` naming the excluded game. An
      events-feed reconstruction of pre-snap state was built and measured
      against this exact scenario and found unreliable (77.5%/46.8% down/
      ballOn agreement, both under the required 95% bar), so this case is
      never accepted on `unified-plays.context` either -- that source was
      already proven unreliable by the 2026-09-07 `/plays`-primary rewrite.
    - **Anything else unusable** (missing file, unparseable file, or an empty
      play list with no reconciliation reason at all -- a genuine zero-play
      forfeit): falls back to `unified-plays_{game_id}.json`
      (`flatten_unified_plays`, unchanged from the pre-2026-09-07 primary
      path), stamped `source_detail = "unified-plays-fallback"` so those rows
      stay distinguishable. A primary-path row's `source_detail` stays null.

    Game discovery is the union of both snapshot kinds' filenames under
    `raw_dir` (a game with only a `plays_*.json` file, or only a
    `unified-plays_*.json` file, is still discovered), not just
    `unified-plays_*.json` as before.

    `games.json` and `tournament_*.json` (if present in `raw_dir`) supply
    home/away team labels and competition/season/gender per game; their absence
    degrades gracefully to null metadata rather than raising. `tournament_*_teams.json`
    roster files (if present) supply the `qb`/`thrown_by`/`received_by`/`target`
    player-name resolution for the primary path (`_load_teams_meta`); their
    absence degrades to null player names, not a raised error. A snapshot whose
    payload is unparseable is recorded as a skipped, zero-row, still-canonical-
    shaped result with a notice — it never aborts the remaining games. A snapshot
    with a real but empty play array (a forfeit) is not a skip; it is a genuine
    zero-row game. Any failure in the per-game chain from the flatten step
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
    player_names = _load_teams_meta(raw_dir)

    discovered_ids = {
        p.stem.removeprefix("unified-plays_") for p in raw_dir.glob("unified-plays_*.json")
    } | {p.stem.removeprefix("plays_") for p in raw_dir.glob("plays_*.json")}
    wanted = set(game_ids) if game_ids is not None else None
    wanted_tournaments = set(tournaments) if tournaments is not None else None

    results: list[tuple[str, pl.DataFrame, IngestNotices]] = []

    for gid in sorted(discovered_ids):
        if wanted is not None and gid not in wanted:
            continue
        if wanted_tournaments is not None:
            game_tournament_id = games_meta.get(gid, {}).get("tournamentId")
            if game_tournament_id not in wanted_tournaments:
                continue

        notices = IngestNotices(game_id=gid)

        game_entry = games_meta.get(gid, {})
        tournament_entry = tournaments_meta.get(game_entry.get("tournamentId"), {})
        game_meta = _build_game_meta(game_entry, tournament_entry)

        # Diagnostic only -- never affects which rows reach the canonical
        # frame. See `_events_score_ledger_summary`'s docstring for why this
        # stays a report line, not a source of fabricated rows.
        ledger_message = _events_score_ledger_summary(raw_dir, gid, game_entry)
        if ledger_message:
            notices.messages.append(ledger_message)

        plays_records, exclude_reason = _load_usable_plays_records(raw_dir, gid, notices)

        if exclude_reason is not None:
            # A real, structured "not reviewed" signal from `/plays` -- never
            # accepted on unified-plays.context (see `_load_usable_plays_records`'s
            # docstring). Excluded exactly like an unparseable snapshot: zero
            # rows, `skipped = True`, reason named in `skip_reason`.
            notices.skipped = True
            notices.skip_reason = exclude_reason
            results.append((gid, _empty_canonical_frame(), notices))
            continue

        if plays_records is not None:
            # Primary path: `/plays`. Same per-game exception containment as
            # the fallback path below (T-1.2-44 / T-1.2-45) -- any failure
            # anywhere in this chain skips only this game, with a notice.
            try:
                df = flatten_plays_records(plays_records, game_meta, gid, player_names)

                notices.missing_context_keys = {
                    "down": int(df["_missing_down"].sum()) if df.height else 0,
                    "ballOn": int(df["_missing_ballon"].sum()) if df.height else 0,
                    "offenseTeamId": int(df["_missing_offense"].sum()) if df.height else 0,
                }
                notices.missing_context_keys = {
                    k: v for k, v in notices.missing_context_keys.items() if v
                }

                # User-authorized (2026-09-07, sixth follow-up): for a game
                # whose events-feed SCORE ledger reproduces games.json's
                # official score, the ledger becomes the sole scoring
                # source, including inserting a synthetic row for a
                # ledger-confirmed conversion /plays never recorded. Must
                # run before map_teams -- the ledger's own teamId values
                # are raw cpx ids, matching posteam/defteam/home_team/
                # away_team here, not yet the canonical codes map_teams
                # produces. A strict no-op for every other game
                # (ffwc26-wd4, forfeits, any game with no events snapshot).
                events_list = _load_events_list(raw_dir, gid)
                if events_list is not None:
                    official = game_entry.get("currentScore") or {}
                    df, ledger_notices = apply_events_ledger(
                        df,
                        events_list,
                        game_meta.get("home_team"),
                        game_meta.get("away_team"),
                        official.get("home"),
                        official.get("away"),
                    )
                    notices.messages.extend(ledger_notices)

                df = map_teams(
                    df, team_mapping, "ifaf", ["posteam", "defteam", "home_team", "away_team"]
                )
                df = derive_yardage_columns_plays(df)
                df = derive_yards_to_go(df)

                if df.height:
                    unmapped = (
                        df.filter(pl.col("_unknown_action").is_not_null())
                        .group_by("_unknown_action")
                        .agg(pl.len().alias("count"))
                    )
                    if unmapped.height:
                        notices.unmapped_outcomes = dict(
                            zip(
                                unmapped["_unknown_action"].to_list(),
                                unmapped["count"].to_list(),
                            )
                        )
                        notices.messages.append(
                            f"{unmapped.height} unmapped /plays action value(s): "
                            f"{notices.unmapped_outcomes}"
                        )
                    nullified_count = int(df["_nullified"].sum())
                    if nullified_count:
                        notices.messages.append(
                            f"{nullified_count} nullified /plays record(s) folded in as "
                            "no_play rows"
                        )

                df = add_scoring_play_team(df, credit_defense=True)
                df = add_score_columns(df)

                df, conform_report = conform_to_canonical(df, "ifaf")
                if conform_report.cast_failures:
                    notices.messages.append(f"cast failures: {conform_report.cast_failures}")
            except (
                TypeError,
                AttributeError,
                ValueError,
                KeyError,
                pl.exceptions.PolarsError,
            ) as exc:
                notices.skipped = True
                notices.skip_reason = f"{type(exc).__name__}: {exc}"
                notices.messages.append(f"game {gid}: {type(exc).__name__}: {exc}")
                results.append((gid, _empty_canonical_frame(), notices))
                continue

            results.append((gid, df, notices))
            continue

        # Fallback path: no usable `/plays` snapshot for this game -- fall
        # back to `unified-plays` (the pre-2026-09-07 primary path,
        # unchanged). Every row this branch produces is stamped
        # `source_detail = "unified-plays-fallback"`.
        notices.messages.append(
            f"game {gid}: no usable /plays snapshot -- fell back to unified-plays"
        )

        unified_path = raw_dir / f"unified-plays_{gid}.json"
        try:
            payload, _ = load_snapshot(unified_path)
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
            df = df.with_columns(pl.lit("unified-plays-fallback").alias("source_detail"))
        except (TypeError, AttributeError, ValueError, KeyError, pl.exceptions.PolarsError) as exc:
            notices.skipped = True
            notices.skip_reason = f"{type(exc).__name__}: {exc}"
            notices.messages.append(f"game {gid}: {type(exc).__name__}: {exc}")
            results.append((gid, _empty_canonical_frame(), notices))
            continue

        results.append((gid, df, notices))

    return results
