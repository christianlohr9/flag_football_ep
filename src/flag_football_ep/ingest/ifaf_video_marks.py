"""Per-play video-mark table from the redacted cpx.studio `/games/{id}/plays` snapshots.

Reads `plays_{game_id}.json` (already redacted by `fetch/ifaf.py::_write_json` before it
touched disk -- no PII passes through this module) and builds one row per play with just
enough to locate the play in the source video: game/team/half/down/spot context, the
compact action-sequence label, and the video URL + timestamp. Contains no player names,
no emails, no user ids -- see docs/ifaf-field-mapping.md's 2026-09-06 addendum for the
redaction contract this table relies on.

No network access happens here -- this is a pure snapshot-to-table transform, the same
"parse only what's on disk" contract every other `ingest/*` module in this package follows.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from flag_football_ep.canonical import make_game_id

_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "source_game_id": pl.Utf8,
    "sequence": pl.Int64,
    "half": pl.Int32,
    "offense_team_id": pl.Utf8,
    "down": pl.Int32,
    "ball_on": pl.Int32,
    "nullified": pl.Boolean,
    "outcome": pl.Utf8,
    "video_url": pl.Utf8,
    "video_time_sec": pl.Float64,
    "video_time_source": pl.Utf8,
}


def _outcome_label(events: Any) -> str | None:
    """Join every event's `action` (e.g. "PASS+COMPLETE", "PENALTY") into one
    compact string. `None` when there are no events with a usable action."""
    if not isinstance(events, list):
        return None
    actions = [e.get("action") for e in events if isinstance(e, dict) and e.get("action")]
    return "+".join(actions) if actions else None


def _video_fields(play: dict, doc_video_url: str | None) -> tuple[str | None, float | None, str | None]:
    """Prefer the play's own `videoMark` (present when `videoTimeSource ==
    "marked"`); fall back to the document-level `videoUrl` (the game's single
    source recording) paired with the play's own `videoTimeSec` (present even
    for `videoTimeSource == "derived"` plays, an estimated offset)."""
    video_mark = play.get("videoMark")
    if isinstance(video_mark, dict) and video_mark.get("videoUrl"):
        return (
            video_mark.get("videoUrl"),
            video_mark.get("videoTimeSec"),
            play.get("videoTimeSource"),
        )
    time_sec = play.get("videoTimeSec")
    if time_sec is not None:
        return doc_video_url, time_sec, play.get("videoTimeSource")
    return None, None, play.get("videoTimeSource")


def build_video_marks_table(raw_dir: Path) -> pl.DataFrame:
    """Build one row per play across every `plays_{game_id}.json` snapshot under
    `raw_dir`. A missing/unparseable/empty snapshot is skipped silently (a
    forfeit or a reconciliation gap, same non-fatal-per-game contract as
    `ingest_snapshots`) -- never aborts the whole table.
    """
    raw_dir = Path(raw_dir)
    rows: list[dict] = []

    for path in sorted(raw_dir.glob("plays_*.json")):
        game_id = path.stem.removeprefix("plays_")
        try:
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        plays = payload.get("plays")
        if not isinstance(plays, list):
            continue
        doc_video_url = payload.get("videoUrl")

        for play in plays:
            if not isinstance(play, dict):
                continue
            video_url, video_time_sec, video_time_source = _video_fields(play, doc_video_url)
            rows.append(
                {
                    "game_id": make_game_id("ifaf", game_id),
                    "source_game_id": str(game_id),
                    "sequence": play.get("sequence"),
                    "half": play.get("half"),
                    "offense_team_id": play.get("offenseTeamId"),
                    "down": play.get("down"),
                    "ball_on": play.get("ballOn"),
                    "nullified": bool(play.get("nullified", False)),
                    "outcome": _outcome_label(play.get("events")),
                    "video_url": video_url,
                    "video_time_sec": video_time_sec,
                    "video_time_source": video_time_source,
                }
            )

    if not rows:
        return pl.DataFrame(schema=dict(_SCHEMA))
    return pl.DataFrame(rows, schema=_SCHEMA)
