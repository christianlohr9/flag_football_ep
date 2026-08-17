"""cpx.studio (IFAF WM-2026) discovery and paginated snapshot fetch.

Snapshot-first, same shape as `fetch/sportapp.py`: every response is written
verbatim to `data/raw/ifaf/`; no parsing happens here. Auth requirements and
the real `/games` payload shape were unverified at research time, so
discovery is defensive by design — an unrecognized shape is snapshotted and
reported, never guessed at or raised on.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import typer

from flag_football_ep.fetch.http import http_get_json

_MAX_EVENT_PAGES = 50
_GAMES_LIST_KEYS = ("data", "games", "items")
_TOURNAMENT_MATCH_KEYS = ("tournament", "tournamentId", "tournamentSlug")


def _write_json(path: Path, payload: object) -> None:
    """Atomically write `payload` as JSON to `path` (`.tmp` then `os.replace`)."""
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, path)


def _auth_headers(api_key: str | None) -> dict[str, str] | None:
    if not api_key:
        return None
    return {"Authorization": f"Bearer {api_key}"}


def _extract_games_list(payload: Any) -> list[dict] | None:
    """Defensively pull a list of game dicts out of the /games payload.

    Accepts either a top-level list, or an object exposing the list under
    one of `data`/`games`/`items`. Returns None if no recognized shape is
    found.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in _GAMES_LIST_KEYS:
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return None


def _game_matches_tournament(game: dict, tournament: str) -> bool:
    return any(game.get(key) == tournament for key in _TOURNAMENT_MATCH_KEYS)


def _game_id(game: dict) -> str | None:
    for key in ("id", "gameId"):
        value = game.get(key)
        if value:
            return str(value)
    return None


_EVENTS_PAGE_LIST_KEYS = ("events", "items", "data")


def _extract_events_page(page: Any) -> list:
    """Pull the list of event objects out of one `/events` page response.

    Verified live (2026-08-17) as `{"events": [...], "total": N,
    "hasMore": bool}`; `items`/`data` are kept as defensive fallbacks in
    case the endpoint's shape changes.
    """
    if isinstance(page, list):
        return page
    if isinstance(page, dict):
        for key in _EVENTS_PAGE_LIST_KEYS:
            value = page.get(key)
            if isinstance(value, list):
                return value
    return []


def _fetch_events(
    base_url: str,
    game_id: str,
    limit: int,
    headers: dict[str, str] | None,
    secret_values: list[str],
) -> list | None:
    """Paginate `/games/{id}/events`, merging pages into one list.

    Stops when a page returns fewer than `limit` items, or after
    `_MAX_EVENT_PAGES` pages (with a warning), whichever comes first.
    Returns None if any page request fails.
    """
    merged: list = []
    offset = 0
    for _page in range(_MAX_EVENT_PAGES):
        page = http_get_json(
            f"{base_url}/games/{game_id}/events",
            params={"includeReverted": "false", "limit": limit, "offset": offset},
            headers=headers,
            secret_values=secret_values,
        )
        if page is None:
            return None
        page_items = _extract_events_page(page)
        merged.extend(page_items)
        if len(page_items) < limit:
            return merged
        offset += limit
    typer.echo(
        f"events pagination for game {game_id} stopped after {_MAX_EVENT_PAGES} pages"
    )
    return merged


def _fetch_one_game(
    base_url: str,
    game_id: str,
    out_dir: Path,
    limit: int,
    headers: dict[str, str] | None,
    secret_values: list[str],
    force: bool,
) -> list[Path]:
    unified_path = out_dir / f"unified-plays_{game_id}.json"
    events_path = out_dir / f"events_{game_id}.json"

    if not force and unified_path.exists() and events_path.exists():
        return [unified_path, events_path]

    unified = http_get_json(
        f"{base_url}/games/{game_id}/unified-plays",
        headers=headers,
        secret_values=secret_values,
    )
    if unified is None:
        return []

    events = _fetch_events(base_url, game_id, limit, headers, secret_values)
    if events is None:
        return []

    _write_json(unified_path, unified)
    _write_json(events_path, events)
    return [unified_path, events_path]


def fetch_tournament(
    base_url: str,
    tournament: str,
    out_dir: Path,
    game_id: str | None = None,
    limit: int = 500,
    api_key: str | None = None,
    force: bool = False,
) -> list[Path]:
    """Discover and snapshot a cpx.studio tournament's plays, or one game.

    With `game_id=None`: fetches tournament metadata, teams and the full
    `/games` list, filters games to `tournament`, then snapshots
    unified-plays + paginated events per matched game. With an explicit
    `game_id`: skips discovery entirely and fetches only that game's two
    files.

    Every URL is built from `base_url` plus a fixed path — callers cannot
    redirect the fetch to another host. `api_key=None` sends no auth header;
    a set key is sent as `Authorization: Bearer` and redacted from all
    reported output.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    headers = _auth_headers(api_key)
    secret_values = [api_key] if api_key else []

    if game_id is not None:
        return _fetch_one_game(
            base_url, game_id, out_dir, limit, headers, secret_values, force
        )

    tournament_payload = http_get_json(
        f"{base_url}/tournaments/{tournament}", headers=headers, secret_values=secret_values
    )
    teams_payload = http_get_json(
        f"{base_url}/tournaments/{tournament}/teams",
        headers=headers,
        secret_values=secret_values,
    )
    games_payload = http_get_json(
        f"{base_url}/games", headers=headers, secret_values=secret_values
    )

    tournament_path = out_dir / f"tournament_{tournament}.json"
    teams_path = out_dir / f"tournament_{tournament}_teams.json"
    games_path = out_dir / "games.json"

    _write_json(tournament_path, tournament_payload)
    _write_json(teams_path, teams_payload)
    _write_json(games_path, games_payload)

    discovery_paths = [tournament_path, teams_path, games_path]

    games_list = _extract_games_list(games_payload)
    if games_list is None:
        top_level_keys = (
            list(games_payload.keys()) if isinstance(games_payload, dict) else []
        )
        typer.echo(f"unrecognized /games payload shape, top-level keys: {top_level_keys}")
        return discovery_paths

    matched = [g for g in games_list if _game_matches_tournament(g, tournament)]
    if not matched:
        typer.echo("0 games matched")
        return discovery_paths

    written = list(discovery_paths)
    for game in matched:
        gid = _game_id(game)
        if gid is None:
            continue
        written.extend(
            _fetch_one_game(base_url, gid, out_dir, limit, headers, secret_values, force)
        )

    return written
