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
import time
from pathlib import Path
from typing import Any

import typer

from flag_football_ep.fetch.http import http_get_json

_MAX_EVENT_PAGES = 50
_GAMES_LIST_KEYS = ("data", "games", "items")
_TOURNAMENT_MATCH_KEYS = ("tournament", "tournamentId", "tournamentSlug")

# Person-identifying keys observed live on `/games/{id}/plays` and
# `/games/{id}/events` (2026-09-06 probe): reviewer/editor identity fields and
# the events feed's `recordedByUserId` (a Firebase-style uid — confirmed live,
# not just the literal string "venue-console"). Any key ending in `Email` or
# `UserId` is redacted defensively too, in case a future endpoint adds a new
# person-identifying field with the same suffix convention. `videoMark` and
# its nested `videoUrl`/`videoTimeSec` are deliberately never touched — they
# name a video asset, not a person.
_PII_KEYS_EXACT = {
    "lastEditedBy",
    "lastEditedByEmail",
    "reviewedBy",
    "reviewedByEmail",
    "recordedByUserId",
}
_PII_KEY_SUFFIXES = ("Email", "UserId")


def _is_pii_key(key: str) -> bool:
    if key in _PII_KEYS_EXACT:
        return True
    return any(key.endswith(suffix) for suffix in _PII_KEY_SUFFIXES)


def redact_pii(payload: Any) -> Any:
    """Recursively null every person-identifying field in `payload`.

    Keeps the key present (set to `None`) rather than deleting it, so the
    payload shape stays diffable against an unredacted response. Leaves every
    other key (including `videoMark`/`videoUrl`/`videoTimeSec`) untouched.
    Player names inside `sequence`/`description`/`players` are NOT redacted —
    snapshots live only under the gitignored `data/raw/` tree, never
    committed; only emails and user ids are redacted at the fetch boundary.
    """
    if isinstance(payload, dict):
        return {
            key: (None if _is_pii_key(key) else redact_pii(value))
            for key, value in payload.items()
        }
    if isinstance(payload, list):
        return [redact_pii(item) for item in payload]
    return payload


def _write_json(path: Path, payload: object) -> None:
    """Atomically write `payload` as JSON to `path` (`.tmp` then `os.replace`).

    Applies `redact_pii` first — this is the single choke point for every
    snapshot write in this module, so a new call site can never accidentally
    skip redaction.
    """
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(redact_pii(payload), f, indent=2, ensure_ascii=False)
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


def _fetch_game_detail(
    base_url: str,
    game_id: str,
    headers: dict[str, str] | None,
    secret_values: list[str],
    max_retries: int,
    retry_backoff: float,
) -> dict | list | None:
    """`GET /games/{id}` — full game document (rosters, player/team stat
    aggregates, current context). Never fetched before the 2026-09-06 probe."""
    return http_get_json(
        f"{base_url}/games/{game_id}",
        headers=headers,
        secret_values=secret_values,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
    )


def _fetch_game_plays(
    base_url: str,
    game_id: str,
    headers: dict[str, str] | None,
    secret_values: list[str],
    max_retries: int,
    retry_backoff: float,
) -> dict | list | None:
    """`GET /games/{id}/plays` — the reviewer-facing per-play feed: `ballOn`,
    `down`, `half`, `offenseTeamId`, `events[]` (action/penaltyType/player),
    `videoMark`, `nullified`, `officialScore`. Carries PII
    (`lastEditedBy(Email)`, `reviewedBy(Email)`) — always redacted by
    `_write_json` before this reaches disk."""
    return http_get_json(
        f"{base_url}/games/{game_id}/plays",
        headers=headers,
        secret_values=secret_values,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
    )


def _fetch_one_game(
    base_url: str,
    game_id: str,
    out_dir: Path,
    limit: int,
    headers: dict[str, str] | None,
    secret_values: list[str],
    force: bool,
    include_full: bool = False,
    max_retries: int = 0,
    retry_backoff: float = 1.0,
) -> list[Path]:
    unified_path = out_dir / f"unified-plays_{game_id}.json"
    events_path = out_dir / f"events_{game_id}.json"
    game_path = out_dir / f"game_{game_id}.json"
    plays_path = out_dir / f"plays_{game_id}.json"

    expected = [unified_path, events_path]
    if include_full:
        expected += [game_path, plays_path]

    if not force and all(p.exists() for p in expected):
        return expected

    unified = http_get_json(
        f"{base_url}/games/{game_id}/unified-plays",
        headers=headers,
        secret_values=secret_values,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
    )
    if unified is None:
        return []

    events = _fetch_events(base_url, game_id, limit, headers, secret_values)
    if events is None:
        return []

    written = [unified_path, events_path]
    _write_json(unified_path, unified)
    _write_json(events_path, events)

    if include_full:
        game_detail = _fetch_game_detail(
            base_url, game_id, headers, secret_values, max_retries, retry_backoff
        )
        if game_detail is not None:
            _write_json(game_path, game_detail)
            written.append(game_path)

        plays = _fetch_game_plays(
            base_url, game_id, headers, secret_values, max_retries, retry_backoff
        )
        if plays is not None:
            _write_json(plays_path, plays)
            written.append(plays_path)

    return written


def fetch_tournament(
    base_url: str,
    tournament: str,
    out_dir: Path,
    game_id: str | None = None,
    limit: int = 500,
    api_key: str | None = None,
    force: bool = False,
    all_games: bool = False,
    include_full: bool = False,
    pause_sec: float = 0.0,
    max_retries: int = 0,
    retry_backoff: float = 1.0,
) -> list[Path]:
    """Discover and snapshot a cpx.studio tournament's plays, or one game.

    With `game_id=None`: fetches tournament metadata, teams and the full
    `/games` list, filters games to `tournament` (unless `all_games=True`,
    which processes every game in the `/games` payload regardless of its
    `tournamentId` — used to snapshot every tournament the API exposes, e.g.
    `ffwc26-women` and `ffwc26-men`, from one `/games` response), then
    snapshots unified-plays + paginated events per matched game. With an
    explicit `game_id`: skips discovery entirely and fetches only that
    game's files.

    `include_full=True` additionally fetches `/games/{id}` and
    `/games/{id}/plays` per game (written as `game_{id}.json` /
    `plays_{id}.json`); default `False` preserves the original two-file
    contract exactly (existing callers/tests are unaffected).

    `pause_sec` sleeps between games (politeness — default 0.0, no change to
    existing timing). `max_retries`/`retry_backoff` (default 0 / 1.0 — no
    retries, matching the original behavior) retry once per attempt on a
    429/5xx response; see `http.http_get_json`.

    Every URL is built from `base_url` plus a fixed path — callers cannot
    redirect the fetch to another host. `api_key=None` sends no auth header;
    a set key is sent as `Authorization: Bearer` and redacted from all
    reported output. Every written file is passed through `redact_pii`
    (`_write_json`'s single choke point) before it touches disk.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    headers = _auth_headers(api_key)
    secret_values = [api_key] if api_key else []

    if game_id is not None:
        return _fetch_one_game(
            base_url,
            game_id,
            out_dir,
            limit,
            headers,
            secret_values,
            force,
            include_full=include_full,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
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

    if all_games:
        matched = games_list
    else:
        matched = [g for g in games_list if _game_matches_tournament(g, tournament)]
    if not matched:
        typer.echo("0 games matched")
        return discovery_paths

    written = list(discovery_paths)
    for i, game in enumerate(matched):
        gid = _game_id(game)
        if gid is None:
            continue
        if i > 0 and pause_sec > 0:
            time.sleep(pause_sec)
        written.extend(
            _fetch_one_game(
                base_url,
                gid,
                out_dir,
                limit,
                headers,
                secret_values,
                force,
                include_full=include_full,
                max_retries=max_retries,
                retry_backoff=retry_backoff,
            )
        )

    return written


# Read-only endpoints plausible from the API's URL shape but never confirmed
# live before the 2026-09-06 probe. Status-only — no body is parsed or
# written; this never becomes part of the snapshot corpus. `{game_id}` and
# `{team_id}` are substituted with one real id from the already-fetched
# `/games` list so the probe reflects an actual resource, not a 404 by
# construction.
_PROBE_PATH_TEMPLATES = (
    "/tournaments",
    "/tournaments/{tournament}/games",
    "/games/{game_id}/unified-plays?includeSequence=true",
    "/teams/{team_id}",
    "/players?teamId={team_id}",
)


def probe_extra_endpoints(
    base_url: str,
    tournament: str,
    game_id: str,
    team_id: str,
    api_key: str | None = None,
) -> dict[str, int | str]:
    """One-shot, read-only status probe of a handful of plausible endpoints.

    Reports only the HTTP status per path (`requests.get`, `raise_for_status`
    skipped deliberately so a 404 is captured as a status code rather than
    triggering `http_get_json`'s error-echo path) — no body is ever parsed or
    written to disk. No auth beyond the same optional bearer token every
    other call in this module uses.
    """
    import requests

    headers = _auth_headers(api_key)
    secret_values = [api_key] if api_key else []
    results: dict[str, int | str] = {}
    for template in _PROBE_PATH_TEMPLATES:
        path = template.format(tournament=tournament, game_id=game_id, team_id=team_id)
        url = f"{base_url}{path}"
        try:
            response = requests.get(url, headers=headers, timeout=30.0)
            results[path] = response.status_code
        except requests.RequestException as exc:
            from flag_football_ep.fetch.http import redact

            results[path] = redact(f"error: {exc}", secret_values)
    return results
