"""sportapp.fi raw JSON snapshot fetch.

Snapshot-first: downloads `match-drives` and `match-v1` payloads verbatim to
`data/raw/sportapp/` and stops. No dataframe parsing, no mutation — that is
`ingest/sportapp.py`'s job (a later plan). Keeping this module free of
transform logic keeps network I/O testable/mockable in isolation from
parsing.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Sequence

from flag_football_ep.fetch.http import http_get_json

_ENDPOINTS = ("match-drives", "match-v1")


def _write_json(path: Path, payload: object) -> None:
    """Atomically write `payload` as JSON to `path` (`.tmp` then `os.replace`)."""
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, path)


def fetch_games(
    game_ids: Sequence[str],
    out_dir: Path,
    api_key: str,
    base_url: str,
    force: bool = False,
) -> list[Path]:
    """Fetch match-drives and match-v1 for each game id, snapshot to `out_dir`.

    Writes `match-drives_{game_id}.json` and `match-v1_{game_id}.json` per
    game and returns the paths actually written. A game whose match-drives
    (or match-v1) request fails is skipped entirely: no partial file is left
    behind for that game, and the remaining games still download. With
    `force=False`, a game whose two files already exist is skipped without a
    request; `force=True` always re-downloads and overwrites.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for game_id in game_ids:
        targets = {
            endpoint: out_dir / f"{endpoint}_{game_id}.json" for endpoint in _ENDPOINTS
        }

        if not force and all(p.exists() for p in targets.values()):
            written.extend(targets.values())
            continue

        payloads: dict[str, object] = {}
        failed = False
        for endpoint, path in targets.items():
            payload = http_get_json(
                f"{base_url}/{endpoint}",
                params={"id": game_id, "apikey": api_key},
                secret_values=[api_key],
            )
            if payload is None:
                failed = True
                break
            payloads[endpoint] = payload

        if failed:
            continue

        for endpoint, path in targets.items():
            _write_json(path, payloads[endpoint])
            written.append(path)

    return written
