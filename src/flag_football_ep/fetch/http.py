"""Single HTTP wrapper for every fetch-stage request.

`fetch/sportapp.py` and `fetch/ifaf.py` both call `http_get_json` instead of
`requests.get` directly, so timeout enforcement, status handling and secret
redaction live in exactly one place. A single bad game/request must never
abort a multi-game run: every failure mode reports through `typer.echo` and
returns `None` rather than raising.

Never logs response bodies — only status codes and redacted URLs/messages.
"""

from __future__ import annotations

import time
from typing import Sequence

import requests
import typer

DEFAULT_TIMEOUT = 30.0
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


def redact(text: str, secret_values: Sequence[str] = ()) -> str:
    """Replace every occurrence of each secret value in `text` with `***`.

    Returns `text` unchanged when `secret_values` is empty.
    """
    if not secret_values:
        return text
    redacted = text
    for secret in secret_values:
        if secret:
            redacted = redacted.replace(secret, "***")
    return redacted


def http_get_json(
    url: str,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    secret_values: Sequence[str] = (),
    max_retries: int = 0,
    retry_backoff: float = 1.0,
) -> dict | list | None:
    """GET `url` and return parsed JSON, or `None` after reporting the failure.

    Rules:
    - always passes `timeout` to `requests.get` — no unbounded request.
    - a non-2xx status is reported (status code + redacted URL) and returns `None`.
    - a 200 response whose body is not JSON is reported ("not JSON" + redacted
      URL) and returns `None`.
    - a `requests.RequestException` (connection error, timeout, etc.) is
      reported (redacted error text) and returns `None`.
    - never raises for any of the above — a single bad game must never abort
      a multi-game run.

    `max_retries` (default 0, i.e. the original no-retry behavior — existing
    callers are unaffected) retries only on a 429 or 5xx status
    (`_RETRYABLE_STATUSES`), sleeping `retry_backoff * attempt` seconds between
    attempts. A non-retryable 4xx, a JSON decode failure, or a connection
    error is never retried — those are not transient.
    """
    attempt = 0
    while True:
        try:
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            if isinstance(status, int) and status in _RETRYABLE_STATUSES and attempt < max_retries:
                attempt += 1
                typer.echo(
                    redact(
                        f"HTTP {status} for {url} — retry {attempt}/{max_retries}", secret_values
                    )
                )
                time.sleep(retry_backoff * attempt)
                continue
            typer.echo(redact(f"HTTP {status} for {url}", secret_values))
            return None
        except requests.RequestException as exc:
            typer.echo(redact(f"request failed for {url}: {exc}", secret_values))
            return None

        try:
            return response.json()
        except requests.JSONDecodeError:
            typer.echo(redact(f"response not JSON for {url}", secret_values))
            return None
