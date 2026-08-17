"""Tests for flag_football_ep.fetch.http and flag_football_ep.fetch.sportapp.

`requests.get` is monkeypatched with fake response objects; no live network
calls are made. `SENTINEL-KEY-DO-NOT-LEAK` stands in for the sportapp.fi
apikey to assert it never reaches stdout or any written artifact.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests

from flag_football_ep.fetch import http as fetch_http
from flag_football_ep.fetch import sportapp

SENTINEL = "SENTINEL-KEY-DO-NOT-LEAK"


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, json_error=False):
        self.status_code = status_code
        self._json_data = json_data
        self._json_error = json_error
        self.text = "" if json_data is None else json.dumps(json_data)

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.HTTPError(f"{self.status_code} Error")
            err.response = self
            raise err

    def json(self):
        if self._json_error:
            raise requests.JSONDecodeError("Expecting value", "not json", 0)
        return self._json_data


# --- http_get_json / redact -------------------------------------------------


def test_http_get_json_passes_timeout_on_every_call(monkeypatch):
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(timeout)
        return FakeResponse(200, {"ok": True})

    monkeypatch.setattr(fetch_http.requests, "get", fake_get)
    fetch_http.http_get_json("https://example.test/x")

    assert calls == [fetch_http.DEFAULT_TIMEOUT]
    assert calls[0] is not None


def test_http_get_json_returns_parsed_json_on_200(monkeypatch):
    monkeypatch.setattr(
        fetch_http.requests, "get", lambda *a, **k: FakeResponse(200, {"a": 1})
    )
    result = fetch_http.http_get_json("https://example.test/x")
    assert result == {"a": 1}


def test_http_get_json_on_500_returns_none_and_reports(monkeypatch, capsys):
    monkeypatch.setattr(fetch_http.requests, "get", lambda *a, **k: FakeResponse(500))
    result = fetch_http.http_get_json("https://example.test/x")
    captured = capsys.readouterr()
    assert result is None
    assert "500" in captured.out
    assert "https://example.test/x" in captured.out


def test_http_get_json_on_non_json_returns_none_and_reports(monkeypatch, capsys):
    monkeypatch.setattr(
        fetch_http.requests, "get", lambda *a, **k: FakeResponse(200, json_error=True)
    )
    result = fetch_http.http_get_json("https://example.test/x")
    captured = capsys.readouterr()
    assert result is None
    assert "not JSON" in captured.out
    assert "https://example.test/x" in captured.out


def test_http_get_json_on_request_exception_returns_none_and_reports(monkeypatch, capsys):
    def fake_get(*a, **k):
        raise requests.ConnectionError("boom")

    monkeypatch.setattr(fetch_http.requests, "get", fake_get)
    result = fetch_http.http_get_json("https://example.test/x", secret_values=[SENTINEL])
    captured = capsys.readouterr()
    assert result is None
    assert "boom" in captured.out


def test_redact_replaces_every_occurrence_of_each_secret():
    text = f"url?apikey={SENTINEL}&other={SENTINEL}"
    assert fetch_http.redact(text, [SENTINEL]) == "url?apikey=***&other=***"


def test_redact_returns_unchanged_when_no_secrets():
    text = "nothing secret here"
    assert fetch_http.redact(text, []) == text


def test_sentinel_key_never_appears_in_stdout_on_request_exception(monkeypatch, capsys):
    def fake_get(*a, **k):
        raise requests.ConnectionError(f"failed for url with apikey={SENTINEL}")

    monkeypatch.setattr(fetch_http.requests, "get", fake_get)
    fetch_http.http_get_json("https://example.test/x", secret_values=[SENTINEL])
    captured = capsys.readouterr()
    assert SENTINEL not in captured.out


# --- fetch_games -------------------------------------------------------------


def _make_dispatch(fail_endpoints: set[tuple[str, str]] | None = None):
    """Build a fake requests.get dispatching on endpoint path + game id."""
    fail_endpoints = fail_endpoints or set()

    def fake_get(url, params=None, headers=None, timeout=None):
        game_id = params["id"]
        endpoint = url.rsplit("/", 1)[-1]
        if (endpoint, game_id) in fail_endpoints:
            return FakeResponse(500)
        return FakeResponse(200, {"endpoint": endpoint, "game_id": game_id})

    return fake_get


def test_fetch_games_writes_four_files_and_returns_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch_http.requests, "get", _make_dispatch())

    result = sportapp.fetch_games(
        ["991", "992"], tmp_path, api_key=SENTINEL, base_url="https://example.test"
    )

    expected_names = {
        "match-drives_991.json",
        "match-v1_991.json",
        "match-drives_992.json",
        "match-v1_992.json",
    }
    assert {p.name for p in result} == expected_names
    for path in result:
        assert path.exists()
        data = json.loads(path.read_text())
        assert SENTINEL not in path.read_text()
        assert data["game_id"] in ("991", "992")


def test_fetch_games_skips_game_with_failing_match_drives(tmp_path, monkeypatch):
    monkeypatch.setattr(
        fetch_http.requests, "get", _make_dispatch({("match-drives", "991")})
    )

    result = sportapp.fetch_games(
        ["991", "992"], tmp_path, api_key=SENTINEL, base_url="https://example.test"
    )

    result_names = {p.name for p in result}
    assert "match-drives_991.json" not in result_names
    assert "match-v1_991.json" not in result_names
    assert not (tmp_path / "match-drives_991.json").exists()
    assert not (tmp_path / "match-drives_991.json.tmp").exists()
    assert not (tmp_path / "match-v1_991.json").exists()
    assert (tmp_path / "match-drives_992.json").exists()
    assert (tmp_path / "match-v1_992.json").exists()


def test_fetch_games_force_false_skips_existing_files(tmp_path, monkeypatch):
    for name in ("match-drives_991.json", "match-v1_991.json"):
        (tmp_path / name).write_text(json.dumps({"cached": True}))

    def fail_if_called(*a, **k):
        raise AssertionError("requests.get should not be called when force=False")

    monkeypatch.setattr(fetch_http.requests, "get", fail_if_called)

    result = sportapp.fetch_games(
        ["991"], tmp_path, api_key=SENTINEL, base_url="https://example.test", force=False
    )

    assert {p.name for p in result} == {"match-drives_991.json", "match-v1_991.json"}
    assert json.loads((tmp_path / "match-drives_991.json").read_text()) == {"cached": True}


def test_fetch_games_force_true_redownloads_and_overwrites(tmp_path, monkeypatch):
    for name in ("match-drives_991.json", "match-v1_991.json"):
        (tmp_path / name).write_text(json.dumps({"cached": True}))

    monkeypatch.setattr(fetch_http.requests, "get", _make_dispatch())

    sportapp.fetch_games(
        ["991"], tmp_path, api_key=SENTINEL, base_url="https://example.test", force=True
    )

    data = json.loads((tmp_path / "match-drives_991.json").read_text())
    assert data != {"cached": True}
    assert data["game_id"] == "991"


def test_fetch_games_no_sentinel_key_in_written_files_or_stdout(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        fetch_http.requests, "get", _make_dispatch({("match-drives", "991")})
    )

    sportapp.fetch_games(
        ["991", "992"], tmp_path, api_key=SENTINEL, base_url="https://example.test"
    )

    captured = capsys.readouterr()
    assert SENTINEL not in captured.out
    for path in tmp_path.glob("*.json"):
        assert SENTINEL not in path.read_text()
