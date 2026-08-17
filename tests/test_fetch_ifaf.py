"""Tests for flag_football_ep.fetch.ifaf.

`requests.get` is monkeypatched with a fake dispatching on URL path and
query params; no live network calls are made. `SENTINEL-KEY-DO-NOT-LEAK`
stands in for a cpx.studio bearer token to assert redaction.
"""

from __future__ import annotations

import json

import requests

from flag_football_ep.fetch import http as fetch_http
from flag_football_ep.fetch import ifaf

SENTINEL = "SENTINEL-KEY-DO-NOT-LEAK"
BASE = "https://example.test"


class FakeResponse:
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self._json_data = json_data
        self.text = "" if json_data is None else json.dumps(json_data)

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.HTTPError(f"{self.status_code} Error")
            err.response = self
            raise err

    def json(self):
        return self._json_data


def _install_dispatch(monkeypatch, handler):
    """Install a fake requests.get that delegates to `handler(url, params, headers)`."""

    def fake_get(url, params=None, headers=None, timeout=None):
        return handler(url, params, headers)

    monkeypatch.setattr(fetch_http.requests, "get", fake_get)


def test_two_game_tournament_writes_discovery_and_per_game_files(tmp_path, monkeypatch):
    games_payload = {
        "data": [
            {"id": "g1", "tournament": "ffwc26-women"},
            {"id": "g2", "tournament": "ffwc26-women"},
            {"id": "g3", "tournament": "other-tournament"},
        ]
    }

    def handler(url, params, headers):
        if url == f"{BASE}/tournaments/ffwc26-women":
            return FakeResponse(200, {"name": "WM 2026"})
        if url == f"{BASE}/tournaments/ffwc26-women/teams":
            return FakeResponse(200, {"teams": []})
        if url == f"{BASE}/games":
            return FakeResponse(200, games_payload)
        if url.endswith("/unified-plays"):
            return FakeResponse(200, [{"play": 1}])
        if url.endswith("/events"):
            return FakeResponse(200, [{"event": 1}])
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    result = ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path)

    expected_names = {
        "tournament_ffwc26-women.json",
        "tournament_ffwc26-women_teams.json",
        "games.json",
        "unified-plays_g1.json",
        "events_g1.json",
        "unified-plays_g2.json",
        "events_g2.json",
    }
    assert {p.name for p in result} == expected_names
    for name in expected_names:
        assert (tmp_path / name).exists()
    assert not (tmp_path / "unified-plays_g3.json").exists()


def test_zero_games_matched_returns_discovery_files_and_reports(tmp_path, monkeypatch, capsys):
    games_payload = {"data": [{"id": "g1", "tournament": "some-other-tournament"}]}

    def handler(url, params, headers):
        if url == f"{BASE}/tournaments/ffwc26-women":
            return FakeResponse(200, {"name": "WM 2026"})
        if url == f"{BASE}/tournaments/ffwc26-women/teams":
            return FakeResponse(200, {"teams": []})
        if url == f"{BASE}/games":
            return FakeResponse(200, games_payload)
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    result = ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path)
    captured = capsys.readouterr()

    assert {p.name for p in result} == {
        "tournament_ffwc26-women.json",
        "tournament_ffwc26-women_teams.json",
        "games.json",
    }
    assert "0 games matched" in captured.out


def test_unrecognized_games_shape_writes_games_json_and_does_not_raise(
    tmp_path, monkeypatch, capsys
):
    unexpected_payload = {"unexpectedKey": "unexpectedValue"}

    def handler(url, params, headers):
        if url == f"{BASE}/tournaments/ffwc26-women":
            return FakeResponse(200, {"name": "WM 2026"})
        if url == f"{BASE}/tournaments/ffwc26-women/teams":
            return FakeResponse(200, {"teams": []})
        if url == f"{BASE}/games":
            return FakeResponse(200, unexpected_payload)
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    result = ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path)
    captured = capsys.readouterr()

    assert json.loads((tmp_path / "games.json").read_text()) == unexpected_payload
    assert {p.name for p in result} == {
        "tournament_ffwc26-women.json",
        "tournament_ffwc26-women_teams.json",
        "games.json",
    }
    assert "unexpectedKey" in captured.out


def test_explicit_game_id_skips_discovery_and_fetches_only_that_game(tmp_path, monkeypatch):
    calls = []

    def handler(url, params, headers):
        calls.append(url)
        if url.endswith("/unified-plays"):
            return FakeResponse(200, [{"play": 1}])
        if url.endswith("/events"):
            return FakeResponse(200, [{"event": 1}])
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    result = ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path, game_id="g1")

    assert {p.name for p in result} == {"unified-plays_g1.json", "events_g1.json"}
    assert all("/tournaments/" not in c and c != f"{BASE}/games" for c in calls)


def test_events_pagination_merges_three_pages_and_stops_short(tmp_path, monkeypatch):
    pages = {
        0: [{"e": 1}, {"e": 2}],
        2: [{"e": 3}, {"e": 4}],
        4: [{"e": 5}],  # shorter than limit=2, pagination stops here
    }

    def handler(url, params, headers):
        if url.endswith("/unified-plays"):
            return FakeResponse(200, [{"play": 1}])
        if url.endswith("/events"):
            offset = params["offset"]
            return FakeResponse(200, pages[offset])
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path, game_id="g1", limit=2)

    events = json.loads((tmp_path / "events_g1.json").read_text())
    assert events == [{"e": 1}, {"e": 2}, {"e": 3}, {"e": 4}, {"e": 5}]


def test_events_pagination_stops_after_fifty_pages_with_warning(tmp_path, monkeypatch, capsys):
    def handler(url, params, headers):
        if url.endswith("/unified-plays"):
            return FakeResponse(200, [{"play": 1}])
        if url.endswith("/events"):
            # always a full page -> would loop forever without the hard cap
            return FakeResponse(200, [{"e": 1}, {"e": 2}])
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path, game_id="g1", limit=2)
    captured = capsys.readouterr()

    events = json.loads((tmp_path / "events_g1.json").read_text())
    assert len(events) == 50 * 2
    assert "50" in captured.out


def test_game_whose_unified_plays_fails_is_skipped_without_aborting(tmp_path, monkeypatch):
    games_payload = {
        "data": [
            {"id": "g1", "tournament": "ffwc26-women"},
            {"id": "g2", "tournament": "ffwc26-women"},
        ]
    }

    def handler(url, params, headers):
        if url == f"{BASE}/tournaments/ffwc26-women":
            return FakeResponse(200, {"name": "WM 2026"})
        if url == f"{BASE}/tournaments/ffwc26-women/teams":
            return FakeResponse(200, {"teams": []})
        if url == f"{BASE}/games":
            return FakeResponse(200, games_payload)
        if url == f"{BASE}/games/g1/unified-plays":
            return FakeResponse(500)
        if url.endswith("/unified-plays"):
            return FakeResponse(200, [{"play": 1}])
        if url.endswith("/events"):
            return FakeResponse(200, [{"event": 1}])
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    result = ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path)

    names = {p.name for p in result}
    assert "unified-plays_g1.json" not in names
    assert "events_g1.json" not in names
    assert not (tmp_path / "unified-plays_g1.json").exists()
    assert (tmp_path / "unified-plays_g2.json").exists()
    assert (tmp_path / "events_g2.json").exists()


def test_api_key_none_sends_no_authorization_header(tmp_path, monkeypatch):
    captured_headers = []

    def handler(url, params, headers):
        captured_headers.append(headers)
        if url.endswith("/unified-plays"):
            return FakeResponse(200, [{"play": 1}])
        if url.endswith("/events"):
            return FakeResponse(200, [{"event": 1}])
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path, game_id="g1", api_key=None)

    assert all(h is None for h in captured_headers)


def test_api_key_set_sends_bearer_header_and_is_redacted(tmp_path, monkeypatch, capsys):
    captured_headers = []

    def handler(url, params, headers):
        captured_headers.append(headers)
        if url.endswith("/unified-plays"):
            return FakeResponse(500)
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path, game_id="g1", api_key=SENTINEL)
    captured = capsys.readouterr()

    assert captured_headers[0] == {"Authorization": f"Bearer {SENTINEL}"}
    assert SENTINEL not in captured.out


def test_force_false_skips_games_whose_files_already_exist(tmp_path, monkeypatch):
    (tmp_path / "unified-plays_g1.json").write_text(json.dumps({"cached": True}))
    (tmp_path / "events_g1.json").write_text(json.dumps({"cached": True}))

    def fail_if_called(url, params, headers):
        raise AssertionError("requests.get should not be called when force=False")

    _install_dispatch(monkeypatch, fail_if_called)

    result = ifaf.fetch_tournament(
        BASE, "ffwc26-women", tmp_path, game_id="g1", force=False
    )

    assert {p.name for p in result} == {"unified-plays_g1.json", "events_g1.json"}
    assert json.loads((tmp_path / "unified-plays_g1.json").read_text()) == {"cached": True}


def test_no_leftover_tmp_files_after_successful_run(tmp_path, monkeypatch):
    def handler(url, params, headers):
        if url.endswith("/unified-plays"):
            return FakeResponse(200, [{"play": 1}])
        if url.endswith("/events"):
            return FakeResponse(200, [{"event": 1}])
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path, game_id="g1")

    assert list(tmp_path.glob("*.tmp")) == []
