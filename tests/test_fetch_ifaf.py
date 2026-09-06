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


def test_events_pagination_handles_real_events_wrapper_shape(tmp_path, monkeypatch):
    """Regression test: live verification (2026-08-17) found the real
    /events response is `{"events": [...], "total": N, "hasMore": bool}`,
    not a bare list or an `items`-keyed object."""
    pages = {
        0: {"events": [{"e": 1}, {"e": 2}], "total": 3, "hasMore": True},
        2: {"events": [{"e": 3}], "total": 3, "hasMore": False},
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
    assert events == [{"e": 1}, {"e": 2}, {"e": 3}]


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


# ---------------------------------------------------------------------------
# redact_pii
# ---------------------------------------------------------------------------


def test_redact_pii_nulls_known_keys_keeps_key_present():
    payload = {
        "playId": "p1",
        "lastEditedBy": "uid-123",
        "lastEditedByEmail": "someone@example.com",
        "reviewedBy": "uid-456",
        "reviewedByEmail": "reviewer@example.com",
        "recordedByUserId": "6mrVht8t3DXG6rALs0pczOwFSY12",
        "videoMark": {"videoUrl": "https://example.test/v.mp4", "videoTimeSec": 12.3},
    }
    result = ifaf.redact_pii(payload)
    assert result["playId"] == "p1"
    assert result["lastEditedBy"] is None
    assert result["lastEditedByEmail"] is None
    assert result["reviewedBy"] is None
    assert result["reviewedByEmail"] is None
    assert result["recordedByUserId"] is None
    assert "lastEditedBy" in result  # key kept, not deleted
    assert result["videoMark"] == {
        "videoUrl": "https://example.test/v.mp4",
        "videoTimeSec": 12.3,
    }


def test_redact_pii_handles_nested_lists_and_unknown_email_useridsuffix_keys():
    payload = {
        "events": [
            {"action": "PENALTY", "adminEmail": "x@y.test", "ownerUserId": "abc"},
            {"action": "SNAP"},
        ]
    }
    result = ifaf.redact_pii(payload)
    assert result["events"][0]["adminEmail"] is None
    assert result["events"][0]["ownerUserId"] is None
    assert result["events"][0]["action"] == "PENALTY"
    assert result["events"][1] == {"action": "SNAP"}


def test_write_json_applies_redaction_before_writing(tmp_path):
    path = tmp_path / "plays_g1.json"
    ifaf._write_json(path, {"reviewedByEmail": "leak@example.com", "playId": "p1"})
    written = json.loads(path.read_text())
    assert written["reviewedByEmail"] is None
    assert written["playId"] == "p1"


# ---------------------------------------------------------------------------
# include_full=True: /games/{id} and /games/{id}/plays
# ---------------------------------------------------------------------------


def test_include_full_fetches_game_and_plays_and_redacts_them(tmp_path, monkeypatch):
    def handler(url, params, headers):
        if url.endswith("/unified-plays"):
            return FakeResponse(200, [{"play": 1}])
        if url.endswith("/events"):
            return FakeResponse(200, [{"event": 1}])
        if url == f"{BASE}/games/g1":
            return FakeResponse(200, {"id": "g1", "homeTeamStats": {}})
        if url == f"{BASE}/games/g1/plays":
            return FakeResponse(
                200, {"plays": [{"playId": "p1", "reviewedByEmail": "leak@example.com"}]}
            )
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    result = ifaf.fetch_tournament(
        BASE, "ffwc26-women", tmp_path, game_id="g1", include_full=True
    )

    names = {p.name for p in result}
    assert names == {
        "unified-plays_g1.json",
        "events_g1.json",
        "game_g1.json",
        "plays_g1.json",
    }
    plays_written = json.loads((tmp_path / "plays_g1.json").read_text())
    assert plays_written["plays"][0]["reviewedByEmail"] is None
    assert plays_written["plays"][0]["playId"] == "p1"


def test_include_full_false_never_calls_game_or_plays_endpoints(tmp_path, monkeypatch):
    """Default behavior (include_full=False) must be identical to the
    original two-file contract — no new endpoint is ever hit."""
    calls = []

    def handler(url, params, headers):
        calls.append(url)
        if url.endswith("/unified-plays"):
            return FakeResponse(200, [{"play": 1}])
        if url.endswith("/events"):
            return FakeResponse(200, [{"event": 1}])
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path, game_id="g1")

    assert not any(c.endswith("/games/g1") or c.endswith("/plays") for c in calls)


def test_all_games_true_ignores_tournament_filter(tmp_path, monkeypatch):
    games_payload = {
        "data": [
            {"id": "g1", "tournament": "ffwc26-women"},
            {"id": "g2", "tournament": "ffwc26-men"},
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

    result = ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path, all_games=True)

    names = {p.name for p in result}
    assert "unified-plays_g1.json" in names
    assert "unified-plays_g2.json" in names


def test_all_games_true_also_fetches_other_tournament_metadata(tmp_path, monkeypatch):
    """Regression: a men's-bracket game processed via all_games=True must not
    silently ingest with competition=season=gender=None -- its own
    tournamentId's metadata must be fetched too, not just the primary
    `tournament` argument's."""
    games_payload = {
        "data": [
            {"id": "g1", "tournamentId": "ffwc26-women"},
            {"id": "g2", "tournamentId": "ffwc26-men"},
        ]
    }
    calls = []

    def handler(url, params, headers):
        calls.append(url)
        if url == f"{BASE}/tournaments/ffwc26-women":
            return FakeResponse(200, {"name": "WM 2026 Women"})
        if url == f"{BASE}/tournaments/ffwc26-women/teams":
            return FakeResponse(200, {"teams": []})
        if url == f"{BASE}/tournaments/ffwc26-men":
            return FakeResponse(200, {"name": "WM 2026 Men"})
        if url == f"{BASE}/tournaments/ffwc26-men/teams":
            return FakeResponse(200, {"teams": []})
        if url == f"{BASE}/games":
            return FakeResponse(200, games_payload)
        if url.endswith("/unified-plays"):
            return FakeResponse(200, [{"play": 1}])
        if url.endswith("/events"):
            return FakeResponse(200, [{"event": 1}])
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    result = ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path, all_games=True)

    assert f"{BASE}/tournaments/ffwc26-men" in calls
    assert f"{BASE}/tournaments/ffwc26-men/teams" in calls
    names = {p.name for p in result}
    assert "tournament_ffwc26-men.json" in names
    assert "tournament_ffwc26-men_teams.json" in names
    written = json.loads((tmp_path / "tournament_ffwc26-men.json").read_text())
    assert written == {"name": "WM 2026 Men"}


def test_max_retries_retries_on_429_then_succeeds(tmp_path, monkeypatch):
    calls = {"unified-plays": 0}
    sleeps = []
    monkeypatch.setattr(ifaf.time, "sleep", lambda s: sleeps.append(s))

    def handler(url, params, headers):
        if url.endswith("/unified-plays"):
            calls["unified-plays"] += 1
            if calls["unified-plays"] < 2:
                return FakeResponse(429)
            return FakeResponse(200, [{"play": 1}])
        if url.endswith("/events"):
            return FakeResponse(200, [{"event": 1}])
        raise AssertionError(f"unexpected url {url}")

    _install_dispatch(monkeypatch, handler)

    result = ifaf.fetch_tournament(
        BASE, "ffwc26-women", tmp_path, game_id="g1", max_retries=3, retry_backoff=0.01
    )

    assert calls["unified-plays"] == 2
    assert {p.name for p in result} == {"unified-plays_g1.json", "events_g1.json"}


def test_pause_sec_sleeps_between_games_not_before_the_first(tmp_path, monkeypatch):
    games_payload = {
        "data": [
            {"id": "g1", "tournament": "ffwc26-women"},
            {"id": "g2", "tournament": "ffwc26-women"},
        ]
    }
    sleeps = []
    monkeypatch.setattr(ifaf.time, "sleep", lambda s: sleeps.append(s))

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

    ifaf.fetch_tournament(BASE, "ffwc26-women", tmp_path, pause_sec=0.5)

    assert sleeps == [0.5]


# ---------------------------------------------------------------------------
# probe_extra_endpoints
# ---------------------------------------------------------------------------


def test_probe_extra_endpoints_reports_status_only_no_writes(tmp_path, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        return FakeResponse(200 if "teams" in url else 404)

    monkeypatch.setattr(requests, "get", fake_get)

    result = ifaf.probe_extra_endpoints(
        BASE, "ffwc26-women", game_id="g1", team_id="w-can"
    )

    assert result["/teams/w-can"] == 200
    assert result["/tournaments"] == 404
    assert list(tmp_path.iterdir()) == []
