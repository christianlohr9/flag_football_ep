"""Unit coverage for the grandfathered `data_raw.csv` ingest path (phase 01.2 plan 10).

Covers the notebook-identical semantics frozen in `ingest/legacy.py`: the
first-drive-is-home heuristic, the RESULT substring quirks, offense-only
scoring credit, and the warn-only validation wiring that never quarantines
a legacy game.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

from flag_football_ep.canonical import CANONICAL_COLUMNS, make_game_id
from flag_football_ep.ingest.legacy import (
    IngestNotices,
    get_games,
    ingest_legacy,
    legacy_mutations,
    read_legacy,
)
from flag_football_ep.reference import UnmappedTeamError
from flag_football_ep.testing import canonical_plays, canonical_plays_with_scores
from flag_football_ep.validation.checks import Status, partition_games, run_checks

# Mirrors data_raw.csv's real header (verified 2026-08-17), `;`-delimited.
LEGACY_COLUMNS = [
    "PLAY #",
    "ODK",
    "OFF FORM",
    "Off Str",
    "OFF PLAY",
    "DN",
    "DIST",
    "YARD LN",
    "RESULT",
    "Drive Success",
    "TARGET ROUTE",
    "RECEIVED BY",
    "GN/LS",
    "Thrown By",
    "YAC",
    "QB",
    "C",
    "X/H",
    "Y/CAT",
    "Z",
    "Target",
    "Drop",
    "B",
    "yardline_50",
    "game_id",
    "play_id",
    "drive_id",
    "half",
    "posteam",
]


def _write_legacy_csv(path: Path, rows: list[dict]) -> Path:
    """Write a `;`-delimited, utf-8-sig legacy-shaped CSV (mirrors data_raw.csv)."""
    lines = [";".join(LEGACY_COLUMNS)]
    for row in rows:
        lines.append(";".join(str(row.get(c, "")) for c in LEGACY_COLUMNS))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


def _row(**kwargs) -> dict:
    """A minimal legacy row with sane defaults, overridden by kwargs."""
    base = {
        "PLAY #": kwargs.get("play_id", 1),
        "ODK": "Slot Left",  # polluted charting text, not O/D/K — deliberately unused
        "DN": 1,
        "DIST": 10,
        "YARD LN": 10,
        "RESULT": "Rush",
        "yardline_50": 10,
        "game_id": 1,
        "play_id": 1,
        "drive_id": 1,
        "half": 1,
        "posteam": "GER",
    }
    base.update(kwargs)
    return base


class TestGetGames:
    def test_home_away_from_first_drive_order(self, tmp_path: Path):
        rows = [
            _row(play_id=1, game_id=1, posteam="GER"),
            _row(play_id=2, game_id=1, posteam="GER"),
            _row(play_id=3, game_id=1, posteam="OPP"),
            _row(play_id=4, game_id=1, posteam="OPP"),
        ]
        path = _write_legacy_csv(tmp_path / "legacy.csv", rows)
        raw = read_legacy(path)

        games = get_games(raw)

        assert games.height == 1
        record = games.row(0, named=True)
        assert record["home_team"] == "GER"
        assert record["away_team"] == "OPP"

    def test_raises_on_three_distinct_posteam_values(self, tmp_path: Path):
        rows = [
            _row(play_id=1, game_id=2, posteam="A"),
            _row(play_id=2, game_id=2, posteam="B"),
            _row(play_id=3, game_id=2, posteam="C"),
        ]
        path = _write_legacy_csv(tmp_path / "legacy.csv", rows)
        raw = read_legacy(path)

        with pytest.raises(ValueError, match="2"):
            get_games(raw)


class TestLegacyMutationsResultSemantics:
    def test_complete_penalty_no_comma_classifies_as_complete_pass(self, tmp_path: Path):
        rows = [_row(play_id=1, game_id=1, posteam="GER", RESULT="Complete Penalty")]
        path = _write_legacy_csv(tmp_path / "legacy.csv", rows)
        raw = read_legacy(path)

        out = legacy_mutations(raw)

        assert out["play_type"][0] == "no_play"
        assert out["complete_pass"][0] == 1

    def test_penalty_declined_classifies_as_no_play(self, tmp_path: Path):
        rows = [_row(play_id=1, game_id=1, posteam="GER", RESULT="Penalty (declined)")]
        path = _write_legacy_csv(tmp_path / "legacy.csv", rows)
        raw = read_legacy(path)

        out = legacy_mutations(raw)

        assert out["play_type"][0] == "no_play"

    def test_offense_only_scoring_credit_never_credits_defense(self, tmp_path: Path):
        rows = [
            _row(play_id=1, game_id=1, posteam="GER", RESULT="Interception, Def TD", DN=2, DIST=5)
        ]
        path = _write_legacy_csv(tmp_path / "legacy.csv", rows)
        raw = read_legacy(path)

        out = legacy_mutations(raw)

        assert out["def_touchdown"][0] == 1
        assert out["scoring_play"][0] == 1
        # credit_defense=False: never credited to any team, matching the
        # notebook's original offense-only scoring_play_team formula.
        assert out["scoring_play_team"][0] is None


class TestIngestLegacyIdentity:
    def test_game_id_pattern_and_source_game_id_preserved(self, tmp_path: Path):
        rows = [
            _row(play_id=1, game_id=5, posteam="GER"),
            _row(play_id=2, game_id=5, posteam="OPP"),
        ]
        path = _write_legacy_csv(tmp_path / "legacy.csv", rows)

        df, _notices = ingest_legacy(path)

        assert df["game_id"].unique().to_list() == ["legacy-5"]
        assert df["source_game_id"].unique().to_list() == ["5"]
        assert make_game_id("legacy", "5") == "legacy-5"

    def test_output_columns_equal_canonical_columns(self, tmp_path: Path):
        rows = [
            _row(play_id=1, game_id=1, posteam="GER"),
            _row(play_id=2, game_id=1, posteam="OPP"),
        ]
        path = _write_legacy_csv(tmp_path / "legacy.csv", rows)

        df, _notices = ingest_legacy(path)

        assert list(df.columns) == list(CANONICAL_COLUMNS)

    def test_team_mapping_none_leaves_labels_untouched(self, tmp_path: Path):
        rows = [
            _row(play_id=1, game_id=1, posteam="GER"),
            _row(play_id=2, game_id=1, posteam="OPP"),
        ]
        path = _write_legacy_csv(tmp_path / "legacy.csv", rows)

        df, _notices = ingest_legacy(path, team_mapping=None)

        assert set(df["posteam"].to_list()) == {"GER", "OPP"}
        assert df["home_team"].unique().to_list() == ["GER"]

    def test_team_mapping_maps_labels(self, tmp_path: Path):
        rows = [
            _row(play_id=1, game_id=1, posteam="GER"),
            _row(play_id=2, game_id=1, posteam="OPP"),
        ]
        path = _write_legacy_csv(tmp_path / "legacy.csv", rows)
        mapping = pl.DataFrame(
            {
                "source": ["legacy", "legacy"],
                "source_team": ["GER", "OPP"],
                "canonical_team": ["DEU-W", "OPP-CANON"],
            },
            schema={"source": pl.Utf8, "source_team": pl.Utf8, "canonical_team": pl.Utf8},
        )

        df, _notices = ingest_legacy(path, team_mapping=mapping)

        assert set(df["posteam"].to_list()) == {"DEU-W", "OPP-CANON"}
        assert df["home_team"].unique().to_list() == ["DEU-W"]

    def test_team_mapping_raises_on_gap(self, tmp_path: Path):
        rows = [
            _row(play_id=1, game_id=1, posteam="GER"),
            _row(play_id=2, game_id=1, posteam="OPP"),
        ]
        path = _write_legacy_csv(tmp_path / "legacy.csv", rows)
        mapping = pl.DataFrame(
            {"source": ["legacy"], "source_team": ["GER"], "canonical_team": ["DEU-W"]},
            schema={"source": pl.Utf8, "source_team": pl.Utf8, "canonical_team": pl.Utf8},
        )

        with pytest.raises(UnmappedTeamError):
            ingest_legacy(path, team_mapping=mapping)


class TestIngestLegacyRealFile:
    def test_real_file_row_count_matches_baseline_and_manifest_hash(
        self, repo_root: Path, legacy_csv_path: Path
    ):
        manifest_path = repo_root / "tests" / "fixtures" / "baseline_manifest.json"
        if not manifest_path.exists():
            pytest.skip("baseline_manifest.json not present")
        manifest = json.loads(manifest_path.read_text())
        expected_hash = manifest["inputs"]["legacy_csv_sha256"]

        digest = hashlib.sha256()
        with open(legacy_csv_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != expected_hash:
            pytest.skip("data_raw.csv content differs from the frozen baseline manifest")

        raw = read_legacy(legacy_csv_path)
        df, _notices = ingest_legacy(legacy_csv_path)

        assert df.height == raw.height

    def test_real_file_posteam_home_away_non_null(self, legacy_csv_path: Path):
        df, _notices = ingest_legacy(legacy_csv_path)

        assert df["posteam"].null_count() == 0
        assert df["home_team"].null_count() == 0
        assert df["away_team"].null_count() == 0


class TestWarnOnlyValidationWiring:
    def test_run_checks_partition_keeps_legacy_rows_in_accepted(self, legacy_csv_path: Path):
        df, notices = ingest_legacy(legacy_csv_path)

        accepted, quarantined, _game_results = partition_games(
            df, notices.check_results, warn_only_sources=frozenset({"legacy"})
        )

        assert accepted.height == df.height
        assert quarantined.height == 0

    def test_ingest_legacy_records_check_outcomes_in_notices(self, legacy_csv_path: Path):
        df, notices = ingest_legacy(legacy_csv_path)

        assert isinstance(notices, IngestNotices)
        assert len(notices.check_results) > 0
        assert len(notices.messages) == len(notices.check_results)
        assert all(isinstance(m, str) for m in notices.messages)

    def test_fail_downgraded_to_warn_with_detail_preserved(self):
        df = canonical_plays(
            n_games=1,
            plays_per_game=4,
            source="legacy",
            overrides={"down": [1, 7, 2, 3]},
        )
        results = run_checks(df)

        _accepted, _quarantined, game_results = partition_games(
            df, results, warn_only_sources=frozenset({"legacy"})
        )

        gr = game_results[0]
        assert gr.quarantined is False
        downs_result = next(r for r in gr.results if r.check == "downs_range")
        assert downs_result.status == Status.WARN
        assert "7" in downs_result.detail

    def test_clean_legacy_game_reports_all_pass(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8, source="legacy")
        game_id = df["game_id"][0]
        final_scores = pl.DataFrame(
            {
                "game_id": [game_id],
                "home_team": ["HOME"],
                "away_team": ["AWAY"],
                "home_score": [0],
                "away_score": [0],
                "note": [None],
            },
            schema={
                "game_id": pl.Utf8,
                "home_team": pl.Utf8,
                "away_team": pl.Utf8,
                "home_score": pl.Int32,
                "away_score": pl.Int32,
                "note": pl.Utf8,
            },
        )

        results = run_checks(df, final_scores=final_scores)

        assert len(results) == 6
        assert all(r.status == Status.PASS for r in results)

    def test_score_reconstruction_skipped_without_reference(self):
        df = canonical_plays_with_scores(n_games=1, plays_per_game=8, source="legacy")

        results = run_checks(df, final_scores=None)

        score_result = next(r for r in results if r.check == "score_reconstruction")
        assert score_result.status == Status.SKIPPED
        assert score_result.status != Status.FAIL

    def test_warn_only_never_quarantines(self):
        """Deliberately broken legacy frame: gap in play_id, an out-of-range down."""
        df = canonical_plays(
            n_games=1,
            plays_per_game=8,
            source="legacy",
            overrides={
                "down": [1, 2, 3, 7, 1, 2, 3, 4],
                "play_id": [1, 2, 3, 5, 6, 7, 8, 9],
            },
        )
        results = run_checks(df)

        accepted, quarantined, game_results = partition_games(
            df, results, warn_only_sources=frozenset({"legacy"})
        )

        assert accepted.height == df.height
        assert quarantined.height == 0
        gr = game_results[0]
        assert gr.quarantined is False
        statuses = {r.check: r.status for r in gr.results}
        assert statuses["downs_range"] == Status.WARN
        assert statuses["gapless_play_ids"] == Status.WARN
