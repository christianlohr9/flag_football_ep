"""End-to-end tests for `flag_football_ep.pipeline`: four sources -> one
validated canonical dataset (REQ-S1-05, REQ-S1-06).

Builds a synthetic `tmp_path` data tree per test: two Hudl exports (one clean,
one with a `play_id` gap so it quarantines), a small legacy CSV carrying a
deliberate check failure (warn-only), the committed sportapp.fi snapshot
fixture (`tests/fixtures/sportapp/match-drives_TEST001.json` +
`match-v1_TEST001.json`), and a small synthetic IFAF snapshot, plus matching
reference CSVs. The real Hudl exports under `data/raw/hudl/` are git-ignored
and intentionally not used here -- everything below is deterministic and
independent of local data availability.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

from flag_football_ep import pipeline
from flag_football_ep.canonical import CANONICAL_COLUMNS
from flag_football_ep.cli import app
from flag_football_ep.config import (
    Config,
    IfafSource,
    Paths,
    ReferenceFiles,
    ReportSettings,
    Sources,
    SportappSource,
    TrainSettings,
)
from flag_football_ep.pipeline import IngestResult, run_ingest
from flag_football_ep.validation.checks import Status

FIXTURE_SPORTAPP_DIR = Path(__file__).parent / "fixtures" / "sportapp"
_CLI_RUNNER = CliRunner()

_HUDL_COLUMNS = ["PLAY #", "ODK", "DN", "DIST", "YARD LN", "PLAY TYPE", "RESULT", "GN/LS"]
_LEGACY_COLUMNS = [
    "PLAY #", "ODK", "DN", "DIST", "YARD LN", "RESULT",
    "yardline_50", "game_id", "play_id", "drive_id", "half", "posteam",
]


def _make_config(root: Path, repo_root: Path) -> Config:
    """Build a `Config` directly (no TOML round-trip) rooted at `root`."""
    data_root = root / "data"
    paths = Paths(
        data_root=data_root,
        raw_hudl=data_root / "raw" / "hudl",
        raw_sportapp=data_root / "raw" / "sportapp",
        raw_ifaf=data_root / "raw" / "ifaf",
        raw_legacy=data_root / "raw" / "legacy",
        processed=data_root / "processed",
        reference=data_root / "reference",
        models=root / "models",
        mlruns=root / "mlruns",
        contract=repo_root / "docs" / "data-contract.schema.json",
        reports=root / "reports",
    )
    reference = ReferenceFiles(
        half_boundaries=data_root / "reference" / "half_boundaries.csv",
        final_scores=data_root / "reference" / "final_scores.csv",
        team_mapping=data_root / "reference" / "team_mapping.csv",
        sportapp_games=data_root / "reference" / "sportapp_games.csv",
        competition_tier=data_root / "reference" / "competition_tier.csv",
        player_mapping=data_root / "reference" / "player_mapping.csv",
        group_opponents=data_root / "reference" / "group_opponents.csv",
    )
    sources = Sources(
        sportapp=SportappSource(base_url="https://example.invalid", api_key_env="SPORTAPP_API_KEY"),
        ifaf=IfafSource(base_url="https://example.invalid", tournament="test", api_key_env="CPX_API_KEY"),
    )
    train = TrainSettings(
        ep_experiment="ep_model", wp_experiment="wp_model", exclude_games_ep=[], exclude_games_wp=[]
    )
    report = ReportSettings(own_team="HOME", cycle_start_season=2026)
    return Config(paths=paths, reference=reference, sources=sources, train=train, report=report)


def _write_hudl_csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [";".join(_HUDL_COLUMNS)]
    for row in rows:
        lines.append(";".join(str(row.get(c, "")) for c in _HUDL_COLUMNS))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


def _write_legacy_csv(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [";".join(_LEGACY_COLUMNS)]
    for row in rows:
        lines.append(";".join(str(row.get(c, "")) for c in _LEGACY_COLUMNS))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path


# --- Hudl fixtures --------------------------------------------------------

_HUDL_CLEAN_FILENAME = "2026-06-14_GER-vs-AUT_EM.csv"
_HUDL_GAPPED_FILENAME = "2026-06-15_GER-vs-AUT_EM.csv"


def _write_hudl_clean_game(hudl_dir: Path) -> None:
    """A 3-play game: rush, rush+TD, PAT good. Final score GER 7 - AUT 0."""
    rows = [
        {"PLAY #": "1", "ODK": "O", "DN": "1", "DIST": "10", "YARD LN": "25",
         "PLAY TYPE": "Rush", "RESULT": "Rush", "GN/LS": "5"},
        {"PLAY #": "2", "ODK": "O", "DN": "2", "DIST": "5", "YARD LN": "5",
         "PLAY TYPE": "Rush", "RESULT": "Rush, TD", "GN/LS": "5"},
        {"PLAY #": "3", "ODK": "O", "DN": "0", "DIST": "0", "YARD LN": "5",
         "PLAY TYPE": "PAT", "RESULT": "Good", "GN/LS": "0"},
    ]
    _write_hudl_csv(hudl_dir / _HUDL_CLEAN_FILENAME, rows)


def _write_hudl_gapped_game(hudl_dir: Path) -> None:
    """PLAY # 1, 2, 4 -- a missing play_id 3 fails gapless_play_ids."""
    rows = [
        {"PLAY #": "1", "ODK": "O", "DN": "1", "DIST": "10", "YARD LN": "25",
         "PLAY TYPE": "Rush", "RESULT": "Rush", "GN/LS": "5"},
        {"PLAY #": "2", "ODK": "O", "DN": "2", "DIST": "5", "YARD LN": "20",
         "PLAY TYPE": "Rush", "RESULT": "Rush", "GN/LS": "5"},
        {"PLAY #": "4", "ODK": "O", "DN": "3", "DIST": "15", "YARD LN": "15",
         "PLAY TYPE": "Rush", "RESULT": "Rush", "GN/LS": "5"},
    ]
    _write_hudl_csv(hudl_dir / _HUDL_GAPPED_FILENAME, rows)


_HUDL_WRONG_DELIMITER_FILENAME = "2026-06-16_GER-vs-AUT_EM.csv"
_HUDL_MALFORMED_DN_FILENAME = "2026-06-17_GER-vs-AUT_EM.csv"


def _write_hudl_wrong_delimiter_file(hudl_dir: Path) -> str:
    """A filename matching the accepted pattern but comma-delimited content --
    `read_export` raises `WrongDelimiterError` (single-column parse). Returns
    the file's stem (its would-be game_id, if it had ingested).
    """
    path = hudl_dir / _HUDL_WRONG_DELIMITER_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(_HUDL_COLUMNS)]
    lines.append(",".join(["1", "O", "1", "10", "25", "Rush", "Rush", "5"]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")
    return path.stem


def _write_hudl_malformed_dn_game(hudl_dir: Path) -> None:
    """A 3-play game whose second row has a non-numeric DN cell -- `derive_identity_columns`
    casts it to a null `down` (strict=False), so `downs_range` FAILs on the null
    rather than raising and dropping the source.
    """
    rows = [
        {"PLAY #": "1", "ODK": "O", "DN": "1", "DIST": "10", "YARD LN": "25",
         "PLAY TYPE": "Rush", "RESULT": "Rush", "GN/LS": "5"},
        {"PLAY #": "2", "ODK": "O", "DN": "N/A", "DIST": "5", "YARD LN": "20",
         "PLAY TYPE": "Rush", "RESULT": "Rush", "GN/LS": "5"},
        {"PLAY #": "3", "ODK": "O", "DN": "0", "DIST": "0", "YARD LN": "5",
         "PLAY TYPE": "PAT", "RESULT": "Good", "GN/LS": "0"},
    ]
    _write_hudl_csv(hudl_dir / _HUDL_MALFORMED_DN_FILENAME, rows)


# --- Legacy fixture (deliberate check failure, warn-only) -----------------


def _write_legacy_game(legacy_dir: Path) -> None:
    """game_id=10: a second play with DN=9, out of the [0,4] downs range."""
    rows = [
        {"PLAY #": 1, "ODK": "x", "DN": 1, "DIST": 10, "YARD LN": 25, "RESULT": "Rush",
         "yardline_50": 25, "game_id": 10, "play_id": 1, "drive_id": 1, "half": 1, "posteam": "GER"},
        {"PLAY #": 2, "ODK": "x", "DN": 9, "DIST": 10, "YARD LN": 20, "RESULT": "Rush",
         "yardline_50": 20, "game_id": 10, "play_id": 2, "drive_id": 1, "half": 1, "posteam": "AUT"},
    ]
    _write_legacy_csv(legacy_dir / "data_raw.csv", rows)


# --- Sportapp fixture (copied from the committed fixture pair) ------------


def _write_sportapp_fixture(sportapp_dir: Path) -> None:
    sportapp_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(FIXTURE_SPORTAPP_DIR / "match-drives_TEST001.json", sportapp_dir)
    shutil.copy(FIXTURE_SPORTAPP_DIR / "match-v1_TEST001.json", sportapp_dir)


# --- IFAF fixture (synthetic, 3-play game GER 7 - USA 0) ------------------


def _write_ifaf_fixture(ifaf_dir: Path) -> None:
    ifaf_dir.mkdir(parents=True, exist_ok=True)
    plays = [
        {
            "gameId": "TESTG1", "playNumber": 1,
            "context": {"gameClockMs": 100000, "half": 1, "down": 1, "ballOn": 20,
                        "possessionTeamId": "w-ger", "score": {"home": 0, "away": 0}},
            "outcome": {"type": "COMPLETE_PASS", "pointsScored": None, "turnover": False},
            "description": {"text": "play 1"},
            "penalty": False,
        },
        {
            "gameId": "TESTG1", "playNumber": 2,
            "context": {"gameClockMs": 90000, "half": 1, "down": 2, "ballOn": 30,
                        "possessionTeamId": "w-ger", "score": {"home": 0, "away": 0}},
            "outcome": {"type": "TOUCHDOWN", "pointsScored": 6, "turnover": False},
            "description": {"text": "play 2"},
            "penalty": False,
        },
        {
            "gameId": "TESTG1", "playNumber": 3,
            "context": {"gameClockMs": 85000, "half": 1, "down": 0, "ballOn": 45,
                        "possessionTeamId": "w-ger", "score": {"home": 6, "away": 0}},
            "outcome": {"type": "XP1", "pointsScored": 1, "turnover": False},
            "description": {"text": "play 3"},
            "penalty": False,
        },
    ]
    (ifaf_dir / "unified-plays_TESTG1.json").write_text(json.dumps(plays), encoding="utf-8")
    games_meta = [
        {"id": "TESTG1", "tournamentId": "test", "homeTeam": {"id": "w-ger"}, "awayTeam": {"id": "w-usa"}}
    ]
    (ifaf_dir / "games.json").write_text(json.dumps(games_meta), encoding="utf-8")
    # `tournamentId: "test"` above must resolve to a real `competition` name (not null) --
    # REQ-S1-09's competition-tier adoption (plan 01.3-09) requires every row's
    # `(source, competition)` pair to be mapped before `train_ep`/`train_wp` can run;
    # a null `competition` (the pre-existing default when no `tournament_*.json` is present)
    # can never match a `competition_tier.csv` row. `_write_reference_csvs` below maps
    # `(ifaf, "IFAF Test Cup")`.
    (ifaf_dir / "tournament_test.json").write_text(
        json.dumps({"id": "test", "name": "IFAF Test Cup"}), encoding="utf-8"
    )


# --- Reference CSVs ---------------------------------------------------------


def _write_reference_csvs(reference_dir: Path) -> None:
    reference_dir.mkdir(parents=True, exist_ok=True)
    (reference_dir / "half_boundaries.csv").write_text(
        "filename,half2_first_play\n"
        f"{_HUDL_CLEAN_FILENAME},3\n"
        f"{_HUDL_GAPPED_FILENAME},2\n",
        encoding="utf-8",
    )
    (reference_dir / "final_scores.csv").write_text(
        "game_id,home_team,away_team,home_score,away_score,note\n"
        "2026-06-14_GER-vs-AUT_EM,GER,AUT,7,0,test\n"
        "2026-06-15_GER-vs-AUT_EM,GER,AUT,0,0,test\n"
        "ifaf-TESTG1,GER,USA,7,0,test\n",
        encoding="utf-8",
    )
    (reference_dir / "team_mapping.csv").write_text(
        "source,source_team,canonical_team\n"
        "hudl,GER,GER\n"
        "hudl,AUT,AUT\n"
        "legacy,GER,GER\n"
        "legacy,AUT,AUT\n"
        "sportapp,101,HOM\n"
        "sportapp,102,AWY\n"
        "ifaf,w-ger,GER\n"
        "ifaf,w-usa,USA\n",
        encoding="utf-8",
    )
    (reference_dir / "sportapp_games.csv").write_text(
        "source_game_id,competition,season,note\n", encoding="utf-8"
    )
    # REQ-S1-09 adoption (plan 01.3-09): `train_ep`/`train_wp` now build the competition-tier
    # one-hot columns unconditionally, which requires every ingested row's `(source,
    # competition)` pair to resolve here. Covers every source/competition pair this module's
    # fixtures produce: Hudl filenames end `_EM.csv` (competition "EM"); `ingest_legacy`
    # hardcodes `competition="legacy"`; the sportapp.fi fixture's `region` is "TestCup"; the
    # IFAF fixture's `tournament_test.json` (added alongside this file) names "IFAF Test Cup".
    (reference_dir / "competition_tier.csv").write_text(
        "source,competition,tier\n"
        "hudl,EM,womens-international\n"
        "legacy,legacy,mixed-other\n"
        "sportapp,TestCup,mixed-other\n"
        "ifaf,IFAF Test Cup,womens-international\n",
        encoding="utf-8",
    )
    (reference_dir / "player_mapping.csv").write_text(
        "source,source_player,canonical_player\nhudl,Test Player,Test Player\n",
        encoding="utf-8",
    )
    (reference_dir / "group_opponents.csv").write_text(
        "canonical_team,team_name\nAUT,Austria\n",
        encoding="utf-8",
    )


@pytest.fixture
def full_tree(tmp_path: Path, repo_root: Path) -> Config:
    """A complete synthetic data tree covering all four sources."""
    config = _make_config(tmp_path, repo_root)
    _write_hudl_clean_game(config.paths.raw_hudl)
    _write_hudl_gapped_game(config.paths.raw_hudl)
    _write_legacy_game(config.paths.raw_legacy)
    _write_sportapp_fixture(config.paths.raw_sportapp)
    _write_ifaf_fixture(config.paths.raw_ifaf)
    _write_reference_csvs(config.reference.half_boundaries.parent)
    return config


@pytest.fixture
def hudl_only_tree(tmp_path: Path, repo_root: Path) -> Config:
    """Only the Hudl directory is populated; the other three raw dirs don't exist."""
    config = _make_config(tmp_path, repo_root)
    _write_hudl_clean_game(config.paths.raw_hudl)
    _write_reference_csvs(config.reference.half_boundaries.parent)
    return config


@pytest.fixture
def hudl_clean_only_tree(tmp_path: Path, repo_root: Path) -> Config:
    """Only the clean Hudl game -- no quarantine anywhere in this tree."""
    config = _make_config(tmp_path, repo_root)
    _write_hudl_clean_game(config.paths.raw_hudl)
    _write_reference_csvs(config.reference.half_boundaries.parent)
    return config


def _write_toml_config(root: Path, repo_root: Path) -> Path:
    """Write an `ffep.toml` at `root` pointing at the same tree `_make_config` would."""
    data_root = root / "data"
    toml_text = f"""
[paths]
data_root = "{data_root}"
raw_hudl = "{data_root / "raw" / "hudl"}"
raw_sportapp = "{data_root / "raw" / "sportapp"}"
raw_ifaf = "{data_root / "raw" / "ifaf"}"
raw_legacy = "{data_root / "raw" / "legacy"}"
processed = "{data_root / "processed"}"
reference = "{data_root / "reference"}"
models = "{root / "models"}"
mlruns = "{root / "mlruns"}"
contract = "{repo_root / "docs" / "data-contract.schema.json"}"
reports = "{root / "reports"}"

[reference]
half_boundaries = "{data_root / "reference" / "half_boundaries.csv"}"
final_scores = "{data_root / "reference" / "final_scores.csv"}"
team_mapping = "{data_root / "reference" / "team_mapping.csv"}"
sportapp_games = "{data_root / "reference" / "sportapp_games.csv"}"
competition_tier = "{data_root / "reference" / "competition_tier.csv"}"
player_mapping = "{data_root / "reference" / "player_mapping.csv"}"
group_opponents = "{data_root / "reference" / "group_opponents.csv"}"

[sources.sportapp]
base_url = "https://example.invalid"
api_key_env = "SPORTAPP_API_KEY"

[sources.ifaf]
base_url = "https://example.invalid"
tournament = "test"
api_key_env = "CPX_API_KEY"

[train]
ep_experiment = "ep_model"
wp_experiment = "wp_model"
exclude_games_ep = []
exclude_games_wp = []

[report]
own_team = "HOME"
cycle_start_season = 2026
"""
    config_path = root / "ffep.toml"
    config_path.write_text(toml_text, encoding="utf-8")
    return config_path


@pytest.fixture
def hudl_clean_only_toml(tmp_path: Path, repo_root: Path) -> Path:
    """Same tree as `hudl_clean_only_tree`, but as a TOML config for CLI tests."""
    config = _make_config(tmp_path, repo_root)
    _write_hudl_clean_game(config.paths.raw_hudl)
    _write_reference_csvs(config.reference.half_boundaries.parent)
    return _write_toml_config(tmp_path, repo_root)


@pytest.fixture
def hudl_both_games_toml(tmp_path: Path, repo_root: Path) -> Path:
    """Clean + gapped Hudl games, as a TOML config -- the gapped game quarantines."""
    config = _make_config(tmp_path, repo_root)
    _write_hudl_clean_game(config.paths.raw_hudl)
    _write_hudl_gapped_game(config.paths.raw_hudl)
    _write_reference_csvs(config.reference.half_boundaries.parent)
    return _write_toml_config(tmp_path, repo_root)


# --- run_ingest orchestration -----------------------------------------------


def test_run_ingest_all_sources_n_plays_equals_sum_of_accepted_rows(full_tree: Config) -> None:
    result = run_ingest(full_tree, ["hudl", "legacy", "sportapp", "ifaf"])

    assert isinstance(result, IngestResult)
    # clean hudl (3) + legacy (2, warn-only, both rows kept) + sportapp (6) + ifaf (3)
    # -- the gapped hudl game (3 rows) is quarantined and excluded from n_plays.
    assert result.n_plays == 3 + 2 + 6 + 3


def test_run_ingest_ifaf_malformed_play_does_not_drop_the_source(full_tree: Config) -> None:
    """Regression guard for 01.2-VERIFICATION.md Truth 5 (partial) and
    01.2-REVIEW.md WR-01: one malformed IFAF game (a null `playNumber`) must
    not remove the other IFAF games from the run.
    """
    ifaf_dir = full_tree.paths.raw_ifaf
    malformed_plays = [
        {
            "gameId": "TESTG2", "playNumber": None,
            "context": {"gameClockMs": 100000, "half": 1, "down": 1, "ballOn": 20,
                        "possessionTeamId": "w-ger", "score": {"home": 0, "away": 0}},
            "outcome": {"type": "COMPLETE_PASS", "pointsScored": None, "turnover": False},
            "description": {"text": "malformed playNumber"},
            "penalty": False,
        },
    ]
    (ifaf_dir / "unified-plays_TESTG2.json").write_text(
        json.dumps(malformed_plays), encoding="utf-8"
    )
    games_meta = [
        {"id": "TESTG1", "tournamentId": "test", "homeTeam": {"id": "w-ger"}, "awayTeam": {"id": "w-usa"}},
        {"id": "TESTG2", "tournamentId": "test", "homeTeam": {"id": "w-ger"}, "awayTeam": {"id": "w-usa"}},
    ]
    (ifaf_dir / "games.json").write_text(json.dumps(games_meta), encoding="utf-8")

    result = run_ingest(full_tree, ["ifaf"])

    assert "ifaf-TESTG1" in {g.game_id for g in result.game_results}
    by_id = {g.game_id: g for g in result.game_results}
    assert by_id["ifaf-TESTG1"].quarantined is False
    assert result.n_plays >= 3
    assert not any("source-level failure" in n for n in result.notices)


def test_run_ingest_missing_source_directory_skipped_with_notice(tmp_path: Path, repo_root: Path) -> None:
    config = _make_config(tmp_path, repo_root)
    _write_hudl_clean_game(config.paths.raw_hudl)
    _write_reference_csvs(config.reference.half_boundaries.parent)
    # raw_sportapp/raw_ifaf/raw_legacy are never created.

    result = run_ingest(config, ["hudl", "legacy", "sportapp", "ifaf"])

    assert result.n_plays == 3
    assert any("legacy" in n and "skipping" in n for n in result.notices)
    assert any("ifaf" in n and "skipping" in n for n in result.notices)


def test_run_ingest_one_failing_game_quarantined_others_still_ingest(full_tree: Config) -> None:
    result = run_ingest(full_tree, ["hudl"])

    by_id = {g.game_id: g for g in result.game_results}
    gapped_id = _HUDL_GAPPED_FILENAME.removesuffix(".csv")
    clean_id = _HUDL_CLEAN_FILENAME.removesuffix(".csv")

    assert by_id[gapped_id].quarantined is True
    assert any("gapless_play_ids" in reason for reason in by_id[gapped_id].reasons)
    assert by_id[clean_id].quarantined is False


def test_run_ingest_warn_only_sources_include_legacy_and_legacy_sportapp() -> None:
    assert "legacy" in pipeline._WARN_ONLY_SOURCES
    assert "legacy-sportapp" in pipeline._WARN_ONLY_SOURCES


def test_run_ingest_legacy_check_failure_downgraded_to_warn_not_quarantined(full_tree: Config) -> None:
    result = run_ingest(full_tree, ["legacy"])

    assert len(result.game_results) == 1
    game = result.game_results[0]
    assert game.source == "legacy"
    assert game.quarantined is False
    assert any("downs_range" in reason for reason in game.reasons)
    assert result.n_plays == 2


def test_run_ingest_strict_flag_does_not_change_written_output(full_tree: Config) -> None:
    strict_result = run_ingest(full_tree, ["hudl"], strict=True)
    lenient_result = run_ingest(full_tree, ["hudl"], strict=False)

    assert strict_result.n_plays == lenient_result.n_plays
    assert strict_result.n_quarantined == lenient_result.n_quarantined == 1


def test_run_ingest_reference_and_contract_loaded_exactly_once(
    full_tree: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"contract": 0, "half_boundaries": 0, "final_scores": 0, "team_mapping": 0}

    real_load_contract = pipeline.load_contract
    real_load_half_boundaries = pipeline.load_half_boundaries
    real_load_final_scores = pipeline.load_final_scores
    real_load_team_mapping = pipeline.load_team_mapping

    def counting_contract(*args, **kwargs):
        calls["contract"] += 1
        return real_load_contract(*args, **kwargs)

    def counting_half_boundaries(*args, **kwargs):
        calls["half_boundaries"] += 1
        return real_load_half_boundaries(*args, **kwargs)

    def counting_final_scores(*args, **kwargs):
        calls["final_scores"] += 1
        return real_load_final_scores(*args, **kwargs)

    def counting_team_mapping(*args, **kwargs):
        calls["team_mapping"] += 1
        return real_load_team_mapping(*args, **kwargs)

    monkeypatch.setattr(pipeline, "load_contract", counting_contract)
    monkeypatch.setattr(pipeline, "load_half_boundaries", counting_half_boundaries)
    monkeypatch.setattr(pipeline, "load_final_scores", counting_final_scores)
    monkeypatch.setattr(pipeline, "load_team_mapping", counting_team_mapping)

    run_ingest(full_tree, ["hudl", "legacy", "sportapp", "ifaf"])

    assert calls == {"contract": 1, "half_boundaries": 1, "final_scores": 1, "team_mapping": 1}


def test_run_ingest_game_results_cover_every_game_including_quarantined(full_tree: Config) -> None:
    result = run_ingest(full_tree, ["hudl"])

    game_ids = {g.game_id for g in result.game_results}
    assert game_ids == {
        _HUDL_CLEAN_FILENAME.removesuffix(".csv"),
        _HUDL_GAPPED_FILENAME.removesuffix(".csv"),
    }


def test_run_ingest_sources_hudl_only_does_not_touch_other_directories(hudl_only_tree: Config) -> None:
    result = run_ingest(hudl_only_tree, ["hudl"])

    assert result.n_plays == 3
    assert not any("legacy" in n for n in result.notices)
    assert not any("sportapp" in n for n in result.notices)
    assert not any("ifaf" in n for n in result.notices)


def test_run_ingest_unknown_source_raises_value_error_listing_valid_names(full_tree: Config) -> None:
    with pytest.raises(ValueError) as exc_info:
        run_ingest(full_tree, ["bogus"])

    message = str(exc_info.value)
    assert "bogus" in message
    for name in ("hudl", "legacy", "sportapp", "ifaf"):
        assert name in message


def test_pipeline_module_never_uses_diagonal_concat() -> None:
    source = Path(pipeline.__file__).read_text(encoding="utf-8")
    assert 'how="diagonal"' not in source
    assert "how='diagonal'" not in source


# --- atomic Parquet outputs and games metadata table ------------------------


def test_run_ingest_plays_parquet_columns_equal_canonical_no_quarantined_rows(
    full_tree: Config,
) -> None:
    run_ingest(full_tree, ["hudl"])

    plays = pl.read_parquet(full_tree.paths.processed / "plays.parquet")

    assert list(plays.columns) == list(CANONICAL_COLUMNS)
    gapped_id = _HUDL_GAPPED_FILENAME.removesuffix(".csv")
    assert gapped_id not in plays["game_id"].to_list()
    clean_id = _HUDL_CLEAN_FILENAME.removesuffix(".csv")
    assert set(plays["game_id"].to_list()) == {clean_id}


def test_run_ingest_games_parquet_one_row_per_game_all_eleven_columns(full_tree: Config) -> None:
    run_ingest(full_tree, ["hudl", "legacy"])

    games = pl.read_parquet(full_tree.paths.processed / "games.parquet")

    expected_columns = [
        "game_id", "source", "competition", "season", "home_team", "away_team",
        "n_plays", "n_drives", "status", "quarantine_reasons", "ingested_at",
    ]
    assert list(games.columns) == expected_columns
    # 2 hudl games (clean + gapped) + 1 legacy game.
    assert games.height == 3

    by_id = {row["game_id"]: row for row in games.to_dicts()}
    gapped_id = _HUDL_GAPPED_FILENAME.removesuffix(".csv")
    clean_id = _HUDL_CLEAN_FILENAME.removesuffix(".csv")
    assert by_id[gapped_id]["status"] == "quarantined"
    assert by_id[gapped_id]["quarantine_reasons"] is not None
    assert by_id[clean_id]["status"] == "accepted"
    assert by_id[clean_id]["quarantine_reasons"] is None
    assert by_id["legacy-10"]["status"] == "accepted-with-warnings"
    assert by_id["legacy-10"]["n_plays"] == 2


def test_run_ingest_validation_report_latest_matches_timestamped(full_tree: Config) -> None:
    result = run_ingest(full_tree, ["hudl"])

    latest_path = full_tree.paths.processed / "validation-report-latest.md"
    assert latest_path.read_text(encoding="utf-8") == result.report_path.read_text(encoding="utf-8")


def test_run_ingest_report_written_even_when_nothing_quarantined(
    hudl_clean_only_tree: Config,
) -> None:
    result = run_ingest(hudl_clean_only_tree, ["hudl"])

    assert result.report_path.exists()
    content = result.report_path.read_text(encoding="utf-8")
    assert "No games were quarantined in this run." in content


def test_run_ingest_out_dir_overrides_processed_directory(
    full_tree: Config, tmp_path: Path
) -> None:
    override_dir = tmp_path / "custom-out"

    result = run_ingest(full_tree, ["hudl"], out_dir=override_dir)

    assert result.plays_path == override_dir / "plays.parquet"
    assert result.games_path == override_dir / "games.parquet"
    assert result.plays_path.exists()
    assert result.games_path.exists()
    assert result.report_path.exists()
    assert not (full_tree.paths.processed / "plays.parquet").exists()


def test_run_ingest_atomic_write_failure_leaves_previous_plays_parquet_untouched(
    full_tree: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_ingest(full_tree, ["hudl"])
    processed_dir = full_tree.paths.processed
    plays_path = processed_dir / "plays.parquet"
    before = plays_path.read_bytes()

    real_write_parquet = pl.DataFrame.write_parquet

    def failing_write_parquet(self, path, *args, **kwargs):
        if Path(path).name.startswith("games"):
            raise RuntimeError("simulated games write failure")
        return real_write_parquet(self, path, *args, **kwargs)

    monkeypatch.setattr(pl.DataFrame, "write_parquet", failing_write_parquet)

    with pytest.raises(RuntimeError, match="simulated games write failure"):
        run_ingest(full_tree, ["hudl"])

    assert plays_path.read_bytes() == before
    assert list(processed_dir.glob("*.tmp")) == []


def test_pipeline_module_uses_os_replace() -> None:
    source = Path(pipeline.__file__).read_text(encoding="utf-8")
    assert source.count("os.replace") >= 1


# --- CLI integration ---------------------------------------------------------


def test_cli_ingest_hudl_produces_the_three_artifacts(hudl_clean_only_toml: Path) -> None:
    result = _CLI_RUNNER.invoke(app, ["ingest", "--config", str(hudl_clean_only_toml), "--source", "hudl"])

    assert result.exit_code == 0, result.output

    processed_dir = hudl_clean_only_toml.parent / "data" / "processed"
    assert (processed_dir / "plays.parquet").exists()
    assert (processed_dir / "games.parquet").exists()
    assert (processed_dir / "validation-report-latest.md").exists()

    plays = pl.read_parquet(processed_dir / "plays.parquet")
    assert plays.height == 3


def test_cli_ingest_strict_exits_1_with_quarantined_game(hudl_both_games_toml: Path) -> None:
    result = _CLI_RUNNER.invoke(
        app, ["ingest", "--config", str(hudl_both_games_toml), "--source", "hudl", "--strict"]
    )

    assert result.exit_code == 1


def test_cli_ingest_strict_exits_0_without_quarantine(hudl_clean_only_toml: Path) -> None:
    result = _CLI_RUNNER.invoke(
        app, ["ingest", "--config", str(hudl_clean_only_toml), "--source", "hudl", "--strict"]
    )

    assert result.exit_code == 0, result.output


# --- End-to-end: a broken export is contained AND visible (CR-02 / WR-01) ---


def test_run_ingest_wrong_delimiter_file_does_not_drop_source_and_is_named_in_report(
    tmp_path: Path, repo_root: Path
) -> None:
    config = _make_config(tmp_path, repo_root)
    _write_hudl_clean_game(config.paths.raw_hudl)
    broken_stem = _write_hudl_wrong_delimiter_file(config.paths.raw_hudl)
    _write_reference_csvs(config.reference.half_boundaries.parent)

    result = run_ingest(config, ["hudl"])

    plays = pl.read_parquet(result.plays_path)
    clean_id = _HUDL_CLEAN_FILENAME.removesuffix(".csv")
    assert plays.height == 3
    assert set(plays["game_id"].to_list()) == {clean_id}

    report_text = Path(result.report_path).read_text(encoding="utf-8")
    skipped_section = report_text[report_text.index("## Skipped files") :]
    assert broken_stem in skipped_section
    assert "WrongDelimiterError" in skipped_section


def test_run_ingest_missing_source_report_shows_source_notices_section(
    tmp_path: Path, repo_root: Path
) -> None:
    config = _make_config(tmp_path, repo_root)
    _write_hudl_clean_game(config.paths.raw_hudl)
    _write_reference_csvs(config.reference.half_boundaries.parent)
    # raw_ifaf/raw_legacy are never created.

    result = run_ingest(config, ["hudl", "legacy", "sportapp", "ifaf"])

    assert any("legacy" in n and "skipping" in n for n in result.notices)
    assert any("ifaf" in n and "skipping" in n for n in result.notices)

    report_text = Path(result.report_path).read_text(encoding="utf-8")
    source_section = report_text[
        report_text.index("## Source notices") : report_text.index("## Summary")
    ]
    assert "## Source notices" in report_text
    assert any("legacy" in line and "skipping" in line for line in source_section.splitlines())
    assert any("ifaf" in line and "skipping" in line for line in source_section.splitlines())


def test_cli_ingest_missing_ifaf_source_prints_notice_and_exits_0(hudl_clean_only_toml: Path) -> None:
    result = _CLI_RUNNER.invoke(
        app, ["ingest", "--config", str(hudl_clean_only_toml), "--source", "hudl", "--source", "ifaf"]
    )

    assert result.exit_code == 0, result.output
    assert any(
        line.startswith("notice: ") and "ifaf" in line for line in result.output.splitlines()
    )


def test_run_ingest_malformed_dn_cell_game_reaches_game_results_with_downs_range_fail(
    tmp_path: Path, repo_root: Path
) -> None:
    config = _make_config(tmp_path, repo_root)
    _write_hudl_malformed_dn_game(config.paths.raw_hudl)
    _write_reference_csvs(config.reference.half_boundaries.parent)

    result = run_ingest(config, ["hudl"])

    game_id = _HUDL_MALFORMED_DN_FILENAME.removesuffix(".csv")
    by_id = {g.game_id: g for g in result.game_results}
    assert game_id in by_id

    downs_result = next(r for r in by_id[game_id].results if r.check == "downs_range")
    assert downs_result.status == Status.FAIL
