"""End-to-end tests for `flag_football_ep.pipeline.run_all` and `ffep run` (REQ-S1-04).

Builds a small multi-drive, multi-game synthetic Hudl tree (reusing `test_pipeline_ingest`'s
`_make_config`/`_write_reference_csvs`/`_write_toml_config` tree builders) that is
deliberately shaped -- alternating "score" and "turnover" drives, with `ODK` flipping on
turnover drives -- so `Drive_Score_Dist`/`score_differential` and `posteam` vary enough for
`make_ep_model_mutations`/`make_wp_model_mutations`'s sample-weight and label derivations
(mirrors `tests/test_model_train.py`'s synthetic-corpus rationale, but driven through real
Hudl CSV ingest so `ffep run`'s whole chain -- ingest -> train -> score -- is exercised, not
just the training stage in isolation).
"""

from __future__ import annotations

import socket
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

import test_pipeline_ingest as tpi
from flag_football_ep.cli import app
from flag_football_ep.config import Config
from flag_football_ep.pipeline import EmptyIngestResultError, RunAllResult, run_all

_CLI_RUNNER = CliRunner()

_HUDL_COLUMNS = tpi._HUDL_COLUMNS


def _write_drive_game(hudl_dir: Path, filename: str, drives: list[tuple[str, int]]) -> int:
    """Write a Hudl CSV built from `drives`: `(kind, n_plays)` pairs.

    "score": `n_plays - 1` rush plays then one `Rush, TD` (always `ODK=O`), followed by a
    separate PAT play -- so the drive closes on the TD (`derive_drive_id`'s closing-token
    rule) and the PAT itself opens the next drive.
    "turnover": `n_plays - 1` rush plays then one `Rush, Fumble` (also drive-closing, but
    not scoring) -- `ODK` alternates O/D across successive turnover drives so `posteam`
    varies too (needed for `make_wp_model_mutations`'s label, which is degenerate -- a
    single class -- if `posteam` never changes).

    Returns the total play count written (used to compute `half_boundaries.csv`'s
    `half2_first_play`).
    """
    rows: list[dict] = []
    play_num = 1
    yard = 20
    turnover_idx = 0
    for kind, n_plays in drives:
        if kind == "score":
            odk = "O"
        else:
            odk = "O" if turnover_idx % 2 == 0 else "D"
            turnover_idx += 1
        for i in range(n_plays):
            is_last = i == n_plays - 1
            if is_last and kind == "score":
                result = "Rush, TD"
            elif is_last and kind == "turnover":
                result = "Rush, Fumble"
            else:
                result = "Rush"
            rows.append(
                {
                    "PLAY #": str(play_num),
                    "ODK": odk,
                    "DN": str((i % 4) + 1),
                    "DIST": "10",
                    "YARD LN": str(yard),
                    "PLAY TYPE": "Rush",
                    "RESULT": result,
                    "GN/LS": "5",
                }
            )
            play_num += 1
            yard = min(yard + 5, 45)
        if kind == "score":
            rows.append(
                {
                    "PLAY #": str(play_num),
                    "ODK": "O",
                    "DN": "0",
                    "DIST": "0",
                    "YARD LN": "5",
                    "PLAY TYPE": "PAT",
                    "RESULT": "Good",
                    "GN/LS": "0",
                }
            )
            play_num += 1
    tpi._write_hudl_csv(hudl_dir / filename, rows)
    return play_num - 1


# game filename -> (half1 drives, half2 drives). Every game has at least one "turnover"
# drive followed later (same half) by a "score" drive -- the shape that produces a
# non-degenerate Drive_Score_Dist range -- plus at least one ODK=D turnover drive, so
# `posteam` (and the WP label) varies too.
_GAMES: dict[str, dict[str, list[tuple[str, int]]]] = {
    "2026-01-01_GER-vs-AUT_EM.csv": {
        "half1": [("turnover", 3), ("turnover", 3), ("score", 4)],
        "half2": [("turnover", 3), ("score", 3)],
    },
    "2026-01-02_GER-vs-AUT_EM.csv": {
        "half1": [("score", 2), ("turnover", 4), ("turnover", 3)],
        "half2": [("turnover", 3), ("turnover", 3), ("score", 5)],
    },
    "2026-01-03_GER-vs-AUT_EM.csv": {
        "half1": [("turnover", 4), ("score", 3), ("turnover", 3)],
        "half2": [("score", 4), ("turnover", 3), ("score", 3)],
    },
}


def _write_training_tree(config: Config) -> None:
    """Write `_GAMES` plus matching reference CSVs into `config`'s raw/reference dirs."""
    tpi._write_reference_csvs(config.reference.half_boundaries.parent)

    hb_lines = []
    fs_lines = []
    for filename, halves in _GAMES.items():
        all_drives = halves["half1"] + halves["half2"]
        half1_len = _write_drive_game(config.paths.raw_hudl, filename, halves["half1"])
        _write_drive_game(config.paths.raw_hudl, filename, all_drives)
        n_td = sum(1 for kind, _ in all_drives if kind == "score")
        hb_lines.append(f"{filename},{half1_len + 1}\n")
        fs_lines.append(f"{filename.removesuffix('.csv')},GER,AUT,{n_td * 7},0,test\n")

    hb_path = config.reference.half_boundaries.parent / "half_boundaries.csv"
    hb_path.write_text(hb_path.read_text() + "".join(hb_lines))
    fs_path = config.reference.final_scores.parent / "final_scores.csv"
    fs_path.write_text(fs_path.read_text() + "".join(fs_lines))


@pytest.fixture
def training_tree(tmp_path: Path, repo_root: Path) -> Config:
    """A three-game synthetic Hudl tree with enough variation for a real (tiny) EP/WP fit."""
    config = tpi._make_config(tmp_path, repo_root)
    _write_training_tree(config)
    return config


@pytest.fixture
def training_tree_toml(tmp_path: Path, repo_root: Path) -> Path:
    """Same tree as `training_tree`, written as an `ffep.toml` for CLI tests."""
    config = tpi._make_config(tmp_path, repo_root)
    _write_training_tree(config)
    return tpi._write_toml_config(tmp_path, repo_root)


@pytest.fixture
def empty_tree(tmp_path: Path, repo_root: Path) -> Config:
    """A tree with only reference CSVs -- every raw source directory is empty/absent."""
    config = tpi._make_config(tmp_path, repo_root)
    tpi._write_reference_csvs(config.reference.half_boundaries.parent)
    return config


# --- run_all: artifacts, durations, tune forwarding --------------------------------------


def test_run_all_returns_run_ids_and_writes_scored_parquet(training_tree: Config) -> None:
    result = run_all(training_tree, tune=False)

    assert isinstance(result, RunAllResult)
    assert result.ep_run
    assert result.wp_run
    assert result.ep_run != result.wp_run
    assert result.scored_path == training_tree.paths.processed / "plays_scored.parquet"
    assert result.scored_path.exists()

    scored = pl.read_parquet(result.scored_path)
    assert scored.height == result.ingest.n_plays


def test_run_all_durations_carries_four_stage_keys(training_tree: Config) -> None:
    result = run_all(training_tree, tune=False)

    assert set(result.durations) == {"ingest", "train_ep", "train_wp", "score"}
    for stage, seconds in result.durations.items():
        assert seconds >= 0, f"{stage} duration must be non-negative"


def test_run_all_tune_forwarded_to_both_training_calls(
    training_tree: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`tune=True` reaches both `train_ep`/`train_wp` -- stubbed out so the test never runs
    the (slow, 100-eval-by-default) real hyperopt search; that behaviour is already covered
    by `tests/test_model_train.py`."""
    import flag_football_ep.model.score as score_module
    import flag_football_ep.model.train as train_module

    calls: dict[str, bool] = {}

    def fake_train_ep(plays, config, tune=False, max_evals=100, export_pkl=False):
        calls["ep"] = tune
        return "fake-ep-run"

    def fake_train_wp(plays, config, tune=False, max_evals=100, export_pkl=False):
        calls["wp"] = tune
        return "fake-wp-run"

    def fake_score_plays(plays, config, ep_run=None, wp_run=None):
        return plays

    monkeypatch.setattr(train_module, "train_ep", fake_train_ep)
    monkeypatch.setattr(train_module, "train_wp", fake_train_wp)
    monkeypatch.setattr(score_module, "score_plays", fake_score_plays)

    result = run_all(training_tree, tune=True)

    assert calls == {"ep": True, "wp": True}
    assert result.ep_run == "fake-ep-run"
    assert result.wp_run == "fake-wp-run"


# --- run_all: empty-ingest abort -----------------------------------------------------------


def test_run_all_aborts_on_empty_ingest_without_training(
    empty_tree: Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    import flag_football_ep.model.train as train_module

    def _fail_if_called(*args, **kwargs):
        raise AssertionError("train_ep must not be called when ingest produced zero plays")

    monkeypatch.setattr(train_module, "train_ep", _fail_if_called)
    monkeypatch.setattr(train_module, "train_wp", _fail_if_called)

    with pytest.raises(EmptyIngestResultError, match="zero accepted plays"):
        run_all(empty_tree, tune=False)


# --- ffep run: no network, CLI output ------------------------------------------------------


def test_cli_run_completes_with_default_skip_fetch_and_no_network(
    training_tree_toml: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _no_network(*args, **kwargs):
        raise AssertionError(
            "no network access should be attempted when --skip-fetch is the default"
        )

    monkeypatch.setattr(socket.socket, "connect", _no_network)

    result = _CLI_RUNNER.invoke(app, ["run", "--config", str(training_tree_toml)])

    assert result.exit_code == 0, result.output


def test_cli_run_output_contains_per_stage_durations(training_tree_toml: Path) -> None:
    result = _CLI_RUNNER.invoke(app, ["run", "--config", str(training_tree_toml)])

    assert result.exit_code == 0, result.output
    for stage in ("ingest", "train_ep", "train_wp", "score", "total"):
        assert stage in result.output, f"expected {stage!r} in CLI output:\n{result.output}"


def test_cli_run_output_reports_ep_and_wp_run_ids(training_tree_toml: Path) -> None:
    result = _CLI_RUNNER.invoke(app, ["run", "--config", str(training_tree_toml)])

    assert result.exit_code == 0, result.output
    assert "ep run:" in result.output
    assert "wp run:" in result.output
    assert "scored:" in result.output
